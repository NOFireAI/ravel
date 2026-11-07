//! Timestamp codec bake-off (issue #312, ADR-0092 decision 6; ADR-3694
//! decision 7 adds the two challengers below, timestamps first).
//!
//! Compares five timestamp encoders on the same streams:
//!
//! 1. **production**: the RSEG TS page the writer frames through
//!    `encode_run_v4` (`segment_support::production_ts_run`), which keeps the
//!    smaller of enc 1 `TS_DELTA_VARINT` (with the 64-byte-floor LZ4 rule)
//!    and enc 2 `TS_GCD_I64`; the row notes which one it picked. Its byte
//!    count *includes the 6-byte page header* (enc, comp, crc) and every
//!    other codec's does not, so each row prints two ratios: as measured,
//!    and header-corrected (challenger bytes against production bytes minus
//!    6). The adoption summary judges the header-corrected one.
//! 2. **encode_i64**: `ravel_codec::encoding::encode_i64` applied to the raw
//!    timestamps (the integer path RSEG v7 uses for its provenance columns).
//! 3. **encode_i64/gcd (enc 2 payload)**: production enc 2 `TS_GCD_I64`'s
//!    payload grammar (docs/segment-format.md) -- `uvarint gcd`, one `Enc`
//!    tag byte, then `encode_i64` of every timestamp divided by the page's
//!    greatest common divisor. Where production picks enc 2 this row equals
//!    production's payload; it is a reference, not a challenger. The divisor
//!    found is reported.
//! 4. **dod_for (challenger a)**: delta-of-delta with a jitter-tolerant
//!    residual -- first value and first delta zigzag-varint, each later delta
//!    coded as its difference from the previous delta, those residuals
//!    frame-of-reference bit-packed per fixed-size block (so one jittery
//!    block costs its own bit width, not the whole page's). Each block is
//!    `zigzag-varint min`, a bit-width byte, then the packed offsets. The
//!    payload carries no sample count: production's TS page does not either
//!    (the reader takes it from the run entry), and every row here decodes
//!    with the count supplied from outside.
//! 5. **gcd_forced_dod (challenger b)**: `uvarint gcd`, then the page divided
//!    by that GCD forced onto the exact `enc_double_delta_i64` arm of `encode_i64`
//!    (absolute first value, first delta, then each later delta's
//!    difference from the previous delta, every field a zigzag varint,
//!    `checked_sub` overflow-checked) -- measured on its own, never as part
//!    of `encode_i64`'s best-of search.
//!
//! Decode throughput is the median over [`MEASURE_ROUNDS`] timed rounds, as
//! in `codec_bakeoff`. RSEG exposes no public TS-only decoder, so the
//! production figure decodes the whole run through `decode_run_pages_soa`,
//! an all-zero VAL page included. That charges production a cost the
//! challengers never pay: a challenger slower than production here is slower
//! than it, while one faster here may not be faster than a TS-only decode.
//!
//! Report-only, per ADR-0092 decision 6 and ADR-3694 decision 7: the numbers
//! feed a future format ADR, they adopt nothing. A challenger is adopted on a
//! row only if it round-trips bit-exactly, costs at most 75% of production's
//! bytes, and decodes at least as fast.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::hint::black_box;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use ravel_bench::bench_env::env_header;
use ravel_bench::metrics_stream_sample::sample_series;
use ravel_bench::segment_support::{decode_scalar_run, production_ts_run};
use ravel_codec::encoding::{Enc, decode_i64, encode_i64};

/// Page lengths measured, from the single-sample fragmentation floor to a
/// full hour of 1 s data.
const PAGE_LENS: [usize; 6] = [1, 2, 12, 60, 240, 3600];

/// The RSEG page header (enc, comp, crc) production's byte count carries.
const PAGE_HEADER_BYTES: usize = 6;
/// Timed rounds per decode-throughput cell; the median is reported.
const MEASURE_ROUNDS: usize = 9;
/// Samples decoded per timed round: a round repeats the page decode
/// `SAMPLES_PER_ROUND / n` times so small pages still span many microseconds.
const SAMPLES_PER_ROUND: usize = 400_000;
/// Adoption threshold on the header-corrected byte ratio.
const BYTE_RATIO_MAX: f64 = 0.75;

const PRODUCTION: &str = "production";
const DOD_FOR: &str = "dod_for";
const GCD_FORCED_DOD: &str = "gcd_forced_dod";
/// Every codec row a page reports, in print order.
const CODECS: [&str; 5] = [
    PRODUCTION,
    "enc_i64",
    "enc_i64/gcd",
    DOD_FOR,
    GCD_FORCED_DOD,
];
/// The codecs the adoption summary judges.
const CHALLENGERS: [&str; 2] = [DOD_FOR, GCD_FORCED_DOD];

/// MetricsBench profiles drawn for timestamp streams, with the scrape steps
/// drawn. Cardinality (1,000,000 active series) costs about 13.5 s per step
/// on a Raspberry Pi 5 because every step generates every active series
/// before the sampler filters, so it is held to 8 steps. History (10,000
/// active series) is cheap per step; 256 steps leave headroom over the 240
/// page length for samples the generator's missing-sample anomaly drops.
const METRICS_PROFILES: [(&str, u64); 2] = [("cardinality", 8), ("history", 256)];

const START_NS: i64 = 1_700_000_000_000_000_000;
const STEP_15S_NS: i64 = 15_000_000_000;
const MS_NS: i64 = 1_000_000;

fn gcd_i64(a: i64, b: i64) -> i64 {
    let (mut a, mut b) = (a.unsigned_abs(), b.unsigned_abs());
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a as i64
}

/// GCD of all timestamps in the page (0 only if every value is 0).
fn page_gcd(ts: &[i64]) -> i64 {
    ts.iter().fold(0i64, |acc, &v| gcd_i64(acc, v)).max(1)
}

/// Builds `n` timestamps for the named stream. Deterministic in `(kind, n)`:
/// fixed per-kind seed, no wall-clock time.
fn stream(kind: &str, n: usize) -> Vec<i64> {
    let mut rng = StdRng::seed_from_u64(0x7357_0000 ^ (n as u64) ^ seed_of(kind));
    match kind {
        // Exactly regular 15 s, millisecond resolution (every value a
        // multiple of 1e6 ns; all deltas identical).
        "regular_ms" => (0..n).map(|i| START_NS + i as i64 * STEP_15S_NS).collect(),
        // 15 s with +/-200 ms jitter, still millisecond resolution.
        "jitter_ms" => (0..n)
            .map(|i| {
                let jitter = rng.random_range(-200..=200) * MS_NS;
                START_NS + i as i64 * STEP_15S_NS + jitter
            })
            .collect(),
        // True-nanosecond timestamps: 15 s nominal, +/-200 ms jitter at
        // full nanosecond resolution (not ms-aligned), as OTLP delivers.
        "nanos_jitter" => (0..n)
            .map(|i| {
                let jitter = rng.random_range(-200_000_000..=200_000_000);
                START_NS + i as i64 * STEP_15S_NS + jitter
            })
            .collect(),
        // Irregular intervals: 1 s..60 s gaps at nanosecond resolution.
        "irregular" => {
            let mut ts = START_NS;
            (0..n)
                .map(|_| {
                    ts += rng.random_range(1..=60) * 1_000_000_000;
                    ts
                })
                .collect()
        }
        other => panic!("unknown stream {other}"),
    }
}

fn seed_of(kind: &str) -> u64 {
    kind.bytes().fold(1469598103934665603u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(1099511628211)
    })
}

/// Every stream is sorted with one sample per timestamp: a run reaches the
/// writer in that shape (`encode_run_v4`'s contract), and the reader rejects
/// a run whose timestamps leave its min/max bounds.
const MULTI_STREAMS: [&str; 4] = ["regular_ms", "jitter_ms", "nanos_jitter", "irregular"];

// --- self-contained varint + frame-of-reference helpers (ADR-3694 decision
// 7 challengers only; deliberately not shared with crates/ravel-codec or
// ravel-bench::codecs, since a bakeoff challenger must not depend on the
// thing it is being measured against) -------------------------------------

fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

fn get_uvarint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let b = *bytes.get(*pos)?;
        *pos += 1;
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Some(result)
}

fn put_ivarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, zigzag_encode(v));
}

fn get_ivarint(bytes: &[u8], pos: &mut usize) -> Option<i64> {
    get_uvarint(bytes, pos).map(zigzag_decode)
}

/// Frame-of-reference bit-pack of one block: `zigzag min`, a bit-width byte,
/// then the `(value - min)` offsets packed LSB-first across byte boundaries.
/// The offset is a wrapping difference: a block holding both extremes of the
/// i64 range spans more than `i64::MAX`, and its offset still fits a u64.
fn for_pack_block(out: &mut Vec<u8>, block: &[i64]) {
    let min = *block.iter().min().expect("block is non-empty");
    put_ivarint(out, min);
    let offsets: Vec<u64> = block
        .iter()
        .map(|&v| (v as u64).wrapping_sub(min as u64))
        .collect();
    let max_off = offsets.iter().copied().max().unwrap_or(0);
    let bit_width: u32 = 64 - max_off.leading_zeros();
    out.push(bit_width as u8);
    if bit_width == 0 {
        return;
    }
    let mut acc: u128 = 0;
    let mut acc_bits: u32 = 0;
    for &off in &offsets {
        acc |= u128::from(off) << acc_bits;
        acc_bits += bit_width;
        while acc_bits >= 8 {
            out.push((acc & 0xff) as u8);
            acc >>= 8;
            acc_bits -= 8;
        }
    }
    if acc_bits > 0 {
        out.push((acc & 0xff) as u8);
    }
}

fn for_unpack_block(bytes: &[u8], pos: &mut usize, count: usize) -> Option<Vec<i64>> {
    let min = get_ivarint(bytes, pos)?;
    let bit_width = u32::from(*bytes.get(*pos)?);
    *pos += 1;
    if bit_width == 0 {
        return Some(vec![min; count]);
    }
    if bit_width > 64 {
        return None;
    }
    let mask: u128 = (1u128 << bit_width) - 1;
    let mut acc: u128 = 0;
    let mut acc_bits: u32 = 0;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        while acc_bits < bit_width {
            let b = *bytes.get(*pos)?;
            *pos += 1;
            acc |= u128::from(b) << acc_bits;
            acc_bits += 8;
        }
        let off = (acc & mask) as u64;
        acc >>= bit_width;
        acc_bits -= bit_width;
        out.push((min as u64).wrapping_add(off) as i64);
    }
    Some(out)
}

/// Blocks of jitter-tolerant delta-of-delta residuals: a bakeoff-wide fixed
/// size, independent of page length, so a jittery run of samples only costs
/// its own block's bit width rather than widening the whole page.
const DOD_BLOCK_LEN: usize = 128;

/// Challenger (a): first value absolute, first delta zigzag-varint, each
/// later delta's difference from the previous delta frame-of-reference
/// bit-packed per `DOD_BLOCK_LEN`-sized block. No sample count: like
/// production's TS page, the reader takes it from outside. `None` on i64
/// overflow.
fn dod_for_encode(values: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let Some(&first) = values.first() else {
        return Some(out);
    };
    put_ivarint(&mut out, first);
    if values.len() < 2 {
        return Some(out);
    }
    let mut prev_delta = values[1].checked_sub(values[0])?;
    put_ivarint(&mut out, prev_delta);
    if values.len() < 3 {
        return Some(out);
    }
    let mut dods = Vec::with_capacity(values.len() - 2);
    for i in 2..values.len() {
        let delta = values[i].checked_sub(values[i - 1])?;
        dods.push(delta.checked_sub(prev_delta)?);
        prev_delta = delta;
    }
    for block in dods.chunks(DOD_BLOCK_LEN) {
        for_pack_block(&mut out, block);
    }
    Some(out)
}

fn dod_for_decode(bytes: &[u8], n: usize) -> Option<Vec<i64>> {
    let mut pos = 0usize;
    let mut out = Vec::with_capacity(n);
    if n == 0 {
        return Some(out);
    }
    let v0 = get_ivarint(bytes, &mut pos)?;
    out.push(v0);
    if n == 1 {
        return Some(out);
    }
    let first_delta = get_ivarint(bytes, &mut pos)?;
    out.push(v0.checked_add(first_delta)?);
    if n == 2 {
        return Some(out);
    }
    let mut remaining = n - 2;
    let mut prev_delta = first_delta;
    while remaining > 0 {
        let this_block = remaining.min(DOD_BLOCK_LEN);
        let dods = for_unpack_block(bytes, &mut pos, this_block)?;
        for dod in dods {
            let delta = prev_delta.checked_add(dod)?;
            let last = *out.last()?;
            out.push(last.checked_add(delta)?);
            prev_delta = delta;
        }
        remaining -= this_block;
    }
    Some(out)
}

/// Challenger (b): mirrors `ravel_codec::encoding`'s private
/// `enc_double_delta_i64` exactly -- absolute first value, first delta, then
/// each later delta's difference from the previous delta, every field a
/// zigzag varint, `checked_sub`-guarded. `None` on i64 overflow.
fn double_delta_encode(values: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let Some(&first) = values.first() else {
        return Some(out);
    };
    put_ivarint(&mut out, first);
    if values.len() < 2 {
        return Some(out);
    }
    let mut prev_delta = values[1].checked_sub(values[0])?;
    put_ivarint(&mut out, prev_delta);
    for i in 2..values.len() {
        let delta = values[i].checked_sub(values[i - 1])?;
        put_ivarint(&mut out, delta.checked_sub(prev_delta)?);
        prev_delta = delta;
    }
    Some(out)
}

fn double_delta_decode(bytes: &[u8], count: usize) -> Option<Vec<i64>> {
    let mut pos = 0usize;
    let mut out = Vec::with_capacity(count);
    if count == 0 {
        return Some(out);
    }
    let v0 = get_ivarint(bytes, &mut pos)?;
    out.push(v0);
    if count == 1 {
        return Some(out);
    }
    let first_delta = get_ivarint(bytes, &mut pos)?;
    out.push(v0.checked_add(first_delta)?);
    let mut prev_delta = first_delta;
    for _ in 2..count {
        let dod = get_ivarint(bytes, &mut pos)?;
        let delta = prev_delta.checked_add(dod)?;
        let last = *out.last()?;
        out.push(last.checked_add(delta)?);
        prev_delta = delta;
    }
    Some(out)
}

/// Multiplies a decoded GCD-divided page back by its divisor; `None` on
/// overflow, which a page the encoder divided never produces.
fn multiply_back(divided: Vec<i64>, g: i64) -> Option<Vec<i64>> {
    divided.into_iter().map(|v| v.checked_mul(g)).collect()
}

/// Reads the `uvarint gcd` both GCD codecs lead their payload with; `None`
/// for a zero divisor or one that does not fit an i64.
fn get_divisor(bytes: &[u8], pos: &mut usize) -> Option<i64> {
    i64::try_from(get_uvarint(bytes, pos)?)
        .ok()
        .filter(|&g| g > 0)
}

/// `encode_i64/gcd`: production enc 2 `TS_GCD_I64`'s payload grammar --
/// `uvarint gcd`, one `Enc` tag byte, then the `encode_i64` bytes of the
/// page divided by its GCD.
fn encode_i64_gcd(ts: &[i64]) -> Vec<u8> {
    let g = page_gcd(ts);
    let divided: Vec<i64> = ts.iter().map(|&v| v / g).collect();
    let (enc, bytes) = encode_i64(&divided);
    let mut out = Vec::with_capacity(bytes.len() + 11);
    put_uvarint(&mut out, g.unsigned_abs());
    out.push(enc.to_u8());
    out.extend_from_slice(&bytes);
    out
}

/// `encode_i64/gcd`'s decode: divisor and tag from the payload,
/// `decode_i64`, then the divisor multiplied back.
fn decode_i64_gcd(bytes: &[u8], n: usize) -> Option<Vec<i64>> {
    let mut pos = 0usize;
    let g = get_divisor(bytes, &mut pos)?;
    let enc = Enc::from_u8(*bytes.get(pos)?).ok()?;
    multiply_back(decode_i64(enc, bytes.get(pos + 1..)?, n).ok()?, g)
}

/// `gcd_forced_dod`: `uvarint gcd`, then the forced double-delta arm over
/// the page divided by its GCD. `None` on i64 overflow.
fn gcd_forced_dod_encode(ts: &[i64]) -> Option<Vec<u8>> {
    let g = page_gcd(ts);
    let divided: Vec<i64> = ts.iter().map(|&v| v / g).collect();
    let body = double_delta_encode(&divided)?;
    let mut out = Vec::with_capacity(body.len() + 10);
    put_uvarint(&mut out, g.unsigned_abs());
    out.extend_from_slice(&body);
    Some(out)
}

/// `gcd_forced_dod`'s decode: divisor from the payload, the forced
/// double-delta arm, then the divisor multiplied back.
fn gcd_forced_dod_decode(bytes: &[u8], n: usize) -> Option<Vec<i64>> {
    let mut pos = 0usize;
    let g = get_divisor(bytes, &mut pos)?;
    multiply_back(double_delta_decode(bytes.get(pos..)?, n)?, g)
}

/// Decode timing knobs. Tests shrink them so the report guard stays cheap.
#[derive(Clone, Copy)]
struct Timing {
    rounds: usize,
    samples_per_round: usize,
}

const FULL_TIMING: Timing = Timing {
    rounds: MEASURE_ROUNDS,
    samples_per_round: SAMPLES_PER_ROUND,
};

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    xs[xs.len() / 2]
}

/// Median decoded Mts/s (millions of timestamps per second) of `decode`
/// over `timing.rounds` timed rounds, after three warm-up calls.
fn decode_mtps<F: FnMut()>(timing: Timing, n: usize, mut decode: F) -> f64 {
    let reps = (timing.samples_per_round / n.max(1)).max(1);
    for _ in 0..3 {
        decode();
    }
    let mut rounds = Vec::with_capacity(timing.rounds);
    for _ in 0..timing.rounds {
        let start = Instant::now();
        for _ in 0..reps {
            decode();
        }
        let elapsed = start.elapsed().as_secs_f64().max(1e-12);
        rounds.push((n * reps) as f64 / elapsed / 1e6);
    }
    median(rounds)
}

/// One measured (page, codec) cell. Every codec is asserted to round-trip
/// bit-exactly before it is timed, so a reported cell is always exact.
struct Cell {
    codec: &'static str,
    bytes: usize,
    dec_mtps: f64,
    /// Production's selected enc tag, or the divisor a GCD codec divided by.
    note: String,
}

/// Encodes one page with every codec in [`CODECS`] order, checks each round
/// trip, and times each decode.
fn measure_page(label: &str, ts: &[i64], timing: Timing) -> Vec<Cell> {
    let n = ts.len();
    let mut cells = Vec::with_capacity(CODECS.len());

    let run = production_ts_run(ts);
    assert_eq!(
        decode_scalar_run(&run).0,
        ts,
        "production round trip failed for {label} n={n}"
    );
    cells.push(Cell {
        codec: PRODUCTION,
        bytes: run.ts_page.len(),
        dec_mtps: decode_mtps(timing, n, || {
            black_box(decode_scalar_run(black_box(&run)));
        }),
        note: format!("enc={}", run.ts_page.first().copied().unwrap_or(0)),
    });

    let (enc, bytes) = encode_i64(ts);
    assert_eq!(
        decode_i64(enc, &bytes, n).ok().as_deref(),
        Some(ts),
        "encode_i64 round trip failed for {label} n={n}"
    );
    cells.push(Cell {
        codec: "enc_i64",
        bytes: bytes.len(),
        dec_mtps: decode_mtps(timing, n, || {
            black_box(decode_i64(enc, black_box(&bytes), n)).ok();
        }),
        note: String::new(),
    });

    let g = page_gcd(ts);
    let gbytes = encode_i64_gcd(ts);
    assert_eq!(
        decode_i64_gcd(&gbytes, n).as_deref(),
        Some(ts),
        "encode_i64/gcd round trip failed for {label} n={n}"
    );
    cells.push(Cell {
        codec: "enc_i64/gcd",
        bytes: gbytes.len(),
        dec_mtps: decode_mtps(timing, n, || {
            black_box(decode_i64_gcd(black_box(&gbytes), n));
        }),
        note: format!("gcd={g}"),
    });

    let dod = dod_for_encode(ts).expect("dod_for encodes every bake-off stream");
    assert_eq!(
        dod_for_decode(&dod, n).as_deref(),
        Some(ts),
        "dod_for round trip failed for {label} n={n}"
    );
    cells.push(Cell {
        codec: DOD_FOR,
        bytes: dod.len(),
        dec_mtps: decode_mtps(timing, n, || {
            black_box(dod_for_decode(black_box(&dod), n));
        }),
        note: String::new(),
    });

    let gdod = gcd_forced_dod_encode(ts).expect("gcd_forced_dod encodes every bake-off stream");
    assert_eq!(
        gcd_forced_dod_decode(&gdod, n).as_deref(),
        Some(ts),
        "gcd_forced_dod round trip failed for {label} n={n}"
    );
    cells.push(Cell {
        codec: GCD_FORCED_DOD,
        bytes: gdod.len(),
        dec_mtps: decode_mtps(timing, n, || {
            black_box(gcd_forced_dod_decode(black_box(&gdod), n));
        }),
        note: format!("gcd={g}"),
    });

    cells
}

/// `num / den` to three places, or `inf` when `den` is zero.
fn ratio(num: usize, den: usize) -> String {
    if den == 0 {
        "inf".to_string()
    } else {
        format!("{:.3}", num as f64 / den as f64)
    }
}

fn header_row() -> String {
    format!(
        "  {:<24} {:>5} {:<15} {:>7} {:>10} {:>8} {:>9} {:>10}  {:<9} {}\n",
        "corpus",
        "n",
        "codec",
        "B/page",
        "B/sample",
        "ratio",
        "ratio_hdr",
        "dec Mts/s",
        "roundtrip",
        "note"
    )
}

/// Renders one page's cell rows and returns its adoption-summary line.
/// `ratio` is the challenger's bytes over production's as measured;
/// `ratio_hdr` is over production's bytes minus its page header.
fn page_rows(out: &mut String, corpus: &str, n: usize, cells: &[Cell]) -> String {
    let prod = cells
        .iter()
        .find(|c| c.codec == PRODUCTION)
        .expect("every page measures production");
    let prod_payload = prod.bytes.saturating_sub(PAGE_HEADER_BYTES);
    for c in cells {
        let (r, r_hdr) = if c.codec == PRODUCTION {
            ("-".to_string(), "-".to_string())
        } else {
            (ratio(c.bytes, prod.bytes), ratio(c.bytes, prod_payload))
        };
        out.push_str(&format!(
            "  {corpus:<24} {n:>5} {:<15} {:>7} {:>10.4} {r:>8} {r_hdr:>9} {:>10.1}  {:<9} {}\n",
            c.codec,
            c.bytes,
            c.bytes as f64 / n as f64,
            c.dec_mtps,
            "ok",
            c.note,
        ));
    }

    let mut winners = Vec::new();
    let mut verdicts = Vec::new();
    for c in cells.iter().filter(|c| CHALLENGERS.contains(&c.codec)) {
        let mut misses = Vec::new();
        if c.bytes as f64 > prod_payload as f64 * BYTE_RATIO_MAX {
            misses.push("bytes");
        }
        if c.dec_mtps < prod.dec_mtps {
            misses.push("dec");
        }
        if misses.is_empty() {
            winners.push(c.codec);
            verdicts.push(format!("{}=clears", c.codec));
        } else {
            verdicts.push(format!("{}=miss({})", c.codec, misses.join(",")));
        }
    }
    format!(
        "  {corpus:<24} {n:>5} adopt: {:<16} {}\n",
        if winners.is_empty() {
            "none".to_string()
        } else {
            winners.join(",")
        },
        verdicts.join("  "),
    )
}

/// One MetricsBench-derived timestamp stream: the first series
/// `metrics_stream_sample::sample_series` draws from `profile_name` over
/// `steps` scrapes. Millisecond timestamps are widened to nanoseconds so
/// they share a domain with the synthetic streams above.
///
/// The generator emits out-of-order and duplicate samples, but a run reaches
/// the writer sorted by timestamp with one sample per timestamp
/// (`encode_run_v4`'s contract), so the stream is sorted and deduplicated
/// here. Returns the seed, the stream, and how many samples arrived out of
/// order and how many duplicates were dropped.
fn metrics_stream(profile_name: &str, steps: u64) -> Option<MetricsStream> {
    let (seed, series) = sample_series(profile_name, steps, 1).ok()?;
    let first = series.into_iter().next()?;
    let mut ts: Vec<i64> = first.ts_ms.into_iter().map(|ms| ms * MS_NS).collect();
    let out_of_order = ts.windows(2).filter(|w| w[1] < w[0]).count();
    ts.sort_unstable();
    let before = ts.len();
    ts.dedup();
    Some(MetricsStream {
        seed,
        out_of_order,
        duplicates: before - ts.len(),
        ts,
    })
}

struct MetricsStream {
    seed: u64,
    ts: Vec<i64>,
    out_of_order: usize,
    duplicates: usize,
}

/// One MetricsBench corpus as drawn: the profile, its scrape steps, and the
/// stream, or `None` when the profile sampled no series.
struct MetricsCorpus {
    profile_name: &'static str,
    steps: u64,
    stream: Option<MetricsStream>,
}

/// Draws every `(profile, steps)` stream once, so the renderer and anything
/// that checks its output read the same samples.
fn draw_metrics(profiles: &[(&'static str, u64)]) -> Vec<MetricsCorpus> {
    profiles
        .iter()
        .map(|&(profile_name, steps)| MetricsCorpus {
            profile_name,
            steps,
            stream: metrics_stream(profile_name, steps),
        })
        .collect()
}

/// What one report run measures.
struct RunConfig<'a> {
    lens: &'a [usize],
    metrics: &'a [MetricsCorpus],
    timing: Timing,
}

/// Renders the whole report below the environment header: every corpus
/// section, then the adoption summary. `main` prints exactly this string.
fn render_report(cfg: &RunConfig) -> String {
    let mut out = String::new();
    out.push_str(
        "\nB/page, B/sample: production INCLUDES the 6-byte RSEG page header\n\
         (enc/comp/crc); every other codec is raw codec output with no header.\n\
         ratio = codec bytes / production bytes, as measured.\n\
         ratio_hdr = codec bytes / (production bytes - 6), header-corrected.\n\
         dec Mts/s = median decoded millions of timestamps/s over 9 timed rounds,\n\
         1 thread. Production decodes a whole run (TS plus an all-zero VAL page)\n\
         through the public API, so its figure understates TS-only decode speed.\n\
         note: production's selected TS enc (1 TS_DELTA_VARINT, 2 TS_GCD_I64);\n\
         gcd = divisor the GCD codecs divided the page by.\n\
         enc_i64/gcd = enc 2's payload: uvarint gcd, Enc tag byte, encode_i64(page / gcd).\n\
         dod_for = challenger (a): zigzag first value, zigzag first delta, then\n\
         delta-of-delta FOR-bit-packed per 128-sample block; no sample count.\n\
         gcd_forced_dod = challenger (b): uvarint gcd, then page / gcd forced onto\n\
         encode_i64's double-delta arm alone (never its best-of search).\n\
         Rule, per row: a challenger is adopted only if bit-exact, ratio_hdr <= 0.75,\n\
         and dec Mts/s >= production's.\n",
    );

    let mut summary = String::new();
    for kind in MULTI_STREAMS {
        out.push_str(&format!("\n===== {kind} =====\n"));
        out.push_str(&header_row());
        for &n in cfg.lens {
            let ts = stream(kind, n);
            let cells = measure_page(kind, &ts, cfg.timing);
            summary.push_str(&page_rows(&mut out, kind, n, &cells));
        }
    }

    for MetricsCorpus {
        profile_name,
        steps,
        stream,
    } in cfg.metrics
    {
        let corpus = format!("metricsbench_{profile_name}");
        match stream {
            Some(MetricsStream {
                seed,
                ts,
                out_of_order,
                duplicates,
            }) => {
                out.push_str(&format!(
                    "\n===== {corpus} (seed={seed}, steps={steps}, series=1, samples={}, \
                     sorted: {out_of_order} out of order, {duplicates} duplicates dropped) =====\n",
                    ts.len()
                ));
                out.push_str(&header_row());
                for &n in cfg.lens.iter().filter(|&&n| n <= ts.len()) {
                    let cells = measure_page(&corpus, &ts[..n], cfg.timing);
                    summary.push_str(&page_rows(&mut out, &corpus, n, &cells));
                }
            }
            None => out.push_str(&format!(
                "\n===== {corpus}: no series sampled, skipped =====\n"
            )),
        }
    }

    out.push_str("\n===== adoption summary (judged on ratio_hdr) =====\n");
    out.push_str(&format!(
        "rule: bit-exact AND ratio_hdr <= {BYTE_RATIO_MAX} AND dec Mts/s >= production\n"
    ));
    out.push_str(&summary);
    out
}

fn main() {
    let mut out = env_header(
        "Timestamp codec bake-off (issue #312, ADR-0092 decision 6; ADR-3694 decision 7)",
    );
    out.push_str(&render_report(&RunConfig {
        lens: &PAGE_LENS,
        metrics: &draw_metrics(&METRICS_PROFILES),
        timing: FULL_TIMING,
    }));
    print!("{out}");
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// True when `line` is a cell row or an adoption-summary row: a corpus,
    /// a page length, then a codec name or `adopt:`.
    fn is_cell_line(line: &str) -> bool {
        let t: Vec<&str> = line.split_whitespace().collect();
        t.len() >= 3
            && t[1].parse::<usize>().is_ok()
            && (CODECS.contains(&t[2]) || t[2] == "adopt:")
    }

    fn cell_line_count(out: &str, corpus: &str, n: usize, third: &str) -> usize {
        let n = n.to_string();
        out.lines()
            .filter(|l| {
                let t: Vec<&str> = l.split_whitespace().take(3).collect();
                t == [corpus, n.as_str(), third]
            })
            .count()
    }

    /// Every (corpus, codec, page length) cell's row, and every (corpus, page
    /// length) adoption line, appears exactly once in the rendered text, and
    /// no other cell line does.
    fn check_cells(out: &str, pages: &[(String, usize)]) -> Result<(), String> {
        for (corpus, n) in pages {
            for third in CODECS.iter().copied().chain(["adopt:"]) {
                let count = cell_line_count(out, corpus, *n, third);
                if count != 1 {
                    return Err(format!(
                        "cell ({corpus}, {third}, n={n}) appears {count} times in the \
                         rendered report, expected exactly 1"
                    ));
                }
            }
        }
        let total = out.lines().filter(|l| is_cell_line(l)).count();
        let expected = pages.len() * (CODECS.len() + 1);
        if total != expected {
            return Err(format!(
                "rendered report holds {total} cell lines, expected {expected}"
            ));
        }
        Ok(())
    }

    const TEST_LENS: [usize; 2] = [1, 2];
    const TEST_METRICS: [(&str, u64); 2] = [("cardinality", 2), ("history", 4)];

    /// The full report `main` prints, on reduced page lengths, both
    /// MetricsBench profiles at few steps, and one timing round, plus the
    /// cells it must hold, derived from the drawn inputs rather than from
    /// the renderer. Each stream is drawn once and rendered once, and the
    /// result is shared, since the cardinality profile generates every
    /// active series per step.
    fn reduced_report() -> (String, Vec<(String, usize)>) {
        static REPORT: std::sync::OnceLock<(String, Vec<(String, usize)>)> =
            std::sync::OnceLock::new();
        REPORT
            .get_or_init(|| {
                let metrics = draw_metrics(&TEST_METRICS);
                let mut pages: Vec<(String, usize)> = MULTI_STREAMS
                    .iter()
                    .flat_map(|k| TEST_LENS.iter().map(move |&n| (k.to_string(), n)))
                    .collect();
                for m in &metrics {
                    let len = m.stream.as_ref().expect("MetricsBench stream").ts.len();
                    pages.extend(
                        TEST_LENS
                            .iter()
                            .filter(|&&n| n <= len)
                            .map(|&n| (format!("metricsbench_{}", m.profile_name), n)),
                    );
                }
                let out = render_report(&RunConfig {
                    lens: &TEST_LENS,
                    metrics: &metrics,
                    timing: Timing {
                        rounds: 1,
                        samples_per_round: 1,
                    },
                });
                (out, pages)
            })
            .clone()
    }

    #[test]
    fn report_has_every_cell_exactly_once() {
        let (out, pages) = reduced_report();
        for (profile_name, _) in TEST_METRICS {
            let corpus = format!("metricsbench_{profile_name}");
            assert!(
                TEST_LENS
                    .iter()
                    .all(|&n| pages.iter().any(|(c, m)| *c == corpus && *m == n)),
                "the guard must cover every {corpus} page"
            );
        }
        if let Err(e) = check_cells(&out, &pages) {
            panic!("{e}\n{out}");
        }
    }

    #[test]
    fn guard_rejects_a_dropped_cardinality_cell() {
        let (out, pages) = reduced_report();
        let line = out
            .lines()
            .find(|l| {
                l.split_whitespace()
                    .take(3)
                    .eq(["metricsbench_cardinality", "2", "enc_i64/gcd"])
            })
            .expect("cell row present");
        let dropped = out.replacen(&format!("{line}\n"), "", 1);
        let err = check_cells(&dropped, &pages).expect_err("a dropped cell must fail");
        assert!(
            err.contains("(metricsbench_cardinality, enc_i64/gcd, n=2) appears 0 times"),
            "{err}"
        );
    }

    #[test]
    fn guard_rejects_a_duplicated_cell() {
        let (out, pages) = reduced_report();
        let line = out
            .lines()
            .find(|l| {
                l.split_whitespace()
                    .take(3)
                    .eq(["metricsbench_history", "2", DOD_FOR])
            })
            .expect("cell row present");
        let dup = out.replacen(line, &format!("{line}\n{line}"), 1);
        let err = check_cells(&dup, &pages).expect_err("a duplicated cell must fail");
        assert!(err.contains("appears 2 times"), "{err}");
    }

    #[test]
    fn guard_rejects_a_dropped_cell() {
        let (out, pages) = reduced_report();
        let line = out
            .lines()
            .find(|l| {
                l.split_whitespace()
                    .take(3)
                    .eq(["regular_ms", "1", GCD_FORCED_DOD])
            })
            .expect("cell row present");
        let dropped = out.replacen(&format!("{line}\n"), "", 1);
        let err = check_cells(&dropped, &pages).expect_err("a dropped cell must fail");
        assert!(err.contains("appears 0 times"), "{err}");
    }

    /// True when every delta and every delta-of-delta of `values` fits an
    /// i64, the exact condition under which both challengers encode.
    fn dod_fits(values: &[i64]) -> bool {
        let deltas: Vec<i128> = values
            .windows(2)
            .map(|w| i128::from(w[1]) - i128::from(w[0]))
            .collect();
        let fits = |v: i128| i64::try_from(v).is_ok();
        deltas.iter().all(|&d| fits(d)) && deltas.windows(2).all(|w| fits(w[1] - w[0]))
    }

    /// Arbitrary i64 sequences, plus timestamp-shaped ones (a start and
    /// bounded steps) so the encode path is reached, not only the overflow
    /// refusal. Lengths cross several `DOD_BLOCK_LEN` block boundaries.
    fn sequences() -> impl Strategy<Value = Vec<i64>> {
        prop_oneof![
            proptest::collection::vec(any::<i64>(), 0..300),
            (
                any::<i64>(),
                proptest::collection::vec(-1_000_000_000_000i64..1_000_000_000_000, 0..300),
            )
                .prop_map(|(start, steps)| {
                    let mut acc = start;
                    let mut out = vec![start];
                    for s in steps {
                        acc = acc.saturating_add(s);
                        out.push(acc);
                    }
                    out
                }),
        ]
    }

    proptest! {
        #[test]
        fn dod_for_round_trips_any_i64_sequence(values in sequences()) {
            let encoded = dod_for_encode(&values);
            prop_assert_eq!(encoded.is_some(), dod_fits(&values));
            if let Some(bytes) = encoded {
                prop_assert_eq!(dod_for_decode(&bytes, values.len()), Some(values));
            }
        }

        #[test]
        fn gcd_forced_dod_round_trips_any_i64_sequence(values in sequences()) {
            let g = page_gcd(&values);
            let divided: Vec<i64> = values.iter().map(|&v| v / g).collect();
            let encoded = gcd_forced_dod_encode(&values);
            prop_assert_eq!(encoded.is_some(), dod_fits(&divided));
            if let Some(bytes) = encoded {
                let mut pos = 0usize;
                prop_assert_eq!(get_divisor(&bytes, &mut pos), Some(g));
                prop_assert_eq!(gcd_forced_dod_decode(&bytes, values.len()), Some(values));
            }
        }

        #[test]
        fn encode_i64_gcd_round_trips_any_i64_sequence(values in sequences()) {
            let bytes = encode_i64_gcd(&values);
            let mut pos = 0usize;
            prop_assert_eq!(get_divisor(&bytes, &mut pos), Some(page_gcd(&values)));
            prop_assert_eq!(decode_i64_gcd(&bytes, values.len()), Some(values));
        }
    }

    /// The GCD challengers carry their divisor (and, for `enc_i64/gcd`, its
    /// tag byte) in the payload, exactly as production enc 2 does: on a page
    /// where production picks enc 2, `enc_i64/gcd` is production's payload
    /// byte for byte.
    #[test]
    fn gcd_codecs_charge_their_divisor() {
        let ts = stream("irregular", 12);
        let run = production_ts_run(&ts);
        assert_eq!(run.ts_page.first(), Some(&2), "production picks enc 2 here");
        let gbytes = encode_i64_gcd(&ts);
        assert_eq!(gbytes.len(), run.ts_page.len() - PAGE_HEADER_BYTES);
        let g = page_gcd(&ts);
        let mut divisor = Vec::new();
        put_uvarint(&mut divisor, g.unsigned_abs());
        let gdod = gcd_forced_dod_encode(&ts).expect("encodes");
        assert!(gdod.starts_with(&divisor));
        let divided: Vec<i64> = ts.iter().map(|&v| v / g).collect();
        assert_eq!(
            gdod.len(),
            divisor.len() + double_delta_encode(&divided).expect("encodes").len()
        );
    }
}
