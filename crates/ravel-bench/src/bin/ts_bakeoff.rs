//! Timestamp codec bake-off (issue #312, ADR-0092 decision 6; ADR-3694
//! decision 7 adds the two challengers below, timestamps first).
//!
//! Compares five timestamp encoders on the same streams:
//!
//! 1. **production (enc 1)**: the RSEG TS page path -- `TS_DELTA_VARINT` plus
//!    the writer's 64-byte-floor LZ4 rule -- reached by framing a real TS page
//!    through `encode_run_v4` (`segment_support::production_ts_run`). Its byte
//!    count therefore *includes the 6-byte page header* (enc, comp, crc);
//!    every other column is raw codec output with no such header, so a
//!    production lead of 6 bytes or less on a page is header, not codec.
//! 2. **encode_i64**: `ravel_codec::encoding::encode_i64` applied to the raw
//!    timestamps (the integer path RSEG v7 uses for its provenance columns).
//! 3. **encode_i64/gcd (enc 2 surrogate)**: the same, after dividing every
//!    timestamp by the page's greatest common divisor -- the `encode_i64`
//!    core of production enc 2 `TS_GCD_I64` (docs/segment-format.md), minus
//!    its `uvarint gcd` + tag byte, which the baseline above also omits. The
//!    divisor found is reported.
//! 4. **dod_for (challenger a)**: delta-of-delta with a jitter-tolerant
//!    residual -- first delta zigzag-varint, each later delta coded as its
//!    difference from the previous delta, those residuals frame-of-reference
//!    bit-packed per fixed-size block (so one jittery block costs its own
//!    bit width, not the whole page's).
//! 5. **gcd_forced_dod (challenger b)**: the page divided by its GCD, then
//!    forced onto the exact `enc_double_delta_i64` arm of `encode_i64`
//!    (absolute first value, first delta, then each later delta's
//!    difference from the previous delta, every field a zigzag varint,
//!    `checked_sub` overflow-checked) -- measured on its own, never as part
//!    of `encode_i64`'s best-of search.
//!
//! Report-only, per ADR-0092 decision 6 and ADR-3694 decision 7: the numbers
//! feed a future format ADR, they adopt nothing. The decision rule to apply
//! is that a timestamp stack must not regress the exactly-regular case, where
//! production already reaches about 0.16 bytes per sample at large pages
//! because LZ4 collapses the repeated varint, and a challenger is adopted
//! only if it round-trips bit-exactly, costs <= 75% of the production bytes,
//! and decodes at least as fast.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use ravel_bench::bench_env::env_header;
use ravel_bench::metrics_stream_sample::sample_series;
use ravel_bench::segment_support::production_ts_run;
use ravel_codec::encoding::{decode_i64, encode_i64};

/// Page lengths measured, from the single-sample fragmentation floor to a
/// full hour of 1 s data.
const PAGE_LENS: [usize; 6] = [1, 2, 12, 60, 240, 3600];

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
        // Regular millisecond cadence with ~15% of samples pushed two
        // intervals back (out of order), as a bursty backfill produces.
        "out_of_order" => (0..n)
            .map(|i| {
                let base = START_NS + i as i64 * STEP_15S_NS;
                if rng.random_bool(0.15) {
                    base - 2 * STEP_15S_NS
                } else {
                    base
                }
            })
            .collect(),
        other => panic!("unknown stream {other}"),
    }
}

fn seed_of(kind: &str) -> u64 {
    kind.bytes().fold(1469598103934665603u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(1099511628211)
    })
}

/// Bytes the production TS page path spends (6-byte header included).
fn production_bytes(ts: &[i64]) -> usize {
    production_ts_run(ts).ts_page.len()
}

/// `encode_i64` bytes and a round-trip check.
fn encode_i64_bytes(ts: &[i64]) -> (usize, bool) {
    let (enc, bytes) = encode_i64(ts);
    let ok = decode_i64(enc, &bytes, ts.len())
        .map(|d| d == ts)
        .unwrap_or(false);
    (bytes.len(), ok)
}

/// `encode_i64` over timestamps divided by the page GCD; returns (bytes,
/// divisor, round-trip ok). Round trip multiplies back by the divisor.
fn encode_i64_gcd_bytes(ts: &[i64]) -> (usize, i64, bool) {
    let g = page_gcd(ts);
    let divided: Vec<i64> = ts.iter().map(|&v| v / g).collect();
    let (enc, bytes) = encode_i64(&divided);
    let ok = decode_i64(enc, &bytes, ts.len())
        .map(|d| d.iter().map(|&v| v * g).eq(ts.iter().copied()))
        .unwrap_or(false);
    (bytes.len(), g, ok)
}

const MULTI_STREAMS: [&str; 5] = [
    "regular_ms",
    "jitter_ms",
    "nanos_jitter",
    "irregular",
    "out_of_order",
];

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
fn for_pack_block(out: &mut Vec<u8>, block: &[i64]) {
    let min = *block.iter().min().expect("block is non-empty");
    put_ivarint(out, min);
    let offsets: Vec<u64> = block.iter().map(|&v| (v - min) as u64).collect();
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
        out.push(min.checked_add(off as i64)?);
    }
    Some(out)
}

/// Blocks of jitter-tolerant delta-of-delta residuals: a bakeoff-wide fixed
/// size, independent of page length, so a jittery run of samples only costs
/// its own block's bit width rather than widening the whole page.
const DOD_BLOCK_LEN: usize = 128;

/// Challenger (a): first value absolute, first delta zigzag-varint, each
/// later delta's difference from the previous delta frame-of-reference
/// bit-packed per `DOD_BLOCK_LEN`-sized block. `None` on i64 overflow.
fn dod_for_encode(values: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    put_uvarint(&mut out, values.len() as u64);
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

fn dod_for_decode(bytes: &[u8]) -> Option<Vec<i64>> {
    let mut pos = 0usize;
    let n = usize::try_from(get_uvarint(bytes, &mut pos)?).ok()?;
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

/// `dod_for` bytes and a round-trip check for one page.
fn dod_for_bytes(ts: &[i64]) -> (usize, bool) {
    match dod_for_encode(ts) {
        Some(bytes) => {
            let ok = dod_for_decode(&bytes).map(|d| d == ts).unwrap_or(false);
            (bytes.len(), ok)
        }
        None => (0, false),
    }
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

/// The page divided by its GCD, forced onto `double_delta_encode` alone
/// (never `encode_i64`'s best-of). Returns (bytes, divisor, round-trip ok).
fn gcd_forced_double_delta_bytes(ts: &[i64]) -> (usize, i64, bool) {
    let g = page_gcd(ts);
    let divided: Vec<i64> = ts.iter().map(|&v| v / g).collect();
    match double_delta_encode(&divided) {
        Some(bytes) => {
            let ok = double_delta_decode(&bytes, ts.len())
                .map(|d| d.iter().map(|&v| v * g).eq(ts.iter().copied()))
                .unwrap_or(false);
            (bytes.len(), g, ok)
        }
        None => (0, g, false),
    }
}

/// One MetricsBench-derived timestamp stream: `profile_name` as drawn via
/// `metrics_stream_sample::sample_series`, bounded to `steps` scrapes of the
/// first series encountered. Millisecond timestamps are widened to
/// nanoseconds so they share a domain with the synthetic streams above.
fn metrics_stream(profile_name: &str, steps: u64) -> Option<(u64, Vec<i64>)> {
    let (seed, mut series) = sample_series(profile_name, steps, 1).ok()?;
    let s = if series.is_empty() {
        return None;
    } else {
        series.remove(0)
    };
    Some((seed, s.ts_ms.into_iter().map(|ms| ms * MS_NS).collect()))
}

fn print_row(out: &mut String, label: &str, ts: &[i64]) {
    let n = ts.len();
    let prod = production_bytes(ts);
    let (raw_bytes, raw_ok) = encode_i64_bytes(ts);
    let (gcd_bytes, gcd, gcd_ok) = encode_i64_gcd_bytes(ts);
    let (dod_bytes, dod_ok) = dod_for_bytes(ts);
    let (gdod_bytes, gdod_gcd, gdod_ok) = gcd_forced_double_delta_bytes(ts);
    assert!(raw_ok, "encode_i64 round trip failed for {label} n={n}");
    assert!(gcd_ok, "encode_i64/gcd round trip failed for {label} n={n}");
    assert!(dod_ok, "dod_for round trip failed for {label} n={n}");
    assert!(
        gdod_ok,
        "gcd_forced_dod round trip failed for {label} n={n}"
    );
    assert_eq!(
        gcd, gdod_gcd,
        "encode_i64/gcd and gcd_forced_dod disagree on the page GCD for {label} n={n}"
    );
    out.push_str(&format!(
        "  {:>6} {:>14.4} {:>14.4} {:>16.4} {:>12.4} {:>16.4} {:>8}\n",
        n,
        prod as f64 / n as f64,
        raw_bytes as f64 / n as f64,
        gcd_bytes as f64 / n as f64,
        dod_bytes as f64 / n as f64,
        gdod_bytes as f64 / n as f64,
        gcd,
    ));
}

fn main() {
    let mut out = String::new();
    out.push_str(&env_header(
        "Timestamp codec bake-off (issue #312, ADR-0092 decision 6; ADR-3694 decision 7)",
    ));
    out.push_str(
        "\nbytes/sample: production INCLUDES the 6-byte RSEG page header (enc/comp/crc);\n\
         every other column is raw codec output with no page header. A production\n\
         lead of <= 6 bytes on a page is header overhead, not codec.\n\
         gcd = divisor found for that page (encode_i64/gcd and gcd_forced_dod both\n\
         divide by it first; they are asserted to find the same divisor).\n\
         dod_for = challenger (a): delta-of-delta, FOR-bit-packed per 128-sample block.\n\
         gcd_forced_dod = challenger (b): page / gcd, forced onto encode_i64's\n\
         double-delta arm alone (never its best-of search).\n\
         Rule: a timestamp stack must not regress the exactly-regular stream; a\n\
         challenger is adopted only if bit-exact, <= 75% of production bytes, and\n\
         decode at least as fast.\n",
    );

    let header_row = format!(
        "  {:>6} {:>14} {:>14} {:>16} {:>12} {:>16} {:>8}\n",
        "n", "prod B/s", "enc_i64 B/s", "enc_i64/gcd B/s", "dod_for B/s", "gcd_dod B/s", "gcd"
    );

    for stream_kind in MULTI_STREAMS {
        out.push_str(&format!("\n===== {stream_kind} =====\n"));
        out.push_str(&header_row);
        for &n in &PAGE_LENS {
            let ts = stream(stream_kind, n);
            print_row(&mut out, stream_kind, &ts);
        }
    }

    // MetricsBench-generator-derived timestamps (ADR-3694 decision 7): the
    // real cardinality/history profiles from the committed workload manifest,
    // drawn through the public `generate_bytes` text API (the per-step
    // sample buffer is private), bounded to a small, fixed, stated number of
    // scrapes so a 1,000,000-series profile stays tractable on a shared box.
    // Measured on this box: the cardinality profile (1,000,000 active
    // series) costs about 13.5s/step regardless of requested series count
    // (every step still generates every active series before filtering), so
    // 8 steps (~108s) is the bound used here; history (10,000 active series)
    // costs about 0.13s/step, so 240 steps (~31s) reaches PAGE_LENS up to 240.
    for (profile_name, steps) in [("cardinality", 8u64), ("history", 240u64)] {
        match metrics_stream(profile_name, steps) {
            Some((seed, ts)) => {
                out.push_str(&format!(
                    "\n===== metricsbench_{profile_name} (seed={seed}, steps={steps}, series=1) =====\n"
                ));
                out.push_str(&header_row);
                let lens: Vec<usize> = PAGE_LENS
                    .iter()
                    .copied()
                    .filter(|&n| n <= ts.len())
                    .collect();
                for n in lens {
                    print_row(&mut out, profile_name, &ts[..n]);
                }
            }
            None => {
                out.push_str(&format!(
                    "\n===== metricsbench_{profile_name}: no series sampled, skipped =====\n"
                ));
            }
        }
    }

    // Single-sample runs: the fragmentation floor ADR-0092 cites (one absolute
    // zigzag varint, ~9 bytes, plus production's 6-byte header, never reaching
    // the LZ4 floor).
    out.push_str("\n===== single_sample (one absolute timestamp) =====\n");
    let ts = vec![START_NS];
    let prod = production_bytes(&ts);
    let (raw_bytes, _) = encode_i64_bytes(&ts);
    let (gcd_bytes, gcd, _) = encode_i64_gcd_bytes(&ts);
    out.push_str(&format!(
        "  prod={prod} B  enc_i64={raw_bytes} B  enc_i64/gcd={gcd_bytes} B  gcd={gcd}\n"
    ));

    print!("{out}");
}
