//! Timestamp codec bake-off (issue #312, ADR-0092 decision 6).
//!
//! Compares three timestamp encoders on the same synthetic streams:
//!
//! 1. **production**: the RSEG TS page path -- `TS_DELTA_VARINT` plus the
//!    writer's 64-byte-floor LZ4 rule -- reached by framing a real TS page
//!    through `encode_run_v4` (`segment_support::production_ts_run`). Its byte
//!    count therefore *includes the 6-byte page header* (enc, comp, crc);
//!    the two comparators below are raw codec output with no such header, so a
//!    production win of 6 bytes or less on a page is header, not codec.
//! 2. **encode_i64**: `ravel_codec::encoding::encode_i64` applied to the raw
//!    timestamps (the integer path RSEG v7 uses for its provenance columns).
//! 3. **encode_i64/gcd**: the same, after dividing every timestamp by the
//!    page's greatest common divisor. The divisor found is reported; it is
//!    where a millisecond-resolution stream (every value a multiple of 1e6 ns)
//!    can shed a constant factor before delta coding.
//!
//! Report-only, per ADR-0092 decision 6: the numbers feed issue #314, they
//! adopt nothing. The decision rule to apply is that a timestamp stack must
//! not regress the exactly-regular case, where production already reaches
//! about 0.16 bytes per sample at large pages because LZ4 collapses the
//! repeated varint.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use ravel_bench::bench_env::env_header;
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

fn main() {
    let mut out = String::new();
    out.push_str(&env_header(
        "Timestamp codec bake-off (issue #312, ADR-0092 decision 6)",
    ));
    out.push_str(
        "\nbytes/sample: production INCLUDES the 6-byte RSEG page header (enc/comp/crc);\n\
         encode_i64 and encode_i64/gcd are raw codec output with no page header. A\n\
         production lead of <= 6 bytes on a page is header overhead, not codec.\n\
         gcd = divisor found for that page (encode_i64/gcd divides by it first).\n\
         Rule: a timestamp stack must not regress the exactly-regular stream.\n",
    );

    for stream_kind in MULTI_STREAMS {
        out.push_str(&format!("\n===== {stream_kind} =====\n"));
        out.push_str(&format!(
            "  {:>6} {:>14} {:>14} {:>16} {:>18}\n",
            "n", "prod B/s", "enc_i64 B/s", "enc_i64/gcd B/s", "gcd"
        ));
        for &n in &PAGE_LENS {
            let ts = stream(stream_kind, n);
            let prod = production_bytes(&ts);
            let (raw_bytes, raw_ok) = encode_i64_bytes(&ts);
            let (gcd_bytes, gcd, gcd_ok) = encode_i64_gcd_bytes(&ts);
            assert!(
                raw_ok,
                "encode_i64 round trip failed for {stream_kind} n={n}"
            );
            assert!(
                gcd_ok,
                "encode_i64/gcd round trip failed for {stream_kind} n={n}"
            );
            out.push_str(&format!(
                "  {:>6} {:>14.4} {:>14.4} {:>16.4} {:>18}\n",
                n,
                prod as f64 / n as f64,
                raw_bytes as f64 / n as f64,
                gcd_bytes as f64 / n as f64,
                gcd,
            ));
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
