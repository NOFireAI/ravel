//! Value codec bake-off, round two (issue #312, ADR-0092 decision 6).
//!
//! Round one (2026-07-28) tested only XOR-family challengers and drove its
//! counter workload with a random *float* increment, so it never exercised the
//! integer-valued counters and gauges that dominate real metrics. This round
//! adds two integer-transform challengers, [`Alp`] and [`GcdDeltaFor`], and
//! measures every challenger against the production Gorilla path reached
//! through the segment public API (`encode_run_v4` / `decode_run_pages_soa`,
//! via `segment_support`) and against raw f64.
//!
//! Report-only: nothing here changes a production path. It produces the
//! numbers issue #314 consumes to decide which value encoding, if any, enters
//! the next RSEG version.
//!
//! ## What is measured, and the one asymmetry to know
//!
//! For every (dataset, page size) each codec reports bytes per sample and the
//! median encode/decode throughput over repeated timed runs. Bytes per sample
//! is the codec's *payload*: for the challengers that is `encode()`'s length;
//! for production it is the VAL page minus its 6-byte enc/comp/crc header, so
//! all rows compare header-less payloads (the 6-byte page header is common
//! overhead paid on top of whichever codec wins).
//!
//! The throughput asymmetry: RSEG exposes no public value-only codec, so the
//! production encode/decode timings go through `encode_run_v4` /
//! `decode_run_pages_soa`, which frame and decode a whole run -- a TS page as
//! well as the VAL page. That charges production a fixed TS-framing cost the
//! payload-only challengers never pay, so these numbers *understate*
//! production's value-only decode speed. A challenger that beats production
//! decode here beats it by at least this much; a challenger that does not is
//! ambiguous on throughput. The bytes-per-sample figures carry no such caveat.
//!
//! ## Adoption rule (ADR-0092 decision 6, applied in the summary)
//!
//! A challenger is adopted only if, bit-exact round trip confirmed, it uses at
//! most 75% of production's bytes per sample *and* decodes at least as fast as
//! production. A codec that cannot round-trip bit-exactly is disqualified
//! outright.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::hint::black_box;
use std::time::Instant;

use ravel_bench::bench_env::env_header;
use ravel_bench::codecs::{Alp, ByteSplitZstd, Chimp128, GcdDeltaFor, RawF64, ValueCodec};
use ravel_bench::segment_support::{decode_scalar_run, production_scalar_run, scalar_val_page};
use ravel_bench::value_shapes::{ALL_SHAPES, ValueShape, value_stream};

/// Page sizes measured: the small-run regime and a full L1-merged run.
const PAGE_SIZES: [usize; 2] = [60, 500];
/// Timed rounds per throughput cell; the median is reported.
const MEASURE_ROUNDS: usize = 9;
/// Inner repetitions per timed round, so each measurement spans many
/// microseconds and swamps timer granularity.
const INNER_REPS: usize = 4000;
/// Adoption threshold: a challenger must use at most this fraction of
/// production's bytes per sample.
const BYTE_RATIO_MAX: f64 = 0.75;

/// One synthetic value stream and its human name.
struct Dataset {
    name: &'static str,
    values: Vec<f64>,
}

/// Builds one dataset of exactly `n` samples from the shared shape
/// definitions ([`value_stream`]). Deterministic in `(shape, n)`: the bake-off
/// uses `salt = 0`, one stream per dataset.
fn dataset(shape: ValueShape, n: usize) -> Dataset {
    Dataset {
        name: shape.label(),
        values: value_stream(shape, n, 0),
    }
}

fn bits_eq(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    xs[xs.len() / 2]
}

/// Median Mval/s (millions of values per second) over [`MEASURE_ROUNDS`] timed
/// rounds of [`INNER_REPS`] repetitions each.
fn throughput_mvps<F: FnMut()>(mut f: F, n_values: usize) -> f64 {
    for _ in 0..3 {
        f();
    }
    let mut rounds = Vec::with_capacity(MEASURE_ROUNDS);
    for _ in 0..MEASURE_ROUNDS {
        let start = Instant::now();
        for _ in 0..INNER_REPS {
            f();
        }
        let elapsed = start.elapsed().as_secs_f64().max(1e-12);
        rounds.push((n_values as f64 * INNER_REPS as f64) / elapsed / 1e6);
    }
    median(rounds)
}

/// One measured cell.
struct Cell {
    codec: &'static str,
    bytes_per_sample: f64,
    enc_mvps: f64,
    dec_mvps: f64,
    roundtrip_ok: bool,
    /// Production enc tag (16 Gorilla, 17 raw) for the production row only.
    prod_enc: Option<u8>,
}

/// One challenger's summary numbers, retained for the adoption pass.
struct ChallengerSummary {
    codec: &'static str,
    bytes_per_sample: f64,
    dec_mvps: f64,
    roundtrip_ok: bool,
}

/// Per (page, dataset) summary: production baselines plus every challenger.
struct DatasetSummary {
    page: usize,
    name: &'static str,
    prod_bytes: f64,
    prod_dec: f64,
    challengers: Vec<ChallengerSummary>,
}

fn measure_challenger(codec: &dyn ValueCodec, values: &[f64]) -> Cell {
    let n = values.len();
    let encoded = codec.encode(values);
    let roundtrip_ok = codec
        .decode(&encoded, n)
        .map(|d| bits_eq(&d, values))
        .unwrap_or(false);
    let enc_mvps = throughput_mvps(
        || {
            black_box(codec.encode(black_box(values)));
        },
        n,
    );
    let dec_mvps = throughput_mvps(
        || {
            black_box(codec.decode(black_box(&encoded), n)).ok();
        },
        n,
    );
    Cell {
        codec: codec.name(),
        bytes_per_sample: encoded.len() as f64 / n as f64,
        enc_mvps,
        dec_mvps,
        roundtrip_ok,
        prod_enc: None,
    }
}

fn measure_production(values: &[f64]) -> Cell {
    let n = values.len();
    let run = production_scalar_run(values);
    let page = scalar_val_page(&run);
    // Payload only: drop the 6-byte enc/comp/crc header so the row compares to
    // the header-less challenger payloads.
    let payload = page.len().saturating_sub(6);
    let prod_enc = page.first().copied();
    let (_ts, decoded) = decode_scalar_run(&run);
    let roundtrip_ok = bits_eq(&decoded, values);
    let enc_mvps = throughput_mvps(
        || {
            black_box(production_scalar_run(black_box(values)));
        },
        n,
    );
    let dec_mvps = throughput_mvps(
        || {
            black_box(decode_scalar_run(black_box(&run)));
        },
        n,
    );
    Cell {
        codec: "production",
        bytes_per_sample: payload as f64 / n as f64,
        enc_mvps,
        dec_mvps,
        roundtrip_ok,
        prod_enc,
    }
}

fn main() {
    let mut out = String::new();
    out.push_str(&env_header(
        "Value codec bake-off (issue #312, ADR-0092 decision 6)",
    ));
    out.push_str(
        "\nbytes/sample = codec payload (production excludes its 6-byte page header).\n\
         throughput = median Mval/s over repeated runs; production timings frame a whole\n\
         run (TS + VAL) through the public API, so they understate value-only speed.\n\
         'ratio' = challenger bytes / production bytes; adoption needs <= 0.75 AND\n\
         dec >= production dec AND a bit-exact round trip.\n",
    );

    // Retained for the adoption pass: per (page, dataset) the production
    // baselines plus each challenger's summary numbers.
    let mut summary: Vec<DatasetSummary> = Vec::new();

    for &page in &PAGE_SIZES {
        out.push_str(&format!(
            "\n======================= {page} samples per page =======================\n"
        ));
        for shape in ALL_SHAPES {
            let ds = dataset(shape, page);
            let prod = measure_production(&ds.values);
            let challengers: Vec<Cell> = [
                &Alp as &dyn ValueCodec,
                &GcdDeltaFor,
                &Chimp128,
                &ByteSplitZstd,
                &RawF64,
            ]
            .iter()
            .map(|c| measure_challenger(*c, &ds.values))
            .collect();

            let enc_tag = prod.prod_enc.map(enc_name).unwrap_or("?");
            out.push_str(&format!(
                "\n{}  (n={page}, production val enc={enc_tag})\n",
                ds.name
            ));
            out.push_str(&format!(
                "  {:<16} {:>10} {:>12} {:>12} {:>8}  {}\n",
                "codec", "B/sample", "enc Mval/s", "dec Mval/s", "ratio", "roundtrip"
            ));
            let prod_bytes = prod.bytes_per_sample;
            out.push_str(&format!(
                "  {:<16} {:>10.4} {:>12.1} {:>12.1} {:>8}  {}\n",
                prod.codec, prod.bytes_per_sample, prod.enc_mvps, prod.dec_mvps, "-", "ok"
            ));
            let mut summary_cells = Vec::new();
            for c in &challengers {
                let ratio = c.bytes_per_sample / prod_bytes.max(f64::MIN_POSITIVE);
                out.push_str(&format!(
                    "  {:<16} {:>10.4} {:>12.1} {:>12.1} {:>8.3}  {}\n",
                    c.codec,
                    c.bytes_per_sample,
                    c.enc_mvps,
                    c.dec_mvps,
                    ratio,
                    if c.roundtrip_ok { "ok" } else { "FAIL" },
                ));
                summary_cells.push(ChallengerSummary {
                    codec: c.codec,
                    bytes_per_sample: c.bytes_per_sample,
                    dec_mvps: c.dec_mvps,
                    roundtrip_ok: c.roundtrip_ok,
                });
            }
            summary.push(DatasetSummary {
                page,
                name: ds.name,
                prod_bytes,
                prod_dec: prod.dec_mvps,
                challengers: summary_cells,
            });
        }
    }

    // Adoption summary per (page, dataset).
    out.push_str("\n======================= adoption summary =======================\n");
    out.push_str(&format!(
        "rule: bytes <= {:.0}% of production AND dec >= production dec AND bit-exact\n",
        BYTE_RATIO_MAX * 100.0
    ));
    for row in &summary {
        let winners: Vec<String> = row
            .challengers
            .iter()
            .filter_map(|c| {
                let clears = c.roundtrip_ok
                    && c.bytes_per_sample <= row.prod_bytes * BYTE_RATIO_MAX
                    && c.dec_mvps >= row.prod_dec;
                clears.then(|| c.codec.to_string())
            })
            .collect();
        let disq: Vec<&str> = row
            .challengers
            .iter()
            .filter(|c| !c.roundtrip_ok)
            .map(|c| c.codec)
            .collect();
        let (page, name) = (row.page, row.name);
        out.push_str(&format!(
            "  n={page:<4} {name:<20} adopt: {}{}\n",
            if winners.is_empty() {
                "none".to_string()
            } else {
                winners.join(", ")
            },
            if disq.is_empty() {
                String::new()
            } else {
                format!("   disqualified(no round-trip): {}", disq.join(", "))
            },
        ));
    }

    print!("{out}");
}

fn enc_name(tag: u8) -> &'static str {
    match tag {
        16 => "16(gorilla)",
        17 => "17(raw_f64)",
        _ => "?",
    }
}
