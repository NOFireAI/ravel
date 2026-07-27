//! Issue #99 float value codec bake-off. Measurement only: nothing here
//! changes a production code path. Compares the production Gorilla codec
//! (RSEG enc 16, reached through the segment public API) against two
//! challengers and a raw baseline (`ravel_bench::codecs`):
//!
//! - `raw_f64`: little-endian f64 bytes (the 8 B/sample floor).
//! - `gorilla`: production Gorilla, measured through the segment page path
//!   (SegmentWriter::write / decode_pages_soa).
//! - `gorilla_val`: the same Gorilla codec, decode throughput isolated from
//!   segment framing by subtraction (see METHOD below).
//! - `chimp128`: Chimp, 128-previous-values variant.
//! - `bytesplit_zstd`: byte-stream-split then zstd level 3.
//!
//! Workloads and distributions match `tests/gorilla_vs_raw.rs`: constant,
//! gauge random walk, cumulative counter; measured at 500 and 60 samples per
//! page. For each (workload, samples, codec) it reports payload bytes per
//! sample and encode/decode throughput in Melem/s, each the median of
//! repeated runs.
//!
//! METHOD (Gorilla throughput isolation): the production Gorilla codec's
//! encode/decode functions are crate-private to `ravel-segment`; the only
//! public API that exercises them is the segment writer/reader, which also
//! pays for the TS_DELTA page, LABEL_DICT, SERIES_TABLE, footer, and per-page
//! crc. The `gorilla` rows report that full segment-path cost (the number a
//! real read/write actually pays). The `gorilla_val` rows isolate the value
//! codec by measuring a second segment built with identical id, labels, and
//! timestamps but incompressible (random-bit) values, which forces the raw
//! VAL fallback (enc 17): every cost except the VAL codec is then shared, so
//!   gorilla_val_time = gorilla_seg_time - raw_seg_time + raw_bare_time
//! isolates the Gorilla value work plus the raw memcpy it replaces. The
//! decision rule is evaluated against `gorilla_val` (a fair codec-to-codec
//! comparison); `bytes/sample` is exact production output either way.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Instant;

use ravel_bench::codecs::{ByteSplitZstd, Chimp128, RawF64, ValueCodec};
use ravel_bench::generator::{WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{build_segment, decode_entries, val_page_bytes};
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesEntry, SeriesInput,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

const TS_PAGES: u32 = 3;
const VAL_GORILLA: u8 = 16;
const VAL_RAW_F64: u8 = 17;
const START_TS_NS: i64 = 1_700_000_000_000_000_000;
const INTERVAL_NS: i64 = 1_000_000_000;

const WARMUP: usize = 5;
const ITERS: usize = 99;

fn bench_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0u8; 16],
        shard: 0,
        writer_id: "bench-writer".to_string(),
        writer_epoch: 0,
        writer_seq: 0,
    }
}

fn bench_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    }
}

/// Median wall-clock nanoseconds of `f` over `ITERS` timed runs after
/// `WARMUP` untimed ones. `f` is timed as a whole; callers keep its body to
/// just the operation under test.
fn median_nanos(mut f: impl FnMut()) -> f64 {
    for _ in 0..WARMUP {
        f();
    }
    let mut samples = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let start = Instant::now();
        f();
        samples.push(start.elapsed().as_nanos() as f64);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).expect("no NaN nanos"));
    samples[samples.len() / 2]
}

fn melem_per_s(count: usize, nanos: f64) -> f64 {
    if nanos <= 0.0 {
        return f64::INFINITY;
    }
    (count as f64) / (nanos * 1e-9) / 1e6
}

// --- workloads (same distributions as tests/gorilla_vs_raw.rs) ----------

fn labels_for(workload: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "workload".to_string(),
        value: workload.to_string(),
    }])
    .expect("labels")
}

fn constant_series(n: usize) -> (SeriesId, LabelSet, Vec<Sample>) {
    let tenant = TenantId::new("bench-tenant");
    let labels = labels_for("constant");
    let samples = (0..n)
        .map(|i| Sample {
            ts_ns: START_TS_NS + i as i64 * INTERVAL_NS,
            value: 42.0,
        })
        .collect();
    let id = SeriesId::compute(&tenant, "bench_gauge", &labels).expect("compute id");
    (id, labels, samples)
}

fn generated_series(n: usize, counter_fraction: f64) -> (SeriesId, LabelSet, Vec<Sample>) {
    let config = WorkloadConfig {
        series_count: 1,
        samples_per_series: n,
        counter_fraction,
        ..Default::default()
    };
    generate_raw(&config)
        .expect("generate workload")
        .into_iter()
        .next()
        .expect("one series")
}

fn workload(name: &str, n: usize) -> (SeriesId, LabelSet, Vec<Sample>) {
    match name {
        "constant" => constant_series(n),
        "gauge_random_walk" => generated_series(n, 0.0),
        "counter" => generated_series(n, 1.0),
        other => panic!("unknown workload {other}"),
    }
}

// --- segment-path helpers for the production Gorilla codec ---------------

fn ts_page_bytes<'a>(
    bytes: &'a [u8],
    footer: &ravel_segment::Footer,
    entry: &SeriesEntry,
) -> &'a [u8] {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == TS_PAGES)
        .expect("TS_PAGES section present");
    let (off, len) = entry.ts_page;
    let abs = (section.offset + off) as usize;
    &bytes[abs..abs + len as usize]
}

/// Builds a single-series segment and returns (object bytes, entry, VAL page
/// enc byte, VAL payload byte length).
fn build_one(series: &(SeriesId, LabelSet, Vec<Sample>)) -> (Vec<u8>, SeriesEntry, u8, usize) {
    let written = build_segment(vec![series.clone()]);
    let bytes = written.bytes.to_vec();
    let (footer, entries) = decode_entries(&bytes);
    let entry = entries.into_iter().next().expect("one entry");
    let page = val_page_bytes(&bytes, &footer, &entry);
    let enc = page[0];
    let payload = page.len() - 6;
    (bytes, entry, enc, payload)
}

/// Times one segment write of `series` (dict + table + TS + VAL + footer).
fn time_segment_encode(series: &(SeriesId, LabelSet, Vec<Sample>)) -> f64 {
    median_nanos(|| {
        let inputs = vec![SeriesInput {
            series_id: series.0,
            labels: series.1.clone(),
            samples: series.2.clone(),
        }];
        let w =
            SegmentWriter::write(inputs, bench_identity(), bench_bounds()).expect("encode segment");
        std::hint::black_box(&w);
    })
}

/// Times one full page decode (TS + VAL) of the single series in `bytes`.
fn time_segment_decode(bytes: &[u8], footer: &ravel_segment::Footer, entry: &SeriesEntry) -> f64 {
    let limits = ReaderLimits::default();
    let ts = ts_page_bytes(bytes, footer, entry);
    let val = val_page_bytes(bytes, footer, entry);
    median_nanos(|| {
        let mut scratch = Vec::new();
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        ravel_segment::decode_pages_soa(
            entry,
            ts,
            val,
            limits,
            &mut scratch,
            &mut timestamps,
            &mut values,
        )
        .expect("decode pages");
        std::hint::black_box((&timestamps, &values));
    })
}

/// A copy of `series` with the same id, labels, and timestamps but
/// incompressible random-bit values, so the writer takes the raw VAL
/// fallback (enc 17). Used to subtract segment framing from Gorilla timings.
fn raw_forcing_twin(
    series: &(SeriesId, LabelSet, Vec<Sample>),
) -> (SeriesId, LabelSet, Vec<Sample>) {
    // Deterministic LCG over the sample index; no external RNG needed and no
    // dependence on wall-clock, so the twin is reproducible run to run.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let samples = series
        .2
        .iter()
        .map(|s| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            Sample {
                ts_ns: s.ts_ns,
                value: f64::from_bits(state),
            }
        })
        .collect();
    (series.0, series.1.clone(), samples)
}

struct Row {
    workload: String,
    samples: usize,
    codec: &'static str,
    bytes_per_sample: f64,
    encode_melem_s: f64,
    decode_melem_s: f64,
    note: &'static str,
}

fn measure_value_codec<C: ValueCodec>(
    codec: &C,
    workload_name: &str,
    n: usize,
    values: &[f64],
) -> Row {
    let encoded = codec.encode(values);
    let decoded = codec.decode(&encoded, values.len()).expect("decode");
    assert_eq!(
        decoded.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        "{} round-trip mismatch",
        codec.name()
    );
    let enc_nanos = median_nanos(|| {
        let e = codec.encode(values);
        std::hint::black_box(&e);
    });
    let dec_nanos = median_nanos(|| {
        let d = codec.decode(&encoded, n).expect("decode");
        std::hint::black_box(&d);
    });
    Row {
        workload: workload_name.to_string(),
        samples: n,
        codec: codec.name(),
        bytes_per_sample: encoded.len() as f64 / n as f64,
        encode_melem_s: melem_per_s(n, enc_nanos),
        decode_melem_s: melem_per_s(n, dec_nanos),
        note: "",
    }
}

fn measure_workload(workload_name: &str, n: usize) -> Vec<Row> {
    let series = workload(workload_name, n);
    let values: Vec<f64> = series.2.iter().map(|s| s.value).collect();

    let mut rows = Vec::new();

    // raw baseline
    rows.push(measure_value_codec(&RawF64, workload_name, n, &values));

    // production Gorilla via the segment public API.
    let (bytes, entry, enc, payload) = build_one(&series);
    let (footer, _) = decode_entries(&bytes);
    let gorilla_enc_note = if enc == VAL_GORILLA {
        "seg page path"
    } else if enc == VAL_RAW_F64 {
        "RAW FALLBACK"
    } else {
        "unexpected enc"
    };
    let g_enc = time_segment_encode(&series);
    let g_dec = time_segment_decode(&bytes, &footer, &entry);
    rows.push(Row {
        workload: workload_name.to_string(),
        samples: n,
        codec: "gorilla",
        bytes_per_sample: payload as f64 / n as f64,
        encode_melem_s: melem_per_s(n, g_enc),
        decode_melem_s: melem_per_s(n, g_dec),
        note: gorilla_enc_note,
    });

    // Isolated Gorilla value codec: subtract the raw-fallback twin's framing.
    let twin = raw_forcing_twin(&series);
    let (twin_bytes, twin_entry, twin_enc, _) = build_one(&twin);
    let (twin_footer, _) = decode_entries(&twin_bytes);
    if twin_enc == VAL_RAW_F64 {
        // Decode only: on read the VAL/TS pages are comp=none, so page decode
        // is not framing-dominated and the subtraction isolates the Gorilla
        // value work cleanly. Encode is NOT isolated this way: SegmentWriter
        // zstd-compresses LABEL_DICT/SERIES_TABLE and builds the footer, so
        // segment-write time swamps VAL encode and the difference is noise
        // (it would collapse to the sentinel). Encode is reported only as the
        // segment-path `gorilla` row; the decision rule uses decode speed.
        let r_dec_seg = time_segment_decode(&twin_bytes, &twin_footer, &twin_entry);
        let raw_vals: Vec<f64> = twin.2.iter().map(|s| s.value).collect();
        let raw_encoded = RawF64.encode(&raw_vals);
        let r_dec_bare = median_nanos(|| {
            let d = RawF64.decode(&raw_encoded, n).expect("decode");
            std::hint::black_box(&d);
        });
        let g_val_dec = (g_dec - r_dec_seg + r_dec_bare).max(1.0);
        rows.push(Row {
            workload: workload_name.to_string(),
            samples: n,
            codec: "gorilla_val",
            bytes_per_sample: payload as f64 / n as f64,
            encode_melem_s: f64::NAN,
            decode_melem_s: melem_per_s(n, g_val_dec),
            note: "decode isolated est.",
        });
    } else {
        rows.push(Row {
            workload: workload_name.to_string(),
            samples: n,
            codec: "gorilla_val",
            bytes_per_sample: payload as f64 / n as f64,
            encode_melem_s: f64::NAN,
            decode_melem_s: f64::NAN,
            note: "no raw twin",
        });
    }

    // challengers
    rows.push(measure_value_codec(&Chimp128, workload_name, n, &values));
    rows.push(measure_value_codec(
        &ByteSplitZstd,
        workload_name,
        n,
        &values,
    ));

    rows
}

// Column headers are width-formatted string literals; that is the point of a
// table header, so the print_literal lint is not useful here.
#[allow(clippy::print_literal)]
fn print_table(rows: &[Row]) {
    println!(
        "{:<18}{:>8}  {:<16}{:>14}{:>16}{:>16}  {}",
        "workload", "samples", "codec", "bytes/sample", "encode Melem/s", "decode Melem/s", "note"
    );
    let fmt = |v: f64| {
        if v.is_nan() {
            "n/a".to_string()
        } else {
            format!("{v:.2}")
        }
    };
    for r in rows {
        println!(
            "{:<18}{:>8}  {:<16}{:>14.4}{:>16}{:>16}  {}",
            r.workload,
            r.samples,
            r.codec,
            r.bytes_per_sample,
            fmt(r.encode_melem_s),
            fmt(r.decode_melem_s),
            r.note
        );
    }
}

/// Applies the issue's decision rule per (workload, samples): a challenger
/// that beats `gorilla_val` by >= 25% on bytes/sample at comparable or better
/// decode speed (>= 100% of gorilla_val's decode Melem/s) recommends the
/// enc-18 ADR for that workload.
fn print_verdict(rows: &[Row]) {
    println!("\nDECISION RULE (issue #99): challenger wins if bytes/sample <= 75% of");
    println!("gorilla_val AND decode Melem/s >= gorilla_val decode Melem/s.\n");
    let challengers = ["chimp128", "bytesplit_zstd"];
    let mut keys: Vec<(String, usize)> = rows
        .iter()
        .map(|r| (r.workload.clone(), r.samples))
        .collect();
    keys.dedup();
    for (wl, n) in keys {
        let base = rows
            .iter()
            .find(|r| r.workload == wl && r.samples == n && r.codec == "gorilla_val");
        let Some(base) = base else { continue };
        for cname in challengers {
            if let Some(c) = rows
                .iter()
                .find(|r| r.workload == wl && r.samples == n && r.codec == cname)
            {
                let bytes_win = 1.0 - c.bytes_per_sample / base.bytes_per_sample;
                let decode_ratio = c.decode_melem_s / base.decode_melem_s;
                let wins = c.bytes_per_sample <= 0.75 * base.bytes_per_sample
                    && c.decode_melem_s >= base.decode_melem_s;
                println!(
                    "{:<18} n={:<4} {:<16} bytes {:+6.1}% vs gorilla_val, decode {:.2}x -> {}",
                    wl,
                    n,
                    cname,
                    bytes_win * 100.0,
                    decode_ratio,
                    if wins { "RECOMMEND enc-18" } else { "no" }
                );
            }
        }
    }
}

fn main() {
    println!("Ravel issue #99 float value codec bake-off");
    println!(
        "warmup={WARMUP} timed_iters={ITERS} (median reported); throughput per value stream\n"
    );

    let mut all = Vec::new();
    for &n in &[500usize, 60usize] {
        for wl in ["constant", "gauge_random_walk", "counter"] {
            all.extend(measure_workload(wl, n));
        }
    }
    print_table(&all);
    print_verdict(&all);
}
