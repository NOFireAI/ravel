//! Gorilla vs raw f64 page sizes on constant, gauge random-walk, and counter
//! workloads. Criterion measures time, not size,
//! so this ships as a plain `#[test]` that prints a small table; run with
//! `cargo test -p ravel-bench --test gorilla_vs_raw -- --nocapture` to see
//! it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::generator::{WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{build_segment, decode_entries, val_page_bytes};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

const SAMPLE_COUNT: usize = 500;
const START_TS_NS: i64 = 1_700_000_000_000_000_000;
const INTERVAL_NS: i64 = 1_000_000_000;
const VAL_GORILLA: u8 = 16;
const VAL_RAW_F64: u8 = 17;
// ADR-0092 decision 6 added two value page encodings, selected per page against
// the prior ones and kept only when smaller. A bench workload can now land on
// either, so this measurement must name them rather than panic on an
// "unexpected" encoding.
const VAL_ALP: u8 = 18;
const VAL_GCD_DELTA_FOR: u8 = 19;

fn labels_for(workload: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "workload".to_string(),
        value: workload.to_string(),
    }])
    .expect("labels")
}

fn constant_series() -> (SeriesId, LabelSet, Vec<Sample>) {
    let tenant = TenantId::new("bench-tenant");
    let labels = labels_for("constant");
    let samples = (0..SAMPLE_COUNT)
        .map(|i| Sample {
            ts_ns: START_TS_NS + i as i64 * INTERVAL_NS,
            value: 42.0,
        })
        .collect();
    let id = SeriesId::compute(&tenant, "bench_gauge", &labels).expect("compute id");
    (id, labels, samples)
}

fn gauge_random_walk_series() -> (SeriesId, LabelSet, Vec<Sample>) {
    let config = WorkloadConfig {
        series_count: 1,
        samples_per_series: SAMPLE_COUNT,
        counter_fraction: 0.0,
        ..Default::default()
    };
    generate_raw(&config)
        .expect("generate workload")
        .into_iter()
        .next()
        .expect("one series")
}

fn counter_series() -> (SeriesId, LabelSet, Vec<Sample>) {
    let config = WorkloadConfig {
        series_count: 1,
        samples_per_series: SAMPLE_COUNT,
        counter_fraction: 1.0,
        ..Default::default()
    };
    generate_raw(&config)
        .expect("generate workload")
        .into_iter()
        .next()
        .expect("one series")
}

struct Row {
    workload: &'static str,
    enc: &'static str,
    payload_bytes: usize,
    bytes_per_sample: f64,
}

fn measure(workload: &'static str, series: (SeriesId, LabelSet, Vec<Sample>)) -> Row {
    let sample_count = series.2.len();
    let written = build_segment(vec![series]);
    let bytes = written.bytes;
    let (footer, entries) = decode_entries(&bytes);
    let entry = entries.first().expect("one series entry");
    let page = val_page_bytes(&bytes, &footer, entry);
    let enc_byte = page[0];
    let payload_bytes = page.len() - 6;
    let enc = match enc_byte {
        VAL_GORILLA => "gorilla",
        VAL_RAW_F64 => "raw_f64",
        VAL_ALP => "alp",
        VAL_GCD_DELTA_FOR => "gcd_delta_for",
        other => panic!("unexpected val page encoding: {other}"),
    };
    Row {
        workload,
        enc,
        payload_bytes,
        bytes_per_sample: payload_bytes as f64 / sample_count as f64,
    }
}

#[test]
fn gorilla_vs_raw_page_sizes() {
    let rows = vec![
        measure("constant", constant_series()),
        measure("gauge_random_walk", gauge_random_walk_series()),
        measure("counter", counter_series()),
    ];

    println!("\nworkload            enc      payload_bytes  bytes/sample");
    for row in &rows {
        println!(
            "{:<20}{:<9}{:<15}{:.3}",
            row.workload, row.enc, row.payload_bytes, row.bytes_per_sample
        );
    }

    for row in &rows {
        assert!(row.payload_bytes > 0);
        assert!(
            row.bytes_per_sample <= 8.0,
            "raw fallback caps at 8 bytes/sample"
        );
    }
    let constant = rows.iter().find(|r| r.workload == "constant").expect("row");
    assert!(
        constant.bytes_per_sample < 1.0,
        "constant values should compress far below 1 byte/sample under Gorilla, got {}",
        constant.bytes_per_sample
    );
}
