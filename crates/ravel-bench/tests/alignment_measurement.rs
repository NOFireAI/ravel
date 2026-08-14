//! Measures, on production-shaped bench workloads, what fraction of VAL pages
//! use VAL_RAW_F64 rather than VAL_GORILLA, by page count and by page byte
//! size.
//!
//! A raw-f64/Gorilla page-kind counter does not exist in ravel-segment or
//! ravel-query, so it is computed here, scoped to ravel-bench, using the enc
//! byte ravel-bench already reads in `tests/gorilla_vs_raw.rs` via
//! `segment_support::val_page_bytes`. Run with `cargo test -p ravel-bench
//! --test alignment_measurement -- --nocapture` to see the table.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{build_segment, decode_entries, val_page_bytes};

const VAL_GORILLA: u8 = 16;
const VAL_RAW_F64: u8 = 17;

#[derive(Default, Clone, Copy)]
struct EncStats {
    pages: u64,
    bytes: u64,
}

#[derive(Default, Clone, Copy)]
struct Tally {
    gorilla: EncStats,
    raw_f64: EncStats,
}

impl Tally {
    fn record(&mut self, enc: u8, page_bytes: usize) {
        let bucket = match enc {
            VAL_GORILLA => &mut self.gorilla,
            VAL_RAW_F64 => &mut self.raw_f64,
            other => panic!("unexpected VAL page encoding: {other}"),
        };
        bucket.pages += 1;
        bucket.bytes += page_bytes as u64;
    }

    fn total_pages(&self) -> u64 {
        self.gorilla.pages + self.raw_f64.pages
    }

    fn total_bytes(&self) -> u64 {
        self.gorilla.bytes + self.raw_f64.bytes
    }

    fn raw_page_fraction(&self) -> f64 {
        self.raw_f64.pages as f64 / self.total_pages().max(1) as f64
    }

    fn raw_byte_fraction(&self) -> f64 {
        self.raw_f64.bytes as f64 / self.total_bytes().max(1) as f64
    }
}

struct Scenario {
    name: &'static str,
    config: WorkloadConfig,
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "baseline (1k series, 100 samples, mixed)",
            config: WorkloadConfig {
                series_count: 1_000,
                samples_per_series: 100,
                counter_fraction: 0.5,
                ..Default::default()
            },
        },
        Scenario {
            name: "short series (1k series, 10 samples)",
            config: WorkloadConfig {
                series_count: 1_000,
                samples_per_series: 10,
                counter_fraction: 0.5,
                ..Default::default()
            },
        },
        Scenario {
            name: "long series (200 series, 2000 samples)",
            config: WorkloadConfig {
                series_count: 200,
                samples_per_series: 2_000,
                counter_fraction: 0.5,
                ..Default::default()
            },
        },
        Scenario {
            name: "counter-heavy (1k series, 100 samples)",
            config: WorkloadConfig {
                series_count: 1_000,
                samples_per_series: 100,
                counter_fraction: 1.0,
                ..Default::default()
            },
        },
        Scenario {
            name: "gauge-only (1k series, 100 samples)",
            config: WorkloadConfig {
                series_count: 1_000,
                samples_per_series: 100,
                counter_fraction: 0.0,
                ..Default::default()
            },
        },
        Scenario {
            name: "high label churn (1k series, 100 samples, churn 0.5)",
            config: WorkloadConfig {
                series_count: 1_000,
                samples_per_series: 100,
                counter_fraction: 0.5,
                label_churn_rate: 0.5,
                ..Default::default()
            },
        },
        Scenario {
            name: "few-big cardinality (1k series, 100 samples)",
            config: WorkloadConfig {
                series_count: 1_000,
                samples_per_series: 100,
                counter_fraction: 0.5,
                cardinality: CardinalityProfile::few_big(1_000),
                ..Default::default()
            },
        },
        Scenario {
            name: "high jitter + out-of-order (1k series, 100 samples)",
            config: WorkloadConfig {
                series_count: 1_000,
                samples_per_series: 100,
                counter_fraction: 0.5,
                jitter_ns: 500_000_000,
                out_of_order_fraction: 0.1,
                ..Default::default()
            },
        },
    ]
}

fn measure(config: &WorkloadConfig) -> Tally {
    let raw = generate_raw(config).expect("generate workload");
    let written = build_segment(raw);
    let bytes = written.bytes;
    let (footer, entries) = decode_entries(&bytes);

    let mut tally = Tally::default();
    for entry in &entries {
        let page = val_page_bytes(&bytes, &footer, entry);
        let enc = page[0];
        tally.record(enc, page.len());
    }
    tally
}

#[test]
fn raw_f64_vs_gorilla_page_fractions() {
    let mut overall = Tally::default();

    println!(
        "\n{:<50} {:>10} {:>10} {:>12} {:>12} {:>10} {:>10}",
        "scenario", "raw_pages", "gor_pages", "raw_bytes", "gor_bytes", "raw%pages", "raw%bytes"
    );
    for scenario in scenarios() {
        let tally = measure(&scenario.config);
        println!(
            "{:<50} {:>10} {:>10} {:>12} {:>12} {:>9.2}% {:>9.2}%",
            scenario.name,
            tally.raw_f64.pages,
            tally.gorilla.pages,
            tally.raw_f64.bytes,
            tally.gorilla.bytes,
            tally.raw_page_fraction() * 100.0,
            tally.raw_byte_fraction() * 100.0,
        );
        overall.gorilla.pages += tally.gorilla.pages;
        overall.gorilla.bytes += tally.gorilla.bytes;
        overall.raw_f64.pages += tally.raw_f64.pages;
        overall.raw_f64.bytes += tally.raw_f64.bytes;

        assert!(tally.total_pages() > 0, "scenario produced no VAL pages");
    }

    println!(
        "{:<50} {:>10} {:>10} {:>12} {:>12} {:>9.2}% {:>9.2}%",
        "OVERALL",
        overall.raw_f64.pages,
        overall.gorilla.pages,
        overall.raw_f64.bytes,
        overall.gorilla.bytes,
        overall.raw_page_fraction() * 100.0,
        overall.raw_byte_fraction() * 100.0,
    );

    assert!(overall.total_pages() > 0);
}
