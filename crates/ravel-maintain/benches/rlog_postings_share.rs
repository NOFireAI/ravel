//! Share of a whole RLOG object build spent in the POSTINGS accumulation
//! (issue #3263), on the `sorted` postings fixture.
//!
//! The same rows are timed three ways per iteration, in an order that rotates
//! each iteration:
//!
//! - `postings`: arm A of `common/postings.rs`, the accumulation loop as
//!   `RlogWriter::build_object` runs it, over the rows' indexed values;
//! - `build_indexed`: `RlogWriter::finish_with_stats` over those rows as
//!   `LogRecord`s, with the three fields indexed, push included;
//! - `build_plain`: the same build with no indexed field.
//!
//! Each record carries the fixture's `service.name` and `k8s.namespace.name`
//! as resource attributes and its `http.status_code` as a record attribute,
//! plus the body and the other attributes of the logseg benches' corpus
//! (`crates/ravel-logseg/benches/common/mod.rs`, `make_record`).
//!
//! Reported per iteration and summarised by the median with a
//! distribution-free 95% interval (order statistics of the binomial):
//! `share_loop` = postings / build_indexed, and `share_delta` =
//! (build_indexed - build_plain) / build_indexed, which also charges the
//! row resolution and POSTINGS encoding that indexing adds.
//!
//! Usage: `cargo bench -p ravel-maintain --bench rlog_postings_share`. Exits
//! non-zero when the build's postings counters disagree with the fixture,
//! or, on `large`, the `share_loop` median falls outside its pre-registered
//! band. Without `--bench` (as `cargo test --all-targets` runs it) it runs
//! `small` for two iterations and checks the counters only.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::hint::black_box;
use std::ops::RangeInclusive;
use std::process::ExitCode;
use std::time::Instant;

use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_types::logstream::log_stream_id;

#[path = "common/postings.rs"]
mod postings;

use postings::{Value, distinct_terms, geometry, run_arm, sorted_fixture};

/// Pre-registered band for the `share_loop` median on `large`, in percent
/// (docs/internal/bbtree-ordered-maps-3254.md, Candidate 2, #3263).
const SHARE_LOOP_BAND_PCT: RangeInclusive<f64> = 1.0..=12.0;

const INDEXED_FIELDS: [&str; 3] = ["service.name", "k8s.namespace.name", "http.status_code"];

const SEVERITIES: [&str; 5] = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"];
const BODY_WORDS: [&str; 8] = [
    "request",
    "completed",
    "connection",
    "refused",
    "timeout",
    "retrying",
    "handler",
    "dispatch",
];

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 1,
        writer_id: [9u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn str_of(v: &Value) -> String {
    match v {
        Value::Str(b) => String::from_utf8(b.clone()).expect("fixture terms are ASCII"),
        Value::I64(_) => panic!("expected a string term"),
    }
}

/// The fixture's rows as records, in row order.
fn records(row_stream: &[u32], rows: &[Vec<(u32, Value)>]) -> Vec<LogRecord> {
    let mut streams: Vec<Option<(ravel_logseg::LogStreamId, Vec<u8>)>> = Vec::new();
    rows.iter()
        .zip(row_stream)
        .enumerate()
        .map(|(i, (row, &s))| {
            let s = s as usize;
            if streams.len() <= s {
                streams.resize(s + 1, None);
            }
            let (stream_id, stream_attrs) = streams[s]
                .get_or_insert_with(|| {
                    let resource = vec![
                        (
                            "service.name".to_string(),
                            AttrValue::Str(str_of(&row[0].1)),
                        ),
                        (
                            "k8s.namespace.name".to_string(),
                            AttrValue::Str(str_of(&row[1].1)),
                        ),
                    ];
                    (
                        log_stream_id(&resource, "otel-scope", "1.0", &[]),
                        stream_attrs_bytes(&resource, "otel-scope", "1.0", &[]),
                    )
                })
                .clone();
            let status = match row[2].1 {
                Value::I64(v) => v,
                Value::Str(_) => panic!("expected an integer status"),
            };
            let word_a = BODY_WORDS[i % BODY_WORDS.len()];
            let word_b = BODY_WORDS[(i / 7) % BODY_WORDS.len()];
            let ts_ns = i as i64 * 1_000;
            LogRecord {
                stream_id,
                stream_attrs,
                ts_ns,
                observed_ts_ns: ts_ns + 1_000,
                severity_num: (i % 24) as u8 + 1,
                severity_text: SEVERITIES[i % SEVERITIES.len()].to_string(),
                body: format!("{word_a} {word_b} id={i}"),
                trace_id: Some([(s as u8).wrapping_add(i as u8); 16]),
                span_id: Some([(i as u8); 8]),
                flags: 0,
                attrs: vec![
                    ("http.status_code".to_string(), AttrValue::I64(status)),
                    (
                        "duration_ms".to_string(),
                        AttrValue::F64((i % 250) as f64 * 1.5),
                    ),
                    (
                        "retryable".to_string(),
                        AttrValue::Bool(i.is_multiple_of(3)),
                    ),
                    (
                        "route".to_string(),
                        AttrValue::Str(format!("/api/v1/resource/{}", i % 20)),
                    ),
                ],
            }
        })
        .collect()
}

fn build(records: Vec<LogRecord>, indexed: bool) -> (Vec<u8>, ravel_logseg::writer::WriteStats) {
    let mut w = RlogWriter::new(RlogConfig::default(), identity());
    if indexed {
        w = w.with_indexed_fields(INDEXED_FIELDS.iter().map(|f| f.to_string()).collect());
    }
    for r in records {
        w.push(r).expect("push");
    }
    w.finish_with_stats().expect("build")
}

/// Ranks (1-based) of the order statistics bounding a distribution-free
/// confidence interval of at least 95% for the median of `n` values.
fn median_ci_ranks(n: usize) -> (usize, usize) {
    // P(X <= j) for X ~ Binomial(n, 1/2).
    let mut cdf = Vec::with_capacity(n + 1);
    let mut pmf = 0.5f64.powi(n as i32);
    let mut acc = 0.0;
    for j in 0..=n {
        if j > 0 {
            pmf *= (n - j + 1) as f64 / j as f64;
        }
        acc += pmf;
        cdf.push(acc);
    }
    let mut k = 1;
    while k < n && cdf[k] <= 0.025 {
        k += 1;
    }
    (k, n + 1 - k)
}

struct Summary {
    median: f64,
    lo: f64,
    hi: f64,
}

fn summarise(values: &[f64]) -> Summary {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    let n = v.len();
    let median = if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    };
    let (lo, hi) = median_ci_ranks(n);
    Summary {
        median,
        lo: v[lo - 1],
        hi: v[hi - 1],
    }
}

fn main() -> ExitCode {
    let bench = std::env::args().any(|a| a == "--bench");
    let (name, iterations) = if bench { ("large", 21) } else { ("small", 2) };
    let g = geometry(name);
    let fx = sorted_fixture(g);
    let distinct = distinct_terms(&fx.rows);
    let expected_distinct: u64 = distinct.iter().map(|d| *d as u64).sum();
    let recs = records(&fx.row_stream, &fx.rows);
    println!(
        "rlog_postings_share family=sorted geometry={} rows={} streams={} distinct={},{},{} \
         iterations={iterations}",
        g.name,
        fx.rows.len(),
        g.streams,
        distinct[0],
        distinct[1],
        distinct[2]
    );

    let mut failed = false;
    let (_, stats) = build(recs.clone(), true);
    println!(
        "build_indexed postings_indexed_fields={} postings_capped_fields={} \
         postings_distinct_total={} postings_bytes={}",
        stats.postings_indexed_fields,
        stats.postings_capped_fields,
        stats.postings_distinct_total,
        stats.postings_bytes
    );
    if stats.postings_indexed_fields != 3
        || stats.postings_capped_fields != 0
        || stats.postings_distinct_total != expected_distinct
    {
        eprintln!(
            "FAIL build_indexed counters: expected 3 indexed fields, 0 capped, \
             {expected_distinct} distinct terms"
        );
        failed = true;
    }
    let (_, plain) = build(recs.clone(), false);
    if plain.postings_indexed_fields != 0 || plain.postings_bytes != 0 {
        eprintln!("FAIL build_plain wrote postings");
        failed = true;
    }

    let mut t_post = Vec::with_capacity(iterations);
    let mut t_idx = Vec::with_capacity(iterations);
    let mut t_plain = Vec::with_capacity(iterations);
    for i in 0..iterations {
        for step in 0..3 {
            match (i + step) % 3 {
                0 => {
                    let start = Instant::now();
                    black_box(run_arm('A', black_box(&fx.rows), &mut || {}));
                    t_post.push(start.elapsed().as_secs_f64());
                }
                1 => {
                    let input = recs.clone();
                    let start = Instant::now();
                    black_box(build(input, true));
                    t_idx.push(start.elapsed().as_secs_f64());
                }
                _ => {
                    let input = recs.clone();
                    let start = Instant::now();
                    black_box(build(input, false));
                    t_plain.push(start.elapsed().as_secs_f64());
                }
            }
        }
    }

    let ms = |v: &[f64]| summarise(&v.iter().map(|s| s * 1e3).collect::<Vec<_>>());
    for (label, v) in [
        ("postings", &t_post),
        ("build_indexed", &t_idx),
        ("build_plain", &t_plain),
    ] {
        let s = ms(v);
        println!(
            "time {label} median_ms={:.2} ci95_ms={:.2}..{:.2}",
            s.median, s.lo, s.hi
        );
    }
    let share_loop: Vec<f64> = t_post
        .iter()
        .zip(&t_idx)
        .map(|(p, b)| p / b * 100.0)
        .collect();
    let share_delta: Vec<f64> = t_idx
        .iter()
        .zip(&t_plain)
        .map(|(b, p)| (b - p) / b * 100.0)
        .collect();
    let loop_s = summarise(&share_loop);
    let delta_s = summarise(&share_delta);
    println!(
        "share_loop median_pct={:.2} ci95_pct={:.2}..{:.2}",
        loop_s.median, loop_s.lo, loop_s.hi
    );
    println!(
        "share_delta median_pct={:.2} ci95_pct={:.2}..{:.2}",
        delta_s.median, delta_s.lo, delta_s.hi
    );
    if g.name == "large" {
        let verdict = if SHARE_LOOP_BAND_PCT.contains(&loop_s.median) {
            "PASS"
        } else {
            failed = true;
            "MISS"
        };
        println!(
            "band share_loop geometry=large median_pct={:.2} band={}..={} {verdict}",
            loop_s.median,
            SHARE_LOOP_BAND_PCT.start(),
            SHARE_LOOP_BAND_PCT.end()
        );
        let clause = if loop_s.lo >= 2.0 { "MET" } else { "NOT-MET" };
        println!(
            "clause share_loop>=2% ci95_lower_pct={:.2} {clause}",
            loop_s.lo
        );
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
