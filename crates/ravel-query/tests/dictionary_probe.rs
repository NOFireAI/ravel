//! The two-step sparse catalog probe (ADR-3694 decision 1, gated per
//! statement by the issue #3862 amendment): LABEL_DICT is read alone first,
//! an object whose dictionary lacks a positive equality value is dropped
//! with no further GET, and a statement whose two-step probes mostly hold
//! the value switches back to the one-step probe.
//!
//! Every fixture object is a sparse RSEG object above the fetcher's 512 KiB
//! whole-object threshold, so its first GET reads only the footer tail and
//! the catalog sections are fetched by the probe phase on their own.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use axum::http::{Request, StatusCode};
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::fault::Occurrence;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_promql::Value;
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine, QueryError, QueryStats};
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7,
    open_from_full,
};
use ravel_types::accounting::AccountedOp;
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// Series per object: the writer's sparse (v5 chunked catalog) threshold.
const SERIES_PER_OBJECT: usize = 4096;
/// Samples per series, enough incompressible values to carry each object
/// past the fetcher's 512 KiB whole-object threshold.
const SAMPLES_PER_SERIES: usize = 20;
/// The fetcher's whole-object threshold and footer tail length.
const WHOLE_OBJECT_THRESHOLD: u64 = 512 * 1024;
const FOOTER_TAIL: u64 = 64 * 1024;
/// Footer section kinds of the four sparse catalog sections.
const LABEL_DICT: u32 = 1;
const SERIES_IDS: u32 = 5;
const SERIES_IDX: u32 = 8;
const SERIES_META_CHUNKS: u32 = 9;

const METRIC: &str = "probe_metric";
const TARGET: &str = "target";

fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn hour_start(hour: u32) -> i64 {
    i64::from(hour) * NS_PER_HOUR
}

/// One published sparse object and the probe-phase bytes its catalog costs.
struct Published {
    data_key: String,
    /// LABEL_DICT's stored length: step one's GET.
    dict_bytes: u64,
    /// The one coalesced GET for the catalog sections the footer tail does
    /// not already hold, after LABEL_DICT is held (step two).
    rest_bytes: u64,
    /// The one-step probe's single coalesced GET for all four sections.
    one_step_bytes: u64,
}

fn span(ranges: &[(u64, u64)]) -> u64 {
    let start = ranges.iter().map(|r| r.0).min().expect("a range");
    let end = ranges.iter().map(|r| r.1).max().expect("a range");
    end - start
}

/// Writes one sparse object of `SERIES_PER_OBJECT` series of `METRIC`, each
/// labelled `series="s{i}"` and `instance="obj{object}"`, except that series
/// `s0` carries `instance="target"` when `holds_target`. Values are
/// incompressible and distinct per object, series and sample.
async fn publish_sparse(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    object: u64,
    hour: u32,
    base_ts: i64,
    holds_target: bool,
) -> Published {
    publish_sparse_named(store, tenant_id, object, hour, base_ts, holds_target, METRIC).await
}

/// [`publish_sparse`] with every series named `metric`.
async fn publish_sparse_named(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    object: u64,
    hour: u32,
    base_ts: i64,
    holds_target: bool,
    metric: &str,
) -> Published {
    let tenant_hash = tenant_id.hash();
    let writer_id = Uuid::from_u128(u128::from(object) + 1);
    let mut inputs = Vec::with_capacity(SERIES_PER_OBJECT);
    for i in 0..SERIES_PER_OBJECT {
        let instance = if holds_target && i == 0 {
            TARGET.to_string()
        } else {
            format!("obj{object}")
        };
        let labels = LabelSet::new(vec![
            Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: metric.to_string(),
            },
            Label {
                name: "instance".to_string(),
                value: instance,
            },
            Label {
                name: "series".to_string(),
                value: format!("s{i}"),
            },
        ])
        .expect("valid labels");
        let series_id = SeriesId::compute(tenant_id, metric, &labels).expect("series id");
        let samples = (0..SAMPLES_PER_SERIES)
            .map(|j| {
                let bits = (object.wrapping_mul(0xD6E8_FEB8_6659_FD93))
                    .wrapping_add((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
                    .wrapping_add((j as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F));
                Sample {
                    ts_ns: base_ts + j as i64 * NS_PER_SEC,
                    value: f64::from_bits((bits & 0x000F_FFFF_FFFF_FFFF) | 0x3FF0_0000_0000_0000),
                }
            })
            .collect();
        inputs.push(SeriesInput {
            series_id,
            labels,
            samples,
        });
    }
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: object + 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write sparse segment");
    let total = written.bytes.len() as u64;
    assert!(
        total > WHOLE_OBJECT_THRESHOLD,
        "the object must exceed the {WHOLE_OBJECT_THRESHOLD} B whole-object threshold, \
         or its first GET holds every catalog section and no probe runs; it is {total} B"
    );

    let footer = open_from_full(&written.bytes, ReaderLimits::default())
        .expect("parse footer")
        .footer;
    let range = |kind: u32| {
        let s = footer
            .sections
            .iter()
            .find(|s| s.kind == kind)
            .unwrap_or_else(|| panic!("section {kind} present: the object must be sparse"));
        (s.offset, s.offset + s.len)
    };
    let dict = range(LABEL_DICT);
    let rest = [
        range(SERIES_IDS),
        range(SERIES_IDX),
        range(SERIES_META_CHUNKS),
    ];
    let tail_start = total - FOOTER_TAIL;
    let uncovered: Vec<(u64, u64)> = rest.iter().copied().filter(|r| r.0 < tail_start).collect();
    assert!(
        dict.0 < tail_start && !uncovered.is_empty(),
        "the footer tail must not already hold LABEL_DICT or every other catalog section"
    );
    let rest_span = (
        uncovered.iter().map(|r| r.0).min().expect("a range"),
        uncovered.iter().map(|r| r.1).max().expect("a range"),
    );
    assert!(
        dict.1 <= rest_span.0 || dict.0 >= rest_span.1,
        "step two's coalesced GET must not span LABEL_DICT, or the dictionary is read twice"
    );
    let mut all = uncovered.clone();
    all.push(dict);

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: object + 1,
        object_size: total,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: 0,
        ingest_hour_bucket: hour,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    Published {
        data_key,
        dict_bytes: dict.1 - dict.0,
        rest_bytes: span(&uncovered),
        one_step_bytes: span(&all),
    }
}

/// Publishes `n` sparse objects into ingest hour `hour`, their samples
/// starting at half past it; the first `h` hold the target series.
async fn publish_fixture(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    hour: u32,
    n: u64,
    h: u64,
) -> Vec<Published> {
    let base_ts = hour_start(hour) + 30 * 60 * NS_PER_SEC;
    let mut out = Vec::new();
    for object in 0..n {
        out.push(publish_sparse(store, tenant_id, object, hour, base_ts, object < h).await);
    }
    out
}

fn engine(store: Arc<dyn ObjectStoreBackend>, fan_out: usize) -> QueryEngine {
    let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
    QueryEngine::new(
        Arc::new(catalog),
        store,
        EngineConfig {
            promql_fetch_fanout: Some(fan_out),
            ..EngineConfig::default()
        },
    )
}

/// Query time: the last sample of every series, inside the unsealed hour so
/// the snapshot is read from listing, unfolded.
fn query_at(hour: u32) -> (i64, i64) {
    let last =
        hour_start(hour) + 30 * 60 * NS_PER_SEC + (SAMPLES_PER_SERIES as i64 - 1) * NS_PER_SEC;
    (last / 1_000_000, last + 60 * NS_PER_SEC)
}

async fn run(
    engine: &QueryEngine,
    th: TenantHash,
    query: &str,
    t_ms: i64,
    now: i64,
) -> (Value, QueryStats) {
    engine
        .instant_with_stats(th, query, t_ms, &[], now, Duration::from_secs(30))
        .await
        .unwrap_or_else(|err| panic!("query {query}: {err}"))
}

/// Sorted `(labels, value bits)` of an instant vector.
fn vector_bits(value: &Value) -> Vec<(String, u64)> {
    match value {
        Value::Vector(v) => {
            let mut out: Vec<(String, u64)> = v
                .iter()
                .map(|s| (format!("{:?}", s.labels), s.value.to_bits()))
                .collect();
            out.sort();
            out
        }
        other => panic!("expected instant vector, got {other:?}"),
    }
}

fn probe_gets(stats: &QueryStats) -> u64 {
    stats.phase_accounting.probe.s3_requests(AccountedOp::Get)
}

fn probe_bytes(stats: &QueryStats) -> u64 {
    stats.phase_accounting.probe.s3_bytes(AccountedOp::Get)
}

/// A one-series equality selector over N objects of which H hold the value:
/// N dictionary GETs plus H remaining-range GETs, N - H objects dropped by
/// their dictionary, and the same samples as the one-step probe. A `!=` or
/// regex matcher never drops an object.
#[tokio::test]
async fn selective_statement_drops_objects_whose_dictionary_lacks_the_value() {
    const N: u64 = 6;
    const H: u64 = 2;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme".to_string());
    let th = tid.hash();
    let hour = 7_000u32;
    let objects = publish_fixture(store.as_ref(), &tid, hour, N, H).await;
    let (t_ms, now) = query_at(hour);
    // Fan-out above N: every probe completes before the gate could decide.
    let engine = engine(store, 8);

    let (value, stats) = run(&engine, th, "probe_metric{instance=\"target\"}", t_ms, now).await;
    assert_eq!(
        stats.segments_pruned, 0,
        "an unfolded snapshot prunes nothing by postings"
    );
    assert_eq!(
        stats.segments_fetched, N,
        "an object dropped by its dictionary still counts as fetched"
    );
    assert_eq!(stats.segments_probed_two_step, N);
    assert_eq!(stats.segments_pruned_by_dictionary, N - H);
    assert!(!stats.dictionary_probe_switched_off);
    assert_eq!(
        probe_gets(&stats),
        N + H,
        "N dictionary GETs plus H remaining-range GETs"
    );
    let expected_bytes: u64 = objects.iter().map(|o| o.dict_bytes).sum::<u64>()
        + objects
            .iter()
            .take(H as usize)
            .map(|o| o.rest_bytes)
            .sum::<u64>();
    assert_eq!(probe_bytes(&stats), expected_bytes);

    // A regex or a negated matcher never drops an object, whatever its value
    // spells: the dictionary is checked for positive equalities only. Every
    // dictionary holds `series="s0"`, so each object costs two GETs.
    for query in [
        "probe_metric{instance=~\"targ.*\",series=\"s0\"}",
        "probe_metric{instance!=\"obj0\",series=\"s0\"}",
    ] {
        let (value, stats) = run(&engine, th, query, t_ms, now).await;
        assert_eq!(stats.segments_pruned_by_dictionary, 0, "{query}");
        assert_eq!(stats.segments_probed_two_step, N, "{query}");
        assert_eq!(probe_gets(&stats), 2 * N, "{query}");
        let one_step = query
            .replace("probe_metric{", "{__name__=~\"probe_metric\",")
            .replace("series=", "series=~");
        let (one_step_value, _) = run(&engine, th, &one_step, t_ms, now).await;
        assert!(!vector_bits(&value).is_empty(), "{query}");
        assert_eq!(vector_bits(&value), vector_bits(&one_step_value), "{query}");
    }

    // The one-step probe over the same series: regex matchers carry no
    // positive equality, so the gate never starts a two-step probe.
    let (one_step_value, one_step) = run(
        &engine,
        th,
        "{__name__=~\"probe_metric\",instance=~\"target\"}",
        t_ms,
        now,
    )
    .await;
    assert_eq!(one_step.segments_probed_two_step, 0);
    assert_eq!(probe_gets(&one_step), N);
    assert_eq!(
        probe_bytes(&one_step),
        objects.iter().map(|o| o.one_step_bytes).sum::<u64>()
    );
    let bits = vector_bits(&value);
    assert_eq!(bits.len(), 1, "one series: {bits:?}");
    assert_eq!(bits, vector_bits(&one_step_value));
}

/// A selector every dictionary holds: the statement starts two-step, sees
/// none of its first K probes drop an object, and switches to the one-step
/// probe, so its extra GETs stay within twice the fan-out.
#[tokio::test]
async fn fan_out_statement_switches_the_dictionary_probe_off() {
    const N: u64 = 8;
    const FAN_OUT: u64 = 2;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme".to_string());
    let th = tid.hash();
    let hour = 7_010u32;
    publish_fixture(store.as_ref(), &tid, hour, N, 0).await;
    let (t_ms, now) = query_at(hour);
    let engine = engine(store, FAN_OUT as usize);

    let (value, stats) = run(&engine, th, "probe_metric{series=\"s0\"}", t_ms, now).await;
    let (one_step_value, one_step) = run(
        &engine,
        th,
        "{__name__=~\"probe_metric\",series=~\"s0\"}",
        t_ms,
        now,
    )
    .await;
    assert_eq!(probe_gets(&one_step), N);
    assert!(
        probe_gets(&stats) <= probe_gets(&one_step) + 2 * FAN_OUT,
        "extra GETs over the one-step probe are bounded by twice the fan-out: {} against {}",
        probe_gets(&stats),
        probe_gets(&one_step)
    );
    assert!(stats.dictionary_probe_switched_off);
    assert_eq!(stats.segments_pruned_by_dictionary, 0);
    assert_eq!(
        probe_gets(&stats),
        probe_gets(&one_step) + stats.segments_probed_two_step,
        "each two-step probe that holds the value costs exactly one extra GET"
    );
    let bits = vector_bits(&value);
    assert_eq!(bits.len(), N as usize);
    assert_eq!(bits, vector_bits(&one_step_value));
}

/// A statement whose only positive equality is a `__name__` the folded
/// snapshot's postings already applied never pays the two-step probe.
#[tokio::test]
async fn postings_applied_name_only_statement_runs_one_step() {
    // Two objects keep the name-only result under the series limit.
    const N: u64 = 2;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme".to_string());
    let th = tid.hash();
    let hour = 7_020u32;
    let objects = publish_fixture(store.as_ref(), &tid, hour, N, 1).await;
    publish_other_metric(store.as_ref(), &tid, hour).await;

    let now = now_at_seal(hour);
    let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
    let report = catalog
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");
    assert!(
        report.postings_built,
        "postings must build for the name to be applied"
    );
    let engine = QueryEngine::new(
        Arc::new(catalog),
        store,
        EngineConfig {
            promql_fetch_fanout: Some(8),
            ..EngineConfig::default()
        },
    );
    let (t_ms, _) = query_at(hour);

    let (value, stats) = run(&engine, th, METRIC, t_ms, now).await;
    assert_eq!(
        stats.segments_pruned, 1,
        "the postings prune the other_metric object"
    );
    assert_eq!(stats.segments_fetched, N);
    assert_eq!(stats.segments_probed_two_step, 0);
    assert_eq!(stats.segments_pruned_by_dictionary, 0);
    assert!(!stats.dictionary_probe_switched_off);
    assert_eq!(
        probe_gets(&stats),
        N,
        "one-step: one catalog GET per object"
    );
    assert_eq!(
        probe_bytes(&stats),
        objects.iter().map(|o| o.one_step_bytes).sum::<u64>()
    );
    assert_eq!(
        vector_bits(&value).len(),
        N as usize * SERIES_PER_OBJECT,
        "every series of every object"
    );
}

/// The name-only rule is per segment: in a partially folded snapshot the
/// part segments the name postings filtered run the one-step probe, while
/// the Recent segments the postings never saw go through the gate, and
/// those whose dictionary lacks the name are dropped by it.
#[tokio::test]
async fn name_only_statement_gates_the_segments_the_postings_did_not_filter() {
    // Sealed and folded: two objects of the metric and one of another, which
    // the postings prune.
    const SEALED: u64 = 2;
    // Listed after the fold: two objects of the metric and two sparse objects
    // of another metric only, whose dictionaries lack the name.
    const RECENT_HOLD: u64 = 2;
    const RECENT_MISS: u64 = 2;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme".to_string());
    let th = tid.hash();
    let hour = 7_050u32;
    let sealed = publish_fixture(store.as_ref(), &tid, hour, SEALED, 1).await;
    publish_other_metric(store.as_ref(), &tid, hour).await;

    let now = now_at_seal(hour);
    let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
    let report = catalog
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");
    assert!(report.postings_built, "postings must build");

    // The same event times, one ingest hour later: unsealed at `now`, so
    // the snapshot lists them past the folded parts.
    let base_ts = hour_start(hour) + 30 * 60 * NS_PER_SEC;
    let mut recent = Vec::new();
    for object in SEALED..SEALED + RECENT_HOLD + RECENT_MISS {
        let metric = if object < SEALED + RECENT_HOLD {
            METRIC
        } else {
            "unrelated_metric"
        };
        recent.push(
            publish_sparse_named(store.as_ref(), &tid, object, hour + 1, base_ts, false, metric)
                .await,
        );
    }
    let (hold, miss) = recent.split_at(RECENT_HOLD as usize);

    let engine = QueryEngine::new(
        Arc::new(catalog),
        store,
        EngineConfig {
            promql_fetch_fanout: Some(8),
            max_series: 1 << 20,
            ..EngineConfig::default()
        },
    );
    let (t_ms, _) = query_at(hour);
    let (value, stats) = run(&engine, th, METRIC, t_ms, now).await;
    assert_eq!(
        stats.segments_pruned, 1,
        "the postings prune the other_metric part segment"
    );
    assert_eq!(
        stats.segments_fetched,
        SEALED + RECENT_HOLD + RECENT_MISS,
        "the Recent segments are not postings-filtered"
    );
    assert_eq!(
        stats.segments_probed_two_step,
        RECENT_HOLD + RECENT_MISS,
        "every Recent segment, and no part segment, probes two-step"
    );
    assert_eq!(
        stats.segments_pruned_by_dictionary, RECENT_MISS,
        "the Recent segments lacking the name are dropped by the dictionary"
    );
    assert!(!stats.dictionary_probe_switched_off);
    assert_eq!(
        probe_gets(&stats),
        SEALED + 2 * RECENT_HOLD + RECENT_MISS,
        "one-step part segments, two GETs per Recent holder, one per Recent miss"
    );
    let expected_bytes = sealed.iter().map(|o| o.one_step_bytes).sum::<u64>()
        + hold
            .iter()
            .map(|o| o.dict_bytes + o.rest_bytes)
            .sum::<u64>()
        + miss.iter().map(|o| o.dict_bytes).sum::<u64>();
    assert_eq!(
        probe_bytes(&stats),
        expected_bytes,
        "the part segments read all four sections in one GET"
    );
    assert_eq!(
        vector_bits(&value).len(),
        (SEALED + RECENT_HOLD) as usize * SERIES_PER_OBJECT,
        "every series of every object holding the name"
    );
}

/// Each selector plan keeps its own gate: in one statement a selective
/// selector keeps the two-step probe and drops the objects that lack its
/// value, while a selector every dictionary holds switches its own gate off
/// after its first K probes, with at most `2K - 1` of them started.
#[tokio::test]
async fn each_selector_plan_switches_its_own_gate() {
    const N: u64 = 8;
    const H: u64 = 1;
    const FAN_OUT: u64 = 2;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme".to_string());
    let th = tid.hash();
    let hour = 7_060u32;
    publish_fixture(store.as_ref(), &tid, hour, N, H).await;
    let (t_ms, now) = query_at(hour);
    let engine = engine(store, FAN_OUT as usize);

    let (value, stats) = run(
        &engine,
        th,
        "probe_metric{instance=\"target\"} or probe_metric{series=\"s1\"}",
        t_ms,
        now,
    )
    .await;
    assert!(
        stats.dictionary_probe_switched_off,
        "the non-selective plan's gate switched off"
    );
    assert_eq!(
        stats.segments_pruned_by_dictionary,
        N - H,
        "the selective plan's gate stayed on for every object"
    );
    let non_selective_two_step = stats.segments_probed_two_step - N;
    assert!(
        (FAN_OUT..2 * FAN_OUT).contains(&non_selective_two_step),
        "the non-selective plan started between K and 2K - 1 two-step probes: \
         {non_selective_two_step}"
    );
    assert_eq!(
        probe_gets(&stats),
        (N + H) + (N + non_selective_two_step),
        "selective: N dictionary GETs plus H; non-selective: N plus one per two-step probe"
    );

    let (one_step_value, _) = run(
        &engine,
        th,
        "{__name__=~\"probe_metric\",instance=~\"target\"} \
         or {__name__=~\"probe_metric\",series=~\"s1\"}",
        t_ms,
        now,
    )
    .await;
    let bits = vector_bits(&value);
    assert_eq!(bits.len(), (N + H) as usize, "{bits:?}");
    assert_eq!(bits, vector_bits(&one_step_value));
}

/// Publishes a one-series dense object holding only `other_metric` into
/// ingest hour `hour`, for the name postings to prune.
async fn publish_other_metric(store: &dyn ObjectStoreBackend, tid: &TenantId, hour: u32) {
    let th = tid.hash();
    let other = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "other_metric".to_string(),
    }])
    .expect("valid labels");
    let written = SegmentWriter::write(
        vec![SeriesInput {
            series_id: SeriesId::compute(tid, "other_metric", &other).expect("series id"),
            labels: other,
            samples: vec![Sample {
                ts_ns: hour_start(hour) + 30 * 60 * NS_PER_SEC,
                value: 1.0,
            }],
        }],
        SegmentIdentity {
            tenant_hash: th.0,
            shard: 0,
            writer_id: Uuid::from_u128(99).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        },
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write other segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash: th,
        signal: Signal::Metrics,
        shard: 0,
        writer_id: Uuid::from_u128(99),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: 0,
        ingest_hour_bucket: hour,
    })
    .expect("valid commit record");
    let key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// A value no dictionary holds: every object is dropped at step one, with
/// no remaining-range GET, no sample and no error.
#[tokio::test]
async fn value_absent_everywhere_issues_dictionary_gets_only() {
    const N: u64 = 4;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme".to_string());
    let th = tid.hash();
    let hour = 7_030u32;
    let objects = publish_fixture(store.as_ref(), &tid, hour, N, 1).await;
    let (t_ms, now) = query_at(hour);
    let engine = engine(store, 2);

    let (value, stats) = run(&engine, th, "probe_metric{instance=\"nowhere\"}", t_ms, now).await;
    assert!(vector_bits(&value).is_empty());
    assert_eq!(probe_gets(&stats), N, "dictionary GETs only");
    assert_eq!(
        probe_bytes(&stats),
        objects.iter().map(|o| o.dict_bytes).sum::<u64>()
    );
    assert_eq!(stats.segments_pruned_by_dictionary, N);
    assert_eq!(stats.segments_probed_two_step, N);
    assert!(
        !stats.dictionary_probe_switched_off,
        "every probe dropped its object"
    );
    assert_eq!(
        stats.phase_accounting.scan.s3_requests(AccountedOp::Get),
        0,
        "no page GET"
    );
}

/// A corrupt LABEL_DICT read at step one is the typed corrupt-segment error
/// the one-step probe reports for the same object.
#[tokio::test]
async fn corrupt_dictionary_at_step_one_is_the_one_step_typed_error() {
    let memory = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme".to_string());
    let th = tid.hash();
    let hour = 7_040u32;
    let objects = publish_fixture(memory.as_ref(), &tid, hour, 1, 1).await;
    let key = objects[0].data_key.clone();
    let (t_ms, now) = query_at(hour);

    // The object's first GET is the footer tail; the second is the probe's
    // first catalog GET: LABEL_DICT alone under the two-step probe, all four
    // sections under the one-step probe. Returns the error and the length of
    // the faulted range.
    let faulted = |query: &'static str| {
        let memory = memory.clone();
        let key = key.clone();
        async move {
            let store = Arc::new(FaultStore::new(
                memory,
                FaultPlan::empty().with_rule(
                    Rule::new(Op::Get, ScriptedFault::CorruptRange)
                        .with_key_contains(key.clone())
                        .with_occurrence(Occurrence::Nth(2)),
                ),
            ));
            let recording = Arc::new(RangeRecordingStore {
                inner: store.clone(),
                gets: Mutex::new(Vec::new()),
            });
            let backend: Arc<dyn ObjectStoreBackend> = recording.clone();
            let err = engine(backend, 2)
                .instant_with_stats(th, query, t_ms, &[], now, Duration::from_secs(30))
                .await
                .expect_err("a corrupt catalog must fail the query");
            assert_eq!(
                store.fault_count(Op::Get, FaultKind::CorruptRange),
                1,
                "the fault must fire exactly once"
            );
            let ranges: Vec<GetRange> = recording
                .gets
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| k.contains(&key))
                .map(|(_, range)| range.clone())
                .collect();
            let faulted_len = match ranges.get(1) {
                Some(GetRange::Range(start, end)) => end - start,
                other => panic!("the faulted GET must be a bounded range: {other:?}"),
            };
            (err, faulted_len)
        }
    };
    let (two_step, two_step_len) = faulted("probe_metric{instance=\"target\"}").await;
    let (one_step, one_step_len) =
        faulted("{__name__=~\"probe_metric\",instance=~\"target\"}").await;
    assert_eq!(
        two_step_len, objects[0].dict_bytes,
        "the two-step probe's faulted GET is LABEL_DICT's stored length"
    );
    assert_eq!(one_step_len, objects[0].one_step_bytes);
    assert!(
        matches!(two_step, QueryError::Fetch(_)),
        "a typed fetch error: {two_step:?}"
    );
    assert_eq!(format!("{two_step:?}"), format!("{one_step:?}"));
}

/// Records each GET's key and range, then forwards it.
struct RangeRecordingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    gets: Mutex<Vec<(String, GetRange)>>,
}

#[async_trait]
impl ObjectStoreBackend for RangeRecordingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets
            .lock()
            .unwrap()
            .push((key.to_string(), range.clone()));
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// The three figures reach `data.stats` on ravel-query's HTTP query path.
#[tokio::test]
async fn http_query_stats_carry_the_dictionary_probe_figures() {
    const N: u64 = 4;
    const H: u64 = 1;
    let memory = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme".to_string());
    // The HTTP path reads the wall clock, so the samples sit two minutes
    // before it, in the current, unsealed ingest hour.
    let wall = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("nanoseconds fit i64");
    let hour = u32::try_from(wall / NS_PER_HOUR).expect("hour bucket");
    let base_ts = wall - 2 * 60 * NS_PER_SEC;
    for object in 0..N {
        publish_sparse(memory.as_ref(), &tid, object, hour, base_ts, object < H).await;
    }
    let last_s = (base_ts + (SAMPLES_PER_SERIES as i64 - 1) * NS_PER_SEC) as f64 / 1e9;

    let backend: Arc<dyn ObjectStoreBackend> = memory;
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(
        catalog,
        backend,
        EngineConfig {
            promql_fetch_fanout: Some(8),
            ..EngineConfig::default()
        },
    ));
    let mut tokens = HashMap::new();
    tokens.insert("tok".to_string(), tid.clone());
    let app = router(AppState::new(
        engine,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
    ));

    let query = "probe_metric%7Binstance%3D%22target%22%7D";
    let uri = format!("/api/v1/query?query={query}&time={last_s}");
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", "Bearer tok")
        .body(Body::empty())
        .expect("build request");
    let response = app.oneshot(request).await.expect("oneshot is infallible");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("parse response json");
    assert_eq!(status, StatusCode::OK, "{json}");
    let stats = &json["data"]["stats"];
    assert_eq!(stats["segmentsPrunedByDictionary"], N - H, "{stats}");
    assert_eq!(stats["segmentsProbedTwoStep"], N, "{stats}");
    assert_eq!(stats["dictionaryProbeSwitchedOff"], false, "{stats}");
    assert_eq!(stats["segmentsPruned"], 0, "{stats}");
}
