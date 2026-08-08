//! EF-4/#724 (ADR-0061 decision 3): postings pruning extended to
//! literal-prefix-anchored `__name__` regex (`^foo.*$`) at query time, via the
//! catalog's sorted-name range scan.
//!
//! The load-bearing test is the correctness oracle: for a spread of prefix
//! shapes it runs the SAME selection two ways -- once through the new pruned
//! path (`^prefix.*$`, which the detector accepts), and once through a
//! semantically identical regex that the detector deliberately rejects
//! (`^prefix.*.*$`, an extra `.*` in the literal portion), so pruning is
//! forcibly disabled. This mirrors the precedent in `postings_pruning.rs`,
//! where the equality-pruning oracle compares an equality query against a
//! bypassing regex. The two runs must return bit-identical results: pruning
//! may only ever change which segments are *fetched*, never which rows come
//! back.
//!
//! Every test folds real RSEG segments first (`now_at_seal` puts the ingest
//! hour at or below the watermark) so pruning is actually reachable.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine, QueryStats};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V6};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn tenant(id: &str) -> TenantId {
    TenantId::new(id.to_string())
}

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn series_input(tenant_id: &TenantId, metric: &str, ts_ns: i64, value: f64) -> SeriesInput {
    let label_set = labels_for(metric);
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample { ts_ns, value }],
    }
}

/// One real RSEG v6 segment carrying `metric`'s single sample, published with
/// its commit record. One metric per segment, so pruning shows directly in the
/// fetched-segment counter.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    input: SeriesInput,
) {
    let writer_id = Uuid::new_v4();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(vec![input], identity, bounds).expect("write v6");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V6),
        created_unix_ns: 0,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Sorted `(metric_name, value_bits)` pairs of an instant vector, for
/// order-independent, exact (bit-level) comparisons.
fn vector_bits(value: &Value) -> Vec<(String, u64)> {
    match value {
        Value::Vector(v) => {
            let mut out: Vec<(String, u64)> = v
                .iter()
                .map(|s| {
                    let name = s.labels.get(METRIC_NAME_LABEL).unwrap_or("").to_string();
                    (name, s.value.to_bits())
                })
                .collect();
            out.sort();
            out
        }
        other => panic!("expected instant vector, got {other:?}"),
    }
}

/// The six metrics, one per segment, in ascending sort order. "app" is itself a
/// prefix of "apple" (the aliasing case). Values are the index, so every row is
/// distinguishable bit-for-bit.
const METRICS: [&str; 6] = ["alpha", "apex", "app", "apple", "banana", "zebra"];

/// Fold a snapshot carrying [`METRICS`], one metric per segment. Returns the
/// engine and the query instant (ms).
async fn folded_engine(tid: &TenantId, hour: u32) -> (QueryEngine, i64, i64) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let th = tid.hash();
    let now = now_at_seal(hour);
    let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;

    for (i, metric) in METRICS.iter().enumerate() {
        publish_segment(
            store.as_ref(),
            th,
            (i + 1) as u64,
            hour,
            series_input(tid, metric, ts, i as f64),
        )
        .await;
    }

    let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
    let report = catalog
        .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[])
        .await
        .expect("fold");
    assert!(
        report.postings_built,
        "the fold must attach a postings object for prefix pruning to be reachable"
    );

    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    (engine, ts / 1_000_000, now)
}

async fn run(
    engine: &QueryEngine,
    tid: &TenantId,
    query: &str,
    t_ms: i64,
    now: i64,
) -> (Vec<(String, u64)>, QueryStats) {
    let (value, stats) = engine
        .instant_with_stats(tid.hash(), query, t_ms, &[], now, Duration::from_secs(5))
        .await
        .unwrap_or_else(|e| panic!("query {query:?} failed: {e:?}"));
    (vector_bits(&value), stats)
}

/// The correctness oracle. For each prefix shape, the pruned query
/// (`^prefix.*$`) and its unpruned twin (`^prefix.*.*$`, which the detector
/// rejects and which selects the identical set) must return bit-identical
/// results, and the pruned run must fetch strictly fewer segments than the
/// unpruned run whenever any segment is actually eliminated.
#[tokio::test]
async fn prefix_pruned_matches_unpruned_across_boundary_cases() {
    let tid = tenant("acme");
    let (engine, t_ms, now) = folded_engine(&tid, 5_100).await;

    // (prefix, expected fetched-segment count under pruning).
    let cases = [
        ("qux", 0usize), // matches zero names -> prune everything
        ("alph", 1),     // exactly the first name in sort order
        ("zeb", 1),      // exactly the last name in sort order
        ("ap", 3),       // multiple distinct names (apex, app, apple)
        ("app", 2),      // one name (app) that is a prefix of another (apple)
        ("banana", 1),   // a whole-name prefix that aliases nothing
    ];

    for (prefix, expected_fetched) in cases {
        let pruned_q = format!("{{__name__=~\"^{prefix}.*$\"}}");
        // The extra `.*` keeps the same match set but forces the detector to
        // reject it (metacharacters remain in the literal portion), so pruning
        // is disabled -- the "run it unpruned" half of the oracle.
        let unpruned_q = format!("{{__name__=~\"^{prefix}.*.*$\"}}");

        let (pruned_rows, pruned_stats) = run(&engine, &tid, &pruned_q, t_ms, now).await;
        let (unpruned_rows, unpruned_stats) = run(&engine, &tid, &unpruned_q, t_ms, now).await;

        assert_eq!(
            pruned_rows, unpruned_rows,
            "prefix {prefix:?}: pruned and unpruned results must be bit-identical"
        );
        assert_eq!(
            pruned_stats.segments_fetched, expected_fetched as u64,
            "prefix {prefix:?}: pruned fetch count"
        );
        assert_eq!(
            pruned_stats.segments_pruned,
            (METRICS.len() - expected_fetched) as u64,
            "prefix {prefix:?}: pruned-away count"
        );
        // The unpruned twin genuinely bypasses pruning (proves the detector
        // rejected `^prefix.*.*$`), so it fetches every segment.
        assert_eq!(
            unpruned_stats.segments_fetched,
            METRICS.len() as u64,
            "prefix {prefix:?}: the unpruned twin must fetch every segment"
        );
        assert_eq!(unpruned_stats.segments_pruned, 0);
    }
}

/// Effectiveness, stated as its own assertion (mirrors
/// `sql_postings_pruning_278.rs`): a prefix query resolves strictly fewer
/// segments than the same selection unpruned.
#[tokio::test]
async fn prefix_scan_narrows_the_resolve() {
    let tid = tenant("acme");
    let (engine, t_ms, now) = folded_engine(&tid, 5_101).await;

    let (_rows, pruned) = run(&engine, &tid, "{__name__=~\"^ap.*$\"}", t_ms, now).await;
    let (_rows, unpruned) = run(&engine, &tid, "{__name__=~\"^ap.*.*$\"}", t_ms, now).await;

    assert_eq!(pruned.segments_fetched, 3, "apex|app|apple");
    assert_eq!(
        unpruned.segments_fetched, 6,
        "unpruned resolves all segments"
    );
    assert!(
        pruned.segments_fetched < unpruned.segments_fetched,
        "the prefix range scan must narrow the resolve"
    );
}

/// A non-prefix regex on `__name__` -- an infix wildcard or an alternation --
/// must still take the unpruned path. This proves the detector is not overly
/// permissive: a regression that classified these as prefixes would silently
/// reintroduce the false-negative risk. The query still returns the correct
/// rows, proving the fallback path genuinely executes rather than erroring.
#[tokio::test]
async fn non_prefix_regex_still_bypasses_pruning() {
    let tid = tenant("acme");
    let (engine, t_ms, now) = folded_engine(&tid, 5_102).await;

    // Infix wildcard: matches names *containing* "ap" (apex, app, apple).
    let (infix_rows, infix_stats) = run(&engine, &tid, "{__name__=~\".*ap.*\"}", t_ms, now).await;
    assert_eq!(
        infix_stats.segments_pruned, 0,
        "an infix-wildcard regex must bypass pruning entirely"
    );
    assert_eq!(
        infix_stats.segments_fetched, 6,
        "bypass means every segment is fetched"
    );
    assert_eq!(
        infix_rows,
        vec![
            ("apex".to_string(), 1.0f64.to_bits()),
            ("app".to_string(), 2.0f64.to_bits()),
            ("apple".to_string(), 3.0f64.to_bits()),
        ],
        "the residual regex filter still selects the right series"
    );

    // Alternation: matches exactly "app" or "apple".
    let (alt_rows, alt_stats) = run(&engine, &tid, "{__name__=~\"app|apple\"}", t_ms, now).await;
    assert_eq!(
        alt_stats.segments_pruned, 0,
        "an alternation regex must bypass pruning entirely"
    );
    assert_eq!(alt_stats.segments_fetched, 6);
    assert_eq!(
        alt_rows,
        vec![
            ("app".to_string(), 2.0f64.to_bits()),
            ("apple".to_string(), 3.0f64.to_bits()),
        ]
    );
}
