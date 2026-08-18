//! Shared helpers for the correctness and failure-point suite
//! (docs/consistency-model.md, docs/catalog-and-mvcc.md). Not part of any
//! crate's public API; used only by integration tests under `tests/`.
#![allow(clippy::expect_used, dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::NormalizedPoint;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

/// Clock whose reading is set explicitly by the test, so flush identity
/// (ingest_hour_bucket, created_unix_ns) is deterministic.
pub struct TestClock(AtomicI64);

impl TestClock {
    pub fn new(start_ns: i64) -> Arc<Self> {
        Arc::new(TestClock(AtomicI64::new(start_ns)))
    }

    pub fn set_ns(&self, ns: i64) {
        self.0.store(ns, Ordering::SeqCst);
    }

    pub fn advance_ns(&self, delta_ns: i64) {
        self.0.fetch_add(delta_ns, Ordering::SeqCst);
    }

    pub fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Clock for TestClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

pub const NS_PER_SEC: i64 = 1_000_000_000;
pub const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;

pub fn tenant(id: &str) -> TenantId {
    TenantId::new(id)
}

pub fn build_labels(pairs: &[(&str, &str)]) -> LabelSet {
    let labels = pairs
        .iter()
        .map(|(name, value)| Label {
            name: (*name).to_string(),
            value: (*value).to_string(),
        })
        .collect();
    LabelSet::new(labels).expect("valid labels")
}

/// Builds one `NormalizedPoint` as if it had come out of `ravel_otlp`, for
/// tests that drive `IngestRouter` directly without going through OTLP
/// normalization.
pub fn make_point(
    tenant: &TenantId,
    metric: &str,
    extra_labels: &[(&str, &str)],
    ts_ns: i64,
    value: f64,
) -> NormalizedPoint {
    let mut pairs = vec![(METRIC_NAME_LABEL, metric)];
    pairs.extend_from_slice(extra_labels);
    let labels = build_labels(&pairs);
    let series_id = SeriesId::compute(tenant, metric, &labels).expect("series id");
    NormalizedPoint {
        series_id,
        labels,
        sample: Sample { ts_ns, value },
        is_monotonic_sum: false,
    }
}

/// Builds one `SeriesInput` for hand-assembled RSEG segments (tests that
/// bypass `IngestRouter` to control commit identity directly).
pub fn series_input(
    tenant_id: &TenantId,
    metric: &str,
    extra: &[(&str, &str)],
    samples: &[(i64, f64)],
) -> SeriesInput {
    let label_set = build_labels(&{
        let mut pairs = vec![(METRIC_NAME_LABEL, metric)];
        pairs.extend_from_slice(extra);
        pairs
    });
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    SeriesInput {
        series_id,
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }
}

/// Writes a real RSEG segment and publishes its commit record directly
/// (bypassing `IngestRouter`), for tests that need to pin exact commit
/// identity (writer_seq, ingest_hour_bucket, created_unix_ns) to exercise
/// the cross-segment dedup total order or catalog listing edges.
#[allow(clippy::too_many_arguments)]
pub async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    shard: u32,
    writer_id: Uuid,
    writer_epoch: u64,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
    series: Vec<SeriesInput>,
) -> (CommitToken, String) {
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written: WrittenSegment =
        SegmentWriter::write(series, identity, bounds).expect("write segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    let token = publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    (token, data_key)
}

pub fn catalog(store: Arc<dyn ObjectStoreBackend>, shard_count: u32) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog")
}

pub fn engine(store: Arc<dyn ObjectStoreBackend>, catalog: Catalog) -> QueryEngine {
    QueryEngine::new(Arc::new(catalog), store, EngineConfig::default())
}

/// Unwraps an instant-query result this suite expects to be a plain
/// instant vector (every query here is a bare selector, never a
/// scalar/string literal), panicking with the actual variant otherwise.
///
/// Takes the `(Value, Coverage)` pair `QueryEngine::instant` returns
/// (ADR-0071 amendment decision 4) and drops the coverage here, in one
/// place, rather than at each of this suite's call sites: every query in
/// this suite is single-cluster, so its coverage is always
/// `Coverage::Complete`.
pub fn expect_vector(
    (value, _coverage): (ravel_promql::Value, ravel_query::Coverage),
) -> ravel_promql::InstantVector {
    match value {
        ravel_promql::Value::Vector(v) => v,
        other => panic!("expected instant vector, got {other:?}"),
    }
}

/// Unwraps a range-query result this suite expects to be a plain range
/// matrix, panicking with the actual variant otherwise. Drops the coverage
/// for the same reason [`expect_vector`] does.
pub fn expect_matrix(
    (value, _coverage): (ravel_promql::Value, ravel_query::Coverage),
) -> ravel_promql::RangeMatrix {
    match value {
        ravel_promql::Value::Matrix(m) => m,
        other => panic!("expected range matrix, got {other:?}"),
    }
}

/// Grace window + max flush lifetime for orphan GC, in milliseconds
/// (docs/consistency-model.md "Deletion and GC"): defaults 24h + 1h.
#[derive(Debug, Clone, Copy)]
pub struct GcWindow {
    pub grace_ms: i64,
    pub max_flush_lifetime_ms: i64,
}

impl Default for GcWindow {
    fn default() -> Self {
        GcWindow {
            grace_ms: 24 * 3_600_000,
            max_flush_lifetime_ms: 3_600_000,
        }
    }
}

/// SPECIFICATION MODEL, NOT PRODUCTION CODE. This is an executable
/// restatement of the orphan-GC rule from docs/consistency-model.md
/// "Deletion and GC" (grace + max_flush_lifetime horizon, re-verify
/// commit-record absence immediately before each delete) and ADR-0010 SS11.
/// It exists only to document the intended behavior; no shipped GC path
/// calls it. A test driving this helper proves a property of the model, NOT
/// of Ravel: the assertion
/// and the code under assertion are the same logic. When the production GC
/// lands, delete this helper and point the crash-row assertions at the real
/// symbol.
///
/// Sweeps L0 data objects for one (tenant, signal, shard) that have no commit
/// record, deleting only those older than `grace + max_flush_lifetime` and
/// re-verifying commit-record absence immediately before each delete. Relies
/// on `MemoryStore`'s strongly consistent listing, exactly as the real GC
/// path is documented to.
///
/// Returns the keys of every data object actually deleted.
pub async fn spec_model_sweep_orphans(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    signal: Signal,
    shard: u32,
    now_ms: i64,
    window: GcWindow,
) -> Vec<String> {
    let l0_prefix = format!(
        "t/{}/{}/l0/{:04}/",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        shard
    );
    let objects = list_all(store, &l0_prefix).await.expect("list l0 objects");
    let commit_prefix = keys::commit_shard_prefix(&tenant_hash, signal, shard).expect("prefix");

    let mut deleted = Vec::new();
    for obj in objects {
        let age_ms = now_ms - obj.last_modified_unix_ms;
        if age_ms <= window.grace_ms + window.max_flush_lifetime_ms {
            continue;
        }
        let parsed = match keys::parse_data_key(&obj.key) {
            Ok(parsed) => parsed,
            Err(_) => continue,
        };

        // Re-verify commit-record absence immediately before deleting: list
        // fresh, right here, rather than reusing a listing taken earlier in
        // the sweep.
        let commits = list_all(store, &commit_prefix)
            .await
            .expect("list commit records");
        let has_commit = commits.iter().any(|c| {
            keys::parse_commit_key(&c.key)
                .map(|pc| {
                    pc.writer_id == parsed.writer_id
                        && pc.epoch == parsed.epoch
                        && pc.seq == parsed.seq
                })
                .unwrap_or(false)
        });
        if has_commit {
            continue;
        }

        store.delete(&obj.key).await.expect("delete orphan");
        deleted.push(obj.key);
    }
    deleted
}
