//! Acceptance: generate a subject, mark it erased with a durable
//! `.dreq`, and assert it is absent from EVERY query surface `ravel-query`
//! exposes -- and, crucially, present through none of them -- while a
//! non-erased subject still appears, so a filter-everything bug fails the test
//! too.
//!
//! Each assertion drives a real public entry point, never a hand-called filter:
//!
//! - metrics instant (`QueryEngine::instant`, scalar vector),
//! - metrics range (`QueryEngine::range`, matrix -- also the exact materialized
//!   view the `/api/v1/analytics` surface consumes, since that surface runs
//!   `QueryEngine::range_with_stats` and hands its matrix to `ravel-analytics`;
//!   see the T2 report),
//! - metrics native-histogram instant (its own decode/`retain_histogram_series`
//!   call site),
//! - metrics metadata (`QueryEngine::resolve_series`, the `fetch_all_series`
//!   funnel),
//! - logs (`LogSegmentFetcher::fetch`, the `retain_log_records` call site),
//! - the ADR-0071 DISTRIBUTED fan-out (`QueryEngine::instant` with a
//!   `Distributed` context over a slice worker that applies the coordinator's
//!   erasure predicates), and
//! - the ADR-0071 FEDERATED fan-out (`QueryEngine::instant` with a `Federation`
//!   context over a remote that enforces its own erasure, the way a real remote
//!   cluster does -- the coordinator sends federation remotes no predicates).
//!
//! Every surface is also driven a second time with erasure disabled on that one
//! surface (no `.dreq`, or a non-compliant worker/remote) and asserted to leak
//! the subject: that is the "the test fails if erasure is disabled on any one
//! surface" flip, made explicit rather than left implicit.
//!
//! Surfaces NOT covered here live outside `ravel-query` and are reported, not
//! reached into (task scope): `query_exemplars`
//! (`services/ravel-server/src/exemplars.rs` -- still leaks; the fix
//! is a `ravel-server` change), the analytics endpoint
//! (`services/ravel-server/src/analytics.rs`, a MATCH: it fetches through
//! `QueryEngine::range` above), and the spans/alerts/audit SQL surfaces
//! (`ravel-sql`, selective-erasure filtered).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record, signal};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::{LabelMatcher, Value};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_proto::queryfrag::v1 as pb;
use ravel_query::distrib::client::{DistribError, SliceFetcher, SliceResponse};
use ravel_query::distrib::codec;
use ravel_query::distrib::partition::DistribThresholds;
use ravel_query::distrib::{Distributed, Federation, RemoteCluster};
use ravel_query::erasure::ErasurePredicate;
use ravel_query::{
    EngineConfig, FetchStats, FetchedSeriesSoa, LogQuery, LogSegmentFetcher, QueryEngine,
};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds, ResetHint,
    SegmentIdentity, SegmentWriter, SeriesInput, SeriesInputV3, SeriesValues,
};
use ravel_types::accounting::QueryAccountingSnapshot;
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash,
    TenantId, TimeRange,
};
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
const TS_A: i64 = 1_000 * NS;
const TS_B: i64 = 2_000 * NS;
const METRIC: &str = "m";
/// The erased subject and the surviving subject, used identically on every
/// surface so the "one subject, every surface" property is literal.
const ERASED: &str = "u1";
const KEPT: &str = "u2";

fn tenant() -> (TenantId, TenantHash) {
    let id = TenantId::new("tenant-a".to_string());
    let hash = id.hash();
    (id, hash)
}

fn label_set(user_id: &str) -> LabelSet {
    LabelSet::new(vec![
        Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: METRIC.to_string(),
        },
        Label {
            name: "user_id".to_string(),
            value: user_id.to_string(),
        },
    ])
    .expect("valid labels")
}

// ---------------------------------------------------------------------------
// Metric + histogram + dreq publishing (mirrors tests/erasure_e2e.rs, the T1
// audit fixtures, so the two surfaces filter the same real objects).
// ---------------------------------------------------------------------------

async fn publish_segment(
    store: &MemoryStore,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
    samples: &[i64],
) -> CommitToken {
    let writer_id = Uuid::from_u128(7);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 0,
    };
    let inputs: Vec<SeriesInput> = [ERASED, KEPT]
        .iter()
        .map(|uid| {
            let labels = label_set(uid);
            SeriesInput {
                series_id: SeriesId::compute(tenant_id, METRIC, &labels).expect("series id"),
                labels,
                samples: samples
                    .iter()
                    .map(|ts| Sample {
                        ts_ns: *ts,
                        value: 1.0,
                    })
                    .collect(),
            }
        })
        .collect();
    let written = SegmentWriter::write(
        inputs,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");
    publish_written(store, tenant_hash, writer_id, written).await
}

async fn publish_histogram_segment(
    store: &MemoryStore,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
) -> CommitToken {
    let writer_id = Uuid::from_u128(9);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 0,
    };
    let one_bucket_histogram = || HistogramValue {
        scale: 0,
        zero_threshold: 0.0,
        sum: Some(1.0),
        custom_values: None,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 1,
        }],
        negative_spans: Vec::new(),
        counts: HistogramCounts::Int {
            zero_count: 0,
            count: 1,
            positive: vec![1],
            negative: Vec::new(),
        },
        reset_hint: ResetHint::Unknown,
    };
    let inputs: Vec<SeriesInputV3> = [ERASED, KEPT]
        .iter()
        .map(|uid| {
            let labels = label_set(uid);
            SeriesInputV3 {
                series_id: SeriesId::compute(tenant_id, METRIC, &labels).expect("series id"),
                labels,
                values: SeriesValues::Histogram(vec![HistogramSample {
                    ts_ns: TS_A,
                    value: one_bucket_histogram(),
                }]),
            }
        })
        .collect();
    let written = SegmentWriter::write_histograms(
        inputs,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write histogram segment");
    publish_written(store, tenant_hash, writer_id, written).await
}

/// Builds a commit record for a written segment, puts the data object, and
/// publishes it, returning the read-your-write token.
async fn publish_written(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    writer_id: Uuid,
    written: ravel_segment::WrittenSegment,
) -> CommitToken {
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 0,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 42,
        ingest_hour_bucket: 0,
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish")
}

/// Writes a durable windowless `.dreq` erasing `user_id = ERASED` under the
/// tenant's metrics `del/` prefix, exactly as the erasure submit path does.
async fn put_dreq(store: &MemoryStore, tenant_hash: TenantHash) {
    let request_id = Uuid::new_v4();
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 1,
        predicate: vec![ErasurePredicateMatcher {
            key: "user_id".to_string(),
            value: ERASED.to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    let key =
        keys::erasure_request_key(&tenant_hash, Signal::Metrics, request_id).expect("dreq key");
    store
        .put(
            &key,
            ravel_commit::erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

fn engine_over(store: &Arc<MemoryStore>) -> QueryEngine {
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    QueryEngine::new(catalog, backend, EngineConfig::default())
}

// ---------------------------------------------------------------------------
// Surface drivers: each returns the sorted `user_id`s a real entry point yields.
// ---------------------------------------------------------------------------

async fn instant_user_ids(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    tokens: &[CommitToken],
    ts_ns: i64,
) -> Vec<String> {
    let (value, _coverage) = engine
        .instant(
            tenant_hash,
            METRIC,
            ts_ns / 1_000_000,
            tokens,
            TS_B + NS,
            Duration::from_secs(30),
        )
        .await
        .expect("instant query");
    let Value::Vector(vector) = value else {
        panic!("instant query over a selector must return a vector");
    };
    let mut ids: Vec<String> = vector
        .iter()
        .map(|s| {
            s.labels
                .get("user_id")
                .expect("series carries user_id")
                .to_string()
        })
        .collect();
    ids.sort();
    ids
}

async fn range_user_ids(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    token: &CommitToken,
) -> Vec<String> {
    let (value, _coverage) = engine
        .range(
            tenant_hash,
            METRIC,
            TS_A / 1_000_000,
            TS_B / 1_000_000,
            1_000_000,
            std::slice::from_ref(token),
            TS_B + NS,
            Duration::from_secs(30),
        )
        .await
        .expect("range query");
    let Value::Matrix(matrix) = value else {
        panic!("range query over a selector must return a matrix");
    };
    let mut ids: Vec<String> = matrix
        .iter()
        .map(|(labels, _samples)| {
            labels
                .get("user_id")
                .expect("series carries user_id")
                .to_string()
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

async fn enumerated_user_ids(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    token: &CommitToken,
) -> Vec<String> {
    let (series, _coverage) = engine
        .resolve_series(
            tenant_hash,
            &[LabelMatcher::equal(METRIC_NAME_LABEL, METRIC)],
            TimeRange {
                start_ns: 0,
                end_ns: TS_B + NS,
            },
            std::slice::from_ref(token),
            TS_B + NS,
            Duration::from_secs(30),
        )
        .await
        .expect("resolve series");
    let mut ids: Vec<String> = series
        .iter()
        .map(|(_, labels)| {
            labels
                .get("user_id")
                .expect("series carries user_id")
                .to_string()
        })
        .collect();
    ids.sort();
    ids
}

fn kept_only() -> Vec<String> {
    vec![KEPT.to_string()]
}

fn both_subjects() -> Vec<String> {
    vec![ERASED.to_string(), KEPT.to_string()]
}

// ===========================================================================
// Local metric surfaces
// ===========================================================================

#[tokio::test]
async fn instant_surface_excludes_erased_keeps_other() {
    let (tenant_id, tenant_hash) = tenant();
    let store = Arc::new(MemoryStore::new());
    let token = publish_segment(&store, &tenant_id, tenant_hash, &[TS_A]).await;
    let engine = engine_over(&store);

    // Flip: with no `.dreq`, the erased subject is present -- so a green result
    // below cannot be a filter-everything artifact.
    assert_eq!(
        instant_user_ids(&engine, tenant_hash, std::slice::from_ref(&token), TS_A).await,
        both_subjects(),
        "before erasure the instant surface returns both subjects",
    );

    put_dreq(&store, tenant_hash).await;
    assert_eq!(
        instant_user_ids(&engine, tenant_hash, std::slice::from_ref(&token), TS_A).await,
        kept_only(),
        "instant surface must exclude the erased subject and keep the other",
    );
}

#[tokio::test]
async fn range_surface_excludes_erased_keeps_other() {
    let (tenant_id, tenant_hash) = tenant();
    let store = Arc::new(MemoryStore::new());
    let token = publish_segment(&store, &tenant_id, tenant_hash, &[TS_A, TS_B]).await;
    let engine = engine_over(&store);

    assert_eq!(
        range_user_ids(&engine, tenant_hash, &token).await,
        both_subjects(),
        "before erasure the range surface returns both subjects",
    );

    put_dreq(&store, tenant_hash).await;
    // This matrix is also the exact materialized view the `/api/v1/analytics`
    // surface consumes (it runs `range_with_stats`), so the analytics surface
    // inherits this exclusion. See the T2 report.
    assert_eq!(
        range_user_ids(&engine, tenant_hash, &token).await,
        kept_only(),
        "range surface (and the analytics view over it) excludes the erased subject",
    );
}

#[tokio::test]
async fn histogram_surface_excludes_erased_keeps_other() {
    let (tenant_id, tenant_hash) = tenant();
    let store = Arc::new(MemoryStore::new());
    let token = publish_histogram_segment(&store, &tenant_id, tenant_hash).await;
    let engine = engine_over(&store);

    assert_eq!(
        instant_user_ids(&engine, tenant_hash, std::slice::from_ref(&token), TS_A).await,
        both_subjects(),
        "before erasure both native-histogram series are returned",
    );

    put_dreq(&store, tenant_hash).await;
    assert_eq!(
        instant_user_ids(&engine, tenant_hash, std::slice::from_ref(&token), TS_A).await,
        kept_only(),
        "histogram surface must exclude the erased subject and keep the other",
    );
}

#[tokio::test]
async fn metadata_surface_excludes_erased_keeps_other() {
    let (tenant_id, tenant_hash) = tenant();
    let store = Arc::new(MemoryStore::new());
    let token = publish_segment(&store, &tenant_id, tenant_hash, &[TS_A]).await;
    let engine = engine_over(&store);

    assert_eq!(
        enumerated_user_ids(&engine, tenant_hash, &token).await,
        both_subjects(),
        "before erasure series enumeration returns both subjects",
    );

    put_dreq(&store, tenant_hash).await;
    assert_eq!(
        enumerated_user_ids(&engine, tenant_hash, &token).await,
        kept_only(),
        "metadata/series-enumeration surface must exclude the erased subject",
    );
}

// ===========================================================================
// Logs surface (LogSegmentFetcher::fetch, the retain_log_records call site)
// ===========================================================================

fn log_identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [1u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// One log record on a fixed stream carrying a per-record `user_id` attribute.
fn log_record(ts: i64, user_id: &str) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "ok".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("user_id".to_string(), AttrValue::Str(user_id.to_string()))],
    }
}

async fn write_log_object(store: &MemoryStore, key: &str, records: &[LogRecord]) -> SegmentRef {
    let mut w = RlogWriter::new(RlogConfig::default(), log_identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// The sorted `user_id`s the log fetch surface yields, given a set of erasure
/// predicates threaded onto the query exactly as the resolved snapshot's
/// pending erasure would be.
async fn log_user_ids(
    fetcher: &LogSegmentFetcher,
    seg_ref: &SegmentRef,
    erasure: Vec<ErasurePredicate>,
) -> Vec<String> {
    let out = fetcher
        .fetch(seg_ref, &LogQuery::new(0, TS_B + NS).with_erasure(erasure))
        .await
        .expect("log fetch")
        .expect("segment in range");
    let mut ids: Vec<String> = out
        .records
        .iter()
        .filter_map(|r| {
            r.attrs.iter().find_map(|(k, v)| match v {
                AttrValue::Str(s) if k == "user_id" => Some(s.clone()),
                _ => None,
            })
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

#[tokio::test]
async fn logs_surface_excludes_erased_keeps_other() {
    let store = MemoryStore::new();
    let records = vec![log_record(TS_A, ERASED), log_record(TS_A + 1, KEPT)];
    let seg_ref = write_log_object(&store, "logs/erasure.rlog", &records).await;
    let fetcher = LogSegmentFetcher::new(Arc::new(store) as Arc<dyn ObjectStoreBackend>);

    // Flip: no predicate -> both subjects present.
    assert_eq!(
        log_user_ids(&fetcher, &seg_ref, vec![]).await,
        both_subjects(),
        "before erasure the log surface returns both subjects",
    );

    assert_eq!(
        log_user_ids(
            &fetcher,
            &seg_ref,
            vec![ErasurePredicate::windowless(vec![(
                "user_id".to_string(),
                ERASED.to_string(),
            )])],
        )
        .await,
        kept_only(),
        "log surface must exclude the erased subject's rows and keep the other",
    );
}

// ===========================================================================
// ADR-0071 DISTRIBUTED and FEDERATED fan-out paths
// ===========================================================================

/// A pre-built two-series scalar run (`ERASED` and `KEPT`), each with one
/// sample at `TS_A`, in the SoA shape a slice worker decodes and streams.
fn worker_runs(tenant_id: &TenantId) -> Vec<FetchedSeriesSoa> {
    [ERASED, KEPT]
        .iter()
        .map(|uid| {
            let labels = label_set(uid);
            FetchedSeriesSoa {
                series_id: SeriesId::compute(tenant_id, METRIC, &labels).expect("series id"),
                labels,
                timestamps: vec![TS_A],
                values: vec![1.0],
                created_unix_ns: 0,
                writer_epoch: 1,
                writer_seq: 0,
            }
        })
        .collect()
}

fn ok_response(scalar: Vec<FetchedSeriesSoa>) -> SliceResponse {
    let series_returned = scalar.len() as u64;
    let samples_returned = scalar.iter().map(|s| s.timestamps.len() as u64).sum();
    SliceResponse {
        scalar,
        accounting: QueryAccountingSnapshot::default(),
        stats: FetchStats::default(),
        series_returned,
        samples_returned,
        status: pb::status::Code::Ok,
        status_message: String::new(),
    }
}

/// An intra-cluster slice worker (ADR-0071): it applies the coordinator's
/// erasure predicates post-decode, exactly as `distrib::service` does, before
/// streaming its runs back. `apply_request_erasure = false` models the
/// regression this suite guards against (a worker that drops that pass).
struct SliceWorker {
    runs: Vec<FetchedSeriesSoa>,
    apply_request_erasure: bool,
}

#[async_trait]
impl SliceFetcher for SliceWorker {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let mut scalar = self.runs.clone();
        if self.apply_request_erasure {
            let predicates = codec::decode_erasure(request.erasure);
            ravel_query::erasure::retain_series_soa(&mut scalar, &predicates);
        }
        Ok(ok_response(scalar))
    }
}

/// A remote cluster (ADR-0071 federation): the coordinator sends it no erasure
/// predicates (they are cluster-local), so a compliant remote enforces its OWN,
/// resolved from its own snapshot. `enforce = false` models a remote that fails
/// to, the leak this suite guards against.
struct RemoteWorker {
    runs: Vec<FetchedSeriesSoa>,
    own_erasure: Vec<ErasurePredicate>,
    enforce: bool,
}

#[async_trait]
impl SliceFetcher for RemoteWorker {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let mut scalar = self.runs.clone();
        if self.enforce {
            ravel_query::erasure::retain_series_soa(&mut scalar, &self.own_erasure);
        }
        Ok(ok_response(scalar))
    }
}

fn all_slices() -> DistribThresholds {
    // Zero thresholds force distribution for any non-empty snapshot; a single
    // slice is enough for this two-series corpus.
    DistribThresholds {
        min_store_bytes: 0,
        min_segments: 0,
        max_parallel_slices: 4,
    }
}

/// Drives `QueryEngine::instant` with a `Distributed` context. The coordinator
/// resolves the local snapshot (which carries the `.dreq`'s pending erasure),
/// partitions it, and dispatches the erasure predicates to the worker.
async fn distributed_instant_user_ids(
    store: &Arc<MemoryStore>,
    tenant_hash: TenantHash,
    worker: Arc<SliceWorker>,
    token: &CommitToken,
) -> Vec<String> {
    let distributed = Arc::new(Distributed::new(worker, all_slices()));
    let engine = engine_over(store).with_distributed(distributed);
    instant_user_ids(&engine, tenant_hash, std::slice::from_ref(token), TS_A).await
}

#[tokio::test]
async fn distributed_surface_excludes_erased_keeps_other() {
    let (tenant_id, tenant_hash) = tenant();
    let store = Arc::new(MemoryStore::new());
    // A real local segment so the coordinator resolves a non-empty snapshot to
    // partition and distribute; the worker supplies the decoded runs.
    let token = publish_segment(&store, &tenant_id, tenant_hash, &[TS_A]).await;
    put_dreq(&store, tenant_hash).await;

    // Compliant worker: applies the coordinator's predicates -> erased absent.
    let compliant = Arc::new(SliceWorker {
        runs: worker_runs(&tenant_id),
        apply_request_erasure: true,
    });
    assert_eq!(
        distributed_instant_user_ids(&store, tenant_hash, compliant, &token).await,
        kept_only(),
        "distributed fan-out must not leak the erased subject through a slice",
    );

    // Flip: a worker that ignores the coordinator's predicates leaks the erased
    // subject, so disabling erasure on this one surface fails the property.
    let leaky = Arc::new(SliceWorker {
        runs: worker_runs(&tenant_id),
        apply_request_erasure: false,
    });
    assert_eq!(
        distributed_instant_user_ids(&store, tenant_hash, leaky, &token).await,
        both_subjects(),
        "a worker that drops the erasure pass leaks the subject (the guarded flip)",
    );
}

/// Drives `QueryEngine::instant` with a `Federation` context and no local data,
/// so the whole result comes across the federated boundary.
async fn federated_instant_user_ids(
    tenant_hash: TenantHash,
    remote: Arc<RemoteWorker>,
) -> Vec<String> {
    let store = Arc::new(MemoryStore::new());
    let federation = Arc::new(Federation::new(vec![RemoteCluster {
        name: "remote-1".to_string(),
        fetcher: remote,
        skip_unavailable: false,
        soft_timeout: Duration::from_secs(10),
    }]));
    let engine = engine_over(&store).with_federation(federation);
    instant_user_ids(&engine, tenant_hash, &[], TS_A).await
}

#[tokio::test]
async fn federated_surface_excludes_erased_keeps_other() {
    let (tenant_id, tenant_hash) = tenant();
    let own = vec![ErasurePredicate::windowless(vec![(
        "user_id".to_string(),
        ERASED.to_string(),
    )])];

    // Compliant remote: enforces its own erasure -> erased absent, kept present.
    let compliant = Arc::new(RemoteWorker {
        runs: worker_runs(&tenant_id),
        own_erasure: own.clone(),
        enforce: true,
    });
    assert_eq!(
        federated_instant_user_ids(tenant_hash, compliant).await,
        kept_only(),
        "federated fan-out must not leak the erased subject from a remote",
    );

    // Flip: a remote that fails to enforce leaks the subject (the coordinator
    // trusts remotes to pre-filter, so this is the guarded regression).
    let leaky = Arc::new(RemoteWorker {
        runs: worker_runs(&tenant_id),
        own_erasure: own,
        enforce: false,
    });
    assert_eq!(
        federated_instant_user_ids(tenant_hash, leaky).await,
        both_subjects(),
        "a remote that skips its own erasure leaks the subject (the guarded flip)",
    );
}
