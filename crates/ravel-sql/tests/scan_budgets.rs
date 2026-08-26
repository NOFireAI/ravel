//! Scan-level budget enforcement.
//!
//! `max_samples` (crate::dedup) and `max_segments` (crate::provider,
//! pre-scan) already had tests; `max_series` and the fetch/decode-phase byte
//! charge inside `RsegScanExec::prepare_partition` did not. Both tests here
//! force a single DataFusion partition (`provider.plan(1)`) so every
//! segment in a snapshot is fetched strictly sequentially by one
//! `prepare_partition` call, which is what lets a GET counter prove an
//! early exit: if the budget trips after segment k, segments after k were
//! never touched, and the counter is the only way to see that from outside
//! the scan.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{ByteLimit, EngineConfig, RequestLimit, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{RavelTableProvider, SqlConfig, TenantMemoryAccountant};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// A pass-through backend that counts GETs, so a test can prove a budget
/// early-exit by observing that not every segment's GET happened.
struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    gets: AtomicU64,
}

impl CountingStore {
    fn wrap(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
        Arc::new(CountingStore {
            inner,
            gets: AtomicU64::new(0),
        })
    }

    fn gets(&self) -> u64 {
        self.gets.load(Ordering::Acquire)
    }
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets.fetch_add(1, Ordering::AcqRel);
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

#[derive(Clone, Debug)]
struct SeriesSpec {
    metric: String,
    samples: Vec<(i64, f64)>,
}

#[derive(Clone, Debug)]
struct SegSpec {
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    series: Vec<SeriesSpec>,
}

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn series_id_for(metric: &str) -> [u8; 16] {
    let tenant = TenantId::new("t".to_string());
    SeriesId::compute(&tenant, metric, &labels_for(metric))
        .expect("series id")
        .0
}

async fn write_segment(
    store: &dyn ObjectStoreBackend,
    key: &str,
    writer_id: Uuid,
    spec: &SegSpec,
) -> SegmentRef {
    let inputs: Vec<SeriesInput> = spec
        .series
        .iter()
        .filter(|s| !s.samples.is_empty())
        .map(|s| SeriesInput {
            series_id: SeriesId(series_id_for(&s.metric)),
            labels: labels_for(&s.metric),
            samples: s
                .samples
                .iter()
                .map(|(ts_ns, value)| Sample {
                    ts_ns: *ts_ns,
                    value: *value,
                })
                .collect(),
        })
        .collect();

    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: spec.writer_epoch,
        writer_seq: spec.writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");
    store
        .put(key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put segment");

    SegmentRef {
        data_object_key: key.to_string(),
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: 0,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard: 0,
        content_hash: written.summary.blake3,
        writer_id,
        writer_epoch: spec.writer_epoch,
        writer_seq: spec.writer_seq,
        created_unix_ns: spec.created_unix_ns,
        level: SegmentLevel::L0,
    }
}

/// Write every spec onto `store` and return the resulting snapshot, in the
/// same deterministic segment order the dedup path uses.
async fn build_snapshot(store: &dyn ObjectStoreBackend, specs: &[SegSpec]) -> Snapshot {
    let mut segments = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        let key = format!("t/metrics/seg-{i}.rseg");
        let writer_id = Uuid::from_u128(1000 + i as u128);
        segments.push(write_segment(store, &key, writer_id, spec).await);
    }
    segments.sort_by_key(|s| (s.created_unix_ns, s.writer_epoch, s.writer_seq, s.shard));
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// Five segments, two distinct series each: 10 distinct series total across
/// the snapshot, none repeated across segments.
fn five_segments_two_series_each() -> Vec<SegSpec> {
    (0..5)
        .map(|i| SegSpec {
            created_unix_ns: 1,
            writer_epoch: 1,
            writer_seq: i + 1,
            series: vec![
                SeriesSpec {
                    metric: format!("m{}", i * 2),
                    samples: vec![(0, i as f64)],
                },
                SeriesSpec {
                    metric: format!("m{}", i * 2 + 1),
                    samples: vec![(1, i as f64)],
                },
            ],
        })
        .collect()
}

/// `max_series` must reject a partition before every remaining
/// segment is fetched, not only after every segment's rows have already
/// been decoded. Proven by comparing GET counts between a generous cap
/// (every segment fetched) and a tiny one (tripped partway through).
#[tokio::test]
async fn max_series_rejects_before_every_segment_is_fetched() {
    let specs = five_segments_two_series_each();
    let raw_store = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(raw_store.as_ref(), &specs).await;

    // Baseline: a generous cap lets every one of the 10 distinct series
    // through, so every one of the 5 segments must be fetched.
    let baseline_store = CountingStore::wrap(raw_store.clone());
    let baseline_fetcher =
        SegmentFetcher::new(Arc::clone(&baseline_store) as Arc<dyn ObjectStoreBackend>);
    let baseline_config = EngineConfig {
        max_series: 100,
        ..EngineConfig::default()
    };
    let baseline_provider = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        baseline_fetcher,
        baseline_config,
        QueryAccounting::new(),
    );
    let baseline_plan = baseline_provider.plan(1).expect("plan");
    collect(baseline_plan, Arc::new(TaskContext::default()))
        .await
        .expect("generous cap must not trip");
    let baseline_gets = baseline_store.gets();
    assert!(
        baseline_gets > 0,
        "the generous-cap run must fetch at least one segment"
    );

    // Capped: max_series=3 is exceeded while the second segment's second
    // series is being added (segment 0 contributes 2, segment 1's first
    // series brings the running count to 3, its second series would push
    // it to 4), so segments 2..4 must never be fetched.
    let capped_store = CountingStore::wrap(raw_store.clone());
    let capped_fetcher =
        SegmentFetcher::new(Arc::clone(&capped_store) as Arc<dyn ObjectStoreBackend>);
    let capped_config = EngineConfig {
        max_series: 3,
        ..EngineConfig::default()
    };
    let capped_provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        capped_fetcher,
        capped_config,
        QueryAccounting::new(),
    );
    let capped_plan = capped_provider.plan(1).expect("plan");
    let err = collect(capped_plan, Arc::new(TaskContext::default()))
        .await
        .expect_err("tiny cap must trip");
    let msg = format!("{err}");
    assert!(msg.contains("too many series"), "got: {msg}");

    let capped_gets = capped_store.gets();
    assert!(
        capped_gets < baseline_gets,
        "a tripped max_series must stop fetching before every segment is \
         pulled (capped={capped_gets}, baseline={baseline_gets})"
    );
}

/// Total rows the five-segments-two-series snapshot yields post-dedup: 5
/// segments x 2 series x 1 sample each, none repeated across segments.
fn total_rows(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// The per-tenant bytes-scanned budget (ADR-0061 decision 1) must
/// reject a partition before every remaining segment is fetched, not only
/// after every segment's bytes have already been paid for. Proven the same
/// way `max_series` is: compare GET counts between an `Unlimited` baseline
/// (every segment fetched) and a bounded cap that trips partway through. The
/// scan loop is genuinely sequential (one partition via `plan(1)`), so a
/// tripped budget means the segments after the trip point never issue a GET.
#[tokio::test]
async fn max_bytes_scanned_rejects_before_every_segment_is_fetched() {
    let specs = five_segments_two_series_each();
    let raw_store = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(raw_store.as_ref(), &specs).await;

    // Baseline: Unlimited fetches every one of the 5 segments. Hold a clone of
    // the accounting handle so the total S3 bytes it scanned is readable after
    // the run, to size the bounded cap below.
    let baseline_store = CountingStore::wrap(raw_store.clone());
    let baseline_fetcher =
        SegmentFetcher::new(Arc::clone(&baseline_store) as Arc<dyn ObjectStoreBackend>);
    let baseline_config = EngineConfig {
        max_bytes_scanned: ByteLimit::Unlimited,
        ..EngineConfig::default()
    };
    let baseline_acc = QueryAccounting::new();
    let baseline_provider = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        baseline_fetcher,
        baseline_config,
        baseline_acc.clone(),
    );
    let baseline_plan = baseline_provider.plan(1).expect("plan");
    collect(baseline_plan, Arc::new(TaskContext::default()))
        .await
        .expect("Unlimited must not trip");
    let baseline_gets = baseline_store.gets();
    let total_bytes = baseline_acc.snapshot().total_s3_bytes();
    assert!(
        baseline_gets > 1,
        "the baseline must fetch more than one segment for an early exit to be \
         observable (baseline_gets={baseline_gets})"
    );
    assert!(total_bytes > 0, "the baseline must have scanned some bytes");

    // Capped: a budget at half the baseline's total bytes trips at a segment
    // boundary partway through, so the later segments are never fetched.
    let capped_store = CountingStore::wrap(raw_store.clone());
    let capped_fetcher =
        SegmentFetcher::new(Arc::clone(&capped_store) as Arc<dyn ObjectStoreBackend>);
    let capped_config = EngineConfig {
        max_bytes_scanned: ByteLimit::Bounded(total_bytes / 2),
        ..EngineConfig::default()
    };
    let capped_provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        capped_fetcher,
        capped_config,
        QueryAccounting::new(),
    );
    let capped_plan = capped_provider.plan(1).expect("plan");
    let err = collect(capped_plan, Arc::new(TaskContext::default()))
        .await
        .expect_err("a bounded byte budget below the scan total must trip");
    let msg = format!("{err}");
    assert!(msg.contains("too many bytes"), "got: {msg}");

    let capped_gets = capped_store.gets();
    assert!(
        capped_gets > 0,
        "at least the first segment must have been fetched (capped_gets={capped_gets})"
    );
    assert!(
        capped_gets < baseline_gets,
        "a tripped bytes-scanned budget must stop fetching before every segment \
         is pulled (capped={capped_gets}, baseline={baseline_gets})"
    );
}

/// No-regression: an `Unlimited` bytes-scanned budget must fetch every segment
/// and return the identical full row set the scan produced before this budget
/// existed. `Unlimited` is the code path an uncapped caller takes.
#[tokio::test]
async fn unlimited_bytes_scanned_matches_prior_behavior() {
    let specs = five_segments_two_series_each();
    let raw_store = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(raw_store.as_ref(), &specs).await;

    let store = CountingStore::wrap(raw_store);
    let fetcher = SegmentFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
    let config = EngineConfig {
        max_bytes_scanned: ByteLimit::Unlimited,
        ..EngineConfig::default()
    };
    let provider =
        RavelTableProvider::new(snapshot, TENANT, fetcher, config, QueryAccounting::new());
    let plan = provider.plan(1).expect("plan");
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("Unlimited must not trip");

    // 5 segments x 2 distinct series x 1 sample each = 10 rows, none deduped.
    assert_eq!(
        total_rows(&batches),
        10,
        "Unlimited must return the full, unmodified row set"
    );
    assert!(
        store.gets() > 0,
        "Unlimited must genuinely fetch the snapshot's segments"
    );
}

/// The S3 request budget (ADR-0073 decision 3) must
/// reject a partition before every remaining segment is fetched, the same
/// early-exit contract `max_series` and `max_bytes_scanned` already have.
/// Proven the same way: compare GET counts between an `Unlimited` baseline
/// (every segment fetched) and a bounded cap that trips after the first.
/// `RsegScanExec::prepare_partition` checks
/// `ravel_query::request_budget_exceeded` once per completed segment fetch
/// (the same seam PromQL's `fetch_all_series` and the exemplars endpoint
/// check), so a cap of 1 must let exactly the first segment's GET through
/// and cancel the rest.
#[tokio::test]
async fn max_s3_requests_rejects_before_every_segment_is_fetched() {
    let specs = five_segments_two_series_each();
    let raw_store = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(raw_store.as_ref(), &specs).await;

    let baseline_store = CountingStore::wrap(raw_store.clone());
    let baseline_fetcher =
        SegmentFetcher::new(Arc::clone(&baseline_store) as Arc<dyn ObjectStoreBackend>);
    let baseline_config = EngineConfig {
        max_s3_requests: RequestLimit::Unlimited,
        ..EngineConfig::default()
    };
    let baseline_provider = RavelTableProvider::new(
        snapshot.clone(),
        TENANT,
        baseline_fetcher,
        baseline_config,
        QueryAccounting::new(),
    );
    let baseline_plan = baseline_provider.plan(1).expect("plan");
    collect(baseline_plan, Arc::new(TaskContext::default()))
        .await
        .expect("Unlimited must not trip");
    let baseline_gets = baseline_store.gets();
    assert_eq!(
        baseline_gets, 5,
        "the baseline must fetch every one of the 5 segments"
    );

    let capped_store = CountingStore::wrap(raw_store.clone());
    let capped_fetcher =
        SegmentFetcher::new(Arc::clone(&capped_store) as Arc<dyn ObjectStoreBackend>);
    let capped_config = EngineConfig {
        max_s3_requests: RequestLimit::Bounded(1),
        ..EngineConfig::default()
    };
    let capped_provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        capped_fetcher,
        capped_config,
        QueryAccounting::new(),
    );
    let capped_plan = capped_provider.plan(1).expect("plan");
    let err = collect(capped_plan, Arc::new(TaskContext::default()))
        .await
        .expect_err("a budget of 1 request must trip after the first segment");
    let msg = format!("{err}");
    assert!(msg.contains("S3 requests"), "got: {msg}");

    let capped_gets = capped_store.gets();
    assert!(
        capped_gets < baseline_gets,
        "a tripped request budget must stop fetching before every segment is \
         pulled (capped={capped_gets}, baseline={baseline_gets})"
    );
}

fn task_ctx_with_query_bytes(max_query_bytes: usize) -> Arc<TaskContext> {
    let config = SqlConfig {
        engine: EngineConfig::default(),
        max_query_bytes,
        parallel_final_aggregation: false,
        skip_partial_aggregation: true,
        // This fixture executes a `LogsScanExec` directly, with no session and
        // no optimizer, so the ADR-0774 rewrite cannot reach it either way.
        // Named explicitly rather than spread from the default so a new field
        // keeps failing this initializer closed.
        late_materialization_extra_columns: None,
    };
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    let rt = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .expect("runtime env");
    Arc::new(TaskContext::default().with_runtime(rt))
}

/// A byte budget that the fetch/decode-phase charge alone overruns must
/// reject the scan during that phase, before any `RecordBatch`
/// is ever built: the stream's very first poll is the error, not a batch.
#[tokio::test]
async fn byte_budget_rejects_during_fetch_decode_not_only_after_batches() {
    const SAMPLES: i64 = 20_000;
    let specs = vec![SegSpec {
        created_unix_ns: 1,
        writer_epoch: 1,
        writer_seq: 1,
        series: vec![SeriesSpec {
            metric: "m".into(),
            samples: (0..SAMPLES).map(|i| (i, i as f64)).collect(),
        }],
    }];
    let raw_store = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(raw_store.as_ref(), &specs).await;
    let counting_store = CountingStore::wrap(raw_store);

    // The fetch/decode charge is the decoded SoA the merge holds: 20,000
    // samples at one i64 timestamp plus one f64 value each is ~313 KiB, plus
    // this segment's fetched raw-f64 page bytes. A 200 KB ceiling is below
    // that on its own, before any batch-phase growth would even be reachable.
    // The unit is the SoA's bytes, not `rows * size_of::<ScanRow>()`: ADR-0099
    // decision 6 deleted the 64-byte row struct this threshold used to be
    // written against, and the reservation charges the buffers that replaced
    // it. Same live-bytes contract, smaller per-sample figure, which is why
    // the ceiling here is now the tighter bound of the two.
    let task_ctx = task_ctx_with_query_bytes(200_000);

    let fetcher = SegmentFetcher::new(Arc::clone(&counting_store) as Arc<dyn ObjectStoreBackend>);
    let provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        fetcher,
        EngineConfig::default(),
        QueryAccounting::new(),
    );
    let plan = provider.plan(1).expect("plan");
    let mut stream = plan.execute(0, task_ctx).expect("execute");

    let first = stream
        .next()
        .await
        .expect("stream must yield something")
        .expect_err("the byte budget must trip on the very first poll");
    let msg = format!("{first}");
    assert!(
        msg.contains("memory pool exhausted") || msg.contains("query memory pool exhausted"),
        "expected the pool's typed error, got: {msg}"
    );

    assert!(
        counting_store.gets() > 0,
        "the single segment must genuinely have been fetched before the \
         decode-phase charge rejected it, otherwise this would be some \
         earlier, unrelated validation failure"
    );
}
