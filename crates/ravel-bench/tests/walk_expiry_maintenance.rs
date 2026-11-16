//! Issue #3779: which maintenance events expire a pinned `ravel_query_sql`
//! walk, and with which cause label. Each case walks S1 and S2 through
//! `ravel_bench::walk_probe::PinnedWalk`, the page loop the `mcp_walk_expiry`
//! bin runs, over a `MemoryStore` and an injected clock, and runs one event
//! between page 2 and page 3: nothing, a new flush, a compaction of the window
//! hour, or a fold that seals it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use ravel_bench::walk_expiry::{Causes, KeyKind, Statement, WalkEnd, key_kind};
use ravel_bench::walk_probe::{PinnedWalk, WalkSpec};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_maintain::{Bucket, CompactionOutcome, CompactorConfig, compact_bucket};
use ravel_mcp::budget::{McpBudgetConfig, McpEffectiveBudgets, McpRequestBudgets};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{
    LogSegmentFetcher, QueryAdmissionController, QueryConcurrencyLimit, SegmentFetcher,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7};
use ravel_server::service::{QueryService, default_query_accounting};
use ravel_server::sql::SqlState;
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange,
};
use uuid::Uuid;

const NS_PER_S: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_S;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;
/// The window hour, an ingest hour bucket in 2026.
const HOUR: u32 = 495_000;
const SHARDS: u32 = 2;
const PAGES: u32 = 3;
/// The S2 family: every series of it lives on shard 0.
const S2_FAMILY: &str = "alpha";

fn hour_start_ns() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn hour_end_ns() -> i64 {
    hour_start_ns() + NS_PER_HOUR
}

/// `max_flush_lifetime + clock_skew_allowance` past the hour end: the
/// compactor's eligibility for the hour.
fn compaction_eligible_ns() -> i64 {
    let c = CatalogConfig::default();
    hour_end_ns() + c.max_flush_lifetime_ns + c.clock_skew_allowance_ns
}

/// `max_flush_lifetime + clock_skew_allowance + fold_safety_margin` past the
/// hour end: the earliest a fold seals the hour.
fn fold_eligible_ns() -> i64 {
    let c = CatalogConfig::default();
    compaction_eligible_ns() + c.fold_safety_margin_ns
}

/// One injected clock for the SQL service and the compactor.
#[derive(Clone)]
struct TestClock(Arc<AtomicI64>);

impl TestClock {
    fn new(now_ns: i64) -> Self {
        TestClock(Arc::new(AtomicI64::new(now_ns)))
    }

    fn set(&self, now_ns: i64) {
        self.0.store(now_ns, Ordering::SeqCst);
    }

    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl ravel_ingest::Clock for TestClock {
    fn now_ns(&self) -> i64 {
        self.now()
    }
}

impl ravel_maintain::Clock for TestClock {
    fn now_ns(&self) -> i64 {
        self.now()
    }
}

/// One flush's series: metric name, instance, sample count.
type FlushSeries<'a> = &'a [(&'a str, &'a str, usize)];

/// The load. Per metric name the totals are alpha 10, beta 6, gamma 4,
/// delta 2, all distinct, so S1 (ordered by count alone) pages one name per
/// page; alpha's four series count 4, 3, 2 and 1, so S2 pages one series per
/// page. Shard 1 holds no alpha series.
const FLUSHES: &[(u32, u128, FlushSeries<'static>)] = &[
    (
        0,
        0x10,
        &[("alpha", "a", 4), ("alpha", "b", 3), ("beta", "a", 6)],
    ),
    (0, 0x11, &[("alpha", "c", 2), ("alpha", "d", 1)]),
    (1, 0x20, &[("gamma", "a", 4)]),
    (1, 0x21, &[("delta", "a", 2)]),
];

/// Write one RSEG of `series` into `(shard, HOUR)` and publish its commit
/// record, as a flush does.
async fn flush(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    shard: u32,
    writer: u128,
    series: FlushSeries<'_>,
    created_unix_ns: i64,
) -> String {
    let base_ts = hour_start_ns() + 10 * NS_PER_MIN;
    let mut inputs: Vec<SeriesInput> = series
        .iter()
        .map(|&(name, instance, samples)| {
            let labels = LabelSet::new(vec![
                Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: name.to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: instance.to_string(),
                },
            ])
            .expect("labels");
            let series_id = SeriesId::compute(tenant, name, &labels).expect("series id");
            let samples = (0..samples)
                .map(|i| Sample {
                    ts_ns: base_ts + i as i64 * NS_PER_S,
                    value: i as f64,
                })
                .collect();
            SeriesInput {
                series_id,
                labels,
                samples,
            }
        })
        .collect();
    inputs.sort_by_key(|s| s.series_id);
    let min_ts = base_ts;
    let max_ts = base_ts + 10 * NS_PER_S;
    let writer_id = Uuid::from_u128(writer);
    let tenant_hash = tenant.hash();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 0,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 0,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns,
        ingest_hour_bucket: HOUR,
    })
    .expect("commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish commit");
    data_key
}

struct Fixture {
    store: Arc<dyn ObjectStoreBackend>,
    tenant: TenantId,
    tenant_hash: TenantHash,
    clock: TestClock,
    catalog: Arc<Catalog>,
    service: QueryService,
    budgets: McpEffectiveBudgets,
}

fn catalog_config() -> CatalogConfig {
    CatalogConfig {
        shard_count: SHARDS,
        // Every resolve reads HEAD, so a fold is visible to the next page
        // rather than after the cache's wall-clock TTL.
        head_cache_ttl_ns: 0,
        ..CatalogConfig::default()
    }
}

/// The load flushed into `HOUR` on both shards, the clock at `now_ns`, and a
/// query service built the way the server builds its SQL surface.
async fn fixture(tenant: &str, now_ns: i64) -> Fixture {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(tenant);
    let tenant_hash = tenant.hash();
    for &(shard, writer, series) in FLUSHES {
        flush(
            store.as_ref(),
            &tenant,
            shard,
            writer,
            series,
            hour_start_ns() + 20 * NS_PER_MIN,
        )
        .await;
    }
    let clock = TestClock::new(now_ns);
    let catalog = Arc::new(Catalog::new(Arc::clone(&store), catalog_config()).expect("catalog"));
    let sql_config = SqlConfig::default();
    let budgets = McpRequestBudgets {
        max_rows: Some(1),
        ..McpRequestBudgets::default()
    }
    .clamp(&sql_config.engine, &McpBudgetConfig::default());
    assert_eq!(budgets.max_rows, 1, "the MCP clamp keeps a one-row page");
    let executor = SqlExecutor::new(
        Arc::clone(&catalog),
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        sql_config,
        1 << 30,
    );
    let mut tokens = HashMap::new();
    tokens.insert("walk-expiry-token".to_string(), tenant.clone());
    let service = SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store: Arc::clone(&store),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
        clock: Arc::new(clock.clone()),
        max_deadline: Duration::from_secs(30),
        query_accounting: default_query_accounting(),
        query_admission: QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
    }
    .service();
    Fixture {
        store,
        tenant,
        tenant_hash,
        clock,
        catalog,
        service,
        budgets,
    }
}

impl Fixture {
    fn window(&self) -> TimeRange {
        TimeRange {
            start_ns: hour_start_ns(),
            end_ns: hour_end_ns(),
        }
    }

    /// Page one of `statement` at the clock's current reading.
    async fn begin(&self, statement: Statement) -> PinnedWalk<'_> {
        let mut walk = PinnedWalk::start(WalkSpec {
            service: &self.service,
            catalog: &self.catalog,
            tenant_hash: self.tenant_hash,
            budgets: &self.budgets,
            sql: statement.sql(S2_FAMILY),
            window: self.window(),
            t0_ns: self.clock.now(),
            pages: PAGES,
        })
        .await
        .expect("walk starts");
        let report = walk.step(self.clock.now()).await.expect("page 1");
        assert_eq!(report.end, None, "{statement:?} page 1 ends the walk");
        walk
    }

    /// Both statements through page 2, `event` between page 2 and page 3, and
    /// page 3 of each. Returns each walk's end and page one's segment set.
    async fn walk_both<F, Fut>(&self, event: F) -> [(WalkEnd, Vec<String>); 2]
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let mut s1 = self.begin(Statement::S1).await;
        let mut s2 = self.begin(Statement::S2).await;
        for (statement, walk) in [(Statement::S1, &mut s1), (Statement::S2, &mut s2)] {
            self.clock.set(self.clock.now() + NS_PER_S);
            let report = walk.step(self.clock.now()).await.expect("page 2");
            assert_eq!(report.end, None, "{statement:?} page 2 ends the walk");
        }
        event().await;
        let mut out = Vec::new();
        for walk in [&mut s1, &mut s2] {
            self.clock.set(self.clock.now() + NS_PER_S);
            let report = walk.step(self.clock.now()).await.expect("page 3");
            assert_eq!(report.unresolved_hours, 0, "every removed l0/ key placed");
            let end = report.end.expect("page 3 is the last page");
            out.push((end, walk.k0().iter().cloned().collect::<Vec<_>>()));
        }
        let [a, b]: [(WalkEnd, Vec<String>); 2] = out.try_into().expect("two walks");
        [a, b]
    }

    async fn compact_hour(&self) {
        let config = CompactorConfig::default();
        for shard in 0..SHARDS {
            let bucket = Bucket::new(self.tenant_hash, Signal::Metrics, shard, HOUR);
            let outcome = compact_bucket(self.store.as_ref(), &self.clock, &config, &bucket)
                .await
                .expect("compact");
            assert!(
                matches!(outcome, CompactionOutcome::Compacted { .. }),
                "shard {shard} compacts, got {outcome:?}"
            );
        }
    }

    async fn watermark(&self) -> Option<u32> {
        self.catalog
            .head_watermark_hour(&self.tenant_hash, Signal::Metrics)
            .await
            .expect("watermark")
    }

    async fn fold(&self) {
        let folder = Catalog::new(Arc::clone(&self.store), catalog_config()).expect("catalog");
        folder
            .fold(
                &self.tenant_hash,
                Signal::Metrics,
                Uuid::from_u128(0xf01d),
                self.clock.now(),
                &[],
                None,
            )
            .await
            .expect("fold");
    }
}

fn expired(page: u32, causes: Causes) -> WalkEnd {
    WalkEnd::Expired { page, causes }
}

const COMMIT: Causes = Causes {
    commit: true,
    compaction: false,
    fold_or_retention: false,
    other: false,
};
const COMPACTION: Causes = Causes {
    commit: false,
    compaction: true,
    fold_or_retention: false,
    other: false,
};
const FOLD: Causes = Causes {
    commit: false,
    compaction: false,
    fold_or_retention: true,
    other: false,
};

fn all_of(keys: &[String], want: fn(KeyKind) -> bool) -> bool {
    !keys.is_empty() && keys.iter().all(|k| want(key_kind(k)))
}

fn is_l0(kind: KeyKind) -> bool {
    kind == KeyKind::L0
}

fn is_l1_of_hour(kind: KeyKind) -> bool {
    kind == KeyKind::L1 { hour: HOUR }
}

/// No event between pages: both walks finish on page one's digest.
#[tokio::test]
async fn no_event_between_pages_finishes() {
    let f = fixture("walk-expiry-control", hour_end_ns() + NS_PER_S).await;
    let [(s1, s1_k0), (s2, s2_k0)] = f.walk_both(|| async {}).await;
    assert_eq!(s1, WalkEnd::Finished, "S1 with no event");
    assert_eq!(s2, WalkEnd::Finished, "S2 with no event");
    assert_eq!(
        s1_k0.len(),
        FLUSHES.len(),
        "S1 reads every flush: {s1_k0:?}"
    );
    assert!(all_of(&s1_k0, is_l0) && all_of(&s2_k0, is_l0));
}

/// A new flush into the window hour between pages expires both walks, label
/// commit and nothing else.
#[tokio::test]
async fn a_flush_into_the_hour_expires_with_commit() {
    let f = fixture("walk-expiry-commit", hour_end_ns() + NS_PER_S).await;
    let [(s1, _), (s2, _)] = f
        .walk_both(|| async {
            flush(
                f.store.as_ref(),
                &f.tenant,
                0,
                0x30,
                &[("alpha", "e", 5)],
                f.clock.now(),
            )
            .await;
        })
        .await;
    assert_eq!(s1, expired(3, COMMIT), "S1 after a flush");
    assert_eq!(s2, expired(3, COMMIT), "S2 after a flush");
}

/// A compaction of the window hour between pages expires both walks, label
/// compaction and nothing else.
#[tokio::test]
async fn a_compaction_of_the_hour_expires_with_compaction() {
    let f = fixture(
        "walk-expiry-compaction",
        compaction_eligible_ns() + NS_PER_S,
    )
    .await;
    let [(s1, s1_k0), (s2, s2_k0)] = f.walk_both(|| f.compact_hour()).await;
    assert!(all_of(&s1_k0, is_l0), "S1 page one reads L0: {s1_k0:?}");
    assert!(all_of(&s2_k0, is_l0), "S2 page one reads L0: {s2_k0:?}");
    assert_eq!(s1, expired(3, COMPACTION), "S1 after a compaction");
    assert_eq!(s2, expired(3, COMPACTION), "S2 after a compaction");
}

/// On a fresh walk after the compaction, a fold that seals the hour expires
/// the name-pruned S2 walk, label fold (`fold_or_retention`), and leaves the
/// S1 walk's digest unchanged.
#[tokio::test]
async fn a_fold_seal_expires_s2_with_fold_and_leaves_s1() {
    let f = fixture("walk-expiry-fold", compaction_eligible_ns() + NS_PER_S).await;
    f.compact_hour().await;
    assert!(
        f.watermark().await < Some(HOUR),
        "the hour is not sealed before the fold"
    );
    let [(s1, s1_k0), (s2, s2_k0)] = f
        .walk_both(|| async {
            f.clock.set(fold_eligible_ns() + NS_PER_S);
            f.fold().await;
            assert_eq!(f.watermark().await, Some(HOUR), "the fold seals the hour");
        })
        .await;
    assert!(all_of(&s1_k0, is_l1_of_hour), "S1 reads L1: {s1_k0:?}");
    assert!(all_of(&s2_k0, is_l1_of_hour), "S2 reads L1: {s2_k0:?}");
    assert_eq!(s1, WalkEnd::Finished, "S1 after the fold seal");
    assert_eq!(s2, expired(3, FOLD), "S2 after the fold seal");
}
