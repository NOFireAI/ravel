//! The bulk-scan size trigger end to end through `SqlExecutor::execute`: the
//! bytes a statement read on its last successful execution, not the bytes its
//! lane resolves to, decide whether its next execution reads as BulkScan.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use ravel_cache::{Cache, CacheKey, CacheLimits, DiskCache, TieredCache};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::instrument::InstrumentedStore;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_query::{
    CacheFetchError, LogSegmentFetcher, ReadCache, SegmentFetcher, SpanSegmentFetcher,
};
use ravel_types::accounting::{AccessIntent, AccountedOp, QueryAccountingSnapshot};
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};

use crate::access_intent::trigger_read_bytes;
use crate::config::SqlConfig;
use crate::declared::{DeclaredColumn, DeclaredType, KeyIndexDeclaration, StaticDeclaredColumns};
use crate::executor::SqlExecutor;
use crate::executor::tests::{executor_over, log_record, publish_log_records, sql_request};

const WINDOW: TimeRange = TimeRange {
    start_ns: 0,
    end_ns: 2_000_000,
};

/// The threshold every test here runs with: below the lane's 1,368 resolved
/// bytes, above [`EARLY_PRUNED`]'s 1,241 read bytes, below [`WHOLE_LANE`]'s
/// 1,914.
const MIN_BYTES: u64 = 1_300;

/// Filters on the late object's time range, so the scan skips the early
/// object: it reads the catalog (546 bytes) and the late object (695).
const EARLY_PRUNED: &str = "SELECT body FROM logs \
     WHERE ts >= TIMESTAMP '1970-01-01 00:00:00.001' AND severity_text = 'ERROR'";

/// Filters on a field no object's statistics prune, so the scan reads both
/// objects (1,368 bytes) after the catalog (546).
const WHOLE_LANE: &str = "SELECT ts, body FROM logs WHERE severity_text = 'ERROR'";

/// The catalog bytes every statement here reads before any data object.
const CATALOG_BYTES: u64 = 546;

type FixtureStore = Arc<InstrumentedStore<FaultStore<MemoryStore>>>;

/// One data object of a [`Lane`]: its store key, its size, and the key a read
/// cache files a whole-object read of it under.
struct DataObject {
    key: String,
    size: u64,
    cache_key: CacheKey,
}

/// Two 200-record RLOG objects on one tenant's `logs` lane, the first at
/// `ts` 0..19,900 ns and the second at 1,000,000..1,019,900 ns.
struct Lane {
    store: FixtureStore,
    tenant_hash: TenantHash,
    early: DataObject,
    late: DataObject,
}

/// A [`Lane`] whose store runs the fault plan `plan` builds from the early and
/// late objects' keys.
async fn lane_with(plan: impl FnOnce(&str, &str) -> FaultPlan) -> Lane {
    let tenant_hash = TenantId::new("size-trigger".to_string()).hash();
    let memory = MemoryStore::new();
    let early: Vec<_> = (0..200).map(|i| log_record("api", i * 100)).collect();
    let late: Vec<_> = (0..200)
        .map(|i| log_record("api", 1_000_000 + i * 100))
        .collect();
    let early_key = publish_log_records(&memory, tenant_hash, Signal::Logs, 1, &early).await;
    let late_key = publish_log_records(&memory, tenant_hash, Signal::Logs, 2, &late).await;
    let early = data_object(&memory, tenant_hash, early_key).await;
    let late = data_object(&memory, tenant_hash, late_key).await;
    let plan = plan(&early.key, &late.key);
    Lane {
        store: Arc::new(InstrumentedStore::new(FaultStore::new(memory, plan))),
        tenant_hash,
        early,
        late,
    }
}

async fn data_object(memory: &MemoryStore, tenant_hash: TenantHash, key: String) -> DataObject {
    let bytes = memory
        .get(&key, GetRange::Full)
        .await
        .expect("get the data object")
        .data;
    let size = bytes.len() as u64;
    DataObject {
        key,
        size,
        cache_key: CacheKey::new(tenant_hash.0, *blake3::hash(&bytes).as_bytes(), 0, size),
    }
}

/// A [`Lane`] with no fault planned, and the sum of its object sizes, which is
/// what the lane's resolve returns.
async fn two_object_lane() -> (FixtureStore, TenantHash, u64) {
    let lane = lane_with(|_, _| FaultPlan::empty()).await;
    let mut resolved = 0;
    for key in [&lane.early.key, &lane.late.key] {
        resolved += lane.store.head(key).await.expect("head").size;
    }
    (lane.store, lane.tenant_hash, resolved)
}

/// `exec` with the size trigger at `min_bytes` and `user_id` declared as a
/// KEY_IDX field.
fn with_trigger(exec: SqlExecutor, min_bytes: u64) -> SqlExecutor {
    let declared =
        StaticDeclaredColumns::new(vec![DeclaredColumn::new("user_id", DeclaredType::Str)])
            .with_key_index(KeyIndexDeclaration {
                columns: vec!["user_id".to_string()],
                trace_id: false,
            });
    exec.with_declared_column_source(Arc::new(declared))
        .with_cache_bulk_scan_min_bytes(Some(min_bytes))
}

/// An executor over `store` with the size trigger at [`MIN_BYTES`].
fn trigger_executor(store: FixtureStore) -> SqlExecutor {
    with_trigger(executor_over(store), MIN_BYTES)
}

/// [`trigger_executor`] whose logs fetcher reads through `cache`.
fn cached_trigger_executor(store: FixtureStore, cache: impl Into<ReadCache>) -> SqlExecutor {
    let store: Arc<dyn ObjectStoreBackend> = store;
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let exec = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()).with_cache(cache),
        SpanSegmentFetcher::new(store),
        SqlConfig::default(),
        1 << 30,
    );
    with_trigger(exec, MIN_BYTES)
}

/// A RAM read cache large enough to hold every object here.
fn ram_cache() -> Arc<Cache<CacheFetchError>> {
    Arc::new(Cache::new(CacheLimits::new(64 << 20, 4096, 64 << 20)))
}

/// Run `sql` once over `window` and return its pooled accounting.
async fn run_accounted(
    exec: &SqlExecutor,
    tenant_hash: TenantHash,
    sql: &str,
    window: TimeRange,
) -> QueryAccountingSnapshot {
    exec.execute(tenant_hash, &sql_request(sql, window))
        .await
        .expect("the statement runs")
        .accounting
}

/// Run `sql` once and return the intent it read under and the figure its
/// execution recorded for the trigger.
async fn run(exec: &SqlExecutor, tenant_hash: TenantHash, sql: &str) -> (AccessIntent, u64) {
    figure(&run_accounted(exec, tenant_hash, sql, WINDOW).await)
}

fn figure(accounting: &QueryAccountingSnapshot) -> (AccessIntent, u64) {
    (accounting.access_intent, trigger_read_bytes(accounting))
}

/// A filtered `logs` statement over a lane whose resolve (1,368 bytes) is at
/// or above the threshold (1,300), but which itself reads 1,241 bytes, stays
/// Interactive on its first execution and on its second.
///
/// FLIP: comparing the lane's resolved object bytes to the threshold, as the
/// trigger did before, reads both executions as BulkScan.
#[tokio::test]
async fn a_statement_that_reads_less_than_the_threshold_stays_interactive_over_a_larger_lane() {
    let (store, tenant_hash, resolved) = two_object_lane().await;
    assert_eq!(resolved, 1_368, "the lane's resolved object bytes");
    let exec = trigger_executor(store);

    let first = run(&exec, tenant_hash, EARLY_PRUNED).await;
    let second = run(&exec, tenant_hash, EARLY_PRUNED).await;

    assert_eq!(first, (AccessIntent::Interactive, 1_241), "first execution");
    assert_eq!(
        second,
        (AccessIntent::Interactive, 1_241),
        "second execution"
    );
}

/// A filtered `logs` statement that reads 1,914 bytes against a 1,300-byte
/// threshold is Interactive on its first execution, having no record yet, and
/// BulkScan on its second. A different statement text over the same lane has
/// no record of its own and is Interactive. An equality on a KEY_IDX field
/// stays Interactive however many bytes it read.
///
/// FLIP: comparing the lane's resolved object bytes to the threshold reads
/// every first execution as BulkScan; dropping the KEY_IDX check from the
/// trigger reads the key lookup's second execution as BulkScan.
#[tokio::test]
async fn a_statement_that_reads_the_threshold_is_bulk_from_its_second_execution() {
    let (store, tenant_hash, _) = two_object_lane().await;
    let exec = trigger_executor(store);

    let whole_lane = [
        run(&exec, tenant_hash, WHOLE_LANE).await,
        run(&exec, tenant_hash, WHOLE_LANE).await,
        run(&exec, tenant_hash, WHOLE_LANE).await,
    ];
    assert_eq!(
        whole_lane,
        [
            (AccessIntent::Interactive, 1_914),
            (AccessIntent::BulkScan, 1_914),
            (AccessIntent::BulkScan, 1_914),
        ],
        "the same statement text, three executions"
    );

    let other_text = "SELECT count(*) FROM logs WHERE severity_text = 'ERROR'";
    assert_eq!(
        run(&exec, tenant_hash, other_text).await,
        (AccessIntent::Interactive, 1_914),
        "a different statement text over the same lane"
    );

    let key_lookup = "SELECT body FROM logs WHERE user_id = 'u-1'";
    let key_runs = [
        run(&exec, tenant_hash, key_lookup).await,
        run(&exec, tenant_hash, key_lookup).await,
        run(&exec, tenant_hash, key_lookup).await,
    ];
    assert_eq!(
        key_runs,
        [(AccessIntent::Interactive, 1_914); 3],
        "a KEY_IDX equality, three executions"
    );
}

/// A statement whose second execution is served from the read cache records
/// the cache-served bytes with its GETs, so its third execution is still
/// BulkScan.
///
/// FLIP: recording the store GET bytes alone records 0 bytes for the second
/// execution and reads the third as Interactive.
#[tokio::test]
async fn a_cache_served_execution_records_its_cache_bytes() {
    let (store, tenant_hash, resolved) = two_object_lane().await;
    let exec = cached_trigger_executor(store, ram_cache());

    let runs = [
        run_accounted(&exec, tenant_hash, WHOLE_LANE, WINDOW).await,
        run_accounted(&exec, tenant_hash, WHOLE_LANE, WINDOW).await,
        run_accounted(&exec, tenant_hash, WHOLE_LANE, WINDOW).await,
    ];

    assert_eq!(
        runs.each_ref().map(figure),
        [
            (AccessIntent::Interactive, 1_914),
            (AccessIntent::BulkScan, 1_914),
            (AccessIntent::BulkScan, 1_914),
        ],
        "the same statement text, three executions"
    );
    assert_eq!(
        (runs[0].cache_bytes, runs[0].s3_bytes(AccountedOp::Get)),
        (0, CATALOG_BYTES + resolved),
        "the first execution fetches both objects"
    );
    for (i, run) in runs.iter().enumerate().skip(1) {
        assert_eq!(
            (run.cache_bytes, run.s3_bytes(AccountedOp::Get)),
            (CATALOG_BYTES + resolved, 0),
            "execution {}: the catalog's cache serves its reads and the read cache \
             serves both objects",
            i + 1
        );
    }
}

/// Two statements reading the same objects at once: the second parks on the
/// first one's in-flight fetch of the early object and is served it late.
/// Each records the 1,914 bytes it reads when run alone, and each reads its
/// next execution as BulkScan.
///
/// FLIP: leaving the late-served bytes out of the recorded figure records
/// the follower's 1,914 less the early object's size, below the threshold,
/// and reads its next execution as Interactive.
#[tokio::test]
async fn a_late_served_read_records_the_same_figure_as_a_fetched_one() {
    let lane = lane_with(|_, _| FaultPlan::empty()).await;
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let limits = CacheLimits::new(64 << 20, 4096, 64 << 20);
    let tiered: Arc<TieredCache<CacheFetchError>> = Arc::new(TieredCache::new(
        Cache::new(limits),
        DiskCache::new(tmp.path().to_path_buf(), limits),
    ));
    let gate = lane
        .store
        .inner()
        .hold(Op::Get, Some(lane.early.key.clone()), Occurrence::Nth(1));
    let exec = cached_trigger_executor(lane.store.clone(), Arc::clone(&tiered));
    let leader_sql = WHOLE_LANE;
    let follower_sql = &format!("{WHOLE_LANE} ");

    let leader = run_accounted(&exec, lane.tenant_hash, leader_sql, WINDOW);
    let follower = async {
        gate.wait_until_held(1).await;
        run_accounted(&exec, lane.tenant_hash, follower_sql, WINDOW).await
    };
    let release = async {
        gate.wait_until_held(1).await;
        tokio::time::timeout(Duration::from_secs(30), async {
            while tiered.in_flight_waiters(&lane.early.cache_key) < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second statement parks on the first one's fetch of the early object");
        for id in gate.held() {
            assert!(gate.release(id));
        }
    };
    let (leader, follower, ()) = tokio::join!(leader, follower, release);

    // Only the early object's GET is gated; after the release the two race
    // through the lane's other object, where either may be served late too.
    assert!(
        follower.late_served_bytes >= lane.early.size,
        "the follower was served the early object late: {} bytes",
        follower.late_served_bytes
    );
    assert_eq!(
        [figure(&leader), figure(&follower)],
        [(AccessIntent::Interactive, 1_914); 2],
        "each statement records what it reads when run alone"
    );
    assert_eq!(
        [
            run(&exec, lane.tenant_hash, leader_sql).await,
            run(&exec, lane.tenant_hash, follower_sql).await,
        ],
        [(AccessIntent::BulkScan, 1_914); 2],
        "each statement's next execution"
    );
}

/// An execution that fails after reading records nothing: the statement's
/// next execution, which succeeds, is still Interactive, and the one after
/// it is BulkScan. The threshold is below the catalog bytes, which every
/// execution reads before any data object, so a record of the failed
/// execution would trip it.
///
/// FLIP: recording the failed execution's read bytes reads its successor
/// as BulkScan.
#[tokio::test]
async fn a_failed_execution_records_nothing() {
    let lane = lane_with(|early, _| {
        FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("injected".to_string()))
                .with_key_contains(early)
                .with_occurrence(Occurrence::Nth(1)),
        )
    })
    .await;
    let exec = with_trigger(executor_over(lane.store.clone()), CATALOG_BYTES - 1);

    let failed = exec
        .execute(lane.tenant_hash, &sql_request(WHOLE_LANE, WINDOW))
        .await;
    assert!(
        failed.is_err(),
        "the injected GET fault fails the statement"
    );
    assert_eq!(
        lane.store
            .inner()
            .fault_count(Op::Get, FaultKind::Permanent),
        1,
        "the fault fired once"
    );

    assert_eq!(
        [
            run(&exec, lane.tenant_hash, WHOLE_LANE).await,
            run(&exec, lane.tenant_hash, WHOLE_LANE).await,
        ],
        [
            (AccessIntent::Interactive, 1_914),
            (AccessIntent::BulkScan, 1_914),
        ],
        "the two executions after the failed one"
    );
}

/// The record is per read shape: the statement text that is BulkScan over
/// [`WINDOW`] has no record over a window a hundred times shorter, and reads
/// Interactive there.
///
/// FLIP: keying the record by tenant and text alone reads the short-window
/// execution as BulkScan.
#[tokio::test]
async fn the_same_text_over_a_much_shorter_window_has_no_record() {
    let (store, tenant_hash, _) = two_object_lane().await;
    let exec = trigger_executor(store);
    let short = TimeRange {
        start_ns: 0,
        end_ns: WINDOW.end_ns / 100,
    };

    let wide = [
        run(&exec, tenant_hash, WHOLE_LANE).await,
        run(&exec, tenant_hash, WHOLE_LANE).await,
    ];
    assert_eq!(
        wide.map(|(intent, _)| intent),
        [AccessIntent::Interactive, AccessIntent::BulkScan],
        "over the wide window"
    );
    assert_eq!(
        run_accounted(&exec, tenant_hash, WHOLE_LANE, short)
            .await
            .access_intent,
        AccessIntent::Interactive,
        "the same text over the short window"
    );
}
