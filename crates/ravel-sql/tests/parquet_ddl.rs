//! Acceptance tests for `CREATE [OR REPLACE] EXTERNAL TABLE` and `DROP TABLE`
//! (ADR-2040 D2, D4): `SqlExecutor::execute_ddl` driven end to end over two
//! `MemoryStore`s, the same fixture shape `parquet_tables.rs` uses for reads.
//! Ravel's own store holds grants and manifests; the external "lake" store
//! holds the Parquet files a `CREATE` statement points at.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_memory::MemoryBudget;
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::instrument::InstrumentedStore;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_pqtable::clock::{Clock, FixedClock};
use ravel_pqtable::grants::{self, GrantsError};
use ravel_pqtable::writer::WriteError;
use ravel_query::{GetLimiter, LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DEFAULT_PARQUET_METADATA_CACHE_BYTES, DdlExecuteError, DdlOutcome, ExternalStoreMap,
    ParquetSources, SpanSegmentFetcher, SqlConfig, SqlExecutor,
};
use ravel_types::{TenantHash, TenantId};

const PROFILE: &str = "lake";
const GRANT: &str = "s3://lake/t";
const NOW: i64 = 1_700_000_000_000_000_000;
const CREATED_BY: &str = "test";

fn tenant(name: &str) -> TenantHash {
    TenantId::new(name).hash()
}

fn deadline() -> Duration {
    Duration::from_secs(5)
}

/// One row group of `id: Int64`, `name: Utf8`, `score: Float64`, a real
/// Parquet file so `snapshot_location`'s footer parse has something to read.
fn parquet_bytes(ids: &[i64], names: &[&str], scores: &[f64]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Float64Array::from(scores.to_vec())),
        ],
    )
    .expect("batch");
    let mut out = Vec::new();
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(&mut out, schema, Some(properties)).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
    Bytes::from(out)
}

struct Lake {
    ravel: Arc<InstrumentedStore<MemoryStore>>,
    lake: Arc<dyn ObjectStoreBackend>,
    executor: Arc<SqlExecutor>,
}

impl Lake {
    /// An executor whose Parquet sources reach [`PROFILE`] through `lake`,
    /// drawing on `budget` as its process memory budget.
    fn with_budget(lake: Arc<dyn ObjectStoreBackend>, budget: Arc<MemoryBudget>) -> Self {
        let ravel = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let store: Arc<dyn ObjectStoreBackend> = ravel.clone();
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let external = Arc::new(ExternalStoreMap::new(HashMap::from([(
            PROFILE.to_string(),
            Arc::clone(&lake),
        )]))) as Arc<dyn ravel_sql::ExternalStores>;
        let sources = ParquetSources::new(
            Arc::clone(&store),
            Some(external),
            Arc::new(GetLimiter::new(8).expect("limiter")),
            None,
            DEFAULT_PARQUET_METADATA_CACHE_BYTES,
        );
        let executor = Arc::new(
            SqlExecutor::new(
                catalog,
                SegmentFetcher::new(Arc::clone(&store)),
                LogSegmentFetcher::new(Arc::clone(&store)),
                SpanSegmentFetcher::new(Arc::clone(&store)),
                SqlConfig::default(),
                1 << 30,
            )
            .with_parquet_sources(sources)
            .with_process_memory_budget(budget),
        );
        Lake {
            ravel,
            lake,
            executor,
        }
    }

    fn unlimited(lake: Arc<dyn ObjectStoreBackend>) -> Self {
        Lake::with_budget(lake, Arc::new(MemoryBudget::unlimited()))
    }

    fn memory_store() -> Self {
        Lake::unlimited(Arc::new(MemoryStore::new()))
    }

    async fn put_file(&self, key: &str, bytes: Bytes) {
        self.lake
            .put(key, bytes, PutOptions::default())
            .await
            .expect("put");
    }

    async fn grant(&self, tenant: &TenantHash) {
        grants::add(
            self.ravel.inner(),
            tenant,
            PROFILE,
            GRANT,
            CREATED_BY,
            &FixedClock::new(NOW),
        )
        .await
        .expect("grant");
    }
}

#[tokio::test]
async fn create_external_table_then_read_back() {
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file(
        "t/hits/0.parquet",
        parquet_bytes(&[1, 2], &["a", "b"], &[0.5, 1.5]),
    )
    .await;

    let outcome = lake
        .executor
        .execute_ddl(
            t,
            &format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'"),
            CREATED_BY,
            deadline(),
        )
        .await
        .expect("create");

    match outcome {
        DdlOutcome::Created {
            table,
            version,
            files,
            ..
        } => {
            assert_eq!(table, "hits");
            assert_eq!(version, 1);
            assert_eq!(files, 1);
        }
        other => panic!("expected Created, got {other:?}"),
    }

    let grants = grants::list(lake.ravel.inner(), &t).await.expect("list");
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].profile, PROFILE);
}

#[tokio::test]
async fn create_if_not_exists_on_existing_table_is_a_no_op() {
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;
    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    lake.executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .expect("first create");

    let sql_if_not_exists = format!(
        "CREATE EXTERNAL TABLE IF NOT EXISTS hits STORED AS PARQUET LOCATION '{GRANT}/hits/'"
    );
    let outcome = lake
        .executor
        .execute_ddl(t, &sql_if_not_exists, CREATED_BY, deadline())
        .await
        .expect("second create is a no-op, not an error");

    assert_eq!(
        outcome,
        DdlOutcome::NoOp {
            table: "hits".to_string()
        }
    );
}

#[tokio::test]
async fn plain_create_on_existing_table_is_table_exists() {
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;
    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    lake.executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .expect("first create");

    let err = lake
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .expect_err("plain CREATE over an existing table must fail");

    assert!(
        matches!(err, DdlExecuteError::Write(WriteError::TableExists { ref table }) if table == "hits"),
        "{err:?}"
    );
}

#[tokio::test]
async fn or_replace_commits_a_new_version_over_an_existing_table() {
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;
    let create = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    lake.executor
        .execute_ddl(t, &create, CREATED_BY, deadline())
        .await
        .expect("first create");

    lake.put_file("t/hits/1.parquet", parquet_bytes(&[2], &["b"], &[1.5]))
        .await;
    let replace =
        format!("CREATE OR REPLACE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let outcome = lake
        .executor
        .execute_ddl(t, &replace, CREATED_BY, deadline())
        .await
        .expect("replace");

    match outcome {
        DdlOutcome::Created {
            table,
            version,
            files,
            ..
        } => {
            assert_eq!(table, "hits");
            assert_eq!(version, 2);
            assert_eq!(files, 2);
        }
        other => panic!("expected Created, got {other:?}"),
    }
}

#[tokio::test]
async fn drop_table_commits_a_tombstone_version() {
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;
    let create = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    lake.executor
        .execute_ddl(t, &create, CREATED_BY, deadline())
        .await
        .expect("create");

    let outcome = lake
        .executor
        .execute_ddl(t, "DROP TABLE hits", CREATED_BY, deadline())
        .await
        .expect("drop");

    assert_eq!(
        outcome,
        DdlOutcome::Dropped {
            table: "hits".to_string(),
            version: 2
        }
    );
}

#[tokio::test]
async fn drop_if_exists_on_a_missing_table_is_a_no_op() {
    let lake = Lake::memory_store();
    let t = tenant("acme");

    let outcome = lake
        .executor
        .execute_ddl(t, "DROP TABLE IF EXISTS ghost", CREATED_BY, deadline())
        .await
        .expect("drop if exists on a missing table is a no-op, not an error");

    assert_eq!(
        outcome,
        DdlOutcome::NoOp {
            table: "ghost".to_string()
        }
    );
}

#[tokio::test]
async fn drop_without_if_exists_on_a_missing_table_is_table_not_found() {
    let lake = Lake::memory_store();
    let t = tenant("acme");

    let err = lake
        .executor
        .execute_ddl(t, "DROP TABLE ghost", CREATED_BY, deadline())
        .await
        .expect_err("plain DROP on a missing table must fail");

    assert!(
        matches!(err, DdlExecuteError::Write(WriteError::TableNotFound { ref table }) if table == "ghost"),
        "{err:?}"
    );
}

#[tokio::test]
async fn tenant_isolation_a_grant_on_one_tenant_does_not_admit_another() {
    let lake = Lake::memory_store();
    let owner = tenant("acme");
    let stranger = tenant("other");
    lake.grant(&owner).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let err = lake
        .executor
        .execute_ddl(stranger, &sql, CREATED_BY, deadline())
        .await
        .expect_err("a tenant with no grant on this location must be refused");

    assert!(
        matches!(
            err,
            DdlExecuteError::Location(GrantsError::LocationNotGranted { .. })
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn ravel_bucket_location_is_refused() {
    // The grant's profile resolves to the SAME store Ravel's own manifests
    // and grants live in, so `probe_not_ravel_bucket`'s same-bucket check
    // must fire and refuse the statement before any manifest is written.
    let ravel = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let store: Arc<dyn ObjectStoreBackend> = ravel.clone();
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let external = Arc::new(ExternalStoreMap::new(HashMap::from([(
        PROFILE.to_string(),
        Arc::clone(&store),
    )]))) as Arc<dyn ravel_sql::ExternalStores>;
    let sources = ParquetSources::new(
        Arc::clone(&store),
        Some(external),
        Arc::new(GetLimiter::new(8).expect("limiter")),
        None,
        DEFAULT_PARQUET_METADATA_CACHE_BYTES,
    );
    let executor = Arc::new(
        SqlExecutor::new(
            catalog,
            SegmentFetcher::new(Arc::clone(&store)),
            LogSegmentFetcher::new(Arc::clone(&store)),
            SpanSegmentFetcher::new(Arc::clone(&store)),
            SqlConfig::default(),
            1 << 30,
        )
        .with_parquet_sources(sources)
        .with_process_memory_budget(Arc::new(MemoryBudget::unlimited())),
    );

    let t = tenant("acme");
    grants::add(
        ravel.inner(),
        &t,
        PROFILE,
        GRANT,
        CREATED_BY,
        &FixedClock::new(NOW),
    )
    .await
    .expect("grant");
    store
        .put(
            "t/hits/0.parquet",
            parquet_bytes(&[1], &["a"], &[0.5]),
            PutOptions::default(),
        )
        .await
        .expect("put");

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let err = executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .expect_err("a location inside Ravel's own bucket must be refused");

    assert!(
        matches!(err, DdlExecuteError::RavelBucketProbe { .. }),
        "{err:?}"
    );
}

#[tokio::test]
async fn precondition_probe_failure_is_refused_before_any_manifest_write() {
    // The lake store refuses the FIRST pinned read of the probe object
    // (the matching-pin read `probe_preconditions` makes to confirm the
    // store honors a pin at all), modeling a store that does not qualify
    // for pinned reads. `execute_ddl` must surface this rather than fall
    // back to an unpinned snapshot.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::FailedPrecondition).with_key_contains("hits/0.parquet"),
    );
    let lake = Arc::new(FaultStore::new(MemoryStore::new(), plan)) as Arc<dyn ObjectStoreBackend>;
    lake.put(
        "t/hits/0.parquet",
        parquet_bytes(&[1], &["a"], &[0.5]),
        PutOptions::default(),
    )
    .await
    .expect("put");

    let fixture = Lake::unlimited(lake);
    let t = tenant("acme");
    fixture.grant(&t).await;

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let err = fixture
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .expect_err("a store that refuses a matching pin must be refused");

    assert!(
        matches!(err, DdlExecuteError::PreconditionProbe { .. }),
        "{err:?}"
    );

    let grants = grants::list(fixture.ravel.inner(), &t).await.expect("list");
    assert_eq!(
        grants.len(),
        1,
        "the grant must still be the only record written"
    );
}

#[tokio::test]
async fn manifest_is_written_under_the_callers_tenant_not_any_other() {
    let lake = Lake::memory_store();
    let caller = tenant("acme");
    let someone_else = tenant("other");
    lake.grant(&caller).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    lake.executor
        .execute_ddl(caller, &sql, CREATED_BY, deadline())
        .await
        .expect("create");

    let caller_tables = ravel_pqtable::resolve::tables(lake.ravel.inner(), &caller)
        .await
        .expect("tables for caller");
    assert!(
        caller_tables.contains_key("hits"),
        "the manifest must be visible under the tenant that ran CREATE"
    );

    let other_tables = ravel_pqtable::resolve::tables(lake.ravel.inner(), &someone_else)
        .await
        .expect("tables for someone_else");
    assert!(
        !other_tables.contains_key("hits"),
        "the manifest must not be visible under any other tenant"
    );
}

#[tokio::test]
async fn memory_budget_refusal_leaves_no_manifest() {
    let lake = Arc::new(MemoryStore::new()) as Arc<dyn ObjectStoreBackend>;
    lake.put(
        "t/hits/0.parquet",
        parquet_bytes(&[1, 2, 3], &["a", "b", "c"], &[0.5, 1.5, 2.5]),
        PutOptions::default(),
    )
    .await
    .expect("put");

    let fixture = Lake::with_budget(lake, Arc::new(MemoryBudget::new(1)));
    let t = tenant("acme");
    fixture.grant(&t).await;

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let err = fixture
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .expect_err("a 1-byte process memory budget cannot decode this file's footer");

    assert!(matches!(err, DdlExecuteError::Snapshot { .. }), "{err:?}");
}

/// A store double that advances a shared [`FixedClock`] the first time its
/// `list` is called, then leaves the clock alone on every later call.
/// Standing in for a manifest-prefix LIST slow enough to burn
/// `writer::apply`'s resolve-to-put budget, without an actual wall-clock
/// sleep. Every other method delegates to `inner` unchanged.
struct ListAdvancingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    clock: FixedClock,
    advance_once_ns: i64,
    advanced: AtomicBool,
    list_calls: AtomicUsize,
}

impl ListAdvancingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>, clock: FixedClock, advance_once_ns: i64) -> Self {
        ListAdvancingStore {
            inner,
            clock,
            advance_once_ns,
            advanced: AtomicBool::new(false),
            list_calls: AtomicUsize::new(0),
        }
    }

    fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for ListAdvancingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        if !self.advanced.swap(true, Ordering::SeqCst) {
            self.clock.advance(self.advance_once_ns);
        }
        self.inner.list(prefix, page).await
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

/// Required regression test for the clock-injection fix (issue #2054,
/// blocking finding 1). `writer::apply` budgets half of `min_grace_ms`
/// between the manifest it resolves and the put it attempts
/// (`DEFAULT_MIN_GRACE_MS / 2`); when that budget is gone before the put, it
/// re-resolves instead of putting a manifest against a listing that may
/// already be stale. [`ListAdvancingStore`] stands in for a resolve whose
/// LIST took that long, by advancing the executor's own injected clock
/// immediately after the first LIST returns.
///
/// Before this fix, `execute_ddl` built its own `FixedClock::new(now_ns)`
/// internally on every call and never read the clock installed through
/// `SqlExecutor::with_clock`. Against that code, `clock.now_ns()` inside
/// `writer::apply` never advances between the resolve and the
/// remaining-budget check, so `remaining_ns` always computes to the full
/// budget and `apply` never re-resolves: this store double would see exactly
/// one `list` call before the put committed. `with_clock` existing and being
/// threaded into `writer::apply` is what makes `list_calls() == 2` below
/// observable at all.
#[tokio::test]
async fn exhausted_resolve_to_put_budget_forces_a_second_resolve() {
    let t = tenant("acme");
    let ravel = Arc::new(MemoryStore::new());
    grants::add(
        ravel.as_ref(),
        &t,
        PROFILE,
        GRANT,
        CREATED_BY,
        &FixedClock::new(NOW),
    )
    .await
    .expect("grant");

    let budget_ns = i64::try_from(ravel_sql::DEFAULT_MIN_GRACE_MS / 2)
        .expect("budget_ms fits i64")
        .saturating_mul(1_000_000);
    let apply_clock = FixedClock::new(NOW);
    let listing = Arc::new(ListAdvancingStore::new(
        ravel.clone() as Arc<dyn ObjectStoreBackend>,
        apply_clock.clone(),
        budget_ns + 1,
    ));
    let store = listing.clone() as Arc<dyn ObjectStoreBackend>;

    let lake: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    lake.put(
        "t/hits/0.parquet",
        parquet_bytes(&[1], &["a"], &[0.5]),
        PutOptions::default(),
    )
    .await
    .expect("put");

    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let external = Arc::new(ExternalStoreMap::new(HashMap::from([(
        PROFILE.to_string(),
        Arc::clone(&lake),
    )]))) as Arc<dyn ravel_sql::ExternalStores>;
    let sources = ParquetSources::new(
        Arc::clone(&store),
        Some(external),
        Arc::new(GetLimiter::new(8).expect("limiter")),
        None,
        DEFAULT_PARQUET_METADATA_CACHE_BYTES,
    );
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_parquet_sources(sources)
    .with_process_memory_budget(Arc::new(MemoryBudget::unlimited()))
    .with_clock(Arc::new(apply_clock) as Arc<dyn Clock>);

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let outcome = executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .expect("create succeeds once the second resolve lands inside budget");

    match outcome {
        DdlOutcome::Created { table, version, .. } => {
            assert_eq!(table, "hits");
            assert_eq!(version, 1);
        }
        other => panic!("expected Created, got {other:?}"),
    }

    assert_eq!(
        listing.list_calls(),
        2,
        "a budget exhausted between the first resolve and the put must force exactly one re-resolve"
    );
}

/// A store double whose `get` never returns, standing in for a grants read
/// that stalls. Every other method delegates to `inner` unchanged. None of
/// `ravel_object_store::fault`'s `ScriptedFault` variants fit this: every one
/// of them (including `ScriptedFault::Timeout`) resolves immediately with a
/// simulated error, so none can stall a caller's own `tokio::time::timeout`
/// race the way an actual stuck call would.
struct StallingStore {
    inner: Arc<dyn ObjectStoreBackend>,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for StallingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
        std::future::pending().await
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
        self.inner.capabilities()
    }
}

/// Required regression test for the whole-statement deadline fix (issue
/// #2054, blocking finding 2). Before this fix, only `snapshot_location`'s
/// own call was bounded by `deadline`; the grants read ahead of it
/// ([`grants::list`], a plain `store.get`) had no bound at all, so a grants
/// read that never returned would hang `execute_ddl` forever regardless of
/// the caller's `deadline`. [`StallingStore`] makes that read never return;
/// with the fix, `execute_ddl`'s own `tokio::time::timeout(deadline, ...)`
/// now wraps the grants read too, so this fails with
/// `DdlExecuteError::Deadline` instead of hanging, and no manifest is ever
/// written.
#[tokio::test]
async fn whole_statement_deadline_expires_during_the_grants_read_and_writes_no_manifest() {
    let t = tenant("acme");
    let ravel_inner = Arc::new(MemoryStore::new());
    grants::add(
        ravel_inner.as_ref(),
        &t,
        PROFILE,
        GRANT,
        CREATED_BY,
        &FixedClock::new(NOW),
    )
    .await
    .expect("grant");

    let stalling: Arc<dyn ObjectStoreBackend> = Arc::new(StallingStore {
        inner: ravel_inner.clone() as Arc<dyn ObjectStoreBackend>,
    });

    let lake: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    lake.put(
        "t/hits/0.parquet",
        parquet_bytes(&[1], &["a"], &[0.5]),
        PutOptions::default(),
    )
    .await
    .expect("put");

    let catalog =
        Arc::new(Catalog::new(Arc::clone(&stalling), CatalogConfig::default()).expect("catalog"));
    let external = Arc::new(ExternalStoreMap::new(HashMap::from([(
        PROFILE.to_string(),
        Arc::clone(&lake),
    )]))) as Arc<dyn ravel_sql::ExternalStores>;
    let sources = ParquetSources::new(
        Arc::clone(&stalling),
        Some(external),
        Arc::new(GetLimiter::new(8).expect("limiter")),
        None,
        DEFAULT_PARQUET_METADATA_CACHE_BYTES,
    );
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&stalling)),
        LogSegmentFetcher::new(Arc::clone(&stalling)),
        SpanSegmentFetcher::new(Arc::clone(&stalling)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_parquet_sources(sources)
    .with_process_memory_budget(Arc::new(MemoryBudget::unlimited()));

    let short_deadline = Duration::from_millis(20);
    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let err = executor
        .execute_ddl(t, &sql, CREATED_BY, short_deadline)
        .await
        .expect_err("a grants read that never returns must fail with the statement deadline");

    assert!(
        matches!(err, DdlExecuteError::Deadline { deadline } if deadline == short_deadline),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(ravel_inner.as_ref(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may be written when the grants read never completed"
    );
}
