//! Parquet tables through the SQL executor (ADR-2040 D3, D4, D6).
//!
//! Every test drives a real `SqlExecutor` with [`ParquetSources`] over two
//! `MemoryStore`s: Ravel's own store, holding the tenant's grants and table
//! manifests as `ravel-pqtable` writes them, and an external store behind one
//! credential profile, holding the Parquet files. Both are instrumented so a
//! test can say how many reads reached each.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_memory::MemoryBudget;
use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};
use ravel_parquet::ParquetReadError;
use ravel_pqtable::clock::FixedClock;
use ravel_pqtable::grants;
use ravel_pqtable::keys::manifest_key;
use ravel_pqtable::manifest::{APPLY_NONCE_LEN, Manifest, ParquetFile, encode_manifest};
use ravel_pqtable::resolve;
use ravel_pqtable::writer::{self, Intent};
use ravel_query::{
    ByteLimit, EngineConfig, GetLimiter, LogSegmentFetcher, QueryPhase, RequestBudgets,
    RequestLimit, SegmentFetcher,
};
use ravel_sql::{
    DEFAULT_PARQUET_METADATA_CACHE_BYTES, DeclaredColumn, DeclaredColumnSource, DeclaredType,
    ErrorClass, ExternalStoreMap, KeyIndexDeclaration, MAX_STATEMENT_TABLE_NAMES, MSG_PLAN,
    ParquetPlan, ParquetQueryError, ParquetSources, PinnedPlanInputs, PinnedQuery,
    SpanSegmentFetcher, SqlConfig, SqlError, SqlExecutor, SqlOutcome, StaticDeclaredColumns,
    TargetSignal,
};
use ravel_types::accounting::AccountedOp;
use ravel_types::{Signal, TenantHash, TenantId};

use util::request;

const PROFILE: &str = "lake";
const BUCKET: &str = "lake";
const GRANT: &str = "s3://lake/t";
const NOW: i64 = 1_700_000_000_000_000_000;
const MIN_GRACE_MS: u64 = 60_000;

fn tenant(name: &str) -> TenantHash {
    TenantId::new(name).hash()
}

/// One row group of `id: Int64`, `name: Utf8`, `score: Float64`.
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

/// The three files every fixture table is made of.
fn hits_files() -> Vec<(&'static str, Bytes)> {
    vec![
        (
            "t/hits/0.parquet",
            parquet_bytes(&[1, 2], &["a", "b"], &[0.5, 1.5]),
        ),
        (
            "t/hits/1.parquet",
            parquet_bytes(&[3, 4], &["c", "a"], &[2.5, 3.5]),
        ),
        (
            "t/hits/2.parquet",
            parquet_bytes(&[5, 6], &["b", "c"], &[4.5, 5.5]),
        ),
    ]
}

/// A base epoch second whose UTC minute, hour and year are known fixed
/// points (2023-11-14T22:13:20Z: minute 13, hour 22, year 2023), so the
/// `EXTRACT`/`date_part` equivalence tests below exercise real field values
/// rather than a single magic timestamp.
const CLICKBENCH_T0: i64 = 1_700_000_000;

/// One row group of `UserID: Int64`, `EventTime: Int64` (seconds since
/// epoch, as ClickBench's `hits` table stores it), `SearchPhrase: Utf8` --
/// the three columns ClickBench Q19 (issue #2458) selects.
fn clickbench_bytes(user_ids: &[i64], event_times: &[i64], phrases: &[&str]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("UserID", DataType::Int64, false),
        Field::new("EventTime", DataType::Int64, false),
        Field::new("SearchPhrase", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(user_ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(event_times.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(phrases.to_vec())),
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

/// Six rows across three files: `(UserID, EventTime, SearchPhrase)`.
/// `(1, CLICKBENCH_T0, "a")` appears three times (minute 13, twice at the
/// exact same second and once five seconds later, same minute), `(2,
/// CLICKBENCH_T0 + 120, "b")` twice (minute 15, two minutes later), and `(3,
/// CLICKBENCH_T0 + 240, "c")` once (minute 17, four minutes later) -- so
/// `GROUP BY "UserID", EXTRACT(minute FROM ...), "SearchPhrase"` has exactly
/// three groups of sizes 3, 2 and 1.
fn clickbench_files() -> Vec<(&'static str, Bytes)> {
    let t0 = CLICKBENCH_T0;
    vec![
        (
            "t/hits/0.parquet",
            clickbench_bytes(&[1, 1], &[t0, t0], &["a", "a"]),
        ),
        (
            "t/hits/1.parquet",
            clickbench_bytes(&[1, 2], &[t0 + 5, t0 + 120], &["a", "b"]),
        ),
        (
            "t/hits/2.parquet",
            clickbench_bytes(&[2, 3], &[t0 + 120, t0 + 240], &["b", "c"]),
        ),
    ]
}

struct Lake {
    ravel: Arc<InstrumentedStore<MemoryStore>>,
    lake: Arc<InstrumentedStore<MemoryStore>>,
    executor: Arc<SqlExecutor>,
}

impl Lake {
    /// An executor whose Parquet sources reach [`PROFILE`] through the lake
    /// store, or reach no profile at all when `configured` is false.
    fn new(configured: bool, config: SqlConfig) -> Self {
        let lake = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let external = configured.then(|| {
            Arc::new(ExternalStoreMap::new(HashMap::from([(
                PROFILE.to_string(),
                Arc::clone(&lake) as Arc<dyn ObjectStoreBackend>,
            )]))) as Arc<dyn ravel_sql::ExternalStores>
        });
        Lake::with_external(lake, external, config)
    }

    /// An executor whose Parquet sources read through `external`; `lake` is
    /// where [`Self::put_file`] writes.
    fn with_external(
        lake: Arc<InstrumentedStore<MemoryStore>>,
        external: Option<Arc<dyn ravel_sql::ExternalStores>>,
        config: SqlConfig,
    ) -> Self {
        Lake::with_budget(lake, external, config, Arc::new(MemoryBudget::unlimited()))
    }

    /// A configured executor over `config` drawing on `budget`.
    fn budgeted(config: SqlConfig, budget: Arc<MemoryBudget>) -> Self {
        let lake = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let external = Arc::new(ExternalStoreMap::new(HashMap::from([(
            PROFILE.to_string(),
            Arc::clone(&lake) as Arc<dyn ObjectStoreBackend>,
        )]))) as Arc<dyn ravel_sql::ExternalStores>;
        Lake::with_budget(lake, Some(external), config, budget)
    }

    /// An executor drawing on `budget`, the process memory budget a server
    /// shares between its fetchers and its SQL pool.
    fn with_budget(
        lake: Arc<InstrumentedStore<MemoryStore>>,
        external: Option<Arc<dyn ravel_sql::ExternalStores>>,
        config: SqlConfig,
        budget: Arc<MemoryBudget>,
    ) -> Self {
        let ravel = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let store: Arc<dyn ObjectStoreBackend> = ravel.clone();
        Lake::assemble(ravel, store, lake, external, config, budget)
    }

    /// A configured executor over `config` whose every request to either
    /// store is also written to the returned log, in issue order.
    fn recorded(config: SqlConfig) -> (Self, RequestLog) {
        let log = RequestLog::default();
        let lake = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let ravel = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let lake_seen = Arc::new(Recorded {
            name: "lake",
            inner: Arc::clone(&lake) as Arc<dyn ObjectStoreBackend>,
            log: Arc::clone(&log),
        }) as Arc<dyn ObjectStoreBackend>;
        let ravel_seen = Arc::new(Recorded {
            name: "ravel",
            inner: Arc::clone(&ravel) as Arc<dyn ObjectStoreBackend>,
            log: Arc::clone(&log),
        }) as Arc<dyn ObjectStoreBackend>;
        let external = Arc::new(ExternalStoreMap::new(HashMap::from([(
            PROFILE.to_string(),
            lake_seen,
        )]))) as Arc<dyn ravel_sql::ExternalStores>;
        let lake = Lake::assemble(
            ravel,
            ravel_seen,
            lake,
            Some(external),
            config,
            Arc::new(MemoryBudget::unlimited()),
        );
        (lake, log)
    }

    /// A configured executor whose tenant declares `declared` with the KEY_IDX
    /// declaration `key_index`, and whose logs fetcher plans every object
    /// (a zero whole-object bound), so a key equality probes KEY_IDX, with a
    /// [`KEYED_SUFFIX_LEN`] tail probe. Every GET it sends to Ravel's store is
    /// written to the returned log with its range.
    fn keyed(declared: Vec<DeclaredColumn>, key_index: KeyIndexDeclaration) -> (Self, RangeLog) {
        let log = RangeLog::default();
        let lake = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let ravel = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let store = Arc::new(RangeRecorded {
            inner: Arc::clone(&ravel) as Arc<dyn ObjectStoreBackend>,
            log: Arc::clone(&log),
        }) as Arc<dyn ObjectStoreBackend>;
        let external = Arc::new(ExternalStoreMap::new(HashMap::from([(
            PROFILE.to_string(),
            Arc::clone(&lake) as Arc<dyn ObjectStoreBackend>,
        )]))) as Arc<dyn ravel_sql::ExternalStores>;
        let logs = LogSegmentFetcher::new(Arc::clone(&store))
            .with_block_range_threshold(0)
            .with_suffix_len(KEYED_SUFFIX_LEN);
        let lake = Lake::assemble_with(
            ravel,
            store,
            lake,
            Some(external),
            SqlConfig::default(),
            Arc::new(MemoryBudget::unlimited()),
            logs,
            Arc::new(StaticDeclaredColumns::new(declared).with_key_index(key_index)),
        );
        (lake, log)
    }

    /// An executor reading Ravel's store through `store`, which reaches
    /// `ravel`.
    fn assemble(
        ravel: Arc<InstrumentedStore<MemoryStore>>,
        store: Arc<dyn ObjectStoreBackend>,
        lake: Arc<InstrumentedStore<MemoryStore>>,
        external: Option<Arc<dyn ravel_sql::ExternalStores>>,
        config: SqlConfig,
        budget: Arc<MemoryBudget>,
    ) -> Self {
        let logs = LogSegmentFetcher::new(Arc::clone(&store));
        Lake::assemble_with(
            ravel,
            store,
            lake,
            external,
            config,
            budget,
            logs,
            Arc::new(StaticDeclaredColumns::empty()),
        )
    }

    /// [`Self::assemble`] with the logs fetcher `logs` and the declared-column
    /// source `declared`.
    #[allow(clippy::too_many_arguments)]
    fn assemble_with(
        ravel: Arc<InstrumentedStore<MemoryStore>>,
        store: Arc<dyn ObjectStoreBackend>,
        lake: Arc<InstrumentedStore<MemoryStore>>,
        external: Option<Arc<dyn ravel_sql::ExternalStores>>,
        config: SqlConfig,
        budget: Arc<MemoryBudget>,
        logs: LogSegmentFetcher,
        declared: Arc<dyn DeclaredColumnSource>,
    ) -> Self {
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let sources = ParquetSources::new(
            Arc::clone(&store),
            external,
            Arc::new(GetLimiter::new(8).expect("limiter")),
            None,
            DEFAULT_PARQUET_METADATA_CACHE_BYTES,
        );
        let executor = Arc::new(
            SqlExecutor::new(
                catalog,
                SegmentFetcher::new(Arc::clone(&store)),
                logs,
                SpanSegmentFetcher::new(Arc::clone(&store)),
                config,
                1 << 30,
            )
            .with_parquet_sources(sources)
            .with_process_memory_budget(budget)
            .with_declared_column_source(declared),
        );
        Lake {
            ravel,
            lake,
            executor,
        }
    }

    fn configured() -> Self {
        Lake::new(true, SqlConfig::default())
    }

    async fn put_file(&self, key: &str, bytes: Bytes) -> ParquetFile {
        let size = bytes.len() as u64;
        let mut word = [0u8; 4];
        word.copy_from_slice(&bytes[bytes.len() - 8..bytes.len() - 4]);
        let footer_len = u32::from_le_bytes(word);
        let put = self
            .lake
            .inner()
            .put(key, bytes, PutOptions::default())
            .await
            .expect("put");
        ParquetFile {
            profile: PROFILE.to_string(),
            bucket: BUCKET.to_string(),
            key: key.as_bytes().to_vec(),
            size,
            etag: put.etag.0,
            version: String::new(),
            row_count: 2,
            footer_len,
        }
    }

    async fn grant(&self, tenant: &TenantHash) {
        grants::add(
            self.ravel.inner(),
            tenant,
            PROFILE,
            GRANT,
            "test",
            &FixedClock::new(NOW),
        )
        .await
        .expect("grant");
    }

    async fn create(&self, tenant: &TenantHash, table: &str, files: Vec<ParquetFile>) {
        writer::apply(
            self.ravel.inner(),
            tenant,
            table,
            Intent::Create {
                if_not_exists: false,
                location: format!("{GRANT}/{table}/"),
                grant: GRANT.to_string(),
                files,
                options: BTreeMap::new(),
                created_by: "test".to_string(),
                statement: format!("CREATE EXTERNAL TABLE {table} ..."),
            },
            &FixedClock::new(NOW),
            MIN_GRACE_MS,
        )
        .await
        .expect("create");
    }

    /// Replace `table` with one made of `files`: a newer manifest version.
    #[cfg(feature = "flight-sql")]
    async fn replace(&self, tenant: &TenantHash, table: &str, files: Vec<ParquetFile>) {
        writer::apply(
            self.ravel.inner(),
            tenant,
            table,
            Intent::CreateOrReplace {
                location: format!("{GRANT}/{table}/"),
                grant: GRANT.to_string(),
                files,
                options: BTreeMap::new(),
                created_by: "test".to_string(),
                statement: format!("CREATE OR REPLACE EXTERNAL TABLE {table} ..."),
            },
            &FixedClock::new(NOW),
            MIN_GRACE_MS,
        )
        .await
        .expect("replace");
    }

    /// Grant [`GRANT`] to `tenant` and create `hits` over [`hits_files`].
    async fn hits_for(&self, tenant: &TenantHash) {
        let mut files = Vec::new();
        for (key, bytes) in hits_files() {
            files.push(self.put_file(key, bytes).await);
        }
        self.grant(tenant).await;
        self.create(tenant, "hits", files).await;
    }

    /// Grant [`GRANT`] to `tenant` and create `hits` over
    /// [`clickbench_files`]: `UserID`/`EventTime`/`SearchPhrase`, the
    /// ClickBench Q19 (issue #2458) shape.
    async fn clickbench_hits_for(&self, tenant: &TenantHash) {
        let mut files = Vec::new();
        for (key, bytes) in clickbench_files() {
            files.push(self.put_file(key, bytes).await);
        }
        self.grant(tenant).await;
        self.create(tenant, "hits", files).await;
    }

    async fn execute(&self, tenant: &TenantHash, sql: &str) -> Result<SqlOutcome, SqlError> {
        self.executor.execute(*tenant, &request(sql)).await
    }

    fn gets(store: &InstrumentedStore<MemoryStore>) -> u64 {
        store.metrics().snapshot().op(StoreOp::Get).calls
    }

    fn lists(store: &InstrumentedStore<MemoryStore>) -> u64 {
        store.metrics().snapshot().op(StoreOp::List).calls
    }

    /// The (Ravel, lake) GET counts.
    fn all_gets(&self) -> (u64, u64) {
        (Self::gets(&self.ravel), Self::gets(&self.lake))
    }
}

/// Every request an executor sent to a [`Recorded`] store, in issue order,
/// each as `<store> <op> <key>`.
type RequestLog = Arc<Mutex<Vec<String>>>;

/// A store that writes each read it forwards to a shared [`RequestLog`].
/// Writes are not recorded: the fixture stages through the inner stores.
struct Recorded {
    name: &'static str,
    inner: Arc<dyn ObjectStoreBackend>,
    log: RequestLog,
}

impl Recorded {
    fn note(&self, op: &str, key: &str) {
        self.log
            .lock()
            .expect("request log")
            .push(format!("{} {op} {key}", self.name));
    }
}

#[async_trait]
impl ObjectStoreBackend for Recorded {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.note("GET", key);
        self.inner.get(key, range).await
    }
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.note("GET", key);
        self.inner.get_pinned(key, range, pin).await
    }
    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.note("GET", key);
        self.inner.get_with_pin(key, range).await
    }
    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.note("HEAD", key);
        self.inner.pin_of(key).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.note("HEAD", key);
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.note("LIST", prefix);
        self.inner.list(prefix, page).await
    }
    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.note("LIST", prefix);
        self.inner.list_after(prefix, start_after, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.note("LIST", prefix);
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Every GET a [`RangeRecorded`] store forwarded, as its key and range, in
/// issue order.
type RangeLog = Arc<Mutex<Vec<(String, GetRange)>>>;

/// A store that writes each GET it forwards, with its range, to a shared
/// [`RangeLog`].
struct RangeRecorded {
    inner: Arc<dyn ObjectStoreBackend>,
    log: RangeLog,
}

impl RangeRecorded {
    fn note(&self, key: &str, range: GetRange) {
        self.log
            .lock()
            .expect("range log")
            .push((key.to_string(), range));
    }
}

#[async_trait]
impl ObjectStoreBackend for RangeRecorded {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.note(key, range);
        self.inner.get(key, range).await
    }
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.note(key, range);
        self.inner.get_pinned(key, range, pin).await
    }
    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.note(key, range);
        self.inner.get_with_pin(key, range).await
    }
    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.inner.pin_of(key).await
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

/// The Parquet refusal `err` carries, if it is one.
fn parquet_error(err: &SqlError) -> Option<&ParquetQueryError> {
    match err {
        SqlError::Parquet(parquet) => Some(parquet.as_ref()),
        _ => None,
    }
}

/// Every row of `outcome`, each rendered as `v|v|...`, in result order.
fn rows(outcome: &SqlOutcome) -> Vec<String> {
    batch_rows(outcome.output.batches())
}

/// [`rows`] of a stream's batches.
fn batch_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|column| array_value_to_string(batch.column(column), row).expect("cell"))
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

/// The reachability test at the executor: a filtered SELECT over a
/// three-file table returns the exact rows, reads the manifest and grants in
/// the Resolve phase, footers in Probe, and data in Scan.
#[tokio::test]
async fn a_parquet_table_answers_a_filtered_select_with_its_reads_split_by_phase() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let (ravel_before, lake_before) = lake.all_gets();

    let outcome = lake
        .execute(&acme, "SELECT id, name FROM hits WHERE id > 2 ORDER BY id")
        .await
        .expect("query");
    assert_eq!(rows(&outcome), vec!["3|c", "4|a", "5|b", "6|c"]);
    assert_eq!(outcome.target, TargetSignal::Parquet);

    let phases = &outcome.phase_accounting;
    let resolve = phases.phase(QueryPhase::Resolve);
    assert_eq!(resolve.s3_requests(AccountedOp::List), 1, "one LIST of v/");
    assert_eq!(
        resolve.s3_requests(AccountedOp::Get),
        2,
        "the newest manifest and the grants record"
    );
    let probe = phases.phase(QueryPhase::Probe);
    assert_eq!(
        probe.s3_requests(AccountedOp::Get),
        3,
        "a footer for each of three files and no page index"
    );
    assert_eq!(
        probe.cache_hits, 1,
        "the scan reuses the footer the table was built from"
    );
    let scan = phases.phase(QueryPhase::Scan);
    assert!(scan.s3_requests(AccountedOp::Get) > 0);

    let (ravel_after, lake_after) = lake.all_gets();
    assert_eq!(ravel_after - ravel_before, 2);
    assert_eq!(
        lake_after - lake_before,
        probe.s3_requests(AccountedOp::Get) + scan.s3_requests(AccountedOp::Get),
        "every lake GET is charged to Probe or Scan"
    );
}

/// A Parquet table has no `stats.signals` entry, so a Parquet-only statement
/// reports none while its statement total still holds every read.
#[tokio::test]
async fn a_parquet_only_statement_has_no_signals_entry() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;

    let outcome = lake
        .execute(&acme, "SELECT count(*) FROM hits")
        .await
        .expect("query");
    assert_eq!(outcome.target, TargetSignal::Parquet);
    assert!(outcome.signals.is_empty(), "{:?}", outcome.signals);
    assert!(outcome.accounting.total_s3_requests() > 0);
}

/// `LIMIT n` caps a plain projection over a three-file Parquet table of six
/// rows, the shape that over-returned on `logs` (issue #2616).
#[tokio::test]
async fn limit_caps_rows_of_a_plain_projection() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    for (sql, want) in [
        ("SELECT name FROM hits LIMIT 1", 1),
        ("SELECT name FROM hits LIMIT 3", 3),
        ("SELECT * FROM hits LIMIT 2", 2),
        ("SELECT name FROM hits LIMIT 9", 6),
    ] {
        let outcome = lake
            .execute(&acme, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} must execute: {e}"));
        assert_eq!(outcome.output.num_rows(), want, "{sql}");
    }
}

/// Issue #2458: `EXTRACT(field FROM expr)` plans (via `DatetimeFunctionPlanner`,
/// registered in `crate::session::build_session`) into the already-admitted
/// `date_part('field', expr)` call it rewrites to, and returns identical
/// rows, over a Parquet table -- the same entry point ClickBench Q19 plans
/// on. Checked for `minute`, `hour` and `year`.
#[tokio::test]
async fn extract_matches_date_part_on_parquet_table() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.clickbench_hits_for(&acme).await;

    for field in ["minute", "hour", "year"] {
        let date_part_sql = format!(
            "SELECT \"UserID\", date_part('{field}', to_timestamp_seconds(\"EventTime\")) FROM hits ORDER BY \"UserID\", \"EventTime\""
        );
        let extract_sql = format!(
            "SELECT \"UserID\", EXTRACT({field} FROM to_timestamp_seconds(\"EventTime\")) FROM hits ORDER BY \"UserID\", \"EventTime\""
        );

        let expected = lake
            .execute(&acme, &date_part_sql)
            .await
            .unwrap_or_else(|e| panic!("{date_part_sql} must plan and execute: {e}"));
        let actual = lake
            .execute(&acme, &extract_sql)
            .await
            .unwrap_or_else(|e| panic!("{extract_sql} must plan and execute: {e}"));

        let expected_rows = rows(&expected);
        let actual_rows = rows(&actual);
        assert_eq!(
            actual_rows, expected_rows,
            "EXTRACT({field} FROM ...) must match date_part('{field}', ...) row for row"
        );
        assert_eq!(
            actual_rows.len(),
            6,
            "all six fixture rows must be returned"
        );
    }
}

/// ClickBench Q19's exact text (issue #2458), over the
/// [`Lake::clickbench_hits_for`] fixture: three groups of `(UserID, minute,
/// SearchPhrase)`, of sizes 3, 2 and 1, ordered by `COUNT(*)` descending.
#[tokio::test]
async fn clickbench_q19_plans_and_groups_by_extracted_minute() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.clickbench_hits_for(&acme).await;

    let outcome = lake
        .execute(
            &acme,
            "SELECT \"UserID\", extract(minute FROM to_timestamp_seconds(\"EventTime\")) AS m, \"SearchPhrase\", COUNT(*) FROM hits GROUP BY \"UserID\", m, \"SearchPhrase\" ORDER BY COUNT(*) DESC LIMIT 10;",
        )
        .await
        .expect("Q19 must plan and execute");

    assert_eq!(
        rows(&outcome),
        vec!["1|13|a|3", "2|15|b|2", "3|17|c|1"],
        "three groups, largest first: (UserID 1, minute 13, \"a\") x3, \
         (UserID 2, minute 15, \"b\") x2, (UserID 3, minute 17, \"c\") x1"
    );
}

/// Two rows of one column per Arrow type the JSON encoder had no arm for
/// before issues #2390, #2496 and #2512, written to Parquet with the Arrow
/// schema embedded so the reader restores each type. The reader hands both
/// `Binary` and `LargeBinary` back as `BinaryView`, so one binary column
/// covers both. The #2512 columns are null in the second row.
fn typed_bytes() -> Bytes {
    use datafusion::arrow::array::{
        BinaryArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array, Decimal128Array,
        Decimal256Array, DictionaryArray, DurationMillisecondArray, FixedSizeListArray,
        Float16Array, Int8Array, Int16Array, IntervalDayTimeArray, LargeListArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray, UInt16Array,
    };
    use datafusion::arrow::datatypes::{
        ArrowPrimitiveType, Float16Type, Int32Type, IntervalDayTime, IntervalUnit, TimeUnit,
        UInt16Type, i256,
    };
    type F16 = <Float16Type as ArrowPrimitiveType>::Native;

    let beyond_i128 = i256::from_i128(i128::MAX)
        .checked_add(i256::ONE)
        .expect("in range");

    let schema = Arc::new(Schema::new(vec![
        Field::new("i8", DataType::Int8, false),
        Field::new("i16", DataType::Int16, false),
        Field::new("u16", DataType::UInt16, false),
        Field::new("d32", DataType::Date32, false),
        Field::new("d64", DataType::Date64, false),
        Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), false),
        Field::new(
            "ts_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "ts_us",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("dec", DataType::Decimal128(10, 2), false),
        Field::new("bin", DataType::Binary, false),
        Field::new("dec256", DataType::Decimal256(40, 2), false),
        Field::new("f16", DataType::Float16, false),
        Field::new("t32_s", DataType::Time32(TimeUnit::Second), false),
        Field::new("t32_ms", DataType::Time32(TimeUnit::Millisecond), false),
        Field::new("t64_us", DataType::Time64(TimeUnit::Microsecond), false),
        Field::new("t64_ns", DataType::Time64(TimeUnit::Nanosecond), false),
        Field::new("dec32", DataType::Decimal32(9, 2), false),
        Field::new("dec64", DataType::Decimal64(18, 3), false),
        Field::new("dur", DataType::Duration(TimeUnit::Millisecond), false),
        Field::new("iv", DataType::Interval(IntervalUnit::DayTime), true),
        Field::new(
            "ll",
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true))),
            true,
        ),
        Field::new(
            "fsl",
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 2),
            true,
        ),
        Field::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            true,
        ),
    ]));
    let dict: DictionaryArray<UInt16Type> = vec![Some("minus"), None].into_iter().collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int8Array::from(vec![-8, 8])) as ArrayRef,
            Arc::new(Int16Array::from(vec![-1_600, 1_600])),
            Arc::new(UInt16Array::from(vec![0, 65_535])),
            Arc::new(Date32Array::from(vec![-1, 19_675])),
            Arc::new(Date64Array::from(vec![-86_400_000, 11_016 * 86_400_000])),
            Arc::new(TimestampSecondArray::from(vec![-1, CLICKBENCH_T0])),
            Arc::new(TimestampMillisecondArray::from(vec![
                -1,
                CLICKBENCH_T0 * 1_000 + 123,
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                -1,
                CLICKBENCH_T0 * 1_000_000 + 123_456,
            ])),
            Arc::new(
                Decimal128Array::from(vec![-5, 12_345])
                    .with_precision_and_scale(10, 2)
                    .expect("decimal"),
            ),
            Arc::new(BinaryArray::from(vec![&[0xbe_u8][..], &[0xef][..]])),
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(-5), beyond_i128])
                    .with_precision_and_scale(40, 2)
                    .expect("decimal"),
            ),
            Arc::new(Float16Array::from(vec![F16::NAN, F16::from_f64(1.5)])),
            Arc::new(Time32SecondArray::from(vec![0, 86_399])),
            Arc::new(Time32MillisecondArray::from(vec![1, 3_661_123])),
            Arc::new(Time64MicrosecondArray::from(vec![1, 3_661_123_456])),
            Arc::new(Time64NanosecondArray::from(vec![1, 86_399_999_999_999])),
            Arc::new(
                Decimal32Array::from(vec![-5, 999_999_999])
                    .with_precision_and_scale(9, 2)
                    .expect("decimal"),
            ),
            Arc::new(
                Decimal64Array::from(vec![-1, 999_999_999_999_999_999])
                    .with_precision_and_scale(18, 3)
                    .expect("decimal"),
            ),
            Arc::new(DurationMillisecondArray::from(vec![-1, 1_500])),
            Arc::new(IntervalDayTimeArray::from(vec![
                Some(IntervalDayTime::new(-2, -1_500)),
                None,
            ])),
            Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![Some(vec![Some(-1), None]), None],
            )),
            Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![Some(vec![Some(-1), Some(2)]), None],
                2,
            )),
            Arc::new(dict),
        ],
    )
    .expect("batch");
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
    Bytes::from(out)
}

/// Issues #2390, #2496 and #2512: a Parquet table's Int8, Int16, UInt16,
/// Date32, Date64, second/millisecond/microsecond timestamp, Decimal128,
/// binary, Decimal256, Float16, time-of-day, Decimal32, Decimal64, duration,
/// day-time interval, large list, fixed-size list and UInt16-keyed dictionary
/// columns encode through the JSON output path the SQL endpoint serves,
/// instead of failing with "no JSON encoding for arrow type".
#[tokio::test]
async fn parquet_column_types_encode_as_json() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    let file = lake.put_file("t/typed/0.parquet", typed_bytes()).await;
    lake.grant(&acme).await;
    lake.create(&acme, "typed", vec![file]).await;

    let outcome = lake
        .execute(&acme, "SELECT * FROM typed ORDER BY i16")
        .await
        .expect("the statement runs");
    let json = outcome
        .output
        .to_json()
        .expect("every column encodes as JSON");

    let types: Vec<(String, String)> = json["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .map(|c| {
            (
                c["name"].as_str().expect("name").to_string(),
                c["type"].as_str().expect("type").to_string(),
            )
        })
        .collect();
    let expected_types = [
        ("i8", "Int8"),
        ("i16", "Int16"),
        ("u16", "UInt16"),
        ("d32", "Date32"),
        ("d64", "Date64"),
        ("ts_s", "Timestamp(s)"),
        ("ts_ms", "Timestamp(ms)"),
        ("ts_us", "Timestamp(µs)"),
        ("dec", "Decimal128(10, 2)"),
        ("bin", "BinaryView"),
        ("dec256", "Decimal256(40, 2)"),
        ("f16", "Float16"),
        ("t32_s", "Time32(s)"),
        ("t32_ms", "Time32(ms)"),
        ("t64_us", "Time64(µs)"),
        ("t64_ns", "Time64(ns)"),
        ("dec32", "Decimal32(9, 2)"),
        ("dec64", "Decimal64(18, 3)"),
        ("dur", "Duration(ms)"),
        ("iv", "Interval(DayTime)"),
        ("ll", "LargeList(Int32)"),
        ("fsl", "FixedSizeList(2 x Int32)"),
        ("dict", "Dictionary(UInt16, Utf8)"),
    ];
    assert_eq!(
        types,
        expected_types
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect::<Vec<_>>(),
        "the Parquet round trip keeps each type the rows below exercise"
    );

    assert_eq!(
        json["rows"],
        serde_json::json!([
            [
                -8,
                -1_600,
                0,
                "1969-12-31",
                "1969-12-31",
                -1_000_000_000,
                -1_000_000,
                -1_000,
                "-0.05",
                "be",
                "-0.05",
                "NaN",
                0,
                1_000_000,
                1_000,
                1,
                "-0.05",
                "-0.001",
                -1_000_000,
                { "months": 0, "days": -2, "nanoseconds": -1_500_000_000_i64 },
                [-1, null],
                [-1, 2],
                "minus"
            ],
            [
                8,
                1_600,
                65_535,
                "2023-11-14",
                "2000-02-29",
                1_700_000_000_000_000_000_i64,
                1_700_000_000_123_000_000_i64,
                1_700_000_000_123_456_000_i64,
                "123.45",
                "ef",
                "1701411834604692317316873037158841057.28",
                1.5,
                86_399_000_000_000_i64,
                3_661_123_000_000_i64,
                3_661_123_456_000_i64,
                86_399_999_999_999_i64,
                "9999999.99",
                "999999999999999.999",
                1_500_000_000,
                null,
                null,
                null,
                null
            ]
        ])
    );
}

/// The unknown-table failure of `sql` for `tenant`, which every name that is
/// no table of that tenant must reproduce.
async fn unknown_table_error(lake: &Lake, tenant: &TenantHash, sql: &str) -> SqlError {
    let err = lake.execute(tenant, sql).await.expect_err("unknown table");
    assert!(matches!(err, SqlError::Plan(_)), "{err}");
    assert_eq!(err.client_message(), MSG_PLAN);
    err
}

/// Another tenant's table is no table of this one: the same planning failure
/// as a name nobody created, not an authorization error that would say the
/// name exists.
#[tokio::test]
async fn another_tenants_table_is_an_unknown_table() {
    let lake = Lake::configured();
    let (acme, globex) = (tenant("acme"), tenant("globex"));
    lake.hits_for(&globex).await;
    lake.grant(&acme).await;

    let theirs = lake
        .execute(&globex, "SELECT count(*) FROM hits")
        .await
        .expect("the owner reads it");
    assert_eq!(rows(&theirs), vec!["6"]);

    let lake_before = Lake::gets(&lake.lake);
    let err = unknown_table_error(&lake, &acme, "SELECT count(*) FROM hits").await;
    let nosuch = unknown_table_error(&lake, &acme, "SELECT count(*) FROM nosuch").await;
    assert_eq!(err.class(), nosuch.class());
    assert_eq!(
        Lake::gets(&lake.lake),
        lake_before,
        "nothing of globex's is read"
    );
}

/// The event times of the logs [`hits_and_logs`] publishes: one record per
/// `hits` id from 1 to 4, each with the body `line <ts>`.
const LOG_TS: [i64; 4] = [1, 2, 3, 4];

/// `hits` joined to the logs record whose body names its id.
const HITS_JOIN_LOGS: &str = "SELECT h.id, l.body FROM hits h JOIN logs l \
     ON l.body = concat('line ', CAST(h.id AS VARCHAR)) ORDER BY h.id";

/// `hits` for `acme`, and one logs object for `acme` holding a record at each
/// of [`LOG_TS`]. Returns that object's commit token and data key.
async fn hits_and_logs(lake: &Lake) -> (ravel_types::CommitToken, String) {
    let acme = util::tenant_id("acme");
    lake.hits_for(&acme.hash()).await;
    util::publish_logs(lake.ravel.as_ref(), &acme, 0, &LOG_TS).await
}

/// The (Ravel GET, Ravel LIST, lake GET) counts `sql` adds on a fresh
/// [`hits_and_logs`] lake, and its outcome.
async fn fresh_reads(sql: &str) -> ((u64, u64, u64), SqlOutcome) {
    let lake = Lake::configured();
    hits_and_logs(&lake).await;
    let before = reads(&lake);
    let outcome = lake.execute(&tenant("acme"), sql).await.expect(sql);
    let after = reads(&lake);
    (
        (after.0 - before.0, after.1 - before.1, after.2 - before.2),
        outcome,
    )
}

/// A Parquet table and a signal table in one statement (ADR-2904 decision 11)
/// return the exact rows, with the Parquet table on either side. Each
/// statement's reads are exactly the Parquet table's alone plus the logs
/// lane's alone, each measured from a fresh store: the mixed resolve reads
/// both and nothing more. The statement reports one `stats.signals` entry,
/// for `logs`.
///
/// FLIP: a mixed resolve that skips the signal lanes gives `logs` an empty
/// snapshot, so the join returns no row and the counts lose the logs lane's
/// LIST and GETs.
#[tokio::test]
async fn a_parquet_table_joined_with_logs_returns_exact_rows() {
    let cases = [
        (
            HITS_JOIN_LOGS,
            vec!["1|line 1", "2|line 2", "3|line 3", "4|line 4"],
            "SELECT id FROM hits",
        ),
        (
            "SELECT body FROM logs WHERE body IN \
             (SELECT concat('line ', CAST(id AS VARCHAR)) FROM hits WHERE name = 'a') \
             ORDER BY body",
            vec!["line 1", "line 4"],
            "SELECT id FROM hits WHERE name = 'a'",
        ),
    ];
    let (logs, _) = fresh_reads("SELECT body FROM logs").await;
    for (sql, expected, parquet_alone) in cases {
        let (mixed, outcome) = fresh_reads(sql).await;
        assert_eq!(rows(&outcome), expected, "{sql}");
        assert_eq!(outcome.target, TargetSignal::Logs, "{sql}");
        assert_eq!(
            outcome
                .signals
                .iter()
                .map(|signal| signal.table)
                .collect::<Vec<_>>(),
            vec!["logs"],
            "{sql}"
        );
        let (parquet, _) = fresh_reads(parquet_alone).await;
        assert_eq!(
            mixed,
            (parquet.0 + logs.0, parquet.1 + logs.1, parquet.2 + logs.2),
            "{sql}: (Ravel GET, Ravel LIST, lake GET) of {parquet_alone:?} {parquet:?} \
             plus the logs lane's {logs:?}"
        );
    }
}

/// `(status_code, uid, latency_ms)` of each record of the two keyed logs
/// objects: `status_code = 500` is in object 0 only, twice joining a `hits`
/// id and once not. Object 1 holds 400 and 600, so its block min/max keep it
/// for `status_code = 500` and only KEY_IDX can drop it.
const KEYED_LOGS: [&[(i64, i64, i64)]; 2] = [
    &[
        (500, 1, 30),
        (200, 2, 5),
        (500, 4, 12),
        (200, 9, 1),
        (500, 9, 2),
    ],
    &[(400, 3, 7), (600, 6, 3)],
];

/// The keyed fixture's tail probe length: shorter than what follows KEY_IDX
/// in either object, so every KEY_IDX read is a range GET of its own.
const KEYED_SUFFIX_LEN: u64 = 256;

/// The keyed fixture's declared columns. The tenant indexes `status_code` in
/// KEY_IDX and neither of the others.
fn keyed_declared() -> (Vec<DeclaredColumn>, KeyIndexDeclaration) {
    (
        vec![
            DeclaredColumn::new("status_code", DeclaredType::I64),
            DeclaredColumn::new("uid", DeclaredType::I64),
            DeclaredColumn::new("latency_ms", DeclaredType::I64),
        ],
        KeyIndexDeclaration {
            columns: vec!["status_code".to_string()],
            trace_id: false,
        },
    )
}

/// Write `records` as one RLOG object whose KEY_IDX indexes `status_code`,
/// and publish its logs commit record. Returns the data key and the bytes.
async fn publish_keyed_logs(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    records: &[(i64, i64, i64)],
) -> (String, Bytes) {
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_logseg::key_index::KeyType;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};

    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("web".to_string()),
    )];
    let writer_id = uuid::Uuid::from_u128(0x2914_0000 + index as u128);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: index as u64 + 1,
        },
    )
    .with_key_index_fields(vec![("status_code".to_string(), KeyType::I64)]);
    let ts: Vec<i64> = (0..records.len())
        .map(|i| 10 + (index * 10 + i) as i64)
        .collect();
    for (&(code, uid, latency), &ts) in records.iter().zip(&ts) {
        writer
            .push(LogRecord {
                stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("o{index} t{ts}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![
                    ("status_code".to_string(), AttrValue::I64(code)),
                    ("uid".to_string(), AttrValue::I64(uid)),
                    ("latency_ms".to_string(), AttrValue::I64(latency)),
                ],
            })
            .expect("push log record");
    }
    let bytes = Bytes::from(writer.finish().expect("finish rlog object"));
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: ts[0],
        max_event_ts_ns: ts[ts.len() - 1],
        min_ingest_ts_ns: ts[0],
        max_ingest_ts_ns: ts[ts.len() - 1],
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10 + index as i64,
        ingest_hour_bucket: 0,
    })
    .expect("valid logs commit record");
    let key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&key, bytes.clone(), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
    (key, bytes)
}

/// The KEY_IDX reads a `status_code = 500` probe issues on `bytes`, computed
/// from the format crate rather than the fetcher under test, and the blocks
/// the probe names (empty when the key is absent).
fn expected_key_idx_reads(bytes: &Bytes) -> (Vec<GetRange>, Vec<u32>) {
    use ravel_logseg::key_index::{KeyType, key8_i64, probe_ranged};

    let footer = ravel_logseg::footer::open(bytes).expect("footer");
    let mut reads = Vec::new();
    let blocks = probe_ranged(
        &footer,
        "status_code",
        KeyType::I64,
        &key8_i64(500),
        |r: std::ops::Range<u64>| {
            reads.push(GetRange::Range(r.start, r.end));
            Ok::<_, ravel_logseg::LogSegError>(bytes.slice(r.start as usize..r.end as usize))
        },
    )
    .expect("probe")
    .expect("the object indexes status_code");
    (reads, blocks)
}

/// The GETs in `log` on `key` that lie inside the KEY_IDX section of `bytes`,
/// the object stored under it, in issue order.
fn key_idx_gets(log: &RangeLog, key: &str, bytes: &Bytes) -> Vec<GetRange> {
    let footer = ravel_logseg::footer::open(bytes).expect("footer");
    let section = footer
        .section(ravel_logseg::footer::kind::KEY_IDX)
        .expect("KEY_IDX section");
    let (start, end) = (section.offset, section.offset + section.len);
    let size = bytes.len() as u64;
    log.lock()
        .expect("range log")
        .iter()
        .filter(|(k, _)| k == key)
        .map(|(_, range)| *range)
        .filter(|range| {
            let (from, to) = match *range {
                GetRange::Full => (0, size),
                GetRange::Range(from, to) => (from, to),
                GetRange::Suffix(n) => (size.saturating_sub(n), size),
            };
            start <= from && to <= end
        })
        .collect()
}

/// `hits` as a `VALUES` list: the same six `(id, name)` rows.
const HITS_AS_VALUES: &str = "(VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'a'), (5, 'b'), (6, 'c')) \
     AS h(id, name)";

/// A logs equality on a KEY_IDX-indexed declared column, in a statement that
/// also reads a Parquet table, returns exactly the rows of the same statement
/// with `hits` replaced by a `VALUES` list of its rows: for a join, and for
/// `COUNT(*)` and `MIN` of a declared column over the join, which plans
/// through the logs column-stats gate. Both differ from the statement without
/// the equality, so the key filter is applied, and the mixed statement probes
/// KEY_IDX: each object's KEY_IDX reads are exactly the reads the format
/// crate's own probe issues, charged to the `keyIndex` phase, and object 1,
/// which lacks the key, is read for its tail probe and KEY_IDX prefix only,
/// where the statement without the equality reads it further.
///
/// FLIP: a mixed plan that drops the logs equality returns the unkeyed rows,
/// `5|3` for the aggregate; with no column in the tenant's key-index
/// declaration the statement reads no KEY_IDX byte and the KEY_IDX assertion
/// fails.
#[tokio::test]
async fn a_logs_key_equality_beside_a_parquet_table_returns_exact_rows() {
    let (declared, key_index) = keyed_declared();
    let (lake, gets) = Lake::keyed(declared, key_index);
    let acme = util::tenant_id("acme");
    lake.hits_for(&acme.hash()).await;
    let mut objects = Vec::new();
    for (index, records) in KEYED_LOGS.iter().enumerate() {
        objects.push(publish_keyed_logs(lake.ravel.as_ref(), &acme, index, records).await);
    }
    let probes: Vec<(Vec<GetRange>, Vec<u32>)> = objects
        .iter()
        .map(|(_, bytes)| expected_key_idx_reads(bytes))
        .collect();
    assert!(!probes[0].1.is_empty(), "object 0 holds the key");
    assert!(probes[1].1.is_empty(), "object 1 does not");

    let cases = [
        (
            "SELECT h.id, h.name, l.latency_ms FROM hits h JOIN logs l ON l.uid = h.id \
             WHERE l.status_code = 500 ORDER BY h.id",
            vec!["1|a|30", "4|a|12"],
            vec!["1|a|30", "2|b|5", "3|c|7", "4|a|12", "6|c|3"],
        ),
        (
            "SELECT COUNT(*), MIN(l.latency_ms) FROM logs l JOIN hits h ON l.uid = h.id \
             WHERE l.status_code = 500",
            vec!["2|12"],
            vec!["5|3"],
        ),
    ];
    let run = |sql: String| {
        let (lake, gets, objects, acme) = (&lake, &gets, &objects, &acme);
        async move {
            gets.lock().expect("range log").clear();
            let outcome = lake.execute(&acme.hash(), &sql).await.expect(&sql);
            let key_idx: Vec<Vec<GetRange>> = objects
                .iter()
                .map(|(key, bytes)| key_idx_gets(gets, key, bytes))
                .collect();
            let charged = outcome
                .phase_accounting
                .phase(QueryPhase::KeyIndex)
                .s3_requests(AccountedOp::Get);
            let on_object_1: Vec<GetRange> = gets
                .lock()
                .expect("range log")
                .iter()
                .filter(|(key, _)| *key == objects[1].0)
                .map(|(_, range)| *range)
                .collect();
            (rows(&outcome), key_idx, charged, on_object_1)
        }
    };
    let expected_reads: Vec<Vec<GetRange>> = probes.iter().map(|(r, _)| r.clone()).collect();
    let expected_charged: u64 = expected_reads.iter().map(|r| r.len() as u64).sum();
    for (sql, keyed_rows, unkeyed_rows) in cases {
        let values = sql.replace("hits h", HITS_AS_VALUES);
        let unkeyed = sql.replace(" WHERE l.status_code = 500", "");
        assert_ne!(unkeyed, sql);

        let (mixed, mixed_reads, mixed_charged, mixed_dropped) = run(sql.to_string()).await;
        let (alone, alone_reads, alone_charged, alone_dropped) = run(values.clone()).await;
        assert_eq!(mixed, alone, "{sql}\nagainst {values}");
        assert_eq!(mixed, keyed_rows, "{sql}");
        assert_eq!(mixed_reads, expected_reads, "{sql}: KEY_IDX GETs");
        assert_eq!(mixed_charged, expected_charged, "{sql}: keyIndex GETs");
        assert_eq!(alone_reads, expected_reads, "{values}: KEY_IDX GETs");
        assert_eq!(alone_charged, expected_charged, "{values}: keyIndex GETs");
        let dropped: Vec<GetRange> = std::iter::once(GetRange::Suffix(KEYED_SUFFIX_LEN))
            .chain(expected_reads[1].iter().copied())
            .collect();
        assert_eq!(
            mixed_dropped, dropped,
            "{sql}: object 1 is dropped by KEY_IDX"
        );
        assert_eq!(
            alone_dropped, dropped,
            "{values}: object 1 is dropped by KEY_IDX"
        );

        let (unkeyed_mixed, unkeyed_reads, unkeyed_charged, unkeyed_object_1) =
            run(unkeyed.clone()).await;
        let (unkeyed_alone, _, _, _) = run(unkeyed.replace("hits h", HITS_AS_VALUES)).await;
        assert_eq!(unkeyed_mixed, unkeyed_alone, "{unkeyed}");
        assert_eq!(unkeyed_mixed, unkeyed_rows, "{unkeyed}");
        assert_ne!(mixed, unkeyed_mixed, "{sql}: the key equality filters");
        assert_eq!(unkeyed_reads, vec![Vec::new(); 2], "{unkeyed}");
        assert_eq!(unkeyed_charged, 0, "{unkeyed}");
        assert!(
            unkeyed_object_1.len() > dropped.len(),
            "{unkeyed}: object 1 is read past its tail probe: {unkeyed_object_1:?}"
        );
    }
}

/// A Parquet table has no commit tokens, so a `min_commit_token` on a
/// statement that reads only Parquet tables is
/// [`SqlError::CommitTokenOnParquetStatement`], unqualified or qualified with
/// the Parquet table's name, refused from the statement text before either
/// store is read. A Parquet-qualified token on a statement that also names a
/// signal table is [`SqlError::CommitTokenQualifierNotRead`], refused before
/// the Parquet table is resolved.
#[tokio::test]
async fn a_commit_token_on_a_parquet_only_statement_is_refused() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let token = ravel_types::CommitToken {
        shard: 0,
        writer_id: uuid::Uuid::from_u128(11),
        epoch: 1,
        seq: 1,
        ingest_hour_bucket: 0,
    };
    let with_tokens = |sql: &str, tokens: Vec<ravel_sql::MinCommitToken>| {
        let mut req = request(sql);
        req.min_tokens = tokens;
        req
    };

    for tokens in [
        vec![ravel_sql::MinCommitToken::unqualified(token.clone())],
        vec![ravel_sql::MinCommitToken::qualified("hits", token.clone())],
    ] {
        let before = reads(&lake);
        let err = lake
            .executor
            .execute(
                acme,
                &with_tokens("SELECT count(*) FROM hits", tokens.clone()),
            )
            .await
            .expect_err("a token on a Parquet-only statement");
        assert!(
            matches!(err, SqlError::CommitTokenOnParquetStatement),
            "{tokens:?}: {err}"
        );
        assert_eq!(err.class(), ErrorClass::BadRequest);
        assert!(
            err.client_message().contains("min_commit_token"),
            "{}",
            err.client_message()
        );
        assert_eq!(reads(&lake), before, "{tokens:?}: nothing is read");
    }

    let outcome = lake
        .execute(&acme, "SELECT count(*) FROM hits")
        .await
        .expect("the same statement without a token runs");
    assert_eq!(rows(&outcome), vec!["6".to_string()]);

    let before = reads(&lake);
    let err = lake
        .executor
        .execute(
            acme,
            &with_tokens(
                "SELECT count(*) FROM hits JOIN logs ON true",
                vec![ravel_sql::MinCommitToken::qualified("hits", token)],
            ),
        )
        .await
        .expect_err("a Parquet qualifier beside a signal table");
    match &err {
        SqlError::CommitTokenQualifierNotRead { qualifier, tables } => {
            assert_eq!((qualifier.as_str(), tables.as_str()), ("hits", "logs"));
        }
        other => panic!("expected CommitTokenQualifierNotRead, got {other}"),
    }
    assert_eq!(err.class(), ErrorClass::BadRequest);
    assert_eq!(
        reads(&lake),
        before,
        "refused before the Parquet table is resolved"
    );
}

/// A statement naming one signal table beside a Parquet table takes a plain
/// token, which bounds its one signal table, and a token qualified with that
/// table's name; a token qualified with the Parquet table's name is
/// [`SqlError::CommitTokenQualifierNotRead`], refused before either store is
/// read.
#[tokio::test]
async fn a_mixed_statement_takes_a_token_for_its_signal_table_only() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    let (token, _) = hits_and_logs(&lake).await;
    let with_tokens = |tokens: Vec<ravel_sql::MinCommitToken>| {
        let mut req = request(HITS_JOIN_LOGS);
        req.min_tokens = tokens;
        req
    };

    for tokens in [
        vec![ravel_sql::MinCommitToken::unqualified(token.clone())],
        vec![ravel_sql::MinCommitToken::qualified("logs", token.clone())],
    ] {
        let outcome = lake
            .executor
            .execute(acme, &with_tokens(tokens.clone()))
            .await
            .unwrap_or_else(|err| panic!("{tokens:?}: {err}"));
        assert_eq!(
            rows(&outcome),
            vec!["1|line 1", "2|line 2", "3|line 3", "4|line 4"],
            "{tokens:?}"
        );
    }

    let before = reads(&lake);
    let err = lake
        .executor
        .execute(
            acme,
            &with_tokens(vec![ravel_sql::MinCommitToken::qualified("hits", token)]),
        )
        .await
        .expect_err("a token qualified with the Parquet table");
    match &err {
        SqlError::CommitTokenQualifierNotRead { qualifier, tables } => {
            assert_eq!((qualifier.as_str(), tables.as_str()), ("hits", "logs"));
        }
        other => panic!("expected CommitTokenQualifierNotRead, got {other}"),
    }
    assert_eq!(reads(&lake), before, "refused before either store is read");
}

/// Grants are checked when a query resolves the table: after the grant a
/// table was created under is removed, the table's files lie outside every
/// grant and the query fails typed, reading none of them.
#[tokio::test]
async fn a_file_outside_every_current_grant_fails_the_query() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    lake.execute(&acme, "SELECT count(*) FROM hits")
        .await
        .expect("granted");
    grants::remove(lake.ravel.inner(), &acme, GRANT)
        .await
        .expect("revoke");
    let lake_before = Lake::gets(&lake.lake);

    let err = lake
        .execute(&acme, "SELECT count(*) FROM hits")
        .await
        .expect_err("revoked");
    assert!(
        matches!(
            parquet_error(&err),
            Some(ParquetQueryError::LocationNotGranted { table }) if table == "hits"
        ),
        "{err}"
    );
    assert!(
        err.client_message().contains("hits"),
        "{}",
        err.client_message()
    );
    assert_eq!(Lake::gets(&lake.lake), lake_before);
}

/// A grant of another prefix of the same bucket does not admit the table's
/// files either: containment is by segment, not by string prefix.
#[tokio::test]
async fn a_grant_sharing_only_a_string_prefix_does_not_admit_a_file() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    grants::remove(lake.ravel.inner(), &acme, GRANT)
        .await
        .expect("revoke");
    grants::add(
        lake.ravel.inner(),
        &acme,
        PROFILE,
        "s3://lake/t/hit",
        "test",
        &FixedClock::new(NOW),
    )
    .await
    .expect("a narrower sibling grant");
    let err = lake
        .execute(&acme, "SELECT count(*) FROM hits")
        .await
        .expect_err("not granted");
    assert!(
        matches!(
            parquet_error(&err),
            Some(ParquetQueryError::LocationNotGranted { .. })
        ),
        "{err}"
    );
}

/// A URL table, another tenant's `ravel-pq://` path and every table-function
/// spelling fail to plan before any store is read.
///
/// Every statement here fails to plan with or without the executor's early
/// refusal, so the error alone cannot tell the two apart. The store counters
/// can: without the refusal, each statement is resolved first (as a signal
/// query, whose catalog resolve reads Ravel's store with one GET and two
/// LISTs, or as a Parquet table, whose manifest resolve LISTs and GETs it)
/// and only then fails. The Ravel-store GET and LIST counts are therefore
/// checked unchanged after each statement, not only after all of them.
#[tokio::test]
async fn url_tables_and_table_functions_read_nothing() {
    let lake = Lake::configured();
    let (acme, globex) = (tenant("acme"), tenant("globex"));
    lake.hits_for(&acme).await;
    lake.hits_for(&globex).await;
    let other = ravel_parquet::store_url(&globex);
    let (ravel_before, lake_before) = lake.all_gets();
    let lists_before = Lake::lists(&lake.ravel);

    for sql in [
        "SELECT * FROM 's3://lake/t/hits/0.parquet'".to_string(),
        "SELECT * FROM 'file:///etc/passwd'".to_string(),
        format!("SELECT * FROM '{other}hits/1/f/0'"),
        "SELECT * FROM \"ravel-pq://x/hits/1/f/0\"".to_string(),
        "SELECT * FROM read_parquet('s3://lake/t/hits/0.parquet')".to_string(),
        "SELECT * FROM TABLE(read_parquet('s3://lake/t/hits/0.parquet'))".to_string(),
        "SELECT * FROM hits WHERE id IN (SELECT value FROM range(0, 10))".to_string(),
    ] {
        let before = (lake.all_gets(), Lake::lists(&lake.ravel));
        let err = lake.execute(&acme, &sql).await.expect_err(&sql);
        assert!(matches!(err, SqlError::Plan(_)), "{sql}: {err}");
        assert_eq!(err.client_message(), MSG_PLAN, "{sql}");
        assert_eq!(
            (lake.all_gets(), Lake::lists(&lake.ravel)),
            before,
            "{sql}: the refusal reads nothing from either store"
        );
    }
    assert_eq!(lake.all_gets(), (ravel_before, lake_before));
    assert_eq!(Lake::lists(&lake.ravel), lists_before);
}

/// The physical plan's `file_groups={N groups: ...}` count for `sql`.
async fn file_groups(lake: &Lake, tenant: &TenantHash, sql: &str) -> usize {
    let report = lake
        .executor
        .explain(*tenant, &request(sql))
        .await
        .expect("explain");
    assert_eq!(report.target, TargetSignal::Parquet);
    let text = report.plan_text;
    let at = text
        .find("file_groups={")
        .unwrap_or_else(|| panic!("no file groups in {text}"));
    let rest = &text[at + "file_groups={".len()..];
    let count: String = rest.chars().take_while(char::is_ascii_digit).collect();
    count
        .parse()
        .unwrap_or_else(|_| panic!("unparsed groups in {text}"))
}

/// `explain` over a Parquet table reports the estimate's requests and store
/// bytes, and names all three components as unbounded: the requests assume a
/// cold cache and one GET per file, and the bytes count each file once, so a
/// real scan can exceed either, and the decompressed bytes are not computed at
/// all.
///
/// FLIP: listing only `estimated_decompressed_bytes` fails the equality.
#[tokio::test]
async fn the_parquet_estimate_names_every_component_it_does_not_bound() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let report = lake
        .executor
        .explain(acme, &request("SELECT id FROM hits"))
        .await
        .expect("explain");
    assert_eq!(report.target, TargetSignal::Parquet);
    assert_eq!(
        report.unbounded_components,
        vec![
            "estimated_requests",
            "estimated_store_bytes",
            "estimated_decompressed_bytes",
        ]
    );
    let files = hits_files();
    assert_eq!(
        report.estimate.estimated_store_bytes,
        files
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>(),
        "the figures are still reported"
    );
    assert_eq!(report.estimate.estimated_decompressed_bytes, 0);
}

/// ADR-2040 D6: an exact-typed statement scans in one group per file (three
/// files, eight target partitions), and one whose float sum is not exact
/// scans in exactly one.
#[tokio::test]
async fn an_exact_typed_aggregate_scans_in_parallel_and_a_float_sum_does_not() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    assert!(SqlConfig::default().parallel_final_aggregation);
    assert!(SqlConfig::default().engine.sql_partition_count() >= 3);

    assert_eq!(
        file_groups(&lake, &acme, "SELECT count(*) FROM hits").await,
        3
    );
    assert_eq!(
        file_groups(&lake, &acme, "SELECT sum(id) FROM hits").await,
        3
    );
    assert_eq!(
        file_groups(&lake, &acme, "SELECT sum(score) FROM hits").await,
        1
    );

    let exact = lake
        .execute(&acme, "SELECT count(*), sum(id) FROM hits")
        .await
        .expect("exact");
    assert_eq!(rows(&exact), vec!["6|21"]);
    let float = lake
        .execute(&acme, "SELECT sum(score) FROM hits")
        .await
        .expect("float");
    assert_eq!(rows(&float), vec!["18.0"]);
}

/// With the operator's switch off, every Parquet scan is one group.
#[tokio::test]
async fn parallel_final_aggregation_off_keeps_one_group() {
    let lake = Lake::new(
        true,
        SqlConfig {
            parallel_final_aggregation: false,
            ..SqlConfig::default()
        },
    );
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    assert_eq!(
        file_groups(&lake, &acme, "SELECT count(*) FROM hits").await,
        1
    );
}

/// ADR-2040 D6: `BoundedTopKAggregate` fires on a Parquet plan, and the
/// bounded answer is the exact one.
#[tokio::test]
async fn bounded_topk_fires_on_a_parquet_plan_with_the_exact_answer() {
    let sql = "SELECT name, max(id) AS m FROM hits GROUP BY name ORDER BY m DESC LIMIT 2";
    let on = Lake::configured();
    let acme = tenant("acme");
    on.hits_for(&acme).await;
    let report = on
        .executor
        .explain(acme, &request(sql))
        .await
        .expect("explain");
    assert!(report.plan_text.contains("lim=[2]"), "{}", report.plan_text);
    let bounded = on.execute(&acme, sql).await.expect("bounded");
    assert_eq!(rows(&bounded), vec!["c|6", "b|5"]);

    let off = Lake::new(
        true,
        SqlConfig {
            bounded_topk_max_limit: None,
            ..SqlConfig::default()
        },
    );
    off.hits_for(&acme).await;
    let report = off
        .executor
        .explain(acme, &request(sql))
        .await
        .expect("explain");
    assert!(!report.plan_text.contains("lim=["), "{}", report.plan_text);
    assert_eq!(
        rows(&off.execute(&acme, sql).await.expect("unbounded")),
        rows(&bounded)
    );
}

/// With no credential profile file a Parquet table is not queryable: the
/// statement fails typed, after one LIST and one GET of the newest manifest on
/// Ravel's store, and no GET on the lake.
///
/// The manifest GET is what says the name is a live table rather than a
/// dropped one. The Ravel-store GET count is what tells this early refusal
/// from resolving the table anyway and refusing afterwards: a full resolve
/// also GETs the grants record.
#[tokio::test]
async fn no_profile_file_is_a_typed_error_that_reads_nothing() {
    let lake = Lake::new(false, SqlConfig::default());
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let lists_before = Lake::lists(&lake.ravel);
    let (ravel_before, lake_before) = lake.all_gets();

    let err = lake
        .execute(&acme, "SELECT * FROM hits")
        .await
        .expect_err("no profiles");
    assert!(
        matches!(
            parquet_error(&err),
            Some(ParquetQueryError::NotConfigured { table }) if table == "hits"
        ),
        "{err}"
    );
    assert!(
        err.client_message().contains("--parquet-profiles"),
        "{}",
        err.client_message()
    );
    assert_eq!(
        Lake::gets(&lake.ravel) - ravel_before,
        1,
        "the newest manifest, and not the grants record a full resolve reads"
    );
    assert_eq!(Lake::gets(&lake.lake), lake_before, "no file is read");
    assert_eq!(
        Lake::lists(&lake.ravel) - lists_before,
        1,
        "one LIST of the table's versions"
    );

    unknown_table_error(&lake, &acme, "SELECT * FROM nosuch").await;
}

/// A file overwritten after CREATE, and one deleted after it, fail the query
/// with `FileChanged` and `FileMissing` (ADR-2040 D3): class `Unsupported`,
/// which HTTP answers with 422, and a client message that names the file and
/// tells the caller to run `CREATE OR REPLACE`. Neither is the table's first
/// file, so the table still builds and the failure comes from the scan.
#[tokio::test]
async fn a_file_changed_or_deleted_after_create_fails_the_query_naming_it() {
    let sql = "SELECT id, name FROM hits ORDER BY id";
    for changed in [true, false] {
        let lake = Lake::configured();
        let acme = tenant("acme");
        lake.hits_for(&acme).await;
        let before = lake.execute(&acme, sql).await.expect("reads before");
        assert_eq!(rows(&before).len(), 6);

        let key = if changed {
            "t/hits/1.parquet"
        } else {
            "t/hits/2.parquet"
        };
        if changed {
            lake.lake
                .inner()
                .put(
                    key,
                    parquet_bytes(&[7, 8], &["x", "y"], &[0.0, 0.0]),
                    PutOptions::default(),
                )
                .await
                .expect("overwrite");
        } else {
            lake.lake.inner().delete(key).await.expect("delete");
        }

        let err = lake.execute(&acme, sql).await.expect_err(key);
        let read = match parquet_error(&err) {
            Some(ParquetQueryError::Read(read)) => read,
            _ => panic!("{key}: expected a Parquet read error, got {err:?}"),
        };
        if changed {
            assert!(
                matches!(read, ParquetReadError::FileChanged { key: k } if k == key),
                "{read:?}"
            );
        } else {
            assert!(
                matches!(read, ParquetReadError::FileMissing { key: k } if k == key),
                "{read:?}"
            );
        }
        assert_eq!(err.class(), ErrorClass::Unsupported, "{key}");
        let message = err.client_message();
        assert!(message.contains(key), "{message}");
        assert!(message.contains("CREATE OR REPLACE"), "{message}");
    }
}

/// ADR-2040 D4: a manifest naming a file in Ravel's own data bucket is refused
/// when the query reads it. The production `ProfileStores` is configured with
/// Ravel's bucket at the profile's own endpoint, which the profile addresses
/// virtual-hosted, so every key it reads is sent under that endpoint as
/// written; the refusal comes before the store is opened, so the query fails
/// typed without a network.
#[tokio::test]
async fn a_file_in_ravels_own_bucket_is_refused_on_read() {
    let dir = tempfile::tempdir().expect("temp dir");
    let key = dir.path().join("key");
    std::fs::write(&key, "test-key").expect("write key");
    let json = format!(
        r#"[{{"name": {PROFILE:?}, "kind": "s3", "region": "us-east-1",
             "endpoint": "http://127.0.0.1:9", "allow_http": true,
             "credentials": {{"mode": "static",
               "access_key_id": {{"from": "file", "path": {key:?}}},
               "secret_access_key": {{"from": "file", "path": {key:?}}}}}}}]"#
    );
    let profiles = ravel_object_store::external::load_profiles(&json).expect("profiles");
    let stores = ravel_sql::ProfileStores::new(profiles).refusing(ravel_sql::RavelBucket {
        endpoint: Some("http://127.0.0.1:9".to_string()),
        region: "us-east-1".to_string(),
        bucket: BUCKET.to_string(),
    });
    let lake = Lake::with_external(
        Arc::new(InstrumentedStore::new(MemoryStore::new())),
        Some(Arc::new(stores) as Arc<dyn ravel_sql::ExternalStores>),
        SqlConfig::default(),
    );
    let acme = tenant("acme");
    lake.hits_for(&acme).await;

    let err = lake
        .execute(&acme, "SELECT count(*) FROM hits")
        .await
        .expect_err("Ravel's bucket");
    assert!(
        matches!(
            parquet_error(&err),
            Some(ParquetQueryError::Store {
                table,
                source: ravel_sql::ExternalStoreError::RavelBucket { .. },
            }) if table == "hits"
        ),
        "{err:?}"
    );
    assert_eq!(err.class(), ErrorClass::Unsupported);
    let message = err.client_message();
    assert!(message.contains("Ravel's own data bucket"), "{message}");
}

/// Drop `table` of `tenant`: its newest manifest version becomes a drop.
async fn drop_table(lake: &Lake, tenant: &TenantHash, table: &str) {
    writer::apply(
        lake.ravel.inner(),
        tenant,
        table,
        Intent::Drop {
            if_exists: false,
            created_by: "test".to_string(),
            statement: format!("DROP TABLE {table}"),
        },
        &FixedClock::new(NOW),
        MIN_GRACE_MS,
    )
    .await
    .expect("drop");
}

/// A dropped table is no table: the same planning failure as a name nobody
/// created.
#[tokio::test]
async fn a_dropped_table_is_an_unknown_table() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    drop_table(&lake, &acme, "hits").await;
    unknown_table_error(&lake, &acme, "SELECT * FROM hits").await;
}

/// A forged newest version above the manifest version bound does not win:
/// the query serves the newest version at or below the bound, the table is
/// counted as holding a forged version, and DDL on it still commits.
///
/// FLIP: resolving the highest listed version reads the forged drop, so
/// `hits` is an unknown table.
#[tokio::test]
async fn a_forged_parquet_version_above_the_bound_is_ignored_and_the_older_one_served() {
    let lake = Lake::configured();
    // A tenant no other test uses: the forged-version count is per process.
    let acme = tenant("acme-forged-version");
    lake.hits_for(&acme).await;
    let sql = "SELECT id FROM hits ORDER BY id";
    let before = rows(&lake.execute(&acme, sql).await.expect("query"));
    assert_eq!(before.len(), 6, "{before:?}");

    let forged = Manifest {
        table: "hits".to_string(),
        version: u64::MAX,
        dropped: true,
        location: String::new(),
        grant: String::new(),
        files: Vec::new(),
        options: BTreeMap::new(),
        created_by: "forger".to_string(),
        created_unix_ns: NOW,
        statement: "DROP TABLE hits".to_string(),
        apply_nonce: vec![0; APPLY_NONCE_LEN],
    };
    lake.ravel
        .inner()
        .put(
            &manifest_key(&acme, "hits", u64::MAX).expect("key"),
            Bytes::from(encode_manifest(&acme, &forged).expect("encode")),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("forged put");

    let after = rows(&lake.execute(&acme, sql).await.expect("query"));
    assert_eq!(after, before);
    assert!(resolve::above_bound_resolves(&acme, "hits") > 0);

    drop_table(&lake, &acme, "hits").await;
    assert_eq!(
        resolve::versions(lake.ravel.inner(), &acme, "hits")
            .await
            .expect("versions"),
        vec![1, 2, u64::MAX]
    );
    unknown_table_error(&lake, &acme, sql).await;
}

/// A dropped table beside a signal table is an unknown table, not a Parquet
/// table to plan beside it: the resolve reads the name's newest manifest,
/// finds a drop, and goes on, the same failure as a name nobody created.
///
/// FLIP: deciding from the LIST alone (a name with versions is found) takes
/// the dropped name for a live table, and the statement no longer fails as
/// an unknown table.
#[tokio::test]
async fn a_dropped_table_beside_samples_is_an_unknown_table() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    drop_table(&lake, &acme, "hits").await;
    let before = reads(&lake);
    let err = unknown_table_error(&lake, &acme, "SELECT 1 FROM samples, hits").await;
    let nosuch = unknown_table_error(&lake, &acme, "SELECT 1 FROM samples, nosuch").await;
    assert_eq!(err.class(), nosuch.class());
    assert_eq!(
        reads(&lake).2,
        before.2,
        "the dropped table's files are never read"
    );
}

/// Without a profile file a dropped table is an unknown table, not
/// `NotConfigured`; a live table named after it still decides
/// `NotConfigured`, at one extra GET for the dropped name.
///
/// FLIP: deciding from the LIST alone makes the first statement
/// `NotConfigured`.
#[tokio::test]
async fn a_dropped_table_without_a_profile_file_is_an_unknown_table() {
    let lake = Lake::new(false, SqlConfig::default());
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let names = lake
        .put_file(
            "t/dropped_tbl/0.parquet",
            parquet_bytes(&[1], &["x"], &[0.0]),
        )
        .await;
    lake.create(&acme, "dropped_tbl", vec![names]).await;
    drop_table(&lake, &acme, "dropped_tbl").await;

    unknown_table_error(&lake, &acme, "SELECT * FROM dropped_tbl").await;

    let before = reads(&lake);
    let err = lake
        .execute(&acme, "SELECT 1 FROM dropped_tbl, hits")
        .await
        .expect_err("hits decides");
    assert!(
        matches!(
            parquet_error(&err),
            Some(ParquetQueryError::NotConfigured { table }) if table == "hits"
        ),
        "{err}"
    );
    let after = reads(&lake);
    assert_eq!(after.1 - before.1, 2, "one LIST per name");
    assert_eq!(
        after.0 - before.0,
        2,
        "one manifest GET per name that has versions, the dropped one included"
    );
    assert_eq!(after.2, before.2, "no file is read");
}

/// Two Parquet tables of one tenant join in one statement.
#[tokio::test]
async fn two_parquet_tables_join() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let names = lake
        .put_file(
            "t/names/0.parquet",
            parquet_bytes(&[1, 5], &["first", "fifth"], &[0.0, 0.0]),
        )
        .await;
    lake.create(&acme, "names", vec![names]).await;
    let outcome = lake
        .execute(
            &acme,
            "SELECT h.id, n.name FROM hits h JOIN names n ON h.id = n.id ORDER BY h.id",
        )
        .await
        .expect("join");
    assert_eq!(rows(&outcome), vec!["1|first", "5|fifth"]);
}

/// `count` distinct table names that sort after `hits` and are no table.
fn unknown_names(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("u{i:02}")).collect()
}

/// The (Ravel GET, Ravel LIST, lake GET) counts.
fn reads(lake: &Lake) -> (u64, u64, u64) {
    (
        Lake::gets(&lake.ravel),
        Lake::lists(&lake.ravel),
        Lake::gets(&lake.lake),
    )
}

/// One name past the cap fails from the statement text with its own 400,
/// before a single LIST or GET reaches either store.
#[tokio::test]
async fn a_statement_naming_one_table_too_many_reads_nothing() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let before = reads(&lake);

    let names = unknown_names(MAX_STATEMENT_TABLE_NAMES + 1);
    let sql = format!("SELECT 1 FROM {}", names.join(", "));
    let err = lake
        .execute(&acme, &sql)
        .await
        .expect_err("too many tables");
    assert!(
        matches!(
            err,
            SqlError::TooManyTables { count, max }
                if count == MAX_STATEMENT_TABLE_NAMES + 1 && max == MAX_STATEMENT_TABLE_NAMES
        ),
        "{err}"
    );
    assert_eq!(err.class(), ErrorClass::BadRequest);
    assert_eq!(reads(&lake), before, "no LIST or GET on either store");
}

/// Exactly the cap still resolves: each name costs one manifest LIST and the
/// statement then fails as an unknown table. The catalog resolve an unknown
/// name falls through to is measured on its own, from a statement naming no
/// table, and subtracted.
#[tokio::test]
async fn a_statement_naming_the_cap_is_an_unknown_table_after_one_list_each() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;

    let before = Lake::lists(&lake.ravel);
    lake.execute(&acme, "SELECT 1").await.expect("no table");
    let catalog_lists = Lake::lists(&lake.ravel) - before;

    let names = unknown_names(MAX_STATEMENT_TABLE_NAMES);
    let sql = format!("SELECT 1 FROM {}", names.join(", "));
    let before = Lake::lists(&lake.ravel);
    unknown_table_error(&lake, &acme, &sql).await;
    assert_eq!(
        Lake::lists(&lake.ravel) - before - catalog_lists,
        MAX_STATEMENT_TABLE_NAMES as u64,
        "one manifest LIST per name"
    );
}

/// `hits` for `acme` beside two folded `samples` segments in ingest hour 0:
/// `cpu` at the values 1, 2 and 5, and `mem` at 3.
async fn hits_and_samples(lake: &Lake) {
    let acme = util::tenant_id("acme");
    lake.hits_for(&acme.hash()).await;
    let minute = 60_000_000_000;
    let segments = [
        util::SegSpec::new(
            1,
            1,
            1,
            vec![util::SeriesSpec::new(
                "cpu",
                vec![(minute, 1.0), (2 * minute, 2.0), (3 * minute, 5.0)],
            )],
        ),
        util::SegSpec::new(
            2,
            1,
            2,
            vec![util::SeriesSpec::new("mem", vec![(minute, 3.0)])],
        ),
    ];
    for (index, spec) in segments.iter().enumerate() {
        util::publish_segment(lake.ravel.as_ref(), &acme, index, spec).await;
    }
    // Fold so the name postings `pushed_down_name_filter` prunes with exist.
    // Hour 0 is sealed at NOW_NS (4h).
    let report = Catalog::new(
        Arc::clone(&lake.ravel) as Arc<dyn ObjectStoreBackend>,
        CatalogConfig::default(),
    )
    .expect("catalog")
    .fold(
        &acme.hash(),
        Signal::Metrics,
        uuid::Uuid::new_v4(),
        util::NOW_NS,
        &[],
        None,
    )
    .await
    .expect("fold");
    assert!(report.watermark_hour.is_some(), "hour 0 is sealed");
    assert!(report.postings_built);
}

/// `samples` joined with a Parquet table returns the exact rows. The derived
/// `__name__` filter that prunes a `samples`-only statement by postings
/// (`pushed_down_name_filter`) is not applied beside a Parquet table: the
/// mixed statement resolves both segments and the filter runs above the scan,
/// so the rows are still exact. This pins today's behaviour; pruning a mixed
/// statement is a separate change.
///
/// FLIP: dropping the filter above the scan joins `mem`'s 3 to `hits` id 3.
#[tokio::test]
async fn samples_joined_with_a_parquet_table_returns_exact_rows() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    hits_and_samples(&lake).await;
    let filter = "label(s.labels, '__name__') = 'cpu'";

    let alone = lake
        .execute(
            &acme,
            &format!("SELECT s.value FROM samples s WHERE {filter} ORDER BY s.value"),
        )
        .await
        .expect("samples alone");
    assert_eq!(rows(&alone), vec!["1.0", "2.0", "5.0"]);
    assert_eq!(alone.stats.segments, 1, "postings pruned the mem segment");

    let mixed = lake
        .execute(
            &acme,
            &format!(
                "SELECT h.id, s.value FROM samples s JOIN hits h \
                 ON CAST(s.value AS BIGINT) = h.id WHERE {filter} ORDER BY h.id"
            ),
        )
        .await
        .expect("samples beside hits");
    assert_eq!(rows(&mixed), vec!["1|1.0", "2|2.0", "5|5.0"]);
    assert_eq!(mixed.target, TargetSignal::Metrics);
    assert_eq!(
        mixed.stats.segments, 2,
        "no postings pruning beside a Parquet table"
    );
}

/// Two signal tables and a Parquet table in one statement return the exact
/// rows: `hits` joined to the log line naming each id and to the `samples`
/// value equal to it. The statement reports one `stats.signals` entry per
/// signal table and none for `hits`.
///
/// FLIP: a resolve that reads only the first signal lane beside the Parquet
/// tables gives the other an empty snapshot, and the join returns no row.
#[tokio::test]
async fn two_signal_tables_joined_with_a_parquet_table_return_exact_rows() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    hits_and_samples(&lake).await;
    util::publish_logs(lake.ravel.as_ref(), &util::tenant_id("acme"), 0, &LOG_TS).await;

    let outcome = lake
        .execute(
            &acme,
            "SELECT h.id, l.body, s.value FROM hits h \
             JOIN logs l ON l.body = concat('line ', CAST(h.id AS VARCHAR)) \
             JOIN samples s ON CAST(s.value AS BIGINT) = h.id \
             ORDER BY h.id",
        )
        .await
        .expect("hits beside logs and samples");
    assert_eq!(
        rows(&outcome),
        vec!["1|line 1|1.0", "2|line 2|2.0", "3|line 3|3.0"]
    );
    let mut tables = outcome
        .signals
        .iter()
        .map(|signal| signal.table)
        .collect::<Vec<_>>();
    tables.sort_unstable();
    assert_eq!(tables, vec!["logs", "samples"]);
}

/// Without a profile file, the first name that is a live table decides
/// `NotConfigured`: the unknown names after it are never listed.
#[tokio::test]
async fn not_configured_stops_at_the_first_found_name() {
    let lake = Lake::new(false, SqlConfig::default());
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let before = reads(&lake);

    let names = unknown_names(MAX_STATEMENT_TABLE_NAMES - 1);
    let sql = format!("SELECT 1 FROM hits, {}", names.join(", "));
    let err = lake.execute(&acme, &sql).await.expect_err("no profiles");
    assert!(
        matches!(
            parquet_error(&err),
            Some(ParquetQueryError::NotConfigured { table }) if table == "hits"
        ),
        "{err}"
    );
    let (ravel_gets, ravel_lists, lake_gets) = reads(&lake);
    assert_eq!(ravel_lists - before.1, 1, "one LIST, of hits's versions");
    assert_eq!(
        (ravel_gets - before.0, lake_gets - before.2),
        (1, 0),
        "hits's newest manifest, and no file"
    );
}

/// A row window names an event-time column, which a Parquet table has none
/// of: it is refused rather than silently ignored, and before the table is
/// resolved, so the refusal costs no store request.
///
/// FLIP: refusing in `plan_pinned_with`, after resolve, leaves a manifest LIST
/// and two GETs in the counts.
#[tokio::test]
async fn a_row_window_is_refused_on_a_parquet_table() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let before = reads(&lake);
    let mut req = request("SELECT * FROM hits");
    req.row_window = true;
    let err = lake
        .executor
        .execute(acme, &req)
        .await
        .expect_err("row window");
    assert!(
        matches!(
            parquet_error(&err),
            Some(ParquetQueryError::RowWindowUnsupported)
        ),
        "{err}"
    );
    assert_eq!(err.class(), ErrorClass::BadRequest);
    assert_eq!(
        reads(&lake),
        before,
        "refused before resolve: no LIST or GET reached either store"
    );
}

/// Beside a signal table the row window applies to the signal scans only:
/// every `hits` row survives a `LEFT JOIN`, and only the logs records inside
/// the half-open window `[0, 3)` join to one.
///
/// FLIP: refusing the row window whenever the statement names a Parquet table
/// fails this statement with `RowWindowUnsupported`.
#[tokio::test]
async fn a_row_window_bounds_only_the_signal_scans_beside_a_parquet_table() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    hits_and_logs(&lake).await;
    let mut req = request(
        "SELECT h.id, l.body FROM hits h LEFT JOIN logs l \
         ON l.body = concat('line ', CAST(h.id AS VARCHAR)) ORDER BY h.id",
    );
    req.window = ravel_types::TimeRange {
        start_ns: 0,
        end_ns: 3,
    };
    req.row_window = true;
    let outcome = lake
        .executor
        .execute(acme, &req)
        .await
        .expect("row window beside Parquet");
    assert_eq!(
        rows(&outcome),
        vec!["1|line 1", "2|line 2", "3|", "4|", "5|", "6|"]
    );
    assert!(outcome.stats.window_predicate.is_some());

    // Non-vacuity: without the row window the same listing window joins all
    // four records.
    req.row_window = false;
    let outcome = lake.executor.execute(acme, &req).await.expect("no window");
    assert_eq!(
        rows(&outcome),
        vec!["1|line 1", "2|line 2", "3|line 3", "4|line 4", "5|", "6|"]
    );
}

/// `sql` for `acme` planned through the pinned lanes surface, the pair the
/// Flight SQL service runs, handing the plan the Parquet tables the resolve
/// read.
async fn pinned_lanes_plan(lake: &Lake, sql: &str) -> PinnedQuery {
    let acme = tenant("acme");
    let accounting = ravel_types::accounting::QueryAccounting::new();
    let lanes = lake
        .executor
        .resolve_pinned_lanes(acme, &request(sql), &accounting)
        .await
        .expect("resolve lanes");
    lake.executor
        .plan_pinned_lanes(
            acme,
            lanes.lanes,
            sql,
            &accounting,
            PinnedPlanInputs {
                declared: Vec::new(),
                parquet: ParquetPlan::Resolved(lanes.parquet),
                budgets: None,
            },
        )
        .await
        .expect("plan lanes")
}

/// [`pinned_lanes_plan`] through the one-snapshot pinned surface.
async fn pinned_plan(lake: &Lake, sql: &str) -> PinnedQuery {
    let acme = tenant("acme");
    let accounting = ravel_types::accounting::QueryAccounting::new();
    let resolved = lake
        .executor
        .resolve_pinned(acme, &request(sql), &accounting)
        .await
        .expect("resolve");
    lake.executor
        .plan_pinned_with_inputs(
            acme,
            resolved.snapshot,
            sql,
            &accounting,
            PinnedPlanInputs {
                declared: Vec::new(),
                parquet: ParquetPlan::Resolved(resolved.parquet),
                budgets: None,
            },
        )
        .await
        .expect("plan")
}

/// A statement naming no Parquet table plans in a session without the Parquet
/// store registry, on both pinned surfaces, while a mixed statement's session
/// has the registry and both tables.
///
/// FLIP: building every session with the registry makes the signal-only
/// session reach the tenant's Parquet store URL.
#[tokio::test]
async fn a_signal_only_statement_gets_no_parquet_store_registry() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    hits_and_logs(&lake).await;
    let signal_only = "SELECT body FROM logs";
    for plan in [
        pinned_lanes_plan(&lake, signal_only).await,
        pinned_plan(&lake, signal_only).await,
    ] {
        assert!(!plan.reaches_parquet_store(&acme));
        assert_eq!(plan.registered_tables(), vec!["logs"]);
    }
    for plan in [
        pinned_lanes_plan(&lake, HITS_JOIN_LOGS).await,
        pinned_plan(&lake, HITS_JOIN_LOGS).await,
    ] {
        assert!(plan.reaches_parquet_store(&acme));
        assert_eq!(plan.registered_tables(), vec!["hits", "logs"]);
    }
}

/// A statement naming only Parquet tables registers no signal table, on both
/// pinned surfaces; the pinned lanes pair is the one the Flight SQL service
/// runs. On HTTP it reports no `stats.signals` entry and returns the rows.
///
/// FLIP: registering the empty `samples` provider a Parquet-only statement's
/// lane used to carry adds `samples` to the session.
#[tokio::test]
async fn a_parquet_only_statement_registers_no_signal_table() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    hits_and_logs(&lake).await;
    let sql = "SELECT count(*) FROM hits";
    for plan in [
        pinned_lanes_plan(&lake, sql).await,
        pinned_plan(&lake, sql).await,
    ] {
        assert!(plan.reaches_parquet_store(&acme));
        assert_eq!(plan.registered_tables(), vec!["hits"]);
    }
    let outcome = lake.execute(&acme, sql).await.expect("HTTP");
    assert_eq!(rows(&outcome), vec!["6"]);
    assert!(outcome.signals.is_empty(), "{:?}", outcome.signals);
}

/// Each node named `name` in `plan`'s physical plan, as its one-line display
/// and its output partition count.
async fn scan_nodes(plan: &PinnedQuery, name: &str) -> Vec<(String, usize)> {
    fn walk(plan: &Arc<dyn ExecutionPlan>, name: &str, out: &mut Vec<(String, usize)>) {
        if plan.name() == name {
            out.push((
                displayable(plan.as_ref()).one_line().to_string(),
                plan.properties().partitioning.partition_count(),
            ));
        }
        for child in plan.children() {
            walk(child, name, out);
        }
    }
    let physical = plan.create_physical_plan().await.expect("physical plan");
    let mut out = Vec::new();
    walk(&physical, name, &mut out);
    out
}

/// A signal scan beside a Parquet table is the scan the same signal statement
/// plans alone: the Parquet session settings a mixed session takes
/// (`parquet_session_config`) leave the logs and samples scans' display and
/// partitioning unchanged.
#[tokio::test]
async fn a_signal_scan_plans_the_same_beside_a_parquet_table() {
    let lake = Lake::configured();
    hits_and_logs(&lake).await;
    util::publish_segment(
        lake.ravel.as_ref(),
        &util::tenant_id("acme"),
        0,
        &util::SegSpec::new(
            1,
            1,
            1,
            vec![util::SeriesSpec::new("cpu", vec![(1, 1.0), (2, 2.0)])],
        ),
    )
    .await;
    for (alone, beside, node) in [
        (
            "SELECT body FROM logs WHERE severity_num = 9",
            "SELECT body FROM logs WHERE severity_num = 9 UNION ALL SELECT name FROM hits",
            "LogsScanExec",
        ),
        (
            "SELECT value FROM samples WHERE value > 1",
            "SELECT value FROM samples WHERE value > 1 UNION ALL SELECT score FROM hits",
            "RsegScanExec",
        ),
    ] {
        let alone_nodes = scan_nodes(&pinned_lanes_plan(&lake, alone).await, node).await;
        let beside_nodes = scan_nodes(&pinned_lanes_plan(&lake, beside).await, node).await;
        assert_eq!(alone_nodes.len(), 1, "{alone}: {alone_nodes:?}");
        assert_eq!(beside_nodes, alone_nodes, "{beside}");
    }
}

/// A mixed statement's `stats.signals` holds one entry per signal table and
/// none for the Parquet table, and the entries plus the Parquet table's share
/// equal `stats.phases`, phase by phase: the share is what the same Parquet
/// read costs alone, measured from a fresh store.
#[tokio::test]
async fn a_mixed_statements_signals_and_parquet_share_sum_to_its_phases() {
    let sql = "SELECT body FROM logs UNION ALL SELECT name FROM hits";
    let (mixed_reads, mixed) = fresh_reads(sql).await;
    assert_eq!(rows(&mixed).len(), LOG_TS.len() + 6);
    assert_eq!(
        mixed
            .signals
            .iter()
            .map(|signal| signal.table)
            .collect::<Vec<_>>(),
        vec!["logs"]
    );
    let (parquet_reads, parquet) = fresh_reads("SELECT name FROM hits").await;
    assert!(parquet.signals.is_empty());
    let logs = &mixed.signals[0].phases;
    for phase in QueryPhase::ALL {
        let (signal, share, total) = (
            logs.phase(phase),
            parquet.phase_accounting.phase(phase),
            mixed.phase_accounting.phase(phase),
        );
        for op in AccountedOp::ALL {
            assert_eq!(
                signal.s3_requests(op) + share.s3_requests(op),
                total.s3_requests(op),
                "{phase:?} {op:?} requests"
            );
        }
        assert_eq!(
            signal.total_s3_bytes() + share.total_s3_bytes(),
            total.total_s3_bytes(),
            "{phase:?} bytes"
        );
    }
    assert!(
        parquet.accounting.total_s3_requests() > 0 && logs.pooled().total_s3_requests() > 0,
        "both sides read"
    );
    assert_eq!(
        mixed_reads.2, parquet_reads.2,
        "every lake GET is the Parquet table's"
    );
}

/// A mixed statement is held to one `max_s3_requests`: the Parquet reader
/// checks each request against the statement's total, the logs lane's reads
/// included, not against its own. The statement aggregates `logs` into the
/// build side of a nested loop join, through a filter so the aggregate reads
/// the logs object rather than its commit record's count, so the logs reads
/// are charged before the Parquet reader's data GETs, and a budget of one less
/// than the statement's unbudgeted cost refuses its last request, a lake GET,
/// before it is sent. The Parquet share alone is under that budget.
///
/// FLIP: checking the Parquet reader's own handle sees only the Parquet share,
/// which is under the budget, and the statement runs to its end.
#[tokio::test]
async fn a_mixed_statement_is_held_to_one_request_budget() {
    let sql = LOGS_AGGREGATE_BESIDE_HITS;
    let acme = tenant("acme");

    let (lake, log) = Lake::recorded(SqlConfig::default());
    let (_, logs_key) = hits_and_logs(&lake).await;
    log.lock().expect("request log").clear();
    let outcome = lake.execute(&acme, sql).await.expect("unbudgeted");
    assert_eq!(rows(&outcome), vec!["5", "6"]);
    let order = log.lock().expect("request log").clone();
    let total = outcome.accounting.total_s3_requests();
    assert_eq!(order.len() as u64, total, "every charged request is logged");
    let logs_share = outcome.signals[0].phases.pooled().total_s3_requests();
    let parquet_share = total - logs_share;
    let logs_read = order
        .iter()
        .position(|request| request == &format!("ravel GET {logs_key}"))
        .expect("the logs object is read");
    assert!(
        order
            .last()
            .is_some_and(|request| request.starts_with("lake GET")),
        "the last request is a lake GET"
    );
    assert!(
        logs_read < order.len() - 1,
        "the logs object is read before the last lake GET"
    );
    let unbudgeted_lake = Lake::gets(&lake.lake);
    let max = total - 1;
    assert!(
        parquet_share < max,
        "the Parquet share {parquet_share} alone is under the budget {max}"
    );

    let (lake, log) = Lake::recorded(budgeted_config(
        ByteLimit::Unlimited,
        RequestLimit::Bounded(max),
    ));
    hits_and_logs(&lake).await;
    log.lock().expect("request log").clear();
    let err = lake
        .execute(&acme, sql)
        .await
        .expect_err("one request over");
    assert!(
        matches!(
            err,
            SqlError::RequestBudgetExceeded { requests, max: limit, .. }
                if requests == total && limit == max
        ),
        "{err:?}"
    );
    let budgeted = log.lock().expect("request log").clone();
    assert_eq!(
        budgeted,
        order[..order.len() - 1],
        "every request but the unbudgeted run's last, in the same order"
    );
    assert_eq!(
        Lake::gets(&lake.lake),
        unbudgeted_lake - 1,
        "the refused request is the last lake GET"
    );
}

/// The statement of [`a_mixed_statement_is_held_to_one_request_budget`]: a
/// `logs` aggregate read before the Parquet table's data GETs.
const LOGS_AGGREGATE_BESIDE_HITS: &str = "SELECT h.id FROM \
     (SELECT count(*) AS n FROM logs WHERE body LIKE 'line%') c, hits h \
     WHERE h.id > c.n ORDER BY h.id";

/// A mixed statement is held to one `max_bytes_scanned`: the Parquet reader
/// checks each GET's bytes against the statement's total, the bytes the logs
/// lane read included. The `logs` scan checks no byte budget itself, so the
/// refusal is the Parquet reader's, of the statement's last request, a lake
/// GET, before it is sent, under a budget one byte below the statement's
/// unbudgeted wire bytes. The Parquet bytes alone are under that budget.
///
/// FLIP: checking the Parquet reader's own handle sees only the Parquet
/// bytes, which are under the budget, and the statement runs to its end.
#[tokio::test]
async fn a_mixed_statement_is_held_to_one_byte_budget() {
    let sql = LOGS_AGGREGATE_BESIDE_HITS;
    let acme = tenant("acme");

    let (lake, log) = Lake::recorded(SqlConfig::default());
    let (_, logs_key) = hits_and_logs(&lake).await;
    log.lock().expect("request log").clear();
    let outcome = lake.execute(&acme, sql).await.expect("unbudgeted");
    assert_eq!(rows(&outcome), vec!["5", "6"]);
    let order = log.lock().expect("request log").clone();
    let total = outcome.accounting.total_s3_bytes();
    let logs_bytes = outcome.signals[0].phases.pooled().total_s3_bytes();
    let parquet_bytes = total - logs_bytes;
    assert!(logs_bytes > 0, "the logs lane read bytes");
    let logs_read = order
        .iter()
        .position(|request| request == &format!("ravel GET {logs_key}"))
        .expect("the logs object is read");
    assert!(
        order
            .last()
            .is_some_and(|request| request.starts_with("lake GET")),
        "the last request is a lake GET"
    );
    assert!(
        logs_read < order.len() - 1,
        "the logs object is read before the last lake GET"
    );
    let unbudgeted_lake = Lake::gets(&lake.lake);
    let max = total - 1;
    assert!(
        parquet_bytes < max,
        "the Parquet bytes {parquet_bytes} alone are under the budget {max}"
    );

    let (lake, log) = Lake::recorded(budgeted_config(
        ByteLimit::Bounded(max),
        RequestLimit::Unlimited,
    ));
    hits_and_logs(&lake).await;
    log.lock().expect("request log").clear();
    let err = lake.execute(&acme, sql).await.expect_err("one byte over");
    assert!(
        matches!(
            err,
            SqlError::TooManyBytesScanned { scanned, max: limit }
                if scanned == total && limit == max
        ),
        "{err:?}"
    );
    let budgeted = log.lock().expect("request log").clone();
    assert_eq!(
        budgeted,
        order[..order.len() - 1],
        "every request but the unbudgeted run's last, in the same order"
    );
    assert_eq!(
        Lake::gets(&lake.lake),
        unbudgeted_lake - 1,
        "the refused request is the last lake GET"
    );
}

/// A lane's resolve-time request budget check counts the Parquet resolve
/// reads, because the Parquet tables resolve first. The budget here is at
/// least the logs lane's own resolve reads and the Parquet estimate (its
/// resolve reads plus one GET per file), so neither check alone refuses, and
/// below their sum: the logs lane's check refuses with exactly that sum,
/// before any scan GET.
///
/// FLIP: resolving the Parquet tables after the lanes lets the logs lane's
/// check see its own reads alone, and the Parquet check then refuses with
/// its estimate on top of the logs reads, a different count.
#[tokio::test]
async fn a_lanes_resolve_budget_check_counts_the_parquet_resolve_reads() {
    let acme = tenant("acme");
    let (_, outcome) = fresh_reads(HITS_JOIN_LOGS).await;
    let resolve = outcome
        .phase_accounting
        .phase(QueryPhase::Resolve)
        .total_s3_requests();
    let logs_resolve = outcome.signals[0]
        .phases
        .phase(QueryPhase::Resolve)
        .total_s3_requests();
    let parquet_resolve = resolve - logs_resolve;
    let files = hits_files().len() as u64;
    let max = resolve - 1;
    assert!(
        parquet_resolve > 0 && logs_resolve <= max && parquet_resolve + files <= max,
        "the budget {max} admits the logs resolve {logs_resolve} and the Parquet \
         estimate {parquet_resolve} + {files} alone"
    );

    let (lake, log) = Lake::recorded(budgeted_config(
        ByteLimit::Unlimited,
        RequestLimit::Bounded(max),
    ));
    let (_, logs_key) = hits_and_logs(&lake).await;
    log.lock().expect("request log").clear();
    let err = lake
        .execute(&acme, HITS_JOIN_LOGS)
        .await
        .expect_err("over at resolve");
    assert!(
        matches!(
            err,
            SqlError::RequestBudgetExceeded { requests, max: limit, table: None, .. }
                if requests == resolve && limit == max
        ),
        "{err:?}"
    );
    let sent = log.lock().expect("request log").clone();
    assert_eq!(
        sent.len() as u64,
        resolve,
        "only the resolve reads: {sent:?}"
    );
    assert!(
        !sent.iter().any(|request| request.starts_with("lake GET")
            || request == &format!("ravel GET {logs_key}")),
        "no scan GET: {sent:?}"
    );
}

/// A mixed statement's reported estimate is the Parquet-only estimate of its
/// Parquet table plus the logs-only estimate, field by field.
///
/// FLIP: starting the lanes' sum from zero drops the Parquet term, and the
/// estimate equals the logs-only one.
#[tokio::test]
async fn a_mixed_estimate_is_the_parquet_estimate_plus_the_lanes() {
    let (_, mixed) = fresh_reads(HITS_JOIN_LOGS).await;
    let (_, parquet) = fresh_reads("SELECT id FROM hits").await;
    let (_, logs) = fresh_reads("SELECT body FROM logs").await;
    assert!(
        parquet.estimate.estimated_requests > 0 && logs.estimate.estimated_requests > 0,
        "both terms are non-zero: {:?} {:?}",
        parquet.estimate,
        logs.estimate
    );
    assert_eq!(
        mixed.estimate,
        parquet.estimate.saturating_add(&logs.estimate)
    );
}

/// The pinned pair the Flight SQL transport uses, `resolve_snapshot` then
/// `plan_pinned`, reaches the same Parquet tables and rows as `execute`.
#[tokio::test]
async fn the_pinned_surface_reads_the_same_rows() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let sql = "SELECT id FROM hits WHERE name = 'a' ORDER BY id";
    let accounting = ravel_types::accounting::QueryAccounting::new();
    let (snapshot, _) = lake
        .executor
        .resolve_snapshot(acme, &request(sql), &accounting)
        .await
        .expect("resolve");
    assert!(snapshot.segments.is_empty());
    let planned = lake
        .executor
        .plan_pinned(acme, snapshot, sql, &accounting, &[])
        .await
        .expect("plan");
    let mut stream = planned.execute().await.expect("execute");
    let mut ids = Vec::new();
    while let Some(batch) = futures::StreamExt::next(&mut stream).await {
        let batch = batch.expect("batch");
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        ids.extend((0..column.len()).map(|row| column.value(row)));
    }
    assert_eq!(ids, vec![1, 4]);
    let http = lake.execute(&acme, sql).await.expect("execute");
    assert_eq!(rows(&http), vec!["1", "4"]);
}

/// The keys the services from [`flight_harness`] sign their tickets with, so
/// a test can open a ticket, change a field, and sign it again.
#[cfg(feature = "flight-sql")]
fn flight_ticket_keys() -> ravel_sql::SqlTicketKeys {
    ravel_sql::SqlTicketKeys::from_file_key(b"parquet-tables-test-key")
}

/// A Flight SQL service over `lake`'s executor, authenticating `tenant` under
/// the token `acme`.
#[cfg(feature = "flight-sql")]
fn flight_harness(lake: &Lake, tenant: &TenantId) -> util::flight_harness::Harness {
    flight_harness_with(
        lake,
        util::flight_harness::TestAuth::new(&[("acme", tenant)]),
    )
}

/// [`flight_harness`] authenticating through `auth`.
#[cfg(feature = "flight-sql")]
fn flight_harness_with(
    lake: &Lake,
    auth: Arc<dyn ravel_sql::FlightAuth>,
) -> util::flight_harness::Harness {
    use std::time::Duration;

    use ravel_sql::{FlightClock, FlightSqlConfig, RavelFlightSqlService};
    use util::flight_harness::{Harness, TestClock};

    let clock = TestClock::at(util::NOW_NS);
    let service = RavelFlightSqlService::new(
        Arc::clone(&lake.executor),
        auth,
        Arc::clone(&clock) as Arc<dyn FlightClock>,
        FlightSqlConfig {
            max_deadline: Duration::from_secs(30),
            ..FlightSqlConfig::default()
        },
        Arc::new(ravel_types::accounting::NoopQueryCostRecorder),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
    )
    .with_ticket_keys(flight_ticket_keys());
    Harness {
        service,
        executor: Arc::clone(&lake.executor),
        clock,
        store: Arc::clone(&lake.ravel) as Arc<dyn ObjectStoreBackend>,
    }
}

/// Flight SQL reaches Parquet tables through the same executor funnel:
/// `GetFlightInfo` then `DoGet` return the rows `execute` returns.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn flight_sql_reads_a_parquet_table() {
    use util::flight_harness::merged;

    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    lake.hits_for(&acme.hash()).await;
    let harness = flight_harness(&lake, &acme);
    let sql = "SELECT id, name FROM hits WHERE id >= 4 ORDER BY id";
    let ticket = harness
        .get_flight_info("acme", sql)
        .await
        .expect("flight info");
    let flight = harness.do_get("acme", &ticket).await.expect("do get");
    let http = lake.execute(&acme.hash(), sql).await.expect("execute");
    assert_eq!(rows(&http), vec!["4|a", "5|b", "6|c"]);
    assert_eq!(merged(&flight), merged(http.output.batches()));
}

/// `GetFlightInfo` resolves a Parquet table once: one LIST of its manifest
/// prefix, one GET of the newest manifest and one GET of the grants record on
/// Ravel's store, which is what a single `execute` reads too. Planning the
/// statement after the resolve used to resolve the table a second time.
///
/// FLIP: planning with `ParquetPlan::Unresolved` in `get_flight_info_statement`
/// reads 2 LISTs and 4 GETs.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn get_flight_info_resolves_a_parquet_table_once() {
    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    lake.hits_for(&acme.hash()).await;
    let harness = flight_harness(&lake, &acme);

    let (gets, lists) = (Lake::gets(&lake.ravel), Lake::lists(&lake.ravel));
    harness
        .get_flight_info("acme", "SELECT id FROM hits ORDER BY id")
        .await
        .expect("flight info");
    assert_eq!(
        Lake::lists(&lake.ravel) - lists,
        1,
        "one LIST of the manifest prefix"
    );
    assert_eq!(
        Lake::gets(&lake.ravel) - gets,
        2,
        "one GET of the newest manifest and one of the grants record"
    );
}

/// One row group of `id: Int64` and `extra: Utf8`: a schema other than
/// [`parquet_bytes`]'s, for a table replaced by one of another shape.
#[cfg(feature = "flight-sql")]
fn replacement_parquet_bytes(ids: &[i64], extras: &[&str]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(extras.to_vec())),
        ],
    )
    .expect("batch");
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
    Bytes::from(out)
}

#[cfg(feature = "flight-sql")]
fn field_names(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

/// A table replaced between `GetFlightInfo` and `DoGet` (a newer manifest
/// version with another schema) is still read at the version `GetFlightInfo`
/// resolved: `DoGet` streams that version's rows under the schema the
/// `FlightInfo` advertised, and a statement planned after the replacement sees
/// the new table.
///
/// FLIP: a `DoGet` that resolves the table's newest manifest streams the
/// replacement's `id, extra` schema and rows.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn a_table_replaced_between_the_rpcs_is_read_at_the_pinned_version() {
    use util::flight_harness::merged;

    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    lake.hits_for(&acme.hash()).await;
    let harness = flight_harness(&lake, &acme);
    let sql = "SELECT * FROM hits ORDER BY id";

    let info = harness
        .get_flight_info_full("acme", sql)
        .await
        .expect("flight info");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");
    let advertised = info.try_decode_schema().expect("schema");
    assert_eq!(field_names(&advertised), vec!["id", "name", "score"]);

    let replacement = lake
        .put_file(
            "t/hits/replacement.parquet",
            replacement_parquet_bytes(&[10, 20], &["x", "y"]),
        )
        .await;
    lake.replace(&acme.hash(), "hits", vec![replacement]).await;

    let streamed = harness.do_get("acme", &ticket).await.expect("do get");
    let batch = merged(&streamed);
    assert_eq!(
        field_names(batch.schema().as_ref()),
        vec!["id", "name", "score"],
        "the schema GetFlightInfo advertised"
    );
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");
    assert_eq!(
        (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6],
        "the rows of the pinned version"
    );

    // Non-vacuity: the replacement is live, so a statement planned now sees it.
    let later = harness
        .get_flight_info_full("acme", sql)
        .await
        .expect("flight info after the replacement");
    assert_eq!(
        field_names(&later.try_decode_schema().expect("schema")),
        vec!["id", "extra"]
    );
}

/// A grant removed between the RPCs fails `DoGet` with the typed refusal the
/// one-shot path gives, before any file is read: the ticket pins manifests, not
/// the tenant's grants (ADR-2040 D3).
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn a_grant_removed_between_the_rpcs_fails_do_get() {
    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    lake.hits_for(&acme.hash()).await;
    let harness = flight_harness(&lake, &acme);
    let ticket = harness
        .get_flight_info("acme", "SELECT id FROM hits ORDER BY id")
        .await
        .expect("flight info");

    grants::remove(lake.ravel.inner(), &acme.hash(), GRANT)
        .await
        .expect("revoke");
    let lake_before = Lake::gets(&lake.lake);
    let status = harness
        .do_get("acme", &ticket)
        .await
        .expect_err("the grant is gone");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status
            .message()
            .contains("outside every location currently granted"),
        "{}",
        status.message()
    );
    assert_eq!(
        Lake::gets(&lake.lake),
        lake_before,
        "no file of the table was read"
    );
}

/// `DoGet` reads the pinned manifest object by its version and the grants
/// record: two GETs on Ravel's store and no LIST of the manifest prefix.
///
/// FLIP: resolving the newest manifest at `DoGet` adds one LIST.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn do_get_reads_the_pinned_manifest_without_a_list() {
    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    lake.hits_for(&acme.hash()).await;
    let harness = flight_harness(&lake, &acme);
    let ticket = harness
        .get_flight_info("acme", "SELECT id FROM hits ORDER BY id")
        .await
        .expect("flight info");

    let (gets, lists) = (Lake::gets(&lake.ravel), Lake::lists(&lake.ravel));
    harness.do_get("acme", &ticket).await.expect("do get");
    assert_eq!(Lake::lists(&lake.ravel) - lists, 0, "no LIST");
    assert_eq!(
        Lake::gets(&lake.ravel) - gets,
        2,
        "the pinned manifest and the grants record"
    );
}

/// A pinned manifest version that no longer exists (swept after a newer one
/// replaced it) invalidates the ticket: `DoGet` does not fall back to the
/// newest version.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn a_pinned_manifest_that_is_gone_invalidates_the_ticket() {
    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    lake.hits_for(&acme.hash()).await;
    let harness = flight_harness(&lake, &acme);
    let ticket = harness
        .get_flight_info("acme", "SELECT id FROM hits ORDER BY id")
        .await
        .expect("flight info");

    let replacement = lake
        .put_file(
            "t/hits/replacement.parquet",
            replacement_parquet_bytes(&[10, 20], &["x", "y"]),
        )
        .await;
    lake.replace(&acme.hash(), "hits", vec![replacement]).await;
    let pinned = ravel_pqtable::keys::manifest_key(&acme.hash(), "hits", 1).expect("key");
    lake.ravel.inner().delete(&pinned).await.expect("sweep");

    let status = harness
        .do_get("acme", &ticket)
        .await
        .expect_err("the pinned version is gone");
    assert_eq!(status.code(), tonic::Code::Unavailable);
}

/// A statement over two Parquet tables pins both, and each is read at the
/// version `GetFlightInfo` saw: replacing one of them afterwards changes
/// neither the join's schema nor its rows.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn a_flight_join_of_two_parquet_tables_pins_both() {
    use util::flight_harness::merged;

    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    lake.hits_for(&acme.hash()).await;
    let names = lake
        .put_file(
            "t/names/0.parquet",
            parquet_bytes(&[1, 5], &["first", "fifth"], &[0.0, 0.0]),
        )
        .await;
    lake.create(&acme.hash(), "names", vec![names]).await;
    let harness = flight_harness(&lake, &acme);
    let sql = "SELECT h.id, n.name FROM hits h JOIN names n ON h.id = n.id ORDER BY h.id";
    let ticket = harness
        .get_flight_info("acme", sql)
        .await
        .expect("flight info");

    let replacement = lake
        .put_file(
            "t/names/1.parquet",
            parquet_bytes(&[2], &["second"], &[0.0]),
        )
        .await;
    lake.replace(&acme.hash(), "names", vec![replacement]).await;

    let streamed = harness.do_get("acme", &ticket).await.expect("do get");
    let batch = merged(&streamed);
    let names: Vec<String> = (0..batch.num_rows())
        .map(|row| array_value_to_string(batch.column(1), row).expect("cell"))
        .collect();
    assert_eq!(
        names,
        vec!["first", "fifth"],
        "the join reads `names` at the version it had when the ticket was minted"
    );
}

/// `groups` row groups of `rows` rows each, so every column chunk is about
/// `8 * rows` bytes: `id: Int64` counting up from 0, `name: Utf8` and
/// `score: Float64` (`id / 2`). Dictionary encoding and compression are off,
/// which keeps a chunk's size a function of its row count alone.
fn big_parquet_bytes(rows: usize, groups: usize) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(rows))
        .build();
    let mut out = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut out, Arc::clone(&schema), Some(properties)).expect("writer");
    for group in 0..groups {
        let ids: Vec<i64> = (0..rows).map(|row| (group * rows + row) as i64).collect();
        let scores: Vec<f64> = ids.iter().map(|id| *id as f64 / 2.0).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(vec!["n"; rows])),
                Arc::new(Float64Array::from(scores)),
            ],
        )
        .expect("batch");
        writer.write(&batch).expect("write");
        writer.flush().expect("one row group per batch");
    }
    writer.close().expect("close");
    Bytes::from(out)
}

/// The bytes of column `column` in the first row group of `file`, and the
/// footer's own length, from the bytes themselves.
fn chunk_and_footer_len(file: &Bytes, column: usize) -> (u64, u64) {
    let end = file.len() - 8;
    let mut word = [0u8; 4];
    word.copy_from_slice(&file[end..end + 4]);
    let footer_len = u64::from(u32::from_le_bytes(word));
    let metadata = parquet::file::metadata::ParquetMetaDataReader::decode_metadata(
        &file[end - footer_len as usize..end],
    )
    .expect("footer");
    let chunk = metadata.row_group(0).column(column).byte_range().1;
    (chunk, footer_len)
}

/// One table `big` over one two-row-group file whose `id` and `score` chunks
/// are each 8 * `ROWS` bytes; the file's bytes are returned with it.
const ROWS: usize = 1 << 19;

async fn big_for(lake: &Lake, tenant: &TenantHash) -> Bytes {
    let bytes = big_parquet_bytes(ROWS, 2);
    let file = lake.put_file("t/big/0.parquet", bytes.clone()).await;
    lake.grant(tenant).await;
    lake.create(tenant, "big", vec![file]).await;
    bytes
}

/// The memory error a signal fetcher's refused reservation is, with the same
/// figures, built for the comparison of class, status and client message.
fn signal_memory_error(requested: u64, reserved: u64, limit: u64) -> SqlError {
    SqlError::Fetch(ravel_query::FetchError::FetchMemoryExhausted {
        requested,
        reserved,
        limit,
        cause: ravel_memory::ExhaustionCause::Accounted,
    })
}

/// A column chunk larger than the process memory budget is refused with the
/// signal fetchers' `FetchMemoryExhausted` (same class and client message)
/// and its GET is never issued; a budget that fits one chunk at a time admits
/// a query that reads two chunks one after the other, because the first
/// chunk's reservation is released when parquet drops its buffer; and a query
/// that needs two chunks at once is refused by that same budget.
///
/// FLIP: reserving after the GET leaves a second lake GET in the first case;
/// never releasing (or releasing only at the end of the query) fails the
/// sequential case with `FetchMemoryExhausted` for the second chunk; and a
/// release at the moment the GET returns leaves `reserved()` at zero in the
/// mid-stream check.
#[tokio::test]
async fn a_parquet_read_past_the_memory_budget_is_refused() {
    let acme = tenant("acme");
    let sum_id = "SELECT sum(id) FROM big";
    let expected_sum = (0..(2 * ROWS) as i64).sum::<i64>().to_string();

    // A budget under one chunk: the footer read fits, the chunk's does not.
    let probe = Lake::configured();
    let bytes = big_for(&probe, &acme).await;
    let (chunk, footer_len) = chunk_and_footer_len(&bytes, 0);
    assert!(
        chunk > 100_000,
        "a chunk of {chunk} bytes dwarfs the footer"
    );
    assert!(footer_len + 8 < chunk / 10);

    let budget = Arc::new(MemoryBudget::new(chunk - 1));
    let refused = Lake::budgeted(SqlConfig::default(), Arc::clone(&budget));
    big_for(&refused, &acme).await;
    let before = Lake::gets(&refused.lake);
    let err = refused
        .execute(&acme, sum_id)
        .await
        .expect_err("a chunk over the budget");
    let signal = match &err {
        SqlError::Fetch(ravel_query::FetchError::FetchMemoryExhausted {
            requested,
            reserved,
            limit,
            cause: ravel_memory::ExhaustionCause::Accounted,
        }) => {
            assert_eq!(*requested, chunk);
            assert_eq!(*limit, chunk - 1);
            signal_memory_error(*requested, *reserved, *limit)
        }
        other => panic!("expected an accounted FetchMemoryExhausted, got {other:?}"),
    };
    assert_eq!(err.class(), signal.class());
    assert_eq!(err.client_message(), signal.client_message());
    assert_eq!(
        Lake::gets(&refused.lake) - before,
        1,
        "the footer, and no GET for the chunk that was refused"
    );
    assert_eq!(
        budget.reserved(),
        0,
        "nothing stays reserved after a refusal"
    );

    // A budget of one and a half chunks (the half is the SQL pool's own
    // draw on the same budget): two chunks, one at a time.
    let budget = Arc::new(MemoryBudget::new(chunk + chunk / 2));
    let fits = Lake::budgeted(SqlConfig::default(), Arc::clone(&budget));
    big_for(&fits, &acme).await;
    let outcome = fits
        .execute(&acme, sum_id)
        .await
        .expect("the first chunk's reservation was released before the second");
    assert_eq!(rows(&outcome), vec![expected_sum.clone()]);
    assert_eq!(budget.reserved(), 0);

    // While the stream is inside the first row group its chunk is reserved:
    // the reservation follows the buffer, not the GET.
    let accounting = ravel_types::accounting::QueryAccounting::new();
    let (snapshot, _) = fits
        .executor
        .resolve_snapshot(acme, &request("SELECT id FROM big"), &accounting)
        .await
        .expect("resolve");
    let planned = fits
        .executor
        .plan_pinned(acme, snapshot, "SELECT id FROM big", &accounting, &[])
        .await
        .expect("plan");
    let mut stream = planned.execute().await.expect("execute");
    let first = futures::StreamExt::next(&mut stream)
        .await
        .expect("a batch")
        .expect("batch");
    assert!(
        first.num_rows() < ROWS,
        "the row group is not yet exhausted"
    );
    assert!(
        budget.reserved() >= chunk,
        "the chunk is reserved while parquet holds it: {}",
        budget.reserved()
    );
    drop(stream);
    // The stream's partition tasks are aborted, not joined, by the drop.
    for _ in 0..1_000 {
        if budget.reserved() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(budget.reserved(), 0, "released when the data is dropped");

    // Two chunks at once (`id` and `score` of one row group) do not fit the
    // same budget, and neither is requested.
    let before = Lake::gets(&fits.lake);
    let err = fits
        .execute(&acme, "SELECT sum(id), sum(score) FROM big")
        .await
        .expect_err("two chunks over a one-chunk budget");
    assert!(
        matches!(
            err,
            SqlError::Fetch(ravel_query::FetchError::FetchMemoryExhausted { .. })
        ),
        "{err:?}"
    );
    assert_eq!(
        Lake::gets(&fits.lake),
        before,
        "the batch is refused before either chunk is requested"
    );

    // A footer read that does not fit is refused when the table is built.
    let tiny = Lake::budgeted(
        SqlConfig::default(),
        Arc::new(MemoryBudget::new(footer_len)),
    );
    big_for(&tiny, &acme).await;
    let err = tiny
        .execute(&acme, sum_id)
        .await
        .expect_err("a footer over the budget");
    assert!(
        matches!(
            err,
            SqlError::Fetch(ravel_query::FetchError::FetchMemoryExhausted { .. })
        ),
        "{err:?}"
    );
    assert_eq!(Lake::gets(&tiny.lake), 0, "no GET for the refused footer");
}

/// `SqlConfig::default()` with the byte and request budgets replaced.
fn budgeted_config(max_bytes_scanned: ByteLimit, max_s3_requests: RequestLimit) -> SqlConfig {
    SqlConfig {
        engine: EngineConfig {
            max_bytes_scanned,
            max_s3_requests,
            ..EngineConfig::default()
        },
        ..SqlConfig::default()
    }
}

/// Every LIST and GET that has reached Ravel's store or the lake.
fn store_requests(lake: &Lake) -> u64 {
    let (ravel_gets, ravel_lists, lake_gets) = reads(lake);
    ravel_gets + ravel_lists + lake_gets
}

/// What `sql` costs over `hits` with no budget: the resolve phase's
/// requests, every phase's requests, and every phase's wire bytes.
async fn unbudgeted_cost(sql: &str) -> (u64, u64, u64) {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let outcome = lake.execute(&acme, sql).await.expect("unbudgeted");
    let phases = &outcome.phase_accounting;
    let resolve = phases.phase(QueryPhase::Resolve).total_s3_requests();
    let pooled = phases.pooled();
    (resolve, pooled.total_s3_requests(), pooled.total_s3_bytes())
}

/// A Parquet query is held to `max_s3_requests` at resolve and again as each
/// request is issued: the request that would be one past the budget is never
/// sent, whatever phase it belongs to.
///
/// FLIP: counting only the requests the resolve made (the wrong
/// implementation) admits the budget of 6 here and the query runs to its end,
/// so the execution-time `expect_err` fails; dropping the resolve check
/// leaves the first case running three data GETs.
#[tokio::test]
async fn a_parquet_query_over_its_request_budget_is_refused() {
    let sql = "SELECT sum(id) FROM hits";
    let (resolve, total, _) = unbudgeted_cost(sql).await;
    let files = hits_files().len() as u64;
    let floor = resolve + files;
    assert!(
        total > floor,
        "the scan issues requests beyond one per file: {total} against a floor of {floor}"
    );

    // Below the floor: refused at resolve, having read only the manifest and
    // the grants record.
    let lake = Lake::new(
        true,
        budgeted_config(ByteLimit::Unlimited, RequestLimit::Bounded(floor - 1)),
    );
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let before = store_requests(&lake);
    let err = lake.execute(&acme, sql).await.expect_err("over at resolve");
    assert!(
        matches!(
            err,
            SqlError::RequestBudgetExceeded { requests, max, .. }
                if requests == floor && max == floor - 1
        ),
        "{err:?}"
    );
    assert_eq!(err.class(), ErrorClass::Unsupported);
    assert_eq!(Lake::gets(&lake.lake), 0, "no data GET before the refusal");
    assert_eq!(
        store_requests(&lake) - before,
        resolve,
        "the refusal came from the resolve's own reads"
    );

    // At the floor: the resolve admits it, and the first request past the
    // budget is refused before it is issued.
    let lake = Lake::new(
        true,
        budgeted_config(ByteLimit::Unlimited, RequestLimit::Bounded(floor)),
    );
    lake.hits_for(&acme).await;
    let before = store_requests(&lake);
    let err = lake.execute(&acme, sql).await.expect_err("over at scan");
    assert!(
        matches!(
            err,
            SqlError::RequestBudgetExceeded { requests, max, .. }
                if requests == floor + 1 && max == floor
        ),
        "{err:?}"
    );
    assert_eq!(
        store_requests(&lake) - before,
        floor,
        "exactly the budget's requests reached a store, none past it"
    );
    assert!(Lake::gets(&lake.lake) > 0, "it got as far as reading data");

    // A budget of exactly the query's cost runs it; one less stops it one
    // request short.
    let lake = Lake::new(
        true,
        budgeted_config(ByteLimit::Unlimited, RequestLimit::Bounded(total)),
    );
    lake.hits_for(&acme).await;
    let outcome = lake.execute(&acme, sql).await.expect("exactly affordable");
    assert_eq!(rows(&outcome), vec!["21"]);
    let lake = Lake::new(
        true,
        budgeted_config(ByteLimit::Unlimited, RequestLimit::Bounded(total - 1)),
    );
    lake.hits_for(&acme).await;
    let before = store_requests(&lake);
    lake.execute(&acme, sql).await.expect_err("one short");
    assert_eq!(store_requests(&lake) - before, total - 1);
}

/// `max_bytes_scanned` counts the wire bytes of each GET body, the resolve
/// phase's manifest and grants reads included, and refuses the request whose
/// body would cross it before that request is issued.
///
/// FLIP: checking the recorded bytes after the GET (as the signal scan does
/// per segment) leaves the refused chunk's GET in the lake's count; comparing
/// the recorded bytes without the range about to be read admits the chunk and
/// the query runs to its end.
#[tokio::test]
async fn a_parquet_query_over_its_byte_budget_is_refused() {
    let acme = tenant("acme");
    let sql = "SELECT sum(id) FROM big";
    let unbudgeted = Lake::configured();
    let bytes = big_for(&unbudgeted, &acme).await;
    let (chunk, _) = chunk_and_footer_len(&bytes, 0);
    let outcome = unbudgeted.execute(&acme, sql).await.expect("unbudgeted");
    let total = outcome.phase_accounting.pooled().total_s3_bytes();
    let lake_gets = Lake::gets(&unbudgeted.lake);
    assert!(total > 2 * chunk, "two chunks, a footer and the manifests");

    // Below one chunk: the footer is read, the chunk is not.
    let lake = Lake::new(
        true,
        budgeted_config(ByteLimit::Bounded(chunk - 1), RequestLimit::Unlimited),
    );
    big_for(&lake, &acme).await;
    let err = lake.execute(&acme, sql).await.expect_err("under one chunk");
    match err {
        SqlError::TooManyBytesScanned { scanned, max } => {
            assert_eq!(max, chunk - 1);
            assert!(
                scanned > chunk && scanned - chunk < 4096,
                "the chunk on top of what the resolve and the footer read: {scanned}"
            );
        }
        other => panic!("expected TooManyBytesScanned, got {other:?}"),
    }
    assert_eq!(
        Lake::gets(&lake.lake),
        1,
        "the footer, and no GET for the chunk that was refused"
    );

    // Exactly the bytes the query reads: admitted. One fewer: the last chunk
    // is refused before its GET.
    let lake = Lake::new(
        true,
        budgeted_config(ByteLimit::Bounded(total), RequestLimit::Unlimited),
    );
    big_for(&lake, &acme).await;
    lake.execute(&acme, sql).await.expect("exactly affordable");
    let lake = Lake::new(
        true,
        budgeted_config(ByteLimit::Bounded(total - 1), RequestLimit::Unlimited),
    );
    big_for(&lake, &acme).await;
    let err = lake.execute(&acme, sql).await.expect_err("one byte short");
    assert!(
        matches!(err, SqlError::TooManyBytesScanned { scanned, max }
            if scanned == total && max == total - 1),
        "{err:?}"
    );
    assert_eq!(
        Lake::gets(&lake.lake),
        lake_gets - 1,
        "the read that would cross the budget was not issued"
    );
}

/// A request's own budgets, clamped to the engine's by `effective_config`,
/// govern a Parquet query: a request budget lower than the engine's refuses,
/// and one higher than the engine's does not raise it.
///
/// FLIP: building the limits from `self.config` instead of the effective
/// config runs the lowered-budget statements to their ends.
#[tokio::test]
async fn a_clamped_request_budget_governs_a_parquet_query() {
    let acme = tenant("acme");
    let sql = "SELECT sum(id) FROM hits";
    let (resolve, total, _) = unbudgeted_cost(sql).await;
    let floor = resolve + hits_files().len() as u64;

    // The engine's own budget is far above this query; the request lowers it.
    let lake = Lake::configured();
    lake.hits_for(&acme).await;
    let mut req = request(sql);
    req.budgets = Some(RequestBudgets {
        max_store_requests: Some(RequestLimit::Bounded(floor - 1)),
        ..RequestBudgets::default()
    });
    let err = lake
        .executor
        .execute(acme, &req)
        .await
        .expect_err("lowered request budget, at resolve");
    assert!(
        matches!(err, SqlError::RequestBudgetExceeded { max, .. } if max == floor - 1),
        "{err:?}"
    );
    assert_eq!(Lake::gets(&lake.lake), 0);

    let mut req = request(sql);
    req.budgets = Some(RequestBudgets {
        max_store_requests: Some(RequestLimit::Bounded(floor)),
        ..RequestBudgets::default()
    });
    let before = store_requests(&lake);
    let err = lake
        .executor
        .execute(acme, &req)
        .await
        .expect_err("lowered request budget, at scan");
    assert!(
        matches!(err, SqlError::RequestBudgetExceeded { max, .. } if max == floor),
        "{err:?}"
    );
    assert_eq!(store_requests(&lake) - before, floor);

    let mut req = request(sql);
    req.budgets = Some(RequestBudgets {
        max_bytes_scanned: Some(ByteLimit::Bounded(1)),
        ..RequestBudgets::default()
    });
    let err = lake
        .executor
        .execute(acme, &req)
        .await
        .expect_err("lowered byte budget");
    assert!(
        matches!(err, SqlError::TooManyBytesScanned { max: 1, .. }),
        "{err:?}"
    );

    // A request cannot raise the engine's budget.
    let lake = Lake::new(
        true,
        budgeted_config(ByteLimit::Unlimited, RequestLimit::Bounded(total - 1)),
    );
    lake.hits_for(&acme).await;
    let mut req = request(sql);
    req.budgets = Some(RequestBudgets {
        max_store_requests: Some(RequestLimit::Bounded(u64::MAX)),
        ..RequestBudgets::default()
    });
    let err = lake
        .executor
        .execute(acme, &req)
        .await
        .expect_err("the engine's budget stands");
    assert!(
        matches!(err, SqlError::RequestBudgetExceeded { max, .. } if max == total - 1),
        "{err:?}"
    );
}

/// Plan `sql` over an already-resolved statement the way the Flight service
/// does and drain it, so a refusal at plan time or at scan time is one error.
async fn plan_and_drain(
    lake: &Lake,
    tenant: TenantHash,
    resolved: &ravel_sql::PinnedResolve,
    sql: &str,
    accounting: &ravel_types::accounting::QueryAccounting,
    budgets: Option<RequestBudgets>,
) -> Result<Vec<RecordBatch>, SqlError> {
    let planned = lake
        .executor
        .plan_pinned_with_inputs(
            tenant,
            resolved.snapshot.clone(),
            sql,
            accounting,
            ravel_sql::PinnedPlanInputs {
                declared: Vec::new(),
                parquet: ravel_sql::ParquetPlan::Resolved(resolved.parquet.clone()),
                budgets,
            },
        )
        .await?;
    let mut stream = planned.execute().await?;
    let mut batches = Vec::new();
    while let Some(batch) = futures::StreamExt::next(&mut stream).await {
        batches.push(batch?);
    }
    Ok(batches)
}

/// A request's clamped `max_store_requests` binds the scan of a pinned plan,
/// not only the resolve: `resolve_pinned` admits a statement at exactly its
/// floor, and the plan built from that resolve is refused at scan by the
/// clamped figure, not run under the executor's far higher ceiling.
///
/// FLIP: a plan built with `budgets: None`, which is what `plan_pinned` builds,
/// runs the same statement to its end (the control below).
#[tokio::test]
async fn a_clamped_request_budget_binds_the_pinned_plans_scan() {
    let acme = tenant("acme");
    let sql = "SELECT sum(id) FROM hits";
    let (resolve, _, _) = unbudgeted_cost(sql).await;
    let floor = resolve + hits_files().len() as u64;

    let lake = Lake::configured();
    lake.hits_for(&acme).await;
    let mut req = request(sql);
    req.budgets = Some(RequestBudgets {
        max_store_requests: Some(RequestLimit::Bounded(floor)),
        ..RequestBudgets::default()
    });

    let accounting = ravel_types::accounting::QueryAccounting::new();
    let resolved = lake
        .executor
        .resolve_pinned(acme, &req, &accounting)
        .await
        .expect("the resolve admits a budget of exactly its floor");
    let err = plan_and_drain(&lake, acme, &resolved, sql, &accounting, req.budgets)
        .await
        .expect_err("refused at scan");
    assert!(
        matches!(
            err,
            SqlError::RequestBudgetExceeded { requests, max, .. }
                if max == floor && requests > max
        ),
        "{err:?}"
    );
    assert!(Lake::gets(&lake.lake) > 0, "it got as far as reading data");

    // Control: the same resolve, planned with no budgets, runs to its end.
    let accounting = ravel_types::accounting::QueryAccounting::new();
    let resolved = lake
        .executor
        .resolve_pinned(acme, &req, &accounting)
        .await
        .expect("resolve");
    let batches = plan_and_drain(&lake, acme, &resolved, sql, &accounting, None)
        .await
        .expect("no budgets, no refusal");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

/// A ticket that carries a clamped `max_store_requests` has `DoGet` refuse
/// the scan by that figure: the ticket pins the budget along with the
/// manifests. The same ticket without it streams the rows.
///
/// FLIP: `DoGet` planning with `budgets: None` streams the statement under the
/// executor's own ceiling, and the `expect_err` below fails.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn do_get_applies_the_budgets_its_ticket_carries() {
    use arrow_flight::Ticket;
    use arrow_flight::sql::{ProstMessageExt, TicketStatementQuery};
    use prost::Message as _;
    use ravel_sql::TicketSurface;
    use util::flight_harness::{merged, statement_ticket};

    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    lake.hits_for(&acme.hash()).await;
    let harness = flight_harness(&lake, &acme);
    let ticket = harness
        .get_flight_info("acme", "SELECT sum(id) FROM hits")
        .await
        .expect("flight info");

    let keys = flight_ticket_keys();
    let handle = statement_ticket(&ticket).statement_handle;
    let mut decoded = keys
        .decode(&handle, TicketSurface::Client)
        .expect("the service's own ticket");
    assert_eq!(decoded.budgets, None);
    assert_eq!(decoded.parquet_tables.len(), 1);

    // DoGet's own reads are the pinned manifest, the grants record, three
    // footers and then data: a budget of five is passed on the way.
    const BUDGET: u64 = 5;
    decoded.budgets = Some(RequestBudgets {
        max_store_requests: Some(RequestLimit::Bounded(BUDGET)),
        ..RequestBudgets::default()
    });
    let lowered = Ticket::new(
        TicketStatementQuery {
            statement_handle: keys
                .encode(&decoded, TicketSurface::Client)
                .expect("encode")
                .into(),
        }
        .as_any()
        .encode_to_vec(),
    );
    let status = harness
        .do_get("acme", &lowered)
        .await
        .expect_err("refused by the ticket's budget");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status
            .message()
            .contains(&format!("exceeding the budget of {BUDGET}")),
        "{}",
        status.message()
    );

    // Control: the ticket as the service minted it streams the sum.
    let streamed = harness.do_get("acme", &ticket).await.expect("do get");
    assert_eq!(merged(&streamed).num_rows(), 1);
}

/// Flight SQL runs a Parquet table joined with a signal table and returns the
/// rows `execute` returns.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn flight_sql_joins_a_parquet_table_with_logs() {
    use util::flight_harness::merged;

    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    hits_and_logs(&lake).await;
    let harness = flight_harness(&lake, &acme);
    let ticket = harness
        .get_flight_info("acme", HITS_JOIN_LOGS)
        .await
        .expect("flight info");
    let flight = harness.do_get("acme", &ticket).await.expect("do get");
    let http = lake
        .execute(&acme.hash(), HITS_JOIN_LOGS)
        .await
        .expect("execute");
    assert_eq!(
        rows(&http),
        vec!["1|line 1", "2|line 2", "3|line 3", "4|line 4"]
    );
    assert_eq!(merged(&flight), merged(http.output.batches()));
}

/// A mixed statement's ticket pins both sides: a `hits` replacement and a new
/// logs commit between `GetFlightInfo` and `DoGet` are both invisible to
/// `DoGet`, and a statement planned after them sees both.
///
/// FLIP: a `DoGet` that resolves the Parquet table's newest manifest streams
/// only ids 2 and 6; one that resolves the logs catalog again joins `line 6`.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn a_flight_mixed_statement_pins_the_parquet_table_and_the_signal_lane() {
    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    hits_and_logs(&lake).await;
    let harness = flight_harness(&lake, &acme);
    let ticket = harness
        .get_flight_info("acme", HITS_JOIN_LOGS)
        .await
        .expect("flight info");

    let replacement = lake
        .put_file(
            "t/hits/replacement.parquet",
            parquet_bytes(&[2, 6], &["b", "c"], &[1.5, 5.5]),
        )
        .await;
    lake.replace(&acme.hash(), "hits", vec![replacement]).await;
    util::publish_logs(lake.ravel.as_ref(), &acme, 1, &[5, 6]).await;

    let streamed = harness.do_get("acme", &ticket).await.expect("do get");
    assert_eq!(
        batch_rows(&streamed),
        vec!["1|line 1", "2|line 2", "3|line 3", "4|line 4"],
        "the table and the logs the ticket pinned"
    );

    // Non-vacuity: both changes are live for a statement planned now.
    let later = harness
        .get_flight_info("acme", HITS_JOIN_LOGS)
        .await
        .expect("flight info after the changes");
    let streamed = harness.do_get("acme", &later).await.expect("do get");
    assert_eq!(batch_rows(&streamed), vec!["2|line 2", "6|line 6"]);
}

/// A tenant resolver handing every request one plain commit token, as a
/// deployment's Flight metadata does.
#[cfg(feature = "flight-sql")]
struct TokenAuth {
    tenant: TenantHash,
    token: ravel_types::CommitToken,
}

#[cfg(feature = "flight-sql")]
impl ravel_sql::FlightAuth for TokenAuth {
    fn tenant(&self, metadata: &tonic::metadata::MetadataMap) -> Result<TenantHash, tonic::Status> {
        match metadata
            .get(util::flight_harness::TOKEN_KEY)
            .and_then(|value| value.to_str().ok())
        {
            Some("acme") => Ok(self.tenant),
            _ => Err(tonic::Status::unauthenticated(
                "invalid or missing tenant credentials",
            )),
        }
    }

    fn min_commit_tokens(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Vec<ravel_types::CommitToken>, tonic::Status> {
        Ok(vec![self.token.clone()])
    }
}

/// A plain commit token on a Flight statement that reads only Parquet tables
/// is `CommitTokenOnParquetStatement`, INVALID_ARGUMENT at `GetFlightInfo`
/// before either store is read; the same token on a statement naming one
/// signal table beside the Parquet table bounds that table and runs.
///
/// FLIP: dropping the check in `resolve_pinned_lanes` lets the Parquet-only
/// statement mint a ticket.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn a_commit_token_on_a_parquet_only_flight_statement_is_refused() {
    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    let (token, _) = hits_and_logs(&lake).await;
    let harness = flight_harness_with(
        &lake,
        Arc::new(TokenAuth {
            tenant: acme.hash(),
            token,
        }),
    );

    let before = reads(&lake);
    let status = harness
        .get_flight_info("acme", "SELECT count(*) FROM hits")
        .await
        .expect_err("a token on a Parquet-only statement");
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status}");
    assert_eq!(
        status.message(),
        SqlError::CommitTokenOnParquetStatement.client_message(),
        "the text the HTTP path returns"
    );
    assert_eq!(reads(&lake), before, "nothing is read");

    let ticket = harness
        .get_flight_info("acme", HITS_JOIN_LOGS)
        .await
        .expect("the token bounds logs");
    let streamed = harness.do_get("acme", &ticket).await.expect("do get");
    assert_eq!(batch_rows(&streamed).len(), LOG_TS.len());
}
