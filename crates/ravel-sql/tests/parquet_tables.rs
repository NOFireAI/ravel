//! Parquet tables through the SQL executor (ADR-2040 D3, D4, D6).
//!
//! Every test drives a real `SqlExecutor` with [`ParquetSources`] over two
//! `MemoryStore`s: Ravel's own store, holding the tenant's grants and table
//! manifests as `ravel-pqtable` writes them, and an external store behind one
//! credential profile, holding the Parquet files. Both are instrumented so a
//! test can say how many reads reached each.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_memory::MemoryBudget;
use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_parquet::ParquetReadError;
use ravel_pqtable::clock::FixedClock;
use ravel_pqtable::grants;
use ravel_pqtable::manifest::ParquetFile;
use ravel_pqtable::writer::{self, Intent};
use ravel_query::{
    ByteLimit, EngineConfig, GetLimiter, LogSegmentFetcher, QueryPhase, RequestBudgets,
    RequestLimit, SegmentFetcher,
};
use ravel_sql::{
    DEFAULT_PARQUET_METADATA_CACHE_BYTES, ErrorClass, ExternalStoreMap, MAX_STATEMENT_TABLE_NAMES,
    MSG_PLAN, ParquetQueryError, ParquetSources, SpanSegmentFetcher, SqlConfig, SqlError,
    SqlExecutor, SqlOutcome, TargetSignal,
};
use ravel_types::accounting::AccountedOp;
use ravel_types::{TenantHash, TenantId};

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
                LogSegmentFetcher::new(Arc::clone(&store)),
                SpanSegmentFetcher::new(Arc::clone(&store)),
                config,
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

/// The Parquet refusal `err` carries, if it is one.
fn parquet_error(err: &SqlError) -> Option<&ParquetQueryError> {
    match err {
        SqlError::Parquet(parquet) => Some(parquet.as_ref()),
        _ => None,
    }
}

/// Every row of `outcome`, each rendered as `v|v|...`, in result order.
fn rows(outcome: &SqlOutcome) -> Vec<String> {
    let mut out = Vec::new();
    for batch in outcome.output.batches() {
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

/// A Parquet table beside a signal table is a cross-signal statement, in
/// either order, and it is refused before any file is read.
#[tokio::test]
async fn a_parquet_table_joined_with_logs_is_cross_signal() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    for sql in [
        "SELECT count(*) FROM hits JOIN logs ON true",
        "SELECT count(*) FROM logs WHERE body IN (SELECT name FROM hits)",
    ] {
        let err = lake.execute(&acme, sql).await.expect_err("cross signal");
        assert!(matches!(err, SqlError::CrossSignalQuery), "{sql}: {err}");
    }
    assert_eq!(Lake::gets(&lake.lake), 0);
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

/// A dropped table beside a signal table is an unknown table, not a
/// cross-signal statement: the early-exit resolve reads the name's newest
/// manifest, finds a drop, and goes on.
///
/// FLIP: deciding from the LIST alone (a name with versions is found) makes
/// this `CrossSignalQuery`.
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

/// With a signal table present, the first name that is a live table decides
/// `CrossSignalQuery`: the unknown names after it are never listed.
#[tokio::test]
async fn samples_beside_a_parquet_table_stops_at_the_first_found_name() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    let before = reads(&lake);

    let names = unknown_names(MAX_STATEMENT_TABLE_NAMES - 1);
    let sql = format!("SELECT 1 FROM samples, hits, {}", names.join(", "));
    let err = lake.execute(&acme, &sql).await.expect_err("cross signal");
    assert!(matches!(err, SqlError::CrossSignalQuery), "{err}");
    let (ravel_gets, ravel_lists, lake_gets) = reads(&lake);
    assert_eq!(ravel_lists - before.1, 1, "one LIST, of hits's versions");
    assert_eq!(
        (ravel_gets - before.0, lake_gets - before.2),
        (1, 0),
        "hits's newest manifest, and no file"
    );
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
    use std::time::Duration;

    use ravel_sql::{FlightClock, FlightSqlConfig, RavelFlightSqlService};
    use util::flight_harness::{Harness, TestAuth, TestClock};

    let clock = TestClock::at(util::NOW_NS);
    let service = RavelFlightSqlService::new(
        Arc::clone(&lake.executor),
        TestAuth::new(&[("acme", tenant)]),
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

/// The memory error a signal fetcher's refused reservation is, built for the
/// comparison of class, status and client message.
fn signal_memory_error() -> SqlError {
    SqlError::Fetch(ravel_query::FetchError::FetchMemoryExhausted {
        requested: 1,
        reserved: 0,
        limit: 0,
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
    match &err {
        SqlError::Fetch(ravel_query::FetchError::FetchMemoryExhausted {
            requested, limit, ..
        }) => {
            assert_eq!(*requested, chunk);
            assert_eq!(*limit, chunk - 1);
        }
        other => panic!("expected FetchMemoryExhausted, got {other:?}"),
    }
    assert_eq!(err.class(), signal_memory_error().class());
    assert_eq!(err.client_message(), signal_memory_error().client_message());
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
