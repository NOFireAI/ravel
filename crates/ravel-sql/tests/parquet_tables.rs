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
use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_pqtable::clock::FixedClock;
use ravel_pqtable::grants;
use ravel_pqtable::manifest::ParquetFile;
use ravel_pqtable::writer::{self, Intent};
use ravel_query::{GetLimiter, LogSegmentFetcher, QueryPhase, SegmentFetcher};
use ravel_sql::{
    DEFAULT_PARQUET_METADATA_CACHE_BYTES, ExternalStoreMap, MSG_PLAN, ParquetQueryError,
    ParquetSources, SpanSegmentFetcher, SqlConfig, SqlError, SqlExecutor, SqlOutcome, TargetSignal,
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
        let ravel = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let lake = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let store: Arc<dyn ObjectStoreBackend> = ravel.clone();
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let external = configured.then(|| {
            Arc::new(ExternalStoreMap::new(HashMap::from([(
                PROFILE.to_string(),
                Arc::clone(&lake) as Arc<dyn ObjectStoreBackend>,
            )]))) as Arc<dyn ravel_sql::ExternalStores>
        });
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
            .with_parquet_sources(sources),
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
/// the Resolve phase, footers and page indexes in Probe, and data in Scan.
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
        6,
        "a footer and a page index for each of three files"
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
        let err = lake.execute(&acme, &sql).await.expect_err(&sql);
        assert!(matches!(err, SqlError::Plan(_)), "{sql}: {err}");
        assert_eq!(err.client_message(), MSG_PLAN, "{sql}");
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
/// statement fails typed, after one LIST and no GET on either store.
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
    assert_eq!(lake.all_gets(), (ravel_before, lake_before));
    assert_eq!(Lake::lists(&lake.ravel) - lists_before, 1);

    unknown_table_error(&lake, &acme, "SELECT * FROM nosuch").await;
}

/// A dropped table is no table: the same planning failure as a name nobody
/// created.
#[tokio::test]
async fn a_dropped_table_is_an_unknown_table() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
    writer::apply(
        lake.ravel.inner(),
        &acme,
        "hits",
        Intent::Drop {
            if_exists: false,
            created_by: "test".to_string(),
            statement: "DROP TABLE hits".to_string(),
        },
        &FixedClock::new(NOW),
        MIN_GRACE_MS,
    )
    .await
    .expect("drop");
    unknown_table_error(&lake, &acme, "SELECT * FROM hits").await;
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

/// A row window names an event-time column, which a Parquet table has none
/// of: it is refused rather than silently ignored.
#[tokio::test]
async fn a_row_window_is_refused_on_a_parquet_table() {
    let lake = Lake::configured();
    let acme = tenant("acme");
    lake.hits_for(&acme).await;
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

/// Flight SQL reaches Parquet tables through the same executor funnel:
/// `GetFlightInfo` then `DoGet` return the rows `execute` returns.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn flight_sql_reads_a_parquet_table() {
    use std::time::Duration;

    use ravel_sql::{FlightClock, FlightSqlConfig, RavelFlightSqlService};
    use util::flight_harness::{Harness, TestAuth, TestClock, merged};

    let lake = Lake::configured();
    let acme = TenantId::new("acme");
    lake.hits_for(&acme.hash()).await;
    let clock = TestClock::at(util::NOW_NS);
    let service = RavelFlightSqlService::new(
        Arc::clone(&lake.executor),
        TestAuth::new(&[("acme", &acme)]),
        Arc::clone(&clock) as Arc<dyn FlightClock>,
        FlightSqlConfig {
            max_deadline: Duration::from_secs(30),
            ..FlightSqlConfig::default()
        },
        Arc::new(ravel_types::accounting::NoopQueryCostRecorder),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
    );
    let harness = Harness {
        service,
        executor: Arc::clone(&lake.executor),
        clock,
        store: Arc::clone(&lake.ravel) as Arc<dyn ObjectStoreBackend>,
    };
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
