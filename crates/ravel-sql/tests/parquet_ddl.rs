//! Acceptance tests for `CREATE [OR REPLACE] EXTERNAL TABLE` and `DROP TABLE`
//! (ADR-2040 D2, D4): `SqlExecutor::execute_ddl` driven end to end over two
//! `MemoryStore`s, the same fixture shape `parquet_tables.rs` uses for reads.
//! Ravel's own store holds grants and manifests; the external "lake" store
//! holds the Parquet files a `CREATE` statement points at.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_memory::MemoryBudget;
use ravel_object_store::external::probe::{
    PROBE_PREFIX, PreconditionProbeFailure, RavelBucketProbeFailure,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::instrument::{InstrumentedStore, STORE_OP_COUNT, StoreOp};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_parquet::snapshot::SnapshotError;
use ravel_pqtable::clock::{Clock, FixedClock};
use ravel_pqtable::grants::{self, GrantsError};
use ravel_pqtable::writer::WriteError;
use ravel_query::{GetLimiter, LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DEFAULT_PARQUET_METADATA_CACHE_BYTES, DdlCost, DdlExecuteError, DdlOutcome, DdlPhase,
    ExternalStoreMap, ParquetSources, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlOutcome,
};
use ravel_types::{TenantHash, TenantId};
use util::request;

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

/// One row group of `id: Int64`, `EventDate: Int64`, a real Parquet file
/// whose `EventDate` column holds days-since-epoch values for a
/// `ravel.cast.EventDate` `date-from-days` cast to exercise.
fn event_date_parquet_bytes(ids: &[i64], days: &[i64]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("EventDate", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(days.to_vec())) as ArrayRef,
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

    async fn query(&self, tenant: TenantHash, sql: &str) -> SqlOutcome {
        self.executor
            .execute(tenant, &request(sql))
            .await
            .expect("select")
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
        .result
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

    let select = lake
        .query(t, "SELECT id, name, score FROM hits ORDER BY id")
        .await;
    assert_eq!(rows(&select), vec!["1|a|0.5", "2|b|1.5"]);
}

#[tokio::test]
async fn create_external_table_over_a_single_object_location_then_read_back() {
    // A LOCATION naming one object directly (no trailing slash) takes
    // `one_object_under`'s HEAD path, not the directory-listing path: it
    // must still succeed and the table must still read back.
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
            &format!(
                "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/0.parquet'"
            ),
            CREATED_BY,
            deadline(),
        )
        .await
        .result
        .expect("create over a single-object location");

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

    let select = lake
        .query(t, "SELECT id, name, score FROM hits ORDER BY id")
        .await;
    assert_eq!(rows(&select), vec!["1|a|0.5", "2|b|1.5"]);
}

#[tokio::test]
async fn create_external_table_over_a_single_object_without_the_parquet_suffix() {
    // ADR-2040 reads a single-object LOCATION whatever its suffix; the DDL
    // probe must not refuse one the snapshot and the query path accept.
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file(
        "t/hits/export-2026-09",
        parquet_bytes(&[1, 2], &["a", "b"], &[0.5, 1.5]),
    )
    .await;

    let outcome = lake
        .executor
        .execute_ddl(
            t,
            &format!(
                "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/export-2026-09'"
            ),
            CREATED_BY,
            deadline(),
        )
        .await
        .result
        .expect("create over a single object with no .parquet suffix");

    match outcome {
        DdlOutcome::Created { files, .. } => assert_eq!(files, 1),
        other => panic!("expected Created, got {other:?}"),
    }

    let select = lake
        .query(t, "SELECT id, name, score FROM hits ORDER BY id")
        .await;
    assert_eq!(rows(&select), vec!["1|a|0.5", "2|b|1.5"]);
}

#[tokio::test]
async fn create_external_table_over_a_zero_byte_single_object_is_refused() {
    // The HEAD path's own emptiness check (`one_object_under`, ddl.rs): a
    // zero-byte object at the named key is not a snapshot-able Parquet file.
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file("t/hits/0.parquet", Bytes::new()).await;

    let err = lake
        .executor
        .execute_ddl(
            t,
            &format!(
                "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/0.parquet'"
            ),
            CREATED_BY,
            deadline(),
        )
        .await
        .result
        .expect_err("a zero-byte single object must be refused");

    assert!(
        matches!(err, DdlExecuteError::ProbeObjectEmpty { ref location } if location == &format!("{GRANT}/hits/0.parquet")),
        "{err:?}"
    );
}

#[tokio::test]
async fn ravel_cast_naming_a_column_absent_from_the_snapshot_schema_is_refused() {
    // `validate_ddl` admits `ravel.cast.<column>` for any column name that
    // passes the charset rule; it has no schema to check the column against.
    // The Parquet file under LOCATION has `id`, `name`, `score`, not
    // `missing`, and that can only be known once the snapshot above has run.
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;

    let sql = format!(
        "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/' \
         OPTIONS ('ravel.cast.missing' 'date-from-days')"
    );
    let err = lake
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .result
        .expect_err("a cast naming an absent column must be refused");

    assert!(
        matches!(err, DdlExecuteError::UnknownCastColumn { ref column } if column == "missing"),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(lake.ravel.inner(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "a refused cast column must not write a manifest: {tables:?}"
    );
}

#[tokio::test]
async fn ravel_cast_option_naming_a_mixed_case_column_casts_and_reads_back() {
    // Fix for issue #2054: `ravel.cast.EventDate` (ADR-2040 D5's own
    // example) must be admitted end to end, not merely by `validate_ddl`:
    // `execute_ddl` must accept it over a file carrying an actual
    // `EventDate` column, and the cast must take effect on SELECT.
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file(
        "t/hits/0.parquet",
        event_date_parquet_bytes(&[1, 2], &[0, 1]),
    )
    .await;

    let sql = format!(
        "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/' \
         OPTIONS ('ravel.cast.EventDate' 'date-from-days')"
    );
    let outcome = lake
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .result
        .expect("create with a mixed-case ravel.cast column");
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

    let select = lake
        .query(t, "SELECT id, \"EventDate\" FROM hits ORDER BY id")
        .await;
    assert_eq!(
        rows(&select),
        vec!["1|1970-01-01", "2|1970-01-02"],
        "EventDate must read back as a DATE, not the raw day count"
    );
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
        .result
        .expect("first create");

    let sql_if_not_exists = format!(
        "CREATE EXTERNAL TABLE IF NOT EXISTS hits STORED AS PARQUET LOCATION '{GRANT}/hits/'"
    );
    let outcome = lake
        .executor
        .execute_ddl(t, &sql_if_not_exists, CREATED_BY, deadline())
        .await
        .result
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
        .result
        .expect("first create");

    let err = lake
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .result
        .expect_err("plain CREATE over an existing table must fail");

    assert!(
        matches!(err, DdlExecuteError::Write(WriteError::TableExists { ref table }) if table == "hits"),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(lake.ravel.inner(), &t)
        .await
        .expect("tables");
    assert_eq!(
        tables
            .get("hits")
            .and_then(|versions| versions.last().copied()),
        Some(1),
        "a refused plain CREATE must not advance the manifest version"
    );
}

#[tokio::test]
async fn create_if_not_exists_on_existing_table_issues_no_lake_store_calls() {
    // The existence check runs against ravel's own manifest store, before the
    // grant is resolved or the lake store is ever touched: a plain `CREATE [IF
    // NOT EXISTS]` on a table that already exists must cost zero GET, HEAD,
    // or LIST calls against the lake, not merely return the right outcome.
    let lake_store = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let lake = Lake::unlimited(Arc::clone(&lake_store) as Arc<dyn ObjectStoreBackend>);
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;
    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    lake.executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .result
        .expect("first create");

    let before = lake_store.metrics().snapshot();

    let sql_if_not_exists = format!(
        "CREATE EXTERNAL TABLE IF NOT EXISTS hits STORED AS PARQUET LOCATION '{GRANT}/hits/'"
    );
    let outcome = lake
        .executor
        .execute_ddl(t, &sql_if_not_exists, CREATED_BY, deadline())
        .await
        .result
        .expect("second create is a no-op, not an error");
    assert_eq!(
        outcome,
        DdlOutcome::NoOp {
            table: "hits".to_string()
        }
    );

    let after = lake_store.metrics().snapshot();
    assert_eq!(
        after.op(StoreOp::Get).calls,
        before.op(StoreOp::Get).calls,
        "existing-table short-circuit must not read a footer"
    );
    assert_eq!(
        after.op(StoreOp::Head).calls,
        before.op(StoreOp::Head).calls
    );
    assert_eq!(
        after.op(StoreOp::List).calls,
        before.op(StoreOp::List).calls
    );

    // The plain-CREATE case (no IF NOT EXISTS) hits the identical
    // before-any-grant-or-store-call existence check, on its way to a
    // `TableExists` error instead of a `NoOp`: that outcome must cost
    // exactly as little.
    let before_plain = lake_store.metrics().snapshot();
    let err = lake
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .result
        .expect_err("plain CREATE over an existing table must fail");
    assert!(
        matches!(err, DdlExecuteError::Write(WriteError::TableExists { ref table }) if table == "hits"),
        "{err:?}"
    );
    let after_plain = lake_store.metrics().snapshot();
    assert_eq!(
        after_plain.op(StoreOp::Get).calls,
        before_plain.op(StoreOp::Get).calls,
        "TableExists short-circuit must not read a footer"
    );
    assert_eq!(
        after_plain.op(StoreOp::Head).calls,
        before_plain.op(StoreOp::Head).calls
    );
    assert_eq!(
        after_plain.op(StoreOp::List).calls,
        before_plain.op(StoreOp::List).calls
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
        .result
        .expect("first create");

    lake.put_file("t/hits/1.parquet", parquet_bytes(&[2], &["b"], &[1.5]))
        .await;
    let replace =
        format!("CREATE OR REPLACE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let outcome = lake
        .executor
        .execute_ddl(t, &replace, CREATED_BY, deadline())
        .await
        .result
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
        .result
        .expect("create");

    let outcome = lake
        .executor
        .execute_ddl(t, "DROP TABLE hits", CREATED_BY, deadline())
        .await
        .result
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
async fn create_after_drop_on_the_same_name_proceeds() {
    // The existence check at ddl.rs (`resolve::newest` plus `is_live()`)
    // must treat a dropped manifest as not existing: a tombstone version is
    // still the newest manifest for the name, so a check that tested
    // `is_some()` instead of liveness would wrongly refuse this CREATE with
    // `TableExists`.
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;
    let create = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    lake.executor
        .execute_ddl(t, &create, CREATED_BY, deadline())
        .await
        .result
        .expect("first create");

    lake.executor
        .execute_ddl(t, "DROP TABLE hits", CREATED_BY, deadline())
        .await
        .result
        .expect("drop");

    lake.put_file("t/hits/1.parquet", parquet_bytes(&[2], &["b"], &[1.5]))
        .await;
    let outcome = lake
        .executor
        .execute_ddl(t, &create, CREATED_BY, deadline())
        .await
        .result
        .expect("create after drop must proceed, not report TableExists");

    match outcome {
        DdlOutcome::Created { table, version, .. } => {
            assert_eq!(table, "hits");
            assert_eq!(version, 3, "tombstone (2) then this create (3)");
        }
        other => panic!("expected Created, got {other:?}"),
    }

    let select = lake
        .query(t, "SELECT id, name, score FROM hits ORDER BY id")
        .await;
    assert_eq!(rows(&select), vec!["1|a|0.5", "2|b|1.5"]);
}

#[tokio::test]
async fn drop_if_exists_on_a_missing_table_is_a_no_op() {
    let lake = Lake::memory_store();
    let t = tenant("acme");

    let outcome = lake
        .executor
        .execute_ddl(t, "DROP TABLE IF EXISTS ghost", CREATED_BY, deadline())
        .await
        .result
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
        .result
        .expect_err("plain DROP on a missing table must fail");

    assert!(
        matches!(err, DdlExecuteError::Write(WriteError::TableNotFound { ref table }) if table == "ghost"),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(lake.ravel.inner(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may exist for a missing table"
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
        .result
        .expect_err("a tenant with no grant on this location must be refused");

    assert!(
        matches!(
            err,
            DdlExecuteError::Location(GrantsError::LocationNotGranted { .. })
        ),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(lake.ravel.inner(), &stranger)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may be written for the refused tenant"
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
        .result
        .expect_err("a location inside Ravel's own bucket must be refused");

    assert!(
        matches!(
            err,
            DdlExecuteError::RavelBucketProbe {
                source: RavelBucketProbeFailure::SameBucket { .. },
                ..
            }
        ),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(ravel.inner(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may be written for a location inside Ravel's own bucket"
    );
}

#[tokio::test]
async fn folder_marker_object_is_not_treated_as_the_probe_object() {
    // Some writers leave a zero-byte object named exactly like the directory
    // (`t/hits/`) as a folder marker. No store operation can write it
    // (`Path::from` drops the trailing `/`), and listing `t/hits/` reports it
    // in `unaddressable`, so it must not satisfy the probe.
    let store = MemoryStore::new();
    store.insert_foreign("t/hits/", Bytes::new());
    let lake = Lake::unlimited(Arc::new(store));
    let t = tenant("acme");
    lake.grant(&t).await;
    let listed = lake.lake.list("t/hits/", None).await.expect("list");
    assert!(listed.objects.is_empty(), "{:?}", listed.objects);
    let markers: Vec<&str> = listed.unaddressable.iter().map(|u| u.key.as_str()).collect();
    assert_eq!(markers, ["t/hits/"]);

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let err = lake
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .result
        .expect_err("a bare folder marker must not qualify as the probe object");

    assert!(
        matches!(err, DdlExecuteError::ProbeObjectEmpty { .. }),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(lake.ravel.inner(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may be written when the probe object is a folder marker"
    );
}

#[tokio::test]
async fn sibling_directory_sharing_the_same_string_prefix_is_not_a_match() {
    // `t/hits-archive/0.parquet` shares the string prefix "t/hits" with the
    // LOCATION "t/hits/" but is not inside it. A listing scoped to the bare
    // key "t/hits" (no trailing slash) would match it anyway; one scoped to
    // "t/hits/" must not.
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file(
        "t/hits-archive/0.parquet",
        parquet_bytes(&[1], &["a"], &[0.5]),
    )
    .await;

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let err = lake
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .result
        .expect_err("a sibling directory's file must not qualify as the probe object");

    assert!(
        matches!(err, DdlExecuteError::ProbeObjectEmpty { .. }),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(lake.ravel.inner(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may be written when the probe object is a sibling directory's file"
    );
}

#[tokio::test]
async fn zero_byte_object_under_the_location_is_not_treated_as_a_match() {
    // A zero-byte object named like a real data file (e.g. an interrupted or
    // placeholder upload) must not satisfy the probe: `snapshot_location`
    // could never derive a schema from it.
    let lake = Lake::memory_store();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file("t/hits/0.parquet", Bytes::new()).await;

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let err = lake
        .executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .result
        .expect_err("a zero-byte object must not qualify as the probe object");

    assert!(
        matches!(err, DdlExecuteError::ProbeObjectEmpty { .. }),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(lake.ravel.inner(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may be written when the probe object is zero bytes"
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
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let lake = Arc::clone(&fault_store) as Arc<dyn ObjectStoreBackend>;
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
        .result
        .expect_err("a store that refuses a matching pin must be refused");

    assert!(
        matches!(
            err,
            DdlExecuteError::PreconditionProbe {
                source: PreconditionProbeFailure::MatchingPinRefused { .. },
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(
        fault_store.fault_count(Op::Get, FaultKind::FailedPrecondition),
        1,
        "the scripted precondition fault must have fired exactly once"
    );

    let grants = grants::list(fixture.ravel.inner(), &t).await.expect("list");
    assert_eq!(
        grants.len(),
        1,
        "the grant must still be the only record written"
    );

    let tables = ravel_pqtable::resolve::tables(fixture.ravel.inner(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may be written when the precondition probe is refused"
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
        .result
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
        .result
        .expect_err("a 1-byte process memory budget cannot decode this file's footer");

    assert!(
        matches!(
            err,
            DdlExecuteError::Snapshot {
                source: SnapshotError::MemoryExhausted { .. },
                ..
            }
        ),
        "{err:?}"
    );

    let tables = ravel_pqtable::resolve::tables(fixture.ravel.inner(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may be written when the memory budget refuses the footer read"
    );
}

/// A store double that advances a shared [`FixedClock`] the second time its
/// `list` is called, then leaves the clock alone on every other call.
/// Standing in for a manifest-prefix LIST slow enough to burn
/// `writer::apply`'s resolve-to-put budget, without an actual wall-clock
/// sleep. The first `list` call is `execute_ddl`'s own existence check
/// (`resolve::newest`, before `writer::apply` is ever entered); the second is
/// `apply`'s own initial resolve, which is the LIST whose slowness this double
/// models. Every other method delegates to `inner` unchanged.
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
        let call = self.list_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == 2 && !self.advanced.swap(true, Ordering::SeqCst) {
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
/// immediately after `apply`'s own first LIST returns (the second LIST
/// against Ravel's store in the whole statement -- the first is
/// `execute_ddl`'s existence check, which runs before `apply` is ever
/// entered and must not be mistaken for the slow resolve).
///
/// Before this fix, `execute_ddl` built its own `FixedClock::new(now_ns)`
/// internally on every call and never read the clock installed through
/// `SqlExecutor::with_clock`. Against that code, `clock.now_ns()` inside
/// `writer::apply` never advances between the resolve and the
/// remaining-budget check, so `remaining_ns` always computes to the full
/// budget and `apply` never re-resolves: this store double would see exactly
/// one `list` call from `apply` before the put committed (two overall,
/// counting the existence check). `with_clock` existing and being threaded
/// into `writer::apply` is what makes `list_calls() == 3` below observable
/// at all: the existence check's LIST, `apply`'s first resolve (the one the
/// advance lands inside), and the forced re-resolve the exhausted budget
/// triggers.
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
        .result
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
        3,
        "expected the existence check's LIST, apply's first resolve, and the \
         forced re-resolve a budget exhausted between resolve and put must \
         trigger"
    );
}

/// How many LISTs a `CREATE` makes against Ravel's store when the resolve
/// inside `writer::apply` is made to take `advance_ns` of the executor's clock,
/// with `grace_ms` installed through `SqlExecutor::with_ddl_min_grace_ms` or
/// left at its default when `None`.
async fn creates_lists_with_slow_resolve(grace_ms: Option<u64>, advance_ns: i64) -> usize {
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

    let apply_clock = FixedClock::new(NOW);
    let listing = Arc::new(ListAdvancingStore::new(
        ravel.clone() as Arc<dyn ObjectStoreBackend>,
        apply_clock.clone(),
        advance_ns,
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
    let executor = match grace_ms {
        Some(grace_ms) => executor.with_ddl_min_grace_ms(grace_ms),
        None => executor,
    };

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    executor
        .execute_ddl(t, &sql, CREATED_BY, deadline())
        .await
        .result
        .expect("create succeeds");
    listing.list_calls()
}

/// `SqlExecutor::with_ddl_min_grace_ms` reaches `writer::apply`: a resolve
/// that takes 1 s of clock time is inside the budget under the default grace
/// (half of `DEFAULT_MIN_GRACE_MS`, 330 s) and costs no extra LIST, but
/// exhausts the budget of a 1 s grace (half of it, 500 ms) and forces a second
/// resolve. Without the setter reaching `apply`, both runs would list twice.
#[tokio::test]
async fn with_ddl_min_grace_ms_sets_the_grace_writer_apply_budgets_against() {
    let one_second_ns = 1_000_000_000;
    assert_eq!(
        creates_lists_with_slow_resolve(None, one_second_ns).await,
        2,
        "under the default grace a 1 s resolve is inside the budget: the \
         existence check's LIST and apply's one resolve"
    );
    assert_eq!(
        creates_lists_with_slow_resolve(Some(1_000), one_second_ns).await,
        3,
        "a 1 s grace budgets 500 ms, so the same 1 s resolve forces a second \
         resolve"
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
    let execution = executor
        .execute_ddl(t, &sql, CREATED_BY, short_deadline)
        .await;
    let err = execution
        .result
        .expect_err("a grants read that never returns must fail with the statement deadline");

    assert!(
        matches!(err, DdlExecuteError::Deadline { deadline } if deadline == short_deadline),
        "{err:?}"
    );
    // The cost survives the deadline: the existence check's LIST completed,
    // and the stalled grants GET was issued, so both are counted.
    let cost = execution.cost;
    assert_eq!(
        op_counts(&cost, DdlPhase::Write),
        vec![("list", 1)],
        "{cost:?}"
    );
    assert_eq!(
        op_counts(&cost, DdlPhase::Grant),
        vec![("get", 1)],
        "{cost:?}"
    );
    assert_eq!(cost.total_requests(), 2, "{cost:?}");
    assert_eq!(cost.total_bytes(), 0, "{cost:?}");

    let tables = ravel_pqtable::resolve::tables(ravel_inner.as_ref(), &t)
        .await
        .expect("tables");
    assert!(
        tables.is_empty(),
        "no manifest may be written when the grants read never completed"
    );
}

/// The probe objects under [`PROBE_PREFIX`] in Ravel's own store.
async fn probe_objects(ravel: &InstrumentedStore<MemoryStore>) -> Vec<String> {
    ravel
        .inner()
        .list(PROBE_PREFIX, None)
        .await
        .expect("list probe prefix")
        .objects
        .into_iter()
        .map(|meta| meta.key)
        .collect()
}

/// A statement whose deadline expires inside `probe_not_ravel_bucket`, after
/// the probe object was written to Ravel's bucket and before the probe's own
/// delete, still deletes that object. The lake store holds the identity read
/// of the probe key open forever, so the statement can only end through its
/// deadline, which drops the probe mid-flight.
#[tokio::test(start_paused = true)]
async fn deadline_inside_the_ravel_bucket_probe_deletes_the_probe_object() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let gate = fault_store.hold(Op::Get, Some(PROBE_PREFIX.to_string()), Occurrence::Always);
    let lake = Arc::clone(&fault_store) as Arc<dyn ObjectStoreBackend>;
    let fixture = Lake::unlimited(lake);
    fixture
        .put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;
    let t = tenant("acme");
    fixture.grant(&t).await;

    let sql = format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'");
    let executor = Arc::clone(&fixture.executor);
    let statement = tokio::spawn(async move {
        executor
            .execute_ddl(t, &sql, CREATED_BY, deadline())
            .await
            .result
    });

    gate.wait_until_held(1).await;
    let held = gate.held_details();
    assert_eq!(held.len(), 1, "{held:?}");
    let (_, held_op, held_key) = &held[0];
    assert_eq!(*held_op, Op::Get);
    assert_eq!(
        probe_objects(&fixture.ravel).await,
        vec![held_key.clone()],
        "the probe object must be in Ravel's bucket while its identity read is held"
    );

    let err = statement
        .await
        .expect("join")
        .expect_err("a held identity read must end the statement at its deadline");
    assert!(
        matches!(err, DdlExecuteError::Deadline { deadline: d } if d == deadline()),
        "{err:?}"
    );
    // The held read was never released: the probe was dropped while parked
    // on it, not completed past it.
    assert_eq!(gate.held_count(), 1);

    let mut leaked = probe_objects(&fixture.ravel).await;
    for _ in 0..100 {
        if leaked.is_empty() {
            break;
        }
        tokio::task::yield_now().await;
        leaked = probe_objects(&fixture.ravel).await;
    }
    assert!(
        leaked.is_empty(),
        "a statement cancelled inside the probe must delete its probe object: {leaked:?}"
    );
}

/// The nonzero request counts of one phase, as `(op, count)` in
/// [`StoreOp::ALL`] order, so a test can pin a phase's whole request mix in
/// one assertion.
fn op_counts(cost: &DdlCost, phase: DdlPhase) -> Vec<(&'static str, u64)> {
    StoreOp::ALL
        .into_iter()
        .map(|op| (op.name(), cost.phase(phase).requests(op)))
        .filter(|(_, count)| *count > 0)
        .collect()
}

/// Requests per [`StoreOp`] and GET bytes the counting stores have completed
/// so far, summed over all of them.
fn counted(stores: &[&InstrumentedStore<MemoryStore>]) -> ([u64; STORE_OP_COUNT], u64) {
    let mut requests = [0; STORE_OP_COUNT];
    let mut get_bytes = 0;
    for store in stores {
        let snapshot = store.metrics().snapshot();
        for op in StoreOp::ALL {
            requests[op.index()] += snapshot.op(op).calls;
        }
        get_bytes += snapshot.op(StoreOp::Get).bytes;
    }
    (requests, get_bytes)
}

/// Asserts the sum of `cost`'s requests across every phase equals, op by op,
/// what the counting stores completed between `before` and `after`, and that
/// its bytes equal their GET bytes over the same span: every request the
/// statement made is in exactly one phase, and none is missing.
fn assert_cost_matches_stores(
    cost: &DdlCost,
    before: ([u64; STORE_OP_COUNT], u64),
    after: ([u64; STORE_OP_COUNT], u64),
) {
    for op in StoreOp::ALL {
        let costed: u64 = DdlPhase::ALL
            .into_iter()
            .map(|phase| cost.phase(phase).requests(op))
            .sum();
        assert_eq!(
            costed,
            after.0[op.index()] - before.0[op.index()],
            "{op:?} requests: the per-phase cost must sum to what the stores saw; {cost:?}"
        );
    }
    assert_eq!(
        cost.total_bytes(),
        after.1 - before.1,
        "GET bytes: the per-phase cost must sum to what the stores saw; {cost:?}"
    );
}

/// A [`Lake`] whose lake store is a counting store too, so the requests of
/// both stores a `CREATE` touches can be compared with its [`DdlCost`].
fn counted_lake() -> (Lake, Arc<InstrumentedStore<MemoryStore>>) {
    let lake_store = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let lake = Lake::unlimited(Arc::clone(&lake_store) as Arc<dyn ObjectStoreBackend>);
    (lake, lake_store)
}

#[tokio::test]
async fn ddl_cost_sums_to_the_store_requests_and_pins_each_phase_for_create_and_drop() {
    const FILES: u64 = 3;
    let (lake, lake_store) = counted_lake();
    let t = tenant("acme");
    lake.grant(&t).await;
    for i in 0..FILES {
        lake.put_file(
            &format!("t/hits/{i}.parquet"),
            parquet_bytes(&[i as i64], &["a"], &[0.5]),
        )
        .await;
    }

    let before = counted(&[&lake.ravel, &lake_store]);
    let execution = lake
        .executor
        .execute_ddl(
            t,
            &format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'"),
            CREATED_BY,
            deadline(),
        )
        .await;
    let after = counted(&[&lake.ravel, &lake_store]);
    assert!(
        matches!(execution.result, Ok(DdlOutcome::Created { files, .. }) if files == FILES as usize),
        "{:?}",
        execution.result
    );
    let cost = execution.cost;
    assert_cost_matches_stores(&cost, before, after);

    // The grants record, one GET.
    assert_eq!(op_counts(&cost, DdlPhase::Grant), vec![("get", 1)]);
    // One listing page to find the probe object; the precondition probe's
    // HEAD (`pin_of`) and two pinned GETs; the Ravel-bucket probe's PUT and
    // DELETE on Ravel's own store and its two GETs on the lake store.
    assert_eq!(
        op_counts(&cost, DdlPhase::Probe),
        vec![
            ("put", 1),
            ("get", 4),
            ("head", 1),
            ("list", 1),
            ("delete", 1)
        ]
    );
    // Only the matching-pin read returns bytes, and it reads one byte.
    assert_eq!(cost.phase(DdlPhase::Probe).bytes(), 1);
    // The snapshot lists once and reads one footer per file.
    assert_eq!(
        op_counts(&cost, DdlPhase::Snapshot),
        vec![("get", FILES), ("list", 1)]
    );
    assert!(cost.phase(DdlPhase::Snapshot).bytes() > 0, "{cost:?}");
    // The existence check's LIST, then `writer::apply`'s own resolve LIST
    // and its conditional PUT.
    assert_eq!(
        op_counts(&cost, DdlPhase::Write),
        vec![("put", 1), ("list", 2)]
    );

    let before = counted(&[&lake.ravel, &lake_store]);
    let execution = lake
        .executor
        .execute_ddl(t, "DROP TABLE hits", CREATED_BY, deadline())
        .await;
    let after = counted(&[&lake.ravel, &lake_store]);
    assert!(
        matches!(execution.result, Ok(DdlOutcome::Dropped { version: 2, .. })),
        "{:?}",
        execution.result
    );
    let cost = execution.cost;
    assert_cost_matches_stores(&cost, before, after);
    // A DROP is all write: resolve the newest version (LIST, then GET of
    // version 1) and put the dropped version.
    for phase in [DdlPhase::Grant, DdlPhase::Probe, DdlPhase::Snapshot] {
        assert_eq!(op_counts(&cost, phase), vec![], "{phase:?}");
    }
    assert_eq!(
        op_counts(&cost, DdlPhase::Write),
        vec![("put", 1), ("get", 1), ("list", 1)]
    );
    assert_eq!(after.1 - before.1, cost.phase(DdlPhase::Write).bytes());
    assert!(cost.phase(DdlPhase::Write).bytes() > 0, "{cost:?}");
}

#[tokio::test]
async fn a_create_refused_at_the_grant_still_reports_its_cost() {
    let (lake, lake_store) = counted_lake();
    let owner = tenant("acme");
    let stranger = tenant("other");
    lake.grant(&owner).await;
    lake.put_file("t/hits/0.parquet", parquet_bytes(&[1], &["a"], &[0.5]))
        .await;

    let before = counted(&[&lake.ravel, &lake_store]);
    let execution = lake
        .executor
        .execute_ddl(
            stranger,
            &format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'"),
            CREATED_BY,
            deadline(),
        )
        .await;
    let after = counted(&[&lake.ravel, &lake_store]);
    assert!(
        matches!(execution.result, Err(DdlExecuteError::Location(_))),
        "{:?}",
        execution.result
    );
    let cost = execution.cost;
    assert_cost_matches_stores(&cost, before, after);
    assert_eq!(op_counts(&cost, DdlPhase::Write), vec![("list", 1)]);
    assert_eq!(op_counts(&cost, DdlPhase::Grant), vec![("get", 1)]);
    assert_eq!(op_counts(&cost, DdlPhase::Probe), vec![]);
    assert_eq!(op_counts(&cost, DdlPhase::Snapshot), vec![]);
    assert_eq!(cost.total_bytes(), 0, "the stranger has no grants record");
}

#[tokio::test]
async fn a_create_failing_in_the_snapshot_still_reports_its_cost() {
    // Not a Parquet file: the probes qualify it (a nonzero object the grant
    // admits), and the snapshot's footer read then refuses it.
    let (lake, lake_store) = counted_lake();
    let t = tenant("acme");
    lake.grant(&t).await;
    lake.put_file(
        "t/hits/0.parquet",
        Bytes::from_static(b"definitely not parquet"),
    )
    .await;

    let before = counted(&[&lake.ravel, &lake_store]);
    let execution = lake
        .executor
        .execute_ddl(
            t,
            &format!("CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{GRANT}/hits/'"),
            CREATED_BY,
            deadline(),
        )
        .await;
    let after = counted(&[&lake.ravel, &lake_store]);
    assert!(
        matches!(execution.result, Err(DdlExecuteError::Snapshot { .. })),
        "{:?}",
        execution.result
    );
    let cost = execution.cost;
    assert_cost_matches_stores(&cost, before, after);
    assert_eq!(op_counts(&cost, DdlPhase::Grant), vec![("get", 1)]);
    assert_eq!(
        op_counts(&cost, DdlPhase::Probe),
        vec![
            ("put", 1),
            ("get", 4),
            ("head", 1),
            ("list", 1),
            ("delete", 1)
        ]
    );
    assert_eq!(
        op_counts(&cost, DdlPhase::Snapshot),
        vec![("get", 1), ("list", 1)]
    );
    // Only the existence check ran; the refused snapshot left nothing to write.
    assert_eq!(op_counts(&cost, DdlPhase::Write), vec![("list", 1)]);
}

#[tokio::test]
async fn a_statement_refused_by_validation_reports_zero_cost() {
    let (lake, lake_store) = counted_lake();
    let before = counted(&[&lake.ravel, &lake_store]);
    let execution = lake
        .executor
        .execute_ddl(tenant("acme"), "SELECT 1", CREATED_BY, deadline())
        .await;
    let after = counted(&[&lake.ravel, &lake_store]);
    assert!(
        matches!(execution.result, Err(DdlExecuteError::Validation(_))),
        "{:?}",
        execution.result
    );
    assert_eq!(execution.cost, DdlCost::default());
    assert_eq!(before, after);
}
