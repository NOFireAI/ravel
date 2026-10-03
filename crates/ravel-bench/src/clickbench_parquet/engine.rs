//! The engine seam a later task's acceptance test drives (ADR-2040, issue
//! #2055 task T5b). [`SuiteEngine`] is the only contract this module
//! defines: no implementation lives here. A later task wires a concrete
//! engine (an in-process DataFusion session, a running `ravel-server`'s
//! Flight SQL endpoint, an upstream reference engine) against the suite and
//! fixture this crate already ships.

use datafusion::arrow::record_batch::RecordBatch;

/// What a `CREATE EXTERNAL TABLE` (or equivalent DDL) call reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlReceipt {
    /// The engine's own human-readable outcome string (a status line, a
    /// row count message); not interpreted by callers beyond logging it.
    pub outcome: String,
    /// Number of files the engine reports it mounted, when the engine
    /// exposes that count.
    pub files: Option<u64>,
}

/// Everything a [`SuiteEngine`] call can fail on.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// The DDL statement was rejected.
    #[error("ddl rejected: {0}")]
    Ddl(String),
    /// The query statement was rejected or failed during execution.
    #[error("query failed: {0}")]
    Query(String),
    /// The engine could not be reached at all (a transport error, a closed
    /// connection), as distinct from the engine reaching back with a
    /// rejection.
    #[error("engine unreachable: {0}")]
    Unreachable(String),
}

/// One query engine under test. A later task implements this against each
/// engine the acceptance test compares (ADR-2040 section D7): every
/// implementation runs the exact same suite statements and DDL template,
/// so the comparator in [`super::comparator`] is comparing engines, not
/// comparing a hand-written harness against itself.
#[async_trait::async_trait]
pub trait SuiteEngine: Send + Sync {
    /// Runs one DDL statement (the rendered `suite.toml` table template)
    /// against the engine, mounting the fixture for subsequent `query`
    /// calls.
    async fn ddl(&self, sql: &str) -> Result<DdlReceipt, EngineError>;

    /// Runs one suite statement and returns its result rows.
    async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, EngineError>;
}

/// In-process `SqlExecutor`, wired to a second "lake" `MemoryStore` the way
/// `ravel-sql`'s own `tests/parquet_ddl.rs` wires one, with the ClickBench
/// fixture uploaded under a grant this engine mounts (ADR-2040).
#[cfg(feature = "sql-latency")]
mod in_process {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use ravel_catalog::{Catalog, CatalogConfig};
    use ravel_memory::MemoryBudget;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_pqtable::clock::FixedClock;
    use ravel_pqtable::grants;
    use ravel_query::{GetLimiter, LogSegmentFetcher, SegmentFetcher};
    use ravel_sql::{
        DEFAULT_PARQUET_METADATA_CACHE_BYTES, DdlOutcome, ExternalStoreMap, ExternalStores,
        ParquetSources, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    };
    use ravel_types::{TenantHash, TenantId, TimeRange};

    use super::{DdlReceipt, EngineError, RecordBatch, SuiteEngine};

    const PROFILE: &str = "lake";
    const BUCKET: &str = "clickbench";
    const CREATED_BY: &str = "clickbench-parquet-lane";
    const NOW_NS: i64 = 1_700_000_000_000_000_000;
    const TENANT: &str = "clickbench";

    /// Every `LOCATION` the suite's rendered DDL template points at must sit
    /// under this grant.
    pub fn grant_url() -> String {
        format!("s3://{BUCKET}/")
    }

    fn full_window() -> TimeRange {
        // Parquet tables resolve through grants and manifests, not the RSEG
        // commit-listing window (`SqlRequest::window`'s own doc comment), so
        // any window wide enough to never clip is safe here.
        TimeRange {
            start_ns: 0,
            end_ns: i64::MAX / 2,
        }
    }

    pub struct InProcessEngine {
        tenant: TenantHash,
        executor: Arc<SqlExecutor>,
    }

    impl InProcessEngine {
        /// Uploads every file directly under `fixture_dir` to the lake store
        /// (`clickbench/hits.parquet` for the combined file,
        /// `clickbench/hits/<name>` for each part), grants the tenant the
        /// whole `clickbench` bucket on profile `lake`, and builds a
        /// `SqlExecutor` whose Parquet sources reach it through that profile.
        pub async fn new(fixture_dir: &Path) -> Result<Self, EngineError> {
            let ravel_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let lake_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

            let tenant = TenantId::new(TENANT.to_string()).hash();
            let clock = FixedClock::new(NOW_NS);
            grants::add(
                ravel_store.as_ref(),
                &tenant,
                PROFILE,
                &grant_url(),
                CREATED_BY,
                &clock,
            )
            .await
            .map_err(|e| EngineError::Unreachable(format!("grant: {e}")))?;

            upload_fixture(lake_store.as_ref(), fixture_dir).await?;

            let catalog = Arc::new(
                Catalog::new(Arc::clone(&ravel_store), CatalogConfig::default())
                    .map_err(|e| EngineError::Unreachable(format!("catalog: {e}")))?,
            );
            let external = Arc::new(ExternalStoreMap::new(HashMap::from([(
                PROFILE.to_string(),
                Arc::clone(&lake_store),
            )]))) as Arc<dyn ExternalStores>;
            let limiter = Arc::new(
                GetLimiter::new(8)
                    .map_err(|e| EngineError::Unreachable(format!("limiter: {e}")))?,
            );
            let sources = ParquetSources::new(
                Arc::clone(&ravel_store),
                Some(external),
                limiter,
                None,
                DEFAULT_PARQUET_METADATA_CACHE_BYTES,
            );
            let executor = Arc::new(
                SqlExecutor::new(
                    catalog,
                    SegmentFetcher::new(Arc::clone(&ravel_store)),
                    LogSegmentFetcher::new(Arc::clone(&ravel_store)),
                    SpanSegmentFetcher::new(Arc::clone(&ravel_store)),
                    SqlConfig::default(),
                    1 << 30,
                )
                .with_parquet_sources(sources)
                .with_process_memory_budget(Arc::new(MemoryBudget::unlimited()))
                .with_clock(Arc::new(clock)),
            );

            Ok(InProcessEngine { tenant, executor })
        }
    }

    async fn upload_fixture(lake: &dyn ObjectStoreBackend, dir: &Path) -> Result<(), EngineError> {
        let entries = std::fs::read_dir(dir)
            .map_err(|e| EngineError::Unreachable(format!("read fixture dir: {e}")))?;
        for entry in entries {
            let entry =
                entry.map_err(|e| EngineError::Unreachable(format!("read fixture entry: {e}")))?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                EngineError::Unreachable(format!("non-utf8 fixture name: {path:?}"))
            })?;
            let key = if name == "hits.parquet" {
                "clickbench/hits.parquet".to_string()
            } else {
                format!("clickbench/hits/{name}")
            };
            let bytes = std::fs::read(&path).map_err(|e| {
                EngineError::Unreachable(format!("read fixture file {path:?}: {e}"))
            })?;
            lake.put(&key, Bytes::from(bytes), PutOptions::default())
                .await
                .map_err(|e| EngineError::Unreachable(format!("upload {key}: {e}")))?;
        }
        Ok(())
    }

    fn ddl_receipt(outcome: &DdlOutcome) -> DdlReceipt {
        match outcome {
            DdlOutcome::Created { files, .. } => DdlReceipt {
                outcome: "created".to_string(),
                files: Some(*files as u64),
            },
            DdlOutcome::Dropped { .. } => DdlReceipt {
                outcome: "dropped".to_string(),
                files: None,
            },
            DdlOutcome::NoOp { .. } => DdlReceipt {
                outcome: "noop".to_string(),
                files: None,
            },
        }
    }

    #[async_trait::async_trait]
    impl SuiteEngine for InProcessEngine {
        async fn ddl(&self, sql: &str) -> Result<DdlReceipt, EngineError> {
            let outcome = self
                .executor
                .execute_ddl(self.tenant, sql, CREATED_BY, Duration::from_secs(30))
                .await
                .map_err(|e| EngineError::Ddl(e.to_string()))?;
            Ok(ddl_receipt(&outcome))
        }

        async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, EngineError> {
            let request = SqlRequest {
                sql: sql.to_string(),
                window: full_window(),
                min_tokens: Vec::new(),
                now_ns: NOW_NS,
                deadline: Duration::from_secs(30),
                row_window: false,
                max_rows: None,
                budgets: None,
            };
            let outcome = self
                .executor
                .execute(self.tenant, &request)
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok(outcome.output.batches().to_vec())
        }
    }
}

#[cfg(feature = "sql-latency")]
pub use in_process::InProcessEngine;

/// In-process DataFusion over the ClickBench fixture on the local
/// filesystem: the oracle the suite's engines are compared against. Shares
/// no Ravel code with [`in_process::InProcessEngine`], so a defect common to
/// both the subject engine and its oracle cannot hide a wrong answer.
#[cfg(feature = "sql-latency")]
mod reference {
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;
    use datafusion_datasource_parquet::ParquetFormat;

    use super::{DdlReceipt, EngineError, RecordBatch, SuiteEngine};

    /// The checked-in `create.sql` text, embedded at compile time. Its first
    /// statement (`CREATE EXTERNAL TABLE hits_raw ... LOCATION
    /// 'hits.parquet'`) is never run as SQL here: the relative `LOCATION` is
    /// meaningless outside the fixture's own temp directory, so this engine
    /// instead registers `hits_raw` directly as a `ListingTable` over
    /// whatever `location` the caller names. Only the second statement
    /// (`CREATE VIEW hits ...`) runs, verbatim, against that table.
    const CREATE_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/clickbench/parquet/create.sql"
    ));

    fn create_view_statement() -> Result<&'static str, EngineError> {
        CREATE_SQL
            .split(';')
            .map(str::trim)
            .find(|stmt| stmt.to_ascii_uppercase().starts_with("CREATE VIEW"))
            .ok_or_else(|| {
                EngineError::Unreachable(
                    "create.sql carries no CREATE VIEW hits statement".to_string(),
                )
            })
    }

    pub struct ReferenceEngine {
        ctx: SessionContext,
    }

    impl ReferenceEngine {
        /// Registers `hits_raw` as a `ListingTable` over `location` -- a
        /// directory holding the four fixture parts, or the single combined
        /// file, whichever `location` names -- using
        /// `ParquetFormat::default().with_binary_as_string(true)`, then runs
        /// `create.sql`'s `CREATE VIEW hits` statement on top of it.
        pub async fn new(location: &Path) -> Result<Self, EngineError> {
            let ctx = SessionContext::new();
            let format = Arc::new(ParquetFormat::default().with_binary_as_string(true));
            let options = ListingOptions::new(format);
            let url = ListingTableUrl::parse(location.to_string_lossy())
                .map_err(|e| EngineError::Unreachable(format!("listing url: {e}")))?;
            let state = ctx.state();
            let config = ListingTableConfig::new(url)
                .with_listing_options(options)
                .infer_schema(&state)
                .await
                .map_err(|e| EngineError::Unreachable(format!("infer schema: {e}")))?;
            let table = ListingTable::try_new(config)
                .map_err(|e| EngineError::Unreachable(format!("listing table: {e}")))?;
            ctx.register_table("hits_raw", Arc::new(table))
                .map_err(|e| EngineError::Unreachable(format!("register hits_raw: {e}")))?;

            ctx.sql(create_view_statement()?)
                .await
                .map_err(|e| EngineError::Ddl(format!("create view hits: {e}")))?;

            Ok(ReferenceEngine { ctx })
        }
    }

    #[async_trait::async_trait]
    impl SuiteEngine for ReferenceEngine {
        async fn ddl(&self, sql: &str) -> Result<DdlReceipt, EngineError> {
            self.ctx
                .sql(sql)
                .await
                .map_err(|e| EngineError::Ddl(e.to_string()))?;
            Ok(DdlReceipt {
                outcome: "ok".to_string(),
                files: None,
            })
        }

        async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, EngineError> {
            self.ctx
                .sql(sql)
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?
                .collect()
                .await
                .map_err(|e| EngineError::Query(e.to_string()))
        }
    }
}

#[cfg(feature = "sql-latency")]
pub use reference::ReferenceEngine;
