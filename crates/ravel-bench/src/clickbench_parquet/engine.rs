//! The engine seam the ClickBench Parquet lane drives (ADR-2040, issue
//! #2055). [`SuiteEngine`] is the contract; [`InProcessEngine`] (Ravel's
//! `SqlExecutor` over an in-memory lake store), [`HttpEngine`] (a running
//! `ravel-server` over `POST /api/v1/sql`) and [`ReferenceEngine`] (plain
//! DataFusion over the fixture on local disk) implement it.

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

/// One query engine under test (ADR-2040 section D7): every implementation
/// runs the exact same suite statements and DDL template,
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

/// DataFusion `target_partitions` for both [`InProcessEngine`] and
/// [`ReferenceEngine`]. It fixes how many partial aggregates a
/// floating-point sum is combined from; left to DataFusion's default, the
/// reference would follow the host's available parallelism. It does not fix
/// the order the final aggregate merges those partials in, which follows
/// stream arrival, so a float result can still move in its last bits between
/// runs and the comparator's ULP tolerance has to cover that. 8 is what `SqlConfig::default()` already
/// resolves to (`ravel_query::DEFAULT_FETCH_CONCURRENCY`), so pinning it
/// leaves Ravel's plan the shape its default configuration produces.
pub const PLAN_PARTITIONS: usize = 8;

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
    use ravel_query::{EngineConfig, GetLimiter, LogSegmentFetcher, SegmentFetcher};
    use ravel_sql::{
        DEFAULT_PARQUET_METADATA_CACHE_BYTES, DdlOutcome, ExternalStoreMap, ExternalStores,
        ParquetSources, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    };
    use ravel_types::{TenantHash, TenantId, TimeRange};

    use super::{DdlReceipt, EngineError, PLAN_PARTITIONS, RecordBatch, SuiteEngine};

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
        /// Uploads every file directly under `fixture_dir` to the lake store,
        /// which stands for the `clickbench` bucket
        /// (`s3://clickbench/hits.parquet` for the combined file,
        /// `s3://clickbench/hits/<name>` for each part), grants the tenant
        /// the whole bucket on profile `lake`, and builds a
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
                    SqlConfig {
                        engine: EngineConfig {
                            sql_partition_count: Some(PLAN_PARTITIONS),
                            ..EngineConfig::default()
                        },
                        ..SqlConfig::default()
                    },
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
            // The lake store is the `clickbench` bucket itself, so keys carry
            // no bucket segment: `s3://clickbench/hits/` names `hits/...`.
            let key = if name == "hits.parquet" {
                name.to_string()
            } else {
                format!("hits/{name}")
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

/// A running `ravel-server`, reached over `POST /api/v1/sql`. Request
/// building and response decoding are plain functions so they can be tested
/// without a server.
#[cfg(feature = "sql-latency")]
mod http {
    use datafusion::arrow::ipc::reader::StreamReader;
    use reqwest::header::{ACCEPT, HeaderValue};
    use reqwest::{Client, Request, StatusCode};

    use super::{DdlReceipt, EngineError, RecordBatch, SuiteEngine};

    /// The media type that makes `/api/v1/sql` answer a query with an Arrow
    /// IPC stream instead of JSON.
    pub const ARROW_STREAM_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";

    /// Which of the two calls a request or response belongs to; picks the
    /// `EngineError` variant a rejection maps to.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Call {
        Ddl,
        Query,
    }

    impl Call {
        fn error(self, message: String) -> EngineError {
            match self {
                Call::Ddl => EngineError::Ddl(message),
                Call::Query => EngineError::Query(message),
            }
        }
    }

    pub struct HttpEngine {
        client: Client,
        base_url: String,
        token: String,
    }

    impl HttpEngine {
        /// `base_url` is the server root (for example `http://127.0.0.1:9090`);
        /// `token` is sent as `Authorization: Bearer <token>`.
        pub fn new(base_url: &str, token: &str) -> Self {
            HttpEngine {
                client: Client::new(),
                base_url: base_url.to_string(),
                token: token.to_string(),
            }
        }

        async fn send(&self, call: Call, sql: &str) -> Result<(StatusCode, Vec<u8>), EngineError> {
            let request = build_request(&self.client, &self.base_url, &self.token, call, sql)?;
            let response = self
                .client
                .execute(request)
                .await
                .map_err(|e| EngineError::Unreachable(format!("POST /api/v1/sql: {e}")))?;
            let status = response.status();
            let body = response
                .bytes()
                .await
                .map_err(|e| EngineError::Unreachable(format!("read response body: {e}")))?;
            Ok((status, body.to_vec()))
        }
    }

    /// `<base_url>/api/v1/sql`, tolerating a trailing `/` on `base_url`.
    pub fn sql_url(base_url: &str) -> String {
        format!("{}/api/v1/sql", base_url.trim_end_matches('/'))
    }

    /// The `POST` a call sends: `{"query": sql}` as JSON, a bearer token,
    /// and for a query, `Accept: application/vnd.apache.arrow.stream`.
    pub fn build_request(
        client: &Client,
        base_url: &str,
        token: &str,
        call: Call,
        sql: &str,
    ) -> Result<Request, EngineError> {
        let mut builder = client
            .post(sql_url(base_url))
            .bearer_auth(token)
            .json(&serde_json::json!({ "query": sql }));
        if call == Call::Query {
            builder = builder.header(ACCEPT, HeaderValue::from_static(ARROW_STREAM_MEDIA_TYPE));
        }
        builder
            .build()
            .map_err(|e| call.error(format!("build request: {e}")))
    }

    /// A non-2xx status as the call's `EngineError`, carrying the status and
    /// the body text; `Ok(())` for a 2xx.
    pub fn check_status(call: Call, status: StatusCode, body: &[u8]) -> Result<(), EngineError> {
        if status.is_success() {
            return Ok(());
        }
        Err(call.error(format!("HTTP {status}: {}", String::from_utf8_lossy(body))))
    }

    /// `data.outcome` and `data.files` from a DDL success body
    /// (`{"status":"success","data":{"outcome":"created","files":N,...}}`).
    /// `files` is absent from a `dropped` or `noop` outcome, and reads as
    /// `None` then.
    pub fn parse_ddl_body(body: &[u8]) -> Result<DdlReceipt, EngineError> {
        let value: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| EngineError::Ddl(format!("DDL response is not JSON: {e}")))?;
        let data = value
            .get("data")
            .ok_or_else(|| EngineError::Ddl(format!("DDL response has no data: {value}")))?;
        let outcome = data
            .get("outcome")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                EngineError::Ddl(format!("DDL response has no string data.outcome: {value}"))
            })?;
        let files = match data.get("files") {
            None => None,
            Some(files) => Some(files.as_u64().ok_or_else(|| {
                EngineError::Ddl(format!("DDL response data.files is not a count: {value}"))
            })?),
        };
        Ok(DdlReceipt {
            outcome: outcome.to_string(),
            files,
        })
    }

    /// Every batch of an Arrow IPC stream body, in stream order.
    pub fn decode_arrow_stream(body: &[u8]) -> Result<Vec<RecordBatch>, EngineError> {
        let reader = StreamReader::try_new(body, None)
            .map_err(|e| EngineError::Query(format!("Arrow IPC stream header: {e}")))?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| EngineError::Query(format!("Arrow IPC stream batch: {e}")))
    }

    #[async_trait::async_trait]
    impl SuiteEngine for HttpEngine {
        async fn ddl(&self, sql: &str) -> Result<DdlReceipt, EngineError> {
            let (status, body) = self.send(Call::Ddl, sql).await?;
            check_status(Call::Ddl, status, &body)?;
            parse_ddl_body(&body)
        }

        async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, EngineError> {
            let (status, body) = self.send(Call::Query, sql).await?;
            check_status(Call::Query, status, &body)?;
            decode_arrow_stream(&body)
        }
    }

    #[cfg(test)]
    #[allow(clippy::expect_used)]
    mod tests {
        use std::sync::Arc;

        use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::ipc::writer::StreamWriter;
        use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};

        use super::*;

        fn body_json(request: &Request) -> serde_json::Value {
            let bytes = request
                .body()
                .and_then(reqwest::Body::as_bytes)
                .expect("a JSON body is buffered, not streamed");
            serde_json::from_slice(bytes).expect("body is JSON")
        }

        #[test]
        fn a_query_posts_the_sql_with_a_bearer_token_and_asks_for_arrow() {
            let client = Client::new();
            let request = build_request(
                &client,
                "http://127.0.0.1:9090/",
                "secret-token",
                Call::Query,
                "SELECT COUNT(*) FROM hits",
            )
            .expect("request builds");
            assert_eq!(request.method(), reqwest::Method::POST);
            assert_eq!(request.url().as_str(), "http://127.0.0.1:9090/api/v1/sql");
            let headers = request.headers();
            assert_eq!(
                headers.get(AUTHORIZATION).expect("auth header"),
                "Bearer secret-token"
            );
            assert_eq!(
                headers.get(ACCEPT).expect("accept header"),
                ARROW_STREAM_MEDIA_TYPE
            );
            assert_eq!(
                headers.get(CONTENT_TYPE).expect("content type"),
                "application/json"
            );
            assert_eq!(
                body_json(&request),
                serde_json::json!({ "query": "SELECT COUNT(*) FROM hits" })
            );
        }

        #[test]
        fn a_ddl_posts_the_sql_without_asking_for_arrow() {
            let client = Client::new();
            let sql = "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION 's3://b/hits/'";
            let request =
                build_request(&client, "http://h:1", "t", Call::Ddl, sql).expect("request builds");
            assert_eq!(request.url().as_str(), "http://h:1/api/v1/sql");
            assert_eq!(
                request.headers().get(AUTHORIZATION).expect("auth header"),
                "Bearer t"
            );
            assert!(request.headers().get(ACCEPT).is_none());
            assert_eq!(body_json(&request), serde_json::json!({ "query": sql }));
        }

        /// The body shape `ravel-server`'s `ddl_outcome_json` writes for a
        /// created table.
        #[test]
        fn a_recorded_create_body_parses_to_its_receipt() {
            let body = br#"{"status":"success","data":{"outcome":"created","table":"hits","version":1,"files":4,"skipped_directory_markers":0,"skipped_other_suffixes":0}}"#;
            assert_eq!(
                parse_ddl_body(body).expect("parses"),
                DdlReceipt {
                    outcome: "created".to_string(),
                    files: Some(4),
                }
            );
        }

        #[test]
        fn a_recorded_drop_body_parses_with_no_file_count() {
            let body =
                br#"{"status":"success","data":{"outcome":"dropped","table":"hits","version":2}}"#;
            assert_eq!(
                parse_ddl_body(body).expect("parses"),
                DdlReceipt {
                    outcome: "dropped".to_string(),
                    files: None,
                }
            );
        }

        #[test]
        fn a_ddl_body_without_an_outcome_or_with_a_non_count_files_is_refused() {
            assert!(matches!(
                parse_ddl_body(br#"{"status":"success","data":{"files":1}}"#),
                Err(EngineError::Ddl(_))
            ));
            assert!(matches!(
                parse_ddl_body(br#"{"status":"success","data":{"outcome":"created","files":"4"}}"#),
                Err(EngineError::Ddl(_))
            ));
            assert!(matches!(
                parse_ddl_body(b"not json"),
                Err(EngineError::Ddl(_))
            ));
        }

        #[test]
        fn a_non_success_status_carries_the_status_and_the_body_text() {
            let err = check_status(
                Call::Ddl,
                StatusCode::UNPROCESSABLE_ENTITY,
                b"location outside grants",
            )
            .expect_err("422 is a rejection");
            assert_eq!(
                err,
                EngineError::Ddl(
                    "HTTP 422 Unprocessable Entity: location outside grants".to_string()
                )
            );
            let err = check_status(Call::Query, StatusCode::BAD_REQUEST, b"bad sql")
                .expect_err("400 is a rejection");
            assert_eq!(
                err,
                EngineError::Query("HTTP 400 Bad Request: bad sql".to_string())
            );
            check_status(Call::Query, StatusCode::OK, b"").expect("200 passes");
        }

        #[test]
        fn an_encoded_arrow_stream_decodes_to_the_same_batches() {
            let schema = Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("s", DataType::Utf8, true),
                Field::new("f", DataType::Float64, false),
            ]));
            let first = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec![Some("a"), None])),
                    Arc::new(Float64Array::from(vec![0.1, -0.0])),
                ],
            )
            .expect("batch");
            let second = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![3])),
                    Arc::new(StringArray::from(vec![Some("c")])),
                    Arc::new(Float64Array::from(vec![f64::NAN])),
                ],
            )
            .expect("batch");
            let mut buf = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, &schema).expect("writer");
                writer.write(&first).expect("write");
                writer.write(&second).expect("write");
                writer.finish().expect("finish");
            }
            let decoded = decode_arrow_stream(&buf).expect("decodes");
            assert_eq!(decoded, vec![first, second]);
        }

        #[test]
        fn a_truncated_arrow_stream_is_a_query_error() {
            assert!(matches!(
                decode_arrow_stream(b"\xff\xff"),
                Err(EngineError::Query(_))
            ));
        }
    }
}

#[cfg(feature = "sql-latency")]
pub use http::HttpEngine;

/// In-process DataFusion over the ClickBench fixture on the local
/// filesystem: the reference the suite's engines are compared against in CI.
/// It shares no Ravel code with [`in_process::InProcessEngine`], so it
/// catches defects in Ravel's own SQL, scan and aggregate layers. It links
/// the same DataFusion build as Ravel, so a defect in that build gives both
/// sides the same answer and passes; ADR-2040 D7's reference is datafusion-cli
/// output generated on the reference machine, which this does not replace.
#[cfg(feature = "sql-latency")]
mod reference {
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_datasource_parquet::ParquetFormat;

    use super::{DdlReceipt, EngineError, PLAN_PARTITIONS, RecordBatch, SuiteEngine};

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
        /// `create.sql`'s `CREATE VIEW hits` statement on top of it. The
        /// session runs at [`PLAN_PARTITIONS`] target partitions.
        pub async fn new(location: &Path) -> Result<Self, EngineError> {
            let ctx = SessionContext::new_with_config(
                SessionConfig::new().with_target_partitions(PLAN_PARTITIONS),
            );
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

#[cfg(all(test, feature = "sql-latency"))]
mod tests {
    use ravel_sql::SqlConfig;

    use super::PLAN_PARTITIONS;

    /// [`PLAN_PARTITIONS`] equals the partition count `SqlConfig::default()`
    /// resolves to, as its doc states; a change to that default fails here
    /// rather than leaving the doc stale.
    #[test]
    fn plan_partitions_matches_the_default_sql_partition_count() {
        assert_eq!(
            SqlConfig::default().engine.sql_partition_count(),
            PLAN_PARTITIONS
        );
    }
}
