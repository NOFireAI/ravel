//! Reachability proof for the MCP adapter mounted into the shipping
//! `ravel-server` binary's start path (issue #1381): a real
//! [`ravel_server::start`] server over a `MemoryStore`, not a hand-built
//! router, serves `POST /mcp` when the `mcp` feature and `--mcp` are both on,
//! refuses every request the pre-protocol checks are there to refuse, and
//! answers `ravel_capabilities` end to end for a real MCP client.
//!
//! The MCP tests are behind `feature = "mcp"`; the one test that proves the
//! route is absent without it is behind `not(feature = "mcp")`, so both
//! halves of the packaging decision (ADR-1374 decision 9) are covered by the
//! two builds the gate list runs.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::{McpConfig, QueryBudgets};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

/// The bearer token the primary listener's resolver accepts.
const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
/// The one origin the deployment under test allows. Every MCP test starts its
/// server with a non-empty allowlist, so the origin check is live rather than
/// disabled by an empty list.
const ALLOWED_ORIGIN: &str = "https://console.example";
/// Small enough that a test can exceed it with a body it writes by hand, and
/// large enough that every legitimate message below fits.
const MAX_BODY_BYTES: u64 = 4096;

/// The deployment's config, with the MCP surface as `--mcp`,
/// `--mcp-allowed-origins`, and `--mcp-max-body-bytes` resolve it.
fn server_config(mcp: McpConfig) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: ravel_maintain::AuditTextPolicy::default(),
        query_budgets: QueryBudgets {
            mcp,
            ..Default::default()
        },
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Query,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![TenantId::new(TENANT).hash()],
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        // The two periodic tasks that touch object storage on their own
        // schedule, pushed past any test's lifetime: a store probe or an
        // admission reconcile landing mid-test would add store calls that
        // `request_without_credential_is_401_before_any_store_access` would
        // then have to tolerate, and a tolerance is not a zero.
        store_probe_interval: Duration::from_secs(3600),
        admission_reconcile_interval: Duration::from_secs(3600),
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}

/// The MCP posture every test that expects the route to exist starts with.
fn mcp_on() -> McpConfig {
    McpConfig {
        enabled: true,
        allowed_origins: vec![ALLOWED_ORIGIN.to_string()],
        max_body_bytes: MAX_BODY_BYTES,
    }
}

/// Start the real server over `store`, sharing `store_metrics` so a test can
/// read what the process spent on object storage.
async fn start_with(
    config: ServerConfig,
    store: Arc<dyn ObjectStoreBackend>,
    store_metrics: Arc<ravel_object_store::StoreMetrics>,
) -> ravel_server::Running {
    ravel_server::start(config, store.clone(), store, store_metrics, None)
        .await
        .expect("server starts")
}

/// [`start_with`] on the config every test but the admission one uses.
async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    store_metrics: Arc<ravel_object_store::StoreMetrics>,
    mcp: McpConfig,
) -> ravel_server::Running {
    start_with(server_config(mcp), store, store_metrics).await
}

/// A default store stack: a `MemoryStore` behind the instrumentation decorator
/// whose counters the store-access assertions read.
fn instrumented_memory() -> (
    Arc<dyn ObjectStoreBackend>,
    Arc<ravel_object_store::StoreMetrics>,
) {
    let metrics = Arc::new(ravel_object_store::StoreMetrics::default());
    let store = Arc::new(ravel_object_store::InstrumentedStore::with_metrics(
        MemoryStore::new(),
        Arc::clone(&metrics),
    ));
    (store, metrics)
}

/// Total completed store calls across every operation kind. A delta of zero
/// over a request means that request read and wrote nothing.
#[cfg(feature = "mcp")]
fn total_store_calls(metrics: &ravel_object_store::StoreMetrics) -> u64 {
    let snapshot = metrics.snapshot();
    snapshot.put.calls
        + snapshot.get.calls
        + snapshot.head.calls
        + snapshot.list.calls
        + snapshot.list_delimited.calls
        + snapshot.delete.calls
}

fn mcp_url(running: &ravel_server::Running) -> String {
    format!("http://{}/mcp", running.http_addr)
}

/// The tenant every authenticated request below resolves to.
#[cfg(feature = "mcp")]
fn expected_tenant() -> ravel_types::TenantHash {
    TenantId::new(TENANT).hash()
}

/// A JSON-RPC `initialize` body stating `revision`, the legacy lifecycle's
/// first message. The protocol layer requires the stated revision to agree
/// with the `MCP-Protocol-Version` header when both are present.
fn initialize_body(revision: &str) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": revision,
            "capabilities": {},
            "clientInfo": { "name": "ravel-test-client", "version": "0.0.0" }
        }
    })
}

/// A JSON-RPC `tools/call` body for `ravel_capabilities`, carrying the
/// per-request `_meta` that the sessionless current revision requires in place
/// of the state a handshake would have established.
#[cfg(feature = "mcp")]
fn tools_call_body() -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "ravel_capabilities",
            "arguments": {},
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }
    })
}

/// The `mcp` feature is off, so the route the feature mounts does not exist
/// and the server answers the shipping binary's own 404 for an unknown path.
/// Nothing about the configuration changes that: `--mcp` is on here, and the
/// route is still absent, which is what ADR-1374 decision 9 requires of a
/// build that does not carry the surface.
#[cfg(not(feature = "mcp"))]
#[tokio::test]
async fn feature_off_build_serves_no_mcp_route() {
    let (store, metrics) = instrumented_memory();
    let running = start_server(store, metrics, mcp_on()).await;

    let response = reqwest::Client::new()
        .post(mcp_url(&running))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("accept", "application/json, text/event-stream")
        .json(&initialize_body("2025-11-25"))
        .send()
        .await
        .expect("the request reaches the server");
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    running.shutdown().await.expect("server stops");
}

#[cfg(feature = "mcp")]
mod with_mcp {
    use super::*;

    use ravel_mcp::budget::{McpBudgetConfig, McpEffectiveBudgets, McpRequestBudgets};
    use ravel_mcp::draft::{Draft, EstimateComponent, NoCursorSignal, PageOrder, PinScope};
    use ravel_mcp::envelope::{FailureClass, Status};
    use ravel_mcp::service::QueryBackend;
    use ravel_mcp::tools::{ToolContext, describe_data};
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
    use ravel_query::http::InstantRequest;
    use ravel_server::mcp::ServiceBackend;
    use ravel_server::metrics::QueryOutcomeStatus;
    use reqwest::StatusCode;
    use rmcp::model::{CallToolRequestParams, ProtocolVersion};
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use rmcp::{ClientServiceExt, service::ClientLifecycleMode};

    /// A client transport pointed at `running`'s MCP route, carrying `TOKEN`.
    fn transport(
        running: &ravel_server::Running,
    ) -> StreamableHttpClientTransport<reqwest::Client> {
        StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(mcp_url(running)).auth_header(TOKEN),
        )
    }

    /// The lifecycle a client on the current revision uses: no `initialize`,
    /// a `server/discover` and then self-contained per-request metadata.
    fn current_revision() -> ClientLifecycleMode {
        ClientLifecycleMode::Discover {
            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
        }
    }

    /// One raw POST to the MCP route, so a test can state exactly which
    /// headers the request carries. `headers` are applied after the defaults,
    /// so a test can override or omit any of them.
    async fn post_raw(
        running: &ravel_server::Running,
        headers: &[(&str, &str)],
        body: String,
    ) -> reqwest::Response {
        let mut request = reqwest::Client::new()
            .post(mcp_url(running))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .body(body);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request
            .send()
            .await
            .expect("the request reaches the server")
    }

    /// A 400 raised by the adapter's per-revision header check, distinguished
    /// from one the protocol layer raises by the body shape: the adapter
    /// returns the same `errorType`-tagged JSON the HTTP query routes return,
    /// and rmcp returns a JSON-RPC `error` object.
    async fn assert_refused_by_the_header_check(response: reqwest::Response) {
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = response.json().await.expect("a JSON error body");
        assert_eq!(body["status"], serde_json::json!("error"));
        assert_eq!(body["errorType"], serde_json::json!("bad_data"));
        assert!(body["error"].is_string());
    }

    /// A real MCP client on the current revision reaches `ravel_capabilities`
    /// through the route the shipping start path mounted, authenticating with
    /// a bearer token the deployment's own resolver accepts. This is the
    /// epic's reachability test: nothing here builds a router.
    ///
    /// The call reads no object storage, which the store counters state
    /// exactly rather than approximately: `ravel_capabilities` answers from
    /// compile-time facts and the call's own resolved context.
    #[tokio::test]
    async fn capabilities_over_streamable_http_with_bearer() {
        let (store, metrics) = instrumented_memory();
        let running = start_server(store, Arc::clone(&metrics), mcp_on()).await;

        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let tools = client.list_all_tools().await.expect("tools/list answers");
        assert_eq!(tools.len(), 9);

        let before = total_store_calls(&metrics);
        let result = client
            .call_tool(
                CallToolRequestParams::new("ravel_capabilities")
                    .with_arguments(serde_json::Map::new()),
            )
            .await
            .expect("ravel_capabilities answers");
        assert_eq!(total_store_calls(&metrics) - before, 0);

        assert_eq!(result.is_error, Some(false));
        let envelope = result
            .structured_content
            .as_ref()
            .expect("the tool result carries the envelope as structured content");
        assert_eq!(envelope["status"], serde_json::json!("ok"));
        assert_eq!(
            envelope["scope"]["table"],
            serde_json::json!("capabilities")
        );
        assert_eq!(envelope["data"]["row_count"], serde_json::json!(6));

        let rows = envelope["data"]["rows"].as_array().expect("rows");
        assert_eq!(rows.len(), 6);
        assert_eq!(
            rows[0][1]["revisions"],
            serde_json::json!(["2026-07-28", "2025-11-25"])
        );
        // The tenant the transport authenticated, not one the arguments could
        // name: the tool reads it off the context `auth` resolved.
        assert_eq!(
            rows[3][1]["hash"],
            serde_json::json!(expected_tenant().to_hex())
        );
        // Both renderings of the same envelope reach the caller.
        assert_eq!(result.content.len(), 1);

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// A budget field of the wrong type refuses the call and names the field,
    /// rather than dropping the caller's whole budget block and running at the
    /// server defaults.
    ///
    /// `max_rows` is well formed here and `deadline_ms` is not, which is the
    /// case a permissive deserialize of the block as a whole loses: the call
    /// would run at 200 rows, report that ceiling as `budget.effective`, and
    /// give the caller nothing to notice. The tool body cannot catch it
    /// either, because `ravel_capabilities` takes no arguments and accepts any
    /// object. So the envelope must carry the failure, `budget.effective` must
    /// stay empty rather than state a ceiling nothing ran under, and the store
    /// counters must show exactly zero calls.
    #[tokio::test]
    async fn a_malformed_budget_field_is_invalid_argument_not_a_silent_default() {
        let (store, metrics) = instrumented_memory();
        let running = start_server(store, Arc::clone(&metrics), mcp_on()).await;

        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let before = total_store_calls(&metrics);
        let mut arguments = serde_json::Map::new();
        arguments.insert("max_rows".to_string(), serde_json::json!(1));
        arguments.insert("deadline_ms".to_string(), serde_json::json!("oops"));
        let result = client
            .call_tool(CallToolRequestParams::new("ravel_capabilities").with_arguments(arguments))
            .await
            .expect("the refusal comes back as a tool result, not a protocol error");
        assert_eq!(total_store_calls(&metrics) - before, 0);

        assert_eq!(result.is_error, Some(true));
        let envelope = result
            .structured_content
            .as_ref()
            .expect("the tool result carries the envelope as structured content");
        assert_eq!(envelope["status"], serde_json::json!("error"));
        assert_eq!(
            envelope["failure"]["class"],
            serde_json::json!("invalid_argument")
        );
        let message = envelope["failure"]["message"]
            .as_str()
            .expect("the failure carries a message");
        assert!(
            message.contains("deadline_ms"),
            "the message must name the offending field, got {message:?}"
        );
        // No budget was resolved, so none is reported: a ceiling here would
        // state what the call was allowed when the call never ran.
        assert_eq!(envelope["budget"]["effective"], serde_json::Value::Null);
        assert_eq!(envelope["data"]["row_count"], serde_json::json!(0));
        assert_eq!(envelope["data"]["rows"].as_array().expect("rows").len(), 0);

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// A request with no resolvable credential is refused before the protocol
    /// layer sees it, and before this process touches object storage at all.
    /// The store counters are the proof: exactly zero calls over the request.
    #[tokio::test]
    async fn request_without_credential_is_401_before_any_store_access() {
        let (store, metrics) = instrumented_memory();
        let running = start_server(store, Arc::clone(&metrics), mcp_on()).await;

        let before = total_store_calls(&metrics);
        let response = post_raw(&running, &[], initialize_body("2025-11-25").to_string()).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(total_store_calls(&metrics) - before, 0);

        // The same error contract the HTTP query routes return, so a caller
        // reading both surfaces parses one shape.
        let body: serde_json::Value = response.json().await.expect("a JSON error body");
        assert_eq!(body["status"], serde_json::json!("error"));
        assert_eq!(body["errorType"], serde_json::json!("unauthorized"));

        running.shutdown().await.expect("server stops");
    }

    /// A browser page on another site can reach this route with the user's
    /// ambient credentials, so an `Origin` outside the allowlist is refused
    /// even when the credential is valid.
    #[tokio::test]
    async fn wrong_origin_is_403() {
        let (store, metrics) = instrumented_memory();
        let running = start_server(store, metrics, mcp_on()).await;

        let refused = post_raw(
            &running,
            &[
                ("authorization", &format!("Bearer {TOKEN}")),
                ("origin", "https://evil.example"),
            ],
            initialize_body("2025-11-25").to_string(),
        )
        .await;
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        let body: serde_json::Value = refused.json().await.expect("a JSON error body");
        assert_eq!(body["errorType"], serde_json::json!("forbidden"));

        // The allowed origin, everything else equal, is served.
        let allowed = post_raw(
            &running,
            &[
                ("authorization", &format!("Bearer {TOKEN}")),
                ("origin", ALLOWED_ORIGIN),
            ],
            initialize_body("2025-11-25").to_string(),
        )
        .await;
        assert_eq!(allowed.status(), StatusCode::OK);

        running.shutdown().await.expect("server stops");
    }

    /// A client on the legacy revision still gets the handshake that revision
    /// defines, negotiates `2025-11-25`, and reaches the same tool.
    #[tokio::test]
    async fn legacy_2025_11_25_client_initializes() {
        let (store, metrics) = instrumented_memory();
        let running = start_server(store, metrics, mcp_on()).await;

        let client =
            ().serve_with_lifecycle(transport(&running), ClientLifecycleMode::Initialize)
                .await
                .expect("the legacy handshake completes");

        let info = client.peer_info().expect("the server answered initialize");
        assert_eq!(info.protocol_version, ProtocolVersion::V_2025_11_25);

        let result = client
            .call_tool(
                CallToolRequestParams::new("ravel_capabilities")
                    .with_arguments(serde_json::Map::new()),
            )
            .await
            .expect("ravel_capabilities answers on the legacy revision");
        let envelope = result
            .structured_content
            .as_ref()
            .expect("the tool result carries the envelope");
        assert_eq!(envelope["status"], serde_json::json!("ok"));
        assert_eq!(envelope["data"]["row_count"], serde_json::json!(6));

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// `initialize` belongs to the legacy lifecycle and is the message that
    /// establishes which revision applies, so the current revision's
    /// per-request metadata cannot be demanded of it. A client that states
    /// `2026-07-28` on its handshake and sends no `Mcp-Method` is served, and
    /// so is one that states the legacy revision or no revision at all.
    #[tokio::test]
    async fn legacy_initialize_is_accepted_without_mcp_method_header() {
        let (store, metrics) = instrumented_memory();
        let running = start_server(store, metrics, mcp_on()).await;

        let auth = format!("Bearer {TOKEN}");
        // Stating the current revision on the handshake: the header check
        // exempts `initialize` rather than refusing it for the missing
        // `Mcp-Method`, and the protocol layer negotiates from there.
        let current = post_raw(
            &running,
            &[
                ("authorization", &auth),
                ("mcp-protocol-version", "2026-07-28"),
            ],
            initialize_body("2026-07-28").to_string(),
        )
        .await;
        assert_eq!(current.status(), StatusCode::OK);

        // The legacy revision, where the header check applies to nothing.
        let legacy = post_raw(
            &running,
            &[
                ("authorization", &auth),
                ("mcp-protocol-version", "2025-11-25"),
            ],
            initialize_body("2025-11-25").to_string(),
        )
        .await;
        assert_eq!(legacy.status(), StatusCode::OK);

        // No revision stated at all, which is what a legacy client's first
        // message looks like before anything is negotiated.
        let unstated = post_raw(
            &running,
            &[("authorization", &auth)],
            initialize_body("2025-11-25").to_string(),
        )
        .await;
        assert_eq!(unstated.status(), StatusCode::OK);

        running.shutdown().await.expect("server stops");
    }

    /// On the current revision a request states its method and its target in
    /// headers, so an intermediary can route and authorize it without parsing
    /// JSON-RPC. A header that disagrees with the body states two different
    /// intents, and serving either one would be a guess.
    #[tokio::test]
    async fn current_revision_tools_call_requires_mcp_method_and_name() {
        let (store, metrics) = instrumented_memory();
        let running = start_server(store, metrics, mcp_on()).await;

        let auth = format!("Bearer {TOKEN}");
        let body = tools_call_body().to_string();

        // Each refusal is the adapter's own, not the protocol layer's: the
        // body is the `errorType`-tagged shape the HTTP query routes return,
        // and rmcp answers a request it dislikes with a JSON-RPC error object
        // instead. Without this the test would pass on a build whose header
        // check does nothing, because a `tools/call` rmcp rejects for its own
        // reasons is also a 400.
        let missing_method = post_raw(
            &running,
            &[
                ("authorization", &auth),
                ("mcp-protocol-version", "2026-07-28"),
            ],
            body.clone(),
        )
        .await;
        assert_refused_by_the_header_check(missing_method).await;

        let wrong_method = post_raw(
            &running,
            &[
                ("authorization", &auth),
                ("mcp-protocol-version", "2026-07-28"),
                ("mcp-method", "tools/list"),
                ("mcp-name", "ravel_capabilities"),
            ],
            body.clone(),
        )
        .await;
        assert_refused_by_the_header_check(wrong_method).await;

        let missing_name = post_raw(
            &running,
            &[
                ("authorization", &auth),
                ("mcp-protocol-version", "2026-07-28"),
                ("mcp-method", "tools/call"),
            ],
            body.clone(),
        )
        .await;
        assert_refused_by_the_header_check(missing_name).await;

        let wrong_name = post_raw(
            &running,
            &[
                ("authorization", &auth),
                ("mcp-protocol-version", "2026-07-28"),
                ("mcp-method", "tools/call"),
                ("mcp-name", "ravel_query_sql"),
            ],
            body.clone(),
        )
        .await;
        assert_refused_by_the_header_check(wrong_name).await;

        let agreeing = post_raw(
            &running,
            &[
                ("authorization", &auth),
                ("mcp-protocol-version", "2026-07-28"),
                ("mcp-method", "tools/call"),
                ("mcp-name", "ravel_capabilities"),
            ],
            body,
        )
        .await;
        assert_eq!(agreeing.status(), StatusCode::OK);

        running.shutdown().await.expect("server stops");
    }

    /// A body past the configured cap is refused before it is parsed, so a
    /// caller cannot make this process allocate or decode an oversized frame.
    #[tokio::test]
    async fn body_over_the_cap_is_413() {
        let (store, metrics) = instrumented_memory();
        let running = start_server(store, metrics, mcp_on()).await;

        let mut oversized = tools_call_body();
        oversized["params"]["arguments"] = serde_json::json!({
            "padding": "x".repeat(MAX_BODY_BYTES as usize),
        });
        let body = oversized.to_string();
        assert!(body.len() > MAX_BODY_BYTES as usize);

        let response = post_raw(
            &running,
            &[("authorization", &format!("Bearer {TOKEN}"))],
            body,
        )
        .await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let error: serde_json::Value = response.json().await.expect("a JSON error body");
        assert_eq!(error["errorType"], serde_json::json!("payload_too_large"));

        running.shutdown().await.expect("server stops");
    }

    /// A tool call whose caller goes away drops the engine call, and the drop
    /// releases the admission permit and bills what the dropped call had
    /// already spent.
    ///
    /// Driven through the [`ServiceBackend`] the adapter hands the tool layer
    /// rather than through the transport, because the only tool this build
    /// serves (`ravel_capabilities`) reaches no backend at all: it takes no
    /// permit, so cancelling it could not show one being released. The
    /// transport hop above it is what
    /// [`capabilities_over_streamable_http_with_bearer`] covers, and the
    /// adapter awaits this exact future beside the request's cancellation
    /// token with nothing spawned in between, so a dropped stream drops
    /// exactly the future this test drops.
    #[tokio::test]
    async fn dropped_stream_releases_admission_permit() {
        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let metrics = Arc::new(ravel_object_store::StoreMetrics::default());
        let mut config = server_config(mcp_on());
        // One permit, so a leaked one is the difference between a served call
        // and a refused one.
        config.query_concurrency_limit = ravel_query::QueryConcurrencyLimit::Bounded(1);
        // One catalog LIST at a time, so the gate below parks the resolve at
        // its first store call and no sibling completes behind it. With the
        // default fan-out the dropped call's spend depends on how many
        // concurrent LISTs happened to finish, which no exact figure can pin.
        config.catalog_resolve_concurrency = Some(1);
        let running = start_with(
            config,
            Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
            metrics,
        )
        .await;

        let service = running
            .query_service
            .clone()
            .expect("a query-serving mode builds one");
        let budgets = McpRequestBudgets::default().clamp(
            &ravel_query::EngineConfig::default(),
            &McpBudgetConfig::default(),
        );
        let backend = ServiceBackend::new(service, budgets);
        let tenant = expected_tenant();
        let request = InstantRequest {
            query: "up".to_string(),
            time_ms: 0,
            min_tokens: Vec::new(),
            deadline: Duration::from_secs(30),
            allow_partial: false,
            now_ns: 0,
            budgets: None,
        };

        // Hold the first catalog LIST, so the call below is parked inside the
        // engine holding the process's one permit.
        let gate = store.hold(Op::List, None, Occurrence::Nth(1));
        {
            let call = backend.promql_instant(tenant, &request);
            tokio::pin!(call);
            tokio::select! {
                _ = &mut call => panic!("the gated call cannot complete"),
                () = gate.wait_until_held(1) => {}
            }

            // The ceiling really is one: with the first call in flight, the
            // second is refused rather than queued. Without this the test
            // would pass on an unbounded deployment, where no permit is
            // scarce enough to leak.
            let refused = backend
                .promql_instant(tenant, &request)
                .await
                .expect_err("the ceiling refuses a second concurrent call");
            assert_eq!(refused.class, FailureClass::Unavailable);
        }
        // `call` is dropped here, which is what a closed stream does to it.
        for (id, _, _) in gate.held_details() {
            gate.release(id);
        }

        // The permit came back: the same call now runs to completion.
        backend
            .promql_instant(tenant, &request)
            .await
            .expect("the released permit admits the next call");

        let outcomes = running.query_accounting.outcome_snapshot();
        let canceled: Vec<_> = outcomes
            .iter()
            .filter(|row| row.status == QueryOutcomeStatus::Canceled)
            .collect();
        assert_eq!(canceled.len(), 1);
        // The dropped call's own usage record, with the exact spend it had
        // reached rather than the spend of a call that finished: three store
        // requests completed and the fourth was still parked in the gate, so
        // no bytes had arrived. A record is written all the same, because a
        // call abandoned mid-flight is still a call this tenant made.
        assert_eq!(
            canceled[0].counters,
            ravel_server::metrics::QueryCostCounters {
                queries: 1,
                s3_requests: 3,
                s3_bytes: 0,
                cache_hits: 0,
                cache_misses: 1,
                decompressed_bytes: 0,
                estimated_requests: 0,
                estimated_store_bytes: 0,
                estimated_decompressed_bytes: 0,
            }
        );

        // The call the released permit admitted, which is the same query run
        // to completion: it reached one further request beyond the dropped
        // call's three. The dropped call cached the HEAD's absence, so this
        // call's HEAD lookup is a cache hit and issues no GET. Only a
        // completed plan carries an estimate.
        let succeeded: Vec<_> = outcomes
            .iter()
            .filter(|row| row.status == QueryOutcomeStatus::Success)
            .collect();
        assert_eq!(succeeded.len(), 1);
        assert_eq!(
            succeeded[0].counters,
            ravel_server::metrics::QueryCostCounters {
                queries: 1,
                s3_requests: 4,
                s3_bytes: 0,
                cache_hits: 1,
                cache_misses: 0,
                decompressed_bytes: 0,
                estimated_requests: 4,
                estimated_store_bytes: 0,
                estimated_decompressed_bytes: 0,
            }
        );

        running.shutdown().await.expect("server stops");
    }

    /// Small, so the catalog's per-hour LIST fan-out over the SQL window stays
    /// a handful of calls.
    const SQL_NOW_NS: i64 = 4 * 3_600_000_000_000;

    /// Publish one sample of metric `m` at `ts_ns` `1 + ingest_hour` for the
    /// test tenant, as a real segment plus its commit record flushed in ingest
    /// hour `ingest_hour` by writer sequence `writer_seq`, so `samples` has a
    /// row to project and several calls with distinct hours and sequences land
    /// several segments, each with its own row.
    async fn publish_sample(store: &dyn ObjectStoreBackend, ingest_hour: u32, writer_seq: u64) {
        use ravel_commit::publish::RetryPolicy;
        use ravel_commit::record::NewCommitRecord;
        use ravel_commit::{keys, publish, record};
        use ravel_object_store::PutOptions;
        use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
        use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal};

        let tenant = TenantId::new(TENANT);
        let tenant_hash = tenant.hash();
        let labels = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "m".to_string(),
        }])
        .expect("valid labels");
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(&tenant, "m", &labels).expect("series id"),
            labels,
            samples: vec![Sample {
                ts_ns: 1 + i64::from(ingest_hour),
                value: 1.0,
            }],
        }];
        let writer_id = uuid::Uuid::from_u128(2_000);
        let hour_start_ns = i64::from(ingest_hour) * 3_600_000_000_000;
        let written = SegmentWriter::write(
            series,
            SegmentIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.to_string(),
                writer_epoch: 1,
                writer_seq,
            },
            IngestBounds {
                min_ingest_ts_ns: hour_start_ns,
                max_ingest_ts_ns: hour_start_ns,
            },
        )
        .expect("write segment");
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: hour_start_ns + 10,
            ingest_hour_bucket: ingest_hour,
        })
        .expect("valid commit record");
        store
            .put(
                &keys::reconstruct_data_key(&rec).expect("data key"),
                written.bytes,
                PutOptions::default(),
            )
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// A started server holding one [`publish_sample`] row from ingest hour 0,
    /// and the [`ServiceBackend`] the adapter hands `ravel_query_sql`'s body.
    ///
    /// Driven through the backend rather than the transport because
    /// `ravel_query_sql` is in the catalog but not served by this build: a
    /// `tools/call` for it answers `NotShipped` before any backend is reached.
    /// The mapping under test is the backend's own `sql_execute`, which is the
    /// call that tool body makes.
    async fn sql_backend() -> (ravel_server::Running, ServiceBackend) {
        let (running, backend, _) = sql_backend_over(&[0]).await;
        (running, backend)
    }

    /// [`sql_backend`] over one [`publish_sample`] segment per entry of
    /// `ingest_hours`, plus the query service the backend wraps.
    async fn sql_backend_over(
        ingest_hours: &[u32],
    ) -> (
        ravel_server::Running,
        ServiceBackend,
        ravel_server::service::QueryService,
    ) {
        let (store, metrics) = instrumented_memory();
        for (writer_seq, hour) in (1u64..).zip(ingest_hours) {
            publish_sample(store.as_ref(), *hour, writer_seq).await;
        }
        let running = start_server(store, metrics, mcp_on()).await;
        let service = running
            .query_service
            .clone()
            .expect("a query-serving mode builds one");
        (
            running,
            ServiceBackend::new(service.clone(), sql_budgets()),
            service,
        )
    }

    fn sql_budgets() -> McpEffectiveBudgets {
        McpRequestBudgets::default().clamp(
            &ravel_query::EngineConfig::default(),
            &McpBudgetConfig::default(),
        )
    }

    /// Finish `draft` the way a tool without a total order does.
    fn finish_unpaged(draft: Draft) -> ravel_mcp::envelope::Envelope {
        let scope = PinScope {
            tenant: expected_tenant(),
            range_start_ns: 0,
            range_end_ns: SQL_NOW_NS,
            min_commit_watermark: Vec::new(),
        };
        draft.finish_page(&sql_budgets(), &scope, PageOrder::Partial, |_, _| {
            panic!("a partial order never mints")
        })
    }

    fn sql_request(sql: &str) -> ravel_sql::SqlRequest {
        ravel_sql::SqlRequest {
            sql: sql.to_string(),
            window: ravel_types::TimeRange {
                start_ns: 0,
                end_ns: SQL_NOW_NS,
            },
            min_tokens: Vec::new(),
            now_ns: SQL_NOW_NS,
            deadline: Duration::from_secs(30),
            row_window: false,
            max_rows: None,
            budgets: None,
        }
    }

    /// The MCP twin of `/api/v1/sql`'s
    /// `a_result_json_cannot_encode_is_422_naming_the_column_and_arrow_ipc_reads_it`:
    /// a `Date64` that is not a whole day has no JSON date, so the call fails
    /// as `unsupported` naming the column, its type and the reason. The advice
    /// is what an MCP caller can act on: `/mcp` has no Arrow IPC output, so
    /// the HTTP message's "request the Arrow IPC format" must not appear, and
    /// the value itself never does.
    #[tokio::test]
    async fn a_result_json_cannot_encode_is_unsupported_naming_the_column() {
        const MILLIS: i64 = 1_700_000_000_123;
        let (running, backend) = sql_backend().await;

        let failure = backend
            .sql_execute(
                expected_tenant(),
                &sql_request(&format!(
                    "SELECT arrow_cast({MILLIS}, 'Date64') AS d FROM samples LIMIT 1"
                )),
            )
            .await
            .expect_err("a Date64 that is not a whole day has no JSON form");

        assert_eq!(failure.class, FailureClass::Unsupported);
        // The wire spelling a caller reads as `envelope["failure"]["class"]`.
        let wire = serde_json::to_value(&failure).expect("a failure serializes");
        assert_eq!(wire["class"], serde_json::json!("unsupported"));
        assert_eq!(
            failure.message,
            "column \"d\" of type Date64 cannot be encoded as JSON: a JSON date must be a whole \
             day from 0000-01-01 to 9999-12-31; cast the column in the query to a type JSON \
             can hold, or read it over /api/v1/sql with the Arrow IPC format"
        );
        assert!(
            !failure.message.contains(&MILLIS.to_string()),
            "the message must not carry the value: {}",
            failure.message
        );
        assert!(
            !failure.message.contains("request the Arrow IPC format"),
            "the HTTP advice has no MCP equivalent: {}",
            failure.message
        );

        running.shutdown().await.expect("server stops");
    }

    /// The contrast: a `Date64` that is a whole day, through the same backend
    /// call on the same fixture, encodes. So the failure above is the value's
    /// JSON form, not the fixture, the type or the statement shape.
    #[tokio::test]
    async fn a_whole_day_date64_on_the_same_fixture_encodes() {
        const WHOLE_DAY_MILLIS: i64 = 19_675 * 86_400_000;
        let (running, backend) = sql_backend().await;

        let draft = backend
            .sql_execute(
                expected_tenant(),
                &sql_request(&format!(
                    "SELECT arrow_cast({WHOLE_DAY_MILLIS}, 'Date64') AS d FROM samples LIMIT 1"
                )),
            )
            .await
            .expect("a whole-day Date64 has a JSON date");

        let envelope = serde_json::to_value(finish_unpaged(draft)).expect("an envelope serializes");
        assert_eq!(envelope["status"], serde_json::json!("ok"), "{envelope}");
        assert_eq!(envelope["data"]["row_count"], serde_json::json!(1));
        assert_eq!(
            envelope["data"]["rows"],
            serde_json::json!([["2023-11-14"]]),
            "{envelope}"
        );

        running.shutdown().await.expect("server stops");
    }

    /// `sql_execute` through the real adapter hands the tool the pin inputs
    /// of the snapshot the rows came from, equal to the `SqlOutcome` the
    /// query service returns for the same statement: the signal of its one
    /// lane, its pending erasure, its declared columns, and as the watermark
    /// the greatest `ingest_hour_bucket` across segments flushed in three
    /// different hours, written into `visibility.ingest_watermark_hour`.
    ///
    /// FLIP: a watermark taken as the least hour, or from the first segment
    /// in key order, reads 1 here instead of 3.
    /// FLIP: a draft whose resolve instant is the clock's and not the
    /// request's `now_ns` breaks the `resolve_at_ns` equality.
    #[tokio::test]
    async fn sql_execute_carries_the_outcomes_pin_inputs() {
        let (running, backend, service) = sql_backend_over(&[1, 3, 2]).await;
        let request = sql_request("SELECT value FROM samples");

        let outcome = service
            .sql_execute(expected_tenant(), &request)
            .await
            .expect("the service executes");
        let draft = backend
            .sql_execute(expected_tenant(), &request)
            .await
            .expect("the adapter executes");

        assert_eq!(outcome.ingest_watermark_hour, Some(3));
        assert_eq!(draft.ingest_watermark_hour, outcome.ingest_watermark_hour);
        assert_eq!(draft.pending_erasure, outcome.pending_erasure);
        assert_eq!(draft.declared_columns, outcome.declared_columns);
        assert_eq!(outcome.signals.len(), 1);
        assert_eq!(draft.signal, Ok(ravel_types::Signal::Metrics));
        assert_eq!(draft.resolve_at_ns, SQL_NOW_NS);
        assert_eq!(draft.envelope.visibility.ingest_watermark_hour, "3");
        assert!(!draft.envelope.visibility.resolved_no_segments);
        assert_eq!(draft.envelope.data.row_count, 3);

        running.shutdown().await.expect("server stops");
    }

    /// A statement over a tenant with no segments resolves none, so it has no
    /// watermark: `None`, an empty `visibility.ingest_watermark_hour`, and
    /// `resolved_no_segments` saying why it is empty.
    ///
    /// FLIP: defaulting the watermark to hour 0 reports a measurement the
    /// resolve never made.
    #[tokio::test]
    async fn sql_execute_over_an_empty_resolve_has_no_watermark() {
        let (running, backend, service) = sql_backend_over(&[]).await;
        let request = sql_request("SELECT value FROM samples");

        let outcome = service
            .sql_execute(expected_tenant(), &request)
            .await
            .expect("the service executes");
        let draft = backend
            .sql_execute(expected_tenant(), &request)
            .await
            .expect("the adapter executes");

        assert_eq!(outcome.ingest_watermark_hour, None);
        assert_eq!(draft.ingest_watermark_hour, None);
        assert_eq!(draft.signal, Ok(ravel_types::Signal::Metrics));
        assert!(draft.envelope.visibility.ingest_watermark_hour.is_empty());
        assert!(draft.envelope.visibility.resolved_no_segments);

        running.shutdown().await.expect("server stops");
    }

    /// A statement naming no table resolves the default `samples` lane, but
    /// that lane is not a table it read: its draft has no cursor signal, and
    /// its page reports neither the lane's watermark nor that it resolved no
    /// segments. The service outcome shows the lane is there to be misread:
    /// one signal entry, and on the tenant with data, its watermark.
    ///
    /// FLIP: classifying from `outcome.signals.len()` gives
    /// `Ok(Signal::Metrics)` here, and on the empty tenant sets
    /// `resolved_no_segments`.
    /// FLIP: filling `signal_tables_named` from `StatementTables::lanes()`
    /// rather than the named tables reads 1 and gives the same
    /// `Ok(Signal::Metrics)`.
    /// FLIP: a draft that writes the default lane's watermark reports hour 3
    /// on the tenant with data.
    #[tokio::test]
    async fn sql_execute_naming_no_table_has_no_cursor_signal() {
        for hours in [&[][..], &[3][..]] {
            let (running, backend, service) = sql_backend_over(hours).await;
            let request = sql_request("SELECT 1 AS x");

            let outcome = service
                .sql_execute(expected_tenant(), &request)
                .await
                .expect("the service executes");
            assert_eq!(outcome.signals.len(), 1, "the default lane is reported");
            assert_eq!(outcome.signal_tables_named, 0);
            assert_eq!(outcome.ingest_watermark_hour, hours.first().copied());

            let draft = backend
                .sql_execute(expected_tenant(), &request)
                .await
                .expect("the adapter executes");
            assert_eq!(draft.signal, Err(NoCursorSignal::NoTable));
            let page = finish_unpaged(draft);
            assert!(!page.visibility.resolved_no_segments, "hours {hours:?}");
            assert!(page.visibility.ingest_watermark_hour.is_empty());
            assert!(page.visibility.snapshot_id.is_empty());

            running.shutdown().await.expect("server stops");
        }
    }

    /// A statement naming no table can still trip a cap: two constant rows
    /// over a one-row cap. Under a total order the page is `ok_bounded` with
    /// no cursor and a warning naming why, and the mint never runs.
    ///
    /// FLIP: a cursor signal taken from the default lane mints here and the
    /// mint panics.
    #[tokio::test]
    async fn a_capped_statement_naming_no_table_is_ok_bounded() {
        let (running, backend, _) = sql_backend_over(&[3]).await;

        let draft = backend
            .sql_execute(
                expected_tenant(),
                &sql_request("SELECT 1 AS x UNION ALL SELECT 2 AS x"),
            )
            .await
            .expect("the adapter executes");
        let mut budgets = sql_budgets();
        budgets.max_rows = 1;
        let scope = PinScope {
            tenant: expected_tenant(),
            range_start_ns: 0,
            range_end_ns: SQL_NOW_NS,
            min_commit_watermark: Vec::new(),
        };
        let page = draft.finish_page(&budgets, &scope, PageOrder::Total, |_, _| {
            panic!("a statement naming no table must not reach the mint")
        });

        let envelope = serde_json::to_value(&page).expect("an envelope serializes");
        assert_eq!(
            envelope["status"],
            serde_json::json!("ok_bounded"),
            "{envelope}"
        );
        assert_eq!(page.presentation.cursor, None);
        assert_eq!(page.data.rows.len(), 1);
        assert_eq!(page.presentation.rows_omitted, 1);
        assert!(page.visibility.snapshot_id.is_empty());
        assert!(
            page.warnings
                .contains(&format!("no cursor: {}", NoCursorSignal::NoTable)),
            "{:?}",
            page.warnings
        );

        running.shutdown().await.expect("server stops");
    }

    /// `sql_explain` through the adapter carries the service report's
    /// estimate figure for figure, its segment counts, and its schema, and
    /// names the components the estimator cannot bound: none for a metrics
    /// statement, `estimated_decompressed_bytes` for a logs one.
    ///
    /// FLIP: dropping `unbounded` (or mapping every name to one component)
    /// fails the logs statement's list.
    /// FLIP: a conversion that drops a figure, or fills `estimate` from the
    /// segment count alone, fails the equalities against the report.
    #[tokio::test]
    async fn sql_explain_carries_exact_estimates_and_names_the_unbounded() {
        let (running, backend, service) = sql_backend_over(&[1, 3, 2]).await;

        let request = sql_request("SELECT value FROM samples");
        let report = service
            .sql_explain(expected_tenant(), &request)
            .await
            .expect("the service explains");
        let explained = backend
            .sql_explain(expected_tenant(), &request)
            .await
            .expect("the adapter explains");
        assert_eq!(explained.estimate, report.estimate);
        assert_eq!(explained.estimate.segments, 3);
        assert_eq!(explained.segments_resolved, 3);
        assert_eq!(explained.segments_resolved, report.segments_resolved as u64);
        assert_eq!(explained.segments_admitted, report.segments_admitted);
        assert_eq!(
            explained.segments_recent_exempt,
            report.segments_recent_exempt
        );
        assert_eq!(explained.target, ravel_sql::TargetSignal::Metrics);
        assert_eq!(explained.plan_text, report.plan_text);
        assert!(explained.unbounded.is_empty());
        assert_eq!(
            explained
                .columns
                .iter()
                .map(|column| (column.name.as_str(), column.data_type.as_str()))
                .collect::<Vec<_>>(),
            [("value", "Float64")]
        );

        let logs = backend
            .sql_explain(expected_tenant(), &sql_request("SELECT * FROM logs"))
            .await
            .expect("the adapter explains a logs statement");
        assert_eq!(logs.target, ravel_sql::TargetSignal::Logs);
        assert_eq!(logs.unbounded, [EstimateComponent::DecompressedBytes]);

        running.shutdown().await.expect("server stops");
    }

    /// One `tools/call` through the real client and route, returning the
    /// result's error flag and its envelope.
    async fn call(
        client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
        name: &'static str,
        arguments: serde_json::Value,
    ) -> (Option<bool>, serde_json::Value) {
        let serde_json::Value::Object(arguments) = arguments else {
            panic!("tool arguments are an object");
        };
        let result = client
            .call_tool(CallToolRequestParams::new(name).with_arguments(arguments))
            .await
            .unwrap_or_else(|err| panic!("{name} answers: {err}"));
        let envelope = result
            .structured_content
            .clone()
            .expect("the tool result carries the envelope as structured content");
        (result.is_error, envelope)
    }

    fn sql_window() -> serde_json::Value {
        serde_json::json!({"start_ns": "0", "end_ns": SQL_NOW_NS.to_string()})
    }

    /// `ravel_find_labels` and `ravel_explain_query` through `POST /mcp` on
    /// the shipping start path, over a MemoryStore tenant holding two
    /// segments of metric `m`.
    ///
    /// FLIP: leaving either name off `SERVED_TOOLS`, or its dispatch arm on
    /// `NotShipped`, makes its call a protocol error and fails `call`.
    /// FLIP: an explain that ignored the caller's lowered
    /// `max_store_requests` answers `ok` for the quartered budget.
    /// FLIP: passing `include_log_metric_names: false` for a bare `__name__`
    /// drops the two log metric names from the list.
    /// FLIP: leaving the server's metadata envelope `presentation.max_rows`
    /// (200) on the find_labels answer, or setting the effective value on
    /// the explain answer, reports a row cap neither tool applies.
    #[tokio::test]
    async fn find_labels_and_explain_query_answer_through_the_route() {
        let (store, metrics) = instrumented_memory();
        for (writer_seq, hour) in [(1, 0), (2, 1)] {
            publish_sample(store.as_ref(), hour, writer_seq).await;
        }
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, names) = call(
            &client,
            "ravel_find_labels",
            serde_json::json!({"label_name": "__name__", "time_range": sql_window()}),
        )
        .await;
        assert_eq!(is_error, Some(false), "{names}");
        assert_eq!(names["status"], serde_json::json!("ok"), "{names}");
        // A bare `__name__` includes the log metric names, as the HTTP
        // label-values endpoint does for the same request.
        assert_eq!(
            names["data"]["rows"],
            serde_json::json!([["m"], ["ravel_log_bytes"], ["ravel_log_lines"]])
        );
        assert_eq!(
            names["presentation"]["max_rows"],
            serde_json::json!(0),
            "{names}"
        );
        assert!(
            names["warnings"]
                .as_array()
                .expect("warnings")
                .iter()
                .any(|w| w
                    .as_str()
                    .is_some_and(|w| w.starts_with("metric names (the values of __name__)"))),
            "{names}"
        );

        let (_, filtered) = call(
            &client,
            "ravel_find_labels",
            serde_json::json!({
                "label_name": "__name__",
                "filter": "M",
                "time_range": sql_window(),
            }),
        )
        .await;
        assert_eq!(
            filtered["data"]["rows"],
            serde_json::json!([]),
            "{filtered}"
        );
        assert_eq!(
            filtered["scope"]["predicates_applied"],
            serde_json::json!(["label=__name__", "filter contains \"M\" (case-sensitive)"])
        );

        let (is_error, unbounded) = call(
            &client,
            "ravel_find_labels",
            serde_json::json!({"filter": "m", "time_range": sql_window()}),
        )
        .await;
        assert_eq!(is_error, Some(true));
        assert_eq!(
            unbounded["failure"]["class"],
            serde_json::json!("missing_argument")
        );

        let (is_error, explained) = call(
            &client,
            "ravel_explain_query",
            serde_json::json!({
                "query": "SELECT value FROM samples",
                "time_range": sql_window(),
            }),
        )
        .await;
        assert_eq!(is_error, Some(false), "{explained}");
        assert_eq!(explained["status"], serde_json::json!("ok"), "{explained}");
        assert_eq!(
            explained["data"]["columns"],
            serde_json::json!([{"name": "value", "type": "Float64"}])
        );
        assert_eq!(explained["data"]["rows"], serde_json::json!([]));
        assert_eq!(
            explained["presentation"]["max_rows"],
            serde_json::json!(0),
            "{explained}"
        );
        assert!(
            explained["plan"]
                .as_str()
                .is_some_and(|plan| !plan.is_empty())
        );
        assert_eq!(
            explained["budget"]["estimate"]["segments_resolved"],
            serde_json::json!(2)
        );
        assert_eq!(
            explained["budget"]["estimate_is_upper_envelope"],
            serde_json::json!(true)
        );

        // Lowering `max_store_requests` to a quarter of the estimate the
        // same statement just reported refuses it with that ratio. The
        // schema does not declare the field; the adapter reads every budget
        // knob from the raw arguments.
        let requests = explained["budget"]["estimate"]["store_requests"]
            .as_u64()
            .expect("a metrics estimate bounds its requests");
        let ceiling = requests / 4;
        assert!(ceiling > 0, "{explained}");
        let (is_error, over) = call(
            &client,
            "ravel_explain_query",
            serde_json::json!({
                "query": "SELECT value FROM samples",
                "time_range": sql_window(),
                "max_store_requests": ceiling,
            }),
        )
        .await;
        assert_eq!(is_error, Some(true), "{over}");
        assert_eq!(
            over["failure"]["class"],
            serde_json::json!("budget_estimate_exceeds_ceiling")
        );
        assert_eq!(
            over["budget"]["estimate"]["narrowing_factor"],
            serde_json::json!(requests as f64 / ceiling as f64)
        );

        let (_, logs) = call(
            &client,
            "ravel_explain_query",
            serde_json::json!({"query": "SELECT * FROM logs", "time_range": sql_window()}),
        )
        .await;
        assert_eq!(logs["status"], serde_json::json!("ok"), "{logs}");
        assert_eq!(
            logs["budget"]["estimate"]["unbounded"],
            serde_json::json!(["estimated_decompressed_bytes"])
        );
        assert_eq!(
            logs["budget"]["estimate_is_upper_envelope"],
            serde_json::json!(false)
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// Write the test tenant's metric metadata record with one gauge family
    /// per name.
    async fn write_families(store: &dyn ObjectStoreBackend, names: &[String]) {
        let entries: Vec<_> = names
            .iter()
            .map(|name| ravel_catalog::MetricMetadataEntry {
                family_name: name.clone(),
                kind: ravel_catalog::MetricKind::Gauge,
                help: format!("help for {name}"),
                unit: String::new(),
                updated_unix_ns: 1,
            })
            .collect();
        ravel_catalog::write_metrics_meta(store, &expected_tenant(), &entries, None)
            .await
            .expect("metadata record written");
    }

    /// Write a pending (no `.done`) windowless erasure request on `signal`
    /// for the test tenant.
    async fn request_erasure(
        store: &dyn ObjectStoreBackend,
        signal: ravel_types::Signal,
        request_id: u128,
    ) {
        use ravel_commit::{erasure, keys, signal as commit_signal};
        use ravel_object_store::{PutMode, PutOptions};
        use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};

        let request_id = uuid::Uuid::from_u128(request_id);
        let request = ErasureRequest {
            format_version: 1,
            tenant_hash: expected_tenant().0.to_vec(),
            signal: commit_signal::to_proto(signal) as i32,
            request_id: request_id.to_string(),
            created_unix_ns: 1,
            predicate: vec![ErasurePredicateMatcher {
                key: "user".to_string(),
                value: "u1".to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        };
        let key = keys::erasure_request_key(&expected_tenant(), signal, request_id)
            .expect("erasure request key");
        store
            .put(
                &key,
                erasure::encode_request(&request),
                PutOptions {
                    mode: PutMode::CreateIfAbsent,
                    checksum: None,
                },
            )
            .await
            .expect("erasure request written");
    }

    /// The `family` rows' names, in the order the page carries them.
    fn family_names(page: &serde_json::Value) -> Vec<String> {
        page["data"]["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .filter(|row| row[0] == serde_json::json!("family"))
            .map(|row| row[1].as_str().expect("a family name").to_string())
            .collect()
    }

    fn cursor_of(page: &serde_json::Value) -> Option<String> {
        page["presentation"]["cursor"].as_str().map(str::to_string)
    }

    /// `ravel_describe_data` through `POST /mcp` on the shipping start path,
    /// over a MemoryStore tenant with 250 metric families: three pages of
    /// 100, 100, and 50, each later page redeeming the cursor the one before
    /// it returned.
    ///
    /// FLIP: paging by offset, or resuming at `>=` rather than `>` the last
    /// name, repeats or skips a family and the concatenation differs from
    /// the sorted list.
    /// FLIP: a case-insensitive or locale-aware collation interleaves the
    /// `Fam_` and `fam_` names, which byte order keeps apart.
    /// FLIP: minting on a complete last page leaves a cursor on page three.
    /// FLIP: re-measuring the watermark on a later page reports hour 7, the
    /// segment committed between page one and page two, which a fresh page
    /// one does report.
    #[tokio::test]
    async fn describe_data_pages_families_by_keyset_through_the_route() {
        let (store, metrics) = instrumented_memory();
        publish_sample(store.as_ref(), 1, 1).await;
        let mut names: Vec<String> = (0..249)
            .map(|index| {
                if index % 2 == 0 {
                    format!("fam_{index:03}")
                } else {
                    format!("Fam_{index:03}")
                }
            })
            .collect();
        names.push("\u{e9}tat_total".to_string());
        write_families(store.as_ref(), &names).await;
        names.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(names[0], "Fam_001");
        assert_eq!(names[123], "Fam_247");
        assert_eq!(names[124], "fam_000");
        assert_eq!(names[249], "\u{e9}tat_total");

        let running = start_server(Arc::clone(&store), metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, one) = call(
            &client,
            "ravel_describe_data",
            serde_json::json!({"signal": "metrics"}),
        )
        .await;
        assert_eq!(is_error, Some(false), "{one}");
        assert_eq!(one["status"], serde_json::json!("ok_page"), "{one}");
        assert_eq!(family_names(&one), names[..100]);
        assert_eq!(one["data"]["rows"][0][0], serde_json::json!("table"));
        assert_eq!(
            one["data"]["rows"][0][2]["families_total"],
            serde_json::json!(250)
        );
        assert_eq!(
            one["visibility"]["ingest_watermark_hour"],
            serde_json::json!("1")
        );
        assert_eq!(one["visibility"]["pinned"], serde_json::json!(false));

        publish_sample(store.as_ref(), 7, 2).await;

        let (is_error, two) = call(
            &client,
            "ravel_describe_data",
            serde_json::json!({"signal": "metrics", "cursor": cursor_of(&one).expect("page one cursor")}),
        )
        .await;
        assert_eq!(is_error, Some(false), "{two}");
        assert_eq!(two["status"], serde_json::json!("ok_page"), "{two}");
        assert_eq!(family_names(&two), names[100..200]);
        assert_eq!(two["data"]["rows"].as_array().map(Vec::len), Some(100));
        assert_eq!(
            two["visibility"]["ingest_watermark_hour"],
            serde_json::json!("1")
        );
        assert_eq!(two["visibility"]["pinned"], serde_json::json!(true));
        assert_eq!(
            two["visibility"]["snapshot_id"],
            one["visibility"]["snapshot_id"]
        );

        let (is_error, three) = call(
            &client,
            "ravel_describe_data",
            serde_json::json!({"signal": "metrics", "cursor": cursor_of(&two).expect("page two cursor")}),
        )
        .await;
        assert_eq!(is_error, Some(false), "{three}");
        assert_eq!(three["status"], serde_json::json!("ok"), "{three}");
        assert_eq!(family_names(&three), names[200..]);
        assert_eq!(cursor_of(&three), None);
        assert_eq!(
            three["visibility"]["ingest_watermark_hour"],
            serde_json::json!("1")
        );
        assert_eq!(
            three["visibility"]["snapshot_id"],
            one["visibility"]["snapshot_id"]
        );

        let mut served = family_names(&one);
        served.extend(family_names(&two));
        served.extend(family_names(&three));
        assert_eq!(served, names);

        let (_, fresh) = call(
            &client,
            "ravel_describe_data",
            serde_json::json!({"signal": "metrics"}),
        )
        .await;
        assert_eq!(
            fresh["visibility"]["ingest_watermark_hour"],
            serde_json::json!("7"),
            "{fresh}"
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// A metrics cursor through the route survives a pending erasure on
    /// logs and is expired by one on metrics, with no rows served.
    ///
    /// FLIP: confirming against every signal's erasure expires the cursor
    /// after the logs request; confirming against none, or serving the rows
    /// before `confirm`, answers page two after the metrics request.
    #[tokio::test]
    async fn a_metrics_cursor_survives_a_logs_erasure_and_expires_on_a_metrics_one() {
        let (store, metrics) = instrumented_memory();
        publish_sample(store.as_ref(), 1, 1).await;
        let names: Vec<String> = (0..150).map(|index| format!("m_{index:03}")).collect();
        write_families(store.as_ref(), &names).await;
        let running = start_server(Arc::clone(&store), metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (_, one) = call(
            &client,
            "ravel_describe_data",
            serde_json::json!({"signal": "metrics"}),
        )
        .await;
        assert_eq!(one["status"], serde_json::json!("ok_page"), "{one}");
        let cursor = cursor_of(&one).expect("page one cursor");

        request_erasure(store.as_ref(), ravel_types::Signal::Logs, 0x3641).await;
        let (is_error, two) = call(
            &client,
            "ravel_describe_data",
            serde_json::json!({"signal": "metrics", "cursor": cursor}),
        )
        .await;
        assert_eq!(is_error, Some(false), "{two}");
        assert_eq!(family_names(&two), names[100..]);

        request_erasure(store.as_ref(), ravel_types::Signal::Metrics, 0x3642).await;
        let (is_error, expired) = call(
            &client,
            "ravel_describe_data",
            serde_json::json!({"signal": "metrics", "cursor": cursor}),
        )
        .await;
        assert_eq!(is_error, Some(true), "{expired}");
        assert_eq!(
            expired["failure"]["class"],
            serde_json::json!("cursor_expired")
        );
        assert_eq!(expired["data"]["rows"], serde_json::json!([]));

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    const NS_PER_SEC: i64 = 1_000_000_000;

    fn wall_now_ns() -> i64 {
        let since_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is past the epoch");
        i64::try_from(since_epoch.as_nanos()).expect("the epoch fits an i64")
    }

    /// Declare `count` string columns as the test tenant's typed attribute
    /// columns, through the same config write the operator path makes.
    async fn declare_columns(store: &dyn ObjectStoreBackend, count: usize) {
        let declared = (0..count)
            .map(|index| ravel_catalog::DeclaredTypedColumn {
                key: format!("attr_{index:02}"),
                ty: ravel_catalog::DeclaredColumnType::Str,
            })
            .collect();
        let config = ravel_catalog::TenantConfig {
            typed_attr_columns: Some(declared),
            ..ravel_catalog::TenantConfig::new(ravel_catalog::TenantLifecycleState::Active)
        };
        ravel_catalog::set_tenant_config(store, &expected_tenant(), &config, wall_now_ns())
            .await
            .expect("tenant config written");
    }

    /// A `ravel_describe_data` call context at `now_ns` under the
    /// deployment's default protection horizon, with the response byte cap
    /// at its floor.
    fn describe_context(now_ns: i64, key: &ravel_mcp::cursor::CursorKey) -> ToolContext<'_> {
        let mut budgets = sql_budgets();
        budgets.max_response_bytes = ravel_mcp::envelope::MAX_RESPONSE_BYTES_FLOOR;
        ToolContext {
            tenant_hash: expected_tenant(),
            now_ns,
            deadline_ns: now_ns + 60 * NS_PER_SEC,
            protection_horizon_ns: now_ns
                + ravel_maintain::GcConfigValues::maintain_defaults().protection_horizon_ns,
            budgets,
            cursor_key: key,
        }
    }

    /// A started server over `store`, and the backend the describe tool body
    /// calls.
    async fn describe_backend(
        store: &Arc<dyn ObjectStoreBackend>,
        metrics: Arc<ravel_object_store::StoreMetrics>,
        config: ServerConfig,
    ) -> (ravel_server::Running, ServiceBackend) {
        let running = start_with(config, Arc::clone(store), metrics).await;
        let service = running
            .query_service
            .clone()
            .expect("a query-serving mode builds one");
        (running, ServiceBackend::new(service, sql_budgets()))
    }

    /// The `column` rows' names, in the order the page carries them.
    fn column_names(page: &ravel_mcp::envelope::Envelope) -> Vec<String> {
        use ravel_mcp::envelope::Cell;
        page.data
            .rows
            .iter()
            .filter_map(|row| match (row.first(), row.get(1)) {
                (Some(Cell::Str(kind)), Some(Cell::Str(name))) if kind == "column" => {
                    Some(name.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Fixed columns [`Padded`] appends to a logs outcome.
    const PAD_COLUMNS: usize = 2_500;

    /// The real backend, with [`PAD_COLUMNS`] wide fixed columns appended to
    /// every logs describe outcome so a logs page one is cut by the response
    /// byte floor. Without them it never is: the cursor pins the whole
    /// declared set and is bounded at 4 KiB, while the floor is 256 KiB.
    /// Fixed columns are not pinned, and the request reaches the real service
    /// unchanged, so the declared-column lookup and both clocks are the
    /// service's own.
    struct Padded<'a>(&'a ServiceBackend);

    impl QueryBackend for Padded<'_> {
        async fn promql_instant(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &InstantRequest,
        ) -> Result<ravel_mcp::envelope::Envelope, ravel_mcp::envelope::Failure> {
            self.0.promql_instant(tenant_hash, request).await
        }

        async fn promql_range(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_query::http::RangeRequest,
        ) -> Result<ravel_mcp::envelope::Envelope, ravel_mcp::envelope::Failure> {
            self.0.promql_range(tenant_hash, request).await
        }

        async fn labels(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_query::http::MetadataRequest,
        ) -> Result<ravel_mcp::envelope::Envelope, ravel_mcp::envelope::Failure> {
            self.0.labels(tenant_hash, request).await
        }

        async fn label_values(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_query::http::MetadataRequest,
            name: &str,
            include_log_metric_names: bool,
        ) -> Result<ravel_mcp::envelope::Envelope, ravel_mcp::envelope::Failure> {
            self.0
                .label_values(tenant_hash, request, name, include_log_metric_names)
                .await
        }

        async fn series(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_query::http::MetadataRequest,
        ) -> Result<ravel_mcp::envelope::Envelope, ravel_mcp::envelope::Failure> {
            self.0.series(tenant_hash, request).await
        }

        async fn analytics(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_mcp::envelope::AnyJson,
        ) -> Result<ravel_mcp::envelope::Envelope, ravel_mcp::envelope::Failure> {
            self.0.analytics(tenant_hash, request).await
        }

        async fn exemplars(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_mcp::envelope::AnyJson,
        ) -> Result<ravel_mcp::envelope::Envelope, ravel_mcp::envelope::Failure> {
            self.0.exemplars(tenant_hash, request).await
        }

        async fn sql_execute(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_sql::SqlRequest,
        ) -> Result<Draft, ravel_mcp::envelope::Failure> {
            self.0.sql_execute(tenant_hash, request).await
        }

        async fn trace_lookup(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_mcp::service::TraceLookupRequest,
        ) -> Result<ravel_mcp::envelope::Envelope, ravel_mcp::envelope::Failure> {
            self.0.trace_lookup(tenant_hash, request).await
        }

        async fn sql_explain(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_sql::SqlRequest,
        ) -> Result<ravel_mcp::draft::ExplainOutcome, ravel_mcp::envelope::Failure> {
            self.0.sql_explain(tenant_hash, request).await
        }

        async fn describe_data(
            &self,
            tenant_hash: ravel_types::TenantHash,
            request: &ravel_mcp::service::DescribeRequest,
        ) -> Result<ravel_mcp::service::DescribeOutcome, ravel_mcp::envelope::Failure> {
            let mut outcome = self.0.describe_data(tenant_hash, request).await?;
            if request.signal == ravel_types::Signal::Logs {
                outcome.fixed_columns.extend((0..PAD_COLUMNS).map(|index| {
                    ravel_mcp::service::DescribedColumn {
                        name: format!("pad_{index:04}_{}", "x".repeat(100)),
                        data_type: "Utf8".to_string(),
                        nullable: true,
                    }
                }));
            }
            Ok(outcome)
        }
    }

    /// A logs cursor minted at T0 and redeemed at T1, 120 s later on the
    /// caller's clock, after a typed column was declared between the pages
    /// through the real config write: the declared-column lookup on page two
    /// runs at T1, past its 60 s freshness horizon, sees the new column, and
    /// the cursor expires with no rows served.
    ///
    /// FLIP: handing the backdated resolve instant (T0) to the declared-column
    /// lookup finds the entry page one stamped at T0 still fresh, keeps the
    /// old set, and serves page two against a declaration that has moved.
    #[tokio::test]
    async fn a_logs_cursor_expires_on_a_column_declared_between_pages_past_the_horizon() {
        let (store, metrics) = instrumented_memory();
        declare_columns(store.as_ref(), 3).await;
        let (running, service) = describe_backend(&store, metrics, server_config(mcp_on())).await;
        let backend = Padded(&service);
        let key = ravel_mcp::cursor::CursorKey::from_process_secret(
            [0x41; ravel_mcp::cursor::CURSOR_KEY_LEN],
        );
        let t1 = wall_now_ns();
        let t0 = t1 - 120 * NS_PER_SEC;

        let one = describe_data::run(
            serde_json::json!({"signal": "logs"}),
            &describe_context(t0, &key),
            &backend,
        )
        .await;
        assert_eq!(one.status, Status::OkPage, "{:?}", one.failure);
        let declared_on_page_one: Vec<String> = column_names(&one)
            .into_iter()
            .filter(|name| name.starts_with("attr_"))
            .collect();
        assert_eq!(declared_on_page_one, ["attr_00", "attr_01", "attr_02"]);
        let cursor = one.presentation.cursor.clone().expect("page one cursor");

        declare_columns(store.as_ref(), 4).await;

        let two = describe_data::run(
            serde_json::json!({"signal": "logs", "cursor": cursor}),
            &describe_context(t1, &key),
            &backend,
        )
        .await;
        assert_eq!(two.status, Status::Error);
        assert_eq!(
            two.failure.as_ref().map(|failure| failure.class),
            Some(FailureClass::CursorExpired),
            "{:?}",
            two.failure
        );
        assert!(two.data.rows.is_empty());

        running.shutdown().await.expect("server stops");
    }

    /// Every `kind=query` record on the test tenant's query-audit shard.
    async fn query_audit_records(store: &dyn ObjectStoreBackend) -> Vec<ravel_logseg::LogRecord> {
        use ravel_commit::{keys, record};
        use ravel_logseg::{Predicate, RlogConfig, RlogReader};
        use ravel_object_store::{GetRange, list_all};

        let prefix = keys::commit_shard_prefix(
            &expected_tenant(),
            ravel_types::Signal::Audit,
            ravel_maintain::QUERY_AUDIT_SHARD,
        )
        .expect("audit commit prefix");
        let config = RlogConfig::default();
        let mut out = Vec::new();
        for meta in list_all(store, &prefix).await.expect("list audit commits") {
            let commit = store
                .get(&meta.key, GetRange::Full)
                .await
                .expect("get commit");
            let commit = record::decode(&commit.data).expect("decode commit");
            let data_key = keys::reconstruct_data_key(&commit).expect("data key");
            let object = store
                .get(&data_key, GetRange::Full)
                .await
                .expect("get data");
            let reader = RlogReader::new(object.data.as_ref(), &config).expect("rlog reader");
            let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
            out.extend(
                rows.into_iter()
                    .filter(|row| audit_attr(row, "kind") == Some("query")),
            );
        }
        out
    }

    fn audit_attr<'a>(row: &'a ravel_logseg::LogRecord, key: &str) -> Option<&'a str> {
        row.attrs
            .iter()
            .find(|(name, _)| name == key)
            .and_then(|(_, value)| match value {
                ravel_logseg::AttrValue::Str(text) => Some(text.as_str()),
                _ => None,
            })
    }

    /// A later metrics page redeemed at T1 writes its audit record at T1, the
    /// caller's clock, both as the record's timestamp and as its window end,
    /// while the cursor it redeemed was minted at T0.
    ///
    /// FLIP: stamping the audit record with the backdated resolve instant
    /// writes page two at T0, and both records carry T0.
    #[tokio::test]
    async fn a_later_describe_page_is_audited_at_the_callers_clock() {
        let (store, metrics) = instrumented_memory();
        let names: Vec<String> = (0..150).map(|index| format!("m_{index:03}")).collect();
        write_families(store.as_ref(), &names).await;
        let mut config = server_config(mcp_on());
        config.audit_pipeline = Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        });
        let (running, backend) = describe_backend(&store, metrics, config).await;
        let key = ravel_mcp::cursor::CursorKey::from_process_secret(
            [0x42; ravel_mcp::cursor::CURSOR_KEY_LEN],
        );
        let t1 = wall_now_ns();
        let t0 = t1 - 120 * NS_PER_SEC;

        let one = describe_data::run(
            serde_json::json!({"signal": "metrics"}),
            &describe_context(t0, &key),
            &backend,
        )
        .await;
        assert_eq!(one.status, Status::OkPage, "{:?}", one.failure);
        let two = describe_data::run(
            serde_json::json!({
                "signal": "metrics",
                "cursor": one.presentation.cursor.clone().expect("page one cursor"),
            }),
            &describe_context(t1, &key),
            &backend,
        )
        .await;
        assert_eq!(two.status, Status::Ok, "{:?}", two.failure);
        assert_eq!(two.visibility.snapshot_id, one.visibility.snapshot_id);

        let mut stamps: Vec<(i64, Option<String>)> = query_audit_records(store.as_ref())
            .await
            .iter()
            .filter(|row| audit_attr(row, "query.language") == Some("describe_data"))
            .map(|row| {
                (
                    row.ts_ns,
                    audit_attr(row, "query.window_end_ns").map(str::to_string),
                )
            })
            .collect();
        stamps.sort_unstable();
        assert_eq!(
            stamps,
            vec![(t0, Some(t0.to_string())), (t1, Some(t1.to_string()))]
        );

        running.shutdown().await.expect("server stops");
    }
}
