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
        audit_pipeline: ravel_maintain::AuditPipelineConfig::default(),
        audit_text: ravel_maintain::AuditTextPolicy::default(),
        query_budgets: QueryBudgets {
            mcp,
            ..Default::default()
        },
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
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
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
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

    use ravel_mcp::budget::{McpBudgetConfig, McpRequestBudgets};
    use ravel_mcp::envelope::FailureClass;
    use ravel_mcp::service::QueryBackend;
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
    /// ships (`ravel_capabilities`) reaches no backend at all: it takes no
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
        // to completion: it reached the two further requests the dropped one
        // never issued, and only a completed plan carries an estimate.
        let succeeded: Vec<_> = outcomes
            .iter()
            .filter(|row| row.status == QueryOutcomeStatus::Success)
            .collect();
        assert_eq!(succeeded.len(), 1);
        assert_eq!(
            succeeded[0].counters,
            ravel_server::metrics::QueryCostCounters {
                queries: 1,
                s3_requests: 5,
                s3_bytes: 0,
                cache_hits: 0,
                cache_misses: 1,
                decompressed_bytes: 0,
                estimated_requests: 4,
                estimated_store_bytes: 0,
                estimated_decompressed_bytes: 0,
            }
        );

        running.shutdown().await.expect("server stops");
    }
}
