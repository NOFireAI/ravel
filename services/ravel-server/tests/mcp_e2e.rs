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
        flush_object_backstop_multiple: 4,
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
        ingest_hold: Default::default(),
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
    use ravel_mcp::envelope::{CapAdvice, FailureClass, Status};
    use ravel_mcp::service::QueryBackend;
    use ravel_mcp::tools::{ToolContext, describe_data, query_sql};
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
        publish_samples(
            store,
            ingest_hour,
            writer_seq,
            &[(1 + i64::from(ingest_hour), 1.0)],
        )
        .await;
    }

    /// [`publish_sample`] with one segment holding every `(ts_ns, value)` of
    /// `samples`, all of metric `m`'s one series.
    async fn publish_samples(
        store: &dyn ObjectStoreBackend,
        ingest_hour: u32,
        writer_seq: u64,
        samples: &[(i64, f64)],
    ) {
        publish_series(store, ingest_hour, writer_seq, &[(None, samples)]).await;
    }

    /// One series for [`publish_series`]: its label `k`, if any, and its
    /// `(ts_ns, value)` samples.
    type SeriesEntry<'a> = (Option<&'a str>, &'a [(i64, f64)]);

    /// [`publish_samples`] with one series of metric `m` per entry, the entry
    /// carrying label `k` set to its value when one is given.
    async fn publish_series(
        store: &dyn ObjectStoreBackend,
        ingest_hour: u32,
        writer_seq: u64,
        entries: &[SeriesEntry<'_>],
    ) {
        use ravel_commit::publish::RetryPolicy;
        use ravel_commit::record::NewCommitRecord;
        use ravel_commit::{keys, publish, record};
        use ravel_object_store::PutOptions;
        use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
        use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal};

        let tenant = TenantId::new(TENANT);
        let tenant_hash = tenant.hash();
        let series = entries
            .iter()
            .map(|&(k, samples)| {
                let mut labels = vec![Label {
                    name: "__name__".to_string(),
                    value: "m".to_string(),
                }];
                if let Some(k) = k {
                    labels.push(Label {
                        name: "k".to_string(),
                        value: k.to_string(),
                    });
                }
                let labels = LabelSet::new(labels).expect("valid labels");
                SeriesInput {
                    series_id: SeriesId::compute(&tenant, "m", &labels).expect("series id"),
                    labels,
                    samples: samples
                        .iter()
                        .map(|&(ts_ns, value)| Sample { ts_ns, value })
                        .collect(),
                }
            })
            .collect();
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
    /// Driven through the backend rather than the transport because the
    /// mapping under test is the backend's own `sql_execute`, which is the
    /// call that tool body makes; the tool itself is driven through the
    /// transport further down.
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
        draft.finish_page(
            &sql_budgets(),
            &scope,
            &SQL_ADVICE,
            PageOrder::Partial,
            |_, _, _| panic!("a partial order never mints"),
        )
    }

    /// The wording `ravel_query_sql` gives the cap step rule.
    const SQL_ADVICE: CapAdvice<'static> = CapAdvice {
        fewer_rows: "shorten time_range or add WHERE predicates",
        smaller_rows_action: "narrow_the_projection",
        smaller_rows: "select fewer or narrower columns",
        read_cell: Some("select fewer or narrower columns to give the cut cells more room"),
    };

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
                SQL_NOW_NS,
                &[],
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
                SQL_NOW_NS,
                &[],
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
    /// `resolve_at_ns` the call passed breaks the `resolve_at_ns` equality;
    /// `a_later_sql_page_is_audited_at_the_callers_clock` covers one that
    /// takes the request's `now_ns` instead.
    #[tokio::test]
    async fn sql_execute_carries_the_outcomes_pin_inputs() {
        let (running, backend, service) = sql_backend_over(&[1, 3, 2]).await;
        let request = sql_request("SELECT value FROM samples");

        let outcome = service
            .sql_execute(expected_tenant(), &request)
            .await
            .expect("the service executes");
        let draft = backend
            .sql_execute(expected_tenant(), &request, SQL_NOW_NS, &[])
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
            .sql_execute(expected_tenant(), &request, SQL_NOW_NS, &[])
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
                .sql_execute(expected_tenant(), &request, SQL_NOW_NS, &[])
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
                SQL_NOW_NS,
                &[],
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
        let page = draft.finish_page(
            &budgets,
            &scope,
            &SQL_ADVICE,
            PageOrder::Total,
            |_, _, _| panic!("a statement naming no table must not reach the mint"),
        );

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

    /// `ravel_query_sql` and `ravel_explain_query` report the same
    /// `estimate_is_upper_envelope` for one statement, through the route: true
    /// over `samples`, whose estimate is bounded, and false over `logs`,
    /// `spans`, `alerts` and `audit`, whose decompressed-byte figure the
    /// estimator cannot bound. The executed call reports that figure as
    /// unknown, `null` and named in `unbounded`, in the spelling the explain
    /// uses, not as its structural 0. A Parquet table is not reachable here
    /// (the server runs with no `parquet_profiles`); ravel-sql's
    /// `explain_and_execute_name_the_same_unbounded_components` pins that arm.
    ///
    /// FLIP: `estimate_is_upper_envelope: true` hard-coded on the SQL execute
    /// path fails the logs statement's flag; rendering the raw figure fails
    /// its `null`.
    #[tokio::test]
    async fn query_sql_and_explain_agree_on_the_upper_envelope() {
        let (store, metrics) = instrumented_memory();
        publish_sample(store.as_ref(), 0, 1).await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        for (query, upper_envelope, unbounded) in [
            ("SELECT value FROM samples", true, serde_json::json!([])),
            (
                "SELECT * FROM logs",
                false,
                serde_json::json!(["estimated_decompressed_bytes"]),
            ),
            (
                "SELECT * FROM spans",
                false,
                serde_json::json!(["estimated_decompressed_bytes"]),
            ),
            (
                "SELECT * FROM alerts",
                false,
                serde_json::json!(["estimated_decompressed_bytes"]),
            ),
            (
                "SELECT * FROM audit",
                false,
                serde_json::json!(["estimated_decompressed_bytes"]),
            ),
        ] {
            let arguments = serde_json::json!({"query": query, "time_range": sql_window()});
            let (is_error, executed) = call(&client, "ravel_query_sql", arguments.clone()).await;
            assert_eq!(is_error, Some(false), "{query}: {executed}");
            let (is_error, explained) = call(&client, "ravel_explain_query", arguments).await;
            assert_eq!(is_error, Some(false), "{query}: {explained}");

            let executed_budget = &executed["budget"];
            assert_eq!(
                executed_budget["estimate_is_upper_envelope"],
                serde_json::json!(upper_envelope),
                "{query}: {executed}"
            );
            assert_eq!(
                executed_budget["estimate_is_upper_envelope"],
                explained["budget"]["estimate_is_upper_envelope"],
                "{query}"
            );
            assert_eq!(
                executed_budget["estimate"]["unbounded"], unbounded,
                "{query}: {executed}"
            );
            assert_eq!(
                executed_budget["estimate"]["unbounded"],
                explained["budget"]["estimate"]["unbounded"],
                "{query}"
            );
            if upper_envelope {
                assert!(
                    executed_budget["estimate"]["decompressed_bytes"].is_u64(),
                    "{query}: {executed}"
                );
            } else {
                assert_eq!(
                    executed_budget["estimate"]["decompressed_bytes"],
                    serde_json::Value::Null,
                    "{query}: {executed}"
                );
            }
        }

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
    /// over a metrics tenant whose metadata GET fails every time: the call is
    /// an `unavailable` failure saying the metadata read failed, not an
    /// `ok_page` with zero families, and the second call, which the metadata
    /// cache serves from the failed read without a second GET, fails the
    /// same way.
    ///
    /// FLIP: today's code answers both calls with `ok` and `families_total`
    /// 0. FLIP: a check on the miss path only (the flag the miss returns, not
    /// the cached entry) answers the second call with `ok`.
    #[tokio::test]
    async fn describe_data_fails_unavailable_on_a_failed_metadata_read() {
        use ravel_object_store::fault::{FaultKind, Rule, ScriptedFault};

        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Transient("injected".into()))
                .with_key_contains("/m/meta")
                .with_occurrence(Occurrence::Always),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        publish_sample(fault.inner(), 1, 1).await;
        write_families(fault.inner(), &["up".to_string()]).await;
        let metrics = Arc::new(ravel_object_store::StoreMetrics::default());
        let running = start_server(
            Arc::clone(&fault) as Arc<dyn ObjectStoreBackend>,
            metrics,
            mcp_on(),
        )
        .await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");
        assert_eq!(fault.fault_count(Op::Get, FaultKind::Transient), 0);

        for (call_index, expected_faults) in [(1, 1), (2, 1)] {
            let (is_error, envelope) = call(
                &client,
                "ravel_describe_data",
                serde_json::json!({"signal": "metrics"}),
            )
            .await;
            assert_eq!(is_error, Some(true), "call {call_index}: {envelope}");
            assert_eq!(envelope["status"], serde_json::json!("error"), "{envelope}");
            assert_eq!(
                envelope["failure"]["class"],
                serde_json::json!("unavailable"),
                "call {call_index}: {envelope}"
            );
            let message = envelope["failure"]["message"]
                .as_str()
                .expect("the failure carries a message");
            assert!(
                message.contains("metadata read failed"),
                "call {call_index}: {message:?}"
            );
            assert_eq!(
                fault.fault_count(Op::Get, FaultKind::Transient),
                expected_faults,
                "call {call_index}: the metadata fault fired once, on the miss"
            );
        }

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// `ravel_describe_data` through `POST /mcp` over a metrics tenant with
    /// no metadata record answers `ok` with no families and says why in
    /// `warnings`.
    ///
    /// FLIP: treating an absent record as a failed read answers an
    /// `unavailable` failure. FLIP: dropping the no-record reason leaves
    /// `warnings` without the `families:` line.
    #[tokio::test]
    async fn describe_data_answers_an_absent_metadata_record_with_its_reason() {
        let (store, metrics) = instrumented_memory();
        publish_sample(store.as_ref(), 1, 1).await;
        let running = start_server(Arc::clone(&store), metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, page) = call(
            &client,
            "ravel_describe_data",
            serde_json::json!({"signal": "metrics"}),
        )
        .await;
        assert_eq!(is_error, Some(false), "{page}");
        assert_eq!(page["status"], serde_json::json!("ok"), "{page}");
        assert!(family_names(&page).is_empty(), "{page}");
        assert_eq!(
            page["data"]["rows"][0][2]["families_total"],
            serde_json::json!(0),
            "{page}"
        );
        assert!(
            page["warnings"]
                .as_array()
                .expect("warnings")
                .iter()
                .any(|w| w.as_str() == Some("families: the tenant has no metric metadata record")),
            "{page}"
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
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
            resolve_at_ns: i64,
            order_columns: &[String],
        ) -> Result<Draft, ravel_mcp::envelope::Failure> {
            self.0
                .sql_execute(tenant_hash, request, resolve_at_ns, order_columns)
                .await
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

    /// A later SQL page, resolved two hours before the call, reads the
    /// snapshot of its resolve instant (one of the two segments; the other
    /// was flushed in the call's own ingest hour) and writes its audit record
    /// at the call's clock. The draft carries the resolve instant.
    ///
    /// FLIP: `sql_execute_resolved_at` stamping the audit record with
    /// `resolve_at_ns` writes it two hours early.
    /// FLIP: `ServiceBackend::sql_execute` calling `sql_execute` (resolving
    /// at the request's `now_ns`) reads both segments.
    /// FLIP: `ServiceBackend::sql_execute` building the draft at
    /// `request.now_ns` breaks the `resolve_at_ns` equality.
    #[tokio::test]
    async fn a_later_sql_page_is_audited_at_the_callers_clock() {
        const NS_PER_HOUR: i64 = 3_600_000_000_000;
        let caller = wall_now_ns();
        let resolve_at = caller - 2 * NS_PER_HOUR;
        let caller_hour = u32::try_from(caller / NS_PER_HOUR).expect("the hour fits a u32");
        let (store, metrics) = instrumented_memory();
        publish_sample(store.as_ref(), caller_hour - 3, 1).await;
        publish_sample(store.as_ref(), caller_hour, 2).await;
        let mut config = server_config(mcp_on());
        config.audit_pipeline = Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        });
        let (running, backend) = describe_backend(&store, metrics, config).await;
        let request = ravel_sql::SqlRequest {
            window: ravel_types::TimeRange {
                start_ns: 0,
                end_ns: caller,
            },
            now_ns: caller,
            ..sql_request("SELECT value FROM samples")
        };

        let draft = backend
            .sql_execute(expected_tenant(), &request, resolve_at, &[])
            .await
            .expect("the adapter executes");

        assert_eq!(draft.resolve_at_ns, resolve_at);
        assert_eq!(draft.envelope.data.row_count, 1);
        let stamps: Vec<i64> = query_audit_records(store.as_ref())
            .await
            .iter()
            .filter(|row| audit_attr(row, "query.language") == Some("sql"))
            .map(|row| row.ts_ns)
            .collect();
        assert_eq!(stamps, vec![caller]);

        running.shutdown().await.expect("server stops");
    }

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    /// A `samples` statement under a total order, which `ravel_query_sql`
    /// pages by keyset.
    const SAMPLES_BY_TS: &str = "SELECT * FROM samples ORDER BY ts";

    /// The `value` cell of every row on `page`, in page order.
    fn sql_values(page: &serde_json::Value) -> Vec<f64> {
        let index = page["data"]["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .position(|column| column["name"] == serde_json::json!("value"))
            .expect("the page projects value");
        page["data"]["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| {
                let cell = &row[index];
                cell.as_f64()
                    .or_else(|| cell.as_str().and_then(|text| text.parse().ok()))
                    .unwrap_or_else(|| panic!("a float value cell: {cell}"))
            })
            .collect()
    }

    /// `ravel_query_sql` through `POST /mcp` on the shipping start path walks
    /// a total order three pages deep by keyset, over two segments whose rows
    /// interleave: two rows, two rows, then the last row with no cursor, every
    /// page under one snapshot id and the order the tool appended its
    /// tiebreak to.
    ///
    /// FLIP: leaving `ravel_query_sql` off `SERVED_TOOLS`, or its dispatch
    /// arm on `NotShipped`, makes the first call a protocol error.
    /// FLIP: a cursor that resumed at or before the page's last row repeats
    /// a row on the next page.
    /// FLIP: minting a cursor on a page with no row past `max_rows` leaves
    /// page three `ok_page` with a cursor to an empty page.
    #[tokio::test]
    async fn query_sql_walks_a_total_order_three_pages_by_keyset() {
        let (store, metrics) = instrumented_memory();
        publish_samples(store.as_ref(), 0, 1, &[(10, 1.0), (30, 3.0), (50, 5.0)]).await;
        publish_samples(store.as_ref(), 1, 2, &[(20, 2.0), (40, 4.0)]).await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let mut cursor: Option<String> = None;
        let mut pages = Vec::new();
        for _ in 0..3 {
            let mut arguments = serde_json::json!({
                "query": SAMPLES_BY_TS,
                "time_range": sql_window(),
                "max_rows": 2,
            });
            if let Some(token) = &cursor {
                arguments["cursor"] = serde_json::json!(token);
            }
            let (is_error, page) = call(&client, "ravel_query_sql", arguments).await;
            assert_eq!(is_error, Some(false), "{page}");
            cursor = cursor_of(&page);
            pages.push(page);
        }

        let statuses: Vec<&serde_json::Value> = pages.iter().map(|page| &page["status"]).collect();
        assert_eq!(
            statuses,
            [
                &serde_json::json!("ok_page"),
                &serde_json::json!("ok_page"),
                &serde_json::json!("ok"),
            ]
        );
        let values: Vec<Vec<f64>> = pages.iter().map(sql_values).collect();
        assert_eq!(values, vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0]]);
        assert_eq!(cursor, None, "{}", pages[2]);
        for page in &pages {
            assert_eq!(
                page["scope"]["order_by"],
                serde_json::json!(["\"ts\" ASC", "\"series_id\" ASC"]),
                "{page}"
            );
            assert_eq!(
                page["visibility"]["snapshot_id"],
                pages[0]["visibility"]["snapshot_id"]
            );
        }

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// `time_range` filters rows, not only the segments the resolve lists: a
    /// segment whose two samples straddle the window's end overlaps the
    /// window, and contributes only the sample inside it.
    ///
    /// FLIP: `row_window: false` in `ravel_query_sql`'s request bounds the
    /// listing only, and the page carries the 2.0 row, whose timestamp is past
    /// the window's end.
    #[tokio::test]
    async fn query_sql_excludes_a_row_outside_time_range_from_an_overlapping_segment() {
        let (store, metrics) = instrumented_memory();
        publish_samples(store.as_ref(), 0, 1, &[(10, 1.0), (100, 2.0)]).await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, page) = call(
            &client,
            "ravel_query_sql",
            serde_json::json!({
                "query": SAMPLES_BY_TS,
                "time_range": {"start_ns": "0", "end_ns": "50"},
            }),
        )
        .await;

        assert_eq!(is_error, Some(false), "{page}");
        assert_eq!(page["status"], serde_json::json!("ok"), "{page}");
        assert_eq!(sql_values(&page), vec![1.0]);
        assert_eq!(page["data"]["row_count"], serde_json::json!(1), "{page}");

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// A `ravel_query_sql` call context at `now_ns` with a two-row page and a
    /// 30 h protection horizon, so a cursor outlives the two hours between
    /// the pages of the test below.
    fn query_sql_context(now_ns: i64, key: &ravel_mcp::cursor::CursorKey) -> ToolContext<'_> {
        let mut budgets = sql_budgets();
        budgets.max_rows = 2;
        ToolContext {
            tenant_hash: expected_tenant(),
            now_ns,
            deadline_ns: now_ns + 60 * NS_PER_SEC,
            protection_horizon_ns: now_ns + 30 * NS_PER_HOUR,
            budgets,
            cursor_key: key,
        }
    }

    /// A later `ravel_query_sql` page resolves at the instant its cursor was
    /// minted and writes its audit record at the caller's clock. Page one, at
    /// T0, two hours before the call, sees one segment of three rows; a
    /// segment flushed in the caller's own ingest hour is not on page two,
    /// which ends with the third row under page one's snapshot id, and the two
    /// pages' audit records carry T0 and T1.
    ///
    /// FLIP: `later_page` resolving at `ctx.now_ns` reads the newer segment,
    /// and page two carries 4.0 as well.
    /// FLIP: stamping page two's audit record with the resolve instant writes
    /// both records at T0.
    #[tokio::test]
    async fn a_later_query_sql_page_resolves_at_the_mint_instant_and_audits_at_the_callers_clock() {
        let t1 = wall_now_ns();
        let t0 = t1 - 2 * NS_PER_HOUR;
        let caller_hour = u32::try_from(t1 / NS_PER_HOUR).expect("the hour fits a u32");
        let (store, metrics) = instrumented_memory();
        publish_samples(
            store.as_ref(),
            caller_hour - 3,
            1,
            &[(10, 1.0), (20, 2.0), (30, 3.0)],
        )
        .await;
        publish_samples(store.as_ref(), caller_hour, 2, &[(40, 4.0)]).await;
        let mut config = server_config(mcp_on());
        config.audit_pipeline = Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        });
        let (running, backend) = describe_backend(&store, metrics, config).await;
        let key = ravel_mcp::cursor::CursorKey::from_process_secret(
            [0x42; ravel_mcp::cursor::CURSOR_KEY_LEN],
        );
        let window = serde_json::json!({"start_ns": "0", "end_ns": "1000"});

        let one = query_sql::run(
            serde_json::json!({"query": SAMPLES_BY_TS, "time_range": window}),
            &query_sql_context(t0, &key),
            &backend,
        )
        .await;
        assert_eq!(one.status, Status::OkPage, "{:?}", one.failure);
        let two = query_sql::run(
            serde_json::json!({
                "query": SAMPLES_BY_TS,
                "time_range": window,
                "cursor": one.presentation.cursor.clone().expect("page one cursor"),
            }),
            &query_sql_context(t1, &key),
            &backend,
        )
        .await;
        assert_eq!(two.status, Status::Ok, "{:?}", two.failure);

        let values = |page: &ravel_mcp::envelope::Envelope| {
            sql_values(&serde_json::to_value(page).expect("the envelope serializes"))
        };
        assert_eq!(values(&one), vec![1.0, 2.0]);
        assert_eq!(values(&two), vec![3.0]);
        assert_eq!(two.visibility.snapshot_id, one.visibility.snapshot_id);
        let mut stamps: Vec<i64> = query_audit_records(store.as_ref())
            .await
            .iter()
            .filter(|row| audit_attr(row, "query.language") == Some("sql"))
            .map(|row| row.ts_ns)
            .collect();
        stamps.sort_unstable();
        assert_eq!(stamps, vec![t0, t1]);

        running.shutdown().await.expect("server stops");
    }

    /// The lookback `ravel_query_promql` reports in its scope is the one the
    /// engine applies.
    ///
    /// FLIP: a tool constant of 60 s while the engine keeps its 5 m default
    /// fails here, and the scope line would describe a lookback no query ran
    /// with.
    #[test]
    fn query_promql_reports_the_engines_lookback() {
        assert_eq!(
            ravel_mcp::tools::query_promql::LOOKBACK_NS,
            ravel_promql::DEFAULT_LOOKBACK_NS
        );
    }

    /// `ravel_query_promql` through `POST /mcp` in both modes over one series
    /// of metric `m` holding 1.5 at 60 s, -0.0 at 90 s and NaN at 120 s. The
    /// instant call at 100 s answers -0.0 stamped with the evaluation time;
    /// the range call over [60 s, 150 s) with a 30 s step answers the three
    /// samples at their own steps, in ns strings, and reports the alignment.
    ///
    /// FLIP: leaving `ravel_query_promql` off `SERVED_TOOLS`, or its dispatch
    /// arm on `NotShipped`, makes the first call a protocol error.
    /// FLIP: handing the engine `end_ns` itself as the closed range end adds
    /// a fourth step at 150 s carrying the NaN again.
    /// FLIP: writing a sample value with `serde_json::json!(f64)` instead of
    /// the cell's serializer turns the NaN into `null`.
    /// FLIP: the port finishing its own envelope before the tool's caps adds
    /// each identity warning twice.
    #[tokio::test]
    async fn query_promql_answers_both_modes_with_exact_samples() {
        let (store, metrics) = instrumented_memory();
        publish_samples(
            store.as_ref(),
            0,
            1,
            &[
                (60 * NS_PER_SEC, 1.5),
                (90 * NS_PER_SEC, -0.0),
                (120 * NS_PER_SEC, f64::NAN),
            ],
        )
        .await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, instant) = call(
            &client,
            "ravel_query_promql",
            serde_json::json!({
                "query": "m",
                "evaluation_time": (100 * NS_PER_SEC).to_string(),
            }),
        )
        .await;
        assert_eq!(is_error, Some(false), "{instant}");
        assert_eq!(instant["status"], serde_json::json!("ok"), "{instant}");
        assert_eq!(
            instant["data"]["rows"].to_string(),
            r#"[[{"__name__":"m"},{"ts_ns":["100000000000"],"value":[-0.0]}]]"#
        );
        assert_eq!(
            instant["scope"]["time_range"],
            serde_json::json!({"start_ns": "100000000000", "end_ns": "100000000000"})
        );
        assert_eq!(
            instant["scope"]["predicates_applied"]
                .as_array()
                .expect("predicates")[..2],
            [
                serde_json::json!("m"),
                serde_json::json!("instant evaluation at 100000000000 ns"),
            ]
        );

        let (is_error, range) = call(
            &client,
            "ravel_query_promql",
            serde_json::json!({
                "query": "m",
                "time_range": {
                    "start_ns": (60 * NS_PER_SEC).to_string(),
                    "end_ns": (150 * NS_PER_SEC).to_string(),
                },
                "step": "30s",
            }),
        )
        .await;
        assert_eq!(is_error, Some(false), "{range}");
        assert_eq!(range["status"], serde_json::json!("ok"), "{range}");
        assert_eq!(
            range["data"]["rows"].to_string(),
            r#"[[{"__name__":"m"},{"ts_ns":["60000000000","90000000000","120000000000"],"value":[1.5,-0.0,"NaN"]}]]"#
        );
        assert_eq!(
            range["scope"]["predicates_applied"],
            serde_json::json!([
                "m",
                "steps aligned to time_range.start_ns: 60000000000 + k * 30000000000 ns, every \
                 step before 150000000000 ns, the last at 120000000000 ns",
                "lookback 300000000000 ns (5m): an instant selector at step t reads the latest \
                 sample in (t - 5m, t]",
            ])
        );

        for envelope in [&instant, &range] {
            let warnings = envelope["warnings"].as_array().expect("warnings");
            let distinct: std::collections::HashSet<&serde_json::Value> = warnings.iter().collect();
            assert_eq!(distinct.len(), warnings.len(), "{envelope}");
        }

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// The step limit `ravel_query_promql` refuses past is the engine's.
    ///
    /// FLIP: a tool copy of 10,000 or 12,000 while the engine keeps 11,000
    /// fails here.
    #[test]
    fn query_promql_step_limit_is_the_engines() {
        assert_eq!(
            ravel_mcp::tools::query_promql::MAX_RANGE_POINTS,
            ravel_promql::DEFAULT_MAX_RANGE_POINTS
        );
    }

    /// Through `POST /mcp` over one series of metric `m` sampled every 10 s
    /// in [0 s, 1100 s): a range of 11,001 steps of 100 ms is
    /// `invalid_argument` with a next step, and the engine's own limit is
    /// never reached; a range of exactly 11,000 steps is evaluated, and its
    /// one series, over the 256 KiB floor cap at about 34 B a sample, comes
    /// back as an object of parallel arrays of one length, a prefix of the
    /// series, with `samples_omitted` and one truncated cell. No series is
    /// missing, so the result is `ok` with no cursor, and its one next step
    /// is `read_full_cells`.
    ///
    /// FLIP: dropping the tool's step check lets the engine refuse the first
    /// call as `unsupported`, naming the HTTP route's `step_ms`, with no next
    /// step; a tool copy of the limit above the engine's does the same.
    /// FLIP: leaving the samples cell to `Envelope::fit` answers it as a
    /// string, and `samples["ts_ns"]` is null.
    #[tokio::test]
    async fn query_promql_bounds_a_long_series_by_steps_and_bytes() {
        let (store, metrics) = instrumented_memory();
        let samples: Vec<(i64, f64)> = (0..110)
            .map(|k| (k * 10 * NS_PER_SEC, k as f64 + 0.123_456_789_012_345))
            .collect();
        publish_samples(store.as_ref(), 0, 1, &samples).await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");
        let range = |end_ms: i64| {
            serde_json::json!({
                "query": "m",
                "time_range": {"start_ns": "0", "end_ns": (end_ms * 1_000_000).to_string()},
                "step": "100ms",
                "max_response_bytes": 1,
            })
        };

        let (is_error, over) = call(&client, "ravel_query_promql", range(1_100_100)).await;
        assert_eq!(is_error, Some(true), "{over}");
        assert_eq!(
            over["failure"]["class"],
            serde_json::json!("invalid_argument"),
            "{over}"
        );
        assert_eq!(
            over["next_steps"][0]["detail"],
            serde_json::json!(
                "widen step to at least 101ms, or narrow time_range, so the range evaluates \
                 at most 11000 steps"
            )
        );

        let (is_error, cut) = call(&client, "ravel_query_promql", range(1_100_000)).await;
        assert_eq!(is_error, Some(false), "{}", cut["failure"]);
        assert_eq!(cut["status"], serde_json::json!("ok"));
        assert_eq!(cut["presentation"]["cursor"], serde_json::Value::Null);
        assert_eq!(cut["presentation"]["cells_truncated"], serde_json::json!(1));
        assert_eq!(cut["presentation"]["rows_omitted"], serde_json::json!(0));
        let rows = cut["data"]["rows"].as_array().expect("rows");
        assert_eq!(rows.len(), 1);
        let ts = rows[0][1]["ts_ns"].as_array().expect("ts_ns is an array");
        let values = rows[0][1]["value"].as_array().expect("value is an array");
        assert_eq!(ts.len(), values.len());
        assert!(ts.len() > 1_000 && ts.len() < 11_000, "{}", ts.len());
        for (k, (ts_ns, value)) in ts.iter().zip(values).enumerate() {
            let step_ns = k as i64 * 100_000_000;
            assert_eq!(ts_ns, &serde_json::json!(step_ns.to_string()));
            let sample = samples[(step_ns / (10 * NS_PER_SEC)) as usize].1;
            assert_eq!(value, &serde_json::json!(sample));
        }
        assert_eq!(
            rows[0][1]["samples_omitted"],
            serde_json::json!((11_000 - ts.len()).to_string())
        );
        assert_eq!(
            cut["next_steps"],
            serde_json::json!([{
                "action": "read_full_cells",
                "detail": "the result is complete but the byte cap shortened cells; raise \
                           max_response_bytes (up to 4 MiB) to read them in full, or narrow \
                           time_range to a window after the last sample returned to read the \
                           samples the cut omitted",
            }])
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// `max_rows` counts series: three series of three steps each under
    /// `max_rows: 2` answer two whole series, `ok_bounded`, one series
    /// omitted, and no cursor.
    ///
    /// FLIP: capping the port's per-sample rows before grouping them into
    /// series answers two samples of one series, `ok`.
    #[tokio::test]
    async fn query_promql_max_rows_caps_series_and_is_ok_bounded() {
        let (store, metrics) = instrumented_memory();
        let samples = [
            (60 * NS_PER_SEC, 1.0),
            (90 * NS_PER_SEC, 2.0),
            (120 * NS_PER_SEC, 3.0),
        ];
        publish_series(
            store.as_ref(),
            0,
            1,
            &[
                (Some("a"), &samples[..]),
                (Some("b"), &samples[..]),
                (Some("c"), &samples[..]),
            ],
        )
        .await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, bounded) = call(
            &client,
            "ravel_query_promql",
            serde_json::json!({
                "query": "m",
                "time_range": {
                    "start_ns": (60 * NS_PER_SEC).to_string(),
                    "end_ns": (150 * NS_PER_SEC).to_string(),
                },
                "step": "30s",
                "max_rows": 2,
            }),
        )
        .await;

        assert_eq!(is_error, Some(false), "{bounded}");
        assert_eq!(
            bounded["status"],
            serde_json::json!("ok_bounded"),
            "{bounded}"
        );
        let rows = bounded["data"]["rows"].as_array().expect("rows");
        assert_eq!(rows.len(), 2, "{bounded}");
        for row in rows {
            assert_eq!(
                row[1],
                serde_json::json!({
                    "ts_ns": ["60000000000", "90000000000", "120000000000"],
                    "value": [1.0, 2.0, 3.0],
                }),
                "{bounded}"
            );
        }
        assert_eq!(bounded["data"]["row_count"], serde_json::json!(3));
        assert_eq!(
            bounded["presentation"]["rows_omitted"],
            serde_json::json!(1)
        );
        assert!(bounded["presentation"]["cursor"].is_null(), "{bounded}");

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// Publish `lines` as one RLOG object of service `api` plus its
    /// `Signal::Logs` commit record in ingest hour 0, the way log ingest does.
    async fn publish_log_lines(store: &dyn ObjectStoreBackend, lines: &[(i64, &str)]) {
        let records: Vec<TestLog> = lines
            .iter()
            .map(|&(ts_ns, body)| TestLog {
                ts_ns,
                severity_num: 9,
                body,
                trace_id: None,
                attrs: Vec::new(),
            })
            .collect();
        publish_logs(store, &records).await;
    }

    /// One log record [`publish_logs`] writes, observed at its `ts_ns`.
    struct TestLog<'a> {
        ts_ns: i64,
        severity_num: u8,
        body: &'a str,
        trace_id: Option<[u8; 16]>,
        attrs: Vec<(&'a str, &'a str)>,
    }

    /// Publish `records` as one RLOG object of service `api` plus its
    /// `Signal::Logs` commit record in ingest hour 0, the way log ingest does.
    async fn publish_logs(store: &dyn ObjectStoreBackend, records: &[TestLog<'_>]) {
        use ravel_commit::publish::RetryPolicy;
        use ravel_commit::record::NewCommitRecord;
        use ravel_commit::{keys, publish, record};
        use ravel_logseg::writer::ObjectIdentity;
        use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
        use ravel_object_store::PutOptions;
        use ravel_types::Signal;
        use ravel_types::logstream::log_stream_id;

        let lines: Vec<(i64, &str)> = records.iter().map(|log| (log.ts_ns, log.body)).collect();
        let tenant_hash = TenantId::new(TENANT).hash();
        let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
        let writer_id = uuid::Uuid::from_u128(3_000);
        let mut writer = RlogWriter::new(
            RlogConfig::default(),
            ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.into_bytes(),
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for log in records {
            writer
                .push(LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: log.ts_ns,
                    observed_ts_ns: log.ts_ns,
                    severity_num: log.severity_num,
                    severity_text: "INFO".into(),
                    body: log.body.into(),
                    trace_id: log.trace_id,
                    span_id: None,
                    flags: 0,
                    attrs: log
                        .attrs
                        .iter()
                        .map(|&(key, value)| (key.to_string(), AttrValue::Str(value.into())))
                        .collect(),
                })
                .expect("push log record");
        }
        let bytes = writer.finish().expect("finish rlog object");
        let min_ts = lines.iter().map(|&(ts, _)| ts).min().expect("a line");
        let max_ts = lines.iter().map(|&(ts, _)| ts).max().expect("a line");
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: lines.len() as u64,
            series_count: 1,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: min_ts,
            max_ingest_ts_ns: max_ts,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid log commit record");
        store
            .put(
                &keys::reconstruct_data_key(&rec).expect("data key"),
                bytes::Bytes::from(bytes),
                PutOptions::default(),
            )
            .await
            .expect("put log data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish log commit");
    }

    /// The two log pseudo-metrics through `ravel_query_promql` over a logs
    /// tenant: three lines of 5, 8 and 5 bytes at 10, 20 and 30 s count 3
    /// lines and 18 bytes at 60 s.
    ///
    /// FLIP: dropping the rows of a query that names a log metric, the result
    /// a path reading the metrics signal alone gives, answers `[]` for both.
    #[tokio::test]
    async fn query_promql_counts_log_lines_and_bytes() {
        let (store, metrics) = instrumented_memory();
        publish_log_lines(
            store.as_ref(),
            &[
                (10 * NS_PER_SEC, "alpha"),
                (20 * NS_PER_SEC, "beta two"),
                (30 * NS_PER_SEC, "gamma"),
            ],
        )
        .await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        for (query, total) in [
            ("sum(count_over_time(ravel_log_lines[1h]))", 3.0),
            ("sum(sum_over_time(ravel_log_bytes[1h]))", 18.0),
        ] {
            let (is_error, counted) = call(
                &client,
                "ravel_query_promql",
                serde_json::json!({
                    "query": query,
                    "evaluation_time": (60 * NS_PER_SEC).to_string(),
                }),
            )
            .await;
            assert_eq!(is_error, Some(false), "{counted}");
            assert_eq!(counted["status"], serde_json::json!("ok"), "{counted}");
            assert_eq!(
                counted["data"]["rows"],
                serde_json::json!([[{}, {"ts_ns": ["60000000000"], "value": [total]}]]),
                "{query}"
            );
        }

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// `ravel_query_promql`'s estimate covers the metrics lane only. Over a
    /// metrics selector it is an upper envelope with nothing unbounded; over
    /// `ravel_log_lines` the query also spends through the log lane, so every
    /// component is `null` and named in `unbounded`, and the estimate is not
    /// called an upper envelope.
    ///
    /// FLIP: `estimate_excludes_spend` left false on the log lane in
    /// `prefetch` fails the log query's assertions.
    #[tokio::test]
    async fn query_promql_names_every_component_unbounded_when_it_reads_the_log_lane() {
        let (store, metrics) = instrumented_memory();
        publish_samples(store.as_ref(), 0, 1, &[(30 * NS_PER_SEC, 1.5)]).await;
        publish_log_lines(
            store.as_ref(),
            &[(10 * NS_PER_SEC, "alpha"), (20 * NS_PER_SEC, "beta two")],
        )
        .await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let promql = |query: &str| {
            serde_json::json!({
                "query": query,
                "evaluation_time": (60 * NS_PER_SEC).to_string(),
            })
        };

        let (is_error, metric) = call(&client, "ravel_query_promql", promql("m")).await;
        assert_eq!(is_error, Some(false), "{metric}");
        assert_eq!(metric["status"], serde_json::json!("ok"), "{metric}");
        let estimate = &metric["budget"]["estimate"];
        assert_eq!(estimate["unbounded"], serde_json::json!([]), "{metric}");
        for field in ["store_requests", "bytes_scanned", "decompressed_bytes"] {
            assert!(estimate[field].is_u64(), "{field}: {metric}");
        }
        assert_eq!(
            metric["budget"]["estimate_is_upper_envelope"],
            serde_json::json!(true),
            "{metric}"
        );

        let (is_error, logs) = call(
            &client,
            "ravel_query_promql",
            promql("sum(count_over_time(ravel_log_lines[1h]))"),
        )
        .await;
        assert_eq!(is_error, Some(false), "{logs}");
        assert_eq!(logs["status"], serde_json::json!("ok"), "{logs}");
        assert_eq!(
            logs["data"]["rows"],
            serde_json::json!([[{}, {"ts_ns": ["60000000000"], "value": [2.0]}]]),
            "{logs}"
        );
        let estimate = &logs["budget"]["estimate"];
        assert_eq!(
            estimate["unbounded"],
            serde_json::json!([
                "estimated_requests",
                "estimated_store_bytes",
                "estimated_decompressed_bytes",
            ]),
            "{logs}"
        );
        for field in ["store_requests", "bytes_scanned", "decompressed_bytes"] {
            assert!(estimate[field].is_null(), "{field}: {logs}");
        }
        assert_eq!(
            logs["budget"]["estimate_is_upper_envelope"],
            serde_json::json!(false),
            "{logs}"
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// A `COUNT` over `logs` through `ravel_query_sql` counts a line a retry
    /// stored twice as two rows, and `accuracy.lower_bound_count` stays
    /// `false`: the field is reserved, and a count that can include
    /// duplicates is not a lower bound.
    ///
    /// FLIP: a SQL body that set `lower_bound_count` on a count over logs
    /// fails the accuracy assertion.
    #[tokio::test]
    async fn query_sql_count_over_logs_leaves_lower_bound_count_unset() {
        let (store, metrics) = instrumented_memory();
        publish_log_lines(
            store.as_ref(),
            &[(10 * NS_PER_SEC, "alpha"), (10 * NS_PER_SEC, "alpha")],
        )
        .await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, counted) = call(
            &client,
            "ravel_query_sql",
            serde_json::json!({
                "query": "SELECT count(*) AS n FROM logs",
                "time_range": sql_window(),
            }),
        )
        .await;

        assert_eq!(is_error, Some(false), "{counted}");
        assert_eq!(counted["status"], serde_json::json!("ok"), "{counted}");
        assert_eq!(
            counted["data"]["rows"],
            serde_json::json!([["2"]]),
            "{counted}"
        );
        assert_eq!(
            counted["accuracy"]["lower_bound_count"],
            serde_json::json!(false),
            "{counted}"
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    const TRACE_A: [u8; 16] = [0xab; 16];
    const HOSTILE: &str = "x' OR 1=1; -- /*";

    /// Four log lines 10 s apart: severities info, error, warn and fatal,
    /// trace A on the first and last, and `attr_00` (declared) set to `web`
    /// on all but the second, which carries [`HOSTILE`].
    fn search_corpus() -> Vec<TestLog<'static>> {
        vec![
            TestLog {
                ts_ns: 10 * NS_PER_SEC,
                severity_num: 9,
                body: "user login ok",
                trace_id: Some(TRACE_A),
                attrs: vec![("attr_00", "web"), ("region", "eu")],
            },
            TestLog {
                ts_ns: 20 * NS_PER_SEC,
                severity_num: 17,
                body: "disk timeout on node",
                trace_id: Some([0xcd; 16]),
                attrs: vec![("attr_00", HOSTILE), ("region", "eu")],
            },
            TestLog {
                ts_ns: 30 * NS_PER_SEC,
                severity_num: 13,
                body: "time drift",
                trace_id: None,
                attrs: vec![("attr_00", "web")],
            },
            TestLog {
                ts_ns: 40 * NS_PER_SEC,
                severity_num: 21,
                body: "it's down; -- /* now",
                trace_id: Some(TRACE_A),
                attrs: vec![("attr_00", "web"), ("region", "us")],
            },
        ]
    }

    /// The `body` column of an envelope's rows, in page order.
    fn bodies(envelope: &serde_json::Value) -> Vec<String> {
        let at = envelope["data"]["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .position(|column| column["name"] == "body")
            .expect("a body column");
        envelope["data"]["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| row[at].as_str().expect("a body string").to_string())
            .collect()
    }

    /// `ravel_search_logs` through `POST /mcp` over a MemoryStore logs
    /// tenant with `attr_00` declared: `has_word`, `severity`, `trace_id`
    /// and a predicate each select exactly the rows they name, a predicate
    /// value and a `has_word` term carrying a quote, a semicolon and comment
    /// markers match literally, and a two-row cap walks the four rows in two
    /// pages, every row once, in `(ts, observed_ts)` order.
    ///
    /// FLIP: compiling `severity` to `=` instead of `>=` drops the fatal
    /// line; `format!` of the raw predicate value refuses the statement as
    /// several; leaving the tool off `SERVED_TOOLS` or dispatch answers a
    /// protocol error.
    #[tokio::test]
    async fn search_logs_filters_and_pages_a_logs_tenant() {
        let (store, metrics) = instrumented_memory();
        // attr_01 is declared and no line carries it, so it is null on every row.
        declare_columns(store.as_ref(), 2).await;
        publish_logs(store.as_ref(), &search_corpus()).await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");
        let window =
            serde_json::json!({"start_ns": "0", "end_ns": (3_600 * NS_PER_SEC).to_string()});
        let search = |extra: serde_json::Value| {
            let mut arguments = serde_json::json!({"time_range": window});
            if let (Some(arguments), serde_json::Value::Object(extra)) =
                (arguments.as_object_mut(), extra)
            {
                arguments.extend(extra);
            }
            arguments
        };
        let trace_a: String = TRACE_A.iter().map(|byte| format!("{byte:02X}")).collect();

        let cases = [
            (
                "has_word",
                serde_json::json!({"has_word": "timeout"}),
                vec!["disk timeout on node"],
            ),
            (
                "has_word token",
                serde_json::json!({"has_word": "time"}),
                vec!["time drift"],
            ),
            (
                "has_word case",
                serde_json::json!({"has_word": "DISK Timeout"}),
                vec!["disk timeout on node"],
            ),
            (
                "hostile has_word",
                serde_json::json!({"has_word": "it's down; -- /* now"}),
                vec!["it's down; -- /* now"],
            ),
            (
                "severity",
                serde_json::json!({"severity": "error"}),
                vec!["disk timeout on node", "it's down; -- /* now"],
            ),
            (
                "trace_id",
                serde_json::json!({"trace_id": trace_a}),
                vec!["user login ok", "it's down; -- /* now"],
            ),
            (
                "hostile predicate",
                serde_json::json!({"predicates": [{"key": "attr_00", "op": "eq", "value": HOSTILE}]}),
                vec!["disk timeout on node"],
            ),
            (
                "ne predicate",
                serde_json::json!({"predicates": [{"key": "attr_00", "op": "ne", "value": HOSTILE}]}),
                vec!["user login ok", "time drift", "it's down; -- /* now"],
            ),
            (
                "ne on a null column",
                serde_json::json!({"predicates": [{"key": "attr_01", "op": "ne", "value": "x"}]}),
                vec![],
            ),
            (
                "exists on a null column",
                serde_json::json!({"predicates": [{"key": "attr_01", "op": "exists"}]}),
                vec![],
            ),
        ];
        for (name, extra, expected) in cases {
            let (is_error, found) = call(&client, "ravel_search_logs", search(extra)).await;
            assert_eq!(is_error, Some(false), "{name}: {found}");
            assert_eq!(found["status"], serde_json::json!("ok"), "{name}: {found}");
            assert_eq!(bodies(&found), expected, "{name}");
        }

        let (_, all) = call(&client, "ravel_search_logs", search(serde_json::json!({}))).await;
        let grouping: Vec<serde_json::Value> = all["data"]["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| serde_json::json!([row[0], row[1]]))
            .collect();
        // Lines one, two and four carry attr_00 and region beside the
        // resource's service.name, line three no region. Only service.name
        // is equal across the first group, so it alone is hoisted there; the
        // second group's one row hoists all it has.
        assert_eq!(
            grouping,
            [
                serde_json::json!(["0", {"service.name": "api"}]),
                serde_json::json!(["0", null]),
                serde_json::json!(["1", {"attr_00": "web", "service.name": "api"}]),
                serde_json::json!(["0", null]),
            ],
            "{all}"
        );
        assert_eq!(
            all["data"]["rows"][1][10],
            serde_json::json!({"attr_00": HOSTILE, "region": "eu"})
        );
        assert_eq!(all["scope"]["table"], serde_json::json!("logs"));

        let (is_error, unknown) = call(
            &client,
            "ravel_search_logs",
            search(serde_json::json!({"predicates": [{"key": "region", "op": "exists"}]})),
        )
        .await;
        assert_eq!(is_error, Some(true), "{unknown}");
        assert_eq!(
            unknown["failure"]["class"],
            serde_json::json!("invalid_argument")
        );
        assert_eq!(
            unknown["next_steps"][0]["action"],
            serde_json::json!("ravel_describe_data")
        );

        let (_, one) = call(
            &client,
            "ravel_search_logs",
            search(serde_json::json!({"max_rows": 2})),
        )
        .await;
        assert_eq!(one["status"], serde_json::json!("ok_page"), "{one}");
        assert_eq!(bodies(&one), ["user login ok", "disk timeout on node"]);
        let cursor = one["presentation"]["cursor"].clone();
        assert!(cursor.is_string(), "{one}");
        let (_, two) = call(
            &client,
            "ravel_search_logs",
            search(serde_json::json!({"max_rows": 2, "cursor": cursor})),
        )
        .await;
        assert_eq!(two["status"], serde_json::json!("ok"), "{two}");
        assert_eq!(bodies(&two), ["time drift", "it's down; -- /* now"]);
        assert_eq!(
            one["visibility"]["snapshot_id"],
            two["visibility"]["snapshot_id"]
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// A tenant that declares attribute columns named `group` and
    /// `group_attrs` gets each of them once beside the tool's own
    /// `__ravel_group` and `__ravel_group_attrs`, and every one of the four
    /// carries its own values: the declared columns the rows' attributes, the
    /// tool's the group number and the hoisted shared attributes.
    ///
    /// FLIP: naming the tool's columns `group` and `group_attrs`, as before,
    /// puts each name in `data.columns` twice.
    #[tokio::test]
    async fn search_logs_group_columns_cannot_collide_with_a_declared_column() {
        let (store, metrics) = instrumented_memory();
        let declared = ["group", "group_attrs"]
            .into_iter()
            .map(|key| ravel_catalog::DeclaredTypedColumn {
                key: key.to_string(),
                ty: ravel_catalog::DeclaredColumnType::Str,
            })
            .collect();
        let config = ravel_catalog::TenantConfig {
            typed_attr_columns: Some(declared),
            ..ravel_catalog::TenantConfig::new(ravel_catalog::TenantLifecycleState::Active)
        };
        ravel_catalog::set_tenant_config(
            store.as_ref(),
            &expected_tenant(),
            &config,
            wall_now_ns(),
        )
        .await
        .expect("tenant config written");
        publish_logs(
            store.as_ref(),
            &[
                TestLog {
                    ts_ns: 10 * NS_PER_SEC,
                    severity_num: 9,
                    body: "one",
                    trace_id: None,
                    attrs: vec![("group", "blue"), ("group_attrs", "x")],
                },
                TestLog {
                    ts_ns: 20 * NS_PER_SEC,
                    severity_num: 9,
                    body: "two",
                    trace_id: None,
                    attrs: vec![("group", "green"), ("group_attrs", "x")],
                },
            ],
        )
        .await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, found) = call(
            &client,
            "ravel_search_logs",
            serde_json::json!({
                "time_range": {"start_ns": "0", "end_ns": (3_600 * NS_PER_SEC).to_string()},
            }),
        )
        .await;
        assert_eq!(is_error, Some(false), "{found}");
        assert_eq!(found["status"], serde_json::json!("ok"), "{found}");

        let names: Vec<&str> = found["data"]["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .map(|column| column["name"].as_str().expect("a column name"))
            .collect();
        let at = |name: &str| {
            let found: Vec<usize> = names
                .iter()
                .enumerate()
                .filter(|(_, column)| **column == name)
                .map(|(index, _)| index)
                .collect();
            let [at] = found.as_slice() else {
                panic!("{name} is a column exactly once: {names:?}");
            };
            *at
        };
        assert_eq!(
            (at("__ravel_group"), at("__ravel_group_attrs")),
            (0, 1),
            "{names:?}"
        );
        let column = |name: &str| -> Vec<serde_json::Value> {
            let at = at(name);
            found["data"]["rows"]
                .as_array()
                .expect("rows")
                .iter()
                .map(|row| row[at].clone())
                .collect()
        };
        assert_eq!(
            column("group"),
            [serde_json::json!("blue"), serde_json::json!("green")]
        );
        assert_eq!(
            column("group_attrs"),
            [serde_json::json!("x"), serde_json::json!("x")]
        );
        assert_eq!(
            column("__ravel_group"),
            [serde_json::json!("0"), serde_json::json!("0")]
        );
        assert_eq!(
            column("__ravel_group_attrs"),
            [
                serde_json::json!({"group_attrs": "x", "service.name": "api"}),
                serde_json::Value::Null,
            ]
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// A started server over a held-gate store holding [`search_corpus`]
    /// with `attr_00` declared, the query service, the store's counters, and
    /// the gate store itself.
    async fn held_logs_server() -> (
        ravel_server::Running,
        ravel_server::service::QueryService,
        Arc<ravel_object_store::StoreMetrics>,
        Arc<FaultStore<ravel_object_store::InstrumentedStore<MemoryStore>>>,
    ) {
        let metrics = Arc::new(ravel_object_store::StoreMetrics::default());
        let faults = Arc::new(FaultStore::new(
            ravel_object_store::InstrumentedStore::with_metrics(
                MemoryStore::new(),
                Arc::clone(&metrics),
            ),
            FaultPlan::empty(),
        ));
        let store: Arc<dyn ObjectStoreBackend> = faults.clone();
        declare_columns(store.as_ref(), 1).await;
        publish_logs(store.as_ref(), &search_corpus()).await;
        let running = start_server(store, Arc::clone(&metrics), mcp_on()).await;
        let service = running
            .query_service
            .clone()
            .expect("a query-serving mode builds one");
        (running, service, metrics, faults)
    }

    /// `ravel_search_logs` arguments with one predicate, so the call reads
    /// the declared columns before its statement.
    fn search_with_predicate() -> serde_json::Value {
        serde_json::json!({
            "time_range": {"start_ns": "0", "end_ns": (3_600 * NS_PER_SEC).to_string()},
            "predicates": [{"key": "attr_00", "op": "eq", "value": "web"}],
        })
    }

    /// A `ravel_search_logs` call with predicates is bounded by one deadline
    /// across its declared-column lookup and its statement. The lookup's
    /// first catalog LIST is held while the paused clock moves 90% of the
    /// deadline on, and the statement's data GET is then held until a timer
    /// fires: the call answers `deadline` once the call's own deadline has
    /// passed, having reached the statement, with the time the lookup took
    /// counted against it.
    ///
    /// FLIP: building the backend without `with_call_start`, which gives the
    /// statement a whole deadline of its own from its own start, answers
    /// `deadline` only at 1.9 deadlines.
    #[tokio::test]
    async fn search_logs_lookup_and_statement_share_one_deadline() {
        let (running, service, _, faults) = held_logs_server().await;
        let budgets = sql_budgets();
        let deadline = budgets.deadline;
        let key = ravel_mcp::cursor::CursorKey::from_process_secret(
            [0x53; ravel_mcp::cursor::CURSOR_KEY_LEN],
        );
        let ctx = describe_context(wall_now_ns(), &key);

        tokio::time::pause();
        let started = tokio::time::Instant::now();
        let backend = ServiceBackend::new(service, budgets).with_call_start(started);
        let lookup = faults.hold(Op::List, None, Occurrence::Nth(1));
        let scan = faults.hold(Op::Get, Some("/l0/".to_string()), Occurrence::Nth(1));

        let call = ravel_mcp::tools::search_logs::run(search_with_predicate(), &ctx, &backend);
        tokio::pin!(call);
        tokio::select! {
            _ = &mut call => panic!("the held lookup cannot complete"),
            () = lookup.wait_until_held(1) => {}
        }
        tokio::time::advance(deadline * 9 / 10).await;
        for id in lookup.held() {
            lookup.release(id);
        }
        let envelope = call.await;
        let elapsed = started.elapsed();

        assert_eq!(envelope.status, Status::Error, "{envelope:?}");
        let failure = envelope.failure.as_ref().expect("an error carries one");
        assert_eq!(failure.class, FailureClass::Deadline, "{failure:?}");
        assert_eq!(scan.held_count(), 1, "the statement reached its data read");
        // hygiene-allow: wall-clock -- `started` is a tokio Instant under
        // tokio::time::pause(), so `elapsed` is the virtual time this test
        // advances itself, not real time.
        assert!(
            elapsed >= deadline && elapsed <= deadline + Duration::from_millis(1),
            "answered after {elapsed:?} under a {deadline:?} deadline: {failure:?}"
        );
        for id in scan.held() {
            scan.release(id);
        }

        tokio::time::resume();
        running.shutdown().await.expect("server stops");
    }

    /// A service call that would start with none of the tool call's deadline
    /// left fails as `deadline` without running: the declared-column lookup
    /// of a call whose whole deadline has passed issues no store request.
    ///
    /// FLIP: dropping the zero check from `ServiceBackend::remaining` runs
    /// both calls under a zero deadline. `tokio::time::timeout` polls its
    /// future once before it reads the timer, and over an in-memory store the
    /// lookup and the statement finish inside that poll, so the call answers
    /// `ok` after reading the store.
    #[tokio::test]
    async fn a_service_call_with_no_time_left_fails_as_deadline_before_the_store() {
        let (running, service, metrics, _) = held_logs_server().await;
        let budgets = sql_budgets();
        let deadline = budgets.deadline;
        let key = ravel_mcp::cursor::CursorKey::from_process_secret(
            [0x54; ravel_mcp::cursor::CURSOR_KEY_LEN],
        );
        let ctx = describe_context(wall_now_ns(), &key);

        tokio::time::pause();
        let started = tokio::time::Instant::now();
        tokio::time::advance(deadline).await;
        let backend = ServiceBackend::new(service, budgets).with_call_start(started);
        let before = total_store_calls(&metrics);
        let envelope =
            ravel_mcp::tools::search_logs::run(search_with_predicate(), &ctx, &backend).await;

        assert_eq!(envelope.status, Status::Error, "{envelope:?}");
        let failure = envelope.failure.as_ref().expect("an error carries one");
        assert_eq!(failure.class, FailureClass::Deadline, "{failure:?}");
        assert_eq!(total_store_calls(&metrics), before);

        tokio::time::resume();
        running.shutdown().await.expect("server stops");
    }

    /// `values` as samples 10 s apart, the first at 10 s.
    fn every_ten_seconds(values: &[f64]) -> Vec<(i64, f64)> {
        values
            .iter()
            .enumerate()
            .map(|(index, &value)| ((index as i64 + 1) * 10 * NS_PER_SEC, value))
            .collect()
    }

    /// `n` values at `before` up to index `change_at` and at `after` from
    /// it, each with a deterministic xorshift noise of at most 0.3, so every
    /// segment has a variance for the detector's cost to read.
    fn step_values(n: usize, before: f64, after: f64, change_at: usize) -> Vec<f64> {
        let mut state: u64 = 0x1234_5678_9abc_def1;
        (0..n)
            .map(|index| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let unit = (state >> 11) as f64 / (1u64 << 53) as f64;
                let level = if index < change_at { before } else { after };
                level + (unit * 2.0 - 1.0) * 0.3
            })
            .collect()
    }

    /// One `ravel_analyze_timeseries` call over `[start_s, end_s)`.
    async fn analyze(
        client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
        query: &str,
        (start_s, end_s): (i64, i64),
        step: &str,
        op: &str,
    ) -> (Option<bool>, serde_json::Value) {
        call(
            client,
            "ravel_analyze_timeseries",
            serde_json::json!({
                "query": query,
                "time_range": {
                    "start_ns": (start_s * NS_PER_SEC).to_string(),
                    "end_ns": (end_s * NS_PER_SEC).to_string(),
                },
                "step": step,
                "op": op,
            }),
        )
        .await
    }

    /// Each row of an analysis envelope as an object keyed by its columns.
    fn analysis_rows(
        envelope: &serde_json::Value,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        let names: Vec<String> = envelope["data"]["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .map(|column| column["name"].as_str().expect("a column name").to_string())
            .collect();
        envelope["data"]["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| {
                names
                    .iter()
                    .cloned()
                    .zip(row.as_array().expect("a row").iter().cloned())
                    .collect()
            })
            .collect()
    }

    /// `change_point` through `POST /mcp` over a series of metric `m` that
    /// steps from 5 to 25 at its 31st sample (310 s), sampled every 10 s and
    /// read at a 10 s step, so step k reads sample k: the change lands at
    /// 310 s, as a `step_change`, over all 60 points. The result is a
    /// heuristic, not exact, and names its method and minimum.
    ///
    /// FLIP: leaving `ravel_analyze_timeseries` off `SERVED_TOOLS` makes the
    /// call a protocol error.
    /// FLIP: an adapter writing the change's timestamp in milliseconds
    /// answers "310000", not "310000000000".
    /// FLIP: an envelope left at its default `exact: true` fails `accuracy`.
    #[tokio::test]
    async fn analyze_timeseries_places_a_change_point_where_it_was_built() {
        let (store, metrics) = instrumented_memory();
        let samples = every_ten_seconds(&step_values(60, 5.0, 25.0, 30));
        publish_series(store.as_ref(), 0, 1, &[(Some("step"), &samples)]).await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, envelope) =
            analyze(&client, r#"m{k="step"}"#, (10, 610), "10s", "change_point").await;
        assert_eq!(is_error, Some(false), "{envelope}");
        assert_eq!(envelope["status"], serde_json::json!("ok"), "{envelope}");
        let rows = analysis_rows(&envelope);
        assert_eq!(rows.len(), 1, "{envelope}");
        let row = &rows[0];
        assert_eq!(
            row["series"],
            serde_json::json!({"__name__": "m", "k": "step"})
        );
        assert_eq!(row["kind"], serde_json::json!("step_change"));
        assert_eq!(row["ts_ns"], serde_json::json!("310000000000"));
        assert!(row["score"].as_f64().expect("a finite score") > 0.0);
        assert_eq!(row["points"], serde_json::json!("60"));
        assert_eq!(row["nan_excluded"], serde_json::json!("0"));
        assert_eq!(row["downsampled"], serde_json::json!(false));
        assert_eq!(row["min_points"], serde_json::json!("22"));
        assert_eq!(row["too_few_points"], serde_json::json!(false));

        assert_eq!(envelope["accuracy"]["exact"], serde_json::json!(false));
        let approximation = envelope["accuracy"]["approximation"]
            .as_str()
            .expect("an approximation");
        assert!(
            approximation.starts_with("heuristic: change_point"),
            "{approximation}"
        );
        assert!(
            approximation.contains("needs 22 non-NaN points"),
            "{approximation}"
        );
        assert!(
            approximation.ends_with("0 of 1 series downsampled"),
            "{approximation}"
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// `summary` through `POST /mcp` over the values 1, 2, 3, 4 at 10, 20,
    /// 30 and 40 s, read at a 10 s step: median 2.5, MAD 1, population
    /// variance 1.25 and stddev its square root, all exact to the bit. A
    /// second series holding -0.0 alone keeps the sign of its median. And
    /// `POST /api/v1/analytics` over the same series and steps, on the same
    /// server, answers the same figures, with one `analytics` audit record
    /// per call: both reach `QueryService::analytics`.
    ///
    /// FLIP: an adapter computing the sample variance (divide by n - 1)
    /// answers 1.666..., not 1.25.
    /// FLIP: writing a figure with `serde_json::json!(f64)` instead of the
    /// cell's serializer turns -0.0 into 0.0's spelling or a NaN into null.
    /// FLIP: an adapter evaluating the range itself rather than through the
    /// service writes no audit record for the tool's call.
    #[tokio::test]
    async fn analyze_timeseries_summary_is_exact_and_matches_the_http_route() {
        let (store, metrics) = instrumented_memory();
        let quad = every_ten_seconds(&[1.0, 2.0, 3.0, 4.0]);
        let negative_zero = [(10 * NS_PER_SEC, -0.0)];
        publish_series(
            store.as_ref(),
            0,
            1,
            &[(Some("quad"), &quad), (Some("zero"), &negative_zero)],
        )
        .await;
        let mut config = server_config(mcp_on());
        config.audit_pipeline = Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        });
        let running = start_with(config, store.clone(), metrics).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, envelope) =
            analyze(&client, r#"m{k="quad"}"#, (10, 50), "10s", "summary").await;
        assert_eq!(is_error, Some(false), "{envelope}");
        assert_eq!(envelope["status"], serde_json::json!("ok"), "{envelope}");
        let rows = analysis_rows(&envelope);
        assert_eq!(rows.len(), 1, "{envelope}");
        let row = &rows[0];
        let figure = |name: &str| row[name].as_f64().expect("a finite figure").to_bits();
        assert_eq!(figure("median"), 2.5f64.to_bits());
        assert_eq!(figure("mad"), 1.0f64.to_bits());
        assert_eq!(figure("variance"), 1.25f64.to_bits());
        assert_eq!(figure("stddev"), 1.25f64.sqrt().to_bits());
        assert_eq!(row["points"], serde_json::json!("4"));
        assert_eq!(row["downsampled"], serde_json::json!(false));
        assert_eq!(row["min_points"], serde_json::json!("1"));
        assert_eq!(row["too_few_points"], serde_json::json!(false));
        assert_eq!(envelope["accuracy"]["exact"], serde_json::json!(false));
        let approximation = envelope["accuracy"]["approximation"]
            .as_str()
            .expect("an approximation");
        assert!(
            approximation.contains("summary never downsamples"),
            "{approximation}"
        );

        let (is_error, zero) = analyze(&client, r#"m{k="zero"}"#, (10, 20), "10s", "summary").await;
        assert_eq!(is_error, Some(false), "{zero}");
        assert_eq!(
            zero["data"]["rows"][0][1].to_string(),
            "-0.0",
            "the median keeps the sign of zero: {zero}"
        );

        let response = reqwest::Client::new()
            .post(format!("http://{}/api/v1/analytics", running.http_addr))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/json")
            .body(
                serde_json::json!({
                    "query": r#"m{k="quad"}"#,
                    "start": 10.0,
                    "end": 40.0,
                    "step": 10.0,
                    "op": {"type": "summary", "percentiles": []},
                })
                .to_string(),
            )
            .send()
            .await
            .expect("the request reaches the server");
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.expect("a JSON body");
        let http = &body["data"]["result"][0]["result"];
        for name in ["median", "mad", "variance", "stddev"] {
            assert_eq!(
                http[name].as_f64().map(f64::to_bits),
                Some(figure(name)),
                "{name}: {body}"
            );
        }

        // The MCP calls' records name the tool in `query.language` and keep
        // the engine's in `query.engine_language`; the HTTP call's does not.
        let records = query_audit_records(store.as_ref()).await;
        let mcp_records = records
            .iter()
            .filter(|row| {
                audit_attr(row, "query.language") == Some("mcp:ravel_analyze_timeseries")
                    && audit_attr(row, "query.engine_language") == Some("analytics")
            })
            .count();
        let http_records = records
            .iter()
            .filter(|row| audit_attr(row, "query.language") == Some("analytics"))
            .count();
        assert_eq!((mcp_records, http_records), (2, 1));

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// A series read at 21 steps is below `change_point`'s minimum of 22: it
    /// comes back as a row of an `ok` result with `too_few_points: true`,
    /// `min_points: 22`, `points: 21`, an `indeterminable` kind and null
    /// location and score. The same series read at 22 steps is analyzed.
    ///
    /// FLIP: refusing the call when a series is short makes the first call
    /// an error envelope.
    /// FLIP: a minimum of 21, or `<=` in the too-few test, flips one of the
    /// two `too_few_points` assertions.
    #[tokio::test]
    async fn analyze_timeseries_reports_too_few_points_per_row() {
        let (store, metrics) = instrumented_memory();
        let samples = every_ten_seconds(&step_values(22, 5.0, 25.0, 11));
        publish_samples(store.as_ref(), 0, 1, &samples).await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, short) = analyze(&client, "m", (10, 220), "10s", "change_point").await;
        assert_eq!(is_error, Some(false), "{short}");
        assert_eq!(short["status"], serde_json::json!("ok"), "{short}");
        let rows = analysis_rows(&short);
        assert_eq!(rows.len(), 1, "{short}");
        let row = &rows[0];
        assert_eq!(row["too_few_points"], serde_json::json!(true));
        assert_eq!(row["min_points"], serde_json::json!("22"));
        assert_eq!(row["points"], serde_json::json!("21"));
        assert_eq!(row["kind"], serde_json::json!("indeterminable"));
        assert_eq!(row["ts_ns"], serde_json::Value::Null);
        assert_eq!(row["score"], serde_json::Value::Null);

        let (is_error, enough) = analyze(&client, "m", (10, 230), "10s", "change_point").await;
        assert_eq!(is_error, Some(false), "{enough}");
        let rows = analysis_rows(&enough);
        assert_eq!(rows[0]["too_few_points"], serde_json::json!(false));
        assert_eq!(rows[0]["points"], serde_json::json!("22"));

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// A series read at 2,500 steps is over `change_point`'s 2,000-point cap:
    /// it is bucket-averaged and its row says `downsampled: true`, as does
    /// the approximation's count. `summary` over the same range reads all
    /// 2,500 points and says `downsampled: false`, because it never
    /// downsamples.
    ///
    /// FLIP: an adapter that does not set `downsample` refuses the
    /// change-point call as too long.
    /// FLIP: a summary row copying the change-point flag says `true`.
    #[tokio::test]
    async fn analyze_timeseries_reports_downsampling_and_summary_never_does() {
        let (store, metrics) = instrumented_memory();
        let samples = every_ten_seconds(&step_values(26, 5.0, 25.0, 13));
        publish_samples(store.as_ref(), 0, 1, &samples).await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let (is_error, changed) = analyze(&client, "m", (10, 260), "100ms", "change_point").await;
        assert_eq!(is_error, Some(false), "{changed}");
        let rows = analysis_rows(&changed);
        assert_eq!(rows.len(), 1, "{changed}");
        assert_eq!(rows[0]["points"], serde_json::json!("2500"));
        assert_eq!(rows[0]["downsampled"], serde_json::json!(true));
        let approximation = changed["accuracy"]["approximation"]
            .as_str()
            .expect("an approximation");
        assert!(
            approximation.ends_with("1 of 1 series downsampled"),
            "{approximation}"
        );

        let (is_error, summarized) = analyze(&client, "m", (10, 260), "100ms", "summary").await;
        assert_eq!(is_error, Some(false), "{summarized}");
        let rows = analysis_rows(&summarized);
        assert_eq!(rows[0]["points"], serde_json::json!("2500"));
        assert_eq!(rows[0]["downsampled"], serde_json::json!(false));

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// Through `POST /mcp`, a bare-number step, a sub-millisecond start, a
    /// range of 11,001 steps and an unknown op are each `invalid_argument`
    /// from `ravel_analyze_timeseries` with no store request, and the three
    /// range cases get the same message from `ravel_query_promql`.
    ///
    /// FLIP: a range check left to the engine reads the store before it
    /// refuses, and the store counter moves.
    #[tokio::test]
    async fn analyze_timeseries_refuses_bad_ranges_before_the_store() {
        let (store, metrics) = instrumented_memory();
        let running = start_server(store, Arc::clone(&metrics), mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");

        let range = |start_ns: i64, end_ns: i64| serde_json::json!({"start_ns": start_ns.to_string(), "end_ns": end_ns.to_string()});
        let cases = [
            (range(0, 60 * NS_PER_SEC), "30"),
            (range(1, 60 * NS_PER_SEC), "30s"),
            (range(0, 11_001 * NS_PER_SEC), "1s"),
        ];
        let before = total_store_calls(&metrics);
        for (time_range, step) in cases {
            let (is_error, analyzed) = call(
                &client,
                "ravel_analyze_timeseries",
                serde_json::json!({
                    "query": "m", "time_range": time_range, "step": step, "op": "summary",
                }),
            )
            .await;
            assert_eq!(is_error, Some(true), "{analyzed}");
            assert_eq!(
                analyzed["failure"]["class"],
                serde_json::json!("invalid_argument"),
                "{analyzed}"
            );
            let (_, evaluated) = call(
                &client,
                "ravel_query_promql",
                serde_json::json!({"query": "m", "time_range": time_range, "step": step}),
            )
            .await;
            assert_eq!(analyzed["failure"], evaluated["failure"], "{step}");
        }
        let (is_error, unknown) = call(
            &client,
            "ravel_analyze_timeseries",
            serde_json::json!({
                "query": "m", "time_range": range(0, 60 * NS_PER_SEC), "step": "30s",
                "op": "forecast",
            }),
        )
        .await;
        assert_eq!(is_error, Some(true), "{unknown}");
        assert_eq!(
            unknown["failure"]["class"],
            serde_json::json!("invalid_argument")
        );
        assert_eq!(total_store_calls(&metrics), before);

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    const TRACE_T: [u8; 16] = [0x7e; 16];

    /// `(span_id, parent_span_id, start_ts_ns)`.
    type TraceSpan = ([u8; 8], Option<[u8; 8]>, i64);

    /// One RSPAN object of [`TENANT`] holding trace [`TRACE_T`]'s spans.
    async fn publish_trace(store: &dyn ObjectStoreBackend, spans: &[TraceSpan]) {
        use ravel_commit::publish::RetryPolicy;
        use ravel_commit::record::NewCommitRecord;
        use ravel_commit::{keys, publish, record};
        use ravel_object_store::PutOptions;
        use ravel_types::Signal;

        let tenant_hash = TenantId::new(TENANT).hash();
        let writer_id = uuid::Uuid::from_u128(4_000);
        let mut writer = ravel_rspan::RspanWriter::new(
            ravel_rspan::RspanConfig::default(),
            ravel_rspan::ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.into_bytes(),
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for &(span_id, parent_span_id, start_ts_ns) in spans {
            writer.push(ravel_rspan::SpanRecord {
                trace_id: TRACE_T,
                span_id,
                parent_span_id,
                name: "op".to_string(),
                start_ts_ns,
                end_ts_ns: start_ts_ns + 1_000,
                status_code: ravel_rspan::StatusCode::Ok,
                status_message: None,
                attrs: vec![("service.name".to_string(), "svc".to_string())],
            });
        }
        let bytes = writer.finish().expect("finish rspan object");
        let min_ts = spans.iter().map(|span| span.2).min().expect("a span");
        let max_ts = spans
            .iter()
            .map(|span| span.2 + 1_000)
            .max()
            .expect("a span");
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Spans,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: spans.len() as u64,
            series_count: spans.len() as u64,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: min_ts,
            max_ingest_ts_ns: max_ts,
            segment_format_version: u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid span commit record");
        store
            .put(
                &keys::reconstruct_data_key(&rec).expect("data key"),
                bytes::Bytes::from(bytes),
                PutOptions::default(),
            )
            .await
            .expect("put span data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish span commit");
    }

    /// Each returned span as `[span_id, __ravel_depth, __ravel_parent]`,
    /// read by column name.
    fn span_tree(envelope: &serde_json::Value) -> Vec<serde_json::Value> {
        let columns = envelope["data"]["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .map(|column| column["name"].as_str().expect("a column name"))
            .collect::<Vec<_>>();
        let at = |name: &str| {
            columns
                .iter()
                .position(|column| *column == name)
                .unwrap_or_else(|| panic!("no {name} column in {columns:?}"))
        };
        let (span, depth, parent) = (at("span_id"), at("__ravel_depth"), at("__ravel_parent"));
        envelope["data"]["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| serde_json::json!([row[span], row[depth], row[parent]]))
            .collect()
    }

    /// A trace whose root starts at 10 s, its child at 20 s and grandchild
    /// at 30 s. Over the whole trace the tree is root, depth 1, depth 2. A
    /// `time_range` from 15 s drops the root: the child is `missing` at depth
    /// 0, the grandchild `present` at depth 1, both added columns lead the
    /// row, and the one next step is to widen the window.
    ///
    /// FLIP: the body that returns the port's envelope as it comes has no
    /// `__ravel_depth` column.
    #[tokio::test]
    async fn get_trace_reports_the_span_tree_and_a_dropped_root() {
        let (store, metrics) = instrumented_memory();
        let (root, child, grandchild) = ([0x01; 8], [0x02; 8], [0x03; 8]);
        publish_trace(
            store.as_ref(),
            &[
                (root, None, 10 * NS_PER_SEC),
                (child, Some(root), 20 * NS_PER_SEC),
                (grandchild, Some(child), 30 * NS_PER_SEC),
            ],
        )
        .await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");
        let trace_id = "7e".repeat(16);

        let (is_error, whole) = call(
            &client,
            "ravel_get_trace",
            serde_json::json!({"trace_id": trace_id}),
        )
        .await;
        assert_eq!(is_error, Some(false), "{whole}");
        assert_eq!(whole["status"], serde_json::json!("ok"), "{whole}");
        assert_eq!(
            span_tree(&whole),
            [
                serde_json::json!(["0101010101010101", "0", "root"]),
                serde_json::json!(["0202020202020202", "1", "present"]),
                serde_json::json!(["0303030303030303", "2", "present"]),
            ]
        );
        assert_eq!(whole["next_steps"], serde_json::json!([]), "{whole}");

        let (is_error, windowed) = call(
            &client,
            "ravel_get_trace",
            serde_json::json!({
                "trace_id": trace_id,
                "time_range": {
                    "start_ns": (15 * NS_PER_SEC).to_string(),
                    "end_ns": (60 * NS_PER_SEC).to_string(),
                },
            }),
        )
        .await;
        assert_eq!(is_error, Some(false), "{windowed}");
        assert_eq!(windowed["status"], serde_json::json!("ok"), "{windowed}");
        assert_eq!(
            windowed["data"]["columns"][0]["name"],
            serde_json::json!("__ravel_depth")
        );
        assert_eq!(
            windowed["data"]["columns"][1]["name"],
            serde_json::json!("__ravel_parent")
        );
        assert_eq!(
            span_tree(&windowed),
            [
                serde_json::json!(["0202020202020202", "0", "missing"]),
                serde_json::json!(["0303030303030303", "1", "present"]),
            ]
        );
        let actions: Vec<&serde_json::Value> = windowed["next_steps"]
            .as_array()
            .expect("next_steps")
            .iter()
            .map(|next| &next["action"])
            .collect();
        assert_eq!(
            actions,
            [&serde_json::json!("widen_time_range")],
            "{windowed}"
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }

    /// `ravel_get_trace` with `include_logs: true` through `POST /mcp`, over
    /// published spans of trace [`TRACE_T`] and log lines of which two carry
    /// it inside the window: the spans come back as without the leg, the
    /// `logs` block holds exactly those two lines in `ts` order, the scan
    /// warning is present, the leg's spend is reported apart from the span
    /// lookup's, and `budget.actual` is their sum.
    ///
    /// FLIP: a body that ignores `include_logs` has no `logs` block.
    #[tokio::test]
    async fn get_trace_include_logs_returns_the_trace_log_lines() {
        let (store, metrics) = instrumented_memory();
        let (root, child) = ([0x01; 8], [0x02; 8]);
        publish_trace(
            store.as_ref(),
            &[
                (root, None, 10 * NS_PER_SEC),
                (child, Some(root), 20 * NS_PER_SEC),
            ],
        )
        .await;
        let log = |ts_s: i64, body, trace_id| TestLog {
            ts_ns: ts_s * NS_PER_SEC,
            severity_num: 9,
            body,
            trace_id,
            attrs: vec![("region", "eu")],
        };
        publish_logs(
            store.as_ref(),
            &[
                log(12, "trace start", Some(TRACE_T)),
                log(15, "other trace", Some([0xcd; 16])),
                log(18, "no trace", None),
                log(25, "trace end", Some(TRACE_T)),
                log(70, "trace late", Some(TRACE_T)),
            ],
        )
        .await;
        let running = start_server(store, metrics, mcp_on()).await;
        let client =
            ().serve_with_lifecycle(transport(&running), current_revision())
                .await
                .expect("the client reaches the server");
        let arguments = |include_logs: bool| {
            serde_json::json!({
                "trace_id": "7e".repeat(16),
                "time_range": {"start_ns": "0", "end_ns": (60 * NS_PER_SEC).to_string()},
                "include_logs": include_logs,
            })
        };

        let (is_error, plain) = call(&client, "ravel_get_trace", arguments(false)).await;
        assert_eq!(is_error, Some(false), "{plain}");
        assert!(plain.get("logs").is_none(), "{plain}");

        let (is_error, found) = call(&client, "ravel_get_trace", arguments(true)).await;
        assert_eq!(is_error, Some(false), "{found}");
        assert_eq!(found["status"], serde_json::json!("ok"), "{found}");
        assert_eq!(found["data"], plain["data"]);
        assert_eq!(
            span_tree(&found),
            [
                serde_json::json!(["0101010101010101", "0", "root"]),
                serde_json::json!(["0202020202020202", "1", "present"]),
            ]
        );
        let logs = &found["logs"];
        assert_eq!(logs["status"], serde_json::json!("ok"), "{found}");
        assert_eq!(
            bodies(&serde_json::json!({"data": logs})),
            ["trace start", "trace end"],
            "{found}"
        );
        assert_eq!(logs["rows_omitted"], serde_json::json!(0));
        assert!(
            found["warnings"]
                .as_array()
                .expect("warnings")
                .contains(&serde_json::json!(
                    ravel_mcp::tools::get_trace::LOGS_SCAN_WARNING
                )),
            "{found}"
        );
        assert_eq!(found["budget"]["estimate_is_upper_envelope"], false);
        for figure in [
            "bytes_scanned",
            "store_requests",
            "segments_read",
            "decompressed_bytes",
        ] {
            let read = |value: &serde_json::Value| {
                value
                    .as_u64()
                    .unwrap_or_else(|| panic!("{figure} is a count in {found}"))
            };
            let spans = read(&logs["span_lookup_budget"]["actual"][figure]);
            let leg = read(&logs["budget"]["actual"][figure]);
            assert_eq!(
                read(&found["budget"]["actual"][figure]),
                spans + leg,
                "{figure}"
            );
        }
        assert!(
            logs["budget"]["actual"]["store_requests"]
                .as_u64()
                .is_some_and(|requests| requests > 0),
            "the leg read the log object: {found}"
        );

        client.cancel().await.expect("client stops");
        running.shutdown().await.expect("server stops");
    }
}
