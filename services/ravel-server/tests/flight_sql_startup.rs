//! Ticket C1a (issue #149): the server starts with `--features flight-sql` and
//! serves a Flight service on the gRPC listener.
//!
//! There is no Flight SQL logic yet, so the assertion is exactly what the
//! ticket delivers: a query-only process binds gRPC (it would not without the
//! feature, since gRPC otherwise carries only OTLP ingest), a Flight client
//! reaches the registered service, and every method answers UNIMPLEMENTED
//! rather than UNIMPLEMENTED-by-absence. Issue #152 replaces this with real
//! method coverage.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::Criteria;
use arrow_flight::flight_service_client::FlightServiceClient;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

#[tokio::test]
async fn flight_service_is_registered_and_answers_unimplemented() {
    let tenant = TenantId::new("acme");
    let mut tokens = HashMap::new();
    tokens.insert("testtoken".to_string(), tenant.clone());
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let config = ServerConfig {
        mode: Mode::Query,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            fold_interval: Duration::from_secs(60),
        },
    };
    let running = ravel_server::start(config, store).await.expect("starts");

    let grpc_addr = running
        .grpc_addr
        .expect("query mode binds gRPC when flight-sql is on");

    let channel = tonic::transport::Channel::from_shared(format!("http://{grpc_addr}"))
        .expect("valid endpoint uri")
        .connect()
        .await
        .expect("Flight client connects");
    let mut client = FlightServiceClient::new(channel);
    let status = client
        .list_flights(Criteria::default())
        .await
        .expect_err("no Flight method is implemented yet");
    assert_eq!(
        status.code(),
        tonic::Code::Unimplemented,
        "expected the registered stub service to answer, got {status:?}"
    );

    running.shutdown().await.expect("graceful shutdown");
}
