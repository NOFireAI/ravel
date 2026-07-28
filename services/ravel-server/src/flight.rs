//! Flight SQL registration on the gRPC listener, compiled only under the
//! `flight-sql` feature.
//!
//! Ticket C1a (issue #149) wires the dependency and the feature flag; there is
//! no protocol logic here yet. The service registered below answers every RPC
//! with `unimplemented`, which is what arrow-flight's `FlightSqlService`
//! default methods do: the point is to prove the binary links arrow-flight,
//! registers a Flight service on the same tonic server as OTLP, and starts.
//!
//! Issue #152 replaces [`UnimplementedFlightSqlService`] with the real service
//! from `ravel_sql::flight`, which executes through the same per-query
//! `SessionContext` the HTTP SQL endpoint uses.

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::SqlInfo;
use arrow_flight::sql::server::FlightSqlService;

/// A Flight SQL service that implements no method.
///
/// Every RPC falls through to arrow-flight's default trait bodies, which
/// return `Status::unimplemented`. `register_sql_info` is the one required
/// method, and it records nothing because there is no SQL-info catalog to
/// record into until issue #152.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnimplementedFlightSqlService;

#[tonic::async_trait]
impl FlightSqlService for UnimplementedFlightSqlService {
    type FlightService = Self;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Builds the Flight service to register on the gRPC server.
pub fn service() -> FlightServiceServer<UnimplementedFlightSqlService> {
    FlightServiceServer::new(UnimplementedFlightSqlService)
}
