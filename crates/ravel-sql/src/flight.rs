//! Flight SQL transport for the SQL execution path, compiled only under the
//! `flight-sql` feature.
//!
//! The module is deliberately empty. Ticket C1a (issue #149) wires the
//! arrow-flight dependency and this feature flag so the version pairing is
//! settled and both builds are green before any protocol code exists.
//!
//! The `arrow_flight::sql::server::FlightSqlService` implementation lands here
//! under issue #152: statement and prepared-statement execution through the
//! same per-query `SessionContext` the HTTP path uses, GetFlightInfo/DoGet
//! streaming off the scan pipeline, snapshot pinning through an opaque ticket
//! that encodes tenant, statement, and resolved snapshot identity, and gRPC
//! metadata auth and tenant resolve (docs/arrow-datafusion-plan.md Phase C).
//! Do not add protocol logic here outside that ticket.
