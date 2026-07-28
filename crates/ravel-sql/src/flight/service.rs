//! The `FlightSqlService` implementation.
//!
//! Every method starts the same way: resolve the authoritative tenant from
//! gRPC metadata. Nothing -- not a statement, not a catalog listing, not a
//! table type -- is answered before that succeeds.
//!
//! # Ticket shapes
//!
//! Two kinds of ticket cross this service, and both are ordinary Flight SQL
//! commands so that arrow-flight's own `do_get` dispatcher routes them:
//!
//! - Statement: a `TicketStatementQuery` whose `statement_handle` is the
//!   encoded [`FlightTicket`] (tenant, statement, pinned segment set,
//!   resolve inputs, deadline). The handle is opaque to the protocol and
//!   self-describing to us.
//! - Metadata: the request command itself, packed straight back. There is
//!   nothing to pin -- the answers are constants -- so re-encoding the command
//!   is both the smallest ticket and the one the dispatcher already
//!   understands.

use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
    CommandStatementQuery, DoPutPreparedStatementResult, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{Action, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use futures::StreamExt;
use prost::Message;
use ravel_types::TenantHash;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::executor::SqlExecutor;
use crate::flight::request::{sql_request, status_from_sql};
use crate::flight::stream::{DoGetStream, statement_stream};
use crate::flight::{ClockRef, FlightAuth, FlightClock, FlightSqlConfig, metadata};
use crate::flight_ticket::{FlightTicket, SegmentPin};
use crate::validate::validate;

/// The message every prepared-statement method returns. Prepared statements
/// are out of the v1 Flight SQL scope (docs/arrow-datafusion-plan.md Phase C);
/// they are refused explicitly rather than left to arrow-flight's default
/// bodies so the refusal is a decision this crate states and tests, not an
/// accident of what happens to be unimplemented.
const PREPARED_UNSUPPORTED: &str =
    "prepared statements are not supported; use GetFlightInfo with a statement query";

/// Ravel's Flight SQL service: statement execution over pinned snapshots,
/// plus the constant catalog/metadata surface.
pub struct RavelFlightSqlService {
    executor: Arc<SqlExecutor>,
    auth: Arc<dyn FlightAuth>,
    clock: ClockRef,
    config: FlightSqlConfig,
}

impl RavelFlightSqlService {
    /// Build the service.
    ///
    /// `executor` is shared with the HTTP SQL endpoint in a process that runs
    /// both, which is deliberate: the per-tenant memory accountants live
    /// there, so a tenant's Flight and HTTP queries account against one
    /// budget rather than two independent ones.
    pub fn new(
        executor: Arc<SqlExecutor>,
        auth: Arc<dyn FlightAuth>,
        clock: Arc<dyn FlightClock>,
        config: FlightSqlConfig,
    ) -> Self {
        RavelFlightSqlService {
            executor,
            auth,
            clock,
            config,
        }
    }

    /// The authoritative tenant for a request. Every method calls this first.
    fn tenant(&self, metadata: &MetadataMap) -> Result<TenantHash, Status> {
        self.auth.tenant(metadata)
    }

    /// Wrap a single metadata `RecordBatch` as a `DoGet` response.
    fn one_batch(batch: RecordBatch) -> Result<Response<DoGetStream>, Status> {
        let schema = batch.schema();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async move {
                Ok::<RecordBatch, FlightError>(batch)
            }))
            .map(|item| {
                item.map_err(|err| {
                    tracing::warn!(error = %err, "failed to encode flight sql metadata");
                    Status::internal("failed to encode query results")
                })
            });
        Ok(Response::new(Box::pin(stream) as DoGetStream))
    }

    /// A `FlightInfo` for a metadata command: the response schema plus a
    /// ticket that is the command itself.
    fn metadata_info(
        command: &impl ProstMessageExt,
        schema: SchemaRef,
        descriptor: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = Ticket::new(command.as_any().encode_to_vec());
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|err| {
                tracing::error!(error = %err, "failed to encode flight sql metadata schema");
                Status::internal("failed to build catalog metadata")
            })?
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(descriptor);
        Ok(Response::new(info))
    }
}

#[tonic::async_trait]
impl FlightSqlService for RavelFlightSqlService {
    type FlightService = Self;

    // -----------------------------------------------------------------
    // Statement execution
    // -----------------------------------------------------------------

    /// Validate, resolve the snapshot exactly once, and mint a ticket pinning
    /// it.
    ///
    /// The order is the one docs/arrow-datafusion-plan.md section 2 fixes and
    /// `SqlExecutor::execute` follows: authenticate, then the security gate,
    /// then one `Catalog::resolve`, then plan. A rejected statement costs no
    /// catalog LIST.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let tenant = self.tenant(request.metadata())?;
        let min_tokens = self.auth.min_commit_tokens(request.metadata())?;

        // Step 1: the security gate, before any catalog or plan work.
        validate(&query.query).map_err(|err| status_from_sql(&err.into(), tenant))?;

        let now_ns = self.clock.now_ns();
        let req = sql_request(
            query.query.clone(),
            request.metadata(),
            min_tokens,
            now_ns,
            &self.config,
        )?;

        // Step 2: resolve exactly once. This snapshot, and only this
        // snapshot, is what DoGet will execute against (review F18).
        let snapshot = self
            .executor
            .resolve_snapshot(tenant, &req)
            .await
            .map_err(|err| status_from_sql(&err, tenant))?;
        let segments: Vec<SegmentPin> = snapshot
            .segments
            .iter()
            .map(SegmentPin::from_segment_ref)
            .collect();

        // Step 3: plan against the pinned snapshot so the FlightInfo can
        // carry the real result schema and so an unplannable query is
        // rejected now rather than at DoGet. Planning is logical only: the
        // provider's `scan` runs at physical planning time, so this reads no
        // segments.
        let planned = self
            .executor
            .plan_pinned(tenant, snapshot, &query.query)
            .await
            .map_err(|err| status_from_sql(&err, tenant))?;
        let schema = planned.schema();
        drop(planned);

        let ticket = FlightTicket {
            tenant,
            statement: query.query.clone(),
            segments,
            min_commit_tokens: req.min_tokens.clone(),
            now_ns,
            deadline_ns: self.config.ticket_deadline_ns(now_ns, req.deadline),
        };
        let handle = ticket.encode().map_err(|err| {
            // The only reachable case is an over-long statement, which is the
            // caller's own input.
            tracing::debug!(tenant = %tenant.to_hex(), error = %err, "flight ticket encode failed");
            Status::invalid_argument(err.to_string())
        })?;

        let ticket = Ticket::new(
            TicketStatementQuery {
                statement_handle: handle.into(),
            }
            .as_any()
            .encode_to_vec(),
        );
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|err| {
                tracing::error!(error = %err, "failed to encode flight sql result schema");
                Status::internal("failed to build query plan")
            })?
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(request.into_inner());
        Ok(Response::new(info))
    }

    /// Redeem a statement ticket against its pinned snapshot.
    ///
    /// The tenant comparison below is the security decision this ticket
    /// exists to enforce: the metadata-resolved tenant is authoritative and
    /// the ticket's embedded tenant is only a value to check it against. A
    /// mismatch is denied before the pinned snapshot is touched, so a stolen
    /// or replayed ticket reads nothing under different credentials.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let tenant = self.tenant(request.metadata())?;

        let decoded = FlightTicket::decode(&ticket.statement_handle).map_err(|err| {
            tracing::debug!(tenant = %tenant.to_hex(), error = %err, "flight ticket decode failed");
            Status::invalid_argument("malformed flight ticket")
        })?;

        if decoded.tenant != tenant {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                "flight ticket presented with credentials for a different tenant",
            );
            return Err(Status::permission_denied(
                "this ticket was not issued to the authenticated tenant",
            ));
        }

        let stream =
            statement_stream(&self.executor, Arc::clone(&self.clock), tenant, decoded).await?;
        Ok(Response::new(stream))
    }

    // -----------------------------------------------------------------
    // Catalog / metadata
    // -----------------------------------------------------------------

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.tenant(request.metadata())?;
        let schema = metadata::catalogs_schema();
        Self::metadata_info(&query, schema, request.into_inner())
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        self.tenant(request.metadata())?;
        Self::one_batch(metadata::catalogs()?)
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.tenant(request.metadata())?;
        let schema = metadata::db_schemas_schema(query.clone());
        Self::metadata_info(&query, schema, request.into_inner())
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        self.tenant(request.metadata())?;
        Self::one_batch(metadata::db_schemas(query)?)
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.tenant(request.metadata())?;
        let schema = metadata::tables_schema(query.clone());
        Self::metadata_info(&query, schema, request.into_inner())
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        self.tenant(request.metadata())?;
        Self::one_batch(metadata::tables(query)?)
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.tenant(request.metadata())?;
        let schema = metadata::table_types_schema(query);
        Self::metadata_info(&query, schema, request.into_inner())
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        self.tenant(request.metadata())?;
        Self::one_batch(metadata::table_types(query)?)
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.tenant(request.metadata())?;
        let schema = metadata::sql_info_schema(query.clone())?;
        Self::metadata_info(&query, schema, request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        self.tenant(request.metadata())?;
        Self::one_batch(metadata::sql_info(query)?)
    }

    // -----------------------------------------------------------------
    // Prepared statements: out of v1 scope
    // -----------------------------------------------------------------

    async fn get_flight_info_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(PREPARED_UNSUPPORTED))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        Err(Status::unimplemented(PREPARED_UNSUPPORTED))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        Err(Status::unimplemented(PREPARED_UNSUPPORTED))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(PREPARED_UNSUPPORTED))
    }

    async fn do_action_create_prepared_statement(
        &self,
        _query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(PREPARED_UNSUPPORTED))
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(PREPARED_UNSUPPORTED))
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(PREPARED_UNSUPPORTED))
    }

    /// No-op.
    ///
    /// The trait's one required method exists so a server can accumulate
    /// SqlInfo entries at registration time. Ravel's SqlInfo set is a
    /// compile-time constant built once in `super::metadata`, so there is
    /// nothing to register into and recording per-id state here would create
    /// a second, divergent source for the same answers.
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Assert at compile time that the service satisfies the `FlightService`
/// bound arrow-flight's blanket impl provides, so a signature drift in
/// arrow-flight fails here rather than at the registration site in
/// services/ravel-server.
fn _assert_flight_service()
where
    RavelFlightSqlService: FlightService,
{
}
