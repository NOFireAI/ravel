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
use ravel_maintain::{QueryStatus, write_query_audit};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use rand::Rng as _;

use crate::executor::SqlExecutor;
use crate::flight::request::{sql_request, status_from_sql};
use crate::flight::stream::{DoGetStream, statement_stream};
use crate::flight::{ClockRef, FlightAuth, FlightClock, FlightSqlConfig, metadata};
use crate::flight_ticket::{FlightTicket, SegmentPin, TicketKey};
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
    /// The in-process secret this service's tickets are signed and verified
    /// with (issue #185). Generated once, here, at construction time and
    /// held only in memory: never logged, never sent to a client, never
    /// persisted. A process restart mints a fresh key and invalidates every
    /// ticket the previous process signed, which is safe because a ticket is
    /// ephemeral by construction (bounded by `deadline_ns`) and never meant
    /// to outlive the process that minted it. Key persistence/rotation across
    /// restarts is not implemented; see the module docs if a deployment ever
    /// needs tickets to survive a restart.
    ticket_key: TicketKey,
    /// Object store handle used to write the query-audit record (ADR-0042
    /// decision 4, issue #395). The record is written by the server itself
    /// from the statement-execution path, never derived from the client's
    /// ticket, so a tenant using Flight SQL cannot forge or suppress its own
    /// query-audit trail any more than an HTTP `POST /api/v1/sql` caller can.
    /// Shared with the HTTP endpoint's own store handle in a process that runs
    /// both, exactly as `executor` is.
    store: Arc<dyn ObjectStoreBackend>,
}

impl RavelFlightSqlService {
    /// Build the service.
    ///
    /// `executor` is shared with the HTTP SQL endpoint in a process that runs
    /// both, which is deliberate: the per-tenant memory accountants live
    /// there, so a tenant's Flight and HTTP queries account against one
    /// budget rather than two independent ones.
    ///
    /// `store` is the object store the query-audit record is written to (issue
    /// #395). It is the same handle the HTTP endpoint's `SqlState` carries, so
    /// both transports write the audit trail through one store.
    pub fn new(
        executor: Arc<SqlExecutor>,
        auth: Arc<dyn FlightAuth>,
        clock: Arc<dyn FlightClock>,
        config: FlightSqlConfig,
        store: Arc<dyn ObjectStoreBackend>,
    ) -> Self {
        let mut ticket_key = TicketKey::default();
        rand::rng().fill_bytes(&mut ticket_key);
        RavelFlightSqlService {
            executor,
            auth,
            clock,
            config,
            ticket_key,
            store,
        }
    }

    /// Write the query-audit record for one executed statement (ADR-0042
    /// decision 4, issue #395).
    ///
    /// The audit trail is a server-side obligation independent of the query
    /// outcome, so a failure to write it never changes the client's response:
    /// it is logged loudly (`tracing::error!`) - a silently dropped audit
    /// record would defeat the whole feature - and then swallowed, the same
    /// failure-isolation discipline `services/ravel-server/src/sql.rs` applies
    /// to the HTTP path.
    ///
    /// The window bounds are recorded as `0, 0`. Unlike the HTTP request body,
    /// a Flight statement carries no event-time window on the redemption path:
    /// the window a client sends via metadata is consumed at `GetFlightInfo`
    /// to resolve and pin the snapshot, and is not carried in the ticket, so at
    /// `DoGet` - where the statement actually executes and this record is
    /// written - no resolved window is available to record. Recording `0, 0`
    /// states "unknown" plainly rather than fabricating a plausible-looking
    /// range the request never used.
    async fn write_audit(
        &self,
        tenant: TenantHash,
        now_ns: i64,
        query_text: &str,
        status: QueryStatus,
    ) {
        if let Err(err) = write_query_audit(
            self.store.as_ref(),
            &tenant,
            now_ns,
            query_text,
            "sql",
            status,
            0,
            0,
        )
        .await
        {
            tracing::error!(
                tenant = %tenant.to_hex(),
                error = %err,
                "failed to write flight sql query-audit record; client response \
                 unaffected but the audit trail is now incomplete for this request",
            );
        }
    }

    /// The in-process key this service signs and verifies tickets with.
    ///
    /// Exposed so a test can decode a ticket outside the normal `DoGet` path
    /// (for example, to assert what `GetFlightInfo` minted). Never logged or
    /// sent to a client by any code path in this crate.
    pub fn ticket_key(&self) -> &TicketKey {
        &self.ticket_key
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
        //
        // This accounting handle covers only this RPC's resolve and logical
        // plan; DoGet (crate::flight::stream) builds its own for the
        // execution it runs, so a Flight SQL statement's accounting is split
        // across two handles rather than unified across the two RPCs like
        // the HTTP path's single `SqlExecutor::execute` call. Known,
        // documented gap (ADR-0044); not fixed by this ticket.
        let accounting = QueryAccounting::new();
        let (snapshot, _estimate) = self
            .executor
            .resolve_snapshot(tenant, &req, &accounting)
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
            .plan_pinned(tenant, snapshot, &query.query, &accounting)
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
        let handle = ticket.encode(&self.ticket_key).map_err(|err| {
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

        let decoded = FlightTicket::decode(&ticket.statement_handle, &self.ticket_key).map_err(
            |err| {
                tracing::debug!(tenant = %tenant.to_hex(), error = %err, "flight ticket decode failed");
                Status::invalid_argument("malformed flight ticket")
            },
        )?;

        if decoded.tenant != tenant {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                "flight ticket presented with credentials for a different tenant",
            );
            return Err(Status::permission_denied(
                "this ticket was not issued to the authenticated tenant",
            ));
        }

        // The statement now reaches execution against its pinned snapshot for a
        // resolved tenant, so it is auditable (ADR-0042 decision 4), exactly as
        // `POST /api/v1/sql` audits after `SqlExecutor::execute` returns. Run
        // it, then write one query-audit record for the outcome - success, or
        // the specific `SqlError` `statement_stream` surfaced as a redacted
        // `Status` - before handing the stream (or the error) back. A request
        // rejected above (an undecodable or MAC-invalid ticket, or a ticket
        // presented under a different tenant) returned before this point and is
        // not audited: no statement executed, so there is nothing to attribute,
        // the same rule the HTTP endpoint follows for its own early rejections.
        //
        // The audit is written here, synchronously, from whether execution
        // *started* (the first batch was pulled without error), not from the
        // whole stream draining: buffering the entire result to learn the final
        // status would defeat streaming and block the response, and the HTTP
        // path likewise audits the executor's result before the encoder runs.
        let now_ns = self.clock.now_ns();
        let query_text = decoded.statement.clone();
        let result = statement_stream(
            &self.executor,
            Arc::clone(&self.clock),
            tenant,
            decoded,
            &self.config,
        )
        .await;
        let status = match &result {
            Ok(_) => QueryStatus::Ok,
            Err(_) => QueryStatus::Error,
        };
        self.write_audit(tenant, now_ns, &query_text, status).await;

        result.map(Response::new)
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
