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
use ravel_maintain::{QueryAuditSink, QueryStatus, query_audit_event};
use ravel_query::QueryAdmissionController;
use ravel_types::TenantHash;
use ravel_types::accounting::{QueryAccounting, QueryCostRecorder, QueryWorkloadClass};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use tracing::Instrument as _;

use rand::Rng as _;

use crate::executor::SqlExecutor;
use crate::flight::request::{sql_request, status_from_sql};
use crate::flight::stream::{DoGetStream, audited_stream, fragment_stream, statement_stream};
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
    /// The evidential audit sink one event per executed statement is submitted
    /// through (ADR-0042 decision 4, ADR-0062 §2a, issues #395/#762/#413).
    /// Migrated from the raw object-store handle this service previously wrote
    /// the record to directly at stream *construction*: the audit is now
    /// submitted at stream *completion* (see [`do_get_statement`]), so the
    /// recorded status reflects the stream's actual outcome, and its durability
    /// is awaited before the client observes the stream end. The record is
    /// written by the server from the resolved tenant, never derived from the
    /// client's ticket, so a tenant cannot forge or suppress it. Defaults to
    /// [`NoopQueryAuditSink`](ravel_maintain::NoopQueryAuditSink); a deployment
    /// passes the one shared pipeline.
    audit_sink: Arc<dyn QueryAuditSink>,
    /// The process-global per-query cost aggregator (ADR-0044 section 4, issue
    /// #425). Each Flight statement folds its cost into it: `GetFlightInfo`
    /// records the resolve-and-plan cost with the query's estimate, and `DoGet`
    /// records its execution cost when the result stream ends. Shared with the
    /// PromQL and HTTP SQL paths through one instance per process, so all the
    /// read surfaces sum into one `ravel_query_*` family. Defaults to the no-op
    /// so an embedding with no aggregator needs no branch.
    recorder: Arc<dyn QueryCostRecorder>,
    /// The fleet-global query concurrency ceiling (ADR-0061 decision 2), shared
    /// with the PromQL and HTTP SQL surfaces so the process holds one honest
    /// in-flight count across every transport. Flight admits at both RPCs:
    /// `get_flight_info_statement` acquires a permit for resolve + logical plan
    /// (released when that RPC returns), and `do_get_statement` acquires its own
    /// for the streaming execution phase (held for the result stream's whole
    /// lifetime). The `do_get_statement` gate is the one that bounds the actual
    /// scan work -- see that method.
    query_admission: Arc<QueryAdmissionController>,
}

impl RavelFlightSqlService {
    /// Build the service.
    ///
    /// `executor` is shared with the HTTP SQL endpoint in a process that runs
    /// both, which is deliberate: the per-tenant memory accountants live
    /// there, so a tenant's Flight and HTTP queries account against one
    /// budget rather than two independent ones.
    ///
    /// `audit_sink` is the evidential seam one event per executed statement is
    /// submitted through (issues #395/#762). It defaults to the no-op when a
    /// deployment installs no pipeline; use [`with_audit_sink`](Self::with_audit_sink)
    /// to attach the shared pipeline. Keeping it out of `new`'s signature lets
    /// existing call sites stay unchanged.
    pub fn new(
        executor: Arc<SqlExecutor>,
        auth: Arc<dyn FlightAuth>,
        clock: Arc<dyn FlightClock>,
        config: FlightSqlConfig,
        recorder: Arc<dyn QueryCostRecorder>,
        query_admission: Arc<QueryAdmissionController>,
    ) -> Self {
        let mut ticket_key = TicketKey::default();
        rand::rng().fill_bytes(&mut ticket_key);
        RavelFlightSqlService {
            executor,
            auth,
            clock,
            config,
            ticket_key,
            audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
            recorder,
            query_admission,
        }
    }

    /// Attach the shared evidential audit sink (ADR-0062 §2a). Returns `self`
    /// so it chains off [`new`](Self::new).
    pub fn with_audit_sink(mut self, audit_sink: Arc<dyn QueryAuditSink>) -> Self {
        self.audit_sink = audit_sink;
        self
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

    /// Encode `ticket` into the opaque Flight `Ticket` a statement endpoint
    /// carries: sign it with the in-process key and wrap it in the
    /// `TicketStatementQuery` the `do_get` dispatcher routes on.
    fn encode_statement_ticket(
        &self,
        tenant: TenantHash,
        ticket: FlightTicket,
    ) -> Result<Ticket, Status> {
        let handle = ticket.encode(&self.ticket_key).map_err(|err| {
            // The only reachable case is an over-long statement, which is the
            // caller's own input.
            tracing::debug!(tenant = %tenant.to_hex(), error = %err, "flight ticket encode failed");
            Status::invalid_argument(err.to_string())
        })?;
        Ok(Ticket::new(
            TicketStatementQuery {
                statement_handle: handle.into(),
            }
            .as_any()
            .encode_to_vec(),
        ))
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

        // Fleet-global concurrency admission (ADR-0061 decision 2): decide before
        // the resolve below (and so before any GET). This permit covers only this
        // RPC -- resolve and logical plan -- and releases when the RPC returns.
        //
        // `do_get_statement` takes its OWN permit, held for the whole streaming
        // execution phase (the segment scans and object GETs that actually run
        // there), because this RPC's permit is already released by the time the
        // ticket is redeemed. The two RPCs are separate points in time, so one
        // logical Flight query never holds two slots at once. See
        // `do_get_statement`.
        let _permit = self.query_admission.try_admit().map_err(|_| {
            Status::resource_exhausted("fleet query concurrency ceiling reached; retry")
        })?;

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
        // This accounting handle covers this RPC's resolve and logical plan.
        // DoGet (crate::flight::stream) builds its own handle for the execution
        // it runs, so a Flight SQL statement's cost is recorded as two folds,
        // one per RPC (ADR-0044's documented two-handle split): the resolve and
        // plan cost here, the execution cost there. Both now reach `/metrics`
        // (issue #425). The `estimate` is this query's whole-query upper
        // envelope, so it is recorded once, here, against this RPC's actual;
        // DoGet records its actual with a zero estimate so the two folds sum to
        // one whole-query estimate beside the summed whole-query actual.
        let accounting = QueryAccounting::new();
        let (snapshot, estimate) = self
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

        // Fold this RPC's resolve-and-plan cost, with the query's estimate, into
        // the process aggregator (ADR-0044 section 4, issue #425). Recorded here,
        // after planning succeeds, so a statement rejected at resolve or plan
        // records nothing, the same rule the HTTP paths follow. DoGet folds the
        // execution cost separately.
        self.recorder.record(
            &accounting.snapshot(),
            &estimate,
            tenant,
            QueryWorkloadClass::Interactive,
        );

        let deadline_ns = self.config.ticket_deadline_ns(now_ns, req.deadline);

        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|err| {
                tracing::error!(error = %err, "failed to encode flight sql result schema");
                Status::internal("failed to build query plan")
            })?;

        // ADR-0071 (issue #866): an external Flight SQL client ALWAYS receives
        // exactly ONE endpoint, distribution installed or not. Arrow Flight's
        // two-RPC contract makes the N endpoints of one FlightInfo the
        // partitions of a SINGLE result set that a client unions; a cross-shard
        // scan fan-out is not that (unioning N per-slice partial scans would
        // return cross-slice duplicates undeduped, N partial aggregates, and up
        // to n*N rows for a LIMIT n). Distribution is instead an internal
        // coordinator-to-worker concern: the coordinator (issue #868) mints
        // slice tickets with [`crate::distributed::plan_distributed_slices`] and
        // redeems them against workers' `do_get` slice-fragment path
        // (`slice_count > 1`, served by [`do_get_statement`]). This client-facing
        // endpoint is therefore always the whole pinned set, redeemed on this
        // coordinator itself (no location); `slice_index`/`slice_count` mark it
        // as the whole snapshot (0 of 1).
        let ticket = FlightTicket {
            tenant,
            statement: query.query.clone(),
            segments,
            min_commit_tokens: req.min_tokens.clone(),
            now_ns,
            deadline_ns,
            slice_index: 0,
            slice_count: 1,
        };
        let info = info.with_endpoint(
            FlightEndpoint::new().with_ticket(self.encode_statement_ticket(tenant, ticket)?),
        );

        Ok(Response::new(info.with_descriptor(request.into_inner())))
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

        // Fleet-global concurrency admission for the execution phase (ADR-0061
        // decision 2). `get_flight_info_statement`'s permit released when that RPC
        // returned, so without this gate the actual scan-and-stream work below --
        // every segment GET this query performs -- would run unbounded regardless
        // of the configured ceiling. The permit moves into `statement_stream`,
        // where it lives inside the same `RecordOnStreamEnd` guard that folds the
        // query's cost, so it releases at exactly the moment the stream ends or
        // the client abandons it.
        //
        // A client can therefore be rejected HERE even after a successful
        // `GetFlightInfo`. That is admission back-pressure, not a ticket problem:
        // the minted ticket stays valid and redeemable later within its deadline,
        // so the client should simply retry the DoGet. This rejection is emitted
        // before any store work, mirroring `get_flight_info_statement`.
        let permit = self.query_admission.try_admit().map_err(|_| {
            Status::resource_exhausted("fleet query concurrency ceiling reached; retry")
        })?;

        // ADR-0071 (issue #866): a slice ticket (`slice_count > 1`) is an
        // INTERNAL coordinator-to-worker request, never something an external
        // client holds -- `get_flight_info_statement` only ever mints the
        // whole-set ticket (`slice_count == 1`). When one arrives, serve the raw
        // scan fragment over this slice's pinned segments: the internal-schema,
        // `(series_id, ts)`-sorted rows with provenance retained, scan-only, with
        // no statement planning (a slice ticket carries `statement: String::new()`,
        // so there is nothing to `validate`), no aggregation, and no dedup. The
        // authoritative cross-slice dedup and any aggregate stay at the
        // coordinator (crate::distributed). A slice fetch is internal fan-out,
        // not a distinct auditable query, so it is not audit-wrapped; the audited
        // unit is the client-facing statement the coordinator serves. The tenant
        // check above still gates it: a slice ticket carries the coordinator's
        // resolved tenant, and only this deployment's own ticket key MACs it.
        if decoded.slice_count > 1 {
            let span = tracing::info_span!(
                "flight_sql_slice_fragment",
                tenant_hash = %tenant.to_hex(),
                workload_class = "interactive",
                s3_requests = tracing::field::Empty,
                s3_bytes = tracing::field::Empty,
            );
            let stream = fragment_stream(
                &self.executor,
                Arc::clone(&self.clock),
                tenant,
                decoded,
                &self.config,
                Arc::clone(&self.recorder),
                span.clone(),
                permit,
            )
            .instrument(span)
            .await?;
            return Ok(Response::new(stream));
        }

        // The statement now reaches execution against its pinned snapshot for a
        // resolved tenant, so it is auditable (ADR-0042 decision 4, ADR-0062
        // §2a). A request rejected above (an undecodable or MAC-invalid ticket,
        // or a ticket presented under a different tenant) returned before this
        // point and is not audited: no statement executed, so there is nothing
        // to attribute, the same rule the HTTP endpoint follows for its own
        // early rejections.
        //
        // The audit is submitted at stream *completion*, not construction
        // (issue #413). Writing it here from whether the first batch pulled
        // cleanly recorded "ok" for a query that then failed partway or was
        // cancelled by the client -- the status-accuracy gap this closes. The
        // returned stream is wrapped by `audited_stream` so the recorded status
        // is the stream's actual terminal outcome (ok / errored / cancelled),
        // and the event's durability is awaited before the client observes the
        // stream's end. A statement that fails before any stream exists (a plan
        // or first-batch error surfaced by `statement_stream` as a redacted
        // `Status`) is that query's final outcome, so it is audited Error and
        // awaited here, synchronously, before the error is returned.
        let now_ns = self.clock.now_ns();
        let query_text = decoded.statement.clone();

        // A request-level span carrying only bounded values (ADR-0044 section
        // 5), the Flight SQL counterpart of the `sql_query`/`analytics_query`
        // spans the HTTP handlers build: the tenant hash, the workload class,
        // and -- once the query finishes -- its final store request and byte
        // counts. No statement text, label values, or object keys ever become
        // span fields; the allowlist (ADR-0044 section 4: tenant_hash, signal,
        // mode, op, error_kind, workload_class, level, plus the two count
        // fields) is closed. Every Flight SQL statement is an interactive,
        // client-driven query.
        //
        // Unlike the HTTP path, execution here is streamed. `statement_stream`
        // returns after only the first batch is pulled, and the query's cost
        // keeps rising as the client drains the rest, so the two count fields
        // are recorded not here but when the stream ends -- from the same
        // snapshot the cost recorder folds into `/metrics` -- so the span and
        // the scrape report the same totals for one query (crate::flight::
        // stream::RecordOnStreamEnd). `.instrument` scopes only the setup
        // future (through the first batch) under the span, matching the way
        // sql.rs instruments its executor call.
        //
        // `workload_class` is stamped as the literal "interactive": the label
        // spelling lives on `services/ravel-server`'s `WorkloadClass::name`,
        // which this crate cannot name, and `QueryWorkloadClass` in
        // `ravel-types` carries no spelling. The string matches what that
        // method renders for `QueryWorkloadClass::Interactive`, which is the
        // class the cost fold below already stamps.
        let span = tracing::info_span!(
            "flight_sql_statement",
            tenant_hash = %tenant.to_hex(),
            workload_class = "interactive",
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
        );

        let result = statement_stream(
            &self.executor,
            Arc::clone(&self.clock),
            tenant,
            decoded,
            &self.config,
            Arc::clone(&self.recorder),
            span.clone(),
            permit,
        )
        .instrument(span.clone())
        .await;

        match result {
            // The stream started: wrap it so the audit fires at completion with
            // the stream's real terminal status, awaiting durability before the
            // client observes the stream end (issue #413).
            Ok(stream) => Ok(Response::new(audited_stream(
                stream,
                Arc::clone(&self.audit_sink),
                tenant,
                now_ns,
                query_text,
            ))),
            // The statement never produced a stream (plan / first-batch error):
            // that is its final outcome. Audit Error and await durability before
            // returning; a submission failure fails the request closed with an
            // `Unavailable` status (audit_mode=required), the deliberate
            // inversion of "queries outlive the trail".
            Err(status) => {
                let event = query_audit_event(
                    &tenant,
                    now_ns,
                    &query_text,
                    "sql",
                    QueryStatus::Error,
                    0,
                    0,
                );
                if self.audit_sink.submit(event).await.is_err() {
                    return Err(Status::unavailable(
                        "query audit is temporarily unavailable; retry",
                    ));
                }
                Err(status)
            }
        }
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
