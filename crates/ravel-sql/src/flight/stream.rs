//! `DoGet`: execute a redeemed ticket against its pinned snapshot and stream
//! the result as `FlightData`.
//!
//! # The retry contract on a streaming transport
//!
//! The HTTP path re-resolves and retries once when a pinned segment vanishes
//! before any batch was emitted. This path cannot re-resolve: the snapshot is
//! fixed by the ticket, and resolving again would produce a *different*
//! snapshot, which is exactly what the pin exists to prevent. So the retry is
//! the same retry against the same pin:
//!
//! - A store `NotFound` **before the first batch has been emitted** rebuilds
//!   the session over the *same* pinned snapshot and re-executes once. A
//!   second `NotFound` fails [`SqlError::SnapshotInvalidated`]: the segment is
//!   genuinely gone, and no other snapshot may be substituted for it.
//! - A store `NotFound` **after** any batch has been sent fails
//!   `SnapshotInvalidated` immediately, with zero retries. Rows are already on
//!   the wire; re-running the plan would duplicate them.
//!
//! The decision itself is not restated here. It is
//! [`retry_decision`](crate::executor::retry_decision), the same pure function
//! the HTTP driver calls, so the two transports cannot drift apart in the one
//! place where drifting would silently duplicate or drop rows.
//!
//! This is also the path that makes `retry_decision`'s post-emission branch
//! reachable in production for the first time (see its docs): a Flight client
//! consumes batches as they are produced, so a fault can arrive after emission
//! has started.
//!
//! # Deadline
//!
//! The ticket's `deadline_ns` is a client-supplied wall-clock value, so it is
//! never trusted verbatim: [`FlightSqlConfig::clamp_ticket_deadline_ns`]
//! re-derives the deployment's own bound (`now_ns + max_deadline`, itself
//! capped by the GC protection horizon) and takes the minimum with the
//! ticket's value, so the embedded deadline can only ever shorten the
//! effective budget, never lengthen it past what this deployment would mint
//! today. That effective deadline is checked once before
//! execution starts, and again as each batch is handed out, so a consumer
//! slow enough to cross it mid-stream fails `SnapshotInvalidated` rather than
//! being served batches read under a pin that is no longer protected.
//!
//! # Cancellation
//!
//! There is no cancellation code here, and that is the design. The stream owns
//! its [`PinnedStream`](crate::executor::PinnedStream), which owns the
//! session, which owns the `TenantDelegatingPool`. A client that disconnects
//! or a server that drops the stream drops all of it; every
//! `MemoryReservation` shrinks through the pool into the tenant accountant on
//! the way down (crate::memory). A second release mechanism here
//! would double-count against the tenant budget.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow_flight::FlightData;
use arrow_flight::encode::{DictionaryHandling, FlightDataEncoderBuilder};
use arrow_flight::error::FlightError;
use datafusion::arrow::array::RecordBatch;
use futures::{Stream, StreamExt};
use ravel_catalog::Snapshot;
use ravel_maintain::{QueryAuditSink, QueryStatus, query_audit_event};
use ravel_query::QueryPermit;
use ravel_types::TenantHash;
use ravel_types::accounting::{
    CostEstimate, QueryAccounting, QueryCostRecorder, QueryWorkloadClass,
};
use std::sync::Arc;
use tonic::Status;

use crate::error::SqlError;
use crate::executor::{DistributedScan, PinnedStream, RetryDecision, SqlExecutor, retry_decision};
use crate::flight::ClockRef;
use crate::flight::FlightSqlConfig;
use crate::flight::request::status_from_sql;
use crate::flight_ticket::FlightTicket;
use crate::validate::validate;

/// The `DoGet` response stream type the blanket `FlightService` impl expects.
pub(super) type DoGetStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

/// Execute `ticket` against its pinned snapshot and return the encoded
/// `FlightData` stream.
///
/// `tenant` is the authoritative, metadata-resolved tenant; the caller has
/// already rejected a ticket whose embedded tenant disagrees with it. `permit`
/// is the fleet-global concurrency slot the caller admitted for this execution;
/// it moves into the stream's guard so the slot is held for the whole streaming
/// phase.
#[allow(clippy::too_many_arguments)]
pub(super) async fn statement_stream(
    executor: &SqlExecutor,
    clock: ClockRef,
    tenant: TenantHash,
    ticket: FlightTicket,
    config: &FlightSqlConfig,
    recorder: Arc<dyn QueryCostRecorder>,
    span: tracing::Span,
    permit: QueryPermit,
    distributed: Option<DistributedScan>,
) -> Result<DoGetStream, Status> {
    // Re-run the security gate on redemption. The statement is carried in
    // bytes a client holds, and the gate is cheap; running it again means a
    // tampered or replayed ticket cannot reach the planner even if it somehow
    // survived the MAC.
    validate(&ticket.statement).map_err(|err| status_from_sql(&SqlError::from(err), tenant))?;

    let now_ns = clock.now_ns();
    // The ticket's own deadline_ns is client-supplied and may only shorten
    // the effective budget below what this deployment's config would mint
    // today, never lengthen it.
    let deadline_ns = config.clamp_ticket_deadline_ns(ticket.deadline_ns, now_ns);
    if now_ns >= deadline_ns {
        // The pin's GC protection window has closed. This is exactly the
        // "ticket replay after deadline expiry" case: never different data,
        // always SnapshotInvalidated.
        return Err(status_from_sql(&SqlError::SnapshotInvalidated, tenant));
    }
    let budget = remaining(now_ns, deadline_ns);

    let snapshot = ticket.snapshot();
    let started = tokio::time::timeout(
        budget,
        // The declared typed attribute columns pinned into the ticket at
        // `get_flight_info` (ADR-0090): `DoGet` plans against exactly this
        // list, never a re-resolution that a concurrent cache refresh could
        // have changed.
        start_pinned(
            executor,
            tenant,
            &snapshot,
            &ticket.statement,
            distributed,
            &ticket.declared_columns,
        ),
    )
    .await
    .unwrap_or_else(|_| {
        Err(SqlError::DeadlineExceeded {
            millis: u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
        })
    });

    let (first, stream, accounting) = started.map_err(|err| status_from_sql(&err, tenant))?;
    let schema = stream.schema();

    // Fold this RPC's execution cost into the aggregator when the result stream
    // ends (ADR-0044 section 4). `accounting` is the successful
    // attempt's handle; the executing stream holds clones of it
    // (SqlExecutor::plan_pinned), so its counters keep rising as batches are
    // pulled. The guard snapshots it on drop -- when the client finishes the
    // stream or drops it -- so the recorded cost covers the whole DoGet, not
    // just the first batch. The estimate is zero here: the query's whole-query
    // estimate was recorded at GetFlightInfo (crate::flight::service), so the
    // two RPCs' folds sum to one estimate beside the summed actual. A DoGet
    // that failed before this point returned above and records nothing.
    //
    // `permit` is the fleet-global concurrency slot `do_get_statement` acquired
    // for this execution (ADR-0061 decision 2). It moves into the guard so the
    // slot is held for the whole streaming phase and released at exactly the
    // moment the cost fold happens -- stream end or client abandonment -- by the
    // same `Drop`. A DoGet that failed before this point dropped the permit on
    // its early return, so the slot never leaks on the error paths above.
    let record_guard = RecordOnStreamEnd {
        recorder,
        accounting,
        tenant,
        span,
        _permit: permit,
    };

    // Post-emission rule: any vanished segment from here on is terminal.
    let tail = stream.map(|item| {
        item.map_err(|err| {
            if err.is_segment_not_found() {
                SqlError::SnapshotInvalidated
            } else {
                err
            }
        })
    });

    let batches = futures::stream::iter(first.map(Ok)).chain(tail);
    let batches = batches.map(move |item| {
        item.and_then(|batch| {
            if clock.now_ns() >= deadline_ns {
                Err(SqlError::SnapshotInvalidated)
            } else {
                Ok(batch)
            }
        })
        // The encoder's error type is the only channel out of the stream, so
        // the already-redacted `Status` rides inside it and is unwrapped
        // again by `flight_error_to_status` below.
        .map_err(|err| FlightError::Tonic(Box::new(status_from_sql(&err, tenant))))
    });

    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batches)
        .map(move |item| {
            // Owning the guard in the encoder's per-item closure ties its
            // lifetime to the stream: when the stream is exhausted or dropped,
            // this closure drops and the guard folds the final cost. The
            // reference keeps the move explicit without touching the value.
            let _keep = &record_guard;
            item.map_err(flight_error_to_status)
        });

    Ok(Box::pin(encoded))
}

/// Serve one distributed slice's worker fragment for `DoGet` (ADR-0071) and return the encoded, internal-schema `FlightData` stream.
///
/// This is the worker half of the SQL distributed lane, engaged only when the
/// redeemed ticket is a slice ticket (`slice_count > 1`). It differs from
/// [`statement_stream`] in exactly the ways ADR-0071 requires, and no others:
///
/// - No [`validate`]: a slice ticket carries no SQL statement
///   (`statement: String::new()`), so there is nothing to re-gate. The ticket
///   MAC already authenticated that this coordinator minted it; only the
///   coordinator, never an external client, ever holds one.
/// - No statement planning, aggregation, or dedup: the fragment is the
///   internal-schema, `(series_id, ts)`-sorted scan over this slice's pinned
///   segments ([`SqlExecutor::worker_fragment_stream`]). Cross-slice dedup and
///   any aggregate stay authoritative at the coordinator (crate::distributed).
/// - No audit wrap: the audited unit is the client-facing statement the
///   coordinator serves, recorded once on its own `do_get`; a per-slice fetch
///   is internal fan-out, not a distinct auditable query.
///
/// The deadline clamp, the per-batch deadline re-check, the segment-vanished to
/// `SnapshotInvalidated` mapping, and the cost fold on stream end are identical
/// to the statement path, so a slice fetch is accounted and bounded exactly as
/// a local scan is.
#[allow(clippy::too_many_arguments)]
pub(super) async fn fragment_stream(
    executor: &SqlExecutor,
    clock: ClockRef,
    tenant: TenantHash,
    ticket: FlightTicket,
    config: &FlightSqlConfig,
    recorder: Arc<dyn QueryCostRecorder>,
    span: tracing::Span,
    permit: QueryPermit,
) -> Result<DoGetStream, Status> {
    let now_ns = clock.now_ns();
    // Same deadline discipline as the statement path: the ticket's embedded
    // deadline can only shorten the effective budget, never lengthen it past
    // what this deployment would mint today. An expired slice
    // ticket is a replay after its pin's GC protection closed: never different
    // data, always SnapshotInvalidated.
    let deadline_ns = config.clamp_ticket_deadline_ns(ticket.deadline_ns, now_ns);
    if now_ns >= deadline_ns {
        return Err(status_from_sql(&SqlError::SnapshotInvalidated, tenant));
    }
    let budget = remaining(now_ns, deadline_ns);

    let snapshot = ticket.snapshot();
    // A fresh accounting handle, folded into the coordinator's aggregator when
    // this slice's stream ends, exactly as the statement path folds its own
    //. The executing fragment holds clones of it, so its counters
    // keep rising as batches are pulled.
    let accounting = QueryAccounting::new();
    let stream = tokio::time::timeout(
        budget,
        executor.worker_fragment_stream(tenant, snapshot, &accounting),
    )
    .await
    .unwrap_or_else(|_| {
        Err(SqlError::DeadlineExceeded {
            millis: u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
        })
    })
    .map_err(|err| status_from_sql(&err, tenant))?;
    let schema = stream.schema();

    // The cost/permit guard, dropped at stream end or client abandonment, folds
    // this slice's fetched bytes into the aggregator and releases the
    // concurrency slot in the same `Drop` (ADR-0061 decision 2, ADR-0044).
    let record_guard = RecordOnStreamEnd {
        recorder,
        accounting,
        tenant,
        span,
        _permit: permit,
    };

    let batches = stream
        .map(|item| {
            item.map_err(|err| {
                if err.is_segment_not_found() {
                    SqlError::SnapshotInvalidated
                } else {
                    err
                }
            })
        })
        .map(move |item| {
            item.and_then(|batch| {
                if clock.now_ns() >= deadline_ns {
                    Err(SqlError::SnapshotInvalidated)
                } else {
                    Ok(batch)
                }
            })
            .map_err(|err| FlightError::Tonic(Box::new(status_from_sql(&err, tenant))))
        });

    // Resend dictionaries rather than hydrating them (the encoder default).
    // A slice fragment's consumer is the coordinator's `DistributedScanExec`,
    // which declares `internal_schema()` and hands the decoded batches straight
    // into `SortPreservingMergeExec`; DataFusion validates each batch against
    // that schema. `internal_schema()` dictionary-encodes the `labels` column
    // (`Dictionary(Int32, Map)`, crate::schema), so hydrating it to a plain
    // `Map` on the wire would make every decoded batch fail the coordinator's
    // schema check. The public statement path (`statement_stream`) can keep the
    // hydrating default because its consumer is an external client with no
    // downstream plan to satisfy; this internal fan-out cannot.
    let encoded = FlightDataEncoderBuilder::new()
        .with_dictionary_handling(DictionaryHandling::Resend)
        .with_schema(schema)
        .build(batches)
        .map(move |item| {
            let _keep = &record_guard;
            item.map_err(flight_error_to_status)
        });

    Ok(Box::pin(encoded))
}

/// Records a completed `DoGet`'s execution cost into the aggregator on drop,
/// i.e. when the result stream ends or the client drops it. Holds a clone of
/// the query's [`QueryAccounting`] handle, whose counters the executing stream
/// keeps incrementing, so the snapshot taken here is the whole RPC's cost. The
/// estimate is zero: the whole-query estimate was recorded once at
/// `GetFlightInfo`.
///
/// It also carries the fleet-global concurrency [`QueryPermit`] for this
/// execution (ADR-0061 decision 2): one guard, held for the stream's whole
/// lifetime, both folds the cost and releases the slot in the same `Drop`, so
/// the concurrency ceiling binds the actual streaming phase and not merely the
/// planning RPC.
struct RecordOnStreamEnd {
    recorder: Arc<dyn QueryCostRecorder>,
    accounting: QueryAccounting,
    tenant: TenantHash,
    /// The request-level `flight_sql_statement` span (crate::flight::service).
    /// The query's final store totals are recorded on it here, from the same
    /// snapshot folded into `/metrics`, so the span and the scrape agree.
    span: tracing::Span,
    /// The concurrency slot this execution holds. Never read; its only job is to
    /// release the slot (via [`QueryPermit`]'s own `Drop`) when this guard drops,
    /// which is the same instant the cost fold above happens.
    _permit: QueryPermit,
}

impl Drop for RecordOnStreamEnd {
    fn drop(&mut self) {
        let snapshot = self.accounting.snapshot();
        // Record the query's final store totals on the request-level span
        // (ADR-0044 section 5). These are the only two count fields the span's
        // closed allowlist permits, and they are taken from the same snapshot
        // the aggregator fold below uses so the span and `/metrics` never
        // disagree for one query.
        self.span
            .record("s3_requests", snapshot.total_s3_requests());
        self.span.record("s3_bytes", snapshot.total_s3_bytes());
        self.recorder.record(
            &snapshot,
            &CostEstimate::new(0, 0, 0, 0, 0),
            self.tenant,
            QueryWorkloadClass::Interactive,
        );
    }
}

/// The fixed client message when a `required`-mode audit flush fails and the
/// Flight stream must fail closed. Carries no server state.
const AUDIT_UNAVAILABLE_MSG: &str = "query audit is temporarily unavailable; retry";

/// The evidential-audit context one Flight statement submits at stream
/// completion: the sink, the resolved tenant, the request timestamp, and the
/// statement text. Held by [`AuditedStream`] until the terminal event, then
/// consumed to build and submit the [`AuditEvent`](ravel_maintain::AuditEvent).
struct AuditCtx {
    sink: Arc<dyn QueryAuditSink>,
    tenant: TenantHash,
    now_ns: i64,
    query_text: String,
}

impl AuditCtx {
    /// Build the query-audit event for this statement at `status`. The window
    /// bounds are recorded as `0, 0`: a Flight statement carries no event-time
    /// window on the `DoGet` redemption path (the window a client sends via
    /// metadata is consumed at `GetFlightInfo` to resolve and pin the snapshot
    /// and is not carried in the ticket), so recording `0, 0` states "unknown"
    /// plainly rather than fabricating a range the request never used.
    fn event(&self, status: QueryStatus) -> ravel_maintain::AuditEvent {
        query_audit_event(
            &self.tenant,
            self.now_ns,
            &self.query_text,
            "sql",
            status,
            0,
            0,
        )
    }
}

/// The phase of an [`AuditedStream`].
enum AuditPhase {
    /// Passing batches through from the inner encoded stream.
    Streaming,
    /// The inner stream ended cleanly (terminal `None`); the audit event is in
    /// flight and its durability is being awaited before the client observes
    /// the stream's end.
    Flushing(Pin<Box<dyn Future<Output = ravel_maintain::Result<()>> + Send>>),
    /// The inner stream yielded an error item (terminal on a real transport:
    /// tonic's server-role `EncodeBody` ends the body on the first `Err` and
    /// never polls again). The audit event is in flight and its durability is
    /// being awaited before the client observes the error; once the flush
    /// resolves, the carried original stream error `Status` is yielded to the
    /// client (never the flush's own error -- see the poll impl). This mirrors
    /// `Flushing`'s await-before-yield discipline for the error path, so the
    /// audit is submitted and awaited inline rather than fire-and-forget from
    /// `Drop` (ADR-0062 §2a).
    FlushingError(
        Pin<Box<dyn Future<Output = ravel_maintain::Result<()>> + Send>>,
        Status,
    ),
    /// The stream is exhausted.
    Done,
}

/// Wraps a `DoGet` result stream so exactly one audit event is submitted at
/// stream *completion*, with the stream's actual terminal status, and its
/// durability is awaited before the client observes the stream's end (ADR-0062
/// §2a).
///
/// This closes the status-accuracy gap of auditing at stream construction: the
/// recorded status is `Ok` only when the inner stream ended with no error
/// item, `Error` when any error item was seen (a query that failed partway),
/// and `Error` on client cancellation (see the `Drop` impl). In
/// `audit_mode=required` a flush failure turns the stream's end into a trailing
/// `Unavailable` error so the client fails closed rather than believing an
/// unaudited query succeeded.
///
/// The error path is awaited inline just like the success path: a mid-stream
/// error is the terminal event on a real transport (tonic's server-role
/// `EncodeBody` stops polling after the first `Err`), so the `Error` audit is
/// submitted and its durability awaited before that error item is released to
/// the client, rather than being left to `Drop`'s detached best-effort task
/// (which would race the response). `Drop` remains only for the one path that
/// genuinely cannot await -- client cancellation, where no response is left to
/// gate.
struct AuditedStream {
    inner: DoGetStream,
    /// The audit context, taken when the audit begins (terminal event) or when
    /// the stream is dropped before completion. `None` once taken, so the audit
    /// fires at most once across both the poll path and `Drop`.
    ctx: Option<AuditCtx>,
    /// Set when the inner stream yielded an error item, so the terminal audit
    /// records `Error` rather than `Ok`.
    saw_error: bool,
    phase: AuditPhase,
}

impl Stream for AuditedStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `AuditedStream` is `Unpin` (every field is: a boxed stream, an
        // `Option`, a `bool`, and a phase whose only non-trivial variant holds
        // a boxed future), so it is safe to operate on `&mut Self`.
        let this = self.get_mut();
        loop {
            match &mut this.phase {
                AuditPhase::Done => return Poll::Ready(None),
                AuditPhase::Flushing(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        this.phase = AuditPhase::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(_)) => {
                        this.phase = AuditPhase::Done;
                        // Fail closed on an audit flush failure, unless the
                        // client already saw a real error item for this stream
                        // (then the failure is already signalled and a second
                        // error item would be noise).
                        if this.saw_error {
                            return Poll::Ready(None);
                        }
                        return Poll::Ready(Some(Err(Status::unavailable(AUDIT_UNAVAILABLE_MSG))));
                    }
                },
                AuditPhase::FlushingError(fut, carried) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    // The audit for the failed query is now durable. Yield the
                    // ORIGINAL stream error to the client, whether the flush
                    // itself succeeded or failed: the client is already being
                    // handed a real error, so the success-path fail-closed
                    // `Unavailable` substitution does not apply here. The
                    // carried `Status` is swapped out in place rather than
                    // destructured out of a re-matched phase, so no
                    // unreachable-arm panic is needed: panicking in a query
                    // path is never an acceptable failure mode (`start_pinned`
                    // takes the same rule). The placeholder left behind is
                    // never observed; the phase becomes `Done` immediately.
                    Poll::Ready(_) => {
                        let status = std::mem::replace(carried, Status::ok(""));
                        this.phase = AuditPhase::Done;
                        return Poll::Ready(Some(Err(status)));
                    }
                },
                AuditPhase::Streaming => match this.inner.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(batch))) => return Poll::Ready(Some(Ok(batch))),
                    Poll::Ready(Some(Err(status))) => {
                        // A mid-stream error is the true terminal event on a
                        // real transport (tonic's server-role `EncodeBody` ends
                        // the body on the first `Err` and never polls again), so
                        // this is where the query's outcome must be audited and
                        // its durability awaited -- not later from `Drop`'s
                        // detached, unawaited task, which races the response the
                        // client has already received (ADR-0062
                        // §2a). Follow the same await-before-yield discipline as
                        // the terminal-`None` success path: take `ctx`, submit
                        // the `Error` audit, and hold the original `status` in
                        // `FlushingError` to yield once the flush resolves.
                        this.saw_error = true;
                        let Some(ctx) = this.ctx.take() else {
                            // `ctx` already consumed (defensive: unreachable on
                            // a live poll path, since a terminal event is seen
                            // at most once). Nothing left to audit inline.
                            return Poll::Ready(Some(Err(status)));
                        };
                        let event = ctx.event(QueryStatus::Error);
                        let sink = Arc::clone(&ctx.sink);
                        this.phase = AuditPhase::FlushingError(
                            Box::pin(async move { sink.submit(event).await }),
                            status,
                        );
                    }
                    Poll::Ready(None) => {
                        // Terminal: submit the audit with the real outcome and
                        // await durability before the client observes the end.
                        // `ctx` is present exactly once (the pre-completion Drop
                        // path also takes it), so the audit fires at most once.
                        let Some(ctx) = this.ctx.take() else {
                            this.phase = AuditPhase::Done;
                            return Poll::Ready(None);
                        };
                        let status = if this.saw_error {
                            QueryStatus::Error
                        } else {
                            QueryStatus::Ok
                        };
                        let event = ctx.event(status);
                        let sink = Arc::clone(&ctx.sink);
                        this.phase =
                            AuditPhase::Flushing(Box::pin(async move { sink.submit(event).await }));
                    }
                },
            }
        }
    }
}

impl Drop for AuditedStream {
    fn drop(&mut self) {
        // If `ctx` is still present the stream was dropped before its terminal
        // event: the client cancelled (disconnected) mid-stream. Record that
        // outcome as `Error` so the trail never shows a false "ok" for a
        // cancelled query. This is the one audit path that cannot
        // await durability: `Drop` is synchronous and there is no response left
        // to gate -- the client is already gone -- so it is submitted
        // best-effort on a detached task rather than fire-and-forget on a live
        // response path. Once the terminal poll path has taken `ctx` (audit
        // submitted and awaited inline), this does nothing.
        if let Some(ctx) = self.ctx.take() {
            let event = ctx.event(QueryStatus::Error);
            let sink = ctx.sink;
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = sink.submit(event).await;
                });
            }
        }
    }
}

/// Wrap `inner` so exactly one audit event is submitted at stream completion
/// with the stream's actual terminal status, its durability awaited before the
/// client observes the end. See [`AuditedStream`].
pub(super) fn audited_stream(
    inner: DoGetStream,
    sink: Arc<dyn QueryAuditSink>,
    tenant: TenantHash,
    now_ns: i64,
    query_text: String,
) -> DoGetStream {
    Box::pin(AuditedStream {
        inner,
        ctx: Some(AuditCtx {
            sink,
            tenant,
            now_ns,
            query_text,
        }),
        saw_error: false,
        phase: AuditPhase::Streaming,
    })
}

/// Nanoseconds left before `deadline_ns`, as a duration. Never negative; the
/// caller has already rejected an expired ticket, so a zero here can only come
/// from a deadline reached between that check and this call.
fn remaining(now_ns: i64, deadline_ns: i64) -> Duration {
    let left = deadline_ns.saturating_sub(now_ns).max(0);
    Duration::from_nanos(u64::try_from(left).unwrap_or(u64::MAX))
}

/// Plan and start the query against the pinned snapshot, pulling the first
/// batch so the pre-emission retry can still fire.
///
/// Returns the first batch (if any) alongside the live stream. Pulling it here
/// is what makes "before the first batch has been emitted" a decidable
/// condition on a streaming transport: nothing has been handed to the encoder
/// yet, so a retry is still invisible to the client.
async fn start_pinned(
    executor: &SqlExecutor,
    tenant: TenantHash,
    snapshot: &Snapshot,
    sql: &str,
    distributed: Option<DistributedScan>,
    declared: &[crate::DeclaredColumn],
) -> Result<(Option<RecordBatch>, PinnedStream, QueryAccounting), SqlError> {
    // At most two passes: the original and the one retry the consistency
    // model allows. Both passes install the same distributed scan; it clones
    // cheaply (a slice `Vec` and an `Arc` client), so a retry re-plans over
    // the same fan-out rather than silently falling back to local.
    for attempt in 0..2u32 {
        match first_batch(
            executor,
            tenant,
            snapshot,
            sql,
            distributed.clone(),
            declared,
        )
        .await
        {
            Ok(started) => return Ok(started),
            Err(err) => match retry_decision(err.is_segment_not_found(), 0, attempt) {
                RetryDecision::RetryOnce => continue,
                RetryDecision::FailInvalidated => return Err(SqlError::SnapshotInvalidated),
                RetryDecision::Propagate => return Err(err),
            },
        }
    }

    // Unreachable: the loop either returns or `continue`s exactly once, and
    // the second pass always returns. A typed error rather than a panic,
    // because panicking in a query path is never an acceptable failure mode.
    Err(SqlError::Internal(
        "pinned retry loop exited without a result".to_string(),
    ))
}

/// One attempt: build the session over the pinned snapshot, plan, execute,
/// and pull the first batch.
async fn first_batch(
    executor: &SqlExecutor,
    tenant: TenantHash,
    snapshot: &Snapshot,
    sql: &str,
    distributed: Option<DistributedScan>,
    declared: &[crate::DeclaredColumn],
) -> Result<(Option<RecordBatch>, PinnedStream, QueryAccounting), SqlError> {
    // A fresh handle per attempt, matching `SqlExecutor::run`'s per-attempt
    // accounting so a retried attempt's counters never bleed in. Returned to
    // the caller so the successful attempt's cost is folded into `/metrics`
    // once the stream ends; `plan_pinned` clones it into the
    // execution stream, so its counters keep rising as batches are pulled.
    let accounting = QueryAccounting::new();
    let planned = executor
        .plan_pinned_distributed(
            tenant,
            snapshot.clone(),
            sql,
            &accounting,
            distributed,
            declared,
        )
        .await?;
    let mut stream = planned.execute().await?;
    match stream.next().await {
        None => Ok((None, stream, accounting)),
        Some(Ok(batch)) => Ok((Some(batch), stream, accounting)),
        Some(Err(err)) => Err(err),
    }
}

/// Unwrap the redacted `Status` the pipeline put inside the encoder's error
/// type, and give the encoder's own failures a fixed message.
fn flight_error_to_status(err: FlightError) -> Status {
    match err {
        FlightError::Tonic(status) => *status,
        other => {
            // An encoder failure is a server-side fault with arrow detail in
            // it; log it and hand the client a fixed string, the same rule
            // every other error takes here.
            tracing::warn!(error = %other, "failed to encode flight sql results");
            Status::internal("failed to encode query results")
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn remaining_never_goes_negative() {
        assert_eq!(remaining(100, 100), Duration::ZERO);
        assert_eq!(remaining(200, 100), Duration::ZERO);
        assert_eq!(remaining(100, 200), Duration::from_nanos(100));
        // An extreme span saturates rather than wrapping.
        assert!(remaining(i64::MIN, i64::MAX) > Duration::from_secs(1));
    }

    #[test]
    fn an_encoder_failure_becomes_a_fixed_internal_status() {
        let status = flight_error_to_status(FlightError::ProtocolError(
            "t/hash/metrics/l0/0/w.1.2.abc.rseg".to_string(),
        ));
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(
            !status.message().contains(".rseg"),
            "object keys must not reach the client"
        );
    }

    #[test]
    fn a_redacted_status_rides_through_the_encoder_error_unchanged() {
        let inner = Status::unavailable("storage temporarily unavailable");
        let status = flight_error_to_status(FlightError::Tonic(Box::new(inner)));
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "storage temporarily unavailable");
    }

    // -----------------------------------------------------------------
    // Stream-completion audit (ADR-0062 §2a)
    // -----------------------------------------------------------------

    use std::sync::Mutex;

    use ravel_maintain::{AuditEvent, MaintainError};

    const TENANT: TenantHash = TenantHash([7u8; 16]);

    /// Captures every submitted audit event and reports durability success.
    struct RecordingSink {
        events: Arc<Mutex<Vec<AuditEvent>>>,
    }

    #[async_trait::async_trait]
    impl QueryAuditSink for RecordingSink {
        async fn submit(&self, event: AuditEvent) -> Result<(), MaintainError> {
            self.events.lock().expect("lock").push(event);
            Ok(())
        }
    }

    /// Always fails its submission (an `audit_mode=required` flush failure).
    struct FailingSink;

    #[async_trait::async_trait]
    impl QueryAuditSink for FailingSink {
        async fn submit(&self, _event: AuditEvent) -> Result<(), MaintainError> {
            Err(MaintainError::AuditFlush("down".to_string()))
        }
    }

    fn inner_stream(items: Vec<Result<FlightData, Status>>) -> DoGetStream {
        Box::pin(futures::stream::iter(items))
    }

    /// Drain a stream the way a real transport consumes it: pull items until
    /// the first `Err`, then stop. tonic's server-role `EncodeBody` ends the
    /// response body on the first `Err` item and never polls the stream again,
    /// so a test that kept polling past an error would exercise a terminal
    /// `None` branch no live client ever reaches -- and would let an audit that
    /// only fires on that unreachable branch masquerade as covered.
    async fn drain(mut stream: DoGetStream) -> Vec<Result<FlightData, Status>> {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let is_err = item.is_err();
            out.push(item);
            if is_err {
                break;
            }
        }
        out
    }

    fn status_text(event: &AuditEvent) -> &str {
        // `query_audit_event` derives severity from status: INFO for ok, ERROR
        // for error. Asserting on it avoids naming the attr value type here.
        &event.severity_text
    }

    /// A clean stream (no error item) records exactly one `ok` audit event at
    /// completion.
    #[tokio::test]
    async fn a_clean_stream_records_one_ok_event() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink {
            events: Arc::clone(&events),
        });
        let inner = inner_stream(vec![Ok(FlightData::default())]);
        let stream = audited_stream(inner, sink, TENANT, 42, "SELECT 1".to_string());

        let items = drain(stream).await;
        assert_eq!(items.len(), 1, "the one data item passes through");
        assert!(items[0].is_ok());

        let guard = events.lock().expect("lock");
        assert_eq!(guard.len(), 1, "exactly one audit event at completion");
        assert_eq!(status_text(&guard[0]), "INFO", "a clean stream records ok");
    }

    /// The status-accuracy proof: a stream that fails partway records
    /// `error`, not the blind "started/ok" the construction-time audit would
    /// have written after the first batch pulled cleanly.
    #[tokio::test]
    async fn a_stream_that_fails_partway_records_error_not_ok() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink {
            events: Arc::clone(&events),
        });
        // First batch succeeds (a construction-time audit would record "ok"
        // here), then the stream errors partway.
        let inner = inner_stream(vec![
            Ok(FlightData::default()),
            Err(Status::internal("boom")),
        ]);
        let stream = audited_stream(inner, sink, TENANT, 42, "SELECT 1".to_string());

        let items = drain(stream).await;
        assert_eq!(items.len(), 2);
        assert!(items[0].is_ok(), "first batch reached the client");
        assert!(items[1].is_err(), "the mid-stream error reached the client");

        let guard = events.lock().expect("lock");
        assert_eq!(guard.len(), 1, "exactly one audit event");
        assert_eq!(
            status_text(&guard[0]),
            "ERROR",
            "a stream that failed partway must record error, not a blind ok"
        );
    }

    /// The regression guard for the mid-stream-error fire-and-forget bug: under
    /// real-transport consumption (poll until the first `Err`, then stop -- the
    /// way tonic's server-role `EncodeBody` does), the `Error` audit must be
    /// submitted AND awaited by the time the client observes that error item,
    /// not deferred to `Drop`'s detached task.
    ///
    /// This fails on the unfixed code: there the error item was returned from
    /// `poll_next` immediately, with the audit only ever submitted later from
    /// `Drop`'s unawaited `handle.spawn`, so `events` is still empty at the
    /// moment the error is observed. The stream is deliberately NOT dropped and
    /// no yield point is crossed before the assertion, so a racing `Drop`
    /// submission cannot make it pass spuriously.
    #[tokio::test]
    async fn mid_stream_error_audit_is_awaited_under_tonic_consumption() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink {
            events: Arc::clone(&events),
        });
        let inner = inner_stream(vec![
            Ok(FlightData::default()),
            Err(Status::internal("boom")),
        ]);
        let mut stream = audited_stream(inner, sink, TENANT, 42, "SELECT 1".to_string());

        // Consume as tonic's server role does: the first batch, then the error.
        let first = stream.next().await;
        assert!(
            first.is_some_and(|i| i.is_ok()),
            "first batch reached client"
        );
        let err = stream.next().await;
        assert!(
            err.is_some_and(|i| i.is_err()),
            "the mid-stream error reached the client"
        );

        // The moment the client observes the error, the audit must already be
        // durable. Check synchronously, without dropping the stream or awaiting
        // anything, so this cannot be satisfied by `Drop`'s detached task.
        let guard = events.lock().expect("lock");
        assert_eq!(
            guard.len(),
            1,
            "the Error audit must be submitted and awaited before the error item is released"
        );
        assert_eq!(
            status_text(&guard[0]),
            "ERROR",
            "the audit for a failed query records error"
        );
    }

    /// The error path yields the ORIGINAL stream error even when the audit
    /// flush itself fails. The clean-stream fail-closed substitution does not
    /// apply here: the client is already being handed a real error, and
    /// replacing it with `Unavailable` would hide why the query actually
    /// failed behind an audit-infrastructure message.
    #[tokio::test]
    async fn a_failed_flush_on_the_error_path_still_yields_the_original_error() {
        let inner = inner_stream(vec![
            Ok(FlightData::default()),
            Err(Status::internal("boom")),
        ]);
        let stream = audited_stream(
            inner,
            Arc::new(FailingSink),
            TENANT,
            42,
            "SELECT 1".to_string(),
        );

        let items = drain(stream).await;
        assert_eq!(items.len(), 2, "one data item, then the original error");
        assert!(items[0].is_ok());
        let err = items[1].as_ref().expect_err("the mid-stream error");
        assert_eq!(
            err.code(),
            tonic::Code::Internal,
            "the original error code survives a failed audit flush"
        );
        assert_eq!(
            err.message(),
            "boom",
            "the original error message survives a failed audit flush"
        );
    }

    /// Fail-closed: when the audit flush fails in `required` mode, a clean
    /// stream's end is turned into a trailing `Unavailable` error so the client
    /// never believes an unaudited query succeeded.
    #[tokio::test]
    async fn a_required_audit_flush_failure_fails_the_stream_closed() {
        let inner = inner_stream(vec![Ok(FlightData::default())]);
        let stream = audited_stream(
            inner,
            Arc::new(FailingSink),
            TENANT,
            42,
            "SELECT 1".to_string(),
        );

        let items = drain(stream).await;
        assert_eq!(items.len(), 2, "one data item, then a trailing audit error");
        assert!(items[0].is_ok());
        let err = items[1].as_ref().expect_err("trailing error");
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    /// A client that cancels (drops the stream) mid-flight records `error`
    /// best-effort, so the trail never shows a false "ok" for a cancelled
    /// query. This is the one audit path that cannot await durability (Drop is
    /// synchronous and no response is pending); it is submitted on a detached
    /// task, so the assertion allows it a moment to run.
    #[tokio::test]
    async fn a_cancelled_stream_records_error_best_effort() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink {
            events: Arc::clone(&events),
        });
        // A stream that yields one batch then would continue; we drop it after
        // the first item, before its terminal event.
        let inner = inner_stream(vec![Ok(FlightData::default()), Ok(FlightData::default())]);
        let mut stream = audited_stream(inner, sink, TENANT, 42, "SELECT 1".to_string());
        let first = stream.next().await;
        assert!(first.is_some_and(|i| i.is_ok()), "pulled the first batch");
        drop(stream);

        // The Drop path spawns the cancellation audit; give it a moment.
        for _ in 0..50 {
            if !events.lock().expect("lock").is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let guard = events.lock().expect("lock");
        assert_eq!(
            guard.len(),
            1,
            "a cancelled stream records exactly one event"
        );
        assert_eq!(
            status_text(&guard[0]),
            "ERROR",
            "a cancelled stream records error, never a blind ok"
        );
    }
}
