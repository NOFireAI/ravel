//! `DoGet`: execute a redeemed ticket against its pinned snapshot and stream
//! the result as `FlightData`.
//!
//! # The retry contract on a streaming transport (review F9, plan Phase C)
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
//! today (issue #186). That effective deadline is checked once before
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
//! the way down (crate::memory, review F13). A second release mechanism here
//! would double-count against the tenant budget.

use std::time::Duration;

use arrow_flight::FlightData;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use datafusion::arrow::array::RecordBatch;
use futures::{Stream, StreamExt};
use ravel_catalog::Snapshot;
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use tonic::Status;

use crate::error::SqlError;
use crate::executor::{PinnedStream, RetryDecision, SqlExecutor, retry_decision};
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
/// already rejected a ticket whose embedded tenant disagrees with it.
pub(super) async fn statement_stream(
    executor: &SqlExecutor,
    clock: ClockRef,
    tenant: TenantHash,
    ticket: FlightTicket,
    config: &FlightSqlConfig,
) -> Result<DoGetStream, Status> {
    // Re-run the security gate on redemption. The statement is carried in
    // bytes a client holds, and the gate is cheap; running it again means a
    // tampered or replayed ticket cannot reach the planner even if it somehow
    // survived the MAC.
    validate(&ticket.statement).map_err(|err| status_from_sql(&SqlError::from(err), tenant))?;

    let now_ns = clock.now_ns();
    // The ticket's own deadline_ns is client-supplied and may only shorten
    // the effective budget below what this deployment's config would mint
    // today, never lengthen it (issue #186).
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
        start_pinned(executor, tenant, &snapshot, &ticket.statement),
    )
    .await
    .unwrap_or_else(|_| {
        Err(SqlError::DeadlineExceeded {
            millis: u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
        })
    });

    let (first, stream) = started.map_err(|err| status_from_sql(&err, tenant))?;
    let schema = stream.schema();

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
        .map(|item| item.map_err(flight_error_to_status));

    Ok(Box::pin(encoded))
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
) -> Result<(Option<RecordBatch>, PinnedStream), SqlError> {
    // At most two passes: the original and the one retry the consistency
    // model allows.
    for attempt in 0..2u32 {
        match first_batch(executor, tenant, snapshot, sql).await {
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
) -> Result<(Option<RecordBatch>, PinnedStream), SqlError> {
    // A fresh handle per attempt, matching `SqlExecutor::run`'s per-attempt
    // accounting: DoGet's own execution accounting (crate::flight::service
    // covers only its own RPC, a documented gap, see the comment there).
    let accounting = QueryAccounting::new();
    let planned = executor
        .plan_pinned(tenant, snapshot.clone(), sql, &accounting)
        .await?;
    let mut stream = planned.execute().await?;
    match stream.next().await {
        None => Ok((None, stream)),
        Some(Ok(batch)) => Ok((Some(batch), stream)),
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
}
