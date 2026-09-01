//! Wall-clock bound on a Flight SQL `DoGet` stream so a stalled consumer cannot
//! pin a query-concurrency slot indefinitely.
//!
//! # The problem
//!
//! `do_get_statement` (ravel-sql) acquires a fleet-global [`QueryPermit`]
//! (ADR-0061 decision 2) and moves it into the returned result stream's
//! `RecordOnStreamEnd` guard, so the slot is held until the stream ends or the
//! client drops it. A client that opens the `DoGet` and then reads *nothing*
//! stalls the stream forever at the transport layer: the HTTP/2 flow-control
//! window sits at 0, so the server never gets send capacity, the result stream
//! is never polled to completion, and the permit is never released. One idle
//! consumer can therefore hold a slot until its ticket deadline, and enough of
//! them starve the whole fleet ceiling.
//!
//! # Why a stream combinator does not work (the first attempt)
//!
//! The obvious fix -- wrap the response body in a `Stream` that arms a
//! `tokio::time::Sleep` and polls it inside `poll_next` -- is a no-op in exactly
//! the scenario it targets. tonic's server-role `EncodeBody::poll_frame` drives
//! the response body synchronously from the hyper connection task, and hyper's
//! server body pump loops on `poll_capacity` *before* it calls `poll_frame`
//! again once a frame is awaiting send capacity. A non-reading client holds the
//! window at 0, so hyper parks in the capacity branch and never calls
//! `poll_frame` again: the wrapper's `Sleep` is never re-polled, and its waker
//! wake just re-checks capacity (still 0) and re-parks. Any deadline evaluated
//! only via the body's own `poll_next`/`poll_frame` is dead precisely when the
//! client reads nothing. The deadline must be driven by a task the tokio runtime
//! schedules independently of hyper's send pump.
//!
//! # The design: a spawned driver plus a bounded channel
//!
//! [`DeadlineBoundedFlightService`] wraps the whole [`FlightService`] and touches
//! only `do_get`; every other RPC is delegated verbatim. For each `DoGet`, the
//! permit-holding inner result stream is moved into a [`tokio::spawn`]ed driver
//! task that forwards its items into a bounded [`tokio::sync::mpsc`] channel; the
//! Flight response body is that channel's receiver. The driver runs a
//! `tokio::select!` racing
//!
//!  a. forwarding the next inner item (`tx.send(..).await`, which applies
//!     back-pressure when the non-reading client lets the channel fill), against
//!  b. a wall-clock `tokio::time::sleep` of `max_deadline`.
//!
//! Because the driver is polled by the runtime and not by hyper's send window,
//! the sleep fires even while the client reads nothing. On the deadline arm the
//! driver stops and drops the inner stream, which drops its `RecordOnStreamEnd`
//! guard and releases the `QueryPermit`. That permit release is the goal, and it
//! happens on every deadline exit regardless of what the client observes.
//!
//! What the client observes is *not* uniform, because the deadline arm only
//! attempts a non-blocking `try_send` of the typed error (blocking for channel
//! room would wait out the very stall this exists to break):
//!
//!  - A client still draining the stream leaves the cap-1 channel empty, the
//!    `try_send` succeeds, and it receives the typed `Unavailable` error as the
//!    stream's terminal item. This is the case a slow-but-progressing consumer
//!    that crosses the ceiling lands in.
//!  - A fully stalled (non-reading) client is stalled *because* the cap-1 channel
//!    is full -- that is what parks the driver's forwarding `send` -- so the
//!    `try_send` fails and the error is dropped. Such a client, if it later reads,
//!    gets the one buffered frame and then a bare stream close with no gRPC error
//!    status. It cannot distinguish that from a normal end of results.
//!
//! So the typed error is best-effort signalling, not a guarantee; only the permit
//! release is guaranteed. Improving the stalled client's signal would need an
//! out-of-band path to the trailers rather than a slot in the same full channel.
//!
//! # Not a shorter timeout
//!
//! `max_deadline` is the deployment's own server ceiling, the same bound the
//! per-ticket effective deadline is already clamped against
//! (`effective = min(ticket.deadline_ns, now + max_deadline)`,
//! crate::flight::stream). The inner stream already fails `SnapshotInvalidated`
//! at that effective deadline on every batch it hands out, so a consumer that is
//! actually making progress hits the inner check at or before this outer bound
//! and ends naturally. This outer bound only bites the case the inner check
//! cannot reach: a stream suspended mid-send because the client is not reading,
//! where no batch is being handed out for the per-batch check to run. It never
//! shortens a legitimately slow-but-progressing consumer's budget.

use std::pin::Pin;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, PollInfo,
    SchemaResult, Ticket,
};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};

/// The `DoGet` response body this wrapper produces: the bounded channel's
/// receiver, boxed to the same shape tonic's blanket Flight SQL impl uses.
type DeadlineDoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

/// How many forwarded items the driver-to-response channel buffers.
///
/// One. A single slot is enough to keep a *reading* client fed (the driver
/// stages the next item while the client drains the current one), and small
/// enough that a *non-reading* client fills it after a single un-drained item,
/// so `tx.send` on the driver blocks promptly and the deadline race becomes the
/// only thing keeping the task alive. A larger buffer would let the driver keep
/// pulling the inner stream -- doing real scan work under the held permit -- for
/// several items before back-pressure engages, weakening exactly the property
/// this bound exists to enforce.
const DOGET_CHANNEL_CAPACITY: usize = 1;

/// Wraps a [`FlightService`] so every `DoGet` response body is bounded by a
/// wall-clock deadline driven off the hyper send pump. All other RPCs delegate
/// to `inner` unchanged. See the module docs.
pub struct DeadlineBoundedFlightService<S> {
    inner: S,
    /// The server ceiling every `DoGet` stream is bounded by, reused from
    /// `SqlState::max_deadline` so this bound and the per-ticket effective
    /// deadline share one source (crate::flight::stream).
    max_deadline: Duration,
}

impl<S> DeadlineBoundedFlightService<S> {
    /// Wrap `inner`, bounding each `DoGet` stream by `max_deadline`.
    pub fn new(inner: S, max_deadline: Duration) -> Self {
        DeadlineBoundedFlightService {
            inner,
            max_deadline,
        }
    }
}

/// The deadline error the driver *attempts* to hand the client (see
/// [`deadline_reached`]: it is a `try_send`, so a stalled client whose full
/// channel caused the deadline never receives it). When it is delivered it is
/// byte-identical to how ravel-sql maps
/// [`SqlError::SnapshotInvalidated`](ravel_sql) through its own Flight status
/// boundary: a `Unavailable` with the fixed redacted message. A `DoGet` that
/// crossed the server ceiling is the same class of event as a pin whose GC
/// protection window closed mid-stream, so it presents identically and a client
/// cannot tell the two apart (nor should it: both mean "retry, the snapshot is
/// no longer being served").
fn deadline_status() -> Status {
    Status::new(tonic::Code::Unavailable, ravel_sql::MSG_UNAVAILABLE)
}

/// Own `inner` and forward its items into `tx` until the inner stream ends, the
/// client disconnects, or `max_deadline` elapses.
///
/// This runs as a spawned task, so its `sleep` is polled by the tokio runtime
/// independently of hyper's send pump -- the property the combinator approach
/// lacked. On any exit the inner stream is dropped, which releases the
/// `QueryPermit` its `RecordOnStreamEnd` guard holds.
async fn drive<St>(inner: St, tx: mpsc::Sender<Result<FlightData, Status>>, max_deadline: Duration)
where
    St: Stream<Item = Result<FlightData, Status>> + Send + 'static,
{
    // Pin the inner stream on the stack so it can be polled without requiring
    // `Unpin`; it is dropped when this function returns, releasing the permit.
    let mut inner = std::pin::pin!(inner);
    let sleep = tokio::time::sleep(max_deadline);
    tokio::pin!(sleep);

    loop {
        // Phase 1: pull the next item, racing the deadline. `biased` polls the
        // deadline first each iteration so a stream that is always immediately
        // ready can never starve it.
        let item = tokio::select! {
            biased;
            () = &mut sleep => return deadline_reached(&tx),
            item = inner.next() => item,
        };
        let Some(item) = item else {
            // The inner stream ended on its own (completed, or failed its own
            // per-batch deadline check). Dropping `tx` closes the channel, which
            // ends the client's response.
            return;
        };

        // Phase 2: forward the item, racing the deadline. The `send` -- not the
        // `next` above -- is where a non-reading client stalls: it fills the
        // channel and `tx.send` stays `Pending`. Racing it against `sleep` here
        // (rather than awaiting it outside the `select!`) is the whole point,
        // and is exactly what a plain `tx.send(item).await` would get wrong:
        // that await would park with no timer in the race, so the deadline could
        // never fire during the stall.
        tokio::select! {
            biased;
            () = &mut sleep => return deadline_reached(&tx),
            result = tx.send(item) => {
                // A `send` error means the receiver was dropped (the client
                // disconnected); stop and release the permit rather than spin.
                if result.is_err() {
                    return;
                }
            }
        }
    }
}

/// The deadline arm shared by both `select!`s in [`drive`]: offer the client the
/// typed error if the channel has room right now -- never block waiting for
/// room, since a full channel is the very stall this deadline exists to break --
/// then let the caller `return`, dropping the inner stream and releasing the
/// permit.
///
/// The `try_send` result is deliberately discarded, and it does fail in a real
/// stall: a non-reading client's un-drained item occupies the single slot, so the
/// error is dropped and that client sees a bare stream close instead of a status
/// (module docs, "What the client observes"). A client still draining gets the
/// error. The permit release below does not depend on either outcome.
fn deadline_reached(tx: &mpsc::Sender<Result<FlightData, Status>>) {
    let _ = tx.try_send(Err(deadline_status()));
}

#[tonic::async_trait]
impl<S> FlightService for DeadlineBoundedFlightService<S>
where
    S: FlightService,
{
    type HandshakeStream = S::HandshakeStream;
    type ListFlightsStream = S::ListFlightsStream;
    type DoGetStream = DeadlineDoGetStream;
    type DoPutStream = S::DoPutStream;
    type DoExchangeStream = S::DoExchangeStream;
    type DoActionStream = S::DoActionStream;
    type ListActionsStream = S::ListActionsStream;

    /// The only wrapped method: run the inner `do_get`, then move its
    /// permit-holding body into a spawned driver bounded by `max_deadline` and
    /// return the channel receiver as the response body. Metadata and extensions
    /// ride through `into_parts`/`from_parts` unchanged.
    ///
    /// An error from the inner `do_get` (bad ticket, cross-tenant redemption,
    /// admission rejection) is returned before anything is spawned, so those
    /// paths are untouched.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let response = self.inner.do_get(request).await?;
        let (metadata, body, extensions) = response.into_parts();

        let (tx, rx) = mpsc::channel::<Result<FlightData, Status>>(DOGET_CHANNEL_CAPACITY);
        // The driver owns the permit-holding inner stream from here on. It is a
        // detached task; it terminates on inner-stream end, client disconnect
        // (the `tx.send` error arm), or the deadline, so it never leaks.
        tokio::spawn(drive(body, tx, self.max_deadline));

        let stream: Self::DoGetStream =
            Box::pin(futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            }));
        Ok(Response::from_parts(metadata, stream, extensions))
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.inner.handshake(request).await
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.inner.list_flights(request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.inner.get_flight_info(request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.inner.poll_flight_info(request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.inner.get_schema(request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.inner.do_put(request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.inner.do_exchange(request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.inner.do_action(request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.inner.list_actions(request).await
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// An inner result stream that sets a flag when it is dropped, standing in
    /// for the `QueryPermit` release that dropping `ravel-sql`'s permit-holding
    /// stream performs. `remaining == None` means it never ends on its own, so
    /// only the deadline or a disconnect can end the driver.
    struct DropFlagStream {
        dropped: Arc<AtomicBool>,
        remaining: Option<usize>,
    }

    impl Stream for DropFlagStream {
        type Item = Result<FlightData, Status>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            match &mut self.remaining {
                Some(0) => std::task::Poll::Ready(None),
                Some(n) => {
                    *n -= 1;
                    std::task::Poll::Ready(Some(Ok(FlightData::default())))
                }
                None => std::task::Poll::Ready(Some(Ok(FlightData::default()))),
            }
        }
    }

    impl Drop for DropFlagStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    /// The deadline releases the inner stream (its permit) even when the
    /// receiver never reads: the driver is parked on a full-channel `send`, and
    /// the runtime-scheduled sleep is what ends it. This is the driver-layer
    /// analogue of the end-to-end stalled-client test; it cannot see hyper's
    /// send pump, so the transport-level proof lives in
    /// `tests/flight_deadline_e2e.rs`.
    #[tokio::test(start_paused = true)]
    async fn deadline_releases_inner_when_receiver_never_reads() {
        let dropped = Arc::new(AtomicBool::new(false));
        let inner = DropFlagStream {
            dropped: Arc::clone(&dropped),
            remaining: None,
        };
        let (tx, _rx) = mpsc::channel::<Result<FlightData, Status>>(DOGET_CHANNEL_CAPACITY);
        let deadline = Duration::from_secs(30);
        // `_rx` is held but never polled: the stall this bound targets.
        let handle = tokio::spawn(drive(inner, tx, deadline));

        // Let the driver run once so it parks on the full-channel `send` with
        // its `sleep` already armed (biased `select!` polls the sleep first, so
        // one poll registers it). Only then advance virtual time past the
        // deadline: advancing before the sleep exists would leave it armed for a
        // now-later deadline and never fire.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(deadline + Duration::from_secs(1)).await;
        handle.await.expect("driver task joins");

        assert!(
            dropped.load(Ordering::SeqCst),
            "the inner stream (and its permit) is dropped by the deadline"
        );
    }

    /// What a fully stalled client actually observes after the deadline, and the
    /// reason the module docs do not promise it the typed error: the one item it
    /// never drained is still occupying the cap-1 channel when the deadline arm
    /// runs, so `try_send` of the error fails and is discarded. Reading the
    /// channel afterwards yields that buffered frame and then a close, with no
    /// status anywhere. The permit release (asserted above) is unaffected.
    #[tokio::test(start_paused = true)]
    async fn stalled_receiver_gets_the_buffered_frame_then_a_bare_close() {
        let dropped = Arc::new(AtomicBool::new(false));
        let inner = DropFlagStream {
            dropped: Arc::clone(&dropped),
            remaining: None,
        };
        let (tx, mut rx) = mpsc::channel::<Result<FlightData, Status>>(DOGET_CHANNEL_CAPACITY);
        let deadline = Duration::from_secs(30);
        let handle = tokio::spawn(drive(inner, tx, deadline));

        // Same shape as the test above: let the driver fill the channel and park
        // on `send` with its sleep armed, then cross the deadline.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(deadline + Duration::from_secs(1)).await;
        handle.await.expect("driver task joins");
        assert!(
            dropped.load(Ordering::SeqCst),
            "the permit is still released"
        );

        // Only now does this client read.
        let first = rx
            .recv()
            .await
            .expect("the one buffered frame is still there");
        assert!(first.is_ok(), "the buffered item is a frame, not the error");
        assert!(
            rx.recv().await.is_none(),
            "the channel closes with no status: the deadline error's try_send lost \
             the single slot to the un-drained frame"
        );
    }

    /// A client disconnect (receiver dropped) makes the forwarding `send` fail,
    /// which stops the driver and drops the inner stream rather than spinning or
    /// leaking the permit until the deadline.
    #[tokio::test(start_paused = true)]
    async fn client_disconnect_stops_driver_and_releases_inner() {
        let dropped = Arc::new(AtomicBool::new(false));
        let inner = DropFlagStream {
            dropped: Arc::clone(&dropped),
            remaining: None,
        };
        let (tx, rx) = mpsc::channel::<Result<FlightData, Status>>(DOGET_CHANNEL_CAPACITY);
        drop(rx); // the client is gone before any item is forwarded
        let handle = tokio::spawn(drive(inner, tx, Duration::from_secs(30)));

        // No time is advanced: the driver must exit on the `send` error, not by
        // waiting out the deadline.
        handle.await.expect("driver task joins");
        assert!(
            dropped.load(Ordering::SeqCst),
            "a disconnected client releases the permit immediately, not at the deadline"
        );
    }

    /// A finite stream drained by a reading receiver delivers every item and no
    /// deadline error, then ends and drops the inner stream.
    #[tokio::test(start_paused = true)]
    async fn progressing_stream_delivers_all_items_without_deadline_error() {
        let dropped = Arc::new(AtomicBool::new(false));
        let inner = DropFlagStream {
            dropped: Arc::clone(&dropped),
            remaining: Some(3),
        };
        let (tx, mut rx) = mpsc::channel::<Result<FlightData, Status>>(DOGET_CHANNEL_CAPACITY);
        let handle = tokio::spawn(drive(inner, tx, Duration::from_secs(30)));

        let mut count = 0;
        while let Some(item) = rx.recv().await {
            item.expect("no deadline error on a drained stream");
            count += 1;
        }
        handle.await.expect("driver task joins");

        assert_eq!(count, 3, "every item is delivered");
        assert!(dropped.load(Ordering::SeqCst), "stream end drops the inner");
    }
}
