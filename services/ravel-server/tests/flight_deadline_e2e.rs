//! End-to-end proof that a stalled `DoGet` releases its query-concurrency
//! permit by the server deadline, over the *real* tonic + hyper transport.
//!
//! # Why this must be an end-to-end test
//!
//! The bug this guards against only exists at the transport layer. tonic's
//! server-role `EncodeBody` drives the response body synchronously from the
//! hyper connection task, and hyper parks in `poll_capacity` -- never calling
//! the body's `poll_frame` again -- once a frame is awaiting HTTP/2 send
//! capacity and a non-reading client is holding the flow-control window at 0. A
//! deadline evaluated inside the body's own `poll` is therefore never re-polled
//! during the exact stall it targets, so the first attempt at this fix (a stream
//! combinator over the response body) was a no-op in production while passing
//! its own `poll!`-driven unit tests. A test that drives the stream directly
//! cannot catch that; only one that runs the bytes through tonic + hyper can.
//!
//! So these tests stand up a real `FlightService` on a loopback tonic server and
//! drive it with a real `FlightServiceClient`. The service is a minimal mock
//! whose `do_get` holds a real [`QueryPermit`] from a real
//! [`QueryAdmissionController`] and then streams frames forever: exactly the
//! shape of `ravel-sql`'s permit-holding `DoGet` stream, minus the SQL executor
//! this bound is indifferent to. What is proven is the property the combinator
//! lacked: with the client reading nothing, the permit is still released by the
//! deadline.
//!
//! `stalled_doget_releases_permit_by_deadline_end_to_end` fails against a
//! combinator-only implementation: there the deadline `Sleep` is polled only
//! from the body, which hyper stops polling at window 0, so the permit is never
//! released and `in_flight()` stays pinned at 1.
//!
//! # How to run these (they are easy to skip by accident)
//!
//! ```text
//! cargo test -p ravel-server --features flight-sql --test flight_deadline_e2e
//! ```
//!
//! Both halves of that command matter, and each has silently run zero of these
//! tests before:
//!
//!  - `--features flight-sql` is required. This file is `#![cfg(feature =
//!    "flight-sql")]` and the feature is off by default, so without it the
//!    integration binary compiles to an empty test list and reports success.
//!  - `--test flight_deadline_e2e` (or any filter that matches the *binary*
//!    name) is required for a filter-based run. A bare substring filter such as
//!    `cargo test -p ravel-server flight_deadline` matches only the lib unit
//!    tests in `src/flight_deadline.rs`: an integration test's name path is
//!    `<fn>`, not `<binary>::<fn>`, so neither `stalled_doget_..` nor
//!    `slow_..` nor `bound_fires_..` contains the string `flight_deadline`.
//!
//! Two harness paths do run this binary without any filter, and are the reason
//! it is not skippable in practice:
//!
//!  - `scripts/gates.sh` (workspace mode, and scoped mode naming `ravel-server`)
//!    runs a `flight-sql` feature lane: `cargo nextest run --locked -p
//!    ravel-server -p ravel-sql --features flight-sql`, unfiltered. CI's
//!    `flight-sql` job mirrors it.
//!  - `scripts/affected-tests.sh -p ravel-server` does **not**: it passes no
//!    `--features`, so it builds this binary with `flight-sql` off and runs 0
//!    tests from it (verified: `--features flight-sql` lists 3 tests here, the
//!    default feature set lists 0). Executor-side runs of that script are not
//!    evidence about this file; the `gates.sh` flight-sql lane is.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt};
use ravel_query::{QueryAdmissionController, QueryConcurrencyLimit, QueryPermit};
use ravel_server::flight_deadline::DeadlineBoundedFlightService;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, Streaming};

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// A result stream that holds a [`QueryPermit`] for its whole lifetime and emits
/// `remaining` frames of `frame_bytes` each, waiting `frame_delay` before each
/// one. Dropping it releases the permit, mirroring `ravel-sql`'s
/// `RecordOnStreamEnd { _permit }` guard.
///
/// `frame_delay` is what makes a total-timeout test deterministic rather than a
/// race with the scheduler: a zero-delay stream of a few small frames completes
/// inside a single scheduler burst, so even a zero deadline loses to it. With a
/// real per-frame sleep, `frames * frame_delay` is a hard lower bound on how long
/// the stream needs, and a deadline below that bound must fire mid-stream.
struct PermitStream {
    _permit: QueryPermit,
    remaining: usize,
    frame_bytes: usize,
    frame_delay: Duration,
    /// The in-progress `frame_delay` sleep, armed on the poll that starts a frame
    /// and cleared when it fires. Held across polls so the waker registration
    /// survives; `None` when `frame_delay` is zero or between frames.
    delay: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Stream for PermitStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        if !self.frame_delay.is_zero() {
            let delay = self.frame_delay;
            let sleep = self
                .delay
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(delay)));
            match sleep.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => self.delay = None,
            }
        }
        self.remaining -= 1;
        // A frame large enough that a few of them exhaust the default HTTP/2
        // window, so a non-reading client stalls the send pump quickly.
        let body = vec![0u8; self.frame_bytes];
        Poll::Ready(Some(Ok(FlightData {
            data_body: body.into(),
            ..FlightData::default()
        })))
    }
}

/// A minimal Flight service whose `do_get` acquires a real permit and returns a
/// [`PermitStream`]. Every other method is unimplemented; the deadline wrapper
/// only touches `do_get`.
struct PermitHoldingService {
    controller: Arc<QueryAdmissionController>,
    frames: usize,
    frame_bytes: usize,
    /// Per-frame delay handed to the [`PermitStream`]; `Duration::ZERO` for a
    /// stream that is always immediately ready.
    frame_delay: Duration,
}

#[tonic::async_trait]
impl FlightService for PermitHoldingService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let permit = self
            .controller
            .try_admit()
            .map_err(|_| Status::resource_exhausted("ceiling reached"))?;
        let stream = PermitStream {
            _permit: permit,
            remaining: self.frames,
            frame_bytes: self.frame_bytes,
            frame_delay: self.frame_delay,
            delay: None,
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

/// A running loopback tonic server carrying the deadline-wrapped mock service.
struct Server {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(service: PermitHoldingService, max_deadline: Duration) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = oneshot::channel::<()>();
        let wrapped =
            FlightServiceServer::new(DeadlineBoundedFlightService::new(service, max_deadline));
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(wrapped)
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await
                .expect("serve");
        });
        Server {
            addr,
            shutdown: tx,
            task,
        }
    }

    async fn client(&self) -> FlightServiceClient<tonic::transport::Channel> {
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", self.addr))
            .expect("valid endpoint uri")
            .connect()
            .await
            .expect("connect");
        FlightServiceClient::new(channel)
    }

    async fn stop(self) {
        // Signal graceful shutdown, but do not wait it out: the stalled-client
        // test deliberately leaves a connection open with an un-read stream, so
        // a graceful wait would hang. Abort the task to reclaim it promptly.
        let _ = self.shutdown.send(());
        self.task.abort();
        let _ = self.task.await;
    }
}

/// Test 1: a client that opens `DoGet` and never reads has its permit released
/// by the deadline, over the real transport, and a subsequent admission that the
/// held permit would have blocked then succeeds.
///
/// Flip-to-fail (verified): replace the phase-2 `tokio::select!` in
/// `flight_deadline::drive` -- the one racing `result = tx.send(item)` against
/// `() = &mut sleep` -- with a plain `tx.send(item).await`. That is the original
/// bug shape: the forwarding send then parks with no timer in the race, so the
/// deadline can never fire while the client stalls the channel, the permit is
/// never released, and this test's "permit is released by the deadline"
/// assertion fails (in ~10s, not a hang). A combinator over the response body
/// fails the same assertion for the same reason.
#[tokio::test]
async fn stalled_doget_releases_permit_by_deadline_end_to_end() {
    // Ceiling of 1: once the stalled DoGet holds the permit, no other query can
    // be admitted until it is released. That is the starvation this bound ends.
    let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(1));
    // A short real deadline: the driver's sleep is a real tokio timer, so it
    // fires in real time even though the client reads nothing. This avoids the
    // ordering hazard of pausing time before the spawned driver has registered
    // its sleep, and a short bound keeps the test fast without waiting out a
    // production-length ceiling. The driver still uses a `tokio::time::sleep`,
    // never `SystemTime::now`.
    let max_deadline = Duration::from_millis(500);
    let service = PermitHoldingService {
        controller: Arc::clone(&controller),
        // Effectively infinite: the stream never ends on its own, so only the
        // deadline (or a disconnect) can release the permit.
        frames: usize::MAX,
        frame_bytes: 16 * 1024,
        // Always immediately ready: the stall under test is the send side (the
        // client's zero flow-control window), not the producer.
        frame_delay: Duration::ZERO,
    };
    let server = Server::start(service, max_deadline).await;
    let mut client = server.client().await;

    assert_eq!(controller.in_flight(), 0, "no query in flight before DoGet");

    // Open the stream but never read from it. `do_get` resolves once response
    // headers arrive, by which point the server has acquired the permit.
    let response = client
        .do_get(Request::new(Ticket::new("stall")))
        .await
        .expect("do_get headers");
    let _body = response.into_inner(); // deliberately never polled

    assert_eq!(
        controller.in_flight(),
        1,
        "the stalled DoGet holds the concurrency permit"
    );
    assert!(
        controller.try_admit().is_err(),
        "the ceiling is full while the stalled stream holds its slot"
    );

    // The driver's sleep fires ~max_deadline later, drops the inner
    // PermitStream, and releases the permit. Poll for the release well past the
    // deadline; a combinator-only implementation never releases here because
    // hyper stops polling the body at window 0, so this loop times out and the
    // assertion fails -- which is the point.
    let mut released = false;
    for _ in 0..400 {
        if controller.in_flight() == 0 {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        released,
        "the permit is released by the deadline even though the client read nothing"
    );

    // The slot the stalled stream held is now free: an admission that was
    // blocked above succeeds.
    let permit = controller
        .try_admit()
        .expect("a slot is free once the stalled permit is released");
    drop(permit);

    server.stop().await;
}

/// Test 2 (no-spurious-abort): a consumer that reads every item receives the full
/// finite stream with no deadline error, and its permit is released by stream end.
/// The wrapper must be transparent to a stream that is progressing.
///
/// This test is **not** a total-timeout guard, despite what an earlier version of
/// this comment claimed. It cannot be: the producer here is always immediately
/// ready, so the whole 5-frame stream is forwarded in one scheduler burst, before
/// the runtime's coarse timer wheel has marked even a `Duration::ZERO` bound as
/// elapsed. Setting `max_deadline` to zero leaves this test passing, so its pass
/// says nothing about the bound's length. `bound_fires_mid_stream_..` below is the
/// deadline-length guard; it uses a producer with a real per-frame sleep so a
/// too-short bound must fire.
///
/// The teeth this test does have are in the `item.expect(..)` inside the drain
/// loop and the `ok_frames == FRAMES` count. Flip-to-fail (verified): make
/// `PermitStream::poll_next` yield `Some(Err(Status::internal("injected")))` for
/// one frame; the `expect` fires and the test fails. So the assertions are live,
/// they just bound the wrong axis to be a timeout guard.
#[tokio::test]
async fn slow_but_progressing_doget_completes_without_spurious_abort() {
    let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(1));
    // A deadline far longer than a loopback stream of 5 small frames can take, so
    // the run is nowhere near it. Real time throughout: this test never pauses or
    // advances the clock.
    let max_deadline = Duration::from_secs(300);
    const FRAMES: usize = 5;
    let service = PermitHoldingService {
        controller: Arc::clone(&controller),
        frames: FRAMES,
        frame_bytes: 1024,
        frame_delay: Duration::ZERO,
    };
    let server = Server::start(service, max_deadline).await;
    let mut client = server.client().await;

    let response = client
        .do_get(Request::new(Ticket::new("progress")))
        .await
        .expect("do_get headers");
    let mut body = response.into_inner();

    let mut ok_frames = 0usize;
    while let Some(item) = body.next().await {
        // No item may be the deadline error: a progressing consumer is never
        // aborted by this bound.
        let frame = item.expect("no deadline error on a progressing stream");
        assert!(!frame.data_body.is_empty(), "each frame carries its bytes");
        ok_frames += 1;
    }
    assert_eq!(ok_frames, FRAMES, "every frame reaches the reading client");

    // Stream end released the permit.
    let mut released = false;
    for _ in 0..1000 {
        if controller.in_flight() == 0 {
            released = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(released, "a completed stream releases its permit");

    server.stop().await;
}

/// Test 3 (total-timeout guard, deterministic): a stream whose producer cannot
/// finish inside the bound is aborted mid-stream, the draining client receives the
/// typed `Unavailable` status as its terminal item, and the permit is released.
///
/// Why this is deterministic rather than a scheduler race. The producer sleeps
/// `FRAME_DELAY` before every frame, so the stream needs at least
/// `FRAMES * FRAME_DELAY` = 2000 ms of wall clock no matter how fast the machine
/// is; `tokio::time::sleep` never returns early. With `max_deadline` at 1000 ms,
/// the driver's sleep must win before frame 3 is produced. A faster or more
/// loaded host changes how many frames arrive first, never whether the bound
/// fires: the failure mode of an overloaded host is fewer frames, not a completed
/// stream. Hence the frame-count assertion is `< FRAMES`, not an exact number.
///
/// This is also the case where the deadline's typed error genuinely reaches the
/// client: the consumer is draining, so the cap-1 channel is empty when the
/// deadline arm runs and its `try_send` succeeds. A fully stalled client (test 1)
/// gets a bare stream close instead, because its own un-drained frame is what
/// occupies the slot. See the "What the client observes" section in
/// `src/flight_deadline.rs`.
///
/// Flip-to-fail (verified): raise `max_deadline` to 60 s. The stream then
/// completes on its own in ~2 s, the terminal item is `None` instead of the
/// status, and the `expect("the bound fires mid-stream ..")` below fails. That
/// flip leaves test 2 passing, which is the point: test 2 does not constrain the
/// bound's length and this one does.
#[tokio::test]
async fn bound_fires_mid_stream_for_a_producer_slower_than_the_bound() {
    let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(1));
    const FRAMES: usize = 5;
    const FRAME_DELAY: Duration = Duration::from_millis(400);
    // Below FRAMES * FRAME_DELAY (2000 ms), above FRAME_DELAY, so the abort lands
    // after some frames and before the end. The margin either way is 2x+.
    let max_deadline = Duration::from_millis(1000);
    let service = PermitHoldingService {
        controller: Arc::clone(&controller),
        frames: FRAMES,
        frame_bytes: 1024,
        frame_delay: FRAME_DELAY,
    };
    let server = Server::start(service, max_deadline).await;
    let mut client = server.client().await;

    let response = client
        .do_get(Request::new(Ticket::new("slow")))
        .await
        .expect("do_get headers");
    let mut body = response.into_inner();

    // Drain to the end of the stream, keeping the last item: for a stream the
    // bound aborts, that item is the deadline status.
    let mut ok_frames = 0usize;
    let mut terminal: Option<Status> = None;
    while let Some(item) = body.next().await {
        match item {
            Ok(frame) => {
                assert!(!frame.data_body.is_empty(), "each frame carries its bytes");
                ok_frames += 1;
            }
            Err(status) => {
                terminal = Some(status);
                break;
            }
        }
    }

    let status = terminal.expect("the bound fires mid-stream and ends it with a status");
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "the deadline presents as Unavailable, like a mid-stream snapshot invalidation"
    );
    assert!(
        ok_frames < FRAMES,
        "the abort landed before the producer could finish: {ok_frames} of {FRAMES} frames"
    );

    // The abort dropped the inner PermitStream, releasing the permit.
    let mut released = false;
    for _ in 0..400 {
        if controller.in_flight() == 0 {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(released, "an aborted stream releases its permit");

    server.stop().await;
}
