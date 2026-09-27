//! The dedicated liveness and readiness listener, and the runtime heartbeat
//! that keeps it honest (ADR-1702 decisions 8 and 9).
//!
//! `--listen-health <addr>` binds a listener served by a `current_thread`
//! runtime on its own OS thread, so a main runtime whose workers are all busy
//! or wedged cannot stop it answering. It serves `/healthz`, `/readyz`,
//! `/-/healthy` and `/-/ready` and nothing else: no tenant identity, no tenant
//! route. The flag is unset by default and then nothing binds. The copies of
//! these routes on the main HTTP router ([`crate::health`]) are unchanged.
//!
//! Because this listener answers even when the main runtime cannot, its
//! answers read a [`Heartbeat`] that a task on the main runtime refreshes once
//! per second:
//!
//! - `/healthz` (and `/-/healthy`): 503 once the heartbeat is older than
//!   [`LIVENESS_MAX_HEARTBEAT_AGE`], else 200 `ok`.
//! - `/readyz` (and `/-/ready`): 503 when [`Readiness::is_ready`] is false or
//!   the heartbeat is older than [`READINESS_MAX_HEARTBEAT_AGE`], else 200.
//!   Also 503 until `main` attaches the server's readiness handle, which
//!   happens once startup has returned.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use ravel_ingest::Clock;
use tokio::sync::oneshot;

use crate::health::Readiness;

/// How often the heartbeat task refreshes the heartbeat.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// Heartbeat age above which the dedicated `/healthz` returns 503
/// (ADR-1702 decision 9): only a stall far past any single unit of on-worker
/// work means the main runtime is stuck and the process should restart.
pub const LIVENESS_MAX_HEARTBEAT_AGE: Duration = Duration::from_secs(60);

/// Heartbeat age above which the dedicated `/readyz` returns 503
/// (ADR-1702 decision 9): far above the longest unit of work left on the
/// workers, so a busy node stays in the Service while a deadlocked one leaves.
pub const READINESS_MAX_HEARTBEAT_AGE: Duration = Duration::from_secs(30);

/// The time of the main runtime's last heartbeat, read from the server's
/// injected [`Clock`]. Clones share one atomic.
#[derive(Clone)]
pub struct Heartbeat {
    last_beat_ns: Arc<AtomicI64>,
    clock: Arc<dyn Clock>,
}

impl Heartbeat {
    /// A heartbeat stamped with the clock's current time, so a fresh process
    /// reads an age near zero. Construct it before binding the listener.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let now_ns = clock.now_ns();
        Self {
            last_beat_ns: Arc::new(AtomicI64::new(now_ns)),
            clock,
        }
    }

    /// Store the clock's current time as the last heartbeat.
    pub fn beat(&self) {
        self.last_beat_ns
            .store(self.clock.now_ns(), Ordering::Release);
    }

    /// Time since the last heartbeat on the clock. Zero if the clock reads
    /// earlier than the last heartbeat.
    pub fn age(&self) -> Duration {
        let age_ns = self
            .clock
            .now_ns()
            .saturating_sub(self.last_beat_ns.load(Ordering::Acquire));
        Duration::from_nanos(u64::try_from(age_ns).unwrap_or(0))
    }

    /// Spawn the heartbeat task on the current tokio runtime, which must be
    /// the main runtime: the heartbeat measures whether that runtime still
    /// polls its tasks. Abort the returned handle to stop it.
    pub fn spawn(&self) -> tokio::task::JoinHandle<()> {
        let heartbeat = self.clone();
        tokio::spawn(async move {
            loop {
                heartbeat.clock.sleep(HEARTBEAT_INTERVAL).await;
                heartbeat.beat();
            }
        })
    }
}

#[derive(Clone)]
struct ListenerState {
    heartbeat: Heartbeat,
    readiness: Arc<OnceLock<Readiness>>,
}

/// A bound `--listen-health` listener and the thread serving it. Dropping it
/// signals the thread to stop without waiting; [`HealthListener::shutdown`]
/// also joins it.
pub struct HealthListener {
    local_addr: SocketAddr,
    readiness: Arc<OnceLock<Readiness>>,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
}

impl HealthListener {
    /// Bind `addr` and start serving it on a dedicated thread with its own
    /// `current_thread` runtime. The socket is bound on the calling thread,
    /// so a bind failure is returned here as a startup error naming the flag
    /// and the address.
    pub fn bind(addr: SocketAddr, heartbeat: Heartbeat) -> anyhow::Result<Self> {
        let listener = std::net::TcpListener::bind(addr).with_context(|| {
            format!("--listen-health {addr}: failed to bind the health listener")
        })?;
        listener
            .set_nonblocking(true)
            .with_context(|| format!("--listen-health {addr}: failed to configure the socket"))?;
        let local_addr = listener
            .local_addr()
            .with_context(|| format!("--listen-health {addr}: failed to read the bound address"))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("--listen-health: failed to build the health listener runtime")?;

        let readiness = Arc::new(OnceLock::new());
        let app = router(ListenerState {
            heartbeat,
            readiness: readiness.clone(),
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("ravel-health".to_string())
            .spawn(move || runtime.block_on(serve(listener, app, shutdown_rx)))
            .context("--listen-health: failed to spawn the health listener thread")?;

        Ok(Self {
            local_addr,
            readiness,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    /// The address the listener actually bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Attach the server's readiness handle. Until this is called `/readyz`
    /// answers 503. Only the first call takes effect.
    pub fn attach_readiness(&self, readiness: Readiness) {
        let _ = self.readiness.set(readiness);
    }

    /// Stop accepting, finish in-flight probes, and join the thread. Blocks
    /// the calling thread; call it from `spawn_blocking` inside a runtime.
    pub fn shutdown(mut self) -> anyhow::Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        match self.thread.take() {
            Some(thread) => thread
                .join()
                .map_err(|_| anyhow::anyhow!("the health listener thread panicked"))?,
            None => Ok(()),
        }
    }
}

async fn serve(
    listener: std::net::TcpListener,
    app: Router,
    shutdown: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::from_std(listener)
        .context("failed to register the health listener socket")?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            // A dropped sender stops the listener the same as a sent signal.
            let _ = shutdown.await;
        })
        .await
        .context("health listener failed")
}

fn router(state: ListenerState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/-/healthy", get(healthz))
        .route("/-/ready", get(readyz))
        .with_state(state)
}

async fn healthz(State(state): State<ListenerState>) -> (StatusCode, &'static str) {
    if state.heartbeat.age() > LIVENESS_MAX_HEARTBEAT_AGE {
        (StatusCode::SERVICE_UNAVAILABLE, "heartbeat stalled")
    } else {
        (StatusCode::OK, "ok")
    }
}

async fn readyz(State(state): State<ListenerState>) -> StatusCode {
    let ready = state.readiness.get().is_some_and(Readiness::is_ready)
        && state.heartbeat.age() <= READINESS_MAX_HEARTBEAT_AGE;
    if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::mpsc::{SyncSender, sync_channel};

    use ravel_test_support::{ParkOnFirstArmedCall, run_with_watchdog};
    use tokio::runtime::Runtime;

    use super::*;

    const BASE_NS: i64 = 1_800_000_000_000_000_000;

    /// Bounds the whole test body. Shorter than [`IO_BOUND`], so a request
    /// the listener never answers reaches the watchdog's panic, which names
    /// the cause, before the socket's own read timeout.
    const WATCHDOG_BOUND: Duration = Duration::from_secs(20);
    const IO_BOUND: Duration = Duration::from_secs(60);
    const PARK_BOUND: Duration = Duration::from_secs(10);
    const MAIN_WORKERS: usize = 2;

    struct TestClock(AtomicI64);

    impl TestClock {
        fn at(now_ns: i64) -> Arc<Self> {
            Arc::new(Self(AtomicI64::new(now_ns)))
        }

        fn set_age(&self, age: Duration) {
            let age_ns = i64::try_from(age.as_nanos()).expect("age fits i64");
            self.0.store(BASE_NS + age_ns, Ordering::SeqCst);
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn loopback() -> SocketAddr {
        "127.0.0.1:0".parse().expect("valid loopback addr")
    }

    fn main_runtime() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(MAIN_WORKERS)
            .enable_all()
            .build()
            .expect("main runtime builds")
    }

    /// One blocking HTTP/1.1 GET over a plain socket, so the request itself
    /// needs no runtime at all. Returns the status code and body.
    fn get(addr: SocketAddr, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect_timeout(&addr, IO_BOUND).expect("connect");
        stream
            .set_read_timeout(Some(IO_BOUND))
            .expect("read timeout");
        stream
            .set_write_timeout(Some(IO_BOUND))
            .expect("write timeout");
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
        )
        .expect("write request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        let status = response
            .split(' ')
            .nth(1)
            .and_then(|code| code.parse().ok())
            .expect("status line");
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .unwrap_or_default();
        (status, body)
    }

    /// Parks every worker of `rt`: one task per worker, each blocked inside a
    /// [`ParkOnFirstArmedCall`] until its release sender is sent to. Returns
    /// once every worker has announced it is parked.
    fn park_workers(rt: &Runtime) -> Vec<SyncSender<()>> {
        let (announce_tx, announce_rx) = sync_channel::<()>(MAIN_WORKERS);
        let mut releases = Vec::with_capacity(MAIN_WORKERS);
        for _ in 0..MAIN_WORKERS {
            let (release_tx, release_rx) = sync_channel::<()>(0);
            let announce_tx = announce_tx.clone();
            let park = ParkOnFirstArmedCall::new(
                move || {
                    let _ = announce_tx.send(());
                },
                release_rx,
            );
            park.arm();
            rt.spawn(async move {
                park.now_ns();
            });
            releases.push(release_tx);
        }
        for worker in 0..MAIN_WORKERS {
            announce_rx
                .recv_timeout(PARK_BOUND)
                .unwrap_or_else(|_| panic!("main runtime worker {worker} never parked"));
        }
        releases
    }

    fn release_workers(releases: Vec<SyncSender<()>>) {
        for release in releases {
            release.send(()).expect("a parked worker takes its release");
        }
    }

    /// Binds the health listener the way `main` does: from inside the main
    /// runtime's context, with the heartbeat task spawned on that runtime.
    fn start_on(rt: &Runtime, heartbeat: &Heartbeat) -> HealthListener {
        let _entered = rt.enter();
        let _heartbeat_task = heartbeat.spawn();
        HealthListener::bind(loopback(), heartbeat.clone()).expect("health listener binds")
    }

    #[test]
    fn healthz_answers_while_every_worker_is_parked() {
        run_with_watchdog(
            WATCHDOG_BOUND,
            || {
                "GET /healthz on the health listener never completed while every main \
                 runtime worker was parked: the listener is served by the main runtime"
                    .to_string()
            },
            || {
                let main_rt = main_runtime();
                let heartbeat = Heartbeat::new(TestClock::at(BASE_NS));
                let listener = start_on(&main_rt, &heartbeat);

                let releases = park_workers(&main_rt);
                let (status, body) = get(listener.local_addr(), "/healthz");
                assert_eq!(status, 200, "/healthz while every worker is parked");
                assert_eq!(body, "ok");
                release_workers(releases);

                listener.shutdown().expect("health listener stops");
                main_rt.shutdown_timeout(PARK_BOUND);
            },
        );
    }

    #[test]
    fn liveness_fails_after_heartbeat_stall() {
        run_with_watchdog(
            WATCHDOG_BOUND,
            || "the health listener stopped answering during a heartbeat stall".to_string(),
            || {
                let main_rt = main_runtime();
                let clock = TestClock::at(BASE_NS);
                let heartbeat = Heartbeat::new(clock.clone());
                let listener = start_on(&main_rt, &heartbeat);
                let readiness = Readiness::new();
                readiness.mark_ready();
                listener.attach_readiness(readiness.clone());
                let addr = listener.local_addr();

                // The clock moves only after every worker is parked, so every
                // beat the heartbeat task managed to store reads BASE_NS and
                // none can run from here on.
                let releases = park_workers(&main_rt);

                clock.set_age(Duration::from_secs(29));
                assert_eq!(get(addr, "/readyz").0, 200, "/readyz at 29 s");
                assert_eq!(get(addr, "/-/ready").0, 200, "/-/ready at 29 s");

                clock.set_age(Duration::from_secs(31));
                assert!(
                    readiness.is_ready(),
                    "all four readiness flags say ready, so only the heartbeat can fail /readyz"
                );
                assert_eq!(get(addr, "/readyz").0, 503, "/readyz at 31 s");
                assert_eq!(get(addr, "/-/ready").0, 503, "/-/ready at 31 s");
                assert_eq!(get(addr, "/healthz").0, 200, "/healthz at 31 s");

                clock.set_age(Duration::from_secs(59));
                assert_eq!(get(addr, "/healthz"), (200, "ok".to_string()), "at 59 s");
                assert_eq!(get(addr, "/-/healthy").0, 200, "/-/healthy at 59 s");

                clock.set_age(Duration::from_secs(61));
                assert_eq!(get(addr, "/healthz").0, 503, "/healthz at 61 s");
                assert_eq!(get(addr, "/-/healthy").0, 503, "/-/healthy at 61 s");

                release_workers(releases);
                listener.shutdown().expect("health listener stops");
                main_rt.shutdown_timeout(PARK_BOUND);
            },
        );
    }

    #[test]
    fn serves_the_four_probe_routes_and_nothing_else() {
        run_with_watchdog(
            WATCHDOG_BOUND,
            || "the health listener did not answer".to_string(),
            || {
                let heartbeat = Heartbeat::new(TestClock::at(BASE_NS));
                let listener = HealthListener::bind(loopback(), heartbeat).expect("binds");
                let addr = listener.local_addr();

                assert_eq!(
                    get(addr, "/readyz").0,
                    503,
                    "503 until readiness is attached"
                );
                let readiness = Readiness::new();
                readiness.mark_ready();
                listener.attach_readiness(readiness);

                assert_eq!(get(addr, "/healthz"), (200, "ok".to_string()));
                assert_eq!(get(addr, "/-/healthy"), (200, "ok".to_string()));
                assert_eq!(get(addr, "/readyz").0, 200);
                assert_eq!(get(addr, "/-/ready").0, 200);
                for path in [
                    "/",
                    "/metrics",
                    "/v1/metrics",
                    "/api/v1/query",
                    "/api/v1/sql",
                ] {
                    assert_eq!(get(addr, path).0, 404, "{path} is not served here");
                }

                listener.shutdown().expect("health listener stops");
                assert!(
                    TcpStream::connect_timeout(&addr, IO_BOUND).is_err(),
                    "the listener socket is closed after shutdown"
                );
            },
        );
    }

    #[test]
    fn bind_failure_names_the_flag_and_the_address() {
        let taken = std::net::TcpListener::bind(loopback()).expect("bind a port to collide with");
        let addr = taken.local_addr().expect("bound address");
        let heartbeat = Heartbeat::new(TestClock::at(BASE_NS));
        let Err(err) = HealthListener::bind(addr, heartbeat) else {
            panic!("binding a taken address must fail");
        };
        let message = format!("{err:#}");
        assert!(
            message.contains(&format!("--listen-health {addr}")),
            "{message}"
        );
    }

    #[test]
    fn a_beat_resets_the_age_on_the_injected_clock() {
        let clock = TestClock::at(BASE_NS);
        let heartbeat = Heartbeat::new(clock.clone());
        assert_eq!(heartbeat.age(), Duration::ZERO, "stamped at construction");
        clock.set_age(Duration::from_secs(61));
        assert_eq!(heartbeat.age(), Duration::from_secs(61));
        heartbeat.beat();
        assert_eq!(heartbeat.age(), Duration::ZERO);
    }
}
