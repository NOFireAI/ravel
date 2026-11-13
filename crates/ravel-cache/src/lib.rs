//! ADR-0046's read cache: a RAM tier and a local-disk tier
//! (docs/adrs/0046-read-cache-tier.md, decisions 1-7). This crate is not
//! wired into anything yet: it does not depend on `ravel-types`, does not
//! touch `ravel-catalog` or `ravel-query`, and does not reference
//! `QueryAccounting`. Later tasks in this epic connect it to those.
//!
//! This crate is consulted at a read funnel; it is never a store
//! decorator (decision 1).
//!
//! [`CacheKey`] is content-addressed (decision 2): `(tenant_hash,
//! content_hash, offset, len)`, with no constructor from an object key
//! string, because the mutable objects that own such strings (the
//! catalog HEAD, the maintenance cursor) have no content hash and must
//! stay unrepresentable rather than merely un-cached-by-convention.
//!
//! [`Cache`] is the RAM tier. It bounds total bytes, entry count, and
//! maximum single-entry size (deliverable 4); evicts with S3-FIFO, not
//! LRU, because the compactor and the folder scan cold data in the same
//! process as queries and a single scan must not evict the query working
//! set (decision 6, see [`s3fifo`]); collapses concurrent misses on one
//! key into a single upstream call (decision 5, see [`single_flight`]);
//! and has a constructor, [`Cache::with_corruption`], that makes every
//! hit return deliberately corrupted bytes -- the supported
//! acceptance-gate mode for the whole epic (decision 4), not a test
//! fixture. Like the disk tier, each RAM entry carries a stamped write
//! time and a configured per-entry max-age ([`DEFAULT_MAX_ENTRY_AGE_NS`]):
//! an entry older than the max-age is treated as a miss on `get`, and a
//! background sweep drops over-age idle entries on [`DEFAULT_SWEEP_INTERVAL_NS`]
//! regardless of access, so raw bytes of a subject erased by ADR-0064's sweep
//! persist in a query node's RAM at most the max-age plus one sweep interval
//! past the sweep, a sum the defaults keep within 24 h.
//! The maintain node's erasure sweep cannot reach a query node's memory, so the
//! bound is enforced locally, the same way it is for the disk tier.
//!
//! [`disk::DiskCache`] is the local-disk tier: content-addressed raw byte
//! ranges under a configured directory, opt-in (no directory, no disk
//! tier, behavior unchanged), plaintext (decision 7 -- **with SSE-KMS
//! configured, cached bytes on local disk are not protected by that
//! key**), evicted with the same [`s3fifo`] policy as [`Cache`] rather than
//! a second implementation (decision 6, amended 2026-08-02 -- scan
//! resistance matters more on disk than in RAM, since a disk miss costs an
//! S3 fetch and disk is the tier that holds the working set), and built so
//! every failure degrades to a miss rather than an error (see the
//! [`disk`] module docs for the crash-safety mechanism and what crc32c and
//! the header actually prove on a hit, per decision 4 as amended
//! 2026-08-02). A disk entry also carries a stamped write time and a
//! configured per-entry max-age (default 23 h,
//! [`DEFAULT_MAX_ENTRY_AGE_NS`]): an entry older than the max-age is treated
//! as a miss and dropped, so raw bytes of a subject erased by ADR-0064's
//! sweep persist on a node's disposable local disk at most the max-age plus
//! one sweep interval past the sweep -- the defaults are tuned to keep that
//! sum within ADR-0064's 24 h bound. Time is injected through
//! [`Clock`] so this ageing is deterministic under test.
//!
//! [`tiered::TieredCache`] composes the two tiers behind one handle the three
//! ADR-0046 read funnels can hold (decision 3). It is read-through: a RAM miss
//! consults the disk tier before the caller's upstream fetch is allowed to
//! run, and a successful upstream fetch populates *both* tiers, not only RAM,
//! so a later RAM eviction is served from disk instead of paying an S3 round
//! trip again. Single-flight spans both tiers: concurrent callers for one key
//! collapse to a single disk consult and, on a disk miss, a single upstream
//! fetch. When its RAM tier is in [`Cache::with_corruption`] mode, a
//! disk-served hit is corrupted by the identical transform a RAM hit uses, so
//! ADR-0046 decision 4's acceptance gate reaches the disk tier the moment the
//! funnels start using this handle. A caller that cannot express its miss
//! handling as one upstream-fetch closure instead has [`tiered::TieredCache::get`]
//! and [`tiered::TieredCache::insert`]: the same read-through and dual-tier
//! admission, with no fetch and no single-flight participation. A caller that
//! peeked both tiers with `get`, saw a confirmed miss, and now wants that miss
//! resolved *coalesced* -- so concurrent callers for one key collapse onto a
//! single upstream fetch -- uses [`tiered::TieredCache::resolve_peeked_miss`]:
//! it joins the same single-flight `get_or_fetch` uses and admits to both tiers
//! on success, but makes no counted tier consultation and records no hit or
//! miss of its own, because the caller's earlier `get` already accounted the
//! one miss. Its leader does check the RAM tier once, uncounted, so a caller
//! whose peek raced a flight that has since finished is served that flight's
//! bytes when the RAM tier still holds them.
//!
//! No disk operation reached through [`tiered::TieredCache`] runs on a runtime
//! worker thread (issue #1891): every `std::fs` call it makes is dispatched to
//! `spawn_blocking`. The fetch-free pair stays synchronous for callers off a
//! runtime, and an async caller uses
//! [`tiered::TieredCache::get_off_worker`] / [`tiered::TieredCache::insert_off_worker`].
//! The disk tier's background age sweep (ADR-0064) is dispatched the same way,
//! so a tick walks the namespace directory on the blocking pool rather than
//! parking a worker for the length of the walk.
//!
//! Both tiers' counters are readable independently:
//! [`tiered::TieredCache::ram_metrics`] (which also carries the cross-tier
//! single-flight collapses) and [`tiered::TieredCache::disk_metrics`], so a
//! disk-served hit is visible on the disk tier rather than hidden behind a
//! RAM-only miss.
//!
//! Every read carries an [`AccessIntent`], `Interactive` unless a
//! `_with_intent` method names another. The RAM tier admits a `BulkScan`
//! read into a bulk region that serves a stable subset of a repeated scan
//! larger than the cache, without evicting the main queue (see [`s3fifo`]).

mod cache;
mod clock;
pub mod disk;
mod key;
mod limits;
mod metrics;
mod s3fifo;
mod single_flight;
#[cfg(test)]
mod test_support;
pub mod tiered;

pub use cache::Cache;
pub use clock::{Clock, SystemClock};
pub use disk::{DiskCache, ReclaimReport, RootEntryKind, classify_root_entry, reclaim_legacy};
pub use key::{CacheKey, PinnedIdentity};
pub use limits::{
    CacheLimits, DEFAULT_BULK_SHARE, DEFAULT_MAX_ENTRY_AGE_NS, DEFAULT_SWEEP_INTERVAL_NS,
};
pub use metrics::{CacheMetrics, CacheMetricsSnapshot, IntentCounters, IntentMetricsSnapshot};
pub use single_flight::{Role, SingleFlight, SingleFlightError};
pub use tiered::{ReadOutcome, Source, TieredCache};

/// What a read is for, which decides how the RAM tier admits and promotes
/// its bytes (ADR-3072 decision 3). It is admission policy only: it never
/// skips a read and never changes what a hit returns. The `_with_intent`
/// methods of [`Cache`] and [`TieredCache`] take it; every other method
/// reads as [`AccessIntent::Interactive`]. The disk tier applies the
/// interactive policy whatever the intent.
///
/// No caller outside this crate passes anything but `Interactive` yet;
/// issue #3084 is the change that has the SQL executor set `BulkScan`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum AccessIntent {
    /// A read whose bytes may be read again soon: the S3-FIFO policy as it
    /// was before intents existed.
    #[default]
    Interactive,
    /// A read of a statement that reads every object of a tenant. A hit does
    /// not count toward promotion, a ghost hit does not promote, and an
    /// admission goes to the bulk region, which keeps a stable subset of a
    /// repeated scan larger than itself and never evicts the main queue.
    BulkScan,
    /// A read by a maintenance pass. Admitted to the small queue only and
    /// never promoted by its own hits or by a ghost hit. Nothing sets it yet.
    Maintenance,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;
    use crate::single_flight::MAX_CANCELLED_LEADER_REENTRIES;

    fn test_key(n: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([7u8; 16], content_hash, 0, 0)
    }

    /// Poll `future` exactly once and report whether it completed. The first
    /// poll of a `SingleFlight::run` future is where it takes the in-flight
    /// map lock and becomes a leader or a follower, so polling once is how
    /// these tests make follower registration an ordering they enforce rather
    /// than a wall-clock race decided by how promptly the runtime schedules a
    /// spawned task against a leader that holds its slot for a fixed sleep.
    async fn poll_once<F: Future + ?Sized>(future: &mut Pin<Box<F>>) -> Poll<F::Output> {
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
    }

    #[tokio::test]
    async fn single_flight_collapses_concurrent_misses_and_propagates_errors() {
        let flight: Arc<SingleFlight<CacheKey, Bytes, &'static str>> =
            Arc::new(SingleFlight::new());
        let key = test_key(1);
        const CALLERS: usize = 8;

        // N concurrent misses on one key produce exactly one upstream
        // call, and every waiter receives the same bytes. The leader is held
        // inside its fetch by a `oneshot` this test releases only after every
        // follower has been polled once, so exactly one of the N is the leader
        // by construction.
        let upstream_calls = Arc::new(AtomicUsize::new(0));
        let follower_fetches = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let leader = {
            let flight = flight.clone();
            let upstream_calls = upstream_calls.clone();
            tokio::spawn(async move {
                flight
                    .run(key, move || async move {
                        upstream_calls.fetch_add(1, Ordering::SeqCst);
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        Ok::<Bytes, &'static str>(Bytes::from_static(b"payload"))
                    })
                    .await
            })
        };
        entered_rx.await.unwrap();

        let mut followers = Vec::new();
        for _ in 1..CALLERS {
            let ran = follower_fetches.clone();
            followers.push(Box::pin(flight.run(key, move || async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<Bytes, &'static str>(Bytes::from_static(b"never"))
            })));
        }
        for follower in &mut followers {
            assert!(
                poll_once(follower).await.is_pending(),
                "a follower parks on the held leader instead of completing on its own"
            );
        }
        release_tx.send(()).expect("the leader is still parked");

        for follower in followers {
            let (result, role) = follower.await;
            assert_eq!(result.unwrap().as_ref(), b"payload".as_slice());
            assert_eq!(role, Role::Follower, "every later caller is a follower");
        }
        let (result, role) = leader.await.unwrap();
        assert_eq!(result.unwrap().as_ref(), b"payload".as_slice());
        assert_eq!(role, Role::Leader, "the held caller is the one leader");
        assert_eq!(
            upstream_calls.load(Ordering::SeqCst),
            1,
            "N concurrent misses on one key must produce exactly one upstream call"
        );
        assert_eq!(
            follower_fetches.load(Ordering::SeqCst),
            0,
            "a follower never runs its own fetch"
        );

        // A failing upstream call reaches every waiter as an error,
        // rather than hanging any of them. Held the same way.
        let key2 = test_key(2);
        let upstream_calls2 = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let leader = {
            let flight = flight.clone();
            let upstream_calls2 = upstream_calls2.clone();
            tokio::spawn(async move {
                flight
                    .run(key2, move || async move {
                        upstream_calls2.fetch_add(1, Ordering::SeqCst);
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        Err::<Bytes, &'static str>("upstream exploded")
                    })
                    .await
            })
        };
        entered_rx.await.unwrap();

        let mut followers = Vec::new();
        for _ in 1..CALLERS {
            followers.push(Box::pin(flight.run(key2, || async {
                Ok::<Bytes, &'static str>(Bytes::from_static(b"never"))
            })));
        }
        for follower in &mut followers {
            assert!(
                poll_once(follower).await.is_pending(),
                "a follower parks on the held leader instead of completing on its own"
            );
        }
        release_tx.send(()).expect("the leader is still parked");

        for follower in followers {
            let (result, role) = follower.await;
            match result {
                Err(SingleFlightError::Upstream(message)) => {
                    assert_eq!(message, "upstream exploded")
                }
                other => panic!("expected every waiter to see the upstream error, got {other:?}"),
            }
            assert_eq!(role, Role::Follower);
        }
        let (result, role) = leader.await.unwrap();
        assert!(matches!(result, Err(SingleFlightError::Upstream(_))));
        assert_eq!(role, Role::Leader);
        assert_eq!(upstream_calls2.load(Ordering::SeqCst), 1);

        // A second, later miss on the same key after the first completed
        // does not reuse the completed slot: it starts a fresh leader.
        let third_call_ran = Arc::new(AtomicUsize::new(0));
        let third_call_ran_clone = third_call_ran.clone();
        let (third_result, third_role) = flight
            .run(key2, move || async move {
                third_call_ran_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<Bytes, &'static str>(Bytes::from_static(b"fresh"))
            })
            .await;
        assert_eq!(
            third_role,
            Role::Leader,
            "a miss after completion must start a fresh leader"
        );
        assert_eq!(third_result.unwrap().as_ref(), b"fresh".as_slice());
        assert_eq!(third_call_ran.load(Ordering::SeqCst), 1);
    }

    /// A leader that PANICS keeps the pre-#4174 contract: its follower receives
    /// [`SingleFlightError::LeaderLost`] and never runs its own fetch, because a
    /// deterministic panic would otherwise repeat once per follower.
    ///
    /// Distinguishing wrong impl (c): if `LeaderGuard::drop` recorded
    /// `LeaderOutcome::Cancelled` for a panicking leader too (dropping the
    /// `std::thread::panicking()` branch), the follower would re-enter and run
    /// its own fetch, so the `follower_ran == 0` assertion fails.
    #[tokio::test]
    async fn single_flight_panicking_leader_gives_follower_leader_lost_no_retry() {
        let flight: Arc<SingleFlight<CacheKey, Bytes, &'static str>> =
            Arc::new(SingleFlight::new());

        // The leader parks inside its fetch until this test releases it, and the
        // follower is registered (polled once) before that release, so "the
        // follower joined while the leader was still in flight" is ordered here
        // rather than raced against a sleep.
        let key = test_key(100);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let leader = {
            let flight = flight.clone();
            tokio::spawn(async move {
                flight
                    .run(key, move || async move {
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        panic!("leader blew up before producing a result");
                    })
                    .await
            })
        };
        entered_rx.await.unwrap();

        let follower_ran = Arc::new(AtomicUsize::new(0));
        let mut follower = {
            let ran = follower_ran.clone();
            Box::pin(flight.run_with_reentries(key, move || async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<Bytes, &'static str>(Bytes::from_static(b"never"))
            }))
        };
        assert!(
            poll_once(&mut follower).await.is_pending(),
            "the follower parks on the still-in-flight leader"
        );
        release_tx.send(()).expect("the leader is still parked");

        assert!(
            leader.await.is_err(),
            "the leader task itself should have panicked"
        );
        let (follower_result, follower_role, reentries) = timeout(Duration::from_secs(2), follower)
            .await
            .expect("a panicking leader must not leave a follower parked forever");
        assert!(matches!(
            follower_result,
            Err(SingleFlightError::LeaderLost)
        ));
        assert_eq!(follower_role, Role::Follower);
        assert_eq!(
            reentries, 0,
            "a follower does not re-enter after a panicking leader"
        );
        assert_eq!(
            follower_ran.load(Ordering::SeqCst),
            0,
            "a follower of a panicking leader never runs its own fetch"
        );
    }

    /// A leader that is CANCELLED (its future dropped before producing a result)
    /// no longer fails its follower (issue #4174): the follower re-enters the
    /// slot, becomes the new leader, and runs its own fetch exactly once,
    /// returning its own result.
    ///
    /// Distinguishing wrong impl (a) -- the pre-fix code, where
    /// `LeaderGuard::drop` sent `Err(LeaderLost)` and `wait` returned it
    /// unconditionally: the follower would see `LeaderLost` instead of its
    /// bytes, so `result.unwrap()` and the `Role::Leader`/`reentries == 1`
    /// assertions all fail.
    #[tokio::test]
    async fn single_flight_cancelled_leader_follower_runs_its_own_fetch_once() {
        let flight: Arc<SingleFlight<CacheKey, Bytes, &'static str>> =
            Arc::new(SingleFlight::new());

        // The leader parks on a `oneshot` this test never fires, so it is still
        // in flight when the abort lands.
        let key = test_key(101);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (_never_tx, never_rx) = oneshot::channel::<()>();
        let leader = {
            let flight = flight.clone();
            tokio::spawn(async move {
                flight
                    .run(key, move || async move {
                        let _ = entered_tx.send(());
                        let _ = never_rx.await;
                        Ok::<Bytes, &'static str>(Bytes::from_static(b"unreachable"))
                    })
                    .await
            })
        };
        entered_rx.await.unwrap();

        let follower_ran = Arc::new(AtomicUsize::new(0));
        let mut follower = {
            let ran = follower_ran.clone();
            Box::pin(flight.run_with_reentries(key, move || async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<Bytes, &'static str>(Bytes::from_static(b"follower bytes"))
            }))
        };
        assert!(
            poll_once(&mut follower).await.is_pending(),
            "the follower parks on the still-in-flight leader"
        );

        leader.abort();
        // Join the aborted task so its `LeaderGuard::drop` has fired (cancelled,
        // slot removed) before the follower is polled again.
        let _ = leader.await;

        let (result, role, reentries) = timeout(Duration::from_secs(2), follower)
            .await
            .expect("a cancelled leader must let its follower finish, not park it");
        assert_eq!(
            result
                .expect("the follower resolves its own fetch")
                .as_ref(),
            b"follower bytes".as_slice()
        );
        assert_eq!(
            role,
            Role::Leader,
            "the re-entering follower took over as leader and ran its own fetch"
        );
        assert_eq!(
            reentries, 1,
            "it re-entered once after the one cancellation"
        );
        assert_eq!(
            follower_ran.load(Ordering::SeqCst),
            1,
            "the follower ran its own fetch exactly once"
        );
    }

    /// Three callers on one key, leader cancelled: exactly one of the two
    /// followers takes over and runs the fetch; the other follows it and
    /// receives its bytes. The fetch runs exactly once after the cancellation.
    ///
    /// Distinguishing wrong impl (b) -- a retried follower that runs its own
    /// fetch WITHOUT re-entering the slot: both followers would fetch, so
    /// `fetches` reads 2 instead of 1 and the single-leader/single-follower
    /// role split fails.
    #[tokio::test]
    async fn single_flight_cancelled_leader_two_followers_fetch_once() {
        let flight: Arc<SingleFlight<CacheKey, Bytes, &'static str>> =
            Arc::new(SingleFlight::new());
        let key = test_key(201);
        let fetches = Arc::new(AtomicUsize::new(0));

        // The original leader parks and never touches `fetches`.
        let (entered_tx, entered_rx) = oneshot::channel();
        let (_never_tx, never_rx) = oneshot::channel::<()>();
        let leader = {
            let flight = flight.clone();
            tokio::spawn(async move {
                flight
                    .run(key, move || async move {
                        let _ = entered_tx.send(());
                        let _ = never_rx.await;
                        Ok::<Bytes, &'static str>(Bytes::from_static(b"unreachable"))
                    })
                    .await
            })
        };
        entered_rx.await.unwrap();

        // Two followers, each of whose fetch counts itself and then parks on the
        // shared gate, so the follower that takes over as leader stays in flight
        // long enough for the other to re-enter and follow it rather than
        // leading a second fetch.
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let flight = flight.clone();
            let fetches = fetches.clone();
            let gate = gate.clone();
            handles.push(tokio::spawn(async move {
                flight
                    .run_with_reentries(key, move || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        let _ = gate.acquire().await;
                        Ok::<Bytes, &'static str>(Bytes::from_static(b"took over"))
                    })
                    .await
            }));
        }
        // Both followers registered on the original leader. The timeout only
        // guards against a hang.
        timeout(Duration::from_secs(10), async {
            while flight.waiters(&key) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both followers subscribe to the original leader");

        leader.abort();
        let _ = leader.await;

        // One follower re-enters, becomes the new leader, and parks its fetch;
        // the other re-enters and follows it. Wait for that steady state: one
        // fetch started and exactly one caller parked as a follower. The timeout
        // only guards against a hang -- a wrong impl where both followers lead
        // (no follower ever parks) never reaches this state, and the assertions
        // below then catch the doubled fetch count cleanly instead of hanging.
        let _ = timeout(Duration::from_secs(10), async {
            while fetches.load(Ordering::SeqCst) < 1 || flight.waiters(&key) < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        // Release generously so every parked fetch finishes regardless of impl.
        gate.add_permits(2);

        let mut results = Vec::new();
        for handle in handles {
            results.push(
                timeout(Duration::from_secs(2), handle)
                    .await
                    .expect("no caller parks forever")
                    .expect("the follower task joins"),
            );
        }

        assert_eq!(
            fetches.load(Ordering::SeqCst),
            1,
            "the two followers collapse onto one re-fetch after the cancellation"
        );
        for (result, _role, reentries) in &results {
            assert_eq!(
                result.as_ref().expect("each caller resolves").as_ref(),
                b"took over".as_slice()
            );
            assert_eq!(*reentries, 1, "each re-entered once after the cancellation");
        }
        let leaders = results
            .iter()
            .filter(|(_, role, _)| *role == Role::Leader)
            .count();
        let followers = results
            .iter()
            .filter(|(_, role, _)| *role == Role::Follower)
            .count();
        assert_eq!(leaders, 1, "exactly one follower took over as leader");
        assert_eq!(followers, 1, "the other rode its result as a follower");
    }

    /// Re-entry after a cancelled leader is bounded: a follower that keeps
    /// finding a fresh (then cancelled) leader re-enters up to
    /// [`MAX_CANCELLED_LEADER_REENTRIES`] times and, one cancellation past the
    /// bound, returns [`SingleFlightError::LeaderLost`] without ever running its
    /// own fetch.
    ///
    /// Driven entirely by hand with `poll_once` and `drop`, so the leader chain
    /// and the follower's re-entries are a fixed order, not a race. Each dropped
    /// leader parks on `std::future::pending`, so dropping its future cancels an
    /// in-flight leader exactly as a torn-down query would.
    #[tokio::test]
    async fn single_flight_cancelled_leader_reentry_is_bounded() {
        // The result type of `run_with_reentries`, so the parked leaders (a new
        // closure type each iteration) and the follower share one reassignable
        // boxed type.
        type RunOut = (Result<Bytes, SingleFlightError<&'static str>>, Role, u32);
        let flight = Arc::new(SingleFlight::<CacheKey, Bytes, &'static str>::new());
        let key = test_key(202);

        // A parked leader that holds the slot until its future is dropped. Owns
        // an `Arc` clone so the boxed future is `'static` and reassignable.
        let park_leader = |flight: Arc<SingleFlight<CacheKey, Bytes, &'static str>>| -> Pin<Box<dyn Future<Output = RunOut>>> {
            Box::pin(async move {
                flight
                    .run_with_reentries(key, || async {
                        std::future::pending::<Result<Bytes, &'static str>>().await
                    })
                    .await
            })
        };

        let mut leader = park_leader(flight.clone());
        assert!(poll_once(&mut leader).await.is_pending());

        let follower_ran = Arc::new(AtomicUsize::new(0));
        let mut follower: Pin<Box<dyn Future<Output = RunOut>>> = {
            let flight = flight.clone();
            let ran = follower_ran.clone();
            Box::pin(async move {
                flight
                    .run_with_reentries(key, move || async move {
                        ran.fetch_add(1, Ordering::SeqCst);
                        Ok::<Bytes, &'static str>(Bytes::from_static(b"never"))
                    })
                    .await
            })
        };
        assert!(
            poll_once(&mut follower).await.is_pending(),
            "the follower parks on the initial leader"
        );

        // Each iteration cancels the current leader and installs a fresh one
        // before polling the follower, so the follower re-enters and follows the
        // replacement instead of leading.
        for _ in 1..=MAX_CANCELLED_LEADER_REENTRIES {
            drop(leader);
            leader = park_leader(flight.clone());
            assert!(poll_once(&mut leader).await.is_pending());
            assert!(
                poll_once(&mut follower).await.is_pending(),
                "the follower re-enters and follows the fresh leader while under the bound"
            );
        }

        // One cancellation past the bound, with no replacement leader: the
        // follower gives up.
        drop(leader);
        match poll_once(&mut follower).await {
            Poll::Ready((result, role, reentries)) => {
                assert!(
                    matches!(result, Err(SingleFlightError::LeaderLost)),
                    "past the bound the follower returns LeaderLost"
                );
                assert_eq!(role, Role::Follower);
                assert_eq!(reentries, MAX_CANCELLED_LEADER_REENTRIES);
            }
            Poll::Pending => panic!("the follower must stop re-entering at the bound"),
        }
        assert_eq!(
            follower_ran.load(Ordering::SeqCst),
            0,
            "a bounded-out follower never runs its own fetch"
        );
    }

    /// A follower dropped while waiting runs no fetch and leaves no slot behind:
    /// once the leader it was following is gone too, nothing is in flight.
    #[tokio::test]
    async fn single_flight_follower_dropped_while_waiting_leaves_no_slot() {
        let flight: SingleFlight<CacheKey, Bytes, &'static str> = SingleFlight::new();
        let key = test_key(203);

        let mut leader = Box::pin(flight.run(key, || async {
            std::future::pending::<Result<Bytes, &'static str>>().await
        }));
        assert!(poll_once(&mut leader).await.is_pending());
        assert!(flight.is_in_flight(&key), "the leader holds the slot");

        let ran = Arc::new(AtomicUsize::new(0));
        let mut follower = {
            let ran = ran.clone();
            Box::pin(flight.run(key, move || async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<Bytes, &'static str>(Bytes::new())
            }))
        };
        assert!(poll_once(&mut follower).await.is_pending());
        assert_eq!(flight.waiters(&key), 1, "the follower parked on the leader");

        // Drop the follower while it waits: it never re-enters, so it creates no
        // slot and never runs its fetch.
        drop(follower);
        assert_eq!(
            ran.load(Ordering::SeqCst),
            0,
            "the dropped follower never fetched"
        );
        assert_eq!(
            flight.waiters(&key),
            0,
            "the dropped follower left no waiter behind"
        );
        assert!(
            flight.is_in_flight(&key),
            "only the leader's slot remains, no stray follower slot"
        );

        // With the leader gone too, nothing is in flight.
        drop(leader);
        assert!(
            !flight.is_in_flight(&key),
            "no slot is left behind once both callers are gone"
        );
    }

    #[test]
    fn s3_fifo_scan_resistance_keeps_working_set_resident() {
        let entry_size = 1024u64;
        let working_set_len = 20u64;
        let limits = CacheLimits::new(entry_size * 100, 1000, entry_size * 10);
        let cache: Cache<&'static str> = Cache::new(limits);

        let working_set: Vec<CacheKey> = (0..working_set_len).map(test_key).collect();
        for key in &working_set {
            cache.insert(*key, Bytes::from(vec![0xAAu8; entry_size as usize]));
        }
        // Access each working-set entry a second time so S3-FIFO promotes
        // it out of the small probation queue into main.
        for key in &working_set {
            assert!(cache.get(key).is_some());
        }

        // A long one-shot scan: far more distinct keys than the cache
        // holds, each admitted and never touched again -- exactly the
        // compactor/folder access pattern ADR-0046 decision 6 is written
        // against.
        for i in 1_000..1_500u64 {
            cache.insert(test_key(i), Bytes::from(vec![0xEEu8; entry_size as usize]));
        }

        let resident = working_set
            .iter()
            .filter(|key| cache.get(key).is_some())
            .count();
        assert!(
            resident as f64 >= working_set_len as f64 * 0.9,
            "expected the working set to stay resident under a one-shot scan, only {resident}/{working_set_len} survived (a plain-LRU cache would evict nearly all of it)"
        );
    }

    /// Scan resistance must hold across repeated scans, not just the first.
    ///
    /// ADR-0046 decision 6 exists because the compactor and the folder scan
    /// cold data *continuously*. An earlier version sized the ghost queue from
    /// `max_entries` rather than from resident capacity, which made the ghost
    /// many times wider than the cache: every key of a finished scan was still
    /// remembered when the next began, so each was re-admitted straight to
    /// main, bypassing probation, and the second pass evicted everything the
    /// first pass protected. That version passes the single-pass test above
    /// and fails this one at pass 1.
    #[test]
    fn s3_fifo_scan_resistance_survives_repeated_scans() {
        let entry_size = 1024u64;
        let working_set_len = 20u64;
        let limits = CacheLimits::new(entry_size * 100, 1000, entry_size * 10);
        let cache: Cache<&'static str> = Cache::new(limits);

        let working_set: Vec<CacheKey> = (0..working_set_len).map(test_key).collect();
        for key in &working_set {
            cache.insert(*key, Bytes::from(vec![0xAAu8; entry_size as usize]));
        }
        for key in &working_set {
            assert!(cache.get(key).is_some());
        }

        for pass in 0..3u64 {
            let base = 1_000 + pass * 500;
            for i in base..base + 500 {
                cache.insert(test_key(i), Bytes::from(vec![0xEEu8; entry_size as usize]));
            }
            let resident = working_set
                .iter()
                .filter(|key| cache.get(key).is_some())
                .count();
            assert!(
                resident as f64 >= working_set_len as f64 * 0.9,
                "working set collapsed on scan pass {pass}: only {resident}/{working_set_len} resident"
            );
        }
    }

    /// Hits and bytes served are split by intent and sum to the totals, a bulk
    /// hit leaves its entry in the bulk region, and the bookkeeping gauge
    /// matches the cache's own figure.
    ///
    /// FLIP: recording `record_hit` instead of `record_hit_with_intent` in
    /// `Inner::get` leaves every per-intent counter at 0.
    #[test]
    fn hits_are_counted_under_their_intent() {
        let limits = CacheLimits::new(64 * 1024 * 1024, 10_000, 16 * 1024 * 1024);
        let cache: Cache<&'static str> = Cache::new(limits);
        let bulk = test_key(1);
        let interactive = test_key(2);
        let maintenance = test_key(3);
        cache.insert_with_intent(bulk, Bytes::from(vec![1u8; 300]), AccessIntent::BulkScan);
        cache.insert(interactive, Bytes::from(vec![2u8; 200]));
        cache.insert_with_intent(
            maintenance,
            Bytes::from(vec![3u8; 100]),
            AccessIntent::Maintenance,
        );
        assert_eq!(cache.bulk_region_bytes(), 300);

        assert!(
            cache
                .get_with_intent(&bulk, AccessIntent::BulkScan)
                .is_some()
        );
        assert!(
            cache
                .get_with_intent(&bulk, AccessIntent::BulkScan)
                .is_some()
        );
        assert!(cache.get(&interactive).is_some());
        assert!(
            cache
                .get_with_intent(&maintenance, AccessIntent::Maintenance)
                .is_some()
        );

        let intents = cache.metrics().intent_snapshot();
        assert_eq!(
            intents.for_intent(AccessIntent::BulkScan),
            IntentCounters {
                hits: 2,
                bytes_served: 600
            }
        );
        assert_eq!(
            intents.interactive,
            IntentCounters {
                hits: 1,
                bytes_served: 200
            }
        );
        assert_eq!(
            intents.maintenance,
            IntentCounters {
                hits: 1,
                bytes_served: 100
            }
        );
        let totals = cache.metrics().snapshot();
        assert_eq!(totals.hits, 4);
        assert_eq!(totals.bytes_served, 900);
        assert_eq!(
            cache.bulk_region_bytes(),
            300,
            "a bulk hit stays in the region"
        );
        assert!(intents.bulk_bookkeeping_bytes > 0);
        assert_eq!(
            intents.bulk_bookkeeping_bytes,
            cache.bulk_bookkeeping_bytes()
        );
    }

    #[test]
    fn bounds_respected_under_insertion_pressure_and_oversized_entry_not_admitted() {
        let limits = CacheLimits::new(10 * 1024, 5, 4096);
        let cache: Cache<&'static str> = Cache::new(limits);

        for i in 0..50u64 {
            cache.insert(test_key(i), Bytes::from(vec![0u8; 512]));
        }
        assert!(
            cache.len() <= 5,
            "entry-count bound violated: {} entries resident",
            cache.len()
        );
        assert!(
            cache.total_bytes() <= 10 * 1024,
            "byte bound violated: {} bytes resident",
            cache.total_bytes()
        );

        let oversized_key = test_key(9_999);
        cache.insert(oversized_key, Bytes::from(vec![0u8; 8192]));
        assert!(
            cache.get(&oversized_key).is_none(),
            "an oversized entry must not be admitted"
        );
        assert_eq!(cache.metrics().snapshot().admissions_rejected_size, 1);
    }

    #[test]
    fn corruption_mode_returns_bytes_that_differ_on_every_hit() {
        let limits = CacheLimits::new(1024 * 1024, 100, 1024 * 1024);
        let cache: Cache<&'static str> = Cache::with_corruption(limits);
        let key = test_key(1);
        let original = Bytes::from_static(b"trust no cached byte");
        cache.insert(key, original.clone());

        for _ in 0..5 {
            let hit = cache.get(&key).expect("entry should be resident");
            assert_ne!(hit.as_ref(), original.as_ref());
            assert_eq!(hit.len(), original.len());
        }
    }

    /// `start_paused`: the single-flight half below orders two `get_or_fetch`
    /// calls with `tokio::time::sleep`. On a real clock that ordering is only
    /// probable -- the second call has to reach the cache inside the first
    /// call's 15 ms fetch, and a loaded host can starve the task past it, at
    /// which point the collapse never happens and the count is 0. Paused time
    /// advances only when every task is idle, so the 1 ms timer always fires
    /// before the 15 ms one and the interleaving is fixed.
    #[tokio::test(start_paused = true)]
    async fn counters_match_hand_computed_sequence() {
        let limits = CacheLimits::new(1024 * 1024, 1, 1024 * 1024);
        let cache: Cache<&'static str> = Cache::new(limits);
        let a = test_key(1);
        let b = test_key(2);

        assert!(cache.get(&a).is_none()); // miss 1
        cache.insert(a, Bytes::from_static(b"aaa"));
        assert!(cache.get(&a).is_some()); // hit 1

        // max_entries == 1, so admitting `b` must make room. The `get` above
        // bumped `a`'s freq to 1, so `a` is promoted out of the probation
        // queue into main and then evicted from main in the same pass; it does
        // not survive either way. (An earlier comment here claimed `a` was
        // evicted from probation with freq == 0, which is not what happens.)
        cache.insert(b, Bytes::from_static(b"bb"));

        assert!(cache.get(&a).is_none()); // miss 2 (evicted)
        assert!(cache.get(&b).is_some()); // hit 2

        let snap = cache.metrics().snapshot();
        assert_eq!(snap.hits, 2);
        assert_eq!(snap.misses, 2);
        assert_eq!(snap.evictions, 1);
        assert_eq!(snap.bytes_admitted, 5); // "aaa" (3) + "bb" (2)
        assert_eq!(snap.bytes_served, 5); // "aaa" (3) + "bb" (2)
        assert_eq!(snap.admissions_rejected_size, 0);
        assert_eq!(snap.single_flight_collapses, 0);

        // Single-flight collapse: two concurrent get_or_fetch calls for a
        // new key collapse into one upstream call and one collapse.
        let limits2 = CacheLimits::new(1024 * 1024, 10, 1024 * 1024);
        let cache = Arc::new(Cache::<&'static str>::new(limits2));
        let c = test_key(3);
        let (first, second) = tokio::join!(
            {
                let cache = cache.clone();
                async move {
                    cache
                        .get_or_fetch(c, || async {
                            tokio::time::sleep(Duration::from_millis(15)).await;
                            Ok::<Bytes, &'static str>(Bytes::from_static(b"c"))
                        })
                        .await
                }
            },
            {
                let cache = cache.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    cache
                        .get_or_fetch(c, || async {
                            Ok::<Bytes, &'static str>(Bytes::from_static(b"c"))
                        })
                        .await
                }
            }
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(cache.metrics().snapshot().single_flight_collapses, 1);
    }
}
