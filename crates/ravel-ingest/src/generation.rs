//! Router live switch for online resharding (ADR-0052 sections 2-3).
//!
//! Each ingest router owns a fixed shard-actor set built once at construction.
//! Resharding replaces that with a *generation-versioned* view: a tenant's
//! provisioning record ([`ravel_catalog`]) carries an append-only history of
//! `(generation, shard_count, activation_hour)` entries, and a record routed at
//! wall-clock hour `h` uses the count of the latest generation whose
//! `activation_hour <= h` (the pure rule [`ravel_catalog::active_shard_count`]).
//!
//! [`GenerationSwitch`] is the shared mechanism all three routers embed. It:
//!
//! - keeps one shard-actor set per distinct `shard_count` seen, keyed by count
//!   and shared across every tenant currently routing at that count (a set is
//!   constructed lazily via the router-supplied factory the first time a count
//!   becomes active, and old sets are never force-closed: they keep draining
//!   and flushing under their original shard indices, ADR-0052 section 2); and
//! - caches each tenant's decoded generation history plus the wall-clock time
//!   its view was last refreshed.
//!
//! **Refresh interval `C` and fail-closed staleness (ADR-0052 section 3).** A
//! router routes on a cached view only while it is younger than `C`
//! ([`DEFAULT_REFRESH_INTERVAL_NS`]). When the cache is older than `C` (or the
//! tenant has never been resolved from a persisted record), the router re-reads
//! the provisioning record and refreshes before routing, so it never routes on
//! a view older than `C`. If that re-read cannot complete (a store error, or a
//! corrupt/undecodable record whose true generation set is unknown), the router
//! **fails the flush closed** with a typed error and a metrics-visible counter,
//! rather than route on a stale or untrusted view. This is the load-bearing
//! safety property: the CLI computes a reshard's activation lead
//! `L >= ceil(C) + 1` hours, so a writer that keeps writing always refreshes and
//! observes a new generation within `C` — well before it activates — and a
//! writer that cannot reach the record stops rather than route past an
//! activation it may not have seen.
//!
//! Nothing here moves, rewrites, or re-keys existing data: a switch only
//! changes which shard-actor set *new* records route to (ADR-0052, "No data
//! movement anywhere").
//!
//! **The scan-set check at flush open (ADR-1642 scan-set amendment).** Each
//! shard actor holds a [`FlushScope`] onto its router's switch. Before a flush
//! pins ingest hour `h`, the actor asks [`GenerationSwitch::scan_check`]
//! whether its shard index is inside `ravel_catalog::scan_count(h)` for the
//! tenant, the read side's own rule, on the same cached view routing uses and
//! trusted only inside the grace-window horizon. Outside it, the rows are
//! handed back to the shard set of the tenant's current generation through
//! [`FlushScope::live_sender`]; on a view it cannot trust, the flush does not
//! open and the switch starts one background re-read of the record. A drain
//! waits for that re-read instead ([`reread_and_check`]), so one drain flushes
//! every buffer whose view the re-read confirms.
//!
//! The same check hands rows back when the hour is owned by a generation that
//! routes at another count (issue #2429): a flush writes only in hours its
//! routing generation can own, so every hour a pushdown split treats as one
//! generation's holds each key at that generation's index.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::{Arc, Weak};
use std::time::Duration;

use prost::Message;
use ravel_catalog::{
    DEFAULT_SCAN_SLACK_HOURS, ShardGeneration, active_shard_count, provisioning_key,
    read_generations_checked, scan_count, stable_generation_for_hour,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{Signal, TenantHash};
use tokio::sync::{mpsc, watch};

use crate::clock::Clock;

/// Default router refresh interval `C` (ADR-0052 section 3, an open question
/// the ADR left to the implementing task). 60 seconds: frequent enough that a
/// reshard's minimum lead of `ceil(C) + 1 = 2` hours leaves a wide margin, and
/// cheap enough that re-reading one small provisioning record per active
/// (tenant, signal) at most once per `C` is negligible. A router whose cached
/// view for a tenant is older than this re-reads the record before routing, and
/// fails the flush closed if the re-read cannot complete.
pub const DEFAULT_REFRESH_INTERVAL_NS: i64 = 60 * 1_000_000_000;

/// Nanoseconds per unix hour, the unit `activation_hour` counts in.
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Wall-clock hour bucket for a `now_ns` reading, clamped to `u32` (the width
/// `activation_hour` uses). A non-positive reading maps to hour 0.
fn hour_of(now_ns: i64) -> u32 {
    u32::try_from(now_ns.div_euclid(NS_PER_HOUR).max(0)).unwrap_or(u32::MAX)
}

/// Ceiling of `ns` in whole hours, mirroring the ADR-0052 section 3 lead-time
/// floor `L >= ceil(C) + 1`. Zero or negative maps to zero hours.
fn ceil_hours(ns: i64) -> u32 {
    if ns <= 0 {
        return 0;
    }
    u32::try_from((ns + NS_PER_HOUR - 1) / NS_PER_HOUR).unwrap_or(u32::MAX)
}

/// The ADR-0052 section 3 minimum reshard lead time `L >= ceil(C) + 1` for a
/// refresh interval `C`, in hours. The grace window reuses
/// this exact bound in reverse: a generation appended after a router's last
/// successful refresh cannot activate within `min_lead_hours(C)` hours of that
/// refresh, so a cached view stays provably authoritative for any wall-clock
/// hour strictly before that horizon, even once the view is older than `C`
/// itself. See [`GenerationSwitch::try_grace_extend`].
fn min_lead_hours(refresh_interval_ns: i64) -> u32 {
    ceil_hours(refresh_interval_ns) + 1
}

/// A tenant's cached view of its provisioning record's generation history, plus
/// when the router last refreshed it. `refreshed_at_ns` is what the staleness
/// guard compares against `C`.
///
/// `last_touched_ns` is a separate stamp used only by idle-tenant eviction
/// (ADR-0069 decision 2): it advances on every access (a cache-hit route as
/// well as a refresh), whereas `refreshed_at_ns` advances only on a refresh.
/// The two differ precisely for a tenant whose view stays fresh under `C` and
/// is hit repeatedly without a re-read: it is not idle, but its
/// `refreshed_at_ns` does not move. Eviction keys off last touch so such a
/// tenant is never evicted mid-use.
#[derive(Debug, Clone)]
struct TenantView {
    generations: Vec<ShardGeneration>,
    refreshed_at_ns: i64,
    last_touched_ns: i64,
}

/// The outcome of consulting the cache for a tenant's write ([`GenerationSwitch::route_cached`]).
pub enum Routed<H> {
    /// A cached view younger than `C` routed the write to this shard-actor set.
    Fresh(Arc<Vec<H>>),
    /// The cached view is older than `C`. The caller must re-read the
    /// provisioning record and call [`GenerationSwitch::refresh`] before
    /// routing, and fail closed if that read cannot complete.
    Stale,
}

/// A provisioning-record read for the live switch could not be trusted, so the
/// flush must fail closed (ADR-0052 section 3): a store error, or a
/// corrupt/undecodable record whose true generation set is unknown. A genuine
/// absence is not an error — it resolves to the default (generation 0) view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationLoadError;

/// What the scan-set check at flush open found (ADR-1642 scan-set amendment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanCheck {
    /// The shard index is inside the scan set of the hour: write in place.
    InScanSet,
    /// Write nothing under this shard index and hand the rows back to the
    /// `target_count`-shard set, for `reason`.
    HandBack {
        scan_count: u32,
        target_count: u32,
        reason: HandBackReason,
    },
    /// No view this router can trust for the hour: do not open the flush.
    Unknown,
}

/// Why a flush hands its rows back instead of writing them (ADR-1642 scan-set
/// amendment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandBackReason {
    /// The shard index is outside the scan set of the hour, after a decrease.
    /// The rows go to the count active at the flush-open reading.
    RetiredIndex,
    /// The index is inside the scan set, but the hour is owned alone by a
    /// generation that routes at another count (issue #2429). The rows go to
    /// the owner's count, so each lands at the index the owner routes it to.
    GenerationMismatch,
}

impl HandBackReason {
    /// The name of this reason in the hand-back WARN line, and the value of
    /// the `reason` label on `ravel_ingest_rerouted_flushes_total`.
    pub fn label(self) -> &'static str {
        match self {
            HandBackReason::RetiredIndex => "retired_index",
            HandBackReason::GenerationMismatch => "generation_mismatch",
        }
    }

    /// The `Abandoned` message a strict waiter on the handed-back buffer gets.
    pub(crate) fn abandoned_message(self) -> &'static str {
        match self {
            HandBackReason::RetiredIndex => SCAN_SET_HANDBACK_ABANDONED,
            HandBackReason::GenerationMismatch => GENERATION_HANDBACK_ABANDONED,
        }
    }
}

/// The `Abandoned` message for a [`HandBackReason::GenerationMismatch`]
/// hand-back, the counterpart of [`SCAN_SET_HANDBACK_ABANDONED`]. The waiter
/// is answered before the rows are sent, so a send that fails leaves them with
/// the source shard, which retries and may write them in place.
pub(crate) const GENERATION_HANDBACK_ABANDONED: &str = "strict ack withheld: the flush \
     would have written rows routed under one shard generation into an ingest hour another \
     generation owns; its rows are being handed to that generation's shards, and if those \
     cannot take them they stay with this shard, which may write them where they are";

/// The `Abandoned` message a strict waiter gets when its buffer is handed back
/// at flush open. The waiter is answered before the rows are sent, and they
/// are written either by a shard of the target generation or, if a send fails,
/// by this shard on a later attempt, so the outcome is unknown to this waiter
/// rather than failed.
pub(crate) const SCAN_SET_HANDBACK_ABANDONED: &str = "strict ack withheld: the flush would \
     have written under a shard index outside the read-side scan set of its ingest hour; \
     its rows are being handed to the tenant's current shard generation, and if those \
     shards cannot take them they stay with this shard for a later attempt";

/// The arrival bookkeeping of handed-back rows, carried so the receiving
/// buffer's age trigger and ingest bounds reflect when the rows really arrived
/// rather than when they were handed over.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HandBackArrival {
    pub(crate) oldest_arrival_ns: Option<i64>,
    pub(crate) min_ingest_ts_ns: Option<i64>,
    pub(crate) max_ingest_ts_ns: Option<i64>,
}

impl HandBackArrival {
    /// Widen a receiving buffer's bookkeeping to cover these rows.
    pub(crate) fn fold_into(
        self,
        oldest_arrival_ns: &mut Option<i64>,
        min_ingest_ts_ns: &mut Option<i64>,
        max_ingest_ts_ns: &mut Option<i64>,
    ) {
        fn widen(slot: &mut Option<i64>, other: Option<i64>, pick: fn(i64, i64) -> i64) {
            *slot = match (*slot, other) {
                (Some(a), Some(b)) => Some(pick(a, b)),
                (a, b) => a.or(b),
            };
        }
        widen(oldest_arrival_ns, self.oldest_arrival_ns, i64::min);
        widen(min_ingest_ts_ns, self.min_ingest_ts_ns, i64::min);
        widen(max_ingest_ts_ns, self.max_ingest_ts_ns, i64::max);
    }
}

/// A router's shard handle as the hand-back path sees it.
pub(crate) trait LiveSender<M>: Send + Sync {
    /// The live actor's mailbox, or `None` when the actor is dead or its
    /// mailbox closed: a hand-back never sends to a dead shard.
    fn live_sender(&self) -> Option<mpsc::Sender<M>>;
}

/// A shard actor's handle on its router's generation state, consulted at
/// flush open (ADR-1642 scan-set amendment).
pub(crate) trait FlushScope<M>: Send + Sync {
    /// Whether shard `shard` may write the tenant's flush that is about to pin
    /// ingest hour `hour`. `now_ns` is the flush-open clock reading, which
    /// selects the generation a hand-back routes under.
    fn check(&self, tenant: TenantHash, shard: u32, hour: u32, now_ns: i64) -> ScanCheck;
    /// The live mailbox of shard `shard` in the `count`-shard set.
    fn live_sender(&self, count: u32, shard: u32) -> Option<mpsc::Sender<M>>;
    /// Whether a hand-back to the `count`-shard set may wait for room in the
    /// target's mailbox. Only a strictly smaller set than this actor's may be
    /// awaited, so the waits between sets form no cycle; a hand-back to any
    /// other set sends only if the mailbox has room now.
    fn may_wait_on(&self, count: u32) -> bool;
    /// Re-read the tenant's provisioning record, joining a read already in
    /// flight. Resolves `true` once a read succeeded and its view is installed.
    fn reread(&self, tenant: TenantHash) -> Reread;
}

/// A hand-back send that did not deliver, with the message it returns.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SendRefused<M> {
    pub(crate) msg: M,
    /// The mailbox is closed, so its actor will never take the message. A
    /// `false` here is a full mailbox on a send that may not wait.
    pub(crate) closed: bool,
}

/// Sends one hand-back message, awaiting room in the target's mailbox only
/// when `wait` is set ([`FlushScope::may_wait_on`]). A closed or, without
/// `wait`, full mailbox returns the message.
pub(crate) async fn send_hand_back<M>(
    tx: &mpsc::Sender<M>,
    msg: M,
    wait: bool,
) -> Result<(), SendRefused<M>> {
    if wait {
        tx.send(msg).await.map_err(|err| SendRefused {
            msg: err.0,
            closed: true,
        })
    } else {
        tx.try_send(msg).map_err(|err| match err {
            mpsc::error::TrySendError::Full(msg) => SendRefused { msg, closed: false },
            mpsc::error::TrySendError::Closed(msg) => SendRefused { msg, closed: true },
        })
    }
}

/// What a shard actor's hand-back did with a buffer `B`'s rows.
pub(crate) struct HandedBack<B> {
    /// Whether any target took rows.
    pub(crate) delivered: bool,
    /// The rows no target took, with their charges: the whole buffer when a
    /// target was not live at the check, or the rows of every send that
    /// failed on a closed or full mailbox.
    pub(crate) kept: Option<B>,
    /// Whether a target that left rows in `kept` is not live: not live at the
    /// check (dead, condemned, or its mailbox closed), or closed by the send.
    /// Such a target cannot take the rows on a later flush, which a full
    /// mailbox can.
    pub(crate) target_dead: bool,
}

/// Whether a buffer whose generation-mismatch hand-back already left rows
/// undelivered, at `mismatch_held_since_ns`, has been retried for the whole
/// flush deferral cap (`cap_ns`, [`crate::IngestConfig::flush_deferral_cap_ns`]),
/// counted from the earlier of that and its queued-flush deferral, the start
/// the shard's at-cap flag reads. The flush then writes the rows in place,
/// where readers find them, rather than keep them, and with them the
/// deferral that would keep the shard refusing writes, for as long as the
/// target's mailbox stays full. A target that is not live is not retried at
/// all ([`HandedBack::target_dead`]).
pub(crate) fn mismatch_retry_spent(
    mismatch_held_since_ns: Option<i64>,
    deferred_since_ns: Option<i64>,
    now_ns: i64,
    cap_ns: i64,
) -> bool {
    mismatch_held_since_ns.is_some_and(|held| {
        let since = deferred_since_ns.map_or(held, |deferred| deferred.min(held));
        crate::deferral::deferral_cap_reached(since, now_ns, cap_ns)
    })
}

/// A pending [`FlushScope::reread`].
pub(crate) type Reread = Pin<Box<dyn Future<Output = bool> + Send>>;

/// How many synchronous re-reads one drain makes for one tenant. The second
/// covers joining a read issued long enough ago that the view it installs is
/// already past its horizon.
const DRAIN_REREADS: usize = 2;

/// The scan-set check a drain applies after a flush found no view it can
/// trust: re-read the tenant's record, waiting at most `bound` on `clock` for
/// each read, and check again. Returns [`ScanCheck::Unknown`] only when a
/// re-read failed, timed out, or installed a view still past its horizon.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reread_and_check<M>(
    scope: &dyn FlushScope<M>,
    clock: &dyn Clock,
    bound: Duration,
    tenant: TenantHash,
    shard: u32,
    hour: u32,
    now_ns: i64,
) -> ScanCheck {
    let mut verdict = ScanCheck::Unknown;
    for _ in 0..DRAIN_REREADS {
        let reread = scope.reread(tenant);
        let installed = tokio::select! {
            installed = reread => installed,
            () = clock.sleep(bound) => false,
        };
        if !installed {
            break;
        }
        verdict = scope.check(tenant, shard, hour, now_ns);
        if verdict != ScanCheck::Unknown {
            break;
        }
    }
    verdict
}

/// The production [`FlushScope`] of one shard-actor set: a weak reference to
/// the router's switch, so an actor never keeps its router's shard sets (and
/// with them every actor's mailbox) alive after the router is dropped, and the
/// shard count of the set, which is what identifies the generation that routed
/// its rows.
pub(crate) struct SwitchScope<H> {
    switch: Weak<GenerationSwitch<H>>,
    count: u32,
}

impl<H> SwitchScope<H> {
    pub(crate) fn new(switch: Weak<GenerationSwitch<H>>, count: u32) -> Self {
        SwitchScope { switch, count }
    }
}

impl<H, M> FlushScope<M> for SwitchScope<H>
where
    H: LiveSender<M> + 'static,
    M: Send + 'static,
{
    fn check(&self, tenant: TenantHash, shard: u32, hour: u32, now_ns: i64) -> ScanCheck {
        match self.switch.upgrade() {
            Some(switch) => switch.scan_check(tenant, self.count, shard, hour, now_ns),
            None => ScanCheck::Unknown,
        }
    }

    fn live_sender(&self, count: u32, shard: u32) -> Option<mpsc::Sender<M>> {
        let switch = self.switch.upgrade()?;
        let set = switch.set_for_count(count);
        set.get(shard as usize)?.live_sender()
    }

    fn may_wait_on(&self, count: u32) -> bool {
        count < self.count
    }

    fn reread(&self, tenant: TenantHash) -> Reread {
        match self.switch.upgrade() {
            Some(switch) => switch.reread(tenant),
            None => Box::pin(std::future::ready(false)),
        }
    }
}

/// A [`FlushScope`] that finds every flush inside the scan set, for unit tests
/// that drive one actor with no router behind it.
#[cfg(test)]
pub(crate) struct AlwaysInScope;

#[cfg(test)]
impl<M> FlushScope<M> for AlwaysInScope {
    fn check(&self, _tenant: TenantHash, _shard: u32, _hour: u32, _now_ns: i64) -> ScanCheck {
        ScanCheck::InScanSet
    }

    fn live_sender(&self, _count: u32, _shard: u32) -> Option<mpsc::Sender<M>> {
        None
    }

    fn may_wait_on(&self, _count: u32) -> bool {
        true
    }

    fn reread(&self, _tenant: TenantHash) -> Reread {
        Box::pin(std::future::ready(true))
    }
}

/// Collects the message of every WARN event on the thread that installed it,
/// until the guard [`WarnCapture::install`] returns is dropped. A shard actor
/// a `#[tokio::test]` spawns runs on the test's own thread, so its WARNs land
/// in that test's capture.
///
/// The layer behind it is the process-wide default, installed once, and not
/// a thread-scoped `set_default`: a callsite another test thread registers
/// while a scoped default is being registered can cache an interest that
/// leaves the scoped subscriber out, and the capture then sees nothing.
/// Because that layer marks every non-WARN callsite never, it is safe only
/// while no other unit test in this crate's `src` installs a subscriber.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct WarnCapture {
    messages: Arc<Mutex<Vec<String>>>,
}

#[cfg(test)]
thread_local! {
    static THREAD_CAPTURE: std::cell::RefCell<Option<WarnCapture>> =
        const { std::cell::RefCell::new(None) };
}

/// Hands each WARN event to the capture installed on its thread, if any.
#[cfg(test)]
struct ThreadWarnLayer;

#[cfg(test)]
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ThreadWarnLayer {
    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if *metadata.level() == tracing::Level::WARN {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        THREAD_CAPTURE.with(|slot| {
            if let Some(capture) = slot.borrow().as_ref() {
                let mut visitor = MessageVisitor::default();
                event.record(&mut visitor);
                capture
                    .messages
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(visitor.0);
            }
        });
    }
}

#[cfg(test)]
#[derive(Default)]
struct MessageVisitor(String);

#[cfg(test)]
impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// Removes this thread's capture when dropped.
#[cfg(test)]
pub(crate) struct WarnCaptureGuard;

#[cfg(test)]
impl Drop for WarnCaptureGuard {
    fn drop(&mut self) {
        THREAD_CAPTURE.with(|slot| slot.borrow_mut().take());
    }
}

#[cfg(test)]
impl WarnCapture {
    /// Captures this thread's WARN events until the guard drops.
    pub(crate) fn install(&self) -> WarnCaptureGuard {
        use tracing_subscriber::layer::SubscriberExt;
        static GLOBAL: std::sync::Once = std::sync::Once::new();
        GLOBAL.call_once(|| {
            // Err only when another global subscriber is already set, which
            // this test binary never does; the capture would then see nothing
            // and the caller's count assertion fails.
            let _ = tracing::subscriber::set_global_default(
                tracing_subscriber::registry().with(ThreadWarnLayer),
            );
        });
        THREAD_CAPTURE.with(|slot| *slot.borrow_mut() = Some(self.clone()));
        WarnCaptureGuard
    }

    /// Captured messages containing `needle`.
    pub(crate) fn count(&self, needle: &str) -> usize {
        self.messages
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|m| m.contains(needle))
            .count()
    }
}

/// A [`FlushScope`] whose verdict and hand-back target a unit test sets, so a
/// test can hand an actor a closed or absent target and read what it sends.
#[cfg(test)]
pub(crate) struct ScriptedScope<M> {
    verdict: Mutex<ScanCheck>,
    target: Mutex<Option<mpsc::Sender<M>>>,
    /// One target per shard index, used in place of `target` when non-empty.
    split: Mutex<Vec<mpsc::Sender<M>>>,
    wait: Mutex<bool>,
}

#[cfg(test)]
impl<M> ScriptedScope<M> {
    pub(crate) fn new(verdict: ScanCheck, target: Option<mpsc::Sender<M>>) -> Arc<Self> {
        Arc::new(ScriptedScope {
            verdict: Mutex::new(verdict),
            target: Mutex::new(target),
            split: Mutex::new(Vec::new()),
            wait: Mutex::new(true),
        })
    }

    pub(crate) fn set(&self, verdict: ScanCheck, target: Option<mpsc::Sender<M>>) {
        *self.verdict.lock().unwrap_or_else(|p| p.into_inner()) = verdict;
        *self.target.lock().unwrap_or_else(|p| p.into_inner()) = target;
        self.split.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    /// [`Self::set`] with target shard `i` of every set handed `targets[i]`,
    /// so one hand-back can deliver to one target and fail on another.
    pub(crate) fn set_split(&self, verdict: ScanCheck, targets: Vec<mpsc::Sender<M>>) {
        self.set(verdict, None);
        *self.split.lock().unwrap_or_else(|p| p.into_inner()) = targets;
    }

    /// Whether a hand-back may wait for room in the target's mailbox, as
    /// for a strictly smaller set; `false` scripts a larger one.
    pub(crate) fn set_wait(&self, wait: bool) {
        *self.wait.lock().unwrap_or_else(|p| p.into_inner()) = wait;
    }
}

#[cfg(test)]
impl<M: Send + 'static> FlushScope<M> for ScriptedScope<M> {
    fn check(&self, _tenant: TenantHash, _shard: u32, _hour: u32, _now_ns: i64) -> ScanCheck {
        *self.verdict.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Every shard of every set is this one target, or its split target,
    /// returned even when its mailbox is closed: a target that closes after
    /// the liveness check.
    fn live_sender(&self, _count: u32, shard: u32) -> Option<mpsc::Sender<M>> {
        let split = self.split.lock().unwrap_or_else(|p| p.into_inner());
        if !split.is_empty() {
            return split.get(shard as usize).cloned();
        }
        self.target
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn may_wait_on(&self, _count: u32) -> bool {
        *self.wait.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn reread(&self, _tenant: TenantHash) -> Reread {
        Box::pin(std::future::ready(false))
    }
}

/// Where the switch re-reads a tenant's provisioning record when a flush finds
/// no view it can trust.
struct RefreshSource {
    store: Arc<dyn ObjectStoreBackend>,
    signal: Signal,
    clock: Arc<dyn Clock>,
}

/// The last ingest hour a view refreshed at `refreshed_at_ns` is trusted for,
/// exclusive: the grace-window horizon [`GenerationSwitch::try_grace_extend`]
/// routes inside. A generation appended after the refresh cannot shrink the
/// scan set of any hour before it (ADR-1642 scan-set amendment).
fn trust_horizon(refreshed_at_ns: i64, refresh_interval_ns: i64) -> u32 {
    hour_of(refreshed_at_ns).saturating_add(min_lead_hours(refresh_interval_ns))
}

struct Inner<H> {
    /// The count a tenant with no persisted record routes at: the process's
    /// configured `shard_count`, which is generation 0's count for every tenant
    /// (the provisioning record's scalar, ADR-0050 section 5).
    default_count: u32,
    /// One shard-actor set per distinct active `shard_count`, shared across
    /// tenants at that count. Never removed: an old generation's actors drain
    /// and flush on their own (ADR-0052 section 2).
    sets: HashMap<u32, Arc<Vec<H>>>,
    /// Per-tenant cached generation history and last-refresh time.
    views: HashMap<TenantHash, TenantView>,
    /// Tenants with a flush-open re-read in flight, so a flush retried every
    /// tick against a stalled store starts one read, not one per tick, and a
    /// drain waiting for a re-read joins the one in flight. The receiver
    /// reads `Some(installed)` once the read ends.
    refreshing: HashMap<TenantHash, watch::Receiver<Option<bool>>>,
}

/// The generation-versioned shard-actor topology of one router (ADR-0052
/// sections 2-3). Generic over the router's shard-handle type `H` so the
/// metrics, log, and span routers share one implementation.
pub struct GenerationSwitch<H> {
    inner: Mutex<Inner<H>>,
    /// Builds a fresh shard-actor set of the given size. Called once per
    /// distinct active count. Each call mints a fresh writer identity inside
    /// the router, so two sets never collide on a commit key for the same shard
    /// index.
    factory: Box<dyn Fn(u32) -> Vec<H> + Send + Sync>,
    /// The refresh interval `C`: a cached tenant view older than this is
    /// re-read before routing.
    refresh_interval_ns: i64,
    /// Where a flush-open check that found no trusted view re-reads the
    /// record. `None` only for a switch built without one (unit tests), whose
    /// untrusted views stay untrusted until the router refreshes them.
    refresh_source: Option<RefreshSource>,
}

impl<H> GenerationSwitch<H> {
    /// Build a switch whose default (generation 0) set has `default_count`
    /// shards, constructed eagerly via `factory` so a never-resharded tenant
    /// routes with no lazy construction on its first write.
    pub fn new(
        default_count: u32,
        refresh_interval_ns: i64,
        factory: impl Fn(u32) -> Vec<H> + Send + Sync + 'static,
    ) -> Self {
        let mut sets = HashMap::new();
        sets.insert(default_count, Arc::new(factory(default_count)));
        GenerationSwitch {
            inner: Mutex::new(Inner {
                default_count,
                sets,
                views: HashMap::new(),
                refreshing: HashMap::new(),
            }),
            factory: Box::new(factory),
            refresh_interval_ns,
            refresh_source: None,
        }
    }

    /// Give the switch the store, signal and clock its flush-open check
    /// re-reads a tenant's provisioning record with when it finds no view it
    /// can trust (ADR-1642 scan-set amendment).
    #[must_use]
    pub(crate) fn with_refresh_source(
        mut self,
        store: Arc<dyn ObjectStoreBackend>,
        signal: Signal,
        clock: Arc<dyn Clock>,
    ) -> Self {
        self.refresh_source = Some(RefreshSource {
            store,
            signal,
            clock,
        });
        self
    }

    /// The refresh interval `C` this switch enforces.
    pub fn refresh_interval_ns(&self) -> i64 {
        self.refresh_interval_ns
    }

    /// Lock the inner state, recovering the guard if a prior holder panicked.
    /// The inner state is plain maps with no cross-field invariant a panic mid
    /// update could half-break, so recovering the poisoned guard is safe and
    /// keeps routing available rather than propagating a panic (no `expect` on a
    /// production path).
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<H>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The default (generation 0) shard count, used to resolve a tenant with no
    /// persisted provisioning record.
    pub fn default_count(&self) -> u32 {
        let inner = self.lock();
        inner.default_count
    }

    /// Route a write against the tenant's cached view. Returns [`Routed::Fresh`]
    /// with the active shard-actor set when a view younger than `C` exists, and
    /// [`Routed::Stale`] when the cached view is older than `C`, or when the
    /// tenant has never been resolved from a persisted record, and must be
    /// (re-)read before routing.
    ///
    /// A never-seen tenant cannot be assumed to be at generation 0: the ADR-0052
    /// section 3 safety argument ("a reshard's lead time `L >= ceil(C) + 1`
    /// means every *live* writer refreshes and observes a new generation before
    /// it activates") only covers a process that was already routing before the
    /// append happened. A process that starts after the append — the common
    /// case for the first write this process ever sees for a tenant — has never
    /// read the record and has no basis for assuming generation 0 is still
    /// active; a prior process could have appended and activated a new
    /// generation before this one even started. So a first-touch tenant must go
    /// through the same real store read as a stale one, not a synthesized
    /// default view.
    pub fn route_cached(&self, tenant: TenantHash, now_ns: i64) -> Routed<H> {
        let mut inner = self.lock();
        // Stamp last-touch on a cache hit so idle-tenant eviction (ADR-0069
        // decision 2) never reaps a view that is still routing writes, even one
        // whose `refreshed_at_ns` has not moved since its last re-read. `get_mut`
        // so the stamp is recorded; the borrow ends before `ensure_set`.
        let count = match inner.views.get_mut(&tenant) {
            Some(view)
                if now_ns.saturating_sub(view.refreshed_at_ns) <= self.refresh_interval_ns =>
            {
                view.last_touched_ns = now_ns;
                active_shard_count(&view.generations, hour_of(now_ns))
            }
            Some(_) | None => return Routed::Stale,
        };
        Routed::Fresh(self.ensure_set(&mut inner, count))
    }

    /// Bounded degraded-safe fallback (ADR-0052 sections 2-3):
    /// called by a router whose provisioning re-read could not complete after
    /// [`GenerationSwitch::route_cached`] returned [`Routed::Stale`], so it can
    /// continue routing on the last-known-good view instead of failing every
    /// flush closed for as long as the store stays slow or unreachable — but
    /// only inside a bounded grace window where doing so is still provably
    /// correct, never as a general staleness override.
    ///
    /// Safety predicate: continue-on-stale is safe only when the cached
    /// generation's validity horizon has not been crossed AND no pending
    /// generation change is knowable. Concretely, this returns a set only when
    /// `hour_of(now_ns) < hour_of(view.refreshed_at_ns) + min_lead_hours(C)`.
    /// The ADR's own lead-time floor `L >= ceil(C) + 1` guarantees a generation
    /// appended after `refreshed_at_ns` cannot activate before
    /// `hour_of(refreshed_at_ns) + min_lead_hours(C)`; while `now_ns` maps to an
    /// hour strictly before that horizon, no unseen append could have already
    /// activated, so `active_shard_count` computed against the *cached* history
    /// is exactly what a fresh read would also return — the cached view is not
    /// approximated, it is proven current. A genuine, already-visible future
    /// generation already in the cached history is not a risk either way, since
    /// `active_shard_count` accounts for it regardless of staleness; the only
    /// risk this predicate rules out is an append this router has never seen.
    /// Once the horizon is crossed an unseen append becomes possible and this
    /// returns `None`, so the caller fails closed exactly as it does today —
    /// correctness beats availability once the bound cannot be proven.
    ///
    /// Returns `None` for a tenant never resolved from a real read (no cached
    /// view exists to extend) and once the horizon above is crossed. The
    /// caller is responsible for emitting the "routing on grace-extended stale
    /// view" metric on `Some` and the ordinary stale-flush metric on `None`;
    /// this method only decides, it does not record.
    pub fn try_grace_extend(&self, tenant: TenantHash, now_ns: i64) -> Option<Arc<Vec<H>>> {
        let mut inner = self.lock();
        let refreshed_at_ns = inner.views.get(&tenant)?.refreshed_at_ns;
        if hour_of(now_ns) >= trust_horizon(refreshed_at_ns, self.refresh_interval_ns) {
            return None;
        }
        let count = {
            let view = inner.views.get_mut(&tenant)?;
            view.last_touched_ns = now_ns;
            active_shard_count(&view.generations, hour_of(now_ns))
        };
        Some(self.ensure_set(&mut inner, count))
    }

    /// Install a freshly-read generation history for a tenant, stamp the refresh
    /// time to `now_ns`, and return the active shard-actor set for the write.
    /// Called by the router after a successful re-read, and by the server's
    /// background refresher. An empty history is ignored (the caller passes the
    /// normalized, never-empty history [`ravel_catalog::read_generations`]
    /// returns) and falls back to the default view.
    pub fn refresh(
        &self,
        tenant: TenantHash,
        generations: Vec<ShardGeneration>,
        now_ns: i64,
    ) -> Arc<Vec<H>> {
        let mut inner = self.lock();
        let generations = if generations.is_empty() {
            vec![ShardGeneration {
                generation: 0,
                shard_count: inner.default_count,
                activation_hour: 0,
                appended_unix_ns: 0,
            }]
        } else {
            generations
        };
        let count = active_shard_count(&generations, hour_of(now_ns));
        let set = self.ensure_set(&mut inner, count);
        inner.views.insert(
            tenant,
            TenantView {
                generations,
                refreshed_at_ns: now_ns,
                last_touched_ns: now_ns,
            },
        );
        set
    }

    /// Evict every cached tenant view last touched before `now_ns - ttl_ns`
    /// (ADR-0069 decision 2, idle-tenant state eviction). Returns the number of
    /// views dropped.
    ///
    /// Only the per-tenant `views` map is swept: the `sets` map holds one
    /// shard-actor set per distinct active `shard_count`, shared across every
    /// tenant at that count and drained on its own (ADR-0052 section 2), so it
    /// is topology, not per-tenant idle state, and is never touched here.
    ///
    /// Evicting a view is safe because it is re-derivable: the next
    /// [`GenerationSwitch::route_cached`] for that tenant returns
    /// [`Routed::Stale`] exactly as a first-touch tenant does, so the router
    /// re-reads the provisioning record and refreshes before routing. The cost
    /// is one provisioning-record read on the tenant's next write, bounded and
    /// rare (ADR-0069 consequences).
    ///
    /// `ttl_ns` is the injected idle threshold and `now_ns` the injected clock
    /// reading; this type never reads a clock (the caller's sweep loop supplies
    /// both), so eviction is deterministic under test.
    pub fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        let mut inner = self.lock();
        let before = inner.views.len();
        inner
            .views
            .retain(|_, view| now_ns.saturating_sub(view.last_touched_ns) <= ttl_ns);
        before - inner.views.len()
    }

    /// Every live shard-actor set, for the router's `flush_all`/`shutdown`
    /// fan-out. Includes retiring generations so their buffers flush too.
    pub fn all_sets(&self) -> Vec<Arc<Vec<H>>> {
        let inner = self.lock();
        inner.sets.values().cloned().collect()
    }

    /// The largest live shard-actor set whose count is not in `drained`, for a
    /// shutdown that drains one set at a time. A retired-index hand-back
    /// always goes to a strictly smaller set, which is drained after the
    /// source; a generation-mismatch hand-back to a larger set reaches one
    /// that is draining or drained, whose closed mailbox refuses it and leaves
    /// the rows with the source. A hand-back may construct its target set
    /// mid-drain, so the caller asks again after each set rather than draining
    /// a list taken once: every set is drained, and none before a larger one.
    pub(crate) fn largest_undrained_set(&self, drained: &[u32]) -> Option<(u32, Arc<Vec<H>>)> {
        let inner = self.lock();
        inner
            .sets
            .iter()
            .filter(|(count, _)| !drained.contains(count))
            .max_by_key(|(count, _)| **count)
            .map(|(count, set)| (*count, Arc::clone(set)))
    }

    /// The shard-actor set for `count`, constructed if no tenant has routed at
    /// that count yet.
    pub(crate) fn set_for_count(&self, count: u32) -> Arc<Vec<H>> {
        let mut inner = self.lock();
        self.ensure_set(&mut inner, count)
    }

    /// Get or lazily construct the shard-actor set for `count`.
    fn ensure_set(&self, inner: &mut Inner<H>, count: u32) -> Arc<Vec<H>> {
        if let Some(set) = inner.sets.get(&count) {
            return Arc::clone(set);
        }
        let set = Arc::new((self.factory)(count));
        inner.sets.insert(count, Arc::clone(&set));
        set
    }
}

impl<H: Send + Sync + 'static> GenerationSwitch<H> {
    /// The scan-set check at flush open (ADR-1642 scan-set amendment): whether
    /// shard `shard` of the `count`-shard set may write a flush of `tenant`
    /// that is about to pin ingest hour `hour`, by the read side's own rules,
    /// `ravel_catalog::scan_count` and `ravel_catalog::stable_generation_for_hour`
    /// with [`DEFAULT_SCAN_SLACK_HOURS`].
    ///
    /// The cached view is trusted for `hour` only before [`trust_horizon`], the
    /// grace-window horizon routing uses, so a view younger than `C` always
    /// qualifies. A tenant with no view, or one past its horizon, is
    /// [`ScanCheck::Unknown`]: the caller does not open the flush, and this
    /// starts one background re-read of the tenant's record. An index outside
    /// the scan set is [`ScanCheck::HandBack`] to the count active at
    /// `now_ns`, which is never above `scan_count(hour)` because the latest
    /// generation activated by `now_ns` stays in the scan set of every later
    /// hour until `S` hours past its successor; a view that claimed otherwise
    /// is treated as untrusted rather than handed a loop.
    ///
    /// An index inside the scan set is still handed back, with
    /// [`HandBackReason::GenerationMismatch`], when one generation owns `hour`
    /// alone and routes at a count other than `count` (issue #2429): written
    /// in place, its rows would sit at indices the owner does not route them
    /// to, in an hour a pushdown split treats as the owner's. The rows go to
    /// the owner's set, whose own check passes them. The generation that
    /// routed the rows is identified by `count`, since the sets are keyed by
    /// count and two generations with one count route every row alike. An
    /// hour no single generation owns, the overlap after an activation, writes
    /// in place as the scan set allows.
    pub(crate) fn scan_check(
        self: &Arc<Self>,
        tenant: TenantHash,
        count: u32,
        shard: u32,
        hour: u32,
        now_ns: i64,
    ) -> ScanCheck {
        let verdict = {
            let inner = self.lock();
            match inner.views.get(&tenant) {
                Some(view)
                    if hour < trust_horizon(view.refreshed_at_ns, self.refresh_interval_ns) =>
                {
                    let scan = scan_count(&view.generations, hour, DEFAULT_SCAN_SLACK_HOURS);
                    let active = active_shard_count(&view.generations, hour_of(now_ns));
                    if shard < scan {
                        // `Err` for an owner the view does not list. The owner
                        // is read from this same list, so that cannot happen;
                        // it fails closed like an untrusted view rather than
                        // write.
                        let owner = match stable_generation_for_hour(
                            &view.generations,
                            hour,
                            DEFAULT_SCAN_SLACK_HOURS,
                        ) {
                            None => Ok(None),
                            Some(owner) => view
                                .generations
                                .iter()
                                .find(|g| g.generation == owner)
                                .map(|owner| Some(owner.shard_count))
                                .ok_or(()),
                        };
                        match owner {
                            Ok(Some(owner)) if owner != count => ScanCheck::HandBack {
                                scan_count: scan,
                                target_count: owner,
                                reason: HandBackReason::GenerationMismatch,
                            },
                            Ok(_) => ScanCheck::InScanSet,
                            Err(()) => ScanCheck::Unknown,
                        }
                    } else if active <= shard {
                        ScanCheck::HandBack {
                            scan_count: scan,
                            target_count: active,
                            reason: HandBackReason::RetiredIndex,
                        }
                    } else {
                        ScanCheck::Unknown
                    }
                }
                Some(_) | None => ScanCheck::Unknown,
            }
        };
        if verdict == ScanCheck::Unknown {
            self.request_refresh(tenant);
        }
        verdict
    }

    /// Start one background re-read of `tenant`'s provisioning record unless
    /// one is already in flight.
    fn request_refresh(self: &Arc<Self>, tenant: TenantHash) {
        let _ = self.start_refresh(tenant);
    }

    /// Re-read `tenant`'s provisioning record, joining a read already in
    /// flight, and resolve `true` once a read succeeded and its view is
    /// installed. A switch with no refresh source resolves `false`.
    pub(crate) fn reread(self: &Arc<Self>, tenant: TenantHash) -> Reread {
        let Some(mut done) = self.start_refresh(tenant) else {
            return Box::pin(std::future::ready(false));
        };
        Box::pin(async move {
            match done.wait_for(Option::is_some).await {
                Ok(outcome) => *outcome == Some(true),
                Err(_) => false,
            }
        })
    }

    /// The in-flight re-read of `tenant`'s record, started here if none is. A
    /// successful read is installed stamped with the time the read was issued,
    /// not when it returned, since the record may have been appended to while
    /// the read was in flight. The task holds only a weak reference, so a
    /// store that never answers cannot keep the router's shard sets alive
    /// after the router is dropped.
    fn start_refresh(
        self: &Arc<Self>,
        tenant: TenantHash,
    ) -> Option<watch::Receiver<Option<bool>>> {
        let source = self.refresh_source.as_ref()?;
        let (default_count, done_tx, done_rx) = {
            let mut inner = self.lock();
            if let Some(in_flight) = inner.refreshing.get(&tenant) {
                return Some(in_flight.clone());
            }
            let (done_tx, done_rx) = watch::channel(None);
            inner.refreshing.insert(tenant, done_rx.clone());
            (inner.default_count, done_tx, done_rx)
        };
        let store = Arc::clone(&source.store);
        let clock = Arc::clone(&source.clock);
        let signal = source.signal;
        let switch = Arc::downgrade(self);
        tokio::spawn(async move {
            let issued_ns = clock.now_ns();
            let loaded = load_generations(store.as_ref(), signal, &tenant, default_count).await;
            let installed = loaded.is_ok();
            if let Some(switch) = switch.upgrade() {
                let mut inner = switch.lock();
                inner.refreshing.remove(&tenant);
                if let Ok(generations) = loaded {
                    install_if_newer(&mut inner.views, tenant, generations, issued_ns);
                }
            }
            done_tx.send_replace(Some(installed));
        });
        Some(done_rx)
    }
}

/// Install a re-read view unless the cache already holds one refreshed at or
/// after `refreshed_at_ns`: a router write may have refreshed the tenant while
/// the background read was in flight.
fn install_if_newer(
    views: &mut HashMap<TenantHash, TenantView>,
    tenant: TenantHash,
    generations: Vec<ShardGeneration>,
    refreshed_at_ns: i64,
) {
    if generations.is_empty() {
        return;
    }
    match views.get_mut(&tenant) {
        Some(view) if view.refreshed_at_ns >= refreshed_at_ns => {}
        Some(view) => {
            view.generations = generations;
            view.refreshed_at_ns = refreshed_at_ns;
        }
        None => {
            views.insert(
                tenant,
                TenantView {
                    generations,
                    refreshed_at_ns,
                    last_touched_ns: refreshed_at_ns,
                },
            );
        }
    }
}

/// Read and decode the current shard-generation history for one (tenant,
/// signal) for a live-switch refresh (ADR-0052 section 3). A genuine absence
/// (`NotFound`) resolves to the single implicit generation 0 at `default_count`
/// — a tenant whose record has not been written yet routes at the configured
/// count. A store error, an undecodable/corrupt record, a record from a future
/// format version, or a record misfiled for a different tenant or signal is a
/// [`GenerationLoadError`]: the caller fails the flush closed rather than route
/// on an untrusted view. Uses [`read_generations_checked`], not
/// [`read_generations`] directly, because this path reads the record straight
/// off the store rather than through [`ravel_catalog::validate_or_adopt`],
/// which is where that guard normally lives.
pub(crate) async fn load_generations(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    tenant: &TenantHash,
    default_count: u32,
) -> Result<Vec<ShardGeneration>, GenerationLoadError> {
    let key = provisioning_key(tenant, signal);
    match store.get(&key, GetRange::Full).await {
        Ok(outcome) => {
            let record = sysproto::ProvisioningRecord::decode(outcome.data.as_ref())
                .map_err(|_| GenerationLoadError)?;
            read_generations_checked(&record, &key, tenant, signal).map_err(|_| GenerationLoadError)
        }
        Err(StoreError::NotFound) => Ok(vec![ShardGeneration {
            generation: 0,
            shard_count: default_count,
            activation_hour: 0,
            appended_unix_ns: 0,
        }]),
        Err(_) => Err(GenerationLoadError),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn gen0(count: u32) -> ShardGeneration {
        ShardGeneration {
            generation: 0,
            shard_count: count,
            activation_hour: 0,
            appended_unix_ns: 0,
        }
    }

    fn gen1(count: u32, activation_hour: u32) -> ShardGeneration {
        ShardGeneration {
            generation: 1,
            shard_count: count,
            activation_hour,
            appended_unix_ns: 0,
        }
    }

    fn index_switch(default_count: u32, c_ns: i64) -> GenerationSwitch<u32> {
        GenerationSwitch::new(default_count, c_ns, |count| (0..count).collect())
    }

    fn tenant(byte: u8) -> TenantHash {
        TenantHash([byte; 16])
    }

    /// A never-seen tenant reports Stale on its first write: this process has
    /// never read the tenant's provisioning record, so it cannot assume
    /// generation 0 is still active (a prior process could have appended and
    /// activated a reshard before this one started). The caller must do a real
    /// store read via `load_generations` and `refresh` before routing.
    #[test]
    fn first_touch_tenant_is_stale_and_requires_a_real_read() {
        let sw = index_switch(4, DEFAULT_REFRESH_INTERVAL_NS);
        assert!(matches!(
            sw.route_cached(tenant(1), 10 * NS_PER_HOUR),
            Routed::Stale
        ));
        // After the caller refreshes from the (in this case NotFound-mapped)
        // record, routing resumes from cache at the default count.
        let set = sw.refresh(tenant(1), vec![gen0(4)], 10 * NS_PER_HOUR);
        assert_eq!(set.len(), 4);
    }

    /// The live switch: after refreshing to a history whose latest active
    /// generation is 8 shards, routing lands on the count-8 set, while the
    /// count-4 set handed out before the switch stays valid (an in-flight write
    /// on the old generation completes; the old actors are not force-closed).
    #[test]
    fn refresh_switches_routing_and_old_set_survives() {
        let sw = index_switch(4, DEFAULT_REFRESH_INTERVAL_NS);
        let t = tenant(2);
        let history = vec![gen0(4), gen1(8, 100)];

        // Before activation (hour 50): active count is still 4.
        let before_ns = 50 * NS_PER_HOUR;
        let old_set = sw.refresh(t, history.clone(), before_ns);
        assert_eq!(old_set.len(), 4);

        // At activation (hour 100): active count becomes 8.
        let after_ns = 100 * NS_PER_HOUR;
        let new_set = sw.refresh(t, history, after_ns);
        assert_eq!(new_set.len(), 8);

        // The set handed out before the switch is still alive and usable.
        assert_eq!(
            old_set.len(),
            4,
            "the old generation's set survives the switch"
        );
    }

    /// A cached view younger than `C` routes from cache; once it ages past `C`
    /// the switch reports `Stale` so the router re-reads before routing.
    #[test]
    fn cached_view_goes_stale_past_c() {
        let c = DEFAULT_REFRESH_INTERVAL_NS;
        let sw = index_switch(4, c);
        let t = tenant(3);
        let t0 = 10 * NS_PER_HOUR;
        sw.refresh(t, vec![gen0(4)], t0);

        // Just within C: routes from cache.
        assert!(
            matches!(sw.route_cached(t, t0 + c), Routed::Fresh(_)),
            "within C routes from cache"
        );
        // Past C: stale, must re-read.
        assert!(
            matches!(sw.route_cached(t, t0 + c + 1), Routed::Stale),
            "past C reports stale"
        );
        // A refresh clears staleness.
        sw.refresh(t, vec![gen0(4)], t0 + c + 1);
        assert!(matches!(sw.route_cached(t, t0 + c + 1), Routed::Fresh(_)));
    }

    /// ADR-0069 decision 2: an idle tenant's generation view is
    /// evicted once it has gone untouched past the TTL, its next access
    /// re-derives it by re-reading the provisioning record and succeeds, and a
    /// tenant still being written survives the same sweep.
    ///
    /// Deterministic via the injected `now_ns`: no clock is read anywhere on
    /// this path. The re-read step drives the real [`load_generations`] free
    /// function against a `MemoryStore` (a `NotFound` record resolves to the
    /// implicit generation 0, a genuine successful re-read), then `refresh`
    /// reinstalls the view exactly as the router's `active_set` does after a
    /// `Routed::Stale`.
    #[tokio::test]
    async fn idle_tenant_state_evicted_and_rederived() {
        use ravel_object_store::memory::MemoryStore;

        let ttl_ns = 100 * NS_PER_HOUR;
        // A refresh interval `C` far larger than the whole test window, so a
        // cached view never goes stale-by-`C`: this isolates the last-touch
        // refinement (which advances on a cache-hit route, not only a refresh)
        // as the sole thing keeping the active tenant alive across the sweep.
        let big_c = 1_000_000 * NS_PER_HOUR;
        let sw = index_switch(4, big_c);
        let idle = tenant(1);
        let active = tenant(2);

        // Both tenants routed at t0: each gets a cached view.
        let t0 = 1_000 * NS_PER_HOUR;
        sw.refresh(idle, vec![gen0(4)], t0);
        sw.refresh(active, vec![gen0(4)], t0);

        // The active tenant keeps writing: a cache-hit route just before the
        // sweep advances its last-touch even though its `refreshed_at_ns` (t0)
        // does not move. This is the case last-touch tracking exists to protect.
        let sweep_ns = t0 + ttl_ns + 1;
        assert!(
            matches!(sw.route_cached(active, sweep_ns), Routed::Fresh(_)),
            "the active tenant routes from cache right before the sweep"
        );

        // Sweep: the idle tenant is past the TTL (last touched at t0), the
        // active tenant was just touched, so exactly one view is evicted.
        let evicted = sw.evict_idle(sweep_ns, ttl_ns);
        assert_eq!(evicted, 1, "only the idle tenant's view is evicted");

        // The active tenant's view survives: still routes from cache.
        assert!(
            matches!(sw.route_cached(active, sweep_ns), Routed::Fresh(_)),
            "the active tenant's view survives the sweep"
        );
        // The idle tenant's view is gone: it now reports Stale exactly like a
        // first-touch tenant, which is the trigger for the router to re-read.
        assert!(
            matches!(sw.route_cached(idle, sweep_ns), Routed::Stale),
            "the evicted view forces a re-read on the next access"
        );

        // Re-derive: the router's stale path re-reads the provisioning record
        // via `load_generations` and refreshes. The record is absent, which is
        // a successful read resolving to the implicit generation 0.
        let store = MemoryStore::new();
        let generations = load_generations(&store, Signal::Metrics, &idle, sw.default_count())
            .await
            .expect("re-read of the provisioning record succeeds");
        let set = sw.refresh(idle, generations, sweep_ns);
        assert_eq!(
            set.len(),
            4,
            "the re-derived view routes to the default set"
        );
        assert!(
            matches!(sw.route_cached(idle, sweep_ns), Routed::Fresh(_)),
            "after re-derivation the idle tenant routes from cache again"
        );
    }

    /// `min_lead_hours` mirrors the ADR-0052 section 3 lead-time floor `L >=
    /// ceil(C) + 1` for a range of refresh intervals, including the default and
    /// a non-hour-aligned one, and is deterministic (repeat calls agree).
    #[test]
    fn min_lead_hours_matches_ceil_c_plus_one() {
        assert_eq!(min_lead_hours(0), 1, "C = 0 still requires 1 hour of lead");
        assert_eq!(min_lead_hours(DEFAULT_REFRESH_INTERVAL_NS), 2, "C = 60s");
        assert_eq!(
            min_lead_hours(NS_PER_HOUR + 1),
            3,
            "C just over 1h ceils to 2h, plus 1"
        );
        assert_eq!(
            min_lead_hours(DEFAULT_REFRESH_INTERVAL_NS),
            min_lead_hours(DEFAULT_REFRESH_INTERVAL_NS),
            "deterministic: no clock read, repeat calls agree"
        );
    }

    /// A router whose cached view has gone stale by `C` but
    /// whose `shard_count` provably has not (and cannot have) changed continues
    /// routing inside the bounded grace window, rather than failing every flush
    /// closed. `min_lead_hours(C) = 2` for the default `C`, so the grace window
    /// here spans wall-clock hours `[10, 12)` from a refresh at hour 10.
    #[test]
    fn try_grace_extend_continues_routing_within_horizon() {
        let c = DEFAULT_REFRESH_INTERVAL_NS;
        let sw = index_switch(4, c);
        let t = tenant(5);
        let t0 = 10 * NS_PER_HOUR;
        sw.refresh(t, vec![gen0(4)], t0);

        // Past C (a store re-read could not complete), but still inside the
        // horizon: continues routing on the cached view.
        let still_within_horizon_ns = 11 * NS_PER_HOUR;
        let set = sw
            .try_grace_extend(t, still_within_horizon_ns)
            .expect("within the horizon, grace-extension continues routing");
        assert_eq!(
            set.len(),
            4,
            "routes with the cached, unchanged shard_count"
        );
    }

    /// Once the grace horizon is crossed, an unseen generation append
    /// becomes possible, so the switch refuses to extend and the caller must
    /// fail the flush closed exactly as it did before this fallback existed.
    #[test]
    fn try_grace_extend_refuses_once_horizon_crossed() {
        let c = DEFAULT_REFRESH_INTERVAL_NS;
        let sw = index_switch(4, c);
        let t = tenant(6);
        let t0 = 10 * NS_PER_HOUR;
        sw.refresh(t, vec![gen0(4)], t0);

        // Horizon is hour 10 + min_lead_hours(C) = hour 12. At hour 12 an
        // append this router never saw could already have activated, so no
        // amount of "shard_count looks unchanged in the cached view" is provable
        // any more.
        let horizon_crossed_ns = 12 * NS_PER_HOUR;
        assert!(
            sw.try_grace_extend(t, horizon_crossed_ns).is_none(),
            "past the horizon, grace-extension must refuse (fail closed)"
        );
    }

    /// A tenant never resolved from a real read has no cached view to
    /// extend, so grace-extension cannot help it either -- it must go through
    /// the normal re-read (and fail closed if that fails), identically to a
    /// tenant whose view has gone stale.
    #[test]
    fn try_grace_extend_refuses_for_a_never_resolved_tenant() {
        let sw = index_switch(4, DEFAULT_REFRESH_INTERVAL_NS);
        let t = tenant(7);
        assert!(
            sw.try_grace_extend(t, 10 * NS_PER_HOUR).is_none(),
            "no cached view exists yet, so there is nothing provable to extend"
        );
    }

    /// Grace-extension is exactly as safe when a future generation change
    /// is already visible in the cached view -- `active_shard_count` accounts
    /// for it regardless of staleness, so routing through the grace window on a
    /// tenant with a known upcoming reshard still returns the count that will
    /// actually be active at `now_ns`, not a stale pre-reshard count.
    #[test]
    fn try_grace_extend_honors_a_known_future_generation() {
        let c = DEFAULT_REFRESH_INTERVAL_NS;
        let sw = index_switch(4, c);
        let t = tenant(8);
        let t0 = 10 * NS_PER_HOUR;
        // The cached view already knows about a reshard to 8 shards activating
        // at hour 11, refreshed at hour 10.
        sw.refresh(t, vec![gen0(4), gen1(8, 11)], t0);

        // Still within the grace horizon (hour 10 + 2 = 12), and past the known
        // activation: routes at the new count, not the stale old one.
        let after_known_activation_ns = 11 * NS_PER_HOUR;
        let set = sw
            .try_grace_extend(t, after_known_activation_ns)
            .expect("within the horizon, grace-extension continues routing");
        assert_eq!(
            set.len(),
            8,
            "a known future generation is honored even through the grace window"
        );
    }

    /// The flush-open check against a 4 to 3 decrease activating at hour 100,
    /// on a view refreshed at hour 102 (trusted before hour 104): the retired
    /// index 3 is in the scan set through hour 102 (`S` = 3), handed back to
    /// the 3-shard set from hour 103, and an hour past the trust horizon or a
    /// tenant with no view is unknown. An index the successor covers writes in
    /// place through the overlap, hours 100 to 102, and from hour 103, which
    /// the 3-shard generation owns alone, a 4-shard flush on it is handed back
    /// to the owner while a 3-shard flush on it writes in place.
    #[test]
    fn scan_check_follows_the_read_side_rule_inside_the_trust_horizon() {
        let sw = Arc::new(index_switch(4, DEFAULT_REFRESH_INTERVAL_NS));
        let t = tenant(9);
        sw.refresh(t, vec![gen0(4), gen1(3, 100)], 102 * NS_PER_HOUR);
        assert_eq!(
            sw.scan_check(t, 4, 3, 102, 102 * NS_PER_HOUR),
            ScanCheck::InScanSet
        );
        assert_eq!(
            sw.scan_check(t, 4, 3, 103, 103 * NS_PER_HOUR),
            ScanCheck::HandBack {
                scan_count: 3,
                target_count: 3,
                reason: HandBackReason::RetiredIndex,
            }
        );
        for hour in 100..=102 {
            assert_eq!(
                sw.scan_check(t, 4, 2, hour, 102 * NS_PER_HOUR),
                ScanCheck::InScanSet,
                "hour {hour} is inside the activation overlap"
            );
        }
        assert_eq!(
            sw.scan_check(t, 4, 2, 103, 103 * NS_PER_HOUR),
            ScanCheck::HandBack {
                scan_count: 3,
                target_count: 3,
                reason: HandBackReason::GenerationMismatch,
            }
        );
        assert_eq!(
            sw.scan_check(t, 3, 2, 103, 103 * NS_PER_HOUR),
            ScanCheck::InScanSet
        );
        assert_eq!(
            sw.scan_check(t, 4, 3, 104, 104 * NS_PER_HOUR),
            ScanCheck::Unknown
        );
        assert_eq!(
            sw.scan_check(tenant(10), 4, 0, 102, 102 * NS_PER_HOUR),
            ScanCheck::Unknown
        );
    }

    /// The generation-mismatch hand-back compares shard counts: after 4 to 3
    /// to 4, an hour the second 4-shard generation owns alone takes a flush of
    /// the first generation's 4-shard set in place, since both route every row
    /// alike. On an increase, 3 to 4, a 3-shard flush into an hour the 4-shard
    /// generation owns is handed back up to the 4-shard set, and before the
    /// increase an hour generation 0 owns takes it in place.
    #[test]
    fn generation_mismatch_compares_the_routing_count() {
        let sw = Arc::new(index_switch(4, DEFAULT_REFRESH_INTERVAL_NS));
        let t = tenant(11);
        let history = vec![
            gen0(4),
            gen1(3, 100),
            ShardGeneration {
                generation: 2,
                shard_count: 4,
                activation_hour: 110,
                appended_unix_ns: 0,
            },
        ];
        sw.refresh(t, history, 114 * NS_PER_HOUR);
        assert_eq!(
            sw.scan_check(t, 4, 1, 114, 114 * NS_PER_HOUR),
            ScanCheck::InScanSet
        );
        assert_eq!(
            sw.scan_check(t, 3, 1, 114, 114 * NS_PER_HOUR),
            ScanCheck::HandBack {
                scan_count: 4,
                target_count: 4,
                reason: HandBackReason::GenerationMismatch,
            }
        );

        let up = tenant(12);
        sw.refresh(up, vec![gen0(3), gen1(4, 100)], 103 * NS_PER_HOUR);
        assert_eq!(
            sw.scan_check(up, 3, 2, 103, 103 * NS_PER_HOUR),
            ScanCheck::HandBack {
                scan_count: 4,
                target_count: 4,
                reason: HandBackReason::GenerationMismatch,
            }
        );
        assert_eq!(
            sw.scan_check(up, 3, 2, 99, 103 * NS_PER_HOUR),
            ScanCheck::InScanSet
        );
    }

    /// The generation-mismatch hand-back targets the owner of the pinned hour,
    /// while routing uses the generation active at the flush-open clock
    /// reading. The pinned hour comes from the flush-open stamp, which the
    /// ADR-1307 floor holds at most `MAX_FLUSH_CLOCK_HOLD_NS` (300 s) ahead of
    /// that reading, so it is at most one hour later; an owned hour is at
    /// least `DEFAULT_SCAN_SLACK_HOURS` (3) hours past its owner's activation,
    /// and the reading is never later than the stamp. Hence whenever the
    /// pinned hour has an owner, the reading's active generation is that
    /// owner, and the hand-back goes to the current generation's set. Checked
    /// over every hour and hold of three histories, through `scan_check`
    /// itself, and shown to depend on the bound: a hold longer than the
    /// overlap reaches an owned hour from a reading before its activation.
    #[test]
    fn the_owner_of_the_pinned_hour_is_active_at_the_reading() {
        use crate::config::MAX_FLUSH_CLOCK_HOLD_NS;
        let overlap_ns = i64::from(DEFAULT_SCAN_SLACK_HOURS) * NS_PER_HOUR;
        const { assert!(MAX_FLUSH_CLOCK_HOLD_NS < NS_PER_HOUR) };
        assert!(MAX_FLUSH_CLOCK_HOLD_NS < overlap_ns);
        let gen2 = |count: u32, activation_hour: u32| ShardGeneration {
            generation: 2,
            shard_count: count,
            activation_hour,
            appended_unix_ns: 0,
        };
        let histories = [
            vec![gen0(4), gen1(3, 100)],
            vec![gen0(3), gen1(4, 100)],
            vec![gen0(4), gen1(3, 100), gen2(4, 106)],
        ];
        let owner_count = |history: &[ShardGeneration], hour: u32| {
            stable_generation_for_hour(history, hour, DEFAULT_SCAN_SLACK_HOURS).map(|owner| {
                history
                    .iter()
                    .find(|g| g.generation == owner)
                    .map(|g| g.shard_count)
            })
        };
        // A reading whose active generation is not the owner of the hour
        // `hold` later pins, if any.
        let disagreement = |history: &[ShardGeneration], hold: i64| {
            (95 * NS_PER_HOUR..115 * NS_PER_HOUR)
                .step_by((NS_PER_HOUR / 4) as usize)
                .flat_map(|ns| [ns, ns + NS_PER_HOUR / 4 - 1])
                .find(|&raw_ns| {
                    owner_count(history, hour_of(raw_ns + hold)).is_some_and(|owner| {
                        owner != Some(active_shard_count(history, hour_of(raw_ns)))
                    })
                })
        };
        for (i, history) in histories.iter().enumerate() {
            for hold in [0, 1, MAX_FLUSH_CLOCK_HOLD_NS / 2, MAX_FLUSH_CLOCK_HOLD_NS] {
                assert_eq!(
                    disagreement(history, hold),
                    None,
                    "history {i}, hold {hold} ns"
                );
            }
        }
        assert!(
            disagreement(&histories[0], overlap_ns + NS_PER_HOUR / 2).is_some(),
            "a hold longer than the overlap would hand back to a generation not yet active"
        );

        // Through the check itself: a 4-shard flush pinned one hold ahead of
        // a reading inside the 3-shard generation's owned hours is handed to
        // the 3-shard set, the one active at the reading.
        let sw = Arc::new(index_switch(4, DEFAULT_REFRESH_INTERVAL_NS));
        let t = tenant(13);
        sw.refresh(t, histories[0].clone(), 104 * NS_PER_HOUR);
        let raw_ns = 104 * NS_PER_HOUR - 1;
        let pinned = hour_of(raw_ns + MAX_FLUSH_CLOCK_HOLD_NS);
        assert_eq!(pinned, 104);
        assert_eq!(
            sw.scan_check(t, 4, 1, pinned, raw_ns),
            ScanCheck::HandBack {
                scan_count: 3,
                target_count: active_shard_count(&histories[0], hour_of(raw_ns)),
                reason: HandBackReason::GenerationMismatch,
            }
        );
    }

    /// Only a strictly smaller set is awaited by a hand-back, and a send that
    /// may not wait returns the message when the mailbox is full.
    #[tokio::test]
    async fn a_hand_back_waits_only_on_a_smaller_set() {
        struct NoSender;
        impl LiveSender<()> for NoSender {
            fn live_sender(&self) -> Option<mpsc::Sender<()>> {
                None
            }
        }
        let scope = SwitchScope::<NoSender>::new(Weak::new(), 4);
        assert!(FlushScope::<()>::may_wait_on(&scope, 3));
        assert!(!FlushScope::<()>::may_wait_on(&scope, 4));
        assert!(!FlushScope::<()>::may_wait_on(&scope, 5));

        let (tx, mut rx) = mpsc::channel::<u32>(1);
        assert_eq!(send_hand_back(&tx, 1, false).await, Ok(()));
        assert_eq!(
            send_hand_back(&tx, 2, false).await,
            Err(SendRefused {
                msg: 2,
                closed: false
            }),
            "a full mailbox is not a closed one"
        );
        assert_eq!(rx.recv().await, Some(1));
        drop(rx);
        assert_eq!(
            send_hand_back(&tx, 3, true).await,
            Err(SendRefused {
                msg: 3,
                closed: true
            })
        );
        assert_eq!(
            send_hand_back(&tx, 4, false).await,
            Err(SendRefused {
                msg: 4,
                closed: true
            })
        );
    }

    /// Handed-back rows widen the receiving buffer's arrival bookkeeping in
    /// the direction each field means: oldest and min down, max up.
    #[test]
    fn hand_back_arrival_widens_the_receiving_buffer() {
        let arrival = HandBackArrival {
            oldest_arrival_ns: Some(10),
            min_ingest_ts_ns: Some(10),
            max_ingest_ts_ns: Some(20),
        };
        let (mut oldest, mut min, mut max) = (Some(15), Some(15), Some(15));
        arrival.fold_into(&mut oldest, &mut min, &mut max);
        assert_eq!((oldest, min, max), (Some(10), Some(10), Some(20)));
        let (mut oldest, mut min, mut max) = (None, None, None);
        arrival.fold_into(&mut oldest, &mut min, &mut max);
        assert_eq!((oldest, min, max), (Some(10), Some(10), Some(20)));
    }

    /// `all_sets` exposes every live generation's set for the flush/shutdown
    /// fan-out, including a retired one.
    #[test]
    fn all_sets_includes_every_generation() {
        let sw = index_switch(4, DEFAULT_REFRESH_INTERVAL_NS);
        let t = tenant(4);
        sw.refresh(t, vec![gen0(4), gen1(8, 100)], 100 * NS_PER_HOUR);
        let lens: std::collections::BTreeSet<usize> =
            sw.all_sets().iter().map(|s| s.len()).collect();
        assert!(lens.contains(&4), "the default generation set is present");
        assert!(lens.contains(&8), "the activated generation set is present");
    }
}
