//! The compaction side of the advisory claim protocol (ADR-1029 decisions 3
//! to 5): take a per-bucket claim before an expensive merge, consult it at the
//! pipeline's quiescent checkpoints, and cancel a run whose claim is gone.
//!
//! # What this buys, and what it does not
//!
//! A claim suppresses DUPLICATE WORK and nothing else. Two processes that
//! compact one sealed bucket already converge on a single compaction record at
//! its `CreateIfAbsent` ([`crate::publish`]), so a duplicate is a cost problem,
//! never a correctness one. The claim confers zero publication rights and its
//! absence removes none (ADR-1029 decision 2): the publish path never reads
//! one, and a paused owner that lost its claim, woke, and finished anyway still
//! converges at the content-addressed part keys and the record PUT.
//!
//! The primitive itself -- the key space, the payload, acquire/renew/steal/
//! complete and their outcome enums -- lives in [`ravel_fleet::claim`]. This
//! module is the compaction-specific half: when a claim is worth taking, where
//! the run is allowed to stop, and how a lost claim turns into
//! [`crate::publish::PublishOutcome::Abandoned`].
//!
//! # The five checkpoints
//!
//! [`Checkpoint`] names the quiescent points the merge pipeline already has.
//! At each one the guard renews if a third of the lease has elapsed since its
//! last successful write, and reports [`Verdict::Cancel`] if that renewal
//! failed or the claim was observed stolen. A cancelled run publishes nothing:
//! the parts it already PUT stay where they are, which is safe for exactly the
//! reason [`crate::publish::PublishOutcome::Abandoned`] documents (parts are
//! content-addressed and deterministic over the frozen input set, so a later
//! run republishes the identical keys, and sweep rule 3 collects any leftover).
//!
//! # Time and requests
//!
//! Every decision reads the injected [`crate::clock::Clock`] the caller
//! installed on [`ClaimParticipant`]; nothing here reads `SystemTime::now()`.
//! The store requests the protocol issues are counted under
//! [`RequestPhase::Coordinate`], never pooled into the merge's own phases, and
//! as requests only: the payloads are built inside [`ravel_fleet::claim`] and
//! do not cross this seam. One interleaving undercounts by a single metadata
//! request, and says so where it is counted ([`ClaimGuard::acquire`]'s
//! `Vanished` arm).
//!
//! # A lost claim is not a store error
//!
//! A CAS that comes back `PreconditionFailed` means the claim was stolen, and
//! `NotFound` means the claim object is gone (a `MemoryStore` maps a CAS
//! against a missing key to `PreconditionFailed`, while the S3 adapter answers
//! `NotFound`, so both spellings reach this code from a live deployment).
//! Both are the same protocol fact -- this attempt is no longer the owner --
//! and both cancel the run at the next checkpoint rather than escalating.

use std::sync::Arc;

use ravel_fleet::claim::{
    self, Acquisition, ClaimConfig, ClaimObservation, ClaimOwner, Completion, Renewal, Steal,
    WorkId, WorkIdentity, jitter_ms,
};
use ravel_object_store::{ObjectStoreBackend, StoreError, Version};
use ravel_proto::sys::v1::CompactionClaim;
use uuid::Uuid;

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::config::{ClaimParticipant, CompactorConfig, Coordination};
use crate::error::{MaintainError, Result};
use crate::request_ledger::{RequestLedger, RequestPhase};

/// One of the five quiescent points a claimed run may be cancelled at
/// (ADR-1029 decision 3). Ordered as the pipeline reaches them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checkpoint {
    /// After the seal / tombstone / already-compacted / min-input gates and
    /// before any input read: where the claim is acquired.
    Gates,
    /// After the input listing and `input_set_hash`, before the per-input
    /// catalog fan-out.
    InputSet,
    /// The per-signal merge loop head: one stream (RLOG), one trace (RSPAN),
    /// one fetch window (RSEG).
    MergeLoop,
    /// Each part boundary, after the part PUT returns.
    PartBoundary,
    /// Immediately before the record publish.
    Publish,
}

impl Checkpoint {
    /// Stable snake_case name, for logs and error text.
    pub fn name(self) -> &'static str {
        match self {
            Checkpoint::Gates => "gates",
            Checkpoint::InputSet => "input_set",
            Checkpoint::MergeLoop => "merge_loop",
            Checkpoint::PartBoundary => "part_boundary",
            Checkpoint::Publish => "publish",
        }
    }
}

/// What a checkpoint decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The claim is held (or none was taken); the run continues.
    Continue,
    /// The claim is gone; the run stops here and publishes nothing.
    Cancel,
}

/// Why a bucket was not claimed by this run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimSkipReason {
    /// Another attempt holds a live claim.
    HeldByAnother,
    /// The claim had expired, but another thief's CAS landed first (or the
    /// owner was not dead and renewed, which moves the version and defeats the
    /// steal).
    StealLost,
    /// The claim's payload does not decode, or declares a format floor this
    /// build does not understand. Such a claim is never stolen: a reader that
    /// cannot read a claim cannot know it is safe to take.
    UnreadableClaim,
    /// The claim existed at the `CreateIfAbsent` and was gone by the read that
    /// followed, twice in a row. One retry is free; a second vanishing means
    /// something outside this protocol is deleting claims, so the bucket is
    /// skipped rather than merged on an unproven assumption.
    VanishedTwice,
}

impl ClaimSkipReason {
    /// Stable snake_case name, for logs and reports.
    pub fn name(self) -> &'static str {
        match self {
            ClaimSkipReason::HeldByAnother => "held_by_another",
            ClaimSkipReason::StealLost => "steal_lost",
            ClaimSkipReason::UnreadableClaim => "unreadable_claim",
            ClaimSkipReason::VanishedTwice => "vanished_twice",
        }
    }
}

/// Everything a caller needs to report a claimed-away bucket and schedule its
/// retry, from one observation and with no polling.
#[derive(Debug, Clone)]
pub struct ClaimSkip {
    /// Why this run did not take the claim.
    pub reason: ClaimSkipReason,
    /// The claim's work id, in hex (the tail of its object key).
    pub work_id_hex: String,
    /// The holding process, when the claim payload was readable.
    pub holder_process_id: Option<Uuid>,
    /// `last_modified + lease`, from the store's own timestamp. `0` when there
    /// was no observation to date (a vanished claim).
    pub expiry_unix_ms: i64,
    /// The earliest this bucket should be retried: strictly after the holder's
    /// expiry, offset by this contender's deterministic jitter. Never poll
    /// before it.
    pub reschedule_after_unix_ms: i64,
}

/// What [`ClaimGuard::acquire`] did.
#[derive(Debug)]
pub enum Acquire {
    /// This run holds the claim (fresh, or stolen from an expired holder).
    Acquired,
    /// The claim is not available to this run; see [`ClaimSkip`].
    Skipped(ClaimSkip),
}

/// One bucket's claim, for the length of one compaction run.
///
/// Cheap to clone (one `Arc`): the driver installs a clone on its per-run
/// [`CompactorConfig`] so the merge's checkpoints can reach the same state.
/// Interior state is behind an async mutex, so a checkpoint reached from
/// inside a merge future is safe without the merge threading a `&mut`
/// anywhere.
#[derive(Clone)]
pub struct ClaimGuard {
    inner: Arc<Inner>,
}

struct Inner {
    identity: WorkIdentity,
    work_id: WorkId,
    owner: ClaimOwner,
    cfg: ClaimConfig,
    clock: Arc<dyn Clock>,
    ledger: Option<RequestLedger>,
    /// A third of the lease, in nanoseconds: the renewal cadence
    /// (ADR-1029 decision 3), evaluated at checkpoints and never on a timer.
    renew_after_ns: i64,
    state: tokio::sync::Mutex<State>,
}

#[derive(Default)]
struct State {
    /// `Some` while this run owns the claim.
    held: Option<Held>,
    /// The checkpoint that cancelled the run, once one has.
    cancelled_at: Option<Checkpoint>,
    /// Successful renewals this run made.
    renewals: u32,
    /// Store requests the claim protocol issued for this run. The ledger
    /// counts the same events; this is readable without one installed.
    requests: u32,
}

struct Held {
    key: String,
    version: Version,
    payload: CompactionClaim,
    /// The injected clock's reading at the last successful acquire, steal, or
    /// renewal: what the renewal cadence measures from.
    last_write_ns: i64,
}

impl std::fmt::Debug for ClaimGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaimGuard")
            .field("work_id", &self.inner.work_id.hex())
            .field("owner_process_id", &self.inner.owner.process_id)
            .finish_non_exhaustive()
    }
}

impl ClaimGuard {
    /// A guard for `bucket`, claiming as `participant`, with `cfg`'s lease and
    /// jitter. No store request is issued until [`Self::acquire`].
    ///
    /// `attempt_id` is fresh per guard, so two successive runs by one process
    /// are distinguishable in the payload, which `process_id` alone cannot
    /// express.
    pub fn new(
        bucket: &Bucket,
        participant: &ClaimParticipant,
        cfg: ClaimConfig,
        ledger: Option<RequestLedger>,
    ) -> Self {
        let identity = WorkIdentity::new(
            bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
            bucket.ingest_hour_bucket,
        );
        let clock = Arc::clone(participant.clock());
        let owner = ClaimOwner::new(participant.process_id(), Uuid::new_v4(), clock.now_ns());
        let lease_ns = i64::try_from(cfg.lease_duration.as_nanos()).unwrap_or(i64::MAX);
        ClaimGuard {
            inner: Arc::new(Inner {
                work_id: identity.work_id(),
                identity,
                owner,
                cfg,
                clock,
                ledger,
                renew_after_ns: (lease_ns / 3).max(1),
                state: tokio::sync::Mutex::new(State::default()),
            }),
        }
    }

    /// The claimed bucket's work id, in hex.
    pub fn work_id_hex(&self) -> String {
        self.inner.work_id.hex()
    }

    /// The checkpoint that cancelled this run, if one did.
    pub async fn cancelled_at(&self) -> Option<Checkpoint> {
        self.inner.state.lock().await.cancelled_at
    }

    /// Successful renewals this run has made.
    pub async fn renewals(&self) -> u32 {
        self.inner.state.lock().await.renewals
    }

    /// Store requests this run's claim protocol has issued so far. The same
    /// events land in [`RequestPhase::Coordinate`] when a ledger is installed.
    pub async fn requests(&self) -> u32 {
        self.inner.state.lock().await.requests
    }

    /// Whether this run currently holds the claim.
    pub async fn is_held(&self) -> bool {
        self.inner.state.lock().await.held.is_some()
    }

    /// Take the claim, after this contender's deterministic jitter delay.
    ///
    /// The jitter (a pure function of the work id and this process id, see
    /// [`jitter_ms`]) spreads simultaneous starts so N contenders do not all
    /// issue their `CreateIfAbsent` in the same instant. It precedes the
    /// attempt, never follows it.
    ///
    /// An expired claim is stolen in the same call: the steal CASes against the
    /// observed version, so exactly one of N thieves wins and a renewal by an
    /// owner that was merely slow defeats every thief.
    pub async fn acquire(&self, store: &dyn ObjectStoreBackend) -> Result<Acquire> {
        let inner = &self.inner;
        // The jitter span is a fraction of the lease, so it is priced from the
        // lease this guard was configured with (the same arithmetic
        // `ClaimConfig` uses internally: whole milliseconds, floored at 1).
        let lease_ms = i64::try_from(inner.cfg.lease_duration.as_millis())
            .unwrap_or(i64::MAX)
            .max(1);
        let jitter = jitter_ms(
            &inner.work_id,
            &inner.owner.process_id,
            lease_ms,
            &inner.cfg,
        );
        if jitter > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(jitter.unsigned_abs())).await;
        }

        // One free retry: a claim that existed at the `CreateIfAbsent` and was
        // gone by the read that followed is an ordinary interleaving, and the
        // retry takes the now-absent key.
        for attempt in 0..2 {
            let outcome = claim::acquire(store, &inner.identity, &inner.owner, &inner.cfg).await?;
            match outcome {
                Acquisition::Acquired {
                    key,
                    version,
                    payload,
                    ..
                } => {
                    // The uncontended path is exactly one PUT and no reads.
                    self.note_requests(1).await;
                    let now_ns = inner.clock.now_ns();
                    let mut state = inner.state.lock().await;
                    state.held = Some(Held {
                        key,
                        version,
                        payload,
                        last_write_ns: now_ns,
                    });
                    return Ok(Acquire::Acquired);
                }
                Acquisition::Held { observed } => {
                    // The contention path is the rejected PUT plus exactly one
                    // GET and one `head()`.
                    self.note_requests(3).await;
                    return self.contend(store, observed).await;
                }
                Acquisition::Vanished { .. } => {
                    // The rejected PUT plus the GET that found the key gone.
                    // The observation's `head()` is only reached when the GET
                    // succeeded, and which of the two 404ed is not visible at
                    // this seam, so a claim that vanishes between them counts
                    // one request short here.
                    self.note_requests(2).await;
                    if attempt == 1 {
                        let now_ms = self.now_unix_ms();
                        return Ok(Acquire::Skipped(ClaimSkip {
                            reason: ClaimSkipReason::VanishedTwice,
                            work_id_hex: inner.work_id.hex(),
                            holder_process_id: None,
                            expiry_unix_ms: 0,
                            reschedule_after_unix_ms: now_ms
                                .saturating_add(jitter)
                                .saturating_add(1),
                        }));
                    }
                }
            }
        }
        // Unreachable in practice: the loop returns on every outcome except a
        // first `Vanished`, which retries once and then returns above. Written
        // as a typed invariant rather than a panic, per the crate's no-panic
        // rule.
        Err(MaintainError::Invariant(
            "claim acquisition loop ended without an outcome".to_string(),
        ))
    }

    /// The contention path: steal an expired claim, or report the skip.
    async fn contend(
        &self,
        store: &dyn ObjectStoreBackend,
        observed: ClaimObservation,
    ) -> Result<Acquire> {
        let inner = &self.inner;
        let now_ms = self.now_unix_ms();
        let skip = |reason: ClaimSkipReason| {
            Acquire::Skipped(ClaimSkip {
                reason,
                work_id_hex: observed.work_id.hex(),
                holder_process_id: observed.holder_process_id(),
                expiry_unix_ms: observed.expiry_unix_ms,
                reschedule_after_unix_ms: observed.reschedule_after_unix_ms,
            })
        };
        if !observed.is_expired(now_ms) {
            return Ok(skip(ClaimSkipReason::HeldByAnother));
        }

        match claim::steal(store, &observed, &inner.owner, &inner.cfg, now_ms).await? {
            Steal::Acquired {
                key,
                version,
                payload,
            } => {
                self.note_requests(1).await;
                let now_ns = inner.clock.now_ns();
                let mut state = inner.state.lock().await;
                state.held = Some(Held {
                    key,
                    version,
                    payload,
                    last_write_ns: now_ns,
                });
                Ok(Acquire::Acquired)
            }
            Steal::Lost => {
                self.note_requests(1).await;
                Ok(skip(ClaimSkipReason::StealLost))
            }
            // Refused locally: no store request was issued at all.
            Steal::Refused(claim::StealRefused::UnreadableClaim) => {
                Ok(skip(ClaimSkipReason::UnreadableClaim))
            }
            Steal::Refused(claim::StealRefused::NotExpired { .. }) => {
                Ok(skip(ClaimSkipReason::HeldByAnother))
            }
        }
    }

    /// Consult the claim at `at`: renew when a third of the lease has elapsed
    /// since the last successful write, and report [`Verdict::Cancel`] once the
    /// claim is gone.
    ///
    /// A run that never took a claim (coordination off, or a bucket below the
    /// cost gate) never reaches here: the driver installs no guard, so the
    /// checkpoint is a single `Option` check. A run whose claim was already
    /// lost keeps reporting `Cancel`, so a second checkpoint after the first
    /// costs no request.
    pub async fn checkpoint(
        &self,
        store: &dyn ObjectStoreBackend,
        at: Checkpoint,
    ) -> Result<Verdict> {
        let inner = &self.inner;
        let now_ns = inner.clock.now_ns();
        let mut state = inner.state.lock().await;
        if state.cancelled_at.is_some() {
            return Ok(Verdict::Cancel);
        }
        let Some(held) = state.held.as_ref() else {
            // No claim was taken for this run.
            return Ok(Verdict::Continue);
        };
        if now_ns.saturating_sub(held.last_write_ns) < inner.renew_after_ns {
            return Ok(Verdict::Continue);
        }
        // The CAS inputs are copied out so the borrow ends before the await;
        // the lock is held across it either way, so no other checkpoint can
        // interleave and the copies cannot go stale.
        let (key, version, payload) =
            (held.key.clone(), held.version.clone(), held.payload.clone());

        let renewal = claim::renew(store, &key, &version, &payload, now_ns).await;
        state.requests = state.requests.saturating_add(1);
        if let Some(ledger) = inner.ledger.as_ref() {
            ledger.record_metadata(RequestPhase::Coordinate);
        }
        match renewal {
            Ok(Renewal::Renewed { version, payload }) => {
                state.held = Some(Held {
                    key,
                    version,
                    payload,
                    last_write_ns: now_ns,
                });
                state.renewals = state.renewals.saturating_add(1);
                Ok(Verdict::Continue)
            }
            // The claim was stolen, or the claim object is gone. Both mean this
            // attempt is no longer the owner; neither is escalated.
            Ok(Renewal::ClaimLost) | Err(StoreError::NotFound) => {
                tracing::warn!(
                    work_id = %inner.work_id.hex(),
                    checkpoint = at.name(),
                    "compaction claim lost; cancelling the run without publishing (ADR-1029)"
                );
                state.held = None;
                state.cancelled_at = Some(at);
                Ok(Verdict::Cancel)
            }
            Err(err) => Err(MaintainError::Store(err)),
        }
    }

    /// Mark the claim completed after a successful run (ADR-1029 decision 1
    /// step 6). A courtesy marker for operators and for a contender that would
    /// otherwise wait out the lease: the published compaction record is the
    /// only completion marker that means anything, so a `NotOwner` outcome
    /// (someone stole the claim) and a vanished claim are both fine.
    ///
    /// Never a DELETE. An unconditional delete here is exactly the write that
    /// would destroy a newer owner's claim (ADR-1029 rejected alternative 4).
    pub async fn complete(&self, store: &dyn ObjectStoreBackend) -> Result<()> {
        let inner = &self.inner;
        let now_ns = inner.clock.now_ns();
        let mut state = inner.state.lock().await;
        let Some(held) = state.held.as_ref() else {
            return Ok(());
        };
        let (key, version, payload) =
            (held.key.clone(), held.version.clone(), held.payload.clone());
        let marked = claim::mark_completed(store, &key, &version, &payload, now_ns).await;
        state.requests = state.requests.saturating_add(1);
        if let Some(ledger) = inner.ledger.as_ref() {
            ledger.record_metadata(RequestPhase::Coordinate);
        }
        match marked {
            Ok(Completion::Marked { version, payload }) => {
                state.held = Some(Held {
                    key,
                    version,
                    payload,
                    last_write_ns: now_ns,
                });
                Ok(())
            }
            Ok(Completion::NotOwner) | Err(StoreError::NotFound) => {
                state.held = None;
                Ok(())
            }
            Err(err) => Err(MaintainError::Store(err)),
        }
    }

    /// The injected clock as unix milliseconds, the base the store's
    /// `last_modified` and every expiry are compared in.
    fn now_unix_ms(&self) -> i64 {
        self.inner.clock.now_ns() / 1_000_000
    }

    /// Account `n` store requests the claim protocol just issued, in this
    /// guard's own counter and in the run's ledger when one is installed.
    ///
    /// Requests only: the claim payloads are built inside
    /// [`ravel_fleet::claim`] and never cross this seam, so the coordinate
    /// phase's two byte figures stay zero, exactly as they do for a phase that
    /// only issues LIST or HEAD.
    async fn note_requests(&self, n: u32) {
        let mut state = self.inner.state.lock().await;
        state.requests = state.requests.saturating_add(n);
        if let Some(ledger) = self.inner.ledger.as_ref() {
            for _ in 0..n {
                ledger.record_metadata(RequestPhase::Coordinate);
            }
        }
    }
}

/// Consult the run's claim, if one is installed, and turn a cancel verdict into
/// the typed signal the pipeline unwinds on.
///
/// This is what every seam inside the merge calls. With no guard installed
/// (every unclaimed run: coordination off, a bucket below the cost gate, or a
/// caller that installed no [`ClaimParticipant`]) it is one `Option` check and
/// no await of anything.
///
/// The error is internal plumbing, not an operator-facing failure:
/// [`crate::rewrite::rewrite_and_publish`] catches it and returns
/// [`crate::publish::PublishOutcome::Abandoned`], so a cancelled run is an
/// ordinary outcome that published nothing.
pub(crate) async fn checkpoint(
    config: &CompactorConfig,
    store: &dyn ObjectStoreBackend,
    at: Checkpoint,
) -> Result<()> {
    if let Some(guard) = config.claim_guard.as_ref()
        && guard.checkpoint(store, at).await? == Verdict::Cancel
    {
        return Err(MaintainError::ClaimLost { at: at.name() });
    }
    Ok(())
}

/// Whether `config` would claim a bucket whose listed input bytes are
/// `input_bytes`: a participant is installed, coordination is on, and the
/// bucket is at or above the cost gate (ADR-1029 decision 4).
pub(crate) fn claims_bucket(config: &CompactorConfig, input_bytes: u64) -> bool {
    config.claim_participant.is_some()
        && config.coordination == Coordination::On
        && input_bytes >= config.claim_min_input_bytes
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, PutOptions};
    use ravel_types::{Signal, TenantId};

    use super::*;
    use crate::clock::FixedClock;

    const HOUR: u32 = 495_000;

    fn bucket() -> Bucket {
        Bucket::new(TenantId::new("acme").hash(), Signal::Logs, 3, HOUR)
    }

    /// A participant on a fixed clock, so every renewal and expiry decision in
    /// these tests is driven by the test rather than by wall time.
    fn participant(process: u128, clock: &FixedClock) -> ClaimParticipant {
        ClaimParticipant::new(
            Uuid::from_u128(process),
            Arc::new(clock.clone()) as Arc<dyn Clock>,
        )
    }

    /// A short lease keeps the deterministic acquisition jitter (10% of the
    /// lease) well under a second, so these tests spend milliseconds rather
    /// than the 30 s a 300 s production lease would draw from.
    fn cfg() -> ClaimConfig {
        ClaimConfig {
            lease_duration: Duration::from_secs(3),
            ..ClaimConfig::default()
        }
    }

    fn guard(clock: &FixedClock, process: u128, ledger: Option<RequestLedger>) -> ClaimGuard {
        ClaimGuard::new(&bucket(), &participant(process, clock), cfg(), ledger)
    }

    /// The uncontended acquisition, the renewal cadence, and completion, with
    /// exact coordinate-phase figures at each step.
    ///
    /// Renewal cadence (ADR-1029 decision 3): a checkpoint BEFORE a third of
    /// the lease has elapsed issues no renewal, and one after it issues exactly
    /// one. Demonstrated failing against a guard that renews at every
    /// checkpoint (the first assertion below reads 1 renewal instead of 0) and
    /// against one that never renews (the second reads 0 instead of 1).
    #[tokio::test]
    async fn renewal_waits_for_a_third_of_the_lease() {
        let store = MemoryStore::new();
        let ledger = RequestLedger::new();
        ledger.reset_for_run();
        let clock = FixedClock::new(0);
        let guard = guard(&clock, 1, Some(ledger.clone()));

        assert!(matches!(
            guard.acquire(&store).await.expect("acquire"),
            Acquire::Acquired
        ));
        assert_eq!(
            ledger.report().coordinate.requests,
            1,
            "the uncontended acquisition is exactly one PUT and no reads"
        );

        // One third of a 3 s lease is 1 s. Just under it: no renewal.
        clock.set(999_000_000);
        assert_eq!(
            guard
                .checkpoint(&store, Checkpoint::MergeLoop)
                .await
                .expect("checkpoint"),
            Verdict::Continue
        );
        assert_eq!(guard.renewals().await, 0, "no renewal before the cadence");
        assert_eq!(
            ledger.report().coordinate.requests,
            1,
            "and no request for it"
        );

        // At the cadence: exactly one renewal.
        clock.set(1_000_000_000);
        assert_eq!(
            guard
                .checkpoint(&store, Checkpoint::MergeLoop)
                .await
                .expect("checkpoint"),
            Verdict::Continue
        );
        assert_eq!(guard.renewals().await, 1, "exactly one renewal");
        assert_eq!(ledger.report().coordinate.requests, 2);

        // The cadence restarts from the renewal, so the next checkpoint at the
        // same instant renews nothing.
        assert_eq!(
            guard
                .checkpoint(&store, Checkpoint::PartBoundary)
                .await
                .expect("checkpoint"),
            Verdict::Continue
        );
        assert_eq!(guard.renewals().await, 1);
        assert_eq!(ledger.report().coordinate.requests, 2);

        guard.complete(&store).await.expect("complete");
        assert_eq!(
            ledger.report().coordinate.requests,
            3,
            "acquire, one renewal, one completion"
        );
        assert_eq!(
            ledger.report().coordinate.wire_bytes_sent,
            0,
            "claim payloads are built inside the primitive and never cross this seam"
        );
    }

    /// A second contender does not take a live claim: it observes the holder
    /// and gets a reschedule point strictly past the holder's expiry, so it
    /// walks away instead of polling.
    #[tokio::test]
    async fn a_live_claim_is_observed_and_rescheduled_around() {
        let store = MemoryStore::new();
        store.set_clock_ms(1_700_000_000_000);
        let clock = FixedClock::new(1_700_000_000_000 * 1_000_000);
        let a = guard(&clock, 1, None);
        let b = guard(&clock, 2, None);

        assert!(matches!(
            a.acquire(&store).await.expect("a acquires"),
            Acquire::Acquired
        ));
        let skip = match b.acquire(&store).await.expect("b observes") {
            Acquire::Skipped(skip) => skip,
            other => panic!("expected a skip, got {other:?}"),
        };
        assert_eq!(skip.reason, ClaimSkipReason::HeldByAnother);
        assert_eq!(skip.holder_process_id, Some(Uuid::from_u128(1)));
        assert_eq!(
            skip.expiry_unix_ms,
            1_700_000_000_000 + 3_000,
            "expiry is the store's last_modified plus the lease"
        );
        assert!(
            skip.reschedule_after_unix_ms > skip.expiry_unix_ms,
            "the retry lands strictly after expiry: {} vs {}",
            skip.reschedule_after_unix_ms,
            skip.expiry_unix_ms
        );
        assert!(!b.is_held().await, "the contender holds nothing");
        assert_eq!(b.requests().await, 3, "one rejected PUT, one GET, one HEAD");
    }

    /// An expired claim is stolen, and the dispossessed owner learns it lost at
    /// its next checkpoint: the checkpoint names itself as the cancel site and
    /// the run publishes nothing after it.
    #[tokio::test]
    async fn an_expired_claim_is_stolen_and_the_owner_cancels() {
        let store = MemoryStore::new();
        store.set_clock_ms(1_700_000_000_000);
        let clock = FixedClock::new(1_700_000_000_000 * 1_000_000);
        let a = guard(&clock, 1, None);
        assert!(matches!(
            a.acquire(&store).await.expect("a acquires"),
            Acquire::Acquired
        ));

        // Past the lease: the store's own timestamp is the expiry base, so the
        // thief's clock moves with it.
        store.set_clock_ms(1_700_000_004_000);
        clock.set(1_700_000_004_000 * 1_000_000);
        let b = guard(&clock, 2, None);
        assert!(
            matches!(
                b.acquire(&store).await.expect("b steals"),
                Acquire::Acquired
            ),
            "an expired claim is stealable"
        );

        assert_eq!(
            a.checkpoint(&store, Checkpoint::PartBoundary)
                .await
                .expect("checkpoint"),
            Verdict::Cancel,
            "the dispossessed owner cancels at its next checkpoint"
        );
        assert_eq!(a.cancelled_at().await, Some(Checkpoint::PartBoundary));
        assert!(!a.is_held().await);
        // A later checkpoint keeps cancelling, and costs no further request.
        let after = a.requests().await;
        assert_eq!(
            a.checkpoint(&store, Checkpoint::Publish)
                .await
                .expect("checkpoint"),
            Verdict::Cancel
        );
        assert_eq!(a.requests().await, after);
        assert_eq!(
            a.cancelled_at().await,
            Some(Checkpoint::PartBoundary),
            "the cancel site is the first checkpoint that saw the loss"
        );
    }

    /// `NotFound` from a claim CAS is treated exactly like a lost claim, never
    /// escalated as an error: a `MemoryStore` answers `PreconditionFailed` for
    /// a CAS against a missing key while the S3 adapter answers `NotFound`, so
    /// both spellings reach this code from a live deployment. Driven by
    /// deleting the claim object out from under a live owner on a store whose
    /// `delete` really removes it, then renewing.
    #[tokio::test]
    async fn not_found_on_renew_cancels_rather_than_erroring() {
        let store = NotFoundOnCas(MemoryStore::new());
        let clock = FixedClock::new(0);
        let guard = guard(&clock, 1, None);
        assert!(matches!(
            guard.acquire(&store).await.expect("acquire"),
            Acquire::Acquired
        ));

        clock.set(2_000_000_000);
        let verdict = guard
            .checkpoint(&store, Checkpoint::InputSet)
            .await
            .expect("a NotFound CAS must not surface as an error");
        assert_eq!(verdict, Verdict::Cancel);
        assert_eq!(guard.cancelled_at().await, Some(Checkpoint::InputSet));
    }

    /// A store that answers every conditional-version PUT with `NotFound`, the
    /// S3 adapter's spelling for a CAS against a key that is gone. Only the
    /// `CasVersion` mode is affected, so the acquisition still lands.
    struct NotFoundOnCas(MemoryStore);

    #[async_trait::async_trait]
    impl ObjectStoreBackend for NotFoundOnCas {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> std::result::Result<ravel_object_store::PutOutcome, StoreError> {
            if matches!(opts.mode, ravel_object_store::PutMode::CasVersion(_)) {
                return Err(StoreError::NotFound);
            }
            self.0.put(key, data, opts).await
        }
        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> std::result::Result<ravel_object_store::GetOutcome, StoreError> {
            self.0.get(key, range).await
        }
        async fn head(
            &self,
            key: &str,
        ) -> std::result::Result<ravel_object_store::ObjectMeta, StoreError> {
            self.0.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> std::result::Result<ravel_object_store::ListPage, StoreError> {
            self.0.list(prefix, page).await
        }
        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> std::result::Result<ravel_object_store::DelimitedList, StoreError> {
            self.0.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
            self.0.delete(key).await
        }
        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.0.capabilities()
        }
    }

    /// The claim key is the bucket's identity and nothing else: two runs over
    /// one bucket collide on one claim however their input listings differ,
    /// which is what makes excluding `input_set_hash` from the work id
    /// load-bearing (ADR-1029 rejected alternative 3).
    #[tokio::test]
    async fn two_runs_over_one_bucket_share_one_claim_key() {
        let store = MemoryStore::new();
        let clock = FixedClock::new(0);
        let a = guard(&clock, 1, None);
        let b = guard(&clock, 2, None);
        assert_eq!(a.work_id_hex(), b.work_id_hex());
        a.acquire(&store).await.expect("a acquires");
        let keys = ravel_object_store::list_all(&store, claim::COMPACTION_CLAIMS_PREFIX)
            .await
            .expect("list claims");
        assert_eq!(keys.len(), 1, "exactly one claim object exists");
        assert_eq!(
            keys[0].key,
            format!("{}{}", claim::COMPACTION_CLAIMS_PREFIX, a.work_id_hex())
        );
        assert!(matches!(
            b.acquire(&store).await.expect("b observes"),
            Acquire::Skipped(_)
        ));
    }
}
