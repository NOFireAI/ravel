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
//! # The checkpoints
//!
//! The claim is acquired at the first of ADR-1029 decision 3's five quiescent
//! points, and [`Checkpoint`] names the other four. At each of those the guard
//! renews if a third of the lease has elapsed since its last successful write,
//! and reports [`Verdict::Cancel`] if that renewal was
//! rejected because the claim was stolen or is gone. Any other store error on
//! the renewal is not a cancel: the checkpoint returns it, and the run fails
//! with it before its record PUT. A cancelled run publishes nothing:
//! the parts it already PUT stay where they are, which is safe for exactly the
//! reason [`crate::publish::PublishOutcome::Abandoned`] documents (parts are
//! content-addressed and deterministic over the frozen input set, so a later
//! run republishes the identical keys, and sweep rule 3 collects any leftover).
//!
//! # Where the claim is taken
//!
//! The claim is acquired after the bucket listing and after the input commit
//! records are read, because the cost gate is priced on the `object_size`
//! those records carry. It precedes every catalog and block read and every
//! PUT. Outside its claim requests, a contender refused the claim has paid the
//! bucket listing and one GET per input commit record, and no catalog, block
//! or part request (ADR-1029, the amendment on where the claim is taken).
//!
//! # Jitter is paid on the contended path only
//!
//! The first `CreateIfAbsent` is issued with no wait: it resolves the race on
//! its own, and an uncontended bucket has nobody to decorrelate from. The
//! deterministic jitter is waited out, through the participant's
//! [`ClaimSleeper`], before the two writes that follow a refused create: the
//! steal of an expired claim, and the second `CreateIfAbsent` after a claim
//! vanished between the refusal and its read. Each of those waits once.
//! [`ClaimSkip::reschedule_after_unix_ms`] for a held claim is one millisecond
//! past the holder's expiry and carries no jitter, so the retry scheduled from
//! it pays the jitter once, before its steal (ADR-1029, the amendment on
//! jitter on the contended path).
//!
//! # An unreadable claim does not starve its bucket
//!
//! A claim whose payload does not decode (corruption, or a newer format
//! written before a rollback) is never stolen: a reader that cannot read a
//! claim cannot know it is safe to overwrite. Claims are advisory
//! (ADR-1029 decision 2), so once the unreadable object's age by the store's
//! `last_modified` exceeds one lease plus this contender's jitter, the run
//! proceeds UNCLAIMED ([`Acquire::Unclaimed`]) with a warning naming the key.
//! Before that age the bucket is deferred like any held claim.
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
//! and both cancel the run at the next checkpoint rather than escalating. A
//! steal answered either way is the same lost race
//! ([`ClaimSkipReason::StealLost`]), and a completion answered either way is a
//! no-op.
//!
//! # A process reclaims its own leftover claim at once
//!
//! A renewal that fails with a genuine store error aborts the run
//! ([`crate::error::MaintainError::ClaimRenewFailed`]) and leaves the claim in
//! place: it was never confirmed lost, so it is never released. The next time
//! this same process contends for that bucket, [`ClaimGuard::contend`]
//! recognizes the observed holder as itself (by process id) and reclaims the
//! claim at once via [`ravel_fleet::claim::reclaim`], without waiting for the
//! lease to expire and without the jitter wait a contended steal pays. A
//! reclaim is reported as an acquisition, not a steal; a different process
//! still waits out the lease as before (ADR-1029, 2026-09-30 amendment; issue
//! #2156).

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use ravel_fleet::claim::{
    self, Acquisition, ClaimConfig, ClaimObservation, ClaimOwner, Completion, Reclaim, Renewal,
    Steal, WorkId, WorkIdentity, jitter_ms,
};
use ravel_object_store::{ObjectStoreBackend, StoreError, Version};
use ravel_proto::sys::v1::CompactionClaim;
use uuid::Uuid;

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::config::{ClaimParticipant, CompactorConfig, Coordination};
use crate::error::{MaintainError, Result};
use crate::request_ledger::{RequestLedger, RequestPhase};

/// One of the quiescent points a claimed run may be cancelled at (ADR-1029
/// decision 3). Ordered as the pipeline reaches them. Decision 3's first
/// point, after the gates, is where the claim is acquired, so it has no
/// variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checkpoint {
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
    /// steal), or the claim object was gone by the steal's CAS.
    StealLost,
    /// The claim's payload does not decode, or declares a format floor this
    /// build does not understand, and it is not yet older than one lease plus
    /// this contender's jitter. Such a claim is never stolen: a reader that
    /// cannot read a claim cannot know it is safe to take. Past that age the
    /// run goes ahead unclaimed instead ([`Acquire::Unclaimed`]).
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
    /// The earliest this bucket should be retried: one millisecond past the
    /// holder's expiry (past the observation instant for a vanished claim).
    /// It carries no jitter, because the retry waits out this contender's
    /// jitter itself before it steals the expired claim. For an unreadable
    /// claim it is the instant the claim stops holding the bucket back, one
    /// millisecond past one lease plus this contender's jitter, since that
    /// retry steals nothing and so waits nothing. Never poll before it.
    pub reschedule_after_unix_ms: i64,
}

/// What [`ClaimGuard::acquire`] did.
#[derive(Debug)]
pub enum Acquire {
    /// This run holds the claim: fresh, or stolen from an expired holder.
    /// Which is [`ClaimGuard::stolen`] (ADR-1029 decision 3's contended
    /// path), read after this returns.
    Acquired,
    /// The claim is not available to this run; see [`ClaimSkip`].
    Skipped(ClaimSkip),
    /// The claim object is unreadable and older than one lease plus this
    /// contender's jitter by the store's `last_modified`. It is left in place
    /// (never stolen, never deleted), and this run proceeds without a claim so
    /// the bucket is not deferred forever. Advisory either way: the record's
    /// `CreateIfAbsent` still serializes racing publishes.
    Unclaimed {
        /// The unreadable claim's object key.
        key: String,
    },
}

/// How the contended-path jitter is waited out.
///
/// The jitter is a duration, not an instant, so the injected
/// [`crate::clock::Clock`] cannot shorten it; this hook is what lets a test
/// record the requested wait without paying it. Production uses
/// [`TokioSleeper`].
pub trait ClaimSleeper: Send + Sync {
    /// Wait for `duration`.
    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()>;
}

/// The production [`ClaimSleeper`]: tokio's timer.
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioSleeper;

impl ClaimSleeper for TokioSleeper {
    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
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
    sleeper: Arc<dyn ClaimSleeper>,
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
    /// Whether [`ClaimGuard::acquire`] took this claim over from an expired
    /// holder (the contended path) rather than creating it fresh.
    stolen: bool,
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
                sleeper: Arc::clone(participant.sleeper()),
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

    /// Whether [`Self::acquire`] took this claim over from an expired holder
    /// rather than creating it fresh (ADR-1029 decision 3). `false` before
    /// `acquire` has returned `Ok(Acquire::Acquired)`.
    pub async fn stolen(&self) -> bool {
        self.inner.state.lock().await.stolen
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

    /// Take the claim.
    ///
    /// The first `CreateIfAbsent` goes out with no wait. The deterministic
    /// jitter (a pure function of the work id and this process id, see
    /// [`jitter_ms`]) is waited out through the participant's [`ClaimSleeper`]
    /// only on the contended path: once before the retried `CreateIfAbsent`
    /// that follows a vanished claim, and once before a steal. The reschedule
    /// point a held claim's skip reports carries none.
    ///
    /// An expired claim is stolen in the same call: the steal CASes against the
    /// observed version, so exactly one of N thieves wins and a renewal by an
    /// owner that was merely slow defeats every thief.
    pub async fn acquire(&self, store: &dyn ObjectStoreBackend) -> Result<Acquire> {
        let inner = &self.inner;
        // One free retry: a claim that existed at the `CreateIfAbsent` and was
        // gone by the read that followed is an ordinary interleaving, and the
        // retry takes the now-absent key.
        for attempt in 0..2 {
            if attempt > 0 {
                self.wait_jitter().await;
            }
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
                            reschedule_after_unix_ms: now_ms.saturating_add(1),
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

    /// Wait out this contender's deterministic jitter through the
    /// participant's sleeper. Called only on the contended path.
    async fn wait_jitter(&self) {
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
            inner
                .sleeper
                .sleep(Duration::from_millis(jitter.unsigned_abs()))
                .await;
        }
    }

    /// The contention path: steal an expired claim, run past a stale
    /// unreadable one, or report the skip.
    async fn contend(
        &self,
        store: &dyn ObjectStoreBackend,
        observed: ClaimObservation,
    ) -> Result<Acquire> {
        let inner = &self.inner;
        let now_ms = self.now_unix_ms();
        let skip = |reason: ClaimSkipReason| {
            // A held claim's retry steals, and waits out the jitter before it,
            // so its reschedule point is the bare expiry. An unreadable claim's
            // retry steals nothing, so it is rescheduled to the observation's
            // own point, which adds the jitter: the instant `past_unreadable`
            // lets the run proceed. A lost steal means another contender just
            // wrote a claim with a fresh lease, and the observed expiry is
            // already past, so its retry waits one full lease from now.
            let reschedule_after_unix_ms = match reason {
                ClaimSkipReason::UnreadableClaim => observed.reschedule_after_unix_ms,
                ClaimSkipReason::StealLost => {
                    let lease_ms =
                        i64::try_from(inner.cfg.lease_duration.as_millis()).unwrap_or(i64::MAX);
                    now_ms.saturating_add(lease_ms)
                }
                _ => observed.expiry_unix_ms.saturating_add(1),
            };
            Acquire::Skipped(ClaimSkip {
                reason,
                work_id_hex: observed.work_id.hex(),
                holder_process_id: observed.holder_process_id(),
                expiry_unix_ms: observed.expiry_unix_ms,
                reschedule_after_unix_ms,
            })
        };
        if observed.holder.is_none() {
            return Ok(self
                .past_unreadable(&observed, now_ms)
                .unwrap_or_else(|| skip(ClaimSkipReason::UnreadableClaim)));
        }
        if observed.holder_process_id() == Some(inner.owner.process_id) {
            // This process's own claim, left in place by a renewal that
            // failed with a genuine store error rather than being lost to
            // another owner. No contention with another process, so no
            // jitter wait, and no requirement that it be expired.
            return match claim::reclaim(store, &observed, &inner.owner, &inner.cfg).await? {
                Reclaim::Acquired {
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
                    // `stolen` stays false: a reclaim of this process's own
                    // claim is reported as an acquisition, not a steal.
                    Ok(Acquire::Acquired)
                }
                Reclaim::Lost => {
                    self.note_requests(1).await;
                    Ok(skip(ClaimSkipReason::StealLost))
                }
                Reclaim::Refused => Err(MaintainError::Invariant(
                    "reclaim refused despite a matching holder process id".to_string(),
                )),
            };
        }
        if !observed.is_expired(now_ms) {
            return Ok(skip(ClaimSkipReason::HeldByAnother));
        }

        // Contenders rescheduled to the same expiry arrive here together; the
        // jitter spreads their steals.
        self.wait_jitter().await;
        let now_ms = self.now_unix_ms();
        let stolen = match claim::steal(store, &observed, &inner.owner, &inner.cfg, now_ms).await {
            // The claim object was deleted between the observation and the
            // steal's CAS: the S3 adapter's spelling of the lost race a
            // `MemoryStore` answers with `PreconditionFailed`.
            Err(StoreError::NotFound) => Steal::Lost,
            other => other?,
        };
        match stolen {
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
                state.stolen = true;
                Ok(Acquire::Acquired)
            }
            Steal::Lost => {
                self.note_requests(1).await;
                Ok(skip(ClaimSkipReason::StealLost))
            }
            // Refused locally: no store request was issued at all. The
            // unreadable case is answered above before any steal; this arm
            // only keeps the primitive's refusal from being misread.
            Steal::Refused(claim::StealRefused::UnreadableClaim) => Ok(self
                .past_unreadable(&observed, now_ms)
                .unwrap_or_else(|| skip(ClaimSkipReason::UnreadableClaim))),
            Steal::Refused(claim::StealRefused::NotExpired { .. }) => {
                Ok(skip(ClaimSkipReason::HeldByAnother))
            }
        }
    }

    /// [`Acquire::Unclaimed`] once an unreadable claim's age by the store's
    /// `last_modified` exceeds one lease plus this contender's jitter, and
    /// `None` before that.
    ///
    /// The threshold is the observation's own reschedule point
    /// (`last_modified + lease + jitter + 1`), which is also the reschedule
    /// point an unreadable claim's skip reports, so the retry scheduled from
    /// that skip is the attempt that runs.
    fn past_unreadable(&self, observed: &ClaimObservation, now_ms: i64) -> Option<Acquire> {
        if now_ms < observed.reschedule_after_unix_ms {
            return None;
        }
        tracing::warn!(
            key = %observed.key,
            work_id = %observed.work_id.hex(),
            last_modified_unix_ms = observed.last_modified_unix_ms,
            "compaction claim is unreadable and older than one lease plus jitter; \
             running the bucket unclaimed and leaving the claim in place (ADR-1029)"
        );
        Some(Acquire::Unclaimed {
            key: observed.key.clone(),
        })
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
            // A genuine store error, distinct from a lost claim above: the
            // claim may still be held, this renewal just could not confirm it
            // (ADR-1029 decision 3).
            Err(err) => Err(MaintainError::ClaimRenewFailed {
                at: at.name(),
                source: err,
            }),
        }
    }

    /// Mark the claim completed after a successful run (ADR-1029 decision 1
    /// step 6). A forensic marker for operators, and nothing more: neither
    /// expiry nor steal reads the completed state, so a contender that
    /// observes a completed claim still defers until its expiry. What spares
    /// later runs is the published compaction record, which sends them out at
    /// the already-compacted gate before they reach the claim. A `NotOwner`
    /// outcome (someone stole the claim) and a vanished claim are both fine.
    ///
    /// Any other store error is returned, and the driver logs it rather than
    /// failing a run whose pipeline has already finished.
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

    use ravel_fleet::claim::COMPACTION_CLAIMS_PREFIX;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
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
    ///
    /// Its sleeper records the jitter waits it is asked for and returns at
    /// once, so no test here waits on the real timer.
    fn participant(process: u128, clock: &FixedClock) -> ClaimParticipant {
        recording_participant(process, clock).0
    }

    /// [`participant`], also returning the waits its sleeper was asked for.
    fn recording_participant(
        process: u128,
        clock: &FixedClock,
    ) -> (ClaimParticipant, Arc<RecordingSleeper>) {
        let sleeper = Arc::new(RecordingSleeper::default());
        let participant = ClaimParticipant::new(
            Uuid::from_u128(process),
            Arc::new(clock.clone()) as Arc<dyn Clock>,
        )
        .with_sleeper(Arc::clone(&sleeper) as Arc<dyn ClaimSleeper>);
        (participant, sleeper)
    }

    /// A [`ClaimSleeper`] that records every requested wait and returns at
    /// once.
    #[derive(Default)]
    struct RecordingSleeper {
        waits: std::sync::Mutex<Vec<Duration>>,
    }

    impl RecordingSleeper {
        fn waits(&self) -> Vec<Duration> {
            self.waits.lock().expect("sleeper lock").clone()
        }
    }

    impl ClaimSleeper for RecordingSleeper {
        fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
            self.waits.lock().expect("sleeper lock").push(duration);
            Box::pin(std::future::ready(()))
        }
    }

    /// A 3 s lease: a 1 s renewal cadence the tests move the clock across.
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
    /// both spellings reach this code from a live deployment. Driven by a
    /// store that answers `NotFound` to every `CasVersion` PUT, which is what
    /// the S3 adapter returns for a renewal of a claim deleted under its
    /// owner; the claim object itself is never deleted here.
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

    /// Contender 2's jitter draw for [`bucket`] under [`cfg`], asserted
    /// non-zero so a test built on it cannot pass vacuously.
    fn contender_jitter() -> Duration {
        let draw = jitter_ms(
            &bucket_identity().work_id(),
            &Uuid::from_u128(2),
            cfg().lease_duration.as_millis() as i64,
            &cfg(),
        );
        assert!(draw > 0, "a zero draw would make this test vacuous");
        Duration::from_millis(draw as u64)
    }

    /// An uncontended acquisition asks the sleeper for no wait at all: its one
    /// `CreateIfAbsent` settles the claim, and there is no contender to
    /// decorrelate from.
    ///
    /// Shown failing against the guard that waited out the jitter before every
    /// first `CreateIfAbsent`: "an uncontended claim waits for nothing" reads
    /// left [163ms], right [].
    #[tokio::test]
    async fn an_uncontended_claim_requests_no_jitter_wait() {
        let store = MemoryStore::new();
        let clock = FixedClock::new(1_700_000_000_000 * 1_000_000);
        let expected = contender_jitter();
        let (participant, sleeper) = recording_participant(2, &clock);
        let guard = ClaimGuard::new(&bucket(), &participant, cfg(), None);
        assert!(matches!(
            guard.acquire(&store).await.expect("acquire"),
            Acquire::Acquired
        ));
        assert_eq!(
            sleeper.waits(),
            Vec::<Duration>::new(),
            "an uncontended claim waits for nothing (its draw would be {expected:?})"
        );
        assert_eq!(guard.requests().await, 1, "one CreateIfAbsent, no reads");
    }

    /// A held claim is observed with no wait, the skip it reports reschedules
    /// to one millisecond past expiry with no jitter folded in, and the retry
    /// at that point asks for exactly its jitter once, before the steal.
    ///
    /// Shown failing against the pre-change reschedule (the observation's own
    /// `reschedule_after_unix_ms`, which adds the jitter): "the reschedule
    /// point carries no jitter" reads left 1700000003164, right
    /// 1700000003001 (a 163 ms draw for this work id and process). Against the
    /// guard that waited before every first `CreateIfAbsent`, "observing a
    /// live claim waits for nothing" reads left [163ms], right [].
    #[tokio::test]
    async fn a_steal_requests_the_jitter_once_and_the_reschedule_carries_none() {
        let store = MemoryStore::new();
        store.set_clock_ms(1_700_000_000_000);
        let clock = FixedClock::new(1_700_000_000_000 * 1_000_000);
        let holder = guard(&clock, 1, None);
        assert!(matches!(
            holder.acquire(&store).await.expect("holder acquires"),
            Acquire::Acquired
        ));

        let (contender, sleeper) = recording_participant(2, &clock);
        let expected = contender_jitter();

        let first = ClaimGuard::new(&bucket(), &contender, cfg(), None);
        let skip = match first.acquire(&store).await.expect("contender observes") {
            Acquire::Skipped(skip) => skip,
            other => panic!("expected a skip, got {other:?}"),
        };
        assert_eq!(skip.reason, ClaimSkipReason::HeldByAnother);
        assert_eq!(
            sleeper.waits(),
            Vec::<Duration>::new(),
            "observing a live claim waits for nothing"
        );
        assert_eq!(
            skip.reschedule_after_unix_ms,
            skip.expiry_unix_ms + 1,
            "the reschedule point carries no jitter"
        );

        // The retry lands exactly at the reschedule point, where the holder's
        // claim has expired and is stolen.
        store.set_clock_ms(skip.reschedule_after_unix_ms as u64);
        clock.set(skip.reschedule_after_unix_ms * 1_000_000);
        let retry = ClaimGuard::new(&bucket(), &contender, cfg(), None);
        assert!(matches!(
            retry.acquire(&store).await.expect("retry steals"),
            Acquire::Acquired
        ));
        assert_eq!(
            sleeper.waits(),
            vec![expected],
            "the steal requests the jitter exactly once"
        );
        assert_eq!(
            retry.requests().await,
            4,
            "rejected PUT, GET, HEAD, and the steal CAS"
        );
    }

    /// A create refused for a claim that is gone by the read retries its
    /// `CreateIfAbsent` once, and asks for exactly its jitter once, before
    /// that retry and not before the first attempt.
    ///
    /// The total wait is one jitter either way, so the figure that tells the
    /// two orders apart is what the sleeper had been asked for at the moment
    /// the first create was refused. Shown failing against the guard that
    /// waited before every first `CreateIfAbsent`: "the refused first attempt
    /// waited for nothing" reads left Some([163ms]), right Some([]).
    #[tokio::test]
    async fn a_refused_then_retried_create_requests_the_jitter_once() {
        let clock = FixedClock::new(1_700_000_000_000 * 1_000_000);
        let expected = contender_jitter();
        let (participant, sleeper) = recording_participant(2, &clock);
        let store = RefuseFirstCreate {
            inner: MemoryStore::new(),
            refused: std::sync::atomic::AtomicBool::new(false),
            sleeper: Arc::clone(&sleeper),
            waits_at_refusal: std::sync::Mutex::new(None),
        };
        let guard = ClaimGuard::new(&bucket(), &participant, cfg(), None);

        assert!(matches!(
            guard.acquire(&store).await.expect("acquire"),
            Acquire::Acquired
        ));
        assert_eq!(
            store.waits_at_refusal.lock().expect("refusal lock").clone(),
            Some(Vec::<Duration>::new()),
            "the refused first attempt waited for nothing"
        );
        assert_eq!(
            sleeper.waits(),
            vec![expected],
            "the retried create waits out the jitter once"
        );
        assert_eq!(
            guard.requests().await,
            3,
            "the refused PUT, the GET that found the claim gone, and the retried PUT"
        );
    }

    /// A store that answers the first `CreateIfAbsent` with `AlreadyExists`
    /// without writing anything, so the GET that follows finds the key absent:
    /// the claim-vanished interleaving, on demand. At that refusal it records
    /// the waits `sleeper` had been asked for so far.
    struct RefuseFirstCreate {
        inner: MemoryStore,
        refused: std::sync::atomic::AtomicBool,
        sleeper: Arc<RecordingSleeper>,
        waits_at_refusal: std::sync::Mutex<Option<Vec<Duration>>>,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for RefuseFirstCreate {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> std::result::Result<ravel_object_store::PutOutcome, StoreError> {
            if matches!(opts.mode, ravel_object_store::PutMode::CreateIfAbsent)
                && !self.refused.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                *self.waits_at_refusal.lock().expect("refusal lock") = Some(self.sleeper.waits());
                return Err(StoreError::AlreadyExists);
            }
            self.inner.put(key, data, opts).await
        }
        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> std::result::Result<ravel_object_store::GetOutcome, StoreError> {
            self.inner.get(key, range).await
        }
        async fn head(
            &self,
            key: &str,
        ) -> std::result::Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> std::result::Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }
        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> std::result::Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// An unreadable claim is deferred while its age by the store's
    /// `last_modified` is at most one lease plus this contender's jitter, and
    /// past that the run goes ahead unclaimed. Neither side steals: the
    /// unreadable object is left exactly as it was, and no PUT is issued
    /// beyond the rejected `CreateIfAbsent`.
    ///
    /// Shown failing against a guard that skips an unreadable claim whatever
    /// its age (`past_unreadable` always `None`): "past one lease plus jitter
    /// the run proceeds unclaimed" panics with `Skipped(ClaimSkip { reason:
    /// UnreadableClaim, .. })`. Against a skip that reschedules an unreadable
    /// claim to one millisecond past expiry, as a held one is, "the retry is
    /// scheduled for the attempt that runs unclaimed" reads left
    /// 1700000003001, right 1700000003164.
    #[tokio::test]
    async fn an_unreadable_claim_defers_until_a_lease_plus_jitter_then_runs_unclaimed() {
        let store = MemoryStore::new();
        let written_ms: i64 = 1_700_000_000_000;
        store.set_clock_ms(written_ms as u64);
        let key = claim::compaction_claim_key(&bucket_identity().work_id());
        let garbage = bytes::Bytes::from_static(b"\xffnot a claim");
        let written = store
            .put(&key, garbage.clone(), PutOptions::create_if_absent())
            .await
            .expect("seed an unreadable claim");
        let lease_ms = cfg().lease_duration.as_millis() as i64;
        let jitter = jitter_ms(
            &bucket_identity().work_id(),
            &Uuid::from_u128(2),
            lease_ms,
            &cfg(),
        );
        let clock = FixedClock::new(0);

        // Younger than the lease: deferred to the instant it stops holding the
        // bucket back, one millisecond past one lease plus jitter.
        clock.set((written_ms + 1_000) * 1_000_000);
        let young = guard(&clock, 2, None);
        let skip = match young.acquire(&store).await.expect("young observes") {
            Acquire::Skipped(skip) => skip,
            other => panic!("an unreadable claim younger than the lease defers: {other:?}"),
        };
        assert_eq!(skip.reason, ClaimSkipReason::UnreadableClaim);
        assert_eq!(skip.expiry_unix_ms, written_ms + lease_ms);
        assert_eq!(
            skip.reschedule_after_unix_ms,
            written_ms + lease_ms + jitter + 1,
            "the retry is scheduled for the attempt that runs unclaimed"
        );
        assert_eq!(
            young.requests().await,
            3,
            "rejected PUT, GET, HEAD; no steal"
        );

        // Exactly one lease plus jitter old: still deferred.
        clock.set((written_ms + lease_ms + jitter) * 1_000_000);
        let boundary = guard(&clock, 2, None);
        assert!(
            matches!(
                boundary.acquire(&store).await.expect("boundary observes"),
                Acquire::Skipped(ClaimSkip {
                    reason: ClaimSkipReason::UnreadableClaim,
                    ..
                })
            ),
            "at exactly one lease plus jitter the claim still defers"
        );

        // Past it: the run proceeds unclaimed, and the claim is untouched.
        clock.set((written_ms + lease_ms + jitter + 1) * 1_000_000);
        let old = guard(&clock, 2, None);
        match old.acquire(&store).await.expect("old observes") {
            Acquire::Unclaimed { key: seen } => assert_eq!(seen, key),
            other => panic!("past one lease plus jitter the run proceeds unclaimed: {other:?}"),
        }
        assert!(!old.is_held().await, "an unclaimed run holds nothing");
        assert_eq!(
            old.requests().await,
            3,
            "rejected PUT, GET, HEAD; no steal was issued"
        );
        let after = store.get(&key, GetRange::Full).await.expect("claim read");
        assert_eq!(
            after.data, garbage,
            "the unreadable claim was not overwritten"
        );
        assert_eq!(
            after.version, written.version,
            "and its version never moved"
        );
    }

    /// `NotFound` on the steal's CAS (the claim was deleted between the
    /// observation and the steal, as the S3 adapter reports it) is the same
    /// lost race a `MemoryStore`'s `PreconditionFailed` is, not a store error.
    ///
    /// Shown failing with the `NotFound` arm removed from `contend`, which is
    /// the pre-change propagation: "a NotFound steal is a lost race, not an
    /// error" panics with `Store(NotFound)`.
    #[tokio::test]
    async fn not_found_on_steal_is_a_lost_race() {
        let store = NotFoundOnCas(MemoryStore::new());
        store.0.set_clock_ms(1_700_000_000_000);
        let clock = FixedClock::new(1_700_000_000_000 * 1_000_000);
        let holder = guard(&clock, 1, None);
        assert!(matches!(
            holder.acquire(&store).await.expect("holder acquires"),
            Acquire::Acquired
        ));

        store.0.set_clock_ms(1_700_000_004_000);
        clock.set(1_700_000_004_000 * 1_000_000);
        let thief = guard(&clock, 2, None);
        let skip = match thief
            .acquire(&store)
            .await
            .expect("a NotFound steal is a lost race, not an error")
        {
            Acquire::Skipped(skip) => skip,
            other => panic!("expected a skip, got {other:?}"),
        };
        assert_eq!(skip.reason, ClaimSkipReason::StealLost);
        assert_eq!(
            skip.reschedule_after_unix_ms,
            1_700_000_004_000 + cfg().lease_duration.as_millis() as i64,
            "a lost steal waits one full lease from now, not the lapsed expiry"
        );
        assert!(!thief.is_held().await);
        assert_eq!(
            thief.requests().await,
            4,
            "rejected PUT, GET, HEAD, and the steal CAS"
        );
    }

    fn bucket_identity() -> WorkIdentity {
        let b = bucket();
        WorkIdentity::new(b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
    }

    /// A scripted fault that rejects the SECOND PUT to the claim key: the
    /// first is the acquisition's `CreateIfAbsent`, which must succeed for a
    /// renewal to be reachable at all.
    fn faulted_store() -> FaultStore<MemoryStore> {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Transient("renewal store error".into()),
            )
            .with_key_contains(COMPACTION_CLAIMS_PREFIX)
            .with_occurrence(Occurrence::Nth(2)),
        );
        FaultStore::new(MemoryStore::new(), plan)
    }

    /// A genuine store error on renewal surfaces as `ClaimRenewFailed`, never
    /// swallowed into `ClaimLost`: the claim was never confirmed lost, so the
    /// run must abort loudly rather than silently cancel. The fault counter
    /// proves the scripted rejection actually fired.
    #[tokio::test]
    async fn renewal_store_error_is_claim_renew_failed_not_lost() {
        let store = faulted_store();
        let clock = FixedClock::new(0);
        let guard = guard(&clock, 1, None);

        assert!(matches!(
            guard.acquire(&store).await.expect("acquire"),
            Acquire::Acquired
        ));

        // One third of the 3 s lease: past the renewal cadence, nowhere near
        // expiry.
        clock.set(1_000_000_000);
        let err = guard
            .checkpoint(&store, Checkpoint::MergeLoop)
            .await
            .expect_err("a genuine store error on renewal must not be swallowed");
        assert!(
            matches!(err, MaintainError::ClaimRenewFailed { .. }),
            "expected ClaimRenewFailed, got {err:?}"
        );
        assert_eq!(
            store.fault_count(Op::Put, ravel_object_store::fault::FaultKind::Transient),
            1,
            "the scripted renewal fault fired exactly once"
        );
        assert!(
            guard.is_held().await,
            "the claim was never confirmed lost, so this run still believes it holds it"
        );
    }

    /// After a renewal store error leaves the claim in place, a NEW guard for
    /// the SAME bucket in the SAME process reclaims it at once, well before
    /// the lease would expire, and reports the reclaim as an acquisition, not
    /// a steal (ADR-1029, 2026-09-30 amendment; issue #2156).
    ///
    /// Demonstrated failing (mutation c) by reverting the `contend` branch
    /// that recognizes a matching process id: the new guard would then read
    /// the still-unexpired claim as `HeldByAnother` and skip its own bucket.
    #[tokio::test]
    async fn same_process_reclaims_leftover_claim_before_lease_expiry() {
        let store = faulted_store();
        let clock = FixedClock::new(0);
        let first = guard(&clock, 1, None);
        assert!(matches!(
            first.acquire(&store).await.expect("acquire"),
            Acquire::Acquired
        ));

        clock.set(1_000_000_000);
        first
            .checkpoint(&store, Checkpoint::MergeLoop)
            .await
            .expect_err("renewal store error");
        assert_eq!(
            store.fault_count(Op::Put, ravel_object_store::fault::FaultKind::Transient),
            1,
            "the scripted renewal fault fired exactly once"
        );

        // Nowhere near the 3 s lease's expiry (the store's clock never moved
        // off 0): a plain `steal` would refuse this locally.
        let retry = guard(&clock, 1, None);
        assert!(
            matches!(
                retry.acquire(&store).await.expect("same-process reclaim"),
                Acquire::Acquired
            ),
            "the same process reclaims its own leftover claim at once"
        );
        assert!(
            !retry.stolen().await,
            "a reclaim of this process's own claim is reported as an acquisition, not a steal"
        );
        assert!(retry.is_held().await);
    }

    /// A DIFFERENT process observing the same leftover claim still waits out
    /// the lease as before: only a matching process id skips the expiry
    /// check. Demonstrated failing (mutation a) by dropping the process-id
    /// equality check from `contend`, which would let this different process
    /// take the claim over immediately too.
    #[tokio::test]
    async fn different_process_still_skips_held_by_another_after_renew_failure() {
        let store = faulted_store();
        let clock = FixedClock::new(0);
        let first = guard(&clock, 1, None);
        assert!(matches!(
            first.acquire(&store).await.expect("acquire"),
            Acquire::Acquired
        ));

        clock.set(1_000_000_000);
        first
            .checkpoint(&store, Checkpoint::MergeLoop)
            .await
            .expect_err("renewal store error");

        let other = guard(&clock, 2, None);
        let skip = match other.acquire(&store).await.expect("other observes") {
            Acquire::Skipped(skip) => skip,
            other => panic!("expected a skip, got {other:?}"),
        };
        assert_eq!(skip.reason, ClaimSkipReason::HeldByAnother);
        assert!(!other.is_held().await);
    }

    /// A same-process race: guard A still believes it holds the claim (its
    /// last successful write was the original acquisition) when guard B, in
    /// the SAME process, reclaims the same bucket. B's reclaim is a real CAS
    /// against the version A last wrote, so it moves the version out from
    /// under A, and A's next checkpoint discovers the loss exactly like a
    /// steal by another process would.
    ///
    /// Demonstrated failing (mutation b) by setting `stolen` true on the
    /// reclaim path: the counter assertion in the supervisor-level test would
    /// then misreport this as a steal.
    #[tokio::test]
    async fn same_process_race_reclaim_cancels_the_original_guard() {
        let store = MemoryStore::new();
        store.set_clock_ms(1_700_000_000_000);
        let clock = FixedClock::new(1_700_000_000_000 * 1_000_000);
        let a = guard(&clock, 1, None);
        assert!(matches!(
            a.acquire(&store).await.expect("a acquires"),
            Acquire::Acquired
        ));

        // B is a second guard in the same process, contending for the same
        // bucket while A's claim is still live and unexpired.
        let b = guard(&clock, 1, None);
        assert!(
            matches!(
                b.acquire(&store).await.expect("b reclaims"),
                Acquire::Acquired
            ),
            "the same process reclaims its own live claim without waiting for expiry"
        );
        assert!(!b.stolen().await);

        // Past the renewal cadence (a third of the 3 s lease), so A's
        // checkpoint actually attempts a renewal rather than short-circuiting
        // on elapsed time.
        clock.set(1_700_000_001_000 * 1_000_000);
        assert_eq!(
            a.checkpoint(&store, Checkpoint::PartBoundary)
                .await
                .expect("checkpoint"),
            Verdict::Cancel,
            "A's version was moved out from under it by B's reclaim"
        );
        assert!(!a.is_held().await);
    }
}
