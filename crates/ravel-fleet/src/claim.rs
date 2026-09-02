//! Advisory work claims over object-store CAS (ADR-1029 decisions 1 and 2).
//!
//! # What a claim is
//!
//! A claim is one small mutable advisory object per unit of expensive work:
//!
//! ```text
//! sys/maintain/claims/compaction/<work_id_hex>
//! ```
//!
//! It exists to stop two processes paying for the same merge. Compaction is
//! already coordinated for *correctness* -- racing runs serialize at the
//! compaction record's `CreateIfAbsent` and converge on one record -- but the
//! loser pays the full merge first and discards it. Claims suppress that
//! duplicate work in the three windows the rendezvous ownership gate cannot
//! reach: a `ravel-cli maintain compact-tenant` run the fleet's membership
//! cannot see, a membership handoff's bounded double-ownership window, and a
//! live-but-wedged owner an operator takes over from by hand.
//!
//! # This is NOT a lease, and NOT membership
//!
//! Three vocabularies coexist in this codebase and must not blur (ADR-0065
//! decision 1, ADR-1029 decision 1):
//!
//! - [`crate::worker_set::WorkerSet`] is **membership**: heartbeat keys, a
//!   `live_set`, and rendezvous `owner`/`owns` over it. It answers "which
//!   process should look at this unit".
//! - `ravel_maintain::sweep::LeaseCheck` is the **GC reader-protection gate**.
//!   It answers "may this object be physically deleted yet".
//! - This module is a **claim**. It answers "is someone already paying for this
//!   merge". Its vocabulary is `claim`, `acquire`, `renew`, `steal`,
//!   `complete`, `work_id`, and nothing here is called a lease or a lock.
//!
//! The distinction is not cosmetic. A lease is logically a lock, and Ravel's
//! architectural statement is that there are no correctness-critical
//! distributed locks: immutable content-addressed parts and CAS record
//! publication remain the sole correctness mechanism. A claim confers zero
//! publication rights and its absence removes none (ADR-1029 decision 2); the
//! publish path never reads one. A claim bug can waste work, it cannot corrupt
//! data.
//!
//! # Work identity
//!
//! ```text
//! work_id = blake3::derive_key("ravel-compaction-claim-v1",
//!                              tenant_hash || signal_key_prefix
//!                                          || shard_le || hour_le)
//! ```
//!
//! `derive_key` is blake3 keyed by the domain tag, so this hash space cannot
//! collide with any other blake3 use in the codebase even on identical input
//! bytes.
//!
//! The identity is exactly the four fields of a compaction bucket, the
//! granularity at which merges are paid for and the same granularity the
//! compaction record key's bucket prefix is built from. Two things are
//! deliberately **excluded**:
//!
//! - `input_set_hash`. Two nodes whose listings diverge on a sealed bucket must
//!   collide on ONE claim, run once, and surface the divergence through the
//!   existing `InputSetHashDivergence` machinery. Hashing the input view into
//!   the key would let both nodes run and publish under two separate claims,
//!   which is the exact duplicate this module exists to prevent (ADR-1029
//!   rejected alternative 3). It travels in the payload as forensics instead.
//! - Policy and geometry knobs (`max_l1_part_bytes`,
//!   `l1_part_memory_target_bytes`, and any future compaction policy version).
//!   They already change part sets without changing compaction record identity,
//!   and the claim mirrors record identity rather than inventing a finer one. If
//!   a policy version is ever introduced, the domain tag versions the whole
//!   claim key space at once.
//!
//! # Protocol
//!
//! Every step uses only contract-mandatory operations
//! ([`PutMode::CreateIfAbsent`], [`PutMode::CasVersion`], `head()`):
//!
//! 1. [`acquire`] with `CreateIfAbsent`. Success means this attempt owns the
//!    claim and holds the [`Version`] every later write CASes against, with no
//!    extra read. `AlreadyExists` means someone else holds it: read it ONCE
//!    (one GET) plus ONE `head()` for `last_modified`, and return a
//!    [`ClaimObservation`]. **Never poll an active claim**: the caller
//!    reschedules the bucket to [`ClaimObservation::reschedule_after_unix_ms`]
//!    and moves on.
//! 2. [`renew`] with `CasVersion`, using the version from the last successful
//!    write. `PreconditionFailed` is not an error to escalate, it is
//!    [`Renewal::ClaimLost`]: the claim was stolen, and the run stops at its
//!    next cancellation checkpoint and publishes nothing.
//! 3. [`steal`] an expired claim with `CasVersion` against the **observed**
//!    version. The version token is what makes exactly one of N simultaneous
//!    thieves win, and what makes a concurrent renewal by a not-actually-dead
//!    owner defeat the steal (the owner's renewal moves the version, so every
//!    thief's CAS fails). A steal is refused locally, with no store request at
//!    all, while `now < expiry`.
//! 4. [`mark_completed`] with `CasVersion`, or let the claim age out.
//!
//! There is **no unconditional delete anywhere in this module**, and there must
//! never be one: a stale worker's DELETE, issued after its claim was already
//! stolen, would destroy the newer owner's claim and re-open the duplicate
//! window this module closes (ADR-1029 rejected alternative 4). Every mutation
//! is conditional, so a write by a process that is no longer the owner fails
//! instead of clobbering. Claims are reclaimed by lifecycle aging, not by this
//! code.
//!
//! # Expiry comes from the store, not from any node
//!
//! Expiry is `last_modified + lease_duration`, where `last_modified` is the
//! store's own server-generated timestamp read by `head()` on the contention
//! path. The expiry BASE is store time (ADR-1029 rejected alternative 5): the
//! store is the one time base every contender shares. The observer still
//! compares that base against its own clock, so skew can steal early, which
//! the ADR accepts as safe-but-wasteful (advisory layer). `last_modified`'s
//! 1-second granularity is noise against the
//! 300 s default lease, and early stealing from residual skew is safe (the
//! claim is advisory), merely wasteful. `CompactionClaim::owner_clock_ns`
//! exists for operator forensics and is never read here.
//!
//! # Time and randomness are injected
//!
//! No `SystemTime::now()` and no RNG. Time enters only as explicit
//! `now_unix_ms` / `now_unix_ns` parameters, and the acquisition jitter is
//! derived by hashing (see [`jitter_ms`]), so a test pins both.

use std::time::Duration;

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_proto::sys::v1::{ClaimState, CompactionClaim};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

/// Domain tag separating this hash space from every other blake3 use in the
/// codebase, and versioning the whole claim key space at once: bumping it
/// retires every existing claim key rather than mutating any (ADR-1029
/// decision 1).
pub const WORK_ID_DOMAIN_TAG: &str = "ravel-compaction-claim-v1";

/// Format floor for [`CompactionClaim`] (the guard every `sys/` message uses).
/// A stored claim advertising a higher floor is one this reader does not
/// understand: it is observed and rescheduled around, but never stolen and
/// never overwritten.
pub const CLAIM_FORMAT_VERSION: u32 = 1;

/// Ceiling on EVERY lease an observer honors when computing expiry: the
/// holder-declared lease and the observer's own configured fallback alike,
/// since a local misconfiguration above 24 hours has the same
/// suppress-the-bucket-forever property as a holder's. 24 hours is far above
/// any real merge stage and far below "forever". Nothing ever deletes a
/// claim, so an unclamped absurd lease (misconfiguration, or corruption that
/// still decodes) would suppress claimed compaction of its bucket
/// indefinitely; the clamp bounds the outage to one day per incident on the
/// advisory layer.
pub const MAX_OBSERVED_LEASE_MS: i64 = 24 * 60 * 60 * 1000;

/// The prefix every compaction claim lives under: a new additive advisory
/// control-plane prefix beside `sys/maintain/workers/` and `sys/maintain/memo/`.
/// Like both siblings it must sit outside any WORM-protected prefix, since
/// claims are mutated under CAS.
pub const COMPACTION_CLAIMS_PREFIX: &str = "sys/maintain/claims/compaction/";

/// Default lease duration (ADR-1029 decision 1): 300 seconds.
///
/// The lease must exceed the longest non-cancellable stage of a merge (one
/// stream's cursor drain, or one part encode plus PUT), not the whole job: the
/// owner renews at cancellation checkpoints, so only a stage it cannot be
/// interrupted inside can let a live owner's claim expire under it.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(300);

/// Default jitter span, as a fraction of the lease duration: 10% (30 s at the
/// 300 s default). See [`jitter_ms`] for the formula and why it is a fraction
/// of the lease rather than an absolute span.
pub const DEFAULT_JITTER_SPAN_FRACTION: f64 = 0.10;

/// The four fields that identify one unit of claimable compaction work: exactly
/// the fields of a compaction bucket, which is exactly the granularity at which
/// a merge is paid for.
///
/// Held here rather than taken as `ravel_maintain::bucket::Bucket` so this crate
/// keeps its existing dependency direction (`ravel-maintain` depends on
/// `ravel-fleet`, never the reverse) and so the primitive stays reusable by
/// retention, sweep, folds, and erasure later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkIdentity {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub shard: u32,
    pub ingest_hour_bucket: u32,
}

impl WorkIdentity {
    pub fn new(
        tenant_hash: TenantHash,
        signal: Signal,
        shard: u32,
        ingest_hour_bucket: u32,
    ) -> Self {
        WorkIdentity {
            tenant_hash,
            signal,
            shard,
            ingest_hour_bucket,
        }
    }

    /// The keyed-blake3 work id of this identity (see the module docs).
    ///
    /// The hashed body is fixed-width and self-delimiting: the 16-byte tenant
    /// hash, the signal's one-byte key-prefix discriminator (part of the frozen
    /// object-key layout), then the shard and ingest hour as 4 little-endian
    /// bytes each. No field can borrow bytes from its neighbour, so two
    /// different identities always differ in at least one hashed byte.
    pub fn work_id(&self) -> WorkId {
        let mut body = Vec::with_capacity(16 + 1 + 4 + 4);
        body.extend_from_slice(&self.tenant_hash.0);
        body.extend_from_slice(self.signal.key_prefix().as_bytes());
        body.extend_from_slice(&self.shard.to_le_bytes());
        body.extend_from_slice(&self.ingest_hour_bucket.to_le_bytes());
        WorkId(blake3::derive_key(WORK_ID_DOMAIN_TAG, &body))
    }
}

/// The 32-byte identity of one unit of claimable work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkId(pub [u8; 32]);

impl WorkId {
    /// Lowercase hex, as it appears in the claim key.
    pub fn hex(&self) -> String {
        hex::encode(self.0)
    }
}

/// The claim key for one work id:
/// `sys/maintain/claims/compaction/<work_id_hex>`.
pub fn compaction_claim_key(work_id: &WorkId) -> String {
    format!("{COMPACTION_CLAIMS_PREFIX}{}", work_id.hex())
}

/// Claim timing configuration. A plain struct held by the caller: no global
/// state, no clock, no RNG.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClaimConfig {
    /// How long after its last write a claim is treated as expired and
    /// stealable. Default [`DEFAULT_LEASE_DURATION`].
    pub lease_duration: Duration,
    /// Width of the acquisition jitter window, as a fraction of
    /// `lease_duration`. Default [`DEFAULT_JITTER_SPAN_FRACTION`].
    pub jitter_span_fraction: f64,
}

impl Default for ClaimConfig {
    fn default() -> Self {
        ClaimConfig {
            lease_duration: DEFAULT_LEASE_DURATION,
            jitter_span_fraction: DEFAULT_JITTER_SPAN_FRACTION,
        }
    }
}

impl ClaimConfig {
    /// The lease in whole milliseconds, saturating and floored at 1 ms so a
    /// zero or absurd configuration cannot make every claim instantly
    /// stealable.
    fn lease_ms(&self) -> i64 {
        i64::try_from(self.lease_duration.as_millis())
            .unwrap_or(i64::MAX)
            .max(1)
    }

    fn lease_ns(&self) -> i64 {
        i64::try_from(self.lease_duration.as_nanos()).unwrap_or(i64::MAX)
    }
}

/// Who is acquiring, and the forensic fields the payload carries for them.
///
/// `input_set_hash` and `owner_clock_ns` are written for operator forensics and
/// are never read by any decision in this module; see [`CompactionClaim`].
#[derive(Debug, Clone)]
pub struct ClaimOwner {
    /// The ADR-0057/0065 startup UUID, the same identity this process's
    /// `sys/maintain/workers/<process_id>` heartbeat is keyed under.
    pub process_id: Uuid,
    /// Fresh per acquisition, so two successive acquisitions by one process are
    /// distinguishable (which `process_id` alone cannot express).
    pub attempt_id: Uuid,
    /// Informational only.
    pub input_set_hash: Vec<u8>,
    /// The acquiring process's clock, informational only. Injected rather than
    /// read here, per the no-`SystemTime::now()`-in-library-logic rule.
    pub owner_clock_ns: i64,
}

impl ClaimOwner {
    /// An owner carrying no input-set hash and a zero clock stamp: the shape a
    /// caller uses before it has listed the bucket's inputs.
    pub fn new(process_id: Uuid, attempt_id: Uuid, owner_clock_ns: i64) -> Self {
        ClaimOwner {
            process_id,
            attempt_id,
            input_set_hash: Vec::new(),
            owner_clock_ns,
        }
    }

    fn payload(&self, cfg: &ClaimConfig) -> CompactionClaim {
        CompactionClaim {
            format_version: CLAIM_FORMAT_VERSION,
            owner_process_id: self.process_id.as_bytes().to_vec(),
            attempt_id: self.attempt_id.as_bytes().to_vec(),
            input_set_hash: self.input_set_hash.clone(),
            state: ClaimState::Running as i32,
            renewed_count: 0,
            lease_duration_ns: cfg.lease_ns(),
            owner_clock_ns: self.owner_clock_ns,
        }
    }
}

/// The decoded facts about whoever currently holds an observed claim. Every
/// field is forensic: the protocol decides ownership by CAS version tokens, not
/// by comparing any of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimHolder {
    /// `None` when the stored bytes are not a 16-byte UUID.
    pub process_id: Option<Uuid>,
    /// `None` when the stored bytes are not a 16-byte UUID.
    pub attempt_id: Option<Uuid>,
    pub state: ClaimState,
    pub renewed_count: u32,
    pub lease_duration_ns: i64,
    pub input_set_hash: Vec<u8>,
    pub owner_clock_ns: i64,
}

/// One contention-path observation of a claim another attempt holds: the result
/// of exactly one GET plus one `head()`, and everything a caller needs to
/// reschedule without ever polling.
#[derive(Debug, Clone)]
pub struct ClaimObservation {
    pub key: String,
    pub work_id: WorkId,
    /// The version the GET returned. A [`steal`] CASes against exactly this, so
    /// any write that lands between the observation and the steal (another
    /// thief winning, or the owner renewing) defeats it.
    pub version: Version,
    /// The store's server-generated write time, from `head()`. The only time
    /// base expiry is judged against.
    pub last_modified_unix_ms: i64,
    /// `last_modified_unix_ms + lease_duration`, where the lease is the one the
    /// holder declared when readable, else this reader's configured default.
    pub expiry_unix_ms: i64,
    /// When to retry this unit: strictly after `expiry_unix_ms`, offset by this
    /// contender's deterministic [`jitter_ms`] so N contenders do not stampede
    /// the instant a claim expires.
    pub reschedule_after_unix_ms: i64,
    /// `None` when the stored payload does not decode, or advertises a format
    /// floor above [`CLAIM_FORMAT_VERSION`]. Such a claim is rescheduled around
    /// but never stolen: a reader that cannot read a claim cannot know it is
    /// safe to take.
    pub holder: Option<ClaimHolder>,
}

impl ClaimObservation {
    /// The holding process id, when the payload was readable and carried a
    /// well-formed UUID.
    pub fn holder_process_id(&self) -> Option<Uuid> {
        self.holder.as_ref().and_then(|h| h.process_id)
    }

    /// Whether `now_unix_ms` is at or past the observed expiry. Exactly at
    /// expiry counts as expired (the claim is advisory and its base timestamp
    /// is 1-second-granular, so the boundary is noise either way).
    pub fn is_expired(&self, now_unix_ms: i64) -> bool {
        now_unix_ms >= self.expiry_unix_ms
    }
}

/// The result of an [`acquire`].
#[derive(Debug)]
pub enum Acquisition {
    /// This attempt owns the claim. `version` is the CAS token every later
    /// [`renew`] and [`mark_completed`] writes against; it came back with the
    /// PUT, so acquiring costs exactly one request and zero reads.
    Acquired {
        key: String,
        work_id: WorkId,
        version: Version,
        payload: CompactionClaim,
    },
    /// Another attempt holds the claim. The caller reschedules to
    /// `observed.reschedule_after_unix_ms` and does NOT poll.
    Held { observed: ClaimObservation },
    /// The claim existed at the `CreateIfAbsent` and was gone by the read that
    /// followed (a lifecycle sweep, or the owner's own completion aging out
    /// under a bucket policy). A typed retryable outcome, not an error: the
    /// caller may immediately re-`acquire`, which will take the now-absent key.
    Vanished { key: String, work_id: WorkId },
}

/// The result of a [`renew`].
#[derive(Debug)]
pub enum Renewal {
    Renewed {
        version: Version,
        payload: CompactionClaim,
    },
    /// The CAS precondition failed: this attempt is no longer the owner (its
    /// expired claim was stolen). Typed, deliberately not an escalated
    /// `StoreError`: losing a claim is an expected protocol outcome, and the
    /// caller's response is to stop at its next cancellation checkpoint and
    /// publish nothing, not to retry a failed request.
    ClaimLost,
}

/// Why a [`steal`] was refused locally, before any store request was issued.
#[derive(Debug, PartialEq, Eq)]
pub enum StealRefused {
    /// The claim is still live. Stealing an unexpired claim would defeat the
    /// whole mechanism, so it is refused here rather than attempted and lost.
    NotExpired { expiry_unix_ms: i64 },
    /// The observed payload did not decode, or advertises a format floor this
    /// reader does not understand. Never overwrite what you cannot read.
    UnreadableClaim,
}

/// The result of a [`steal`].
#[derive(Debug)]
pub enum Steal {
    /// This attempt took the expired claim over. `version` is the fresh CAS
    /// token for its renewals.
    Acquired {
        key: String,
        version: Version,
        payload: CompactionClaim,
    },
    /// The CAS precondition failed: another thief moved first, or the owner was
    /// not actually dead and renewed. Back off to the reschedule path.
    Lost,
    /// Refused locally; no store request was issued.
    Refused(StealRefused),
}

/// The result of a [`mark_completed`].
#[derive(Debug)]
pub enum Completion {
    Marked {
        version: Version,
        payload: CompactionClaim,
    },
    /// The CAS precondition failed, so nothing was written. Fine, and not an
    /// error: someone stole the claim, which makes it their lifecycle to close.
    /// The published compaction record, not this marker, is what actually means
    /// the work is done.
    NotOwner,
}

/// Deterministic per-contender acquisition jitter, in milliseconds.
///
/// ```text
/// h          = first 8 bytes, little-endian, of blake3(work_id || process_id)
/// span_ms    = lease_ms * jitter_span_fraction        (clamped to >= 0)
/// jitter_ms  = (h * span_ms) >> 64                    (uniform in [0, span_ms))
/// ```
///
/// Properties this shape is chosen for:
///
/// - **No clock and no RNG.** The value is a pure function of `(work_id,
///   process_id, cfg)`, so it is identical on every call in a process and
///   reproducible in a test, and it needs no shared time base between
///   contenders.
/// - **Stable per contender, distinct across contenders.** Hashing the process
///   id in means two contenders on the same work id draw different offsets, so
///   they do not re-collide the instant a claim expires; hashing the work id in
///   means one process's N concurrent buckets (issue #1028's bucket-level
///   concurrency) spread rather than stampede together.
/// - **Relative to the lease, not absolute.** The jitter's job is to spread
///   retries around an expiry whose own scale is the lease, so a deployment
///   that shortens the lease shortens the spread with it automatically.
///
/// The multiply-and-shift maps a full-width `u64` into `[0, span_ms)` without
/// the modulo bias a `%` would introduce, in u128 so nothing overflows.
pub fn jitter_ms(work_id: &WorkId, process_id: &Uuid, cfg: &ClaimConfig) -> i64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&work_id.0);
    hasher.update(process_id.as_bytes());
    let digest = *hasher.finalize().as_bytes();
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest[..8]);
    let h = u64::from_le_bytes(head);

    let span_ms = (cfg.lease_ms() as f64 * cfg.jitter_span_fraction).max(0.0) as u64;
    if span_ms == 0 {
        return 0;
    }
    let scaled = (u128::from(h) * u128::from(span_ms)) >> 64;
    i64::try_from(scaled).unwrap_or(i64::MAX)
}

/// Acquire the claim for `identity`, or observe whoever already holds it.
///
/// One PUT (`CreateIfAbsent`) on the uncontended path, and zero reads: the
/// version needed for later renewals comes back with the PUT. On contention,
/// exactly one GET plus one `head()` and no polling at all -- the caller
/// reschedules the unit to [`ClaimObservation::reschedule_after_unix_ms`].
///
/// Errors are real store failures only. Contention
/// ([`StoreError::AlreadyExists`]) and a claim vanishing under the read
/// ([`StoreError::NotFound`]) are typed [`Acquisition`] outcomes, since both are
/// ordinary protocol states rather than faults.
pub async fn acquire(
    store: &dyn ObjectStoreBackend,
    identity: &WorkIdentity,
    owner: &ClaimOwner,
    cfg: &ClaimConfig,
) -> Result<Acquisition, StoreError> {
    let work_id = identity.work_id();
    let key = compaction_claim_key(&work_id);
    let payload = owner.payload(cfg);

    match store
        .put(
            &key,
            payload.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
    {
        Ok(outcome) => Ok(Acquisition::Acquired {
            key,
            work_id,
            version: outcome.version,
            payload,
        }),
        Err(StoreError::AlreadyExists) => {
            match observe(store, &key, work_id, &owner.process_id, cfg).await? {
                Some(observed) => Ok(Acquisition::Held { observed }),
                None => Ok(Acquisition::Vanished { key, work_id }),
            }
        }
        Err(err) => Err(err),
    }
}

/// Read a held claim once and describe it: one GET for the payload and version,
/// one `head()` for the store's `last_modified`.
///
/// Returns `Ok(None)` when either read finds the key absent -- the claim was
/// swept between the `CreateIfAbsent` and this read. That is a real, benign
/// interleaving, so it is a typed retryable outcome rather than an error or a
/// panic.
async fn observe(
    store: &dyn ObjectStoreBackend,
    key: &str,
    work_id: WorkId,
    observer_process_id: &Uuid,
    cfg: &ClaimConfig,
) -> Result<Option<ClaimObservation>, StoreError> {
    let got = match store.get(key, GetRange::Full).await {
        Ok(got) => got,
        Err(StoreError::NotFound) => return Ok(None),
        Err(err) => return Err(err),
    };
    let meta = match store.head(key).await {
        Ok(meta) => meta,
        Err(StoreError::NotFound) => return Ok(None),
        Err(err) => return Err(err),
    };

    let holder = decode_holder(got.data.as_ref());
    // Expiry uses the lease the OWNER declared when it is readable and sane, so
    // an observer configured with a shorter lease does not steal out from under
    // a longer-leased owner. The base is always the store's timestamp.
    // Clamped above by MAX_OBSERVED_LEASE_MS: nothing ever deletes a claim, so
    // trusting an absurd holder-declared lease (misconfiguration, or decodable
    // corruption) would suppress claimed compaction of the bucket until the
    // heat death of i64. The clamp is an availability guard on the advisory
    // layer; correctness never depended on the lease.
    let lease_ms = holder
        .as_ref()
        .map(|h| h.lease_duration_ns)
        .filter(|ns| *ns > 0)
        .map(|ns| ns / 1_000_000)
        .filter(|ms| *ms > 0)
        .unwrap_or_else(|| cfg.lease_ms())
        .min(MAX_OBSERVED_LEASE_MS);
    let expiry_unix_ms = meta.last_modified_unix_ms.saturating_add(lease_ms);
    let jitter = jitter_ms(&work_id, observer_process_id, cfg);
    // Strictly after expiry even when the jitter draw is 0: retrying in the
    // same millisecond expiry lands on would race the boundary for nothing.
    let reschedule_after_unix_ms = expiry_unix_ms.saturating_add(jitter).saturating_add(1);

    Ok(Some(ClaimObservation {
        key: key.to_string(),
        work_id,
        version: got.version,
        last_modified_unix_ms: meta.last_modified_unix_ms,
        expiry_unix_ms,
        reschedule_after_unix_ms,
        holder,
    }))
}

/// Decode an observed claim's forensic fields, or `None` when the bytes are not
/// a `CompactionClaim`, advertise a format floor above
/// [`CLAIM_FORMAT_VERSION`], or carry the never-written `UNSPECIFIED` state.
/// All three are treated identically by the protocol: observe and reschedule,
/// never steal.
fn decode_holder(bytes: &[u8]) -> Option<ClaimHolder> {
    let claim = CompactionClaim::decode(bytes).ok()?;
    if claim.format_version > CLAIM_FORMAT_VERSION {
        tracing::debug!(
            version = claim.format_version,
            "claim: observing a future-version claim; it will not be stolen"
        );
        return None;
    }
    let state = ClaimState::try_from(claim.state).ok()?;
    if state == ClaimState::Unspecified {
        return None;
    }
    Some(ClaimHolder {
        process_id: uuid_from(&claim.owner_process_id),
        attempt_id: uuid_from(&claim.attempt_id),
        state,
        renewed_count: claim.renewed_count,
        lease_duration_ns: claim.lease_duration_ns,
        input_set_hash: claim.input_set_hash,
        owner_clock_ns: claim.owner_clock_ns,
    })
}

fn uuid_from(bytes: &[u8]) -> Option<Uuid> {
    <[u8; 16]>::try_from(bytes).ok().map(Uuid::from_bytes)
}

/// Renew a held claim under `CasVersion(current_version)`, refreshing the
/// store-side `last_modified` that expiry is judged from.
///
/// The written payload is `payload` with `renewed_count` incremented and
/// `owner_clock_ns` restamped, so the counter means what its name says without
/// every caller having to maintain it. `PreconditionFailed` maps to
/// [`Renewal::ClaimLost`], never to an error: the claim was stolen, and the run
/// stops at its next cancellation checkpoint.
pub async fn renew(
    store: &dyn ObjectStoreBackend,
    key: &str,
    current_version: &Version,
    payload: &CompactionClaim,
    now_unix_ns: i64,
) -> Result<Renewal, StoreError> {
    let next = CompactionClaim {
        renewed_count: payload.renewed_count.saturating_add(1),
        owner_clock_ns: now_unix_ns,
        ..payload.clone()
    };
    match store
        .put(
            key,
            next.encode_to_vec().into(),
            PutOptions {
                mode: PutMode::CasVersion(current_version.clone()),
                checksum: None,
            },
        )
        .await
    {
        Ok(outcome) => Ok(Renewal::Renewed {
            version: outcome.version,
            payload: next,
        }),
        Err(StoreError::PreconditionFailed) => Ok(Renewal::ClaimLost),
        Err(err) => Err(err),
    }
}

/// Take over an expired claim under `CasVersion(observed.version)`.
///
/// Legal only once `now_unix_ms >= observed.expiry_unix_ms`; an earlier attempt
/// is [`StealRefused::NotExpired`] and issues no store request at all. A claim
/// whose payload could not be read is [`StealRefused::UnreadableClaim`] and is
/// likewise never overwritten.
///
/// CASing against the **observed** version is what makes the race safe in both
/// directions: of N thieves observing the same version exactly one write lands,
/// and a renewal by an owner that was merely slow rather than dead moves the
/// version first, defeating every thief.
pub async fn steal(
    store: &dyn ObjectStoreBackend,
    observed: &ClaimObservation,
    owner: &ClaimOwner,
    cfg: &ClaimConfig,
    now_unix_ms: i64,
) -> Result<Steal, StoreError> {
    if observed.holder.is_none() {
        return Ok(Steal::Refused(StealRefused::UnreadableClaim));
    }
    if !observed.is_expired(now_unix_ms) {
        return Ok(Steal::Refused(StealRefused::NotExpired {
            expiry_unix_ms: observed.expiry_unix_ms,
        }));
    }

    // A fresh acquisition: `renewed_count` restarts at 0 and `attempt_id` is
    // the thief's, so a stolen claim is distinguishable from a renewed one.
    let payload = owner.payload(cfg);
    match store
        .put(
            &observed.key,
            payload.encode_to_vec().into(),
            PutOptions {
                mode: PutMode::CasVersion(observed.version.clone()),
                checksum: None,
            },
        )
        .await
    {
        Ok(outcome) => Ok(Steal::Acquired {
            key: observed.key.clone(),
            version: outcome.version,
            payload,
        }),
        Err(StoreError::PreconditionFailed) => Ok(Steal::Lost),
        Err(err) => Err(err),
    }
}

/// Mark a held claim `completed` under `CasVersion(current_version)`.
///
/// This is a courtesy marker for operators and for a contender that would
/// otherwise wait out the lease; it is **not** a completion record. The
/// published compaction record is the only completion marker that means
/// anything (ADR-1029 decision 2).
///
/// `PreconditionFailed` is [`Completion::NotOwner`], not an error: someone stole
/// the claim, so its lifecycle is theirs. Deliberately a CAS update and never a
/// DELETE -- an unconditional delete here is exactly the write that would
/// destroy a newer owner's claim (ADR-1029 rejected alternative 4).
pub async fn mark_completed(
    store: &dyn ObjectStoreBackend,
    key: &str,
    current_version: &Version,
    payload: &CompactionClaim,
    now_unix_ns: i64,
) -> Result<Completion, StoreError> {
    let next = CompactionClaim {
        state: ClaimState::Completed as i32,
        owner_clock_ns: now_unix_ns,
        ..payload.clone()
    };
    match store
        .put(
            key,
            next.encode_to_vec().into(),
            PutOptions {
                mode: PutMode::CasVersion(current_version.clone()),
                checksum: None,
            },
        )
        .await
    {
        Ok(outcome) => Ok(Completion::Marked {
            version: outcome.version,
            payload: next,
        }),
        Err(StoreError::PreconditionFailed) => Ok(Completion::NotOwner),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{InstrumentedStore, StoreOp};
    use ravel_types::TenantId;

    use super::*;

    const LEASE_MS: i64 = 300_000;

    fn identity() -> WorkIdentity {
        WorkIdentity::new(TenantId::new("acme").hash(), Signal::Logs, 3, 480_000)
    }

    fn owner_at(clock_ns: i64) -> ClaimOwner {
        ClaimOwner::new(Uuid::new_v4(), Uuid::new_v4(), clock_ns)
    }

    fn instrumented() -> InstrumentedStore<MemoryStore> {
        InstrumentedStore::new(MemoryStore::new())
    }

    /// `(put, get, head)` completed-call counts on an instrumented store.
    fn counts(store: &InstrumentedStore<MemoryStore>) -> (u64, u64, u64) {
        let snap = store.metrics().snapshot();
        (
            snap.op(StoreOp::Put).calls,
            snap.op(StoreOp::Get).calls,
            snap.op(StoreOp::Head).calls,
        )
    }

    fn acquired(outcome: Acquisition) -> (String, Version, CompactionClaim) {
        match outcome {
            Acquisition::Acquired {
                key,
                version,
                payload,
                ..
            } => (key, version, payload),
            other => panic!("expected Acquired, got {other:?}"),
        }
    }

    fn held(outcome: Acquisition) -> ClaimObservation {
        match outcome {
            Acquisition::Held { observed } => observed,
            other => panic!("expected Held, got {other:?}"),
        }
    }

    async fn stored_claim(store: &dyn ObjectStoreBackend, key: &str) -> CompactionClaim {
        let got = store.get(key, GetRange::Full).await.expect("claim present");
        CompactionClaim::decode(got.data.as_ref()).expect("claim decodes")
    }

    /// The uncontended acquisition costs exactly one PUT and no reads: the CAS
    /// token every later renewal needs comes back with the `CreateIfAbsent`.
    /// This is the "+1 PUT, +0 GET per claimed bucket" figure ADR-1029's
    /// Consequences section commits to, asserted rather than assumed.
    #[tokio::test]
    async fn uncontended_acquire_costs_one_put_and_no_reads() {
        let store = instrumented();
        let cfg = ClaimConfig::default();
        let a = owner_at(1_000);

        let outcome = acquire(&store, &identity(), &a, &cfg)
            .await
            .expect("acquire");
        let (key, _, payload) = acquired(outcome);

        assert_eq!(key, compaction_claim_key(&identity().work_id()));
        assert_eq!(payload.state, ClaimState::Running as i32);
        assert_eq!(payload.renewed_count, 0);
        assert_eq!(counts(&store), (1, 0, 0), "one PUT, zero reads");
    }

    /// An absurd holder-declared lease is clamped to [`MAX_OBSERVED_LEASE_MS`]:
    /// nothing ever deletes a claim, so an unclamped ten-year lease would
    /// suppress claimed compaction of the bucket for ten years. Demonstrated
    /// failing by removing the `.min(MAX_OBSERVED_LEASE_MS)` in `observe`.
    #[tokio::test]
    async fn an_absurd_declared_lease_is_clamped() {
        let store = instrumented();
        store.inner().set_clock_ms(1_700_000_000_000);
        let ten_years = ClaimConfig {
            lease_duration: Duration::from_secs(10 * 365 * 24 * 60 * 60),
            ..ClaimConfig::default()
        };
        let owner = owner_at(1);
        acquired(
            acquire(&store, &identity(), &owner, &ten_years)
                .await
                .expect("acquire"),
        );
        let observed = held(
            acquire(&store, &identity(), &owner_at(2), &ClaimConfig::default())
                .await
                .expect("observe"),
        );
        assert_eq!(
            observed.expiry_unix_ms,
            1_700_000_000_000 + MAX_OBSERVED_LEASE_MS,
            "the declared ten-year lease is honored only up to the 24 h ceiling"
        );
    }

    /// A second contender does NOT take the claim: it observes the holder,
    /// computes expiry from the STORE's `last_modified` (never from any node
    /// clock), and gets a reschedule point strictly past that expiry -- so it
    /// walks away instead of polling. The observation costs exactly one GET and
    /// one HEAD, the "+1 GET +1 HEAD per contender observation" figure.
    #[tokio::test]
    async fn acquire_conflict_observes_holder_and_reschedules() {
        let store = instrumented();
        // A non-zero store clock, so an expiry accidentally computed from a
        // node clock (0 here) could not coincidentally match.
        store.inner().set_clock_ms(1_700_000_000_000);
        let cfg = ClaimConfig::default();
        let a = owner_at(11);
        let b = owner_at(22);

        let first = acquire(&store, &identity(), &a, &cfg)
            .await
            .expect("acquire");
        let (_, _, first_payload) = acquired(first);
        let before = counts(&store);

        let observed = held(
            acquire(&store, &identity(), &b, &cfg)
                .await
                .expect("acquire"),
        );

        assert_eq!(
            observed.holder_process_id(),
            Some(a.process_id),
            "the observation names the first holder"
        );
        assert_eq!(
            observed.holder.as_ref().map(|h| h.attempt_id),
            Some(uuid_from(&first_payload.attempt_id)),
            "and the first holder's attempt"
        );
        assert_eq!(observed.last_modified_unix_ms, 1_700_000_000_000);
        assert_eq!(
            observed.expiry_unix_ms,
            1_700_000_000_000 + LEASE_MS,
            "expiry is the store's last_modified plus the lease, exactly"
        );
        assert!(
            observed.reschedule_after_unix_ms > observed.expiry_unix_ms,
            "reschedule lands strictly after expiry: {} vs {}",
            observed.reschedule_after_unix_ms,
            observed.expiry_unix_ms
        );
        assert_eq!(
            observed.reschedule_after_unix_ms,
            observed.expiry_unix_ms + jitter_ms(&identity().work_id(), &b.process_id, &cfg) + 1,
            "and is exactly expiry + this contender's jitter + 1ms"
        );

        let after = counts(&store);
        assert_eq!(
            (after.0 - before.0, after.1 - before.1, after.2 - before.2),
            (1, 1, 1),
            "the contention path is one rejected PUT, exactly one GET, exactly one HEAD"
        );
    }

    /// Two thieves observing the same expired claim race on `CasVersion`
    /// against the SAME observed version: exactly one lands, the other gets
    /// `PreconditionFailed` mapped to `Lost`. The winner's payload carries its
    /// own fresh `attempt_id`, proving the claim really changed hands rather
    /// than being renewed in place.
    #[tokio::test]
    async fn steal_requires_matching_version() {
        let store = MemoryStore::new();
        let cfg = ClaimConfig::default();
        let original = owner_at(1);
        let thief_one = owner_at(2);
        let thief_two = owner_at(3);

        let (_, _, original_payload) = acquired(
            acquire(&store, &identity(), &original, &cfg)
                .await
                .expect("acquire"),
        );

        let seen_one = held(
            acquire(&store, &identity(), &thief_one, &cfg)
                .await
                .expect("acquire"),
        );
        let seen_two = held(
            acquire(&store, &identity(), &thief_two, &cfg)
                .await
                .expect("acquire"),
        );
        assert_eq!(
            seen_one.version, seen_two.version,
            "both thieves observed the same version"
        );
        let expiry = seen_one.expiry_unix_ms;

        let first = steal(&store, &seen_one, &thief_one, &cfg, expiry)
            .await
            .expect("steal");
        let (_, _, winner_payload) = match first {
            Steal::Acquired {
                key,
                version,
                payload,
            } => (key, version, payload),
            other => panic!("the first thief must win, got {other:?}"),
        };

        let second = steal(&store, &seen_two, &thief_two, &cfg, expiry)
            .await
            .expect("steal");
        assert!(
            matches!(second, Steal::Lost),
            "the second thief must lose on the stale version, got {second:?}"
        );

        assert_ne!(
            winner_payload.attempt_id, original_payload.attempt_id,
            "the winner wrote a fresh attempt id"
        );
        assert_eq!(
            winner_payload.owner_process_id,
            thief_one.process_id.as_bytes().to_vec()
        );
        assert_eq!(
            stored_claim(&store, &seen_one.key).await.attempt_id,
            winner_payload.attempt_id,
            "and exactly one of the two writes survives in the store"
        );
    }

    /// The dispossessed owner learns it lost only when it tries to renew: its
    /// version is stale, so the CAS fails and the outcome is the typed
    /// `ClaimLost`, not an escalated store error. That is the signal the merge
    /// pipeline cancels on.
    #[tokio::test]
    async fn renew_after_steal_fails_precondition() {
        let store = MemoryStore::new();
        let cfg = ClaimConfig::default();
        let original = owner_at(1);
        let thief = owner_at(2);

        let (key, original_version, original_payload) = acquired(
            acquire(&store, &identity(), &original, &cfg)
                .await
                .expect("acquire"),
        );

        let observed = held(
            acquire(&store, &identity(), &thief, &cfg)
                .await
                .expect("acquire"),
        );
        let taken = steal(&store, &observed, &thief, &cfg, observed.expiry_unix_ms)
            .await
            .expect("steal");
        assert!(matches!(taken, Steal::Acquired { .. }), "the steal lands");

        let renewal = renew(&store, &key, &original_version, &original_payload, 99)
            .await
            .expect("renew is not an error");
        assert!(
            matches!(renewal, Renewal::ClaimLost),
            "a renewal on a stolen claim is ClaimLost, got {renewal:?}"
        );
        assert_eq!(
            stored_claim(&store, &key).await.owner_process_id,
            thief.process_id.as_bytes().to_vec(),
            "and the losing renewal wrote nothing over the thief's claim"
        );
    }

    /// Completion is CAS-guarded like every other write in this module: a
    /// process holding a stale version cannot flip the state, and the stored
    /// payload is byte-identical afterwards. This is what makes "never an
    /// unconditional delete" more than a comment -- even the benign-looking
    /// terminal write cannot clobber a newer owner.
    #[tokio::test]
    async fn completed_mark_is_cas_guarded() {
        let store = MemoryStore::new();
        let cfg = ClaimConfig::default();
        let owner = owner_at(1);

        let (key, v1, payload) = acquired(
            acquire(&store, &identity(), &owner, &cfg)
                .await
                .expect("acquire"),
        );
        let renewed = renew(&store, &key, &v1, &payload, 500)
            .await
            .expect("renew");
        let (v2, payload_v2) = match renewed {
            Renewal::Renewed { version, payload } => (version, payload),
            other => panic!("expected Renewed, got {other:?}"),
        };
        assert_eq!(payload_v2.renewed_count, 1, "renewal counts itself");
        let before = stored_claim(&store, &key).await;

        let stale = mark_completed(&store, &key, &v1, &payload_v2, 600)
            .await
            .expect("mark_completed is not an error");
        assert!(
            matches!(stale, Completion::NotOwner),
            "a stale version cannot complete the claim, got {stale:?}"
        );
        let after = stored_claim(&store, &key).await;
        assert_eq!(before, after, "the stored payload is unchanged");
        assert_eq!(after.state, ClaimState::Running as i32);

        // The current version still can, and that write is also a CAS.
        let fresh = mark_completed(&store, &key, &v2, &payload_v2, 700)
            .await
            .expect("mark_completed");
        assert!(matches!(fresh, Completion::Marked { .. }));
        assert_eq!(
            stored_claim(&store, &key).await.state,
            ClaimState::Completed as i32
        );
    }

    /// A steal before expiry is refused in this process, with ZERO store
    /// requests. The refusal has to be local: an attempted-and-lost steal would
    /// still cost a PUT (the expensive request class) on every contender on
    /// every tick, which is exactly the per-tick cost shape ADR-1029 rejects.
    #[tokio::test]
    async fn steal_before_expiry_is_refused() {
        let store = instrumented();
        let cfg = ClaimConfig::default();
        let owner = owner_at(1);
        let thief = owner_at(2);

        acquire(&store, &identity(), &owner, &cfg)
            .await
            .expect("acquire");
        let observed = held(
            acquire(&store, &identity(), &thief, &cfg)
                .await
                .expect("acquire"),
        );
        let before = counts(&store);

        let refused = steal(&store, &observed, &thief, &cfg, observed.expiry_unix_ms - 1)
            .await
            .expect("steal");

        assert_eq!(
            refused_reason(refused),
            StealRefused::NotExpired {
                expiry_unix_ms: observed.expiry_unix_ms
            }
        );
        assert_eq!(
            counts(&store),
            before,
            "no store request of any kind was issued"
        );
        assert_eq!(counts(&store).0 - before.0, 0, "zero PUTs in particular");
    }

    fn refused_reason(outcome: Steal) -> StealRefused {
        match outcome {
            Steal::Refused(reason) => reason,
            other => panic!("expected a local refusal, got {other:?}"),
        }
    }

    /// A store that rejects the renewal's conditional write surfaces as
    /// `ClaimLost`, the same typed outcome a real steal produces: the caller
    /// has one cancellation path, not two. The fault counter proves the
    /// scripted rejection actually fired rather than the test passing on a
    /// happy path.
    #[tokio::test]
    async fn scripted_precondition_failure_on_renew_is_claim_lost() {
        // Nth(2): the acquire's CreateIfAbsent is PUT 1 and must succeed; the
        // renewal's CasVersion is PUT 2 and is the one rejected.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
                .with_key_contains(COMPACTION_CLAIMS_PREFIX)
                .with_occurrence(Occurrence::Nth(2)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        let cfg = ClaimConfig::default();
        let owner = owner_at(1);

        let (key, version, payload) = acquired(
            acquire(&store, &identity(), &owner, &cfg)
                .await
                .expect("acquire"),
        );

        let renewal = renew(&store, &key, &version, &payload, 42)
            .await
            .expect("a rejected conditional write is not an error");
        assert!(
            matches!(renewal, Renewal::ClaimLost),
            "a scripted PreconditionFailed maps to ClaimLost, got {renewal:?}"
        );
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
            1,
            "the scripted rejection fired exactly once"
        );
    }

    /// The claim is swept between the contention-path GET and the HEAD that
    /// dates it. There is no observation to return and nothing to reschedule
    /// against, so the outcome is the typed retryable `Vanished` -- not a
    /// panic, and not an unwrap on a missing `ObjectMeta`.
    #[tokio::test]
    async fn swept_claim_on_head_surfaces_as_vanished() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Head, ScriptedFault::NotFoundBlip)
                .with_key_contains(COMPACTION_CLAIMS_PREFIX),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        let cfg = ClaimConfig::default();
        let owner = owner_at(1);
        let contender = owner_at(2);

        acquire(&store, &identity(), &owner, &cfg)
            .await
            .expect("acquire");
        let outcome = acquire(&store, &identity(), &contender, &cfg)
            .await
            .expect("a vanished claim is not an error");

        match outcome {
            Acquisition::Vanished { key, work_id } => {
                assert_eq!(key, compaction_claim_key(&identity().work_id()));
                assert_eq!(work_id, identity().work_id());
            }
            other => panic!("expected Vanished, got {other:?}"),
        }
        assert_eq!(
            store.fault_count(Op::Head, FaultKind::NotFoundBlip),
            1,
            "the scripted NotFound on head() fired exactly once"
        );
    }

    /// Jitter is a pure function of `(work_id, process_id, cfg)`: stable across
    /// calls (so a contender does not re-draw and re-collide), and different per
    /// contender (so N contenders on one work id do not stampede the instant it
    /// expires). Both halves matter; either alone is satisfiable by a constant.
    #[test]
    fn jitter_is_deterministic_and_distinct() {
        let cfg = ClaimConfig::default();
        let work = identity().work_id();
        let a = Uuid::from_u128(0xA);
        let b = Uuid::from_u128(0xB);

        let first = jitter_ms(&work, &a, &cfg);
        for _ in 0..16 {
            assert_eq!(jitter_ms(&work, &a, &cfg), first, "jitter must not vary");
        }
        assert_ne!(
            jitter_ms(&work, &b, &cfg),
            first,
            "two contenders on one work id must draw different offsets"
        );

        // And it stays inside the configured span, which is what bounds the
        // reschedule delay a contender pays.
        let span = (LEASE_MS as f64 * DEFAULT_JITTER_SPAN_FRACTION) as i64;
        for pid in [a, b, Uuid::from_u128(0xC), Uuid::from_u128(0xD)] {
            let j = jitter_ms(&work, &pid, &cfg);
            assert!((0..span).contains(&j), "jitter {j} outside [0, {span})");
        }

        // A different work id moves the offset too, so one process's concurrent
        // buckets spread rather than firing together.
        let other =
            WorkIdentity::new(TenantId::new("acme").hash(), Signal::Logs, 4, 480_000).work_id();
        assert_ne!(jitter_ms(&other, &a, &cfg), first);
    }

    /// A claim whose payload is raw garbage (not protobuf at all) degrades
    /// gracefully: acquire returns Held rather than panicking or erroring, the
    /// observation falls back to the observer's configured lease for expiry,
    /// and steal refuses it as unreadable. The operator repair for a wedged
    /// malformed claim is a manual delete, safe because the claim is advisory
    /// (docs/catalog-and-mvcc.md states the path).
    #[tokio::test]
    async fn a_corrupt_claim_payload_is_observed_and_never_stolen() {
        let store = instrumented();
        store.inner().set_clock_ms(1_700_000_000_000);
        let cfg = ClaimConfig::default();
        let key = compaction_claim_key(&identity().work_id());
        store
            .put(
                &key,
                b"\xff\xfenot a protobuf".to_vec().into(),
                PutOptions {
                    mode: PutMode::CreateIfAbsent,
                    checksum: None,
                },
            )
            .await
            .expect("seed the corrupt claim");

        let observed = held(
            acquire(&store, &identity(), &owner_at(1), &cfg)
                .await
                .expect("acquire over a corrupt claim must not error"),
        );
        assert!(observed.holder.is_none(), "the payload must not decode");
        assert_eq!(
            observed.expiry_unix_ms,
            1_700_000_000_000 + i64::try_from(cfg.lease_duration.as_millis()).expect("fits"),
            "an undecodable payload falls back to the observer's own lease"
        );
        let refused = steal(
            &store,
            &observed,
            &owner_at(2),
            &cfg,
            observed.expiry_unix_ms + 1,
        )
        .await
        .expect("steal");
        assert_eq!(refused_reason(refused), StealRefused::UnreadableClaim);
    }

    /// A claim this reader cannot understand is observed and rescheduled
    /// around, never stolen: taking over a claim whose payload you cannot read
    /// means overwriting state whose meaning you do not know.
    #[tokio::test]
    async fn a_future_version_claim_is_never_stolen() {
        let store = MemoryStore::new();
        let cfg = ClaimConfig::default();
        let contender = owner_at(1);
        let key = compaction_claim_key(&identity().work_id());
        let future = CompactionClaim {
            format_version: CLAIM_FORMAT_VERSION + 1,
            ..owner_at(0).payload(&cfg)
        };
        store
            .put(
                &key,
                future.encode_to_vec().into(),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("seed a future-version claim");

        let observed = held(
            acquire(&store, &identity(), &contender, &cfg)
                .await
                .expect("acquire"),
        );
        assert!(observed.holder.is_none(), "the payload is not understood");
        assert_eq!(
            observed.expiry_unix_ms,
            observed.last_modified_unix_ms + LEASE_MS,
            "expiry falls back to this reader's configured lease"
        );

        // Even long past expiry, the steal is refused locally.
        let refused = steal(
            &store,
            &observed,
            &contender,
            &cfg,
            observed.expiry_unix_ms + 1_000_000,
        )
        .await
        .expect("steal");
        assert_eq!(refused_reason(refused), StealRefused::UnreadableClaim);
        assert_eq!(
            stored_claim(&store, &key).await,
            future,
            "the future-version claim is untouched"
        );
    }

    /// Two divergent input-set views of one bucket collide on ONE claim. This
    /// is the property that makes excluding `input_set_hash` from the work id
    /// load-bearing: if it were hashed in, these two would take two claims and
    /// both merge, which is the duplicate the module exists to prevent.
    #[tokio::test]
    async fn divergent_input_set_hashes_collide_on_one_claim() {
        let store = MemoryStore::new();
        let cfg = ClaimConfig::default();
        let mut a = owner_at(1);
        a.input_set_hash = vec![0xAA; 16];
        let mut b = owner_at(2);
        b.input_set_hash = vec![0xBB; 16];

        let (key_a, _, _) = acquired(acquire(&store, &identity(), &a, &cfg).await.expect("a"));
        let observed = held(acquire(&store, &identity(), &b, &cfg).await.expect("b"));

        assert_eq!(key_a, observed.key, "one key, whatever each node listed");
        assert_eq!(observed.holder_process_id(), Some(a.process_id));
    }

    /// Golden vector pinning the byte-level derivation the key layout doc
    /// freezes: 16 raw tenant-hash bytes, the one-byte signal prefix, shard
    /// and hour as little-endian u32, under the v1 domain tag. The proptest
    /// below proves stability and collision-freedom but survives a field
    /// reorder or an endianness flip; this exact hex does not.
    #[test]
    fn work_id_matches_the_frozen_golden_vector() {
        let id = WorkIdentity::new(
            TenantHash([
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ]),
            Signal::Logs,
            3,
            491_000,
        );
        assert_eq!(
            id.work_id().hex(),
            "9dce3a3a7f3fbda97c47741ec90e0e09f91a4140bd6303967a8cfdcf3c816039",
            "the work-id derivation is frozen by docs/catalog-and-mvcc.md; a \
             field reorder, endianness flip, or domain-tag edit lands here"
        );
    }

    proptest::proptest! {
        /// The work id is a function of the four identity fields alone: stable
        /// across calls, and collision-free over the sampled identity domain.
        /// The key is derived from it, so a collision would silently merge two
        /// buckets' claims and let one starve the other.
        #[test]
        fn work_id_is_stable_and_collision_free(
            tenants in proptest::collection::vec("[a-z]{1,12}", 1..6),
            shards in proptest::collection::vec(0u32..64, 1..6),
            hours in proptest::collection::vec(0u32..1_000_000, 1..6),
        ) {
            let signals = [Signal::Metrics, Signal::Logs, Signal::Spans];
            let mut seen = std::collections::HashMap::new();
            for tenant in &tenants {
                let hash = TenantId::new(tenant).hash();
                for signal in signals {
                    for shard in &shards {
                        for hour in &hours {
                            let identity =
                                WorkIdentity::new(hash, signal, *shard, *hour);
                            let id = identity.work_id();
                            proptest::prop_assert_eq!(
                                id,
                                identity.work_id(),
                                "work_id must be stable across calls"
                            );
                            let previous = seen.insert(id, identity);
                            proptest::prop_assert!(
                                previous.is_none_or(|p| p == identity),
                                "work id collision between distinct identities"
                            );
                        }
                    }
                }
            }
        }
    }
}
