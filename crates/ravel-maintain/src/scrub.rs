//! At-rest integrity scrubber: the library half of the durability-hardening
//! epic (ADR-0059 decisions 1 and 3). Ravel's checksum hierarchy
//! (whole-object blake3 at write time, footer/section crc32c on read) is
//! otherwise verified only when a query happens to touch the covered bytes, so
//! bytes nobody queries are never checked by anything. This module re-verifies
//! them on a schedule instead.
//!
//! # Two tiers, one per-object entry point
//!
//! [`scrub_one_object`] is the single unit both the eventual scheduled cursor
//! and the tests call. For one object identified by its [`CommitRecord`] it
//! runs, in order:
//!
//! 1. **Structural tier** (cheap): a suffix GET of the footer, re-running the
//!    footer/section crc32c verification that already lives in
//!    [`ravel_segment`] / [`ravel_logseg`] (the same reader-protocol probe
//!    [`crate::read::load_input_catalog`] uses). The footer crc protects the
//!    section table, including every section's stored crc32c *value*; the
//!    section *bytes* are re-hashed by the content tier below rather than
//!    re-fetched here, so the two tiers together cover the whole hierarchy
//!    without the structural tier ever reading more than the footer.
//! 2. **Content tier** (expensive): a full-object GET, blake3 rehash compared
//!    against `record.content_hash` (bit-rot / partial-write detection).
//!    This is the one check that actually proves the object still
//!    matches what was written.
//! 3. **Postings tier** (only when covering postings are supplied): re-derive
//!    this object's true `__name__` set from its own catalog via
//!    [`ravel_catalog::fetch_segment_names`] -- the exact derivation the fold
//!    that wrote the postings used -- and diff it against what the covering
//!    name-postings object claims for this object's ordinal. A name present in
//!    the true set but absent from the postings' claims for this object is the
//!    false-negative disagreement this tier exists to catch (a query would get "no
//!    match" for data that has a match).
//!
//! ADR-0059 decision 1 folds the content and postings checks onto the one
//! expensive full-object read the cursor is already paying for; that
//! read-sharing is a scheduling concern for the follow-up task's cursor
//! wrapper. [`scrub_one_object`] itself takes no cursor or scheduling state --
//! it is deliberately callable in complete isolation so the scheduled wrapper
//! (and the acceptance test, ADR-0059 decision 4) can drive it directly.
//!
//! # Detection only, never repair
//!
//! Every anomaly is returned as a [`ScrubResult`] variant, never auto-repaired:
//! there is no redundant copy to repair a corrupt segment from (ADR-0058), so
//! this module's job is detection and alarming (the metrics wiring lands in the
//! follow-up task), exactly as ADR-0059's consequences state.

use ravel_catalog::{PostingsBuildError, PostingsLimits, decode_postings, fetch_segment_names};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::catalog::v1::SnapshotEntry;
use ravel_proto::commit::v1::CommitRecord;
use ravel_segment::{FooterOutcome, ReaderLimits};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::clock::Clock;

/// Suffix size probed to locate a footer in the structural tier. Matches
/// [`crate::config::DEFAULT_FOOTER_PROBE_BYTES`]; a footer larger than this
/// costs one extra ranged GET, exactly as the compactor's own footer read does.
const FOOTER_PROBE_BYTES: u64 = 64 * 1024;

/// The covering name-postings object for the postings tier, plus the minimal
/// context the false-negative diff provably needs.
///
/// ADR-0059's shorthand is "the covering postings bytes", but resolving *which*
/// ordinal(s) name this object -- and validating the postings object's exact
/// part binding -- both require the covered parts' concatenated
/// [`SnapshotEntry`] list, which is the same list this object's own entry is
/// one element of. This struct carries that list (`covered_entries`, in
/// `SnapshotHead.parts` order, the order postings ordinals index into) and the
/// covered parts' blake3 (`part_blake3`, the binding [`decode_postings`]
/// checks) alongside the raw `bytes`. The scheduled cursor already holds all of
/// this from the resolved snapshot head.
#[derive(Clone, Copy, Debug)]
pub struct CoveringPostings<'a> {
    /// The RNP1 postings object's full bytes.
    pub bytes: &'a [u8],
    /// The covered parts' blake3 hashes, in `SnapshotHead.parts` order. This is
    /// the part binding [`decode_postings`] rejects a mismatch against.
    pub part_blake3: &'a [[u8; 32]],
    /// Every covered part's entries, concatenated in `SnapshotHead.parts`
    /// order. Postings ordinals index into this list, so this object's own
    /// entry must appear here for the check to run.
    pub covered_entries: &'a [SnapshotEntry],
    /// Decompressed-body cap for [`decode_postings`] (ADR-0020 postings limit).
    pub max_postings_bytes: u64,
}

/// The outcome of scrubbing one object. Anomalies (structural corruption,
/// checksum mismatch, postings disagreement, an object the store refuses to
/// return) are distinguished from a [`ScrubResult::ReadError`], which is a
/// retryable store error, a missing object, or an input/decode inconsistency,
/// and is deliberately *not* a corruption finding: a throttle or a timeout
/// must not be counted as bit rot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScrubResult {
    /// Every requested check passed.
    Clean,
    /// The structural tier found footer/section corruption: the footer crc or
    /// the section table failed to verify. A real anomaly.
    StructuralCorruption {
        /// The typed reader error, rendered for reporting.
        detail: String,
    },
    /// The whole-object blake3 rehash did not match `record.content_hash`
    /// (bit rot or a partially written object). A real anomaly.
    ChecksumMismatch {
        /// The hash the commit record recorded at write time.
        expected: [u8; 32],
        /// The hash of the bytes actually stored now.
        actual: [u8; 32],
    },
    /// The postings tier found a false negative: `name` really is
    /// present on this object, but the covering postings object omits this
    /// object's `ordinal` from that name's postings list, so a query filtering
    /// on `name` would wrongly skip this object.
    PostingsDisagreement {
        /// The `__name__` value present on this object but not claimed for it.
        name: String,
        /// This object's ordinal in the covered-entry list.
        ordinal: u64,
    },
    /// A store error retrying can clear, a missing object, or an input/decode
    /// inconsistency prevented the scrub. Not a corruption finding.
    ReadError {
        /// What went wrong, rendered for reporting.
        detail: String,
        /// The error was a retryable store error
        /// ([`StoreError::is_retryable`]: throttled, timeout, transient) on a
        /// GET of this object, so the cursor should hold its place and retry
        /// the object on a later tick. `false` for `NotFound` (retention
        /// deleted the object after it was listed) and for every input or
        /// decode inconsistency, which a retry would only hit again.
        retryable: bool,
    },
    /// A GET of the object itself (the footer probe, the footer range chase,
    /// or the whole-object read) failed with a store error retrying cannot
    /// clear: anything but `NotFound` and the retryable kinds, such as
    /// `Permanent`, `AccessDenied` or `Corrupted`. The object cannot be
    /// verified, now or later, so this is a finding like a checksum mismatch.
    Unreadable {
        /// The failed GET and its error, rendered for reporting.
        detail: String,
    },
}

/// Classify a failed GET of the object under scrub: a retryable error or
/// `NotFound` is a [`ScrubResult::ReadError`], anything else is
/// [`ScrubResult::Unreadable`].
fn get_failure(what: &str, err: StoreError) -> ScrubResult {
    let detail = format!("{what} GET failed: {err}");
    if err.is_retryable() || matches!(err, StoreError::NotFound) {
        ScrubResult::ReadError {
            detail,
            retryable: err.is_retryable(),
        }
    } else {
        ScrubResult::Unreadable { detail }
    }
}

/// Scrub one object identified by its commit record (ADR-0059 decision 4's
/// deterministic per-object entry point). Runs the structural, content, and
/// (when `covering` is supplied) postings tiers, returning the first anomaly
/// found or [`ScrubResult::Clean`].
///
/// `clock` stamps the detection time on the operational log emitted for each
/// anomaly; it carries no scheduling state and does not influence the verdict.
pub async fn scrub_one_object(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    record: &CommitRecord,
    covering: Option<CoveringPostings<'_>>,
) -> ScrubResult {
    let signal = match ravel_commit::signal::from_proto(record.signal) {
        Ok(signal) => signal,
        Err(_) => {
            return ScrubResult::ReadError {
                detail: format!("commit record carries unknown signal {}", record.signal),
                retryable: false,
            };
        }
    };
    let key = record.object_key.as_str();

    // Tier 1: structural. A store error is classified by `get_failure`; a
    // reader error is real corruption.
    match verify_structure(store, key, signal).await {
        Ok(()) => {}
        Err(StructuralOutcome::Read(result)) => {
            log_unreadable(clock, key, &result);
            return result;
        }
        Err(StructuralOutcome::Corrupt(detail)) => {
            tracing::warn!(
                object_key = key,
                detected_unix_ns = clock.now_ns(),
                detail = %detail,
                "scrub: structural corruption"
            );
            return ScrubResult::StructuralCorruption { detail };
        }
    }

    // Tier 2: content. Full-object blake3 vs the recorded content hash.
    let expected: [u8; 32] = match record.content_hash.as_slice().try_into() {
        Ok(hash) => hash,
        Err(_) => {
            return ScrubResult::ReadError {
                detail: format!(
                    "commit record content_hash is {} bytes, expected 32",
                    record.content_hash.len()
                ),
                retryable: false,
            };
        }
    };
    let full = match store.get(key, GetRange::Full).await {
        Ok(got) => got,
        Err(err) => {
            let result = get_failure("full-object", err);
            log_unreadable(clock, key, &result);
            return result;
        }
    };
    let actual = *blake3::hash(full.data.as_ref()).as_bytes();
    if actual != expected {
        tracing::warn!(
            object_key = key,
            detected_unix_ns = clock.now_ns(),
            "scrub: content-hash mismatch"
        );
        return ScrubResult::ChecksumMismatch { expected, actual };
    }

    // Tier 3: postings, only when a covering object is supplied.
    if let Some(covering) = covering {
        match check_postings(store, record, signal, &covering).await {
            Ok(None) => {}
            Ok(Some((name, ordinal))) => {
                tracing::warn!(
                    object_key = key,
                    detected_unix_ns = clock.now_ns(),
                    name = %name,
                    ordinal,
                    "scrub: postings disagreement (false negative)"
                );
                return ScrubResult::PostingsDisagreement { name, ordinal };
            }
            Err(result) => return result,
        }
    }

    ScrubResult::Clean
}

/// Log an [`ScrubResult::Unreadable`] outcome the way the other anomalies are
/// logged; any other result is left to the caller.
fn log_unreadable(clock: &dyn Clock, key: &str, result: &ScrubResult) {
    if let ScrubResult::Unreadable { detail } = result {
        tracing::warn!(
            object_key = key,
            detected_unix_ns = clock.now_ns(),
            detail = %detail,
            "scrub: object unreadable (non-retryable store error)"
        );
    }
}

/// Structural-tier outcome: a failed GET, already classified by
/// [`get_failure`], or genuine corruption to alarm.
enum StructuralOutcome {
    Read(ScrubResult),
    Corrupt(String),
}

/// Suffix-GET the footer and re-verify the footer/section crc32c hierarchy for
/// the object's format. RSEG (metrics and the other segment signals) goes
/// through [`ravel_segment`]; RLOG (logs) through [`ravel_logseg`]. Both follow
/// the same probe-then-range-chase protocol the compactor's reader uses: one
/// suffix GET, growing to a second ranged GET only if the probe missed the
/// footer.
async fn verify_structure(
    store: &dyn ObjectStoreBackend,
    key: &str,
    signal: Signal,
) -> Result<(), StructuralOutcome> {
    let probe = store
        .get(key, GetRange::Suffix(FOOTER_PROBE_BYTES))
        .await
        .map_err(|err| StructuralOutcome::Read(get_failure("footer suffix", err)))?;
    let total = probe.total_size;

    match signal {
        Signal::Logs => verify_structure_rlog(store, key, &probe.data, total).await,
        _ => verify_structure_rseg(store, key, &probe.data, total).await,
    }
}

async fn verify_structure_rseg(
    store: &dyn ObjectStoreBackend,
    key: &str,
    probe: &[u8],
    total: u64,
) -> Result<(), StructuralOutcome> {
    let limits = ReaderLimits::default();
    match ravel_segment::open_from_suffix(probe, total, limits)
        .map_err(|err| StructuralOutcome::Corrupt(format!("RSEG footer: {err}")))?
    {
        FooterOutcome::Ready(_) => Ok(()),
        FooterOutcome::NeedRange { offset, len } => {
            let tail = store
                .get(key, GetRange::Range(offset, offset + len))
                .await
                .map_err(|err| StructuralOutcome::Read(get_failure("footer range", err)))?;
            match ravel_segment::open_from_suffix(&tail.data, total, limits)
                .map_err(|err| StructuralOutcome::Corrupt(format!("RSEG footer: {err}")))?
            {
                FooterOutcome::Ready(_) => Ok(()),
                FooterOutcome::NeedRange { .. } => Err(StructuralOutcome::Corrupt(
                    "RSEG footer not covered even after range chase".to_string(),
                )),
            }
        }
    }
}

async fn verify_structure_rlog(
    store: &dyn ObjectStoreBackend,
    key: &str,
    probe: &[u8],
    total: u64,
) -> Result<(), StructuralOutcome> {
    match ravel_logseg::open_from_suffix(probe, total)
        .map_err(|err| StructuralOutcome::Corrupt(format!("RLOG footer: {err}")))?
    {
        ravel_logseg::SuffixOutcome::Ready(_) => Ok(()),
        ravel_logseg::SuffixOutcome::NeedRange { offset, len } => {
            let tail = store
                .get(key, GetRange::Range(offset, offset + len))
                .await
                .map_err(|err| StructuralOutcome::Read(get_failure("footer range", err)))?;
            match ravel_logseg::open_from_suffix(&tail.data, total)
                .map_err(|err| StructuralOutcome::Corrupt(format!("RLOG footer: {err}")))?
            {
                ravel_logseg::SuffixOutcome::Ready(_) => Ok(()),
                ravel_logseg::SuffixOutcome::NeedRange { .. } => Err(StructuralOutcome::Corrupt(
                    "RLOG footer not covered even after range chase".to_string(),
                )),
            }
        }
    }
}

/// Run the postings tier. Returns `Ok(None)` when the postings' claims agree
/// with this object's true name set, `Ok(Some((name, ordinal)))` on the first
/// false negative, or `Err` with a [`ScrubResult::ReadError`] on a read or
/// decode failure, never a corruption finding: the content tier has already
/// verified the object's bytes by then. The error is retryable only when the
/// re-derivation's own store read failed retryably.
async fn check_postings(
    store: &dyn ObjectStoreBackend,
    record: &CommitRecord,
    signal: Signal,
    covering: &CoveringPostings<'_>,
) -> Result<Option<(String, u64)>, ScrubResult> {
    let inconsistent = |detail: String| ScrubResult::ReadError {
        detail,
        retryable: false,
    };
    let limits = PostingsLimits {
        max_postings_bytes: covering.max_postings_bytes,
    };
    let decoded = decode_postings(covering.bytes, &limits, covering.part_blake3)
        .map_err(|err| inconsistent(format!("covering postings failed to decode: {err}")))?;

    // Locate this object's ordinal in the concatenated covered-entry list.
    let ordinal = match self_ordinal(covering.covered_entries, record) {
        Some(index) => index as u64,
        None => {
            return Err(inconsistent(
                "this object's entry was not found among the covering postings' covered entries"
                    .to_string(),
            ));
        }
    };
    let entry = &covering.covered_entries[ordinal as usize];

    let tenant_hash: [u8; 16] = record.tenant_hash.as_slice().try_into().map_err(|_| {
        inconsistent(format!(
            "commit record tenant_hash is {} bytes, expected 16",
            record.tenant_hash.len()
        ))
    })?;
    let tenant = TenantHash(tenant_hash);

    // Re-derive the object's true `__name__` set exactly the way the fold that
    // wrote the postings did (shared function, ADR-0059 decision 3).
    let mut true_names: Vec<String> = fetch_segment_names(store, &tenant, signal, entry)
        .await
        .map_err(|err| ScrubResult::ReadError {
            retryable: matches!(&err, PostingsBuildError::Store(store) if store.is_retryable()),
            detail: format!("re-deriving segment names failed: {err}"),
        })?
        .into_iter()
        .collect();
    // Deterministic order so the reported disagreement is stable.
    true_names.sort_unstable();

    for name in true_names {
        let claimed = decoded
            .names
            .binary_search_by(|np| np.name.as_str().cmp(name.as_str()))
            .ok()
            .map(|i| decoded.names[i].ordinals.as_slice());
        let present = matches!(claimed, Some(ordinals) if ordinals.binary_search(&ordinal).is_ok());
        if !present {
            return Ok(Some((name, ordinal)));
        }
    }
    Ok(None)
}

/// Index of `record`'s object within a concatenated covered-entry list, by
/// full commit identity (content hash plus the writer identity fields). Content
/// hashes are the content-addressed identity, so a match is unambiguous; the
/// writer fields are compared too so a hash collision could never mislabel an
/// ordinal.
fn self_ordinal(covered_entries: &[SnapshotEntry], record: &CommitRecord) -> Option<usize> {
    let record_writer_id = Uuid::parse_str(&record.writer_id).ok()?.into_bytes();
    covered_entries.iter().position(|entry| {
        entry.content_hash == record.content_hash
            && entry.shard == record.shard
            && entry.writer_epoch == record.writer_epoch
            && entry.writer_seq == record.writer_seq
            && entry.writer_id.as_slice() == record_writer_id.as_slice()
    })
}

// ---------------------------------------------------------------------------
// Rotating cursor (ADR-0059 decision 1, content tier)
// ---------------------------------------------------------------------------

/// Which part of the commit lineage a [`ScrubTarget`] came from. Compaction
/// folds a set of L0 commit records into an L1 part, and selective-subject
/// erasure (ADR-0064) folds a set into a rewrite part; both leave the L0
/// commit records in place until a later sweep deletes them, so the same
/// bytes can briefly exist at two levels. Once compaction runs on a bucket,
/// the L1 part becomes the only copy of that data still worth scrubbing, so
/// the corpus must carry L1 and rewrite parts too, not just L0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrubLevel {
    /// An L0 commit record: the original segment written at ingest time.
    L0,
    /// An L1 part produced by compaction folding a set of L0s together.
    L1,
    /// A part produced by a selective-subject erasure rewrite.
    Rewrite,
}

impl ScrubLevel {
    /// The label value this level renders as on `/metrics`.
    pub fn as_str(self) -> &'static str {
        match self {
            ScrubLevel::L0 => "l0",
            ScrubLevel::L1 => "l1",
            ScrubLevel::Rewrite => "rewrite",
        }
    }
}

/// One object in a scrub rotation, in the cursor's iteration order.
///
/// The target carries no [`ScrubLevel`]: the caller that builds the corpus
/// also keeps the per-key record it will scrub, and the level belongs beside
/// that record so there is exactly one owner of it. A copy here could disagree
/// with that one, and nothing in the rotation reads a level anyway.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScrubTarget {
    /// The object's full key.
    pub object_key: String,
    /// The object's size in bytes, for byte-budgeted ticks.
    pub object_size: u64,
}

/// Worst-case store requests verifying one object issues: the footer suffix
/// probe, the ranged footer chase the probe grows to when it missed, the
/// whole-object read of the content tier, and the postings tier's one
/// re-derivation read. Charged per object so a unit naming an unusual number
/// of parts fills the request budget instead of running unbounded.
pub const SCRUB_REQUESTS_PER_OBJECT: u64 = 4;

/// Request allowance per listing entry a tick is budgeted. An ordinary commit
/// record costs one record GET plus one object, so the request cap leaves
/// headroom above that and binds only on a unit that names several objects.
pub const SCRUB_REQUESTS_PER_ENTRY: u64 = SCRUB_REQUESTS_PER_OBJECT + 4;

/// How far above the sustained rate a tick may go to catch a rotation up with
/// its deadline. Above this the rotation cannot finish in time and the scrub
/// reports it instead of doing one unbounded tick.
pub const SCRUB_MAX_CATCHUP: u64 = 4;

/// The bounded amount of work one content-tier tick may do (ADR-1686 decision
/// 2, amended). A tick consumes listing entries in key order until either cap
/// is reached, and always consumes at least one unit, so the cursor makes
/// progress even when one unit alone exceeds the budget.
///
/// Both caps are counted, not estimated: every listing entry the walk consumes
/// counts against `max_entries`, and every store request the tick issues
/// counts against `max_requests` whether or not it succeeded. A byte cap
/// cannot do that job, because a failing GET moves no bytes, and a tick that
/// stops on bytes alone walks and GETs the rest of the prefix during a store
/// outage while its marker skips all of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScrubBudget {
    /// Listing entries this tick may consume.
    pub max_entries: u64,
    /// Store requests this tick may issue: listing pages, record GETs, and
    /// [`SCRUB_REQUESTS_PER_OBJECT`] for each object it verifies.
    pub max_requests: u64,
}

impl ScrubBudget {
    /// Whether a tick that has consumed `entries` listing entries and issued
    /// `requests` store requests has filled this budget. An empty slice never
    /// has, so every tick consumes at least one unit.
    pub fn is_filled(self, entries: u64, requests: u64) -> bool {
        if entries == 0 {
            return false;
        }
        entries >= self.max_entries || requests >= self.max_requests
    }
}

/// One tick's budget together with the rotation numbers it was derived from,
/// so the caller can report a rotation that cannot finish in time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickPlan {
    /// The caps this tick runs under.
    pub budget: ScrubBudget,
    /// The rotation's allotted length in seconds: the scrub period `P`, or
    /// half the operator's retention window when that is shorter. An object
    /// deleted before the rotation reaches it is never verified at all, and a
    /// rotation as long as the retention window reaches its oldest objects at
    /// about the age they expire.
    pub rotation_secs: u64,
    /// Entries this tick would have had to consume for the rotation to reach
    /// the end of the listing by its deadline.
    pub needed_entries: u64,
    /// `needed_entries` exceeded the catch-up ceiling, so this rotation will
    /// not finish within `rotation_secs`: the scrub cannot keep up at the
    /// configured period.
    pub behind: bool,
}

/// The rotating content-tier cursor (ADR-0059 decision 1, ADR-1686). Plain
/// data with no I/O; the scheduled wrapper persists it to object storage.
///
/// The position is a start-after marker over the commit shard prefix: the
/// next tick lists strictly after `last_commit_key`, so the store's listing
/// order is the rotation order and no corpus is ever materialised. `None`
/// means "start of a rotation". The entry totals size each tick's budget
/// ([`ScrubCursor::plan_tick`]) and feed the position gauge; the byte totals
/// report the rotation's read bandwidth (ADR-0059 decision 1) and no longer
/// bound a tick, since bytes cannot bound one whose GETs fail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScrubCursor {
    /// Tenant this cursor rotates over.
    pub tenant_hash: TenantHash,
    /// Signal this cursor rotates over.
    pub signal: Signal,
    /// Shard this cursor rotates over.
    pub shard: u32,
    /// The last commit-shard-prefix key this rotation consumed. `None` at a
    /// rotation boundary.
    pub last_commit_key: Option<String>,
    /// Unix-ns anchor for the current rotation's start.
    pub rotation_started_unix_ns: i64,
    /// Sum of `object_size` over the objects this rotation has consumed.
    pub rotation_bytes_seen: u64,
    /// `rotation_bytes_seen` at the end of the previous completed rotation;
    /// `None` until one completes.
    pub last_rotation_bytes: Option<u64>,
    /// Listing entries the LIST-only count at this rotation's start found;
    /// `None` until this rotation has been counted.
    pub rotation_total_entries: Option<u64>,
    /// Listing entries this rotation has consumed so far.
    pub rotation_entries_visited: u64,
    /// Listing entries appended since this rotation began, counted by a
    /// LIST-only pass over the tail window each tick
    /// ([`observe_tail`](Self::observe_tail)). Added to
    /// `rotation_total_entries` so the budget is sized from what the walk has
    /// actually observed rather than from a total that went stale the moment
    /// the rotation opened.
    pub rotation_appended_entries: u64,
    /// The directory (ingest hour) the tail window starts at: the
    /// second-greatest hour directory the last count saw, or the only one.
    /// `None` means the prefix was empty then, so the next count covers the
    /// whole prefix.
    pub rotation_tail_dir: Option<String>,
    /// Entries in and after `rotation_tail_dir` when the last count ran.
    pub rotation_tail_entries: u64,
}

/// The running tally of one LIST-only count pass over the commit shard prefix
/// (ADR-1686 decision 3, amended): how many entries it saw, and how many of
/// them fell in each of the two greatest directories (ingest hours) it met.
///
/// Every writer commits into the current hour, and a commit key is
/// `<hour>/<writer_id>.<epoch>.<seq>.cmt`, so a new commit from a writer whose
/// id sorts low lands below keys already listed in that hour. A count that only
/// looks past the greatest key seen misses it. Counting the whole window from
/// the start of its first directory, and comparing with the same window's
/// count last time, catches every append into those hours whatever its writer.
/// The window keeps the hour before the greatest one as well, so a commit that
/// lands late in the previous hour after the next hour has begun is counted
/// too.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TailTally {
    /// Entries the pass saw.
    pub entries: u64,
    last_dir: Option<String>,
    last_dir_entries: u64,
    prev_dir: Option<String>,
    prev_dir_entries: u64,
}

impl TailTally {
    /// Count one listed key. Keys must arrive in listing order.
    pub fn observe(&mut self, key: &str) {
        self.entries = self.entries.saturating_add(1);
        let dir = match key.rfind('/') {
            Some(slash) => &key[..=slash],
            None => key,
        };
        if self.last_dir.as_deref() == Some(dir) {
            self.last_dir_entries = self.last_dir_entries.saturating_add(1);
        } else {
            self.prev_dir = self.last_dir.replace(dir.to_string());
            self.prev_dir_entries = self.last_dir_entries;
            self.last_dir_entries = 1;
        }
    }

    /// Where the next count starts and how many entries lie in and after it:
    /// the second-greatest directory this pass met with both directories'
    /// entries, or the only directory it met. `None` when it saw nothing.
    pub fn window(&self) -> Option<(String, u64)> {
        match (&self.prev_dir, &self.last_dir) {
            (Some(prev), Some(_)) => Some((
                prev.clone(),
                self.prev_dir_entries.saturating_add(self.last_dir_entries),
            )),
            (None, Some(last)) => Some((last.clone(), self.last_dir_entries)),
            _ => None,
        }
    }
}

impl ScrubCursor {
    /// A fresh cursor at the start of its first rotation.
    pub fn new(tenant_hash: TenantHash, signal: Signal, shard: u32, now_ns: i64) -> Self {
        ScrubCursor {
            tenant_hash,
            signal,
            shard,
            last_commit_key: None,
            rotation_started_unix_ns: now_ns,
            rotation_bytes_seen: 0,
            last_rotation_bytes: None,
            rotation_total_entries: None,
            rotation_entries_visited: 0,
            rotation_appended_entries: 0,
            rotation_tail_dir: None,
            rotation_tail_entries: 0,
        }
    }

    /// Whether this rotation still needs its LIST-only entry count.
    pub fn needs_entry_count(&self) -> bool {
        self.rotation_total_entries.is_none()
    }

    /// Open a rotation from the start of the prefix with the tally of the
    /// LIST-only pass over the whole prefix. Keeps `last_rotation_bytes`,
    /// which reports the previous rotation's bandwidth.
    pub fn start_rotation(&mut self, tally: &TailTally, now_ns: i64) {
        self.last_commit_key = None;
        self.rotation_started_unix_ns = now_ns;
        self.rotation_bytes_seen = 0;
        self.rotation_total_entries = Some(tally.entries);
        self.rotation_entries_visited = 0;
        self.rotation_appended_entries = 0;
        let (dir, entries) = tally.window().map_or((None, 0), |(d, n)| (Some(d), n));
        self.rotation_tail_dir = dir;
        self.rotation_tail_entries = entries;
    }

    /// The start-after key of this tick's LIST-only tail count: the tail
    /// window's directory, which sorts before every key inside it. `None`
    /// counts the whole prefix.
    pub fn tail_count_start(&self) -> Option<&str> {
        self.rotation_tail_dir.as_deref()
    }

    /// Record this tick's tail count, a tally of every entry listed after
    /// [`tail_count_start`](Self::tail_count_start). Its growth over the
    /// window's count last time is what was appended since, and the window
    /// moves to the directories this pass saw last. A window that shrank
    /// (retention or a sweep deleted entries in it) counts no appends, which
    /// can only overstate what is left to walk, never understate it.
    pub fn observe_tail(&mut self, tally: &TailTally) {
        let appended = tally.entries.saturating_sub(self.rotation_tail_entries);
        self.rotation_appended_entries = self.rotation_appended_entries.saturating_add(appended);
        match tally.window() {
            Some((dir, entries)) => {
                self.rotation_tail_dir = Some(dir);
                self.rotation_tail_entries = entries;
            }
            None => self.rotation_tail_entries = 0,
        }
    }

    /// Entries this rotation is expected to cover: the count its opening
    /// LIST-only pass found plus everything appended since.
    pub fn estimated_rotation_entries(&self) -> u64 {
        self.rotation_total_entries
            .unwrap_or(0)
            .saturating_add(self.rotation_appended_entries)
    }

    /// This tick's plan (ADR-1686 decision 3, amended). The rotation is
    /// allotted `min(period_secs, retention_secs / 2)`, since an object
    /// deleted by retention before the walk reaches it is never verified at
    /// all, and the oldest-first walk would reach each object at about its
    /// expiry age if it were allotted the whole retention window. What is
    /// left to cover is divided by the ticks left before that deadline, so a
    /// rotation that has fallen behind speeds up instead of running forever:
    /// on the last tick before the deadline the whole remainder is budgeted.
    ///
    /// The estimate is recomputed every tick from
    /// [`estimated_rotation_entries`](Self::estimated_rotation_entries), which
    /// tracks appends, so a shard that keeps committing cannot outrun the
    /// walk. [`SCRUB_MAX_CATCHUP`] caps how far above the sustained rate one
    /// tick may go; past that cap the returned plan is `behind` and the
    /// caller reports that the scrub cannot keep up.
    pub fn plan_tick(
        &self,
        period_secs: u64,
        tick_secs: u64,
        retention_secs: Option<u64>,
        now_ns: i64,
    ) -> TickPlan {
        let tick_secs = tick_secs.max(1);
        let rotation_secs = match retention_secs {
            Some(retention) => period_secs.max(1).min((retention / 2).max(1)),
            None => period_secs.max(1),
        };
        let deadline_ticks = rotation_secs.div_ceil(tick_secs).max(1);
        let elapsed_ns = now_ns.saturating_sub(self.rotation_started_unix_ns).max(0) as u64;
        let ticks_elapsed = elapsed_ns / tick_secs.saturating_mul(1_000_000_000).max(1);
        let ticks_remaining = deadline_ticks.saturating_sub(ticks_elapsed).max(1);

        let estimated = self.estimated_rotation_entries();
        let remaining = estimated.saturating_sub(self.rotation_entries_visited);
        let needed_entries = remaining.div_ceil(ticks_remaining).max(1);
        let sustained = estimated
            .saturating_mul(tick_secs)
            .div_ceil(rotation_secs)
            .max(1);
        let ceiling = sustained.saturating_mul(SCRUB_MAX_CATCHUP);
        let max_entries = needed_entries.max(sustained).min(ceiling).max(1);
        TickPlan {
            budget: ScrubBudget {
                max_entries,
                max_requests: max_entries.saturating_mul(SCRUB_REQUESTS_PER_ENTRY),
            },
            rotation_secs,
            needed_entries,
            behind: needed_entries > ceiling,
        }
    }

    /// Advance the marker past `entries` consumed listing entries ending at
    /// `last_key`, whose objects total `bytes`.
    pub fn consume(&mut self, last_key: String, entries: u64, bytes: u64) {
        self.last_commit_key = Some(last_key);
        self.rotation_entries_visited = self.rotation_entries_visited.saturating_add(entries);
        self.rotation_bytes_seen = self.rotation_bytes_seen.saturating_add(bytes);
    }

    /// The listing ended: roll the byte total over and return to the start of
    /// the prefix. The next tick counts the new rotation's entries.
    pub fn complete_rotation(&mut self, now_ns: i64) {
        self.last_commit_key = None;
        self.rotation_started_unix_ns = now_ns;
        self.last_rotation_bytes = Some(self.rotation_bytes_seen);
        self.rotation_bytes_seen = 0;
        self.rotation_total_entries = None;
        self.rotation_entries_visited = 0;
        self.rotation_appended_entries = 0;
        self.rotation_tail_dir = None;
        self.rotation_tail_entries = 0;
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_catalog::{DEFAULT_MAX_POSTINGS_BYTES, NamePostings, encode_postings};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::clock::FixedClock;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    fn tenant() -> TenantHash {
        TenantHash([0xab; 16])
    }

    /// Write a real RSEG v6 segment carrying `metrics`, publish its data object,
    /// and return the commit record. The record's `content_hash` is the
    /// segment's true whole-object blake3, so an unmodified object scrubs clean.
    async fn publish_metric_segment(
        store: &MemoryStore,
        writer_id: Uuid,
        seq: u64,
        metrics: &[&str],
    ) -> CommitRecord {
        let created_unix_ns = 500_000 * NS_PER_HOUR;
        let ingest_hour_bucket = 500_000u32;
        let tenant_id = TenantId::new("scrub-test-tenant");
        let series: Vec<SeriesInput> = metrics
            .iter()
            .map(|metric| {
                let labels = LabelSet::new(vec![Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: (*metric).to_string(),
                }])
                .expect("valid labels");
                let series_id = SeriesId::compute(&tenant_id, metric, &labels).expect("series id");
                SeriesInput {
                    series_id,
                    labels,
                    samples: vec![Sample {
                        ts_ns: created_unix_ns,
                        value: 1.0,
                    }],
                }
            })
            .collect();
        let identity = SegmentIdentity {
            tenant_hash: tenant().0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: seq,
        };
        let min_ingest_ts_ns = created_unix_ns - 1_000;
        let max_ingest_ts_ns = created_unix_ns;
        let bounds = IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid record");
        store
            .put(&record.object_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        record
    }

    /// The `SnapshotEntry` a fold would derive from this commit record.
    fn entry_for(record: &CommitRecord) -> SnapshotEntry {
        let writer_id = Uuid::parse_str(&record.writer_id).expect("uuid");
        SnapshotEntry {
            level: 0,
            shard: record.shard,
            ingest_hour_bucket: record.ingest_hour_bucket,
            writer_id: writer_id.into_bytes().to_vec(),
            writer_epoch: record.writer_epoch,
            writer_seq: record.writer_seq,
            content_hash: record.content_hash.clone(),
            object_size: record.object_size,
            min_event_ts_ns: record.min_event_ts_ns,
            max_event_ts_ns: record.max_event_ts_ns,
            sample_count: record.sample_count,
            series_count: record.series_count,
            segment_format_version: record.segment_format_version,
            created_unix_ns: record.created_unix_ns,
            declared_column_stats: Vec::new(),
        }
    }

    /// Encode a name-postings object claiming `claims` (name -> ordinals) over a
    /// single covered part, returning the bytes and that part's blake3 list.
    fn encode_postings_for(
        entry_count: u64,
        claims: &[(&str, &[u64])],
    ) -> (Vec<u8>, Vec<[u8; 32]>) {
        let part_blake3 = vec![[0x11u8; 32]];
        let names: Vec<NamePostings> = claims
            .iter()
            .map(|(name, ordinals)| NamePostings {
                name: (*name).to_string(),
                ordinals: ordinals.to_vec(),
            })
            .collect();
        let bytes = encode_postings(
            tenant().0,
            Signal::Metrics as u32,
            &part_blake3,
            entry_count,
            &names,
        )
        .expect("encode postings");
        (bytes, part_blake3)
    }

    fn covering<'a>(
        bytes: &'a [u8],
        part_blake3: &'a [[u8; 32]],
        entries: &'a [SnapshotEntry],
    ) -> CoveringPostings<'a> {
        CoveringPostings {
            bytes,
            part_blake3,
            covered_entries: entries,
            max_postings_bytes: DEFAULT_MAX_POSTINGS_BYTES,
        }
    }

    #[tokio::test]
    async fn clean_object_scrubs_clean() {
        let store = MemoryStore::new();
        let clock = FixedClock::new(123);
        let record = publish_metric_segment(&store, Uuid::new_v4(), 1, &["cpu", "mem"]).await;

        let result = scrub_one_object(&store, &clock, &record, None).await;
        assert_eq!(result, ScrubResult::Clean);
    }

    #[tokio::test]
    async fn single_bit_flip_is_a_checksum_mismatch() {
        let store = MemoryStore::new();
        let clock = FixedClock::new(0);
        let record = publish_metric_segment(&store, Uuid::new_v4(), 1, &["cpu", "mem"]).await;

        // Flip a bit in the object's page region (the first byte), which the
        // footer-only structural tier never reads, so the content tier's blake3
        // is what catches it. Same GET/flip/Overwrite pattern as
        // crates/ravel-failure-tests/tests/corruption.rs:54-64.
        let existing = store
            .get(&record.object_key, GetRange::Full)
            .await
            .expect("get object");
        let mut corrupted = existing.data.to_vec();
        corrupted[0] ^= 0x01;
        store
            .put(
                &record.object_key,
                Bytes::from(corrupted),
                PutOptions::default(),
            )
            .await
            .expect("overwrite corrupted object");

        let result = scrub_one_object(&store, &clock, &record, None).await;
        match result {
            ScrubResult::ChecksumMismatch { expected, actual } => {
                assert_eq!(expected.as_slice(), record.content_hash.as_slice());
                assert_ne!(expected, actual);
            }
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_object_get_failure_is_classified_by_whether_a_retry_can_clear_it() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault,
        };

        let memory = std::sync::Arc::new(MemoryStore::new());
        let clock = FixedClock::new(0);
        let record = publish_metric_segment(&memory, Uuid::new_v4(), 1, &["cpu"]).await;
        let faulted = |fault: ScriptedFault| {
            FaultStore::new(
                memory.clone(),
                FaultPlan::empty()
                    .with_rule(Rule::new(Op::Get, fault).with_key_contains(&record.object_key)),
            )
        };

        let transient = faulted(ScriptedFault::Transient("injected".to_string()));
        let result = scrub_one_object(&transient, &clock, &record, None).await;
        assert!(
            matches!(
                result,
                ScrubResult::ReadError {
                    retryable: true,
                    ..
                }
            ),
            "a transient GET error is retried, got {result:?}"
        );
        assert_eq!(transient.fault_count(Op::Get, FaultKind::Transient), 1);

        let throttled = faulted(ScriptedFault::Throttled { retry_after_ms: 5 });
        let result = scrub_one_object(&throttled, &clock, &record, None).await;
        assert!(
            matches!(
                result,
                ScrubResult::ReadError {
                    retryable: true,
                    ..
                }
            ),
            "a throttled GET is retried, got {result:?}"
        );
        assert_eq!(throttled.fault_count(Op::Get, FaultKind::Throttled), 1);

        let permanent = faulted(ScriptedFault::Permanent("injected".to_string()));
        let result = scrub_one_object(&permanent, &clock, &record, None).await;
        assert!(
            matches!(result, ScrubResult::Unreadable { .. }),
            "a permanent GET error is a finding, got {result:?}"
        );
        assert_eq!(permanent.fault_count(Op::Get, FaultKind::Permanent), 1);

        memory
            .delete(&record.object_key)
            .await
            .expect("delete object");
        let result = scrub_one_object(&memory, &clock, &record, None).await;
        assert!(
            matches!(
                result,
                ScrubResult::ReadError {
                    retryable: false,
                    ..
                }
            ),
            "a missing object is neither retried nor a finding, got {result:?}"
        );
    }

    #[tokio::test]
    async fn accurate_postings_scrub_clean() {
        let store = MemoryStore::new();
        let clock = FixedClock::new(0);
        let record = publish_metric_segment(&store, Uuid::new_v4(), 1, &["cpu", "mem"]).await;
        let entries = vec![entry_for(&record)];

        // Postings correctly claim both names for ordinal 0 (this object).
        let (bytes, part_blake3) =
            encode_postings_for(entries.len() as u64, &[("cpu", &[0]), ("mem", &[0])]);

        let result = scrub_one_object(
            &store,
            &clock,
            &record,
            Some(covering(&bytes, &part_blake3, &entries)),
        )
        .await;
        assert_eq!(result, ScrubResult::Clean);
    }

    #[tokio::test]
    async fn postings_missing_one_name_is_a_disagreement() {
        let store = MemoryStore::new();
        let clock = FixedClock::new(0);
        let record = publish_metric_segment(&store, Uuid::new_v4(), 1, &["cpu", "mem"]).await;
        let entries = vec![entry_for(&record)];

        // Postings omit "mem" entirely: a false negative for this object. Encode
        // a valid postings object (its own crc passes) whose *claims* are wrong,
        // exactly the anomaly the content-tier cross-check exists to catch.
        let (bytes, part_blake3) = encode_postings_for(entries.len() as u64, &[("cpu", &[0])]);

        let result = scrub_one_object(
            &store,
            &clock,
            &record,
            Some(covering(&bytes, &part_blake3, &entries)),
        )
        .await;
        assert_eq!(
            result,
            ScrubResult::PostingsDisagreement {
                name: "mem".to_string(),
                ordinal: 0,
            }
        );
    }

    #[tokio::test]
    async fn postings_missing_ordinal_for_one_object_is_a_disagreement() {
        // Two objects; "mem" lives on both, but the postings only claim it for
        // ordinal 0, so scrubbing object 1 must catch the missing ordinal.
        let store = MemoryStore::new();
        let clock = FixedClock::new(0);
        let record0 = publish_metric_segment(&store, Uuid::new_v4(), 1, &["cpu", "mem"]).await;
        let record1 = publish_metric_segment(&store, Uuid::new_v4(), 2, &["mem"]).await;
        let entries = vec![entry_for(&record0), entry_for(&record1)];

        // "mem" is present on both objects (ordinals 0 and 1) but postings only
        // claim it for 0. "cpu" is only on object 0.
        let (bytes, part_blake3) =
            encode_postings_for(entries.len() as u64, &[("cpu", &[0]), ("mem", &[0])]);

        // Object 0 agrees.
        let clean = scrub_one_object(
            &store,
            &clock,
            &record0,
            Some(covering(&bytes, &part_blake3, &entries)),
        )
        .await;
        assert_eq!(clean, ScrubResult::Clean);

        // Object 1's "mem" ordinal is missing.
        let result = scrub_one_object(
            &store,
            &clock,
            &record1,
            Some(covering(&bytes, &part_blake3, &entries)),
        )
        .await;
        assert_eq!(
            result,
            ScrubResult::PostingsDisagreement {
                name: "mem".to_string(),
                ordinal: 1,
            }
        );
    }

    /// One tick of the marker walk over `keys` (every entry naming one object
    /// of `size` bytes), driven only through the cursor's own methods the way
    /// the scheduled wrapper drives them. Returns the keys consumed.
    fn walk_tick(
        cursor: &mut ScrubCursor,
        keys: &[String],
        size: u64,
        period_secs: u64,
        tick_secs: u64,
        now_ns: i64,
    ) -> Vec<String> {
        walk_tick_with_retention(cursor, keys, size, period_secs, tick_secs, None, now_ns)
    }

    /// [`walk_tick`] with an operator retention window, which shortens the
    /// rotation's deadline.
    fn walk_tick_with_retention(
        cursor: &mut ScrubCursor,
        keys: &[String],
        size: u64,
        period_secs: u64,
        tick_secs: u64,
        retention_secs: Option<u64>,
        now_ns: i64,
    ) -> Vec<String> {
        walk_tick_planned(
            cursor,
            keys,
            size,
            period_secs,
            tick_secs,
            retention_secs,
            now_ns,
        )
        .0
    }

    /// [`walk_tick_with_retention`], also returning the tick's plan.
    fn walk_tick_planned(
        cursor: &mut ScrubCursor,
        keys: &[String],
        size: u64,
        period_secs: u64,
        tick_secs: u64,
        retention_secs: Option<u64>,
        now_ns: i64,
    ) -> (Vec<String>, TickPlan) {
        if cursor.needs_entry_count() {
            cursor.start_rotation(&tally(keys), now_ns);
        } else {
            // The LIST-only tail count the scheduled wrapper runs each tick:
            // every entry listed after the tail window's start.
            let start = match cursor.tail_count_start() {
                Some(dir) => keys.partition_point(|key| key.as_str() <= dir),
                None => 0,
            };
            cursor.observe_tail(&tally(&keys[start..]));
        }
        let plan = cursor.plan_tick(period_secs, tick_secs, retention_secs, now_ns);
        let start = match &cursor.last_commit_key {
            Some(last) => keys.partition_point(|key| key <= last),
            None => 0,
        };
        let mut consumed = Vec::new();
        let mut requests = 0u64;
        let mut index = start;
        while index < keys.len() && !plan.budget.is_filled(consumed.len() as u64, requests) {
            cursor.consume(keys[index].clone(), 1, size);
            consumed.push(keys[index].clone());
            // One record GET plus one object's verification, the cost of an
            // ordinary commit-record entry.
            requests += 1 + SCRUB_REQUESTS_PER_OBJECT;
            index += 1;
        }
        if index >= keys.len() {
            cursor.complete_rotation(now_ns);
        }
        (consumed, plan)
    }

    /// The tally a LIST-only pass over `keys` produces.
    fn tally(keys: &[String]) -> TailTally {
        let mut tally = TailTally::default();
        for key in keys {
            tally.observe(key);
        }
        tally
    }

    fn entry_keys(n: usize) -> Vec<String> {
        // Zero-padded so lexical order matches numeric order.
        (0..n).map(|i| format!("c/0000/{i:04}.cmt")).collect()
    }

    #[test]
    fn a_first_rotation_covers_every_entry_once_in_ceil_n_over_budget_ticks() {
        // ceil(10 * 1 / 4) = 3 entries per tick, so ceil(10 / 3) = 4 ticks.
        let keys = entry_keys(10);
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        let mut visited: Vec<String> = Vec::new();
        let mut per_tick: Vec<usize> = Vec::new();
        for tick in 0..4 {
            let consumed = walk_tick(&mut cursor, &keys, 5, 4, 1, 1_000 + tick);
            per_tick.push(consumed.len());
            visited.extend(consumed);
        }
        assert_eq!(per_tick, vec![3, 3, 3, 1]);
        assert_eq!(visited, keys, "every entry consumed exactly once, in order");
        assert_eq!(cursor.last_commit_key, None, "the rotation wrapped");
        assert_eq!(cursor.last_rotation_bytes, Some(50));
        assert_eq!(cursor.rotation_bytes_seen, 0);
        assert_eq!(cursor.rotation_total_entries, None);
        assert_eq!(cursor.rotation_entries_visited, 0);
        assert_eq!(cursor.rotation_started_unix_ns, 1_003);
    }

    #[test]
    fn a_completed_rotation_reports_its_bytes_and_sizes_the_next_by_entries() {
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        cursor.start_rotation(&tally(&entry_keys(4)), 1);
        // ceil(4 * 1 / 2) = 2 entries, the sustained rate over the count.
        assert_eq!(cursor.plan_tick(2, 1, None, 1).budget.max_entries, 2);
        cursor.consume("c/0000/a.cmt".to_string(), 3, 700);
        assert_eq!(cursor.rotation_entries_visited, 3);
        assert_eq!(cursor.rotation_bytes_seen, 700);
        cursor.complete_rotation(9);
        assert!(cursor.needs_entry_count());
        cursor.start_rotation(&tally(&entry_keys(4)), 10);
        // The next rotation is sized by entries, not by the 700 bytes the
        // previous one read: those are reported, never a budget.
        assert_eq!(cursor.plan_tick(2, 1, None, 10).budget.max_entries, 2);
        assert_eq!(cursor.last_rotation_bytes, Some(700));
        assert_eq!(cursor.rotation_bytes_seen, 0);
        assert_eq!(cursor.rotation_total_entries, Some(4));
    }

    #[test]
    fn every_tick_consumes_at_least_one_entry_even_past_a_tiny_budget() {
        let tiny = ScrubBudget {
            max_entries: 1,
            max_requests: 1,
        };
        assert!(!tiny.is_filled(0, 0));
        assert!(tiny.is_filled(1, 0));
        // The request cap fills a tick whose entries alone have not.
        let wide = ScrubBudget {
            max_entries: 100,
            max_requests: 8,
        };
        assert!(!wide.is_filled(1, 7));
        assert!(wide.is_filled(1, 8));
        let keys = entry_keys(3);
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        let per_tick: Vec<usize> = (0..3)
            .map(|tick| walk_tick(&mut cursor, &keys, 100, 1_000, 1, tick).len())
            .collect();
        assert_eq!(per_tick, vec![1, 1, 1]);
        assert_eq!(cursor.last_rotation_bytes, Some(300));
    }

    #[test]
    fn period_sized_budget_completes_one_rotation_in_about_p_over_tick_ticks() {
        // 100 entries, P = 7 days, tick = 1 hour. Each rotation's entry budget
        // is ceil(100 * 3600 / 604800) = 1 entry, so it takes 100 ticks, which
        // fits in P / tick = 168.
        let keys = entry_keys(100);
        let period_secs = 7 * 86_400;
        let tick_secs = 3_600;
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        for rotation in 0..2 {
            let mut visited: Vec<String> = Vec::new();
            let mut ticks = 0u64;
            while ticks < period_secs / tick_secs {
                visited.extend(walk_tick(&mut cursor, &keys, 1, period_secs, tick_secs, 42));
                ticks += 1;
                if cursor.last_commit_key.is_none() {
                    break;
                }
            }
            assert_eq!(ticks, 100, "rotation {rotation}");
            assert_eq!(visited, keys, "rotation {rotation}: every entry once");
        }
        assert_eq!(cursor.last_rotation_bytes, Some(100));
    }

    /// Appends per tick in the property below: equal to the rotation's
    /// sustained rate when it opens, `ceil(100 * 3600 / 604800)` = 1.
    const APPENDS_PER_TICK: usize = 1;

    #[test]
    fn a_rotation_completes_while_the_shard_keeps_committing_at_the_sustained_rate() {
        // P = 7 days, tick = 1 hour, so a rotation is allotted 168 ticks. The
        // shard holds 100 entries when the rotation opens and appends
        // `APPENDS_PER_TICK` more every tick, the sustained rate at the start.
        // The walk takes at least the sustained rate every tick, and that rate
        // rises as the appends raise the estimate, so the rotation must still
        // reach the end of the listing inside its 168 ticks.
        let period_secs = 7 * 86_400;
        let tick_secs = 3_600;
        let deadline_ticks = period_secs / tick_secs;
        let mut keys = entry_keys(100);
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        let mut ticks = 0u64;
        while ticks < deadline_ticks {
            let now_ns = ticks as i64 * tick_secs as i64 * 1_000_000_000;
            walk_tick(&mut cursor, &keys, 1, period_secs, tick_secs, now_ns);
            ticks += 1;
            if cursor.last_commit_key.is_none() {
                break;
            }
            for append in 0..APPENDS_PER_TICK {
                keys.push(format!(
                    "c/0000/{:04}.cmt",
                    100 + ticks as usize * APPENDS_PER_TICK + append
                ));
            }
        }
        assert!(
            cursor.last_commit_key.is_none(),
            "rotation did not complete in {deadline_ticks} ticks: marker still at {:?} with \
             {} entries listed",
            cursor.last_commit_key,
            keys.len()
        );
        assert!(ticks <= deadline_ticks, "took {ticks} ticks");
    }

    /// A commit key in the real layout's shape, `<hour>/<writer>.<epoch>.<seq>`.
    fn writer_key(hour: u64, writer: &str, seq: u64) -> String {
        format!("c/0000/{hour:06}/{writer}.1.{seq:08}.cmt")
    }

    #[test]
    fn appends_from_writers_on_both_sides_of_the_tail_key_are_all_counted() {
        // Two writers share the shard: `0a` sorts below `zz` inside every
        // hour. Ticks fall mid-hour, so between two ticks each writer commits
        // three records into the hour the last tick saw and three into the
        // next hour. The `0a` records landing in the hour the last tick saw
        // sort below that tick's greatest key, and a count that only looks
        // above that key misses them. The rotation must either finish inside
        // its 168 ticks or report that it cannot.
        let period_secs = 7 * 86_400;
        let tick_secs = 3_600;
        let deadline_ticks = period_secs / tick_secs;
        let writers = ["0a", "zz"];
        let mut seq = [0u64; 2];
        let mut keys: Vec<String> = Vec::new();
        let mut commit = |keys: &mut Vec<String>, hour: u64, count: u64| {
            for (index, writer) in writers.iter().enumerate() {
                for _ in 0..count {
                    keys.push(writer_key(hour, writer, seq[index]));
                    seq[index] += 1;
                }
            }
            keys.sort();
        };
        // 1,000 entries when the rotation opens, and twelve appended a tick:
        // below the sustained rate `ceil(estimate / 168)` once the appends are
        // counted, so an exact count finishes inside the deadline.
        for hour in 0..100 {
            commit(&mut keys, hour, 5);
        }
        let mut hour = 100;
        commit(&mut keys, hour, 3);

        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        let mut ticks = 0u64;
        let mut behind = 0u64;
        let mut completed = false;
        while ticks < deadline_ticks {
            let now_ns = ticks as i64 * tick_secs as i64 * 1_000_000_000;
            let (_, plan) =
                walk_tick_planned(&mut cursor, &keys, 1, period_secs, tick_secs, None, now_ns);
            ticks += 1;
            if plan.behind {
                behind += 1;
            }
            if cursor.last_commit_key.is_none() {
                completed = true;
                break;
            }
            commit(&mut keys, hour, 3);
            hour += 1;
            commit(&mut keys, hour, 3);
        }
        assert!(
            completed || behind > 0,
            "after {ticks} ticks the rotation neither finished nor reported behind: marker \
             at {:?}, {} entries listed, estimate {}",
            cursor.last_commit_key,
            keys.len(),
            cursor.estimated_rotation_entries()
        );
    }

    #[test]
    fn the_budget_follows_entries_appended_after_the_rotation_began() {
        // 10 entries when the rotation opens, P = 20 ticks: the sustained rate
        // is ceil(10 / 20) = 1 entry per tick. Appending 20 more entries on the
        // first tick must raise the budget within this same rotation rather
        // than waiting for the next one to be sized from it.
        let period_secs = 20;
        let tick_secs = 1;
        let mut keys = entry_keys(10);
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        let first = walk_tick(&mut cursor, &keys, 1, period_secs, tick_secs, 0).len();
        assert_eq!(first, 1, "sustained rate over the entries counted at start");
        keys.extend((10..30).map(|i| format!("c/0000/{i:04}.cmt")));
        let second = walk_tick(&mut cursor, &keys, 1, period_secs, tick_secs, 1_000_000_000).len();
        assert!(
            second > first,
            "the budget ignored the 20 entries appended after the rotation began: \
             tick 1 consumed {first}, tick 2 consumed {second}"
        );
    }

    #[test]
    fn a_rotation_finishes_inside_half_the_configured_retention() {
        // P = 7 days but retention is 24 hours: an object older than 24 hours
        // is deleted, so a rotation that takes longer than that leaves objects
        // unscrubbed for their whole life. A rotation allotted the whole
        // retention window reaches the oldest objects at about the age they
        // expire, so it is allotted half: `retention / 2 / tick` = 12 ticks.
        let period_secs = 7 * 86_400;
        let tick_secs = 3_600;
        let retention_secs = 24 * 3_600;
        let deadline_ticks = retention_secs / 2 / tick_secs;
        let keys = entry_keys(100);
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        let mut ticks = 0u64;
        while ticks < deadline_ticks {
            let now_ns = ticks as i64 * tick_secs as i64 * 1_000_000_000;
            walk_tick_with_retention(
                &mut cursor,
                &keys,
                1,
                period_secs,
                tick_secs,
                Some(retention_secs),
                now_ns,
            );
            ticks += 1;
            if cursor.last_commit_key.is_none() {
                break;
            }
        }
        assert!(
            cursor.last_commit_key.is_none(),
            "the rotation did not finish inside half the {retention_secs}s retention \
             window ({deadline_ticks} ticks)"
        );
        assert_eq!(
            ticks, 12,
            "100 entries at ceil(100 * 3600 / 43200) = 9 a tick"
        );
    }

    #[test]
    fn the_rotation_window_is_the_period_capped_at_half_the_retention() {
        let cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        let day = 86_400;
        let window = |period: u64, retention: Option<u64>| {
            cursor.plan_tick(period, 3_600, retention, 0).rotation_secs
        };
        assert_eq!(window(7 * day, None), 7 * day, "no retention caps nothing");
        assert_eq!(window(7 * day, Some(30 * day)), 7 * day);
        assert_eq!(window(7 * day, Some(14 * day)), 7 * day);
        assert_eq!(window(7 * day, Some(10 * day)), 5 * day);
        assert_eq!(
            window(7 * day, Some(7 * day)),
            7 * day / 2,
            "a period equal to retention would reach each object as it expires"
        );
        assert_eq!(window(7 * day, Some(day)), day / 2);
        assert_eq!(window(7 * day, Some(1)), 1, "never below one second");
    }

    #[test]
    fn a_rotation_that_cannot_finish_by_its_deadline_reports_behind() {
        // 100 entries, retention 24 hours (a 12-hour rotation window), tick 1
        // hour, and the walk has consumed nothing 23 hours in: past the
        // deadline one tick is left and it would have to take all 100
        // entries, well past the catch-up ceiling of
        // 4 * ceil(100 * 3600 / 43200) = 36.
        let period_secs = 7 * 86_400;
        let tick_secs = 3_600;
        let retention_secs = 24 * 3_600;
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        cursor.start_rotation(&tally(&entry_keys(100)), 0);
        let plan = cursor.plan_tick(
            period_secs,
            tick_secs,
            Some(retention_secs),
            23 * tick_secs as i64 * 1_000_000_000,
        );
        assert_eq!(plan.rotation_secs, retention_secs / 2);
        assert_eq!(plan.needed_entries, 100);
        assert_eq!(plan.budget.max_entries, 36);
        assert!(plan.behind);

        // The same rotation at its start is on schedule and not behind.
        let on_schedule = cursor.plan_tick(period_secs, tick_secs, Some(retention_secs), 0);
        assert_eq!(on_schedule.needed_entries, 9);
        assert_eq!(on_schedule.budget.max_entries, 9);
        assert!(!on_schedule.behind);
    }
}
