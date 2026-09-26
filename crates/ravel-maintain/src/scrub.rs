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

use ravel_catalog::{PostingsLimits, decode_postings, fetch_segment_names};
use ravel_object_store::{GetRange, ObjectStoreBackend};
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
/// checksum mismatch, postings disagreement) are distinguished from a
/// [`ScrubResult::ReadError`], which is a transient store or decode failure and
/// is deliberately *not* a corruption finding: a throttle or a timeout must not
/// be counted as bit rot.
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
    /// A transient store error or an input/decode inconsistency prevented the
    /// scrub. Not a corruption finding; the cursor should retry it on a later
    /// tick rather than alarm.
    ReadError {
        /// What went wrong, rendered for reporting.
        detail: String,
    },
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
            };
        }
    };
    let key = record.object_key.as_str();

    // Tier 1: structural. A store error is a ReadError; a reader error is real
    // corruption.
    match verify_structure(store, key, signal).await {
        Ok(()) => {}
        Err(StructuralOutcome::Read(detail)) => return ScrubResult::ReadError { detail },
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
            };
        }
    };
    let full = match store.get(key, GetRange::Full).await {
        Ok(got) => got,
        Err(err) => {
            return ScrubResult::ReadError {
                detail: format!("full-object GET failed: {err}"),
            };
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
            Err(detail) => return ScrubResult::ReadError { detail },
        }
    }

    ScrubResult::Clean
}

/// Structural-tier outcome: a store/read failure to retry, or genuine
/// corruption to alarm.
enum StructuralOutcome {
    Read(String),
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
        .map_err(|err| StructuralOutcome::Read(format!("footer suffix GET failed: {err}")))?;
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
                .map_err(|err| {
                    StructuralOutcome::Read(format!("footer range GET failed: {err}"))
                })?;
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
                .map_err(|err| {
                    StructuralOutcome::Read(format!("footer range GET failed: {err}"))
                })?;
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
/// false negative, or `Err(detail)` on a read/decode inconsistency (classified
/// by the caller as a [`ScrubResult::ReadError`], never a corruption finding).
async fn check_postings(
    store: &dyn ObjectStoreBackend,
    record: &CommitRecord,
    signal: Signal,
    covering: &CoveringPostings<'_>,
) -> Result<Option<(String, u64)>, String> {
    let limits = PostingsLimits {
        max_postings_bytes: covering.max_postings_bytes,
    };
    let decoded = decode_postings(covering.bytes, &limits, covering.part_blake3)
        .map_err(|err| format!("covering postings failed to decode: {err}"))?;

    // Locate this object's ordinal in the concatenated covered-entry list.
    let ordinal = match self_ordinal(covering.covered_entries, record) {
        Some(index) => index as u64,
        None => {
            return Err(
                "this object's entry was not found among the covering postings' covered entries"
                    .to_string(),
            );
        }
    };
    let entry = &covering.covered_entries[ordinal as usize];

    let tenant_hash: [u8; 16] = record.tenant_hash.as_slice().try_into().map_err(|_| {
        format!(
            "commit record tenant_hash is {} bytes, expected 16",
            record.tenant_hash.len()
        )
    })?;
    let tenant = TenantHash(tenant_hash);

    // Re-derive the object's true `__name__` set exactly the way the fold that
    // wrote the postings did (shared function, ADR-0059 decision 3).
    let mut true_names: Vec<String> = fetch_segment_names(store, &tenant, signal, entry)
        .await
        .map_err(|err| format!("re-deriving segment names failed: {err}"))?
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

/// The bounded amount of work one content-tier tick may do (ADR-1686 decision
/// 2). A tick consumes listing entries in key order until the budget is
/// filled, and always consumes at least one, so the cursor makes progress
/// even when a single entry's objects exceed the budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrubBudget {
    /// At most this many listing entries per tick: the budget of a rotation
    /// that has no previous rotation's byte total to size from.
    MaxObjects(u64),
    /// Stop once the slice holds at least this many object bytes.
    MaxBytes(u64),
}

impl ScrubBudget {
    /// Whether a slice that has consumed `entries` listing entries naming
    /// `bytes` object bytes has filled this budget. An empty slice never has.
    pub fn is_filled(self, entries: u64, bytes: u64) -> bool {
        if entries == 0 {
            return false;
        }
        match self {
            ScrubBudget::MaxObjects(max) => entries >= max,
            ScrubBudget::MaxBytes(max) => bytes >= max,
        }
    }
}

/// The rotating content-tier cursor (ADR-0059 decision 1, ADR-1686). Plain
/// data with no I/O; the scheduled wrapper persists it to object storage.
///
/// The position is a start-after marker over the commit shard prefix: the
/// next tick lists strictly after `last_commit_key`, so the store's listing
/// order is the rotation order and no corpus is ever materialised. `None`
/// means "start of a rotation". The byte and entry totals size each tick's
/// budget ([`ScrubCursor::tick_budget`]) and feed the position gauge.
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
        }
    }

    /// Whether this rotation still needs its LIST-only entry count.
    pub fn needs_entry_count(&self) -> bool {
        self.rotation_total_entries.is_none()
    }

    /// Open a rotation from the start of the prefix with the entry count its
    /// LIST-only pass found. Keeps `last_rotation_bytes`, which sizes it.
    pub fn start_rotation(&mut self, total_entries: u64, now_ns: i64) {
        self.last_commit_key = None;
        self.rotation_started_unix_ns = now_ns;
        self.rotation_bytes_seen = 0;
        self.rotation_total_entries = Some(total_entries);
        self.rotation_entries_visited = 0;
    }

    /// This tick's budget: the byte budget from the previous rotation's total
    /// when one completed, else an entry budget from this rotation's count.
    pub fn tick_budget(&self, period_secs: u64, tick_secs: u64) -> ScrubBudget {
        match self.last_rotation_bytes {
            Some(bytes) => per_tick_byte_budget(bytes, period_secs, tick_secs),
            None => per_tick_entry_budget(
                self.rotation_total_entries.unwrap_or(0),
                period_secs,
                tick_secs,
            ),
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
    }
}

/// Size one content-tier tick's byte budget so a full rotation over
/// `total_corpus_bytes` completes in about the scrub period `P`: sustained read
/// bandwidth is `total_corpus_bytes / P`, so one tick of length `tick_secs`
/// reads `total_corpus_bytes * tick_secs / P` (rounded up, and at least one
/// byte so a tiny corpus still advances). This is the explicit, operator-sized
/// budget ADR-0059 decision 1 calls for.
pub fn per_tick_byte_budget(
    total_corpus_bytes: u64,
    period_secs: u64,
    tick_secs: u64,
) -> ScrubBudget {
    let period = period_secs.max(1);
    let per_tick = total_corpus_bytes
        .saturating_mul(tick_secs)
        .div_ceil(period)
        .max(1);
    ScrubBudget::MaxBytes(per_tick)
}

/// The entry budget of a rotation with no previous byte total (ADR-1686
/// decision 3): `ceil(total_entries * tick_secs / P)` listing entries, at
/// least one, so the rotation still completes in about `P`.
pub fn per_tick_entry_budget(total_entries: u64, period_secs: u64, tick_secs: u64) -> ScrubBudget {
    let period = period_secs.max(1);
    let per_tick = total_entries
        .saturating_mul(tick_secs)
        .div_ceil(period)
        .max(1);
    ScrubBudget::MaxObjects(per_tick)
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
        if cursor.needs_entry_count() {
            cursor.start_rotation(keys.len() as u64, now_ns);
        }
        let budget = cursor.tick_budget(period_secs, tick_secs);
        let start = match &cursor.last_commit_key {
            Some(last) => keys.partition_point(|key| key <= last),
            None => 0,
        };
        let mut consumed = Vec::new();
        let mut index = start;
        while index < keys.len() && !budget.is_filled(consumed.len() as u64, size * consumed.len() as u64) {
            cursor.consume(keys[index].clone(), 1, size);
            consumed.push(keys[index].clone());
            index += 1;
        }
        if index >= keys.len() {
            cursor.complete_rotation(now_ns);
        }
        consumed
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
    fn a_completed_rotation_sizes_the_next_by_its_bytes() {
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        cursor.start_rotation(4, 1);
        assert_eq!(cursor.tick_budget(2, 1), ScrubBudget::MaxObjects(2));
        cursor.consume("c/0000/a.cmt".to_string(), 3, 700);
        assert_eq!(cursor.rotation_entries_visited, 3);
        assert_eq!(cursor.rotation_bytes_seen, 700);
        cursor.complete_rotation(9);
        assert!(cursor.needs_entry_count());
        cursor.start_rotation(4, 10);
        // ceil(700 * 1 / 2) = 350 bytes, whatever the entry count.
        assert_eq!(cursor.tick_budget(2, 1), ScrubBudget::MaxBytes(350));
        assert_eq!(cursor.last_rotation_bytes, Some(700));
        assert_eq!(cursor.rotation_bytes_seen, 0);
        assert_eq!(cursor.rotation_total_entries, Some(4));
    }

    #[test]
    fn every_tick_consumes_at_least_one_entry_even_past_a_tiny_byte_budget() {
        // A one-byte budget over 100-byte objects: one entry per tick.
        assert!(!ScrubBudget::MaxBytes(1).is_filled(0, 0));
        assert!(ScrubBudget::MaxBytes(1).is_filled(1, 100));
        assert!(!ScrubBudget::MaxObjects(0).is_filled(0, 0));
        assert!(ScrubBudget::MaxObjects(0).is_filled(1, 0));
        let keys = entry_keys(3);
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        cursor.last_rotation_bytes = Some(1);
        let per_tick: Vec<usize> = (0..3)
            .map(|tick| walk_tick(&mut cursor, &keys, 100, 1_000, 1, tick).len())
            .collect();
        assert_eq!(per_tick, vec![1, 1, 1]);
        assert_eq!(cursor.last_rotation_bytes, Some(300));
    }

    #[test]
    fn period_sized_budget_completes_one_rotation_in_about_p_over_tick_ticks() {
        // 100 entries, P = 7 days, tick = 1 hour. The first rotation's entry
        // budget is ceil(100 * 3600 / 604800) = 1 entry, so it takes 100 ticks;
        // the second's byte budget is ceil(100 * 3600 / 604800) = 1 byte over
        // one-byte objects, 100 ticks again. Both fit in P / tick = 168.
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
}
