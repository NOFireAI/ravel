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

/// One object in a scrub rotation, in the cursor's iteration order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScrubTarget {
    /// The object's full key.
    pub object_key: String,
    /// The object's size in bytes, for byte-budgeted ticks.
    pub object_size: u64,
}

/// The bounded amount of work one content-tier tick may do. Every tick scrubs
/// at least one object even when a single object exceeds the byte budget, so
/// the cursor always makes progress and can never wedge on an oversized object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrubBudget {
    /// At most this many objects per tick.
    MaxObjects(u64),
    /// At most this many bytes per tick.
    MaxBytes(u64),
}

/// The rotating content-tier cursor (ADR-0059 decision 1). A small plain-data
/// record the follow-up task persists to object storage the same way the fold
/// watermark is; this task implements only the pure advancement logic, so the
/// struct carries no I/O and every field is public.
///
/// `last_object_key` is the position within the current rotation: the next tick
/// resumes at the first object whose key sorts strictly after it. `None` means
/// "start of a rotation" (either the very first tick or a just-completed
/// rotation that wrapped). `rotation_started_unix_ns` anchors the current
/// rotation's start for the `ravel_scrub_cursor_position` gauge the follow-up
/// exposes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScrubCursor {
    /// Tenant this cursor rotates over.
    pub tenant_hash: TenantHash,
    /// Signal this cursor rotates over.
    pub signal: Signal,
    /// Shard this cursor rotates over.
    pub shard: u32,
    /// Position within the current rotation: resume after this key. `None` at a
    /// rotation boundary.
    pub last_object_key: Option<String>,
    /// Unix-ns anchor for the current rotation's start.
    pub rotation_started_unix_ns: i64,
}

impl ScrubCursor {
    /// A fresh cursor at the start of its first rotation.
    pub fn new(tenant_hash: TenantHash, signal: Signal, shard: u32, now_ns: i64) -> Self {
        ScrubCursor {
            tenant_hash,
            signal,
            shard,
            last_object_key: None,
            rotation_started_unix_ns: now_ns,
        }
    }
}

/// One tick's plan: the objects to scrub now and the cursor to persist after.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScrubSlice {
    /// Object keys to scrub this tick, in corpus order.
    pub scrub_keys: Vec<String>,
    /// The cursor to persist once this slice's objects have been scrubbed.
    pub next_cursor: ScrubCursor,
    /// `true` when this slice reached the corpus tail: the rotation is complete
    /// and `next_cursor` has wrapped to the start with a fresh
    /// `rotation_started_unix_ns`.
    pub rotation_complete: bool,
}

/// Compute the next content-tier slice given the current cursor, the corpus to
/// rotate over (in key order, as a strongly consistent LIST returns it), a
/// per-tick budget, and the current wall-clock ns for stamping a new rotation.
///
/// Pure and I/O-free: the scheduled wrapper LISTs the corpus, calls this,
/// scrubs `scrub_keys` via [`scrub_one_object`], then persists `next_cursor`.
/// Deletions and insertions between ticks are tolerated because the resume
/// point is a key comparison, not an index: the cursor simply resumes at the
/// first surviving key after `last_object_key`.
pub fn advance_cursor(
    cursor: &ScrubCursor,
    corpus: &[ScrubTarget],
    budget: ScrubBudget,
    now_ns: i64,
) -> ScrubSlice {
    if corpus.is_empty() {
        // Nothing to rotate over: the rotation is trivially complete.
        return ScrubSlice {
            scrub_keys: Vec::new(),
            next_cursor: ScrubCursor {
                last_object_key: None,
                rotation_started_unix_ns: now_ns,
                ..cursor.clone()
            },
            rotation_complete: true,
        };
    }

    let mut rotation_started = cursor.rotation_started_unix_ns;
    let mut start = match &cursor.last_object_key {
        None => 0,
        // First index whose key sorts strictly after the resume point.
        Some(last) => corpus.partition_point(|target| &target.object_key <= last),
    };
    if start >= corpus.len() {
        // The resume point sits past the current tail (every trailing object
        // was swept since the last tick): wrap into a fresh rotation.
        start = 0;
        rotation_started = now_ns;
    }

    let mut scrub_keys = Vec::new();
    let mut bytes = 0u64;
    let mut index = start;
    while index < corpus.len() {
        let target = &corpus[index];
        if !scrub_keys.is_empty() {
            let over_budget = match budget {
                ScrubBudget::MaxObjects(max) => scrub_keys.len() as u64 >= max,
                ScrubBudget::MaxBytes(max) => bytes.saturating_add(target.object_size) > max,
            };
            if over_budget {
                break;
            }
        }
        scrub_keys.push(target.object_key.clone());
        bytes = bytes.saturating_add(target.object_size);
        index += 1;
    }

    let rotation_complete = index >= corpus.len();
    let next_cursor = if rotation_complete {
        ScrubCursor {
            last_object_key: None,
            rotation_started_unix_ns: now_ns,
            ..cursor.clone()
        }
    } else {
        ScrubCursor {
            last_object_key: Some(corpus[index - 1].object_key.clone()),
            rotation_started_unix_ns: rotation_started,
            ..cursor.clone()
        }
    };

    ScrubSlice {
        scrub_keys,
        next_cursor,
        rotation_complete,
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

    fn corpus(n: usize) -> Vec<ScrubTarget> {
        // Keys are zero-padded so lexical order matches numeric order.
        (0..n)
            .map(|i| ScrubTarget {
                object_key: format!("obj-{i:04}"),
                object_size: 1,
            })
            .collect()
    }

    #[test]
    fn cursor_covers_the_whole_corpus_in_ceil_n_over_budget_ticks() {
        let corpus = corpus(10);
        let budget = ScrubBudget::MaxObjects(3);
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);

        let mut scrubbed: Vec<String> = Vec::new();
        let mut ticks = 0;
        let mut completed = false;
        for _ in 0..100 {
            let slice = advance_cursor(&cursor, &corpus, budget, 1_000 + ticks as i64);
            scrubbed.extend(slice.scrub_keys.iter().cloned());
            cursor = slice.next_cursor.clone();
            ticks += 1;
            if slice.rotation_complete {
                completed = true;
                break;
            }
        }

        // ceil(10 / 3) == 4 ticks.
        assert_eq!(ticks, 4);
        assert!(completed);
        // Every object visited exactly once, in order.
        let expected: Vec<String> = corpus.iter().map(|t| t.object_key.clone()).collect();
        assert_eq!(scrubbed, expected);
        // The completed rotation wraps to the start.
        assert_eq!(cursor.last_object_key, None);
    }

    #[test]
    fn cursor_always_advances_even_when_one_object_exceeds_the_byte_budget() {
        // Budget is 1 byte but every object is 100 bytes: each tick must still
        // scrub exactly one object rather than wedging.
        let corpus: Vec<ScrubTarget> = (0..3)
            .map(|i| ScrubTarget {
                object_key: format!("big-{i:02}"),
                object_size: 100,
            })
            .collect();
        let budget = ScrubBudget::MaxBytes(1);
        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);

        let mut ticks = 0;
        loop {
            let slice = advance_cursor(&cursor, &corpus, budget, 5);
            assert_eq!(
                slice.scrub_keys.len(),
                1,
                "one object per tick under a tiny byte budget"
            );
            cursor = slice.next_cursor.clone();
            ticks += 1;
            if slice.rotation_complete {
                break;
            }
            assert!(ticks < 10, "cursor failed to converge");
        }
        assert_eq!(ticks, 3);
    }

    #[test]
    fn period_sized_budget_completes_one_rotation_in_about_p_over_tick_ticks() {
        // 100 one-byte objects, P = 7 days, tick = 1 hour. per-tick byte budget
        // = ceil(100 * 3600 / 604800) = ceil(0.595) = 1 byte, so a full rotation
        // takes ~100 ticks; assert it completes within P/tick + a small slack
        // and covers everything exactly once.
        let corpus = corpus(100);
        let total_bytes: u64 = corpus.iter().map(|t| t.object_size).sum();
        let period_secs = 7 * 86_400;
        let tick_secs = 3_600;
        let budget = per_tick_byte_budget(total_bytes, period_secs, tick_secs);
        let ticks_per_period = period_secs / tick_secs; // 168

        let mut cursor = ScrubCursor::new(tenant(), Signal::Metrics, 0, 0);
        let mut scrubbed: Vec<String> = Vec::new();
        let mut ticks = 0u64;
        let mut completed = false;
        while ticks < ticks_per_period + 5 {
            let slice = advance_cursor(&cursor, &corpus, budget, 42);
            scrubbed.extend(slice.scrub_keys.iter().cloned());
            cursor = slice.next_cursor.clone();
            ticks += 1;
            if slice.rotation_complete {
                completed = true;
                break;
            }
        }

        assert!(completed, "rotation did not complete within one period");
        assert!(
            ticks <= ticks_per_period,
            "rotation took {ticks} ticks, expected <= {ticks_per_period}"
        );
        let expected: Vec<String> = corpus.iter().map(|t| t.object_key.clone()).collect();
        assert_eq!(
            scrubbed, expected,
            "every object scrubbed exactly once, in order"
        );
    }

    #[test]
    fn cursor_resumes_after_a_swept_tail() {
        // The resume key points past the current tail (its object was swept):
        // the cursor must wrap into a fresh rotation rather than stall.
        let corpus = corpus(3);
        let cursor = ScrubCursor {
            last_object_key: Some("obj-9999".to_string()),
            rotation_started_unix_ns: 1,
            ..ScrubCursor::new(tenant(), Signal::Metrics, 0, 1)
        };
        let slice = advance_cursor(&cursor, &corpus, ScrubBudget::MaxObjects(10), 500);
        assert!(slice.rotation_complete);
        assert_eq!(slice.scrub_keys.len(), 3);
        assert_eq!(slice.next_cursor.rotation_started_unix_ns, 500);
    }
}
