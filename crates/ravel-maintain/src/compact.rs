//! The per-bucket compaction driver: seal + trigger checks, then the
//! plan-build-publish pipeline.
//! Stateless and idempotent: a crashed run re-run from scratch reuses
//! content-addressed part keys and converges at the record's
//! `CreateIfAbsent`.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use ravel_object_store::ObjectStoreBackend;
use ravel_types::Signal;

use crate::bucket::Bucket;
use crate::claim_guard::{BucketClaim, Checkpoint, ClaimSkip, claim_bucket};
use crate::clock::Clock;
use crate::codec::{RsegCodec, SegmentCodec};
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::publish::{PublishOutcome, conserve_exact};
use crate::read;
use crate::read::list_bucket_with_ledger;
use crate::rewrite::{FencedRewrite, UnwritableInputs};
use crate::rlog::RlogCodec;
use crate::rspan_codec::SpanCodec;

/// The result of a `compact_bucket` call. Every variant except
/// [`CompactionOutcome::Compacted`] means the bucket was left untouched, with
/// the reason; the scan driver treats all of them except `NotSealed` as
/// "this hour is done" for cursor advancement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionOutcome {
    /// Not yet sealed: the writer interlock does not yet guarantee a complete
    /// input set. Later hours are also unsealed.
    NotSealed,
    /// A retention tombstone is present: the bucket contributes nothing and is
    /// never compacted (ADR-0019).
    Tombstoned,
    /// A compaction record already exists; nothing to do.
    AlreadyCompacted,
    /// The bucket holds a live erasure rewrite record, so producing a second
    /// record set over the same inputs would make the catalog serve both
    /// (ADR-0064 decision 3 point 5: overlap harmlessness does not hold for a
    /// rewrite, whose outputs deliberately lack records its inputs contain, so
    /// a compaction record built from those same inputs would resurrect the
    /// erased records through query-time dedup).
    RewritePresent,
    /// Fewer than `min_compaction_inputs` L0 records; not worth compacting.
    /// Also returned when compaction skips an input as unrewritable
    /// ([`CompactionInputSkipReason`]) and fewer than the minimum remain;
    /// `count` is then the number of writable inputs left, 0 when none is.
    BelowMinInputs { count: usize },
    /// Built and published (or converged / abandoned): `parts` parts written,
    /// `publish` records how the record PUT resolved.
    Compacted {
        parts: usize,
        publish: PublishOutcome,
    },
}

/// Why compaction left one input object of a bucket out of its merge (issue
/// #2554): the `reason` label of `ravel_maintain_compaction_inputs_skipped_total`.
///
/// A skipped input is left in storage as it is. The published compaction
/// record does not name it, so the catalog keeps serving it as an L0 object and
/// no sweep deletes it as superseded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactionInputSkipReason {
    /// The input carries a `stream_attrs` blob the RLOG writer refuses, because
    /// the reader's decoder cannot decode it (issue #2548). Such an object was
    /// written before the writer validated its blobs.
    UnwritableStreamAttrs,
}

impl CompactionInputSkipReason {
    pub const ALL: [CompactionInputSkipReason; 1] =
        [CompactionInputSkipReason::UnwritableStreamAttrs];

    /// The `reason` label value.
    pub fn name(self) -> &'static str {
        match self {
            CompactionInputSkipReason::UnwritableStreamAttrs => "unwritable_stream_attrs",
        }
    }
}

/// The signals whose compaction can skip an input: the two that compact
/// through RLOG (logs, and the query-audit shard of audit).
pub const COMPACTION_INPUT_SKIP_SIGNALS: [Signal; 2] = [Signal::Logs, Signal::Audit];

/// The input objects this process has skipped, and the counts behind
/// [`compaction_inputs_skipped_total`]. Process-wide, so the next compaction of
/// the same bucket leaves a remembered object out without reading it again.
#[derive(Default)]
struct SkippedInputs {
    keys: HashSet<String>,
    counts: HashMap<(Signal, CompactionInputSkipReason), u64>,
}

static SKIPPED_INPUTS: LazyLock<Mutex<SkippedInputs>> =
    LazyLock::new(|| Mutex::new(SkippedInputs::default()));

fn skipped_inputs() -> MutexGuard<'static, SkippedInputs> {
    SKIPPED_INPUTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Input objects of `signal` this process's compactions have skipped for
/// `reason` since it started, each object counted once.
pub fn compaction_inputs_skipped_total(signal: Signal, reason: CompactionInputSkipReason) -> u64 {
    skipped_inputs()
        .counts
        .get(&(signal, reason))
        .copied()
        .unwrap_or(0)
}

/// `inputs` less every object this process already skipped, in order.
fn drop_skipped_inputs(
    inputs: Vec<crate::read::InputRecord>,
) -> Result<Vec<crate::read::InputRecord>> {
    let mut kept = Vec::with_capacity(inputs.len());
    for input in inputs {
        let key = ravel_commit::keys::reconstruct_data_key(&input.record)?;
        if !skipped_inputs().keys.contains(&key) {
            kept.push(input);
        }
    }
    Ok(kept)
}

/// Record that compaction of `bucket` skipped the input object `key` for
/// `reason`, `error` being the decoder's refusal. The first time per object per
/// process it counts the skip and warns with the key and the error.
pub(crate) fn note_skipped_input(
    bucket: &Bucket,
    key: &str,
    reason: CompactionInputSkipReason,
    error: &str,
) {
    {
        let mut skipped = skipped_inputs();
        if !skipped.keys.insert(key.to_string()) {
            return;
        }
        *skipped.counts.entry((bucket.signal, reason)).or_insert(0) += 1;
    }
    tracing::warn!(
        signal = ?bucket.signal,
        shard = bucket.shard,
        ingest_hour_bucket = bucket.ingest_hour_bucket,
        object_key = key,
        reason = reason.name(),
        error,
        "compaction skipped an input object it cannot rewrite; the object stays in place, \
         unnamed by the compaction record, and still fails merged-view reads until it is \
         deleted or rewritten by hand (issue #2554)"
    );
}

/// What a coordinated compaction run did (ADR-1029 decisions 3 and 5).
///
/// The extra two variants are the ones an advisory claim adds to
/// [`CompactionOutcome`]: a run that never started because another attempt
/// holds the claim, and a run that stopped mid-merge because its claim was
/// gone. Neither is a compaction, and a caller that counts compacted buckets
/// must not count either.
#[derive(Debug)]
pub enum ClaimedCompaction {
    /// The pipeline ran: claimed, or unclaimed because coordination is off or
    /// no [`ClaimParticipant`] is installed. Identical in every respect to what
    /// [`compact_bucket`] returns.
    ///
    /// [`ClaimParticipant`]: crate::config::ClaimParticipant
    Ran(CompactionOutcome),
    /// Another attempt holds a live claim on this bucket (a compaction or an
    /// erasure rewrite), or the claim object is unreadable. Nothing was read
    /// beyond the bucket listing and the input commit records, nothing was
    /// merged, and nothing was published. [`ClaimSkip::reschedule_after_unix_ms`]
    /// is the earliest this bucket should be tried again; polling before it is
    /// exactly what the claim protocol exists to avoid.
    SkippedClaimed(ClaimSkip),
    /// The claim was lost mid-run (stolen after expiry, or the claim object is
    /// gone) and the run cancelled at `at`, publishing nothing. `outcome`
    /// is the pipeline's own result, whose `publish` is
    /// [`PublishOutcome::Abandoned`]; parts already PUT are left in place, safe
    /// for the reason that variant documents.
    Cancelled {
        at: Checkpoint,
        outcome: CompactionOutcome,
    },
}

impl ClaimedCompaction {
    /// The compaction outcome when the pipeline ran to completion, and `None`
    /// when the bucket was skipped or the run cancelled.
    pub fn ran(&self) -> Option<&CompactionOutcome> {
        match self {
            ClaimedCompaction::Ran(outcome) => Some(outcome),
            ClaimedCompaction::SkippedClaimed(_) | ClaimedCompaction::Cancelled { .. } => None,
        }
    }
}

/// Compact one sealed bucket end to end, taking no claim. Safe to call
/// concurrently with other compactors over the same bucket: the record's
/// `CreateIfAbsent` picks a single winner and losers converge. Against an
/// erasure rewrite of the same bucket its only fence is the pre-publish
/// re-list, since it takes no claim (ADR-1029, the 2026-10-03 amendment).
///
/// This entry point never claims, whatever [`CompactorConfig::coordination`]
/// says: the coordinated entry point is [`compact_bucket_claimed`], which the
/// background supervisor drives (ADR-1029 decision 5) and which `ravel-cli`'s
/// `compact-bucket` and `compact-tenant` drive too (#1034). Both run the
/// identical pipeline; a claimed run merely has a guard installed for its
/// cancellation checkpoints.
pub async fn compact_bucket(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
) -> Result<CompactionOutcome> {
    match drive(store, clock, config, bucket, Coordinate::No).await?.0 {
        ClaimedCompaction::Ran(outcome) => Ok(outcome),
        // Unreachable by construction: `Coordinate::No` never acquires a claim,
        // so no claim can be held against this run or lost under it. Typed
        // rather than panicked, per this crate's no-panic rule.
        other => Err(MaintainError::Invariant(format!(
            "an unclaimed compaction produced a claim outcome: {other:?}"
        ))),
    }
}

/// [`compact_bucket`] with the advisory claim protocol engaged (ADR-1029
/// decisions 3 to 5).
///
/// The claim is taken once the bucket has passed every gate, whatever its
/// size, and released (marked completed) when the run
/// finishes. Between those points the merge consults it at the five
/// cancellation checkpoints, so a claim lost to a steal stops the run at the
/// next quiescent point instead of paying out the whole merge.
///
/// A bucket runs UNCLAIMED, through this same pipeline, only when no
/// [`crate::config::ClaimParticipant`] is installed or
/// [`CompactorConfig::coordination`] is off. A participating run claims every
/// bucket whatever its size: the claim fences this publish against an erasure
/// rewrite of the same bucket (ADR-1029, the 2026-10-03 amendment), and a run
/// refused it, including by an unreadable claim object, backs off.
pub async fn compact_bucket_claimed(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
) -> Result<ClaimedCompaction> {
    Ok(drive(store, clock, config, bucket, Coordinate::Yes)
        .await?
        .0)
}

/// [`compact_bucket_claimed`], additionally reporting whether checkpoint 1
/// acquired a claim and whether that acquisition was a steal (ADR-1029
/// decision 3). Used only by [`crate::retention::maintain_bucket_with_reach`],
/// which folds the acquisition into the run's [`crate::scan::MaintainReport`]
/// counters (#1035); `compact_bucket_claimed`'s own signature and every
/// existing caller are unchanged.
pub(crate) async fn compact_bucket_claimed_with_acquisition(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
) -> Result<(ClaimedCompaction, Option<ClaimAcquisition>)> {
    drive(store, clock, config, bucket, Coordinate::Yes).await
}

/// Whether this run may take a claim at all. `No` is the legacy
/// [`compact_bucket`] entry point; `Yes` still claims only when a participant
/// is installed and coordination is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Coordinate {
    Yes,
    No,
}

/// The one driver both entry points run: ledger scope, gates, claim, pipeline.
async fn drive(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    coordinate: Coordinate,
) -> Result<(ClaimedCompaction, Option<ClaimAcquisition>)> {
    // This is the run's outermost driver, so it opens the request ledger's
    // scope here, BEFORE the bucket LIST below: that LIST is the run's first
    // store request and belongs in the run's own report, and the rewrite
    // primitive it later dispatches to therefore must not reset (ADR-0996 task
    // 996-8). The scope is closed on every exit path, including the gates that
    // return before any rewrite runs.
    let scope = config.request_ledger.as_ref().map(|l| {
        l.reset_for_run();
        l.run_scope_guard()
    });
    let outcome = compact_bucket_scoped(store, clock, config, bucket, coordinate).await;
    // As the opener, this driver emits the run's report on EVERY outcome: the
    // gates that return before any rewrite (NotSealed, AlreadyCompacted,
    // RewritePresent, Tombstoned, BelowMinInputs) still paid their LIST and
    // report it
    // (ADR-0996 task 996-8). The guard closes the scope even on cancellation.
    crate::rewrite::emit_request_report(config, bucket, outcome.is_ok());
    if let Some(scope) = scope {
        scope.close();
    }
    outcome
}

/// [`compact_bucket`]'s body, with the request ledger's run scope already
/// opened and guaranteed to be closed by its caller.
async fn compact_bucket_scoped(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    coordinate: Coordinate,
) -> Result<(ClaimedCompaction, Option<ClaimAcquisition>)> {
    crate::rlog::check_rlog_zstd_level(config, bucket)?;
    let start_ns = clock.now_ns();
    if !bucket.is_sealed(start_ns, config) {
        return Ok((ClaimedCompaction::Ran(CompactionOutcome::NotSealed), None));
    }

    let listing = list_bucket_with_ledger(store, bucket, config.request_ledger.as_ref()).await?;
    if let Some(refused) = listing_gate(&listing) {
        return Ok((ClaimedCompaction::Ran(refused), None));
    }
    if listing.commit_keys.len() < config.min_compaction_inputs {
        return Ok((
            ClaimedCompaction::Ran(CompactionOutcome::BelowMinInputs {
                count: listing.commit_keys.len(),
            }),
            None,
        ));
    }

    // The input set, read once for the whole run. The claim is taken after
    // this read and before every read whose cost scales with the bucket
    // (catalogs, blocks) and every PUT.
    let inputs = crate::read::load_inputs_with_ledger(
        store,
        bucket,
        &listing.commit_keys,
        config.input_read_concurrency,
        config.request_ledger.as_ref(),
    )
    .await?;

    // An object an earlier run of this process skipped is left out before the
    // claim and before its catalog is read again, and the minimum is applied
    // to what remains.
    let loaded = inputs.len();
    let inputs = drop_skipped_inputs(inputs)?;
    if inputs.len() != loaded && (inputs.is_empty() || inputs.len() < config.min_compaction_inputs)
    {
        return Ok((
            ClaimedCompaction::Ran(CompactionOutcome::BelowMinInputs {
                count: inputs.len(),
            }),
            None,
        ));
    }

    // Cancellation checkpoint 1 (ADR-1029 decision 3), which is also where the
    // claim is acquired. A participating run claims every bucket whatever its
    // size, because the claim fences this publish against an erasure rewrite of
    // the same bucket (the 2026-10-03 amendment); one refused the claim backs
    // off without building anything.
    let (guard, acquisition) = match acquire_claim(store, config, bucket, coordinate).await? {
        BucketClaim::Skipped(skip) => return Ok((ClaimedCompaction::SkippedClaimed(skip), None)),
        BucketClaim::NotParticipating => (None, None),
        BucketClaim::Held { guard, stolen } => (Some(guard), Some(ClaimAcquisition { stolen })),
    };
    // The guard rides on this run's OWN config clone, never on the caller's:
    // two buckets compacted concurrently under one base config each get their
    // own, so a checkpoint can only ever renew its own bucket's claim.
    let run_config = match guard.as_ref() {
        Some(guard) => CompactorConfig {
            claim_guard: Some(guard.clone()),
            ..config.clone()
        },
        None => config.clone(),
    };

    // Everything up to here is signal-generic (seal, tombstone, already-done,
    // and input-count gates on the bucket listing). The plan-build step is the
    // only signal-specific part: dispatch it to the codec for this bucket's
    // signal and run the identical shared pipeline (canonical ordering,
    // input_set_hash, publish) around it (ADR-0032).
    let planned = &listing;
    let outcome = match bucket.signal {
        Signal::Metrics => {
            run_pipeline::<RsegCodec>(store, clock, &run_config, bucket, inputs, start_ns, planned)
                .await
        }
        Signal::Logs => {
            run_pipeline::<RlogCodec>(store, clock, &run_config, bucket, inputs, start_ns, planned)
                .await
        }
        Signal::Spans => {
            run_pipeline::<SpanCodec>(store, clock, &run_config, bucket, inputs, start_ns, planned)
                .await
        }
        // Query-audit records ride RLOG (see `query_audit`), so they compact
        // through the same RLOG codec as logs -- the machinery is reused, only
        // the signal and shard are new. The legal-hold shard
        // (`legal_hold::AUDIT_HOLD_SHARD` = 0) is deliberately excluded: the
        // legal-hold fold (`legal_hold::load_hold_records`) reads L0 commit
        // records only and ignores compaction records, so compacting its records
        // into L1 and letting the superseded sweep delete the L0 originals would
        // silently drop every hold from the fold. Only the query-audit shard
        // gains compaction here.
        Signal::Audit if bucket.shard == crate::query_audit::QUERY_AUDIT_SHARD => {
            run_pipeline::<RlogCodec>(store, clock, &run_config, bucket, inputs, start_ns, planned)
                .await
        }
        Signal::Audit => Err(MaintainError::Invariant(format!(
            "audit compaction is only implemented for the query-audit shard \
             (QUERY_AUDIT_SHARD = {}), never the legal-hold shard; got shard {}",
            crate::query_audit::QUERY_AUDIT_SHARD,
            bucket.shard
        ))),
        other => Err(MaintainError::Invariant(format!(
            "compaction is not implemented for signal {other:?}"
        ))),
    }?;

    // A run that lost its claim cancelled at a checkpoint and published
    // nothing; only a run that still holds its claim marks it completed
    // (ADR-1029 decision 1 step 6), including one the pre-publish re-list
    // stopped. The marker is forensic: the published record is the only
    // completion marker that decides anything, so a failure to write the
    // marker is logged and the pipeline's outcome stands. The claim then ages
    // out under its lease.
    if let Some(guard) = guard {
        if let Some(at) = guard.cancelled_at().await {
            return Ok((ClaimedCompaction::Cancelled { at, outcome }, acquisition));
        }
        if let Err(err) = guard.complete(store).await {
            tracing::warn!(
                signal = ?bucket.signal,
                shard = bucket.shard,
                ingest_hour_bucket = bucket.ingest_hour_bucket,
                work_id = %guard.work_id_hex(),
                error = %err,
                "compaction finished, but marking its claim completed failed; \
                 the claim ages out under its lease (ADR-1029)"
            );
        }
    }
    Ok((ClaimedCompaction::Ran(outcome), acquisition))
}

/// Whether checkpoint 1's claim acquisition was a fresh claim or a steal
/// from an expired holder (ADR-1029 decision 3). Reported out of
/// [`compact_bucket_claimed_with_acquisition`] for the caller's metrics;
/// [`ClaimedCompaction`]'s own shape carries no acquisition detail. `pub`,
/// not `pub(crate)`, because [`crate::retention::maintain_bucket_with_reach`]
/// is itself re-exported and returns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimAcquisition {
    pub stolen: bool,
}

/// Take this bucket's claim, if this run takes claims at all (ADR-1029
/// decision 5 and the 2026-10-03 amendment).
///
/// The legacy [`compact_bucket`] entry point takes none, and neither does a
/// caller with no participant installed or coordination off; those runs are
/// fenced against an erasure rewrite by the pre-publish re-list alone. Every
/// other run claims, whatever the bucket's size.
async fn acquire_claim(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    coordinate: Coordinate,
) -> Result<BucketClaim> {
    if coordinate == Coordinate::No {
        return Ok(BucketClaim::NotParticipating);
    }
    claim_bucket(store, config, bucket, "compaction").await
}

/// The listing gates a compaction applies to the listing it plans from, and
/// again to its pre-publish re-list: `Some` is the reason the bucket is not
/// compacted.
fn listing_gate(listing: &BucketListing) -> Option<CompactionOutcome> {
    if listing.tombstone_key.is_some() {
        return Some(CompactionOutcome::Tombstoned);
    }
    if !listing.compaction_record_keys.is_empty() {
        return Some(CompactionOutcome::AlreadyCompacted);
    }
    // One bucket serves one record set. A live rewrite record already covers
    // these inputs with records deliberately removed from its outputs, and a
    // compaction record over the same inputs is not overlap-harmless against
    // it: a snapshot including both resurrects the erased records
    // (ADR-0064 decision 3 point 5). Refuse rather than publish the second set.
    if !listing.rewrite_record_keys.is_empty() {
        return Some(CompactionOutcome::RewritePresent);
    }
    None
}

/// The signal-generic plan-build-publish pipeline, parameterized over the
/// per-signal [`SegmentCodec`]. Compaction is the exact-conservation case of
/// the shared rewrite primitive ([`crate::rewrite::rewrite_and_publish`],
/// ADR-0066 decision 5): it loads and canonically orders the inputs, derives
/// the `input_set_hash`, decodes each input's catalog metadata through the
/// codec, streams the merge into size-capped parts through the codec, and
/// publishes the record with [`conserve_exact`]. Only the two `C::` calls know
/// the on-object format; everything else is identical for every signal.
///
/// `planned` is the listing the run planned from. When the pre-publish re-list
/// finds a different record set, the run publishes nothing and reports the
/// gate the new listing fails (an erasure rewrite record that landed meanwhile
/// is [`CompactionOutcome::RewritePresent`]), or an abandoned publish when it
/// fails none.
///
/// An input the codec cannot rewrite is left out of the merge and of the
/// record ([`UnwritableInputs::Skip`]); when fewer than `min_compaction_inputs`
/// inputs are left the bucket reports [`CompactionOutcome::BelowMinInputs`]
/// with the count left.
async fn run_pipeline<C: SegmentCodec>(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    inputs: Vec<crate::read::InputRecord>,
    start_ns: i64,
    planned: &BucketListing,
) -> Result<CompactionOutcome> {
    let fenced = crate::rewrite::rewrite_and_publish_loaded::<C>(
        store,
        clock,
        config,
        bucket,
        inputs,
        conserve_exact(),
        start_ns,
        Some(planned),
        UnwritableInputs::Skip,
    )
    .await?;

    Ok(match fenced {
        FencedRewrite::Ran(outcome) => CompactionOutcome::Compacted {
            parts: outcome.parts,
            publish: outcome.publish,
        },
        FencedRewrite::TooFewWritableInputs { remaining } => {
            CompactionOutcome::BelowMinInputs { count: remaining }
        }
        FencedRewrite::RecordSetChanged(now) => {
            listing_gate(&now).unwrap_or(CompactionOutcome::Compacted {
                parts: 0,
                publish: PublishOutcome::Abandoned,
            })
        }
    })
}

// Re-export the input-listing type so callers (and tests) can inspect a
// bucket without reaching into the module.
pub use read::BucketListing;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    //! Exemplar carry-through for L0-to-L1 metric compaction (ADR-0047
    //! decision 3). Seeds real L0 RSEG v6 objects through the
    //! ingest flush writer, runs the whole `compact_bucket` pipeline over a
    //! `MemoryStore`, and reads the L1 parts' EXEMPLARS sections back with
    //! `ravel-segment`'s own decoder.
    //!
    //! The invariant under test is ADR-0018's overlap harmlessness applied to
    //! exemplars: an L1 object's exemplars are the exact multiset of its
    //! inputs', with only `series_index` remapped. Every assertion here is on
    //! counts, not sets, so any deduplication fails the test.

    use std::collections::BTreeMap;

    use bytes::Bytes;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_commit::{erasure, keys, signal};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::{
        CompactionInputIdentity, CompactionPart, CompactionRecord, RewriteDrop, RewriteRecord,
    };
    use ravel_segment::{
        ExemplarInput, IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInputV3,
        SeriesValues, VERSION_V7,
    };
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantHash, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::{CompactorConfig, FixedClock};

    const TENANT: &str = "acme";
    const SHARD: u32 = 7;
    const HOUR: u32 = 495_000;
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;
    /// LABEL_DICT and EXEMPLARS section kinds (docs/segment-format.md); named
    /// here as the format contract, as `read.rs` does.
    const LABEL_DICT: u32 = 1;
    const EXEMPLARS: u32 = 10;

    fn tenant_hash() -> TenantHash {
        TenantId::new(TENANT).hash()
    }

    fn bucket() -> Bucket {
        Bucket::new(tenant_hash(), Signal::Metrics, SHARD, HOUR)
    }

    /// Past the seal margin for [`HOUR`] under default config.
    fn sealed_now_ns() -> i64 {
        (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
    }

    fn labels(metric: &str) -> LabelSet {
        LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels")
    }

    fn series_id(metric: &str) -> SeriesId {
        SeriesId::compute(&TenantId::new(TENANT), metric, &labels(metric)).expect("series id")
    }

    fn series(metric: &str, samples: &[(i64, f64)]) -> SeriesInputV3 {
        SeriesInputV3 {
            series_id: series_id(metric),
            labels: labels(metric),
            values: SeriesValues::Scalar(
                samples
                    .iter()
                    .map(|(ts_ns, value)| Sample {
                        ts_ns: *ts_ns,
                        value: *value,
                    })
                    .collect(),
            ),
        }
    }

    /// One exemplar for `metric`, tagged so it is distinguishable after the
    /// round trip: `tag` fills the trace and span ids and rides in an
    /// attribute value.
    fn exemplar(metric: &str, ts_ns: i64, value: f64, tag: u8) -> ExemplarInput {
        ExemplarInput {
            series_id: series_id(metric),
            ts_ns,
            value,
            trace_id: [tag; 16],
            span_id: [tag; 8],
            attrs: vec![("peer".to_string(), format!("svc-{tag}"))],
        }
    }

    /// Seed one L0 input (data object + commit record) through the production
    /// ingest flush writer, so the seeded object's EXEMPLARS section is
    /// byte-for-byte what a real flush would have written.
    async fn seed(
        store: &dyn ObjectStoreBackend,
        seq: u64,
        series: Vec<SeriesInputV3>,
        exemplars: Vec<ExemplarInput>,
    ) -> Bytes {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(u128::from(seq));
        let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        let identity = SegmentIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.to_string(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
        };
        let written =
            SegmentWriter::write_histograms_with_exemplars(series, identity, bounds, exemplars)
                .expect("write L0");
        let content_hash = written.summary.blake3;
        let data_key = keys::data_key(
            &th,
            Signal::Metrics,
            SHARD,
            writer_id,
            EPOCH,
            seq,
            &content_hash,
        )
        .expect("data key");
        store
            .put(&data_key, written.bytes.clone(), PutOptions::default())
            .await
            .expect("put data object");

        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Metrics,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: seq,
            object_size: written.bytes.len() as u64,
            content_hash,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: u32::from(VERSION_V7),
            created_unix_ns: created,
            ingest_hour_bucket: HOUR,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
        written.bytes
    }

    /// The single compaction record in the bucket plus every L1 part's bytes,
    /// in record (ascending series-range) order.
    async fn read_output(store: &dyn ObjectStoreBackend) -> (CompactionRecord, Vec<Bytes>) {
        use prost::Message;
        let b = bucket();
        let prefix =
            keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
                .expect("prefix");
        let metas = list_all(store, &prefix).await.expect("list");
        let mut rec_keys: Vec<String> = metas
            .into_iter()
            .map(|m| m.key)
            .filter(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .collect();
        rec_keys.sort();
        assert_eq!(rec_keys.len(), 1, "expected exactly one compaction record");
        let got = store
            .get(&rec_keys[0], GetRange::Full)
            .await
            .expect("get record");
        let record = CompactionRecord::decode(got.data.as_ref()).expect("decode record");
        let mut parts = Vec::new();
        for p in &record.parts {
            let key = keys::reconstruct_l1_part_key(&record, p).expect("part key");
            parts.push(
                store
                    .get(&key, GetRange::Full)
                    .await
                    .expect("get part")
                    .data,
            );
        }
        (record, parts)
    }

    /// One exemplar in the canonical, order-independent form the multiset
    /// assertions compare: the series it names (not its index, which is
    /// object-relative), the timestamp, the value's bit pattern (never `==`:
    /// NaN and -0.0 are significant), both ids, and its attributes.
    type Canon = ([u8; 16], i64, u64, [u8; 16], [u8; 8], Vec<(String, String)>);

    /// Whether an object carries an EXEMPLARS section at all, plus every
    /// record in it canonicalized against the object's own SERIES_IDS
    /// ordering, and the raw `(series_index, ts_ns)` sort keys in stored
    /// order.
    fn read_exemplars(obj: &[u8]) -> (bool, Vec<Canon>, Vec<(u64, i64)>) {
        let limits = ReaderLimits::default();
        let loc = ravel_segment::open_from_full(obj, limits).expect("open object");
        let footer = &loc.footer;
        if !footer.sections.iter().any(|s| s.kind == EXEMPLARS) {
            return (false, Vec::new(), Vec::new());
        }
        let section = |kind: u32| -> &[u8] {
            let s = footer
                .sections
                .iter()
                .find(|s| s.kind == kind)
                .unwrap_or_else(|| panic!("missing section kind {kind}"));
            &obj[s.offset as usize..(s.offset + s.len) as usize]
        };
        let records = ravel_segment::decode_exemplars_section(
            footer,
            section(LABEL_DICT),
            section(EXEMPLARS),
            limits,
        )
        .expect("decode EXEMPLARS");
        // SERIES_IDS order, which is what `series_index` indexes into.
        let entries = ravel_segment::decode_catalog_v5(footer, obj, limits).expect("catalog");
        let ids: Vec<[u8; 16]> = entries.iter().map(|e| e.entry.series_id.0).collect();

        let keys = records
            .iter()
            .map(|r| (r.series_index, r.ts_ns))
            .collect::<Vec<_>>();
        let canon = records
            .iter()
            .map(|r| {
                let id = ids[usize::try_from(r.series_index).expect("index fits")];
                (
                    id,
                    r.ts_ns,
                    r.value.to_bits(),
                    r.trace_id,
                    r.span_id,
                    r.attrs.clone(),
                )
            })
            .collect();
        (true, canon, keys)
    }

    /// The exemplar multiset of a whole set of objects (inputs or L1 parts),
    /// sorted so two multisets compare directly. A `Vec`, never a set: a
    /// deduplicating compactor must fail these assertions.
    fn canon_multiset(objects: &[Bytes]) -> Vec<Canon> {
        let mut all: Vec<Canon> = Vec::new();
        for obj in objects {
            let (_, canon, _) = read_exemplars(obj);
            all.extend(canon);
        }
        all.sort();
        all
    }

    /// The L0 identity of the input [`seed`] writes for `seq`.
    fn input_identity(seq: u64) -> CompactionInputIdentity {
        CompactionInputIdentity {
            writer_id: Uuid::from_u128(u128::from(seq)).to_string(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        }
    }

    /// One rewrite output part covering `[min_ts, max_ts]`. `tag` fills the
    /// content hash, so the reconstructed part key is distinct per part.
    fn rewrite_part(tag: u8, min_ts: i64, max_ts: i64) -> CompactionPart {
        CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xff; 16],
            content_hash: vec![tag; 32],
            object_size: 4096,
            sample_count: 1,
            series_count: 1,
            run_count: 1,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            segment_format_version: VERSION_V7 as u32,
            declared_column_stats: Vec::new(),
        }
    }

    /// Publish a `RewriteRecord` over `inputs` into the bucket, plus an object
    /// at each of its parts' reconstructed keys, exactly as the erasure pass
    /// publishes one. Returns the record key.
    async fn put_rewrite_record(
        store: &dyn ObjectStoreBackend,
        inputs: Vec<CompactionInputIdentity>,
        parts: Vec<CompactionPart>,
        request_id: Uuid,
        created_unix_ns: i64,
    ) -> String {
        use prost::Message;
        let b = bucket();
        let request_ids = vec![request_id.to_string()];
        let input_set_hash = erasure::compute_rewrite_input_set_hash(&inputs, None, &request_ids);
        let record = RewriteRecord {
            format_version: 1,
            tenant_hash: b.tenant_hash.0.to_vec(),
            signal: signal::to_proto(b.signal) as i32,
            shard: b.shard,
            ingest_hour_bucket: b.ingest_hour_bucket,
            inputs,
            input_set_hash: input_set_hash.to_vec(),
            parts,
            drops: request_ids
                .iter()
                .map(|id| RewriteDrop {
                    request_id: id.clone(),
                    dropped_count: 1,
                })
                .collect(),
            created_unix_ns,
            superseded_record_key: String::new(),
        };
        for p in &record.parts {
            let part_key =
                keys::reconstruct_rewrite_part_key(&record, p).expect("rewrite part key");
            store
                .put(
                    &part_key,
                    Bytes::from_static(b"rw-part"),
                    PutOptions::default(),
                )
                .await
                .expect("put rewrite part object");
        }
        let key = keys::rewrite_record_key_for(&record).expect("rewrite record key");
        store
            .put(
                &key,
                record.encode_to_vec().into(),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put rewrite record");
        key
    }

    /// Compaction refuses a bucket that already holds a live erasure rewrite
    /// record. A rewrite's outputs deliberately lack records its inputs
    /// contain, so overlap harmlessness does not hold for it (ADR-0064
    /// decision 3 point 5): a compaction record built from those same L0
    /// inputs would leave the catalog serving two record sets over one bucket,
    /// and a snapshot including both resurrects the erased records through
    /// query-time dedup.
    ///
    /// The guarded production line is the `RewritePresent` early return in
    /// `compact_bucket_scoped`. With that return removed the outcome is
    /// `Compacted { .. }` and the bucket ends the call holding one compaction
    /// record next to the rewrite record.
    #[tokio::test]
    async fn compaction_refuses_a_bucket_holding_a_live_rewrite_record() {
        let store = MemoryStore::new();
        let base = i64::from(HOUR) * NS_PER_HOUR;
        seed(
            &store,
            1,
            vec![series("alpha", &[(base + 1_000, 1.0)])],
            Vec::new(),
        )
        .await;
        seed(
            &store,
            2,
            vec![series("beta", &[(base + 2_000, 2.0)])],
            Vec::new(),
        )
        .await;
        // One erasure rewrite already supersedes both L0 inputs.
        put_rewrite_record(
            &store,
            vec![input_identity(1), input_identity(2)],
            vec![rewrite_part(0x11, base + 1_000, base + 2_000)],
            Uuid::from_u128(0x0E45),
            base + 3_000,
        )
        .await;

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");

        assert_eq!(
            outcome,
            CompactionOutcome::RewritePresent,
            "a live rewrite record refuses compaction"
        );
        assert!(
            !matches!(outcome, CompactionOutcome::Compacted { .. }),
            "nothing was compacted"
        );
        let listing = read::list_bucket(&store, &bucket()).await.expect("list");
        assert_eq!(
            listing.compaction_record_keys.len(),
            0,
            "no second record set was published over the rewritten inputs"
        );
        assert_eq!(listing.rewrite_record_keys.len(), 1);
        assert_eq!(
            listing.commit_keys.len(),
            2,
            "both L0 inputs stay live behind the rewrite record"
        );
    }

    /// The acceptance test: the exemplars in the L1 output are
    /// exactly the union (as a multiset) of the inputs', with series indices
    /// remapped into each part's own SERIES_IDS ordering. Series ids are
    /// chosen so the two inputs' orderings differ from the output's, so a
    /// missing remap would surface as a wrong series id rather than passing by
    /// accident.
    #[tokio::test]
    async fn exemplars_survive_compaction_verbatim() {
        let store = MemoryStore::new();
        // Input 1 carries alpha and gamma; input 2 carries beta and gamma. The
        // merged output carries all three, so every input's local series
        // indices differ from the output's.
        let a = seed(
            &store,
            1,
            vec![
                series("alpha", &[(10, 1.0)]),
                series("gamma", &[(10, 3.0), (30, 3.3)]),
            ],
            vec![exemplar("alpha", 10, 1.0, 1), exemplar("gamma", 30, 3.3, 2)],
        )
        .await;
        let b = seed(
            &store,
            2,
            vec![series("beta", &[(20, 2.0)]), series("gamma", &[(40, 3.4)])],
            vec![exemplar("beta", 20, 2.0, 3), exemplar("gamma", 40, 3.4, 4)],
        )
        .await;

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

        let (record, parts) = read_output(&store).await;
        assert_eq!(record.level, 1);
        assert_eq!(parts.len(), 1, "small corpus fits one part");

        let expected = canon_multiset(&[a, b]);
        assert_eq!(expected.len(), 4, "the inputs carry four exemplars");
        assert_eq!(
            canon_multiset(&parts),
            expected,
            "L1 exemplars must be the exact multiset of the inputs'"
        );

        // The remap is real: gamma's two exemplars name the same series id,
        // which in the output is a different index than in either input.
        let (_, canon, keys) = read_exemplars(&parts[0]);
        let gamma = series_id("gamma").0;
        assert_eq!(
            canon.iter().filter(|c| c.0 == gamma).count(),
            2,
            "both gamma exemplars survive, from two different inputs"
        );
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "records ascending by (series_index, ts_ns)");
    }

    /// Two inputs each carrying an exemplar for the same `(series, ts)` with
    /// different trace ids: both reach the output. This is what a retried
    /// write produces (the ingest cap is flush-scoped), and the ADR-0047
    /// 2026-08-03 amendment made equal sort keys legal precisely so the
    /// compactor can encode both. Deduplicating here would make an L1 object
    /// something other than the multiset of its inputs.
    #[tokio::test]
    async fn two_exemplars_sharing_a_series_and_timestamp_both_survive() {
        let store = MemoryStore::new();
        seed(
            &store,
            1,
            vec![series("alpha", &[(10, 1.0)])],
            vec![exemplar("alpha", 10, 1.0, 0xAA)],
        )
        .await;
        seed(
            &store,
            2,
            vec![series("alpha", &[(10, 1.0)])],
            vec![exemplar("alpha", 10, 1.0, 0xBB)],
        )
        .await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_, parts) = read_output(&store).await;

        let (present, canon, keys) = read_exemplars(&parts[0]);
        assert!(present);
        assert_eq!(canon.len(), 2, "both records survive, not one");
        assert_eq!(
            keys,
            vec![(0, 10), (0, 10)],
            "equal sort keys are legal and are not collapsed"
        );
        let mut traces: Vec<[u8; 16]> = canon.iter().map(|c| c.3).collect();
        traces.sort();
        assert_eq!(traces, vec![[0xAA; 16], [0xBB; 16]]);
    }

    /// Inputs with no exemplars produce parts with no EXEMPLARS section:
    /// absence, not a zero-count section, is how "no exemplars" is represented
    /// (ADR-0047 decision 1).
    #[tokio::test]
    async fn inputs_without_exemplars_produce_a_part_without_the_section() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0)])], Vec::new()).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])], Vec::new()).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_, parts) = read_output(&store).await;
        let (present, canon, _) = read_exemplars(&parts[0]);
        assert!(!present, "an L1 part with no exemplars emits no section");
        assert!(canon.is_empty());
    }

    /// A part cap small enough to split forces exemplars to follow their own
    /// series into whichever part carries it: no part may hold an exemplar for
    /// a series it does not carry (that is a writer error, not a silent drop),
    /// and across parts the multiset is still conserved.
    #[tokio::test]
    async fn part_splitting_keeps_each_exemplar_with_its_series() {
        let store = MemoryStore::new();
        let mut seeded = Vec::new();
        for seq in 1..=2u64 {
            let mut batch = Vec::new();
            let mut exemplars = Vec::new();
            for n in 0..8u8 {
                let metric = format!("m{n}");
                batch.push(series(&metric, &[(10 * i64::from(n) + 1, f64::from(n))]));
                exemplars.push(exemplar(
                    &metric,
                    10 * i64::from(n) + 1,
                    f64::from(n),
                    n * 16 + seq as u8,
                ));
            }
            seeded.push(seed(&store, seq, batch, exemplars).await);
        }

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig {
            max_l1_part_bytes: 128,
            ..CompactorConfig::default()
        };
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (_, parts) = read_output(&store).await;
        assert!(parts.len() >= 2, "a tiny cap must split into parts");

        assert_eq!(
            canon_multiset(&parts),
            canon_multiset(&seeded),
            "the exemplar multiset is conserved across the split"
        );

        // Each part's records name only series that part carries, and stay
        // ascending after the per-part remap.
        for part in &parts {
            let limits = ReaderLimits::default();
            let loc = ravel_segment::open_from_full(part, limits).expect("open part");
            let entries =
                ravel_segment::decode_catalog_v5(&loc.footer, part, limits).expect("catalog");
            let ids: BTreeMap<[u8; 16], ()> =
                entries.iter().map(|e| (e.entry.series_id.0, ())).collect();
            let (_, canon, keys) = read_exemplars(part);
            for c in &canon {
                assert!(
                    ids.contains_key(&c.0),
                    "a part must not carry an exemplar for a series it lacks"
                );
            }
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(keys, sorted, "records ascending by (series_index, ts_ns)");
        }
    }

    /// Exemplars on the sparse-catalog decode path.
    ///
    /// `read.rs` routes an input whose object carries the sparse sections
    /// (SERIES_IDX + SERIES_META_CHUNKS) to the whole-object `decode_catalog_v5`
    /// via `catalog_is_sparse` -- presence-signalled since #311, not the old
    /// `series_count >= V5_SPARSE_THRESHOLD` test. A 4096+-series object takes
    /// the sparse form, and every other test here (and `ravel-ingest`'s
    /// `exemplar_flush.rs`) stays far below that, so the chunked decoder is the
    /// only one exercised for exemplars. Exemplar
    /// `series_index` resolution depends on the decoder returning entries in
    /// SERIES_IDS order; if a future change to the sparse decoder broke that,
    /// every exemplar here would resolve to the wrong series while the smaller
    /// tests still passed. This drives two inputs that each cross the threshold
    /// (4096 distinct series, one exemplar apiece) and asserts the L1 output's
    /// exemplars are the exact multiset of the inputs', which pins the resolved
    /// series ids on both the sparse input decode and the merge remap.
    #[tokio::test]
    async fn sparse_catalog_path_exemplars_resolve_to_same_series() {
        // At the threshold exactly, so each input's own catalog decodes through
        // the sparse (whole-object) path, not the chunked one.
        const N: usize = ravel_segment::V5_SPARSE_THRESHOLD as usize;

        let start = std::time::Instant::now();
        let store = MemoryStore::new();

        // Two inputs carrying disjoint series ranges, so the merged output's
        // SERIES_IDS ordering differs from either input's local ordering and the
        // per-part remap is non-trivial (not the identity).
        let mut seeded = Vec::new();
        for seq in 1..=2u64 {
            let base = (seq as usize - 1) * N;
            let mut batch = Vec::with_capacity(N);
            let mut exemplars = Vec::with_capacity(N);
            for i in 0..N {
                let n = base + i;
                let metric = format!("m{n:05}");
                let ts = n as i64 + 1;
                batch.push(series(&metric, &[(ts, n as f64)]));
                exemplars.push(exemplar(&metric, ts, n as f64, (n % 251) as u8));
            }
            seeded.push(seed(&store, seq, batch, exemplars).await);
        }

        // Each seeded input really is on the sparse branch.
        for obj in &seeded {
            let loc = ravel_segment::open_from_full(obj, ReaderLimits::default()).expect("open");
            assert!(
                loc.footer.series_count >= ravel_segment::V5_SPARSE_THRESHOLD,
                "input must cross the sparse threshold to exercise the sparse decode"
            );
        }

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_, parts) = read_output(&store).await;

        let expected = canon_multiset(&seeded);
        assert_eq!(
            expected.len(),
            2 * N,
            "inputs carry one exemplar per series"
        );
        assert_eq!(
            canon_multiset(&parts),
            expected,
            "L1 exemplars from a sparse-catalog input must resolve to the same \
             series ids as the inputs'"
        );

        eprintln!(
            "sparse_catalog_path_exemplars_resolve_to_same_series: {} series, {} exemplars, {:?}",
            2 * N,
            2 * N,
            start.elapsed()
        );
    }
}
