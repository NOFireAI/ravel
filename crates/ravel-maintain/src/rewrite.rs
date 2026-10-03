//! The shared segment-rewrite primitive (ADR-0066 decision 5).
//!
//! A rewrite reads a named set of live objects, decodes them through this
//! crate's existing per-signal reader machinery, re-encodes them through the
//! current writer, and publishes the result through the same
//! `CreateIfAbsent`-then-supersede path compaction uses -- but with the
//! record-count conservation gate taken as a parameter instead of hardcoded to
//! exact match. That one generalization is what lets a single primitive serve
//! two epics:
//!
//! - EM (this crate's caller below): format migration. An old-format
//!   object is decoded and re-encoded into the current format, conserving the
//!   record count exactly ([`crate::publish::conserve_exact`]). Nothing is
//!   dropped; the bytes change format, not content.
//! - EJ (a later task, not built here): selective erasure. The same read
//!   -> re-encode -> publish shape, but the writer drops the erased subject's
//!   records and the predicate is "input equals output plus the erased count"
//!   (ADR-0064 decision 3 point 4). EJ supplies that predicate; it does not
//!   re-implement the primitive.
//!
//! ## The N-1 reader
//!
//! The primitive does not contain a decoder; it calls
//! [`SegmentCodec::load_input_catalog`] and [`SegmentCodec::build_parts`],
//! exactly as [`crate::compact::compact_bucket`] does. For RLOG and RSPAN
//! those genuinely decode and re-encode on every merge, so once
//! ravel-logseg/ravel-rspan gain an N-1 reader (ADR-0066 decision 1, at first
//! public release), the codec's read path accepts the older version and this
//! primitive migrates real old-version objects with no change here.
//!
//! RSEG is now the same shape: since ADR-0066 decision 5, its `build_parts`
//! (`build.rs`) still copies a *current*-version input's pages verbatim, but
//! decodes and re-encodes an input recorded *below* the current output version
//! at the current version before the shared merge/plan step, so an RSEG
//! rewrite is a real format migration, not just a same-version round trip.
//! [`RsegCodec::validate_rewrite_inputs`] no longer refuses an older
//! input; it now fails closed only on an input recorded *newer* than the
//! current output version (ADR-0066 decision 2), which a forward migration
//! cannot write. Whether an older object's bytes are actually decodable is the
//! ravel-segment reader's `SUPPORTED_VERSIONS` window to enforce at open time;
//! today that window is a single version (ADR-0027), so the decode-and-re-encode
//! path is exercised by tests and dry-runs rather than by carrying real dual
//! versions in anger (ADR-0066 Consequences), and it converges an
//! older-recorded input to the current version the day the window opens.
//!
//! ## Durability is unchanged
//!
//! The primitive generalizes the read side and the conservation check, never
//! the publish/durability side. [`crate::build::build_parts`] still PUTs every
//! part `CreateIfAbsent` under a content-addressed key, and
//! [`crate::publish::publish_record_with_conservation`] still runs the
//! abandonment deadline, the single-winner record PUT, and the racing-loser
//! convergence/repair unchanged. A crashed rewrite re-run from scratch rebuilds
//! the identical content-addressed parts and converges at the record's
//! `CreateIfAbsent`, the same statelessness compaction has.
//!
//! ## Force 2: re-encoding a compaction record's parts
//!
//! [`reencode_compaction_parts`] is ADR-0066's force 2 (the 2026-09-28
//! amendment): a bucket whose one compaction record has parts below the
//! current segment format version gets those parts re-encoded at the current
//! version and a version 2 compaction record that supersedes the old one. It
//! sits behind [`CompactorConfig::reencode_writer_enabled`], off by default,
//! and has no production caller yet.

use std::collections::BTreeMap;

use futures::stream::{StreamExt, TryStreamExt, iter as stream_iter};
use ravel_catalog::select_authoritative_compaction_records;
use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::{CompactionPart, CompactionRecord};
use ravel_segment::{
    CompactionMetaV4, ExemplarInput, IngestBounds, ReaderLimits, RunInputV7, SegmentIdentity,
    SegmentWriter, SeriesEntryV4, SeriesInputV7, SeriesValues, ValueKind, decode_catalog_v5,
    decode_exemplars_section, decode_run_histogram_pages, decode_run_pages_soa, encode_run_v4,
    open_from_full, plan_ranges_v4,
};
use ravel_types::declared_stats::DeclaredStatType;
use ravel_types::{Sample, Signal};

use crate::bucket::Bucket;
use crate::build::{BuiltPart, PartPut, put_part_with_ledger};
use crate::claim_guard::{BucketClaim, Checkpoint, ClaimSkipReason, claim_bucket};
use crate::clock::Clock;
use crate::codec::{RsegCodec, SegmentCodec};
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::publish::{
    ConservationPredicate, PublishOutcome, conserve_exact, publish_record_with_conservation,
    publish_superseding_record,
};
use crate::read::{
    BucketListing, input_set_hash, list_bucket_with_ledger, load_inputs_with_ledger,
};
use crate::request_ledger::{RequestLedger, RequestPhase, note_get};
use crate::rlog::RlogCodec;
use crate::rspan_codec::SpanCodec;

/// The result of a [`rewrite_and_publish`] call: how many parts were built and
/// how the record PUT resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteOutcome {
    /// Parts built and PUT for the new object set.
    pub parts: usize,
    /// How publishing the record resolved (won, converged, or abandoned).
    pub publish: PublishOutcome,
}

/// Read the objects named by `commit_keys`, decode each through codec `C`,
/// re-encode through `C`'s current writer into size-capped parts, and publish
/// the superseding record with `conservation` as the record-count gate.
///
/// This is the shared primitive. `C` selects the signal's read/re-encode path
/// (`RsegCodec`, `RlogCodec`, `SpanCodec`); `conservation` selects the epic's
/// conservation arithmetic ([`conserve_exact`] for compaction and format
/// migration, a drop-aware predicate for erasure). Every other step -- input
/// ordering, the `input_set_hash`, part building, and the whole publish
/// protocol -- is identical to compaction and shared verbatim.
///
/// `start_ns` is the run's start for the abandonment deadline, as in
/// [`crate::publish::publish_record`].
pub async fn rewrite_and_publish<C: SegmentCodec>(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    commit_keys: &[String],
    conservation: impl ConservationPredicate,
    start_ns: i64,
) -> Result<RewriteOutcome> {
    // Request-ledger scope (ADR-0996 task 996-8). A run driven by
    // `compact_bucket` or `migrate_bucket_format` already opened its scope
    // before the bucket LIST, and those figures belong to this run's report; a
    // rewrite driven directly opens its own. The scope closes on EVERY exit
    // path, including an error, so a later directly-driven rewrite still starts
    // from zero. Closing leaves the counters readable: a caller inspecting an
    // aborted run's figures reads them after this returns.
    // The guard closes the scope even if this future is cancelled mid-run, so
    // a stale open flag cannot silence a later direct rewrite's report.
    let scope = config
        .request_ledger
        .as_ref()
        .and_then(|l| l.reset_for_run_unless_open().then(|| l.run_scope_guard()));
    let outcome = load_then_rewrite::<C>(
        store,
        clock,
        config,
        bucket,
        commit_keys,
        conservation,
        start_ns,
        None,
    )
    .await
    .and_then(|fenced| match fenced {
        FencedRewrite::Ran(outcome) => Ok(outcome),
        // Unreachable by construction: with no planned listing there is no
        // re-list to differ. Typed rather than panicked, per this crate's
        // no-panic rule.
        FencedRewrite::RecordSetChanged(_) => Err(MaintainError::Invariant(
            "a rewrite given no planned listing reported a changed record set".to_string(),
        )),
    });
    // The frame that OPENED the scope emits the run's report, exactly once
    // (ADR-0996 task 996-8): a directly-driven rewrite reports here; a rewrite
    // dispatched by `compact_bucket` or `migrate_bucket_format` stays silent
    // and the outer driver reports every outcome, gate refusals included.
    if let Some(scope) = scope {
        emit_request_report(config, bucket, outcome.is_ok());
        scope.close();
    }
    outcome
}

/// Emit the run's per-phase request report on the outcome the run actually
/// had (ADR-0996 task 996-8). A failed run's request costs are exactly what
/// an operator wants to see (a conservation abort still paid for its whole
/// read side, and a gate refusal still paid its LIST), so every outermost
/// driver calls this on success and error alike, before closing the scope.
/// Counters only -- nothing in this crate reads a figure back to route a
/// fetch. Requests are logical store calls, not billed attempts (that seam is
/// the S3 adapter's, ADR-0996 decision 3). Received and sent bytes are
/// different kinds and are never summed with each other or with the
/// decoded-heap peaks. Only fires when a ledger is installed (never in
/// production today).
pub(crate) fn emit_request_report(config: &CompactorConfig, bucket: &Bucket, ok: bool) {
    if let Some(l) = config.request_ledger.as_ref() {
        let r = l.report();
        tracing::info!(
            signal = ?bucket.signal,
            shard = bucket.shard,
            ingest_hour_bucket = bucket.ingest_hour_bucket,
            outcome = if ok { "ok" } else { "err" },
            list_requests = r.list.requests,
            record_read_requests = r.record_read.requests,
            record_read_wire_bytes_received = r.record_read.wire_bytes_received,
            catalog_read_requests = r.catalog_read.requests,
            catalog_read_wire_bytes_received = r.catalog_read.wire_bytes_received,
            block_read_requests = r.block_read.requests,
            block_read_wire_bytes_received = r.block_read.wire_bytes_received,
            part_put_requests = r.part_put.requests,
            part_put_wire_bytes_sent = r.part_put.wire_bytes_sent,
            publish_requests = r.publish.requests,
            publish_wire_bytes_received = r.publish.wire_bytes_received,
            publish_wire_bytes_sent = r.publish.wire_bytes_sent,
            // The advisory claim protocol's own requests (ADR-1029 decision 4),
            // never pooled into the merge's phases. Requests only: the claim
            // payloads are built inside `ravel_fleet::claim` and do not cross
            // this crate's seam, so its byte figures stay zero.
            coordinate_requests = r.coordinate.requests,
            total_requests = r.total_requests(),
            total_wire_bytes_received = r.total_wire_bytes_received(),
            total_wire_bytes_sent = r.total_wire_bytes_sent(),
            "compaction store requests by phase (received and sent bytes are different kinds; do not sum them together)"
        );
    }
}

/// [`rewrite_and_publish`]'s body, with the request ledger's run scope already
/// opened and guaranteed to be closed by its caller: read the input commit
/// records, then run the primitive over them. `planned` is passed through to
/// [`rewrite_and_publish_loaded`], whose pre-publish re-list it enables.
#[allow(clippy::too_many_arguments)]
async fn load_then_rewrite<C: SegmentCodec>(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    commit_keys: &[String],
    conservation: impl ConservationPredicate,
    start_ns: i64,
    planned: Option<&BucketListing>,
) -> Result<FencedRewrite> {
    crate::rlog::check_rlog_zstd_level(config, bucket)?;
    let inputs = load_inputs_with_ledger(
        store,
        bucket,
        commit_keys,
        config.input_read_concurrency,
        config.request_ledger.as_ref(),
    )
    .await?;
    rewrite_and_publish_loaded::<C>(
        store,
        clock,
        config,
        bucket,
        inputs,
        conservation,
        start_ns,
        planned,
    )
    .await
}

/// What [`rewrite_and_publish_loaded`] did.
#[derive(Debug)]
pub(crate) enum FencedRewrite {
    /// The run published, converged, or abandoned at a claim checkpoint.
    Ran(RewriteOutcome),
    /// The pre-publish re-list found a record set other than the one the run
    /// planned from, so the run published nothing (ADR-1029, the 2026-10-03
    /// amendment). Carries the new listing, so the caller can report why.
    RecordSetChanged(BucketListing),
}

/// Whether two listings of one bucket name the same record set: the same L0
/// commit records, compaction records, rewrite records and tombstone,
/// whatever order the store listed them in.
fn same_record_set(a: &BucketListing, b: &BucketListing) -> bool {
    fn sorted(keys: &[String]) -> Vec<&str> {
        let mut v: Vec<&str> = keys.iter().map(String::as_str).collect();
        v.sort_unstable();
        v
    }
    sorted(&a.commit_keys) == sorted(&b.commit_keys)
        && sorted(&a.compaction_record_keys) == sorted(&b.compaction_record_keys)
        && sorted(&a.rewrite_record_keys) == sorted(&b.rewrite_record_keys)
        && a.tombstone_key == b.tombstone_key
}

/// The pre-publish re-list (ADR-1029, the 2026-10-03 amendment): list `bucket`
/// again and return the new listing when its record set differs from
/// `planned`, the listing the pass planned from, or `None` when it is the same.
///
/// Compaction and the erasure rewrite both call this after their last claim
/// checkpoint and before their record PUT. A compaction record and a rewrite
/// record have different keys, so neither's `CreateIfAbsent` refuses the other;
/// a pass that publishes over a record set it did not plan from is what would
/// serve erased rows again (issue #2199). The LIST is counted under the
/// ledger's list phase, like the planning listing.
pub(crate) async fn relist_changed(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    planned: &BucketListing,
    ledger: Option<&RequestLedger>,
) -> Result<Option<BucketListing>> {
    let now = list_bucket_with_ledger(store, bucket, ledger).await?;
    if same_record_set(planned, &now) {
        return Ok(None);
    }
    tracing::warn!(
        signal = ?bucket.signal,
        shard = bucket.shard,
        ingest_hour_bucket = bucket.ingest_hour_bucket,
        planned_commit_records = planned.commit_keys.len(),
        planned_compaction_records = planned.compaction_record_keys.len(),
        planned_rewrite_records = planned.rewrite_record_keys.len(),
        commit_records = now.commit_keys.len(),
        compaction_records = now.compaction_record_keys.len(),
        rewrite_records = now.rewrite_record_keys.len(),
        tombstoned = now.tombstone_key.is_some(),
        "bucket record set changed since this pass planned; publishing nothing (ADR-1029)"
    );
    Ok(Some(now))
}

/// [`rewrite_and_publish`] over inputs the caller already read.
///
/// Compaction's driver ([`crate::compact::compact_bucket_claimed`]) enters
/// here: it reads the input records before it takes the bucket claim, and the
/// claim is taken before the first catalog read. Loading them there and handing
/// them over keeps the input GETs at exactly one set per run.
///
/// The caller owns the request ledger's run scope, as it does for
/// [`rewrite_and_publish`] when an outer driver opened one.
///
/// `planned` is the bucket listing the caller planned from. When it is given,
/// the run re-lists the bucket after its last claim checkpoint and publishes
/// nothing if the record set changed ([`relist_changed`]).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rewrite_and_publish_loaded<C: SegmentCodec>(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    inputs: Vec<crate::read::InputRecord>,
    conservation: impl ConservationPredicate,
    start_ns: i64,
    planned: Option<&BucketListing>,
) -> Result<FencedRewrite> {
    // A cancellation checkpoint anywhere below unwinds to here as the typed
    // `ClaimLost` signal (ADR-1029 decision 3): the run stops and publishes
    // nothing, which is exactly `PublishOutcome::Abandoned`'s shape and
    // inherits its safety argument verbatim. `parts` is 0 because a cancelled
    // run publishes no part set; the parts it had already PUT are left in
    // place, content-addressed and byte-identical to what a later run over the
    // same frozen input set republishes.
    match rewrite_and_publish_guarded::<C>(
        store,
        clock,
        config,
        bucket,
        inputs,
        conservation,
        start_ns,
        planned,
    )
    .await
    {
        Err(MaintainError::ClaimLost { at }) => {
            tracing::info!(
                signal = ?bucket.signal,
                shard = bucket.shard,
                ingest_hour_bucket = bucket.ingest_hour_bucket,
                checkpoint = at,
                "compaction run cancelled at a claim checkpoint; nothing published (ADR-1029)"
            );
            Ok(FencedRewrite::Ran(RewriteOutcome {
                parts: 0,
                publish: PublishOutcome::Abandoned,
            }))
        }
        other => other,
    }
}

/// [`rewrite_and_publish_loaded`]'s body, which may unwind with
/// [`MaintainError::ClaimLost`] from any of the cancellation checkpoints.
#[allow(clippy::too_many_arguments)]
async fn rewrite_and_publish_guarded<C: SegmentCodec>(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    inputs: Vec<crate::read::InputRecord>,
    conservation: impl ConservationPredicate,
    start_ns: i64,
    planned: Option<&BucketListing>,
) -> Result<FencedRewrite> {
    // A new run's accounting starts from zero: a tracker left installed in a
    // long-lived config would otherwise carry the previous bucket's peaks
    // into this bucket's emission (serial reuse; concurrent sharing is the
    // installer's contract to avoid).
    if let Some(t) = config.merge_memory_tracker.as_ref() {
        t.reset_for_run();
    }
    C::validate_rewrite_inputs(&inputs)?;
    let hash = input_set_hash(&inputs);

    // Cancellation checkpoint 2 (ADR-1029 decision 3): the input set is
    // listed, read and hashed, and nothing has been fetched per input yet. The
    // catalog fan-out below is the first read whose cost scales with the
    // bucket, so a claim lost by here saves all of it.
    crate::claim_guard::checkpoint(config, store, crate::claim_guard::Checkpoint::InputSet).await?;

    // Catalogs aligned one-to-one with `inputs` (canonical order): the merge
    // relies on that alignment for deterministic tie-breaking, same as
    // compaction's pipeline. `buffered` (not `buffer_unordered`) keeps that
    // alignment while `input_read_concurrency` catalog loads are in flight:
    // it yields results in input order regardless of completion order.
    //
    // The futures are built in a plain loop rather than a `.map()` closure:
    // a closure returning a future that borrows its `&InputRecord` argument is
    // inferred as higher-ranked over that lifetime, and its auto-traits then
    // fail to leak through the stream adapter with "implementation of `FnOnce`
    // is not general enough" wherever the maintain loop is `tokio::spawn`ed.
    let mut pending = Vec::with_capacity(inputs.len());
    for input in &inputs {
        pending.push(C::load_input_catalog(store, config, input));
    }
    let catalogs: Vec<C::Catalog> = stream_iter(pending)
        .buffered(config.input_read_concurrency.max(1))
        .try_collect()
        .await?;

    let parts = C::build_parts(store, config, bucket, &inputs, catalogs, &hash).await?;

    // Cancellation checkpoint 5 (ADR-1029 decision 3): the last quiescent
    // point before the record PUT. A claim lost here stops the run with every
    // part already written and no record naming them, which is what
    // `PublishOutcome::Abandoned` is for.
    crate::claim_guard::checkpoint(config, store, crate::claim_guard::Checkpoint::Publish).await?;

    // The second check of the fence (ADR-1029, the 2026-10-03 amendment): a
    // record set that moved since planning, an erasure rewrite record above all,
    // means this run's inputs are no longer the bucket's, so it publishes
    // nothing. The parts already PUT are left as an abandoned run leaves them.
    if let Some(planned) = planned
        && let Some(now) =
            relist_changed(store, bucket, planned, config.request_ledger.as_ref()).await?
    {
        return Ok(FencedRewrite::RecordSetChanged(now));
    }

    let publish = publish_record_with_conservation(
        store,
        config,
        clock,
        bucket,
        &inputs,
        &hash,
        &parts,
        start_ns,
        conservation,
    )
    .await?;

    // Issue #977: surface the compaction's peak memory split by phase. Every
    // term is populated by now (catalog load before build_parts, cursor/writer/
    // retained during build_parts, publish record during publish), and `parts`
    // is still alive so the retained high-water reflects the whole set. Each
    // field names its byte kind; they are NOT summable across kinds. Only fires
    // when a tracker is installed (never in production today, see
    // `MergeMemoryTracker`); installing one is the one-line service change that
    // makes this visible to an operator.
    if let Some(t) = config.merge_memory_tracker.as_ref() {
        let peaks = t.phase_peaks();
        tracing::info!(
            signal = ?bucket.signal,
            shard = bucket.shard,
            ingest_hour_bucket = bucket.ingest_hour_bucket,
            parts = parts.len(),
            catalog_directory_decoded_bytes = peaks.catalog_directory_decoded_bytes,
            cursor_bytes = peaks.cursor_bytes,
            writer_heap_bytes = peaks.writer_heap_bytes,
            retained_part_encoded_bytes = peaks.retained_part_encoded_bytes,
            publish_record_encoded_bytes = peaks.publish_record_encoded_bytes,
            probe_bytes = peaks.probe_bytes,
            "compaction peak memory by phase (byte kinds differ per term; do not sum)"
        );
    }

    Ok(FencedRewrite::Ran(RewriteOutcome {
        parts: parts.len(),
        publish,
    }))
}

/// The result of a [`migrate_bucket_format`] call. Every variant except
/// [`MigrateOutcome::Rewritten`] left the bucket untouched, with the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrateOutcome {
    /// Not yet sealed: the input set is not guaranteed complete.
    NotSealed,
    /// A retention tombstone is present; the bucket is never rewritten.
    Tombstoned,
    /// A compaction record already exists. The live record set is
    /// L1 parts, not raw L0 objects; migrating those is the rewrite-on-
    /// touch, which reads L1 parts as inputs. This caller handles the pre-
    /// compaction L0 case only.
    AlreadyCompacted,
    /// The bucket holds a live erasure rewrite record, so producing a second
    /// record set over the same inputs would make the catalog serve both
    /// (ADR-0064 decision 3 point 5: overlap harmlessness does not hold for a
    /// rewrite, whose outputs deliberately lack records its inputs contain, so
    /// a migration record built from those same inputs would resurrect the
    /// erased records through query-time dedup).
    RewritePresent,
    /// No L0 input below `target_version`: the bucket is already at or above
    /// the floor, nothing to migrate.
    UpToDate,
    /// At least one below-target L0 input; the whole live L0 set was rewritten
    /// into current-format L1 parts and published, conserving the record count
    /// exactly.
    Rewritten {
        parts: usize,
        publish: PublishOutcome,
    },
    /// The bucket's claim was not available (another process holds it, or
    /// `reason` names the other refusal), so the migration built and published
    /// nothing (ADR-1029, the 2026-10-03 amendment). A later run retries.
    SkippedClaimed { reason: ClaimSkipReason },
    /// The migration took the bucket's claim and lost it before its record PUT,
    /// and cancelled at `at` with nothing published.
    Cancelled { at: Checkpoint },
}

/// EM's compaction-variant caller of the shared primitive: migrate a sealed
/// bucket's live L0 objects to the current format when any of them was written
/// below `target_version`.
///
/// This is the "N-1 decode, re-encode, publish, conservation" shape named in
/// The rewrite primitive, wired end to end: it gates the bucket exactly as
/// [`crate::compact::compact_bucket`] does (seal, tombstone, already-compacted),
/// reads the L0 commit records, and -- if any records the current writer would
/// supersede was written below the target format version -- rewrites the whole
/// live L0 set through the rewrite primitive with [`conserve_exact`],
/// producing current-format L1 parts and a superseding record. The old objects
/// become sweepable exactly like any superseded compaction input.
///
/// `target_version` is the format version the migration is raising toward
/// (`ravel_segment::VERSION_V7 as u32` in production). Passing a version above
/// the current writer's output makes every current-version object eligible,
/// which is how the tests exercise the migration path before an actual N-1
/// version exists to read.
///
/// The `maintain migrate` driver (resumable cursor, budget, verify-and-
/// raise-floor) and rewrite-on-touch build on this entry point; the
/// server/CLI wiring is their scope, not this task's.
///
/// A migration publishes a compaction record, so it is fenced against an
/// erasure rewrite of the same bucket exactly as a compaction is (ADR-1029,
/// the 2026-10-03 amendment): with a claim participant installed and
/// coordination on it takes the bucket's claim before it rewrites and holds it
/// through the record PUT, backing off ([`MigrateOutcome::SkippedClaimed`])
/// when it cannot, and it re-lists the bucket before that PUT either way,
/// publishing nothing when the record set changed since its listing.
pub async fn migrate_bucket_format(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    target_version: u32,
) -> Result<MigrateOutcome> {
    // This is the run's outermost driver: it opens the request ledger's scope
    // before the bucket LIST below and, as the opener, emits the run's report
    // on EVERY outcome -- the gates that return before any rewrite dispatches
    // (NotSealed, UpToDate, listing errors) still paid their LIST and report
    // it. The rewrite primitive it dispatches to sees the open scope and stays
    // silent (ADR-0996 task 996-8).
    let scope = config.request_ledger.as_ref().map(|l| {
        l.reset_for_run();
        l.run_scope_guard()
    });
    let outcome = migrate_bucket_format_scoped(store, clock, config, bucket, target_version).await;
    emit_request_report(config, bucket, outcome.is_ok());
    if let Some(scope) = scope {
        scope.close();
    }
    outcome
}

/// [`migrate_bucket_format`]'s body, with the request ledger's run scope
/// already opened and guaranteed to be closed by its caller.
async fn migrate_bucket_format_scoped(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    target_version: u32,
) -> Result<MigrateOutcome> {
    crate::rlog::check_rlog_zstd_level(config, bucket)?;
    let start_ns = clock.now_ns();
    if !bucket.is_sealed(start_ns, config) {
        return Ok(MigrateOutcome::NotSealed);
    }

    let listing = list_bucket_with_ledger(store, bucket, config.request_ledger.as_ref()).await?;
    if let Some(refused) = migrate_listing_gate(&listing) {
        return Ok(refused);
    }
    if listing.commit_keys.is_empty() {
        return Ok(MigrateOutcome::UpToDate);
    }

    // Decide eligibility from the commit records' recorded format version,
    // never a data-object GET (ADR-0066: the catalog knows every live object's
    // version without reading it). `load_inputs` verifies each record's key and
    // bucket, so the eligibility read rides on the same decode the rewrite
    // needs anyway.
    let inputs = load_inputs_with_ledger(
        store,
        bucket,
        &listing.commit_keys,
        config.input_read_concurrency,
        config.request_ledger.as_ref(),
    )
    .await?;
    let needs_migration = inputs
        .iter()
        .any(|i| i.record.segment_format_version < target_version);
    if !needs_migration {
        return Ok(MigrateOutcome::UpToDate);
    }

    // The bucket claim, the same one a compaction or an erasure rewrite of this
    // bucket takes, held from here through the record PUT. A pass refused it
    // backs off without building anything.
    let guard = match claim_bucket(store, config, bucket, "migrate").await? {
        BucketClaim::Held { guard, .. } => Some(guard),
        BucketClaim::NotParticipating => None,
        BucketClaim::Skipped(skip) => {
            return Ok(MigrateOutcome::SkippedClaimed {
                reason: skip.reason,
            });
        }
    };
    let run_config = match guard.as_ref() {
        Some(guard) => CompactorConfig {
            claim_guard: Some(guard.clone()),
            ..config.clone()
        },
        None => config.clone(),
    };

    let fenced = dispatch_rewrite(
        bucket.signal,
        store,
        clock,
        &run_config,
        bucket,
        &listing,
        start_ns,
    )
    .await?;

    // As in compaction: a run that lost its claim cancelled and published
    // nothing; one that still holds it marks it completed, and a failure to
    // write that forensic marker leaves the claim to age out under its lease.
    if let Some(guard) = guard {
        if let Some(at) = guard.cancelled_at().await {
            return Ok(MigrateOutcome::Cancelled { at });
        }
        if let Err(err) = guard.complete(store).await {
            tracing::warn!(
                signal = ?bucket.signal,
                shard = bucket.shard,
                ingest_hour_bucket = bucket.ingest_hour_bucket,
                work_id = %guard.work_id_hex(),
                error = %err,
                "migration finished, but marking its claim completed failed; \
                 the claim ages out under its lease (ADR-1029)"
            );
        }
    }

    Ok(match fenced {
        FencedRewrite::Ran(outcome) => MigrateOutcome::Rewritten {
            parts: outcome.parts,
            publish: outcome.publish,
        },
        // The pre-publish re-list found a record set this run did not plan
        // from, so it published nothing: report the gate the new listing
        // fails, an erasure rewrite record above all.
        FencedRewrite::RecordSetChanged(now) => {
            migrate_listing_gate(&now).unwrap_or(MigrateOutcome::Rewritten {
                parts: 0,
                publish: PublishOutcome::Abandoned,
            })
        }
    })
}

/// The listing gates a migration applies to the listing it plans from, and
/// again to its pre-publish re-list: `Some` is the reason the bucket is not
/// migrated.
fn migrate_listing_gate(listing: &BucketListing) -> Option<MigrateOutcome> {
    if listing.tombstone_key.is_some() {
        return Some(MigrateOutcome::Tombstoned);
    }
    if !listing.compaction_record_keys.is_empty() {
        return Some(MigrateOutcome::AlreadyCompacted);
    }
    // One bucket serves one record set. A live rewrite record already covers
    // these inputs with records deliberately removed from its outputs, and a
    // migration record over the same inputs is not overlap-harmless against
    // it: a snapshot including both resurrects the erased records
    // (ADR-0064 decision 3 point 5). The format floor stays unraised for this
    // bucket until the erasure rewrite's own output is what gets migrated.
    if !listing.rewrite_record_keys.is_empty() {
        return Some(MigrateOutcome::RewritePresent);
    }
    None
}

/// Dispatch the rewrite primitive on the bucket's signal to the matching
/// codec, with the exact-conservation predicate, over `planned`'s commit
/// records and with `planned` as the listing its pre-publish re-list compares
/// against. Mirrors [`crate::compact::compact_bucket`]'s signal dispatch so the
/// rewrite path covers the same three signals with the same codecs.
///
/// The caller owns the request ledger's run scope.
async fn dispatch_rewrite(
    signal: Signal,
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    planned: &BucketListing,
    start_ns: i64,
) -> Result<FencedRewrite> {
    let commit_keys = &planned.commit_keys;
    match signal {
        Signal::Metrics => {
            load_then_rewrite::<RsegCodec>(
                store,
                clock,
                config,
                bucket,
                commit_keys,
                conserve_exact(),
                start_ns,
                Some(planned),
            )
            .await
        }
        Signal::Logs => {
            load_then_rewrite::<RlogCodec>(
                store,
                clock,
                config,
                bucket,
                commit_keys,
                conserve_exact(),
                start_ns,
                Some(planned),
            )
            .await
        }
        Signal::Spans => {
            load_then_rewrite::<SpanCodec>(
                store,
                clock,
                config,
                bucket,
                commit_keys,
                conserve_exact(),
                start_ns,
                Some(planned),
            )
            .await
        }
        other => Err(MaintainError::Invariant(format!(
            "rewrite is not implemented for signal {other:?}"
        ))),
    }
}

/// RSEG section kinds the force 2 re-encode reads out of a whole part object
/// (docs/segment-format.md), named as `read.rs` names them.
const RSEG_LABEL_DICT: u32 = 1;
const RSEG_EXEMPLARS: u32 = 10;

/// The result of a [`reencode_compaction_parts`] call. Every variant except
/// [`ReencodeOutcome::Reencoded`] published nothing, and names why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReencodeOutcome {
    /// [`CompactorConfig::reencode_writer_enabled`] is off. No store request
    /// was made.
    WriterDisabled,
    /// A retention tombstone is present; the bucket is never rewritten.
    Tombstoned,
    /// The bucket holds a rewrite record, which blocks it from every
    /// migration (ADR-1331).
    RewritePresent,
    /// The bucket holds no compaction record. Raw L0 records are
    /// [`migrate_bucket_format`]'s.
    NoCompactionRecord,
    /// An overlap component holds more than one compaction record once the
    /// records a version 2 record supersedes are set aside (ADR-0066 force 2
    /// amendment, item 4): a new record's hash could lose the tie-break to the
    /// old loser.
    ContestedOverlap { largest_component: usize },
    /// More than one compaction record survives supersession, each in its own
    /// overlap component. Force 2 re-encodes a bucket's one record only.
    MultipleRecords { records: usize },
    /// Every part of the bucket's one compaction record is at the current
    /// segment format version.
    UpToDate,
    /// The parts of the record at `superseded_record_key` were re-encoded into
    /// `parts` current-version parts, and the version 2 record naming it
    /// resolved as `publish` says.
    Reencoded {
        superseded_record_key: String,
        parts: usize,
        publish: PublishOutcome,
    },
    /// The pre-publish re-list found a record set other than the one this run
    /// planned from, so it published nothing (ADR-1029, the 2026-10-03
    /// amendment). A later run plans again.
    RecordSetChanged,
    /// The bucket's claim was not available, so the run read no part and
    /// published nothing.
    SkippedClaimed { reason: ClaimSkipReason },
    /// The run took the bucket's claim and lost it before its record PUT, and
    /// cancelled at `at` with nothing published.
    Cancelled { at: Checkpoint },
}

/// The segment format version the current writer emits for `signal`'s
/// compaction parts.
fn current_part_version(signal: Signal) -> Result<u32> {
    match signal {
        Signal::Metrics => Ok(crate::build::OUTPUT_FORMAT_VERSION),
        Signal::Logs => Ok(crate::rlog::OUTPUT_FORMAT_VERSION),
        Signal::Spans => Ok(crate::rspan_codec::OUTPUT_FORMAT_VERSION),
        other => Err(MaintainError::Invariant(format!(
            "re-encode is not implemented for signal {other:?}"
        ))),
    }
}

/// ADR-0066 force 2: re-encode the parts of `bucket`'s one compaction record at
/// the current segment format version and publish a version 2 compaction
/// record superseding it.
///
/// It runs only when [`CompactorConfig::reencode_writer_enabled`] is on, and
/// only when the force 2 amendment says it applies: no tombstone, no rewrite
/// record in the bucket (ADR-1331), exactly one compaction record once the
/// shared selector ([`select_authoritative_compaction_records`]) has set aside
/// the records a version 2 record supersedes (item 4), and at least one of that
/// record's parts below the current version. Every other case returns the
/// [`ReencodeOutcome`] that names it, having written nothing. A part recorded
/// above the current version is an [`MaintainError::Invariant`]: a forward
/// re-encode cannot write it (ADR-0066 decision 2).
///
/// Every part is re-encoded with exact contents (item 7). An RSEG part is read
/// whole and re-encoded part for part: each run is decoded and re-encoded with
/// its own run-wide provenance, its per-sample dedup provenance column and its
/// exemplars carried over, so part boundaries and part indexes are the
/// predecessor's. RLOG and RSPAN parts go through their codec's compaction
/// merge, which reads them by range and decodes and re-encodes every record,
/// so their part split follows this run's configuration. The new parts are written
/// `CreateIfAbsent` as compaction writes them, and the record goes through
/// compaction's publish path: its inputs are the predecessor's, verbatim, its
/// `superseded_record_key` names the predecessor, and its key is the
/// canonical key of its version 2 hash. The conservation gate compares the
/// predecessor's part record counts with the new parts'.
///
/// The run takes the bucket's claim through `claim_guard::claim_bucket` after
/// it has read the compaction records and before it reads any part, holds it
/// through the record PUT, and consults it at every merge checkpoint and at the
/// publish checkpoint. Immediately before the record PUT it lists the bucket again and
/// publishes nothing if the record set changed (ADR-1029, the 2026-10-03
/// amendment), the same fence compaction and the erasure rewrite use.
///
/// Nothing in production calls this yet: `migrate` wiring is ADR-0066 force 2
/// task T6.
pub async fn reencode_compaction_parts(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
) -> Result<ReencodeOutcome> {
    if !config.reencode_writer_enabled {
        return Ok(ReencodeOutcome::WriterDisabled);
    }
    let scope = config.request_ledger.as_ref().map(|l| {
        l.reset_for_run();
        l.run_scope_guard()
    });
    let outcome = reencode_compaction_parts_scoped(store, clock, config, bucket).await;
    emit_request_report(config, bucket, outcome.is_ok());
    if let Some(scope) = scope {
        scope.close();
    }
    outcome
}

/// [`reencode_compaction_parts`]'s body, with the request ledger's run scope
/// already opened and guaranteed to be closed by its caller.
async fn reencode_compaction_parts_scoped(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
) -> Result<ReencodeOutcome> {
    crate::rlog::check_rlog_zstd_level(config, bucket)?;
    let target_version = current_part_version(bucket.signal)?;
    let start_ns = clock.now_ns();
    let ledger = config.request_ledger.as_ref();

    let listing = list_bucket_with_ledger(store, bucket, ledger).await?;
    if listing.tombstone_key.is_some() {
        return Ok(ReencodeOutcome::Tombstoned);
    }
    if !listing.rewrite_record_keys.is_empty() {
        return Ok(ReencodeOutcome::RewritePresent);
    }
    if listing.compaction_record_keys.is_empty() {
        return Ok(ReencodeOutcome::NoCompactionRecord);
    }

    let records = read_compaction_records(store, &listing.compaction_record_keys, ledger).await?;
    let selection = select_authoritative_compaction_records(&records).map_err(|err| {
        MaintainError::Invariant(format!(
            "compaction records have an unresolvable supersession chain: {err}"
        ))
    })?;
    if selection.largest_component() > 1 {
        return Ok(ReencodeOutcome::ContestedOverlap {
            largest_component: selection.largest_component(),
        });
    }
    let live: Vec<&(String, CompactionRecord)> = records
        .iter()
        .filter(|(key, _)| !selection.is_excluded(key))
        .collect();
    let (predecessor_key, predecessor) = match live.as_slice() {
        [] => return Ok(ReencodeOutcome::NoCompactionRecord),
        [only] => (only.0.as_str(), &only.1),
        more => {
            return Ok(ReencodeOutcome::MultipleRecords {
                records: more.len(),
            });
        }
    };

    if let Some(newer) = predecessor
        .parts
        .iter()
        .find(|p| p.segment_format_version > target_version)
    {
        return Err(MaintainError::Invariant(format!(
            "compaction record {predecessor_key} has part {} recorded at format version {}, \
             newer than the current output version {target_version}: a re-encode cannot \
             write it (ADR-0066 decision 2)",
            newer.part_index, newer.segment_format_version
        )));
    }
    if !predecessor
        .parts
        .iter()
        .any(|p| p.segment_format_version < target_version)
    {
        return Ok(ReencodeOutcome::UpToDate);
    }

    // The bucket claim, the one compaction, the erasure rewrite and `migrate`
    // take, held from here through the record PUT.
    let guard = match claim_bucket(store, config, bucket, "reencode").await? {
        BucketClaim::Held { guard, .. } => Some(guard),
        BucketClaim::NotParticipating => None,
        BucketClaim::Skipped(skip) => {
            return Ok(ReencodeOutcome::SkippedClaimed {
                reason: skip.reason,
            });
        }
    };
    let run_config = match guard.as_ref() {
        Some(guard) => CompactorConfig {
            claim_guard: Some(guard.clone()),
            ..config.clone()
        },
        None => config.clone(),
    };

    let fenced = match reencode_and_publish(
        store,
        clock,
        &run_config,
        bucket,
        &listing,
        predecessor_key,
        predecessor,
        start_ns,
    )
    .await
    {
        Err(MaintainError::ClaimLost { .. }) if guard.is_some() => None,
        other => Some(other?),
    };

    if let Some(guard) = guard {
        if let Some(at) = guard.cancelled_at().await {
            return Ok(ReencodeOutcome::Cancelled { at });
        }
        if let Err(err) = guard.complete(store).await {
            tracing::warn!(
                signal = ?bucket.signal,
                shard = bucket.shard,
                ingest_hour_bucket = bucket.ingest_hour_bucket,
                work_id = %guard.work_id_hex(),
                error = %err,
                "re-encode finished, but marking its claim completed failed; \
                 the claim ages out under its lease (ADR-1029)"
            );
        }
    }

    match fenced {
        Some(FencedRewrite::Ran(outcome)) => Ok(ReencodeOutcome::Reencoded {
            superseded_record_key: predecessor_key.to_string(),
            parts: outcome.parts,
            publish: outcome.publish,
        }),
        Some(FencedRewrite::RecordSetChanged(_)) => Ok(ReencodeOutcome::RecordSetChanged),
        None => Err(MaintainError::Invariant(
            "a re-encode lost its claim but its guard names no checkpoint".to_string(),
        )),
    }
}

/// GET and decode each compaction record in `record_keys`, checking each one
/// reconstructs to the key it was listed at.
async fn read_compaction_records(
    store: &dyn ObjectStoreBackend,
    record_keys: &[String],
    ledger: Option<&RequestLedger>,
) -> Result<Vec<(String, CompactionRecord)>> {
    let mut records = Vec::with_capacity(record_keys.len());
    for key in record_keys {
        let got = store.get(key, GetRange::Full).await;
        note_get(ledger, RequestPhase::RecordRead, &got);
        let record = ravel_commit::record::decode_compaction(got?.data.as_ref()).map_err(|e| {
            MaintainError::Invariant(format!("compaction record {key} does not decode: {e}"))
        })?;
        keys::verify_compaction_record_key(&record, key)?;
        records.push((key.clone(), record));
    }
    Ok(records)
}

/// Re-encode `predecessor`'s parts and publish the version 2 record over them,
/// fenced by the claim checkpoints in `config` and by the pre-publish re-list
/// against `planned`. May unwind with [`MaintainError::ClaimLost`] from any
/// checkpoint.
#[allow(clippy::too_many_arguments)]
async fn reencode_and_publish(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
    planned: &BucketListing,
    predecessor_key: &str,
    predecessor: &CompactionRecord,
    start_ns: i64,
) -> Result<FencedRewrite> {
    if let Some(t) = config.merge_memory_tracker.as_ref() {
        t.reset_for_run();
    }
    let hash = ravel_commit::erasure::compute_superseding_compaction_input_set_hash(
        &predecessor.inputs,
        predecessor_key,
    );
    crate::claim_guard::checkpoint(config, store, Checkpoint::InputSet).await?;

    let part_keys = predecessor
        .parts
        .iter()
        .map(|p| keys::reconstruct_l1_part_key(predecessor, p))
        .collect::<std::result::Result<Vec<String>, _>>()?;
    let parts = match bucket.signal {
        Signal::Metrics => {
            reencode_rseg_parts(store, config, bucket, predecessor, &part_keys, &hash).await?
        }
        Signal::Logs => {
            let catalogs =
                crate::rlog::load_catalogs_by_key(store, config, &part_keys, true).await?;
            let mut indexed_fields: Vec<String> = catalogs
                .iter()
                .flat_map(|c| c.indexed_fields.iter().cloned())
                .collect();
            indexed_fields.sort();
            indexed_fields.dedup();
            crate::rlog::merge_catalogs(
                store,
                config,
                bucket,
                &catalogs,
                &hash,
                indexed_fields,
                declared_columns_from_parts(&predecessor.parts),
                config.dry_run,
                false,
                &mut |_| Ok(true),
            )
            .await?
            .parts
        }
        Signal::Spans => {
            let catalogs =
                crate::rspan_codec::load_catalogs_by_key(store, config, &part_keys).await?;
            crate::rspan_codec::merge(
                store,
                config,
                bucket,
                &catalogs,
                &hash,
                config.dry_run,
                &mut |_| Ok(true),
            )
            .await?
            .parts
        }
        other => {
            return Err(MaintainError::Invariant(format!(
                "re-encode is not implemented for signal {other:?}"
            )));
        }
    };

    crate::claim_guard::checkpoint(config, store, Checkpoint::Publish).await?;
    if let Some(now) =
        relist_changed(store, bucket, planned, config.request_ledger.as_ref()).await?
    {
        return Ok(FencedRewrite::RecordSetChanged(now));
    }

    let publish = publish_superseding_record(
        store,
        config,
        clock,
        bucket,
        predecessor_key,
        predecessor,
        &hash,
        &parts,
        start_ns,
        conserve_exact(),
    )
    .await?;
    Ok(FencedRewrite::Ran(RewriteOutcome {
        parts: parts.len(),
        publish,
    }))
}

/// The declared columns the predecessor's RLOG parts carry stamps for, so the
/// re-encoded parts recompute stamps for the same columns (ADR-0873 decision
/// 3).
fn declared_columns_from_parts(parts: &[CompactionPart]) -> Vec<(String, DeclaredStatType)> {
    let mut seen: BTreeMap<String, DeclaredStatType> = BTreeMap::new();
    for part in parts {
        for stat in ravel_commit::declared_stats::read_compaction_part(part).covered() {
            seen.entry(stat.name().to_string())
                .or_insert_with(|| stat.declared_type());
        }
    }
    seen.into_iter().collect()
}

/// Re-encode each RSEG part of `predecessor` into one current-version part
/// with the same part index, PUT it `CreateIfAbsent`, and release its bytes,
/// as compaction does. The compactor's `build_parts` is not used: it stamps
/// every run with its input's commit-record provenance and writes a single-run
/// series without a provenance column, which would rewrite the dedup keys an
/// L1 part already carries.
async fn reencode_rseg_parts(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    predecessor: &CompactionRecord,
    part_keys: &[String],
    input_set_hash: &[u8; 32],
) -> Result<Vec<BuiltPart>> {
    let ledger = config.request_ledger.as_ref();
    let mut built = Vec::with_capacity(part_keys.len());
    for (part, key) in predecessor.parts.iter().zip(part_keys) {
        crate::claim_guard::checkpoint(config, store, Checkpoint::MergeLoop).await?;
        let got = store.get(key, GetRange::Full).await;
        note_get(ledger, RequestPhase::BlockRead, &got);
        let object = got?.data;
        if blake3::hash(&object).as_bytes().as_slice() != part.content_hash.as_slice() {
            return Err(MaintainError::Invariant(format!(
                "part {key} does not match its record's content hash"
            )));
        }
        let mut new_part = reencode_rseg_part(
            bucket,
            config,
            &object,
            part,
            predecessor.level,
            input_set_hash,
        )?;
        if !config.dry_run
            && put_part_with_ledger(store, &new_part, ledger).await? == PartPut::AlreadyExisted
        {
            new_part.put_already_existed = true;
        }
        new_part.bytes = None;
        crate::claim_guard::checkpoint(config, store, Checkpoint::PartBoundary).await?;
        built.push(new_part);
    }
    Ok(built)
}

/// `(offset, len)` of `object`, or a typed error when it falls outside.
fn object_range(object: &[u8], (offset, len): (u64, u64)) -> Result<&[u8]> {
    let start = usize::try_from(offset).ok();
    let end = start
        .zip(usize::try_from(len).ok())
        .and_then(|(s, l)| s.checked_add(l));
    start
        .zip(end)
        .and_then(|(s, e)| object.get(s..e))
        .ok_or_else(|| {
            MaintainError::Invariant(format!(
                "range ({offset}, {len}) falls outside a {}-byte part",
                object.len()
            ))
        })
}

/// Re-encode one whole RSEG part at the current version: every run decoded and
/// re-encoded under its own run-wide provenance, its per-sample provenance
/// column (when it has one) and the part's exemplars carried as they are, and
/// the footer's ingest bounds kept.
fn reencode_rseg_part(
    bucket: &Bucket,
    config: &CompactorConfig,
    object: &[u8],
    part: &CompactionPart,
    level: u32,
    input_set_hash: &[u8; 32],
) -> Result<BuiltPart> {
    let limits = ReaderLimits::default();
    let loc = open_from_full(object, limits)?;
    let footer = &loc.footer;
    let entries = decode_catalog_v5(footer, object, limits)?;
    let refs: Vec<&SeriesEntryV4> = entries.iter().collect();
    let mut planned = plan_ranges_v4(footer, &refs)?.into_iter();

    let mut series = Vec::with_capacity(entries.len());
    let mut run_count: u64 = 0;
    let mut scratch = Vec::new();
    for entry in &entries {
        let series_id = entry.entry.series_id;
        let mut runs = Vec::with_capacity(entry.runs.len());
        for (i, run) in entry.runs.iter().enumerate() {
            let range = planned.next().ok_or_else(|| {
                MaintainError::Invariant("plan_ranges_v4 produced fewer ranges than runs".into())
            })?;
            let ts_page = object_range(object, range.ts_range)?;
            let values = match entry.entry.value_kind {
                ValueKind::Scalar => {
                    let mut timestamps = Vec::new();
                    let mut values = Vec::new();
                    decode_run_pages_soa(
                        &series_id,
                        run,
                        ts_page,
                        object_range(object, range.val_range)?,
                        limits,
                        &mut scratch,
                        &mut timestamps,
                        &mut values,
                    )?;
                    SeriesValues::Scalar(
                        timestamps
                            .into_iter()
                            .zip(values)
                            .map(|(ts_ns, value)| Sample { ts_ns, value })
                            .collect(),
                    )
                }
                ValueKind::Histogram => SeriesValues::Histogram(decode_run_histogram_pages(
                    &series_id,
                    run,
                    ts_page,
                    object_range(object, range.hist_range)?,
                    limits,
                )?),
            };
            runs.push(RunInputV7 {
                run: encode_run_v4(
                    &series_id,
                    run.created_unix_ns,
                    run.writer_epoch,
                    run.writer_seq,
                    &values,
                )?,
                provenance: entry.per_sample_provenance.get(i).cloned().flatten(),
            });
        }
        run_count += runs.len() as u64;
        series.push(SeriesInputV7 {
            series_id,
            labels: entry.entry.labels.clone(),
            runs,
        });
    }
    if planned.next().is_some() {
        return Err(MaintainError::Invariant(
            "plan_ranges_v4 produced more ranges than runs".into(),
        ));
    }
    let exemplars = rseg_part_exemplars(object, footer, &entries, limits)?;

    let first_series_id = series.iter().map(|s| s.series_id).min();
    let last_series_id = series.iter().map(|s| s.series_id).max();
    let identity = SegmentIdentity {
        tenant_hash: bucket.tenant_hash.0,
        shard: bucket.shard,
        writer_id: config.compactor_writer_id.to_string(),
        writer_epoch: 0,
        writer_seq: 0,
    };
    let ingest = IngestBounds {
        min_ingest_ts_ns: footer.min_ingest_ts_ns,
        max_ingest_ts_ns: footer.max_ingest_ts_ns,
    };
    let meta = CompactionMetaV4 {
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        input_set_hash: *input_set_hash,
        part_index: part.part_index,
        level,
    };
    let written =
        SegmentWriter::write_v7_with_provenance(series, identity, ingest, meta, exemplars)?;
    let content_hash = written.summary.blake3;
    let key = keys::l1_part_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
        &hex::encode(&input_set_hash[..8]),
        part.part_index,
        &hex::encode(&content_hash[..8]),
    )?;
    let mut new_part = CompactionPart {
        part_index: part.part_index,
        first_series_id: first_series_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        last_series_id: last_series_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        content_hash: content_hash.to_vec(),
        object_size: written.bytes.len() as u64,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        run_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: crate::build::OUTPUT_FORMAT_VERSION,
        declared_column_stats: Vec::new(),
    };
    // Metrics carry no declared columns; stamped the one way `build.rs` does.
    ravel_commit::declared_stats::stamp_compaction_part(&mut new_part, &[]);
    Ok(BuiltPart {
        key,
        bytes: Some(written.bytes),
        part: new_part,
        put_already_existed: false,
    })
}

/// The exemplars of one whole RSEG part, each resolved from its
/// `series_index` to the series id it names, so the writer re-resolves it
/// against the new part's SERIES_IDS (ADR-0047 decision 3).
fn rseg_part_exemplars(
    object: &[u8],
    footer: &ravel_segment::Footer,
    entries: &[SeriesEntryV4],
    limits: ReaderLimits,
) -> Result<Vec<ExemplarInput>> {
    let section = |kind: u32| footer.sections.iter().find(|s| s.kind == kind);
    let Some(exemplars) = section(RSEG_EXEMPLARS) else {
        return Ok(Vec::new());
    };
    let dict = section(RSEG_LABEL_DICT).ok_or_else(|| {
        MaintainError::Invariant("RSEG part carries EXEMPLARS without LABEL_DICT".into())
    })?;
    let records = decode_exemplars_section(
        footer,
        object_range(object, (dict.offset, dict.len))?,
        object_range(object, (exemplars.offset, exemplars.len))?,
        limits,
    )?;
    records
        .into_iter()
        .map(|r| {
            let entry = usize::try_from(r.series_index)
                .ok()
                .and_then(|idx| entries.get(idx))
                .ok_or_else(|| {
                    MaintainError::Invariant(format!(
                        "exemplar series_index {} is outside the part's catalog",
                        r.series_index
                    ))
                })?;
            Ok(ExemplarInput {
                series_id: entry.entry.series_id,
                ts_ns: r.ts_ns,
                value: r.value,
                trace_id: r.trace_id,
                span_id: r.span_id,
                attrs: r.attrs,
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    //! Exercises the shared primitive end to end over real RSEG v7 objects on a
    //! `MemoryStore`: a format-migration round trip that conserves record
    //! content, the conservation-abort path (a rejecting predicate publishes
    //! nothing), the crash-and-rerun convergence at the content-addressed key,
    //! and proof the conservation predicate is genuinely pluggable rather than
    //! silently exact.

    use bytes::Bytes;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_commit::{erasure, keys, signal};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::{
        CompactionInputIdentity, CompactionPart, CompactionRecord, RewriteDrop, RewriteRecord,
    };
    use ravel_segment::{
        IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues,
        VERSION_V7,
    };
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantHash, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::publish::conserve_exact;
    use crate::read::{input_set_hash, list_bucket, load_inputs};
    use crate::{CompactorConfig, FixedClock};

    const TENANT: &str = "acme";
    const SHARD: u32 = 7;
    const HOUR: u32 = 495_000;
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;
    /// A version above the current writer's output, so every real v7 object
    /// counts as "old" and the migration path runs. Stands in for the N-1
    /// version an actual reader window would supply.
    const FUTURE_VERSION: u32 = VERSION_V7 as u32 + 1;

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

    /// Seed one L0 input (data object + commit record) through the production
    /// flush writer, so the seeded object is byte-for-byte a real v7 L0 flush.
    /// Returns the data object's bytes.
    async fn seed(store: &dyn ObjectStoreBackend, seq: u64, series: Vec<SeriesInputV3>) -> Bytes {
        seed_with_version(store, seq, series, VERSION_V7 as u32).await
    }

    /// Like [`seed`], but records `segment_format_version` in the commit
    /// record as given rather than the writer's true output version. The
    /// object bytes are always real v7 -- only the metadata a caller reads
    /// without decoding (the guard under test) can disagree with them.
    async fn seed_with_version(
        store: &dyn ObjectStoreBackend,
        seq: u64,
        series: Vec<SeriesInputV3>,
        segment_format_version: u32,
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
            SegmentWriter::write_histograms_with_exemplars(series, identity, bounds, Vec::new())
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
            segment_format_version,
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

    /// The single compaction record in the bucket, if one was published.
    async fn read_record(store: &dyn ObjectStoreBackend) -> Option<CompactionRecord> {
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
        let key = rec_keys.first()?;
        let got = store.get(key, GetRange::Full).await.expect("get record");
        Some(CompactionRecord::decode(got.data.as_ref()).expect("decode record"))
    }

    /// Every series id carried by an object's catalog, in stored order.
    fn series_ids_of(obj: &[u8]) -> Vec<[u8; 16]> {
        let limits = ReaderLimits::default();
        let loc = ravel_segment::open_from_full(obj, limits).expect("open object");
        let entries = ravel_segment::decode_catalog_v5(&loc.footer, obj, limits).expect("catalog");
        entries.iter().map(|e| e.entry.series_id.0).collect()
    }

    /// Every scalar sample an RSEG object carries, as a canonical multiset of
    /// `(series_id, ts_ns, value_bits)` -- value compared by bit pattern, never
    /// `==`, per the storage-path float rule. Decodes the whole object (catalog
    /// plus every run's TS/VAL pages) so a round-trip check can assert real data
    /// survived, not just that the sample counts matched.
    fn decode_scalar_multiset(obj: &[u8]) -> Vec<([u8; 16], i64, u64)> {
        use ravel_segment::{ValueKind, decode_run_pages_soa, plan_ranges_v4};
        let limits = ReaderLimits::default();
        let loc = ravel_segment::open_from_full(obj, limits).expect("open object");
        let entries = ravel_segment::decode_catalog_v5(&loc.footer, obj, limits).expect("catalog");
        let refs: Vec<&ravel_segment::SeriesEntryV4> = entries.iter().collect();
        let planned = plan_ranges_v4(&loc.footer, &refs).expect("plan ranges");
        let mut planned = planned.into_iter();
        let mut out = Vec::new();
        for entry in &entries {
            let series_id = entry.entry.series_id;
            for run in &entry.runs {
                let range = planned.next().expect("a range per run");
                assert!(
                    matches!(entry.entry.value_kind, ValueKind::Scalar),
                    "helper only handles scalar series"
                );
                let ts =
                    &obj[range.ts_range.0 as usize..(range.ts_range.0 + range.ts_range.1) as usize];
                let val = &obj
                    [range.val_range.0 as usize..(range.val_range.0 + range.val_range.1) as usize];
                let mut scratch = Vec::new();
                let mut timestamps = Vec::new();
                let mut values = Vec::new();
                decode_run_pages_soa(
                    &series_id,
                    run,
                    ts,
                    val,
                    limits,
                    &mut scratch,
                    &mut timestamps,
                    &mut values,
                )
                .expect("decode run");
                for (ts_ns, value) in timestamps.into_iter().zip(values) {
                    out.push((series_id.0, ts_ns, value.to_bits()));
                }
            }
        }
        out.sort();
        out
    }

    /// The migration round trip: two real v7 L0 objects decode, re-encode, and
    /// publish into a current-version L1 part carrying the same series content,
    /// with the record-count conserved exactly.
    #[tokio::test]
    async fn migrate_round_trips_and_conserves_content() {
        let store = MemoryStore::new();
        let a = seed(
            &store,
            1,
            vec![series("alpha", &[(10, 1.0)]), series("gamma", &[(30, 3.0)])],
        )
        .await;
        let b = seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = migrate_bucket_format(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            FUTURE_VERSION,
        )
        .await
        .expect("migrate");

        let (parts, publish) = match outcome {
            MigrateOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(publish, PublishOutcome::Published);
        assert_eq!(parts, 1, "small corpus fits one part");

        let record = read_record(&store).await.expect("a record was published");
        assert_eq!(record.level, 1);
        assert_eq!(record.parts.len(), 1);
        let part = &record.parts[0];
        assert_eq!(
            part.segment_format_version, VERSION_V7 as u32,
            "the output is stamped the current writer version"
        );

        // Record count conserved: the L1 part carries every input sample.
        let input_samples: u64 = [&a, &b]
            .iter()
            .map(|obj| {
                let loc = ravel_segment::open_from_full(obj, ReaderLimits::default())
                    .expect("open input");
                loc.footer.sample_count
            })
            .sum();
        assert_eq!(part.sample_count, input_samples, "sample count conserved");

        // Series content conserved: the output's series ids are the union of
        // the inputs'.
        let part_key = keys::reconstruct_l1_part_key(&record, part).expect("part key");
        let part_bytes = store
            .get(&part_key, GetRange::Full)
            .await
            .expect("get part")
            .data;
        let mut got = series_ids_of(&part_bytes);
        got.sort();
        let mut want = vec![
            series_id("alpha").0,
            series_id("beta").0,
            series_id("gamma").0,
        ];
        want.sort();
        assert_eq!(got, want, "L1 carries exactly the inputs' series");
    }

    /// Slice B headline (ADR-0066 decision 5): the synthetic-N-1 mixed-fleet
    /// convergence case. An RSEG object recorded *below* the current output
    /// version is genuinely decoded and re-encoded to the current version by
    /// the rewrite primitive -- not refused (the pre-slice-B guard), and not
    /// verbatim-copied under a mislabeled trailer.
    ///
    /// No real N-1 RSEG version has ever shipped, so the "old" object is built
    /// the way slice A's window tests use a synthetic version number
    /// (`n_and_prev_window_is_exactly_two_wide_with_a_floor`): real, fully
    /// decodable v7 bytes under a commit record that records an older version.
    /// The reader accepts the bytes (they are real v7); the compactor's
    /// `build_parts` sees an input recorded below the output version and routes
    /// its runs through the decode-and-re-encode path
    /// ([`crate::build::build_parts`]'s `migrate_keys` branch) rather than the
    /// verbatim page copy; every sample round-trips and the record count is
    /// conserved exactly by the shared publish gate.
    ///
    /// Flip proof (non-vacuous): against `RsegCodec::validate_rewrite_inputs`'s
    /// pre-slice-B refusal (reject any input recorded `!= OUTPUT_FORMAT_VERSION`)
    /// this test fails at `expect("migrate")` -- the migration returns the
    /// guard's `Invariant` error instead of `Rewritten`. Verified by running it
    /// against the old guard before the build/codec change landed.
    #[tokio::test]
    async fn migrates_an_input_recorded_below_the_output_version() {
        let store = MemoryStore::new();
        let old = VERSION_V7 as u32 - 1;
        // Two inputs recorded below the output version, over real v7 bytes; one
        // carries two series (one of them with two samples) so the round-trip
        // check spans multiple series and a multi-sample run.
        let a = seed_with_version(
            &store,
            1,
            vec![
                series("alpha", &[(10, 1.0), (11, 1.5)]),
                series("gamma", &[(30, 3.0)]),
            ],
            old,
        )
        .await;
        let b = seed_with_version(&store, 2, vec![series("beta", &[(20, 2.0)])], old).await;

        let clock = FixedClock::new(sealed_now_ns());
        // Target the current output version: eligibility is `recorded < target`,
        // and the rewrite re-encodes to the current version (== target), so the
        // output lands at the target -- the convergence RSEG could not reach
        // before slice B, because its verbatim path could never lift an
        // older-recorded input to the current version.
        let outcome = migrate_bucket_format(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            VERSION_V7 as u32,
        )
        .await
        .expect("migrate");

        let (parts, publish) = match outcome {
            MigrateOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(publish, PublishOutcome::Published);
        assert_eq!(parts, 1, "small corpus fits one part");

        let record = read_record(&store).await.expect("a record was published");
        assert_eq!(record.parts.len(), 1);
        let part = &record.parts[0];
        assert_eq!(
            part.segment_format_version, VERSION_V7 as u32,
            "the re-encoded output is stamped the current writer version"
        );

        // Record count conserved: the L1 part carries every input sample. The
        // input total is read from the objects' own footers (the real v7 bytes),
        // independent of the commit records' (deliberately older) version tag.
        let input_samples: u64 = [&a, &b]
            .iter()
            .map(|obj| {
                ravel_segment::open_from_full(obj, ReaderLimits::default())
                    .expect("open input")
                    .footer
                    .sample_count
            })
            .sum();
        assert_eq!(part.sample_count, input_samples, "sample count conserved");

        // Real data round-trips: the L1 part's decoded (series, ts, value-bits)
        // multiset equals the union of the inputs', proving the decode-and-
        // re-encode preserved every sample's value, not merely its count.
        let part_key = keys::reconstruct_l1_part_key(&record, part).expect("part key");
        let part_bytes = store
            .get(&part_key, GetRange::Full)
            .await
            .expect("get part")
            .data;
        let mut got = decode_scalar_multiset(&part_bytes);
        let mut want: Vec<([u8; 16], i64, u64)> = decode_scalar_multiset(&a)
            .into_iter()
            .chain(decode_scalar_multiset(&b))
            .collect();
        got.sort();
        want.sort();
        assert_eq!(
            got, want,
            "every input sample round-trips through the rewrite"
        );
    }

    /// Fail-closed-on-newer (ADR-0066 decision 2): an input recorded *above* the
    /// current output version -- newer than this build can read or write -- is
    /// refused before any decode or PUT, never mislabeled downward. This is the
    /// half of the old below-output refusal that survives slice B: the guard
    /// stopped rejecting older (migratable) inputs and now rejects only
    /// newer-than-writable ones.
    #[tokio::test]
    async fn rseg_rewrite_rejects_input_recorded_above_output_version() {
        let store = MemoryStore::new();
        seed_with_version(
            &store,
            1,
            vec![series("alpha", &[(10, 1.0)])],
            VERSION_V7 as u32 + 1,
        )
        .await;
        let listing = list_bucket(&store, &bucket()).await.expect("list");
        let clock = FixedClock::new(sealed_now_ns());

        let err = rewrite_and_publish::<RsegCodec>(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            &listing.commit_keys,
            conserve_exact(),
            sealed_now_ns(),
        )
        .await
        .expect_err("an input recorded above the output version must be rejected");
        assert!(
            matches!(err, MaintainError::Invariant(_)),
            "expected the validate_rewrite_inputs fail-closed-on-newer guard, got {err:?}"
        );
        assert!(
            read_record(&store).await.is_none(),
            "a rejected input set publishes nothing and PUTs nothing"
        );
    }

    /// A conservation predicate that rejects the built parts' true count aborts
    /// the publish: the run returns the typed [`MaintainError::ConservationViolation`]
    /// and no compaction record is written, so the L0 inputs stay live -- the
    /// same abort posture compaction has (services/ravel-server maintain records
    /// it as a conservation abort).
    #[tokio::test]
    async fn conservation_failure_aborts_publish() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0)])]).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;

        let listing = list_bucket(&store, &bucket()).await.expect("list");
        let clock = FixedClock::new(sealed_now_ns());

        // Demand exactly one dropped record where the rewrite drops none: the
        // exact output count is rejected, so the publish aborts.
        let err = rewrite_and_publish::<RsegCodec>(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            &listing.commit_keys,
            |input: u64, output: u64| input == output + 1,
            sealed_now_ns(),
        )
        .await
        .expect_err("a rejected count must abort");
        assert!(
            matches!(err, MaintainError::ConservationViolation { .. }),
            "expected ConservationViolation, got {err:?}"
        );
        assert!(
            read_record(&store).await.is_none(),
            "an aborted publish writes no compaction record; the L0 inputs stay live"
        );
    }

    /// A crash-and-rerun converges at the same content-addressed key: the
    /// second run rebuilds identical parts, its record PUT sees the winner, and
    /// it reports convergence rather than a second record. Mirrors compaction's
    /// stateless-and-idempotent property.
    #[tokio::test]
    async fn crash_rerun_converges_at_content_addressed_key() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0)])]).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;
        let listing = list_bucket(&store, &bucket()).await.expect("list");
        let clock = FixedClock::new(sealed_now_ns());

        let first = rewrite_and_publish::<RsegCodec>(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            &listing.commit_keys,
            conserve_exact(),
            sealed_now_ns(),
        )
        .await
        .expect("first rewrite");
        assert_eq!(first.publish, PublishOutcome::Published);
        let record_after_first = read_record(&store).await.expect("record after first");

        // Re-run from scratch, as a crashed run would.
        let second = rewrite_and_publish::<RsegCodec>(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            &listing.commit_keys,
            conserve_exact(),
            sealed_now_ns(),
        )
        .await
        .expect("second rewrite");
        assert_eq!(
            second.publish,
            PublishOutcome::Converged { parts_repaired: 0 },
            "the rerun converges on the first run's record, repairing nothing"
        );

        let record_after_second = read_record(&store).await.expect("record after second");
        assert_eq!(
            record_after_first.parts, record_after_second.parts,
            "the same content-addressed parts; no second object set"
        );
        // Exactly one record object in the bucket.
        let prefix =
            keys::commit_shard_hour_prefix(&bucket().tenant_hash, Signal::Metrics, SHARD, HOUR)
                .expect("prefix");
        let record_keys: Vec<String> = list_all(&store, &prefix)
            .await
            .expect("list")
            .into_iter()
            .map(|m| m.key)
            .filter(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .collect();
        assert_eq!(record_keys.len(), 1, "exactly one compaction record");
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

    /// The format migration refuses a bucket that already holds a live erasure
    /// rewrite record, for the same reason compaction does: the rewrite's
    /// outputs deliberately lack records its inputs contain, so a migration
    /// record built from those same L0 inputs would leave the catalog serving
    /// two record sets over one bucket (ADR-0064 decision 3 point 5).
    ///
    /// The guarded production line is the `RewritePresent` early return in
    /// `migrate_bucket_format_scoped`. With that return removed the outcome is
    /// `Rewritten { .. }`, because every seeded input is below `FUTURE_VERSION`.
    #[tokio::test]
    async fn migrate_refuses_a_bucket_holding_a_live_rewrite_record() {
        let store = MemoryStore::new();
        let base = i64::from(HOUR) * NS_PER_HOUR;
        seed(&store, 1, vec![series("alpha", &[(base + 1_000, 1.0)])]).await;
        seed(&store, 2, vec![series("beta", &[(base + 2_000, 2.0)])]).await;
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
        let outcome = migrate_bucket_format(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            FUTURE_VERSION,
        )
        .await
        .expect("migrate");

        assert_eq!(
            outcome,
            MigrateOutcome::RewritePresent,
            "a live rewrite record refuses the format migration"
        );
        assert!(
            !matches!(outcome, MigrateOutcome::Rewritten { .. }),
            "nothing was migrated"
        );
        assert!(
            read_record(&store).await.is_none(),
            "no second record set was published over the rewritten inputs"
        );
        let listing = list_bucket(&store, &bucket()).await.expect("list");
        assert_eq!(listing.rewrite_record_keys.len(), 1);
        assert_eq!(
            listing.commit_keys.len(),
            2,
            "both L0 inputs stay live behind the rewrite record"
        );
    }

    /// The migration is a no-op when every input is already at or above the
    /// floor: passing the current writer version as the target leaves the
    /// bucket untouched.
    #[tokio::test]
    async fn migrate_up_to_date_is_a_noop() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0)])]).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = migrate_bucket_format(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket(),
            VERSION_V7 as u32,
        )
        .await
        .expect("migrate");
        assert_eq!(outcome, MigrateOutcome::UpToDate);
        assert!(
            read_record(&store).await.is_none(),
            "an up-to-date bucket publishes nothing"
        );
    }

    /// The predicate is genuinely pluggable: a drop-aware predicate that
    /// accepts an output count one short of the inputs (the erasure shape)
    /// publishes, where the exact-match default would abort. Driven at the
    /// publish layer so a deliberately-reduced count can be supplied directly.
    #[tokio::test]
    async fn pluggable_predicate_accepts_a_reduced_count() {
        use crate::build::{BuiltPart, OUTPUT_FORMAT_VERSION};
        use crate::publish::publish_record_with_conservation;
        use ravel_proto::commit::v1::CompactionPart;

        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0), (11, 1.1)])]).await;
        seed(&store, 2, vec![series("beta", &[(20, 2.0)])]).await;
        let b = bucket();
        let listing = list_bucket(&store, &b).await.expect("list");
        let inputs = load_inputs(&store, &b, &listing.commit_keys, 1)
            .await
            .expect("inputs");
        let hash = input_set_hash(&inputs);
        let input_total: u64 = inputs.iter().map(|i| i.record.sample_count).sum();

        // A synthetic part carrying one fewer record than the inputs: the
        // erasure case, where exactly one record was dropped. The bytes are
        // irrelevant to the publish gate, which reads `part.sample_count`.
        let part = BuiltPart {
            key: "l1/synthetic".to_string(),
            bytes: Some(Bytes::new()),
            put_already_existed: false,
            part: CompactionPart {
                part_index: 0,
                content_hash: vec![0u8; 32],
                sample_count: input_total - 1,
                segment_format_version: OUTPUT_FORMAT_VERSION,
                ..CompactionPart::default()
            },
        };
        let clock = FixedClock::new(sealed_now_ns());

        // The exact-match default rejects the short count.
        let exact = publish_record_with_conservation(
            &store,
            &CompactorConfig::default(),
            &clock,
            &b,
            &inputs,
            &hash,
            std::slice::from_ref(&part),
            sealed_now_ns(),
            conserve_exact(),
        )
        .await;
        assert!(
            matches!(exact, Err(MaintainError::ConservationViolation { .. })),
            "exact conservation must reject a dropped record, got {exact:?}"
        );

        // The drop-aware predicate accepts it and the record publishes: the
        // primitive honored the supplied predicate rather than the default.
        let dropped = 1u64;
        let outcome = publish_record_with_conservation(
            &store,
            &CompactorConfig::default(),
            &clock,
            &b,
            &inputs,
            &hash,
            std::slice::from_ref(&part),
            sealed_now_ns(),
            move |input: u64, output: u64| input == output + dropped,
        )
        .await
        .expect("drop-aware predicate must accept the reduced count");
        assert_eq!(outcome, PublishOutcome::Published);
    }
}
