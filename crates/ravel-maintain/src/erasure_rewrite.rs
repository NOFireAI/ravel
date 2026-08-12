//! Selective-erasure rewrite pass for the metrics (RSEG) signal (ADR-0064
//! decision 3, epic EJ, issue #754). Logs and spans are EJ-T4b, a follow-up;
//! [`crate::memo_snapshot`] promptness invalidation on publish is also a
//! follow-up (deferred per this task's scope) -- without it a rewritten
//! bucket's interior zone re-verifies on the normal cadence rather than
//! immediately, which is correct, just slower.
//!
//! ## Why this is not [`crate::rewrite::rewrite_and_publish`]
//!
//! [`crate::rewrite`]'s module doc and this function's sibling
//! [`crate::rewrite::rewrite_and_publish`] both say EJ should reuse that
//! primitive by supplying a drop-aware [`crate::publish::ConservationPredicate`]
//! and nothing else. That is no longer possible without a code change this
//! task does not make: [`crate::publish::publish_record_with_conservation`]
//! unconditionally builds and publishes a `CompactionRecord` (`l1.<hash>.cmt`)
//! and has no path to a `RewriteRecord`'s `drops`/`superseded_record_key`
//! fields at all. That doc comment predates the 2026-08-08 ADR-0064
//! amendment that added `superseded_record_key` and the drops-based
//! structure; it was true of an older shape of `RewriteRecord` that this
//! amendment replaced. This module therefore publishes through its own
//! [`publish_rewrite_record`], structurally mirroring
//! `publish_record_with_conservation`'s shape (abandonment deadline,
//! conservation gate, `CreateIfAbsent`, `AlreadyExists` convergence) rather
//! than calling it. Flagged here and in the task's final report rather than
//! silently patched, per this repo's contradiction-reporting rule; fixing the
//! stale doc comment and/or generalizing the shared primitive to cover both
//! record shapes is left to a follow-up so as not to risk `rewrite.rs`'s
//! existing, well-tested format-migration callers in this change.
//!
//! ## Why this does not reuse [`crate::build::build_parts`] either
//!
//! `build_parts` copies compaction's verbatim page bytes without decoding
//! samples, which is correct for compaction (nothing is dropped) but cannot
//! drop individual erased records. This module decodes every surviving and
//! erased sample via [`ravel_segment::decode_run_pages_soa`] /
//! [`ravel_segment::decode_run_histogram_pages`], filters, and re-encodes
//! survivors via [`ravel_segment::encode_run_v4`]. It also does not replicate
//! `build_parts`'s size-capped multi-part splitting or its coalesced ranged
//! fetch: every live input/part is read with one whole-object GET and every
//! bucket's survivors are written as a single output part. Both are
//! deliberate, transparent scope reductions for a first correct
//! implementation (erasure rewrites are rare maintenance passes, not a
//! request-hot path); a follow-up can adopt `build.rs`'s batching machinery
//! without changing this module's public shape.
//!
//! ## Exemplars are dropped, not carried forward (open gap)
//!
//! ADR-0047 decision 3 says exemplars ride along verbatim through compaction
//! and format-migration, with only `series_index` remapped. This module does
//! not do that: [`build_rewrite`] calls
//! `SegmentWriter::write_v5_with_exemplars` with an empty exemplar list, so
//! every input's exemplars (including ones belonging to series this rewrite
//! never touches) are dropped from the output. This does not violate the
//! sample-count conservation gate (exemplars are not counted samples), but it
//! is a real, silent loss of exemplar data on any bucket this pass rewrites.
//! `read.rs`'s [`crate::read::load_catalog_from_object`] already loads each
//! input's `InputCatalog::exemplars`, so wiring correct carry-forward (drop
//! only exemplars whose named series has zero surviving samples, remap the
//! rest through the same series-id resolution `build_parts` relies on) is
//! straightforward for a follow-up but is not done here: this task's
//! dispatch does not name exemplars among its deliverables, and reusing
//! `build_parts`'s per-batch exemplar assignment machinery would have meant
//! adopting its batching complexity too, which the scope reduction above
//! deliberately avoids. Flagged here and in the task's final report.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::Bytes;
use prost::Message;
use ravel_commit::erasure;
use ravel_commit::keys;
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum, list_all,
};
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionPart, CompactionRecord, ErasurePredicateMatcher,
    ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_segment::{
    CompactionMetaV4, IngestBounds, ReaderLimits, RunEntry, RunInputV4, RunValuePageV4,
    SegmentIdentity, SegmentWriter, SeriesInputV4, SeriesValues, ValueKind,
    decode_run_histogram_pages, decode_run_pages_soa, encode_run_v4,
};
use ravel_types::{LabelSet, Sample, Signal, TenantHash};

use crate::bucket::Bucket;
use crate::build::{BuiltPart, OUTPUT_FORMAT_VERSION};
use crate::clock::Clock;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::publish::PublishOutcome;
use crate::read::{BucketListing, InputCatalog, RunPlan, SeriesPlan};
use crate::sweep::LeaseCheck;

/// One pending selective-erasure request: its `.dreq` key and decoded body.
#[derive(Debug, Clone)]
pub struct PendingErasureRequest {
    pub request_key: String,
    pub request: ErasureRequest,
}

/// List every erasure request on `(tenant_hash, signal)` that is still
/// pending -- a `.dreq` object exists and no matching `.done` completion has
/// been written yet (ADR-0064 decision 1/5). Both suffixes share
/// [`keys::del_prefix`], so this is one LIST.
///
/// An entry under the prefix that is neither a `.dreq` nor a `.done` is
/// layout drift, not silently skipped (matches [`crate::read::list_bucket`]'s
/// fail-loud discipline for unrecognized shapes).
pub async fn pending_erasure_requests(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
) -> Result<Vec<PendingErasureRequest>> {
    let prefix = keys::del_prefix(tenant_hash, signal);
    let metas = list_all(store, &prefix).await?;

    let mut done_ids = HashSet::new();
    let mut dreq_keys = Vec::new();
    for meta in &metas {
        if meta.key.ends_with(".done") {
            let parsed = keys::parse_erasure_completion_key(&meta.key)?;
            done_ids.insert(parsed.request_id);
        } else if meta.key.ends_with(".dreq") {
            dreq_keys.push(meta.key.clone());
        } else {
            return Err(MaintainError::UnknownBucketEntry(meta.key.clone()));
        }
    }

    let mut out = Vec::with_capacity(dreq_keys.len());
    for key in dreq_keys {
        let parsed = keys::parse_erasure_request_key(&key)?;
        if done_ids.contains(&parsed.request_id) {
            continue;
        }
        let got = store.get(&key, GetRange::Full).await?;
        let request = erasure::decode_request(&got.data)?;
        keys::verify_erasure_request_key(&request, &key)?;
        out.push(PendingErasureRequest {
            request_key: key,
            request,
        });
    }
    out.sort_by(|a, b| a.request_key.cmp(&b.request_key));
    Ok(out)
}

/// A minimal, semantically-faithful duplicate of
/// `ravel_query::erasure::ErasurePredicate`'s metric-series matching rule,
/// reimplemented here because `ravel-maintain` cannot depend on
/// `ravel-query` (the dependency runs the other way: `ravel-query` depends
/// on `ravel-maintain`). This is a genuine cross-crate duplication forced by
/// that dependency direction, not a design choice -- flagged in the task's
/// final report as a required scope contradiction.
///
/// Semantics (mirroring `ravel_query::erasure::ErasurePredicate`, confirmed
/// against its test suite): a sample is dropped iff its series' labels
/// satisfy every matcher (conjunction; an empty matcher set matches nothing,
/// fail-safe) AND its timestamp falls in the half-open `[window_start_ns,
/// window_end_ns)` window. A "windowless" request has both bounds zero,
/// which the zero-as-unset convention below treats as unrestricted on that
/// side -- so a windowless predicate drops every sample of a matching
/// series, which is exactly "drop the whole series", with no separate code
/// path required.
#[derive(Debug, Clone)]
pub struct ErasureMatcher {
    matchers: Vec<(String, String)>,
    window_start_ns: i64,
    window_end_ns: i64,
}

impl ErasureMatcher {
    pub fn from_request(request: &ErasureRequest) -> Self {
        ErasureMatcher {
            matchers: request
                .predicate
                .iter()
                .map(|m: &ErasurePredicateMatcher| (m.key.clone(), m.value.clone()))
                .collect(),
            window_start_ns: request.window_start_ns,
            window_end_ns: request.window_end_ns,
        }
    }

    /// Whether this predicate carries an event-time restriction at all.
    /// Zero on both sides is the documented "no range restriction" sentinel
    /// (proto3 scalar no-presence, `ErasureRequest.window_start_ns` doc).
    pub fn has_window(&self) -> bool {
        self.window_start_ns != 0 || self.window_end_ns != 0
    }

    /// Conjunction of exact-match matchers against `labels`. Empty matchers
    /// matches nothing (fail-safe: `validate_request` also rejects an empty
    /// predicate, so this only guards a defensively-constructed matcher).
    pub fn matches_labels(&self, labels: &LabelSet) -> bool {
        if self.matchers.is_empty() {
            return false;
        }
        self.matchers
            .iter()
            .all(|(k, v)| labels.get(k).is_some_and(|found| found == v.as_str()))
    }

    /// Half-open `[window_start_ns, window_end_ns)`; zero on either side is
    /// unset (no restriction on that side).
    pub fn ts_in_window(&self, ts_ns: i64) -> bool {
        let after_start = self.window_start_ns == 0 || ts_ns >= self.window_start_ns;
        let before_end = self.window_end_ns == 0 || ts_ns < self.window_end_ns;
        after_start && before_end
    }

    /// Whether this predicate drops a sample at `ts_ns` belonging to a
    /// series with `labels`.
    pub fn drops_sample(&self, labels: &LabelSet, ts_ns: i64) -> bool {
        self.matches_labels(labels) && self.ts_in_window(ts_ns)
    }
}

/// Record-derived prefilter: could the bucket's *live* records' actual
/// event-time span, `[min_event_ts_ns, max_event_ts_ns]`, overlap
/// `request`'s event-time window at all? Rejects before the expensive
/// whole-object data GETs `build_rewrite` pays for, but -- unlike a
/// key-derived check -- only after the cheap record-level metadata
/// ([`live_input_event_bounds`]) is in hand, mirroring the sweep's own
/// age/horizon-gate prefilters (`object_age_ns` and friends in `sweep.rs`)
/// in spirit: reject on the cheapest metadata that can answer the question
/// correctly, not the cheapest metadata available at all.
///
/// This deliberately does NOT use `bucket.start_ns()/end_ns()` (the
/// bucket's *ingest*-hour range, derived from the flush-open clock): ingest
/// time and sample event time are decoupled for backfilled or
/// clock-skewed writes, so a windowed request's event-time window can miss
/// a bucket's ingest hour while still matching samples the bucket
/// physically stores. Using ingest bounds here previously produced
/// physical under-erasure that diverged from EJ-T3's scan-time exclusion
/// (which always filters on the real sample `ts_ns`): the query path would
/// correctly hide the matching samples while this prefilter skipped the
/// bucket that should have erased them (a GDPR gap, ADR-0064).
///
/// A windowless request (`has_window() == false`) can match a series
/// regardless of when its samples were recorded, so it overlaps every
/// bucket that has any live record at all. A bucket whose live record set
/// carries no samples (`min_event_ts_ns > max_event_ts_ns`, the empty-parts
/// sentinel [`live_input_event_bounds`] returns) has nothing left to
/// physically erase, so this always returns `false` for a windowed
/// request in that case regardless of the window.
pub fn bucket_may_overlap(min_event_ts_ns: i64, max_event_ts_ns: i64, request: &ErasureRequest) -> bool {
    let has_window = request.window_start_ns != 0 || request.window_end_ns != 0;
    if !has_window {
        return true;
    }
    if min_event_ts_ns > max_event_ts_ns {
        return false;
    }
    let starts_before_records_end =
        request.window_start_ns == 0 || request.window_start_ns <= max_event_ts_ns;
    let ends_after_records_start =
        request.window_end_ns == 0 || request.window_end_ns > min_event_ts_ns;
    starts_before_records_end && ends_after_records_start
}

/// GET, decode, and key-verify a compaction record (ADR-0010 §7). Duplicated
/// from `sweep.rs`'s private helper of the same shape: that one is not
/// `pub(crate)`, and this module's task scope excludes touching `sweep.rs`.
async fn get_compaction_record(
    store: &dyn ObjectStoreBackend,
    key: &str,
) -> Result<CompactionRecord> {
    let got = store.get(key, GetRange::Full).await?;
    let record = CompactionRecord::decode(got.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("compaction record decode failed: {e}")))?;
    keys::verify_compaction_record_key(&record, key)?;
    Ok(record)
}

/// GET, decode, validate, and key-verify a rewrite record (ADR-0064 decision
/// 3, ADR-0010 §7). `decode_rewrite` also re-verifies the record's own
/// `input_set_hash` and `superseded_record_key` bucket-match on decode.
/// Duplicated from `sweep.rs`'s private helper for the same reason as
/// [`get_compaction_record`].
async fn get_rewrite_record(store: &dyn ObjectStoreBackend, key: &str) -> Result<RewriteRecord> {
    let got = store.get(key, GetRange::Full).await?;
    let record = erasure::decode_rewrite(got.data.as_ref())
        .map_err(|e| MaintainError::Invariant(format!("rewrite record decode failed: {e}")))?;
    keys::verify_rewrite_record_key(&record, key)?;
    Ok(record)
}

/// One decoded compaction-or-rewrite record present in a bucket's listing,
/// tagged by which it is (the two share `parts`/`created_unix_ns` shape but
/// not a common proto type).
#[derive(Debug, Clone)]
enum LiveRecordBody {
    Compaction(CompactionRecord),
    Rewrite(RewriteRecord),
}

impl LiveRecordBody {
    fn parts(&self) -> &[ravel_proto::commit::v1::CompactionPart] {
        match self {
            LiveRecordBody::Compaction(r) => &r.parts,
            LiveRecordBody::Rewrite(r) => &r.parts,
        }
    }

    fn created_unix_ns(&self) -> i64 {
        match self {
            LiveRecordBody::Compaction(r) => r.created_unix_ns,
            LiveRecordBody::Rewrite(r) => r.created_unix_ns,
        }
    }

    fn reconstruct_part_key(
        &self,
        part: &ravel_proto::commit::v1::CompactionPart,
    ) -> Result<String> {
        let key = match self {
            LiveRecordBody::Compaction(r) => keys::reconstruct_l1_part_key(r, part)?,
            LiveRecordBody::Rewrite(r) => keys::reconstruct_rewrite_part_key(r, part)?,
        };
        Ok(key)
    }
}

/// Which live generation currently owns a bucket's record set, resolved by
/// the one-hop `superseded_record_key` rule: the live key is whichever
/// compaction/rewrite record present in the bucket's listing is not named by
/// any other present record's `superseded_record_key`. This is deliberately
/// NOT `ravel_catalog::catalog::resolve_rewrite_supersession`'s job (the full
/// predecessor-chain chase a query snapshot needs to build its L0-input
/// exclusion set): a rewrite pass only ever needs the immediate live
/// generation's own parts to decode, which is one hop, never a chase, because
/// each generation's rewrite record fully supersedes everything before it.
#[derive(Debug, Clone)]
enum LiveRecord {
    /// The bucket has never been compacted or rewritten: the live set is the
    /// raw L0 inputs in [`BucketListing::commit_keys`].
    RawL0,
    /// `key` is the live compaction/rewrite record, decoded as `body`.
    Existing { key: String, body: LiveRecordBody },
}

/// Resolve [`LiveRecord`] for one bucket listing. [`MaintainError::NoLiveRecord`]
/// / [`MaintainError::MultipleLiveRecords`] fire when the bucket's compaction
/// and rewrite records do not form the resolver's guaranteed
/// exactly-one-live-record chain -- a fatal invariant breach, never silently
/// picked around.
async fn resolve_live_record(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    listing: &BucketListing,
) -> Result<LiveRecord> {
    if listing.compaction_record_keys.is_empty() && listing.rewrite_record_keys.is_empty() {
        return Ok(LiveRecord::RawL0);
    }

    let mut decoded: Vec<(String, LiveRecordBody)> = Vec::new();
    for key in &listing.compaction_record_keys {
        let record = get_compaction_record(store, key).await?;
        decoded.push((key.clone(), LiveRecordBody::Compaction(record)));
    }
    for key in &listing.rewrite_record_keys {
        let record = get_rewrite_record(store, key).await?;
        decoded.push((key.clone(), LiveRecordBody::Rewrite(record)));
    }

    let superseded: HashSet<String> = decoded
        .iter()
        .filter_map(|(_, body)| match body {
            LiveRecordBody::Rewrite(r) if !r.superseded_record_key.is_empty() => {
                Some(r.superseded_record_key.clone())
            }
            _ => None,
        })
        .collect();

    let mut live: Vec<(String, LiveRecordBody)> = decoded
        .into_iter()
        .filter(|(key, _)| !superseded.contains(key.as_str()))
        .collect();

    match live.len() {
        1 => {
            let (key, body) = live.remove(0);
            Ok(LiveRecord::Existing { key, body })
        }
        0 => Err(MaintainError::NoLiveRecord {
            bucket_prefix: bucket_prefix_for_error(bucket),
            live_count: 0,
        }),
        _ => Err(MaintainError::MultipleLiveRecords {
            bucket_prefix: bucket_prefix_for_error(bucket),
            live_keys: live.into_iter().map(|(k, _)| k).collect(),
        }),
    }
}

fn bucket_prefix_for_error(bucket: &Bucket) -> String {
    keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap_or_else(|_| "<unreconstructable bucket prefix>".to_string())
}

/// Every object key in a bucket's listing that legal hold might name: the raw
/// L0 commits, any existing compaction/rewrite record, and (for symmetry with
/// a held bucket already rewritten once) the tombstone slot. A rewrite pass
/// must check all of them before its first GET of segment/part bytes, not
/// just the live record's own key, because [`crate::legal_hold::LegalHoldCheck`]
/// holds by *prefix* (ADR tenant/shard scoping) rather than by a specific
/// live-record key, and a superseded-but-still-listed key is exactly the kind
/// of leftover a hold is meant to still catch during the hold window.
fn bucket_is_held(listing: &BucketListing, lease: &dyn LeaseCheck) -> bool {
    listing.commit_keys.iter().any(|k| lease.is_protected(k))
        || listing
            .compaction_record_keys
            .iter()
            .any(|k| lease.is_protected(k))
        || listing
            .rewrite_record_keys
            .iter()
            .any(|k| lease.is_protected(k))
}

/// A bucket's live record set, resolved (via [`resolve_live_record`]) down
/// to whatever cheap per-record metadata already carries real event-time
/// bounds -- stopping short of the per-input catalog fetch
/// [`load_live_catalogs_and_target`] pays for, so [`erasure_rewrite_bucket`]
/// can apply [`bucket_may_overlap`] and bail out before that heavier GET.
#[derive(Debug)]
enum LiveInputs {
    /// The bucket has never been compacted or rewritten: every raw L0
    /// input's own decoded [`ravel_commit::record`] (a small, already-GET
    /// metadata object, not the segment data object it names).
    RawL0(Vec<crate::read::InputRecord>),
    /// `key` is the live compaction/rewrite record, decoded as `body`; its
    /// `parts()` already carry each part's `min_event_ts_ns`/
    /// `max_event_ts_ns` (`ravel_proto::commit::v1::CompactionPart`).
    Existing { key: String, body: LiveRecordBody },
}

/// Resolve [`LiveInputs`], paying for the small per-record metadata GET
/// (raw L0 commit records, or the single already-required compaction/rewrite
/// record) but not yet the larger per-input catalog fetch.
async fn resolve_live_inputs(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    listing: &BucketListing,
) -> Result<LiveInputs> {
    match resolve_live_record(store, bucket, listing).await? {
        LiveRecord::RawL0 => {
            let inputs = crate::read::load_inputs(store, bucket, &listing.commit_keys).await?;
            Ok(LiveInputs::RawL0(inputs))
        }
        LiveRecord::Existing { key, body } => Ok(LiveInputs::Existing { key, body }),
    }
}

/// `[min_event_ts_ns, max_event_ts_ns]` spanning every record in `live` --
/// the real event-time bounds [`bucket_may_overlap`]'s GDPR fix needs,
/// since ingest hour and event time are decoupled for backfilled or
/// clock-skewed samples. An empty live record set (a bucket rewritten down
/// to zero parts) returns the `min > max` empty-range sentinel
/// [`bucket_may_overlap`] treats as "nothing here to overlap."
fn live_input_event_bounds(live: &LiveInputs) -> (i64, i64) {
    let mut min_event_ts_ns = i64::MAX;
    let mut max_event_ts_ns = i64::MIN;
    match live {
        LiveInputs::RawL0(inputs) => {
            for input in inputs {
                min_event_ts_ns = min_event_ts_ns.min(input.record.min_event_ts_ns);
                max_event_ts_ns = max_event_ts_ns.max(input.record.max_event_ts_ns);
            }
        }
        LiveInputs::Existing { body, .. } => {
            for part in body.parts() {
                min_event_ts_ns = min_event_ts_ns.min(part.min_event_ts_ns);
                max_event_ts_ns = max_event_ts_ns.max(part.max_event_ts_ns);
            }
        }
    }
    (min_event_ts_ns, max_event_ts_ns)
}

/// Resolve `live` down to both the [`InputCatalog`]s [`build_rewrite`]
/// decodes and the [`RewriteSupersession`] target the eventual
/// `RewriteRecord` names: the raw L0 inputs' own catalogs plus their
/// identities if the bucket was never compacted, or the live
/// compaction/rewrite record's own parts' catalogs plus its own key
/// otherwise. An L1/rewrite part carries no writer identity of its own, so
/// its catalog's runs are stamped with the live record's `created_unix_ns`
/// and zeroed epoch/seq -- the same nil-writer-identity convention
/// [`crate::read::load_catalog_from_object`]'s own doc comment names.
async fn load_live_catalogs_and_target(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    live: LiveInputs,
) -> Result<(Vec<InputCatalog>, RewriteSupersession)> {
    match live {
        LiveInputs::RawL0(inputs) => {
            let mut catalogs = Vec::with_capacity(inputs.len());
            for input in &inputs {
                catalogs.push(crate::read::load_input_catalog(store, config, input).await?);
            }
            let identities = inputs
                .iter()
                .map(|i| CompactionInputIdentity {
                    writer_id: i.record.writer_id.clone(),
                    writer_epoch: i.record.writer_epoch,
                    writer_seq: i.record.writer_seq,
                })
                .collect();
            Ok((catalogs, RewriteSupersession::RawL0(identities)))
        }
        LiveInputs::Existing { key, body } => {
            let created_unix_ns = body.created_unix_ns();
            let mut catalogs = Vec::with_capacity(body.parts().len());
            for part in body.parts() {
                let object_key = body.reconstruct_part_key(part)?;
                let catalog = crate::read::load_catalog_from_object(
                    store,
                    config,
                    object_key,
                    created_unix_ns,
                    0,
                    0,
                )
                .await?;
                catalogs.push(catalog);
            }
            Ok((catalogs, RewriteSupersession::Existing(key)))
        }
    }
}

/// One pending erasure request whose prefilter overlaps the bucket being
/// rewritten, paired with the label/window matcher built from its predicate.
/// ADR-0064 §4's "sibling case" rule requires every request in this slice --
/// not just the one that triggered the rewrite -- to end up with a `drops[]`
/// entry in the resulting `RewriteRecord`, even `dropped_count: 0` if nothing
/// in this bucket happened to match that particular request. Callers (the
/// driver, EJ-T7) are responsible for building this batch once per bucket,
/// covering every pending request whose [`bucket_may_overlap`] prefilter
/// passed, never one request at a time.
#[derive(Debug, Clone)]
pub struct ApplicableRequest {
    pub request_id: String,
    pub matcher: ErasureMatcher,
}

/// Fetch every distinct input object referenced by `catalogs` exactly once,
/// whole (`GetRange::Full`). A rewrite pass may need to decode any run of any
/// series against any matcher, so range-fetching individual pages
/// (`build.rs`'s coalesced-batch machinery) would save bytes but not GETs for
/// anything but a very wide bucket; the "single whole-object GET" scope
/// reduction documented at the top of this module is what keeps this
/// function this simple.
async fn fetch_whole_objects(
    store: &dyn ObjectStoreBackend,
    catalogs: &[InputCatalog],
) -> Result<HashMap<String, Bytes>> {
    let mut out = HashMap::new();
    for catalog in catalogs {
        if out.contains_key(&catalog.object_key) {
            continue;
        }
        let got = store.get(&catalog.object_key, GetRange::Full).await?;
        out.insert(catalog.object_key.clone(), got.data);
    }
    Ok(out)
}

/// Bounds-checked zero-copy slice of an absolute `(offset, len)` page range
/// out of a whole fetched object. Overflowing or overrunning range math is a
/// corrupt or truncated input, surfaced as a typed [`MaintainError::Segment`]
/// error -- never a panic (CLAUDE.md invariant), and never silently clamped.
fn slice_whole(whole: &Bytes, range: (u64, u64)) -> Result<Bytes> {
    let (offset, len) = range;
    let end = offset.checked_add(len).ok_or(MaintainError::Segment(
        ravel_segment::SegmentError::Truncated,
    ))?;
    let offset = usize::try_from(offset)
        .map_err(|_| MaintainError::Segment(ravel_segment::SegmentError::Truncated))?;
    let end_usize = usize::try_from(end)
        .map_err(|_| MaintainError::Segment(ravel_segment::SegmentError::Truncated))?;
    if end_usize > whole.len() {
        return Err(MaintainError::Segment(
            ravel_segment::SegmentError::Truncated,
        ));
    }
    Ok(whole.slice(offset..end_usize))
}

/// Copy one run's TS and VAL-or-HIST pages verbatim (no decode) out of its
/// whole fetched object. Used for a run whose series matches no applicable
/// request's labels at all: every sample survives, so there is nothing to
/// filter and no reason to pay for a decode/re-encode round trip.
fn copy_run_verbatim(object: &Bytes, run: &RunPlan) -> Result<RunInputV4> {
    let ts_page = slice_whole(object, run.ts_abs)?.to_vec();
    let page = slice_whole(object, run.page_abs)?.to_vec();
    let value_page = match run.kind {
        ValueKind::Scalar => RunValuePageV4::Scalar(page),
        ValueKind::Histogram => RunValuePageV4::Histogram(page),
    };
    Ok(RunInputV4 {
        created_unix_ns: run.created_unix_ns,
        writer_epoch: run.writer_epoch,
        writer_seq: run.writer_seq,
        min_ts_ns: run.min_ts_ns,
        max_ts_ns: run.max_ts_ns,
        sample_count: run.sample_count,
        ts_page,
        value_page,
    })
}

/// The first applicable request (in `requests`' fixed order) whose matcher
/// drops the sample at `(labels, ts_ns)`, if any. First-match-wins: a sample
/// matched by more than one pending request is attributed to whichever
/// request comes first in the batch, which is an arbitrary but stable and
/// deterministic tie-break -- the sample is dropped from the output exactly
/// once either way, which is all the conservation gate checks.
fn first_dropping_request(
    applicable: &[usize],
    requests: &[ApplicableRequest],
    labels: &LabelSet,
    ts_ns: i64,
) -> Option<usize> {
    applicable
        .iter()
        .copied()
        .find(|&i| requests[i].matcher.drops_sample(labels, ts_ns))
}

/// Result of building one bucket's rewrite: the survivor parts (zero, one --
/// this module never splits into more than one, per its scope-reduction doc
/// note -- since "parts may be empty" is explicitly legal per the
/// `RewriteRecord` proto when every live record was dropped in full), the
/// sample counts [`MaintainError::ErasureConservationViolation`]'s gate
/// checks, and the `drops[]` entries for every request in the input batch
/// (including `dropped_count: 0` ones, ADR-0064 §4).
#[derive(Debug)]
pub struct RewriteBuild {
    pub parts: Vec<BuiltPart>,
    pub input_sample_count: u64,
    pub output_sample_count: u64,
    pub drops: Vec<RewriteDrop>,
}

/// Decode-filter-reencode one bucket's live record set against every
/// applicable request's matcher in one pass. `requests` MUST already be every
/// pending request whose prefilter overlaps this bucket (ADR-0064 §4's
/// "sibling case": batched once per bucket by the caller, never once per
/// request), so every one gets a `drops[]` entry from this single call, even
/// a zero one.
///
/// A run whose series matches no applicable request's labels is copied
/// verbatim ([`copy_run_verbatim`]), no decode needed. A run whose series
/// does match at least one request's labels is fully decoded via
/// [`ravel_segment::decode_run_pages_soa`] /
/// [`ravel_segment::decode_run_histogram_pages`], every sample is tested
/// against every applicable matcher, survivors are re-encoded via
/// [`ravel_segment::encode_run_v4`], and a run with zero survivors is
/// dropped from the output entirely (never emitted as an empty run). A
/// series with zero surviving runs is likewise dropped from the output
/// series list.
pub async fn build_rewrite(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    config: &CompactorConfig,
    catalogs: &[InputCatalog],
    requests: &[ApplicableRequest],
    input_set_hash: &[u8; 32],
) -> Result<RewriteBuild> {
    let whole = fetch_whole_objects(store, catalogs).await?;
    let limits = ReaderLimits::default();

    let mut input_sample_count: u64 = 0;
    let mut output_sample_count: u64 = 0;
    let mut dropped_counts = vec![0u64; requests.len()];

    // Group every series across every input catalog by id before encoding,
    // mirroring build.rs::build_parts's cross-input grouping (canonical
    // input order, then each object's own run order). A sealed bucket's
    // live RawL0 input set is normally >=2 commits sharing a series_id --
    // every metrics flush is its own L0 commit repeating the same series --
    // so processing catalogs one at a time and pushing a fresh
    // `SeriesInputV4` per (catalog, series) pair, as this loop used to,
    // produces a duplicate series_id in series_out and aborts the whole
    // bucket with `DuplicateSeriesId` in `assemble_v4_body`.
    let mut by_series: BTreeMap<[u8; 16], Vec<(usize, &SeriesPlan)>> = BTreeMap::new();
    for (idx, catalog) in catalogs.iter().enumerate() {
        for series in &catalog.series {
            by_series
                .entry(series.series_id.0)
                .or_default()
                .push((idx, series));
        }
    }

    let mut series_out: Vec<SeriesInputV4> = Vec::with_capacity(by_series.len());

    for (_id, contributions) in by_series {
        let (_, first) = contributions[0];
        let series_id = first.series_id;
        let labels = first.labels.clone();

        let applicable: Vec<usize> = requests
            .iter()
            .enumerate()
            .filter(|(_, r)| r.matcher.matches_labels(&labels))
            .map(|(i, _)| i)
            .collect();

        let mut runs_out = Vec::new();
        for (idx, series) in &contributions {
            let catalog = &catalogs[*idx];
            let object = whole.get(&catalog.object_key).ok_or_else(|| {
                MaintainError::Invariant(format!("no fetched object for {}", catalog.object_key))
            })?;
            for run in &series.runs {
                input_sample_count = input_sample_count
                    .checked_add(u64::from(run.sample_count))
                    .ok_or_else(|| {
                        MaintainError::Invariant("input_sample_count sum overflowed u64".to_string())
                    })?;

                if applicable.is_empty() {
                    output_sample_count = output_sample_count
                        .checked_add(u64::from(run.sample_count))
                        .ok_or_else(|| {
                            MaintainError::Invariant(
                                "output_sample_count sum overflowed u64".to_string(),
                            )
                        })?;
                    runs_out.push(copy_run_verbatim(object, run)?);
                    continue;
                }

                let ts_page = slice_whole(object, run.ts_abs)?;
                let val_page = slice_whole(object, run.page_abs)?;
                let entry = RunEntry {
                    created_unix_ns: run.created_unix_ns,
                    writer_epoch: run.writer_epoch,
                    writer_seq: run.writer_seq,
                    sample_count: run.sample_count,
                    min_ts_ns: run.min_ts_ns,
                    max_ts_ns: run.max_ts_ns,
                    ts_page: (0, 0),
                    val_page: (0, 0),
                    hist_page: (0, 0),
                };

                match run.kind {
                    ValueKind::Scalar => {
                        let mut scratch = Vec::new();
                        let mut timestamps = Vec::new();
                        let mut values = Vec::new();
                        decode_run_pages_soa(
                            &series_id,
                            &entry,
                            ts_page.as_ref(),
                            val_page.as_ref(),
                            limits,
                            &mut scratch,
                            &mut timestamps,
                            &mut values,
                        )?;
                        let mut survivors = Vec::with_capacity(timestamps.len());
                        for (ts, value) in timestamps.into_iter().zip(values) {
                            match first_dropping_request(&applicable, requests, &labels, ts) {
                                Some(i) => dropped_counts[i] += 1,
                                None => survivors.push(Sample { ts_ns: ts, value }),
                            }
                        }
                        if !survivors.is_empty() {
                            output_sample_count = output_sample_count
                                .checked_add(survivors.len() as u64)
                                .ok_or_else(|| {
                                    MaintainError::Invariant(
                                        "output_sample_count sum overflowed u64".to_string(),
                                    )
                                })?;
                            runs_out.push(encode_run_v4(
                                &series_id,
                                run.created_unix_ns,
                                run.writer_epoch,
                                run.writer_seq,
                                &SeriesValues::Scalar(survivors),
                            )?);
                        }
                    }
                    ValueKind::Histogram => {
                        let samples = decode_run_histogram_pages(
                            &series_id,
                            &entry,
                            ts_page.as_ref(),
                            val_page.as_ref(),
                            limits,
                        )?;
                        let mut survivors = Vec::with_capacity(samples.len());
                        for s in samples {
                            match first_dropping_request(&applicable, requests, &labels, s.ts_ns) {
                                Some(i) => dropped_counts[i] += 1,
                                None => survivors.push(s),
                            }
                        }
                        if !survivors.is_empty() {
                            output_sample_count = output_sample_count
                                .checked_add(survivors.len() as u64)
                                .ok_or_else(|| {
                                    MaintainError::Invariant(
                                        "output_sample_count sum overflowed u64".to_string(),
                                    )
                                })?;
                            runs_out.push(encode_run_v4(
                                &series_id,
                                run.created_unix_ns,
                                run.writer_epoch,
                                run.writer_seq,
                                &SeriesValues::Histogram(survivors),
                            )?);
                        }
                    }
                }
            }
        }

        if !runs_out.is_empty() {
            series_out.push(SeriesInputV4 {
                series_id,
                labels,
                runs: runs_out,
            });
        }
    }

    let parts = if series_out.is_empty() {
        Vec::new()
    } else {
        vec![build_rewrite_part(
            bucket,
            config,
            input_set_hash,
            series_out,
        )?]
    };

    let drops = requests
        .iter()
        .zip(dropped_counts)
        .map(|(r, dropped_count)| RewriteDrop {
            request_id: r.request_id.clone(),
            dropped_count,
        })
        .collect();

    Ok(RewriteBuild {
        parts,
        input_sample_count,
        output_sample_count,
        drops,
    })
}

/// Encode the surviving series into a single RSEG v5 output part, mirroring
/// `build.rs`'s private `flush_part` (`level: 1`, the compaction convention;
/// `part_index: 0`, since this module never splits into more than one part).
/// Ingest bounds are zeroed: they describe the original *ingest-time* window
/// (`min_ingest_ts_ns`/`max_ingest_ts_ns`, carried only by raw L0 commit
/// records), which a rewrite's live input may not have at all when it is
/// itself an L1/rewrite part, and no query-correctness path reads a
/// compaction/rewrite part's ingest bounds (they gate ingest-time admission,
/// not query results) -- a further transparent scope reduction alongside the
/// exemplar drop documented at the top of this module.
fn build_rewrite_part(
    bucket: &Bucket,
    config: &CompactorConfig,
    input_set_hash: &[u8; 32],
    batch: Vec<SeriesInputV4>,
) -> Result<BuiltPart> {
    let run_count: u64 = batch.iter().map(|s| s.runs.len() as u64).sum();
    let first_series_id = batch.iter().map(|s| s.series_id).min();
    let last_series_id = batch.iter().map(|s| s.series_id).max();

    let identity = SegmentIdentity {
        tenant_hash: bucket.tenant_hash.0,
        shard: bucket.shard,
        writer_id: config.compactor_writer_id.to_string(),
        writer_epoch: 0,
        writer_seq: 0,
    };
    let meta = CompactionMetaV4 {
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        input_set_hash: *input_set_hash,
        part_index: 0,
        level: 1,
    };
    let ingest = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written =
        SegmentWriter::write_v5_with_exemplars(batch, identity, ingest, meta, Vec::new())?;
    let content_hash = written.summary.blake3;
    let hash16 = hex::encode(&content_hash[..8]);
    let input_set_hash16 = hex::encode(&input_set_hash[..8]);
    let key = keys::l1_part_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
        &input_set_hash16,
        0,
        &hash16,
    )?;

    let part = CompactionPart {
        part_index: 0,
        first_series_id: first_series_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        last_series_id: last_series_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        content_hash: content_hash.to_vec(),
        object_size: written.bytes.len() as u64,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        run_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: OUTPUT_FORMAT_VERSION,
    };
    Ok(BuiltPart {
        key,
        bytes: written.bytes,
        part,
    })
}

/// What one rewrite's `RewriteRecord` supersedes: either the raw L0 commit
/// records themselves (the bucket has never been compacted, mirroring
/// `CompactionRecord.inputs`), or a whole prior compaction/rewrite record
/// (the common case once a bucket has been compacted or previously
/// rewritten). Exactly one of `RewriteRecord.inputs` /
/// `.superseded_record_key` is ever set (`ravel_commit::erasure::validate_rewrite`),
/// and which one is a property of [`resolve_live_record`]'s result at the
/// point [`build_rewrite`] ran -- the driver (EJ-T7) threads it through
/// unchanged since [`build_rewrite`] itself does not need to know.
#[derive(Debug, Clone)]
pub enum RewriteSupersession {
    RawL0(Vec<CompactionInputIdentity>),
    Existing(String),
}

/// Sum `dropped_count`/`sample_count`s in u64 with checked addition,
/// mirroring `publish.rs`'s private `checked_sample_sum` (duplicated for the
/// same privacy reason as [`get_compaction_record`]): an overflowing sum is
/// itself an invariant breach, never a silent wrap that could fake or mask
/// the conservation gate below.
fn checked_sample_sum(counts: impl Iterator<Item = u64>) -> Result<u64> {
    let mut sum: u64 = 0;
    for count in counts {
        sum = sum.checked_add(count).ok_or_else(|| {
            MaintainError::Invariant("sample_count sum overflowed u64".to_string())
        })?;
    }
    Ok(sum)
}

/// Conservation gate ([`MaintainError::ErasureConservationViolation`],
/// ADR-0064 decision 3 point 4) plus assembly and `CreateIfAbsent` publish of
/// one bucket's `RewriteRecord`, structurally mirroring
/// `publish.rs::publish_record_with_conservation`'s shape (abandonment
/// deadline first, then the gate, then assembly, then the racing-loser
/// convergence/repair path on `AlreadyExists`) -- see the module doc's "Why
/// this is not `rewrite_and_publish`" note for why this is a sibling
/// function rather than a call into that shared primitive.
///
/// The gate: `sum(output sample_count) + sum(dropped sample_count) ==
/// sum(input sample_count)`. Any inequality -- a decode/re-encode bug losing
/// or duplicating survivors, a drop miscounted against the wrong request --
/// aborts before the first byte is written: no part, no record, nothing
/// converges on a lossy or inflated rewrite. The live L0/L1 inputs stay live
/// and queryable and the `.dreq` stays pending, exactly like an aborted
/// compaction (plan §3.4 point 3).
pub async fn publish_rewrite_record(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    clock: &dyn Clock,
    bucket: &Bucket,
    supersession: RewriteSupersession,
    build: RewriteBuild,
    start_ns: i64,
) -> Result<PublishOutcome> {
    let now = clock.now_ns();
    if now.saturating_sub(start_ns) > config.max_compaction_lifetime_ns {
        tracing::warn!(
            elapsed_ns = now.saturating_sub(start_ns),
            "erasure rewrite run exceeded max_compaction_lifetime; abandoning without publish"
        );
        return Ok(PublishOutcome::Abandoned);
    }

    let dropped_sample_count = checked_sample_sum(build.drops.iter().map(|d| d.dropped_count))?;
    let reconstructed = build
        .output_sample_count
        .checked_add(dropped_sample_count)
        .ok_or_else(|| {
            MaintainError::Invariant("output + dropped sample_count sum overflowed u64".to_string())
        })?;
    if reconstructed != build.input_sample_count {
        return Err(MaintainError::ErasureConservationViolation {
            tenant_hash: hex::encode(bucket.tenant_hash.0),
            signal: bucket.signal.key_prefix().to_string(),
            shard: bucket.shard,
            ingest_hour_bucket: bucket.ingest_hour_bucket,
            input_sample_count: build.input_sample_count,
            output_sample_count: build.output_sample_count,
            dropped_sample_count,
        });
    }

    // Encode reconciliation: the conservation gate above only proves the
    // decode/filter tally didn't lose or duplicate a sample -- it says
    // nothing about whether the bytes this run is about to publish actually
    // contain those `output_sample_count` survivors. Recount from the
    // *encoded* parts' own summaries and abort before the first byte is
    // written if they disagree, closing that gap.
    let encoded_sample_count = checked_sample_sum(build.parts.iter().map(|p| p.part.sample_count))?;
    if encoded_sample_count != build.output_sample_count {
        return Err(MaintainError::Invariant(format!(
            "erasure rewrite encode reconciliation failed: output_sample_count {} does not \
             match sum(part.sample_count) {} across {} written part(s)",
            build.output_sample_count,
            encoded_sample_count,
            build.parts.len()
        )));
    }

    let (inputs, superseded_record_key) = match &supersession {
        RewriteSupersession::RawL0(identities) => (identities.clone(), String::new()),
        RewriteSupersession::Existing(key) => (Vec::new(), key.clone()),
    };

    let mut applied_request_ids: Vec<String> =
        build.drops.iter().map(|d| d.request_id.clone()).collect();
    applied_request_ids.sort();

    let input_set_hash = erasure::compute_rewrite_input_set_hash(
        &inputs,
        if superseded_record_key.is_empty() {
            None
        } else {
            Some(superseded_record_key.as_str())
        },
        &applied_request_ids,
    );

    let signal = ravel_commit::signal::to_proto(bucket.signal) as i32;
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        inputs,
        input_set_hash: input_set_hash.to_vec(),
        parts: build.parts.iter().map(|p| p.part.clone()).collect(),
        drops: build.drops,
        created_unix_ns: now,
        superseded_record_key,
    };

    let record_key = keys::rewrite_record_key_for(&record)?;
    let payload = record.encode_to_vec();
    let checksum = UploadChecksum::Crc32c(crc32c::crc32c(&payload));
    let opts = PutOptions::create_if_absent().with_checksum(checksum);

    // Dry-run: assembled identically, publishing PUT skipped, exactly like
    // `publish_record_with_conservation`'s dry-run short-circuit. Applies to
    // the parts too: `build.rs`'s own build path skips `put_part` under
    // `dry_run` for the same reason (a dry run reports what it would have
    // written, without writing it).
    if config.dry_run {
        return Ok(PublishOutcome::Published);
    }

    // Content-addressed parts are written before the record that names them
    // (mirrors `build.rs`'s build-time `put_part` calls): a crash or a lost
    // race between this write and the record `CreateIfAbsent` below can never
    // leave a published `RewriteRecord` pointing at a part object that does
    // not exist. `put_part` is idempotent (`AlreadyExists` is success), so
    // re-running this on a retry is safe.
    for part in &build.parts {
        crate::build::put_part(store, part).await?;
    }

    match store.put(&record_key, payload.into(), opts).await {
        Ok(_) => {
            tracing::info!(
                key = %record_key,
                parts = record.parts.len(),
                drops = record.drops.len(),
                "erasure rewrite record published"
            );
            Ok(PublishOutcome::Published)
        }
        Err(StoreError::AlreadyExists) => {
            resolve_already_exists_rewrite(store, &record_key, &input_set_hash, &build.parts).await
        }
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// GET the rewrite record that beat us. Same `input_set_hash`: HEAD every
/// part it references and re-PUT any our-built part that is missing
/// (content-addressed keys make this safe -- same shape as
/// `publish.rs::resolve_already_exists`, generalized here to
/// `RewriteRecord`/`reconstruct_rewrite_part_key` since a rewrite's winner is
/// never a `CompactionRecord`). Different `input_set_hash`: a sealed bucket
/// cannot legitimately hold two rewrite generations at once, so alarm and
/// stop without deleting anything.
async fn resolve_already_exists_rewrite(
    store: &dyn ObjectStoreBackend,
    record_key: &str,
    our_hash: &[u8; 32],
    our_parts: &[BuiltPart],
) -> Result<PublishOutcome> {
    let winner = get_rewrite_record(store, record_key).await?;

    if winner.input_set_hash.as_slice() != our_hash.as_slice() {
        return Err(MaintainError::InputSetHashDivergence {
            observed_key: record_key.to_string(),
            ours: hex::encode(our_hash),
            theirs: hex::encode(&winner.input_set_hash),
        });
    }

    let mut repaired = 0usize;
    for part in &winner.parts {
        let part_key = keys::reconstruct_rewrite_part_key(&winner, part)?;
        match store.head(&part_key).await {
            Ok(_) => {}
            Err(StoreError::NotFound) => {
                if let Some(ours) = our_parts.iter().find(|p| p.key == part_key) {
                    crate::build::put_part(store, ours).await?;
                    repaired += 1;
                } else {
                    tracing::warn!(
                        key = %part_key,
                        "winner references a rewrite part this run did not build; cannot repair"
                    );
                }
            }
            Err(e) => return Err(MaintainError::Store(e)),
        }
    }
    tracing::info!(
        parts_repaired = repaired,
        "converged on prior erasure rewrite record"
    );
    Ok(PublishOutcome::Converged {
        parts_repaired: repaired,
    })
}

/// The result of an [`erasure_rewrite_bucket`] call. Every variant except
/// [`ErasureRewriteOutcome::Rewritten`] means the bucket was left untouched,
/// with the reason -- mirroring [`crate::compact::CompactionOutcome`]'s shape
/// so a driver scanning many buckets can treat them uniformly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErasureRewriteOutcome {
    /// Not yet sealed: mirrors [`crate::bucket::Bucket::is_sealed`]'s gate in
    /// [`crate::compact::compact_bucket`]. Later hours are also unsealed.
    NotSealed,
    /// A retention tombstone is present: the bucket contributes nothing, so
    /// there is nothing left to erase from it.
    Tombstoned,
    /// No pending request's [`bucket_may_overlap`] prefilter overlapped this
    /// bucket; nothing to do.
    NoApplicableRequests,
    /// The bucket (or some object still listed in it) is under legal hold
    /// ([`bucket_is_held`]); skipped, every applicable `.dreq` stays pending,
    /// and EJ-T3's scan-time exclusion keeps hiding the held data from
    /// queries in the meantime.
    Held,
    /// Built and published (or converged / abandoned): `parts` output parts
    /// written, `publish` records how the `RewriteRecord` PUT resolved.
    Rewritten {
        parts: usize,
        publish: PublishOutcome,
    },
}

/// Rewrite one sealed bucket against every pending erasure request that
/// applies to it (ADR-0064 decision 3, EJ-T4). `pending` MUST already be
/// every request pending on this bucket's `(tenant_hash, signal)`
/// ([`pending_erasure_requests`]); this function does the per-bucket
/// [`bucket_may_overlap`] prefiltering itself, so the same `pending` slice is
/// reusable across every bucket a scan visits without re-listing `.dreq`s per
/// bucket.
///
/// This is the ADR-0064 section 4 "sibling case" entry point: every request
/// that overlaps the bucket is batched into one [`build_rewrite`] /
/// [`publish_rewrite_record`] call, so a bucket is never rewritten once per
/// request -- one `RewriteRecord` per bucket per rewrite generation, with a
/// `drops[]` entry (possibly `dropped_count: 0`) for every applicable
/// request.
///
/// [`crate::memo_snapshot`] promptness invalidation is deliberately not
/// called here (deferred to the EJ-T4b/invalidate follow-up per this
/// function's module doc): a caller wiring that in later adds one call after
/// a `Rewritten` outcome with `publish: PublishOutcome::Published`, using the
/// bucket's own identity to invalidate the interior zone -- nothing else in
/// this function's structure needs to change for that wiring.
pub async fn erasure_rewrite_bucket(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
    pending: &[PendingErasureRequest],
) -> Result<ErasureRewriteOutcome> {
    let start_ns = clock.now_ns();
    if !bucket.is_sealed(start_ns, config) {
        return Ok(ErasureRewriteOutcome::NotSealed);
    }
    if pending.is_empty() {
        return Ok(ErasureRewriteOutcome::NoApplicableRequests);
    }

    let listing = crate::read::list_bucket(store, bucket).await?;
    if listing.tombstone_key.is_some() {
        return Ok(ErasureRewriteOutcome::Tombstoned);
    }
    if bucket_is_held(&listing, lease) {
        return Ok(ErasureRewriteOutcome::Held);
    }

    // Record-derived overlap check (ADR-0064 GDPR fix): this needs the live
    // record set's actual event-time bounds, not the bucket's ingest-hour
    // key, so it can only run after `resolve_live_inputs`'s cheap metadata
    // GET, unlike the old purely key-derived prefilter this replaced.
    let live = resolve_live_inputs(store, bucket, &listing).await?;
    let (min_event_ts_ns, max_event_ts_ns) = live_input_event_bounds(&live);
    let overlapping: Vec<&PendingErasureRequest> = pending
        .iter()
        .filter(|p| bucket_may_overlap(min_event_ts_ns, max_event_ts_ns, &p.request))
        .collect();
    if overlapping.is_empty() {
        return Ok(ErasureRewriteOutcome::NoApplicableRequests);
    }

    let applicable: Vec<ApplicableRequest> = overlapping
        .iter()
        .map(|p| ApplicableRequest {
            request_id: p.request.request_id.clone(),
            matcher: ErasureMatcher::from_request(&p.request),
        })
        .collect();

    let (catalogs, supersession) = load_live_catalogs_and_target(store, config, live).await?;

    let mut applied_request_ids: Vec<String> =
        applicable.iter().map(|r| r.request_id.clone()).collect();
    applied_request_ids.sort();
    let input_set_hash = match &supersession {
        RewriteSupersession::RawL0(ids) => {
            erasure::compute_rewrite_input_set_hash(ids, None, &applied_request_ids)
        }
        RewriteSupersession::Existing(key) => {
            erasure::compute_rewrite_input_set_hash(&[], Some(key.as_str()), &applied_request_ids)
        }
    };

    let build = build_rewrite(
        store,
        bucket,
        config,
        &catalogs,
        &applicable,
        &input_set_hash,
    )
    .await?;
    let parts = build.parts.len();
    let publish =
        publish_rewrite_record(store, config, clock, bucket, supersession, build, start_ns).await?;
    Ok(ErasureRewriteOutcome::Rewritten { parts, publish })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    //! Eight required tests (EJ-T4, issue #754): drop-exact-preserve-others,
    //! conservation-abort, legal-hold-skip, idempotence (no double-count on
    //! resolve), corrupt/truncated input, cross-input same-series merge
    //! (review blocker 1), backfilled/clock-skewed windowed selection
    //! (review blocker 2), and partial-survival re-encode bit-identity
    //! (review hardening item 4). Each test's doc comment names the exact
    //! line whose flip breaks it, per the task's "prove-the-test"
    //! discipline.
    //!
    //! Neither `build_rewrite`/`publish_rewrite_record`/`erasure_rewrite_bucket`
    //! nor `PendingErasureRequest`/`ApplicableRequest` ever re-fetch a `.dreq`
    //! object by `request_key`; every request they need is passed in already
    //! decoded. So no test here writes a `.dreq` object to the store -- only a
    //! bucket's real L0/compaction/rewrite objects are ever real store
    //! objects. The L0-seeding pattern mirrors `compact.rs`'s own test module
    //! exactly, since this module reads L0 buckets through the identical
    //! `read.rs` pipeline.

    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_segment::{SeriesInputV3, VERSION_V6};
    use ravel_types::{Label, METRIC_NAME_LABEL, SeriesId, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::clock::FixedClock;
    use crate::read::SeriesPlan;
    use crate::sweep::NoLeases;

    const TENANT: &str = "acme";
    const SHARD: u32 = 7;
    const HOUR: u32 = 495_000;
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;

    fn tenant_hash() -> TenantHash {
        TenantId::new(TENANT).hash()
    }

    fn bucket() -> Bucket {
        Bucket::new(tenant_hash(), Signal::Metrics, SHARD, HOUR)
    }

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

    async fn seed(store: &dyn ObjectStoreBackend, seq: u64, series: Vec<SeriesInputV3>) {
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
            segment_format_version: u32::from(VERSION_V6),
            created_unix_ns: created,
            ingest_hour_bucket: HOUR,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
    }

    fn erasure_request(id_seed: u128, metric: &str) -> ErasureRequest {
        ErasureRequest {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            request_id: Uuid::from_u128(id_seed).to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: METRIC_NAME_LABEL.to_string(),
                value: metric.to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        }
    }

    async fn read_rewrite_record(store: &dyn ObjectStoreBackend) -> RewriteRecord {
        let listing = crate::read::list_bucket(store, &bucket())
            .await
            .expect("list bucket");
        assert_eq!(
            listing.rewrite_record_keys.len(),
            1,
            "expected exactly one rewrite record"
        );
        let got = store
            .get(&listing.rewrite_record_keys[0], GetRange::Full)
            .await
            .expect("get record");
        RewriteRecord::decode(got.data.as_ref()).expect("decode record")
    }

    async fn decode_series_samples(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        object_key: &str,
        target: SeriesId,
    ) -> Vec<(i64, u64)> {
        let catalog =
            crate::read::load_catalog_from_object(store, config, object_key.to_string(), 0, 0, 0)
                .await
                .expect("load catalog");
        let whole = store
            .get(object_key, GetRange::Full)
            .await
            .expect("get whole")
            .data;
        let mut out = Vec::new();
        for series in &catalog.series {
            if series.series_id.0 != target.0 {
                continue;
            }
            for run in &series.runs {
                let entry = RunEntry {
                    created_unix_ns: run.created_unix_ns,
                    writer_epoch: run.writer_epoch,
                    writer_seq: run.writer_seq,
                    sample_count: run.sample_count,
                    min_ts_ns: run.min_ts_ns,
                    max_ts_ns: run.max_ts_ns,
                    ts_page: (0, 0),
                    val_page: (0, 0),
                    hist_page: (0, 0),
                };
                let ts_page = slice_whole(&whole, run.ts_abs).expect("ts slice");
                let val_page = slice_whole(&whole, run.page_abs).expect("val slice");
                let mut scratch = Vec::new();
                let mut timestamps = Vec::new();
                let mut values = Vec::new();
                decode_run_pages_soa(
                    &target,
                    &entry,
                    ts_page.as_ref(),
                    val_page.as_ref(),
                    ReaderLimits::default(),
                    &mut scratch,
                    &mut timestamps,
                    &mut values,
                )
                .expect("decode run");
                for (ts, v) in timestamps.into_iter().zip(values) {
                    out.push((ts, v.to_bits()));
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// Drops exactly the series matched by the erasure predicate and leaves
    /// every other series' samples bit-identical (float comparison by
    /// `to_bits`, including a `-0.0`/`NaN` payload, per this repo's float
    /// discipline). Flip-line proof: `ErasureMatcher::matches_labels`
    /// returning `false` unconditionally makes `alpha` survive, failing the
    /// `alpha_after.is_empty()` assertion below.
    #[tokio::test]
    async fn rewrite_drops_matching_series_preserves_others_bit_identically() {
        let store = MemoryStore::new();
        seed(
            &store,
            1,
            vec![
                series("alpha", &[(10, 1.0), (20, 2.0)]),
                series("beta", &[(15, 9.5), (25, -0.0), (35, f64::NAN)]),
            ],
        )
        .await;

        let request = erasure_request(1, "alpha");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let outcome =
            erasure_rewrite_bucket(&store, &clock, &config, &NoLeases, &bucket(), &pending)
                .await
                .expect("rewrite");

        let (parts, publish) = match outcome {
            ErasureRewriteOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(parts, 1);
        assert_eq!(publish, PublishOutcome::Published);

        let record = read_rewrite_record(&store).await;
        assert_eq!(record.drops.len(), 1);
        assert_eq!(record.drops[0].request_id, Uuid::from_u128(1).to_string());
        assert_eq!(
            record.drops[0].dropped_count, 2,
            "both alpha samples dropped"
        );
        assert_eq!(record.parts.len(), 1);

        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");

        let alpha_after =
            decode_series_samples(&store, &config, &part_key, series_id("alpha")).await;
        assert!(alpha_after.is_empty(), "alpha must not survive the rewrite");

        let beta_after = decode_series_samples(&store, &config, &part_key, series_id("beta")).await;
        let mut beta_expected = vec![
            (15, 9.5f64.to_bits()),
            (25, (-0.0f64).to_bits()),
            (35, f64::NAN.to_bits()),
        ];
        beta_expected.sort_unstable();
        assert_eq!(
            beta_after, beta_expected,
            "beta must survive bit-identically, including -0.0 and the NaN payload"
        );
    }

    /// A hand-built [`RewriteBuild`] whose `output_sample_count +
    /// dropped_count` (5 + 3 = 8) does not equal `input_sample_count` (10)
    /// must abort before any write (ADR-0064 decision 3 point 4). Flip-line
    /// proof: `if reconstructed != build.input_sample_count` in
    /// `publish_rewrite_record` flipped to `==` (or deleted) lets this publish
    /// `Ok`, failing the `expect_err`.
    #[tokio::test]
    async fn conservation_mismatch_aborts_rewrite_publish() {
        let store = MemoryStore::new();
        let clock = FixedClock::new(0);
        let config = CompactorConfig::default();
        let build = RewriteBuild {
            parts: Vec::new(),
            input_sample_count: 10,
            output_sample_count: 5,
            drops: vec![RewriteDrop {
                request_id: Uuid::from_u128(9).to_string(),
                dropped_count: 3,
            }],
        };

        let err = publish_rewrite_record(
            &store,
            &config,
            &clock,
            &bucket(),
            RewriteSupersession::RawL0(Vec::new()),
            build,
            0,
        )
        .await
        .expect_err("mismatched conservation must abort");

        match err {
            MaintainError::ErasureConservationViolation {
                input_sample_count,
                output_sample_count,
                dropped_sample_count,
                ..
            } => {
                assert_eq!(input_sample_count, 10);
                assert_eq!(output_sample_count, 5);
                assert_eq!(dropped_sample_count, 3);
            }
            other => panic!("expected ErasureConservationViolation, got {other:?}"),
        }
        assert!(
            list_all(&store, "").await.expect("list").is_empty(),
            "an aborted rewrite must publish nothing, not even the record"
        );
    }

    /// A `LeaseCheck` that protects any key under one prefix -- `NoLeases`
    /// alone can never produce a `Held` outcome, so this local double
    /// simulates a legal hold on the fixture bucket.
    struct HoldPrefix(String);
    impl LeaseCheck for HoldPrefix {
        fn is_protected(&self, key: &str) -> bool {
            key.starts_with(self.0.as_str())
        }
    }

    /// A bucket under legal hold is skipped outright: no record, no part,
    /// `.dreq` stays pending. Flip-line proof: removing the `if
    /// bucket_is_held(&listing, lease) { return Ok(ErasureRewriteOutcome::Held); }`
    /// gate in `erasure_rewrite_bucket` makes this proceed to `Rewritten`,
    /// failing `assert_eq!(outcome, ErasureRewriteOutcome::Held)`.
    #[tokio::test]
    async fn legal_hold_skips_bucket_leaves_dreq_pending() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0)])]).await;

        let request = erasure_request(1, "alpha");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let b = bucket();
        let prefix =
            keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
                .expect("prefix");
        let lease = HoldPrefix(prefix);

        let before = list_all(&store, "").await.expect("list before");

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &CompactorConfig::default(),
            &lease,
            &b,
            &pending,
        )
        .await
        .expect("call succeeds even when held");
        assert_eq!(outcome, ErasureRewriteOutcome::Held);

        let after = list_all(&store, "").await.expect("list after");
        assert_eq!(
            before.len(),
            after.len(),
            "held bucket must be left untouched"
        );
    }

    /// Running the same rewrite twice from the same pre-publish snapshot (a
    /// crash-retry before either write lands, or two racing compactors)
    /// converges on one `RewriteRecord`, never a double count of dropped
    /// samples. Flip-line proof: in `publish_rewrite_record`, routing
    /// `Err(StoreError::AlreadyExists)` to a hard `Err` instead of
    /// `resolve_already_exists_rewrite` makes the second `publish_rewrite_record`
    /// call return `Err`, failing `.expect("publish 2")`.
    #[tokio::test]
    async fn republishing_the_same_rewrite_converges_without_double_counting() {
        let store = MemoryStore::new();
        seed(
            &store,
            1,
            vec![
                series("alpha", &[(10, 1.0), (20, 2.0)]),
                series("beta", &[(15, 9.5)]),
            ],
        )
        .await;

        let request = erasure_request(2, "alpha");
        let applicable = vec![ApplicableRequest {
            request_id: request.request_id.clone(),
            matcher: ErasureMatcher::from_request(&request),
        }];

        let b = bucket();
        let config = CompactorConfig::default();
        let listing = crate::read::list_bucket(&store, &b)
            .await
            .expect("list bucket");
        let live = resolve_live_inputs(&store, &b, &listing)
            .await
            .expect("resolve live inputs");
        let (catalogs, supersession) = load_live_catalogs_and_target(&store, &config, live)
            .await
            .expect("load live catalogs");

        let mut ids: Vec<String> = applicable.iter().map(|r| r.request_id.clone()).collect();
        ids.sort();
        let input_set_hash = match &supersession {
            RewriteSupersession::RawL0(idents) => {
                erasure::compute_rewrite_input_set_hash(idents, None, &ids)
            }
            RewriteSupersession::Existing(key) => {
                erasure::compute_rewrite_input_set_hash(&[], Some(key.as_str()), &ids)
            }
        };

        let clock = FixedClock::new(0);

        let build1 = build_rewrite(&store, &b, &config, &catalogs, &applicable, &input_set_hash)
            .await
            .expect("build 1");
        let outcome1 =
            publish_rewrite_record(&store, &config, &clock, &b, supersession.clone(), build1, 0)
                .await
                .expect("publish 1");
        assert_eq!(outcome1, PublishOutcome::Published);

        let build2 = build_rewrite(&store, &b, &config, &catalogs, &applicable, &input_set_hash)
            .await
            .expect("build 2");
        let outcome2 = publish_rewrite_record(&store, &config, &clock, &b, supersession, build2, 0)
            .await
            .expect("publish 2");
        assert_eq!(
            outcome2,
            PublishOutcome::Converged { parts_repaired: 0 },
            "second identical rewrite converges, does not republish"
        );

        let record = read_rewrite_record(&store).await;
        assert_eq!(record.drops.len(), 1);
        assert_eq!(
            record.drops[0].dropped_count, 2,
            "dropped_count must not double-count across the two runs"
        );

        let listing_after = crate::read::list_bucket(&store, &b)
            .await
            .expect("list after");
        assert_eq!(
            listing_after.rewrite_record_keys.len(),
            1,
            "exactly one RewriteRecord after two identical publishes"
        );
    }

    /// A page range that overruns its backing object (truncated/corrupt
    /// input) must surface as a typed error, never a panic. Flip-line proof:
    /// removing the `if end_usize > whole.len() { return Err(...) }` bounds
    /// check in `slice_whole` turns this into a `Bytes::slice` panic on an
    /// out-of-range index instead of a typed `MaintainError::Segment`.
    #[tokio::test]
    async fn truncated_input_page_yields_typed_error_not_panic() {
        let store = MemoryStore::new();
        let object_key = "t/deadbeef/m/corrupt-object";
        store
            .put(
                object_key,
                Bytes::from_static(b"01234567890123456789"),
                PutOptions::default(),
            )
            .await
            .expect("put short object");

        let catalog = InputCatalog {
            object_key: object_key.to_string(),
            series: vec![SeriesPlan {
                series_id: series_id("alpha"),
                labels: labels("alpha"),
                kind: ValueKind::Scalar,
                runs: vec![RunPlan {
                    created_unix_ns: 0,
                    writer_epoch: 0,
                    writer_seq: 0,
                    min_ts_ns: 10,
                    max_ts_ns: 10,
                    sample_count: 1,
                    ts_abs: (0, 8),
                    page_abs: (0, 10_000),
                    kind: ValueKind::Scalar,
                }],
            }],
            exemplars: Vec::new(),
        };

        let err = build_rewrite(
            &store,
            &bucket(),
            &CompactorConfig::default(),
            &[catalog],
            &[],
            &[0u8; 32],
        )
        .await
        .expect_err("a page range beyond the object's length must not decode");
        assert!(
            matches!(
                err,
                MaintainError::Segment(ravel_segment::SegmentError::Truncated)
            ),
            "expected a typed Truncated error, got {err:?}"
        );

        let after = list_all(&store, "").await.expect("list after");
        assert_eq!(
            after.len(),
            1,
            "a failed rewrite must not publish anything, not even a partial part"
        );
    }

    /// Review blocker 1: a sealed bucket's live RawL0 input set is normally
    /// >=2 commits that each repeat the same series (every metrics flush is
    /// its own L0 commit), so `build_rewrite` must merge same-`series_id`
    /// runs across every input catalog into one `SeriesInputV4`, not one per
    /// (catalog, series) pair. Flip-line proof: reverting `build_rewrite`'s
    /// `by_series` cross-input grouping to the old per-catalog loop (push a
    /// fresh `SeriesInputV4` for every catalog's own `series_id`, `runs:
    /// runs_out` scoped to that one catalog) reintroduces a duplicate
    /// `series_id` in `series_out` for the `alpha` series seeded twice below,
    /// so `assemble_v4_body` (crates/ravel-segment/src/writer.rs) returns
    /// `Err(WriteError::DuplicateSeriesId)` and this call's `.expect(...)`
    /// panics instead of returning `Rewritten`.
    #[tokio::test]
    async fn rewrite_merges_same_series_runs_across_multiple_l0_inputs() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0), (20, 2.0)])]).await;
        seed(&store, 2, vec![series("alpha", &[(30, 3.0), (40, 4.0)])]).await;

        // A request that matches no series in this bucket at all: the bug
        // reproduces even when nothing is dropped, because the old code
        // pushed a duplicate `SeriesInputV4` for every catalog regardless of
        // whether any request applied to it.
        let request = erasure_request(1, "unrelated-metric");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let outcome =
            erasure_rewrite_bucket(&store, &clock, &config, &NoLeases, &bucket(), &pending)
                .await
                .expect("rewrite must succeed even when >=2 live L0 commits share a series_id");

        let (parts, publish) = match outcome {
            ErasureRewriteOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(parts, 1);
        assert_eq!(publish, PublishOutcome::Published);

        let record = read_rewrite_record(&store).await;
        assert_eq!(record.parts.len(), 1);
        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");

        let alpha_after =
            decode_series_samples(&store, &config, &part_key, series_id("alpha")).await;
        let mut expected = vec![
            (10, 1.0f64.to_bits()),
            (20, 2.0f64.to_bits()),
            (30, 3.0f64.to_bits()),
            (40, 4.0f64.to_bits()),
        ];
        expected.sort_unstable();
        assert_eq!(
            alpha_after, expected,
            "alpha's samples from both L0 inputs must survive bit-identically, \
             merged under one series entry"
        );
    }

    /// Review blocker 2 (GDPR physical under-erasure): a windowed erasure
    /// request's event-time window is checked against `bucket_may_overlap`,
    /// which must overlap against the live records' real
    /// `[min_event_ts_ns, max_event_ts_ns]`, not the bucket's ingest-hour
    /// range -- the two are decoupled for a backfilled or clock-skewed
    /// write. This fixture's samples (event ts 10 and 20) are, like every
    /// other fixture in this module, nowhere near the bucket's own
    /// astronomically large ingest-hour bounds (`HOUR = 495_000`), which is
    /// exactly the backfill/clock-skew shape this bug needs. Flip-line
    /// proof: reverting `bucket_may_overlap` to compare
    /// `request.window_start_ns`/`window_end_ns` against
    /// `bucket.start_ns()`/`bucket.end_ns()` instead of
    /// `min_event_ts_ns`/`max_event_ts_ns` makes the prefilter in
    /// `erasure_rewrite_bucket` reject this request (window `[0, 25)` is
    /// nowhere near the bucket's ingest-hour bounds), yielding
    /// `NoApplicableRequests` and failing the `Rewritten` match below.
    #[tokio::test]
    async fn windowed_request_matches_backfilled_samples_outside_ingest_hour() {
        let store = MemoryStore::new();
        seed(&store, 1, vec![series("alpha", &[(10, 1.0), (20, 2.0)])]).await;

        let mut request = erasure_request(1, "alpha");
        request.window_start_ns = 0;
        request.window_end_ns = 25;
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let outcome =
            erasure_rewrite_bucket(&store, &clock, &config, &NoLeases, &bucket(), &pending)
                .await
                .expect("rewrite");

        let (parts, publish) = match outcome {
            ErasureRewriteOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!(
                "expected Rewritten -- a windowed request matching physically-stored \
                 event timestamps must select the bucket even when those timestamps \
                 fall outside its ingest hour, got {other:?}"
            ),
        };
        assert_eq!(
            parts, 0,
            "alpha is the bucket's only series and both its samples fall in the \
             erasure window, so nothing survives to write"
        );
        assert_eq!(publish, PublishOutcome::Published);

        let record = read_rewrite_record(&store).await;
        assert_eq!(record.drops.len(), 1);
        assert_eq!(
            record.drops[0].dropped_count, 2,
            "both alpha samples fall inside the erasure window and are physically erased"
        );
        assert!(
            record.parts.is_empty(),
            "alpha fully erased leaves zero output parts"
        );
    }

    /// Review hardening item 4: the existing drop tests each exercise either
    /// `copy_run_verbatim` (a series matching no request survives whole) or
    /// full-series erasure (every sample of a matched series is dropped).
    /// Neither exercises a matched series that PARTIALLY survives, which is
    /// the one path that actually goes through
    /// `decode_run_pages_soa`/`encode_run_v4` with a mixed keep/drop
    /// outcome. Flip-line proof: in `build_rewrite`'s `ValueKind::Scalar`
    /// arm, swapping `first_dropping_request(...).is_some()`'s branches
    /// (treat a match as "keep" and a non-match as "drop") flips which
    /// samples survive, failing the bit-identity assertion below (and would
    /// also flip which two samples the `dropped_count` covers).
    #[tokio::test]
    async fn rewrite_reencodes_partially_surviving_series_bit_identically() {
        let store = MemoryStore::new();
        seed(
            &store,
            1,
            vec![series(
                "alpha",
                &[(10, 1.0), (20, 2.0), (30, -0.0), (40, f64::NAN)],
            )],
        )
        .await;

        let mut request = erasure_request(1, "alpha");
        request.window_start_ns = 15;
        request.window_end_ns = 35;
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let outcome =
            erasure_rewrite_bucket(&store, &clock, &config, &NoLeases, &bucket(), &pending)
                .await
                .expect("rewrite");

        let (parts, publish) = match outcome {
            ErasureRewriteOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(parts, 1);
        assert_eq!(publish, PublishOutcome::Published);

        let record = read_rewrite_record(&store).await;
        assert_eq!(
            record.drops[0].dropped_count, 2,
            "only the two in-window samples (20, 30) are dropped"
        );

        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");
        let alpha_after =
            decode_series_samples(&store, &config, &part_key, series_id("alpha")).await;
        let mut expected = vec![(10, 1.0f64.to_bits()), (40, f64::NAN.to_bits())];
        expected.sort_unstable();
        assert_eq!(
            alpha_after, expected,
            "surviving samples must re-encode bit-identically, including the NaN payload"
        );
    }
}
