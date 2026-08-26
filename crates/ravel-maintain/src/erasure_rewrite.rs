//! Selective-erasure rewrite pass for the metrics (RSEG), logs (RLOG), and
//! spans (RSPAN) signals (ADR-0064 decision 3).
//! [`build_rewrite`] is the metrics (RSEG) driver; [`build_rewrite_logs`] and
//! [`build_rewrite_spans`] are its logs/spans siblings, decoding whole
//! `.rlog`/`.rspan` objects rather than RSEG's per-run catalogs (see
//! [`build_rewrite_logs`]'s doc for why). [`erasure_rewrite_bucket`] dispatches
//! to whichever of the three applies by `bucket.signal` and, on a successful
//! publish, calls [`crate::scan::MaintainMemo::invalidate`] for the rewritten
//! bucket's hour so the interior zone re-verifies immediately instead of
//! waiting for the next re-verify cadence -- for all three signals, closing
//! the gap for metrics too.
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
use futures::stream::{StreamExt, TryStreamExt, iter as stream_iter};
use prost::Message;
use ravel_commit::erasure;
use ravel_commit::keys;
use ravel_logseg::{
    AttrValue as LogAttrValue, LogStreamId, Predicate as LogPredicate, RlogConfig, RlogReader,
    RlogWriter,
};
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutOptions, StoreError, UploadChecksum, list_all,
};
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionPart, CompactionRecord, ErasurePredicateMatcher,
    ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_rspan::{RspanConfig, RspanReader, RspanWriter, SpanQuery};
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
use crate::scan::MaintainMemo;
use crate::sweep::LeaseCheck;
use crate::{rlog, rspan_codec};

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

/// A minimal, semantically-faithful duplicate of
/// `ravel_query::erasure::ErasurePredicate::matches_log_attrs`, reimplemented
/// here for the same dependency-direction reason as [`ErasureMatcher`]. A log
/// record's own `attrs` field is matched (never `stream_attrs`/resource, matching the query path), and only [`LogAttrValue::Str`] values can satisfy a matcher --
/// any other attribute-value variant never matches, mirroring
/// `ravel-query`'s `logs_non_string_attr_never_matches` test.
#[derive(Debug, Clone)]
pub struct LogErasureMatcher {
    matchers: Vec<(String, String)>,
    window_start_ns: i64,
    window_end_ns: i64,
}

impl LogErasureMatcher {
    pub fn from_request(request: &ErasureRequest) -> Self {
        LogErasureMatcher {
            matchers: request
                .predicate
                .iter()
                .map(|m: &ErasurePredicateMatcher| (m.key.clone(), m.value.clone()))
                .collect(),
            window_start_ns: request.window_start_ns,
            window_end_ns: request.window_end_ns,
        }
    }

    /// Conjunction of exact-match matchers against a log record's `attrs`.
    /// Empty matchers matches nothing (fail-safe, same as [`ErasureMatcher`]).
    pub fn matches_attrs(&self, attrs: &[(String, LogAttrValue)]) -> bool {
        if self.matchers.is_empty() {
            return false;
        }
        self.matchers.iter().all(|(k, v)| {
            attrs.iter().any(|(ak, av)| {
                ak == k && matches!(av, LogAttrValue::Str(s) if s.as_str() == v.as_str())
            })
        })
    }

    /// Half-open `[window_start_ns, window_end_ns)`; zero on either side is
    /// unset, identical to [`ErasureMatcher::ts_in_window`].
    pub fn ts_in_window(&self, ts_ns: i64) -> bool {
        let after_start = self.window_start_ns == 0 || ts_ns >= self.window_start_ns;
        let before_end = self.window_end_ns == 0 || ts_ns < self.window_end_ns;
        after_start && before_end
    }

    /// Whether this predicate drops a log record with `attrs` recorded at
    /// `ts_ns`.
    pub fn drops_record(&self, attrs: &[(String, LogAttrValue)], ts_ns: i64) -> bool {
        self.matches_attrs(attrs) && self.ts_in_window(ts_ns)
    }
}

/// A minimal, semantically-faithful duplicate of
/// `ravel_query::erasure::ErasurePredicate::matches_str_attrs`/
/// `is_erased_span`, reimplemented here for the same dependency-direction
/// reason as [`ErasureMatcher`]. Spans carry no separate resource/scope
/// attribute set to exclude: [`ravel_rspan::merge_attrs`] already folds
/// resource+scope+span attributes into [`ravel_rspan::SpanRecord::attrs`] at
/// ingest time, so matching that one field is matching the same full merged set
/// the span query path matches.
#[derive(Debug, Clone)]
pub struct SpanErasureMatcher {
    matchers: Vec<(String, String)>,
    window_start_ns: i64,
    window_end_ns: i64,
}

impl SpanErasureMatcher {
    pub fn from_request(request: &ErasureRequest) -> Self {
        SpanErasureMatcher {
            matchers: request
                .predicate
                .iter()
                .map(|m: &ErasurePredicateMatcher| (m.key.clone(), m.value.clone()))
                .collect(),
            window_start_ns: request.window_start_ns,
            window_end_ns: request.window_end_ns,
        }
    }

    /// Conjunction of exact-match matchers against a span's merged string
    /// attributes. Empty matchers matches nothing (fail-safe).
    pub fn matches_attrs(&self, attrs: &[(String, String)]) -> bool {
        if self.matchers.is_empty() {
            return false;
        }
        self.matchers
            .iter()
            .all(|(k, v)| attrs.iter().any(|(ak, av)| ak == k && av == v))
    }

    /// Half-open `[window_start_ns, window_end_ns)`; zero on either side is
    /// unset, identical to [`ErasureMatcher::ts_in_window`].
    pub fn ts_in_window(&self, ts_ns: i64) -> bool {
        let after_start = self.window_start_ns == 0 || ts_ns >= self.window_start_ns;
        let before_end = self.window_end_ns == 0 || ts_ns < self.window_end_ns;
        after_start && before_end
    }

    /// Whether this predicate drops a span whose merged attributes are
    /// `attrs`, using its `start_ts_ns` as the event time (query-path parity: the
    /// span query surface passes `start_ts_ns` to `is_erased_span` as
    /// `event_ts_ns`).
    pub fn drops_record(&self, attrs: &[(String, String)], ts_ns: i64) -> bool {
        self.matches_attrs(attrs) && self.ts_in_window(ts_ns)
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
/// physical under-erasure that diverged from the query path's scan-time exclusion
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
pub fn bucket_may_overlap(
    min_event_ts_ns: i64,
    max_event_ts_ns: i64,
    request: &ErasureRequest,
) -> bool {
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
    concurrency: usize,
) -> Result<LiveInputs> {
    match resolve_live_record(store, bucket, listing).await? {
        LiveRecord::RawL0 => {
            let inputs =
                crate::read::load_inputs(store, bucket, &listing.commit_keys, concurrency).await?;
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

/// The set of `request_id`s a bucket's live record already names in its
/// `drops`, when that live record is a [`RewriteRecord`]; `None` when the live
/// record is raw L0 or a compaction record (nothing has been rewritten yet, so
/// no request has been applied here). [`erasure_rewrite_bucket`] uses this to
/// skip a bucket already rewritten for every overlapping request,
/// so a completed generation never churns a fresh no-op record.
fn live_rewrite_applied_request_ids(live: &LiveInputs) -> Option<HashSet<String>> {
    match live {
        LiveInputs::Existing {
            body: LiveRecordBody::Rewrite(r),
            ..
        } => Some(r.drops.iter().map(|d| d.request_id.clone()).collect()),
        _ => None,
    }
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
            // `buffered`, not `buffer_unordered`: the catalogs stay aligned
            // one-to-one with `inputs` in canonical order (the merge's
            // tie-break depends on it) while `input_read_concurrency` loads
            // are in flight.
            let catalogs: Vec<InputCatalog> = stream_iter(
                inputs
                    .iter()
                    .map(|input| crate::read::load_input_catalog(store, config, input)),
            )
            .buffered(config.input_read_concurrency.max(1))
            .try_collect()
            .await?;
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

/// Resolve `live` down to the whole-object keys [`build_rewrite_logs`] /
/// [`build_rewrite_spans`] GET-and-decode-in-full and the [`RewriteSupersession`]
/// target the eventual `RewriteRecord` names -- the LOGS/SPANS sibling of
/// [`load_live_catalogs_and_target`], stopping short of that function's
/// per-run [`InputCatalog`] fetch because the logs/spans rewrite path (module
/// doc's scope reduction) decodes every live object whole rather than through
/// ranged per-run catalogs, so there is no catalog to build here at all.
fn live_input_object_keys_and_target(
    live: LiveInputs,
) -> Result<(Vec<String>, RewriteSupersession)> {
    match live {
        LiveInputs::RawL0(inputs) => {
            let object_keys = inputs
                .iter()
                .map(|i| keys::reconstruct_data_key(&i.record).map_err(MaintainError::from))
                .collect::<Result<Vec<_>>>()?;
            let identities = inputs
                .iter()
                .map(|i| CompactionInputIdentity {
                    writer_id: i.record.writer_id.clone(),
                    writer_epoch: i.record.writer_epoch,
                    writer_seq: i.record.writer_seq,
                })
                .collect();
            Ok((object_keys, RewriteSupersession::RawL0(identities)))
        }
        LiveInputs::Existing { key, body } => {
            let object_keys = body
                .parts()
                .iter()
                .map(|p| body.reconstruct_part_key(p))
                .collect::<Result<Vec<_>>>()?;
            Ok((object_keys, RewriteSupersession::Existing(key)))
        }
    }
}

/// One pending erasure request whose prefilter overlaps the bucket being
/// rewritten, paired with the label/window matcher built from its predicate.
/// ADR-0064 §4's "sibling case" rule requires every request in this slice --
/// not just the one that triggered the rewrite -- to end up with a `drops[]`
/// entry in the resulting `RewriteRecord`, even `dropped_count: 0` if nothing
/// in this bucket happened to match that particular request. Callers (the
/// driver) are responsible for building this batch once per bucket,
/// covering every pending request whose [`bucket_may_overlap`] prefilter
/// passed, never one request at a time.
#[derive(Debug, Clone)]
pub struct ApplicableRequest {
    pub request_id: String,
    pub matcher: ErasureMatcher,
}

/// [`ApplicableRequest`]'s LOGS sibling: same batching contract (every
/// prefilter-overlapping pending request, never one call per request), paired
/// with a [`LogErasureMatcher`] instead.
#[derive(Debug, Clone)]
pub struct ApplicableLogRequest {
    pub request_id: String,
    pub matcher: LogErasureMatcher,
}

/// [`ApplicableRequest`]'s SPANS sibling: same batching contract, paired with
/// a [`SpanErasureMatcher`] instead.
#[derive(Debug, Clone)]
pub struct ApplicableSpanRequest {
    pub request_id: String,
    pub matcher: SpanErasureMatcher,
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
                        MaintainError::Invariant(
                            "input_sample_count sum overflowed u64".to_string(),
                        )
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

/// The first request in `requests` (fixed order, first-match-wins, same
/// tie-break rationale as [`first_dropping_request`]) whose matcher drops a
/// log record with `attrs` at `ts_ns`, if any.
fn first_dropping_log_request(
    requests: &[ApplicableLogRequest],
    attrs: &[(String, LogAttrValue)],
    ts_ns: i64,
) -> Option<usize> {
    requests
        .iter()
        .position(|r| r.matcher.drops_record(attrs, ts_ns))
}

/// Decode-filter-reencode one LOGS bucket's live record set against every
/// applicable request's matcher, mirroring [`build_rewrite`]'s batching
/// contract (every request in `requests` gets a `drops[]` entry, even a zero
/// one) but not its per-run catalog machinery: this module's "single
/// whole-object GET" scope reduction (top-of-module doc) means every object
/// named by `object_keys` is fetched whole and every one of its records is
/// decoded via [`RlogReader::scan`] with an unbounded [`LogPredicate::TsRange`],
/// rather than range-fetching individual blocks. Records are pushed into one
/// [`RlogWriter`] with no indexed fields at all -- POSTINGS is a widen-only
/// pruning index (ADR-0013), so a rewritten part carrying none loses a rare
/// maintenance pass some query pruning, never correctness, the same shape of
/// tradeoff as this module's documented exemplar-drop for metrics.
pub async fn build_rewrite_logs(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    config: &CompactorConfig,
    object_keys: &[String],
    requests: &[ApplicableLogRequest],
    input_set_hash: &[u8; 32],
) -> Result<RewriteBuild> {
    let read_cfg = RlogConfig::default();
    let full_range = LogPredicate::TsRange {
        min_ns: i64::MIN,
        max_ns: i64::MAX,
    };

    let mut input_sample_count: u64 = 0;
    let mut output_sample_count: u64 = 0;
    let mut footer_record_count: u64 = 0;
    let mut dropped_counts = vec![0u64; requests.len()];

    let identity = rlog::compactor_identity(bucket, config);
    let mut writer = RlogWriter::new(read_cfg, identity);
    let mut first_stream_id: Option<LogStreamId> = None;
    let mut last_stream_id: Option<LogStreamId> = None;
    let mut any_survivor = false;

    for object_key in object_keys {
        let got = store.get(object_key, GetRange::Full).await?;
        // Input-side record-count authority: each input object's
        // RLOG footer declares its own `record_count`, written at flush/compact
        // time independently of the decode path this pass runs. Summing it and
        // cross-checking against the scan tally below closes the silent
        // data-loss gap the conservation gate cannot: that gate proves
        // survivors + drops == the scan's own count, so a decode that silently
        // dropped input records would satisfy it against a deflated input total
        // and permanently supersede the originals. Metrics catches the same
        // class via the catalog `run.sample_count` cross-check; logs/spans had
        // no equivalent until here.
        let footer = ravel_logseg::footer::open(got.data.as_ref())?;
        footer_record_count = footer_record_count
            .checked_add(footer.record_count)
            .ok_or_else(|| {
                MaintainError::Invariant("footer record_count sum overflowed u64".to_string())
            })?;
        let reader = RlogReader::new(got.data.as_ref(), &read_cfg)?;
        let (records, _stats) = reader.scan(&full_range)?;
        for record in records {
            input_sample_count = input_sample_count.checked_add(1).ok_or_else(|| {
                MaintainError::Invariant("input_sample_count sum overflowed u64".to_string())
            })?;
            match first_dropping_log_request(requests, &record.attrs, record.ts_ns) {
                Some(i) => dropped_counts[i] += 1,
                None => {
                    output_sample_count = output_sample_count.checked_add(1).ok_or_else(|| {
                        MaintainError::Invariant(
                            "output_sample_count sum overflowed u64".to_string(),
                        )
                    })?;
                    let sid = record.stream_id;
                    first_stream_id = Some(first_stream_id.map_or(sid, |f| f.min(sid)));
                    last_stream_id = Some(last_stream_id.map_or(sid, |l| l.max(sid)));
                    any_survivor = true;
                    writer.push(record)?;
                }
            }
        }
    }

    input_footer_cross_check(bucket, input_sample_count, footer_record_count)?;

    let parts = if any_survivor {
        // dry_run: true -- `publish_rewrite_record` PUTs `build.parts` itself,
        // only after its conservation gate passes, mirroring how
        // `build_rewrite_part` (metrics) also defers the PUT to that shared
        // publish path instead of writing here.
        let built = rlog::finalize_part(
            store,
            bucket,
            writer,
            first_stream_id,
            last_stream_id,
            input_set_hash,
            0,
            true,
        )
        .await?;
        vec![built]
    } else {
        Vec::new()
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

/// The first request in `requests` whose matcher drops a span with `attrs`
/// at `ts_ns`, if any -- the SPANS sibling of [`first_dropping_log_request`].
fn first_dropping_span_request(
    requests: &[ApplicableSpanRequest],
    attrs: &[(String, String)],
    ts_ns: i64,
) -> Option<usize> {
    requests
        .iter()
        .position(|r| r.matcher.drops_record(attrs, ts_ns))
}

/// Decode-filter-reencode one SPANS bucket's live record set against every
/// applicable request's matcher -- the SPANS sibling of [`build_rewrite_logs`],
/// same whole-object-GET scope reduction, [`RspanReader::scan`] with an
/// unbounded [`SpanQuery::ts_range`]. A span's whole [`ravel_rspan::SpanRecord`]
/// (including any links/events, which per that type's own field-shape and doc
/// comment live only as opaque blob entries inside `attrs`, never as separate
/// columns) is decoded, tested, and either pushed to the output writer intact
/// or dropped intact -- there is no per-field split that could leave a partial
/// span behind.
pub async fn build_rewrite_spans(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    config: &CompactorConfig,
    object_keys: &[String],
    requests: &[ApplicableSpanRequest],
    input_set_hash: &[u8; 32],
) -> Result<RewriteBuild> {
    let read_cfg = RspanConfig::default();
    let full_range = SpanQuery::ts_range(i64::MIN, i64::MAX);

    let mut input_sample_count: u64 = 0;
    let mut output_sample_count: u64 = 0;
    let mut footer_record_count: u64 = 0;
    let mut dropped_counts = vec![0u64; requests.len()];

    let identity = rspan_codec::compactor_identity(bucket, config);
    let mut writer = RspanWriter::new(read_cfg, identity);
    let mut trace_ids: HashSet<[u8; 16]> = HashSet::new();
    let mut any_survivor = false;

    for object_key in object_keys {
        let got = store.get(object_key, GetRange::Full).await?;
        // Input-side record-count authority: the RSPAN footer's
        // own `record_count`, summed and cross-checked against the scan tally
        // below. Same rationale as the logs path in `build_rewrite_logs`: the
        // output-side conservation gate cannot see an input record the decode
        // silently lost, and the originals get superseded, so the loss would be
        // permanent and invisible.
        let footer = ravel_rspan::open(got.data.as_ref())?;
        footer_record_count = footer_record_count
            .checked_add(footer.record_count)
            .ok_or_else(|| {
                MaintainError::Invariant("footer record_count sum overflowed u64".to_string())
            })?;
        let reader = RspanReader::new(got.data.as_ref(), &read_cfg)?;
        let (records, _stats) = reader.scan(&full_range)?;
        for record in records {
            input_sample_count = input_sample_count.checked_add(1).ok_or_else(|| {
                MaintainError::Invariant("input_sample_count sum overflowed u64".to_string())
            })?;
            match first_dropping_span_request(requests, &record.attrs, record.start_ts_ns) {
                Some(i) => dropped_counts[i] += 1,
                None => {
                    output_sample_count = output_sample_count.checked_add(1).ok_or_else(|| {
                        MaintainError::Invariant(
                            "output_sample_count sum overflowed u64".to_string(),
                        )
                    })?;
                    trace_ids.insert(record.trace_id);
                    any_survivor = true;
                    writer.push(record);
                }
            }
        }
    }

    input_footer_cross_check(bucket, input_sample_count, footer_record_count)?;

    let parts = if any_survivor {
        // dry_run: true, same reason as build_rewrite_logs.
        let built = rspan_codec::finalize_part(
            store,
            bucket,
            writer,
            input_set_hash,
            0,
            trace_ids.len() as u64,
            true,
        )
        .await?;
        vec![built]
    } else {
        Vec::new()
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

/// What one rewrite's `RewriteRecord` supersedes: either the raw L0 commit
/// records themselves (the bucket has never been compacted, mirroring
/// `CompactionRecord.inputs`), or a whole prior compaction/rewrite record
/// (the common case once a bucket has been compacted or previously
/// rewritten). Exactly one of `RewriteRecord.inputs` /
/// `.superseded_record_key` is ever set (`ravel_commit::erasure::validate_rewrite`),
/// and which one is a property of [`resolve_live_record`]'s result at the
/// point [`build_rewrite`] ran -- the driver threads it through
/// unchanged since [`build_rewrite`] itself does not need to know.
#[derive(Debug, Clone)]
pub enum RewriteSupersession {
    RawL0(Vec<CompactionInputIdentity>),
    Existing(String),
}

/// Input-side record-count conservation gate for logs/spans:
/// the scan tally `scanned_record_count` (what [`build_rewrite_logs`] /
/// [`build_rewrite_spans`] counted while decoding every live input object)
/// MUST equal `footer_record_count`, the sum of every input object's own
/// footer `record_count`. A mismatch means the decode silently lost (or
/// duplicated) input records: the output-side
/// [`MaintainError::ErasureConservationViolation`] gate cannot catch this
/// because it checks survivors + drops against that same deflated scan tally,
/// so a lossy decode balances against itself. Aborting here -- before
/// [`publish_rewrite_record`] writes any part or record -- keeps the originals
/// live and the `.dreq` pending, exactly as the output-side gate does. Metrics
/// needs no equivalent: [`build_rewrite`] cross-checks decoded runs against the
/// catalog `run.sample_count` already.
fn input_footer_cross_check(
    bucket: &Bucket,
    scanned_record_count: u64,
    footer_record_count: u64,
) -> Result<()> {
    if scanned_record_count != footer_record_count {
        return Err(MaintainError::ErasureInputConservationViolation {
            tenant_hash: hex::encode(bucket.tenant_hash.0),
            signal: bucket.signal.key_prefix().to_string(),
            shard: bucket.shard,
            ingest_hour_bucket: bucket.ingest_hour_bucket,
            scanned_record_count,
            footer_record_count,
        });
    }
    Ok(())
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
/// compaction.
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
    /// and the query path's scan-time exclusion keeps hiding the held data from
    /// queries in the meantime.
    Held,
    /// Every pending request that overlaps this bucket is already named in the
    /// bucket's live [`RewriteRecord`]'s `drops`: the bucket
    /// has already been rewritten for all of them, so there is nothing new to
    /// erase. Skipped without any build or publish. Without this skip, a second
    /// pass over an already-rewritten bucket recomputes a *different*
    /// `input_set_hash` (its live record set is now the rewrite output -- the
    /// superseded target -- not the original L0 inputs) and lands a no-op
    /// `RewriteRecord` (`dropped_count: 0`) superseding the prior one,
    /// repeatable every pass, churning generations while erasing nothing.
    AlreadyApplied,
    /// Built and published (or converged / abandoned): `parts` output parts
    /// written, `publish` records how the `RewriteRecord` PUT resolved.
    Rewritten {
        parts: usize,
        publish: PublishOutcome,
    },
}

/// Rewrite one sealed bucket against every pending erasure request that
/// applies to it (ADR-0064 decision 3). `pending` MUST already be
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
/// On a successful publish, invalidates [`MaintainMemo`]'s memoized terminal
/// state for the rewritten bucket's hour ([`MaintainMemo::invalidate`],
/// ADR-0065's "public invalidate seam"), so the interior zone re-evaluates on
/// the next tick instead of waiting for the re-verify cadence. This is the
/// The invalidation is applied uniformly after
/// every signal's publish, including metrics.
/// Scoped to [`PublishOutcome::Published`] only (not `Converged`/`Abandoned`):
/// a converged run observed a record that already existed, so whichever run
/// actually published it already invalidated the memo; an abandoned run wrote
/// nothing.
fn invalidate_after_publish(memo: &mut MaintainMemo, bucket: &Bucket, publish: &PublishOutcome) {
    if matches!(publish, PublishOutcome::Published) {
        memo.invalidate(
            bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
            &[bucket.ingest_hour_bucket],
        );
    }
}

/// Rewrite one sealed bucket against every pending erasure request that
/// applies to it (ADR-0064 decision 3). `pending` MUST already be
/// every request pending on this bucket's `(tenant_hash, signal)`
/// ([`pending_erasure_requests`]); this function does the per-bucket
/// [`bucket_may_overlap`] prefiltering itself, so the same `pending` slice is
/// reusable across every bucket a scan visits without re-listing `.dreq`s per
/// bucket.
///
/// This is the ADR-0064 section 4 "sibling case" entry point: every request
/// that overlaps the bucket is batched into one build/publish call, so a
/// bucket is never rewritten once per request -- one `RewriteRecord` per
/// bucket per rewrite generation, with a `drops[]` entry (possibly
/// `dropped_count: 0`) for every applicable request.
///
/// Every step through `overlapping` is signal-generic; only the
/// matcher/build step below dispatches on `bucket.signal` to the metrics
/// ([`build_rewrite`]), logs ([`build_rewrite_logs`]), or spans
/// ([`build_rewrite_spans`]) driver. [`MaintainError::Invariant`] surfaces
/// for any other signal: this dispatch scopes erasure
/// rewrite to metrics/logs/spans only, and profiles/alerts/audit have no
/// driver here to dispatch to.
pub async fn erasure_rewrite_bucket(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
    pending: &[PendingErasureRequest],
    memo: &mut MaintainMemo,
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
    let live = resolve_live_inputs(store, bucket, &listing, config.input_read_concurrency).await?;
    let (min_event_ts_ns, max_event_ts_ns) = live_input_event_bounds(&live);
    let overlapping: Vec<&PendingErasureRequest> = pending
        .iter()
        .filter(|p| bucket_may_overlap(min_event_ts_ns, max_event_ts_ns, &p.request))
        .collect();
    if overlapping.is_empty() {
        return Ok(ErasureRewriteOutcome::NoApplicableRequests);
    }

    // Idempotence/termination guard: if the bucket's live record
    // is already a RewriteRecord whose `drops` name every overlapping request,
    // this bucket has already been rewritten for all of them and there is
    // nothing new to erase. Re-publishing here would recompute a different
    // input_set_hash -- the live record set is now the rewrite output (the
    // superseded target), not the original L0 inputs -- and land a no-op
    // RewriteRecord (dropped_count 0) superseding the prior one, repeatable on
    // every maintenance pass. Skipping converges the generation instead. A
    // genuinely new request (one not yet named) still falls through to a
    // rewrite whose `drops` name every overlapping request, including the
    // already-applied ones with dropped_count 0, so the sibling-case
    // completion condition (ADR-0064 §4) stays satisfiable.
    if let Some(applied) = live_rewrite_applied_request_ids(&live)
        && overlapping
            .iter()
            .all(|p| applied.contains(&p.request.request_id))
    {
        return Ok(ErasureRewriteOutcome::AlreadyApplied);
    }

    let (parts, publish) = match bucket.signal {
        Signal::Metrics => {
            let applicable: Vec<ApplicableRequest> = overlapping
                .iter()
                .map(|p| ApplicableRequest {
                    request_id: p.request.request_id.clone(),
                    matcher: ErasureMatcher::from_request(&p.request),
                })
                .collect();

            let (catalogs, supersession) =
                load_live_catalogs_and_target(store, config, live).await?;

            let mut applied_request_ids: Vec<String> =
                applicable.iter().map(|r| r.request_id.clone()).collect();
            applied_request_ids.sort();
            let input_set_hash = match &supersession {
                RewriteSupersession::RawL0(ids) => {
                    erasure::compute_rewrite_input_set_hash(ids, None, &applied_request_ids)
                }
                RewriteSupersession::Existing(key) => erasure::compute_rewrite_input_set_hash(
                    &[],
                    Some(key.as_str()),
                    &applied_request_ids,
                ),
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
                publish_rewrite_record(store, config, clock, bucket, supersession, build, start_ns)
                    .await?;
            (parts, publish)
        }
        Signal::Logs => {
            let applicable: Vec<ApplicableLogRequest> = overlapping
                .iter()
                .map(|p| ApplicableLogRequest {
                    request_id: p.request.request_id.clone(),
                    matcher: LogErasureMatcher::from_request(&p.request),
                })
                .collect();

            let (object_keys, supersession) = live_input_object_keys_and_target(live)?;

            let mut applied_request_ids: Vec<String> =
                applicable.iter().map(|r| r.request_id.clone()).collect();
            applied_request_ids.sort();
            let input_set_hash = match &supersession {
                RewriteSupersession::RawL0(ids) => {
                    erasure::compute_rewrite_input_set_hash(ids, None, &applied_request_ids)
                }
                RewriteSupersession::Existing(key) => erasure::compute_rewrite_input_set_hash(
                    &[],
                    Some(key.as_str()),
                    &applied_request_ids,
                ),
            };

            let build = build_rewrite_logs(
                store,
                bucket,
                config,
                &object_keys,
                &applicable,
                &input_set_hash,
            )
            .await?;
            let parts = build.parts.len();
            let publish =
                publish_rewrite_record(store, config, clock, bucket, supersession, build, start_ns)
                    .await?;
            (parts, publish)
        }
        Signal::Spans => {
            let applicable: Vec<ApplicableSpanRequest> = overlapping
                .iter()
                .map(|p| ApplicableSpanRequest {
                    request_id: p.request.request_id.clone(),
                    matcher: SpanErasureMatcher::from_request(&p.request),
                })
                .collect();

            let (object_keys, supersession) = live_input_object_keys_and_target(live)?;

            let mut applied_request_ids: Vec<String> =
                applicable.iter().map(|r| r.request_id.clone()).collect();
            applied_request_ids.sort();
            let input_set_hash = match &supersession {
                RewriteSupersession::RawL0(ids) => {
                    erasure::compute_rewrite_input_set_hash(ids, None, &applied_request_ids)
                }
                RewriteSupersession::Existing(key) => erasure::compute_rewrite_input_set_hash(
                    &[],
                    Some(key.as_str()),
                    &applied_request_ids,
                ),
            };

            let build = build_rewrite_spans(
                store,
                bucket,
                config,
                &object_keys,
                &applicable,
                &input_set_hash,
            )
            .await?;
            let parts = build.parts.len();
            let publish =
                publish_rewrite_record(store, config, clock, bucket, supersession, build, start_ns)
                    .await?;
            (parts, publish)
        }
        other => {
            return Err(MaintainError::Invariant(format!(
                "erasure rewrite has no driver for signal {other:?} (erasure rewrite scopes metrics/logs/spans only)"
            )));
        }
    };

    invalidate_after_publish(memo, bucket, &publish);
    Ok(ErasureRewriteOutcome::Rewritten { parts, publish })
}

/// The catalog-resolver completion verdict for one bucket (ADR-0064 §4 F1).
///
/// Every field is derived through [`ravel_catalog::resolve_rewrite_supersession`]
/// -- the exact supersession chase a snapshot resolve
/// (`ravel_catalog::Catalog::process_bucket`) and the index fold use -- never
/// through the one-hop [`resolve_live_record`] the rewrite path uses to decode
/// its own generation. That is the whole point: completion cannot diverge from
/// what a query serves, because it is computed by the same code the query runs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BucketErasureCompletion {
    /// `request_id`s that overlap this bucket's live event range AND for which
    /// the catalog resolver's live view still serves a record that could carry
    /// the subject (a live raw L0 input, a live un-rewritten compaction part,
    /// or a live sibling rewrite whose `drops` do not name the request). Their
    /// `.done` must not be written.
    pub blocked: HashSet<String>,
    /// The catalog view could not be established for a reason that must block
    /// completion for every pending request, not just the overlapping ones: the
    /// bucket is under legal hold (ADR-0064 §6 -- the request stays pending
    /// until the hold clears). A list/decode/resolve failure surfaces as an
    /// `Err` from [`bucket_erasure_completion`] instead, so the caller can log
    /// and defer it exactly as the rewrite pass defers its own failures.
    pub unresolved: bool,
}

/// Whether one bucket, resolved through the SAME supersession logic the query
/// path uses, still serves any record that could contain the subject of any
/// pending erasure request (ADR-0064 §4, the 2026-08-08 F1 correction).
///
/// This is the completion gate the ADR requires the rewrite pass to derive
/// "through `resolve_rewrite_supersession` and `classify_bucket`, not a bucket
/// LIST in isolation." The rewrite pass classifies a bucket `AlreadyApplied`
/// off [`resolve_live_record`], which is one hop: it picks the live
/// compaction/rewrite record and never computes which raw L0 inputs a query
/// still resolves. So a bucket whose live rewrite names the request but whose
/// chain fails to exclude an L0 input the query still serves (the
/// absent-predecessor / partial-input case §4 names, or a live sibling rewrite)
/// reads "done" to the rewrite pass while a snapshot keeps serving the subject.
/// This function closes that gap by reconstructing the query's exact served set:
///
/// 1. `excluded` (raw L0 identities) and `superseded_records` (whole
///    compaction/rewrite records whose parts are dropped) are built exactly as
///    [`ravel_catalog::Catalog::process_bucket`] builds them -- compaction
///    inputs, then [`ravel_catalog::resolve_rewrite_supersession`] over every
///    rewrite record.
/// 2. A request is `blocked` if, restricted to its event-time window, the live
///    (non-excluded / non-superseded) view still serves: a raw L0 record, a
///    compaction part, or a rewrite output part from a rewrite that does not
///    name the request. Each of those is a record a snapshot would resolve and
///    that could still carry the subject.
///
/// A request whose window overlaps nothing live here is not blocked. A `.done`
/// is safe for a request only when NO in-scope bucket blocks it.
///
/// The front gates (`is_sealed`, tombstone, legal hold) mirror
/// [`erasure_rewrite_bucket`] so this reasons about exactly the buckets the
/// rewrite pass treats as in scope: an unsealed bucket is out of scope
/// (ADR-0064 decision 3 point 1, the documented completion gap); a tombstoned
/// bucket serves nothing; a held bucket keeps every request pending.
pub async fn bucket_erasure_completion(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    lease: &dyn LeaseCheck,
    bucket: &Bucket,
    pending: &[PendingErasureRequest],
) -> Result<BucketErasureCompletion> {
    let mut out = BucketErasureCompletion::default();
    if pending.is_empty() {
        return Ok(out);
    }
    // Unsealed: out of scope, exactly as `erasure_rewrite_bucket` defers it
    // (ADR-0064 decision 3 point 1). Its data is already unreturnable via the
    // query-time filter; blocking completion on it would let a continuously
    // ingesting tenant never complete, retaining the `.dreq` (and its subject)
    // forever, which is the failure ADR-0064 decision 5 exists to prevent.
    if !bucket.is_sealed(clock.now_ns(), config) {
        return Ok(out);
    }
    let listing = crate::read::list_bucket(store, bucket).await?;
    if listing.tombstone_key.is_some() {
        // A retention tombstone hides the whole bucket from every snapshot, so
        // it serves nothing and blocks no request.
        return Ok(out);
    }
    if bucket_is_held(&listing, lease) {
        // Legal hold wins over erasure (ADR-0064 §6): the request stays pending
        // and the query-time filter keeps hiding the data until the hold clears.
        out.unresolved = true;
        return Ok(out);
    }

    // Decode every compaction and rewrite record present, exactly as
    // `process_bucket` does before resolving supersession.
    let mut compaction_records: Vec<(String, CompactionRecord)> =
        Vec::with_capacity(listing.compaction_record_keys.len());
    for key in &listing.compaction_record_keys {
        compaction_records.push((key.clone(), get_compaction_record(store, key).await?));
    }
    let mut rewrite_records: Vec<(String, RewriteRecord)> =
        Vec::with_capacity(listing.rewrite_record_keys.len());
    for key in &listing.rewrite_record_keys {
        rewrite_records.push((key.clone(), get_rewrite_record(store, key).await?));
    }

    // Build the query path's two exclusion sets. `excluded` names raw L0
    // identities that a compaction or rewrite superseded; `superseded_records`
    // names whole compaction/rewrite records whose output parts are dropped.
    let mut excluded: HashSet<(String, u64, u64)> = HashSet::new();
    for (_, record) in &compaction_records {
        for input in &record.inputs {
            excluded.insert((
                input.writer_id.clone(),
                input.writer_epoch,
                input.writer_seq,
            ));
        }
    }
    let mut superseded_records: HashSet<String> = HashSet::new();
    if !rewrite_records.is_empty() {
        let compaction_by_key: HashMap<&str, &CompactionRecord> = compaction_records
            .iter()
            .map(|(k, r)| (k.as_str(), r))
            .collect();
        let rewrite_by_key: HashMap<&str, &RewriteRecord> = rewrite_records
            .iter()
            .map(|(k, r)| (k.as_str(), r))
            .collect();
        let prefix = keys::commit_shard_hour_prefix(
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
            bucket.ingest_hour_bucket,
        )?;
        for (rkey, record) in &rewrite_records {
            ravel_catalog::resolve_rewrite_supersession(
                rkey,
                record,
                &prefix,
                &compaction_by_key,
                &rewrite_by_key,
                &mut excluded,
                &mut superseded_records,
            )
            .map_err(|e| {
                MaintainError::Invariant(format!(
                    "erasure completion: catalog supersession resolution failed for {rkey}: {e}"
                ))
            })?;
        }
    }

    // The live view a snapshot would serve: raw L0 records whose identity no
    // record excluded, compaction records no rewrite superseded, and rewrite
    // records no newer rewrite superseded.
    let l0_inputs = crate::read::load_inputs(
        store,
        bucket,
        &listing.commit_keys,
        config.input_read_concurrency,
    )
    .await?;
    let live_l0: Vec<&crate::read::InputRecord> = l0_inputs
        .iter()
        .filter(|ir| {
            !excluded.contains(&(
                ir.record.writer_id.to_string(),
                ir.record.writer_epoch,
                ir.record.writer_seq,
            ))
        })
        .collect();
    let live_compactions: Vec<&CompactionRecord> = compaction_records
        .iter()
        .filter(|(key, _)| !superseded_records.contains(key))
        .map(|(_, record)| record)
        .collect();
    let live_rewrites: Vec<&RewriteRecord> = rewrite_records
        .iter()
        .filter(|(key, _)| !superseded_records.contains(key))
        .map(|(_, record)| record)
        .collect();

    for pending_request in pending {
        let request = &pending_request.request;
        if bucket_serves_subject(request, &live_l0, &live_compactions, &live_rewrites) {
            out.blocked.insert(request.request_id.clone());
        }
    }
    Ok(out)
}

/// Whether the catalog-resolved live view still serves any record that could
/// carry `request`'s subject, restricted to its event-time window. Used only by
/// [`bucket_erasure_completion`]; a `true` here means the subject is not yet
/// provably gone from this bucket for this request, so its `.done` is withheld.
fn bucket_serves_subject(
    request: &ErasureRequest,
    live_l0: &[&crate::read::InputRecord],
    live_compactions: &[&CompactionRecord],
    live_rewrites: &[&RewriteRecord],
) -> bool {
    // A live raw L0 record overlapping the window: un-rewritten input a
    // snapshot still resolves (the F1 blindness -- the one-hop resolver never
    // saw this, because it only inspects compaction/rewrite records).
    if live_l0.iter().any(|ir| {
        bucket_may_overlap(
            ir.record.min_event_ts_ns,
            ir.record.max_event_ts_ns,
            request,
        )
    }) {
        return true;
    }
    // A live compaction part overlapping the window: un-rewritten L1 data.
    if live_compactions.iter().any(|record| {
        record
            .parts
            .iter()
            .any(|part| bucket_may_overlap(part.min_event_ts_ns, part.max_event_ts_ns, request))
    }) {
        return true;
    }
    // A live rewrite that does NOT name this request, with an output part
    // overlapping the window: the sibling case (ADR-0064 §4). Overlap
    // harmlessness does not hold across a rewrite, so this output may still
    // hold the subject this request erases. A live rewrite that DOES name the
    // request dropped the subject and is safe.
    live_rewrites.iter().any(|record| {
        let names_request = record
            .drops
            .iter()
            .any(|drop| drop.request_id == request.request_id);
        !names_request
            && record
                .parts
                .iter()
                .any(|part| bucket_may_overlap(part.min_event_ts_ns, part.max_event_ts_ns, request))
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    //! Eight required tests: drop-exact-preserve-others,
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

    use std::sync::Arc;

    use ravel_catalog::{Catalog, CatalogConfig};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_segment::{SeriesInputV3, VERSION_V7};
    use ravel_types::{Label, METRIC_NAME_LABEL, SeriesId, TenantId, TimeRange};
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
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket(),
            &pending,
            &mut memo,
        )
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
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &CompactorConfig::default(),
            &lease,
            &b,
            &pending,
            &mut memo,
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
        let live = resolve_live_inputs(&store, &b, &listing, 1)
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

    /// Idempotence/termination regression: a completed
    /// generation must not churn. After one full `erasure_rewrite_bucket` pass
    /// publishes a `RewriteRecord` naming the request, a SECOND pass with the
    /// same still-pending request must return [`ErasureRewriteOutcome::AlreadyApplied`]
    /// and publish nothing -- the bucket's live rewrite record already names
    /// the request, so there is nothing new to erase.
    ///
    /// The existing `republishing_the_same_rewrite_converges_without_double_counting`
    /// test does NOT cover this: it drives `build_rewrite`/`publish_rewrite_record`
    /// twice from the *same* pre-publish snapshot (same L0 inputs, same
    /// input_set_hash), which converges via `AlreadyExists`. This test instead
    /// runs the whole driver twice, so the second pass observes the FIRST
    /// pass's published rewrite record as the live set and would (without the
    /// guard) recompute a different input_set_hash over that record as the
    /// superseded target, publishing a genuinely new second-generation no-op
    /// record rather than colliding with the first.
    ///
    /// Flip-line proof: deleting the `live_rewrite_applied_request_ids` skip
    /// guard in `erasure_rewrite_bucket` makes the second pass publish a second
    /// `RewriteRecord`, growing `rewrite_record_keys` to 2 and failing the
    /// `len() == 1` assertion below (and returning `Rewritten`, failing the
    /// `AlreadyApplied` assertion).
    #[tokio::test]
    async fn completed_request_does_not_republish_on_second_pass() {
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

        let request = erasure_request(7, "alpha");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let mut memo = MaintainMemo::with_default_interval();

        let outcome1 = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect("pass 1 succeeds");
        assert!(
            matches!(outcome1, ErasureRewriteOutcome::Rewritten { .. }),
            "first pass rewrites the bucket, got {outcome1:?}"
        );
        let after1 = crate::read::list_bucket(&store, &bucket())
            .await
            .expect("list after pass 1");
        assert_eq!(
            after1.rewrite_record_keys.len(),
            1,
            "exactly one RewriteRecord after the first pass"
        );

        let outcome2 = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect("pass 2 succeeds");
        assert_eq!(
            outcome2,
            ErasureRewriteOutcome::AlreadyApplied,
            "second pass over an already-rewritten bucket must skip, not republish"
        );
        let after2 = crate::read::list_bucket(&store, &bucket())
            .await
            .expect("list after pass 2");
        assert_eq!(
            after2.rewrite_record_keys.len(),
            1,
            "no new RewriteRecord generation on the second pass (finding-2: no churn)"
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
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket(),
            &pending,
            &mut memo,
        )
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
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket(),
            &pending,
            &mut memo,
        )
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
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket(),
            &pending,
            &mut memo,
        )
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

    // -----------------------------------------------------------------
    // LOGS (RLOG)
    // -----------------------------------------------------------------

    use ravel_logseg::LogRecord;
    use ravel_logseg::writer::ObjectIdentity as LogObjectIdentity;
    use ravel_rspan::writer::ObjectIdentity as SpanObjectIdentity;
    use ravel_rspan::{SpanRecord, StatusCode};

    fn logs_bucket() -> Bucket {
        Bucket::new(tenant_hash(), Signal::Logs, SHARD, HOUR)
    }

    fn spans_bucket() -> Bucket {
        Bucket::new(tenant_hash(), Signal::Spans, SHARD, HOUR)
    }

    /// A synthetic log stream's id and canonical resource+scope blob,
    /// identical in shape to `rlog.rs`'s own `stream_ident` test fixture.
    fn log_stream_ident(n: u32) -> (LogStreamId, Vec<u8>) {
        let res = vec![(
            "service.name".to_string(),
            LogAttrValue::Str(format!("svc{n}")),
        )];
        let id = ravel_types::logstream::log_stream_id(&res, "scope", "1", &[]);
        let blob = ravel_logseg::stream_attrs_bytes(&res, "scope", "1", &[]);
        (id, blob)
    }

    fn log_record(
        stream_n: u32,
        ts: i64,
        body: &str,
        attrs: Vec<(String, LogAttrValue)>,
    ) -> LogRecord {
        let (stream_id, stream_attrs) = log_stream_ident(stream_n);
        LogRecord {
            stream_id,
            stream_attrs,
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: body.into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        }
    }

    fn logs_erasure_request(id_seed: u128, key: &str, value: &str) -> ErasureRequest {
        ErasureRequest {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Logs) as i32,
            request_id: Uuid::from_u128(id_seed).to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: key.to_string(),
                value: value.to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        }
    }

    /// Seed one L0 `.rlog` input (data object + commit record), matching the
    /// shape `rlog.rs`'s own test module writes for a real ingest shard.
    async fn seed_logs(store: &dyn ObjectStoreBackend, seq: u64, records: &[LogRecord]) {
        seed_logs_indexed(store, seq, records, &[]).await;
    }

    /// [`seed_logs`] with an explicit POSTINGS indexed-field list, so a fixture
    /// can seed an input that really carries a POSTINGS section
    /// (ADR-0049 decision 3: opt-in per field, never automatic).
    async fn seed_logs_indexed(
        store: &dyn ObjectStoreBackend,
        seq: u64,
        records: &[LogRecord],
        indexed_fields: &[&str],
    ) {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(u128::from(seq));
        let identity = LogObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let mut w = RlogWriter::new(RlogConfig::default(), identity)
            .with_indexed_fields(indexed_fields.iter().map(|f| (*f).to_string()).collect());
        for r in records {
            w.push(r.clone()).expect("push log record");
        }
        let bytes = Bytes::from(w.finish().expect("finish L0"));
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data_key = keys::data_key(
            &th,
            Signal::Logs,
            SHARD,
            writer_id,
            EPOCH,
            seq,
            &content_hash,
        )
        .expect("data key");
        store
            .put(&data_key, bytes.clone(), PutOptions::default())
            .await
            .expect("put data object");

        let mut ids: HashSet<LogStreamId> = HashSet::new();
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for r in records {
            ids.insert(r.stream_id);
            min_ts = min_ts.min(r.ts_ns);
            max_ts = max_ts.max(r.ts_ns);
        }
        let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Logs,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: ids.len() as u64,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: OUTPUT_FORMAT_VERSION,
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

    /// [`read_rewrite_record`]'s generalization to any bucket, needed once
    /// this module's tests span three buckets (metrics/logs/spans) instead
    /// of the one hardcoded metrics `bucket()` the original helper reads.
    async fn read_rewrite_record_for(store: &dyn ObjectStoreBackend, b: &Bucket) -> RewriteRecord {
        let listing = crate::read::list_bucket(store, b)
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

    async fn decode_logs_part(store: &dyn ObjectStoreBackend, object_key: &str) -> Vec<LogRecord> {
        let got = store
            .get(object_key, GetRange::Full)
            .await
            .expect("get part");
        let reader = RlogReader::new(got.data.as_ref(), &RlogConfig::default()).expect("open");
        let (records, _stats) = reader.scan(&LogPredicate::And(Vec::new())).expect("scan");
        records
    }

    /// Drops exactly the log record matched by the erasure predicate and
    /// leaves every other record in the bucket byte-for-byte (via
    /// `LogRecord`'s own `PartialEq`) intact. Flip-line proof:
    /// `LogErasureMatcher::matches_attrs` returning `false`
    /// unconditionally makes the "checkout" record survive, growing the
    /// survivor set to two records and failing the `assert_eq!(survivors,
    /// vec![kept])` below.
    #[tokio::test]
    async fn logs_rewrite_drops_matching_preserves_others_bit_identically() {
        let store = MemoryStore::new();
        let dropped = log_record(
            1,
            10,
            "checkout failed",
            vec![(
                "service".to_string(),
                LogAttrValue::Str("checkout".to_string()),
            )],
        );
        let kept = log_record(
            2,
            20,
            "shipping ok",
            vec![(
                "service".to_string(),
                LogAttrValue::Str("shipping".to_string()),
            )],
        );
        seed_logs(&store, 1, &[dropped.clone(), kept.clone()]).await;

        let request = logs_erasure_request(1, "service", "checkout");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &logs_bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect("rewrite");

        let (parts, publish) = match outcome {
            ErasureRewriteOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(parts, 1);
        assert_eq!(publish, PublishOutcome::Published);

        let record = read_rewrite_record_for(&store, &logs_bucket()).await;
        assert_eq!(record.drops.len(), 1);
        assert_eq!(
            record.drops[0].dropped_count, 1,
            "only the checkout record is dropped"
        );

        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");
        let survivors = decode_logs_part(&store, &part_key).await;
        assert_eq!(
            survivors,
            vec![kept],
            "the shipping record must survive bit-identically and alone"
        );
    }

    /// A hand-built `RewriteBuild` for a LOGS bucket whose
    /// `output_sample_count + dropped_count` (1 + 1 = 2) does not equal
    /// `input_sample_count` (5) must abort before any write -- the same
    /// gate the metrics test above exercises, proving it is not
    /// accidentally scoped to metrics-shaped buckets. Flip-line proof: `if
    /// reconstructed != build.input_sample_count` in
    /// `publish_rewrite_record` flipped to `==` (or deleted) lets this
    /// publish `Ok`, failing the `expect_err`.
    #[tokio::test]
    async fn logs_conservation_mismatch_aborts_rewrite_publish() {
        let store = MemoryStore::new();
        let clock = FixedClock::new(0);
        let config = CompactorConfig::default();
        let build = RewriteBuild {
            parts: Vec::new(),
            input_sample_count: 5,
            output_sample_count: 1,
            drops: vec![RewriteDrop {
                request_id: Uuid::from_u128(9).to_string(),
                dropped_count: 1,
            }],
        };

        let err = publish_rewrite_record(
            &store,
            &config,
            &clock,
            &logs_bucket(),
            RewriteSupersession::RawL0(Vec::new()),
            build,
            0,
        )
        .await
        .expect_err("mismatched conservation must abort");

        assert!(
            matches!(err, MaintainError::ErasureConservationViolation { .. }),
            "expected ErasureConservationViolation, got {err:?}"
        );
        assert!(
            list_all(&store, "").await.expect("list").is_empty(),
            "an aborted rewrite must publish nothing, not even the record"
        );
    }

    /// A LOGS bucket under legal hold is skipped outright, same as the
    /// metrics case above -- `bucket_is_held` gates every signal's rewrite
    /// before any signal-specific dispatch runs. Flip-line proof: removing
    /// the `if bucket_is_held(&listing, lease) { return
    /// Ok(ErasureRewriteOutcome::Held); }` gate makes this proceed to
    /// `Rewritten`, failing `assert_eq!(outcome, ErasureRewriteOutcome::Held)`.
    #[tokio::test]
    async fn logs_legal_hold_skips_bucket_leaves_dreq_pending() {
        let store = MemoryStore::new();
        seed_logs(
            &store,
            1,
            &[log_record(
                1,
                10,
                "checkout failed",
                vec![(
                    "service".to_string(),
                    LogAttrValue::Str("checkout".to_string()),
                )],
            )],
        )
        .await;

        let request = logs_erasure_request(1, "service", "checkout");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let b = logs_bucket();
        let prefix =
            keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
                .expect("prefix");
        let lease = HoldPrefix(prefix);

        let before = list_all(&store, "").await.expect("list before");

        let clock = FixedClock::new(sealed_now_ns());
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &CompactorConfig::default(),
            &lease,
            &b,
            &pending,
            &mut memo,
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

    /// A sealed LOGS bucket's live input set is normally >=2 L0 commits (one
    /// per ingest flush); `build_rewrite_logs` must merge every input's
    /// surviving records into ONE output part, not one part per input.
    /// Flip-line proof: moving `RlogWriter::new(read_cfg, identity)` inside
    /// the `for object_key in object_keys` loop (instead of once before it)
    /// would finish and push a part per input, making `parts` come out `2`
    /// instead of `1`, failing `assert_eq!(parts, 1, ...)`.
    #[tokio::test]
    async fn logs_rewrite_merges_records_across_multiple_l0_inputs_into_one_part() {
        let store = MemoryStore::new();
        seed_logs(&store, 1, &[log_record(1, 10, "a", vec![])]).await;
        seed_logs(&store, 2, &[log_record(2, 20, "b", vec![])]).await;

        // Matches nothing: the merge bug reproduces even with zero drops.
        let request = logs_erasure_request(1, "service", "unrelated");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &logs_bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect("rewrite must merge >=2 live L0 inputs into one part");

        let (parts, publish) = match outcome {
            ErasureRewriteOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(
            parts, 1,
            "both L0 inputs must merge into a single output part"
        );
        assert_eq!(publish, PublishOutcome::Published);

        let record = read_rewrite_record_for(&store, &logs_bucket()).await;
        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");
        let survivors = decode_logs_part(&store, &part_key).await;
        assert_eq!(
            survivors.len(),
            2,
            "records from both inputs must survive in the one merged part"
        );
    }

    /// Running the same LOGS rewrite twice from the same pre-publish state
    /// converges on one `RewriteRecord`, never a double count of dropped
    /// records -- the LOGS sibling of the metrics
    /// `republishing_the_same_rewrite_converges_without_double_counting`
    /// test above. Flip-line proof: in `publish_rewrite_record`, routing
    /// `Err(StoreError::AlreadyExists)` to a hard `Err` instead of
    /// `resolve_already_exists_rewrite` makes the second
    /// `publish_rewrite_record` call return `Err`, failing
    /// `.expect("publish 2")`.
    #[tokio::test]
    async fn logs_republishing_same_rewrite_converges_without_double_counting() {
        let store = MemoryStore::new();
        let dropped = log_record(
            1,
            10,
            "checkout failed",
            vec![(
                "service".to_string(),
                LogAttrValue::Str("checkout".to_string()),
            )],
        );
        let kept = log_record(
            2,
            20,
            "shipping ok",
            vec![(
                "service".to_string(),
                LogAttrValue::Str("shipping".to_string()),
            )],
        );
        seed_logs(&store, 1, &[dropped, kept]).await;

        let request = logs_erasure_request(2, "service", "checkout");
        let applicable = vec![ApplicableLogRequest {
            request_id: request.request_id.clone(),
            matcher: LogErasureMatcher::from_request(&request),
        }];

        let b = logs_bucket();
        let config = CompactorConfig::default();
        let listing = crate::read::list_bucket(&store, &b)
            .await
            .expect("list bucket");
        let live = resolve_live_inputs(&store, &b, &listing, 1)
            .await
            .expect("resolve live inputs");
        let (object_keys, supersession) =
            live_input_object_keys_and_target(live).expect("object keys");

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

        let build1 = build_rewrite_logs(
            &store,
            &b,
            &config,
            &object_keys,
            &applicable,
            &input_set_hash,
        )
        .await
        .expect("build 1");
        let outcome1 =
            publish_rewrite_record(&store, &config, &clock, &b, supersession.clone(), build1, 0)
                .await
                .expect("publish 1");
        assert_eq!(outcome1, PublishOutcome::Published);

        let build2 = build_rewrite_logs(
            &store,
            &b,
            &config,
            &object_keys,
            &applicable,
            &input_set_hash,
        )
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

        let record = read_rewrite_record_for(&store, &b).await;
        assert_eq!(record.drops.len(), 1);
        assert_eq!(
            record.drops[0].dropped_count, 1,
            "dropped_count must not double-count across the two runs"
        );
    }

    // -----------------------------------------------------------------
    // SPANS (RSPAN)
    // -----------------------------------------------------------------

    fn span_record(t: u8, s: u8, start: i64, end: i64, attrs: Vec<(String, String)>) -> SpanRecord {
        SpanRecord {
            trace_id: [t; 16],
            span_id: [s; 8],
            parent_span_id: None,
            name: format!("op-{s}"),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs,
        }
    }

    fn spans_erasure_request(id_seed: u128, key: &str, value: &str) -> ErasureRequest {
        ErasureRequest {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Spans) as i32,
            request_id: Uuid::from_u128(id_seed).to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: key.to_string(),
                value: value.to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        }
    }

    /// Seed one L0 `.rspan` input (data object + commit record), matching
    /// the shape `rspan_codec.rs`'s own test module writes for a real span
    /// ingest shard.
    async fn seed_spans(store: &dyn ObjectStoreBackend, seq: u64, records: &[SpanRecord]) {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(u128::from(seq));
        let identity = SpanObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let mut w = RspanWriter::new(RspanConfig::default(), identity);
        for r in records {
            w.push(r.clone());
        }
        let bytes = Bytes::from(w.finish().expect("finish L0"));
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data_key = keys::data_key(
            &th,
            Signal::Spans,
            SHARD,
            writer_id,
            EPOCH,
            seq,
            &content_hash,
        )
        .expect("data key");
        store
            .put(&data_key, bytes.clone(), PutOptions::default())
            .await
            .expect("put data object");

        let mut traces: HashSet<[u8; 16]> = HashSet::new();
        let mut min_start = i64::MAX;
        let mut max_end = i64::MIN;
        for r in records {
            traces.insert(r.trace_id);
            min_start = min_start.min(r.start_ts_ns);
            max_end = max_end.max(r.end_ts_ns);
        }
        let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Spans,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: traces.len() as u64,
            min_event_ts_ns: min_start,
            max_event_ts_ns: max_end,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: OUTPUT_FORMAT_VERSION,
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

    async fn decode_spans_part(
        store: &dyn ObjectStoreBackend,
        object_key: &str,
    ) -> Vec<SpanRecord> {
        let got = store
            .get(object_key, GetRange::Full)
            .await
            .expect("get part");
        let reader = RspanReader::new(got.data.as_ref(), &RspanConfig::default()).expect("open");
        let (records, _stats) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan");
        records
    }

    /// Drops exactly the span matched by the erasure predicate and leaves
    /// every other span in the bucket byte-for-byte (via `SpanRecord`'s own
    /// `PartialEq`) intact -- the SPANS sibling of the LOGS drop test
    /// above. Flip-line proof: `SpanErasureMatcher::matches_attrs`
    /// returning `false` unconditionally makes the matched span survive,
    /// failing the `assert_eq!(survivors, vec![kept])` below.
    #[tokio::test]
    async fn spans_rewrite_drops_matching_preserves_others_bit_identically() {
        let store = MemoryStore::new();
        let dropped = span_record(
            1,
            1,
            10,
            15,
            vec![("service".to_string(), "checkout".to_string())],
        );
        let kept = span_record(
            2,
            1,
            20,
            25,
            vec![("service".to_string(), "shipping".to_string())],
        );
        seed_spans(&store, 1, &[dropped.clone(), kept.clone()]).await;

        let request = spans_erasure_request(1, "service", "checkout");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &spans_bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect("rewrite");

        let (parts, publish) = match outcome {
            ErasureRewriteOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(parts, 1);
        assert_eq!(publish, PublishOutcome::Published);

        let record = read_rewrite_record_for(&store, &spans_bucket()).await;
        assert_eq!(record.drops.len(), 1);
        assert_eq!(
            record.drops[0].dropped_count, 1,
            "only the checkout span is dropped"
        );

        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");
        let survivors = decode_spans_part(&store, &part_key).await;
        assert_eq!(
            survivors,
            vec![kept],
            "the shipping span must survive bit-identically and alone"
        );
    }

    /// A hand-built `RewriteBuild` for a SPANS bucket whose
    /// `output_sample_count + dropped_count` (1 + 1 = 2) does not equal
    /// `input_sample_count` (5) must abort before any write -- the SPANS
    /// sibling of the LOGS/metrics conservation-abort tests above,
    /// confirming the shared gate is not scoped to any one signal.
    /// Flip-line proof: `if reconstructed != build.input_sample_count` in
    /// `publish_rewrite_record` flipped to `==` (or deleted) lets this
    /// publish `Ok`, failing the `expect_err`.
    #[tokio::test]
    async fn spans_conservation_mismatch_aborts_rewrite_publish() {
        let store = MemoryStore::new();
        let clock = FixedClock::new(0);
        let config = CompactorConfig::default();
        let build = RewriteBuild {
            parts: Vec::new(),
            input_sample_count: 5,
            output_sample_count: 1,
            drops: vec![RewriteDrop {
                request_id: Uuid::from_u128(9).to_string(),
                dropped_count: 1,
            }],
        };

        let err = publish_rewrite_record(
            &store,
            &config,
            &clock,
            &spans_bucket(),
            RewriteSupersession::RawL0(Vec::new()),
            build,
            0,
        )
        .await
        .expect_err("mismatched conservation must abort");

        assert!(
            matches!(err, MaintainError::ErasureConservationViolation { .. }),
            "expected ErasureConservationViolation, got {err:?}"
        );
        assert!(
            list_all(&store, "").await.expect("list").is_empty(),
            "an aborted rewrite must publish nothing, not even the record"
        );
    }

    /// A SPANS bucket under legal hold is skipped outright, same as the
    /// LOGS/metrics cases above. Flip-line proof: removing the `if
    /// bucket_is_held(&listing, lease) { return
    /// Ok(ErasureRewriteOutcome::Held); }` gate makes this proceed to
    /// `Rewritten`, failing `assert_eq!(outcome, ErasureRewriteOutcome::Held)`.
    #[tokio::test]
    async fn spans_legal_hold_skips_bucket_leaves_dreq_pending() {
        let store = MemoryStore::new();
        seed_spans(
            &store,
            1,
            &[span_record(
                1,
                1,
                10,
                15,
                vec![("service".to_string(), "checkout".to_string())],
            )],
        )
        .await;

        let request = spans_erasure_request(1, "service", "checkout");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let b = spans_bucket();
        let prefix =
            keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
                .expect("prefix");
        let lease = HoldPrefix(prefix);

        let before = list_all(&store, "").await.expect("list before");

        let clock = FixedClock::new(sealed_now_ns());
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &CompactorConfig::default(),
            &lease,
            &b,
            &pending,
            &mut memo,
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

    /// A sealed SPANS bucket's live input set is normally >=2 L0 commits;
    /// `build_rewrite_spans` must merge every input's surviving spans into
    /// ONE output part, not one part per input -- the SPANS sibling of the
    /// LOGS merge test above. Flip-line proof: moving `RspanWriter::new(read_cfg,
    /// identity)` inside the `for object_key in object_keys` loop (instead
    /// of once before it) would finish and push a part per input, making
    /// `parts` come out `2` instead of `1`, failing `assert_eq!(parts, 1, ...)`.
    #[tokio::test]
    async fn spans_rewrite_merges_records_across_multiple_l0_inputs_into_one_part() {
        let store = MemoryStore::new();
        seed_spans(&store, 1, &[span_record(1, 1, 10, 15, vec![])]).await;
        seed_spans(&store, 2, &[span_record(2, 1, 20, 25, vec![])]).await;

        // Matches nothing: the merge bug reproduces even with zero drops.
        let request = spans_erasure_request(1, "service", "unrelated");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &spans_bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect("rewrite must merge >=2 live L0 inputs into one part");

        let (parts, publish) = match outcome {
            ErasureRewriteOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(
            parts, 1,
            "both L0 inputs must merge into a single output part"
        );
        assert_eq!(publish, PublishOutcome::Published);

        let record = read_rewrite_record_for(&store, &spans_bucket()).await;
        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");
        let survivors = decode_spans_part(&store, &part_key).await;
        assert_eq!(
            survivors.len(),
            2,
            "spans from both inputs must survive in the one merged part"
        );
    }

    /// Running the same SPANS rewrite twice from the same pre-publish state
    /// converges on one `RewriteRecord`, never a double count of dropped
    /// spans -- the SPANS sibling of the LOGS/metrics idempotence tests
    /// above. Flip-line proof: in `publish_rewrite_record`, routing
    /// `Err(StoreError::AlreadyExists)` to a hard `Err` instead of
    /// `resolve_already_exists_rewrite` makes the second
    /// `publish_rewrite_record` call return `Err`, failing
    /// `.expect("publish 2")`.
    #[tokio::test]
    async fn spans_republishing_same_rewrite_converges_without_double_counting() {
        let store = MemoryStore::new();
        let dropped = span_record(
            1,
            1,
            10,
            15,
            vec![("service".to_string(), "checkout".to_string())],
        );
        let kept = span_record(
            2,
            1,
            20,
            25,
            vec![("service".to_string(), "shipping".to_string())],
        );
        seed_spans(&store, 1, &[dropped, kept]).await;

        let request = spans_erasure_request(2, "service", "checkout");
        let applicable = vec![ApplicableSpanRequest {
            request_id: request.request_id.clone(),
            matcher: SpanErasureMatcher::from_request(&request),
        }];

        let b = spans_bucket();
        let config = CompactorConfig::default();
        let listing = crate::read::list_bucket(&store, &b)
            .await
            .expect("list bucket");
        let live = resolve_live_inputs(&store, &b, &listing, 1)
            .await
            .expect("resolve live inputs");
        let (object_keys, supersession) =
            live_input_object_keys_and_target(live).expect("object keys");

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

        let build1 = build_rewrite_spans(
            &store,
            &b,
            &config,
            &object_keys,
            &applicable,
            &input_set_hash,
        )
        .await
        .expect("build 1");
        let outcome1 =
            publish_rewrite_record(&store, &config, &clock, &b, supersession.clone(), build1, 0)
                .await
                .expect("publish 1");
        assert_eq!(outcome1, PublishOutcome::Published);

        let build2 = build_rewrite_spans(
            &store,
            &b,
            &config,
            &object_keys,
            &applicable,
            &input_set_hash,
        )
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

        let record = read_rewrite_record_for(&store, &b).await;
        assert_eq!(record.drops.len(), 1);
        assert_eq!(
            record.drops[0].dropped_count, 1,
            "dropped_count must not double-count across the two runs"
        );
    }

    /// Span links/events have no dedicated columns in this format (per
    /// `SpanRecord`'s own doc comment, they live only as an opaque blob
    /// entry inside `attrs`, never as separate fields), so
    /// `build_rewrite_spans` cannot partially strip a span: it decodes,
    /// tests, and either pushes the WHOLE record or drops the WHOLE
    /// record, never a record with some attrs removed. This seeds a
    /// surviving span whose `attrs` carries such a blob entry, and asserts
    /// it comes through byte-for-byte alongside the rest of the record.
    /// Flip-line proof: `build_rewrite_spans`'s survivor arm (`None =>
    /// {...; writer.push(record)}`) pushing anything other than the
    /// original, unmodified `record` -- e.g. a copy with `attrs` filtered
    /// down to just the matched key -- would still satisfy every other
    /// assertion here but drop the blob entry, failing the
    /// `assert_eq!(survivors, vec![kept])` below.
    #[tokio::test]
    async fn spans_links_events_survive_or_drop_atomically_with_whole_record() {
        let store = MemoryStore::new();
        let dropped = span_record(
            1,
            1,
            10,
            15,
            vec![
                ("service".to_string(), "checkout".to_string()),
                ("otel.span.links.blob".to_string(), "linkblobA".to_string()),
            ],
        );
        let kept = span_record(
            2,
            1,
            20,
            25,
            vec![
                ("service".to_string(), "shipping".to_string()),
                (
                    "otel.span.events.blob".to_string(),
                    "eventblobB".to_string(),
                ),
            ],
        );
        seed_spans(&store, 1, &[dropped.clone(), kept.clone()]).await;

        let request = spans_erasure_request(1, "service", "checkout");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &spans_bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect("rewrite");

        let (parts, publish) = match outcome {
            ErasureRewriteOutcome::Rewritten { parts, publish } => (parts, publish),
            other => panic!("expected Rewritten, got {other:?}"),
        };
        assert_eq!(parts, 1);
        assert_eq!(publish, PublishOutcome::Published);

        let record = read_rewrite_record_for(&store, &spans_bucket()).await;
        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");
        let mut survivors = decode_spans_part(&store, &part_key).await;
        // The codec canonicalizes attr order on encode (a column-oriented
        // detail orthogonal to what this test proves), so compare with attrs
        // sorted on both sides rather than requiring the original push order.
        for s in &mut survivors {
            s.attrs.sort();
        }
        let mut expected = kept;
        expected.attrs.sort();
        assert_eq!(
            survivors,
            vec![expected],
            "the surviving span's events/links blob attr must come through whole, unstripped"
        );

        let leaked_links = survivors
            .iter()
            .any(|s| s.attrs.iter().any(|(k, _)| k == "otel.span.links.blob"));
        assert!(
            !leaked_links,
            "the dropped span's links blob must not survive in any partial form"
        );
    }

    // -----------------------------------------------------------------
    // Input-side record-count cross-check
    // -----------------------------------------------------------------

    /// Re-encode an RLOG object's footer with `record_count` bumped by
    /// `delta`, recomputing the trailer crc so the object still opens cleanly.
    /// This stands in for a decode that silently loses `delta` input records:
    /// the footer (the honest authority, written at flush time) over-declares
    /// relative to what a scan can decode. Body bytes before the footer are
    /// left untouched, so the block data a scan reads is unchanged.
    fn bump_rlog_footer_record_count(bytes: &[u8], delta: u64) -> Bytes {
        use ravel_logseg::footer::{TRAILER_LEN, open, write_footer_and_trailer};
        let total = bytes.len();
        let footer_len = u32::from_le_bytes(
            bytes[total - TRAILER_LEN..total - TRAILER_LEN + 4]
                .try_into()
                .expect("footer_len bytes"),
        ) as usize;
        let footer_start = total - TRAILER_LEN - footer_len;
        let mut footer = open(bytes).expect("open rlog footer");
        footer.record_count += delta;
        let mut out = bytes[..footer_start].to_vec();
        write_footer_and_trailer(&mut out, &footer);
        Bytes::from(out)
    }

    /// The RSPAN sibling of [`bump_rlog_footer_record_count`].
    fn bump_rspan_footer_record_count(bytes: &[u8], delta: u64) -> Bytes {
        use ravel_rspan::footer::{TRAILER_LEN, open, write_footer_and_trailer};
        let total = bytes.len();
        let footer_len = u32::from_le_bytes(
            bytes[total - TRAILER_LEN..total - TRAILER_LEN + 4]
                .try_into()
                .expect("footer_len bytes"),
        ) as usize;
        let footer_start = total - TRAILER_LEN - footer_len;
        let mut footer = open(bytes).expect("open rspan footer");
        footer.record_count += delta;
        let mut out = bytes[..footer_start].to_vec();
        write_footer_and_trailer(&mut out, &footer);
        Bytes::from(out)
    }

    /// Seed one L0 `.rlog` input exactly like [`seed_logs`], except the stored
    /// data object's footer over-declares its `record_count` by one. The
    /// commit record and the data key still describe the honest object (same
    /// content hash, honest `sample_count`), so the bucket resolves and scans
    /// normally; only [`build_rewrite_logs`]'s footer-vs-scan cross-check sees
    /// the discrepancy.
    async fn seed_logs_footer_overcount(
        store: &dyn ObjectStoreBackend,
        seq: u64,
        records: &[LogRecord],
    ) {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(u128::from(seq));
        let identity = LogObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let mut w = RlogWriter::new(RlogConfig::default(), identity);
        for r in records {
            w.push(r.clone()).expect("push log record");
        }
        let bytes = Bytes::from(w.finish().expect("finish L0"));
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data_key = keys::data_key(
            &th,
            Signal::Logs,
            SHARD,
            writer_id,
            EPOCH,
            seq,
            &content_hash,
        )
        .expect("data key");
        let bumped = bump_rlog_footer_record_count(&bytes, 1);
        store
            .put(&data_key, bumped, PutOptions::default())
            .await
            .expect("put data object");

        let mut ids: HashSet<LogStreamId> = HashSet::new();
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for r in records {
            ids.insert(r.stream_id);
            min_ts = min_ts.min(r.ts_ns);
            max_ts = max_ts.max(r.ts_ns);
        }
        let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Logs,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: ids.len() as u64,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: OUTPUT_FORMAT_VERSION,
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

    /// The RSPAN sibling of [`seed_logs_footer_overcount`].
    async fn seed_spans_footer_overcount(
        store: &dyn ObjectStoreBackend,
        seq: u64,
        records: &[SpanRecord],
    ) {
        let th = tenant_hash();
        let writer_id = Uuid::from_u128(u128::from(seq));
        let identity = SpanObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let mut w = RspanWriter::new(RspanConfig::default(), identity);
        for r in records {
            w.push(r.clone());
        }
        let bytes = Bytes::from(w.finish().expect("finish L0"));
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data_key = keys::data_key(
            &th,
            Signal::Spans,
            SHARD,
            writer_id,
            EPOCH,
            seq,
            &content_hash,
        )
        .expect("data key");
        let bumped = bump_rspan_footer_record_count(&bytes, 1);
        store
            .put(&data_key, bumped, PutOptions::default())
            .await
            .expect("put data object");

        let mut traces: HashSet<[u8; 16]> = HashSet::new();
        let mut min_start = i64::MAX;
        let mut max_end = i64::MIN;
        for r in records {
            traces.insert(r.trace_id);
            min_start = min_start.min(r.start_ts_ns);
            max_end = max_end.max(r.end_ts_ns);
        }
        let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Spans,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: traces.len() as u64,
            min_event_ts_ns: min_start,
            max_event_ts_ns: max_end,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: OUTPUT_FORMAT_VERSION,
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

    /// #981: a LOGS input whose footer over-declares its `record_count` (a
    /// stand-in for a decode that silently loses an input record) must abort
    /// the rewrite before any publish. The output-side conservation gate
    /// cannot catch this: it checks survivors + drops against the same
    /// deflated scan tally, so a lossy decode balances against itself. Because
    /// the rewrite supersedes the originals, an unguarded loss would be
    /// permanent and invisible.
    ///
    /// Flip-line proof: deleting the `input_footer_cross_check(bucket,
    /// input_sample_count, footer_record_count)?` call in `build_rewrite_logs`
    /// (or flipping `scanned_record_count != footer_record_count` to `==` in
    /// `input_footer_cross_check`) lets this reach `Ok(Rewritten { .. })`,
    /// failing the `expect_err`.
    #[tokio::test]
    async fn logs_input_footer_overcount_aborts_rewrite_publish() {
        let store = MemoryStore::new();
        seed_logs_footer_overcount(
            &store,
            1,
            &[
                log_record(
                    1,
                    10,
                    "checkout failed",
                    vec![(
                        "service".to_string(),
                        LogAttrValue::Str("checkout".to_string()),
                    )],
                ),
                log_record(
                    2,
                    20,
                    "shipping ok",
                    vec![(
                        "service".to_string(),
                        LogAttrValue::Str("shipping".to_string()),
                    )],
                ),
            ],
        )
        .await;

        let request = logs_erasure_request(1, "service", "checkout");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let before = list_all(&store, "").await.expect("list before");

        let clock = FixedClock::new(sealed_now_ns());
        let mut memo = MaintainMemo::with_default_interval();
        let err = erasure_rewrite_bucket(
            &store,
            &clock,
            &CompactorConfig::default(),
            &NoLeases,
            &logs_bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect_err("footer/scan record-count mismatch must abort the rewrite");

        assert!(
            matches!(err, MaintainError::ErasureInputConservationViolation { .. }),
            "expected ErasureInputConservationViolation, got {err:?}"
        );

        let after = list_all(&store, "").await.expect("list after");
        assert_eq!(
            before.len(),
            after.len(),
            "an aborted rewrite must write nothing new: originals stay, no part, no record"
        );
        let listing = crate::read::list_bucket(&store, &logs_bucket())
            .await
            .expect("list bucket");
        assert!(
            listing.rewrite_record_keys.is_empty(),
            "no rewrite record may be published on an input-side conservation abort"
        );
        assert_eq!(
            listing.commit_keys.len(),
            1,
            "the original L0 commit (and its data object) must be preserved"
        );
    }

    /// The SPANS sibling of [`logs_input_footer_overcount_aborts_rewrite_publish`].
    /// Flip-line proof: the same, over the `input_footer_cross_check(...)?` call
    /// in `build_rewrite_spans`.
    #[tokio::test]
    async fn spans_input_footer_overcount_aborts_rewrite_publish() {
        let store = MemoryStore::new();
        seed_spans_footer_overcount(
            &store,
            1,
            &[
                span_record(
                    1,
                    1,
                    10,
                    15,
                    vec![("service".to_string(), "checkout".to_string())],
                ),
                span_record(
                    2,
                    2,
                    20,
                    25,
                    vec![("service".to_string(), "shipping".to_string())],
                ),
            ],
        )
        .await;

        let request = spans_erasure_request(1, "service", "checkout");
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];

        let before = list_all(&store, "").await.expect("list before");

        let clock = FixedClock::new(sealed_now_ns());
        let mut memo = MaintainMemo::with_default_interval();
        let err = erasure_rewrite_bucket(
            &store,
            &clock,
            &CompactorConfig::default(),
            &NoLeases,
            &spans_bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect_err("footer/scan record-count mismatch must abort the rewrite");

        assert!(
            matches!(err, MaintainError::ErasureInputConservationViolation { .. }),
            "expected ErasureInputConservationViolation, got {err:?}"
        );

        let after = list_all(&store, "").await.expect("list after");
        assert_eq!(
            before.len(),
            after.len(),
            "an aborted rewrite must write nothing new: originals stay, no part, no record"
        );
        let listing = crate::read::list_bucket(&store, &spans_bucket())
            .await
            .expect("list bucket");
        assert!(
            listing.rewrite_record_keys.is_empty(),
            "no rewrite record may be published on an input-side conservation abort"
        );
        assert_eq!(
            listing.commit_keys.len(),
            1,
            "the original L0 commit (and its data object) must be preserved"
        );
    }

    // -----------------------------------------------------------------
    // `.done` completion soundness regressions
    // -----------------------------------------------------------------

    /// Event-time base for the fidelity fixture below: the start of the
    /// fixture bucket's own ingest hour, so every seeded sample's event time
    /// sits inside the hour its bucket is keyed by (the ordinary,
    /// non-backfilled shape) and the catalog's listing window covers it.
    const FIDELITY_BASE_NS: i64 = HOUR as i64 * NS_PER_HOUR;
    /// Width of one fidelity probe slot. Every object in the fixture carries
    /// its samples inside exactly one slot, and no two live objects share a
    /// slot, so "does this bucket serve anything in slot k" identifies one
    /// object rather than a set.
    const FIDELITY_SLOT_NS: i64 = 1_000_000_000;

    /// The half-open event-time window of probe slot `k`.
    fn fidelity_slot(k: i64) -> (i64, i64) {
        let start = FIDELITY_BASE_NS + k * FIDELITY_SLOT_NS;
        (start, start + FIDELITY_SLOT_NS)
    }

    /// A sample timestamp in the middle of slot `k` -- never on a boundary, so
    /// the catalog's inclusive-end [`TimeRange::overlaps`] and the erasure
    /// window's half-open [`bucket_may_overlap`] select the same objects.
    fn fidelity_slot_mid(k: i64) -> i64 {
        let (start, _) = fidelity_slot(k);
        start + FIDELITY_SLOT_NS / 2
    }

    /// A windowed erasure request used purely as a probe: its window is one
    /// fidelity slot, and its `request_id` is deliberately one no rewrite
    /// record in the fixture names in its `drops`. That second property is
    /// what makes [`bucket_erasure_completion`]'s answer a pure "does the
    /// catalog-resolved live view still serve an object overlapping this
    /// window" question -- the sibling-rewrite exemption
    /// (`!names_request && ...`) can never fire for an unknown request id.
    fn fidelity_probe_request(id_seed: u128, window: (i64, i64)) -> ErasureRequest {
        ErasureRequest {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            request_id: Uuid::from_u128(id_seed).to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: METRIC_NAME_LABEL.to_string(),
                value: "no-such-metric".to_string(),
            }],
            window_start_ns: window.0,
            window_end_ns: window.1,
            reason: String::new(),
        }
    }

    /// The data object keys the QUERY path serves for `range`, resolved through
    /// the real [`ravel_catalog::Catalog`] -- `Catalog::resolve` funnels every
    /// bucket through `Catalog::process_bucket`, the served-set logic
    /// [`bucket_erasure_completion`] reconstructs.
    async fn query_served_keys(
        store: &Arc<MemoryStore>,
        range: TimeRange,
    ) -> std::collections::BTreeSet<String> {
        let catalog = Catalog::new(
            Arc::clone(store) as Arc<dyn ObjectStoreBackend>,
            CatalogConfig {
                // The fixture bucket lives on shard SHARD; the resolve must
                // list at least that many shards to see it at all.
                shard_count: SHARD + 1,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog");
        catalog
            .resolve(&tenant_hash(), Signal::Metrics, range, &[], sealed_now_ns())
            .await
            .expect("resolve")
            .segments
            .into_iter()
            .map(|s| s.data_object_key)
            .collect()
    }

    /// Build the shared fidelity fixture in one bucket, through the real
    /// production passes only:
    ///
    /// - four L0 inputs, each carrying one series in its own event-time slot;
    /// - a real [`crate::compact::compact_bucket`] over the first two, so the
    ///   bucket holds a genuine `CompactionRecord` naming them as inputs;
    /// - two more L0 inputs seeded AFTER that compaction (the interlock shape
    ///   the catalog resolves by including them: they are named by no record's
    ///   input list, so a snapshot still serves them);
    /// - a real [`erasure_rewrite_bucket`] pass, which resolves its own inputs
    ///   ONE-HOP, sees only the compaction record, and publishes a
    ///   `RewriteRecord` superseding it -- blind to the two later L0s.
    ///
    /// The result exercises all three exclusion mechanisms at once:
    /// compaction-input exclusion, whole-record supersession through
    /// `resolve_rewrite_supersession`, and live raw-L0 inclusion.
    ///
    /// Returns the erased request's id and the rewrite record's part key.
    async fn build_fidelity_fixture(store: &Arc<MemoryStore>) -> (String, String) {
        // Slot 0: the erasure subject. Slot 1: an unrelated series. Both are
        // compacted, then rewritten, so the rewrite output must cover slot 1
        // only.
        seed(
            store.as_ref(),
            1,
            vec![series("alpha", &[(fidelity_slot_mid(0), 1.0)])],
        )
        .await;
        seed(
            store.as_ref(),
            2,
            vec![series("beta", &[(fidelity_slot_mid(1), 2.0)])],
        )
        .await;

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let outcome = crate::compact::compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compact");
        assert!(
            matches!(outcome, crate::compact::CompactionOutcome::Compacted { .. }),
            "fixture needs a real compaction record, got {outcome:?}"
        );

        // Slots 2 and 3: raw L0s no record names. The one-hop resolver the
        // rewrite pass uses never sees these; the catalog resolver serves them.
        seed(
            store.as_ref(),
            3,
            vec![series("gamma", &[(fidelity_slot_mid(2), 3.0)])],
        )
        .await;
        seed(
            store.as_ref(),
            4,
            vec![series("delta", &[(fidelity_slot_mid(3), 4.0)])],
        )
        .await;

        let request = erasure_request(0xA11, "alpha");
        let request_id = request.request_id.clone();
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            store.as_ref(),
            &clock,
            &config,
            &NoLeases,
            &bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect("rewrite");
        assert!(
            matches!(
                outcome,
                ErasureRewriteOutcome::Rewritten {
                    publish: PublishOutcome::Published,
                    ..
                }
            ),
            "fixture needs a published rewrite, got {outcome:?}"
        );

        let record = read_rewrite_record(store.as_ref()).await;
        assert!(
            !record.superseded_record_key.is_empty(),
            "the rewrite must supersede the compaction record as a whole, which is what makes \
             the supersession chase (not a one-hop lookup) load-bearing here"
        );
        assert_eq!(record.parts.len(), 1);
        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");
        (request_id, part_key)
    }

    /// ADR-0064 §4 (2026-08-08 F1 correction, #1000): `bucket_erasure_completion`
    /// reconstructs `Catalog::process_bucket`'s served set -- compaction-input
    /// exclusion, `resolve_rewrite_supersession`, and the live
    /// L0/compaction-part/rewrite-part filtering -- on a fresh per-bucket
    /// listing. That reconstruction is a SECOND COPY of the query path's logic,
    /// and `.done` soundness rests entirely on the two copies agreeing: a
    /// refactor of `process_bucket` that widens what a query serves, without a
    /// matching change here, makes `.done` over-complete and permanently
    /// resurrects an erased subject (§4's "writing `.done` from a fresh-LIST
    /// check alone is not acceptable" failure, one level up).
    ///
    /// One shared fixture bucket (see [`build_fidelity_fixture`]) is run
    /// through BOTH paths and they must agree on exactly which objects the
    /// bucket serves:
    ///
    /// - the full-range query served set is pinned object-key by object-key
    ///   (the rewrite output plus the two live raw L0s -- never the compacted
    ///   L0s, never the superseded compaction part);
    /// - per event-time slot, "the query serves something here" must equal
    ///   "the reconstruction still considers the subject servable here", with
    ///   the expected served/not-served pattern pinned so agreement-on-empty
    ///   cannot pass the test.
    ///
    /// Flip-line proof (each flipped alone in `bucket_erasure_completion`,
    /// re-run, assert fires):
    ///
    /// - the live-L0 filter `!excluded.contains(&(...))` with its `!` removed:
    ///   the compacted-away L0 in slot 0 becomes live, the reconstruction
    ///   blocks slot 0, the query serves nothing there, and the slot-0
    ///   `assert_eq!` fails.
    /// - the live-compaction filter `!superseded_records.contains(key)` with
    ///   its `!` removed: the rewrite-superseded compaction part (slots 0-1)
    ///   comes back, and slot 0 fails the same way.
    #[tokio::test]
    async fn maintain_reconstruction_agrees_with_the_query_served_set() {
        let store = Arc::new(MemoryStore::new());
        let (_request_id, rewrite_part_key) = build_fidelity_fixture(&store).await;

        // Every object ever written to the bucket, so the served set can be
        // asserted by identity rather than by count.
        let listing = crate::read::list_bucket(store.as_ref(), &bucket())
            .await
            .expect("list bucket");
        let inputs = crate::read::load_inputs(store.as_ref(), &bucket(), &listing.commit_keys, 1)
            .await
            .expect("load inputs");
        let l0_key_by_seq: HashMap<u64, String> = inputs
            .iter()
            .map(|i| {
                (
                    i.record.writer_seq,
                    keys::reconstruct_data_key(&i.record).expect("data key"),
                )
            })
            .collect();

        // 1. The query path's served set over the whole fixture range, by key.
        let (full_start, _) = fidelity_slot(0);
        let (_, full_end) = fidelity_slot(5);
        let served = query_served_keys(
            &store,
            TimeRange {
                start_ns: full_start,
                end_ns: full_end,
            },
        )
        .await;
        let expected: std::collections::BTreeSet<String> = [
            rewrite_part_key.clone(),
            l0_key_by_seq[&3].clone(),
            l0_key_by_seq[&4].clone(),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            served, expected,
            "the query serves exactly the rewrite output and the two raw L0s no record names; \
             the compacted L0s are excluded and the compaction part is superseded"
        );

        // 2. Slot by slot, the two paths must agree. The expected pattern is
        // pinned so a fixture that degenerated to "nothing anywhere" (which
        // both paths would agree on) cannot pass.
        let expected_served = [false, true, true, true, false];
        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        for (k, expect) in expected_served.iter().enumerate() {
            let k = k as i64;
            let (start, end) = fidelity_slot(k);
            let query_serves = !query_served_keys(
                &store,
                TimeRange {
                    start_ns: start,
                    // Inclusive end (`TimeRange::overlaps`), so stop one ns
                    // short of the next slot's first ns.
                    end_ns: end - 1,
                },
            )
            .await
            .is_empty();
            assert_eq!(
                query_serves, *expect,
                "fixture shape changed: slot {k} query-served expectation"
            );

            let probe = PendingErasureRequest {
                request_key: "unused-in-memory-only".to_string(),
                request: fidelity_probe_request(0x9000 + k as u128, (start, end)),
            };
            let completion = bucket_erasure_completion(
                store.as_ref(),
                &clock,
                &config,
                &NoLeases,
                &bucket(),
                std::slice::from_ref(&probe),
            )
            .await
            .expect("completion");
            let reconstruction_serves = completion.blocked.contains(&probe.request.request_id);

            assert_eq!(
                query_serves, reconstruction_serves,
                "slot {k}: the maintain-side reconstruction and Catalog::process_bucket disagree \
                 about what this bucket serves. A `.done` written on a reconstruction that serves \
                 LESS than the query path resurrects the erased subject permanently (ADR-0064 §4)."
            );
        }
    }

    /// The subject value carried by the log records the erasure request names.
    const ERASED_SUBJECT: &str = "u123";
    /// A second subject value in the same indexed field that must survive.
    const SURVIVING_SUBJECT: &str = "u999";
    /// Column ids probed when asking whether an object's POSTINGS/BLOOM still
    /// resolve a value. Both sections are keyed by column id, and a rewritten
    /// object may assign different ids than its input did (the surviving
    /// records' distinct column set differs), so every plausible id is probed
    /// rather than the one the input happened to use: "no column resolves the
    /// subject" is the claim, not "one particular column does not".
    const PROBE_COLUMN_IDS: std::ops::Range<u32> = 0..48;

    /// Whether an RLOG object's POSTINGS (footer kind 6) and BLOOM (footer
    /// kind 5) sections still resolve `value`, as `(postings_hit, bloom_hit)`.
    ///
    /// A missing POSTINGS section resolves nothing (ADR-0049 decision 5:
    /// absence is always legal, and it is exactly a "this object cannot prove
    /// the term present" answer). A missing BLOOM section is not tolerated:
    /// every RLOG object carries one, so its absence would silently turn the
    /// bloom half of this check into a vacuous pass.
    fn rlog_index_resolves(bytes: &[u8], value: &str) -> (bool, bool) {
        use ravel_logseg::bloom_section::BloomSection;
        use ravel_logseg::footer::kind;
        use ravel_logseg::postings::PostingsSection;

        let cfg = RlogConfig::default();
        let footer = ravel_logseg::footer::open(bytes).expect("open RLOG footer");

        let postings_hit = match footer.section(kind::POSTINGS) {
            None => false,
            Some(desc) => {
                let raw =
                    ravel_logseg::read_section(bytes, desc, &cfg).expect("read POSTINGS section");
                let section = PostingsSection::parse(&raw).expect("parse POSTINGS section");
                PROBE_COLUMN_IDS.into_iter().any(|cid| {
                    matches!(
                        section.probe(cid, value.as_bytes()),
                        Ok(Some(blocks)) if !blocks.is_empty()
                    )
                })
            }
        };

        let bloom_desc = footer
            .section(kind::BLOOM)
            .expect("every RLOG object carries a BLOOM section");
        let raw = ravel_logseg::read_section(bytes, bloom_desc, &cfg).expect("read BLOOM section");
        let section = BloomSection::parse(&raw).expect("parse BLOOM section");
        let bloom_hit = (0..section.len()).any(|i| {
            let view = section.entry(i).expect("bloom entry");
            PROBE_COLUMN_IDS
                .into_iter()
                .any(|cid| view.may_contain(cid, value.as_bytes()))
        });

        (postings_hit, bloom_hit)
    }

    /// ADR-0064 §4 as narrowed by the 2026-08-13 amendment (#1000 finding F3):
    /// the `.done` record asserts every live segment and index entry is free of
    /// the erased subject, while the pass itself only walks commit records. For
    /// a rewritten LOG segment that claim rests on the rewrite REGENERATING the
    /// segment's own index sections from the surviving records -- POSTINGS
    /// (footer kind 6) and BLOOM (kind 5) are inside the data object, so they
    /// are not "index objects that cannot hold a subject value"; a rewrite that
    /// carried its inputs' index sections through would leave the erased
    /// subject's field-term values resolvable inside a live object after
    /// `.done`.
    ///
    /// Drive the real `erasure_rewrite_bucket` over an L0 whose POSTINGS and
    /// BLOOM demonstrably do resolve the subject, and assert the rewritten
    /// part's do not, while still resolving the surviving subject.
    ///
    /// Non-vacuity is built in two ways. The same probe run against the
    /// pre-rewrite INPUT object -- which is exactly "an object whose postings
    /// and bloom carry the erased terms unfiltered" -- must return `true` on
    /// both channels; and the surviving value must still be resolvable in the
    /// OUTPUT, so a bloom that is merely empty or unparseable cannot pass.
    ///
    /// Flip-line proof, both verified:
    ///
    /// - make `first_dropping_log_request`'s body `None` unconditionally
    ///   (equivalently, flip `LogErasureMatcher::drops_record` to `false`).
    ///   The erased record is re-encoded into the output, its `user_id` value
    ///   goes into the output block's bloom, and `!out_bloom` fires.
    /// - additionally build `build_rewrite_logs`'s writer as
    ///   `RlogWriter::new(read_cfg, identity).with_indexed_fields(vec!["user_id"
    ///   .to_string()])`, and `!out_postings` fires first: the output then also
    ///   carries a POSTINGS section naming the erased term.
    ///
    /// The index assertions deliberately precede the record-level check below,
    /// so an unsound rewrite fails on the index claim this test exists for.
    #[tokio::test]
    async fn rewritten_log_segment_index_no_longer_resolves_the_erased_subject() {
        let store = MemoryStore::new();
        let erased = log_record(
            1,
            10,
            "checkout failed",
            vec![(
                "user_id".to_string(),
                LogAttrValue::Str(ERASED_SUBJECT.to_string()),
            )],
        );
        let kept = log_record(
            2,
            20,
            "shipping ok",
            vec![(
                "user_id".to_string(),
                LogAttrValue::Str(SURVIVING_SUBJECT.to_string()),
            )],
        );
        // `user_id` indexed, so the input carries a real POSTINGS section
        // holding the erased subject's term (ADR-0049 decision 3: opt-in).
        seed_logs_indexed(&store, 1, &[erased, kept.clone()], &["user_id"]).await;

        let listing = crate::read::list_bucket(&store, &logs_bucket())
            .await
            .expect("list bucket");
        let inputs = crate::read::load_inputs(&store, &logs_bucket(), &listing.commit_keys, 1)
            .await
            .expect("load inputs");
        assert_eq!(inputs.len(), 1);
        let input_key = keys::reconstruct_data_key(&inputs[0].record).expect("input data key");
        let input_bytes = store
            .get(&input_key, GetRange::Full)
            .await
            .expect("get input")
            .data;

        let (in_postings, in_bloom) = rlog_index_resolves(input_bytes.as_ref(), ERASED_SUBJECT);
        assert!(
            in_postings,
            "fixture is meaningless unless the INPUT's POSTINGS resolves the subject: this is the \
             unfiltered-index state the rewrite has to eliminate"
        );
        assert!(
            in_bloom,
            "fixture is meaningless unless the INPUT's BLOOM resolves the subject"
        );

        let request = logs_erasure_request(1, "user_id", ERASED_SUBJECT);
        let pending = vec![PendingErasureRequest {
            request_key: "unused-in-memory-only".to_string(),
            request,
        }];
        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            &store,
            &clock,
            &config,
            &NoLeases,
            &logs_bucket(),
            &pending,
            &mut memo,
        )
        .await
        .expect("rewrite");
        assert!(
            matches!(
                outcome,
                ErasureRewriteOutcome::Rewritten {
                    publish: PublishOutcome::Published,
                    ..
                }
            ),
            "expected a published rewrite, got {outcome:?}"
        );

        let record = read_rewrite_record_for(&store, &logs_bucket()).await;
        assert_eq!(record.parts.len(), 1);
        let part_key =
            keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");
        let part_bytes = store
            .get(&part_key, GetRange::Full)
            .await
            .expect("get part")
            .data;

        // The subject's terms are gone from both index sections of the
        // rewritten object. Asserted before the record-level check below so
        // that an unsound rewrite fails on the claim this test exists for.
        let (out_postings, out_bloom) = rlog_index_resolves(part_bytes.as_ref(), ERASED_SUBJECT);
        assert!(
            !out_postings,
            "the rewritten part's POSTINGS still resolves the erased subject's term: `.done` \
             would claim an object free of the subject while a live index inside it names it"
        );
        assert!(
            !out_bloom,
            "the rewritten part's BLOOM still resolves the erased subject's value: the rewrite \
             carried its input's index through instead of regenerating it from survivors"
        );

        // The negative above is only meaningful if the rewritten object's index
        // is populated at all.
        let (_, survivor_bloom) = rlog_index_resolves(part_bytes.as_ref(), SURVIVING_SUBJECT);
        assert!(
            survivor_bloom,
            "the rewritten BLOOM must still resolve the SURVIVING subject: an empty or \
             unpopulated bloom would make the erased-subject assertion above vacuous"
        );

        // The record itself is gone too.
        assert_eq!(
            decode_logs_part(&store, &part_key).await,
            vec![kept],
            "only the surviving record may remain in the rewritten part"
        );
    }
}
