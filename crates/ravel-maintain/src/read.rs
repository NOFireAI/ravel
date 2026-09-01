//! Input discovery and decode. Lists a sealed bucket, partitions its objects by key shape, decodes
//! every L0 commit record, sorts the inputs canonically, computes the
//! `input_set_hash`, and decodes each input segment's catalog down to
//! per-run absolute page ranges.
//!
//! Only catalog metadata is retained here: footers, the LABEL_DICT /
//! SERIES_IDS / SERIES_META sections, and (when present) the EXEMPLARS
//! section, whose records the merge copies verbatim into the output with only
//! their `series_index` remapped (ADR-0047 decision 3). Exemplar records are
//! small and bounded by the ingest admission cap, so they stay inside this
//! metadata bound. The verbatim TS/VAL/HIST page bytes are
//! fetched lazily during the merge ([`crate::build`]) with ranged GETs, so
//! peak memory is catalog metadata plus one fetch window, plus the series being
//! materialized, plus the parts held until publish -- not the whole bucket's
//! page data ([`crate::build`]'s header comment splits those terms and says
//! which of them a config knob sizes).

use std::future::Future;
use std::pin::Pin;

use futures::stream::{StreamExt, TryStreamExt, iter as stream_iter};
use ravel_commit::erasure::compute_compaction_input_set_hash;
use ravel_commit::keys::{self, BucketEntry};
use ravel_commit::record;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_proto::commit::v1::{CommitRecord, CompactionInputIdentity};
use ravel_segment::{
    ExemplarInput, FooterLocation, FooterOutcome, ReaderLimits, ValueKind, decode_catalog_v4,
    decode_catalog_v5, decode_exemplars_section, open_from_suffix, plan_ranges_v4,
};
use ravel_types::{LabelSet, SeriesId, Signal};

use crate::bucket::Bucket;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};

/// Persistent section-kind numbers (docs/segment-format.md); not re-exported
/// by `ravel_segment`, named here as the format contract, same as ravel-bench.
const LABEL_DICT: u32 = 1;
const SERIES_IDS: u32 = 5;
const SERIES_META: u32 = 6;
const SERIES_IDX: u32 = 8;
const SERIES_META_CHUNKS: u32 = 9;
const EXEMPLARS: u32 = 10;

/// The partitioned result of listing a bucket's `c/<shard>/<hour>/` prefix
/// (partitions listed keys by shape). Unknown shapes are a hard
/// error surfaced by [`list_bucket`], never silently dropped.
#[derive(Debug, Default)]
pub struct BucketListing {
    /// L0 commit record keys (the compaction inputs).
    pub commit_keys: Vec<String>,
    /// Compaction record keys already present in the bucket.
    pub compaction_record_keys: Vec<String>,
    /// Selective-erasure rewrite record keys (`rw.<hash16>.cmt`, ADR-0064
    /// decision 3) present in the bucket. Recognized and surfaced here rather
    /// than treated as layout drift; the rewrite pass is what acts on
    /// them. Classifying them keeps `list_bucket` from hard-erroring on a
    /// bucket that has already been erased once.
    pub rewrite_record_keys: Vec<String>,
    /// The retention tombstone key, if the bucket is tombstoned.
    pub tombstone_key: Option<String>,
}

/// List one bucket and classify every object by key shape.
/// A key matching no known shape is [`MaintainError::UnknownBucketEntry`]
/// (fail loud on layout drift), never skipped.
pub async fn list_bucket(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> Result<BucketListing> {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )?;
    let metas = list_all(store, &prefix).await?;
    let mut listing = BucketListing::default();
    for meta in metas {
        match keys::partition_bucket_entry(&meta.key) {
            Ok(BucketEntry::CommitRecord(_)) => listing.commit_keys.push(meta.key),
            Ok(BucketEntry::CompactionRecord(_)) => listing.compaction_record_keys.push(meta.key),
            Ok(BucketEntry::RewriteRecord(_)) => listing.rewrite_record_keys.push(meta.key),
            Ok(BucketEntry::Tombstone(_)) => listing.tombstone_key = Some(meta.key),
            Err(keys::KeyError::UnknownBucketEntryShape(k)) => {
                return Err(MaintainError::UnknownBucketEntry(k));
            }
            Err(e) => return Err(MaintainError::Key(e)),
        }
    }
    // Deterministic order regardless of how the store paginated the listing.
    listing.commit_keys.sort();
    listing.compaction_record_keys.sort();
    listing.rewrite_record_keys.sort();
    Ok(listing)
}

/// One decoded L0 input: its commit record plus the key it was listed at.
#[derive(Debug, Clone)]
pub struct InputRecord {
    pub commit_key: String,
    pub record: CommitRecord,
}

/// GET and decode every L0 commit record, verifying each record's key against
/// its own identity fields (ADR-0010 §7 discipline) and its signal against
/// the bucket, then sort the inputs canonically by
/// `(writer_id, writer_epoch, writer_seq)`.
///
/// The GETs run `concurrency` at a time (`CompactorConfig::input_read_concurrency`).
/// A bucket with hundreds of L0 inputs is otherwise one full store round trip
/// per input in sequence. Completion order does not reach the caller: the
/// canonical sort below re-establishes the one ordering the merge's tie-break
/// depends on, and `(writer_id, writer_epoch, writer_seq)` is unique per input
/// (it is what the commit key is built from), so the sort is total and the
/// result is identical at any concurrency.
pub async fn load_inputs(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    commit_keys: &[String],
    concurrency: usize,
) -> Result<Vec<InputRecord>> {
    // Box each GET future with an explicit `+ Send` bound before handing it to
    // `buffer_unordered`, the same workaround `crate::build::fetch_batch_pages`
    // documents: a bare `async` block borrowing the `&dyn ObjectStoreBackend`
    // makes rustc infer a late-bound lifetime whose `Send` it cannot prove is
    // general enough where the maintain loop is `tokio::spawn`ed.
    type InputFuture<'f> = Pin<Box<dyn Future<Output = Result<InputRecord>> + Send + 'f>>;
    let futures: Vec<InputFuture<'_>> = commit_keys
        .iter()
        .map(|key| Box::pin(load_one_input(store, bucket, key)) as InputFuture<'_>)
        .collect();
    let mut inputs: Vec<InputRecord> = stream_iter(futures)
        .buffer_unordered(concurrency.max(1))
        .try_collect()
        .await?;
    inputs.sort_by(|a, b| {
        (
            a.record.writer_id.as_str(),
            a.record.writer_epoch,
            a.record.writer_seq,
        )
            .cmp(&(
                b.record.writer_id.as_str(),
                b.record.writer_epoch,
                b.record.writer_seq,
            ))
    });
    Ok(inputs)
}

/// GET, decode and verify one L0 commit record. Named (not an inline `async`
/// block) so its future is `Send`-general over the borrowed store; see the
/// call site in [`load_inputs`].
async fn load_one_input(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    key: &str,
) -> Result<InputRecord> {
    let got = store.get(key, GetRange::Full).await?;
    let record = record::decode(&got.data)?;
    // The record's key must reconstruct to the key we listed it at.
    verify_commit_key(&record, key)?;
    verify_input_matches_bucket(bucket, &record)?;
    Ok(InputRecord {
        commit_key: key.to_string(),
        record,
    })
}

/// Verify a decoded commit record reconstructs to the key it was fetched at
/// (ADR-0010 §7 discipline). A corrupted-but-still-decodable record's own
/// identity fields, which `keys::reconstruct_data_key` later trusts, must not
/// name an object outside the tenant/shard/signal/hour the key it was stored
/// under implies. Shared by [`load_inputs`], the retention sweep, and the
/// superseded-input sweep.
pub(crate) fn verify_commit_key(record: &CommitRecord, key: &str) -> Result<()> {
    let expected = keys::commit_key_for_record(record)?;
    if expected == key {
        Ok(())
    } else {
        Err(MaintainError::Key(keys::KeyError::Malformed {
            key: key.to_string(),
            reason: format!("commit key does not match record identity (expected {expected:?})"),
        }))
    }
}

fn verify_input_matches_bucket(bucket: &Bucket, record: &CommitRecord) -> Result<()> {
    if record.tenant_hash.as_slice() != bucket.tenant_hash.0.as_slice() {
        return Err(MaintainError::Invariant(
            "input commit record tenant_hash does not match bucket".to_string(),
        ));
    }
    if record.shard != bucket.shard {
        return Err(MaintainError::Invariant(format!(
            "input commit record shard {} does not match bucket shard {}",
            record.shard, bucket.shard
        )));
    }
    if record.ingest_hour_bucket != bucket.ingest_hour_bucket {
        return Err(MaintainError::Invariant(format!(
            "input commit record hour {} does not match bucket hour {}",
            record.ingest_hour_bucket, bucket.ingest_hour_bucket
        )));
    }
    let signal = ravel_commit::signal::from_proto(record.signal)
        .map_err(|_| MaintainError::Invariant(format!("unknown signal {}", record.signal)))?;
    if signal != bucket.signal {
        return Err(MaintainError::SignalMismatch {
            expected: signal_name(bucket.signal),
            actual: signal_name(signal),
        });
    }
    Ok(())
}

fn signal_name(s: Signal) -> String {
    s.key_prefix().to_string()
}

/// Canonical `input_set_hash` for `inputs`. Inputs MUST already be in
/// canonical order (as [`load_inputs`] leaves them).
///
/// A thin wrapper over
/// [`ravel_commit::erasure::compute_compaction_input_set_hash`], the single
/// source of truth for this hash (issue #830): builds the identity list from
/// the decoded `InputRecord`s and delegates, so this crate carries no
/// hash-preimage logic of its own to drift from `ravel-catalog`'s
/// `seal_divergence`, which recomputes the same hash from a stored record's
/// declared `inputs`.
pub fn input_set_hash(inputs: &[InputRecord]) -> [u8; 32] {
    let ids: Vec<CompactionInputIdentity> = inputs
        .iter()
        .map(|input| CompactionInputIdentity {
            writer_id: input.record.writer_id.clone(),
            writer_epoch: input.record.writer_epoch,
            writer_seq: input.record.writer_seq,
        })
        .collect();
    compute_compaction_input_set_hash(&ids)
}

/// One run's copy plan: dedup-priority provenance (from the input's COMMIT
/// RECORD), event-time bounds, sample count, and the
/// absolute byte ranges of its verbatim TS and VAL-or-HIST pages within the
/// input object. `page_abs` is the VAL page for a scalar series and the HIST
/// page for a histogram series (`kind` says which).
#[derive(Debug, Clone)]
pub struct RunPlan {
    pub created_unix_ns: i64,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    pub sample_count: u32,
    pub ts_abs: (u64, u64),
    pub page_abs: (u64, u64),
    pub kind: ValueKind,
}

/// One series' merge input from one object: identity, labels, value kind, and
/// its ordered runs.
#[derive(Debug, Clone)]
pub struct SeriesPlan {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub kind: ValueKind,
    pub runs: Vec<RunPlan>,
}

/// One decoded input object ready for the merge: the data-object key to fetch
/// pages from, and its per-series catalog in series-id order.
#[derive(Debug, Clone)]
pub struct InputCatalog {
    pub object_key: String,
    pub series: Vec<SeriesPlan>,
    /// Every exemplar this input carries (ADR-0047 decision 3), in the
    /// object's own stored order, with `series_index` already resolved to the
    /// series id it named. The output writer resolves the id back into an
    /// index in the *output's* SERIES_IDS ordering, which is the whole remap:
    /// the record's other fields are copied verbatim and never merged,
    /// deduplicated, re-capped, or re-sampled.
    ///
    /// Empty when the input has no EXEMPLARS section, which is the common case
    /// and always legal. Unlike page bytes, these are retained (not fetched
    /// lazily during the merge): a record is ~40 bytes plus attributes and the
    /// admission cap bounds how many an object can hold, so this stays inside
    /// the "catalog metadata" memory bound rather than scaling with the
    /// bucket's data.
    pub exemplars: Vec<ExemplarInput>,
}

/// Decode one input's catalog into per-run absolute page ranges, stamping
/// every run's provenance from the input's commit record.
///
/// Footer is located by a suffix probe (one GET, growing to a second GET only
/// if the probe missed the footer). The catalog sections are then read: an
/// object carrying the sparse catalog pair needs the whole object for the
/// sparse decode, but an object carrying the whole-section SERIES_META needs
/// only its three catalog sections, fetched by range. Either shape occurs at
/// L0: a busy shard flushes 4096+ series in one object, so the sparse branch
/// is a routine L0 input, not just a compacted output.
pub async fn load_input_catalog(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    input: &InputRecord,
) -> Result<InputCatalog> {
    let object_key = keys::reconstruct_data_key(&input.record)?;
    load_catalog_from_object(
        store,
        config,
        object_key,
        input.record.created_unix_ns,
        input.record.writer_epoch,
        input.record.writer_seq,
    )
    .await
}

/// The object-key-parametrized core of [`load_input_catalog`], generalized so
/// a caller with an object key from something other than an L0
/// [`InputRecord`] (an L1 compaction or rewrite part, which carries no
/// writer identity of its own) can still decode a catalog. `created_unix_ns`,
/// `writer_epoch`, and `writer_seq` stamp every [`RunPlan`]'s provenance
/// fields exactly as [`load_input_catalog`] does from its `InputRecord`; an
/// L1-part caller with no meaningful per-run writer identity passes the
/// record's own `created_unix_ns` and zeros for epoch/seq, the same
/// nil-writer-identity convention `ravel-catalog`'s `build_l1_segment_ref`
/// uses for L1-level refs.
pub async fn load_catalog_from_object(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    object_key: String,
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
) -> Result<InputCatalog> {
    let limits = ReaderLimits::default();

    // Locate the footer from a suffix probe.
    let probe = store
        .get(&object_key, GetRange::Suffix(config.footer_probe_bytes))
        .await?;
    let total = probe.total_size;
    let loc = match open_from_suffix(&probe.data, total, limits)? {
        FooterOutcome::Ready(loc) => loc,
        FooterOutcome::NeedRange { offset, len } => {
            let tail = store
                .get(&object_key, GetRange::Range(offset, offset + len))
                .await?;
            match open_from_suffix(&tail.data, total, limits)? {
                FooterOutcome::Ready(loc) => loc,
                FooterOutcome::NeedRange { .. } => {
                    return Err(MaintainError::Segment(
                        ravel_segment::SegmentError::Truncated,
                    ));
                }
            }
        }
    };
    let footer = &loc.footer;

    let sparse = catalog_is_sparse(&loc)?;
    let entries = if sparse {
        // Sparse decode needs the whole object. An L0 flush of 4096+ series is
        // ordinary (a busy shard reaches it in one flush), so this branch is a
        // routine input shape, not a rare one; the whole-object GET is the
        // stated cost of the sparse catalog decode, not evidence of a corner
        // case.
        let whole = store.get(&object_key, GetRange::Full).await?;
        decode_catalog_v5(footer, &whole.data, limits)?
    } else {
        let dict = get_section(store, &object_key, footer, LABEL_DICT).await?;
        let ids = get_section(store, &object_key, footer, SERIES_IDS).await?;
        let meta = get_section(store, &object_key, footer, SERIES_META).await?;
        decode_catalog_v4(footer, &dict, &ids, &meta, limits)?
    };

    // Absolute page ranges for every (series, run), in the same nested order
    // decode_catalog_v4/v5 produced the entries.
    let refs: Vec<&ravel_segment::SeriesEntryV4> = entries.iter().collect();
    let planned = plan_ranges_v4(footer, &refs)?;

    let mut series = Vec::with_capacity(entries.len());
    let mut planned_iter = planned.into_iter();
    for entry in &entries {
        let kind = entry.entry.value_kind;
        let mut runs = Vec::with_capacity(entry.runs.len());
        for run in &entry.runs {
            let range = planned_iter.next().ok_or_else(|| {
                MaintainError::Invariant("plan_ranges_v4 produced fewer ranges than runs".into())
            })?;
            let page_abs = match kind {
                ValueKind::Scalar => range.val_range,
                ValueKind::Histogram => range.hist_range,
            };
            runs.push(RunPlan {
                created_unix_ns,
                writer_epoch,
                writer_seq,
                min_ts_ns: run.min_ts_ns,
                max_ts_ns: run.max_ts_ns,
                sample_count: run.sample_count,
                ts_abs: range.ts_range,
                page_abs,
                kind,
            });
        }
        series.push(SeriesPlan {
            series_id: entry.entry.series_id,
            labels: entry.entry.labels.clone(),
            kind,
            runs,
        });
    }
    if planned_iter.next().is_some() {
        return Err(MaintainError::Invariant(
            "plan_ranges_v4 produced more ranges than runs".into(),
        ));
    }

    let exemplars = load_input_exemplars(store, &object_key, footer, limits, &series).await?;

    Ok(InputCatalog {
        object_key,
        series,
        exemplars,
    })
}

/// Whether an object carries the sparse catalog (SERIES_IDX kind 8 +
/// SERIES_META_CHUNKS kind 9) rather than the whole-section SERIES_META
/// (kind 6), decided by which sections the footer lists.
///
/// Presence, not the trailer version and not the series count, is the reader
/// contract: docs/segment-format.md's "Sparse catalog" section states that the
/// sparse-emission threshold is a writer-side constant and that "presence is
/// signalled by the sections themselves". Selecting on the version instead
/// would silently route every object written at some later version to the
/// whole-catalog branch, where it would look for a section the object does not
/// carry (ADR-0092 decision 7). The whole [`FooterLocation`] is taken rather
/// than just its footer so that `version` is visibly in scope and visibly
/// unused: this decision does not depend on it.
///
/// Fails closed on half a pair. The format doc makes "one half of the sparse
/// pair without the other" Corrupted, so it is reported as
/// [`ravel_segment::SegmentError::SparseSectionsIncomplete`], never resolved
/// to one branch or the other. `validate_sections` already rejects that shape
/// at open time; the check is repeated here so this branch never depends on a
/// caller having run it.
fn catalog_is_sparse(loc: &FooterLocation) -> Result<bool> {
    let has = |kind: u32| loc.footer.sections.iter().any(|s| s.kind == kind);
    match (has(SERIES_IDX), has(SERIES_META_CHUNKS)) {
        (true, true) => Ok(true),
        (false, false) => Ok(false),
        _ => Err(MaintainError::Segment(
            ravel_segment::SegmentError::SparseSectionsIncomplete,
        )),
    }
}

/// Decode one input's EXEMPLARS section (kind 10, ADR-0047) and resolve each
/// record's `series_index` to the series id it names, so the merge can hand the
/// records to the output writer and let it re-resolve the index against the
/// output's own SERIES_IDS ordering (docs/segment-format.md "Compaction rule").
///
/// An absent section is legal and yields no exemplars, which is the common
/// case: this costs two extra ranged GETs (the LABEL_DICT the attributes intern
/// into, and the section itself) only for an object that actually carries
/// exemplars.
///
/// `series` MUST be the object's catalog in SERIES_IDS order, which is what
/// `decode_catalog_v4`/`decode_catalog_v5` return: that ordering is what makes
/// `series_index` an index into it. The decoder already rejects an index at or
/// beyond `footer.series_count`, and a catalog whose length disagrees with
/// `series_count` is a corrupt object rather than something to index into
/// hopefully, so it fails loud here.
async fn load_input_exemplars(
    store: &dyn ObjectStoreBackend,
    object_key: &str,
    footer: &ravel_segment::Footer,
    limits: ReaderLimits,
    series: &[SeriesPlan],
) -> Result<Vec<ExemplarInput>> {
    if !footer.sections.iter().any(|s| s.kind == EXEMPLARS) {
        return Ok(Vec::new());
    }
    if series.len() as u64 != footer.series_count {
        return Err(MaintainError::Invariant(format!(
            "input {object_key} decoded {} series but its footer claims {}; \
             exemplar series_index values cannot be resolved",
            series.len(),
            footer.series_count
        )));
    }
    let dict = get_section(store, object_key, footer, LABEL_DICT).await?;
    let section = get_section(store, object_key, footer, EXEMPLARS).await?;
    let records = decode_exemplars_section(footer, &dict, &section, limits)?;

    let mut out = Vec::with_capacity(records.len());
    for r in records {
        let idx = usize::try_from(r.series_index).map_err(|_| {
            MaintainError::Invariant("exemplar series_index overflows usize".into())
        })?;
        let plan = series.get(idx).ok_or_else(|| {
            MaintainError::Invariant(format!(
                "exemplar series_index {idx} is outside input {object_key}'s catalog"
            ))
        })?;
        out.push(ExemplarInput {
            series_id: plan.series_id,
            ts_ns: r.ts_ns,
            value: r.value,
            trace_id: r.trace_id,
            span_id: r.span_id,
            attrs: r.attrs,
        });
    }
    Ok(out)
}

async fn get_section(
    store: &dyn ObjectStoreBackend,
    key: &str,
    footer: &ravel_segment::Footer,
    kind: u32,
) -> Result<bytes::Bytes> {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .ok_or(StoreError::Corrupted(format!(
            "missing section kind {kind}"
        )))?;
    let got = store
        .get(
            key,
            GetRange::Range(section.offset, section.offset + section.len),
        )
        .await?;
    Ok(got.data)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    //! Catalog-form selection (ADR-0092 decision 7): which of the two catalog
    //! bodies an input carries is read off the footer's section list, not off
    //! the trailer version and not off the series count.

    use ravel_segment::{
        IngestBounds, SUPPORTED_VERSIONS, SegmentError, SegmentIdentity, SegmentWriter, SeriesInput,
    };
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};

    use super::*;

    const TENANT: &str = "acme";

    /// A real object above the sparse-emission threshold, written by the
    /// production writer, so its footer carries the SERIES_IDX +
    /// SERIES_META_CHUNKS pair exactly as a busy shard's L0 flush would.
    fn sparse_object() -> Vec<u8> {
        let tenant = TenantId::new(TENANT);
        let n = ravel_segment::V5_SPARSE_THRESHOLD as usize;
        let mut series = Vec::with_capacity(n);
        for i in 0..n {
            let metric = format!("m{i:05}");
            let labels = LabelSet::new(vec![Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: metric.clone(),
            }])
            .expect("valid labels");
            series.push(SeriesInput {
                series_id: SeriesId::compute(&tenant, &metric, &labels).expect("series id"),
                labels,
                samples: vec![Sample {
                    ts_ns: i as i64 + 1,
                    value: i as f64,
                }],
            });
        }
        let identity = SegmentIdentity {
            tenant_hash: tenant.hash().0,
            shard: 3,
            writer_id: uuid::Uuid::from_u128(1).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 1,
            max_ingest_ts_ns: 1,
        };
        SegmentWriter::write(series, identity, bounds)
            .expect("write sparse object")
            .bytes
            .to_vec()
    }

    fn has_section(loc: &FooterLocation, kind: u32) -> bool {
        loc.footer.sections.iter().any(|s| s.kind == kind)
    }

    /// Rewrites the trailer's version field in place and recomputes
    /// `footer_crc32c` over the changed bytes, so the object is well formed at
    /// every other layer and only its version differs.
    fn set_trailer_version(bytes: &mut [u8], loc: &FooterLocation, version: u16) {
        let trailer = loc.trailer_offset as usize;
        let footer_bytes = &bytes[loc.footer_offset as usize..trailer];
        let footer_len = (trailer - loc.footer_offset as usize) as u32;
        let signal = bytes[trailer + 10];
        let reserved = bytes[trailer + 11];
        let magic = [
            bytes[trailer + 12],
            bytes[trailer + 13],
            bytes[trailer + 14],
            bytes[trailer + 15],
        ];
        let mut crc = crc32c::crc32c(footer_bytes);
        crc = crc32c::crc32c_append(crc, &footer_len.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &version.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &[signal, reserved]);
        crc = crc32c::crc32c_append(crc, &magic);
        bytes[trailer + 4..trailer + 8].copy_from_slice(&crc.to_le_bytes());
        bytes[trailer + 8..trailer + 10].copy_from_slice(&version.to_le_bytes());
    }

    /// An object carrying the sparse pair takes the sparse decode branch
    /// whatever its trailer version says. A version-keyed selection would send
    /// a future object down the whole-catalog branch, which then looks for a
    /// kind-6 SERIES_META the object does not carry.
    ///
    /// The full reader path cannot be driven with such an object: `parse_footer`
    /// rejects any version outside `SUPPORTED_VERSIONS` before the branch is
    /// reached, which the middle of this test asserts. So the branch selection
    /// is driven directly, on the footer decoded from the retrailered bytes.
    #[test]
    fn sparse_branch_selected_by_section_presence_not_version() {
        let mut bytes = sparse_object();
        let loc = ravel_segment::open_from_full(&bytes, ReaderLimits::default())
            .expect("open the sparse object");
        assert!(
            has_section(&loc, SERIES_IDX) && has_section(&loc, SERIES_META_CHUNKS),
            "the writer must have emitted the sparse pair"
        );
        assert!(
            !has_section(&loc, SERIES_META),
            "the sparse pair replaces the whole-section catalog"
        );
        assert!(
            catalog_is_sparse(&loc).expect("classify the untampered object"),
            "an object carrying the sparse pair is on the sparse branch"
        );

        // A trailer version other than the one this build writes.
        let other = SUPPORTED_VERSIONS.newest() + 1;
        set_trailer_version(&mut bytes, &loc, other);

        // The reader gate rejects it before the branch is reached, so the
        // branch is exercised directly below rather than through a load.
        let opened = ravel_segment::parse_footer(bytes.len() as u64, &bytes);
        assert!(
            matches!(opened, Err(SegmentError::UnsupportedVersion(v)) if v == other),
            "expected the version gate to reject the retrailered object, got {opened:?}"
        );

        // The retrailer changed no footer byte, so the section list the branch
        // reads is the object's own.
        let footer: ravel_segment::Footer =
            prost::Message::decode(&bytes[loc.footer_offset as usize..loc.trailer_offset as usize])
                .expect("decode the retrailered object's footer");
        assert_eq!(footer, loc.footer);
        let future = FooterLocation {
            footer,
            version: other,
            ..loc
        };
        assert!(
            catalog_is_sparse(&future).expect("classify the retrailered object"),
            "section presence, not the trailer version, selects the sparse branch"
        );
    }

    /// Half a sparse pair stays Corrupted (docs/segment-format.md validation:
    /// "one half of the sparse pair without the other"), never silently
    /// resolved to either branch.
    #[test]
    fn half_a_sparse_pair_is_corrupted() {
        let bytes = sparse_object();
        let loc = ravel_segment::open_from_full(&bytes, ReaderLimits::default())
            .expect("open the sparse object");

        for dropped in [SERIES_IDX, SERIES_META_CHUNKS] {
            let mut half = loc.clone();
            half.footer.sections.retain(|s| s.kind != dropped);
            assert!(
                has_section(&half, SERIES_IDX) != has_section(&half, SERIES_META_CHUNKS),
                "the fixture must carry exactly one half of the pair"
            );
            let got = catalog_is_sparse(&half);
            assert!(
                matches!(
                    got,
                    Err(MaintainError::Segment(
                        SegmentError::SparseSectionsIncomplete
                    ))
                ),
                "dropping section kind {dropped} must fail closed, got {got:?}"
            );
        }
    }
}
