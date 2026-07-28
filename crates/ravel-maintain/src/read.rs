//! Input discovery and decode (docs/compaction-retention-plan.md §3.3 steps
//! 1-2). Lists a sealed bucket, partitions its objects by key shape, decodes
//! every L0 commit record, sorts the inputs canonically, computes the
//! `input_set_hash`, and decodes each input segment's catalog down to
//! per-run absolute page ranges.
//!
//! Only catalog metadata is retained here: footers and the LABEL_DICT /
//! SERIES_IDS / SERIES_META sections. The verbatim TS/VAL/HIST page bytes are
//! fetched lazily during the merge ([`crate::build`]) with ranged GETs, so
//! peak memory is bounded by catalog metadata plus one in-flight part buffer
//! (plan §3.3 memory bound), not by the whole bucket's page data.

use ravel_commit::keys::{self, BucketEntry};
use ravel_commit::record;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError, list_all};
use ravel_proto::commit::v1::CommitRecord;
use ravel_segment::{
    FooterOutcome, ReaderLimits, ValueKind, decode_catalog_v4, decode_catalog_v5, open_from_suffix,
    plan_ranges_v4,
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

/// Domain-separated prefix for the `input_set_hash` preimage. Fixes the
/// canonical byte stream so any two compactors over the same sealed bucket
/// derive the same hash and therefore the same record key (plan §3.1/§3.4).
const INPUT_SET_HASH_DOMAIN: &[u8] = b"ravel-compaction-input-set-v1\0";

/// The partitioned result of listing a bucket's `c/<shard>/<hour>/` prefix
/// (plan §3.5: "partitions listed keys by shape"). Unknown shapes are a hard
/// error surfaced by [`list_bucket`], never silently dropped.
#[derive(Debug, Default)]
pub struct BucketListing {
    /// L0 commit record keys (the compaction inputs).
    pub commit_keys: Vec<String>,
    /// Compaction record keys already present in the bucket.
    pub compaction_record_keys: Vec<String>,
    /// The retention tombstone key, if the bucket is tombstoned.
    pub tombstone_key: Option<String>,
}

/// List one bucket and classify every object by key shape (plan §3.1/§3.5).
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
/// `(writer_id, writer_epoch, writer_seq)` (plan §3.3 step 1).
pub async fn load_inputs(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    commit_keys: &[String],
) -> Result<Vec<InputRecord>> {
    let mut inputs = Vec::with_capacity(commit_keys.len());
    for key in commit_keys {
        let got = store.get(key, GetRange::Full).await?;
        let record = record::decode(&got.data)?;
        // The record's key must reconstruct to the key we listed it at.
        keys::commit_key_for_record(&record).and_then(|expected| {
            if expected == *key {
                Ok(())
            } else {
                Err(keys::KeyError::Malformed {
                    key: key.clone(),
                    reason: format!(
                        "commit key does not match record identity (expected {expected:?})"
                    ),
                })
            }
        })?;
        verify_input_matches_bucket(bucket, &record)?;
        inputs.push(InputRecord {
            commit_key: key.clone(),
            record,
        });
    }
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

/// Canonical `input_set_hash`: blake3 over a domain-separated, length-framed
/// encoding of the sorted input identities (plan §3.1). Inputs MUST already
/// be in canonical order (as [`load_inputs`] leaves them).
pub fn input_set_hash(inputs: &[InputRecord]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(INPUT_SET_HASH_DOMAIN);
    let count = inputs.len() as u64;
    hasher.update(&count.to_le_bytes());
    for input in inputs {
        let wid = input.record.writer_id.as_bytes();
        hasher.update(&(wid.len() as u64).to_le_bytes());
        hasher.update(wid);
        hasher.update(&input.record.writer_epoch.to_le_bytes());
        hasher.update(&input.record.writer_seq.to_le_bytes());
    }
    *hasher.finalize().as_bytes()
}

/// One run's copy plan: dedup-priority provenance (from the input's COMMIT
/// RECORD, plan §3.3 step 3), event-time bounds, sample count, and the
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
}

/// Decode one input's catalog into per-run absolute page ranges, stamping
/// every run's provenance from the input's commit record (plan §3.3 step 3).
///
/// Footer is located by a suffix probe (one GET, growing to a second GET only
/// if the probe missed the footer). The catalog sections are then read: a
/// sparse v5 object (series_count at or above the threshold) needs the whole
/// object for the sparse decode, but an L0-shaped object below the threshold
/// needs only its three catalog sections, fetched by range.
pub async fn load_input_catalog(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    input: &InputRecord,
) -> Result<InputCatalog> {
    let limits = ReaderLimits::default();
    let object_key = keys::reconstruct_data_key(&input.record)?;

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

    let sparse = loc.version == 5 && footer.series_count >= ravel_segment::V5_SPARSE_THRESHOLD;
    let entries = if sparse {
        // Sparse decode needs the whole object; L0 inputs rarely reach here.
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
                created_unix_ns: input.record.created_unix_ns,
                writer_epoch: input.record.writer_epoch,
                writer_seq: input.record.writer_seq,
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

    Ok(InputCatalog { object_key, series })
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
