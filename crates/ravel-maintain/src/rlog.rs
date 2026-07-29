//! The RLOG side of the codec seam (ADR-0032, issue #231): L0-to-L1 log
//! segment compaction.
//!
//! # What the merge does (docs/log-segment-format.md, ADR-0032)
//!
//! An L1 `.rlog` part is the sorted union of its inputs' records, re-blocked
//! from scratch. Concretely the merge:
//!
//! - takes the sorted `STREAM_DIR`s of all inputs and forms one merged, sorted
//!   stream set (the *global `stream_ref` remap* -- every input's local
//!   `stream_ref` values are renumbered into this one directory);
//! - checks the cross-object stream-identity invariant explicitly: two inputs
//!   may list the same `stream_id` only with byte-identical resource+scope
//!   blobs, because `stream_id` is the canonical hash of exactly those bytes
//!   (a disagreement is an upstream bug or a hash collision, and a merge is the
//!   first place it becomes visible across objects) -- a mismatch is a typed
//!   [`MaintainError::StreamAttrsConflict`], never a silent pick;
//! - re-sorts the merged record set by `(stream_ref, ts)` ascending, rebuilds
//!   `FIELD_DIR` from the merged column set under the same 1000-dynamic-column
//!   cap with overflow folded into `attrs_raw`, and rebuilds `SKIP_IDX` and the
//!   per-block `BLOOM`s over the merged, re-blocked contents at the same 8192
//!   record block target.
//!
//! # Reuse, not reimplementation
//!
//! Every one of those encode steps is exactly what [`ravel_logseg::RlogWriter`]
//! already does for a single-object L0 write. So the merge does not re-derive
//! any of them: it decodes each input's records back to [`LogRecord`]s with
//! [`RlogReader`], pushes the merged records into a fresh `RlogWriter`, and
//! calls [`RlogWriter::finish_compacted`] to stamp `level = 1`, the compaction
//! `input_set_hash`, and the `part_index`. The dynamic-column cap, the
//! `attrs_raw` overflow encoding, the bloom sizing rule (per-block, sized by
//! that block's own token cardinality), and the block framing all come from the
//! one writer implementation, so an L0 write and an L1 merge cannot drift. The
//! only ravel-logseg addition this required is `finish_compacted` itself.
//!
//! # Memory
//!
//! The read side ([`RlogCodec::load_input_catalog`]) retains only per-input
//! metadata (the `STREAM_DIR` and the object key), never block/bloom bytes.
//! The merge decodes one merged stream at a time (via a `StreamIn` scan per
//! input) and accumulates at most one in-flight part's records before flushing,
//! so decoded page data is bounded by one part plus one stream, never the whole
//! bucket's decoded data at once (the plan's load-bearing memory bound).
//!
//! Unlike RSEG, the merge does re-fetch each input object whole during the
//! merge: RLOG's reader is whole-object (there is no ranged footer/section
//! reader for `.rlog` the way `ravel_segment::open_from_suffix` gives RSEG), so
//! the raw object bytes are resident while their blocks are decoded. That keeps
//! *decoded* memory bounded, which is the part the plan cares about, but it
//! does hold the raw input bytes; a ranged RLOG reader would close that gap and
//! is noted as a follow-up rather than built here (it is outside this task's
//! one authorized ravel-logseg change).

use std::collections::BTreeMap;

use ravel_commit::keys;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::stream_dir::StreamDir;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, Predicate, RlogConfig, RlogReader, RlogWriter,
    field_dir::FieldDir, read_section, writer::ObjectIdentity,
};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::CompactionPart;

use crate::bucket::Bucket;
use crate::build::{BuiltPart, put_part};
use crate::codec::SegmentCodec;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read::InputRecord;

/// The RLOG output trailer version every L1 part carries (ADR-0032, trailer
/// version 2). Recorded in each part's `CompactionPart.segment_format_version`,
/// the log analogue of RSEG's [`crate::build::OUTPUT_FORMAT_VERSION`].
pub const OUTPUT_FORMAT_VERSION: u32 = 2;

/// Untrusted-input caps for the two directory sections decoded on the read
/// path (mirroring the reader's own private caps: a real object never
/// approaches them, they only bound allocation from a hostile footer).
const MAX_STREAMS: u64 = 1 << 24;
const MAX_FIELDS: u64 = 1 << 20;

/// One decoded RLOG input's catalog metadata: the data-object key and its
/// `STREAM_DIR`. This is all the read side retains; the block/skip/bloom bytes
/// are decoded lazily during the merge (docs/log-segment-format.md, this
/// module's memory note).
#[derive(Debug, Clone)]
pub struct RlogInputCatalog {
    pub object_key: String,
    pub stream_dir: StreamDir,
}

/// The logs codec: implements the [`SegmentCodec`] seam for `.rlog` objects.
pub struct RlogCodec;

impl SegmentCodec for RlogCodec {
    type Catalog = RlogInputCatalog;

    async fn load_input_catalog(
        store: &dyn ObjectStoreBackend,
        _config: &CompactorConfig,
        input: &InputRecord,
    ) -> Result<Self::Catalog> {
        let object_key = keys::reconstruct_data_key(&input.record)?;
        // RLOG has no ranged footer reader, so the whole object is fetched to
        // read its footer; only metadata is retained past this scope.
        let got = store.get(&object_key, GetRange::Full).await?;
        let bytes = got.data;
        let cfg = RlogConfig::default();

        let ftr = footer::open(&bytes)?;
        // Decode STREAM_DIR and FIELD_DIR (catalog metadata only, mirroring the
        // RSEG read path). FIELD_DIR is decoded to validate it and to fail loud
        // on a corrupt input before the merge, but the merge rebuilds it from
        // the merged column set, so it is not retained.
        let stream_raw = read_section(&bytes, section(&ftr, kind::STREAM_DIR)?, &cfg)?;
        let stream_dir = StreamDir::decode(&stream_raw, MAX_STREAMS)?;
        let field_raw = read_section(&bytes, section(&ftr, kind::FIELD_DIR)?, &cfg)?;
        let _field_dir = FieldDir::decode(&field_raw, MAX_FIELDS)?;

        Ok(RlogInputCatalog {
            object_key,
            stream_dir,
        })
    }

    async fn build_parts(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        bucket: &Bucket,
        inputs: &[InputRecord],
        catalogs: &[Self::Catalog],
        input_set_hash: &[u8; 32],
    ) -> Result<Vec<BuiltPart>> {
        if inputs.len() != catalogs.len() {
            return Err(MaintainError::Invariant(
                "inputs and catalogs length mismatch".to_string(),
            ));
        }

        // Global stream_ref remap + cross-object stream-identity check. The
        // merged set is the sorted union of every input's STREAM_DIR; the dense
        // merged stream_ref is the ordinal in this set (the writer re-derives it
        // per part, so we need only the ordering here). Two inputs claiming the
        // same stream_id with different blobs is a fatal invariant breach.
        let mut merged: BTreeMap<LogStreamId, Vec<u8>> = BTreeMap::new();
        for catalog in catalogs {
            for entry in catalog.stream_dir.entries() {
                match merged.entry(entry.stream_id) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(entry.blob.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(slot) => {
                        if slot.get() != &entry.blob {
                            return Err(MaintainError::StreamAttrsConflict {
                                stream_id: entry.stream_id.to_hex(),
                                a_len: slot.get().len(),
                                b_len: entry.blob.len(),
                            });
                        }
                    }
                }
            }
        }

        // Fetch every input whole and open a reader over it. RLOG's reader is
        // whole-object; the raw bytes stay resident for the merge, but only one
        // stream's records are decoded at a time (StreamIn scan below).
        let cfg = RlogConfig::default();
        let mut objects: Vec<bytes::Bytes> = Vec::with_capacity(catalogs.len());
        for catalog in catalogs {
            let got = store.get(&catalog.object_key, GetRange::Full).await?;
            objects.push(got.data);
        }
        let mut readers: Vec<RlogReader<'_>> = Vec::with_capacity(objects.len());
        for obj in &objects {
            readers.push(RlogReader::new(obj, &cfg)?);
        }

        let identity = compactor_identity(bucket, config);
        let mut parts = Vec::new();
        let mut part_index: u32 = 0;
        let mut batch: Vec<LogRecord> = Vec::new();
        let mut batch_bytes: u64 = 0;

        // Merge stream by stream in sorted stream_id order. Each stream's
        // records are gathered from every input carrying it, ts-merged, and
        // pushed into the current part; a part flushes on a stream boundary once
        // it reaches the size cap (so a stream never straddles two parts, the
        // log analogue of RSEG's series-boundary split).
        for stream_id in merged.keys() {
            let mut recs = gather_stream(&readers, stream_id)?;
            // Stable sort by ts: within one stream this is the format's
            // (stream_ref, ts) order, and ts ties keep canonical input order
            // (readers are iterated in the inputs' canonical order). No dedup:
            // distinct submissions of identical content are distinct records
            // (ADR-0032).
            recs.sort_by_key(|r| r.ts_ns);
            for r in recs {
                batch_bytes = batch_bytes.saturating_add(estimate_record(&r));
                batch.push(r);
            }
            if batch_bytes >= config.max_l1_part_bytes && !batch.is_empty() {
                let part = flush_part(
                    store,
                    bucket,
                    &identity,
                    input_set_hash,
                    part_index,
                    std::mem::take(&mut batch),
                )
                .await?;
                parts.push(part);
                part_index += 1;
                batch_bytes = 0;
            }
        }
        if !batch.is_empty() {
            let part =
                flush_part(store, bucket, &identity, input_set_hash, part_index, batch).await?;
            parts.push(part);
        }

        Ok(parts)
    }
}

/// Gather one stream's records from every input carrying it. A `StreamIn` scan
/// prunes to the stream's blocks and returns exactly its records, decoded in ts
/// order; an input that does not carry the stream returns nothing. This reuses
/// the reader's full decode path (fixed columns, dynamic columns, and
/// `attrs_raw` overflow), so a merged record is a faithful round-trip of the
/// input record.
fn gather_stream(readers: &[RlogReader<'_>], stream_id: &LogStreamId) -> Result<Vec<LogRecord>> {
    let mut out = Vec::new();
    let pred = Predicate::StreamIn(vec![*stream_id]);
    for reader in readers {
        let (rows, _stats) = reader.scan(&pred)?;
        out.extend(rows);
    }
    Ok(out)
}

/// Build one L1 part from an accumulated record batch: encode it through the
/// shared writer pipeline via [`RlogWriter::finish_compacted`] (stamping
/// `level = 1`, the `input_set_hash`, and `part_index`), then PUT it
/// `CreateIfAbsent`. The part's summary stats are read back from the produced
/// object's own footer, so they describe exactly what was written.
async fn flush_part(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    identity: &ObjectIdentity,
    input_set_hash: &[u8; 32],
    part_index: u32,
    batch: Vec<LogRecord>,
) -> Result<BuiltPart> {
    let (first_stream_id, last_stream_id) = stream_id_bounds(&batch);

    let mut writer = RlogWriter::new(RlogConfig::default(), *identity);
    for r in batch {
        writer.push(r)?;
    }
    let object = writer.finish_compacted(1, input_set_hash.to_vec(), part_index)?;
    let object = bytes::Bytes::from(object);

    // Authoritative summary from the object we just wrote.
    let ftr = footer::open(&object)?;
    let content_hash: [u8; 32] = *blake3::hash(&object).as_bytes();

    let input_set_hash16 = hex::encode(&input_set_hash[..8]);
    let hash16 = hex::encode(&content_hash[..8]);
    let key = keys::l1_part_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
        &input_set_hash16,
        part_index,
        &hash16,
    )?;

    let part = CompactionPart {
        part_index,
        first_series_id: first_stream_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        last_series_id: last_stream_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        content_hash: content_hash.to_vec(),
        object_size: object.len() as u64,
        sample_count: ftr.record_count,
        series_count: ftr.stream_count,
        // Logs have no run concept (no cross-record dedup runs, ADR-0032); the
        // per-record count already lives in sample_count.
        run_count: 0,
        min_event_ts_ns: ftr.min_ts_ns,
        max_event_ts_ns: ftr.max_ts_ns,
        segment_format_version: OUTPUT_FORMAT_VERSION,
    };
    let built = BuiltPart {
        key,
        bytes: object,
        part,
    };
    put_part(store, &built).await?;
    Ok(built)
}

/// The min and max `stream_id` over a batch's records: the part's inclusive
/// id range (`first_series_id`/`last_series_id` in the record). Because streams
/// are merged in sorted order and a stream never straddles a part, adjacent
/// parts' ranges are disjoint and ascending.
fn stream_id_bounds(batch: &[LogRecord]) -> (Option<LogStreamId>, Option<LogStreamId>) {
    let mut min: Option<LogStreamId> = None;
    let mut max: Option<LogStreamId> = None;
    for r in batch {
        min = Some(min.map_or(r.stream_id, |m| m.min(r.stream_id)));
        max = Some(max.map_or(r.stream_id, |m| m.max(r.stream_id)));
    }
    (min, max)
}

/// The compactor's object identity for an L1 part. `writer_epoch`/`writer_seq`
/// are zero and `writer_id` is the compactor's uuid: informational only, never
/// part of any identity or dedup order (RLOG has none), matching the RSEG L1
/// writer's identity convention (`build.rs`).
fn compactor_identity(bucket: &Bucket, config: &CompactorConfig) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: bucket.tenant_hash.0,
        shard: bucket.shard,
        writer_id: config.compactor_writer_id.into_bytes(),
        writer_epoch: 0,
        writer_seq: 0,
    }
}

/// A rough uncompressed byte estimate for one record, for the part size cap.
/// Approximate on purpose (like RSEG's accumulated page-byte estimate): it only
/// decides where parts split, never correctness.
fn estimate_record(r: &LogRecord) -> u64 {
    let mut est: u64 = 48; // fixed-column overhead per row
    est += r.body.len() as u64;
    est += r.severity_text.len() as u64;
    for (k, v) in &r.attrs {
        est += k.len() as u64 + attr_value_estimate(v);
    }
    est
}

fn attr_value_estimate(v: &AttrValue) -> u64 {
    match v {
        AttrValue::Str(s) => s.len() as u64 + 2,
        AttrValue::Bytes(b) => b.len() as u64 + 2,
        AttrValue::List(items) => items.iter().map(attr_value_estimate).sum::<u64>() + 2,
        AttrValue::Map(kvs) => {
            kvs.iter()
                .map(|(k, v)| k.len() as u64 + attr_value_estimate(v))
                .sum::<u64>()
                + 2
        }
        _ => 8,
    }
}

/// The descriptor for a required section kind, or a typed error if absent.
fn section(ftr: &footer::LogFooter, k: u32) -> Result<&footer::SectionDesc> {
    ftr.section(k).ok_or_else(|| {
        MaintainError::Invariant(format!("input .rlog object missing section kind {k}"))
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeSet;

    use bytes::Bytes;
    use proptest::prelude::*;
    use prost::Message;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_logseg::field_dir::FieldDir;
    use ravel_logseg::{FieldSel, RlogConfig, RlogReader, RlogWriter, footer, read_section};
    use ravel_logseg::{LogRecord, Predicate, stream_attrs_bytes, writer::ObjectIdentity};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::CompactionRecord;
    use ravel_types::logstream::{AttrValue, LogStreamId, canonical_attr_bytes, log_stream_id};
    use ravel_types::{Signal, TenantHash, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::{Bucket, CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};

    const TENANT: &str = "acme";
    const SHARD: u32 = 7;
    const HOUR: u32 = 495_000;
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;

    fn tenant_hash() -> TenantHash {
        TenantId::new(TENANT).hash()
    }

    fn bucket() -> Bucket {
        Bucket::new(tenant_hash(), Signal::Logs, SHARD, HOUR)
    }

    /// Past the seal margin for [`HOUR`] under default config.
    fn sealed_now_ns() -> i64 {
        (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
    }

    /// A synthetic stream `n`'s id and canonical resource+scope blob. Distinct
    /// per `n`, and the id is the true hash of the blob, so the object records
    /// real stream identity, never a placeholder.
    fn stream_ident(n: u32) -> (LogStreamId, Vec<u8>) {
        let res = vec![(
            "service.name".to_string(),
            AttrValue::Str(format!("svc{n}")),
        )];
        let id = log_stream_id(&res, "scope", "1", &[]);
        let blob = stream_attrs_bytes(&res, "scope", "1", &[]);
        (id, blob)
    }

    fn record(stream_n: u32, ts: i64, body: &str, attrs: Vec<(String, AttrValue)>) -> LogRecord {
        let (stream_id, stream_attrs) = stream_ident(stream_n);
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

    /// Seed one L0 `.rlog` input (data object + commit record), exactly as the
    /// ingest log shard would (`ravel-ingest`), and return the object bytes so a
    /// test can decode the input directly for a differential check.
    async fn seed(
        store: &dyn ObjectStoreBackend,
        writer_id: Uuid,
        seq: u64,
        records: &[LogRecord],
    ) -> Bytes {
        let th = tenant_hash();
        let identity = ObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let mut w = RlogWriter::new(RlogConfig::default(), identity);
        for r in records {
            w.push(r.clone()).expect("push");
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
            .expect("put data");

        let mut ids: BTreeSet<LogStreamId> = BTreeSet::new();
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
            .expect("put commit");
        bytes
    }

    /// Decode every record of an RLOG object (no predicate), in the object's
    /// stored `(stream_ref, ts)` order.
    fn decode_all(bytes: &[u8]) -> Vec<LogRecord> {
        let cfg = RlogConfig::default();
        let reader = RlogReader::new(bytes, &cfg).expect("open");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        rows
    }

    /// Fetch the single compaction record in the bucket and every L1 part it
    /// references (parts in record order = ascending stream ranges).
    async fn read_output(store: &dyn ObjectStoreBackend) -> (CompactionRecord, Vec<Bytes>) {
        let b = bucket();
        let prefix =
            keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
                .unwrap();
        let metas = list_all(store, &prefix).await.unwrap();
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
        let got = store.get(&rec_keys[0], GetRange::Full).await.unwrap();
        let recrd = CompactionRecord::decode(got.data.as_ref()).unwrap();
        let mut parts = Vec::new();
        for p in &recrd.parts {
            let key = keys::reconstruct_l1_part_key(&recrd, p).unwrap();
            parts.push(store.get(&key, GetRange::Full).await.unwrap().data);
        }
        (recrd, parts)
    }

    /// An order-independent canonical key for a record: the attribute set is
    /// folded through the frozen `canonical_attr_bytes` grammar so that whether
    /// an attribute was stored columnar or in `attrs_raw` (which can differ
    /// between an L0 input and the L1 merge) does not affect equality.
    type Canon = (
        [u8; 16],
        i64,
        i64,
        u8,
        String,
        String,
        Option<[u8; 16]>,
        Option<[u8; 8]>,
        u32,
        Vec<u8>,
        Vec<u8>,
    );
    fn canon(r: &LogRecord) -> Canon {
        (
            r.stream_id.0,
            r.ts_ns,
            r.observed_ts_ns,
            r.severity_num,
            r.severity_text.clone(),
            r.body.clone(),
            r.trace_id,
            r.span_id,
            r.flags,
            canonical_attr_bytes(&r.attrs),
            r.stream_attrs.clone(),
        )
    }

    fn canon_multiset(records: &[LogRecord]) -> Vec<Canon> {
        let mut v: Vec<Canon> = records.iter().map(canon).collect();
        v.sort();
        v
    }

    /// The FIELD_DIR entry count of an RLOG object.
    fn field_dir_len(bytes: &[u8]) -> usize {
        let cfg = RlogConfig::default();
        let ftr = footer::open(bytes).expect("open");
        let raw =
            read_section(bytes, ftr.section(kind::FIELD_DIR).unwrap(), &cfg).expect("section");
        FieldDir::decode(&raw, MAX_FIELDS).expect("decode").len()
    }

    #[tokio::test]
    async fn compacts_two_l0_rlog_objects_into_one_l1_part_verbatim() {
        let store = MemoryStore::new();
        let a = vec![
            record(0, 10, "alpha", vec![("k".into(), AttrValue::I64(1))]),
            record(1, 20, "bravo", Vec::new()),
        ];
        let b = vec![
            record(0, 15, "charlie", vec![("k".into(), AttrValue::I64(2))]),
            record(2, 5, "delta", Vec::new()),
        ];
        let a_bytes = seed(&store, Uuid::from_u128(1), 1, &a).await;
        let b_bytes = seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

        let (rec, parts) = read_output(&store).await;
        assert_eq!(rec.level, 1);
        assert!(!rec.input_set_hash.is_empty());
        assert_eq!(parts.len(), 1, "small corpus fits one part");
        // The single L1 part decodes as an L1 object with a non-empty hash.
        let ftr = footer::open(&parts[0]).expect("open l1");
        assert_eq!(ftr.level, 1);
        assert!(!ftr.input_set_hash.is_empty());
        assert_eq!(rec.parts[0].segment_format_version, OUTPUT_FORMAT_VERSION);

        // The L1 records are the union of both inputs, decoded in (stream_ref,
        // ts) order. The part's own STREAM_DIR resolves stream identity.
        let l1 = decode_all(&parts[0]);
        // Order check: stored order is (stream_ref, ts) ascending.
        let order: Vec<(LogStreamId, i64)> = l1.iter().map(|r| (r.stream_id, r.ts_ns)).collect();
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(order, sorted, "L1 records in (stream, ts) order");

        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));

        // And decoding the inputs directly then concatenating gives the same set.
        let mut direct = decode_all(&a_bytes);
        direct.extend(decode_all(&b_bytes));
        assert_eq!(canon_multiset(&l1), canon_multiset(&direct));
    }

    #[tokio::test]
    async fn same_stream_different_attrs_across_objects_is_typed_error() {
        // Two inputs claim the same stream_id with different resource+scope
        // blobs: an upstream identity violation the merge must fail loud on
        // (the cross-object analogue of writer.rs's
        // `same_stream_different_attrs_rejected`).
        let store = MemoryStore::new();
        let (id, blob_ok) = stream_ident(0);
        let mut good = record(0, 1, "x", Vec::new());
        good.stream_id = id;
        good.stream_attrs = blob_ok;
        // Same id, a different (truthful-looking but conflicting) blob.
        let mut clash = record(0, 2, "y", Vec::new());
        clash.stream_id = id;
        clash.stream_attrs = stream_attrs_bytes(
            &[("service.name".into(), AttrValue::Str("OTHER".into()))],
            "scope",
            "1",
            &[],
        );

        seed(&store, Uuid::from_u128(1), 1, &[good]).await;
        seed(&store, Uuid::from_u128(2), 2, &[clash]).await;

        let clock = FixedClock::new(sealed_now_ns());
        let err = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect_err("must reject conflicting stream attrs");
        match err {
            MaintainError::StreamAttrsConflict { stream_id, .. } => {
                assert_eq!(stream_id, id.to_hex(), "error must name the stream");
            }
            other => panic!("expected StreamAttrsConflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dynamic_column_union_over_cap_folds_overflow_into_attrs_raw() {
        // Each input stays well under the 1000-column cap, but their union
        // exceeds it: the merge must apply the same cap-and-spill rule, folding
        // the overflow keys into attrs_raw. No value is dropped and FIELD_DIR is
        // never left over-cap.
        let store = MemoryStore::new();
        let attrs_a: Vec<(String, AttrValue)> = (0..600)
            .map(|i| (format!("a{i:03}"), AttrValue::I64(i)))
            .collect();
        let attrs_b: Vec<(String, AttrValue)> = (0..600)
            .map(|i| (format!("b{i:03}"), AttrValue::I64(i)))
            .collect();
        let a = vec![record(0, 1, "x", attrs_a.clone())];
        let b = vec![record(0, 2, "y", attrs_b.clone())];
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1);

        // FIELD_DIR is capped at exactly 1000 dynamic columns (union was 1200).
        assert_eq!(field_dir_len(&parts[0]), 1000, "FIELD_DIR capped at 1000");

        // Every attribute of every input record still round-trips (the 200
        // overflow keys live in attrs_raw, never dropped).
        let l1 = decode_all(&parts[0]);
        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));
        for r in &l1 {
            assert_eq!(r.attrs.len(), 600, "all 600 attrs preserved per record");
        }
    }

    #[tokio::test]
    async fn bloom_is_rebuilt_over_merged_block_not_copied_from_one_input() {
        // Two inputs contribute the same stream; input A's records carry the
        // token "alpha" in body, input B's carry "beta". They merge into the
        // same output block. A bloom copied from one input would be missing the
        // other's token and wrongly prune it; a rebuilt bloom (sized by the
        // merged block's own tokens) contains both, so a HasWord scan over the
        // L1 output finds both.
        let store = MemoryStore::new();
        let a: Vec<LogRecord> = (0..8)
            .map(|i| record(0, i * 2, "alpha alpha", Vec::new()))
            .collect();
        let b: Vec<LogRecord> = (0..8)
            .map(|i| record(0, i * 2 + 1, "beta beta", Vec::new()))
            .collect();
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1);

        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&parts[0], &cfg).expect("open l1");
        for (word, want) in [("alpha", 8usize), ("beta", 8usize)] {
            let (rows, stats) = reader
                .scan(&Predicate::HasWord {
                    field: FieldSel::Body,
                    word: word.into(),
                })
                .expect("scan");
            assert_eq!(rows.len(), want, "HasWord({word}) must find every match");
            assert!(
                stats.blocks_scanned >= 1,
                "the merged block survived bloom pruning for {word}"
            );
        }
    }

    #[tokio::test]
    async fn part_splitting_keeps_streams_whole_and_disjoint() {
        // Many distinct streams and a tiny part cap force splits on stream
        // boundaries: parts get disjoint, ascending stream-id ranges (a stream
        // never straddles two parts), and the union of records is preserved.
        let store = MemoryStore::new();
        let mk = |seq_body: &str| -> Vec<LogRecord> {
            (0..20u32)
                .map(|s| record(s, i64::from(s), seq_body, Vec::new()))
                .collect()
        };
        let a = mk("aaaa");
        let b = mk("bbbb");
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        // Tiny cap to force splits on stream boundaries.
        let config = CompactorConfig {
            max_l1_part_bytes: 256,
            ..CompactorConfig::default()
        };

        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (rec, parts) = read_output(&store).await;
        assert!(parts.len() >= 2, "tiny cap must split into parts");

        // Part stream-id ranges are disjoint and ascending.
        let mut prev_last: Option<[u8; 16]> = None;
        for (i, p) in rec.parts.iter().enumerate() {
            let first: [u8; 16] = p.first_series_id.as_slice().try_into().unwrap();
            let last: [u8; 16] = p.last_series_id.as_slice().try_into().unwrap();
            assert!(first <= last);
            if let Some(pl) = prev_last {
                assert!(
                    pl < first,
                    "part stream ranges must be disjoint and ordered"
                );
            }
            prev_last = Some(last);
            // Every part is a valid L1 object.
            assert_eq!(footer::open(&parts[i]).expect("open").level, 1);
        }

        // Content complete across all parts.
        let mut l1: Vec<LogRecord> = Vec::new();
        for p in &parts {
            l1.extend(decode_all(p));
        }
        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));
    }

    // --- keystone differential property test ---------------------------------

    #[derive(Debug, Clone)]
    struct RecSpec {
        stream_n: u32,
        ts: i64,
        body: String,
        attrs: Vec<(String, AttrValue)>,
    }

    fn attr_strategy() -> impl Strategy<Value = (String, AttrValue)> {
        let key = prop::sample::select(vec!["k0", "k1", "k2", "k3"]).prop_map(String::from);
        let val = prop_oneof![
            (0i64..8).prop_map(AttrValue::I64),
            prop::sample::select(vec!["p", "q", "r"]).prop_map(|s| AttrValue::Str(s.into())),
            any::<bool>().prop_map(AttrValue::Bool),
        ];
        (key, val)
    }

    fn rec_strategy() -> impl Strategy<Value = RecSpec> {
        (
            0u32..4,
            0i64..40,
            prop::sample::select(vec!["ok", "warn timeout", "connection refused", "fine"]),
            prop::collection::vec(attr_strategy(), 0..3),
        )
            .prop_map(|(stream_n, ts, body, attrs)| RecSpec {
                stream_n,
                ts,
                body: body.into(),
                attrs,
            })
    }

    fn corpus_strategy() -> impl Strategy<Value = Vec<Vec<RecSpec>>> {
        // 2..=5 inputs, each 1..=15 records.
        prop::collection::vec(prop::collection::vec(rec_strategy(), 1..15), 2..6)
    }

    async fn differential_check(corpus: Vec<Vec<RecSpec>>) {
        let store = MemoryStore::new();
        let mut all_input_records: Vec<LogRecord> = Vec::new();
        for (i, input) in corpus.iter().enumerate() {
            let records: Vec<LogRecord> = input
                .iter()
                .map(|s| record(s.stream_n, s.ts, &s.body, s.attrs.clone()))
                .collect();
            all_input_records.extend(records.clone());
            seed(
                &store,
                Uuid::from_u128((i + 1) as u128),
                (i + 1) as u64,
                &records,
            )
            .await;
        }

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (rec, parts) = read_output(&store).await;
        assert_eq!(rec.level, 1);

        // Decode every L1 part (concatenated in part order) and compare its
        // record set to the inputs decoded directly. Both are compared as an
        // order-independent canonical multiset (the correctness core).
        let mut l1: Vec<LogRecord> = Vec::new();
        for p in &parts {
            l1.extend(decode_all(p));
        }
        assert_eq!(
            canon_multiset(&l1),
            canon_multiset(&all_input_records),
            "L1 decoded set must equal the input union"
        );

        // Within each part, records are in (stream_ref, ts) order.
        for p in &parts {
            let recs = decode_all(p);
            let order: Vec<(LogStreamId, i64)> =
                recs.iter().map(|r| (r.stream_id, r.ts_ns)).collect();
            let mut sorted = order.clone();
            sorted.sort();
            assert_eq!(order, sorted, "part records in (stream, ts) order");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

        /// The correctness core (ADR-0032, issue #231): for a random corpus of
        /// log records split across N L0 objects, the full decoded record set is
        /// identical whether the N L0 inputs are decoded and concatenated or the
        /// single compacted L1 output is decoded.
        #[test]
        fn differential_l0_union_equals_l1_output(corpus in corpus_strategy()) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(differential_check(corpus));
        }
    }
}
