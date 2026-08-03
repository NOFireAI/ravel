//! The RSPAN side of the codec seam (ADR-0032 seam, ADR-0041 format):
//! L0-to-L1 span segment compaction.
//!
//! # What the merge does (docs/span-segment-format.md, ADR-0041)
//!
//! An L1 `.rspan` part is the sorted union of its inputs' span records,
//! re-blocked from scratch. Concretely the merge:
//!
//! - decodes every input's span records and groups them by `trace_id`;
//! - iterates the merged trace set in ascending `trace_id` order, pushing each
//!   trace's records into a fresh [`RspanWriter`], and splits a part on a
//!   *trace boundary* once it reaches the size cap (so a trace never straddles
//!   two parts, giving parts disjoint, ascending `trace_id` ranges -- the span
//!   analogue of RLOG's stream-boundary split);
//! - lets the writer re-sort each part by `(trace_id, start_ts)`, re-chunk the
//!   blocks, and rebuild the interval-aware SKIP_IDX over the merged contents.
//!
//! # No `stream_ref`-equivalent remap (the RLOG departure)
//!
//! RLOG renumbers every input's local `stream_ref` values into one merged
//! STREAM_DIR and checks a cross-object stream-identity invariant, because an
//! RLOG record references its resource+scope identity indirectly through a
//! per-object stream directory. RSPAN has *no such indirection*: `trace_id` is
//! the direct sort key and every field of a [`SpanRecord`] (ids, timestamps,
//! status, the already-merged `attrs` map) is stored inline per row, not as a
//! reference into an object-local table (ADR-0041 sections 1 and 4;
//! `record.rs`: "no FIELD_DIR-style per-key column directory"). So the merge
//! needs no remap and no cross-object identity reconciliation -- a decoded span
//! from any input is already self-contained and re-encodes verbatim. This is a
//! genuine simplification over RLOG, not an omission.
//!
//! # Reuse, not reimplementation
//!
//! Every encode step (sort, block chunking, skip-index build, section framing,
//! footer/trailer) is exactly what [`RspanWriter`] already does for a
//! single-object L0 write. The merge does not re-derive any of it: it decodes
//! each input back to [`SpanRecord`]s with [`RspanReader`], pushes the merged
//! records into a fresh `RspanWriter`, and calls
//! [`RspanWriter::finish_compacted`] to stamp `level = 1`, the compaction
//! `input_set_hash`, and the `part_index`. An L0 write and an L1 merge share the
//! one writer implementation and so cannot drift.
//!
//! # Memory
//!
//! RSPAN v1's reader ([`RspanReader`]) opens over a whole object; unlike RLOG's
//! post-#275 `RlogRangeReader`, it has no ranged block API to fetch one trace's
//! blocks in isolation. So this codec fetches each input object whole, decodes
//! its records, and drops the raw bytes before fetching the next -- peak *raw*
//! resident bytes are bounded to one input object at a time, never the whole
//! bucket. Peak *decoded* data is the bucket's span records grouped by trace
//! (an L0 span bucket is a handful of small flush objects). This is the v1
//! analogue of the first RLOG merge, which likewise held decoded records before
//! RLOG grew a ranged section reader; a ranged `.rspan` reader is the natural
//! #275-style follow-up if span bucket sizes ever demand it.

use std::collections::BTreeMap;

use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::CompactionPart;
use ravel_rspan::footer::{self, SuffixOutcome};
use ravel_rspan::{ObjectIdentity, RspanConfig, RspanReader, RspanWriter, SpanQuery, SpanRecord};

use crate::bucket::Bucket;
use crate::build::{BuiltPart, put_part};
use crate::codec::SegmentCodec;
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};
use crate::read::InputRecord;

/// The RSPAN output trailer version every L1 part carries. Recorded in each
/// part's `CompactionPart.segment_format_version`, the span analogue of RSEG's
/// [`crate::build::OUTPUT_FORMAT_VERSION`] and RLOG's
/// [`crate::rlog::OUTPUT_FORMAT_VERSION`].
///
/// Tied to `ravel_rspan`'s own trailer version at compile time. As a mirrored
/// literal it went stale on the RSPAN v2 bump: `finish_compacted` stamps
/// `footer::VERSION` into the trailer while this recorded 1, so the compactor
/// wrote v2 parts that claimed to be v1.
pub const OUTPUT_FORMAT_VERSION: u32 = ravel_rspan::footer::VERSION as u32;

/// One RSPAN input's retained catalog metadata: the data-object key and its
/// decoded footer. The footer is the object's cheap metadata (trace_id/ts
/// bounds, record/block counts); the block bytes are fetched whole during the
/// merge (see the module memory note). Retaining the footer lets a corrupt or
/// missing input fail loud at load time, and its `record_count` cross-checks
/// the whole-object decode during the merge.
#[derive(Debug, Clone)]
pub struct SpanInputCatalog {
    pub object_key: String,
    pub footer: footer::SpanFooter,
}

/// The spans codec: implements the [`SegmentCodec`] seam for `.rspan` objects.
pub struct SpanCodec;

impl SegmentCodec for SpanCodec {
    type Catalog = SpanInputCatalog;

    async fn load_input_catalog(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        input: &InputRecord,
    ) -> Result<Self::Catalog> {
        let object_key = keys::reconstruct_data_key(&input.record)?;

        // Locate and validate the footer from a suffix probe: one ranged GET,
        // growing to a second only if the probe missed the whole footer (the
        // RSPAN analogue of the RSEG/RLOG read path). This fails loud here on a
        // missing or corrupt input, before the merge fetches any block bytes.
        let probe = store
            .get(&object_key, GetRange::Suffix(config.footer_probe_bytes))
            .await?;
        let total = probe.total_size;
        let ftr = match footer::open_from_suffix(&probe.data, total)? {
            SuffixOutcome::Ready(f) => f,
            SuffixOutcome::NeedRange { offset, len } => {
                let tail = store
                    .get(&object_key, GetRange::Range(offset, offset + len))
                    .await?;
                match footer::open_from_suffix(&tail.data, total)? {
                    SuffixOutcome::Ready(f) => f,
                    SuffixOutcome::NeedRange { .. } => {
                        return Err(MaintainError::Invariant(
                            "rspan footer not covered by ranged fetch".into(),
                        ));
                    }
                }
            }
        };
        Ok(SpanInputCatalog {
            object_key,
            footer: ftr,
        })
    }

    async fn build_parts(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        bucket: &Bucket,
        inputs: &[InputRecord],
        catalogs: Vec<Self::Catalog>,
        input_set_hash: &[u8; 32],
    ) -> Result<Vec<BuiltPart>> {
        if inputs.len() != catalogs.len() {
            return Err(MaintainError::Invariant(
                "inputs and catalogs length mismatch".to_string(),
            ));
        }

        // Group every input's span records by trace_id. Inputs are visited in
        // canonical order (catalogs are aligned one-to-one with the canonically
        // ordered inputs), and records are appended in that order, so records
        // tying on the writer's `(trace_id, start_ts)` key keep canonical input
        // order under the writer's stable sort -- deterministic output that
        // crash-recovery convergence depends on (plan §3.4). Only one input's
        // raw bytes are resident at a time (module memory note).
        let cfg = RspanConfig::default();
        let mut by_trace: BTreeMap<[u8; 16], Vec<SpanRecord>> = BTreeMap::new();
        for catalog in &catalogs {
            let got = store.get(&catalog.object_key, GetRange::Full).await?;
            let reader = RspanReader::new(got.data.as_ref(), &cfg)?;
            // A full-range scan prunes nothing and returns every record.
            let (recs, _stats) = reader.scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))?;
            // Integrity cross-check: a full scan must return exactly as many
            // records as the footer claims. A mismatch means a truncated or
            // inconsistent input; fail loud rather than silently merge less.
            if recs.len() as u64 != catalog.footer.record_count {
                return Err(MaintainError::Invariant(format!(
                    "rspan input {} decoded {} records but its footer claims {}",
                    catalog.object_key,
                    recs.len(),
                    catalog.footer.record_count
                )));
            }
            for r in recs {
                by_trace.entry(r.trace_id).or_default().push(r);
            }
        }

        let identity = compactor_identity(bucket, config);
        let mut parts = Vec::new();
        let mut part_index: u32 = 0;
        let mut batch: Vec<SpanRecord> = Vec::new();
        let mut batch_bytes: u64 = 0;
        let mut batch_traces: u64 = 0;

        // Emit parts trace by trace in ascending trace_id order. A part flushes
        // on a trace boundary once it reaches the size cap, so a trace never
        // straddles two parts and adjacent parts' trace_id ranges are disjoint
        // and ascending (the span analogue of RSEG's series-boundary and RLOG's
        // stream-boundary split).
        for (_trace_id, recs) in by_trace {
            batch_traces += 1;
            for r in &recs {
                batch_bytes = batch_bytes.saturating_add(estimate_record(r));
            }
            batch.extend(recs);
            if batch_bytes >= config.max_l1_part_bytes && !batch.is_empty() {
                let part = flush_part(
                    store,
                    bucket,
                    &identity,
                    input_set_hash,
                    part_index,
                    std::mem::take(&mut batch),
                    batch_traces,
                    config.dry_run,
                )
                .await?;
                parts.push(part);
                part_index += 1;
                batch_bytes = 0;
                batch_traces = 0;
            }
        }
        if !batch.is_empty() {
            let part = flush_part(
                store,
                bucket,
                &identity,
                input_set_hash,
                part_index,
                batch,
                batch_traces,
                config.dry_run,
            )
            .await?;
            parts.push(part);
        }

        Ok(parts)
    }
}

/// Build one L1 part from an accumulated span batch: encode it through the
/// shared writer via [`RspanWriter::finish_compacted`] (stamping `level = 1`,
/// the `input_set_hash`, and `part_index`), then PUT it `CreateIfAbsent`. The
/// part's summary stats are read back from the produced object's own footer, so
/// they describe exactly what was written.
#[allow(clippy::too_many_arguments)]
async fn flush_part(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    identity: &ObjectIdentity,
    input_set_hash: &[u8; 32],
    part_index: u32,
    batch: Vec<SpanRecord>,
    trace_count: u64,
    dry_run: bool,
) -> Result<BuiltPart> {
    let mut writer = RspanWriter::new(RspanConfig::default(), *identity);
    for r in batch {
        writer.push(r);
    }
    let object = writer.finish_compacted(1, input_set_hash.to_vec(), part_index)?;
    let object = bytes::Bytes::from(object);

    // Authoritative summary (bounds, counts) from the object we just wrote. The
    // footer's trace_id bounds are the part's inclusive id range; because
    // traces are merged in sorted order and a trace never straddles a part,
    // adjacent parts' ranges are disjoint and ascending.
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
        first_series_id: ftr.min_trace_id.to_vec(),
        last_series_id: ftr.max_trace_id.to_vec(),
        content_hash: content_hash.to_vec(),
        object_size: object.len() as u64,
        sample_count: ftr.record_count,
        // A span's "series" identity for the record's series-count field is its
        // trace_id; the footer carries no distinct-trace count, so it is the
        // number of traces accumulated into this part.
        series_count: trace_count,
        // Spans have no run concept (no cross-record dedup runs, ADR-0041); the
        // per-span count already lives in sample_count, like logs.
        run_count: 0,
        min_event_ts_ns: ftr.min_start_ts_ns,
        max_event_ts_ns: ftr.max_end_ts_ns,
        segment_format_version: OUTPUT_FORMAT_VERSION,
    };
    let built = BuiltPart {
        key,
        bytes: object,
        part,
    };
    if !dry_run {
        put_part(store, &built).await?;
    }
    Ok(built)
}

/// The compactor's object identity for an L1 part. `writer_epoch`/`writer_seq`
/// are zero and `writer_id` is the compactor's uuid: informational only, never
/// part of any identity or dedup order (RSPAN has none), matching the RSEG/RLOG
/// L1 writer identity convention.
fn compactor_identity(bucket: &Bucket, config: &CompactorConfig) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: bucket.tenant_hash.0,
        shard: bucket.shard,
        writer_id: config.compactor_writer_id.into_bytes(),
        writer_epoch: 0,
        writer_seq: 0,
    }
}

/// A rough uncompressed byte estimate for one span, for the part size cap.
/// Approximate on purpose (it only decides where parts split, never
/// correctness); mirrors the writer's own `row_estimate`.
fn estimate_record(r: &SpanRecord) -> u64 {
    let mut est: u64 = 48; // fixed-field overhead (ids, timestamps, status)
    est += r.name.len() as u64;
    if let Some(m) = &r.status_message {
        est += m.len() as u64;
    }
    for (k, v) in &r.attrs {
        est += k.len() as u64 + v.len() as u64 + 4;
    }
    est
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use bytes::Bytes;
    use proptest::prelude::*;
    use prost::Message;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::CompactionRecord;
    use ravel_rspan::{RspanConfig, RspanReader, RspanWriter, SpanQuery, StatusCode};
    use ravel_rspan::{SpanRecord, writer::ObjectIdentity};
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
        Bucket::new(tenant_hash(), Signal::Spans, SHARD, HOUR)
    }

    /// Past the seal margin for [`HOUR`] under default config.
    fn sealed_now_ns() -> i64 {
        (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
    }

    /// A synthetic span for trace `t`, span `s`, over `[start, end]`, with a
    /// distinct attr so attrs round-trip through the merge.
    fn span(t: u8, s: u8, start: i64, end: i64) -> SpanRecord {
        SpanRecord {
            trace_id: [t; 16],
            span_id: [s; 8],
            parent_span_id: if s == 0 { None } else { Some([s - 1; 8]) },
            name: format!("op-{s}"),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: StatusCode::Ok,
            status_message: Some(format!("msg-{s}")),
            attrs: vec![("svc".into(), format!("s{t}"))],
        }
    }

    /// Seed one L0 `.rspan` input (data object + commit record) exactly as a
    /// span ingest shard would, returning the object bytes for a direct
    /// differential decode.
    async fn seed(
        store: &dyn ObjectStoreBackend,
        writer_id: Uuid,
        seq: u64,
        records: &[SpanRecord],
    ) -> Bytes {
        let th = tenant_hash();
        let identity = ObjectIdentity {
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
            .expect("put data");

        let mut traces = std::collections::BTreeSet::new();
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
            .expect("put commit");
        bytes
    }

    /// Decode every span of an RSPAN object (full-range scan, so nothing is
    /// pruned), in the object's stored `(trace_id, start_ts)` order.
    fn decode_all(bytes: &[u8]) -> Vec<SpanRecord> {
        let cfg = RspanConfig::default();
        let reader = RspanReader::new(bytes, &cfg).expect("open");
        let (rows, _) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan");
        rows
    }

    /// An order-independent canonical key for a span: attrs folded to a sorted
    /// unique-key map (the writer's canonical form), everything else verbatim.
    type Canon = (
        [u8; 16],
        [u8; 8],
        Option<[u8; 8]>,
        String,
        i64,
        i64,
        u8,
        Option<String>,
        Vec<(String, String)>,
    );
    fn canon(r: &SpanRecord) -> Canon {
        let attrs: std::collections::BTreeMap<String, String> = r.attrs.iter().cloned().collect();
        (
            r.trace_id,
            r.span_id,
            r.parent_span_id,
            r.name.clone(),
            r.start_ts_ns,
            r.end_ts_ns,
            r.status_code.to_u8(),
            r.status_message.clone(),
            attrs.into_iter().collect(),
        )
    }

    fn canon_multiset(records: &[SpanRecord]) -> Vec<Canon> {
        let mut v: Vec<Canon> = records.iter().map(canon).collect();
        v.sort();
        v
    }

    /// Fetch the single compaction record in the bucket and every L1 part it
    /// references (parts in record order = ascending trace ranges).
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

    #[tokio::test]
    async fn compacts_two_l0_rspan_objects_into_one_l1_part_verbatim() {
        let store = MemoryStore::new();
        let a = vec![span(0, 0, 10, 20), span(1, 0, 5, 9)];
        let b = vec![span(0, 1, 15, 18), span(2, 0, 1, 2)];
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
        let ftr = footer::open(&parts[0]).expect("open l1");
        assert_eq!(ftr.level, 1);
        assert!(!ftr.input_set_hash.is_empty());
        assert_eq!(rec.parts[0].segment_format_version, OUTPUT_FORMAT_VERSION);
        assert_eq!(rec.parts[0].sample_count, 4);
        assert_eq!(rec.parts[0].series_count, 3, "three distinct traces");

        // The L1 records are the union of both inputs, stored in (trace_id,
        // start_ts) order.
        let l1 = decode_all(&parts[0]);
        let order: Vec<([u8; 16], i64)> = l1.iter().map(|r| (r.trace_id, r.start_ts_ns)).collect();
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(order, sorted, "L1 records in (trace, start_ts) order");

        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));

        // Decoding the inputs directly then concatenating gives the same set.
        let mut direct = decode_all(&a_bytes);
        direct.extend(decode_all(&b_bytes));
        assert_eq!(canon_multiset(&l1), canon_multiset(&direct));
    }

    #[tokio::test]
    async fn part_splitting_keeps_traces_whole_and_disjoint() {
        // Many distinct traces and a tiny part cap force splits on trace
        // boundaries: parts get disjoint, ascending trace_id ranges (a trace
        // never straddles two parts), and the union of records is preserved.
        let store = MemoryStore::new();
        let mk = |name_suffix: u8| -> Vec<SpanRecord> {
            (0..20u8)
                .map(|t| span(t, name_suffix, i64::from(t), i64::from(t) + 1))
                .collect()
        };
        let a = mk(0);
        let b = mk(1);
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig {
            max_l1_part_bytes: 256,
            ..CompactorConfig::default()
        };
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (rec, parts) = read_output(&store).await;
        assert!(parts.len() >= 2, "tiny cap must split into parts");

        // Part trace_id ranges are disjoint and ascending.
        let mut prev_last: Option<[u8; 16]> = None;
        for (i, p) in rec.parts.iter().enumerate() {
            let first: [u8; 16] = p.first_series_id.as_slice().try_into().unwrap();
            let last: [u8; 16] = p.last_series_id.as_slice().try_into().unwrap();
            assert!(first <= last);
            if let Some(pl) = prev_last {
                assert!(pl < first, "part trace ranges must be disjoint and ordered");
            }
            prev_last = Some(last);
            assert_eq!(footer::open(&parts[i]).expect("open").level, 1);
        }

        // Content complete across all parts.
        let mut l1: Vec<SpanRecord> = Vec::new();
        for p in &parts {
            l1.extend(decode_all(p));
        }
        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));
    }

    #[tokio::test]
    async fn corrupt_input_footer_fails_loud_at_load() {
        // A truncated/garbled input object must fail the compaction with a typed
        // error, never a panic and never a silent partial merge. Two inputs so
        // the compaction clears the min-inputs gate and reaches the codec; one
        // input's data object is then corrupted.
        let store = MemoryStore::new();
        let a = vec![span(0, 0, 1, 2)];
        let b = vec![span(1, 0, 3, 4)];
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;
        // Corrupt one data object's trailer so its footer no longer opens.
        let prefix = format!(
            "t/{}/{}/l0/{:04}/",
            tenant_hash().to_hex(),
            Signal::Spans.key_prefix(),
            SHARD
        );
        let data_key = list_all(&store, &prefix).await.unwrap()[0].key.clone();
        let mut bad = store
            .get(&data_key, GetRange::Full)
            .await
            .unwrap()
            .data
            .to_vec();
        let n = bad.len();
        bad[n - 1] ^= 0xff; // clobber the trailer magic
        store
            .put(&data_key, Bytes::from(bad), PutOptions::default())
            .await
            .unwrap();

        let clock = FixedClock::new(sealed_now_ns());
        let err = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect_err("corrupt input must fail the compaction");
        // Any typed error is acceptable; it must not panic or succeed.
        let _ = format!("{err}");
    }

    // --- keystone differential property test ---------------------------------

    #[derive(Debug, Clone)]
    struct SpanSpec {
        trace: u8,
        span: u8,
        start: i64,
        dur: i64,
        name: String,
        msg: Option<String>,
        attrs: Vec<(String, String)>,
    }

    fn attr_strategy() -> impl Strategy<Value = (String, String)> {
        let key = prop::sample::select(vec!["k0", "k1", "k2", "svc"]).prop_map(String::from);
        let val = prop::sample::select(vec!["p", "q", "r", "s"]).prop_map(String::from);
        (key, val)
    }

    fn spec_strategy() -> impl Strategy<Value = SpanSpec> {
        (
            0u8..4,
            any::<u8>(),
            0i64..40,
            0i64..1000,
            prop::sample::select(vec!["get", "put", "query", "flush"]),
            prop::option::of("[a-z ]{0,8}"),
            prop::collection::vec(attr_strategy(), 0..4),
        )
            .prop_map(|(trace, span, start, dur, name, msg, attrs)| SpanSpec {
                trace,
                span,
                start,
                dur,
                name: name.into(),
                msg,
                attrs,
            })
    }

    fn spec_to_record(s: &SpanSpec) -> SpanRecord {
        SpanRecord {
            trace_id: [s.trace; 16],
            span_id: [s.span; 8],
            parent_span_id: None,
            name: s.name.clone(),
            start_ts_ns: s.start,
            end_ts_ns: s.start.saturating_add(s.dur),
            status_code: StatusCode::Unset,
            status_message: s.msg.clone(),
            attrs: s.attrs.clone(),
        }
    }

    fn corpus_strategy() -> impl Strategy<Value = Vec<Vec<SpanSpec>>> {
        // 2..=5 inputs, each 1..=15 spans.
        prop::collection::vec(prop::collection::vec(spec_strategy(), 1..15), 2..6)
    }

    async fn differential_check(corpus: Vec<Vec<SpanSpec>>, max_l1_part_bytes: u64) {
        let store = MemoryStore::new();
        let mut all_input_records: Vec<SpanRecord> = Vec::new();
        for (i, input) in corpus.iter().enumerate() {
            let records: Vec<SpanRecord> = input.iter().map(spec_to_record).collect();
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
        let config = CompactorConfig {
            max_l1_part_bytes,
            ..CompactorConfig::default()
        };
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (rec, parts) = read_output(&store).await;
        assert_eq!(rec.level, 1);

        // Decode every L1 part (concatenated in part order) and compare its
        // record set to the inputs decoded directly, as an order-independent
        // canonical multiset (the correctness core).
        let mut l1: Vec<SpanRecord> = Vec::new();
        for p in &parts {
            l1.extend(decode_all(p));
        }
        assert_eq!(
            canon_multiset(&l1),
            canon_multiset(&all_input_records),
            "L1 decoded set must equal the input union"
        );

        // Within each part, records are in (trace_id, start_ts) order.
        for p in &parts {
            let recs = decode_all(p);
            let order: Vec<([u8; 16], i64)> =
                recs.iter().map(|r| (r.trace_id, r.start_ts_ns)).collect();
            let mut sorted = order.clone();
            sorted.sort();
            assert_eq!(order, sorted, "part records in (trace, start_ts) order");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

        /// The correctness core (ADR-0041): for a random corpus of span records
        /// split across N L0 objects, the full decoded record set is identical
        /// whether the N L0 inputs are decoded and concatenated or the single
        /// compacted L1 output is decoded. Default part cap: a single L1 part.
        #[test]
        fn differential_l0_union_equals_l1_output(corpus in corpus_strategy()) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(differential_check(corpus, CompactorConfig::default().max_l1_part_bytes));
        }

        /// The same union-equality property under a tiny part cap that forces
        /// the merge to split across multiple parts on trace boundaries, so the
        /// "concatenate all parts" side of the differential actually crosses
        /// part boundaries. Single-trace corpora stay one part (a trace never
        /// straddles) and still exercise the property.
        #[test]
        fn differential_holds_across_part_boundaries(corpus in corpus_strategy()) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(differential_check(corpus, 512));
        }
    }
}
