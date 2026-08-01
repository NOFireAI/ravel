//! Shared test fixtures for the compactor's integration tests.
//!
//! Seeds a `MemoryStore` (or a `FaultStore` wrapping one) with L0 RSEG v5
//! objects and their commit records, exactly as a post-#179 ingest tier
//! would, then reads compaction output back down to samples for equivalence
//! checks.
//!
//! Bootstrapping note: sample encoding goes through the production
//! raw-samples adapter (`SegmentWriter::write`, #179), and the framed
//! pages are then sliced into single-run v5 inputs so each seeded
//! object carries this fixture's explicit per-run provenance.
#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use std::collections::BTreeMap;

use bytes::Bytes;
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_segment::{
    CompactionMetaV4, ReaderLimits, RunInputV4, RunValuePageV4, SegmentIdentity, SegmentWriter,
    SeriesEntryV4, SeriesInput, SeriesInputV4, ValueKind, decode_catalog_v5, decode_run_pages_soa,
    open_from_full, plan_ranges_v4,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

use ravel_maintain::Bucket;

const LABEL_DICT: u32 = 1;
const TS_PAGES: u32 = 3;
const VAL_PAGES: u32 = 4;
const SERIES_IDS: u32 = 5;
const SERIES_META: u32 = 6;

pub const NS_PER_HOUR: i64 = 3_600_000_000_000;
pub const TENANT: &str = "acme";
pub const SHARD: u32 = 7;
pub const HOUR: u32 = 495_000;

pub fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

pub fn bucket() -> Bucket {
    bucket_at(HOUR)
}

pub fn bucket_at(hour: u32) -> Bucket {
    Bucket::new(tenant_hash(), Signal::Metrics, SHARD, hour)
}

/// A time comfortably past the seal margin for [`HOUR`] with default config
/// (bucket end + 1h flush + 5m skew), so `is_sealed` is true.
pub fn sealed_now_ns() -> i64 {
    (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
}

pub fn labels(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::new(
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: (*n).to_string(),
                value: (*v).to_string(),
            })
            .collect(),
    )
    .expect("valid labels")
}

pub fn series_id(name: &str, pairs: &[(&str, &str)]) -> SeriesId {
    SeriesId::compute(&TenantId::new(TENANT), name, &labels(pairs)).expect("series id")
}

/// One raw series: id, its labels (including `__name__`), and samples.
pub type RawSeries = (SeriesId, LabelSet, Vec<Sample>);

pub fn raw_series(name: &str, pairs: &[(&str, &str)], samples: &[(i64, f64)]) -> RawSeries {
    let mut lp: Vec<(&str, &str)> = vec![("__name__", name)];
    lp.extend_from_slice(pairs);
    let ls = labels(&lp);
    let id = SeriesId::compute(&TenantId::new(TENANT), name, &ls).expect("id");
    let samples = samples
        .iter()
        .map(|(ts, v)| Sample {
            ts_ns: *ts,
            value: *v,
        })
        .collect();
    (id, ls, samples)
}

/// One L0 flush's identity and payload.
pub struct InputSpec {
    pub writer_id: Uuid,
    pub epoch: u64,
    pub seq: u64,
    pub hour: u32,
    pub created_unix_ns: i64,
    pub series: Vec<RawSeries>,
}

impl InputSpec {
    pub fn new(writer_id: Uuid, epoch: u64, seq: u64, series: Vec<RawSeries>) -> Self {
        Self::new_at(HOUR, writer_id, epoch, seq, series)
    }

    pub fn new_at(
        hour: u32,
        writer_id: Uuid,
        epoch: u64,
        seq: u64,
        series: Vec<RawSeries>,
    ) -> Self {
        // created within the bucket hour so the commit record validates
        // (ingest_hour_bucket <= created's hour).
        let created_unix_ns = i64::from(hour) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        InputSpec {
            writer_id,
            epoch,
            seq,
            hour,
            created_unix_ns,
            series,
        }
    }
}

/// Encode a raw batch into a single-run RSEG v5 L0 object (bootstrap note
/// above). Every run's provenance is set to `provenance`, matching the commit
/// record; the compactor overrides run provenance from the commit record
/// anyway (plan §3.3 step 3).
pub fn build_v5_l0(
    raw: &[RawSeries],
    hour: u32,
    provenance: (i64, u64, u64),
) -> ravel_segment::WrittenSegment {
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash().0,
        shard: SHARD,
        writer_id: Uuid::nil().to_string(),
        writer_epoch: provenance.1,
        writer_seq: provenance.2,
    };
    let bounds = ravel_segment::IngestBounds {
        min_ingest_ts_ns: provenance.0,
        max_ingest_ts_ns: provenance.0,
    };
    // Bootstrap: encode through the production raw adapter (one run per
    // series, v5), then slice the framed pages so this fixture keeps
    // explicit control of per-run provenance.
    let raw_series: Vec<SeriesInput> = raw
        .iter()
        .map(|(id, ls, samples)| SeriesInput {
            series_id: *id,
            labels: ls.clone(),
            samples: samples.clone(),
        })
        .collect();
    let l0 = SegmentWriter::write(
        raw_series,
        SegmentIdentity {
            tenant_hash: tenant_hash().0,
            shard: SHARD,
            writer_id: Uuid::nil().to_string(),
            writer_epoch: provenance.1,
            writer_seq: provenance.2,
        },
        ravel_segment::IngestBounds {
            min_ingest_ts_ns: provenance.0,
            max_ingest_ts_ns: provenance.0,
        },
    )
    .expect("write v5 bootstrap");
    let obj = l0.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = open_from_full(obj, limits).expect("open v5");
    let footer = &loc.footer;
    let entries = decode_catalog_v5(footer, obj, limits).expect("decode v5 catalog");
    let ts_sec = footer.sections.iter().find(|s| s.kind == TS_PAGES).unwrap();
    let val_sec = footer
        .sections
        .iter()
        .find(|s| s.kind == VAL_PAGES)
        .unwrap();
    let inputs: Vec<SeriesInputV4> = entries
        .iter()
        .map(|e| {
            // The raw adapter frames exactly one run per series.
            let run = &e.runs[0];
            let (o, l) = run.ts_page;
            let a = (ts_sec.offset + o) as usize;
            let ts_page = obj[a..a + l as usize].to_vec();
            let (o, l) = run.val_page;
            let a = (val_sec.offset + o) as usize;
            let val_page = obj[a..a + l as usize].to_vec();
            SeriesInputV4 {
                series_id: e.entry.series_id,
                labels: e.entry.labels.clone(),
                runs: vec![RunInputV4 {
                    created_unix_ns: provenance.0,
                    writer_epoch: provenance.1,
                    writer_seq: provenance.2,
                    min_ts_ns: run.min_ts_ns,
                    max_ts_ns: run.max_ts_ns,
                    sample_count: run.sample_count,
                    ts_page,
                    value_page: RunValuePageV4::Scalar(val_page),
                }],
            }
        })
        .collect();
    let meta = CompactionMetaV4 {
        ingest_hour_bucket: hour,
        input_set_hash: [0u8; 32],
        part_index: 0,
        level: 0,
    };
    SegmentWriter::write_v5(inputs, identity, bounds, meta).expect("write v5 l0")
}

/// Seed one L0 input (data object + commit record) into `store`. Returns the
/// commit key.
pub async fn seed_input(store: &dyn ObjectStoreBackend, spec: &InputSpec) -> String {
    let th = tenant_hash();
    let written = build_v5_l0(
        &spec.series,
        spec.hour,
        (spec.created_unix_ns, spec.epoch, spec.seq),
    );
    let content_hash = written.summary.blake3;
    let data_key = keys::data_key(
        &th,
        Signal::Metrics,
        SHARD,
        spec.writer_id,
        spec.epoch,
        spec.seq,
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
        writer_id: spec.writer_id,
        writer_epoch: spec.epoch,
        writer_seq: spec.seq,
        object_size: written.bytes.len() as u64,
        content_hash,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: spec.created_unix_ns,
        max_ingest_ts_ns: spec.created_unix_ns,
        segment_format_version: 5,
        created_unix_ns: spec.created_unix_ns,
        ingest_hour_bucket: spec.hour,
    })
    .expect("build commit record");
    let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
    store
        .put(&commit_key, record::encode(&rec), PutOptions::default())
        .await
        .expect("put commit record");
    commit_key
}

/// Decode every scalar sample of every series in an RSEG v5 object, keyed by
/// series id, samples concatenated across runs in catalog order.
pub fn read_scalar_samples(obj: &[u8]) -> BTreeMap<[u8; 16], Vec<(i64, u64)>> {
    let limits = ReaderLimits::default();
    let loc = open_from_full(obj, limits).expect("open v5");
    let footer = &loc.footer;
    let entries = decode_catalog_v5(footer, obj, limits).expect("decode v5 catalog");
    let refs: Vec<&SeriesEntryV4> = entries.iter().collect();
    let planned = plan_ranges_v4(footer, &refs).expect("plan ranges");

    let mut out: BTreeMap<[u8; 16], Vec<(i64, u64)>> = BTreeMap::new();
    let mut scratch = Vec::new();
    let mut idx = 0usize;
    for entry in &entries {
        assert_eq!(entry.entry.value_kind, ValueKind::Scalar);
        let samples = out.entry(entry.entry.series_id.0).or_default();
        for run in &entry.runs {
            let pr = &planned[idx];
            idx += 1;
            let ts = slice(obj, pr.ts_range);
            let val = slice(obj, pr.val_range);
            let mut timestamps = Vec::new();
            let mut values = Vec::new();
            decode_run_pages_soa(
                &entry.entry.series_id,
                run,
                ts,
                val,
                limits,
                &mut scratch,
                &mut timestamps,
                &mut values,
            )
            .expect("decode run pages");
            for (t, v) in timestamps.iter().zip(values.iter()) {
                // Bit pattern so -0.0 and NaN payloads are significant.
                samples.push((*t, v.to_bits()));
            }
        }
    }
    out
}

/// The union of every input spec's samples, keyed by series id, sorted by
/// timestamp (the compaction-invariant view: query dedup happens downstream).
pub fn expected_samples(specs: &[InputSpec]) -> BTreeMap<[u8; 16], Vec<(i64, u64)>> {
    let mut out: BTreeMap<[u8; 16], Vec<(i64, u64)>> = BTreeMap::new();
    for spec in specs {
        for (id, _ls, samples) in &spec.series {
            let e = out.entry(id.0).or_default();
            for s in samples {
                e.push((s.ts_ns, s.value.to_bits()));
            }
        }
    }
    for v in out.values_mut() {
        v.sort();
    }
    out
}

fn section_bytes<'a>(bytes: &'a [u8], footer: &ravel_segment::Footer, kind: u32) -> &'a [u8] {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    &bytes[s.offset as usize..(s.offset + s.len) as usize]
}

fn slice(bytes: &[u8], range: (u64, u64)) -> &[u8] {
    &bytes[range.0 as usize..(range.0 + range.1) as usize]
}

// --- RLOG (logs) fixtures --------------------------------------------------
//
// The sweeper is signal-generic, so its crash-matrix suite runs once over an
// RSEG (metrics) fixture and once over an RLOG (logs) fixture. These helpers
// seed real, compactable L0 `.rlog` objects exactly as the ingest log shard
// would, so `compact_bucket` produces a genuine compaction record and L1
// parts for the logs bucket (mirroring `rlog.rs`'s own test seeding).

/// The logs bucket for the shared `SHARD`/`HOUR` (Signal::Logs).
pub fn logs_bucket() -> Bucket {
    Bucket::new(tenant_hash(), Signal::Logs, SHARD, HOUR)
}

/// Synthetic log stream `n`: its id and canonical resource+scope blob, the id
/// being the true hash of the blob (real stream identity, never a placeholder).
pub fn log_stream_ident(n: u32) -> (ravel_types::logstream::LogStreamId, Vec<u8>) {
    use ravel_types::logstream::{AttrValue, log_stream_id};
    let res = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc{n}")),
    )];
    let id = log_stream_id(&res, "scope", "1", &[]);
    let blob = ravel_logseg::stream_attrs_bytes(&res, "scope", "1", &[]);
    (id, blob)
}

pub fn log_record(stream_n: u32, ts: i64, body: &str) -> ravel_logseg::LogRecord {
    let (stream_id, stream_attrs) = log_stream_ident(stream_n);
    ravel_logseg::LogRecord {
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
        attrs: Vec::new(),
    }
}

/// Seed one L0 `.rlog` input (data object + commit record) for the logs
/// bucket, exactly as the ingest log shard would. Returns the commit key.
pub async fn seed_rlog_input(
    store: &dyn ObjectStoreBackend,
    writer_id: Uuid,
    epoch: u64,
    seq: u64,
    records: &[ravel_logseg::LogRecord],
) -> String {
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{RlogConfig, RlogWriter};
    let th = tenant_hash();
    let identity = ObjectIdentity {
        tenant_hash: th.0,
        shard: SHARD,
        writer_id: writer_id.into_bytes(),
        writer_epoch: epoch,
        writer_seq: seq,
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity);
    for r in records {
        w.push(r.clone()).expect("push log record");
    }
    let bytes = Bytes::from(w.finish().expect("finish rlog L0"));
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let data_key = keys::data_key(
        &th,
        Signal::Logs,
        SHARD,
        writer_id,
        epoch,
        seq,
        &content_hash,
    )
    .expect("data key");
    store
        .put(&data_key, bytes.clone(), PutOptions::default())
        .await
        .expect("put rlog data");

    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut streams = std::collections::BTreeSet::new();
    for r in records {
        min_ts = min_ts.min(r.ts_ns);
        max_ts = max_ts.max(r.ts_ns);
        streams.insert(r.stream_id);
    }
    let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
    let rec = record::build(NewCommitRecord {
        tenant_hash: th,
        signal: Signal::Logs,
        shard: SHARD,
        writer_id,
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: records.len() as u64,
        series_count: streams.len() as u64,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: created,
        max_ingest_ts_ns: created,
        segment_format_version: 2,
        created_unix_ns: created,
        ingest_hour_bucket: HOUR,
    })
    .expect("build logs commit record");
    let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
    store
        .put(&commit_key, record::encode(&rec), PutOptions::default())
        .await
        .expect("put logs commit record");
    commit_key
}

/// Seed two compactable L0 `.rlog` inputs into the logs bucket, the RLOG
/// analogue of [`seed`]/[`two_inputs`] for metrics. Returns the logs bucket.
pub async fn seed_rlog_two_inputs(store: &dyn ObjectStoreBackend) -> Bucket {
    seed_rlog_input(
        store,
        Uuid::from_u128(1),
        10,
        1,
        &[log_record(0, 10, "alpha"), log_record(1, 20, "bravo")],
    )
    .await;
    seed_rlog_input(
        store,
        Uuid::from_u128(2),
        10,
        2,
        &[log_record(0, 15, "charlie"), log_record(2, 5, "delta")],
    )
    .await;
    logs_bucket()
}

// --- RSPAN (spans) fixtures ------------------------------------------------
//
// The sweeper is signal-generic, so its crash-matrix suite runs once over an
// RSEG (metrics) fixture, once over an RLOG (logs) fixture, and once over an
// RSPAN (spans) fixture. These helpers seed real, compactable L0 `.rspan`
// objects exactly as a span ingest shard would, so `compact_bucket` produces a
// genuine compaction record and L1 parts for the spans bucket (mirroring
// `rspan_codec.rs`'s own test seeding).

/// The spans bucket for the shared `SHARD`/`HOUR` (Signal::Spans).
pub fn spans_bucket() -> Bucket {
    Bucket::new(tenant_hash(), Signal::Spans, SHARD, HOUR)
}

/// A synthetic span for trace `t`, span `s`, over `[start, end]`.
pub fn span_record(t: u8, s: u8, start: i64, end: i64) -> ravel_rspan::SpanRecord {
    ravel_rspan::SpanRecord {
        trace_id: [t; 16],
        span_id: [s; 8],
        parent_span_id: if s == 0 { None } else { Some([s - 1; 8]) },
        name: format!("op-{s}"),
        start_ts_ns: start,
        end_ts_ns: end,
        status_code: ravel_rspan::StatusCode::Ok,
        status_message: Some(format!("msg-{s}")),
        attrs: vec![("svc".into(), format!("s{t}"))],
    }
}

/// Seed one L0 `.rspan` input (data object + commit record) for the spans
/// bucket, exactly as a span ingest shard would. Returns the commit key.
pub async fn seed_rspan_input(
    store: &dyn ObjectStoreBackend,
    writer_id: Uuid,
    epoch: u64,
    seq: u64,
    records: &[ravel_rspan::SpanRecord],
) -> String {
    use ravel_rspan::writer::ObjectIdentity;
    use ravel_rspan::{RspanConfig, RspanWriter};
    let th = tenant_hash();
    let identity = ObjectIdentity {
        tenant_hash: th.0,
        shard: SHARD,
        writer_id: writer_id.into_bytes(),
        writer_epoch: epoch,
        writer_seq: seq,
    };
    let mut w = RspanWriter::new(RspanConfig::default(), identity);
    for r in records {
        w.push(r.clone());
    }
    let bytes = Bytes::from(w.finish().expect("finish rspan L0"));
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let data_key = keys::data_key(
        &th,
        Signal::Spans,
        SHARD,
        writer_id,
        epoch,
        seq,
        &content_hash,
    )
    .expect("data key");
    store
        .put(&data_key, bytes.clone(), PutOptions::default())
        .await
        .expect("put rspan data");

    let mut min_start = i64::MAX;
    let mut max_end = i64::MIN;
    let mut traces = std::collections::BTreeSet::new();
    for r in records {
        min_start = min_start.min(r.start_ts_ns);
        max_end = max_end.max(r.end_ts_ns);
        traces.insert(r.trace_id);
    }
    let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
    let rec = record::build(NewCommitRecord {
        tenant_hash: th,
        signal: Signal::Spans,
        shard: SHARD,
        writer_id,
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: records.len() as u64,
        series_count: traces.len() as u64,
        min_event_ts_ns: min_start,
        max_event_ts_ns: max_end,
        min_ingest_ts_ns: created,
        max_ingest_ts_ns: created,
        // RSPAN trailer version 1 (ADR-0041).
        segment_format_version: 1,
        created_unix_ns: created,
        ingest_hour_bucket: HOUR,
    })
    .expect("build spans commit record");
    let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
    store
        .put(&commit_key, record::encode(&rec), PutOptions::default())
        .await
        .expect("put spans commit record");
    commit_key
}

/// Seed two compactable L0 `.rspan` inputs into the spans bucket, the RSPAN
/// analogue of [`seed_rlog_two_inputs`]. Returns the spans bucket.
pub async fn seed_rspan_two_inputs(store: &dyn ObjectStoreBackend) -> Bucket {
    seed_rspan_input(
        store,
        Uuid::from_u128(1),
        10,
        1,
        &[span_record(0, 0, 10, 20), span_record(1, 0, 5, 9)],
    )
    .await;
    seed_rspan_input(
        store,
        Uuid::from_u128(2),
        10,
        2,
        &[span_record(0, 1, 15, 18), span_record(2, 0, 1, 2)],
    )
    .await;
    spans_bucket()
}

/// Convenience: full-object GET as `Bytes`.
pub async fn get_full(store: &dyn ObjectStoreBackend, key: &str) -> Bytes {
    use ravel_object_store::GetRange;
    store
        .get(key, GetRange::Full)
        .await
        .expect("get object")
        .data
}

/// Find and decode the single compaction record in a bucket, verifying its
/// key reconstructs (ADR-0010 §7). Panics if there is not exactly one.
pub async fn fetch_compaction_record(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
) -> ravel_proto::commit::v1::CompactionRecord {
    use prost::Message;
    use ravel_object_store::list_all;
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    let metas = list_all(store, &prefix).await.expect("list bucket");
    let mut record_keys: Vec<String> = metas
        .into_iter()
        .map(|m| m.key)
        .filter(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .collect();
    record_keys.sort();
    assert_eq!(
        record_keys.len(),
        1,
        "expected exactly one compaction record"
    );
    let bytes = get_full(store, &record_keys[0]).await;
    let record =
        ravel_proto::commit::v1::CompactionRecord::decode(bytes.as_ref()).expect("decode record");
    keys::verify_compaction_record_key(&record, &record_keys[0]).expect("record key verifies");
    record
}

/// Read every part of a compaction record back down to per-series samples,
/// merged across parts.
pub async fn read_record_samples(
    store: &dyn ObjectStoreBackend,
    record: &ravel_proto::commit::v1::CompactionRecord,
) -> BTreeMap<[u8; 16], Vec<(i64, u64)>> {
    let mut out: BTreeMap<[u8; 16], Vec<(i64, u64)>> = BTreeMap::new();
    for part in &record.parts {
        let key = keys::reconstruct_l1_part_key(record, part).expect("part key");
        let bytes = get_full(store, &key).await;
        for (id, samples) in read_scalar_samples(&bytes) {
            out.entry(id).or_default().extend(samples);
        }
    }
    for v in out.values_mut() {
        v.sort();
    }
    out
}
