//! ADR-0066 force 2: re-encoding a below-target compaction part set into a
//! version 2 compaction record (`rewrite::reencode_compaction_parts`).
//!
//! Every bucket here is a real compaction: L0 inputs seeded through the
//! production writers and compacted by `compact_bucket`, so the record's parts
//! are genuine L1 objects. The parts are then made below-target the way the
//! L0 migration tests make an input below-target: the bytes stay at the
//! current version and the record's `segment_format_version` says one less,
//! because only one RSEG, RLOG and RSPAN version is writable today. The record
//! is overwritten in place to do that, which only a fixture may do.
//!
//! Served rows are read the way a resolver picks them: the bucket's compaction
//! records go through the shared selector
//! (`ravel_catalog::select_authoritative_compaction_records`) and the parts of
//! the records it does not exclude are decoded. Interleavings are driven by
//! `FaultStore` hold gates and `FixedClock`s the test moves; nothing sleeps.
//! Each test's doc comment names the line whose removal fails it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use ravel_catalog::select_authoritative_compaction_records;
use ravel_commit::{erasure, keys, record, signal};
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::rewrite::{ReencodeOutcome, reencode_compaction_parts};
use ravel_maintain::{
    Checkpoint, ClaimParticipant, Clock, CompactionOutcome, CompactorConfig, Coordination,
    ErasureRewriteOutcome, FixedClock, MaintainMemo, NoLeases, PendingErasureRequest,
    PublishOutcome, RequestLedger, compact_bucket, erasure_rewrite_bucket, read,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, GateHandle, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionPart, CompactionRecord, ErasurePredicateMatcher,
    ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_segment::{
    CompactionMetaV4, ExemplarInput, HistogramCounts, HistogramSample, HistogramSpan,
    HistogramValue, IngestBounds, ReaderLimits, ResetHint, RunInputV7, SampleProvenance,
    SegmentIdentity, SegmentWriter, SeriesEntryV4, SeriesInputV7, SeriesValues,
    V5_SPARSE_THRESHOLD, ValueKind, decode_catalog_v5, decode_exemplars_section,
    decode_run_histogram_pages, decode_run_pages_soa, encode_run_v4, open_from_full,
    plan_ranges_v4,
};
use ravel_types::{LabelSet, Sample, SeriesId, Signal};
use uuid::Uuid;

/// A lease long enough that no run here comes due for a renewal unless the
/// test moves the clock by a third of it.
const LEASE: Duration = Duration::from_secs(3);

/// Past the lease, on every clock a test moves.
const PAST_LEASE_NS: i64 = 4 * 1_000_000_000;

/// The key fragment of every L1 and rewrite part object.
const PART_KEYS: &str = "/l1/";

/// A [`ClaimSleeper`] that returns at once, so no jitter wait touches the real
/// timer.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// The writer switch on, no claims.
fn reencode_cfg() -> CompactorConfig {
    CompactorConfig {
        reencode_writer_enabled: true,
        ..CompactorConfig::default()
    }
}

/// A config for one pass that takes claims as process `process` on `clock`,
/// counting its requests into `ledger`.
fn claiming_cfg(process: u128, clock: &FixedClock, ledger: &RequestLedger) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_lease_duration: LEASE,
        claim_participant: Some(
            ClaimParticipant::new(
                Uuid::from_u128(process),
                Arc::new(clock.clone()) as Arc<dyn Clock>,
            )
            .with_sleeper(Arc::new(NoWait)),
        ),
        request_ledger: Some(ledger.clone()),
        reencode_writer_enabled: true,
        ..CompactorConfig::default()
    }
}

/// The segment format version the current build writes `signal`'s compaction
/// parts at.
fn current_version(signal: Signal) -> u32 {
    match signal {
        Signal::Metrics => ravel_maintain::build::OUTPUT_FORMAT_VERSION,
        Signal::Logs => ravel_maintain::rlog::OUTPUT_FORMAT_VERSION,
        Signal::Spans => ravel_maintain::rspan_codec::OUTPUT_FORMAT_VERSION,
        other => panic!("no compaction parts for {other:?}"),
    }
}

/// Two metrics inputs that both carry the series `keep` and `victim`, so
/// compaction merges each series' runs into one run carrying the per-sample
/// provenance column. An erasure test drops `victim`. The duplicate
/// `(keep, 2000)` timestamp is the case the provenance column exists for.
async fn seed_metrics(store: &dyn ObjectStoreBackend) -> ravel_maintain::Bucket {
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(1_000, 1.0), (2_000, 2.0)]),
                raw_series("victim", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![
                raw_series("keep", &[("k", "a")], &[(1_500, 1.5), (2_000, -0.0)]),
                raw_series("victim", &[("k", "b")], &[(3_000, 3.0)]),
            ],
        ),
    ] {
        seed_input(store, &spec).await;
    }
    bucket()
}

/// Seed `signal`'s two L0 inputs into `store` and compact them.
async fn seed_compacted(store: &dyn ObjectStoreBackend, signal: Signal) -> ravel_maintain::Bucket {
    let b = match signal {
        Signal::Metrics => seed_metrics(store).await,
        Signal::Logs => seed_rlog_two_inputs(store).await,
        Signal::Spans => seed_rspan_two_inputs(store).await,
        other => panic!("no fixture for {other:?}"),
    };
    let outcome = compact_bucket(
        store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &b,
    )
    .await
    .expect("compact");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the fixture compacts: {outcome:?}"
    );
    b
}

/// The bucket's compaction records, decoded, by key.
async fn compaction_records(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
) -> Vec<(String, CompactionRecord)> {
    let listing = read::list_bucket(store, b).await.expect("list");
    let mut out = Vec::new();
    for key in listing.compaction_record_keys {
        let rec = record::decode_compaction(&get_full(store, &key).await).expect("decode");
        keys::verify_compaction_record_key(&rec, &key).expect("record key verifies");
        out.push((key, rec));
    }
    out
}

/// Overwrite the bucket's one compaction record with every part recorded one
/// version below the current one. Returns the record and its key.
async fn stamp_parts_below_target(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
) -> (String, CompactionRecord) {
    let mut records = compaction_records(store, b).await;
    assert_eq!(records.len(), 1, "one compaction record to stamp");
    let (key, mut rec) = records.remove(0);
    for part in &mut rec.parts {
        part.segment_format_version = current_version(b.signal) - 1;
    }
    store
        .put(&key, record::encode_compaction(&rec), PutOptions::default())
        .await
        .expect("overwrite the fixture record");
    (key, rec)
}

/// The records the shared selector serves: every compaction record it does not
/// exclude.
async fn served(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
) -> Vec<(String, CompactionRecord)> {
    let records = compaction_records(store, b).await;
    let selection = select_authoritative_compaction_records(&records).expect("selector");
    let excluded: Vec<String> = selection.excluded().map(str::to_string).collect();
    records
        .into_iter()
        .filter(|(key, _)| !excluded.contains(key))
        .collect()
}

/// The single record the selector serves.
async fn served_one(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
) -> (String, CompactionRecord) {
    let mut served = served(store, b).await;
    assert_eq!(served.len(), 1, "the selector serves one record");
    served.remove(0)
}

/// One served RSEG sample: series id, its run's run-wide provenance
/// `(created_unix_ns, writer_epoch, writer_seq)`, its position in the run, its
/// timestamp, its value's bit pattern, and its per-sample provenance when the
/// run carries the column.
type RsegRow = (
    [u8; 16],
    (i64, u64, u64),
    usize,
    i64,
    u64,
    Option<(i64, u64, u64, u32)>,
);

/// Every sample of every part of `rec`, in the order the parts store them.
async fn rseg_rows(store: &dyn ObjectStoreBackend, rec: &CompactionRecord) -> Vec<RsegRow> {
    let limits = ReaderLimits::default();
    let mut out = Vec::new();
    for part in &rec.parts {
        let obj = get_full(store, &keys::reconstruct_l1_part_key(rec, part).unwrap()).await;
        let loc = open_from_full(&obj, limits).expect("open part");
        let entries = decode_catalog_v5(&loc.footer, &obj, limits).expect("catalog");
        let refs: Vec<&SeriesEntryV4> = entries.iter().collect();
        let mut planned = plan_ranges_v4(&loc.footer, &refs)
            .expect("plan")
            .into_iter();
        for entry in &entries {
            assert_eq!(entry.entry.value_kind, ValueKind::Scalar);
            for (i, run) in entry.runs.iter().enumerate() {
                let range = planned.next().expect("a range per run");
                let slice = |(off, len): (u64, u64)| &obj[off as usize..(off + len) as usize];
                let (mut scratch, mut ts, mut vals) = (Vec::new(), Vec::new(), Vec::new());
                decode_run_pages_soa(
                    &entry.entry.series_id,
                    run,
                    slice(range.ts_range),
                    slice(range.val_range),
                    limits,
                    &mut scratch,
                    &mut ts,
                    &mut vals,
                )
                .expect("decode run");
                let provenance = entry.per_sample_provenance.get(i).cloned().flatten();
                for (j, (t, v)) in ts.into_iter().zip(vals).enumerate() {
                    out.push((
                        entry.entry.series_id.0,
                        (run.created_unix_ns, run.writer_epoch, run.writer_seq),
                        j,
                        t,
                        v.to_bits(),
                        provenance.as_ref().map(|p| {
                            let p = p[j];
                            (
                                p.created_unix_ns,
                                p.writer_epoch,
                                p.writer_seq,
                                p.in_page_index,
                            )
                        }),
                    ));
                }
            }
        }
    }
    out.sort();
    out
}

/// Every record of every part of `rec`, decoded and printed with every field,
/// sorted. Logs and spans records carry no float the printing could merge.
async fn record_rows(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    rec: &CompactionRecord,
) -> Vec<String> {
    let mut out = Vec::new();
    for part in &rec.parts {
        let obj = get_full(store, &keys::reconstruct_l1_part_key(rec, part).unwrap()).await;
        match signal {
            Signal::Logs => {
                let reader =
                    ravel_logseg::RlogReader::new(&obj, &ravel_logseg::RlogConfig::default())
                        .expect("open rlog part");
                let (rows, _) = reader
                    .scan(&ravel_logseg::Predicate::And(vec![]))
                    .expect("scan rlog part");
                out.extend(rows.iter().map(|r| format!("{r:?}")));
            }
            Signal::Spans => {
                let reader =
                    ravel_rspan::RspanReader::new(&obj, &ravel_rspan::RspanConfig::default())
                        .expect("open rspan part");
                let (rows, _) = reader
                    .scan(&ravel_rspan::SpanQuery::ts_range(i64::MIN, i64::MAX))
                    .expect("scan rspan part");
                out.extend(rows.iter().map(|r| format!("{r:?}")));
            }
            other => panic!("no record decoder for {other:?}"),
        }
    }
    out.sort();
    out
}

/// Re-encode `b` on `store` with the switch on and no claims, expecting a
/// published version 2 record over `predecessor_key`.
async fn reencode_published(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
    predecessor_key: &str,
) {
    let outcome = reencode_compaction_parts(
        store,
        &FixedClock::new(sealed_now_ns() + 1_000),
        &reencode_cfg(),
        b,
    )
    .await
    .expect("re-encode");
    match outcome {
        ReencodeOutcome::Reencoded {
            superseded_record_key,
            parts,
            publish,
        } => {
            assert_eq!(superseded_record_key, predecessor_key);
            assert!(parts >= 1);
            assert_eq!(publish, PublishOutcome::Published);
        }
        other => panic!("expected Reencoded, got {other:?}"),
    }
}

/// The served record after a re-encode is a version 2 record whose parts are
/// all at the current version, and which is not the predecessor.
fn assert_current_successor(
    signal: Signal,
    predecessor_key: &str,
    (key, rec): &(String, CompactionRecord),
) {
    assert_ne!(key, predecessor_key, "the selector serves the successor");
    assert_eq!(rec.format_version, 2);
    assert!(!rec.parts.is_empty());
    for part in &rec.parts {
        assert_eq!(
            part.segment_format_version,
            current_version(signal),
            "every new part carries the current version"
        );
    }
}

/// Metrics differential: the rows the selector serves after the re-encode are
/// the predecessor's, sample for sample, with every run's run-wide provenance
/// and every per-sample provenance entry, and the new parts are current.
///
/// The fixture's predecessor carries a provenance column (asserted), so the
/// comparison covers it. Removing the `provenance:` line in
/// `reencode_rseg_part` (so every run is written with `None`) fails the
/// row equality: all six samples lose their provenance entries.
#[tokio::test]
async fn metrics_reencode_serves_the_predecessors_exact_rows() {
    let store = MemoryStore::new();
    let b = seed_compacted(&store, Signal::Metrics).await;
    let (pred_key, pred) = stamp_parts_below_target(&store, &b).await;
    let before = served_one(&store, &b).await;
    assert_eq!(before.0, pred_key);
    let want = rseg_rows(&store, &pred).await;
    assert!(
        want.iter().any(|row| row.5.is_some()),
        "the predecessor carries per-sample provenance, so the test covers it"
    );
    assert_eq!(want.len(), 6, "every seeded sample is served");

    reencode_published(&store, &b, &pred_key).await;

    let after = served_one(&store, &b).await;
    assert_current_successor(Signal::Metrics, &pred_key, &after);
    assert_eq!(after.1.parts.len(), pred.parts.len(), "part for part");
    assert_eq!(rseg_rows(&store, &after.1).await, want);
}

/// The level the hand-built metrics predecessor is recorded at. Not 1, so a
/// re-encode that stamps compaction's own level instead of the predecessor's
/// is visible.
const RICH_LEVEL: u32 = 2;

/// Footer ingest bounds of the hand-built metrics predecessor's two parts.
const RICH_INGEST: [(i64, i64); 2] = [(7_000, 99_000), (11_000, 55_000)];

fn provenance(created_unix_ns: i64, writer_seq: u64, in_page_index: u32) -> SampleProvenance {
    SampleProvenance {
        created_unix_ns,
        writer_epoch: 10,
        writer_seq,
        in_page_index,
    }
}

/// One run of `values` under the run-wide provenance `(created, 10, seq)`.
fn rich_run(
    id: SeriesId,
    created: i64,
    seq: u64,
    values: SeriesValues,
    provenance: Option<Vec<SampleProvenance>>,
) -> RunInputV7 {
    RunInputV7 {
        run: encode_run_v4(&id, created, 10, seq, &values).expect("encode run"),
        provenance,
    }
}

fn scalar(samples: &[(i64, f64)]) -> SeriesValues {
    SeriesValues::Scalar(
        samples
            .iter()
            .map(|&(ts_ns, value)| Sample { ts_ns, value })
            .collect(),
    )
}

fn histogram(ts_ns: i64, zero_count: u64, sum: f64) -> HistogramSample {
    HistogramSample {
        ts_ns,
        value: HistogramValue {
            scale: 0,
            zero_threshold: 0.0,
            sum: Some(sum),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 2,
            }],
            negative_spans: Vec::new(),
            counts: HistogramCounts::Int {
                zero_count,
                count: zero_count + 5,
                positive: vec![2, 3],
                negative: Vec::new(),
            },
            reset_hint: ResetHint::No,
        },
    }
}

fn exemplar(id: SeriesId, ts_ns: i64, value: f64, attrs: &[(&str, &str)]) -> ExemplarInput {
    ExemplarInput {
        series_id: id,
        ts_ns,
        value,
        trace_id: [ts_ns as u8; 16],
        span_id: [value.to_bits() as u8; 8],
        attrs: attrs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
    }
}

/// Write one part of the hand-built predecessor at `level`, PUT it, and return
/// its record entry, recorded one version below the current one.
async fn put_rich_part(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
    input_set_hash: &[u8; 32],
    part_index: u32,
    mut series: Vec<SeriesInputV7>,
    exemplars: Vec<ExemplarInput>,
) -> CompactionPart {
    series.sort_by_key(|s| s.series_id);
    let run_count = series.iter().map(|s| s.runs.len() as u64).sum();
    let first = series.first().expect("a series").series_id;
    let last = series.last().expect("a series").series_id;
    let (min_ingest_ts_ns, max_ingest_ts_ns) = RICH_INGEST[part_index as usize];
    let written = SegmentWriter::write_v7_with_provenance(
        series,
        SegmentIdentity {
            tenant_hash: b.tenant_hash.0,
            shard: b.shard,
            writer_id: "fixture".to_string(),
            writer_epoch: 0,
            writer_seq: 0,
        },
        IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        },
        CompactionMetaV4 {
            ingest_hour_bucket: b.ingest_hour_bucket,
            input_set_hash: *input_set_hash,
            part_index,
            level: RICH_LEVEL,
        },
        exemplars,
    )
    .expect("write part");
    let content_hash = written.summary.blake3;
    let key = keys::l1_part_key(
        &b.tenant_hash,
        b.signal,
        b.shard,
        b.ingest_hour_bucket,
        &hex::encode(&input_set_hash[..8]),
        part_index,
        &hex::encode(&content_hash[..8]),
    )
    .expect("part key");
    let mut part = CompactionPart {
        part_index,
        first_series_id: first.0.to_vec(),
        last_series_id: last.0.to_vec(),
        content_hash: content_hash.to_vec(),
        object_size: written.bytes.len() as u64,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        run_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: current_version(Signal::Metrics) - 1,
        declared_column_stats: Vec::new(),
    };
    ravel_commit::declared_stats::stamp_compaction_part(&mut part, &[]);
    store
        .put(&key, written.bytes, PutOptions::create_if_absent())
        .await
        .expect("put part");
    part
}

/// A metrics bucket whose one compaction record, at level [`RICH_LEVEL`], has
/// two below-target parts written directly through the RSEG writer, since the
/// L0 fixtures carry no histograms or exemplars:
///
/// - part 0 is dense: a scalar series with two runs, each with its own
///   run-wide provenance and a per-sample provenance column (one run holds a
///   NaN with a payload and a -0.0), a native histogram series with a
///   provenance column, and a scalar series with none; exemplars on the
///   first two.
/// - part 1 has [`V5_SPARSE_THRESHOLD`] series, so its catalog is the sparse
///   (chunked) form; every 512th series carries a provenance column, and two
///   series carry exemplars.
///
/// Each part's footer ingest bounds are its entry in [`RICH_INGEST`].
async fn seed_rich_metrics(store: &dyn ObjectStoreBackend) -> (String, CompactionRecord) {
    let b = bucket();
    let inputs = vec![
        CompactionInputIdentity {
            writer_id: Uuid::from_u128(1).to_string(),
            writer_epoch: 10,
            writer_seq: 1,
        },
        CompactionInputIdentity {
            writer_id: Uuid::from_u128(2).to_string(),
            writer_epoch: 10,
            writer_seq: 2,
        },
    ];
    let hash = erasure::compute_compaction_input_set_hash(&inputs);

    let (keep, keep_labels, _) = raw_series("rich_keep", &[("k", "a")], &[]);
    let (hist, hist_labels, _) = raw_series("rich_hist", &[("k", "h")], &[]);
    let (plain, plain_labels, _) = raw_series("rich_plain", &[("k", "p")], &[]);
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let dense = vec![
        SeriesInputV7 {
            series_id: keep,
            labels: keep_labels,
            runs: vec![
                rich_run(
                    keep,
                    100,
                    1,
                    scalar(&[(1_000, 1.0), (2_000, -0.0)]),
                    Some(vec![provenance(100, 1, 0), provenance(101, 1, 3)]),
                ),
                rich_run(
                    keep,
                    200,
                    2,
                    scalar(&[(1_500, nan), (2_000, 2.0)]),
                    Some(vec![provenance(200, 2, 1), provenance(202, 2, 0)]),
                ),
            ],
        },
        SeriesInputV7 {
            series_id: hist,
            labels: hist_labels,
            runs: vec![rich_run(
                hist,
                300,
                3,
                SeriesValues::Histogram(vec![histogram(1_000, 1, 10.5), histogram(2_000, 4, -0.0)]),
                Some(vec![provenance(300, 3, 2), provenance(301, 3, 0)]),
            )],
        },
        SeriesInputV7 {
            series_id: plain,
            labels: plain_labels,
            runs: vec![rich_run(plain, 400, 4, scalar(&[(3_000, 3.0)]), None)],
        },
    ];
    let dense_exemplars = vec![
        exemplar(keep, 1_000, 1.0, &[("trace", "a")]),
        exemplar(keep, 2_000, -0.0, &[]),
        exemplar(hist, 1_500, 0.5, &[("le", "1"), ("pod", "p")]),
    ];

    let mut sparse = Vec::new();
    let mut sparse_exemplars = Vec::new();
    for n in 0..V5_SPARSE_THRESHOLD {
        let (id, labels, _) = raw_series("rich_sparse", &[("i", &n.to_string())], &[]);
        let ts = 1_000 + n as i64;
        let column = (n % 512 == 0).then(|| vec![provenance(500 + n as i64, 5, n as u32)]);
        if n == 0 || n == V5_SPARSE_THRESHOLD - 1 {
            sparse_exemplars.push(exemplar(id, ts, n as f64, &[("n", &n.to_string())]));
        }
        sparse.push(SeriesInputV7 {
            series_id: id,
            labels,
            runs: vec![rich_run(id, 500, 5, scalar(&[(ts, n as f64)]), column)],
        });
    }

    let parts = vec![
        put_rich_part(store, &b, &hash, 0, dense, dense_exemplars).await,
        put_rich_part(store, &b, &hash, 1, sparse, sparse_exemplars).await,
    ];
    let rec = CompactionRecord {
        format_version: 1,
        tenant_hash: b.tenant_hash.0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        shard: b.shard,
        ingest_hour_bucket: b.ingest_hour_bucket,
        level: RICH_LEVEL,
        inputs,
        input_set_hash: hash.to_vec(),
        parts,
        created_unix_ns: sealed_now_ns() - 1_000,
        superseded_record_key: String::new(),
    };
    let key = keys::compaction_record_key_for(&rec).expect("record key");
    store
        .put(
            &key,
            record::encode_compaction(&rec),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put record");
    (key, rec)
}

/// One sample of an RSEG run: its timestamp, its value (a scalar's bit
/// pattern, or a histogram printed with every field), and its per-sample
/// provenance when the run carries the column.
type RichSample = (i64, String, Option<(i64, u64, u64, u32)>);

/// One run: its run-wide provenance and its samples, in stored order.
type RichRun = ((i64, u64, u64), Vec<RichSample>);

/// One exemplar, resolved to its series id: timestamp, value bits, trace id,
/// span id, attributes.
type RichExemplar = ([u8; 16], i64, u64, [u8; 16], [u8; 8], Vec<(String, String)>);

/// Everything item 7 says a re-encoded RSEG part keeps, read from one part.
#[derive(Debug, PartialEq)]
struct RsegPartContents {
    part_index: u32,
    level: u32,
    ingest: (i64, i64),
    sparse: bool,
    series: Vec<([u8; 16], LabelSet, ValueKind, Vec<RichRun>)>,
    exemplars: Vec<RichExemplar>,
}

/// The contents of every part of `rec`, in part-index order.
async fn rseg_part_contents(
    store: &dyn ObjectStoreBackend,
    rec: &CompactionRecord,
) -> Vec<RsegPartContents> {
    let limits = ReaderLimits::default();
    let mut out = Vec::new();
    for part in &rec.parts {
        let obj = get_full(store, &keys::reconstruct_l1_part_key(rec, part).unwrap()).await;
        let loc = open_from_full(&obj, limits).expect("open part");
        let footer = &loc.footer;
        let entries = decode_catalog_v5(footer, &obj, limits).expect("catalog");
        let refs: Vec<&SeriesEntryV4> = entries.iter().collect();
        let mut planned = plan_ranges_v4(footer, &refs).expect("plan").into_iter();
        let slice = |(off, len): (u64, u64)| &obj[off as usize..(off + len) as usize];
        let mut series = Vec::new();
        for entry in &entries {
            let id = entry.entry.series_id;
            let mut runs = Vec::new();
            for (i, run) in entry.runs.iter().enumerate() {
                let range = planned.next().expect("a range per run");
                let values: Vec<(i64, String)> = match entry.entry.value_kind {
                    ValueKind::Scalar => {
                        let (mut scratch, mut ts, mut vals) = (Vec::new(), Vec::new(), Vec::new());
                        decode_run_pages_soa(
                            &id,
                            run,
                            slice(range.ts_range),
                            slice(range.val_range),
                            limits,
                            &mut scratch,
                            &mut ts,
                            &mut vals,
                        )
                        .expect("decode scalar run");
                        ts.into_iter()
                            .zip(vals)
                            .map(|(t, v)| (t, format!("{:#018x}", v.to_bits())))
                            .collect()
                    }
                    ValueKind::Histogram => decode_run_histogram_pages(
                        &id,
                        run,
                        slice(range.ts_range),
                        slice(range.hist_range),
                        limits,
                    )
                    .expect("decode histogram run")
                    .into_iter()
                    .map(|h| (h.ts_ns, format!("{:?}", h.value)))
                    .collect(),
                };
                let column = entry.per_sample_provenance.get(i).cloned().flatten();
                let samples = values
                    .into_iter()
                    .enumerate()
                    .map(|(j, (t, v))| {
                        let p = column.as_ref().map(|c| {
                            let p = c[j];
                            (
                                p.created_unix_ns,
                                p.writer_epoch,
                                p.writer_seq,
                                p.in_page_index,
                            )
                        });
                        (t, v, p)
                    })
                    .collect();
                runs.push((
                    (run.created_unix_ns, run.writer_epoch, run.writer_seq),
                    samples,
                ));
            }
            series.push((
                id.0,
                entry.entry.labels.clone(),
                entry.entry.value_kind,
                runs,
            ));
        }
        let section = |kind: u32| footer.sections.iter().find(|s| s.kind == kind);
        let mut exemplars: Vec<RichExemplar> = match (section(10), section(1)) {
            (Some(ex), Some(dict)) => decode_exemplars_section(
                footer,
                slice((dict.offset, dict.len)),
                slice((ex.offset, ex.len)),
                limits,
            )
            .expect("decode exemplars")
            .into_iter()
            .map(|r| {
                (
                    entries[r.series_index as usize].entry.series_id.0,
                    r.ts_ns,
                    r.value.to_bits(),
                    r.trace_id,
                    r.span_id,
                    r.attrs,
                )
            })
            .collect(),
            _ => Vec::new(),
        };
        exemplars.sort();
        out.push(RsegPartContents {
            part_index: footer.part_index,
            level: footer.level,
            ingest: (footer.min_ingest_ts_ns, footer.max_ingest_ts_ns),
            sparse: section(8).is_some(),
            series,
            exemplars,
        });
    }
    out.sort_by_key(|p| p.part_index);
    out
}

/// Metrics exact contents per part (ADR-0066 force 2 amendment, item 7): each
/// new part holds what its predecessor part held, part for part: the series
/// and their labels and value kinds, every run's run-wide provenance, every
/// sample's timestamp and value bits (a NaN payload and -0.0 among them, and a
/// native histogram's every field), every per-sample provenance entry, the
/// exemplars, and the footer's ingest bounds and level. The record carries the
/// predecessor's level too. One predecessor part has a sparse catalog.
///
/// The fixture is asserted to hold each of these, so each comparison bites. In
/// `reencode_rseg_part`, each of these changes fails `assert_eq!(after_parts,
/// want)`, run one at a time:
/// - `rseg_part_exemplars(object, footer, &entries, limits)?` replaced with an
///   empty `Vec` (the exemplar copy): part 0 and part 1 lose their exemplars.
/// - `IngestBounds { min_ingest_ts_ns: footer.min_ingest_ts_ns, .. }` replaced
///   with zeroes (the ingest bounds copy): both parts' `ingest` differ.
/// - `provenance: entry.per_sample_provenance.get(i).cloned().flatten()`
///   replaced with `None` (the provenance copy): every column is lost.
/// - the `ValueKind::Histogram` arm replaced with an `Err` (the histogram
///   branch): the re-encode itself fails, so `reencode_published` panics.
///
/// In `publish_superseding_record`, passing `1` instead of `predecessor.level`
/// fails the record level assertion; in `reencode_rseg_parts`, passing `1`
/// instead of `predecessor.level` fails the footer level comparison.
#[tokio::test]
async fn metrics_reencode_keeps_each_parts_exact_contents() {
    let store = MemoryStore::new();
    let b = bucket();
    let (pred_key, pred) = seed_rich_metrics(&store).await;
    let want = rseg_part_contents(&store, &pred).await;
    assert_eq!(want.len(), 2);
    let (dense, sparse) = (&want[0], &want[1]);
    assert!(!dense.sparse && sparse.sparse, "part 1 alone is sparse");
    assert_eq!(sparse.series.len() as u64, V5_SPARSE_THRESHOLD);
    for (part, ingest) in want.iter().zip(RICH_INGEST) {
        assert_eq!(part.level, RICH_LEVEL);
        assert_eq!(part.ingest, ingest);
        assert!(
            !part.exemplars.is_empty(),
            "part {} has exemplars",
            part.part_index
        );
        assert!(
            part.series
                .iter()
                .flat_map(|s| &s.3)
                .flat_map(|r| &r.1)
                .any(|sample| sample.2.is_some()),
            "part {} carries per-sample provenance",
            part.part_index
        );
    }
    assert_eq!(dense.exemplars.len(), 3);
    assert_eq!(sparse.exemplars.len(), 2);
    assert!(
        dense
            .series
            .iter()
            .any(|s| s.2 == ValueKind::Histogram && !s.3.is_empty()),
        "part 0 holds a native histogram series"
    );

    reencode_published(&store, &b, &pred_key).await;

    let after = served_one(&store, &b).await;
    assert_current_successor(Signal::Metrics, &pred_key, &after);
    assert_eq!(after.1.level, pred.level, "the record keeps its level");
    let after_parts = rseg_part_contents(&store, &after.1).await;
    assert_eq!(after_parts.len(), want.len(), "part for part");
    for (new, old) in after_parts.iter().zip(&want) {
        assert_eq!(new, old, "part {} keeps its exact contents", old.part_index);
    }
}

/// Logs differential: the selector serves the predecessor's log records,
/// every field of each, from current-version parts.
///
/// No single line of the primitive decides RLOG record contents: the merge is
/// the compactor's. Replacing the keep-everything closure passed to
/// `rlog::merge_catalogs` with one that drops a record, and the
/// `conserve_exact()` gate with an always-true one, fails the row equality.
#[tokio::test]
async fn logs_reencode_serves_the_predecessors_exact_records() {
    differential_records(Signal::Logs).await;
}

/// Spans differential, as for logs. The same two-line change against the
/// closure passed to `rspan_codec::merge` and the conservation gate fails the
/// row equality.
#[tokio::test]
async fn spans_reencode_serves_the_predecessors_exact_records() {
    differential_records(Signal::Spans).await;
}

async fn differential_records(signal: Signal) {
    let store = MemoryStore::new();
    let b = seed_compacted(&store, signal).await;
    let (pred_key, pred) = stamp_parts_below_target(&store, &b).await;
    let want = record_rows(&store, signal, &pred).await;
    assert_eq!(want.len(), 4, "every seeded record is served");

    reencode_published(&store, &b, &pred_key).await;

    let after = served_one(&store, &b).await;
    assert_current_successor(signal, &pred_key, &after);
    assert_eq!(record_rows(&store, signal, &after.1).await, want);
}

/// The version 2 record copies the predecessor's inputs verbatim, names it in
/// `superseded_record_key`, carries the version 2 hash of both, and is stored
/// at the canonical key for that hash. The predecessor stays in place for the
/// sweep.
///
/// Removing `Some(predecessor_key)` from `publish_superseding_record`'s call
/// (passing `None`) writes a version 1 record with the version 2 hash: the
/// `format_version` and `superseded_record_key` assertions fail.
#[tokio::test]
async fn the_version_2_record_names_its_predecessor_under_its_canonical_key() {
    let store = MemoryStore::new();
    let b = seed_compacted(&store, Signal::Logs).await;
    let (pred_key, pred) = stamp_parts_below_target(&store, &b).await;

    reencode_published(&store, &b, &pred_key).await;

    let records = compaction_records(&store, &b).await;
    assert_eq!(records.len(), 2, "the predecessor and its successor");
    let (key, v2) = records
        .iter()
        .find(|(key, _)| *key != pred_key)
        .expect("a successor");
    assert_eq!(v2.format_version, 2);
    assert_eq!(v2.inputs, pred.inputs, "inputs copied verbatim");
    assert_eq!(v2.superseded_record_key, pred_key);
    assert_eq!(
        v2.input_set_hash,
        erasure::compute_superseding_compaction_input_set_hash(&pred.inputs, &pred_key).to_vec()
    );
    assert_eq!(
        *key,
        keys::compaction_record_key_for(v2).expect("canonical key"),
        "stored at the canonical key of its version 2 hash"
    );
}

/// A store over `seed`'s bucket that fails every PUT made through it, so a
/// refusal test proves it wrote nothing by the fault counter staying at 0.
/// Seeding goes through the inner store, under the fault layer.
async fn put_refusing_store(signal: Signal) -> (FaultStore<MemoryStore>, ravel_maintain::Bucket) {
    let store = FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(Rule::new(
            Op::Put,
            ScriptedFault::Permanent("this run must not write".to_string()),
        )),
    );
    let b = seed_compacted(store.inner(), signal).await;
    (store, b)
}

fn puts_attempted(store: &FaultStore<MemoryStore>) -> u64 {
    store.fault_count(Op::Put, FaultKind::Permanent)
}

async fn all_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    list_all(store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect()
}

/// With the writer switch off (the default) the primitive refuses with
/// `WriterDisabled` and makes no PUT.
///
/// Removing the `if !config.reencode_writer_enabled` return in
/// `reencode_compaction_parts` lets the run reach its first part PUT, which
/// the store fails, so the `expect` fails and the PUT count is 1.
#[tokio::test]
async fn the_switch_off_refuses_and_writes_nothing() {
    let (store, b) = put_refusing_store(Signal::Metrics).await;
    stamp_parts_below_target(store.inner(), &b).await;
    assert!(!CompactorConfig::default().reencode_writer_enabled);
    let before = all_keys(store.inner()).await;

    let outcome = reencode_compaction_parts(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &b,
    )
    .await
    .expect("a refusal is not an error");

    assert_eq!(outcome, ReencodeOutcome::WriterDisabled);
    assert_eq!(puts_attempted(&store), 0, "no PUT was made");
    assert_eq!(all_keys(store.inner()).await, before);
}

/// A bucket whose overlap component holds two compaction records is refused
/// with `ContestedOverlap` and nothing is written (ADR-0066 force 2 item 4).
/// The second record names one of the first record's two inputs, so the two
/// share a component, and the selector picks one winner between them.
///
/// Removing the `selection.largest_component() > 1` return lets the winner be
/// re-encoded. The winner is the stamped first record, whose parts exist (the
/// second record's parts would be keyed under its own hash, where nothing
/// is stored), so the run reads them and reaches its first part PUT, which
/// the store fails with `Store(Permanent("this run must not write"))`, so the
/// `expect` fails.
#[tokio::test]
async fn a_contested_overlap_component_is_refused_and_writes_nothing() {
    let (store, b) = put_refusing_store(Signal::Metrics).await;
    let (_, first) = stamp_parts_below_target(store.inner(), &b).await;
    let inputs = vec![first.inputs[0].clone()];
    let second = CompactionRecord {
        input_set_hash: erasure::compute_compaction_input_set_hash(&inputs).to_vec(),
        inputs,
        ..first.clone()
    };
    store
        .inner()
        .put(
            &keys::compaction_record_key_for(&second).expect("key"),
            record::encode_compaction(&second),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put the second record");
    let before = all_keys(store.inner()).await;

    let outcome = reencode_compaction_parts(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &reencode_cfg(),
        &b,
    )
    .await
    .expect("a refusal is not an error");

    assert_eq!(
        outcome,
        ReencodeOutcome::ContestedOverlap {
            largest_component: 2
        }
    );
    assert_eq!(puts_attempted(&store), 0, "no PUT was made");
    assert_eq!(all_keys(store.inner()).await, before);
}

/// A bucket holding a rewrite record is refused with `RewritePresent` and
/// nothing is written (ADR-1331).
///
/// Removing the `!listing.rewrite_record_keys.is_empty()` return lets the
/// compaction record be re-encoded: the run reaches its first part PUT, which
/// the store fails, so the `expect` fails.
#[tokio::test]
async fn a_bucket_with_a_rewrite_record_is_refused_and_writes_nothing() {
    let (store, b) = put_refusing_store(Signal::Metrics).await;
    let (pred_key, pred) = stamp_parts_below_target(store.inner(), &b).await;
    let request_id = Uuid::from_u128(0x2093).to_string();
    let rewrite = RewriteRecord {
        format_version: 1,
        tenant_hash: b.tenant_hash.0.to_vec(),
        signal: signal::to_proto(b.signal) as i32,
        shard: b.shard,
        ingest_hour_bucket: b.ingest_hour_bucket,
        inputs: Vec::new(),
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &[],
            Some(&pred_key),
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        parts: pred.parts.clone(),
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: pred.created_unix_ns + 1_000,
        superseded_record_key: pred_key.clone(),
    };
    store
        .inner()
        .put(
            &keys::rewrite_record_key_for(&rewrite).expect("key"),
            erasure::encode_rewrite(&rewrite),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put the rewrite record");
    let before = all_keys(store.inner()).await;

    let outcome = reencode_compaction_parts(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &reencode_cfg(),
        &b,
    )
    .await
    .expect("a refusal is not an error");

    assert_eq!(outcome, ReencodeOutcome::RewritePresent);
    assert_eq!(puts_attempted(&store), 0, "no PUT was made");
    assert_eq!(all_keys(store.inner()).await, before);
}

/// A bucket whose one record is already at the current version is a no-op.
///
/// Removing the `UpToDate` return (the `segment_format_version <
/// target_version` test) re-encodes the current parts anyway: the run reaches
/// its first part PUT, which the store fails, so the `expect` fails.
#[tokio::test]
async fn a_bucket_at_target_is_a_no_op() {
    for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
        let (store, b) = put_refusing_store(signal).await;
        let before = all_keys(store.inner()).await;

        let outcome = reencode_compaction_parts(
            &store,
            &FixedClock::new(sealed_now_ns()),
            &reencode_cfg(),
            &b,
        )
        .await
        .expect("a no-op is not an error");

        assert_eq!(outcome, ReencodeOutcome::UpToDate, "{signal:?}");
        assert_eq!(puts_attempted(&store), 0, "no PUT was made");
        assert_eq!(all_keys(store.inner()).await, before);
    }
}

fn ms(ns: i64) -> u64 {
    u64::try_from(ns / 1_000_000).expect("positive instant")
}

/// A fault store whose own clock (the base claim expiry is judged on) reads
/// `now_ns`, holding a compacted metrics bucket with below-target parts.
async fn fenced_store(now_ns: i64) -> (Arc<FaultStore<MemoryStore>>, String) {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    store.inner().set_clock_ms(ms(now_ns));
    let b = seed_compacted(store.inner(), Signal::Metrics).await;
    let (pred_key, _) = stamp_parts_below_target(store.inner(), &b).await;
    (store, pred_key)
}

/// A windowless erasure request for every series named `victim`.
fn pending() -> Vec<PendingErasureRequest> {
    let request_id = Uuid::from_u128(0x2093);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 0,
        predicate: vec![ErasurePredicateMatcher {
            key: "__name__".to_string(),
            value: "victim".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request,
    }]
}

async fn erasure(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
) -> ErasureRewriteOutcome {
    let mut memo = MaintainMemo::with_default_interval();
    erasure_rewrite_bucket(
        store,
        clock,
        config,
        &NoLeases,
        &bucket(),
        &pending(),
        &mut memo,
    )
    .await
    .expect("erasure rewrite")
}

/// Wait until `gate` parks exactly one call, check it is a PUT on a part key,
/// and return its id.
async fn parked_part_put(gate: &GateHandle) -> u64 {
    gate.wait_until_held(1).await;
    let held = gate.held_details();
    assert_eq!(held.len(), 1, "the gate parked exactly one call: {held:?}");
    let (id, op, key) = &held[0];
    assert_eq!(*op, Op::Put, "the parked call is a PUT: {key}");
    assert!(key.contains(PART_KEYS), "the parked call is a part: {key}");
    *id
}

/// `(compaction records, rewrite records)` in the bucket.
async fn record_sets(store: &dyn ObjectStoreBackend) -> (usize, usize) {
    let listing = read::list_bucket(store, &bucket()).await.expect("list");
    (
        listing.compaction_record_keys.len(),
        listing.rewrite_record_keys.len(),
    )
}

fn is_published(outcome: &ErasureRewriteOutcome) -> bool {
    matches!(
        outcome,
        ErasureRewriteOutcome::Rewritten {
            publish: PublishOutcome::Published,
            ..
        }
    )
}

/// The claim fence: re-encode R claims the bucket and is parked at its first
/// part PUT. The lease expires; erasure E, a second process, steals the claim
/// and publishes its rewrite record. R resumes, its part-boundary checkpoint
/// renews, the renewal finds the claim stolen, and R cancels there with
/// nothing published.
///
/// Removing the `claim_bucket(store, config, bucket, "reencode")` call (so R
/// runs unclaimed) lets E take the claim without a steal and R's checkpoints
/// pass; R is then stopped by its re-list instead, `RewritePresent`, which
/// fails the `Cancelled` assertion. The re-list test below pins the re-list.
#[tokio::test]
async fn an_erasure_rewrite_that_steals_the_claim_cancels_the_reencode() {
    let now_ns = sealed_now_ns();
    let (store, _) = fenced_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let r_ledger = RequestLedger::new();
    let r_config = claiming_cfg(1, &clock, &r_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = claiming_cfg(2, &clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let r = async {
        reencode_compaction_parts(store.as_ref(), &clock, &r_config, &bucket())
            .await
            .expect("re-encode")
    };
    let e = async {
        let id = parked_part_put(&gate).await;
        clock.set(now_ns + PAST_LEASE_NS);
        store.inner().set_clock_ms(ms(now_ns + PAST_LEASE_NS));
        let outcome = erasure(store.as_ref(), &clock, &e_config).await;
        assert!(gate.release(id), "the parked part PUT was released");
        outcome
    };
    let (r_outcome, e_outcome) = tokio::join!(r, e);

    assert!(
        is_published(&e_outcome),
        "E steals the expired claim and publishes: {e_outcome:?}"
    );
    assert_eq!(
        r_outcome,
        ReencodeOutcome::Cancelled {
            at: Checkpoint::PartBoundary
        }
    );
    assert_eq!(
        record_sets(store.as_ref()).await,
        (1, 1),
        "no version 2 record beside the predecessor and E's rewrite record"
    );
    assert_eq!(r_ledger.report().publish.requests, 0, "R PUT no record");
    assert_eq!(
        r_ledger.report().list.requests,
        1,
        "R stopped at its checkpoint, before its re-list"
    );
}

/// The re-list fence, with no claims anywhere: re-encode R is parked at its
/// first part PUT while erasure E publishes its rewrite record. R resumes,
/// builds, and its pre-publish re-list finds the rewrite record, so it
/// publishes nothing and reports `RewritePresent`.
///
/// Removing the `relist_changed` check in `reencode_and_publish` lets R
/// publish a version 2 record beside E's rewrite record (`Reencoded`), which
/// fails the `RewritePresent` assertion and the record count.
#[tokio::test]
async fn an_erasure_rewrite_that_publishes_mid_reencode_stops_it_at_the_relist() {
    let now_ns = sealed_now_ns();
    let (store, _) = fenced_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let r_ledger = RequestLedger::new();
    let r_config = CompactorConfig {
        request_ledger: Some(r_ledger.clone()),
        ..reencode_cfg()
    };
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let r = async {
        reencode_compaction_parts(store.as_ref(), &clock, &r_config, &bucket())
            .await
            .expect("re-encode")
    };
    let e = async {
        let id = parked_part_put(&gate).await;
        let outcome = erasure(store.as_ref(), &clock, &CompactorConfig::default()).await;
        assert!(gate.release(id), "the parked part PUT was released");
        outcome
    };
    let (r_outcome, e_outcome) = tokio::join!(r, e);

    assert!(is_published(&e_outcome), "E publishes: {e_outcome:?}");
    assert_eq!(r_outcome, ReencodeOutcome::RewritePresent);
    assert_eq!(record_sets(store.as_ref()).await, (1, 1));
    assert_eq!(r_ledger.report().publish.requests, 0, "R PUT no record");
    assert_eq!(
        r_ledger.report().list.requests,
        2,
        "the plan and the re-list"
    );
}
