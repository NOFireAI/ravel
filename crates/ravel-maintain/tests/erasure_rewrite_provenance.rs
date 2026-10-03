//! Dedup winners across a metrics erasure rewrite (issue #2409).
//!
//! ADR-0064 decision 3 requires the rewrite to preserve every non-erased record
//! exactly. For a run-merged L1 run (ADR-0092 decision 1) "exactly" includes
//! each sample's dedup key (docs/catalog-and-mvcc.md "Cross-segment duplicate
//! samples"): the samples are resolved against each other and against any later
//! commit by that key, not by the bytes of the value page alone.
//!
//! The fixture holds duplicate timestamps from four writers whose provenance
//! differs in each component of the key, a NaN with a payload, a -0.0, a
//! duplicate inside one write, a series written by one writer only (copied
//! verbatim by compaction), and a native histogram series. It is resolved by
//! [`resolve`], which mirrors the query fetcher's provenance rule, in three
//! states: the raw L0 inputs, the live record before the erasure, and the
//! rewrite record that erased an unrelated series or part of a run. Then later
//! commits from two more writers duplicate contested timestamps, one winning
//! and one losing each, and the winner must be the same whether those commits
//! are resolved against the record the rewrite replaced or the rewrite. They
//! stand for a duplicate written in another ingest-hour bucket, since a sealed
//! bucket admits no commit; resolution does not look at the bucket.
//!
//! Every rewrite is also checked structurally ([`assert_rewrite_structure`]):
//! each output run's triple, per-sample provenance column and samples must be
//! its source run's with the erased samples removed, bit for bit.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use common::*;
use prost::Message;
use ravel_commit::keys::{self, BucketEntry};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::signal;
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, ErasureRewriteOutcome, FixedClock, MaintainMemo, NoLeases,
    PendingErasureRequest, compact_bucket, erasure_rewrite_bucket,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{
    CompactionRecord, ErasurePredicateMatcher, ErasureRequest, RewriteRecord,
};
use ravel_segment::{
    CompactionMetaV4, HistogramCounts, HistogramSample, HistogramValue, IngestBounds, ReaderLimits,
    ResetHint, SegmentIdentity, SegmentWriter, SeriesInputV4, SeriesValues, VERSION_V7, ValueKind,
    decode_catalog_v5, decode_run_histogram_pages, decode_run_pages_soa, encode_run_v4,
    open_from_full, plan_ranges_v4,
};
use ravel_types::{LabelSet, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const MS: i64 = 1_000_000;

/// A quiet NaN whose payload is significant (values compare by bit pattern).
const NAN_PAYLOAD: u64 = 0x7ff8_0000_0000_beef;

/// One L0 writer's commit-record identity, which is its dedup priority prefix.
#[derive(Clone, Copy)]
struct Writer {
    id: u128,
    epoch: u64,
    seq: u64,
    created_unix_ns: i64,
}

impl Writer {
    fn triple(self) -> (i64, u64, u64) {
        (self.created_unix_ns, self.epoch, self.seq)
    }
}

/// Highest priority among the originals at a created tie with `W4`: the epoch
/// decides against `W1`.
const W3: Writer = Writer {
    id: 3,
    epoch: 30,
    seq: 2,
    created_unix_ns: hour_start() + 5 * MS,
};
const W1: Writer = Writer {
    id: 1,
    epoch: 20,
    seq: 5,
    created_unix_ns: hour_start() + 5 * MS,
};
/// Ties `W1` on created and epoch; the seq decides, and `W4` loses.
const W4: Writer = Writer {
    id: 4,
    epoch: 20,
    seq: 4,
    created_unix_ns: hour_start() + 5 * MS,
};
/// The oldest flush, carrying the largest epoch-independent seq: loses every
/// duplicate on created alone, though it is seeded last.
const W2: Writer = Writer {
    id: 2,
    epoch: 10,
    seq: 9,
    created_unix_ns: hour_start() + 3 * MS,
};
/// A later commit that wins: newer than every original flush, older than the
/// compaction record and the rewrite record, both stamped by the maintenance
/// clock at `sealed_now_ns()`.
const W5: Writer = Writer {
    id: 5,
    epoch: 1,
    seq: 1,
    created_unix_ns: hour_start() + 30 * 60_000 * MS,
};
/// A later commit that loses: older than W1, W3 and W4, newer than W2. A
/// merged run whose samples all took W2's run-wide triple (the minimum the
/// compactor stamps on it) would lose to it.
const W6: Writer = Writer {
    id: 6,
    epoch: 99,
    seq: 99,
    created_unix_ns: hour_start() + 4 * MS,
};

const fn hour_start() -> i64 {
    HOUR as i64 * NS_PER_HOUR
}

/// What one (series, timestamp) resolves to, by bit pattern. A histogram is
/// identified by its integer count, distinct for every sample written here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Resolved {
    Scalar(u64),
    Histogram(u64),
}

type Priority = (i64, u64, u64, u32);
type Key = ([u8; 16], i64);
/// The samples one erasure removes, by (series, timestamp).
type Erased = fn(&Key) -> bool;
type Resolution = BTreeMap<Key, Resolved>;
/// One flush's series, each with its labels and samples.
type Batch = Vec<(SeriesId, LabelSet, SeriesValues)>;

/// How the query fetcher stamps a segment's samples: an L0 object by its commit
/// record, an L1 or rewrite part by each run's own catalog provenance or, when
/// the run carries one, its per-sample provenance column.
#[derive(Clone, Copy)]
enum Level {
    L0(Writer),
    L1,
}

/// One run as its segment's catalog stores it: the run-wide triple, the
/// optional per-sample provenance column, and the decoded samples in order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RunView {
    triple: (i64, u64, u64),
    column: Option<Vec<Priority>>,
    samples: Vec<(i64, Resolved)>,
}

impl RunView {
    /// Every sample's dedup key in an L1 or rewrite part: its column entry when
    /// the run carries one, else the run's triple and the sample's position.
    fn keys(&self) -> Vec<Priority> {
        match &self.column {
            Some(column) => column.clone(),
            None => positional(self.triple, self.samples.len()),
        }
    }
}

fn positional((created, epoch, seq): (i64, u64, u64), n: usize) -> Vec<Priority> {
    (0..n)
        .map(|pos| {
            (
                created,
                epoch,
                seq,
                u32::try_from(pos).expect("in-page index"),
            )
        })
        .collect()
}

fn series(name: &str, k: &str) -> (SeriesId, LabelSet) {
    let ls = labels(&[("__name__", name), ("k", k)]);
    let id = SeriesId::compute(&TenantId::new(TENANT), name, &ls).expect("series id");
    (id, ls)
}

fn scalar(name: &str, k: &str, samples: &[(i64, u64)]) -> (SeriesId, LabelSet, SeriesValues) {
    let (id, ls) = series(name, k);
    let samples = samples
        .iter()
        .map(|&(ts_ns, bits)| Sample {
            ts_ns,
            value: f64::from_bits(bits),
        })
        .collect();
    (id, ls, SeriesValues::Scalar(samples))
}

fn histogram(name: &str, k: &str, samples: &[(i64, u64)]) -> (SeriesId, LabelSet, SeriesValues) {
    let (id, ls) = series(name, k);
    let samples = samples
        .iter()
        .map(|&(ts_ns, count)| HistogramSample {
            ts_ns,
            value: HistogramValue {
                scale: 0,
                zero_threshold: 0.0,
                sum: Some(count as f64 * 0.5),
                custom_values: None,
                positive_spans: Vec::new(),
                negative_spans: Vec::new(),
                counts: HistogramCounts::Int {
                    zero_count: count,
                    count,
                    positive: Vec::new(),
                    negative: Vec::new(),
                },
                reset_hint: ResetHint::Unknown,
            },
        })
        .collect();
    (id, ls, SeriesValues::Histogram(samples))
}

fn bits(v: f64) -> u64 {
    v.to_bits()
}

/// Seed one single-run-per-series L0 object and its commit record, with every
/// run stamped by `w`'s commit identity. Returns the data object key.
async fn seed_l0(store: &dyn ObjectStoreBackend, w: Writer, batch: Batch) -> String {
    let th = tenant_hash();
    let mut inputs: Vec<SeriesInputV4> = batch
        .into_iter()
        .map(|(id, ls, values)| SeriesInputV4 {
            series_id: id,
            labels: ls,
            runs: vec![
                encode_run_v4(&id, w.created_unix_ns, w.epoch, w.seq, &values).expect("encode"),
            ],
        })
        .collect();
    inputs.sort_by_key(|s| s.series_id.0);
    let written = SegmentWriter::write_v5(
        inputs,
        SegmentIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: Uuid::from_u128(w.id).to_string(),
            writer_epoch: w.epoch,
            writer_seq: w.seq,
        },
        IngestBounds {
            min_ingest_ts_ns: w.created_unix_ns,
            max_ingest_ts_ns: w.created_unix_ns,
        },
        CompactionMetaV4 {
            ingest_hour_bucket: HOUR,
            input_set_hash: [0u8; 32],
            part_index: 0,
            level: 0,
        },
    )
    .expect("write L0");
    let content_hash = written.summary.blake3;
    let writer_id = Uuid::from_u128(w.id);
    let data_key = keys::data_key(
        &th,
        Signal::Metrics,
        SHARD,
        writer_id,
        w.epoch,
        w.seq,
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
        writer_epoch: w.epoch,
        writer_seq: w.seq,
        object_size: written.bytes.len() as u64,
        content_hash,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: w.created_unix_ns,
        max_ingest_ts_ns: w.created_unix_ns,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: w.created_unix_ns,
        ingest_hour_bucket: HOUR,
    })
    .expect("build commit record");
    let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
    store
        .put(&commit_key, record::encode(&rec), PutOptions::default())
        .await
        .expect("put commit record");
    data_key
}

/// The original four flushes. Seeded W3, W1, W4, W2, so the last write seeded
/// is the lowest priority, and the winner at each duplicate is set by a
/// different component of the dedup key.
fn original_flushes() -> Vec<(Writer, Batch)> {
    vec![
        (
            W3,
            vec![
                scalar("alpha", "a", &[(1_000, bits(3.0))]),
                histogram("hist", "h", &[(2_000, 40)]),
            ],
        ),
        (
            W1,
            vec![
                // A duplicate inside one write: the in-page index decides.
                scalar(
                    "alpha",
                    "a",
                    &[
                        (1_000, bits(1.0)),
                        (2_000, bits(-0.0)),
                        (4_000, bits(7.0)),
                        (4_000, bits(8.0)),
                    ],
                ),
                scalar("beta", "b", &[(1_000, NAN_PAYLOAD)]),
                histogram("hist", "h", &[(1_000, 10)]),
                // Only W1 writes `solo`: compaction copies its run verbatim.
                scalar("solo", "s", &[(1_000, bits(4.0)), (2_000, bits(-0.0))]),
                scalar("victim", "v", &[(1_000, bits(9.0))]),
            ],
        ),
        (
            W4,
            vec![scalar(
                "beta",
                "b",
                &[(1_000, bits(5.0)), (2_000, bits(0.0))],
            )],
        ),
        (
            W2,
            vec![
                scalar(
                    "alpha",
                    "a",
                    &[(1_000, bits(2.0)), (2_000, NAN_PAYLOAD), (3_000, bits(0.5))],
                ),
                scalar("beta", "b", &[(2_000, bits(-0.0))]),
                histogram("hist", "h", &[(1_000, 20), (2_000, 30)]),
                scalar("victim", "v", &[(1_000, bits(10.0))]),
            ],
        ),
    ]
}

/// The later commits: W5 adds one more duplicate on a merged scalar run, a
/// verbatim single-writer run, and a merged histogram run, and wins each; W6
/// duplicates a timestamp on each of those runs and on `beta`, and loses each.
fn later_flushes() -> Vec<(Writer, Batch)> {
    let mut later = vec![(
        W5,
        vec![
            scalar("alpha", "a", &[(1_000, bits(11.0))]),
            scalar("solo", "s", &[(1_000, bits(12.0))]),
            histogram("hist", "h", &[(1_000, 50)]),
        ],
    )];
    later.extend(later_losers());
    later
}

/// W6 alone: the later commit that loses every duplicate it adds, at
/// timestamps 2000 only, so it stays clear of an erasure of 1000.
fn later_losers() -> Vec<(Writer, Batch)> {
    vec![(
        W6,
        vec![
            scalar("alpha", "a", &[(2_000, bits(13.0))]),
            scalar("beta", "b", &[(2_000, bits(14.0))]),
            scalar("solo", "s", &[(2_000, bits(15.0))]),
            histogram("hist", "h", &[(2_000, 60)]),
        ],
    )]
}

/// Every run of every series in one segment, in catalog order.
fn decode_runs(obj: &Bytes) -> Vec<([u8; 16], RunView)> {
    let limits = ReaderLimits::default();
    let loc = open_from_full(obj, limits).expect("open segment");
    let entries = decode_catalog_v5(&loc.footer, obj, limits).expect("decode catalog");
    let refs: Vec<_> = entries.iter().collect();
    let mut planned = plan_ranges_v4(&loc.footer, &refs)
        .expect("plan ranges")
        .into_iter();
    let mut out = Vec::new();
    for entry in &entries {
        let id = entry.entry.series_id;
        for (run_index, run) in entry.runs.iter().enumerate() {
            let range = planned.next().expect("one range per run");
            let ts_page = slice(obj, range.ts_range);
            let samples: Vec<(i64, Resolved)> = match entry.entry.value_kind {
                ValueKind::Scalar => {
                    let (mut ts, mut vals, mut scratch) = (Vec::new(), Vec::new(), Vec::new());
                    decode_run_pages_soa(
                        &id,
                        run,
                        ts_page,
                        slice(obj, range.val_range),
                        limits,
                        &mut scratch,
                        &mut ts,
                        &mut vals,
                    )
                    .expect("decode scalar run");
                    ts.into_iter()
                        .zip(vals)
                        .map(|(t, v)| (t, Resolved::Scalar(v.to_bits())))
                        .collect()
                }
                ValueKind::Histogram => decode_run_histogram_pages(
                    &id,
                    run,
                    ts_page,
                    slice(obj, range.hist_range),
                    limits,
                )
                .expect("decode histogram run")
                .into_iter()
                .map(|s| {
                    let HistogramCounts::Int { count, .. } = s.value.counts else {
                        panic!("fixture histograms carry integer counts");
                    };
                    (s.ts_ns, Resolved::Histogram(count))
                })
                .collect(),
            };
            let column = entry
                .per_sample_provenance
                .get(run_index)
                .and_then(Option::as_ref)
                .map(|col| {
                    col.iter()
                        .map(|p| {
                            (
                                p.created_unix_ns,
                                p.writer_epoch,
                                p.writer_seq,
                                p.in_page_index,
                            )
                        })
                        .collect::<Vec<_>>()
                });
            if let Some(column) = &column {
                assert_eq!(column.len(), samples.len(), "one column entry per sample");
            }
            out.push((
                id.0,
                RunView {
                    triple: (run.created_unix_ns, run.writer_epoch, run.writer_seq),
                    column,
                    samples,
                },
            ));
        }
    }
    assert!(planned.next().is_none(), "one range per run");
    out
}

/// Resolve every (series, timestamp) over `segments` under the ADR-0010 §5
/// order, greatest `(created_unix_ns, writer_epoch, writer_seq, in_page_index)`
/// winning, with the provenance source the query fetcher uses per level
/// (`ravel-query` `fetcher.rs`, `SegmentLevel::L0` and `SegmentLevel::L1`).
fn resolve(segments: &[(Level, Bytes)]) -> Resolution {
    let mut best: BTreeMap<Key, (Priority, Resolved)> = BTreeMap::new();
    for (level, obj) in segments {
        for (id, run) in decode_runs(obj) {
            let keys = match level {
                Level::L0(w) => positional(w.triple(), run.samples.len()),
                Level::L1 => run.keys(),
            };
            for (&(ts, value), priority) in run.samples.iter().zip(keys) {
                let candidate = (priority, value);
                let slot = best.entry((id, ts)).or_insert(candidate);
                if candidate > *slot {
                    *slot = candidate;
                }
            }
        }
    }
    best.into_iter().map(|(k, (_, v))| (k, v)).collect()
}

/// Every run of every series across `segments`, each series' runs sorted so
/// two sets compare independently of run order. An L0 run takes its commit
/// record's triple, as the rewrite's raw L0 path does.
fn runs_by_series(segments: &[(Level, Bytes)]) -> BTreeMap<[u8; 16], Vec<RunView>> {
    let mut out: BTreeMap<[u8; 16], Vec<RunView>> = BTreeMap::new();
    for (level, obj) in segments {
        for (id, mut run) in decode_runs(obj) {
            if let Level::L0(w) = level {
                run.triple = w.triple();
            }
            out.entry(id).or_default().push(run);
        }
    }
    for runs in out.values_mut() {
        runs.sort();
    }
    out
}

/// What the rewrite must make of `source` when it erases the samples `erased`
/// names: each run keeps its surviving samples in order, a run with a column
/// keeps those samples' entries, a run without one that loses samples gets a
/// column of its triple and each survivor's original position, and a run with
/// a column takes the minimum of its survivors' keys as its triple, the way
/// the compactor stamps a merged run. A run with no survivor disappears.
fn expected_rewrite(
    source: &BTreeMap<[u8; 16], Vec<RunView>>,
    erased: impl Fn(&Key) -> bool,
) -> BTreeMap<[u8; 16], Vec<RunView>> {
    let mut out: BTreeMap<[u8; 16], Vec<RunView>> = BTreeMap::new();
    for (id, runs) in source {
        for run in runs {
            let (samples, keys): (Vec<_>, Vec<_>) = run
                .samples
                .iter()
                .zip(run.keys())
                .filter(|((ts, _), _)| !erased(&(*id, *ts)))
                .map(|(s, k)| (*s, k))
                .unzip();
            if samples.is_empty() {
                continue;
            }
            let lost = samples.len() < run.samples.len();
            let column = (run.column.is_some() || lost).then_some(keys);
            let triple = match &column {
                Some(column) => column
                    .iter()
                    .map(|&(c, e, s, _)| (c, e, s))
                    .min()
                    .expect("a surviving run has a sample"),
                None => run.triple,
            };
            out.entry(*id).or_default().push(RunView {
                triple,
                column,
                samples,
            });
        }
    }
    for runs in out.values_mut() {
        runs.sort();
    }
    out
}

/// The rewritten part holds exactly [`expected_rewrite`] of its source, triple,
/// column and sample bits included.
fn assert_rewrite_structure(
    source: &BTreeMap<[u8; 16], Vec<RunView>>,
    rewritten: &BTreeMap<[u8; 16], Vec<RunView>>,
    erased: impl Fn(&Key) -> bool,
) {
    let expected = expected_rewrite(source, erased);
    assert_eq!(
        rewritten.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "the rewrite keeps exactly the series with a surviving sample"
    );
    for (id, runs) in &expected {
        assert_eq!(
            &rewritten[id],
            runs,
            "series {}: every rewritten run is its source run less the erased samples",
            hex::encode(id)
        );
    }
}

fn slice(bytes: &[u8], (offset, len): (u64, u64)) -> &[u8] {
    &bytes[offset as usize..(offset + len) as usize]
}

/// An erasure request for every series named `name`, over the half-open
/// `[window_start_ns, window_end_ns)` (zero on a side leaves it open).
fn erasure_request(name: &str, window_start_ns: i64, window_end_ns: i64) -> PendingErasureRequest {
    erasure_request_with_id(0x2409, name, window_start_ns, window_end_ns)
}

fn erasure_request_with_id(
    id_seed: u128,
    name: &str,
    window_start_ns: i64,
    window_end_ns: i64,
) -> PendingErasureRequest {
    let request_id = Uuid::from_u128(id_seed);
    PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request: ErasureRequest {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            request_id: request_id.to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: "__name__".to_string(),
                value: name.to_string(),
            }],
            window_start_ns,
            window_end_ns,
            reason: String::new(),
        },
    }
}

/// Every record key of one kind in the bucket.
async fn record_keys(store: &dyn ObjectStoreBackend, rewrite: bool) -> BTreeSet<String> {
    let b = bucket();
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .unwrap();
    list_all(store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .filter(|k| match keys::partition_bucket_entry(k) {
            Ok(BucketEntry::RewriteRecord(_)) => rewrite,
            Ok(BucketEntry::CompactionRecord(_)) => !rewrite,
            _ => false,
        })
        .collect()
}

async fn l1_segments(store: &dyn ObjectStoreBackend, keys: &[String]) -> Vec<(Level, Bytes)> {
    let mut out = Vec::new();
    for key in keys {
        out.push((Level::L1, get_full(store, key).await));
    }
    out
}

/// Apply `request` to the bucket and return the part keys of the rewrite
/// record it publishes.
async fn erase(
    store: &MemoryStore,
    clock: &FixedClock,
    request: &PendingErasureRequest,
) -> Vec<String> {
    let before = record_keys(store, true).await;
    let mut memo = MaintainMemo::with_default_interval();
    let outcome = erasure_rewrite_bucket(
        store,
        clock,
        &CompactorConfig::default(),
        &NoLeases,
        &bucket(),
        std::slice::from_ref(request),
        &mut memo,
    )
    .await
    .expect("erasure rewrite");
    assert!(
        matches!(outcome, ErasureRewriteOutcome::Rewritten { .. }),
        "the rewrite publishes, got {outcome:?}"
    );
    let published: Vec<String> = record_keys(store, true)
        .await
        .difference(&before)
        .cloned()
        .collect();
    assert_eq!(published.len(), 1, "the rewrite publishes one record");
    let rec = RewriteRecord::decode(get_full(store, &published[0]).await.as_ref()).unwrap();
    rec.parts
        .iter()
        .map(|p| keys::reconstruct_rewrite_part_key(&rec, p).unwrap())
        .collect()
}

fn id(name: &str, k: &str) -> [u8; 16] {
    series(name, k).0.0
}

/// The fixture resolves the way its comments say, so a later equality cannot
/// pass over a fixture that never put a contested duplicate on disk.
fn assert_original_winners(r: &Resolution) {
    let scalar_at = |name, k, ts| r[&(id(name, k), ts)];
    assert_eq!(scalar_at("alpha", "a", 1_000), Resolved::Scalar(bits(3.0)));
    assert_eq!(scalar_at("alpha", "a", 2_000), Resolved::Scalar(bits(-0.0)));
    assert_eq!(scalar_at("alpha", "a", 3_000), Resolved::Scalar(bits(0.5)));
    assert_eq!(scalar_at("alpha", "a", 4_000), Resolved::Scalar(bits(8.0)));
    assert_eq!(scalar_at("beta", "b", 1_000), Resolved::Scalar(NAN_PAYLOAD));
    assert_eq!(scalar_at("beta", "b", 2_000), Resolved::Scalar(bits(0.0)));
    assert_eq!(scalar_at("solo", "s", 1_000), Resolved::Scalar(bits(4.0)));
    assert_eq!(scalar_at("solo", "s", 2_000), Resolved::Scalar(bits(-0.0)));
    assert_eq!(scalar_at("hist", "h", 1_000), Resolved::Histogram(10));
    assert_eq!(scalar_at("hist", "h", 2_000), Resolved::Histogram(40));
    assert_eq!(scalar_at("victim", "v", 1_000), Resolved::Scalar(bits(9.0)));
}

/// The later commits resolve the way [`later_flushes`] says.
fn assert_later_winners(r: &Resolution) {
    let at = |name, k, ts| r[&(id(name, k), ts)];
    assert_eq!(at("alpha", "a", 1_000), Resolved::Scalar(bits(11.0)));
    assert_eq!(at("solo", "s", 1_000), Resolved::Scalar(bits(12.0)));
    assert_eq!(at("hist", "h", 1_000), Resolved::Histogram(50));
    assert_later_losers(r);
}

/// W6 loses every duplicate [`later_losers`] adds.
fn assert_later_losers(r: &Resolution) {
    let at = |name, k, ts| r[&(id(name, k), ts)];
    assert_eq!(at("alpha", "a", 2_000), Resolved::Scalar(bits(-0.0)));
    assert_eq!(at("beta", "b", 2_000), Resolved::Scalar(bits(0.0)));
    assert_eq!(at("solo", "s", 2_000), Resolved::Scalar(bits(-0.0)));
    assert_eq!(at("hist", "h", 2_000), Resolved::Histogram(40));
}

/// `r` with every (series, ts) `erased` names removed.
fn without(r: &Resolution, erased: impl Fn(&Key) -> bool) -> Resolution {
    r.iter()
        .filter(|(key, _)| !erased(key))
        .map(|(k, v)| (*k, *v))
        .collect()
}

/// One bucket's history up to the erasure under test: the original flushes,
/// whether they are compacted, and the erasures already applied (each with the
/// samples it removes). `later` lands after the erasure under test; it must not
/// write a (series, ts) any erasure names.
struct Case {
    originals: Vec<(Writer, Batch)>,
    compact: bool,
    prior: Vec<(PendingErasureRequest, Erased)>,
    later: Vec<(Writer, Batch)>,
}

impl Case {
    fn new(originals: Vec<(Writer, Batch)>, compact: bool, later: Vec<(Writer, Batch)>) -> Self {
        Case {
            originals,
            compact,
            prior: Vec::new(),
            later,
        }
    }

    fn prior_erased(&self, key: &Key) -> bool {
        self.prior.iter().any(|(_, erased)| erased(key))
    }
}

/// Seed `case`'s original flushes into `store`, compact them when the case
/// says, and apply its prior erasures. Returns the L0 segments as the query
/// reads them before compaction and the bucket's live segments afterwards.
async fn prepare(
    store: &MemoryStore,
    clock: &FixedClock,
    case: &Case,
) -> (Vec<(Level, Bytes)>, Vec<(Level, Bytes)>) {
    let mut l0 = Vec::new();
    for (w, batch) in &case.originals {
        let key = seed_l0(store, *w, batch.clone()).await;
        l0.push((Level::L0(*w), get_full(store, &key).await));
    }
    let mut live = l0.clone();
    if case.compact {
        let outcome = compact_bucket(store, clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        assert!(
            matches!(outcome, CompactionOutcome::Compacted { .. }),
            "compaction publishes, got {outcome:?}"
        );
        let compactions = record_keys(store, false).await;
        assert_eq!(compactions.len(), 1, "exactly one compaction record");
        let key = compactions.first().unwrap();
        let rec = CompactionRecord::decode(get_full(store, key).await.as_ref()).unwrap();
        let parts: Vec<String> = rec
            .parts
            .iter()
            .map(|p| keys::reconstruct_l1_part_key(&rec, p).unwrap())
            .collect();
        live = l1_segments(store, &parts).await;
    }
    for (request, _) in &case.prior {
        let parts = erase(store, clock, request).await;
        live = l1_segments(store, &parts).await;
    }
    (l0, live)
}

/// What [`assert_rewrite_keeps_winners`] resolved and decoded, for a test's own
/// assertions on top.
struct Checked {
    /// The original flushes, as raw L0 inputs.
    from_l0: Resolution,
    /// The live record before the erasure plus the later commits.
    late_a: Resolution,
    /// The runs the erasure under test read.
    source: BTreeMap<[u8; 16], Vec<RunView>>,
    /// The runs it wrote.
    rewritten: BTreeMap<[u8; 16], Vec<RunView>>,
}

/// World A holds `case`'s live record and world B the same, then `request`
/// applied by the erasure rewrite. Every (series, ts) that `erased` does not
/// name must resolve to identical bits in both, before and after the later
/// commits land in each, and the rewritten part must hold its source's runs
/// less the erased samples ([`assert_rewrite_structure`]).
async fn assert_rewrite_keeps_winners(
    case: &Case,
    request: PendingErasureRequest,
    erased: impl Fn(&Key) -> bool + Copy,
) -> Checked {
    let clock = FixedClock::new(sealed_now_ns());

    let world_a = MemoryStore::new();
    let (mut l0, mut a_live) = prepare(&world_a, &clock, case).await;
    let from_l0 = resolve(&l0);
    let from_a = resolve(&a_live);
    assert_eq!(
        from_a,
        without(&from_l0, |k| case.prior_erased(k)),
        "the live record set resolves every (series, ts) as the L0 inputs do"
    );
    if case.compact {
        assert!(
            a_live.iter().any(|(_, obj)| {
                let loc = open_from_full(obj, ReaderLimits::default()).unwrap();
                decode_catalog_v5(&loc.footer, obj, ReaderLimits::default())
                    .unwrap()
                    .iter()
                    .any(|e| e.per_sample_provenance.iter().any(Option::is_some))
            }),
            "compaction merged runs, so its parts carry per-sample provenance"
        );
    }
    let erased_in_a = from_a.keys().filter(|k| erased(k)).count();
    assert!(erased_in_a > 0, "the request matches samples in the bucket");

    let world_b = MemoryStore::new();
    let (_, b_source) = prepare(&world_b, &clock, case).await;
    let rewrite_parts = erase(&world_b, &clock, &request).await;
    let mut b_live = l1_segments(&world_b, &rewrite_parts).await;
    let source = runs_by_series(&b_source);
    let rewritten = runs_by_series(&b_live);
    assert_rewrite_structure(&source, &rewritten, erased);
    let from_b = resolve(&b_live);
    assert!(
        from_b.keys().all(|k| !erased(k)),
        "the rewrite erased every matching sample"
    );
    assert_eq!(
        from_b,
        without(&from_a, erased),
        "every surviving (series, ts) resolves to identical bits after the rewrite"
    );

    for (w, batch) in &case.later {
        assert!(
            batch.iter().all(|(id, _, values)| {
                let timestamps: Vec<i64> = match values {
                    SeriesValues::Scalar(s) => s.iter().map(|s| s.ts_ns).collect(),
                    SeriesValues::Histogram(s) => s.iter().map(|s| s.ts_ns).collect(),
                };
                timestamps.iter().all(|&ts| {
                    let key = (id.0, ts);
                    !erased(&key) && !case.prior_erased(&key)
                })
            }),
            "a later commit writes no erased (series, ts)"
        );
        let a_key = seed_l0(&world_a, *w, batch.clone()).await;
        let b_key = seed_l0(&world_b, *w, batch.clone()).await;
        let a_obj = get_full(&world_a, &a_key).await;
        l0.push((Level::L0(*w), a_obj.clone()));
        a_live.push((Level::L0(*w), a_obj));
        b_live.push((Level::L0(*w), get_full(&world_b, &b_key).await));
    }
    let late_a = resolve(&a_live);
    assert_eq!(
        late_a,
        without(&resolve(&l0), |k| case.prior_erased(k)),
        "the live record set resolves the later commits as the L0 inputs do"
    );
    let late_b = resolve(&b_live);
    for (key, winner) in &without(&late_a, erased) {
        assert_eq!(
            late_b.get(key),
            Some(winner),
            "series {} ts {}: the later commits resolve the same against the \
             rewrite as against the record it replaced",
            hex::encode(key.0),
            key.1
        );
    }
    assert_eq!(late_b, without(&late_a, erased));
    Checked {
        from_l0,
        late_a,
        source,
        rewritten,
    }
}

/// Erasing an unrelated series from a compacted bucket: every other run is
/// copied through, including the merged runs and their provenance columns.
#[tokio::test]
async fn erasing_another_series_keeps_every_dedup_winner() {
    let case = Case::new(original_flushes(), true, later_flushes());
    let checked =
        assert_rewrite_keeps_winners(&case, erasure_request("victim", 0, 0), |(series, _)| {
            *series == id("victim", "v")
        })
        .await;
    assert_original_winners(&checked.from_l0);
    assert_later_winners(&checked.late_a);
}

/// A windowed erasure inside a merged run: W2's `alpha` sample at 3000 goes,
/// and the run's survivors are re-encoded with their own provenance entries.
#[tokio::test]
async fn erasing_part_of_a_merged_run_keeps_every_dedup_winner() {
    let case = Case::new(original_flushes(), true, later_flushes());
    let checked = assert_rewrite_keeps_winners(
        &case,
        erasure_request("alpha", 3_000, 4_000),
        |(series, ts)| *series == id("alpha", "a") && *ts == 3_000,
    )
    .await;
    assert_original_winners(&checked.from_l0);
    assert_later_winners(&checked.late_a);
}

/// The same windowed erasure over the raw L0 inputs of a bucket never
/// compacted: each run keeps its commit record's identity.
#[tokio::test]
async fn erasing_part_of_a_raw_l0_bucket_keeps_every_dedup_winner() {
    let case = Case::new(original_flushes(), false, later_flushes());
    let checked = assert_rewrite_keeps_winners(
        &case,
        erasure_request("alpha", 3_000, 4_000),
        |(series, ts)| *series == id("alpha", "a") && *ts == 3_000,
    )
    .await;
    assert_original_winners(&checked.from_l0);
    assert_later_winners(&checked.late_a);
}

/// A merged run loses a sample from its middle, ahead of a timestamp that two
/// writers contest. Each survivor after the gap must keep its own column entry,
/// not the one at its new position: there, W1's 4.0 would take W2's key and
/// lose to the later W6 commit that W1 beats.
#[tokio::test]
async fn erasing_a_gap_in_a_merged_run_keeps_each_survivors_key() {
    let originals = vec![
        (
            W2,
            vec![scalar(
                "gap",
                "g",
                &[(1_000, bits(1.0)), (2_000, bits(2.0)), (3_000, bits(3.0))],
            )],
        ),
        (W1, vec![scalar("gap", "g", &[(3_000, bits(4.0))])]),
    ];
    let later = vec![(W6, vec![scalar("gap", "g", &[(3_000, bits(6.0))])])];
    let case = Case::new(originals, true, later);
    let gap = id("gap", "g");
    let checked = assert_rewrite_keeps_winners(
        &case,
        erasure_request("gap", 2_000, 3_000),
        |(series, ts)| *series == gap && *ts == 2_000,
    )
    .await;
    assert_eq!(checked.from_l0[&(gap, 3_000)], Resolved::Scalar(bits(4.0)));
    assert_eq!(
        checked.late_a[&(gap, 3_000)],
        Resolved::Scalar(bits(4.0)),
        "W1 beats the later W6 commit, which beats W2"
    );
    let [merged] = checked.source[&gap].as_slice() else {
        panic!("compaction merges `gap` into one run");
    };
    assert!(merged.column.is_some(), "the merged run carries a column");
}

/// A windowed erasure of the merged histogram run removes both samples at
/// 1000; the survivors at 2000 must keep W2's and W3's keys so the later W6
/// commit still loses to W3.
#[tokio::test]
async fn erasing_part_of_a_merged_histogram_run_keeps_every_dedup_winner() {
    let case = Case::new(original_flushes(), true, later_losers());
    let hist = id("hist", "h");
    let checked = assert_rewrite_keeps_winners(
        &case,
        erasure_request("hist", 1_000, 2_000),
        |(series, ts)| *series == hist && *ts == 1_000,
    )
    .await;
    assert_original_winners(&checked.from_l0);
    assert_later_losers(&checked.late_a);
    let [run] = checked.rewritten[&hist].as_slice() else {
        panic!("the rewrite keeps one `hist` run");
    };
    assert_eq!(
        run.column,
        Some(vec![
            (W2.created_unix_ns, W2.epoch, W2.seq, 1),
            (W3.created_unix_ns, W3.epoch, W3.seq, 0),
        ]),
        "the histogram run keeps its survivors' column entries"
    );
}

/// A single-writer run that compaction copied verbatim has no column and a
/// stored triple of W1. When it loses a sample, the column synthesized for its
/// survivors must carry W1's triple: the later W6 commit beats only a zero one.
#[tokio::test]
async fn erasing_part_of_a_verbatim_run_keeps_its_writer() {
    let case = Case::new(original_flushes(), true, later_losers());
    let solo = id("solo", "s");
    let checked = assert_rewrite_keeps_winners(
        &case,
        erasure_request("solo", 1_000, 2_000),
        |(series, ts)| *series == solo && *ts == 1_000,
    )
    .await;
    assert_original_winners(&checked.from_l0);
    assert_later_losers(&checked.late_a);
    let [source] = checked.source[&solo].as_slice() else {
        panic!("compaction copies `solo` as one run");
    };
    assert_eq!(source.column, None, "compaction copied `solo` verbatim");
    let [run] = checked.rewritten[&solo].as_slice() else {
        panic!("the rewrite keeps one `solo` run");
    };
    assert_eq!(run.triple, W1.triple());
    assert_eq!(
        run.column,
        Some(vec![(W1.created_unix_ns, W1.epoch, W1.seq, 1)]),
        "the survivor keeps W1's triple and its original index"
    );
}

/// A second erasure over a bucket whose first rewrite read raw L0 inputs: the
/// first rewrite keeps one run per writer of each series, so the second reads
/// several runs of `alpha` and must line each up with its own stored triple.
#[tokio::test]
async fn erasing_from_a_rewrite_of_a_raw_l0_bucket_keeps_every_dedup_winner() {
    let mut case = Case::new(original_flushes(), false, later_flushes());
    case.prior.push((
        erasure_request_with_id(0x2410, "victim", 0, 0),
        |(series, _)| *series == id("victim", "v"),
    ));
    let alpha = id("alpha", "a");
    let checked = assert_rewrite_keeps_winners(
        &case,
        erasure_request("alpha", 3_000, 4_000),
        |(series, ts)| *series == alpha && *ts == 3_000,
    )
    .await;
    assert_original_winners(&checked.from_l0);
    assert_later_winners(&checked.late_a);
    let triples: Vec<_> = checked.source[&alpha].iter().map(|r| r.triple).collect();
    assert_eq!(
        triples,
        vec![W2.triple(), W1.triple(), W3.triple()],
        "the first rewrite keeps one `alpha` run per writer"
    );
}

/// A merged run whose minimum writer (W2) loses every sample to the erasure
/// takes the minimum of its survivors' keys as its triple, so it is the run
/// the compactor builds from the survivors alone.
#[tokio::test]
async fn a_filtered_merged_run_takes_its_survivors_minimum_triple() {
    let survivors = || {
        vec![
            (
                W1,
                vec![scalar(
                    "floor",
                    "f",
                    &[(2_000, bits(2.0)), (3_000, bits(3.0))],
                )],
            ),
            (W3, vec![scalar("floor", "f", &[(3_000, bits(5.0))])]),
        ]
    };
    let mut originals = vec![(W2, vec![scalar("floor", "f", &[(1_000, bits(1.0))])])];
    originals.extend(survivors());
    let later = vec![(W6, vec![scalar("floor", "f", &[(2_000, bits(6.0))])])];
    let case = Case::new(originals, true, later);
    let floor = id("floor", "f");
    let checked = assert_rewrite_keeps_winners(
        &case,
        erasure_request("floor", 1_000, 2_000),
        |(series, ts)| *series == floor && *ts == 1_000,
    )
    .await;
    assert_eq!(checked.late_a[&(floor, 2_000)], Resolved::Scalar(bits(2.0)));
    assert_eq!(checked.late_a[&(floor, 3_000)], Resolved::Scalar(bits(5.0)));

    let [source] = checked.source[&floor].as_slice() else {
        panic!("compaction merges `floor` into one run");
    };
    assert_eq!(source.triple, W2.triple(), "the merged run's minimum is W2");
    let [run] = checked.rewritten[&floor].as_slice() else {
        panic!("the rewrite keeps one `floor` run");
    };
    assert_eq!(run.triple, W1.triple(), "W2 no longer names the run");

    let clock = FixedClock::new(sealed_now_ns());
    let world_c = MemoryStore::new();
    let (_, compacted) = prepare(&world_c, &clock, &Case::new(survivors(), true, Vec::new())).await;
    assert_eq!(
        runs_by_series(&compacted)[&floor],
        checked.rewritten[&floor],
        "the rewritten run is the compactor's run over the survivors"
    );
}

/// Two writers whose commits tie on `(created_unix_ns, writer_epoch,
/// writer_seq)`, which the provenance order allows across writer ids: the
/// in-page index decides their duplicate. Erasing samples ahead of the
/// contested one in its run must not move it to a lower index.
#[tokio::test]
async fn erasing_ahead_of_a_tied_duplicate_keeps_its_in_page_index() {
    let ta = Writer { id: 7, ..W1 };
    let tb = Writer { id: 8, ..W1 };
    let flushes = || {
        vec![
            (
                ta,
                vec![scalar(
                    "tie",
                    "t",
                    &[(100, bits(1.0)), (200, bits(2.0)), (5_000, bits(3.0))],
                )],
            ),
            (tb, vec![scalar("tie", "t", &[(5_000, bits(4.0))])]),
        ]
    };
    let clock = FixedClock::new(sealed_now_ns());

    let world_a = MemoryStore::new();
    let mut l0 = Vec::new();
    for (w, batch) in flushes() {
        let key = seed_l0(&world_a, w, batch).await;
        l0.push((Level::L0(w), get_full(&world_a, &key).await));
    }
    let from_a = resolve(&l0);
    let contested = (id("tie", "t"), 5_000);
    assert_eq!(
        from_a[&contested],
        Resolved::Scalar(bits(3.0)),
        "index 2 beats index 0 at a full provenance tie"
    );

    let world_b = MemoryStore::new();
    for (w, batch) in flushes() {
        seed_l0(&world_b, w, batch).await;
    }
    let parts = erase(&world_b, &clock, &erasure_request("tie", 100, 300)).await;
    let from_b = resolve(&l1_segments(&world_b, &parts).await);
    assert_eq!(
        from_b,
        without(&from_a, |(_, ts)| *ts < 300),
        "the rewrite erased the window and kept the winner"
    );
}
