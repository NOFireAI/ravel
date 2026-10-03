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
//! states: the raw L0 inputs, the compaction record, and the rewrite record
//! that erased an unrelated series. Then a later commit from a fifth writer
//! duplicates three of those timestamps, and the winner must be the same
//! whether that commit is resolved against the compaction or the rewrite.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;

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
    CompactionMetaV4, HistogramCounts, HistogramSample, HistogramValue, IngestBounds,
    ReaderLimits, ResetHint, SegmentIdentity, SegmentWriter, SeriesInputV4, SeriesValues,
    VERSION_V7, ValueKind, decode_catalog_v5, decode_run_histogram_pages, decode_run_pages_soa,
    encode_run_v4, open_from_full, plan_ranges_v4,
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
/// The later commit: newer than every original flush, older than the
/// compaction record and the rewrite record, both stamped by the maintenance
/// clock at `sealed_now_ns()`.
const W5: Writer = Writer {
    id: 5,
    epoch: 1,
    seq: 1,
    created_unix_ns: hour_start() + 30 * 60_000 * MS,
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
type Resolution = BTreeMap<([u8; 16], i64), Resolved>;

/// How the query fetcher stamps a segment's samples: an L0 object by its commit
/// record, an L1 or rewrite part by each run's own catalog provenance or, when
/// the run carries one, its per-sample provenance column.
enum Level {
    L0(Writer),
    L1,
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
async fn seed_l0(
    store: &dyn ObjectStoreBackend,
    w: Writer,
    batch: Vec<(SeriesId, LabelSet, SeriesValues)>,
) -> String {
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
fn original_flushes() -> Vec<(Writer, Vec<(SeriesId, LabelSet, SeriesValues)>)> {
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
                // Only W2 writes `solo`: compaction copies its run verbatim.
                scalar("solo", "s", &[(1_000, bits(4.0)), (2_000, bits(-0.0))]),
                histogram("hist", "h", &[(1_000, 20), (2_000, 30)]),
                scalar("victim", "v", &[(1_000, bits(10.0))]),
            ],
        ),
    ]
}

/// The later commit: one more duplicate on a merged scalar run, a verbatim
/// single-writer run, and a merged histogram run.
fn later_flush() -> Vec<(SeriesId, LabelSet, SeriesValues)> {
    vec![
        scalar("alpha", "a", &[(1_000, bits(11.0))]),
        scalar("solo", "s", &[(1_000, bits(12.0))]),
        histogram("hist", "h", &[(1_000, 50)]),
    ]
}

/// Resolve every (series, timestamp) over `segments` under the ADR-0010 §5
/// order, greatest `(created_unix_ns, writer_epoch, writer_seq, in_page_index)`
/// winning, with the provenance source the query fetcher uses per level
/// (`ravel-query` `fetcher.rs`, `SegmentLevel::L0` and `SegmentLevel::L1`).
fn resolve(segments: &[(Level, Bytes)]) -> Resolution {
    let limits = ReaderLimits::default();
    let mut best: BTreeMap<([u8; 16], i64), (Priority, Resolved)> = BTreeMap::new();
    for (level, obj) in segments {
        let loc = open_from_full(obj, limits).expect("open segment");
        let entries = decode_catalog_v5(&loc.footer, obj, limits).expect("decode catalog");
        let refs: Vec<_> = entries.iter().collect();
        let mut planned = plan_ranges_v4(&loc.footer, &refs)
            .expect("plan ranges")
            .into_iter();
        for entry in &entries {
            let id = entry.entry.series_id;
            for (run_index, run) in entry.runs.iter().enumerate() {
                let range = planned.next().expect("one range per run");
                let ts_page = slice(obj, range.ts_range);
                let decoded: Vec<(i64, Resolved)> = match entry.entry.value_kind {
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
                    .and_then(Option::as_ref);
                for (pos, (ts, value)) in decoded.into_iter().enumerate() {
                    let pos = u32::try_from(pos).expect("in-page index");
                    let priority = match (level, column) {
                        (Level::L0(w), _) => (w.created_unix_ns, w.epoch, w.seq, pos),
                        (Level::L1, Some(col)) => {
                            let p = col[pos as usize];
                            (
                                p.created_unix_ns,
                                p.writer_epoch,
                                p.writer_seq,
                                p.in_page_index,
                            )
                        }
                        (Level::L1, None) => {
                            (run.created_unix_ns, run.writer_epoch, run.writer_seq, pos)
                        }
                    };
                    let candidate = (priority, value);
                    let slot = best.entry((id.0, ts)).or_insert(candidate);
                    if candidate > *slot {
                        *slot = candidate;
                    }
                }
            }
        }
        assert!(planned.next().is_none(), "one range per run");
    }
    best.into_iter().map(|(k, (_, v))| (k, v)).collect()
}

fn slice(bytes: &[u8], (offset, len): (u64, u64)) -> &[u8] {
    &bytes[offset as usize..(offset + len) as usize]
}

/// A windowless erasure request for every series named `victim`.
fn erase_victim() -> PendingErasureRequest {
    let request_id = Uuid::from_u128(0x2409);
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
                value: "victim".to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        },
    }
}

/// The single record key of one kind in the bucket.
async fn record_key(store: &dyn ObjectStoreBackend, rewrite: bool) -> String {
    let b = bucket();
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .unwrap();
    let found: Vec<String> = list_all(store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .filter(|k| match keys::partition_bucket_entry(k) {
            Ok(BucketEntry::RewriteRecord(_)) => rewrite,
            Ok(BucketEntry::CompactionRecord(_)) => !rewrite,
            _ => false,
        })
        .collect();
    assert_eq!(found.len(), 1, "exactly one record (rewrite={rewrite})");
    found.into_iter().next().unwrap()
}

async fn l1_segments(store: &dyn ObjectStoreBackend, keys: &[String]) -> Vec<(Level, Bytes)> {
    let mut out = Vec::new();
    for key in keys {
        out.push((Level::L1, get_full(store, key).await));
    }
    out
}

/// Compact the original flushes into `store`. Returns the L0 segments as the
/// query would read them before compaction and the compaction part keys.
async fn seed_and_compact(
    store: &MemoryStore,
    clock: &FixedClock,
) -> (Vec<(Level, Bytes)>, Vec<String>) {
    let mut l0 = Vec::new();
    for (w, batch) in original_flushes() {
        let key = seed_l0(store, w, batch).await;
        l0.push((Level::L0(w), get_full(store, &key).await));
    }
    let outcome = compact_bucket(store, clock, &CompactorConfig::default(), &bucket())
        .await
        .expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "compaction publishes, got {outcome:?}"
    );
    let key = record_key(store, false).await;
    let rec = CompactionRecord::decode(get_full(store, &key).await.as_ref()).unwrap();
    let parts = rec
        .parts
        .iter()
        .map(|p| keys::reconstruct_l1_part_key(&rec, p).unwrap())
        .collect();
    (l0, parts)
}

async fn erase(store: &MemoryStore, clock: &FixedClock) -> Vec<String> {
    let mut memo = MaintainMemo::with_default_interval();
    let outcome = erasure_rewrite_bucket(
        store,
        clock,
        &CompactorConfig::default(),
        &NoLeases,
        &bucket(),
        &[erase_victim()],
        &mut memo,
    )
    .await
    .expect("erasure rewrite");
    assert!(
        matches!(outcome, ErasureRewriteOutcome::Rewritten { .. }),
        "the rewrite publishes, got {outcome:?}"
    );
    let key = record_key(store, true).await;
    let rec = RewriteRecord::decode(get_full(store, &key).await.as_ref()).unwrap();
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

/// Erasing an unrelated series leaves every surviving (series, timestamp)
/// resolving to the identical bits, and a later commit duplicating three of
/// those timestamps wins or loses the same way against the rewrite as against
/// the compaction it replaced.
#[tokio::test]
async fn erasure_rewrite_keeps_every_dedup_winner() {
    let clock = FixedClock::new(sealed_now_ns());

    // World A: compaction only.
    let compacted = MemoryStore::new();
    let (l0, compacted_parts) = seed_and_compact(&compacted, &clock).await;
    let from_l0 = resolve(&l0);
    assert_original_winners(&from_l0);
    let compacted_segments = l1_segments(&compacted, &compacted_parts).await;
    assert!(
        compacted_segments.iter().any(|(_, obj)| {
            let loc = open_from_full(obj, ReaderLimits::default()).unwrap();
            decode_catalog_v5(&loc.footer, obj, ReaderLimits::default())
                .unwrap()
                .iter()
                .any(|e| e.per_sample_provenance.iter().any(Option::is_some))
        }),
        "compaction merged runs, so its parts carry per-sample provenance"
    );
    let from_compaction = resolve(&compacted_segments);
    assert_eq!(
        from_compaction, from_l0,
        "compaction resolves every (series, ts) as its L0 inputs did"
    );

    // World B: the same compaction, then an erasure of `victim`.
    let rewritten = MemoryStore::new();
    let (_, rewritten_compaction) = seed_and_compact(&rewritten, &clock).await;
    assert_eq!(
        rewritten_compaction, compacted_parts,
        "both worlds hold the same compaction"
    );
    let rewrite_parts = erase(&rewritten, &clock).await;
    let from_rewrite = resolve(&l1_segments(&rewritten, &rewrite_parts).await);
    let victim = id("victim", "v");
    let mut surviving = from_compaction.clone();
    surviving.retain(|(series, _), _| *series != victim);
    assert!(
        from_rewrite.keys().all(|(series, _)| *series != victim),
        "the rewrite erased the subject"
    );
    assert_eq!(
        from_rewrite, surviving,
        "every surviving (series, ts) resolves to identical bits after the rewrite"
    );

    // A later commit from a fifth writer, against each world.
    let a_late = seed_l0(&compacted, W5, later_flush()).await;
    let b_late = seed_l0(&rewritten, W5, later_flush()).await;
    let mut a_segments = compacted_segments;
    a_segments.push((Level::L0(W5), get_full(&compacted, &a_late).await));
    let mut b_segments = l1_segments(&rewritten, &rewrite_parts).await;
    b_segments.push((Level::L0(W5), get_full(&rewritten, &b_late).await));
    let a_resolved = resolve(&a_segments);
    let b_resolved = resolve(&b_segments);

    let mut l0_late = l0;
    l0_late.push((Level::L0(W5), get_full(&compacted, &a_late).await));
    assert_eq!(
        a_resolved,
        resolve(&l0_late),
        "the compaction resolves the later commit as its L0 inputs would"
    );
    assert_eq!(
        a_resolved[&(id("alpha", "a"), 1_000)],
        Resolved::Scalar(bits(11.0)),
        "the later commit is newer than every original flush"
    );
    assert_eq!(
        a_resolved[&(id("solo", "s"), 1_000)],
        Resolved::Scalar(bits(12.0))
    );
    assert_eq!(
        a_resolved[&(id("hist", "h"), 1_000)],
        Resolved::Histogram(50)
    );

    let mut a_surviving = a_resolved;
    a_surviving.retain(|(series, _), _| *series != victim);
    for (key, winner) in &a_surviving {
        assert_eq!(
            b_resolved.get(key),
            Some(winner),
            "series {} ts {}: the later commit resolves the same against the \
             rewrite as against the compaction",
            hex::encode(key.0),
            key.1
        );
    }
    assert_eq!(b_resolved, a_surviving);
}
