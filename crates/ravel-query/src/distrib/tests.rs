//! Acceptance and coordinator-invariant tests for the ADR-0071 distributed
//! read fan-out.
//!
//! The centerpiece is [`distributed_merge_equals_local_bitwise`]: over a real
//! in-process `tonic` loopback worker (bound on `127.0.0.1:0`), a distributed
//! fetch merged by the coordinator is byte-for-byte identical to the local
//! fetch of the same pinned snapshot, for generated corpora and arbitrary slice
//! partitions -- including a corpus where one logical series spans two shards
//! across a reshard activation hour, so the same series id lands in different
//! slices. Both paths feed the *same* total-order k-way merge
//! (`crate::engine::merge_soa_runs`), so the test proves the codec preserves
//! every run bit-exactly and the partition is total (no run dropped or
//! duplicated), which is exactly what makes the two results identical.

#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use proptest::prelude::*;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::SeriesData;
use ravel_proto::queryfrag::v1 as pb;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::accounting::{QueryAccounting, QueryAccountingSnapshot};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};
use uuid::Uuid;

use crate::config::EngineConfig;
use crate::distrib::Distributed;
use crate::distrib::client::{DistribError, RemoteSliceFetcher, SliceFetcher, SliceResponse};
use crate::distrib::partition::DistribThresholds;
use crate::distrib::service::{SeriesFetchService, SnapshotSegmentResolver};
use crate::engine::merge_soa_runs;
use crate::erasure::ErasurePredicate;
use crate::fetcher::SegmentFetcher;

const NS: i64 = 1_000_000;
const TENANT: TenantHash = TenantHash([7u8; 16]);

fn tenant_id() -> TenantId {
    TenantId::new("acme".to_string())
}

fn labels(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

/// One series in one segment: its metric name and its ascending, distinct-ts
/// samples (values carried as raw `u64` bit patterns so NaN/-0.0 appear).
#[derive(Debug)]
struct SeriesDesc {
    metric: String,
    samples: Vec<(i64, u64)>,
}

/// Writes one real RSEG segment holding `descs` and returns its `SegmentRef`.
///
/// The corpus deliberately gives every segment the *same* `created_unix_ns`
/// (0) and a per-segment `writer_seq` (from `seq`), leaving `writer_epoch`
/// constant at 1. So the ADR-0010 cross-segment dedup total order
/// `(created_unix_ns, writer_epoch, writer_seq, ...)` is decided by
/// `writer_seq`, not `created_unix_ns` -- exercising a tie-break field past the
/// first across the wire (finding 9). The full four-field chain is isolated
/// field-by-field in [`dedup_tiebreak_chain_survives_the_wire`].
async fn write_segment(
    store: &MemoryStore,
    seq: u64,
    shard: u32,
    hour_bucket: u32,
    descs: &[SeriesDesc],
) -> SegmentRef {
    write_segment_prov(store, seq, 0, 1, seq, shard, hour_bucket, descs).await
}

/// Writes one real RSEG segment with explicit dedup-provenance fields. `key`
/// makes the object key and `writer_id` unique even when the dedup priority
/// tuple `(created_unix_ns, writer_epoch, writer_seq)` collides with another
/// segment's (writer_id is not part of the priority, so a pair can tie on the
/// whole prefix and be decided by the value bit pattern). The written RSEG
/// `SegmentIdentity` carries `writer_epoch`/`writer_seq` so the fetch path's
/// footer identity check passes; `created_unix_ns` lives on the `SegmentRef`
/// only (it is not part of the footer identity), so it can be set freely.
#[allow(clippy::too_many_arguments)]
async fn write_segment_prov(
    store: &MemoryStore,
    key: u64,
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    shard: u32,
    hour_bucket: u32,
    descs: &[SeriesDesc],
) -> SegmentRef {
    let writer_id = Uuid::from_u128(u128::from(key) + 1);
    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch,
        writer_seq,
    };
    let series: Vec<SeriesInput> = descs
        .iter()
        .map(|d| {
            let label_set = labels(&d.metric);
            let series_id =
                SeriesId::compute(&tenant_id(), &d.metric, &label_set).expect("series id");
            SeriesInput {
                series_id,
                labels: label_set,
                samples: d
                    .samples
                    .iter()
                    .map(|(ts, bits)| Sample {
                        ts_ns: *ts,
                        value: f64::from_bits(*bits),
                    })
                    .collect(),
            }
        })
        .collect();
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
    let object_key = format!("seg/{key}.rseg");
    store
        .put(&object_key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put segment");
    SegmentRef {
        data_object_key: object_key,
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: hour_bucket,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard,
        content_hash: written.summary.blake3,
        writer_id,
        writer_epoch,
        writer_seq,
        created_unix_ns,
        level: SegmentLevel::L0,
    }
}

/// Starts a real `tonic` `SeriesFetch` worker on `127.0.0.1:0` over `store` and
/// `segments`, and returns a `RemoteSliceFetcher` connected to it plus the
/// server task handle (abort it to shut the worker down).
async fn spawn_worker(
    store: Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
) -> (RemoteSliceFetcher, JoinHandle<()>) {
    let fetcher = SegmentFetcher::new(store);
    let resolver = Arc::new(SnapshotSegmentResolver::new(segments));
    let service = SeriesFetchService::new(fetcher, resolver).into_server();

    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("addr")).expect("bind");
    let addr = incoming.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });

    // Connect a channel to the just-bound worker. `connect_lazy` avoids a
    // startup race against the spawned server's first poll.
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_lazy();
    (RemoteSliceFetcher::new(channel), handle)
}

/// The local reference: fetch every snapshot segment's scalar runs directly,
/// exactly as `QueryEngine::fetch_all_samples_and_histograms` does (no matchers,
/// no erasure -- the corpus carries none), producing the per-segment run pool
/// the local path would merge, plus the accounting snapshot and summed
/// `FetchStats` the local path reports. The distributed path must reproduce all
/// three (finding 4: a distributed query reports the same cost and stats a
/// local one does, not zeros).
async fn local_scalar(
    store: Arc<MemoryStore>,
    snapshot: &Snapshot,
) -> (
    Vec<Vec<crate::fetcher::FetchedSeriesSoa>>,
    QueryAccountingSnapshot,
    crate::fetcher::FetchStats,
) {
    let fetcher = SegmentFetcher::new(store);
    let accounting = QueryAccounting::new();
    let mut out = Vec::with_capacity(snapshot.segments.len());
    let mut stats = crate::fetcher::FetchStats::default();
    for seg in &snapshot.segments {
        let (scalar, seg_stats, _hist) = fetcher
            .fetch_soa_and_histograms_accounted(TENANT, seg, &[], &accounting)
            .await
            .expect("local fetch");
        stats.raw_f64_pages += seg_stats.raw_f64_pages;
        stats.raw_f64_bytes += seg_stats.raw_f64_bytes;
        out.push(scalar);
    }
    (out, accounting.snapshot(), stats)
}

fn assert_series_bit_identical(local: &[SeriesData], distributed: &[SeriesData]) {
    let key = |s: &SeriesData| {
        s.labels
            .iter()
            .map(|l| (l.name.clone(), l.value.clone()))
            .collect::<Vec<_>>()
    };
    let mut a: Vec<&SeriesData> = local.iter().collect();
    let mut b: Vec<&SeriesData> = distributed.iter().collect();
    a.sort_by_key(|s| key(s));
    b.sort_by_key(|s| key(s));
    assert_eq!(
        a.len(),
        b.len(),
        "series count differs local vs distributed"
    );
    for (la, lb) in a.iter().zip(b.iter()) {
        assert_eq!(key(la), key(lb), "series labels differ");
        assert_eq!(
            la.samples.len(),
            lb.samples.len(),
            "sample count differs for a series"
        );
        for (sa, sb) in la.samples.iter().zip(lb.samples.iter()) {
            assert_eq!(sa.ts_ns, sb.ts_ns, "timestamp differs");
            // Bit-exact, never `==`: NaN and -0.0 must match by pattern.
            assert_eq!(
                sa.value.to_bits(),
                sb.value.to_bits(),
                "value bit pattern differs (NaN/-0.0 corruption)"
            );
        }
    }
}

/// Runs one acceptance case: build the corpus, then assert the distributed
/// fetch matches the local one over it.
async fn run_acceptance(
    segments_desc: Vec<(u32, u32, Vec<SeriesDesc>)>,
    max_parallel_slices: usize,
) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for (seq, (shard, hour, descs)) in segments_desc.into_iter().enumerate() {
        segments.push(write_segment(&store, seq as u64, shard, hour, &descs).await);
    }
    assert_distributed_matches_local(store, segments, max_parallel_slices).await;
}

/// Fetches `segments` both locally and distributed over a loopback worker and
/// asserts the two coordinator-merged results are byte-identical and that the
/// distributed path reports the same accounting and stats the local path does.
async fn assert_distributed_matches_local(
    store: Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
    max_parallel_slices: usize,
) {
    let snapshot = Snapshot {
        segments: segments.clone(),
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };

    // Local reference.
    let (local_runs, local_acct, local_stats) = local_scalar(Arc::clone(&store), &snapshot).await;
    let local_merged = merge_soa_runs(local_runs, usize::MAX, usize::MAX).expect("local merge");

    // Distributed over a real tonic worker.
    let (fetcher, server) = spawn_worker(Arc::clone(&store), segments).await;
    let thresholds = DistribThresholds {
        min_store_bytes: 0,
        min_segments: 0,
        max_parallel_slices,
    };
    let distributed = Distributed::new(Arc::new(fetcher), thresholds);
    let config = EngineConfig::default();
    let accounting = QueryAccounting::new();
    let triple = distributed
        .fetch(
            TENANT,
            Signal::Metrics,
            &snapshot,
            &[],
            &[],
            &accounting,
            &config,
            i64::MAX,
        )
        .await
        .expect("distributed fetch")
        .expect("distributed produced a result (not a fallback)");
    let distributed_stats = triple.1;
    let distributed_merged = merge_soa_runs(triple.0, usize::MAX, usize::MAX).expect("dist merge");

    assert_series_bit_identical(&local_merged, &distributed_merged);
    // The distributed path folds every slice's accounting and stats, so it
    // reports the same cost the local path does over the same disjoint
    // segments -- not `FetchStats::default()` zeros, and not a wrapped or
    // dropped accounting counter (findings 3 and 4).
    assert_eq!(
        accounting.snapshot(),
        local_acct,
        "distributed accounting must equal local accounting"
    );
    assert_eq!(
        distributed_stats, local_stats,
        "distributed FetchStats must equal local FetchStats, not zeros"
    );
    server.abort();
}

// --- corpus strategy -------------------------------------------------------

fn arb_samples() -> impl Strategy<Value = Vec<(i64, u64)>> {
    // Distinct, ascending timestamps within a run (RSEG page order); arbitrary
    // value bit patterns so NaN, signalling NaN, and -0.0 all occur.
    prop::collection::vec((0u32..64, any::<u64>()), 1..8).prop_map(|mut v| {
        v.sort_by_key(|(t, _)| *t);
        v.dedup_by_key(|(t, _)| *t);
        v.into_iter()
            .map(|(t, bits)| (i64::from(t) * NS, bits))
            .collect()
    })
}

fn arb_segment() -> impl Strategy<Value = (u32, u32, Vec<SeriesDesc>)> {
    // A small metric pool (m0..m3) forces the same series id to recur across
    // segments and shards -- the cross-segment dedup / reshard case.
    let series = prop::collection::vec((0u8..4, arb_samples()), 1..4).prop_map(|v| {
        // De-duplicate metrics within one segment (RSEG rejects duplicate
        // series ids in a single object).
        let mut seen = std::collections::HashSet::new();
        v.into_iter()
            .filter(|(id, _)| seen.insert(*id))
            .map(|(id, samples)| SeriesDesc {
                metric: format!("m{id}"),
                samples,
            })
            .collect::<Vec<_>>()
    });
    // Shards span 0..8 (finding 10): with up to 7 segments below, a corpus can
    // hold more distinct shards than the slice cap (1..=6), so the cap actually
    // binds and slice counts past the cap boundary are generated -- caps 4..6
    // are no longer indistinguishable from 3. A narrower `0u32..3` capped
    // distinct shards at 3, so every cap >= 3 behaved identically.
    (0u32..8, 100u32..104, series)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// ADR-0071 acceptance: coordinator-merged distributed fetch == local fetch,
    /// bit-for-bit, over a real loopback worker, for arbitrary corpora and
    /// arbitrary slice counts.
    #[test]
    fn distributed_merge_equals_local_bitwise(
        segments in prop::collection::vec(arb_segment(), 1..8),
        cap in 1usize..=6,
    ) {
        let rt = Runtime::new().expect("runtime");
        rt.block_on(run_acceptance(segments, cap));
    }
}

/// A hand-built corpus where metric `m0` is written under shard 0 in hour 100
/// and again under shard 1 in hour 101: a reshard activation moved the series
/// to a new shard across the hour boundary, so partitioning (which is
/// shard-major) puts the two runs of one series id in *different* slices. The
/// merge must still reassemble them identically to local.
#[test]
fn reshard_activation_hour_series_spans_two_slices() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let corpus = vec![
            (
                0u32,
                100u32,
                vec![SeriesDesc {
                    metric: "m0".to_string(),
                    samples: vec![(NS, 1.0f64.to_bits()), (2 * NS, 2.0f64.to_bits())],
                }],
            ),
            (
                1u32,
                101u32,
                vec![SeriesDesc {
                    metric: "m0".to_string(),
                    samples: vec![(3 * NS, 3.0f64.to_bits()), (4 * NS, (-0.0f64).to_bits())],
                }],
            ),
        ];
        // cap 2 => the two shards land in two distinct slices.
        run_acceptance(corpus, 2).await;
    });
}

// --- worker-side erasure ---------------------------------------------------

/// Runs a distributed fetch with the given erasure predicates over a loopback
/// worker, merges the result, and returns the sorted set of `__name__`s
/// present. Threads erasure through the fetch exactly as the engine does.
async fn distributed_metric_names(
    store: Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
    snapshot: &Snapshot,
    erasure: &[ErasurePredicate],
    cap: usize,
) -> Vec<String> {
    let (fetcher, server) = spawn_worker(store, segments).await;
    let distributed = Distributed::new(
        Arc::new(fetcher),
        DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: cap,
        },
    );
    let accounting = QueryAccounting::new();
    let triple = distributed
        .fetch(
            TENANT,
            Signal::Metrics,
            snapshot,
            &[],
            erasure,
            &accounting,
            &EngineConfig::default(),
            i64::MAX,
        )
        .await
        .expect("distributed fetch")
        .expect("distributed produced a result");
    let merged = merge_soa_runs(triple.0, usize::MAX, usize::MAX).expect("merge");
    server.abort();
    let mut names: Vec<String> = merged
        .iter()
        .map(|s| {
            s.labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.clone())
                .unwrap_or_default()
        })
        .collect();
    names.sort();
    names
}

/// The worker applies the request's erasure predicates post-decode, before
/// streaming, exactly as the local path would (ADR-0064, ADR-0071): the
/// coordinator does not re-apply, so a series the predicate erases must be
/// absent from the distributed result. Deleting the worker's
/// `retain_series_soa` call (`service.rs`) makes the erased series reappear and
/// the "erased absent" assertion below fails (finding 8).
#[test]
fn worker_applies_erasure_before_streaming() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let descs = vec![
            SeriesDesc {
                metric: "keep".to_string(),
                samples: vec![(NS, 1.0f64.to_bits()), (2 * NS, 2.0f64.to_bits())],
            },
            SeriesDesc {
                metric: "erased".to_string(),
                samples: vec![(NS, 3.0f64.to_bits()), (2 * NS, 4.0f64.to_bits())],
            },
        ];
        let seg = write_segment(&store, 0, 0, 100, &descs).await;
        let segments = vec![seg];
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // No erasure: both series present (baseline, so the assertion below is
        // about erasure, not a corpus that never held the series).
        let both =
            distributed_metric_names(Arc::clone(&store), segments.clone(), &snapshot, &[], 1).await;
        assert_eq!(
            both,
            vec!["erased".to_string(), "keep".to_string()],
            "without erasure both series are present"
        );

        // Windowless predicate on __name__="erased": the whole series is erased.
        let erasure = vec![ErasurePredicate::windowless(vec![(
            "__name__".to_string(),
            "erased".to_string(),
        )])];
        let kept =
            distributed_metric_names(Arc::clone(&store), segments, &snapshot, &erasure, 1).await;
        assert_eq!(
            kept,
            vec!["keep".to_string()],
            "the erased series must be absent from the distributed result"
        );
    });
}

// --- dedup tie-break chain across the wire ---------------------------------

/// Exercises every field of the ADR-0010 cross-segment dedup total order
/// -- `(created_unix_ns, writer_epoch, writer_seq, ...)` then the f64 value bit
/// pattern -- end to end across the wire. For each of four metrics, two
/// single-series segments carry the *same* `(series_id, ts)` duplicate and
/// differ in exactly one priority field; the field's winner is engineered to
/// carry the *smaller* value bit pattern, so if that field were dropped on the
/// wire (encoded as 0) the value tie-break would pick the other record and the
/// distributed result would diverge from local. Deleting `created_unix_ns`,
/// `writer_epoch`, or `writer_seq` from `encode_series_frame` (`codec.rs`)
/// therefore fails this differential (finding 9); the local path always uses
/// the real provenance, so only the wire-carried side changes.
/// One segment of a dedup tie-break pair: its provenance priority fields and
/// the value bit pattern it carries for a single sample at `ts=NS`.
struct TiebreakRecord {
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    value_bits: u64,
}

#[test]
fn dedup_tiebreak_chain_survives_the_wire() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        // Two segments per metric carry the same (series_id, ts) duplicate and
        // differ in exactly one priority field; the winner (higher priority)
        // carries the smaller value, so zeroing the deciding field on the wire
        // flips the winner to the loser.
        let hi = 9.0f64.to_bits(); // larger value bits
        let lo = 1.0f64.to_bits(); // smaller value bits
        let rec = |created_unix_ns, writer_epoch, writer_seq, value_bits| TiebreakRecord {
            created_unix_ns,
            writer_epoch,
            writer_seq,
            value_bits,
        };
        // created decides: equal epoch/seq, created 10 vs 20; winner=created 20.
        // epoch decides:   equal created/seq, epoch 1 vs 2;   winner=epoch 2.
        // seq decides:     equal created/epoch, seq 1 vs 2;    winner=seq 2.
        // value decides:   equal created/epoch/seq;            winner=hi value.
        let pairs: [(&str, [TiebreakRecord; 2]); 4] = [
            ("mCreated", [rec(10, 5, 5, hi), rec(20, 5, 5, lo)]),
            ("mEpoch", [rec(100, 1, 7, hi), rec(100, 2, 7, lo)]),
            ("mSeq", [rec(200, 3, 1, hi), rec(200, 3, 2, lo)]),
            ("mValue", [rec(300, 4, 6, lo), rec(300, 4, 6, hi)]),
        ];
        let mut segments = Vec::new();
        let mut key = 0u64;
        for (metric, records) in pairs {
            for r in records {
                let descs = vec![SeriesDesc {
                    metric: metric.to_string(),
                    samples: vec![(NS, r.value_bits)],
                }];
                segments.push(
                    write_segment_prov(
                        &store,
                        key,
                        r.created_unix_ns,
                        r.writer_epoch,
                        r.writer_seq,
                        0,
                        100,
                        &descs,
                    )
                    .await,
                );
                key += 1;
            }
        }
        // cap 1 forces all eight segments into one slice; the merge (and its
        // tie-break) runs over the full pool, identical to local.
        assert_distributed_matches_local(store, segments, 1).await;
    });
}

// --- coordinator budget re-enforcement -------------------------------------

/// ADR-0071: the coordinator re-enforces the series budget independently of any
/// per-slice budget a worker claims to honor. A worker that returns more
/// distinct series than `config.max_series` must fail the query with a typed
/// `TooManySeries`, not silently over-materialize.
#[test]
fn coordinator_reenforces_series_budget_over_honest_worker() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        // Five distinct series in one segment.
        let descs: Vec<SeriesDesc> = (0..5)
            .map(|i| SeriesDesc {
                metric: format!("m{i}"),
                samples: vec![(NS, (i as f64).to_bits())],
            })
            .collect();
        let seg = write_segment(&store, 0, 0, 100, &descs).await;
        let segments = vec![seg];
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments).await;
        let distributed = Distributed::new(
            Arc::new(fetcher),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
            },
        );
        // Cap below the worker's five series.
        let config = EngineConfig {
            max_series: 3,
            ..EngineConfig::default()
        };
        let accounting = QueryAccounting::new();
        let err = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                i64::MAX,
            )
            .await
            .expect_err("budget must trip");
        assert!(
            matches!(err, crate::error::QueryError::TooManySeries { max: 3, .. }),
            "expected TooManySeries, got {err:?}"
        );
        server.abort();
    });
}

/// ADR-0061/ADR-0071 worker-side budget: a worker enforces the request's
/// per-segment bytes-scanned budget itself (finding 1), tripping the moment a
/// completed segment fetch pushes the slice over, and returns a
/// `BudgetExceeded` summary that still carries the real accounting spent so far
/// (so the coordinator folds the cost before failing, not a lost double-spend).
/// Driving the worker directly (not through the coordinator) isolates the
/// worker's own check: deleting the `bytes_scanned_exceeded` block in
/// `run_slice_inner` (`service.rs`) makes the worker return `Ok` instead, and
/// the `BudgetExceeded` assertion below fails.
#[test]
fn worker_trips_bytes_budget_per_segment() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let descs = vec![SeriesDesc {
            metric: "m0".to_string(),
            samples: vec![(NS, 1.0f64.to_bits()), (2 * NS, 2.0f64.to_bits())],
        }];
        let seg = write_segment(&store, 0, 0, 100, &descs).await;
        let segments = vec![seg.clone()];
        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments).await;

        // A one-byte budget trips on the first completed segment fetch (a real
        // fetch always scans more than one byte).
        let request = pb::FetchRequest {
            protocol_version: crate::distrib::codec::PROTOCOL_VERSION,
            query_id: Vec::new(),
            tenant_hash: TENANT.0.to_vec(),
            signal: crate::distrib::codec::signal_to_u32(Signal::Metrics),
            scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
                segments: vec![crate::distrib::codec::encode_segment_identity(&seg)],
            })),
            matchers: Vec::new(),
            window_start_ns: 0,
            window_end_ns: 0,
            budgets: Some(pb::Budgets {
                max_series: u64::MAX,
                max_samples: u64::MAX,
                max_bytes_scanned: 1,
                max_segments: u64::MAX,
            }),
            deadline_unix_ns: 0,
            erasure: Vec::new(),
            trace_context: String::new(),
            fragment_capability: Vec::new(),
        };
        let response = SliceFetcher::fetch(&fetcher, request)
            .await
            .expect("worker responds");
        server.abort();
        assert_eq!(
            response.status,
            pb::status::Code::BudgetExceeded,
            "worker must trip its own per-segment bytes budget"
        );
        assert!(
            response.accounting.total_s3_bytes() > 0,
            "the BudgetExceeded summary must carry the real spend, not zeros"
        );
    });
}

/// A [`SliceFetcher`] double that reports `Ok` while claiming (via its
/// accounting snapshot) to have scanned `spend_bytes` -- a worker that
/// under-enforces or lies about its own budget. Used to prove the coordinator
/// re-enforces the bytes-scanned cap independently (finding 2).
struct LyingBudgetWorker {
    spend_bytes: u64,
}

#[async_trait::async_trait]
impl SliceFetcher for LyingBudgetWorker {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let acct = QueryAccounting::new();
        acct.add_s3_bytes(ravel_types::accounting::AccountedOp::Get, self.spend_bytes);
        Ok(SliceResponse {
            scalar: Vec::new(),
            accounting: acct.snapshot(),
            stats: crate::fetcher::FetchStats::default(),
            series_returned: 0,
            samples_returned: 0,
            status: pb::status::Code::Ok,
            status_message: String::new(),
        })
    }
}

/// ADR-0071: the coordinator re-enforces the bytes-scanned budget over the
/// folded per-slice accounting, so a worker that returns `Ok` while reporting a
/// spend above the query's cap still fails the query with the typed
/// `TooManyBytesScanned` -- a distributed query is bounded as tightly as a local
/// one even if a worker under-reports its own trip (finding 2). Deleting the
/// coordinator's `bytes_scanned_exceeded` check in the `Ok` arm (`mod.rs`) lets
/// the over-spend through as `Ok(Some(..))` and the `expect_err` below fails.
#[test]
fn coordinator_reenforces_bytes_budget_over_lying_worker() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let seg = write_segment(
            &store,
            0,
            0,
            100,
            &[SeriesDesc {
                metric: "m0".to_string(),
                samples: vec![(NS, 1.0f64.to_bits())],
            }],
        )
        .await;
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let distributed = Distributed::new(
            Arc::new(LyingBudgetWorker {
                spend_bytes: 10_000,
            }),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
            },
        );
        let config = EngineConfig {
            max_bytes_scanned: crate::config::ByteLimit::Bounded(100),
            ..EngineConfig::default()
        };
        let accounting = QueryAccounting::new();
        let err = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                i64::MAX,
            )
            .await
            .expect_err("coordinator must re-enforce the bytes budget");
        assert!(
            matches!(err, crate::error::QueryError::TooManyBytesScanned { .. }),
            "expected TooManyBytesScanned, got {err:?}"
        );
        // The lying worker's spend was still folded into the query's reported
        // cost before the failure (never silently dropped).
        assert!(
            accounting.snapshot().total_s3_bytes() >= 10_000,
            "the folded spend must survive on the live accounting handle"
        );
    });
}

// --- snapshot invalidation collapses to one retryable error ----------------

/// A [`SliceFetcher`] double that returns a `SnapshotInvalidated` summary for
/// every slice and counts its calls, so a test can assert the coordinator maps
/// N invalidated slices to exactly one retryable error rather than one per
/// slice.
struct AlwaysInvalidated {
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl SliceFetcher for AlwaysInvalidated {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(SliceResponse {
            scalar: Vec::new(),
            accounting: ravel_types::accounting::QueryAccountingSnapshot::default(),
            stats: crate::fetcher::FetchStats::default(),
            series_returned: 0,
            samples_returned: 0,
            status: pb::status::Code::SnapshotInvalidated,
            status_message: "segment vanished".to_string(),
        })
    }
}

/// Multiple slices reporting `SnapshotInvalidated` collapse to a single
/// `Fetch(Store { NotFound })` -- the exact error `resolve_snapshot_with_retry`
/// keys on -- so the engine re-resolves the whole query once, never once per
/// slice. (The single whole-query re-resolve itself is covered end-to-end by
/// `engine::tests::distributed_snapshot_invalidation_reresolves_once`.)
#[test]
fn many_invalidated_slices_map_to_one_retryable_error() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        // Three shards => three slices at cap 3, all invalidated.
        let mut segments = Vec::new();
        for shard in 0..3u32 {
            segments.push(
                write_segment(
                    &store,
                    u64::from(shard),
                    shard,
                    100,
                    &[SeriesDesc {
                        metric: format!("m{shard}"),
                        samples: vec![(NS, 1.0f64.to_bits())],
                    }],
                )
                .await,
            );
        }
        let snapshot = Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let distributed = Distributed::new(
            Arc::new(AlwaysInvalidated {
                calls: Arc::clone(&calls),
            }),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 3,
            },
        );
        let accounting = QueryAccounting::new();
        let err = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &EngineConfig::default(),
                i64::MAX,
            )
            .await
            .expect_err("invalidation must surface as an error");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "all slices dispatched once"
        );
        assert!(
            matches!(
                err,
                crate::error::QueryError::Fetch(crate::fetcher::FetchError::Store {
                    source: ravel_object_store::StoreError::NotFound,
                    ..
                })
            ),
            "expected the single retryable Store(NotFound), got {err:?}"
        );
    });
}

/// ADR-0071 protocol: the worker validates `request.signal` before touching
/// storage. A known non-metrics signal (logs, spans, ...) is answered with an
/// `Unsupported` summary so the coordinator falls back to the local path, and
/// an unknown discriminant is `BadData` (a broken or newer peer, not a
/// capability gap). Deleting the signal-validation block in `run_slice_inner`
/// (`service.rs`) makes both requests fetch and return `Ok` scalar frames, and
/// the status assertions below fail.
#[test]
fn worker_rejects_non_metrics_and_unknown_signals() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let seg = write_segment(
            &store,
            0,
            0,
            100,
            &[SeriesDesc {
                metric: "m0".to_string(),
                samples: vec![(NS, 1.0f64.to_bits())],
            }],
        )
        .await;
        let (fetcher, server) = spawn_worker(Arc::clone(&store), vec![seg.clone()]).await;

        let request = |signal: u32| pb::FetchRequest {
            protocol_version: crate::distrib::codec::PROTOCOL_VERSION,
            query_id: Vec::new(),
            tenant_hash: TENANT.0.to_vec(),
            signal,
            scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
                segments: vec![crate::distrib::codec::encode_segment_identity(&seg)],
            })),
            matchers: Vec::new(),
            window_start_ns: 0,
            window_end_ns: 0,
            budgets: None,
            deadline_unix_ns: 0,
            erasure: Vec::new(),
            trace_context: String::new(),
            fragment_capability: Vec::new(),
        };

        let logs = SliceFetcher::fetch(
            &fetcher,
            request(crate::distrib::codec::signal_to_u32(Signal::Logs)),
        )
        .await
        .expect("worker responds to a logs request");
        assert_eq!(
            logs.status,
            pb::status::Code::Unsupported,
            "a known non-metrics signal must be Unsupported, not silently answered with metrics"
        );

        let unknown = SliceFetcher::fetch(&fetcher, request(u32::MAX))
            .await
            .expect("worker responds to an unknown discriminant");
        assert_eq!(
            unknown.status,
            pb::status::Code::BadData,
            "an unknown signal discriminant must be BadData"
        );
        server.abort();
    });
}

/// #283: `run_slice_inner` dispatches on the signal via a real per-signal match
/// arm reached AFTER decoding tenant/matchers/erasure, not the former
/// pre-decode blanket rejection. Logs and Spans route to their own branch (a
/// stub returning a distinct "not yet implemented" Unsupported until #284/#285
/// fill it in); Profiles stays rejected exactly as every non-Metrics signal was
/// before #283.
///
/// Two facts here would each FAIL against the old blanket-rejection code:
///  1. Logs/Spans now carry the distinct "not yet implemented" message; the old
///     code produced "not distributed yet; only Metrics" for every non-Metrics
///     signal, so it never distinguished the stubbed family from Profiles.
///  2. A Logs request with a malformed `tenant_hash` now returns `BadData`
///     (tenant decode runs before the signal dispatch); the old code returned
///     `Unsupported` because the signal check preceded any decode.
#[test]
fn run_slice_inner_dispatches_on_signal_not_blanket_rejects() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let seg = write_segment(
            &store,
            0,
            0,
            100,
            &[SeriesDesc {
                metric: "m0".to_string(),
                samples: vec![(NS, 1.0f64.to_bits())],
            }],
        )
        .await;
        let (fetcher, server) = spawn_worker(Arc::clone(&store), vec![seg.clone()]).await;

        let request = |signal: u32, tenant: Vec<u8>| pb::FetchRequest {
            protocol_version: crate::distrib::codec::PROTOCOL_VERSION,
            query_id: Vec::new(),
            tenant_hash: tenant,
            signal,
            scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
                segments: vec![crate::distrib::codec::encode_segment_identity(&seg)],
            })),
            matchers: Vec::new(),
            window_start_ns: 0,
            window_end_ns: 0,
            budgets: None,
            deadline_unix_ns: 0,
            erasure: Vec::new(),
            trace_context: String::new(),
            fragment_capability: Vec::new(),
        };

        // Logs and Spans reach the new stub branch: Unsupported, but with the
        // distinct "not yet implemented" message the old code never produced.
        for signal in [Signal::Logs, Signal::Spans] {
            let resp = SliceFetcher::fetch(
                &fetcher,
                request(
                    crate::distrib::codec::signal_to_u32(signal),
                    TENANT.0.to_vec(),
                ),
            )
            .await
            .unwrap_or_else(|e| panic!("worker responds to a {signal:?} request: {e:?}"));
            assert_eq!(
                resp.status,
                pb::status::Code::Unsupported,
                "{signal:?} stub is still Unsupported so the coordinator falls back to local"
            );
            assert!(
                resp.status_message.contains("not yet implemented"),
                "{signal:?} must reach the new per-signal stub branch (distinct message), \
                 got {:?}",
                resp.status_message
            );
        }

        // Structural proof the signal check now runs AFTER decoding: a Logs
        // request with a malformed tenant hash is BadData (decode failed), not
        // the Unsupported the pre-decode blanket check would have returned.
        let bad_tenant = SliceFetcher::fetch(
            &fetcher,
            request(
                crate::distrib::codec::signal_to_u32(Signal::Logs),
                vec![0u8; 3],
            ),
        )
        .await
        .expect("worker responds to a malformed-tenant logs request");
        assert_eq!(
            bad_tenant.status,
            pb::status::Code::BadData,
            "tenant decode runs before the signal dispatch, so a bad tenant is BadData"
        );

        // Profiles stays on the reject arm, exactly as before #283: Unsupported,
        // and specifically NOT the stubbed-family "not yet implemented" message.
        let profiles = SliceFetcher::fetch(
            &fetcher,
            request(
                crate::distrib::codec::signal_to_u32(Signal::Profiles),
                TENANT.0.to_vec(),
            ),
        )
        .await
        .expect("worker responds to a profiles request");
        assert_eq!(
            profiles.status,
            pb::status::Code::Unsupported,
            "Profiles keeps returning Unsupported like every non-Metrics signal did"
        );
        assert!(
            !profiles.status_message.contains("not yet implemented"),
            "Profiles must take the reject arm, not the stubbed-family branch, got {:?}",
            profiles.status_message
        );

        server.abort();
    });
}

/// A [`SliceFetcher`] double whose reported spend is keyed on the slice's
/// shard, with the overflow-sized report delayed so it always completes (and
/// folds) second -- the one completion order a wrapping fold is blind to.
struct OverflowingLyingWorker;

#[async_trait::async_trait]
impl SliceFetcher for OverflowingLyingWorker {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let shard = match &request.scope {
            Some(pb::fetch_request::Scope::Pinned(p)) => p.segments[0].shard,
            _ => panic!("pinned scope expected"),
        };
        let spend = if shard == 0 {
            500
        } else {
            // Delay so the honest-looking small report folds first: 500
            // wrapping_add (u64::MAX - 100) == 399, under the cap, which is
            // exactly the blindness this test exists to rule out.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            u64::MAX - 100
        };
        let acct = QueryAccounting::new();
        acct.add_s3_bytes(ravel_types::accounting::AccountedOp::Get, spend);
        Ok(SliceResponse {
            scalar: Vec::new(),
            accounting: acct.snapshot(),
            stats: crate::fetcher::FetchStats::default(),
            series_returned: 0,
            samples_returned: 0,
            status: pb::status::Code::Ok,
            status_message: String::new(),
        })
    }
}

/// The coordinator's per-slice fold must SATURATE, not wrap (finding 3): two
/// slices reporting 500 and `u64::MAX - 100` bytes wrap to 399 under a
/// `wrapping_add` fold -- below the 1_000-byte cap, so the incremental check
/// never trips and the query sails through. Under `saturating_merge` the fold
/// clamps to `u64::MAX` and trips `TooManyBytesScanned` on the second slice.
/// This drives `Distributed::fetch` directly, so the engine's final backstop
/// (which saturates independently) cannot mask the coordinator fold: swapping
/// `saturating_merge` back to a wrapping per-field add makes this test fail.
/// The worker delays the overflow-sized report so it always folds second,
/// making the wrap-blind completion order deterministic.
#[test]
fn coordinator_fold_saturates_overflowing_worker_reports() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let mut segments = Vec::new();
        for shard in 0..2u32 {
            segments.push(
                write_segment(
                    &store,
                    u64::from(shard),
                    shard,
                    100,
                    &[SeriesDesc {
                        metric: format!("m{shard}"),
                        samples: vec![(NS, 1.0f64.to_bits())],
                    }],
                )
                .await,
            );
        }
        let snapshot = Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let distributed = Distributed::new(
            Arc::new(OverflowingLyingWorker),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 2,
            },
        );
        let config = EngineConfig {
            max_bytes_scanned: crate::config::ByteLimit::Bounded(1_000),
            ..EngineConfig::default()
        };
        let accounting = QueryAccounting::new();
        let err = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                i64::MAX,
            )
            .await
            .expect_err("a wrapped fold would let the overflowing report through");
        assert!(
            matches!(err, crate::error::QueryError::TooManyBytesScanned { .. }),
            "expected TooManyBytesScanned, got {err:?}"
        );
    });
}

/// ADR-0071 scalar-only distribution (finding 5): a histogram-bearing slice is
/// handed back to the coordinator as `Unsupported` -- but its S3 spend is real,
/// so the worker's summary carries the slice's true accounting and stats, and
/// the coordinator folds them into the query's accounting handle BEFORE
/// signalling local fallback (`Ok(None)`). Without the fold, a histogram query
/// pays for the distributed fetch twice and reports it once. The fold this
/// pins is the precondition for the documented engine consequence (the local
/// fallback re-enforces `max_bytes_scanned` against a handle carrying the
/// remote spend -- see the fallthrough comment in `engine.rs`).
/// Deleting the worker's `any_histograms` Unsupported branch, or the
/// coordinator's pre-fallback fold, fails the assertions below.
#[test]
fn histogram_slice_falls_back_with_spend_folded() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        use ravel_segment::{
            HistogramCounts, HistogramSample, HistogramValue, ResetHint, SeriesInputV3,
            SeriesValues,
        };

        let store = Arc::new(MemoryStore::new());
        let metric = "h0";
        let label_set = labels(metric);
        let series_id = SeriesId::compute(&tenant_id(), metric, &label_set).expect("series id");
        let hist = HistogramValue {
            scale: 0,
            zero_threshold: 0.0,
            sum: Some(1.0),
            custom_values: None,
            positive_spans: vec![ravel_segment::HistogramSpan {
                offset: 0,
                length: 1,
            }],
            negative_spans: Vec::new(),
            counts: HistogramCounts::Int {
                zero_count: 0,
                count: 1,
                positive: vec![1],
                negative: Vec::new(),
            },
            reset_hint: ResetHint::Unknown,
        };
        let identity = SegmentIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: Uuid::from_u128(1).to_string(),
            writer_epoch: 1,
            writer_seq: 0,
        };
        let written = SegmentWriter::write_histograms(
            vec![SeriesInputV3 {
                series_id,
                labels: label_set,
                values: SeriesValues::Histogram(vec![HistogramSample {
                    ts_ns: NS,
                    value: hist,
                }]),
            }],
            identity,
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write histogram segment");
        let object_key = "seg/hist0.rseg".to_string();
        store
            .put(&object_key, written.bytes.clone(), PutOptions::default())
            .await
            .expect("put segment");
        let seg = SegmentRef {
            data_object_key: object_key,
            object_size: written.bytes.len() as u64,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            ingest_hour_bucket: 100,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            shard: 0,
            content_hash: written.summary.blake3,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 0,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
        };
        let snapshot = Snapshot {
            segments: vec![seg.clone()],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        let (fetcher, server) = spawn_worker(Arc::clone(&store), vec![seg]).await;
        let distributed = Distributed::new(
            Arc::new(fetcher),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
            },
        );
        let accounting = QueryAccounting::new();
        let result = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &EngineConfig::default(),
                i64::MAX,
            )
            .await
            .expect("unsupported must not be an error");
        assert!(
            result.is_none(),
            "a histogram-bearing snapshot signals local fallback (Ok(None))"
        );
        assert!(
            accounting.snapshot().total_s3_bytes() > 0,
            "the worker's real spend must be folded into the query accounting before fallback"
        );
        server.abort();
    });
}
