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
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
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

use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_types::logstream::{LogStreamId, log_stream_id};

use crate::config::EngineConfig;
use crate::distrib::Distributed;
use crate::distrib::client::{DistribError, RemoteSliceFetcher, SliceFetcher, SliceResponse};
use crate::distrib::partition::{DistribThresholds, partition_snapshot};
use crate::distrib::{
    log_record_order_key, service::SeriesFetchService, service::SnapshotSegmentResolver,
    span_order_key,
};
use crate::engine::merge_soa_runs;
use crate::erasure::{ErasurePredicate, is_erased_span};
use crate::fetcher::SegmentFetcher;
use crate::log_fetcher::{LogQuery, LogSegmentFetcher};
use crate::span_fetcher::{SpanRow, SpanSegmentFetcher};

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
    // Wire both the metric and the RLOG-family fetch path over the same store,
    // so one worker serves Metrics and Logs/Alerts/Audit slices.
    let log_store: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let log_fetcher = LogSegmentFetcher::new(log_store);
    let metrics_store: Arc<dyn ObjectStoreBackend> = store as Arc<dyn ObjectStoreBackend>;
    spawn_worker_with_log_fetcher(metrics_store, log_fetcher, segments).await
}

/// `spawn_worker` with an explicitly built (possibly cache-wired) log fetcher, so
/// a test can prove the worker's log path is cache-aware.
async fn spawn_worker_with_log_fetcher(
    metrics_store: Arc<dyn ObjectStoreBackend>,
    log_fetcher: LogSegmentFetcher,
    segments: Vec<SegmentRef>,
) -> (RemoteSliceFetcher, JoinHandle<()>) {
    // Wire the span fetch path (#285) over the same store, so one worker also
    // serves Spans slices. Harmless to the metric/log tests: it is only reached
    // on a `Signal::Spans` request.
    let span_fetcher = SpanSegmentFetcher::new(Arc::clone(&metrics_store));
    let fetcher = SegmentFetcher::new(metrics_store);
    let resolver = Arc::new(SnapshotSegmentResolver::new(segments));
    let service = SeriesFetchService::new(fetcher, resolver)
        .with_log_fetcher(log_fetcher)
        .with_span_fetcher(span_fetcher)
        .into_server();

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

/// ADR-0071 protocol: the worker dispatches on `request.signal` after decoding.
/// Profiles has no distributed path, so it is answered with an `Unsupported`
/// summary so the coordinator falls back to the local path, and an unknown
/// discriminant is `BadData` (a broken or newer peer, not a capability gap). The
/// RLOG family (Logs/Alerts/Audit, #284) and Spans (#285) are now served, so
/// those requests reach a real fetch path rather than a blanket rejection.
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

        // Spans is now served (#285): the request reaches the real span path.
        // This RSEG-only corpus has no spans in the [0,0] window, so the span
        // fetcher's ts-relevance check skips the object (no GET, no RSPAN decode
        // of the RSEG bytes) and the slice returns an empty Ok summary -- proving
        // Spans is dispatched to a real path, not the former blanket rejection.
        let spans = SliceFetcher::fetch(
            &fetcher,
            request(crate::distrib::codec::signal_to_u32(Signal::Spans)),
        )
        .await
        .expect("worker responds to a spans request");
        assert_eq!(
            spans.status,
            pb::status::Code::Ok,
            "Spans is now served, not rejected"
        );

        // Logs is now served (#284): the request reaches the real RLOG path.
        // This RSEG-only corpus has no records in the [0,0] window, so the log
        // fetcher skips it and the slice returns an empty Ok summary -- proving
        // Logs is dispatched to a real path, not the former blanket rejection.
        let logs = SliceFetcher::fetch(
            &fetcher,
            request(crate::distrib::codec::signal_to_u32(Signal::Logs)),
        )
        .await
        .expect("worker responds to a logs request");
        assert_eq!(
            logs.status,
            pb::status::Code::Ok,
            "the RLOG family is now served, not rejected"
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
/// pre-decode blanket rejection. Logs (#284) and Spans (#285) now route to their
/// real fetch paths; Profiles stays rejected exactly as every non-Metrics signal
/// was before #283.
///
/// Two facts here would each FAIL against the old blanket-rejection code:
///  1. Logs/Spans reach a real path (served Ok on this empty-window corpus), and
///     Profiles keeps the reject arm; the old code produced one identical
///     "not distributed yet; only Metrics" for every non-Metrics signal, so it
///     never distinguished a served signal from Profiles.
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

        // Spans now dispatches to the real span path (#285), no longer a stub.
        // The RSEG-only corpus has no spans in the [0,0] window, so the span
        // fetcher skips it and the slice returns an empty Ok summary. The key
        // point is that the status is served, not the old "not yet implemented"
        // stub.
        let spans = SliceFetcher::fetch(
            &fetcher,
            request(
                crate::distrib::codec::signal_to_u32(Signal::Spans),
                TENANT.0.to_vec(),
            ),
        )
        .await
        .expect("worker responds to a spans request");
        assert_eq!(
            spans.status,
            pb::status::Code::Ok,
            "Spans is served by the real path, not the stub"
        );
        assert!(
            !spans.status_message.contains("not yet implemented"),
            "Spans must not take a stub branch, got {:?}",
            spans.status_message
        );

        // Logs now dispatches to the real RLOG path (#284), no longer a stub. The
        // RSEG-only corpus has no records in the [0,0] window, so the log fetcher
        // skips it and the slice returns an empty Ok summary. The key point is
        // that the status is not the "not yet implemented" stub.
        let logs = SliceFetcher::fetch(
            &fetcher,
            request(
                crate::distrib::codec::signal_to_u32(Signal::Logs),
                TENANT.0.to_vec(),
            ),
        )
        .await
        .expect("worker responds to a logs request");
        assert_eq!(
            logs.status,
            pb::status::Code::Ok,
            "Logs is served by the real path, not the stub"
        );
        assert!(
            !logs.status_message.contains("not yet implemented"),
            "Logs must not take a stub branch, got {:?}",
            logs.status_message
        );

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

// --- RLOG-family distributed fan-out (#284) --------------------------------
//
// The centerpiece is `run_log_differential`: over a real loopback worker, a
// distributed Logs/Alerts/Audit fetch merged by the coordinator equals, as a
// multiset under the stated total order, a raw local `LogSegmentFetcher` read
// over the same segments -- concatenated directly, NOT through
// `merge_log_records`, so the reference is independent of the function under
// test and the differential can actually catch a merge regression. This
// includes a corpus where one stream's segments straddle two slices (a
// reshard-activation window). The test proves the codec preserves every record
// and the partition is total -- and, on the split corpus, that the coordinator
// ORDERS rather than concatenates in slice order. Duplicate-record preservation
// (no query-time dedup for logs) is pinned separately by
// `logs_coordinator_preserves_duplicate_records_across_slices`.

/// Resource attributes and the derived stream identity/blob for a named
/// service. Records sharing a service name share one `LogStreamId`, so a corpus
/// can place one stream's records in segments under different shards (the
/// reshard-straddle case).
fn log_stream(service: &str) -> (LogStreamId, Vec<u8>) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(service.to_string()),
    )];
    let id = log_stream_id(&resource, "scope", "1.0", &[]);
    let blob = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    (id, blob)
}

/// One log record on `service`'s stream at event time `ts`, with `body` and the
/// given per-record string attributes.
fn log_record(service: &str, ts: i64, body: &str, attrs: &[(&str, &str)]) -> LogRecord {
    let (stream_id, stream_attrs) = log_stream(service);
    LogRecord {
        stream_id,
        stream_attrs,
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs
            .iter()
            .map(|(k, v)| ((*k).to_string(), AttrValue::Str((*v).to_string())))
            .collect(),
    }
}

/// Writes one RLOG object holding `records` under `shard`/`hour`, returning an
/// L0 `SegmentRef` with a distinct content hash (blake3 of the bytes) so the
/// worker's content-hash resolver keys each segment uniquely.
async fn write_log_segment(
    store: &MemoryStore,
    key: u64,
    shard: u32,
    hour: u32,
    records: &[LogRecord],
) -> SegmentRef {
    // Small blocks so a multi-block object is exercised; this never changes the
    // record set returned.
    let cfg = RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: TENANT.0,
        shard,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: key,
    };
    let mut writer = RlogWriter::new(cfg, identity);
    for r in records {
        writer.push(r.clone()).expect("push record");
    }
    let bytes = writer.finish().expect("finish object");
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let size = bytes.len() as u64;
    let object_key = format!("logs/{key}.rlog");
    store
        .put(
            &object_key,
            bytes::Bytes::from(bytes),
            PutOptions::default(),
        )
        .await
        .expect("put log object");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: object_key,
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: hour,
        sample_count: records.len() as u64,
        series_count: 0,
        shard,
        content_hash,
        writer_id: Uuid::from_u128(u128::from(key) + 1),
        writer_epoch: 1,
        writer_seq: key,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// The whole-snapshot event window: `[min over all segments, max over all]`, a
/// superset of every segment's own span, so a local read over it prunes nothing.
fn whole_window(segments: &[SegmentRef]) -> (i64, i64) {
    let min = segments
        .iter()
        .map(|s| s.min_event_ts_ns)
        .min()
        .unwrap_or(i64::MIN);
    let max = segments
        .iter()
        .map(|s| s.max_event_ts_ns)
        .max()
        .unwrap_or(i64::MAX);
    (min, max)
}

/// The local reference for logs: read every segment with `LogSegmentFetcher`
/// over the whole-snapshot window (a superset of each segment) and concatenate
/// the per-segment records directly, with NO merge or dedup. This is exactly the
/// multiset of records a local multi-segment read observes, and it is
/// deliberately independent of `merge_log_records` (the function under test) so
/// the differential compares distributed output against a reference the function
/// cannot influence. Callers compare via [`sorted_by_order_key`] as a multiset
/// (duplicates preserved on both sides).
async fn local_log_records(
    store: Arc<MemoryStore>,
    segments: &[SegmentRef],
    erasure: &[ErasurePredicate],
) -> Vec<LogRecord> {
    let fetcher = LogSegmentFetcher::new(store);
    let (min, max) = whole_window(segments);
    let query = LogQuery::new(min, max).with_erasure(erasure.to_vec());
    let mut all = Vec::new();
    for seg in segments {
        let out = fetcher.fetch(seg, &query).await.expect("local log fetch");
        all.extend(out.map(|o| o.records).unwrap_or_default());
    }
    all
}

/// Stable-sort a record multiset under the production total-order key, preserving
/// duplicates. Applying this to both sides of a differential turns an
/// order-sensitive `Vec` equality into a multiset equality under the stated
/// order, without ever calling `merge_log_records`.
fn sorted_by_order_key(mut records: Vec<LogRecord>) -> Vec<LogRecord> {
    records.sort_by_key(log_record_order_key);
    records
}

/// The distributed reference for logs: dispatch `signal` over a real loopback
/// worker at width `cap`, merged by the coordinator's `fetch_logs`.
async fn distributed_log_records(
    store: Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
    snapshot: &Snapshot,
    signal: Signal,
    erasure: &[ErasurePredicate],
    cap: usize,
) -> Vec<LogRecord> {
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
    let records = distributed
        .fetch_logs(
            TENANT,
            signal,
            snapshot,
            &[],
            erasure,
            &accounting,
            &EngineConfig::default(),
            i64::MAX,
        )
        .await
        .expect("distributed log fetch")
        .expect("distributed produced a result (not a fallback)");
    server.abort();
    records
}

/// The split corpus: two segments under two DIFFERENT shards (a reshard-
/// activation window), each carrying records of BOTH the `alpha` and `beta`
/// streams. Because the corpus has exactly two shards, `cap = 2` puts each
/// segment in its own slice (shard-major partitioning), so both streams straddle
/// the two slices: `alpha`'s and `beta`'s records live on both sides. Their
/// timestamps interleave across the two segments, so a coordinator that
/// concatenated slice results in arrival order would misorder them.
///
/// Per-record `user_id`: u1 at ts {10, 15, 20}, u2 at ts {25, 30, 40}. Erasing
/// u1 therefore leaves {25, 30, 40}, and the erased rows straddle the two slices.
async fn split_stream_corpus(store: &MemoryStore) -> Vec<SegmentRef> {
    // seg_a shard 0: alpha ts 10 (u1), 40 (u2); beta ts 15 (u1).
    // seg_b shard 1: alpha ts 20 (u1), 30 (u2); beta ts 25 (u2).
    // Global ts order 10,15,20,25,30,40 interleaves the two shards, so slice-order
    // concatenation (all of seg_a's ts before all of seg_b's) is NOT sorted.
    let seg_a = write_log_segment(
        store,
        0,
        0,
        100,
        &[
            log_record("alpha", 10, "a-early", &[("user_id", "u1")]),
            log_record("alpha", 40, "a-late", &[("user_id", "u2")]),
            log_record("beta", 15, "b-early", &[("user_id", "u1")]),
        ],
    )
    .await;
    let seg_b = write_log_segment(
        store,
        1,
        1,
        101,
        &[
            log_record("alpha", 20, "a-mid1", &[("user_id", "u1")]),
            log_record("alpha", 30, "a-mid2", &[("user_id", "u2")]),
            log_record("beta", 25, "b-mid", &[("user_id", "u2")]),
        ],
    )
    .await;
    vec![seg_a, seg_b]
}

/// Drives the per-signal differential: distributed == local, over both a
/// single-slice (`cap = 1`) and a stream-straddling two-slice (`cap = 2`)
/// partition of the same split corpus. On the two-slice case it also proves the
/// corpus is discriminating: a naive concatenate-in-slice-order coordinator
/// would emit an order the local read never produces, so the `sort_by` line in
/// `merge_log_records` is the line under test.
async fn run_log_differential(signal: Signal) {
    let store = Arc::new(MemoryStore::new());
    let segments = split_stream_corpus(&store).await;
    let snapshot = Snapshot {
        segments: segments.clone(),
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };

    // Single-slice: every segment in one slice. Distributed == local.
    let local = local_log_records(Arc::clone(&store), &segments, &[]).await;
    let one_slice = distributed_log_records(
        Arc::clone(&store),
        segments.clone(),
        &snapshot,
        signal,
        &[],
        1,
    )
    .await;
    assert_eq!(
        sorted_by_order_key(one_slice),
        sorted_by_order_key(local.clone()),
        "{signal:?}: single-slice distributed must equal local"
    );

    // Two-slice straddle: confirm the partition genuinely places each segment in
    // its own slice, so both streams' segments straddle two slices (not the easy
    // single-slice case). One segment per slice, two slices, and each segment
    // carries alpha and beta records, so both streams are split across slices.
    let slices = partition_snapshot(&snapshot, 2);
    assert_eq!(
        slices.len(),
        2,
        "{signal:?}: the reshard-straddle corpus must produce two slices"
    );
    assert_eq!(
        slices[0].segments.len(),
        1,
        "{signal:?}: slice 0 must hold exactly one segment, so the streams straddle"
    );
    assert_eq!(
        slices[1].segments.len(),
        1,
        "{signal:?}: slice 1 must hold exactly one segment, so the streams straddle"
    );

    let two_slice = distributed_log_records(
        Arc::clone(&store),
        segments.clone(),
        &snapshot,
        signal,
        &[],
        2,
    )
    .await;
    assert_eq!(
        sorted_by_order_key(two_slice.clone()),
        sorted_by_order_key(local),
        "{signal:?}: two-slice distributed must equal local even when a stream straddles slices"
    );

    // Prove-the-test: the naive wrong coordinator concatenates each slice's local
    // read in slice order. On this corpus that order is observably NOT the global
    // total order the correct merge produces, so the differential is not vacuous.
    let ref_store: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let ref_fetcher = LogSegmentFetcher::new(ref_store);
    let (min, max) = whole_window(&segments);
    let full = LogQuery::new(min, max);
    let mut naive_ts = Vec::new();
    for slice in &slices {
        for seg in &slice.segments {
            let out = ref_fetcher
                .fetch(seg, &full)
                .await
                .expect("fetch")
                .expect("in range");
            naive_ts.extend(out.records.iter().map(|r| r.ts_ns));
        }
    }
    let correct_ts: Vec<i64> = two_slice.iter().map(|r| r.ts_ns).collect();
    assert_ne!(
        naive_ts, correct_ts,
        "{signal:?}: slice-order concatenation must differ from the correct merge, else the test is vacuous"
    );
    // The correct order is the globally sorted timestamps: 10,15,20,25,30,40.
    assert_eq!(
        correct_ts,
        vec![10, 15, 20, 25, 30, 40],
        "{signal:?}: the merge must emit the global cross-segment ts order"
    );
}

#[test]
fn logs_distributed_matches_local_including_reshard_straddle() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(run_log_differential(Signal::Logs));
}

#[test]
fn alerts_distributed_matches_local_including_reshard_straddle() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(run_log_differential(Signal::Alerts));
}

#[test]
fn audit_distributed_matches_local_including_reshard_straddle() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(run_log_differential(Signal::Audit));
}

/// Two byte-identical records split across two segments/slices must BOTH survive
/// the coordinator's merge. Per docs/consistency-model.md ("logs and spans"),
/// logs/alerts/audit have NO query-time dedup: a retry after a lost ack produces
/// byte-identical rows that are legitimately duplicate user data and must stay
/// visible, so collapsing them is silent data loss. This is the focused
/// minimal-repro for the dedup bug that shipped in #284 (`merge_log_records`
/// applied the metric-path `(series_id, ts)` dedup where it is forbidden).
#[test]
fn logs_coordinator_preserves_duplicate_records_across_slices() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        // The SAME record (same stream, ts, body, attrs) written into two
        // segments under two shards -> two slices at cap 2. A raw local read of
        // the two segments returns TWO records; the distributed merge must too.
        let dup = log_record("gamma", 50, "dup", &[("k", "v")]);
        let seg_a = write_log_segment(&store, 0, 0, 100, std::slice::from_ref(&dup)).await;
        let seg_b = write_log_segment(&store, 1, 1, 100, std::slice::from_ref(&dup)).await;
        let segments = vec![seg_a, seg_b];
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // Confirm the two duplicate records genuinely land in two different
        // slices, not the easy single-slice case where any merge would trivially
        // preserve both.
        let slices = partition_snapshot(&snapshot, 2);
        assert_eq!(
            slices.len(),
            2,
            "the two segments must partition into two slices for this to be a
             cross-slice duplicate, not a within-slice one"
        );

        // Reference: a raw local read of both segments, independent of the merge.
        let local = local_log_records(Arc::clone(&store), &segments, &[]).await;
        assert_eq!(
            local.len(),
            2,
            "a raw local read of the two segments returns both duplicate records"
        );

        let distributed = distributed_log_records(
            Arc::clone(&store),
            segments,
            &snapshot,
            Signal::Logs,
            &[],
            2,
        )
        .await;
        assert_eq!(
            distributed.len(),
            2,
            "the coordinator must preserve both cross-slice duplicate records, not collapse them"
        );
        assert_eq!(
            sorted_by_order_key(distributed),
            sorted_by_order_key(local),
            "the distributed result is the same multiset as the raw local read"
        );
    });
}

/// The worker's log fetch path is cache-aware: it goes through
/// [`LogSegmentFetcher::fetch_accounted_with_tenant`] (ADR-0046's read cache),
/// not the cache-blind `fetch_accounted`. Proven by wiring a cache into the
/// worker's fetcher over a `FaultStore` that fails the SECOND store GET of the
/// segment object. The first distributed fetch is a cache miss (one real GET,
/// which populates the cache); the second must be served from the cache with no
/// further GET, so the scripted second-GET fault never fires. With the
/// cache-blind funnel the second fetch would GET again and trip the fault; the
/// worker then returns a hard error for that slice rather than silently
/// substituting stale or partial data, and the coordinator surfaces it as a
/// `QueryError::Distrib` -- exactly the wiring gap this fixes.
#[test]
fn logs_worker_serves_repeat_reads_from_the_read_cache() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        // Write the segment, then wrap a snapshot of that store in a FaultStore.
        let seed = MemoryStore::new();
        let records = [
            log_record("gamma", 10, "one", &[("k", "v")]),
            log_record("gamma", 20, "two", &[("k", "v")]),
        ];
        let seg = write_log_segment(&seed, 0, 0, 100, &records).await;
        let segments = vec![seg];
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // Fail the SECOND GET of any object; the first (cache-miss) GET passes
        // through and populates the cache.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent(
                    "second GET must not happen: cache should serve it".into(),
                ),
            )
            .with_occurrence(Occurrence::Nth(2)),
        );
        let fault_store = Arc::new(FaultStore::new(seed, plan));
        let cache: Arc<Cache<crate::fetcher::CacheFetchError>> = Arc::new(Cache::new(
            CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024),
        ));
        let log_store: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&fault_store) as Arc<dyn ObjectStoreBackend>;
        let log_fetcher = LogSegmentFetcher::new(log_store).with_cache(cache);

        // Metrics path is unused for a Logs signal; give it an empty store.
        let metrics_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let (fetcher, server) =
            spawn_worker_with_log_fetcher(metrics_store, log_fetcher, segments).await;
        let distributed = Distributed::new(
            Arc::new(fetcher),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
            },
        );

        let fetch = || async {
            distributed
                .fetch_logs(
                    TENANT,
                    Signal::Logs,
                    &snapshot,
                    &[],
                    &[],
                    &QueryAccounting::new(),
                    &EngineConfig::default(),
                    i64::MAX,
                )
                .await
                .expect("distributed log fetch")
                .expect("distributed produced a result (not a fallback)")
        };

        // Miss: one real GET populates the cache.
        let first = fetch().await;
        assert_eq!(first.len(), 2, "cache-miss fetch returns both records");

        // Hit: served from the cache, so the scripted second GET never happens.
        let second = fetch().await;
        server.abort();
        assert_eq!(
            sorted_by_order_key(second),
            sorted_by_order_key(first),
            "cache-hit fetch returns the same records as the miss"
        );
        assert_eq!(
            fault_store.fault_count(Op::Get, FaultKind::Permanent),
            0,
            "the second fetch must be served from the read cache, issuing no store GET"
        );
    });
}

/// Worker-side erasure property: erasing rows by a per-record attribute against
/// a distributed slice set equals a local read of the same segments erased the
/// same way, including when the affected stream's segments straddle two slices.
/// The worker applies erasure per segment through the same `LogSegmentFetcher`
/// funnel the local path uses; correctness rests on segment self-containment,
/// not slice atomicity (ADR-0071 amendment decision 5).
///
/// Note on "resource-only key": `ravel-query`'s erasure funnel
/// (`retain_log_records`) evaluates PER-RECORD attributes only; merged-view
/// (resource-attribute) exclusion is the SQL lane's residual (ADR-0064,
/// `logs_provider`), out of #284's scope. So the row-removing case uses a
/// per-record key (`user_id`), and a resource-only key is separately shown to be
/// a consistent no-op through this funnel.
#[test]
fn logs_worker_applies_erasure_across_straddling_slices() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        for cap in [1usize, 2] {
            let store = Arc::new(MemoryStore::new());
            let segments = split_stream_corpus(&store).await;
            let snapshot = Snapshot {
                segments: segments.clone(),
                segments_pruned: 0,
                pending_erasure: Vec::new(),
            };

            // Erase every row carrying user_id=u1. alpha's u1 rows are ts 10
            // (slice 0) and ts 20 (slice 1), so the erased stream straddles
            // slices at cap 2.
            let erasure = vec![ErasurePredicate::windowless(vec![(
                "user_id".to_string(),
                "u1".to_string(),
            )])];

            let local = local_log_records(Arc::clone(&store), &segments, &erasure).await;
            let distributed = distributed_log_records(
                Arc::clone(&store),
                segments.clone(),
                &snapshot,
                Signal::Logs,
                &erasure,
                cap,
            )
            .await;
            assert_eq!(
                sorted_by_order_key(distributed.clone()),
                sorted_by_order_key(local),
                "cap {cap}: distributed erasure must equal local erasure over the same segments"
            );
            // The erasure genuinely removed the u1 rows (not a vacuous no-op),
            // and did so across the straddling slices.
            assert!(
                distributed.iter().all(|r| !r
                    .attrs
                    .iter()
                    .any(|(k, v)| k == "user_id" && *v == AttrValue::Str("u1".to_string()))),
                "cap {cap}: no surviving row carries the erased user_id=u1"
            );
            let kept_ts: Vec<i64> = distributed.iter().map(|r| r.ts_ns).collect();
            assert_eq!(
                kept_ts,
                vec![25, 30, 40],
                "cap {cap}: exactly the u2 rows survive, in global ts order"
            );
        }
    });
}

/// A resource-only attribute key erases nothing through this funnel (per-record
/// attributes only), consistently in both paths -- the "resource-only" wording
/// of the ADR's acceptance test, made a consistent no-op here. The row-removing
/// straddle case above carries the real erasure property.
#[test]
fn logs_resource_only_erasure_key_is_a_consistent_no_op() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = split_stream_corpus(&store).await;
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        // `service.name` is a resource attribute (in `stream_attrs`), never a
        // per-record attribute, so this funnel matches no row.
        let erasure = vec![ErasurePredicate::windowless(vec![(
            "service.name".to_string(),
            "alpha".to_string(),
        )])];

        let baseline = local_log_records(Arc::clone(&store), &segments, &[]).await;
        let local = local_log_records(Arc::clone(&store), &segments, &erasure).await;
        assert_eq!(
            sorted_by_order_key(local),
            sorted_by_order_key(baseline.clone()),
            "resource-only key erases nothing locally"
        );

        let distributed = distributed_log_records(
            Arc::clone(&store),
            segments,
            &snapshot,
            Signal::Logs,
            &erasure,
            2,
        )
        .await;
        assert_eq!(
            sorted_by_order_key(distributed),
            sorted_by_order_key(baseline),
            "resource-only key erases nothing distributed either (consistent no-op)"
        );
    });
}

/// A worker with no log fetcher wired returns `Unsupported` for a Logs slice, so
/// the coordinator falls back to whole-query local execution (`Ok(None)`), never
/// an error. This is the load-bearing skew direction (a not-yet-upgraded worker)
/// exercised over the real wire.
#[test]
fn logs_worker_without_log_fetcher_signals_local_fallback() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = split_stream_corpus(&store).await;
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // A worker WITHOUT `.with_log_fetcher(..)`: the metric path only.
        let metric_store: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
        let metric_fetcher = SegmentFetcher::new(metric_store);
        let resolver = Arc::new(SnapshotSegmentResolver::new(segments.clone()));
        let service = SeriesFetchService::new(metric_fetcher, resolver).into_server();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("addr")).expect("bind");
        let addr = incoming.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });
        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect_lazy();
        let fetcher = RemoteSliceFetcher::new(channel);

        let distributed = Distributed::new(
            Arc::new(fetcher),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 2,
            },
        );
        let accounting = QueryAccounting::new();
        let result = distributed
            .fetch_logs(
                TENANT,
                Signal::Logs,
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
            "a worker with no log fetcher signals whole-query local fallback (Ok(None))"
        );
        server.abort();
    });
}

// --- Spans distributed fan-out (#285) --------------------------------------
//
// The centerpiece is `run_span_differential`: over a real loopback worker, a
// distributed Spans fetch merged by the coordinator equals, as a multiset under
// the stated span total order, a raw local `SpanSegmentFetcher` read over the
// same segments -- concatenated directly, NOT through `merge_spans`, so the
// reference is independent of the function under test and the differential can
// actually catch a merge regression. This includes a corpus where one trace's
// spans straddle two slices (a reshard-activation window). The test proves the
// codec preserves every span and the partition is total -- and, on the split
// corpus, that the coordinator ORDERS rather than concatenates in slice order.
// Duplicate-span preservation (no query-time dedup for spans) is pinned
// separately by `spans_coordinator_preserves_duplicate_spans_across_slices`.

const TA: [u8; 16] = [0xAA; 16];
const TB: [u8; 16] = [0xBB; 16];
const S1: [u8; 8] = [1u8; 8];
const S2: [u8; 8] = [2u8; 8];

/// A span record on `trace_id`/`span_id` over `[start, end]`, with string
/// attributes. The attrs include `service.name` so the RSPAN `service_name`
/// lifted column (ADR-0054) is exercised: the writer lifts it out and the reader
/// re-inserts it, so a round-tripped record's `attrs` still carry it.
fn span_record(
    trace_id: [u8; 16],
    span_id: [u8; 8],
    start: i64,
    end: i64,
    attrs: &[(&str, &str)],
) -> ravel_rspan::SpanRecord {
    ravel_rspan::SpanRecord {
        trace_id,
        span_id,
        parent_span_id: None,
        name: "op".to_string(),
        start_ts_ns: start,
        end_ts_ns: end,
        status_code: ravel_rspan::StatusCode::Unset,
        status_message: None,
        attrs: attrs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
    }
}

/// Writes one RSPAN object holding `records` under `shard`/`hour`, returning an
/// L0 `SegmentRef` with a distinct content hash (blake3 of the bytes) so the
/// worker's content-hash resolver keys each segment uniquely. Small blocks so a
/// multi-block object is exercised; this never changes the span set returned.
async fn write_span_segment(
    store: &MemoryStore,
    key: u64,
    shard: u32,
    hour: u32,
    records: &[ravel_rspan::SpanRecord],
) -> SegmentRef {
    let cfg = ravel_rspan::RspanConfig {
        block_target_records: 2,
        ..ravel_rspan::RspanConfig::default()
    };
    let identity = ravel_rspan::ObjectIdentity {
        tenant_hash: TENANT.0,
        shard,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: key,
    };
    let mut writer = ravel_rspan::RspanWriter::new(cfg, identity);
    for r in records {
        writer.push(r.clone());
    }
    let bytes = writer.finish().expect("finish rspan object");
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let size = bytes.len() as u64;
    let object_key = format!("spans/{key}.rspan");
    store
        .put(
            &object_key,
            bytes::Bytes::from(bytes),
            PutOptions::default(),
        )
        .await
        .expect("put span object");
    let min = records
        .iter()
        .map(|r| r.start_ts_ns)
        .min()
        .expect("nonempty");
    let max = records.iter().map(|r| r.end_ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: object_key,
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: hour,
        sample_count: records.len() as u64,
        series_count: 0,
        shard,
        content_hash,
        writer_id: Uuid::from_u128(u128::from(key) + 1),
        writer_epoch: 1,
        writer_seq: key,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// The local reference for spans: read every segment with `SpanSegmentFetcher`
/// over the whole-snapshot window (a superset of each segment) and concatenate
/// the per-segment spans directly, with NO merge or dedup, applying erasure per
/// segment through the same `is_erased_span` funnel the local `spans` scan uses.
/// This is exactly the multiset of spans a local multi-segment read observes, and
/// it is deliberately independent of `merge_spans` (the function under test) so
/// the differential compares distributed output against a reference the function
/// cannot influence. Callers compare via [`sorted_spans_by_order_key`] as a
/// multiset (duplicates preserved on both sides).
async fn local_span_rows(
    store: Arc<MemoryStore>,
    segments: &[SegmentRef],
    erasure: &[ErasurePredicate],
) -> Vec<SpanRow> {
    let fetcher = SpanSegmentFetcher::new(store);
    let (min, max) = whole_window(segments);
    let query = ravel_rspan::SpanQuery::ts_range(min, max);
    let mut all = Vec::new();
    for seg in segments {
        let out = fetcher
            .fetch(seg, &query, None, None, &[])
            .await
            .expect("local span fetch");
        let mut rows = out.map(|o| o.records).unwrap_or_default();
        if !erasure.is_empty() {
            rows.retain(|row| !is_erased_span(&row.record.attrs, row.record.start_ts_ns, erasure));
        }
        all.extend(rows);
    }
    all
}

/// Stable-sort a span multiset under the production total-order key, preserving
/// duplicates. Applying this to both sides of a differential turns an
/// order-sensitive `Vec` equality into a multiset equality under the stated
/// order, without ever calling `merge_spans`.
fn sorted_spans_by_order_key(mut spans: Vec<SpanRow>) -> Vec<SpanRow> {
    spans.sort_by_key(span_order_key);
    spans
}

/// The distributed reference for spans: dispatch over a real loopback worker at
/// width `cap`, merged by the coordinator's `fetch_spans`.
async fn distributed_span_rows(
    store: Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
    snapshot: &Snapshot,
    erasure: &[ErasurePredicate],
    cap: usize,
) -> Vec<SpanRow> {
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
    let spans = distributed
        .fetch_spans(
            TENANT,
            Signal::Spans,
            snapshot,
            &[],
            erasure,
            &accounting,
            &EngineConfig::default(),
            i64::MAX,
        )
        .await
        .expect("distributed span fetch")
        .expect("distributed produced a result (not a fallback)");
    server.abort();
    spans
}

/// The (trace_id, span_id) identity sequence of a span list, for order-sensitive
/// prove-the-test comparisons.
fn span_id_seq(spans: &[SpanRow]) -> Vec<([u8; 16], [u8; 8])> {
    spans
        .iter()
        .map(|s| (s.record.trace_id, s.record.span_id))
        .collect()
}

/// The split corpus: two segments under two DIFFERENT shards (a reshard-
/// activation window), each carrying spans of BOTH trace `TA` and trace `TB`.
/// Because the corpus has exactly two shards, `cap = 2` puts each segment in its
/// own slice (shard-major partitioning), so both traces straddle the two slices.
///
/// Span identities are laid out so slice-order concatenation is observably NOT
/// the correct total order `(trace_id, span_id, ...)`:
/// - seg_a (shard 0), sorted on disk by (trace_id, start): TA/S2 (start 20),
///   TB/S1 (start 15).
/// - seg_b (shard 1): TA/S1 (start 10), TB/S2 (start 25).
///
/// The correct merge order is TA/S1, TA/S2, TB/S1, TB/S2; slice-order concat
/// (seg_a then seg_b) is TA/S2, TB/S1, TA/S1, TB/S2 -- different, so the merge's
/// `sort_by` line is under test.
///
/// Per-span `user_id`: the two TA spans carry u1, the two TB spans carry u2.
/// Erasing u1 therefore removes both TA spans, which straddle the two slices.
async fn split_trace_corpus(store: &MemoryStore) -> Vec<SegmentRef> {
    let seg_a = write_span_segment(
        store,
        0,
        0,
        100,
        &[
            span_record(
                TA,
                S2,
                20,
                21,
                &[("service.name", "alpha"), ("user_id", "u1")],
            ),
            span_record(
                TB,
                S1,
                15,
                16,
                &[("service.name", "beta"), ("user_id", "u2")],
            ),
        ],
    )
    .await;
    let seg_b = write_span_segment(
        store,
        1,
        1,
        101,
        &[
            span_record(
                TA,
                S1,
                10,
                11,
                &[("service.name", "alpha"), ("user_id", "u1")],
            ),
            span_record(
                TB,
                S2,
                25,
                26,
                &[("service.name", "beta"), ("user_id", "u2")],
            ),
        ],
    )
    .await;
    vec![seg_a, seg_b]
}

/// Differential: distributed span fetch == local read, as a multiset under the
/// stated total order, at cap 1 (one slice) and cap 2 (a stream-straddling two
/// slice partition of the same split corpus). On the two-slice case it also
/// proves the corpus is discriminating: a naive concatenate-in-slice-order
/// coordinator would emit an order the local read never produces, so the
/// `sort_by` line in `merge_spans` is the line under test. Reintroducing a
/// slice-order concatenation there fails the `assert_ne!` prove-the-test and the
/// cap-2 multiset assertion.
#[test]
fn spans_distributed_matches_local_including_reshard_straddle() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = split_trace_corpus(&store).await;
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // Single-slice: every segment in one slice. Distributed == local, bit
        // identical (SpanRow: PartialEq) as a multiset under the total order.
        let local = local_span_rows(Arc::clone(&store), &segments, &[]).await;
        let one_slice =
            distributed_span_rows(Arc::clone(&store), segments.clone(), &snapshot, &[], 1).await;
        assert_eq!(
            sorted_spans_by_order_key(one_slice),
            sorted_spans_by_order_key(local.clone()),
            "single-slice distributed spans must equal local, bit identical"
        );

        // Two-slice straddle: confirm the partition genuinely places each segment
        // in its own slice, so both traces' spans straddle two slices (not the
        // easy single-slice case).
        let slices = partition_snapshot(&snapshot, 2);
        assert_eq!(
            slices.len(),
            2,
            "the reshard-straddle corpus must produce two slices"
        );
        assert_eq!(
            slices[0].segments.len(),
            1,
            "slice 0 must hold exactly one segment, so the traces straddle"
        );
        assert_eq!(
            slices[1].segments.len(),
            1,
            "slice 1 must hold exactly one segment, so the traces straddle"
        );

        let two_slice =
            distributed_span_rows(Arc::clone(&store), segments.clone(), &snapshot, &[], 2).await;
        assert_eq!(
            sorted_spans_by_order_key(two_slice.clone()),
            sorted_spans_by_order_key(local),
            "two-slice distributed spans must equal local even when a trace straddles slices"
        );

        // Prove-the-test: the naive wrong coordinator concatenates each slice's
        // local read in slice order. On this corpus that order is observably NOT
        // the global total order the correct merge produces, so the differential
        // is not vacuous.
        let ref_store: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
        let ref_fetcher = SpanSegmentFetcher::new(ref_store);
        let (min, max) = whole_window(&segments);
        let full = ravel_rspan::SpanQuery::ts_range(min, max);
        let mut naive = Vec::new();
        for slice in &slices {
            for seg in &slice.segments {
                let out = ref_fetcher
                    .fetch(seg, &full, None, None, &[])
                    .await
                    .expect("fetch")
                    .expect("in range");
                naive.extend(out.records);
            }
        }
        let naive_ids = span_id_seq(&naive);
        let correct_ids = span_id_seq(&two_slice);
        assert_ne!(
            naive_ids, correct_ids,
            "slice-order concatenation must differ from the correct merge, else the test is vacuous"
        );
        assert_eq!(
            correct_ids,
            vec![(TA, S1), (TA, S2), (TB, S1), (TB, S2)],
            "the merge must emit the global (trace_id, span_id) order"
        );
    });
}

/// Two byte-identical spans split across two segments/slices must BOTH survive
/// the coordinator's merge. Per docs/consistency-model.md ("logs and spans") and
/// ADR-0051 section 5, spans have NO query-time dedup: a retry after a lost ack
/// produces byte-identical spans that are legitimately duplicate user data and
/// must stay visible, so collapsing them is silent data loss. This is the
/// focused minimal-repro for the dedup trap #284 shipped and this task must not
/// repeat: reintroducing a `dedup_by` on the merged pool collapses the two
/// spans to one and fails the `distributed.len() == 2` assertion.
#[test]
fn spans_coordinator_preserves_duplicate_spans_across_slices() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        // The SAME span written into two segments under two shards -> two slices
        // at cap 2. A raw local read of the two segments returns TWO spans; the
        // distributed merge must too.
        let dup = span_record(
            [0xCC; 16],
            [9u8; 8],
            50,
            51,
            &[("service.name", "gamma"), ("k", "v")],
        );
        let seg_a = write_span_segment(&store, 0, 0, 100, std::slice::from_ref(&dup)).await;
        let seg_b = write_span_segment(&store, 1, 1, 100, std::slice::from_ref(&dup)).await;
        let segments = vec![seg_a, seg_b];
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // Confirm the two duplicate spans genuinely land in two different slices,
        // not the easy single-slice case where any merge would trivially preserve
        // both.
        let slices = partition_snapshot(&snapshot, 2);
        assert_eq!(
            slices.len(),
            2,
            "the two segments must partition into two slices for this to be a
             cross-slice duplicate, not a within-slice one"
        );

        // Reference: a raw local read of both segments, independent of the merge.
        let local = local_span_rows(Arc::clone(&store), &segments, &[]).await;
        assert_eq!(
            local.len(),
            2,
            "a raw local read of the two segments returns both duplicate spans"
        );

        let distributed =
            distributed_span_rows(Arc::clone(&store), segments, &snapshot, &[], 2).await;
        assert_eq!(
            distributed.len(),
            2,
            "the coordinator must preserve both cross-slice duplicate spans, not collapse them"
        );
        assert_eq!(
            sorted_spans_by_order_key(distributed),
            sorted_spans_by_order_key(local),
            "the distributed result is the same multiset as the raw local read"
        );
    });
}

/// Worker-side erasure property: erasing spans by a per-span attribute against a
/// distributed slice set equals a local read of the same segments erased the
/// same way, including when the affected trace's spans straddle two slices. The
/// worker applies erasure per segment through the same `is_erased_span` funnel
/// the local scan uses; correctness rests on segment self-containment, not slice
/// atomicity (ADR-0071 amendment decision 5). Deleting the worker's
/// `is_erased_span` retain in `run_slice_spans` makes the erased spans reappear
/// and the "no surviving u1" / kept-set assertions below fail.
#[test]
fn spans_worker_applies_erasure_across_straddling_slices() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        for cap in [1usize, 2] {
            let store = Arc::new(MemoryStore::new());
            let segments = split_trace_corpus(&store).await;
            let snapshot = Snapshot {
                segments: segments.clone(),
                segments_pruned: 0,
                pending_erasure: Vec::new(),
            };

            // Erase every span carrying user_id=u1. Both TA spans carry u1, at
            // ts 10 (slice 1) and ts 20 (slice 0), so the erased trace straddles
            // slices at cap 2.
            let erasure = vec![ErasurePredicate::windowless(vec![(
                "user_id".to_string(),
                "u1".to_string(),
            )])];

            let local = local_span_rows(Arc::clone(&store), &segments, &erasure).await;
            let distributed = distributed_span_rows(
                Arc::clone(&store),
                segments.clone(),
                &snapshot,
                &erasure,
                cap,
            )
            .await;
            assert_eq!(
                sorted_spans_by_order_key(distributed.clone()),
                sorted_spans_by_order_key(local),
                "cap {cap}: distributed erasure must equal local erasure over the same segments"
            );
            // The erasure genuinely removed the u1 spans (not a vacuous no-op),
            // and did so across the straddling slices.
            assert!(
                distributed.iter().all(|row| !row
                    .record
                    .attrs
                    .iter()
                    .any(|(k, v)| k == "user_id" && v == "u1")),
                "cap {cap}: no surviving span carries the erased user_id=u1"
            );
            assert_eq!(
                span_id_seq(&sorted_spans_by_order_key(distributed)),
                vec![(TB, S1), (TB, S2)],
                "cap {cap}: exactly the u2 (TB) spans survive, in total order"
            );
        }
    });
}

/// A worker with no span fetcher wired returns `Unsupported` for a Spans slice,
/// so the coordinator falls back to whole-query local execution (`Ok(None)`),
/// never an error. This is the load-bearing skew direction (a not-yet-upgraded
/// worker) exercised over the real wire.
#[test]
fn spans_worker_without_span_fetcher_signals_local_fallback() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = split_trace_corpus(&store).await;
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // A worker WITHOUT `.with_span_fetcher(..)`: the metric path only.
        let metric_store: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
        let metric_fetcher = SegmentFetcher::new(metric_store);
        let resolver = Arc::new(SnapshotSegmentResolver::new(segments.clone()));
        let service = SeriesFetchService::new(metric_fetcher, resolver).into_server();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("addr")).expect("bind");
        let addr = incoming.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });
        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect_lazy();
        let fetcher = RemoteSliceFetcher::new(channel);

        let distributed = Distributed::new(
            Arc::new(fetcher),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 2,
            },
        );
        let accounting = QueryAccounting::new();
        let result = distributed
            .fetch_spans(
                TENANT,
                Signal::Spans,
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
            "a worker with no span fetcher signals whole-query local fallback (Ok(None))"
        );
        server.abort();
    });
}
