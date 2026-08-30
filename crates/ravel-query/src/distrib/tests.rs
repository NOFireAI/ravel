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
use ravel_segment::{
    CompactionMetaV4, IngestBounds, RunInputV7, SampleProvenance, SegmentIdentity, SegmentWriter,
    SeriesInput, SeriesInputV7, SeriesValues, encode_run_v4,
};
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
use crate::distrib::proto::series_fetch_server::{SeriesFetch, SeriesFetchServer};
use crate::distrib::{
    log_record_order_key, service::SeriesFetchService, service::SnapshotSegmentResolver, span_cmp,
    span_order_key,
};
use crate::engine::merge_soa_runs;
use crate::erasure::{ErasurePredicate, is_erased_span};
use crate::fetcher::SegmentFetcher;
use crate::log_fetcher::{LogQuery, LogSegmentFetcher};
use crate::phase_accounting::{PhaseAccounting, PhaseAccountingSnapshot, QueryPhase};
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
        segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
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
    PhaseAccountingSnapshot,
    crate::fetcher::FetchStats,
) {
    let fetcher = SegmentFetcher::new(store);
    let accounting = PhaseAccounting::new();
    let mut out = Vec::with_capacity(snapshot.segments.len());
    let mut stats = crate::fetcher::FetchStats::default();
    for seg in &snapshot.segments {
        let (scalar, seg_stats, _hist) = fetcher
            .fetch_soa_and_histograms_phase_accounted(TENANT, seg, &[], &accounting)
            .await
            .expect("local fetch");
        stats.raw_f64_pages += seg_stats.raw_f64_pages;
        stats.raw_f64_bytes += seg_stats.raw_f64_bytes;
        out.push(scalar);
    }
    (out, accounting.snapshot(), stats)
}

/// The local histogram reference: fetch every snapshot segment's histogram runs
/// directly (the third element of the fetch tuple the local path merges), so a
/// distributed histogram fetch can be compared run-for-run against it.
async fn local_histograms(
    store: Arc<MemoryStore>,
    snapshot: &Snapshot,
) -> Vec<Vec<crate::fetcher::FetchedHistogramSeries>> {
    let fetcher = SegmentFetcher::new(store);
    let accounting = PhaseAccounting::new();
    let mut out = Vec::with_capacity(snapshot.segments.len());
    for seg in &snapshot.segments {
        let (_scalar, _stats, hist) = fetcher
            .fetch_soa_and_histograms_phase_accounted(TENANT, seg, &[], &accounting)
            .await
            .expect("local histogram fetch");
        out.push(hist);
    }
    out
}

/// Asserts two histogram run pools carry the same series with the same
/// timestamps and bit-identical records. Both pools are flattened and sorted by
/// series id + first timestamp, so per-slice vs per-segment grouping does not
/// matter. Records compare via `encode_histogram_records`, whose every `f64`
/// crosses as its `to_bits` pattern, so `-0.0`/NaN bucket counts and sums cannot
/// pass as equal when they differ.
fn assert_histograms_bit_identical(
    local: &[Vec<crate::fetcher::FetchedHistogramSeries>],
    distributed: &[Vec<crate::fetcher::FetchedHistogramSeries>],
) {
    let flatten = |pool: &[Vec<crate::fetcher::FetchedHistogramSeries>]| {
        let mut v: Vec<crate::fetcher::FetchedHistogramSeries> =
            pool.iter().flatten().cloned().collect();
        v.sort_by_key(|s| {
            (
                s.series_id.0,
                s.timestamps.first().copied().unwrap_or(i64::MIN),
            )
        });
        v
    };
    let a = flatten(local);
    let b = flatten(distributed);
    assert_eq!(
        a.len(),
        b.len(),
        "histogram series count differs local vs distributed"
    );
    for (la, lb) in a.iter().zip(b.iter()) {
        assert_eq!(la.series_id, lb.series_id, "histogram series id differs");
        assert_eq!(la.timestamps, lb.timestamps, "histogram timestamps differ");
        assert_eq!(
            crate::distrib::codec::encode_histogram_records(&la.values),
            crate::distrib::codec::encode_histogram_records(&lb.values),
            "histogram record bit patterns differ (sum/count/bucket corruption)"
        );
    }
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
    let accounting = PhaseAccounting::new();
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
            None,
        )
        .await
        .expect("distributed fetch")
        .expect("distributed produced a result (not a fallback)")
        .0;
    let distributed_stats = triple.1;
    let distributed_merged = merge_soa_runs(triple.0, usize::MAX, usize::MAX).expect("dist merge");

    assert_series_bit_identical(&local_merged, &distributed_merged);
    // The distributed path folds every slice's accounting and stats, so it
    // reports the same cost the local path does over the same disjoint
    // segments -- not `FetchStats::default()` zeros, and not a wrapped or
    // dropped accounting counter (findings 3 and 4).
    //
    // Compared PER PHASE, not pooled (issue #959): both sides are
    // `PhaseAccountingSnapshot`s, so this fails if the coordinator charges a
    // remote slice's plan/probe reads to `scan` even though the pooled totals
    // still agree. That is exactly the degenerate split this comparison used
    // to be blind to, over the whole proptest corpus rather than one shape.
    assert_eq!(
        accounting.snapshot(),
        local_acct,
        "distributed accounting must equal local accounting, phase for phase"
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

// --- run-merged (per-sample provenance) refusal (#315/#348) -----------------

/// Writes one L1 merged RSEG segment for series `m0` whose single run carries a
/// per-sample provenance column (the shape an L1 compaction produces since
/// issue #315), and returns a matching L1 `SegmentRef`.
///
/// The layout is the reviewed hazard: the merged run's run-wide minimum-prefix
/// `(created_unix_ns, ...)` picks a DIFFERENT dedup winner at a duplicate
/// timestamp than the explicit per-sample column does. At ts=10 the column gives
/// the `created=200` sample (value 1.0) the win, while the run-wide prefix
/// `created=100` applied by array position would instead pick the `created=100`
/// sample (value 9.0). So a distributed path that dropped the column (degrading
/// the frame rather than refusing it) returns 9.0 where the local path returns
/// 1.0 -- the silent wrong result this fix closes.
async fn write_l1_merged_provenance(store: &MemoryStore) -> SegmentRef {
    let label_set = labels("m0");
    let series_id = SeriesId::compute(&tenant_id(), "m0", &label_set).expect("series id");
    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: Uuid::nil().to_string(),
        writer_epoch: 0,
        writer_seq: 0,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let input_set_hash = [0x33u8; 32];
    let meta = CompactionMetaV4 {
        ingest_hour_bucket: 100,
        input_set_hash,
        part_index: 0,
        level: 1,
    };
    // Merged run, samples in on-disk order (ascending ts, dup ts kept):
    //   idx0: ts=10 val=1.0  from write A (created 200)
    //   idx1: ts=10 val=9.0  from write B (created 100)
    //   idx2: ts=20 val=2.0  from write A (created 200)
    let samples = SeriesValues::Scalar(vec![
        Sample {
            ts_ns: 10,
            value: 1.0,
        },
        Sample {
            ts_ns: 10,
            value: 9.0,
        },
        Sample {
            ts_ns: 20,
            value: 2.0,
        },
    ]);
    // Run-wide created deliberately 100 (the min-prefix): a reader that ignored
    // the columns would let idx1 (9.0) win ts=10 by array position.
    let run = encode_run_v4(&series_id, 100, 0, 0, &samples).expect("frame merged run");
    let provenance = Some(vec![
        SampleProvenance {
            created_unix_ns: 200,
            writer_epoch: 1,
            writer_seq: 1,
            in_page_index: 0,
        },
        SampleProvenance {
            created_unix_ns: 100,
            writer_epoch: 1,
            writer_seq: 1,
            in_page_index: 0,
        },
        SampleProvenance {
            created_unix_ns: 200,
            writer_epoch: 1,
            writer_seq: 1,
            in_page_index: 1,
        },
    ]);
    let series = vec![SeriesInputV7 {
        series_id,
        labels: label_set,
        runs: vec![RunInputV7 { run, provenance }],
    }];
    let written =
        SegmentWriter::write_v7_with_provenance(series, identity, bounds, meta, Vec::new())
            .expect("write L1 with provenance");
    let key = "seg/l1-merged-provenance.rseg";
    store
        .put(key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put L1 object");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: 100,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard: 0,
        content_hash: written.summary.blake3,
        writer_id: Uuid::nil(),
        writer_epoch: 0,
        writer_seq: 0,
        created_unix_ns: 100,
        level: SegmentLevel::L1 {
            input_set_hash,
            part_index: 0,
        },
        segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
    }
}

/// ADR-0096 acceptance (decision 3 step 4, deliverable 7 bullet 1): a
/// distributed query over run-merged L1 data (a segment written with an explicit
/// `per_sample_priorities` column) is SERVED over the wire -- not refused to the
/// coordinator's local fallback -- and its coordinator-merged result is
/// bit-identical (`f64::to_bits`) to the same query run purely locally,
/// including at the overlapping timestamp where the winner depends on per-sample
/// provenance.
///
/// This is the direct inverse of the pre-flip
/// `run_merged_series_refuses_over_the_wire_not_degrades`, which asserted the
/// slice was refused (`Ok(None)`). Post-flip the encoder emits the four packed
/// provenance columns, so `Distributed::fetch` returns `Ok(Some(..))` and the
/// distributed path itself (not a fallback) carries the column-dictated winners.
///
/// The corpus is the reviewed hazard: at ts=10 the per-sample column gives the
/// `created=200` sample (value 1.0) the win, while the run-wide prefix
/// `created=100` applied by array position would pick the `created=100` sample
/// (value 9.0). A degraded run-wide frame would return 9.0 here; the assertion
/// pins 1.0, so it fails against any encode that drops the column.
///
/// Mutation proof: stubbing `encode_series_frame` to emit empty provenance
/// columns (the pre-flip degraded encode) turns ts=10's winner into 9.0 and this
/// test goes RED at the `to_bits` comparison.
#[test]
fn run_merged_series_distributed_over_the_wire_not_refused() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let seg = write_l1_merged_provenance(&store).await;
        let snapshot = Snapshot {
            segments: vec![seg.clone()],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // Pure-local reference plus the ground-truth single-fetch cost.
        let (local_runs, local_acct, _local_stats) =
            local_scalar(Arc::clone(&store), &snapshot).await;
        let local_merged = merge_soa_runs(local_runs, usize::MAX, usize::MAX).expect("local merge");

        let (fetcher, server) = spawn_worker(Arc::clone(&store), vec![seg]).await;
        let distributed = Distributed::new(
            Arc::new(fetcher),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
            },
        );
        let accounting = PhaseAccounting::new();
        let triple = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &EngineConfig::default(),
                i64::MAX,
                None,
            )
            .await
            .expect("distributed fetch")
            .expect(
                "a run-merged series is now served over the wire (PROTOCOL_VERSION 3), \
                 not refused to local fallback",
            )
            .0;
        server.abort();

        let distributed_merged =
            merge_soa_runs(triple.0, usize::MAX, usize::MAX).expect("dist merge");
        assert_series_bit_identical(&local_merged, &distributed_merged);

        // Pin the column-dictated winners so this is not "two equal empties": the
        // merge keeps 1.0 at ts=10 (created=200 beats created=100's 9.0) and 2.0
        // at ts=20. A degraded run-wide frame would put 9.0 at ts=10.
        assert_eq!(distributed_merged.len(), 1);
        let samples = &distributed_merged[0].samples;
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].ts_ns, 10);
        assert_eq!(samples[0].value.to_bits(), 1.0f64.to_bits());
        assert_eq!(samples[1].ts_ns, 20);
        assert_eq!(samples[1].value.to_bits(), 2.0f64.to_bits());

        // The served path folded the slice's real S3 spend exactly once: equal to
        // a single local fetch of the segment, not zero and not doubled.
        assert_eq!(
            accounting.snapshot(),
            local_acct,
            "the distributed fetch must fold the slice's spend exactly once"
        );
    });
}

/// Closes the reviewed coverage gap: drive a run-merged series (per-sample
/// provenance column) through the distributed query path and assert the result
/// is bit-identical to the same query run purely locally, compared by
/// `f64::to_bits`.
///
/// Today the slice is refused and the coordinator falls back to local, so the
/// distributed-with-fallback result IS the local result. The winner pin proves
/// the corpus is the discriminating one (the column winner 1.0 at ts=10, never
/// the degraded run-wide 9.0). When #348 lands and the frame carries the column,
/// `fetch` returns the real distributed result and this same assertion proves the
/// wire preserved the column's winners. The test survives that change because it
/// compares the coordinator's answer -- however produced -- to local, mirroring
/// the engine's own `None`-means-local-fallback rule (engine.rs).
#[test]
fn run_merged_series_distributed_equals_local_bitwise() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let seg = write_l1_merged_provenance(&store).await;
        let snapshot = Snapshot {
            segments: vec![seg.clone()],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // Pure-local reference.
        let (local_runs, _acct, _stats) = local_scalar(Arc::clone(&store), &snapshot).await;
        let local_merged = merge_soa_runs(local_runs, usize::MAX, usize::MAX).expect("local merge");

        let (fetcher, server) = spawn_worker(Arc::clone(&store), vec![seg]).await;
        let distributed = Distributed::new(
            Arc::new(fetcher),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
            },
        );
        let accounting = PhaseAccounting::new();
        let distributed_merged = match distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &EngineConfig::default(),
                i64::MAX,
                None,
            )
            .await
            .expect("distributed fetch")
        {
            // #348: the frame carries the column; merge the real distributed runs.
            Some((triple, _partials)) => {
                merge_soa_runs(triple.0, usize::MAX, usize::MAX).expect("dist merge")
            }
            // Today: refusal -> the engine runs the query locally instead.
            None => {
                let (runs, _a, _s) = local_scalar(Arc::clone(&store), &snapshot).await;
                merge_soa_runs(runs, usize::MAX, usize::MAX).expect("fallback merge")
            }
        };
        server.abort();

        assert_series_bit_identical(&local_merged, &distributed_merged);
        // Pin the column-dictated winners so this is not "two equal empties": the
        // merge keeps 1.0 at ts=10 (created=200 beats created=100's 9.0) and 2.0
        // at ts=20. A degraded run-wide path would put 9.0 at ts=10.
        assert_eq!(local_merged.len(), 1);
        let samples = &local_merged[0].samples;
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].ts_ns, 10);
        assert_eq!(samples[0].value.to_bits(), 1.0f64.to_bits());
        assert_eq!(samples[1].ts_ns, 20);
        assert_eq!(samples[1].value.to_bits(), 2.0f64.to_bits());
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
    let accounting = PhaseAccounting::new();
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
            None,
        )
        .await
        .expect("distributed fetch")
        .expect("distributed produced a result")
        .0;
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
        let accounting = PhaseAccounting::new();
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
                None,
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
            partial_aggregate: None,
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
            response.phase_accounting.pooled().total_s3_bytes() > 0,
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
        let acct = PhaseAccounting::new();
        acct.scan()
            .add_s3_bytes(ravel_types::accounting::AccountedOp::Get, self.spend_bytes);
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: Vec::new(),
            phase_accounting: acct.snapshot(),
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
        let accounting = PhaseAccounting::new();
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
                None,
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
            accounting.snapshot().pooled().total_s3_bytes() >= 10_000,
            "the folded spend must survive on the live accounting handle"
        );
    });
}

// --- byte budget scoped by actual slice count (issue #588) -----------------

/// A [`SliceFetcher`] double that records every `request.budgets` it receives
/// (so a test can assert what each dispatched slice was actually authorized
/// for) and answers with an empty, successful response.
struct RecordingBudgetWorker {
    seen: Arc<std::sync::Mutex<Vec<pb::Budgets>>>,
}

#[async_trait::async_trait]
impl SliceFetcher for RecordingBudgetWorker {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        if let Some(budgets) = request.budgets {
            self.seen.lock().expect("lock").push(budgets);
        }
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: Vec::new(),
            phase_accounting: PhaseAccountingSnapshot::default(),
            stats: crate::fetcher::FetchStats::default(),
            series_returned: 0,
            samples_returned: 0,
            status: pb::status::Code::Ok,
            status_message: String::new(),
        })
    }
}

async fn sharded_snapshot(store: &MemoryStore, shard_count: u32) -> Snapshot {
    let mut segments = Vec::new();
    for shard in 0..shard_count {
        segments.push(
            write_segment(
                store,
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
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// ADR-0071 (issue #588): every slice used to carry the tenant's FULL byte
/// budget verbatim, so a `max_parallel_slices`-wide fan-out could authorize
/// up to Nx the configured budget before the coordinator's post-merge
/// re-check observed anything. Three shards at cap 3 dispatch three slices;
/// each must be authorized for exactly a third of the query's byte budget,
/// summing to no more than the configured total. `max_series`/
/// `max_samples`/`max_segments` stay unscoped (deliberately, per the fix's
/// doc comment): each slice still carries the full count-based caps.
///
/// Mutation proof: reverting `encode_budgets` in `mod.rs` to the pre-fix
/// `max_bytes_scanned: n` (no division) makes the per-slice assertion below
/// fail with `900`, not `300`.
#[test]
fn distrib_fetch_scopes_byte_budget_by_actual_slice_count() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = MemoryStore::new();
        let snapshot = sharded_snapshot(&store, 3).await;
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let distributed = Distributed::new(
            Arc::new(RecordingBudgetWorker {
                seen: Arc::clone(&seen),
            }),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 3,
            },
        );
        let config = EngineConfig {
            max_bytes_scanned: crate::config::ByteLimit::Bounded(900),
            max_series: 42,
            max_samples: 43,
            max_segments: 44,
            ..EngineConfig::default()
        };
        let accounting = PhaseAccounting::new();
        distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                i64::MAX,
                None,
            )
            .await
            .expect("fetch succeeds");

        let recorded = seen.lock().expect("lock");
        assert_eq!(recorded.len(), 3, "all three slices dispatched once");
        for budgets in recorded.iter() {
            assert_eq!(
                budgets.max_bytes_scanned, 300,
                "each of 3 slices gets a third of the 900-byte budget, not the whole thing"
            );
            assert_eq!(budgets.max_series, 42, "count-based caps stay unscoped");
            assert_eq!(budgets.max_samples, 43, "count-based caps stay unscoped");
            assert_eq!(budgets.max_segments, 44, "count-based caps stay unscoped");
        }
        let total: u64 = recorded.iter().map(|b| b.max_bytes_scanned).sum();
        assert!(
            total <= 900,
            "the sum of every slice's share must never exceed the configured budget"
        );
    });
}

/// The division must scope to how many slices `partition_snapshot` actually
/// produced, not the configured `max_parallel_slices` cap: two shards at cap
/// 8 dispatch only two slices, so each must get half the budget, not an
/// eighth. Dividing by the cap instead of the real count would starve every
/// slice with a spurious budget far below what the query is actually
/// entitled to.
#[test]
fn distrib_fetch_scopes_byte_budget_by_real_not_configured_slice_count() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = MemoryStore::new();
        let snapshot = sharded_snapshot(&store, 2).await;
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let distributed = Distributed::new(
            Arc::new(RecordingBudgetWorker {
                seen: Arc::clone(&seen),
            }),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 8,
            },
        );
        let config = EngineConfig {
            max_bytes_scanned: crate::config::ByteLimit::Bounded(1_000),
            ..EngineConfig::default()
        };
        let accounting = PhaseAccounting::new();
        distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                i64::MAX,
                None,
            )
            .await
            .expect("fetch succeeds");

        let recorded = seen.lock().expect("lock");
        assert_eq!(recorded.len(), 2, "only two shards, so only two slices");
        for budgets in recorded.iter() {
            assert_eq!(
                budgets.max_bytes_scanned, 500,
                "two real slices split the budget in half, not by the cap of 8"
            );
        }
    });
}

/// `Unlimited` stays the wire's `0` sentinel regardless of slice count: a
/// query with no configured byte cap must not have one manufactured by
/// dividing `0` (or crashing on it).
#[test]
fn distrib_fetch_unlimited_byte_budget_stays_the_zero_sentinel_across_slices() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = MemoryStore::new();
        let snapshot = sharded_snapshot(&store, 3).await;
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let distributed = Distributed::new(
            Arc::new(RecordingBudgetWorker {
                seen: Arc::clone(&seen),
            }),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 3,
            },
        );
        let config = EngineConfig {
            max_bytes_scanned: crate::config::ByteLimit::Unlimited,
            ..EngineConfig::default()
        };
        let accounting = PhaseAccounting::new();
        distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                i64::MAX,
                None,
            )
            .await
            .expect("fetch succeeds");

        let recorded = seen.lock().expect("lock");
        assert_eq!(recorded.len(), 3);
        for budgets in recorded.iter() {
            assert_eq!(
                budgets.max_bytes_scanned, 0,
                "unlimited stays the 0 sentinel"
            );
        }
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
            histogram: Vec::new(),
            partials: Vec::new(),
            phase_accounting: PhaseAccountingSnapshot::default(),
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
        let accounting = PhaseAccounting::new();
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
                None,
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
            partial_aggregate: None,
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
            partial_aggregate: None,
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
        let acct = PhaseAccounting::new();
        acct.scan()
            .add_s3_bytes(ravel_types::accounting::AccountedOp::Get, spend);
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: Vec::new(),
            phase_accounting: acct.snapshot(),
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
        let accounting = PhaseAccounting::new();
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
                None,
            )
            .await
            .expect_err("a wrapped fold would let the overflowing report through");
        assert!(
            matches!(err, crate::error::QueryError::TooManyBytesScanned { .. }),
            "expected TooManyBytesScanned, got {err:?}"
        );
    });
}

/// ADR-0096 acceptance (decision 3 step 4, deliverable 7 bullet 2): a
/// distributed query over a segment with real native-histogram series returns
/// results bit-identical to the local equivalent. The histogram records now
/// cross the wire (`encode_histogram_frame`/`decode_histogram_frame`) rather
/// than triggering the removed refusal, so `Distributed::fetch` returns the real
/// histogram runs in the triple's third element and they equal the local fetch
/// run-for-run, `to_bits` on every `f64`.
///
/// Mutation proof: stubbing `encode_histogram_frame` to omit `records` (the
/// pre-flip degraded encode) makes the decoded run length disagree with its
/// timestamps, so the coordinator's decode raises
/// `CodecError::HistogramRunLengthMismatch` and `Distributed::fetch` errors --
/// this test goes RED at `.expect("distributed fetch")`.
#[test]
fn histogram_series_distributed_equals_local_bitwise() {
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
        // Two samples with distinct, data-bearing sums, so the fixture's answer
        // depends on the records actually crossing the wire (not just an empty
        // shell): a dropped/blanked record set changes the compared bit patterns.
        let hist = |sum: f64, count: u64, bucket: u64| HistogramValue {
            scale: 0,
            zero_threshold: 0.0,
            sum: Some(sum),
            custom_values: None,
            positive_spans: vec![ravel_segment::HistogramSpan {
                offset: 0,
                length: 1,
            }],
            negative_spans: Vec::new(),
            counts: HistogramCounts::Int {
                zero_count: 0,
                count,
                positive: vec![bucket],
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
                values: SeriesValues::Histogram(vec![
                    HistogramSample {
                        ts_ns: NS,
                        value: hist(2.5, 1, 1),
                    },
                    HistogramSample {
                        ts_ns: 2 * NS,
                        value: hist(9.0, 3, 3),
                    },
                ]),
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
            segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
        };
        let snapshot = Snapshot {
            segments: vec![seg.clone()],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // Local reference: the same segment's histogram runs, fetched directly.
        let local_hist = local_histograms(Arc::clone(&store), &snapshot).await;

        let (fetcher, server) = spawn_worker(Arc::clone(&store), vec![seg]).await;
        let distributed = Distributed::new(
            Arc::new(fetcher),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
            },
        );
        let accounting = PhaseAccounting::new();
        let triple = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &EngineConfig::default(),
                i64::MAX,
                None,
            )
            .await
            .expect("distributed fetch")
            .expect(
                "a histogram slice is now served over the wire (PROTOCOL_VERSION 3), \
                 not refused to local fallback",
            )
            .0;
        server.abort();

        // The distributed histogram runs (triple's third element) equal the local
        // fetch run-for-run, records bit-identical. The scalar half is empty.
        assert!(
            triple.0.iter().all(|s| s.is_empty()),
            "a histogram-only segment yields no scalar series"
        );
        assert_histograms_bit_identical(&local_hist, &triple.2);
        // Not "two equal empties": the segment really carried histogram series.
        assert!(
            triple.2.iter().any(|s| !s.is_empty()),
            "the distributed path must actually carry the histogram series"
        );
        assert!(
            accounting.snapshot().pooled().total_s3_bytes() > 0,
            "the worker's real spend is folded into the query accounting"
        );
    });
}

/// The worker-side histogram erasure wiring (ADR-0096 decision 3 step 3) is
/// real, not latent, and runs before the histogram series are encoded onto the
/// wire (decision 3 step 4). An erasure predicate that erases every sample of
/// the only histogram series present drops it entirely, so the distributed
/// result carries no histogram runs (and no scalar ones), exactly as a local
/// fetch over the same erasure would.
///
/// The single line that makes this hold is the
/// `crate::erasure::retain_histogram_series(&mut histograms, &erasure)` call in
/// `service.rs::run_slice_metrics`: deleting it leaves the histogram series
/// standing and the distributed result would carry it, failing the empty-result
/// assertions below.
#[test]
fn erased_histogram_series_is_dropped_before_the_wire() {
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
        let object_key = "seg/hist_erase0.rseg".to_string();
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
            segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
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
        // A windowless predicate on the series' own label erases the whole
        // series (`retain_histogram_series` drops it entirely).
        let erasure = vec![ErasurePredicate::windowless(vec![(
            "__name__".to_string(),
            metric.to_string(),
        )])];
        let accounting = PhaseAccounting::new();
        let result = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &erasure,
                &accounting,
                &EngineConfig::default(),
                i64::MAX,
                None,
            )
            .await
            .expect("unsupported must not be an error");
        assert!(
            result.is_some(),
            "an erased histogram slice is served (empty), never a local fallback"
        );
        let ((per_slice, _stats, per_slice_hist), _partials) =
            result.expect("a real (non-fallback) result");
        assert!(
            per_slice.iter().all(|series| series.is_empty()),
            "the erased histogram series must not surface as a scalar series"
        );
        assert!(
            per_slice_hist.iter().all(|series| series.is_empty()),
            "the erased histogram series must be dropped before the wire, not sent"
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
        // RLOG and RSPAN are both on trailer version 4; this helper's segment
        // set is format-agnostic to the distributed path under test.
        segment_format_version: 4,
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
    let accounting = PhaseAccounting::new();
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
                    &PhaseAccounting::new(),
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
        let accounting = PhaseAccounting::new();
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
        // RLOG and RSPAN are both on trailer version 4; this helper's segment
        // set is format-agnostic to the distributed path under test.
        segment_format_version: 4,
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
    let accounting = PhaseAccounting::new();
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
/// - seg_a (shard 0), sorted on disk by (trace_id, start): TA/S2 (start 10),
///   TB/S1 (start 15).
/// - seg_b (shard 1): TA/S1 (start 20), TB/S2 (start 25).
///
/// The correct merge order is TA/S1, TA/S2, TB/S1, TB/S2; slice-order concat
/// (seg_a then seg_b) is TA/S2, TB/S1, TA/S1, TB/S2 -- different, so the merge's
/// `sort_by` line is under test.
///
/// Crucially, trace TA's span_id order DISAGREES with its start_ts order: S1 < S2
/// by span_id, but S1 starts at 20 while S2 starts at 10, so by start_ts S2 comes
/// first. This is what makes the corpus discriminate the key's field sequence: a
/// key that put `start_ts_ns` ahead of `span_id`, or dropped `span_id` entirely
/// and ordered TA by `start_ts` alone, would emit TA/S2 before TA/S1 and fail the
/// `correct_ids == [TA/S1, TA/S2, ...]` assertion. Under the old geometry (S1
/// start 10, S2 start 20) span_id and start_ts order agreed for both traces, so
/// swapping those two key fields, or truncating the key to `(trace_id,
/// start_ts)`, left every distrib test green.
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
                10,
                11,
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
                20,
                21,
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
            // ts 10 (slice 0) and ts 20 (slice 1), so the erased trace straddles
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
        let accounting = PhaseAccounting::new();
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

/// Direct discrimination test for every field of the documented span total order
/// `(trace_id, span_id, start_ts_ns, end_ts_ns, parent_span_id, name,
/// status_code, status_message, service_name, attrs)`. For each of the ten
/// fields, two `SpanRow`s are built identical in every OTHER field and differing
/// only in that one, and `span_order_key` must map them to different keys.
///
/// This is the field-level complement to the corpus geometry in
/// `split_trace_corpus`: it covers the seven fields the corpus can never reach
/// (end_ts_ns, parent_span_id, name, status_code, status_message, service_name,
/// attrs) and, unlike the corpus, does not decay if a later edit reshuffles the
/// corpus. Dropping any field from `span_order_key` -- in particular truncating
/// it to `(trace_id, start_ts_ns)` -- collapses that field's pair to an equal key
/// and fails the matching assertion below.
#[test]
fn span_order_key_discriminates_every_field() {
    // A canonical base row; each case clones it and perturbs exactly one field.
    let base = SpanRow {
        record: ravel_rspan::SpanRecord {
            trace_id: TA,
            span_id: S1,
            parent_span_id: Some([3u8; 8]),
            name: "op".to_string(),
            start_ts_ns: 10,
            end_ts_ns: 11,
            status_code: ravel_rspan::StatusCode::Unset,
            status_message: Some("msg".to_string()),
            attrs: vec![("k".to_string(), "v".to_string())],
        },
        service_name: Some("alpha".to_string()),
    };

    // Assert that perturbing exactly the named field (all others equal to `base`)
    // moves the key: proof that field participates and has not been dropped.
    let check = |field: &str, mutate: &dyn Fn(&mut SpanRow)| {
        let mut other = base.clone();
        mutate(&mut other);
        assert_ne!(
            span_order_key(&base),
            span_order_key(&other),
            "span_order_key must distinguish spans differing only in {field}; \
             if it does not, that field has been dropped from the key"
        );
    };

    // One case per documented key field, in the key's field order.
    check("trace_id", &|r| r.record.trace_id = TB);
    check("span_id", &|r| r.record.span_id = S2);
    check("start_ts_ns", &|r| r.record.start_ts_ns = 20);
    check("end_ts_ns", &|r| r.record.end_ts_ns = 99);
    check("parent_span_id", &|r| {
        r.record.parent_span_id = Some([7u8; 8])
    });
    check("name", &|r| r.record.name = "other".to_string());
    check("status_code", &|r| {
        r.record.status_code = ravel_rspan::StatusCode::Error
    });
    check("status_message", &|r| {
        r.record.status_message = Some("boom".to_string())
    });
    check("service_name", &|r| {
        r.service_name = Some("beta".to_string())
    });
    check("attrs", &|r| {
        r.record.attrs = vec![("k".to_string(), "w".to_string())]
    });
}

/// Field-precedence complement to [`span_order_key_discriminates_every_field`].
/// That test only proves each field participates in the key; it passes even if
/// two adjacent fields were swapped in `SpanOrderKey`'s declared order, because
/// a single-field mutation can't observe field *position*, only presence.
///
/// For every adjacent pair `(early, late)` in the documented order
/// `(trace_id, span_id, start_ts_ns, end_ts_ns, parent_span_id, name,
/// status_code, status_message, service_name, attrs)`, this builds two rows
/// identical everywhere except `early` and `late`, deliberately set so `early`
/// alone must decide the order: row "lo" has the smaller `early` value but the
/// LARGER `late` value; row "hi" has the larger `early` value but the smaller
/// `late` value. `span_order_key(lo) < span_order_key(hi)` only holds if
/// `early` is compared, and compared before `late`. Swapping the pair's
/// declared order (or dropping `early` from the key) would let `late` decide
/// instead, flipping the comparison and failing the assertion.
#[test]
fn span_order_key_respects_field_precedence() {
    let base = SpanRow {
        record: ravel_rspan::SpanRecord {
            trace_id: TA,
            span_id: S1,
            parent_span_id: Some([3u8; 8]),
            name: "op".to_string(),
            start_ts_ns: 10,
            end_ts_ns: 11,
            status_code: ravel_rspan::StatusCode::Unset,
            status_message: Some("msg".to_string()),
            attrs: vec![("k".to_string(), "v".to_string())],
        },
        service_name: Some("alpha".to_string()),
    };

    // (pair name, set `early`+`late` low on `lo` / high on `hi`, set `late`+`early`
    // reversed -- high on `lo` / low on `hi` -- on the SAME row).
    let pair_check = |pair: &str, lo_mut: &dyn Fn(&mut SpanRow), hi_mut: &dyn Fn(&mut SpanRow)| {
        let mut lo = base.clone();
        lo_mut(&mut lo);
        let mut hi = base.clone();
        hi_mut(&mut hi);
        assert!(
            span_order_key(&lo) < span_order_key(&hi),
            "in the ({pair}) pair, the earlier field must decide the order even \
             when the later field disagrees; if it does not, the fields are \
             either out of order or one has been dropped from the key"
        );
    };

    pair_check(
        "trace_id, span_id",
        &|r| {
            r.record.trace_id = TA;
            r.record.span_id = S2;
        },
        &|r| {
            r.record.trace_id = TB;
            r.record.span_id = S1;
        },
    );
    pair_check(
        "span_id, start_ts_ns",
        &|r| {
            r.record.span_id = S1;
            r.record.start_ts_ns = 20;
        },
        &|r| {
            r.record.span_id = S2;
            r.record.start_ts_ns = 10;
        },
    );
    pair_check(
        "start_ts_ns, end_ts_ns",
        &|r| {
            r.record.start_ts_ns = 10;
            r.record.end_ts_ns = 99;
        },
        &|r| {
            r.record.start_ts_ns = 20;
            r.record.end_ts_ns = 11;
        },
    );
    pair_check(
        "end_ts_ns, parent_span_id",
        &|r| {
            r.record.end_ts_ns = 10;
            r.record.parent_span_id = Some([7u8; 8]);
        },
        &|r| {
            r.record.end_ts_ns = 20;
            r.record.parent_span_id = Some([3u8; 8]);
        },
    );
    pair_check(
        "parent_span_id, name",
        &|r| {
            r.record.parent_span_id = Some([3u8; 8]);
            r.record.name = "z".to_string();
        },
        &|r| {
            r.record.parent_span_id = Some([7u8; 8]);
            r.record.name = "a".to_string();
        },
    );
    pair_check(
        "name, status_code",
        &|r| {
            r.record.name = "op".to_string();
            r.record.status_code = ravel_rspan::StatusCode::Error;
        },
        &|r| {
            r.record.name = "other".to_string();
            r.record.status_code = ravel_rspan::StatusCode::Unset;
        },
    );
    pair_check(
        "status_code, status_message",
        &|r| {
            r.record.status_code = ravel_rspan::StatusCode::Unset;
            r.record.status_message = Some("z".to_string());
        },
        &|r| {
            r.record.status_code = ravel_rspan::StatusCode::Error;
            r.record.status_message = Some("a".to_string());
        },
    );
    pair_check(
        "status_message, service_name",
        &|r| {
            r.record.status_message = Some("boom".to_string());
            r.service_name = Some("zeta".to_string());
        },
        &|r| {
            r.record.status_message = Some("msg".to_string());
            r.service_name = Some("alpha".to_string());
        },
    );
    pair_check(
        "service_name, attrs",
        &|r| {
            r.service_name = Some("alpha".to_string());
            r.record.attrs = vec![("k".to_string(), "z".to_string())];
        },
        &|r| {
            r.service_name = Some("beta".to_string());
            r.record.attrs = vec![("k".to_string(), "a".to_string())];
        },
    );
}

/// `merge_spans` sorts with the allocation-free [`span_cmp`] comparator rather
/// than building an owned [`span_order_key`] tuple per span (#307). The two are
/// separate definitions of the same total order and could silently drift, so
/// this pins them together: over a matrix of rows built to differ in each key
/// field (at least one differing pair per field, plus every cross pair),
/// `span_cmp(a, b)` must equal `span_order_key(a).cmp(&span_order_key(b))` for
/// every ordered pair -- including the reflexive `Equal` pairs. If a later edit
/// reordered `span_cmp`'s `then_with` chain, dropped a field, or flipped a
/// comparison relative to the key tuple, some pair would disagree and this fails.
#[test]
fn span_cmp_agrees_with_span_order_key() {
    let base = SpanRow {
        record: ravel_rspan::SpanRecord {
            trace_id: TA,
            span_id: S1,
            parent_span_id: Some([3u8; 8]),
            name: "op".to_string(),
            start_ts_ns: 10,
            end_ts_ns: 11,
            status_code: ravel_rspan::StatusCode::Unset,
            status_message: Some("msg".to_string()),
            attrs: vec![("k".to_string(), "v".to_string())],
        },
        service_name: Some("alpha".to_string()),
    };

    let mutate = |m: &dyn Fn(&mut SpanRow)| {
        let mut r = base.clone();
        m(&mut r);
        r
    };
    // The base plus one row differing in exactly each key field: every field
    // therefore appears in at least one differing pair (base vs its mutant), and
    // the full cartesian product below also exercises multi-field differences.
    let rows = vec![
        base.clone(),
        mutate(&|r| r.record.trace_id = TB),
        mutate(&|r| r.record.span_id = S2),
        mutate(&|r| r.record.start_ts_ns = 20),
        mutate(&|r| r.record.end_ts_ns = 99),
        mutate(&|r| r.record.parent_span_id = Some([7u8; 8])),
        mutate(&|r| r.record.parent_span_id = None),
        mutate(&|r| r.record.name = "other".to_string()),
        mutate(&|r| r.record.status_code = ravel_rspan::StatusCode::Error),
        mutate(&|r| r.record.status_message = Some("boom".to_string())),
        mutate(&|r| r.record.status_message = None),
        mutate(&|r| r.service_name = Some("beta".to_string())),
        mutate(&|r| r.service_name = None),
        mutate(&|r| r.record.attrs = vec![("k".to_string(), "w".to_string())]),
    ];

    for (i, a) in rows.iter().enumerate() {
        for (j, b) in rows.iter().enumerate() {
            assert_eq!(
                span_cmp(a, b),
                span_order_key(a).cmp(&span_order_key(b)),
                "span_cmp and span_order_key disagree on rows ({i}, {j}); \
                 the comparator has drifted from the key tuple"
            );
        }
    }
}

// --- Old-worker version skew (T4, #286) ------------------------------------
//
// ADR-0071's amendment (Decision 2) governs the skew direction a NEW coordinator
// faces against an OLD worker that predates the log/span fan-out: such a worker
// still runs the deleted pre-#283 `run_slice_inner` stub, which rejected ANY
// non-Metrics `Signal` with `Unsupported` BEFORE decoding the request body. The
// coordinator maps that `Unsupported` to a silent whole-query local fallback
// (`Distributed::fetch*` returning `Ok(None)`, distrib/mod.rs), never a wrong or
// partial answer. That fallback direction has never been exercised over the wire
// because today's coordinator refuses non-Metrics before dispatch and the real
// worker no longer carries the stub; this section reconstructs the old wire
// behavior with a worker double and pins it for Logs, Alerts, Audit, and Spans.

/// A gRPC `SeriesFetch` worker double pinned to the PRE-#283 behavior: it rejects
/// ANY non-Metrics `Signal` with `Unsupported` BEFORE decoding the request body,
/// exactly as `run_slice_inner`'s now-deleted stub did (a "not yet implemented;
/// only Metrics is distributed" summary). This is the not-yet-upgraded-worker
/// skew direction ADR-0071's amendment (Decision 2) calls out.
///
/// It is deliberately NOT `SeriesFetchService` with a fetcher left unwired (that
/// is the `*_without_*_fetcher_signals_local_fallback` tests): the real service
/// now decodes tenant/matchers/erasure and dispatches on the signal, so a
/// fetcher-missing worker rejects INSIDE the per-signal path (after decode) and
/// never emits the "not yet implemented" stub message. This double reproduces
/// the old pre-decode blanket rejection instead, so the skew test exercises an
/// old worker's actual wire behavior, not a mis-wired new one.
struct OldMetricsOnlyWorker;

#[tonic::async_trait]
impl SeriesFetch for OldMetricsOnlyWorker {
    type FetchStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<pb::FetchResponse, tonic::Status>> + Send + 'static>,
    >;

    async fn fetch(
        &self,
        request: tonic::Request<pb::FetchRequest>,
    ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
        // The old worker inspected the raw signal discriminant and rejected any
        // non-Metrics value immediately, before decoding tenant/matchers/erasure.
        // Reproduce exactly that: no request-body decode happens here.
        let signal = request.into_inner().signal;
        let (code, message) = if signal == crate::distrib::codec::signal_to_u32(Signal::Metrics) {
            // Metrics was served by the old worker; not exercised by this test
            // (which only dispatches non-Metrics signals). An empty Ok summary.
            (pb::status::Code::Ok, String::new())
        } else {
            (
                pb::status::Code::Unsupported,
                "signal not yet implemented; only Metrics is distributed".to_string(),
            )
        };
        let summary = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                accounting: None,
                phase_accounting: vec![
                    pb::QueryAccountingSnapshot::default();
                    QueryPhase::ALL.len()
                ],
                series_returned: 0,
                samples_returned: 0,
                status: Some(pb::Status {
                    code: code as i32,
                    message,
                }),
                raw_f64_pages: 0,
                raw_f64_bytes: 0,
            })),
        };
        let stream = futures::stream::iter(vec![Ok(summary)]);
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

/// Starts an [`OldMetricsOnlyWorker`] on `127.0.0.1:0` and returns a
/// `RemoteSliceFetcher` connected to it plus the server task handle.
async fn spawn_old_worker() -> (RemoteSliceFetcher, JoinHandle<()>) {
    let service = SeriesFetchServer::new(OldMetricsOnlyWorker);
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("addr")).expect("bind");
    let addr = incoming.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_lazy();
    (RemoteSliceFetcher::new(channel), handle)
}

/// A raw slice request for a signal, used to probe the worker double at the wire
/// level (below the coordinator). `tenant` is passed through verbatim so a
/// malformed value can prove the old worker rejects BEFORE tenant decode.
fn raw_slice_request(signal: Signal, tenant: Vec<u8>) -> pb::FetchRequest {
    pb::FetchRequest {
        protocol_version: crate::distrib::codec::PROTOCOL_VERSION,
        query_id: Vec::new(),
        tenant_hash: tenant,
        signal: crate::distrib::codec::signal_to_u32(signal),
        scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
            segments: Vec::new(),
        })),
        matchers: Vec::new(),
        window_start_ns: 0,
        window_end_ns: 0,
        budgets: None,
        deadline_unix_ns: 0,
        erasure: Vec::new(),
        trace_context: String::new(),
        fragment_capability: Vec::new(),
        partial_aggregate: None,
    }
}

/// The one skew direction the epic confirmed is not yet covered: a NEW
/// coordinator dispatching a non-Metrics signal to an OLD worker that still runs
/// the pre-#283 blanket rejection. For each of Logs, Alerts, Audit, and Spans the
/// coordinator must degrade to a silent whole-query local fallback (`Ok(None)`,
/// never an error and never a partial result), and the local path it falls back
/// to must produce the complete, correctly ordered result over the same corpus.
///
/// Distinct from `logs_worker_without_log_fetcher_signals_local_fallback` /
/// `spans_worker_without_span_fetcher_signals_local_fallback`, which use the real
/// dispatch service with a fetcher left unwired (a mis-wired NEW worker). Here the
/// double reproduces the OLD worker's pre-decode reject, proven two ways below:
/// the "not yet implemented" stub message on the wire, and a malformed-tenant
/// Logs request that still returns `Unsupported` (the new worker returns `BadData`
/// there, because it decodes the tenant before dispatching on the signal -- see
/// `run_slice_inner_dispatches_on_signal_not_blanket_rejects`).
#[test]
fn old_worker_rejecting_nonmetrics_signals_yields_silent_local_fallback() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        // --- Wire-level proof the double IS the old pre-decode reject. ---
        let (old, server) = spawn_old_worker().await;

        // A Logs request with a malformed tenant hash still comes back
        // Unsupported, NOT BadData: the old worker rejects the signal before it
        // decodes the tenant. This is the structural inverse of the new worker's
        // `BadData`-on-bad-tenant behavior, so it pins the double to the deleted
        // pre-decode stub rather than a fetcher-missing new service.
        let bad_tenant =
            SliceFetcher::fetch_logs(&old, raw_slice_request(Signal::Logs, vec![0u8; 3]))
                .await
                .expect("old worker responds to a malformed-tenant logs request");
        assert_eq!(
            bad_tenant.status,
            pb::status::Code::Unsupported,
            "the old worker rejects the signal before decoding the tenant (pre-decode stub), \
             so a bad tenant is still Unsupported, not BadData"
        );
        assert!(
            bad_tenant.status_message.contains("not yet implemented"),
            "the reject carries the old stub message, got {:?}",
            bad_tenant.status_message
        );

        // Every RLOG-family signal is rejected with the old stub message.
        for signal in [Signal::Logs, Signal::Alerts, Signal::Audit] {
            let resp = SliceFetcher::fetch_logs(&old, raw_slice_request(signal, TENANT.0.to_vec()))
                .await
                .expect("old worker responds to a log-family request");
            assert_eq!(
                resp.status,
                pb::status::Code::Unsupported,
                "{signal:?}: the old worker rejects it as Unsupported"
            );
            assert!(
                resp.status_message.contains("not yet implemented"),
                "{signal:?}: the reject carries the old stub message, got {:?}",
                resp.status_message
            );
        }
        // Spans likewise, over the span decode path.
        let span_reject =
            SliceFetcher::fetch_spans(&old, raw_slice_request(Signal::Spans, TENANT.0.to_vec()))
                .await
                .expect("old worker responds to a spans request");
        assert_eq!(
            span_reject.status,
            pb::status::Code::Unsupported,
            "Spans: the old worker rejects it as Unsupported"
        );
        assert!(
            span_reject.status_message.contains("not yet implemented"),
            "Spans: the reject carries the old stub message, got {:?}",
            span_reject.status_message
        );
        server.abort();

        // --- RLOG family: coordinator degrades to silent Ok(None). ---
        let store = Arc::new(MemoryStore::new());
        let segments = split_stream_corpus(&store).await;
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        // A non-empty corpus that partitions into two slices, so an Ok(None) here
        // is a genuine whole-query fallback, not the empty-snapshot
        // Ok(Some(vec![])) shortcut in `fetch_logs`.
        assert_eq!(
            partition_snapshot(&snapshot, 2).len(),
            2,
            "the corpus must produce a real multi-slice fan-out for the fallback to be meaningful"
        );
        // The result the local fallback path yields over this corpus: complete
        // (all six records) and in the global cross-segment total order. Ok(None)
        // routes the engine to exactly this local read, so this is the eventual
        // answer -- correct, never partial.
        let local_logs = local_log_records(Arc::clone(&store), &segments, &[]).await;
        let local_log_ts: Vec<i64> = sorted_by_order_key(local_logs)
            .iter()
            .map(|r| r.ts_ns)
            .collect();
        assert_eq!(
            local_log_ts,
            vec![10, 15, 20, 25, 30, 40],
            "the local fallback produces the complete, correctly ordered record set"
        );

        for signal in [Signal::Logs, Signal::Alerts, Signal::Audit] {
            let (old, server) = spawn_old_worker().await;
            let distributed = Distributed::new(
                Arc::new(old),
                DistribThresholds {
                    min_store_bytes: 0,
                    min_segments: 0,
                    max_parallel_slices: 2,
                },
            );
            let result = distributed
                .fetch_logs(
                    TENANT,
                    signal,
                    &snapshot,
                    &[],
                    &[],
                    &PhaseAccounting::new(),
                    &EngineConfig::default(),
                    i64::MAX,
                )
                .await
                .expect("an old worker's Unsupported must not surface as an error");
            assert!(
                result.is_none(),
                "{signal:?}: an old worker rejecting the signal yields whole-query local \
                 fallback Ok(None), never a wrong or partial result"
            );
            server.abort();
        }

        // --- Spans: same silent Ok(None) fallback, correct local result. ---
        let span_store = Arc::new(MemoryStore::new());
        let span_segments = split_trace_corpus(&span_store).await;
        let span_snapshot = Snapshot {
            segments: span_segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        assert_eq!(
            partition_snapshot(&span_snapshot, 2).len(),
            2,
            "the span corpus must produce a real multi-slice fan-out"
        );
        let local_spans = local_span_rows(Arc::clone(&span_store), &span_segments, &[]).await;
        assert_eq!(
            span_id_seq(&sorted_spans_by_order_key(local_spans)),
            vec![(TA, S1), (TA, S2), (TB, S1), (TB, S2)],
            "the local fallback produces the complete, correctly ordered span set"
        );

        let (old, server) = spawn_old_worker().await;
        let distributed = Distributed::new(
            Arc::new(old),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 2,
            },
        );
        let result = distributed
            .fetch_spans(
                TENANT,
                Signal::Spans,
                &span_snapshot,
                &[],
                &[],
                &PhaseAccounting::new(),
                &EngineConfig::default(),
                i64::MAX,
            )
            .await
            .expect("an old worker's Unsupported must not surface as an error");
        assert!(
            result.is_none(),
            "Spans: an old worker rejecting the signal yields whole-query local fallback \
             Ok(None), never a wrong or partial result"
        );
        server.abort();
    });
}

// --- ADR-0103 aggregation pushdown (worker side) ---------------------------
//
// A slice whose request carries a `PartialAggregateRequest` returns one
// `PartialAggregate` per series instead of its raw runs. The property that makes
// such a partial exact is the worker's OWN local merge: it must dedup its runs
// before reducing, or a sample two of its segments both carry is counted twice.
// The tests below drive the real worker service (no doubles) and compare against
// a local fetch-merge-reduce over the identical data.

/// The corpus the pushdown tests share: two segments in the SAME shard and hour
/// (so a coordinator would put them in ONE slice, i.e. on one worker) whose runs
/// of series `m0` overlap at two timestamps, plus a second series `m1` carrying
/// `0.0` and `-0.0` so the min/max fold's total order is observable.
///
/// `m0` merged (the later `writer_seq` wins each duplicate timestamp):
/// `1.0, -0.0, 42.0, 7.5` -- four samples, from six fetched.
async fn partial_pushdown_corpus(store: &MemoryStore) -> Vec<SegmentRef> {
    let first = write_segment(
        store,
        0,
        0,
        100,
        &[
            SeriesDesc {
                metric: "m0".to_string(),
                samples: vec![
                    (NS, 1.0f64.to_bits()),
                    (2 * NS, 2.0f64.to_bits()),
                    (3 * NS, 3.0f64.to_bits()),
                ],
            },
            SeriesDesc {
                metric: "m1".to_string(),
                samples: vec![(NS, 0.0f64.to_bits()), (2 * NS, (-0.0f64).to_bits())],
            },
        ],
    )
    .await;
    let second = write_segment(
        store,
        1,
        0,
        100,
        &[SeriesDesc {
            metric: "m0".to_string(),
            // ts 2 and ts 3 duplicate the first segment's samples with different
            // values; this segment's higher `writer_seq` wins both.
            samples: vec![
                (2 * NS, (-0.0f64).to_bits()),
                (3 * NS, 42.0f64.to_bits()),
                (4 * NS, 7.5f64.to_bits()),
            ],
        }],
    )
    .await;
    vec![first, second]
}

/// A `FetchRequest` for the whole pinned set, with `partial_aggregate` set to
/// `want`.
fn pushdown_request(
    segments: &[SegmentRef],
    want: Option<pb::PartialAggregateRequest>,
) -> pb::FetchRequest {
    pb::FetchRequest {
        protocol_version: crate::distrib::codec::PROTOCOL_VERSION,
        query_id: Vec::new(),
        tenant_hash: TENANT.0.to_vec(),
        signal: crate::distrib::codec::signal_to_u32(Signal::Metrics),
        scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
            segments: segments
                .iter()
                .map(crate::distrib::codec::encode_segment_identity)
                .collect(),
        })),
        matchers: Vec::new(),
        window_start_ns: 0,
        window_end_ns: 0,
        budgets: None,
        deadline_unix_ns: 0,
        erasure: Vec::new(),
        trace_context: String::new(),
        fragment_capability: Vec::new(),
        partial_aggregate: want,
    }
}

/// The local reference reduction: `count`, `min` bits, and `max` bits over one
/// coordinator-merged series, folded under `f64::total_cmp` exactly as ADR-0023's
/// min/max UDAF does. This is the answer the worker must reproduce.
fn reference_reduction(series: &SeriesData) -> (u64, Option<u64>, Option<u64>) {
    let fold = |want: std::cmp::Ordering| {
        series
            .samples
            .iter()
            .map(|s| s.value)
            .reduce(|current, candidate| {
                if candidate.total_cmp(&current) == want {
                    candidate
                } else {
                    current
                }
            })
            .map(f64::to_bits)
    };
    (
        series.samples.len() as u64,
        fold(std::cmp::Ordering::Less),
        fold(std::cmp::Ordering::Greater),
    )
}

/// Indexes decoded partials by their metric name, which is this corpus' whole
/// label set.
fn partials_by_metric(
    partials: &[crate::distrib::codec::PartialAggregate],
) -> std::collections::HashMap<String, &crate::distrib::codec::PartialAggregate> {
    partials
        .iter()
        .map(|p| {
            let metric = p
                .labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.clone())
                .expect("corpus labels carry __name__");
            (metric, p)
        })
        .collect()
}

/// ADR-0103 acceptance (worker side): a slice asked for `count`/`min`/`max`
/// merges its own runs FIRST and returns exactly what a local
/// fetch-merge-reduce over the identical data produces, over a real loopback
/// worker and a real decoder.
///
/// The corpus is the case that separates a correct implementation from a naive
/// one: series `m0` has six fetched samples across two segments of one slice,
/// two of which are duplicate timestamps, so the exact answer is `count = 4`.
/// Replacing the `merge_soa_runs` call in `reduce_partial_aggregates`
/// (`service.rs`) with a per-run concatenation reports `count = 6` -- the
/// duplicates double-counted -- and the `count` assertion below fails. The
/// `-0.0` min on `m0` and the `0.0`/`-0.0` pair on `m1` pin the fold's total
/// order: under `PartialOrd` (where `-0.0 == 0.0`) `m1`'s min comes back as
/// `0.0`, whose bit pattern differs from the asserted `-0.0`.
#[test]
fn worker_partial_aggregate_merges_before_reducing() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = partial_pushdown_corpus(&store).await;
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };

        // Local reference: the same fetch, the same coordinator-side merge, then
        // the reduction the worker is supposed to have done.
        let (local_runs, _acct, _stats) = local_scalar(Arc::clone(&store), &snapshot).await;
        let fetched_samples: usize = local_runs
            .iter()
            .flatten()
            .map(|s| s.timestamps.len())
            .sum();
        let local_merged = merge_soa_runs(local_runs, usize::MAX, usize::MAX).expect("local merge");
        let merged_samples: usize = local_merged.iter().map(|s| s.samples.len()).sum();
        // The corpus must actually exercise dedup, or the merge-first property is
        // untested: fewer samples survive the merge than were fetched.
        assert!(
            merged_samples < fetched_samples,
            "corpus must carry cross-segment duplicates: fetched {fetched_samples}, \
             merged {merged_samples}"
        );

        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments.clone()).await;
        let response = SliceFetcher::fetch(
            &fetcher,
            pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: true,
                    want_max: true,
                    reduce_start_ns: None,
                    reduce_end_ns: None,
                }),
            ),
        )
        .await
        .expect("worker responds to a pushdown request");
        server.abort();

        assert_eq!(response.status, pb::status::Code::Ok);
        // All partials, no raw frames: the branch is per request, not per series.
        assert!(
            response.scalar.is_empty(),
            "a pushdown slice must not also stream raw series frames"
        );
        assert!(response.histogram.is_empty());
        assert_eq!(
            response.partials.len(),
            local_merged.len(),
            "one partial per merged series, not one per segment run"
        );

        let got = partials_by_metric(&response.partials);
        for series in &local_merged {
            let metric = series
                .labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.clone())
                .expect("corpus labels carry __name__");
            let partial = got.get(&metric).expect("a partial for every merged series");
            let (count, min_bits, max_bits) = reference_reduction(series);
            assert_eq!(
                partial.count,
                Some(count),
                "{metric}: count must be the deduped sample count"
            );
            assert_eq!(
                partial.min.map(f64::to_bits),
                min_bits,
                "{metric}: min bit pattern differs from the local reduction"
            );
            assert_eq!(
                partial.max.map(f64::to_bits),
                max_bits,
                "{metric}: max bit pattern differs from the local reduction"
            );
        }

        // Hand-computed, independent of the reference fold above: `m0` dedups to
        // four samples with `-0.0` as its minimum, and `m1`'s `0.0`/`-0.0` pair
        // separates `total_cmp` from `PartialOrd`.
        let m0 = got.get("m0").expect("m0 partial");
        assert_eq!(m0.count, Some(4));
        assert_eq!(m0.min.map(f64::to_bits), Some((-0.0f64).to_bits()));
        assert_eq!(m0.max.map(f64::to_bits), Some(42.0f64.to_bits()));
        let m1 = got.get("m1").expect("m1 partial");
        assert_eq!(m1.count, Some(2));
        assert_eq!(m1.min.map(f64::to_bits), Some((-0.0f64).to_bits()));
        assert_eq!(m1.max.map(f64::to_bits), Some(0.0f64.to_bits()));

        // The summary still reports this slice's real yield, so the coordinator's
        // own sample-budget re-check works even though no sample crossed the wire.
        assert_eq!(response.series_returned, local_merged.len() as u64);
        assert_eq!(response.samples_returned, merged_samples as u64);
    });
}

/// A group-only request (no aggregate flag set, ADR-0103 decision 3's set-union
/// case) enumerates the worker's distinct series: one frame per merged series,
/// identity only, with every value field absent rather than a zero.
#[test]
fn worker_group_only_partial_aggregate_enumerates_series() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = partial_pushdown_corpus(&store).await;
        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments.clone()).await;
        let response = SliceFetcher::fetch(
            &fetcher,
            pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: false,
                    want_min: false,
                    want_max: false,
                    reduce_start_ns: None,
                    reduce_end_ns: None,
                }),
            ),
        )
        .await
        .expect("worker responds to a group-only request");
        server.abort();

        assert_eq!(response.status, pb::status::Code::Ok);
        assert!(response.scalar.is_empty());
        // Two distinct series in the corpus, each held once even though `m0`'s
        // runs came from two segments.
        assert_eq!(response.partials.len(), 2);
        let got = partials_by_metric(&response.partials);
        for metric in ["m0", "m1"] {
            let partial = got.get(metric).expect("a partial per distinct series");
            assert_eq!(partial.count, None, "{metric}: count was not requested");
            assert_eq!(partial.min, None, "{metric}: min was not requested");
            assert_eq!(partial.max, None, "{metric}: max was not requested");
        }
        // The identity a group enumeration exists to carry is present.
        let expected_id = SeriesId::compute(&tenant_id(), "m0", &labels("m0")).expect("series id");
        assert_eq!(got.get("m0").expect("m0 partial").series_id, expected_id);
    });
}

/// Regression guard for the "completely unchanged" half of the request-level
/// branch: a request with NO `partial_aggregate` produces exactly the raw-frame
/// sequence the pre-pushdown worker produced -- one `SeriesFrame` per fetched
/// per-segment run, in fetch order, byte-identical on the wire, and no
/// `PartialAggregate` frame anywhere.
///
/// The expected bytes are built from an independent local fetch through
/// `encode_series_frame`, which is exactly what the raw path's encode loop does,
/// so this compares the worker's output against the encoding rather than against
/// itself.
#[test]
fn request_without_partial_aggregate_streams_unchanged_raw_frames() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        use prost::Message;

        let store = Arc::new(MemoryStore::new());
        let segments = partial_pushdown_corpus(&store).await;
        let snapshot = Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let (local_runs, _acct, _stats) = local_scalar(Arc::clone(&store), &snapshot).await;
        let expected: Vec<Vec<u8>> = local_runs
            .iter()
            .flatten()
            .map(|soa| {
                pb::FetchResponse {
                    frame: Some(pb::fetch_response::Frame::Series(
                        crate::distrib::codec::encode_series_frame(soa),
                    )),
                }
                .encode_to_vec()
            })
            .collect();

        // Drive the service directly so the raw frames are observable before any
        // decode collapses them.
        let metrics_store: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
        let service = SeriesFetchService::new(
            SegmentFetcher::new(metrics_store),
            Arc::new(SnapshotSegmentResolver::new(segments.clone())),
        );
        let response = SeriesFetch::fetch(
            &service,
            tonic::Request::new(pushdown_request(&segments, None)),
        )
        .await
        .expect("worker serves the raw request");
        let frames: Vec<pb::FetchResponse> =
            futures::StreamExt::collect::<Vec<_>>(response.into_inner())
                .await
                .into_iter()
                .map(|f| f.expect("frame"))
                .collect();

        let (summary, series): (Vec<_>, Vec<_>) = frames
            .iter()
            .partition(|f| matches!(f.frame, Some(pb::fetch_response::Frame::Summary(_))));
        assert_eq!(summary.len(), 1, "exactly one terminal summary");
        assert!(
            !frames.iter().any(|f| matches!(
                f.frame,
                Some(pb::fetch_response::Frame::PartialAggregate(_))
            )),
            "a request with no partial_aggregate must never yield a partial frame"
        );
        let got: Vec<Vec<u8>> = series.iter().map(|f| f.encode_to_vec()).collect();
        assert_eq!(
            got, expected,
            "raw series frames must be byte-identical to the unchanged encode path"
        );
    });
}

/// Pushdown is metrics-only (ADR-0103): a Logs slice carrying an aggregate
/// request is refused with `Unsupported` so the coordinator falls back, rather
/// than being served raw log frames a pushdown-expecting caller never asked for.
#[test]
fn partial_aggregate_on_a_log_slice_is_unsupported() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = partial_pushdown_corpus(&store).await;
        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments.clone()).await;
        let mut request = pushdown_request(
            &segments,
            Some(pb::PartialAggregateRequest {
                want_count: true,
                want_min: false,
                want_max: false,
                reduce_start_ns: None,
                reduce_end_ns: None,
            }),
        );
        request.signal = crate::distrib::codec::signal_to_u32(Signal::Logs);
        let response = SliceFetcher::fetch_logs(&fetcher, request)
            .await
            .expect("worker responds");
        server.abort();
        assert_eq!(response.status, pb::status::Code::Unsupported);
        assert!(
            response.status_message.contains("pushdown"),
            "expected the pushdown-not-defined refusal, got {:?}",
            response.status_message
        );
    });
}

/// ADR-0103 amendment: the reduction-window fields are load-bearing. A worker
/// given `reduce_start_ns`/`reduce_end_ns` counts and folds only the samples in
/// `(reduce_start_ns, reduce_end_ns]` -- exclusive start, inclusive end, matching
/// `eval_matrix_selector`.
///
/// The corpus is one series with four samples at `1..=4` NS. The window
/// `(2*NS, 4*NS]` keeps exactly `{3*NS, 4*NS}`. Two boundary cases are the point,
/// not just somewhere-inside vs somewhere-outside:
///   - `2*NS` sits exactly AT the exclusive start and must be EXCLUDED (an
///     inclusive start would count it, giving `count = 3` and `min = 5.0`).
///   - `4*NS` sits exactly AT the inclusive end and must be INCLUDED (an
///     exclusive end would drop it, giving `count = 1`).
///
/// The `1*NS` sample sits outside the window entirely; its `100.0` value would
/// become the `max` if the window were ignored, so `max = 7.0` proves the window
/// gates the fold, not just the count.
#[test]
fn worker_partial_aggregate_reduction_window_is_load_bearing() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = vec![
            write_segment(
                &store,
                0,
                0,
                100,
                &[SeriesDesc {
                    metric: "m".to_string(),
                    samples: vec![
                        (NS, 100.0f64.to_bits()),
                        (2 * NS, 5.0f64.to_bits()),
                        (3 * NS, 7.0f64.to_bits()),
                        (4 * NS, 3.0f64.to_bits()),
                    ],
                }],
            )
            .await,
        ];

        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments.clone()).await;
        let response = SliceFetcher::fetch(
            &fetcher,
            pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: true,
                    want_max: true,
                    reduce_start_ns: Some(2 * NS),
                    reduce_end_ns: Some(4 * NS),
                }),
            ),
        )
        .await
        .expect("worker responds to a windowed pushdown request");
        server.abort();

        assert_eq!(response.status, pb::status::Code::Ok);
        let got = partials_by_metric(&response.partials);
        let m = got.get("m").expect("m partial");
        assert_eq!(
            m.count,
            Some(2),
            "only the two in-window samples (3*NS, 4*NS) are counted; \
             the AT-start (2*NS) and out-of-window (1*NS) samples are excluded"
        );
        assert_eq!(
            m.min.map(f64::to_bits),
            Some(3.0f64.to_bits()),
            "min folds only in-window values (3.0 at 4*NS)"
        );
        assert_eq!(
            m.max.map(f64::to_bits),
            Some(7.0f64.to_bits()),
            "max folds only in-window values (7.0 at 3*NS), never the \
             out-of-window 100.0 at 1*NS"
        );
        // The summary's reduced-sample count also reflects the window, not the
        // fetched total.
        assert_eq!(response.samples_returned, 2);
    });
}

/// ADR-0103 amendment: the two reduction-window fields are one window, so a lone
/// bound is a caller bug. The worker rejects a request carrying exactly one of
/// `reduce_start_ns`/`reduce_end_ns` with a typed `Internal` status (never a
/// silent one-sided filter), in either order.
#[test]
fn worker_partial_aggregate_lone_window_bound_is_internal_error() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = partial_pushdown_corpus(&store).await;
        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments.clone()).await;

        // Only the start bound set.
        let start_only = SliceFetcher::fetch(
            &fetcher,
            pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: false,
                    want_max: false,
                    reduce_start_ns: Some(NS),
                    reduce_end_ns: None,
                }),
            ),
        )
        .await
        .expect("worker responds");
        assert_eq!(start_only.status, pb::status::Code::Internal);
        assert!(
            start_only.status_message.contains("reduce_start_ns")
                && start_only.status_message.contains("both or neither"),
            "expected the lone-bound refusal, got {:?}",
            start_only.status_message
        );

        // Only the end bound set: the mirror-image caller bug is rejected the
        // same way.
        let end_only = SliceFetcher::fetch(
            &fetcher,
            pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: false,
                    want_max: false,
                    reduce_start_ns: None,
                    reduce_end_ns: Some(4 * NS),
                }),
            ),
        )
        .await
        .expect("worker responds");
        server.abort();
        assert_eq!(end_only.status, pb::status::Code::Internal);
        assert!(
            end_only.status_message.contains("reduce_end_ns")
                && end_only.status_message.contains("both or neither"),
            "expected the lone-bound refusal, got {:?}",
            end_only.status_message
        );
    });
}

/// ADR-0103 amendment: staleness filtering is load-bearing. A series whose only
/// merged sample in the reduction window is a `STALE_NAN_BITS` marker must
/// contribute `count: Some(0)`, not `Some(1)` -- the evaluator drops the marker
/// before any range function sees it, and the worker's pushed-down count must
/// match.
///
/// Mutation proof: this test is RED against a worker missing the staleness
/// filter. Removing the `.filter(|s| s.value.to_bits() != STALE_NAN_BITS)` line
/// in `reduce_partial_aggregates` (`service.rs`) makes the marker count as a
/// real sample, so `count` comes back `Some(1)` and the assertion below fails.
/// The window here covers the marker's timestamp, so it is the staleness filter,
/// not the window, that removes it.
#[test]
fn worker_partial_aggregate_filters_staleness_before_counting() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
        let store = Arc::new(MemoryStore::new());
        let segments = vec![
            write_segment(
                &store,
                0,
                0,
                100,
                &[SeriesDesc {
                    metric: "s".to_string(),
                    // The series' only sample is a staleness marker, inside the
                    // window below.
                    samples: vec![(2 * NS, STALE_NAN_BITS)],
                }],
            )
            .await,
        ];

        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments.clone()).await;
        let response = SliceFetcher::fetch(
            &fetcher,
            pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: false,
                    want_max: false,
                    reduce_start_ns: Some(NS),
                    reduce_end_ns: Some(3 * NS),
                }),
            ),
        )
        .await
        .expect("worker responds to a pushdown request");
        server.abort();

        assert_eq!(response.status, pb::status::Code::Ok);
        let got = partials_by_metric(&response.partials);
        let s = got.get("s").expect("a partial for the stale-only series");
        assert_eq!(
            s.count,
            Some(0),
            "a staleness marker must not inflate the pushed-down count"
        );
        // The reduced-sample tally the summary reports also excludes the marker.
        assert_eq!(response.samples_returned, 0);
    });
}

/// ADR-0103 amendment: the staleness filter must run AFTER `merge_soa_runs`,
/// not before. Two segments carry the same series at the same timestamp: one
/// a real value (written first, lower priority), one a staleness marker
/// (written later, higher priority, so it wins the merge's dedup tie-break).
/// A pre-merge filter would drop the marker before the merge sees it, letting
/// the losing real value take the slot the raw path resolves to the marker --
/// diverging from what the same query gets on the local path. This test is
/// the discriminator `worker_partial_aggregate_filters_staleness_before_counting`
/// alone cannot be: that test's single-segment, single-sample corpus makes
/// pre-merge and post-merge filtering produce the identical answer.
#[test]
fn staleness_filter_runs_after_the_merge() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
        let store = Arc::new(MemoryStore::new());
        // priority (0,1,0): the real sample, written first.
        let real = write_segment_prov(
            &store,
            0,
            0,
            1,
            0,
            0,
            100,
            &[SeriesDesc {
                metric: "d".to_string(),
                samples: vec![(2 * NS, 5.0f64.to_bits())],
            }],
        )
        .await;
        // priority (0,1,1): the marker, written later, so it wins the merge at
        // the shared timestamp 2*NS.
        let marker = write_segment_prov(
            &store,
            1,
            0,
            1,
            1,
            0,
            100,
            &[SeriesDesc {
                metric: "d".to_string(),
                samples: vec![(2 * NS, STALE_NAN_BITS)],
            }],
        )
        .await;
        let segments = vec![real, marker];

        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments.clone()).await;
        let response = SliceFetcher::fetch(
            &fetcher,
            pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: false,
                    want_max: false,
                    reduce_start_ns: Some(NS),
                    reduce_end_ns: Some(3 * NS),
                }),
            ),
        )
        .await
        .expect("worker responds to a pushdown request");
        server.abort();

        assert_eq!(response.status, pb::status::Code::Ok);
        let got = partials_by_metric(&response.partials);
        let d = got.get("d").expect("a partial for the merged series");
        assert_eq!(
            d.count,
            Some(0),
            "the marker wins the merge at 2*NS, so the post-merge staleness \
             filter must leave nothing to count; a pre-merge filter would let \
             the losing real value (5.0) take the slot instead"
        );
    });
}

/// ADR-0103 amendment F1: a slice that spends real fetch cost before refusing a
/// pushdown over native-histogram data must report that cost, not lose it. The
/// native-histogram refusal now returns an `Unsupported` terminal summary
/// carrying the accounting the segment fetches already paid for, instead of an
/// `Err` that `run_slice` rebuilds from a zero-cost default snapshot.
///
/// Mutation proof: this test is RED against the pre-fix code on `main`. Restoring
/// the refusal to `return Err((pb::status::Code::Unsupported, ...))` sends the
/// outcome through `run_slice`'s catch-all, whose summary carries
/// `QueryAccountingSnapshot::default()` (all zeros), so the nonzero-spend
/// assertion below fails while the `Unsupported` status still passes.
#[test]
fn native_histogram_pushdown_refusal_reports_real_accounting() {
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
            sum: Some(2.5),
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
        let object_key = "seg/f1_hist.rseg".to_string();
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
            segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
        };
        let segments = vec![seg];

        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments.clone()).await;
        let response = SliceFetcher::fetch(
            &fetcher,
            pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: false,
                    want_max: false,
                    reduce_start_ns: None,
                    reduce_end_ns: None,
                }),
            ),
        )
        .await
        .expect("worker responds to the histogram pushdown request");
        server.abort();

        assert_eq!(
            response.status,
            pb::status::Code::Unsupported,
            "a native-histogram pushdown is refused so the coordinator falls back"
        );
        assert!(
            response.status_message.contains("native-histogram"),
            "expected the native-histogram refusal, got {:?}",
            response.status_message
        );
        assert!(
            response.phase_accounting.pooled().total_s3_bytes() > 0,
            "the refusal summary must carry the real fetch cost already spent, \
             not a zero-cost default"
        );
    });
}

/// Regression guard for the "completely unchanged when no window is set" contract
/// (ADR-0103 amendment, deliverable 1): a `want_count`-only request with NEITHER
/// reduction-window field set (T3's exact shape) produces the byte-identical
/// `PartialAggregate` frames the pre-amendment worker produced. The staleness
/// filter this task adds is unconditional but a no-op over this non-stale corpus,
/// and with no window the fold runs over every merged sample, so the wire output
/// must not move.
///
/// The expected bytes are built independently from the corpus' hand-known merged
/// counts (`m0` dedups six fetched samples to four; `m1` keeps two), encoded
/// through the same `encode_partial_aggregate` path, then compared as a sorted
/// set so the assertion turns on frame content, not on fetch/group order.
#[test]
fn count_only_no_window_request_is_byte_identical() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        use prost::Message;

        let store = Arc::new(MemoryStore::new());
        let segments = partial_pushdown_corpus(&store).await;

        let encode = |p: &crate::distrib::codec::PartialAggregate| {
            pb::FetchResponse {
                frame: Some(pb::fetch_response::Frame::PartialAggregate(
                    crate::distrib::codec::encode_partial_aggregate(p),
                )),
            }
            .encode_to_vec()
        };
        let expected_partial = |metric: &str, count: u64| crate::distrib::codec::PartialAggregate {
            series_id: SeriesId::compute(&tenant_id(), metric, &labels(metric)).expect("series id"),
            labels: labels(metric),
            count: Some(count),
            min: None,
            max: None,
        };
        let mut expected: Vec<Vec<u8>> = vec![
            encode(&expected_partial("m0", 4)),
            encode(&expected_partial("m1", 2)),
        ];
        expected.sort();

        // Drive the service directly so the partial frames are observable on the
        // wire before any decode collapses them.
        let metrics_store: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
        let service = SeriesFetchService::new(
            SegmentFetcher::new(metrics_store),
            Arc::new(SnapshotSegmentResolver::new(segments.clone())),
        );
        let response = SeriesFetch::fetch(
            &service,
            tonic::Request::new(pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: false,
                    want_max: false,
                    reduce_start_ns: None,
                    reduce_end_ns: None,
                }),
            )),
        )
        .await
        .expect("worker serves the count-only request");
        let frames: Vec<pb::FetchResponse> =
            futures::StreamExt::collect::<Vec<_>>(response.into_inner())
                .await
                .into_iter()
                .map(|f| f.expect("frame"))
                .collect();

        let mut got: Vec<Vec<u8>> = frames
            .iter()
            .filter(|f| {
                matches!(
                    f.frame,
                    Some(pb::fetch_response::Frame::PartialAggregate(_))
                )
            })
            .map(|f| f.encode_to_vec())
            .collect();
        got.sort();
        assert_eq!(
            got, expected,
            "count-only, no-window partial frames must be byte-identical to the \
             pre-amendment encode"
        );
    });
}

/// A [`SliceFetcher`] double that returns two `PartialAggregate`s carrying the
/// SAME series id in one slice response. ADR-0103's eligibility gate guarantees
/// each series lives on exactly one worker, so the coordinator's collect step
/// must never see a repeat; if it does, the query fails closed rather than
/// silently keeping one of the two values.
struct DuplicatePartialWorker;

#[async_trait::async_trait]
impl SliceFetcher for DuplicatePartialWorker {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let pa = crate::distrib::codec::PartialAggregate {
            series_id: SeriesId([7u8; 16]),
            labels: labels("m0"),
            count: Some(3),
            min: None,
            max: None,
        };
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: vec![pa.clone(), pa],
            phase_accounting: PhaseAccountingSnapshot::default(),
            stats: crate::fetcher::FetchStats::default(),
            series_returned: 0,
            samples_returned: 0,
            status: pb::status::Code::Ok,
            status_message: String::new(),
        })
    }
}

/// ADR-0103 amendment: a duplicate series id across collected partials is a hard
/// error (fail closed), never last-wins. Mutation proof: neutering the dedup
/// insert in `Distributed::fetch`'s drain loop (mod.rs, the
/// `if !partial_series.insert(pa.series_id)` guard) makes this `expect_err`
/// fail, since the query would then succeed keeping one of the two values.
#[test]
fn duplicate_partial_series_id_is_a_hard_error() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let descs = vec![SeriesDesc {
            metric: "m0".to_string(),
            samples: vec![(NS, 1.0f64.to_bits())],
        }];
        let seg = write_segment(&store, 0, 0, 100, &descs).await;
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let distributed = Distributed::new(
            Arc::new(DuplicatePartialWorker),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
            },
        );
        let accounting = PhaseAccounting::new();
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
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: false,
                    want_max: false,
                    reduce_start_ns: Some(0),
                    reduce_end_ns: Some(NS),
                }),
            )
            .await
            .expect_err("a duplicate series id across partials must fail closed");
        assert!(
            matches!(
                err,
                crate::error::QueryError::DuplicatePushdownSeries { .. }
            ),
            "expected DuplicatePushdownSeries, got {err:?}"
        );
    });
}
