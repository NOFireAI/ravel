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
use std::time::Duration;

use bytes::Bytes;
use proptest::prelude::*;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity as LogObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::SeriesData;
use ravel_proto::queryfrag::v1 as pb;
use ravel_rspan::{
    ObjectIdentity as SpanObjectIdentity, RspanConfig, RspanWriter, SpanQuery, SpanRecord,
};
use ravel_segment::{
    CompactionMetaV4, IngestBounds, RunInputV7, SampleProvenance, SegmentIdentity, SegmentWriter,
    SeriesInput, SeriesInputV7, SeriesValues, encode_run_v4,
};
use ravel_types::accounting::{AccountedOp, QueryAccounting, QueryAccountingSnapshot};
use ravel_types::logstream::{LogStreamId, log_stream_id};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};
use uuid::Uuid;

use crate::config::EngineConfig;
use crate::distrib::client::{
    DistribError, RemoteSliceFetcher, SliceFetcher, SliceLogResponse, SliceResponse,
    SliceSpanResponse,
};
use crate::distrib::codec::CodecError;
use crate::distrib::federation::{Federation, RemoteCluster};
use crate::distrib::partition::DistribThresholds;
use crate::distrib::proto::series_fetch_server::SeriesFetch;
use crate::distrib::{Distributed, WallDeadline};
use crate::distrib::{
    log_record_order_key, service::ReconstructingSegmentResolver, service::SeriesFetchService,
    service::SnapshotSegmentResolver, span_cmp, span_order_key,
};
use crate::engine::merge_soa_runs;
use crate::erasure::ErasurePredicate;
use crate::error::QueryError;
use crate::fetcher::SegmentFetcher;
use crate::log_fetcher::{LogFetchError, LogQuery, LogSegmentFetcher};
use crate::span_fetcher::{SpanFetchError, SpanRow, SpanSegmentFetcher};

const NS: i64 = 1_000_000;
const TENANT: TenantHash = TenantHash([7u8; 16]);
/// The deadline a test fan-out carries: never reached on either clock, and a
/// request deadline distinct enough that a stop reporting it is recognisable.
fn test_deadline() -> WallDeadline {
    WallDeadline {
        unix_ns: i64::MAX,
        request: Duration::from_secs(7),
        instant: tokio::time::Instant::now() + Duration::from_secs(365 * 24 * 3600),
    }
}

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
        declared_column_stats: Default::default(),
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

/// The local histogram reference: fetch every snapshot segment's histogram runs
/// directly (the third element of the fetch tuple the local path merges), so a
/// distributed histogram fetch can be compared run-for-run against it.
async fn local_histograms(
    store: Arc<MemoryStore>,
    snapshot: &Snapshot,
) -> Vec<Vec<crate::fetcher::FetchedHistogramSeries>> {
    let fetcher = SegmentFetcher::new(store);
    let accounting = QueryAccounting::new();
    let mut out = Vec::with_capacity(snapshot.segments.len());
    for seg in &snapshot.segments {
        let (_scalar, _stats, hist) = fetcher
            .fetch_soa_and_histograms_accounted(TENANT, seg, &[], &accounting)
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
            test_deadline(),
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

    /// ADR-0103 count-pushdown acceptance at a fan-out above one: the
    /// coordinator collects each slice's worker partials in slice-completion
    /// order (which varies run to run under `buffer_unordered`), and
    /// `sorted_pushdown_counts` must turn them into the same per-series count
    /// table a purely local fetch-and-count produces, in the same label-set
    /// order the raw path sorts its merged series into.
    ///
    /// Each series lives in its own segment under its own shard, so the
    /// shard-major partition places every series in exactly one slice for any
    /// cap: the pushdown path hard-errors (`DuplicatePushdownSeries`) on a
    /// series id that appears in two slices, so a differential over it must
    /// keep every series slice-local.
    #[test]
    fn distributed_pushdown_count_equals_local(
        per_series in prop::collection::vec(arb_samples(), 2..8),
        cap in 2usize..=6,
    ) {
        let rt = Runtime::new().expect("runtime");
        rt.block_on(run_pushdown_count_acceptance(per_series, cap));
    }
}

/// The metric name (`__name__`) of a label set, the whole distinguishing label
/// in the pushdown corpora.
fn metric_name(labels: &LabelSet) -> String {
    labels
        .iter()
        .find(|l| l.name == "__name__")
        .map(|l| l.value.clone())
        .expect("corpus labels carry __name__")
}

/// Drives one count-pushdown differential: build `per_series.len()` distinct
/// series, one per segment and one per shard, fetch their counts both locally
/// and through the distributed pushdown path at `cap`, and assert the built
/// count table equals the local counts and is in label-set order.
async fn run_pushdown_count_acceptance(per_series: Vec<Vec<(i64, u64)>>, cap: usize) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for (i, samples) in per_series.iter().enumerate() {
        let desc = SeriesDesc {
            metric: format!("m{i}"),
            samples: samples.clone(),
        };
        // One series, its own segment, its own shard, so no series id can span
        // two slices at any cap.
        segments.push(
            write_segment(&store, i as u64, i as u32, 100, std::slice::from_ref(&desc)).await,
        );
    }
    let snapshot = Snapshot {
        segments: segments.clone(),
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };

    // Local reference: the deduped per-series sample count, keyed by metric.
    let (local_runs, _acct, _stats) = local_scalar(Arc::clone(&store), &snapshot).await;
    let local_merged = merge_soa_runs(local_runs, usize::MAX, usize::MAX).expect("local merge");
    let mut local_counts: Vec<(String, u64)> = local_merged
        .iter()
        .map(|s| (metric_name(&s.labels), s.samples.len() as u64))
        .collect();
    local_counts.sort();

    // Distributed count pushdown over a real worker at a fan-out above one.
    let (fetcher, server) = spawn_worker(Arc::clone(&store), segments).await;
    let distributed = Distributed::new(
        Arc::new(fetcher),
        DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: cap,
        },
    );
    let accounting = QueryAccounting::new();
    let (_triple, partials) = distributed
        .fetch(
            TENANT,
            Signal::Metrics,
            &snapshot,
            &[],
            &[],
            &accounting,
            &EngineConfig::default(),
            test_deadline(),
            Some(pb::PartialAggregateRequest {
                want_count: true,
                want_min: false,
                want_max: false,
                reduce_start_ns: None,
                reduce_end_ns: None,
            }),
        )
        .await
        .expect("distributed fetch")
        .expect("count pushdown produced a result, not a fallback");
    server.abort();

    let table = crate::engine::sorted_pushdown_counts(partials);

    // In label-set order regardless of which slice finished first: the raw path
    // sorts its merged series by this same comparison, so the pushdown table
    // must too.
    let mut resorted = table.clone();
    resorted.sort_by(|a, b| a.0.iter().cmp(b.0.iter()));
    assert_eq!(
        table, resorted,
        "pushdown count table is not in label-set order"
    );

    // And it carries exactly the local per-series counts.
    let mut got: Vec<(String, u64)> = table
        .iter()
        .map(|(labels, count)| (metric_name(labels), *count))
        .collect();
    got.sort();
    assert_eq!(
        got, local_counts,
        "distributed pushdown counts differ from the local reduction"
    );
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
        declared_column_stats: Default::default(),
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
        let accounting = QueryAccounting::new();
        let triple = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &EngineConfig::default(),
                test_deadline(),
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
        let accounting = QueryAccounting::new();
        let distributed_merged = match distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &EngineConfig::default(),
                test_deadline(),
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
            test_deadline(),
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
                test_deadline(),
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
            histogram: Vec::new(),
            partials: Vec::new(),
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
                test_deadline(),
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
            accounting.snapshot().total_s3_bytes() >= 10_000,
            "the folded spend must survive on the live accounting handle"
        );
    });
}

// --- pinned-record GETs and the byte budget (issue #1721) ------------------

/// Moves the object `seg` was written at to the ADR-0010 data key its identity
/// reconstructs and PUTs its commit record at the commit key, so a
/// [`ReconstructingSegmentResolver`] can resolve it. Returns the ref at its new
/// key and the encoded record's length.
async fn commit_segment(store: &MemoryStore, seg: SegmentRef) -> (SegmentRef, u64) {
    let record = ravel_commit::record::build(ravel_commit::record::NewCommitRecord {
        tenant_hash: TENANT,
        signal: Signal::Metrics,
        shard: seg.shard,
        writer_id: seg.writer_id,
        writer_epoch: seg.writer_epoch,
        writer_seq: seg.writer_seq,
        object_size: seg.object_size,
        content_hash: seg.content_hash,
        sample_count: seg.sample_count,
        series_count: seg.series_count,
        min_event_ts_ns: seg.min_event_ts_ns,
        max_event_ts_ns: seg.max_event_ts_ns,
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
        segment_format_version: seg.segment_format_version,
        created_unix_ns: seg.created_unix_ns,
        ingest_hour_bucket: seg.ingest_hour_bucket,
    })
    .expect("valid commit record");
    let object = store
        .get(&seg.data_object_key, ravel_object_store::GetRange::Full)
        .await
        .expect("read segment")
        .data;
    store
        .delete(&seg.data_object_key)
        .await
        .expect("delete old key");
    store
        .put(&record.object_key, object, PutOptions::default())
        .await
        .expect("put segment at its data key");
    let encoded = prost::Message::encode_to_vec(&record);
    let record_len = encoded.len() as u64;
    let commit_key = ravel_commit::keys::commit_key_for_record(&record).expect("commit key");
    store
        .put(&commit_key, Bytes::from(encoded), PutOptions::default())
        .await
        .expect("put commit record");
    let seg = SegmentRef {
        data_object_key: record.object_key.clone(),
        ..seg
    };
    (seg, record_len)
}

/// Starts a real `tonic` metrics worker whose resolver reads each pinned
/// segment's own commit record, as the production fragment service does.
async fn spawn_record_worker(store: Arc<MemoryStore>) -> (RemoteSliceFetcher, JoinHandle<()>) {
    let store: Arc<dyn ObjectStoreBackend> = store;
    let limiter = Arc::new(crate::GetLimiter::new(8).expect("nonzero permits"));
    let resolver = Arc::new(ReconstructingSegmentResolver::new(
        Arc::clone(&store),
        TENANT,
        Signal::Metrics,
        Arc::clone(&limiter),
    ));
    let fetcher = SegmentFetcher::new(store).with_get_limiter(limiter);
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
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_lazy();
    (RemoteSliceFetcher::new(channel), handle)
}

/// ADR-0071: distribution changes where bytes are fetched, never what a query
/// computes, so a query whose data bytes fit `max_bytes_scanned` exactly must
/// succeed distributed as it does locally, although every pinned segment
/// costs its worker one commit-record GET the local path never issues. The
/// budget is set to the local path's exact byte figure over two segments on
/// two shards; the distributed fetch must succeed, and the query's folded
/// cost must equal the local cost exactly, so the record GETs are neither
/// charged to the budget nor counted in the pooled data cost.
///
/// Fails before the fix: the worker charged its record GETs to the slice, so
/// the first completed segment pushed the slice over the exact budget and the
/// query failed `TooManyBytesScanned`.
#[test]
fn record_gets_do_not_count_toward_the_byte_budget() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let mut segments = Vec::new();
        let mut record_bytes = 0;
        for shard in 0..2u32 {
            let seg = write_segment(
                &store,
                u64::from(shard),
                shard,
                100,
                &[SeriesDesc {
                    metric: format!("m{shard}"),
                    samples: vec![(NS, 1.0f64.to_bits()), (2 * NS, 2.0f64.to_bits())],
                }],
            )
            .await;
            let (seg, len) = commit_segment(&store, seg).await;
            record_bytes += len;
            segments.push(seg);
        }
        let snapshot = Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let (local_runs, local_acct, _stats) = local_scalar(Arc::clone(&store), &snapshot).await;
        let data_bytes = local_acct.total_s3_bytes();
        assert!(record_bytes > 0, "the fixture must need record GETs");

        let (fetcher, server) = spawn_record_worker(Arc::clone(&store)).await;
        let distributed = Distributed::new(
            Arc::new(fetcher),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 2,
            },
        );
        let config = EngineConfig {
            max_bytes_scanned: crate::config::ByteLimit::Bounded(data_bytes),
            ..EngineConfig::default()
        };
        let accounting = QueryAccounting::new();
        let result = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                test_deadline(),
                None,
            )
            .await;
        server.abort();
        let triple = result
            .expect("a query inside its data budget succeeds distributed")
            .expect("distributed produced a result (not a fallback)")
            .0;
        let local = merge_soa_runs(local_runs, usize::MAX, usize::MAX).expect("local merge");
        let dist = merge_soa_runs(triple.0, usize::MAX, usize::MAX).expect("dist merge");
        assert_series_bit_identical(&local, &dist);
        assert_eq!(
            accounting.snapshot(),
            local_acct,
            "the distributed data cost must equal the local cost exactly"
        );
    });
}

// --- the full byte budget goes to every slice (issue #1725) ----------------

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
            accounting: QueryAccounting::new().snapshot(),
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

/// ADR-0071 (issue #1725): every slice carries the query's WHOLE byte
/// budget, not a `1/slice_count` share of it. A share is not a budget: the
/// slice holding the hot shard trips its share while the query as a whole
/// sits far under the tenant's cap. Three shards at cap 3 dispatch three
/// slices, and each must be authorized for the full 900 bytes; the
/// coordinator's fold, not the per-slice share, is what enforces the total.
/// `max_series`/`max_samples`/`max_segments` are sent whole for the same
/// reason.
///
/// Mutation proof: restoring the `n / slice_count.max(1)` division in
/// `encode_budgets` (`mod.rs`) makes the per-slice assertion below fail with
/// `300`, not `900`.
#[test]
fn distrib_fetch_sends_the_full_byte_budget_to_every_slice() {
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
        let accounting = QueryAccounting::new();
        distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                test_deadline(),
                None,
            )
            .await
            .expect("fetch succeeds");

        let recorded = seen.lock().expect("lock");
        assert_eq!(recorded.len(), 3, "all three slices dispatched once");
        for budgets in recorded.iter() {
            assert_eq!(
                budgets.max_bytes_scanned, 900,
                "every slice carries the query's whole 900-byte budget, not a third of it"
            );
            assert_eq!(budgets.max_series, 42, "count-based caps are sent whole");
            assert_eq!(budgets.max_samples, 43, "count-based caps are sent whole");
            assert_eq!(budgets.max_segments, 44, "count-based caps are sent whole");
        }
    });
}

/// The per-slice byte budget does not depend on how many slices were
/// dispatched: two shards at cap 8 dispatch two slices, and each is
/// authorized for the same whole 1000 bytes a single-slice query would get.
/// That independence is the point of #1725: a query's authorization must not
/// shrink because its data happens to span more shards.
#[test]
fn distrib_fetch_byte_budget_does_not_shrink_with_slice_count() {
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
        let accounting = QueryAccounting::new();
        distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                test_deadline(),
                None,
            )
            .await
            .expect("fetch succeeds");

        let recorded = seen.lock().expect("lock");
        assert_eq!(recorded.len(), 2, "only two shards, so only two slices");
        for budgets in recorded.iter() {
            assert_eq!(
                budgets.max_bytes_scanned, 1_000,
                "each of two slices carries the whole budget, not half of it"
            );
        }
    });
}

/// `Unlimited` stays the wire's `0` sentinel regardless of slice count: a
/// query with no configured byte cap must not have one manufactured on the
/// wire, where `0` means "no cap from the coordinator".
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
        let accounting = QueryAccounting::new();
        distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                test_deadline(),
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

/// A [`SliceFetcher`] double that behaves the way a faithful worker does: it
/// spends a fixed number of bytes per dispatch (popped in dispatch order),
/// reports that spend in its accounting snapshot, and refuses with
/// `BudgetExceeded` ONLY when its own spend exceeds the wire budget it was
/// actually given. The refusal message is the rendered
/// `QueryError::TooManyBytesScanned` a real worker's `summary_frame` carries,
/// so the coordinator's fold sees exactly the text the wire really delivers.
/// `0` on the wire is the no-cap sentinel.
struct FaithfulBudgetWorker {
    spends: std::sync::Mutex<std::collections::VecDeque<u64>>,
    /// The wire budget of every dispatch, in arrival order.
    seen_wire: Arc<std::sync::Mutex<Vec<u64>>>,
}

impl FaithfulBudgetWorker {
    fn new(
        spends: impl IntoIterator<Item = u64>,
        seen_wire: Arc<std::sync::Mutex<Vec<u64>>>,
    ) -> Self {
        FaithfulBudgetWorker {
            spends: std::sync::Mutex::new(spends.into_iter().collect()),
            seen_wire,
        }
    }
}

#[async_trait::async_trait]
impl SliceFetcher for FaithfulBudgetWorker {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let spend = self
            .spends
            .lock()
            .expect("lock")
            .pop_front()
            .expect("a dispatch with no configured spend");
        let wire = request.budgets.map(|b| b.max_bytes_scanned).unwrap_or(0);
        self.seen_wire.lock().expect("lock").push(wire);
        let acct = QueryAccounting::new();
        acct.add_s3_bytes(ravel_types::accounting::AccountedOp::Get, spend);
        let (status, status_message) = if wire != 0 && spend > wire {
            (
                pb::status::Code::BudgetExceeded,
                crate::error::QueryError::TooManyBytesScanned {
                    scanned: spend,
                    max: wire,
                }
                .to_string(),
            )
        } else {
            (pb::status::Code::Ok, String::new())
        };
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: Vec::new(),
            accounting: acct.snapshot(),
            stats: crate::fetcher::FetchStats::default(),
            series_returned: 0,
            samples_returned: 0,
            status,
            status_message,
        })
    }
}

/// The acceptance test for issue #1725. Work is skewed across shards: of two
/// slices under a 1000-byte query budget, one scans 900 bytes and the other
/// 50, for a folded total of 950 -- comfortably under the query's own cap.
/// Such a query must not fail at all, and it must NEVER fail as
/// `QueryError::Distrib`, which maps to a retryable 503 and reports a budget
/// outcome as an outage.
///
/// Pre-fix both halves failed. `encode_budgets` divided the budget by the
/// slice count, so each slice was authorized for 500: the 900-byte slice
/// tripped and returned `BudgetExceeded`, and because the folded total (950)
/// was under the query's cap, `budget_exceeded_error` found no bytes trip, no
/// memory refusal, and fell through to `QueryError::Distrib`.
///
/// Mutation proof: restoring `n / (slice_count.max(1) as u64)` in
/// `encode_budgets` (`mod.rs`) makes the wire assertion fail with 500 and
/// the query fail.
///
/// That is the only mutation this test catches. Under the fix no slice
/// refuses, so the `Err` arm below is unreachable and nothing here exercises
/// `budget_exceeded_error`'s refusal parsing: deleting the
/// `parse_worker_cap_refusal` branch leaves this test green.
/// `worker_local_clamp_refusal_surfaces_typed_not_distrib` is the test that
/// mutation does break.
#[test]
fn skewed_slice_over_its_share_but_under_query_budget_never_returns_distrib() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = MemoryStore::new();
        let snapshot = sharded_snapshot(&store, 2).await;
        let seen_wire = Arc::new(std::sync::Mutex::new(Vec::new()));
        let distributed = Distributed::new(
            Arc::new(FaithfulBudgetWorker::new(
                [900, 50],
                Arc::clone(&seen_wire),
            )),
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
        let result = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &config,
                test_deadline(),
                None,
            )
            .await;

        match result {
            Ok(_) => {}
            Err(err) => {
                assert!(
                    !matches!(err, crate::error::QueryError::Distrib { .. }),
                    "a byte residual under the query's own budget must never be Distrib, got {err:?}"
                );
                assert!(
                    matches!(err, crate::error::QueryError::TooManyBytesScanned { .. }),
                    "the only acceptable failure here is a typed budget error, got {err:?}"
                );
            }
        }

        let wire = seen_wire.lock().expect("lock");
        assert_eq!(wire.len(), 2, "both slices dispatched once");
        for budget in wire.iter() {
            assert_eq!(
                *budget, 1_000,
                "each slice is authorized for the query's whole budget, not a 500-byte share"
            );
        }
        assert_eq!(
            accounting.snapshot().total_s3_bytes(),
            950,
            "both slices' real spend is folded into the query's reported cost"
        );
    });
}

/// The other half of the semantics: a fan-out whose FOLDED total really is
/// over the query's budget still fails, as the typed
/// `TooManyBytesScanned`, and renders as 422 `execution` through the HTTP
/// mapping -- never the 503 a `Distrib` would render. Two slices spend 900
/// and 600 bytes; neither exceeds the 1000-byte budget each was authorized
/// for on its own, so the refusal can only come from the coordinator's fold.
///
/// Mutation proof: deleting the folded `bytes_scanned_exceeded` check in
/// `Distributed::fetch`'s `Ok` arm (`mod.rs`) lets the over-spend through and
/// `expect_err` below fails.
#[test]
fn distrib_fold_over_query_budget_renders_422_not_503() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = MemoryStore::new();
        let snapshot = sharded_snapshot(&store, 2).await;
        let seen_wire = Arc::new(std::sync::Mutex::new(Vec::new()));
        let distributed = Distributed::new(
            Arc::new(FaithfulBudgetWorker::new(
                [900, 600],
                Arc::clone(&seen_wire),
            )),
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
                test_deadline(),
                None,
            )
            .await
            .expect_err("a folded total of 1500 over a 1000-byte budget must fail");
        assert!(
            matches!(
                err,
                crate::error::QueryError::TooManyBytesScanned {
                    scanned: 1_500,
                    max: 1_000
                }
            ),
            "the fold must report the exact folded total against the query's cap, got {err:?}"
        );

        let rendered = crate::http::QueryErrorResponse::from_query_error(err);
        assert_eq!(
            rendered.status,
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "a budget refusal is a 422, not the 503 a Distrib error renders"
        );
        assert_eq!(rendered.error_type, "execution");
    });
}

/// A worker that refuses on ITS OWN tighter limit (issue #1687 part A: the
/// worker clamps every wire budget to its local `EngineConfig`) reports a
/// `BudgetExceeded` whose figures are the worker's, not the query's, while
/// the coordinator's folded total stays under the query's cap. That residual
/// is a budget outcome the caller can act on, so it surfaces as the typed
/// `TooManyBytesScanned` carrying the WORKER's figures, never as `Distrib`.
///
/// Mutation proof: deleting the `parse_worker_cap_refusal` branch from
/// `budget_exceeded_error` (`mod.rs`) makes this fall back to `Distrib` and
/// the `matches!` below fails.
#[test]
fn worker_local_clamp_refusal_surfaces_typed_not_distrib() {
    let msg = crate::error::QueryError::TooManyBytesScanned {
        scanned: 900,
        max: 100,
    }
    .to_string();
    let err = super::budget_exceeded_error(900, crate::config::ByteLimit::Bounded(1_000), &msg);
    assert!(
        matches!(
            err,
            crate::error::QueryError::TooManyBytesScanned {
                scanned: 900,
                max: 100
            }
        ),
        "the worker's own clamp figures must survive the fold typed, got {err:?}"
    );
    let rendered = crate::http::QueryErrorResponse::from_query_error(err);
    assert_eq!(
        rendered.status,
        axum::http::StatusCode::UNPROCESSABLE_ENTITY
    );
}

/// `parse_worker_cap_refusal` inverts the `Display` of all four cap errors a
/// worker's `summary_frame` can carry. Changing any of those `#[error(..)]`
/// formats without updating the parser fails here rather than silently
/// degrading the coordinator to a generic `Distrib`.
#[test]
fn worker_cap_refusal_messages_round_trip() {
    let bytes = crate::error::QueryError::TooManyBytesScanned {
        scanned: 4_194_304,
        max: 1_048_576,
    };
    assert!(
        matches!(
            super::parse_worker_cap_refusal(&bytes.to_string()),
            Some(crate::error::QueryError::TooManyBytesScanned {
                scanned: 4_194_304,
                max: 1_048_576
            })
        ),
        "the bytes refusal must round-trip its exact figures"
    );

    let series = crate::error::QueryError::TooManySeries {
        count: 10_001,
        max: 10_000,
    };
    assert!(
        matches!(
            super::parse_worker_cap_refusal(&series.to_string()),
            Some(crate::error::QueryError::TooManySeries {
                count: 10_001,
                max: 10_000
            })
        ),
        "the series refusal must round-trip its exact figures"
    );

    let samples = crate::error::QueryError::TooManySamples {
        count: 10_000_001,
        max: 10_000_000,
    };
    assert!(
        matches!(
            super::parse_worker_cap_refusal(&samples.to_string()),
            Some(crate::error::QueryError::TooManySamples {
                count: 10_000_001,
                max: 10_000_000
            })
        ),
        "the samples refusal must round-trip its exact figures"
    );

    let segments = crate::error::QueryError::TooManySegments {
        count: 1_025,
        max: 1_024,
    };
    assert!(
        matches!(
            super::parse_worker_cap_refusal(&segments.to_string()),
            Some(crate::error::QueryError::TooManySegments {
                count: 1_025,
                max: 1_024
            })
        ),
        "the segments refusal must round-trip its exact figures"
    );

    // An unrelated message parses to None, so the fold still falls back to a
    // generic Distrib rather than fabricating a budget outcome.
    assert!(
        super::parse_worker_cap_refusal("worker exploded").is_none(),
        "an unrelated message must not parse as a cap refusal"
    );
    assert!(
        super::parse_worker_cap_refusal("query matched some series, exceeding the limit of 3")
            .is_none(),
        "a non-numeric count must not parse"
    );
}

/// A federated remote whose answer is a terminal `BudgetExceeded` summary
/// carrying `message` as its rendered refusal, plus the bytes it really spent
/// before refusing. This is byte-for-byte what a remote's `summary_frame`
/// puts on the wire when its wire-budget clamp trips or its
/// `resolve_scope_count_refusal` fires (service.rs): a status code plus the
/// `Display` text of the typed cap error it raised.
struct RemoteCapRefusalFetcher {
    message: String,
    spent: u64,
}

#[async_trait::async_trait]
impl SliceFetcher for RemoteCapRefusalFetcher {
    async fn fetch(&self, _r: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let acct = QueryAccounting::new();
        acct.add_s3_bytes(ravel_types::accounting::AccountedOp::Get, self.spent);
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: Vec::new(),
            accounting: acct.snapshot(),
            stats: crate::fetcher::FetchStats::default(),
            series_returned: 0,
            samples_returned: 0,
            status: pb::status::Code::BudgetExceeded,
            status_message: self.message.clone(),
        })
    }
}

/// One remote cluster wired for the two federation tests below.
fn federation_with(fetcher: Arc<dyn SliceFetcher>, skip_unavailable: bool) -> Federation {
    Federation::new(vec![RemoteCluster {
        name: "eu-west".to_string(),
        fetcher,
        tenant: None,
        skip_unavailable,
        soft_timeout: std::time::Duration::from_secs(5),
    }])
}

async fn federation_fetch(fed: &Federation, config: EngineConfig) -> crate::error::QueryError {
    fed.fetch(
        TenantHash([1u8; 16]),
        Signal::Metrics,
        Vec::new(),
        Vec::new(),
        0,
        1_000,
        Vec::new(),
        QueryAccounting::new(),
        config,
        test_deadline(),
    )
    .await
    .expect_err("a remote refusal must fail the query")
}

/// Issue #1725 across the cluster boundary. A remote that answers
/// `BudgetExceeded` REFUSED the query under its own configured caps: it
/// clamps every wire budget to its own `EngineConfig` and, on the resolve
/// scope, enforces its own `max_series`/`max_samples` over the result it is
/// about to return (`resolve_scope_count_refusal`, service.rs). The
/// coordinator is under its own budget in both cases here, so before the fix
/// the refusal fell through to `QueryError::Federation`, which http/error.rs
/// redacts to a retryable 503 -- telling a client to retry a query the remote
/// will refuse identically every time. ADR-0071 decision 5 and
/// docs/query-engine.md both reserve 503 for a fan-out that itself failed.
///
/// Both refusal shapes this pull request introduces are covered: the
/// count refusal (`TooManySeries`) and the wire-budget clamp
/// (`TooManyBytesScanned` with the REMOTE's figures, not the query's). The
/// count case also runs with `skip_unavailable = true`, because a cap refusal
/// is a correctness outcome and is never skippable.
///
/// Mutation proof: replacing the `typed_budget_refusal` call in
/// `federation.rs`'s `BudgetExceeded` arm with the pre-fix
/// `bytes_scanned_exceeded(...).unwrap_or_else(|| QueryError::Federation ...)`
/// makes both `matches!` assertions below fail with `Federation { .. }` and
/// both status assertions fail with 503.
#[test]
fn remote_cap_refusal_under_the_local_budget_is_a_422_not_a_503() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let config = EngineConfig {
            max_bytes_scanned: crate::config::ByteLimit::Bounded(1_000_000),
            ..EngineConfig::default()
        };

        // The remote enforced its own max_series over the resolve-scope
        // result. Nothing about the coordinator's budget was reached.
        let fed = federation_with(
            Arc::new(RemoteCapRefusalFetcher {
                message: crate::error::QueryError::TooManySeries {
                    count: 50_000,
                    max: 10_000,
                }
                .to_string(),
                spent: 500,
            }),
            true,
        );
        let err = federation_fetch(&fed, config).await;
        assert!(
            matches!(
                err,
                crate::error::QueryError::TooManySeries {
                    count: 50_000,
                    max: 10_000
                }
            ),
            "a remote count refusal keeps its type and the remote's figures, got {err:?}"
        );
        let rendered = crate::http::QueryErrorResponse::from_query_error(err);
        assert_eq!(
            rendered.status,
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "a cap refusal is a 422, not the 503 a Federation error renders"
        );
        assert_eq!(rendered.error_type, "execution");

        // The remote clamped the wire budget to its own tighter
        // `max_bytes_scanned` and refused with ITS figures (900 scanned
        // against its own 100), while the coordinator's folded total of 900
        // is far under the 1_000_000 it authorized.
        let fed = federation_with(
            Arc::new(RemoteCapRefusalFetcher {
                message: crate::error::QueryError::TooManyBytesScanned {
                    scanned: 900,
                    max: 100,
                }
                .to_string(),
                spent: 900,
            }),
            false,
        );
        let err = federation_fetch(&fed, config).await;
        assert!(
            matches!(
                err,
                crate::error::QueryError::TooManyBytesScanned {
                    scanned: 900,
                    max: 100
                }
            ),
            "the remote's own clamp figures must survive the fold typed, got {err:?}"
        );
        let rendered = crate::http::QueryErrorResponse::from_query_error(err);
        assert_eq!(
            rendered.status,
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            "a remote clamp refusal is a 422, not a 503"
        );
        assert_eq!(rendered.error_type, "execution");
    });
}

/// The other side of that fix, and what stops it from swallowing a genuine
/// fan-out failure: a remote that never answered at all is still
/// `QueryError::Federation` and still renders as the retryable 503. The two
/// are told apart by the wire status code -- a refusal is a `Status` a remote
/// had to answer to send, while a transport failure produces no `Status` at
/// all and reaches `handle_unavailable` instead. A remote therefore cannot
/// talk its way out of 503 by putting cap-refusal text in a transport error:
/// the text below is the exact `Display` of a `TooManyBytesScanned` that
/// `parse_worker_cap_refusal` would happily parse, and it must change
/// nothing.
///
/// Mutation proof: routing the `DistribError::Transport` arm in
/// `Federation::fetch` through `typed_budget_refusal` on the error text (a
/// plausible over-broad reading of the fix) makes the `Federation` assertion
/// below fail with `TooManyBytesScanned` and the 503 assertion fail with 422.
#[test]
fn federated_transport_failure_still_maps_to_503() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        struct SpoofingTransportFetcher(String);
        #[async_trait::async_trait]
        impl SliceFetcher for SpoofingTransportFetcher {
            async fn fetch(&self, _r: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
                Err(DistribError::Transport(self.0.clone()))
            }
        }

        let fed = federation_with(
            Arc::new(SpoofingTransportFetcher(
                crate::error::QueryError::TooManyBytesScanned {
                    scanned: 900,
                    max: 100,
                }
                .to_string(),
            )),
            false,
        );
        let err = federation_fetch(&fed, EngineConfig::default()).await;
        assert!(
            matches!(err, crate::error::QueryError::Federation { .. }),
            "an unreachable remote is a fan-out failure, whatever its error text says, got {err:?}"
        );
        let rendered = crate::http::QueryErrorResponse::from_query_error(err);
        assert_eq!(
            rendered.status,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "a genuine transport failure keeps the retryable 503"
        );
        assert_eq!(rendered.error_type, "unavailable");
    });
}

/// Issue #1687 part A: the worker's own configuration is binding on every
/// wire budget. `0` and an absent `Budgets` are the wire's no-cap sentinel,
/// which means "no cap from the coordinator" and therefore resolves to the
/// WORKER's limit, never to `Unlimited`. A coordinator asking for more than
/// the worker allows gets the worker's number; asking for less gets its own.
///
/// Mutation proof: restoring the pre-fix `Some(0) | None =>
/// ByteLimit::Unlimited` arm makes the first two cases below fail with
/// `Unlimited`, and dropping the `wire.min(own)` to a bare `wire` makes the
/// oversized case fail with 1_000_000.
#[test]
fn worker_clamps_every_wire_budget_to_its_own_engine_config() {
    let worker = crate::config::ByteLimit::Bounded(4_096);
    let budgets = |max_bytes_scanned| pb::Budgets {
        max_series: u64::MAX,
        max_samples: u64::MAX,
        max_bytes_scanned,
        max_segments: u64::MAX,
    };

    assert_eq!(
        super::service::slice_byte_limit(None, worker),
        crate::config::ByteLimit::Bounded(4_096),
        "an absent Budgets falls back to the worker's own limit"
    );
    assert_eq!(
        super::service::slice_byte_limit(Some(&budgets(0)), worker),
        crate::config::ByteLimit::Bounded(4_096),
        "the 0 sentinel means no cap from the coordinator, not no cap at all"
    );
    assert_eq!(
        super::service::slice_byte_limit(Some(&budgets(1_000_000)), worker),
        crate::config::ByteLimit::Bounded(4_096),
        "an oversized wire budget is clamped down to the worker's own"
    );
    assert_eq!(
        super::service::slice_byte_limit(Some(&budgets(512)), worker),
        crate::config::ByteLimit::Bounded(512),
        "a tighter wire budget wins: the clamp takes the minimum of the two"
    );
    assert_eq!(
        super::service::slice_byte_limit(Some(&budgets(512)), crate::config::ByteLimit::Unlimited),
        crate::config::ByteLimit::Bounded(512),
        "an unconfigured worker still honours the coordinator's budget"
    );
    assert_eq!(
        super::service::slice_byte_limit(None, crate::config::ByteLimit::Unlimited),
        crate::config::ByteLimit::Unlimited,
        "no cap on either side is the only way to get Unlimited"
    );
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
                test_deadline(),
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
        let acct = QueryAccounting::new();
        acct.add_s3_bytes(ravel_types::accounting::AccountedOp::Get, spend);
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: Vec::new(),
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
                test_deadline(),
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
            declared_column_stats: Default::default(),
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
        let accounting = QueryAccounting::new();
        let triple = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &[],
                &accounting,
                &EngineConfig::default(),
                test_deadline(),
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
            accounting.snapshot().total_s3_bytes() > 0,
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
            declared_column_stats: Default::default(),
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
        let accounting = QueryAccounting::new();
        let result = distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                &snapshot,
                &[],
                &erasure,
                &accounting,
                &EngineConfig::default(),
                test_deadline(),
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

// --- RLOG-family and Spans distributed fan-out (#284/#285) ------------------
//
// The worker's log and span slice serving and the coordinator's log and span
// fan-out are covered end to end over the loopback worker by the issue #1946
// suites at the end of this file, through the test-only
// `LogSliceStreamDecoder`/`SpanSliceStreamDecoder` and `LoopbackSliceFetcher`.
// Production stays unwired: `RemoteSliceFetcher` keeps the `Unsupported`
// defaults for `fetch_logs`/`fetch_spans`. The span total-order pins below are
// coordinator-side and independent of any transport.

const TA: [u8; 16] = [0xAA; 16];
const TB: [u8; 16] = [0xBB; 16];
const S1: [u8; 8] = [1u8; 8];
const S2: [u8; 8] = [2u8; 8];

/// Direct discrimination test for every field of the documented span total order
/// `(trace_id, span_id, start_ts_ns, end_ts_ns, parent_span_id, name,
/// status_code, status_message, service_name, attrs)`. For each of the ten
/// fields, two `SpanRow`s are built identical in every OTHER field and differing
/// only in that one, and `span_order_key` must map them to different keys.
///
/// Every field is covered here directly, so this test pins the key on its own
/// whatever fields a differential corpus happens to vary. Dropping any field
/// from `span_order_key` -- in particular truncating it to
/// `(trace_id, start_ts_ns)` -- collapses that field's pair to an equal key and
/// fails the matching assertion below.
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
///
/// The request names `Signal::Logs` but is read back through
/// [`SliceFetcher::fetch`]: the worker refuses before it serves a single log
/// frame, so the whole response is one terminal summary, which the metrics
/// decoder reads as well as a log decoder would. That is the only production
/// decode (#1912), and it is what makes this a worker-side assertion.
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
        let response = SliceFetcher::fetch(&fetcher, request)
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
///
/// The refusal runs after the whole per-segment fetch loop, so it also reports
/// what those fetches cost (issue #1723). The oracle is the same request with a
/// well-formed window over the same worker and the same store: identical
/// segments fetched by an uncached `SegmentFetcher`, so an identical cost.
///
/// Mutation proof: RED against the reverted line. Dropping
/// `.with_spend(&accounting, &stats)` from the lone-bound refusal in
/// `SeriesFetchService::run_slice_metrics` (`service.rs`) makes both refusals
/// report a default, zero-cost snapshot while the oracle's figure is nonzero.
#[test]
fn worker_partial_aggregate_lone_window_bound_is_internal_error() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let segments = partial_pushdown_corpus(&store).await;
        let (fetcher, server) = spawn_worker(Arc::clone(&store), segments.clone()).await;

        // The cost oracle: the same slice, a well-formed window, no refusal.
        let accepted = SliceFetcher::fetch(
            &fetcher,
            pushdown_request(
                &segments,
                Some(pb::PartialAggregateRequest {
                    want_count: true,
                    want_min: false,
                    want_max: false,
                    reduce_start_ns: Some(NS),
                    reduce_end_ns: Some(4 * NS),
                }),
            ),
        )
        .await
        .expect("worker responds");
        assert_eq!(accepted.status, pb::status::Code::Ok);
        assert!(
            accepted.accounting.total_s3_bytes() > 0,
            "the corpus must cost real bytes for the refusals to have something \
             to report"
        );

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
        assert_eq!(
            start_only.accounting, accepted.accounting,
            "the refusal reports what its fetch loop already spent, to the byte"
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
        assert_eq!(
            end_only.accounting, accepted.accounting,
            "and so does the mirror-image refusal"
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
            declared_column_stats: Default::default(),
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
            response.accounting.total_s3_bytes() > 0,
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
            accounting: QueryAccounting::new().snapshot(),
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
                test_deadline(),
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

/// The coordinator reconstructs a worker's memory refusal by parsing the
/// figures back out of the gRPC status message, because today's `Status`
/// carries only a code and a message: `parse_fetch_memory_exhausted` is the
/// exact inverse of `FetchMemoryExhausted`'s `Display`. That is a stopgap, not
/// a constraint -- the proto evolves additively, so three `uint64` fields would
/// carry the figures and delete this coupling. Three independently-editable
/// `#[error(..)]` strings render that variant -- `FetchError` (series fold,
/// `distrib/mod.rs:355`), `LogFetchError` (log fold, `:529`), and
/// `SpanFetchError` (span fold, `:682`) -- and nothing but this test keeps them
/// textually identical to each other and to the parser. This pins all three:
/// a reword of any one of them, alone, fails the corresponding `assert_eq`
/// below rather than silently degrading that fold's coordinator-side
/// reconstruction to a generic `Distrib`.
#[test]
fn fetch_memory_exhausted_message_round_trips() {
    let original = crate::fetcher::FetchError::FetchMemoryExhausted {
        requested: 4_194_304,
        reserved: 268_435_456,
        limit: 268_500_000,
    };
    let rendered = original.to_string();
    assert_eq!(
        super::parse_fetch_memory_exhausted(&rendered),
        Some((4_194_304, 268_435_456, 268_500_000)),
        "parser must invert the Display of FetchError::FetchMemoryExhausted {rendered:?}"
    );

    let original = LogFetchError::FetchMemoryExhausted {
        requested: 4_194_304,
        reserved: 268_435_456,
        limit: 268_500_000,
    };
    let rendered = original.to_string();
    assert_eq!(
        super::parse_fetch_memory_exhausted(&rendered),
        Some((4_194_304, 268_435_456, 268_500_000)),
        "parser must invert the Display of LogFetchError::FetchMemoryExhausted {rendered:?}"
    );

    let original = SpanFetchError::FetchMemoryExhausted {
        requested: 4_194_304,
        reserved: 268_435_456,
        limit: 268_500_000,
    };
    let rendered = original.to_string();
    assert_eq!(
        super::parse_fetch_memory_exhausted(&rendered),
        Some((4_194_304, 268_435_456, 268_500_000)),
        "parser must invert the Display of SpanFetchError::FetchMemoryExhausted {rendered:?}"
    );

    // A message that is not this error's Display parses to None, so the fold
    // falls back to a generic Distrib rather than fabricating figures.
    assert_eq!(
        super::parse_fetch_memory_exhausted("slice tripped its budget: something else"),
        None,
        "an unrelated message must not parse as a memory refusal"
    );
}

/// The coordinator's `BudgetExceeded` fold surfaces a fetch-layer memory refusal
/// as the typed `Fetch(FetchMemoryExhausted)` -- the same error the local path
/// raises -- when the folded bytes-scanned total is under the query's cap (so
/// the trip is memory, not bytes). Replacing the `parse_fetch_memory_exhausted`
/// branch in `budget_exceeded_error` with the `Distrib` fallback makes the
/// `matches!` below fail. When the folded total is at or over the bytes cap, the
/// same helper yields the bytes-scanned error instead, proving the
/// disambiguation.
#[test]
fn budget_exceeded_fold_renders_memory_refusal_typed() {
    let msg = crate::fetcher::FetchError::FetchMemoryExhausted {
        requested: 4_194_304,
        reserved: 268_435_456,
        limit: 268_500_000,
    }
    .to_string();

    // Folded bytes under the cap: the trip is a memory refusal, surfaced typed.
    let err =
        super::budget_exceeded_error(1_000, crate::config::ByteLimit::Bounded(1_000_000), &msg);
    assert!(
        matches!(
            err,
            crate::error::QueryError::Fetch(crate::fetcher::FetchError::FetchMemoryExhausted {
                requested: 4_194_304,
                reserved: 268_435_456,
                limit: 268_500_000,
            })
        ),
        "a memory refusal under the bytes cap must fold to a typed FetchMemoryExhausted, got {err:?}"
    );

    // Folded bytes at or over the cap: the bytes-scanned trip dominates.
    let err = super::budget_exceeded_error(
        2_000_000,
        crate::config::ByteLimit::Bounded(1_000_000),
        &msg,
    );
    assert!(
        matches!(err, crate::error::QueryError::TooManyBytesScanned { .. }),
        "a folded total over the bytes cap must fold to TooManyBytesScanned, got {err:?}"
    );
}

/// `log_stream_id` and its attribute blob for `service`, the shape a log fan-out
/// row carries.
fn log_stream(service: &str) -> (LogStreamId, Vec<u8>) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(service.to_string()),
    )];
    let id = log_stream_id(&resource, "scope", "1.0", &[]);
    let blob = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    (id, blob)
}

/// One log record on `service`'s stream at event time `ts`.
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

/// Every field of the log total order participates in the key.
///
/// The counterpart of [`span_order_key_discriminates_every_field`] for the log
/// side: a field silently dropped from `log_record_order_key` reorders a
/// distributed log read against a local one only on a corpus where that field
/// decides the order, so every field is pinned here directly. This pin is
/// transport-independent; [`distributed_log_fetch_equals_local_bitwise`] covers
/// the order end to end.
#[test]
fn log_record_order_key_discriminates_every_field() {
    let base = log_record("alpha", 10, "body", &[("k", "v")]);

    let check = |field: &str, mutate: &dyn Fn(&mut LogRecord)| {
        let mut other = base.clone();
        mutate(&mut other);
        assert_ne!(
            log_record_order_key(&base),
            log_record_order_key(&other),
            "log_record_order_key must distinguish records differing only in \
             {field}; if it does not, that field has been dropped from the key"
        );
    };

    // One case per key field, in the key's own field order. `stream_id` and
    // `stream_attrs` are two separate tuple elements in `LogOrderKey`
    // (mod.rs), so each needs its own case that varies it alone; going
    // through `log_stream`, which derives both from the same resource, would
    // move them together and let either field cover for a dropped other one.
    // `LogRecord`'s fields are independently settable (ravel-logseg's
    // `stream_attrs`-consistency invariant is enforced by the writer, not by
    // this struct), so each case is built directly against the record.
    check("ts_ns", &|r| r.ts_ns = 20);
    check("stream_id", &|r| {
        r.stream_id = LogStreamId([9u8; 16]);
    });
    check("stream_attrs", &|r| {
        r.stream_attrs = vec![0xffu8, 0xee, 0xdd];
    });
    check("observed_ts_ns", &|r| r.observed_ts_ns = 99);
    check("severity_num", &|r| r.severity_num = 17);
    check("severity_text", &|r| r.severity_text = "WARN".to_string());
    check("body", &|r| r.body = "other".to_string());
    check("trace_id", &|r| r.trace_id = Some([1u8; 16]));
    check("span_id", &|r| r.span_id = Some([2u8; 8]));
    check("flags", &|r| r.flags = 1);
    check("attrs", &|r| {
        r.attrs = vec![("k".to_string(), AttrValue::Str("other".to_string()))]
    });
}

// ---- issue #1723: a failed slice reports what it already spent -------------

/// The worker half of issue #1723: a slice that GET its first segment and then
/// hit a store 503 on the second reports the first segment's real cost on its
/// terminal `Unavailable` summary.
///
/// The fault is scoped to the SECOND segment's object key, so the first fetch
/// completes for real and the failure lands mid-slice, which is the shape a
/// store outage produces. The expected figures come from fetching that first
/// segment alone under a clean store (the oracle below), plus the one GET the
/// failing attempt issued before the fault fired: bytes are the oracle's
/// exactly, since a failed GET transfers none, and requests are the oracle's
/// plus that one attempt.
///
/// Mutation proof: RED when the fetch-error site in
/// `SeriesFetchService::run_slice` goes back to
/// `SliceFailure::from(map_fetch_error(e))` with no `.with_spend(..)`. The
/// failure then takes the catch-all, whose summary is built from
/// `QueryAccountingSnapshot::default()`, and the byte assertion below fails
/// against a summary reporting zero for a slice that really moved the oracle's
/// bytes off the store.
#[test]
fn failed_slice_reports_the_segments_it_already_paid_for() {
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};

    /// The one GET the failing attempt issued before the fault fired. A failed
    /// GET is counted (the request is recorded before the call) and transfers
    /// no bytes.
    const FAILED_ATTEMPT_REQUESTS: u64 = 1;

    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let first = write_segment(
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
        let second = write_segment(
            &store,
            1,
            0,
            100,
            &[SeriesDesc {
                metric: "m1".to_string(),
                samples: vec![(2 * NS, 2.0f64.to_bits())],
            }],
        )
        .await;

        // The oracle: what fetching ONLY the first segment costs, measured over
        // the same clean store through the same fetch path.
        let oracle = {
            let accounting = QueryAccounting::new();
            SegmentFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)
                .fetch_soa_and_histograms_accounted(TENANT, &first, &[], &accounting)
                .await
                .expect("the first segment fetches cleanly");
            accounting.snapshot()
        };
        assert!(
            oracle.total_s3_bytes() > 0,
            "the fixture's first segment must cost real bytes"
        );

        // Every GET of the SECOND segment's object fails with a non-NotFound
        // store error, which the worker maps to `Unavailable` (the S3 503 case).
        let faulty = Arc::new(FaultStore::new(
            Arc::clone(&store),
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Get,
                    ScriptedFault::Transient("store 503 on the second segment".into()),
                )
                .with_key_contains(second.data_object_key.clone()),
            ),
        ));
        let backend: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
        let (fetcher, server) = spawn_worker_with_log_fetcher(
            Arc::clone(&backend),
            LogSegmentFetcher::new(backend),
            vec![first.clone(), second.clone()],
        )
        .await;
        let response = SliceFetcher::fetch(&fetcher, pushdown_request(&[first, second], None))
            .await
            .expect("the worker answers with a terminal summary");
        server.abort();

        assert_eq!(
            faulty.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the fault must actually have fired, once"
        );
        assert_eq!(
            response.status,
            pb::status::Code::Unavailable,
            "a non-NotFound store error is the re-dispatchable class"
        );
        assert_eq!(
            response.accounting.total_s3_bytes(),
            oracle.total_s3_bytes(),
            "the failure summary carries the bytes the first segment really \
             moved; the failed GET transferred none"
        );
        assert_eq!(
            response.accounting.total_s3_requests(),
            oracle.total_s3_requests() + FAILED_ATTEMPT_REQUESTS,
            "and the requests it issued, the failed one included"
        );
    });
}

/// A [`SliceFetcher`] double that reports `Unavailable` while carrying the spend
/// its attempts really made. This is what `RoutingSliceFetcher::dispatch` hands
/// the coordinator once every attempt at a slice has failed: one response whose
/// accounting is the SUM over those attempts (issue #1723).
struct UnavailableWorker {
    spend_bytes: u64,
    spend_requests: u64,
}

#[async_trait::async_trait]
impl SliceFetcher for UnavailableWorker {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let acct = QueryAccounting::new();
        for _ in 0..self.spend_requests {
            acct.record_s3_request(ravel_types::accounting::AccountedOp::Get);
        }
        acct.add_s3_bytes(ravel_types::accounting::AccountedOp::Get, self.spend_bytes);
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: Vec::new(),
            accounting: acct.snapshot(),
            stats: crate::fetcher::FetchStats::default(),
            series_returned: 0,
            samples_returned: 0,
            status: pb::status::Code::Unavailable,
            status_message: "every attempt was unavailable".to_string(),
        })
    }
}

/// The coordinator half of issue #1723: a slice that fails terminally still has
/// its spend folded into the query's LIVE accounting handle before the typed
/// error is raised. That handle is what `QueryEngine` reports as the query's
/// cost and what `bytes_scanned_exceeded` reads (`engine.rs`), so a fold that
/// happens only in the `Ok` arm loses the whole cost of a failed fan-out.
///
/// The scripted spend is the sum `RoutingSliceFetcher::dispatch` now carries:
/// three attempts of 4 GETs over 4096 bytes each.
///
/// Mutation proof: RED when the `fold_slice(..)` call in the `Unavailable` arm
/// of the metrics fan-out in `Distributed::fetch` is deleted. The live handle
/// then reads zero for a fan-out whose store served every attempt, and the byte
/// assertion below fails.
#[test]
fn failed_slice_spend_reaches_the_live_accounting_handle() {
    const ATTEMPTS: u64 = 3;
    const PER_ATTEMPT_BYTES: u64 = 4_096;
    const PER_ATTEMPT_REQUESTS: u64 = 4;

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
            Arc::new(UnavailableWorker {
                spend_bytes: ATTEMPTS * PER_ATTEMPT_BYTES,
                spend_requests: ATTEMPTS * PER_ATTEMPT_REQUESTS,
            }),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: 1,
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
                test_deadline(),
                None,
            )
            .await
            .expect_err("an unavailable slice fails the query typed");
        assert!(
            matches!(err, crate::error::QueryError::Distrib { .. }),
            "expected the typed distrib failure, got {err:?}"
        );
        assert_eq!(
            accounting.snapshot().total_s3_bytes(),
            ATTEMPTS * PER_ATTEMPT_BYTES,
            "the whole fan-out's spend is on the live handle, not only the \
             share a successful slice would have contributed"
        );
        assert_eq!(
            accounting.snapshot().total_s3_requests(),
            ATTEMPTS * PER_ATTEMPT_REQUESTS
        );
    });
}

/// A scripted per-attempt spend: `requests` GETs that moved `bytes`.
fn scripted_spend(requests: u64, bytes: u64) -> QueryAccountingSnapshot {
    let acct = QueryAccounting::new();
    for _ in 0..requests {
        acct.record_s3_request(AccountedOp::Get);
    }
    acct.add_s3_bytes(AccountedOp::Get, bytes);
    acct.snapshot()
}

/// A [`SliceFetcher`] double answering every signal with one scripted terminal
/// status and the spend the slice reached before it ended.
struct StatusSpendWorker {
    status: pb::status::Code,
    spend: QueryAccountingSnapshot,
}

#[async_trait::async_trait]
impl SliceFetcher for StatusSpendWorker {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        Ok(SliceResponse {
            scalar: Vec::new(),
            histogram: Vec::new(),
            partials: Vec::new(),
            accounting: self.spend,
            stats: crate::fetcher::FetchStats::default(),
            series_returned: 0,
            samples_returned: 0,
            status: self.status,
            status_message: "scripted".to_string(),
        })
    }

    async fn fetch_logs(
        &self,
        _request: pb::FetchRequest,
    ) -> Result<SliceLogResponse, DistribError> {
        Ok(SliceLogResponse {
            records: Vec::new(),
            accounting: self.spend,
            stats: crate::fetcher::FetchStats::default(),
            records_returned: 0,
            status: self.status,
            status_message: "scripted".to_string(),
        })
    }

    async fn fetch_spans(
        &self,
        _request: pb::FetchRequest,
    ) -> Result<SliceSpanResponse, DistribError> {
        Ok(SliceSpanResponse {
            spans: Vec::new(),
            accounting: self.spend,
            stats: crate::fetcher::FetchStats::default(),
            spans_returned: 0,
            status: self.status,
            status_message: "scripted".to_string(),
        })
    }
}

/// A [`SliceFetcher`] double whose every signal fails with `make()`, carrying
/// the spend the failed attempts made. This is the shape
/// `RoutingSliceFetcher::dispatch` produces once the attempt that ends a slice
/// ends it with an `Err`: there is no response left to carry the cost, so it
/// rides on the error (issue #1723).
struct FailingSpendWorker {
    make: fn() -> DistribError,
    spend: QueryAccountingSnapshot,
}

#[async_trait::async_trait]
impl SliceFetcher for FailingSpendWorker {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        Err((self.make)().with_spend(&self.spend))
    }

    async fn fetch_logs(
        &self,
        _request: pb::FetchRequest,
    ) -> Result<SliceLogResponse, DistribError> {
        Err((self.make)().with_spend(&self.spend))
    }

    async fn fetch_spans(
        &self,
        _request: pb::FetchRequest,
    ) -> Result<SliceSpanResponse, DistribError> {
        Err((self.make)().with_spend(&self.spend))
    }
}

/// The one-segment snapshot the coordinator-fold tests fan out over. What the
/// segment holds does not matter: every worker double below answers from a
/// script, never from the store.
async fn one_slice_snapshot(store: &MemoryStore) -> Snapshot {
    let seg = write_segment(
        store,
        0,
        0,
        100,
        &[SeriesDesc {
            metric: "m0".to_string(),
            samples: vec![(NS, 1.0f64.to_bits())],
        }],
    )
    .await;
    Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// Runs one fan-out of `signal` over `fetcher` and returns the live handle's
/// snapshot afterwards, so a caller can assert the exact cost a failed or
/// refused query reported.
async fn folded_spend_of(
    fetcher: Arc<dyn SliceFetcher>,
    signal: Signal,
    snapshot: &Snapshot,
    config: &EngineConfig,
) -> (Option<QueryError>, QueryAccountingSnapshot) {
    let distributed = Distributed::new(
        fetcher,
        DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 1,
        },
    );
    let accounting = QueryAccounting::new();
    let outcome = match signal {
        Signal::Logs => distributed
            .fetch_logs(
                TENANT,
                Signal::Logs,
                snapshot,
                &[],
                &[],
                &accounting,
                config,
                test_deadline(),
            )
            .await
            .err(),
        Signal::Spans => distributed
            .fetch_spans(
                TENANT,
                Signal::Spans,
                snapshot,
                &[],
                &[],
                &accounting,
                config,
                test_deadline(),
            )
            .await
            .err(),
        _ => distributed
            .fetch(
                TENANT,
                Signal::Metrics,
                snapshot,
                &[],
                &[],
                &accounting,
                config,
                test_deadline(),
                None,
            )
            .await
            .err(),
    };
    (outcome, accounting.snapshot())
}

/// Every terminal status arm of all three coordinator fan-out loops folds the
/// slice's reported spend into the query's live accounting handle (issue
/// #1723). That handle is what `QueryEngine` reports as the query's cost and
/// what the metrics, logs and spans of a query are built from, so an arm that
/// returns without folding charges the tenant nothing for work the store really
/// did.
///
/// One case per (signal, status) pair, each with its OWN spend figure, so no
/// assertion can be satisfied by another case's fold or by a last-attempt
/// figure multiplied by the case count. Both counters are asserted exactly.
///
/// Mutation proof: RED against each fold individually. Deleting the
/// `fold_slice(..)` call from the `SnapshotInvalidated`, `Corrupt` or catch-all
/// arm of `Distributed::fetch`, or the `fold_log_slice(..)`/`fold_span_slice(..)`
/// call from the corresponding arm of `fetch_logs`/`fetch_spans` (`mod.rs`),
/// leaves the live handle at zero for that case while the script says the store
/// served thousands of bytes, and the `total_s3_bytes` assertion for exactly
/// that pair fails.
#[test]
fn every_terminal_slice_status_folds_its_spend_on_every_signal() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = MemoryStore::new();
        let snapshot = one_slice_snapshot(&store).await;
        let config = EngineConfig::default();

        let signals = [Signal::Metrics, Signal::Logs, Signal::Spans];
        let statuses = [
            pb::status::Code::SnapshotInvalidated,
            pb::status::Code::Unsupported,
            pb::status::Code::BudgetExceeded,
            pb::status::Code::Corrupt,
            pb::status::Code::Unavailable,
            pb::status::Code::Timeout,
            // The catch-all arm: a status none of the arms above name.
            pb::status::Code::BadData,
        ];

        // Every case runs, and every mismatch is reported: each case is its own
        // fan-out over its own accounting handle, so one arm's fold cannot
        // affect another's result and the full list names exactly the arms that
        // dropped their spend.
        let mut wrong: Vec<String> = Vec::new();
        for (s, signal) in signals.iter().enumerate() {
            for (i, status) in statuses.iter().enumerate() {
                // A distinct figure per case, so one case's fold can never
                // stand in for another's.
                let case = (s * statuses.len() + i + 1) as u64;
                let spend = scripted_spend(case, case * 1_024);
                let (_outcome, folded) = folded_spend_of(
                    Arc::new(StatusSpendWorker {
                        status: *status,
                        spend,
                    }),
                    *signal,
                    &snapshot,
                    &config,
                )
                .await;
                if folded.total_s3_bytes() != case * 1_024 || folded.total_s3_requests() != case {
                    wrong.push(format!(
                        "{signal:?}/{status:?}: folded {} bytes in {} requests, \
                         the slice reported {} in {}",
                        folded.total_s3_bytes(),
                        folded.total_s3_requests(),
                        case * 1_024,
                        case
                    ));
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "these arms did not fold the spend their slice reported:\n{}",
            wrong.join("\n")
        );
    });
}

/// Issue #2385: a slice a worker stopped at the query's deadline (`TIMEOUT`)
/// fails the query with `DeadlineExceeded` naming the request's own deadline,
/// the error the engine's own timer raises, on every signal, never with a
/// `Distrib` error, and the spend the worker made before the stop is folded
/// first. The fan-out is called directly, outside any engine deadline wrapper,
/// so the deadline in the error is the slice loop's own.
///
/// Mutation proof: deleting the `Timeout` arm from `Distributed::fetch`,
/// `fetch_logs` or `fetch_spans` (`mod.rs`) sends that signal's slice to the
/// catch-all arm, which fails the query with `Distrib`, and the list below
/// names exactly that signal; `Duration::ZERO` in `slice_deadline_exceeded`
/// names all three.
#[test]
fn a_timeout_slice_fails_the_query_with_deadline_exceeded_on_every_signal() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = MemoryStore::new();
        let snapshot = one_slice_snapshot(&store).await;
        let config = EngineConfig::default();

        let mut wrong: Vec<String> = Vec::new();
        for (case, signal) in [Signal::Metrics, Signal::Logs, Signal::Spans]
            .into_iter()
            .enumerate()
        {
            let case = case as u64 + 1;
            let (outcome, folded) = folded_spend_of(
                Arc::new(StatusSpendWorker {
                    status: pb::status::Code::Timeout,
                    spend: scripted_spend(case, case * 2_048),
                }),
                signal,
                &snapshot,
                &config,
            )
            .await;
            if !matches!(
                outcome,
                Some(QueryError::DeadlineExceeded { deadline }) if deadline == test_deadline().request
            ) {
                wrong.push(format!("{signal:?}: failed with {outcome:?}"));
            }
            if folded.total_s3_bytes() != case * 2_048 || folded.total_s3_requests() != case {
                wrong.push(format!(
                    "{signal:?}: folded {} bytes in {} requests, the slice reported {} in {}",
                    folded.total_s3_bytes(),
                    folded.total_s3_requests(),
                    case * 2_048,
                    case
                ));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    });
}

/// The gap issue #1723's first round left open: a slice whose FINAL attempt
/// ends in an `Err` rather than a summary. `SliceFetcher::fetch` answers
/// `Result<SliceResponse, DistribError>`, so there is no response to fold and
/// the cost of every attempt rides on the error itself; the coordinator folds it
/// before it maps the error. Pinned on all three signals, each with its own
/// figure.
///
/// Mutation proof: RED against the pre-fix line. Restoring any of the three
/// `let response = result.map_err(distrib_error)?;` lines in `mod.rs` (the head
/// of the `fetch`, `fetch_logs` and `fetch_spans` collect loops) drops the
/// carried spend on the floor, and that signal's `total_s3_bytes` assertion
/// reads 0 against the thousands of bytes the script says the store served.
#[test]
fn failed_slice_error_spend_reaches_the_live_handle_on_every_signal() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = MemoryStore::new();
        let snapshot = one_slice_snapshot(&store).await;
        let config = EngineConfig::default();

        let mut wrong: Vec<String> = Vec::new();
        for (i, signal) in [Signal::Metrics, Signal::Logs, Signal::Spans]
            .iter()
            .enumerate()
        {
            let case = (i + 1) as u64;
            let spend = scripted_spend(3 * case, 7_000 * case);
            let (outcome, folded) = folded_spend_of(
                Arc::new(FailingSpendWorker {
                    make: || DistribError::Transport("worker died mid-slice".to_string()),
                    spend,
                }),
                *signal,
                &snapshot,
                &config,
            )
            .await;
            let err = outcome.expect("a failed slice fails the query");
            assert!(
                matches!(err, QueryError::Distrib { .. }),
                "{signal:?}: expected the typed distrib failure, got {err:?}"
            );
            if folded.total_s3_bytes() != 7_000 * case || folded.total_s3_requests() != 3 * case {
                wrong.push(format!(
                    "{signal:?}: folded {} bytes in {} requests, the failed \
                     attempts spent {} in {}",
                    folded.total_s3_bytes(),
                    folded.total_s3_requests(),
                    7_000 * case,
                    3 * case
                ));
            }
        }
        assert!(
            wrong.is_empty(),
            "these fan-out loops dropped the spend carried on the error:\n{}",
            wrong.join("\n")
        );
    });
}

/// A byte-cap refusal that reaches the coordinator carrying spend keeps its
/// typed 422 class with both counts intact (issue #1687 part B) AND still
/// reports that spend (issue #1723); the two must not be traded against each
/// other.
///
/// The spend on such an error is what EARLIER abandoned attempts of the same
/// slice carried, which is what `AttemptSpend::fold_into` attaches. A refusal
/// reports none of its own: `SliceStreamDecoder::push` checks the cap before it
/// stores a frame and a worker streams its summary last, so nothing of that
/// attempt's accounting is decoded by the time the cap trips. The double here
/// fabricates the carried figure directly, so this test pins the coordinator's
/// fold and its classification, not the decoder's timing.
///
/// Mutation proof: RED against either line. Restoring `mod.rs`'s
/// `let response = result.map_err(distrib_error)?;` leaves the live handle at 0
/// instead of 12288. Restoring `cap_refusal_error`'s `match err` (instead of
/// `match err.unspent()`) stops it seeing the cap breach through the spend
/// wrapper, so the refusal renders as a retryable `QueryError::Distrib` and the
/// `TooManySliceBytes` assertion fails.
#[test]
fn byte_cap_refusal_keeps_its_422_and_reports_what_it_paid() {
    const REFUSED_BYTES: u64 = 9_001;
    const CAP: u64 = 8_192;
    const SPENT_BYTES: u64 = 12_288;
    const SPENT_REQUESTS: u64 = 6;

    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = MemoryStore::new();
        let snapshot = one_slice_snapshot(&store).await;
        let (outcome, folded) = folded_spend_of(
            Arc::new(FailingSpendWorker {
                make: || {
                    DistribError::Codec(crate::distrib::codec::CodecError::SliceByteCapExceeded {
                        bytes: REFUSED_BYTES,
                        max: CAP,
                    })
                },
                spend: scripted_spend(SPENT_REQUESTS, SPENT_BYTES),
            }),
            Signal::Metrics,
            &snapshot,
            &EngineConfig::default(),
        )
        .await;

        match outcome.expect("a refused slice fails the query") {
            QueryError::TooManySliceBytes { bytes, max } => {
                assert_eq!(bytes, REFUSED_BYTES);
                assert_eq!(max, CAP);
            }
            other => panic!("expected the typed cap refusal, got {other:?}"),
        }
        assert_eq!(
            folded.total_s3_bytes(),
            SPENT_BYTES,
            "the bytes carried onto the refusal are on the live handle: the \
             store served them for this slice before the coordinator declined \
             to hold the result"
        );
        assert_eq!(folded.total_s3_requests(), SPENT_REQUESTS);
    });
}

/// Writes one real RLOG object holding `count` records and returns a matching
/// L0 `SegmentRef`. `key` makes the object key, the writer identity and the
/// content hash unique, so two segments of one slice resolve to different refs
/// on the worker.
async fn write_log_segment(store: &MemoryStore, key: u64, count: i64) -> SegmentRef {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc-{key}")),
    )];
    let stream_id = log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    let records: Vec<LogRecord> = (0..count)
        .map(|i| LogRecord {
            stream_id,
            stream_attrs: stream_attrs.clone(),
            ts_ns: i,
            observed_ts_ns: i,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: format!("line {i} of {key}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        })
        .collect();
    write_log_records(store, key, 0, &records).await
}

/// Writes `records` as one real RLOG object on `shard` and returns a matching
/// L0 `SegmentRef` whose event-time range is the records' own, on the same
/// uniqueness terms as [`write_log_segment`].
async fn write_log_records(
    store: &MemoryStore,
    key: u64,
    shard: u32,
    records: &[LogRecord],
) -> SegmentRef {
    let identity = LogObjectIdentity {
        tenant_hash: TENANT.0,
        shard,
        writer_id: [key as u8; 16],
        writer_epoch: 1,
        writer_seq: key,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for record in records {
        writer.push(record.clone()).expect("push record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let size = bytes.len() as u64;
    let object_key = format!("seg/{key}.rlog");
    store
        .put(&object_key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put log segment object");

    SegmentRef {
        data_object_key: object_key,
        object_size: size,
        min_event_ts_ns: records.iter().map(|r| r.ts_ns).min().unwrap_or(0),
        max_event_ts_ns: records.iter().map(|r| r.ts_ns).max().unwrap_or(0),
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard,
        content_hash: [key as u8; 32],
        writer_id: Uuid::from_u128(u128::from(key) + 100),
        writer_epoch: 1,
        writer_seq: key,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

/// Writes one real RSPAN object holding `count` spans and returns a matching L0
/// `SegmentRef`, on the same terms as [`write_log_segment`].
async fn write_span_segment(store: &MemoryStore, key: u64, count: u8) -> SegmentRef {
    let records: Vec<SpanRecord> = (0..count)
        .map(|i| SpanRecord {
            trace_id: [key as u8 ^ i; 16],
            span_id: [i + 1; 8],
            parent_span_id: None,
            name: "op".to_string(),
            start_ts_ns: i64::from(i),
            end_ts_ns: i64::from(i) + 10,
            status_code: ravel_rspan::StatusCode::Unset,
            status_message: None,
            attrs: Vec::new(),
        })
        .collect();
    write_span_records(store, key, 0, &records).await
}

/// Writes `records` as one real RSPAN object on `shard` and returns a matching
/// L0 `SegmentRef` whose event-time range runs from the earliest start to the
/// latest end, on the same uniqueness terms as [`write_log_segment`].
async fn write_span_records(
    store: &MemoryStore,
    key: u64,
    shard: u32,
    records: &[SpanRecord],
) -> SegmentRef {
    let identity = SpanObjectIdentity {
        tenant_hash: TENANT.0,
        shard,
        writer_id: [key as u8; 16],
        writer_epoch: 1,
        writer_seq: key,
    };
    let mut writer = RspanWriter::new(RspanConfig::default(), identity);
    for record in records {
        writer.push(record.clone());
    }
    let bytes = writer.finish().expect("finish rspan object");
    let size = bytes.len() as u64;
    let object_key = format!("seg/{key}.rspan");
    store
        .put(&object_key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put span segment object");

    SegmentRef {
        data_object_key: object_key,
        object_size: size,
        min_event_ts_ns: records.iter().map(|r| r.start_ts_ns).min().unwrap_or(0),
        max_event_ts_ns: records.iter().map(|r| r.end_ts_ns).max().unwrap_or(0),
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard,
        content_hash: [key as u8 ^ 0x5a; 32],
        writer_id: Uuid::from_u128(u128::from(key) + 200),
        writer_epoch: 1,
        writer_seq: key,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_rspan::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

/// A pinned-scope request for `segments` on `signal`, over the whole time
/// range so no segment is pruned by the ts filter.
fn signal_request(segments: &[SegmentRef], signal: Signal) -> pb::FetchRequest {
    pb::FetchRequest {
        signal: crate::distrib::codec::signal_to_u32(signal),
        window_start_ns: i64::MIN,
        window_end_ns: i64::MAX,
        ..pushdown_request(segments, None)
    }
}

/// The worker half of issue #1723 on the RLOG-family path: a log slice that
/// fetched its first segment and then took a store 503 on the second reports the
/// first segment's real cost on its terminal `Unavailable` summary, exactly as
/// the metric path does.
///
/// The oracle is what fetching that first segment alone costs through the same
/// `LogSegmentFetcher` over a clean store. The GET the fault answered adds
/// nothing to either counter: the fetcher records a request and its bytes once
/// the store has served them, so a refused GET is charged for neither.
///
/// Mutation proof: RED against the reverted line. Dropping
/// `.with_spend(&accounting, &stats)` from the fetch-error site in
/// `SeriesFetchService::run_slice_logs` (`service.rs`) sends the failure through
/// the catch-all, whose summary is built from a default snapshot, so the bytes
/// assertion reads 0 against the oracle's figure.
#[test]
fn failed_log_slice_reports_the_segments_it_already_paid_for() {
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};

    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let first = write_log_segment(&store, 1, 20).await;
        let second = write_log_segment(&store, 2, 20).await;

        let oracle = {
            let accounting = QueryAccounting::new();
            LogSegmentFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)
                .fetch_accounted_with_tenant(
                    &first,
                    TENANT,
                    &LogQuery::new(i64::MIN, i64::MAX),
                    &accounting,
                )
                .await
                .expect("the first log segment fetches cleanly");
            accounting.snapshot()
        };
        assert!(
            oracle.total_s3_bytes() > 0,
            "the fixture's first log segment must cost real bytes"
        );

        let faulty = Arc::new(FaultStore::new(
            Arc::clone(&store),
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Get,
                    ScriptedFault::Transient("store 503 on the second log segment".into()),
                )
                .with_key_contains(second.data_object_key.clone()),
            ),
        ));
        let backend: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
        let (fetcher, server) = spawn_worker_with_log_fetcher(
            Arc::clone(&backend),
            LogSegmentFetcher::new(backend),
            vec![first.clone(), second.clone()],
        )
        .await;
        let response =
            SliceFetcher::fetch(&fetcher, signal_request(&[first, second], Signal::Logs))
                .await
                .expect("the worker answers with a terminal summary");
        server.abort();

        assert_eq!(
            faulty.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the fault must actually have fired, once"
        );
        assert_eq!(
            response.status,
            pb::status::Code::Unavailable,
            "a non-NotFound store error is the re-dispatchable class"
        );
        assert_eq!(
            response.accounting.total_s3_bytes(),
            oracle.total_s3_bytes(),
            "the failure summary carries the bytes the first log segment really \
             moved; the refused GET transferred none"
        );
        assert_eq!(
            response.accounting.total_s3_requests(),
            oracle.total_s3_requests(),
            "and the requests that served them; the refused GET is charged for \
             neither bytes nor a request on this path"
        );
    });
}

/// The worker half of issue #1723 on the Spans path, on the same terms as
/// [`failed_log_slice_reports_the_segments_it_already_paid_for`].
///
/// Mutation proof: RED against the reverted line. Dropping
/// `.with_spend(&accounting, &stats)` from the fetch-error site in
/// `SeriesFetchService::run_slice_spans` (`service.rs`) makes the summary report
/// zero for a slice that really moved the oracle's bytes off the store.
#[test]
fn failed_span_slice_reports_the_segments_it_already_paid_for() {
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};

    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let first = write_span_segment(&store, 1, 8).await;
        let second = write_span_segment(&store, 2, 8).await;

        let oracle = {
            let accounting = QueryAccounting::new();
            SpanSegmentFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)
                .fetch_accounted(
                    &first,
                    TENANT,
                    &SpanQuery::ts_range(i64::MIN, i64::MAX),
                    None,
                    None,
                    &[],
                    &accounting,
                )
                .await
                .expect("the first span segment fetches cleanly");
            accounting.snapshot()
        };
        assert!(
            oracle.total_s3_bytes() > 0,
            "the fixture's first span segment must cost real bytes"
        );

        let faulty = Arc::new(FaultStore::new(
            Arc::clone(&store),
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Get,
                    ScriptedFault::Transient("store 503 on the second span segment".into()),
                )
                .with_key_contains(second.data_object_key.clone()),
            ),
        ));
        let backend: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
        let (fetcher, server) = spawn_worker_with_log_fetcher(
            Arc::clone(&backend),
            LogSegmentFetcher::new(backend),
            vec![first.clone(), second.clone()],
        )
        .await;
        let response =
            SliceFetcher::fetch(&fetcher, signal_request(&[first, second], Signal::Spans))
                .await
                .expect("the worker answers with a terminal summary");
        server.abort();

        assert_eq!(
            faulty.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the fault must actually have fired, once"
        );
        assert_eq!(response.status, pb::status::Code::Unavailable);
        assert_eq!(
            response.accounting.total_s3_bytes(),
            oracle.total_s3_bytes(),
            "the failure summary carries the bytes the first span segment \
             really moved; the refused GET transferred none"
        );
        assert_eq!(
            response.accounting.total_s3_requests(),
            oracle.total_s3_requests(),
            "and the requests that served them; the refused GET is charged for \
             neither bytes nor a request on this path"
        );
    });
}

// ---- issue #1946: log and span slices over the loopback worker -------------

/// The per-slice frame and wire-byte caps of a record-slice decoder, checked
/// exactly as [`crate::distrib::SliceStreamDecoder::push`] checks them: the
/// frame count first, then the frame's protobuf-encoded length, both before the
/// frame's payload is decoded or kept, so a refusal holds one frame past the cap
/// and nothing else.
struct SliceCaps {
    max_frames: usize,
    max_bytes: u64,
    frames: usize,
    bytes: u64,
}

impl SliceCaps {
    /// The caps a production coordinator decodes a metrics slice under.
    fn new() -> Self {
        SliceCaps {
            max_frames: crate::distrib::codec::MAX_SLICE_RESPONSE_FRAMES,
            max_bytes: crate::distrib::codec::slice_byte_cap(&EngineConfig::default()),
            frames: 0,
            bytes: 0,
        }
    }

    fn admit(&mut self, frame: &pb::FetchResponse) -> Result<(), DistribError> {
        use prost::Message;
        self.frames += 1;
        if self.frames > self.max_frames {
            return Err(DistribError::Codec(CodecError::SliceFrameCapExceeded {
                frames: self.frames,
                max: self.max_frames,
            }));
        }
        self.bytes = self.bytes.saturating_add(frame.encoded_len() as u64);
        if self.bytes > self.max_bytes {
            return Err(DistribError::Codec(CodecError::SliceByteCapExceeded {
                bytes: self.bytes,
                max: self.max_bytes,
            }));
        }
        Ok(())
    }
}

/// A record slice's terminal summary, read back.
struct RecordSummary {
    status: pb::status::Code,
    status_message: String,
    accounting: QueryAccountingSnapshot,
    stats: crate::fetcher::FetchStats,
    /// The record count the worker reported (it rides `series_returned`).
    returned: u64,
}

fn accept_summary(
    slot: &mut Option<pb::Summary>,
    summary: pb::Summary,
) -> Result<(), DistribError> {
    if slot.is_some() {
        return Err(DistribError::MultipleSummaries);
    }
    *slot = Some(summary);
    Ok(())
}

fn read_summary(summary: Option<pb::Summary>) -> Result<RecordSummary, DistribError> {
    let summary = summary.ok_or(DistribError::NoSummary)?;
    let status = summary
        .status
        .ok_or(DistribError::Codec(CodecError::MissingStatus))?;
    Ok(RecordSummary {
        status: crate::distrib::codec::decode_status_code(status.code)?,
        status_message: status.message,
        accounting: summary
            .accounting
            .map(crate::distrib::codec::decode_accounting)
            .unwrap_or_default(),
        stats: crate::fetcher::FetchStats {
            raw_f64_pages: summary.raw_f64_pages,
            raw_f64_bytes: summary.raw_f64_bytes,
            histogram_series_skipped: 0,
        },
        returned: summary.series_returned,
    })
}

/// Test support: the bounded, incremental decoder for one log slice's frames,
/// the log sibling of [`crate::distrib::SliceStreamDecoder`]. The same frame and
/// byte caps and the same typed refusals; a frame of any other signal is refused
/// with [`DistribError::FrameSignalUnsupported`]; the terminal summary ends the
/// slice. Production keeps the `Unsupported` default for log slices, so nothing
/// outside these tests decodes a `LogRecord` frame.
struct LogSliceStreamDecoder {
    caps: SliceCaps,
    records: Vec<LogRecord>,
    summary: Option<pb::Summary>,
}

impl LogSliceStreamDecoder {
    fn new() -> Self {
        LogSliceStreamDecoder {
            caps: SliceCaps::new(),
            records: Vec::new(),
            summary: None,
        }
    }

    fn with_max_frames(mut self, max_frames: usize) -> Self {
        self.caps.max_frames = max_frames;
        self
    }

    fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.caps.max_bytes = max_bytes;
        self
    }

    /// Whether the terminal summary has arrived; the reader stops pulling here.
    fn finished(&self) -> bool {
        self.summary.is_some()
    }

    fn push(&mut self, frame: pb::FetchResponse) -> Result<(), DistribError> {
        use pb::fetch_response::Frame;
        self.caps.admit(&frame)?;
        match frame.frame {
            Some(Frame::LogRecord(record)) => self
                .records
                .push(crate::distrib::codec::decode_log_record(record)?),
            Some(Frame::Summary(summary)) => accept_summary(&mut self.summary, summary)?,
            Some(Frame::Series(_)) => return Err(DistribError::FrameSignalUnsupported("series")),
            Some(Frame::Hist(_)) => return Err(DistribError::FrameSignalUnsupported("histogram")),
            Some(Frame::Span(_)) => return Err(DistribError::FrameSignalUnsupported("span")),
            Some(Frame::PartialAggregate(_)) => {
                return Err(DistribError::FrameSignalUnsupported("partial-aggregate"));
            }
            None => return Err(DistribError::EmptyFrame),
        }
        Ok(())
    }

    fn finish(self) -> Result<SliceLogResponse, DistribError> {
        let summary = read_summary(self.summary)?;
        Ok(SliceLogResponse {
            records: self.records,
            accounting: summary.accounting,
            stats: summary.stats,
            records_returned: summary.returned,
            status: summary.status,
            status_message: summary.status_message,
        })
    }
}

/// Test support: the span sibling of [`LogSliceStreamDecoder`], on the same
/// terms.
struct SpanSliceStreamDecoder {
    caps: SliceCaps,
    spans: Vec<SpanRow>,
    summary: Option<pb::Summary>,
}

impl SpanSliceStreamDecoder {
    fn new() -> Self {
        SpanSliceStreamDecoder {
            caps: SliceCaps::new(),
            spans: Vec::new(),
            summary: None,
        }
    }

    fn with_max_frames(mut self, max_frames: usize) -> Self {
        self.caps.max_frames = max_frames;
        self
    }

    fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.caps.max_bytes = max_bytes;
        self
    }

    fn finished(&self) -> bool {
        self.summary.is_some()
    }

    fn push(&mut self, frame: pb::FetchResponse) -> Result<(), DistribError> {
        use pb::fetch_response::Frame;
        self.caps.admit(&frame)?;
        match frame.frame {
            Some(Frame::Span(span)) => self
                .spans
                .push(crate::distrib::codec::decode_span_frame(span)?),
            Some(Frame::Summary(summary)) => accept_summary(&mut self.summary, summary)?,
            Some(Frame::Series(_)) => return Err(DistribError::FrameSignalUnsupported("series")),
            Some(Frame::Hist(_)) => return Err(DistribError::FrameSignalUnsupported("histogram")),
            Some(Frame::LogRecord(_)) => {
                return Err(DistribError::FrameSignalUnsupported("log-record"));
            }
            Some(Frame::PartialAggregate(_)) => {
                return Err(DistribError::FrameSignalUnsupported("partial-aggregate"));
            }
            None => return Err(DistribError::EmptyFrame),
        }
        Ok(())
    }

    fn finish(self) -> Result<SliceSpanResponse, DistribError> {
        let summary = read_summary(self.summary)?;
        Ok(SliceSpanResponse {
            spans: self.spans,
            accounting: summary.accounting,
            stats: summary.stats,
            spans_returned: summary.returned,
            status: summary.status,
            status_message: summary.status_message,
        })
    }
}

/// What one log or span slice brought across the boundary.
#[derive(Debug, Clone, PartialEq)]
struct CrossedSlice {
    status: pb::status::Code,
    /// Record frames decoded off the stream.
    records: usize,
    /// The record count the worker's summary reported.
    returned: u64,
}

/// Test support: a [`SliceFetcher`] over a loopback worker that overrides
/// `fetch_logs` and `fetch_spans` with the bounded decoders above, so
/// `Distributed::fetch_logs`/`fetch_spans` run their real fan-out and fold over
/// frames a real worker served over `tonic`. Metrics delegate to
/// [`RemoteSliceFetcher`]. Every decoded log or span slice is recorded in
/// `crossed`, in completion order.
///
/// One difference from the production metrics read: `RemoteSliceFetcher`
/// pushes every frame until the stream ends, while this fetcher stops pulling
/// at the first summary. A frame after that summary never reaches the decoder
/// here, so `DistribError::MultipleSummaries` is reachable only by calling the
/// decoders' `push` directly, as
/// `record_decoders_refuse_foreign_frames_and_need_one_summary` does.
struct LoopbackSliceFetcher {
    metrics: RemoteSliceFetcher,
    channel: Channel,
    max_frames: Option<usize>,
    max_bytes: Option<u64>,
    crossed: Arc<std::sync::Mutex<Vec<CrossedSlice>>>,
}

impl LoopbackSliceFetcher {
    fn new(channel: Channel) -> Self {
        LoopbackSliceFetcher {
            metrics: RemoteSliceFetcher::new(channel.clone()),
            channel,
            max_frames: None,
            max_bytes: None,
            crossed: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn with_max_frames(mut self, max_frames: usize) -> Self {
        self.max_frames = Some(max_frames);
        self
    }

    fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    fn crossed(&self) -> Arc<std::sync::Mutex<Vec<CrossedSlice>>> {
        Arc::clone(&self.crossed)
    }

    fn record(&self, slice: CrossedSlice) {
        self.crossed.lock().expect("crossed slices").push(slice);
    }

    async fn open(
        &self,
        request: pb::FetchRequest,
    ) -> Result<tonic::Streaming<pb::FetchResponse>, DistribError> {
        crate::distrib::proto::series_fetch_client::SeriesFetchClient::new(self.channel.clone())
            .fetch(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|s| DistribError::Transport(s.to_string()))
    }
}

#[async_trait::async_trait]
impl SliceFetcher for LoopbackSliceFetcher {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        self.metrics.fetch(request).await
    }

    async fn fetch_logs(
        &self,
        request: pb::FetchRequest,
    ) -> Result<SliceLogResponse, DistribError> {
        let mut stream = self.open(request).await?;
        let mut decoder = LogSliceStreamDecoder::new();
        if let Some(max_frames) = self.max_frames {
            decoder = decoder.with_max_frames(max_frames);
        }
        if let Some(max_bytes) = self.max_bytes {
            decoder = decoder.with_max_bytes(max_bytes);
        }
        // A refusal returns from here, dropping the stream: nothing past the
        // cap is pulled.
        while !decoder.finished() {
            let Some(frame) = stream
                .message()
                .await
                .map_err(|s| DistribError::Transport(s.to_string()))?
            else {
                break;
            };
            decoder.push(frame)?;
        }
        let response = decoder.finish()?;
        self.record(CrossedSlice {
            status: response.status,
            records: response.records.len(),
            returned: response.records_returned,
        });
        Ok(response)
    }

    async fn fetch_spans(
        &self,
        request: pb::FetchRequest,
    ) -> Result<SliceSpanResponse, DistribError> {
        let mut stream = self.open(request).await?;
        let mut decoder = SpanSliceStreamDecoder::new();
        if let Some(max_frames) = self.max_frames {
            decoder = decoder.with_max_frames(max_frames);
        }
        if let Some(max_bytes) = self.max_bytes {
            decoder = decoder.with_max_bytes(max_bytes);
        }
        while !decoder.finished() {
            let Some(frame) = stream
                .message()
                .await
                .map_err(|s| DistribError::Transport(s.to_string()))?
            else {
                break;
            };
            decoder.push(frame)?;
        }
        let response = decoder.finish()?;
        self.record(CrossedSlice {
            status: response.status,
            records: response.spans.len(),
            returned: response.spans_returned,
        });
        Ok(response)
    }
}

type WorkerStream = std::pin::Pin<
    Box<dyn futures::Stream<Item = Result<pb::FetchResponse, tonic::Status>> + Send + 'static>,
>;

/// A worker that counts every frame its transport pulls off the slice stream,
/// so a test can tell a reader that stopped at a cap from one that drained.
struct CountingWorker<S> {
    inner: S,
    produced: Arc<std::sync::atomic::AtomicUsize>,
}

#[tonic::async_trait]
impl<S: SeriesFetch> SeriesFetch for CountingWorker<S> {
    type FetchStream = WorkerStream;

    async fn fetch(
        &self,
        request: tonic::Request<pb::FetchRequest>,
    ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
        let stream = self.inner.fetch(request).await?.into_inner();
        let produced = Arc::clone(&self.produced);
        Ok(tonic::Response::new(Box::pin(futures::StreamExt::inspect(
            stream,
            move |_| {
                produced.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
        ))))
    }
}

/// Serves `service` on `127.0.0.1:0` and returns a lazily connected channel to
/// it plus the server task (abort it to shut the worker down).
async fn serve_loopback<S: SeriesFetch>(service: S) -> (Channel, JoinHandle<()>) {
    let server = crate::distrib::proto::series_fetch_server::SeriesFetchServer::new(service);
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("addr")).expect("bind");
    let addr = incoming.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(server)
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_lazy();
    (channel, handle)
}

/// A loopback worker over `store` whose resolver knows `resolvable`, with the
/// log and span fetch paths wired as asked, behind a [`LoopbackSliceFetcher`].
/// The counter is every frame the worker's transport pulled.
async fn spawn_slice_worker(
    store: Arc<MemoryStore>,
    resolvable: Vec<SegmentRef>,
    logs: bool,
    spans: bool,
) -> (
    LoopbackSliceFetcher,
    JoinHandle<()>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let mut service = SeriesFetchService::new(
        SegmentFetcher::new(Arc::clone(&backend)),
        Arc::new(SnapshotSegmentResolver::new(resolvable)),
    );
    if logs {
        service = service.with_log_fetcher(LogSegmentFetcher::new(Arc::clone(&backend)));
    }
    if spans {
        service = service.with_span_fetcher(SpanSegmentFetcher::new(backend));
    }
    let produced = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (channel, server) = serve_loopback(CountingWorker {
        inner: service,
        produced: Arc::clone(&produced),
    })
    .await;
    (LoopbackSliceFetcher::new(channel), server, produced)
}

fn fan_out(max_parallel_slices: usize) -> DistribThresholds {
    DistribThresholds {
        min_store_bytes: 0,
        min_segments: 0,
        max_parallel_slices,
    }
}

fn snapshot_of(segments: &[SegmentRef]) -> Snapshot {
    Snapshot {
        segments: segments.to_vec(),
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// The local log reference: every segment read through `LogSegmentFetcher` over
/// the whole time range, concatenated in `segments` order, plus what it cost.
async fn local_log_records(
    store: &Arc<MemoryStore>,
    segments: &[SegmentRef],
    erasure: &[ErasurePredicate],
) -> (Vec<LogRecord>, QueryAccountingSnapshot) {
    let fetcher = LogSegmentFetcher::new(Arc::clone(store) as Arc<dyn ObjectStoreBackend>);
    let accounting = QueryAccounting::new();
    let query = LogQuery::new(i64::MIN, i64::MAX).with_erasure(erasure.to_vec());
    let mut records = Vec::new();
    for seg in segments {
        if let Some(out) = fetcher
            .fetch_accounted_with_tenant(seg, TENANT, &query, &accounting)
            .await
            .expect("local log fetch")
        {
            records.extend(out.records);
        }
    }
    (records, accounting.snapshot())
}

/// The local span reference, on the same terms as [`local_log_records`], with
/// erasure applied through `is_erased_span` as the local span scan applies it.
async fn local_span_rows(
    store: &Arc<MemoryStore>,
    segments: &[SegmentRef],
    erasure: &[ErasurePredicate],
) -> (Vec<SpanRow>, QueryAccountingSnapshot) {
    let fetcher = SpanSegmentFetcher::new(Arc::clone(store) as Arc<dyn ObjectStoreBackend>);
    let accounting = QueryAccounting::new();
    let query = SpanQuery::ts_range(i64::MIN, i64::MAX);
    let mut rows = Vec::new();
    for seg in segments {
        if let Some(out) = fetcher
            .fetch_accounted(seg, TENANT, &query, None, None, &[], &accounting)
            .await
            .expect("local span fetch")
        {
            rows.extend(out.records);
        }
    }
    rows.retain(|row| {
        !crate::erasure::is_erased_span(&row.record.attrs, row.record.start_ts_ns, erasure)
    });
    (rows, accounting.snapshot())
}

/// The part of `local` a slice summary can carry: the page-byte and open
/// counters are process-local and not on the wire (`codec::decode_accounting`),
/// so a fan-out folds every counter except those.
fn wire_visible(local: QueryAccountingSnapshot) -> QueryAccountingSnapshot {
    crate::distrib::codec::decode_accounting(crate::distrib::codec::encode_accounting(&local))
}

/// Sorts `records` under the documented RLOG total order (ADR-0071, log and
/// span fan-out amendment, "Shipped status"), written out here field by field
/// rather than through `log_record_order_key`, so a change to the production key
/// diverges from it.
fn documented_log_order(records: &mut [LogRecord]) {
    records.sort_by_cached_key(|r| {
        let mut attrs = Vec::new();
        for pair in &r.attrs {
            attrs.extend_from_slice(&ravel_types::logstream::canonical_attr_bytes(
                std::slice::from_ref(pair),
            ));
        }
        (
            r.ts_ns,
            r.stream_id.0,
            r.stream_attrs.clone(),
            r.observed_ts_ns,
            r.severity_num,
            r.severity_text.clone(),
            r.body.clone(),
            r.trace_id,
            r.span_id,
            r.flags,
            attrs,
        )
    });
}

/// Sorts `rows` under the documented span total order, through the
/// `span_order_key` reference definition rather than the production `span_cmp`.
fn documented_span_order(rows: &mut [SpanRow]) {
    rows.sort_by_cached_key(span_order_key);
}

/// Each record as its wire encoding, so a comparison is bit for bit (`f64`
/// attribute values cross as their bit patterns).
fn log_wire(records: &[LogRecord]) -> Vec<Vec<u8>> {
    use prost::Message;
    records
        .iter()
        .map(|r| crate::distrib::codec::encode_log_record(r).encode_to_vec())
        .collect()
}

fn span_wire(rows: &[SpanRow]) -> Vec<Vec<u8>> {
    use prost::Message;
    rows.iter()
        .map(|r| crate::distrib::codec::encode_span_frame(r).encode_to_vec())
        .collect()
}

fn assert_same_sequence(what: &str, got: &[Vec<u8>], want: &[Vec<u8>]) {
    assert_eq!(got.len(), want.len(), "{what}: record count differs");
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        assert_eq!(g, w, "{what}: record {i} differs");
    }
}

fn severity(i: u8) -> (u8, &'static str) {
    match i % 3 {
        0 => (9, "INFO"),
        1 => (13, "WARN"),
        _ => (17, "ERROR"),
    }
}

/// Attribute value bit patterns whose distinctions a `==` comparison loses.
fn float_bits() -> Vec<u64> {
    vec![
        0.0f64.to_bits(),
        (-0.0f64).to_bits(),
        f64::NAN.to_bits(),
        0x7ff8_0000_0000_0001,
        1.5f64.to_bits(),
    ]
}

fn arb_log_record() -> impl Strategy<Value = LogRecord> {
    (
        (0u8..3, 0i64..12, 0i64..3, 0u8..3),
        (
            0u8..3,
            prop::option::of(0u8..2),
            prop::option::of(1u8..3),
            0u32..2,
        ),
        (
            prop::option::of(0u8..3),
            prop::option::of(prop::sample::select(float_bits())),
        ),
    )
        .prop_map(
            |((service, ts, observed, sev), (body, trace, span, flags), (user, float))| {
                let (stream_id, stream_attrs) = log_stream(&format!("svc{service}"));
                let (severity_num, severity_text) = severity(sev);
                let mut attrs = Vec::new();
                if let Some(bits) = float {
                    attrs.push(("f".to_string(), AttrValue::F64(f64::from_bits(bits))));
                }
                if let Some(user) = user {
                    attrs.push(("user".to_string(), AttrValue::Str(format!("u{user}"))));
                }
                LogRecord {
                    stream_id,
                    stream_attrs,
                    ts_ns: ts,
                    observed_ts_ns: ts + observed,
                    severity_num,
                    severity_text: severity_text.to_string(),
                    body: format!("body {body}"),
                    trace_id: trace.map(|t| [0xA0 + t; 16]),
                    span_id: span.map(|s| [s; 8]),
                    flags,
                    attrs,
                }
            },
        )
}

fn span_status(i: u8) -> ravel_rspan::StatusCode {
    match i % 3 {
        0 => ravel_rspan::StatusCode::Unset,
        1 => ravel_rspan::StatusCode::Ok,
        _ => ravel_rspan::StatusCode::Error,
    }
}

/// A span with its attribute keys in ascending order, as RSPAN stores them.
#[allow(clippy::too_many_arguments)]
fn span(
    trace: u8,
    span_id: u8,
    parent: Option<u8>,
    name: &str,
    start: i64,
    end: i64,
    status: u8,
    service: Option<&str>,
    user: Option<&str>,
) -> SpanRecord {
    let mut attrs = Vec::new();
    if let Some(service) = service {
        attrs.push(("service.name".to_string(), service.to_string()));
    }
    if let Some(user) = user {
        attrs.push(("user".to_string(), user.to_string()));
    }
    SpanRecord {
        trace_id: [0xA0 + trace; 16],
        span_id: [span_id; 8],
        parent_span_id: parent.map(|p| [p; 8]),
        name: name.to_string(),
        start_ts_ns: start,
        end_ts_ns: end,
        status_code: span_status(status),
        status_message: None,
        attrs,
    }
}

fn arb_span_record() -> impl Strategy<Value = SpanRecord> {
    (
        (0u8..3, 1u8..4, prop::option::of(1u8..4), 0u8..2),
        (0i64..12, 0i64..5, 0u8..3, prop::option::of(0u8..2)),
        (prop::option::of(0u8..2), prop::option::of(0u8..3)),
    )
        .prop_map(
            |((trace, span_id, parent, name), (start, dur, status, message), (svc, user))| {
                let service = svc.map(|s| format!("svc{s}"));
                let user = user.map(|u| format!("u{u}"));
                SpanRecord {
                    status_message: message.map(|m| format!("msg{m}")),
                    ..span(
                        trace,
                        span_id,
                        parent,
                        &format!("op{name}"),
                        start,
                        start + dur,
                        status,
                        service.as_deref(),
                        user.as_deref(),
                    )
                }
            },
        )
}

/// The signals a worker serves through `run_slice_logs`: the RLOG family.
const LOG_SIGNALS: [Signal; 3] = [Signal::Logs, Signal::Alerts, Signal::Audit];

/// One proptest log differential on `signal`: write the corpus, then assert the
/// worker's emission order for one slice of every segment, the coordinator's
/// merged result over `cap` slices, the folded accounting, and every slice
/// summary.
async fn run_log_differential(corpus: Vec<(u32, Vec<LogRecord>)>, cap: usize, signal: Signal) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for (key, (shard, records)) in corpus.iter().enumerate() {
        segments.push(write_log_records(&store, key as u64 + 1, *shard, records).await);
    }
    let (local, local_cost) = local_log_records(&store, &segments, &[]).await;
    let local_cost = wire_visible(local_cost);
    let mut expected = local.clone();
    documented_log_order(&mut expected);

    let (fetcher, server, _produced) =
        spawn_slice_worker(Arc::clone(&store), segments.clone(), true, true).await;
    let crossed = fetcher.crossed();

    // The order the worker emits: each pinned segment's local read order,
    // segment after segment. The coordinator, not the worker, orders.
    let one = SliceFetcher::fetch_logs(&fetcher, signal_request(&segments, signal))
        .await
        .expect("one log slice over every segment");
    assert_eq!(
        one.status,
        pb::status::Code::Ok,
        "{signal:?}: the worker serves the slice"
    );
    assert_same_sequence(
        &format!("{signal:?}: worker emission order"),
        &log_wire(&one.records),
        &log_wire(&local),
    );
    assert_eq!(one.records_returned, local.len() as u64);
    assert_eq!(one.accounting, local_cost);
    crossed.lock().expect("crossed").clear();

    let snapshot = snapshot_of(&segments);
    let distributed = Distributed::new(Arc::new(fetcher), fan_out(cap));
    let accounting = QueryAccounting::new();
    let got = distributed
        .fetch_logs(
            TENANT,
            signal,
            &snapshot,
            &[],
            &[],
            &accounting,
            &EngineConfig::default(),
            test_deadline(),
        )
        .await
        .expect("distributed log fetch")
        .expect("served, not a local fallback");
    server.abort();

    assert_same_sequence(
        &format!("{signal:?}: coordinator merge vs the documented log order"),
        &log_wire(&got),
        &log_wire(&expected),
    );
    assert_eq!(
        accounting.snapshot(),
        local_cost,
        "the fan-out folds exactly the cost a local read pays"
    );
    let crossed = crossed.lock().expect("crossed").clone();
    assert_eq!(
        crossed.len(),
        crate::distrib::partition::partition_snapshot(&snapshot, cap).len()
    );
    for slice in &crossed {
        assert_eq!(slice.status, pb::status::Code::Ok);
        assert_eq!(slice.returned, slice.records as u64, "{slice:?}");
    }
    assert_eq!(
        crossed.iter().map(|s| s.records).sum::<usize>(),
        local.len()
    );
}

/// The span sibling of [`run_log_differential`].
async fn run_span_differential(corpus: Vec<(u32, Vec<SpanRecord>)>, cap: usize) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for (key, (shard, records)) in corpus.iter().enumerate() {
        segments.push(write_span_records(&store, key as u64 + 1, *shard, records).await);
    }
    let (local, local_cost) = local_span_rows(&store, &segments, &[]).await;
    let local_cost = wire_visible(local_cost);
    let mut expected = local.clone();
    documented_span_order(&mut expected);

    let (fetcher, server, _produced) =
        spawn_slice_worker(Arc::clone(&store), segments.clone(), true, true).await;
    let crossed = fetcher.crossed();

    let one = SliceFetcher::fetch_spans(&fetcher, signal_request(&segments, Signal::Spans))
        .await
        .expect("one span slice over every segment");
    assert_eq!(one.status, pb::status::Code::Ok);
    assert_same_sequence(
        "worker emission order",
        &span_wire(&one.spans),
        &span_wire(&local),
    );
    assert_eq!(one.spans_returned, local.len() as u64);
    assert_eq!(one.accounting, local_cost);
    crossed.lock().expect("crossed").clear();

    let snapshot = snapshot_of(&segments);
    let distributed = Distributed::new(Arc::new(fetcher), fan_out(cap));
    let accounting = QueryAccounting::new();
    let got = distributed
        .fetch_spans(
            TENANT,
            Signal::Spans,
            &snapshot,
            &[],
            &[],
            &accounting,
            &EngineConfig::default(),
            test_deadline(),
        )
        .await
        .expect("distributed span fetch")
        .expect("served, not a local fallback");
    server.abort();

    assert_same_sequence(
        "coordinator merge vs the documented span order",
        &span_wire(&got),
        &span_wire(&expected),
    );
    assert_eq!(accounting.snapshot(), local_cost);
    let crossed = crossed.lock().expect("crossed").clone();
    assert_eq!(
        crossed.len(),
        crate::distrib::partition::partition_snapshot(&snapshot, cap).len()
    );
    for slice in &crossed {
        assert_eq!(slice.status, pb::status::Code::Ok);
        assert_eq!(slice.returned, slice.records as u64, "{slice:?}");
    }
    assert_eq!(
        crossed.iter().map(|s| s.records).sum::<usize>(),
        local.len()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// A distributed log fetch over the loopback worker equals the local fetch
    /// of the same segments merged under the documented total order, bit for
    /// bit, for arbitrary corpora and slice counts; the worker emits each
    /// segment's local read order; the folded cost and every slice summary
    /// match. Every case runs on each of Logs, Alerts and Audit, the signals
    /// `run_slice_logs` serves.
    ///
    /// Mutation proof: RED when `log_record_order_key` (mod.rs) swaps its
    /// `ts_ns` and `stream_id` fields, or when `merge_log_records` sorts with
    /// `b.0.cmp(&a.0)`: the merge then disagrees with the documented order.
    /// RED when `Signal::Alerts` is moved from the `run_slice_logs` arm of
    /// `SeriesFetchService::run_slice` (service.rs) to the Unsupported arm: the
    /// Alerts slice is then refused.
    #[test]
    fn distributed_log_fetch_equals_local_bitwise(
        corpus in prop::collection::vec(
            (0u32..4, prop::collection::vec(arb_log_record(), 1..10)),
            1..6,
        ),
        cap in 1usize..=4,
    ) {
        let rt = Runtime::new().expect("runtime");
        for signal in LOG_SIGNALS {
            rt.block_on(run_log_differential(corpus.clone(), cap, signal));
        }
    }

    /// The span sibling of `distributed_log_fetch_equals_local_bitwise`.
    ///
    /// Mutation proof: RED when `span_cmp` (mod.rs) compares `span_id` before
    /// `trace_id`, or when `merge_spans` sorts with the comparator reversed.
    #[test]
    fn distributed_span_fetch_equals_local_bitwise(
        corpus in prop::collection::vec(
            (0u32..4, prop::collection::vec(arb_span_record(), 1..10)),
            1..6,
        ),
        cap in 1usize..=4,
    ) {
        let rt = Runtime::new().expect("runtime");
        rt.block_on(run_span_differential(corpus, cap));
    }
}

/// Runs `fetch_logs` on `Signal::Logs` over `snapshot` through `fetcher` with
/// `cap` slices.
async fn distributed_logs(
    fetcher: Arc<dyn SliceFetcher>,
    snapshot: &Snapshot,
    erasure: &[ErasurePredicate],
    cap: usize,
    accounting: &QueryAccounting,
) -> Result<Option<Vec<LogRecord>>, QueryError> {
    distributed_logs_on(Signal::Logs, fetcher, snapshot, erasure, cap, accounting).await
}

/// [`distributed_logs`] on `signal`.
async fn distributed_logs_on(
    signal: Signal,
    fetcher: Arc<dyn SliceFetcher>,
    snapshot: &Snapshot,
    erasure: &[ErasurePredicate],
    cap: usize,
    accounting: &QueryAccounting,
) -> Result<Option<Vec<LogRecord>>, QueryError> {
    Distributed::new(fetcher, fan_out(cap))
        .fetch_logs(
            TENANT,
            signal,
            snapshot,
            &[],
            erasure,
            accounting,
            &EngineConfig::default(),
            test_deadline(),
        )
        .await
}

/// Runs `fetch_spans` over `snapshot` through `fetcher` with `cap` slices.
async fn distributed_spans(
    fetcher: Arc<dyn SliceFetcher>,
    snapshot: &Snapshot,
    erasure: &[ErasurePredicate],
    cap: usize,
    accounting: &QueryAccounting,
) -> Result<Option<Vec<SpanRow>>, QueryError> {
    Distributed::new(fetcher, fan_out(cap))
        .fetch_spans(
            TENANT,
            Signal::Spans,
            snapshot,
            &[],
            erasure,
            accounting,
            &EngineConfig::default(),
            test_deadline(),
        )
        .await
}

fn crossed_now(crossed: &Arc<std::sync::Mutex<Vec<CrossedSlice>>>) -> Vec<CrossedSlice> {
    let mut slices = crossed.lock().expect("crossed").clone();
    slices.sort_by_key(|s| s.records);
    slices
}

fn user_erasure(user: &str) -> Vec<ErasurePredicate> {
    vec![ErasurePredicate::windowless(vec![(
        "user".to_string(),
        user.to_string(),
    )])]
}

/// Three segments on three shards whose records interleave in time, with the
/// `alpha` stream in every one of them. Read as one slice, the worker emits the
/// records segment after segment, which is not the total order; read as three
/// slices, the stream straddles all three. Either way the coordinator merge
/// produces the documented order. The precondition assertion keeps the corpus
/// honest: if the concatenation were already sorted, the merge would not be
/// under test. It runs on each of Logs, Alerts and Audit.
///
/// Mutation proof: RED when `merge_log_records` (mod.rs) sorts with
/// `b.0.cmp(&a.0)`, when its `sort_by` is deleted (slice-order concatenation),
/// or when `log_record_order_key` leads with `stream_id` instead of `ts_ns`.
/// RED when `Signal::Alerts` is moved from the `run_slice_logs` arm of
/// `SeriesFetchService::run_slice` (service.rs) to the Unsupported arm.
#[test]
fn log_slice_over_several_segments_merges_into_the_total_order() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let mut segments = Vec::new();
        for key in 1..=3u64 {
            let records: Vec<LogRecord> = (0..3)
                .map(|i| {
                    let service = if i == 1 { "beta" } else { "alpha" };
                    log_record(service, i * 3 + key as i64, "line", &[])
                })
                .collect();
            segments.push(write_log_records(&store, key, key as u32 - 1, &records).await);
        }
        let (local, _) = local_log_records(&store, &segments, &[]).await;
        let mut expected = local.clone();
        documented_log_order(&mut expected);
        assert_ne!(
            log_wire(&local),
            log_wire(&expected),
            "precondition: segment order is not already the total order"
        );

        let (fetcher, server, _produced) =
            spawn_slice_worker(Arc::clone(&store), segments.clone(), true, true).await;
        let crossed = fetcher.crossed();
        let fetcher = Arc::new(fetcher);
        for signal in LOG_SIGNALS {
            let one = fetcher
                .fetch_logs(signal_request(&segments, signal))
                .await
                .expect("one log slice");
            assert_eq!(one.status, pb::status::Code::Ok, "{signal:?}");
            assert_same_sequence(
                &format!("{signal:?}: worker emission"),
                &log_wire(&one.records),
                &log_wire(&local),
            );
            for (cap, per_slice) in [(1, vec![9]), (3, vec![3, 3, 3])] {
                crossed.lock().expect("crossed").clear();
                let got = distributed_logs_on(
                    signal,
                    Arc::clone(&fetcher) as Arc<dyn SliceFetcher>,
                    &snapshot_of(&segments),
                    &[],
                    cap,
                    &QueryAccounting::new(),
                )
                .await
                .expect("distributed log fetch")
                .expect("served");
                assert_same_sequence(
                    &format!("{signal:?}: coordinator merge over {cap} slice(s)"),
                    &log_wire(&got),
                    &log_wire(&expected),
                );
                let slices: Vec<usize> = crossed_now(&crossed).iter().map(|s| s.records).collect();
                assert_eq!(
                    slices, per_slice,
                    "{signal:?}: records per slice at cap {cap}"
                );
            }
        }
        server.abort();
    });
}

/// The span sibling of
/// [`log_slice_over_several_segments_merges_into_the_total_order`]: three
/// segments on three shards, each holding one span of each of three traces, so
/// every trace straddles every slice when read as three.
///
/// Mutation proof: RED when `merge_spans` (mod.rs) reverses its comparator,
/// when its `sort_by` is deleted, or when `span_cmp` compares `span_id` before
/// `trace_id`.
#[test]
fn span_slice_over_several_segments_merges_into_the_total_order() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let mut segments = Vec::new();
        for key in 1..=3u8 {
            let records: Vec<SpanRecord> = (0..3u8)
                .map(|trace| {
                    let start = i64::from(2 - trace) * 10 + i64::from(key);
                    span(
                        trace,
                        key,
                        None,
                        "op",
                        start,
                        start + 5,
                        0,
                        Some("svc"),
                        None,
                    )
                })
                .collect();
            segments.push(
                write_span_records(&store, u64::from(key), u32::from(key) - 1, &records).await,
            );
        }
        let (local, _) = local_span_rows(&store, &segments, &[]).await;
        let mut expected = local.clone();
        documented_span_order(&mut expected);
        assert_ne!(
            span_wire(&local),
            span_wire(&expected),
            "precondition: segment order is not already the total order"
        );

        let (fetcher, server, _produced) =
            spawn_slice_worker(Arc::clone(&store), segments.clone(), true, true).await;
        let crossed = fetcher.crossed();
        let fetcher = Arc::new(fetcher);
        let one = fetcher
            .fetch_spans(signal_request(&segments, Signal::Spans))
            .await
            .expect("one span slice");
        assert_same_sequence(
            "worker emission",
            &span_wire(&one.spans),
            &span_wire(&local),
        );
        for (cap, per_slice) in [(1, vec![9]), (3, vec![3, 3, 3])] {
            crossed.lock().expect("crossed").clear();
            let got = distributed_spans(
                Arc::clone(&fetcher) as Arc<dyn SliceFetcher>,
                &snapshot_of(&segments),
                &[],
                cap,
                &QueryAccounting::new(),
            )
            .await
            .expect("distributed span fetch")
            .expect("served");
            assert_same_sequence(
                &format!("coordinator merge over {cap} slice(s)"),
                &span_wire(&got),
                &span_wire(&expected),
            );
            let slices: Vec<usize> = crossed_now(&crossed).iter().map(|s| s.records).collect();
            assert_eq!(slices, per_slice, "spans per slice at cap {cap}");
        }
        server.abort();
    });
}

/// How many entries of `got` are byte-identical to `one`.
fn copies_of(got: &[Vec<u8>], one: &[u8]) -> usize {
    got.iter().filter(|g| g.as_slice() == one).count()
}

/// A byte-identical log record written into two segments on two shards comes
/// back twice from one distributed fetch, whether the two segments share a
/// slice or not, and the result equals the local read of both segments under
/// the documented order. Logs have no query-time dedup
/// (docs/consistency-model.md, "logs and spans"): the second copy is a retry's
/// legitimate duplicate user data.
///
/// Mutation proof: RED when `merge_log_records` (mod.rs) runs
/// `keyed.dedup_by(|a, b| a.0 == b.0)` after its sort: the merged result then
/// holds one copy.
#[test]
fn logs_coordinator_preserves_duplicate_records_across_slices() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let dup = log_record("alpha", 5, "retried", &[("user", "u1")]);
        let other = log_record("beta", 3, "other", &[]);
        let segments = vec![
            write_log_records(&store, 1, 0, &[dup.clone(), other]).await,
            write_log_records(&store, 2, 1, std::slice::from_ref(&dup)).await,
        ];
        let dup_wire = log_wire(std::slice::from_ref(&dup)).remove(0);
        let (local, _) = local_log_records(&store, &segments, &[]).await;
        assert_eq!(
            copies_of(&log_wire(&local), &dup_wire),
            2,
            "precondition: the local read returns both copies"
        );
        let mut expected = local.clone();
        documented_log_order(&mut expected);

        let (fetcher, server, _produced) =
            spawn_slice_worker(Arc::clone(&store), segments.clone(), true, true).await;
        let crossed = fetcher.crossed();
        let fetcher = Arc::new(fetcher);
        for (cap, per_slice) in [(1, vec![3]), (2, vec![1, 2])] {
            crossed.lock().expect("crossed").clear();
            let got = distributed_logs(
                Arc::clone(&fetcher) as Arc<dyn SliceFetcher>,
                &snapshot_of(&segments),
                &[],
                cap,
                &QueryAccounting::new(),
            )
            .await
            .expect("distributed log fetch")
            .expect("served");
            let got = log_wire(&got);
            assert_eq!(
                copies_of(&got, &dup_wire),
                2,
                "both copies of the duplicate record survive the merge over {cap} slice(s)"
            );
            assert_same_sequence(
                &format!("coordinator merge over {cap} slice(s) vs the local read"),
                &got,
                &log_wire(&expected),
            );
            let slices: Vec<usize> = crossed_now(&crossed).iter().map(|s| s.records).collect();
            assert_eq!(slices, per_slice, "records per slice at cap {cap}");
        }
        server.abort();
    });
}

/// The span sibling of
/// [`logs_coordinator_preserves_duplicate_records_across_slices`]: a
/// byte-identical span in two segments on two shards comes back twice, as the
/// local read returns it. Spans have no query-time dedup either
/// (docs/consistency-model.md, "logs and spans").
///
/// Mutation proof: RED when `merge_spans` (mod.rs) runs
/// `spans.dedup_by(|a, b| span_cmp(a, b).is_eq())` after its sort.
#[test]
fn spans_coordinator_preserves_duplicate_spans_across_slices() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let dup = span(0, 1, None, "retried", 5, 9, 0, Some("svc"), Some("u1"));
        let other = span(1, 2, Some(1), "other", 3, 4, 1, Some("svc"), None);
        let segments = vec![
            write_span_records(&store, 1, 0, &[dup.clone(), other]).await,
            write_span_records(&store, 2, 1, std::slice::from_ref(&dup)).await,
        ];
        let (local, _) = local_span_rows(&store, &segments, &[]).await;
        let dup_wire = span_wire(&local[..1]).remove(0);
        assert_eq!(
            copies_of(&span_wire(&local), &dup_wire),
            2,
            "precondition: the local read returns both copies"
        );
        let mut expected = local.clone();
        documented_span_order(&mut expected);

        let (fetcher, server, _produced) =
            spawn_slice_worker(Arc::clone(&store), segments.clone(), true, true).await;
        let crossed = fetcher.crossed();
        let fetcher = Arc::new(fetcher);
        for (cap, per_slice) in [(1, vec![3]), (2, vec![1, 2])] {
            crossed.lock().expect("crossed").clear();
            let got = distributed_spans(
                Arc::clone(&fetcher) as Arc<dyn SliceFetcher>,
                &snapshot_of(&segments),
                &[],
                cap,
                &QueryAccounting::new(),
            )
            .await
            .expect("distributed span fetch")
            .expect("served");
            let got = span_wire(&got);
            assert_eq!(
                copies_of(&got, &dup_wire),
                2,
                "both copies of the duplicate span survive the merge over {cap} slice(s)"
            );
            assert_same_sequence(
                &format!("coordinator merge over {cap} slice(s) vs the local read"),
                &got,
                &span_wire(&expected),
            );
            let slices: Vec<usize> = crossed_now(&crossed).iter().map(|s| s.records).collect();
            assert_eq!(slices, per_slice, "spans per slice at cap {cap}");
        }
        server.abort();
    });
}

/// A slice whose every record is erased is served as an empty `Ok` slice, not
/// read as a fallback or a failure: the query still gets the other slice's
/// records, equal to the local read.
///
/// Mutation proof: RED when `run_slice_logs` (service.rs) builds its query
/// without `.with_erasure(erasure)`: the shard-1 slice then carries its two
/// records and the crossed-slice assertion fails.
#[test]
fn an_empty_log_slice_is_served_not_fallen_back() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let kept: Vec<LogRecord> = (0..3)
            .map(|i| log_record("alpha", i, "kept", &[("user", "u1")]))
            .collect();
        let erased: Vec<LogRecord> = (0..2)
            .map(|i| log_record("beta", i, "erased", &[("user", "u0")]))
            .collect();
        let segments = vec![
            write_log_records(&store, 1, 0, &kept).await,
            write_log_records(&store, 2, 1, &erased).await,
        ];
        let erasure = user_erasure("u0");
        let (mut expected, _) = local_log_records(&store, &segments, &erasure).await;
        documented_log_order(&mut expected);
        assert_eq!(expected.len(), 3);

        let (fetcher, server, _produced) =
            spawn_slice_worker(Arc::clone(&store), segments.clone(), true, true).await;
        let crossed = fetcher.crossed();
        let got = distributed_logs(
            Arc::new(fetcher),
            &snapshot_of(&segments),
            &erasure,
            2,
            &QueryAccounting::new(),
        )
        .await
        .expect("distributed log fetch")
        .expect("an empty slice is served, never a local fallback");
        server.abort();
        assert_same_sequence("served records", &log_wire(&got), &log_wire(&expected));
        assert_eq!(
            crossed_now(&crossed),
            vec![
                CrossedSlice {
                    status: pb::status::Code::Ok,
                    records: 0,
                    returned: 0,
                },
                CrossedSlice {
                    status: pb::status::Code::Ok,
                    records: 3,
                    returned: 3,
                },
            ]
        );
    });
}

/// The span sibling of [`an_empty_log_slice_is_served_not_fallen_back`].
///
/// Mutation proof: RED when the erasure `retain` in `run_slice_spans`
/// (service.rs) is deleted.
#[test]
fn an_empty_span_slice_is_served_not_fallen_back() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let kept: Vec<SpanRecord> = (0..3u8)
            .map(|i| span(0, i + 1, None, "op", 1, 2, 0, None, Some("u1")))
            .collect();
        let erased: Vec<SpanRecord> = (0..2u8)
            .map(|i| span(1, i + 1, None, "op", 1, 2, 0, None, Some("u0")))
            .collect();
        let segments = vec![
            write_span_records(&store, 1, 0, &kept).await,
            write_span_records(&store, 2, 1, &erased).await,
        ];
        let erasure = user_erasure("u0");
        let (mut expected, _) = local_span_rows(&store, &segments, &erasure).await;
        documented_span_order(&mut expected);
        assert_eq!(expected.len(), 3);

        let (fetcher, server, _produced) =
            spawn_slice_worker(Arc::clone(&store), segments.clone(), true, true).await;
        let crossed = fetcher.crossed();
        let got = distributed_spans(
            Arc::new(fetcher),
            &snapshot_of(&segments),
            &erasure,
            2,
            &QueryAccounting::new(),
        )
        .await
        .expect("distributed span fetch")
        .expect("an empty slice is served, never a local fallback");
        server.abort();
        assert_same_sequence("served spans", &span_wire(&got), &span_wire(&expected));
        assert_eq!(
            crossed_now(&crossed),
            vec![
                CrossedSlice {
                    status: pb::status::Code::Ok,
                    records: 0,
                    returned: 0,
                },
                CrossedSlice {
                    status: pb::status::Code::Ok,
                    records: 3,
                    returned: 3,
                },
            ]
        );
    });
}

/// The erasure fixture: two shards, three of seven records carrying the
/// erased subject `user=u0`.
fn erasure_log_corpus() -> [Vec<LogRecord>; 2] {
    [
        vec![
            log_record("alpha", 0, "a", &[("user", "u0")]),
            log_record("alpha", 1, "b", &[("user", "u1")]),
            log_record("alpha", 2, "c", &[]),
            log_record("alpha", 3, "d", &[("user", "u0")]),
        ],
        vec![
            log_record("beta", 0, "e", &[("user", "u1")]),
            log_record("beta", 1, "f", &[("user", "u0")]),
            log_record("beta", 2, "g", &[("user", "u2")]),
        ],
    ]
}

/// The worker applies the request's erasure predicates before it streams, so
/// an erased record never crosses the boundary: of seven records, exactly the
/// four survivors are decoded, and the transport pulled exactly those four
/// record frames plus one summary per slice. The baseline run without erasure
/// proves the fixture's erased records really are served otherwise.
///
/// Mutation proof: RED when `run_slice_logs` (service.rs) builds its query
/// without `.with_erasure(erasure)`: seven records cross and both counts fail.
#[test]
fn worker_erases_log_records_before_the_wire() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let [first, second] = erasure_log_corpus();
        let segments = vec![
            write_log_records(&store, 1, 0, &first).await,
            write_log_records(&store, 2, 1, &second).await,
        ];
        let snapshot = snapshot_of(&segments);

        let run = |erasure: Vec<ErasurePredicate>| {
            let store = Arc::clone(&store);
            let segments = segments.clone();
            let snapshot = snapshot.clone();
            async move {
                let (fetcher, server, produced) =
                    spawn_slice_worker(store, segments, true, true).await;
                let crossed = fetcher.crossed();
                let got = distributed_logs(
                    Arc::new(fetcher),
                    &snapshot,
                    &erasure,
                    2,
                    &QueryAccounting::new(),
                )
                .await
                .expect("distributed log fetch")
                .expect("served");
                server.abort();
                let crossed: usize = crossed_now(&crossed).iter().map(|s| s.records).sum();
                (
                    got,
                    crossed,
                    produced.load(std::sync::atomic::Ordering::SeqCst),
                )
            }
        };

        let (all, crossed, produced) = run(Vec::new()).await;
        assert_eq!((all.len(), crossed, produced), (7, 7, 7 + 2));

        let erasure = user_erasure("u0");
        let (mut expected, _) = local_log_records(&store, &segments, &erasure).await;
        documented_log_order(&mut expected);
        let (got, crossed, produced) = run(erasure).await;
        assert_same_sequence("erased fetch", &log_wire(&got), &log_wire(&expected));
        assert!(
            got.iter().all(|r| !r
                .attrs
                .contains(&("user".to_string(), AttrValue::Str("u0".to_string())))),
            "no erased record is in the result"
        );
        assert_eq!(crossed, 4, "only the four survivors were decoded");
        assert_eq!(
            produced,
            4 + 2,
            "the transport carried four record frames and two summaries"
        );
    });
}

/// The span sibling of [`worker_erases_log_records_before_the_wire`].
///
/// Mutation proof: RED when the erasure `retain` in `run_slice_spans`
/// (service.rs) is deleted.
#[test]
fn worker_erases_spans_before_the_wire() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        let first = vec![
            span(0, 1, None, "op", 0, 1, 0, None, Some("u0")),
            span(0, 2, None, "op", 1, 2, 0, None, Some("u1")),
            span(0, 3, None, "op", 2, 3, 0, Some("svc"), None),
            span(0, 4, None, "op", 3, 4, 0, None, Some("u0")),
        ];
        let second = vec![
            span(1, 1, None, "op", 0, 1, 0, None, Some("u1")),
            span(1, 2, None, "op", 1, 2, 0, None, Some("u0")),
            span(1, 3, None, "op", 2, 3, 0, None, Some("u2")),
        ];
        let segments = vec![
            write_span_records(&store, 1, 0, &first).await,
            write_span_records(&store, 2, 1, &second).await,
        ];
        let snapshot = snapshot_of(&segments);

        let run = |erasure: Vec<ErasurePredicate>| {
            let store = Arc::clone(&store);
            let segments = segments.clone();
            let snapshot = snapshot.clone();
            async move {
                let (fetcher, server, produced) =
                    spawn_slice_worker(store, segments, true, true).await;
                let crossed = fetcher.crossed();
                let got = distributed_spans(
                    Arc::new(fetcher),
                    &snapshot,
                    &erasure,
                    2,
                    &QueryAccounting::new(),
                )
                .await
                .expect("distributed span fetch")
                .expect("served");
                server.abort();
                let crossed: usize = crossed_now(&crossed).iter().map(|s| s.records).sum();
                (
                    got,
                    crossed,
                    produced.load(std::sync::atomic::Ordering::SeqCst),
                )
            }
        };

        let (all, crossed, produced) = run(Vec::new()).await;
        assert_eq!((all.len(), crossed, produced), (7, 7, 7 + 2));

        let erasure = user_erasure("u0");
        let (mut expected, _) = local_span_rows(&store, &segments, &erasure).await;
        documented_span_order(&mut expected);
        let (got, crossed, produced) = run(erasure).await;
        assert_same_sequence("erased fetch", &span_wire(&got), &span_wire(&expected));
        assert!(
            got.iter().all(|r| !r
                .record
                .attrs
                .contains(&("user".to_string(), "u0".to_string()))),
            "no erased span is in the result"
        );
        assert_eq!(crossed, 4, "only the four survivors were decoded");
        assert_eq!(produced, 4 + 2);
    });
}

/// Routes each slice to a worker by the shard of its first pinned segment and
/// makes `first_shard`'s slice complete before any other, so a test fixes the
/// order in which the coordinator sees two slice outcomes. Records each slice's
/// status in completion order.
struct OrderedRouter {
    workers: Vec<(u32, LoopbackSliceFetcher)>,
    first_shard: u32,
    first_done: tokio::sync::Notify,
    arrivals: std::sync::Mutex<Vec<pb::status::Code>>,
}

impl OrderedRouter {
    fn route(&self, request: &pb::FetchRequest) -> (u32, &LoopbackSliceFetcher) {
        let shard = match &request.scope {
            Some(pb::fetch_request::Scope::Pinned(pinned)) => pinned.segments[0].shard,
            other => panic!("a fan-out slice is pinned, got {other:?}"),
        };
        let worker = self
            .workers
            .iter()
            .find(|(s, _)| *s == shard)
            .map(|(_, w)| w)
            .expect("a worker for every shard");
        (shard, worker)
    }

    async fn wait_turn(&self, shard: u32) {
        if shard != self.first_shard {
            self.first_done.notified().await;
        }
    }

    fn arrived(&self, shard: u32, status: pb::status::Code) {
        self.arrivals.lock().expect("arrivals").push(status);
        if shard == self.first_shard {
            self.first_done.notify_one();
        }
    }
}

#[async_trait::async_trait]
impl SliceFetcher for OrderedRouter {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        Err(DistribError::Transport(
            "metrics are not routed here".to_string(),
        ))
    }

    async fn fetch_logs(
        &self,
        request: pb::FetchRequest,
    ) -> Result<SliceLogResponse, DistribError> {
        let (shard, worker) = self.route(&request);
        self.wait_turn(shard).await;
        let response = worker.fetch_logs(request).await?;
        self.arrived(shard, response.status);
        Ok(response)
    }

    async fn fetch_spans(
        &self,
        request: pb::FetchRequest,
    ) -> Result<SliceSpanResponse, DistribError> {
        let (shard, worker) = self.route(&request);
        self.wait_turn(shard).await;
        let response = worker.fetch_spans(request).await?;
        self.arrived(shard, response.status);
        Ok(response)
    }
}

/// One fan-out of `signal` over two slices: shard 0 goes to a worker with no
/// log or span fetch path (`Unsupported`), shard 1 to a worker whose resolver
/// knows no segment (`SnapshotInvalidated`), with `first_shard`'s slice landing
/// first. Returns whether the fetch was served, fell back, or failed, plus the
/// slice statuses in the order the coordinator saw them.
async fn precedence_outcome(
    signal: Signal,
    first_shard: u32,
) -> (Result<Option<usize>, QueryError>, Vec<pb::status::Code>) {
    let store = Arc::new(MemoryStore::new());
    let segments = match signal {
        Signal::Logs => vec![
            write_log_records(&store, 1, 0, &[log_record("alpha", 1, "x", &[])]).await,
            write_log_records(&store, 2, 1, &[log_record("beta", 2, "y", &[])]).await,
        ],
        _ => vec![
            write_span_records(&store, 1, 0, &[span(0, 1, None, "op", 1, 2, 0, None, None)]).await,
            write_span_records(&store, 2, 1, &[span(1, 1, None, "op", 1, 2, 0, None, None)]).await,
        ],
    };
    let (unsupported, unsupported_server, _) =
        spawn_slice_worker(Arc::clone(&store), segments.clone(), false, false).await;
    let (invalidated, invalidated_server, _) =
        spawn_slice_worker(Arc::clone(&store), Vec::new(), true, true).await;
    let router = Arc::new(OrderedRouter {
        workers: vec![(0, unsupported), (1, invalidated)],
        first_shard,
        first_done: tokio::sync::Notify::new(),
        arrivals: std::sync::Mutex::new(Vec::new()),
    });
    let snapshot = snapshot_of(&segments);
    let accounting = QueryAccounting::new();
    let outcome = match signal {
        Signal::Logs => distributed_logs(router.clone(), &snapshot, &[], 2, &accounting)
            .await
            .map(|r| r.map(|v| v.len())),
        _ => distributed_spans(router.clone(), &snapshot, &[], 2, &accounting)
            .await
            .map(|r| r.map(|v| v.len())),
    };
    unsupported_server.abort();
    invalidated_server.abort();
    let arrivals = router.arrivals.lock().expect("arrivals").clone();
    (outcome, arrivals)
}

/// An invalidated slice outranks an `Unsupported` one whichever lands first:
/// the query re-resolves (`Store { NotFound }`) rather than falling back to a
/// local read of a snapshot that is already stale. Both orders are driven over
/// real workers, and the recorded arrivals prove each order really happened.
///
/// Mutation proof: RED when the `if invalidated` and `if unsupported` blocks
/// after the collect loop of `Distributed::fetch_logs` or `fetch_spans`
/// (mod.rs) swap places, and when the `Unsupported` arm of `fetch_logs` returns
/// `Ok(None)` at once instead of setting its flag: the fetch then falls back in
/// both orders.
#[test]
fn an_invalidated_record_slice_outranks_an_unsupported_one_in_either_order() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        use pb::status::Code::{SnapshotInvalidated, Unsupported};
        let mut wrong = Vec::new();
        for signal in [Signal::Logs, Signal::Spans] {
            for (first_shard, order) in [
                (0, vec![Unsupported, SnapshotInvalidated]),
                (1, vec![SnapshotInvalidated, Unsupported]),
            ] {
                let (outcome, arrivals) = precedence_outcome(signal, first_shard).await;
                // A coordinator may stop reading after the first slice, so only
                // the first arrival is the precondition; the outcome is the claim.
                assert_eq!(
                    arrivals.first(),
                    order.first(),
                    "{signal:?}: the first slice landed as arranged"
                );
                if arrivals != order {
                    wrong.push(format!("{signal:?} {order:?}: slices landed {arrivals:?}"));
                }
                if !matches!(
                    outcome,
                    Err(QueryError::Fetch(crate::fetcher::FetchError::Store {
                        source: ravel_object_store::StoreError::NotFound,
                        ..
                    }))
                ) {
                    wrong.push(format!("{signal:?} {order:?}: {outcome:?}"));
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "an invalidated slice must re-resolve the query:\n{}",
            wrong.join("\n")
        );
    });
}

/// Record frames per flood slice, and the size of each record's payload. Each
/// frame is about 16 KiB on the wire, so the HTTP/2 windows hold a bounded
/// number of them ahead of the reader.
const FLOOD_RECORDS: usize = 1024;
const FLOOD_PAYLOAD: usize = 16 * 1024;
/// How many frames past the cap the worker's transport may send before flow
/// control holds it: the client's 2 MiB stream window plus the server's send
/// buffer is about 150 of these frames. A constant, far below `FLOOD_RECORDS`.
/// The worker builds every frame of the slice up front; this bounds only how
/// many of them the transport pulled.
const FLOOD_MAX_OVERRUN: usize = 384;

/// One segment of [`FLOOD_RECORDS`] records of `signal`, and each record
/// frame's encoded length in the order the worker emits them.
async fn flood_segment(store: &Arc<MemoryStore>, signal: Signal) -> (SegmentRef, Vec<u64>) {
    use prost::Message;
    let payload = "x".repeat(FLOOD_PAYLOAD);
    if signal == Signal::Logs {
        let records: Vec<LogRecord> = (0..FLOOD_RECORDS as i64)
            .map(|i| log_record("flood", i, &format!("{i:06}{payload}"), &[]))
            .collect();
        let seg = write_log_records(store, 1, 0, &records).await;
        let (local, _) = local_log_records(store, std::slice::from_ref(&seg), &[]).await;
        let lens = local
            .iter()
            .map(|r| {
                pb::FetchResponse {
                    frame: Some(pb::fetch_response::Frame::LogRecord(
                        crate::distrib::codec::encode_log_record(r),
                    )),
                }
                .encoded_len() as u64
            })
            .collect();
        (seg, lens)
    } else {
        let records: Vec<SpanRecord> = (0..FLOOD_RECORDS as i64)
            .map(|i| SpanRecord {
                trace_id: (i as u128).to_be_bytes(),
                name: format!("{i:06}{payload}"),
                ..span(0, 1, None, "", i, i + 1, 0, None, None)
            })
            .collect();
        let seg = write_span_records(store, 1, 0, &records).await;
        let (local, _) = local_span_rows(store, std::slice::from_ref(&seg), &[]).await;
        let lens = local
            .iter()
            .map(|r| {
                pb::FetchResponse {
                    frame: Some(pb::fetch_response::Frame::Span(
                        crate::distrib::codec::encode_span_frame(r),
                    )),
                }
                .encoded_len() as u64
            })
            .collect();
        (seg, lens)
    }
}

/// Fans `signal` out over `seg` through a loopback fetcher carrying the given
/// caps, expects a refusal, and returns it with the number of frames the
/// worker's transport had sent when the refusal returned.
async fn refused_at_cap(
    store: &Arc<MemoryStore>,
    seg: &SegmentRef,
    signal: Signal,
    max_frames: Option<usize>,
    max_bytes: Option<u64>,
) -> (QueryError, usize) {
    let (mut fetcher, server, produced) =
        spawn_slice_worker(Arc::clone(store), vec![seg.clone()], true, true).await;
    if let Some(max_frames) = max_frames {
        fetcher = fetcher.with_max_frames(max_frames);
    }
    if let Some(max_bytes) = max_bytes {
        fetcher = fetcher.with_max_bytes(max_bytes);
    }
    let snapshot = snapshot_of(std::slice::from_ref(seg));
    let accounting = QueryAccounting::new();
    let err = match signal {
        Signal::Logs => distributed_logs(Arc::new(fetcher), &snapshot, &[], 1, &accounting)
            .await
            .map(|_| ())
            .expect_err("a slice past the cap is refused"),
        _ => distributed_spans(Arc::new(fetcher), &snapshot, &[], 1, &accounting)
            .await
            .map(|_| ())
            .expect_err("a slice past the cap is refused"),
    };
    let produced = produced.load(std::sync::atomic::Ordering::SeqCst);
    server.abort();
    (err, produced)
}

/// The frame cap refuses a log or span slice on the first frame past it, naming
/// the exact counts in the budget-class error the coordinator maps it to, and
/// the reader stops there: the worker's transport sent a bounded overrun of
/// frames past the cap, not the whole slice. The worker built every frame
/// before sending; what is bounded is the frames sent.
///
/// Mutation proof: RED when `SliceCaps::admit` trips at `max_frames + 1`
/// instead of `max_frames` (the error names 66 frames, not 65), and when the
/// fetcher's read loop drains the stream before pushing frames (the transport
/// sends all 1025 frames).
#[test]
fn record_slice_frame_cap_refuses_at_the_cap_and_bounds_the_frames_sent() {
    const CAP: usize = 64;
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        for signal in [Signal::Logs, Signal::Spans] {
            let store = Arc::new(MemoryStore::new());
            let (seg, _lens) = flood_segment(&store, signal).await;
            let (err, produced) = refused_at_cap(&store, &seg, signal, Some(CAP), None).await;
            match err {
                QueryError::TooManySliceFrames { frames, max } => assert_eq!(
                    (frames, max),
                    (CAP + 1, CAP),
                    "{signal:?}: the refusal trips on the first frame past the cap"
                ),
                other => panic!("{signal:?}: expected a frame-cap refusal, got {other:?}"),
            }
            assert!(
                produced > CAP && produced <= CAP + 1 + FLOOD_MAX_OVERRUN,
                "{signal:?}: the frames sent stayed near the cap, sent {produced}"
            );
            assert!(
                produced < FLOOD_RECORDS + 1,
                "{signal:?}: the transport did not send the whole slice, sent {produced}"
            );
        }
    });
}

/// The byte cap refuses on the first frame whose bytes cross it, naming the
/// exact byte count. The cap sits one byte below the eighth frame's running
/// total, so the eighth frame is the first one past it by exactly one byte, and
/// the reader stops there: the frames the worker's transport sent stay within
/// a bounded overrun of the eighth.
///
/// Mutation proof: RED when `SliceCaps::admit` tests `bytes > max_bytes + 1`
/// (the eighth frame no longer trips and the error names nine frames' bytes),
/// and when the fetcher's read loop drains the stream before pushing frames.
#[test]
fn record_slice_byte_cap_refuses_at_the_cap_and_bounds_the_frames_sent() {
    const TRIP_FRAME: usize = 8;
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        for signal in [Signal::Logs, Signal::Spans] {
            let store = Arc::new(MemoryStore::new());
            let (seg, lens) = flood_segment(&store, signal).await;
            let crossing: u64 = lens[..TRIP_FRAME].iter().sum();
            let cap = crossing - 1;
            let (err, produced) = refused_at_cap(&store, &seg, signal, None, Some(cap)).await;
            match err {
                QueryError::TooManySliceBytes { bytes, max } => assert_eq!(
                    (bytes, max),
                    (crossing, cap),
                    "{signal:?}: the refusal names the bytes through the first frame past the cap"
                ),
                other => panic!("{signal:?}: expected a byte-cap refusal, got {other:?}"),
            }
            assert!(
                (TRIP_FRAME..=TRIP_FRAME + FLOOD_MAX_OVERRUN).contains(&produced),
                "{signal:?}: the frames sent stayed near the cap, sent {produced}"
            );
            assert!(
                produced < FLOOD_RECORDS + 1,
                "{signal:?}: the transport did not send the whole slice, sent {produced}"
            );
        }
    });
}

/// A store whose GETs of one key never complete. Reaching that GET is the
/// moment the test's query deadline passes, so a slice is stopped exactly
/// after its earlier segments were paid for and before the stalled one is.
struct StallStore {
    inner: Arc<MemoryStore>,
    stall_key: String,
    deadline: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for StallStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(
        &self,
        key: &str,
        range: ravel_object_store::GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        if key == self.stall_key {
            self.deadline.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, ravel_object_store::StoreError>
    {
        self.inner.put_multipart(key).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        page: Option<ravel_object_store::PageToken>,
    ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(
        &self,
        prefix: &str,
    ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> ravel_object_store::Capabilities {
        self.inner.capabilities()
    }
}

/// A worker that stops a slice when its query's deadline passes, in the shape
/// of the server's fragment service (`run_until_deadline` in
/// `services/ravel-server/src/distrib.rs`): the run is dropped and the slice
/// ends in-band with [`expired_slice_summary`] carrying what the run had spent.
/// The deadline is `deadline` firing.
///
/// [`expired_slice_summary`]: crate::distrib::service::expired_slice_summary
struct DeadlineWorker {
    store: Arc<dyn ObjectStoreBackend>,
    segments: Vec<SegmentRef>,
    deadline: Arc<tokio::sync::Notify>,
}

#[tonic::async_trait]
impl SeriesFetch for DeadlineWorker {
    type FetchStream = WorkerStream;

    async fn fetch(
        &self,
        request: tonic::Request<pb::FetchRequest>,
    ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
        let spent = QueryAccounting::new();
        let service = SeriesFetchService::new(
            SegmentFetcher::new(Arc::clone(&self.store)),
            Arc::new(SnapshotSegmentResolver::new(self.segments.clone())),
        )
        .with_log_fetcher(LogSegmentFetcher::new(Arc::clone(&self.store)))
        .with_span_fetcher(SpanSegmentFetcher::new(Arc::clone(&self.store)))
        .with_slice_accounting(spent.clone());
        let stream: WorkerStream = tokio::select! {
            biased;
            () = self.deadline.notified() => {
                let summary = crate::distrib::service::expired_slice_summary(
                    &spent.snapshot(),
                    "slice stopped: the query's deadline passed while it ran".to_string(),
                );
                Box::pin(futures::stream::iter([Ok(summary)]))
            }
            served = SeriesFetch::fetch(&service, request) => served?.into_inner(),
        };
        Ok(tonic::Response::new(stream))
    }
}

/// A log or span slice whose deadline passes after its first segment was
/// fetched and while its second is in flight ends in the expired `TIMEOUT`
/// summary, and the coordinator fails the query with `DeadlineExceeded` naming
/// the request's own deadline, after folding exactly what the first segment
/// cost. No record crosses.
///
/// What this covers is the coordinator: the `Timeout` arm of the log and span
/// fan-out and the fold of the expired slice's spend. It runs no server code:
/// [`DeadlineWorker`] stands in for the server's fragment service, which never
/// serves a log or span slice, because ravel-server's `build_resolver`
/// (`services/ravel-server/src/distrib.rs`) returns `None` for every signal but
/// metrics.
///
/// Mutation proof: RED when the `Timeout` arm of `Distributed::fetch_logs` or
/// `fetch_spans` (mod.rs) is deleted, sending the slice to the catch-all
/// `Distrib` error, and when `expired_slice_summary` (service.rs) reports any
/// status but `Timeout`.
#[test]
fn a_record_slice_stopped_at_its_deadline_fails_the_query_with_its_spend() {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        for signal in [Signal::Logs, Signal::Spans] {
            let store = Arc::new(MemoryStore::new());
            let (first, second) = if signal == Signal::Logs {
                let records: Vec<LogRecord> = (0..4)
                    .map(|i| log_record("alpha", i, "line", &[]))
                    .collect();
                (
                    write_log_records(&store, 1, 0, &records).await,
                    write_log_records(&store, 2, 0, &records).await,
                )
            } else {
                let records: Vec<SpanRecord> = (0..4u8)
                    .map(|i| span(0, i + 1, None, "op", i64::from(i), 9, 0, None, None))
                    .collect();
                (
                    write_span_records(&store, 1, 0, &records).await,
                    write_span_records(&store, 2, 0, &records).await,
                )
            };
            let oracle = if signal == Signal::Logs {
                local_log_records(&store, std::slice::from_ref(&first), &[])
                    .await
                    .1
            } else {
                local_span_rows(&store, std::slice::from_ref(&first), &[])
                    .await
                    .1
            };
            let oracle = wire_visible(oracle);
            assert!(oracle.total_s3_bytes() > 0, "the first segment costs bytes");

            let deadline = Arc::new(tokio::sync::Notify::new());
            let stalling: Arc<dyn ObjectStoreBackend> = Arc::new(StallStore {
                inner: Arc::clone(&store),
                stall_key: second.data_object_key.clone(),
                deadline: Arc::clone(&deadline),
            });
            let segments = vec![first, second];
            let (channel, server) = serve_loopback(DeadlineWorker {
                store: stalling,
                segments: segments.clone(),
                deadline,
            })
            .await;
            let fetcher = LoopbackSliceFetcher::new(channel);
            let crossed = fetcher.crossed();
            let snapshot = snapshot_of(&segments);
            let accounting = QueryAccounting::new();
            let err = match signal {
                Signal::Logs => distributed_logs(Arc::new(fetcher), &snapshot, &[], 1, &accounting)
                    .await
                    .map(|_| ()),
                _ => distributed_spans(Arc::new(fetcher), &snapshot, &[], 1, &accounting)
                    .await
                    .map(|_| ()),
            }
            .expect_err("a slice stopped at its deadline fails the query");
            server.abort();

            assert!(
                matches!(
                    err,
                    QueryError::DeadlineExceeded { deadline } if deadline == test_deadline().request
                ),
                "{signal:?}: got {err:?}"
            );
            assert_eq!(
                crossed_now(&crossed),
                vec![CrossedSlice {
                    status: pb::status::Code::Timeout,
                    records: 0,
                    returned: 0,
                }],
                "{signal:?}: the expired summary ended the slice and no record crossed"
            );
            assert_eq!(
                accounting.snapshot(),
                oracle,
                "{signal:?}: the query reports exactly the first segment's cost"
            );
        }
    });
}

/// The record decoders refuse a frame of any other signal, an empty frame and a
/// second summary with the typed errors the metrics decoder uses, and a slice
/// without a summary does not finish.
///
/// Mutation proof: RED when the log decoder's `Series` arm accepts the frame
/// instead of refusing it, or when `accept_summary` lets a second summary
/// replace the first.
#[test]
fn record_decoders_refuse_foreign_frames_and_need_one_summary() {
    use pb::fetch_response::Frame;
    let frame = |f: Frame| pb::FetchResponse { frame: Some(f) };
    let summary = || frame(Frame::Summary(pb::Summary::default()));

    let refusal = |err: DistribError| match err {
        DistribError::FrameSignalUnsupported(kind) => kind,
        other => panic!("expected a typed signal refusal, got {other:?}"),
    };
    let mut logs = LogSliceStreamDecoder::new();
    assert_eq!(
        refusal(
            logs.push(frame(Frame::Series(pb::SeriesFrame::default())))
                .expect_err("series")
        ),
        "series"
    );
    assert_eq!(
        refusal(
            logs.push(frame(Frame::Span(pb::SpanFrame::default())))
                .expect_err("span")
        ),
        "span"
    );
    let mut spans = SpanSliceStreamDecoder::new();
    assert_eq!(
        refusal(
            spans
                .push(frame(Frame::LogRecord(pb::LogRecordFrame::default())))
                .expect_err("log record")
        ),
        "log-record"
    );
    assert_eq!(
        refusal(
            spans
                .push(frame(Frame::Series(pb::SeriesFrame::default())))
                .expect_err("series")
        ),
        "series"
    );
    assert!(matches!(
        spans.push(pb::FetchResponse { frame: None }),
        Err(DistribError::EmptyFrame)
    ));

    assert!(matches!(
        LogSliceStreamDecoder::new().finish(),
        Err(DistribError::NoSummary)
    ));
    assert!(matches!(
        SpanSliceStreamDecoder::new().finish(),
        Err(DistribError::NoSummary)
    ));
    let mut logs = LogSliceStreamDecoder::new();
    logs.push(summary()).expect("one summary");
    assert!(logs.finished(), "the summary ends the slice");
    assert!(matches!(
        logs.push(summary()),
        Err(DistribError::MultipleSummaries)
    ));
}
