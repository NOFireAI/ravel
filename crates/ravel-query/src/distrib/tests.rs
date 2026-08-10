//! Acceptance and coordinator-invariant tests for the ADR-0071 distributed
//! read fan-out (issue #864).
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
use ravel_types::accounting::QueryAccounting;
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
/// Each segment gets a unique `writer_seq`/`created_unix_ns` (from `seq`) so
/// the ADR-0010 cross-segment dedup total order is fully determined and the
/// merge result cannot depend on slice grouping.
async fn write_segment(
    store: &MemoryStore,
    seq: u64,
    shard: u32,
    hour_bucket: u32,
    descs: &[SeriesDesc],
) -> SegmentRef {
    let writer_id = Uuid::from_u128(u128::from(seq) + 1);
    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: seq,
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
    let key = format!("seg/{seq}.rseg");
    store
        .put(&key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put segment");
    SegmentRef {
        data_object_key: key,
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: hour_bucket,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard,
        content_hash: written.summary.blake3,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        // Unique per segment so the dedup priority prefix is a total order.
        created_unix_ns: seq as i64,
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
/// the local path would merge.
async fn local_scalar(
    store: Arc<MemoryStore>,
    snapshot: &Snapshot,
) -> Vec<Vec<crate::fetcher::FetchedSeriesSoa>> {
    let fetcher = SegmentFetcher::new(store);
    let accounting = QueryAccounting::new();
    let mut out = Vec::with_capacity(snapshot.segments.len());
    for seg in &snapshot.segments {
        let (scalar, _stats, _hist) = fetcher
            .fetch_soa_and_histograms_accounted(TENANT, seg, &[], &accounting)
            .await
            .expect("local fetch");
        out.push(scalar);
    }
    out
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

/// Runs one acceptance case: build the corpus, resolve it as a snapshot, fetch
/// it both locally and distributed over the loopback worker, and assert the two
/// coordinator-merged results are byte-identical.
async fn run_acceptance(
    segments_desc: Vec<(u32, u32, Vec<SeriesDesc>)>,
    max_parallel_slices: usize,
) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for (seq, (shard, hour, descs)) in segments_desc.into_iter().enumerate() {
        segments.push(write_segment(&store, seq as u64, shard, hour, &descs).await);
    }
    let snapshot = Snapshot {
        segments: segments.clone(),
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };

    // Local reference.
    let local_runs = local_scalar(Arc::clone(&store), &snapshot).await;
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
        )
        .await
        .expect("distributed fetch")
        .expect("distributed produced a result (not a fallback)");
    let distributed_merged = merge_soa_runs(triple.0, usize::MAX, usize::MAX).expect("dist merge");

    assert_series_bit_identical(&local_merged, &distributed_merged);
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
    (0u32..3, 100u32..104, series)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// ADR-0071 acceptance: coordinator-merged distributed fetch == local fetch,
    /// bit-for-bit, over a real loopback worker, for arbitrary corpora and
    /// arbitrary slice counts.
    #[test]
    fn distributed_merge_equals_local_bitwise(
        segments in prop::collection::vec(arb_segment(), 1..6),
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
