//! Metrics-scan batch-building throughput (ADR-0099 decision 7).
//!
//! Both benchmarks drain `RsegScanExec` itself over a snapshot held in a
//! `MemoryStore`, so what they time is fetch/decode plus the merge and batch
//! construction this ADR rewrote, with no object-store latency in the way.
//! The two fixtures pick the two batch-building paths:
//!
//! - `single_run_per_series`: one long run per series, so a batch sits
//!   contiguously inside one run and its `ts`/`value` columns are buffer
//!   slices adopted from the fetched SoA;
//! - `overlapping_runs`: each series written by three segments over
//!   interleaved timestamps, so every batch straddles runs and its values are
//!   gathered through the merge cursors.
//!
//! Throughput is reported in samples per second (`Throughput::Elements`).
//! Allocation counting lives in `tests/scan_batch_allocations.rs`, which needs
//! a one-test binary to keep the count deterministic.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::execution::TaskContext;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::RavelTableProvider;
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);

struct SeriesSpec {
    metric: String,
    samples: Vec<(i64, f64)>,
}

struct SegSpec {
    created_unix_ns: i64,
    writer_seq: u64,
    series: Vec<SeriesSpec>,
}

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn series_id_for(metric: &str) -> [u8; 16] {
    let tenant = TenantId::new("t".to_string());
    SeriesId::compute(&tenant, metric, &labels_for(metric))
        .expect("series id")
        .0
}

async fn build(specs: &[SegSpec]) -> (Arc<dyn ObjectStoreBackend>, Snapshot) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        let inputs: Vec<SeriesInput> = spec
            .series
            .iter()
            .map(|s| SeriesInput {
                series_id: SeriesId(series_id_for(&s.metric)),
                labels: labels_for(&s.metric),
                samples: s
                    .samples
                    .iter()
                    .map(|(ts_ns, value)| Sample {
                        ts_ns: *ts_ns,
                        value: *value,
                    })
                    .collect(),
            })
            .collect();
        let writer_id = Uuid::from_u128(1000 + i as u128);
        let identity = SegmentIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: spec.writer_seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        };
        let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");
        let key = format!("t/metrics/seg-{i}.rseg");
        store
            .put(&key, written.bytes.clone(), PutOptions::default())
            .await
            .expect("put segment");
        segments.push(SegmentRef {
            data_object_key: key,
            object_size: written.bytes.len() as u64,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            ingest_hour_bucket: 0,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            shard: 0,
            content_hash: written.summary.blake3,
            writer_id,
            writer_epoch: 1,
            writer_seq: spec.writer_seq,
            created_unix_ns: spec.created_unix_ns,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
            declared_column_stats: Default::default(),
        });
    }
    segments.sort_by_key(|s| (s.created_unix_ns, s.writer_epoch, s.writer_seq, s.shard));
    (
        store,
        Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
    )
}

/// Drain the scan node, returning the rows it emitted.
async fn drain(store: Arc<dyn ObjectStoreBackend>, snapshot: Snapshot) -> usize {
    let segments = snapshot.segments.clone();
    let provider = RavelTableProvider::new(
        snapshot,
        TENANT,
        SegmentFetcher::new(store),
        EngineConfig::default(),
        QueryAccounting::new(),
    );
    let fragment = provider.worker_fragment(1, &segments).expect("fragment");
    let children = fragment.children();
    let scan = Arc::clone(children.first().expect("scan under the merge"));
    let mut stream = scan
        .execute(0, Arc::new(TaskContext::default()))
        .expect("execute scan");
    let mut rows = 0;
    while let Some(next) = stream.next().await {
        rows += next.expect("batch").num_rows();
    }
    rows
}

/// Four series of 60,000 samples, one segment each: batches sit inside one run.
fn single_run_per_series() -> Vec<SegSpec> {
    (0..4)
        .map(|s| SegSpec {
            created_unix_ns: 10 + s,
            writer_seq: u64::try_from(s).expect("small") + 1,
            series: vec![SeriesSpec {
                metric: format!("m{s}"),
                samples: (0..60_000).map(|i| (i, i as f64 * 1.5)).collect(),
            }],
        })
        .collect()
}

/// Four series in three segments each, interleaved sample by sample: every
/// batch straddles runs.
fn overlapping_runs() -> Vec<SegSpec> {
    (0..3)
        .map(|s| SegSpec {
            created_unix_ns: 10 + s,
            writer_seq: u64::try_from(s).expect("small") + 1,
            series: (0..4)
                .map(|m| SeriesSpec {
                    metric: format!("m{m}"),
                    samples: (0..20_000).map(|i| (i * 3 + s, i as f64 + 0.5)).collect(),
                })
                .collect(),
        })
        .collect()
}

fn bench_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    for (name, specs) in [
        ("single_run_per_series", single_run_per_series()),
        ("overlapping_runs", overlapping_runs()),
    ] {
        let (store, snapshot) = rt.block_on(build(&specs));
        let rows = rt.block_on(drain(Arc::clone(&store), snapshot.clone()));

        let mut group = c.benchmark_group("samples_scan");
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                let emitted = rt.block_on(drain(Arc::clone(&store), snapshot.clone()));
                std::hint::black_box(emitted);
            });
        });
        group.finish();
    }
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
