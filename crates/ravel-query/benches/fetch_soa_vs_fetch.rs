//! End-to-end AoS (`SegmentFetcher::fetch`) vs SoA (`fetch_soa`) throughput
//! over a `MemoryStore`-backed segment, exercising both the `Bytes::slice`
//! fix and the SoA decode path together as the actual query-path caller
//! would (see `bytes_slice_vs_copy` for the `Bytes::slice` fix measured in
//! isolation). Self-contained: builds its own segment with `SegmentWriter`
//! rather than depending on `ravel-bench` (out of scope here).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_catalog::SegmentRef;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::SegmentFetcher;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantHash, TenantId};
use uuid::Uuid;

const SERIES_COUNT: usize = 2_000;
const SAMPLES_PER_SERIES: usize = 60;

async fn build_store() -> (Arc<MemoryStore>, TenantHash, SegmentRef) {
    let tenant_hash = TenantHash([9u8; 16]);
    let tenant_id = TenantId::new("bench-tenant".to_string());
    let writer_id = Uuid::from_u128(1);

    let mut series = Vec::with_capacity(SERIES_COUNT);
    for i in 0..SERIES_COUNT {
        let metric = format!("bench_metric_{i}");
        let labels = LabelSet::new(vec![
            Label {
                name: "__name__".to_string(),
                value: metric.clone(),
            },
            Label {
                name: "shard".to_string(),
                value: (i % 16).to_string(),
            },
        ])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, &metric, &labels).expect("series id");
        let samples = (0..SAMPLES_PER_SERIES)
            .map(|j| Sample {
                ts_ns: 1_000_000_000 * (j as i64),
                value: 100.0 + (j as f64) * 0.5 + (i as f64) * 0.001,
            })
            .collect();
        series.push(SeriesInput {
            series_id,
            labels,
            samples,
        });
    }

    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write segment");

    let store = Arc::new(MemoryStore::new());
    let key = "bench/segment.rseg";
    store
        .put(key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put segment object");

    let seg_ref = SegmentRef {
        data_object_key: key.to_string(),
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
        writer_seq: 1,
        created_unix_ns: 42,
        level: ravel_catalog::SegmentLevel::L0,
    };
    (store, tenant_hash, seg_ref)
}

fn bench_fetch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let (store, tenant_hash, seg_ref) = rt.block_on(build_store());
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(backend);
    let total_samples = (SERIES_COUNT * SAMPLES_PER_SERIES) as u64;

    let mut group = c.benchmark_group("fetch_soa_vs_fetch");
    group.throughput(Throughput::Elements(total_samples));
    group.sample_size(20);

    group.bench_function("fetch_aos", |b| {
        b.iter(|| {
            let out = rt
                .block_on(fetcher.fetch(tenant_hash, &seg_ref, &[]))
                .expect("fetch");
            let decoded: usize = out.iter().map(|s| s.samples.len()).sum();
            assert_eq!(decoded, total_samples as usize);
        });
    });

    group.bench_function("fetch_soa", |b| {
        b.iter(|| {
            let (out, _stats) = rt
                .block_on(fetcher.fetch_soa(tenant_hash, &seg_ref, &[]))
                .expect("fetch_soa");
            let decoded: usize = out.iter().map(|s| s.timestamps.len()).sum();
            assert_eq!(decoded, total_samples as usize);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_fetch);
criterion_main!(benches);
