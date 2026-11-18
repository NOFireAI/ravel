//! ADR-4146 W1: the metrics series index the fold builds behind
//! `metrics_series_index`.

use super::*;

/// The fixture's second metric name.
const HTTP_REQUESTS: &str = "http_requests";

/// One series of the fixture: its metric name and labels besides
/// `__name__`.
type SeriesSpec = (&'static str, &'static [(&'static str, &'static str)]);

const A: SeriesSpec = (CPU_SECONDS, &[("instance", "i0"), ("job", "api")]);
const B: SeriesSpec = (CPU_SECONDS, &[("instance", "i1"), ("job", "api")]);
const C: SeriesSpec = (HTTP_REQUESTS, &[("instance", "i0"), ("job", "db")]);
const D: SeriesSpec = (HTTP_REQUESTS, &[("instance", "i2"), ("job", "db")]);
const E: SeriesSpec = (CPU_SECONDS, &[("instance", "i3"), ("job", "api")]);
const F: SeriesSpec = (CPU_SECONDS, &[("instance", "i1"), ("job", "web")]);

/// The fixture's objects, `(writer_seq, shard, series)`, all in
/// [`KIDX_HOUR`]: shard 0 holds A, B, E and F, shard 1 holds C and D. B and
/// F are held by the same two objects, so they share a class.
const OBJECTS: [(u64, u32, &[SeriesSpec]); 6] = [
    (1, 0, &[A, B, F]),
    (2, 0, &[A, B, F]),
    (3, 1, &[C, D]),
    (4, 1, &[C, D]),
    (5, 1, &[C]),
    (6, 0, &[A, E]),
];

/// Samples per series: enough data bytes that the fixture's series leaves
/// stay within the default 1 % cap.
const SAMPLES: usize = 2_000;

fn series_tenant_id() -> TenantId {
    TenantId::new("metrics-series-index")
}

fn series_id_of(spec: &SeriesSpec) -> SeriesId {
    SeriesId::compute(&series_tenant_id(), spec.0, &label_set_of(spec)).expect("series id")
}

fn label_set_of(spec: &SeriesSpec) -> LabelSet {
    let mut all = vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: spec.0.to_string(),
    }];
    all.extend(spec.1.iter().map(|(name, value)| Label {
        name: (*name).to_string(),
        value: (*value).to_string(),
    }));
    LabelSet::new(all).expect("valid labels")
}

/// Publishes one real RSEG object into `hour` on `shard` holding `series`,
/// each with `samples` pseudo-random samples.
async fn publish_series_object(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    writer_seq: u64,
    hour: u32,
    series: &[SeriesSpec],
    samples: usize,
) -> CommitRecord {
    let base_ts = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
    let mut state = writer_seq.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut inputs = Vec::with_capacity(series.len());
    for spec in series {
        let mut points = Vec::with_capacity(samples);
        for i in 0..samples {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            points.push(Sample {
                ts_ns: base_ts + i as i64 * 1_000_000,
                value: (state >> 11) as f64 / (1u64 << 53) as f64,
            });
        }
        inputs.push(SeriesInput {
            series_id: series_id_of(spec),
            labels: label_set_of(spec),
            samples: points,
        });
    }
    let writer_id = Uuid::from_u128(4_146_000 + u128::from(writer_seq));
    let min_ingest_ts_ns = base_ts;
    let max_ingest_ts_ns = base_ts + samples as i64 * 1_000_000;
    let written = SegmentWriter::write(
        inputs,
        SegmentIdentity {
            tenant_hash: tenant().0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq,
        },
        IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        },
    )
    .expect("write segment");
    let bytes = written.bytes.to_vec();
    let record = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns,
        max_ingest_ts_ns,
        segment_format_version: 1,
        created_unix_ns: base_ts,
        ingest_hour_bucket: hour,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(bytes))
        .await
        .expect("put data object");
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .expect("publish");
    record
}

async fn publish_series_fixture(store: &dyn ObjectStoreBackend) -> Vec<CommitRecord> {
    let mut records = Vec::new();
    for (seq, shard, series) in OBJECTS {
        records.push(publish_series_object(store, shard, seq, KIDX_HOUR, series, SAMPLES).await);
    }
    records
}

/// Two shards, the leaf build and the label index on, the series index as
/// `series_index` says.
fn series_config(series_index: bool) -> CatalogConfig {
    CatalogConfig {
        build_key_index_leaves: true,
        metrics_label_index: true,
        ..config(2)
    }
}

/// GOLDEN CAPTURE, run once on the code before the series index existed.
#[tokio::test]
async fn capture_golden_label_value_fold() {
    let store = recording_store();
    let records = publish_series_fixture(store.as_ref()).await;
    let catalog = Catalog::new(store.clone(), series_config(false)).expect("catalog");
    let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
    let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
    println!("entries {}", report.entry_count);
    println!(
        "data bytes {}",
        records.iter().map(|r| r.object_size).sum::<u64>()
    );
    for part in &head.parts {
        println!("part {} {}", hex_encode(&part.blake3), part.entry_count);
        for leaf in &part.key_index {
            println!(
                "leaf {} {} {} {}",
                leaf.field,
                hex_encode(&leaf.blake3),
                leaf.size,
                leaf.key
            );
        }
    }
}
