//! A pending selective-erasure request must not change which duplicate wins
//! at a timestamp it does not erase (ADR-0064 decision 2, issue #2423).
//!
//! The query path masks a pending request's matching samples out of each
//! fetched run before the dedup merge. A run with no per-sample provenance
//! column derives the fourth element of every sample's dedup key from its
//! position in the run, so the mask must keep each survivor's original
//! position: otherwise dropping an early sample shifts every later one, and a
//! duplicate that ties another run on the run-wide triple resolves to a
//! different value while the request is pending than before it or after the
//! erasure rewrite.
//!
//! Each test publishes two L0 segments of one series that share the run-wide
//! triple `(created_unix_ns, writer_epoch, writer_seq)` (only the writer id
//! differs, which is not part of the key), so the in-run index decides the
//! contested timestamp:
//!
//! - segment A: `[EARLY, CONTESTED = A_LOW, CONTESTED = A_HIGH]`, indexes
//!   0, 1, 2 (a duplicate inside one write, kept in insertion order);
//! - segment B: `[B_EARLY, CONTESTED = B]`, indexes 0, 1.
//!
//! Without a pending request `A_HIGH` wins on index 2 over `B`'s index 1. The
//! request erases only `EARLY`. If the mask renumbered A's survivors, `A_HIGH`
//! would tie `B` at index 1 and the value tie-break (`B` is the greater bit
//! pattern) would serve `B` instead.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{erasure, keys, publish, record, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::Value;
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds, ResetHint,
    SegmentIdentity, SegmentWriter, SeriesInput, SeriesInputV3, SeriesValues, WrittenSegment,
};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
const CONTESTED: i64 = 1_000 * NS;
/// The only sample the pending request erases.
const EARLY: i64 = CONTESTED - 10 * NS;
/// Segment B's leading sample: outside the erasure window, so B's run is never
/// masked and its contested sample keeps index 1 either way.
const B_EARLY: i64 = CONTESTED - 20 * NS;
const METRIC: &str = "m";
const USER: &str = "u1";

/// Values chosen so the to_bits tie-break prefers `B` over `A_HIGH`, and the
/// index alone (not the value) decides when the positions are intact.
const EARLY_VALUE: f64 = 1.0;
const A_LOW: f64 = 1.5;
const A_HIGH: f64 = 2.0;
const B: f64 = 3.0;
const B_EARLY_VALUE: f64 = 4.0;

/// The run-wide triple both segments carry.
const CREATED_UNIX_NS: i64 = 42;
const WRITER_EPOCH: u64 = 1;
const WRITER_SEQ: u64 = 0;

fn label_set() -> LabelSet {
    LabelSet::new(vec![
        Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: METRIC.to_string(),
        },
        Label {
            name: "user_id".to_string(),
            value: USER.to_string(),
        },
    ])
    .expect("valid labels")
}

fn identity(tenant_hash: TenantHash, writer_id: Uuid) -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: WRITER_EPOCH,
        writer_seq: WRITER_SEQ,
    }
}

const NO_INGEST_BOUNDS: IngestBounds = IngestBounds {
    min_ingest_ts_ns: 0,
    max_ingest_ts_ns: 0,
};

/// Commits an already-written segment under `writer_id` with the shared
/// run-wide triple and returns its read-your-write token.
async fn publish_written(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    writer_id: Uuid,
    written: WrittenSegment,
) -> CommitToken {
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: WRITER_EPOCH,
        writer_seq: WRITER_SEQ,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: CREATED_UNIX_NS,
        ingest_hour_bucket: 0,
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish")
}

async fn publish_scalar(
    store: &MemoryStore,
    tenant_id: &TenantId,
    writer_id: Uuid,
    samples: &[(i64, f64)],
) -> CommitToken {
    let tenant_hash = tenant_id.hash();
    let labels = label_set();
    let written = SegmentWriter::write(
        vec![SeriesInput {
            series_id: SeriesId::compute(tenant_id, METRIC, &labels).expect("series id"),
            labels,
            samples: samples
                .iter()
                .map(|&(ts_ns, value)| Sample { ts_ns, value })
                .collect(),
        }],
        identity(tenant_hash, writer_id),
        NO_INGEST_BOUNDS,
    )
    .expect("write segment");
    publish_written(store, tenant_hash, writer_id, written).await
}

/// A single-bucket int histogram distinguished only by `sum`, so the
/// histogram tie-break orders these exactly as the scalar one orders the same
/// numbers.
fn histogram(sum: f64) -> HistogramValue {
    HistogramValue {
        scale: 0,
        zero_threshold: 0.0,
        sum: Some(sum),
        custom_values: None,
        positive_spans: vec![HistogramSpan {
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
    }
}

async fn publish_histogram(
    store: &MemoryStore,
    tenant_id: &TenantId,
    writer_id: Uuid,
    samples: &[(i64, f64)],
) -> CommitToken {
    let tenant_hash = tenant_id.hash();
    let labels = label_set();
    let written = SegmentWriter::write_histograms(
        vec![SeriesInputV3 {
            series_id: SeriesId::compute(tenant_id, METRIC, &labels).expect("series id"),
            labels,
            values: SeriesValues::Histogram(
                samples
                    .iter()
                    .map(|&(ts_ns, sum)| HistogramSample {
                        ts_ns,
                        value: histogram(sum),
                    })
                    .collect(),
            ),
        }],
        identity(tenant_hash, writer_id),
        NO_INGEST_BOUNDS,
    )
    .expect("write histogram segment");
    publish_written(store, tenant_hash, writer_id, written).await
}

/// A durable windowed request erasing `user_id = u1` at `EARLY` only.
async fn put_early_dreq(store: &MemoryStore, tenant_hash: TenantHash) {
    let request_id = Uuid::from_u128(0x2423);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 1,
        predicate: vec![ErasurePredicateMatcher {
            key: "user_id".to_string(),
            value: USER.to_string(),
        }],
        window_start_ns: EARLY,
        window_end_ns: EARLY + 1,
        reason: String::new(),
    };
    let key =
        keys::erasure_request_key(&tenant_hash, Signal::Metrics, request_id).expect("dreq key");
    store
        .put(
            &key,
            erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

/// Which representation of the sample an instant query's single element
/// carries, as the bit pattern a test compares.
#[derive(Clone, Copy)]
enum Kind {
    Scalar,
    Histogram,
}

/// The bits of the single element an instant query at `ts_ns` returns: the
/// float value for a scalar series, the histogram's `sum` for a native one.
async fn served_bits(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    tokens: &[CommitToken],
    ts_ns: i64,
    kind: Kind,
) -> u64 {
    let (value, _coverage) = engine
        .instant(
            tenant_hash,
            METRIC,
            ts_ns / 1_000_000,
            tokens,
            CONTESTED + NS,
            Duration::from_secs(30),
        )
        .await
        .expect("instant query");
    let Value::Vector(vector) = value else {
        panic!("instant query over a selector must return a vector");
    };
    assert_eq!(vector.len(), 1, "exactly the one series: {vector:?}");
    match kind {
        Kind::Scalar => {
            assert!(vector[0].histogram.is_none(), "a float sample");
            vector[0].value.to_bits()
        }
        Kind::Histogram => vector[0]
            .histogram
            .as_ref()
            .expect("a native histogram sample")
            .sum
            .to_bits(),
    }
}

async fn assert_contested_winner_survives_the_mask(kind: Kind) {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();
    let store = Arc::new(MemoryStore::new());
    let a_samples = [
        (EARLY, EARLY_VALUE),
        (CONTESTED, A_LOW),
        (CONTESTED, A_HIGH),
    ];
    let b_samples = [(B_EARLY, B_EARLY_VALUE), (CONTESTED, B)];
    let (writer_a, writer_b) = (Uuid::from_u128(7), Uuid::from_u128(8));
    let tokens = match kind {
        Kind::Scalar => [
            publish_scalar(&store, &tenant_id, writer_a, &a_samples).await,
            publish_scalar(&store, &tenant_id, writer_b, &b_samples).await,
        ],
        Kind::Histogram => [
            publish_histogram(&store, &tenant_id, writer_a, &a_samples).await,
            publish_histogram(&store, &tenant_id, writer_b, &b_samples).await,
        ],
    };
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, backend, EngineConfig::default());

    let before = served_bits(&engine, tenant_hash, &tokens, CONTESTED, kind).await;
    assert_eq!(
        before,
        A_HIGH.to_bits(),
        "baseline: in-run index 2 beats index 1 on the shared triple"
    );
    assert_eq!(
        served_bits(&engine, tenant_hash, &tokens, EARLY, kind).await,
        EARLY_VALUE.to_bits(),
        "baseline: the early sample is visible before the request"
    );

    put_early_dreq(&store, tenant_hash).await;

    // The request really applies: the early sample is gone and the lookback
    // falls through to segment B's older sample.
    assert_eq!(
        served_bits(&engine, tenant_hash, &tokens, EARLY, kind).await,
        B_EARLY_VALUE.to_bits(),
        "the pending request must exclude the early sample"
    );
    assert_eq!(
        served_bits(&engine, tenant_hash, &tokens, CONTESTED, kind).await,
        before,
        "a non-erased duplicate must resolve the same with the request pending"
    );
}

#[tokio::test]
async fn pending_erasure_keeps_scalar_dedup_positions() {
    assert_contested_winner_survives_the_mask(Kind::Scalar).await;
}

#[tokio::test]
async fn pending_erasure_keeps_histogram_dedup_positions() {
    assert_contested_winner_survives_the_mask(Kind::Histogram).await;
}
