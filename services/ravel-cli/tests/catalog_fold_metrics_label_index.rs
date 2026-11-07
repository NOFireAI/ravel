//! `ravel-cli catalog fold --metrics-label-index` (ADR-3694 decision 2): the
//! flag reaches the catalog the fold and the rebuild run with, the fold
//! refuses it without `--key-index-leaves`, and a metrics rebuild without it
//! is refused.
//!
//! Driven in-process against one `MemoryStore` per case, for the reason
//! `tests/catalog_signal.rs` gives.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

/// Clears the default seal margin whatever minute of the hour the test runs
/// in (the same constant as `tests/catalog_fold_report.rs`).
const SEALED_AGE_NS: i64 = 3 * 60 * 60 * 1_000_000_000;

const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

const TENANT: &str = "cli-fold-metrics-label-index";

fn now_ns() -> i64 {
    ravel_cli::now_ns().expect("system clock readable")
}

/// 4,000 samples ending at `end_ns` with xorshift values, which compress
/// poorly, so the object is large enough that its `label_value` leaf stays
/// under the default 1% byte cap.
fn random_samples(seed: u64, end_ns: i64) -> Vec<Sample> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..4_000i64)
        .map(|i| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            Sample {
                ts_ns: end_ns - (4_000 - i) * 1_000_000,
                value: f64::from_bits(x >> 12 | 0x3ff0_0000_0000_0000),
            }
        })
        .collect()
}

/// Publishes one real RSEG object, writer `writer_seq`, with one `up` series
/// labelled `region=<region>`.
async fn publish_metrics(store: &MemoryStore, writer_seq: u64, region: &str, created_unix_ns: i64) {
    let tenant_hash = TenantId::new(TENANT).hash();
    let ingest_hour_bucket = u32::try_from(created_unix_ns / 3_600_000_000_000).expect("fits u32");
    let writer_id = Uuid::from_u128(u128::from(writer_seq));
    let series = vec![SeriesInput {
        series_id: SeriesId([writer_seq as u8; 16]),
        labels: LabelSet::new(vec![
            Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: "up".to_string(),
            },
            Label {
                name: "region".to_string(),
                value: region.to_string(),
            },
        ])
        .expect("valid labels"),
        samples: random_samples(writer_seq, created_unix_ns),
    }];
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write RSEG");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

async fn publish_fixture(store: &MemoryStore) {
    let created = now_ns() - SEALED_AGE_NS;
    publish_metrics(store, 1, "east", created).await;
    publish_metrics(store, 2, "west", created).await;
}

async fn kidx_objects(store: &MemoryStore) -> usize {
    let prefix = format!("t/{}/", TenantId::new(TENANT).hash().to_hex());
    list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .filter(|meta| meta.key.ends_with(".kidx"))
        .count()
}

async fn fold_metrics(
    store: Arc<MemoryStore>,
    key_index_leaves: bool,
    metrics_label_index: bool,
) -> anyhow::Result<(ravel_catalog::FoldReport, String)> {
    catalog::fold_with_metrics_label_index(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        TENANT,
        1,
        SignalArg::Metrics,
        None,
        false,
        key_index_leaves,
        ravel_catalog::DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS,
        metrics_label_index,
        None,
        now_ns(),
        false,
    )
    .await
}

/// With `--key-index-leaves`, `--metrics-label-index` is what makes a
/// metrics fold build the `label_value` leaf: on, one leaf is written and the
/// pending erasure set is listed once; off, no leaf and no listing.
#[tokio::test]
async fn the_metrics_label_index_flag_reaches_the_fold() {
    let on = Arc::new(MemoryStore::new());
    publish_fixture(&on).await;
    let (report, printed) = fold_metrics(on.clone(), true, true)
        .await
        .expect("fold with the metrics leaf");
    assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
    assert_eq!(report.metrics_label_index_erasure_list_requests, 1);
    assert_eq!(report.metrics_label_index_parts_over_cap, 0);
    assert_eq!(report.metrics_label_index_parts_erasure_gated, 0);
    assert_eq!(kidx_objects(&on).await, 1);
    assert!(
        printed.contains("metrics_label_index_erasure_list_requests: 1\n"),
        "{printed}"
    );

    let off = Arc::new(MemoryStore::new());
    publish_fixture(&off).await;
    let (report, _) = fold_metrics(off.clone(), true, false)
        .await
        .expect("fold without the metrics leaf");
    assert_eq!(report.key_index_leaves_written, 0, "{report:?}");
    assert_eq!(report.metrics_label_index_erasure_list_requests, 0);
    assert_eq!(kidx_objects(&off).await, 0);
}

/// `--metrics-label-index` without `--key-index-leaves` would build nothing,
/// so the fold refuses it before reading or writing anything.
#[tokio::test]
async fn the_metrics_label_index_flag_is_refused_without_key_index_leaves() {
    let store = Arc::new(MemoryStore::new());
    publish_fixture(&store).await;
    let prefix = format!("t/{}/", TenantId::new(TENANT).hash().to_hex());
    let before = list_all(store.as_ref(), &prefix).await.expect("list").len();
    let err = fold_metrics(store.clone(), false, true)
        .await
        .expect_err("refused without --key-index-leaves");
    assert!(
        err.to_string()
            .contains("--metrics-label-index needs --key-index-leaves"),
        "{err}"
    );
    assert_eq!(
        list_all(store.as_ref(), &prefix).await.expect("list").len(),
        before,
        "the refused fold wrote nothing"
    );
}

/// `--rebuild-key-index --signal metrics` attaches the `label_value` leaf to
/// a part folded without it when `--metrics-label-index` is set, and is
/// refused without it.
#[tokio::test]
async fn the_metrics_label_index_flag_reaches_the_rebuild() {
    let store = Arc::new(MemoryStore::new());
    publish_fixture(&store).await;
    fold_metrics(store.clone(), false, false)
        .await
        .expect("fold without leaves");
    assert_eq!(kidx_objects(&store).await, 0);

    let (refused, _) = catalog::rebuild_key_index_outcome(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        TENANT,
        1,
        SignalArg::Metrics,
        None,
        250_000,
        false,
        false,
    )
    .await
    .expect("the rebuild ran");
    assert!(
        matches!(
            refused,
            Err(ravel_catalog::KeyIndexRebuildError::UnsupportedSignal(
                Signal::Metrics
            ))
        ),
        "{refused:?}"
    );
    assert_eq!(kidx_objects(&store).await, 0);

    let (report, printed) = catalog::rebuild_key_index(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        TENANT,
        1,
        SignalArg::Metrics,
        None,
        250_000,
        true,
        false,
    )
    .await
    .expect("rebuild with the metrics leaf");
    assert_eq!(report.fields, vec!["label_value".to_string()]);
    assert_eq!(report.parts_rebuilt, 1, "{report:?}");
    assert_eq!(report.erasure_list_requests, 1, "{report:?}");
    assert_eq!(kidx_objects(&store).await, 1);
    assert!(printed.contains("erasure_list_requests: 1\n"), "{printed}");
}
