//! End-to-end reachability test for the at-rest integrity scrubber (ADR-0059,
//! issue #694). This is the epic's designated proof that the capability is
//! actually constructed and reachable, not merely unit-tested in isolation: it
//! spins up a real `Mode::Maintain` `ravel_server::start` server (whose
//! `start` spawns the real `ScrubTask` on a real timer), injects a corrupted
//! data object into the shared store using the proven GET/flip/Overwrite
//! pattern (crates/ravel-failure-tests/tests/corruption.rs), waits for a real
//! scrub tick to run through that spawned task, and asserts the anomaly
//! surfaces on the real `GET /metrics` endpoint.
//!
//! The scrub task's cadence and period are not injectable through the server's
//! public surface, so this test uses a short `--scrub-period` (1s), which makes
//! the loop's derived tick 1s as well: the whole corpus is one budgeted slice,
//! scrubbed on the first tick. That is a property of the shipped lifecycle
//! (the loop reads `SystemClock` and its own configured period), not a bypass:
//! the anomaly travels the real ScrubTask -> ScrubMetrics -> /metrics path.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{Mode, ServerConfig};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Build a `Mode::Maintain` server config over `store` with the scrubber
/// running on a 1-second period (so a tick fires within the test's polling
/// window) and the maintenance loop left disabled, so only the scrubber acts.
fn maintain_config(mode: Mode, tenant: &TenantId) -> ServerConfig {
    ServerConfig {
        mode,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(Default::default(), false),
        mtls_listener: None,
        fold_tenants: vec![tenant.hash()],
        fold: ravel_server::FoldTaskConfig::default(),
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        scrub_period: Duration::from_secs(1),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
    }
}

/// Publish a real RSEG segment plus its commit record into `(tenant, Metrics,
/// shard 0)`, exactly as ingest would, and return the data-object key. An
/// unmodified object scrubs clean; the caller corrupts it to trigger the
/// anomaly.
async fn publish_segment(store: &MemoryStore, tenant: &TenantId) -> String {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(7);
    let created_unix_ns = 500_000 * NS_PER_HOUR;
    let ingest_hour_bucket = 500_000u32;
    let series: Vec<SeriesInput> = ["cpu", "mem"]
        .iter()
        .map(|metric| {
            let labels = LabelSet::new(vec![Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: (*metric).to_string(),
            }])
            .expect("valid labels");
            let series_id = SeriesId::compute(tenant, metric, &labels).expect("series id");
            SeriesInput {
                series_id,
                labels,
                samples: vec![Sample {
                    ts_ns: created_unix_ns,
                    value: 1.0,
                }],
            }
        })
        .collect();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let min_ingest_ts_ns = created_unix_ns - 1_000;
    let max_ingest_ts_ns = created_unix_ns;
    let bounds = IngestBounds {
        min_ingest_ts_ns,
        max_ingest_ts_ns,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns,
        max_ingest_ts_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish commit record");
    data_key
}

/// THE reachability test (ADR-0059 acceptance criterion): a corrupted data
/// object, injected at rest, is detected within one scrub cycle and surfaces on
/// `/metrics`, driven end to end through the real spawned `ScrubTask`.
#[tokio::test]
async fn injected_corruption_surfaces_on_metrics_through_the_real_scrub_task() {
    let tenant = TenantId::new("scrub-e2e");
    let store = Arc::new(MemoryStore::new());

    // Seed a real committed segment, then corrupt its data object at rest.
    let data_key = publish_segment(store.as_ref(), &tenant).await;
    let existing = store
        .get(&data_key, GetRange::Full)
        .await
        .expect("get object");
    let mut corrupted = existing.data.to_vec();
    corrupted[0] ^= 0x01; // page-region bit flip: caught by the content tier's blake3
    store
        .put(
            &data_key,
            bytes::Bytes::from(corrupted),
            PutOptions::default(),
        )
        .await
        .expect("overwrite corrupted object");

    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let running = ravel_server::start(
        maintain_config(Mode::Maintain, &tenant),
        store_dyn,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("maintain server starts");

    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // Poll /metrics until the real scrub tick has run and recorded the anomaly.
    let mut detected = false;
    for _ in 0..60 {
        let body = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("scrape /metrics")
            .text()
            .await
            .expect("metrics body");
        if body
            .contains("ravel_scrub_checksum_mismatch_total{mode=\"maintain\",signal=\"metrics\"} 1")
        {
            detected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    running.shutdown().await.expect("graceful shutdown");
    assert!(
        detected,
        "the injected corruption must surface as ravel_scrub_checksum_mismatch_total on /metrics \
         within one scrub cycle, through the real spawned ScrubTask"
    );
}

/// The scrub metrics family is exposed only in `Mode::Maintain`, the one mode
/// that spawns the scrubber (mirrors the maintain discovery/safety families'
/// Maintain-only gating). A query- or gateway-mode process spawns no scrub task
/// and renders no `ravel_scrub_*` series.
#[tokio::test]
async fn scrub_family_present_only_in_maintain_mode() {
    let tenant = TenantId::new("scrub-gate");

    for (mode, expect_present) in [
        (Mode::Query, false),
        (Mode::Gateway, false),
        (Mode::Maintain, true),
    ] {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let running = ravel_server::start(
            maintain_config(mode, &tenant),
            store,
            Arc::new(ravel_object_store::StoreMetrics::default()),
            None,
        )
        .await
        .expect("server starts");

        let base = format!("http://{}", running.http_addr);
        let body = reqwest::Client::new()
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("scrape /metrics")
            .text()
            .await
            .expect("metrics body");
        running.shutdown().await.expect("graceful shutdown");

        let present = body.contains("ravel_scrub_checksum_mismatch_total");
        assert_eq!(
            present, expect_present,
            "scrub family presence for {mode:?} should be {expect_present}"
        );
    }
}
