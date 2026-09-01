//! End-to-end reachability test for the at-rest integrity scrubber (ADR-0059).
//! This is the designated proof that the capability is
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

use ravel_catalog::CatalogConfig;
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
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
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
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(1),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        cache_dir: None,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
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
        store_dyn.clone(),
        store_dyn.clone(),
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

/// System wall clock in nanoseconds, for sealing records relative to "now" the
/// way real ingest does (the fold's seal margins are computed against the same
/// clock the running server reads).
fn now_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos() as i64
}

/// Comfortably past the default seal margins (matches the CLI `catalog verify`
/// suite's `SEALED_AGE_NS` and `fold_e2e.rs`'s `SEALED_AGE`), so a record
/// created this far back is sealed by fold time.
const SEALED_AGE_NS: i64 = 3 * NS_PER_HOUR;

/// Publish a real sealed RSEG segment plus its commit record into `(tenant,
/// Metrics, shard 0)` with the given `writer_seq` and `created_unix_ns`, so two
/// calls with distinct `seq` produce two distinct sealed commit records.
async fn publish_sealed_segment(
    store: &MemoryStore,
    tenant: &TenantId,
    seq: u64,
    created_unix_ns: i64,
) {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(u128::from(seq));
    let ingest_hour_bucket = u32::try_from(created_unix_ns / NS_PER_HOUR).expect("fits u32");
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
        writer_seq: seq,
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
        writer_seq: seq,
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
}

/// A real seal divergence, injected at rest, surfaces on `/metrics` through the
/// real scheduled scrub task (ADR-0059 decision 2). One sealed
/// record is folded into the snapshot; a second is sealed *after* the fold, so
/// the snapshot under-counts against the re-listed sealed commit history.
/// The running `Mode::Maintain` server's scrub task detects this on a real tick
/// and reports it as `ravel_scrub_seal_divergence_total{reason="missing"} 1`.
#[tokio::test]
async fn seal_divergence_surfaces_on_metrics_through_the_real_scrub_task() {
    let tenant = TenantId::new("scrub-seal-e2e");
    let store = Arc::new(MemoryStore::new());
    let now = now_ns();
    let created = now - SEALED_AGE_NS;

    // One sealed record, folded into a real HEAD.
    publish_sealed_segment(store.as_ref(), &tenant, 1, created).await;
    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = ravel_catalog::Catalog::new(
        store_dyn.clone(),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog")
    .with_provisioning_enforcement();
    catalog
        .fold(
            &tenant.hash(),
            Signal::Metrics,
            Uuid::new_v4(),
            now,
            &[],
            None,
        )
        .await
        .expect("fold produces a HEAD");

    // A second record, sealed but published after the fold: the snapshot
    // under-counts against the re-listed sealed commit history.
    publish_sealed_segment(store.as_ref(), &tenant, 2, created).await;

    let running = ravel_server::start(
        maintain_config(Mode::Maintain, &tenant),
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("maintain server starts");

    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

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
        if body.contains(
            "ravel_scrub_seal_divergence_total{mode=\"maintain\",signal=\"metrics\",reason=\"missing\"} 1",
        ) {
            detected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    running.shutdown().await.expect("graceful shutdown");
    assert!(
        detected,
        "the sealed-after-fold record must surface as \
         ravel_scrub_seal_divergence_total{{reason=\"missing\"}} on /metrics within one scrub \
         cycle, through the real spawned ScrubTask"
    );
}

/// A real postings disagreement (a false negative), injected at rest,
/// surfaces on `/metrics` through the real scheduled scrub task (ADR-0059
/// decision 1). A tenant's real data is folded so a real,
/// correctly-bound name-postings object exists at `head.postings`; then a NEW,
/// internally valid postings object that omits a name the folded segment really
/// carries is written over the real postings key and the HEAD's postings ref is
/// repointed at it (matching blake3), so `load_covering_postings` accepts it
/// instead of degrading to `None`. The running `Mode::Maintain` server's scrub
/// task loads it, re-derives the segment's true name set on the content tier's
/// read, and reports the disagreement as
/// `ravel_scrub_postings_disagreement_total 1`.
#[tokio::test]
async fn postings_disagreement_surfaces_on_metrics_through_the_real_scrub_task() {
    let tenant = TenantId::new("scrub-postings-e2e");
    let store = Arc::new(MemoryStore::new());
    let now = now_ns();
    let created = now - SEALED_AGE_NS;

    // One real sealed segment carrying "cpu" and "mem", folded into a real HEAD
    // whose postings correctly claim both names for the single covered entry.
    publish_sealed_segment(store.as_ref(), &tenant, 1, created).await;
    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = ravel_catalog::Catalog::new(
        store_dyn.clone(),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog")
    .with_provisioning_enforcement();
    catalog
        .fold(
            &tenant.hash(),
            Signal::Metrics,
            Uuid::new_v4(),
            now,
            &[],
            None,
        )
        .await
        .expect("fold produces a HEAD with postings");

    // Read the folded HEAD and the covered part(s)' blake3, then encode a NEW,
    // internally valid postings object claiming only "cpu" for ordinal 0 --
    // omitting "mem", which the segment really carries. This is a genuine false
    // negative, not a bit flip: a raw flip would trip StructuralCorruption, but
    // a validly-encoded object with wrong claims is exactly what the postings
    // tier exists to catch.
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant.hash().to_hex(),
        Signal::Metrics.key_prefix(),
    );
    let head_bytes = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("head present after fold")
        .data;
    let mut head = ravel_catalog::decode_head(&head_bytes).expect("decode head");
    let part_blake3: Vec<[u8; 32]> = head
        .parts
        .iter()
        .map(|p| <[u8; 32]>::try_from(p.blake3.as_slice()).expect("32-byte part blake3"))
        .collect();
    let names = vec![ravel_catalog::NamePostings {
        name: "cpu".to_string(),
        ordinals: vec![0],
    }];
    let tampered = ravel_catalog::encode_postings(
        tenant.hash().0,
        Signal::Metrics as u32,
        &part_blake3,
        1,
        &names,
    )
    .expect("encode tampered postings");
    let tampered_hash = *blake3::hash(&tampered).as_bytes();

    // Overwrite the postings object over its real key, then repoint the HEAD's
    // postings ref at it with the matching blake3/size so the load accepts it.
    let postings_ref = head.postings.as_mut().expect("postings ref after fold");
    store
        .put(
            &postings_ref.key,
            bytes::Bytes::from(tampered.clone()),
            PutOptions::default(),
        )
        .await
        .expect("overwrite postings object over its real key");
    postings_ref.blake3 = tampered_hash.to_vec();
    postings_ref.size = tampered.len() as u64;
    let rewritten = ravel_catalog::encode_head(&head).expect("re-encode head");
    store
        .put(
            &head_key,
            bytes::Bytes::from(rewritten),
            PutOptions::default(),
        )
        .await
        .expect("overwrite head");

    let running = ravel_server::start(
        maintain_config(Mode::Maintain, &tenant),
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("maintain server starts");

    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

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
        if body.contains(
            "ravel_scrub_postings_disagreement_total{mode=\"maintain\",signal=\"metrics\"} 1",
        ) {
            detected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    running.shutdown().await.expect("graceful shutdown");
    assert!(
        detected,
        "the omitted-name postings disagreement must surface as \
         ravel_scrub_postings_disagreement_total on /metrics within one scrub cycle, through the \
         real spawned ScrubTask"
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
            store.clone(),
            store.clone(),
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
