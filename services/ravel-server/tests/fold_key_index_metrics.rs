//! `/metrics` renders the fold's key-index leaf build figures (ADR-3692
//! decision 7) under their names, HELP text and counter TYPE, with the
//! values a real fold through `POST /api/v1/admin/fold` produced.
//!
//! One test per binary on purpose: the four totals are process-global, so a
//! second folding test in this file would make the exact figures below
//! unpinnable.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ravel_catalog::{
    DeclaredColumnType, DeclaredTypedColumn, StorageLayoutWrite, TenantConfig, TenantLifecycleState,
};
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::key_index::KeyType;
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, StoreMetrics};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "operator-token";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const USER_ID: &str = "user_id";

/// The first hour of the UTC day before the one `now` falls in. Every hour
/// this test seeds or folds through lies in that day, so the on-demand fold,
/// which reads the wall clock, finds both batches on one UTC day whatever
/// hour it runs at: batches on two UTC days would seal the first day apart
/// from the second and add a build pass.
fn yesterday_start_hour(now: i64) -> u32 {
    u32::try_from((now / NS_PER_HOUR / 24 - 1) * 24).expect("hour fits u32")
}

/// The first batch's hour of that day.
const FIRST_BATCH_HOUR_OF_DAY: u32 = 1;
/// The first fold runs at the start of this hour of that day, directly
/// through the catalog, and seals every hour ending at least the default
/// `max_flush_lifetime (1h) + clock_skew_allowance (5m) + fold_safety_margin
/// (15m)` before it: the first batch's hour and none after hour 2.
const FIRST_FOLD_HOUR_OF_DAY: u32 = 4;
/// The second batch's hour: after every hour the first fold sealed, and
/// ending 3 h before the day does, before the earliest time the on-demand
/// fold can run, so only that fold, through the on-demand route, folds it,
/// re-encoding the first fold's tail part.
const SECOND_BATCH_HOUR_OF_DAY: u32 = 20;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// Declares `user_id` as an I64 key-index column, which is what makes the
/// fold build a leaf for it.
async fn declare_user_id_key_index(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let mut config = TenantConfig {
        typed_attr_columns: Some(vec![DeclaredTypedColumn {
            key: USER_ID.to_string(),
            ty: DeclaredColumnType::I64,
        }]),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    config
        .set_key_index_columns(
            vec![USER_ID.to_string()],
            StorageLayoutWrite::ReadersRolledOut,
        )
        .expect("declare the key index");
    ravel_catalog::set_tenant_config(store, &tenant.hash(), &config, 1)
        .await
        .expect("write tenant config");
}

/// Seeds one durable log flush whose RLOG object carries a KEY_IDX section
/// for `user_id`, one row per value.
async fn seed_sealed_log_commit(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    ingest_hour: u32,
    seq: u64,
    user_ids: &[i64],
) {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(u128::from(seq) + 1);
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("checkout".to_string()),
    )];
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: seq,
        },
    )
    .with_key_index_fields(vec![(USER_ID.to_string(), KeyType::I64)]);
    let base_ts = i64::from(ingest_hour) * NS_PER_HOUR + 60_000_000_000;
    for (i, user_id) in user_ids.iter().enumerate() {
        let ts = base_ts + i as i64;
        writer
            .push(LogRecord {
                stream_id: log_stream_id(&resource, "", "", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "", "", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: "checkout completed".to_string(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![(USER_ID.to_string(), AttrValue::I64(*user_id))],
            })
            .expect("push log record");
    }
    let bytes = bytes::Bytes::from(writer.finish().expect("finish RLOG object"));
    let commit = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: user_ids.len() as u64,
        series_count: 1,
        min_event_ts_ns: base_ts,
        max_event_ts_ns: base_ts + user_ids.len() as i64,
        min_ingest_ts_ns: base_ts,
        max_ingest_ts_ns: base_ts,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: base_ts,
        ingest_hour_bucket: ingest_hour,
    })
    .expect("build log commit record");
    let data_key = ravel_commit::keys::reconstruct_data_key(&commit).expect("data key");
    publish::put_data_object(store, &data_key, bytes)
        .await
        .expect("put RLOG data object");
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish log commit record");
}

/// `--mode all --disable-fold --fold-key-index-leaves`: the on-demand route
/// folds and builds leaves; no background fold races it.
fn leaf_building_config(
    tenant_resolver: Arc<dyn ravel_query::http::TenantResolver>,
) -> ServerConfig {
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            build_key_index_leaves: true,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        ingest_hold: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}

/// Three commits with a KEY_IDX section each, folded with the leaf build on
/// directly through the catalog: a first fold has no previous tail, so it
/// reads all three sections in one pass, carries nothing and GETs no leaf.
/// Two later commits on the same UTC day are then folded by the on-demand
/// route, which re-encodes that tail: it GETs the previous tail's one leaf, carries its
/// three entries and reads the two new sections in one pass. The totals are
/// process-wide, so `/metrics` must render each of the four families exactly
/// once with its HELP and counter TYPE and the two folds' sums: 3 + 2 = 5
/// sections, 0 + 3 = 3 entries carried, 1 + 1 = 2 passes and 0 + 1 = 1 leaf
/// GET, four different values so a family rendering another's value fails.
#[tokio::test]
async fn a_leaf_building_fold_renders_the_key_index_build_families() {
    let tenant = TenantId::new("acme");
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant.clone());
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());

    declare_user_id_key_index(store.as_ref(), &tenant).await;
    let day = yesterday_start_hour(now_ns());
    let first_hour = day + FIRST_BATCH_HOUR_OF_DAY;
    let second_hour = day + SECOND_BATCH_HOUR_OF_DAY;
    assert_eq!(first_hour / 24, second_hour / 24, "one UTC day");
    for (seq, user_ids) in [(0u64, [1i64, 2]), (1, [2, 3]), (2, [3, 4])] {
        seed_sealed_log_commit(store.as_ref(), &tenant, first_hour, seq, &user_ids).await;
    }
    let catalog = ravel_catalog::Catalog::new(
        store.clone(),
        ravel_catalog::CatalogConfig {
            shard_count: 1,
            build_key_index_leaves: true,
            ..ravel_catalog::CatalogConfig::default()
        },
    )
    .expect("catalog");
    let first = catalog
        .fold(
            &tenant.hash(),
            Signal::Logs,
            Uuid::from_u128(7),
            i64::from(day + FIRST_FOLD_HOUR_OF_DAY) * NS_PER_HOUR,
            &[],
            None,
        )
        .await
        .expect("first fold");
    assert_eq!(first.entry_count, 3, "{first:?}");
    assert_eq!(first.key_index_leaves_written, 1, "{first:?}");
    assert!(
        first.watermark_hour.is_some_and(|w| w < second_hour),
        "{first:?}"
    );
    for (seq, user_ids) in [(3u64, [5i64, 6]), (4, [6, 7])] {
        seed_sealed_log_commit(store.as_ref(), &tenant, second_hour, seq, &user_ids).await;
    }

    let running = ravel_server::start(
        leaf_building_config(tenant_resolver),
        store.clone(),
        store.clone(),
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let fold: serde_json::Value = client
        .post(format!("{base}/api/v1/admin/fold"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({ "signal": "logs" }))
        .send()
        .await
        .expect("on-demand fold request completes")
        .json()
        .await
        .expect("fold response is JSON");
    assert_eq!(fold["status"], "published", "{fold}");
    assert_eq!(fold["entry_count"], 5, "{fold}");

    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text");

    for (name, help, value) in [
        (
            "ravel_catalog_fold_key_index_sections_read_total",
            "KEY_IDX sections the fold's key-index leaf build read, one per entry per build pass. An entry carried from a previous part's leaves reads none.",
            5,
        ),
        (
            "ravel_catalog_fold_key_index_entries_carried_total",
            "Entries whose key-index pairs the fold carried from the leaves of the previous parts their part overlaps, matched by data object key, instead of reading their sections.",
            3,
        ),
        (
            "ravel_catalog_fold_key_index_build_passes_total",
            "Passes of the fold's key-index leaf build over disjoint key8 ranges: one per re-encoded part whose pairs fit the pair budget, one per key8 range above it, and the ranges of a field rebuilt after a pass found a new uncovered entry.",
            2,
        ),
        (
            "ravel_catalog_fold_key_index_leaf_gets_total",
            "GETs of previous parts' key-index leaves the fold issued to carry their pairs forward, one per (field, slice) a build fetches, and another when a field's rebuild fetches a released slice again.",
            1,
        ),
    ] {
        assert_eq!(
            body.matches(&format!("# HELP {name} {help}\n")).count(),
            1,
            "{name} HELP renders exactly once:\n{body}"
        );
        assert_eq!(
            body.matches(&format!("# TYPE {name} counter\n")).count(),
            1,
            "{name} TYPE renders exactly once:\n{body}"
        );
        assert_eq!(
            body.matches(&format!("{name}{{mode=\"all\"}} {value}\n"))
                .count(),
            1,
            "{name} renders exactly once at {value}:\n{body}"
        );
    }

    running.shutdown().await.expect("graceful shutdown");
}
