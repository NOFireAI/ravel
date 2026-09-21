//! A process run with `--disable-fold` still folds through
//! `POST /api/v1/admin/fold`, and `/metrics` must report the stamp coverage
//! that fold produced.
//!
//! The background fold task is not the only thing in the process that folds.
//! The on-demand route is mounted in `all` and `query` whatever
//! `--disable-fold` says, it runs the same `Catalog::fold`, and its coverage
//! lands in the same process-global totals
//! (`ravel_catalog::fold_stamped_{records,entries}_total`). A render gate that
//! followed only the spawned task therefore hid real, moving coverage from the
//! one operator who had to ask for every fold by hand.
//!
//! One test per binary on purpose: the two counters are process-global, so a
//! second folding test in this file would make the exact figures below
//! unpinnable.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ravel_commit::declared_stats::stamp_commit_record;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreMetrics};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "operator-token";
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Sealed well past the default `max_flush_lifetime (1h) +
/// clock_skew_allowance (5m) + fold_safety_margin (15m)`, so the one fold this
/// test triggers finds the seeded hour eligible.
const SEALED_AGE_HOURS: i64 = 3;

/// Rows per seeded segment. The stamp below declares no NULLs, and a stamp
/// carrying extrema is only valid when `null_count < sample_count`, so this
/// has to be at least one.
const SAMPLE_COUNT: u64 = 4;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// One valid `EventDate` stamp: extrema present, no NULLs in a four-row
/// object, which is what the fold's carriage predicate admits.
fn event_date_stamp() -> DeclaredColumnStat {
    DeclaredColumnStat::new(
        "EventDate",
        DeclaredStatType::I64,
        Some(DeclaredStatValue::I64(19_000)),
        Some(DeclaredStatValue::I64(19_004)),
        0,
    )
    .expect("a four-row object with no NULLs and both extrema is a valid stamp")
}

/// Seeds one durable log flush (a real RLOG object plus its commit record) at
/// `ingest_hour`, the shape `tests/fold_e2e.rs` seeds. `stamped` decides
/// whether the commit record carries declared-column statistics, which is
/// exactly what the coverage pair counts.
async fn seed_sealed_log_commit(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    ingest_hour: u32,
    event_ts_ns: i64,
    seq: u64,
    stamped: bool,
) {
    let tenant_hash = tenant.hash();
    let shard = 0u32;
    let writer_id = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0007);
    let epoch = 1u64;

    let resource_attrs = vec![(
        "service.name".to_string(),
        AttrValue::Str("checkout".to_string()),
    )];
    let stream_attrs = stream_attrs_bytes(&resource_attrs, "", "", &[]);
    let stream_id = log_stream_id(&resource_attrs, "", "", &[]);

    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.into_bytes(),
            writer_epoch: epoch,
            writer_seq: seq,
        },
    );
    for row in 0..SAMPLE_COUNT {
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: event_ts_ns + row as i64,
                observed_ts_ns: event_ts_ns + row as i64,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: "checkout completed".to_string(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish RLOG object");

    // The commit record's `content_hash` only has to name the object key the
    // fold will GET, exactly as in `fold_e2e.rs`: nothing verifies it against
    // the bytes, so a per-seq fixed value keeps this seed dependency-free.
    let content_hash = [0x5au8 + u8::try_from(seq).expect("seq fits a byte"); 32];
    let data_key = ravel_commit::keys::data_key(
        &tenant_hash,
        Signal::Logs,
        shard,
        writer_id,
        epoch,
        seq,
        &content_hash,
    )
    .expect("build data key");
    store
        .put(
            &data_key,
            bytes::Bytes::from(bytes),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put RLOG data object");

    let created_unix_ns = i64::from(ingest_hour) * NS_PER_HOUR;
    let mut commit = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard,
        writer_id,
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: 0,
        content_hash,
        sample_count: SAMPLE_COUNT,
        series_count: 1,
        min_event_ts_ns: event_ts_ns,
        max_event_ts_ns: event_ts_ns + SAMPLE_COUNT as i64,
        min_ingest_ts_ns: created_unix_ns,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns,
        ingest_hour_bucket: ingest_hour,
    })
    .expect("build log commit record");
    if stamped {
        stamp_commit_record(&mut commit, &[event_date_stamp()]);
    }
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish log commit record");
}

/// `--mode all --disable-fold`: no background fold task is spawned, and the
/// on-demand route is mounted all the same.
fn disabled_background_fold_config(
    tenant_resolver: Arc<dyn ravel_query::http::TenantResolver>,
) -> ServerConfig {
    ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
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

/// Two stamped commit records and one unstamped one, folded by the on-demand
/// route in a `--disable-fold` process. `/metrics` must then render both
/// coverage families with the counts THAT fold produced: two stamped records
/// read, two stamped entries written. Gating the render on the background
/// task rendered neither family here, so an operator driving folds by hand
/// saw no coverage at all while coverage was moving.
#[tokio::test]
async fn on_demand_fold_in_a_disable_fold_process_renders_the_coverage_it_produced() {
    let tenant = TenantId::new("acme");
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant.clone());
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());

    let sealed_ts_ns = now_ns() - SEALED_AGE_HOURS * NS_PER_HOUR;
    let sealed_hour = u32::try_from(sealed_ts_ns.div_euclid(NS_PER_HOUR)).expect("hour fits u32");
    for (seq, stamped) in [(0u64, true), (1, true), (2, false)] {
        seed_sealed_log_commit(
            store.as_ref(),
            &tenant,
            sealed_hour,
            sealed_ts_ns,
            seq,
            stamped,
        )
        .await;
    }

    let running = ravel_server::start(
        disabled_background_fold_config(tenant_resolver),
        store.clone(),
        store.clone(),
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // The route is reachable with the background fold off, and it really
    // folds: this is the premise the metrics gate has to follow.
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
    assert_eq!(
        fold["status"], "published",
        "the sealed hour must fold and publish through the route:\n{fold}"
    );
    assert_eq!(
        fold["entry_count"], 3,
        "all three seeded commits fold in:\n{fold}"
    );

    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text");

    assert_eq!(
        body.matches("ravel_catalog_fold_stamped_records_total{mode=\"all\"} 2\n")
            .count(),
        1,
        "the two stamped commit records the on-demand fold read must render exactly once, \
         at 2:\n{body}"
    );
    assert_eq!(
        body.matches("ravel_catalog_fold_stamped_entries_total{mode=\"all\"} 2\n")
            .count(),
        1,
        "the two stamped snapshot entries that fold wrote must render exactly once, \
         at 2:\n{body}"
    );

    running.shutdown().await.expect("graceful shutdown");
}
