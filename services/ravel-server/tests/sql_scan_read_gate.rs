//! ADR-1702 task 8: a server built by [`ravel_server::start`] attaches its
//! read CPU gate to the SQL `logs` and `spans` scans, and to the `samples`
//! scan's RSEG catalog decodes.
//!
//! The SQL state is built inside `start` by
//! `query::build_sql_state_with_parquet`, and the gate is only observable from
//! outside through `/metrics`. A started server runs one `logs` and one
//! `spans` statement over published fixture objects, and each scan's gate site
//! counts its block decodes. The fixture blocks sit far below the default
//! 256 KiB inline floor, so every decode is an inline run, which the gate
//! still counts per site (decision 4); with no gate on the fetchers, nothing
//! is counted at all.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;
/// Records in each fixture object. Both writers cut a block per record
/// (`block_target_records: 1`), so this is each object's block count; the
/// statements push no predicate and their window covers every record, so
/// each block is decoded exactly once.
const BLOCKS: u64 = 4;
/// RSEG segments in the `samples` fixture, each one series of
/// [`METRIC_SAMPLES`] samples inside the statement's window.
const METRIC_SEGMENTS: u64 = 3;
const METRIC_SAMPLES: i64 = 5;

fn server_config(tokens: HashMap<String, TenantId>) -> ServerConfig {
    ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
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
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
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
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}

/// The commit record for one fixture object, published after the object.
#[allow(clippy::too_many_arguments)]
async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
    writer_id: Uuid,
    bytes: Vec<u8>,
    series_count: u64,
    max_event_ts_ns: i64,
    segment_format_version: u32,
) {
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: BLOCKS,
        series_count,
        min_event_ts_ns: 0,
        max_event_ts_ns,
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: max_event_ts_ns,
        segment_format_version,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish commit");
}

/// One RLOG object of [`BLOCKS`] one-record blocks, all one stream.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let writer_id = Uuid::from_u128(4_000);
    let mut writer = RlogWriter::new(
        RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        },
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
    for ts in 0..BLOCKS as i64 {
        writer
            .push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("row {ts}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    publish_object(
        store,
        tenant,
        Signal::Logs,
        writer_id,
        bytes,
        1,
        BLOCKS as i64 - 1,
        u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
    )
    .await;
}

/// One RSPAN object of [`BLOCKS`] one-span blocks, one trace per span.
async fn publish_spans(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let writer_id = Uuid::from_u128(5_000);
    let mut writer = ravel_rspan::RspanWriter::new(
        ravel_rspan::RspanConfig {
            block_target_records: 1,
            ..ravel_rspan::RspanConfig::default()
        },
        ravel_rspan::ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for n in 0..BLOCKS as u8 {
        writer.push(ravel_rspan::SpanRecord {
            trace_id: [n + 1; 16],
            span_id: [n + 1; 8],
            parent_span_id: None,
            name: format!("op-{n}"),
            start_ts_ns: i64::from(n),
            end_ts_ns: i64::from(n) + 1,
            status_code: ravel_rspan::StatusCode::Ok,
            status_message: None,
            attrs: vec![("service.name".to_string(), "svc".to_string())],
        });
    }
    let bytes = writer.finish().expect("finish rspan object");
    publish_object(
        store,
        tenant,
        Signal::Spans,
        writer_id,
        bytes,
        BLOCKS,
        BLOCKS as i64,
        u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
    )
    .await;
}

/// [`METRIC_SEGMENTS`] real RSEG segments, one `gate_metric` series each.
async fn publish_metric_segments(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let labels = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: "gate_metric".to_string(),
    }])
    .expect("valid labels");
    for seq in 1..=METRIC_SEGMENTS {
        let writer_id = Uuid::from_u128(6_000 + u128::from(seq));
        let base_ns = i64::try_from(seq).expect("seq fits") * 60_000_000_000;
        let written = SegmentWriter::write(
            vec![SeriesInput {
                series_id: SeriesId::compute(tenant, "gate_metric", &labels).expect("series id"),
                labels: labels.clone(),
                samples: (0..METRIC_SAMPLES)
                    .map(|i| Sample {
                        ts_ns: base_ns + i * 1_000_000_000,
                        value: i as f64,
                    })
                    .collect(),
            }],
            SegmentIdentity {
                tenant_hash: tenant.hash().0,
                shard: 0,
                writer_id: writer_id.to_string(),
                writer_epoch: 1,
                writer_seq: seq,
            },
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write segment");
        let rec = record::build(NewCommitRecord {
            tenant_hash: tenant.hash(),
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
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: written.summary.max_event_ts_ns,
            ingest_hour_bucket: 0,
        })
        .expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish commit");
    }
}

/// Runs `sql` and returns how many result rows came back.
async fn run_sql(client: &reqwest::Client, base: &str, sql: &str) -> usize {
    let response = client
        .post(format!("{base}/api/v1/sql"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "query": sql,
                "start": 0.0,
                "end": NOW_NS as f64 / 1_000_000_000.0,
            })
            .to_string(),
        )
        .send()
        .await
        .expect("sql request sent");
    let status = response.status();
    let body: serde_json::Value = response.json().await.expect("sql response is JSON");
    assert_eq!(status, 200, "{sql} succeeds: {body}");
    body["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("{sql} returned no row array: {body}"))
        .len()
}

/// The read gate's `(jobs, inline)` counts for `site` on `/metrics`, each
/// required exactly once.
async fn read_gate_site(client: &reqwest::Client, base: &str, site: &str) -> (u64, u64) {
    let text = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request sent")
        .text()
        .await
        .expect("metrics body");
    let sample = |family: &str| {
        let matched: Vec<u64> = text
            .lines()
            .filter(|line| {
                line.starts_with(&format!("{family}{{"))
                    && line.contains("gate=\"read\"")
                    && line.contains(&format!("site=\"{site}\""))
            })
            .map(|line| {
                line.rsplit(' ')
                    .next()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or_else(|| panic!("unparsable sample: {line}")) as u64
            })
            .collect();
        assert_eq!(
            matched.len(),
            1,
            "{family} for site {site} renders exactly once"
        );
        matched[0]
    };
    (
        sample("ravel_cpu_gate_jobs_total"),
        sample("ravel_cpu_gate_inline_total"),
    )
}

/// A `logs` and a `spans` statement against a started server each count
/// exactly [`BLOCKS`] decodes at their scan's read gate site, all inline under
/// the default floor. Fails with the gate left off either fetcher in
/// `build_sql_state_inner`, or with `start` passing no gate: that site then
/// reads `(0, 0)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_server_runs_sql_scan_block_decodes_through_its_read_gate() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    publish_logs(store.as_ref(), &tenant).await;
    publish_spans(store.as_ref(), &tenant).await;
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant);
    let running = ravel_server::start(
        server_config(tokens),
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    assert_eq!(read_gate_site(&client, &base, "log_block").await, (0, 0));
    assert_eq!(read_gate_site(&client, &base, "span_block").await, (0, 0));

    assert_eq!(
        run_sql(&client, &base, "SELECT ts FROM logs").await,
        BLOCKS as usize
    );
    assert_eq!(
        read_gate_site(&client, &base, "log_block").await,
        (0, BLOCKS),
        "every logs block decode was counted at the read gate"
    );

    assert_eq!(
        run_sql(&client, &base, "SELECT trace_id FROM spans").await,
        BLOCKS as usize
    );
    assert_eq!(
        read_gate_site(&client, &base, "span_block").await,
        (0, BLOCKS),
        "every spans block decode was counted at the read gate"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// A `samples` statement against a started server decodes each fixture
/// segment's RSEG catalog once, and each decode counts one `segment_section`
/// run at the read gate. The catalogs are a few hundred bytes, under the
/// 256 KiB floor, so every run is inline. Fails with the gate left off the
/// metrics fetcher in `build_sql_state_inner`: the site then reads `(0, 0)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_server_runs_sql_metrics_catalog_decodes_through_its_read_gate() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    publish_metric_segments(store.as_ref(), &tenant).await;
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant);
    let running = ravel_server::start(
        server_config(tokens),
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    assert_eq!(
        read_gate_site(&client, &base, "segment_section").await,
        (0, 0)
    );
    assert_eq!(
        run_sql(&client, &base, "SELECT ts, value FROM samples").await,
        (METRIC_SEGMENTS as i64 * METRIC_SAMPLES) as usize
    );
    assert_eq!(
        read_gate_site(&client, &base, "segment_section").await,
        (0, METRIC_SEGMENTS),
        "every metrics segment's catalog decode was counted at the read gate"
    );

    running.shutdown().await.expect("graceful shutdown");
}
