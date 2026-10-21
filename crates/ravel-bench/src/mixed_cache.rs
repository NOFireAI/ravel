//! Mixed scan-and-interactive SQL workload against a sized read cache
//! (ADR-3072 decision 3, track P5; issue #3084).
//!
//! The read cache admits a statement's reads either `Interactive` (normal
//! S3-FIFO admission) or `BulkScan` (a region capped at `bulk_share` of the
//! RAM tier, admitting a stable subset, with hits that do not raise
//! frequency). Its purpose is scan resistance: large scans must not evict the
//! working set that small, repeated statements re-read. A one-statement-at-a-
//! time benchmark cannot show that, so this mode runs, for a fixed duration on
//! one generated logs tenant:
//!
//! - `background` connections, each looping full-tenant scans whose predicates
//!   the block statistics cannot prune, plus a predicate-free aggregate, so
//!   every object is read on every pass ([`background_statements`]);
//! - `foreground` connections, each looping a small interactive set over a
//!   narrow window of the most recent objects, whose combined read set is far
//!   below the cache ([`foreground_statements`]).
//!
//! An arm is one value of the `--cache-bulk-scan-min-bytes` trigger
//! ([`Arm`]). Arms run interleaved (A1 B1 C1 A2 B2 C2 ...), each repetition on
//! a fresh cache and executor, so a drift in the host affects every arm alike.
//!
//! Every figure is taken from the statements' own `stats.cache` and
//! object-store accounting, never estimated. [`check_report`] refuses a
//! report that lacks a figure or whose class completed no statement, and the
//! bin exits non-zero on that refusal. `served_share` is cache hit bytes over
//! cache hit bytes plus object-store GET wire bytes (bytes as transferred,
//! range reads included); `wire_bytes` is that GET figure alone.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::CacheFetchError;
use ravel_sql::{DeclaredColumn, DeclaredType, QueryOutput, SqlExecutor, SqlRequest};
use ravel_types::accounting::{AccessIntent, AccountedOp};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::sql_latency::{
    DatasetInfo, Error, ExecutorSettings, NOW_NS, cold_executor, dataset_info,
};

/// One value of the bulk-scan trigger under measurement.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Arm {
    /// The arm's name in the report (`A`, `B`, `C`).
    pub label: String,
    /// The `--cache-bulk-scan-min-bytes` value the arm's executor runs with;
    /// `None` is the flag unset, today's behaviour.
    pub bulk_scan_min_bytes: Option<u64>,
}

/// Parse `--mixed-arms`: comma-separated `label=value` entries, where value is
/// `unset`, `region` (the bulk region's bytes, `cache_bytes * bulk_share` as
/// the cache computes it), `cache` (the whole cache), or a byte count.
pub fn parse_arms(spec: &str, cache_bytes: u64, bulk_share: f64) -> Result<Vec<Arm>, String> {
    let mut arms = Vec::new();
    for entry in spec.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            return Err(format!("empty arm entry in `{spec}`"));
        }
        let Some((label, value)) = entry.split_once('=') else {
            return Err(format!("arm `{entry}` is not `label=value`"));
        };
        let label = label.trim();
        if label.is_empty() {
            return Err(format!("arm `{entry}` has an empty label"));
        }
        if arms.iter().any(|a: &Arm| a.label == label) {
            return Err(format!("arm label `{label}` is given twice"));
        }
        let bulk_scan_min_bytes = match value.trim() {
            "unset" => None,
            "region" => Some(bulk_region_bytes(cache_bytes, bulk_share)),
            "cache" => Some(cache_bytes),
            other => Some(
                other
                    .parse::<u64>()
                    .map_err(|e| format!("arm `{entry}`: `{other}` is not a byte count: {e}"))?,
            ),
        };
        arms.push(Arm {
            label: label.to_string(),
            bulk_scan_min_bytes,
        });
    }
    if arms.is_empty() {
        return Err("no arms given".to_string());
    }
    Ok(arms)
}

/// The bulk region's byte cap, computed exactly as the cache computes it from
/// the same two inputs.
pub fn bulk_region_bytes(cache_bytes: u64, bulk_share: f64) -> u64 {
    let share = if bulk_share.is_nan() {
        ravel_cache::DEFAULT_BULK_SHARE
    } else {
        bulk_share.clamp(0.0, 1.0)
    };
    (cache_bytes as f64 * share) as u64
}

/// The whole configuration of one mixed run.
pub struct MixedConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_backend: String,
    /// Log records to generate, split into objects of `records_per_object`.
    pub records: usize,
    pub records_per_object: usize,
    /// Filler attribute keys per record.
    pub extra_attrs: usize,
    /// Length of each record's high-entropy body, so the on-object bytes do
    /// not compress away.
    pub body_bytes: usize,
    /// RAM read cache byte budget; must be non-zero.
    pub cache_bytes: u64,
    /// The cache's bulk region share (`--cache-bulk-share`).
    pub cache_bulk_share: f64,
    /// How long each arm run loops its connections.
    pub duration: Duration,
    /// Background (scan) connections.
    pub background: usize,
    /// Foreground (interactive) connections.
    pub foreground: usize,
    /// Arms, in the order they run within one repetition; the first is the
    /// reference the bands compare against.
    pub arms: Vec<Arm>,
    pub repetitions: usize,
    /// How many of the most recent objects the foreground window covers.
    pub window_objects: usize,
    /// Restrict the background set to these statement ids; empty runs every
    /// statement of [`background_statements`]. The scans-only check loops one.
    pub background_ids: Vec<String>,
    /// Per-statement wall deadline.
    pub deadline: Duration,
    /// Executor knobs; `cache_bulk_scan_min_bytes` is overridden per arm.
    pub settings: ExecutorSettings,
}

/// Which connection class ran a statement.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadClass {
    Foreground,
    Background,
}

impl WorkloadClass {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkloadClass::Foreground => "foreground",
            WorkloadClass::Background => "background",
        }
    }
}

/// One statement of the workload with the resolve window it runs under.
#[derive(Clone, Debug)]
pub struct Statement {
    pub id: String,
    pub class: WorkloadClass,
    pub sql: String,
    pub window: TimeRange,
}

/// The background set: two filtered scans whose predicates no block
/// statistic prunes (`req_id` is unique per record and the modulo is not a
/// range; `duration_ms` takes every one of its five values in every block),
/// and one predicate-free aggregate, which today's rule already reads as a
/// bulk scan over a whole-tenant window.
pub fn background_statements(window: TimeRange) -> Vec<Statement> {
    [
        (
            "bg_filter_count",
            "SELECT count(*), sum(duration_ms) FROM logs WHERE req_id % 1000 = 7",
        ),
        (
            "bg_filter_group",
            "SELECT severity_text, count(*), max(duration_ms) FROM logs \
             WHERE duration_ms >= 400 GROUP BY severity_text ORDER BY severity_text",
        ),
        (
            "bg_full_aggregate",
            "SELECT count(*), min(ts), max(ts), avg(duration_ms) FROM logs",
        ),
    ]
    .into_iter()
    .map(|(id, sql)| Statement {
        id: id.to_string(),
        class: WorkloadClass::Background,
        sql: sql.to_string(),
        window,
    })
    .collect()
}

/// The foreground set: selective statements over the narrow window of the
/// most recent objects, whose combined read set is those objects alone.
pub fn foreground_statements(window: TimeRange) -> Vec<Statement> {
    [
        (
            "fg_error_count",
            "SELECT count(*) FROM logs WHERE severity_text = 'ERROR'",
        ),
        (
            "fg_slow_recent",
            "SELECT ts, body FROM logs WHERE duration_ms >= 1200 ORDER BY ts DESC LIMIT 20",
        ),
        (
            "fg_severity_hist",
            "SELECT severity_text, count(*) FROM logs GROUP BY severity_text \
             ORDER BY severity_text",
        ),
    ]
    .into_iter()
    .map(|(id, sql)| Statement {
        id: id.to_string(),
        class: WorkloadClass::Foreground,
        sql: sql.to_string(),
        window,
    })
    .collect()
}

/// The declared columns the workload's statements filter and aggregate on.
pub fn declared_columns() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("duration_ms", DeclaredType::I64),
        DeclaredColumn::new("req_id", DeclaredType::I64),
    ]
}

/// Statements by intent, as `stats.cache.intent` reported them.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IntentCounts {
    pub interactive: u64,
    pub bulk_scan: u64,
    pub maintenance: u64,
}

impl IntentCounts {
    fn add(&mut self, intent: AccessIntent) {
        match intent {
            AccessIntent::Interactive => self.interactive += 1,
            AccessIntent::BulkScan => self.bulk_scan += 1,
            AccessIntent::Maintenance => self.maintenance += 1,
        }
    }
}

/// One class's figures over one arm run. Every field named in
/// [`REQUIRED_FIGURES`] is a measured figure the report contract requires.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassFigures {
    pub connections: usize,
    pub statements_completed: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub cache_hits: u64,
    pub cache_hit_bytes: u64,
    pub cache_misses: u64,
    /// `cache_hit_bytes / (cache_hit_bytes + wire_bytes)`; 0 when nothing
    /// was read.
    pub served_share: f64,
    pub object_store_gets: u64,
    /// Object-store GET bytes as transferred, range reads included.
    pub wire_bytes: u64,
    pub intents: IntentCounts,
}

/// The figures every class of every arm run must report, once each.
pub const REQUIRED_FIGURES: [&str; 10] = [
    "statements_completed",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "cache_hits",
    "cache_hit_bytes",
    "cache_misses",
    "served_share",
    "object_store_gets",
    "wire_bytes",
];

/// One statement's figures over one arm run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatementFigures {
    pub id: String,
    pub class: WorkloadClass,
    pub completed: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub rows: usize,
    /// blake3 of the sorted, rendered result rows of the first execution.
    pub digest: String,
    /// Whether every execution in this run returned that digest.
    pub digest_stable: bool,
    pub intents: IntentCounts,
}

/// One arm's run within one repetition.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArmRun {
    pub arm: String,
    pub bulk_scan_min_bytes: Option<u64>,
    pub repetition: usize,
    /// Wall time from the first statement to the last one finishing; the
    /// configured duration plus whatever was in flight at it.
    pub wall_secs: f64,
    pub foreground: ClassFigures,
    pub background: ClassFigures,
    pub statements: Vec<StatementFigures>,
}

/// Medians over the repetitions of one class's figures.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassMedians {
    pub statements_completed: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub cache_hits: f64,
    pub cache_hit_bytes: f64,
    pub cache_misses: f64,
    pub served_share: f64,
    pub object_store_gets: f64,
    pub wire_bytes: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArmMedians {
    pub arm: String,
    pub repetitions: usize,
    pub foreground: ClassMedians,
    pub background: ClassMedians,
}

/// One pre-registered band row, as HIT or MISS with its figures.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BandRow {
    pub id: String,
    pub arm: String,
    pub observed: f64,
    pub relation: String,
    pub bound: f64,
    pub verdict: String,
}

/// Whether every statement returned identical rows in every arm run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResultsCheck {
    pub identical: bool,
    /// Statement ids whose digests differed across runs, or whose executions
    /// within one run differed.
    pub differing: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InteractiveWindow {
    pub objects: usize,
    pub start_ns: i64,
    pub end_ns: i64,
    /// The on-object bytes of the segments the window resolves to.
    pub resolved_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MixedProvenance {
    pub store_backend: String,
    pub host_logical_cores: usize,
    pub cache_bytes: u64,
    pub cache_bulk_share: f64,
    pub bulk_region_bytes: u64,
    pub duration_secs: f64,
    pub background: usize,
    pub foreground: usize,
    pub repetitions: usize,
    pub arms: Vec<Arm>,
    pub records: usize,
    pub records_per_object: usize,
    pub extra_attrs: usize,
    pub body_bytes: usize,
    pub window_objects: usize,
    /// The background statement ids the run looped (every one of
    /// [`background_statements`] when `--mixed-background-ids` was unset).
    pub background_ids: Vec<String>,
    pub deadline_secs: u64,
    pub fetch_concurrency: usize,
    pub sql_partition_count_effective: usize,
    pub store_get_concurrency_effective: usize,
    pub logs_fetch_policy: String,
    pub logs_block_range_threshold_effective: u64,
    pub logs_request_cost_bytes_effective: u64,
    pub sql_max_query_bytes: usize,
    pub tenant_max_bytes: usize,
    pub parallel_final_aggregation: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MixedReport {
    pub provenance: MixedProvenance,
    pub dataset: DatasetInfo,
    pub interactive_window: InteractiveWindow,
    pub runs: Vec<ArmRun>,
    pub medians: Vec<ArmMedians>,
    pub bands: Vec<BandRow>,
    pub results: ResultsCheck,
}

/// One statement execution's measured figures.
struct Execution {
    statement: usize,
    latency_ns: u64,
    intent: AccessIntent,
    hits: u64,
    hit_bytes: u64,
    misses: u64,
    gets: u64,
    wire_bytes: u64,
    rows: usize,
    digest: String,
}

/// The time span and size of one written object.
struct ObjectSpan {
    min_ts: i64,
    max_ts: i64,
}

const SEVERITIES: [(u8, &str); 4] = [(9, "INFO"), (13, "WARN"), (17, "ERROR"), (21, "FATAL")];
const SCOPE_NAME: &str = "mixed-cache-bench";
const SCOPE_VERSION: &str = "1.0";

fn resource() -> Vec<(String, AttrValue)> {
    vec![(
        "service.name".to_string(),
        AttrValue::Str(SCOPE_NAME.to_string()),
    )]
}

/// A body of `len` hex characters from a per-record xorshift stream, so the
/// bodies compress by about half and no further.
fn body(i: usize, len: usize) -> String {
    let mut x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut s = String::with_capacity(len + 16);
    while s.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        // Writing to a String cannot fail.
        let _ = write!(s, "{x:016x}");
    }
    s.truncate(len);
    s
}

fn build_record(
    i: usize,
    stream_id: LogStreamId,
    stream_attrs: &[u8],
    extra_attrs: usize,
    body_bytes: usize,
) -> LogRecord {
    let (severity_num, severity_text) = SEVERITIES[i % SEVERITIES.len()];
    let duration_ms = ((i % 5) as i64) * 400;
    let mut attrs: Vec<(String, AttrValue)> = vec![
        ("duration_ms".to_string(), AttrValue::I64(duration_ms)),
        ("req_id".to_string(), AttrValue::I64(i as i64)),
    ];
    for k in 0..extra_attrs {
        attrs.push((
            format!("attr_{k}"),
            AttrValue::Str(format!("v{}", (i + k) % 7)),
        ));
    }
    let ts_ns = 1_000 + (i as i64) * 1_000;
    LogRecord {
        stream_id,
        stream_attrs: stream_attrs.to_vec(),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num,
        severity_text: severity_text.to_string(),
        body: body(i, body_bytes),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// Write the tenant one object at a time (records are built per object and
/// dropped after it is written, so a gigabyte corpus never sits in memory
/// twice), each published with its own commit record and its bytes' blake3
/// hash, which is what the read cache keys entries by.
async fn generate(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &TenantId,
    cfg: &MixedConfig,
) -> Result<Vec<ObjectSpan>, Error> {
    let res = resource();
    let stream_id = log_stream_id(&res, SCOPE_NAME, SCOPE_VERSION, &[]);
    let stream_attrs = stream_attrs_bytes(&res, SCOPE_NAME, SCOPE_VERSION, &[]);
    let records = cfg.records.max(1);
    let per_object = cfg.records_per_object.max(1);
    let writer_id = Uuid::from_u128(0x5100_0300);
    let mut spans = Vec::with_capacity(records.div_ceil(per_object));
    let mut first = 0usize;
    while first < records {
        let last = (first + per_object).min(records);
        let writer_seq = spans.len() as u64 + 1;
        let identity = ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for i in first..last {
            let rec = build_record(i, stream_id, &stream_attrs, cfg.extra_attrs, cfg.body_bytes);
            min = min.min(rec.ts_ns);
            max = max.max(rec.ts_ns);
            writer.push(rec)?;
        }
        let bytes = writer.finish()?;
        let new_record = NewCommitRecord {
            tenant_hash: tenant.hash(),
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: (last - first) as u64,
            series_count: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        };
        let rec = record::build(new_record)?;
        let data_key = keys::reconstruct_data_key(&rec)?;
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await?;
        publish::publish(store.as_ref(), &rec, &RetryPolicy::default()).await?;
        spans.push(ObjectSpan {
            min_ts: min,
            max_ts: max,
        });
        first = last;
    }
    Ok(spans)
}

/// The segments `window` resolves to, as (count, on-object bytes).
async fn resolved_set(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    window: TimeRange,
) -> Result<(usize, u64), Error> {
    let catalog = Catalog::new(
        Arc::clone(store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )?;
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Logs, window, &[], NOW_NS)
        .await?;
    let bytes = snapshot.segments.iter().map(|s| s.object_size).sum();
    Ok((snapshot.segments.len(), bytes))
}

/// blake3 over the column names and the rendered rows in sorted order, so
/// two executions that return the same rows in a different order digest
/// alike.
fn digest_rows(output: &QueryOutput) -> Result<String, Error> {
    let options = FormatOptions::default();
    let mut rows: Vec<String> = Vec::with_capacity(output.num_rows());
    for batch in output.batches() {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            let mut line = String::new();
            for f in &formatters {
                line.push_str(&f.value(row).try_to_string()?);
                line.push('\u{1f}');
            }
            rows.push(line);
        }
    }
    rows.sort_unstable();
    let mut hasher = blake3::Hasher::new();
    for name in output.column_names() {
        hasher.update(name.as_bytes());
        hasher.update(b"\x1f");
    }
    hasher.update(b"\n");
    for row in &rows {
        hasher.update(row.as_bytes());
        hasher.update(b"\n");
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// One connection: loop the class's statements round-robin from `offset`
/// until `end`, recording each execution's own figures.
async fn connection(
    executor: Arc<SqlExecutor>,
    tenant_hash: TenantHash,
    statements: Arc<Vec<Statement>>,
    class: WorkloadClass,
    offset: usize,
    end: Instant,
    deadline: Duration,
) -> Result<Vec<Execution>, Error> {
    let mine: Vec<usize> = (0..statements.len())
        .filter(|&i| statements[i].class == class)
        .collect();
    if mine.is_empty() {
        return Err(format!("no {} statements to run", class.as_str()).into());
    }
    let mut executions = Vec::new();
    let mut next = offset;
    while Instant::now() < end {
        let idx = mine[next % mine.len()];
        next += 1;
        let statement = &statements[idx];
        let req = SqlRequest {
            sql: statement.sql.clone(),
            window: statement.window,
            min_tokens: Vec::new(),
            now_ns: NOW_NS,
            deadline,
            row_window: false,
            max_rows: None,
            budgets: None,
        };
        let start = Instant::now();
        let outcome = executor
            .execute(tenant_hash, &req)
            .await
            .map_err(|e| format!("statement `{}` failed to execute: {e}", statement.id))?;
        let latency_ns = start.elapsed().as_nanos() as u64;
        let cache = outcome.stats.cache.as_ref().ok_or_else(|| {
            format!(
                "statement `{}` reported no stats.cache; the read cache is not attached",
                statement.id
            )
        })?;
        let acc = &outcome.accounting;
        executions.push(Execution {
            statement: idx,
            latency_ns,
            intent: cache.intent,
            hits: cache.interactive.hits + cache.bulk_scan.hits + cache.maintenance.hits,
            hit_bytes: cache.interactive.served_bytes
                + cache.bulk_scan.served_bytes
                + cache.maintenance.served_bytes,
            misses: cache.misses,
            gets: acc.s3_requests(AccountedOp::Get),
            wire_bytes: acc.s3_bytes(AccountedOp::Get),
            rows: outcome.output.num_rows(),
            digest: digest_rows(&outcome.output)?,
        });
    }
    Ok(executions)
}

/// Nearest-rank percentile of `sorted`, in milliseconds; 0 for no samples.
fn percentile_ms(sorted: &[u64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[rank.min(sorted.len() - 1)] as f64 / 1e6
}

fn served_share(hit_bytes: u64, wire_bytes: u64) -> f64 {
    let read = hit_bytes + wire_bytes;
    if read == 0 {
        0.0
    } else {
        hit_bytes as f64 / read as f64
    }
}

fn class_figures(connections: usize, executions: &[&Execution]) -> ClassFigures {
    let mut sorted: Vec<u64> = executions.iter().map(|e| e.latency_ns).collect();
    sorted.sort_unstable();
    let mut intents = IntentCounts::default();
    for e in executions {
        intents.add(e.intent);
    }
    let cache_hit_bytes = executions.iter().map(|e| e.hit_bytes).sum();
    let wire_bytes = executions.iter().map(|e| e.wire_bytes).sum();
    ClassFigures {
        connections,
        statements_completed: executions.len() as u64,
        p50_ms: percentile_ms(&sorted, 0.50),
        p95_ms: percentile_ms(&sorted, 0.95),
        p99_ms: percentile_ms(&sorted, 0.99),
        cache_hits: executions.iter().map(|e| e.hits).sum(),
        cache_hit_bytes,
        cache_misses: executions.iter().map(|e| e.misses).sum(),
        served_share: served_share(cache_hit_bytes, wire_bytes),
        object_store_gets: executions.iter().map(|e| e.gets).sum(),
        wire_bytes,
        intents,
    }
}

fn statement_figures(statements: &[Statement], executions: &[Execution]) -> Vec<StatementFigures> {
    statements
        .iter()
        .enumerate()
        .map(|(idx, statement)| {
            let mine: Vec<&Execution> = executions.iter().filter(|e| e.statement == idx).collect();
            let mut sorted: Vec<u64> = mine.iter().map(|e| e.latency_ns).collect();
            sorted.sort_unstable();
            let mut intents = IntentCounts::default();
            for e in &mine {
                intents.add(e.intent);
            }
            let digest = mine.first().map(|e| e.digest.clone()).unwrap_or_default();
            StatementFigures {
                id: statement.id.clone(),
                class: statement.class,
                completed: mine.len() as u64,
                p50_ms: percentile_ms(&sorted, 0.50),
                p95_ms: percentile_ms(&sorted, 0.95),
                p99_ms: percentile_ms(&sorted, 0.99),
                rows: mine.first().map(|e| e.rows).unwrap_or(0),
                digest_stable: mine.iter().all(|e| e.digest == digest),
                digest,
                intents,
            }
        })
        .collect()
}

/// Run one arm once: a fresh cache and executor, then every connection
/// looping until the duration elapses.
async fn run_arm(
    cfg: &MixedConfig,
    tenant_hash: TenantHash,
    declared: &[DeclaredColumn],
    statements: &Arc<Vec<Statement>>,
    arm: &Arm,
    repetition: usize,
) -> Result<ArmRun, Error> {
    let max_entries = (cfg.cache_bytes / 4096).max(64) as usize;
    let cache: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(
        CacheLimits::new(cfg.cache_bytes, max_entries, cfg.cache_bytes)
            .with_bulk_share(cfg.cache_bulk_share),
    ));
    let settings = ExecutorSettings {
        cache_bulk_scan_min_bytes: arm.bulk_scan_min_bytes,
        ..cfg.settings
    };
    let built = cold_executor(&cfg.store, declared, Some(cache), settings)?;
    let executor = Arc::new(built.executor);

    let start = Instant::now();
    let end = start + cfg.duration;
    let mut handles = Vec::with_capacity(cfg.background + cfg.foreground);
    for (class, count) in [
        (WorkloadClass::Foreground, cfg.foreground),
        (WorkloadClass::Background, cfg.background),
    ] {
        for offset in 0..count {
            handles.push(tokio::spawn(connection(
                Arc::clone(&executor),
                tenant_hash,
                Arc::clone(statements),
                class,
                offset,
                end,
                cfg.deadline,
            )));
        }
    }
    let mut executions = Vec::new();
    for handle in handles {
        executions.extend(
            handle
                .await
                .map_err(|e| format!("connection task failed: {e}"))??,
        );
    }
    let wall_secs = start.elapsed().as_secs_f64();

    let class_executions = |class: WorkloadClass| -> Vec<&Execution> {
        executions
            .iter()
            .filter(|e| statements[e.statement].class == class)
            .collect()
    };
    Ok(ArmRun {
        arm: arm.label.clone(),
        bulk_scan_min_bytes: arm.bulk_scan_min_bytes,
        repetition,
        wall_secs,
        foreground: class_figures(cfg.foreground, &class_executions(WorkloadClass::Foreground)),
        background: class_figures(cfg.background, &class_executions(WorkloadClass::Background)),
        statements: statement_figures(statements, &executions),
    })
}

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let n = values.len();
    if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    }
}

fn class_medians(figures: &[&ClassFigures]) -> ClassMedians {
    let med = |f: &dyn Fn(&ClassFigures) -> f64| {
        let mut values: Vec<f64> = figures.iter().map(|c| f(c)).collect();
        median(&mut values)
    };
    ClassMedians {
        statements_completed: med(&|c| c.statements_completed as f64),
        p50_ms: med(&|c| c.p50_ms),
        p95_ms: med(&|c| c.p95_ms),
        p99_ms: med(&|c| c.p99_ms),
        cache_hits: med(&|c| c.cache_hits as f64),
        cache_hit_bytes: med(&|c| c.cache_hit_bytes as f64),
        cache_misses: med(&|c| c.cache_misses as f64),
        served_share: med(&|c| c.served_share),
        object_store_gets: med(&|c| c.object_store_gets as f64),
        wire_bytes: med(&|c| c.wire_bytes as f64),
    }
}

/// Medians per arm, in the arms' configured order.
pub fn arm_medians(arms: &[Arm], runs: &[ArmRun]) -> Vec<ArmMedians> {
    arms.iter()
        .map(|arm| {
            let mine: Vec<&ArmRun> = runs.iter().filter(|r| r.arm == arm.label).collect();
            ArmMedians {
                arm: arm.label.clone(),
                repetitions: mine.len(),
                foreground: class_medians(&mine.iter().map(|r| &r.foreground).collect::<Vec<_>>()),
                background: class_medians(&mine.iter().map(|r| &r.background).collect::<Vec<_>>()),
            }
        })
        .collect()
}

/// The pre-registered bands (issue #3084), each arm after the first against
/// the first. With foreground connections: the foreground served share at
/// least the reference's plus 0.20 absolute, its p99 at most 0.80x the
/// reference's, and background statements completed at least 0.90x the
/// reference's. Without: the background served share at least 0.95x the
/// reference's. Plus one row for identical results across every run.
pub fn bands(medians: &[ArmMedians], foreground: usize, results: &ResultsCheck) -> Vec<BandRow> {
    let mut rows = Vec::new();
    let row = |id: &str, arm: &str, observed: f64, relation: &str, bound: f64| {
        let hit = match relation {
            ">=" => observed >= bound,
            _ => observed <= bound,
        };
        BandRow {
            id: id.to_string(),
            arm: arm.to_string(),
            observed,
            relation: relation.to_string(),
            bound,
            verdict: if hit { "HIT" } else { "MISS" }.to_string(),
        }
    };
    if let Some((reference, others)) = medians.split_first() {
        for arm in others {
            if foreground > 0 {
                rows.push(row(
                    "mixed_foreground_served_share",
                    &arm.arm,
                    arm.foreground.served_share,
                    ">=",
                    reference.foreground.served_share + 0.20,
                ));
                rows.push(row(
                    "mixed_foreground_p99_ms",
                    &arm.arm,
                    arm.foreground.p99_ms,
                    "<=",
                    reference.foreground.p99_ms * 0.80,
                ));
                rows.push(row(
                    "mixed_background_statements_completed",
                    &arm.arm,
                    arm.background.statements_completed,
                    ">=",
                    reference.background.statements_completed * 0.90,
                ));
            } else {
                rows.push(row(
                    "scans_only_background_served_share",
                    &arm.arm,
                    arm.background.served_share,
                    ">=",
                    reference.background.served_share * 0.95,
                ));
            }
        }
    }
    rows.push(BandRow {
        id: "results_identical".to_string(),
        arm: "all".to_string(),
        observed: results.differing.len() as f64,
        relation: "<=".to_string(),
        bound: 0.0,
        verdict: if results.identical { "HIT" } else { "MISS" }.to_string(),
    });
    rows
}

/// Every statement's digest across every run: one digest per statement, and
/// stable within each run.
pub fn results_check(runs: &[ArmRun]) -> ResultsCheck {
    let mut digests: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut differing: Vec<String> = Vec::new();
    for run in runs {
        for s in &run.statements {
            if s.completed == 0 {
                continue;
            }
            if !s.digest_stable && !differing.contains(&s.id) {
                differing.push(s.id.clone());
            }
            digests
                .entry(s.id.as_str())
                .or_default()
                .push(s.digest.as_str());
        }
    }
    for (id, seen) in digests {
        if seen.iter().any(|d| *d != seen[0]) && !differing.iter().any(|d| d == id) {
            differing.push(id.to_string());
        }
    }
    ResultsCheck {
        identical: differing.is_empty(),
        differing,
    }
}

/// Run the whole mode: generate the tenant once, then every repetition of
/// every arm, interleaved.
pub async fn run_mixed(cfg: &MixedConfig) -> Result<MixedReport, Error> {
    if cfg.cache_bytes == 0 {
        return Err("the mixed mode needs a read cache: pass --cache-bytes > 0".into());
    }
    if cfg.arms.is_empty() {
        return Err("the mixed mode needs at least one arm".into());
    }
    if cfg.background == 0 && cfg.foreground == 0 {
        return Err("the mixed mode needs at least one connection".into());
    }
    if cfg.window_objects == 0 {
        return Err("--mixed-window-objects must be at least 1".into());
    }
    let tenant = TenantId::new(format!("mixed-cache-{}", Uuid::new_v4()));
    let tenant_hash = tenant.hash();
    let load_start = Instant::now();
    let spans = generate(&cfg.store, &tenant, cfg).await?;
    let load_wall_ms = load_start.elapsed().as_secs_f64() * 1e3;
    if spans.len() < cfg.window_objects {
        return Err(format!(
            "the corpus has {} objects, fewer than the {} the foreground window covers",
            spans.len(),
            cfg.window_objects
        )
        .into());
    }
    let full_window = TimeRange {
        start_ns: 0,
        end_ns: NOW_NS,
    };
    let dataset = dataset_info(
        &cfg.store,
        tenant_hash,
        full_window,
        NOW_NS,
        Some(load_wall_ms),
        1,
    )
    .await?;
    if dataset.object_count != spans.len() {
        return Err(format!(
            "the catalog resolves {} objects but {} were written",
            dataset.object_count,
            spans.len()
        )
        .into());
    }
    let narrow = TimeRange {
        start_ns: spans[spans.len() - cfg.window_objects].min_ts,
        end_ns: spans[spans.len() - 1].max_ts,
    };
    let (resolved_objects, resolved_bytes) = resolved_set(&cfg.store, tenant_hash, narrow).await?;
    if resolved_objects != cfg.window_objects {
        return Err(format!(
            "the foreground window resolves {resolved_objects} objects, not the {} asked for",
            cfg.window_objects
        )
        .into());
    }
    if resolved_bytes >= cfg.cache_bytes {
        return Err(format!(
            "the foreground read set ({resolved_bytes} bytes) is not below the cache \
             ({} bytes); narrow --mixed-window-objects",
            cfg.cache_bytes
        )
        .into());
    }
    let interactive_window = InteractiveWindow {
        objects: cfg.window_objects,
        start_ns: narrow.start_ns,
        end_ns: narrow.end_ns,
        resolved_bytes,
    };

    let mut statements = foreground_statements(narrow);
    let mut background = background_statements(full_window);
    if !cfg.background_ids.is_empty() {
        for id in &cfg.background_ids {
            if !background.iter().any(|s| &s.id == id) {
                return Err(format!(
                    "no background statement `{id}`; the set is {}",
                    background
                        .iter()
                        .map(|s| s.id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
                .into());
            }
        }
        background.retain(|s| cfg.background_ids.contains(&s.id));
    }
    statements.extend(background);
    let statements = Arc::new(statements);
    let declared = declared_columns();

    let mut runs = Vec::with_capacity(cfg.repetitions * cfg.arms.len());
    for repetition in 0..cfg.repetitions.max(1) {
        for arm in &cfg.arms {
            runs.push(run_arm(cfg, tenant_hash, &declared, &statements, arm, repetition).await?);
        }
    }
    let medians = arm_medians(&cfg.arms, &runs);
    let results = results_check(&runs);
    let bands = bands(&medians, cfg.foreground, &results);
    let s = &cfg.settings;
    Ok(MixedReport {
        provenance: MixedProvenance {
            store_backend: cfg.store_backend.clone(),
            host_logical_cores: std::thread::available_parallelism().map_or(0, |n| n.get()),
            cache_bytes: cfg.cache_bytes,
            cache_bulk_share: cfg.cache_bulk_share,
            bulk_region_bytes: bulk_region_bytes(cfg.cache_bytes, cfg.cache_bulk_share),
            duration_secs: cfg.duration.as_secs_f64(),
            background: cfg.background,
            foreground: cfg.foreground,
            repetitions: cfg.repetitions.max(1),
            arms: cfg.arms.clone(),
            records: cfg.records,
            records_per_object: cfg.records_per_object,
            extra_attrs: cfg.extra_attrs,
            body_bytes: cfg.body_bytes,
            window_objects: cfg.window_objects,
            background_ids: statements
                .iter()
                .filter(|s| s.class == WorkloadClass::Background)
                .map(|s| s.id.clone())
                .collect(),
            deadline_secs: cfg.deadline.as_secs(),
            fetch_concurrency: s.fetch_concurrency.max(1),
            sql_partition_count_effective: s
                .sql_partition_count
                .unwrap_or(s.fetch_concurrency)
                .max(1),
            store_get_concurrency_effective: s
                .store_get_concurrency
                .unwrap_or(s.fetch_concurrency)
                .max(1),
            logs_fetch_policy: s.logs_fetch_policy.as_str().to_string(),
            logs_block_range_threshold_effective: s.logs_block_range_threshold,
            logs_request_cost_bytes_effective: s.logs_request_cost_bytes,
            sql_max_query_bytes: s.max_query_bytes,
            tenant_max_bytes: s.tenant_max_bytes,
            parallel_final_aggregation: s.parallel_final_aggregation,
        },
        dataset,
        interactive_window,
        runs,
        medians,
        bands,
        results,
    })
}

/// Refuse a report that is not a measurement: a class with connections that
/// completed no statement, or a class missing one of [`REQUIRED_FIGURES`].
/// Checked on the serialized JSON so the check reads what a consumer reads.
pub fn check_report(report: &MixedReport) -> Result<(), String> {
    let json = serde_json::to_value(report).map_err(|e| format!("serialize report: {e}"))?;
    check_report_json(&json)
}

/// [`check_report`] over an already serialized report.
pub fn check_report_json(json: &Value) -> Result<(), String> {
    let runs = json
        .get("runs")
        .and_then(Value::as_array)
        .ok_or("report has no `runs` array")?;
    if runs.is_empty() {
        return Err("report has no arm runs".to_string());
    }
    for run in runs {
        let arm = run.get("arm").and_then(Value::as_str).unwrap_or("?");
        let repetition = run.get("repetition").and_then(Value::as_u64).unwrap_or(0);
        for class in ["foreground", "background"] {
            let figures = run
                .get(class)
                .and_then(Value::as_object)
                .ok_or_else(|| format!("arm {arm} repetition {repetition}: no `{class}` object"))?;
            let connections = figures
                .get("connections")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    format!("arm {arm} repetition {repetition} {class}: no `connections`")
                })?;
            if connections == 0 {
                continue;
            }
            for figure in REQUIRED_FIGURES {
                let value = figures.get(figure).ok_or_else(|| {
                    format!("arm {arm} repetition {repetition} {class}: figure `{figure}` missing")
                })?;
                if !value.is_number() {
                    return Err(format!(
                        "arm {arm} repetition {repetition} {class}: figure `{figure}` is not a \
                         number"
                    ));
                }
            }
            let completed = figures
                .get("statements_completed")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if completed == 0 {
                return Err(format!(
                    "arm {arm} repetition {repetition} {class}: {connections} connection(s) \
                     completed zero statements"
                ));
            }
        }
    }
    Ok(())
}

/// The per-run table: one row per arm run and class, every required figure
/// as its own column, named as in the JSON.
pub fn render_runs_table(report: &MixedReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "  {:<4} {:>3} {:<10} {:>5} {:>20} {:>10} {:>10} {:>10} {:>10} {:>15} {:>12} {:>12} {:>17} {:>14} {:>14}",
        "arm",
        "rep",
        "class",
        "conns",
        REQUIRED_FIGURES[0],
        REQUIRED_FIGURES[1],
        REQUIRED_FIGURES[2],
        REQUIRED_FIGURES[3],
        REQUIRED_FIGURES[4],
        REQUIRED_FIGURES[5],
        REQUIRED_FIGURES[6],
        REQUIRED_FIGURES[7],
        REQUIRED_FIGURES[8],
        REQUIRED_FIGURES[9],
        "intents(i/b/m)",
    );
    for run in &report.runs {
        for (class, f) in [
            ("foreground", &run.foreground),
            ("background", &run.background),
        ] {
            if f.connections == 0 {
                continue;
            }
            let _ = writeln!(
                out,
                "  {:<4} {:>3} {:<10} {:>5} {:>20} {:>10.1} {:>10.1} {:>10.1} {:>10} {:>15} {:>12} {:>12.4} {:>17} {:>14} {:>14}",
                run.arm,
                run.repetition,
                class,
                f.connections,
                f.statements_completed,
                f.p50_ms,
                f.p95_ms,
                f.p99_ms,
                f.cache_hits,
                f.cache_hit_bytes,
                f.cache_misses,
                f.served_share,
                f.object_store_gets,
                f.wire_bytes,
                format!(
                    "{}/{}/{}",
                    f.intents.interactive, f.intents.bulk_scan, f.intents.maintenance
                ),
            );
        }
    }
    out
}

/// Check the rendered per-run table carries every figure exactly once as a
/// column and exactly one row per arm run and class with connections.
pub fn check_runs_table(table: &str, report: &MixedReport) -> Result<(), String> {
    let mut lines = table.lines();
    let header = lines.next().ok_or("the runs table is empty")?;
    for figure in REQUIRED_FIGURES {
        let count = header.split_whitespace().filter(|w| *w == figure).count();
        if count != 1 {
            return Err(format!(
                "figure `{figure}` appears {count} times in the runs table header, not once"
            ));
        }
    }
    let rows: Vec<&str> = lines.collect();
    for run in &report.runs {
        for (class, f) in [
            ("foreground", &run.foreground),
            ("background", &run.background),
        ] {
            if f.connections == 0 {
                continue;
            }
            let count = rows
                .iter()
                .filter(|row| {
                    let mut words = row.split_whitespace();
                    words.next() == Some(run.arm.as_str())
                        && words.next() == Some(run.repetition.to_string().as_str())
                        && words.next() == Some(class)
                })
                .count();
            if count != 1 {
                return Err(format!(
                    "arm {} repetition {} {class} has {count} rows in the runs table, not one",
                    run.arm, run.repetition
                ));
            }
        }
    }
    Ok(())
}

/// The whole human-readable report: provenance, the per-run table, the
/// per-arm medians, the band rows and the results check.
pub fn render_report(report: &MixedReport) -> String {
    let p = &report.provenance;
    let d = &report.dataset;
    let w = &report.interactive_window;
    let mut out = String::new();
    let _ = writeln!(out, "\nmixed cache admission report");
    let _ = writeln!(
        out,
        "  backend    : {} ({} logical cores)",
        p.store_backend, p.host_logical_cores
    );
    let _ = writeln!(
        out,
        "  corpus     : {} objects, {} bytes on object, {} rows, load={:.1}ms",
        d.object_count,
        d.stored_bytes,
        d.rows,
        d.load_wall_ms.unwrap_or(0.0)
    );
    let _ = writeln!(
        out,
        "  cache      : {} bytes, bulk_share={}, bulk region {} bytes",
        p.cache_bytes, p.cache_bulk_share, p.bulk_region_bytes
    );
    let _ = writeln!(
        out,
        "  window     : foreground over the last {} object(s), [{}, {}] ns, {} resolved bytes",
        w.objects, w.start_ns, w.end_ns, w.resolved_bytes
    );
    let arms: Vec<String> = p
        .arms
        .iter()
        .map(|a| {
            format!(
                "{}={}",
                a.label,
                a.bulk_scan_min_bytes
                    .map_or("unset".to_string(), |b| b.to_string())
            )
        })
        .collect();
    let _ = writeln!(out, "  arms       : {}", arms.join(" "));
    let _ = writeln!(
        out,
        "  workload   : background={} foreground={} duration={}s repetitions={} \
         fetch_concurrency={} policy={}",
        p.background,
        p.foreground,
        p.duration_secs,
        p.repetitions,
        p.fetch_concurrency,
        p.logs_fetch_policy
    );
    let _ = writeln!(out, "\nper arm run and class");
    out.push_str(&render_runs_table(report));
    let _ = writeln!(
        out,
        "\nper statement (completed, p50/p95/p99 ms, rows, intents i/b/m)"
    );
    for run in &report.runs {
        for s in &run.statements {
            let _ = writeln!(
                out,
                "  {:<4} {:>3} {:<18} {:>8} {:>10.1} {:>10.1} {:>10.1} {:>6} {:>10} digest={}{}",
                run.arm,
                run.repetition,
                s.id,
                s.completed,
                s.p50_ms,
                s.p95_ms,
                s.p99_ms,
                s.rows,
                format!(
                    "{}/{}/{}",
                    s.intents.interactive, s.intents.bulk_scan, s.intents.maintenance
                ),
                &s.digest[..s.digest.len().min(12)],
                if s.digest_stable { "" } else { " UNSTABLE" }
            );
        }
    }
    let _ = writeln!(out, "\nmedians over repetitions");
    let _ = writeln!(
        out,
        "  {:<4} {:<10} {:>12} {:>10} {:>10} {:>10} {:>12} {:>15} {:>12} {:>12} {:>12} {:>14}",
        "arm",
        "class",
        "completed",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "hits",
        "hit_bytes",
        "misses",
        "served",
        "gets",
        "wire_bytes"
    );
    for m in &report.medians {
        for (class, c, conns) in [
            ("foreground", &m.foreground, p.foreground),
            ("background", &m.background, p.background),
        ] {
            if conns == 0 {
                continue;
            }
            let _ = writeln!(
                out,
                "  {:<4} {:<10} {:>12.1} {:>10.1} {:>10.1} {:>10.1} {:>12.0} {:>15.0} {:>12.0} {:>12.4} {:>12.0} {:>14.0}",
                m.arm,
                class,
                c.statements_completed,
                c.p50_ms,
                c.p95_ms,
                c.p99_ms,
                c.cache_hits,
                c.cache_hit_bytes,
                c.cache_misses,
                c.served_share,
                c.object_store_gets,
                c.wire_bytes
            );
        }
    }
    let _ = writeln!(out, "\nbands");
    for b in &report.bands {
        let _ = writeln!(
            out,
            "  {:<4} {:<40} observed {:>14.4} {} {:>14.4}  {}",
            b.arm, b.id, b.observed, b.relation, b.bound, b.verdict
        );
    }
    let _ = writeln!(
        out,
        "\nresults    : {}",
        if report.results.identical {
            "identical rows per statement in every arm run".to_string()
        } else {
            format!("DIFFERING: {}", report.results.differing.join(", "))
        }
    );
    out
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn figures(completed: u64) -> ClassFigures {
        ClassFigures {
            connections: 1,
            statements_completed: completed,
            p50_ms: 1.0,
            p95_ms: 2.0,
            p99_ms: 3.0,
            cache_hits: 4,
            cache_hit_bytes: 400,
            cache_misses: 1,
            served_share: 0.8,
            object_store_gets: 1,
            wire_bytes: 100,
            intents: IntentCounts::default(),
        }
    }

    fn run(arm: &str, repetition: usize) -> ArmRun {
        ArmRun {
            arm: arm.to_string(),
            bulk_scan_min_bytes: None,
            repetition,
            wall_secs: 1.0,
            foreground: figures(10),
            background: figures(2),
            statements: Vec::new(),
        }
    }

    fn report(runs: Vec<ArmRun>) -> MixedReport {
        let arms = vec![
            Arm {
                label: "A".to_string(),
                bulk_scan_min_bytes: None,
            },
            Arm {
                label: "B".to_string(),
                bulk_scan_min_bytes: Some(10),
            },
        ];
        let medians = arm_medians(&arms, &runs);
        let results = results_check(&runs);
        let bands = bands(&medians, 1, &results);
        MixedReport {
            provenance: MixedProvenance {
                store_backend: "memory".to_string(),
                host_logical_cores: 1,
                cache_bytes: 100,
                cache_bulk_share: 0.5,
                bulk_region_bytes: 50,
                duration_secs: 1.0,
                background: 1,
                foreground: 1,
                repetitions: 1,
                arms,
                records: 1,
                records_per_object: 1,
                extra_attrs: 0,
                body_bytes: 1,
                window_objects: 1,
                background_ids: Vec::new(),
                deadline_secs: 1,
                fetch_concurrency: 1,
                sql_partition_count_effective: 1,
                store_get_concurrency_effective: 1,
                logs_fetch_policy: "cost-based".to_string(),
                logs_block_range_threshold_effective: 0,
                logs_request_cost_bytes_effective: 0,
                sql_max_query_bytes: 1,
                tenant_max_bytes: 1,
                parallel_final_aggregation: true,
            },
            dataset: DatasetInfo {
                load_wall_ms: None,
                stored_bytes: 1,
                object_count: 1,
                rows: 1,
                layout: "pre".to_string(),
            },
            interactive_window: InteractiveWindow {
                objects: 1,
                start_ns: 0,
                end_ns: 1,
                resolved_bytes: 1,
            },
            runs,
            medians,
            bands,
            results,
        }
    }

    /// Dropping any one required figure from a class fails the check that the
    /// bin exits on, naming the figure; a complete report passes.
    #[test]
    fn a_report_missing_one_figure_is_refused() {
        let r = report(vec![run("A", 0), run("B", 0)]);
        check_report(&r).expect("complete report passes");
        for figure in REQUIRED_FIGURES {
            let mut json = serde_json::to_value(&r).unwrap();
            let class = json["runs"][1]["background"].as_object_mut().unwrap();
            assert!(class.remove(figure).is_some(), "{figure} present to remove");
            let err = check_report_json(&json).expect_err("dropped figure is refused");
            assert!(err.contains(figure), "{err} names {figure}");
            assert!(err.contains("arm B repetition 0 background"), "{err}");
        }
    }

    /// A class with connections that completed nothing is refused; a class
    /// with no connections (the scans-only shape) is not.
    #[test]
    fn a_class_that_completed_zero_statements_is_refused() {
        let mut r = report(vec![run("A", 0)]);
        r.runs[0].foreground.statements_completed = 0;
        let err = check_report(&r).expect_err("zero completed is refused");
        assert!(err.contains("completed zero statements"), "{err}");
        r.runs[0].foreground.connections = 0;
        check_report(&r).expect("a class with no connections is skipped");
        let mut json = serde_json::to_value(&r).unwrap();
        json.as_object_mut().unwrap().remove("runs");
        assert!(check_report_json(&json).is_err());
    }

    /// The runs table carries each figure once as a column and one row per
    /// run and class; a duplicated row or a dropped column is refused.
    #[test]
    fn the_runs_table_has_each_figure_once_and_one_row_per_run_and_class() {
        let r = report(vec![run("A", 0), run("B", 0)]);
        let table = render_runs_table(&r);
        check_runs_table(&table, &r).expect("rendered table passes");
        let duplicated = format!("{table}{}", table.lines().nth(1).unwrap());
        assert!(check_runs_table(&duplicated, &r).is_err());
        let dropped = table.replacen("served_share", "served", 1);
        let err = check_runs_table(&dropped, &r).expect_err("dropped column");
        assert!(err.contains("served_share"), "{err}");
        let doubled = table.replacen("wire_bytes", "wire_bytes wire_bytes", 1);
        assert!(check_runs_table(&doubled, &r).is_err());
        assert!(check_runs_table("", &r).is_err());
    }

    #[test]
    fn arms_parse_the_keywords_and_byte_counts_against_the_cache() {
        let arms = parse_arms("A=unset, B=region ,C=cache,D=12345", 1_000, 0.5).unwrap();
        assert_eq!(
            arms.iter()
                .map(|a| (a.label.as_str(), a.bulk_scan_min_bytes))
                .collect::<Vec<_>>(),
            vec![
                ("A", None),
                ("B", Some(500)),
                ("C", Some(1_000)),
                ("D", Some(12_345))
            ]
        );
        assert!(parse_arms("A", 1, 0.5).is_err());
        assert!(parse_arms("A=unset,A=cache", 1, 0.5).is_err());
        assert!(parse_arms("A=unset,,B=cache", 1, 0.5).is_err());
        assert!(parse_arms("=unset", 1, 0.5).is_err());
        assert!(parse_arms("A=lots", 1, 0.5).is_err());
        assert_eq!(bulk_region_bytes(1_000, f64::NAN), 500);
        assert_eq!(bulk_region_bytes(1_000, 7.0), 1_000);
    }

    /// Bands compare each later arm against the first, the mixed rows with
    /// foreground connections and the scans-only row without, and the results
    /// row reports a differing digest.
    #[test]
    fn bands_follow_the_pre_registered_rows() {
        let mut a = run("A", 0);
        a.foreground.served_share = 0.5;
        a.foreground.p99_ms = 100.0;
        a.background.statements_completed = 10;
        let mut b = run("B", 0);
        b.foreground.served_share = 0.7;
        b.foreground.p99_ms = 80.0;
        b.background.statements_completed = 9;
        let mut c = run("C", 0);
        c.foreground.served_share = 0.69;
        c.foreground.p99_ms = 81.0;
        c.background.statements_completed = 8;
        let arms: Vec<Arm> = ["A", "B", "C"]
            .iter()
            .map(|l| Arm {
                label: l.to_string(),
                bulk_scan_min_bytes: None,
            })
            .collect();
        let medians = arm_medians(&arms, &[a.clone(), b.clone(), c.clone()]);
        let ok = ResultsCheck {
            identical: true,
            differing: Vec::new(),
        };
        let rows = bands(&medians, 4, &ok);
        let verdict = |id: &str, arm: &str| {
            rows.iter()
                .find(|r| r.id == id && r.arm == arm)
                .map(|r| r.verdict.as_str())
                .unwrap()
        };
        assert_eq!(verdict("mixed_foreground_served_share", "B"), "HIT");
        assert_eq!(verdict("mixed_foreground_p99_ms", "B"), "HIT");
        assert_eq!(verdict("mixed_background_statements_completed", "B"), "HIT");
        assert_eq!(verdict("mixed_foreground_served_share", "C"), "MISS");
        assert_eq!(verdict("mixed_foreground_p99_ms", "C"), "MISS");
        assert_eq!(
            verdict("mixed_background_statements_completed", "C"),
            "MISS"
        );
        assert_eq!(verdict("results_identical", "all"), "HIT");
        assert_eq!(rows.len(), 7);

        let scans_only = bands(
            &medians,
            0,
            &ResultsCheck {
                identical: false,
                differing: vec!["bg_filter_count".to_string()],
            },
        );
        assert_eq!(scans_only.len(), 3);
        assert!(
            scans_only.iter().all(
                |r| r.id == "scans_only_background_served_share" || r.id == "results_identical"
            )
        );
        assert_eq!(scans_only[2].verdict, "MISS");
    }

    /// Medians are over repetitions per arm; an even count averages the two
    /// middle values.
    #[test]
    fn medians_are_per_arm_over_repetitions() {
        let mut a0 = run("A", 0);
        a0.foreground.p99_ms = 10.0;
        let mut a1 = run("A", 1);
        a1.foreground.p99_ms = 30.0;
        let mut b0 = run("B", 0);
        b0.foreground.p99_ms = 5.0;
        let arms: Vec<Arm> = ["A", "B"]
            .iter()
            .map(|l| Arm {
                label: l.to_string(),
                bulk_scan_min_bytes: None,
            })
            .collect();
        let m = arm_medians(&arms, &[a0, b0, a1]);
        assert_eq!(m[0].repetitions, 2);
        assert_eq!(m[0].foreground.p99_ms, 20.0);
        assert_eq!(m[1].repetitions, 1);
        assert_eq!(m[1].foreground.p99_ms, 5.0);
        let mut odd = vec![3.0, 1.0, 2.0];
        assert_eq!(median(&mut odd), 2.0);
        assert_eq!(median(&mut []), 0.0);
    }

    /// A statement whose digest differs between runs, or within one run, is
    /// named by the results check.
    #[test]
    fn results_check_names_a_statement_whose_rows_changed() {
        let figure = |digest: &str, stable: bool| StatementFigures {
            id: "fg_error_count".to_string(),
            class: WorkloadClass::Foreground,
            completed: 2,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            rows: 1,
            digest: digest.to_string(),
            digest_stable: stable,
            intents: IntentCounts::default(),
        };
        let mut a = run("A", 0);
        a.statements = vec![figure("x", true)];
        let mut b = run("B", 0);
        b.statements = vec![figure("x", true)];
        assert!(results_check(&[a.clone(), b.clone()]).identical);
        b.statements[0].digest = "y".to_string();
        let differing = results_check(&[a.clone(), b]);
        assert!(!differing.identical);
        assert_eq!(differing.differing, vec!["fg_error_count"]);
        a.statements[0].digest_stable = false;
        assert_eq!(results_check(&[a]).differing, vec!["fg_error_count"]);
    }

    #[test]
    fn percentiles_are_nearest_rank_in_milliseconds() {
        let sorted: Vec<u64> = (1..=100).map(|i| i * 1_000_000).collect();
        assert_eq!(percentile_ms(&sorted, 0.50), 51.0);
        assert_eq!(percentile_ms(&sorted, 0.99), 99.0);
        assert_eq!(percentile_ms(&[], 0.5), 0.0);
        assert_eq!(served_share(0, 0), 0.0);
        assert_eq!(served_share(3, 1), 0.75);
    }

    #[test]
    fn bodies_are_the_asked_length_and_differ_per_record() {
        assert_eq!(body(1, 100).len(), 100);
        assert_eq!(body(7, 5).len(), 5);
        assert_ne!(body(1, 64), body(2, 64));
        assert_eq!(body(3, 64), body(3, 64));
    }
}
