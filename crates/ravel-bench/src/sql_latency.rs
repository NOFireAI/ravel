//! Per-query SQL latency harness over the ADR-0100 corpus (decision 4).
//!
//! Where `query_latency` times the PromQL evaluator, this module times the SQL
//! executor one corpus statement at a time and reports, per statement, the
//! minimum / median / maximum of `--runs` executions with the first flagged
//! cold, the rows it returned, and the scan diagnostics the executor already
//! computes (`SqlStats` block counters plus the query's `QueryAccounting`
//! object-store request/byte and cache counters). A stopwatch says a query is
//! slow; those counters say where the time went.
//!
//! Two dataset sources feed the same measurement core ([`measure_corpus`]):
//!
//! - [`run_generated`] builds a wide-schema logs dataset in process (the way
//!   `flight_sql_egress` publishes its dataset: write RLOG objects and their
//!   commit records straight to the store) and installs the union of the
//!   corpus's `required_declarations` through [`StaticDeclaredColumns`], which
//!   sidesteps the server cache's staleness horizon. This is the lane the smoke
//!   test exercises.
//! - [`run_tenant`] runs against a tenant already loaded in the configured
//!   object store (by `ravel-cli load --parquet`). It does *not* install
//!   declarations; it resolves the tenant's real durable declaration
//!   ([`ravel_catalog::read_config_values`]), because that is the configuration
//!   under measurement, and it *verifies* every entry's `required_declarations`
//!   against what resolved, skipping (never running) any entry whose declared
//!   column is absent: a missing declared column projects NULL for every row,
//!   so the query would return wrong numbers with a plausible latency instead
//!   of an error.
//!
//! Report-only: like the rest of `ravel-bench`, this never changes library
//! behavior, it only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use ravel_catalog::{Catalog, CatalogConfig, DeclaredColumnType, read_config_values};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    StaticDeclaredColumns,
};
use ravel_types::accounting::AccountedOp;
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::sql_corpus::CorpusEntry;

/// A frozen query clock, bounding the catalog resolve's ingest-hour fan-out.
/// The generated data lands in ingest-hour bucket 0 with event timestamps a few
/// microseconds after the epoch, so a resolve over `[0, NOW_NS]` lists a handful
/// of buckets per shard rather than a wall-clock number of them (the same
/// technique `flight_sql_egress` uses).
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// Anything the harness can fail on. Boxed so `?` composes over the executor,
/// catalog, writer, publish, and tenant-config error types without a bespoke
/// enum; a bench never needs to match on the variant.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Whether the measured object layout is a freshly loaded tenant (many small
/// objects) or one the maintenance machinery has compacted (fewer, larger).
/// ADR-0100 decision 4 requires the report to *state* this rather than guess,
/// so the `--tenant` lane takes it as an operator-supplied flag; the generated
/// lane is always freshly written and never compacted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compaction {
    /// A freshly loaded (or freshly generated) layout, never compacted.
    Pre,
    /// A layout the maintenance machinery has compacted.
    Post,
}

impl Compaction {
    fn label(self) -> &'static str {
        match self {
            Compaction::Pre => "pre-compaction",
            Compaction::Post => "post-compaction",
        }
    }
}

/// Run provenance: without it two latency tables are not comparable (the same
/// corpus against local memory and against S3 differs by an order of
/// magnitude), so the backend, host shape, and dataset identity are recorded
/// beside the numbers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Provenance {
    /// Object-store backend that actually ran: `"memory"`, `"minio"`, or
    /// `"s3"`. The caller classifies it (a custom S3 endpoint is MinIO), since
    /// the store is handed in already constructed.
    pub store_backend: String,
    /// Backend region, or the sentinel `"n/a"` for a backend with none
    /// (`MemoryStore`), keeping the no-null contract.
    pub region: String,
    /// Backend endpoint, or `"n/a"` when the backend has none.
    pub endpoint: String,
    /// Logical cores on the measuring host.
    pub host_logical_cores: usize,
    /// Which lane produced the run: `"generate"` or `"tenant"`.
    pub source: String,
    /// The tenant the numbers describe (a generated run mints a unique id).
    pub dataset_id: String,
    /// Executions per statement behind the min/median/max.
    pub runs: usize,
}

/// The dataset as it sits in the object store, independent of any one query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetInfo {
    /// Wall time to build and publish the dataset. Measured for the generated
    /// lane; `0.0` for the `--tenant` lane, whose load happened out of process
    /// through `ravel-cli` and is not this harness's to time.
    pub load_wall_ms: f64,
    /// Bytes stored across the dataset's data objects (summed over the resolved
    /// snapshot's segments).
    pub stored_bytes: u64,
    /// Data objects the dataset comprises: the count of segments a full-window
    /// resolve returns. Per-object cost (LIST, footer read, decode setup) is
    /// paid once per object per query, so this is a first-class figure, not
    /// decoration.
    pub object_count: usize,
    /// Durable rows across the dataset (summed segment sample counts).
    pub rows: u64,
    /// `"pre-compaction"` or `"post-compaction"`: which layout was measured.
    pub layout: String,
}

/// The scan diagnostics for one statement, read off the executor's own
/// counters: block pruning selectivity plus the object-store traffic and cache
/// behavior of the cold run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanDiagnostics {
    /// Segments in the snapshot the successful attempt used.
    pub segments: usize,
    /// Blocks the logs scan saw.
    pub blocks_total: u64,
    /// Blocks the logs scan actually read.
    pub blocks_scanned: u64,
    /// Blocks pruned by POSTINGS before any read (ADR-0049).
    pub blocks_pruned_by_postings: u64,
    /// Object-store GET requests the cold run issued.
    pub object_store_get_requests: u64,
    /// Object-store LIST requests the cold run issued.
    pub object_store_list_requests: u64,
    /// Bytes transferred from the object store across every operation kind.
    pub object_store_bytes: u64,
    /// Fetch-cache hits on the cold run.
    pub cache_hits: u64,
    /// Fetch-cache misses on the cold run.
    pub cache_misses: u64,
    /// Bytes served from the fetch cache on the cold run.
    pub cache_bytes: u64,
}

/// One measured statement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntryReport {
    /// The corpus entry id.
    pub id: String,
    /// Fastest of the `runs` executions, in milliseconds.
    pub min_ms: f64,
    /// Median of the `runs` executions (nearest-rank), in milliseconds.
    pub median_ms: f64,
    /// Slowest of the `runs` executions, in milliseconds.
    pub max_ms: f64,
    /// The first (cold) execution against a fresh catalog and executor, in
    /// milliseconds.
    pub cold_ms: f64,
    /// Rows the statement returned.
    pub rows_returned: usize,
    /// Where the time went.
    pub scan: ScanDiagnostics,
}

/// One statement that was not run because the dataset does not satisfy its
/// declared-column dependency. Distinct from a zero-latency measurement: a skip
/// carries no timing at all.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SkippedEntry {
    /// The corpus entry id.
    pub id: String,
    /// The declared attribute key that was missing (or present at the wrong
    /// type).
    pub missing_key: String,
    /// A human-readable reason naming the key and the type it needed.
    pub reason: String,
}

/// The full report: provenance, dataset shape, measured statements, and the
/// statements skipped for an unsatisfied declared-column dependency.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlLatencyReport {
    pub provenance: Provenance,
    pub dataset: DatasetInfo,
    pub entries: Vec<EntryReport>,
    pub skipped: Vec<SkippedEntry>,
}

/// Inputs for the generated lane.
pub struct GenerateConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_backend: String,
    pub region: String,
    pub endpoint: String,
    /// Statements to measure (the checked-in corpus, or an external file).
    pub entries: Vec<CorpusEntry>,
    /// Executions per statement.
    pub runs: usize,
    /// Distinct log records to generate.
    pub records: usize,
    /// Records per RLOG object; the record set is split into
    /// `ceil(records / records_per_object)` objects, so this is the lever that
    /// makes object count a measurable variable in process.
    pub records_per_object: usize,
    /// Extra distinct filler attribute keys per record, on top of the fixed
    /// `duration_ms` declared column, to widen the schema.
    pub extra_attrs: usize,
}

/// Inputs for the loaded-tenant lane.
pub struct TenantConfigInput {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_backend: String,
    pub region: String,
    pub endpoint: String,
    /// The tenant already loaded in `store`.
    pub tenant: String,
    /// Statements to measure.
    pub entries: Vec<CorpusEntry>,
    /// Executions per statement.
    pub runs: usize,
    /// Event-time window handed to the catalog resolve.
    pub window: TimeRange,
    /// Injected clock reading bounding that window.
    pub now_ns: i64,
    /// Which layout the operator is measuring.
    pub compaction: Compaction,
}

fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

fn host_logical_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// The first `required_declaration` of `entry` that `declared` does not satisfy
/// (absent key, or present at the wrong type), or `None` if every requirement
/// is met. Shared by both lanes: the generated lane installs the union so this
/// returns `None`, the tenant lane resolves the durable set so it may not.
fn first_unsatisfied(
    entry: &CorpusEntry,
    declared: &[DeclaredColumn],
) -> Option<(String, DeclaredType)> {
    for req in &entry.required_declarations {
        let want = req.ty.as_declared_type();
        let satisfied = declared.iter().any(|d| d.key == req.key && d.ty == want);
        if !satisfied {
            return Some((req.key.clone(), want));
        }
    }
    None
}

/// The union of every entry's `required_declarations`, as a declared-column
/// set to install for the generated lane. A key required at two types by two
/// entries keeps its first-seen type; the corpus never does that (the gate
/// would still run, but the second entry would then be skipped, which the smoke
/// test would catch).
fn declaration_union(entries: &[CorpusEntry]) -> Vec<DeclaredColumn> {
    let mut out: Vec<DeclaredColumn> = Vec::new();
    for entry in entries {
        for req in &entry.required_declarations {
            let ty = req.ty.as_declared_type();
            if !out.iter().any(|d| d.key == req.key) {
                out.push(DeclaredColumn::new(req.key.clone(), ty));
            }
        }
    }
    out
}

/// Build a fresh executor over `store` with `declared` installed. Fresh per
/// corpus entry so the first run is genuinely cold: a shared executor would let
/// one statement warm the next through the catalog and fetch caches.
fn cold_executor(
    store: &Arc<dyn ObjectStoreBackend>,
    declared: &[DeclaredColumn],
) -> Result<SqlExecutor, Error> {
    let catalog = Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default())?);
    Ok(SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared.to_vec()))))
}

/// Measure `entries` against a dataset already durable in `store`, using
/// `declared` as the tenant's declared-column configuration. This is the shared
/// core both lanes drive; they differ only in how the dataset was written and
/// how `declared` was obtained.
///
/// Each entry gets a fresh [`cold_executor`] and is executed `runs` times; the
/// first run is the cold number. An entry whose `required_declarations` are not
/// all satisfied by `declared` is skipped with its missing key named and never
/// executed.
pub async fn measure_corpus(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    entries: &[CorpusEntry],
    declared: &[DeclaredColumn],
    runs: usize,
    window: TimeRange,
    now_ns: i64,
) -> Result<(Vec<EntryReport>, Vec<SkippedEntry>), Error> {
    let runs = runs.max(1);
    let mut measured = Vec::new();
    let mut skipped = Vec::new();

    for entry in entries {
        // Verify the declared-column dependency before running anything. A
        // missing declared column reads NULL for every row, so an unsatisfied
        // entry must be skipped, not run: removing this guard lets the entry
        // execute and report a plausible-but-wrong latency.
        if let Some((missing_key, want)) = first_unsatisfied(entry, declared) {
            skipped.push(SkippedEntry {
                id: entry.id.clone(),
                missing_key: missing_key.clone(),
                reason: format!(
                    "required declared column `{missing_key}` ({want:?}) is not satisfied by the \
                     dataset under measurement"
                ),
            });
            continue;
        }

        let executor = cold_executor(store, declared)?;
        let req = SqlRequest {
            sql: entry.sql.clone(),
            window,
            min_tokens: Vec::new(),
            now_ns,
            deadline: Duration::from_secs(30),
        };

        let mut latencies_ns = Vec::with_capacity(runs);
        let mut cold_ns = 0u64;
        let mut rows_returned = 0usize;
        let mut scan = ScanDiagnostics {
            segments: 0,
            blocks_total: 0,
            blocks_scanned: 0,
            blocks_pruned_by_postings: 0,
            object_store_get_requests: 0,
            object_store_list_requests: 0,
            object_store_bytes: 0,
            cache_hits: 0,
            cache_misses: 0,
            cache_bytes: 0,
        };

        for run in 0..runs {
            let start = Instant::now();
            let outcome = executor
                .execute(tenant_hash, &req)
                .await
                .map_err(|e| Error::from(format!("entry `{}` failed to execute: {e}", entry.id)))?;
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            latencies_ns.push(elapsed_ns);
            if run == 0 {
                // The cold run's counters are the informative ones: it pays the
                // object-store traffic and cache misses a warm repeat elides.
                // Block counters are deterministic across runs regardless.
                cold_ns = elapsed_ns;
                rows_returned = outcome.output.num_rows();
                let acc = &outcome.accounting;
                scan = ScanDiagnostics {
                    segments: outcome.stats.segments,
                    blocks_total: outcome.stats.blocks_total,
                    blocks_scanned: outcome.stats.blocks_scanned,
                    blocks_pruned_by_postings: outcome.stats.blocks_pruned_by_postings,
                    object_store_get_requests: acc.s3_requests(AccountedOp::Get),
                    object_store_list_requests: acc.s3_requests(AccountedOp::List),
                    object_store_bytes: acc.total_s3_bytes(),
                    cache_hits: acc.cache_hits,
                    cache_misses: acc.cache_misses,
                    cache_bytes: acc.cache_bytes,
                };
            }
        }

        let mut sorted = latencies_ns.clone();
        sorted.sort_unstable();
        let to_ms = |ns: u64| ns as f64 / 1e6;
        measured.push(EntryReport {
            id: entry.id.clone(),
            min_ms: to_ms(*sorted.first().unwrap()),
            median_ms: to_ms(percentile(&sorted, 0.50)),
            max_ms: to_ms(*sorted.last().unwrap()),
            cold_ms: to_ms(cold_ns),
            rows_returned,
            scan,
        });
    }

    Ok((measured, skipped))
}

/// Resolve the dataset-level figures (bytes, object count, rows) from a
/// full-window catalog resolve over the logs signal. Shared by both lanes so
/// object count is defined identically however the dataset was written.
async fn dataset_info(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    window: TimeRange,
    now_ns: i64,
    load_wall_ms: f64,
    compaction: Compaction,
) -> Result<DatasetInfo, Error> {
    let catalog = Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default())?);
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Logs, window, &[], now_ns)
        .await?;
    let stored_bytes = snapshot.segments.iter().map(|s| s.object_size).sum();
    let rows = snapshot.segments.iter().map(|s| s.sample_count).sum();
    Ok(DatasetInfo {
        load_wall_ms,
        stored_bytes,
        object_count: snapshot.segments.len(),
        rows,
        layout: compaction.label().to_string(),
    })
}

/// Run the generated lane: build a wide-schema logs dataset in process, install
/// the corpus's declared-column union, and measure every statement.
pub async fn run_generated(cfg: &GenerateConfig) -> Result<SqlLatencyReport, Error> {
    // Unique tenant per run so repeated runs against one shared bucket never
    // read each other's objects (mirrors the other bench cores).
    let tenant = TenantId::new(format!("sql-latency-gen-{}", Uuid::new_v4()));
    let tenant_hash = tenant.hash();

    let load_start = Instant::now();
    generate_dataset(&cfg.store, &tenant, cfg).await?;
    let load_wall_ms = load_start.elapsed().as_secs_f64() * 1e3;

    let window = TimeRange {
        start_ns: 0,
        end_ns: NOW_NS,
    };
    let declared = declaration_union(&cfg.entries);
    let dataset = dataset_info(
        &cfg.store,
        tenant_hash,
        window,
        NOW_NS,
        load_wall_ms,
        Compaction::Pre,
    )
    .await?;
    let (entries, skipped) = measure_corpus(
        &cfg.store,
        tenant_hash,
        &cfg.entries,
        &declared,
        cfg.runs,
        window,
        NOW_NS,
    )
    .await?;

    Ok(SqlLatencyReport {
        provenance: Provenance {
            store_backend: cfg.store_backend.clone(),
            region: cfg.region.clone(),
            endpoint: cfg.endpoint.clone(),
            host_logical_cores: host_logical_cores(),
            source: "generate".to_string(),
            dataset_id: tenant.as_str().to_string(),
            runs: cfg.runs.max(1),
        },
        dataset,
        entries,
        skipped,
    })
}

/// Run the loaded-tenant lane: resolve the tenant's real durable declaration,
/// verify each entry against it, and measure the statements it satisfies.
///
/// Kept deliberately thin over [`measure_corpus`]/[`dataset_info`]: the only
/// lane-specific step is resolving the durable declaration, so the generated
/// lane (which the smoke test drives) exercises the rest of this path too.
pub async fn run_tenant(cfg: &TenantConfigInput) -> Result<SqlLatencyReport, Error> {
    let tenant = TenantId::new(cfg.tenant.clone());
    let tenant_hash = tenant.hash();

    // The configuration under measurement: the tenant's real durable declared
    // columns, not a set this harness installs. An absent config, or a config
    // with no typed columns, means the tenant declared nothing.
    let declared = resolve_durable_declarations(&cfg.store, &tenant_hash).await?;

    let dataset = dataset_info(
        &cfg.store,
        tenant_hash,
        cfg.window,
        cfg.now_ns,
        0.0,
        cfg.compaction,
    )
    .await?;
    let (entries, skipped) = measure_corpus(
        &cfg.store,
        tenant_hash,
        &cfg.entries,
        &declared,
        cfg.runs,
        cfg.window,
        cfg.now_ns,
    )
    .await?;

    Ok(SqlLatencyReport {
        provenance: Provenance {
            store_backend: cfg.store_backend.clone(),
            region: cfg.region.clone(),
            endpoint: cfg.endpoint.clone(),
            host_logical_cores: host_logical_cores(),
            source: "tenant".to_string(),
            dataset_id: cfg.tenant.clone(),
            runs: cfg.runs.max(1),
        },
        dataset,
        entries,
        skipped,
    })
}

/// Read the tenant's durable declared typed attribute columns from its
/// `TenantConfig` and map them to `ravel-sql`'s [`DeclaredColumn`]. This is the
/// same durable record `ravel-cli typed-attr-column set` writes; reading it
/// directly (rather than through the server's cache-aside overlay) is a
/// point-in-time resolution of exactly that configuration.
async fn resolve_durable_declarations(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant_hash: &TenantHash,
) -> Result<Vec<DeclaredColumn>, Error> {
    let config = read_config_values(store.as_ref(), tenant_hash).await?;
    let columns = config
        .and_then(|c| c.typed_attr_columns)
        .unwrap_or_default()
        .into_iter()
        .map(|col| {
            let ty = match col.ty {
                DeclaredColumnType::Str => DeclaredType::Str,
                DeclaredColumnType::I64 => DeclaredType::I64,
                DeclaredColumnType::Bool => DeclaredType::Bool,
                DeclaredColumnType::Bytes => DeclaredType::Bytes,
            };
            DeclaredColumn::new(col.key, ty)
        })
        .collect();
    Ok(columns)
}

/// The one resource+scope every generated record shares.
fn resource() -> Vec<(String, AttrValue)> {
    vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )]
}

const SCOPE_NAME: &str = "sql-latency-bench";
const SCOPE_VERSION: &str = "1.0";

/// Build the wide-schema dataset: `cfg.records` log records split into RLOG
/// objects of `cfg.records_per_object`, each published with its own commit
/// record so a real `Catalog::resolve` finds it. Every record carries a
/// `duration_ms` i64 attribute (the corpus's declared column) plus
/// `cfg.extra_attrs` filler keys.
async fn generate_dataset(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &TenantId,
    cfg: &GenerateConfig,
) -> Result<(), Error> {
    let records = build_records(cfg.records.max(1), cfg.extra_attrs);
    let per_object = cfg.records_per_object.max(1);
    let writer_id = Uuid::from_u128(0x5100_0100);

    for (obj_idx, chunk) in records.chunks(per_object).enumerate() {
        let writer_seq = (obj_idx + 1) as u64;
        let identity = ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        for rec in chunk {
            writer.push(rec.clone())?;
        }
        let bytes = writer.finish()?;

        let min = chunk.iter().map(|r| r.ts_ns).min().unwrap_or(0);
        let max = chunk.iter().map(|r| r.ts_ns).max().unwrap_or(0);
        let new_record = NewCommitRecord {
            tenant_hash: tenant.hash(),
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash: [0u8; 32],
            sample_count: chunk.len() as u64,
            series_count: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: 1,
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        };
        let rec = record::build(new_record)?;
        let data_key = keys::reconstruct_data_key(&rec)?;
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await?;
        publish::publish(store.as_ref(), &rec, &RetryPolicy::default()).await?;
    }
    Ok(())
}

/// The four severities the corpus filters on, as `(severity_num, text)`.
const SEVERITIES: [(u8, &str); 4] = [(9, "INFO"), (13, "WARN"), (17, "ERROR"), (21, "FATAL")];

/// Generate `count` records on one stream. The values are chosen so every
/// corpus statement returns something non-trivial: severities cycle through all
/// four the corpus names, some bodies carry the word `timeout`, and
/// `duration_ms` spans below and above the corpus's `1000` threshold.
fn build_records(count: usize, extra_attrs: usize) -> Vec<LogRecord> {
    let res = resource();
    let stream_id = log_stream_id(&res, SCOPE_NAME, SCOPE_VERSION, &[]);
    let stream_attrs = stream_attrs_bytes(&res, SCOPE_NAME, SCOPE_VERSION, &[]);

    (0..count)
        .map(|i| {
            let (severity_num, severity_text) = SEVERITIES[i % SEVERITIES.len()];
            // Some bodies contain `timeout` (for has_word), and a few digits so
            // regexp_replace has something to fold.
            let body = if i % 3 == 0 {
                format!("request {i} timeout after 30s")
            } else {
                format!("request {i} ok in {}ms", (i % 900) as i64)
            };
            let duration_ms = ((i % 5) as i64) * 400; // 0,400,800,1200,1600

            let mut attrs: Vec<(String, AttrValue)> =
                vec![("duration_ms".to_string(), AttrValue::I64(duration_ms))];
            for k in 0..extra_attrs {
                attrs.push((
                    format!("attr_{k}"),
                    AttrValue::Str(format!("v{}", (i + k) % 7)),
                ));
            }

            LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: 1_000 + (i as i64) * 1_000,
                observed_ts_ns: 1_000 + (i as i64) * 1_000,
                severity_num,
                severity_text: severity_text.to_string(),
                body,
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs,
            }
        })
        .collect()
}
