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
//! The tenant lane has a second mode, behind the `flight-lane` feature: with
//! [`TenantConfigInput::flight`] set it executes each statement through a
//! running `ravel-server`'s Flight SQL endpoint instead of an in-process
//! executor, so the number a user would see (server planning, gRPC, Arrow IPC
//! encode and decode, all of it) can be produced from the same corpus into the
//! same report. It still resolves the dataset stanza and the declared-column
//! set from the object store directly, because a Flight client cannot read the
//! tenant's catalog and the skip check needs the declarations. What it loses is
//! [`ScanDiagnostics`]: those come off the executor's own counters, which no
//! Flight response carries, so its entries report `scan: None`.
//!
//! Report-only: like the rest of `ravel-bench`, this never changes library
//! behavior, it only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::displayable;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{
    Catalog, CatalogConfig, DeclaredColumnType, SegmentLevel, read_config_values,
    read_generations_from_store, shard_ceiling,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{
    CacheFetchError, DEFAULT_FETCH_CONCURRENCY, DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
    DEFAULT_MAX_SEGMENTS, EngineConfig, FetchByteCounter, LogSegmentFetcher, ProbeMissCounter,
    SegmentFetcher, fetch_amplification_ratio,
};
use ravel_sql::{
    DEFAULT_MAX_QUERY_BYTES, DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig,
    SqlExecutor, SqlRequest, StaticDeclaredColumns,
};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
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

/// The `SqlExecutor` tenant-accountant ceiling the bench used before it was a
/// flag: 1 GiB, matching `ravel-server`'s own default.
pub const DEFAULT_TENANT_MAX_BYTES: usize = 1 << 30;

/// Anything the harness can fail on. Boxed so `?` composes over the executor,
/// catalog, writer, publish, and tenant-config error types without a bespoke
/// enum; a bench never needs to match on the variant.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// The tenant lane's fail-closed refusals (issue #677). Each names exactly what
/// it resolved so a misdirected run (wrong `--window-hours`, wrong tenant, a
/// tenant loaded before provisioning records existed, or a `--shards` that
/// contradicts the durable record) is loud instead of silently measuring an
/// empty or shard-0-only dataset. Boxed into [`Error`] like every other lane
/// failure; tests downcast to the variant.
#[derive(Debug, thiserror::Error)]
pub enum TenantLaneError {
    /// No `--shards` and no durable provisioning record: the tenant predates
    /// provisioning records, so its shard count cannot be resolved.
    #[error(
        "tenant `{tenant}` has no shard-count provisioning record (loaded before provisioning \
         records existed): pass --shards <N> to name its shard count"
    )]
    NoProvisioningRecord { tenant: String },
    /// `--shards` disagrees with the record's shard ceiling. Refuse rather than
    /// silently prefer one over the other.
    #[error(
        "--shards {requested} disagrees with tenant `{tenant}`'s provisioning record shard ceiling \
         {ceiling}: refusing to measure over a shard count the record contradicts"
    )]
    ShardCountDisagreement {
        tenant: String,
        requested: u32,
        ceiling: u32,
    },
    /// A full-window resolve found zero objects. A report over an empty dataset
    /// is impossible to produce from the tenant lane.
    #[error(
        "tenant `{tenant}` resolved 0 objects over shard_count {shard_count}, window \
         [{start_ns}, {end_ns}] ns, now_ns {now_ns}: refusing to report an empty dataset \
         (check --window-hours, --shards, and the tenant id)"
    )]
    EmptyDataset {
        tenant: String,
        shard_count: u32,
        start_ns: i64,
        end_ns: i64,
        now_ns: i64,
    },
    /// `--compaction` asserted a layout that disagrees with what the tenant's
    /// resolved snapshot actually contains. The label itself is always the
    /// observed one ([`DatasetInfo::layout`] is never an echo of this flag,
    /// issue #834); this is a separate, optional sanity check that fails
    /// closed when the operator's belief about the tenant is stale, instead
    /// of letting a mislabeled report through silently.
    #[error(
        "--compaction {asserted} disagrees with tenant `{tenant}`'s resolved snapshot: observed \
         layout is `{observed}` across {object_count} resolved objects; recheck the tenant or \
         drop the wrong --compaction flag"
    )]
    CompactionMismatch {
        tenant: String,
        asserted: &'static str,
        observed: String,
        object_count: usize,
    },
}

/// An operator's belief about whether the measured tenant is a freshly loaded
/// layout (many small objects) or one the maintenance machinery has compacted
/// (fewer, larger).
///
/// This no longer supplies [`DatasetInfo::layout`] (issue #834: a
/// `--compaction pre` label survived a real compaction and nearly entered a
/// published comparison unchallenged). `layout` is always derived from the
/// resolved snapshot's segment levels: any [`SegmentLevel::L1`] segment means
/// compaction has run over at least part of the tenant. `Compaction` now
/// feeds only an optional check ([`TenantConfigInput::compaction`]): when the
/// operator states one, the tenant lane refuses to run if it disagrees with
/// what the snapshot actually is, rather than silently trusting either side.
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
    /// Which lane produced the run: `"generate"`, `"tenant"`, or `"flight"`.
    pub source: String,
    /// The tenant the numbers describe (a generated run mints a unique id).
    pub dataset_id: String,
    /// Executions per statement behind the min/median/max.
    pub runs: usize,
    /// Configured read-cache byte budget (ADR-0046) attached to the query
    /// fetcher, or `0` when no cache was wired. Recorded so a report states
    /// whether a cache was on: the per-statement `cache_*` counters are only
    /// meaningful against this.
    pub cache_bytes: u64,
    /// Per-statement wall deadline, in seconds, handed to every
    /// [`SqlRequest`] the run issued. Recorded so a report states the budget
    /// its statements ran under: a statement that exceeded it appears in
    /// `failed`, not as a latency.
    #[serde(default = "default_deadline_secs")]
    pub deadline_secs: u64,
    /// The executor's `fetch_concurrency` (logs scan partitions and in-flight
    /// segment fetches per query). Recorded because the cold floor of a
    /// full-scan statement is latency-bound and moves nearly linearly with it.
    #[serde(default = "default_fetch_concurrency")]
    pub fetch_concurrency: usize,
    /// The per-query DataFusion memory-pool ceiling this run ASKED for
    /// (`--sql-max-query-bytes`, ADR-0088). A report written before this field
    /// existed deserializes to [`ravel_sql::DEFAULT_MAX_QUERY_BYTES`].
    #[serde(default = "default_max_query_bytes")]
    pub sql_max_query_bytes_requested: usize,
    /// The ceiling that actually governed execution, or `None` when this
    /// process cannot know it. The Flight lane does not send the setting to the
    /// server (it is not a Flight header), so what governed there is the
    /// server's own config: a statement that failed with `query memory pool
    /// exhausted` on that lane was refused by a ceiling this report cannot
    /// name. Recording the requested value as effective would let two Flight
    /// tables taken at different `--sql-max-query-bytes` values look
    /// comparable while having run under identical server ceilings.
    #[serde(default)]
    pub sql_max_query_bytes_effective: Option<usize>,
    /// The tenant accountant's ceiling for the run. A statement that failed
    /// with `tenant memory budget exhausted` was refused by this, not by
    /// `max_query_bytes`.
    #[serde(default = "default_tenant_max_bytes")]
    pub tenant_max_bytes: usize,
    /// The engine's `max_segments` ceiling for the run (`ravel-server
    /// --max-segments`): the number of sealed, below-watermark segments a
    /// statement may fan out over before it is refused with `query fans out
    /// over too many segments` (ADR-0073 decision 2). A statement in `failed`
    /// with that error was refused by this, not by any byte ceiling.
    #[serde(default = "default_max_segments")]
    pub sql_max_segments: usize,
    /// What this benchmark process was *asked* to do about repartitioning an
    /// exact-typed final aggregation (ADR-0094): the local
    /// `--sql-parallel-final-aggregation` CLI value. This is only the request,
    /// not necessarily what governed execution -- see
    /// `parallel_final_aggregation_effective`. Named `requested` (not the bare
    /// `parallel_final_aggregation`) precisely because the two can differ under
    /// the Flight lane.
    #[serde(default, alias = "parallel_final_aggregation")]
    pub parallel_final_aggregation_requested: bool,
    /// The value that actually governed execution, when this harness knows it.
    /// `Some(v)` for an in-process lane (`generate`/`tenant`), which applies the
    /// requested value directly to `SqlConfig::parallel_final_aggregation`.
    /// `None` for the Flight lane: the benchmark does not send this setting over
    /// the wire (it is not a Flight header), so a Flight run's effective value is
    /// the running server's own -- the compiled-in default (`true`, on) unless
    /// the server was started with `--sql-parallel-final-aggregation=false`.
    /// Recording `None` rather than the local request stops a Flight report from
    /// claiming a setting it never established.
    #[serde(default)]
    pub parallel_final_aggregation_effective: Option<bool>,
    /// Whether the run wrote a per-statement physical plan (`--explain`). The
    /// plans are a side artifact under `--explain-dir`, never part of the
    /// numbers above.
    #[serde(default)]
    pub explain: bool,
    /// Whether one `SqlExecutor` (and its in-process catalog caches) was reused
    /// across every statement (`--warm-catalog`), instead of a fresh cold
    /// executor per statement. A server holds one process-level catalog and
    /// `RecordCache` for a tenant's whole query stream, so its resolve phase is
    /// warm for every statement after the first; a cold-per-statement bench
    /// re-pays the resolve GETs on every statement and so overstates
    /// resolve-phase cost relative to that server (issue #857).
    ///
    /// `Some(v)` for an in-process lane (`generate`/`tenant`), which builds the
    /// executor the flag governs: under `Some(true)`, only the first statement's
    /// run 0 is a genuine cold resolve. `None` for the Flight lane: it builds no
    /// in-process executor, so the flag governs nothing there and reporting its
    /// value would be a mislabelled measurement (the same class of defect as the
    /// effective-ceiling fields above avoid on that lane). See
    /// [`recorded_warm_catalog`].
    #[serde(default)]
    pub warm_catalog: Option<bool>,
    /// The logs suffix-probe length (`--logs-suffix-len`, issue #883) that
    /// governed this run's log reads, or `None` when the per-object derivation
    /// ([`ravel_query::derive_suffix_len`]) governed instead. Recorded so a
    /// probe sweep's arms are distinguishable: one report per pinned window, and
    /// an arm that cannot say which window produced it is not a measurement.
    ///
    /// `Some(n)` for an in-process lane (`generate`/`tenant`) with the flag set;
    /// `None` when the flag was unset (derivation governs). The Flight lane
    /// records `None` even when the flag was set: it builds no in-process
    /// fetcher, the setting is not on the Flight wire, and no `ravel-server`
    /// flag corresponds to it, so the pinned window governed nothing there --
    /// exactly as the effective-ceiling fields above record `None` on that lane.
    #[serde(default)]
    pub logs_suffix_len: Option<u64>,
    /// The Flight SQL endpoint (`host:port`) the statements were executed
    /// against, or `None` for an in-process lane. Deliberately not `endpoint`
    /// above: that one names the object store, and the two are different
    /// machines in any deployment worth measuring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flight_endpoint: Option<String>,
}

/// The `warm_catalog` regime a report should record. `--warm-catalog` reuses
/// one in-process [`SqlExecutor`] across statements, so it governs only the
/// in-process lanes; the Flight lane builds no such executor
/// ([`measure_over_flight`] runs each statement over the wire), so the flag
/// changes nothing there. Recording its value on that lane would claim a regime
/// the run never measured (issue #857 review), so a Flight run records `None`,
/// exactly as the effective-ceiling fields do. `flight` is
/// `cfg.flight.is_some()`.
fn recorded_warm_catalog(flight: bool, warm_catalog: bool) -> Option<bool> {
    if flight { None } else { Some(warm_catalog) }
}

/// The tenant ceiling every run used before the knob was configurable.
fn default_tenant_max_bytes() -> usize {
    DEFAULT_TENANT_MAX_BYTES
}

/// The per-query pool ceiling every run used before the knob was recorded in
/// provenance (issue #615); also what a report written before the field existed
/// deserializes to.
fn default_max_query_bytes() -> usize {
    DEFAULT_MAX_QUERY_BYTES
}

/// The engine segment ceiling every run used before the knob was configurable
/// (issue #720); also what a report written before the field existed
/// deserializes to.
fn default_max_segments() -> usize {
    DEFAULT_MAX_SEGMENTS
}

/// What every run used before the knob was configurable (issue #680); also
/// what a report written before the field existed deserializes to.
fn default_fetch_concurrency() -> usize {
    DEFAULT_FETCH_CONCURRENCY
}

/// The deadline every run used before it became configurable (issue #688);
/// also what a report written before the field existed deserializes to.
fn default_deadline_secs() -> u64 {
    30
}

/// The dataset as it sits in the object store, independent of any one query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetInfo {
    /// Wall time to build and publish the dataset. `Some` for the generated
    /// lane, which builds and times it in process. `None` for the `--tenant`
    /// lane: its load happened out of process through `ravel-cli` before this
    /// run started, so there is nothing here to time, and a load that never
    /// ran must never render as a measured `0.0ms` (issue #834).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_wall_ms: Option<f64>,
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
    /// `"pre-compaction"` or `"post-compaction"`: which layout the tenant's
    /// resolved snapshot actually contains, derived from each segment's
    /// `SegmentLevel` (any `L1` segment means compaction has run over at
    /// least part of the tenant). Observed, never an echo of an operator flag
    /// (issue #834).
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

/// The object-store traffic and cache behavior of a single execution, read off
/// the executor's `QueryAccounting`. Recorded once per run (not only the cold
/// run) so a warm repeat can be read for the question it exists to answer: did
/// the second execution drop to plan reads only, or does it still fetch objects
/// (issue #767). The cold run's figures also live in [`ScanDiagnostics`] (the
/// `object_store_*`/`cache_*` fields), kept there for report compatibility.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunAccounting {
    /// Object-store GET requests this run issued.
    pub object_store_get_requests: u64,
    /// Object-store LIST requests this run issued.
    pub object_store_list_requests: u64,
    /// Bytes transferred from the object store across every operation kind.
    pub object_store_bytes: u64,
    /// Fetch-cache hits on this run.
    pub cache_hits: u64,
    /// Fetch-cache misses on this run.
    pub cache_misses: u64,
    /// Bytes served from the fetch cache on this run.
    pub cache_bytes: u64,
    /// Tail sections (SKIP_IDX, and PAGE_DIR on a version-4 object) that this
    /// run's PLAN-phase probes failed to reach, charged to the phase that issued
    /// the probe (`ravel_query::ProbePhase::Plan`): the footer/skip-index reads
    /// `plan_segment` and `plan_segment_block_stats` issue, plus the whole-object
    /// plan fallback for a predicate the skip index cannot decide.
    ///
    /// Counts uncovered tail SECTIONS, not GETs. A short version-4 probe can
    /// miss both SKIP_IDX and PAGE_DIR and increment twice while the fetcher
    /// coalesces their adjacent ranges into a single GET, so this is an upper
    /// bound on the extra requests, never a one-to-one count. Reading it as
    /// GETs overstates the cost of a short probe by up to a factor of two,
    /// which matters because this is the number a probe floor is tightened
    /// against (issue #883, `ravel_query::LOG_SUFFIX_FLOOR_BYTES`). Compare it
    /// with [`Self::object_store_get_requests`] rather than substituting for
    /// it.
    /// It is measured against the probe WINDOW, not against cache residency, so
    /// a warm run reports the same value as the cold one even though its GETs
    /// fall.
    #[serde(default)]
    pub probe_misses_plan: u64,
    /// Tail sections this run's SCAN-phase probes failed to reach, charged to
    /// `ravel_query::ProbePhase::Scan`: the block/page/chunk data reads. Same
    /// units and same window-not-cache semantics as [`Self::probe_misses_plan`];
    /// the run's total is the two summed.
    #[serde(default)]
    pub probe_misses_scan: u64,
    /// PLAN-phase WIRE bytes this run's log fetcher transferred: the footer
    /// suffix probe, SKIP_IDX, PAGE_DIR, FIELD_DIR, and STREAM_DIR of the log
    /// data objects (issue #913, `ravel_query::FetchByteCounts::plan_wire_bytes`).
    /// WIRE bytes, i.e. bytes moved over the network -- the same kind as
    /// [`Self::object_store_bytes`] and [`Self::scan_blocks_wire_bytes`], and a
    /// different kind from the STORED [`Self::projected_page_decoded_bytes`].
    /// Reported SEPARATELY from the fetch-amplification numerator: this is the
    /// directory/probe overhead the numerator deliberately excludes.
    #[serde(default)]
    pub plan_wire_bytes: u64,
    /// SCAN-phase WIRE bytes this run's log fetcher transferred: the
    /// BLOCKS-section data ranges a query's column projection pulled, retries
    /// and coalescing holes included (issue #913,
    /// `ravel_query::FetchByteCounts::scan_wire_bytes`). WIRE bytes. This is the
    /// NUMERATOR of [`Self::fetch_amplification`].
    #[serde(default)]
    pub scan_blocks_wire_bytes: u64,
    /// STORED bytes of the pages this run decoded after column projection
    /// (`ravel_types::QueryAccountingSnapshot::page_bytes_decoded`, issue #913).
    /// STORED bytes, NOT wire bytes: the on-object size of the pages the decode
    /// touched, never a network transfer figure, so it must not be summed or
    /// compared with the WIRE fields above. This is the DENOMINATOR of
    /// [`Self::fetch_amplification`].
    #[serde(default)]
    pub projected_page_decoded_bytes: u64,
    /// Fetch amplification: [`Self::scan_blocks_wire_bytes`] (WIRE) divided by
    /// [`Self::projected_page_decoded_bytes`] (STORED), issue #913. The ratio of
    /// BLOCKS-section bytes moved over the wire to the stored page bytes the
    /// statement actually decoded after projection. `0.0` when nothing was
    /// decoded. Unitless, but formed from two different byte kinds; see the two
    /// source fields for which is which.
    #[serde(default)]
    pub fetch_amplification: f64,
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
    /// Where the time went. `Some` for the in-process lanes, which read the
    /// executor's own counters. `None` for the Flight lane: those counters are
    /// executor-side state a Flight SQL response does not carry, and reporting
    /// zeros would read as "nothing was scanned".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan: Option<ScanDiagnostics>,
    /// Object-store accounting for every execution, in run order (index 0 is
    /// the cold run), so a warm repeat's GETs/bytes/cache hits can be read
    /// rather than only the cold run's (issue #767). Length equals the report's
    /// `runs` for an in-process lane. `None` for the Flight lane, whose response
    /// carries no executor accounting (same reason `scan` is `None`). Index 0
    /// duplicates the `cold_*` figures in `scan`, which stay for compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_run_accounting: Option<Vec<RunAccounting>>,
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

/// One statement that was run and failed (a wall-deadline expiry, a memory
/// budget exhaustion, a planning error). Recorded only when the run was asked to
/// continue past failures; otherwise the first failure aborts the run. Distinct
/// from a skip: this statement was executed and produced no number.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailedEntry {
    /// The corpus entry id.
    pub id: String,
    /// Which execution failed (0 is the cold run). Earlier runs' latencies are
    /// discarded: a partial min/median/max would read as a measurement.
    pub run: usize,
    /// The executor's error, verbatim.
    pub error: String,
}

/// One statement's outcome, emitted the moment it is known. This is what the
/// progress stream carries: a run over a large dataset can take hours and the
/// full [`SqlLatencyReport`] is only written at the end, so a crash or a kill
/// late in the run would otherwise lose every number already measured.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum EntryEvent {
    Measured(EntryReport),
    Skipped(SkippedEntry),
    Failed(FailedEntry),
}

/// An append-only JSON-lines sink for [`EntryEvent`]s, flushed after every
/// line so the file is complete up to the last finished statement at any
/// instant. `None` when no progress path was configured.
struct ProgressSink {
    file: std::fs::File,
}

impl ProgressSink {
    fn open(path: &std::path::Path) -> Result<Self, Error> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open progress file {}: {e}", path.display()))?;
        Ok(ProgressSink { file })
    }

    fn write(&mut self, event: &EntryEvent) -> Result<(), Error> {
        use std::io::Write;
        let line = serde_json::to_string(event)?;
        writeln!(self.file, "{line}")?;
        self.file.flush()?;
        Ok(())
    }
}

/// The full report: provenance, dataset shape, measured statements, the
/// statements skipped for an unsatisfied declared-column dependency, and the
/// statements that ran and failed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlLatencyReport {
    pub provenance: Provenance,
    pub dataset: DatasetInfo,
    pub entries: Vec<EntryReport>,
    pub skipped: Vec<SkippedEntry>,
    #[serde(default)]
    pub failed: Vec<FailedEntry>,
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
    /// Per-query DataFusion memory-pool ceiling, in bytes, fed to
    /// [`SqlConfig::max_query_bytes`]. Mirrors `ravel-server`'s
    /// `--sql-max-query-bytes` (ADR-0088): raise it to measure a heavy query
    /// that otherwise aborts with `query memory budget exhausted`. Defaults to
    /// [`ravel_sql::DEFAULT_MAX_QUERY_BYTES`], so an unset flag leaves the
    /// executor's budget byte-for-byte unchanged.
    pub max_query_bytes: usize,
    /// Read-cache byte budget (ADR-0046) to attach to the query fetcher. `0`
    /// (the default) attaches no cache, leaving the fetcher byte-for-byte as
    /// before; `> 0` builds a RAM tier of this size.
    pub cache_bytes: u64,
    /// Per-statement wall deadline handed to every [`SqlRequest`].
    pub deadline: Duration,
    /// When `true`, a statement whose execution fails is recorded in
    /// [`SqlLatencyReport::failed`] and the run moves on; when `false` the first
    /// failure aborts the run with its error.
    pub continue_on_error: bool,
    /// [`EngineConfig::fetch_concurrency`] for the executor: the logs scan's
    /// partition count and the bound on in-flight segment fetches per query
    /// (ADR-0088's single coupled knob, `ravel-server --fetch-concurrency`).
    /// Defaults to [`ravel_query::DEFAULT_FETCH_CONCURRENCY`].
    pub fetch_concurrency: usize,
    /// Append one JSON line per finished statement ([`EntryEvent`]) to this
    /// file as the run goes, flushed per line, so a run killed hours in still
    /// leaves every number it had measured. `None` writes nothing.
    pub progress_jsonl: Option<std::path::PathBuf>,
    /// Per-tenant SQL memory ceiling, the `SqlExecutor`'s tenant accountant
    /// limit (`ravel-server --sql-tenant-max-bytes`). Enforced across a
    /// tenant's concurrent queries and SEPARATE from `max_query_bytes`: a
    /// statement can clear the per-query pool and still be refused here, so
    /// both have to be raised together to measure a heavy aggregate.
    pub tenant_max_bytes: usize,
    /// Whether an exact-typed query may repartition its final aggregation
    /// (ADR-0094 amended by issue #741, `ravel-server
    /// --sql-parallel-final-aggregation`). Reaches
    /// `SqlConfig::parallel_final_aggregation`; `true` is the compiled-in
    /// default, and `--sql-parallel-final-aggregation=false` is the opt-out.
    pub parallel_final_aggregation: bool,
    /// The engine's `max_segments` ceiling, the same knob as `ravel-server
    /// --max-segments`. Reaches `SqlConfig::engine.max_segments`; a statement
    /// fanning out over more sealed, below-watermark segments than this is
    /// refused with `query fans out over too many segments` (ADR-0073
    /// decision 2). Defaults to [`ravel_query::DEFAULT_MAX_SEGMENTS`], so an
    /// unset flag leaves the ceiling byte-for-byte unchanged.
    pub max_segments: usize,
    /// When `Some`, write each statement's physical plan (`--explain`) to
    /// `<dir>/<id>.txt` before measuring it, so the DataFusion optimizer rules
    /// that fired (AggregateStatistics, single_distinct_to_groupby, pushdown)
    /// are readable per statement. Not timed and not part of the report's
    /// numbers. `None` writes no plan.
    pub explain_dir: Option<std::path::PathBuf>,
    /// Reuse one `SqlExecutor` (and its in-process catalog caches) across every
    /// statement (`--warm-catalog`) instead of building a fresh cold executor
    /// per statement. See [`Provenance::warm_catalog`] for why this matters: a
    /// server's resolve phase is warm for every statement after the first, and
    /// a cold-per-statement bench diverges from it by re-paying the resolve
    /// GETs each time (issue #857).
    pub warm_catalog: bool,
    /// Pin the logs suffix-probe window to this many bytes, or `None` for the
    /// per-object derivation ([`ravel_query::derive_suffix_len`], issue #883).
    /// `None` leaves today's derivation byte-for-byte untouched; `Some(n)`
    /// reaches [`ExecutorSettings::logs_suffix_len`] and so
    /// [`ravel_query::LogSegmentFetcher::with_suffix_len`]. No `ravel-server`
    /// flag corresponds to it: this is the seam a probe sweep sets the window
    /// through.
    pub logs_suffix_len: Option<u64>,
}

/// Where the Flight lane sends its statements.
///
/// Present unconditionally so [`TenantConfigInput`] has one shape under every
/// feature set; only the code that *uses* it is behind `flight-lane`, and a
/// binary built without that feature refuses a run that sets this rather than
/// silently measuring in process.
#[derive(Clone, Debug)]
pub struct FlightTarget {
    /// `host:port` of the server's gRPC listener (`ravel-server
    /// --listen-grpc`, which carries Flight SQL when the server is built with
    /// its `flight-sql` feature).
    pub endpoint: String,
    /// Bearer credential the server's tenant resolver maps to the tenant under
    /// measurement, sent as `authorization: Bearer <token>`. `None` sends no
    /// credential, which only reaches a server configured to resolve tenants
    /// some other way.
    pub token: Option<String>,
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
    /// The operator's optional belief about which layout the tenant is in,
    /// checked against the resolved snapshot rather than trusted (issue
    /// #834). `Some` refuses the run if it disagrees with what the snapshot's
    /// segments actually are; `None` skips the check. Either way,
    /// `SqlLatencyReport::dataset.layout` is always the observed value, never
    /// this flag echoed back.
    pub compaction: Option<Compaction>,
    /// Per-query DataFusion memory-pool ceiling, in bytes, fed to
    /// [`SqlConfig::max_query_bytes`]. Mirrors `ravel-server`'s
    /// `--sql-max-query-bytes` (ADR-0088): raise it to measure a heavy query
    /// that otherwise aborts with `query memory budget exhausted`. Defaults to
    /// [`ravel_sql::DEFAULT_MAX_QUERY_BYTES`], so an unset flag leaves the
    /// executor's budget byte-for-byte unchanged.
    pub max_query_bytes: usize,
    /// Operator-supplied shard count. `Some(n)` overrides the durable
    /// provisioning record (and is required for a tenant that predates those
    /// records); `None` resolves the count from the record's shard ceiling
    /// (issue #677).
    pub shards: Option<u32>,
    /// Read-cache byte budget (ADR-0046) to attach to the query fetcher. `0`
    /// (the default) attaches no cache; `> 0` builds a RAM tier of this size so
    /// the second and later runs of a statement can serve from cache.
    pub cache_bytes: u64,
    /// Per-statement wall deadline handed to every [`SqlRequest`].
    pub deadline: Duration,
    /// When `true`, a statement whose execution fails is recorded in
    /// [`SqlLatencyReport::failed`] and the run moves on; when `false` the first
    /// failure aborts the run with its error.
    pub continue_on_error: bool,
    /// [`EngineConfig::fetch_concurrency`] for the executor: the logs scan's
    /// partition count and the bound on in-flight segment fetches per query
    /// (ADR-0088's single coupled knob, `ravel-server --fetch-concurrency`).
    /// Defaults to [`ravel_query::DEFAULT_FETCH_CONCURRENCY`].
    pub fetch_concurrency: usize,
    /// Append one JSON line per finished statement ([`EntryEvent`]) to this
    /// file as the run goes, flushed per line, so a run killed hours in still
    /// leaves every number it had measured. `None` writes nothing.
    pub progress_jsonl: Option<std::path::PathBuf>,
    /// Per-tenant SQL memory ceiling, the `SqlExecutor`'s tenant accountant
    /// limit (`ravel-server --sql-tenant-max-bytes`). Enforced across a
    /// tenant's concurrent queries and SEPARATE from `max_query_bytes`: a
    /// statement can clear the per-query pool and still be refused here, so
    /// both have to be raised together to measure a heavy aggregate.
    pub tenant_max_bytes: usize,
    /// Whether an exact-typed query may repartition its final aggregation
    /// (ADR-0094 amended by issue #741, `ravel-server
    /// --sql-parallel-final-aggregation`). Reaches
    /// `SqlConfig::parallel_final_aggregation`; `true` is the compiled-in
    /// default, and `--sql-parallel-final-aggregation=false` is the opt-out.
    pub parallel_final_aggregation: bool,
    /// The engine's `max_segments` ceiling, the same knob as `ravel-server
    /// --max-segments`. Reaches `SqlConfig::engine.max_segments`; a statement
    /// fanning out over more sealed, below-watermark segments than this is
    /// refused with `query fans out over too many segments` (ADR-0073
    /// decision 2). ADR-0073 decision 2 counts only sealed, below-watermark
    /// segments, so a freshly loaded tenant can sit far above this ceiling and
    /// only trip it once a fold seals its hours (issue #720). Defaults to
    /// [`ravel_query::DEFAULT_MAX_SEGMENTS`].
    pub max_segments: usize,
    /// When `Some`, write each statement's physical plan (`--explain`) to
    /// `<dir>/<id>.txt` before measuring it, so the DataFusion optimizer rules
    /// that fired (AggregateStatistics, single_distinct_to_groupby, pushdown)
    /// are readable per statement. Not timed and not part of the report's
    /// numbers. Ignored by the Flight lane, which has no in-process plan to
    /// display. `None` writes no plan.
    pub explain_dir: Option<std::path::PathBuf>,
    /// `Some` executes every statement through a running server's Flight SQL
    /// endpoint instead of an in-process [`SqlExecutor`]. The dataset stanza
    /// and the declared-column set are still resolved from `store` directly:
    /// a Flight client cannot read the tenant's catalog, and the skip check
    /// needs the declarations.
    pub flight: Option<FlightTarget>,
    /// Reuse one `SqlExecutor` (and its in-process catalog caches) across every
    /// statement (`--warm-catalog`) instead of building a fresh cold executor
    /// per statement. See [`Provenance::warm_catalog`]. Ignored by the Flight
    /// lane, which builds no in-process executor.
    pub warm_catalog: bool,
    /// Pin the logs suffix-probe window to this many bytes, or `None` for the
    /// per-object derivation ([`ravel_query::derive_suffix_len`], issue #883).
    /// `None` leaves today's derivation byte-for-byte untouched; `Some(n)`
    /// reaches [`ExecutorSettings::logs_suffix_len`] and so
    /// [`ravel_query::LogSegmentFetcher::with_suffix_len`]. No `ravel-server`
    /// flag corresponds to it: this is the seam a probe sweep sets the window
    /// through. Reaches the in-process fetcher only; the Flight lane never
    /// applies it (the setting is not on the Flight wire).
    pub logs_suffix_len: Option<u64>,
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

/// Build ADR-0046's RAM read cache sized to `cache_bytes` (never called for a
/// `0` budget; that path attaches no cache at all).
///
/// `max_entry_bytes` is the whole budget so no RLOG object the bench measures is
/// silently rejected as too large; the byte budget still bounds total
/// residency. `max_entries` is derived rather than fixed: RLOG objects are tens
/// of KiB or larger, so byte pressure, not entry count, should be the binding
/// limit -- one entry per 4 KiB of budget, floored so a tiny budget stays
/// usable, keeps the count well clear of that role.
fn build_read_cache(cache_bytes: u64) -> Arc<Cache<CacheFetchError>> {
    let max_entries = (cache_bytes / 4096).max(64) as usize;
    Arc::new(Cache::new(CacheLimits::new(
        cache_bytes,
        max_entries,
        cache_bytes,
    )))
}

/// Build a fresh executor over `store` with `declared` installed, its catalog
/// configured for `shard_count` shards, and `cache` (when present) attached to
/// the log fetcher. Fresh per corpus entry so the first run is genuinely cold: a
/// shared executor would let one statement warm the next through the catalog and
/// fetch caches.
///
/// `shard_count` reaches the resolve because these direct-built catalogs run
/// with provisioning enforcement off, where the scan set is `0..shard_count`
/// for every hour (ravel_catalog docs on `read_scan_generations`); the tenant
/// lane resolves that count before calling here so a multi-shard tenant is no
/// longer measured over shard 0 alone.
/// The executor knobs a run configures. They travel together because they are
/// only meaningful together: a statement refused by one ceiling says nothing
/// about the others, and a table is comparable only when all of them match.
///
/// Every field but [`Self::logs_suffix_len`] is a flag `ravel-server` also has.
/// That one is a measurement knob with no server equivalent, for the reason its
/// own doc gives.
// issue #720: carries `--sql-max-segments` and `--explain`.
#[derive(Clone, Copy, Debug)]
pub struct ExecutorSettings {
    /// Per-query DataFusion pool ceiling (`--sql-max-query-bytes`).
    pub max_query_bytes: usize,
    /// Shards the catalog resolve scans (issue #677).
    pub shard_count: u32,
    /// Scan partitions and in-flight segment fetches (`--fetch-concurrency`).
    pub fetch_concurrency: usize,
    /// Per-tenant ceiling across a tenant's concurrent queries
    /// (`--sql-tenant-max-bytes`), a SECOND limit under `max_query_bytes`.
    pub tenant_max_bytes: usize,
    /// ADR-0094 repartitioned final aggregation
    /// (`--sql-parallel-final-aggregation`).
    pub parallel_final_aggregation: bool,
    /// Sealed, below-watermark segments a statement may fan out over
    /// (`--max-segments`), ADR-0073 decision 2.
    pub max_segments: usize,
    /// Object size above which a logs read fetches only the pruning-relevant
    /// parts instead of the whole object (ADR-0107), `ravel-server`'s
    /// `logs_block_range_threshold`. `0` routes every object through the ranged
    /// probe-then-fetch path, which is the only path that probes at all: at or
    /// below the threshold an object is read whole in one GET with no suffix
    /// probe and therefore no probe miss.
    pub logs_block_range_threshold: u64,
    /// Suffix-probe length to pin, or `None` for the per-object derivation
    /// ([`ravel_query::derive_suffix_len`], #883). No server flag corresponds to
    /// it: the probe floor is a calibrated constant, and the only sound way to
    /// move it is to sweep the window against measured probe misses first, which
    /// is what this knob exists for.
    pub logs_suffix_len: Option<u64>,
}

impl Default for ExecutorSettings {
    fn default() -> Self {
        ExecutorSettings {
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            shard_count: 1,
            fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
            tenant_max_bytes: DEFAULT_TENANT_MAX_BYTES,
            parallel_final_aggregation: true,
            max_segments: DEFAULT_MAX_SEGMENTS,
            logs_block_range_threshold: DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            logs_suffix_len: None,
        }
    }
}

/// The executor plus the probe-miss counter of the log fetcher inside it
/// (#883). The counter has to be cloned out here, before the fetcher is moved
/// into the executor, because nothing downstream hands it back: `SqlOutcome`
/// carries a `QueryAccountingSnapshot`, which has no probe-miss field, and
/// widening that type would change the distributed accounting protobuf, a frozen
/// contract. See [`measure_corpus`] for how a per-run figure is taken from an
/// accumulating counter.
struct ColdExecutor {
    executor: SqlExecutor,
    probe_misses: ProbeMissCounter,
    /// The same clone-and-snapshot pattern as `probe_misses`, for the per-phase
    /// WIRE-byte counter behind fetch amplification (#913): cloned out here
    /// before the fetcher moves into the executor, snapshotted around each run.
    fetch_bytes: FetchByteCounter,
}

fn cold_executor(
    store: &Arc<dyn ObjectStoreBackend>,
    declared: &[DeclaredColumn],
    cache: Option<Arc<Cache<CacheFetchError>>>,
    settings: ExecutorSettings,
) -> Result<ColdExecutor, Error> {
    let ExecutorSettings {
        max_query_bytes,
        shard_count,
        fetch_concurrency,
        tenant_max_bytes,
        parallel_final_aggregation,
        max_segments,
        logs_block_range_threshold,
        logs_suffix_len,
    } = settings;
    let catalog = Arc::new(Catalog::new(
        Arc::clone(store),
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )?);
    // The same wiring `ravel-server` does (issue #700): without it the logs
    // fetcher's permit pool stays at its compiled-in 16 and a scan planned at
    // more partitions than that queues on it, so the bench would measure a
    // ceiling the flag cannot move.
    let mut log_fetcher = LogSegmentFetcher::new(Arc::clone(store))
        .with_max_concurrent_gets(fetch_concurrency.max(1))
        .with_block_range_threshold(logs_block_range_threshold);
    if let Some(n) = logs_suffix_len {
        log_fetcher = log_fetcher.with_suffix_len(n);
    }
    if let Some(cache) = cache {
        log_fetcher = log_fetcher.with_cache(cache);
    }
    // Cloned after every builder, because `with_block_range_threshold` and the
    // rest rebuild the inner `BlockRangeFetcher`; the counter itself survives
    // them, but taking the handle last keeps that a local fact rather than an
    // ordering the reader has to verify.
    let probe_misses = log_fetcher.probe_miss_counter();
    let fetch_bytes = log_fetcher.fetch_byte_counter();
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        log_fetcher,
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig {
            max_query_bytes,
            engine: EngineConfig {
                fetch_concurrency: fetch_concurrency.max(1),
                max_segments,
                ..EngineConfig::default()
            },
            parallel_final_aggregation,
            ..SqlConfig::default()
        },
        tenant_max_bytes.max(1),
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared.to_vec())));
    Ok(ColdExecutor {
        executor,
        probe_misses,
        fetch_bytes,
    })
}

/// The physical plan for `sql`, rendered as the indented text `--explain`
/// writes. Resolves and plans through the same executor the measurement uses
/// (`resolve_snapshot` then `plan_pinned`), so the plan reflects the exact
/// declared columns, snapshot, and engine config the timed run would see, then
/// formats it with DataFusion's [`displayable`]. Not executed and not timed.
///
/// `EXPLAIN <statement>` is not run as SQL: `ravel_sql::validate` rejects an
/// `EXPLAIN` statement as not read-only, so the plan is built through the
/// executor's existing `create_physical_plan` API instead, which is the same
/// first step [`PinnedQuery::execute`] takes.
async fn explain_plan_text(
    executor: &SqlExecutor,
    tenant_hash: TenantHash,
    req: &SqlRequest,
    sql: &str,
    declared: &[DeclaredColumn],
) -> Result<String, Error> {
    let accounting = QueryAccounting::new();
    let (snapshot, _estimate) = executor
        .resolve_snapshot(tenant_hash, req, &accounting)
        .await?;
    let planned = executor
        .plan_pinned(tenant_hash, snapshot, sql, &accounting, declared)
        .await?;
    let plan = planned.create_physical_plan().await?;
    Ok(displayable(plan.as_ref()).indent(true).to_string())
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
///
/// When `warm_catalog` is set, one executor (and one read cache, when
/// `cache_bytes > 0`) is built once and reused for every statement instead of
/// a fresh one per statement. This mirrors a server's process-level catalog and
/// `RecordCache`, whose resolve phase is warm for every statement after the
/// first; under it only the first statement's run 0 is a genuine cold resolve
/// (issue #857).
#[allow(clippy::too_many_arguments)]
pub async fn measure_corpus(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    entries: &[CorpusEntry],
    declared: &[DeclaredColumn],
    runs: usize,
    window: TimeRange,
    now_ns: i64,
    cache_bytes: u64,
    deadline: Duration,
    continue_on_error: bool,
    progress_jsonl: Option<&std::path::Path>,
    settings: ExecutorSettings,
    warm_catalog: bool,
    explain_dir: Option<&std::path::Path>,
) -> Result<(Vec<EntryReport>, Vec<SkippedEntry>, Vec<FailedEntry>), Error> {
    let runs = runs.max(1);
    let mut measured = Vec::new();
    let mut skipped = Vec::new();
    let mut failed = Vec::new();
    let mut progress = progress_jsonl.map(ProgressSink::open).transpose()?;

    // `--warm-catalog`: one executor (and one read cache) reused across every
    // statement, so the catalog resolve of every statement after the first is
    // served from the warm in-process `RecordCache`/`HeadCache`, exactly as a
    // server's process-level catalog would (issue #857). Built once here; the
    // per-entry `None` path below keeps the cold-per-statement default.
    let shared_executor = if warm_catalog {
        let cache = (cache_bytes > 0).then(|| build_read_cache(cache_bytes));
        Some(cold_executor(store, declared, cache, settings)?)
    } else {
        None
    };

    // CPU flamegraph lane (issue #365's pattern; see crate::profiling). Covers
    // every entry's execution, both lanes (run_generated/run_tenant funnel
    // through this one function), so a query-side profile always reflects the
    // full corpus rather than one statement in isolation.
    //
    // The profiled multi-run refusal (issue #616) lives in the binary, not here:
    // deciding it from process-global env inside library code would make an
    // exported RAVEL_BENCH_PROFILE_SVG fail unrelated tests that legitimately
    // pass runs > 1, and it would fire only after the resolve and LIST fan-out
    // this function is called with.
    let profile = crate::profiling::ProfileSession::from_env("sql_latency_bench");

    'entries: for entry in entries {
        // Verify the declared-column dependency before running anything. A
        // missing declared column reads NULL for every row, so an unsatisfied
        // entry must be skipped, not run: removing this guard lets the entry
        // execute and report a plausible-but-wrong latency.
        if let Some((missing_key, want)) = first_unsatisfied(entry, declared) {
            let skip = SkippedEntry {
                id: entry.id.clone(),
                missing_key: missing_key.clone(),
                reason: format!(
                    "required declared column `{missing_key}` ({want:?}) is not satisfied by the \
                     dataset under measurement"
                ),
            };
            if let Some(sink) = progress.as_mut() {
                sink.write(&EntryEvent::Skipped(skip.clone()))?;
            }
            skipped.push(skip);
            continue;
        }

        // Cold default: a fresh cache and executor per entry (not per run) so
        // run 0 is genuinely cold while later runs of the SAME statement can
        // serve from it; sharing one cache across entries would warm one
        // statement from another's reads over the same segments. Under
        // `--warm-catalog` the one `shared_executor` built above is reused
        // instead, deliberately warming each statement from the last.
        // The cold path builds a fresh executor owned by this iteration;
        // `owned_executor` holds it alive for the run loop below. The warm path
        // borrows the one `shared_executor` instead and leaves this unused.
        let owned_executor;
        let built: &ColdExecutor = match &shared_executor {
            Some(shared) => shared,
            None => {
                let cache = (cache_bytes > 0).then(|| build_read_cache(cache_bytes));
                owned_executor = cold_executor(store, declared, cache, settings)?;
                &owned_executor
            }
        };
        let executor: &SqlExecutor = &built.executor;
        // #883: the log fetcher's probe-miss counter accumulates for the life of
        // the fetcher, which under `--warm-catalog` is the whole corpus. Every
        // per-run figure below is a difference of two snapshots taken around
        // that run, so a shared executor attributes each statement's misses to
        // that statement and never to the ones before it.
        let probe_misses = &built.probe_misses;
        // #913: the same shared-counter/per-run-snapshot pattern for the log
        // fetcher's per-phase WIRE-byte counter behind fetch amplification.
        let fetch_bytes = &built.fetch_bytes;
        let req = SqlRequest {
            sql: entry.sql.clone(),
            window,
            min_tokens: Vec::new(),
            now_ns,
            deadline,
        };

        // EXPLAIN side artifact, before any timed run and never counted in the
        // numbers below. A query-side failure here (a snapshot resolve error,
        // a planning error, an over-`max_segments` fan-out) is left for the
        // measurement loop to record in `failed`: writing a plan is not the
        // question the report answers, so it must not change control flow.
        // Only a file-write failure propagates, because that is an operator
        // configuration error (`--explain-dir` unwritable), not a query result.
        if let Some(dir) = explain_dir
            && let Ok(plan_text) =
                explain_plan_text(executor, tenant_hash, &req, &entry.sql, declared).await
        {
            let path = dir.join(format!("{}.txt", entry.id));
            std::fs::write(&path, plan_text)
                .map_err(|e| format!("write explain plan {}: {e}", path.display()))?;
        }

        let mut latencies_ns = Vec::with_capacity(runs);
        let mut per_run_accounting = Vec::with_capacity(runs);
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
            // Taken here, after the EXPLAIN side artifact above, so a plan
            // written for `--explain-dir` is not charged to run 0.
            let probe_misses_before = probe_misses.snapshot();
            let fetch_bytes_before = fetch_bytes.snapshot();
            let start = Instant::now();
            let outcome = match executor.execute(tenant_hash, &req).await {
                Ok(outcome) => outcome,
                Err(e) if continue_on_error => {
                    let failure = FailedEntry {
                        id: entry.id.clone(),
                        run,
                        error: e.to_string(),
                    };
                    if let Some(sink) = progress.as_mut() {
                        sink.write(&EntryEvent::Failed(failure.clone()))?;
                    }
                    failed.push(failure);
                    continue 'entries;
                }
                Err(e) => {
                    return Err(Error::from(format!(
                        "entry `{}` failed to execute: {e}",
                        entry.id
                    )));
                }
            };
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            latencies_ns.push(elapsed_ns);
            // Record accounting for every run, not only the cold one: the warm
            // run exists to answer whether the second execution drops to plan
            // reads only or still fetches objects, and that is unreadable from
            // the cold figures alone (issue #767).
            let acc = &outcome.accounting;
            // #883: this run's share of an accumulating counter. The GETs a miss
            // costs are already in `object_store_get_requests` above; this says
            // how many of them the probe window is responsible for.
            let run_probe_misses = probe_misses.snapshot().saturating_sub(&probe_misses_before);
            // #913: this run's per-phase WIRE bytes, and the fetch amplification
            // they form with the STORED decoded page bytes on the pooled
            // accounting snapshot. `page_bytes_decoded` is the denominator; the
            // scan-phase WIRE bytes are the numerator.
            let run_fetch_bytes = fetch_bytes.snapshot().saturating_sub(&fetch_bytes_before);
            let projected_page_decoded_bytes = acc.page_bytes_decoded;
            per_run_accounting.push(RunAccounting {
                object_store_get_requests: acc.s3_requests(AccountedOp::Get),
                object_store_list_requests: acc.s3_requests(AccountedOp::List),
                object_store_bytes: acc.total_s3_bytes(),
                cache_hits: acc.cache_hits,
                cache_misses: acc.cache_misses,
                cache_bytes: acc.cache_bytes,
                probe_misses_plan: run_probe_misses.plan,
                probe_misses_scan: run_probe_misses.scan,
                plan_wire_bytes: run_fetch_bytes.plan_wire_bytes,
                scan_blocks_wire_bytes: run_fetch_bytes.scan_wire_bytes,
                projected_page_decoded_bytes,
                fetch_amplification: fetch_amplification_ratio(
                    run_fetch_bytes.scan_wire_bytes,
                    projected_page_decoded_bytes,
                ),
            });
            if run == 0 {
                // The cold run's block counters and rows go into `scan`; its
                // object-store/cache figures are kept there too (as the
                // `cold_*` fields) for report compatibility, and now also live
                // at index 0 of `per_run_accounting`.
                cold_ns = elapsed_ns;
                rows_returned = outcome.output.num_rows();
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
        let report = EntryReport {
            id: entry.id.clone(),
            min_ms: to_ms(*sorted.first().unwrap()),
            median_ms: to_ms(percentile(&sorted, 0.50)),
            max_ms: to_ms(*sorted.last().unwrap()),
            cold_ms: to_ms(cold_ns),
            rows_returned,
            scan: Some(scan),
            per_run_accounting: Some(per_run_accounting),
        };
        if let Some(sink) = progress.as_mut() {
            sink.write(&EntryEvent::Measured(report.clone()))?;
        }
        measured.push(report);
    }

    profile.finish();

    Ok((measured, skipped, failed))
}

/// Measure `entries` by executing each statement through a running server's
/// Flight SQL endpoint, the path an external client actually takes.
///
/// Same loop as [`measure_corpus`], same [`EntryReport`], same progress
/// stream; the differences are forced by the wire. The skip check still runs
/// off `declared`, which the caller resolved from the object store, because a
/// Flight client cannot read the tenant's catalog. Every entry it produces
/// carries `scan: None`: block counters, object-store request counts, and
/// cache hits are executor-side state the Flight response does not carry.
///
/// The window travels in the request metadata (`x-ravel-start` /
/// `x-ravel-end`, Unix float seconds), which is how ravel-sql's Flight service
/// reads it -- the Flight SQL command itself carries no window. The deadline
/// travels the same way as `x-ravel-timeout`, and the server clamps it to its
/// own maximum.
#[cfg(feature = "flight-lane")]
async fn measure_over_flight(
    target: &FlightTarget,
    cfg: &TenantConfigInput,
    declared: &[DeclaredColumn],
) -> Result<(Vec<EntryReport>, Vec<SkippedEntry>, Vec<FailedEntry>), Error> {
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use tonic::transport::Channel;

    let runs = cfg.runs.max(1);
    let mut measured = Vec::new();
    let mut skipped = Vec::new();
    let mut failed = Vec::new();
    let mut progress = cfg
        .progress_jsonl
        .as_deref()
        .map(ProgressSink::open)
        .transpose()?;

    let uri = format!("http://{}", target.endpoint);
    let channel = Channel::from_shared(uri.clone())
        .map_err(|e| format!("invalid Flight endpoint `{uri}`: {e}"))?;

    'entries: for entry in &cfg.entries {
        if let Some((missing_key, want)) = first_unsatisfied(entry, declared) {
            let skip = SkippedEntry {
                id: entry.id.clone(),
                missing_key: missing_key.clone(),
                reason: format!(
                    "required declared column `{missing_key}` ({want:?}) is not satisfied by the \
                     dataset under measurement"
                ),
            };
            if let Some(sink) = progress.as_mut() {
                sink.write(&EntryEvent::Skipped(skip.clone()))?;
            }
            skipped.push(skip);
            continue;
        }

        // One client per entry, connected lazily. Lazily matters: an
        // unreachable endpoint must surface as the statement's error inside the
        // run loop, where `continue_on_error` governs it, rather than aborting
        // the whole run before any entry has been attempted.
        let mut client = FlightSqlServiceClient::new(channel.clone().connect_lazy());
        if let Some(token) = &target.token {
            client.set_header("authorization", format!("Bearer {token}"));
        }
        client.set_header("x-ravel-start", seconds_header(cfg.window.start_ns));
        client.set_header("x-ravel-end", seconds_header(cfg.window.end_ns));
        client.set_header("x-ravel-timeout", format!("{}", cfg.deadline.as_secs_f64()));

        let mut latencies_ns = Vec::with_capacity(runs);
        let mut cold_ns = 0u64;
        let mut rows_returned = 0usize;

        for run in 0..runs {
            let start = Instant::now();
            let rows = match execute_over_flight(&mut client, &entry.sql).await {
                Ok(rows) => rows,
                Err(e) if cfg.continue_on_error => {
                    let failure = FailedEntry {
                        id: entry.id.clone(),
                        run,
                        error: e.to_string(),
                    };
                    if let Some(sink) = progress.as_mut() {
                        sink.write(&EntryEvent::Failed(failure.clone()))?;
                    }
                    failed.push(failure);
                    continue 'entries;
                }
                Err(e) => {
                    return Err(Error::from(format!(
                        "entry `{}` failed to execute: {e}",
                        entry.id
                    )));
                }
            };
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            latencies_ns.push(elapsed_ns);
            if run == 0 {
                cold_ns = elapsed_ns;
                rows_returned = rows;
            }
        }

        let mut sorted = latencies_ns.clone();
        sorted.sort_unstable();
        let to_ms = |ns: u64| ns as f64 / 1e6;
        let report = EntryReport {
            id: entry.id.clone(),
            min_ms: to_ms(*sorted.first().unwrap()),
            median_ms: to_ms(percentile(&sorted, 0.50)),
            max_ms: to_ms(*sorted.last().unwrap()),
            cold_ms: to_ms(cold_ns),
            rows_returned,
            scan: None,
            per_run_accounting: None,
        };
        if let Some(sink) = progress.as_mut() {
            sink.write(&EntryEvent::Measured(report.clone()))?;
        }
        measured.push(report);
    }

    Ok((measured, skipped, failed))
}

/// Render a nanosecond instant as the Unix float seconds the window metadata
/// keys are defined in.
#[cfg(feature = "flight-lane")]
fn seconds_header(ns: i64) -> String {
    format!("{}", ns as f64 / 1e9)
}

/// One statement over Flight SQL: `GetFlightInfo` to plan and mint a ticket,
/// then `DoGet` per endpoint to redeem it, draining every batch. Returns the
/// rows the statement produced.
#[cfg(feature = "flight-lane")]
async fn execute_over_flight(
    client: &mut arrow_flight::sql::client::FlightSqlServiceClient<tonic::transport::Channel>,
    sql: &str,
) -> Result<usize, Error> {
    use futures::TryStreamExt;

    let info = client
        .execute(sql.to_string(), None)
        .await
        .map_err(|e| format!("GetFlightInfo: {e}"))?;

    let mut rows = 0usize;
    for endpoint in &info.endpoint {
        let ticket = endpoint
            .ticket
            .clone()
            .ok_or_else(|| "Flight endpoint carries no ticket".to_string())?;
        let batches = client
            .do_get(ticket)
            .await
            .map_err(|e| format!("DoGet: {e}"))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| format!("decode Flight batches: {e}"))?;
        rows += batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    }
    Ok(rows)
}

/// Refusal for a binary built without the `flight-lane` feature: the Flight
/// client is not linked, so the only honest answer is an error naming the
/// feature. Measuring in process instead would silently answer a different
/// question than the one `--flight` asked.
#[cfg(not(feature = "flight-lane"))]
async fn measure_over_flight(
    target: &FlightTarget,
    _cfg: &TenantConfigInput,
    _declared: &[DeclaredColumn],
) -> Result<(Vec<EntryReport>, Vec<SkippedEntry>, Vec<FailedEntry>), Error> {
    Err(Error::from(format!(
        "cannot execute against Flight SQL endpoint `{}`: this binary was built without the \
         `flight-lane` feature (rebuild with --features sql-latency,flight-lane)",
        target.endpoint
    )))
}

/// Resolve the dataset-level figures (bytes, object count, rows, layout)
/// from a full-window catalog resolve over the logs signal. Shared by both
/// lanes so every figure is defined identically however the dataset was
/// written.
///
/// `layout` is derived here, never taken as a parameter (issue #834): a
/// resolved snapshot with any [`SegmentLevel::L1`] segment has had
/// compaction run over at least part of it, so it is reported
/// `"post-compaction"`; an all-L0 snapshot is `"pre-compaction"`. This is the
/// same signal ADR-0018's L0/L1 discriminator already carries per segment,
/// read off the resolve every caller already performs -- no new state, no
/// operator input, and no way for it to drift from what the tenant actually
/// is.
async fn dataset_info(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    window: TimeRange,
    now_ns: i64,
    load_wall_ms: Option<f64>,
    shard_count: u32,
) -> Result<DatasetInfo, Error> {
    let catalog = Arc::new(Catalog::new(
        Arc::clone(store),
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )?);
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Logs, window, &[], now_ns)
        .await?;
    let stored_bytes = snapshot.segments.iter().map(|s| s.object_size).sum();
    let rows = snapshot.segments.iter().map(|s| s.sample_count).sum();
    let layout = if snapshot
        .segments
        .iter()
        .any(|s| matches!(s.level, SegmentLevel::L1 { .. }))
    {
        Compaction::Post.label()
    } else {
        Compaction::Pre.label()
    };
    Ok(DatasetInfo {
        load_wall_ms,
        stored_bytes,
        object_count: snapshot.segments.len(),
        rows,
        layout: layout.to_string(),
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
    // The generated lane writes shard 0 only by construction (see
    // `generate_dataset`), so a single shard resolves the whole dataset.
    let shard_count = 1;
    let dataset = dataset_info(
        &cfg.store,
        tenant_hash,
        window,
        NOW_NS,
        Some(load_wall_ms),
        shard_count,
    )
    .await?;
    let settings = ExecutorSettings {
        max_query_bytes: cfg.max_query_bytes,
        shard_count,
        fetch_concurrency: cfg.fetch_concurrency,
        tenant_max_bytes: cfg.tenant_max_bytes,
        parallel_final_aggregation: cfg.parallel_final_aggregation,
        max_segments: cfg.max_segments,
        logs_suffix_len: cfg.logs_suffix_len,
        ..ExecutorSettings::default()
    };
    let (entries, skipped, failed) = measure_corpus(
        &cfg.store,
        tenant_hash,
        &cfg.entries,
        &declared,
        cfg.runs,
        window,
        NOW_NS,
        cfg.cache_bytes,
        cfg.deadline,
        cfg.continue_on_error,
        cfg.progress_jsonl.as_deref(),
        settings,
        cfg.warm_catalog,
        cfg.explain_dir.as_deref(),
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
            cache_bytes: cfg.cache_bytes,
            deadline_secs: cfg.deadline.as_secs(),
            fetch_concurrency: cfg.fetch_concurrency.max(1),
            sql_max_query_bytes_requested: cfg.max_query_bytes,
            // In-process lane: the requested ceiling reaches the executor's
            // `SqlConfig`, so it is also the effective one.
            sql_max_query_bytes_effective: Some(cfg.max_query_bytes),
            tenant_max_bytes: cfg.tenant_max_bytes.max(1),
            sql_max_segments: cfg.max_segments,
            parallel_final_aggregation_requested: cfg.parallel_final_aggregation,
            // In-process lane: the requested value reaches the executor's
            // `SqlConfig`, so it is also the effective one.
            parallel_final_aggregation_effective: Some(cfg.parallel_final_aggregation),
            explain: cfg.explain_dir.is_some(),
            // The generate lane is always in-process (no Flight target), so the
            // flag governed the run it describes.
            warm_catalog: recorded_warm_catalog(false, cfg.warm_catalog),
            // In-process lane: the pinned window (or the derivation, when unset)
            // reaches the fetcher, so the requested value is the effective one.
            logs_suffix_len: cfg.logs_suffix_len,
            flight_endpoint: None,
        },
        dataset,
        entries,
        skipped,
        failed,
    })
}

/// Run the loaded-tenant lane: resolve the tenant's real durable declaration,
/// verify each entry against it, and measure the statements it satisfies.
///
/// Kept deliberately thin over [`measure_corpus`]/[`dataset_info`]: the only
/// lane-specific step is resolving the durable declaration, so the generated
/// lane (which the smoke test drives) exercises the rest of this path too.
/// The executor knobs the loaded-tenant lane builds from its config. Pulled out
/// so the config-to-fetcher seam is testable in one place: a test can assert the
/// pinned `logs_suffix_len` survives this mapping (issue #883) instead of the
/// mapping being buried inline where only an end-to-end run could catch a
/// dropped field. `shard_count` is resolved separately (issue #677) and passed
/// in.
fn tenant_executor_settings(cfg: &TenantConfigInput, shard_count: u32) -> ExecutorSettings {
    ExecutorSettings {
        max_query_bytes: cfg.max_query_bytes,
        shard_count,
        fetch_concurrency: cfg.fetch_concurrency,
        tenant_max_bytes: cfg.tenant_max_bytes,
        parallel_final_aggregation: cfg.parallel_final_aggregation,
        max_segments: cfg.max_segments,
        logs_suffix_len: cfg.logs_suffix_len,
        ..ExecutorSettings::default()
    }
}

pub async fn run_tenant(cfg: &TenantConfigInput) -> Result<SqlLatencyReport, Error> {
    let tenant = TenantId::new(cfg.tenant.clone());
    let tenant_hash = tenant.hash();

    // How many shards to resolve over. Before #677 this was implicitly 1
    // (`CatalogConfig::default()`), so a multi-shard tenant was measured over
    // shard 0 alone and any tenant whose data all sat on a higher shard read as
    // empty. Resolve it up front, fail-closed, from the operator flag or the
    // durable provisioning record.
    let now_hour = (cfg.now_ns / NS_PER_HOUR).max(0) as u32;
    let shard_count =
        resolve_shard_count(&cfg.store, &tenant_hash, &cfg.tenant, cfg.shards, now_hour).await?;

    // The configuration under measurement: the tenant's real durable declared
    // columns, not a set this harness installs. An absent config, or a config
    // with no typed columns, means the tenant declared nothing.
    let declared = resolve_durable_declarations(&cfg.store, &tenant_hash).await?;

    let dataset = dataset_info(
        &cfg.store,
        tenant_hash,
        cfg.window,
        cfg.now_ns,
        None,
        shard_count,
    )
    .await?;
    // Fail closed on an empty resolve: a report over zero objects is never a
    // valid measurement, and silently producing one hides a wrong window,
    // tenant, or shard count.
    if dataset.object_count == 0 {
        return Err(TenantLaneError::EmptyDataset {
            tenant: cfg.tenant.clone(),
            shard_count,
            start_ns: cfg.window.start_ns,
            end_ns: cfg.window.end_ns,
            now_ns: cfg.now_ns,
        }
        .into());
    }
    // `dataset.layout` above is always the observed value; this is a separate,
    // optional check of the operator's belief against it (issue #834). Refuse
    // rather than let a stale `--compaction` claim sit next to a report that
    // silently disagrees with it.
    if let Some(asserted) = cfg.compaction {
        let observed_post = dataset.layout == Compaction::Post.label();
        if (asserted == Compaction::Post) != observed_post {
            return Err(TenantLaneError::CompactionMismatch {
                tenant: cfg.tenant.clone(),
                asserted: asserted.label(),
                observed: dataset.layout.clone(),
                object_count: dataset.object_count,
            }
            .into());
        }
    }
    let settings = tenant_executor_settings(cfg, shard_count);
    // The two lanes measure the same statements over the same dataset stanza
    // and the same declared-column set; they differ only in what executes the
    // SQL, in process or across a gRPC channel to a server.
    let (entries, skipped, failed) = match &cfg.flight {
        Some(target) => measure_over_flight(target, cfg, &declared).await?,
        None => {
            measure_corpus(
                &cfg.store,
                tenant_hash,
                &cfg.entries,
                &declared,
                cfg.runs,
                cfg.window,
                cfg.now_ns,
                cfg.cache_bytes,
                cfg.deadline,
                cfg.continue_on_error,
                cfg.progress_jsonl.as_deref(),
                settings,
                cfg.warm_catalog,
                cfg.explain_dir.as_deref(),
            )
            .await?
        }
    };

    Ok(SqlLatencyReport {
        provenance: Provenance {
            store_backend: cfg.store_backend.clone(),
            region: cfg.region.clone(),
            endpoint: cfg.endpoint.clone(),
            host_logical_cores: host_logical_cores(),
            source: match &cfg.flight {
                Some(_) => "flight".to_string(),
                None => "tenant".to_string(),
            },
            dataset_id: cfg.tenant.clone(),
            runs: cfg.runs.max(1),
            cache_bytes: cfg.cache_bytes,
            deadline_secs: cfg.deadline.as_secs(),
            fetch_concurrency: cfg.fetch_concurrency.max(1),
            sql_max_query_bytes_requested: cfg.max_query_bytes,
            // `settings` is passed only on the in-process arm of the match
            // above, so on the Flight lane this ceiling never left the process
            // and the server's own config governed instead.
            sql_max_query_bytes_effective: match &cfg.flight {
                Some(_) => None,
                None => Some(cfg.max_query_bytes),
            },
            tenant_max_bytes: cfg.tenant_max_bytes.max(1),
            sql_max_segments: cfg.max_segments,
            parallel_final_aggregation_requested: cfg.parallel_final_aggregation,
            // The Flight lane does not send this setting to the server (it is not
            // a Flight header), so what governed execution is the server's own
            // config, unknown to this process: record `None`. An in-process
            // tenant run applies the requested value directly, so it is effective.
            parallel_final_aggregation_effective: match &cfg.flight {
                Some(_) => None,
                None => Some(cfg.parallel_final_aggregation),
            },
            explain: cfg.explain_dir.is_some(),
            // Not-applicable on the Flight lane: it builds no in-process
            // executor, so `--warm-catalog` governed nothing there (issue #857
            // review). The in-process tenant lane applies it directly.
            warm_catalog: recorded_warm_catalog(cfg.flight.is_some(), cfg.warm_catalog),
            // `settings` reaches the fetcher only on the in-process arm of the
            // match above; the Flight lane never applies the pinned window (it
            // is not on the wire and no server flag corresponds to it), so it
            // governed nothing there and records `None`, exactly as the
            // effective-ceiling fields do.
            logs_suffix_len: match &cfg.flight {
                Some(_) => None,
                None => cfg.logs_suffix_len,
            },
            flight_endpoint: cfg.flight.as_ref().map(|t| t.endpoint.clone()),
        },
        dataset,
        entries,
        skipped,
        failed,
    })
}

/// Resolve the shard count the tenant lane measures over (issue #677).
///
/// `override_shards` is the operator's `--shards`. The durable provisioning
/// record (`ravel-cli load` writes it via `validate_or_adopt`) carries the
/// shard-generation history; [`shard_ceiling`] over it at `now_hour` is the max
/// shard count active at or before that hour, which a resolve over the window
/// must scan. The four cases:
///
/// - flag and record present: they must agree, else refuse (never silently
///   prefer one);
/// - flag only (no record): trust the flag -- this is a tenant loaded before
///   provisioning records existed;
/// - record only (no flag): use the record's ceiling;
/// - neither: refuse, naming the tenant and telling the operator to pass
///   `--shards`.
async fn resolve_shard_count(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant_hash: &TenantHash,
    tenant: &str,
    override_shards: Option<u32>,
    now_hour: u32,
) -> Result<u32, Error> {
    let generations =
        read_generations_from_store(store.as_ref(), tenant_hash, Signal::Logs).await?;
    match (override_shards, generations) {
        (Some(requested), Some(gens)) => {
            let ceiling = shard_ceiling(&gens, now_hour);
            if requested != ceiling {
                return Err(TenantLaneError::ShardCountDisagreement {
                    tenant: tenant.to_string(),
                    requested,
                    ceiling,
                }
                .into());
            }
            Ok(requested)
        }
        (Some(requested), None) => Ok(requested),
        (None, Some(gens)) => Ok(shard_ceiling(&gens, now_hour)),
        (None, None) => Err(TenantLaneError::NoProvisioningRecord {
            tenant: tenant.to_string(),
        }
        .into()),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_corpus::Modification;
    use ravel_catalog::{AbsentPolicy, validate_or_adopt};
    use ravel_object_store::memory::MemoryStore;

    fn empty_store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    /// Write `objects` RLOG objects (one record each) on `shard` for `tenant`,
    /// each with its own commit record, so a real `Catalog::resolve` over the
    /// shard finds them. Mirrors [`generate_dataset`] but parameterized by shard
    /// (distinct writer id per shard, so keys never collide) -- the generated
    /// lane only ever writes shard 0.
    async fn write_shard_objects(
        store: &Arc<dyn ObjectStoreBackend>,
        tenant: &TenantId,
        shard: u32,
        objects: usize,
    ) -> Vec<Vec<u8>> {
        write_records_as_objects(store, tenant, shard, &build_records(objects.max(1), 0)).await
    }

    /// One RLOG object per record in `records`, each with its own commit
    /// record, on `shard`. Returns the object bytes in write order, so a test
    /// that needs the object's own layout (section offsets, total size) reads it
    /// from what was written rather than rebuilding a second copy that could
    /// drift from it.
    async fn write_records_as_objects(
        store: &Arc<dyn ObjectStoreBackend>,
        tenant: &TenantId,
        shard: u32,
        records: &[LogRecord],
    ) -> Vec<Vec<u8>> {
        let mut written = Vec::with_capacity(records.len());
        let writer_id = Uuid::from_u128(0x5100_0100 + u128::from(shard));
        for (obj_idx, rec) in records.iter().enumerate() {
            let writer_seq = (obj_idx + 1) as u64;
            let identity = ObjectIdentity {
                tenant_hash: tenant.hash().0,
                shard,
                writer_id: *writer_id.as_bytes(),
                writer_epoch: 1,
                writer_seq,
            };
            let mut writer = RlogWriter::new(RlogConfig::default(), identity);
            writer.push(rec.clone()).expect("push record");
            let bytes = writer.finish().expect("finish object");
            let new_record = NewCommitRecord {
                tenant_hash: tenant.hash(),
                signal: Signal::Logs,
                shard,
                writer_id,
                writer_epoch: 1,
                writer_seq,
                object_size: bytes.len() as u64,
                content_hash: [0u8; 32],
                sample_count: 1,
                series_count: 1,
                min_event_ts_ns: rec.ts_ns,
                max_event_ts_ns: rec.ts_ns,
                min_ingest_ts_ns: rec.ts_ns,
                max_ingest_ts_ns: rec.ts_ns,
                segment_format_version: u32::from(ravel_logseg::footer::VERSION),
                created_unix_ns: 10,
                ingest_hour_bucket: 0,
            };
            let built = record::build(new_record).expect("build commit record");
            let data_key = keys::reconstruct_data_key(&built).expect("data key");
            store
                .put(
                    &data_key,
                    bytes::Bytes::from(bytes.clone()),
                    PutOptions::default(),
                )
                .await
                .expect("put object");
            publish::publish(store.as_ref(), &built, &RetryPolicy::default())
                .await
                .expect("publish commit record");
            written.push(bytes);
        }
        written
    }

    /// A tenant-lane config for the shard/cache tests: memory provenance, no
    /// corpus entries (these tests exercise resolution and fail-closed paths,
    /// not statement timing).
    fn tenant_cfg(
        store: &Arc<dyn ObjectStoreBackend>,
        tenant: &str,
        shards: Option<u32>,
        cache_bytes: u64,
    ) -> TenantConfigInput {
        TenantConfigInput {
            store: Arc::clone(store),
            store_backend: "memory".to_string(),
            region: "n/a".to_string(),
            endpoint: "n/a".to_string(),
            tenant: tenant.to_string(),
            entries: Vec::new(),
            runs: 1,
            window: TimeRange {
                start_ns: 0,
                end_ns: NOW_NS,
            },
            now_ns: NOW_NS,
            compaction: None,
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            shards,
            cache_bytes,
            deadline: Duration::from_secs(30),
            continue_on_error: false,
            fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
            progress_jsonl: None,
            tenant_max_bytes: DEFAULT_TENANT_MAX_BYTES,
            parallel_final_aggregation: false,
            max_segments: DEFAULT_MAX_SEGMENTS,
            explain_dir: None,
            flight: None,
            warm_catalog: false,
            logs_suffix_len: None,
        }
    }

    /// Every statement outcome is appended to the progress file as a JSON
    /// line the moment it is known, one line per statement in run order, so a
    /// run cut short still leaves every finished number on disk. The final
    /// report is built from the same values.
    #[tokio::test]
    async fn progress_jsonl_carries_every_outcome_in_order() {
        let store = empty_store();
        provisioned_tenant(&store, "progress-tenant").await;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("progress.jsonl");

        let mut cfg = tenant_cfg(&store, "progress-tenant", None, 0);
        cfg.entries = vec![
            entry("fine", "SELECT body FROM logs"),
            entry("broken", "SELECT no_such_column FROM logs"),
            entry("also_fine", "SELECT ts FROM logs"),
        ];
        cfg.continue_on_error = true;
        cfg.progress_jsonl = Some(path.clone());
        let report = run_tenant(&cfg).await.expect("tenant lane runs");

        let lines: Vec<EntryEvent> = std::fs::read_to_string(&path)
            .expect("progress file")
            .lines()
            .map(|l| serde_json::from_str(l).expect("each line is an EntryEvent"))
            .collect();
        let ids: Vec<(&str, &str)> = lines
            .iter()
            .map(|e| match e {
                EntryEvent::Measured(m) => ("measured", m.id.as_str()),
                EntryEvent::Skipped(s) => ("skipped", s.id.as_str()),
                EntryEvent::Failed(f) => ("failed", f.id.as_str()),
            })
            .collect();
        assert_eq!(
            ids,
            vec![
                ("measured", "fine"),
                ("failed", "broken"),
                ("measured", "also_fine")
            ],
            "one line per statement, in run order, each with its outcome"
        );
        assert_eq!(report.entries.len(), 2);
        assert_eq!(report.failed.len(), 1);
        let EntryEvent::Measured(first) = &lines[0] else {
            panic!("first line is the measured entry");
        };
        assert_eq!(
            first.rows_returned, report.entries[0].rows_returned,
            "the streamed line carries the same numbers the report does"
        );
    }

    /// A corpus entry with no declared-column dependency, so the tenant lane
    /// executes it rather than skipping it.
    fn entry(id: &str, sql: &str) -> CorpusEntry {
        CorpusEntry {
            id: id.to_string(),
            sql: sql.to_string(),
            constructs: Vec::new(),
            expected_rows: None,
            upstream_id: None,
            modified: Modification::Verbatim,
            required_declarations: Vec::new(),
        }
    }

    /// A 1-shard tenant with a provisioning record and two objects, for the
    /// failure-path tests below.
    async fn provisioned_tenant(store: &Arc<dyn ObjectStoreBackend>, name: &str) {
        let tenant = TenantId::new(name);
        write_shard_objects(store, &tenant, 0, 2).await;
        validate_or_adopt(
            store.as_ref(),
            &tenant.hash(),
            Signal::Logs,
            1,
            10,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("write provisioning record");
    }

    /// A 1-shard tenant with `objects` hour-0 objects, a provisioning record,
    /// and a fold at `NOW_NS` so those objects are sealed below the fold
    /// watermark. ADR-0073 decision 2 counts only sealed, below-watermark
    /// segments against `max_segments`, so before this fold the objects are
    /// `Recent` (exempt) and no ceiling can be tripped; after it they are
    /// `SealedBelowWatermark` and counted. `NOW_NS` (4 h) minus the default
    /// seal margin (~80 min) seals hour buckets at or below hour 1, so hour 0
    /// is sealed. Mirrors `ravel-cli catalog fold` (with provisioning
    /// enforcement, `Signal::Logs`).
    async fn folded_tenant(store: &Arc<dyn ObjectStoreBackend>, name: &str, objects: usize) {
        let tenant = TenantId::new(name);
        write_shard_objects(store, &tenant, 0, objects).await;
        validate_or_adopt(
            store.as_ref(),
            &tenant.hash(),
            Signal::Logs,
            1,
            10,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("write provisioning record");
        let catalog = Catalog::new(
            Arc::clone(store),
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
                Signal::Logs,
                Uuid::new_v4(),
                NOW_NS,
                &[],
                None,
            )
            .await
            .expect("fold seals hour 0 below the watermark");
    }

    /// A folded tenant's sealed segments count against `--sql-max-segments`:
    /// below the sealed count every statement is refused with the typed
    /// `TooManySegments` error carried verbatim in `failed`; raised above it
    /// every statement measures. Reverting `cold_executor` to
    /// `EngineConfig::default()` (segments unthreaded) leaves the ceiling at
    /// 1024 for both arms, so the low-ceiling arm below stops failing: the
    /// `report.failed` assertion flips red.
    #[tokio::test]
    async fn sql_max_segments_gate_blocks_folded_tenant_then_raised_measures() {
        let store = empty_store();
        // Five sealed hour-0 objects, above the ceiling the first arm sets.
        folded_tenant(&store, "maxseg-tenant", 5).await;

        // Ceiling below the sealed count: every statement fails to fan out.
        let mut cfg = tenant_cfg(&store, "maxseg-tenant", Some(1), 0);
        cfg.entries = vec![entry("q", "SELECT body FROM logs")];
        cfg.continue_on_error = true;
        cfg.max_segments = 2;
        let report = run_tenant(&cfg)
            .await
            .expect("run completes, recording the refusal");
        assert!(
            report.entries.is_empty(),
            "no statement measures under a ceiling below the sealed count"
        );
        assert_eq!(report.failed.len(), 1, "the one statement was refused");
        assert_eq!(report.failed[0].id, "q");
        assert!(
            report.failed[0]
                .error
                .contains("query fans out over too many segments: 5 exceeds max 2"),
            "the typed TooManySegments error is carried verbatim: {}",
            report.failed[0].error
        );
        assert_eq!(
            report.provenance.sql_max_segments, 2,
            "the ceiling under measurement is recorded in provenance"
        );

        // Raised above the sealed count: every statement measures.
        let mut cfg = tenant_cfg(&store, "maxseg-tenant", Some(1), 0);
        cfg.entries = vec![entry("q", "SELECT body FROM logs")];
        cfg.max_segments = DEFAULT_MAX_SEGMENTS;
        let report = run_tenant(&cfg)
            .await
            .expect("run measures every statement once the ceiling is raised");
        assert!(
            report.failed.is_empty(),
            "no refusal once the ceiling clears the sealed count"
        );
        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].id, "q");
        assert_eq!(
            report.provenance.sql_max_segments, DEFAULT_MAX_SEGMENTS,
            "the raised ceiling is recorded in provenance"
        );
    }

    /// `--explain` writes one physical-plan file per measured statement, and an
    /// aggregate's plan names `AggregateExec`; with the flag off nothing is
    /// written. The plans are a side artifact: the numbers are unaffected and
    /// `provenance.explain` records that the flag was on.
    #[tokio::test]
    async fn explain_writes_physical_plan_per_statement_and_off_writes_nothing() {
        let store = empty_store();
        provisioned_tenant(&store, "explain-tenant").await;

        let on_dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = tenant_cfg(&store, "explain-tenant", None, 0);
        cfg.entries = vec![
            // A GROUP BY aggregate: `count(*)` alone is constant-folded by the
            // AggregateStatistics rule into a `ProjectionExec` over a
            // `PlaceholderRowExec` (no `AggregateExec` at all), which is itself
            // an optimizer outcome `--explain` is meant to surface. A GROUP BY
            // key defeats that fold and leaves a real `AggregateExec`.
            entry("agg", "SELECT body, count(*) FROM logs GROUP BY body"),
            entry("scan", "SELECT body FROM logs"),
        ];
        cfg.explain_dir = Some(on_dir.path().to_path_buf());
        let report = run_tenant(&cfg)
            .await
            .expect("run measures with explain on");
        assert_eq!(report.entries.len(), 2, "both statements still measured");
        assert!(report.provenance.explain, "provenance records explain on");

        let agg_plan = std::fs::read_to_string(on_dir.path().join("agg.txt"))
            .expect("aggregate statement's explain file exists");
        assert!(
            agg_plan.contains("AggregateExec"),
            "an aggregate's physical plan names AggregateExec: {agg_plan}"
        );
        assert!(
            on_dir.path().join("scan.txt").exists(),
            "one explain file per statement, keyed by id"
        );

        // Off: the same corpus writes no plan and provenance says so.
        let off_dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = tenant_cfg(&store, "explain-tenant", None, 0);
        cfg.entries = vec![entry("agg", "SELECT count(*) FROM logs")];
        let report = run_tenant(&cfg)
            .await
            .expect("run measures with explain off");
        assert!(!report.provenance.explain, "provenance records explain off");
        assert!(
            !off_dir.path().join("agg.txt").exists(),
            "explain off writes no plan file"
        );
    }

    /// An in-process (`tenant`) run applies its `parallel_final_aggregation`
    /// request directly to the executor's `SqlConfig`, so the report records it
    /// under both `_requested` and `_effective`, equal. (The Flight lane, where
    /// the setting is not on the wire and `_effective` is `None`, is pinned by
    /// `sql_latency_flight_smoke.rs` under the `flight-lane` feature.)
    #[tokio::test]
    async fn in_process_report_records_requested_and_effective_equal() {
        let store = empty_store();
        provisioned_tenant(&store, "agg-tenant").await;

        let mut cfg = tenant_cfg(&store, "agg-tenant", None, 0);
        cfg.entries = vec![entry("scan", "SELECT body FROM logs")];
        cfg.parallel_final_aggregation = true;
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert!(
            report.provenance.parallel_final_aggregation_requested,
            "the requested field carries the config value"
        );
        assert_eq!(
            report.provenance.parallel_final_aggregation_effective,
            Some(true),
            "an in-process run's effective value is the applied request"
        );

        cfg.parallel_final_aggregation = false;
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert!(!report.provenance.parallel_final_aggregation_requested);
        assert_eq!(
            report.provenance.parallel_final_aggregation_effective,
            Some(false),
            "requested and effective stay equal for an in-process run"
        );
    }

    /// `--max-segments` must land on `EngineConfig::max_segments`, not stop at a
    /// parsed field. A value distinct from the compiled-in default proves the
    /// override is wired; its companion (the default arm) guards against a
    /// silent-drop regression passing on a coincidental default.
    #[test]
    fn cold_executor_threads_max_segments_override() {
        let store = empty_store();
        let custom = DEFAULT_MAX_SEGMENTS * 2;
        assert_ne!(custom, DEFAULT_MAX_SEGMENTS);
        let executor = cold_executor(
            &store,
            &[],
            None,
            ExecutorSettings {
                max_segments: custom,
                ..ExecutorSettings::default()
            },
        )
        .expect("build executor")
        .executor;
        assert_eq!(executor.config().engine.max_segments, custom);
        let executor = cold_executor(&store, &[], None, ExecutorSettings::default())
            .expect("build executor")
            .executor;
        assert_eq!(
            executor.config().engine.max_segments,
            DEFAULT_MAX_SEGMENTS,
            "an omitted flag leaves the compiled-in ceiling unchanged"
        );
    }

    /// With `continue_on_error`, a statement that fails to execute lands in
    /// `failed` (id, run index, verbatim error) and the statements after it are
    /// still measured. Without it, the same corpus aborts the run at the first
    /// failure, which is the behaviour every run had before issue #688.
    #[tokio::test]
    async fn continue_on_error_records_failure_and_measures_later_entries() {
        let store = empty_store();
        provisioned_tenant(&store, "continue-tenant").await;
        let entries = vec![
            entry("broken", "SELECT no_such_column FROM logs"),
            entry("fine", "SELECT body FROM logs"),
        ];

        let mut cfg = tenant_cfg(&store, "continue-tenant", None, 0);
        cfg.entries = entries.clone();
        cfg.continue_on_error = true;
        let report = run_tenant(&cfg)
            .await
            .expect("run continues past the failure");
        assert_eq!(report.failed.len(), 1, "exactly the broken entry failed");
        assert_eq!(report.failed[0].id, "broken");
        assert_eq!(report.failed[0].run, 0, "it failed on the cold run");
        assert!(
            !report.failed[0].error.is_empty(),
            "the executor's error is carried verbatim"
        );
        assert_eq!(
            report
                .entries
                .iter()
                .map(|e| e.id.as_str())
                .collect::<Vec<_>>(),
            vec!["fine"],
            "the entry after the failure is measured, not dropped"
        );
        assert_eq!(report.entries[0].rows_returned, 2);

        let mut cfg = tenant_cfg(&store, "continue-tenant", None, 0);
        cfg.entries = entries;
        cfg.continue_on_error = false;
        let err = run_tenant(&cfg)
            .await
            .expect_err("without continue_on_error the first failure aborts the run");
        assert!(
            err.to_string().contains("entry `broken` failed to execute"),
            "abort names the entry: {err}"
        );
    }

    /// The configured deadline reaches the executor. `MemoryStore` answers
    /// every read without yielding, so a statement over it can finish inside a
    /// zero budget; a `FaultStore` gate that holds every GET makes the fetch
    /// genuinely pending, and the configured deadline is what ends the wait.
    #[tokio::test]
    async fn deadline_is_threaded_to_the_executor() {
        use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};

        let faulty = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
        let tenant = TenantId::new("deadline-tenant");
        write_shard_objects(&store, &tenant, 0, 2).await;
        let gate = faulty.hold(Op::Get, None, Occurrence::Always);

        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };
        let (measured, skipped, failed) = measure_corpus(
            &store,
            tenant.hash(),
            &[entry("q", "SELECT body FROM logs")],
            &[],
            1,
            window,
            NOW_NS,
            0,
            Duration::from_millis(20),
            true,
            None,
            ExecutorSettings::default(),
            false,
            None,
        )
        .await
        .expect("run continues past the expiry");
        assert!(measured.is_empty());
        assert!(skipped.is_empty());
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].id, "q");
        assert!(
            failed[0].error.contains("20 ms wall deadline"),
            "the statement expired under the configured budget: {}",
            failed[0].error
        );
        assert!(
            gate.held_count() >= 1,
            "the expiry came from a GET the gate was holding, not from anything else"
        );
    }

    /// `fetch_concurrency` bounds the in-flight GETs, not just the partition
    /// count: with every GET held behind a gate, an executor built at 24
    /// parks 24 of them. Before issue #700 the logs fetcher kept its own
    /// compiled-in cap of 16 whatever the knob said, so this waited forever
    /// at 16.
    #[tokio::test]
    async fn fetch_concurrency_bounds_in_flight_gets() {
        use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};

        const FETCH_CONCURRENCY: usize = 24;
        let faulty = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
        let tenant = TenantId::new("gets-tenant");
        // The permit pool guards the block-range path only (objects above the
        // ADR-0107 threshold); a small object is one whole-object GET that
        // never takes a permit. So every object here carries a
        // poorly-compressible body well above the threshold, making the pool
        // the thing under test.
        let mut records = build_records(40, 0);
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        for rec in &mut records {
            let mut body = String::with_capacity(2 << 20);
            while body.len() < (2 << 20) {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let sym = ((seed >> 58) as u8) % 62;
                body.push(char::from(match sym {
                    0..=25 => b'a' + sym,
                    26..=51 => b'A' + (sym - 26),
                    _ => b'0' + (sym - 52),
                }));
            }
            rec.body = body;
        }
        write_records_as_objects(&store, &tenant, 0, &records).await;
        // Hold data-object reads only: the catalog resolve's own GETs (`l/HEAD`,
        // commit records) run before any partition exists, and holding those
        // would stop the query before the pool is ever touched.
        let gate = faulty.hold(Op::Get, Some("/l/l0/".to_string()), Occurrence::Always);

        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };
        let run = tokio::spawn({
            let store = Arc::clone(&store);
            let th = tenant.hash();
            async move {
                measure_corpus(
                    &store,
                    th,
                    &[entry("q", "SELECT body FROM logs")],
                    &[],
                    1,
                    window,
                    NOW_NS,
                    0,
                    Duration::from_secs(30),
                    true,
                    None,
                    ExecutorSettings {
                        fetch_concurrency: FETCH_CONCURRENCY,
                        ..ExecutorSettings::default()
                    },
                    false,
                    None,
                )
                .await
            }
        });
        if tokio::time::timeout(
            Duration::from_secs(5),
            gate.wait_until_held(FETCH_CONCURRENCY),
        )
        .await
        .is_err()
        {
            panic!(
                "fetch_concurrency GETs are in flight at once, not the compiled-in 16: held {} \
                 of {} after 5 s; held keys: {:?}",
                gate.held_count(),
                FETCH_CONCURRENCY,
                gate.held_details()
                    .iter()
                    .map(|(_, op, key)| format!("{op:?} {key}"))
                    .take(4)
                    .collect::<Vec<_>>()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            gate.held_count(),
            FETCH_CONCURRENCY,
            "in-flight GETs are bounded by fetch_concurrency"
        );
        run.abort();
    }

    /// `--sql-tenant-max-bytes` and `--sql-parallel-final-aggregation` must
    /// reach the executor, not stop at parsed fields. The tenant ceiling is a
    /// SECOND limit under `max_query_bytes`: raising only the per-query pool
    /// leaves a heavy aggregate refused by the tenant accountant with a
    /// different error (issue #680 hit exactly that on 18 ClickBench
    /// statements). Under the #741 amendment the default is on, so the override
    /// threaded here is the `false` opt-out; both directions must reach the
    /// executor.
    #[test]
    fn cold_executor_threads_tenant_and_parallel_agg_overrides() {
        let store = empty_store();
        let custom_tenant = DEFAULT_TENANT_MAX_BYTES * 8;
        assert_ne!(custom_tenant, DEFAULT_TENANT_MAX_BYTES);
        let executor = cold_executor(
            &store,
            &[],
            None,
            ExecutorSettings {
                tenant_max_bytes: custom_tenant,
                parallel_final_aggregation: false,
                ..ExecutorSettings::default()
            },
        )
        .expect("build executor")
        .executor;
        assert_eq!(executor.max_tenant_bytes(), custom_tenant);
        assert!(
            !executor.config().parallel_final_aggregation,
            "the false opt-out must reach the executor"
        );

        let executor = cold_executor(&store, &[], None, ExecutorSettings::default())
            .expect("build executor")
            .executor;
        assert_eq!(executor.max_tenant_bytes(), DEFAULT_TENANT_MAX_BYTES);
        assert!(
            executor.config().parallel_final_aggregation,
            "the amended default (issue #741) reaches the executor as on"
        );
    }

    /// The deadline a run used is part of its provenance.
    #[tokio::test]
    async fn deadline_is_recorded_in_provenance() {
        let store = empty_store();
        provisioned_tenant(&store, "deadline-prov-tenant").await;

        let mut cfg = tenant_cfg(&store, "deadline-prov-tenant", None, 0);
        cfg.entries = vec![entry("q", "SELECT body FROM logs")];
        cfg.deadline = Duration::from_secs(7);
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(report.provenance.deadline_secs, 7);
        assert!(report.failed.is_empty());
        assert_eq!(report.entries.len(), 1);
    }

    /// A 2-shard tenant with objects on shard 0 AND shard 1 is measured over
    /// both shards. The pre-#677 behaviour (a shard_count of 1, which is what
    /// `CatalogConfig::default()` set) resolves shard 0 alone and undercounts;
    /// the fix resolves the record's ceiling (2) and counts every object.
    #[tokio::test]
    async fn tenant_lane_object_count_spans_all_shards() {
        let store = empty_store();
        let tenant = TenantId::new("shard-span-tenant");
        let th = tenant.hash();
        write_shard_objects(&store, &tenant, 0, 3).await;
        write_shard_objects(&store, &tenant, 1, 2).await;
        validate_or_adopt(
            store.as_ref(),
            &th,
            Signal::Logs,
            2,
            10,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("write provisioning record");

        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };

        // Pre-fix: shard_count 1 resolves shard 0 only.
        let pre = dataset_info(&store, th, window, NOW_NS, None, 1)
            .await
            .expect("resolve at shard_count 1");
        assert_eq!(
            pre.object_count, 3,
            "shard_count=1 sees only shard 0's 3 objects (the pre-#677 undercount)"
        );

        // Fix: shard_count 2 counts both shards.
        let post = dataset_info(&store, th, window, NOW_NS, None, 2)
            .await
            .expect("resolve at shard_count 2");
        assert_eq!(
            post.object_count, 5,
            "shard_count=2 counts shard 0's 3 plus shard 1's 2"
        );

        // End to end: run_tenant resolves the count from the record, so it too
        // reports 5.
        let cfg = tenant_cfg(&store, "shard-span-tenant", None, 0);
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(
            report.dataset.object_count, 5,
            "run_tenant resolves both shards from the provisioning record"
        );
    }

    /// A resolve that finds zero objects is a fail-closed error naming what it
    /// resolved, never an `Ok` report over an empty dataset (the pre-#677
    /// behaviour, which returned `object_count == 0` silently).
    #[tokio::test]
    async fn tenant_lane_empty_dataset_is_error() {
        let store = empty_store();
        let tenant = TenantId::new("empty-tenant");
        let th = tenant.hash();
        validate_or_adopt(
            store.as_ref(),
            &th,
            Signal::Logs,
            2,
            10,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("write provisioning record");
        // No objects written.
        let cfg = tenant_cfg(&store, "empty-tenant", None, 0);
        let err = run_tenant(&cfg)
            .await
            .expect_err("a 0-object resolve must be an error");
        match err.downcast_ref::<TenantLaneError>() {
            Some(TenantLaneError::EmptyDataset {
                tenant,
                shard_count,
                ..
            }) => {
                assert_eq!(tenant, "empty-tenant");
                assert_eq!(*shard_count, 2);
            }
            other => panic!("expected EmptyDataset, got {other:?}"),
        }
    }

    /// A tenant with no provisioning record and no `--shards` is refused: its
    /// shard count is unknowable, so measuring it would silently fall back to
    /// shard 0.
    #[tokio::test]
    async fn tenant_lane_missing_record_without_shards_is_error() {
        let store = empty_store();
        let cfg = tenant_cfg(&store, "no-record-tenant", None, 0);
        let err = run_tenant(&cfg)
            .await
            .expect_err("a missing record with no --shards must be an error");
        assert!(
            matches!(
                err.downcast_ref::<TenantLaneError>(),
                Some(TenantLaneError::NoProvisioningRecord { .. })
            ),
            "expected NoProvisioningRecord, got {err}"
        );
    }

    /// `--shards` that contradicts the record's shard ceiling is refused rather
    /// than silently preferring one over the other.
    #[tokio::test]
    async fn tenant_lane_shards_disagreeing_with_record_is_error() {
        let store = empty_store();
        let tenant = TenantId::new("disagree-tenant");
        let th = tenant.hash();
        write_shard_objects(&store, &tenant, 0, 1).await;
        write_shard_objects(&store, &tenant, 1, 1).await;
        validate_or_adopt(
            store.as_ref(),
            &th,
            Signal::Logs,
            2,
            10,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("write provisioning record");
        let cfg = tenant_cfg(&store, "disagree-tenant", Some(3), 0);
        let err = run_tenant(&cfg)
            .await
            .expect_err("a --shards that disagrees with the record must error");
        match err.downcast_ref::<TenantLaneError>() {
            Some(TenantLaneError::ShardCountDisagreement {
                requested, ceiling, ..
            }) => {
                assert_eq!((*requested, *ceiling), (3, 2));
            }
            other => panic!("expected ShardCountDisagreement, got {other:?}"),
        }
    }

    /// The cold run 0 GET count of every entry, in entry order. Index 0 of
    /// `per_run_accounting` is the cold run; the pooled `QueryAccounting` the
    /// executor surfaces is all the phase attribution `SqlExecutor::execute`
    /// exposes (issue #857 report), and the catalog resolve's GETs are a
    /// component of it.
    fn cold_gets(entries: &[EntryReport]) -> Vec<u64> {
        entries
            .iter()
            .map(|e| {
                e.per_run_accounting
                    .as_ref()
                    .expect("in-process lane records per-run accounting")[0]
                    .object_store_get_requests
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    async fn measure_same_statement(
        store: &Arc<dyn ObjectStoreBackend>,
        tenant_hash: TenantHash,
        statements: usize,
        warm_catalog: bool,
    ) -> Vec<EntryReport> {
        let entries: Vec<CorpusEntry> = (0..statements)
            .map(|i| entry(&format!("q{i}"), "SELECT body FROM logs"))
            .collect();
        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };
        let (measured, _skipped, failed) = measure_corpus(
            store,
            tenant_hash,
            &entries,
            &[],
            1,
            window,
            NOW_NS,
            0,
            Duration::from_secs(30),
            false,
            None,
            ExecutorSettings::default(),
            warm_catalog,
            None,
        )
        .await
        .expect("measure_corpus runs");
        assert!(failed.is_empty(), "no statement fails: {failed:?}");
        assert_eq!(measured.len(), statements, "every statement is measured");
        measured
    }

    /// `--warm-catalog` reuses one executor across statements, so the catalog
    /// resolve of every statement after the first is served from the warm
    /// in-process `RecordCache`/`HeadCache` instead of re-listing and re-GETting
    /// the commit records. The resolve GETs are a component of the pooled
    /// `object_store_get_requests` the executor surfaces, so with the flag on
    /// they stop recurring: later statements cost strictly fewer GETs than the
    /// first and plateau. With the flag off (a fresh cold executor per
    /// statement, the default) every statement re-pays the same resolve GETs,
    /// so the per-statement GET count is flat and higher. This is the divergence
    /// from server behaviour issue #857 names (a server holds one process-level
    /// catalog for a tenant's whole query stream).
    ///
    /// Reverting `measure_corpus` to build a cold executor per statement under
    /// `warm_catalog` (dropping the `shared_executor` branch) makes the warm
    /// arm behave like the cold one, so the `warm[later] < warm[0]` assertion
    /// flips red.
    #[tokio::test]
    async fn warm_catalog_reuses_resolve_across_statements() {
        let store = empty_store();
        let tenant = TenantId::new("warm-catalog-tenant");
        // Several small objects, each its own commit record, so the resolve's
        // per-commit-record GETs are a visible share of every statement's cost
        // and the warm/cold difference is unambiguous.
        write_shard_objects(&store, &tenant, 0, 8).await;
        let th = tenant.hash();
        const STATEMENTS: usize = 4;

        let cold = measure_same_statement(&store, th, STATEMENTS, false).await;
        let warm = measure_same_statement(&store, th, STATEMENTS, true).await;

        let cold_g = cold_gets(&cold);
        let warm_g = cold_gets(&warm);

        // Cold: a fresh executor per statement re-pays the same GETs every time.
        for i in 1..STATEMENTS {
            assert_eq!(
                cold_g[i], cold_g[0],
                "cold executor re-pays the same GETs on every statement: {cold_g:?}"
            );
        }
        // Warm: the first statement warms the catalog; every statement after it
        // pays strictly fewer GETs (the resolve component no longer recurs) and
        // the count plateaus.
        assert!(
            warm_g[1] < warm_g[0],
            "the warm catalog serves the resolve of later statements: {warm_g:?}"
        );
        for i in 2..STATEMENTS {
            assert_eq!(
                warm_g[i], warm_g[1],
                "warm per-statement GETs plateau once the catalog is warm: {warm_g:?}"
            );
        }
        // The whole point of the flag: a later warm statement costs fewer GETs
        // than the same statement on the cold path.
        assert!(
            warm_g[STATEMENTS - 1] < cold_g[STATEMENTS - 1],
            "a warm later statement costs fewer GETs than the cold path's: warm {warm_g:?} \
             vs cold {cold_g:?}"
        );
    }

    /// Provenance records which regime the run measured, so a report states
    /// whether its resolve-phase figures are server-like (warm) or
    /// cold-per-statement.
    #[tokio::test]
    async fn warm_catalog_is_recorded_in_provenance() {
        let store = empty_store();
        provisioned_tenant(&store, "warm-prov-tenant").await;

        let mut cfg = tenant_cfg(&store, "warm-prov-tenant", None, 0);
        cfg.entries = vec![entry("q", "SELECT body FROM logs")];
        cfg.warm_catalog = true;
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(
            report.provenance.warm_catalog,
            Some(true),
            "warm-catalog on is recorded on the in-process tenant lane"
        );

        cfg.warm_catalog = false;
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(
            report.provenance.warm_catalog,
            Some(false),
            "warm-catalog off is recorded on the in-process tenant lane"
        );
    }

    /// Item 2 (issue #857 review): the Flight lane builds no in-process
    /// executor, so `--warm-catalog` governs nothing there. A `--flight
    /// --warm-catalog` run must therefore not report `warm_catalog: true`; it
    /// records the regime as not-applicable, exactly as the effective-ceiling
    /// fields do. The Flight lane cannot run without a live server, so this
    /// tests the recording rule and its serialisation directly.
    ///
    /// Prove-the-test: reverting `recorded_warm_catalog` to `Some(warm_catalog)`
    /// unconditionally makes the flight case serialise `true`, failing both the
    /// `None` assertion and the "not true" serialisation assertion below.
    #[test]
    fn flight_lane_never_records_warm_catalog_as_true() {
        // The recording rule: in-process lanes carry the flag; the Flight lane
        // is not-applicable regardless of the flag.
        assert_eq!(recorded_warm_catalog(true, true), None);
        assert_eq!(recorded_warm_catalog(true, false), None);
        assert_eq!(recorded_warm_catalog(false, true), Some(true));
        assert_eq!(recorded_warm_catalog(false, false), Some(false));

        // The serialised report of a `--flight --warm-catalog` run must not
        // claim `true`.
        let flight_warm = serde_json::json!({
            "warm_catalog": recorded_warm_catalog(true, true),
        });
        assert_ne!(
            flight_warm["warm_catalog"],
            serde_json::json!(true),
            "a Flight run must not serialise warm_catalog: true"
        );
        assert_eq!(
            flight_warm["warm_catalog"],
            serde_json::Value::Null,
            "a Flight run serialises warm_catalog as not-applicable (null)"
        );
    }

    /// The `--cache-bytes` cache is not inert: attaching it to the log fetcher
    /// cuts object-store GETs and adds fetch-cache hits.
    ///
    /// Reachability (issue #677): the bench's SQL scan reaches the cache through
    /// `LogSegmentFetcher::tenant_bytes` (via `plan_segment` +
    /// `scan_accounted_with_tenant_subset` in `ravel_sql::logs_scan`), NOT the
    /// `fetch_accounted_with_tenant` funnel the flag's ADR names. That path reads
    /// each segment more than once per query (once to plan the surviving blocks,
    /// once to scan them), so with the fetcher cache attached the scan is served
    /// from the plan's fill within a single run. The catalog's own byte cache is
    /// on under `CatalogConfig::default()` in both arms, so an isolated,
    /// attributable comparison holds the catalog state identical (two FRESH
    /// executors, one cold run each) and reads off the delta the fetcher cache
    /// alone makes: strictly fewer store GETs, strictly more cache hits. This is
    /// why the literal "warm hits == cold misses" identity does not hold and is
    /// not asserted -- the cold run already hits, and the catalog cache
    /// contributes hits of its own. Building the cached arm without `with_cache`
    /// collapses both deltas to zero, which is what flips the assertions red.
    ///
    /// The `has_word` predicate is what keeps the query on that plan-then-scan
    /// path (issue #739). A predicate-free full-window statement now takes
    /// `LogsScanExec`'s whole-segment fast path, which reads each segment exactly
    /// once and so leaves the fetcher cache nothing to absorb within a run -- the
    /// double read this test is about is precisely what that path eliminates. The
    /// fixture's single record body is `request 0 timeout after 30s`, so the
    /// predicate matches it and the returned rows are the same as without it.
    #[tokio::test]
    async fn cache_flag_cuts_store_gets_within_a_run() {
        use ravel_types::accounting::AccountedOp;

        let store = empty_store();
        let tenant = TenantId::new("cache-tenant");
        let th = tenant.hash();
        write_shard_objects(&store, &tenant, 0, 1).await;
        let req = SqlRequest {
            sql: "SELECT body FROM logs WHERE has_word(body, 'timeout')".to_string(),
            window: TimeRange {
                start_ns: 0,
                end_ns: NOW_NS,
            },
            min_tokens: Vec::new(),
            now_ns: NOW_NS,
            deadline: Duration::from_secs(30),
        };

        // Fetcher cache ON: a fresh executor (cold catalog byte cache, cold
        // fetcher cache), one run.
        let cache = build_read_cache(64 << 20);
        let cached = cold_executor(&store, &[], Some(cache), ExecutorSettings::default())
            .expect("build cached executor")
            .executor;
        let on = cached.execute(th, &req).await.expect("cached run");

        // Fetcher cache OFF: a fresh executor (identical cold catalog byte cache,
        // no fetcher cache), one run. The catalog contribution is the same in
        // both, so any delta is the fetcher cache alone.
        let uncached = cold_executor(&store, &[], None, ExecutorSettings::default())
            .expect("build uncached executor")
            .executor;
        let off = uncached.execute(th, &req).await.expect("uncached run");

        let get_on = on.accounting.s3_requests(AccountedOp::Get);
        let get_off = off.accounting.s3_requests(AccountedOp::Get);
        assert!(
            get_on < get_off,
            "the fetcher cache must cut store GETs within a run; got get_on={get_on} \
             get_off={get_off}"
        );
        assert!(
            on.accounting.cache_hits > off.accounting.cache_hits,
            "the fetcher cache must add cache hits over the catalog-only baseline; got \
             on={} off={}",
            on.accounting.cache_hits,
            off.accounting.cache_hits
        );
    }

    /// The `--sql-max-query-bytes` value threaded through `GenerateConfig`/
    /// `TenantConfigInput` into `measure_corpus` must land on
    /// `SqlConfig::max_query_bytes`, not stop at a parsed field. A value distinct
    /// from the compiled-in default proves the override is wired end to end;
    /// reverting `cold_executor` to `SqlConfig::default()` makes this fail rather
    /// than pass on a coincidental default (its companion test covers the
    /// default itself).
    /// `--fetch-concurrency` must land on `EngineConfig::fetch_concurrency`
    /// (the logs scan's partition count and in-flight fetch bound), not stop at
    /// a parsed field; reverting `cold_executor` to `EngineConfig::default()`
    /// fails this on the distinct value.
    #[test]
    fn cold_executor_threads_fetch_concurrency_override() {
        let store = empty_store();
        let custom = DEFAULT_FETCH_CONCURRENCY * 4;
        assert_ne!(custom, DEFAULT_FETCH_CONCURRENCY);
        let executor = cold_executor(
            &store,
            &[],
            None,
            ExecutorSettings {
                fetch_concurrency: custom,
                ..ExecutorSettings::default()
            },
        )
        .expect("build executor")
        .executor;
        assert_eq!(executor.config().engine.fetch_concurrency, custom);
        let executor = cold_executor(
            &store,
            &[],
            None,
            ExecutorSettings {
                fetch_concurrency: 0,
                ..ExecutorSettings::default()
            },
        )
        .expect("build executor")
        .executor;
        assert_eq!(
            executor.config().engine.fetch_concurrency,
            1,
            "a zero is clamped to one partition, never a zero-partition plan"
        );
    }

    /// A one-object, cache-wired fixture measured twice. Accounting is recorded
    /// for BOTH runs, not only the cold one (issue #767): the array is length 2,
    /// the cold run pays the object-store traffic, and the warm run serves from
    /// cache instead of refetching -- the "does the second execution drop to
    /// plan reads only" answer the warm run exists to give.
    ///
    /// The exact figures the fixture produces (pinned below, never `> 0`): the
    /// single RLOG object plus the catalog resolve are 3 store GETs on the cold
    /// run (`l/HEAD` pointer, the commit record, the data object; the
    /// plan-then-scan second read of the data object is absorbed by the fetcher
    /// cache, 2 hits). On the warm run the data object and most of the resolve
    /// are cache-served, so store GETs fall to 1 and cache hits rise to 4, with
    /// no store bytes transferred. The `has_word` predicate keeps the query on
    /// the plan-then-scan path so the cache has a within-run second read to
    /// absorb (see `cache_flag_cuts_store_gets_within_a_run`).
    ///
    /// Red against the `run == 0` guard: moving the `per_run_accounting.push`
    /// back under `if run == 0` leaves the array length 1 with the warm entry
    /// absent, so the `acc.len() == 2` and every `acc[1]` assertion fail.
    #[tokio::test]
    async fn per_run_accounting_records_warm_run_not_only_cold() {
        let store = empty_store();
        let tenant = TenantId::new("perrun-tenant");
        write_shard_objects(&store, &tenant, 0, 1).await;
        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };
        let (measured, skipped, failed) = measure_corpus(
            &store,
            tenant.hash(),
            &[entry(
                "q",
                "SELECT body FROM logs WHERE has_word(body, 'timeout')",
            )],
            &[],
            2,
            window,
            NOW_NS,
            64 << 20,
            Duration::from_secs(30),
            false,
            None,
            ExecutorSettings::default(),
            false,
            None,
        )
        .await
        .expect("run");
        assert!(skipped.is_empty() && failed.is_empty());
        let acc = measured[0]
            .per_run_accounting
            .as_ref()
            .expect("an in-process lane records per-run accounting");
        assert_eq!(
            acc.len(),
            2,
            "one accounting entry per run; the run==0 guard leaves length 1"
        );

        // Cold run: the one object plus the catalog resolve.
        assert_eq!(
            acc[0].object_store_get_requests, 3,
            "cold run's store GETs: l/HEAD + commit record + the data object"
        );
        assert_eq!(acc[0].cache_hits, 2, "cold run's fetcher-cache hits");
        assert_eq!(acc[0].cache_misses, 3, "cold run's fetcher-cache misses");
        assert_eq!(acc[0].object_store_bytes, 836, "cold run's store bytes");

        // Warm run: served from cache, so it drops to plan reads only.
        assert_eq!(
            acc[1].object_store_get_requests, 1,
            "warm run's store GETs fall from 3 to 1 (the rest cache-served)"
        );
        assert_eq!(
            acc[1].cache_hits, 4,
            "warm run's cache hits rise from 2 to 4"
        );
        assert_eq!(
            acc[1].object_store_bytes, 0,
            "warm run transfers no store bytes"
        );

        // The cold figures still live in `scan`, equal to index 0, so a
        // report reader that only knows `scan` is unaffected.
        let scan = measured[0].scan.as_ref().expect("cold scan diagnostics");
        assert_eq!(
            scan.object_store_get_requests,
            acc[0].object_store_get_requests
        );
        assert_eq!(scan.cache_hits, acc[0].cache_hits);
    }

    // ---- Probe misses in the per-run accounting (#883) ---------------------

    /// The suffix-probe length that just fails to reach SKIP_IDX in `object`: it
    /// starts exactly at SKIP_IDX's end, so it covers the footer (no footer
    /// chase) and every tail section the writer emits after SKIP_IDX (PAGE_DIR
    /// on a version-4 object), leaving SKIP_IDX -- the one section a read has to
    /// locate blocks through -- as the only thing outside the window. Stands in
    /// for an under-sized probe derivation on an object whose trailer exceeds it.
    fn suffix_just_past_skip_idx(object: &[u8]) -> u64 {
        let total = object.len() as u64;
        let footer = ravel_logseg::footer::open(object).expect("footer");
        let skip = *footer
            .section(ravel_logseg::footer::kind::SKIP_IDX)
            .expect("SKIP_IDX");
        let suffix = total - (skip.offset + skip.len);
        assert!(
            suffix < total - skip.offset,
            "the probe must not reach SKIP_IDX's start"
        );
        suffix
    }

    /// The `has_word` statement the probe-miss tests measure. It is deliberately
    /// not skip-decidable: `plan_segment` falls back to a whole-object plan read
    /// and hands no footer forward, so the scan probes the object a second time
    /// on its own. That is what makes both phases counted, which is the split
    /// the two `probe_misses_*` fields exist to show.
    const PROBE_MISS_SQL: &str = "SELECT body FROM logs WHERE has_word(body, 'timeout')";

    /// A trailer that EXCEEDS the probe is reported in the per-run accounting,
    /// per phase, on every run.
    ///
    /// The fixture is one RLOG object read through the ranged path
    /// (`logs_block_range_threshold: 0`, the whole-object crossover disabled --
    /// at the default threshold this object is read whole in one GET and never
    /// probes at all, so none of the miss-counting sites is reached and the test
    /// would be vacuous). The probe is pinned to start at SKIP_IDX's end, so
    /// each read that has to locate blocks through SKIP_IDX misses exactly one
    /// tail section: one in the plan phase (`plan_segment`'s whole-object
    /// fallback) and one in the scan phase (the data read, which re-probes
    /// because the fallback carried no footer).
    ///
    /// Both runs report the same figures even though the warm run's store GETs
    /// fall: a miss is measured against the probe WINDOW, not against what the
    /// read cache happened to hold.
    ///
    /// Prove-the-test: pinned exact values, never `> 0`. Demonstrated failing
    /// during development by deleting the
    /// `self.probe_misses.record(phase, stats.probe_misses)` line from
    /// `LogSegmentFetcher::record_probe_misses` in `ravel-query` (reads 0
    /// against the expected 1), and by deleting `stats.probe_misses += 1` from
    /// `fetch_object_v4`'s tail-section loop, the counting site this version-4
    /// fixture actually reaches (same result, from the other end of the
    /// plumbing). Deleting the version-3 site in `fetch_object_with_footer`
    /// instead leaves it green, which is what says the fixture is version 4.
    #[tokio::test]
    async fn probe_misses_reach_per_run_accounting_when_the_trailer_exceeds_the_probe() {
        let store = empty_store();
        let tenant = TenantId::new("probe-miss-tenant");
        let objects = write_shard_objects(&store, &tenant, 0, 1).await;
        let suffix = suffix_just_past_skip_idx(&objects[0]);
        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };
        let (measured, skipped, failed) = measure_corpus(
            &store,
            tenant.hash(),
            &[entry("q", PROBE_MISS_SQL)],
            &[],
            2,
            window,
            NOW_NS,
            64 << 20,
            Duration::from_secs(30),
            false,
            None,
            ExecutorSettings {
                logs_block_range_threshold: 0,
                logs_suffix_len: Some(suffix),
                ..ExecutorSettings::default()
            },
            false,
            None,
        )
        .await
        .expect("run");
        assert!(skipped.is_empty() && failed.is_empty());
        let acc = measured[0]
            .per_run_accounting
            .as_ref()
            .expect("an in-process lane records per-run accounting");
        assert_eq!(acc.len(), 2, "one accounting entry per run");

        assert_eq!(
            acc[0].probe_misses_plan, 1,
            "cold run: the plan-phase read missed SKIP_IDX exactly once"
        );
        assert_eq!(
            acc[0].probe_misses_scan, 1,
            "cold run: the scan-phase read missed SKIP_IDX exactly once"
        );
        assert_eq!(
            acc[1].probe_misses_plan, 1,
            "warm run: the same window misses the same section"
        );
        assert_eq!(
            acc[1].probe_misses_scan, 1,
            "warm run: a miss is a property of the probe window, not of the cache"
        );

        // What a measurement pass actually reads: the figure has to be in the
        // report JSON, per statement per run, not only in the in-memory struct.
        let v: serde_json::Value = serde_json::to_value(&measured[0]).expect("serialize entry");
        let per_run = v["per_run_accounting"]
            .as_array()
            .expect("per_run_accounting is a JSON array");
        assert_eq!(per_run[0]["probe_misses_plan"], 1);
        assert_eq!(per_run[0]["probe_misses_scan"], 1);
        assert_eq!(per_run[1]["probe_misses_plan"], 1);
        assert_eq!(per_run[1]["probe_misses_scan"], 1);
    }

    /// A trailer that FITS inside the derived probe reports exactly zero, on
    /// both phases and both runs.
    ///
    /// Same fixture and same ranged path as the exceeding case, with the probe
    /// left at the per-object derivation: this object is far below
    /// `LOG_SUFFIX_FLOOR_BYTES`, so the derived window is the whole object and
    /// covers every tail section. Paired with the exceeding case so a plumbing
    /// change that reported a constant zero could not pass both.
    ///
    /// Prove-the-test: pinned to 0, never `>= 0`. Demonstrated failing during
    /// development by making `probe_window_covers` in `ravel-query` return
    /// `false` unconditionally: every counting read then reports its tail
    /// sections as missed and the first assertion reads 2 (SKIP_IDX and
    /// PAGE_DIR) against the expected 0.
    #[tokio::test]
    async fn probe_misses_are_zero_when_the_derived_probe_covers_the_trailer() {
        let store = empty_store();
        let tenant = TenantId::new("probe-fit-tenant");
        let objects = write_shard_objects(&store, &tenant, 0, 1).await;
        let total = objects[0].len() as u64;
        assert!(
            ravel_query::derive_suffix_len(total) >= total,
            "fixture precondition: the derived probe ({} B) must cover the whole \
             object ({total} B), or this test would be measuring a miss",
            ravel_query::derive_suffix_len(total)
        );
        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };
        let (measured, skipped, failed) = measure_corpus(
            &store,
            tenant.hash(),
            &[entry("q", PROBE_MISS_SQL)],
            &[],
            2,
            window,
            NOW_NS,
            64 << 20,
            Duration::from_secs(30),
            false,
            None,
            ExecutorSettings {
                logs_block_range_threshold: 0,
                logs_suffix_len: None,
                ..ExecutorSettings::default()
            },
            false,
            None,
        )
        .await
        .expect("run");
        assert!(skipped.is_empty() && failed.is_empty());
        let acc = measured[0]
            .per_run_accounting
            .as_ref()
            .expect("an in-process lane records per-run accounting");
        assert_eq!(acc.len(), 2, "one accounting entry per run");
        for (run, entry) in acc.iter().enumerate() {
            assert_eq!(
                entry.probe_misses_plan, 0,
                "run {run}: the derived probe covered every plan-phase tail section"
            );
            assert_eq!(
                entry.probe_misses_scan, 0,
                "run {run}: the derived probe covered every scan-phase tail section"
            );
        }
    }

    // ---- The `--logs-suffix-len` seam (#883) -------------------------------

    /// A `logs_suffix_len` set on the loaded-tenant config reaches the fetcher.
    /// Asserted at the point of reliance, not the parse site: the exact pinned
    /// window flows through the config-to-settings seam
    /// ([`tenant_executor_settings`]) and, at the fetcher, misses exactly the
    /// SKIP_IDX section it cannot cover -- once in the plan phase and once in the
    /// scan phase, the same fixture and reasoning as
    /// [`probe_misses_reach_per_run_accounting_when_the_trailer_exceeds_the_probe`].
    ///
    /// The whole-object crossover is dropped for this tiny fixture
    /// (`logs_block_range_threshold: 0`) only so the ranged probe path runs at
    /// all; the value under test is `logs_suffix_len`, which the settings step
    /// carries verbatim (first assertion) and the fetcher then applies (the
    /// probe-miss assertions).
    ///
    /// Prove-the-test: pinned exact values, never `> 0`. Demonstrated failing
    /// against the pre-change code by removing `logs_suffix_len: cfg.logs_suffix_len`
    /// from [`tenant_executor_settings`] (the field then falls back to
    /// `ExecutorSettings::default()`'s `None`): the settings assertion reads
    /// `None` against `Some(suffix)`, and the derived probe covers the whole
    /// tiny object so both probe-miss assertions read 0 against the expected 1.
    #[tokio::test]
    async fn tenant_config_logs_suffix_len_reaches_the_fetcher() {
        let store = empty_store();
        let tenant = TenantId::new("suffix-flag-tenant");
        let objects = write_shard_objects(&store, &tenant, 0, 1).await;
        let suffix = suffix_just_past_skip_idx(&objects[0]);

        let mut cfg = tenant_cfg(&store, "suffix-flag-tenant", None, 0);
        cfg.logs_suffix_len = Some(suffix);
        // The seam the loaded-tenant lane builds carries the pinned window
        // verbatim; run_tenant feeds this same value to measure_corpus.
        let settings = tenant_executor_settings(&cfg, 1);
        assert_eq!(
            settings.logs_suffix_len,
            Some(suffix),
            "the config's pinned window must survive into the executor settings"
        );

        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };
        let (measured, skipped, failed) = measure_corpus(
            &store,
            tenant.hash(),
            &[entry("q", PROBE_MISS_SQL)],
            &[],
            1,
            window,
            NOW_NS,
            64 << 20,
            Duration::from_secs(30),
            false,
            None,
            ExecutorSettings {
                logs_block_range_threshold: 0,
                ..settings
            },
            false,
            None,
        )
        .await
        .expect("run");
        assert!(skipped.is_empty() && failed.is_empty());
        let acc = measured[0]
            .per_run_accounting
            .as_ref()
            .expect("an in-process lane records per-run accounting");
        assert_eq!(
            acc[0].probe_misses_plan, 1,
            "the pinned window misses SKIP_IDX in the plan phase exactly once"
        );
        assert_eq!(
            acc[0].probe_misses_scan, 1,
            "the pinned window misses SKIP_IDX in the scan phase exactly once"
        );
    }

    /// An unset `logs_suffix_len` leaves the per-object derivation in place, so
    /// the run behaves exactly as it does today: the settings carry `None`, and
    /// the derived probe covers this tiny object's whole trailer, reporting zero
    /// misses on both phases.
    ///
    /// Prove-the-test: pinned to 0 and to `None`, never `>= 0`. Changing
    /// [`tenant_executor_settings`] to hardcode `logs_suffix_len: Some(1)` makes
    /// the settings assertion read `Some(1)` against `None`, and that 1-byte
    /// probe misses every tail section, so the probe-miss assertions read nonzero
    /// against the expected 0.
    #[tokio::test]
    async fn tenant_config_unset_logs_suffix_len_keeps_the_derivation() {
        let store = empty_store();
        let tenant = TenantId::new("suffix-unset-tenant");
        let objects = write_shard_objects(&store, &tenant, 0, 1).await;
        let total = objects[0].len() as u64;
        assert!(
            ravel_query::derive_suffix_len(total) >= total,
            "fixture precondition: the derived probe ({} B) must cover the whole \
             object ({total} B), or an unset run would already show a miss",
            ravel_query::derive_suffix_len(total)
        );

        let cfg = tenant_cfg(&store, "suffix-unset-tenant", None, 0);
        assert_eq!(
            cfg.logs_suffix_len, None,
            "the default config leaves the probe length unset"
        );
        let settings = tenant_executor_settings(&cfg, 1);
        assert_eq!(
            settings.logs_suffix_len, None,
            "an unset config leaves the derivation in place"
        );

        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };
        let (measured, skipped, failed) = measure_corpus(
            &store,
            tenant.hash(),
            &[entry("q", PROBE_MISS_SQL)],
            &[],
            1,
            window,
            NOW_NS,
            64 << 20,
            Duration::from_secs(30),
            false,
            None,
            ExecutorSettings {
                logs_block_range_threshold: 0,
                ..settings
            },
            false,
            None,
        )
        .await
        .expect("run");
        assert!(skipped.is_empty() && failed.is_empty());
        let acc = measured[0]
            .per_run_accounting
            .as_ref()
            .expect("an in-process lane records per-run accounting");
        assert_eq!(
            acc[0].probe_misses_plan, 0,
            "the derived probe covers every plan-phase tail section, as today"
        );
        assert_eq!(
            acc[0].probe_misses_scan, 0,
            "the derived probe covers every scan-phase tail section, as today"
        );
    }

    /// The provenance records the exact pinned window when the flag is set and
    /// `None` when it is unset. The fixture tenant's objects sit below the
    /// whole-object crossover, so a pinned window changes nothing the fetcher
    /// does here; this test pins the recording, not the effect (which the two
    /// tests above pin).
    ///
    /// Prove-the-test: pinned to `Some(4096)` and `None`, never merely present.
    /// Replacing the tenant lane's `logs_suffix_len: match &cfg.flight { ... }`
    /// provenance line with a hardcoded `None` makes the set arm read `None`
    /// against `Some(4096)`.
    #[tokio::test]
    async fn provenance_records_logs_suffix_len() {
        let store = empty_store();
        provisioned_tenant(&store, "suffix-prov-tenant").await;

        let mut cfg = tenant_cfg(&store, "suffix-prov-tenant", None, 0);
        cfg.entries = vec![entry("scan", "SELECT body FROM logs")];
        cfg.logs_suffix_len = Some(4096);
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(
            report.provenance.logs_suffix_len,
            Some(4096),
            "a set --logs-suffix-len is recorded verbatim in provenance"
        );
        let json = serde_json::to_value(&report.provenance).expect("provenance serializes");
        assert_eq!(json["logs_suffix_len"], 4096);

        let mut cfg = tenant_cfg(&store, "suffix-prov-tenant", None, 0);
        cfg.entries = vec![entry("scan", "SELECT body FROM logs")];
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(
            report.provenance.logs_suffix_len, None,
            "an unset flag records null (the per-object derivation governs)"
        );
        let json = serde_json::to_value(&report.provenance).expect("provenance serializes");
        assert!(
            json["logs_suffix_len"].is_null(),
            "an unset flag serializes as null, not a figure: {json}"
        );
    }

    /// The report JSON keeps its existing shape and gains the per-run array.
    /// A cold accounting field under `scan` is unchanged (pinned), and
    /// `per_run_accounting` is an array whose length equals `runs`.
    #[tokio::test]
    async fn report_json_keeps_cold_fields_and_adds_per_run_array() {
        let store = empty_store();
        let tenant = TenantId::new("json-tenant");
        write_shard_objects(&store, &tenant, 0, 1).await;
        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };
        let (measured, _skipped, _failed) = measure_corpus(
            &store,
            tenant.hash(),
            &[entry(
                "q",
                "SELECT body FROM logs WHERE has_word(body, 'timeout')",
            )],
            &[],
            2,
            window,
            NOW_NS,
            64 << 20,
            Duration::from_secs(30),
            false,
            None,
            ExecutorSettings::default(),
            false,
            None,
        )
        .await
        .expect("run");

        let v: serde_json::Value = serde_json::to_value(&measured[0]).expect("serialize entry");
        // The existing cold field under `scan` is unchanged.
        assert_eq!(
            v["scan"]["object_store_get_requests"], 3,
            "the cold run's store GETs are still carried under `scan`"
        );
        // The new array is present and one entry long per run.
        let per_run = v["per_run_accounting"]
            .as_array()
            .expect("per_run_accounting is a JSON array");
        assert_eq!(per_run.len(), 2, "per_run_accounting has one entry per run");
        assert_eq!(per_run[0]["object_store_get_requests"], 3);
        assert_eq!(per_run[1]["object_store_get_requests"], 1);
    }

    #[test]
    fn cold_executor_threads_max_query_bytes_override() {
        let store = empty_store();
        let custom = DEFAULT_MAX_QUERY_BYTES * 4;
        assert_ne!(custom, DEFAULT_MAX_QUERY_BYTES);
        let executor = cold_executor(
            &store,
            &[],
            None,
            ExecutorSettings {
                max_query_bytes: custom,
                ..ExecutorSettings::default()
            },
        )
        .expect("build executor")
        .executor;
        assert_eq!(executor.config().max_query_bytes, custom);
    }

    /// Passing the default (an omitted flag) leaves the measured budget
    /// byte-for-byte unchanged. Paired with the override test so a regression
    /// that silently drops the override cannot pass by leaving the default
    /// coincidentally correct.
    #[test]
    fn cold_executor_defaults_to_compiled_in_budget() {
        let store = empty_store();
        let executor = cold_executor(&store, &[], None, ExecutorSettings::default())
            .expect("build executor")
            .executor;
        assert_eq!(executor.config().max_query_bytes, DEFAULT_MAX_QUERY_BYTES);
    }

    /// The per-query pool ceiling is recorded in the run's provenance block
    /// (issue #615): a run with a raised `--sql-max-query-bytes` records exactly
    /// that value, and an unset flag records the compiled-in default. The
    /// in-process lane applies what it asks for, so `effective` is `Some` of the
    /// same figure. Before the field existed the block carried no per-query
    /// budget at all, so this assertion has nothing to read and fails to compile
    /// against the old provenance; with the field present but unpopulated it
    /// would read the serde default and the raised-value arm goes red.
    #[tokio::test]
    async fn provenance_records_effective_max_query_bytes() {
        let store = empty_store();
        provisioned_tenant(&store, "maxquerybytes-tenant").await;

        let custom = DEFAULT_MAX_QUERY_BYTES * 4;
        assert_ne!(custom, DEFAULT_MAX_QUERY_BYTES);
        let mut cfg = tenant_cfg(&store, "maxquerybytes-tenant", None, 0);
        cfg.entries = vec![entry("scan", "SELECT body FROM logs")];
        cfg.max_query_bytes = custom;
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(
            report.provenance.sql_max_query_bytes_requested, custom,
            "the raised per-query pool ceiling is recorded in provenance"
        );
        assert_eq!(
            report.provenance.sql_max_query_bytes_effective,
            Some(custom),
            "the in-process lane applies the requested ceiling, so it is effective"
        );

        // Unset flag (the config default): the block records the compiled-in
        // budget byte-for-byte.
        let cfg = tenant_cfg(&store, "maxquerybytes-tenant", None, 0);
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(
            report.provenance.sql_max_query_bytes_requested, DEFAULT_MAX_QUERY_BYTES,
            "an unset flag records the compiled-in per-query budget"
        );
        assert_eq!(
            report.provenance.sql_max_query_bytes_effective,
            Some(DEFAULT_MAX_QUERY_BYTES),
            "an unset flag is still effective on the in-process lane"
        );
    }

    /// The recorded ceiling must be the one that GOVERNED, not the one that was
    /// asked for. Echoing `cfg.max_query_bytes` into provenance passes an
    /// equality check even when the value never reaches the executor, so this
    /// pins the wiring instead: a ceiling small enough to refuse the statement
    /// must actually refuse it. Replace `max_query_bytes: cfg.max_query_bytes`
    /// in the `ExecutorSettings` built by `run_tenant` with the compiled-in
    /// default and this goes red while the provenance assertions above stay
    /// green, which is exactly the gap it exists to close.
    #[tokio::test]
    async fn a_tiny_max_query_bytes_actually_refuses_the_statement() {
        let store = empty_store();
        provisioned_tenant(&store, "maxquerybytes-wired").await;

        let mut cfg = tenant_cfg(&store, "maxquerybytes-wired", None, 0);
        cfg.entries = vec![entry(
            "agg",
            "SELECT body, count(*) FROM logs GROUP BY body",
        )];
        cfg.continue_on_error = true;
        cfg.max_query_bytes = 1024;
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(
            report.provenance.sql_max_query_bytes_effective,
            Some(1024),
            "the tiny ceiling is recorded as effective on the in-process lane"
        );
        assert!(
            !report.failed.is_empty(),
            "a 1 KiB per-query pool must refuse the statement; if the ceiling never \
             reached the executor the statement succeeds and `failed` is empty. \
             failed={:?} entries={}",
            report.failed,
            report.entries.len()
        );
    }

    /// A 1-shard tenant with `objects` hour-0 RLOG objects, a provisioning
    /// record, and a real compaction over that bucket (issue #834): unlike
    /// [`folded_tenant`], which only seals the hour for `max_segments`
    /// accounting, this drives `ravel_maintain::compact_bucket` so the
    /// resolved snapshot's segments are genuinely `SegmentLevel::L1`, the
    /// ground truth `dataset_info`'s derived `layout` reads. `objects` must be
    /// at least `DEFAULT_MIN_COMPACTION_INPUTS` (2) or the bucket has too few
    /// inputs to compact.
    async fn compacted_tenant(store: &Arc<dyn ObjectStoreBackend>, name: &str, objects: usize) {
        let tenant = TenantId::new(name);
        let tenant_hash = tenant.hash();
        write_shard_objects(store, &tenant, 0, objects).await;
        validate_or_adopt(
            store.as_ref(),
            &tenant_hash,
            Signal::Logs,
            1,
            10,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("write provisioning record");

        let clock = ravel_maintain::FixedClock::new(NOW_NS);
        let config = ravel_maintain::CompactorConfig::default();
        let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Logs, 0, 0);
        let outcome = ravel_maintain::compact_bucket(store.as_ref(), &clock, &config, &bucket)
            .await
            .expect("compact hour-0 bucket");
        assert!(
            matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
            "fixture must actually compact, got {outcome:?}"
        );
    }

    /// `dataset_info` must report BOTH layout directions from the same
    /// derivation, not a hardcoded string: an all-L0 tenant is
    /// `"pre-compaction"` and a tenant with a real compaction over its bucket
    /// is `"post-compaction"` (issue #834). A one-directional fix -- for
    /// instance one that always returns `Compaction::Pre.label()` -- passes
    /// only the first of these two assertions.
    ///
    /// Prove-the-test: reverting `dataset_info`'s `layout` to echo a
    /// `Compaction` parameter fixed at `Compaction::Pre` (the pre-#834
    /// behavior) makes the `post` assertion below fail with
    /// `left: "post-compaction", right: "pre-compaction"` -- exactly the
    /// mislabel this ticket reports (a compacted tenant printed
    /// `layout=pre-compaction`).
    #[tokio::test]
    async fn dataset_info_layout_reports_both_directions_from_observed_state() {
        let window = TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        };

        let pre_store = empty_store();
        provisioned_tenant(&pre_store, "layout-pre-tenant").await;
        let pre = dataset_info(
            &pre_store,
            TenantId::new("layout-pre-tenant").hash(),
            window,
            NOW_NS,
            None,
            1,
        )
        .await
        .expect("resolve pre-compaction tenant");
        assert_eq!(pre.layout, "pre-compaction");

        let post_store = empty_store();
        compacted_tenant(&post_store, "layout-post-tenant", 2).await;
        let post = dataset_info(
            &post_store,
            TenantId::new("layout-post-tenant").hash(),
            window,
            NOW_NS,
            None,
            1,
        )
        .await
        .expect("resolve post-compaction tenant");
        assert_eq!(
            post.layout, "post-compaction",
            "a tenant with a real compaction over its bucket must report \
             post-compaction, derived from the resolved snapshot's L1 segments, \
             not echoed from an operator flag"
        );
    }

    /// `run_tenant`'s end-to-end report agrees with `dataset_info` on both
    /// directions, and JSON serializes the same value the struct holds (one
    /// code path, not two that could drift apart).
    #[tokio::test]
    async fn run_tenant_reports_observed_layout_not_the_compaction_flag() {
        let store = empty_store();
        compacted_tenant(&store, "run-tenant-post-layout", 2).await;
        let mut cfg = tenant_cfg(&store, "run-tenant-post-layout", None, 0);
        // The operator's belief is wrong (stale `--compaction pre`), but the
        // flag is unset here so no CompactionMismatch refusal fires; the
        // report must still say what the snapshot actually is.
        cfg.compaction = None;
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(report.dataset.layout, "post-compaction");
        let json = serde_json::to_value(&report.dataset).expect("DatasetInfo serializes");
        assert_eq!(json["layout"], "post-compaction");
    }

    /// An operator's stated `--compaction` belief is checked against the
    /// observed snapshot, in both mismatch directions, and refuses rather
    /// than letting a report print next to a silently wrong assertion (issue
    /// #834 deliverable 3). Agreement in either direction runs cleanly.
    #[tokio::test]
    async fn compaction_flag_mismatch_refuses_in_both_directions() {
        let post_store = empty_store();
        compacted_tenant(&post_store, "mismatch-post-tenant", 2).await;
        let mut cfg = tenant_cfg(&post_store, "mismatch-post-tenant", None, 0);
        cfg.compaction = Some(Compaction::Pre);
        let err = run_tenant(&cfg)
            .await
            .expect_err("asserting pre-compaction on a compacted tenant must refuse");
        assert!(
            err.to_string().contains("disagrees with"),
            "error must name the disagreement: {err}"
        );

        let pre_store = empty_store();
        provisioned_tenant(&pre_store, "mismatch-pre-tenant").await;
        let mut cfg = tenant_cfg(&pre_store, "mismatch-pre-tenant", None, 0);
        cfg.compaction = Some(Compaction::Post);
        let err = run_tenant(&cfg)
            .await
            .expect_err("asserting post-compaction on an uncompacted tenant must refuse");
        assert!(
            err.to_string().contains("disagrees with"),
            "error must name the disagreement: {err}"
        );

        // Agreement in either direction runs cleanly (this is a check, not a
        // ban on stating the flag at all).
        let agree_store = empty_store();
        compacted_tenant(&agree_store, "mismatch-agree-tenant", 2).await;
        let mut cfg = tenant_cfg(&agree_store, "mismatch-agree-tenant", None, 0);
        cfg.compaction = Some(Compaction::Post);
        run_tenant(&cfg)
            .await
            .expect("a --compaction flag that agrees with the observed layout must not refuse");
    }

    /// A run that performed no load must report `load_wall_ms` as `None`, not
    /// a measured-looking `0.0`, and JSON must OMIT the key entirely rather
    /// than serialize a `null` (issue #834 deliverable 2): a reader scanning
    /// for the key's presence, not its value, is how this is meant to be
    /// checked at the wire level.
    #[tokio::test]
    async fn tenant_lane_omits_load_wall_ms_generated_lane_reports_it() {
        let store = empty_store();
        provisioned_tenant(&store, "no-load-tenant").await;
        let cfg = tenant_cfg(&store, "no-load-tenant", None, 0);
        let report = run_tenant(&cfg).await.expect("tenant lane runs");
        assert_eq!(
            report.dataset.load_wall_ms, None,
            "the tenant lane performed no load in this invocation"
        );
        let json = serde_json::to_value(&report.dataset).expect("DatasetInfo serializes");
        assert!(
            json.get("load_wall_ms").is_none(),
            "load_wall_ms must be absent from JSON, not null, when no load ran: {json}"
        );

        let gen_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let gen_cfg = GenerateConfig {
            store: gen_store,
            store_backend: "memory".to_string(),
            region: "n/a".to_string(),
            endpoint: "n/a".to_string(),
            entries: Vec::new(),
            runs: 1,
            records: 4,
            records_per_object: 4,
            extra_attrs: 0,
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            cache_bytes: 0,
            deadline: Duration::from_secs(30),
            continue_on_error: false,
            fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
            progress_jsonl: None,
            tenant_max_bytes: DEFAULT_TENANT_MAX_BYTES,
            parallel_final_aggregation: false,
            max_segments: DEFAULT_MAX_SEGMENTS,
            explain_dir: None,
            warm_catalog: false,
            logs_suffix_len: None,
        };
        let gen_report = run_generated(&gen_cfg).await.expect("generated lane runs");
        let load_ms = gen_report
            .dataset
            .load_wall_ms
            .expect("the generated lane builds and times its own load");
        assert!(load_ms >= 0.0, "load wall time must be a real duration");
        let json = serde_json::to_value(&gen_report.dataset).expect("DatasetInfo serializes");
        assert!(
            json.get("load_wall_ms").is_some(),
            "load_wall_ms must be present in JSON when the lane actually loaded: {json}"
        );
    }

    /// T4 (#913): each fetch-amplification figure appears EXACTLY ONCE per phase
    /// in the report structure -- one plan-phase WIRE-byte field, one scan-phase
    /// WIRE-byte field, one decoded-page STORED-byte field, and one ratio -- with
    /// no duplication that could double-count a transfer, and each round-trips
    /// through serde with its value intact.
    #[test]
    fn fetch_amplification_figures_are_each_present_once() {
        let run = RunAccounting {
            object_store_get_requests: 3,
            object_store_list_requests: 0,
            object_store_bytes: 1080,
            cache_hits: 0,
            cache_misses: 3,
            cache_bytes: 0,
            probe_misses_plan: 0,
            probe_misses_scan: 0,
            plan_wire_bytes: 1008,
            scan_blocks_wire_bytes: 72,
            projected_page_decoded_bytes: 72,
            fetch_amplification: 1.0,
        };
        let json = serde_json::to_string(&run).expect("RunAccounting serializes");
        for key in [
            "plan_wire_bytes",
            "scan_blocks_wire_bytes",
            "projected_page_decoded_bytes",
            "fetch_amplification",
        ] {
            assert_eq!(
                json.matches(&format!("\"{key}\"")).count(),
                1,
                "`{key}` must appear exactly once in the report structure: {json}"
            );
        }
        let back: RunAccounting = serde_json::from_str(&json).expect("round-trips");
        assert_eq!(back.plan_wire_bytes, 1008);
        assert_eq!(back.scan_blocks_wire_bytes, 72);
        assert_eq!(back.projected_page_decoded_bytes, 72);
        assert_eq!(back.fetch_amplification, 1.0);

        // An older report that predates these fields still deserializes: the new
        // fields carry `#[serde(default)]`, exactly as `probe_misses_*` do.
        let legacy = serde_json::json!({
            "object_store_get_requests": 3u64,
            "object_store_list_requests": 0u64,
            "object_store_bytes": 1080u64,
            "cache_hits": 0u64,
            "cache_misses": 3u64,
            "cache_bytes": 0u64,
        });
        let old: RunAccounting =
            serde_json::from_value(legacy).expect("legacy report without the new fields");
        assert_eq!(old.scan_blocks_wire_bytes, 0);
        assert_eq!(old.fetch_amplification, 0.0);
    }
}
