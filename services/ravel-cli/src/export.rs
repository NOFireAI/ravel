//! `ravel-cli export`: bulk read-out of a tenant's stored records into a
//! Parquet file, the inverse of `ravel-cli load` (ADR-1751 decision 4).
//!
//! `--signal logs`, `--signal metrics` and `--signal spans` are implemented.
//!
//! # What makes this a store read rather than a query
//!
//! The export resolves the catalog once, at a single snapshot, and reads the
//! RLOG (logs), RSEG (metrics) or RSPAN (spans) objects that snapshot names.
//! It plans no SQL and contacts no
//! `ravel-server`; it does read object storage directly, one LIST wave per
//! resolve and a GET per surviving segment, so against S3 it is as remote as
//! any other read. What it shares with a query is the visibility layer: the
//! decoders and the segment fetcher are the same ones the query path uses, so
//! a record excluded from a query is excluded here too. Concretely, the two
//! exclusion mechanisms both apply:
//!
//! - Retention tombstones and superseded (compacted-away) objects never enter
//!   the snapshot, because `Catalog::resolve` applies them.
//! - Pending selective-erasure requests (ADR-0064) are attached to the
//!   snapshot and applied in two places, exactly as the SQL log scan applies
//!   them: handed to the fetcher as
//!   [`ravel_query::erasure::ErasurePredicate`]s, which matches per-record
//!   attributes, and then run over the decoded records by
//!   [`ravel_query::erasure::retain_unerased_log_records`], the same function
//!   the SQL scan's exclusion calls. That second pass matches the merged
//!   resource + scope + record attributes, so a subject named only in a
//!   resource or scope attribute is excluded too. A metrics export runs the
//!   same predicates over the fetched series with
//!   [`ravel_query::erasure::retain_series_soa`] and
//!   [`ravel_query::erasure::retain_histogram_series`], the functions the
//!   query engine calls on its own fetch. A spans export fetches through
//!   [`ravel_query::SpanSegmentFetcher`] and drops every span
//!   [`ravel_query::erasure::is_erased_span`] matches on its merged attributes
//!   and start time, the call the SQL spans scan makes on each fetched row.
//!
//! # Memory and mid-export store changes
//!
//! Every decoded record in the window is held in memory at once, because the
//! output is sorted by event time before the first row is written. Peak memory
//! is therefore proportional to the window, not to the output batch size;
//! export a wide range in several narrower windows rather than in one call.
//!
//! The snapshot is resolved once, so a compaction or a flush landing while the
//! export runs does not change what it writes. Garbage collection is the one
//! mid-export change that is not invisible: a GC pass that deletes an object
//! this snapshot already named makes its GET fail with not-found, and the
//! export fails with that error rather than retrying or skipping the object.
//!
//! # Window semantics
//!
//! `--start`/`--end` are a half-open event-time window `[start, end)`: a
//! record at exactly `--end` is not exported. For logs, `LogQuery`'s own
//! range is inclusive on both ends, so the fetch asks for `[start, end - 1]`
//! ([`fetch_range_end_ns`]), and the half-open bound is applied a second time
//! over the decoded rows ([`in_export_window`]) so the two spellings of the
//! same bound cannot drift apart unnoticed. For metrics, the segment fetch
//! takes no range, so [`in_export_window`] over the decoded samples is the
//! only place the window is applied. A span's event time is its start: the
//! span fetch returns every span whose `[start, end]` interval overlaps the
//! window, and [`in_export_window`] over each span's start keeps exactly the
//! ones that start inside it.
//!
//! # Round-tripping through `ravel-cli load`
//!
//! The output columns are exactly the ones the `--mapping` TOML names, in the
//! Arrow types that mapping's reader accepts, so `ravel-cli load --parquet
//! <exported> --mapping <same file>` reads the file back. The one lossy axis
//! is `ts_unit`: the stored event time is nanoseconds and the `ts` column is
//! written in the mapping's unit, so a mapping declaring `millis` truncates
//! sub-millisecond precision. A file exported under the same mapping it was
//! loaded with never has sub-unit precision to lose. A metrics export refuses
//! a sample with sub-unit precision instead of truncating it, because a
//! truncated timestamp re-loads as a different sample.
//!
//! # Metrics: duplicates and series identity
//!
//! The query path serves one sample per `(series, ts)`, chosen by
//! [`ravel_query::serves_over`]; the export orders its candidates with
//! [`ravel_query::DedupKey::serve_cmp`], the comparison that function is built
//! on, so it compares candidates with the same function. The export writes
//! exactly that sample and counts every other candidate as
//! `samples_deduplicated`, so two loads of one sample export as one row
//! whatever their bit patterns.
//!
//! Loading a metrics file does not store names as written: it sanitizes the
//! metric and label names, appends the unit suffix and, for `kind =
//! "counter"`, `_total`, and drops an empty label value. A stored name already
//! carries those suffixes, so the export writes, for each series, the name the
//! load maps back onto the stored one: the stored name itself when the load
//! leaves it unchanged (every suffix the load would add is already there),
//! otherwise the stored name less its trailing `_total` (a counter with a
//! unit, where the unit suffix sits before `_total`), less its unit suffix, or
//! less both (a name the suffixes took past the metric-name length cap, which
//! the load applies to the name as written). Each candidate is checked
//! by running the load's own naming rule over it, so an export never writes a
//! name that re-loads onto a different series; a series no candidate
//! reproduces is refused by name. A `name` literal mapping writes no name
//! column, since the load names every row from the literal, and exports only
//! the series that literal names. A series carrying a label the mapping does
//! not name is refused, because the re-load would drop it. A
//! `[metrics.histogram]` mapping is refused outright
//! ([`HISTOGRAM_MAPPING_REFUSAL`]): export the exploded series with a scalar
//! mapping instead.
//!
//! # Spans: stored as they are
//!
//! Spans have no deduplication: every stored span in the window is one row,
//! so a span loaded twice exports twice. Rows sort by `(start_ts, trace_id,
//! span_id)`; spans equal on all three keep the snapshot's fetch order. Each
//! timestamp is written in its own declared unit, and a span whose start or
//! end is not a whole number of that unit is refused rather than truncated,
//! as a metrics sample is. RSPAN stores every attribute as a string, which a
//! load produced by coercing the typed cell; each mapped attribute is written
//! as the value of its declared type that coerces back to the stored string,
//! and a stored string no such value produces (`"007"` under `i64`) is
//! refused by name. A `[spans]` mapping has no `attrs_map_column`, so only
//! the mapped fields are written: the reserved attributes holding a span's
//! kind, trace state, flags, events and links, any stored attribute the
//! mapping does not name, and a parent id, status code or status message the
//! mapping has no column for, are not, and the report counts the spans that
//! carried one ([`SpansExportReport::spans_with_unwritten_data`]).

use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    MapBuilder, StringBuilder,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use ravel_logseg::record::{attr_value_to_string, decode_stream_attrs};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::IngestLimits;
use ravel_otlp::normalize::prometheus_family_name;
use ravel_otlp::promcompat::format_float;
use ravel_query::erasure::{
    is_erased_span, retain_histogram_series, retain_series_soa, retain_unerased_log_records,
    snapshot_pending_erasure_predicates,
};
use ravel_query::{
    DedupKey, FetchedSeriesSoa, LogQuery, LogSegmentFetcher, SegmentFetcher, SpanSegmentFetcher,
};
use ravel_rspan::{SpanQuery, SpanRecord, StatusCode};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{LabelSet, METRIC_NAME_LABEL, SeriesId, Signal, TenantHash, TenantId, TimeRange};

use crate::load::{
    AttrMap, ColType, Mapping, MetricsMapping, SpansMapping, TsUnit, normalized_family_name,
};
use crate::maintain::SignalArg;
use crate::store::{StoreSelection, require_tenant_data_present};

/// Rows per output Arrow batch. The decoded records are already resident, so
/// this bounds only the Arrow/Parquet write buffer, not the peak of the read.
const EXPORT_BATCH_ROWS: usize = 10_000;

/// What one `export --signal logs` run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportReport {
    /// Rows written to the output file: exactly the records in
    /// `[start_ns, end_ns)` that survived erasure exclusion.
    pub rows_written: u64,
    /// Segments the resolved snapshot named and the fetcher actually read
    /// (a segment whose blocks were all pruned reads as zero rows, not as a
    /// skipped segment).
    pub segments_read: u64,
    /// Segments `Catalog::resolve` pruned by event-time range before any
    /// object was fetched.
    pub segments_pruned: u64,
    /// Pending selective-erasure predicates applied to this read. Nonzero
    /// means rows may have been excluded that the raw objects still hold.
    pub erasure_predicates: usize,
}

/// What one `export --signal metrics` run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsExportReport {
    /// Rows written to the output file: one per `(series, ts)` in
    /// `[start_ns, end_ns)` that survived erasure exclusion, after
    /// deduplication.
    pub rows_written: u64,
    /// Series that contributed at least one row.
    pub series_written: u64,
    /// Series with samples in the window that a `name` literal mapping does
    /// not select, because their metric name is not the one the literal
    /// loads as. Always zero for a `name_column` mapping, which selects every
    /// series or refuses.
    pub series_skipped: u64,
    /// Segments the resolved snapshot named and the fetcher read.
    pub segments_read: u64,
    /// Segments `Catalog::resolve` pruned by event-time range before any
    /// object was fetched.
    pub segments_pruned: u64,
    /// Pending selective-erasure predicates applied to this read.
    pub erasure_predicates: usize,
    /// In-window samples of a selected series that lost the per-`(series,
    /// ts)` duplicate resolution to another sample and were not written.
    pub samples_deduplicated: u64,
}

/// What one `export --signal spans` run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpansExportReport {
    /// Rows written to the output file: one per stored span whose start is in
    /// `[start_ns, end_ns)` and that survived erasure exclusion.
    pub rows_written: u64,
    /// Segments the resolved snapshot named and the fetcher read.
    pub segments_read: u64,
    /// Segments `Catalog::resolve` pruned by event-time range before any
    /// object was fetched.
    pub segments_pruned: u64,
    /// Pending selective-erasure predicates applied to this read.
    pub erasure_predicates: usize,
    /// Written spans that carried at least one stored value the file does not
    /// carry: an attribute under a reserved key holding a span field the
    /// `[spans]` mapping cannot name (kind, trace state, flags, events,
    /// links) or under a key the mapping does not name, a parent id when the
    /// mapping has no `parent_span_id_column`, a status code other than Unset
    /// when it has no `status_code_column`, or a status message when it has
    /// no `status_message_column`. A load of the file gives those spans
    /// without them.
    pub spans_with_unwritten_data: u64,
}

/// The refusal a `[metrics.histogram]` mapping gets from `export --signal
/// metrics`.
pub const HISTOGRAM_MAPPING_REFUSAL: &str = "export --signal metrics cannot write a mapping with \
     [metrics.histogram]: a load explodes each of its rows into _bucket, _sum and _count series \
     and accumulates the bucket counts, and the stored series do not record which of them were \
     one data point, so no file in that shape re-loads onto the same series. Export the exploded \
     series with a scalar mapping instead (no [metrics.histogram], no unit, no kind, name_column \
     for the metric name and a [[metrics.label]] for le); loading that file with the same scalar \
     mapping reproduces the same series and samples.";

/// Why `signal` cannot be exported, or `None` when it can.
///
/// Every signal `load` imports is exported, so this is `None` for all of
/// them. The match is exhaustive so a new signal has to be classified here
/// before `run` accepts it.
pub fn unsupported_signal_message(signal: SignalArg) -> Option<String> {
    match signal {
        SignalArg::Logs | SignalArg::Metrics | SignalArg::Spans => None,
    }
}

/// Parse the `--mapping` file and run the export, or refuse the signal.
///
/// The signal check runs before the mapping file is opened, so refusing an
/// unsupported signal does not first fail on an unrelated path error. The
/// mapping section read is the one `--signal` names, under the same section
/// rules `load` applies.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    store: Arc<dyn ObjectStoreBackend>,
    selection: StoreSelection,
    tenant: &str,
    signal: SignalArg,
    start_ns: i64,
    end_ns: i64,
    mapping_path: &Path,
    out: &Path,
    shards: u32,
    max_ingest_lag_ns: Option<i64>,
    now_ns: i64,
) -> anyhow::Result<()> {
    if let Some(message) = unsupported_signal_message(signal) {
        return Err(anyhow::anyhow!(message));
    }
    let text = std::fs::read_to_string(mapping_path)
        .with_context(|| format!("failed to read --mapping {}", mapping_path.display()))?;
    if signal == SignalArg::Metrics {
        let mapping = crate::load::parse_metrics_mapping(&text)?;
        selection.print_header();
        let report = export_metrics(
            store,
            selection,
            tenant,
            start_ns,
            end_ns,
            &mapping,
            out,
            shards,
            max_ingest_lag_ns,
            now_ns,
        )
        .await?;
        println!("output: {}", out.display());
        println!("rows_written: {}", report.rows_written);
        println!("series_written: {}", report.series_written);
        println!("series_skipped: {}", report.series_skipped);
        println!("segments_read: {}", report.segments_read);
        println!("segments_pruned: {}", report.segments_pruned);
        println!("erasure_predicates: {}", report.erasure_predicates);
        println!("samples_deduplicated: {}", report.samples_deduplicated);
        return Ok(());
    }
    if signal == SignalArg::Spans {
        let mapping = crate::load::parse_spans_mapping(&text)?;
        selection.print_header();
        let report = export_spans(
            store,
            selection,
            tenant,
            start_ns,
            end_ns,
            &mapping,
            out,
            shards,
            max_ingest_lag_ns,
            now_ns,
        )
        .await?;
        println!("output: {}", out.display());
        println!("rows_written: {}", report.rows_written);
        println!("segments_read: {}", report.segments_read);
        println!("segments_pruned: {}", report.segments_pruned);
        println!("erasure_predicates: {}", report.erasure_predicates);
        println!(
            "spans_with_unwritten_data: {}",
            report.spans_with_unwritten_data
        );
        return Ok(());
    }
    let mapping = crate::load::parse_mapping(&text)?;
    selection.print_header();
    let report = export_logs(
        store,
        selection,
        tenant,
        start_ns,
        end_ns,
        &mapping,
        out,
        shards,
        max_ingest_lag_ns,
        now_ns,
    )
    .await?;
    println!("output: {}", out.display());
    println!("rows_written: {}", report.rows_written);
    println!("segments_read: {}", report.segments_read);
    println!("segments_pruned: {}", report.segments_pruned);
    println!("erasure_predicates: {}", report.erasure_predicates);
    Ok(())
}

/// Export the logs a tenant holds in `[start_ns, end_ns)` to a Parquet file
/// laid out by `mapping`.
///
/// `shards` is the configured shard count, resolved the same way the other
/// read commands resolve it; the catalog reads the tenant's real shard
/// generations from its provisioning record on top of it.
///
/// `max_ingest_lag_ns` replaces `CatalogConfig::max_ingest_lag_ns` (2 hours
/// when `None`). `Catalog::resolve` lists ingest-hour buckets from `start_ns`
/// minus this value forward, which is what reaches the bucket of a record
/// whose event time falls in a later ingest hour than the bucket it was written
/// into, so an export resolving with a different value than the server's own
/// `--max-ingest-lag` answers a different window than a query over the same
/// range. Nothing on the bucket records what the server was configured with.
///
/// `out` is checked before any object-store request: an existing directory,
/// any other existing non-regular file, and any path under `/dev` are refused
/// up front, because the final step is a rename that cannot write through to
/// them (see [`write_parquet`]).
#[allow(clippy::too_many_arguments)]
pub async fn export_logs(
    store: Arc<dyn ObjectStoreBackend>,
    selection: StoreSelection,
    tenant: &str,
    start_ns: i64,
    end_ns: i64,
    mapping: &Mapping,
    out: &Path,
    shards: u32,
    max_ingest_lag_ns: Option<i64>,
    now_ns: i64,
) -> anyhow::Result<ExportReport> {
    check_window(start_ns, end_ns)?;
    check_output_path(out)?;
    let tenant_hash = TenantId::new(tenant).hash();
    require_tenant_data_present(selection, store.as_ref(), "export", tenant, &tenant_hash).await?;

    let snapshot = resolve_snapshot(
        &store,
        &tenant_hash,
        Signal::Logs,
        start_ns,
        end_ns,
        shards,
        max_ingest_lag_ns,
        now_ns,
    )
    .await?;

    let predicates = snapshot_pending_erasure_predicates(&snapshot);
    let erasure_predicates = predicates.len();
    let query =
        LogQuery::new(start_ns, fetch_range_end_ns(end_ns)).with_erasure(predicates.clone());
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store));
    let accounting = QueryAccounting::new();

    let mut records: Vec<LogRecord> = Vec::new();
    let mut segments_read = 0u64;
    for seg_ref in &snapshot.segments {
        let fetched = fetcher
            .fetch_accounted_with_tenant(seg_ref, tenant_hash, &query, &accounting)
            .await
            .map_err(|err| {
                anyhow::anyhow!(
                    "failed to read log segment {}: {err}",
                    seg_ref.data_object_key
                )
            })?;
        if let Some(output) = fetched {
            segments_read += 1;
            records.extend(output.records);
        }
    }
    records.retain(|record| in_export_window(record.ts_ns, start_ns, end_ns));
    retain_unerased_log_records(&mut records, &predicates)
        .map_err(|err| anyhow::anyhow!("failed to decode a record's stream attributes: {err}"))?;
    records.sort_by_key(|record| record.ts_ns);

    let resource_by_stream = decode_resources(&records)?;
    let rows_written = write_parquet(mapping, &records, &resource_by_stream, out)?;

    Ok(ExportReport {
        rows_written,
        segments_read,
        segments_pruned: snapshot.segments_pruned,
        erasure_predicates,
    })
}

/// The inclusive upper bound of the `LogQuery` range that covers the
/// half-open export window `[start_ns, end_ns)`.
///
/// `LogQuery`'s range is inclusive on both ends and the RLOG reader applies it
/// per row as well as per block, so this one subtraction is what keeps a
/// record at exactly `--end` out of the fetch. `saturating_sub` leaves an
/// `end_ns` of `i64::MIN` alone; `export_logs` has already refused an empty
/// window by then, so no reachable call is at that bound.
fn fetch_range_end_ns(end_ns: i64) -> i64 {
    end_ns.saturating_sub(1)
}

/// Whether `ts_ns` falls inside the half-open export window
/// `[start_ns, end_ns)`.
///
/// Applying this over the decoded rows is deliberately a second application
/// of a bound the fetch already carries: [`fetch_range_end_ns`] gives the
/// fetcher the same window in its own inclusive spelling, and its row filter
/// is exact, so in the normal case this rejects nothing. The two spellings are
/// what make the redundancy worth keeping -- an off-by-one introduced in
/// either one is a silently widened window, and an inclusive `end - 1` in one
/// crate and an exclusive `end` in another do not fail together. This is the
/// half that can be checked directly, and the boundary cases below do.
fn in_export_window(ts_ns: i64, start_ns: i64, end_ns: i64) -> bool {
    ts_ns >= start_ns && ts_ns < end_ns
}

fn check_window(start_ns: i64, end_ns: i64) -> anyhow::Result<()> {
    if end_ns <= start_ns {
        anyhow::bail!(
            "--end must be after --start: the export window is half-open [start, end), and \
             [{start_ns}, {end_ns}) is empty"
        );
    }
    Ok(())
}

/// Resolves the tenant's `signal` catalog for `[start_ns, end_ns)` at one
/// snapshot.
///
/// Enforcing, matching the server's query path and `catalog list`: the
/// tenant's real shard-generation history decides which shards each hour is
/// scanned across, instead of short-circuiting to generation 0 and
/// under-scanning `0..--shards` after a reshard-increase.
#[allow(clippy::too_many_arguments)]
async fn resolve_snapshot(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant_hash: &TenantHash,
    signal: Signal,
    start_ns: i64,
    end_ns: i64,
    shards: u32,
    max_ingest_lag_ns: Option<i64>,
    now_ns: i64,
) -> anyhow::Result<ravel_catalog::Snapshot> {
    let mut catalog_config = ravel_catalog::CatalogConfig {
        shard_count: shards,
        ..ravel_catalog::CatalogConfig::default()
    };
    if let Some(ns) = max_ingest_lag_ns {
        catalog_config.max_ingest_lag_ns = ns;
    }
    let catalog = ravel_catalog::Catalog::new(Arc::clone(store), catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?
        .with_provisioning_enforcement();
    let range = TimeRange { start_ns, end_ns };
    catalog
        .resolve(tenant_hash, signal, range, &[], now_ns)
        .await
        .map_err(|err| anyhow::anyhow!("failed to resolve catalog: {err}"))
}

/// Export the metric samples a tenant holds in `[start_ns, end_ns)` to a
/// Parquet file laid out by `mapping`: one row per `(series, ts)`, sorted by
/// event time, carrying the columns the `[metrics]` section names.
///
/// `shards`, `max_ingest_lag_ns` and `out` mean what they mean for
/// [`export_logs`]. A mapping shape the export refuses (a
/// `[metrics.histogram]`, an unusable `name` literal, two fields sharing one
/// output column), and an unusable `out`, are all refused before any
/// object-store request.
///
/// The whole export is refused, and nothing is written, when a series in the
/// window cannot be written so that `load` with the same mapping lands it on
/// the same series (see the module documentation): a stored name no
/// `name_column` spelling loads back as, a label the mapping does not name, a
/// native-histogram series, or a sample whose timestamp is not a whole number
/// of the mapping's `ts_unit`.
#[allow(clippy::too_many_arguments)]
pub async fn export_metrics(
    store: Arc<dyn ObjectStoreBackend>,
    selection: StoreSelection,
    tenant: &str,
    start_ns: i64,
    end_ns: i64,
    mapping: &MetricsMapping,
    out: &Path,
    shards: u32,
    max_ingest_lag_ns: Option<i64>,
    now_ns: i64,
) -> anyhow::Result<MetricsExportReport> {
    check_window(start_ns, end_ns)?;
    if mapping.is_histogram() {
        anyhow::bail!(HISTOGRAM_MAPPING_REFUSAL);
    }
    check_metrics_output_columns(mapping)?;
    let limits = IngestLimits::default();
    let literal_family_name = match &mapping.name {
        Some(literal) => Some(reloaded_family_name(literal, mapping, &limits).ok_or_else(
            || {
                anyhow::anyhow!(
                    "--mapping [metrics] name {literal:?} is unusable: it is empty or longer \
                     than the metric-name limit of {}",
                    limits.max_metric_name_len
                )
            },
        )?),
        None => None,
    };
    check_output_path(out)?;
    let tenant_hash = TenantId::new(tenant).hash();
    require_tenant_data_present(selection, store.as_ref(), "export", tenant, &tenant_hash).await?;

    let snapshot = resolve_snapshot(
        &store,
        &tenant_hash,
        Signal::Metrics,
        start_ns,
        end_ns,
        shards,
        max_ingest_lag_ns,
        now_ns,
    )
    .await?;
    let predicates = snapshot_pending_erasure_predicates(&snapshot);
    let erasure_predicates = predicates.len();
    let fetcher = SegmentFetcher::new(Arc::clone(&store));
    let accounting = QueryAccounting::new();
    let selects = |labels: &LabelSet| match &literal_family_name {
        Some(family) => labels.get(METRIC_NAME_LABEL) == Some(family.as_str()),
        None => true,
    };

    let mut by_series: HashMap<SeriesId, SeriesCandidates> = HashMap::new();
    let mut skipped: HashSet<SeriesId> = HashSet::new();
    let mut segments_read = 0u64;
    for seg_ref in &snapshot.segments {
        let (mut scalar, _stats, mut histograms) = fetcher
            .fetch_soa_and_histograms_accounted(tenant_hash, seg_ref, &[], &accounting)
            .await
            .map_err(|err| {
                anyhow::anyhow!(
                    "failed to read metric segment {}: {err}",
                    seg_ref.data_object_key
                )
            })?;
        segments_read += 1;
        retain_series_soa(&mut scalar, &predicates);
        retain_histogram_series(&mut histograms, &predicates);
        for series in &histograms {
            if !series
                .timestamps
                .iter()
                .any(|ts_ns| in_export_window(*ts_ns, start_ns, end_ns))
            {
                continue;
            }
            if !selects(&series.labels) {
                skipped.insert(series.series_id);
                continue;
            }
            anyhow::bail!(
                "series {} holds native (exponential) histogram samples in [{start_ns}, \
                 {end_ns}), which a [metrics] mapping cannot carry: native histograms are not \
                 mappable in this version, and an export that left them out would not round-trip \
                 the window. Export a window that holds none{}.",
                describe_series(&series.labels),
                if literal_family_name.is_some() {
                    ""
                } else {
                    ", or name one metric with a name literal"
                }
            );
        }
        for run in scalar {
            collect_run(&mut by_series, run, start_ns, end_ns)?;
        }
    }

    let label_index: HashMap<String, usize> = mapping
        .sanitized_label_names()
        .into_iter()
        .enumerate()
        .map(|(i, name)| (name, i))
        .collect();
    let factor = mapping.ts_unit.factor();
    let mut samples_deduplicated = 0u64;
    let mut series_out: Vec<OutputSeries> = Vec::new();
    let mut refusals = SeriesRefusals::default();
    for (series_id, candidates) in by_series {
        if candidates.samples.is_empty() {
            continue;
        }
        if !selects(&candidates.labels) {
            skipped.insert(series_id);
            continue;
        }
        let labels = &candidates.labels;
        let sort_key: SeriesSortKey = labels
            .iter()
            .map(|l| (l.name.clone(), l.value.clone()))
            .collect();
        let written_name = match (labels.get(METRIC_NAME_LABEL), &literal_family_name) {
            (None, _) => {
                refusals.add(
                    Refusal::NoMetricName,
                    &sort_key,
                    format!(
                        "series {} carries no {METRIC_NAME_LABEL} label, so no mapping can name it",
                        describe_series(labels)
                    ),
                );
                None
            }
            (Some(_), Some(_)) => None,
            (Some(stored_name), None) => {
                let written = written_metric_name(stored_name, mapping, &limits);
                if written.is_none() {
                    refusals.add(
                        Refusal::UnwritableName,
                        &sort_key,
                        unwritable_name(labels, mapping, &limits),
                    );
                }
                written
            }
        };
        let label_values =
            mapped_label_values(labels, mapping, &label_index, &sort_key, &mut refusals);
        let (samples, dropped) = resolve_duplicates(candidates.samples);
        samples_deduplicated += dropped;
        if let Some((ts_ns, _)) = samples.iter().find(|(ts_ns, _)| ts_ns % factor != 0) {
            refusals.add(
                Refusal::SubUnitTimestamp,
                &sort_key,
                format!(
                    "a sample of series {} is at {ts_ns} ns, which is not a whole number of {} \
                     (the mapping's ts_unit); writing it in {} would move it onto a different \
                     timestamp. Export with a finer ts_unit.",
                    describe_series(labels),
                    mapping.ts_unit.as_str(),
                    mapping.ts_unit.as_str()
                ),
            );
        }
        series_out.push(OutputSeries {
            sort_key,
            written_name,
            label_values,
            samples,
        });
    }
    refusals.into_result()?;
    // Rows sort by event time, then by label set, so the file is the same for
    // the same store contents whatever order the segments were fetched in.
    series_out.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
    let mut rows: Vec<MetricRow> = series_out
        .iter()
        .enumerate()
        .flat_map(|(series, s)| {
            s.samples
                .iter()
                .enumerate()
                .map(move |(sample, (ts_ns, _))| MetricRow {
                    ts_ns: *ts_ns,
                    series,
                    sample,
                })
        })
        .collect();
    rows.sort_by_key(|row| (row.ts_ns, row.series));

    let empty = build_metrics_batch(mapping, &series_out, &[])?;
    let rows_written = write_output(out, empty.schema(), |writer| {
        let mut rows_written = 0u64;
        for chunk in rows.chunks(EXPORT_BATCH_ROWS) {
            let batch = build_metrics_batch(mapping, &series_out, chunk)?;
            writer
                .write(&batch)
                .with_context(|| format!("failed to write a batch to {}", out.display()))?;
            rows_written += batch.num_rows() as u64;
        }
        Ok(rows_written)
    })?;

    Ok(MetricsExportReport {
        rows_written,
        series_written: series_out.len() as u64,
        series_skipped: skipped.len() as u64,
        segments_read,
        segments_pruned: snapshot.segments_pruned,
        erasure_predicates,
        samples_deduplicated,
    })
}

/// One in-window sample of a series before duplicate resolution, with the
/// key the query path's merge orders duplicates by.
struct SampleCandidate {
    ts_ns: i64,
    key: DedupKey,
}

/// Every in-window candidate sample of one series, across all the runs and
/// segments that hold it.
struct SeriesCandidates {
    labels: LabelSet,
    samples: Vec<SampleCandidate>,
}

/// A series' full label set as `(name, value)` pairs, `__name__` included, in
/// label-name order: the order the output file lists series in.
type SeriesSortKey = Vec<(String, String)>;

/// One series as it is written: the name for the `name_column` (`None` under a
/// `name` literal), one value per `[[metrics.label]]` in mapping order (`None`
/// writes a null, which the load reads as an absent label), and the
/// deduplicated `(ts_ns, value)` samples in ascending `ts_ns`.
struct OutputSeries {
    sort_key: SeriesSortKey,
    written_name: Option<String>,
    label_values: Vec<Option<String>>,
    samples: Vec<(i64, f64)>,
}

/// One output row: sample `sample` of `series_out[series]`.
struct MetricRow {
    ts_ns: i64,
    series: usize,
    sample: usize,
}

/// Adds the in-window samples of one fetched run to its series' candidates.
///
/// A sample's key is the run's per-sample priority when it carries a column,
/// otherwise the run-wide `(created_unix_ns, writer_epoch, writer_seq)` plus
/// the sample's position in the run, which is how the query path's merge keys
/// the same run.
fn collect_run(
    by_series: &mut HashMap<SeriesId, SeriesCandidates>,
    run: FetchedSeriesSoa,
    start_ns: i64,
    end_ns: i64,
) -> anyhow::Result<()> {
    let samples = run.timestamps.len();
    if run.values.len() != samples {
        anyhow::bail!(
            "series {} decoded {samples} timestamps but {} values in one run",
            describe_series(&run.labels),
            run.values.len()
        );
    }
    if let Some(column) = &run.per_sample_priorities
        && column.len() != samples
    {
        anyhow::bail!(
            "series {} decoded {samples} samples but {} dedup priorities in one run",
            describe_series(&run.labels),
            column.len()
        );
    }
    let entry = by_series
        .entry(run.series_id)
        .or_insert_with(|| SeriesCandidates {
            labels: run.labels.clone(),
            samples: Vec::new(),
        });
    for (pos, (&ts_ns, &value)) in run.timestamps.iter().zip(&run.values).enumerate() {
        if !in_export_window(ts_ns, start_ns, end_ns) {
            continue;
        }
        let key = match run
            .per_sample_priorities
            .as_ref()
            .and_then(|column| column.get(pos))
        {
            Some(priority) => DedupKey::from_parts(
                priority.created_unix_ns,
                priority.writer_epoch,
                priority.writer_seq,
                priority.in_page_index,
                value,
            ),
            None => DedupKey::from_parts(
                run.created_unix_ns,
                run.writer_epoch,
                run.writer_seq,
                u32::try_from(pos).unwrap_or(u32::MAX),
                value,
            ),
        };
        entry.samples.push(SampleCandidate { ts_ns, key });
    }
    Ok(())
}

/// Resolves duplicate timestamps the way the query path's merge does: at each
/// `ts`, the candidate [`DedupKey::serve_cmp`] orders greatest wins. Returns
/// the winners in ascending `ts` and the number of candidates dropped.
fn resolve_duplicates(mut samples: Vec<SampleCandidate>) -> (Vec<(i64, f64)>, u64) {
    samples.sort_by(|a, b| a.ts_ns.cmp(&b.ts_ns).then(a.key.serve_cmp(&b.key)));
    let total = samples.len();
    let mut winners: Vec<(i64, f64)> = Vec::with_capacity(total);
    for candidate in samples {
        // Ascending order puts the served candidate of a timestamp last, so
        // the last one seen replaces every earlier one.
        let sample = (candidate.ts_ns, f64::from_bits(candidate.key.value_bits));
        match winners.last_mut() {
            Some(last) if last.0 == candidate.ts_ns => *last = sample,
            _ => winners.push(sample),
        }
    }
    let dropped = (total - winners.len()) as u64;
    (winners, dropped)
}

/// The family name `load` gives a row whose name cell is `raw` under
/// `mapping`, or `None` where the load refuses the name.
fn reloaded_family_name(
    raw: &str,
    mapping: &MetricsMapping,
    limits: &IngestLimits,
) -> Option<String> {
    let (kind, is_monotonic_sum) = mapping.metric_kind();
    normalized_family_name(raw, mapping, kind, is_monotonic_sum, limits).ok()
}

/// The `name_column` value that loads back as `stored` under `mapping`: the
/// stored name itself, else the stored name less a trailing `_total`, less
/// the mapping's unit suffix, or less both. The last two reach a name the
/// suffixes took past the metric-name length cap. Each candidate is checked
/// against [`reloaded_family_name`], so `None` means no candidate reproduces
/// the series.
fn written_metric_name(
    stored: &str,
    mapping: &MetricsMapping,
    limits: &IngestLimits,
) -> Option<String> {
    let (kind, _) = mapping.metric_kind();
    // The unit suffix as a load spells it after a name, `_seconds` for `s`.
    let with_unit = prometheus_family_name("a", mapping.unit(), kind, false);
    let unit_suffix = with_unit.strip_prefix('a').filter(|s| !s.is_empty());
    let less_total = stored.strip_suffix("_total");
    let less_unit = unit_suffix.and_then(|suffix| stored.strip_suffix(suffix));
    let less_both = less_total
        .zip(unit_suffix)
        .and_then(|(name, suffix)| name.strip_suffix(suffix));
    [Some(stored), less_total, less_unit, less_both]
        .into_iter()
        .flatten()
        .find(|candidate| {
            reloaded_family_name(candidate, mapping, limits).as_deref() == Some(stored)
        })
        .map(str::to_string)
}

/// The refusal for a series whose stored name no `name_column` value loads
/// back as.
fn unwritable_name(labels: &LabelSet, mapping: &MetricsMapping, limits: &IngestLimits) -> String {
    let stored = labels.get(METRIC_NAME_LABEL).unwrap_or_default();
    let kind = if mapping.metric_kind().1 {
        "counter"
    } else {
        "gauge"
    };
    let as_written = match reloaded_family_name(stored, mapping, limits) {
        Some(name) => format!("names it {name:?}"),
        None => "refuses it".to_string(),
    };
    format!(
        "series {} cannot be exported under this mapping: no name_column value loads back as \
         {stored:?} with unit = {:?} and kind = {kind:?} (written as {stored:?}, a load \
         {as_written}), so the exported file would re-load onto a different series. Export it \
         with the unit and kind it was loaded with; a mapping with neither loads every sanitized \
         metric name back unchanged.",
        describe_series(labels),
        mapping.unit(),
    )
}

/// One output value per `[[metrics.label]]`, in mapping order, for a series'
/// stored labels. Records a refusal for the first stored label the mapping
/// does not name and the first stored empty value, since the load drops both
/// and would land the samples on a different series.
fn mapped_label_values(
    labels: &LabelSet,
    mapping: &MetricsMapping,
    label_index: &HashMap<String, usize>,
    sort_key: &[(String, String)],
    refusals: &mut SeriesRefusals,
) -> Vec<Option<String>> {
    let mut values: Vec<Option<String>> = vec![None; mapping.labels.len()];
    let mut unmapped = false;
    let mut empty = false;
    for label in labels.iter() {
        if label.name == METRIC_NAME_LABEL {
            continue;
        }
        let Some(&i) = label_index.get(label.name.as_str()) else {
            if !unmapped {
                unmapped = true;
                refusals.add(
                    Refusal::UnmappedLabel,
                    sort_key,
                    format!(
                        "series {} carries the label {:?}, which no [[metrics.label]] in the \
                         mapping names; a load of the exported file would drop it and land the \
                         samples on a different series. Add a [[metrics.label]] for it.",
                        describe_series(labels),
                        label.name
                    ),
                );
            }
            continue;
        };
        if label.value.is_empty() {
            if !empty {
                empty = true;
                refusals.add(
                    Refusal::EmptyLabelValue,
                    sort_key,
                    format!(
                        "series {} carries the label {:?} with an empty value, which a load \
                         drops, so the exported file would re-load onto a different series",
                        describe_series(labels),
                        label.name
                    ),
                );
            }
            continue;
        }
        if let Some(slot) = values.get_mut(i) {
            *slot = Some(label.value.clone());
        }
    }
    values
}

/// Why a series cannot be written. The declaration order is the order the
/// kinds are reported in when several apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Refusal {
    NoMetricName,
    UnwritableName,
    UnmappedLabel,
    EmptyLabelValue,
    SubUnitTimestamp,
}

/// Every per-offender refusal of one export, gathered so the one reported does
/// not depend on the order the offenders were visited in. `K` is the refusal
/// kind, whose declaration order is the report order; `S` is an offender's
/// output sort key, which identifies it.
struct Refusals<K, S> {
    by_kind: BTreeMap<K, KindOffenders<S>>,
}

impl<K, S> Default for Refusals<K, S> {
    fn default() -> Self {
        Refusals {
            by_kind: BTreeMap::new(),
        }
    }
}

/// The offenders one refusal kind covers, by output sort key, and the message
/// of the first of them in output order.
struct KindOffenders<S> {
    offenders: BTreeSet<S>,
    first_message: String,
}

impl<K: Ord, S: Ord> Refusals<K, S> {
    /// Records the offender `sort_key` under `kind`. Adding one offender twice
    /// under one kind counts it once.
    fn add<Q>(&mut self, kind: K, sort_key: &Q, message: String)
    where
        Q: ?Sized + Ord + ToOwned<Owned = S>,
        S: Borrow<Q>,
    {
        let offenders = self.by_kind.entry(kind).or_insert_with(|| KindOffenders {
            offenders: BTreeSet::new(),
            first_message: String::new(),
        });
        if !offenders.offenders.insert(sort_key.to_owned()) {
            return;
        }
        if offenders.offenders.first().map(Borrow::borrow) == Some(sort_key) {
            offenders.first_message = message;
        }
    }

    /// Refuses on the first kind any offender hit, naming the first offender
    /// in output order and how many offenders that kind covers; offenders
    /// refused only for a later kind are not counted.
    fn into_result_for(self, signal: &str, noun: &str) -> anyhow::Result<()> {
        match self.by_kind.into_values().next() {
            Some(KindOffenders {
                offenders,
                first_message,
            }) => anyhow::bail!(
                "export --signal {signal} refused on {} {noun} for this reason; first: \
                 {first_message}",
                offenders.len()
            ),
            None => Ok(()),
        }
    }
}

/// The per-series refusals of one metrics export.
type SeriesRefusals = Refusals<Refusal, SeriesSortKey>;

impl SeriesRefusals {
    fn into_result(self) -> anyhow::Result<()> {
        self.into_result_for("metrics", "series")
    }
}

/// `name{label="value", ...}` for a refusal message.
fn describe_series(labels: &LabelSet) -> String {
    let name = labels.get(METRIC_NAME_LABEL).unwrap_or_default();
    let rest: Vec<String> = labels
        .iter()
        .filter(|l| l.name != METRIC_NAME_LABEL)
        .map(|l| format!("{}={:?}", l.name, l.value))
        .collect();
    format!("{name}{{{}}}", rest.join(", "))
}

/// Refuses a mapping that writes two fields to one output column, the columns
/// [`build_metrics_batch`] writes, before the export reads anything.
fn check_metrics_output_columns(mapping: &MetricsMapping) -> anyhow::Result<()> {
    check_distinct_columns(
        std::iter::once(&mapping.ts_column)
            .chain(&mapping.name_column)
            .chain(std::iter::once(&mapping.value_column))
            .chain(mapping.labels.iter().map(|label| &label.column)),
    )
}

/// Refuses a list of output column names that names one column twice.
fn check_distinct_columns<'a>(columns: impl Iterator<Item = &'a String>) -> anyhow::Result<()> {
    let mut seen: HashSet<&str> = HashSet::new();
    for name in columns {
        if !seen.insert(name.as_str()) {
            anyhow::bail!(
                "the mapping writes two different fields to the output column {name:?}; give \
                 each one its own column name"
            );
        }
    }
    Ok(())
}

/// Builds the output batch for `rows`: the `ts` column in the mapping's
/// `ts_unit`, the `name_column` when the mapping has one, the `value` column,
/// and one `Utf8` column per `[[metrics.label]]`, null where the series lacks
/// the label. These are the Arrow types the metrics `--mapping` reader accepts.
fn build_metrics_batch(
    mapping: &MetricsMapping,
    series: &[OutputSeries],
    rows: &[MetricRow],
) -> anyhow::Result<RecordBatch> {
    let factor = mapping.ts_unit.factor();
    let mut columns: Vec<(String, ArrayRef)> = Vec::new();

    let mut ts = Int64Builder::with_capacity(rows.len());
    let mut value = Float64Builder::with_capacity(rows.len());
    for row in rows {
        ts.append_value(row.ts_ns / factor);
        let sample = series
            .get(row.series)
            .and_then(|s| s.samples.get(row.sample))
            .ok_or_else(|| anyhow::anyhow!("internal error: an export row names no sample"))?;
        value.append_value(sample.1);
    }
    columns.push((mapping.ts_column.clone(), Arc::new(ts.finish())));

    if let Some(name_column) = &mapping.name_column {
        let mut names = StringBuilder::new();
        for row in rows {
            names.append_option(
                series
                    .get(row.series)
                    .and_then(|s| s.written_name.as_deref()),
            );
        }
        columns.push((name_column.clone(), Arc::new(names.finish())));
    }

    columns.push((mapping.value_column.clone(), Arc::new(value.finish())));

    for (i, label) in mapping.labels.iter().enumerate() {
        let mut values = StringBuilder::new();
        for row in rows {
            values.append_option(
                series
                    .get(row.series)
                    .and_then(|s| s.label_values.get(i))
                    .and_then(Option::as_deref),
            );
        }
        columns.push((label.column.clone(), Arc::new(values.finish())));
    }

    // Declared nullable explicitly, for the reason `build_batch` gives.
    RecordBatch::try_from_iter_with_nullable(
        columns.into_iter().map(|(name, array)| (name, array, true)),
    )
    .context("failed to build the export record batch")
}

/// Export the spans a tenant holds whose start is in `[start_ns, end_ns)` to
/// a Parquet file laid out by `mapping`: one row per stored span, sorted by
/// `(start_ts, trace_id, span_id)`, carrying the columns the `[spans]` section
/// names.
///
/// `shards`, `max_ingest_lag_ns` and `out` mean what they mean for
/// [`export_logs`]. Two fields sharing one output column, and an unusable
/// `out`, are refused before any object-store request.
///
/// The whole export is refused, and nothing is written, when a mapped field of
/// a span in the window would not re-load as stored under `mapping` (see the
/// module documentation): a timestamp that is not a whole number of its
/// declared unit, a start a load would re-time or refuse, or a mapped
/// attribute whose stored string the declared type does not read back. The
/// attributes the mapping cannot or does not name, and a parent id, status
/// code or status message the mapping has no column for, are not written and
/// are counted, not refused.
#[allow(clippy::too_many_arguments)]
pub async fn export_spans(
    store: Arc<dyn ObjectStoreBackend>,
    selection: StoreSelection,
    tenant: &str,
    start_ns: i64,
    end_ns: i64,
    mapping: &SpansMapping,
    out: &Path,
    shards: u32,
    max_ingest_lag_ns: Option<i64>,
    now_ns: i64,
) -> anyhow::Result<SpansExportReport> {
    check_window(start_ns, end_ns)?;
    check_spans_output_columns(mapping)?;
    check_output_path(out)?;
    let tenant_hash = TenantId::new(tenant).hash();
    require_tenant_data_present(selection, store.as_ref(), "export", tenant, &tenant_hash).await?;

    let snapshot = resolve_snapshot(
        &store,
        &tenant_hash,
        Signal::Spans,
        start_ns,
        end_ns,
        shards,
        max_ingest_lag_ns,
        now_ns,
    )
    .await?;
    let predicates = snapshot_pending_erasure_predicates(&snapshot);
    let erasure_predicates = predicates.len();
    // The fetch window is an interval-overlap test on `[start_ts, end_ts]`, so
    // it returns every span starting in the export window (a stored span never
    // ends before it starts) plus spans that started earlier and are still
    // open; `in_export_window` below keeps only the former.
    let query = SpanQuery::ts_range(start_ns, fetch_range_end_ns(end_ns));
    let fetcher = SpanSegmentFetcher::new(Arc::clone(&store));
    let accounting = QueryAccounting::new();

    let mut spans: Vec<SpanRecord> = Vec::new();
    let mut segments_read = 0u64;
    for seg_ref in &snapshot.segments {
        let fetched = fetcher
            .fetch_accounted(seg_ref, tenant_hash, &query, None, None, &[], &accounting)
            .await
            .map_err(|err| {
                anyhow::anyhow!(
                    "failed to read span segment {}: {err}",
                    seg_ref.data_object_key
                )
            })?;
        if let Some(output) = fetched {
            segments_read += 1;
            spans.extend(output.records.into_iter().map(|row| row.record));
        }
    }
    spans.retain(|span| in_export_window(span.start_ts_ns, start_ns, end_ns));
    if !predicates.is_empty() {
        spans.retain(|span| !is_erased_span(&span.attrs, span.start_ts_ns, &predicates));
    }
    spans.sort_by_key(|span| (span.start_ts_ns, span.trace_id, span.span_id));

    let rows = span_output_rows(mapping, &spans)?;
    let spans_with_unwritten_data = spans_with_unwritten_data(mapping, &spans);
    let empty = build_spans_batch(mapping, &[])?;
    let rows_written = write_output(out, empty.schema(), |writer| {
        let mut rows_written = 0u64;
        for chunk in rows.chunks(EXPORT_BATCH_ROWS) {
            let batch = build_spans_batch(mapping, chunk)?;
            writer
                .write(&batch)
                .with_context(|| format!("failed to write a batch to {}", out.display()))?;
            rows_written += batch.num_rows() as u64;
        }
        Ok(rows_written)
    })?;

    Ok(SpansExportReport {
        rows_written,
        segments_read,
        segments_pruned: snapshot.segments_pruned,
        erasure_predicates,
        spans_with_unwritten_data,
    })
}

/// How many of `spans` carry a stored value [`build_spans_batch`] does not
/// write under `mapping`: an attribute no mapped attribute names, or a
/// parent id, non-Unset status code or status message whose column the
/// mapping omits.
fn spans_with_unwritten_data(mapping: &SpansMapping, spans: &[SpanRecord]) -> u64 {
    let mapped: BTreeSet<&str> = span_mapped_attributes(mapping)
        .map(|spec| spec.key.as_str())
        .collect();
    let mut unwritten = 0u64;
    for span in spans {
        let lost_parent = mapping.parent_span_id_column.is_none() && span.parent_span_id.is_some();
        let lost_status =
            mapping.status_code_column.is_none() && span.status_code != StatusCode::Unset;
        let lost_message = mapping.status_message_column.is_none()
            && span
                .status_message
                .as_deref()
                .is_some_and(|message| !message.is_empty());
        let lost_attribute = span
            .attrs
            .iter()
            .any(|(key, _)| !mapped.contains(key.as_str()));
        if lost_parent || lost_status || lost_message || lost_attribute {
            unwritten += 1;
        }
    }
    unwritten
}

/// Why a span cannot be written. The declaration order is the order the
/// kinds are reported in when several apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SpanRefusal {
    UnloadableInterval,
    SubUnitTimestamp,
    UnwritableAttribute,
}

/// The per-span refusals of one spans export, keyed by each span's position
/// in the sorted output. Two stored copies of one span are two rows and two
/// offenders.
type SpanRefusals = Refusals<SpanRefusal, usize>;

impl SpanRefusals {
    fn into_result(self) -> anyhow::Result<()> {
        self.into_result_for("spans", "spans")
    }
}

/// One output row: a stored span plus one value per mapped attribute,
/// `resource_attribute` entries first and then `attribute` entries, each
/// already in the declared type, `None` where the span lacks the key.
struct SpanOutRow<'a> {
    span: &'a SpanRecord,
    attrs: Vec<Option<AttrValue>>,
}

/// Every mapped attribute, in the order [`SpanOutRow::attrs`] holds them.
fn span_mapped_attributes(mapping: &SpansMapping) -> impl Iterator<Item = &AttrMap> {
    mapping
        .resource_attributes
        .iter()
        .chain(&mapping.attributes)
}

/// Checks every span in output order against what a load reads back, and
/// returns the rows to write, or the refusal for the first kind any span hit.
fn span_output_rows<'a>(
    mapping: &SpansMapping,
    spans: &'a [SpanRecord],
) -> anyhow::Result<Vec<SpanOutRow<'a>>> {
    let mut refusals = SpanRefusals::default();
    let mut rows = Vec::with_capacity(spans.len());
    for (position, span) in spans.iter().enumerate() {
        if span.start_ts_ns <= 0 || span.end_ts_ns < span.start_ts_ns {
            refusals.add(
                SpanRefusal::UnloadableInterval,
                &position,
                format!(
                    "{} starts at {} ns and ends at {} ns; a load reads a zero start as load \
                     time and refuses a negative start or an end before the start, so the \
                     exported file would not re-load as the same span",
                    describe_span(span),
                    span.start_ts_ns,
                    span.end_ts_ns
                ),
            );
        }
        for (field, ts_ns, unit) in [
            ("start_ts", span.start_ts_ns, mapping.start_ts_unit),
            ("end_ts", span.end_ts_ns, mapping.end_ts_unit),
        ] {
            if ts_ns % unit.factor() != 0 {
                refusals.add(
                    SpanRefusal::SubUnitTimestamp,
                    &position,
                    sub_unit_span_message(span, field, ts_ns, unit),
                );
            }
        }
        let mut attrs = Vec::new();
        for spec in span_mapped_attributes(mapping) {
            let stored = span
                .attrs
                .iter()
                .find_map(|(key, value)| (key == &spec.key).then_some(value.as_str()));
            let value = match stored {
                None => None,
                Some(stored) => {
                    let typed = typed_span_attr(stored, spec.value_type);
                    if typed.is_none() {
                        refusals.add(
                            SpanRefusal::UnwritableAttribute,
                            &position,
                            format!(
                                "{} carries the attribute {:?} with the stored value {stored:?}, \
                                 which the mapping declares {}; no {} cell loads back as that \
                                 string, so the exported file would not re-load as the same \
                                 span. Declare the attribute as str.",
                                describe_span(span),
                                spec.key,
                                col_type_name(spec.value_type),
                                col_type_name(spec.value_type)
                            ),
                        );
                    }
                    typed
                }
            };
            attrs.push(value);
        }
        rows.push(SpanOutRow { span, attrs });
    }
    refusals.into_result()?;
    Ok(rows)
}

fn sub_unit_span_message(span: &SpanRecord, field: &str, ts_ns: i64, unit: TsUnit) -> String {
    let unit = unit.as_str();
    format!(
        "{} has {field} {ts_ns} ns, which is not a whole number of {unit} (the mapping's \
         {field}_unit); writing it in {unit} would move it onto a different timestamp. Export \
         with a finer {field}_unit.",
        describe_span(span)
    )
}

/// `span "name" (trace_id <hex>, span_id <hex>)` for a refusal message.
fn describe_span(span: &SpanRecord) -> String {
    format!(
        "span {:?} (trace_id {}, span_id {})",
        span.name,
        hex::encode(span.trace_id),
        hex::encode(span.span_id)
    )
}

/// The value of declared type `ty` whose load-side string coercion is exactly
/// `stored`, or `None` when there is none.
///
/// RSPAN stores every attribute as a string, and a spans load coerces a typed
/// cell to one by `ravel_otlp`'s own mapping: a bool and an integer take their
/// canonical string form, a float goes through `format_float`, and bytes
/// become lowercase hex. The candidate is parsed from `stored` and kept only
/// when that coercion gives `stored` back, so `"007"` is not an `i64` and
/// `"ABCD"` is not `bytes`.
fn typed_span_attr(stored: &str, ty: ColType) -> Option<AttrValue> {
    let (value, reloaded) = match ty {
        ColType::Str => return Some(AttrValue::Str(stored.to_string())),
        ColType::I64 => {
            let v: i64 = stored.parse().ok()?;
            (AttrValue::I64(v), v.to_string())
        }
        ColType::F64 => {
            let v: f64 = stored.parse().ok()?;
            (AttrValue::F64(v), format_float(v))
        }
        ColType::Bool => {
            let v: bool = stored.parse().ok()?;
            (AttrValue::Bool(v), v.to_string())
        }
        ColType::Bytes => {
            let v = hex::decode(stored).ok()?;
            let reloaded = hex::encode(&v);
            (AttrValue::Bytes(v), reloaded)
        }
    };
    (reloaded == stored).then_some(value)
}

/// The mapping's spelling of a declared attribute type.
fn col_type_name(ty: ColType) -> &'static str {
    match ty {
        ColType::Str => "str",
        ColType::I64 => "i64",
        ColType::F64 => "f64",
        ColType::Bool => "bool",
        ColType::Bytes => "bytes",
    }
}

/// Refuses a spans mapping that writes two fields to one output column, the
/// columns [`build_spans_batch`] writes, before the export reads anything.
fn check_spans_output_columns(mapping: &SpansMapping) -> anyhow::Result<()> {
    check_distinct_columns(
        [&mapping.trace_id_column, &mapping.span_id_column]
            .into_iter()
            .chain(&mapping.parent_span_id_column)
            .chain([
                &mapping.name_column,
                &mapping.start_ts_column,
                &mapping.end_ts_column,
            ])
            .chain(&mapping.status_code_column)
            .chain(&mapping.status_message_column)
            .chain(span_mapped_attributes(mapping).map(|spec| &spec.column)),
    )
}

/// Builds the output batch for `rows`, one column per field the `[spans]`
/// section names, in the Arrow types the spans `--mapping` reader accepts:
/// `FixedSizeBinary` of the id width for the ids (a null parent is a root
/// span), `Utf8` for the name and status message, `Int64` for the two
/// timestamps (each in its own declared unit) and for the status code as
/// OTLP's integer enum, and each mapped attribute in its declared type.
fn build_spans_batch(
    mapping: &SpansMapping,
    rows: &[SpanOutRow<'_>],
) -> anyhow::Result<RecordBatch> {
    let mut columns: Vec<(String, ArrayRef)> = Vec::new();

    let mut trace_ids = FixedSizeBinaryBuilder::new(16);
    let mut span_ids = FixedSizeBinaryBuilder::new(8);
    for row in rows {
        append_id(&mut trace_ids, Some(&row.span.trace_id[..]))?;
        append_id(&mut span_ids, Some(&row.span.span_id[..]))?;
    }
    columns.push((
        mapping.trace_id_column.clone(),
        Arc::new(trace_ids.finish()),
    ));
    columns.push((mapping.span_id_column.clone(), Arc::new(span_ids.finish())));

    if let Some(name) = &mapping.parent_span_id_column {
        let mut parents = FixedSizeBinaryBuilder::new(8);
        for row in rows {
            append_id(
                &mut parents,
                row.span.parent_span_id.as_ref().map(|id| &id[..]),
            )?;
        }
        columns.push((name.clone(), Arc::new(parents.finish())));
    }

    let mut names = StringBuilder::new();
    for row in rows {
        names.append_value(&row.span.name);
    }
    columns.push((mapping.name_column.clone(), Arc::new(names.finish())));

    let ts_column = |unit: TsUnit, ts_of: fn(&SpanRecord) -> i64| -> ArrayRef {
        let factor = unit.factor();
        let mut ts = Int64Builder::with_capacity(rows.len());
        for row in rows {
            ts.append_value(ts_of(row.span) / factor);
        }
        Arc::new(ts.finish())
    };
    columns.push((
        mapping.start_ts_column.clone(),
        ts_column(mapping.start_ts_unit, |span| span.start_ts_ns),
    ));
    columns.push((
        mapping.end_ts_column.clone(),
        ts_column(mapping.end_ts_unit, |span| span.end_ts_ns),
    ));

    if let Some(name) = &mapping.status_code_column {
        let mut codes = Int64Builder::with_capacity(rows.len());
        for row in rows {
            codes.append_value(row.span.status_code as i64);
        }
        columns.push((name.clone(), Arc::new(codes.finish())));
    }

    if let Some(name) = &mapping.status_message_column {
        let mut messages = StringBuilder::new();
        for row in rows {
            messages.append_option(row.span.status_message.as_deref());
        }
        columns.push((name.clone(), Arc::new(messages.finish())));
    }

    for (i, spec) in span_mapped_attributes(mapping).enumerate() {
        let mut column = AttrColumn::new(spec.value_type);
        for row in rows {
            column.push(&spec.key, row.attrs.get(i).and_then(Option::as_ref))?;
        }
        columns.push((spec.column.clone(), column.finish()));
    }

    // Declared nullable explicitly, for the reason `build_batch` gives.
    RecordBatch::try_from_iter_with_nullable(
        columns.into_iter().map(|(name, array)| (name, array, true)),
    )
    .context("failed to build the export record batch")
}

/// One decoded resource attribute set per distinct stream, so a stream's
/// `stream_attrs` blob is decoded once rather than once per record.
fn decode_resources(
    records: &[LogRecord],
) -> anyhow::Result<HashMap<LogStreamId, Vec<(String, AttrValue)>>> {
    let mut by_stream: HashMap<LogStreamId, Vec<(String, AttrValue)>> = HashMap::new();
    for record in records {
        if by_stream.contains_key(&record.stream_id) {
            continue;
        }
        let decoded = decode_stream_attrs(&record.stream_attrs).map_err(|err| {
            anyhow::anyhow!(
                "failed to decode the stream attributes of stream {}: {err}",
                hex::encode(record.stream_id.0)
            )
        })?;
        by_stream.insert(record.stream_id, decoded.resource);
    }
    Ok(by_stream)
}

/// Refuses an `--parquet` path the final rename cannot replace, before the
/// export reads anything: a path under `/dev` (`/dev/stdout` included, since
/// a rename would replace the device node, or fail, rather than write to it),
/// and an existing path that is not a regular file once symlinks are
/// followed, such as a directory, which would otherwise fail only at the
/// rename after the whole window had been read and encoded.
fn check_output_path(out: &Path) -> anyhow::Result<()> {
    if out.starts_with("/dev") {
        anyhow::bail!(
            "--parquet {} is under /dev: the export replaces its output by renaming a \
             finished file over it, so it cannot write to a device; pass a regular file path",
            out.display()
        );
    }
    match std::fs::metadata(out) {
        Ok(meta) if meta.is_dir() => anyhow::bail!(
            "--parquet {} is a directory: pass the path of the Parquet file to write",
            out.display()
        ),
        Ok(meta) if !meta.is_file() => anyhow::bail!(
            "--parquet {} exists and is not a regular file: the export replaces its output \
             by renaming a finished file over it, so it can only replace a regular file",
            out.display()
        ),
        _ => Ok(()),
    }
}

/// Writes `records` to `out` in `EXPORT_BATCH_ROWS`-row batches through
/// [`write_output`] and returns the row count written.
fn write_parquet(
    mapping: &Mapping,
    records: &[LogRecord],
    resource_by_stream: &HashMap<LogStreamId, Vec<(String, AttrValue)>>,
    out: &Path,
) -> anyhow::Result<u64> {
    let empty = build_batch(mapping, &[])?;
    write_output(out, empty.schema(), |writer| {
        let mut rows_written = 0u64;
        for chunk in records.chunks(EXPORT_BATCH_ROWS) {
            let rows: Vec<ExportRow<'_>> = chunk
                .iter()
                .map(|record| ExportRow {
                    record,
                    resource: resource_by_stream
                        .get(&record.stream_id)
                        .map_or(&[][..], Vec::as_slice),
                })
                .collect();
            let batch = build_batch(mapping, &rows)?;
            writer
                .write(&batch)
                .with_context(|| format!("failed to write a batch to {}", out.display()))?;
            rows_written += batch.num_rows() as u64;
        }
        Ok(rows_written)
    })
}

/// Opens a Parquet writer of `schema` on a temporary file beside `out`, hands
/// it to `write_rows`, and moves the finished file into place; returns the row
/// count `write_rows` reports. An export that writes no batch still produces a
/// schema-only file, so a loader pointed at it reads zero rows rather than
/// failing to open it.
///
/// `out` is only ever replaced by a `rename` of a finished file. The rows are
/// written to a sibling temporary file named `.<file name>.<pid>.<n>.tmp` in
/// the same directory, synced to disk, and renamed over `out` after the
/// Parquet writer closes; the directory is synced after the rename, so the
/// replace survives a power loss. A failure part-way through (a stored
/// attribute whose type the mapping does not declare, a full disk) leaves any
/// pre-existing `out` exactly as it was instead of having truncated it into a
/// footer-less fragment. The temporary file is removed on every failure path
/// that returns, including a failed rename; a SIGINT or a panic mid-write
/// leaves it behind.
///
/// Because the replace is a rename rather than a write into the existing file:
/// a symlink at `out` is itself replaced by the new file, and the file it
/// pointed to is left unchanged; the new file's mode comes from the default
/// creation mode and the umask, not from the file it replaces, and the old
/// file's owner and ACLs are not carried over.
fn write_output(
    out: &Path,
    schema: SchemaRef,
    write_rows: impl FnOnce(&mut ArrowWriter<std::fs::File>) -> anyhow::Result<u64>,
) -> anyhow::Result<u64> {
    let (tmp_path, file) = create_temp_output(out)?;
    let written = file
        .try_clone()
        .context("failed to duplicate the temporary export file handle")
        .and_then(|writer_file| write_batches(writer_file, schema, write_rows, out))
        .and_then(|rows| {
            file.sync_all().with_context(|| {
                format!(
                    "failed to sync the temporary export file {}",
                    tmp_path.display()
                )
            })?;
            Ok(rows)
        });
    let rows_written = match written {
        Ok(rows) => rows,
        Err(err) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
    };
    if let Err(err) = std::fs::rename(&tmp_path, out) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(anyhow::Error::new(err).context(format!(
            "failed to move the finished export from {} into place at {}",
            tmp_path.display(),
            out.display()
        )));
    }
    std::fs::File::open(output_dir(out))
        .and_then(|dir| dir.sync_all())
        .with_context(|| {
            format!(
                "moved the finished export into place at {} but failed to sync its directory",
                out.display()
            )
        })?;
    Ok(rows_written)
}

/// The directory `out` is created in: its parent, or `.` for a bare file name.
fn output_dir(out: &Path) -> &Path {
    match out.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Creates the sibling temporary file [`write_parquet`] writes into, and
/// returns its path alongside the open handle.
///
/// The name is derived from `out` and made unique with the process id and an
/// attempt counter, and every attempt opens with `create_new`, so two exports
/// racing on one output directory never share a temporary file. Creating it
/// beside `out` rather than in the system temp directory is what keeps the
/// final step a rename within one filesystem, which is the atomic replace this
/// relies on.
fn create_temp_output(out: &Path) -> anyhow::Result<(std::path::PathBuf, std::fs::File)> {
    let name = out
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("{} names no output file", out.display()))?;
    let dir = output_dir(out);
    let pid = std::process::id();
    for attempt in 0..1024u32 {
        let mut candidate_name = std::ffi::OsString::from(".");
        candidate_name.push(name);
        candidate_name.push(format!(".{pid}.{attempt}.tmp"));
        let candidate = dir.join(candidate_name);
        match std::fs::File::options()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(anyhow::Error::new(err).context(format!(
                    "failed to create the temporary export file {}",
                    candidate.display()
                )));
            }
        }
    }
    anyhow::bail!(
        "failed to create a temporary export file beside {}: 1024 candidate names were all taken",
        out.display()
    )
}

/// Writes every batch into the already-open `file` and returns the row count.
/// `out` is the final destination, used only to name the file in error
/// messages: an operator reading one wants the path they passed, not the
/// temporary name it is on its way through.
fn write_batches(
    file: std::fs::File,
    schema: SchemaRef,
    write_rows: impl FnOnce(&mut ArrowWriter<std::fs::File>) -> anyhow::Result<u64>,
    out: &Path,
) -> anyhow::Result<u64> {
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .with_context(|| format!("failed to open a Parquet writer on {}", out.display()))?;
    let rows_written = write_rows(&mut writer)?;
    writer
        .close()
        .with_context(|| format!("failed to finish writing {}", out.display()))?;
    Ok(rows_written)
}

/// One output row: a decoded record plus its stream's resource attributes,
/// which live in the stream blob rather than on the record.
struct ExportRow<'a> {
    record: &'a LogRecord,
    resource: &'a [(String, AttrValue)],
}

/// Builds the output batch for `rows`, one column per field the mapping names.
///
/// The Arrow types are chosen to be exactly what the `--mapping` reader in
/// `load` accepts: `Int64` for the timestamp (already divided by the mapping's
/// unit) and for `i64` attributes, `Utf8` for the body, severity text and
/// `str` attributes, `FixedSizeBinary` of the id width for trace and span ids,
/// `Float64`/`Boolean`/`Binary` for the remaining attribute types.
fn build_batch(mapping: &Mapping, rows: &[ExportRow<'_>]) -> anyhow::Result<RecordBatch> {
    let mut columns: Vec<(String, ArrayRef)> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();

    let factor = mapping.ts_unit.factor();
    let mut ts = Int64Builder::with_capacity(rows.len());
    for row in rows {
        ts.append_value(row.record.ts_ns / factor);
    }
    columns.push((mapping.ts_column.clone(), Arc::new(ts.finish())));

    if let Some(name) = &mapping.body_column {
        let mut body = StringBuilder::new();
        for row in rows {
            body.append_value(&row.record.body);
        }
        columns.push((name.clone(), Arc::new(body.finish())));
    }

    if let Some(name) = &mapping.severity_number_column {
        let mut severity = Int64Builder::with_capacity(rows.len());
        for row in rows {
            severity.append_value(i64::from(row.record.severity_num));
        }
        columns.push((name.clone(), Arc::new(severity.finish())));
    }

    if let Some(name) = &mapping.severity_text_column {
        let mut severity = StringBuilder::new();
        for row in rows {
            severity.append_value(&row.record.severity_text);
        }
        columns.push((name.clone(), Arc::new(severity.finish())));
    }

    if let Some(name) = &mapping.trace_id_column {
        let mut ids = FixedSizeBinaryBuilder::new(16);
        for row in rows {
            append_id(&mut ids, row.record.trace_id.as_ref().map(|id| &id[..]))?;
        }
        columns.push((name.clone(), Arc::new(ids.finish())));
    }

    if let Some(name) = &mapping.span_id_column {
        let mut ids = FixedSizeBinaryBuilder::new(8);
        for row in rows {
            append_id(&mut ids, row.record.span_id.as_ref().map(|id| &id[..]))?;
        }
        columns.push((name.clone(), Arc::new(ids.finish())));
    }

    for spec in &mapping.resource_attributes {
        let mut column = AttrColumn::new(spec.value_type);
        for row in rows {
            column.push(&spec.key, find_attr(row.resource, &spec.key))?;
        }
        columns.push((spec.column.clone(), column.finish()));
    }

    for spec in &mapping.attributes {
        let mut column = AttrColumn::new(spec.value_type);
        for row in rows {
            column.push(&spec.key, find_attr(&row.record.attrs, &spec.key))?;
        }
        columns.push((spec.column.clone(), column.finish()));
    }

    if let Some(name) = &mapping.attrs_map_column {
        columns.push((name.clone(), build_attrs_map(mapping, rows)?));
    }

    for (name, _) in &columns {
        if !seen.insert(name.as_str()) {
            anyhow::bail!(
                "the mapping writes two different fields to the output column {name:?}; give \
                 each one its own column name"
            );
        }
    }

    // Every field is declared nullable explicitly. `RecordBatch::try_from_iter`
    // would infer nullability from each array's own null count, which differs
    // between batches of the same export (and is always "not nullable" for the
    // empty batch the writer takes its schema from), so a later batch's nulls
    // would be written into a column the file declares required and read back
    // as values.
    RecordBatch::try_from_iter_with_nullable(
        columns.into_iter().map(|(name, array)| (name, array, true)),
    )
    .context("failed to build the export record batch")
}

/// The `attrs_map_column` overflow column: every record attribute the mapping
/// does not give a typed column of its own, stringified the same way
/// `attrs['<key>']` stringifies a value for SQL.
fn build_attrs_map(mapping: &Mapping, rows: &[ExportRow<'_>]) -> anyhow::Result<ArrayRef> {
    let typed: HashSet<&str> = mapping
        .attributes
        .iter()
        .map(|spec| spec.key.as_str())
        .collect();
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for row in rows {
        for (key, value) in &row.record.attrs {
            if typed.contains(key.as_str()) {
                continue;
            }
            builder.keys().append_value(key);
            builder.values().append_value(attr_value_to_string(value));
        }
        builder
            .append(true)
            .context("failed to build the attrs map column")?;
    }
    Ok(Arc::new(builder.finish()))
}

fn append_id(builder: &mut FixedSizeBinaryBuilder, id: Option<&[u8]>) -> anyhow::Result<()> {
    match id {
        Some(bytes) => builder
            .append_value(bytes)
            .context("failed to append a trace or span id"),
        None => {
            builder.append_null();
            Ok(())
        }
    }
}

fn find_attr<'a>(attrs: &'a [(String, AttrValue)], key: &str) -> Option<&'a AttrValue> {
    attrs
        .iter()
        .find_map(|(k, v)| (k.as_str() == key).then_some(v))
}

/// The stored kind of an attribute value, for the mismatch message below.
fn attr_type_name(value: &AttrValue) -> &'static str {
    match value {
        AttrValue::Str(_) => "str",
        AttrValue::I64(_) => "i64",
        AttrValue::F64(_) => "f64",
        AttrValue::Bool(_) => "bool",
        AttrValue::Bytes(_) => "bytes",
        AttrValue::List(_) => "list",
        AttrValue::Map(_) => "map",
    }
}

/// One typed attribute output column, in the Arrow type the mapping's
/// declared [`ColType`] corresponds to.
///
/// A record that carries no value for the key gets a null (the loader omits a
/// null source cell rather than storing an attribute, so null is what a round
/// trip must produce). A record whose stored value has a different type than
/// the mapping declares is an error rather than a null: silently writing null
/// would drop a value the store holds, with nothing in the output to say so.
enum AttrColumn {
    Str(StringBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    Bool(BooleanBuilder),
    Bytes(BinaryBuilder),
}

impl AttrColumn {
    fn new(value_type: ColType) -> Self {
        match value_type {
            ColType::Str => AttrColumn::Str(StringBuilder::new()),
            ColType::I64 => AttrColumn::I64(Int64Builder::new()),
            ColType::F64 => AttrColumn::F64(Float64Builder::new()),
            ColType::Bool => AttrColumn::Bool(BooleanBuilder::new()),
            ColType::Bytes => AttrColumn::Bytes(BinaryBuilder::new()),
        }
    }

    fn declared(&self) -> &'static str {
        match self {
            AttrColumn::Str(_) => "str",
            AttrColumn::I64(_) => "i64",
            AttrColumn::F64(_) => "f64",
            AttrColumn::Bool(_) => "bool",
            AttrColumn::Bytes(_) => "bytes",
        }
    }

    fn append_null(&mut self) {
        match self {
            AttrColumn::Str(b) => b.append_null(),
            AttrColumn::I64(b) => b.append_null(),
            AttrColumn::F64(b) => b.append_null(),
            AttrColumn::Bool(b) => b.append_null(),
            AttrColumn::Bytes(b) => b.append_null(),
        }
    }

    fn push(&mut self, key: &str, value: Option<&AttrValue>) -> anyhow::Result<()> {
        let Some(value) = value else {
            self.append_null();
            return Ok(());
        };
        match (&mut *self, value) {
            (AttrColumn::Str(b), AttrValue::Str(v)) => b.append_value(v),
            (AttrColumn::I64(b), AttrValue::I64(v)) => b.append_value(*v),
            (AttrColumn::F64(b), AttrValue::F64(v)) => b.append_value(*v),
            (AttrColumn::Bool(b), AttrValue::Bool(v)) => b.append_value(*v),
            (AttrColumn::Bytes(b), AttrValue::Bytes(v)) => b.append_value(v),
            (column, found) => anyhow::bail!(
                "attribute {key:?} is declared {} in the mapping but the stored value is {}; \
                 fix the mapping's type for this key, or drop the column and let \
                 attrs_map_column carry it",
                column.declared(),
                attr_type_name(found),
            ),
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            AttrColumn::Str(b) => Arc::new(b.finish()),
            AttrColumn::I64(b) => Arc::new(b.finish()),
            AttrColumn::F64(b) => Arc::new(b.finish()),
            AttrColumn::Bool(b) => Arc::new(b.finish()),
            AttrColumn::Bytes(b) => Arc::new(b.finish()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use arrow::array::{
        Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, MapArray,
        StringArray,
    };
    use ravel_logseg::stream_attrs_bytes;

    use super::*;

    fn record(ts_ns: i64, attrs: Vec<(String, AttrValue)>) -> LogRecord {
        LogRecord {
            stream_id: LogStreamId([0u8; 16]),
            stream_attrs: stream_attrs_bytes(&[], "", "", &[]),
            ts_ns,
            observed_ts_ns: ts_ns,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "body".to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        }
    }

    fn mapping(text: &str) -> Mapping {
        crate::load::parse_mapping(text).expect("valid mapping")
    }

    fn batch_of(mapping: &Mapping, records: &[LogRecord]) -> RecordBatch {
        let resource: Vec<(String, AttrValue)> = Vec::new();
        let rows: Vec<ExportRow<'_>> = records
            .iter()
            .map(|record| ExportRow {
                record,
                resource: &resource,
            })
            .collect();
        build_batch(mapping, &rows).expect("batch builds")
    }

    #[test]
    fn every_signal_is_exported() {
        for signal in [SignalArg::Logs, SignalArg::Metrics, SignalArg::Spans] {
            assert_eq!(unsupported_signal_message(signal), None, "{signal:?}");
        }
    }

    /// The window is half-open at both spellings of its end: the last
    /// exportable nanosecond is `end_ns - 1`, and the fetch is asked for
    /// exactly that as its inclusive bound.
    #[test]
    fn the_window_end_is_exclusive_at_the_nanosecond() {
        let start_ns = 1_700_000_000_000_000_000;
        let end_ns = start_ns + 1_000;

        assert!(in_export_window(start_ns, start_ns, end_ns));
        assert!(in_export_window(end_ns - 1, start_ns, end_ns));
        assert!(!in_export_window(end_ns, start_ns, end_ns));
        assert!(!in_export_window(start_ns - 1, start_ns, end_ns));

        assert_eq!(fetch_range_end_ns(end_ns), end_ns - 1);
        assert_eq!(fetch_range_end_ns(i64::MIN), i64::MIN);
    }

    #[test]
    fn ts_column_is_written_in_the_mappings_declared_unit() {
        let m = mapping("ts_column = \"ts\"\nts_unit = \"millis\"\n");
        let batch = batch_of(&m, &[record(1_700_000_000_123_000_000, Vec::new())]);
        let ts = batch
            .column_by_name("ts")
            .expect("ts")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ts is Int64");
        assert_eq!(ts.values(), &[1_700_000_000_123]);
    }

    #[test]
    fn a_typed_attribute_column_is_null_exactly_when_the_record_lacks_the_key() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[attribute]]\nkey = \"count\"\ncolumn = \"count_col\"\ntype = \"i64\"\n",
        );
        let batch = batch_of(
            &m,
            &[
                record(1, vec![("count".to_string(), AttrValue::I64(7))]),
                record(2, Vec::new()),
            ],
        );
        let count = batch
            .column_by_name("count_col")
            .expect("count_col")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count_col is Int64");
        assert_eq!(count.len(), 2);
        assert!(!count.is_null(0));
        assert_eq!(count.value(0), 7);
        assert!(count.is_null(1));
    }

    #[test]
    fn every_declared_attribute_type_gets_its_matching_arrow_type() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[attribute]]\nkey = \"s\"\ncolumn = \"s_col\"\ntype = \"str\"\n\n\
             [[attribute]]\nkey = \"f\"\ncolumn = \"f_col\"\ntype = \"f64\"\n\n\
             [[attribute]]\nkey = \"b\"\ncolumn = \"b_col\"\ntype = \"bool\"\n\n\
             [[attribute]]\nkey = \"y\"\ncolumn = \"y_col\"\ntype = \"bytes\"\n",
        );
        let batch = batch_of(
            &m,
            &[record(
                1,
                vec![
                    ("s".to_string(), AttrValue::Str("v".to_string())),
                    ("f".to_string(), AttrValue::F64(-0.5)),
                    ("b".to_string(), AttrValue::Bool(true)),
                    ("y".to_string(), AttrValue::Bytes(vec![1, 2])),
                ],
            )],
        );
        assert_eq!(
            batch
                .column_by_name("s_col")
                .expect("s_col")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("s_col is Utf8")
                .value(0),
            "v"
        );
        assert_eq!(
            batch
                .column_by_name("f_col")
                .expect("f_col")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("f_col is Float64")
                .values(),
            &[-0.5]
        );
        assert!(
            batch
                .column_by_name("b_col")
                .expect("b_col")
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("b_col is Boolean")
                .value(0)
        );
        assert_eq!(
            batch
                .column_by_name("y_col")
                .expect("y_col")
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("y_col is Binary")
                .value(0),
            &[1u8, 2][..]
        );
    }

    #[test]
    fn a_stored_type_the_mapping_does_not_declare_is_refused_by_name() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[attribute]]\nkey = \"count\"\ncolumn = \"count_col\"\ntype = \"i64\"\n",
        );
        let records = [record(
            1,
            vec![("count".to_string(), AttrValue::Str("seven".to_string()))],
        )];
        let resource: Vec<(String, AttrValue)> = Vec::new();
        let rows: Vec<ExportRow<'_>> = records
            .iter()
            .map(|record| ExportRow {
                record,
                resource: &resource,
            })
            .collect();
        let err = build_batch(&m, &rows).expect_err("a type mismatch is refused");
        assert_eq!(
            err.to_string(),
            "attribute \"count\" is declared i64 in the mapping but the stored value is str; fix \
             the mapping's type for this key, or drop the column and let attrs_map_column carry it"
        );
    }

    #[test]
    fn attrs_map_column_carries_exactly_the_attributes_no_typed_column_names() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\nattrs_map_column = \"rest\"\n\n\
             [[attribute]]\nkey = \"count\"\ncolumn = \"count_col\"\ntype = \"i64\"\n",
        );
        let batch = batch_of(
            &m,
            &[record(
                1,
                vec![
                    ("count".to_string(), AttrValue::I64(7)),
                    ("note".to_string(), AttrValue::Str("hi".to_string())),
                    ("ratio".to_string(), AttrValue::F64(1.5)),
                ],
            )],
        );
        let rest = batch
            .column_by_name("rest")
            .expect("rest")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("rest is a Map")
            .value(0);
        let keys = rest
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map keys are Utf8");
        let values = rest
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map values are Utf8");
        let pairs: Vec<(&str, &str)> = (0..keys.len())
            .map(|i| (keys.value(i), values.value(i)))
            .collect();
        assert_eq!(pairs, vec![("note", "hi"), ("ratio", "1.5")]);
    }

    #[test]
    fn two_mapping_fields_sharing_one_output_column_are_refused() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"same\"\n\n\
             [[attribute]]\nkey = \"count\"\ncolumn = \"same\"\ntype = \"i64\"\n",
        );
        let err = build_batch(&m, &[]).expect_err("a duplicate output column is refused");
        assert_eq!(
            err.to_string(),
            "the mapping writes two different fields to the output column \"same\"; give each \
             one its own column name"
        );
    }

    #[test]
    fn trace_and_span_ids_round_trip_as_fixed_width_or_null() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\
             trace_id_column = \"trace_id\"\nspan_id_column = \"span_id\"\n",
        );
        let mut with_ids = record(1, Vec::new());
        with_ids.trace_id = Some([0xAB; 16]);
        with_ids.span_id = Some([0xCD; 8]);
        let batch = batch_of(&m, &[with_ids, record(2, Vec::new())]);
        let trace = batch
            .column_by_name("trace_id")
            .expect("trace_id")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("trace_id is FixedSizeBinary");
        assert_eq!(trace.value(0), [0xAB; 16]);
        assert!(trace.is_null(1));
        let span = batch
            .column_by_name("span_id")
            .expect("span_id")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("span_id is FixedSizeBinary");
        assert_eq!(span.value(0), [0xCD; 8]);
        assert!(span.is_null(1));
    }

    /// A series added twice under one kind is one offender: the count is
    /// keyed by the series' output sort key, not by the number of calls.
    #[test]
    fn one_series_added_twice_under_one_kind_counts_once() {
        let key: SeriesSortKey = vec![(METRIC_NAME_LABEL.to_string(), "cpu".to_string())];
        let mut refusals = SeriesRefusals::default();
        refusals.add(Refusal::UnmappedLabel, &key, "series cpu{}".to_string());
        refusals.add(Refusal::UnmappedLabel, &key, "series cpu{}".to_string());
        let err = refusals.into_result().expect_err("the series is refused");
        assert_eq!(
            err.to_string(),
            "export --signal metrics refused on 1 series for this reason; first: series cpu{}"
        );
    }

    /// One scalar run of series `cpu` holding a single sample.
    fn one_sample_run(
        ts_ns: i64,
        value: f64,
        writer_epoch: u64,
        writer_seq: u64,
    ) -> FetchedSeriesSoa {
        FetchedSeriesSoa {
            series_id: SeriesId([7; 16]),
            labels: LabelSet::new(vec![ravel_types::Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: "cpu".to_string(),
            }])
            .expect("label set"),
            timestamps: vec![ts_ns],
            values: vec![value],
            created_unix_ns: 5,
            writer_epoch,
            writer_seq,
            per_sample_priorities: None,
        }
    }

    /// Two duplicates at one ts that differ only in writer_epoch and
    /// writer_seq: the higher epoch wins although its seq is lower, so a key
    /// that transposed the two fields would keep the other value.
    #[test]
    fn a_higher_writer_epoch_wins_over_a_higher_writer_seq() {
        for runs in [[(1, 9, 1.0), (2, 3, 2.0)], [(2, 3, 2.0), (1, 9, 1.0)]] {
            let mut by_series = HashMap::new();
            for (epoch, seq, value) in runs {
                collect_run(
                    &mut by_series,
                    one_sample_run(100, value, epoch, seq),
                    0,
                    200,
                )
                .expect("run collects");
            }
            let candidates = by_series.remove(&SeriesId([7; 16])).expect("one series");
            let (samples, dropped) = resolve_duplicates(candidates.samples);
            assert_eq!(dropped, 1);
            assert_eq!(
                samples
                    .iter()
                    .map(|(ts, v)| (*ts, v.to_bits()))
                    .collect::<Vec<_>>(),
                vec![(100, 2.0f64.to_bits())],
                "the epoch-2 write is served"
            );
        }
    }

    /// A `[spans]` mapping with `start_ts` in `start_unit`, `end_ts` in nanos,
    /// and `http.status_code` declared i64.
    fn spans_mapping(start_unit: &str) -> SpansMapping {
        crate::load::parse_spans_mapping(&format!(
            "[spans]\ntrace_id_column = \"trace_id\"\nspan_id_column = \"span_id\"\n\
             name_column = \"name\"\nstart_ts_column = \"start\"\n\
             start_ts_unit = \"{start_unit}\"\nend_ts_column = \"end\"\n\
             end_ts_unit = \"nanos\"\n\n\
             [[spans.attribute]]\nkey = \"http.status_code\"\ncolumn = \"http_status\"\n\
             type = \"i64\"\n"
        ))
        .expect("valid spans mapping")
    }

    fn span(name: &str, start_ts_ns: i64, end_ts_ns: i64) -> SpanRecord {
        SpanRecord {
            trace_id: [1; 16],
            span_id: [0x11; 8],
            parent_span_id: None,
            name: name.to_string(),
            start_ts_ns,
            end_ts_ns,
            status_code: ravel_rspan::StatusCode::Unset,
            status_message: None,
            attrs: Vec::new(),
        }
    }

    fn span_refusal(mapping: &SpansMapping, span: SpanRecord) -> String {
        match span_output_rows(mapping, &[span]) {
            Ok(_) => panic!("the span is refused"),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn a_span_starting_at_zero_is_refused_as_unloadable() {
        assert_eq!(
            span_refusal(&spans_mapping("nanos"), span("zero start", 0, 5)),
            "export --signal spans refused on 1 spans for this reason; first: span \"zero \
             start\" (trace_id 01010101010101010101010101010101, span_id 1111111111111111) \
             starts at 0 ns and ends at 5 ns; a load reads a zero start as load time and refuses \
             a negative start or an end before the start, so the exported file would not re-load \
             as the same span"
        );
    }

    #[test]
    fn a_span_ending_before_its_start_is_refused_as_unloadable() {
        assert_eq!(
            span_refusal(&spans_mapping("nanos"), span("backwards", 10, 9)),
            "export --signal spans refused on 1 spans for this reason; first: span \
             \"backwards\" (trace_id 01010101010101010101010101010101, span_id \
             1111111111111111) starts at 10 ns and ends at 9 ns; a load reads a zero start as \
             load time and refuses a negative start or an end before the start, so the exported \
             file would not re-load as the same span"
        );
    }

    /// One span both finer than its `start_ts_unit` and carrying an attribute
    /// its declared type does not read back is refused for the timestamp.
    #[test]
    fn a_sub_unit_timestamp_is_reported_before_an_unwritable_attribute() {
        let mut both = span("both", 1_000_000_001, 2_000_000_000);
        both.attrs = vec![("http.status_code".to_string(), "007".to_string())];
        assert_eq!(
            span_refusal(&spans_mapping("millis"), both),
            "export --signal spans refused on 1 spans for this reason; first: span \"both\" \
             (trace_id 01010101010101010101010101010101, span_id 1111111111111111) has start_ts \
             1000000001 ns, which is not a whole number of millis (the mapping's start_ts_unit); \
             writing it in millis would move it onto a different timestamp. Export with a finer \
             start_ts_unit."
        );
    }
}
