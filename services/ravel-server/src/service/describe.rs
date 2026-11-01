//! The engine side of the MCP tool `ravel_describe_data` (ADR-1374 decision
//! 2): one signal's columns, indexed keys, metric families, ingest watermark
//! and event-time coverage, read through the same controls as every other
//! operation in this module.
//!
//! The fixed columns are the SQL table's own schema, so this crate needs the
//! `sql` feature for them; the MCP adapter, the only caller, implies it.

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, MetricKind};
use ravel_query::QueryError;
use ravel_query::http::{LiveUsage, MetadataCache};
use ravel_sql::{DeclaredColumnSource, KeyIndexDeclaration};
use ravel_types::accounting::{QueryAccounting, QueryAccountingSnapshot};
use ravel_types::{Signal, TenantHash, TimeRange};

use super::{LiveCost, QueryService, ServiceError, core, finish_usage, query_span};

/// The most metric families one page carries, and the page size of a request
/// that names none.
pub const MAX_FAMILIES_PER_PAGE: usize = 100;

/// Why a non-metrics signal's family page is empty.
pub const FAMILIES_METRICS_ONLY: &str = "metric families are recorded for the metrics signal only";

/// The `logs` column a tenant's `index_trace_id` declaration indexes.
const LOGS_TRACE_ID: &str = "trace_id";

/// The `audit` language of a describe call.
const AUDIT_LANGUAGE: &str = "describe_data";

/// What [`QueryService::describe_data`] reads, shared with the routes that
/// serve the same data: the process's one catalog, its one metric-metadata
/// cache (the one `/api/v1/metadata` serves from), and the declared-column
/// source the SQL executor plans with.
#[derive(Clone)]
pub struct DescribeState {
    pub catalog: Arc<Catalog>,
    pub metadata_cache: Arc<MetadataCache>,
    pub declared_columns: Arc<dyn DeclaredColumnSource>,
    /// The ceiling a request's deadline is lowered to.
    pub max_deadline: Duration,
}

/// A signal `describe_data` serves, by the name the MCP surface uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeSignal {
    Metrics,
    Logs,
    Traces,
}

impl DescribeSignal {
    /// The signal named `name`; any other name is
    /// [`super::ServiceErrorKind::InvalidArgument`].
    pub fn parse(name: &str) -> Result<Self, ServiceError> {
        match name {
            "metrics" => Ok(DescribeSignal::Metrics),
            "logs" => Ok(DescribeSignal::Logs),
            "traces" => Ok(DescribeSignal::Traces),
            other => Err(ServiceError::invalid_argument(format!(
                "unknown signal {other:?}: expected \"metrics\", \"logs\" or \"traces\""
            ))),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            DescribeSignal::Metrics => "metrics",
            DescribeSignal::Logs => "logs",
            DescribeSignal::Traces => "traces",
        }
    }

    /// The SQL table this signal is queried through.
    pub fn table(self) -> &'static str {
        match self {
            DescribeSignal::Metrics => "samples",
            DescribeSignal::Logs => "logs",
            DescribeSignal::Traces => "spans",
        }
    }

    fn catalog_signal(self) -> Signal {
        match self {
            DescribeSignal::Metrics => Signal::Metrics,
            DescribeSignal::Logs => Signal::Logs,
            DescribeSignal::Traces => Signal::Spans,
        }
    }
}

/// One `describe_data` call, already authenticated.
#[derive(Debug, Clone)]
pub struct DescribeDataRequest {
    /// `metrics`, `logs` or `traces`.
    pub signal: String,
    /// The first family of the page, in [`DescribeData::families`] order.
    pub offset: usize,
    /// The page size; `None` and anything above [`MAX_FAMILIES_PER_PAGE`] are
    /// [`MAX_FAMILIES_PER_PAGE`].
    pub limit: Option<usize>,
    /// The resolve instant. A later page passes the instant its first page
    /// resolved at, so every page describes one snapshot.
    pub now_ns: i64,
    pub deadline: Duration,
}

/// One column of the signal's SQL table, as the table's Arrow schema states it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeColumn {
    pub name: String,
    /// The Arrow type's display form, e.g. `Int64`.
    pub data_type: String,
    pub nullable: bool,
}

/// One metric family from the tenant's metric metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeFamily {
    pub name: String,
    pub kind: MetricKind,
    /// Empty when the protocol supplied none.
    pub unit: String,
    /// Empty when the protocol supplied none.
    pub help: String,
}

/// Where [`IndexedKeys::keys`] comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexedKeysSource {
    /// Logs: the tenant's KEY_IDX declaration in its `TenantConfig` (the
    /// declared `key_index` columns, then `trace_id` when `index_trace_id` is
    /// set), as the SQL executor's declared-column source resolves it.
    TenantKeyIndex,
    /// Traces: `trace_id`, the one field the RSPAN format's KEY_IDX indexes
    /// for every tenant.
    SpanFormat,
    /// Metrics: none. The `samples` table has no KEY_IDX; a series is found
    /// through its label postings, which cover every label.
    None,
}

/// The keys a point lookup on this signal can probe instead of scanning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedKeys {
    pub keys: Vec<String>,
    pub source: IndexedKeysSource,
}

/// The event-time span of the resolved segments, from their commit records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventTimeCoverage {
    pub min_event_ts_ns: i64,
    pub max_event_ts_ns: i64,
}

/// What [`QueryService::describe_data`] returns.
#[derive(Debug, Clone)]
pub struct DescribeData {
    pub signal: DescribeSignal,
    /// The SQL table the columns below belong to.
    pub table: &'static str,
    /// The table's fixed columns, in schema order.
    pub fixed_columns: Vec<DescribeColumn>,
    /// The tenant's declared typed attribute columns, in declaration order,
    /// which is the order they follow the fixed columns in. Logs only; empty
    /// for the other signals and for a tenant that declares none.
    pub declared_columns: Vec<DescribeColumn>,
    pub indexed_keys: IndexedKeys,
    /// One page of families, ordered by name in byte order. Empty for a
    /// non-metrics signal.
    pub families: Vec<DescribeFamily>,
    /// Every family the record holds, not only this page's.
    pub families_total: usize,
    /// Why the family page is empty by construction; `None` for metrics.
    pub families_absent_reason: Option<&'static str>,
    /// The greatest ingest-hour bucket among the resolved segments, in unix
    /// hours; `None` when the resolve found no segment.
    pub ingest_watermark_hour: Option<u32>,
    /// `None` when the resolve found no segment.
    pub coverage: Option<EventTimeCoverage>,
    /// Segments the resolve returned.
    pub segments: usize,
    /// The sum of the resolved segments' commit-record row counts
    /// (`sample_count`: samples, log records or spans). Exact for what was
    /// committed, counted before cross-segment duplicates collapse and before
    /// a pending erasure applies, so a scan can return fewer.
    pub committed_rows: u64,
    /// The instant the snapshot was resolved at, the request's `now_ns`.
    pub resolved_at_ns: i64,
    /// What the call spent: the resolve's requests and bytes. The metadata
    /// record and the tenant configuration are read through process-wide
    /// caches whose fills no request is charged for.
    pub accounting: QueryAccountingSnapshot,
}

impl QueryService {
    pub fn with_describe(mut self, state: DescribeState) -> Self {
        self.describe = Some(state);
        self
    }

    /// One signal's shape and freshness for `ravel_describe_data`, under one
    /// admission permit and one catalog resolve of the whole tenant at
    /// `request.now_ns`.
    ///
    /// The ingest watermark is the greatest `ingest_hour_bucket` over the
    /// segments that resolve returned, and is absent when it returned none.
    /// It is not the fold watermark: that one is a cost boundary that trails
    /// ingest by the seal margin, costs a HEAD read of its own, and is not a
    /// freshness signal (ADR-1374, 2026-09-13 amendment, items 4 and 8).
    ///
    /// An unknown signal is refused before the permit is taken, and a process
    /// built without [`DescribeState`] refuses every call as unsupported.
    pub async fn describe_data(
        &self,
        tenant_hash: TenantHash,
        request: &DescribeDataRequest,
    ) -> Result<DescribeData, ServiceError> {
        let state = self
            .describe
            .as_ref()
            .ok_or_else(|| ServiceError::unsupported_surface("describe_data"))?;
        let signal = DescribeSignal::parse(&request.signal)?;
        let limit = request.limit.map_or(MAX_FAMILIES_PER_PAGE, |limit| {
            limit.min(MAX_FAMILIES_PER_PAGE)
        });

        let _permit = self.controls.admit()?;
        let deadline = self
            .controls
            .clamp_deadline(request.deadline, state.max_deadline);
        let accounting = QueryAccounting::new();
        let live = LiveCost::new();
        live.install(&accounting);
        let guard = self
            .controls
            .usage_guard(tenant_hash, Arc::new(live.clone()));

        let span = query_span("describe_data", tenant_hash);
        let run = describe(
            state,
            tenant_hash,
            signal,
            request.offset,
            limit,
            request.now_ns,
            &accounting,
        );
        let result = tokio::time::timeout(deadline, tracing::Instrument::instrument(run, span))
            .await
            .unwrap_or(Err(ServiceError::from_query(
                QueryError::DeadlineExceeded { deadline },
            )));

        let status = finish_usage(guard, &result, |_| (live.snapshot(), core::zero_estimate()));

        let text = format!(
            "describe_data signal={} offset={} limit={limit}",
            signal.name(),
            request.offset
        );
        self.controls
            .audit(
                tenant_hash,
                request.now_ns,
                &text,
                AUDIT_LANGUAGE,
                (0, request.now_ns),
                status,
                self.audit_subject.as_ref(),
            )
            .await?;
        let data = result?;

        self.controls
            .record_cost(tenant_hash, &data.accounting, &core::zero_estimate());
        Ok(data)
    }
}

async fn describe(
    state: &DescribeState,
    tenant_hash: TenantHash,
    signal: DescribeSignal,
    offset: usize,
    limit: usize,
    now_ns: i64,
    accounting: &QueryAccounting,
) -> Result<DescribeData, ServiceError> {
    let snapshot = state
        .catalog
        .resolve_with_accounting(
            &tenant_hash,
            signal.catalog_signal(),
            TimeRange {
                start_ns: 0,
                end_ns: i64::MAX,
            },
            &[],
            now_ns,
            accounting,
        )
        .await
        .map_err(|err| ServiceError::from_query(QueryError::from(err)))?;

    let segments = &snapshot.segments;
    let ingest_watermark_hour = segments.iter().map(|s| s.ingest_hour_bucket).max();
    let coverage = segments
        .iter()
        .map(|s| s.min_event_ts_ns)
        .min()
        .zip(segments.iter().map(|s| s.max_event_ts_ns).max())
        .map(|(min_event_ts_ns, max_event_ts_ns)| EventTimeCoverage {
            min_event_ts_ns,
            max_event_ts_ns,
        });
    let committed_rows = segments
        .iter()
        .fold(0u64, |rows, s| rows.saturating_add(s.sample_count));

    let (families, families_total, families_absent_reason) = match signal {
        DescribeSignal::Metrics => {
            let record = state.metadata_cache.get(tenant_hash).await;
            let mut entries: Vec<_> = record.iter().collect();
            entries.sort_by(|a, b| a.family_name.cmp(&b.family_name));
            let page = entries
                .iter()
                .skip(offset)
                .take(limit)
                .map(|entry| DescribeFamily {
                    name: entry.family_name.clone(),
                    kind: entry.kind,
                    unit: entry.unit.clone(),
                    help: entry.help.clone(),
                })
                .collect();
            (page, entries.len(), None)
        }
        DescribeSignal::Logs | DescribeSignal::Traces => {
            (Vec::new(), 0, Some(FAMILIES_METRICS_ONLY))
        }
    };

    let (declared_columns, indexed_keys) = match signal {
        DescribeSignal::Logs => {
            let declared = state
                .declared_columns
                .declared_columns(tenant_hash, now_ns)
                .await;
            let key_index = state.declared_columns.key_index(tenant_hash, now_ns).await;
            let declared = ravel_sql::logs_schema_with_declared(&declared)
                .fields()
                .iter()
                .skip(ravel_sql::FIRST_DECLARED_COL)
                .map(|field| column(field.name(), field.data_type(), field.is_nullable()))
                .collect();
            (declared, tenant_key_index(key_index))
        }
        DescribeSignal::Traces => (
            Vec::new(),
            IndexedKeys {
                keys: vec![ravel_rspan::key_index::TRACE_ID_FIELD.to_string()],
                source: IndexedKeysSource::SpanFormat,
            },
        ),
        DescribeSignal::Metrics => (
            Vec::new(),
            IndexedKeys {
                keys: Vec::new(),
                source: IndexedKeysSource::None,
            },
        ),
    };

    Ok(DescribeData {
        signal,
        table: signal.table(),
        fixed_columns: fixed_columns(signal),
        declared_columns,
        indexed_keys,
        families,
        families_total,
        families_absent_reason,
        ingest_watermark_hour,
        coverage,
        segments: segments.len(),
        committed_rows,
        resolved_at_ns: now_ns,
        accounting: accounting.snapshot(),
    })
}

fn fixed_columns(signal: DescribeSignal) -> Vec<DescribeColumn> {
    let schema = match signal {
        DescribeSignal::Metrics => ravel_sql::public_schema(),
        DescribeSignal::Logs => ravel_sql::logs_schema(),
        DescribeSignal::Traces => ravel_sql::spans_schema(),
    };
    schema
        .fields()
        .iter()
        .map(|field| column(field.name(), field.data_type(), field.is_nullable()))
        .collect()
}

fn column(name: &str, data_type: &impl std::fmt::Display, nullable: bool) -> DescribeColumn {
    DescribeColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable,
    }
}

fn tenant_key_index(declaration: KeyIndexDeclaration) -> IndexedKeys {
    let mut keys = declaration.columns;
    if declaration.trace_id {
        keys.push(LOGS_TRACE_ID.to_string());
    }
    IndexedKeys {
        keys,
        source: IndexedKeysSource::TenantKeyIndex,
    }
}
