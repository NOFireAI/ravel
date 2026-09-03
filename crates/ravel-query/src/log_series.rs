//! PromQL over logs (ADR-1103): answers a log selector -- a vector selector
//! whose `__name__` matcher equals one of the two reserved metric names --
//! from the logs signal, with one sample per matching log record.
//!
//! Nothing in production calls [`fetch_log_series`] or [`log_metric_of`]
//! yet: task #1108 wires them into `QueryEngine::prefetch` and
//! `resolve_series_inner`. This module is self-contained and has no
//! dependency on `engine.rs`.
//!
//! # Plan-phase stream discovery
//!
//! ADR-1103 decision 2's cost model -- stream-label-set discovery "costs one
//! STREAM_DIR decode per candidate object, the same cost
//! [`LogSegmentFetcher::plan_segment`] already pays" -- is implemented
//! exactly, not approximated: [`LogSegmentFetcher::fetch_stream_dir`] reads
//! the footer plus the STREAM_DIR section only, moving no BLOCKS byte. This
//! module decodes each entry's stream-attrs blob once (cached by
//! [`LogStreamId`] across segments, so a stream repeated in a later segment
//! costs no further decode) and evaluates the selector's stream-level
//! matchers against the resulting label set, building an exact
//! `Predicate::StreamIn` naming every matching stream. A segment with at
//! least one matching stream is then read exactly once, under Scan, with the
//! projected column set the selector needs; a segment with none is pruned
//! before any Scan-phase GET. No segment is read twice.

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Instant;

use ravel_catalog::SegmentRef;
use ravel_logseg::columns::ColumnSelection;
use ravel_logseg::error::LogSegError;
use ravel_logseg::record::{
    FieldSel, Predicate, StreamAttrs, attr_value_to_string, decode_stream_attrs,
};
use ravel_promql::{LabelMatcher, MatchOp, SeriesData};
use ravel_types::logstream::{AttrValue, LogStreamId};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, TenantHash, TimeRange};

use crate::erasure::ErasurePredicate;
use crate::phase_accounting::PhaseAccounting;
use crate::{ByteLimit, LogFetchError, LogQuery, LogSegmentFetcher};

/// Reserved metric name for the log-lines series (ADR-1103 decision 1): one
/// sample per matching log record, value `1.0`.
pub const LOG_LINES_METRIC: &str = "ravel_log_lines";
/// Reserved metric name for the log-bytes series (ADR-1103 decision 1): one
/// sample per matching log record, value the record body's length in bytes.
pub const LOG_BYTES_METRIC: &str = "ravel_log_bytes";
/// Matcher-only pseudo-label for the record body (ADR-1103 decision 3).
/// Never appears in a returned label set and is never part of series
/// identity.
pub const BODY_MATCHER_LABEL: &str = "__body__";
/// The one per-record field that becomes a label (ADR-1103 decision 2 step 5).
pub const SEVERITY_LABEL: &str = "severity_text";

/// Which reserved log metric a selector named, and what its samples mean.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogMetric {
    /// `ravel_log_lines`: one sample per matching record, value `1.0`.
    Lines,
    /// `ravel_log_bytes`: one sample per matching record, value the record
    /// body's length in bytes.
    Bytes,
}

impl LogMetric {
    /// This metric's reserved name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            LogMetric::Lines => LOG_LINES_METRIC,
            LogMetric::Bytes => LOG_BYTES_METRIC,
        }
    }

    /// Whether this metric's samples need the record body decoded. `Bytes`
    /// needs the body's length; `Lines` does not, unless a `__body__`
    /// matcher separately requires it (checked by the caller).
    #[must_use]
    pub fn needs_body(self) -> bool {
        matches!(self, LogMetric::Bytes)
    }
}

/// ADR-1103 decision 3: `Some` iff `matchers` holds a `__name__` matcher with
/// [`MatchOp::Eq`] whose value is a reserved log metric name. A regex or `!=`
/// on `__name__`, or no `__name__` matcher at all, is `None`: `{job="api"}`
/// keeps its Prometheus meaning over metrics, and
/// `{__name__=~"ravel_log.*"}` matches metrics only.
#[must_use]
pub fn log_metric_of(matchers: &[LabelMatcher]) -> Option<LogMetric> {
    let name_matcher = matchers.iter().find(|m| m.name == METRIC_NAME_LABEL)?;
    match &name_matcher.op {
        MatchOp::Eq if name_matcher.value == LOG_LINES_METRIC => Some(LogMetric::Lines),
        MatchOp::Eq if name_matcher.value == LOG_BYTES_METRIC => Some(LogMetric::Bytes),
        _ => None,
    }
}

fn is_label_name_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_label_name_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// The metrics label-name rule (ADR-1103 decision 2 step 4, mirroring
/// `ravel_otlp::normalize::sanitize_label_name`): the first character must
/// match `[A-Za-z_]` and every later character `[A-Za-z0-9_]`; a disallowed
/// character is rewritten to `_` in place. Borrows `name` unchanged when it
/// is already clean.
#[must_use]
pub fn sanitize_label_name(name: &str) -> Cow<'_, str> {
    let mut chars = name.chars();
    let clean = match chars.next() {
        None => true,
        Some(c) => is_label_name_start(c) && chars.as_str().chars().all(is_label_name_continue),
    };
    if clean {
        return Cow::Borrowed(name);
    }
    let mut out = String::with_capacity(name.len());
    for (i, c) in name.chars().enumerate() {
        let ok = if i == 0 {
            is_label_name_start(c)
        } else {
            is_label_name_continue(c)
        };
        out.push(if ok { c } else { '_' });
    }
    Cow::Owned(out)
}

/// Pushes one first-writer-wins label (ADR-1103 decision 2): a candidate with
/// an empty value is dropped (treated as absent, matching
/// `ravel_otlp::normalize::push_checked`'s convention), and a name already
/// written by an earlier step -- or one that sanitizes to `severity_text` or
/// begins with `__` -- is dropped rather than overwriting.
fn push_first_writer_wins(
    labels: &mut Vec<Label>,
    seen: &mut std::collections::HashSet<String>,
    name: String,
    value: String,
) {
    if value.is_empty() || name.starts_with("__") || name == SEVERITY_LABEL {
        return;
    }
    if seen.insert(name.clone()) {
        labels.push(Label { name, value });
    }
}

/// ADR-1103 decision 2 steps 1-4: the label set built from a stream's
/// identity alone, with no `severity_text` (that is decision 2 step 5,
/// [`series_label_set`], since severity is per record, not per stream).
#[must_use]
pub fn stream_label_set(metric: LogMetric, stream: &StreamAttrs) -> LabelSet {
    let mut seen = std::collections::HashSet::new();
    let mut labels = Vec::new();

    // Step 1: __name__. Pushed directly, not through
    // `push_first_writer_wins`: that helper drops every `__`-prefixed name
    // to stop a resource attribute from spoofing a reserved label, but
    // `__name__` is the one dunder label this function is required to set.
    seen.insert(METRIC_NAME_LABEL.to_string());
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.name().to_string(),
    });

    // Step 2: job/instance, exactly as the metrics ingest path derives them.
    // The three source attributes never also appear under their own names.
    let mut namespace = None;
    let mut service_name = None;
    let mut instance_id = None;
    for (k, v) in &stream.resource {
        match k.as_str() {
            "service.namespace" => namespace = Some(attr_value_to_string(v)),
            "service.name" => service_name = Some(attr_value_to_string(v)),
            "service.instance.id" => instance_id = Some(attr_value_to_string(v)),
            _ => {}
        }
    }
    if let Some(name) = service_name {
        let job = match namespace.as_deref() {
            Some(ns) if !ns.is_empty() => format!("{ns}/{name}"),
            _ => name,
        };
        push_first_writer_wins(&mut labels, &mut seen, "job".to_string(), job);
    }
    if let Some(id) = instance_id {
        push_first_writer_wins(&mut labels, &mut seen, "instance".to_string(), id);
    }

    // Step 3: OpenTelemetry-to-Prometheus scope compatibility names.
    if !stream.scope_name.is_empty() {
        push_first_writer_wins(
            &mut labels,
            &mut seen,
            "otel_scope_name".to_string(),
            stream.scope_name.clone(),
        );
    }
    if !stream.scope_version.is_empty() {
        push_first_writer_wins(
            &mut labels,
            &mut seen,
            "otel_scope_version".to_string(),
            stream.scope_version.clone(),
        );
    }
    let mut scope_attrs = stream.scope_attrs.clone();
    scope_attrs.sort_by(|a, b| a.0.cmp(&b.0));
    for (k, v) in &scope_attrs {
        if matches!(
            v,
            AttrValue::Bytes(_) | AttrValue::List(_) | AttrValue::Map(_)
        ) {
            continue;
        }
        let name = format!("otel_scope_{}", sanitize_label_name(k));
        push_first_writer_wins(&mut labels, &mut seen, name, attr_value_to_string(v));
    }

    // Step 4: every remaining resource attribute, sanitized, no allowlist.
    let mut resource = stream.resource.clone();
    resource.sort_by(|a, b| a.0.cmp(&b.0));
    for (k, v) in &resource {
        if matches!(
            k.as_str(),
            "service.namespace" | "service.name" | "service.instance.id"
        ) {
            continue;
        }
        if matches!(
            v,
            AttrValue::Bytes(_) | AttrValue::List(_) | AttrValue::Map(_)
        ) {
            continue;
        }
        let name = sanitize_label_name(k).into_owned();
        push_first_writer_wins(&mut labels, &mut seen, name, attr_value_to_string(v));
    }

    // `push_first_writer_wins` already de-duplicates by name, so `new` never
    // sees a repeat; `unwrap_or_default` (never `.expect`/`.unwrap`, per this
    // workspace's no-unwrap-in-production-paths rule) matches the fallback
    // `ravel_promql` itself uses at every other infallible `LabelSet::new`
    // call site (aggregate.rs, eval.rs, binop.rs).
    LabelSet::new(labels).unwrap_or_default()
}

/// ADR-1103 decision 2 step 5: `stream_labels` plus `severity_text` when the
/// record's severity text is non-empty. Empty severity means no label, not
/// an empty-valued one.
#[must_use]
pub fn series_label_set(stream_labels: &LabelSet, severity_text: &str) -> LabelSet {
    if severity_text.is_empty() {
        return stream_labels.clone();
    }
    let mut labels: Vec<Label> = stream_labels.iter().cloned().collect();
    labels.push(Label {
        name: SEVERITY_LABEL.to_string(),
        value: severity_text.to_string(),
    });
    LabelSet::new(labels).unwrap_or_default()
}

/// One log selector's request, resolved down to the inputs
/// [`fetch_log_series`] needs. `matchers` includes the `__name__` matcher;
/// [`fetch_log_series`] ignores it (the caller already used [`log_metric_of`]
/// to pick `metric`).
pub struct LogSeriesRequest<'a> {
    pub metric: LogMetric,
    pub matchers: &'a [LabelMatcher],
    /// Closed `[start_ns, end_ns]` event-time window.
    pub window: TimeRange,
    pub erasure: &'a [ErasurePredicate],
    pub max_samples: usize,
    pub max_series: usize,
    /// Checked once per fetched segment, after
    /// [`LogSegmentFetcher::scan_accounted_with_tenant`] has already fetched
    /// and charged it (see the check at the end of the `for seg_ref in
    /// segments` loop in [`fetch_log_series`]). This bound is therefore a
    /// per-segment limit in practice, not an exact byte ceiling: a query can
    /// overshoot it by up to one whole segment's bytes before the next
    /// iteration's check catches it. An exact bound would need a budget-aware
    /// change to the read path itself (checking before or during a segment's
    /// fetch, not only after), which nothing here implements.
    pub max_bytes_scanned: ByteLimit,
    pub deadline: Option<Instant>,
}

/// The result of answering one log selector.
#[derive(Debug)]
pub struct LogSeriesOutput {
    pub series: Vec<SeriesData>,
    pub records_scanned: u64,
    pub segments_fetched: usize,
    pub segments_pruned: usize,
}

/// Errors [`fetch_log_series`] can return. Every arm is a typed budget or
/// fetch failure, never a panic on a malformed input.
#[derive(Debug, thiserror::Error)]
pub enum LogSeriesError {
    #[error("query matched {count} samples, exceeding the limit of {max}")]
    SamplesExceeded { count: usize, max: usize },
    #[error("query matched {count} series, exceeding the limit of {max}")]
    SeriesExceeded { count: usize, max: usize },
    #[error("query scanned {scanned} bytes, exceeding the budget of {max}")]
    BytesScannedExceeded { scanned: u64, max: u64 },
    #[error("query exceeded its deadline")]
    DeadlineExceeded,
    #[error(transparent)]
    Fetch(#[from] LogFetchError),
    #[error("corrupt stream_attrs blob: {0}")]
    Decode(#[from] LogSegError),
    #[error("record's stream {stream_id:?} is not present in its segment's STREAM_DIR")]
    UnknownStream { stream_id: LogStreamId },
}

/// Every stream-level matcher (every matcher other than `__name__`,
/// `severity_text`, and `__body__`) evaluated against one stream's label
/// set with [`LabelMatcher::is_match`], PromQL's fully anchored regex
/// semantics.
fn stream_matchers(matchers: &[LabelMatcher]) -> Vec<&LabelMatcher> {
    matchers
        .iter()
        .filter(|m| {
            m.name != METRIC_NAME_LABEL && m.name != SEVERITY_LABEL && m.name != BODY_MATCHER_LABEL
        })
        .collect()
}

/// [`LabelMatcher::is_match`] evaluates a matcher against a [`LabelSet`];
/// `__body__` and the severity post-filters instead evaluate one matcher's
/// operator directly against a raw per-record string (the body, or
/// `severity_text`), so this mirrors its match arms without a `LabelSet`.
fn matches_str(m: &LabelMatcher, subject: &str) -> bool {
    match &m.op {
        MatchOp::Eq => m.value == subject,
        MatchOp::Ne => m.value != subject,
        MatchOp::Re(re) => re.is_match(subject),
        MatchOp::Nre(re) => !re.is_match(subject),
    }
}

/// Every `__body__` matcher (ADR-1103 decision 3): a matcher-only pseudo-label
/// evaluated per decoded record with no block-level pushdown. `matchers` must
/// come from [`body_matchers`], called on the full request matcher list --
/// `__body__` is filtered out of [`stream_matchers`]'s stream-level list, so
/// deriving this from that list instead silently drops every `__body__`
/// matcher and this function then evaluates over an empty slice.
fn body_matchers(matchers: &[LabelMatcher]) -> Vec<&LabelMatcher> {
    matchers
        .iter()
        .filter(|m| m.name == BODY_MATCHER_LABEL)
        .collect()
}

fn body_matches(matchers: &[&LabelMatcher], body: &str) -> bool {
    matchers.iter().all(|m| matches_str(m, body))
}

/// The first `severity_text` equality matcher, kept by index so
/// [`severity_postfilters`] can skip exactly that one matcher and evaluate
/// every other `severity_text` matcher -- including a second, third, ...
/// equality -- as a post-filter. PromQL selector matchers conjoin: a
/// selector naming `severity_text` twice must satisfy both, not just the
/// first.
fn severity_content_predicate(matchers: &[LabelMatcher]) -> Option<(usize, &LabelMatcher)> {
    matchers
        .iter()
        .enumerate()
        .find(|(_, m)| m.name == SEVERITY_LABEL && matches!(m.op, MatchOp::Eq))
}

/// Every `severity_text` matcher other than the one
/// [`severity_content_predicate`] pushed down (identified by index, not by
/// value or pointer, so a second matcher identical to the pushed-down one is
/// still evaluated rather than silently treated as already satisfied).
fn severity_postfilters(
    matchers: &[LabelMatcher],
    pushed_index: Option<usize>,
) -> Vec<&LabelMatcher> {
    matchers
        .iter()
        .enumerate()
        .filter(|(i, m)| m.name == SEVERITY_LABEL && Some(*i) != pushed_index)
        .map(|(_, m)| m)
        .collect()
}

fn bytes_scanned_exceeded(scanned: u64, max: ByteLimit) -> Option<LogSeriesError> {
    match max {
        ByteLimit::Bounded(max) if scanned > max => {
            Some(LogSeriesError::BytesScannedExceeded { scanned, max })
        }
        _ => None,
    }
}

fn deadline_exceeded(deadline: Option<Instant>) -> Option<LogSeriesError> {
    match deadline {
        Some(d) if Instant::now() >= d => Some(LogSeriesError::DeadlineExceeded),
        _ => None,
    }
}

/// Answers one log selector by planning and scanning `segments` in order
/// (ADR-1103 decision 4). See the module doc for the Plan-phase stream
/// discovery mechanism.
pub async fn fetch_log_series(
    fetcher: &LogSegmentFetcher,
    tenant_hash: TenantHash,
    segments: &[SegmentRef],
    req: &LogSeriesRequest<'_>,
    accounting: &PhaseAccounting,
) -> Result<LogSeriesOutput, LogSeriesError> {
    let stream_ms = stream_matchers(req.matchers);
    let body_ms = body_matchers(req.matchers);
    let needs_body = req.metric.needs_body() || !body_ms.is_empty();
    let severity_eq = severity_content_predicate(req.matchers);
    let severity_post = severity_postfilters(req.matchers, severity_eq.map(|(i, _)| i));

    // Column selection: ts and stream_ref are implicit; severity_text always
    // (it is either a content predicate or a label/postfilter source); body
    // iff the metric or a __body__ matcher needs it; the record-attribute
    // columns any pending erasure predicate names (crates/ravel-sql/src/
    // logs_scan.rs:395-480's pattern -- resource/scope attrs cost nothing
    // extra, they live in STREAM_DIR, so only record-level erasure keys widen
    // the selection).
    let mut columns = ColumnSelection::fixed_only().with_severity_text();
    if needs_body {
        columns = columns.with_body();
    }
    for p in req.erasure {
        for (key, _) in p.matchers() {
            columns = columns.with_attr(key.clone());
        }
    }

    let mut stream_label_cache: HashMap<LogStreamId, LabelSet> = HashMap::new();
    let mut series: HashMap<Vec<u8>, (LabelSet, Vec<Sample>)> = HashMap::new();
    let mut records_scanned: u64 = 0u64;
    let mut segments_fetched = 0usize;
    let mut segments_pruned = 0usize;

    for seg_ref in segments {
        if let Some(err) = deadline_exceeded(req.deadline) {
            return Err(err);
        }

        let seg_window = TimeRange {
            start_ns: seg_ref.min_event_ts_ns,
            end_ns: seg_ref.max_event_ts_ns,
        };
        if !req.window.overlaps(&seg_window) {
            segments_pruned += 1;
            continue;
        }

        // (a) Plan phase: STREAM_DIR-only discovery (ADR-1103 decision 2) --
        // reads the footer plus the STREAM_DIR section, no BLOCKS byte.
        // Stream label sets are cached by LogStreamId across segments so a
        // stream repeated in a later segment costs no further decode.
        let Some(entries) = fetcher
            .fetch_stream_dir(seg_ref, tenant_hash, accounting.plan())
            .await?
        else {
            segments_pruned += 1;
            continue;
        };

        let mut matching_streams: Vec<LogStreamId> = Vec::new();
        for (stream_id, blob) in &entries {
            let labels = match stream_label_cache.get(stream_id) {
                Some(labels) => labels.clone(),
                None => {
                    let attrs = decode_stream_attrs(blob)?;
                    let labels = stream_label_set(req.metric, &attrs);
                    stream_label_cache.insert(*stream_id, labels.clone());
                    labels
                }
            };
            if stream_ms.iter().all(|m| m.is_match(&labels)) {
                matching_streams.push(*stream_id);
            }
        }

        // (b) If no stream matches, this segment is pruned: skip the scan
        // entirely.
        if matching_streams.is_empty() {
            segments_pruned += 1;
            continue;
        }

        // (c) Scan phase: exact content predicates only (StreamIn, and
        // severity_text when the matcher is an equality); everything else is
        // a per-record filter after decode.
        let mut query = LogQuery::new(req.window.start_ns, req.window.end_ns)
            .with_content(Predicate::StreamIn(matching_streams))
            .with_erasure(req.erasure.to_vec());
        if let Some((_, m)) = severity_eq {
            query = query.with_content(Predicate::Equals {
                field: FieldSel::SeverityText,
                value: AttrValue::Str(m.value.clone()),
            });
        }

        let Some(mut scan) = fetcher
            .scan_accounted_with_tenant(seg_ref, tenant_hash, &query, &columns, accounting.scan())
            .await?
        else {
            segments_pruned += 1;
            continue;
        };
        segments_fetched += 1;

        while let Some(records) = scan.next_block()? {
            if let Some(err) = deadline_exceeded(req.deadline) {
                return Err(err);
            }
            for record in &records {
                if !severity_post
                    .iter()
                    .all(|m| matches_str(m, &record.severity_text))
                {
                    continue;
                }
                if !body_matches(&body_ms, &record.body) {
                    continue;
                }

                // Every record surviving `Predicate::StreamIn` names a
                // stream from `matching_streams`, which the Plan-phase
                // STREAM_DIR discovery above always caches first: a record
                // whose stream is absent from the same segment's STREAM_DIR
                // is corrupt input, not a case to paper over with fabricated
                // labels.
                let stream_labels = stream_label_cache.get(&record.stream_id).cloned().ok_or(
                    LogSeriesError::UnknownStream {
                        stream_id: record.stream_id,
                    },
                )?;
                let labels = series_label_set(&stream_labels, &record.severity_text);

                let key = labels.iter().fold(Vec::new(), |mut acc, l| {
                    acc.extend_from_slice(l.name.as_bytes());
                    acc.push(0);
                    acc.extend_from_slice(l.value.as_bytes());
                    acc.push(0);
                    acc
                });
                if !series.contains_key(&key) && series.len() >= req.max_series {
                    return Err(LogSeriesError::SeriesExceeded {
                        count: series.len() + 1,
                        max: req.max_series,
                    });
                }
                let value = match req.metric {
                    LogMetric::Lines => 1.0,
                    LogMetric::Bytes => record.body.len() as f64,
                };
                let entry = series.entry(key).or_insert_with(|| (labels, Vec::new()));
                entry.1.push(Sample {
                    ts_ns: record.ts_ns,
                    value,
                });

                records_scanned += 1;
                if records_scanned as usize > req.max_samples {
                    return Err(LogSeriesError::SamplesExceeded {
                        count: records_scanned as usize,
                        max: req.max_samples,
                    });
                }
            }
        }

        // Checked only here, once per completed segment: `scan_accounted_with_tenant`
        // has already fetched and charged this segment's bytes above, and
        // `next_block` only decodes bytes already resident, so nothing inside
        // the block loop can react to the budget mid-segment. A query can
        // therefore overshoot `max_bytes_scanned` by up to one segment's
        // bytes before this check catches it (see the field doc on
        // `LogSeriesRequest::max_bytes_scanned`).
        let scanned = accounting.snapshot().pooled().total_s3_bytes();
        if let Some(err) = bytes_scanned_exceeded(scanned, req.max_bytes_scanned) {
            return Err(err);
        }
    }

    let mut out: Vec<(Vec<u8>, LabelSet, Vec<Sample>)> = series
        .into_iter()
        .map(|(key, (labels, mut samples))| {
            samples.sort_by_key(|s| s.ts_ns);
            (key, labels, samples)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(LogSeriesOutput {
        series: out
            .into_iter()
            .map(|(_, labels, samples)| SeriesData { labels, samples })
            .collect(),
        records_scanned,
        segments_fetched,
        segments_pruned,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn attrs(
        resource: &[(&str, AttrValue)],
        scope_name: &str,
        scope_version: &str,
        scope_attrs: &[(&str, AttrValue)],
    ) -> StreamAttrs {
        StreamAttrs {
            resource: resource
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            scope_name: scope_name.to_string(),
            scope_version: scope_version.to_string(),
            scope_attrs: scope_attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    fn s(v: &str) -> AttrValue {
        AttrValue::Str(v.to_string())
    }

    // --- log_metric_of: routing table ---

    #[test]
    fn log_metric_of_routes_reserved_names() {
        let lines = [LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC)];
        assert_eq!(log_metric_of(&lines), Some(LogMetric::Lines));

        let bytes = [LabelMatcher::equal(METRIC_NAME_LABEL, LOG_BYTES_METRIC)];
        assert_eq!(log_metric_of(&bytes), Some(LogMetric::Bytes));
    }

    #[test]
    fn log_metric_of_none_without_name_matcher() {
        let ms = [LabelMatcher::equal("job", "api")];
        assert_eq!(log_metric_of(&ms), None);
    }

    #[test]
    fn log_metric_of_none_for_other_metric_name() {
        let ms = [LabelMatcher::equal(
            METRIC_NAME_LABEL,
            "http_requests_total",
        )];
        assert_eq!(log_metric_of(&ms), None);
    }

    #[test]
    fn log_metric_of_none_for_regex_name_matcher() {
        // The regex's own value is the exact literal `ravel_log_lines`, so a
        // `MatchOp::Eq`-only guard removed from `log_metric_of` would make
        // this pass under `=~` too; pinning it to the literal (rather than a
        // pattern like `ravel_log.*` the `MatchOp::Eq` guard trivially never
        // equals) is what makes the guard's removal fail this test.
        let ms = [LabelMatcher::regex(METRIC_NAME_LABEL, "ravel_log_lines").unwrap()];
        assert_eq!(log_metric_of(&ms), None);
    }

    #[test]
    fn log_metric_of_none_for_negated_name_matcher() {
        let ms = [LabelMatcher::not_equal(METRIC_NAME_LABEL, LOG_LINES_METRIC)];
        assert_eq!(log_metric_of(&ms), None);
    }

    // --- sanitize_label_name ---

    #[test]
    fn sanitize_label_name_borrows_when_already_clean() {
        assert_eq!(
            sanitize_label_name("service_name"),
            Cow::Borrowed("service_name")
        );
        assert_eq!(sanitize_label_name("_leading"), Cow::Borrowed("_leading"));
    }

    #[test]
    fn sanitize_label_name_rewrites_disallowed_chars() {
        assert_eq!(
            sanitize_label_name("service.name"),
            Cow::<str>::Owned("service_name".to_string())
        );
        assert_eq!(
            sanitize_label_name("k8s.pod-name"),
            Cow::<str>::Owned("k8s_pod_name".to_string())
        );
    }

    #[test]
    fn sanitize_label_name_rewrites_leading_digit() {
        assert_eq!(
            sanitize_label_name("2fast"),
            Cow::<str>::Owned("_fast".to_string())
        );
    }

    #[test]
    fn sanitize_label_name_empty_is_clean() {
        assert_eq!(sanitize_label_name(""), Cow::Borrowed(""));
    }

    // --- stream_label_set: label mapping ---

    #[test]
    fn stream_label_set_derives_job_from_namespace_and_name() {
        let a = attrs(
            &[
                ("service.namespace", s("payments")),
                ("service.name", s("api")),
                ("service.instance.id", s("i-1")),
            ],
            "",
            "",
            &[],
        );
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get(METRIC_NAME_LABEL), Some(LOG_LINES_METRIC));
        assert_eq!(labels.get("job"), Some("payments/api"));
        assert_eq!(labels.get("instance"), Some("i-1"));
        // Source attrs never also appear under their own sanitized names.
        assert_eq!(labels.get("service_namespace"), None);
        assert_eq!(labels.get("service_name"), None);
        assert_eq!(labels.get("service_instance_id"), None);
    }

    #[test]
    fn stream_label_set_job_without_namespace_is_bare_service_name() {
        let a = attrs(&[("service.name", s("api"))], "", "", &[]);
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("job"), Some("api"));
    }

    #[test]
    fn stream_label_set_no_job_without_service_name() {
        let a = attrs(&[("service.namespace", s("payments"))], "", "", &[]);
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("job"), None);
    }

    #[test]
    fn stream_label_set_otel_scope_names() {
        let a = attrs(&[], "libfoo", "1.2.3", &[("lib.tag", s("x"))]);
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("otel_scope_name"), Some("libfoo"));
        assert_eq!(labels.get("otel_scope_version"), Some("1.2.3"));
        assert_eq!(labels.get("otel_scope_lib_tag"), Some("x"));
    }

    #[test]
    fn stream_label_set_empty_scope_name_and_version_produce_no_label() {
        let a = attrs(&[], "", "", &[]);
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("otel_scope_name"), None);
        assert_eq!(labels.get("otel_scope_version"), None);
    }

    #[test]
    fn stream_label_set_remaining_resource_attrs_no_allowlist() {
        let a = attrs(
            &[("host.name", s("h1")), ("region", s("us-east"))],
            "",
            "",
            &[],
        );
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("host_name"), Some("h1"));
        assert_eq!(labels.get("region"), Some("us-east"));
    }

    #[test]
    fn stream_label_set_dedups_by_sanitized_name_first_writer_wins() {
        // "host.name" and "host_name" both sanitize to "host_name"; whichever
        // sorts first in canonical byte order wins.
        let a = attrs(
            &[("host.name", s("dotted")), ("host_name", s("under"))],
            "",
            "",
            &[],
        );
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("host_name"), Some("dotted"));
    }

    #[test]
    fn stream_label_set_drops_reserved_and_dunder_names() {
        let a = attrs(
            &[("__reserved__", s("x")), ("severity_text", s("y"))],
            "",
            "",
            &[],
        );
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("__reserved__"), None);
        assert_eq!(labels.get("severity_text"), None);
    }

    #[test]
    fn stream_label_set_drops_empty_values() {
        let a = attrs(&[("empty", s(""))], "", "", &[]);
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("empty"), None);
    }

    #[test]
    fn stream_label_set_excludes_bytes_list_map_values() {
        let a = attrs(
            &[
                ("blob", AttrValue::Bytes(vec![1, 2, 3])),
                ("tags", AttrValue::List(vec![s("a")])),
                ("nested", AttrValue::Map(vec![("k".into(), s("v"))])),
            ],
            "",
            "",
            &[],
        );
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("blob"), None);
        assert_eq!(labels.get("tags"), None);
        assert_eq!(labels.get("nested"), None);
    }

    #[test]
    fn stream_label_set_stringifies_non_string_values() {
        let a = attrs(
            &[
                ("port", AttrValue::I64(8080)),
                ("ok", AttrValue::Bool(true)),
            ],
            "",
            "",
            &[],
        );
        let labels = stream_label_set(LogMetric::Lines, &a);
        assert_eq!(labels.get("port"), Some("8080"));
        assert_eq!(labels.get("ok"), Some("true"));
    }

    #[test]
    fn stream_label_set_bytes_metric_name() {
        let a = attrs(&[], "", "", &[]);
        let labels = stream_label_set(LogMetric::Bytes, &a);
        assert_eq!(labels.get(METRIC_NAME_LABEL), Some(LOG_BYTES_METRIC));
    }

    // --- series_label_set ---

    #[test]
    fn series_label_set_adds_severity_when_present() {
        let stream = stream_label_set(LogMetric::Lines, &attrs(&[], "", "", &[]));
        let series = series_label_set(&stream, "ERROR");
        assert_eq!(series.get(SEVERITY_LABEL), Some("ERROR"));
    }

    #[test]
    fn series_label_set_empty_severity_means_no_label() {
        let stream = stream_label_set(LogMetric::Lines, &attrs(&[], "", "", &[]));
        let series = series_label_set(&stream, "");
        assert_eq!(series.get(SEVERITY_LABEL), None);
        assert_eq!(series, stream);
    }

    // --- matches_str ---

    #[test]
    fn matches_str_covers_all_ops() {
        let eq = LabelMatcher::equal(BODY_MATCHER_LABEL, "boom");
        assert!(matches_str(&eq, "boom"));
        assert!(!matches_str(&eq, "other"));

        let ne = LabelMatcher::not_equal(BODY_MATCHER_LABEL, "boom");
        assert!(!matches_str(&ne, "boom"));
        assert!(matches_str(&ne, "other"));

        let re = LabelMatcher::regex(BODY_MATCHER_LABEL, "^boo.*").unwrap();
        assert!(matches_str(&re, "boom"));
        assert!(!matches_str(&re, "other"));

        let nre = LabelMatcher::not_regex(BODY_MATCHER_LABEL, "^boo.*").unwrap();
        assert!(!matches_str(&nre, "boom"));
        assert!(matches_str(&nre, "other"));
    }
}
