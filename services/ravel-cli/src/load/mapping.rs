use super::*;

/// Source time unit for the mapped `ts` column, converted to nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TsUnit {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl TsUnit {
    pub(crate) fn factor(self) -> i64 {
        match self {
            TsUnit::Seconds => 1_000_000_000,
            TsUnit::Millis => 1_000_000,
            TsUnit::Micros => 1_000,
            TsUnit::Nanos => 1,
        }
    }

    /// The spelling a mapping writes this unit as, for a rejection that has to
    /// point the operator back at the line that declared it.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            TsUnit::Seconds => "seconds",
            TsUnit::Millis => "millis",
            TsUnit::Micros => "micros",
            TsUnit::Nanos => "nanos",
        }
    }
}

/// Declared type for a mapped attribute column, one of the scalar
/// [`AttrValue`] kinds. (Lists and maps have no Parquet-column source and are
/// not producible by this path.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColType {
    Str,
    I64,
    F64,
    Bool,
    Bytes,
}

/// One mapped attribute: a source Parquet column, the record/resource key it
/// becomes, and its declared type.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttrMap {
    /// The attribute key stored in the record (e.g. `service.name`).
    pub key: String,
    /// The source Parquet column name.
    pub column: String,
    /// Declared value type, used to build the typed [`AttrValue`].
    #[serde(rename = "type")]
    pub value_type: ColType,
}

/// The `--mapping` TOML: source Parquet columns to record fields.
///
/// ```toml
/// ts_column = "timestamp"
/// ts_unit   = "millis"        # seconds | millis | micros | nanos
///
/// body_column            = "message"   # optional
/// severity_number_column = "sev_num"   # optional (integer column)
/// severity_text_column   = "sev_text"  # optional (string column)
/// trace_id_column        = "trace_id"  # optional (16-byte binary or 32-hex str)
/// span_id_column         = "span_id"   # optional (8-byte binary or 16-hex str)
///
/// # Resource attributes: part of stream identity.
/// [[resource_attribute]]
/// key = "service.name"
/// column = "svc"
/// type = "str"
///
/// # Record attributes: typed values in `attrs`, NOT part of stream identity.
/// [[attribute]]
/// key = "http.status_code"
/// column = "status"
/// type = "i64"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub ts_column: String,
    pub ts_unit: TsUnit,
    #[serde(default)]
    pub body_column: Option<String>,
    #[serde(default)]
    pub severity_number_column: Option<String>,
    #[serde(default)]
    pub severity_text_column: Option<String>,
    #[serde(default)]
    pub trace_id_column: Option<String>,
    #[serde(default)]
    pub span_id_column: Option<String>,
    /// Columns that determine stream identity (ADR-0029): distinct from record
    /// attributes.
    #[serde(default, rename = "resource_attribute")]
    pub resource_attributes: Vec<AttrMap>,
    /// Columns that become typed values in the record's `attrs`; never part of
    /// stream identity.
    #[serde(default, rename = "attribute")]
    pub attributes: Vec<AttrMap>,
    /// Opt-in output column for `ravel-cli export` (ADR-1751 decision 4): every
    /// record attribute not named by `attributes` above is written into this
    /// one `Map<Utf8, Utf8>` column, stringified the same way `attrs['<key>']`
    /// stringifies a value for SQL. Write side only: `ravel-cli load` does not
    /// read this column back. A mapping that never sets it is unaffected.
    #[serde(default)]
    pub attrs_map_column: Option<String>,
}

/// Parse the logs section of a `--mapping` TOML document.
///
/// Accepts both spellings ADR-1751 decision 2 leaves valid for logs: a
/// document whose one signal section is `[logs]`, and the pre-ADR-1751
/// top-level form whose logs keys sit at the document root. See
/// [`parse_mapping_document`] for the section rules.
pub fn parse_mapping(text: &str) -> Result<Mapping, LoadError> {
    match parse_mapping_document(text, SignalArg::Logs)? {
        MappingSection::Logs(mapping) => Ok(mapping),
        // `parse_mapping_document` returns the section matching the signal it
        // was asked for, so these arms are not reachable through this call.
        other => Err(wrong_section_resolved("logs", other)),
    }
}

/// Parse the metrics section of a `--mapping` TOML document (ADR-1751
/// decision 2).
pub fn parse_metrics_mapping(text: &str) -> Result<MetricsMapping, LoadError> {
    match parse_mapping_document(text, SignalArg::Metrics)? {
        MappingSection::Metrics(mapping) => Ok(mapping),
        other => Err(wrong_section_resolved("metrics", other)),
    }
}

/// Parse the spans section of a `--mapping` TOML document (ADR-1751
/// decision 2).
pub fn parse_spans_mapping(text: &str) -> Result<SpansMapping, LoadError> {
    match parse_mapping_document(text, SignalArg::Spans)? {
        MappingSection::Spans(mapping) => Ok(mapping),
        other => Err(wrong_section_resolved("spans", other)),
    }
}

/// The internal error a per-signal parse helper returns if
/// [`parse_mapping_document`] ever handed it another signal's section. Not
/// reachable: that function returns the section matching the signal it was
/// asked for, or an error.
fn wrong_section_resolved(wanted: &str, got: MappingSection) -> LoadError {
    let got = match got {
        MappingSection::Logs(_) => "logs",
        MappingSection::Metrics(_) => "metrics",
        MappingSection::Spans(_) => "spans",
    };
    LoadError::Setup(format!(
        "internal error: the {wanted} mapping resolved to a {got} section"
    ))
}

/// The signal section names a `--mapping` document may carry (ADR-1751
/// decision 2). Any other top-level key is read as the pre-ADR-1751
/// top-level logs form.
const MAPPING_SECTION_NAMES: [&str; 3] = ["logs", "metrics", "spans"];

/// The section name a `--signal` value selects.
fn mapping_section_name(signal: SignalArg) -> &'static str {
    match signal {
        SignalArg::Metrics => "metrics",
        SignalArg::Logs => "logs",
        SignalArg::Spans => "spans",
    }
}

/// The resolved mapping section for one load's `--signal`.
#[derive(Debug, Clone)]
pub enum MappingSection {
    Logs(Mapping),
    Metrics(MetricsMapping),
    Spans(SpansMapping),
}

/// Resolve the one signal section of a `--mapping` document and deserialize
/// it (ADR-1751 decision 2).
///
/// Exactly one of `[logs]`, `[metrics]` and `[spans]` may be present, and it
/// must match `--signal`. Two exceptions carry the pre-ADR-1751 form forward:
///
/// - a document with no signal section at all, whose top-level keys are the
///   ADR-0089 logs keys, is read as the `[logs]` section (so every mapping
///   written before this change keeps loading, unchanged, under the default
///   `--signal logs`);
/// - that same document under any other `--signal` is refused by name rather
///   than by a serde "unknown field" error, since the operator's real mistake
///   is a missing section, not a typo.
///
/// Mixing the two spellings (a signal section *and* top-level logs keys) is
/// refused: which one wins would otherwise be an invisible precedence rule.
pub fn parse_mapping_document(text: &str, signal: SignalArg) -> Result<MappingSection, LoadError> {
    let mut doc: toml::Table = toml::from_str(text)
        .map_err(|e| LoadError::Setup(format!("invalid --mapping TOML: {e}")))?;

    let present: Vec<&'static str> = MAPPING_SECTION_NAMES
        .iter()
        .copied()
        .filter(|name| doc.contains_key(*name))
        .collect();
    let top_level: Vec<String> = doc
        .keys()
        .filter(|k| !MAPPING_SECTION_NAMES.contains(&k.as_str()))
        .cloned()
        .collect();
    let wanted = mapping_section_name(signal);

    if present.len() > 1 {
        return Err(LoadError::Setup(format!(
            "--mapping file declares {} signal sections ({}). Exactly one must be present, and it \
             must match --signal {wanted}.",
            present.len(),
            present.join(", ")
        )));
    }
    if !present.is_empty() && !top_level.is_empty() {
        return Err(LoadError::Setup(format!(
            "--mapping file mixes a [{}] signal section with top-level keys ({}). Move every \
             mapped field inside the section: with both spellings present there is no rule \
             saying which one a load would use.",
            present.join(""),
            top_level.join(", ")
        )));
    }

    if let Some(section) = present.first().copied() {
        if section != wanted {
            return Err(LoadError::Setup(format!(
                "--mapping file declares a [{section}] section but --signal is {wanted}. Exactly \
                 one section must be present and it must match --signal (ADR-1751 decision 2)."
            )));
        }
        let value = doc
            .remove(section)
            .unwrap_or(toml::Value::Table(toml::Table::new()));
        return deserialize_section(text, &value, Some(section), signal);
    }

    if signal != SignalArg::Logs {
        return Err(LoadError::Setup(format!(
            "--mapping file has no [{wanted}] section, which --signal {wanted} requires \
             (ADR-1751 decision 2). Its top-level keys are {}.",
            if top_level.is_empty() {
                "none (the file is empty)".to_string()
            } else {
                format!(
                    "{} (the pre-ADR-1751 logs-only form, read as the [logs] section)",
                    top_level.join(", ")
                )
            }
        )));
    }
    deserialize_section(text, &toml::Value::Table(doc), None, signal)
}

/// Deserialize one already-selected section table into its typed mapping.
///
/// `section` is the section name the keys were read from, or `None` for the
/// pre-ADR-1751 top-level logs form. The distinction is only in the error
/// prefix, and it is there so a mapping written before ADR-1751 still fails
/// with the message it has always failed with (`invalid --mapping TOML: ...`)
/// rather than with one naming a section its author never wrote.
///
/// The typed pass re-reads `text` rather than converting `value`: only a
/// deserializer over the source text carries spans, so this is what keeps the
/// line and column in a schema error. The caller has already checked that the
/// document holds this one section and nothing beside it.
fn deserialize_section(
    text: &str,
    value: &toml::Value,
    section: Option<&str>,
    signal: SignalArg,
) -> Result<MappingSection, LoadError> {
    #[derive(Deserialize)]
    struct LogsSection {
        logs: Mapping,
    }
    #[derive(Deserialize)]
    struct MetricsSection {
        metrics: MetricsMapping,
    }
    #[derive(Deserialize)]
    struct SpansSection {
        spans: SpansMapping,
    }

    let bad = |e: toml::de::Error| match section {
        Some(section) => LoadError::Setup(format!("invalid --mapping [{section}] section: {e}")),
        None => LoadError::Setup(format!("invalid --mapping TOML: {e}")),
    };
    match (signal, section) {
        (SignalArg::Logs, None) => toml::from_str::<Mapping>(text)
            .map(MappingSection::Logs)
            .map_err(bad),
        (SignalArg::Logs, Some(_)) => toml::from_str::<LogsSection>(text)
            .map(|doc| MappingSection::Logs(doc.logs))
            .map_err(bad),
        (SignalArg::Metrics, _) => {
            reject_native_histogram_keys(value)?;
            let mapping = toml::from_str::<MetricsSection>(text).map_err(bad)?.metrics;
            mapping.validate()?;
            Ok(MappingSection::Metrics(mapping))
        }
        (SignalArg::Spans, _) => {
            reject_unmappable_span_keys(value)?;
            let mapping = toml::from_str::<SpansSection>(text).map_err(bad)?.spans;
            mapping.validate()?;
            Ok(MappingSection::Spans(mapping))
        }
    }
}

/// Keys that name a native (exponential) histogram, refused by name before
/// `deny_unknown_fields` can report them as a generic typo (ADR-1751
/// decision 2: native histograms are not mappable in this version).
const NATIVE_HISTOGRAM_KEYS: [&str; 2] = ["native_histogram", "exponential_histogram"];

/// Refuse a metrics section that names a native/exponential histogram at the
/// section's top level.
fn reject_native_histogram_keys(value: &toml::Value) -> Result<(), LoadError> {
    let Some(table) = value.as_table() else {
        return Ok(());
    };
    for key in NATIVE_HISTOGRAM_KEYS {
        if table.contains_key(key) {
            return Err(native_histogram_rejected(key));
        }
    }
    Ok(())
}

/// The refusal a native/exponential histogram mapping gets (ADR-1751
/// decision 2).
fn native_histogram_rejected(what: &str) -> LoadError {
    LoadError::Setup(format!(
        "--mapping names a native (exponential) histogram ({what}), which this version does not \
         map (ADR-1751 decision 2: native histograms, span events and span links are not \
         mappable, and a mapping that names them is rejected). Only the classic-histogram shape \
         (le plus sum and count columns) is supported."
    ))
}

/// Whether a metric's exploded/scalar series behaves as a monotonic counter
/// (`kind = "counter"`) or a gauge. Absent means gauge, which is what
/// `ravel-otlp` reports for every non-`Sum` point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKindArg {
    Gauge,
    Counter,
}

/// One mapped label: the Prometheus label name it becomes and the source
/// Parquet column. Values are read as strings (an integer, float or boolean
/// column is stringified), since a Prometheus label value is a string.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelMap {
    /// The label name stored on the series (e.g. `job`).
    pub name: String,
    /// The source Parquet column name.
    pub column: String,
}

/// Which histogram encoding a `[metrics.histogram]` section describes. Only
/// `classic` is mappable in this version (ADR-1751 decision 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistogramTypeArg {
    Classic,
    Native,
    Exponential,
}

/// The classic-histogram shape of a metrics mapping (ADR-1751 decision 2):
/// the `le` column plus the data point's `sum` and `count` columns.
///
/// One input ROW is one bucket of one data point. The rows of one data point
/// are those sharing a metric name, a label set and a `ts`, and they must be
/// CONTIGUOUS in the file; the loader groups a contiguous run and refuses a
/// run it has already closed rather than exploding a data point twice (see
/// [`MetricsLoadError`]'s non-contiguity message). Within a group:
///
/// - the row's `value` column is that bucket's OWN count, the OTLP
///   `bucket_counts[i]` convention, not a running total. The loader
///   accumulates, exactly as `ravel_otlp::normalize`'s `explode_histogram`
///   does, so a Prometheus-style already-cumulative `_bucket` export must be
///   de-accumulated before it is loaded;
/// - the row's `le` column is that bucket's explicit upper bound, and must be
///   finite. The `+Inf` bucket is NOT a row: it is synthesized from the
///   `count` column, matching OTLP, where `explicit_bounds` carries only the
///   finite bounds;
/// - `sum` and `count` are the whole data point's, so every row of one group
///   must carry the same values (compared by bit pattern for `sum`). A null
///   `sum` cell emits no `_sum` series, matching an OTLP data point with no
///   `sum` field.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistogramMap {
    /// `classic` (the default) or an explicit rejection of `native` /
    /// `exponential`.
    #[serde(default, rename = "type")]
    pub histogram_type: Option<HistogramTypeArg>,
    /// Source column carrying each row's explicit bucket upper bound.
    pub le_column: String,
    /// Source column carrying the data point's `sum`. A null cell emits no
    /// `_sum` series.
    pub sum_column: String,
    /// Source column carrying the data point's total count, which is both the
    /// `+Inf` bucket's value and the `_count` series' value.
    pub count_column: String,
}

/// The `[metrics]` section of a `--mapping` TOML (ADR-1751 decision 2).
///
/// ```toml
/// [metrics]
/// name_column  = "metric"      # a column, OR name = "http_requests_total"
/// value_column = "value"
/// ts_column    = "ts"
/// ts_unit      = "millis"      # seconds | millis | micros | nanos
/// unit         = "s"           # optional UCUM unit, suffixed into the name
/// kind         = "counter"     # optional: gauge (default) | counter
///
/// [[metrics.label]]
/// name   = "job"
/// column = "svc"
///
/// # Optional classic-histogram shape. With it, one row is one bucket.
/// [metrics.histogram]
/// le_column    = "le"
/// sum_column   = "sum"
/// count_column = "count"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsMapping {
    /// Literal metric name, used for every row. Mutually exclusive with
    /// [`MetricsMapping::name_column`]; exactly one is required.
    #[serde(default)]
    pub name: Option<String>,
    /// Source column carrying each row's metric name. Mutually exclusive with
    /// [`MetricsMapping::name`]; exactly one is required.
    #[serde(default)]
    pub name_column: Option<String>,
    /// Source column carrying the sample value. For a classic histogram this
    /// is the row's own bucket count (see [`HistogramMap`]).
    pub value_column: String,
    /// Source column carrying the event timestamp.
    pub ts_column: String,
    /// Unit of [`MetricsMapping::ts_column`] when it is an integer column. A
    /// native Arrow `Timestamp` column carries its own unit and this is not
    /// applied again (see [`read_ts`]).
    pub ts_unit: TsUnit,
    /// The metric's UCUM unit, played by this mapping exactly as an OTLP
    /// `Metric`'s `unit` field is: it selects the Prometheus unit suffix
    /// appended to the family name (ADR-0085 decision 2, applied through
    /// `ravel_otlp::normalize::prometheus_family_name`). Absent is the empty
    /// unit, which suffixes nothing.
    #[serde(default)]
    pub unit: Option<String>,
    /// `counter` sets `is_monotonic_sum` on every point this mapping
    /// produces, as a monotonic OTLP `Sum` does, and adds the `_total` suffix
    /// its family name gets; absent or `gauge` leaves both off. Refused
    /// together with `[metrics.histogram]`: OTLP has no monotonic histogram,
    /// and every series a classic histogram explodes into is non-monotonic.
    #[serde(default)]
    pub kind: Option<MetricKindArg>,
    /// Columns that become Prometheus labels on the series.
    #[serde(default, rename = "label")]
    pub labels: Vec<LabelMap>,
    /// Optional classic-histogram shape. Absent means one row is one scalar
    /// sample.
    #[serde(default)]
    pub histogram: Option<HistogramMap>,
}

impl MetricsMapping {
    /// `true` when this mapping describes a classic histogram.
    pub fn is_histogram(&self) -> bool {
        self.histogram.is_some()
    }

    /// The OTLP metric kind this mapping stands for and whether its points
    /// are a monotonic sum, the two inputs
    /// `ravel_otlp::normalize::prometheus_family_name` takes beside the unit.
    ///
    /// A classic histogram is `Histogram`, never monotonic, exactly as
    /// `ravel_otlp::normalize` classifies an OTLP `Histogram`; `kind` cannot
    /// be set alongside one (see [`MetricsMapping::validate`]).
    pub(crate) fn metric_kind(&self) -> (MetricKind, bool) {
        if self.is_histogram() {
            return (MetricKind::Histogram, false);
        }
        match self.kind {
            Some(MetricKindArg::Counter) => (MetricKind::Counter, true),
            Some(MetricKindArg::Gauge) | None => (MetricKind::Gauge, false),
        }
    }

    /// The mapping's unit, empty when it declares none. Fed to
    /// `prometheus_family_name` where OTLP feeds `Metric::unit`.
    pub(crate) fn unit(&self) -> &str {
        self.unit.as_deref().unwrap_or("")
    }

    /// Each mapped label's Prometheus name, put through the same
    /// `sanitize_label_name` OTLP applies to an attribute key, paired with
    /// the source column. Computed once per batch rather than once per row.
    pub(crate) fn sanitized_label_names(&self) -> Vec<String> {
        self.labels
            .iter()
            .map(|l| sanitize_label_name(l.name.clone()))
            .collect()
    }

    /// The checks a metrics mapping fails before any Parquet byte is read.
    ///
    /// Everything here is a property of the mapping alone, so it is worth
    /// refusing at setup: a name that is neither a column nor a literal, a
    /// label name that would collide with a synthesized one, or a native
    /// histogram, each of which would otherwise be discovered per row (or,
    /// for the collision, only as a `DuplicateLabelName`-shaped rejection on
    /// the first row).
    pub fn validate(&self) -> Result<(), LoadError> {
        match (&self.name, &self.name_column) {
            (Some(_), Some(_)) => {
                return Err(LoadError::Setup(
                    "--mapping [metrics] sets both name and name_column. The metric name is \
                     either a literal (name) or a column (name_column), never both."
                        .to_string(),
                ));
            }
            (None, None) => {
                return Err(LoadError::Setup(
                    "--mapping [metrics] sets neither name nor name_column. The metric name is \
                     either a literal (name) or a column (name_column)."
                        .to_string(),
                ));
            }
            _ => {}
        }
        if let Some(literal) = &self.name {
            let limits = IngestLimits::default();
            if literal.is_empty() {
                return Err(LoadError::Setup(
                    "--mapping [metrics] name is empty; a metric name is required".to_string(),
                ));
            }
            if literal.len() > limits.max_metric_name_len {
                return Err(LoadError::Setup(format!(
                    "--mapping [metrics] name is {} bytes, more than the metric-name limit of {}",
                    literal.len(),
                    limits.max_metric_name_len
                )));
            }
        }

        let limits = IngestLimits::default();
        // Every check below is against the SANITIZED label name, because that
        // is the name the series carries: OTLP sanitizes an attribute key
        // before it becomes a label, so two mapped names that differ only in
        // characters the sanitizer rewrites are one label, not two, and a name
        // that sanitizes to `__name__` or `le` collides with a synthesized one
        // however it was spelled.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for label in &self.labels {
            if label.name.is_empty() {
                return Err(LoadError::Setup(
                    "--mapping [[metrics.label]] has an empty name".to_string(),
                ));
            }
            let name = sanitize_label_name(label.name.clone());
            if name.len() > limits.max_label_name_len {
                return Err(LoadError::Setup(format!(
                    "--mapping [[metrics.label]] name {:?} is {} bytes, more than the label-name \
                     limit of {}",
                    label.name,
                    name.len(),
                    limits.max_label_name_len
                )));
            }
            if name == METRIC_NAME_LABEL {
                return Err(LoadError::Setup(format!(
                    "--mapping [[metrics.label]] maps {:?}, which becomes {METRIC_NAME_LABEL:?} \
                     and carries the metric name. Use name or name_column instead.",
                    label.name
                )));
            }
            if self.is_histogram() && name == LE_LABEL {
                return Err(LoadError::Setup(format!(
                    "--mapping [[metrics.label]] maps {:?}, which becomes {LE_LABEL:?}, the label \
                     the classic-histogram explosion synthesizes per bucket. Two labels of the \
                     same name cannot exist on one series.",
                    label.name
                )));
            }
            if !seen.insert(name.clone()) {
                return Err(LoadError::Setup(format!(
                    "--mapping [[metrics.label]] declares {:?} twice (label names are compared \
                     after the OTLP sanitizer rewrites them to {name:?}); label names are unique \
                     on a series",
                    label.name
                )));
            }
        }

        if self.is_histogram() && self.kind.is_some() {
            return Err(LoadError::Setup(
                "--mapping [metrics] sets kind together with [metrics.histogram]. A classic \
                 histogram has no monotonic form in OTLP: every series it explodes into \
                 (_bucket, _sum, _count) is non-monotonic and its family name takes no _total \
                 suffix, so kind here would name a behaviour the load cannot produce. Remove it."
                    .to_string(),
            ));
        }

        if let Some(histogram) = &self.histogram {
            match histogram.histogram_type {
                None | Some(HistogramTypeArg::Classic) => {}
                Some(HistogramTypeArg::Native) => {
                    return Err(native_histogram_rejected("histogram.type = \"native\""));
                }
                Some(HistogramTypeArg::Exponential) => {
                    return Err(native_histogram_rejected(
                        "histogram.type = \"exponential\"",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Keys that name a span shape ADR-1751 decision 2 does not map (span events
/// and span links), refused by name before `deny_unknown_fields` can report
/// them as a generic typo.
const UNMAPPABLE_SPAN_KEYS: [&str; 10] = [
    "event",
    "events",
    "span_event",
    "span_events",
    "events_column",
    "link",
    "links",
    "span_link",
    "span_links",
    "links_column",
];

/// Refuse a spans section that names events or links at the section's top
/// level.
fn reject_unmappable_span_keys(value: &toml::Value) -> Result<(), LoadError> {
    let Some(table) = value.as_table() else {
        return Ok(());
    };
    for key in UNMAPPABLE_SPAN_KEYS {
        if table.contains_key(key) {
            return Err(span_shape_rejected(key));
        }
    }
    Ok(())
}

/// The refusal a mapping naming span events or links gets (ADR-1751
/// decision 2).
fn span_shape_rejected(what: &str) -> LoadError {
    LoadError::Setup(format!(
        "--mapping [spans] names {what}, which this version does not map (ADR-1751 decision 2: \
         native histograms, span events and span links are not mappable, and a mapping that names \
         them is rejected). The mappable span fields are trace_id, span_id, parent_span_id, name, \
         start_ts, end_ts, status_code, status_message, the resource_attribute and attribute \
         column lists, and attrs_map_column."
    ))
}

/// The `[spans]` section of a `--mapping` TOML (ADR-1751 decision 2).
///
/// ```toml
/// [spans]
/// trace_id_column       = "trace_id"   # 16-byte binary or 32-char hex string
/// span_id_column        = "span_id"    # 8-byte binary or 16-char hex string
/// parent_span_id_column = "parent"     # optional, same shape as span_id
/// name_column           = "name"
/// start_ts_column       = "start"
/// start_ts_unit         = "nanos"      # seconds | millis | micros | nanos
/// end_ts_column         = "end"
/// end_ts_unit           = "nanos"
/// status_code_column    = "status"     # optional, OTLP's 0/1/2 integer enum
/// status_message_column = "status_msg" # optional
/// attrs_map_column      = "attrs"      # optional, Map<Utf8, Utf8>
///
/// # Resource attributes: merged into every span's one attrs map. A key may
/// # not appear in both attribute lists (such a mapping is refused), so this
/// # merge never has a collision to resolve.
/// [[spans.resource_attribute]]
/// key = "service.name"
/// column = "svc"
/// type = "str"
///
/// # Span attributes.
/// [[spans.attribute]]
/// key = "http.method"
/// column = "method"
/// type = "str"
/// ```
///
/// A span has no stream identity (ADR-0041 routes by `trace_id`), so unlike
/// the logs section the two attribute lists differ only in merge precedence,
/// not in what they identify.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpansMapping {
    /// Source column carrying the 16-byte trace id.
    pub trace_id_column: String,
    /// Source column carrying the 8-byte span id.
    pub span_id_column: String,
    /// Source column carrying the parent's 8-byte span id. A null cell and an
    /// EMPTY cell (empty binary, empty string, or a zero-width fixed-size
    /// value) are both a root span, as OTLP's own empty `parent_span_id` is.
    #[serde(default)]
    pub parent_span_id_column: Option<String>,
    /// Source column carrying the span name.
    pub name_column: String,
    /// Source column carrying the span's start timestamp.
    pub start_ts_column: String,
    /// Unit of [`SpansMapping::start_ts_column`] when it is an integer
    /// column. A native Arrow `Timestamp` column carries its own unit and this
    /// is not applied again (see [`read_ts`]).
    pub start_ts_unit: TsUnit,
    /// Source column carrying the span's end timestamp.
    pub end_ts_column: String,
    /// Unit of [`SpansMapping::end_ts_column`], read like
    /// [`SpansMapping::start_ts_unit`]. Declared separately because a source
    /// file may well carry a second-granularity start beside a nanosecond
    /// duration-derived end.
    pub end_ts_unit: TsUnit,
    /// Source column carrying OTLP's status code as its integer enum (0
    /// unset, 1 ok, 2 error). Absent, or a null cell, is `Unset`. Read
    /// through `ravel_otlp`'s own mapping, so a value outside `0..=2`
    /// normalizes to `Unset` here exactly as it does on the OTLP path.
    #[serde(default)]
    pub status_code_column: Option<String>,
    /// Source column carrying the status message. An absent column, a null
    /// cell and an empty string all store no message, as an OTLP status with
    /// an empty `message` does.
    #[serde(default)]
    pub status_message_column: Option<String>,
    /// Columns merged into the span's `attrs` map with resource precedence.
    /// A key may not appear in both attribute lists, so that precedence never
    /// decides anything here; see [`SpansMapping::validate`].
    #[serde(default, rename = "resource_attribute")]
    pub resource_attributes: Vec<AttrMap>,
    /// Columns merged into the span's `attrs` map at span precedence.
    #[serde(default, rename = "attribute")]
    pub attributes: Vec<AttrMap>,
    /// One `Map<Utf8, Utf8>` column for the attributes neither list names
    /// (ADR-1751 decision 4). `ravel-cli export` writes the stored attributes
    /// the mapping does not name, reserved keys aside, into it, up to the
    /// loader's per-record attribute cap, and
    /// `ravel-cli load` merges its entries into the span's `attrs` at span
    /// precedence; see [`read_span_attrs_map`] for what a load refuses. A
    /// mapping that never sets it is unaffected.
    #[serde(default)]
    pub attrs_map_column: Option<String>,
}

impl SpansMapping {
    /// Every mapped attribute column, resource ones first, paired with the
    /// precedence set it belongs to.
    fn mapped_attributes(&self) -> impl Iterator<Item = (&AttrMap, AttrScope)> {
        self.resource_attributes
            .iter()
            .map(|a| (a, AttrScope::Resource))
            .chain(self.attributes.iter().map(|a| (a, AttrScope::Span)))
    }

    /// Every key a mapped attribute names, with the column it reads: the keys
    /// an `attrs_map_column` entry may not repeat. Constant for a load, so a
    /// load builds it once rather than once per batch.
    pub(super) fn mapped_keys(&self) -> MappedKeys {
        let mut mapped_keys = MappedKeys::new();
        for (spec, _) in self.mapped_attributes() {
            mapped_keys
                .entry(spec.key.clone())
                .or_insert_with(|| spec.column.clone());
        }
        mapped_keys
    }

    /// The checks a spans mapping fails before any Parquet byte is read.
    ///
    /// Everything here is a property of the mapping alone: an attribute key
    /// that is empty, over the OTLP key-length cap, reserved for a span field
    /// this version does not map, or declared twice. The duplicate check spans
    /// both lists, not each list on its own: `attrs` is one map per span and
    /// `ravel_rspan::merge_attrs` resolves a collision by resource precedence,
    /// so a key named in both lists would silently make the span column dead.
    /// Which column reached the record is exactly the kind of thing a mapping
    /// must not decide invisibly.
    ///
    /// Both attribute-count caps are here for the same reason: the mapping
    /// bounds every row, since a row carries at most one attribute per list
    /// entry, so a mapping within a cap can never produce a span over it. The
    /// span cap is the loader per-record cap standing in for OTLP's
    /// `max_attributes_per_span`; the resource cap is OTLP's own
    /// `max_resource_attributes`, which bounds how much gets merged into every
    /// span under the resource and which the OTLP path enforces by rejecting
    /// those spans.
    ///
    /// Attribute VALUE lengths, the span name length and the status message
    /// length are per-row and are checked as rows are decoded.
    pub fn validate(&self) -> Result<(), LoadError> {
        let limits = SpanIngestLimits::default();
        if self.attributes.len() > LOADER_MAX_ATTRIBUTES_PER_RECORD {
            return Err(LoadError::Setup(format!(
                "--mapping [spans] declares {} attribute columns, more than the loader per-record \
                 cap of {}",
                self.attributes.len(),
                LOADER_MAX_ATTRIBUTES_PER_RECORD
            )));
        }
        if self.resource_attributes.len() > limits.max_resource_attributes {
            return Err(LoadError::Setup(format!(
                "--mapping [spans] declares {} resource_attribute columns, more than the OTLP \
                 per-resource cap of {}",
                self.resource_attributes.len(),
                limits.max_resource_attributes
            )));
        }
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (attr, scope) in self.mapped_attributes() {
            let list = scope.list_name();
            if attr.key.is_empty() {
                return Err(LoadError::Setup(format!(
                    "--mapping [[spans.{list}]] has an empty key"
                )));
            }
            if attr.key.len() > limits.max_attribute_key_len {
                return Err(LoadError::Setup(format!(
                    "--mapping [[spans.{list}]] key {:?} is {} bytes, more than the \
                     attribute-key limit of {}",
                    attr.key,
                    attr.key.len(),
                    limits.max_attribute_key_len
                )));
            }
            // The OTLP path strips a sender's own attribute under a reserved
            // key and writes the span's real field there, so a mapped column
            // could only fabricate a field this version does not map.
            if is_reserved_key(&attr.key) {
                return Err(span_shape_rejected(&format!(
                    "the reserved attribute key {:?}",
                    attr.key
                )));
            }
            if !seen.insert(attr.key.as_str()) {
                return Err(LoadError::Setup(format!(
                    "--mapping [spans] declares the attribute key {:?} twice. A span carries one \
                     merged attrs map with unique keys, so one of the two columns would never \
                     reach the record.",
                    attr.key
                )));
            }
        }
        Ok(())
    }
}

/// Which precedence set a mapped span attribute belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttrScope {
    Resource,
    Span,
}

impl AttrScope {
    /// The mapping list this scope is spelled as.
    fn list_name(self) -> &'static str {
        match self {
            AttrScope::Resource => "resource_attribute",
            AttrScope::Span => "attribute",
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
