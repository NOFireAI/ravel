//! The metrics loader: metric rows, classic histogram grouping and expansion,
//! and load_metrics.

use super::*;

// ---------------------------------------------------------------------------
// Metrics load (ADR-1751 decisions 1 and 2)
//
// The same shape as the logs load above -- provision or validate the signal,
// build the router from the same `build_ingest_config`, write `WriteMode::
// Strict` batches through an in-flight window -- against
// `ravel_ingest::IngestRouter` and `ravel_otlp::NormalizedPoint`. What differs
// is normalisation: a metric point is a series id, a label set and one `f64`
// sample, and a classic histogram row set explodes into the
// Prometheus-convention `_bucket`/`_sum`/`_count` series.
//
// It deliberately does not reuse the logs path's columnar fast path or its K
// stride cursors. The columnar builder targets `ColumnarLogBatch`, which has
// no metrics counterpart, and the stride cursors interleave far-apart file
// regions inside one batch, which would break the contiguity a classic
// histogram's row group depends on.
// ---------------------------------------------------------------------------

/// Result of a successful (or partially-durable) metrics load.
#[derive(Debug, Clone, Default)]
pub struct MetricsLoadReport {
    /// Source rows whose points acked durable, in submission order.
    pub rows_processed: u64,
    /// `--skip-rows`, clamped to the file's total row count.
    pub rows_skipped: u64,
    /// `--skip-rows` as the operator gave it, before the clamp.
    pub skip_rows_requested: u64,
    /// The file's total row count, read from the Parquet footer.
    pub file_total_rows: u64,
    /// Points acked durable. This is NOT the row count: a classic-histogram
    /// data point of `n` finite bounds explodes into `n + 2` or `n + 3`
    /// points out of `n` rows, and it is the reachability signal a caller of
    /// the real entry point reads to prove the explosion ran rather than that
    /// its builder compiles.
    pub points_written: u64,
    /// Classic-histogram data points exploded, zero on a scalar mapping.
    pub histogram_points_exploded: u64,
    /// One token per shard acked, across every batch, in submission order.
    pub tokens: Vec<CommitToken>,
    pub elapsed: Duration,
}

impl MetricsLoadReport {
    /// Distinct commit tokens, which is the number of RSEG objects the load
    /// wrote. Same derivation as [`LoadReport::objects_written`].
    pub fn objects_written(&self) -> usize {
        let mut seen = std::collections::HashSet::new();
        self.tokens
            .iter()
            .filter(|t| seen.insert(t.encode()))
            .count()
    }
}

/// Resolved column indices for the mapped fields of one metrics batch, plus
/// the parts of the name/label normalization that depend on the mapping alone
/// and are therefore resolved once per batch rather than once per row.
struct MetricsColumnIndex {
    /// `None` when the mapping carries a literal `name`.
    name: Option<usize>,
    /// The normalized family name of a literal `name`, `None` when the name
    /// comes from a column (it is then normalized per row).
    literal_family_name: Option<String>,
    value: usize,
    ts: usize,
    /// One `(sanitized label name, column index)` per `[[metrics.label]]`, in
    /// mapping order.
    labels: Vec<(String, usize)>,
    histogram: Option<HistogramColumnIndex>,
    /// The metric kind and monotonicity the family name is suffixed under.
    kind: MetricKind,
    is_monotonic_sum: bool,
    /// The batch's columns with every mapped dictionary column resolved once.
    columns: ResolvedColumns,
}

struct HistogramColumnIndex {
    le: usize,
    sum: usize,
    count: usize,
}

impl MetricsColumnIndex {
    fn resolve(
        batch: &RecordBatch,
        mapping: &MetricsMapping,
    ) -> Result<MetricsColumnIndex, String> {
        let schema = batch.schema();
        let idx = |name: &str| -> Result<usize, String> {
            schema
                .index_of(name)
                .map_err(|_| format!("mapped column {name:?} is not present in the Parquet file"))
        };
        let histogram = match &mapping.histogram {
            Some(h) => Some(HistogramColumnIndex {
                le: idx(&h.le_column)?,
                sum: idx(&h.sum_column)?,
                count: idx(&h.count_column)?,
            }),
            None => None,
        };
        let (kind, is_monotonic_sum) = mapping.metric_kind();
        let limits = IngestLimits::default();
        let literal_family_name = match &mapping.name {
            Some(literal) => Some(
                normalized_family_name(literal, mapping, kind, is_monotonic_sum, &limits)
                    .map_err(|e| format!("--mapping [metrics] name is unusable: {e}"))?,
            ),
            None => None,
        };
        let name = match &mapping.name_column {
            Some(c) => Some(idx(c)?),
            None => None,
        };
        let value = idx(&mapping.value_column)?;
        let ts = idx(&mapping.ts_column)?;
        let labels = mapping
            .sanitized_label_names()
            .into_iter()
            .zip(&mapping.labels)
            .map(|(name, l)| Ok((name, idx(&l.column)?)))
            .collect::<Result<Vec<_>, String>>()?;
        // The name and the label columns are the two a row reader reads a
        // string out of; the value, ts and histogram columns are numeric.
        let columns = ResolvedColumns::resolve(
            batch,
            name.into_iter().chain(labels.iter().map(|(_, i)| *i)),
        )?;
        Ok(MetricsColumnIndex {
            name,
            literal_family_name,
            value,
            ts,
            labels,
            histogram,
            kind,
            is_monotonic_sum,
            columns,
        })
    }

    /// The array and index the row readers read the cell at (`row`, column
    /// `i`) from, as [`ResolvedColumns::cell`].
    fn cell<'a>(
        &'a self,
        batch: &'a RecordBatch,
        i: usize,
        row: usize,
    ) -> Result<(&'a ArrayRef, usize), String> {
        self.columns.cell(batch, i, row)
    }

    /// Read the cell at (`row`, column `i`) with `read`, as
    /// [`ResolvedColumns::read`].
    fn read<T>(
        &self,
        batch: &RecordBatch,
        i: usize,
        row: usize,
        read: impl FnOnce(&ArrayRef, usize) -> Result<T, String>,
    ) -> Result<T, String> {
        self.columns.read(batch, i, row, read)
    }
}

/// Put one raw metric name through the OTLP name pipeline: the raw-length cap,
/// `sanitize_metric_name`, the empty check, then the ADR-0085 decision 2
/// suffix pass in `prometheus_family_name`.
///
/// The order is `ravel_otlp::normalize`'s, and it is the order that matters:
/// the length cap is applied to the name as it arrives (as OTLP applies it to
/// `Metric::name`), and the suffixes are appended after sanitization, so a
/// mapped name and the OTLP metric it mirrors reach `SeriesId::compute` as the
/// same string.
pub(crate) fn normalized_family_name(
    raw: &str,
    mapping: &MetricsMapping,
    kind: MetricKind,
    is_monotonic_sum: bool,
    limits: &IngestLimits,
) -> Result<String, String> {
    if raw.len() > limits.max_metric_name_len {
        return Err(format!(
            "metric name {raw:?} is {} bytes, more than the limit of {}",
            raw.len(),
            limits.max_metric_name_len
        ));
    }
    let sanitized = sanitize_metric_name(raw);
    if sanitized.is_empty() {
        return Err("metric name is empty".to_string());
    }
    Ok(prometheus_family_name(
        &sanitized,
        mapping.unit(),
        kind,
        is_monotonic_sum,
    ))
}

/// One decoded source row, before it becomes a point or joins a histogram
/// group. Labels are the mapped ones only: `__name__` and the synthesized
/// `le` are added when the point is built.
struct MetricRow {
    name: String,
    labels: Vec<Label>,
    ts_ns: i64,
    payload: RowPayload,
}

/// What one source row carries beside its identity: a scalar sample, or one
/// bucket of a classic-histogram data point.
enum RowPayload {
    Scalar(f64),
    Bucket(BucketRow),
}

/// The classic-histogram fields of one source row.
struct BucketRow {
    /// This bucket's explicit (finite) upper bound.
    le: f64,
    /// This bucket's OWN count, from the `value` column (OTLP
    /// `bucket_counts[i]`). Read as an integer, never through `f64`: a count
    /// above 2^53 must survive, and OTLP carries these as `u64`.
    own_count: u64,
    /// The data point's `sum`, `None` for a null cell (no `_sum` series).
    sum: Option<f64>,
    /// The data point's total count: the `+Inf` bucket's value and `_count`.
    count: u64,
}

/// Read one numeric cell as `f64`, accepting float and integer columns.
///
/// An integer wider than 2^53 rounds to the nearest representable `f64`, the
/// same loss `ravel_otlp::normalize` documents for an OTLP `as_int` value:
/// the segment format stores `f64` only, so there is nowhere else for the
/// value to go.
fn read_metric_number(arr: &ArrayRef, row: usize) -> Result<Option<f64>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Float32 | DataType::Float64 => read_f64(arr, row),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(read_i64(arr, row)?.map(|v| v as f64)),
        other => Err(format!("expected a numeric column, found {other:?}")),
    }
}

/// Read one cell as a Prometheus label value. A label value is a string, so a
/// numeric or boolean column is stringified: floats through
/// [`format_float`], the same Go-compatible spelling the `le` label uses, so
/// two columns carrying the same number never produce two series.
fn read_label_value(arr: &ArrayRef, row: usize) -> Result<Option<String>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _) => read_string(arr, row),
        DataType::Boolean => Ok(read_bool(arr, row)?.map(|b| b.to_string())),
        DataType::Float32 | DataType::Float64 => Ok(read_f64(arr, row)?.map(format_float)),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(read_i64(arr, row)?.map(|v| v.to_string())),
        other => Err(format!(
            "expected a string, numeric or boolean label column, found {other:?}"
        )),
    }
}

/// A bucket or data-point count read from a numeric cell: a non-negative
/// integer.
///
/// An integer column is read as an integer, never through `f64`: OTLP carries
/// `bucket_counts` and `count` as `u64`, and a round trip through `f64` would
/// silently move a count above 2^53 to the nearest representable value. A
/// float column is accepted only when its value is exactly integral, so a
/// fractional count is refused rather than truncated.
fn read_count(arr: &ArrayRef, row: usize) -> Result<Option<u64>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::UInt64 => Ok(Some(downcast::<UInt64Array>(arr)?.value(row))),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => {
            let v = read_i64(arr, row)?.ok_or_else(|| "count column is null".to_string())?;
            u64::try_from(v)
                .map(Some)
                .map_err(|_| format!("expected a non-negative whole-number count, found {v}"))
        }
        DataType::Float32 | DataType::Float64 => {
            let v = read_f64(arr, row)?.ok_or_else(|| "count column is null".to_string())?;
            exact_count(v).map(Some)
        }
        other => Err(format!("expected a numeric column, found {other:?}")),
    }
}

/// Decode one source row against the mapping, applying the kept admission
/// checks (future skew, the metric-name and label length caps, the loader
/// label cap). `Err` carries a per-row rejection reason.
fn build_metric_row(
    batch: &RecordBatch,
    cols: &MetricsColumnIndex,
    mapping: &MetricsMapping,
    limits: &IngestLimits,
    now_ns: i64,
    row: usize,
) -> Result<MetricRow, String> {
    let (ts_col, ts_at) = cols.cell(batch, cols.ts, row)?;
    let raw_ts = read_ts(ts_col, ts_at, mapping.ts_unit)?
        .ok_or_else(|| format!("ts column {:?} is null", mapping.ts_column))?;
    if raw_ts < 0 {
        return Err(negative_ts_rejection(
            raw_ts,
            ts_col.data_type(),
            mapping.ts_unit,
        ));
    }

    // Kept: the future-skew bound, at the metrics OTLP limit. The past-lag
    // check is deliberately omitted (ADR-0089 relaxation, widened to every
    // signal by ADR-1751 decision 1).
    let skew_ns = raw_ts.saturating_sub(now_ns);
    if skew_ns > limits.max_future_skew_ns {
        return Err(format!(
            "timestamp is {skew_ns} ns ahead of load time, more than the max future skew of {} ns",
            limits.max_future_skew_ns
        ));
    }

    // The name goes through OTLP's own pipeline (length cap, sanitize, empty
    // check, unit and `_total` suffixes), so a metric loaded here lands on the
    // same family name, and therefore the same SeriesId, as the same metric
    // admitted over OTLP.
    let name = match (cols.name, &cols.literal_family_name) {
        (Some(i), _) => {
            let raw = cols.read(batch, i, row, read_string)?.ok_or_else(|| {
                format!(
                    "metric name column {:?} is null",
                    mapping.name_column.as_deref().unwrap_or_default()
                )
            })?;
            normalized_family_name(&raw, mapping, cols.kind, cols.is_monotonic_sum, limits)?
        }
        (None, Some(literal)) => literal.clone(),
        // `MetricsMapping::validate` refuses a mapping with neither, before
        // any row is read.
        (None, None) => return Err("mapping declares no metric name".to_string()),
    };

    // A null label cell omits that label, and so does an EMPTY one: OTLP drops
    // an empty attribute value before the label set is built (ADR-0038,
    // `push_checked`), so `{job=""}` and `{}` are one series there and must be
    // one series here too.
    let mut labels: Vec<Label> = Vec::with_capacity(cols.labels.len());
    for (name, col_idx) in &cols.labels {
        if let Some(value) = cols.read(batch, *col_idx, row, read_label_value)? {
            if value.is_empty() {
                continue;
            }
            check_label(name, &value, limits)?;
            labels.push(Label {
                name: name.clone(),
                value,
            });
        }
    }
    // The loader label cap stands in for OTLP's `max_attributes_per_point`
    // (ADR-1751 decision 1), rejected rather than silently truncated.
    if labels.len() > LOADER_MAX_ATTRIBUTES_PER_RECORD {
        return Err(format!(
            "point has {} labels, more than the loader per-record cap of {}",
            labels.len(),
            LOADER_MAX_ATTRIBUTES_PER_RECORD
        ));
    }

    let payload = match &cols.histogram {
        None => RowPayload::Scalar(
            cols.read(batch, cols.value, row, read_metric_number)?
                .ok_or_else(|| format!("value column {:?} is null", mapping.value_column))?,
        ),
        Some(h) => {
            let le = cols
                .read(batch, h.le, row, read_metric_number)?
                .ok_or_else(|| "histogram le column is null".to_string())?;
            if !le.is_finite() {
                // Matches `Rejection::NonFiniteHistogramBound`: OTLP's
                // `explicit_bounds` carries finite bounds only, and the `+Inf`
                // bucket is synthesized from `count`, never mapped as a row.
                return Err(format!(
                    "histogram le is {le}, which is not a finite bucket bound. The +Inf bucket is \
                     synthesized from the count column and must not be a row of its own."
                ));
            }
            // With a histogram mapping the value column is this bucket's own
            // count, so it is read as a count, not as a sample value.
            let own_count = cols
                .read(batch, cols.value, row, read_count)
                .map_err(|e| {
                    format!(
                        "value column {:?} is this bucket's own count on a classic-histogram \
                         mapping: {e}",
                        mapping.value_column
                    )
                })?
                .ok_or_else(|| format!("value column {:?} is null", mapping.value_column))?;
            let sum = cols.read(batch, h.sum, row, read_metric_number)?;
            let count = cols
                .read(batch, h.count, row, read_count)?
                .ok_or_else(|| "histogram count column is null".to_string())?;
            RowPayload::Bucket(BucketRow {
                le,
                own_count,
                sum,
                count,
            })
        }
    };

    Ok(MetricRow {
        name,
        labels,
        ts_ns: raw_ts,
        payload,
    })
}

/// Kept length caps for one label, at the metrics OTLP limits.
fn check_label(name: &str, value: &str, limits: &IngestLimits) -> Result<(), String> {
    if name.len() > limits.max_label_name_len {
        return Err(format!(
            "label name {name:?} is {} bytes, more than the limit of {}",
            name.len(),
            limits.max_label_name_len
        ));
    }
    if value.len() > limits.max_label_value_len {
        return Err(format!(
            "label {name:?} value is {} bytes, more than the limit of {}",
            value.len(),
            limits.max_label_value_len
        ));
    }
    Ok(())
}

/// Build one [`NormalizedPoint`], the metrics loader's counterpart of
/// `ravel_otlp::normalize`'s private `finish_point`: base labels plus
/// `__name__`, plus at most one synthesized label (`le`), through
/// [`LabelSet::new`] and [`SeriesId::compute`].
#[allow(clippy::too_many_arguments)]
fn metrics_point(
    tenant: &TenantId,
    base_labels: &[Label],
    metric_name: &str,
    extra_label: Option<(&str, String)>,
    ts_ns: i64,
    value: f64,
    is_monotonic_sum: bool,
    limits: &IngestLimits,
) -> Result<NormalizedPoint, String> {
    // No metric-name check here: the caller has already put the family name
    // through `normalized_family_name`, and the explosion's `_bucket`/`_sum`/
    // `_count` suffixes are appended after OTLP's own length cap the same way,
    // so re-checking would reject a name OTLP admits.
    let mut labels = Vec::with_capacity(base_labels.len() + 2);
    labels.extend_from_slice(base_labels);
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric_name.to_string(),
    });
    if let Some((name, value)) = extra_label {
        check_label(name, &value, limits)?;
        labels.push(Label {
            name: name.to_string(),
            value,
        });
    }
    let label_set = LabelSet::new(labels)
        .map_err(|e| format!("labels are not a valid series label set: {e}"))?;
    let series_id = SeriesId::compute(tenant, metric_name, &label_set)
        .map_err(|e| format!("series identity could not be computed: {e}"))?;
    Ok(NormalizedPoint {
        series_id,
        labels: Arc::new(label_set),
        sample: Sample { ts_ns, value },
        is_monotonic_sum,
    })
}

/// One classic-histogram data point accumulated from a contiguous run of
/// source rows.
struct PendingHistogram {
    name: String,
    labels: Vec<Label>,
    ts_ns: i64,
    /// The finite explicit bounds, in row order.
    bounds: Vec<f64>,
    /// Each bound's OWN bucket count, in row order (OTLP `bucket_counts`).
    counts: Vec<u64>,
    /// The data point's `sum`, from its rows' `sum` column.
    sum: Option<f64>,
    /// The data point's total count, from its rows' `count` column.
    count: u64,
    /// File-absolute index of the group's first row, so a rejection raised
    /// when the group closes points at a row the operator can find.
    first_row: u64,
    /// Source rows folded into this group so far, possibly spanning a batch
    /// boundary. These rows are not durable until the write carrying this
    /// group's points acks, so they are credited to that write and to no
    /// earlier one.
    rows: u64,
}

impl PendingHistogram {
    /// Is this row another bucket of this same data point? A data point is one
    /// metric name, one label set and one `ts`.
    fn accepts(&self, name: &str, ts_ns: i64, labels: &[Label]) -> bool {
        self.name == name && self.ts_ns == ts_ns && self.labels == labels
    }
}

/// What closing zero or more histogram groups produced: their points, and the
/// source rows those points were built from.
#[derive(Default)]
struct Closed {
    points: Vec<NormalizedPoint>,
    rows: u64,
}

/// Groups contiguous classic-histogram rows into data points and explodes
/// each into its Prometheus-convention series.
struct HistogramGrouper {
    tenant: TenantId,
    pending: Option<PendingHistogram>,
    /// Every group already closed, keyed by its base series id and `ts`. A
    /// key that comes back means the file interleaves two data points'
    /// buckets, which no contiguous-run grouping can read correctly, so it is
    /// refused rather than exploded twice. This costs one 24-byte key per
    /// data point for the length of the load.
    closed: std::collections::HashSet<(SeriesId, i64)>,
    exploded: u64,
}

impl HistogramGrouper {
    fn new(tenant: TenantId) -> Self {
        HistogramGrouper {
            tenant,
            pending: None,
            closed: std::collections::HashSet::new(),
            exploded: 0,
        }
    }

    /// Fold one row into the open group, returning the points of the group it
    /// closed and the number of SOURCE ROWS those points came from (both empty
    /// and zero while the group is still accumulating). `file_row` is the
    /// row's file-absolute index, used for the rejection the caller reports.
    ///
    /// The row count is the group's, not the batch's: a group opened in an
    /// earlier batch carries its earlier rows here, and the rows of a group
    /// this row has just opened stay uncounted until that group closes. That
    /// is what keeps `rows_written`, and the resume offset derived from it,
    /// equal to a whole number of exploded data points.
    fn push(
        &mut self,
        row: MetricRow,
        file_row: u64,
        limits: &IngestLimits,
    ) -> Result<Closed, (u64, String)> {
        let MetricRow {
            name,
            labels,
            ts_ns,
            payload,
        } = row;
        let RowPayload::Bucket(bucket) = payload else {
            return Err((
                file_row,
                "internal error: a histogram mapping produced a row with no bucket".to_string(),
            ));
        };
        let mut closed = Closed::default();
        match &mut self.pending {
            Some(open) if open.accepts(&name, ts_ns, &labels) => {
                // Every row of one data point carries the SAME sum and count:
                // they describe the point, not the bucket. Compared by bit
                // pattern, so a NaN sum is compared like any other payload.
                if open.count != bucket.count {
                    return Err((
                        file_row,
                        format!(
                            "histogram count is {} here but {} on the first row of the same data \
                             point (row {}); the count column carries the DATA POINT's total, so \
                             it must repeat on every one of its bucket rows",
                            bucket.count, open.count, open.first_row
                        ),
                    ));
                }
                if open.sum.map(f64::to_bits) != bucket.sum.map(f64::to_bits) {
                    return Err((
                        file_row,
                        format!(
                            "histogram sum differs from the first row of the same data point (row \
                             {}); the sum column carries the DATA POINT's sum, so it must repeat \
                             on every one of its bucket rows",
                            open.first_row
                        ),
                    ));
                }
                // Refused here rather than at close so a mis-declared mapping
                // that makes the whole file one data point is not buffered
                // whole before the limit is checked.
                if open.bounds.len() >= limits.max_histogram_buckets {
                    return Err((
                        open.first_row,
                        too_many_bounds(
                            &open.name,
                            open.ts_ns,
                            open.bounds.len() + 1,
                            limits.max_histogram_buckets,
                        ),
                    ));
                }
                open.bounds.push(bucket.le);
                open.counts.push(bucket.own_count);
                open.rows += 1;
            }
            _ => {
                if let Some(open) = self.pending.take() {
                    closed = self.close(open, limits)?;
                }
                self.pending = Some(PendingHistogram {
                    name,
                    labels,
                    ts_ns,
                    bounds: vec![bucket.le],
                    counts: vec![bucket.own_count],
                    sum: bucket.sum,
                    count: bucket.count,
                    first_row: file_row,
                    rows: 1,
                });
            }
        }
        Ok(closed)
    }

    /// Close whatever group is open at end of input.
    fn finish(&mut self, limits: &IngestLimits) -> Result<Closed, (u64, String)> {
        match self.pending.take() {
            Some(open) => self.close(open, limits),
            None => Ok(Closed::default()),
        }
    }

    fn close(
        &mut self,
        group: PendingHistogram,
        limits: &IngestLimits,
    ) -> Result<Closed, (u64, String)> {
        let first_row = group.first_row;
        let key = histogram_group_key(&self.tenant, &group).map_err(|e| (first_row, e))?;
        if !self.closed.insert(key) {
            return Err((
                first_row,
                format!(
                    "the rows of the {:?} data point at ts {} are not contiguous: a group with \
                     this identity was already closed earlier in the file. Sort the input by \
                     metric name, labels and ts so every data point's bucket rows sit together.",
                    group.name, group.ts_ns
                ),
            ));
        }
        let points =
            explode_classic_histogram(&self.tenant, &group, limits).map_err(|e| (first_row, e))?;
        self.exploded += 1;
        Ok(Closed {
            points,
            rows: group.rows,
        })
    }
}

/// The base series identity of a histogram data point (its labels without the
/// synthesized `le`, under its unsuffixed name), paired with its `ts`. This
/// is the grouping key, computed once per group rather than once per row.
fn histogram_group_key(
    tenant: &TenantId,
    group: &PendingHistogram,
) -> Result<(SeriesId, i64), String> {
    let mut labels = group.labels.clone();
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: group.name.clone(),
    });
    let label_set = LabelSet::new(labels)
        .map_err(|e| format!("labels are not a valid series label set: {e}"))?;
    let series_id = SeriesId::compute(tenant, &group.name, &label_set)
        .map_err(|e| format!("series identity could not be computed: {e}"))?;
    Ok((series_id, group.ts_ns))
}

/// A count read from a float cell: a non-negative whole number representable
/// as `u64`.
///
/// The upper bound is `>=`, not `>`: `u64::MAX as f64` rounds UP to 2^64,
/// which is one past the largest `u64`, so a cell holding exactly 2^64 passes
/// a `>` test and then saturates to `u64::MAX` on the cast. Refused instead.
fn exact_count(value: f64) -> Result<u64, String> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value >= u64::MAX as f64 {
        return Err(format!(
            "expected a non-negative whole-number count that fits in u64, found {value}"
        ));
    }
    Ok(value as u64)
}

/// The refusal for a data point with more explicit bounds than
/// `IngestLimits::max_histogram_buckets`.
fn too_many_bounds(name: &str, ts_ns: i64, bounds: usize, limit: usize) -> String {
    format!(
        "the {name:?} data point at ts {ts_ns} has {bounds} explicit bounds, more than the limit \
         of {limit}"
    )
}

/// Explode one classic-histogram data point into its Prometheus-convention
/// series: one `{name}_bucket{le=<bound>}` per explicit bound plus
/// `{name}_bucket{le="+Inf"}` (= the point's count), `{name}_sum` when the
/// `sum` column was not null, and `{name}_count`.
///
/// Mirrors `ravel_otlp::normalize`'s private `explode_histogram` (which takes
/// an OTLP `HistogramDataPoint` and applies the past-lag check this path
/// relaxes, so it cannot be called directly), including its check order,
/// its accumulation of per-bucket counts into cumulative values, its use of
/// the point's raw count for BOTH the `+Inf` bucket and `_count`, and its
/// atomic-rejection contract: an `Err` means none of this point's series were
/// admitted, never a partial set (ADR-0016). `crates/ravel-otap/src/
/// normalize.rs`'s `explode_histogram_point` is the same mirror for the OTAP
/// surface. The `le` value goes through
/// [`ravel_otlp::promcompat::format_float`], the shared formatter that makes
/// the same histogram admitted through any of these surfaces land on the same
/// `SeriesId`.
///
/// Two OTLP behaviours have no source here and are therefore absent rather
/// than reimplemented: the stale marker (`DataPointFlags`, which a Parquet
/// row carries no equivalent of) and exemplars (ADR-1751 decision 2 maps
/// none).
fn explode_classic_histogram(
    tenant: &TenantId,
    group: &PendingHistogram,
    limits: &IngestLimits,
) -> Result<Vec<NormalizedPoint>, String> {
    if !group.bounds.windows(2).all(|w| w[0] < w[1]) {
        return Err(format!(
            "the {:?} data point at ts {} has bucket bounds that do not strictly increase in row \
             order ({:?}); sort each data point's rows by le",
            group.name, group.ts_ns, group.bounds
        ));
    }
    if group.bounds.len() > limits.max_histogram_buckets {
        return Err(too_many_bounds(
            &group.name,
            group.ts_ns,
            group.bounds.len(),
            limits.max_histogram_buckets,
        ));
    }

    let bucket_name = format!("{}_bucket", group.name);
    let mut series = Vec::with_capacity(group.bounds.len() + 3);
    let mut cumulative: u64 = 0;
    for (bound, count) in group.bounds.iter().zip(&group.counts) {
        cumulative = cumulative
            .checked_add(*count)
            .ok_or_else(|| "histogram bucket counts overflow u64".to_string())?;
        series.push(metrics_point(
            tenant,
            &group.labels,
            &bucket_name,
            Some((LE_LABEL, format_float(*bound))),
            group.ts_ns,
            cumulative as f64,
            false,
            limits,
        )?);
    }

    // The +Inf bucket and _count both use the point's raw count directly, not
    // the accumulated bucket sum (matches the collector's mapping and
    // ravel_otlp::explode_histogram).
    let count_value = group.count as f64;
    series.push(metrics_point(
        tenant,
        &group.labels,
        &bucket_name,
        Some((LE_LABEL, "+Inf".to_string())),
        group.ts_ns,
        count_value,
        false,
        limits,
    )?);

    if let Some(sum) = group.sum {
        series.push(metrics_point(
            tenant,
            &group.labels,
            &format!("{}_sum", group.name),
            None,
            group.ts_ns,
            sum,
            false,
            limits,
        )?);
    }

    series.push(metrics_point(
        tenant,
        &group.labels,
        &format!("{}_count", group.name),
        None,
        group.ts_ns,
        count_value,
        false,
        limits,
    )?);

    Ok(series)
}

/// The decode/build state the metrics loader shuttles into and back out of
/// each batch's `spawn_blocking` task: the single sequential cursor, plus the
/// histogram grouper whose open group can span a batch boundary.
struct MetricsDecodeState {
    cursor: CursorState,
    grouper: Option<HistogramGrouper>,
    /// `--skip-rows`, applied against each span's own file-absolute base, as
    /// [`collect_spans`](super::logs::collect_spans) applies it on the logs path.
    skip_rows: u64,
}

/// One batch's decode outcome.
enum MetricsDecoded {
    /// Points built from this batch, the source rows THOSE POINTS came from,
    /// and whether the input is exhausted.
    ///
    /// `rows` is not the batch's row count on a histogram mapping: it is the
    /// row count of the groups this batch closed, which can include rows read
    /// in an earlier batch and excludes the rows of a group this batch only
    /// opened. Those uncounted rows are credited to the later write that
    /// carries their group's points, so every ack advances the resume offset
    /// to a group boundary and never past one.
    Batch {
        points: Vec<NormalizedPoint>,
        rows: u64,
        done: bool,
    },
    /// The batch failed to read from Parquet or to resolve against the
    /// mapping.
    Failed(String),
    /// A row failed a kept admission check, at its FILE-absolute index.
    Rejected { row: u64, reason: String },
}

/// Decode and build one metrics batch from the sequential cursor.
#[allow(clippy::too_many_arguments)]
fn decode_metrics_batch(
    state: &mut MetricsDecodeState,
    tenant: &TenantId,
    mapping: &MetricsMapping,
    limits: &IngestLimits,
    now_ns: i64,
    batch_rows: usize,
    is_monotonic_sum: bool,
) -> MetricsDecoded {
    let taken = match cursor_take(&mut state.cursor, batch_rows) {
        Ok(taken) => taken,
        Err(reason) => return MetricsDecoded::Failed(reason),
    };
    let Some((batch, file_base)) = taken else {
        // Input exhausted: close whatever histogram group is still open, so
        // the last data point in the file is exploded rather than dropped.
        let closed = match state.grouper.as_mut() {
            Some(grouper) => match grouper.finish(limits) {
                Ok(closed) => closed,
                Err((row, reason)) => return MetricsDecoded::Rejected { row, reason },
            },
            None => Closed::default(),
        };
        return MetricsDecoded::Batch {
            points: closed.points,
            rows: closed.rows,
            done: true,
        };
    };

    // `--skip-rows` by file-absolute position, before mapping sees a row.
    let (batch, file_base) = {
        let end = file_base + batch.num_rows() as u64;
        if end <= state.skip_rows {
            return MetricsDecoded::Batch {
                points: Vec::new(),
                rows: 0,
                done: false,
            };
        }
        if file_base < state.skip_rows {
            let cut = (state.skip_rows - file_base) as usize;
            (
                batch.slice(cut, batch.num_rows() - cut),
                file_base + cut as u64,
            )
        } else {
            (batch, file_base)
        }
    };

    let cols = match MetricsColumnIndex::resolve(&batch, mapping) {
        Ok(cols) => cols,
        Err(reason) => return MetricsDecoded::Failed(reason),
    };

    match build_batch_points(
        &batch,
        &cols,
        file_base,
        tenant,
        mapping,
        limits,
        now_ns,
        is_monotonic_sum,
        state.grouper.as_mut(),
    ) {
        Ok((points, rows)) => MetricsDecoded::Batch {
            points,
            rows,
            done: false,
        },
        Err((row, reason)) => MetricsDecoded::Rejected { row, reason },
    }
}

/// Turn one resolved Arrow batch into points, and report the SOURCE ROWS those
/// points came from.
///
/// Split out of [`decode_metrics_batch`] so the mapping-to-points half can be
/// driven directly over a hand-built `RecordBatch`, without a Parquet file and
/// a read cursor in the way. `Err` is `(file-absolute row, reason)`.
#[allow(clippy::too_many_arguments)]
fn build_batch_points(
    batch: &RecordBatch,
    cols: &MetricsColumnIndex,
    file_base: u64,
    tenant: &TenantId,
    mapping: &MetricsMapping,
    limits: &IngestLimits,
    now_ns: i64,
    is_monotonic_sum: bool,
    mut grouper: Option<&mut HistogramGrouper>,
) -> Result<(Vec<NormalizedPoint>, u64), (u64, String)> {
    let mut points = Vec::with_capacity(batch.num_rows());
    // Source rows the points above were built from. On a scalar mapping that
    // is one per row; on a histogram mapping it is the row count of the groups
    // this batch closed (see [`MetricsDecoded::Batch`]).
    let mut rows: u64 = 0;
    for row in 0..batch.num_rows() {
        let file_row = file_base + row as u64;
        let decoded = build_metric_row(batch, cols, mapping, limits, now_ns, row)
            .map_err(|reason| (file_row, reason))?;
        match grouper.as_deref_mut() {
            Some(grouper) => {
                let closed = grouper.push(decoded, file_row, limits)?;
                points.extend(closed.points);
                rows += closed.rows;
            }
            None => {
                let RowPayload::Scalar(value) = decoded.payload else {
                    return Err((
                        file_row,
                        "internal error: a scalar mapping produced a bucket row".to_string(),
                    ));
                };
                points.push(
                    metrics_point(
                        tenant,
                        &decoded.labels,
                        &decoded.name,
                        None,
                        decoded.ts_ns,
                        value,
                        is_monotonic_sum,
                        limits,
                    )
                    .map_err(|reason| (file_row, reason))?,
                );
                rows += 1;
            }
        }
    }
    Ok((points, rows))
}

/// Bulk-import `parquet_path` into `tenant`'s metrics signal (ADR-1751
/// decision 1).
///
/// The same contract as [`load`] on the logs side: the shard count is
/// validated against (or, for a fresh signal, written to) the durable
/// provisioning record through [`validate_or_adopt`]; the router is built
/// from the same [`build_ingest_config`]; every batch is a
/// [`WriteMode::Strict`] write whose ack means durable; and a failure
/// mid-file is a genuine partial load whose durable commit tokens are
/// reported rather than swallowed.
///
/// `now_ns` anchors the future-skew check only. Bucketing is by the router's
/// own clock (load-time wall clock), so a thirty-day-old sample lands in
/// today's ingest hour and is reached by every later query's listing window,
/// whose upper bound is `now` (ADR-1751 Context, ADR-0089's discoverability
/// argument).
#[allow(clippy::too_many_arguments)]
pub async fn load_metrics(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &MetricsMapping,
    shards: u32,
    batch_rows: usize,
    skip_rows: u64,
    pipeline_depth: usize,
    max_inflight_flushes: u32,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    now_ns: i64,
    clock: Arc<dyn Clock>,
) -> Result<MetricsLoadReport, LoadError> {
    // Same operator-facing lever guards as the logs path: a value that cannot
    // express what the flag means is rejected, never silently clamped.
    if batch_rows == 0 {
        return Err(LoadError::Setup(
            "--batch-rows must be at least 1 (each batch is one Strict flush per shard); 0 was \
             given"
                .to_string(),
        ));
    }
    if pipeline_depth == 0 {
        return Err(LoadError::Setup(
            "--pipeline-depth must be at least 1 (the number of concurrent in-flight writes); 0 \
             was given"
                .to_string(),
        ));
    }
    if max_inflight_flushes == 0 {
        return Err(LoadError::Setup(
            "--max-inflight-flushes must be at least 1 (the number of flushes one shard may have \
             in flight at once); 0 would deadlock every flush, since a shard could never acquire \
             a permit to run one"
                .to_string(),
        ));
    }
    if target_bytes == 0 {
        return Err(LoadError::Setup(
            "--target-bytes must be at least 1 (1 flushes every batch as its own object); 0 was \
             given"
                .to_string(),
        ));
    }
    mapping.validate()?;

    let limits = IngestLimits::default();
    let tenant_id = TenantId::new(tenant);

    // Provision or validate the METRICS signal, the same first-touch path
    // `services/ravel-server` runs and the same one the logs load takes for
    // `Signal::Logs`.
    validate_or_adopt(
        store.as_ref(),
        &tenant_id.hash(),
        Signal::Metrics,
        shards,
        now_ns,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .map_err(|e| {
        LoadError::Setup(format!(
            "shard-count provisioning check failed for tenant {tenant:?} \
             (configured --shards {shards}): {e}"
        ))
    })?;

    let router = Arc::new(IngestRouter::new(
        build_ingest_config(shards, target_bytes, max_inflight_flushes, max_flush_delay),
        Arc::clone(&store),
        Signal::Metrics,
        clock,
    ));
    let ack_deadline = write_ack_deadline(max_flush_delay);

    let input = FileInput { path: parquet_path };
    let metadata = read_input_metadata(&input)?;
    let row_group_lens = row_group_row_counts(&metadata);
    // One sequential cursor, not the logs path's K stride cursors: a classic
    // histogram's data point is a CONTIGUOUS run of rows, and interleaving
    // far-apart file regions inside one batch would split every such run.
    let mut cursors = open_stride_cursors(
        &input,
        &metadata,
        &row_group_lens,
        1,
        batch_rows,
        batch_rows,
    )?;
    let Some(cursor) = cursors.pop() else {
        return Err(LoadError::Setup(
            "internal error: no read cursor was opened for the input file".to_string(),
        ));
    };

    let started = Instant::now();
    let mut report = MetricsLoadReport::default();
    let total_rows: u64 = row_group_lens.iter().sum();
    report.rows_skipped = skip_rows.min(total_rows);
    report.skip_rows_requested = skip_rows;
    report.file_total_rows = total_rows;

    let is_monotonic_sum = mapping.kind == Some(MetricKindArg::Counter);
    let mut state = MetricsDecodeState {
        cursor,
        grouper: mapping
            .is_histogram()
            .then(|| HistogramGrouper::new(tenant_id.clone())),
        skip_rows,
    };
    let mapping = Arc::new(mapping.clone());

    let mut inflight: std::collections::VecDeque<Inflight<WriteReceipt, WriteError>> =
        std::collections::VecDeque::with_capacity(pipeline_depth);

    loop {
        let tenant_for_decode = tenant_id.clone();
        let mapping_for_decode = Arc::clone(&mapping);
        let limits_for_decode = limits.clone();
        let (returned, decoded) = tokio::task::spawn_blocking(move || {
            let mut state = state;
            let outcome = decode_metrics_batch(
                &mut state,
                &tenant_for_decode,
                &mapping_for_decode,
                &limits_for_decode,
                now_ns,
                batch_rows,
                is_monotonic_sum,
            );
            (state, outcome)
        })
        .await
        .map_err(|join_err| LoadError::BatchFailed {
            reason: format!("Parquet decode/build task failed: {join_err}"),
            durable: report.tokens.clone(),
            resume: report.resume(),
        })?;
        state = returned;

        let (points, rows, done) = match decoded {
            MetricsDecoded::Failed(reason) => {
                let (durable, reason) =
                    drain_sequential_before_refusal(&mut inflight, &mut report, reason).await;
                return Err(LoadError::BatchFailed {
                    reason,
                    durable,
                    resume: report.resume(),
                });
            }
            MetricsDecoded::Rejected { row, reason } => {
                let (durable, reason) =
                    drain_sequential_before_refusal(&mut inflight, &mut report, reason).await;
                return Err(LoadError::RowRejected {
                    row,
                    reason,
                    durable,
                    resume: report.resume(),
                });
            }
            MetricsDecoded::Batch { points, rows, done } => (points, rows, done),
        };

        // No points means no group closed (or every row was skipped), so this
        // batch's rows belong to a group still open and are credited to the
        // later write that carries it, not to any write here.
        if points.is_empty() {
            if done {
                break;
            }
            continue;
        }

        let batch_points = points.len() as u64;
        let batch_rows_written = rows;
        let handle = {
            let router = Arc::clone(&router);
            let tenant = tenant_id.clone();
            tokio::spawn(async move {
                router
                    .write(tenant, points, WriteMode::Strict, ack_deadline)
                    .await
            })
        };
        inflight.push_back((batch_rows_written, batch_points, handle));

        // Bound true concurrency to `pipeline_depth`, resolving strictly
        // oldest-first so the token list and the first error stay in
        // submission order however the underlying PUTs complete.
        while inflight.len() >= pipeline_depth {
            let Some(entry) = inflight.pop_front() else {
                break;
            };
            if let Err(mut e) = resolve_sequential_write(entry, &mut report).await {
                harvest_sequential_after_failure(&mut inflight, &mut e).await;
                return Err(e);
            }
        }

        if done {
            break;
        }
    }

    // Publish the tail buffers before draining, for the same reason the logs
    // path does: no later batch is coming to push a buffer past
    // `--target-bytes`, so its writes' acks would otherwise wait out the age
    // trigger. The ticker sweeps a straggler whose send landed after this
    // flush.
    router.flush_all().await;
    let drain_result = {
        let ticker_router = Arc::clone(&router);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let ticker = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    () = tokio::time::sleep(Duration::from_secs(2)) => {
                        ticker_router.flush_all().await;
                    }
                }
            }
        });
        let result = drain_sequential_inflight(&mut inflight, &mut report).await;
        let _ = stop_tx.send(());
        let _ = ticker.await;
        result
    };
    drain_result?;

    report.histogram_points_exploded = state.grouper.as_ref().map_or(0, |g| g.exploded);
    report.elapsed = started.elapsed();
    Ok(report)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
