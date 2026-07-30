//! `LogsScanExec`: the leaf of the `logs` pipeline, the log-signal sibling of
//! [`crate::scan::RsegScanExec`] (ADR-0033).
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions. Each partition
//! fetches its segments through [`LogSegmentFetcher::fetch`] (one
//! [`LogQuery`] per segment: the extracted ts range, stream-attribute
//! equalities, and content predicates), decodes the returned
//! [`ravel_logseg::LogRecord`]s into Arrow arrays matching
//! [`crate::logs_schema::logs_schema`], and emits them sorted by `ts`
//! ascending. That per-partition ordering is declared through
//! `PlanProperties` so a later merge stage (#240) can honor it with a
//! `SortPreservingMergeExec`.
//!
//! `RlogReader::scan` (inside `fetch`) already emits a segment's records
//! grouped by `(stream_ref, ts)`, not globally by `ts`, and a partition draws
//! from several segments, so this stage sorts each partition's collected
//! records by `ts` before emitting. Declaring `ts` ascending is therefore
//! truthful for the stream this stage produces; nothing declares a global
//! cross-partition order (that is a merge's job, not the leaf's).
//!
//! # Mandatory stream-attribute re-verification (issue #239 correctness gate)
//!
//! [`LogSegmentFetcher::matching_streams`] — used internally by `fetch` to
//! resolve a `attrs['k'] = 'v'` predicate into a `Predicate::StreamIn` — is
//! **documented to over-approximate**: it is a byte-containment search over each
//! stream's canonical resource+scope blob, so it also matches a `(key, value)`
//! pair that appears only *nested* inside a `Map`/`List` attribute value, not as
//! a genuine top-level resource or scope attribute. Its own doc comment
//! requires the caller to re-apply the equality on returned records. This stage
//! does exactly that: for every record `fetch` returns whose query carried a
//! stream-attribute equality, it re-checks the equality against the record's
//! actual top-level resource/scope attributes ([`ravel_logseg::LogRecord::stream_attrs`],
//! the canonical hash-preimage blob) before emitting the row, dropping the
//! fetcher's false positives. The check walks the blob's top-level entries
//! only, so a nested pair never satisfies it. This makes the scan's output
//! exact for stream-attribute predicates rather than relying on DataFusion's
//! residual, which would re-apply `attrs['k']='v'` against the *dynamic* attrs
//! column and could not correctly re-check a resource/scope attribute there.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, MapBuilder, StringArray, StringBuilder,
    TimestampNanosecondArray, UInt8Array, UInt32Array,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use ravel_catalog::SegmentRef;
use ravel_logseg::{LogRecord, Predicate};
use ravel_query::{LogQuery, LogSegmentFetcher, StreamAttrEquals};
use ravel_types::logstream::{AttrValue, canonical_attr_bytes};

use crate::error::SqlError;
use crate::logs_schema::{SPAN_ID_WIDTH, TRACE_ID_WIDTH, logs_schema};

/// Rows accumulated into one output batch before it is emitted.
const BATCH_ROWS: usize = 8192;

/// Depth cap when walking a record's canonical resource/scope blob for
/// stream-attribute re-verification (mirrors the reader's `MAX_ATTR_DEPTH`),
/// so hostile nesting cannot exhaust the stack.
const MAX_ATTR_DEPTH: u32 = 32;

/// Entry-count cap per attribute set when walking the blob, matching the
/// reader's own cap so a corrupt count is rejected rather than allocated on.
const MAX_ATTR_ENTRIES: u64 = 1 << 20;

/// Log segment scan producing per-partition ts-ascending batches over the
/// public `logs` schema.
pub struct LogsScanExec {
    fetcher: LogSegmentFetcher,
    /// Round-robin segment assignment; `partitions[k]` runs as DataFusion
    /// partition `k`.
    partitions: Vec<Vec<SegmentRef>>,
    /// Inclusive ts bounds for the fetch's [`LogQuery`].
    ts_min: i64,
    ts_max: i64,
    /// Stream-attribute equalities pushed into the fetch and re-verified per
    /// record (see the module doc).
    stream_attrs: Arc<Vec<StreamAttrEquals>>,
    /// Content predicates (`has_word`) handed to `RlogReader::scan`, applied
    /// exactly there.
    content: Arc<Vec<Predicate>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl LogsScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, with the given ts
    /// bounds, stream-attribute equalities, and content predicates.
    pub fn new(
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        ts_min: i64,
        ts_max: i64,
        stream_attrs: Arc<Vec<StreamAttrEquals>>,
        content: Arc<Vec<Predicate>>,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let schema = logs_schema();
        let properties = Arc::new(Self::compute_properties(&schema, n)?);
        Ok(LogsScanExec {
            fetcher,
            partitions,
            ts_min,
            ts_max,
            stream_attrs,
            content,
            schema,
            properties,
        })
    }

    fn compute_properties(schema: &SchemaRef, n: usize) -> DFResult<PlanProperties> {
        let asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let sort_expr = PhysicalSortExpr::new(col("ts", schema)?, asc);
        let ordering = LexOrdering::new(vec![sort_expr])
            .ok_or_else(|| DataFusionError::Internal("empty logs scan ordering".into()))?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering]);
        Ok(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl fmt::Debug for LogsScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogsScanExec {{ partitions: {} }}",
            self.partitions.len()
        )
    }
}

impl DisplayAs for LogsScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogsScanExec: partitions={}, stream_attrs={}, content={}",
            self.partitions.len(),
            self.stream_attrs.len(),
            self.content.len()
        )
    }
}

impl ExecutionPlan for LogsScanExec {
    fn name(&self) -> &str {
        "LogsScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let segs = self.partitions.get(partition).cloned().unwrap_or_default();
        let fetcher = self.fetcher.clone();
        let stream_attrs = Arc::clone(&self.stream_attrs);
        let content = Arc::clone(&self.content);
        let schema = Arc::clone(&self.schema);

        let reservation = MemoryConsumer::new(format!("LogsScanExec[{partition}]"))
            .register(context.memory_pool());

        let fut = Box::pin(prepare_partition(
            fetcher,
            segs,
            self.ts_min,
            self.ts_max,
            stream_attrs,
            content,
        ));
        Ok(Box::pin(LogScanStream {
            schema,
            reservation,
            state: LogScanState::Fetching(fut),
        }))
    }
}

/// Fetch every segment in this partition, re-verify stream-attribute equalities
/// against each returned record's genuine resource/scope attributes, and return
/// the surviving records sorted by `ts` ascending.
async fn prepare_partition(
    fetcher: LogSegmentFetcher,
    segs: Vec<SegmentRef>,
    ts_min: i64,
    ts_max: i64,
    stream_attrs: Arc<Vec<StreamAttrEquals>>,
    content: Arc<Vec<Predicate>>,
) -> DFResult<Vec<LogRecord>> {
    let mut query = LogQuery::new(ts_min, ts_max);
    for sa in stream_attrs.iter() {
        query = query.with_stream_attr(sa.clone());
    }
    for c in content.iter() {
        query = query.with_content(c.clone());
    }

    let mut out: Vec<LogRecord> = Vec::new();
    for seg in &segs {
        let Some(output) = fetcher.fetch(seg, &query).await.map_err(SqlError::from)? else {
            continue;
        };
        for rec in output.records {
            // Mandatory re-verification: drop the fetcher's stream-attribute
            // over-approximation false positives (see the module doc). With no
            // stream-attribute predicate this is a no-op keeping every record.
            if record_satisfies_stream_attrs(&rec.stream_attrs, &stream_attrs)? {
                out.push(rec);
            }
        }
    }
    // Stable sort so records with equal ts keep the reader's emission order.
    out.sort_by_key(|r| r.ts_ns);
    Ok(out)
}

type PrepareFuture = Pin<Box<dyn Future<Output = DFResult<Vec<LogRecord>>> + Send>>;

enum LogScanState {
    Fetching(PrepareFuture),
    Emitting { records: Vec<LogRecord>, pos: usize },
    Done,
}

/// Per-partition record-batch stream: awaits the fetch, then emits ts-ascending
/// bounded batches, growing the memory reservation by each batch's measured
/// size so a byte-budget overrun surfaces as the pool's `ResourcesExhausted`.
/// The reservation lives on the stream (not the state) so it is the same one
/// for the partition's lifetime and frees exactly once on drop.
struct LogScanStream {
    schema: SchemaRef,
    reservation: MemoryReservation,
    state: LogScanState,
}

impl Stream for LogScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                LogScanState::Fetching(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(records)) => {
                        this.state = LogScanState::Emitting { records, pos: 0 };
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = LogScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                LogScanState::Emitting { records, pos } => {
                    if *pos >= records.len() {
                        this.state = LogScanState::Done;
                        return Poll::Ready(None);
                    }
                    let end = (*pos + BATCH_ROWS).min(records.len());
                    let batch = match build_batch(&records[*pos..end], Arc::clone(&this.schema)) {
                        Ok(b) => b,
                        Err(e) => {
                            this.state = LogScanState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    *pos = end;
                    if let Err(e) = this.reservation.try_grow(batch.get_array_memory_size()) {
                        this.state = LogScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                LogScanState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for LogScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Decode a slice of records into one `logs`-schema [`RecordBatch`].
fn build_batch(records: &[LogRecord], schema: SchemaRef) -> DFResult<RecordBatch> {
    let ts = TimestampNanosecondArray::from(records.iter().map(|r| r.ts_ns).collect::<Vec<_>>());
    let observed_ts = TimestampNanosecondArray::from(
        records.iter().map(|r| r.observed_ts_ns).collect::<Vec<_>>(),
    );
    let severity_num = UInt8Array::from(records.iter().map(|r| r.severity_num).collect::<Vec<_>>());
    let severity_text = StringArray::from(
        records
            .iter()
            .map(|r| r.severity_text.as_str())
            .collect::<Vec<_>>(),
    );
    let body = StringArray::from(records.iter().map(|r| r.body.as_str()).collect::<Vec<_>>());

    let mut trace = FixedSizeBinaryBuilder::with_capacity(records.len(), TRACE_ID_WIDTH);
    for r in records {
        match &r.trace_id {
            Some(id) => trace
                .append_value(id)
                .map_err(|e| SqlError::Internal(format!("trace_id array build: {e}")))?,
            None => trace.append_null(),
        }
    }
    let trace = trace.finish();

    let mut span = FixedSizeBinaryBuilder::with_capacity(records.len(), SPAN_ID_WIDTH);
    for r in records {
        match &r.span_id {
            Some(id) => span
                .append_value(id)
                .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?,
            None => span.append_null(),
        }
    }
    let span = span.finish();

    let flags = UInt32Array::from(records.iter().map(|r| r.flags).collect::<Vec<_>>());

    // `attrs` map: the record's dynamic attributes, values rendered to text.
    let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for r in records {
        for (k, v) in &r.attrs {
            attrs.keys().append_value(k);
            attrs.values().append_value(attr_value_to_string(v));
        }
        attrs
            .append(true)
            .map_err(|e| SqlError::Internal(format!("attrs map build: {e}")))?;
    }
    let attrs = attrs.finish();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts),
        Arc::new(observed_ts),
        Arc::new(severity_num),
        Arc::new(severity_text),
        Arc::new(body),
        Arc::new(trace),
        Arc::new(span),
        Arc::new(flags),
        Arc::new(attrs),
    ];
    debug_assert_eq!(schema.fields().len(), columns.len());
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

/// v1 stringification of a dynamic attribute value for the `Map(Utf8, Utf8)`
/// `attrs` column. Scalar values render to their natural text; `Bytes`, `List`,
/// and `Map` render to the lowercase hex of their canonical encoding, a
/// deterministic, injective form pending a richer typed column (ADR-0033's
/// v-next refinement).
fn attr_value_to_string(v: &AttrValue) -> String {
    match v {
        AttrValue::Str(s) => s.clone(),
        AttrValue::I64(i) => i.to_string(),
        AttrValue::F64(f) => f.to_string(),
        AttrValue::Bool(b) => b.to_string(),
        AttrValue::Bytes(b) => hex_lower(b),
        AttrValue::List(_) | AttrValue::Map(_) => hex_lower(&canonical_attr_bytes(
            std::slice::from_ref(&(String::new(), v.clone())),
        )),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String never fails.
        let _ = write!(out, "{b:02x}");
    }
    out
}

// --- Stream-attribute re-verification over the canonical resource/scope blob ---

/// True iff `blob` (a record's [`ravel_logseg::LogRecord::stream_attrs`], the
/// canonical `resource ++ scope-name ++ scope-version ++ scope-attrs` bytes)
/// genuinely carries every filter's `(key, value)` as a **top-level** resource
/// or scope attribute. A pair nested inside a `Map`/`List` value never
/// satisfies it, which is exactly what distinguishes a real match from the
/// fetcher's byte-containment over-approximation. Empty `filters` is true.
fn record_satisfies_stream_attrs(blob: &[u8], filters: &[StreamAttrEquals]) -> DFResult<bool> {
    for f in filters {
        if !blob_has_top_level_attr(blob, f)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn blob_has_top_level_attr(blob: &[u8], filter: &StreamAttrEquals) -> DFResult<bool> {
    let needle = value_needle(&filter.value);
    let key = filter.key.as_bytes();
    let mut pos = 0usize;
    // Resource attribute set.
    if walk_attr_set(blob, &mut pos, key, &needle, 0)? {
        return Ok(true);
    }
    // Scope name and scope version, each a length-prefixed string.
    skip_len_prefixed(blob, &mut pos)?;
    skip_len_prefixed(blob, &mut pos)?;
    // Scope attribute set.
    walk_attr_set(blob, &mut pos, key, &needle, 0)
}

/// The canonical value bytes of `v` as they appear for a single attribute entry
/// (i.e. `encode_value(v)`): `canonical_attr_bytes([("", v)])` with its leading
/// one-entry count byte (`0x01`) and empty-key length byte (`0x00`) stripped.
fn value_needle(v: &AttrValue) -> Vec<u8> {
    let full = canonical_attr_bytes(std::slice::from_ref(&(String::new(), v.clone())));
    full.get(2..).map(<[u8]>::to_vec).unwrap_or_default()
}

/// Walk one canonical attribute set from `pos`, advancing `pos` to its end, and
/// return whether any **top-level** entry's key and encoded value equal
/// `(key, needle)`. Nested `Map`/`List` values are consumed but never matched.
fn walk_attr_set(
    buf: &[u8],
    pos: &mut usize,
    key: &[u8],
    needle: &[u8],
    depth: u32,
) -> DFResult<bool> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let count = read_uvarint(buf, pos)?;
    if count > MAX_ATTR_ENTRIES {
        return Err(corrupt("stream_attrs entry count over cap"));
    }
    let mut found = false;
    for _ in 0..count {
        let klen = usize_of(read_uvarint(buf, pos)?)?;
        let kstart = *pos;
        advance(buf, pos, klen)?;
        let kbytes = &buf[kstart..*pos];
        let vstart = *pos;
        skip_value(buf, pos, depth + 1)?;
        let vbytes = &buf[vstart..*pos];
        if !found && kbytes == key && vbytes == needle {
            found = true;
        }
    }
    Ok(found)
}

/// Advance `pos` past one encoded attribute value (frozen grammar,
/// `ravel_types::logstream`: 1=Str 2=I64 3=F64 4=Bool 5=Bytes 6=List 7=Map).
fn skip_value(buf: &[u8], pos: &mut usize, depth: u32) -> DFResult<()> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let tag = read_u8(buf, pos)?;
    match tag {
        // Str / Bytes: length-prefixed payload.
        1 | 5 => {
            let len = usize_of(read_uvarint(buf, pos)?)?;
            advance(buf, pos, len)?;
        }
        // I64: a single zigzag varint.
        2 => {
            read_uvarint(buf, pos)?;
        }
        // F64: eight little-endian bytes.
        3 => advance(buf, pos, 8)?,
        // Bool: one byte.
        4 => advance(buf, pos, 1)?,
        // List: a count then each element.
        6 => {
            let n = read_uvarint(buf, pos)?;
            for _ in 0..n {
                skip_value(buf, pos, depth + 1)?;
            }
        }
        // Map: a nested canonical attribute set; consumed, never matched here.
        7 => {
            walk_attr_set(buf, pos, b"", &[], depth + 1)?;
        }
        _ => return Err(corrupt("bad stream_attrs value tag")),
    }
    Ok(())
}

/// Skip a length-prefixed string (the scope name / version fields).
fn skip_len_prefixed(buf: &[u8], pos: &mut usize) -> DFResult<()> {
    let len = usize_of(read_uvarint(buf, pos)?)?;
    advance(buf, pos, len)
}

fn read_u8(buf: &[u8], pos: &mut usize) -> DFResult<u8> {
    let b = *buf
        .get(*pos)
        .ok_or_else(|| corrupt("stream_attrs truncated"))?;
    *pos += 1;
    Ok(b)
}

fn advance(buf: &[u8], pos: &mut usize, n: usize) -> DFResult<()> {
    let end = pos
        .checked_add(n)
        .ok_or_else(|| corrupt("stream_attrs length overflow"))?;
    if end > buf.len() {
        return Err(corrupt("stream_attrs truncated"));
    }
    *pos = end;
    Ok(())
}

/// Read one unsigned LEB128 varint, rejecting an over-long encoding rather than
/// looping or overflowing.
fn read_uvarint(buf: &[u8], pos: &mut usize) -> DFResult<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        if shift >= 64 {
            return Err(corrupt("stream_attrs varint overflow"));
        }
        let byte = read_u8(buf, pos)?;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

fn usize_of(v: u64) -> DFResult<usize> {
    usize::try_from(v).map_err(|_| corrupt("stream_attrs length exceeds usize"))
}

fn corrupt(what: &str) -> DataFusionError {
    // A malformed blob here means a record we decoded carried corrupt canonical
    // stream_attrs bytes; surface it as an internal error (redacted to the
    // client), never a panic or a silently-wrong filter result.
    SqlError::Internal(what.to_string()).into()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_logseg::stream_attrs_bytes;

    use super::*;

    fn s(v: &str) -> AttrValue {
        AttrValue::Str(v.into())
    }

    #[test]
    fn top_level_resource_attr_matches() {
        let blob = stream_attrs_bytes(
            &[("service.name".into(), s("api")), ("host".into(), s("h1"))],
            "scope",
            "1.0",
            &[],
        );
        assert!(
            blob_has_top_level_attr(&blob, &StreamAttrEquals::new("service.name", s("api")))
                .unwrap()
        );
        // Wrong value does not match.
        assert!(
            !blob_has_top_level_attr(&blob, &StreamAttrEquals::new("service.name", s("worker")))
                .unwrap()
        );
    }

    #[test]
    fn top_level_scope_attr_matches() {
        let blob = stream_attrs_bytes(&[], "scope", "1.0", &[("lib".into(), s("otel"))]);
        assert!(blob_has_top_level_attr(&blob, &StreamAttrEquals::new("lib", s("otel"))).unwrap());
    }

    #[test]
    fn nested_map_value_is_not_a_top_level_match() {
        // The pair only appears nested inside a `k8s.labels` map value; the
        // fetcher's byte containment would match it, this must not.
        let blob = stream_attrs_bytes(
            &[(
                "k8s.labels".into(),
                AttrValue::Map(vec![("service.name".into(), s("api"))]),
            )],
            "scope",
            "1.0",
            &[],
        );
        assert!(
            !blob_has_top_level_attr(&blob, &StreamAttrEquals::new("service.name", s("api")))
                .unwrap(),
            "nested-map pair must not count as a genuine top-level attribute"
        );
    }

    #[test]
    fn empty_filter_set_is_satisfied() {
        let blob = stream_attrs_bytes(&[("a".into(), s("b"))], "s", "1", &[]);
        assert!(record_satisfies_stream_attrs(&blob, &[]).unwrap());
    }
}
