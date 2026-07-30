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
//! # Correctness: the merged `attrs` column plus DataFusion's residual
//!
//! This scan pushes only two predicate kinds into [`LogSegmentFetcher::fetch`]:
//! the ts range (a segment-level and reader-level prune, exact) and content
//! predicates (`has_word`, whose SQL semantics equal the reader's exact filter,
//! [`crate::logs_pushdown`]). It does **not** push stream-attribute equalities,
//! and it performs no per-record re-verification: it emits every record the
//! fetcher returns. Attribute filtering is entirely DataFusion's job.
//!
//! The reason is the ADR-0033 merge. `attrs` is the resource + scope + record
//! attributes merged into one map with the record winning on a key collision, so
//! a record's `attrs['k']` value can differ from its stream-identifying
//! resource/scope attributes. Any prune keyed on stream-level attributes — the
//! fetcher's STREAM_DIR match resolved into a `Predicate::StreamIn`, or a
//! scan-level re-check of `stream_attrs` — is therefore **not** a sound
//! over-approximation of `attrs['k'] = 'v'`: it drops a record whose match lives
//! only in its per-record dynamic attributes (resource `service.name = worker`,
//! record attribute `service.name = api`, query `= 'api'`), which the merged map
//! resolves to `api` and must keep. Pushing such a predicate as a fetch prune is
//! a data-loss bug; so this scan does not, and stream-attribute equalities are
//! not extracted into the fetch at all ([`crate::logs_pushdown`]).
//!
//! Correctness comes solely from the merged `attrs` column plus the residual.
//! Pushdown is always `Inexact`, so DataFusion re-applies the *original*
//! predicate against the emitted batch. [`build_batch`] populates the `attrs`
//! column from the fully merged view (ADR-0033 amendment, issue #239), so the
//! residual evaluates `attrs['k'] = 'v'` against exactly the data a row's SQL
//! semantics demand: a resource-only match survives (the residual sees it in the
//! merged column), and a record-attribute override survives (the merge resolves
//! the key to the record's value, which wins). The merged column and the
//! residual are the whole correctness story.

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
use ravel_query::{LogQuery, LogSegmentFetcher};
use ravel_types::logstream::{AttrValue, canonical_attr_bytes};

use crate::error::SqlError;
use crate::logs_schema::{SPAN_ID_WIDTH, TRACE_ID_WIDTH, logs_schema};

/// Rows accumulated into one output batch before it is emitted.
const BATCH_ROWS: usize = 8192;

/// Depth cap when walking a record's canonical resource/scope blob for the
/// merged-`attrs` decode (mirrors the reader's `MAX_ATTR_DEPTH`), so hostile
/// nesting cannot exhaust the stack.
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
    /// Content predicates (`has_word`) handed to `RlogReader::scan`, applied
    /// exactly there.
    content: Arc<Vec<Predicate>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl LogsScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, with the given ts
    /// bounds and content predicates. Stream-attribute equalities are
    /// deliberately not accepted: they are not pushed into the fetch, because a
    /// stream-level prune is unsound against the merged `attrs` column (see the
    /// module doc). DataFusion's residual filters attributes.
    pub fn new(
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        ts_min: i64,
        ts_max: i64,
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
            "LogsScanExec: partitions={}, content={}",
            self.partitions.len(),
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
        let content = Arc::clone(&self.content);
        let schema = Arc::clone(&self.schema);

        let reservation = MemoryConsumer::new(format!("LogsScanExec[{partition}]"))
            .register(context.memory_pool());

        let fut = Box::pin(prepare_partition(
            fetcher,
            segs,
            self.ts_min,
            self.ts_max,
            content,
        ));
        Ok(Box::pin(LogScanStream {
            schema,
            reservation,
            state: LogScanState::Fetching(fut),
        }))
    }
}

/// Fetch every segment in this partition and return its records sorted by `ts`
/// ascending. Only the ts range and content predicates prune the fetch;
/// attribute filtering is DataFusion's residual over [`build_batch`]'s merged
/// `attrs` column (see the module doc). Every fetched record is emitted.
async fn prepare_partition(
    fetcher: LogSegmentFetcher,
    segs: Vec<SegmentRef>,
    ts_min: i64,
    ts_max: i64,
    content: Arc<Vec<Predicate>>,
) -> DFResult<Vec<LogRecord>> {
    let mut query = LogQuery::new(ts_min, ts_max);
    for c in content.iter() {
        query = query.with_content(c.clone());
    }

    let mut out: Vec<LogRecord> = Vec::new();
    for seg in &segs {
        let Some(output) = fetcher.fetch(seg, &query).await.map_err(SqlError::from)? else {
            continue;
        };
        // Emit every fetched record: stream-attribute equalities are not pushed
        // (a stream-level prune is unsound against the merged `attrs` column),
        // so nothing here narrows below what DataFusion's residual keeps.
        out.extend(output.records);
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

    // `attrs` map: each record's stream-identity (resource + scope) attributes
    // merged with its dynamic per-record attributes, values rendered to text.
    // DataFusion's mandatory `Inexact` residual re-applies `attrs['k'] = 'v'`
    // against this column, and that residual is the sole exactness mechanism, so
    // the column must carry the fully merged view. Populating it from `r.attrs`
    // alone silently dropped every record whose matched attribute was a genuine
    // resource attribute (ADR-0033 amendment, issue #239). See `merged_attrs`.
    let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for r in records {
        for (k, v) in merged_attrs(r)? {
            attrs.keys().append_value(&k);
            attrs.values().append_value(attr_value_to_string(&v));
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

// --- `attrs` column contents: merged resource/scope + record attributes ---

/// The `attrs` column contents for one record: its decoded stream-identity
/// (resource + scope) attributes overlaid with its dynamic per-record
/// attributes, the record's value winning on a key collision. The residual
/// re-check and the scan's stream-identity check therefore see the same data
/// (ADR-0033 amendment, issue #239).
fn merged_attrs(r: &LogRecord) -> DFResult<Vec<(String, AttrValue)>> {
    let mut merged = decode_stream_attrs(&r.stream_attrs)?;
    for (k, v) in &r.attrs {
        if let Some(slot) = merged.iter_mut().find(|(mk, _)| mk == k) {
            slot.1 = v.clone();
        } else {
            merged.push((k.clone(), v.clone()));
        }
    }
    Ok(merged)
}

/// Decode the **top-level** resource and scope attribute entries of a
/// `stream_attrs` blob (a record's [`ravel_logseg::LogRecord::stream_attrs`],
/// the canonical `resource ++ scope-name ++ scope-version ++ scope-attrs`
/// bytes) into `(key, value)` pairs, in blob order (resource set first, then the
/// scope attribute set). The scope name and version are length-prefixed
/// *positional* fields, not key-value entries, so they are skipped over and
/// never become synthetic `scope.name`/`scope.version` keys.
///
/// A top-level entry whose value is a nested `Map` or `List` is walked past
/// (consumed, so decoding stays in frame) but **omitted** from the returned
/// pairs. This mirrors [`walk_attr_set`]'s matching, which likewise never
/// matches a nested value, and is a deliberate, documented v1 limitation of the
/// merged `attrs` column: a resource/scope attribute whose value is itself a
/// map or list is not projected into the map column (a richer typed
/// representation is ADR-0033's v-next refinement). Per-record dynamic
/// attributes with nested values are unaffected -- they are merged in verbatim
/// by [`merged_attrs`] and rendered by [`attr_value_to_string`].
fn decode_stream_attrs(blob: &[u8]) -> DFResult<Vec<(String, AttrValue)>> {
    let mut pos = 0usize;
    let mut out = Vec::new();
    // Resource attribute set.
    decode_attr_set(blob, &mut pos, 0, &mut out)?;
    // Scope name and scope version, each a length-prefixed string (positional).
    skip_len_prefixed(blob, &mut pos)?;
    skip_len_prefixed(blob, &mut pos)?;
    // Scope attribute set.
    decode_attr_set(blob, &mut pos, 0, &mut out)?;
    Ok(out)
}

/// Walk one canonical attribute set from `pos`, advancing `pos` to its end, and
/// push every top-level scalar entry as a decoded `(key, value)` pair onto
/// `out`. A nested `Map`/`List` value is consumed but not pushed (see
/// [`decode_stream_attrs`]). Byte-walks the same frozen grammar as
/// [`walk_attr_set`]; kept a separate function so that walker's behavior stays
/// untouched.
fn decode_attr_set(
    buf: &[u8],
    pos: &mut usize,
    depth: u32,
    out: &mut Vec<(String, AttrValue)>,
) -> DFResult<()> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let count = read_uvarint(buf, pos)?;
    if count > MAX_ATTR_ENTRIES {
        return Err(corrupt("stream_attrs entry count over cap"));
    }
    for _ in 0..count {
        let klen = usize_of(read_uvarint(buf, pos)?)?;
        let kstart = *pos;
        advance(buf, pos, klen)?;
        let kbytes = &buf[kstart..*pos];
        if let Some(value) = decode_value(buf, pos, depth + 1)? {
            let key = std::str::from_utf8(kbytes)
                .map_err(|_| corrupt("stream_attrs key not utf-8"))?
                .to_string();
            out.push((key, value));
        }
    }
    Ok(())
}

/// Decode one encoded attribute value at `pos` (frozen grammar,
/// `ravel_types::logstream`: 1=Str 2=I64 3=F64 4=Bool 5=Bytes 6=List 7=Map),
/// advancing `pos` past it. Returns the decoded scalar, or `None` for a
/// `List`/`Map`, which is consumed via the untouched [`skip_value`]/
/// [`walk_attr_set`] walkers but not decoded into an entry.
fn decode_value(buf: &[u8], pos: &mut usize, depth: u32) -> DFResult<Option<AttrValue>> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let tag = read_u8(buf, pos)?;
    let value = match tag {
        // Str: length-prefixed UTF-8 payload.
        1 => {
            let len = usize_of(read_uvarint(buf, pos)?)?;
            let start = *pos;
            advance(buf, pos, len)?;
            let s = std::str::from_utf8(&buf[start..*pos])
                .map_err(|_| corrupt("stream_attrs str not utf-8"))?
                .to_string();
            Some(AttrValue::Str(s))
        }
        // I64: a single zigzag varint.
        2 => Some(AttrValue::I64(unzigzag(read_uvarint(buf, pos)?))),
        // F64: eight little-endian bytes of `to_bits` (NaN payloads / -0.0
        // preserved by decoding through the bit pattern).
        3 => {
            let start = *pos;
            advance(buf, pos, 8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[start..*pos]);
            Some(AttrValue::F64(f64::from_bits(u64::from_le_bytes(b))))
        }
        // Bool: one byte.
        4 => Some(AttrValue::Bool(read_u8(buf, pos)? != 0)),
        // Bytes: length-prefixed payload.
        5 => {
            let len = usize_of(read_uvarint(buf, pos)?)?;
            let start = *pos;
            advance(buf, pos, len)?;
            Some(AttrValue::Bytes(buf[start..*pos].to_vec()))
        }
        // List: a count then each element; consumed, not decoded.
        6 => {
            let n = read_uvarint(buf, pos)?;
            for _ in 0..n {
                skip_value(buf, pos, depth + 1)?;
            }
            None
        }
        // Map: a nested canonical attribute set; consumed, not decoded.
        7 => {
            walk_attr_set(buf, pos, b"", &[], depth + 1)?;
            None
        }
        _ => return Err(corrupt("bad stream_attrs value tag")),
    };
    Ok(value)
}

/// Inverse of the writer's zigzag mapping (`ravel_types::logstream`): recover a
/// signed integer from its unsigned LEB128 zigzag form.
fn unzigzag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

// --- Byte-walkers shared by the merged-attrs decode ---

/// Walk one canonical attribute set from `pos`, advancing `pos` to its end, and
/// return whether any **top-level** entry's key and encoded value equal
/// `(key, needle)`. Nested `Map`/`List` values are consumed but never matched.
///
/// [`decode_value`]'s `Map` case calls this with an empty `key`/`needle` purely
/// to skip past a nested set (ignoring the returned bool); the matching logic is
/// retained unchanged so that skip stays byte-exact.
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
    // stream_attrs bytes: the same data-integrity fault the fetcher reports as
    // `LogFetchError::Corrupt`, just detected one layer up. Surface it with the
    // identical client class/message (`MSG_CORRUPT`, `ErrorClass::Unavailable`)
    // via `SqlError::CorruptStreamAttrs`, not a distinct internal-error class,
    // so one underlying fault never maps to two client-visible classes. Never a
    // panic or a silently-wrong filter result.
    SqlError::CorruptStreamAttrs(what.to_string()).into()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_logseg::stream_attrs_bytes;

    use super::*;

    fn s(v: &str) -> AttrValue {
        AttrValue::Str(v.into())
    }

    // --- decoder / merge (issue #239 fix) ---

    #[test]
    fn decode_yields_top_level_resource_and_scope_scalars() {
        let blob = stream_attrs_bytes(
            &[
                ("service.name".into(), s("api")),
                ("port".into(), AttrValue::I64(8080)),
            ],
            "libscope",
            "2.1",
            &[("lib".into(), s("otel"))],
        );
        let got = decode_stream_attrs(&blob).unwrap();
        assert!(got.contains(&("service.name".to_string(), s("api"))));
        assert!(got.contains(&("port".to_string(), AttrValue::I64(8080))));
        assert!(got.contains(&("lib".to_string(), s("otel"))));
        // Scope name/version are positional, never synthesized into keys or values.
        assert!(
            !got.iter()
                .any(|(k, _)| k == "scope.name" || k == "scope.version")
        );
        assert!(
            !got.iter()
                .any(|(_, v)| matches!(v, AttrValue::Str(x) if x == "libscope" || x == "2.1"))
        );
    }

    #[test]
    fn decode_omits_nested_map_and_list_but_stays_in_frame() {
        let blob = stream_attrs_bytes(
            &[
                (
                    "k8s.labels".into(),
                    AttrValue::Map(vec![("service.name".into(), s("api"))]),
                ),
                ("tags".into(), AttrValue::List(vec![s("a"), s("b")])),
                ("host".into(), s("h1")),
            ],
            "s",
            "1",
            &[],
        );
        // Nested map/list top-level entries are consumed but omitted; the scalar
        // sibling still decodes, proving the walk stayed byte-aligned.
        assert_eq!(
            decode_stream_attrs(&blob).unwrap(),
            vec![("host".into(), s("h1"))]
        );
    }

    #[test]
    fn decode_roundtrips_scalar_types() {
        let blob = stream_attrs_bytes(
            &[
                ("b".into(), AttrValue::Bool(true)),
                ("f".into(), AttrValue::F64(-0.0)),
                ("by".into(), AttrValue::Bytes(vec![1, 2, 3])),
                ("i".into(), AttrValue::I64(-42)),
            ],
            "s",
            "1",
            &[],
        );
        let got: std::collections::BTreeMap<_, _> =
            decode_stream_attrs(&blob).unwrap().into_iter().collect();
        assert_eq!(got["b"], AttrValue::Bool(true));
        assert_eq!(got["by"], AttrValue::Bytes(vec![1, 2, 3]));
        assert_eq!(got["i"], AttrValue::I64(-42));
        // -0.0 preserved through the bit pattern (writer's f64::to_bits discipline).
        match &got["f"] {
            AttrValue::F64(x) => assert_eq!(x.to_bits(), (-0.0f64).to_bits()),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_truncated_blob_as_corrupt() {
        let blob = stream_attrs_bytes(&[("k".into(), s("v"))], "s", "1", &[]);
        // Chop the last byte: the value payload is now truncated.
        let err = decode_stream_attrs(&blob[..blob.len() - 1]).unwrap_err();
        // Surfaces as the shared corruption class, not a panic or wrong data.
        let sql = match err {
            DataFusionError::External(b) => b.downcast::<SqlError>().expect("SqlError"),
            other => panic!("expected External, got {other:?}"),
        };
        assert_eq!(sql.class(), crate::error::ErrorClass::Unavailable);
        assert_eq!(sql.client_message(), crate::error::MSG_CORRUPT);
    }

    #[test]
    fn merged_attrs_record_wins_collision_and_keeps_resource() {
        let resource = [("service.name".into(), s("api")), ("host".into(), s("h1"))];
        let r = LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&resource, "sc", "1", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "sc", "1", &[]),
            ts_ns: 1,
            observed_ts_ns: 1,
            severity_num: 0,
            severity_text: String::new(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![
                ("service.name".into(), s("override")),
                ("dyn".into(), s("v")),
            ],
        };
        let merged = merged_attrs(&r).unwrap();
        // Exactly one service.name entry (no duplicate key from the merge).
        assert_eq!(
            merged.iter().filter(|(k, _)| k == "service.name").count(),
            1
        );
        let map: std::collections::BTreeMap<_, _> = merged.into_iter().collect();
        assert_eq!(map["service.name"], s("override")); // record wins
        assert_eq!(map["host"], s("h1")); // resource-only attribute retained
        assert_eq!(map["dyn"], s("v")); // record-only attribute added
    }
}
