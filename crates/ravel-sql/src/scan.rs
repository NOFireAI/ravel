//! `RsegScanExec`: the leaf of the samples pipeline.
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions. Each partition
//! fetches its segments through the shared `SegmentFetcher` SoA surface
//! (ticket A1a), gathers every sample with its provenance, and emits one
//! batch sorted by `(series_id, ts, created_unix_ns, writer_epoch,
//! writer_seq, in_page_index)`. That full ordering is declared through
//! `PlanProperties` so the optimizer honors it with a
//! `SortPreservingMergeExec`, never a `CoalescePartitionsExec` (review F12).

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryArray, Float64Array, Int64Array, TimestampNanosecondArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use ravel_catalog::SegmentRef;
use ravel_query::SegmentFetcher;
use ravel_types::{LabelSet, TenantHash};

use crate::error::SqlError;
use crate::labels::build_labels_dict;
use crate::schema::{COL_SERIES_ID, COL_TS, internal_schema};

/// One decoded sample plus its full provenance tuple, before sorting.
struct ScanRow {
    series_id: [u8; 16],
    ts: i64,
    value: f64,
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    in_page_index: u32,
}

impl ScanRow {
    /// The total sort order the scan declares: `(series_id, ts,
    /// created_unix_ns, writer_epoch, writer_seq, in_page_index)`.
    fn sort_key(&self) -> ([u8; 16], i64, i64, u64, u64, u32) {
        (
            self.series_id,
            self.ts,
            self.created_unix_ns,
            self.writer_epoch,
            self.writer_seq,
            self.in_page_index,
        )
    }
}

/// Segment scan producing per-partition `(series_id, ts, provenance)`-sorted
/// batches over the internal schema.
pub struct RsegScanExec {
    tenant_hash: TenantHash,
    fetcher: SegmentFetcher,
    /// Round-robin segment assignment; `partitions[k]` is executed as
    /// DataFusion partition `k`.
    partitions: Vec<Vec<SegmentRef>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RsegScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions.
    pub fn new(
        tenant_hash: TenantHash,
        fetcher: SegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let schema = internal_schema();
        let properties = Arc::new(Self::compute_properties(&schema, n)?);
        Ok(RsegScanExec {
            tenant_hash,
            fetcher,
            partitions,
            schema,
            properties,
        })
    }

    fn compute_properties(schema: &SchemaRef, n: usize) -> DFResult<PlanProperties> {
        let asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let sort_exprs = [
            "series_id",
            "ts",
            "created_unix_ns",
            "writer_epoch",
            "writer_seq",
            "in_page_index",
        ]
        .into_iter()
        .map(|name| Ok(PhysicalSortExpr::new(col(name, schema)?, asc)))
        .collect::<DFResult<Vec<_>>>()?;
        let ordering = LexOrdering::new(sort_exprs)
            .ok_or_else(|| DataFusionError::Internal("empty scan ordering".into()))?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering]);
        Ok(PlanProperties::new(
            eq,
            datafusion::physical_plan::Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl fmt::Debug for RsegScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RsegScanExec {{ partitions: {} }}",
            self.partitions.len()
        )
    }
}

impl DisplayAs for RsegScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RsegScanExec: partitions={}", self.partitions.len())
    }
}

impl ExecutionPlan for RsegScanExec {
    fn name(&self) -> &str {
        "RsegScanExec"
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
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let segs = self.partitions.get(partition).cloned().unwrap_or_default();
        let fetcher = self.fetcher.clone();
        let tenant = self.tenant_hash;
        let schema = Arc::clone(&self.schema);
        let build_schema = Arc::clone(&self.schema);
        let fut = async move { build_partition_batch(fetcher, tenant, segs, build_schema).await };
        let stream = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Fetch every segment in this partition, gather all samples, sort by the
/// declared order, and build one batch.
async fn build_partition_batch(
    fetcher: SegmentFetcher,
    tenant: TenantHash,
    segs: Vec<SegmentRef>,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let mut rows: Vec<ScanRow> = Vec::new();
    let mut labels_by_series: HashMap<[u8; 16], LabelSet> = HashMap::new();

    for seg in &segs {
        // B1 has no pushdown: every series in the segment is fetched. The
        // empty matcher slice keeps `LabelMatcher` unnamed here.
        let (series, _stats) = fetcher
            .fetch_soa(tenant, seg, &[])
            .await
            .map_err(SqlError::from)?;
        for fs in series {
            labels_by_series
                .entry(fs.series_id.0)
                .or_insert_with(|| fs.labels.clone());
            for (i, (&ts, &value)) in fs.timestamps.iter().zip(fs.values.iter()).enumerate() {
                rows.push(ScanRow {
                    series_id: fs.series_id.0,
                    ts,
                    value,
                    created_unix_ns: fs.created_unix_ns,
                    writer_epoch: fs.writer_epoch,
                    writer_seq: fs.writer_seq,
                    in_page_index: u32::try_from(i).unwrap_or(u32::MAX),
                });
            }
        }
    }

    rows.sort_by_key(|r| r.sort_key());
    build_batch(&rows, &labels_by_series, schema)
}

fn build_batch(
    rows: &[ScanRow],
    labels_by_series: &HashMap<[u8; 16], LabelSet>,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let ts = TimestampNanosecondArray::from(rows.iter().map(|r| r.ts).collect::<Vec<_>>());
    let value = Float64Array::from(rows.iter().map(|r| r.value).collect::<Vec<_>>());
    let series_id = FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| r.series_id))
        .map_err(|e| SqlError::Internal(format!("series_id array build: {e}")))?;
    let created = Int64Array::from(rows.iter().map(|r| r.created_unix_ns).collect::<Vec<_>>());
    let epoch = UInt64Array::from(rows.iter().map(|r| r.writer_epoch).collect::<Vec<_>>());
    let seq = UInt64Array::from(rows.iter().map(|r| r.writer_seq).collect::<Vec<_>>());
    let in_page = UInt32Array::from(rows.iter().map(|r| r.in_page_index).collect::<Vec<_>>());

    // Dictionary-encode labels: one dictionary entry per distinct series in
    // this batch, an Int32 key per row. Rows are already grouped by
    // series_id (the sort above), so distinct series appear in contiguous
    // runs.
    let mut distinct: Vec<LabelSet> = Vec::new();
    let mut keys: Vec<i32> = Vec::with_capacity(rows.len());
    let mut last: Option<[u8; 16]> = None;
    let mut idx: i32 = -1;
    for r in rows {
        if last != Some(r.series_id) {
            let labels = labels_by_series
                .get(&r.series_id)
                .cloned()
                .unwrap_or_default();
            distinct.push(labels);
            idx += 1;
            last = Some(r.series_id);
        }
        keys.push(idx);
    }
    let labels = build_labels_dict(&distinct, &keys).map_err(DataFusionError::from)?;

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts),
        Arc::new(value),
        Arc::new(series_id),
        labels,
        Arc::new(created),
        Arc::new(epoch),
        Arc::new(seq),
        Arc::new(in_page),
    ];
    debug_assert_eq!(schema.fields().len(), columns.len());
    debug_assert_eq!(COL_TS, 0);
    debug_assert_eq!(COL_SERIES_ID, 2);
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
