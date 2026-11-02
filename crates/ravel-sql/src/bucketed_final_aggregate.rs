//! Bucketed final aggregation (ADR-3565 decision 1, issue #3608).
//!
//! A `FinalPartitioned` aggregate over unique-ish keys builds one hash table
//! per partition as large as the partition's groups, and every probe into it
//! misses cache. [`BucketedFinalAggregateExec`] replaces it, per partition, with
//! two phases:
//!
//! 1. **Single table, then route.** It merges its input into one
//!    `GroupValues` table and one accumulator set, exactly as the stock final,
//!    until the table holds `engage_groups` groups. It then takes the table's
//!    state (`GroupsAccumulator::state` and `GroupValues::emit`, both
//!    `EmitTo::All`) and routes those rows, and every later input row, into
//!    `2^bits` buckets chosen by the top `bits` bits of the key hash. The hash
//!    uses [`BUCKET_HASH_SEED`], not the repartition exchange's seed 0: with
//!    the exchange's own hash every row of a partition would share its low
//!    bits, and the buckets would be correlated with the partition. A bucket
//!    stores its rows column by column (keys, then every accumulator's state)
//!    in chunks of `routed_chunk_rows` rows, so its slack is at most one chunk
//!    per column.
//! 2. **One bucket at a time.** When input ends it merges bucket 0 into a
//!    reused table and accumulator set, evaluates and emits it, releases the
//!    bucket's chunks, clears the table with `clear_shrink`, and moves to
//!    bucket 1. A partition that never reaches `engage_groups` evaluates its
//!    single table instead, as the stock final does.
//!
//! The bucket is a function of the key alone, so every group lands in exactly
//! one bucket and each bucket's groups are complete when input ends: each
//! bucket runs the same `merge_batch` and `evaluate` over the same state the
//! stock final would, and the answers are the same up to row order
//! (`answers_match_the_stock_final_at_1_4_and_32_partitions` in
//! `tests/bucketed_final_aggregate.rs`).
//!
//! # What qualifies
//!
//! [`BucketedFinalAggregateRule::admits`] is the whole predicate: a
//! `FinalPartitioned` aggregate with no limit, linear input order and no
//! dynamic filter; one grouping set with no `__grouping_id` key; every key a
//! fixed-width integer, date, timestamp, time or interval; every aggregate
//! with a `GroupsAccumulator`, no ordering requirement, and state fields that
//! are fixed-width integer, temporal or `Decimal128`, never float. Anything
//! else keeps the stock plan (`every_refusal_condition_leaves_the_plan_unchanged`).
//!
//! # Memory
//!
//! One consumer per partition, named `BucketedFinalAggregate[p]`, outside
//! every aggregate prefix `crate::memory` recognises, so it is charged one to
//! one with no reserve-ahead factor and no held release. Every bucket chunk is
//! charged at its allocated capacity before a row is written to it; the
//! tables, the per-batch scratch and a bucket's pending output are charged by
//! their `size()` after each batch. A refused grow fails the stream with the
//! pool's `ResourcesExhausted`, prefixed with the consumer's name. Nothing
//! spills. Every byte is released as its bucket is emitted, and all of it
//! when the stream drops.
//!
//! # Yielding and output
//!
//! The stream returns `Pending` (after waking itself) after every input batch
//! it merges or routes, and emits each bucket before it aggregates the next,
//! so a dropped stream or a breached ceiling acts within one batch in phase 1
//! and one bucket in phase 2. Every output batch is a copy of at most
//! `batch_size` rows: no operator above holds a slice that pins a whole
//! bucket's buffers. No output is emitted before input ends.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use datafusion::arrow::array::{Array, ArrayData, ArrayRef, MutableArrayData, make_array};
use datafusion::arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::util::bit_util;
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::hash_utils::{RandomState, create_hashes};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::logical_expr::{Aggregate, EmitTo, GroupsAccumulator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::group_values::{GroupValues, new_group_values};
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy, aggregate_expressions, evaluate_group_by,
    evaluate_many,
};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    InputDistributionRequirements, InputOrderMode, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream, Statistics, StatisticsArgs,
};
use futures::{Stream, StreamExt};

use crate::config::BucketedFinalAggregation;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const BUCKETED_FINAL_AGGREGATE_RULE: &str = "bucketed_final_aggregate";

/// The name prefix of the exec's per-partition memory consumer:
/// `"BucketedFinalAggregate[{partition}]"`.
pub const BUCKETED_FINAL_AGGREGATE_CONSUMER_PREFIX: &str = "BucketedFinalAggregate[";

/// The largest `bits` the exec honours; a larger configured value is read as
/// this. 65,536 buckets per partition is already far past the point where a
/// bucket's table fits a cache level.
pub const BUCKETED_FINAL_AGGREGATE_MAX_BITS: u8 = 16;

/// The seed of the bucket hash. Any value but the repartition exchange's 0
/// (`REPARTITION_RANDOM_STATE`) decorrelates the bucket from the partition.
const BUCKET_HASH_SEED: u64 = 0x5241_5645_4c42_4b54;

/// Replaces every qualifying `FinalPartitioned` [`AggregateExec`] with a
/// [`BucketedFinalAggregateExec`]. See the module docs.
#[derive(Debug)]
pub struct BucketedFinalAggregateRule {
    config: BucketedFinalAggregation,
}

impl BucketedFinalAggregateRule {
    /// A rule that builds every exec it plans with `config`.
    pub fn new(config: BucketedFinalAggregation) -> Self {
        BucketedFinalAggregateRule { config }
    }

    /// Whether `aggregate` qualifies for the bucketed final (ADR-3565
    /// decision 1's conditions). See the module docs.
    pub fn admits(aggregate: &AggregateExec) -> bool {
        if *aggregate.mode() != AggregateMode::FinalPartitioned
            || aggregate.limit_options().is_some()
            || *aggregate.input_order_mode() != InputOrderMode::Linear
            || !aggregate.dynamic_expressions_produced().is_empty()
        {
            return false;
        }
        let group_by = aggregate.group_expr();
        let single_set = matches!(group_by.groups(), [set] if set.iter().all(|null| !null));
        if group_by.has_grouping_set()
            || !single_set
            || group_by.expr().is_empty()
            || !group_by.null_expr().is_empty()
        {
            return false;
        }
        let Ok(group_schema) = group_by.group_schema(&aggregate.input().schema()) else {
            return false;
        };
        let keys_qualify = group_schema.fields().iter().all(|field| {
            field.name() != Aggregate::INTERNAL_GROUPING_ID && is_bucketed_key(field.data_type())
        });
        keys_qualify
            && aggregate.aggr_expr().iter().all(|aggr| {
                aggr.groups_accumulator_supported()
                    && aggr.order_bys().is_empty()
                    && aggr.state_fields().is_ok_and(|fields| {
                        fields
                            .iter()
                            .all(|field| is_bucketed_state(field.data_type()))
                    })
            })
    }
}

impl PhysicalOptimizerRule for BucketedFinalAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(aggregate) = node.downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            if !Self::admits(aggregate) {
                return Ok(Transformed::no(node));
            }
            let exec = BucketedFinalAggregateExec::new(Arc::new(aggregate.clone()), self.config);
            Ok(Transformed::yes(Arc::new(exec) as Arc<dyn ExecutionPlan>))
        })
        .data()
    }

    fn name(&self) -> &str {
        BUCKETED_FINAL_AGGREGATE_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A group key type the exec buckets: a fixed-width integer or temporal type
/// `new_group_values` keeps in a primitive table. Decimal128 and Duration
/// also dispatch to one and are left out until measured (ADR-3565).
fn is_bucketed_key(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Interval(_)
    )
}

/// An accumulator state type the exec routes: fixed width and not float, so
/// a chunk copies it by bytes and no float sum is reassociated.
fn is_bucketed_state(data_type: &DataType) -> bool {
    is_bucketed_key(data_type)
        || matches!(
            data_type,
            DataType::Duration(_) | DataType::Decimal128(_, _)
        )
}

/// The two-phase final aggregate. One child, the replaced aggregate's
/// partitioning, output schema and input distribution. See the module docs.
pub struct BucketedFinalAggregateExec {
    aggregate: Arc<AggregateExec>,
    config: BucketedFinalAggregation,
    metrics: ExecutionPlanMetricsSet,
}

impl BucketedFinalAggregateExec {
    /// Run `aggregate`, a `FinalPartitioned` aggregate that
    /// [`BucketedFinalAggregateRule::admits`], in two phases under `config`.
    /// A stream over an aggregate that does not qualify fails with an
    /// internal error on its first batch rather than misreading a column.
    pub fn new(aggregate: Arc<AggregateExec>, config: BucketedFinalAggregation) -> Self {
        BucketedFinalAggregateExec {
            aggregate,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The aggregate this exec replaced, whose input, expressions and
    /// properties it runs with.
    pub fn aggregate(&self) -> &Arc<AggregateExec> {
        &self.aggregate
    }

    /// The tuning this exec runs with.
    pub fn config(&self) -> BucketedFinalAggregation {
        self.config
    }
}

impl fmt::Debug for BucketedFinalAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BucketedFinalAggregateExec")
    }
}

impl DisplayAs for BucketedFinalAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let keys: Vec<&str> = self
                    .aggregate
                    .group_expr()
                    .expr()
                    .iter()
                    .map(|(_, name)| name.as_str())
                    .collect();
                let aggregates: Vec<&str> = self
                    .aggregate
                    .aggr_expr()
                    .iter()
                    .map(|aggr| aggr.name())
                    .collect();
                write!(
                    f,
                    "BucketedFinalAggregateExec: gby=[{}], aggr=[{}], engage_groups={}, \
                     bits={}, routed_chunk_rows={}",
                    keys.join(", "),
                    aggregates.join(", "),
                    self.config.engage_groups,
                    self.config.bits.min(BUCKETED_FINAL_AGGREGATE_MAX_BITS),
                    self.config.routed_chunk_rows.max(1),
                )
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for BucketedFinalAggregateExec {
    fn name(&self) -> &'static str {
        "BucketedFinalAggregateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.aggregate.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.aggregate.children()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.aggregate.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.aggregate.benefits_from_input_partitioning()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        self.aggregate.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let rebuilt = Arc::clone(&self.aggregate).replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let Some(aggregate) = rebuilt.downcast_ref::<AggregateExec>() else {
            return Err(DataFusionError::Internal(
                "BucketedFinalAggregateExec: the rebuilt aggregate is not an AggregateExec"
                    .to_string(),
            ));
        };
        Ok(Arc::new(BucketedFinalAggregateExec::new(
            Arc::new(aggregate.clone()),
            self.config,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self
            .aggregate
            .input()
            .execute(partition, Arc::clone(&context))?;
        let stream = BucketedStream::try_new(
            &self.aggregate,
            self.config,
            input,
            partition,
            &context,
            BucketMetrics::new(&self.metrics, partition),
        )?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.aggregate.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> DFResult<Arc<Statistics>> {
        self.aggregate.statistics_from_inputs(input_stats, args)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.aggregate.cardinality_effect()
    }
}

/// What the exec reports through `EXPLAIN ANALYZE`, per partition.
struct BucketMetrics {
    baseline: BaselineMetrics,
    /// Buckets holding at least one routed row when input ended; zero for a
    /// partition that never engaged.
    buckets_non_empty: Count,
    /// Rows routed into buckets: the single table's state rows at
    /// engagement plus every later input row.
    routed_rows: Count,
    /// 1 when the partition's table reached `engage_groups` and routed, else
    /// 0.
    engaged: Count,
}

impl BucketMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        BucketMetrics {
            baseline: BaselineMetrics::new(metrics, partition),
            buckets_non_empty: MetricBuilder::new(metrics).counter("buckets_non_empty", partition),
            routed_rows: MetricBuilder::new(metrics).counter("routed_rows", partition),
            engaged: MetricBuilder::new(metrics).counter("engaged", partition),
        }
    }
}

/// One fixed-width column of an input batch, read row by row.
struct SourceColumn<'a> {
    values: &'a [u8],
    offset: usize,
    nulls: Option<&'a NullBuffer>,
}

/// One column of a bucket's open chunk: values and a validity bitmap,
/// allocated at the chunk's full row capacity.
struct ColumnChunk {
    data_type: DataType,
    width: usize,
    values: MutableBuffer,
    validity: MutableBuffer,
    null_count: usize,
}

impl ColumnChunk {
    fn finish(self, len: usize) -> DFResult<ArrayRef> {
        let nulls = (self.null_count > 0)
            .then(|| NullBuffer::new(BooleanBuffer::new(Buffer::from(self.validity), 0, len)));
        let data = ArrayData::builder(self.data_type)
            .len(len)
            .add_buffer(Buffer::from(self.values))
            .nulls(nulls)
            .build()?;
        Ok(make_array(data))
    }
}

/// A bucket's chunk still taking rows.
struct OpenChunk {
    columns: Vec<ColumnChunk>,
    len: usize,
    charged: usize,
}

impl OpenChunk {
    /// A chunk of `rows` rows per column, allocated but not yet charged.
    fn allocate(types: &[(DataType, usize)], rows: usize) -> Self {
        let columns: Vec<ColumnChunk> = types
            .iter()
            .map(|(data_type, width)| ColumnChunk {
                data_type: data_type.clone(),
                width: *width,
                values: MutableBuffer::with_capacity(rows * width),
                validity: MutableBuffer::from_len_zeroed(bit_util::ceil(rows, 8)),
                null_count: 0,
            })
            .collect();
        let charged = columns
            .iter()
            .map(|column| column.values.capacity() + column.validity.capacity())
            .sum();
        OpenChunk {
            columns,
            len: 0,
            charged,
        }
    }

    fn append(&mut self, sources: &[SourceColumn<'_>], rows: &[u32]) {
        for (column, source) in self.columns.iter_mut().zip(sources) {
            let width = column.width;
            for (at, &row) in rows.iter().enumerate() {
                let row = row as usize;
                let start = (source.offset + row) * width;
                column
                    .values
                    .extend_from_slice(&source.values[start..start + width]);
                if source.nulls.is_some_and(|nulls| nulls.is_null(row)) {
                    column.null_count += 1;
                } else {
                    bit_util::set_bit(column.validity.as_slice_mut(), self.len + at);
                }
            }
        }
        self.len += rows.len();
    }

    fn seal(self) -> DFResult<SealedChunk> {
        let len = self.len;
        let columns = self
            .columns
            .into_iter()
            .map(|column| column.finish(len))
            .collect::<DFResult<Vec<_>>>()?;
        Ok(SealedChunk {
            columns,
            charged: self.charged,
        })
    }
}

/// A finished chunk: one array per routed column, and the bytes charged for
/// it.
struct SealedChunk {
    columns: Vec<ArrayRef>,
    charged: usize,
}

#[derive(Default)]
struct Bucket {
    sealed: Vec<SealedChunk>,
    open: Option<OpenChunk>,
    rows: usize,
}

/// The phase-1 router: every column's type and width, and the buckets.
struct Router {
    types: Vec<(DataType, usize)>,
    key_count: usize,
    bits: u8,
    chunk_rows: usize,
    random_state: RandomState,
    buckets: Vec<Bucket>,
    hashes: Vec<u64>,
    order: Vec<u32>,
    offsets: Vec<usize>,
}

impl Router {
    fn new(types: Vec<(DataType, usize)>, key_count: usize, bits: u8, chunk_rows: usize) -> Self {
        let bucket_count = 1usize << bits;
        Router {
            types,
            key_count,
            bits,
            chunk_rows,
            random_state: RandomState::with_seed(BUCKET_HASH_SEED),
            buckets: (0..bucket_count).map(|_| Bucket::default()).collect(),
            hashes: Vec::new(),
            order: Vec::new(),
            offsets: vec![0; bucket_count + 1],
        }
    }

    /// The bucket of a key hash: its top `bits` bits.
    fn bucket_of(&self, hash: u64) -> usize {
        match self.bits {
            0 => 0,
            bits => (hash >> (64 - u32::from(bits))) as usize,
        }
    }

    fn scratch_size(&self) -> usize {
        self.hashes.capacity() * size_of::<u64>()
            + self.order.capacity() * size_of::<u32>()
            + self.offsets.capacity() * size_of::<usize>()
            + self.buckets.capacity() * size_of::<Bucket>()
    }

    /// Route `columns` (keys, then every accumulator's state, in the router's
    /// order) into the buckets, charging each new chunk to `reservation`
    /// before a row is written to it.
    fn route(
        &mut self,
        columns: &[ArrayRef],
        reservation: &mut MemoryReservation,
    ) -> DFResult<usize> {
        if columns.len() != self.types.len() {
            return Err(DataFusionError::Internal(format!(
                "BucketedFinalAggregateExec: {} routed columns, expected {}",
                columns.len(),
                self.types.len()
            )));
        }
        let rows = columns.first().map_or(0, |column| column.len());
        if rows == 0 {
            return Ok(0);
        }
        let data: Vec<ArrayData> = columns.iter().map(|column| column.to_data()).collect();
        let mut sources = Vec::with_capacity(columns.len());
        for ((column, data), (data_type, width)) in columns.iter().zip(&data).zip(&self.types) {
            let values = match data.buffers() {
                [values] => values.as_slice(),
                _ => &[],
            };
            if column.data_type() != data_type
                || column.len() != rows
                || values.len() < (data.offset() + rows) * width
            {
                return Err(DataFusionError::Internal(format!(
                    "BucketedFinalAggregateExec: a routed {} column of {} rows does not match \
                     the expected {data_type} column of {rows} rows",
                    column.data_type(),
                    column.len(),
                )));
            }
            sources.push(SourceColumn {
                values,
                offset: data.offset(),
                nulls: column.nulls(),
            });
        }

        self.hashes.clear();
        self.hashes.resize(rows, 0);
        create_hashes(
            &columns[..self.key_count],
            &self.random_state,
            &mut self.hashes,
        )?;

        // Counting sort of the rows by bucket, so each bucket's rows are
        // appended column by column.
        let bucket_count = self.buckets.len();
        self.offsets.clear();
        self.offsets.resize(bucket_count + 1, 0);
        for index in 0..rows {
            let bucket = self.bucket_of(self.hashes[index]);
            self.offsets[bucket + 1] += 1;
        }
        for bucket in 0..bucket_count {
            self.offsets[bucket + 1] += self.offsets[bucket];
        }
        self.order.clear();
        self.order.resize(rows, 0);
        let mut next = self.offsets.clone();
        for index in 0..rows {
            let bucket = self.bucket_of(self.hashes[index]);
            self.order[next[bucket]] = index as u32;
            next[bucket] += 1;
        }

        for bucket in 0..bucket_count {
            let (start, end) = (self.offsets[bucket], self.offsets[bucket + 1]);
            let mut done = start;
            while done < end {
                let target = &mut self.buckets[bucket];
                if target
                    .open
                    .as_ref()
                    .is_none_or(|open| open.len == self.chunk_rows)
                {
                    if let Some(full) = target.open.take() {
                        target.sealed.push(full.seal()?);
                    }
                    let chunk = OpenChunk::allocate(&self.types, self.chunk_rows);
                    try_grow_named(reservation, chunk.charged)?;
                    target.open = Some(chunk);
                }
                let Some(open) = target.open.as_mut() else {
                    return Err(DataFusionError::Internal(
                        "BucketedFinalAggregateExec: a bucket has no open chunk".to_string(),
                    ));
                };
                let take = (end - done).min(self.chunk_rows - open.len);
                open.append(&sources, &self.order[done..done + take]);
                target.rows += take;
                done += take;
            }
        }
        Ok(rows)
    }

    /// Every chunk of `bucket`, the open one sealed, leaving the bucket
    /// empty.
    fn take_bucket(&mut self, bucket: usize) -> DFResult<Vec<SealedChunk>> {
        let Some(target) = self.buckets.get_mut(bucket) else {
            return Ok(Vec::new());
        };
        let mut chunks = std::mem::take(&mut target.sealed);
        if let Some(open) = target.open.take() {
            chunks.push(open.seal()?);
        }
        target.rows = 0;
        Ok(chunks)
    }
}

/// `reservation.try_grow(additional)`, with a refusal naming the consumer.
fn try_grow_named(reservation: &mut MemoryReservation, additional: usize) -> DFResult<()> {
    reservation.try_grow(additional).map_err(|err| match err {
        DataFusionError::ResourcesExhausted(message) => {
            DataFusionError::ResourcesExhausted(format!(
                "{} refused {additional} bytes: {message}",
                reservation.consumer().name()
            ))
        }
        other => other,
    })
}

/// A bucket's evaluated output, handed out `batch_size` rows at a time.
struct PendingOutput {
    batch: RecordBatch,
    offset: usize,
}

enum Phase {
    Ingest,
    Drain { next_bucket: usize },
    Done,
}

struct BucketedStream {
    schema: SchemaRef,
    input: Option<SendableRecordBatchStream>,
    group_by: PhysicalGroupBy,
    state_args: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    group_values: Box<dyn GroupValues>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    group_indices: Vec<usize>,
    /// Every routed column's type and width: keys, then each accumulator's
    /// state.
    routed_types: Vec<(DataType, usize)>,
    key_count: usize,
    router: Option<Router>,
    pending: Option<PendingOutput>,
    phase: Phase,
    config: BucketedFinalAggregation,
    batch_size: usize,
    reservation: MemoryReservation,
    /// The part of the reservation charged by `size()`: tables, scratch and
    /// pending output. The rest is bucket chunks, charged at capacity.
    table_bytes: usize,
    metrics: BucketMetrics,
}

impl BucketedStream {
    fn try_new(
        aggregate: &AggregateExec,
        config: BucketedFinalAggregation,
        input: SendableRecordBatchStream,
        partition: usize,
        context: &Arc<TaskContext>,
        metrics: BucketMetrics,
    ) -> DFResult<Self> {
        let input_schema = aggregate.input().schema();
        let group_by = aggregate.group_expr().clone();
        let state_args = aggregate_expressions(
            aggregate.aggr_expr(),
            aggregate.mode(),
            group_by.num_group_exprs(),
        )?;
        let accumulators = aggregate
            .aggr_expr()
            .iter()
            .map(|aggr| aggr.create_groups_accumulator())
            .collect::<DFResult<Vec<_>>>()?;
        let group_schema = group_by.group_schema(&input_schema)?;
        let key_count = group_schema.fields().len();
        let mut routed_types = Vec::new();
        for data_type in group_schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .chain(
                state_args
                    .iter()
                    .flatten()
                    .map(|expr| expr.data_type(&input_schema))
                    .collect::<DFResult<Vec<_>>>()?,
            )
        {
            let Some(width) = data_type.primitive_width() else {
                return Err(DataFusionError::Internal(format!(
                    "BucketedFinalAggregateExec: routed column type {data_type} is not fixed width"
                )));
            };
            routed_types.push((data_type, width));
        }
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;
        let reservation = MemoryConsumer::new(format!(
            "{BUCKETED_FINAL_AGGREGATE_CONSUMER_PREFIX}{partition}]"
        ))
        .register(context.memory_pool());
        Ok(BucketedStream {
            schema: aggregate.schema(),
            input: Some(input),
            group_by,
            state_args,
            group_values,
            accumulators,
            group_indices: Vec::new(),
            routed_types,
            key_count,
            router: None,
            pending: None,
            phase: Phase::Ingest,
            config,
            batch_size: context.session_config().batch_size().max(1),
            reservation,
            table_bytes: 0,
            metrics,
        })
    }

    /// Merge `columns` (keys, then state) into the reused table and
    /// accumulators.
    fn merge(&mut self, columns: &[ArrayRef]) -> DFResult<()> {
        let (keys, mut states) = columns.split_at(self.key_count.min(columns.len()));
        self.group_values.intern(keys, &mut self.group_indices)?;
        let total_groups = self.group_values.len();
        for (accumulator, args) in self.accumulators.iter_mut().zip(&self.state_args) {
            let (values, rest) = states.split_at(args.len().min(states.len()));
            accumulator.merge_batch(values, &self.group_indices, total_groups)?;
            states = rest;
        }
        Ok(())
    }

    /// Keys, then every accumulator's state columns, of one input batch.
    fn routed_columns(&self, batch: &RecordBatch) -> DFResult<Vec<ArrayRef>> {
        let Some(mut columns) = evaluate_group_by(&self.group_by, batch)?.into_iter().next() else {
            return Err(DataFusionError::Internal(
                "BucketedFinalAggregateExec: the group by evaluated to no grouping set".to_string(),
            ));
        };
        for args in evaluate_many(&self.state_args, batch)? {
            columns.extend(args);
        }
        Ok(columns)
    }

    fn ingest(&mut self, batch: &RecordBatch) -> DFResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let elapsed = self.metrics.baseline.elapsed_compute().clone();
        let _timer = elapsed.timer();
        let columns = self.routed_columns(batch)?;
        if let Some(router) = self.router.as_mut() {
            let routed = router.route(&columns, &mut self.reservation)?;
            self.metrics.routed_rows.add(routed);
        } else {
            self.merge(&columns)?;
            if self.group_values.len() >= self.config.engage_groups {
                self.engage()?;
            }
        }
        self.charge_table()
    }

    /// Take the single table's state and route it; every later batch is
    /// routed too.
    fn engage(&mut self) -> DFResult<()> {
        let bits = self.config.bits.min(BUCKETED_FINAL_AGGREGATE_MAX_BITS);
        let mut router = Router::new(
            self.routed_types.clone(),
            self.key_count,
            bits,
            self.config.routed_chunk_rows.max(1),
        );
        let mut state = self.group_values.emit(EmitTo::All)?;
        for accumulator in &mut self.accumulators {
            state.extend(accumulator.state(EmitTo::All)?);
        }
        self.group_values.clear_shrink(0);
        self.group_indices = Vec::new();
        let routed = router.route(&state, &mut self.reservation)?;
        self.metrics.routed_rows.add(routed);
        self.metrics.engaged.add(1);
        self.router = Some(router);
        Ok(())
    }

    /// Bytes charged by `size()`: the tables, the per-batch scratch and the
    /// pending output.
    fn table_size(&self) -> usize {
        self.group_values.size()
            + self
                .accumulators
                .iter()
                .map(|accumulator| accumulator.size())
                .sum::<usize>()
            + self.group_indices.capacity() * size_of::<usize>()
            + self.router.as_ref().map_or(0, Router::scratch_size)
            + self
                .pending
                .as_ref()
                .map_or(0, |pending| pending.batch.get_array_memory_size())
    }

    fn charge_table(&mut self) -> DFResult<()> {
        let size = self.table_size();
        if size > self.table_bytes {
            try_grow_named(&mut self.reservation, size - self.table_bytes)?;
        } else {
            self.reservation.shrink(self.table_bytes - size);
        }
        self.table_bytes = size;
        Ok(())
    }

    /// Evaluate the reused table into the pending output and clear it,
    /// keeping room for `keep_groups` groups.
    fn evaluate_table(&mut self, keep_groups: usize) -> DFResult<()> {
        if self.group_values.is_empty() {
            return Ok(());
        }
        let mut columns = self.group_values.emit(EmitTo::All)?;
        for accumulator in &mut self.accumulators {
            columns.push(accumulator.evaluate(EmitTo::All)?);
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        self.group_values.clear_shrink(keep_groups);
        self.pending = Some(PendingOutput { batch, offset: 0 });
        Ok(())
    }

    /// Merge one bucket, release its chunks as they are merged, and evaluate
    /// it into the pending output.
    fn aggregate_bucket(&mut self, bucket: usize) -> DFResult<()> {
        let elapsed = self.metrics.baseline.elapsed_compute().clone();
        let _timer = elapsed.timer();
        let chunks = match self.router.as_mut() {
            Some(router) => router.take_bucket(bucket)?,
            None => Vec::new(),
        };
        for chunk in chunks {
            self.merge(&chunk.columns)?;
            drop(chunk.columns);
            self.reservation.shrink(chunk.charged);
            self.charge_table()?;
        }
        let groups = self.group_values.len();
        self.evaluate_table(groups)?;
        self.charge_table()
    }

    /// The next `batch_size` rows of the pending output, copied so the batch
    /// owns its buffers.
    fn next_output(&mut self) -> DFResult<Option<RecordBatch>> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(None);
        };
        let total = pending.batch.num_rows();
        let len = (total - pending.offset).min(self.batch_size);
        let batch = copy_rows(&pending.batch, pending.offset, len)?;
        pending.offset += len;
        if pending.offset >= total {
            self.pending = None;
            self.charge_table()?;
        }
        self.metrics.baseline.record_output(batch.num_rows());
        Ok(Some(batch))
    }

    fn finish_input(&mut self) -> DFResult<()> {
        self.input = None;
        match self.router.as_ref() {
            Some(router) => {
                let non_empty = router
                    .buckets
                    .iter()
                    .filter(|bucket| bucket.rows > 0)
                    .count();
                self.metrics.buckets_non_empty.add(non_empty);
                Ok(())
            }
            None => {
                self.evaluate_table(0)?;
                self.charge_table()
            }
        }
    }

    /// Drop every buffer and release the whole reservation.
    fn release(&mut self) {
        self.input = None;
        self.router = None;
        self.pending = None;
        self.group_values.clear_shrink(0);
        self.accumulators.clear();
        self.group_indices = Vec::new();
        self.reservation.free();
        self.table_bytes = 0;
        self.phase = Phase::Done;
        self.metrics.baseline.done();
    }

    fn fail(&mut self, err: DataFusionError) -> Poll<Option<DFResult<RecordBatch>>> {
        self.release();
        Poll::Ready(Some(Err(err)))
    }
}

/// `len` rows of `batch` from `offset`, copied into fresh buffers.
fn copy_rows(batch: &RecordBatch, offset: usize, len: usize) -> DFResult<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let data = column.to_data();
            let mut copy = MutableArrayData::new(vec![&data], false, len);
            copy.try_extend(0, offset, offset + len)?;
            Ok(make_array(copy.freeze()))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(len)),
    )?)
}

impl Stream for BucketedStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            match this.phase {
                Phase::Ingest => {
                    let Some(input) = this.input.as_mut() else {
                        this.phase = Phase::Drain { next_bucket: 0 };
                        continue;
                    };
                    match ready!(input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            if let Err(err) = this.ingest(&batch) {
                                return this.fail(err);
                            }
                            // Yield after every batch, so a dropped stream
                            // or a breached ceiling acts within one batch.
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Some(Err(err)) => return this.fail(err),
                        None => {
                            if let Err(err) = this.finish_input() {
                                return this.fail(err);
                            }
                            this.phase = Phase::Drain { next_bucket: 0 };
                        }
                    }
                }
                Phase::Drain { next_bucket } => {
                    match this.next_output() {
                        Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                        Ok(None) => {}
                        Err(err) => return this.fail(err),
                    }
                    let bucket_count = this
                        .router
                        .as_ref()
                        .map_or(0, |router| router.buckets.len());
                    if next_bucket >= bucket_count {
                        this.release();
                        return Poll::Ready(None);
                    }
                    this.phase = Phase::Drain {
                        next_bucket: next_bucket + 1,
                    };
                    if let Err(err) = this.aggregate_bucket(next_bucket) {
                        return this.fail(err);
                    }
                }
                Phase::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for BucketedStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
