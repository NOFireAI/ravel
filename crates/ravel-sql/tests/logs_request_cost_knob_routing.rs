//! Differential fixture for ADR-0904 task 904-4: the request-cost knob
//! (`--logs-request-cost-bytes`, `EngineConfig::logs_request_cost_bytes`,
//! reaching the fetcher through `LogSegmentFetcher::with_request_cost_bytes`)
//! selects the logs read ROUTE, and the accounting handle's opens-by-shape
//! counters are the evidence of which route ran.
//!
//! The same statement over the same fixture runs at two extremes of the knob and
//! the counters must INVERT:
//!
//! - A very low request cost makes bytes expensive relative to round trips, so
//!   the narrow projection routes ranged: `logs_ranged_opens == SEGMENTS` and
//!   `logs_whole_object_opens == 0`.
//! - A very high request cost makes round trips expensive, so the SAME statement
//!   routes whole-object: the two counters swap.
//!
//! Both are exact equalities against the fixture's segment count, in both
//! directions. A routing change that only ever moves one way cannot be caught by
//! a "at least one" assertion, and neither can a recording site that was never
//! reached: `PartitionCtx::record_open_shape` is what turns
//! `PartitionCtx::open_by_column_chunk`'s answer into those counters, and
//! reverting either route's increment fails an assertion here.
//!
//! The answer must not change. The knob selects a read path, never a result, so
//! the full `(ts, d00)` row set is compared between the two runs as well as
//! against the fixture. A counters-only differential would pass on a ranged read
//! that dropped or duplicated rows, which is the one bug that matters most here.
//!
//! # Why the fixture's objects are a megabyte each
//!
//! The knob does not reach `ranged_projection_pays` unclamped.
//! `BlockRangeFetcher::effective_whole_object_threshold` is
//! `max(request_cost_bytes * WHOLE_OBJECT_REQUEST_MULTIPLE,
//! DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD)`, so the low extreme bottoms out at the
//! 512 KiB floor however small the configured cost is. The ranged route is then
//! reachable only for a segment whose SKIPPED bytes (`object_size * (1 -
//! projected_fraction)`) exceed that floor, which is why each segment here
//! carries about a megabyte instead of the ~100 KiB the sibling routing fixture
//! uses. `low_extreme_clears_the_whole_object_threshold_floor` pins that
//! precondition, so a fixture that shrinks below it fails as a fixture bug
//! rather than silently pinning nothing.
//!
//! # Why the block-range threshold is left at its default
//!
//! `LogSegmentFetcher::with_block_range_threshold` sets the block-range
//! fetcher's own whole-object crossover EXPLICITLY, and an explicit crossover
//! wins over the request-cost derivation. Calling it here (as most fixtures in
//! this crate do) would pin the break-even to a constant and the knob would move
//! nothing: both extremes would route the same way and the test would pin the
//! fixture rather than the knob. The default `block_range_threshold` (512 KiB)
//! is left in place instead, which every object in this fixture clears, so the
//! request-cost model is the only thing deciding.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{
    BlockRangeFetcher, CacheFetchError, DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD, LogSegmentFetcher,
};
use ravel_sql::{DeclaredColumn, DeclaredType, FIRST_DECLARED_COL, LOG_COL_TS, LogsTableProvider};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: [u8; 16] = [7u8; 16];

/// Segments in the fixture, and the partitions requested. Equal so a
/// predicate-free full-window statement clears the whole-segment fast path's
/// `relevant_segments >= target_partitions` conjunct, which is the path both
/// routes live on. This is also the exact count both counters are pinned to.
const SEGMENTS: usize = 3;
const PARTS: usize = 3;
/// Blocks per segment (one record per block).
const BLOCKS_PER_SEG: usize = 6;

/// Declared typed attribute columns the tenant carries, `d00`..`d09`, so a
/// single-column projection is genuinely narrow: three object columns of twenty
/// (`ts` and `stream_ref` are always decoded).
const DECLARED_COLUMNS: usize = 10;

/// Filler bytes in each declared column's value, per record. Sized so a segment
/// clears the 512 KiB floor on the low extreme's break-even with room to spare;
/// see the header.
const DECLARED_BYTES: usize = 16_384;

/// Suffix probe length for the ranged path, sized to this fixture's object tail
/// rather than to production objects.
const SUFFIX_LEN: u64 = 16_384;

/// The fraction of the object's columns the narrow projection reads: `ts`,
/// `stream_ref` and `d00` out of the ten fixed object columns plus
/// `DECLARED_COLUMNS` declared ones. This is what `LogsScanExec` computes for
/// [`narrow_projection`] and hands `ranged_projection_pays`; it is restated here
/// so the reachability precondition can be checked without executing a plan.
const NARROW_FRACTION: f64 = 3.0 / 20.0;

/// Request cost at the low extreme: one byte, so a saved round trip is worth
/// almost nothing and the ranged route wins wherever the floor allows.
const LOW_REQUEST_COST_BYTES: u64 = 1;

/// Request cost at the high extreme: 64 MiB, so the break-even is 320 MiB and no
/// segment in any plausible fixture can save its way past it.
const HIGH_REQUEST_COST_BYTES: u64 = 64 << 20;

fn identity(seq: u64) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    }
}

fn one_record_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    }
}

/// Pseudo-random printable filler so the writer's compression cannot shrink the
/// object back under the threshold this fixture has to clear.
fn filler(seed: u64, len: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = seed;
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        out.push(ALPHABET[(z & 63) as usize] as char);
    }
    out
}

fn declared_key(k: usize) -> String {
    format!("d{k:02}")
}

fn declared_columns() -> Vec<DeclaredColumn> {
    (0..DECLARED_COLUMNS)
        .map(|k| DeclaredColumn::new(declared_key(k), DeclaredType::Str))
        .collect()
}

fn record(seg: usize, blk: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("svc".to_string()),
    )];
    let ts = (seg * 1_000_000 + blk) as i64;
    let attrs = (0..DECLARED_COLUMNS)
        .map(|k| {
            let seed = (seg as u64) << 40 | (blk as u64) << 20 | k as u64;
            (
                declared_key(k),
                AttrValue::Str(filler(seed, DECLARED_BYTES)),
            )
        })
        .collect();
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("s{seg}-b{blk}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

async fn write_segment(store: &dyn ObjectStoreBackend, seg: usize) -> SegmentRef {
    let recs: Vec<LogRecord> = (0..BLOCKS_PER_SEG).map(|b| record(seg, b)).collect();
    let mut w = RlogWriter::new(one_record_blocks(), identity((seg + 1) as u64));
    for r in &recs {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    let key = format!("logs/seg{seg}.rlog");
    let content_hash = *blake3::hash(&bytes).as_bytes();
    store
        .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    let min = recs.iter().map(|r| r.ts_ns).min().unwrap();
    let max = recs.iter().map(|r| r.ts_ns).max().unwrap();
    SegmentRef {
        data_object_key: key,
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: recs.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: (seg + 1) as u64,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
    }
}

async fn build_snapshot(store: &dyn ObjectStoreBackend) -> Snapshot {
    let mut segments = Vec::with_capacity(SEGMENTS);
    for s in 0..SEGMENTS {
        segments.push(write_segment(store, s).await);
    }
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

fn read_cache() -> Arc<Cache<CacheFetchError>> {
    let bytes = 64 << 20;
    Arc::new(Cache::new(CacheLimits::new(
        bytes,
        (bytes / 4096) as usize,
        bytes,
    )))
}

/// The fixture's fetcher at one setting of the request-cost knob, wired the way
/// `services/ravel-server`'s query path wires `logs_request_cost_bytes`: through
/// the block-range fetcher's request cost, with NO explicit whole-object
/// crossover to override the derivation. See the header for why the block-range
/// threshold is left at its default here.
fn fetcher(store: Arc<dyn ObjectStoreBackend>, request_cost_bytes: u64) -> LogSegmentFetcher {
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_suffix_len(SUFFIX_LEN)
        // Under RLOG v4 the hole between two wanted pages of one column is the
        // other blocks' pages of that column, so a nonzero gap would fuse the
        // BLOCKS section into one range and put the narrow shape back at
        // full-object bytes.
        .with_coalesce_gap(0)
        .with_request_cost_bytes(request_cost_bytes);
    LogSegmentFetcher::new(store)
        .with_block_range(block_range)
        .with_cache(read_cache())
}

/// Projection over the resolved full schema: `ts` plus the FIRST declared
/// column. Three object columns of twenty, the [`NARROW_FRACTION`] shape.
fn narrow_projection() -> Vec<usize> {
    vec![LOG_COL_TS, FIRST_DECLARED_COL]
}

/// What one run of the statement routed and returned.
struct Routed {
    whole_object_opens: u64,
    ranged_opens: u64,
    rows: usize,
    /// Every `(ts, d00)` pair the run emitted, for the cross-extreme row
    /// comparison.
    pairs: BTreeSet<(i64, String)>,
}

/// Plan and run the narrow-projection statement over a fresh fixture whose
/// fetcher carries `request_cost_bytes`, and report the accounting handle's
/// opens-by-shape counters alongside the rows.
async fn run(request_cost_bytes: u64) -> Routed {
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref()).await;
    let store: Arc<dyn ObjectStoreBackend> = base;
    let accounting = QueryAccounting::new();
    let prov = Arc::new(
        LogsTableProvider::new(
            snapshot,
            TenantHash(TENANT),
            fetcher(store, request_cost_bytes),
            accounting.clone(),
        )
        .with_declared_columns(declared_columns()),
    );

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(PARTS));
    let projection = narrow_projection();
    let plan = TableProvider::scan(prov.as_ref(), &ctx.state(), Some(&projection), &[], None)
        .await
        .expect("scan");
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect");

    let snap = accounting.snapshot();
    Routed {
        whole_object_opens: snap.logs_whole_object_opens,
        ranged_opens: snap.logs_ranged_opens,
        rows: batches.iter().map(RecordBatch::num_rows).sum(),
        pairs: ts_and_first_declared(&batches),
    }
}

/// The `(ts, d00)` pairs in a result whose first column is `ts`. `d00` is
/// located by name because the two runs need not agree on anything but values.
fn ts_and_first_declared(batches: &[RecordBatch]) -> BTreeSet<(i64, String)> {
    let mut out = BTreeSet::new();
    for batch in batches {
        let Some(d0) = batch.schema().index_of(&declared_key(0)).ok() else {
            continue;
        };
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts at output position 0");
        // A declared `Str` column is a `Dictionary(Int32, Utf8)`; flatten it so
        // the comparison is over values, not over dictionary layout.
        let flat = cast(batch.column(d0), &DataType::Utf8).expect("d00 casts to Utf8");
        let vals = flat
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8 yields a StringArray");
        for i in 0..batch.num_rows() {
            let v = if vals.is_null(i) {
                String::new()
            } else {
                vals.value(i).to_string()
            };
            out.insert((ts.value(i), v));
        }
    }
    out
}

const TOTAL_ROWS: usize = SEGMENTS * BLOCKS_PER_SEG;

/// The fixture precondition the header describes: at the LOW extreme the
/// request-cost break-even is clamped to `DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`,
/// so every segment's skipped bytes must beat that floor for the ranged route to
/// be reachable at all. At the HIGH extreme none of them may.
///
/// Without this, a fixture that shrank below the floor would route whole-object
/// at both extremes, and the differential test would pin nothing while still
/// passing its "identical rows" half.
#[tokio::test]
async fn low_extreme_clears_the_whole_object_threshold_floor() {
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref()).await;
    let store: Arc<dyn ObjectStoreBackend> = base;
    let low = fetcher(Arc::clone(&store), LOW_REQUEST_COST_BYTES);
    let high = fetcher(store, HIGH_REQUEST_COST_BYTES);

    for seg in &snapshot.segments {
        let skipped = seg.object_size as f64 * (1.0 - NARROW_FRACTION);
        assert!(
            skipped > DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD as f64,
            "segment {} skips {skipped} bytes under the narrow projection, which must beat the \
             {DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD}-byte floor \
             `effective_whole_object_threshold` clamps the low extreme to; enlarge the fixture",
            seg.data_object_key
        );
        assert!(
            low.ranged_projection_pays(seg.object_size, NARROW_FRACTION),
            "the low extreme must route segment {} ranged",
            seg.data_object_key
        );
        assert!(
            !high.ranged_projection_pays(seg.object_size, NARROW_FRACTION),
            "the high extreme must route segment {} whole-object",
            seg.data_object_key
        );
    }
}

/// The knob-extremes differential: same statement, same fixture, opposite ends
/// of `--logs-request-cost-bytes`. The opens-by-shape counters invert exactly,
/// and the rows do not move.
#[tokio::test]
async fn request_cost_extremes_invert_the_opens_by_shape_counters() {
    let low = run(LOW_REQUEST_COST_BYTES).await;
    let high = run(HIGH_REQUEST_COST_BYTES).await;

    assert_eq!(
        (low.ranged_opens, low.whole_object_opens),
        (SEGMENTS as u64, 0),
        "a low request cost routes every segment ranged"
    );
    assert_eq!(
        (high.whole_object_opens, high.ranged_opens),
        (SEGMENTS as u64, 0),
        "a high request cost routes every segment whole-object"
    );

    // The knob selects a read path, never a result.
    assert_eq!(
        (low.rows, high.rows),
        (TOTAL_ROWS, TOTAL_ROWS),
        "both extremes return every row"
    );
    assert_eq!(
        low.pairs, high.pairs,
        "both extremes return identical rows, value for value"
    );
    assert_eq!(
        low.pairs.len(),
        TOTAL_ROWS,
        "the compared row set is the whole fixture, not an empty set compared to itself"
    );
}
