//! Issue #3237: does DataFusion's `FilterExec` batch coalescer pin whole-block
//! string values arrays the logs scan has already stopped charging to the SQL
//! memory pool?
//!
//! The scan builds a PLAIN declared `Str` page as one values `StringArray` for
//! the whole block and emits the block as a `DictionaryArray` over it.
//! `filter_dict` keeps that values array, and the coalescer behind
//! `FilterExec` keeps a slice of each filtered batch until it has 8,192 rows.
//! Neither holds a reservation, so a selective `LIKE` could hold every
//! block's values while the pool reads one block's charge.
//!
//! The fixture is one RLOG object of `N` blocks of [`BLOCK_ROWS`] rows. Every
//! `URL` value is [`L`] bytes and distinct across the whole object, so each
//! block stores its `URL` page PLAIN, which [`fixture`] reads back from the
//! object's PAGE_DIR. A fixed number of rows per block contain `needle`.
//!
//! The measurement plans the statement through `build_session` at
//! `target_partitions = 1`, finds the `FilterExec` node, executes that node
//! alone, and records what its output batches hold and what the pool reads
//! once the stream is exhausted, before the batches drop. Every figure is
//! printed before it is asserted, so a miss still yields the numbers.
//!
//! The bands are the ones pre-registered on #3237; a miss is a finding.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::encoding::Enc;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{kind, open as open_footer};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::reader::read_section;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, FieldType, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    DeclaredColumn, DeclaredType, LogsTableProvider, SessionPrincipal, SessionTable,
    SpillDecision, SqlConfig, build_session,
};
use ravel_types::logstream::log_stream_id;
use ravel_types::{TenantHash, TenantId};
use uuid::Uuid;

const KEY: &str = "URL";
const BLOCK_ROWS: usize = 8_192;
/// Every `URL` value's length in bytes.
const L: usize = 64;
const OBJECT_KEY: &str = "t/coalescer-pin.rlog";
/// `FILTER_EXEC_DEFAULT_BATCH_SIZE`: the coalescer flushes at this many rows.
const COALESCE_ROWS: usize = 8_192;

const SELECT_URL: &str = r#"SELECT "URL" FROM logs WHERE "URL" LIKE '%needle%'"#;
const COUNT_URL: &str = r#"SELECT COUNT(*) FROM logs WHERE "URL" LIKE '%needle%'"#;

fn tenant() -> TenantId {
    TenantId::new("filter-coalescer-pin-3237".to_string())
}

fn declared() -> Vec<DeclaredColumn> {
    vec![DeclaredColumn::new(KEY, DeclaredType::Str)]
}

/// One block's whole values array as the prediction counts it: `BLOCK_ROWS`
/// values of `L` bytes plus `BLOCK_ROWS + 1` i32 offsets.
const fn block_values_bytes() -> usize {
    BLOCK_ROWS * L + 4 * (BLOCK_ROWS + 1)
}

/// splitmix64.
fn mix(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// `L` hex characters, distinct per row; a matching row's first six are
/// `needle`, which no hex string contains.
fn url(row: usize, matching: bool) -> String {
    let r = row as u64;
    let hex = format!(
        "{:016x}{:016x}{:016x}{:016x}",
        mix(r),
        mix(!r),
        mix(r ^ 0x5555_5555),
        mix(r ^ 0xaaaa_aaaa)
    );
    let value = if matching {
        format!("needle{}", &hex[6..])
    } else {
        hex
    };
    debug_assert_eq!(value.len(), L);
    value
}

/// Whether row `r` of its block matches, for `per_block` matching rows spread
/// evenly over the block.
fn is_match(r: usize, per_block: usize) -> bool {
    r.is_multiple_of(BLOCK_ROWS / per_block)
}

fn build_object(blocks: usize, per_block: usize) -> Vec<u8> {
    let identity = ObjectIdentity {
        tenant_hash: tenant().hash().0,
        shard: 0,
        writer_id: [5u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let stream_id = log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    let config = RlogConfig {
        block_target_records: BLOCK_ROWS,
        ..RlogConfig::default()
    };
    let mut writer = RlogWriter::new(config, identity);
    for row in 0..blocks * BLOCK_ROWS {
        let ts = row as i64;
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "b".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![(
                    KEY.to_string(),
                    AttrValue::Str(url(row, is_match(row % BLOCK_ROWS, per_block))),
                )],
            })
            .expect("push");
    }
    writer.finish().expect("finish")
}

/// The encodings PAGE_DIR records for the `URL` column's pages, in order.
fn url_page_encs(obj: &[u8]) -> Vec<Enc> {
    let cfg = RlogConfig::default();
    let footer = open_footer(obj).expect("open footer");
    let desc = *footer.section(kind::FIELD_DIR).expect("a FIELD_DIR");
    let bytes = read_section(obj, &desc, &cfg).expect("read FIELD_DIR");
    let dir = FieldDir::decode(&bytes, u64::MAX).expect("decode FIELD_DIR");
    let column_id = dir
        .column(KEY, FieldType::Str)
        .expect("a Str column for URL")
        .column_id;
    let desc = *footer.section(kind::PAGE_DIR).expect("a PAGE_DIR");
    let bytes = read_section(obj, &desc, &cfg).expect("read PAGE_DIR");
    PageDir::decode(&bytes)
        .expect("decode PAGE_DIR")
        .groups
        .iter()
        .flat_map(|g| &g.chunks)
        .filter(|c| c.column_id == column_id)
        .flat_map(|c| c.pages.iter().map(|p| p.enc))
        .collect()
}

struct Fixture {
    blocks: usize,
    store: Arc<dyn ObjectStoreBackend>,
    segment: SegmentRef,
}

type Objects = Mutex<HashMap<(usize, usize), Arc<Vec<u8>>>>;

/// Each `(blocks, per_block)` object is written once per process.
fn object(blocks: usize, per_block: usize) -> Arc<Vec<u8>> {
    static OBJECTS: std::sync::OnceLock<Objects> = std::sync::OnceLock::new();
    let mut objects = OBJECTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    Arc::clone(
        objects
            .entry((blocks, per_block))
            .or_insert_with(|| Arc::new(build_object(blocks, per_block))),
    )
}

async fn fixture(blocks: usize, per_block: usize) -> Fixture {
    let obj = object(blocks, per_block);
    let encs = url_page_encs(&obj);
    println!(
        "[fixture N={blocks} matches/block={per_block}] object {} bytes, URL page encodings: {} pages, {:?}",
        obj.len(),
        encs.len(),
        encs.iter().fold(BTreeMap::<String, usize>::new(), |mut m, e| {
            *m.entry(format!("{e:?}")).or_default() += 1;
            m
        }),
    );
    assert_eq!(
        encs,
        vec![Enc::Plain; blocks],
        "every block stores its URL page PLAIN"
    );
    let rows = blocks * BLOCK_ROWS;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    store
        .put(
            OBJECT_KEY,
            bytes::Bytes::from(obj.as_ref().clone()),
            PutOptions::default(),
        )
        .await
        .expect("put");
    let segment = SegmentRef {
        data_object_key: OBJECT_KEY.to_string(),
        object_size: obj.len() as u64,
        min_event_ts_ns: 0,
        max_event_ts_ns: rows as i64 - 1,
        ingest_hour_bucket: 0,
        sample_count: rows as u64,
        series_count: 0,
        shard: 0,
        content_hash: [3u8; 32],
        writer_id: Uuid::from_u128(5),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    };
    Fixture {
        blocks,
        store,
        segment,
    }
}

fn session(fx: &Fixture, pool: Arc<dyn MemoryPool>) -> SessionContext {
    let snapshot = Snapshot {
        segments: vec![fx.segment.clone()],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let provider = LogsTableProvider::new(
        snapshot,
        tenant().hash(),
        LogSegmentFetcher::new(Arc::clone(&fx.store)),
        PhaseAccounting::new(),
    )
    .with_declared_columns(declared());
    let config = SqlConfig {
        engine: EngineConfig {
            sql_partition_count: Some(1),
            ..EngineConfig::default()
        },
        ..SqlConfig::default()
    };
    build_session(
        &config,
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
        &SessionPrincipal::without_capabilities(TenantHash(tenant().hash().0)),
    )
    .expect("session")
}

fn find_filter(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.is::<FilterExec>() {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find_filter)
}

/// The statement's physical plan, its text, and its `FilterExec` node.
async fn plan(
    fx: &Fixture,
    pool: &Arc<UnboundedMemoryPool>,
    sql: &str,
) -> (SessionContext, String, Arc<dyn ExecutionPlan>) {
    let ctx = session(fx, Arc::clone(pool) as Arc<dyn MemoryPool>);
    let physical = ctx
        .sql(sql)
        .await
        .expect("plans")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let text = displayable(physical.as_ref()).indent(true).to_string();
    let Some(filter) = find_filter(&physical) else {
        panic!("no FilterExec in the plan of {sql}:\n{text}");
    };
    (ctx, text, filter)
}

/// The largest pool reading after any batch of the `FilterExec`'s input,
/// executed alone over a fresh pool: the scan's charge for one block.
async fn block_charge(fx: &Fixture, sql: &str) -> usize {
    let pool = Arc::new(UnboundedMemoryPool::default());
    let (ctx, _, filter) = plan(fx, &pool, sql).await;
    let input = Arc::clone(filter.children()[0]);
    let mut stream = input.execute(0, ctx.task_ctx()).expect("execute input");
    let mut peak = 0;
    let mut batches = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("input batch");
        peak = peak.max(pool.reserved());
        batches += 1;
        drop(batch);
    }
    println!("  scan alone: {batches} batches, peak pool reading after a batch {peak}");
    peak
}

struct Measured {
    plan: String,
    /// `(rows, get_array_memory_size)` of each `FilterExec` output batch.
    batches: Vec<(usize, usize)>,
    emitted: usize,
    /// Distinct `URL` values arrays the output references, by data pointer,
    /// and their summed `get_array_memory_size`.
    distinct_values: usize,
    distinct_values_bytes: usize,
    /// Largest number of distinct values arrays one output batch references.
    max_values_per_batch: usize,
    /// The pool after the stream is exhausted, before the batches drop, and
    /// after the stream drops with the batches still held.
    reserved_exhausted: usize,
    reserved_stream_dropped: usize,
    /// The `URL` column rebuilt as a plain `Utf8` array, summed over batches.
    cast_utf8_bytes: usize,
    url_type: Option<DataType>,
    matched_rows: usize,
}

/// The data pointers and sizes of the values arrays a column references:
/// one for a dictionary column, none for any other type.
fn values_arrays(column: &dyn Array) -> Vec<(usize, usize)> {
    match column.data_type() {
        DataType::Dictionary(_, _) => {
            let values = column.as_any_dictionary().values();
            let data = values.to_data();
            let ptr = data
                .buffers()
                .last()
                .map_or(0, |b| b.as_ptr() as usize);
            vec![(ptr, values.get_array_memory_size())]
        }
        _ => Vec::new(),
    }
}

async fn measure(fx: &Fixture, sql: &str) -> Measured {
    let pool = Arc::new(UnboundedMemoryPool::default());
    let (ctx, plan_text, filter) = plan(fx, &pool, sql).await;
    assert_eq!(
        filter.output_partitioning().partition_count(),
        1,
        "target_partitions = 1 gives the FilterExec one partition"
    );
    let mut stream = filter.execute(0, ctx.task_ctx()).expect("execute filter");
    let mut held: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = stream.next().await {
        held.push(batch.expect("filter batch"));
    }
    let reserved_exhausted = pool.reserved();
    drop(stream);
    let reserved_stream_dropped = pool.reserved();

    let schema = filter.schema();
    let url_idx = schema.index_of(KEY).ok();
    let url_type = url_idx.map(|i| schema.field(i).data_type().clone());
    let mut distinct: BTreeMap<usize, usize> = BTreeMap::new();
    let mut max_values_per_batch = 0;
    let mut cast_utf8_bytes = 0;
    let mut batches = Vec::new();
    for batch in &held {
        batches.push((batch.num_rows(), batch.get_array_memory_size()));
        if let Some(i) = url_idx {
            let column = batch.column(i);
            let arrays = values_arrays(column.as_ref());
            max_values_per_batch = max_values_per_batch.max(arrays.len());
            distinct.extend(arrays);
            cast_utf8_bytes += cast(column, &DataType::Utf8)
                .expect("cast to Utf8")
                .get_array_memory_size();
        }
    }
    let matched_rows = batches.iter().map(|(rows, _)| rows).sum();
    let measured = Measured {
        plan: plan_text,
        emitted: batches.iter().map(|(_, bytes)| bytes).sum(),
        batches,
        distinct_values: distinct.len(),
        distinct_values_bytes: distinct.values().sum(),
        max_values_per_batch,
        reserved_exhausted,
        reserved_stream_dropped,
        cast_utf8_bytes,
        url_type,
        matched_rows,
    };
    drop(held);
    measured
}

fn report(label: &str, fx: &Fixture, m: &Measured) {
    println!(
        "[{label} N={}] url type {:?}, matched rows {}, FilterExec batches {} (rows, bytes) {:?}",
        fx.blocks,
        m.url_type,
        m.matched_rows,
        m.batches.len(),
        m.batches
    );
    println!(
        "  emitted get_array_memory_size {}, expected N x (8192 x L + 4 x 8193) = {}",
        m.emitted,
        fx.blocks * block_values_bytes()
    );
    println!(
        "  distinct values arrays referenced {} totalling {} bytes, at most {} per batch",
        m.distinct_values, m.distinct_values_bytes, m.max_values_per_batch
    );
    println!(
        "  pool reserved: exhausted before the drop {}, stream dropped with batches held {}",
        m.reserved_exhausted, m.reserved_stream_dropped
    );
    println!("  URL cast to Utf8, summed: {} bytes", m.cast_utf8_bytes);
    println!("  plan:\n{}", m.plan);
}

fn within_ten_percent(got: usize, want: usize) -> bool {
    got.abs_diff(want) * 10 <= want
}

/// Prediction 1: with one matching row per block, the `FilterExec` output
/// holds `N x (8192 x L + 4 x 8193)` bytes within 10% while the pool reads at
/// most one block's charge.
#[tokio::test]
async fn p1_one_match_per_block_pins_every_blocks_values_array() {
    let mut misses = Vec::new();
    for n in [8, 64, 256] {
        let fx = fixture(n, 1).await;
        let m = measure(&fx, SELECT_URL).await;
        report("p1 SELECT URL", &fx, &m);
        let charge = block_charge(&fx, SELECT_URL).await;
        println!("  one block's charge (scan alone, peak): {charge}");
        assert_eq!(m.matched_rows, n, "one matching row per block");
        let want = n * block_values_bytes();
        if !within_ten_percent(m.emitted, want) {
            misses.push(format!(
                "N={n}: emitted {} outside 10% of {want}",
                m.emitted
            ));
        }
        if m.reserved_exhausted > charge {
            misses.push(format!(
                "N={n}: pool {} above one block's charge {charge}",
                m.reserved_exhausted
            ));
        }
    }
    assert!(misses.is_empty(), "prediction 1 missed: {misses:#?}");
}

/// Prediction 2: `COUNT(*)` over the same filter holds about no string bytes
/// in the `FilterExec` output.
#[tokio::test]
async fn p2_count_star_filter_output_holds_no_string_bytes() {
    let mut emitted = Vec::new();
    for n in [8, 64, 256] {
        let fx = fixture(n, 1).await;
        let m = measure(&fx, COUNT_URL).await;
        report("p2 COUNT(*)", &fx, &m);
        assert_eq!(m.matched_rows, n, "one matching row per block");
        emitted.push((n, m.emitted, m.distinct_values_bytes));
    }
    // "About 0": under one block's offsets, which no values array fits in.
    let bound = 4 * (BLOCK_ROWS + 1);
    assert!(
        emitted
            .iter()
            .all(|(_, bytes, values)| *bytes < bound && *values == 0),
        "prediction 2 missed, (N, emitted, values bytes): {emitted:?}, bound {bound}"
    );
}

/// Prediction 3: with 64 matching rows per block the coalescer flushes every
/// 128 blocks, so no emitted batch holds more than 128 blocks' values arrays
/// and the largest batch stops growing with N.
#[tokio::test]
async fn p3_flushed_batches_hold_at_most_128_blocks() {
    let per_block = 64;
    let flush_blocks = COALESCE_ROWS / per_block;
    let bound = flush_blocks * block_values_bytes();
    let mut largest = Vec::new();
    for n in [64, 128, 256] {
        let fx = fixture(n, per_block).await;
        let m = measure(&fx, SELECT_URL).await;
        report("p3 SELECT URL, 64/block", &fx, &m);
        assert_eq!(m.matched_rows, n * per_block);
        let max = m.batches.iter().map(|(_, b)| *b).max().unwrap_or(0);
        println!("  largest emitted batch {max}, bound {flush_blocks} blocks = {bound} (+10%)");
        largest.push((n, max));
    }
    let max_of = |n: usize| largest.iter().find(|(k, _)| *k == n).map(|(_, b)| *b);
    let (b128, b256) = (max_of(128).unwrap(), max_of(256).unwrap());
    assert!(
        largest
            .iter()
            .all(|(_, b)| *b <= bound + bound / 10),
        "prediction 3 missed: a batch above {flush_blocks} blocks' values: {largest:?}"
    );
    assert!(
        within_ten_percent(b256, b128),
        "prediction 3 missed: the largest batch grew from N=128 to N=256: {largest:?}"
    );
}

/// Report only: the size the filtered `URL` column holds rebuilt as a plain
/// `Utf8` array of the surviving values, at N = 256, one match per block.
#[tokio::test]
async fn p4_report_utf8_rebuild_size() {
    let fx = fixture(256, 1).await;
    let m = measure(&fx, SELECT_URL).await;
    report("p4 SELECT URL rebuilt as Utf8", &fx, &m);
    println!(
        "  p4: dictionary output {} bytes, Utf8 rebuild {} bytes",
        m.emitted, m.cast_utf8_bytes
    );
}
