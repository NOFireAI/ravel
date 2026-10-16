//! ADR-2773 decision D3's pool charge for a declared `Str` column read from a
//! row group dictionary: every chunk of every block in the group emits a
//! `DictionaryArray` over one shared values array, and the scan charges that
//! array once while its cache entry is live, plus each chunk's own keys. A
//! per-batch `get_array_memory_size` would count the whole values array and
//! the whole block's key buffer once per chunk.
//!
//! The fixture is one object of [`BLOCKS`] blocks of [`BLOCK_ROWS`] rows in
//! one row group, so each block emits two [`CHUNK_ROWS`]-row batches. Every
//! block reads [`COMMON`] values shared by the whole group and [`UNIQUE`]
//! values of its own, so the group dictionary holds `COMMON + BLOCKS * UNIQUE`
//! entries and a block's narrowed dictionary `COMMON + UNIQUE`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, DictionaryArray, Int32Array, RecordBatch, StringArray, StringBuilder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, ColumnSelection, LogRecord, Predicate, RlogConfig, RlogReader, RlogWriter,
    stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, FIRST_DECLARED_COL, LogsScanExec, SpanSegmentFetcher, SqlConfig,
    SqlError, SqlExecutor, SqlRequest, StaticDeclaredColumns, logs_schema_with_declared,
};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

const KEY: &str = "region";
const BLOCKS: usize = 3;
const BLOCK_ROWS: usize = 16_384;
/// The scan's output batch size: each block emits two batches.
const CHUNK_ROWS: usize = 8_192;
const ROWS: usize = BLOCKS * BLOCK_ROWS;
const COMMON: usize = 2_048;
const UNIQUE: usize = 1_024;
const OBJECT_KEY: &str = "t/strdict0.rlog";

fn tenant() -> TenantId {
    TenantId::new("str-dict-charge-2787".to_string())
}

fn declared() -> Vec<DeclaredColumn> {
    vec![DeclaredColumn::new(KEY, DeclaredType::Str)]
}

/// splitmix64: values that zstd cannot shrink, so the writer's stored-size
/// comparison picks the row group dictionary over per-block pages.
fn mix(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// 24 bytes each, so every dictionary's string bytes are 24 times its length.
fn text(tag: char, seed: u64) -> String {
    format!("{tag}{:016x}{:07x}", mix(seed), mix(!seed) & 0x0fff_ffff)
}

fn common(i: usize) -> String {
    text('c', i as u64)
}

fn unique(block: usize, i: usize) -> String {
    text('u', ((block as u64) << 32) | i as u64)
}

/// Even rows read the common values, each four times per block; odd rows the
/// block's own, each eight times. Both chunks of a block read every value of
/// the block.
fn value_at(row: usize) -> String {
    let (block, r) = (row / BLOCK_ROWS, row % BLOCK_ROWS);
    if r.is_multiple_of(2) {
        common((r / 2) % COMMON)
    } else {
        unique(block, (r / 2) % UNIQUE)
    }
}

fn config() -> RlogConfig {
    RlogConfig {
        block_target_records: BLOCK_ROWS,
        group_target_blocks: BLOCKS,
        ..RlogConfig::default()
    }
}

fn object(tenant_hash: [u8; 16]) -> Vec<u8> {
    let identity = ObjectIdentity {
        tenant_hash,
        shard: 0,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let mut writer = RlogWriter::new(config(), identity);
    for row in 0..ROWS {
        let ts = row as i64;
        writer
            .push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "b".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![(KEY.to_string(), AttrValue::Str(value_at(row)))],
            })
            .expect("push");
    }
    writer.finish().expect("finish")
}

/// Each block's decoded footprint under the scan's column selection, decoded
/// straight off the object rather than read off the pool.
fn decoded_bytes(obj: &[u8]) -> Vec<usize> {
    let reader = RlogReader::new(obj, &RlogConfig::default()).expect("reader");
    let mut scan = reader
        .scan_blocks(
            &Predicate::TsRange {
                min_ns: i64::MIN,
                max_ns: i64::MAX,
            },
            &[],
            &ColumnSelection::fixed_only().with_attr(KEY),
        )
        .expect("scan");
    let mut sizes = Vec::new();
    while let Some(view) = scan.next_block_columnar(obj).expect("decode") {
        sizes.push(view.decoded_bytes());
    }
    sizes
}

fn task_ctx(pool: &Arc<UnboundedMemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool) as Arc<dyn MemoryPool>)
        .build_arc()
        .expect("runtime env");
    Arc::new(TaskContext::default().with_runtime(runtime))
}

/// What main charged a chunk: a fresh `StringArray` of the block's narrowed
/// dictionary (`COMMON + UNIQUE` entries) and nullable keys for the chunk.
fn main_chunk_charge() -> usize {
    let mut values = StringBuilder::new();
    for i in 0..COMMON {
        values.append_value(common(i));
    }
    for i in 0..UNIQUE {
        values.append_value(unique(0, i));
    }
    let keys: Vec<Option<i32>> = vec![Some(0); CHUNK_ROWS];
    DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(values.finish()))
        .expect("dictionary")
        .get_array_memory_size()
}

/// Per block, after the block's first batch is handed out, the pool carries
/// the block's decoded bytes, its two batches, and the group's values. Less
/// the decoded bytes, that is the group's values once plus two chunks' keys:
/// 217,560 bytes, under main's 361,008 (each of two chunks charged a fresh
/// narrowed dictionary) and under the 434,832 a per-batch
/// `get_array_memory_size` charges (the whole values array and the whole
/// block's key buffer, twice).
///
/// The three figures, from `get_array_memory_size` (buffer capacities plus the
/// array structs):
///
/// - this scan, `V + 2K` = 151,736 + 2 x 32,912. `V` is the group values
///   array: 5,120 entries of 24 bytes, 122,880 string bytes in a buffer grown
///   to 131,072, then 5,121 offsets of 4 bytes, 20,484 bytes rounded up to
///   20,544, then the 120-byte `StringArray`. `K` is one chunk's keys: 8,192
///   of 4 bytes, 32,768, plus the 144-byte `DictionaryArray`.
/// - main, `2 (M + K)` = 2 x (147,592 + 32,912). `M` is a chunk's narrowed
///   dictionary from a fresh `StringBuilder`: 3,072 entries, 73,728 string
///   bytes in a buffer grown to 131,072, 3,073 offsets, 12,292 bytes in a
///   buffer grown to 16,400, plus 120.
/// - per batch, `2 (V + B)` = 2 x (151,736 + 65,680). `B` is the block's whole
///   key buffer, 16,384 keys, 65,536 bytes, plus 144, which a chunk's slice
///   still reports.
///
/// Once the stream is exhausted and before it drops, the pool holds only the
/// last batch's keys, `K`: finishing the last segment released the values'
/// charge, so the drop releases only `K`.
///
/// Flipped: charging `get_array_memory_size` per batch in `hold_batches`, and
/// slicing the block's keys rather than copying a chunk's, reads 434,832.
#[tokio::test]
async fn a_row_group_dictionary_is_charged_once_per_block_not_per_batch() {
    let t = tenant();
    let obj = object(t.hash().0);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    store
        .put(
            OBJECT_KEY,
            bytes::Bytes::from(obj.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put");
    let segment = SegmentRef {
        data_object_key: OBJECT_KEY.to_string(),
        object_size: obj.len() as u64,
        min_event_ts_ns: 0,
        max_event_ts_ns: ROWS as i64 - 1,
        ingest_hour_bucket: 0,
        sample_count: ROWS as u64,
        series_count: 0,
        shard: 0,
        content_hash: [3u8; 32],
        writer_id: Uuid::from_u128(4),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    };
    let decoded = decoded_bytes(&obj);
    assert_eq!(decoded.len(), BLOCKS, "the fixture's block count");

    let exec = LogsScanExec::new(
        t.hash(),
        LogSegmentFetcher::new(Arc::clone(&store)),
        &[segment],
        1,
        0,
        ROWS as i64,
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Some(&vec![FIRST_DECLARED_COL]),
        PhaseAccounting::new(),
        logs_schema_with_declared(&declared()),
        Arc::new(declared()),
    )
    .expect("scan");
    let pool = Arc::new(UnboundedMemoryPool::default());
    let mut stream = exec.execute(0, task_ctx(&pool)).expect("execute");

    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut charged = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        if batches.len().is_multiple_of(2) {
            let block = batches.len() / 2;
            charged.push(pool.reserved() - decoded[block]);
        }
        batches.push(batch);
    }
    let exhausted = pool.reserved();
    assert_eq!(batches.len(), 2 * BLOCKS, "two chunks per block");
    let values: &ArrayRef = batches[0].column(0).as_any_dictionary().values();
    assert_eq!(
        values.len(),
        COMMON + BLOCKS * UNIQUE,
        "the group dictionary"
    );
    for batch in &batches {
        assert_eq!(batch.num_rows(), CHUNK_ROWS);
        let dict = batch.column(0).as_any_dictionary();
        assert!(
            Arc::ptr_eq(dict.values(), values),
            "every chunk of every block shares the one values array"
        );
    }

    let values_bytes = values.get_array_memory_size();
    let keys_bytes = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0i32; CHUNK_ROWS]),
        Arc::clone(values),
    )
    .expect("dictionary")
    .get_array_memory_size()
        - values_bytes;
    let after = values_bytes + 2 * keys_bytes;
    let main = 2 * main_chunk_charge();
    let pre_fix = 2 * DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0i32; BLOCK_ROWS]).slice(0, CHUNK_ROWS),
        Arc::clone(values),
    )
    .expect("dictionary")
    .get_array_memory_size();
    assert_eq!(
        (after, main, pre_fix),
        (217_560, 361_008, 434_832),
        "the fixture's charges: this scan's, main's, per-batch"
    );
    let strings = values.as_string::<i32>();
    let (string_buffer, offset_buffer) = (
        strings.values().capacity(),
        strings.offsets().inner().inner().capacity(),
    );
    assert_eq!(
        (string_buffer, offset_buffer),
        (131_072, 20_544),
        "V's string and offset buffer capacities"
    );
    assert_eq!(
        values_bytes,
        string_buffer + offset_buffer + std::mem::size_of::<StringArray>(),
        "V"
    );
    assert_eq!(
        keys_bytes,
        CHUNK_ROWS * std::mem::size_of::<i32>() + std::mem::size_of::<DictionaryArray<Int32Type>>(),
        "K"
    );
    assert!(after <= main, "no more than main's narrowed dictionaries");
    assert_eq!(charged, vec![after; BLOCKS], "each block's charge");
    assert_eq!(
        exhausted, keys_bytes,
        "exhausted, before the drop: the last batch's keys and no values"
    );

    drop(stream);
    assert_eq!(pool.reserved(), 0, "the dropped stream released everything");
}

async fn publish_object(store: &dyn ObjectStoreBackend, t: &TenantId) {
    let bytes = object(t.hash().0);
    let new_record = NewCommitRecord {
        tenant_hash: t.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(9_787),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: ROWS as u64,
        series_count: 1,
        min_event_ts_ns: 0,
        max_event_ts_ns: ROWS as i64 - 1,
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: ROWS as i64 - 1,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

fn executor(store: &Arc<dyn ObjectStoreBackend>, max_query_bytes: usize) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig {
            max_query_bytes,
            ..SqlConfig::default()
        },
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())))
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: ROWS as i64,
        },
        min_tokens: Vec::new(),
        now_ns: ROWS as i64,
        deadline: Duration::from_secs(60),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// The `region` column of every output batch as text, sorted.
fn region_texts(batches: &[RecordBatch], column: usize) -> Vec<Option<String>> {
    let mut texts: Vec<Option<String>> = batches
        .iter()
        .flat_map(|batch| {
            let utf8 = cast(batch.column(column), &DataType::Utf8).expect("cast to Utf8");
            let strings = utf8
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8")
                .clone();
            (0..strings.len())
                .map(|i| strings.is_valid(i).then(|| strings.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect();
    texts.sort();
    texts
}

/// Under a `--sql-max-query-bytes` of [`CEILING`], `SELECT region FROM logs`
/// over the fixture completes on the columnar path and returns the rows the
/// row path returns (`SELECT region, attrs FROM logs`, which projects the
/// `attrs` map and so drains the row path), every record's own value. Its
/// peak is 668,172 bytes.
///
/// Flipped: with the per-batch charge (`get_array_memory_size` summed in
/// `hold_batches`, block keys sliced rather than copied) the same statement
/// peaks at 1,069,948 bytes unbounded and is refused with `ResourcesExhausted`
/// under the ceiling.
const CEILING: usize = 850_000;

#[tokio::test]
async fn a_statement_the_per_batch_charge_refused_fits_the_ceiling() {
    let t = tenant();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_object(store.as_ref(), &t).await;

    let columnar = executor(&store, CEILING)
        .execute(t.hash(), &request("SELECT region FROM logs"))
        .await;
    let columnar = match columnar {
        Ok(outcome) => outcome,
        Err(SqlError::ResourcesExhausted(e)) => {
            panic!("refused under the {CEILING}-byte ceiling: {e}")
        }
        Err(e) => panic!("query failed: {e:?}"),
    };
    let peak = columnar.accounting.peak_intermediate_bytes;
    assert_eq!(peak, 668_172, "the statement's peak under the fixed charge");

    let rows = executor(&store, SqlConfig::default().max_query_bytes)
        .execute(t.hash(), &request("SELECT region, attrs FROM logs"))
        .await
        .expect("row path query");
    let want: Vec<Option<String>> = {
        let mut v: Vec<Option<String>> = (0..ROWS).map(|r| Some(value_at(r))).collect();
        v.sort();
        v
    };
    let row_path = region_texts(rows.output.batches(), 0);
    assert_eq!(row_path, want, "the row path reads every record's value");
    assert_eq!(
        region_texts(columnar.output.batches(), 0),
        row_path,
        "the columnar statement returns the row path's rows"
    );
}
