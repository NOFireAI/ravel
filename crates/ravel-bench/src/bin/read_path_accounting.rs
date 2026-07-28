//! Read-path GET accounting harness (GitHub issue #97 phase b).
//!
//! Measures what a single-series lookup, a ~1% label-matcher selection, and a
//! full scan each cost, in GET requests and bytes transferred, for three
//! stored formats over object storage:
//!
//!   * RSEG v1 (`SegmentWriter::write`)
//!   * RSEG v2 (`SegmentWriter::write_v2`, ADR-0014)
//!   * Parquet (the phase-a write-side layout: series_id/ts/value + one
//!     dictionary column per label, zstd(3), page + offset index)
//!
//! over the two workload shapes the phase-a write baseline uses: a
//! 100k-series `many_small` object and a 500-series ingest-shape object.
//!
//! Every GET is routed through a counting wrapper
//! (`ravel_bench::read_accounting`): [`CountingBackend`] for the RSEG path
//! (ravel-object-store's trait) and [`CountingObjectStore`] for the async
//! Parquet reader (the `object_store` crate's trait). GET count and bytes are
//! backend-independent, so they are the primary deliverable whether this runs
//! against MinIO or the in-process reference stores.
//!
//! Backend selection: if the `RAVEL_S3_*` env vars BENCHMARKS.md documents
//! are set and the endpoint is reachable, both paths run against it (MinIO);
//! otherwise both fall back to the in-process stores (MemoryStore for RSEG,
//! `object_store::memory::InMemory` for Parquet) and wall times are labeled
//! in-process. The chosen backend is printed in the header.
//!
//! Report-only: never changes library behavior. Gated on the
//! `parquet-baseline` feature so the default build never compiles
//! arrow/parquet/object_store.
//!
//! Run: `cargo run -p ravel-bench --release --features parquet-baseline \
//!   --bin read_path_accounting`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    Float64Array, Int64Array, RecordBatch, StringArray, StringDictionaryBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use bytes::Bytes;
use futures::TryStreamExt;

use object_store::memory::InMemory;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ArrowReaderOptions, RowFilter};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::read_accounting::{CountingBackend, CountingObjectStore};
use ravel_bench::segment_support::{build_segment, build_segment_v2};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_segment::{
    ReaderLimits, decode_catalog, decode_catalog_matching, decode_catalog_matching_v2,
    decode_catalog_v2, plan_ranges,
};
use ravel_types::{LabelSet, Sample, SeriesId};

const ROW_GROUP_ROWS: usize = 100_000;
/// RSEG reader-protocol suffix probe (docs/segment-format.md step 2).
const SUFFIX_PROBE: u64 = 64 * 1024;
/// Label used for the ~1% matcher pattern (b): the lowest-cardinality label
/// the workloads carry. Its selectivity depends on the shape's cardinality
/// profile and is reported per cell.
const MATCH_LABEL: &str = "label_000";
const MATCH_VALUE: &str = "v0_0";

type RawSeries = (SeriesId, LabelSet, Vec<Sample>);

#[tokio::main]
async fn main() {
    let backend = detect_backend().await;
    print_header(&backend);

    let many_small = WorkloadConfig {
        series_count: 100_000,
        samples_per_series: 2,
        cardinality: CardinalityProfile::many_small(100_000),
        ..Default::default()
    };
    run_shape(
        "100k-series many_small, 200k samples (2/series)",
        &many_small,
        &backend,
    )
    .await;

    let ingest = WorkloadConfig {
        series_count: 500,
        samples_per_series: 320,
        ..Default::default()
    };
    run_shape(
        "500-series ingest shape, 160k samples (320/series)",
        &ingest,
        &backend,
    )
    .await;
}

// ---------------------------------------------------------- backend choice

enum Backend {
    InProcess,
    // A live MinIO/S3 backend was reachable. (Not expected on the fleet
    // executor; kept so the same harness runs on target hardware.)
    Minio(ravel_object_store::s3::S3Config),
}

async fn detect_backend() -> Backend {
    let Some(config) = s3_config_from_env() else {
        return Backend::InProcess;
    };
    // Probe: a cheap list against the bucket. If it errors (unreachable,
    // bad creds, missing bucket), fall back to in-process.
    match ravel_object_store::s3::S3Store::new(config.clone()) {
        Ok(store) => match store.list_delimited("").await {
            Ok(_) => Backend::Minio(config),
            Err(e) => {
                eprintln!("note: S3 endpoint configured but unreachable ({e}); using in-process");
                Backend::InProcess
            }
        },
        Err(e) => {
            eprintln!("note: S3 config present but client build failed ({e}); using in-process");
            Backend::InProcess
        }
    }
}

fn s3_config_from_env() -> Option<ravel_object_store::s3::S3Config> {
    let bucket = std::env::var("RAVEL_S3_BUCKET").ok()?;
    let region = std::env::var("RAVEL_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let access_key_id = std::env::var("RAVEL_S3_ACCESS_KEY_ID").ok()?;
    let secret_access_key = std::env::var("RAVEL_S3_SECRET_ACCESS_KEY").ok()?;
    let endpoint = std::env::var("RAVEL_S3_ENDPOINT").ok();
    let allow_http = std::env::var("RAVEL_S3_ALLOW_HTTP")
        .map(|v| v == "true")
        .unwrap_or(false);
    // Contract default: path style unless explicitly disabled.
    let force_path_style = std::env::var("RAVEL_S3_FORCE_PATH_STYLE")
        .map(|v| v != "false")
        .unwrap_or(true);
    Some(ravel_object_store::s3::S3Config {
        bucket,
        region,
        endpoint,
        access_key_id,
        secret_access_key,
        allow_http,
        force_path_style,
    })
}

fn backend_label(backend: &Backend) -> &'static str {
    match backend {
        Backend::InProcess => "in-process (MemoryStore / object_store InMemory)",
        Backend::Minio(_) => "MinIO / S3",
    }
}

fn print_header(backend: &Backend) {
    println!("== read-path GET accounting (issue #97 phase b) ==");
    println!("  backend            : {}", backend_label(backend));
    println!("  suffix probe       : {SUFFIX_PROBE} bytes");
    println!(
        "  wall times         : {}",
        match backend {
            Backend::InProcess => "in-process (not network-representative)",
            Backend::Minio(_) => "against live MinIO/S3",
        }
    );
    println!();
}

// ------------------------------------------------------------- per shape

struct Measured {
    gets: u64,
    bytes: u64,
    heads: u64,
    wall_ms: f64,
    /// Rows/series the pattern actually returned (sanity + selectivity).
    selected: usize,
}

async fn run_shape(label: &str, config: &WorkloadConfig, backend: &Backend) {
    println!("-- shape: {label} --");
    let raw = generate_raw(config).expect("generate workload");
    let series_count = raw.len();
    let total_rows: usize = raw.iter().map(|(_, _, s)| s.len()).sum();

    // Single-series target: a series in the middle, addressed by its unique
    // series_idx label (RSEG) and its 16-byte series_id (Parquet).
    let target_idx = series_count / 2;
    let target = &raw[target_idx];
    let target_series_idx = target
        .1
        .get("series_idx")
        .expect("series_idx label present")
        .to_string();
    let target_id: [u8; 16] = target.0.0;

    let match_count = raw
        .iter()
        .filter(|(_, labels, _)| labels.get(MATCH_LABEL) == Some(MATCH_VALUE))
        .count();
    let match_pct = 100.0 * match_count as f64 / series_count as f64;
    println!(
        "  series: {series_count}  rows: {total_rows}  \
         matcher {MATCH_LABEL}={MATCH_VALUE} selects {match_count} ({match_pct:.3}%)"
    );

    // Build all three stored objects once.
    let v1 = build_segment(raw.clone()).bytes;
    let v2 = build_segment_v2(raw.clone()).bytes;
    let label_names = collect_label_names(&raw);
    let parquet = build_parquet_object(&raw, &label_names);
    println!(
        "  object bytes       : rseg-v1 {}  rseg-v2 {}  parquet {}",
        v1.len(),
        v2.len(),
        parquet.len()
    );

    // Run the three formats. Each returns its 3 patterns (a, b, c).
    let rseg1 = measure_rseg(backend, "rseg-v1", &v1, 1, &target_series_idx, target_id).await;
    let rseg2 = measure_rseg(backend, "rseg-v2", &v2, 2, &target_series_idx, target_id).await;
    let pq = measure_parquet(backend, &parquet, target_id, &label_names).await;

    print_table(&rseg1, &rseg2, &pq);
    println!();
}

fn print_table(rseg1: &[Measured; 3], rseg2: &[Measured; 3], pq: &[Measured; 3]) {
    let patterns = [
        "a: single-series lookup",
        "b: 1% label matcher",
        "c: full scan",
    ];
    println!(
        "  {:<26} {:<9} {:>6} {:>13} {:>11} {:>8}",
        "pattern", "format", "GETs", "bytes", "wall ms", "sel"
    );
    for (p, name) in patterns.iter().enumerate() {
        print_measured(name, "rseg-v1", &rseg1[p]);
        print_measured("", "rseg-v2", &rseg2[p]);
        print_measured("", "parquet", &pq[p]);
    }
}

fn print_measured(pattern: &str, format: &str, m: &Measured) {
    let heads = if m.heads > 0 {
        format!(" +{}h", m.heads)
    } else {
        String::new()
    };
    println!(
        "  {:<26} {:<9} {:>6}{:<4} {:>13} {:>11.3} {:>8}",
        pattern, format, m.gets, heads, m.bytes, m.wall_ms, m.selected
    );
}

// ------------------------------------------------------------ RSEG paths

/// Puts `bytes` under `key` into a fresh counting RSEG backend and returns
/// (store, key, total_size). The store's counters start at zero.
async fn rseg_store(
    backend: &Backend,
    bytes: &[u8],
    key: &str,
) -> (Arc<dyn ObjectStoreBackend>, CountingHandle, u64) {
    let counting: Arc<dyn ObjectStoreBackend>;
    let handle: CountingHandle;
    match backend {
        Backend::InProcess => {
            let store = CountingBackend::new(MemoryStore::new());
            handle = CountingHandle::Backend(store.counters());
            let store = Arc::new(store);
            store
                .put(key, Bytes::copy_from_slice(bytes), PutOptions::default())
                .await
                .expect("put rseg object");
            store.reset();
            counting = store;
        }
        Backend::Minio(config) => {
            let inner =
                ravel_object_store::s3::S3Store::new(config.clone()).expect("build s3 for rseg");
            let store = CountingBackend::new(inner);
            handle = CountingHandle::Backend(store.counters());
            let store = Arc::new(store);
            store
                .put(key, Bytes::copy_from_slice(bytes), PutOptions::default())
                .await
                .expect("put rseg object");
            store.reset();
            counting = store;
        }
    }
    (counting, handle, bytes.len() as u64)
}

enum CountingHandle {
    Backend(Arc<ravel_bench::read_accounting::Counters>),
}

impl CountingHandle {
    fn snapshot(&self) -> (u64, u64, u64) {
        match self {
            CountingHandle::Backend(c) => c.snapshot(),
        }
    }
    fn reset(&self) {
        match self {
            CountingHandle::Backend(c) => c.reset(),
        }
    }
}

async fn measure_rseg(
    backend: &Backend,
    key: &str,
    bytes: &[u8],
    version: u16,
    target_series_idx: &str,
    target_id: [u8; 16],
) -> [Measured; 3] {
    let (store, counters, total_size) = rseg_store(backend, bytes, key).await;
    let limits = ReaderLimits::default();

    // (a) single-series lookup.
    counters.reset();
    let start = Instant::now();
    let sel_a = rseg_series_lookup(
        store.as_ref(),
        key,
        total_size,
        version,
        &[("series_idx", target_series_idx)],
        limits,
    )
    .await;
    let a = finish(&counters, start, sel_a);

    // (b) 1% label matcher.
    counters.reset();
    let start = Instant::now();
    let sel_b = rseg_series_lookup(
        store.as_ref(),
        key,
        total_size,
        version,
        &[(MATCH_LABEL, MATCH_VALUE)],
        limits,
    )
    .await;
    let b = finish(&counters, start, sel_b);

    // (c) full scan: one whole-object GET, decode everything.
    counters.reset();
    let start = Instant::now();
    let sel_c = rseg_full_scan(store.as_ref(), key, version, limits).await;
    let c = finish(&counters, start, sel_c);

    // Suppress unused-warning on target_id for the RSEG path (Parquet uses it).
    let _ = target_id;
    [a, b, c]
}

/// RSEG single-series / matcher lookup: suffix GET, catalog-section GETs,
/// then two page GETs per selected series (docs/segment-format.md reader
/// protocol + the fetcher/planner: `select` + `plan_ranges`). Returns the
/// number of selected series.
async fn rseg_series_lookup(
    store: &dyn ObjectStoreBackend,
    key: &str,
    total_size: u64,
    version: u16,
    equals: &[(&str, &str)],
    limits: ReaderLimits,
) -> usize {
    let footer = fetch_footer(store, key, total_size, limits).await;

    // Catalog sections: v1 = LABEL_DICT + SERIES_TABLE; v2 = LABEL_DICT +
    // SERIES_IDS + SERIES_META. Fetch each by its footer offset/len.
    let label_dict = get_section(store, key, &footer, section_kind::LABEL_DICT).await;
    let entries = if version == 2 {
        let series_ids = get_section(store, key, &footer, section_kind::SERIES_IDS).await;
        let series_meta = get_section(store, key, &footer, section_kind::SERIES_META).await;
        decode_catalog_matching_v2(
            &footer,
            &label_dict,
            &series_ids,
            &series_meta,
            equals,
            limits,
        )
        .expect("decode matching catalog v2")
    } else {
        let series_table = get_section(store, key, &footer, section_kind::SERIES_TABLE).await;
        decode_catalog_matching(&footer, &label_dict, &series_table, equals, limits)
            .expect("decode matching catalog v1")
    };

    // Plan the selected series' TS/VAL page ranges and GET each page.
    let selected: Vec<&_> = entries.iter().collect();
    let planned = plan_ranges(&footer, &selected).expect("plan ranges");
    for range in &planned {
        let (ts_off, ts_len) = range.ts_range;
        let (val_off, val_len) = range.val_range;
        get_range(store, key, ts_off, ts_len).await;
        get_range(store, key, val_off, val_len).await;
    }
    selected.len()
}

/// RSEG full scan: one whole-object GET, then decode the entire catalog to
/// confirm the object is fully readable. Returns the series count.
async fn rseg_full_scan(
    store: &dyn ObjectStoreBackend,
    key: &str,
    version: u16,
    limits: ReaderLimits,
) -> usize {
    let full = store.get(key, GetRange::Full).await.expect("full get");
    let bytes = &full.data;
    let loc = ravel_segment::open_from_full(bytes, limits).expect("open full");
    let footer = &loc.footer;
    let label_dict = section_slice(bytes, footer, section_kind::LABEL_DICT);
    let entries = if version == 2 {
        let series_ids = section_slice(bytes, footer, section_kind::SERIES_IDS);
        let series_meta = section_slice(bytes, footer, section_kind::SERIES_META);
        decode_catalog_v2(footer, label_dict, series_ids, series_meta, limits)
            .expect("decode catalog v2")
    } else {
        let series_table = section_slice(bytes, footer, section_kind::SERIES_TABLE);
        decode_catalog(footer, label_dict, series_table, limits).expect("decode catalog v1")
    };
    // Touch page ranges so a full scan really plans over every series.
    let selected: Vec<&_> = entries.iter().collect();
    let planned = plan_ranges(footer, &selected).expect("plan ranges");
    planned.len()
}

/// Reader-protocol footer fetch: suffix GET, and one more ranged GET if the
/// suffix did not cover the footer.
async fn fetch_footer(
    store: &dyn ObjectStoreBackend,
    key: &str,
    total_size: u64,
    limits: ReaderLimits,
) -> ravel_proto::segment::v1::Footer {
    let probe = SUFFIX_PROBE.min(total_size);
    let suffix = store
        .get(key, GetRange::Suffix(probe))
        .await
        .expect("suffix get");
    match ravel_segment::open_from_suffix(&suffix.data, total_size, limits).expect("parse footer") {
        ravel_segment::FooterOutcome::Ready(loc) => loc.footer,
        ravel_segment::FooterOutcome::NeedRange { offset, len } => {
            let more = get_range(store, key, offset, len).await;
            match ravel_segment::open_from_suffix(&more, total_size, limits)
                .expect("parse footer 2")
            {
                ravel_segment::FooterOutcome::Ready(loc) => loc.footer,
                ravel_segment::FooterOutcome::NeedRange { .. } => {
                    panic!("footer still not covered after ranged refetch")
                }
            }
        }
    }
}

async fn get_section(
    store: &dyn ObjectStoreBackend,
    key: &str,
    footer: &ravel_proto::segment::v1::Footer,
    kind: u32,
) -> Vec<u8> {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    get_range(store, key, section.offset, section.len).await
}

async fn get_range(store: &dyn ObjectStoreBackend, key: &str, offset: u64, len: u64) -> Vec<u8> {
    let end = offset + len;
    let outcome = store
        .get(key, GetRange::Range(offset, end))
        .await
        .expect("ranged get");
    outcome.data.to_vec()
}

fn section_slice<'a>(
    bytes: &'a [u8],
    footer: &ravel_proto::segment::v1::Footer,
    kind: u32,
) -> &'a [u8] {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    let start = section.offset as usize;
    &bytes[start..start + section.len as usize]
}

fn finish(counters: &CountingHandle, start: Instant, selected: usize) -> Measured {
    let wall_ms = start.elapsed().as_secs_f64() * 1e3;
    let (gets, bytes, heads) = counters.snapshot();
    Measured {
        gets,
        bytes,
        heads,
        wall_ms,
        selected,
    }
}

/// v1/v2 section kind numbers (docs/segment-format.md; not re-exported by
/// ravel_segment).
mod section_kind {
    pub const LABEL_DICT: u32 = 1;
    pub const SERIES_TABLE: u32 = 2;
    pub const SERIES_IDS: u32 = 5;
    pub const SERIES_META: u32 = 6;
}

// --------------------------------------------------------- Parquet paths

async fn measure_parquet(
    backend: &Backend,
    parquet: &[u8],
    target_id: [u8; 16],
    label_names: &[String],
) -> [Measured; 3] {
    let key = "parquet-obj";
    let (store, counters) = parquet_store(backend, parquet, key).await;
    let path = OsPath::from(key);
    let label_col = 3; // series_id, ts, value, then labels in order.
    let _ = label_names;

    // (a) single-series lookup: row filter on series_id, project ts+value.
    counters.reset();
    let start = Instant::now();
    let sel_a = parquet_scan(
        Arc::clone(&store),
        &path,
        Some(Filter::SeriesId(target_id)),
        &[1, 2],
    )
    .await;
    let a = finish_pq(&counters, start, sel_a);

    // (b) 1% label matcher: predicate pushdown on the label column.
    counters.reset();
    let start = Instant::now();
    let sel_b = parquet_scan(
        Arc::clone(&store),
        &path,
        Some(Filter::Label {
            col: label_col,
            value: MATCH_VALUE.to_string(),
        }),
        &[1, 2],
    )
    .await;
    let b = finish_pq(&counters, start, sel_b);

    // (c) full scan: no filter, all columns.
    counters.reset();
    let start = Instant::now();
    let sel_c = parquet_scan(Arc::clone(&store), &path, None, &[]).await;
    let c = finish_pq(&counters, start, sel_c);

    [a, b, c]
}

fn finish_pq(
    counters: &Arc<ravel_bench::read_accounting::Counters>,
    start: Instant,
    sel: usize,
) -> Measured {
    let wall_ms = start.elapsed().as_secs_f64() * 1e3;
    let (gets, bytes, heads) = counters.snapshot();
    Measured {
        gets,
        bytes,
        heads,
        wall_ms,
        selected: sel,
    }
}

async fn parquet_store(
    backend: &Backend,
    parquet: &[u8],
    key: &str,
) -> (
    Arc<CountingObjectStore>,
    Arc<ravel_bench::read_accounting::Counters>,
) {
    let inner: Arc<dyn ObjectStore> = match backend {
        Backend::InProcess => Arc::new(InMemory::new()),
        Backend::Minio(config) => {
            // A second S3 client, this time through the object_store crate
            // directly, pointed at the same bucket/endpoint.
            Arc::new(build_os_s3(config))
        }
    };
    inner
        .put(&OsPath::from(key), PutPayload::from(parquet.to_vec()))
        .await
        .expect("put parquet object");
    let counting = Arc::new(CountingObjectStore::new(inner));
    let counters = counting.counters();
    (counting, counters)
}

fn build_os_s3(config: &ravel_object_store::s3::S3Config) -> object_store::aws::AmazonS3 {
    use object_store::aws::AmazonS3Builder;
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_allow_http(config.allow_http)
        .with_virtual_hosted_style_request(!config.force_path_style);
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    }
    builder.build().expect("build object_store s3")
}

enum Filter {
    SeriesId([u8; 16]),
    Label { col: usize, value: String },
}

/// Runs one async Parquet scan with page index enabled, an optional row
/// filter (predicate pushdown), and an output projection. Returns the number
/// of rows the scan yielded.
async fn parquet_scan(
    store: Arc<CountingObjectStore>,
    path: &OsPath,
    filter: Option<Filter>,
    output_cols: &[usize],
) -> usize {
    let reader = ParquetObjectReader::new(store, path.clone());
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .expect("open parquet stream");

    let schema_descr = builder.parquet_schema().clone();

    if let Some(filter) = filter {
        let (mask_col, predicate): (
            usize,
            Box<dyn Fn(RecordBatch) -> arrow::error::Result<BooleanArray> + Send + Sync>,
        ) = match filter {
            Filter::SeriesId(target) => (
                0,
                Box::new(move |batch: RecordBatch| {
                    let col = batch.column(0);
                    let arr = col
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .expect("series_id is FixedSizeBinary");
                    Ok((0..arr.len())
                        .map(|i| Some(arr.value(i) == target.as_slice()))
                        .collect::<BooleanArray>())
                }),
            ),
            Filter::Label { col, value } => (
                col,
                Box::new(move |batch: RecordBatch| {
                    let column = batch.column(0);
                    let dict = column
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .expect("label is Dictionary(Int32, Utf8)");
                    let values = dict
                        .values()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("dictionary values are Utf8");
                    let keys = dict.keys();
                    Ok((0..dict.len())
                        .map(|i| {
                            if keys.is_null(i) {
                                Some(false)
                            } else {
                                let k = keys.value(i) as usize;
                                Some(values.value(k) == value)
                            }
                        })
                        .collect::<BooleanArray>())
                }),
            ),
        };
        let mask = ProjectionMask::roots(&schema_descr, [mask_col]);
        let pred = ArrowPredicateFn::new(mask, predicate);
        builder = builder.with_row_filter(RowFilter::new(vec![Box::new(pred)]));
    }

    if !output_cols.is_empty() {
        let mask = ProjectionMask::roots(&schema_descr, output_cols.iter().copied());
        builder = builder.with_projection(mask);
    }

    let mut stream = builder.build().expect("build parquet stream");
    let mut rows = 0usize;
    while let Some(batch) = stream.try_next().await.expect("read batch") {
        rows += batch.num_rows();
    }
    rows
}

// ------------------------------------------------------- parquet writing

fn collect_label_names(raw: &[RawSeries]) -> Vec<String> {
    use std::collections::BTreeSet;
    let mut names: BTreeSet<String> = BTreeSet::new();
    for (_, labels, _) in raw {
        for l in labels.iter() {
            names.insert(l.name.clone());
        }
    }
    names.into_iter().collect()
}

fn build_parquet_object(raw: &[RawSeries], label_names: &[String]) -> Vec<u8> {
    let schema = build_schema(label_names);

    struct SeriesMeta {
        id: [u8; 16],
        label_values: Vec<String>,
    }
    let metas: Vec<SeriesMeta> = raw
        .iter()
        .map(|(id, labels, _)| SeriesMeta {
            id: id.0,
            label_values: label_names
                .iter()
                .map(|n| labels.get(n).unwrap_or("").to_string())
                .collect(),
        })
        .collect();

    // Rows sorted by (series_id bytes, ts), the phase-a "sorted" layout.
    struct Row {
        series: u32,
        ts: i64,
        value: f64,
    }
    let mut rows: Vec<Row> = Vec::new();
    for (sidx, (_, _, samples)) in raw.iter().enumerate() {
        for s in samples {
            rows.push(Row {
                series: sidx as u32,
                ts: s.ts_ns,
                value: s.value,
            });
        }
    }
    rows.sort_by(|a, b| {
        metas[a.series as usize]
            .id
            .cmp(&metas[b.series as usize].id)
            .then_with(|| a.ts.cmp(&b.ts))
    });

    let label_count = label_names.len();
    let mut sid = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut ts: Vec<i64> = Vec::with_capacity(rows.len());
    let mut val: Vec<f64> = Vec::with_capacity(rows.len());
    let mut labels: Vec<StringDictionaryBuilder<Int32Type>> = (0..label_count)
        .map(|_| StringDictionaryBuilder::new())
        .collect();
    for row in &rows {
        let meta = &metas[row.series as usize];
        sid.append_value(meta.id).expect("append series_id");
        ts.push(row.ts);
        val.push(row.value);
        for (col, builder) in labels.iter_mut().enumerate() {
            builder.append_value(&meta.label_values[col]);
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(3 + label_count);
    columns.push(Arc::new(sid.finish()));
    columns.push(Arc::new(Int64Array::from(ts)));
    columns.push(Arc::new(Float64Array::from(val)));
    for mut builder in labels {
        columns.push(Arc::new(builder.finish()));
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("record batch");

    let mut buf: Vec<u8> = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, schema, Some(writer_props())).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    buf
}

fn build_schema(label_names: &[String]) -> SchemaRef {
    let mut fields = vec![
        Field::new("series_id", DataType::FixedSizeBinary(16), false),
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ];
    for name in label_names {
        fields.push(Field::new(
            name,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ));
    }
    Arc::new(Schema::new(fields))
}

fn writer_props() -> WriterProperties {
    let value = ColumnPath::from("value".to_string());
    let ts = ColumnPath::from("ts".to_string());
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("zstd level 3"),
        ))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_dictionary_enabled(value.clone(), false)
        .set_column_encoding(value, Encoding::BYTE_STREAM_SPLIT)
        .set_column_dictionary_enabled(ts.clone(), false)
        .set_column_encoding(ts, Encoding::DELTA_BINARY_PACKED)
        .build()
}
