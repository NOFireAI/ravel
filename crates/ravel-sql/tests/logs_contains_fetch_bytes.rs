//! Issue #3295 item 7: a `LIKE '%lit%'` pushed into the logs scan as a
//! `Contains` content arm fetches more object bytes than the same statement
//! with the LIKE evaluated above the scan.
//!
//! The fixture is the ClickBench q21 shape at small scale: four objects of
//! sixteen blocks, a declared `Str` column `URL` (40-160 byte cells, the
//! needle in one row of a hundred) beside three other declared columns of
//! mixed types, written with the default writer configuration (BLOOM over
//! every string column, no POSTINGS fields). The fetcher is set so both runs
//! take the whole-segment fast path's ranged entry and the probe-and-range
//! read, the route the ClickBench objects take: every object above the size
//! crossover, a 4 KiB suffix probe and a 1 KiB coalesce gap scaled to these
//! small objects.
//!
//! `SELECT COUNT(*) FROM logs WHERE "URL" LIKE '%google%'` runs twice: as main
//! plans it (the arm pushed), and with `ESCAPE '#'`, which the extractor
//! declines, so the LIKE is evaluated above the scan. Every GET either run
//! issues is recorded by `(key, range)` and mapped onto the object's
//! sections and BLOCKS column chunks.
//!
//! What the extra is: one ranged GET per object for the BLOOM section, which
//! the pushed run places because `FilterSections::for_query`
//! (crates/ravel-query/src/log_fetcher.rs) reads any non-empty content list
//! as one the reader may answer from BLOOM or POSTINGS. A `Contains` arm is
//! neither `bloom_shaped` nor a postings arm in `RlogReader::scan_blocks`, so
//! the reader never reads the section it paid for. Every other range is the
//! same in both runs: same column chunks, same offsets.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect};
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::record::{
    COL_ATTRS_RAW, COL_BODY, COL_FLAGS, COL_OBSERVED_TS, COL_SEVERITY_NUM, COL_SEVERITY_TEXT,
    COL_SPAN_ID, COL_STREAM_REF, COL_TRACE_ID, COL_TS,
};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, read_section, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{BlockRangeFetcher, EngineConfig, LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([41u8; 16]);

/// Twice [`PARTITIONS`], so the whole-segment fast path's partition conjunct
/// holds and only the predicate could decide the route.
const OBJECTS: usize = 4;
const PARTITIONS: usize = 2;
const ROWS_PER_BLOCK: usize = 100;
const BLOCKS_PER_OBJECT: usize = 16;
/// Four row groups per object, so the projection is several ranges apart.
const BLOCKS_PER_GROUP: usize = 4;
const ROWS_PER_OBJECT: usize = ROWS_PER_BLOCK * BLOCKS_PER_OBJECT;
/// One row in this many carries the needle in its `URL`.
const NEEDLE_EVERY: usize = 100;

/// Suffix probe length, sized to this fixture's object tail: at the
/// production 256 KiB one probe would hold every tail section of these
/// objects and the sections would cost no GET of their own.
const SUFFIX_LEN: u64 = 4096;
/// Coalesce gap, sized below one `Title` chunk so the `URL` chunks of
/// consecutive row groups stay separate ranges, as the chunks of a wide
/// ClickBench object do under the production 64 KiB gap.
const COALESCE_GAP: u64 = 1024;

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("URL", DeclaredType::Str),
        DeclaredColumn::new("Title", DeclaredType::Str),
        DeclaredColumn::new("RegionID", DeclaredType::I64),
        DeclaredColumn::new("IsRefresh", DeclaredType::Bool),
    ]
}

/// Pseudo-random printable filler (splitmix64), so compression cannot fold
/// the `URL` and `Title` cells away.
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

/// Row `i` of object `n`. The `URL` is 40 to 160 bytes; one row in
/// [`NEEDLE_EVERY`] holds `google`.
fn record(n: usize, i: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("web".to_string()),
    )];
    let ts = (n * ROWS_PER_OBJECT + i + 1) as i64;
    let seed = (n as u64) << 32 | i as u64;
    let url_len = 40 + (i * 37) % 121;
    let host = if i % NEEDLE_EVERY == 7 {
        "www.google.com"
    } else {
        "www.example.org"
    };
    let path = filler(seed, url_len - host.len() - 9);
    let url = format!("https://{host}/{path}");
    let attrs = vec![
        ("URL".to_string(), AttrValue::Str(url)),
        (
            "Title".to_string(),
            AttrValue::Str(filler(seed ^ 0xA5A5, 20 + i % 40)),
        ),
        ("RegionID".to_string(), AttrValue::I64((i % 230) as i64)),
        ("IsRefresh".to_string(), AttrValue::Bool(i % 3 == 0)),
    ];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("hit {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn writer_config() -> RlogConfig {
    RlogConfig {
        block_target_records: ROWS_PER_BLOCK,
        group_target_blocks: BLOCKS_PER_GROUP,
        ..RlogConfig::default()
    }
}

fn key(n: usize) -> String {
    format!("logs/contains-fetch-{n}.rlog")
}

/// Write object `n` and return its catalog entry and its bytes.
async fn write_object(store: &dyn ObjectStoreBackend, n: usize) -> (SegmentRef, Vec<u8>) {
    let records: Vec<LogRecord> = (0..ROWS_PER_OBJECT).map(|i| record(n, i)).collect();
    let mut w = RlogWriter::new(
        writer_config(),
        ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [6u8; 16],
            writer_epoch: 1,
            writer_seq: n as u64 + 1,
        },
    );
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let content_hash = *blake3::hash(&bytes).as_bytes();
    let object_size = bytes.len() as u64;
    store
        .put(
            &key(n),
            bytes::Bytes::from(bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put");
    let seg = SegmentRef {
        data_object_key: key(n),
        object_size,
        min_event_ts_ns: records.iter().map(|r| r.ts_ns).min().expect("nonempty"),
        max_event_ts_ns: records.iter().map(|r| r.ts_ns).max().expect("nonempty"),
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(6),
        writer_epoch: 1,
        writer_seq: n as u64 + 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(footer::VERSION),
        declared_column_stats: Default::default(),
    };
    (seg, bytes)
}

/// One GET the store served: key, `[start, end)` in object coordinates, and
/// the bytes returned.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Get {
    key: String,
    start: u64,
    end: u64,
}

/// A pass-through store that records every GET by key and resolved range.
struct RecordingStore {
    inner: Arc<MemoryStore>,
    gets: Mutex<Vec<Get>>,
}

impl RecordingStore {
    fn take(&self) -> Vec<Get> {
        std::mem::take(&mut *self.gets.lock().expect("gets lock"))
    }
}

#[async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let got = self.inner.get(key, range).await?;
        let len = got.data.len() as u64;
        let (start, end) = match range {
            GetRange::Full => (0, got.total_size),
            GetRange::Range(s, e) => (s, e),
            GetRange::Suffix(_) => (got.total_size - len, got.total_size),
        };
        assert_eq!(end - start, len, "a GET returns exactly its range");
        self.gets.lock().expect("gets lock").push(Get {
            key: key.to_string(),
            start,
            end,
        });
        Ok(got)
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// One named byte region of an object: a section, a BLOCKS column chunk, or
/// the footer and trailer.
#[derive(Clone, Debug)]
struct Region {
    start: u64,
    end: u64,
    label: String,
}

fn kind_name(k: u32) -> &'static str {
    match k {
        kind::STREAM_DIR => "STREAM_DIR",
        kind::FIELD_DIR => "FIELD_DIR",
        kind::BLOCKS => "BLOCKS",
        kind::SKIP_IDX => "SKIP_IDX",
        kind::BLOOM => "BLOOM",
        kind::POSTINGS => "POSTINGS",
        kind::PAGE_DIR => "PAGE_DIR",
        kind::KEY_IDX => "KEY_IDX",
        _ => "UNKNOWN",
    }
}

fn fixed_column_name(id: u32) -> Option<&'static str> {
    Some(match id {
        COL_TS => "ts",
        COL_OBSERVED_TS => "observed_ts",
        COL_STREAM_REF => "stream_ref",
        COL_SEVERITY_NUM => "severity_num",
        COL_SEVERITY_TEXT => "severity_text",
        COL_BODY => "body",
        COL_TRACE_ID => "trace_id",
        COL_SPAN_ID => "span_id",
        COL_FLAGS => "flags",
        COL_ATTRS_RAW => "attrs_raw",
        _ => return None,
    })
}

/// Every region of `object`, in offset order: each non-BLOCKS section whole,
/// each BLOCKS column chunk as `g<group>/c<column id>:<name>`, and the footer
/// plus trailer.
fn regions(object: &[u8]) -> Vec<Region> {
    let cfg = RlogConfig::default();
    let f = footer::open(object).expect("footer");
    let section = |k: u32| {
        f.sections
            .iter()
            .find(|s| s.kind == k)
            .copied()
            .unwrap_or_else(|| panic!("{} section", kind_name(k)))
    };
    let field_dir = FieldDir::decode(
        &read_section(object, &section(kind::FIELD_DIR), &cfg).expect("FIELD_DIR"),
        u64::MAX,
    )
    .expect("decode FIELD_DIR");
    let page_dir = PageDir::decode(
        &read_section(object, &section(kind::PAGE_DIR), &cfg).expect("PAGE_DIR"),
    )
    .expect("decode PAGE_DIR");
    let blocks = section(kind::BLOCKS);

    let mut out = Vec::new();
    for s in &f.sections {
        if s.kind == kind::BLOCKS {
            continue;
        }
        out.push(Region {
            start: s.offset,
            end: s.offset + s.len,
            label: kind_name(s.kind).to_string(),
        });
    }
    for (g, group) in page_dir.groups.iter().enumerate() {
        for chunk in &group.chunks {
            let (offset, len) = chunk.extent().expect("chunk extent");
            let name = fixed_column_name(chunk.column_id)
                .map(str::to_string)
                .or_else(|| {
                    field_dir
                        .by_column_id(chunk.column_id)
                        .map(|e| e.name.clone())
                })
                .unwrap_or_else(|| "?".to_string());
            out.push(Region {
                start: blocks.offset + offset,
                end: blocks.offset + offset + len,
                label: format!("g{g}/c{}:{name}", chunk.column_id),
            });
        }
    }
    let footer_start = f
        .sections
        .iter()
        .map(|s| s.offset + s.len)
        .max()
        .expect("sections");
    out.push(Region {
        start: footer_start,
        end: object.len() as u64,
        label: "footer+trailer".to_string(),
    });
    out.sort_by_key(|r| r.start);
    out
}

/// The regions `[start, end)` overlaps, with the bytes of each it covers.
fn covered(regions: &[Region], start: u64, end: u64) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    let mut mapped = 0;
    for r in regions {
        let s = r.start.max(start);
        let e = r.end.min(end);
        if s < e {
            out.push((r.label.clone(), e - s));
            mapped += e - s;
        }
    }
    if mapped < end - start {
        out.push(("unmapped".to_string(), end - start - mapped));
    }
    out
}

struct Fixture {
    store: Arc<RecordingStore>,
    snapshot: Snapshot,
    objects: BTreeMap<String, Vec<u8>>,
}

async fn fixture() -> Fixture {
    let inner = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    let mut objects = BTreeMap::new();
    for n in 0..OBJECTS {
        let (seg, bytes) = write_object(inner.as_ref(), n).await;
        objects.insert(seg.data_object_key.clone(), bytes);
        segments.push(seg);
    }
    Fixture {
        store: Arc::new(RecordingStore {
            inner,
            gets: Mutex::new(Vec::new()),
        }),
        snapshot: Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        objects,
    }
}

/// The executor over `fx`'s store. Half the smallest object is the
/// block-range threshold and, through `with_block_range_threshold`, the ranged
/// fetch's whole-object crossover and projection break-even; every object is
/// above it. No read cache, so every byte a run reads is a store GET.
fn executor(fx: &Fixture) -> SqlExecutor {
    let backend: Arc<dyn ObjectStoreBackend> =
        Arc::clone(&fx.store) as Arc<dyn ObjectStoreBackend>;
    let threshold = fx
        .snapshot
        .segments
        .iter()
        .map(|s| s.object_size)
        .min()
        .expect("segments")
        / 2;
    let log_fetcher = LogSegmentFetcher::new(Arc::clone(&backend))
        .with_block_range(
            BlockRangeFetcher::new(Arc::clone(&backend))
                .with_suffix_len(SUFFIX_LEN)
                .with_coalesce_gap(COALESCE_GAP),
        )
        .with_block_range_threshold(threshold);
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&backend)),
        log_fetcher,
        SpanSegmentFetcher::new(Arc::clone(&backend)),
        SqlConfig {
            engine: EngineConfig {
                fetch_concurrency: PARTITIONS,
                sql_partition_count: Some(PARTITIONS),
                ..EngineConfig::default()
            },
            ..SqlConfig::default()
        },
        1 << 30,
    )
}

fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    fn walk(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == "LogsScanExec" {
            return Some(Arc::clone(plan));
        }
        plan.children().iter().find_map(|c| walk(c))
    }
    walk(plan).expect("a LogsScanExec leaf")
}

fn sum_metric(scan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    scan.metrics()
        .expect("metrics")
        .iter()
        .filter(|m| m.value().name() == name)
        .map(|m| m.value().as_usize())
        .sum()
}

struct Run {
    count: i64,
    ranged_segments: usize,
    whole_object_segments: usize,
    contains_cells_tested: usize,
    /// Every data-object GET, sorted.
    gets: Vec<Get>,
}

impl Run {
    fn bytes(&self) -> u64 {
        self.gets.iter().map(|g| g.end - g.start).sum()
    }
}

async fn run(fx: &Fixture, sql: &str) -> Run {
    let pinned = executor(fx)
        .plan_pinned(
            TENANT,
            fx.snapshot.clone(),
            sql,
            &QueryAccounting::new(),
            &declared(),
        )
        .await
        .expect("plan");
    let plan = pinned.create_physical_plan().await.expect("physical plan");
    fx.store.take();
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    let mut gets: Vec<Get> = fx
        .store
        .take()
        .into_iter()
        .filter(|g| fx.objects.contains_key(&g.key))
        .collect();
    gets.sort();
    let scan = find_scan(&plan);
    Run {
        count: count(&batches),
        ranged_segments: sum_metric(&scan, "fast_path_ranged_segments"),
        whole_object_segments: sum_metric(&scan, "fast_path_whole_object_segments"),
        contains_cells_tested: sum_metric(&scan, "contains_cells_tested"),
        gets,
    }
}

fn count(batches: &[RecordBatch]) -> i64 {
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count(*) is Int64")
        .value(0)
}

/// Print every GET of `run` with the regions it covers.
fn describe(fx: &Fixture, label: &str, run: &Run) {
    eprintln!(
        "== {label}: {} GETs, {} bytes",
        run.gets.len(),
        run.bytes()
    );
    for g in &run.gets {
        let regions = regions(&fx.objects[&g.key]);
        eprintln!(
            "  {} [{}, {}) {} bytes: {:?}",
            g.key,
            g.start,
            g.end,
            g.end - g.start,
            covered(&regions, g.start, g.end)
        );
    }
}

/// `gets` less every element of `minus`, as multisets.
fn difference(gets: &[Get], minus: &[Get]) -> Vec<Get> {
    let mut left: BTreeMap<&Get, isize> = BTreeMap::new();
    for g in gets {
        *left.entry(g).or_default() += 1;
    }
    for g in minus {
        *left.entry(g).or_default() -= 1;
    }
    left.into_iter()
        .flat_map(|(g, n)| std::iter::repeat_n(g.clone(), n.max(0) as usize))
        .collect()
}

#[tokio::test]
async fn pushed_contains_fetches_the_bloom_section_the_reader_never_reads() {
    let fx = fixture().await;

    let pushed = run(&fx, "SELECT COUNT(*) FROM logs WHERE \"URL\" LIKE '%google%'").await;
    let plain = run(
        &fx,
        "SELECT COUNT(*) FROM logs WHERE \"URL\" LIKE '%google%' ESCAPE '#'",
    )
    .await;

    describe(&fx, "pushed", &pushed);
    describe(&fx, "plain", &plain);

    let expected = (OBJECTS * ROWS_PER_OBJECT / NEEDLE_EVERY) as i64;
    assert_eq!(pushed.count, expected);
    assert_eq!(plain.count, expected);
    assert!(pushed.contains_cells_tested > 0, "the arm is pushed");
    assert_eq!(plain.contains_cells_tested, 0, "ESCAPE '#' pushes no arm");
    assert_eq!(
        (pushed.ranged_segments, pushed.whole_object_segments),
        (OBJECTS, 0)
    );
    assert_eq!(
        (plain.ranged_segments, plain.whole_object_segments),
        (OBJECTS, 0)
    );

    // GETs and bytes per object: (plain GETs, plain bytes, pushed GETs,
    // pushed bytes). Plain is a 4 KiB suffix probe plus four projection
    // ranges; pushed is the same five plus one.
    let per_key = |run: &Run, key: &str| {
        let gets: Vec<&Get> = run.gets.iter().filter(|g| g.key == key).collect();
        (gets.len(), gets.iter().map(|g| g.end - g.start).sum::<u64>())
    };
    let expected_per_key: [(usize, u64, usize, u64); OBJECTS] = [
        (5, 118_365, 6, 135_677),
        (5, 118_353, 6, 135_793),
        (5, 118_359, 6, 135_799),
        (5, 118_379, 6, 135_627),
    ];
    for (n, want) in expected_per_key.iter().enumerate() {
        let (plain_gets, plain_bytes) = per_key(&plain, &key(n));
        let (pushed_gets, pushed_bytes) = per_key(&pushed, &key(n));
        assert_eq!(
            (plain_gets, plain_bytes, pushed_gets, pushed_bytes),
            *want,
            "{}",
            key(n)
        );
    }
    assert_eq!((plain.gets.len(), plain.bytes()), (20, 473_456));
    assert_eq!((pushed.gets.len(), pushed.bytes()), (24, 542_896));

    // Every range plain reads, pushed reads too, at the same offsets: the
    // projection is the same column chunks and nothing is read twice.
    assert_eq!(difference(&plain.gets, &pushed.gets), Vec::<Get>::new());

    // What pushed reads beyond that is one GET per object, exactly that
    // object's BLOOM section, which the reader never parses for a `Contains`
    // arm. Its last bytes sit inside the suffix probe already, so part of
    // each extra GET is a second read of bytes the probe returned.
    let extra = difference(&pushed.gets, &plain.gets);
    let mut bloom_sections = Vec::new();
    let mut reread = 0;
    for (k, object) in &fx.objects {
        let bloom = regions(object)
            .into_iter()
            .find(|r| r.label == "BLOOM")
            .expect("BLOOM section");
        let probe_start = object.len() as u64 - SUFFIX_LEN;
        reread += bloom.end.saturating_sub(probe_start);
        bloom_sections.push(Get {
            key: k.clone(),
            start: bloom.start,
            end: bloom.end,
        });
    }
    assert_eq!(extra, bloom_sections);
    let extra_bytes: u64 = extra.iter().map(|g| g.end - g.start).sum();
    assert_eq!(extra_bytes, pushed.bytes() - plain.bytes());
    assert_eq!(extra_bytes, 69_440);
    assert_eq!(reread, 9_750);
}
