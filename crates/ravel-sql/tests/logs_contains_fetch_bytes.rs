//! Issue #3295 item 7: `"URL" LIKE '%lit%'` pushed into the logs scan as a
//! `Contains` content arm must fetch no more object bytes than the same
//! statement with the LIKE evaluated above the scan.
//!
//! The fixture is a ClickBench-shaped logs tenant: a declared Str column `URL`
//! (cells 40 to 160 bytes, the needle in 1% of rows), declared Str `Title`
//! (300 to 600 bytes) and `Referer` (40 to 160 bytes) columns, so `URL` stays
//! well under the ranged fetch's coverage crossover, a
//! low-cardinality declared Str `SearchPhrase` that is also an indexed field
//! (so the objects carry POSTINGS), a declared I64 `UserID` and a declared Bool
//! `IsRefresh`. The writer's default bloom scope covers every string attribute,
//! so the objects carry a BLOOM section sized by those columns.
//!
//! Pushdown is disabled the way the other logs tests disable it: the same LIKE
//! with `ESCAPE '#'`, which the extractor declines, so no arm is pushed and
//! DataFusion evaluates the LIKE above the scan. Both statements take the
//! whole-segment fast path on its ranged open (`fast_path_ranged_segments`),
//! with the same column selection.
//!
//! A recording store logs every GET by key and byte range; each range is then
//! attributed to the object's footer sections and, inside BLOCKS, to the
//! `(row group, column id)` chunks it covers, so a difference between the two
//! runs names its columns or sections.
//!
//! No `.cstat` object exists, so every statement scans.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect};
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::kind;
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, LogRecord, RlogConfig, RlogWriter, read_section, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{
    BlockRangeFetcher, CacheFetchError, EngineConfig, LogSegmentFetcher, SegmentFetcher,
};
use ravel_sql::{DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([41u8; 16]);

/// Twice [`PARTITIONS`], so the whole-segment fast path's partition conjunct
/// holds and only the predicate could decide the route.
const OBJECTS: usize = 4;
const PARTITIONS: usize = 2;

const ROWS_PER_BLOCK: usize = 50;
const BLOCKS_PER_GROUP: usize = 4;
const GROUPS_PER_OBJECT: usize = 4;
const ROWS_PER_OBJECT: usize = ROWS_PER_BLOCK * BLOCKS_PER_GROUP * GROUPS_PER_OBJECT;

/// One row in [`NEEDLE_EVERY`] carries the needle in its `URL`.
const NEEDLE: &str = "google";
const NEEDLE_EVERY: usize = 100;

/// Sized to the object tail (footer and its directories), so the probe does
/// not swallow the object the way the production 256 KiB default would here.
const SUFFIX_LEN: u64 = 16 << 10;
/// Small enough that the projection's chunks in one row group stay separate
/// ranges, large enough to fuse a chunk with a neighbour a few bytes away.
const COALESCE_GAP: u64 = 256;
/// The routing threshold and projection break-even: well under one object, so
/// the narrow projection routes ranged.
const THRESHOLD: u64 = 8 << 10;

const SEARCH_PHRASES: [&str; 5] = ["", "", "maps", "news", "weather"];

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("URL", DeclaredType::Str),
        DeclaredColumn::new("Title", DeclaredType::Str),
        DeclaredColumn::new("Referer", DeclaredType::Str),
        DeclaredColumn::new("SearchPhrase", DeclaredType::Str),
        DeclaredColumn::new("UserID", DeclaredType::I64),
        DeclaredColumn::new("IsRefresh", DeclaredType::Bool),
    ]
}

/// Pseudo-random printable filler, so compression keeps the cells near their
/// written length.
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

/// Row `i` of the tenant (global ordinal): a `URL` of 40 to 160 bytes holding
/// [`NEEDLE`] when `i % NEEDLE_EVERY == 37`, never otherwise (the filler
/// alphabet is case-mixed, so the check below proves it).
fn url(i: usize) -> String {
    let len = 40 + (i * 37) % 121;
    let body = filler(i as u64 * 5 + 1, len - "https://".len() - NEEDLE.len());
    if i % NEEDLE_EVERY == 37 {
        format!("https://{}{NEEDLE}{}", &body[..len / 4], &body[len / 4..])
    } else {
        format!("https://{body}{}", filler(i as u64 * 5 + 2, NEEDLE.len()))
    }
}

fn record(i: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("web".to_string()),
    )];
    let ts = i as i64 + 1;
    let attrs = vec![
        ("URL".to_string(), AttrValue::Str(url(i))),
        (
            "Title".to_string(),
            AttrValue::Str(filler(i as u64 * 5 + 3, 300 + (i * 13) % 301)),
        ),
        (
            "Referer".to_string(),
            AttrValue::Str(format!(
                "https://{}",
                filler(i as u64 * 5 + 4, 32 + (i * 7) % 121)
            )),
        ),
        (
            "SearchPhrase".to_string(),
            AttrValue::Str(SEARCH_PHRASES[i % SEARCH_PHRASES.len()].to_string()),
        ),
        ("UserID".to_string(), AttrValue::I64((i % 97) as i64)),
        (
            "IsRefresh".to_string(),
            AttrValue::Bool(i.is_multiple_of(3)),
        ),
    ];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("hit {i}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn config() -> RlogConfig {
    RlogConfig {
        block_target_records: ROWS_PER_BLOCK,
        group_target_blocks: BLOCKS_PER_GROUP,
        ..RlogConfig::default()
    }
}

fn key(n: usize) -> String {
    format!("logs/contains-fetch-bytes-{n}.rlog")
}

struct Fixture {
    store: Arc<MemoryStore>,
    snapshot: Snapshot,
    objects: Vec<(String, Vec<u8>)>,
}

async fn fixture() -> Fixture {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    let mut objects = Vec::new();
    for n in 0..OBJECTS {
        let records: Vec<LogRecord> = (0..ROWS_PER_OBJECT)
            .map(|i| record(n * ROWS_PER_OBJECT + i))
            .collect();
        let mut w = RlogWriter::new(
            config(),
            ObjectIdentity {
                tenant_hash: TENANT.0,
                shard: 0,
                writer_id: [6u8; 16],
                writer_epoch: 1,
                writer_seq: n as u64 + 1,
            },
        )
        .with_indexed_fields(vec!["SearchPhrase".to_string()]);
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
        objects.push((key(n), bytes));
        segments.push(SegmentRef {
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
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        });
    }
    Fixture {
        store,
        snapshot: Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        objects,
    }
}

// ---- recording store -------------------------------------------------------

/// One GET: the key, the resolved `[start, end)` it returned, and its bytes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Get {
    key: String,
    start: u64,
    end: u64,
}

struct RecordingStore {
    inner: Arc<MemoryStore>,
    gets: Mutex<Vec<Get>>,
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
        let size = self.inner.head(key).await?.size;
        let got = self.inner.get(key, range).await?;
        let (start, end) = match range {
            GetRange::Full => (0, size),
            GetRange::Range(s, e) => (s, e.min(size)),
            GetRange::Suffix(n) => (size.saturating_sub(n), size),
        };
        assert_eq!(end - start, got.data.len() as u64, "range resolution");
        self.gets.lock().expect("lock").push(Get {
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

fn read_cache() -> Arc<Cache<CacheFetchError>> {
    let bytes = 64 << 20;
    Arc::new(Cache::new(CacheLimits::new(
        bytes,
        (bytes / 4096) as usize,
        bytes,
    )))
}

fn executor(store: Arc<dyn ObjectStoreBackend>) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_suffix_len(SUFFIX_LEN)
        .with_coalesce_gap(COALESCE_GAP);
    let logs = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range(block_range)
        .with_cache(read_cache())
        .with_block_range_threshold(THRESHOLD);
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        logs,
        SpanSegmentFetcher::new(Arc::clone(&store)),
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
    gets: Vec<Get>,
    ranged_segments: usize,
    whole_object_segments: usize,
    contains_cells_tested: usize,
    batches: Vec<RecordBatch>,
}

impl Run {
    fn bytes(&self) -> u64 {
        self.gets.iter().map(|g| g.end - g.start).sum()
    }
}

/// Plan and run `sql` against a fresh executor, fetcher and cache over the
/// fixture's objects, recording every GET.
async fn run(fx: &Fixture, sql: &str) -> Run {
    let recording = Arc::new(RecordingStore {
        inner: Arc::clone(&fx.store),
        gets: Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;
    let pinned = executor(store)
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
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    let scan = find_scan(&plan);
    let mut gets = recording.gets.lock().expect("lock").clone();
    gets.sort();
    Run {
        gets,
        ranged_segments: sum_metric(&scan, "fast_path_ranged_segments"),
        whole_object_segments: sum_metric(&scan, "fast_path_whole_object_segments"),
        contains_cells_tested: sum_metric(&scan, "contains_cells_tested"),
        batches,
    }
}

fn count(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row")
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count(*) is Int64")
        .value(0)
}

// ---- attribution -----------------------------------------------------------

/// One labelled byte extent of an object: a footer section, a column chunk of
/// one row group inside BLOCKS, or the footer and trailer.
struct Region {
    start: u64,
    end: u64,
    label: String,
}

fn section_name(k: u32) -> &'static str {
    match k {
        kind::STREAM_DIR => "STREAM_DIR",
        kind::FIELD_DIR => "FIELD_DIR",
        kind::BLOCKS => "BLOCKS",
        kind::SKIP_IDX => "SKIP_IDX",
        kind::BLOOM => "BLOOM",
        kind::POSTINGS => "POSTINGS",
        kind::PAGE_DIR => "PAGE_DIR",
        kind::KEY_IDX => "KEY_IDX",
        _ => "SECTION?",
    }
}

fn column_name(field_dir: &FieldDir, column_id: u32) -> String {
    const FIXED: [&str; 10] = [
        "ts",
        "observed_ts",
        "stream_ref",
        "severity_num",
        "severity_text",
        "body",
        "trace_id",
        "span_id",
        "flags",
        "attrs_raw",
    ];
    match FIXED.get(column_id as usize) {
        Some(name) => (*name).to_string(),
        None => field_dir
            .by_column_id(column_id)
            .map_or_else(|| "?".to_string(), |e| e.name.clone()),
    }
}

/// Every labelled extent of `bytes`, sorted by start.
fn regions(bytes: &[u8]) -> Vec<Region> {
    let cfg = config();
    let footer = ravel_logseg::footer::open(bytes).expect("footer");
    let desc = |k: u32| *footer.section(k).expect("section");
    let field_dir = FieldDir::decode(
        &read_section(bytes, &desc(kind::FIELD_DIR), &cfg).expect("read"),
        u64::MAX,
    )
    .expect("field dir");
    let page_dir =
        PageDir::decode(&read_section(bytes, &desc(kind::PAGE_DIR), &cfg).expect("read"))
            .expect("page dir");
    let blocks = desc(kind::BLOCKS);
    let mut out = Vec::new();
    for s in &footer.sections {
        if s.kind != kind::BLOCKS {
            out.push(Region {
                start: s.offset,
                end: s.offset + s.len,
                label: section_name(s.kind).to_string(),
            });
        }
    }
    for (g, group) in page_dir.groups.iter().enumerate() {
        for chunk in &group.chunks {
            let (offset, len) = chunk.extent().expect("extent");
            out.push(Region {
                start: blocks.offset + offset,
                end: blocks.offset + offset + len,
                label: format!(
                    "g{g} col {} {}",
                    chunk.column_id,
                    column_name(&field_dir, chunk.column_id)
                ),
            });
        }
    }
    let sections_end = footer
        .sections
        .iter()
        .map(|s| s.offset + s.len)
        .max()
        .expect("sections");
    out.push(Region {
        start: sections_end,
        end: bytes.len() as u64,
        label: "footer+trailer".to_string(),
    });
    out.sort_by_key(|r| r.start);
    out
}

/// Bytes each GET of `gets` fetched, summed per region label with the row
/// group stripped (so `col 14 URL` totals every group's URL chunk), plus
/// `unlabelled` for bytes no region covers.
fn attribute(fx: &Fixture, gets: &[Get]) -> BTreeMap<String, u64> {
    let layouts: BTreeMap<&str, Vec<Region>> = fx
        .objects
        .iter()
        .map(|(k, b)| (k.as_str(), regions(b)))
        .collect();
    let mut out = BTreeMap::new();
    for g in gets {
        let layout = &layouts[g.key.as_str()];
        let mut covered = 0;
        for r in layout {
            let lo = g.start.max(r.start);
            let hi = g.end.min(r.end);
            if lo < hi {
                let label = match r.label.split_once(' ') {
                    Some((group, rest)) if group.starts_with('g') => rest.to_string(),
                    _ => r.label.clone(),
                };
                *out.entry(label).or_insert(0) += hi - lo;
                covered += hi - lo;
            }
        }
        if covered < g.end - g.start {
            *out.entry("unlabelled".to_string()).or_insert(0) += g.end - g.start - covered;
        }
    }
    out
}

/// The section and column-chunk labels `[start, end)` of `key` covers.
fn labels(fx: &Fixture, g: &Get) -> Vec<String> {
    let (_, bytes) = fx
        .objects
        .iter()
        .find(|(k, _)| *k == g.key)
        .expect("object");
    regions(bytes)
        .into_iter()
        .filter(|r| g.start.max(r.start) < g.end.min(r.end))
        .map(|r| r.label)
        .collect()
}

/// The GETs `a` issued that `b` did not, each with the regions it covers.
fn only_in(fx: &Fixture, a: &Run, b: &Run) -> Vec<(Get, Vec<String>)> {
    a.gets
        .iter()
        .filter(|g| !b.gets.contains(g))
        .map(|g| (g.clone(), labels(fx, g)))
        .collect()
}

/// Both runs' totals, per-region attribution and the GETs only one of them
/// issued, for an assertion message.
fn diff(fx: &Fixture, pushed: &Run, unpushed: &Run) -> String {
    let mut out = String::new();
    for (name, r) in [("pushed", pushed), ("unpushed", unpushed)] {
        out.push_str(&format!(
            "{name}: {} GETs, {} bytes {:?}\n",
            r.gets.len(),
            r.bytes(),
            attribute(fx, &r.gets)
        ));
    }
    for (g, labels) in only_in(fx, pushed, unpushed) {
        out.push_str(&format!("pushed only: {g:?} {labels:?}\n"));
    }
    for (g, labels) in only_in(fx, unpushed, pushed) {
        out.push_str(&format!("unpushed only: {g:?} {labels:?}\n"));
    }
    out
}

/// The pushed statement issues exactly the unpushed statement's GETs, every
/// key and byte range the same, and both take the ranged open on every
/// object.
fn assert_same_fetch(fx: &Fixture, pushed: &Run, unpushed: &Run, gets: usize, bytes: u64) {
    assert!(pushed.contains_cells_tested > 0, "the arm was pushed");
    assert_eq!(unpushed.contains_cells_tested, 0, "no arm pushed");
    assert_eq!(
        (pushed.ranged_segments, pushed.whole_object_segments),
        (OBJECTS, 0),
        "pushed opens every object ranged"
    );
    assert_eq!(
        (unpushed.ranged_segments, unpushed.whole_object_segments),
        (OBJECTS, 0),
        "unpushed opens every object ranged"
    );
    assert_eq!(
        (unpushed.gets.len(), unpushed.bytes()),
        (gets, bytes),
        "unpushed fetch\n{}",
        diff(fx, pushed, unpushed)
    );
    assert!(
        pushed.gets == unpushed.gets,
        "pushed fetched other ranges\n{}",
        diff(fx, pushed, unpushed)
    );
}

const PUSHED: &str = r#"SELECT COUNT(*) FROM logs WHERE "URL" LIKE '%google%'"#;
const UNPUSHED: &str = r#"SELECT COUNT(*) FROM logs WHERE "URL" LIKE '%google%' ESCAPE '#'"#;

/// The q21 shape. Both statements count the 32 needle rows and fetch the
/// same 20 GETs and 651,322 bytes: per object, five ranges over the
/// projected chunks (the L0 run bridging folds row group 0 into one range)
/// and the 16 KiB suffix probe.
///
/// Before the fix, the pushed statement fetched 28 GETs and 748,622 bytes.
/// The extra 8 GETs and 97,300 bytes were, per object, one GET over POSTINGS
/// (83 bytes) and one over the part of BLOOM the suffix probe misses (24,226
/// to 24,354 bytes), with no column chunk among them: the ranged fetch
/// placed both sections for any content arm, and the reader reads neither
/// for a `Contains` arm.
#[tokio::test]
async fn q21_shape_pushed_fetches_what_the_unpushed_like_fetches() {
    let fx = fixture().await;
    let pushed = run(&fx, PUSHED).await;
    let unpushed = run(&fx, UNPUSHED).await;

    let matches = (OBJECTS * ROWS_PER_OBJECT / NEEDLE_EVERY) as i64;
    assert_eq!(matches, 32);
    assert_eq!(count(&pushed.batches), matches);
    assert_eq!(count(&unpushed.batches), matches);
    assert_same_fetch(&fx, &pushed, &unpushed, 20, 651_322);
}

const Q22_PUSHED: &str = r#"SELECT "SearchPhrase", MIN("URL"), COUNT(*) AS c FROM logs WHERE "URL" LIKE '%google%' AND "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC, "SearchPhrase" LIMIT 10"#;
const Q22_UNPUSHED: &str = r#"SELECT "SearchPhrase", MIN("URL"), COUNT(*) AS c FROM logs WHERE "URL" LIKE '%google%' ESCAPE '#' AND "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC, "SearchPhrase" LIMIT 10"#;

/// The q22 shape, which adds `SearchPhrase` to the projection and a
/// `SearchPhrase <> ''` filter: the same rows and the same 20 GETs and
/// 853,846 bytes on both statements.
#[tokio::test]
async fn q22_shape_pushed_fetches_what_the_unpushed_like_fetches() {
    let fx = fixture().await;
    let pushed = run(&fx, Q22_PUSHED).await;
    let unpushed = run(&fx, Q22_UNPUSHED).await;

    let rows = |b: &[RecordBatch]| {
        datafusion::arrow::util::pretty::pretty_format_batches(b)
            .expect("format")
            .to_string()
    };
    assert_eq!(rows(&pushed.batches), rows(&unpushed.batches));
    let total: usize = pushed.batches.iter().map(RecordBatch::num_rows).sum();
    assert!(total > 0, "the needle rows carry non-empty phrases");
    assert_same_fetch(&fx, &pushed, &unpushed, 20, 853_846);
}
