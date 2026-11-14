//! Issue #4205 (ADR-4203 decision 1): a `LIKE '%lit%'` scan that takes the
//! striped route because it has more partitions than segments plans each
//! object from its footer and directories, and its plan phase reads no BLOCKS
//! byte.
//!
//! The fixture is the ClickBench q21 shape of `logs_contains_fetch_bytes.rs`:
//! four objects of sixteen blocks in four row groups, a declared `Str` column
//! `URL` (the needle in one row of a hundred) beside three other declared
//! columns, written with the default writer configuration (BLOOM over every
//! string column). Every object is above `plan_whole_object_bound()`, the
//! fetcher carries a read cache, and the scan runs at eight partitions, so the
//! whole-segment fast path rejects it for fewer segments than partitions and
//! every object is planned. The same statement at two partitions takes the
//! fast path and is the reference for the count and the chunks scanned.
//!
//! Every GET is recorded by key and range in issue order. The plan pass runs
//! behind a barrier before any partition drains, so the first GETs, as many
//! as the fetcher's per-phase counter charges to the plan phase, are the plan
//! phase's; their byte total is checked against that counter's.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect};
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::record::{COL_OBSERVED_TS, COL_STREAM_REF, COL_TS};
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
    BlockRangeFetcher, CacheFetchError, EngineConfig, LogSegmentFetcher, PhaseWireByteCounts,
    QueryPhase, SegmentFetcher,
};
use ravel_sql::{DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([43u8; 16]);

const OBJECTS: usize = 4;
/// Twice [`OBJECTS`]: the whole-segment fast path rejects the scan for fewer
/// segments than partitions, so the striped route plans every object.
const STRIPED_PARTITIONS: usize = 8;
/// Half [`OBJECTS`]: the reference run takes the whole-segment fast path.
const FAST_PARTITIONS: usize = 2;
const ROWS_PER_BLOCK: usize = 100;
const BLOCKS_PER_OBJECT: usize = 16;
const BLOCKS_PER_GROUP: usize = 4;
const ROWS_PER_OBJECT: usize = ROWS_PER_BLOCK * BLOCKS_PER_OBJECT;
/// One row in this many carries the needle in its `URL`.
const NEEDLE_EVERY: usize = 100;
/// Suffix probe length, sized to this fixture's object tail (see
/// `logs_contains_fetch_bytes.rs`).
const SUFFIX_LEN: u64 = 4096;
const COALESCE_GAP: u64 = 1024;
/// The four directories the plan decodes.
const DIRECTORIES: [u32; 4] = [
    kind::STREAM_DIR,
    kind::FIELD_DIR,
    kind::SKIP_IDX,
    kind::PAGE_DIR,
];

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

/// Row `i` of object `n`: the q21 record of `logs_contains_fetch_bytes.rs`.
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
        body: format!("hit {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn key(n: usize) -> String {
    format!("logs/contains-plan-{n}.rlog")
}

async fn write_object(store: &dyn ObjectStoreBackend, n: usize) -> (SegmentRef, Vec<u8>) {
    let records: Vec<LogRecord> = (0..ROWS_PER_OBJECT).map(|i| record(n, i)).collect();
    let mut w = RlogWriter::new(
        RlogConfig {
            block_target_records: ROWS_PER_BLOCK,
            group_target_blocks: BLOCKS_PER_GROUP,
            ..RlogConfig::default()
        },
        ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [8u8; 16],
            writer_epoch: 1,
            writer_seq: n as u64 + 1,
        },
    );
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
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
        object_size: bytes.len() as u64,
        min_event_ts_ns: records.iter().map(|r| r.ts_ns).min().expect("nonempty"),
        max_event_ts_ns: records.iter().map(|r| r.ts_ns).max().expect("nonempty"),
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        writer_id: Uuid::from_u128(8),
        writer_epoch: 1,
        writer_seq: n as u64 + 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(footer::VERSION),
        declared_column_stats: Default::default(),
    };
    (seg, bytes)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Get {
    key: String,
    start: u64,
    end: u64,
}

/// A pass-through store that records every GET in issue order.
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

/// One named byte region of an object.
#[derive(Clone, Debug)]
struct Region {
    start: u64,
    end: u64,
    label: String,
}

/// The layout of one object: every non-BLOCKS section, every BLOCKS column
/// chunk labelled `g<group>:<column name>` (fixed columns `fixed<id>`), and
/// the footer plus trailer.
struct Layout {
    regions: Vec<Region>,
    blocks: (u64, u64),
    /// Summed length of the four directory sections.
    directory_bytes: u64,
    /// Length of the footer and trailer.
    footer_bytes: u64,
    /// Every `URL` chunk, `(start, end)`.
    url_chunks: BTreeSet<(u64, u64)>,
}

fn layout(object: &[u8]) -> Layout {
    let cfg = RlogConfig::default();
    let f = footer::open(object).expect("footer");
    let section = |k: u32| {
        *f.sections
            .iter()
            .find(|s| s.kind == k)
            .unwrap_or_else(|| panic!("section {k}"))
    };
    let field_dir = FieldDir::decode(
        &read_section(object, &section(kind::FIELD_DIR), &cfg).expect("FIELD_DIR"),
        u64::MAX,
    )
    .expect("decode FIELD_DIR");
    let page_dir =
        PageDir::decode(&read_section(object, &section(kind::PAGE_DIR), &cfg).expect("PAGE_DIR"))
            .expect("decode PAGE_DIR");
    let blocks = section(kind::BLOCKS);
    let mut regions = Vec::new();
    for s in &f.sections {
        if s.kind != kind::BLOCKS {
            regions.push(Region {
                start: s.offset,
                end: s.offset + s.len,
                label: format!("section{}", s.kind),
            });
        }
    }
    let mut url_chunks = BTreeSet::new();
    for (g, group) in page_dir.groups.iter().enumerate() {
        for chunk in &group.chunks {
            let (offset, len) = chunk.extent().expect("chunk extent");
            let name = match chunk.column_id {
                COL_TS => "ts".to_string(),
                COL_OBSERVED_TS => "observed_ts".to_string(),
                COL_STREAM_REF => "stream_ref".to_string(),
                id => field_dir
                    .by_column_id(id)
                    .map(|e| e.name.clone())
                    .unwrap_or_else(|| format!("fixed{id}")),
            };
            let (start, end) = (blocks.offset + offset, blocks.offset + offset + len);
            if name == "URL" {
                url_chunks.insert((start, end));
            }
            regions.push(Region {
                start,
                end,
                label: format!("g{g}:{name}"),
            });
        }
    }
    let footer_start = f
        .sections
        .iter()
        .map(|s| s.offset + s.len)
        .max()
        .expect("sections");
    regions.push(Region {
        start: footer_start,
        end: object.len() as u64,
        label: "footer+trailer".to_string(),
    });
    regions.sort_by_key(|r| r.start);
    Layout {
        regions,
        blocks: (blocks.offset, blocks.offset + blocks.len),
        directory_bytes: DIRECTORIES.iter().map(|&k| section(k).len).sum(),
        footer_bytes: object.len() as u64 - footer_start,
        url_chunks,
    }
}

/// The labels of the regions `[start, end)` overlaps, `unmapped` for any byte
/// no region holds.
fn covered(regions: &[Region], start: u64, end: u64) -> Vec<String> {
    let mut out = Vec::new();
    let mut mapped = 0;
    for r in regions {
        let s = r.start.max(start);
        let e = r.end.min(end);
        if s < e {
            out.push(r.label.clone());
            mapped += e - s;
        }
    }
    if mapped < end - start {
        out.push("unmapped".to_string());
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

/// The fetcher over `fx`'s store: half the smallest object is the block-range
/// threshold, and so the whole-object bound. It carries a read cache, the
/// precondition for striping past the segment count (ADR-0102 decision 1), so
/// a GET is a cache miss and a repeated extent costs none.
fn log_fetcher(fx: &Fixture) -> LogSegmentFetcher {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&fx.store) as Arc<dyn ObjectStoreBackend>;
    let threshold = fx
        .snapshot
        .segments
        .iter()
        .map(|s| s.object_size)
        .min()
        .expect("segments")
        / 2;
    let cache_bytes = 64u64 << 20;
    let cache: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(CacheLimits::new(
        cache_bytes,
        (cache_bytes / 4096) as usize,
        cache_bytes,
    )));
    LogSegmentFetcher::new(Arc::clone(&backend))
        .with_block_range(
            BlockRangeFetcher::new(backend)
                .with_suffix_len(SUFFIX_LEN)
                .with_coalesce_gap(COALESCE_GAP),
        )
        .with_cache(cache)
        .with_block_range_threshold(threshold)
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
    rejected_fewer_segments: usize,
    plan_full_reads: usize,
    /// Every data-object GET, in issue order.
    gets: Vec<Get>,
    phases: PhaseWireByteCounts,
}

async fn run(fx: &Fixture, partitions: usize, sql: &str) -> Run {
    let fetcher = log_fetcher(fx);
    for seg in &fx.snapshot.segments {
        assert!(
            seg.object_size > fetcher.plan_whole_object_bound(),
            "{} is above the whole-object bound",
            seg.data_object_key
        );
    }
    let wire = fetcher.phase_wire_byte_counter();
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&fx.store) as Arc<dyn ObjectStoreBackend>;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&backend)),
        fetcher,
        SpanSegmentFetcher::new(Arc::clone(&backend)),
        SqlConfig {
            engine: EngineConfig {
                fetch_concurrency: partitions,
                sql_partition_count: Some(partitions),
                ..EngineConfig::default()
            },
            ..SqlConfig::default()
        },
        1 << 30,
    );
    let pinned = executor
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
    let before = wire.snapshot();
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    let phases = wire.snapshot().saturating_sub(&before);
    let gets: Vec<Get> = fx
        .store
        .take()
        .into_iter()
        .filter(|g| fx.objects.contains_key(&g.key))
        .collect();
    let scan = find_scan(&plan);
    assert_eq!(
        scan.properties().output_partitioning().partition_count(),
        partitions,
        "the scan runs at the requested partition count"
    );
    Run {
        count: count(&batches),
        rejected_fewer_segments: sum_metric(
            &scan,
            "fast_path_rejected_fewer_segments_than_partitions",
        ),
        plan_full_reads: sum_metric(&scan, "plan_full_reads"),
        gets,
        phases,
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

#[tokio::test]
async fn a_like_scan_with_more_partitions_than_segments_reads_no_block_bytes_in_plan() {
    let fx = fixture().await;
    let sql = "SELECT COUNT(*) FROM logs WHERE \"URL\" LIKE '%google%'";
    let layouts: BTreeMap<&String, Layout> =
        fx.objects.iter().map(|(k, o)| (k, layout(o))).collect();

    let striped = run(&fx, STRIPED_PARTITIONS, sql).await;
    let fast = run(&fx, FAST_PARTITIONS, sql).await;

    for (i, g) in striped.gets.iter().enumerate() {
        eprintln!(
            "{i:3} {} [{}, {}) {:?}",
            g.key,
            g.start,
            g.end,
            covered(&layouts[&g.key].regions, g.start, g.end)
        );
    }

    // The route: every partition refused the fast path for fewer segments
    // than partitions, and the reference run did not.
    assert_eq!(striped.rejected_fewer_segments, STRIPED_PARTITIONS);
    assert_eq!(fast.rejected_fewer_segments, 0);

    // No plan-phase read of the whole object, or of its blocks.
    assert_eq!(striped.plan_full_reads, 0, "plan_full_reads");

    // The plan phase's GETs come first; their bytes are the plan phase's.
    let plan_gets = striped.phases.phase_requests(QueryPhase::Plan) as usize;
    let (plan, scan) = striped.gets.split_at(plan_gets);
    assert_eq!(
        plan.iter().map(|g| g.end - g.start).sum::<u64>(),
        striped.phases.phase(QueryPhase::Plan),
        "the first GETs are the plan phase's"
    );

    // Every plan-phase GET lies outside BLOCKS. Per object the plan may read
    // the suffix probe, then any directory section or footer byte the probe
    // missed, so its bytes are bounded by SUFFIX_LEN + directory_bytes +
    // footer_bytes. On this fixture the probe holds all of them: one GET of
    // SUFFIX_LEN bytes per object.
    let mut plan_bytes: BTreeMap<&String, (usize, u64)> = BTreeMap::new();
    for g in plan {
        let l = &layouts[&g.key];
        assert!(
            g.end <= l.blocks.0 || l.blocks.1 <= g.start,
            "plan GET {g:?} reads BLOCKS: {:?}",
            covered(&l.regions, g.start, g.end)
        );
        let entry = plan_bytes.entry(&g.key).or_default();
        entry.0 += 1;
        entry.1 += g.end - g.start;
    }
    assert_eq!(plan_bytes.len(), OBJECTS, "every object is planned");
    for (k, &(gets, bytes)) in &plan_bytes {
        let l = &layouts[k];
        assert!(l.directory_bytes + l.footer_bytes <= SUFFIX_LEN, "{k}");
        let bound = SUFFIX_LEN + l.directory_bytes + l.footer_bytes;
        assert!(
            bytes <= bound,
            "{k}: plan read {bytes} bytes, bound {bound}"
        );
        assert_eq!((gets, bytes), (1, SUFFIX_LEN), "{k}: one suffix probe");
    }

    // The scan issues no probe of its own (the plan's footer and directories
    // are carried) and reads BLOCKS chunks of the projected `URL` column and
    // of the `ts`, `observed_ts` and `stream_ref` columns the fast-path run
    // reads too, and nothing else; it reads every `URL` chunk of every object.
    assert_eq!(striped.phases.phase_requests(QueryPhase::Probe), 0);
    assert_eq!(
        scan.iter().map(|g| g.end - g.start).sum::<u64>(),
        striped.phases.phase(QueryPhase::Scan),
        "the remaining GETs are the scan phase's"
    );
    let chunks_read = |gets: &[Get]| {
        let mut out: BTreeSet<(String, String)> = BTreeSet::new();
        for g in gets {
            let l = &layouts[&g.key];
            if g.end <= l.blocks.0 || l.blocks.1 <= g.start {
                continue;
            }
            for label in covered(&l.regions, g.start, g.end) {
                out.insert((g.key.clone(), label));
            }
        }
        out
    };
    let striped_chunks = chunks_read(scan);
    for (k, label) in &striped_chunks {
        let column = label.split_once(':').map_or("", |(_, c)| c);
        assert!(
            ["URL", "ts", "observed_ts", "stream_ref"].contains(&column),
            "{k}: the scan read {label}"
        );
    }
    assert_eq!(
        scan.iter()
            .filter(|g| {
                let l = &layouts[&g.key];
                g.end <= l.blocks.0 || l.blocks.1 <= g.start
            })
            .count(),
        0,
        "every scan GET is a BLOCKS read"
    );
    // The fast-path run's ranges also span other columns of one row group per
    // object, so the striped scan's chunks are a subset of its chunks.
    assert!(striped_chunks.is_subset(&chunks_read(&fast.gets)));
    let url_chunks = striped_chunks
        .iter()
        .filter(|(_, label)| label.ends_with(":URL"))
        .count();
    let all_url_chunks: usize = layouts.values().map(|l| l.url_chunks.len()).sum();
    assert_eq!(url_chunks, all_url_chunks);
    assert_eq!(
        all_url_chunks,
        OBJECTS * BLOCKS_PER_OBJECT / BLOCKS_PER_GROUP
    );

    // The same answer as the fast path on the same objects.
    let expected = (OBJECTS * ROWS_PER_OBJECT / NEEDLE_EVERY) as i64;
    assert_eq!(fast.count, expected);
    assert_eq!(striped.count, fast.count);
}
