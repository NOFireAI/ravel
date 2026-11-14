//! Issue #4205 (ADR-4203 decision 1): a logs query whose content list holds
//! only `Contains` arms plans from the footer and the four directories, and
//! the survivor list that plan names equals the reader's own full prune.
//!
//! Two object shapes are written: the default writer configuration (BLOOM
//! over every string column, POSTINGS on `Title`) and bloom scope `Text`
//! (BLOOM over `body` and `severity_text` only). Each holds twelve blocks of
//! twenty records with disjoint per-block `ts` and `RegionID` ranges, so a
//! time window or a `NumRange` arm excludes a known set of blocks.
//!
//! For every case [`LogSegmentFetcher::plan_segment`] runs on a fetcher that
//! routes the object above `plan_whole_object_bound()`, and its survivor list
//! is compared, block index for block index, with two references: the
//! reader's `scan_blocks` over the whole object, and `plan_segment` on a
//! fetcher whose bound is above the object (the fallback, which prunes over
//! the fetched buffer). A `Contains`-only case must also take the directory
//! branch (a carried footer) and issue no GET that overlaps BLOCKS; a case
//! with any arm an index can prune must keep the fallback.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::footer::{self, kind};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, BloomScope, ColumnSelection, FieldSel, FieldType, LogRecord, Predicate, RlogConfig,
    RlogReader, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{BlockRangeFetcher, LogQuery, LogSegmentFetcher, StreamAttrEquals};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([23u8; 16]);
const BLOCKS: usize = 12;
const ROWS_PER_BLOCK: usize = 20;
const ROWS: usize = BLOCKS * ROWS_PER_BLOCK;
/// The `URL` of exactly one record, in block [`EQUALS_BLOCK`].
const EQUALS_URL: &str = "https://only.example.net/x";
const EQUALS_BLOCK: usize = 5;
/// The only block whose bodies carry the word `alpha`.
const WORD_BLOCK: usize = 3;

fn identity(seq: u64) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    }
}

/// Record `i` of an object whose first `ts` is `base`. Block `b` holds rows
/// `20b..20b+20`, `ts` `base+20b..=base+20b+19`, `RegionID` `10b..=10b+4`
/// and `Title` `title-<b>`.
fn record(base: i64, i: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("web".to_string()),
    )];
    let block = i / ROWS_PER_BLOCK;
    let ts = base + i as i64;
    let url = if block == EQUALS_BLOCK && i % ROWS_PER_BLOCK == 3 {
        EQUALS_URL.to_string()
    } else if i % 7 == 2 {
        format!("https://www.google.com/search?q={i}")
    } else {
        format!("https://www.example.org/page/{i}")
    };
    let body = if block == WORD_BLOCK {
        format!("alpha request {i}")
    } else {
        format!("request {i} ok")
    };
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body,
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("URL".to_string(), AttrValue::Str(url)),
            (
                "Title".to_string(),
                AttrValue::Str(format!("title-{block}")),
            ),
            (
                "RegionID".to_string(),
                AttrValue::I64((block * 10 + i % 5) as i64),
            ),
        ],
    }
}

fn config() -> RlogConfig {
    RlogConfig {
        block_target_records: ROWS_PER_BLOCK,
        group_target_blocks: 4,
        ..RlogConfig::default()
    }
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    /// BLOOM over every string column, POSTINGS on `Title`.
    BloomAll,
    /// BLOOM over `body` and `severity_text` only.
    BloomText,
}

struct Object {
    shape: Shape,
    seg: SegmentRef,
    bytes: Vec<u8>,
    base: i64,
}

impl Object {
    fn blocks_section(&self) -> (u64, u64) {
        let f = footer::open(&self.bytes).expect("footer");
        let b = f.section(kind::BLOCKS).expect("BLOCKS");
        (b.offset, b.offset + b.len)
    }
}

async fn write_object(store: &dyn ObjectStoreBackend, n: u64, shape: Shape) -> Object {
    let base = 1_000_000 * (n as i64 + 1);
    let writer = RlogWriter::new(config(), identity(n + 1));
    let mut writer = match shape {
        Shape::BloomAll => writer.with_indexed_fields(vec!["Title".to_string()]),
        Shape::BloomText => writer.with_bloom_scope(BloomScope::Text),
    };
    for i in 0..ROWS {
        writer.push(record(base, i)).expect("push");
    }
    let bytes = writer.finish().expect("finish");
    let key = format!("logs/contains-plan-{n}.rlog");
    store
        .put(
            &key,
            bytes::Bytes::from(bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put");
    let seg = SegmentRef {
        data_object_key: key,
        object_size: bytes.len() as u64,
        min_event_ts_ns: base,
        max_event_ts_ns: base + ROWS as i64 - 1,
        ingest_hour_bucket: 0,
        sample_count: ROWS as u64,
        series_count: 0,
        shard: 0,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        writer_id: Uuid::from_u128(4),
        writer_epoch: 1,
        writer_seq: n + 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(footer::VERSION),
        declared_column_stats: Default::default(),
    };
    Object {
        shape,
        seg,
        bytes,
        base,
    }
}

/// One GET the store served, in object coordinates.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Get {
    key: String,
    range: GetRange,
    start: u64,
    end: u64,
}

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
            range,
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

/// An un-cached fetcher whose whole-object bound is `bound`: the suffix probe
/// is `suffix` bytes, coalescing and the coverage crossover are off, so every
/// read above the bound is a real byte-range GET.
fn fetcher(store: Arc<dyn ObjectStoreBackend>, suffix: u64, bound: u64) -> LogSegmentFetcher {
    LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range(
            BlockRangeFetcher::new(store)
                .with_suffix_len(suffix)
                .with_coverage_threshold(2.0)
                .with_coalesce_gap(0),
        )
        .with_block_range_threshold(bound)
}

fn contains(key: &str, needle: &str, negated: bool) -> Predicate {
    Predicate::Contains {
        key: key.to_string(),
        needle: needle.to_string(),
        negated,
    }
}

fn google() -> Predicate {
    contains("URL", "google", false)
}

/// Prune-only `RegionID` in `[min, max]`.
fn region(min: i64, max: i64) -> Predicate {
    Predicate::NumRange {
        field: FieldSel::Attr("RegionID".to_string()),
        ty: FieldType::I64,
        min: Some(min as u64),
        max: Some(max as u64),
    }
}

/// What a case must plan to, beyond agreeing with the references.
#[derive(Clone, Copy, Debug)]
enum Expect {
    /// The directory branch: a carried footer, no BLOCKS byte read, and this
    /// many survivors.
    Directories(usize),
    /// The fallback (no carried footer), with fewer survivors than blocks on
    /// the shapes listed, so a plan that kept every block disagrees.
    Fallback { prunes_on: &'static [&'static str] },
}

struct Case {
    name: &'static str,
    /// `ts` window relative to the object's first `ts`; `None` is the whole
    /// object.
    window: Option<(i64, i64)>,
    content: Vec<Predicate>,
    prune: Vec<Predicate>,
    stream_attrs: Vec<StreamAttrEquals>,
    expect: Expect,
}

fn shape_name(shape: Shape) -> &'static str {
    match shape {
        Shape::BloomAll => "bloom-all",
        Shape::BloomText => "bloom-text",
    }
}

fn cases() -> Vec<Case> {
    let case = |name, window, content, prune, expect| Case {
        name,
        window,
        content,
        prune,
        stream_attrs: Vec::new(),
        expect,
    };
    vec![
        // Contains-only: planned from the directories.
        case(
            "contains, whole window",
            None,
            vec![google()],
            vec![],
            Expect::Directories(BLOCKS),
        ),
        // Rows 40..=99: blocks 2, 3 and 4.
        case(
            "contains, window of blocks 2-4",
            Some((40, 99)),
            vec![google()],
            vec![],
            Expect::Directories(3),
        ),
        // Rows 45..=105: blocks 2 to 5, each partly.
        case(
            "contains, partial window of blocks 2-5",
            Some((45, 105)),
            vec![google()],
            vec![],
            Expect::Directories(4),
        ),
        // RegionID 30..=64: blocks 3, 4, 5 and 6.
        case(
            "contains, NumRange of blocks 3-6",
            None,
            vec![google()],
            vec![region(30, 64)],
            Expect::Directories(4),
        ),
        // Window of blocks 2-5 and RegionID of blocks 3-6: blocks 3, 4, 5.
        case(
            "contains, window and NumRange",
            Some((45, 105)),
            vec![google()],
            vec![region(30, 64)],
            Expect::Directories(3),
        ),
        case(
            "contains, NumRange outside every block",
            None,
            vec![google()],
            vec![region(1_000, 2_000)],
            Expect::Directories(0),
        ),
        case(
            "not contains",
            None,
            vec![contains("URL", "google", true)],
            vec![],
            Expect::Directories(BLOCKS),
        ),
        case(
            "two contains arms",
            Some((40, 99)),
            vec![google(), contains("Title", "title", false)],
            vec![],
            Expect::Directories(3),
        ),
        case(
            "contains, needle in no row",
            None,
            vec![contains("URL", "zzzzzz", false)],
            vec![],
            Expect::Directories(BLOCKS),
        ),
        case(
            "contains, key in no row",
            None,
            vec![contains("Nope", "google", false)],
            vec![],
            Expect::Directories(BLOCKS),
        ),
        // Any arm an index can prune keeps the fallback.
        case(
            "contains and a bloom Equals",
            None,
            vec![
                google(),
                Predicate::Equals {
                    field: FieldSel::Attr("URL".to_string()),
                    value: AttrValue::Str(EQUALS_URL.to_string()),
                },
            ],
            vec![],
            Expect::Fallback {
                prunes_on: &["bloom-all"],
            },
        ),
        case(
            "contains and a body HasWord",
            None,
            vec![
                google(),
                Predicate::HasWord {
                    field: FieldSel::Body,
                    word: "alpha".to_string(),
                },
            ],
            vec![],
            Expect::Fallback {
                prunes_on: &["bloom-all", "bloom-text"],
            },
        ),
        case(
            "contains and a POSTINGS prune Equals",
            None,
            vec![google()],
            vec![Predicate::Equals {
                field: FieldSel::Attr("Title".to_string()),
                value: AttrValue::Str("title-2".to_string()),
            }],
            Expect::Fallback {
                prunes_on: &["bloom-all"],
            },
        ),
        Case {
            name: "contains and a stream-attribute arm",
            window: None,
            content: vec![google()],
            prune: vec![],
            stream_attrs: vec![StreamAttrEquals::new(
                "service.name",
                AttrValue::Str("api".to_string()),
            )],
            expect: Expect::Fallback {
                prunes_on: &["bloom-all", "bloom-text"],
            },
        },
    ]
}

fn query(obj: &Object, case: &Case) -> LogQuery {
    let (lo, hi) = case.window.unwrap_or((0, ROWS as i64 - 1));
    let mut q = LogQuery::new(obj.base + lo, obj.base + hi);
    for p in &case.content {
        q = q.with_content(p.clone());
    }
    for p in &case.prune {
        q = q.with_prune(p.clone());
    }
    for s in &case.stream_attrs {
        q = q.with_stream_attr(s.clone());
    }
    q
}

/// The reader's own survivor list over the whole object, for a case with no
/// stream-attribute arm (those resolve through STREAM_DIR in the fetcher).
fn reader_survivors(obj: &Object, q: &LogQuery) -> Vec<usize> {
    let reader = RlogReader::new(&obj.bytes, &RlogConfig::default()).expect("open");
    let mut arms = vec![Predicate::TsRange {
        min_ns: q.ts_min_ns,
        max_ns: q.ts_max_ns,
    }];
    arms.extend(q.content.iter().cloned());
    reader
        .scan_blocks(&Predicate::And(arms), &q.prune, &ColumnSelection::all())
        .expect("scan_blocks")
        .survivor_block_indices()
}

#[tokio::test]
async fn contains_only_plan_matches_the_readers_prune_block_for_block() {
    let inner = Arc::new(MemoryStore::new());
    let objects = vec![
        write_object(inner.as_ref(), 0, Shape::BloomAll).await,
        write_object(inner.as_ref(), 1, Shape::BloomText).await,
    ];
    let store = Arc::new(RecordingStore {
        inner,
        gets: Mutex::new(Vec::new()),
    });
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;

    // A probe of the shortest tail after BLOCKS reaches no block byte on
    // either object.
    let suffix = objects
        .iter()
        .map(|o| o.bytes.len() as u64 - o.blocks_section().1)
        .min()
        .expect("objects");
    let smallest = objects
        .iter()
        .map(|o| o.seg.object_size)
        .min()
        .expect("objects");
    let ranged = fetcher(Arc::clone(&backend), suffix, smallest / 2);
    assert!(
        objects
            .iter()
            .all(|o| o.seg.object_size > ranged.plan_whole_object_bound()),
        "every object is above the ranged fetcher's bound"
    );
    let whole = fetcher(Arc::clone(&backend), suffix, u64::MAX);

    let mut checked = 0;
    for obj in &objects {
        let shape = shape_name(obj.shape);
        let (blocks_start, blocks_end) = obj.blocks_section();
        for case in cases() {
            let label = format!("{shape}: {}", case.name);
            let q = query(obj, &case);

            store.take();
            let plan = ranged
                .plan_segment(&obj.seg, TENANT, &q, &QueryAccounting::new())
                .await
                .expect("ranged plan")
                .expect("relevant segment");
            let plan_gets = store.take();
            let (survivors, _, stats, carried_footer, carried_object) = plan;

            let fallback = whole
                .plan_segment(&obj.seg, TENANT, &q, &QueryAccounting::new())
                .await
                .expect("whole plan")
                .expect("relevant segment");
            store.take();
            assert!(
                fallback.3.is_none(),
                "{label}: the reference took the fallback"
            );
            assert_eq!(
                survivors, fallback.0,
                "{label}: survivors against the fallback"
            );
            if case.stream_attrs.is_empty() {
                assert_eq!(
                    survivors,
                    reader_survivors(obj, &q),
                    "{label}: survivors against the reader"
                );
            }
            assert_eq!(stats.blocks_total as usize, BLOCKS, "{label}");

            let block_gets: Vec<&Get> = plan_gets
                .iter()
                .filter(|g| g.start < blocks_end && blocks_start < g.end)
                .collect();
            match case.expect {
                Expect::Directories(n) => {
                    assert!(
                        carried_footer.is_some(),
                        "{label}: the plan takes the directory branch"
                    );
                    assert!(carried_object.is_none(), "{label}");
                    assert_eq!(
                        block_gets,
                        Vec::<&Get>::new(),
                        "{label}: the plan read BLOCKS bytes"
                    );
                    assert_eq!(survivors.len(), n, "{label}: survivor count");
                }
                Expect::Fallback { prunes_on } => {
                    assert!(
                        carried_footer.is_none(),
                        "{label}: an index-prunable arm keeps the fallback"
                    );
                    if prunes_on.contains(&shape) {
                        assert!(
                            survivors.len() < BLOCKS,
                            "{label}: the arm prunes a block ({survivors:?})"
                        );
                    }
                }
            }
            checked += 1;
        }
    }
    assert_eq!(checked, 2 * cases().len());
    assert_eq!(checked, 28);
}

/// At or under `plan_whole_object_bound()` a `Contains`-only plan reads the
/// object whole, once, and carries those bytes to the scan, rather than a
/// footer probe the scan would follow with a second read under another key.
#[tokio::test]
async fn contains_only_plan_at_the_bound_reads_the_object_once() {
    let inner = Arc::new(MemoryStore::new());
    let obj = write_object(inner.as_ref(), 0, Shape::BloomAll).await;
    let store = Arc::new(RecordingStore {
        inner,
        gets: Mutex::new(Vec::new()),
    });
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let suffix = obj.bytes.len() as u64 - obj.blocks_section().1;
    let at_bound = fetcher(backend, suffix, obj.seg.object_size);
    assert_eq!(at_bound.plan_whole_object_bound(), obj.seg.object_size);

    let q = LogQuery::new(obj.base + 40, obj.base + 99).with_content(google());
    assert!(q.plan_contains_only_decidable());
    let (survivors, _, _, carried_footer, carried_object) = at_bound
        .plan_segment(&obj.seg, TENANT, &q, &QueryAccounting::new())
        .await
        .expect("plan")
        .expect("relevant segment");
    let gets = store.take();
    assert_eq!(
        gets,
        vec![Get {
            key: obj.seg.data_object_key.clone(),
            range: GetRange::Full,
            start: 0,
            end: obj.seg.object_size,
        }],
        "one whole-object GET"
    );
    assert!(carried_footer.is_none(), "the fallback ran");
    assert!(
        carried_object.is_some(),
        "the whole object is carried to the scan"
    );
    assert_eq!(survivors, reader_survivors(&obj, &q));
    assert_eq!(survivors, vec![2, 3, 4]);
}

/// The predicate accepts exactly a non-empty all-`Contains` content list with
/// no stream-attribute arm and only skip-step prune arms.
#[test]
fn plan_contains_only_decidable_accepts_exactly_its_shape() {
    let base = LogQuery::new(0, 10);
    assert!(
        !base.clone().plan_contains_only_decidable(),
        "empty content"
    );
    assert!(
        base.clone()
            .with_content(google())
            .plan_contains_only_decidable()
    );
    assert!(
        base.clone()
            .with_content(google())
            .with_prune(region(1, 2))
            .with_prune(Predicate::NumNotEq {
                field: FieldSel::Attr("RegionID".to_string()),
                value: 3,
            })
            .plan_contains_only_decidable()
    );
    assert!(
        !base
            .clone()
            .with_content(google())
            .with_content(Predicate::HasWord {
                field: FieldSel::Body,
                word: "alpha".to_string(),
            })
            .plan_contains_only_decidable(),
        "a mixed content list"
    );
    assert!(
        !base
            .clone()
            .with_content(google())
            .with_prune(Predicate::Equals {
                field: FieldSel::Attr("Title".to_string()),
                value: AttrValue::Str("t".to_string()),
            })
            .plan_contains_only_decidable(),
        "a POSTINGS prune arm"
    );
    assert!(
        !base
            .clone()
            .with_content(google())
            .with_stream_attr(StreamAttrEquals::new(
                "service.name",
                AttrValue::Str("web".to_string()),
            ))
            .plan_contains_only_decidable(),
        "a stream-attribute arm"
    );
}
