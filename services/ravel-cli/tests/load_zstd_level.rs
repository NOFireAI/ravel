//! `ravel-cli load --zstd-level` (ADR-2135 decision 4, issue #2145).
//!
//! Drives `ravel_cli::load::run`, the function `main.rs` dispatches `load` to,
//! in-process: a subprocess against `--store memory` could not hand its
//! objects back to the test. The same Parquet file is loaded at level 3 and at
//! level 19 into two stores, and the written objects are compared page by
//! page through their PAGE_DIR.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use ravel_cli::load::{self, DEFAULT_DECODE_QUEUE_BATCHES, DEFAULT_TARGET_BYTES};
use ravel_cli::maintain::SignalArg;
use ravel_ingest::RlogZstdLevel;
use ravel_logseg::footer::{self, COMP_ZSTD, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{LogRecord, Predicate, RlogConfig, RlogReader, read_section};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};

const ROWS: usize = 20_000;

const MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "http.status_code"
column = "code"
type = "i64"

[[attribute]]
key = "user"
column = "user"
type = "str"
"#;

const WORDS: [&str; 24] = [
    "request", "served", "cache", "miss", "upstream", "timeout", "retry", "ok", "user", "login",
    "session", "expired", "token", "refresh", "payment", "declined", "order", "shipped", "query",
    "slow", "index", "rebuilt", "disk", "full",
];

/// A deterministic mixed-entropy log file: bodies are word sequences drawn by
/// an LCG, so zstd has repetition to find at every level and more of it to
/// find at a higher one.
fn write_fixture(path: &Path, base_ts: i64) {
    let mut state: u64 = 0x2145;
    let mut next = || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) as usize
    };
    let mut ts = Vec::with_capacity(ROWS);
    let mut body = Vec::with_capacity(ROWS);
    let mut svc = Vec::with_capacity(ROWS);
    let mut code = Vec::with_capacity(ROWS);
    let mut user = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        ts.push(base_ts + (i as i64) * 1_000_000 + (next() % 1000) as i64);
        let n = 4 + next() % 8;
        let words: Vec<&str> = (0..n).map(|_| WORDS[next() % WORDS.len()]).collect();
        body.push(format!("{} id={}", words.join(" "), next() % 100_000));
        svc.push(format!("service-{}", next() % 400));
        code.push([200i64, 200, 200, 404, 500][next() % 5]);
        user.push(format!("user-{}", next() % 500));
    }
    let batch = RecordBatch::try_from_iter(vec![
        ("ts", Arc::new(Int64Array::from(ts)) as ArrayRef),
        ("body", Arc::new(StringArray::from(body)) as ArrayRef),
        ("svc", Arc::new(StringArray::from(svc)) as ArrayRef),
        ("code", Arc::new(Int64Array::from(code)) as ArrayRef),
        ("user", Arc::new(StringArray::from(user)) as ArrayRef),
    ])
    .expect("batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

/// One loaded RLOG object: its stored bytes, keyed by min ts so the two loads'
/// objects pair up whatever their keys are.
struct Loaded {
    objects: BTreeMap<i64, bytes::Bytes>,
}

async fn load_at(level: i32, parquet: &Path, mapping: &Path, now_ns: i64) -> Loaded {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load::run(
        Arc::clone(&store),
        parquet,
        "acme",
        mapping,
        SignalArg::Logs,
        1,
        ROWS,
        0,
        None,
        1,
        1,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        RlogZstdLevel::new(level).expect("in range"),
        None,
        false,
        now_ns,
    )
    .await
    .expect("load succeeds");
    let mut objects = BTreeMap::new();
    for meta in list_all(store.as_ref(), "").await.expect("list") {
        let data = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get")
            .data;
        if let Ok(f) = footer::open(&data) {
            assert!(
                objects.insert(f.min_ts_ns, data).is_none(),
                "two objects share a min ts"
            );
        }
    }
    Loaded { objects }
}

fn records(data: &[u8]) -> Vec<LogRecord> {
    let reader = RlogReader::new(data, &RlogConfig::default()).expect("open rlog");
    reader.scan(&Predicate::And(Vec::new())).expect("scan").0
}

/// One page's compression and where its stored bytes sit in the object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PageInfo {
    comp: u8,
    len: u64,
    uncomp_len: u64,
    /// Absolute offset of the page's stored bytes into the object.
    offset: u64,
}

/// Every page of an object, keyed by (group, column id, block), with its
/// compression, stored length, and absolute location.
fn pages(data: &[u8]) -> BTreeMap<(usize, u32, u64), PageInfo> {
    let f = footer::open(data).expect("footer");
    let blocks = f.section(kind::BLOCKS).expect("BLOCKS");
    let desc = f.section(kind::PAGE_DIR).expect("PAGE_DIR");
    let raw = read_section(data, desc, &RlogConfig::default()).expect("read PAGE_DIR");
    let dir = PageDir::decode(&raw).expect("decode PAGE_DIR");
    let mut out = BTreeMap::new();
    for (g, group) in dir.groups.iter().enumerate() {
        for chunk in &group.chunks {
            let offsets = chunk.page_offsets().expect("page offsets");
            for (page, rel_offset) in chunk.pages.iter().zip(offsets) {
                out.insert(
                    (g, chunk.column_id, u64::from(page.block)),
                    PageInfo {
                        comp: page.comp,
                        len: page.len,
                        uncomp_len: page.uncomp_len,
                        offset: blocks.offset + rel_offset,
                    },
                );
            }
        }
    }
    out
}

/// Every zstd-compressed whole-read section's descriptor, by kind.
fn zstd_sections(data: &[u8]) -> BTreeMap<u32, footer::SectionDesc> {
    footer::open(data)
        .expect("footer")
        .sections
        .iter()
        .filter(|s| s.comp == COMP_ZSTD && s.kind != kind::BLOCKS)
        .map(|s| (s.kind, *s))
        .collect()
}

/// Proves `stored` is exactly what compressing its own content at `level`
/// produces: decompresses `stored` to its recorded uncompressed length, then
/// recompresses at `level` with `zstd::bulk::compress`, which is deterministic
/// for the same input and level, and requires an exact match. This is the
/// claim a stored-size inequality cannot pin, because zstd does not guarantee
/// a higher level stores fewer bytes on small inputs.
fn assert_level_applied(unit: &str, level: i32, stored: &[u8], uncomp_len: u64) {
    let raw = zstd::bulk::decompress(stored, uncomp_len as usize)
        .unwrap_or_else(|e| panic!("{unit}: decompress at level {level}: {e}"));
    assert_eq!(
        raw.len() as u64,
        uncomp_len,
        "{unit}: decompressed length at level {level}"
    );
    let recompressed = zstd::bulk::compress(&raw, level)
        .unwrap_or_else(|e| panic!("{unit}: recompress at level {level}: {e}"));
    assert_eq!(
        recompressed, stored,
        "{unit}: recompressing its content at level {level} does not reproduce the stored bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_zstd_level_reaches_every_written_page() {
    assert_eq!(
        zstd::compression_level_range(),
        RlogZstdLevel::MIN..=RlogZstdLevel::MAX,
        "the accepted range is the linked libzstd's own"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = dir.path().join("logs.parquet");
    let mapping = dir.path().join("mapping.toml");
    std::fs::write(&mapping, MAPPING).expect("write mapping");
    // Pinned, not read from the wall clock: time is injected (load::run takes
    // now_ns as a parameter), so pinning it makes the written corpus bytes
    // identical on every run instead of varying with SystemTime::now().
    let now_ns: i64 = 1_790_000_000_000_000_000;
    // Starts a minute back so every row sits in the past of the load's clock.
    write_fixture(&parquet, now_ns - 60_000_000_000);

    let at3 = load_at(3, &parquet, &mapping, now_ns).await;
    let at19 = load_at(19, &parquet, &mapping, now_ns).await;

    assert!(!at3.objects.is_empty());
    assert_eq!(
        at3.objects.keys().collect::<Vec<_>>(),
        at19.objects.keys().collect::<Vec<_>>(),
        "both loads lay the file out into the same objects"
    );

    let mut rows3 = 0usize;
    let (mut total3, mut total19) = (0u64, 0u64);
    let (mut zpages3, mut zpages19) = (0u64, 0u64);
    let (mut zpage_count3, mut zpage_count19) = (0usize, 0usize);
    let (mut section_count3, mut section_count19) = (0usize, 0usize);
    for (min_ts, obj3) in &at3.objects {
        let obj19 = &at19.objects[min_ts];
        total3 += obj3.len() as u64;
        total19 += obj19.len() as u64;

        let recs3 = records(obj3);
        assert_eq!(recs3, records(obj19), "both loads read back identically");
        rows3 += recs3.len();

        let pages3 = pages(obj3);
        let pages19 = pages(obj19);
        assert_eq!(
            pages3.keys().collect::<Vec<_>>(),
            pages19.keys().collect::<Vec<_>>(),
            "both loads write the same pages"
        );
        for (at, p19) in &pages19 {
            let p3 = &pages3[at];
            if p19.comp == COMP_ZSTD {
                let stored19 = &obj19[p19.offset as usize..(p19.offset + p19.len) as usize];
                assert_level_applied(
                    &format!("page {at:?} (level 19 load)"),
                    19,
                    stored19,
                    p19.uncomp_len,
                );
                zpages19 += p19.len;
                zpage_count19 += 1;
            }
            if p3.comp == COMP_ZSTD {
                let stored3 = &obj3[p3.offset as usize..(p3.offset + p3.len) as usize];
                assert_level_applied(
                    &format!("page {at:?} (level 3 load)"),
                    3,
                    stored3,
                    p3.uncomp_len,
                );
                zpages3 += p3.len;
                zpage_count3 += 1;
            }
        }

        let sections3 = zstd_sections(obj3);
        let sections19 = zstd_sections(obj19);
        assert_eq!(
            sections3.keys().collect::<Vec<_>>(),
            sections19.keys().collect::<Vec<_>>(),
            "the same sections compress at both levels"
        );
        assert_eq!(
            sections3.keys().copied().collect::<Vec<_>>(),
            [
                kind::STREAM_DIR,
                kind::FIELD_DIR,
                kind::SKIP_IDX,
                kind::PAGE_DIR
            ],
        );
        for (k, desc19) in &sections19 {
            let desc3 = &sections3[k];
            let stored19 = &obj19[desc19.offset as usize..(desc19.offset + desc19.len) as usize];
            assert_level_applied(
                &format!("section kind {k} (level 19 load)"),
                19,
                stored19,
                desc19.uncomp_len,
            );
            section_count19 += 1;
            let stored3 = &obj3[desc3.offset as usize..(desc3.offset + desc3.len) as usize];
            assert_level_applied(
                &format!("section kind {k} (level 3 load)"),
                3,
                stored3,
                desc3.uncomp_len,
            );
            section_count3 += 1;
        }
        // Measured 8309 against 9083 bytes (91.5%).
        let (s3, s19): (u64, u64) = (
            sections3.values().map(|d| d.len).sum(),
            sections19.values().map(|d| d.len).sum(),
        );
        assert!(
            s19 * 100 <= s3 * 95,
            "level 19 sections are at least 5% smaller: {s19} vs {s3}"
        );
    }
    assert_eq!(at3.objects.len(), 1, "one shard, one batch, one object");
    assert_eq!(rows3, ROWS, "every row landed");
    // 15 before row-group dictionaries (#2144) replaced some per-block string
    // pages on this corpus with a dictionary page and id pages.
    assert_eq!(zpage_count19, 13, "zstd pages checked, level 19 load");
    assert_eq!(zpage_count3, 13, "zstd pages checked, level 3 load");
    assert_eq!(section_count19, 4, "zstd sections checked, level 19 load");
    assert_eq!(section_count3, 4, "zstd sections checked, level 3 load");
    // A level that reached only the sections leaves every page the same size.
    // Measured 319657 against 396157 bytes (80.7%).
    assert!(
        zpages19 * 100 <= zpages3 * 90,
        "level 19 pages are at least 10% smaller: {zpages19} vs {zpages3}"
    );
    // Measured 392761 against 470035 bytes (83.6%).
    assert!(
        total19 * 100 <= total3 * 90,
        "level 19 objects are at least 10% smaller: {total19} vs {total3}"
    );
}
