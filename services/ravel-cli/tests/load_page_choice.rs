//! `ravel-cli load --page-choice` (issue #4212, ADR-4203 decision 7).
//!
//! Drives `ravel_cli::load::run`, the function `main.rs` dispatches `load` to,
//! in-process, because a subprocess against `--store memory` could not hand
//! its objects back to the test. The same Parquet file is loaded with each
//! page choice into two stores, and every page of every written object is
//! checked against the choice the load was given. The summary and report JSON
//! test runs the built binary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use ravel_cli::load::{
    self, DEFAULT_DECODE_QUEUE_BATCHES, LOAD_REPORT_JSON_PREFIX, LoadPageChoice,
    PER_BATCH_TARGET_BYTES,
};
use ravel_cli::maintain::SignalArg;
use ravel_ingest::{RlogChunkOrder, RlogGroupTargetBlocks, RlogZstdLevel};
use ravel_logseg::encoding::{Enc, decode_i64, decode_strings};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, COMP_ZSTD, kind};
use ravel_logseg::page::{SealedPage, seal_page, smallest_stored};
use ravel_logseg::page_dir::{PageDir, PageEntry};
use ravel_logseg::record::{
    COL_BODY, COL_FLAGS, COL_OBSERVED_TS, COL_SEVERITY_NUM, COL_SEVERITY_TEXT, COL_STREAM_REF,
    COL_TS,
};
use ravel_logseg::rlog_codec::{
    decode_dict_ids, decode_dict_page, decode_gcd_i64, i64_candidates, i64_estimated,
    string_candidates,
};
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::{FieldType, LogRecord, Predicate, RlogConfig, RlogReader, read_section};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};

const ROWS: usize = 20_000;

/// The writer's zstd level at the loader's default `--zstd-level`.
const ZSTD_LEVEL: i32 = 3;

const MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "code"
column = "code"
type = "i64"

[[attribute]]
key = "user"
column = "user"
type = "str"
"#;

/// A dense file the two choices write differently, which the reach test
/// asserts: `ts` has a constant stride and `user` alternates two values, so
/// several encodings of each page land within a few bytes of each other once
/// compressed.
fn write_fixture(path: &Path, base_ts: i64) {
    let pattern = [3i64, 9, 1, 14, 6, 11, 0, 15];
    let words = ["alpha-bravo-charlie-delta", "echo-foxtrot-golf-hotel-india"];
    let ts: Vec<i64> = (0..ROWS).map(|i| base_ts + i as i64 * 1_000).collect();
    let body: Vec<String> = (0..ROWS).map(|i| format!("request {}", i % 7)).collect();
    let svc: Vec<&str> = vec!["api"; ROWS];
    let code: Vec<i64> = (0..ROWS).map(|i| pattern[i % 8]).collect();
    let user: Vec<&str> = (0..ROWS).map(|i| words[i % 2]).collect();
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

/// The fixture and mapping in a temporary directory.
struct Input {
    _dir: tempfile::TempDir,
    parquet: std::path::PathBuf,
    mapping: std::path::PathBuf,
}

fn input(base_ts: i64) -> Input {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = dir.path().join("logs.parquet");
    let mapping = dir.path().join("mapping.toml");
    std::fs::write(&mapping, MAPPING).expect("write mapping");
    write_fixture(&parquet, base_ts);
    Input {
        _dir: dir,
        parquet,
        mapping,
    }
}

/// Pinned, not read from the wall clock, so both loads write the same keys.
const NOW_NS: i64 = 1_790_000_000_000_000_000;

/// Every RLOG object one load wrote, keyed by min ts so two loads' objects
/// pair up whatever their keys are.
async fn load_with(choice: LoadPageChoice, input: &Input) -> BTreeMap<i64, bytes::Bytes> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load::run(
        Arc::clone(&store),
        &input.parquet,
        "acme",
        &input.mapping,
        SignalArg::Logs,
        1,
        ROWS,
        0,
        None,
        Some(1),
        1,
        DEFAULT_DECODE_QUEUE_BATCHES,
        None,
        Some(PER_BATCH_TARGET_BYTES),
        None,
        None,
        RlogZstdLevel::DEFAULT,
        RlogGroupTargetBlocks::DEFAULT,
        RlogChunkOrder::DEFAULT,
        choice,
        None,
        false,
        &ravel_cli::ipcrypt::IpcryptPolicy::empty(),
        None,
        NOW_NS,
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
    objects
}

fn section(data: &[u8], k: u32) -> Vec<u8> {
    let f = footer::open(data).expect("footer");
    let desc = f.section(k).expect("section");
    read_section(data, desc, &RlogConfig::default()).expect("read section")
}

fn records(data: &[u8]) -> Vec<LogRecord> {
    let reader = RlogReader::new(data, &RlogConfig::default()).expect("open rlog");
    reader.scan(&Predicate::And(Vec::new())).expect("scan").0
}

/// What a column's values are, so its pages can be decoded and re-encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    I64,
    Str,
}

fn column_kinds(data: &[u8]) -> BTreeMap<u32, Kind> {
    let mut kinds: BTreeMap<u32, Kind> = [
        COL_TS,
        COL_OBSERVED_TS,
        COL_STREAM_REF,
        COL_SEVERITY_NUM,
        COL_FLAGS,
    ]
    .into_iter()
    .map(|c| (c, Kind::I64))
    .chain([COL_SEVERITY_TEXT, COL_BODY].map(|c| (c, Kind::Str)))
    .collect();
    let dir = FieldDir::decode(&section(data, kind::FIELD_DIR), u64::MAX).expect("FIELD_DIR");
    for e in dir.entries() {
        let k = match e.ty {
            FieldType::I64 => Kind::I64,
            FieldType::Str => Kind::Str,
            other => panic!("the fixture maps no {other:?} attribute"),
        };
        kinds.insert(e.column_id, k);
    }
    kinds
}

/// A page's codec bytes: its stored bytes, decompressed when it is zstd.
fn encoded(data: &[u8], offset: u64, p: &PageEntry) -> Vec<u8> {
    let stored = &data[offset as usize..(offset + p.len) as usize];
    if p.comp == COMP_ZSTD {
        zstd::bulk::decompress(stored, p.uncomp_len as usize).expect("decompress page")
    } else {
        stored.to_vec()
    }
}

/// The page `choice` seals for an i64 page of `values`.
fn i64_page(choice: LoadPageChoice, values: &[i64]) -> SealedPage {
    match choice {
        LoadPageChoice::Exhaustive => {
            smallest_stored(i64_candidates(values), ZSTD_LEVEL).expect("a candidate")
        }
        LoadPageChoice::Estimated => {
            let (enc, bytes) = i64_estimated(values);
            seal_page(enc, bytes, ZSTD_LEVEL)
        }
    }
}

/// The per-block page `choice` seals for a string page of `values`: the
/// smallest stored candidate, or the one the dictionary heuristic ranks first.
fn str_page(choice: LoadPageChoice, values: &[Vec<u8>]) -> SealedPage {
    let refs: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
    let candidates = string_candidates(&refs);
    match choice {
        LoadPageChoice::Exhaustive => smallest_stored(candidates, ZSTD_LEVEL).expect("a candidate"),
        LoadPageChoice::Estimated => {
            let (enc, bytes) = candidates.into_iter().next().expect("a candidate");
            seal_page(enc, bytes, ZSTD_LEVEL)
        }
    }
}

/// The bytes a page costs in the row-group dictionary decision: its stored
/// bytes plus its PAGE_DIR entry.
fn decision_cost(block: u32, page: &SealedPage) -> u64 {
    let entry = PageEntry {
        block,
        enc: page.enc,
        comp: page.comp,
        len: page.stored.len() as u64,
        uncomp_len: page.uncomp_len,
        crc32c: 0,
    };
    page.stored.len() as u64 + entry.encoded_len()
}

/// How many pages [`assert_pages_follow`] checked, by what it checked.
#[derive(Debug, Default)]
struct Checked {
    i64_pages: usize,
    str_pages: usize,
    dict_chunks: usize,
    column_refs: usize,
}

/// Asserts that every page of `data` is the one `choice` writes for the
/// values it holds:
///
/// - an i64 page, and a string page stored per block, is exactly the sealed
///   page the choice picks among the encodings the format defines for its
///   values (under `Exhaustive` the smallest stored candidate);
/// - a string chunk on a row-group dictionary took that form because it is
///   strictly smaller, page bytes plus PAGE_DIR entries, than the per-block
///   pages the choice would have written, and under `Estimated` every block's
///   heuristic picked a dictionary;
/// - an `observed_ts` page referencing `ts` is the reference, which no page
///   choice decides.
///
/// The fixture is dense, so a block's value page holds one value per record.
fn assert_pages_follow(choice: LoadPageChoice, data: &[u8]) -> Checked {
    let kinds = column_kinds(data);
    let skip = SkipIndex::decode(&section(data, kind::SKIP_IDX), u64::MAX).expect("SKIP_IDX");
    let blocks = footer::open(data)
        .expect("footer")
        .section(kind::BLOCKS)
        .expect("BLOCKS")
        .offset;
    let mut checked = Checked::default();
    for group in PageDir::decode(&section(data, kind::PAGE_DIR))
        .expect("PAGE_DIR")
        .groups
    {
        for chunk in &group.chunks {
            let at = format!("{choice:?}: column {}", chunk.column_id);
            let kind = kinds[&chunk.column_id];
            let offsets = chunk.page_offsets().expect("page offsets");
            let dict = chunk.dict_page().map(|p| {
                decode_dict_page(&encoded(data, blocks + offsets[0], p)).expect("dict page")
            });
            let (mut dict_form, mut per_block_form) = (0u64, 0u64);
            for (p, rel) in chunk.pages.iter().zip(&offsets) {
                if p.enc == Enc::DictPage {
                    dict_form += p.len + p.encoded_len();
                    continue;
                }
                assert_ne!(p.enc, Enc::Bitmap, "{at}: the fixture is dense");
                let block = group.first_block + p.block;
                let count = skip.l0[block as usize].record_count as usize;
                let body = encoded(data, blocks + rel, p);
                let stored = &data[(blocks + rel) as usize..(blocks + rel + p.len) as usize];
                let at = format!("{at}, block {block} ({:?})", p.enc);
                let expected = match (kind, p.enc) {
                    (Kind::I64, Enc::ColumnRef) => {
                        assert_eq!(chunk.column_id, COL_OBSERVED_TS, "{at}");
                        checked.column_refs += 1;
                        continue;
                    }
                    (Kind::I64, enc) => {
                        let values = if enc == Enc::GcdI64 {
                            decode_gcd_i64(&body, count).expect("gcd page")
                        } else {
                            decode_i64(enc, &body, count).expect("i64 page")
                        };
                        checked.i64_pages += 1;
                        i64_page(choice, &values)
                    }
                    (Kind::Str, Enc::DictIds) => {
                        let dict = dict.as_ref().expect("an id page has a dictionary");
                        let values: Vec<Vec<u8>> = decode_dict_ids(&body, count, dict.len())
                            .expect("id page")
                            .into_iter()
                            .map(|id| dict[id as usize].clone())
                            .collect();
                        let own = str_page(choice, &values);
                        if choice == LoadPageChoice::Estimated {
                            assert_eq!(own.enc, Enc::Dict, "{at}: the heuristic chose plain");
                        }
                        dict_form += p.len + p.encoded_len();
                        per_block_form += decision_cost(p.block, &own);
                        continue;
                    }
                    (Kind::Str, _) => {
                        checked.str_pages += 1;
                        str_page(choice, &decode_strings(p.enc, &body, count).expect("page"))
                    }
                };
                assert_eq!(
                    (p.enc, p.comp, stored),
                    (expected.enc, expected.comp, expected.stored.as_slice()),
                    "{at}: not the page {choice:?} writes"
                );
            }
            if dict.is_some() {
                assert!(
                    dict_form < per_block_form,
                    "{at}: a dictionary form of {dict_form} bytes against {per_block_form}"
                );
                checked.dict_chunks += 1;
            }
        }
    }
    checked
}

/// The flag reaches every page the load writes, on the path `ravel-cli load`
/// drives: each load's pages are the ones its choice writes for their values,
/// the two loads differ on this fixture, and both read back the same rows.
///
/// Wrong implementations this rules out, each shown failing: a flag that is
/// parsed but not handed to the writer (the loader keeps one fixed choice,
/// and the other load's pages fail `assert_pages_follow`); a choice that
/// reaches only the columnar path (see
/// `load::logs::tests::the_page_choice_reaches_the_row_path`, since the
/// binary drives the columnar path only).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_page_choice_flag_reaches_every_written_page() {
    let input = input(NOW_NS - 60_000_000_000);
    let exhaustive = load_with(LoadPageChoice::Exhaustive, &input).await;
    let estimated = load_with(LoadPageChoice::Estimated, &input).await;

    assert!(!exhaustive.is_empty());
    assert_eq!(
        exhaustive.keys().collect::<Vec<_>>(),
        estimated.keys().collect::<Vec<_>>(),
        "both loads lay the file out into the same objects"
    );
    let mut rows = 0usize;
    let mut differing_pages = 0usize;
    for (min_ts, ex) in &exhaustive {
        let es = &estimated[min_ts];
        let checked_ex = assert_pages_follow(LoadPageChoice::Exhaustive, ex);
        let checked_es = assert_pages_follow(LoadPageChoice::Estimated, es);
        for checked in [&checked_ex, &checked_es] {
            assert!(
                checked.i64_pages > 0 && checked.column_refs > 0,
                "{checked:?}"
            );
            assert!(
                checked.str_pages > 0 && checked.dict_chunks > 0,
                "{checked:?}"
            );
        }

        let recs = records(ex);
        assert_eq!(recs, records(es), "both tenants read back the same rows");
        rows += recs.len();

        let pages = |data: &[u8]| -> BTreeMap<(u32, u32), (Enc, u64)> {
            PageDir::decode(&section(data, kind::PAGE_DIR))
                .expect("PAGE_DIR")
                .groups
                .iter()
                .flat_map(|g| {
                    g.chunks.iter().flat_map(move |c| {
                        c.pages
                            .iter()
                            .map(move |p| ((c.column_id, g.first_block + p.block), (p.enc, p.len)))
                    })
                })
                .collect()
        };
        let (pex, pes) = (pages(ex), pages(es));
        differing_pages += pex
            .iter()
            .filter(|(at, page)| pes.get(at) != Some(page))
            .count()
            + pes.keys().filter(|at| !pex.contains_key(at)).count();
    }
    assert_eq!(rows, ROWS);
    assert!(
        differing_pages > 0,
        "the fixture must make the two choices write different pages"
    );
}

/// Runs the built `ravel-cli load` against its own `--store memory` with
/// `extra` flags and returns its stdout.
fn run_binary(input: &Input, extra: &[&str]) -> String {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("RAVEL_") {
            cmd.env_remove(key);
        }
    }
    let out = cmd
        .args([
            "--store",
            "memory",
            "--tenant-hash-unkeyed",
            "load",
            "--signal",
            "logs",
            "--parquet",
        ])
        .arg(&input.parquet)
        .args(["--tenant", "acme", "--mapping"])
        .arg(&input.mapping)
        .args(extra)
        .output()
        .expect("ravel-cli runs");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf8 stdout")
}

/// The built binary's summary names the page choice on exactly one line and
/// its report JSON in exactly one field, as estimated with no flag and as
/// exhaustive with `--page-choice exhaustive`.
#[test]
fn the_summary_and_report_name_the_page_choice_once() {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos() as i64;
    let input = input(now_ns - 60_000_000_000);
    for (extra, name) in [
        (&[][..], "estimated"),
        (&["--page-choice", "estimated"][..], "estimated"),
        (&["--page-choice", "exhaustive"][..], "exhaustive"),
    ] {
        let stdout = run_binary(&input, extra);
        let lines: Vec<&str> = stdout
            .lines()
            .filter(|l| l.contains("page choice"))
            .collect();
        assert_eq!(
            lines,
            vec![format!("  page choice      : {name}")],
            "{stdout}"
        );
        let json: Vec<&str> = stdout
            .lines()
            .filter_map(|l| l.strip_prefix(LOAD_REPORT_JSON_PREFIX))
            .collect();
        assert_eq!(json.len(), 1, "{stdout}");
        assert_eq!(json[0].matches("\"page_choice\"").count(), 1, "{stdout}");
        let report: serde_json::Value = serde_json::from_str(json[0]).expect("report json");
        assert_eq!(report["page_choice"], name, "{stdout}");
    }
}
