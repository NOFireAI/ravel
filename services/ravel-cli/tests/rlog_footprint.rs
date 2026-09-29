//! Acceptance coverage for `ravel-cli rlog footprint` (issue #2137).
//!
//! The object-list form runs as a subprocess against the built binary, so the
//! test goes through the real clap entry point and the printed JSON. The
//! `--tenant` form needs a store shared with the loader, which a subprocess
//! against `--store memory` cannot have, so it drives the same library
//! functions `main.rs` calls in-process instead.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use ravel_cli::load;
use ravel_cli::rlog_footprint;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_ingest::Clock;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter, read_section,
};
use ravel_object_store::instrument::InstrumentedStore;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::{Signal, TenantId};
use serde_json::Value;

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

fn rec(stream: u8, ts: i64, body: String, attrs: Vec<(String, AttrValue)>) -> LogRecord {
    LogRecord {
        stream_id: sid(stream),
        stream_attrs: ravel_logseg::stream_attrs_bytes(
            &[("service.name".into(), AttrValue::Str(format!("s{stream}")))],
            "scope",
            "1",
            &[],
        ),
        ts_ns: ts,
        observed_ts_ns: ts + 5,
        severity_num: 9,
        severity_text: "INFO".into(),
        body,
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// Object A: 12 records, 4 per block (3 blocks), 2 blocks per row group (2
/// row groups). Every record carries `svc` (two values alternating, so each
/// block's page is dictionary-encoded) and `code` (i64). Bodies are long and
/// repetitive, so each body page crosses the compression floor and is stored
/// zstd-compressed, smaller than its uncompressed size.
fn object_a() -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: 4,
        group_target_blocks: 2,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity(1));
    for i in 0..12i64 {
        let svc = if i % 2 == 0 { "api" } else { "auth" };
        let body = format!("request {i} served ").repeat(20);
        w.push(rec(
            1,
            1_000 + i,
            body,
            vec![
                ("svc".into(), AttrValue::Str(svc.into())),
                ("code".into(), AttrValue::I64(200 + i * 37)),
            ],
        ))
        .expect("push");
    }
    w.finish().expect("finish")
}

/// Object B: 6 records, 3 per block (2 blocks, one row group), and a column set
/// disjoint from A's dynamic columns. `region` is on two of each block's three
/// rows, so each block carries a presence bitmap page and a value page for it.
fn object_b() -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity(2));
    for i in 0..6i64 {
        let mut attrs = vec![("latency_ms".into(), AttrValue::I64(10 + i))];
        if i % 3 != 2 {
            attrs.push(("region".into(), AttrValue::Str("eu-west".into())));
        }
        w.push(rec(2, 5_000 + i, format!("b{i}"), attrs))
            .expect("push");
    }
    w.finish().expect("finish")
}

fn identity(seq: u64) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0x11u8; 16],
        shard: 0,
        writer_id: [0x22u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    }
}

/// Per-column-id `(pages, stored, uncompressed)` read the independent way: the
/// whole object, the reader's own section decode, every page of every group.
fn oracle_pages(bytes: &[u8]) -> BTreeMap<u32, (u64, u64, u64)> {
    let f = footer::open(bytes).expect("open");
    let raw = read_section(
        bytes,
        f.section(kind::PAGE_DIR).expect("PAGE_DIR"),
        &RlogConfig::default(),
    )
    .expect("read PAGE_DIR");
    let dir = PageDir::decode(&raw).expect("decode PAGE_DIR");
    let mut out: BTreeMap<u32, (u64, u64, u64)> = BTreeMap::new();
    for g in &dir.groups {
        for c in &g.chunks {
            let e = out.entry(c.column_id).or_default();
            for p in &c.pages {
                e.0 += 1;
                e.1 += p.len;
                e.2 += p.uncomp_len;
            }
        }
    }
    out
}

fn u(v: &Value, path: &[&str]) -> u64 {
    let mut cur = v;
    for p in path {
        cur = cur
            .get(*p)
            .unwrap_or_else(|| panic!("missing {p} in {path:?}"));
    }
    cur.as_u64()
        .unwrap_or_else(|| panic!("{path:?} is not a u64"))
}

fn sections_sum(fp: &Value) -> u64 {
    fp["sections"]
        .as_object()
        .expect("sections")
        .values()
        .map(|s| s["bytes"].as_u64().expect("bytes"))
        .sum()
}

fn column_stored_sum(fp: &Value) -> u64 {
    fp["columns"]
        .as_object()
        .expect("columns")
        .values()
        .map(|c| c["stored_bytes"].as_u64().expect("stored_bytes"))
        .sum()
}

/// Asserts the footprint reconciliation for one object's JSON entry against
/// the object bytes it was measured from.
fn assert_object_reconciles(entry: &Value, bytes: &[u8], names: &BTreeMap<u32, String>) {
    let f = footer::open(bytes).expect("open");
    assert_eq!(u(entry, &["total_bytes"]), bytes.len() as u64);
    assert_eq!(u(entry, &["record_count"]), f.record_count);
    assert_eq!(u(entry, &["gap_bytes"]), 0);
    assert_eq!(
        sections_sum(entry),
        bytes.len() as u64,
        "section bytes plus footer and trailer must equal the object size"
    );
    let blocks_len = f.section(kind::BLOCKS).expect("BLOCKS").len;
    assert_eq!(u(entry, &["sections", "BLOCKS", "bytes"]), blocks_len);
    assert_eq!(
        column_stored_sum(entry),
        blocks_len,
        "per-column stored page bytes must equal the BLOCKS section length"
    );
    assert_eq!(u(entry, &["sections", "TRAILER", "bytes"]), 16);
    let footer_len = u64::from(u32::from_le_bytes(
        bytes[bytes.len() - 16..bytes.len() - 12]
            .try_into()
            .expect("4 bytes"),
    ));
    assert_eq!(u(entry, &["sections", "FOOTER", "bytes"]), footer_len);
    let trailer_version = u16::from_le_bytes([bytes[bytes.len() - 8], bytes[bytes.len() - 7]]);
    assert_eq!(u(entry, &["trailer_version"]), u64::from(trailer_version));

    let oracle = oracle_pages(bytes);
    assert_eq!(
        entry["columns"].as_object().expect("columns").len(),
        oracle.len(),
        "one column entry per column that has pages"
    );
    for (id, (pages, stored, uncomp)) in &oracle {
        let key = names.get(id).unwrap_or_else(|| panic!("no name for {id}"));
        let col = &entry["columns"][key.as_str()];
        assert_eq!(u(col, &["pages"]), *pages, "{key} pages");
        assert_eq!(u(col, &["stored_bytes"]), *stored, "{key} stored");
        assert_eq!(
            u(col, &["uncompressed_bytes"]),
            *uncomp,
            "{key} uncompressed"
        );
        let enc_pages: u64 = col["encodings"]
            .as_object()
            .expect("encodings")
            .values()
            .map(|p| p["pages"].as_u64().expect("pages"))
            .sum();
        assert_eq!(enc_pages, *pages, "{key} encodings partition its pages");
    }
}

#[test]
fn footprint_sums_to_object_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = object_a();
    let b = object_b();
    let pa = dir.path().join("a.rlog");
    let pb = dir.path().join("b.rlog");
    std::fs::write(&pa, &a).expect("write a");
    std::fs::write(&pb, &b).expect("write b");

    let out = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["--store", "memory", "rlog", "footprint", "--json"])
        .arg(&pa)
        .arg(&pb)
        .output()
        .expect("ravel-cli runs");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: Value = serde_json::from_slice(&out.stdout).expect("stdout is one JSON document");

    let fixed: BTreeMap<u32, String> = [
        (0, "ts:fixed"),
        (1, "observed_ts:fixed"),
        (2, "stream_ref:fixed"),
        (3, "severity_num:fixed"),
        (4, "severity_text:fixed"),
        (5, "body:fixed"),
        (8, "flags:fixed"),
    ]
    .into_iter()
    .map(|(id, name)| (id, name.to_string()))
    .collect();
    let field_names = |bytes: &[u8]| {
        let f = footer::open(bytes).expect("open");
        let raw = read_section(
            bytes,
            f.section(kind::FIELD_DIR).expect("FIELD_DIR"),
            &RlogConfig::default(),
        )
        .expect("FIELD_DIR");
        let fd = ravel_logseg::field_dir::FieldDir::decode(&raw, 1 << 20).expect("decode");
        let mut names = fixed.clone();
        for e in fd.entries() {
            let ty = match e.ty {
                ravel_logseg::FieldType::Str => "str",
                ravel_logseg::FieldType::I64 => "i64",
                _ => "other",
            };
            names.insert(e.column_id, format!("{}:{ty}", e.name));
        }
        names
    };

    let objects = report["objects"].as_array().expect("objects");
    assert_eq!(objects.len(), 2);
    assert_object_reconciles(&objects[0], &a, &field_names(&a));
    assert_object_reconciles(&objects[1], &b, &field_names(&b));

    // Exact page counts: one page per block for an always-present column, two
    // (presence bitmap then values) for `region`, which misses a row per block.
    let oa = &objects[0]["columns"];
    let ob = &objects[1]["columns"];
    assert_eq!(u(oa, &["ts:fixed", "pages"]), 3);
    assert_eq!(u(oa, &["body:fixed", "pages"]), 3);
    assert_eq!(u(oa, &["svc:str", "pages"]), 3);
    assert_eq!(u(oa, &["svc:str", "encodings", "dictionary", "pages"]), 3);
    assert_eq!(u(oa, &["code:i64", "pages"]), 3);
    assert_eq!(u(ob, &["ts:fixed", "pages"]), 2);
    assert_eq!(u(ob, &["latency_ms:i64", "pages"]), 2);
    assert_eq!(u(ob, &["region:str", "pages"]), 4);
    assert_eq!(u(ob, &["region:str", "encodings", "bitmap", "pages"]), 2);
    assert!(oa.get("region:str").is_none() && ob.get("svc:str").is_none());

    // The body pages are zstd-compressed, so stored and uncompressed differ and
    // a report that swapped them fails the per-column equalities above.
    let body_stored = u(oa, &["body:fixed", "stored_bytes"]);
    let body_uncomp = u(oa, &["body:fixed", "uncompressed_bytes"]);
    assert!(
        body_stored * 2 < body_uncomp,
        "body pages must compress for this test to separate the two figures: \
         stored {body_stored}, uncompressed {body_uncomp}"
    );

    let total = &report["total"];
    assert_eq!(u(total, &["object_count"]), 2);
    assert_eq!(u(total, &["record_count"]), 18);
    assert_eq!(u(total, &["total_bytes"]), (a.len() + b.len()) as u64);
    assert_eq!(sections_sum(total), (a.len() + b.len()) as u64);
    let blocks_total = footer::open(&a).unwrap().section(kind::BLOCKS).unwrap().len
        + footer::open(&b).unwrap().section(kind::BLOCKS).unwrap().len;
    assert_eq!(column_stored_sum(total), blocks_total);
    assert_eq!(u(total, &["columns", "ts:fixed", "pages"]), 5);
    assert_eq!(u(total, &["sections", "TRAILER", "count"]), 2);
}

#[test]
fn text_output_reports_the_same_totals() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = object_a();
    let pa = dir.path().join("a.rlog");
    std::fs::write(&pa, &a).expect("write a");
    let out = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["--store", "memory", "rlog", "footprint"])
        .arg(&pa)
        .output()
        .expect("ravel-cli runs");
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).expect("utf-8");
    let blocks = footer::open(&a).unwrap().section(kind::BLOCKS).unwrap().len;
    assert!(
        text.starts_with("object_count: 1\nrecord_count: 12\n"),
        "{text}"
    );
    assert!(
        text.contains(&format!("total_bytes: {}\n", a.len())),
        "{text}"
    );
    assert!(
        text.contains(&format!("page_stored_bytes: {blocks}\n")),
        "{text}"
    );
    assert!(text.contains("  ts type=fixed pages=3 "), "{text}");
}

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// `--tenant` resolves every object the loader wrote, and each object costs
/// exactly four GETs whose bytes are the trailer, the footer, FIELD_DIR and
/// PAGE_DIR: no page body is fetched.
#[tokio::test]
async fn tenant_footprint_reads_only_directories() {
    let dir = tempfile::tempdir().expect("tempdir");
    let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;

    let src = dir.path().join("src.parquet");
    let ts: ArrayRef = Arc::new(Int64Array::from(vec![BASE_NS, BASE_NS + 1, BASE_NS + 2]));
    let body: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "z"]));
    let batch = RecordBatch::try_from_iter(vec![("ts", ts), ("body", body)]).expect("batch");
    write_parquet(&src, &batch);
    let mapping =
        load::parse_mapping("ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n")
            .expect("mapping");
    // One row per batch: every row is its own flush and its own object.
    load::load(
        Arc::clone(&store),
        &src,
        "acme",
        &mapping,
        1,
        1,
        None,
        1,
        BASE_NS,
        Arc::new(FixedClock(BASE_NS)),
    )
    .await
    .expect("load");

    let prefix = format!(
        "t/{}/{}/l0/",
        TenantId::new("acme").hash().to_hex(),
        Signal::Logs.key_prefix()
    );
    let listed = list_all(store.as_ref(), &prefix).await.expect("list");
    let mut data_keys: Vec<String> = listed
        .iter()
        .map(|m| m.key.clone())
        .filter(|k| ravel_commit::keys::parse_data_key(k).is_ok())
        .collect();
    data_keys.sort();
    assert_eq!(data_keys.len(), 3);

    let keys = rlog_footprint::tenant_object_keys(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        1,
        BASE_NS,
    )
    .await
    .expect("resolve");
    assert_eq!(keys, data_keys);

    let mut expected_bytes = 0u64;
    let mut sizes = 0u64;
    for k in &keys {
        let bytes = store
            .get(k, ravel_object_store::GetRange::Full)
            .await
            .expect("get")
            .data;
        let f = footer::open(&bytes).expect("open");
        let footer_len = u64::from(u32::from_le_bytes(
            bytes[bytes.len() - 16..bytes.len() - 12]
                .try_into()
                .expect("4 bytes"),
        ));
        expected_bytes += 16
            + footer_len
            + f.section(kind::FIELD_DIR).unwrap().len
            + f.section(kind::PAGE_DIR).unwrap().len;
        sizes += bytes.len() as u64;
    }

    let before = metrics.snapshot().get;
    let report = rlog_footprint::footprint_targets(store.as_ref(), &keys)
        .await
        .expect("footprint");
    let after = metrics.snapshot().get;
    assert_eq!(after.calls - before.calls, 4 * keys.len() as u64);
    assert_eq!(after.bytes - before.bytes, expected_bytes);
    assert_eq!(report.total.object_count, 3);
    assert_eq!(report.total.record_count, 3);
    assert_eq!(report.total.total_bytes, sizes);
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}
