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
use ravel_cli::maintain::{ClaimOptions, SignalArg, compact_tenant};
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

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Loads one parquet row per timestamp in `ts`, each its own flush and its own
/// L0 object, with the loader's clock at `now_ns`.
async fn load_rows(store: &Arc<dyn ObjectStoreBackend>, dir: &Path, ts: &[i64], now_ns: i64) {
    let src = dir.join(format!("src-{now_ns}.parquet"));
    let bodies: Vec<String> = ts.iter().map(|t| format!("row {t}")).collect();
    let ts: ArrayRef = Arc::new(Int64Array::from(ts.to_vec()));
    let body: ArrayRef = Arc::new(StringArray::from(bodies));
    let batch = RecordBatch::try_from_iter(vec![("ts", ts), ("body", body)]).expect("batch");
    write_parquet(&src, &batch);
    let mapping =
        load::parse_mapping("ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n")
            .expect("mapping");
    load::load(
        Arc::clone(store),
        &src,
        "acme",
        &mapping,
        1,
        1,
        None,
        1,
        now_ns,
        Arc::new(FixedClock(now_ns)),
    )
    .await
    .expect("load");
}

/// Sorted data object keys under one level directory of the tenant's logs.
async fn level_keys(store: &dyn ObjectStoreBackend, level_dir: &str) -> Vec<String> {
    let prefix = format!(
        "t/{}/{}/{level_dir}/",
        TenantId::new("acme").hash().to_hex(),
        Signal::Logs.key_prefix()
    );
    let listed = list_all(store, &prefix).await.expect("list");
    let mut keys: Vec<String> = listed
        .iter()
        .map(|m| m.key.clone())
        .filter(|k| {
            ravel_commit::keys::parse_data_key(k).is_ok()
                || ravel_commit::keys::parse_l1_part_key(k).is_ok()
        })
        .collect();
    keys.sort();
    keys
}

/// `--tenant` resolves every live object: the L1 segment a compaction wrote
/// in place of its L0 inputs, and the L0 object written after it. Each object
/// costs exactly four GETs whose bytes are the trailer, the footer, FIELD_DIR
/// and PAGE_DIR: no page body is fetched.
#[tokio::test]
async fn tenant_footprint_reads_only_directories() {
    let dir = tempfile::tempdir().expect("tempdir");
    let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;

    // Three L0 objects in one hour, compacted into one L1 segment once sealed.
    load_rows(
        &store,
        dir.path(),
        &[BASE_NS, BASE_NS + 1, BASE_NS + 2],
        BASE_NS,
    )
    .await;
    let compacted = compact_tenant(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        SignalArg::Logs,
        Some(1),
        None,
        None,
        false,
        Some(0),
        None,
        None,
        None,
        1,
        BASE_NS + 2 * NS_PER_HOUR,
        &ClaimOptions::fresh(),
    )
    .await
    .expect("compact");
    assert_eq!(compacted.parts_written, 1);
    let l1 = level_keys(store.as_ref(), "l1").await;
    assert_eq!(l1.len(), 1);

    // One more L0 object, three hours later, which nothing compacts.
    let later = BASE_NS + 3 * NS_PER_HOUR;
    load_rows(&store, dir.path(), &[later], later).await;
    let l0 = level_keys(store.as_ref(), "l0").await;
    assert_eq!(
        l0.len(),
        4,
        "the three compacted inputs stay listed until GC"
    );

    let keys = rlog_footprint::tenant_object_keys(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        1,
        later,
    )
    .await
    .expect("resolve");
    assert_eq!(keys.len(), 2, "{keys:?}");
    assert!(keys.contains(&l1[0]), "{keys:?}");
    let live_l0: Vec<&String> = keys.iter().filter(|k| l0.contains(k)).collect();
    assert_eq!(live_l0.len(), 1, "{keys:?}");

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
    let report = rlog_footprint::footprint_keys(store.as_ref(), &keys)
        .await
        .expect("footprint");
    let after = metrics.snapshot().get;
    assert_eq!(after.calls - before.calls, 4 * keys.len() as u64);
    assert_eq!(after.bytes - before.bytes, expected_bytes);
    assert_eq!(report.total.object_count, 2);
    assert_eq!(report.total.record_count, 4);
    assert_eq!(report.total.total_bytes, sizes);
    let levels: BTreeMap<&str, (u32, u64)> = report
        .objects
        .iter()
        .map(|o| (o.key.as_str(), (o.level, o.footprint.record_count)))
        .collect();
    assert_eq!(
        levels[l1[0].as_str()],
        (1, 3),
        "the L1 segment holds all three inputs"
    );
    assert_eq!(levels[live_l0[0].as_str()], (0, 1));
}

/// A key resolved from the catalog is fetched from the store even when a local
/// file sits at the same path; only an explicit target reads local disk.
#[tokio::test]
async fn catalog_keys_are_read_from_the_store_not_local_disk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = object_a();
    let b = object_b();
    let path = dir.path().join("same.rlog");
    std::fs::write(&path, &b).expect("write b");
    let key = path.to_str().expect("utf-8 path").to_string();
    let store = MemoryStore::new();
    store
        .put(
            &key,
            bytes::Bytes::from(a.clone()),
            ravel_object_store::PutOptions::default(),
        )
        .await
        .expect("put a");

    let from_store = rlog_footprint::footprint_keys(&store, std::slice::from_ref(&key))
        .await
        .expect("keys");
    assert_eq!(from_store.total.total_bytes, a.len() as u64);
    assert_eq!(from_store.total.record_count, 12);

    let from_disk = rlog_footprint::footprint_targets(&store, std::slice::from_ref(&key))
        .await
        .expect("targets");
    assert_eq!(from_disk.total.total_bytes, b.len() as u64);
    assert_eq!(from_disk.total.record_count, 6);
}

/// The object's footer length, from its trailer.
fn footer_len(bytes: &[u8]) -> usize {
    u32::from_le_bytes(
        bytes[bytes.len() - 16..bytes.len() - 12]
            .try_into()
            .expect("4 bytes"),
    ) as usize
}

/// `bytes` with its section area kept, `appended` written after it, and a
/// footer rewritten by `edit`, which receives the offset `appended` starts at.
fn rewrite_footer(
    bytes: &[u8],
    appended: &[u8],
    edit: impl FnOnce(&mut footer::LogFooter, u64),
) -> Vec<u8> {
    let mut f = footer::open(bytes).expect("open");
    let footer_start = bytes.len() - 16 - footer_len(bytes);
    let mut out = bytes[..footer_start].to_vec();
    out.extend_from_slice(appended);
    edit(&mut f, footer_start as u64);
    footer::write_footer_and_trailer(&mut out, &f);
    footer::open(&out).expect("the rewritten object still opens");
    out
}

/// Index into `f.sections` of a section the footprint does not decode
/// (STREAM_DIR, SKIP_IDX or BLOOM) that starts past 0 and is directly followed
/// by another section, so growing or shifting it by one byte overlaps a
/// neighbour while staying inside the section area.
fn unread_inner_section(f: &footer::LogFooter) -> usize {
    f.sections
        .iter()
        .position(|s| {
            [kind::STREAM_DIR, kind::SKIP_IDX, kind::BLOOM].contains(&s.kind)
                && s.offset > 0
                && s.len > 0
                && f.sections.iter().any(|n| n.offset == s.offset + s.len)
        })
        .expect("an unread section with a successor")
}

async fn footprint_of(bytes: &[u8]) -> anyhow::Result<rlog_footprint::FootprintReport> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("o.rlog");
    std::fs::write(&path, bytes).expect("write");
    let target = path.to_str().expect("utf-8 path").to_string();
    rlog_footprint::footprint_targets(&MemoryStore::new(), &[target]).await
}

fn reconcile_error(err: &anyhow::Error) -> &rlog_footprint::ReconcileError {
    err.downcast_ref::<rlog_footprint::ReconcileError>()
        .unwrap_or_else(|| panic!("not a ReconcileError: {err:#}"))
}

/// A section grown one byte into its neighbour makes the section bytes sum to
/// one more than the object size, and is refused naming both figures.
#[tokio::test]
async fn footprint_refuses_a_section_grown_into_its_neighbour() {
    let a = object_a();
    let edited = rewrite_footer(&a, &[], |f, _| {
        let i = unread_inner_section(f);
        f.sections[i].len += 1;
    });
    let err = footprint_of(&edited).await.expect_err("must refuse");
    let total = edited.len() as u64;
    match reconcile_error(&err) {
        rlog_footprint::ReconcileError::Sections {
            accounted_bytes,
            object_bytes,
            ..
        } => {
            assert_eq!(*accounted_bytes, total + 1);
            assert_eq!(*object_bytes, total);
        }
        other => panic!("wrong variant: {other:?}"),
    }
    let text = err.to_string();
    assert!(
        text.contains(&format!("account for {} bytes", total + 1)),
        "{text}"
    );
    assert!(
        text.contains(&format!("the object is {total} bytes")),
        "{text}"
    );
    assert!(text.contains("o.rlog"), "{text}");

    // The binary prints no report and exits non-zero with the same error.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("grown.rlog");
    std::fs::write(&path, &edited).expect("write");
    let out = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["--store", "memory", "rlog", "footprint", "--json"])
        .arg(&path)
        .output()
        .expect("ravel-cli runs");
    assert!(!out.status.success());
    assert!(
        out.stdout.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("grown.rlog"), "{stderr}");
    assert!(
        stderr.contains(&format!(
            "account for {} bytes but the object is {total} bytes",
            total + 1
        )),
        "{stderr}"
    );
}

/// A section shifted one byte back overlaps its predecessor and leaves a
/// one-byte hole, so the section lengths alone still sum to the object size;
/// the overlap is counted once in the uncovered bytes and the object is
/// refused.
#[tokio::test]
async fn footprint_refuses_sections_that_overlap_at_equal_lengths() {
    let a = object_a();
    let edited = rewrite_footer(&a, &[], |f, _| {
        let i = unread_inner_section(f);
        f.sections[i].offset -= 1;
    });
    let f = footer::open(&edited).expect("open");
    let total = edited.len() as u64;
    let lens: u64 = f.sections.iter().map(|s| s.len).sum::<u64>() + footer_len(&edited) as u64 + 16;
    assert_eq!(lens, total, "the lengths alone reconcile");
    let err = footprint_of(&edited).await.expect_err("must refuse");
    match reconcile_error(&err) {
        rlog_footprint::ReconcileError::Sections {
            accounted_bytes,
            object_bytes,
            ..
        } => {
            assert_eq!(*accounted_bytes, total + 1);
            assert_eq!(*object_bytes, total);
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

/// A PAGE_DIR whose page lengths sum to one byte less than BLOCKS is refused
/// naming both figures. The replacement PAGE_DIR is appended after the section
/// area, so the section reconciliation still holds (the old copy counts as
/// uncovered bytes) and only the page check can fire.
#[tokio::test]
async fn footprint_refuses_pages_that_do_not_sum_to_blocks() {
    let a = object_a();
    let f = footer::open(&a).expect("open");
    let blocks_len = f.section(kind::BLOCKS).expect("BLOCKS").len;
    let raw = read_section(
        &a,
        f.section(kind::PAGE_DIR).expect("PAGE_DIR"),
        &RlogConfig::default(),
    )
    .expect("read PAGE_DIR");
    let mut dir = PageDir::decode(&raw).expect("decode");
    let page = dir
        .groups
        .iter_mut()
        .flat_map(|g| g.chunks.iter_mut())
        .flat_map(|c| c.pages.iter_mut())
        .find(|p| p.comp == footer::COMP_NONE && p.len > 1)
        .expect("an uncompressed page");
    page.len -= 1;
    page.uncomp_len -= 1;
    let encoded = dir.encode();
    let edited = rewrite_footer(&a, &encoded, |f, at| {
        let desc = f
            .sections
            .iter_mut()
            .find(|s| s.kind == kind::PAGE_DIR)
            .expect("PAGE_DIR");
        desc.offset = at;
        desc.len = encoded.len() as u64;
        desc.uncomp_len = encoded.len() as u64;
        desc.comp = footer::COMP_NONE;
        desc.crc32c = crc32c::crc32c(&encoded);
    });
    let err = footprint_of(&edited).await.expect_err("must refuse");
    match reconcile_error(&err) {
        rlog_footprint::ReconcileError::Pages {
            page_bytes,
            blocks_bytes,
            ..
        } => {
            assert_eq!(*page_bytes, blocks_len - 1);
            assert_eq!(*blocks_bytes, blocks_len);
        }
        other => panic!("wrong variant: {other:?}"),
    }
    let text = err.to_string();
    assert!(
        text.contains(&format!(
            "store {} bytes but BLOCKS is {blocks_len} bytes",
            blocks_len - 1
        )),
        "{text}"
    );
}

/// Page counts for a column absent from one block, fully present in one and
/// partly present in two: one value page per block carrying it, plus one
/// presence page per block where it is only partly present.
#[tokio::test]
async fn partially_present_column_pages_count_value_and_presence_pages() {
    let cfg = RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity(3));
    // Blocks of three rows: none, rows 3 and 4, all of 6..=8, row 9.
    let present = [3i64, 4, 6, 7, 8, 9];
    for i in 0..12i64 {
        let mut attrs = vec![("n".into(), AttrValue::I64(i))];
        if present.contains(&i) {
            attrs.push(("zone".into(), AttrValue::Str("z1".into())));
        }
        w.push(rec(3, 9_000 + i, format!("c{i}"), attrs))
            .expect("push");
    }
    let bytes = w.finish().expect("finish");
    assert_eq!(footer::open(&bytes).expect("open").block_count, 4);
    let report = footprint_of(&bytes).await.expect("footprint");
    let zone = &report.total.columns["zone:str"];
    // Three blocks carry it, two of those partly: 3 + 2.
    assert_eq!(zone.total.pages, 5);
    assert_eq!(zone.encodings["bitmap"].pages, 2);
    assert_eq!(report.total.columns["n:i64"].total.pages, 4);
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}
