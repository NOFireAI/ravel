//! Golden-bytes regression for the RLOG v5 writer (ADR-2135,
//! docs/log-segment-format.md): the writer's output for a fixed representative
//! input must stay byte-for-byte identical across internal refactors of the v5
//! encode path. RLOG v5 is a frozen persistent contract; this test is the
//! tripwire for an accidental format change.
//!
//! Regenerate `golden_rlog_v5.bin` only after a deliberate format change
//! recorded in an ADR or an ADR amendment (never for an internal refactor); the
//! last regeneration is the ADR-0699 2026-10-24 amendment, which moved
//! STREAM_DIR and FIELD_DIR after PAGE_DIR with no trailer bump. Run:
//!   cargo test -p ravel-logseg --test golden_bytes_v5 -- --ignored --nocapture
//! `golden_rlog_v5_front_dirs.bin` is the same input in the order before that
//! amendment, kept so a reader keeps opening objects of either order.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_logseg::encoding::Enc;
use ravel_logseg::footer::{self, kind, open};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, ObjectIdentity, Predicate, RlogConfig, RlogReader,
    RlogWriter, read_section, stream_attrs_bytes,
};

#[path = "../benches/common/page_pool.rs"]
mod page_pool;

fn fixed_identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0xB2; 16],
        shard: 2,
        writer_id: [0x2B; 16],
        writer_epoch: 2,
        writer_seq: 20,
    }
}

/// The representative corpus the golden fixture is built from: two streams,
/// four records each, exercising fixed and dynamic columns of every type.
fn golden_records() -> Vec<LogRecord> {
    let streams: [(LogStreamId, &str); 2] = [
        (LogStreamId([0x11; 16]), "checkout"),
        (LogStreamId([0x22; 16]), "payments"),
    ];
    let mut out = Vec::new();
    for (stream_id, service) in streams {
        let stream_attrs = stream_attrs_bytes(
            &[(
                "service.name".to_string(),
                AttrValue::Str(service.to_string()),
            )],
            "scope",
            "1.0",
            &[],
        );
        for i in 0..4i64 {
            out.push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: 1_650_000_000_000_000_000 + i * 1000,
                observed_ts_ns: 1_650_000_000_000_000_000 + i * 1000 + 5,
                severity_num: 9 + (i as u8 % 3),
                severity_text: "INFO".to_string(),
                body: format!("{service} handled request {i}"),
                trace_id: Some([i as u8; 16]),
                span_id: Some([i as u8; 8]),
                flags: 1,
                attrs: vec![
                    ("http.status".to_string(), AttrValue::I64(200 + i)),
                    ("http.method".to_string(), AttrValue::Str("GET".to_string())),
                    ("cache.hit".to_string(), AttrValue::Bool(i % 2 == 0)),
                ]
                .into_iter()
                .chain(if i % 2 == 1 {
                    vec![
                        (
                            "http.status".to_string(),
                            AttrValue::Bytes(vec![0xAB, 0xCD]),
                        ),
                        ("cache.hit".to_string(), AttrValue::Bool(i % 2 != 0)),
                    ]
                } else {
                    Vec::new()
                })
                .collect(),
            });
        }
    }
    out
}

fn write_golden() -> Vec<u8> {
    let mut w = RlogWriter::new(RlogConfig::default(), fixed_identity());
    for rec in golden_records() {
        w.push(rec).expect("push record");
    }
    w.finish().expect("finish v5 golden")
}

#[test]
fn matches_golden_fixture() {
    let written = write_golden();
    let fixture: &[u8] = include_bytes!("fixtures/golden_rlog_v5.bin");
    assert_eq!(
        written.as_slice(),
        fixture,
        "RLOG v5 writer output diverged from the captured golden fixture; \
         RLOG v5 is frozen (docs/log-segment-format.md) -- this must not change \
         without an ADR or an ADR amendment recording the format change"
    );

    // The trailer carries the live footer::VERSION (never a mirrored literal),
    // and every mandatory section is present, PAGE_DIR now among them.
    let n = written.len();
    assert_eq!(
        u16::from_le_bytes([written[n - 8], written[n - 7]]),
        footer::VERSION,
        "trailer must carry the live footer::VERSION"
    );
    let decoded = open(&written).expect("open golden object");
    for k in [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::BLOCKS,
        kind::SKIP_IDX,
        kind::BLOOM,
        kind::PAGE_DIR,
    ] {
        assert!(
            decoded.section(k).is_some(),
            "mandatory section kind {k} present"
        );
    }

    // The whole object round-trips: every record is recoverable. An empty
    // `And` matches every record.
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(&written, &cfg).expect("reader");
    let (scanned, _stats) = reader.scan(&Predicate::And(vec![])).expect("scan");
    assert_eq!(scanned.len(), golden_records().len());

    // The fixture is one block, so it is one row group, and every column chunk
    // it lists holds exactly one page: the boundary case ADR-0699 decision 1
    // calls out as costing a small object nothing.
    let raw = read_section(
        &written,
        decoded.section(kind::PAGE_DIR).expect("page_dir"),
        &cfg,
    )
    .expect("read page_dir");
    let dir = PageDir::decode(&raw).expect("decode page_dir");
    assert_eq!(dir.groups.len(), 1, "one block is one row group");
    assert_eq!(dir.groups[0].first_block, 0);
    assert_eq!(dir.groups[0].block_count, 1);
    // One block per group means one or two block pages per column chunk: a
    // fully present column contributes its value page, a partially present one
    // its presence bitmap page as well. Both shapes occur in this fixture, which
    // is why a chunk's page_count is not the count of blocks carrying the
    // column. No chunk here takes a row-group dictionary (ADR-2135 decision
    // 6): every page is under the compression floor, and uncompressed a
    // one-block dictionary form holds the block's tag 7 bytes less its width
    // byte plus a second PAGE_DIR entry, so it cannot be smaller.
    assert!(
        dir.groups[0].chunks.iter().all(|c| c.pages.len() <= 2),
        "a one-block group's column chunks are a value page and at most a \
         presence page"
    );
    assert!(
        dir.groups[0]
            .chunks
            .iter()
            .any(|c| c.pages.iter().any(|p| p.enc == Enc::Bitmap)),
        "the fixture has a partially present column, so a presence page exists"
    );
    assert!(
        dir.groups[0].chunks.iter().all(|c| c.dict_page().is_none()),
        "the fixture's repeated `INFO` and `GET` strings stay on per-block pages"
    );
    assert_eq!(dir.block_count(), 1);
}

/// The golden input through the parallel page encode (ADR-3492 decision 7):
/// four threads, results returned out of job order, the same fixture bytes.
#[test]
fn parallel_page_encode_matches_golden_fixture() {
    let mut pool = page_pool::ScopedPool::new(4);
    let mut w = RlogWriter::new(RlogConfig::default(), fixed_identity());
    for rec in golden_records() {
        w.push(rec).expect("push record");
    }
    let written = w.finish_on(&mut pool).expect("finish_on");
    let fixture: &[u8] = include_bytes!("fixtures/golden_rlog_v5.bin");
    assert!(
        written.as_slice() == fixture,
        "parallel page encode diverged from the golden fixture"
    );
    assert_eq!(pool.split_calls, 1);
    assert_eq!(pool.out_of_order_calls, 1);
}

#[test]
fn write_is_deterministic_across_repeated_calls() {
    assert_eq!(
        write_golden(),
        write_golden(),
        "v5 output must be deterministic"
    );
}

/// An object written before the ADR-0699 2026-10-24 amendment (STREAM_DIR and
/// FIELD_DIR at offset 0, ahead of BLOCKS) and the same input written today
/// (both after PAGE_DIR) decode to the same records: readers locate sections by
/// footer offset, so the order is a writer convention and needs no trailer
/// bump.
#[test]
fn front_directory_objects_decode_to_the_same_records_as_tail_directory_objects() {
    let front: &[u8] = include_bytes!("fixtures/golden_rlog_v5_front_dirs.bin");
    let tail = write_golden();
    let cfg = RlogConfig::default();
    let scan_all = |bytes: &[u8]| {
        let reader = RlogReader::new(bytes, &cfg).expect("reader");
        reader.scan(&Predicate::And(vec![])).expect("scan").0
    };
    let from_front = scan_all(front);
    assert_eq!(from_front.len(), golden_records().len());
    assert_eq!(from_front, scan_all(&tail));

    let front_footer = open(front).expect("open front-directory object");
    let tail_footer = open(&tail).expect("open tail-directory object");
    let offset = |f: &footer::LogFooter, k: u32| f.section(k).expect("section present").offset;
    assert_eq!(offset(&front_footer, kind::STREAM_DIR), 0);
    assert!(
        offset(&tail_footer, kind::STREAM_DIR) > offset(&tail_footer, kind::PAGE_DIR),
        "the current writer puts STREAM_DIR after PAGE_DIR"
    );
}

#[test]
#[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
fn capture_golden_rlog_v5() {
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/golden_rlog_v5.bin"
        ),
        write_golden(),
    )
    .expect("write fixture");
}
