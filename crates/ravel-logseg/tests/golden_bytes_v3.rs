//! Golden-bytes regression for RLOG v3 (ADR-0095,
//! docs/log-segment-format.md): the version-3 encode path's output for a fixed
//! representative input must stay byte-for-byte identical, and the frozen
//! fixture must keep decoding through the version-3 reader. RLOG v3 is a frozen
//! persistent contract; this test is the tripwire for an accidental change to
//! it.
//!
//! Since ADR-0699 the writer emits version 4, so the bytes here come from
//! `RlogWriter::finish_v3_for_tests` -- the version-3 producer retained for the
//! version-3 reader, which stays as the N-1 reader (ADR-0066 decision 1). The
//! fixture is never regenerated: version 3 is not written any more, so any
//! divergence from it is a regression in the N-1 path, never a deliberate
//! change. `tests/golden_bytes_v4.rs` pins what the writer actually emits.
//!
//! The fixture's records include one attribute name carried twice over two
//! types, so the pinned bytes cover v3's SKIP_IDX NumStat rule (a stat bounds
//! each row's cross-type merged-view winner, not its raw columnar occurrence)
//! and not just the section framing.
//!
//! Modelled on `ravel-segment/tests/golden_bytes_v6.rs`: one deterministic
//! fixture, byte-pinned to `fixtures/golden_rlog_v3.bin`, plus structural
//! assertions that the trailer carries the live `footer::VERSION`, the mandatory
//! sections are present, and the whole object decodes and round-trips its
//! records. The writer takes all identity, timestamps, and config as inputs
//! (no `SystemTime::now`, no randomness), so the output is deterministic and
//! golden-pinnable.
//!
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, kind, open};
use ravel_logseg::record::FieldType;
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, ObjectIdentity, Predicate, RlogConfig, RlogReader,
    RlogWriter, read_section, stream_attrs_bytes,
};

fn fixed_identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0xB2; 16],
        shard: 2,
        writer_id: [0x2B; 16],
        writer_epoch: 2,
        writer_seq: 20,
    }
}

/// Two streams, a fixed handful of records each, with deterministic bodies,
/// severities, and attributes exercising the string/i64/bool dynamic columns.
///
/// Odd-numbered records carry two duplicate keys on purpose, so the pinned
/// bytes cover v3's NumStat rule and not just the section framing. A record's
/// occurrences of one name resolve by ascending type byte, then overflow, last
/// wins: `http.status` also appears as `Bytes` (type byte 5, above i64's 2), so
/// its i64 stat contribution becomes a null; `cache.hit` appears as `Bool`
/// twice, so the duplicate spills into `attrs_raw` and *it*, not the value in
/// the bool column, is what the bool stat bounds.
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
    w.finish_v3_for_tests().expect("finish v3 golden")
}

#[test]
fn matches_golden_fixture() {
    let written = write_golden();
    let fixture: &[u8] = include_bytes!("fixtures/golden_rlog_v3.bin");
    assert_eq!(
        written.as_slice(),
        fixture,
        "RLOG v3 writer output diverged from the captured golden fixture; \
         RLOG v3 is frozen (docs/log-segment-format.md) -- this must never change \
         without a version bump and ADR"
    );

    // The trailer says 3, which is the window's floor, not its current
    // version; a version-3 object is exactly the N-1 case the window exists
    // for.
    let n = written.len();
    assert_eq!(
        u16::from_le_bytes([written[n - 8], written[n - 7]]),
        footer::SUPPORTED_VERSIONS.oldest(),
        "the version-3 fixture sits at the window floor"
    );
    assert!(
        footer::SUPPORTED_VERSIONS.contains(3),
        "version 3 must stay readable (ADR-0066 decision 1)"
    );
    let decoded = open(&written).expect("open golden object");
    for k in [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::BLOCKS,
        kind::SKIP_IDX,
        kind::BLOOM,
    ] {
        assert!(
            decoded.section(k).is_some(),
            "mandatory section kind {k} present"
        );
    }
    assert!(
        decoded.section(kind::PAGE_DIR).is_none(),
        "version 3 carries no PAGE_DIR"
    );

    // The whole object round-trips: every record is recoverable. An empty
    // `And` matches every record.
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(&written, &cfg).expect("reader");
    let (scanned, _stats) = reader.scan(&Predicate::And(vec![])).expect("scan");
    assert_eq!(scanned.len(), golden_records().len());

    // The pinned bytes really do carry v3 NumStat semantics, spelled out so a
    // regression reads as "the stat rule changed" rather than only as an opaque
    // byte diff. Half the records (the odd ones) give `http.status` a `Bytes`
    // occurrence that outranks their i64 one, and give `cache.hit` an overflow
    // `true` that outranks the `false` in its bool column.
    let field_raw = read_section(
        &written,
        decoded.section(kind::FIELD_DIR).expect("field_dir"),
        &cfg,
    )
    .expect("read field_dir");
    let fd = FieldDir::decode(&field_raw, 1 << 20).expect("decode field_dir");
    let skip_raw = read_section(
        &written,
        decoded.section(kind::SKIP_IDX).expect("skip_idx"),
        &cfg,
    )
    .expect("read skip");
    let skip = SkipIndex::decode(&skip_raw, 1 << 20).expect("decode skip");
    assert_eq!(skip.l0.len(), 1, "the fixture is one block");
    let stat = |name: &str, ty: FieldType| {
        let cid = fd.column(name, ty).expect("column present").column_id;
        *skip.l0[0]
            .stats
            .iter()
            .find(|s| s.column_id == cid)
            .expect("stat present")
    };
    let status = stat("http.status", FieldType::I64);
    assert_eq!(
        status.null_count, 4,
        "the four records whose http.status winner is Bytes contribute nulls"
    );
    let hit = stat("cache.hit", FieldType::Bool);
    assert_eq!(
        (hit.min_bits, hit.max_bits, hit.null_count),
        (1, 1, 0),
        "every row's winning cache.hit is true, including the four whose bool \
         column holds the losing false"
    );
}

#[test]
fn write_is_deterministic_across_repeated_calls() {
    assert_eq!(
        write_golden(),
        write_golden(),
        "v3 output must be deterministic"
    );
}
