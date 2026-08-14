//! Golden-bytes regression for the RLOG v2 writer (ADR-0032,
//! docs/log-segment-format.md): the writer's output for a fixed representative
//! input must stay byte-for-byte identical across internal refactors of the v2
//! encode path. RLOG v2 is a frozen persistent contract; this test is the
//! tripwire for an accidental format change.
//!
//! Modelled on `ravel-segment/tests/golden_bytes_v6.rs`: one deterministic
//! fixture, byte-pinned to `fixtures/golden_rlog_v2.bin`, plus structural
//! assertions that the trailer carries the live `footer::VERSION`, the mandatory
//! sections are present, and the whole object decodes and round-trips its
//! records. The writer takes all identity, timestamps, and config as inputs
//! (no `SystemTime::now`, no randomness), so the output is deterministic and
//! golden-pinnable.
//!
//! To regenerate `golden_rlog_v2.bin` after a deliberate, versioned format
//! change (never for an internal refactor), run:
//!   cargo test -p ravel-logseg --test golden_bytes_v2 -- --ignored --nocapture
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_logseg::footer::{self, kind, open};
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, ObjectIdentity, Predicate, RlogConfig, RlogReader,
    RlogWriter, stream_attrs_bytes,
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
                ],
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
    w.finish().expect("finish v2 golden")
}

#[test]
fn matches_golden_fixture() {
    let written = write_golden();
    let fixture: &[u8] = include_bytes!("fixtures/golden_rlog_v2.bin");
    assert_eq!(
        written.as_slice(),
        fixture,
        "RLOG v2 writer output diverged from the captured golden fixture; \
         RLOG v2 is frozen (docs/log-segment-format.md) -- this must never change \
         without a version bump and ADR"
    );

    // The trailer carries the live footer::VERSION (never a mirrored literal),
    // and every mandatory v1 section is present.
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
}

#[test]
fn write_is_deterministic_across_repeated_calls() {
    assert_eq!(
        write_golden(),
        write_golden(),
        "v2 output must be deterministic"
    );
}

#[test]
#[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
fn capture_golden_rlog_v2() {
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/golden_rlog_v2.bin"
        ),
        write_golden(),
    )
    .expect("write fixture");
}
