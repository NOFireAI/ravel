//! Golden-bytes regression for the RSPAN v3 writer (ADR-0054,
//! docs/span-segment-format.md): the writer's output for a fixed representative
//! input must stay byte-for-byte identical across internal refactors of the v3
//! encode path. RSPAN v3 is a frozen persistent contract; this test is the
//! tripwire for an accidental format change.
//!
//! Modelled on `ravel-segment/tests/golden_bytes_v6.rs`: one deterministic
//! fixture, byte-pinned to `fixtures/golden_rspan_v3.bin`, plus structural
//! assertions that the trailer carries the live `footer::VERSION`, the mandatory
//! v3 sections are present (BLOCKS, SKIP_IDX, and the v3-added BLOOM), and the
//! whole object decodes and round-trips its records. The writer takes all
//! identity, timestamps, and config as inputs (no `SystemTime::now`, no
//! randomness), so the output is deterministic and golden-pinnable.
//!
//! To regenerate `golden_rspan_v3.bin` after a deliberate, versioned format
//! change (never for an internal refactor), run:
//!   cargo test -p ravel-rspan --test golden_bytes_v3 -- --ignored --nocapture
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_rspan::footer::{self, kind};
use ravel_rspan::{
    ObjectIdentity, RspanConfig, RspanReader, RspanWriter, SpanQuery, SpanRecord, StatusCode, open,
};

fn fixed_identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0xB3; 16],
        shard: 3,
        writer_id: [0x3B; 16],
        writer_epoch: 3,
        writer_seq: 30,
    }
}

/// A fixed handful of spans across several services and names, one span per
/// block so each block's BLOOM and service_name column has known content.
fn golden_records() -> Vec<SpanRecord> {
    let rows = [
        ("checkout", "GET /cart", StatusCode::Ok),
        ("payments", "charge card", StatusCode::Error),
        ("checkout", "POST /order", StatusCode::Ok),
        ("inventory", "list items", StatusCode::Unset),
    ];
    rows.iter()
        .enumerate()
        .map(|(i, (service, name, status))| SpanRecord {
            trace_id: [i as u8; 16],
            span_id: [i as u8; 8],
            parent_span_id: None,
            name: (*name).to_string(),
            start_ts_ns: 1_650_000_000_000_000_000 + i as i64 * 1000,
            end_ts_ns: 1_650_000_000_000_000_000 + i as i64 * 1000 + 500,
            status_code: *status,
            status_message: None,
            attrs: vec![
                ("service.name".to_string(), (*service).to_string()),
                ("http.method".to_string(), "GET".to_string()),
            ],
        })
        .collect()
}

fn golden_config() -> RspanConfig {
    RspanConfig {
        block_target_records: 1,
        ..RspanConfig::default()
    }
}

fn write_golden() -> Vec<u8> {
    let mut w = RspanWriter::new(golden_config(), fixed_identity());
    for rec in golden_records() {
        w.push(rec);
    }
    w.finish().expect("finish v3 golden")
}

#[test]
fn matches_golden_fixture() {
    let written = write_golden();
    let fixture: &[u8] = include_bytes!("fixtures/golden_rspan_v3.bin");
    assert_eq!(
        written.as_slice(),
        fixture,
        "RSPAN v3 writer output diverged from the captured golden fixture; \
         RSPAN v3 is frozen (docs/span-segment-format.md) -- this must never change \
         without a version bump and ADR"
    );

    // The trailer carries the live footer::VERSION (never a mirrored literal),
    // and every mandatory v3 section is present, including the v3-added BLOOM.
    let n = written.len();
    assert_eq!(
        u16::from_le_bytes([written[n - 8], written[n - 7]]),
        footer::VERSION,
        "trailer must carry the live footer::VERSION"
    );
    let decoded = open(&written).expect("open golden object");
    for k in [kind::BLOCKS, kind::SKIP_IDX, kind::BLOOM] {
        assert!(
            decoded.section(k).is_some(),
            "mandatory section kind {k} present"
        );
    }

    // The whole object round-trips: every record is recoverable.
    let cfg = golden_config();
    let reader = RspanReader::new(&written, &cfg).expect("reader");
    let (scanned, _stats) = reader
        .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
        .expect("scan");
    assert_eq!(scanned.len(), golden_records().len());
}

#[test]
fn write_is_deterministic_across_repeated_calls() {
    assert_eq!(
        write_golden(),
        write_golden(),
        "v3 output must be deterministic"
    );
}

#[test]
#[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
fn capture_golden_rspan_v3() {
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/golden_rspan_v3.bin"
        ),
        write_golden(),
    )
    .expect("write fixture");
}
