//! Golden-bytes regression for the RSPAN v4 writer (ADR-0045 decision 3,
//! docs/span-segment-format.md): the writer's output for a fixed representative
//! input must stay byte-for-byte identical across internal refactors of the v4
//! encode path. RSPAN v4 is a frozen persistent contract; this test is the
//! tripwire for an accidental format change.
//!
//! The fixture exercises the v4-added grammar: per-key dynamic string columns
//! (`http.method`), a lifted `service_name` column, the mandatory BLOOM
//! section, and the nested event columns (one span carries a `_events_raw`
//! blob). The writer takes all identity, timestamps, and config as inputs (no
//! `SystemTime::now`, no randomness), so the output is deterministic and
//! golden-pinnable.
//!
//! To regenerate `golden_rspan_v4.bin` after a deliberate, versioned format
//! change (never for an internal refactor), run:
//!   cargo test -p ravel-rspan --test golden_bytes_v4 -- --ignored --nocapture
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

/// One deterministic, validly-framed events blob (a single OTLP-shaped event):
/// `varint(len)` then a `Span.Event` with time (field 1, fixed64), name
/// (field 2), and an attributes stand-in (field 3), hex-encoded exactly as the
/// ingest path encodes `_events_raw`.
fn events_blob() -> String {
    let mut event = Vec::new();
    event.push(0x09u8); // field 1, wire type 1 (fixed64) time_unix_nano
    event.extend_from_slice(&1_650_000_000_000_000_100i64.to_le_bytes());
    event.push(0x12); // field 2, wire type 2 (string) name
    event.push(9);
    event.extend_from_slice(b"exception");
    event.push(0x1a); // field 3, wire type 2 (attributes stand-in)
    event.push(5);
    event.extend_from_slice(b"stack");
    let mut raw = Vec::new();
    raw.push(event.len() as u8); // canonical single-byte varint length
    raw.extend_from_slice(&event);
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

/// A fixed handful of spans across several services and names, one span per
/// block so each block's BLOOM, service_name, dynamic, and event columns have
/// known content. The first span carries an events blob.
fn golden_records() -> Vec<SpanRecord> {
    let rows = [
        ("checkout", "GET /cart", StatusCode::Ok),
        ("payments", "charge card", StatusCode::Error),
        ("checkout", "POST /order", StatusCode::Ok),
        ("inventory", "list items", StatusCode::Unset),
    ];
    rows.iter()
        .enumerate()
        .map(|(i, (service, name, status))| {
            let mut attrs = vec![
                ("service.name".to_string(), (*service).to_string()),
                ("http.method".to_string(), "GET".to_string()),
            ];
            if i == 0 {
                attrs.push(("_events_raw".to_string(), events_blob()));
            }
            SpanRecord {
                trace_id: [i as u8; 16],
                span_id: [i as u8; 8],
                parent_span_id: None,
                name: (*name).to_string(),
                start_ts_ns: 1_650_000_000_000_000_000 + i as i64 * 1000,
                end_ts_ns: 1_650_000_000_000_000_000 + i as i64 * 1000 + 500,
                status_code: *status,
                status_message: None,
                attrs,
            }
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
    w.finish().expect("finish v4 golden")
}

#[test]
fn matches_golden_fixture() {
    let written = write_golden();
    let fixture: &[u8] = include_bytes!("fixtures/golden_rspan_v4.bin");
    assert_eq!(
        written.as_slice(),
        fixture,
        "RSPAN v4 writer output diverged from the captured golden fixture; \
         RSPAN v4 is frozen (docs/span-segment-format.md) -- this must never change \
         without a version bump and ADR"
    );

    // The trailer carries the live footer::VERSION (never a mirrored literal),
    // and every mandatory section is present, including the BLOOM.
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

    // The whole object round-trips: every record is recoverable, including the
    // dynamic attribute columns and the events blob (via the nested columns).
    let cfg = golden_config();
    let reader = RspanReader::new(&written, &cfg).expect("reader");
    let (scanned, _stats) = reader
        .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
        .expect("scan");
    assert_eq!(scanned.len(), golden_records().len());
    let mut want = golden_records();
    want.sort_by_key(|r| r.trace_id);
    let mut got = scanned;
    got.sort_by_key(|r| r.trace_id);
    for (g, w) in got.iter().zip(want.iter()) {
        // The attribute map (dynamic columns + service.name + reconstructed
        // _events_raw) round-trips exactly (canonical sorted, unique keys).
        let mut want_attrs: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for (k, v) in &w.attrs {
            want_attrs.insert(k.clone(), v.clone());
        }
        let want_attrs: Vec<(String, String)> = want_attrs.into_iter().collect();
        assert_eq!(g.attrs, want_attrs);
    }
}

#[test]
fn write_is_deterministic_across_repeated_calls() {
    assert_eq!(
        write_golden(),
        write_golden(),
        "v4 output must be deterministic"
    );
}

#[test]
#[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
fn capture_golden_rspan_v4() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
    std::fs::create_dir_all(dir).expect("create fixtures dir");
    std::fs::write(format!("{dir}/golden_rspan_v4.bin"), write_golden()).expect("write fixture");
}
