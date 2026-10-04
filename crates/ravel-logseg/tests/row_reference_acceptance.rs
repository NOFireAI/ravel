//! Named acceptance tests for the routed build path (ADR-2467 decision 1,
//! issue #2564): row-shaped input is folded into a `ColumnarLogBatch` and
//! built through `build_object_columnar`, and the row builder (`build_object`)
//! is reference-only behind the `row-reference` feature from here on.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_logseg::{
    AttrValue, ColumnarLogBatch, LogRecord, LogSegError, LogStreamId, ObjectIdentity, RlogConfig,
    RlogWriter, stream_attrs_bytes,
};

const STREAMS: u8 = 4;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0x11; 16],
        shard: 1,
        writer_id: [0x22; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

/// The canonical stream_attrs blob for synthetic stream `n`. Every record of a
/// stream must carry the same bytes, so it is derived from `n` alone: this
/// keeps generated corpora internally consistent (no `InconsistentStreamAttrs`
/// pair by accident) so the byte-identity property is exercised on valid
/// input. `routed_path_refuses_two_blobs_for_one_stream_id` below hand-builds
/// the inconsistent case instead.
fn stream_blob(n: u8) -> Vec<u8> {
    stream_attrs_bytes(
        &[("service.name".to_string(), AttrValue::Str(format!("s{n}")))],
        "scope",
        "1",
        &[],
    )
}

fn arb_value() -> impl Strategy<Value = AttrValue> {
    prop_oneof![
        "[a-z]{0,12}".prop_map(AttrValue::Str),
        any::<i64>().prop_map(AttrValue::I64),
        any::<f64>().prop_map(AttrValue::F64),
        any::<bool>().prop_map(AttrValue::Bool),
        proptest::collection::vec(any::<u8>(), 0..8).prop_map(AttrValue::Bytes),
    ]
}

fn arb_record() -> impl Strategy<Value = LogRecord> {
    (
        0u8..STREAMS,
        0i64..1_000,
        "[a-z ]{0,40}",
        0u8..30,
        proptest::collection::vec(("[a-z]{1,8}", arb_value()), 0..4),
        proptest::option::of(any::<[u8; 16]>()),
        proptest::option::of(any::<[u8; 8]>()),
    )
        .prop_map(|(stream, ts, body, sev, raw_attrs, trace, span)| {
            let mut seen = std::collections::HashSet::new();
            let mut attrs = Vec::new();
            for (name, v) in raw_attrs {
                if seen.insert(name.clone()) {
                    attrs.push((name, v));
                }
            }
            LogRecord {
                stream_id: sid(stream),
                stream_attrs: stream_blob(stream),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: sev,
                severity_text: if sev % 2 == 0 { "INFO" } else { "WARN" }.to_string(),
                body,
                trace_id: trace,
                span_id: span,
                flags: 0,
                attrs,
            }
        })
}

fn arb_corpus() -> impl Strategy<Value = Vec<LogRecord>> {
    proptest::collection::vec(arb_record(), 0..80)
}

fn write_reference(cfg: &RlogConfig, records: &[LogRecord]) -> Result<Vec<u8>, LogSegError> {
    let mut w = RlogWriter::new(*cfg, identity());
    for r in records {
        w.push(r.clone())?;
    }
    w.finish_row_reference()
}

fn write_routed(cfg: &RlogConfig, records: &[LogRecord]) -> Result<Vec<u8>, LogSegError> {
    let mut w = RlogWriter::new(*cfg, identity());
    for r in records {
        w.push(r.clone())?;
    }
    w.finish()
}

proptest! {
    /// The reference row builder (`build_object`, reached through
    /// `finish_row_reference`) and the routed production path (`finish`,
    /// which folds row input into a `ColumnarLogBatch` and builds it through
    /// `build_object_columnar`) produce byte-identical objects for the same
    /// records, over arbitrary corpora including the empty one.
    ///
    /// Wrong implementations this rules out, each shown failing: a fold that
    /// drops or reorders a record before building; a columnar build that
    /// chooses a different page encoding than the row build for the same
    /// values.
    #[test]
    fn row_reference_and_routed_path_are_byte_identical(records in arb_corpus()) {
        let cfg = RlogConfig::default();
        let reference = write_reference(&cfg, &records);
        let routed = write_routed(&cfg, &records);
        match (&reference, &routed) {
            (Ok(r), Ok(c)) => prop_assert_eq!(r, c),
            (Err(LogSegError::LimitExceeded(a)), Err(LogSegError::LimitExceeded(b))) => {
                prop_assert_eq!(a, b);
            }
            other => prop_assert!(false, "reference and routed paths disagree: {other:?}"),
        }
    }
}

/// Two records sharing a stream id but carrying different `stream_attrs`
/// bytes refuse the whole object on the routed path (`push` + `finish`), the
/// same way the row builder always has: there is no single truthful
/// STREAM_DIR blob to write. The fold in `build()` runs this check as a
/// pre-pass over the records before delegating to `ColumnarLogBatch::from_records`
/// (`ColumnarLogBatch::fold_records`), so removing that pre-pass is exactly
/// the mutation this test is written to catch.
#[test]
fn routed_path_refuses_two_blobs_for_one_stream_id() {
    let mut a = LogRecord {
        stream_id: sid(0),
        stream_attrs: stream_blob(0),
        ts_ns: 1,
        observed_ts_ns: 1,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "a".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    };
    let mut b = a.clone();
    a.ts_ns = 1;
    b.ts_ns = 2;
    b.stream_attrs = stream_blob(1); // same stream_id as `a`, a different blob

    let mut w = RlogWriter::new(RlogConfig::default(), identity());
    w.push(a).expect("push a");
    w.push(b).expect("push b");
    match w.finish() {
        Err(LogSegError::InconsistentStreamAttrs(m)) => {
            assert!(m.contains(&sid(0).to_hex()), "{m}");
        }
        other => panic!(
            "expected InconsistentStreamAttrs, got {:?}",
            other.map(|o| o.len())
        ),
    }
}

/// A writer is row-major or columnar for its whole lifetime (ADR-0109
/// decision 5), never both: a `push` after a `push_columnar`, or a
/// `push_columnar` after a `push`, is refused with a typed `LimitExceeded`
/// rather than silently mixing the two buffers. Neither refusal had a test
/// before this one (found while building the ADR-2467 error-parity table for
/// issue #2564): both are reachable from the row-builder entry point
/// (`push`) and so are in scope for this routing change even though the
/// check itself is unchanged by it.
#[test]
fn cross_mode_push_is_refused() {
    let rec = LogRecord {
        stream_id: sid(0),
        stream_attrs: stream_blob(0),
        ts_ns: 1,
        observed_ts_ns: 1,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "a".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    };
    let batch = ColumnarLogBatch::from_records(std::slice::from_ref(&rec));

    let mut row_first = RlogWriter::new(RlogConfig::default(), identity());
    row_first.push(rec.clone()).expect("push");
    match row_first.push_columnar(batch.clone()) {
        Err(LogSegError::LimitExceeded(m)) => {
            assert!(m.contains("columnar push into a row-major writer"), "{m}");
        }
        other => panic!("expected LimitExceeded, got {other:?}"),
    }

    let mut columnar_first = RlogWriter::new(RlogConfig::default(), identity());
    columnar_first.push_columnar(batch).expect("push_columnar");
    match columnar_first.push(rec) {
        Err(LogSegError::LimitExceeded(m)) => {
            assert!(m.contains("row-major push into a columnar writer"), "{m}");
        }
        other => panic!("expected LimitExceeded, got {other:?}"),
    }
}
