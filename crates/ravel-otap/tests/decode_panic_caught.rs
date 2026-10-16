//! Regression for the arrow-decode panic boundary in `StreamState::decode`.
//!
//! A tenant sending a malformed `BatchArrowRecords` can drive arrow's
//! `StreamDecoder` to panic inside `arrow-buffer` (e.g. "the offset of the new
//! Buffer cannot exceed the existing length") instead of returning `Err`. The
//! crate's own `tests/fuzz_mutation.rs` documents this as a KNOWN LIMITATION
//! and works around it in the *test harness* with `catch_unwind`. This test
//! asserts the *production* fix: `StreamState::decode` now catches that unwind
//! at the arrow boundary and returns a typed
//! `DecodeError::Batch(BatchError::InternalPanic(_))`.
//!
//! Prove-the-test note: the sweep below calls `decode` directly, with NO
//! `catch_unwind` of its own. Against the pre-fix code at least one single-bit
//! flip unwinds straight through `decode` and aborts this test process; the
//! `assert!(internal_panics > 0, ...)` at the end guarantees the sweep really
//! reaches a panicking input, so the test is not vacuously green. After the fix
//! every such flip is a typed error and the count is non-zero.
#![allow(clippy::expect_used)]

use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder,
};
use ravel_otap::proto::experimental::arrow::v1::{ArrowPayload, BatchArrowRecords};
use ravel_otap::stream::{BatchError, DecodeError, StreamConfig, StreamState};

fn zstd_wrap(raw: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(raw, 0).expect("zstd compress")
}

/// The same representative metric shape the fuzz harness seeds from, so the
/// captured IPC stream is rich enough (schema + dictionaries + record batch)
/// that some single-bit flip reaches the panicking arrow-buffer path.
fn representative_metrics() -> Vec<MetricRow> {
    vec![
        MetricRow {
            name: "cpu.load".to_string(),
            kind: MetricKind::Gauge,
            data_points: vec![
                DataPointRow {
                    exemplars: vec![],
                    time_unix_nano: 1_000,
                    value: 0.5,
                    flags: 0,
                    attrs: vec![AttrRow {
                        key: "host".to_string(),
                        value: AttrValue::Str("a".to_string()),
                    }],
                },
                DataPointRow {
                    exemplars: vec![],
                    time_unix_nano: 2_000,
                    value: f64::NAN,
                    flags: 0,
                    attrs: Vec::new(),
                },
            ],
        },
        MetricRow {
            name: "mem.free".to_string(),
            kind: MetricKind::Sum {
                temporality: 2,
                is_monotonic: true,
            },
            data_points: vec![DataPointRow {
                exemplars: vec![],
                time_unix_nano: 3_000,
                value: 128.0,
                flags: 0,
                attrs: Vec::new(),
            }],
        },
    ]
}

/// The largest payload's `(schema_id, type, decompressed IPC bytes)` from the
/// crate's own encoder. Mutating the raw IPC (not the zstd frame) lets the
/// mutation survive decompression and reach the arrow `StreamDecoder`.
fn largest_seed() -> (String, i32, Vec<u8>) {
    let mut encoder = MetricsStreamEncoder::new("panic-fix").expect("encoder construction");
    let batch = encoder
        .encode_batch(1, &representative_metrics())
        .expect("encode representative batch");
    let payload = batch
        .arrow_payloads
        .into_iter()
        .max_by_key(|p| p.record.len())
        .expect("at least one arrow payload");
    let ipc = zstd::stream::decode_all(payload.record.as_ref()).expect("decompress seed payload");
    (payload.schema_id, payload.r#type, ipc)
}

fn one_payload_batch(schema_id: &str, r#type: i32, ipc: &[u8]) -> BatchArrowRecords {
    BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![ArrowPayload {
            schema_id: schema_id.to_string(),
            r#type,
            record: zstd_wrap(ipc).into(),
        }],
        headers: Vec::new(),
    }
}

#[test]
fn arrow_boundary_panic_is_typed_error_not_unwind() {
    let (schema_id, r#type, ipc) = largest_seed();

    let mut internal_panics = 0usize;
    for byte in 0..ipc.len() {
        for bit in 0..8 {
            let mut corrupt = ipc.clone();
            corrupt[byte] ^= 1u8 << bit;

            let mut state = StreamState::new(StreamConfig::default());
            // No catch_unwind here on purpose: a surviving arrow-boundary
            // panic would unwind through this call and abort the test. Any
            // other typed error, or a valid decode, is fine -- the point is
            // only that decode returns rather than unwinds.
            if let Err(DecodeError::Batch(BatchError::InternalPanic(_))) =
                state.decode(one_payload_batch(&schema_id, r#type, &corrupt))
            {
                internal_panics += 1;
            }
        }
    }

    assert!(
        internal_panics > 0,
        "expected at least one single-bit flip to hit the arrow StreamDecoder \
         panic path and be converted to DecodeError::Batch(BatchError::InternalPanic); \
         found none, so this test would be vacuous"
    );
}
