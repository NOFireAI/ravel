//! Integration tests for the OTAP stream state machine (issue #12, Part 1):
//! encode/decode roundtrip, schema resets, and the resource caps that
//! protect a `StreamState` from a hostile or buggy exporter.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{Array, AsArray, DictionaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, UInt8Type};
use proptest::prelude::*;
use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder,
};
use ravel_otap::proto::experimental::arrow::v1::{
    ArrowPayload, ArrowPayloadType, BatchArrowRecords,
};
use ravel_otap::stream::{BatchError, DecodeError, StreamConfig, StreamError, StreamState};

fn metric(name: &str, points: Vec<DataPointRow>) -> MetricRow {
    MetricRow {
        name: name.to_string(),
        kind: MetricKind::Gauge,
        data_points: points,
    }
}

fn point(time_unix_nano: i64, value: f64, attrs: Vec<(&str, &str)>) -> DataPointRow {
    DataPointRow {
        exemplars: vec![],
        time_unix_nano,
        value,
        flags: 0,
        attrs: attrs
            .into_iter()
            .map(|(key, value)| AttrRow {
                key: key.to_string(),
                value: AttrValue::Str(value.to_string()),
            })
            .collect(),
    }
}

fn root_batches(decoded: &[(ArrowPayloadType, RecordBatch)]) -> impl Iterator<Item = &RecordBatch> {
    decoded
        .iter()
        .filter(|(t, _)| *t == ArrowPayloadType::UnivariateMetrics)
        .map(|(_, rb)| rb)
}

fn dp_batches(decoded: &[(ArrowPayloadType, RecordBatch)]) -> impl Iterator<Item = &RecordBatch> {
    decoded
        .iter()
        .filter(|(t, _)| *t == ArrowPayloadType::NumberDataPoints)
        .map(|(_, rb)| rb)
}

fn attr_batches(decoded: &[(ArrowPayloadType, RecordBatch)]) -> impl Iterator<Item = &RecordBatch> {
    decoded
        .iter()
        .filter(|(t, _)| *t == ArrowPayloadType::NumberDpAttrs)
        .map(|(_, rb)| rb)
}

fn dictionary_values(column: &Arc<dyn Array>) -> Vec<String> {
    let dict = column
        .as_any()
        .downcast_ref::<DictionaryArray<UInt8Type>>()
        .expect("name column is a UInt8 dictionary");
    let values = dict.values().as_string::<i32>();
    dict.keys()
        .iter()
        .map(|k| {
            let k = k.expect("dictionary key is never null in our encoder");
            values.value(k as usize).to_string()
        })
        .collect()
}

#[test]
fn roundtrip_preserves_rows_and_values_across_batches_on_one_stream() {
    let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder construction");
    let mut state = StreamState::new(StreamConfig::default());

    let batch1 = encoder
        .encode_batch(
            1,
            &[
                metric("cpu.load", vec![point(1_000, 0.5, vec![("host", "a")])]),
                metric("mem.free", vec![point(1_000, 128.0, vec![])]),
            ],
        )
        .expect("encode batch 1");
    let batch2 = encoder
        .encode_batch(
            2,
            &[metric(
                "cpu.load",
                vec![
                    point(2_000, 0.75, vec![("host", "a")]),
                    point(2_000, 0.25, vec![("host", "b")]),
                ],
            )],
        )
        .expect("encode batch 2");

    let decoded1 = state.decode(batch1).expect("decode batch 1");
    assert_eq!(decoded1.batch_id, 1);
    let root1: Vec<&RecordBatch> = root_batches(&decoded1.payloads).collect();
    assert_eq!(root1.len(), 1);
    assert_eq!(root1[0].num_rows(), 2);
    assert_eq!(
        dictionary_values(root1[0].column_by_name("name").expect("name column")),
        vec!["cpu.load".to_string(), "mem.free".to_string()]
    );
    let dp1: Vec<&RecordBatch> = dp_batches(&decoded1.payloads).collect();
    assert_eq!(dp1[0].num_rows(), 2);
    let attrs1: Vec<&RecordBatch> = attr_batches(&decoded1.payloads).collect();
    assert_eq!(attrs1.len(), 1);
    assert_eq!(attrs1[0].num_rows(), 1);

    let decoded2 = state.decode(batch2).expect("decode batch 2");
    assert_eq!(decoded2.batch_id, 2);
    let root2: Vec<&RecordBatch> = root_batches(&decoded2.payloads).collect();
    assert_eq!(root2.len(), 1);
    assert_eq!(root2[0].num_rows(), 1);
    assert_eq!(
        dictionary_values(root2[0].column_by_name("name").expect("name column")),
        vec!["cpu.load".to_string()]
    );
    let dp2: Vec<&RecordBatch> = dp_batches(&decoded2.payloads).collect();
    assert_eq!(dp2[0].num_rows(), 2);
    let double_values = dp2[0]
        .column_by_name("double_value")
        .expect("double_value column")
        .as_primitive::<arrow::datatypes::Float64Type>();
    assert_eq!(double_values.values(), &[0.75, 0.25]);
    let attrs2: Vec<&RecordBatch> = attr_batches(&decoded2.payloads).collect();
    assert_eq!(attrs2[0].num_rows(), 2);
}

#[test]
fn second_schema_id_on_same_stream_works() {
    let mut encoder_v1 = MetricsStreamEncoder::new("v1").expect("encoder v1");
    let mut encoder_v2 = MetricsStreamEncoder::new("v2").expect("encoder v2");
    let mut state = StreamState::new(StreamConfig::default());

    let batch_v1 = encoder_v1
        .encode_batch(1, &[metric("cpu.load", vec![point(1_000, 0.1, vec![])])])
        .expect("encode v1");
    let batch_v2 = encoder_v2
        .encode_batch(2, &[metric("cpu.load", vec![point(2_000, 0.2, vec![])])])
        .expect("encode v2");

    let decoded_v1 = state.decode(batch_v1).expect("decode v1 schema");
    assert_eq!(root_batches(&decoded_v1.payloads).count(), 1);
    let decoded_v2 = state.decode(batch_v2).expect("decode v2 schema (reset)");
    assert_eq!(root_batches(&decoded_v2.payloads).count(), 1);
}

fn zstd_wrap(raw: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(raw, 0).expect("zstd compress")
}

#[test]
fn decompression_bomb_rejected_before_allocation() {
    let mut state = StreamState::new(StreamConfig::default());
    let bomb = vec![0u8; 64 * 1024 * 1024];
    let compressed = zstd_wrap(&bomb);
    assert!(
        compressed.len() < 1024 * 1024,
        "sanity: bomb should compress small"
    );

    let batch = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![ArrowPayload {
            schema_id: "bomb".to_string(),
            r#type: ArrowPayloadType::UnivariateMetrics as i32,
            record: compressed.into(),
        }],
        headers: Vec::new(),
    };

    let err = state.decode(batch).expect_err("bomb must be rejected");
    match err {
        DecodeError::Batch(BatchError::DecompressedPayloadTooLarge { limit }) => {
            assert_eq!(
                limit,
                StreamConfig::default().max_decompressed_payload_bytes
            );
        }
        other => panic!("expected DecompressedPayloadTooLarge, got {other:?}"),
    }
}

#[test]
fn row_count_cap_rejected() {
    let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
    let config = StreamConfig {
        max_rows_per_batch: 1,
        ..StreamConfig::default()
    };
    let mut state = StreamState::new(config);

    let batch = encoder
        .encode_batch(
            1,
            &[metric(
                "cpu.load",
                vec![point(1_000, 0.1, vec![]), point(2_000, 0.2, vec![])],
            )],
        )
        .expect("encode 2-row data point batch");

    let err = state.decode(batch).expect_err("row cap must reject");
    match err {
        DecodeError::Batch(BatchError::RowCountExceeded { limit, actual }) => {
            assert_eq!(limit, 1);
            assert!(actual > 1);
        }
        other => panic!("expected RowCountExceeded, got {other:?}"),
    }
}

#[test]
fn malformed_ipc_bytes_nack_batch_and_stream_survives() {
    let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
    let mut state = StreamState::new(StreamConfig::default());

    let garbage = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![ArrowPayload {
            schema_id: "garbage".to_string(),
            r#type: ArrowPayloadType::UnivariateMetrics as i32,
            record: zstd_wrap(b"not an arrow ipc stream at all, just noise bytes").into(),
        }],
        headers: Vec::new(),
    };
    let err = state.decode(garbage).expect_err("garbage must nack");
    assert!(matches!(err, DecodeError::Batch(_)));

    let good = encoder
        .encode_batch(2, &[metric("cpu.load", vec![point(1_000, 0.1, vec![])])])
        .expect("encode valid batch");
    let decoded = state
        .decode(good)
        .expect("stream must still accept a valid batch after a nack");
    assert_eq!(root_batches(&decoded.payloads).count(), 1);
}

#[test]
fn schema_store_corruption_yields_stream_error_and_poisons_stream() {
    let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
    let mut state = StreamState::new(StreamConfig::default());

    let batch1 = encoder
        .encode_batch(
            1,
            &[metric(
                "cpu.load",
                vec![point(1_000, 0.1, vec![("host", "a")])],
            )],
        )
        .expect("encode batch 1");
    state
        .decode(batch1)
        .expect("decode batch 1 establishes schema (including NUMBER_DP_ATTRS)");

    // Attribute values land in a plain (non-dictionary) Utf8 column, so a
    // literal copy of this marker string is guaranteed to appear in the
    // record batch's raw string-data buffer -- unlike a numeric column, we
    // don't need to know arrow's buffer layout to find a byte we can
    // corrupt into invalid UTF-8.
    const MARKER: &[u8] = b"ZWQXCORRUPTIONMARKERZWQX";
    let batch2 = encoder
        .encode_batch(
            2,
            &[metric(
                "cpu.load",
                vec![point(
                    2_000,
                    0.2,
                    vec![("host", std::str::from_utf8(MARKER).expect("ascii marker"))],
                )],
            )],
        )
        .expect("encode batch 2");
    let attrs_payload = batch2
        .arrow_payloads
        .iter()
        .find(|p| p.r#type == ArrowPayloadType::NumberDpAttrs as i32)
        .expect("batch 2 has an attrs payload")
        .clone();
    let raw =
        zstd::stream::decode_all(attrs_payload.record.as_ref()).expect("decompress batch 2 attrs");

    let marker_at = raw
        .windows(MARKER.len())
        .position(|w| w == MARKER)
        .expect("marker string present verbatim in raw IPC bytes");
    let mut corrupted = raw.clone();
    // An 0xff byte is invalid as any position in a UTF-8 sequence, so this
    // is rejected by arrow's mandatory Utf8 validation on decode -- but the
    // message framing (lengths, row counts) is untouched, so our own
    // pre-decode scan still accepts it, and the failure only surfaces once
    // the real `StreamDecoder` (which already trusts this schema_id from
    // batch 1) tries to materialize the array.
    corrupted[marker_at] = 0xff;
    let corrupted_payload = ArrowPayload {
        record: zstd_wrap(&corrupted).into(),
        ..attrs_payload
    };
    let corrupted_batch = BatchArrowRecords {
        batch_id: 2,
        arrow_payloads: vec![corrupted_payload],
        headers: Vec::new(),
    };

    let err = state
        .decode(corrupted_batch)
        .expect_err("corrupted mid-stream payload must fail");
    assert!(
        matches!(err, DecodeError::Stream(StreamError::Corrupted { .. })),
        "expected StreamError::Corrupted, got {err:?}"
    );

    let batch3 = encoder
        .encode_batch(3, &[metric("cpu.load", vec![point(3_000, 0.3, vec![])])])
        .expect("encode batch 3");
    let err = state
        .decode(batch3)
        .expect_err("poisoned stream must reject all further batches");
    assert!(matches!(err, DecodeError::Stream(StreamError::Poisoned)));
}

#[test]
fn max_schemas_per_stream_is_enforced() {
    let config = StreamConfig {
        max_schemas_per_stream: 2,
        ..StreamConfig::default()
    };
    let mut state = StreamState::new(config);

    let unknown_field = Field::new("x", DataType::UInt8, false);
    let tiny_schema = Schema::new(vec![unknown_field]);
    let make_payload = |schema_id: &str| {
        let mut buf = Vec::new();
        {
            let mut writer =
                arrow_ipc::writer::StreamWriter::try_new(&mut buf, &tiny_schema).expect("writer");
            let batch = RecordBatch::try_new(
                Arc::new(tiny_schema.clone()),
                vec![Arc::new(arrow::array::UInt8Array::from(vec![1u8]))],
            )
            .expect("batch");
            writer.write(&batch).expect("write");
        }
        ArrowPayload {
            schema_id: schema_id.to_string(),
            r#type: ArrowPayloadType::UnivariateMetrics as i32,
            record: zstd_wrap(&buf).into(),
        }
    };

    state
        .decode(BatchArrowRecords {
            batch_id: 1,
            arrow_payloads: vec![make_payload("schema-a")],
            headers: Vec::new(),
        })
        .expect("first schema admitted");
    state
        .decode(BatchArrowRecords {
            batch_id: 2,
            arrow_payloads: vec![make_payload("schema-b")],
            headers: Vec::new(),
        })
        .expect("second schema admitted");

    let err = state
        .decode(BatchArrowRecords {
            batch_id: 3,
            arrow_payloads: vec![make_payload("schema-c")],
            headers: Vec::new(),
        })
        .expect_err("third distinct schema must exceed cap");
    match err {
        DecodeError::Batch(BatchError::SchemaBudgetExceeded { limit }) => assert_eq!(limit, 2),
        other => panic!("expected SchemaBudgetExceeded, got {other:?}"),
    }

    // The cap rejection is a plain nack, not stream corruption: the two
    // already-admitted decoders are untouched and the stream is not
    // poisoned (a later test covers `StreamError::Poisoned` directly).
    assert!(!state.is_poisoned());
}

// Per-stream dictionary-bytes budget cap (`max_stream_dictionary_bytes`,
// docs/otap-ingest.md Safety, spec section 19: "max schemas and max
// dictionary memory per stream"). Enforced in `decode_payload` at
// stream.rs:382-394 by summing the `body_len` of every `DictionaryBatch` IPC
// message and rejecting once the running per-stream total would exceed the
// cap; the accumulation itself is stream.rs:405. This is issue #74 (a9-F02):
// the sibling caps (decompressed-size, row-count, schema-count) each have a
// test above, this one had none.
//
// The exact IPC framing size of a DictionaryBatch body is opaque (arrow-ipc
// internals, out of scope to read), so the boundary caps below are measured,
// not hardcoded: on a fresh stream `dictionary_bytes_used` starts at 0 and
// the check is `used + bytes > limit`, so the smallest cap that admits a
// fixed batch sequence equals that sequence's total dictionary bytes. The
// encoder's root payload carries a `Dictionary<UInt8, Utf8>` name column, so
// a batch with one metric emits exactly one DictionaryBatch; distinct names
// across batches force a fresh DictionaryBatch each time (a changed
// dictionary is re-sent), which is what makes the total accumulate.

/// Smallest `max_stream_dictionary_bytes` that admits the given sequence of
/// one-metric batches (one distinct metric name per batch) decoded on a
/// single fresh stream. Equal to the cumulative DictionaryBatch body bytes
/// those batches produce. Binary search, since the framing size is opaque.
fn min_dict_cap_admitting(names: &[&str]) -> u64 {
    let admits = |cap: u64| {
        let mut encoder = MetricsStreamEncoder::new("dict-probe").expect("encoder");
        let mut state = StreamState::new(StreamConfig {
            max_stream_dictionary_bytes: cap,
            ..StreamConfig::default()
        });
        for (i, name) in names.iter().enumerate() {
            let batch = encoder
                .encode_batch(i as i64, &[metric(name, vec![])])
                .expect("encode");
            if state.decode(batch).is_err() {
                return false;
            }
        }
        true
    };
    // `admits` is monotonic in `cap` (a larger budget never rejects a
    // sequence a smaller one accepted). Anchor: `admits(0)` is false because
    // any DictionaryBatch has a non-zero body.
    assert!(!admits(0), "a dictionary batch must consume some bytes");
    let mut hi = 1u64;
    while !admits(hi) {
        hi = hi
            .checked_mul(2)
            .expect("dictionary cap search overflowed u64");
    }
    let mut lo = 0u64;
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if admits(mid) {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    hi
}

/// a9-F02 boundary: the cap fires exactly at the documented threshold
/// (`used + bytes > limit`). A single batch is admitted when the cap equals
/// its dictionary bytes, and rejected when the cap is one byte lower. This is
/// deliverable 2 (a just-under-the-cap case that succeeds) paired with the
/// smallest exceedance that trips it, pinning the boundary rather than an
/// arbitrary tiny cap.
#[test]
fn dictionary_budget_cap_fires_at_exact_boundary() {
    let one_batch_bytes = min_dict_cap_admitting(&["cpu.load.aaa"]);
    assert!(
        one_batch_bytes >= 1,
        "sanity: one batch has dictionary bytes"
    );

    // Just under: cap == the batch's dictionary bytes. `used(0) + bytes` is
    // not `> limit`, so the batch is admitted.
    let mut encoder = MetricsStreamEncoder::new("dict-at-cap").expect("encoder");
    let mut state = StreamState::new(StreamConfig {
        max_stream_dictionary_bytes: one_batch_bytes,
        ..StreamConfig::default()
    });
    let batch = encoder
        .encode_batch(1, &[metric("cpu.load.aaa", vec![])])
        .expect("encode");
    state
        .decode(batch)
        .expect("batch at the exact cap must be admitted");
    assert!(!state.is_poisoned());

    // One byte over the boundary: the same batch is rejected. Uses a fresh
    // encoder/stream so the dictionary bytes match the measured value.
    let mut encoder = MetricsStreamEncoder::new("dict-over-cap").expect("encoder");
    let mut state = StreamState::new(StreamConfig {
        max_stream_dictionary_bytes: one_batch_bytes - 1,
        ..StreamConfig::default()
    });
    let batch = encoder
        .encode_batch(1, &[metric("cpu.load.aaa", vec![])])
        .expect("encode");
    let err = state
        .decode(batch)
        .expect_err("one byte below the batch's dictionary bytes must reject");
    match err {
        DecodeError::Batch(BatchError::DictionaryBudgetExceeded { limit }) => {
            assert_eq!(limit, one_batch_bytes - 1);
        }
        other => panic!("expected DictionaryBudgetExceeded, got {other:?}"),
    }
    // A cap nack, like the sibling caps, must not poison the stream.
    assert!(!state.is_poisoned());
}

/// a9-F02 accumulation: the cap is cumulative across batches, not per-batch.
/// This is deliverable 1 -- the budget trips on a later batch once the
/// running total crosses the cap, before dictionary memory grows unbounded.
/// The cap is set to exactly the dictionary bytes of the first two batches,
/// so batch 1 and batch 2 are admitted (batch 2 lands the running total right
/// on the cap) and batch 3, adding a fresh dictionary, is rejected. A
/// per-batch check would have admitted batch 3, so this pins the cumulative
/// semantics.
#[test]
fn dictionary_budget_cap_is_cumulative_across_batches() {
    let names = ["mem.used.aaa", "mem.used.bbb", "mem.used.ccc"];
    // Cap = the two-batch total. Under a per-batch interpretation each batch
    // is far under this, so a broken (per-batch) check would never trip.
    let two_batch_bytes = min_dict_cap_admitting(&names[..2]);
    // A third batch must actually add dictionary bytes, or "cumulative" is
    // vacuous: three batches need a strictly larger budget than two.
    let three_batch_bytes = min_dict_cap_admitting(&names);
    assert!(
        three_batch_bytes > two_batch_bytes,
        "each distinct-name batch must contribute dictionary bytes"
    );

    let mut encoder = MetricsStreamEncoder::new("dict-cumulative").expect("encoder");
    let mut state = StreamState::new(StreamConfig {
        max_stream_dictionary_bytes: two_batch_bytes,
        ..StreamConfig::default()
    });

    state
        .decode(
            encoder
                .encode_batch(1, &[metric(names[0], vec![])])
                .expect("encode batch 1"),
        )
        .expect("batch 1 is under the cumulative cap");
    state
        .decode(
            encoder
                .encode_batch(2, &[metric(names[1], vec![])])
                .expect("encode batch 2"),
        )
        .expect("batch 2 lands the running total exactly on the cap");

    let err = state
        .decode(
            encoder
                .encode_batch(3, &[metric(names[2], vec![])])
                .expect("encode batch 3"),
        )
        .expect_err("batch 3 pushes the cumulative total past the cap");
    match err {
        DecodeError::Batch(BatchError::DictionaryBudgetExceeded { limit }) => {
            assert_eq!(limit, two_batch_bytes);
        }
        other => panic!("expected DictionaryBudgetExceeded, got {other:?}"),
    }
    assert!(!state.is_poisoned());
}

/// Regression coverage for issue #18 (A2): prost `bytes::Bytes` decode,
/// aligned-buffer decompression, and feeding `StreamDecoder` the shared
/// buffer must not change one decoded bit versus what was encoded. There is
/// no pre-existing OTLP-vs-OTAP differential gate or fuzz target in this
/// repo to re-run (see final report); these tests are this crate's
/// equivalent coverage: value-preservation across the new decode path
/// (bit-for-bit on floats, per CLAUDE.md) and corrupt-input robustness.
mod a2_regression {
    use super::*;

    fn double_values(dp_batches: &[&RecordBatch]) -> Vec<f64> {
        dp_batches
            .iter()
            .flat_map(|rb| {
                rb.column_by_name("double_value")
                    .expect("double_value column")
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    fn point_value_strategy() -> impl Strategy<Value = f64> {
        prop_oneof![
            any::<f64>(),
            Just(f64::NAN),
            Just(-0.0_f64),
            Just(0.0_f64),
            Just(f64::INFINITY),
            Just(f64::NEG_INFINITY),
        ]
    }

    fn point_strategy() -> impl Strategy<Value = (i64, f64)> {
        (any::<i64>(), point_value_strategy())
    }

    fn metric_strategy() -> impl Strategy<Value = (String, Vec<(i64, f64)>)> {
        (
            "[a-z]{1,8}",
            proptest::collection::vec(point_strategy(), 0..6),
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Encoding then decoding through the new `Bytes`/aligned-buffer/
        /// `StreamDecoder` path must preserve every `double_value` bit
        /// pattern exactly, including NaN payloads and signed zero.
        #[test]
        fn decode_preserves_double_value_bits(
            metrics in proptest::collection::vec(metric_strategy(), 0..4),
        ) {
            let rows: Vec<MetricRow> = metrics
                .iter()
                .map(|(name, points)| {
                    metric(
                        name,
                        points
                            .iter()
                            .map(|(ts, value)| point(*ts, *value, vec![]))
                            .collect(),
                    )
                })
                .collect();
            let expected_bits: Vec<u64> = metrics
                .iter()
                .flat_map(|(_, points)| points.iter().map(|(_, value)| value.to_bits()))
                .collect();

            let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder construction");
            let batch = encoder.encode_batch(1, &rows).expect("encode batch");
            let mut state = StreamState::new(StreamConfig::default());
            let decoded = state.decode(batch).expect("decode batch");

            let dp: Vec<&RecordBatch> = dp_batches(&decoded.payloads).collect();
            let actual_bits: Vec<u64> = double_values(&dp).into_iter().map(f64::to_bits).collect();
            prop_assert_eq!(actual_bits, expected_bits);
        }

        /// Arbitrary bytes wrapped as a zstd-compressed `ArrowPayload.record`
        /// must never panic the decoder: either it is rejected with a typed
        /// error, or (vanishingly unlikely for random bytes) it happens to
        /// parse, but `StreamState` must stay usable either way.
        #[test]
        fn decode_never_panics_on_arbitrary_bytes(raw in proptest::collection::vec(any::<u8>(), 0..256)) {
            let compressed = zstd::stream::encode_all(raw.as_slice(), 0).expect("zstd compress");
            let mut state = StreamState::new(StreamConfig::default());
            let batch = BatchArrowRecords {
                batch_id: 1,
                arrow_payloads: vec![ArrowPayload {
                    schema_id: "fuzz".to_string(),
                    r#type: ArrowPayloadType::UnivariateMetrics as i32,
                    record: compressed.into(),
                }],
                headers: Vec::new(),
            };
            let _ = state.decode(batch);
        }
    }

    /// The copy-fallback counter added for issue #18 must account for every
    /// decoded frame exactly once: over an ordinary roundtrip, zero-copy and
    /// copy-fallback frames must sum to the number of `RecordBatch`es
    /// decoded, and (with our own encoder producing standard 8-byte-padded
    /// IPC bodies into an aligned destination buffer) the frames must be
    /// zero-copy.
    #[test]
    fn decode_stats_account_for_every_frame() {
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let mut state = StreamState::new(StreamConfig::default());

        let batch = encoder
            .encode_batch(
                1,
                &[metric(
                    "cpu.load",
                    vec![point(1_000, 0.5, vec![("host", "a")])],
                )],
            )
            .expect("encode batch");
        let decoded = state.decode(batch).expect("decode batch");
        let frames = decoded.payloads.len() as u64;

        let stats = state.decode_stats();
        assert_eq!(stats.zero_copy_frames + stats.copy_fallback_frames, frames);
        assert_eq!(
            stats.copy_fallback_frames, 0,
            "our own encoder's IPC bodies are 8-byte aligned into a 64-byte \
             aligned destination buffer, so this frame must be zero-copy"
        );
    }
}
