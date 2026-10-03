//! Correctness gates for the bake-off codecs (`ravel_bench::codecs`).
//! Every codec must round-trip bit-exactly (`f64::to_bits`), including NaN
//! payload bits, -0.0, denormals, and mixed streams, and every decoder must
//! turn corrupt or truncated input into a typed error, never a panic (the
//! repo's testing rules: property tests for every codec, corrupt-input tests
//! produce typed errors).
//!
//! The production Gorilla codec (RSEG enc 16) is not tested here: it has its
//! own round-trip and corrupt-input proptests in `ravel-segment`, and the
//! bake-off reaches it through the segment public API, not a reimplementation.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::prelude::*;
use ravel_bench::codecs::{Alp, ByteSplitZstd, Chimp128, GcdDeltaFor, RawF64, ValueCodec};

fn bits(values: &[f64]) -> Vec<u64> {
    values.iter().map(|v| v.to_bits()).collect()
}

/// Bit-exact round trip for one codec on one value stream.
fn assert_roundtrip<C: ValueCodec>(codec: &C, values: &[f64]) {
    let encoded = codec.encode(values);
    let decoded = codec
        .decode(&encoded, values.len())
        .unwrap_or_else(|e| panic!("{} decode failed: {e}", codec.name()));
    assert_eq!(
        bits(&decoded),
        bits(values),
        "{} did not round-trip bit-exactly",
        codec.name()
    );
}

fn assert_roundtrip_all(values: &[f64]) {
    assert_roundtrip(&Alp, values);
    assert_roundtrip(&GcdDeltaFor, values);
    assert_roundtrip(&Chimp128, values);
    assert_roundtrip(&ByteSplitZstd, values);
    assert_roundtrip(&RawF64, values);
}

/// A grab bag of bit patterns that stress every special case: signed zeros,
/// quiet and signalling NaNs with distinct payloads, both infinities, the
/// smallest normal and denormals, and the f64 extremes.
fn special_values() -> Vec<f64> {
    vec![
        0.0,
        -0.0,
        f64::NAN,
        -f64::NAN,
        f64::from_bits(0x7FF8_0000_0000_0001),
        f64::from_bits(0x7FF0_0000_0000_0001), // signalling NaN
        f64::from_bits(0xFFF8_DEAD_BEEF_CAFE), // negative NaN with payload
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MIN_POSITIVE,
        f64::from_bits(1),                     // smallest positive denormal
        f64::from_bits(0x8000_0000_0000_0001), // smallest negative denormal
        f64::MAX,
        f64::MIN,
    ]
}

#[test]
fn special_values_roundtrip_bit_exact() {
    assert_roundtrip_all(&special_values());
}

#[test]
fn empty_stream_roundtrips() {
    assert_roundtrip_all(&[]);
}

#[test]
fn single_value_roundtrips() {
    for v in special_values() {
        assert_roundtrip_all(&[v]);
    }
}

// A strategy that yields arbitrary f64 by bit pattern, so NaN payloads, signed
// zeros, and denormals are all in the sample space (a plain `any::<f64>()`
// would exclude many NaN bit patterns).
fn any_f64() -> impl Strategy<Value = f64> {
    any::<u64>().prop_map(f64::from_bits)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn arbitrary_bitpatterns_roundtrip(values in proptest::collection::vec(any_f64(), 0..600)) {
        assert_roundtrip(&Alp, &values);
        assert_roundtrip(&GcdDeltaFor, &values);
        assert_roundtrip(&Chimp128, &values);
        assert_roundtrip(&ByteSplitZstd, &values);
        assert_roundtrip(&RawF64, &values);
    }

    // Integer- and fixed-decimal streams are the paths ALP and GCD-FOR model
    // (rather than fall back on), so they exercise the exponent-recovery,
    // exception, and GCD-reduction logic that random bit patterns skip. All
    // must still round-trip bit-exactly.
    #[test]
    fn decimal_streams_roundtrip(
        ints in proptest::collection::vec(-1_000_000i64..1_000_000, 1..300),
        places in 0i32..4,
    ) {
        let scale = 10f64.powi(places);
        let values: Vec<f64> = ints.iter().map(|&i| (i as f64) / scale).collect();
        assert_roundtrip(&Alp, &values);
        assert_roundtrip(&GcdDeltaFor, &values);
        assert_roundtrip(&Chimp128, &values);
        assert_roundtrip(&ByteSplitZstd, &values);
        assert_roundtrip(&RawF64, &values);
    }

    // A decimal stream salted with occasional special values (NaN, -0.0, inf):
    // the model fits most, and the special values must survive as ALP
    // exceptions / GCD-FOR raw-fallback, bit-for-bit.
    #[test]
    fn decimal_with_exceptions_roundtrip(
        ints in proptest::collection::vec(-100_000i64..100_000, 2..200),
        specials in proptest::collection::vec(
            prop_oneof![
                Just(f64::NAN),
                Just(-0.0f64),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
                Just(f64::from_bits(1)),
            ],
            0..8,
        ),
    ) {
        let mut values: Vec<f64> = ints.iter().map(|&i| (i as f64) / 100.0).collect();
        for (k, s) in specials.into_iter().enumerate() {
            let idx = k % values.len();
            values[idx] = s;
        }
        assert_roundtrip(&Alp, &values);
        assert_roundtrip(&GcdDeltaFor, &values);
    }

    // Streams built from a tiny alphabet of close values exercise Chimp's
    // window-reuse and back-reference paths far more than uniform random bits.
    #[test]
    fn clustered_values_roundtrip(
        seeds in proptest::collection::vec(any::<u8>(), 1..300)
    ) {
        let base = 0x4059_0000_0000_0000u64; // ~100.0
        let values: Vec<f64> = seeds
            .iter()
            .map(|&s| f64::from_bits(base ^ u64::from(s)))
            .collect();
        assert_roundtrip(&Alp, &values);
        assert_roundtrip(&GcdDeltaFor, &values);
        assert_roundtrip(&Chimp128, &values);
        assert_roundtrip(&ByteSplitZstd, &values);
        assert_roundtrip(&RawF64, &values);
    }

    // Corrupt/truncated input must never panic: for arbitrary bytes and an
    // arbitrary declared count, every decoder returns Ok or a typed error.
    #[test]
    fn corrupt_input_never_panics(
        bytes in proptest::collection::vec(any::<u8>(), 0..2048),
        count in 0usize..800,
    ) {
        let _ = Alp.decode(&bytes, count);
        let _ = GcdDeltaFor.decode(&bytes, count);
        let _ = Chimp128.decode(&bytes, count);
        let _ = ByteSplitZstd.decode(&bytes, count);
        let _ = RawF64.decode(&bytes, count);
    }

    // Truncating a valid encoding must be caught as a typed error (never a
    // wrong-length success and never a panic) for the fixed-width codecs, and
    // must not panic for Chimp.
    #[test]
    fn truncated_valid_stream_is_error(
        values in proptest::collection::vec(any_f64(), 2..200),
        cut in 1usize..40,
    ) {
        for codec_kind in 0..5u8 {
            let encoded = match codec_kind {
                0 => Chimp128.encode(&values),
                1 => ByteSplitZstd.encode(&values),
                2 => RawF64.encode(&values),
                3 => Alp.encode(&values),
                _ => GcdDeltaFor.encode(&values),
            };
            if cut >= encoded.len() {
                continue;
            }
            let truncated = &encoded[..encoded.len() - cut];
            let decoded = match codec_kind {
                0 => Chimp128.decode(truncated, values.len()),
                1 => ByteSplitZstd.decode(truncated, values.len()),
                2 => RawF64.decode(truncated, values.len()),
                3 => Alp.decode(truncated, values.len()),
                _ => GcdDeltaFor.decode(truncated, values.len()),
            };
            // RawF64 loses exactly `cut` bytes so the length can never match.
            if codec_kind == 2 {
                prop_assert!(decoded.is_err(), "raw_f64 accepted a truncated stream");
            }
            // Others must at least not panic (enforced by reaching here); if
            // they do return a value it must still have the right length.
            if let Ok(v) = decoded {
                prop_assert_eq!(v.len(), values.len());
            }
        }
    }
}
