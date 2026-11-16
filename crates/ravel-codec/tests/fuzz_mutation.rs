//! Mutation fuzzing for the KEY_IDX section reader (ADR-2707 decision 1).
//!
//! Runs on the pinned stable toolchain (proptest only, no cargo-fuzz). Case
//! count honours `PROPTEST_CASES`; the CI fuzz job raises it.
//!
//! Two input sources: arbitrary bytes, and valid writer output with one
//! mutation applied (a bit flip, a byte overwrite, an overwrite of up to four
//! consecutive bytes, or a truncation). Neither `probe` nor `verify_key_idx`
//! may panic on any of them, and any `Ok` they return must be the right one.
//!
//! Every byte of writer output sits under a crc32c (header, each directory,
//! each frame's zstd payload) or is a frame `entry_count`, which the exact
//! decompressed-length check covers. crc32c detects every burst of 32 bits or
//! fewer, so for any of these mutations `verify_key_idx`, which reads the
//! whole section, must fail. `probe` legitimately ignores a mutation that
//! lands in a part of the section it never reads: another field's directory,
//! or a bucket frame other than the probed key's (of the probed field or of
//! another field). There it must return exactly the unmutated answer; a
//! mutation anywhere it does read must make it fail.

#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_codec::key_idx::{
    Key8, KeyIdxFieldInput, KeyType, encode_key_idx_with_bucket_bits, key8_i64, key8_str, probe,
    verify_key_idx,
};

#[derive(Debug, Clone)]
enum Mutation {
    FlipBit { at: usize, bit: u8 },
    SetByte { at: usize, value: u8 },
    Overwrite { at: usize, bytes: Vec<u8> },
    Truncate { len: usize },
}

fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (any::<usize>(), 0u8..8).prop_map(|(at, bit)| Mutation::FlipBit { at, bit }),
        (any::<usize>(), any::<u8>()).prop_map(|(at, value)| Mutation::SetByte { at, value }),
        (any::<usize>(), prop::collection::vec(any::<u8>(), 1..=4))
            .prop_map(|(at, bytes)| Mutation::Overwrite { at, bytes }),
        any::<usize>().prop_map(|len| Mutation::Truncate { len }),
    ]
}

fn mutate(section: &[u8], m: &Mutation) -> Vec<u8> {
    let mut out = section.to_vec();
    if out.is_empty() {
        return out;
    }
    let n = out.len();
    match m {
        Mutation::FlipBit { at, bit } => out[at % n] ^= 1 << bit,
        Mutation::SetByte { at, value } => out[at % n] = *value,
        Mutation::Overwrite { at, bytes } => {
            let start = at % n;
            for (slot, b) in out[start..].iter_mut().zip(bytes) {
                *slot = *b;
            }
        }
        Mutation::Truncate { len } => out.truncate(len % n),
    }
    out
}

/// Two I64 fields and one Str field, with keys drawn from a small domain so
/// buckets hold several keys and probed keys are often present.
fn seed_fields() -> impl Strategy<Value = Vec<KeyIdxFieldInput>> {
    let i64_entries = prop::collection::vec((0i64..40, 0u32..24), 0..40);
    let str_entries = prop::collection::vec(("[a-d]{1,2}", 0u32..24), 0..20);
    (i64_entries.clone(), i64_entries, str_entries).prop_map(|(a, b, c)| {
        vec![
            KeyIdxFieldInput {
                name: "a".into(),
                key_type: KeyType::I64,
                entries: a.into_iter().map(|(k, blk)| (key8_i64(k), blk)).collect(),
            },
            KeyIdxFieldInput {
                name: "b".into(),
                key_type: KeyType::I64,
                entries: b.into_iter().map(|(k, blk)| (key8_i64(k), blk)).collect(),
            },
            KeyIdxFieldInput {
                name: "s".into(),
                key_type: KeyType::Str,
                entries: c.into_iter().map(|(k, blk)| (key8_str(&k), blk)).collect(),
            },
        ]
    })
}

fn probed_field() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("a"), Just("b"), Just("s"), Just("zz")]
}

fn key_type() -> impl Strategy<Value = KeyType> {
    prop_oneof![
        Just(KeyType::Id16),
        Just(KeyType::I64),
        Just(KeyType::Str),
        Just(KeyType::Bytes),
    ]
}

/// The type the seed fields were written under, so a probe is answered.
fn written_type(field: &str) -> KeyType {
    if field == "s" {
        KeyType::Str
    } else {
        KeyType::I64
    }
}

proptest! {
    #[test]
    fn arbitrary_bytes_never_panic(
        bytes in prop::collection::vec(any::<u8>(), 0..512),
        prefix_len in 0u32..520,
        field in probed_field(),
        probed_type in key_type(),
        key in any::<[u8; 8]>(),
    ) {
        let verified = verify_key_idx(&bytes, prefix_len);
        let probed = probe(&bytes, prefix_len, field, probed_type, &key);
        if let (Ok(summary), Ok(found)) = (&verified, &probed) {
            // A section that verifies must answer consistently with itself:
            // a probe answers only for a field named under the probed type.
            let named = summary
                .fields
                .iter()
                .find(|f| f.name == field && f.key_type == probed_type);
            prop_assert_eq!(named.is_some(), found.is_some());
            if let (Some(f), Some(blocks)) = (named, found) {
                prop_assert!(blocks.windows(2).all(|w| w[0] < w[1]));
                prop_assert!(blocks.len() as u64 <= f.entry_count);
            }
        }
    }

    #[test]
    fn mutated_sections_fail_or_answer_unchanged(
        fields in seed_fields(),
        bucket_bits in prop_oneof![Just(1u8), Just(2u8), Just(8u8)],
        m in mutation(),
        field in probed_field(),
        probe_key in 0i64..48,
    ) {
        let section = encode_key_idx_with_bucket_bits(&fields, bucket_bits).expect("encode");
        let key: Key8 = if field == "s" { key8_str("ab") } else { key8_i64(probe_key) };
        let kt = written_type(field);
        let want = probe(&section.bytes, section.prefix_len, field, kt, &key).expect("probe clean");
        verify_key_idx(&section.bytes, section.prefix_len).expect("verify clean");
        let other = if kt == KeyType::I64 { KeyType::Str } else { KeyType::I64 };
        let declined = probe(&section.bytes, section.prefix_len, field, other, &key);
        prop_assert_eq!(declined.expect("probe clean"), None);

        let bytes = mutate(&section.bytes, &m);
        let got = probe(&bytes, section.prefix_len, field, kt, &key);
        let verified = verify_key_idx(&bytes, section.prefix_len);
        if bytes == section.bytes {
            prop_assert_eq!(got.expect("unchanged section probes"), want);
            prop_assert!(verified.is_ok());
        } else {
            prop_assert!(verified.is_err(), "mutation {:?} verified", m);
            if let Ok(found) = got {
                prop_assert_eq!(found, want, "mutation {:?} changed the answer", m);
            }
        }
    }

    #[test]
    fn a_footer_prefix_len_other_than_the_written_one_fails(
        fields in seed_fields(),
        delta in prop_oneof![1i64..=4, -4i64..=-1, Just(i64::from(u32::MAX))],
        field in probed_field(),
        probe_key in 0i64..48,
    ) {
        let section = encode_key_idx_with_bucket_bits(&fields, 2).expect("encode");
        let wrong = u32::try_from((i64::from(section.prefix_len) + delta).rem_euclid(1 << 32))
            .expect("u32");
        let key = key8_i64(probe_key);
        prop_assert!(probe(&section.bytes, wrong, field, written_type(field), &key).is_err());
        prop_assert!(verify_key_idx(&section.bytes, wrong).is_err());
    }
}
