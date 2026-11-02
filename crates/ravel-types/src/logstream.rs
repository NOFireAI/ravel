//! Log stream identity (ADR-0029, docs/log-segment-format.md).
//!
//! A log stream is the canonical hash of the OTLP resource attributes plus
//! scope name, version, and attributes. Per-record attributes never enter
//! identity, which bounds stream cardinality to roughly one per service
//! instance. This mirrors the series-identity discipline (ADR-0005) under its
//! own version domain string `ravel-logstream-v1`.
//!
//! The canonical encoding here is a frozen persistent contract: changing it
//! requires a new domain string (`ravel-logstream-v2`), never an in-place
//! edit.

use std::ops::Range;

/// An OTLP attribute value. Lists and maps nest; maps are canonicalized like
/// the top-level attribute set (entries sorted, values type-tagged).
#[derive(Clone, Debug, PartialEq)]
pub enum AttrValue {
    Str(String),
    I64(i64),
    F64(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    List(Vec<AttrValue>),
    Map(Vec<(String, AttrValue)>),
}

/// 128-bit log stream identity (ADR-0029).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct LogStreamId(pub [u8; 16]);

impl LogStreamId {
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

/// Appends the unsigned LEB128 encoding of `value` to `out`. Local to this
/// module so `ravel-types` stays dependency-light; the encoding matches the
/// protobuf-style varint used everywhere else in the system.
fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

/// Zigzag maps a signed value to an unsigned one (protobuf convention).
fn zigzag(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}

/// Encodes one value: a type tag byte then a payload.
///
/// Type tags (frozen): 1=Str 2=I64 3=F64 4=Bool 5=Bytes 6=List 7=Map.
/// Strings and bytes are LEB128-length-prefixed; I64 is zigzag LEB128; F64 is
/// `to_bits` little-endian (so NaN payloads and -0.0 are significant and
/// distinct); Bool is one byte; List is a LEB128 count then each element;
/// Map is encoded exactly like [`encode_attrs`] (a self-delimiting sorted
/// attribute set).
fn encode_value(out: &mut Vec<u8>, value: &AttrValue) {
    match value {
        AttrValue::Str(s) => {
            out.push(1);
            put_uvarint(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        AttrValue::I64(v) => {
            out.push(2);
            put_uvarint(out, zigzag(*v));
        }
        AttrValue::F64(f) => {
            out.push(3);
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        AttrValue::Bool(b) => {
            out.push(4);
            out.push(u8::from(*b));
        }
        AttrValue::Bytes(b) => {
            out.push(5);
            put_uvarint(out, b.len() as u64);
            out.extend_from_slice(b);
        }
        AttrValue::List(items) => {
            out.push(6);
            put_uvarint(out, items.len() as u64);
            for item in items {
                encode_value(out, item);
            }
        }
        AttrValue::Map(entries) => {
            out.push(7);
            encode_attrs(out, entries);
        }
    }
}

/// Appends the canonical, self-delimiting encoding of an attribute set to
/// `out`: a LEB128 entry count, then each entry as
/// `len(key) key encode_value(value)`, with entries sorted by
/// `(key bytes, encoded value bytes)`.
///
/// Sorting by `(key, encoded value)` (not key alone) keeps identity
/// deterministic even when the input carries duplicate keys: both are kept
/// and ordered stably. The leading count makes the encoding self-delimiting,
/// so concatenating several canonical attribute sets with other length-
/// prefixed fields (as [`log_stream_id`] does) is an injective mapping and
/// cannot alias two distinct logical inputs.
fn encode_attrs(out: &mut Vec<u8>, attrs: &[(String, AttrValue)]) {
    // Every value goes into one scratch buffer; entries remember only their
    // range in it, so a set of scalar values costs a fixed number of
    // allocations however many attributes it holds.
    let mut scratch = Vec::with_capacity(estimated_encoded_len(attrs));
    let mut entries: Vec<(&str, Range<usize>)> = Vec::with_capacity(attrs.len());
    for (key, value) in attrs {
        let start = scratch.len();
        encode_value(&mut scratch, value);
        entries.push((key.as_str(), start..scratch.len()));
    }
    entries.sort_by(|a, b| {
        a.0.as_bytes()
            .cmp(b.0.as_bytes())
            .then_with(|| scratch[a.1.clone()].cmp(&scratch[b.1.clone()]))
    });
    put_uvarint(out, entries.len() as u64);
    for (key, range) in entries {
        put_uvarint(out, key.len() as u64);
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(&scratch[range]);
    }
}

/// A cheap capacity hint for the encoding of `attrs`: exact for strings,
/// a small guess for everything else. It only sizes buffers, never output.
fn estimated_encoded_len(attrs: &[(String, AttrValue)]) -> usize {
    attrs
        .iter()
        .map(|(key, value)| {
            let value_len = match value {
                AttrValue::Str(s) => s.len() + 2,
                AttrValue::Bytes(b) => b.len() + 2,
                _ => 10,
            };
            key.len() + 1 + value_len
        })
        .sum::<usize>()
        + 5
}

/// Canonical byte encoding of an attribute set (frozen contract, ADR-0029).
/// See [`encode_attrs`] for the grammar.
pub fn canonical_attr_bytes(attrs: &[(String, AttrValue)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(estimated_encoded_len(attrs));
    encode_attrs(&mut out, attrs);
    out
}

/// The domain string of the stream identity hash (ADR-0029). Changing it is a
/// format change.
const LOG_STREAM_DOMAIN: &[u8] = b"ravel-logstream-v1";

/// Computes the [`LogStreamId`] from the hash preimage: the first 16 bytes of
/// `blake3("ravel-logstream-v1" || preimage)`. For the preimage
/// `ravel_logseg::stream_attrs_bytes` builds, this equals [`log_stream_id`]
/// for the same resource and scope.
pub fn log_stream_id_from_preimage(preimage: &[u8]) -> LogStreamId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(LOG_STREAM_DOMAIN);
    hasher.update(preimage);
    stream_id_from_digest(&hasher.finalize())
}

fn stream_id_from_digest(digest: &blake3::Hash) -> LogStreamId {
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    LogStreamId(out)
}

/// Computes the canonical [`LogStreamId`] from an OTLP resource+scope
/// (frozen contract, ADR-0029).
///
/// `LogStreamId` is the first 16 bytes of
/// `blake3("ravel-logstream-v1" || canonical(resource_attrs) ||
/// len(scope_name) || scope_name || len(scope_version) || scope_version ||
/// canonical(scope_attrs))`, where `len(..)` is LEB128 and `canonical(..)` is
/// [`canonical_attr_bytes`]. Two batches with the same resource+scope always
/// produce the same id on any writer.
pub fn log_stream_id(
    resource_attrs: &[(String, AttrValue)],
    scope_name: &str,
    scope_version: &str,
    scope_attrs: &[(String, AttrValue)],
) -> LogStreamId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(LOG_STREAM_DOMAIN);
    hasher.update(&canonical_attr_bytes(resource_attrs));
    let mut lenbuf = Vec::new();
    put_uvarint(&mut lenbuf, scope_name.len() as u64);
    hasher.update(&lenbuf);
    hasher.update(scope_name.as_bytes());
    lenbuf.clear();
    put_uvarint(&mut lenbuf, scope_version.len() as u64);
    hasher.update(&lenbuf);
    hasher.update(scope_version.as_bytes());
    hasher.update(&canonical_attr_bytes(scope_attrs));
    stream_id_from_digest(&hasher.finalize())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn s(v: &str) -> AttrValue {
        AttrValue::Str(v.into())
    }

    #[test]
    fn stream_id_is_order_insensitive() {
        let a = vec![("svc".into(), s("api")), ("env".into(), s("prod"))];
        let b = vec![a[1].clone(), a[0].clone()];
        assert_eq!(
            log_stream_id(&a, "s", "1", &[]),
            log_stream_id(&b, "s", "1", &[])
        );
    }

    #[test]
    fn stream_id_distinguishes_types_and_scope() {
        let str_attrs = vec![("n".into(), s("1"))];
        let int_attrs = vec![("n".into(), AttrValue::I64(1))];
        assert_ne!(
            log_stream_id(&str_attrs, "s", "1", &[]),
            log_stream_id(&int_attrs, "s", "1", &[])
        );
        assert_ne!(
            log_stream_id(&str_attrs, "s", "1", &[]),
            log_stream_id(&str_attrs, "s", "2", &[])
        );
        // Scope name and scope attributes are part of identity too.
        assert_ne!(
            log_stream_id(&str_attrs, "s", "1", &[]),
            log_stream_id(&str_attrs, "t", "1", &[])
        );
        assert_ne!(
            log_stream_id(&str_attrs, "s", "1", &[]),
            log_stream_id(&str_attrs, "s", "1", &[("k".into(), s("v"))])
        );
    }

    #[test]
    fn f64_identity_by_bits() {
        let neg_zero = vec![("v".into(), AttrValue::F64(-0.0))];
        let pos_zero = vec![("v".into(), AttrValue::F64(0.0))];
        assert_ne!(
            log_stream_id(&neg_zero, "s", "1", &[]),
            log_stream_id(&pos_zero, "s", "1", &[])
        );
        // A NaN payload is significant.
        let nan_a = vec![(
            "v".into(),
            AttrValue::F64(f64::from_bits(0x7ff8_0000_0000_0001)),
        )];
        let nan_b = vec![(
            "v".into(),
            AttrValue::F64(f64::from_bits(0x7ff8_0000_0000_0002)),
        )];
        assert_ne!(
            log_stream_id(&nan_a, "s", "1", &[]),
            log_stream_id(&nan_b, "s", "1", &[])
        );
    }

    #[test]
    fn nested_list_and_map_are_canonical() {
        let a = vec![(
            "m".into(),
            AttrValue::Map(vec![("b".into(), s("2")), ("a".into(), s("1"))]),
        )];
        let b = vec![(
            "m".into(),
            AttrValue::Map(vec![("a".into(), s("1")), ("b".into(), s("2"))]),
        )];
        assert_eq!(
            log_stream_id(&a, "s", "1", &[]),
            log_stream_id(&b, "s", "1", &[])
        );
        // Lists are ordered, so reordering changes identity.
        let list_ab = vec![("l".into(), AttrValue::List(vec![s("a"), s("b")]))];
        let list_ba = vec![("l".into(), AttrValue::List(vec![s("b"), s("a")]))];
        assert_ne!(
            log_stream_id(&list_ab, "s", "1", &[]),
            log_stream_id(&list_ba, "s", "1", &[])
        );
    }

    /// Fixed inputs for the golden test. The pinned digests below were printed
    /// by this exact function on the base commit (2d10ba4), before
    /// `encode_attrs` was changed.
    fn golden_inputs() -> Vec<(&'static str, Vec<(String, AttrValue)>)> {
        let nested = AttrValue::Map(vec![
            (
                "z".into(),
                AttrValue::List(vec![s("x"), AttrValue::I64(-7)]),
            ),
            ("a".into(), AttrValue::Bytes(vec![0, 255, 1])),
        ]);
        vec![
            ("empty", vec![]),
            (
                "flat",
                vec![
                    ("service.name".into(), s("api")),
                    ("host.name".into(), s("")),
                    ("n".into(), AttrValue::I64(i64::MIN)),
                    ("ok".into(), AttrValue::Bool(true)),
                ],
            ),
            (
                "duplicate_keys",
                vec![
                    ("k".into(), s("b")),
                    ("k".into(), s("a")),
                    ("k".into(), AttrValue::I64(3)),
                    ("j".into(), s("a")),
                ],
            ),
            (
                "floats_and_nested",
                vec![
                    ("neg_zero".into(), AttrValue::F64(-0.0)),
                    (
                        "nan".into(),
                        AttrValue::F64(f64::from_bits(0x7ff8_0000_0000_0001)),
                    ),
                    ("m".into(), nested),
                    ("longer".into(), s("value")),
                    ("long".into(), s("value")),
                ],
            ),
        ]
    }

    /// (name, blake3 of `canonical_attr_bytes`, `log_stream_id` bytes for the
    /// input as resource attributes with scope "scope", "1.0" and no scope
    /// attributes), as printed on the base commit.
    const GOLDEN: &[(&str, &str, &str)] = &[
        (
            "empty",
            "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213",
            "305e0d48bcb4f4a9f22f8cfecbba8d9f",
        ),
        (
            "flat",
            "91e0aa735c3be8c3d0ed77064734444d766480196e4ca3da766a530b4b92af91",
            "3d082f71b9354f90abcd07ed90c5a2f1",
        ),
        (
            "duplicate_keys",
            "5f92c98f9df2c41a88edbb65f226cbe70fd7124ae84c9e9579e50a1d4abfbc40",
            "ec9bb286682cd52e62a1d9ba37410c3c",
        ),
        (
            "floats_and_nested",
            "7ce413c8f0fe0a662e5edf4ba9c8aa513855a0ad8539a37325bf0f3723d1bd07",
            "9e32f30685b12d0575e0af2c53008940",
        ),
    ];

    #[test]
    fn golden_canonical_bytes_and_stream_ids() {
        let inputs = golden_inputs();
        assert_eq!(GOLDEN.len(), inputs.len());
        for ((name, attrs), (gname, gdigest, gid)) in inputs.iter().zip(GOLDEN) {
            assert_eq!(name, gname);
            assert_eq!(
                blake3::hash(&canonical_attr_bytes(attrs)).to_hex().as_str(),
                *gdigest,
                "{name}: canonical bytes"
            );
            assert_eq!(
                log_stream_id(attrs, "scope", "1.0", &[]).to_hex(),
                *gid,
                "{name}: stream id"
            );
        }
    }

    /// Test-only copy of the encoder before the scratch-buffer change.
    fn legacy_encode_value(out: &mut Vec<u8>, value: &AttrValue) {
        match value {
            AttrValue::Map(entries) => {
                out.push(7);
                legacy_encode_attrs(out, entries);
            }
            AttrValue::List(items) => {
                out.push(6);
                put_uvarint(out, items.len() as u64);
                for item in items {
                    legacy_encode_value(out, item);
                }
            }
            // Scalar arms never reach encode_attrs, so the shared encoder is
            // the same code as the old one for them.
            other => encode_value(out, other),
        }
    }

    fn legacy_encode_attrs(out: &mut Vec<u8>, attrs: &[(String, AttrValue)]) {
        let mut items: Vec<(&str, Vec<u8>)> = attrs
            .iter()
            .map(|(k, v)| {
                let mut encoded = Vec::new();
                legacy_encode_value(&mut encoded, v);
                (k.as_str(), encoded)
            })
            .collect();
        items.sort_by(|a, b| {
            a.0.as_bytes()
                .cmp(b.0.as_bytes())
                .then_with(|| a.1.cmp(&b.1))
        });
        put_uvarint(out, items.len() as u64);
        for (key, encoded) in items {
            put_uvarint(out, key.len() as u64);
            out.extend_from_slice(key.as_bytes());
            out.extend_from_slice(&encoded);
        }
    }

    /// Nested values to depth 3, a small key alphabet so duplicate keys with
    /// differing values are common, empty strings, NaN payloads and -0.0.
    fn arb_deep_value() -> impl Strategy<Value = AttrValue> {
        let leaf = prop_oneof![
            prop_oneof![Just(String::new()), "[a-c]{0,3}"].prop_map(AttrValue::Str),
            any::<i64>().prop_map(AttrValue::I64),
            prop_oneof![
                Just(-0.0f64),
                Just(0.0f64),
                any::<u64>().prop_map(f64::from_bits),
                (0u64..4).prop_map(|p| f64::from_bits(0x7ff8_0000_0000_0000 | p)),
            ]
            .prop_map(AttrValue::F64),
            any::<bool>().prop_map(AttrValue::Bool),
            proptest::collection::vec(any::<u8>(), 0..4).prop_map(AttrValue::Bytes),
        ];
        leaf.prop_recursive(3, 24, 4, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..4).prop_map(AttrValue::List),
                proptest::collection::vec(("[a-c]{0,2}", inner), 0..5).prop_map(AttrValue::Map),
            ]
        })
    }

    proptest! {
        #[test]
        fn encode_attrs_matches_the_legacy_encoder(
            attrs in proptest::collection::vec(("[a-c]{0,2}", arb_deep_value()), 0..12),
        ) {
            let mut legacy = Vec::new();
            legacy_encode_attrs(&mut legacy, &attrs);
            prop_assert_eq!(canonical_attr_bytes(&attrs), legacy);
        }

        #[test]
        fn preimage_id_matches_log_stream_id(
            resource in proptest::collection::vec(("[a-c]{0,2}", arb_deep_value()), 0..6),
            scope in proptest::collection::vec(("[a-c]{0,2}", arb_deep_value()), 0..6),
        ) {
            let mut preimage = canonical_attr_bytes(&resource);
            put_uvarint(&mut preimage, 1);
            preimage.extend_from_slice(b"n");
            put_uvarint(&mut preimage, 2);
            preimage.extend_from_slice(b"v1");
            preimage.extend_from_slice(&canonical_attr_bytes(&scope));
            prop_assert_eq!(
                log_stream_id_from_preimage(&preimage),
                log_stream_id(&resource, "n", "v1", &scope)
            );
        }
    }

    // A small strategy for arbitrary flat attribute sets.
    fn arb_value() -> impl Strategy<Value = AttrValue> {
        prop_oneof![
            ".*".prop_map(AttrValue::Str),
            any::<i64>().prop_map(AttrValue::I64),
            any::<u64>().prop_map(|b| AttrValue::F64(f64::from_bits(b))),
            any::<bool>().prop_map(AttrValue::Bool),
            proptest::collection::vec(any::<u8>(), 0..8).prop_map(AttrValue::Bytes),
        ]
    }

    fn arb_attrs() -> impl Strategy<Value = Vec<(String, AttrValue)>> {
        proptest::collection::vec(("[a-z]{1,4}", arb_value()), 0..8)
    }

    proptest! {
        #[test]
        fn permutation_invariant_and_change_sensitive(
            attrs in arb_attrs(),
            seed in any::<u64>(),
        ) {
            // Any permutation yields the same id.
            let mut permuted = attrs.clone();
            if permuted.len() > 1 {
                let rot = (seed as usize) % permuted.len();
                permuted.rotate_left(rot);
            }
            prop_assert_eq!(
                log_stream_id(&attrs, "svc", "1", &[]),
                log_stream_id(&permuted, "svc", "1", &[])
            );

            // Appending a fresh key changes the id (new distinct key name).
            let mut extended = attrs.clone();
            extended.push(("zzzz_unique".into(), AttrValue::I64(seed as i64)));
            prop_assert_ne!(
                log_stream_id(&attrs, "svc", "1", &[]),
                log_stream_id(&extended, "svc", "1", &[])
            );
        }
    }
}
