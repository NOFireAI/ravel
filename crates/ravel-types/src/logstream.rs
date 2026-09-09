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
    let mut items: Vec<(&str, Vec<u8>)> = attrs
        .iter()
        .map(|(k, v)| {
            let mut encoded = Vec::new();
            encode_value(&mut encoded, v);
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

/// Canonical byte encoding of an attribute set (frozen contract, ADR-0029).
/// See [`encode_attrs`] for the grammar.
pub fn canonical_attr_bytes(attrs: &[(String, AttrValue)]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_attrs(&mut out, attrs);
    out
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
    hasher.update(b"ravel-logstream-v1");
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
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    LogStreamId(out)
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
