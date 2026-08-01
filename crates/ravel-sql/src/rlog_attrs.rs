//! The merged-`attrs` decode shared by every RLOG-backed table
//! (`logs` [`crate::logs_scan`], `alerts` [`crate::alerts_scan`], and `audit`
//! [`crate::audit_scan`]).
//!
//! All three signals ride RLOG's format verbatim (ADR-0040), so all three
//! expose their records' attributes through one `Map(Utf8, Utf8)` column built
//! the same way: each record's stream-identity (resource + scope) attributes
//! merged with its dynamic per-record attributes, the record winning on a key
//! collision, values rendered to text.
//!
//! This is the sole owner of that decode. Every table that materializes an
//! `attrs` column funnels through [`merged_attrs`] so one record produces the
//! same map regardless of which table read it, and a corrupt `stream_attrs`
//! blob surfaces as one client-visible class ([`SqlError::CorruptStreamAttrs`],
//! `MSG_CORRUPT`) rather than a different one per table.

use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use ravel_logseg::LogRecord;
use ravel_types::logstream::{AttrValue, canonical_attr_bytes};

use crate::error::SqlError;

/// Depth cap when walking a record's canonical resource/scope blob for the
/// merged-`attrs` decode (mirrors the reader's `MAX_ATTR_DEPTH`), so hostile
/// nesting cannot exhaust the stack.
const MAX_ATTR_DEPTH: u32 = 32;

/// Entry-count cap per attribute set when walking the blob, matching the
/// reader's own cap so a corrupt count is rejected rather than allocated on.
const MAX_ATTR_ENTRIES: u64 = 1 << 20;

/// The `attrs` column contents for one record: its decoded stream-identity
/// (resource + scope) attributes overlaid with its dynamic per-record
/// attributes, the record's value winning on a key collision. Callers that
/// promote well-known keys into typed columns (alerts' `alert_id`/`rule_id`/
/// `state`/`generation`) read them out of this same merged view with
/// [`find_attr`], so a promoted column and the `attrs` map never disagree.
pub(crate) fn merged_attrs(r: &LogRecord) -> DFResult<Vec<(String, AttrValue)>> {
    let mut merged = decode_stream_attrs(&r.stream_attrs)?;
    for (k, v) in &r.attrs {
        if let Some(slot) = merged.iter_mut().find(|(mk, _)| mk == k) {
            slot.1 = v.clone();
        } else {
            merged.push((k.clone(), v.clone()));
        }
    }
    Ok(merged)
}

/// Look up one key in a [`merged_attrs`] result, for tables that promote a
/// well-known attribute into a typed column. Returns the first match in blob
/// order (record attributes have already overwritten a colliding
/// resource/scope value in place, so first-match is the record-wins value).
pub(crate) fn find_attr<'a>(merged: &'a [(String, AttrValue)], key: &str) -> Option<&'a AttrValue> {
    merged.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// v1 stringification of a dynamic attribute value for a `Map(Utf8, Utf8)`
/// `attrs` column. Scalar values render to their natural text; `Bytes`, `List`,
/// and `Map` render to the lowercase hex of their canonical encoding, a
/// deterministic, injective form pending a richer typed column.
pub(crate) fn attr_value_to_string(v: &AttrValue) -> String {
    match v {
        AttrValue::Str(s) => s.clone(),
        AttrValue::I64(i) => i.to_string(),
        AttrValue::F64(f) => f.to_string(),
        AttrValue::Bool(b) => b.to_string(),
        AttrValue::Bytes(b) => hex_lower(b),
        AttrValue::List(_) | AttrValue::Map(_) => hex_lower(&canonical_attr_bytes(
            std::slice::from_ref(&(String::new(), v.clone())),
        )),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String never fails.
        let _ = write!(out, "{b:02x}");
    }
    out
}

// --- `attrs` column contents: merged resource/scope + record attributes ---

/// Decode the **top-level** resource and scope attribute entries of a
/// `stream_attrs` blob (a record's [`ravel_logseg::LogRecord::stream_attrs`],
/// the canonical `resource ++ scope-name ++ scope-version ++ scope-attrs`
/// bytes) into `(key, value)` pairs, in blob order (resource set first, then the
/// scope attribute set). The scope name and version are length-prefixed
/// *positional* fields, not key-value entries, so they are skipped over and
/// never become synthetic `scope.name`/`scope.version` keys.
///
/// A top-level entry whose value is a nested `Map` or `List` is walked past
/// (consumed, so decoding stays in frame) but **omitted** from the returned
/// pairs: a resource/scope attribute whose value is itself a map or list is not
/// projected into the map column (a richer typed representation is a v-next
/// refinement). Per-record dynamic attributes with nested values are unaffected
/// -- they are merged in verbatim by [`merged_attrs`] and rendered by
/// [`attr_value_to_string`].
fn decode_stream_attrs(blob: &[u8]) -> DFResult<Vec<(String, AttrValue)>> {
    let mut pos = 0usize;
    let mut out = Vec::new();
    // Resource attribute set.
    decode_attr_set(blob, &mut pos, 0, &mut out)?;
    // Scope name and scope version, each a length-prefixed string (positional).
    skip_len_prefixed(blob, &mut pos)?;
    skip_len_prefixed(blob, &mut pos)?;
    // Scope attribute set.
    decode_attr_set(blob, &mut pos, 0, &mut out)?;
    Ok(out)
}

/// Walk one canonical attribute set from `pos`, advancing `pos` to its end, and
/// push every top-level scalar entry as a decoded `(key, value)` pair onto
/// `out`. A nested `Map`/`List` value is consumed but not pushed (see
/// [`decode_stream_attrs`]).
fn decode_attr_set(
    buf: &[u8],
    pos: &mut usize,
    depth: u32,
    out: &mut Vec<(String, AttrValue)>,
) -> DFResult<()> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let count = read_uvarint(buf, pos)?;
    if count > MAX_ATTR_ENTRIES {
        return Err(corrupt("stream_attrs entry count over cap"));
    }
    for _ in 0..count {
        let klen = usize_of(read_uvarint(buf, pos)?)?;
        let kstart = *pos;
        advance(buf, pos, klen)?;
        let kbytes = &buf[kstart..*pos];
        if let Some(value) = decode_value(buf, pos, depth + 1)? {
            let key = std::str::from_utf8(kbytes)
                .map_err(|_| corrupt("stream_attrs key not utf-8"))?
                .to_string();
            out.push((key, value));
        }
    }
    Ok(())
}

/// Decode one encoded attribute value at `pos` (frozen grammar,
/// `ravel_types::logstream`: 1=Str 2=I64 3=F64 4=Bool 5=Bytes 6=List 7=Map),
/// advancing `pos` past it. Returns the decoded scalar, or `None` for a
/// `List`/`Map`, which is consumed via the [`skip_value`]/[`walk_attr_set`]
/// walkers but not decoded into an entry.
fn decode_value(buf: &[u8], pos: &mut usize, depth: u32) -> DFResult<Option<AttrValue>> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let tag = read_u8(buf, pos)?;
    let value = match tag {
        // Str: length-prefixed UTF-8 payload.
        1 => {
            let len = usize_of(read_uvarint(buf, pos)?)?;
            let start = *pos;
            advance(buf, pos, len)?;
            let s = std::str::from_utf8(&buf[start..*pos])
                .map_err(|_| corrupt("stream_attrs str not utf-8"))?
                .to_string();
            Some(AttrValue::Str(s))
        }
        // I64: a single zigzag varint.
        2 => Some(AttrValue::I64(unzigzag(read_uvarint(buf, pos)?))),
        // F64: eight little-endian bytes of `to_bits` (NaN payloads / -0.0
        // preserved by decoding through the bit pattern).
        3 => {
            let start = *pos;
            advance(buf, pos, 8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[start..*pos]);
            Some(AttrValue::F64(f64::from_bits(u64::from_le_bytes(b))))
        }
        // Bool: one byte.
        4 => Some(AttrValue::Bool(read_u8(buf, pos)? != 0)),
        // Bytes: length-prefixed payload.
        5 => {
            let len = usize_of(read_uvarint(buf, pos)?)?;
            let start = *pos;
            advance(buf, pos, len)?;
            Some(AttrValue::Bytes(buf[start..*pos].to_vec()))
        }
        // List: a count then each element; consumed, not decoded.
        6 => {
            let n = read_uvarint(buf, pos)?;
            for _ in 0..n {
                skip_value(buf, pos, depth + 1)?;
            }
            None
        }
        // Map: a nested canonical attribute set; consumed, not decoded.
        7 => {
            walk_attr_set(buf, pos, depth + 1)?;
            None
        }
        _ => return Err(corrupt("bad stream_attrs value tag")),
    };
    Ok(value)
}

/// Inverse of the writer's zigzag mapping (`ravel_types::logstream`): recover a
/// signed integer from its unsigned LEB128 zigzag form.
fn unzigzag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// Walk one canonical attribute set from `pos`, advancing `pos` to its end,
/// consuming every entry without decoding it. Used to skip past a nested `Map`
/// value while staying byte-aligned.
fn walk_attr_set(buf: &[u8], pos: &mut usize, depth: u32) -> DFResult<()> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let count = read_uvarint(buf, pos)?;
    if count > MAX_ATTR_ENTRIES {
        return Err(corrupt("stream_attrs entry count over cap"));
    }
    for _ in 0..count {
        let klen = usize_of(read_uvarint(buf, pos)?)?;
        advance(buf, pos, klen)?;
        skip_value(buf, pos, depth + 1)?;
    }
    Ok(())
}

/// Advance `pos` past one encoded attribute value (frozen grammar,
/// `ravel_types::logstream`: 1=Str 2=I64 3=F64 4=Bool 5=Bytes 6=List 7=Map).
fn skip_value(buf: &[u8], pos: &mut usize, depth: u32) -> DFResult<()> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let tag = read_u8(buf, pos)?;
    match tag {
        // Str / Bytes: length-prefixed payload.
        1 | 5 => {
            let len = usize_of(read_uvarint(buf, pos)?)?;
            advance(buf, pos, len)?;
        }
        // I64: a single zigzag varint.
        2 => {
            read_uvarint(buf, pos)?;
        }
        // F64: eight little-endian bytes.
        3 => advance(buf, pos, 8)?,
        // Bool: one byte.
        4 => advance(buf, pos, 1)?,
        // List: a count then each element.
        6 => {
            let n = read_uvarint(buf, pos)?;
            for _ in 0..n {
                skip_value(buf, pos, depth + 1)?;
            }
        }
        // Map: a nested canonical attribute set.
        7 => {
            walk_attr_set(buf, pos, depth + 1)?;
        }
        _ => return Err(corrupt("bad stream_attrs value tag")),
    }
    Ok(())
}

/// Skip a length-prefixed string (the scope name / version fields).
fn skip_len_prefixed(buf: &[u8], pos: &mut usize) -> DFResult<()> {
    let len = usize_of(read_uvarint(buf, pos)?)?;
    advance(buf, pos, len)
}

fn read_u8(buf: &[u8], pos: &mut usize) -> DFResult<u8> {
    let b = *buf
        .get(*pos)
        .ok_or_else(|| corrupt("stream_attrs truncated"))?;
    *pos += 1;
    Ok(b)
}

fn advance(buf: &[u8], pos: &mut usize, n: usize) -> DFResult<()> {
    let end = pos
        .checked_add(n)
        .ok_or_else(|| corrupt("stream_attrs length overflow"))?;
    if end > buf.len() {
        return Err(corrupt("stream_attrs truncated"));
    }
    *pos = end;
    Ok(())
}

/// Read one unsigned LEB128 varint, rejecting an over-long encoding rather than
/// looping or overflowing.
fn read_uvarint(buf: &[u8], pos: &mut usize) -> DFResult<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        if shift >= 64 {
            return Err(corrupt("stream_attrs varint overflow"));
        }
        let byte = read_u8(buf, pos)?;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

fn usize_of(v: u64) -> DFResult<usize> {
    usize::try_from(v).map_err(|_| corrupt("stream_attrs length exceeds usize"))
}

fn corrupt(what: &str) -> DataFusionError {
    // A malformed blob here means a record we decoded carried corrupt canonical
    // stream_attrs bytes: the same data-integrity fault the fetcher reports as
    // `LogFetchError::Corrupt`, just detected one layer up. Surface it with the
    // identical client class/message (`MSG_CORRUPT`, `ErrorClass::Unavailable`)
    // via `SqlError::CorruptStreamAttrs`, not a distinct internal-error class,
    // so one underlying fault never maps to two client-visible classes. Never a
    // panic or a silently-wrong filter result.
    SqlError::CorruptStreamAttrs(what.to_string()).into()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use ravel_logseg::stream_attrs_bytes;

    use super::*;

    fn s(v: &str) -> AttrValue {
        AttrValue::Str(v.into())
    }

    #[test]
    fn decode_yields_top_level_resource_and_scope_scalars() {
        let blob = stream_attrs_bytes(
            &[
                ("service.name".into(), s("api")),
                ("port".into(), AttrValue::I64(8080)),
            ],
            "libscope",
            "2.1",
            &[("lib".into(), s("otel"))],
        );
        let got = decode_stream_attrs(&blob).unwrap();
        assert!(got.contains(&("service.name".to_string(), s("api"))));
        assert!(got.contains(&("port".to_string(), AttrValue::I64(8080))));
        assert!(got.contains(&("lib".to_string(), s("otel"))));
        // Scope name/version are positional, never synthesized into keys or values.
        assert!(
            !got.iter()
                .any(|(k, _)| k == "scope.name" || k == "scope.version")
        );
        assert!(
            !got.iter()
                .any(|(_, v)| matches!(v, AttrValue::Str(x) if x == "libscope" || x == "2.1"))
        );
    }

    #[test]
    fn decode_omits_nested_map_and_list_but_stays_in_frame() {
        let blob = stream_attrs_bytes(
            &[
                (
                    "k8s.labels".into(),
                    AttrValue::Map(vec![("service.name".into(), s("api"))]),
                ),
                ("tags".into(), AttrValue::List(vec![s("a"), s("b")])),
                ("host".into(), s("h1")),
            ],
            "s",
            "1",
            &[],
        );
        // Nested map/list top-level entries are consumed but omitted; the scalar
        // sibling still decodes, proving the walk stayed byte-aligned.
        assert_eq!(
            decode_stream_attrs(&blob).unwrap(),
            vec![("host".into(), s("h1"))]
        );
    }

    #[test]
    fn decode_roundtrips_scalar_types() {
        let blob = stream_attrs_bytes(
            &[
                ("b".into(), AttrValue::Bool(true)),
                ("f".into(), AttrValue::F64(-0.0)),
                ("by".into(), AttrValue::Bytes(vec![1, 2, 3])),
                ("i".into(), AttrValue::I64(-42)),
            ],
            "s",
            "1",
            &[],
        );
        let got: std::collections::BTreeMap<_, _> =
            decode_stream_attrs(&blob).unwrap().into_iter().collect();
        assert_eq!(got["b"], AttrValue::Bool(true));
        assert_eq!(got["by"], AttrValue::Bytes(vec![1, 2, 3]));
        assert_eq!(got["i"], AttrValue::I64(-42));
        // -0.0 preserved through the bit pattern (writer's f64::to_bits discipline).
        match &got["f"] {
            AttrValue::F64(x) => assert_eq!(x.to_bits(), (-0.0f64).to_bits()),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_truncated_blob_as_corrupt() {
        let blob = stream_attrs_bytes(&[("k".into(), s("v"))], "s", "1", &[]);
        // Chop the last byte: the value payload is now truncated.
        let err = decode_stream_attrs(&blob[..blob.len() - 1]).unwrap_err();
        // Surfaces as the shared corruption class, not a panic or wrong data.
        let sql = match err {
            DataFusionError::External(b) => b.downcast::<SqlError>().expect("SqlError"),
            other => panic!("expected External, got {other:?}"),
        };
        assert_eq!(sql.class(), crate::error::ErrorClass::Unavailable);
        assert_eq!(sql.client_message(), crate::error::MSG_CORRUPT);
    }

    #[test]
    fn merged_attrs_record_wins_collision_and_keeps_resource() {
        let resource = [("service.name".into(), s("api")), ("host".into(), s("h1"))];
        let r = LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&resource, "sc", "1", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "sc", "1", &[]),
            ts_ns: 1,
            observed_ts_ns: 1,
            severity_num: 0,
            severity_text: String::new(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![
                ("service.name".into(), s("override")),
                ("dyn".into(), s("v")),
            ],
        };
        let merged = merged_attrs(&r).unwrap();
        // Exactly one service.name entry (no duplicate key from the merge).
        assert_eq!(
            merged.iter().filter(|(k, _)| k == "service.name").count(),
            1
        );
        // find_attr returns the record-wins value.
        assert_eq!(find_attr(&merged, "service.name"), Some(&s("override")));
        assert_eq!(find_attr(&merged, "host"), Some(&s("h1")));
        assert_eq!(find_attr(&merged, "dyn"), Some(&s("v")));
        assert_eq!(find_attr(&merged, "absent"), None);
    }
}
