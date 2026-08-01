//! Span record model, fixed column ids, and the merged-attrs map codec
//! (docs/span-segment-format.md).
//!
//! [`SpanRecord`] is the caller-facing record: one row per span. Every span
//! carries a single `attrs` map (`Map<Utf8, Utf8>`) that already merges the
//! resource, scope, and span attribute sets; [`merge_attrs`] builds that map
//! following the same resource+scope-wins-over-record convention
//! `docs/log-segment-format.md` documents for logs. The map is stored per row
//! as one canonical blob ([`encode_attrs`]) in the fixed `attrs` column, so no
//! FIELD_DIR-style per-key column directory is needed (ADR-0041 leanness: v1 has
//! no attr-level pruning or content search, so a single map column round-trips
//! the attrs exactly with the least machinery).

use std::collections::BTreeMap;

use crate::error::SpanSegError;
use crate::varint::{get_uvarint, put_uvarint};

// Fixed column ids (docs/span-segment-format.md BLOCKS). RSPAN has no dynamic
// attribute columns, so these are the entire column set: the block header lists
// only these ids, always in ascending order.
pub const COL_TRACE_ID: u32 = 0;
pub const COL_SPAN_ID: u32 = 1;
pub const COL_PARENT_SPAN_ID: u32 = 2;
pub const COL_NAME: u32 = 3;
pub const COL_START_TS: u32 = 4;
pub const COL_END_TS: u32 = 5;
pub const COL_STATUS_CODE: u32 = 6;
pub const COL_STATUS_MESSAGE: u32 = 7;
pub const COL_ATTRS: u32 = 8;

/// Fixed byte width of a trace id.
pub const TRACE_ID_WIDTH: usize = 16;
/// Fixed byte width of a span id (also a parent span id).
pub const SPAN_ID_WIDTH: usize = 8;

/// The maximum number of attribute pairs [`decode_attrs`] will accept from one
/// stored blob (untrusted-input guard). A real span never approaches this.
const MAX_ATTR_PAIRS: u64 = 1 << 20;

/// OTLP span status code (`opentelemetry.proto.trace.v1.Status.StatusCode`).
/// Stored as one byte in the `status_code` column.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum StatusCode {
    /// `STATUS_CODE_UNSET`: the default; no explicit status was set.
    Unset = 0,
    /// `STATUS_CODE_OK`: the operation was validated by the application as
    /// having completed successfully.
    Ok = 1,
    /// `STATUS_CODE_ERROR`: the operation contains an error.
    Error = 2,
}

impl StatusCode {
    /// Maps a stored byte to a [`StatusCode`]; an unknown byte is the caller's
    /// to reject as `Corrupted`.
    pub fn from_u8(v: u8) -> Option<StatusCode> {
        Some(match v {
            0 => StatusCode::Unset,
            1 => StatusCode::Ok,
            2 => StatusCode::Error,
            _ => return None,
        })
    }

    /// The stored status byte.
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// A single span record as handed to the writer, one row per span.
///
/// `trace_id` is the primary sort/lookup key (ADR-0041): the writer sorts
/// records by `(trace_id, start_ts_ns)`. `parent_span_id` is `None` for a root
/// span. `start_ts_ns`/`end_ts_ns` are both required event timestamps (ns);
/// pruning is an interval-overlap test over `[start, end]`, not a point test.
/// `attrs` is the already-merged resource+scope+span map (see [`merge_attrs`]);
/// span events and links are out of scope for v1 and, if decoded at all, belong
/// in `attrs` as an opaque blob value (e.g. `"_events_raw"`), never as columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
    pub status_code: StatusCode,
    pub status_message: Option<String>,
    /// The merged attribute map. Keys are unique; the writer canonicalizes to
    /// ascending key order, so two records with the same logical map produce
    /// byte-identical `attrs` columns.
    pub attrs: Vec<(String, String)>,
}

/// Merges resource, scope, and span attribute sets into one map following the
/// resource+scope-wins-over-record convention `docs/log-segment-format.md`
/// documents for logs (reused, not redesigned): on a key collision the
/// resource/scope value wins over the span-level value, and resource wins over
/// scope. The result is sorted ascending by key with unique keys.
///
/// This is a small helper rather than an import from `ravel-logseg`: logs keep
/// resource+scope in a separate identity blob and never merge them into one
/// record-level map, so there is no generic merge to reuse across the crate
/// boundary (ADR-0041 deliverable 5); the rule itself is what is reused.
pub fn merge_attrs(
    resource: &[(String, String)],
    scope: &[(String, String)],
    span: &[(String, String)],
) -> Vec<(String, String)> {
    // Insert lowest-precedence first; higher-precedence inserts overwrite.
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in span {
        map.insert(k.clone(), v.clone());
    }
    for (k, v) in scope {
        map.insert(k.clone(), v.clone());
    }
    for (k, v) in resource {
        map.insert(k.clone(), v.clone());
    }
    map.into_iter().collect()
}

/// Encodes an attribute map to its canonical stored form: the pairs sorted by
/// key with duplicate keys collapsed (last value wins), then
/// `uvarint(count)` followed by, per pair, `uvarint(klen) key uvarint(vlen)
/// value` (UTF-8, no terminators). Sorting makes the encoding injective over
/// maps, so identical maps encode byte-identically regardless of input order.
pub fn encode_attrs(attrs: &[(String, String)]) -> Vec<u8> {
    let map: BTreeMap<&str, &str> = attrs
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let mut out = Vec::new();
    put_uvarint(&mut out, map.len() as u64);
    for (k, v) in map {
        put_uvarint(&mut out, k.len() as u64);
        out.extend_from_slice(k.as_bytes());
        put_uvarint(&mut out, v.len() as u64);
        out.extend_from_slice(v.as_bytes());
    }
    out
}

/// Decodes a canonical attribute-map blob (the write-side [`encode_attrs`])
/// back to its pairs. Untrusted: rejects a count over the cap, a non-ascending
/// or duplicated key sequence, non-UTF-8 bytes, truncation, and trailing bytes.
pub fn decode_attrs(bytes: &[u8]) -> Result<Vec<(String, String)>, SpanSegError> {
    let mut pos = 0usize;
    let count = get_uvarint(bytes, &mut pos)?;
    if count > MAX_ATTR_PAIRS {
        return Err(SpanSegError::Corrupted(format!(
            "attrs pair count {count} over cap {MAX_ATTR_PAIRS}"
        )));
    }
    let mut out: Vec<(String, String)> = Vec::with_capacity((count as usize).min(1 << 12));
    let mut prev: Option<String> = None;
    for _ in 0..count {
        let key = read_str(bytes, &mut pos, "attr key")?;
        if prev.as_ref().is_some_and(|p| *p >= key) {
            return Err(SpanSegError::Corrupted("attrs keys not ascending".into()));
        }
        prev = Some(key.clone());
        let value = read_str(bytes, &mut pos, "attr value")?;
        out.push((key, value));
    }
    if pos != bytes.len() {
        return Err(SpanSegError::Corrupted("attrs trailing bytes".into()));
    }
    Ok(out)
}

/// Reads a `uvarint(len)`-prefixed UTF-8 string starting at `*pos`.
fn read_str(bytes: &[u8], pos: &mut usize, what: &str) -> Result<String, SpanSegError> {
    let len = usize::try_from(get_uvarint(bytes, pos)?)
        .map_err(|_| SpanSegError::Corrupted(format!("{what} len range")))?;
    let end = pos
        .checked_add(len)
        .ok_or_else(|| SpanSegError::Corrupted(format!("{what} overflow")))?;
    let slice = bytes
        .get(*pos..end)
        .ok_or_else(|| SpanSegError::Corrupted(format!("{what} truncated")))?;
    let s = std::str::from_utf8(slice)
        .map_err(|_| SpanSegError::Corrupted(format!("{what} not utf-8")))?
        .to_string();
    *pos = end;
    Ok(s)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn status_code_roundtrip() {
        for b in 0u8..=2 {
            assert_eq!(StatusCode::from_u8(b).expect("known").to_u8(), b);
        }
        assert!(StatusCode::from_u8(3).is_none());
    }

    #[test]
    fn merge_attrs_resource_and_scope_win_over_span() {
        let resource = vec![("k".into(), "resource".into())];
        let scope = vec![
            ("k".into(), "scope".into()),
            ("s".into(), "scope_only".into()),
        ];
        let span = vec![
            ("k".into(), "span".into()),
            ("only".into(), "span_only".into()),
        ];
        let merged = merge_attrs(&resource, &scope, &span);
        // resource beats scope beats span on "k".
        assert_eq!(lookup(&merged, "k"), Some("resource"));
        assert_eq!(lookup(&merged, "s"), Some("scope_only"));
        assert_eq!(lookup(&merged, "only"), Some("span_only"));
        // Sorted ascending by key.
        let keys: Vec<&str> = merged.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["k", "only", "s"]);
    }

    fn lookup<'a>(m: &'a [(String, String)], k: &str) -> Option<&'a str> {
        m.iter().find(|(key, _)| key == k).map(|(_, v)| v.as_str())
    }

    #[test]
    fn attrs_encode_is_order_insensitive_and_roundtrips() {
        let a = vec![
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
        ];
        let b = vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ];
        assert_eq!(encode_attrs(&a), encode_attrs(&b));
        let decoded = decode_attrs(&encode_attrs(&a)).expect("decode");
        assert_eq!(
            decoded,
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]
        );
    }

    #[test]
    fn attrs_empty_roundtrips() {
        let bytes = encode_attrs(&[]);
        assert_eq!(bytes, vec![0x00]);
        assert_eq!(decode_attrs(&bytes).expect("decode"), Vec::new());
    }

    #[test]
    fn attrs_duplicate_key_last_wins_on_encode() {
        let a = vec![
            ("k".to_string(), "first".to_string()),
            ("k".to_string(), "second".to_string()),
        ];
        let decoded = decode_attrs(&encode_attrs(&a)).expect("decode");
        assert_eq!(decoded, vec![("k".to_string(), "second".to_string())]);
    }

    #[test]
    fn decode_rejects_non_ascending_and_trailing_and_bad_utf8() {
        // Hand-build a blob with two equal keys (not strictly ascending).
        let mut bad = Vec::new();
        put_uvarint(&mut bad, 2);
        for _ in 0..2 {
            put_uvarint(&mut bad, 1);
            bad.push(b'k');
            put_uvarint(&mut bad, 0);
        }
        assert!(matches!(
            decode_attrs(&bad),
            Err(SpanSegError::Corrupted(_))
        ));

        // Trailing byte after a valid single-pair blob.
        let mut trailing = encode_attrs(&[("a".into(), "b".into())]);
        trailing.push(0);
        assert!(matches!(
            decode_attrs(&trailing),
            Err(SpanSegError::Corrupted(_))
        ));

        // Invalid UTF-8 in a key.
        let mut bad_utf8 = Vec::new();
        put_uvarint(&mut bad_utf8, 1);
        put_uvarint(&mut bad_utf8, 1);
        bad_utf8.push(0xff);
        put_uvarint(&mut bad_utf8, 0);
        assert!(matches!(
            decode_attrs(&bad_utf8),
            Err(SpanSegError::Corrupted(_))
        ));
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn attrs_roundtrip_any_map(
            pairs in proptest::collection::vec(("[a-z]{1,4}", "[a-z0-9]{0,6}"), 0..40)
        ) {
            let attrs: Vec<(String, String)> = pairs;
            let encoded = encode_attrs(&attrs);
            let decoded = decode_attrs(&encoded).expect("decode");
            // The decoded map equals the input deduplicated by key (last wins),
            // sorted ascending: exactly what encode_attrs canonicalizes to.
            let mut want: std::collections::BTreeMap<String, String> =
                std::collections::BTreeMap::new();
            for (k, v) in &attrs {
                want.insert(k.clone(), v.clone());
            }
            let want: Vec<(String, String)> = want.into_iter().collect();
            prop_assert_eq!(decoded, want);
        }

        #[test]
        fn decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..128)) {
            let _ = decode_attrs(&bytes);
        }
    }
}
