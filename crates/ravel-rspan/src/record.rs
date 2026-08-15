//! Span record model, fixed column ids, the merged-attrs map codec, and the
//! v4 resolved-row form the writer feeds to the block encoder
//! (docs/span-segment-format.md).
//!
//! [`SpanRecord`] is the caller-facing record: one row per span. Every span
//! carries a single `attrs` map (`Map<Utf8, Utf8>`) that already merges the
//! resource, scope, and span attribute sets; [`merge_attrs`] builds that map
//! following the same resource+scope-wins-over-record convention
//! `docs/log-segment-format.md` documents for logs.
//!
//! v4 (ADR-0045 decision 3) replaces v3's single opaque `attrs` blob column
//! with RLOG's per-key column design ([`ResolvedSpanRow`]): each attribute key
//! becomes its own string column, overflow keys past the 1000-column budget
//! fold into a canonical [`COL_ATTRS_RAW`] blob, and the `_events_raw`
//! attribute is promoted into nested [`SpanEvent`] columns. `service.name`
//! stays lifted into its own column (id 9, v3). The caller-facing
//! [`SpanRecord`] is unchanged: the writer resolves it into a
//! [`ResolvedSpanRow`] and the reader rebuilds the identical `SpanRecord`, so
//! every attribute (including whatever landed in `attrs_raw`) and the
//! `_events_raw` blob round-trip byte-identically.

use std::collections::BTreeMap;

use crate::error::SpanSegError;
use crate::varint::{get_uvarint, put_uvarint};

// Fixed column ids (docs/span-segment-format.md BLOCKS). Ids 0..=13 are the
// reserved fixed columns; dynamic per-key attribute columns start at
// [`FIRST_DYNAMIC_COL`] and carry their name in the block's own directory
// (RSPAN v4 blocks are self-describing; there is no object-wide FIELD_DIR
// section).
pub const COL_TRACE_ID: u32 = 0;
pub const COL_SPAN_ID: u32 = 1;
pub const COL_PARENT_SPAN_ID: u32 = 2;
pub const COL_NAME: u32 = 3;
pub const COL_START_TS: u32 = 4;
pub const COL_END_TS: u32 = 5;
pub const COL_STATUS_CODE: u32 = 6;
pub const COL_STATUS_MESSAGE: u32 = 7;
/// `attrs_raw` (v4, ADR-0045 decision 3): the canonical blob of the attributes
/// that overflowed the 1000-dynamic-column budget for a row, or that could not
/// be given a typed column. Nullable; a row with no overflow has no value.
/// This repurposes v3's `attrs` blob column id: v4 retires v3 with no dual
/// reader, so the id is reused rather than versioned.
pub const COL_ATTRS_RAW: u32 = 8;
/// `service_name` (v3, ADR-0054): the value of the `service.name` attribute,
/// lifted out of the attribute map into its own column. Nullable: a span whose
/// merged attrs carry no `service.name` has no value here. This id also scopes
/// the `service.name` bloom (docs/span-segment-format.md "BLOOM").
pub const COL_SERVICE_NAME: u32 = 9;
/// `event_count` (v4): per row, the number of span events it carries (0 when
/// none). Always present in a block that has any events; absent entirely when
/// no row in the block has an event.
pub const COL_EVENT_COUNT: u32 = 10;
/// `event_ts` (v4): the events' `time_unix_nano`, one entry per event across
/// every row of the block (a flattened nested column, not one value per row).
pub const COL_EVENT_TS: u32 = 11;
/// `event_name` (v4): the events' names, flattened one entry per event.
pub const COL_EVENT_NAME: u32 = 12;
/// `event_attrs_blob` (v4): each event's opaque serialized payload, flattened
/// one entry per event. Holds the exception stack traces that are a primary
/// span-investigation target (ADR-0045 decision 3). It is the round-trip
/// source of truth for `_events_raw`; `event_ts`/`event_name` are projections
/// read out of it for columnar access.
pub const COL_EVENT_ATTRS_BLOB: u32 = 13;
/// First column id available to dynamic per-key attribute columns (v4).
pub const FIRST_DYNAMIC_COL: u32 = 14;

/// The attribute key lifted into [`COL_SERVICE_NAME`] (v3, ADR-0054). The
/// writer removes this key from the per-key columns so the value is not
/// duplicated on disk; the reader re-inserts it when rebuilding the record, so
/// a `SpanRecord` round-trips byte-identically.
pub const SERVICE_NAME_KEY: &str = "service.name";

/// The reserved attribute key whose value carries the span's events as a hex
/// blob (`ravel_otlp::traces_normalize::ATTR_EVENTS_RAW`). v4 promotes a
/// parseable value into the nested event columns
/// ([`COL_EVENT_TS`]/[`COL_EVENT_NAME`]/[`COL_EVENT_ATTRS_BLOB`]); the reader
/// reconstructs the identical blob on decode. A value that is not a valid
/// events blob stays an ordinary attribute, so no data is ever lost.
pub const EVENTS_RAW_KEY: &str = "_events_raw";

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
/// `attrs` is the already-merged resource+scope+span map (see [`merge_attrs`]).
/// v4 stores it as per-key columns rather than one opaque blob, promotes the
/// `_events_raw` value into nested event columns, and lifts `service.name` into
/// its own column, but the caller-facing shape is unchanged: a decoded record
/// carries the identical `attrs` map. Span links stay out of scope and are not
/// promoted to columns; a `_links_raw` value (or any other key) round-trips as
/// an ordinary attribute.
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

/// One span event, decoded from the `_events_raw` blob into the nested v4 event
/// columns (docs/span-segment-format.md "BLOCKS"). `attrs_blob` is the event's
/// opaque serialized bytes (the whole OTLP `Span.Event` message); `ts_ns` and
/// `name` are projected out of it for columnar access. The block encoder stores
/// all three as independent columns and returns them verbatim, so a
/// `SpanEvent` round-trips value-for-value regardless of what `attrs_blob`
/// holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanEvent {
    pub ts_ns: i64,
    pub name: String,
    pub attrs_blob: Vec<u8>,
}

/// A [`SpanRecord`] resolved for v4 storage: `service.name` lifted out, the
/// remaining attributes split into per-key string columns (`columns`) with
/// overflow folded into `attrs_raw`, and the `_events_raw` blob decoded into
/// nested `events`. The writer produces these from `SpanRecord`s;
/// [`crate::block::write_block`] encodes them; [`crate::block::DecodedBlock::record`]
/// rebuilds the identical `SpanRecord`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedSpanRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
    pub status_code: StatusCode,
    pub status_message: Option<String>,
    /// The `service.name` value lifted into [`COL_SERVICE_NAME`], if any.
    pub service_name: Option<String>,
    /// Resolved per-key attribute columns, `(column_id, value)`, one per
    /// in-budget attribute this row carries. A column id appears at most once.
    pub columns: Vec<(u32, String)>,
    /// Canonical bytes ([`encode_attrs`]) of the attributes that overflowed the
    /// dynamic-column budget, present only when this row had any. Stored in
    /// [`COL_ATTRS_RAW`].
    pub attrs_raw: Option<Vec<u8>>,
    /// The span's events, decoded from `_events_raw`. Empty when the span had
    /// no (parseable) events blob.
    pub events: Vec<SpanEvent>,
}

/// Parses a `_events_raw` attribute value into span events, or `None` when the
/// value is not a valid events blob (invalid hex, malformed length-delimited
/// framing, trailing bytes, or zero events). A `None` result means the caller
/// must keep `_events_raw` as an ordinary attribute so its value round-trips
/// unchanged; only a `Some` non-empty result is promoted into the event
/// columns.
///
/// The value is the hex encoding of a concatenation of length-delimited OTLP
/// `Span.Event` messages (`ravel_otlp::traces_normalize::encode_blob`). This
/// splits on the self-describing length-delimited framing alone (no OTLP field
/// knowledge), keeping each event's payload verbatim as `attrs_blob`, and does
/// a best-effort top-level scan for `time_unix_nano` (field 1, fixed64) and
/// `name` (field 2, len-delimited) to fill `ts_ns`/`name`. Those projections
/// never affect the round trip: [`reconstruct_events_raw`] rebuilds the value
/// from the verbatim `attrs_blob`s alone.
pub fn parse_events(value: &str) -> Option<Vec<SpanEvent>> {
    let bytes = hex_decode(value)?;
    if bytes.is_empty() {
        return None;
    }
    let mut pos = 0usize;
    let mut events = Vec::new();
    while pos < bytes.len() {
        let len = get_uvarint(&bytes, &mut pos).ok()?;
        let len = usize::try_from(len).ok()?;
        let end = pos.checked_add(len)?;
        let chunk = bytes.get(pos..end)?;
        pos = end;
        let (ts_ns, name) = scan_event_ts_name(chunk);
        events.push(SpanEvent {
            ts_ns,
            name,
            attrs_blob: chunk.to_vec(),
        });
    }
    if pos != bytes.len() || events.is_empty() {
        return None;
    }
    Some(events)
}

/// Rebuilds the exact `_events_raw` attribute value from decoded events: each
/// event's verbatim `attrs_blob`, length-delimited with a canonical varint
/// prefix, concatenated and hex-encoded. This is the inverse of
/// [`parse_events`] and reproduces the ingest-side bytes byte-for-byte, so a
/// `SpanRecord` carrying `_events_raw` round-trips unchanged.
pub fn reconstruct_events_raw(events: &[SpanEvent]) -> String {
    let mut raw = Vec::new();
    for ev in events {
        put_uvarint(&mut raw, ev.attrs_blob.len() as u64);
        raw.extend_from_slice(&ev.attrs_blob);
    }
    hex_encode(&raw)
}

/// Best-effort scan of one serialized OTLP `Span.Event` for its
/// `time_unix_nano` (field 1, wire type 1 / fixed64) and `name` (field 2, wire
/// type 2 / length-delimited). Unknown or differently-typed fields are skipped.
/// A field that cannot be parsed simply leaves its projection at the default
/// (`0` / empty), never an error: `attrs_blob` remains the authoritative copy.
fn scan_event_ts_name(chunk: &[u8]) -> (i64, String) {
    let mut ts_ns = 0i64;
    let mut name = String::new();
    let mut pos = 0usize;
    while pos < chunk.len() {
        let Ok(tag) = get_uvarint(chunk, &mut pos) else {
            break;
        };
        let field = tag >> 3;
        let wire = tag & 0x7;
        match wire {
            0 => {
                // varint
                if get_uvarint(chunk, &mut pos).is_err() {
                    break;
                }
            }
            1 => {
                // fixed64
                let end = match pos.checked_add(8) {
                    Some(e) if e <= chunk.len() => e,
                    _ => break,
                };
                if field == 1 {
                    let mut a = [0u8; 8];
                    a.copy_from_slice(&chunk[pos..end]);
                    ts_ns = i64::from_le_bytes(a);
                }
                pos = end;
            }
            2 => {
                // length-delimited
                let Ok(len) = get_uvarint(chunk, &mut pos) else {
                    break;
                };
                let end = match usize::try_from(len).ok().and_then(|l| pos.checked_add(l)) {
                    Some(e) if e <= chunk.len() => e,
                    _ => break,
                };
                if field == 2
                    && let Ok(s) = std::str::from_utf8(&chunk[pos..end])
                {
                    name = s.to_string();
                }
                pos = end;
            }
            5 => {
                // fixed32
                match pos.checked_add(4) {
                    Some(e) if e <= chunk.len() => pos = e,
                    _ => break,
                }
            }
            _ => break,
        }
    }
    (ts_ns, name)
}

/// Lowercase hex, matching `hex::encode` (the ingest side, so the reconstructed
/// `_events_raw` is byte-identical). Kept inline to avoid a crate dependency
/// for two trivial functions.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// Decodes lowercase-or-uppercase hex, or `None` on an odd length or a non-hex
/// character. Untrusted input: never panics.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let nib = |c: u8| -> Option<u8> {
        Some(match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => return None,
        })
    };
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        out.push((nib(pair[0])? << 4) | nib(pair[1])?);
    }
    Some(out)
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
/// boundary (ADR-0041); the rule itself is what is reused.
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

/// The `service.name` value in a merged attribute map, if present. Used by the
/// writer to fill the [`COL_SERVICE_NAME`] column and to seed the block's
/// `service.name` bloom (docs/span-segment-format.md "BLOOM", ADR-0054).
pub fn service_name_of(attrs: &[(String, String)]) -> Option<&str> {
    attrs
        .iter()
        .find(|(k, _)| k == SERVICE_NAME_KEY)
        .map(|(_, v)| v.as_str())
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

    /// Frame one synthetic event chunk as the ingest side does: a canonical
    /// varint length prefix then the verbatim chunk bytes.
    fn frame(chunk: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, chunk.len() as u64);
        out.extend_from_slice(chunk);
        out
    }

    #[test]
    fn events_round_trip_through_parse_and_reconstruct() {
        // Two OTLP-shaped events; parse_events must extract ts/name and keep the
        // verbatim payload, and reconstruct_events_raw must rebuild the exact
        // input hex byte-for-byte.
        let mut e0 = Vec::new();
        e0.push(0x09); // field 1 fixed64 time
        e0.extend_from_slice(&123_456_789i64.to_le_bytes());
        e0.push(0x12); // field 2 name
        e0.push(3);
        e0.extend_from_slice(b"log");
        e0.push(0x1a); // field 3 attributes stand-in
        e0.push(2);
        e0.extend_from_slice(b"hi");
        let e1 = {
            let mut e = Vec::new();
            e.push(0x09);
            e.extend_from_slice(&(-5i64).to_le_bytes());
            e.push(0x12);
            e.push(9);
            e.extend_from_slice(b"exception");
            e
        };
        let mut raw = frame(&e0);
        raw.extend_from_slice(&frame(&e1));
        let value = hex_encode(&raw);

        let events = parse_events(&value).expect("valid events blob");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].ts_ns, 123_456_789);
        assert_eq!(events[0].name, "log");
        assert_eq!(events[0].attrs_blob, e0);
        assert_eq!(events[1].ts_ns, -5);
        assert_eq!(events[1].name, "exception");
        assert_eq!(events[1].attrs_blob, e1);

        assert_eq!(
            reconstruct_events_raw(&events),
            value,
            "reconstruction must be byte-identical to the ingest blob"
        );
    }

    #[test]
    fn parse_events_rejects_non_events_values() {
        // Not hex.
        assert!(parse_events("zz").is_none());
        // Odd-length hex.
        assert!(parse_events("abc").is_none());
        // Empty.
        assert!(parse_events("").is_none());
        // Valid hex but the length prefix overruns the buffer (trailing/short).
        let short = hex_encode(&[0x05, 0x01, 0x02]); // claims 5 bytes, only 2 follow
        assert!(parse_events(&short).is_none());
        // Trailing bytes after a well-framed chunk.
        let mut raw = frame(&[1, 2, 3]);
        raw.push(0xff);
        assert!(parse_events(&hex_encode(&raw)).is_none());
    }

    #[test]
    fn scan_event_ts_name_is_best_effort() {
        // A chunk with no recognizable fields projects to (0, "") but stays a
        // valid single event (the verbatim blob is authoritative).
        let chunk = vec![0xff, 0x00, 0x13];
        let events = parse_events(&hex_encode(&frame(&chunk))).expect("one event");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].ts_ns, 0);
        assert_eq!(events[0].name, "");
        assert_eq!(events[0].attrs_blob, chunk);
        assert_eq!(reconstruct_events_raw(&events), hex_encode(&frame(&chunk)));
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

        /// parse_events never panics on an arbitrary string (untrusted attr
        /// value); a Some result always reconstructs byte-identically.
        #[test]
        fn parse_events_never_panics_and_round_trips(s in ".{0,200}") {
            if let Some(events) = parse_events(&s) {
                prop_assert_eq!(reconstruct_events_raw(&events), s);
            }
        }

        /// Any list of arbitrary per-event payloads, framed as the ingest side
        /// frames them, round-trips exactly through parse_events +
        /// reconstruct_events_raw (the verbatim payload is authoritative).
        #[test]
        fn arbitrary_event_payloads_round_trip(
            payloads in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 0..16), 1..6)
        ) {
            let mut raw = Vec::new();
            for p in &payloads {
                put_uvarint(&mut raw, p.len() as u64);
                raw.extend_from_slice(p);
            }
            let value = hex_encode(&raw);
            let events = parse_events(&value).expect("well-framed blob parses");
            prop_assert_eq!(events.len(), payloads.len());
            for (ev, p) in events.iter().zip(payloads.iter()) {
                prop_assert_eq!(&ev.attrs_blob, p);
            }
            prop_assert_eq!(reconstruct_events_raw(&events), value);
        }
    }
}
