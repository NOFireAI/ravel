//! Wide-attribute synthetic corpus for the stage-1 width gate (issue #2563,
//! epic #2467, ADR-2467 decision 4): records with 105 dynamic attributes,
//! deterministic, used by `logseg_width_gate_time` and
//! `logseg_peak_by_site_wide`. Body, severity, timestamp spacing, and the
//! stream-identity scheme (resource `service.name`/`host.name`, scope `lib`)
//! match `ravel-logseg/benches/common/mod.rs::make_record` exactly; that file
//! is shared and is not edited here, and its `stream_identity` helper is
//! private to its own module, so the resource/scope shape is reproduced
//! rather than imported.
//!
//! Every attribute value is a pure function of the stream index, the
//! within-stream record ordinal, and the column index: no wall clock, no
//! unseeded randomness.

use ravel_logseg::{AttrValue, LogRecord, LogStreamId, stream_attrs_bytes};
use ravel_types::logstream::log_stream_id;

/// Exactly 105 dynamic attributes per record, distinct names shared by every
/// record. 42 str (40%) + 37 i64 (35%) + 16 f64 (15.2%) + 10 bool (9.5%).
pub const WIDE_ATTR_COUNT: usize = 105;
const STR_COLS: usize = 42;
const I64_COLS: usize = 37;
const F64_COLS: usize = 16;
const BOOL_COLS: usize = 10;

const _: () = assert!(STR_COLS + I64_COLS + F64_COLS + BOOL_COLS == WIDE_ATTR_COUNT);

/// Name-component vocabulary; combined with a type-independent `attr.` prefix
/// and a zero-padded column index so every name is distinct by construction.
/// Chosen lengths put the full name (`attr.<word>.<idx>`) in 11-19 bytes,
/// inside the 6-24 byte band the task asks for.
const NAME_WORDS: [&str; 20] = [
    "id",
    "ip",
    "code",
    "count",
    "depth",
    "scope",
    "region",
    "status",
    "latency",
    "retries",
    "cluster",
    "version",
    "session",
    "queue",
    "tenant",
    "priority",
    "checksum",
    "endpoint",
    "threshold",
    "identifier",
];

const VALUE_WORDS: [&str; 16] = [
    "get", "post", "put", "delete", "ok", "error", "timeout", "retry", "cache", "miss", "hit",
    "queued", "active", "closed", "pending", "failed",
];

#[derive(Clone, Copy)]
enum ColKind {
    Str,
    I64,
    F64,
    Bool,
}

fn col_kind(idx: usize) -> ColKind {
    if idx < STR_COLS {
        ColKind::Str
    } else if idx < STR_COLS + I64_COLS {
        ColKind::I64
    } else if idx < STR_COLS + I64_COLS + F64_COLS {
        ColKind::F64
    } else {
        ColKind::Bool
    }
}

/// Deterministic, distinct-by-construction (the index is embedded) column
/// name, 11-19 bytes.
pub fn attr_name(idx: usize) -> String {
    let w = NAME_WORDS[idx % NAME_WORDS.len()];
    format!("attr.{w}.{idx:03}")
}

/// `rk` ("record key") mixes stream index and within-stream ordinal into one
/// deterministic value so every attribute is a pure function of (record,
/// column), as the task requires.
fn record_key(stream_idx: usize, i: usize) -> usize {
    stream_idx.wrapping_mul(1_000_003).wrapping_add(i)
}

/// String value: a `handful-to-a-few-hundred`-distinct-value column
/// (`distinct` ranges 5..=300 across columns), rendered at a deterministic
/// length in 4..=40 bytes.
fn str_value(col_idx: usize, rk: usize) -> String {
    let distinct = 5 + (col_idx * 37) % 296;
    let v = rk % distinct;
    let word = VALUE_WORDS[(col_idx + v) % VALUE_WORDS.len()];
    let target_len = 4 + (col_idx * 11 + v * 3) % 37;
    let mut s = String::with_capacity(target_len);
    s.push_str(word);
    s.push('-');
    s.push_str(&v.to_string());
    let mut filler = 0usize;
    while s.len() < target_len {
        let c = (b'a' + ((s.len() * 7 + col_idx + filler) % 26) as u8) as char;
        s.push(c);
        filler += 1;
    }
    s.truncate(target_len);
    s
}

fn i64_value(col_idx: usize, rk: usize) -> i64 {
    let base = (rk as i64)
        .wrapping_mul(col_idx as i64 + 1)
        .wrapping_add(col_idx as i64 * 97);
    let v = base % 1_000_000;
    if (rk + col_idx).is_multiple_of(5) { -v } else { v }
}

fn f64_value(col_idx: usize, rk: usize) -> f64 {
    let v = (rk % 997) as f64 * 0.013 + (col_idx as f64) * 0.1;
    if (rk + col_idx).is_multiple_of(4) { -v } else { v }
}

fn wide_attrs(stream_idx: usize, i: usize) -> Vec<(String, AttrValue)> {
    let rk = record_key(stream_idx, i);
    (0..WIDE_ATTR_COUNT)
        .map(|idx| {
            let name = attr_name(idx);
            let value = match col_kind(idx) {
                ColKind::Str => AttrValue::Str(str_value(idx, rk)),
                ColKind::I64 => AttrValue::I64(i64_value(idx, rk)),
                ColKind::F64 => AttrValue::F64(f64_value(idx, rk)),
                ColKind::Bool => AttrValue::Bool((rk + idx).is_multiple_of(2)),
            };
            (name, value)
        })
        .collect()
}

/// Reproduces `common::stream_identity` (private to `common/mod.rs`) exactly:
/// same resource (`service.name`, `host.name`) and scope (`lib`) attributes.
fn stream_identity(stream_idx: usize) -> (LogStreamId, Vec<u8>) {
    let resource = vec![
        (
            "service.name".to_string(),
            AttrValue::Str(format!("svc-{stream_idx}")),
        ),
        (
            "host.name".to_string(),
            AttrValue::Str(format!("host-{:04}", stream_idx % 500)),
        ),
    ];
    let scope_attrs = vec![("lib".to_string(), AttrValue::I64(stream_idx as i64 % 8))];
    let blob = stream_attrs_bytes(&resource, "otel-scope", "1.0", &scope_attrs);
    let id = log_stream_id(&resource, "otel-scope", "1.0", &scope_attrs);
    (id, blob)
}

const SEVERITIES: [&str; 5] = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"];
const BODY_WORDS: [&str; 8] = [
    "request",
    "completed",
    "connection",
    "refused",
    "timeout",
    "retrying",
    "handler",
    "dispatch",
];

/// Builds one wide record for `stream_idx`, record ordinal `i`, at `ts_ns`.
/// Body, severity, trace/span ids, and timestamps use exactly
/// `common::make_record`'s formulas; only `attrs` (105 dynamic columns
/// instead of 4) differs.
pub fn make_wide_record(stream_idx: usize, i: usize, ts_ns: i64) -> LogRecord {
    let (stream_id, stream_attrs) = stream_identity(stream_idx);
    let word_a = BODY_WORDS[i % BODY_WORDS.len()];
    let word_b = BODY_WORDS[(i / 7) % BODY_WORDS.len()];
    let mut body = format!("{word_a} {word_b} id={i}");
    if i.is_multiple_of(97) {
        body.push_str(" needle");
    }
    LogRecord {
        stream_id,
        stream_attrs,
        ts_ns,
        observed_ts_ns: ts_ns + 1_000,
        severity_num: (i % 24) as u8 + 1,
        severity_text: SEVERITIES[i % SEVERITIES.len()].to_string(),
        body,
        trace_id: Some([(stream_idx as u8).wrapping_add(i as u8); 16]),
        span_id: Some([(i as u8); 8]),
        flags: 0,
        attrs: wide_attrs(stream_idx, i),
    }
}

/// Builds `stream_count` streams with `records_per_stream` records each, in
/// push order, matching `common::build_corpus`'s shape.
pub fn build_wide_corpus(stream_count: usize, records_per_stream: usize) -> Vec<LogRecord> {
    let mut out = Vec::with_capacity(stream_count * records_per_stream);
    for s in 0..stream_count {
        for i in 0..records_per_stream {
            out.push(make_wide_record(s, i, i as i64 * 1_000_000));
        }
    }
    out
}
