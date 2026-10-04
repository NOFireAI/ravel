//! Sparse-attribute synthetic corpus for the stage-2 sparse-input measurement
//! (issue #2585, epic #2467). `wide_corpus.rs`'s corpus puts every attribute
//! on every record, so its dynamic-column count equals the per-record
//! attribute count; this corpus instead gives each record a fixed, small
//! attribute count (10) while drawing the attribute NAMES from many distinct
//! keys (`K`), so the per-distinct-key dense vector `from_records` allocates
//! (one `Option<AttrValue>` slot per record, for every distinct `(name,
//! type)` ever seen) grows independently of what any one record carries.
//! Shared by `logseg_peak_by_site_sparse` and `logseg_sparse_gate_time` via
//! `#[path]`, same convention as `wide_corpus.rs`.
//!
//! Every attribute value is a pure function of record index and key index: no
//! wall clock, no unseeded randomness.

use ravel_logseg::{AttrValue, LogRecord, LogStreamId, stream_attrs_bytes};
use ravel_types::logstream::log_stream_id;

/// Attributes per record, fixed regardless of `K`.
pub const ATTRS_PER_RECORD: usize = 10;

/// Odd stride, coprime to every `K` this task uses (100, 1,000, 10,000: all
/// of the form 2^a * 5^b), so `j * STRIDE mod K` is injective over `0..K` and
/// the per-record key selection below never repeats a key within one record
/// (checked in `sparse_attrs`, not assumed).
const STRIDE: usize = 7;

/// Name-component vocabulary, all <=9 bytes, so `attr.<word>.<idx>` (idx
/// always 4-digit zero-padded, the width `K` up to 10,000 needs) stays within
/// 11-19 bytes for every word. `wide_corpus::NAME_WORDS`'s "identifier" (10
/// bytes) is dropped for exactly this reason.
const NAME_WORDS: [&str; 19] = [
    "id", "ip", "code", "count", "depth", "scope", "region", "status", "latency", "retries",
    "cluster", "version", "session", "queue", "tenant", "priority", "checksum", "endpoint",
    "threshold",
];

const VALUE_WORDS: [&str; 16] = [
    "get", "post", "put", "delete", "ok", "error", "timeout", "retry", "cache", "miss", "hit",
    "queued", "active", "closed", "pending", "failed",
];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ColKind {
    Str,
    I64,
    F64,
    Bool,
}

/// Fixed type per key index: ~40% str, ~35% i64, ~15% f64, bool gets the
/// remainder (~10%), exact counts summing to `k` by construction (integer
/// division, no rounding gap).
fn col_kind(key_idx: usize, k: usize) -> ColKind {
    let str_count = k * 40 / 100;
    let i64_count = k * 35 / 100;
    let f64_count = k * 15 / 100;
    if key_idx < str_count {
        ColKind::Str
    } else if key_idx < str_count + i64_count {
        ColKind::I64
    } else if key_idx < str_count + i64_count + f64_count {
        ColKind::F64
    } else {
        ColKind::Bool
    }
}

/// Deterministic key name, 11-19 bytes: `attr.<word>.<idx>`, `idx` always
/// 4-digit zero-padded so the format is uniform across every `K` this task
/// uses (up to 10,000, which needs 4 digits).
pub fn key_name(key_idx: usize) -> String {
    let w = NAME_WORDS[key_idx % NAME_WORDS.len()];
    format!("attr.{w}.{key_idx:04}")
}

fn str_value(key_idx: usize, rk: usize) -> String {
    let distinct = 5 + (key_idx * 37) % 296;
    let v = rk % distinct;
    let word = VALUE_WORDS[(key_idx + v) % VALUE_WORDS.len()];
    let target_len = 4 + (key_idx * 11 + v * 3) % 37;
    let mut s = String::with_capacity(target_len);
    s.push_str(word);
    s.push('-');
    s.push_str(&v.to_string());
    let mut filler = 0usize;
    while s.len() < target_len {
        let c = (b'a' + ((s.len() * 7 + key_idx + filler) % 26) as u8) as char;
        s.push(c);
        filler += 1;
    }
    s.truncate(target_len);
    s
}

fn i64_value(key_idx: usize, rk: usize) -> i64 {
    let base = (rk as i64)
        .wrapping_mul(key_idx as i64 + 1)
        .wrapping_add(key_idx as i64 * 97);
    let v = base % 1_000_000;
    if (rk + key_idx).is_multiple_of(5) { -v } else { v }
}

fn f64_value(key_idx: usize, rk: usize) -> f64 {
    let v = (rk % 997) as f64 * 0.013 + (key_idx as f64) * 0.1;
    if (rk + key_idx).is_multiple_of(4) { -v } else { v }
}

/// The `ATTRS_PER_RECORD` key indices this record uses, in attribute order.
/// `(record_idx * ATTRS_PER_RECORD + j * STRIDE) mod k` for `j` in
/// `0..ATTRS_PER_RECORD`.
pub fn record_key_indices(record_idx: usize, k: usize) -> [usize; ATTRS_PER_RECORD] {
    let mut out = [0usize; ATTRS_PER_RECORD];
    for (j, slot) in out.iter_mut().enumerate() {
        *slot = (record_idx * ATTRS_PER_RECORD + j * STRIDE) % k;
    }
    out
}

fn sparse_attrs(record_idx: usize, k: usize) -> Vec<(String, AttrValue)> {
    let key_indices = record_key_indices(record_idx, k);
    let mut seen = std::collections::HashSet::with_capacity(ATTRS_PER_RECORD);
    for &ki in &key_indices {
        assert!(
            seen.insert(ki),
            "record {record_idx}: repeated key index {ki} (k={k})"
        );
    }
    key_indices
        .iter()
        .map(|&ki| {
            let name = key_name(ki);
            let rk = record_idx.wrapping_mul(1_000_003).wrapping_add(ki);
            let value = match col_kind(ki, k) {
                ColKind::Str => AttrValue::Str(str_value(ki, rk)),
                ColKind::I64 => AttrValue::I64(i64_value(ki, rk)),
                ColKind::F64 => AttrValue::F64(f64_value(ki, rk)),
                ColKind::Bool => AttrValue::Bool((rk + ki).is_multiple_of(2)),
            };
            (name, value)
        })
        .collect()
}

/// Reproduces `common::stream_identity` exactly, for the single fixed stream
/// (index 0) every sparse-corpus shape uses.
fn stream_identity() -> (LogStreamId, Vec<u8>) {
    let resource = vec![
        (
            "service.name".to_string(),
            AttrValue::Str("svc-0".to_string()),
        ),
        (
            "host.name".to_string(),
            AttrValue::Str("host-0000".to_string()),
        ),
    ];
    let scope_attrs = vec![("lib".to_string(), AttrValue::I64(0))];
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

/// Builds one sparse record for ordinal `i` (stream index 0) at `ts_ns`.
/// Body, severity, trace/span ids, and timestamps use exactly
/// `common::make_record`'s formulas; only `attrs` (10 of `k` possible keys
/// instead of a fixed 4) differs.
pub fn make_sparse_record(i: usize, k: usize, ts_ns: i64) -> LogRecord {
    let (stream_id, stream_attrs) = stream_identity();
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
        trace_id: Some([i as u8; 16]),
        span_id: Some([i as u8; 8]),
        flags: 0,
        attrs: sparse_attrs(i, k),
    }
}

/// Builds `n` records (1 stream), each carrying `ATTRS_PER_RECORD` attributes
/// drawn from `k` distinct keys.
pub fn build_sparse_corpus(n: usize, k: usize) -> Vec<LogRecord> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(make_sparse_record(i, k, i as i64 * 1_000_000));
    }
    out
}

/// Distinct key indices actually used across `n` records at this `k` (an
/// exact count, not assumed equal to `k`).
pub fn distinct_keys_used(n: usize, k: usize) -> usize {
    let mut seen = vec![false; k];
    let mut count = 0usize;
    for i in 0..n {
        for ki in record_key_indices(i, k) {
            if !seen[ki] {
                seen[ki] = true;
                count += 1;
            }
        }
    }
    count
}
