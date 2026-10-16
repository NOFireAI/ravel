//! Synthetic corpus generation shared by the logseg benches and by
//! `tests/bench_corpus.rs`. Kept small and self-contained (no dependency on
//! `ravel-bench`): every field mirrors the shapes real OTLP log ingest
//! produces (docs/log-segment-format.md), just deterministic instead of
//! sampled, so bench numbers reflect realistic multi-field records.

use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, stream_attrs_bytes,
};
use ravel_types::logstream::log_stream_id;

/// A fixed identity, reused across every bench object: benches measure the
/// writer/reader, not identity plumbing.
pub fn bench_identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 1,
        writer_id: [9u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

pub fn bench_config() -> RlogConfig {
    RlogConfig::default()
}

/// Deterministic per-stream identity: a distinct `service.name`/`host` pair
/// per stream index, matching how real resource attributes vary per instance.
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

/// Builds one synthetic record for `stream_idx`, record ordinal `i` within
/// that stream, at timestamp `ts_ns`. Bodies are built from a small rotating
/// vocabulary so `HasWord`/bloom benches have both common and rare tokens to
/// select on. Every record carries a mix of dynamic attribute types (i64, f64,
/// str, bool) so encode exercises every column codec path.
pub fn make_record(stream_idx: usize, i: usize, ts_ns: i64) -> LogRecord {
    let (stream_id, stream_attrs) = stream_identity(stream_idx);
    let word_a = BODY_WORDS[i % BODY_WORDS.len()];
    let word_b = BODY_WORDS[(i / 7) % BODY_WORDS.len()];
    let mut body = format!("{word_a} {word_b} id={i}");
    // A rare, selective token planted on a small minority of records, used by
    // the scan bench's selective predicate.
    if i.is_multiple_of(97) {
        body.push_str(" needle");
    }
    let attrs = vec![
        (
            "http.status_code".to_string(),
            AttrValue::I64(200 + (i % 5) as i64 * 100),
        ),
        (
            "duration_ms".to_string(),
            AttrValue::F64((i % 250) as f64 * 1.5),
        ),
        (
            "retryable".to_string(),
            AttrValue::Bool(i.is_multiple_of(3)),
        ),
        (
            "route".to_string(),
            AttrValue::Str(format!("/api/v1/resource/{}", i % 20)),
        ),
    ];
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
        attrs,
    }
}

/// Builds `stream_count` streams with `records_per_stream` records each, one
/// record per nanosecond-spaced timestamp per stream, in push order (the
/// writer sorts, so input order need not be presorted).
pub fn build_corpus(stream_count: usize, records_per_stream: usize) -> Vec<LogRecord> {
    let mut out = Vec::with_capacity(stream_count * records_per_stream);
    for s in 0..stream_count {
        for i in 0..records_per_stream {
            out.push(make_record(s, i, i as i64 * 1_000_000));
        }
    }
    out
}
