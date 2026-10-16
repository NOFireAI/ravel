//! Measures the storage overhead of a realistic four-field POSTINGS list
//! (ADR-0049).
//!
//! Builds one RLOG object from a realistic application-log corpus, writes it
//! twice through the real `RlogWriter` (once with no indexed fields, once with
//! the four-field list), and reports the POSTINGS section size and the object
//! overhead it adds. Deterministic: identical input yields byte-identical
//! output, so the numbers reproduce exactly.
//!
//! Run: `cargo run -p ravel-logseg --release --example postings_overhead`
//!
//! The four-field list is three per-record attributes plus one resource-level
//! attribute (`service.name`), the ordinary OTLP shape. All four emit
//! postings: every indexed stream-level key gets its own FIELD_DIR column,
//! so a key that is only ever a resource attribute is indexed like any
//! other.

// A throwaway measurement binary, not a production path: `expect` on the
// in-memory writer (which cannot fail for this fixed corpus) keeps it short.
#![allow(clippy::expect_used)]

use ravel_logseg::writer::WriteStats;
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_types::logstream::log_stream_id;

/// A corpus sized to fill roughly one flush object: ~64k records over 20
/// services, which lands several blocks under the default 8192-records-per-block
/// target so a posting list spans a realistic number of blocks.
const RECORD_COUNT: usize = 64_000;
const SERVICE_COUNT: usize = 20;

const STATUS_CODES: [i64; 10] = [200, 201, 204, 301, 400, 401, 403, 404, 500, 503];
const METHODS: [&str; 5] = ["GET", "POST", "PUT", "DELETE", "PATCH"];
const ROUTES: [&str; 12] = [
    "/api/v1/orders",
    "/api/v1/orders/{id}",
    "/api/v1/users",
    "/api/v1/users/{id}",
    "/api/v1/cart",
    "/api/v1/checkout",
    "/api/v1/payments",
    "/api/v1/inventory",
    "/api/v1/search",
    "/healthz",
    "/readyz",
    "/metrics",
];

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [1u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// One record for service `svc`, with realistic per-record HTTP attributes and
/// a resource carrying `service.name` and `k8s.namespace.name`.
fn record(i: usize) -> LogRecord {
    let svc = i % SERVICE_COUNT;
    let resource = [
        (
            "service.name".to_string(),
            AttrValue::Str(format!("svc-{svc:02}")),
        ),
        (
            "k8s.namespace.name".to_string(),
            AttrValue::Str(format!("ns-{}", svc % 4)),
        ),
    ];
    let status = STATUS_CODES[i % STATUS_CODES.len()];
    let method = METHODS[i % METHODS.len()];
    let route = ROUTES[i % ROUTES.len()];
    LogRecord {
        stream_id: log_stream_id(&resource, "app", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "app", "1.0", &[]),
        ts_ns: 1_000_000_000 + i as i64 * 1_000,
        observed_ts_ns: 1_000_000_000 + i as i64 * 1_000,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!(
            "handled {method} {route} -> {status} in {}ms for request {i}",
            i % 250
        ),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("http.status_code".to_string(), AttrValue::I64(status)),
            (
                "http.method".to_string(),
                AttrValue::Str(method.to_string()),
            ),
            ("http.route".to_string(), AttrValue::Str(route.to_string())),
        ],
    }
}

fn write(indexed: &[&str]) -> (usize, WriteStats) {
    let mut w = RlogWriter::new(RlogConfig::default(), identity())
        .with_indexed_fields(indexed.iter().map(|s| s.to_string()).collect());
    for i in 0..RECORD_COUNT {
        w.push(record(i)).expect("push");
    }
    let (bytes, stats) = w.finish_with_stats().expect("finish");
    (bytes.len(), stats)
}

fn main() {
    // The four-field list: three per-record HTTP attributes plus the
    // resource-level service.name. All four are indexed.
    let four_fields = [
        "service.name",
        "http.status_code",
        "http.method",
        "http.route",
    ];

    let (baseline, _) = write(&[]);
    let (with_postings, stats) = write(&four_fields);

    let added = with_postings.saturating_sub(baseline);
    let pct_of_object = added as f64 / with_postings as f64 * 100.0;
    let section_pct = stats.postings_bytes as f64 / with_postings as f64 * 100.0;

    println!("corpus: {RECORD_COUNT} records over {SERVICE_COUNT} services");
    println!("indexed-field list: {four_fields:?}");
    println!("  (service.name is resource-only; indexed anyway)");
    println!("baseline object (no indexed fields): {baseline} bytes");
    println!("object with POSTINGS:                {with_postings} bytes");
    println!(
        "POSTINGS section:                    {} bytes",
        stats.postings_bytes
    );
    println!(
        "indexed fields that emitted postings: {}",
        stats.postings_indexed_fields
    );
    println!(
        "summed distinct values:               {}",
        stats.postings_distinct_total
    );
    println!(
        "capped fields:                        {}",
        stats.postings_capped_fields
    );
    println!("overhead vs baseline:  {added} bytes  ({pct_of_object:.2}% of the object)");
    println!("POSTINGS as share of object:          {section_pct:.2}%");
}
