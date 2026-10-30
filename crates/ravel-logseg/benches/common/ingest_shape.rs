//! The record shape an ingest flush encodes (ADR-3492): 50 streams, records
//! arriving in push batches of [`PUSH_BATCH`], about 40 attributes per record
//! (22 strings and 16 integers on every record, two more strings on some)
//! over the cardinalities real OTLP service logs carry, and bodies of about
//! 100 bytes.
//!
//! Shared by `benches/logseg_encode.rs` and `tests/differential.rs` through
//! `#[path]`, so the benchmark and the determinism test encode the same
//! records. Values come from a fixed-seed generator, so a record is a pure
//! function of its ordinal.

use ravel_logseg::{AttrValue, LogRecord, LogStreamId, stream_attrs_bytes};
use ravel_types::logstream::log_stream_id;

/// Records per ingest push batch.
pub const PUSH_BATCH: usize = 2_000;

/// Streams the records spread over.
pub const STREAMS: usize = 50;

const METHODS: [&str; 5] = ["GET", "POST", "PUT", "DELETE", "PATCH"];
const SEVERITIES: [(u8, &str); 4] = [(5, "DEBUG"), (9, "INFO"), (13, "WARN"), (17, "ERROR")];
const ERRORS: [&str; 8] = [
    "timeout",
    "connection_reset",
    "not_found",
    "unauthorized",
    "rate_limited",
    "bad_gateway",
    "deadline_exceeded",
    "invalid_argument",
];

/// splitmix64: one well-mixed 64-bit value per input.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn stream_identity(stream: usize) -> (LogStreamId, Vec<u8>) {
    let resource = vec![
        (
            "service.name".to_string(),
            AttrValue::Str(format!("svc-{}", stream % 12)),
        ),
        (
            "k8s.pod.name".to_string(),
            AttrValue::Str(format!("svc-{}-7d9f8c-{stream:05}", stream % 12)),
        ),
        (
            "k8s.namespace.name".to_string(),
            AttrValue::Str(format!("ns-{}", stream % 4)),
        ),
    ];
    let scope = vec![("lib".to_string(), AttrValue::I64(stream as i64 % 3))];
    let blob = stream_attrs_bytes(&resource, "otel-scope", "1.0", &scope);
    (log_stream_id(&resource, "otel-scope", "1.0", &scope), blob)
}

fn trace_id(hi: u64, lo: u64) -> [u8; 16] {
    (u128::from(hi) << 64 | u128::from(lo)).to_be_bytes()
}

fn s(name: &str, value: String) -> (String, AttrValue) {
    (name.to_string(), AttrValue::Str(value))
}

fn i(name: &str, value: i64) -> (String, AttrValue) {
    (name.to_string(), AttrValue::I64(value))
}

/// The first `n` records of the stream of arrivals, in push order. Record
/// timestamps rise by about 1.3 ms per record with sub-microsecond jitter;
/// each record belongs to one of [`STREAMS`] streams, skewed so a few streams
/// carry most of the traffic.
pub fn ingest_records(n: usize) -> Vec<LogRecord> {
    let streams: Vec<(LogStreamId, Vec<u8>)> = (0..STREAMS).map(stream_identity).collect();
    let base_ts = 1_760_000_000_000_000_000i64;
    (0..n)
        .map(|k| {
            let r = |salt: u64| mix(((k as u64) << 8) | salt);
            let skew = r(0) % 1_000;
            let stream = ((skew * skew) / 20_000) as usize % STREAMS;
            let ts = base_ts + k as i64 * 1_300_000 + (r(1) % 900) as i64;
            let (sev_num, sev_text) = SEVERITIES[[1, 1, 1, 1, 1, 1, 0, 2, 2, 3][(r(2) % 10) as usize]];
            let method = METHODS[(r(3) % 5) as usize];
            let route = format!("/api/v2/{}/{}", ["orders", "users", "carts", "items"][(r(4) % 4) as usize], r(5) % 10);
            let status = [200i64, 200, 200, 201, 204, 301, 400, 404, 500, 503][(r(6) % 10) as usize];
            let duration_us = (r(7) % 250_000) as i64;
            let user = r(8) % 2_000;
            let request_id = format!("{:016x}{:016x}", r(9), r(10));
            let mut body = format!(
                "{method} {route} completed status={status} in {duration_us}us user=u{user:04} req={}",
                &request_id[..16]
            );
            while body.len() < 100 {
                body.push_str(" ok");
            }
            let mut attrs = vec![
                s("http.method", method.to_string()),
                s("http.route", route),
                s("http.scheme", "https".to_string()),
                s("http.flavor", ["1.1", "2"][(r(11) % 2) as usize].to_string()),
                s("http.user_agent", format!("client/{}.{} (os-{})", r(12) % 5, r(13) % 20, r(14) % 6)),
                s("net.peer.name", format!("10.{}.{}.{}", r(15) % 4, r(16) % 16, r(17) % 64)),
                s("user.id", format!("u{user:04}")),
                s("session.id", format!("sess-{:08x}", r(18) % 5_000)),
                s("request.id", request_id),
                s("tenant.id", format!("tenant-{}", r(19) % 100)),
                s("cloud.region", ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "eu-central-1", "sa-east-1"][(r(20) % 6) as usize].to_string()),
                s("cloud.availability_zone", format!("az-{}", r(21) % 12)),
                s("deployment.environment", ["prod", "staging"][(r(22) % 8 / 7) as usize].to_string()),
                s("service.version", format!("1.{}.{}", r(23) % 3, r(24) % 4)),
                s("log.logger", format!("com.example.{}", r(25) % 30)),
                s("thread.name", format!("worker-{}", r(26) % 64)),
                s("code.function", format!("handle_{}", r(27) % 150)),
                s("code.filepath", format!("src/handlers/mod_{}.rs", r(28) % 200)),
                s("feature.flag", format!("flag-{}", r(29) % 20)),
                s("messaging.destination", format!("queue-{}", r(30) % 16)),
                s("db.system", ["postgresql", "redis", "none"][(r(31) % 3) as usize].to_string()),
                s("trace.sampled", ["true", "false"][(r(32) % 4 / 3) as usize].to_string()),
                i("http.status_code", status),
                i("http.request_content_length", (r(33) % 64_000) as i64),
                i("http.response_content_length", (r(34) % 512_000) as i64),
                i("duration_us", duration_us),
                i("net.peer.port", [443i64, 8443, 5432, 6379][(r(35) % 4) as usize]),
                i("thread.id", (r(36) % 64) as i64),
                i("code.lineno", (r(37) % 1_000) as i64),
                i("process.pid", 1_000 + stream as i64),
                i("retry.count", (r(38) % 16 / 13) as i64),
                i("queue.depth", (r(39) % 1_000) as i64),
                i("db.rows", (r(40) % 50) as i64),
                i("cache.hit", (r(41) % 2) as i64),
                i("batch.size", [1i64, 10, 50, 100][(r(42) % 4) as usize]),
                i("user.tier", (user % 3) as i64),
                i("event.sequence", k as i64),
                i("gc.pause_us", (r(43) % 5_000) as i64),
            ];
            if sev_num >= 13 {
                attrs.push(s("error.type", ERRORS[(r(44) % 8) as usize].to_string()));
            }
            if r(45) % 10 < 3 {
                attrs.push(s("db.statement", format!("SELECT * FROM t{} WHERE id = $1", r(46) % 100)));
            }
            let (stream_id, stream_attrs) = streams[stream].clone();
            LogRecord {
                stream_id,
                stream_attrs,
                ts_ns: ts,
                observed_ts_ns: ts + 2_000_000 + (r(47) % 1_000_000) as i64,
                severity_num: sev_num,
                severity_text: sev_text.to_string(),
                body,
                trace_id: Some(trace_id(r(48), r(51))),
                span_id: Some(r(49).to_le_bytes()),
                flags: (r(50) % 2) as u32,
                attrs,
            }
        })
        .collect()
}
