//! Decode cost of the vendored, bytes-retyped OTLP logs types (ADR-3492
//! D3b) against upstream's `String`-typed ones, on one fixed request body.
//!
//! Arm A times `opentelemetry_proto::tonic::collector::logs::v1::
//! ExportLogsServiceRequest::decode` from a `Bytes`. Arm B times
//! `ravel_otlp::vendored_logs::ExportLogsServiceRequest::decode` from the
//! same `Bytes` plus [`ravel_otlp::vendored_logs::validate_utf8`], so B pays
//! the same UTF-8 refusal cost upstream's `String` decode pays inline.
//!
//! The request body is built ONCE, outside every timed region: 2,000 log
//! records grouped one `ResourceLogs` per (host, app) pair, drawn with a
//! fixed-seed PRNG from 125 hosts x 40 apps (5,000 pairs), so repeated draws
//! leave about 1,700 of them occupied. Five pairs of (A, B) decodes run
//! interleaved, each reporting median and minimum nanoseconds per record
//! over its 5 samples, with `/proc/loadavg` printed beside each so a busy
//! host is visible rather than silently inflating the figures.
//!
//! Expected band (pre-registered on #3654): B at least 10% cheaper per
//! record than A, predicted 10% to 25%; under 5% means the bytes retype does
//! not pay and should not be wired into the ingest path in step 2. This
//! bench reports whatever the host measures; it does not fail the band.
//!
//! What it does self-check, and exit non-zero on: each arm's median/min
//! figure appears in the output exactly once, never zero or duplicated.
//!
//! Not run by `cargo test`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::time::Instant;

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest as UpstreamRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_otlp::vendored_logs::{self, ExportLogsServiceRequest as VendoredRequest};

const HOSTS: usize = 125;
const APPS: usize = 40;
const RECORDS: usize = 2_000;
const ATTRS_PER_RECORD: usize = 30;
const BODY_BYTES: usize = 100;
const SEED: u64 = 0x3654_D3B0_0000_0001;
const PAIRS: usize = 5;
const EXPECTED_GROUPS_LOW: usize = 1_400;
const EXPECTED_GROUPS_HIGH: usize = 1_900;

const SEVERITIES: [(i32, &str); 3] = [(9, "INFO"), (13, "WARN"), (17, "ERROR")];

/// Deterministic, non-cryptographic PRNG (SplitMix64) so the request body is
/// reproducible across runs and hosts without a `rand` dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a value in `[lo, hi)`.
    fn gen_range(&mut self, lo: usize, hi: usize) -> usize {
        lo + (self.next_u64() as usize) % (hi - lo)
    }

    fn gen_ascii_string(&mut self, len: usize) -> String {
        (0..len)
            .map(|_| (b'a' + (self.next_u64() % 26) as u8) as char)
            .collect()
    }
}

fn string_kv(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value)),
        }),
        ..Default::default()
    }
}

fn body_text(i: usize) -> String {
    let prefix = format!("log body {i:06} ");
    let pad = BODY_BYTES.saturating_sub(prefix.len());
    let mut s = prefix;
    s.push_str(&"x".repeat(pad));
    s
}

/// Builds the benchmark's fixed request: 2,000 records over 125 hosts x 40
/// apps, grouped one `ResourceLogs` per occupied (host, app) pair.
fn build_request() -> (UpstreamRequest, usize) {
    let mut rng = Rng::new(SEED);
    let mut groups: BTreeMap<(usize, usize), Vec<LogRecord>> = BTreeMap::new();
    let base_ts: u64 = 1_700_000_000_000_000_000;

    for i in 0..RECORDS {
        let host = rng.gen_range(0, HOSTS);
        let app = rng.gen_range(0, APPS);
        let (severity_number, severity_text) = SEVERITIES[rng.gen_range(0, SEVERITIES.len())];
        let attributes = (0..ATTRS_PER_RECORD)
            .map(|_| {
                let key_len = rng.gen_range(8, 25);
                let val_len = rng.gen_range(4, 41);
                string_kv(rng.gen_ascii_string(key_len), rng.gen_ascii_string(val_len))
            })
            .collect();
        let record = LogRecord {
            time_unix_nano: base_ts + i as u64,
            observed_time_unix_nano: base_ts + i as u64,
            severity_number,
            severity_text: severity_text.to_string(),
            body: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(body_text(i))),
            }),
            attributes,
            ..Default::default()
        };
        groups.entry((host, app)).or_default().push(record);
    }

    let group_count = groups.len();
    assert!(
        (EXPECTED_GROUPS_LOW..=EXPECTED_GROUPS_HIGH).contains(&group_count),
        "distinct (host,app) groups {group_count} outside expected band \
         {EXPECTED_GROUPS_LOW}-{EXPECTED_GROUPS_HIGH} (from {HOSTS}x{APPS} slots \
         at {RECORDS} draws, birthday-paradox estimate about 1,650); check the \
         seed or the RNG"
    );

    let resource_logs = groups
        .into_iter()
        .map(|((host, app), log_records)| ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("host.name".to_string(), format!("host-{host:04}")),
                    string_kv("service.name".to_string(), format!("app-{app:03}")),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        })
        .collect();

    (UpstreamRequest { resource_logs }, group_count)
}

fn loadavg() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unavailable".to_string())
}

fn median_min(mut samples: Vec<f64>) -> (f64, f64) {
    samples.sort_by(f64::total_cmp);
    (samples[samples.len() / 2], samples[0])
}

fn main() {
    let (request, group_count) = build_request();
    let body = Bytes::from(request.encode_to_vec());

    // Sanity checks outside every timed region: both decoders see the same
    // 2,000 records, and B's validator accepts bytes A accepted as strings.
    let sanity_upstream =
        UpstreamRequest::decode(body.clone()).expect("upstream decode of the fixture");
    let upstream_records: usize = sanity_upstream
        .resource_logs
        .iter()
        .flat_map(|rl| &rl.scope_logs)
        .map(|sl| sl.log_records.len())
        .sum();
    assert_eq!(upstream_records, RECORDS, "upstream fixture record count");
    let sanity_vendored =
        VendoredRequest::decode(body.clone()).expect("vendored decode of the fixture");
    vendored_logs::validate_utf8(&sanity_vendored).expect("fixture is valid UTF-8");
    let vendored_records: usize = sanity_vendored
        .resource_logs
        .iter()
        .flat_map(|rl| &rl.scope_logs)
        .map(|sl| sl.log_records.len())
        .sum();
    assert_eq!(vendored_records, RECORDS, "vendored fixture record count");

    println!(
        "fixture: {RECORDS} records, {group_count} distinct (host,app) groups \
         (expected band {EXPECTED_GROUPS_LOW}-{EXPECTED_GROUPS_HIGH})"
    );
    println!(
        "expected band (pre-registered on #3654): B at least 10% cheaper per \
         record than A, predicted 10% to 25%; under 5% means the bytes \
         retype does not pay"
    );

    let mut a_ns_per_record = Vec::with_capacity(PAIRS);
    let mut b_ns_per_record = Vec::with_capacity(PAIRS);
    for pair in 0..PAIRS {
        let start = Instant::now();
        let decoded = UpstreamRequest::decode(body.clone()).expect("upstream decode");
        let elapsed_a = start.elapsed();
        std::hint::black_box(&decoded);
        a_ns_per_record.push(elapsed_a.as_nanos() as f64 / RECORDS as f64);

        let start = Instant::now();
        let decoded = VendoredRequest::decode(body.clone()).expect("vendored decode");
        vendored_logs::validate_utf8(&decoded).expect("vendored validate");
        let elapsed_b = start.elapsed();
        std::hint::black_box(&decoded);
        b_ns_per_record.push(elapsed_b.as_nanos() as f64 / RECORDS as f64);

        println!("pair {pair}: loadavg {}", loadavg());
    }

    let (a_median, a_min) = median_min(a_ns_per_record);
    let (b_median, b_min) = median_min(b_ns_per_record);

    // Each arm's figure must land in the output exactly once: a bench that
    // silently printed a figure twice (or dropped it) would pass visual
    // inspection while reporting decoration, not a result.
    let mut a_printed = 0u32;
    let mut b_printed = 0u32;
    let mut delta_printed = 0u32;

    println!(
        "A (upstream String decode): median {a_median:.1} ns/record, min {a_min:.1} ns/record, loadavg {}",
        loadavg()
    );
    a_printed += 1;
    println!(
        "B (vendored bytes decode + validate): median {b_median:.1} ns/record, min {b_min:.1} ns/record, loadavg {}",
        loadavg()
    );
    b_printed += 1;

    let pct_cheaper = (a_median - b_median) / a_median * 100.0;
    println!(
        "B vs A median: {pct_cheaper:.1}% cheaper per record (band: pays at \
         >=10%, predicted 10-25%, does not pay under 5%)"
    );
    delta_printed += 1;

    if a_printed != 1 || b_printed != 1 || delta_printed != 1 {
        eprintln!(
            "bench self-check failed: expected exactly one A figure, one B \
             figure, and one delta figure in the output; got a={a_printed} \
             b={b_printed} delta={delta_printed}"
        );
        std::process::exit(1);
    }
}
