//! HTTP calls to ravel-server (OTLP/HTTP export, the SQL endpoint, readiness)
//! and the OTLP bodies the workload sends. Every call has a timeout and
//! returns what the client saw, never a panic.

use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use reqwest::blocking::Client;

use crate::model::{Placement, SUBJECTS};

pub const COMMIT_TOKEN_HEADER: &str = "x-ravel-commit-token";
pub const INGEST_MODE_HEADER: &str = "x-ravel-ingest-mode";
/// The attribute (and series label) naming a record's subject; the erasure
/// matcher's key.
pub const SUBJECT_KEY: &str = "user_id";
/// Log record attribute holding the record's unique id.
pub const ID_ATTR: &str = "wl.id";
/// The gauge every metrics write appends to; a sample's value is its id.
pub const METRIC_NAME: &str = "lc_gauge";

#[derive(Clone, Debug)]
pub enum Reply {
    Status {
        status: u16,
        commit_token: Option<String>,
        body: Vec<u8>,
    },
    /// No response: the request may or may not have taken effect.
    NoResponse,
}

#[derive(Clone, Debug)]
pub enum SqlFailure {
    NoResponse(String),
    Status(u16, String),
    BadBody(String),
}

impl std::fmt::Display for SqlFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlFailure::NoResponse(e) => write!(f, "no response: {e}"),
            SqlFailure::Status(s, body) => write!(f, "status {s}: {body}"),
            SqlFailure::BadBody(e) => write!(f, "unexpected body: {e}"),
        }
    }
}

pub struct Ravel {
    client: Client,
    pub nodes: Vec<String>,
    token: String,
}

impl Ravel {
    pub fn new(nodes: Vec<String>, token: String) -> Result<Ravel, String> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .pool_max_idle_per_host(2)
            .build()
            .map_err(|e| format!("building the HTTP client: {e}"))?;
        Ok(Ravel {
            client,
            nodes,
            token,
        })
    }

    pub fn export(
        &self,
        node: usize,
        path: &str,
        body: Vec<u8>,
        buffered: bool,
        timeout: Duration,
    ) -> Reply {
        let mut req = self
            .client
            .post(format!("{}{}", self.nodes[node], path))
            .timeout(timeout)
            .bearer_auth(&self.token)
            .header("content-type", "application/x-protobuf");
        if buffered {
            req = req.header(INGEST_MODE_HEADER, "buffered");
        }
        match req.body(body).send() {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let commit_token = resp
                    .headers()
                    .get(COMMIT_TOKEN_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                match resp.bytes() {
                    Ok(body) => Reply::Status {
                        status,
                        commit_token,
                        body: body.to_vec(),
                    },
                    Err(_) => Reply::NoResponse,
                }
            }
            Err(_) => Reply::NoResponse,
        }
    }

    /// One SQL statement over `[start_s, end_s]`, bounded below by `tokens`.
    pub fn sql(
        &self,
        node: usize,
        query: &str,
        start_s: f64,
        end_s: f64,
        tokens: &[String],
        timeout: Duration,
    ) -> Result<Vec<serde_json::Value>, SqlFailure> {
        let body = serde_json::json!({
            "query": query,
            "start": start_s,
            "end": end_s,
            "min_commit_token": tokens,
        });
        let resp = self
            .client
            .post(format!("{}/api/v1/sql", self.nodes[node]))
            .timeout(timeout)
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .map_err(|e| SqlFailure::NoResponse(e.to_string()))?;
        let status = resp.status().as_u16();
        let text = resp
            .text()
            .map_err(|e| SqlFailure::NoResponse(format!("reading the body: {e}")))?;
        if status != 200 {
            return Err(SqlFailure::Status(status, truncate(&text, 300)));
        }
        let value: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| SqlFailure::BadBody(format!("{e}: {}", truncate(&text, 300))))?;
        match value.pointer("/data/rows") {
            Some(serde_json::Value::Array(rows)) => Ok(rows.clone()),
            _ => Err(SqlFailure::BadBody(truncate(&text, 300))),
        }
    }

    pub fn ready(&self, node: usize) -> Result<(), String> {
        match self
            .client
            .get(format!("{}/readyz", self.nodes[node]))
            .timeout(Duration::from_secs(2))
            .send()
        {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => Err(format!("readyz answered {}", resp.status())),
            Err(e) => Err(e.to_string()),
        }
    }
}

pub fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn subject_name(subject: u8) -> &'static str {
    SUBJECTS.get(usize::from(subject)).copied().unwrap_or("?")
}

/// A record's event time: unique per record id within each microsecond, so
/// two metrics samples of one series never share a timestamp.
pub fn record_ts(write_ts_ns: u64, id: u64) -> u64 {
    (write_ts_ns / 1000) * 1000 + id % 1000
}

/// One log export. Each record gets its own resource and scope, so its
/// subject sits exactly where its placement says and nowhere else.
pub fn logs_request(ts_ns: u64, first_id: u64, records: &[(u8, Placement)]) -> Vec<u8> {
    let resource_logs = records
        .iter()
        .enumerate()
        .map(|(i, &(subject, placement))| {
            let id = first_id + i as u64;
            let s = subject_name(subject);
            let mut resource_attrs = vec![kv("service.name", "lc-svc")];
            let mut scope_attrs = Vec::new();
            let mut record_attrs = vec![kv(ID_ATTR, &id.to_string())];
            match placement {
                Placement::Resource => resource_attrs.push(kv(SUBJECT_KEY, s)),
                Placement::Scope => scope_attrs.push(kv(SUBJECT_KEY, s)),
                Placement::Record | Placement::Series => record_attrs.push(kv(SUBJECT_KEY, s)),
            }
            let ts = record_ts(ts_ns, id);
            ResourceLogs {
                resource: Some(Resource {
                    attributes: resource_attrs,
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "lc".to_string(),
                        attributes: scope_attrs,
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: ts,
                        observed_time_unix_nano: ts,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(format!("lc id={id}"))),
                        }),
                        attributes: record_attrs,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportLogsServiceRequest { resource_logs }.encode_to_vec()
}

/// One metrics export: one `lc_gauge` sample per record, labelled with its
/// subject, valued with its id.
pub fn metrics_request(ts_ns: u64, first_id: u64, records: &[(u8, Placement)]) -> Vec<u8> {
    let data_points = records
        .iter()
        .enumerate()
        .map(|(i, &(subject, _))| {
            let id = first_id + i as u64;
            NumberDataPoint {
                attributes: vec![kv(SUBJECT_KEY, subject_name(subject))],
                time_unix_nano: record_ts(ts_ns, id),
                value: Some(number_data_point::Value::AsDouble(id as f64)),
                ..Default::default()
            }
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "lc-metrics")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: METRIC_NAME.to_string(),
                    data: Some(metric::Data::Gauge(Gauge { data_points })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// Whether a 2xx body is a clean acknowledgement: no partial-success
/// message at all (a zero-count one is still a partial success).
pub fn clean_success(logs: bool, body: &[u8]) -> bool {
    if logs {
        matches!(ExportLogsServiceResponse::decode(body), Ok(r) if r.partial_success.is_none())
    } else {
        matches!(ExportMetricsServiceResponse::decode(body), Ok(r) if r.partial_success.is_none())
    }
}

/// The SQL that reads back every workload record of a signal, or only the
/// ids in `ids`. Logs group by id; metrics return one row per sample.
pub fn read_query(logs: bool, ids: Option<(u64, u64)>) -> String {
    if logs {
        let filter = match ids {
            Some((lo, hi)) => format!(
                "attrs['{ID_ATTR}'] IN ({})",
                (lo..=hi)
                    .map(|i| format!("'{i}'"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            None => format!("attrs['{ID_ATTR}'] IS NOT NULL"),
        };
        format!(
            "SELECT attrs['{ID_ATTR}'] AS id, count(*) AS n FROM logs WHERE {filter} \
             GROUP BY attrs['{ID_ATTR}']"
        )
    } else {
        let filter = match ids {
            Some((lo, hi)) => format!(" AND value >= {lo} AND value <= {hi}"),
            None => String::new(),
        };
        format!(
            "SELECT ts, value FROM samples WHERE label(labels, '__name__') = '{METRIC_NAME}'{filter}"
        )
    }
}

fn cell_u64(cell: &serde_json::Value) -> Option<u64> {
    match cell {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Record id to copies, from the rows `read_query` returns.
pub fn read_counts(
    logs: bool,
    rows: &[serde_json::Value],
) -> Result<std::collections::HashMap<u64, u64>, String> {
    let mut counts = std::collections::HashMap::new();
    for row in rows {
        let cells = row
            .as_array()
            .ok_or_else(|| format!("row is not an array: {row}"))?;
        if logs {
            let id = cells.first().and_then(cell_u64);
            let n = cells.get(1).and_then(cell_u64);
            match (id, n) {
                (Some(id), Some(n)) => *counts.entry(id).or_insert(0) += n,
                _ => return Err(format!("unparseable logs row: {row}")),
            }
        } else {
            let v = cells.get(1).and_then(|c| match c {
                serde_json::Value::Number(n) => n.as_f64(),
                serde_json::Value::String(s) => s.parse().ok(),
                _ => None,
            });
            match v {
                Some(v) if v >= 0.0 && v.fract() == 0.0 && v < 9.0e15 => {
                    *counts.entry(v as u64).or_insert(0) += 1;
                }
                _ => return Err(format!("unparseable samples row: {row}")),
            }
        }
    }
    Ok(counts)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn each_placement_puts_the_subject_in_one_place() {
        let body = logs_request(
            1_000_000_000,
            40,
            &[
                (0, Placement::Resource),
                (1, Placement::Scope),
                (2, Placement::Record),
            ],
        );
        let req = ExportLogsServiceRequest::decode(body.as_slice()).expect("decode");
        let has = |kvs: &[KeyValue]| {
            kvs.iter()
                .find(|k| k.key == SUBJECT_KEY)
                .and_then(|k| k.value.as_ref())
                .and_then(|v| match &v.value {
                    Some(any_value::Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                })
        };
        let places: Vec<_> = req
            .resource_logs
            .iter()
            .map(|rl| {
                let res = has(&rl.resource.as_ref().expect("resource").attributes);
                let sl = &rl.scope_logs[0];
                let scope = has(&sl.scope.as_ref().expect("scope").attributes);
                let rec = has(&sl.log_records[0].attributes);
                (res, scope, rec)
            })
            .collect();
        assert_eq!(
            places,
            vec![
                (Some("s0".into()), None, None),
                (None, Some("s1".into()), None),
                (None, None, Some("s2".into())),
            ]
        );
    }

    #[test]
    fn rows_count_per_id() {
        let rows = vec![serde_json::json!(["7", 2]), serde_json::json!([8, "1"])];
        let c = read_counts(true, &rows).expect("logs");
        assert_eq!((c[&7], c[&8]), (2, 1));
        let rows = vec![
            serde_json::json!([1, 7.0]),
            serde_json::json!([2, 7.0]),
            serde_json::json!([3, "9"]),
        ];
        let c = read_counts(false, &rows).expect("metrics");
        assert_eq!((c[&7], c[&9]), (2, 1));
        assert!(read_counts(false, &[serde_json::json!([1, 7.5])]).is_err());
        assert!(read_counts(true, &[serde_json::json!(["x", 1])]).is_err());
    }

    #[test]
    fn timestamps_are_unique_per_id_within_a_microsecond() {
        let a = record_ts(5_000_123_456, 41);
        let b = record_ts(5_000_123_999, 42);
        assert_ne!(a, b);
        assert_eq!(a / 1000, 5_000_123);
    }
}
