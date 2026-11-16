//! The HTTP calls the driver makes to ravel-server: OTLP/HTTP exports, the SQL
//! endpoint, and readiness. Every call has a timeout and returns what the
//! client saw, never a panic.

use std::io::Write as _;
use std::time::Duration;

use reqwest::blocking::Client;

pub const COMMIT_TOKEN_HEADER: &str = "x-ravel-commit-token";
pub const IDEMPOTENCY_KEY_HEADER: &str = "x-ravel-idempotency-key";
pub const INGEST_MODE_HEADER: &str = "x-ravel-ingest-mode";

/// What the client saw for one HTTP request.
#[derive(Clone, Debug)]
pub enum Reply {
    /// A response arrived: its status, the commit-token header if any, and
    /// the body.
    Status {
        status: u16,
        commit_token: Option<String>,
        body: Vec<u8>,
    },
    /// No response: a connect failure, a reset, or the client timeout. The
    /// request may or may not have taken effect.
    NoResponse(String),
}

pub struct Ravel {
    client: Client,
    pub nodes: Vec<String>,
    token: String,
}

impl Ravel {
    pub fn new(nodes: Vec<String>, token: String) -> Result<Self, String> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .pool_max_idle_per_host(4)
            .build()
            .map_err(|e| format!("building the HTTP client: {e}"))?;
        Ok(Ravel {
            client,
            nodes,
            token,
        })
    }

    /// POST one OTLP/HTTP export to `node`.
    #[allow(clippy::too_many_arguments)]
    pub fn export(
        &self,
        node: usize,
        path: &str,
        body: &[u8],
        mode_header: Option<&str>,
        idempotency_key: Option<&str>,
        gzip: bool,
        timeout: Duration,
    ) -> Reply {
        let url = format!("{}{}", self.nodes[node], path);
        let mut req = self
            .client
            .post(url)
            .timeout(timeout)
            .bearer_auth(&self.token)
            .header("content-type", "application/x-protobuf");
        if let Some(mode) = mode_header {
            req = req.header(INGEST_MODE_HEADER, mode);
        }
        if let Some(key) = idempotency_key {
            req = req.header(IDEMPOTENCY_KEY_HEADER, key);
        }
        let payload = if gzip {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            if enc.write_all(body).is_err() {
                return Reply::NoResponse("gzip encode failed".to_string());
            }
            match enc.finish() {
                Ok(bytes) => {
                    req = req.header("content-encoding", "gzip");
                    bytes
                }
                Err(e) => return Reply::NoResponse(format!("gzip encode failed: {e}")),
            }
        } else {
            body.to_vec()
        };
        match req.body(payload).send() {
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
                    Err(e) => Reply::NoResponse(format!("reading the body: {e}")),
                }
            }
            Err(e) => Reply::NoResponse(e.to_string()),
        }
    }

    /// Run one SQL statement on `node` over the window `[start_s, end_s]`
    /// (Unix seconds), bounded below by `tokens` (`<table>:<token>` each).
    pub fn sql(
        &self,
        node: usize,
        query: &str,
        start_s: f64,
        end_s: f64,
        tokens: &[String],
        timeout: Duration,
    ) -> Result<serde_json::Value, SqlFailure> {
        let url = format!("{}/api/v1/sql", self.nodes[node]);
        let body = serde_json::json!({
            "query": query,
            "start": start_s,
            "end": end_s,
            "min_commit_token": tokens,
        });
        let resp = self
            .client
            .post(url)
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
            return Err(SqlFailure::Status(status, truncate(&text, 400)));
        }
        let value: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| SqlFailure::BadBody(format!("{e}: {}", truncate(&text, 400))))?;
        match value.pointer("/data/rows") {
            Some(serde_json::Value::Array(_)) => Ok(value),
            _ => Err(SqlFailure::BadBody(truncate(&text, 400))),
        }
    }

    /// Whether `node` reports ready on `/readyz`.
    pub fn ready(&self, node: usize) -> Result<(), String> {
        let url = format!("{}/readyz", self.nodes[node]);
        match self.client.get(url).timeout(Duration::from_secs(2)).send() {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => Err(format!("readyz answered {}", resp.status())),
            Err(e) => Err(e.to_string()),
        }
    }
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

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

/// The rows of a successful SQL response.
pub fn rows(value: &serde_json::Value) -> &[serde_json::Value] {
    value
        .pointer("/data/rows")
        .and_then(|r| r.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// A cell as a u64: a JSON number, or a decimal string.
pub fn cell_u64(cell: &serde_json::Value) -> Option<u64> {
    match cell {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// A cell as an i64: a JSON number, or a decimal string.
pub fn cell_i64(cell: &serde_json::Value) -> Option<i64> {
    match cell {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// A cell as an f64.
pub fn cell_f64(cell: &serde_json::Value) -> Option<f64> {
    match cell {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}
