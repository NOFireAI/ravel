//! HTTP client for the pinned Prometheus instance: pushes the encoded
//! Remote Write 1.0 payload, then issues the same `/api/v1/query` and
//! `/api/v1/query_range` calls the runner also issues against Ravel.
//!
//! `reqwest` is a crate-local external dependency
//! (crates/ravel-promql-difftest/Cargo.toml only).

use serde_json::Value as Json;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum PrometheusClientError {
    #[error("http request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("remote-write push rejected: status {status}, body: {body}")]
    RemoteWriteRejected { status: u16, body: String },
    #[error("prometheus did not become ready within {0:?}")]
    NotReady(Duration),
}

pub struct PrometheusClient {
    http: reqwest::Client,
    base_url: String,
}

impl PrometheusClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        PrometheusClient {
            http: reqwest::Client::new(),
            base_url: base_url.into(),
        }
    }

    /// Polls `/-/ready` until it returns 2xx or `timeout` elapses.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<(), PrometheusClientError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(resp) = self
                .http
                .get(format!("{}/-/ready", self.base_url))
                .send()
                .await
                && resp.status().is_success()
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(PrometheusClientError::NotReady(timeout));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// POSTs a pre-encoded (protobuf + snappy) Remote Write 1.0 body.
    pub async fn remote_write(&self, body: Vec<u8>) -> Result<(), PrometheusClientError> {
        let resp = self
            .http
            .post(format!("{}/api/v1/write", self.base_url))
            .header("Content-Encoding", "snappy")
            .header("Content-Type", "application/x-protobuf")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(PrometheusClientError::RemoteWriteRejected {
                status: status.as_u16(),
                body,
            });
        }
        Ok(())
    }

    /// `/api/v1/query` at `time_ms`. Returns the parsed JSON envelope even
    /// when the HTTP status is an error (`status: "error"` bodies are the
    /// comparator's input on the error-corpus path).
    pub async fn instant_query(
        &self,
        query: &str,
        time_ms: i64,
    ) -> Result<Json, PrometheusClientError> {
        let resp = self
            .http
            .get(format!("{}/api/v1/query", self.base_url))
            .query(&[("query", query), ("time", &ms_to_seconds_str(time_ms))])
            .send()
            .await?;
        Ok(resp.json::<Json>().await?)
    }

    /// `/api/v1/query_range` over `[start_ms, end_ms]` stepped by `step_ms`.
    pub async fn range_query(
        &self,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Json, PrometheusClientError> {
        let resp = self
            .http
            .get(format!("{}/api/v1/query_range", self.base_url))
            .query(&[
                ("query", query.to_string()),
                ("start", ms_to_seconds_str(start_ms)),
                ("end", ms_to_seconds_str(end_ms)),
                ("step", format!("{}ms", step_ms)),
            ])
            .send()
            .await?;
        Ok(resp.json::<Json>().await?)
    }
}

/// Formats milliseconds as a `[-]seconds.millis` decimal. Splitting on
/// `unsigned_abs` rather than `div_euclid`/`rem_euclid` matters for negative
/// values: `-500ms` must render as `-0.500` (i.e. `-0.5`), not `-1.500`
/// (`-1.5`), which is what a naive euclidean split of the sign and
/// fractional part would produce.
fn ms_to_seconds_str(ms: i64) -> String {
    let sign = if ms < 0 { "-" } else { "" };
    let abs = ms.unsigned_abs();
    format!("{sign}{}.{:03}", abs / 1000, abs % 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_milliseconds_as_seconds_with_millis_precision() {
        assert_eq!(ms_to_seconds_str(1_700_000_000_123), "1700000000.123");
        assert_eq!(ms_to_seconds_str(0), "0.000");
    }

    #[test]
    fn formats_negative_milliseconds_correctly() {
        assert_eq!(ms_to_seconds_str(-500), "-0.500");
        assert_eq!(ms_to_seconds_str(-1_500), "-1.500");
        assert_eq!(ms_to_seconds_str(-1_700_000_000_123), "-1700000000.123");
    }
}
