//! Turning a Flight SQL request into a [`SqlRequest`], and a [`SqlError`]
//! into a `Status`.
//!
//! # Why the window and timeout arrive as metadata
//!
//! `CommandStatementQuery` carries a statement and nothing else: the Flight
//! SQL protocol has no field for an event-time window or a per-request
//! deadline, both of which `POST /api/v1/sql` takes from its JSON body. gRPC
//! metadata is the only channel left, so the three body fields that have no
//! protocol equivalent get one metadata key each, with the same names, the
//! same units (Unix float seconds), and the same defaults the HTTP endpoint
//! applies. A Flight client that sets none of them gets exactly what an HTTP
//! client that sends none of them gets.
//!
//! The parsing is deliberately a second implementation rather than a shared
//! one: the HTTP version deserializes a JSON body in services/ravel-server and
//! this one reads ASCII metadata in ravel-sql, so there is no shared input
//! type to factor over, and the two crates are on opposite sides of the
//! ADR-0013 boundary. What must stay identical is the *semantics*, which the
//! tests below pin against the HTTP endpoint's rules.
//!
//! # Error mapping
//!
//! [`status_from_sql`] is this transport's redaction boundary, and it is the
//! same boundary the HTTP path has: the client gets
//! [`SqlError::client_message`] and never `{err}`, while the full error is
//! logged once with the tenant hash as a structured field. Only the status
//! mapping differs, because gRPC codes differ from HTTP ones:
//!
//! | [`ErrorClass`] | HTTP | gRPC |
//! |---|---|---|
//! | `BadRequest` | 400 | `INVALID_ARGUMENT` |
//! | `Unsupported` | 422 | `FAILED_PRECONDITION` |
//! | `Unavailable` | 503 | `UNAVAILABLE` |
//! | `Timeout` | 504 | `DEADLINE_EXCEEDED` |
//!
//! `Unsupported` is `FAILED_PRECONDITION` rather than `INVALID_ARGUMENT`
//! because the class covers requests that are well-formed but cannot be
//! served in the current state (budget exceeded, pool exhausted, unplannable),
//! which is what gRPC defines that code for. `INVALID_ARGUMENT` is reserved
//! for input that is wrong regardless of state, which is exactly the
//! `BadRequest` class.

use std::time::Duration;

use ravel_types::{CommitToken, TenantHash, TimeRange};
use tonic::{Status, metadata::MetadataMap};

use crate::error::{ErrorClass, SqlError};
use crate::executor::SqlRequest;

/// Window start, Unix float seconds. Defaults to one window-length before
/// `end`.
pub const START_KEY: &str = "x-ravel-start";
/// Window end, Unix float seconds. Defaults to the request's `now`.
pub const END_KEY: &str = "x-ravel-end";
/// Per-request wall deadline in seconds. Clamped to the server maximum.
pub const TIMEOUT_KEY: &str = "x-ravel-timeout";

const NS_PER_SEC: f64 = 1_000_000_000.0;

/// Build the [`SqlRequest`] for a statement, reading the window and timeout
/// from `metadata` and applying the same defaults and clamps the HTTP
/// endpoint applies.
pub(super) fn sql_request(
    sql: String,
    metadata: &MetadataMap,
    min_tokens: Vec<CommitToken>,
    now_ns: i64,
    config: &super::FlightSqlConfig,
) -> Result<SqlRequest, Status> {
    let end_ns = match seconds(metadata, END_KEY)? {
        Some(secs) => seconds_to_ns(END_KEY, secs)?,
        None => now_ns,
    };
    let window_ns = i64::try_from(config.default_window.as_nanos()).unwrap_or(i64::MAX);
    let start_ns = match seconds(metadata, START_KEY)? {
        Some(secs) => seconds_to_ns(START_KEY, secs)?,
        None => end_ns.saturating_sub(window_ns),
    };
    if start_ns > end_ns {
        return Err(Status::invalid_argument(format!(
            "{START_KEY} must not be after {END_KEY}"
        )));
    }

    // A client may shorten its own deadline but never extend it past the
    // server budget (finding a7-F01, same rule as the HTTP endpoint).
    let deadline = match seconds(metadata, TIMEOUT_KEY)? {
        Some(secs) if secs > 0.0 && secs.is_finite() => {
            Duration::from_secs_f64(secs).min(config.max_deadline)
        }
        Some(_) => {
            return Err(Status::invalid_argument(format!(
                "{TIMEOUT_KEY} must be a positive, finite number of seconds"
            )));
        }
        None => config.max_deadline,
    };

    Ok(SqlRequest {
        sql,
        window: TimeRange { start_ns, end_ns },
        min_tokens,
        now_ns,
        deadline,
    })
}

/// Read one optional float-seconds metadata value.
fn seconds(metadata: &MetadataMap, key: &str) -> Result<Option<f64>, Status> {
    let Some(value) = metadata.get(key) else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| Status::invalid_argument(format!("{key} must be ASCII")))?;
    // The value is the caller's own input, so quoting it back is safe and is
    // what makes the error actionable (the rule sql.rs follows for tokens).
    let parsed: f64 = raw
        .parse()
        .map_err(|_| Status::invalid_argument(format!("invalid {key}: {raw:?}")))?;
    Ok(Some(parsed))
}

fn seconds_to_ns(name: &str, secs: f64) -> Result<i64, Status> {
    if !secs.is_finite() {
        return Err(Status::invalid_argument(format!(
            "{name} must be a finite number of Unix seconds"
        )));
    }
    let ns = secs * NS_PER_SEC;
    if ns < i64::MIN as f64 || ns > i64::MAX as f64 {
        return Err(Status::invalid_argument(format!(
            "{name} is out of the representable nanosecond range"
        )));
    }
    Ok(ns as i64)
}

/// The redaction boundary: log the full error once, return only the client
/// message. Never formats `err` into the returned `Status`.
pub(super) fn status_from_sql(err: &SqlError, tenant: TenantHash) -> Status {
    let message = err.client_message();
    let code = match err.class() {
        ErrorClass::BadRequest => tonic::Code::InvalidArgument,
        ErrorClass::Unsupported => tonic::Code::FailedPrecondition,
        ErrorClass::Unavailable => tonic::Code::Unavailable,
        ErrorClass::Timeout => tonic::Code::DeadlineExceeded,
    };

    // Client-caused rejections are not operational events; log them at debug
    // so a scripted client cannot flood warn-level logs. Same split as the
    // HTTP endpoint.
    if code == tonic::Code::InvalidArgument {
        tracing::debug!(
            tenant = %tenant.to_hex(),
            error = %err,
            client_message = %message,
            "flight sql request rejected",
        );
    } else {
        tracing::warn!(
            tenant = %tenant.to_hex(),
            error = %err,
            client_message = %message,
            "flight sql query error redacted from client response",
        );
    }

    Status::new(code, message)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use crate::ValidationError;
    use crate::flight::FlightSqlConfig;

    const ONE_HOUR_NS: i64 = 3_600_000_000_000;

    fn config() -> FlightSqlConfig {
        FlightSqlConfig {
            max_deadline: Duration::from_secs(30),
            ..FlightSqlConfig::default()
        }
    }

    fn metadata(pairs: &[(&'static str, &str)]) -> MetadataMap {
        let mut map = MetadataMap::new();
        for (key, value) in pairs {
            map.insert(*key, value.parse().expect("ascii value"));
        }
        map
    }

    fn build(pairs: &[(&'static str, &str)], now_ns: i64) -> Result<SqlRequest, Status> {
        sql_request(
            "SELECT 1".to_string(),
            &metadata(pairs),
            Vec::new(),
            now_ns,
            &config(),
        )
    }

    #[test]
    fn an_absent_window_defaults_to_the_last_hour_ending_now() {
        let request = build(&[], 5_000_000_000).expect("request");
        assert_eq!(request.now_ns, 5_000_000_000);
        assert_eq!(request.window.end_ns, 5_000_000_000);
        assert_eq!(request.window.start_ns, 5_000_000_000 - ONE_HOUR_NS);
    }

    /// a7-F01 on this transport: a client timeout above the server maximum is
    /// clamped, never honored verbatim.
    #[test]
    fn a_timeout_above_the_server_maximum_is_clamped() {
        let request = build(&[(TIMEOUT_KEY, "3600")], 0).expect("request");
        assert_eq!(request.deadline, Duration::from_secs(30));
    }

    #[test]
    fn a_timeout_below_the_server_maximum_is_honored() {
        let request = build(&[(TIMEOUT_KEY, "5")], 0).expect("request");
        assert_eq!(request.deadline, Duration::from_secs(5));
    }

    #[test]
    fn a_non_positive_or_non_finite_timeout_is_rejected() {
        for raw in ["0", "-1", "inf", "NaN"] {
            let status = build(&[(TIMEOUT_KEY, raw)], 0).expect_err("rejected");
            assert_eq!(status.code(), tonic::Code::InvalidArgument, "{raw}");
        }
    }

    #[test]
    fn an_inverted_window_is_rejected() {
        let status = build(&[(START_KEY, "10"), (END_KEY, "1")], 0).expect_err("rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn seconds_convert_to_nanoseconds() {
        let request = build(&[(START_KEY, "1.5"), (END_KEY, "2.5")], 0).expect("request");
        assert_eq!(request.window.start_ns, 1_500_000_000);
        assert_eq!(request.window.end_ns, 2_500_000_000);
    }

    #[test]
    fn an_unparsable_window_bound_is_rejected() {
        let status = build(&[(START_KEY, "yesterday")], 0).expect_err("rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// The gRPC status mapping is part of the client contract; pin every
    /// class, and pin that no unredacted detail reaches the message.
    #[test]
    fn error_classes_map_to_stable_grpc_codes() {
        let tenant = TenantHash([0u8; 16]);
        let cases: Vec<(SqlError, tonic::Code)> = vec![
            (
                SqlError::Validation(ValidationError::NotReadOnly { kind: "INSERT" }),
                tonic::Code::InvalidArgument,
            ),
            (SqlError::SnapshotInvalidated, tonic::Code::Unavailable),
            (
                SqlError::DeadlineExceeded { millis: 30_000 },
                tonic::Code::DeadlineExceeded,
            ),
            (
                SqlError::Plan("No field named samples.nope".to_string()),
                tonic::Code::FailedPrecondition,
            ),
        ];
        for (err, code) in cases {
            let status = status_from_sql(&err, tenant);
            assert_eq!(status.code(), code);
            assert!(
                !status.message().contains("samples.nope"),
                "plan detail must not reach the client"
            );
        }
    }
}
