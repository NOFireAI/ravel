//! `GET /api/traces/{trace_id}`: every span of one trace, as the rows of the
//! `spans` table.
//!
//! The route is a fixed statement over the SQL surface, not a second read
//! path: it builds `SELECT * FROM spans WHERE trace_id = X'..' ORDER BY
//! start_ts, span_id` and runs it through [`QueryService::sql_execute_timed`],
//! so authentication, tenant resolution, admission, the deadline clamp, the
//! usage record, the audit record and error redaction are the SQL endpoint's
//! own. The JSON body is the SQL endpoint's envelope (`status`, `data`,
//! `stats`), and `Accept: application/vnd.apache.arrow.stream` yields Arrow
//! IPC exactly as it does there.
//!
//! # Request
//!
//! ```text
//! GET /api/traces/00112233445566778899aabbccddeeff?start=1735689600&end=1735693200
//! ```
//!
//! - `trace_id` is 32 hex characters, either case. Anything else is 400
//!   `bad_data` before any store read.
//! - `start` and `end` are Unix float seconds, parsed as the SQL endpoint
//!   parses its own, and bound which objects are listed.
//! - `timeout` (seconds) can only lower the server deadline.
//! - `min_commit_token` may repeat.
//!
//! An unknown trace is 200 with zero rows.
//!
//! # Window
//!
//! See [`lookup_window`]: a lookup that names no window resolves the whole
//! tenant rather than the SQL endpoint's one-hour default.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ravel_query::RequestBudgets;
use ravel_sql::SqlRequest;
use ravel_types::{CommitToken, TimeRange};

use crate::service::{ApiError, QueryService, ServiceError};
use crate::sql::SqlState;

/// The route this module serves, in axum's path syntax.
pub const ROUTE: &str = "/api/traces/{trace_id}";

/// The `GET /api/traces/{trace_id}` router.
pub fn router(state: SqlState) -> Router {
    Router::new().route(ROUTE, get(handle)).with_state(state)
}

/// One trace lookup, independent of the transport that asked for it.
#[derive(Debug, Clone)]
pub struct TraceLookup {
    pub trace_id: [u8; 16],
    /// Window start in nanoseconds; `None` when the caller gave none.
    pub start_ns: Option<i64>,
    /// Window end in nanoseconds; `None` when the caller gave none.
    pub end_ns: Option<i64>,
    pub min_tokens: Vec<CommitToken>,
    pub deadline: Duration,
    /// Apply an explicit window as a row filter on `start_ts` as well as the
    /// listing bound. Ignored when the lookup names no window.
    pub row_window: bool,
    pub max_rows: Option<usize>,
    pub budgets: Option<RequestBudgets>,
}

/// Parse a trace id: exactly 32 hex characters, either case.
pub fn parse_trace_id(raw: &str) -> Result<[u8; 16], ApiError> {
    let invalid = || {
        ApiError::bad_request(format!(
            "invalid trace id {raw:?}: expected 32 hexadecimal characters"
        ))
    };
    let bytes = raw.as_bytes();
    if bytes.len() != 32 {
        return Err(invalid());
    }
    let mut id = [0u8; 16];
    for (out, pair) in id.iter_mut().zip(bytes.chunks_exact(2)) {
        let hi = hex_nibble(pair[0]).ok_or_else(invalid)?;
        let lo = hex_nibble(pair[1]).ok_or_else(invalid)?;
        *out = hi << 4 | lo;
    }
    Ok(id)
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// The statement every trace lookup runs.
pub fn trace_statement(trace_id: &[u8; 16]) -> String {
    let hex: String = trace_id.iter().map(|b| format!("{b:02x}")).collect();
    format!("SELECT * FROM spans WHERE trace_id = X'{hex}' ORDER BY start_ts, span_id")
}

/// The resolve window of a trace lookup.
///
/// A lookup pinned to one trace id with no explicit start resolves from time
/// zero, so it covers every folded part and the unsealed tail the tenant still
/// retains, instead of the SQL endpoint's one hour before `end`; explicit
/// bounds are kept exactly and an omitted `end` is `now_ns`.
pub fn lookup_window(
    start_ns: Option<i64>,
    end_ns: Option<i64>,
    now_ns: i64,
) -> Result<TimeRange, ApiError> {
    let end_ns = end_ns.unwrap_or(now_ns);
    let start_ns = start_ns.unwrap_or(0);
    if start_ns > end_ns {
        return Err(ApiError::bad_request(
            "\"start\" must not be after \"end\"".to_string(),
        ));
    }
    Ok(TimeRange { start_ns, end_ns })
}

impl TraceLookup {
    /// The SQL request this lookup runs, read against `now_ns`.
    pub fn sql_request(&self, now_ns: i64) -> Result<SqlRequest, ApiError> {
        let explicit = self.start_ns.is_some() || self.end_ns.is_some();
        Ok(SqlRequest {
            sql: trace_statement(&self.trace_id),
            window: lookup_window(self.start_ns, self.end_ns, now_ns)?,
            min_tokens: self.min_tokens.clone(),
            now_ns,
            deadline: self.deadline,
            row_window: self.row_window && explicit,
            max_rows: self.max_rows,
            budgets: self.budgets,
        })
    }
}

async fn handle(
    State(state): State<SqlState>,
    Path(trace_id): Path<String>,
    req: Request<Body>,
) -> Response {
    match run(&state, &trace_id, req).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

async fn run(
    state: &SqlState,
    raw_trace_id: &str,
    req: Request<Body>,
) -> Result<Response, ServiceError> {
    let headers = req.headers().clone();
    let principal =
        crate::service::authenticate_principal(state.tenant_resolver.as_ref(), &headers)?;
    let tenant_hash = principal.tenant.hash();

    let trace_id = parse_trace_id(raw_trace_id)?;
    let params = crate::exemplars::Params::parse(req.uri().query(), None);
    let window_bound = |name: &'static str| -> Result<Option<i64>, ApiError> {
        params
            .first(name)
            .map(|raw| {
                let secs = raw
                    .parse::<f64>()
                    .map_err(|_| ApiError::invalid_param(name, raw))?;
                crate::sql::seconds_to_ns(name, secs)
            })
            .transpose()
    };
    let timeout = params
        .first("timeout")
        .map(|raw| {
            raw.parse::<f64>()
                .map_err(|_| ApiError::invalid_param("timeout", raw))
        })
        .transpose()?;
    let lookup = TraceLookup {
        trace_id,
        start_ns: window_bound("start")?,
        end_ns: window_bound("end")?,
        min_tokens: crate::sql::decode_min_tokens(
            params.all("min_commit_token").iter().map(String::as_str),
        )?,
        deadline: crate::sql::clamp_timeout(timeout, state.max_deadline)?,
        // The SQL endpoint's window bounds the listing only, and so does this
        // one.
        row_window: false,
        max_rows: None,
        budgets: None,
    };

    let service = state.service();
    let request = service.trace_request(&lookup)?;
    let execution = service.sql_execute_timed(tenant_hash, &request).await?;
    crate::sql::respond(&headers, execution, tenant_hash)
}

impl QueryService {
    /// The SQL request a trace lookup runs, read against this service's
    /// clock. Every transport that serves a trace lookup builds its request
    /// here and runs it through [`QueryService::sql_execute_timed`].
    pub fn trace_request(&self, lookup: &TraceLookup) -> Result<SqlRequest, ApiError> {
        lookup.sql_request(self.now_ns())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn trace_ids_parse_in_either_case_and_nothing_else_does() {
        let lower = parse_trace_id("00112233445566778899aabbccddeeff").expect("lowercase");
        let upper = parse_trace_id("00112233445566778899AABBCCDDEEFF").expect("uppercase");
        assert_eq!(lower, upper);
        assert_eq!(lower[0], 0x00);
        assert_eq!(lower[15], 0xff);
        for bad in [
            "",
            "0011223344556677",
            "00112233445566778899aabbccddeeff00",
            "00112233445566778899aabbccddeefg",
            "0x112233445566778899aabbccddeeff",
            "00112233445566778899aabbccddee f",
        ] {
            let err = parse_trace_id(bad).expect_err(bad);
            assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST, "{bad}");
            assert_eq!(err.error_type, "bad_data", "{bad}");
        }
    }

    #[test]
    fn the_statement_pins_the_trace_id_as_a_binary_literal() {
        assert_eq!(
            trace_statement(&[0xab; 16]),
            "SELECT * FROM spans WHERE trace_id = X'abababababababababababababababab' \
             ORDER BY start_ts, span_id"
        );
    }

    #[test]
    fn an_omitted_start_reaches_back_to_time_zero_and_explicit_bounds_are_kept() {
        let now = 500 * 3_600_000_000_000;
        assert_eq!(
            lookup_window(None, None, now).expect("no window"),
            TimeRange {
                start_ns: 0,
                end_ns: now
            }
        );
        assert_eq!(
            lookup_window(Some(7), Some(9), now).expect("explicit"),
            TimeRange {
                start_ns: 7,
                end_ns: 9
            }
        );
        assert_eq!(
            lookup_window(None, Some(9), now).expect("end only"),
            TimeRange {
                start_ns: 0,
                end_ns: 9
            }
        );
        assert_eq!(
            lookup_window(Some(7), None, now).expect("start only"),
            TimeRange {
                start_ns: 7,
                end_ns: now
            }
        );
        assert!(lookup_window(Some(10), Some(9), now).is_err());
    }
}
