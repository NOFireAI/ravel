//! MetricsBench status mapper (ADR-0927 decision 6 and the 2026-10-26
//! amendment, sections E and F; issue #938, task T3 of #2821).
//!
//! [`classify`] turns one engine's [`QueryOutcome`] for one corpus case into
//! exactly one of the seven [`ResultStatus`] values, comparing returned data
//! against the oracle through `ravel_promql_difftest::comparator::compare`
//! rather than a second comparator. The rules, in the order they apply:
//!
//! - `timeout`: the harness deadline expired ([`QueryOutcome::Timeout`]), in
//!   any phase. An engine's own configured query timeout firing is a budget
//!   refusal instead (amendment F lists `--query.timeout` and
//!   `-search.maxQueryDuration` among the limits whose firing lands in
//!   `refused`), matched through [`budget_signatures`].
//! - `error` from the transport: [`QueryOutcome::Transport`] and
//!   [`QueryOutcome::BodyTooLarge`], with `error_class` naming the kind (and
//!   the HTTP status when headers arrived before the failure).
//! - `ok`: HTTP 2xx, `status: "success"`, no partial signal, and the data
//!   matches the oracle's.
//! - `incorrect`: a 2xx success whose data does not match the oracle's, an
//!   empty result where the oracle's is not, or a partial response whose
//!   returned data does not match (then `incomplete` is set). A
//!   VictoriaMetrics mismatch on an entry using `rate` or `increase` carries a
//!   `semantic_difference:` prefix on its message: MetricsQL's default
//!   rate/increase semantics differ from PromQL's, and that is reported, not
//!   tolerated.
//! - `partial`: a 2xx success carrying the engine's partial signal (Ravel's
//!   top-level `partial: true`, which only an `allow_partial` request can
//!   receive; VictoriaMetrics' `isPartial: true`) whose returned data matches.
//! - `refused` with reason `partial_coverage`: Ravel's 503 `unavailable`
//!   partial-coverage refusal carrying no data.
//! - `refused` with reason `budget`: an error envelope matching a row of the
//!   engine's [`budget_signatures`] table.
//! - `unsupported_construct`: an error envelope matching the engine's
//!   documented not-implemented error.
//! - `error`: any other non-2xx or `status: "error"` envelope, with
//!   `error_class` the engine's `errorType`. No status code is mapped on its
//!   own: a 503 is `refused`, `error` or neither depending on its body.
//!
//! `warnings` and `infos` are recorded on [`Classified::annotations`] and
//! never change the status: the data comparison runs on `status` and `data`
//! only, so a warning or info on a complete, matching result stays `ok`.
//!
//! Two outcomes are failures of the experiment, not statuses of the engine:
//! an oracle that did not answer ([`ClassifyError::OracleUnavailable`]), and
//! an oracle answer that is empty on an entry not marked intended-empty, or
//! non-empty on one that is ([`ClassifyError::InvalidFixture`]).
//!
//! [`Denominators`] keeps the three denominators apart: `applicable` counts
//! every classified case; `correctness` counts the cases whose data came back
//! and was compared (`ok`, `incorrect`, `partial`); `competitive` counts the
//! cases admissible in a timing table (`ok` only); `coverage_success` counts
//! the coverage numerator (`ok`). `refused`, `timeout`, `error` and
//! `unsupported_construct` are in `applicable` and outside `correctness`.
//!
//! Lane or profile inapplicability is outside the seven statuses: an entry
//! that does not apply to a lane or profile is never classified, and so never
//! enters any denominator.

use ravel_promql_difftest::comparator::{Verdict, compare};
use ravel_promql_difftest::corpus::ComparisonMode;
use ravel_promql_difftest::prometheus_client::{QueryOutcome, TransportErrorKind};
use serde_json::Value as Json;

use crate::report_schema::ResultStatus;

/// The engines MetricsBench classifies outcomes for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Engine {
    Ravel,
    Prometheus,
    VictoriaMetrics,
    Mimir,
}

/// Why an engine declined to answer (ADR-0927 decision 6). Required on every
/// `refused` outcome and absent on every other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalReason {
    /// A configured query limit fired.
    Budget,
    /// Ravel's coverage was incomplete and the request did not set
    /// `allow_partial`.
    PartialCoverage,
}

/// The candidate envelope's annotation channels, kept apart as the engines
/// keep them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Annotations {
    pub warnings: Vec<String>,
    pub infos: Vec<String>,
}

/// One engine's outcome for one case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Classified {
    pub status: ResultStatus,
    /// `Some` exactly when `status` is `refused`.
    pub refusal_reason: Option<RefusalReason>,
    /// The failure class for `error` (the engine's `errorType`, or the
    /// transport failure kind).
    pub error_class: Option<String>,
    /// The response carried the engine's partial signal. Set on `partial`,
    /// and on `incorrect` when a partial response also diverged.
    pub incomplete: bool,
    pub annotations: Annotations,
    /// The comparator's mismatch text, or the engine's error message, capped
    /// at [`MAX_MESSAGE_BYTES`].
    pub redacted_message: Option<String>,
}

/// Upper bound on [`Classified::redacted_message`], so one oversized error
/// body cannot bloat a report row.
pub const MAX_MESSAGE_BYTES: usize = 1024;

/// The oracle's answer for a case: a parsed, successful envelope.
#[derive(Clone, Debug, PartialEq)]
pub struct OracleAnswer {
    envelope: Json,
}

impl OracleAnswer {
    /// Accepts `envelope` as the oracle's answer when it is a `success`
    /// envelope carrying `data.resultType`; anything else is
    /// [`ClassifyError::OracleUnavailable`].
    pub fn from_envelope(envelope: Json) -> Result<Self, ClassifyError> {
        if envelope["status"].as_str() != Some("success") {
            return Err(ClassifyError::OracleUnavailable(format!(
                "oracle envelope status is {:?}, not \"success\"",
                envelope["status"]
            )));
        }
        if envelope["data"]["resultType"].as_str().is_none() {
            return Err(ClassifyError::OracleUnavailable(
                "oracle envelope carries no data.resultType".to_string(),
            ));
        }
        Ok(OracleAnswer { envelope })
    }

    /// The oracle's answer from its query outcome: a 2xx success envelope,
    /// or [`ClassifyError::OracleUnavailable`] for every other outcome.
    pub fn from_outcome(outcome: &QueryOutcome) -> Result<Self, ClassifyError> {
        match outcome {
            QueryOutcome::Envelope {
                http_status, body, ..
            } if is_2xx(*http_status) => Self::from_envelope(body.clone()),
            QueryOutcome::Envelope { http_status, .. } => Err(ClassifyError::OracleUnavailable(
                format!("oracle answered HTTP {http_status}"),
            )),
            QueryOutcome::Timeout { phase, .. } => Err(ClassifyError::OracleUnavailable(format!(
                "oracle timed out in the {phase:?} phase"
            ))),
            QueryOutcome::Transport { kind, .. } => Err(ClassifyError::OracleUnavailable(format!(
                "oracle transport failure: {kind:?}"
            ))),
            QueryOutcome::BodyTooLarge { limit, .. } => Err(ClassifyError::OracleUnavailable(
                format!("oracle body exceeds the {limit}-byte cap"),
            )),
        }
    }

    pub fn envelope(&self) -> &Json {
        &self.envelope
    }

    /// Whether the oracle's result is an empty vector or matrix.
    pub fn is_empty(&self) -> bool {
        result_is_empty(&self.envelope)
    }
}

/// What the corpus entry says about the case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryExpectation {
    /// The entry is marked intended-empty (amendment E): an empty oracle
    /// answer is valid for it, and only for it.
    pub intended_empty: bool,
    /// The entry's ADR-0025 ULP tolerance, passed to the comparator as is.
    pub tolerance_ulps: Option<u32>,
    /// `Unordered` or `Ordered`; the error-comparison modes have no place in
    /// a status mapping and are refused.
    pub mode: ComparisonMode,
    /// The constructs the entry exercises, named as the difftest registry
    /// names them.
    pub constructs: Vec<String>,
}

/// A case that cannot be given a status because the experiment, not the
/// engine, failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClassifyError {
    #[error("the oracle has no answer for this case: {0}")]
    OracleUnavailable(String),
    #[error("invalid fixture: {0}")]
    InvalidFixture(String),
    #[error(
        "comparison mode {0:?} is not a data comparison; a status mapping needs Unordered or Ordered"
    )]
    UnsupportedMode(ComparisonMode),
}

/// One recognised error shape. A row matches an error envelope when every
/// `Some` field equals the response's and `message_contains` is a substring
/// of its `error` string. `source` cites where the string comes from; a row
/// whose shape could not be found documented starts `unverified:`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorSignature {
    pub http_status: Option<u16>,
    pub error_type: Option<&'static str>,
    pub message_contains: &'static str,
    pub source: &'static str,
}

/// A row of an engine's budget-error table (amendment F).
pub type BudgetSignature = ErrorSignature;

impl ErrorSignature {
    pub fn is_verified(&self) -> bool {
        !self.source.starts_with("unverified:")
    }

    fn matches(&self, http_status: u16, error_type: Option<&str>, message: &str) -> bool {
        self.http_status.is_none_or(|s| s == http_status)
            && self.error_type.is_none_or(|t| error_type == Some(t))
            && message.contains(self.message_contains)
    }
}

const RAVEL_BUDGET: &[BudgetSignature] = &[
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "segments, exceeding the limit of",
        source: "crates/ravel-query/src/error.rs:121 (TooManySegments), rendered 422 execution by crates/ravel-query/src/http/error.rs:123-134,440",
    },
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "series, exceeding the limit of",
        source: "crates/ravel-query/src/error.rs:123 (TooManySeries, per-selector series ceiling)",
    },
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "samples, exceeding the limit of",
        source: "crates/ravel-query/src/error.rs:125 (TooManySamples, per-selector sample ceiling)",
    },
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "bytes, exceeding the budget of",
        source: "crates/ravel-query/src/error.rs:127 (TooManyBytesScanned)",
    },
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "response frames, exceeding the limit of",
        source: "crates/ravel-query/src/error.rs:134 (TooManySliceFrames)",
    },
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "response frame wire bytes, exceeding the per-slice limit of",
        source: "crates/ravel-query/src/error.rs:149 (TooManySliceBytes)",
    },
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "S3 requests, exceeding the budget of",
        source: "crates/ravel-query/src/error.rs:159 (RequestBudgetExceeded)",
    },
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "evaluation points exceeds the maximum of",
        source: "crates/ravel-promql/src/eval.rs:437 (TooManyPoints, the evaluation point cap), rendered 422 execution by crates/ravel-query/src/http/error.rs:377",
    },
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "query window too wide:",
        source: "crates/ravel-catalog/src/error.rs:88 (WindowTooWide), rendered 422 execution by crates/ravel-query/src/http/error.rs:142-143",
    },
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "memory exhausted: ",
        source: "crates/ravel-memory/src/lib.rs:578,583 (FetchMemoryExhausted, catalog MemoryExhausted), rendered 422 execution by crates/ravel-query/src/http/error.rs:153-155",
    },
    BudgetSignature {
        http_status: Some(504),
        error_type: Some("timeout"),
        message_contains: "query exceeded its deadline of",
        source: "crates/ravel-query/src/error.rs:165 (DeadlineExceeded), rendered 504 timeout by crates/ravel-query/src/http/error.rs:157,443",
    },
];

const PROMETHEUS_BUDGET: &[BudgetSignature] = &[
    BudgetSignature {
        http_status: Some(422),
        error_type: Some("execution"),
        message_contains: "query processing would load too many samples into memory in",
        source: "Prometheus promql ErrTooManySamples (--query.max-samples), quoted in https://github.com/prometheus/prometheus/issues/7281; 422 execution per https://prometheus.io/docs/prometheus/latest/querying/api/#format-overview",
    },
    BudgetSignature {
        http_status: Some(503),
        error_type: Some("timeout"),
        message_contains: "query timed out in",
        source: "unverified: Prometheus promql ErrQueryTimeout (--query.timeout); only the 503 for a timed-out query is documented, at https://prometheus.io/docs/prometheus/latest/querying/api/#format-overview",
    },
];

const VICTORIAMETRICS_BUDGET: &[BudgetSignature] = &[
    BudgetSignature {
        http_status: None,
        error_type: None,
        message_contains: "-search.maxSamplesPerQuery",
        source: "error body quoted in https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6941 (\"cannot select more than -search.maxSamplesPerQuery=... samples\")",
    },
    BudgetSignature {
        http_status: None,
        error_type: None,
        message_contains: "-search.maxUniqueTimeseries",
        source: "error body quoted in https://github.com/VictoriaMetrics/VictoriaMetrics/issues/7154 and pull 6893 (\"... the most likely limit is -search.maxUniqueTimeseries\")",
    },
    BudgetSignature {
        http_status: None,
        error_type: None,
        message_contains: "-search.maxPointsPerTimeseries",
        source: "unverified: flag named in ADR-0927 amendment F; the error text is assumed to name the flag as the two rows above do",
    },
    BudgetSignature {
        http_status: None,
        error_type: None,
        message_contains: "-search.maxQueryDuration",
        source: "unverified: flag named in ADR-0927 amendment F; the error text is assumed to name the flag as the two rows above do",
    },
];

const MIMIR_BUDGET: &[BudgetSignature] = &[
    BudgetSignature {
        http_status: None,
        error_type: None,
        message_contains: "err-mimir-max-chunks-per-query",
        source: "https://grafana.com/docs/mimir/latest/manage/mimir-runbooks/#err-mimir-max-chunks-per-query (max_fetched_chunks_per_query; 422 since Mimir 2.7.1)",
    },
    BudgetSignature {
        http_status: None,
        error_type: None,
        message_contains: "err-mimir-max-estimated-chunks-per-query",
        source: "https://grafana.com/docs/mimir/latest/manage/mimir-runbooks/#err-mimir-max-estimated-chunks-per-query",
    },
    BudgetSignature {
        http_status: None,
        error_type: None,
        message_contains: "err-mimir-max-series-per-query",
        source: "unverified: error id from the Mimir runbook errors catalog, message text not read",
    },
    BudgetSignature {
        http_status: None,
        error_type: None,
        message_contains: "err-mimir-max-chunks-bytes-per-query",
        source: "unverified: error id from the Mimir runbook errors catalog, message text not read",
    },
    BudgetSignature {
        http_status: None,
        error_type: None,
        message_contains: "query processing would load too many samples into memory in",
        source: "unverified: querier.max_samples is enforced by the Prometheus engine Mimir embeds, so its ErrTooManySamples text is assumed",
    },
    BudgetSignature {
        http_status: None,
        error_type: Some("timeout"),
        message_contains: "",
        source: "unverified: a querier timeout is assumed to use the Prometheus API's timeout errorType",
    },
];

/// The engine's budget-error signatures (ADR-0927 amendment F): an error
/// envelope matching one row is `refused` with reason `budget`.
pub fn budget_signatures(engine: Engine) -> &'static [BudgetSignature] {
    match engine {
        Engine::Ravel => RAVEL_BUDGET,
        Engine::Prometheus => PROMETHEUS_BUDGET,
        Engine::VictoriaMetrics => VICTORIAMETRICS_BUDGET,
        Engine::Mimir => MIMIR_BUDGET,
    }
}

const RAVEL_UNSUPPORTED: &[ErrorSignature] = &[ErrorSignature {
    http_status: Some(422),
    error_type: Some("execution"),
    message_contains: "unsupported PromQL construct: ",
    source: "crates/ravel-query/src/error.rs:102 and crates/ravel-promql/src/eval.rs:426 (Unsupported), rendered 422 execution by crates/ravel-query/src/http/error.rs:440, the same errorType as every budget class",
}];

const VICTORIAMETRICS_UNSUPPORTED: &[ErrorSignature] = &[ErrorSignature {
    http_status: None,
    error_type: None,
    message_contains: "unknown func",
    source: "unverified: MetricsQL's error for a function it does not implement, not found documented",
}];

/// The engine's not-implemented error shapes. Prometheus is the oracle and
/// implements every corpus construct; no Mimir shape was found documented.
pub fn unsupported_signatures(engine: Engine) -> &'static [ErrorSignature] {
    match engine {
        Engine::Ravel => RAVEL_UNSUPPORTED,
        Engine::VictoriaMetrics => VICTORIAMETRICS_UNSUPPORTED,
        Engine::Prometheus | Engine::Mimir => &[],
    }
}

/// The prefix of Ravel's partial-coverage refusal message
/// (crates/ravel-query/src/http/service.rs:343), rendered 503 `unavailable`
/// (crates/ravel-query/src/http/error.rs:442).
pub const RAVEL_PARTIAL_REFUSAL_PREFIX: &str = "partial results: coverage is incomplete";

/// Constructs whose MetricsQL default semantics differ from PromQL's.
const METRICSQL_DIVERGENT_CONSTRUCTS: &[&str] = &["rate", "increase"];

/// Classifies one engine's outcome for one case. See the module doc for the
/// rules.
pub fn classify(
    engine: Engine,
    outcome: &QueryOutcome,
    oracle: Option<&OracleAnswer>,
    entry: &EntryExpectation,
) -> Result<Classified, ClassifyError> {
    if !matches!(
        entry.mode,
        ComparisonMode::Unordered | ComparisonMode::Ordered
    ) {
        return Err(ClassifyError::UnsupportedMode(entry.mode));
    }
    let oracle = oracle.ok_or_else(|| {
        ClassifyError::OracleUnavailable("no oracle answer was supplied".to_string())
    })?;
    match (oracle.is_empty(), entry.intended_empty) {
        (true, false) => {
            return Err(ClassifyError::InvalidFixture(
                "the oracle answer is empty and the entry is not marked intended-empty".to_string(),
            ));
        }
        (false, true) => {
            return Err(ClassifyError::InvalidFixture(
                "the entry is marked intended-empty and the oracle answer is not empty".to_string(),
            ));
        }
        _ => {}
    }

    match outcome {
        QueryOutcome::Timeout { .. } => Ok(Classified::bare(ResultStatus::Timeout)),
        QueryOutcome::Transport {
            kind, http_status, ..
        } => {
            let kind = match kind {
                TransportErrorKind::Connect => "transport_connect",
                TransportErrorKind::Request => "transport_request",
                TransportErrorKind::Body => "transport_body",
                TransportErrorKind::Decode => "transport_decode",
            };
            let class = match http_status {
                Some(status) => format!("{kind}/http_{status}"),
                None => kind.to_string(),
            };
            Ok(Classified::error(class, None))
        }
        QueryOutcome::BodyTooLarge { .. } => {
            Ok(Classified::error("body_too_large".to_string(), None))
        }
        QueryOutcome::Envelope {
            http_status, body, ..
        } => {
            let annotations = annotations_of(body);
            let mut classified =
                if is_2xx(*http_status) && body["status"].as_str() == Some("success") {
                    classify_success(engine, body, oracle, entry)
                } else {
                    classify_error(engine, *http_status, body)
                };
            classified.annotations = annotations;
            Ok(classified)
        }
    }
}

fn classify_success(
    engine: Engine,
    body: &Json,
    oracle: &OracleAnswer,
    entry: &EntryExpectation,
) -> Classified {
    let incomplete = carries_partial_signal(engine, body);
    let verdict = if result_is_empty(body) && !oracle.is_empty() {
        Verdict::Mismatch("the result is empty and the oracle's is not".to_string())
    } else {
        compare(
            entry.mode,
            entry.tolerance_ulps,
            &data_only(oracle.envelope()),
            &data_only(body),
        )
    };
    match verdict {
        Verdict::Match => Classified {
            incomplete,
            ..Classified::bare(if incomplete {
                ResultStatus::Partial
            } else {
                ResultStatus::Ok
            })
        },
        Verdict::Mismatch(text) => {
            let text = if engine == Engine::VictoriaMetrics
                && entry
                    .constructs
                    .iter()
                    .any(|c| METRICSQL_DIVERGENT_CONSTRUCTS.contains(&c.as_str()))
            {
                format!("semantic_difference: {text}")
            } else {
                text
            };
            Classified {
                incomplete,
                redacted_message: Some(cap_message(&text)),
                ..Classified::bare(ResultStatus::Incorrect)
            }
        }
    }
}

fn classify_error(engine: Engine, http_status: u16, body: &Json) -> Classified {
    let error_type = body["errorType"].as_str();
    let message = body["error"].as_str().unwrap_or_default();
    let redacted = (!message.is_empty()).then(|| cap_message(message));

    if engine == Engine::Ravel
        && http_status == 503
        && error_type == Some("unavailable")
        && message.starts_with(RAVEL_PARTIAL_REFUSAL_PREFIX)
        && body.get("data").is_none_or(Json::is_null)
    {
        return Classified::refused(RefusalReason::PartialCoverage, redacted);
    }
    if budget_signatures(engine)
        .iter()
        .any(|s| s.matches(http_status, error_type, message))
    {
        return Classified::refused(RefusalReason::Budget, redacted);
    }
    if unsupported_signatures(engine)
        .iter()
        .any(|s| s.matches(http_status, error_type, message))
    {
        return Classified {
            redacted_message: redacted,
            ..Classified::bare(ResultStatus::UnsupportedConstruct)
        };
    }
    let class = error_type.map_or_else(|| format!("http_{http_status}"), str::to_string);
    Classified::error(class, redacted)
}

impl Classified {
    fn bare(status: ResultStatus) -> Self {
        Classified {
            status,
            refusal_reason: None,
            error_class: None,
            incomplete: false,
            annotations: Annotations::default(),
            redacted_message: None,
        }
    }

    fn error(class: String, redacted_message: Option<String>) -> Self {
        Classified {
            error_class: Some(class),
            redacted_message,
            ..Classified::bare(ResultStatus::Error)
        }
    }

    fn refused(reason: RefusalReason, redacted_message: Option<String>) -> Self {
        Classified {
            refusal_reason: Some(reason),
            redacted_message,
            ..Classified::bare(ResultStatus::Refused)
        }
    }
}

/// The three denominators of ADR-0927 decision 6 (plus the coverage
/// numerator), counted over one engine's classified cases.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Denominators {
    /// Every classified case.
    pub applicable: usize,
    /// Cases whose data came back and was compared: `ok`, `incorrect`,
    /// `partial`.
    pub correctness: usize,
    /// Cases admissible in a timing table: `ok`.
    pub competitive: usize,
    /// Cases counted as a coverage success: `ok`.
    pub coverage_success: usize,
}

impl Denominators {
    pub fn of(cases: &[Classified]) -> Self {
        let mut d = Denominators {
            applicable: cases.len(),
            ..Denominators::default()
        };
        for case in cases {
            match case.status {
                ResultStatus::Ok => {
                    d.correctness += 1;
                    d.competitive += 1;
                    d.coverage_success += 1;
                }
                ResultStatus::Incorrect | ResultStatus::Partial => d.correctness += 1,
                ResultStatus::Timeout
                | ResultStatus::Error
                | ResultStatus::UnsupportedConstruct
                | ResultStatus::Refused => {}
            }
        }
        d
    }
}

fn is_2xx(status: u16) -> bool {
    (200..300).contains(&status)
}

fn result_is_empty(envelope: &Json) -> bool {
    envelope["data"]["result"]
        .as_array()
        .is_some_and(|r| r.is_empty())
}

fn carries_partial_signal(engine: Engine, body: &Json) -> bool {
    match engine {
        Engine::Ravel => body["partial"].as_bool() == Some(true),
        Engine::VictoriaMetrics => body["isPartial"].as_bool() == Some(true),
        Engine::Prometheus | Engine::Mimir => false,
    }
}

/// The envelope reduced to `status` and `data`, so the comparator's
/// annotation-presence checks cannot turn a warning into a mismatch.
fn data_only(envelope: &Json) -> Json {
    serde_json::json!({
        "status": envelope["status"].clone(),
        "data": envelope["data"].clone(),
    })
}

fn annotations_of(body: &Json) -> Annotations {
    let channel = |field: &str| -> Vec<String> {
        body[field]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    Annotations {
        warnings: channel("warnings"),
        infos: channel("infos"),
    }
}

fn cap_message(message: &str) -> String {
    if message.len() <= MAX_MESSAGE_BYTES {
        return message.to_string();
    }
    let mut end = MAX_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_string()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use ravel_promql_difftest::prometheus_client::{QueryTiming, TimeoutPhase};
    use serde_json::json;

    use super::*;

    fn envelope(http_status: u16, body: Json) -> QueryOutcome {
        QueryOutcome::Envelope {
            http_status,
            body,
            timing: QueryTiming::default(),
        }
    }

    fn vector(value: &str) -> Json {
        json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {"__name__": "up", "job": "a"}, "value": [1700000000, value]}]
            }
        })
    }

    fn empty_vector() -> Json {
        json!({"status": "success", "data": {"resultType": "vector", "result": []}})
    }

    fn oracle() -> OracleAnswer {
        OracleAnswer::from_envelope(vector("1")).expect("oracle envelope")
    }

    fn entry() -> EntryExpectation {
        EntryExpectation {
            intended_empty: false,
            tolerance_ulps: None,
            mode: ComparisonMode::Unordered,
            constructs: vec!["vector_selector".to_string()],
        }
    }

    fn run(engine: Engine, outcome: QueryOutcome) -> Classified {
        classify(engine, &outcome, Some(&oracle()), &entry()).expect("classified")
    }

    fn error_body(error_type: &str, error: &str) -> Json {
        json!({"status": "error", "errorType": error_type, "error": error})
    }

    // Ravel: crates/ravel-query/src/error.rs:159 rendered by
    // crates/ravel-query/src/http/error.rs:134,440.
    fn ravel_request_budget() -> QueryOutcome {
        envelope(
            422,
            error_body(
                "execution",
                "query issued 4097 S3 requests, exceeding the budget of 4096",
            ),
        )
    }

    // Ravel: crates/ravel-query/src/http/service.rs:343, 503 unavailable.
    fn ravel_partial_coverage_refusal() -> QueryOutcome {
        envelope(
            503,
            error_body(
                "unavailable",
                "partial results: coverage is incomplete (cluster eu-1 unreachable); \
                 set allow_partial=true to receive partial results",
            ),
        )
    }

    // Ravel: crates/ravel-query/src/http/error.rs:41 (MSG_UNAVAILABLE), the
    // redacted storage fault.
    fn ravel_storage_unavailable() -> QueryOutcome {
        envelope(
            503,
            error_body("unavailable", "upstream storage temporarily unavailable"),
        )
    }

    // Prometheus --query.timeout: 503 per
    // https://prometheus.io/docs/prometheus/latest/querying/api/#format-overview;
    // the message text is unverified.
    fn prometheus_query_timeout() -> QueryOutcome {
        envelope(
            503,
            error_body("timeout", "query timed out in expression evaluation"),
        )
    }

    #[test]
    fn a_budget_refusal_is_not_reported_as_unsupported_construct() {
        let c = run(Engine::Ravel, ravel_request_budget());
        assert_eq!(c.status, ResultStatus::Refused);
        assert_eq!(c.refusal_reason, Some(RefusalReason::Budget));
        assert_eq!(c.error_class, None);
    }

    #[test]
    fn a_refused_case_is_outside_the_correctness_denominator() {
        let refused = run(Engine::Ravel, ravel_request_budget());
        let ok = run(Engine::Ravel, envelope(200, vector("1")));
        let d = Denominators::of(&[refused, ok]);
        assert_eq!(
            d,
            Denominators {
                applicable: 2,
                correctness: 1,
                competitive: 1,
                coverage_success: 1,
            }
        );
    }

    #[test]
    fn denominators_count_each_status_where_decision_6_puts_it() {
        let cases: Vec<Classified> = [
            ResultStatus::Ok,
            ResultStatus::Incorrect,
            ResultStatus::Partial,
            ResultStatus::Timeout,
            ResultStatus::Error,
            ResultStatus::UnsupportedConstruct,
            ResultStatus::Refused,
        ]
        .into_iter()
        .map(Classified::bare)
        .collect();
        assert_eq!(
            Denominators::of(&cases),
            Denominators {
                applicable: 7,
                correctness: 3,
                competitive: 1,
                coverage_success: 1,
            }
        );
    }

    #[test]
    fn a_503_is_classified_by_its_body_not_its_status_code() {
        // Fails against a classifier mapping every 503 to `timeout` (all
        // three) and against one mapping every 503 to `refused` (the second).
        let refusal = run(Engine::Ravel, ravel_partial_coverage_refusal());
        assert_eq!(refusal.status, ResultStatus::Refused);
        assert_eq!(refusal.refusal_reason, Some(RefusalReason::PartialCoverage));

        let outage = run(Engine::Ravel, ravel_storage_unavailable());
        assert_eq!(outage.status, ResultStatus::Error);
        assert_eq!(outage.refusal_reason, None);
        assert_eq!(outage.error_class.as_deref(), Some("unavailable"));

        let prom = run(Engine::Prometheus, prometheus_query_timeout());
        assert_eq!(prom.status, ResultStatus::Refused);
        assert_eq!(prom.refusal_reason, Some(RefusalReason::Budget));
    }

    #[test]
    fn a_partial_coverage_503_carrying_data_is_not_a_refusal() {
        let mut body = error_body(
            "unavailable",
            "partial results: coverage is incomplete (x); set allow_partial=true",
        );
        body["data"] = vector("1")["data"].clone();
        let c = run(Engine::Ravel, envelope(503, body));
        assert_eq!(c.status, ResultStatus::Error);
    }

    #[test]
    fn a_matching_partial_response_is_partial_not_ok() {
        let mut ravel = vector("1");
        ravel["partial"] = json!(true);
        let c = run(Engine::Ravel, envelope(200, ravel));
        assert_eq!(c.status, ResultStatus::Partial);
        assert!(c.incomplete);

        // VictoriaMetrics cluster: "Every partial JSON response contains
        // \"isPartial\": true", https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/
        let mut vm = vector("1");
        vm["isPartial"] = json!(true);
        let c = run(Engine::VictoriaMetrics, envelope(200, vm));
        assert_eq!(c.status, ResultStatus::Partial);
        assert!(c.incomplete);
    }

    #[test]
    fn a_divergent_partial_response_is_incorrect_and_incomplete() {
        let mut ravel = vector("2");
        ravel["partial"] = json!(true);
        let c = run(Engine::Ravel, envelope(200, ravel));
        assert_eq!(c.status, ResultStatus::Incorrect);
        assert!(c.incomplete);
        assert!(c.redacted_message.is_some());
    }

    #[test]
    fn partial_false_is_not_a_partial_signal() {
        let mut ravel = vector("1");
        ravel["partial"] = json!(false);
        let c = run(Engine::Ravel, envelope(200, ravel));
        assert_eq!(c.status, ResultStatus::Ok);
        assert!(!c.incomplete);
    }

    #[test]
    fn warnings_and_infos_are_recorded_and_never_change_the_status() {
        let mut body = vector("1");
        body["warnings"] = json!(["PromQL warning: encountered a mix of histograms and floats"]);
        body["infos"] = json!(["PromQL info: metric might not be a counter"]);
        for engine in [
            Engine::Ravel,
            Engine::Prometheus,
            Engine::VictoriaMetrics,
            Engine::Mimir,
        ] {
            let c = run(engine, envelope(200, body.clone()));
            assert_eq!(c.status, ResultStatus::Ok, "{engine:?}");
            assert!(!c.incomplete, "{engine:?}");
            assert_eq!(c.annotations.warnings.len(), 1, "{engine:?}");
            assert_eq!(c.annotations.infos.len(), 1, "{engine:?}");
        }
    }

    #[test]
    fn a_warning_on_a_divergent_result_does_not_soften_incorrect() {
        let mut body = vector("2");
        body["warnings"] = json!(["something"]);
        let c = run(Engine::Prometheus, envelope(200, body));
        assert_eq!(c.status, ResultStatus::Incorrect);
        assert!(!c.incomplete);
    }

    #[test]
    fn matching_data_is_ok() {
        let c = run(Engine::Prometheus, envelope(200, vector("1")));
        assert_eq!(c, Classified::bare(ResultStatus::Ok));
    }

    #[test]
    fn mismatching_data_is_incorrect_with_the_comparator_text() {
        let c = run(Engine::Mimir, envelope(200, vector("2")));
        assert_eq!(c.status, ResultStatus::Incorrect);
        let text = c.redacted_message.expect("mismatch text");
        assert!(!text.starts_with("semantic_difference:"), "{text}");
    }

    #[test]
    fn an_empty_result_where_the_oracle_is_not_empty_is_incorrect() {
        let c = run(Engine::Ravel, envelope(200, empty_vector()));
        assert_eq!(c.status, ResultStatus::Incorrect);
    }

    #[test]
    fn an_empty_oracle_on_an_entry_not_intended_empty_is_an_invalid_fixture() {
        let oracle = OracleAnswer::from_envelope(empty_vector()).expect("oracle");
        let err = classify(
            Engine::Ravel,
            &envelope(200, empty_vector()),
            Some(&oracle),
            &entry(),
        )
        .expect_err("invalid fixture");
        assert!(matches!(err, ClassifyError::InvalidFixture(_)), "{err:?}");
    }

    #[test]
    fn an_intended_empty_entry_with_both_answers_empty_is_ok() {
        let oracle = OracleAnswer::from_envelope(empty_vector()).expect("oracle");
        let entry = EntryExpectation {
            intended_empty: true,
            ..entry()
        };
        let c = classify(
            Engine::Ravel,
            &envelope(200, empty_vector()),
            Some(&oracle),
            &entry,
        )
        .expect("classified");
        assert_eq!(c.status, ResultStatus::Ok);
    }

    #[test]
    fn an_intended_empty_entry_with_a_non_empty_oracle_is_an_invalid_fixture() {
        let entry = EntryExpectation {
            intended_empty: true,
            ..entry()
        };
        let err = classify(
            Engine::Ravel,
            &envelope(200, vector("1")),
            Some(&oracle()),
            &entry,
        )
        .expect_err("invalid fixture");
        assert!(matches!(err, ClassifyError::InvalidFixture(_)), "{err:?}");
    }

    #[test]
    fn a_missing_or_failed_oracle_is_never_a_candidate_status() {
        let err = classify(Engine::Ravel, &envelope(200, vector("1")), None, &entry())
            .expect_err("no oracle");
        assert!(
            matches!(err, ClassifyError::OracleUnavailable(_)),
            "{err:?}"
        );

        let failed = OracleAnswer::from_outcome(&envelope(
            503,
            error_body("timeout", "query timed out in expression evaluation"),
        ))
        .expect_err("oracle failed");
        assert!(
            matches!(failed, ClassifyError::OracleUnavailable(_)),
            "{failed:?}"
        );
        let timed_out = OracleAnswer::from_outcome(&QueryOutcome::Timeout {
            phase: TimeoutPhase::Headers,
            elapsed: Duration::from_secs(30),
            timing: QueryTiming::default(),
        })
        .expect_err("oracle timed out");
        assert!(matches!(timed_out, ClassifyError::OracleUnavailable(_)));
        let error_200 = OracleAnswer::from_outcome(&envelope(200, error_body("execution", "x")))
            .expect_err("error envelope");
        assert!(matches!(error_200, ClassifyError::OracleUnavailable(_)));
        assert!(OracleAnswer::from_outcome(&envelope(200, vector("1"))).is_ok());
    }

    #[test]
    fn an_error_comparison_mode_is_refused() {
        let entry = EntryExpectation {
            mode: ComparisonMode::ExpectError,
            ..entry()
        };
        let err = classify(
            Engine::Ravel,
            &envelope(200, vector("1")),
            Some(&oracle()),
            &entry,
        )
        .expect_err("mode");
        assert_eq!(
            err,
            ClassifyError::UnsupportedMode(ComparisonMode::ExpectError)
        );
    }

    #[test]
    fn a_harness_timeout_is_timeout_in_every_phase() {
        for phase in [
            TimeoutPhase::Connect,
            TimeoutPhase::Headers,
            TimeoutPhase::Body,
            TimeoutPhase::Parse,
        ] {
            let c = run(
                Engine::VictoriaMetrics,
                QueryOutcome::Timeout {
                    phase,
                    elapsed: Duration::from_secs(30),
                    timing: QueryTiming::default(),
                },
            );
            assert_eq!(c, Classified::bare(ResultStatus::Timeout), "{phase:?}");
        }
    }

    #[test]
    fn transport_failures_are_error_naming_the_kind() {
        let cases = [
            (TransportErrorKind::Connect, None, "transport_connect"),
            (TransportErrorKind::Request, None, "transport_request"),
            (
                TransportErrorKind::Body,
                Some(200),
                "transport_body/http_200",
            ),
            (
                TransportErrorKind::Decode,
                Some(502),
                "transport_decode/http_502",
            ),
        ];
        for (kind, http_status, class) in cases {
            let c = run(
                Engine::Ravel,
                QueryOutcome::Transport {
                    kind,
                    http_status,
                    message: "x".to_string(),
                    timing: QueryTiming::default(),
                },
            );
            assert_eq!(c.status, ResultStatus::Error);
            assert_eq!(c.error_class.as_deref(), Some(class));
        }
        let c = run(
            Engine::Ravel,
            QueryOutcome::BodyTooLarge {
                limit: 1024,
                timing: QueryTiming::default(),
            },
        );
        assert_eq!(c.status, ResultStatus::Error);
        assert_eq!(c.error_class.as_deref(), Some("body_too_large"));
    }

    #[test]
    fn ravel_unsupported_construct_is_split_from_budget_by_message() {
        // Same 422 `execution` as the budget classes:
        // crates/ravel-query/src/http/error.rs:122,440.
        let c = run(
            Engine::Ravel,
            envelope(
                422,
                error_body(
                    "execution",
                    "unsupported PromQL construct: @ modifier with end()",
                ),
            ),
        );
        assert_eq!(c.status, ResultStatus::UnsupportedConstruct);
        assert_eq!(c.refusal_reason, None);
    }

    #[test]
    fn every_ravel_budget_class_is_refused_budget() {
        let messages = [
            "query matched 10 segments, exceeding the limit of 5",
            "query matched 10 series, exceeding the limit of 5",
            "query matched 10 samples, exceeding the limit of 5",
            "query scanned 10 bytes, exceeding the budget of 5",
            "slice returned 10 response frames, exceeding the limit of 5",
            "slice returned 10 response frame wire bytes, exceeding the per-slice limit of 5",
            "query issued 10 S3 requests, exceeding the budget of 5",
            "range query resolution of 20000 evaluation points exceeds the maximum of 11000; widen step_ms or narrow the range",
            "query window too wide: it would issue an estimated 900 catalog list requests, over the limit of 512; narrow the query time range and retry",
            "memory exhausted: requested 3 bytes, 98 of 100 byte limit already reserved",
        ];
        for message in messages {
            let c = run(
                Engine::Ravel,
                envelope(422, error_body("execution", message)),
            );
            assert_eq!(c.status, ResultStatus::Refused, "{message}");
            assert_eq!(c.refusal_reason, Some(RefusalReason::Budget), "{message}");
        }
        let c = run(
            Engine::Ravel,
            envelope(
                504,
                error_body("timeout", "query exceeded its deadline of 30s"),
            ),
        );
        assert_eq!(c.refusal_reason, Some(RefusalReason::Budget));
    }

    #[test]
    fn prometheus_max_samples_is_refused_budget() {
        // https://github.com/prometheus/prometheus/issues/7281
        let c = run(
            Engine::Prometheus,
            envelope(
                422,
                error_body(
                    "execution",
                    "query processing would load too many samples into memory in query execution",
                ),
            ),
        );
        assert_eq!(c.refusal_reason, Some(RefusalReason::Budget));
    }

    #[test]
    fn victoriametrics_search_limits_are_refused_budget() {
        // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6941
        let samples = run(
            Engine::VictoriaMetrics,
            envelope(
                422,
                json!({"status": "error", "errorType": "422", "error":
                    "error when executing query=\"sum(rate(x[5m]))\": cannot select more than \
                     -search.maxSamplesPerQuery=1000 samples; possible solutions: increase \
                     -search.maxSamplesPerQuery"}),
            ),
        );
        assert_eq!(samples.refusal_reason, Some(RefusalReason::Budget));
        // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/7154
        let series = run(
            Engine::VictoriaMetrics,
            envelope(
                422,
                json!({"status": "error", "errorType": "422", "error":
                    "the number of matching timeseries exceeds 300000; either narrow down the \
                     search or increase -search.max* command-line flag values (the most likely \
                     limit is -search.maxUniqueTimeseries)"}),
            ),
        );
        assert_eq!(series.refusal_reason, Some(RefusalReason::Budget));
    }

    #[test]
    fn mimir_chunk_limit_is_refused_budget() {
        // https://grafana.com/docs/mimir/latest/manage/mimir-runbooks/#err-mimir-max-chunks-per-query
        let c = run(
            Engine::Mimir,
            envelope(
                422,
                error_body(
                    "execution",
                    "the query exceeded the maximum number of chunks (limit: 2000000 chunks) \
                     (err-mimir-max-chunks-per-query)",
                ),
            ),
        );
        assert_eq!(c.refusal_reason, Some(RefusalReason::Budget));
    }

    #[test]
    fn victoriametrics_unknown_function_is_unsupported_construct() {
        // unverified: shape not found documented.
        let c = run(
            Engine::VictoriaMetrics,
            envelope(
                422,
                json!({"status": "error", "errorType": "422", "error":
                    "cannot parse query: unknown func \"info\""}),
            ),
        );
        assert_eq!(c.status, ResultStatus::UnsupportedConstruct);
    }

    #[test]
    fn any_other_error_is_error_with_the_engine_error_type() {
        let c = run(
            Engine::Prometheus,
            envelope(400, error_body("bad_data", "invalid parameter \"query\"")),
        );
        assert_eq!(c.status, ResultStatus::Error);
        assert_eq!(c.error_class.as_deref(), Some("bad_data"));

        // A 200 carrying an error envelope is not trusted as success.
        let c = run(Engine::Ravel, envelope(200, error_body("internal", "x")));
        assert_eq!(c.error_class.as_deref(), Some("internal"));

        // No errorType: the class falls back to the status code.
        let c = run(Engine::Mimir, envelope(500, json!({"status": "error"})));
        assert_eq!(c.error_class.as_deref(), Some("http_500"));
    }

    #[test]
    fn a_victoriametrics_rate_divergence_is_a_semantic_difference_not_a_budget() {
        let entry = EntryExpectation {
            constructs: vec!["rate".to_string()],
            ..entry()
        };
        let c = classify(
            Engine::VictoriaMetrics,
            &envelope(200, vector("1.0000001")),
            Some(&oracle()),
            &entry,
        )
        .expect("classified");
        assert_eq!(c.status, ResultStatus::Incorrect);
        assert_eq!(c.refusal_reason, None);
        let text = c.redacted_message.expect("message");
        assert!(text.starts_with("semantic_difference: "), "{text}");

        // The prefix is VictoriaMetrics-only.
        let c = classify(
            Engine::Mimir,
            &envelope(200, vector("1.0000001")),
            Some(&oracle()),
            &entry,
        )
        .expect("classified");
        let text = c.redacted_message.expect("message");
        assert!(!text.starts_with("semantic_difference:"), "{text}");
    }

    #[test]
    fn every_status_is_reachable() {
        let mut partial = vector("1");
        partial["partial"] = json!(true);
        let outcomes = [
            run(Engine::Ravel, envelope(200, vector("1"))),
            run(Engine::Ravel, envelope(200, vector("2"))),
            run(Engine::Ravel, envelope(200, partial)),
            run(
                Engine::Ravel,
                QueryOutcome::Timeout {
                    phase: TimeoutPhase::Body,
                    elapsed: Duration::from_secs(1),
                    timing: QueryTiming::default(),
                },
            ),
            run(Engine::Ravel, ravel_storage_unavailable()),
            run(
                Engine::Ravel,
                envelope(
                    422,
                    error_body("execution", "unsupported PromQL construct: x"),
                ),
            ),
            run(Engine::Ravel, ravel_request_budget()),
        ];
        let statuses: Vec<ResultStatus> = outcomes.iter().map(|c| c.status).collect();
        assert_eq!(
            statuses,
            [
                ResultStatus::Ok,
                ResultStatus::Incorrect,
                ResultStatus::Partial,
                ResultStatus::Timeout,
                ResultStatus::Error,
                ResultStatus::UnsupportedConstruct,
                ResultStatus::Refused,
            ]
        );
    }

    #[test]
    fn refusal_reason_is_set_exactly_on_refused() {
        for c in [
            run(Engine::Ravel, ravel_request_budget()),
            run(Engine::Ravel, ravel_partial_coverage_refusal()),
            run(Engine::Ravel, ravel_storage_unavailable()),
            run(Engine::Ravel, envelope(200, vector("1"))),
        ] {
            assert_eq!(
                c.refusal_reason.is_some(),
                c.status == ResultStatus::Refused,
                "{c:?}"
            );
        }
    }

    #[test]
    fn every_budget_row_cites_a_source() {
        for engine in [
            Engine::Ravel,
            Engine::Prometheus,
            Engine::VictoriaMetrics,
            Engine::Mimir,
        ] {
            let rows = budget_signatures(engine);
            assert!(!rows.is_empty(), "{engine:?}");
            assert!(rows.iter().any(ErrorSignature::is_verified), "{engine:?}");
            for row in rows {
                assert!(!row.source.is_empty(), "{engine:?} {row:?}");
            }
        }
    }

    #[test]
    fn long_messages_are_capped_on_a_char_boundary() {
        let long = "é".repeat(MAX_MESSAGE_BYTES);
        let capped = cap_message(&long);
        assert!(capped.len() <= MAX_MESSAGE_BYTES);
        assert_eq!(capped.len(), MAX_MESSAGE_BYTES);
    }
}
