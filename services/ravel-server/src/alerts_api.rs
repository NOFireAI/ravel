//! `GET /api/v1/rules`: the alert rules this process loaded, for the caller's
//! tenant, in the Prometheus rules-API response shape (issue #1711).
//!
//! The route is a read of configuration, not of data. It serves the very
//! `HashMap<TenantHash, Vec<Rule>>` [`crate::alerting::spawn`] built its
//! evaluator tasks from ([`crate::alerting::AlertEvalConfig::rules`]), so an
//! operator can confirm that the rules file they deployed is the rules file
//! this process is evaluating. It reads no object storage and issues no query.
//!
//! # Tenant scoping
//!
//! The same scoping every other `/api/v1` route on this listener applies: the
//! listener's configured [`TenantResolver`] chain resolves the request's
//! credential to a [`ravel_types::TenantId`], and the response carries that
//! tenant's rules and no other tenant's. The tenant is never taken from a
//! request field: there is no request field. A credential that does not
//! resolve is 401,
//! before anything is rendered; a credential that resolves to a tenant with no
//! rules gets an empty `groups` array, which is the same answer a tenant that
//! exists but was left out of the rules file gets.
//!
//! # Groups
//!
//! Prometheus organizes rules into named groups, one per group block in a
//! rules file, and each group names the file it was read from. Ravel's rules
//! document is one flat `rules` array keyed per rule by tenant
//! ([`crate::alerting::AlertRulesFile`]), with no group blocks, so a tenant's
//! rules render as a single group named [`RULE_GROUP_NAME`]. Its `file` is the
//! empty string: the loaded rule set does not retain the path it came from.
//! A tenant with no rules renders zero groups rather than one empty group.
//!
//! # Health and state
//!
//! Prometheus carries a per-rule `health` (`ok`/`err`/`unknown`) and `state`
//! (`inactive`/`pending`/`firing`) derived from the last evaluation. This
//! endpoint serves the loaded rule set, and an evaluator's per-rule outcome is
//! not exposed to it, so every rule renders [`HEALTH_UNKNOWN`] and
//! [`STATE_UNKNOWN`]. `unknown` is Prometheus's own value for an unevaluated
//! rule's health. For `state` it is a Ravel value outside Prometheus's three,
//! chosen over reporting `inactive`, which would assert that a rule is not
//! firing when this endpoint has not looked.
//!
//! Each rule also omits Prometheus's `alerts` array of currently-active alerts
//! for the same reason: an empty array would assert a negative nothing here
//! checked. `GET /api/v1/alerts`, which is where that data belongs, is not
//! served by this router and is not served anywhere else in this crate; a
//! request for it gets axum's 404.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ravel_alerting::{Rule, RuleCondition, RuleQuery, ThresholdOp};
use ravel_query::http::TenantResolver;
use ravel_types::TenantHash;
use serde_json::{Value, json};

/// The route this module serves. Named here so the wiring, the tests, and the
/// reference doc all refer to one string.
pub const RULES_ROUTE: &str = "/api/v1/rules";

/// The single group a tenant's rules render into. Ravel's rules document has
/// no group blocks (see the module docs), so the name is a constant rather
/// than something read from the file.
pub const RULE_GROUP_NAME: &str = "ravel-alert-rules";

/// The `health` this endpoint reports for every rule, Prometheus's own value
/// for a rule whose last evaluation is not known here.
pub const HEALTH_UNKNOWN: &str = "unknown";

/// The `state` this endpoint reports for every rule. See the module docs: a
/// Ravel value, deliberately not Prometheus's `inactive`.
pub const STATE_UNKNOWN: &str = "unknown";

/// Shared state for the route: the loaded rule set and the listener's tenant
/// resolver.
#[derive(Clone)]
pub struct AlertRulesState {
    /// The same `Arc` [`crate::alerting::spawn`] reads to build its evaluator
    /// tasks, so the rendered rules are the evaluated rules and not a second
    /// copy that could drift.
    pub rules: Arc<HashMap<TenantHash, Vec<Rule>>>,
    /// The listener's resolver chain. The mTLS listener passes its own, so a
    /// certificate-identified caller is scoped by the mTLS identity rather
    /// than by the primary listener's bearer-token map.
    pub tenant_resolver: Arc<dyn TenantResolver>,
    /// The evaluator's tick interval, rendered as the group's `interval`. This
    /// is `AlertEvalConfig::interval`, the cadence every rule in the group is
    /// evaluated on.
    pub eval_interval: Duration,
}

/// The `/api/v1/rules` router.
pub fn router(state: AlertRulesState) -> Router {
    Router::new()
        .route(RULES_ROUTE, get(handle))
        .with_state(state)
}

async fn handle(State(state): State<AlertRulesState>, headers: HeaderMap) -> Response {
    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(err) => return unauthorized(&err),
    };
    let rules = state.rules.get(&tenant.hash());
    axum::Json(json!({
        "status": "success",
        "data": {"groups": render_groups(rules, state.eval_interval)},
    }))
    .into_response()
}

/// The 401 a request whose credential does not resolve gets.
fn unauthorized(err: &dyn std::fmt::Display) -> Response {
    // The same discipline the sibling routes apply: the caller gets a
    // class-level answer and the server keeps the error. A resolver can
    // fail for reasons that are not a bad credential (an unreachable
    // durable auth map), and a bare 401 records none of them.
    tracing::warn!(
        error = %err,
        route = RULES_ROUTE,
        "alert rules API: tenant resolution failed"
    );
    (
        StatusCode::UNAUTHORIZED,
        axum::Json(json!({
            "status": "error",
            "errorType": "unauthorized",
            "error": "authentication required",
        })),
    )
        .into_response()
}

/// Renders one tenant's rules as the `groups` array. `None` (and an empty
/// slice) render as zero groups.
fn render_groups(rules: Option<&Vec<Rule>>, eval_interval: Duration) -> Vec<Value> {
    let rules = match rules {
        Some(rules) if !rules.is_empty() => rules,
        _ => return Vec::new(),
    };
    vec![json!({
        "name": RULE_GROUP_NAME,
        "file": "",
        "interval": eval_interval.as_secs_f64(),
        "rules": rules.iter().map(render_rule).collect::<Vec<_>>(),
    })]
}

fn render_rule(rule: &Rule) -> Value {
    json!({
        "type": "alerting",
        "name": rule.rule_id,
        "query": render_query(rule),
        "duration": rule.for_duration.unwrap_or_default().as_secs_f64(),
        "labels": pairs_to_object(&rule.labels),
        "annotations": pairs_to_object(&rule.annotations),
        "health": HEALTH_UNKNOWN,
        "state": STATE_UNKNOWN,
    })
}

/// The rule's firing expression.
///
/// Prometheus's `query` for an alerting rule is the whole expression that
/// decides firing, comparison included. Ravel splits that across a rule's
/// query text and its condition, so a [`RuleCondition::Threshold`] renders as
/// the query text followed by the comparison it applies, and a
/// [`RuleCondition::NonEmptyResult`] (whose firing test is "the statement
/// returned a row", with nothing to append) renders as the statement text
/// alone.
fn render_query(rule: &Rule) -> String {
    let text = match &rule.query {
        RuleQuery::Promql(text) | RuleQuery::Sql(text) => text,
    };
    match &rule.condition {
        RuleCondition::Threshold { op, threshold } => {
            format!("{text} {} {threshold}", threshold_op_str(*op))
        }
        RuleCondition::NonEmptyResult => text.clone(),
    }
}

fn threshold_op_str(op: ThresholdOp) -> &'static str {
    match op {
        ThresholdOp::Gt => ">",
        ThresholdOp::Ge => ">=",
        ThresholdOp::Lt => "<",
        ThresholdOp::Le => "<=",
        ThresholdOp::Eq => "==",
        ThresholdOp::Ne => "!=",
    }
}

/// A `Rule`'s sorted `(name, value)` pairs as the JSON object Prometheus uses.
/// `parse_rules` sorts them and rejects a rules file that repeats a key inside
/// one map before it reaches here.
fn pairs_to_object(pairs: &[(String, String)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(name, value)| (name.clone(), Value::String(value.clone())))
            .collect(),
    )
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use ravel_query::http::StaticBearerTokenResolver;
    use ravel_types::TenantId;
    use tower::ServiceExt;

    const ACME_TOKEN: &str = "acme-token";
    const OTHER_TOKEN: &str = "other-token";

    /// Two tenants, both authenticating, only one of which has rules. The
    /// second tenant is what makes a handler that ignores the resolved tenant
    /// (serving every tenant's rules, or the first tenant's) fail rather than
    /// pass.
    const RULES: &str = r#"{
      "rules": [
        {
          "tenant": "acme",
          "rule_id": "cpu-hot",
          "promql": "max by (instance) (cpu_usage)",
          "condition": {"type": "threshold", "op": "gt", "value": 0.9},
          "for": "5m",
          "labels": {"severity": "page", "team": "sre"},
          "annotations": {"summary": "CPU over 90% for five minutes"}
        },
        {
          "tenant": "acme",
          "rule_id": "access-denied-burst",
          "sql": "select 1 from logs where has_word(body, 'denied') limit 1",
          "condition": {"type": "non_empty_result"},
          "annotations": {"runbook": "https://runbooks.example.com/denied"}
        },
        {
          "tenant": "third-party",
          "rule_id": "not-yours",
          "promql": "disk_free",
          "condition": {"type": "threshold", "op": "lt", "value": 0.1}
        }
      ]
    }"#;

    fn state() -> AlertRulesState {
        let tokens = HashMap::from([
            (ACME_TOKEN.to_string(), TenantId::new("acme")),
            (OTHER_TOKEN.to_string(), TenantId::new("other")),
        ]);
        AlertRulesState {
            rules: Arc::new(crate::alerting::parse_rules(RULES).expect("valid rules")),
            tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
            eval_interval: Duration::from_secs(60),
        }
    }

    async fn get_rules(state: AlertRulesState, token: Option<&str>) -> (StatusCode, Value) {
        let mut builder = HttpRequest::builder().method("GET").uri(RULES_ROUTE);
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let request = builder.body(Body::empty()).expect("build request");
        let response = router(state)
            .oneshot(request)
            .await
            .expect("route the request");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .expect("read body");
        let body: Value = serde_json::from_slice(&bytes).expect("JSON body");
        (status, body)
    }

    /// The acceptance test: the route renders the rules the evaluator loaded,
    /// for the caller's tenant only, in the Prometheus shape.
    ///
    /// The body is asserted whole rather than field by field, so a dropped or
    /// renamed field fails here. Two wrong implementations this rules out:
    ///
    /// - one that serves any tenant's rules rather than the caller's: the
    ///   `other` tenant's response below would carry acme's two rules (or the
    ///   third tenant's `not-yours`) instead of an empty `groups`;
    /// - one that renders a static shape rather than the loaded rule set: it
    ///   would have to return the same body for both credentials, and the two
    ///   assertions here disagree about what that body is.
    #[tokio::test]
    async fn rules_route_returns_loaded_rules_in_prometheus_shape() {
        let (status, body) = get_rules(state(), Some(ACME_TOKEN)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({
                "status": "success",
                "data": {
                    "groups": [{
                        "name": "ravel-alert-rules",
                        "file": "",
                        "interval": 60.0,
                        "rules": [
                            {
                                "type": "alerting",
                                "name": "cpu-hot",
                                "query": "max by (instance) (cpu_usage) > 0.9",
                                "duration": 300.0,
                                "labels": {"severity": "page", "team": "sre"},
                                "annotations": {"summary": "CPU over 90% for five minutes"},
                                "health": "unknown",
                                "state": "unknown"
                            },
                            {
                                "type": "alerting",
                                "name": "access-denied-burst",
                                "query": "select 1 from logs where has_word(body, 'denied') limit 1",
                                "duration": 0.0,
                                "labels": {},
                                "annotations": {
                                    "runbook": "https://runbooks.example.com/denied"
                                },
                                "health": "unknown",
                                "state": "unknown"
                            }
                        ]
                    }]
                }
            }),
            "the route renders exactly the rules parsed for the caller's tenant"
        );

        // A second tenant that authenticates and has no rules sees an empty
        // group list, never acme's or the third tenant's.
        let (status, body) = get_rules(state(), Some(OTHER_TOKEN)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({"status": "success", "data": {"groups": []}}),
            "a tenant with no loaded rules sees no groups, not another tenant's"
        );
    }

    /// No credential, no rules: the same 401 every tenant-scoped route on this
    /// listener returns, with the typed error body the query surfaces use.
    #[tokio::test]
    async fn a_request_with_no_tenant_is_refused() {
        let (status, body) = get_rules(state(), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            body,
            json!({
                "status": "error",
                "errorType": "unauthorized",
                "error": "authentication required",
            })
        );

        // A credential that does not resolve is the same refusal, so a caller
        // cannot read another tenant's rules by guessing a token.
        let (status, _) = get_rules(state(), Some("not-a-configured-token")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    /// `/api/v1/alerts` is a separate endpoint that this router deliberately
    /// does not serve (module docs). The absence is the framework's 404, not a
    /// stub handler that would report an empty alert list.
    #[tokio::test]
    async fn the_alerts_route_is_not_served_by_this_router() {
        let request = HttpRequest::builder()
            .method("GET")
            .uri("/api/v1/alerts")
            .header("authorization", format!("Bearer {ACME_TOKEN}"))
            .body(Body::empty())
            .expect("build request");
        let response = router(state())
            .oneshot(request)
            .await
            .expect("route the request");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Every threshold comparator renders as the PromQL operator it stands
    /// for, so the `query` field is the rule's whole firing expression.
    #[test]
    fn every_threshold_comparator_renders_its_operator() {
        for (op, expected) in [
            (ThresholdOp::Gt, "cpu > 1"),
            (ThresholdOp::Ge, "cpu >= 1"),
            (ThresholdOp::Lt, "cpu < 1"),
            (ThresholdOp::Le, "cpu <= 1"),
            (ThresholdOp::Eq, "cpu == 1"),
            (ThresholdOp::Ne, "cpu != 1"),
        ] {
            let rule = Rule {
                rule_id: "r".into(),
                query: RuleQuery::Promql("cpu".into()),
                condition: RuleCondition::Threshold { op, threshold: 1.0 },
                labels: Vec::new(),
                annotations: Vec::new(),
                for_duration: None,
                max_alert_generation: None,
                repeat_interval: None,
            };
            assert_eq!(render_query(&rule), expected);
        }
    }
}
