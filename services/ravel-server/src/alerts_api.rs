//! `GET /api/v1/rules` and `GET /api/v1/alerts`: the alert rules this process
//! loaded and the alert state its evaluators last published, for the caller's
//! tenant, in the Prometheus rules-API and alerts-API response shapes (issues
//! #1711 and #2665).
//!
//! Both routes are reads of process memory. The rule set is the very
//! `HashMap<TenantHash, Vec<Rule>>` [`crate::alerting::spawn`] built its
//! evaluator tasks from ([`crate::alerting::AlertEvalConfig::rules`]). Health,
//! state and active alerts come from the [`PublishedAlerts`] handle those
//! tasks write a [`TenantAlertSnapshot`] into at the end of each tick. A
//! request reads no object storage and issues no query: it clones one
//! tenant's snapshot pointer and renders it.
//!
//! # Tenant scoping
//!
//! The same scoping every other `/api/v1` route on this listener applies: the
//! listener's configured [`TenantResolver`] chain resolves the request's
//! credential to a [`ravel_types::TenantId`], and the response carries that
//! tenant's rules and alerts and no other tenant's. The tenant is never taken
//! from a request field: there is no request field. A credential that does not
//! resolve is 401 on both routes, before anything is rendered.
//!
//! # Current state, from this process only
//!
//! What both routes serve is the state the tenant's evaluator in *this*
//! process published after its last tick. There is no cross-process read: a
//! replica serves its own evaluator's snapshot, and a restarted process serves
//! nothing until its first tick. Neither route is history; past transitions
//! are in the `alerts` SQL table (ADR-1101).
//!
//! When no evaluator runs in this process (alerting disabled, or no tenant
//! has rules), `/api/v1/alerts` answers 503 with `errorType` `unavailable`
//! rather than an empty list, which would assert that nothing is firing. A
//! tenant with rules gets the same 503 before its evaluator has finished a
//! tick ([`NOT_YET_EVALUATED`]), and whenever its last tick could not read
//! alert history ([`HISTORY_UNREADABLE`]): an earlier tick's list is not
//! served in place of the one that tick could not read. A tenant with no
//! rules gets an empty list, but only from a process where some tenant has
//! rules and an evaluator runs; otherwise it gets the first 503.
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
//! A rule's `health` is `ok` when the last tick ran its query and wrote its
//! transitions, and `err`, with `lastError`, when its query, condition or
//! write failed or the tick could not read history or take the tenant's
//! lease. `lastEvaluation` is that tick's clock reading, present whenever
//! `health` is `ok` or `err`. A replica whose peer held the lease on the last
//! tick did not evaluate, and reports [`HEALTH_UNKNOWN`] with no
//! `lastEvaluation`.
//!
//! A rule's `state` is `firing` when any of its alerts is firing, else
//! `pending` when any is pending, else `inactive`, read from the tenant's
//! folded alert history as of the last tick; its `alerts` array lists them.
//! When the last tick could not read that history, `state` is
//! [`STATE_UNKNOWN`] and `alerts` is omitted, while `health` is `err` as
//! above. Before the tenant's evaluator in this process has finished a tick,
//! or when no evaluator runs here, `health` and `state` are both
//! [`HEALTH_UNKNOWN`] and [`STATE_UNKNOWN`], with no `lastEvaluation` and no
//! `alerts`. `unknown` is Prometheus's own value for an unevaluated rule's
//! health. For `state` it is a Ravel value outside Prometheus's three, chosen
//! over reporting `inactive`, which would assert that a rule is not firing
//! when nothing has looked.
//!
//! # Alerts
//!
//! An alert object carries the record's labels with `alertname` set the way
//! the Alertmanager sink sets it, the rule's annotations, `state` (`pending`
//! or `firing`), `activeAt` and `value`. Suppressed and resolved alerts are
//! not listed. `activeAt` is the time of the transition into the alert's
//! current state, so a firing alert that was pending first reports when it
//! fired, not when it went pending, where Prometheus keeps the time it went
//! pending. `value` is the sample the alert's series matched with on the last
//! tick, or the row count for a SQL rule. It is present only when this
//! process held the tenant's lease on that tick and evaluated the alert's
//! rule without error; a replica that did not evaluate the rule omits it
//! rather than serving a value from an earlier tick.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ravel_alerting::{AlertState, Rule, RuleCondition, RuleQuery, ThresholdOp};
use ravel_query::http::TenantResolver;
use ravel_types::TenantHash;
use serde_json::{Map, Value, json};

use crate::alerting::{
    PublishedAlert, PublishedAlerts, PublishedRead, RuleHealth, TenantAlertSnapshot,
};

/// The rules route. Named here so the wiring, the tests, and the reference
/// doc all refer to one string.
pub const RULES_ROUTE: &str = "/api/v1/rules";

/// The alerts route.
pub const ALERTS_ROUTE: &str = "/api/v1/alerts";

/// The single group a tenant's rules render into. Ravel's rules document has
/// no group blocks (see the module docs), so the name is a constant rather
/// than something read from the file.
pub const RULE_GROUP_NAME: &str = "ravel-alert-rules";

/// The `health` of a rule this process has not evaluated: Prometheus's own
/// value for a rule whose last evaluation is not known here.
pub const HEALTH_UNKNOWN: &str = "unknown";

/// The `state` of a rule whose alerts this process has not read. See the
/// module docs: a Ravel value, deliberately not Prometheus's `inactive`.
pub const STATE_UNKNOWN: &str = "unknown";

/// The `error` of the 503 `/api/v1/alerts` answers when no evaluator runs in
/// this process.
pub const NOT_EVALUATING: &str = "alert evaluation is not running in this process";

/// The `error` of the 503 `/api/v1/alerts` answers before the tenant's
/// evaluator in this process has finished a tick.
pub const NOT_YET_EVALUATED: &str =
    "no alert evaluation tick in this process has read this tenant's alert history yet";

/// The `error` of the 503 `/api/v1/alerts` answers when the tenant's last
/// tick in this process could not read its alert history, until a tick reads
/// it again.
pub const HISTORY_UNREADABLE: &str =
    "the last alert evaluation tick in this process could not read this tenant's alert history";

/// Shared state for both routes: the loaded rule set, the published alert
/// state, and the listener's tenant resolver.
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
    /// The handle this process's evaluators publish into.
    pub published: PublishedAlerts,
}

/// The `/api/v1/rules` and `/api/v1/alerts` router.
pub fn router(state: AlertRulesState) -> Router {
    Router::new()
        .route(RULES_ROUTE, get(handle_rules))
        .route(ALERTS_ROUTE, get(handle_alerts))
        .with_state(state)
}

async fn handle_rules(State(state): State<AlertRulesState>, headers: HeaderMap) -> Response {
    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant.hash(),
        Err(err) => return unauthorized(&err, RULES_ROUTE),
    };
    let snapshot = match state.published.read(&tenant) {
        PublishedRead::Published(snapshot) => Some(snapshot),
        PublishedRead::NotEvaluating | PublishedRead::NoRules | PublishedRead::NotYetEvaluated => {
            None
        }
    };
    let rules = state.rules.get(&tenant);
    axum::Json(json!({
        "status": "success",
        "data": {"groups": render_groups(rules, snapshot.as_deref(), state.eval_interval)},
    }))
    .into_response()
}

async fn handle_alerts(State(state): State<AlertRulesState>, headers: HeaderMap) -> Response {
    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant.hash(),
        Err(err) => return unauthorized(&err, ALERTS_ROUTE),
    };
    let alerts = match state.published.read(&tenant) {
        PublishedRead::NotEvaluating => return unavailable(NOT_EVALUATING),
        PublishedRead::NotYetEvaluated => return unavailable(NOT_YET_EVALUATED),
        PublishedRead::NoRules => Vec::new(),
        PublishedRead::Published(snapshot) => match &snapshot.alerts {
            None => return unavailable(HISTORY_UNREADABLE),
            Some(alerts) => {
                let rules = state.rules.get(&tenant).map_or(&[][..], Vec::as_slice);
                alerts
                    .iter()
                    .map(|alert| render_alert(alert, rule_labels(rules, &alert.rule_id)))
                    .collect()
            }
        },
    };
    axum::Json(json!({
        "status": "success",
        "data": {"alerts": alerts},
    }))
    .into_response()
}

/// The 401 a request whose credential does not resolve gets.
fn unauthorized(err: &dyn std::fmt::Display, route: &'static str) -> Response {
    // The same discipline the sibling routes apply: the caller gets a
    // class-level answer and the server keeps the error. A resolver can
    // fail for reasons that are not a bad credential (an unreachable
    // durable auth map), and a bare 401 records none of them.
    tracing::warn!(
        error = %err,
        route,
        "alerts API: tenant resolution failed"
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

/// The 503 `/api/v1/alerts` answers when it has no published state to serve.
fn unavailable(error: &'static str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(json!({
            "status": "error",
            "errorType": "unavailable",
            "error": error,
        })),
    )
        .into_response()
}

/// Renders one tenant's rules as the `groups` array. `None` (and an empty
/// slice) render as zero groups.
fn render_groups(
    rules: Option<&Vec<Rule>>,
    snapshot: Option<&TenantAlertSnapshot>,
    eval_interval: Duration,
) -> Vec<Value> {
    let rules = match rules {
        Some(rules) if !rules.is_empty() => rules,
        _ => return Vec::new(),
    };
    vec![json!({
        "name": RULE_GROUP_NAME,
        "file": "",
        "interval": eval_interval.as_secs_f64(),
        "rules": rules.iter().map(|rule| render_rule(rule, snapshot)).collect::<Vec<_>>(),
    })]
}

fn render_rule(rule: &Rule, snapshot: Option<&TenantAlertSnapshot>) -> Value {
    let mut out = Map::new();
    out.insert("type".into(), json!("alerting"));
    out.insert("name".into(), json!(rule.rule_id));
    out.insert("query".into(), json!(render_query(rule)));
    out.insert(
        "duration".into(),
        json!(rule.for_duration.unwrap_or_default().as_secs_f64()),
    );
    out.insert("labels".into(), pairs_to_object(&rule.labels));
    out.insert("annotations".into(), pairs_to_object(&rule.annotations));

    let health = snapshot.and_then(|s| s.rule_health.get(&rule.rule_id));
    match health {
        Some(RuleHealth::Ok) => {
            out.insert("health".into(), json!("ok"));
        }
        Some(RuleHealth::Err(err)) => {
            out.insert("health".into(), json!("err"));
            out.insert("lastError".into(), json!(err));
        }
        Some(RuleHealth::Unknown) | None => {
            out.insert("health".into(), json!(HEALTH_UNKNOWN));
        }
    }
    if let (Some(snapshot), Some(RuleHealth::Ok | RuleHealth::Err(_))) = (snapshot, health) {
        out.insert(
            "lastEvaluation".into(),
            json!(rfc3339(snapshot.evaluated_at_ns)),
        );
    }

    match snapshot.and_then(|s| s.alerts.as_ref()) {
        Some(alerts) => {
            let mine: Vec<&PublishedAlert> = alerts
                .iter()
                .filter(|alert| alert.rule_id == rule.rule_id)
                .collect();
            out.insert("state".into(), json!(rule_state(&mine)));
            out.insert(
                "alerts".into(),
                Value::Array(
                    mine.iter()
                        .map(|alert| render_alert(alert, &rule.labels))
                        .collect(),
                ),
            );
        }
        None => {
            out.insert("state".into(), json!(STATE_UNKNOWN));
        }
    }
    Value::Object(out)
}

/// A rule's Prometheus state from its active alerts.
fn rule_state(alerts: &[&PublishedAlert]) -> &'static str {
    if alerts.iter().any(|a| a.state == AlertState::Firing) {
        "firing"
    } else if alerts.iter().any(|a| a.state == AlertState::Pending) {
        "pending"
    } else {
        "inactive"
    }
}

/// The labels of the configured rule `rule_id`, empty when it is not
/// configured.
fn rule_labels<'a>(rules: &'a [Rule], rule_id: &str) -> &'a [(String, String)] {
    rules
        .iter()
        .find(|rule| rule.rule_id == rule_id)
        .map_or(&[][..], |rule| rule.labels.as_slice())
}

/// One alert in the Prometheus alerts-API shape.
///
/// `alertname` follows [`crate::alert_sink::alertmanager_payload`]: the rule's
/// own `alertname` label when it has one, else the rule id, with a series
/// label of that name moved to the first free `exported_alertname`,
/// `exported_exported_alertname`, ... name.
fn render_alert(alert: &PublishedAlert, rule_labels: &[(String, String)]) -> Value {
    let rule_alertname = rule_labels
        .iter()
        .find(|(name, _)| name == "alertname")
        .map(|(_, value)| value.clone());
    let mut labels: Map<String, Value> = alert
        .labels
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(value.clone())))
        .collect();
    if rule_alertname.is_none()
        && let Some(series_alertname) = labels.remove("alertname")
    {
        let mut exported = "exported_alertname".to_string();
        while labels.contains_key(&exported) {
            exported.insert_str(0, "exported_");
        }
        labels.insert(exported, series_alertname);
    }
    labels.insert(
        "alertname".to_string(),
        Value::String(rule_alertname.unwrap_or_else(|| alert.rule_id.clone())),
    );

    let mut out = Map::new();
    out.insert("labels".into(), Value::Object(labels));
    out.insert("annotations".into(), pairs_to_object(&alert.annotations));
    out.insert("state".into(), json!(alert.state.as_str()));
    out.insert("activeAt".into(), json!(rfc3339(alert.active_at_ns)));
    if let Some(value) = alert.value {
        out.insert("value".into(), json!(format_value(value)));
    }
    Value::Object(out)
}

/// A sample value as Prometheus' HTTP API renders it: a string, with `NaN`,
/// `+Inf` and `-Inf` for the non-finite values.
fn format_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "+Inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        format!("{value}")
    }
}

/// RFC3339 with nanosecond precision. A pre-epoch reading is clamped to the
/// epoch: `humantime`'s formatter panics on a pre-epoch `SystemTime`.
fn rfc3339(ts_ns: i64) -> String {
    let ns = u64::try_from(ts_ns).unwrap_or(0);
    let time = UNIX_EPOCH
        .checked_add(Duration::from_nanos(ns))
        .unwrap_or(UNIX_EPOCH);
    humantime::format_rfc3339_nanos(time).to_string()
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

/// A sorted `(name, value)` pair list as the JSON object Prometheus uses.
/// `parse_rules` sorts a rule's pairs and rejects a rules file that repeats a
/// key inside one map before it reaches here; an alert record's pairs are the
/// same pairs or the merged label set, which has one value per name.
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

    use crate::alerting::AlertEvalConfig;

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
            published: PublishedAlerts::none(),
        }
    }

    /// [`state`] in a process whose evaluators run, none of which has ticked.
    fn evaluating_state() -> AlertRulesState {
        let mut state = state();
        state.published = PublishedAlerts::for_config(&AlertEvalConfig {
            enabled: true,
            rules: state.rules.clone(),
            ..AlertEvalConfig::default()
        });
        state
    }

    async fn get(state: AlertRulesState, route: &str, token: Option<&str>) -> (StatusCode, Value) {
        let mut builder = HttpRequest::builder().method("GET").uri(route);
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

    async fn get_rules(state: AlertRulesState, token: Option<&str>) -> (StatusCode, Value) {
        get(state, RULES_ROUTE, token).await
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

    /// No credential, no rules and no alerts: the same 401 every tenant-scoped
    /// route on this listener returns, with the typed error body the query
    /// surfaces use.
    #[tokio::test]
    async fn a_request_with_no_tenant_is_refused() {
        for route in [RULES_ROUTE, ALERTS_ROUTE] {
            let (status, body) = get(evaluating_state(), route, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{route}");
            assert_eq!(
                body,
                json!({
                    "status": "error",
                    "errorType": "unauthorized",
                    "error": "authentication required",
                }),
                "{route}"
            );

            // A credential that does not resolve is the same refusal, so a
            // caller cannot read another tenant's state by guessing a token.
            let (status, _) = get(evaluating_state(), route, Some("not-a-configured-token")).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{route}");
        }
    }

    /// Replaces `the_alerts_route_is_not_served_by_this_router`, which pinned
    /// a 404 for `/api/v1/alerts` while this router did not serve it. The
    /// route is served now, and what it answers before there is any state to
    /// serve is the property worth pinning: a 503 naming the reason, never an
    /// empty list that would claim nothing is firing.
    #[tokio::test]
    async fn the_alerts_route_refuses_rather_than_answering_empty_without_published_state() {
        let (status, body) = get(state(), ALERTS_ROUTE, Some(ACME_TOKEN)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body,
            json!({"status": "error", "errorType": "unavailable", "error": NOT_EVALUATING}),
            "no evaluator in this process"
        );

        let (status, body) = get(evaluating_state(), ALERTS_ROUTE, Some(ACME_TOKEN)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body,
            json!({"status": "error", "errorType": "unavailable", "error": NOT_YET_EVALUATED}),
            "an evaluator for the tenant that has not ticked"
        );

        // A tenant with no rules has no alerts to raise, so an evaluating
        // process can say so.
        let (status, body) = get(evaluating_state(), ALERTS_ROUTE, Some(OTHER_TOKEN)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"status": "success", "data": {"alerts": []}}));
    }

    fn alert(labels: &[(&str, &str)]) -> PublishedAlert {
        PublishedAlert {
            rule_id: "high-cpu".into(),
            alert_id: ravel_alerting::compute_alert_id("high-cpu", &[]),
            labels: labels
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect(),
            annotations: vec![("summary".into(), "hot".into())],
            state: AlertState::Firing,
            active_at_ns: 1_500_000_000,
            value: Some(f64::INFINITY),
        }
    }

    /// The alert object: Prometheus fields, the Alertmanager sink's
    /// `alertname` rule, and Prometheus' rendering of a non-finite value.
    #[test]
    fn an_alert_renders_in_prometheus_shape_with_the_sinks_alertname() {
        let series_alertname = [("alertname", "x"), ("exported_alertname", "y")];
        assert_eq!(
            render_alert(&alert(&series_alertname), &[]),
            json!({
                "labels": {
                    "alertname": "high-cpu",
                    "exported_alertname": "y",
                    "exported_exported_alertname": "x"
                },
                "annotations": {"summary": "hot"},
                "state": "firing",
                "activeAt": "1970-01-01T00:00:01.500000000Z",
                "value": "+Inf"
            })
        );

        let rule_labels = vec![("alertname".to_string(), "CpuHot".to_string())];
        let mut no_value = alert(&[("alertname", "CpuHot")]);
        no_value.value = None;
        assert_eq!(
            render_alert(&no_value, &rule_labels),
            json!({
                "labels": {"alertname": "CpuHot"},
                "annotations": {"summary": "hot"},
                "state": "firing",
                "activeAt": "1970-01-01T00:00:01.500000000Z"
            }),
            "a rule's own alertname wins, and an unknown value is omitted"
        );

        assert_eq!(format_value(f64::NAN), "NaN");
        assert_eq!(format_value(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_value(0.25), "0.25");
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
