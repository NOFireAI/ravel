//! Notification sinks for alert transitions (ADR-0043 decision 6, issue #382).
//!
//! Two sinks ship in v1: a generic webhook (the transition record as JSON) and
//! an Alertmanager-compatible sink (Alertmanager's own `/api/v2/alerts` payload
//! shape). Both are notification paths, never a second commit path: the durable
//! `Signal::Alerts` record is the source of truth, and
//! [`crate::alerting::AlertEvaluator`] writes it before any sink is contacted.
//! A sink that fails is logged and retried on a later tick from the latest
//! record; it cannot fail, delay, or alter the write.
//!
//! Delivery is at-least-once, which is what the ADR chose over an exactly-once
//! outbox: consumers are expected to be idempotent, the same assumption the
//! rest of Ravel makes downstream of its own acknowledgement.

use std::time::{Duration, UNIX_EPOCH};

use ravel_alerting::{AlertRecord, AlertState};
use serde_json::{Map as JsonMap, Value as Json};

/// Wall-clock ceiling on one sink HTTP request. A slow sink must not stretch an
/// evaluation tick without bound; exceeding this is an ordinary delivery
/// failure, retried on a later tick.
pub const DEFAULT_SINK_TIMEOUT: Duration = Duration::from_secs(10);

/// The path an Alertmanager sink POSTs to, appended to a configured base URL
/// that does not already end in it.
pub const ALERTMANAGER_PATH: &str = "/api/v2/alerts";

/// One configured notification target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertSink {
    /// POSTs [`webhook_payload`] to `url`.
    Webhook { url: String },
    /// POSTs [`alertmanager_payload`] to `url`, which is already the full
    /// `/api/v2/alerts` endpoint (see [`AlertSink::alertmanager`]).
    Alertmanager { url: String },
}

impl AlertSink {
    /// A webhook sink posting to `url` verbatim.
    pub fn webhook(url: impl Into<String>) -> AlertSink {
        AlertSink::Webhook { url: url.into() }
    }

    /// An Alertmanager sink for `base`, which may be either an Alertmanager
    /// base URL (`http://alertmanager:9093`) or the full alerts endpoint. The
    /// well-known path is appended when it is not already there, so an operator
    /// can configure whichever form they have to hand.
    pub fn alertmanager(base: impl Into<String>) -> AlertSink {
        let base = base.into();
        let trimmed = base.trim_end_matches('/');
        let url = if trimmed.ends_with(ALERTMANAGER_PATH) {
            trimmed.to_string()
        } else {
            format!("{trimmed}{ALERTMANAGER_PATH}")
        };
        AlertSink::Alertmanager { url }
    }

    /// The URL this sink POSTs to.
    pub fn url(&self) -> &str {
        match self {
            AlertSink::Webhook { url } | AlertSink::Alertmanager { url } => url,
        }
    }

    /// A short static name for log lines.
    pub fn kind(&self) -> &'static str {
        match self {
            AlertSink::Webhook { .. } => "webhook",
            AlertSink::Alertmanager { .. } => "alertmanager",
        }
    }

    /// The JSON body this sink sends for `notification`, or `None` when this
    /// sink has nothing to say about that transition (see
    /// [`alertmanager_payload`] on pending alerts).
    pub fn payload(&self, notification: &AlertNotification) -> Option<Json> {
        match self {
            AlertSink::Webhook { .. } => Some(webhook_payload(notification)),
            AlertSink::Alertmanager { .. } => alertmanager_payload(notification),
        }
    }
}

/// One durably-written transition, ready to notify on.
///
/// `previous_state` and `started_at_ns` come from the record this transition
/// superseded, so a sink can render "firing since T" without the evaluator
/// holding any in-memory alert state (ADR-0043 decision 3: compute processes
/// are disposable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertNotification {
    /// The record as it was persisted.
    pub record: AlertRecord,
    /// The state the alert was in before this transition, if it had history.
    pub previous_state: Option<AlertState>,
    /// When the current episode began: the prior record's timestamp when that
    /// record opened the episode (pending or firing), else this record's own.
    /// Only an approximation of the true onset when the episode is longer than
    /// one transition (pending then firing reports the firing time), which is
    /// all a single-step fold can know without replaying the whole history.
    pub started_at_ns: i64,
}

impl AlertNotification {
    /// Pairs a freshly written record with the record it superseded.
    pub fn new(record: AlertRecord, prior: Option<&AlertRecord>) -> AlertNotification {
        let previous_state = prior.map(|p| p.state);
        let started_at_ns = match prior {
            Some(p) if matches!(p.state, AlertState::Pending | AlertState::Firing) => p.ts_ns,
            _ => record.ts_ns,
        };
        AlertNotification {
            record,
            previous_state,
            started_at_ns,
        }
    }
}

/// Renders `pairs` as a JSON object. Alert labels and annotations are already
/// sorted by `(name, value)` on the record, so the object's key order is
/// deterministic for a byte-comparing consumer.
fn pairs_to_object(pairs: &[(String, String)]) -> Json {
    let mut map = JsonMap::new();
    for (k, v) in pairs {
        map.insert(k.clone(), Json::String(v.clone()));
    }
    Json::Object(map)
}

/// RFC3339 with nanosecond precision, the timestamp format Alertmanager's API
/// expects.
///
/// Timestamps before the Unix epoch are clamped to the epoch rather than
/// formatted: `humantime`'s formatter panics on a pre-epoch `SystemTime`, and a
/// notification path must never panic. An alert record's timestamp is a
/// wall-clock reading, so the clamp is unreachable outside a badly skewed
/// clock.
fn rfc3339(ts_ns: i64) -> String {
    let ns = u64::try_from(ts_ns).unwrap_or(0);
    let time = UNIX_EPOCH
        .checked_add(Duration::from_nanos(ns))
        .unwrap_or(UNIX_EPOCH);
    humantime::format_rfc3339_nanos(time).to_string()
}

/// The generic webhook body: the transition record, whole, plus the two
/// fold-derived fields a consumer would otherwise have to reconstruct.
///
/// `ts_unix_nano` and `started_at_unix_nano` are JSON numbers carrying the
/// exact nanosecond values (`serde_json` renders an `i64` losslessly); the
/// RFC3339 fields alongside them are for consumers that would round the
/// integers through a float.
pub fn webhook_payload(notification: &AlertNotification) -> Json {
    let record = &notification.record;
    let mut body = JsonMap::new();
    body.insert(
        "alert_id".to_string(),
        Json::String(record.alert_id.to_hex()),
    );
    body.insert("rule_id".to_string(), Json::String(record.rule_id.clone()));
    body.insert(
        "state".to_string(),
        Json::String(record.state.as_str().to_string()),
    );
    body.insert(
        "previous_state".to_string(),
        match notification.previous_state {
            Some(state) => Json::String(state.as_str().to_string()),
            None => Json::Null,
        },
    );
    body.insert("generation".to_string(), Json::from(record.generation));
    body.insert("ts_unix_nano".to_string(), Json::from(record.ts_ns));
    body.insert(
        "ts_rfc3339".to_string(),
        Json::String(rfc3339(record.ts_ns)),
    );
    body.insert(
        "started_at_unix_nano".to_string(),
        Json::from(notification.started_at_ns),
    );
    body.insert(
        "started_at_rfc3339".to_string(),
        Json::String(rfc3339(notification.started_at_ns)),
    );
    body.insert("labels".to_string(), pairs_to_object(&record.labels));
    body.insert(
        "annotations".to_string(),
        pairs_to_object(&record.annotations),
    );
    body.insert("body".to_string(), Json::String(record.body.clone()));
    Json::Object(body)
}

/// Alertmanager's `POST /api/v2/alerts` body: a JSON array of alert objects,
/// here always of length one (Ravel notifies per transition).
///
/// Faithful to how Prometheus itself talks to Alertmanager:
///
/// - Labels are `alertname` (the rule id) plus the rule's own labels, and a
///   rule label named `alertname` deliberately wins, matching Prometheus'
///   override order. Ravel-specific fields are not injected, because in
///   Alertmanager the label set *is* the alert's identity and extra labels
///   would fragment grouping and silences.
/// - A firing alert sends `startsAt` and no `endsAt`, letting Alertmanager
///   apply its own resolve timeout. A resolved or suppressed alert sends
///   `endsAt` at the transition, which is how Alertmanager is told an alert is
///   over.
/// - A `pending` transition returns `None`: Prometheus does not notify
///   Alertmanager until an alert fires, and sending pending alerts would page
///   on conditions the `for` duration exists to filter out. The webhook sink
///   still receives it.
pub fn alertmanager_payload(notification: &AlertNotification) -> Option<Json> {
    let record = &notification.record;
    if record.state == AlertState::Pending {
        return None;
    }

    let mut labels = JsonMap::new();
    labels.insert(
        "alertname".to_string(),
        Json::String(record.rule_id.clone()),
    );
    for (k, v) in &record.labels {
        labels.insert(k.clone(), Json::String(v.clone()));
    }

    let mut alert = JsonMap::new();
    alert.insert("labels".to_string(), Json::Object(labels));
    alert.insert(
        "annotations".to_string(),
        pairs_to_object(&record.annotations),
    );
    alert.insert(
        "startsAt".to_string(),
        Json::String(rfc3339(notification.started_at_ns)),
    );
    if matches!(record.state, AlertState::Resolved | AlertState::Suppressed) {
        alert.insert("endsAt".to_string(), Json::String(rfc3339(record.ts_ns)));
    }
    Some(Json::Array(vec![Json::Object(alert)]))
}

/// POSTs `notification` to `sink`. A sink with nothing to send for this
/// transition succeeds without a request.
///
/// Any transport error or non-2xx status is an error the caller logs and
/// retries later. This function performs no retry of its own: retrying inside
/// a tick would hold the evaluation loop on an endpoint that is already known
/// to be unhealthy, where the next tick costs nothing and carries the same
/// record.
pub async fn deliver(
    client: &reqwest::Client,
    sink: &AlertSink,
    notification: &AlertNotification,
) -> anyhow::Result<()> {
    let Some(body) = sink.payload(notification) else {
        return Ok(());
    };
    let response = client.post(sink.url()).json(&body).send().await?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("sink {} returned HTTP {}", sink.url(), status);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_alerting::compute_alert_id;

    const SEC: i64 = 1_000_000_000;

    fn pairs(kv: &[(&str, &str)]) -> Vec<(String, String)> {
        kv.iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn record(state: AlertState, ts_ns: i64) -> AlertRecord {
        let labels = pairs(&[("region", "eu-1"), ("severity", "page")]);
        AlertRecord {
            alert_id: compute_alert_id("high-cpu", &labels),
            rule_id: "high-cpu".to_string(),
            state,
            generation: 0,
            ts_ns,
            labels,
            annotations: pairs(&[("summary", "cpu is hot")]),
            body: format!("high-cpu {}", state.as_str()),
        }
    }

    #[test]
    fn alertmanager_base_url_gets_the_well_known_path() {
        assert_eq!(
            AlertSink::alertmanager("http://am:9093").url(),
            "http://am:9093/api/v2/alerts"
        );
        // A trailing slash does not produce a doubled separator.
        assert_eq!(
            AlertSink::alertmanager("http://am:9093/").url(),
            "http://am:9093/api/v2/alerts"
        );
        // An already-complete endpoint is left alone rather than doubled.
        assert_eq!(
            AlertSink::alertmanager("http://am:9093/api/v2/alerts").url(),
            "http://am:9093/api/v2/alerts"
        );
    }

    #[test]
    fn webhook_payload_carries_every_record_field() {
        let firing = record(AlertState::Firing, 1_700_000_000 * SEC);
        let prior = record(AlertState::Pending, 1_699_999_940 * SEC);
        let notification = AlertNotification::new(firing.clone(), Some(&prior));
        let body = webhook_payload(&notification);

        assert_eq!(body["alert_id"], Json::String(firing.alert_id.to_hex()));
        assert_eq!(body["rule_id"], Json::String("high-cpu".to_string()));
        assert_eq!(body["state"], Json::String("firing".to_string()));
        assert_eq!(body["previous_state"], Json::String("pending".to_string()));
        assert_eq!(body["generation"], Json::from(0));
        // Exact nanoseconds, not a rounded float.
        assert_eq!(body["ts_unix_nano"], Json::from(1_700_000_000 * SEC));
        assert_eq!(
            body["ts_rfc3339"],
            Json::String("2023-11-14T22:13:20.000000000Z".to_string())
        );
        // The episode started at the pending record, not at this transition.
        assert_eq!(
            body["started_at_unix_nano"],
            Json::from(1_699_999_940 * SEC)
        );
        assert_eq!(body["labels"]["region"], Json::String("eu-1".to_string()));
        assert_eq!(body["labels"]["severity"], Json::String("page".to_string()));
        assert_eq!(
            body["annotations"]["summary"],
            Json::String("cpu is hot".to_string())
        );
        assert_eq!(body["body"], Json::String("high-cpu firing".to_string()));
    }

    #[test]
    fn webhook_payload_previous_state_is_null_for_a_new_alert() {
        let notification = AlertNotification::new(record(AlertState::Firing, SEC), None);
        assert_eq!(webhook_payload(&notification)["previous_state"], Json::Null);
    }

    #[test]
    fn alertmanager_firing_payload_matches_the_api_v2_shape() {
        let firing = record(AlertState::Firing, 1_700_000_000 * SEC);
        let notification = AlertNotification::new(firing, None);
        let body = alertmanager_payload(&notification).expect("firing notifies alertmanager");

        let alerts = body.as_array().expect("api/v2/alerts takes an array");
        assert_eq!(alerts.len(), 1, "one transition, one alert object");
        let alert = &alerts[0];

        // alertname is the rule id; the rule's own labels ride alongside it.
        assert_eq!(
            alert["labels"]["alertname"],
            Json::String("high-cpu".to_string())
        );
        assert_eq!(alert["labels"]["region"], Json::String("eu-1".to_string()));
        assert_eq!(
            alert["labels"]["severity"],
            Json::String("page".to_string())
        );
        assert_eq!(
            alert["annotations"]["summary"],
            Json::String("cpu is hot".to_string())
        );
        assert_eq!(
            alert["startsAt"],
            Json::String("2023-11-14T22:13:20.000000000Z".to_string())
        );
        // A firing alert carries no endsAt: Alertmanager applies its own
        // resolve timeout, exactly as it does for Prometheus.
        assert!(
            alert.get("endsAt").is_none(),
            "a firing alert must not send endsAt"
        );
    }

    #[test]
    fn alertmanager_resolved_payload_sets_ends_at() {
        let resolved = record(AlertState::Resolved, 1_700_000_060 * SEC);
        let prior = record(AlertState::Firing, 1_700_000_000 * SEC);
        let notification = AlertNotification::new(resolved, Some(&prior));
        let body = alertmanager_payload(&notification).expect("resolved notifies alertmanager");
        let alert = &body.as_array().expect("array")[0];

        assert_eq!(
            alert["startsAt"],
            Json::String("2023-11-14T22:13:20.000000000Z".to_string()),
            "startsAt is the firing record, not the resolve"
        );
        assert_eq!(
            alert["endsAt"],
            Json::String("2023-11-14T22:14:20.000000000Z".to_string()),
            "endsAt is how Alertmanager is told the alert is over"
        );
    }

    #[test]
    fn alertmanager_skips_pending_but_the_webhook_does_not() {
        let notification = AlertNotification::new(record(AlertState::Pending, SEC), None);
        assert!(
            alertmanager_payload(&notification).is_none(),
            "Prometheus does not notify Alertmanager before an alert fires"
        );
        assert!(
            AlertSink::webhook("http://x")
                .payload(&notification)
                .is_some(),
            "the generic webhook still sees every transition"
        );
    }

    #[test]
    fn a_rule_label_named_alertname_overrides_the_rule_id() {
        // Matching Prometheus' own override order: alertname is inserted
        // first, so an explicit rule label wins.
        let mut rec = record(AlertState::Firing, SEC);
        rec.labels = pairs(&[("alertname", "explicit")]);
        let body = alertmanager_payload(&AlertNotification::new(rec, None)).expect("payload");
        assert_eq!(
            body.as_array().expect("array")[0]["labels"]["alertname"],
            Json::String("explicit".to_string())
        );
    }

    #[test]
    fn pre_epoch_timestamps_clamp_instead_of_panicking() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(rfc3339(-1), "1970-01-01T00:00:00.000000000Z");
    }
}
