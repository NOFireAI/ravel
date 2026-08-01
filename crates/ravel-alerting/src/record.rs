//! Alert identity, the decoded [`AlertRecord`], and the write path
//! (deliverables 3 and 5).
//!
//! An alert record is structurally a log record (ADR-0040 decision 2): it is
//! produced through `ravel-logseg`'s [`RlogWriter`] verbatim, the same way any
//! other `Signal::Alerts` producer would, and read back through
//! [`RlogReader`]. This crate builds no segment bytes of its own; see the crate
//! doc comment for the attrs-key convention.

use ravel_logseg::{LogRecord, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_types::logstream::{AttrValue, LogStreamId, log_stream_id};

use crate::error::AlertError;
use crate::generation::{compute_generation, guard_generation};
use crate::rule::Rule;
use crate::state::{AlertState, StateTransition};

/// Reserved attr keys carrying an alert record's structured fields. Namespaced
/// so a rule label named `state` cannot collide with the reserved `state` attr
/// (labels live under the `label.` prefix).
pub const ATTR_ALERT_ID: &str = "alert_id";
pub const ATTR_RULE_ID: &str = "rule_id";
pub const ATTR_STATE: &str = "state";
pub const ATTR_GENERATION: &str = "generation";
/// Prefix for one attr entry per rule label: `label.<name>`.
pub const LABEL_PREFIX: &str = "label.";
/// Prefix for one attr entry per annotation: `annotation.<name>`.
pub const ANNOTATION_PREFIX: &str = "annotation.";

/// The stream a rule's alert records ride: OTLP scope name and version stamped
/// on every record, with the rule id as the sole resource attribute. Alert
/// stream cardinality is thus one per rule, well within the log-stream model's
/// ~one-per-service bound.
const SCOPE_NAME: &str = "ravel-alerting";
const SCOPE_VERSION: &str = "1";
const RESOURCE_RULE_ID_KEY: &str = "rule.id";

/// 128-bit stable alert identity: the hash of `rule_id` plus a label *set*
/// (deliverable 3). All records for one distinct alerting condition share it,
/// which is what makes the fold-to-current-state model (ADR-0040 decision 3)
/// work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AlertId(pub [u8; 16]);

impl AlertId {
    /// Lowercase hex, as stored in the `alert_id` attr.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses the 32-char hex form back into an [`AlertId`].
    pub fn from_hex(s: &str) -> Result<AlertId, AlertError> {
        let bytes = hex::decode(s)
            .map_err(|_| AlertError::MalformedRecord(format!("alert_id hex: {s}")))?;
        let arr: [u8; 16] = bytes
            .try_into()
            .map_err(|_| AlertError::MalformedRecord(format!("alert_id length: {s}")))?;
        Ok(AlertId(arr))
    }
}

/// Computes the stable [`AlertId`] for a rule id and its label set.
///
/// Deterministic and order-independent over labels: the labels are sorted by
/// `(name, value)` before hashing, so the same rule id and the same set of
/// label pairs in any order always produce the same id, and any change to the
/// rule id or to any label name or value produces a different id (collision
/// only at blake3's 128-bit truncation probability).
///
/// Canonical preimage (domain-separated so it never aliases another Ravel
/// hash), all lengths little-endian `u32`:
///
/// ```text
/// "ravel-alert-id-v1\0"
/// u32(len(rule_id)) rule_id
/// u32(label_count)
/// per label sorted by (name, value): u32(len(name)) name u32(len(value)) value
/// ```
pub fn compute_alert_id(rule_id: &str, labels: &[(String, String)]) -> AlertId {
    let mut sorted: Vec<(&str, &str)> = labels
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    sorted.sort_unstable();

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ravel-alert-id-v1\0");
    put_len_prefixed(&mut hasher, rule_id.as_bytes());
    hasher.update(&(sorted.len() as u32).to_le_bytes());
    for (name, value) in sorted {
        put_len_prefixed(&mut hasher, name.as_bytes());
        put_len_prefixed(&mut hasher, value.as_bytes());
    }
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    AlertId(out)
}

fn put_len_prefixed(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u32).to_le_bytes());
    hasher.update(bytes);
}

/// A decoded alert record: the fields an evaluator reads back when folding to
/// current state, and the fields the write path emits. Round-trips exactly
/// through [`AlertRecord::to_log_record`] and [`AlertRecord::from_log_record`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertRecord {
    /// Stable identity shared by every record for this alerting condition.
    pub alert_id: AlertId,
    /// The rule that produced the record.
    pub rule_id: String,
    /// The state this record records a transition into.
    pub state: AlertState,
    /// Alerts-on-alerts generation (0 for ordinary metric/log rules).
    pub generation: u32,
    /// Event time of the transition, in nanoseconds. Pending-duration elapse is
    /// measured from this.
    pub ts_ns: i64,
    /// The rule's labels, sorted by `(name, value)` on decode.
    pub labels: Vec<(String, String)>,
    /// The rule's annotations, sorted by `(name, value)` on decode.
    pub annotations: Vec<(String, String)>,
    /// Human-readable summary stored in the record `body`.
    pub body: String,
}

impl AlertRecord {
    /// Maps this record onto a `ravel-logseg` [`LogRecord`] shaped as a
    /// `Signal::Alerts` record: the reserved scalar attrs, `label.<k>` per
    /// label, `annotation.<k>` per annotation, and the state mirrored onto
    /// `severity_text`/`severity_num` (ADR-0040 decision 2).
    pub fn to_log_record(&self) -> LogRecord {
        let resource = [(
            RESOURCE_RULE_ID_KEY.to_string(),
            AttrValue::Str(self.rule_id.clone()),
        )];
        let scope_attrs: [(String, AttrValue); 0] = [];
        let stream_attrs =
            ravel_logseg::stream_attrs_bytes(&resource, SCOPE_NAME, SCOPE_VERSION, &scope_attrs);
        let stream_id: LogStreamId =
            log_stream_id(&resource, SCOPE_NAME, SCOPE_VERSION, &scope_attrs);

        let mut attrs = vec![
            (
                ATTR_ALERT_ID.to_string(),
                AttrValue::Str(self.alert_id.to_hex()),
            ),
            (
                ATTR_RULE_ID.to_string(),
                AttrValue::Str(self.rule_id.clone()),
            ),
            (
                ATTR_STATE.to_string(),
                AttrValue::Str(self.state.as_str().to_string()),
            ),
            (
                ATTR_GENERATION.to_string(),
                AttrValue::I64(i64::from(self.generation)),
            ),
        ];
        for (k, v) in &self.labels {
            attrs.push((format!("{LABEL_PREFIX}{k}"), AttrValue::Str(v.clone())));
        }
        for (k, v) in &self.annotations {
            attrs.push((format!("{ANNOTATION_PREFIX}{k}"), AttrValue::Str(v.clone())));
        }

        LogRecord {
            stream_id,
            stream_attrs,
            ts_ns: self.ts_ns,
            observed_ts_ns: self.ts_ns,
            severity_num: self.state.severity_num(),
            severity_text: self.state.as_str().to_string(),
            body: self.body.clone(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        }
    }

    /// Decodes a scanned [`LogRecord`] back into an [`AlertRecord`], the inverse
    /// of [`AlertRecord::to_log_record`]. Returns [`AlertError::MalformedRecord`]
    /// when a reserved attr is missing or carries the wrong value type; unknown
    /// attrs are ignored so future additions do not break the decoder.
    pub fn from_log_record(rec: &LogRecord) -> Result<AlertRecord, AlertError> {
        let mut alert_id = None;
        let mut rule_id = None;
        let mut state = None;
        let mut generation = None;
        let mut labels = Vec::new();
        let mut annotations = Vec::new();

        for (key, value) in &rec.attrs {
            match key.as_str() {
                ATTR_ALERT_ID => alert_id = Some(AlertId::from_hex(as_str(key, value)?)?),
                ATTR_RULE_ID => rule_id = Some(as_str(key, value)?.to_string()),
                ATTR_STATE => {
                    let s = as_str(key, value)?;
                    state = Some(
                        AlertState::parse(s)
                            .ok_or_else(|| AlertError::MalformedRecord(format!("state: {s}")))?,
                    );
                }
                ATTR_GENERATION => generation = Some(as_generation(key, value)?),
                other => {
                    if let Some(name) = other.strip_prefix(LABEL_PREFIX) {
                        labels.push((name.to_string(), as_str(key, value)?.to_string()));
                    } else if let Some(name) = other.strip_prefix(ANNOTATION_PREFIX) {
                        annotations.push((name.to_string(), as_str(key, value)?.to_string()));
                    }
                    // Any other attr is not part of the alert contract; ignore.
                }
            }
        }

        labels.sort_unstable();
        annotations.sort_unstable();

        Ok(AlertRecord {
            alert_id: alert_id
                .ok_or_else(|| AlertError::MalformedRecord("missing alert_id".into()))?,
            rule_id: rule_id
                .ok_or_else(|| AlertError::MalformedRecord("missing rule_id".into()))?,
            state: state.ok_or_else(|| AlertError::MalformedRecord("missing state".into()))?,
            generation: generation
                .ok_or_else(|| AlertError::MalformedRecord("missing generation".into()))?,
            ts_ns: rec.ts_ns,
            labels,
            annotations,
            body: rec.body.clone(),
        })
    }
}

fn as_str<'a>(key: &str, value: &'a AttrValue) -> Result<&'a str, AlertError> {
    match value {
        AttrValue::Str(s) => Ok(s),
        _ => Err(AlertError::MalformedRecord(format!(
            "{key}: expected string"
        ))),
    }
}

fn as_generation(key: &str, value: &AttrValue) -> Result<u32, AlertError> {
    match value {
        AttrValue::I64(v) => u32::try_from(*v)
            .map_err(|_| AlertError::MalformedRecord(format!("{key}: out of u32 range: {v}"))),
        _ => Err(AlertError::MalformedRecord(format!("{key}: expected i64"))),
    }
}

/// Builds the [`AlertRecord`] a transition writes: the rule's identity and
/// labels, the transition's target state, the computed generation, and the
/// tick's clock reading as the event time.
///
/// `rule.labels` is sorted before storing, mirroring [`compute_alert_id`]'s
/// own canonicalization: `alert_id` already treats labels as an order-
/// independent set, and a record that stored them in whatever order the rule
/// happened to list them would silently disagree with that -- two records
/// for the same `alert_id`, differing only in label insertion order, must
/// compare equal once decoded.
pub fn build_transition_record(
    rule: &Rule,
    state: AlertState,
    generation: u32,
    now_ns: i64,
) -> AlertRecord {
    let mut labels = rule.labels.clone();
    labels.sort_unstable();
    AlertRecord {
        alert_id: compute_alert_id(&rule.rule_id, &rule.labels),
        rule_id: rule.rule_id.clone(),
        state,
        generation,
        ts_ns: now_ns,
        labels,
        annotations: rule.annotations.clone(),
        body: format!("{} {}", rule.rule_id, state.as_str()),
    }
}

/// Encodes a single alert record into a finished RLOG object ready to PUT.
/// This crate produces the bytes; the caller performs the object-store write,
/// the same "produce bytes" / "write them" split `ravel-logseg` keeps.
pub fn encode_record_object(
    record: &AlertRecord,
    cfg: RlogConfig,
    identity: ObjectIdentity,
) -> Result<Vec<u8>, AlertError> {
    let mut writer = RlogWriter::new(cfg, identity);
    writer.push(record.to_log_record())?;
    Ok(writer.finish()?)
}

/// The full write path (deliverable 5): given a rule, the transition decision
/// for this tick, the most recent prior record for this alert (if any), the
/// generations of any alert records this tick's query consumed as input (only
/// meaningful when `rule.query.targets_alerts_table()`; pass `&[]` otherwise),
/// and the RLOG writer parameters, produce the record bytes to PUT.
///
/// Generation is computed here, not passed in by the caller, precisely so a
/// caller cannot accidentally recompute it fresh from a tick's inputs for an
/// alert that already has history: `prior` is threaded straight into
/// [`compute_generation`], which returns the prior record's generation
/// unchanged when one exists (see that function's doc for why - generation is
/// a property of the alert's whole lifecycle, not of any one tick).
///
/// Returns `Ok(None)` when `transition.write_record` is false: a tick that only
/// re-confirms the current state writes nothing (ADR-0043 decision 4). When a
/// record is warranted, the generation is first checked against the rule's
/// effective cap ([`guard_generation`]); exceeding it is an error, not a
/// silently dropped record.
pub fn write_alert_record(
    rule: &Rule,
    transition: &StateTransition,
    prior: Option<&AlertRecord>,
    input_alert_generations: &[u32],
    now_ns: i64,
    cfg: RlogConfig,
    identity: ObjectIdentity,
) -> Result<Option<Vec<u8>>, AlertError> {
    if !transition.write_record {
        return Ok(None);
    }
    let generation = compute_generation(
        prior.map(|r| r.generation),
        rule.query.targets_alerts_table(),
        input_alert_generations,
    );
    guard_generation(generation, rule.max_alert_generation)?;
    let record = build_transition_record(rule, transition.next_state, generation, now_ns);
    Ok(Some(encode_record_object(&record, cfg, identity)?))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::rule::{RuleCondition, RuleQuery};
    use proptest::prelude::*;
    use ravel_logseg::{Predicate, RlogReader};

    fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn alert_id_is_deterministic() {
        let a = compute_alert_id("r", &labels(&[("a", "1"), ("b", "2")]));
        let b = compute_alert_id("r", &labels(&[("a", "1"), ("b", "2")]));
        assert_eq!(a, b);
    }

    #[test]
    fn alert_id_is_label_order_independent() {
        let a = compute_alert_id("r", &labels(&[("a", "1"), ("b", "2")]));
        let b = compute_alert_id("r", &labels(&[("b", "2"), ("a", "1")]));
        assert_eq!(a, b);
    }

    #[test]
    fn alert_id_distinguishes_rule_id_and_labels() {
        let base = compute_alert_id("r", &labels(&[("a", "1")]));
        // Different rule id.
        assert_ne!(base, compute_alert_id("r2", &labels(&[("a", "1")])));
        // Different label value.
        assert_ne!(base, compute_alert_id("r", &labels(&[("a", "2")])));
        // Different label name.
        assert_ne!(base, compute_alert_id("r", &labels(&[("b", "1")])));
        // Extra label.
        assert_ne!(
            base,
            compute_alert_id("r", &labels(&[("a", "1"), ("b", "2")]))
        );
    }

    #[test]
    fn alert_id_no_field_boundary_aliasing() {
        // Length-prefixing keeps the preimage injective: moving a character
        // across the rule_id/label boundary must change the id.
        assert_ne!(
            compute_alert_id("ab", &labels(&[("c", "d")])),
            compute_alert_id("a", &labels(&[("bc", "d")]))
        );
        assert_ne!(
            compute_alert_id("r", &labels(&[("ab", "c")])),
            compute_alert_id("r", &labels(&[("a", "bc")]))
        );
    }

    #[test]
    fn alert_id_hex_roundtrips() {
        let id = compute_alert_id("r", &labels(&[("a", "1")]));
        assert_eq!(AlertId::from_hex(&id.to_hex()).expect("hex"), id);
    }

    fn arb_labels() -> impl Strategy<Value = Vec<(String, String)>> {
        proptest::collection::vec(("[a-z]{1,6}", "[a-z0-9]{0,6}"), 0..6)
    }

    proptest! {
        #[test]
        fn alert_id_same_input_same_hash(rule_id in "[a-z]{1,8}", ls in arb_labels(), seed in any::<u64>()) {
            // Any permutation of the same rule_id + label set hashes the same.
            let mut permuted = ls.clone();
            if permuted.len() > 1 {
                let rot = (seed as usize) % permuted.len();
                permuted.rotate_left(rot);
            }
            prop_assert_eq!(
                compute_alert_id(&rule_id, &ls),
                compute_alert_id(&rule_id, &permuted)
            );
        }

        #[test]
        fn alert_id_distinct_label_sets_hash_distinctly(
            rule_id in "[a-z]{1,8}",
            ls in arb_labels(),
            seed in any::<u64>(),
        ) {
            // Adding a fresh, unique label name changes the identity: distinct
            // label sets produce distinct ids (collision only at blake3's
            // 128-bit truncation probability, negligible here).
            let mut extended = ls.clone();
            extended.push(("zzzz_unique".to_string(), format!("{seed}")));
            prop_assert_ne!(
                compute_alert_id(&rule_id, &ls),
                compute_alert_id(&rule_id, &extended)
            );
        }
    }

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [3u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    #[test]
    fn record_roundtrips_through_writer_and_reader() {
        // Labels and annotations already sorted, matching what from_log_record
        // returns, so the decoded record equals the original.
        let original = AlertRecord {
            alert_id: compute_alert_id("high-cpu", &labels(&[("severity", "page")])),
            rule_id: "high-cpu".into(),
            state: AlertState::Firing,
            generation: 3,
            ts_ns: 1_700_000_000 * 1_000_000_000,
            labels: labels(&[("region", "eu-1"), ("severity", "page")]),
            annotations: labels(&[("summary", "cpu is hot")]),
            body: "high-cpu firing".into(),
        };

        let cfg = RlogConfig::default();
        let bytes = encode_record_object(&original, cfg, identity()).expect("encode");

        let reader = RlogReader::new(&bytes, &cfg).expect("open");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        assert_eq!(rows.len(), 1, "one record in, one out");

        let decoded = AlertRecord::from_log_record(&rows[0]).expect("decode");
        assert_eq!(decoded, original, "every field survives the round trip");
    }

    #[test]
    fn write_alert_record_skips_when_no_transition() {
        let rule = Rule {
            rule_id: "r".into(),
            query: RuleQuery::Sql("select 1".into()),
            condition: RuleCondition::NonEmptyResult,
            labels: vec![],
            annotations: vec![],
            for_duration: None,
            max_alert_generation: None,
        };
        let keep = StateTransition {
            next_state: AlertState::Firing,
            write_record: false,
        };
        let out = write_alert_record(
            &rule,
            &keep,
            None,
            &[],
            0,
            RlogConfig::default(),
            identity(),
        )
        .expect("no error");
        assert!(out.is_none(), "a re-confirming tick writes nothing");
    }

    #[test]
    fn write_alert_record_rejects_over_generation() {
        let rule = Rule {
            rule_id: "r".into(),
            query: RuleQuery::Sql("select * from alerts".into()),
            condition: RuleCondition::NonEmptyResult,
            labels: vec![],
            annotations: vec![],
            for_duration: None,
            max_alert_generation: Some(2),
        };
        let fire = StateTransition {
            next_state: AlertState::Firing,
            write_record: true,
        };
        // targets_alerts_table() is true (query mentions "alerts"), prior is
        // None (new alert), so generation = max(input_alert_generations, 0) +
        // 1: an input at generation 2 produces generation 3, over the cap.
        let err = write_alert_record(
            &rule,
            &fire,
            None,
            &[2],
            0,
            RlogConfig::default(),
            identity(),
        );
        assert!(matches!(err, Err(AlertError::GenerationExceeded { .. })));
    }

    #[test]
    fn write_alert_record_keeps_priors_generation_on_a_later_transition() {
        // The regression this fix targets: a firing record at generation 5,
        // then the same alert_id resolves on a tick whose query (this rule
        // targets the alerts table) matched zero alert rows this time.
        // Without stickiness this would recompute to 1.
        let rule = Rule {
            rule_id: "r".into(),
            query: RuleQuery::Sql("select * from alerts".into()),
            condition: RuleCondition::NonEmptyResult,
            labels: vec![],
            annotations: vec![],
            for_duration: None,
            max_alert_generation: None,
        };
        let firing_record = build_transition_record(&rule, AlertState::Firing, 5, 0);
        let resolve = StateTransition {
            next_state: AlertState::Resolved,
            write_record: true,
        };
        let bytes = write_alert_record(
            &rule,
            &resolve,
            Some(&firing_record),
            &[],
            1,
            RlogConfig::default(),
            identity(),
        )
        .expect("no error")
        .expect("a transition writes a record");
        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&bytes, &cfg).expect("open");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        let decoded = AlertRecord::from_log_record(&rows[0]).expect("decode");
        assert_eq!(
            decoded.generation, 5,
            "generation carries forward unchanged"
        );
        assert_eq!(decoded.state, AlertState::Resolved);
    }
}
