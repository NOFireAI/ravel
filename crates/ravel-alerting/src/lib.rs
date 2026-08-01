//! Ravel's unified alerting engine, Wave 1 (issue #372, ADR-0043).
//!
//! This crate is pure logic: the rule shape, condition evaluation, the alert
//! state machine, the alerts-on-alerts generation guard, and the record write
//! path. It has no service wiring, no HTTP, no scheduler loop, and it never
//! executes PromQL or SQL or performs an object-store PUT. Wave 2 supplies the
//! per-tenant evaluator task, the real query engines, and the sinks; it calls
//! into the functions here.
//!
//! One generic [`Rule`] covers both rule families (ADR-0043 decision 1):
//! observability alert rules ([`RuleQuery::Promql`] + [`RuleCondition::Threshold`])
//! and security detection rules ([`RuleQuery::Sql`] + [`RuleCondition::NonEmptyResult`]).
//! There is no second engine.
//!
//! # Record shape
//!
//! An alert record reuses RLOG v1 verbatim (ADR-0040 decision 2): no new byte
//! layout, only the `Signal::Alerts` key prefix `"a"` at the object-key layer,
//! which is the caller's concern. A record is a [`ravel_logseg::LogRecord`]
//! with:
//!
//! - `ts` / `observed_ts`: the transition's event time.
//! - `body`: a human summary, `"<rule_id> <state>"`.
//! - `severity_text` / `severity_num`: the state mirrored onto the RLOG
//!   severity fields (ADR-0040 decision 2). The authoritative state is the
//!   `state` attr; severity is a sort-friendly mirror.
//! - `attrs` (the RLOG dynamic attribute map), by the following key convention:
//!   - `alert_id` — lowercase hex of the [`AlertId`] (stable hash of rule id +
//!     label set).
//!   - `rule_id` — the producing rule's id.
//!   - `state` — one of `pending` / `firing` / `resolved` / `suppressed`.
//!   - `generation` — an `i64`-typed attr, the alerts-on-alerts generation.
//!   - `label.<name>` — one entry per rule label.
//!   - `annotation.<name>` — one entry per annotation.
//!
//! Labels and annotations are namespaced under `label.` / `annotation.` so a
//! label named `state` cannot shadow the reserved `state` attr. This mirrors
//! how `ravel-rspan` / `ravel-logseg` document their own attrs conventions in a
//! crate-level doc comment rather than a separate markdown file.
//!
//! ## A note on `pending`
//!
//! ADR-0040 decision 2 lists the state vocabulary as `firing` / `resolved` /
//! `suppressed`. This crate adds `pending`, because ADR-0043 decision 3
//! requires deriving pending-duration from the *last written record's*
//! timestamp so a disposable, restarted evaluator resumes correctly. That is
//! only possible if the pending phase is itself a durable record. See
//! [`AlertState::Pending`].
//!
//! # State derivation
//!
//! State is derived by fold, never mutated (ADR-0040 decision 3). The caller
//! folds all records sharing an [`AlertId`] to the greatest-`ts` one and hands
//! it to [`evaluate_transition`] as `prior`; the function is pure over a
//! caller-supplied `now_ns`.

mod condition;
mod error;
mod generation;
mod record;
mod rule;
mod state;

pub use condition::{QueryResultSummary, condition_met};
pub use error::AlertError;
pub use generation::{DEFAULT_MAX_ALERT_GENERATION, compute_generation, guard_generation};
pub use record::{
    ANNOTATION_PREFIX, ATTR_ALERT_ID, ATTR_GENERATION, ATTR_RULE_ID, ATTR_STATE, AlertId,
    AlertRecord, LABEL_PREFIX, build_transition_record, compute_alert_id, encode_record_object,
    write_alert_record,
};
pub use rule::{Rule, RuleCondition, RuleQuery, ThresholdOp};
pub use state::{AlertState, StateTransition, evaluate_transition};
