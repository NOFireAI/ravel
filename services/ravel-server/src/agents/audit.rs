//! The agents audit records: one event per reconcile decision that changed an
//! effect and one per destroy that tombstoned an agent, submitted through the
//! server's audit pipeline into the tenant's audit shard beside the
//! query-audit records.
//!
//! The kernel also appends an audit event to the effect's turn's event
//! stream, but that event names no caller (issue #3408). This one names the
//! caller as far as a [`Principal`] can: its tenant, the capabilities it
//! holds, and its subject with the subject's source when the caller's token
//! or claim carries one (ADR-3541;
//! `reconcile_and_destroy_records_name_each_admins_configured_subject_and_a_token_without_one_writes_no_subject_attr`
//! in `tests/agents_effects_e2e.rs`). Two admins of one tenant whose tokens
//! carry no subject are still not told apart here.
//!
//! The record is submitted before the reconcile or destroy changes HEAD, not
//! after. In
//! required audit mode a refused submission refuses the reconcile, so no
//! change lands unaudited; the cost is a record for a decision whose HEAD
//! write then failed, which names an intent that did not apply. Written
//! after, a crash or a refused submission between the two would leave a
//! changed effect with no record, which is the case the audit exists for.

use ravel_agents_core::wire::AbandonedCounts;
use ravel_logseg::{AttrValue, LogStreamId, stream_attrs_bytes};
use ravel_maintain::AuditEvent;
use ravel_tenant_resolve::Principal;
use ravel_types::TenantHash;
use ravel_types::logstream::log_stream_id;

/// `attrs["kind"]` of an agents reconcile record.
pub const KIND_AGENTS_RECONCILE: &str = "agents_reconcile";
/// `attrs["kind"]` of an agents destroy record.
pub const KIND_AGENTS_DESTROY: &str = "agents_destroy";

const STREAM_RECORD_TYPE: &str = "agents_audit";
const STREAM_SCOPE_NAME: &str = "ravel.agents_audit";
const STREAM_SCOPE_VERSION: &str = "1";

/// What a reconcile did, as its audit record names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileAction {
    /// `record_result`: an operator-verified result was attached.
    RecordResult,
    /// `retry`: one more attempt was authorized.
    Retry,
}

impl ReconcileAction {
    pub fn as_str(self) -> &'static str {
        match self {
            ReconcileAction::RecordResult => "record_result",
            ReconcileAction::Retry => "retry",
        }
    }
}

/// The capabilities `principal` holds, comma separated, in a fixed order.
/// Destructured without `..`, so a capability added to [`Principal`] fails to
/// build here until it is listed.
fn capabilities(principal: &Principal) -> String {
    let Principal {
        tenant: _,
        ddl,
        ipcrypt,
        agents,
        agents_worker,
        agents_admin,
        subject: _,
    } = principal;
    [
        (*agents, "agents"),
        (*agents_worker, "agents_worker"),
        (*agents_admin, "agents_admin"),
        (*ddl, "ddl"),
        (*ipcrypt, "ipcrypt"),
    ]
    .iter()
    .filter(|(held, _)| *held)
    .map(|(_, name)| *name)
    .collect::<Vec<_>>()
    .join(",")
}

/// The attributes that name `principal`: its tenant, its capabilities and,
/// only when it carries a subject, the subject and its source. A principal
/// with no subject writes neither key, rather than an empty value
/// (`the_events_omit_the_subject_attrs_when_the_principal_has_none`).
fn principal_attrs(principal: &Principal) -> Vec<(String, AttrValue)> {
    let mut attrs = vec![
        (
            "agents.principal.tenant".to_string(),
            AttrValue::Str(principal.tenant.as_str().into()),
        ),
        (
            "agents.principal.capabilities".to_string(),
            AttrValue::Str(capabilities(principal)),
        ),
    ];
    if let Some(subject) = &principal.subject {
        attrs.push((
            "agents.principal.subject".to_string(),
            AttrValue::Str(subject.value().into()),
        ));
        attrs.push((
            "agents.principal.subject_source".to_string(),
            AttrValue::Str(subject.source().as_str().into()),
        ));
    }
    attrs
}

/// One agents audit event recording that `principal` reconciled `attempt` of
/// the effect `effect` (its handle) with `action`.
pub fn agents_reconcile_audit_event(
    tenant: &TenantHash,
    now_ns: i64,
    principal: &Principal,
    effect: &str,
    attempt: u32,
    action: ReconcileAction,
) -> AuditEvent {
    let (stream_id, stream_attrs) = agents_stream();
    let mut attrs = vec![
        (
            "kind".to_string(),
            AttrValue::Str(KIND_AGENTS_RECONCILE.into()),
        ),
        ("agents.tenant".to_string(), AttrValue::Str(tenant.to_hex())),
    ];
    attrs.extend(principal_attrs(principal));
    attrs.extend([
        ("agents.effect".to_string(), AttrValue::Str(effect.into())),
        (
            "agents.attempt".to_string(),
            AttrValue::Str(attempt.to_string()),
        ),
        (
            "agents.decision".to_string(),
            AttrValue::Str(action.as_str().into()),
        ),
    ]);
    AuditEvent {
        tenant: *tenant,
        now_ns,
        stream_id,
        stream_attrs,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("agents reconcile {}", action.as_str()),
        attrs,
    }
}

/// One agents audit event recording that `principal` destroyed
/// `incarnation` of the agent `agent_id`. `abandoned` holds the counts the
/// destroy route read from HEAD before its CAS, not the ones its response
/// reports: the store re-reads HEAD on every publish attempt, so a write
/// landing between the read and the CAS changes the response's counts and
/// not these.
pub fn agents_destroy_audit_event(
    tenant: &TenantHash,
    now_ns: i64,
    principal: &Principal,
    agent_id: &str,
    incarnation: u64,
    abandoned: &AbandonedCounts,
) -> AuditEvent {
    let (stream_id, stream_attrs) = agents_stream();
    let mut attrs = vec![
        (
            "kind".to_string(),
            AttrValue::Str(KIND_AGENTS_DESTROY.into()),
        ),
        ("agents.tenant".to_string(), AttrValue::Str(tenant.to_hex())),
    ];
    attrs.extend(principal_attrs(principal));
    attrs.extend([
        (
            "agents.agent_id".to_string(),
            AttrValue::Str(agent_id.into()),
        ),
        (
            "agents.incarnation".to_string(),
            AttrValue::Str(incarnation.to_string()),
        ),
        (
            "agents.abandoned.inputs".to_string(),
            AttrValue::Str(abandoned.inputs.to_string()),
        ),
        (
            "agents.abandoned.active_turn".to_string(),
            AttrValue::Str(abandoned.active_turn.to_string()),
        ),
        (
            "agents.abandoned.effects_in_doubt".to_string(),
            AttrValue::Str(abandoned.effects_in_doubt.to_string()),
        ),
        (
            "agents.abandoned.effects_authorized".to_string(),
            AttrValue::Str(abandoned.effects_authorized.to_string()),
        ),
        (
            "agents.abandoned.timers_scheduled".to_string(),
            AttrValue::Str(abandoned.timers_scheduled.to_string()),
        ),
    ]);
    AuditEvent {
        tenant: *tenant,
        now_ns,
        stream_id,
        stream_attrs,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "agents destroy".to_string(),
        attrs,
    }
}

fn agents_stream() -> (LogStreamId, Vec<u8>) {
    let resource = vec![(
        "ravel.record_type".to_string(),
        AttrValue::Str(STREAM_RECORD_TYPE.into()),
    )];
    let id = log_stream_id(&resource, STREAM_SCOPE_NAME, STREAM_SCOPE_VERSION, &[]);
    let blob = stream_attrs_bytes(&resource, STREAM_SCOPE_NAME, STREAM_SCOPE_VERSION, &[]);
    (id, blob)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_tenant_resolve::{Subject, SubjectSource};
    use ravel_types::TenantId;

    /// An `agents_admin` of `tenant` whose static token carries the subject
    /// `alice`.
    fn admin_with_subject(tenant: &TenantId) -> Principal {
        Principal {
            agents_admin: true,
            subject: Some(
                Subject::new(SubjectSource::StaticToken, "alice").expect("a valid subject"),
            ),
            ..Principal::tenant_only(tenant.clone())
        }
    }

    fn keys(event: &AuditEvent) -> Vec<&str> {
        event.attrs.iter().map(|(k, _)| k.as_str()).collect()
    }

    /// Flip to watch it fail: push the two subject keys with an empty value
    /// when `principal.subject` is `None` in `principal_attrs`.
    #[test]
    fn the_events_omit_the_subject_attrs_when_the_principal_has_none() {
        let tenant = TenantId::new("acme");
        let principal = Principal {
            agents_admin: true,
            ..Principal::tenant_only(tenant.clone())
        };
        let reconcile = agents_reconcile_audit_event(
            &tenant.hash(),
            7,
            &principal,
            "effect-handle",
            2,
            ReconcileAction::Retry,
        );
        let destroy = agents_destroy_audit_event(
            &tenant.hash(),
            7,
            &principal,
            "agent-hex",
            2,
            &AbandonedCounts::default(),
        );
        for event in [&reconcile, &destroy] {
            let keys = keys(event);
            assert!(
                !keys.contains(&"agents.principal.subject"),
                "{}: {keys:?}",
                event.body
            );
            assert!(
                !keys.contains(&"agents.principal.subject_source"),
                "{}: {keys:?}",
                event.body
            );
            assert_eq!(keys[3], "agents.principal.capabilities", "{keys:?}");
        }
        assert_eq!(reconcile.attrs.len(), 7);
        assert_eq!(destroy.attrs.len(), 11);
    }

    #[test]
    fn the_event_names_tenant_effect_attempt_decision_and_caller() {
        let tenant = TenantId::new("acme");
        let principal = admin_with_subject(&tenant);
        let event = agents_reconcile_audit_event(
            &tenant.hash(),
            7,
            &principal,
            "effect-handle",
            2,
            ReconcileAction::Retry,
        );
        let attr = |key: &str| {
            event
                .attrs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .expect("attr present")
        };
        assert_eq!(event.tenant, tenant.hash());
        assert_eq!(event.now_ns, 7);
        assert_eq!(attr("kind"), AttrValue::Str("agents_reconcile".into()));
        assert_eq!(
            attr("agents.tenant"),
            AttrValue::Str(tenant.hash().to_hex())
        );
        assert_eq!(
            attr("agents.principal.tenant"),
            AttrValue::Str("acme".into())
        );
        assert_eq!(
            attr("agents.principal.capabilities"),
            AttrValue::Str("agents_admin".into())
        );
        assert_eq!(
            attr("agents.effect"),
            AttrValue::Str("effect-handle".into())
        );
        assert_eq!(attr("agents.attempt"), AttrValue::Str("2".into()));
        assert_eq!(attr("agents.decision"), AttrValue::Str("retry".into()));
        assert_eq!(
            attr("agents.principal.subject"),
            AttrValue::Str("alice".into())
        );
        assert_eq!(
            attr("agents.principal.subject_source"),
            AttrValue::Str("static_token".into())
        );
        assert_eq!(
            keys(&event)[3..6],
            [
                "agents.principal.capabilities",
                "agents.principal.subject",
                "agents.principal.subject_source"
            ]
        );
        assert_eq!(event.attrs.len(), 9);
    }

    /// Builds the event from the counts read off a HEAD, then grows that
    /// HEAD by one inbox entry: the event keeps the counts it was built from,
    /// which are the ones the destroy route read before its CAS.
    #[test]
    fn the_destroy_event_names_tenant_agent_incarnation_counts_and_caller() {
        use crate::agents::runtime::empty_head;
        use ravel_agents_core::head::{InboxEntry, InputSource};
        use ravel_agents_core::identity::Hash256;

        let tenant = TenantId::new("acme");
        let principal = admin_with_subject(&tenant);
        let entry = |accept_seq: u64| InboxEntry {
            accept_seq,
            handler: "place".to_string(),
            source: InputSource::Message {
                request_id: format!("req-{accept_seq}"),
                request_hash: Hash256([0; 32]),
            },
            payload: None,
            accepted_unix_ns: 0,
            failed_attempts: 0,
            last_failed_turn_id: None,
        };
        let mut head = empty_head();
        head.inbox.push(entry(1));
        head.inbox.push(entry(2));
        let read = AbandonedCounts::from_head(&head, head.inbox_depth());
        let abandoned = AbandonedCounts {
            effects_in_doubt: 3,
            effects_authorized: 4,
            timers_scheduled: 5,
            active_turn: true,
            ..read
        };
        let event =
            agents_destroy_audit_event(&tenant.hash(), 7, &principal, "agent-hex", 2, &abandoned);
        head.inbox.push(entry(3));
        assert_eq!(
            AbandonedCounts::from_head(&head, head.inbox_depth()).inputs,
            3
        );

        let attr = |key: &str| {
            event
                .attrs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .expect("attr present")
        };
        assert_eq!(event.attrs.len(), 13);
        assert_eq!(
            keys(&event)[3..6],
            [
                "agents.principal.capabilities",
                "agents.principal.subject",
                "agents.principal.subject_source"
            ]
        );
        assert_eq!(
            attr("agents.principal.subject"),
            AttrValue::Str("alice".into())
        );
        assert_eq!(
            attr("agents.principal.subject_source"),
            AttrValue::Str("static_token".into())
        );
        assert_eq!(event.tenant, tenant.hash());
        assert_eq!(event.now_ns, 7);
        assert_eq!(event.severity_text, "INFO");
        assert_eq!(event.body, "agents destroy");
        assert_eq!(attr("kind"), AttrValue::Str("agents_destroy".into()));
        assert_eq!(
            attr("agents.tenant"),
            AttrValue::Str(tenant.hash().to_hex())
        );
        assert_eq!(
            attr("agents.principal.tenant"),
            AttrValue::Str("acme".into())
        );
        assert_eq!(
            attr("agents.principal.capabilities"),
            AttrValue::Str("agents_admin".into())
        );
        assert_eq!(attr("agents.agent_id"), AttrValue::Str("agent-hex".into()));
        assert_eq!(attr("agents.incarnation"), AttrValue::Str("2".into()));
        assert_eq!(attr("agents.abandoned.inputs"), AttrValue::Str("2".into()));
        assert_eq!(
            attr("agents.abandoned.active_turn"),
            AttrValue::Str("true".into())
        );
        assert_eq!(
            attr("agents.abandoned.effects_in_doubt"),
            AttrValue::Str("3".into())
        );
        assert_eq!(
            attr("agents.abandoned.effects_authorized"),
            AttrValue::Str("4".into())
        );
        assert_eq!(
            attr("agents.abandoned.timers_scheduled"),
            AttrValue::Str("5".into())
        );
        let (stream_id, _) = agents_stream();
        assert_eq!(event.stream_id, stream_id, "the reconcile record's stream");
    }
}
