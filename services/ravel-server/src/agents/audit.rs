//! The agents audit record: one event per reconcile decision that changed an
//! effect, submitted through the server's audit pipeline into the tenant's
//! audit shard beside the query-audit records.
//!
//! The kernel also appends an audit event to the effect's turn's event
//! stream, but that event names no caller (issue #3408). This one names the
//! caller as far as a [`Principal`] can: its tenant and the capabilities it
//! holds. A principal carries no subject or token name, so two admins of one
//! tenant are not told apart here either.
//!
//! The record is submitted before the reconcile changes HEAD, not after. In
//! required audit mode a refused submission refuses the reconcile, so no
//! change lands unaudited; the cost is a record for a decision whose HEAD
//! write then failed, which names an intent that did not apply. Written
//! after, a crash or a refused submission between the two would leave a
//! changed effect with no record, which is the case the audit exists for.

use ravel_logseg::{AttrValue, LogStreamId, stream_attrs_bytes};
use ravel_maintain::AuditEvent;
use ravel_tenant_resolve::Principal;
use ravel_types::TenantHash;
use ravel_types::logstream::log_stream_id;

/// `attrs["kind"]` of an agents reconcile record.
pub const KIND_AGENTS_RECONCILE: &str = "agents_reconcile";

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
    let attrs = vec![
        (
            "kind".to_string(),
            AttrValue::Str(KIND_AGENTS_RECONCILE.into()),
        ),
        ("agents.tenant".to_string(), AttrValue::Str(tenant.to_hex())),
        (
            "agents.principal.tenant".to_string(),
            AttrValue::Str(principal.tenant.as_str().into()),
        ),
        (
            "agents.principal.capabilities".to_string(),
            AttrValue::Str(capabilities(principal)),
        ),
        ("agents.effect".to_string(), AttrValue::Str(effect.into())),
        (
            "agents.attempt".to_string(),
            AttrValue::Str(attempt.to_string()),
        ),
        (
            "agents.decision".to_string(),
            AttrValue::Str(action.as_str().into()),
        ),
    ];
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
    use ravel_types::TenantId;

    #[test]
    fn the_event_names_tenant_effect_attempt_decision_and_caller() {
        let tenant = TenantId::new("acme");
        let principal = Principal {
            agents_admin: true,
            ..Principal::tenant_only(tenant.clone())
        };
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
    }
}
