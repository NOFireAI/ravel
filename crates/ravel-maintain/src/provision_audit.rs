//! Control-plane audit record for a `provision reshard` (ADR-0052 section 1,
//! ADR-0040/ADR-0042).
//!
//! A reshard changes where a (tenant, signal)'s future data lands, so it is an
//! attributable control-plane mutation and must leave an immutable audit trail,
//! exactly as a legal-hold set/clear does ([`crate::legal_hold`]). This writes
//! one immutable `Signal::Audit` RLOG record through the shared audit-write
//! mechanics ([`crate::audit_write`]), the same object/commit path every other
//! audit kind uses; it invents no new audit mechanism.

use ravel_logseg::{AttrValue, LogStreamId, stream_attrs_bytes};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

use crate::audit_write::{AuditWrite, write_audit_object};
use crate::error::Result;
use crate::legal_hold::AUDIT_HOLD_SHARD;

/// `attrs` key marking an audit record's kind (shared convention with
/// [`crate::legal_hold`]).
const ATTR_KIND: &str = "kind";
/// `attrs[ATTR_KIND]` value identifying a reshard audit record.
const KIND_RESHARD: &str = "reshard";
const ATTR_SIGNAL: &str = "reshard.signal";
const ATTR_FROM_COUNT: &str = "reshard.from_shard_count";
const ATTR_TO_COUNT: &str = "reshard.to_shard_count";
const ATTR_GENERATION: &str = "reshard.generation";
const ATTR_ACTIVATION_HOUR: &str = "reshard.activation_hour";

/// Resource-attr `record_type` value for the shared reshard audit log stream.
const STREAM_RECORD_TYPE: &str = "reshard";
const STREAM_SCOPE_NAME: &str = "ravel.reshard";
const STREAM_SCOPE_VERSION: &str = "1";

/// Write an immutable audit record for a reshard append (ADR-0052 section 1).
/// `record_id` is a caller-supplied unique object identity (a fresh `Uuid`),
/// keeping this library function free of hidden nondeterminism. Never mutates
/// any prior record.
#[allow(clippy::too_many_arguments)]
pub async fn write_reshard_audit(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    record_id: Uuid,
    now_ns: i64,
    signal: &str,
    from_shard_count: u32,
    to_shard_count: u32,
    generation: u32,
    activation_hour: u32,
) -> Result<()> {
    let attrs = vec![
        (ATTR_KIND.to_string(), AttrValue::Str(KIND_RESHARD.into())),
        (ATTR_SIGNAL.to_string(), AttrValue::Str(signal.into())),
        (
            ATTR_FROM_COUNT.to_string(),
            AttrValue::I64(i64::from(from_shard_count)),
        ),
        (
            ATTR_TO_COUNT.to_string(),
            AttrValue::I64(i64::from(to_shard_count)),
        ),
        (
            ATTR_GENERATION.to_string(),
            AttrValue::I64(i64::from(generation)),
        ),
        (
            ATTR_ACTIVATION_HOUR.to_string(),
            AttrValue::I64(i64::from(activation_hour)),
        ),
    ];
    let body = format!(
        "reshard signal={signal} shard_count {from_shard_count} -> {to_shard_count} \
         (generation {generation}, activation_hour {activation_hour})"
    );
    let (stream_id, stream_attrs) = reshard_stream();
    write_audit_object(
        store,
        tenant,
        AuditWrite {
            shard: AUDIT_HOLD_SHARD,
            record_id,
            now_ns,
            stream_id,
            stream_attrs,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body,
            attrs,
        },
    )
    .await
}

/// The shared reshard audit log stream's id and canonical resource+scope blob.
/// The id is the true hash of the blob (no placeholder), so the object records
/// real stream identity.
fn reshard_stream() -> (LogStreamId, Vec<u8>) {
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
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, list_all};
    use std::sync::Arc;

    /// A reshard audit write lands one immutable Audit object plus its commit
    /// record under the control-plane audit shard, proving the shared
    /// audit-write path is exercised (not a new mechanism).
    #[tokio::test]
    async fn write_reshard_audit_lands_one_object_and_commit() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantHash([0x11u8; 16]);
        write_reshard_audit(
            store.as_ref(),
            &tenant,
            Uuid::from_u128(1),
            1_000,
            "m",
            4,
            8,
            1,
            100,
        )
        .await
        .expect("reshard audit write succeeds");

        let sig = ravel_types::Signal::Audit.key_prefix();
        let l0 = format!("t/{}/{}/l0/", tenant.to_hex(), sig);
        let objects = list_all(store.as_ref(), &l0).await.expect("list l0");
        assert_eq!(objects.len(), 1, "one audit object was written");

        let commits = format!("t/{}/{}/c/", tenant.to_hex(), sig);
        let commit_objs = list_all(store.as_ref(), &commits).await.expect("list c");
        assert_eq!(commit_objs.len(), 1, "one commit record was written");
    }
}
