//! Advisory writes made after a successful HEAD publish: the `_dir` listing
//! entry and the `_wake` timer hint (ADR-3331 decisions 2 and 8).
//!
//! Neither is a source of truth. HEAD is, and a reader that finds a `_dir`
//! entry or a `_wake` hint re-reads HEAD before acting on it. So a failure
//! here is logged and the command still reports the outcome its CAS
//! published (`tests::a_failed_wake_hint_does_not_fail_the_command`).

use bytes::Bytes;
use ravel_agents_core::identity::AgentId;
use ravel_agents_core::keys::{DueBucket, dir_key, wake_key};
use ravel_object_store::{ObjectStoreBackend, PutMode, PutOptions, StoreError};
use ravel_types::TenantHash;

use crate::counters::{Counters, Phase, Request};

/// `CreateIfAbsent` the `_dir/<agent_id>` entry. Its body is empty: the key
/// alone names the agent, and `AlreadyExists` is the expected answer for
/// every incarnation after the first.
pub(crate) async fn write_dir_entry(
    store: &dyn ObjectStoreBackend,
    counters: &Counters,
    tenant: &TenantHash,
    agent: &AgentId,
) {
    let key = match dir_key(tenant, agent) {
        Ok(key) => key,
        Err(error) => {
            tracing::warn!(agent = %agent.to_hex(), %error, "agents dir key refused");
            return;
        }
    };
    counters.request(Phase::Publish, Request::Put);
    match store
        .put(&key, Bytes::new(), PutOptions::create_if_absent())
        .await
    {
        Ok(_) | Err(StoreError::AlreadyExists) => {}
        Err(error) => {
            tracing::warn!(agent = %agent.to_hex(), %error, "agents dir entry not written");
        }
    }
}

/// `Overwrite` the `_wake/<bucket>/<agent_id>` hint for a timer due at
/// `due_unix_ns`. The body is empty; the bucket in the key carries the time.
pub(crate) async fn write_wake_hint(
    store: &dyn ObjectStoreBackend,
    counters: &Counters,
    tenant: &TenantHash,
    agent: &AgentId,
    due_unix_ns: i64,
) {
    let key = match DueBucket::for_due(due_unix_ns).and_then(|b| wake_key(tenant, b, agent)) {
        Ok(key) => key,
        Err(error) => {
            tracing::warn!(agent = %agent.to_hex(), %error, "agents wake key refused");
            return;
        }
    };
    counters.request(Phase::Publish, Request::Put);
    let opts = PutOptions {
        mode: PutMode::Overwrite,
        checksum: None,
    };
    if let Err(error) = store.put(&key, Bytes::new(), opts).await {
        tracing::warn!(agent = %agent.to_hex(), %error, "agents wake hint not written");
    }
}
