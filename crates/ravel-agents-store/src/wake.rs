//! Advisory writes made after a successful HEAD publish: the `_dir` listing
//! entry and the `_wake` timer hint (ADR-3331 decisions 2 and 8).
//!
//! Neither is a source of truth. HEAD is, and a reader that finds a `_dir`
//! entry or a `_wake` hint re-reads HEAD before acting on it. So a failure
//! here is logged and the command still reports the outcome its CAS
//! published (`tests::a_failed_wake_hint_does_not_fail_the_command`).
//!
//! The listings here resume strictly after a key rather than from a page
//! token, so a bounded reader that stops mid-page can pick up where it
//! stopped on a later pass. The runtime's sweep is that reader.

use bytes::Bytes;
use ravel_agents_core::error::{AgentError, ErrorKind, SafeIds};
use ravel_agents_core::identity::AgentId;
use ravel_agents_core::keys::{DueBucket, dir_key, dir_prefix, wake_key, wake_prefix};
use ravel_object_store::{ObjectStoreBackend, PutMode, PutOptions, StoreError};
use ravel_types::TenantHash;

use crate::AgentStore;
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

/// One `_wake/<bucket>/<agent_id>` hint a listing found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeHint {
    /// The object key, which is also the cursor a later listing resumes after.
    pub key: String,
    /// The due bucket, `floor(due_unix_ns / WAKE_BUCKET_WIDTH_NS)`.
    pub bucket: u64,
    pub agent: AgentId,
}

/// One `_dir/<agent_id>` entry a listing found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    /// The object key, which is also the cursor a later listing resumes after.
    pub key: String,
    pub agent: AgentId,
}

/// One page of a listing that starts strictly after a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorPage<T> {
    pub items: Vec<T>,
    /// The last key the page listed, whether or not it parsed into an item,
    /// so a page of malformed keys still moves the cursor forward.
    pub last_key: Option<String>,
    /// Whether the store reported more keys after this page.
    pub more: bool,
}

impl AgentStore {
    /// One page of the tenant's `_wake/` hints in key order, which is due
    /// order, starting strictly after `start_after`. Counted as one Resolve
    /// LIST (`tests::wake_hints_list_after_a_cursor_in_due_order`).
    pub async fn list_wake_hints(
        &self,
        start_after: Option<&str>,
    ) -> Result<CursorPage<WakeHint>, AgentError> {
        let prefix = wake_prefix(&self.tenant).map_err(|_| unlisted("wake prefix"))?;
        let (keys, more) = self.list_keys(&prefix, start_after).await?;
        let mut items = Vec::with_capacity(keys.len());
        for key in &keys {
            match parse_wake_suffix(&key[prefix.len()..]) {
                Some((bucket, agent)) => items.push(WakeHint {
                    key: key.clone(),
                    bucket,
                    agent,
                }),
                None => tracing::warn!(%key, "agents wake key is not a bucket and an agent id"),
            }
        }
        Ok(CursorPage {
            items,
            last_key: keys.last().cloned(),
            more,
        })
    }

    /// One page of the tenant's `_dir/` entries in key order, starting
    /// strictly after `start_after`. Counted as one Resolve LIST
    /// (`tests::dir_entries_list_after_a_cursor`).
    pub async fn list_dir_after(
        &self,
        start_after: Option<&str>,
    ) -> Result<CursorPage<DirEntry>, AgentError> {
        let prefix = dir_prefix(&self.tenant).map_err(|_| unlisted("dir prefix"))?;
        let (keys, more) = self.list_keys(&prefix, start_after).await?;
        let mut items = Vec::with_capacity(keys.len());
        for key in &keys {
            match AgentId::from_hex(&key[prefix.len()..]) {
                Some(agent) => items.push(DirEntry {
                    key: key.clone(),
                    agent,
                }),
                None => tracing::warn!(%key, "agents dir entry is not an agent id"),
            }
        }
        Ok(CursorPage {
            items,
            last_key: keys.last().cloned(),
            more,
        })
    }

    /// Delete one wake hint. The caller must have read HEAD and found no
    /// timer the hint still stands for: the hint is advisory, so deleting
    /// one is never unsafe, but deleting a needed one leaves the timer to
    /// the directory walk. Counted under [`AgentStore::wake_hint_deletes`]
    /// (`tests::a_wake_hint_delete_is_counted_and_idempotent`).
    pub async fn delete_wake_hint(&self, hint: &WakeHint) -> Result<(), AgentError> {
        let prefix = wake_prefix(&self.tenant).map_err(|_| unlisted("wake prefix"))?;
        if !hint.key.starts_with(&prefix) {
            return Err(unlisted("wake hint key outside this tenant"));
        }
        self.counters.delete();
        self.store.delete(&hint.key).await.map_err(|error| {
            tracing::warn!(key = %hint.key, %error, "agents wake hint not deleted");
            unlisted("wake hint could not be deleted")
        })
    }

    /// DELETE requests this store issued for wake hints, every attempt
    /// counted whatever it returned. Kept beside the phase counters rather
    /// than in them, because [`crate::counters::PhaseCounts`] has no delete
    /// field.
    pub fn wake_hint_deletes(&self) -> u64 {
        self.counters.deletes()
    }

    async fn list_keys(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<(Vec<String>, bool), AgentError> {
        self.counters.request(Phase::Resolve, Request::List);
        let listed = self
            .store
            .list_after(prefix, start_after, None)
            .await
            .map_err(|error| {
                tracing::warn!(%error, %prefix, "agents listing failed");
                unlisted("agents listing failed")
            })?;
        let keys = listed.objects.into_iter().map(|m| m.key).collect();
        Ok((keys, listed.next.is_some()))
    }
}

/// `<20 digits>/<32 hex>`: the part of a wake key after the tenant's prefix.
fn parse_wake_suffix(suffix: &str) -> Option<(u64, AgentId)> {
    let (bucket, agent) = suffix.split_once('/')?;
    if bucket.len() != 20 || !bucket.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((bucket.parse().ok()?, AgentId::from_hex(agent)?))
}

fn unlisted(reason: &'static str) -> AgentError {
    AgentError::new(ErrorKind::StoreUnavailable, SafeIds::default(), reason)
}
