//! Ravel Agents publication skeleton (ADR-3331 decision 4): the durable
//! operations over [`ObjectStoreBackend`] that the pure kernel in
//! `ravel-agents-core` describes.
//!
//! [`AgentStore`] reads an agent's HEAD, runs one [`Command`] through the
//! kernel, uploads the immutable objects the new root references, and
//! publishes the root with one conditional write. Every operation that
//! writes HEAD goes through the one private skeleton in [`publish`]; read
//! paths ([`AgentStore::read_head`], [`AgentStore::resolve`],
//! [`AgentStore::list_agents`], [`AgentStore::list_wake_hints`],
//! [`AgentStore::list_dir_after`]) never write, and the one delete,
//! [`AgentStore::delete_wake_hint`], removes only an advisory hint. Time
//! and ids are injected
//! ([`Clock`], [`IdSource`]), and every store call is counted by phase in
//! [`counters`].
//!
//! The runtime crate (T9, #3343) drives this store; nothing in ravel-server
//! reaches it until T11.

pub mod backoff;
pub mod counters;
pub mod publish;
mod wake;

pub use wake::{CursorPage, DirEntry, WakeHint};

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ravel_agents_core::command::{Command, Create, Enqueue, IncarnationFence};
use ravel_agents_core::error::{AgentError, ErrorKind, SafeIds};
use ravel_agents_core::head::{Head, Lifecycle, ObjectRef, Receipt};
use ravel_agents_core::identity::{AgentId, Hash256, agent_id, request_hash};
use ravel_agents_core::kernel::{DerivedIds, Effect, IdSource, Kernel, Limits, Reply};
use ravel_agents_core::keys::{AgentKey, AgentKind, dir_prefix, object_key};
use ravel_agents_core::state::{Handle, HandleItem, OperationState, operation_state};
use ravel_object_store::{ObjectStoreBackend, PageToken, StoreError, Version};
use ravel_types::TenantHash;

use crate::backoff::{Backoff, JitteredBackoff};
use crate::counters::{CounterSnapshot, Counters, Phase, Request};
use crate::publish::{ids_for, reply_handle, unavailable};

/// Wall-clock seam. Library logic reads time only through this.
pub trait Clock: Send + Sync {
    fn now_unix_ns(&self) -> i64;
}

/// The production clock: the host's wall clock, saturating at the ends of
/// the `i64` nanosecond range.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ns(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
    }
}

/// The kernel's limits plus the one the store enforces itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreLimits {
    pub kernel: Limits,
    /// Largest immutable object (payload, state, output) one command may
    /// reference. Checked on supplied bytes before any store call, and on
    /// every new reference before any write.
    pub max_object_bytes: usize,
}

/// Default per-object cap: 4 MiB.
pub const DEFAULT_MAX_OBJECT_BYTES: usize = 4 * 1024 * 1024;

impl Default for StoreLimits {
    fn default() -> Self {
        StoreLimits {
            kernel: Limits::default(),
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
        }
    }
}

/// What a HEAD read found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeadState {
    Absent,
    /// A root this release reads, with the version a CAS must name.
    Present {
        head: Box<Head>,
        version: Version,
    },
    /// A root written by a newer release. It is never decoded past its
    /// format version and never CAS-written by this one
    /// (`tests::a_newer_head_format_is_unsupported_and_never_written`).
    Unsupported {
        found: u32,
    },
}

/// The result of one command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub agent: AgentId,
    pub reply: Reply,
    /// The root after the command: the one published, or the one read when
    /// the kernel left it unchanged.
    pub head: Head,
    pub version: Version,
    /// Whether this call's CAS published `head`.
    pub published: bool,
    /// Effects for the runtime. The `_dir` and `_wake` writes among them
    /// were already attempted.
    pub effects: Vec<Effect>,
}

impl Outcome {
    /// The operation handle of an accepted input.
    pub fn handle(&self) -> Option<Handle> {
        reply_handle(&self.agent, &self.head, &self.reply)
    }
}

/// What [`AgentStore::resolve`] found for an operation handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    pub handle: Handle,
    pub accept_seq: u64,
    /// `None` when the current root has no input under this `accept_seq`.
    pub state: Option<OperationState>,
    /// The receipt recorded under this `accept_seq`. After `OutcomeUnknown`
    /// it says whether the input accepted there is the caller's own; see
    /// [`Resolution::is_request`].
    pub receipt: Option<Receipt>,
    pub root_revision: u64,
}

impl Resolution {
    /// Whether the input at this handle is the request named, by id,
    /// handler AND request hash (ADR-3331 decision 4 step 2), so a handle
    /// predicted for a write that never applied is not mistaken for a later
    /// writer accepted there under the same id with a different payload
    /// (`tests::a_lost_write_is_not_mistaken_for_a_later_writer_with_the_same_id`).
    pub fn is_request(&self, request_id: &str, handler: &str, request_hash: &Hash256) -> bool {
        self.receipt.as_ref().is_some_and(|r| {
            r.request_id == request_id && r.handler == handler && r.request_hash == *request_hash
        })
    }
}

/// One page of `_dir` entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPage {
    pub agents: Vec<AgentId>,
    pub next: Option<PageToken>,
}

/// The publication skeleton over one tenant's agents.
pub struct AgentStore {
    pub(crate) store: Arc<dyn ObjectStoreBackend>,
    pub(crate) tenant: TenantHash,
    pub(crate) kernel: Kernel,
    pub(crate) limits: StoreLimits,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) ids: Arc<dyn IdSource + Send + Sync>,
    pub(crate) counters: Arc<Counters>,
    pub(crate) backoff: Arc<dyn Backoff>,
}

impl AgentStore {
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        tenant: TenantHash,
        limits: StoreLimits,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource + Send + Sync>,
    ) -> AgentStore {
        AgentStore {
            store,
            tenant,
            kernel: Kernel::new(limits.kernel),
            limits,
            clock,
            ids,
            counters: Arc::new(Counters::default()),
            backoff: Arc::new(JitteredBackoff::default()),
        }
    }

    /// Replace the retry delay policy ([`JitteredBackoff`] by default).
    #[must_use]
    pub fn with_backoff(mut self, backoff: Arc<dyn Backoff>) -> AgentStore {
        self.backoff = backoff;
        self
    }

    /// Wait before the attempt after `attempt`, for the delay the policy
    /// gives the failure that ended it. A refused conditional write passes
    /// no error.
    pub(crate) async fn pause(&self, attempt: u32, error: Option<&StoreError>) {
        let retry_after_ms = match error {
            Some(StoreError::Throttled { retry_after_ms }) => Some(*retry_after_ms),
            _ => None,
        };
        let delay = self.backoff.delay(attempt, retry_after_ms);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }

    /// [`AgentStore::new`] with [`SystemClock`] and [`DerivedIds`].
    pub fn with_system_clock(
        store: Arc<dyn ObjectStoreBackend>,
        tenant: TenantHash,
        limits: StoreLimits,
    ) -> AgentStore {
        AgentStore::new(
            store,
            tenant,
            limits,
            Arc::new(SystemClock),
            Arc::new(DerivedIds),
        )
    }

    pub fn tenant(&self) -> &TenantHash {
        &self.tenant
    }

    pub fn counters(&self) -> CounterSnapshot {
        self.counters.snapshot()
    }

    /// Create the agent, or return the existing active root. A create that
    /// finds the root absent and then loses the `CreateIfAbsent` race
    /// re-reads and returns the root that won
    /// (`tests::create_is_idempotent_through_already_exists`); one on a
    /// tombstoned root starts the next incarnation
    /// (`tests::create_on_a_tombstoned_agent_bumps_the_incarnation`).
    pub async fn create(&self, kind: AgentKind, key: AgentKey) -> Result<Outcome, AgentError> {
        let agent = agent_id(&kind, &key);
        let command = Command::Create(Create {
            tenant: self.tenant,
            kind,
            key,
        });
        self.publish(&agent, &command, &[]).await
    }

    /// Run one command against `agent`. Objects the command references must
    /// already be stored; see [`AgentStore::execute_with`].
    pub async fn execute(&self, agent: &AgentId, command: &Command) -> Result<Outcome, AgentError> {
        self.execute_with(agent, command, &[]).await
    }

    /// Run one command, uploading the referenced objects found in `deps`
    /// (matched by content hash) before the HEAD write. A `Create` must name
    /// this store's tenant and `agent`.
    pub async fn execute_with(
        &self,
        agent: &AgentId,
        command: &Command,
        deps: &[Bytes],
    ) -> Result<Outcome, AgentError> {
        if let Command::Create(c) = command
            && (c.tenant != self.tenant || agent_id(&c.kind, &c.key) != *agent)
        {
            return Err(AgentError::new(
                ErrorKind::AgentGone,
                ids_for(agent),
                "create names a different agent",
            ));
        }
        self.publish(agent, command, deps).await
    }

    /// Enqueue `payload` for `handler` under `request_id`: the request hash
    /// and payload reference are derived here, and the payload is uploaded
    /// with the command.
    pub async fn enqueue(
        &self,
        agent: &AgentId,
        incarnation: u64,
        request_id: &str,
        handler: &str,
        payload: Bytes,
    ) -> Result<Outcome, AgentError> {
        let command = Command::Enqueue(Enqueue {
            fence: IncarnationFence { incarnation },
            request_id: request_id.to_string(),
            handler: handler.to_string(),
            request_hash: request_hash(handler, &payload),
            payload: Some(ObjectRef::of(&payload)),
        });
        self.publish(agent, &command, &[payload]).await
    }

    /// Store an immutable object ahead of the command that will reference
    /// it (a worker's new state, a step output).
    pub async fn put_object(&self, agent: &AgentId, bytes: Bytes) -> Result<ObjectRef, AgentError> {
        let dep = ObjectRef::of(&bytes);
        if bytes.len() > self.limits.max_object_bytes {
            return Err(AgentError::new(
                ErrorKind::BudgetExceeded,
                ids_for(agent),
                "object is larger than the per-object byte limit",
            ));
        }
        let key = object_key(&self.tenant, agent, &dep.hash)
            .map_err(|_| unavailable(agent, "object key is not addressable"))?;
        self.put_object_at(agent, &key, dep, bytes).await?;
        Ok(dep)
    }

    /// Read an object a root references, refusing bytes that do not hash to
    /// the reference (`tests::a_corrupt_dependency_read_is_a_typed_error`).
    pub async fn read_object(&self, agent: &AgentId, dep: ObjectRef) -> Result<Bytes, AgentError> {
        let key = object_key(&self.tenant, agent, &dep.hash)
            .map_err(|_| unavailable(agent, "object key is not addressable"))?;
        self.read_object_at(agent, &key, dep).await
    }

    /// Read HEAD without writing anything.
    pub async fn read_head(&self, agent: &AgentId) -> Result<HeadState, AgentError> {
        self.read_head_state(agent, true).await
    }

    /// Re-read HEAD and report the operation an operation handle names,
    /// through the receipt index, which survives later root revisions. This
    /// is how a caller settles `OutcomeUnknown`: it compares
    /// [`Resolution::receipt`] with its own request.
    pub async fn resolve(&self, handle: &Handle) -> Result<Resolution, AgentError> {
        let agent = handle.agent;
        let HandleItem::Operation(accept_seq) = handle.item else {
            return Err(AgentError::new(
                ErrorKind::AgentGone,
                ids_for(&agent),
                "handle does not name an operation",
            ));
        };
        let head = match self.read_head_state(&agent, true).await? {
            HeadState::Present { head, .. } => *head,
            // A HEAD is never deleted (ADR-3331 decision 3), so a handle
            // proves the agent existed; a read that finds nothing is a read
            // that failed, and the caller keeps its handle.
            HeadState::Absent => {
                return Err(AgentError::new(
                    ErrorKind::OutcomeUnknown,
                    SafeIds {
                        operation: Some(handle.encode()),
                        ..ids_for(&agent)
                    },
                    "HEAD could not be read; resolve the operation again",
                ));
            }
            HeadState::Unsupported { .. } => {
                return Err(AgentError::new(
                    ErrorKind::UnsupportedFormat,
                    ids_for(&agent),
                    "HEAD format version is newer than this release",
                ));
            }
        };
        if head.incarnation != handle.incarnation || head.lifecycle == Lifecycle::Tombstoned {
            return Err(AgentError::new(
                ErrorKind::AgentGone,
                ids_for(&agent),
                "handle names an incarnation that is not current",
            ));
        }
        Ok(Resolution {
            handle: *handle,
            accept_seq,
            state: operation_state(&head, accept_seq),
            receipt: head
                .receipts
                .iter()
                .find(|r| r.accept_seq == accept_seq)
                .cloned(),
            root_revision: head.root_revision,
        })
    }

    /// One page of the tenant's `_dir` entries: one LIST call, bounded by the
    /// backend's page size. Pass the returned `next` to continue.
    pub async fn list_agents(&self, page: Option<PageToken>) -> Result<AgentPage, AgentError> {
        let prefix = dir_prefix(&self.tenant).map_err(|_| {
            AgentError::new(
                ErrorKind::StoreUnavailable,
                Default::default(),
                "tenant prefix is not addressable",
            )
        })?;
        self.counters.request(Phase::Resolve, Request::List);
        let listed = self.store.list(&prefix, page).await.map_err(|error| {
            tracing::warn!(%error, "agents dir listing failed");
            AgentError::new(
                ErrorKind::StoreUnavailable,
                Default::default(),
                "agent directory could not be listed",
            )
        })?;
        let mut agents = Vec::with_capacity(listed.objects.len());
        for meta in &listed.objects {
            match meta.key.strip_prefix(&prefix).and_then(AgentId::from_hex) {
                Some(agent) => agents.push(agent),
                None => tracing::warn!(key = %meta.key, "agents dir entry is not an agent id"),
            }
        }
        Ok(AgentPage {
            agents,
            next: listed.next,
        })
    }
}
