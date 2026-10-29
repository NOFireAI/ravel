//! Ravel Agents publication skeleton (ADR-3331 decision 4): the durable
//! operations over [`ObjectStoreBackend`] that the pure kernel in
//! `ravel-agents-core` describes.
//!
//! [`AgentStore`] reads an agent's HEAD, runs one [`Command`] through the
//! kernel, uploads the immutable objects the new root references, and
//! publishes the root with one conditional write. Every operation that
//! writes HEAD goes through the one private skeleton in [`publish`]; read
//! paths ([`AgentStore::read_head_shallow`], [`AgentStore::attach`],
//! [`AgentStore::read_head_full`], [`AgentStore::resolve`],
//! [`AgentStore::list_agents`], [`AgentStore::list_wake_hints`],
//! [`AgentStore::list_dir_after`]) never write, and the one delete,
//! [`AgentStore::delete_wake_hint`], removes only an advisory hint. Time
//! and ids are injected
//! ([`Clock`], [`IdSource`]), and every store call is counted by phase in
//! [`counters`].
//!
//! The runtime crate (T9, #3343) drives this store, and `ravel-server`
//! builds one per tenant for its agents routes
//! (`services/ravel-server/src/agents/runtime.rs`, T11 #3345).

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
use ravel_agents_core::head::{
    EventEntry, Head, Lifecycle, ObjectRef, Receipt, TimerEntry, TimerStatus,
};
use ravel_agents_core::identity::{AgentId, Hash256, agent_id, request_hash};
use ravel_agents_core::kernel::{DerivedIds, Effect, IdSource, Kernel, Limits, Reply};
use ravel_agents_core::keys::{AgentKey, AgentKind, dir_prefix, object_key};
use ravel_agents_core::pages::{ListKind, Need, receipt_fingerprint};
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
    /// Whether a publish loads the pages `Kernel::pages_needed` names before
    /// the transition. Always true outside this crate's tests.
    pub(crate) plan_pages: bool,
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
            plan_pages: true,
        }
    }

    /// A store that loads no page before a transition, so any command that
    /// reads a sealed page takes the planner-miss path.
    #[cfg(test)]
    pub(crate) fn without_page_plan(mut self) -> AgentStore {
        self.plan_pages = false;
        self
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

    /// The full read: HEAD with every sealed index page attached, so each
    /// list is whole, at one GET for HEAD plus one per sealed page, a cost
    /// that grows with the agent's history. It is for the inspector, the
    /// CLI and tests. Every hot path reads [`Self::read_head_shallow`] and
    /// attaches only the pages it reads ([`Self::attach`]), or uses one of
    /// the narrower reads below ([`Self::find_receipt`],
    /// [`Self::read_events`], [`Self::due_timers`], [`Self::resolve`]).
    pub async fn read_head_full(&self, agent: &AgentId) -> Result<HeadState, AgentError> {
        let mut state = self.read_head_state(agent, true).await?;
        if let HeadState::Present { head, .. } = &mut state {
            self.load_all_pages(agent, head).await?;
        }
        Ok(state)
    }

    /// HEAD as stored, in one GET: each list holds its open page only. What
    /// is answerable from it without a page is listed on
    /// [`ravel_agents_core::pages`]: the scheduled timers, the active turn,
    /// whether the inbox is empty, and every singleton field.
    pub async fn read_head_shallow(&self, agent: &AgentId) -> Result<HeadState, AgentError> {
        self.read_head_state(agent, true).await
    }

    /// Attach to `head` every sealed page `needs` names that it does not
    /// hold yet, one GET each. Pages are immutable, so `head` may be a root
    /// read earlier, as long as it is the root the caller means to read.
    pub async fn attach(
        &self,
        agent: &AgentId,
        head: &mut Head,
        needs: &[(ListKind, Need)],
    ) -> Result<(), AgentError> {
        let mut pages = Vec::new();
        for &(list, need) in needs {
            for page in head.index.directory(list).missing(need) {
                if !pages.contains(&(list, page)) {
                    pages.push((list, page));
                }
            }
        }
        self.load_pages(agent, head, &pages).await
    }

    /// HEAD as stored, sealed pages not attached; `None` when the agent does
    /// not exist.
    async fn shallow_head(&self, agent: &AgentId) -> Result<Option<Head>, AgentError> {
        match self.read_head_state(agent, true).await? {
            HeadState::Present { head, .. } => Ok(Some(*head)),
            HeadState::Absent => Ok(None),
            HeadState::Unsupported { .. } => Err(AgentError::new(
                ErrorKind::UnsupportedFormat,
                ids_for(agent),
                "HEAD format version is newer than this release",
            )),
        }
    }

    /// The receipt `(handler, request_id)` was accepted under, if any. Reads
    /// HEAD and the sealed receipt pages whose fingerprint set lists the
    /// pair's fingerprint: at most one page unless two pages share a 32-bit
    /// fingerprint
    /// (`tests::send_receipt_lookup_and_wake_read_at_most_one_index_page_each`).
    pub async fn find_receipt(
        &self,
        agent: &AgentId,
        handler: &str,
        request_id: &str,
    ) -> Result<Option<Receipt>, AgentError> {
        let Some(mut head) = self.shallow_head(agent).await? else {
            return Ok(None);
        };
        let need = Need::Fingerprint(receipt_fingerprint(handler, request_id));
        let pages: Vec<_> = head
            .index
            .receipts
            .missing(need)
            .into_iter()
            .map(|page| (ListKind::Receipts, page))
            .collect();
        self.load_pages(agent, &mut head, &pages).await?;
        Ok(head
            .receipts
            .into_iter()
            .find(|r| r.handler == handler && r.request_id == request_id))
    }

    /// Up to `limit` events with `event_seq > after`, in order. Reads HEAD
    /// and the sealed event pages from the one holding `after + 1` onward,
    /// stopping once `limit` entries are in hand: `ceil(limit /
    /// max_page_entries) + 1` pages at most, whatever the history's length
    /// (`tests::events_are_read_by_seq_range`).
    pub async fn read_events(
        &self,
        agent: &AgentId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<EventEntry>, AgentError> {
        let Some(head) = self.shallow_head(agent).await? else {
            return Ok(Vec::new());
        };
        self.events_in(agent, head, after, limit, |_| true).await
    }

    /// Up to `limit` events with `event_seq > after` that `keep` accepts, in
    /// order, over a root the caller already read, so a caller that checks
    /// the root first (an HTTP route refusing a tombstoned agent) pays no
    /// second HEAD GET. With `keep` accepting every event the page bound is
    /// [`Self::read_events`]'s; a narrower `keep` reads on past the events
    /// it skips, up to every page after `after`.
    pub async fn events_in(
        &self,
        agent: &AgentId,
        mut head: Head,
        after: u64,
        limit: usize,
        keep: impl Fn(&EventEntry) -> bool,
    ) -> Result<Vec<EventEntry>, AgentError> {
        let from = after.saturating_add(1);
        let directory = &head.index.events;
        if let Some(first) = directory.page_for(from) {
            let count = directory.pages.len();
            for page in first..count {
                // Only events below this page's range come before it; the
                // open page's are newer than every sealed one.
                let bound = head
                    .index
                    .events
                    .pages
                    .get(page)
                    .map_or(u64::MAX, |p| p.first_key);
                let held = head
                    .events
                    .iter()
                    .filter(|e| e.event_seq > after && e.event_seq < bound && keep(e))
                    .count();
                if held >= limit {
                    break;
                }
                self.load_pages(agent, &mut head, &[(ListKind::Events, page)])
                    .await?;
            }
        }
        let mut events: Vec<EventEntry> = head
            .events
            .into_iter()
            .filter(|e| e.event_seq > after && keep(e))
            .collect();
        events.sort_by_key(|e| e.event_seq);
        events.truncate(limit);
        Ok(events)
    }

    /// The scheduled timers due at `now`, earliest first. A sealed timer page
    /// never holds a scheduled timer, so this reads HEAD and no page
    /// (`tests::send_receipt_lookup_and_wake_read_at_most_one_index_page_each`).
    pub async fn due_timers(
        &self,
        agent: &AgentId,
        now: i64,
    ) -> Result<Vec<TimerEntry>, AgentError> {
        let Some(head) = self.shallow_head(agent).await? else {
            return Ok(Vec::new());
        };
        let mut due: Vec<TimerEntry> = head
            .timers
            .into_iter()
            .filter(|t| t.status == TimerStatus::Scheduled && t.due_unix_ns <= now)
            .collect();
        due.sort_by_key(|t| (t.due_unix_ns, t.generation));
        Ok(due)
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
        let mut head = match self.read_head_state(&agent, true).await? {
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
        // The operation's entry in each list is on the page its key falls
        // in: at most one page per list.
        let pages: Vec<_> = [ListKind::Receipts, ListKind::Results, ListKind::Inbox]
            .into_iter()
            .flat_map(|list| {
                head.index
                    .directory(list)
                    .missing(Need::Key(accept_seq))
                    .into_iter()
                    .map(move |page| (list, page))
            })
            .collect();
        self.load_pages(&agent, &mut head, &pages).await?;
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
