//! Projection of committed agent history into logs and traces (ADR-3331
//! decision 12).
//!
//! A [`Projector`] reads the events a tenant's agents committed and hands
//! them to a [`ProjectionSink`] as one log record per event and one span per
//! turn. It is a reader only: every store call it makes is a GET
//! ([`AgentStore::read_head_shallow`], [`AgentStore::events_in`],
//! [`AgentStore::read_object`]), so a sink outage shows as lag and a failure
//! counter and never as a change to an agent
//! (`tests::a_failing_sink_changes_no_agent_state_and_retries_the_same_ids`).
//!
//! The cursor is in memory, one per agent, and starts at the agent's first
//! event when the coordinator first reports the agent. ADR-3331 does not say
//! where a projection cursor persists, and persisting it in HEAD would make
//! projection a writer, so a restarted process projects each agent's
//! history again as it touches the agent: nothing committed while no
//! projector ran is lost, and the repeats carry the ids they carried before
//! (`tests::a_restarted_projector_redelivers_history_under_the_same_ids`).
//!
//! Every record carries a stable event id derived from the tenant, the
//! agent, its incarnation and the event seq ([`event_id`]), so a record
//! delivered twice (a retry, a restart) carries the same id twice and the
//! id is the deduplication key for queries.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use ravel_agents_core::error::AgentError;
use ravel_agents_core::head::{EventEntry, Head};
use ravel_agents_core::identity::{AgentId, DOMAIN, Id128};
use ravel_agents_store::{AgentStore, Clock, HeadState};
use ravel_types::TenantHash;
use tokio::sync::Notify;

/// Bounds on what a projector holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionLimits {
    /// Records read from the store and not yet delivered. A full backlog
    /// stops the projector reading; it never stops the coordinator.
    pub backlog: usize,
    /// Records handed to the sink in one delivery.
    pub batch: usize,
}

/// The default backlog bound.
pub const DEFAULT_PROJECTION_BACKLOG: usize = 4096;
/// The default delivery batch.
pub const DEFAULT_PROJECTION_BATCH: usize = 256;

impl Default for ProjectionLimits {
    fn default() -> Self {
        ProjectionLimits {
            backlog: DEFAULT_PROJECTION_BACKLOG,
            batch: DEFAULT_PROJECTION_BATCH,
        }
    }
}

/// Whether a record carries the event's payload bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadPolicy {
    /// The default: the payload's hash and size, never its bytes.
    Redacted,
    /// The tenant opted in: the payload bytes as text, beside hash and size.
    Carried,
}

/// What a record says about an event's payload. Built only by
/// [`Projector::payload_view`], the one redaction gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedPayload {
    /// blake3 of the payload bytes, hex; `None` for an event with no payload.
    pub hash: Option<String>,
    pub size_bytes: u64,
    /// The payload as UTF-8 text (lossy), only under [`PayloadPolicy::Carried`].
    pub body: Option<String>,
}

/// One committed event, as a log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedEvent {
    pub event_id: Id128,
    pub agent: AgentId,
    pub incarnation: u64,
    pub event_seq: u64,
    pub accept_seq: u64,
    pub turn_id: Id128,
    pub event_type: String,
    pub payload: ProjectedPayload,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    /// The record's time: its turn's finish time when the projector found it
    /// committed, else the read time. Never the delivery time.
    pub time_unix_ns: i64,
    pub time_source: TimeSource,
}

/// Where a record's time came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSource {
    /// The turn's committed `finished_unix_ns`, from the open page of the
    /// agent's results.
    TurnFinished,
    /// The projector's clock when it read the event: the turn's result had
    /// left the open results page, and the projector reads no sealed page.
    Read,
}

impl TimeSource {
    pub fn as_str(self) -> &'static str {
        match self {
            TimeSource::TurnFinished => "turn_finished",
            TimeSource::Read => "read",
        }
    }
}

/// A record body cut to fit a sink's body limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FittedBody {
    pub text: String,
    /// The carried payload text was cut short to fit.
    pub truncated: bool,
}

impl ProjectedEvent {
    /// The record body every sink writes, so the event id and the redacted
    /// payload are searchable as log text.
    pub fn body(&self) -> String {
        self.body_within(usize::MAX).text
    }

    /// [`Self::body`] in at most `max_bytes`: carried payload text is cut at a
    /// char boundary and the body says `payload_truncated=true` before it. A
    /// body with no carried text is returned whole whatever its length.
    pub fn body_within(&self, max_bytes: usize) -> FittedBody {
        let head = self.body_head();
        let Some(text) = &self.payload.body else {
            return FittedBody {
                text: head,
                truncated: false,
            };
        };
        let whole = head.len() + " payload=".len() + text.len();
        if whole <= max_bytes {
            return FittedBody {
                text: format!("{head} payload={text}"),
                truncated: false,
            };
        }
        let prefix = format!("{head} payload_truncated=true payload=");
        let mut keep = max_bytes.saturating_sub(prefix.len()).min(text.len());
        while !text.is_char_boundary(keep) {
            keep -= 1;
        }
        FittedBody {
            text: format!("{prefix}{}", &text[..keep]),
            truncated: true,
        }
    }

    fn body_head(&self) -> String {
        let mut out = format!(
            "agent event event_id={} agent_id={} incarnation={} event_seq={} accept_seq={} turn_id={} type={}",
            self.event_id.to_hex(),
            self.agent.to_hex(),
            self.incarnation,
            self.event_seq,
            self.accept_seq,
            self.turn_id.to_hex(),
            self.event_type,
        );
        match &self.payload.hash {
            None => out.push_str(" payload=none"),
            Some(hash) => {
                out.push_str(&format!(
                    " payload_hash={hash} payload_size={}",
                    self.payload.size_bytes
                ));
                if self.payload.body.is_none() {
                    out.push_str(" payload=redacted");
                }
            }
        }
        out
    }
}

/// One turn, as a span: the events of the turn the batch holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedTurn {
    pub agent: AgentId,
    pub incarnation: u64,
    pub accept_seq: u64,
    pub turn_id: Id128,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub first_event_seq: u64,
    pub last_event_seq: u64,
    pub start_unix_ns: i64,
    pub end_unix_ns: i64,
}

/// What one delivery hands the sink.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectionBatch {
    pub events: Vec<ProjectedEvent>,
    pub turns: Vec<ProjectedTurn>,
}

/// A sink refused or failed a delivery for a reason that can pass on a later
/// attempt (admission, a rate, the active-stream cap, a buffer budget, the
/// store, the network); the batch is retried whole with the same ids.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("projection sink failed: {0}")]
pub struct SinkError(pub String);

/// A delivery the sink finished. The records it rejected for a reason that
/// fails identically on every retry (too old, too long, malformed) are not
/// written and never retried; the rest are written.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Delivered {
    pub rejected_events: u64,
    pub rejected_spans: u64,
    /// The last rejection's reason, when anything was rejected.
    pub rejection: Option<String>,
}

pub type SinkFuture<'a> = Pin<Box<dyn Future<Output = Result<Delivered, SinkError>> + Send + 'a>>;

/// Where projected records go: the process's own ingest routers, an OTLP
/// endpoint, or a test double. A sink returns [`SinkError`] only for a
/// refusal a retry can get past; one that fails the same way every time is a
/// rejection in [`Delivered`], or the projector would retry it forever.
pub trait ProjectionSink: Send + Sync {
    fn deliver<'a>(&'a self, batch: &'a ProjectionBatch) -> SinkFuture<'a>;
}

/// The stable event id: `blake3(DOMAIN, "projection-event", tenant, agent,
/// incarnation, event_seq)[..16]`, every component length-prefixed.
pub fn event_id(tenant: &TenantHash, agent: &AgentId, incarnation: u64, event_seq: u64) -> Id128 {
    Id128(derive_16(
        "projection-event",
        &[
            &tenant.0,
            &agent.0,
            &incarnation.to_le_bytes(),
            &event_seq.to_le_bytes(),
        ],
    ))
}

/// The trace of one input: tenant, agent, incarnation and accept seq.
pub fn trace_id(
    tenant: &TenantHash,
    agent: &AgentId,
    incarnation: u64,
    accept_seq: u64,
) -> [u8; 16] {
    derive_16(
        "projection-trace",
        &[
            &tenant.0,
            &agent.0,
            &incarnation.to_le_bytes(),
            &accept_seq.to_le_bytes(),
        ],
    )
}

/// The span of one turn. A turn id is unique only within its agent, so the
/// tenant and agent are hashed in.
pub fn span_id(tenant: &TenantHash, agent: &AgentId, turn: &Id128) -> [u8; 8] {
    let full = derive_16("projection-span", &[&tenant.0, &agent.0, &turn.0]);
    let mut out = [0u8; 8];
    out.copy_from_slice(&full[..8]);
    out
}

fn derive_16(purpose: &str, parts: &[&[u8]]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    for part in std::iter::once(purpose.as_bytes()).chain(parts.iter().copied()) {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    out
}

/// A point-in-time copy of a projector's counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionMetricsSnapshot {
    /// The largest committed event seq minus projected event seq over the
    /// tenant's agents. Summed snapshots keep the maximum.
    pub lag_max: u64,
    /// Records read and not yet delivered.
    pub backlog: u64,
    /// The backlog bound.
    pub backlog_bound: u64,
    /// Log records delivered, retries of a failed batch counted once.
    pub events_projected: u64,
    /// Spans delivered.
    pub spans_projected: u64,
    /// Deliveries the sink failed.
    pub sink_failures: u64,
    /// Passes that left committed events unread because the backlog was full.
    pub backlog_full: u64,
    /// Store reads that failed; the events stay committed and are read on a
    /// later pass.
    pub read_failures: u64,
    /// Records (log records and spans) the sink rejected for good and the
    /// projector skipped.
    pub records_rejected: u64,
}

impl ProjectionMetricsSnapshot {
    /// Fold two tenants' snapshots: counters add, lag keeps the maximum.
    pub fn merge(self, other: ProjectionMetricsSnapshot) -> ProjectionMetricsSnapshot {
        ProjectionMetricsSnapshot {
            lag_max: self.lag_max.max(other.lag_max),
            backlog: self.backlog.saturating_add(other.backlog),
            backlog_bound: self.backlog_bound.saturating_add(other.backlog_bound),
            records_rejected: self.records_rejected.saturating_add(other.records_rejected),
            events_projected: self.events_projected.saturating_add(other.events_projected),
            spans_projected: self.spans_projected.saturating_add(other.spans_projected),
            sink_failures: self.sink_failures.saturating_add(other.sink_failures),
            backlog_full: self.backlog_full.saturating_add(other.backlog_full),
            read_failures: self.read_failures.saturating_add(other.read_failures),
        }
    }
}

/// One agent's projection position.
#[derive(Debug, Clone, Copy)]
struct Cursor {
    incarnation: u64,
    /// The newest committed event seq a root has shown.
    committed: u64,
    /// The newest event seq read into the backlog.
    read: u64,
    /// The newest event seq a sink accepted.
    projected: u64,
}

#[derive(Debug, Default)]
struct State {
    cursors: BTreeMap<AgentId, Cursor>,
    backlog: Vec<ProjectedEvent>,
    turns: Vec<ProjectedTurn>,
}

#[derive(Debug, Default)]
struct Counters {
    events_projected: AtomicU64,
    spans_projected: AtomicU64,
    sink_failures: AtomicU64,
    backlog_full: AtomicU64,
    read_failures: AtomicU64,
    records_rejected: AtomicU64,
}

/// The text of a projector's last sink failure and last rejection, for the
/// health route.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectionErrors {
    pub last_sink_error: Option<String>,
    pub last_rejection: Option<String>,
}

impl ProjectionErrors {
    /// Fold two tenants' texts, keeping the first present of each.
    pub fn merge(self, other: ProjectionErrors) -> ProjectionErrors {
        ProjectionErrors {
            last_sink_error: self.last_sink_error.or(other.last_sink_error),
            last_rejection: self.last_rejection.or(other.last_rejection),
        }
    }
}

/// Projects one tenant's committed history.
pub struct Projector {
    tenant: TenantHash,
    store: Arc<AgentStore>,
    sink: Arc<dyn ProjectionSink>,
    limits: ProjectionLimits,
    payload: PayloadPolicy,
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
    /// Serialises passes, so one batch is in flight at a time.
    pass: tokio::sync::Mutex<()>,
    wake: Notify,
    counters: Counters,
    errors: Mutex<ProjectionErrors>,
}

/// What one [`Projector::project_once`] pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionPass {
    pub events_delivered: u64,
    pub spans_delivered: u64,
    pub sink_failed: bool,
    pub backlog_full: bool,
}

impl Projector {
    /// `store` should be an instance of its own, so the projector's GETs
    /// never show in the coordinator's request counters.
    pub fn new(
        tenant: TenantHash,
        store: Arc<AgentStore>,
        sink: Arc<dyn ProjectionSink>,
        limits: ProjectionLimits,
        payload: PayloadPolicy,
        clock: Arc<dyn Clock>,
    ) -> Projector {
        Projector {
            tenant,
            store,
            sink,
            limits: ProjectionLimits {
                backlog: limits.backlog.max(1),
                batch: limits.batch.max(1),
            },
            payload,
            clock,
            state: Mutex::new(State::default()),
            pass: tokio::sync::Mutex::new(()),
            wake: Notify::new(),
            counters: Counters::default(),
            errors: Mutex::new(ProjectionErrors::default()),
        }
    }

    pub fn limits(&self) -> ProjectionLimits {
        self.limits
    }

    pub fn payload_policy(&self) -> PayloadPolicy {
        self.payload
    }

    /// Record a root the coordinator saw. Never blocks on the sink or the
    /// store: a map update and a wake.
    pub fn note(&self, agent: &AgentId, head: &Head) {
        let committed = head.next_event_seq.saturating_sub(1);
        let mut state = self.state.lock();
        let mut moved = false;
        let cursor = state.cursors.entry(*agent).or_insert_with(|| {
            moved = committed > 0;
            Cursor {
                incarnation: head.incarnation,
                committed,
                read: 0,
                projected: 0,
            }
        });
        moved |= if head.incarnation > cursor.incarnation {
            *cursor = Cursor {
                incarnation: head.incarnation,
                committed,
                read: 0,
                projected: 0,
            };
            committed > 0
        } else if head.incarnation == cursor.incarnation && committed > cursor.committed {
            cursor.committed = committed;
            true
        } else {
            false
        };
        drop(state);
        if moved {
            self.wake.notify_one();
        }
    }

    /// Resolves when a note has moved a cursor since the last wait.
    pub async fn notified(&self) {
        self.wake.notified().await;
    }

    pub fn metrics(&self) -> ProjectionMetricsSnapshot {
        let (lag_max, backlog) = {
            let state = self.state.lock();
            let lag = state
                .cursors
                .values()
                .map(|c| c.committed.saturating_sub(c.projected))
                .max()
                .unwrap_or(0);
            (lag, state.backlog.len() as u64)
        };
        let c = &self.counters;
        ProjectionMetricsSnapshot {
            lag_max,
            backlog,
            backlog_bound: self.limits.backlog as u64,
            events_projected: c.events_projected.load(Ordering::Relaxed),
            spans_projected: c.spans_projected.load(Ordering::Relaxed),
            sink_failures: c.sink_failures.load(Ordering::Relaxed),
            backlog_full: c.backlog_full.load(Ordering::Relaxed),
            read_failures: c.read_failures.load(Ordering::Relaxed),
            records_rejected: c.records_rejected.load(Ordering::Relaxed),
        }
    }

    /// The last sink failure's and the last rejection's text.
    pub fn errors(&self) -> ProjectionErrors {
        self.errors.lock().clone()
    }

    /// The store's request counters for the projector's own reads.
    pub fn store_counters(&self) -> ravel_agents_store::counters::CounterSnapshot {
        self.store.counters()
    }

    /// One pass: fill the backlog from every agent with unread committed
    /// events, up to the bound, then deliver it in batches. A failed
    /// delivery keeps the batch, ids and all, for the next pass.
    pub async fn project_once(&self) -> ProjectionPass {
        let _pass = self.pass.lock().await;
        let mut report = ProjectionPass::default();
        self.fill(&mut report).await;
        self.deliver(&mut report).await;
        report
    }

    async fn fill(&self, report: &mut ProjectionPass) {
        let behind: Vec<(AgentId, Cursor)> = {
            let state = self.state.lock();
            state
                .cursors
                .iter()
                .filter(|(_, c)| c.committed > c.read)
                .map(|(a, c)| (*a, *c))
                .collect()
        };
        for (agent, cursor) in behind {
            let room = self
                .limits
                .backlog
                .saturating_sub(self.state.lock().backlog.len());
            if room == 0 {
                self.counters.backlog_full.fetch_add(1, Ordering::Relaxed);
                report.backlog_full = true;
                return;
            }
            if self.read_agent(&agent, cursor, room).await.is_err() {
                self.counters.read_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn read_agent(
        &self,
        agent: &AgentId,
        cursor: Cursor,
        room: usize,
    ) -> Result<(), AgentError> {
        let head = match self.store.read_head_shallow(agent).await? {
            HeadState::Present { head, .. } => *head,
            HeadState::Absent | HeadState::Unsupported { .. } => return Ok(()),
        };
        if head.incarnation != cursor.incarnation {
            // A newer incarnation: the next note resets the cursor.
            self.note(agent, &head);
            return Ok(());
        }
        let finished: HashMap<u64, i64> = head
            .results
            .iter()
            .map(|r| (r.accept_seq, r.finished_unix_ns))
            .collect();
        let incarnation = head.incarnation;
        let events = self
            .store
            .events_in(agent, head, cursor.read, room, None)
            .await?;
        let now = self.clock.now_unix_ns();
        let mut records = Vec::with_capacity(events.len());
        let mut turns: Vec<ProjectedTurn> = Vec::new();
        for event in &events {
            let payload = self.payload_view(agent, event).await?;
            let at = match finished.get(&event.accept_seq) {
                Some(finished) => (*finished, TimeSource::TurnFinished),
                None => (now, TimeSource::Read),
            };
            let record = self.record(agent, incarnation, event, payload, at);
            match turns.last_mut() {
                Some(turn) if turn.turn_id == event.turn_id => {
                    turn.last_event_seq = event.event_seq;
                }
                _ => {
                    let at = record.time_unix_ns;
                    turns.push(ProjectedTurn {
                        agent: *agent,
                        incarnation,
                        accept_seq: event.accept_seq,
                        turn_id: event.turn_id,
                        trace_id: record.trace_id,
                        span_id: record.span_id,
                        first_event_seq: event.event_seq,
                        last_event_seq: event.event_seq,
                        start_unix_ns: at,
                        end_unix_ns: at,
                    });
                }
            }
            records.push(record);
        }
        let Some(last) = events.last().map(|e| e.event_seq) else {
            return Ok(());
        };
        let mut state = self.state.lock();
        let Some(c) = state.cursors.get_mut(agent) else {
            return Ok(());
        };
        if c.incarnation != incarnation || c.read != cursor.read {
            return Ok(());
        }
        c.read = last;
        c.committed = c.committed.max(last);
        state.backlog.extend(records);
        state.turns.extend(turns);
        Ok(())
    }

    /// The redaction gate: the only place a record's payload is built, and
    /// the only place the projector reads payload bytes.
    async fn payload_view(
        &self,
        agent: &AgentId,
        event: &EventEntry,
    ) -> Result<ProjectedPayload, AgentError> {
        let Some(object) = event.payload else {
            return Ok(ProjectedPayload {
                hash: None,
                size_bytes: 0,
                body: None,
            });
        };
        let body = match self.payload {
            PayloadPolicy::Redacted => None,
            PayloadPolicy::Carried => {
                let bytes = self.store.read_object(agent, object).await?;
                Some(String::from_utf8_lossy(&bytes).into_owned())
            }
        };
        Ok(ProjectedPayload {
            hash: Some(object.hash.to_hex()),
            size_bytes: object.size_bytes,
            body,
        })
    }

    fn record(
        &self,
        agent: &AgentId,
        incarnation: u64,
        event: &EventEntry,
        payload: ProjectedPayload,
        (time_unix_ns, time_source): (i64, TimeSource),
    ) -> ProjectedEvent {
        ProjectedEvent {
            event_id: event_id(&self.tenant, agent, incarnation, event.event_seq),
            agent: *agent,
            incarnation,
            event_seq: event.event_seq,
            accept_seq: event.accept_seq,
            turn_id: event.turn_id,
            event_type: event.event_type.clone(),
            payload,
            trace_id: trace_id(&self.tenant, agent, incarnation, event.accept_seq),
            span_id: span_id(&self.tenant, agent, &event.turn_id),
            time_unix_ns,
            time_source,
        }
    }

    async fn deliver(&self, report: &mut ProjectionPass) {
        loop {
            let batch = {
                let state = self.state.lock();
                if state.backlog.is_empty() {
                    return;
                }
                let events: Vec<ProjectedEvent> = state
                    .backlog
                    .iter()
                    .take(self.limits.batch)
                    .cloned()
                    .collect();
                let turns: Vec<ProjectedTurn> = state
                    .turns
                    .iter()
                    .filter(|t| {
                        events.iter().any(|e| {
                            e.agent == t.agent
                                && e.incarnation == t.incarnation
                                && e.event_seq == t.first_event_seq
                        })
                    })
                    .cloned()
                    .collect();
                ProjectionBatch { events, turns }
            };
            let delivered = match self.sink.deliver(&batch).await {
                Ok(delivered) => delivered,
                Err(SinkError(error)) => {
                    self.counters.sink_failures.fetch_add(1, Ordering::Relaxed);
                    self.errors.lock().last_sink_error = Some(error);
                    report.sink_failed = true;
                    return;
                }
            };
            if let Some(rejection) = delivered.rejection {
                self.errors.lock().last_rejection = Some(rejection);
            }
            let mut state = self.state.lock();
            state.backlog.drain(..batch.events.len());
            state.turns.retain(|t| !batch.turns.contains(t));
            for event in &batch.events {
                if let Some(c) = state.cursors.get_mut(&event.agent)
                    && c.incarnation == event.incarnation
                {
                    c.projected = c.projected.max(event.event_seq);
                }
            }
            drop(state);
            let events = (batch.events.len() as u64).saturating_sub(delivered.rejected_events);
            let spans = (batch.turns.len() as u64).saturating_sub(delivered.rejected_spans);
            self.counters.records_rejected.fetch_add(
                delivered.rejected_events + delivered.rejected_spans,
                Ordering::Relaxed,
            );
            self.counters
                .events_projected
                .fetch_add(events, Ordering::Relaxed);
            self.counters
                .spans_projected
                .fetch_add(spans, Ordering::Relaxed);
            report.events_delivered += events;
            report.spans_delivered += spans;
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::atomic::AtomicBool;

    use ravel_agents_core::wire::{EventSpecBody, TurnOutcome, WorkerCommitRequest};
    use ravel_agents_store::counters::CounterSnapshot;
    use ravel_object_store::memory::MemoryStore;
    use serde_json::json;

    use super::*;
    use crate::Coordinator;
    use crate::tests::{
        TestClock, clock, coordinator, create, head, id_of, poll_now, send, store_over, store_with,
        worker,
    };
    use ravel_agents_store::StoreLimits;

    /// Records every delivery; fails while `down` is set, and rejects for
    /// good the events whose seq is in `reject`.
    #[derive(Default)]
    struct TestSink {
        down: AtomicBool,
        reject: Mutex<Vec<u64>>,
        attempts: Mutex<Vec<Id128>>,
        delivered: Mutex<Vec<ProjectedEvent>>,
        spans: Mutex<Vec<ProjectedTurn>>,
    }

    impl ProjectionSink for TestSink {
        fn deliver<'a>(&'a self, batch: &'a ProjectionBatch) -> SinkFuture<'a> {
            Box::pin(async move {
                self.attempts
                    .lock()
                    .extend(batch.events.iter().map(|e| e.event_id));
                if self.down.load(Ordering::SeqCst) {
                    return Err(SinkError("down".to_string()));
                }
                let reject = self.reject.lock().clone();
                let mut delivered = Delivered::default();
                for event in &batch.events {
                    if reject.contains(&event.event_seq) {
                        delivered.rejected_events += 1;
                        delivered.rejection = Some(format!("seq {} too old", event.event_seq));
                    } else {
                        self.delivered.lock().push(event.clone());
                    }
                }
                self.spans.lock().extend(batch.turns.iter().cloned());
                Ok(delivered)
            })
        }
    }

    struct Rig {
        clock: Arc<TestClock>,
        backend: Arc<MemoryStore>,
        co: Arc<Coordinator>,
        projector: Arc<Projector>,
        sink: Arc<TestSink>,
    }

    fn rig(limits: ProjectionLimits, payload: PayloadPolicy) -> Rig {
        rig_at(crate::tests::limits(), limits, payload)
    }

    fn rig_at(store: StoreLimits, limits: ProjectionLimits, payload: PayloadPolicy) -> Rig {
        let clock = clock();
        let backend = Arc::new(MemoryStore::new());
        let sink = Arc::new(TestSink::default());
        let projector = Arc::new(Projector::new(
            TenantHash([7; 16]),
            store_with(backend.clone(), &clock, store),
            sink.clone(),
            limits,
            payload,
            clock.clone(),
        ));
        let store = store_with(backend.clone(), &clock, store);
        let co = coordinator(&store, &clock, 100);
        let co = Arc::new(
            Arc::into_inner(co)
                .expect("sole owner")
                .with_projection(projector.clone()),
        );
        Rig {
            clock,
            backend,
            co,
            projector,
            sink,
        }
    }

    fn projector_over(
        backend: &Arc<MemoryStore>,
        clock: &Arc<TestClock>,
        sink: &Arc<TestSink>,
        limits: ProjectionLimits,
        payload: PayloadPolicy,
    ) -> Arc<Projector> {
        Arc::new(Projector::new(
            TenantHash([7; 16]),
            store_over(backend.clone(), clock),
            sink.clone(),
            limits,
            payload,
            clock.clone(),
        ))
    }

    /// Send one input and commit its turn with `events` events.
    async fn turn(rig: &Rig, request: &str, events: usize) {
        send(&rig.co, "a", request, json!({"n": request})).await;
        let w = worker(&rig.co, "v1");
        let a = poll_now(&rig.co, &w, "v1").await.expect("assignment");
        let events = (0..events)
            .map(|i| EventSpecBody {
                event_type: format!("step.{i}"),
                payload: json!({"secret": format!("{request}-{i}")}),
            })
            .collect();
        rig.co
            .commit(&WorkerCommitRequest {
                lease: a.lease.clone(),
                outcome: TurnOutcome::Completed {
                    state: json!({"last": request}),
                    output: json!("ok"),
                    events,
                    timers: Vec::new(),
                    intents: Vec::new(),
                },
            })
            .await
            .expect("commit");
    }

    fn writes(c: &CounterSnapshot) -> u64 {
        [c.resolve, c.build, c.store, c.publish]
            .iter()
            .map(|p| p.put + p.cas)
            .sum()
    }

    #[tokio::test]
    async fn a_failing_sink_changes_no_agent_state_and_retries_the_same_ids() {
        let rig = rig(ProjectionLimits::default(), PayloadPolicy::Redacted);
        create(&rig.co, "a").await;
        turn(&rig, "r1", 2).await;
        rig.sink.down.store(true, Ordering::SeqCst);

        let before_head = head(&store_over(rig.backend.clone(), &rig.clock), "a")
            .await
            .encode();
        let before_co = rig.co.metrics().store;
        let pass = rig.projector.project_once().await;
        assert!(pass.sink_failed);
        let pass = rig.projector.project_once().await;
        assert!(pass.sink_failed);

        let after_head = head(&store_over(rig.backend.clone(), &rig.clock), "a")
            .await
            .encode();
        assert_eq!(before_head, after_head, "projection wrote HEAD");
        assert_eq!(rig.co.metrics().store, before_co);
        assert_eq!(writes(&rig.projector.store_counters()), 0);
        let m = rig.projector.metrics();
        assert_eq!(m.lag_max, 2);
        assert_eq!(m.sink_failures, 2);
        assert_eq!(m.events_projected, 0);
        assert_eq!(m.records_rejected, 0);
        assert_eq!(
            rig.projector.errors().last_sink_error.as_deref(),
            Some("down")
        );

        rig.sink.down.store(false, Ordering::SeqCst);
        let pass = rig.projector.project_once().await;
        assert_eq!(pass.events_delivered, 2);
        assert_eq!(pass.spans_delivered, 1);

        let attempts = rig.sink.attempts.lock().clone();
        let delivered: Vec<Id128> = rig
            .sink
            .delivered
            .lock()
            .iter()
            .map(|e| e.event_id)
            .collect();
        let tenant = TenantHash([7; 16]);
        let agent = id_of("a");
        let expected = vec![
            event_id(&tenant, &agent, 1, 1),
            event_id(&tenant, &agent, 1, 2),
        ];
        assert_eq!(delivered, expected);
        // Three attempts at the same two ids: two failed, one landed.
        assert_eq!(
            attempts,
            [expected.clone(), expected.clone(), expected].concat()
        );
        assert_eq!(rig.projector.metrics().lag_max, 0);
        assert_eq!(writes(&rig.projector.store_counters()), 0);
    }

    #[tokio::test]
    async fn payloads_are_redacted_by_default_and_carried_on_opt_in() {
        for policy in [PayloadPolicy::Redacted, PayloadPolicy::Carried] {
            let rig = rig(ProjectionLimits::default(), policy);
            create(&rig.co, "a").await;
            turn(&rig, "r1", 1).await;
            rig.projector.project_once().await;
            let delivered = rig.sink.delivered.lock().clone();
            assert_eq!(delivered.len(), 1);
            let event = &delivered[0];
            assert!(event.payload.hash.is_some());
            assert!(event.payload.size_bytes > 0);
            let body = event.body();
            match policy {
                PayloadPolicy::Redacted => {
                    assert_eq!(event.payload.body, None);
                    assert!(body.contains("payload=redacted"), "{body}");
                    assert!(!body.contains("r1-0"), "{body}");
                }
                PayloadPolicy::Carried => {
                    assert_eq!(event.payload.body.as_deref(), Some(r#"{"secret":"r1-0"}"#));
                    assert!(body.contains("r1-0"), "{body}");
                }
            }
        }
    }

    #[tokio::test]
    async fn a_full_backlog_stops_reading_and_counts_and_never_stops_a_commit() {
        let limits = ProjectionLimits {
            backlog: 2,
            batch: 2,
        };
        let rig = rig(limits, PayloadPolicy::Redacted);
        create(&rig.co, "a").await;
        rig.sink.down.store(true, Ordering::SeqCst);
        turn(&rig, "r1", 3).await;
        rig.projector.project_once().await;
        let m = rig.projector.metrics();
        assert_eq!(m.backlog, 2);
        assert_eq!(m.backlog_bound, 2);
        assert_eq!(m.lag_max, 3);
        // The bound is reached: the next pass reads nothing more and counts.
        rig.projector.project_once().await;
        let m = rig.projector.metrics();
        assert_eq!(m.backlog, 2);
        assert_eq!(m.backlog_full, 1);

        // The coordinator commits on while the backlog is full.
        let committed = rig.co.metrics().turns_committed;
        turn(&rig, "r2", 1).await;
        assert_eq!(rig.co.metrics().turns_committed, committed + 1);
        assert_eq!(rig.projector.metrics().lag_max, 4);

        rig.sink.down.store(false, Ordering::SeqCst);
        for _ in 0..3 {
            rig.projector.project_once().await;
        }
        let seqs: Vec<u64> = rig
            .sink
            .delivered
            .lock()
            .iter()
            .map(|e| e.event_seq)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4]);
        assert_eq!(rig.projector.metrics().lag_max, 0);
    }

    #[tokio::test]
    async fn a_restarted_projector_redelivers_history_under_the_same_ids() {
        let rig = rig(ProjectionLimits::default(), PayloadPolicy::Redacted);
        create(&rig.co, "a").await;
        turn(&rig, "r1", 2).await;
        rig.projector.project_once().await;
        assert_eq!(rig.sink.delivered.lock().len(), 2);

        // A second process over the same store, with a sink of its own.
        let store = store_over(rig.backend.clone(), &rig.clock);
        let projector = projector_over(
            &rig.backend,
            &rig.clock,
            &rig.sink,
            ProjectionLimits::default(),
            PayloadPolicy::Redacted,
        );
        let co = Arc::new(
            Arc::into_inner(coordinator(&store, &rig.clock, 200))
                .expect("sole owner")
                .with_projection(projector.clone()),
        );
        let restarted = Rig {
            clock: rig.clock.clone(),
            backend: rig.backend.clone(),
            co,
            projector,
            sink: rig.sink.clone(),
        };
        turn(&restarted, "r2", 1).await;
        restarted.projector.project_once().await;
        let ids: Vec<Id128> = rig
            .sink
            .delivered
            .lock()
            .iter()
            .map(|e| e.event_id)
            .collect();
        let tenant = TenantHash([7; 16]);
        let agent = id_of("a");
        let first: Vec<Id128> = (1..=2).map(|s| event_id(&tenant, &agent, 1, s)).collect();
        let third = event_id(&tenant, &agent, 1, 3);
        // The first process's two, then the restarted one's replay of the
        // same two under the same ids, then the new event.
        assert_eq!(ids, [first.clone(), first, vec![third]].concat());
        assert_eq!(restarted.projector.metrics().lag_max, 0);
    }

    /// A pass over an agent whose history is already projected reads HEAD
    /// and the event pages holding the new events, and nothing else: no
    /// sealed page of older events, results, inbox or receipts.
    #[tokio::test]
    async fn a_pass_reads_head_and_only_the_pages_holding_new_events() {
        let mut small = crate::tests::limits();
        small.kernel.max_page_entries = 2;
        let rig = rig_at(small, ProjectionLimits::default(), PayloadPolicy::Redacted);
        create(&rig.co, "a").await;
        for i in 1..=4 {
            turn(&rig, &format!("r{i}"), 2).await;
        }
        rig.projector.project_once().await;
        assert_eq!(rig.sink.delivered.lock().len(), 8);
        let history = head(&store_with(rig.backend.clone(), &rig.clock, small), "a").await;
        assert!(
            history.index.events.pages.len() >= 3,
            "history spans several sealed event pages"
        );
        let other = &history.index;
        assert!(
            other.inbox.pages.len() + other.receipts.pages.len() + other.results.pages.len() > 0,
            "sealed pages outside the event list exist for a full read to load"
        );

        turn(&rig, "r5", 5).await;
        let now = head(&store_with(rig.backend.clone(), &rig.clock, small), "a").await;
        let mut pages: Vec<usize> = (9..=13)
            .filter_map(|seq| now.index.events.page_for(seq))
            .collect();
        pages.dedup();
        assert!(!pages.is_empty(), "some new events sit on sealed pages");
        let before = rig.projector.store_counters();
        let pass = rig.projector.project_once().await;
        assert_eq!(pass.events_delivered, 5);
        let read = rig.projector.store_counters().since(&before);
        let gets: u64 = [read.resolve, read.build, read.store, read.publish]
            .iter()
            .map(|p| p.get)
            .sum();
        assert_eq!(gets, 1 + pages.len() as u64, "HEAD plus {pages:?}");
        assert_eq!(writes(&read), 0);
    }

    /// A record the sink rejects for good is skipped and counted, its reason
    /// kept, and the turns after it project; nothing about it is retried.
    #[tokio::test]
    async fn a_rejected_record_is_skipped_and_counted_and_never_blocks_the_agent() {
        let rig = rig(ProjectionLimits::default(), PayloadPolicy::Redacted);
        create(&rig.co, "a").await;
        rig.sink.reject.lock().push(1);
        turn(&rig, "r1", 2).await;
        let pass = rig.projector.project_once().await;
        assert!(!pass.sink_failed);
        assert_eq!(pass.events_delivered, 1);
        let m = rig.projector.metrics();
        assert_eq!(m.lag_max, 0);
        assert_eq!(m.records_rejected, 1);
        assert_eq!(m.events_projected, 1);
        assert_eq!(m.sink_failures, 0);
        assert_eq!(
            rig.projector.errors().last_rejection.as_deref(),
            Some("seq 1 too old")
        );

        turn(&rig, "r2", 1).await;
        rig.projector.project_once().await;
        let seqs: Vec<u64> = rig
            .sink
            .delivered
            .lock()
            .iter()
            .map(|e| e.event_seq)
            .collect();
        assert_eq!(seqs, vec![2, 3]);
        let attempts = rig.sink.attempts.lock().clone();
        let agent = id_of("a");
        let rejected = event_id(&TenantHash([7; 16]), &agent, 1, 1);
        assert_eq!(attempts.iter().filter(|id| **id == rejected).count(), 1);
        let m = rig.projector.metrics();
        assert_eq!((m.lag_max, m.records_rejected), (0, 1));
    }

    /// A record carries its turn's committed finish time, not the time the
    /// projector delivered it: a delivery an hour after the commit still
    /// stamps the commit time.
    #[tokio::test]
    async fn a_record_carries_its_turns_finish_time_not_the_delivery_time() {
        let rig = rig(ProjectionLimits::default(), PayloadPolicy::Redacted);
        create(&rig.co, "a").await;
        rig.sink.down.store(true, Ordering::SeqCst);
        turn(&rig, "r1", 1).await;
        let committed = rig.clock.now_unix_ns();
        rig.projector.project_once().await;
        rig.clock.advance_ms(3_600_000);
        rig.sink.down.store(false, Ordering::SeqCst);
        rig.projector.project_once().await;
        let delivered = rig.sink.delivered.lock().clone();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].time_unix_ns, committed);
        assert_eq!(delivered[0].time_source, TimeSource::TurnFinished);
        let spans = rig.sink.spans.lock().clone();
        assert_eq!(spans.len(), 1);
        assert_eq!(
            (spans[0].start_unix_ns, spans[0].end_unix_ns),
            (committed, committed)
        );
    }

    #[test]
    fn a_body_over_the_limit_cuts_the_payload_text_and_says_so() {
        let event = ProjectedEvent {
            event_id: Id128([1; 16]),
            agent: AgentId([2; 16]),
            incarnation: 1,
            event_seq: 1,
            accept_seq: 1,
            turn_id: Id128([3; 16]),
            event_type: "paid".to_string(),
            payload: ProjectedPayload {
                hash: Some("ab".to_string()),
                size_bytes: 400,
                body: Some("é".repeat(200)),
            },
            trace_id: [4; 16],
            span_id: [5; 8],
            time_unix_ns: 1,
            time_source: TimeSource::Read,
        };
        let whole = event.body_within(usize::MAX);
        assert!(!whole.truncated);
        assert_eq!(whole.text, event.body());
        assert!(whole.text.ends_with(&"é".repeat(200)), "{}", whole.text);

        let limit = whole.text.len() - 101;
        let cut = event.body_within(limit);
        assert!(cut.truncated);
        assert!(cut.text.len() <= limit, "{} > {limit}", cut.text.len());
        assert!(
            cut.text.len() >= limit - 1,
            "{} < {limit} - 1",
            cut.text.len()
        );
        assert!(
            cut.text.contains(" payload_truncated=true payload=éé"),
            "{}",
            cut.text
        );

        let redacted = ProjectedEvent {
            payload: ProjectedPayload {
                body: None,
                ..event.payload.clone()
            },
            ..event
        };
        let fitted = redacted.body_within(10);
        assert!(!fitted.truncated);
        assert!(fitted.text.ends_with("payload=redacted"), "{}", fitted.text);
    }

    #[test]
    fn ids_are_stable_and_separate_every_component() {
        let t = TenantHash([1; 16]);
        let a = AgentId([2; 16]);
        assert_eq!(event_id(&t, &a, 1, 5), event_id(&t, &a, 1, 5));
        assert_ne!(event_id(&t, &a, 1, 5), event_id(&t, &a, 1, 6));
        assert_ne!(event_id(&t, &a, 1, 5), event_id(&t, &a, 2, 5));
        assert_ne!(
            event_id(&t, &a, 1, 5),
            event_id(&t, &AgentId([3; 16]), 1, 5)
        );
        assert_ne!(
            event_id(&t, &a, 1, 5),
            event_id(&TenantHash([9; 16]), &a, 1, 5)
        );
        assert_ne!(event_id(&t, &a, 1, 5).0, trace_id(&t, &a, 1, 5));
    }
}
