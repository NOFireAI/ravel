//! Worker long-poll and turn dispatch (ADR-3331 decisions 6 and 9).
//!
//! Within one agent the kernel's `BeginTurn` binds the oldest input in the
//! inbox, so turns run one at a time in `accept_seq` order
//! (`tests::competing_sends_dispatch_in_accept_seq_order`). Across agents no
//! order exists; a poll offers the agent whose next input was accepted
//! earliest, ties broken by agent id.
//!
//! An agent whose HEAD records an active turn re-issues that turn: the same
//! turn id, input and base state, under the poller's own ownership epoch
//! (`tests::cold_restart_resumes_the_active_turn_from_head_alone`). The
//! assignment also copies HEAD's recorded steps, which `effects` journals
//! (`effects::tests::a_completed_step_replays_without_a_new_intent_or_attempt`).
//!
//! The takeover and the `BeginTurn` that follows each get one retry of
//! their own, so losing a takeover race does not use up the settle of a
//! `BeginTurn` whose acknowledgement was lost
//! (`tests::a_takeover_race_does_not_consume_the_begin_turn_settle`).
//! A poller whose pins do not exactly match an active turn's handler name,
//! version and digest is refused `IncompatibleHandler` before any write
//! (`tests::incompatible_handler_is_refused_before_any_write`).

use std::sync::atomic::Ordering;
use std::time::Duration;

use ravel_agents_core::command::{BeginTurn, Command, HandlerPin, OwnerFence};
use ravel_agents_core::error::{AgentError, ErrorKind};
use ravel_agents_core::head::{ActiveTurn, Head, ObjectRef};
use ravel_agents_core::identity::AgentId;
use ravel_agents_core::kernel::Reply;
use ravel_agents_core::state::Handle;
use ravel_agents_core::wire::{AgentRef, StepBody, TurnAssignment, TurnLease, WorkerPollRequest};
use ravel_agents_store::HeadState;
use serde_json::Value;

use crate::ownership::ms_to_ns;
use crate::{Coordinator, parse_pins, refused};

/// What one pass over the loaded agents found.
enum Scan {
    Assigned(Box<TurnAssignment>),
    Refused(AgentError),
    Nothing,
}

impl Coordinator {
    pub(crate) async fn poll_turn(
        &self,
        req: &WorkerPollRequest,
        deadline: tokio::time::Instant,
    ) -> Result<Option<TurnAssignment>, AgentError> {
        let pins = parse_pins(&req.handlers)?;
        let wait_ms = u64::from(req.wait_ms).min(self.config.poll_wait_cap_ms);
        let deadline = deadline.min(tokio::time::Instant::now() + Duration::from_millis(wait_ms));
        let mut waited = false;
        // At most one in-pass rescan per wake, so an interval of zero rescans
        // every pass but never spins without reaching the wait below.
        let mut rescanned = false;
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.unload_idle();
            self.discover().await?;
            match self.scan(&req.worker_id, &pins).await? {
                Scan::Assigned(a) => return Ok(Some(*a)),
                Scan::Refused(e) => return Err(e),
                // Nothing loaded has work: an agent given work through
                // another coordinator is only in the directory, so the next
                // pass lists it again, at most once per rescan interval
                // (`tests::a_poll_finds_an_agent_that_got_work_through_another_coordinator`,
                // `tests::an_idle_poll_rescans_the_directory_at_most_once_per_interval`).
                Scan::Nothing => {
                    let interval = ms_to_ns(self.config.rescan_interval_ms);
                    let last = self.last_scan_unix_ns.load(Ordering::Acquire);
                    let now = self.now();
                    // Claim the rescan slot before clearing the latch: every
                    // poller a send wakes reads the same last-scan stamp, and
                    // only the one whose compare-exchange wins rescans; the
                    // rest fall through to the wait
                    // (`tests::pollers_woken_together_share_one_rescan`).
                    if !rescanned
                        && now.saturating_sub(last) >= interval
                        && self
                            .last_scan_unix_ns
                            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                    {
                        rescanned = true;
                        self.discovered.store(false, Ordering::Release);
                        continue;
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            if !waited {
                waited = true;
                self.metrics.polls_waited.fetch_add(1, Ordering::Relaxed);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Ok(None);
            }
            rescanned = false;
        }
    }

    /// Load every agent in the directory that has work. Runs once per
    /// start, after an unload, and after a pass that found nothing to
    /// dispatch once `rescan_interval_ms` has passed since the last scan,
    /// so work that reached an agent through another coordinator is picked
    /// up within one interval. An agent whose HEAD this release cannot read
    /// is logged and skipped, so a rollback below an agent's format version
    /// is visible in the log rather than as an input that never runs.
    async fn discover(&self) -> Result<(), AgentError> {
        if self.discovered.load(Ordering::Acquire) {
            return Ok(());
        }
        // One scan at a time: a poller that arrives while another is inside
        // the scan returns at once and finds the scan's results in the
        // loaded set on its next pass
        // (`tests::pollers_woken_together_share_one_rescan`).
        if self
            .scanning
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let result = self.scan_directory().await;
        self.scanning.store(false, Ordering::Release);
        result
    }

    async fn scan_directory(&self) -> Result<(), AgentError> {
        let mut page = None;
        loop {
            let listed = self.store.list_agents(page).await?;
            for agent in listed.agents {
                if self.agents.lock().contains_key(&agent) {
                    continue;
                }
                match self.store.read_head(&agent).await? {
                    HeadState::Present { head, .. }
                        if head.active_turn.is_some() || !head.inbox.is_empty() =>
                    {
                        self.observe(&agent, &head);
                    }
                    HeadState::Present { .. } | HeadState::Absent => {}
                    HeadState::Unsupported { found } => {
                        // Once per agent, not once per rescan: the condition
                        // is static for the life of a rollback.
                        if self.unsupported_reported.lock().insert(agent) {
                            tracing::warn!(
                                agent = %agent.to_hex(),
                                format_version = found,
                                "agents directory names an agent whose HEAD format this release cannot read; its inputs are not offered"
                            );
                        }
                    }
                }
            }
            match listed.next {
                Some(next) => page = Some(next),
                None => break,
            }
        }
        self.last_scan_unix_ns.store(self.now(), Ordering::Release);
        self.discovered.store(true, Ordering::Release);
        Ok(())
    }

    /// Agents with work this worker's pins can serve, oldest next input
    /// first. An active turn the pins do not exactly match is counted as a
    /// refusal and left out.
    fn candidates(&self, worker: &str, pins: &[HandlerPin]) -> (Vec<AgentId>, Option<AgentError>) {
        let mut found = Vec::new();
        let mut refusal = None;
        let agents = self.agents.lock();
        for (agent, loaded) in agents.iter() {
            if loaded.dispatching {
                continue;
            }
            let head = &loaded.head;
            let next = match &head.active_turn {
                Some(turn) => {
                    if !pins.iter().any(|p| p.handler == turn.handler) {
                        continue;
                    }
                    if !pins.iter().any(|p| pinned(p, turn)) {
                        refusal = Some(refused(
                            agent,
                            ErrorKind::IncompatibleHandler,
                            "worker lacks the active turn's handler version",
                        ));
                        continue;
                    }
                    head.inbox
                        .iter()
                        .find(|e| e.accept_seq == turn.input_accept_seq)
                }
                None => head
                    .inbox
                    .first()
                    .filter(|e| pins.iter().any(|p| p.handler == e.handler)),
            };
            let Some(next) = next else { continue };
            if head.owner_id != worker
                && !head.owner_id.is_empty()
                && self.takeover_due(agent, head, worker).is_none()
            {
                continue;
            }
            found.push((next.accepted_unix_ns, *agent));
        }
        found.sort();
        (found.into_iter().map(|(_, a)| a).collect(), refusal)
    }

    async fn scan(&self, worker: &str, pins: &[HandlerPin]) -> Result<Scan, AgentError> {
        let (candidates, refusal) = self.candidates(worker, pins);
        for agent in candidates {
            if !self.claim(&agent) {
                continue;
            }
            let result = self.dispatch_one(&agent, worker, pins).await;
            self.release(&agent);
            match result {
                Ok(Some(assignment)) => return Ok(Scan::Assigned(Box::new(assignment))),
                Ok(None) => {}
                Err(e)
                    if matches!(
                        e.kind,
                        ErrorKind::OwnershipLost | ErrorKind::IncompatibleHandler
                    ) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(match refusal {
            // Counted here, not where the candidate was skipped: a pass that
            // assigned another agent refused nothing.
            Some(e) => {
                self.metrics
                    .incompatible_handler_refusals
                    .fetch_add(1, Ordering::Relaxed);
                Scan::Refused(e)
            }
            None => Scan::Nothing,
        })
    }

    fn claim(&self, agent: &AgentId) -> bool {
        let mut agents = self.agents.lock();
        match agents.get_mut(agent) {
            Some(l) if !l.dispatching => {
                l.dispatching = true;
                true
            }
            _ => false,
        }
    }

    fn release(&self, agent: &AgentId) {
        if let Some(l) = self.agents.lock().get_mut(agent) {
            l.dispatching = false;
        }
    }

    /// Take the agent over if due, then `BeginTurn`. A stale loaded root is
    /// reloaded and tried once more.
    async fn dispatch_one(
        &self,
        agent: &AgentId,
        worker: &str,
        pins: &[HandlerPin],
    ) -> Result<Option<TurnAssignment>, AgentError> {
        let mut takeover_retried = false;
        let mut begin_retried = false;
        let mut begin_unknown = false;
        loop {
            let Some(head) = self.cached(agent) else {
                return Ok(None);
            };
            let fence = if head.owner_id == worker {
                OwnerFence {
                    incarnation: head.incarnation,
                    owner_id: worker.to_string(),
                    epoch: head.epoch,
                }
            } else {
                let Some(reason) = self.takeover_due(agent, &head, worker) else {
                    return Ok(None);
                };
                match self.take_over(agent, &head, worker, reason).await {
                    Ok(fence) => fence,
                    Err(e) if e.kind == ErrorKind::OwnershipLost && !takeover_retried => {
                        takeover_retried = true;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            };
            let command = Command::BeginTurn(BeginTurn {
                fence: fence.clone(),
                handlers: pins.to_vec(),
            });
            let out = match self.store.execute(agent, &command).await {
                Ok(out) => out,
                // Re-running against the reloaded root resumes a turn whose
                // `BeginTurn` landed without its acknowledgement.
                Err(e)
                    if matches!(e.kind, ErrorKind::OwnershipLost | ErrorKind::OutcomeUnknown)
                        && !begin_retried =>
                {
                    begin_unknown = e.kind == ErrorKind::OutcomeUnknown;
                    begin_retried = true;
                    self.reload(agent).await?;
                    continue;
                }
                Err(e) => {
                    if e.kind == ErrorKind::IncompatibleHandler {
                        self.metrics
                            .incompatible_handler_refusals
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(e);
                }
            };
            if begin_unknown {
                self.metrics
                    .outcome_unknown_resolved
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.after_outcome(&out);
            let turn = match &out.reply {
                Reply::TurnStarted(turn) | Reply::TurnResumed(turn) => turn.clone(),
                _ => return Ok(None),
            };
            let expires = self.now().saturating_add(self.lease_ns());
            self.leases.grant(agent, worker, fence.epoch, expires);
            return self
                .assignment(agent, &out.head, &turn, &fence)
                .await
                .map(Some);
        }
    }

    /// The assignment for `turn`, built from the root that began or resumed
    /// it: the input it binds, the state at its base and the recorded step
    /// transcript.
    async fn assignment(
        &self,
        agent: &AgentId,
        head: &Head,
        turn: &ActiveTurn,
        fence: &OwnerFence,
    ) -> Result<TurnAssignment, AgentError> {
        let input = head
            .inbox
            .iter()
            .find(|e| e.accept_seq == turn.input_accept_seq)
            .ok_or_else(|| {
                refused(
                    agent,
                    ErrorKind::OwnershipLost,
                    "active turn's input is not in the inbox",
                )
            })?;
        let payload = self.read_json(agent, input.payload).await?;
        let state = self.read_json(agent, head.state).await?;
        let mut steps = Vec::with_capacity(head.steps.len());
        for step in &head.steps {
            steps.push(StepBody {
                step_id: step.step_id.to_hex(),
                step_key: step.step_key.clone(),
                tool_name: step.tool_name.clone(),
                tool_version: step.tool_version.clone(),
                input_hash: step.input_hash.to_hex(),
                status: step.status.into(),
                output: match step.output {
                    Some(r) => Some(self.read_json(agent, Some(r)).await?),
                    None => None,
                },
            });
        }
        Ok(TurnAssignment {
            lease: TurnLease {
                agent_id: agent.to_hex(),
                incarnation: head.incarnation,
                owner_id: fence.owner_id.clone(),
                epoch: fence.epoch,
                turn_id: turn.turn_id.to_hex(),
                base_state_version: turn.base_state_version,
                handler_version: turn.handler_version.clone(),
                handler_digest: turn.handler_digest.to_hex(),
                observed_root_revision: head.root_revision,
            },
            agent: AgentRef {
                kind: head.kind.as_str().to_string(),
                key: head.key.as_str().to_string(),
            },
            operation: Handle::operation(*agent, head.incarnation, input.accept_seq).encode(),
            accept_seq: input.accept_seq,
            handler: turn.handler.clone(),
            state,
            payload,
            steps,
        })
    }

    /// An object decoded as JSON; no object is `null`.
    pub(crate) async fn read_json(
        &self,
        agent: &AgentId,
        dep: Option<ObjectRef>,
    ) -> Result<Value, AgentError> {
        let Some(dep) = dep else {
            return Ok(Value::Null);
        };
        let bytes = self.store.read_object(agent, dep).await?;
        serde_json::from_slice(&bytes).map_err(|_| {
            refused(
                agent,
                ErrorKind::UnsupportedFormat,
                "object is not a JSON value",
            )
        })
    }
}

fn pinned(pin: &HandlerPin, turn: &ActiveTurn) -> bool {
    pin.handler == turn.handler
        && pin.version == turn.handler_version
        && pin.digest == turn.handler_digest
}
