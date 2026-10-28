//! Worker long-poll and turn dispatch (ADR-3331 decisions 6 and 9).
//!
//! Within one agent the kernel's `BeginTurn` binds the oldest input in the
//! inbox, so turns run one at a time in `accept_seq` order
//! (`tests::competing_sends_dispatch_in_accept_seq_order`). Across agents no
//! order exists; a poll offers the agent whose next input was accepted
//! earliest, ties broken by agent id.
//!
//! An agent whose HEAD records an active turn re-issues that turn: the same
//! turn id, input and recorded step transcript, under the poller's own
//! ownership epoch (`tests::cold_restart_resumes_the_active_turn_from_head_alone`).
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
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.unload_idle();
            self.discover().await?;
            match self.scan(&req.worker_id, &pins).await? {
                Scan::Assigned(a) => return Ok(Some(*a)),
                Scan::Refused(e) => return Err(e),
                Scan::Nothing => {}
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
        }
    }

    /// Load every agent in the directory that has work, once per start or
    /// unload.
    async fn discover(&self) -> Result<(), AgentError> {
        if self.discovered.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut page = None;
        loop {
            let listed = self.store.list_agents(page).await?;
            for agent in listed.agents {
                if self.agents.lock().contains_key(&agent) {
                    continue;
                }
                if let HeadState::Present { head, .. } = self.store.read_head(&agent).await?
                    && (head.active_turn.is_some() || !head.inbox.is_empty())
                {
                    self.observe(&agent, &head);
                }
            }
            match listed.next {
                Some(next) => page = Some(next),
                None => break,
            }
        }
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
                        self.metrics
                            .incompatible_handler_refusals
                            .fetch_add(1, Ordering::Relaxed);
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
            Some(e) => Scan::Refused(e),
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
        let mut retried = false;
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
                    Err(e) if e.kind == ErrorKind::OwnershipLost && !retried => {
                        retried = true;
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
                Err(e) if e.kind == ErrorKind::OwnershipLost && !retried => {
                    retried = true;
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
    async fn read_json(
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
