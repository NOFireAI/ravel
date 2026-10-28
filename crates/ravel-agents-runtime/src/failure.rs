//! Turn failure, the retry policy, and settling an unknown outcome
//! (ADR-3331 decisions 4 and 6).
//!
//! A failed attempt publishes one kernel `FailTurn` with
//! `max_attempts = max_turn_attempts`. Attempts below the bound keep the
//! input at the head of the inbox and answer [`FailOutcome::Retry`]; the
//! last one ends the input with [`FailOutcome::Failed`], dead-lettered
//! unless the worker called the failure terminal
//! (`tests::failing_max_attempts_dead_letters_the_input`).
//!
//! `OutcomeUnknown` from any command is settled in one place,
//! [`Coordinator::settle_unknown`] (and its enqueue form): the operation
//! handle the error carries is resolved through `AgentStore::resolve` before
//! anything is retried. A command whose input is already terminal is
//! answered from HEAD without running it again, so a lost acknowledgement
//! never produces a second commit
//! (`tests::lost_acknowledgement_is_resolved_without_a_second_cas`). A
//! command whose input is not terminal runs once more; the kernel answers a
//! retry of a published retryable failure or enqueue without a write.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use ravel_agents_core::command::{Command, FailTurn};
use ravel_agents_core::error::{AgentError, ErrorKind};
use ravel_agents_core::head::ResultStatus;
use ravel_agents_core::identity::{AgentId, request_hash};
use ravel_agents_core::kernel::Reply;
use ravel_agents_core::state::{Handle, OperationState};
use ravel_agents_core::wire::{SendRequest, TurnLease};
use ravel_agents_store::{HeadState, Outcome};

use crate::{Coordinator, fence_from_lease, refused};

/// The verdict on one failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailOutcome {
    /// The input waits for another attempt.
    Retry {
        accept_seq: u64,
        failed_attempts: u32,
    },
    /// The input ended failed.
    Failed {
        accept_seq: u64,
        dead_lettered: bool,
    },
    /// Cancellation was requested during the turn; the input ended
    /// cancelled.
    Cancelled { accept_seq: u64 },
    /// The turn had already committed.
    AlreadyCommitted { accept_seq: u64 },
}

/// A command's outcome, with whether it was confirmed applied by settling a
/// lost acknowledgement rather than published by the last call.
pub(crate) struct Settled {
    pub(crate) outcome: Outcome,
    pub(crate) confirmed_lost_ack: bool,
}

impl Coordinator {
    /// Fail the leased turn under the retry policy.
    pub async fn fail(
        &self,
        lease: &TurnLease,
        error: &str,
        terminal: bool,
    ) -> Result<FailOutcome, AgentError> {
        let (agent, fence) = fence_from_lease(lease)?;
        let command = Command::FailTurn(FailTurn {
            fence,
            error: error.to_string(),
            terminal,
            max_attempts: self.config.max_turn_attempts,
        });
        let settled = self.publish_turn_command(&agent, &command, &[]).await?;
        let counted = settled.outcome.published || settled.confirmed_lost_ack;
        match settled.outcome.reply {
            Reply::TurnRetry {
                accept_seq,
                failed_attempts,
            } => {
                if counted {
                    self.metrics.turns_failed.fetch_add(1, Ordering::Relaxed);
                }
                Ok(FailOutcome::Retry {
                    accept_seq,
                    failed_attempts,
                })
            }
            Reply::TurnFailed {
                accept_seq,
                dead_lettered,
            } => {
                if counted {
                    self.metrics.turns_failed.fetch_add(1, Ordering::Relaxed);
                    if dead_lettered {
                        self.metrics
                            .turns_dead_lettered
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(FailOutcome::Failed {
                    accept_seq,
                    dead_lettered,
                })
            }
            Reply::Cancelled { accept_seq } => Ok(FailOutcome::Cancelled { accept_seq }),
            Reply::AlreadyCommitted { accept_seq } => {
                Ok(FailOutcome::AlreadyCommitted { accept_seq })
            }
            _ => Err(refused(
                &agent,
                ErrorKind::OwnershipLost,
                "failure answered with something other than a verdict",
            )),
        }
    }

    /// Run a turn-scoped command, settling `OutcomeUnknown` and counting
    /// refusals.
    pub(crate) async fn publish_turn_command(
        &self,
        agent: &AgentId,
        command: &Command,
        deps: &[Bytes],
    ) -> Result<Settled, AgentError> {
        let settled = match self.store.execute_with(agent, command, deps).await {
            Ok(outcome) => Settled {
                outcome,
                confirmed_lost_ack: false,
            },
            Err(e) if e.kind == ErrorKind::OutcomeUnknown => {
                self.settle_unknown(agent, command, deps, e).await?
            }
            Err(e) => {
                match e.kind {
                    ErrorKind::OwnershipLost => {
                        self.metrics
                            .ownership_lost_refusals
                            .fetch_add(1, Ordering::Relaxed);
                        if let Err(reload) = self.reload(agent).await {
                            tracing::debug!(%reload, "agents reload after ownership loss failed");
                        }
                    }
                    ErrorKind::IncompatibleHandler => {
                        self.metrics
                            .incompatible_handler_refusals
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
                return Err(e);
            }
        };
        self.after_outcome(&settled.outcome);
        Ok(settled)
    }

    /// Settle `OutcomeUnknown` from `command`: resolve the handle the error
    /// carries, then answer a terminal input from HEAD, or run the command
    /// once more when the input is not terminal.
    pub(crate) async fn settle_unknown(
        &self,
        agent: &AgentId,
        command: &Command,
        deps: &[Bytes],
        unknown: AgentError,
    ) -> Result<Settled, AgentError> {
        let Some(handle) = unknown
            .ids
            .operation
            .as_deref()
            .and_then(|t| Handle::decode(t).ok())
        else {
            return Err(unknown);
        };
        let resolution = self.store.resolve(&handle).await?;
        self.metrics
            .outcome_unknown_resolved
            .fetch_add(1, Ordering::Relaxed);
        let terminal = matches!(
            resolution.state,
            Some(OperationState::Completed | OperationState::Failed | OperationState::Cancelled)
        );
        if !terminal {
            let outcome = self.store.execute_with(agent, command, deps).await?;
            let confirmed_lost_ack = !outcome.published;
            return Ok(Settled {
                outcome,
                confirmed_lost_ack,
            });
        }
        let (head, version) = match self.store.read_head(agent).await? {
            HeadState::Present { head, version } => (*head, version),
            HeadState::Absent | HeadState::Unsupported { .. } => return Err(unknown),
        };
        let accept_seq = resolution.accept_seq;
        let Some(result) = head.results.iter().find(|r| r.accept_seq == accept_seq) else {
            return Err(unknown);
        };
        let turn_id = match command {
            Command::CompleteTurn(c) => Some(c.fence.turn_id),
            Command::FailTurn(c) => Some(c.fence.turn_id),
            _ => None,
        };
        if turn_id.is_some() && result.turn_id != turn_id {
            return Err(refused(
                agent,
                ErrorKind::OwnershipLost,
                "the input ended under another turn",
            ));
        }
        let reply = match (command, result.status) {
            (Command::CompleteTurn(_), ResultStatus::Completed | ResultStatus::Cancelled) => {
                Reply::AlreadyCommitted { accept_seq }
            }
            (Command::FailTurn(_), ResultStatus::Failed) => Reply::TurnFailed {
                accept_seq,
                dead_lettered: result.dead_lettered,
            },
            (Command::FailTurn(_) | Command::Cancel(_), ResultStatus::Cancelled) => {
                Reply::Cancelled { accept_seq }
            }
            (Command::Cancel(_), status) => Reply::AlreadyTerminal { accept_seq, status },
            _ => {
                return Err(refused(
                    agent,
                    ErrorKind::OwnershipLost,
                    "the input ended with a different result",
                ));
            }
        };
        Ok(Settled {
            outcome: Outcome {
                agent: *agent,
                reply,
                head,
                version,
                published: false,
                effects: Vec::new(),
            },
            confirmed_lost_ack: true,
        })
    }

    /// Settle `OutcomeUnknown` from an enqueue: resolve the handle, then run
    /// the enqueue once more under the same request id. The kernel answers a
    /// request id it already holds as a duplicate without a write; when the
    /// resolved receipt is this request's own, it is reported as the first
    /// acceptance.
    pub(crate) async fn settle_enqueue(
        &self,
        agent: &AgentId,
        incarnation: u64,
        req: &SendRequest,
        payload: &Bytes,
        unknown: AgentError,
    ) -> Result<Outcome, AgentError> {
        let Some(handle) = unknown
            .ids
            .operation
            .as_deref()
            .and_then(|t| Handle::decode(t).ok())
        else {
            return Err(unknown);
        };
        let resolution = self.store.resolve(&handle).await?;
        self.metrics
            .outcome_unknown_resolved
            .fetch_add(1, Ordering::Relaxed);
        let own = resolution.is_request(
            &req.request_id,
            &req.handler,
            &request_hash(&req.handler, payload),
        );
        let mut outcome = self
            .store
            .enqueue(
                agent,
                incarnation,
                &req.request_id,
                &req.handler,
                payload.clone(),
            )
            .await?;
        if own && let Reply::Accepted { duplicate, .. } = &mut outcome.reply {
            *duplicate = false;
        }
        Ok(outcome)
    }
}
