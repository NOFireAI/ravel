//! The independent sequential reference machine (ADR-3331 decision 13).
//!
//! A model of one agent's client-visible behaviour, written from the text of
//! ADR-3331 decisions 3, 5, 6, 7 and 8 and not from `ravel-agents-core`'s
//! kernel, so a history checked against it is checked against the ADR rather
//! than against the code under test. It sees abstract [`Event`]s, one at a
//! time, and answers the [`Reply`] a client or worker should observe.
//!
//! Rules it encodes:
//!
//! - `create` is idempotent while the agent is active; `create` after
//!   `destroy` bumps the incarnation, and every handle from an earlier
//!   incarnation is refused as `AgentGone` (decision 3).
//! - `send` is keyed by handler and request id within one incarnation: the
//!   same id with the same payload bytes returns the recorded handle,
//!   different bytes are `IdempotencyConflict`, and a new id takes the next
//!   `accept_seq` (decision 5). The receipt holds the payload bytes rather
//!   than the production `request_hash`, so the check does not share the
//!   code under test.
//! - Turns start one at a time in `accept_seq` order, and each input reaches
//!   exactly one terminal result per incarnation (decisions 5 and 6).
//! - `cancel` is idempotent. A terminal input reports its terminal status,
//!   so a completion that committed first wins; an input not yet started
//!   is cancelled at once; a running input records the request and its next
//!   commit ends it `cancelled` (decision 5, `CancelResponse.requested`).
//! - One timer generation counter per agent, shared by every timer key, so
//!   a timer key has one generation at a time. A due event fires a
//!   generation at most once, a stale or cancelled generation fires
//!   nothing, and cancelling after the firing reports `AlreadyFired`
//!   (decision 8).
//! - The state version changes only when a turn completes (decision 6).
//! - Steps (decision 7). A turn journals each step by key with its tool
//!   name, tool version and input bytes; a key reused with any of them
//!   different blocks the turn `NonDeterministicReplay`, and a blocked turn
//!   cannot complete. A completed step replays its output. A step with no
//!   effect authorizes attempt 1; a step whose effect completed replays the
//!   effect's result; an effect in doubt answers `EffectInDoubt`. Steps are
//!   sequential: a new step while this turn's own dispatched attempt has no
//!   result is `IdempotencyConflict`. An attempt nobody in the current turn
//!   dispatched is lost, and its policy decides the recovery:
//!   `provider_idempotency` dispatches the same attempt again,
//!   `at_least_once` authorizes the next one, and `manual` latches the
//!   effect in doubt and answers `EffectInDoubt`. A turn whose input has an
//!   effect in doubt cannot complete or fail, a turn cannot fail with an
//!   attempt still waiting for its result, and an input running with an
//!   effect in doubt or a blocked turn reads `blocked`.
//! - Effect results (decision 7). Only the effect's latest attempt settles
//!   it, and only while its input has not ended cancelled; a result for an
//!   earlier attempt, a different attempt's result after it settled, and
//!   any result once the input ended cancelled are evidence and change
//!   nothing. A second, different result for the attempt that settled it
//!   is `IdempotencyConflict`; the same one again is recorded. An attempt
//!   the effect never authorized is `OwnershipLost`; a step with no effect
//!   record has no effect to settle and answers `AgentGone`.
//! - Reconciliation (decision 7). An effect in doubt is settled by
//!   attaching a result to its latest attempt (attaching again is
//!   idempotent), or by authorizing the next attempt, which is dispatched
//!   once; a repeat of that authorization dispatches nothing. Either needs
//!   the effect in doubt (attach also accepts a settled one), else it is
//!   `OwnershipLost`.
//!
//! Choices the ADR leaves open, made here and named so a driver can see
//! them: the first incarnation is [`FIRST_INCARNATION`] and accept
//! sequences restart at [`FIRST_ACCEPT_SEQ`] in each incarnation; a new
//! incarnation starts at state version 0 with no receipts and no timers; a
//! cancellation requested while a turn runs ends the input `cancelled` at
//! that turn's next commit, whether the commit was a completion or a
//! failure, and leaves the state version unchanged; a handle naming an
//! `accept_seq` this incarnation never accepted answers
//! [`Reply::UnknownOperation`] on a client read, and a worker event on the
//! same handle answers `OwnershipLost`, except an effect result, attach or
//! authorization, which answers `AgentGone` on that handle and on a step
//! key with no effect record, as the runtime does: it finds the effect by an
//! id derived from incarnation, `accept_seq` and step key, and answers
//! `AgentGone` when HEAD journals none; a `destroy` of an incarnation that
//! is already tombstoned is idempotent and answers [`Reply::Destroyed`]
//! with `already` set, as the kernel's `AlreadyTombstoned` does, while a
//! `destroy` of an agent never created answers `AgentGone`. Retryable
//! failures, quotas and wait timeouts are not modelled: a
//! [`Event::TurnFailed`] is the terminal failure after the handler's retry
//! policy is spent. The timer message's content is not modelled either: a
//! firing enqueues an input known by its handle alone, and
//! [`ClientOp::SetTimer`]'s handler and payload are not checked.
//!
//! Choices for steps and effects: an attempt is this turn's own, and so
//! blocks the next step, when it was dispatched by a step after the input's
//! latest [`Event::TurnStarted`]; every `TurnStarted`, a takeover's
//! included, starts a new owner session, and an attempt authorized by
//! [`Event::ReconcileAuthorize`] is an operator's and belongs to no
//! session (the driver reconciles from a process that runs no turn). A
//! step whose input has a cancellation requested answers `OwnershipLost`
//! before it authorizes or re-dispatches anything, after journaling the
//! step. A cancel of a running input reports `running` even while the input
//! reads `blocked`. Step outputs are dropped when the turn ends; effects
//! and their results are kept for the incarnation. Step and effect results
//! are compared as bytes, not by production's content hash. Intents a
//! completion commits are not modelled.

use std::collections::{BTreeMap, HashMap};

use ravel_agents_core::error::ErrorKind;
pub use ravel_agents_core::head::RecoveryPolicy;
use ravel_agents_core::identity::{AgentId, agent_id, timer_id};
use ravel_agents_core::keys::{AgentKey, AgentKind, KeyError};
use ravel_agents_core::state::{Handle, HandleItem};
use ravel_agents_core::wire::{OperationStatus, TimerCancelOutcome};

/// Incarnation of an agent's first `create`.
pub const FIRST_INCARNATION: u64 = 1;
/// `accept_seq` of the first input accepted in an incarnation.
pub const FIRST_ACCEPT_SEQ: u64 = 1;
/// Generation of an agent's first timer `set`; generations are counted per
/// agent, so a second key's first `set` is already past it.
pub const FIRST_TIMER_GENERATION: u64 = 1;

/// A client call, named by what it asks rather than by its HTTP route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientOp {
    Create,
    Destroy,
    Send {
        handler: String,
        request_id: String,
        payload: Vec<u8>,
    },
    Operation {
        operation: Handle,
    },
    /// A result read whose wait elapsed or was zero.
    Result {
        operation: Handle,
    },
    Resolve {
        handler: String,
        request_id: String,
    },
    Cancel {
        operation: Handle,
    },
    State,
    SetTimer {
        timer_key: String,
        handler: String,
        payload: Vec<u8>,
    },
    CancelTimer {
        timer_key: String,
    },
}

/// One step of an agent's life as the reference machine sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Client(ClientOp),
    /// A worker bound the input to a turn.
    TurnStarted {
        operation: Handle,
    },
    /// The turn's completion committed.
    TurnCompleted {
        operation: Handle,
        output: Vec<u8>,
    },
    /// The turn's terminal failure committed.
    TurnFailed {
        operation: Handle,
        error: String,
    },
    /// A wake hint or the sweep found the timer generation due.
    TimerDue {
        timer_key: String,
        generation: u64,
    },
    /// The turn journals a step without running it.
    StepStarted {
        operation: Handle,
        step: StepCall,
    },
    /// The turn runs a step: journal it, then replay it or dispatch an
    /// attempt.
    Step {
        operation: Handle,
        step: StepCall,
    },
    /// A tool's result (or failure) for one attempt of the step `step_key`
    /// of `operation`, from whoever reports it.
    EffectResult {
        operation: Handle,
        step_key: String,
        attempt: u32,
        result: Vec<u8>,
    },
    /// An operator attaches a verified result to an effect in doubt.
    ReconcileAttach {
        operation: Handle,
        step_key: String,
        result: Vec<u8>,
    },
    /// An operator authorizes and dispatches `attempt` of an effect in
    /// doubt.
    ReconcileAuthorize {
        operation: Handle,
        step_key: String,
        attempt: u32,
    },
}

/// One tool call a handler makes, as the reference identifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepCall {
    pub key: String,
    pub tool: String,
    pub version: String,
    pub input: Vec<u8>,
    pub policy: RecoveryPolicy,
}

/// Where an effect stands. An intent with no attempt is never visible: a
/// step authorizes attempt 1 in the same event that journals the intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectPhase {
    Authorized,
    InDoubt,
    Completed,
}

/// An effect as a step's journal reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectView {
    pub phase: EffectPhase,
    /// The latest authorized attempt.
    pub attempt: u32,
}

/// A terminal result as a client reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub status: OperationStatus,
    pub output: Option<Vec<u8>>,
    pub error: Option<String>,
}

/// What the caller of an [`Event`] observes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Created {
        incarnation: u64,
        created: bool,
    },
    /// `already` when the incarnation was tombstoned before this call.
    Destroyed {
        incarnation: u64,
        already: bool,
    },
    Accepted {
        operation: Handle,
        duplicate: bool,
    },
    Status(OperationStatus),
    /// `None` while the input has no terminal result.
    Result(Option<Outcome>),
    Resolved(Option<(Handle, OperationStatus)>),
    Cancel {
        status: OperationStatus,
        requested: bool,
    },
    State {
        incarnation: u64,
        state_version: u64,
    },
    TimerSet {
        timer: Handle,
        generation: u64,
    },
    TimerCancel(TimerCancelOutcome),
    /// The input the firing enqueued, or `None` when nothing fired.
    TimerFired(Option<Handle>),
    TurnStarted,
    Committed {
        status: OperationStatus,
        state_version: u64,
    },
    /// The handle is of this incarnation but names no accepted input.
    UnknownOperation,
    /// What the journal holds for a step after [`Event::StepStarted`].
    StepJournaled {
        completed: bool,
        effect: Option<EffectView>,
    },
    /// The step dispatched `attempt`.
    StepDispatched {
        attempt: u32,
    },
    /// The step replayed its output.
    StepCompleted {
        output: Vec<u8>,
    },
    /// The result is the effect's.
    EffectRecorded,
    /// The result is kept as evidence; nothing changed.
    EffectEvidence,
    /// A reconcile authorization, `dispatched` when this call dispatched.
    Reconciled {
        dispatched: bool,
    },
    Error(ErrorKind),
}

#[derive(Debug, Clone)]
struct Input {
    status: OperationStatus,
    cancel_requested: bool,
    outcome: Option<Outcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerPhase {
    Armed,
    Delivered,
    Cancelled,
}

#[derive(Debug, Clone)]
struct Timer {
    generation: u64,
    phase: TimerPhase,
}

#[derive(Debug, Clone)]
struct StepRec {
    tool: String,
    version: String,
    input: Vec<u8>,
    completed: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct EffectRec {
    policy: RecoveryPolicy,
    phase: EffectPhase,
    attempt: u32,
    /// The attempt that settled it, and its result.
    result: Option<(u32, Vec<u8>)>,
    /// The owner session that dispatched the latest attempt; `None` when an
    /// operator authorized it.
    dispatched_in: Option<u64>,
}

/// Everything that belongs to one incarnation and dies with it.
#[derive(Debug, Clone)]
struct Life {
    incarnation: u64,
    tombstoned: bool,
    next_accept_seq: u64,
    inputs: BTreeMap<u64, Input>,
    /// Receipts keyed by (handler, request id), holding the accepted payload
    /// bytes, so idempotency is judged on the bytes and not on the
    /// production `request_hash`.
    receipts: HashMap<(String, String), (Vec<u8>, u64)>,
    state_version: u64,
    running: Option<u64>,
    timers: HashMap<String, Timer>,
    /// One generation counter per agent (ADR-3331 decision 3 keeps a single
    /// timer generation counter in HEAD), shared by every timer key.
    timer_generation: u64,
    /// The running turn's step journal, by key.
    steps: BTreeMap<String, StepRec>,
    /// Effects by (accept_seq, step key), kept for the incarnation.
    effects: BTreeMap<(u64, String), EffectRec>,
    /// The running turn is blocked by a non-deterministic replay.
    blocked: bool,
    /// Counts [`Event::TurnStarted`]s; the current owner session.
    session: u64,
}

impl Life {
    fn new(incarnation: u64) -> Life {
        Life {
            incarnation,
            tombstoned: false,
            next_accept_seq: FIRST_ACCEPT_SEQ,
            inputs: BTreeMap::new(),
            receipts: HashMap::new(),
            state_version: 0,
            running: None,
            timers: HashMap::new(),
            timer_generation: 0,
            steps: BTreeMap::new(),
            effects: BTreeMap::new(),
            blocked: false,
            session: 0,
        }
    }

    fn input_effects(&self, seq: u64) -> impl Iterator<Item = &EffectRec> {
        self.effects
            .iter()
            .filter(move |((s, _), _)| *s == seq)
            .map(|(_, e)| e)
    }

    /// The status a read reports: a running input with an effect in doubt
    /// or a blocked turn reads `blocked`.
    fn reported(&self, seq: u64, status: OperationStatus) -> OperationStatus {
        let in_doubt = self
            .input_effects(seq)
            .any(|e| e.phase == EffectPhase::InDoubt);
        if status == OperationStatus::Running
            && (in_doubt || (self.blocked && self.running == Some(seq)))
        {
            OperationStatus::Blocked
        } else {
            status
        }
    }

    fn accept(&mut self) -> u64 {
        let seq = self.next_accept_seq;
        self.next_accept_seq += 1;
        self.inputs.insert(
            seq,
            Input {
                status: OperationStatus::Accepted,
                cancel_requested: false,
                outcome: None,
            },
        );
        seq
    }
}

/// The sequential model of one agent. See the module docs for the rules.
#[derive(Debug, Clone)]
pub struct ReferenceMachine {
    agent: AgentId,
    life: Option<Life>,
    terminal_log: Vec<(Handle, OperationStatus)>,
}

fn is_terminal(status: OperationStatus) -> bool {
    matches!(
        status,
        OperationStatus::Completed | OperationStatus::Failed | OperationStatus::Cancelled
    )
}

impl ReferenceMachine {
    /// A machine for the agent named `(kind, key)`, which does not exist yet.
    pub fn new(kind: &str, key: &str) -> Result<ReferenceMachine, KeyError> {
        let agent = agent_id(&AgentKind::new(kind)?, &AgentKey::new(key)?);
        Ok(ReferenceMachine {
            agent,
            life: None,
            terminal_log: Vec::new(),
        })
    }

    pub fn agent_id(&self) -> AgentId {
        self.agent
    }

    /// The current incarnation, tombstoned or not; `None` before the first
    /// `create`.
    pub fn incarnation(&self) -> Option<u64> {
        self.life.as_ref().map(|l| l.incarnation)
    }

    /// The operation handle for `accept_seq` in `incarnation` of this agent.
    pub fn operation(&self, incarnation: u64, accept_seq: u64) -> Handle {
        Handle::operation(self.agent, incarnation, accept_seq)
    }

    /// The timer handle for `timer_key` in `incarnation` of this agent.
    pub fn timer(&self, incarnation: u64, timer_key: &str) -> Handle {
        Handle {
            agent: self.agent,
            incarnation,
            item: HandleItem::Timer(timer_id(incarnation, timer_key)),
        }
    }

    /// Every input that reached a terminal result, in the order it did.
    pub fn terminal_log(&self) -> &[(Handle, OperationStatus)] {
        &self.terminal_log
    }

    /// The effect of step `step_key` of `operation`'s input in the live
    /// incarnation, as a journal would read it.
    pub fn effect_view(&self, operation: &Handle, step_key: &str) -> Option<EffectView> {
        let (seq, _) = self.input(operation).ok()?;
        let life = self.life.as_ref()?;
        life.effects
            .get(&(seq, step_key.to_string()))
            .map(|e| EffectView {
                phase: e.phase,
                attempt: e.attempt,
            })
    }

    /// Whether the running turn journals step `step_key` as completed.
    pub fn step_completed(&self, step_key: &str) -> bool {
        self.life
            .as_ref()
            .and_then(|life| life.steps.get(step_key))
            .is_some_and(|s| s.completed.is_some())
    }

    /// Apply one event and return the reply its caller should observe.
    pub fn apply(&mut self, event: &Event) -> Reply {
        match event {
            Event::Client(op) => self.client(op),
            Event::TurnStarted { operation } => self.start(operation),
            Event::TurnCompleted { operation, output } => {
                self.commit(operation, OperationStatus::Completed, Some(output), None)
            }
            Event::TurnFailed { operation, error } => {
                self.commit(operation, OperationStatus::Failed, None, Some(error))
            }
            Event::TimerDue {
                timer_key,
                generation,
            } => self.timer_due(timer_key, *generation),
            Event::StepStarted { operation, step } => self.step(operation, step, false),
            Event::Step { operation, step } => self.step(operation, step, true),
            Event::EffectResult {
                operation,
                step_key,
                attempt,
                result,
            } => self.effect_result(operation, step_key, *attempt, result),
            Event::ReconcileAttach {
                operation,
                step_key,
                result,
            } => self.reconcile_attach(operation, step_key, result),
            Event::ReconcileAuthorize {
                operation,
                step_key,
                attempt,
            } => self.reconcile_authorize(operation, step_key, *attempt),
        }
    }

    fn client(&mut self, op: &ClientOp) -> Reply {
        match op {
            ClientOp::Create => self.create(),
            ClientOp::Destroy => match self.life.as_mut() {
                Some(life) => {
                    let already = life.tombstoned;
                    life.tombstoned = true;
                    Reply::Destroyed {
                        incarnation: life.incarnation,
                        already,
                    }
                }
                None => Reply::Error(ErrorKind::AgentGone),
            },
            ClientOp::Send {
                handler,
                request_id,
                payload,
            } => self.send(handler, request_id, payload),
            ClientOp::Operation { operation } => match self.input(operation) {
                Ok((seq, input)) => match self.life.as_ref() {
                    Some(life) => Reply::Status(life.reported(seq, input.status)),
                    None => Reply::Error(ErrorKind::AgentGone),
                },
                Err(reply) => reply,
            },
            ClientOp::Result { operation } => match self.input(operation) {
                Ok((_, input)) => Reply::Result(input.outcome.clone()),
                Err(reply) => reply,
            },
            ClientOp::Resolve {
                handler,
                request_id,
            } => {
                let agent = self.agent;
                match self.live_mut() {
                    Ok(life) => Reply::Resolved(
                        life.receipts
                            .get(&(handler.clone(), request_id.clone()))
                            .and_then(|(_, seq)| {
                                life.inputs.get(seq).map(|input| {
                                    (
                                        Handle::operation(agent, life.incarnation, *seq),
                                        life.reported(*seq, input.status),
                                    )
                                })
                            }),
                    ),
                    Err(reply) => reply,
                }
            }
            ClientOp::Cancel { operation } => self.cancel(operation),
            ClientOp::State => match self.live_mut() {
                Ok(life) => Reply::State {
                    incarnation: life.incarnation,
                    state_version: life.state_version,
                },
                Err(reply) => reply,
            },
            ClientOp::SetTimer { timer_key, .. } => self.set_timer(timer_key),
            ClientOp::CancelTimer { timer_key } => match self.live_mut() {
                Ok(life) => Reply::TimerCancel(match life.timers.get_mut(timer_key) {
                    None => TimerCancelOutcome::Unknown,
                    Some(timer) => match timer.phase {
                        TimerPhase::Delivered => TimerCancelOutcome::AlreadyFired,
                        TimerPhase::Armed | TimerPhase::Cancelled => {
                            timer.phase = TimerPhase::Cancelled;
                            TimerCancelOutcome::Cancelled
                        }
                    },
                }),
                Err(reply) => reply,
            },
        }
    }

    fn create(&mut self) -> Reply {
        match &mut self.life {
            None => {
                self.life = Some(Life::new(FIRST_INCARNATION));
                Reply::Created {
                    incarnation: FIRST_INCARNATION,
                    created: true,
                }
            }
            Some(life) if !life.tombstoned => Reply::Created {
                incarnation: life.incarnation,
                created: false,
            },
            Some(life) => {
                let incarnation = life.incarnation + 1;
                *life = Life::new(incarnation);
                Reply::Created {
                    incarnation,
                    created: true,
                }
            }
        }
    }

    fn send(&mut self, handler: &str, request_id: &str, payload: &[u8]) -> Reply {
        let agent = self.agent;
        let life = match self.live_mut() {
            Ok(life) => life,
            Err(reply) => return reply,
        };
        let receipt = (handler.to_string(), request_id.to_string());
        if let Some((recorded, seq)) = life.receipts.get(&receipt) {
            return if recorded.as_slice() == payload {
                Reply::Accepted {
                    operation: Handle::operation(agent, life.incarnation, *seq),
                    duplicate: true,
                }
            } else {
                Reply::Error(ErrorKind::IdempotencyConflict)
            };
        }
        let seq = life.accept();
        life.receipts.insert(receipt, (payload.to_vec(), seq));
        Reply::Accepted {
            operation: Handle::operation(agent, life.incarnation, seq),
            duplicate: false,
        }
    }

    fn cancel(&mut self, operation: &Handle) -> Reply {
        let seq = match self.input(operation) {
            Ok((seq, _)) => seq,
            Err(reply) => return reply,
        };
        let Some(life) = self.life.as_mut() else {
            return Reply::Error(ErrorKind::AgentGone);
        };
        let Some(input) = life.inputs.get_mut(&seq) else {
            return Reply::UnknownOperation;
        };
        match input.status {
            OperationStatus::Accepted => {
                input.status = OperationStatus::Cancelled;
                input.outcome = Some(Outcome {
                    status: OperationStatus::Cancelled,
                    output: None,
                    error: None,
                });
                self.terminal_log
                    .push((*operation, OperationStatus::Cancelled));
                Reply::Cancel {
                    status: OperationStatus::Cancelled,
                    requested: false,
                }
            }
            OperationStatus::Running | OperationStatus::Blocked => {
                input.cancel_requested = true;
                Reply::Cancel {
                    status: input.status,
                    requested: true,
                }
            }
            terminal => Reply::Cancel {
                status: terminal,
                requested: false,
            },
        }
    }

    fn start(&mut self, operation: &Handle) -> Reply {
        let seq = match self.input(operation) {
            Ok((seq, _)) => seq,
            Err(Reply::UnknownOperation) => return Reply::Error(ErrorKind::OwnershipLost),
            Err(reply) => return reply,
        };
        let Some(life) = self.life.as_mut() else {
            return Reply::Error(ErrorKind::AgentGone);
        };
        if let Some(running) = life.running {
            // A worker that took over resumes the same turn (decision 6).
            return if running == seq {
                life.session += 1;
                Reply::TurnStarted
            } else {
                Reply::Error(ErrorKind::OwnershipLost)
            };
        }
        let head_of_line = life
            .inputs
            .iter()
            .find(|(_, input)| !is_terminal(input.status))
            .map(|(seq, _)| *seq);
        if head_of_line != Some(seq) {
            return Reply::Error(ErrorKind::OwnershipLost);
        }
        if let Some(input) = life.inputs.get_mut(&seq) {
            input.status = OperationStatus::Running;
        }
        life.running = Some(seq);
        life.session += 1;
        Reply::TurnStarted
    }

    fn commit(
        &mut self,
        operation: &Handle,
        status: OperationStatus,
        output: Option<&Vec<u8>>,
        error: Option<&String>,
    ) -> Reply {
        let seq = match self.input(operation) {
            Ok((seq, _)) => seq,
            Err(Reply::UnknownOperation) => return Reply::Error(ErrorKind::OwnershipLost),
            Err(reply) => return reply,
        };
        let Some(life) = self.life.as_mut() else {
            return Reply::Error(ErrorKind::AgentGone);
        };
        if life.running != Some(seq) {
            return Reply::Error(ErrorKind::OwnershipLost);
        }
        if status == OperationStatus::Completed && life.blocked {
            return Reply::Error(ErrorKind::NonDeterministicReplay);
        }
        let waiting = |e: &EffectRec| match status {
            OperationStatus::Completed => e.phase == EffectPhase::InDoubt,
            _ => e.phase != EffectPhase::Completed,
        };
        if life.input_effects(seq).any(waiting) {
            return Reply::Error(ErrorKind::EffectInDoubt);
        }
        life.steps.clear();
        life.blocked = false;
        let Some(input) = life.inputs.get_mut(&seq) else {
            return Reply::Error(ErrorKind::OwnershipLost);
        };
        let outcome = if input.cancel_requested {
            Outcome {
                status: OperationStatus::Cancelled,
                output: None,
                error: None,
            }
        } else {
            Outcome {
                status,
                output: output.cloned(),
                error: error.cloned(),
            }
        };
        if outcome.status == OperationStatus::Completed {
            life.state_version += 1;
        }
        input.status = outcome.status;
        let final_status = outcome.status;
        input.outcome = Some(outcome);
        life.running = None;
        self.terminal_log.push((*operation, final_status));
        Reply::Committed {
            status: final_status,
            state_version: life.state_version,
        }
    }

    fn set_timer(&mut self, timer_key: &str) -> Reply {
        let agent = self.agent;
        let life = match self.live_mut() {
            Ok(life) => life,
            Err(reply) => return reply,
        };
        life.timer_generation += 1;
        let generation = life.timer_generation;
        life.timers.insert(
            timer_key.to_string(),
            Timer {
                generation,
                phase: TimerPhase::Armed,
            },
        );
        Reply::TimerSet {
            timer: Handle {
                agent,
                incarnation: life.incarnation,
                item: HandleItem::Timer(timer_id(life.incarnation, timer_key)),
            },
            generation,
        }
    }

    fn timer_due(&mut self, timer_key: &str, generation: u64) -> Reply {
        let agent = self.agent;
        let life = match self.live_mut() {
            Ok(life) => life,
            Err(reply) => return reply,
        };
        let fires = life
            .timers
            .get(timer_key)
            .is_some_and(|t| t.generation == generation && t.phase == TimerPhase::Armed);
        if !fires {
            return Reply::TimerFired(None);
        }
        if let Some(timer) = life.timers.get_mut(timer_key) {
            timer.phase = TimerPhase::Delivered;
        }
        let seq = life.accept();
        Reply::TimerFired(Some(Handle::operation(agent, life.incarnation, seq)))
    }

    /// A step of the running turn; `run` continues past the journal to
    /// replay or dispatch.
    fn step(&mut self, operation: &Handle, call: &StepCall, run: bool) -> Reply {
        let seq = match self.input(operation) {
            Ok((seq, _)) => seq,
            Err(Reply::UnknownOperation) => return Reply::Error(ErrorKind::OwnershipLost),
            Err(reply) => return reply,
        };
        let Some(life) = self.life.as_mut() else {
            return Reply::Error(ErrorKind::AgentGone);
        };
        if life.running != Some(seq) {
            return Reply::Error(ErrorKind::OwnershipLost);
        }
        let session = life.session;
        let open = life
            .input_effects(seq)
            .any(|e| e.phase == EffectPhase::Authorized && e.dispatched_in == Some(session));
        if open {
            return Reply::Error(ErrorKind::IdempotencyConflict);
        }
        let journaled = life
            .steps
            .entry(call.key.clone())
            .or_insert_with(|| StepRec {
                tool: call.tool.clone(),
                version: call.version.clone(),
                input: call.input.clone(),
                completed: None,
            });
        if journaled.tool != call.tool
            || journaled.version != call.version
            || journaled.input != call.input
        {
            life.blocked = true;
            return Reply::Error(ErrorKind::NonDeterministicReplay);
        }
        let completed = journaled.completed.clone();
        let slot = (seq, call.key.clone());
        if !run {
            return Reply::StepJournaled {
                completed: completed.is_some(),
                effect: life.effects.get(&slot).map(|e| EffectView {
                    phase: e.phase,
                    attempt: e.attempt,
                }),
            };
        }
        if let Some(output) = completed {
            return Reply::StepCompleted { output };
        }
        let cancelling = life.inputs.get(&seq).is_some_and(|i| i.cancel_requested);
        let Some(effect) = life.effects.get_mut(&slot) else {
            if cancelling {
                return Reply::Error(ErrorKind::OwnershipLost);
            }
            life.effects.insert(
                slot,
                EffectRec {
                    policy: call.policy,
                    phase: EffectPhase::Authorized,
                    attempt: 1,
                    result: None,
                    dispatched_in: Some(session),
                },
            );
            return Reply::StepDispatched { attempt: 1 };
        };
        match effect.phase {
            EffectPhase::Completed => {
                let output = effect.result.clone().map(|(_, r)| r).unwrap_or_default();
                if let Some(step) = life.steps.get_mut(&call.key) {
                    step.completed = Some(output.clone());
                }
                Reply::StepCompleted { output }
            }
            EffectPhase::InDoubt => Reply::Error(ErrorKind::EffectInDoubt),
            EffectPhase::Authorized if cancelling => Reply::Error(ErrorKind::OwnershipLost),
            EffectPhase::Authorized => match effect.policy {
                RecoveryPolicy::ProviderIdempotency => {
                    effect.dispatched_in = Some(session);
                    Reply::StepDispatched {
                        attempt: effect.attempt,
                    }
                }
                RecoveryPolicy::AtLeastOnce => {
                    effect.attempt += 1;
                    effect.dispatched_in = Some(session);
                    Reply::StepDispatched {
                        attempt: effect.attempt,
                    }
                }
                RecoveryPolicy::Manual => {
                    effect.phase = EffectPhase::InDoubt;
                    Reply::Error(ErrorKind::EffectInDoubt)
                }
            },
        }
    }

    /// The effect of step `step_key` of `operation`'s input, or the reply
    /// refusing a call on it. An unaccepted handle and a step with no
    /// effect record both answer `AgentGone`, unlike `start` and `commit`:
    /// see the choices paragraph in the module docs.
    fn effect_mut(
        &mut self,
        operation: &Handle,
        step_key: &str,
    ) -> Result<(u64, bool, &mut EffectRec), Reply> {
        let seq = match self.input(operation) {
            Ok((seq, _)) => seq,
            Err(Reply::UnknownOperation) => return Err(Reply::Error(ErrorKind::AgentGone)),
            Err(reply) => return Err(reply),
        };
        let life = self.live_mut()?;
        let cancelled = life
            .inputs
            .get(&seq)
            .is_some_and(|i| i.status == OperationStatus::Cancelled);
        life.effects
            .get_mut(&(seq, step_key.to_string()))
            .map(|e| (seq, cancelled, e))
            .ok_or(Reply::Error(ErrorKind::AgentGone))
    }

    fn effect_result(
        &mut self,
        operation: &Handle,
        step_key: &str,
        attempt: u32,
        result: &[u8],
    ) -> Reply {
        let (_, cancelled, effect) = match self.effect_mut(operation, step_key) {
            Ok(found) => found,
            Err(reply) => return reply,
        };
        if attempt == 0 || attempt > effect.attempt {
            return Reply::Error(ErrorKind::OwnershipLost);
        }
        if let Some((settled_by, recorded)) = &effect.result {
            return if *settled_by != attempt {
                Reply::EffectEvidence
            } else if recorded.as_slice() != result {
                Reply::Error(ErrorKind::IdempotencyConflict)
            } else {
                Reply::EffectRecorded
            };
        }
        if attempt < effect.attempt || cancelled {
            return Reply::EffectEvidence;
        }
        effect.phase = EffectPhase::Completed;
        effect.result = Some((attempt, result.to_vec()));
        Reply::EffectRecorded
    }

    fn reconcile_attach(&mut self, operation: &Handle, step_key: &str, result: &[u8]) -> Reply {
        let attempt = match self.effect_mut(operation, step_key) {
            Ok((_, _, effect)) => match (effect.phase, &effect.result) {
                (EffectPhase::Completed, Some((settled_by, _))) => *settled_by,
                (EffectPhase::InDoubt, _) => effect.attempt,
                _ => return Reply::Error(ErrorKind::OwnershipLost),
            },
            Err(reply) => return reply,
        };
        self.effect_result(operation, step_key, attempt, result)
    }

    fn reconcile_authorize(&mut self, operation: &Handle, step_key: &str, attempt: u32) -> Reply {
        let effect = match self.effect_mut(operation, step_key) {
            Ok((_, _, effect)) => effect,
            Err(reply) => return reply,
        };
        match effect.phase {
            EffectPhase::Authorized if attempt == effect.attempt => {
                Reply::Reconciled { dispatched: false }
            }
            EffectPhase::InDoubt if Some(attempt) == effect.attempt.checked_add(1) => {
                effect.phase = EffectPhase::Authorized;
                effect.attempt = attempt;
                effect.dispatched_in = None;
                Reply::Reconciled { dispatched: true }
            }
            _ => Reply::Error(ErrorKind::OwnershipLost),
        }
    }

    /// The live (created, not tombstoned) incarnation, or the `AgentGone`
    /// reply.
    fn live_mut(&mut self) -> Result<&mut Life, Reply> {
        match self.life.as_mut() {
            Some(life) if !life.tombstoned => Ok(life),
            _ => Err(Reply::Error(ErrorKind::AgentGone)),
        }
    }

    /// Resolve an operation handle against the live incarnation.
    fn input(&self, operation: &Handle) -> Result<(u64, &Input), Reply> {
        let life = match self.life.as_ref() {
            Some(life) if !life.tombstoned => life,
            _ => return Err(Reply::Error(ErrorKind::AgentGone)),
        };
        if operation.agent != self.agent || operation.incarnation != life.incarnation {
            return Err(Reply::Error(ErrorKind::AgentGone));
        }
        let HandleItem::Operation(seq) = operation.item else {
            return Err(Reply::UnknownOperation);
        };
        life.inputs
            .get(&seq)
            .map(|input| (seq, input))
            .ok_or(Reply::UnknownOperation)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::history::{Divergence, History};

    fn machine() -> ReferenceMachine {
        ReferenceMachine::new("support", "ticket-7").expect("valid agent name")
    }

    fn send(request_id: &str, payload: &[u8]) -> Event {
        Event::Client(ClientOp::Send {
            handler: "reply".into(),
            request_id: request_id.into(),
            payload: payload.to_vec(),
        })
    }

    fn client(op: ClientOp) -> Event {
        Event::Client(op)
    }

    fn created(incarnation: u64, created: bool) -> Reply {
        Reply::Created {
            incarnation,
            created,
        }
    }

    fn accepted(operation: Handle, duplicate: bool) -> Reply {
        Reply::Accepted {
            operation,
            duplicate,
        }
    }

    fn committed(status: OperationStatus, state_version: u64) -> Reply {
        Reply::Committed {
            status,
            state_version,
        }
    }

    fn completed(output: &[u8]) -> Reply {
        Reply::Result(Some(Outcome {
            status: OperationStatus::Completed,
            output: Some(output.to_vec()),
            error: None,
        }))
    }

    fn history(steps: Vec<(Event, Reply)>) -> History {
        let mut history = History::default();
        for (event, observed) in steps {
            history.record(event, observed);
        }
        history
    }

    #[test]
    fn reference_machine_rejects_a_history_with_two_terminal_results_for_one_input() {
        let m = machine();
        let op = m.operation(1, 1);

        // One incarnation: the input completes, then a failure for the same
        // input is observed as committed.
        let same_incarnation = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"hi"), accepted(op, false)),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (
                Event::TurnCompleted {
                    operation: op,
                    output: b"ok".to_vec(),
                },
                committed(OperationStatus::Completed, 1),
            ),
            (client(ClientOp::Result { operation: op }), completed(b"ok")),
            (
                Event::TurnFailed {
                    operation: op,
                    error: "boom".into(),
                },
                committed(OperationStatus::Failed, 1),
            ),
        ]);
        assert_eq!(
            same_incarnation.check(m.clone()),
            Err(Box::new(Divergence {
                index: 5,
                event: same_incarnation.calls()[5].event.clone(),
                expected: Reply::Error(ErrorKind::OwnershipLost),
                observed: committed(OperationStatus::Failed, 1),
            }))
        );

        // Across a recreate: the completed input's handle from incarnation 1
        // is shown failing while incarnation 2's first input is running.
        let new_op = m.operation(2, 1);
        let across_recreate = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"hi"), accepted(op, false)),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (
                Event::TurnCompleted {
                    operation: op,
                    output: b"ok".to_vec(),
                },
                committed(OperationStatus::Completed, 1),
            ),
            (
                client(ClientOp::Destroy),
                Reply::Destroyed {
                    incarnation: 1,
                    already: false,
                },
            ),
            (client(ClientOp::Create), created(2, true)),
            (send("r1", b"hi"), accepted(new_op, false)),
            (Event::TurnStarted { operation: new_op }, Reply::TurnStarted),
            (
                Event::TurnFailed {
                    operation: op,
                    error: "boom".into(),
                },
                committed(OperationStatus::Failed, 0),
            ),
        ]);
        assert_eq!(
            across_recreate.check(m),
            Err(Box::new(Divergence {
                index: 8,
                event: across_recreate.calls()[8].event.clone(),
                expected: Reply::Error(ErrorKind::AgentGone),
                observed: committed(OperationStatus::Failed, 0),
            }))
        );
    }

    #[test]
    fn duplicate_send_with_the_same_payload_returns_the_same_handle() {
        let m = machine();
        let op = m.operation(1, 1);
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"hi"), accepted(op, false)),
            (send("r1", b"hi"), accepted(op, true)),
            (
                client(ClientOp::Resolve {
                    handler: "reply".into(),
                    request_id: "r1".into(),
                }),
                Reply::Resolved(Some((op, OperationStatus::Accepted))),
            ),
            (send("r2", b"hi"), accepted(m.operation(1, 2), false)),
        ]);
        assert_eq!(h.check(m), Ok(()));
    }

    #[test]
    fn duplicate_send_with_a_different_payload_conflicts() {
        let m = machine();
        let op = m.operation(1, 1);
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"hi"), accepted(op, false)),
            (
                send("r1", b"bye"),
                Reply::Error(ErrorKind::IdempotencyConflict),
            ),
            // The conflict allocated nothing: the next new id gets seq 2.
            (send("r2", b"bye"), accepted(m.operation(1, 2), false)),
            (send("r1", b"hi"), accepted(op, true)),
        ]);
        assert_eq!(h.check(m), Ok(()));
    }

    #[test]
    fn a_receipt_holds_the_payload_bytes_it_accepted() {
        // Same id, payloads one trailing byte apart: the receipt compares
        // the bytes, so the longer one conflicts and the original still
        // dedupes after the conflict.
        let m = machine();
        let op = m.operation(1, 1);
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"{\"n\":1}"), accepted(op, false)),
            (
                send("r1", b"{\"n\":1} "),
                Reply::Error(ErrorKind::IdempotencyConflict),
            ),
            (send("r1", b"{\"n\":1}"), accepted(op, true)),
        ]);
        assert_eq!(h.check(m.clone()), Ok(()));

        let mut probe = m;
        probe.apply(&client(ClientOp::Create));
        probe.apply(&send("r1", b"{\"n\":1}"));
        let receipts = &probe.life.as_ref().expect("created").receipts;
        assert_eq!(
            receipts.get(&("reply".to_string(), "r1".to_string())),
            Some(&(b"{\"n\":1}".to_vec(), 1))
        );
    }

    #[test]
    fn completion_committed_before_cancel_wins() {
        let m = machine();
        let op = m.operation(1, 1);
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"hi"), accepted(op, false)),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (
                Event::TurnCompleted {
                    operation: op,
                    output: b"ok".to_vec(),
                },
                committed(OperationStatus::Completed, 1),
            ),
            (
                client(ClientOp::Cancel { operation: op }),
                Reply::Cancel {
                    status: OperationStatus::Completed,
                    requested: false,
                },
            ),
            (client(ClientOp::Result { operation: op }), completed(b"ok")),
            (
                client(ClientOp::State),
                Reply::State {
                    incarnation: 1,
                    state_version: 1,
                },
            ),
        ]);
        assert_eq!(h.check(m), Ok(()));
    }

    #[test]
    fn cancel_committed_before_completion_wins() {
        let m = machine();
        let op = m.operation(1, 1);
        let cancelled = Reply::Cancel {
            status: OperationStatus::Running,
            requested: true,
        };
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"hi"), accepted(op, false)),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (
                client(ClientOp::Cancel { operation: op }),
                cancelled.clone(),
            ),
            // Idempotent while the turn still runs.
            (client(ClientOp::Cancel { operation: op }), cancelled),
            (
                Event::TurnCompleted {
                    operation: op,
                    output: b"ok".to_vec(),
                },
                committed(OperationStatus::Cancelled, 0),
            ),
            (
                client(ClientOp::Result { operation: op }),
                Reply::Result(Some(Outcome {
                    status: OperationStatus::Cancelled,
                    output: None,
                    error: None,
                })),
            ),
            (
                client(ClientOp::Cancel { operation: op }),
                Reply::Cancel {
                    status: OperationStatus::Cancelled,
                    requested: false,
                },
            ),
            (
                client(ClientOp::State),
                Reply::State {
                    incarnation: 1,
                    state_version: 0,
                },
            ),
        ]);
        assert_eq!(h.check(m), Ok(()));
    }

    #[test]
    fn cancelling_a_queued_input_ends_it_and_the_next_input_runs() {
        let m = machine();
        let first = m.operation(1, 1);
        let second = m.operation(1, 2);
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"a"), accepted(first, false)),
            (send("r2", b"b"), accepted(second, false)),
            // FIFO: the second input cannot start ahead of the first.
            (
                Event::TurnStarted { operation: second },
                Reply::Error(ErrorKind::OwnershipLost),
            ),
            (
                client(ClientOp::Cancel { operation: first }),
                Reply::Cancel {
                    status: OperationStatus::Cancelled,
                    requested: false,
                },
            ),
            (
                Event::TurnStarted { operation: first },
                Reply::Error(ErrorKind::OwnershipLost),
            ),
            (Event::TurnStarted { operation: second }, Reply::TurnStarted),
            (
                client(ClientOp::Operation { operation: second }),
                Reply::Status(OperationStatus::Running),
            ),
        ]);
        assert_eq!(h.check(m), Ok(()));
    }

    #[test]
    fn timer_fire_then_cancel_reports_already_fired() {
        let m = machine();
        let fired = m.operation(1, 1);
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (
                client(ClientOp::SetTimer {
                    timer_key: "nudge".into(),
                    handler: "remind".into(),
                    payload: b"x".to_vec(),
                }),
                Reply::TimerSet {
                    timer: m.timer(1, "nudge"),
                    generation: 1,
                },
            ),
            (
                Event::TimerDue {
                    timer_key: "nudge".into(),
                    generation: 1,
                },
                Reply::TimerFired(Some(fired)),
            ),
            // A duplicated hint for the delivered generation enqueues nothing.
            (
                Event::TimerDue {
                    timer_key: "nudge".into(),
                    generation: 1,
                },
                Reply::TimerFired(None),
            ),
            (
                client(ClientOp::CancelTimer {
                    timer_key: "nudge".into(),
                }),
                Reply::TimerCancel(TimerCancelOutcome::AlreadyFired),
            ),
            (
                client(ClientOp::Operation { operation: fired }),
                Reply::Status(OperationStatus::Accepted),
            ),
        ]);
        assert_eq!(h.check(m), Ok(()));
    }

    #[test]
    fn timer_cancel_then_fire_hint_fires_nothing() {
        let m = machine();
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (
                client(ClientOp::SetTimer {
                    timer_key: "nudge".into(),
                    handler: "remind".into(),
                    payload: b"x".to_vec(),
                }),
                Reply::TimerSet {
                    timer: m.timer(1, "nudge"),
                    generation: 1,
                },
            ),
            (
                client(ClientOp::CancelTimer {
                    timer_key: "nudge".into(),
                }),
                Reply::TimerCancel(TimerCancelOutcome::Cancelled),
            ),
            (
                Event::TimerDue {
                    timer_key: "nudge".into(),
                    generation: 1,
                },
                Reply::TimerFired(None),
            ),
            // Re-arming takes a new generation; the old one stays dead.
            (
                client(ClientOp::SetTimer {
                    timer_key: "nudge".into(),
                    handler: "remind".into(),
                    payload: b"x".to_vec(),
                }),
                Reply::TimerSet {
                    timer: m.timer(1, "nudge"),
                    generation: 2,
                },
            ),
            (
                Event::TimerDue {
                    timer_key: "nudge".into(),
                    generation: 1,
                },
                Reply::TimerFired(None),
            ),
            (
                Event::TimerDue {
                    timer_key: "nudge".into(),
                    generation: 2,
                },
                Reply::TimerFired(Some(m.operation(1, 1))),
            ),
        ]);
        assert_eq!(h.check(m), Ok(()));
    }

    #[test]
    fn recreate_after_destroy_refuses_the_old_handle() {
        let m = machine();
        let old = m.operation(1, 1);
        let new = m.operation(2, 1);
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (client(ClientOp::Create), created(1, false)),
            (send("r1", b"hi"), accepted(old, false)),
            (
                client(ClientOp::Destroy),
                Reply::Destroyed {
                    incarnation: 1,
                    already: false,
                },
            ),
            (
                client(ClientOp::Operation { operation: old }),
                Reply::Error(ErrorKind::AgentGone),
            ),
            (send("r1", b"hi"), Reply::Error(ErrorKind::AgentGone)),
            (client(ClientOp::Create), created(2, true)),
            (
                client(ClientOp::Operation { operation: old }),
                Reply::Error(ErrorKind::AgentGone),
            ),
            (
                client(ClientOp::Cancel { operation: old }),
                Reply::Error(ErrorKind::AgentGone),
            ),
            // Receipts are per incarnation: the same request id is new here.
            (send("r1", b"hi"), accepted(new, false)),
            (
                Event::TurnStarted { operation: old },
                Reply::Error(ErrorKind::AgentGone),
            ),
            (
                client(ClientOp::Operation { operation: new }),
                Reply::Status(OperationStatus::Accepted),
            ),
        ]);
        assert_eq!(h.check(m), Ok(()));
    }

    #[test]
    fn destroy_before_any_create_is_agent_gone_and_a_repeat_is_idempotent() {
        let h = history(vec![
            (
                client(ClientOp::Destroy),
                Reply::Error(ErrorKind::AgentGone),
            ),
            (client(ClientOp::Create), created(1, true)),
            (
                client(ClientOp::Destroy),
                Reply::Destroyed {
                    incarnation: 1,
                    already: false,
                },
            ),
            (
                client(ClientOp::Destroy),
                Reply::Destroyed {
                    incarnation: 1,
                    already: true,
                },
            ),
            (client(ClientOp::State), Reply::Error(ErrorKind::AgentGone)),
            (client(ClientOp::Create), created(2, true)),
        ]);
        assert_eq!(h.check(machine()), Ok(()));
    }

    #[test]
    fn choices_the_adr_leaves_open_behave_as_documented() {
        let m = machine();
        let op = m.operation(1, 1);
        let h = history(vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"hi"), accepted(op, false)),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (
                client(ClientOp::Cancel { operation: op }),
                Reply::Cancel {
                    status: OperationStatus::Running,
                    requested: true,
                },
            ),
            // A failure committed after the cancel request also ends cancelled.
            (
                Event::TurnFailed {
                    operation: op,
                    error: "boom".into(),
                },
                committed(OperationStatus::Cancelled, 0),
            ),
            (
                client(ClientOp::Operation {
                    operation: m.operation(1, 9),
                }),
                Reply::UnknownOperation,
            ),
            (
                client(ClientOp::SetTimer {
                    timer_key: "nudge".into(),
                    handler: "remind".into(),
                    payload: Vec::new(),
                }),
                Reply::TimerSet {
                    timer: m.timer(1, "nudge"),
                    generation: FIRST_TIMER_GENERATION,
                },
            ),
            (
                client(ClientOp::Destroy),
                Reply::Destroyed {
                    incarnation: 1,
                    already: false,
                },
            ),
            (client(ClientOp::Create), created(2, true)),
            // A new incarnation keeps no timers and starts at state version 0.
            (
                client(ClientOp::CancelTimer {
                    timer_key: "nudge".into(),
                }),
                Reply::TimerCancel(TimerCancelOutcome::Unknown),
            ),
            (
                client(ClientOp::State),
                Reply::State {
                    incarnation: 2,
                    state_version: 0,
                },
            ),
            (
                client(ClientOp::Destroy),
                Reply::Destroyed {
                    incarnation: 2,
                    already: false,
                },
            ),
            (
                client(ClientOp::Destroy),
                Reply::Destroyed {
                    incarnation: 2,
                    already: true,
                },
            ),
        ]);
        assert_eq!(h.check(m), Ok(()));
    }

    fn call(policy: RecoveryPolicy) -> StepCall {
        StepCall {
            key: "lookup".into(),
            tool: "crm".into(),
            version: "v1".into(),
            input: b"q".to_vec(),
            policy,
        }
    }

    fn step(operation: Handle, policy: RecoveryPolicy) -> Event {
        Event::Step {
            operation,
            step: call(policy),
        }
    }

    fn result(operation: Handle, attempt: u32, bytes: &[u8]) -> Event {
        Event::EffectResult {
            operation,
            step_key: "lookup".into(),
            attempt,
            result: bytes.to_vec(),
        }
    }

    fn refused(kind: ErrorKind) -> Reply {
        Reply::Error(kind)
    }

    /// Create, send `r1` and start its turn.
    fn running(op: Handle) -> Vec<(Event, Reply)> {
        vec![
            (client(ClientOp::Create), created(1, true)),
            (send("r1", b"hi"), accepted(op, false)),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
        ]
    }

    #[test]
    fn at_least_once_dispatches_once_per_session_and_records_one_result() {
        let m = machine();
        let op = m.operation(1, 1);
        let alo = RecoveryPolicy::AtLeastOnce;
        let mut steps = running(op);
        steps.extend([
            (step(op, alo), Reply::StepDispatched { attempt: 1 }),
            // The attempt is still open in this session.
            (step(op, alo), refused(ErrorKind::IdempotencyConflict)),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (step(op, alo), Reply::StepDispatched { attempt: 2 }),
            // A superseded attempt reporting first is evidence only.
            (result(op, 1, b"old"), Reply::EffectEvidence),
            (result(op, 3, b"new"), refused(ErrorKind::OwnershipLost)),
            (result(op, 2, b"new"), Reply::EffectRecorded),
            (result(op, 2, b"new"), Reply::EffectRecorded),
            (
                result(op, 2, b"other"),
                refused(ErrorKind::IdempotencyConflict),
            ),
            (result(op, 1, b"old"), Reply::EffectEvidence),
            (
                step(op, alo),
                Reply::StepCompleted {
                    output: b"new".to_vec(),
                },
            ),
            (
                Event::StepStarted {
                    operation: op,
                    step: call(alo),
                },
                Reply::StepJournaled {
                    completed: true,
                    effect: Some(EffectView {
                        phase: EffectPhase::Completed,
                        attempt: 2,
                    }),
                },
            ),
        ]);
        assert_eq!(history(steps).check(m), Ok(()));
    }

    #[test]
    fn provider_idempotency_redispatches_the_same_attempt() {
        let m = machine();
        let op = m.operation(1, 1);
        let pi = RecoveryPolicy::ProviderIdempotency;
        let mut steps = running(op);
        steps.extend([
            (step(op, pi), Reply::StepDispatched { attempt: 1 }),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (step(op, pi), Reply::StepDispatched { attempt: 1 }),
        ]);
        assert_eq!(history(steps.clone()).check(machine()), Ok(()));
        let last = steps.len() - 1;
        steps[last].1 = Reply::StepDispatched { attempt: 2 };
        assert!(history(steps).check(m).is_err());
    }

    #[test]
    fn manual_goes_in_doubt_until_an_operator_authorizes_the_next_attempt() {
        let m = machine();
        let op = m.operation(1, 1);
        let manual = RecoveryPolicy::Manual;
        let authorize = |attempt| Event::ReconcileAuthorize {
            operation: op,
            step_key: "lookup".into(),
            attempt,
        };
        let mut steps = running(op);
        steps.extend([
            (step(op, manual), Reply::StepDispatched { attempt: 1 }),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (step(op, manual), refused(ErrorKind::EffectInDoubt)),
            (step(op, manual), refused(ErrorKind::EffectInDoubt)),
            (authorize(3), refused(ErrorKind::OwnershipLost)),
            (authorize(2), Reply::Reconciled { dispatched: true }),
            (authorize(2), Reply::Reconciled { dispatched: false }),
            (result(op, 1, b"late"), Reply::EffectEvidence),
            (result(op, 2, b"done"), Reply::EffectRecorded),
            (
                step(op, manual),
                Reply::StepCompleted {
                    output: b"done".to_vec(),
                },
            ),
        ]);
        assert_eq!(history(steps).check(m), Ok(()));
    }

    #[test]
    fn manual_redispatch_after_a_new_session_diverges() {
        let m = machine();
        let op = m.operation(1, 1);
        let manual = RecoveryPolicy::Manual;
        let mut steps = running(op);
        steps.extend([
            (step(op, manual), Reply::StepDispatched { attempt: 1 }),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (step(op, manual), Reply::StepDispatched { attempt: 2 }),
        ]);
        assert!(history(steps).check(m).is_err());
    }

    #[test]
    fn reconcile_attach_settles_an_effect_in_doubt() {
        let m = machine();
        let op = m.operation(1, 1);
        let manual = RecoveryPolicy::Manual;
        let attach = |bytes: &[u8]| Event::ReconcileAttach {
            operation: op,
            step_key: "lookup".into(),
            result: bytes.to_vec(),
        };
        let mut steps = running(op);
        steps.extend([
            (step(op, manual), Reply::StepDispatched { attempt: 1 }),
            // Attach needs an effect in doubt or already settled.
            (attach(b"v"), refused(ErrorKind::OwnershipLost)),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (step(op, manual), refused(ErrorKind::EffectInDoubt)),
            (attach(b"v"), Reply::EffectRecorded),
            (attach(b"v"), Reply::EffectRecorded),
            (attach(b"w"), refused(ErrorKind::IdempotencyConflict)),
            (
                step(op, manual),
                Reply::StepCompleted {
                    output: b"v".to_vec(),
                },
            ),
        ]);
        assert_eq!(history(steps).check(m), Ok(()));
    }

    #[test]
    fn a_requested_cancel_stops_new_dispatches() {
        let m = machine();
        let op = m.operation(1, 1);
        let alo = RecoveryPolicy::AtLeastOnce;
        let other = Event::Step {
            operation: op,
            step: StepCall {
                key: "notify".into(),
                ..call(alo)
            },
        };
        let mut steps = running(op);
        steps.extend([
            (step(op, alo), Reply::StepDispatched { attempt: 1 }),
            (
                client(ClientOp::Cancel { operation: op }),
                Reply::Cancel {
                    status: OperationStatus::Running,
                    requested: true,
                },
            ),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (step(op, alo), refused(ErrorKind::OwnershipLost)),
            (other, refused(ErrorKind::OwnershipLost)),
            // The open attempt may still report.
            (result(op, 1, b"r"), Reply::EffectRecorded),
            (
                step(op, alo),
                Reply::StepCompleted {
                    output: b"r".to_vec(),
                },
            ),
        ]);
        assert_eq!(history(steps).check(m), Ok(()));
    }

    /// Each effect event on `operation`'s step `step_key`.
    fn effect_events(operation: Handle, step_key: &str) -> [Event; 3] {
        [
            Event::EffectResult {
                operation,
                step_key: step_key.into(),
                attempt: 1,
                result: b"r".to_vec(),
            },
            Event::ReconcileAttach {
                operation,
                step_key: step_key.into(),
                result: b"r".to_vec(),
            },
            Event::ReconcileAuthorize {
                operation,
                step_key: step_key.into(),
                attempt: 2,
            },
        ]
    }

    /// Check `steps` followed by each effect event answering `AgentGone`,
    /// and that answering `OwnershipLost` instead diverges.
    fn effect_events_answer_agent_gone(steps: &[(Event, Reply)], events: [Event; 3]) {
        for event in events {
            let mut with = steps.to_vec();
            with.push((event.clone(), refused(ErrorKind::AgentGone)));
            assert_eq!(history(with).check(machine()), Ok(()), "{event:?}");
            let mut wrong = steps.to_vec();
            wrong.push((event.clone(), refused(ErrorKind::OwnershipLost)));
            assert!(history(wrong).check(machine()).is_err(), "{event:?}");
        }
    }

    #[test]
    fn an_effect_event_on_a_handle_never_accepted_is_agent_gone() {
        let m = machine();
        let op = m.operation(1, 1);
        let mut steps = running(op);
        steps.push((
            step(op, RecoveryPolicy::Manual),
            Reply::StepDispatched { attempt: 1 },
        ));
        effect_events_answer_agent_gone(&steps, effect_events(m.operation(1, 9), "lookup"));
    }

    #[test]
    fn an_effect_event_on_a_step_with_no_effect_record_is_agent_gone() {
        let m = machine();
        let op = m.operation(1, 1);
        let mut steps = running(op);
        steps.push((
            step(op, RecoveryPolicy::Manual),
            Reply::StepDispatched { attempt: 1 },
        ));
        effect_events_answer_agent_gone(&steps, effect_events(op, "notify"));
    }

    #[test]
    fn a_changed_step_call_blocks_the_agent() {
        let m = machine();
        let op = m.operation(1, 1);
        let alo = RecoveryPolicy::AtLeastOnce;
        let changed = Event::Step {
            operation: op,
            step: StepCall {
                input: b"other".to_vec(),
                ..call(alo)
            },
        };
        let mut steps = running(op);
        steps.extend([
            (step(op, alo), Reply::StepDispatched { attempt: 1 }),
            (Event::TurnStarted { operation: op }, Reply::TurnStarted),
            (changed, refused(ErrorKind::NonDeterministicReplay)),
        ]);
        assert_eq!(history(steps).check(m), Ok(()));
    }

    mod invariants {
        use super::*;
        use proptest::prelude::*;
        use std::collections::HashSet;

        const REQUEST_IDS: [&str; 3] = ["r0", "r1", "r2"];
        const TIMER_KEYS: [&str; 2] = ["t0", "t1"];

        fn handle() -> impl Strategy<Value = Handle> {
            let agent = machine().agent_id();
            // Bias toward the live incarnation's first inputs so turns are
            // started and committed often enough to exercise the commit
            // rules; the uniform tail keeps stale handles in the mix.
            prop_oneof![
                4 => Just((1u64, 1u64)),
                3 => Just((1u64, 2u64)),
                2 => Just((1u64, 3u64)),
                2 => Just((2u64, 1u64)),
                1 => (1u64..=3, 1u64..=5),
            ]
            .prop_map(move |(inc, seq)| Handle::operation(agent, inc, seq))
        }

        /// Every generated history starts with a Create, so the rest of the
        /// events act on a live agent often enough to reach turn commits
        /// (`the_property_generator_reaches_commits` pins the rate).
        fn events() -> impl Strategy<Value = Vec<Event>> {
            prop::collection::vec(event(), 0..48).prop_map(|mut events| {
                events.insert(0, Event::Client(ClientOp::Create));
                events
            })
        }

        fn event() -> impl Strategy<Value = Event> {
            let ids = prop::sample::select(REQUEST_IDS.to_vec());
            let keys = prop::sample::select(TIMER_KEYS.to_vec());
            prop_oneof![
                1 => Just(Event::Client(ClientOp::Create)),
                1 => Just(Event::Client(ClientOp::Destroy)),
                6 => (ids.clone(), 0u8..2).prop_map(|(id, p)| send(id, &[p])),
                2 => handle().prop_map(|operation| Event::Client(ClientOp::Operation { operation })),
                2 => handle().prop_map(|operation| Event::Client(ClientOp::Result { operation })),
                1 => ids.prop_map(|id| Event::Client(ClientOp::Resolve {
                    handler: "reply".into(),
                    request_id: id.into(),
                })),
                3 => handle().prop_map(|operation| Event::Client(ClientOp::Cancel { operation })),
                1 => Just(Event::Client(ClientOp::State)),
                2 => keys.clone().prop_map(|k| Event::Client(ClientOp::SetTimer {
                    timer_key: k.into(),
                    handler: "remind".into(),
                    payload: Vec::new(),
                })),
                1 => keys.clone().prop_map(|k| Event::Client(ClientOp::CancelTimer {
                    timer_key: k.into(),
                })),
                2 => (keys, 0u64..4).prop_map(|(k, generation)| Event::TimerDue {
                    timer_key: k.into(),
                    generation,
                }),
                6 => handle().prop_map(|operation| Event::TurnStarted { operation }),
                4 => handle().prop_map(|operation| Event::TurnCompleted {
                    operation,
                    output: b"ok".to_vec(),
                }),
                3 => handle().prop_map(|operation| Event::TurnFailed {
                    operation,
                    error: "boom".into(),
                }),
            ]
        }

        fn terminal_of(reply: &Reply) -> Option<OperationStatus> {
            let status = match reply {
                Reply::Status(s) | Reply::Cancel { status: s, .. } => *s,
                Reply::Committed { status, .. } => *status,
                Reply::Result(Some(outcome)) => outcome.status,
                _ => return None,
            };
            is_terminal(status).then_some(status)
        }

        fn subject(event: &Event) -> Option<Handle> {
            match event {
                Event::Client(
                    ClientOp::Operation { operation }
                    | ClientOp::Result { operation }
                    | ClientOp::Cancel { operation },
                )
                | Event::TurnStarted { operation }
                | Event::TurnCompleted { operation, .. }
                | Event::TurnFailed { operation, .. } => Some(*operation),
                _ => None,
            }
        }

        #[test]
        fn timer_generations_are_counted_per_agent_not_per_key() {
            let m = machine();
            let h = history(vec![
                (client(ClientOp::Create), created(1, true)),
                (
                    client(ClientOp::SetTimer {
                        timer_key: "t0".into(),
                        handler: "remind".into(),
                        payload: Vec::new(),
                    }),
                    Reply::TimerSet {
                        timer: m.timer(1, "t0"),
                        generation: 1,
                    },
                ),
                (
                    client(ClientOp::SetTimer {
                        timer_key: "t1".into(),
                        handler: "remind".into(),
                        payload: Vec::new(),
                    }),
                    Reply::TimerSet {
                        timer: m.timer(1, "t1"),
                        generation: 2,
                    },
                ),
                (
                    client(ClientOp::SetTimer {
                        timer_key: "t0".into(),
                        handler: "remind".into(),
                        payload: Vec::new(),
                    }),
                    Reply::TimerSet {
                        timer: m.timer(1, "t0"),
                        generation: 3,
                    },
                ),
            ]);
            h.check(machine())
                .expect("one generation counter per agent");
        }

        #[test]
        fn a_new_incarnation_starts_at_state_version_zero_after_a_completion() {
            let m = machine();
            let op = m.operation(1, 1);
            let h = history(vec![
                (client(ClientOp::Create), created(1, true)),
                (send("r1", b"hi"), accepted(op, false)),
                (Event::TurnStarted { operation: op }, Reply::TurnStarted),
                (
                    Event::TurnCompleted {
                        operation: op,
                        output: b"ok".to_vec(),
                    },
                    committed(OperationStatus::Completed, 1),
                ),
                (
                    client(ClientOp::State),
                    Reply::State {
                        incarnation: 1,
                        state_version: 1,
                    },
                ),
                (
                    client(ClientOp::Destroy),
                    Reply::Destroyed {
                        incarnation: 1,
                        already: false,
                    },
                ),
                (client(ClientOp::Create), created(2, true)),
                (
                    client(ClientOp::State),
                    Reply::State {
                        incarnation: 2,
                        state_version: 0,
                    },
                ),
            ]);
            h.check(machine())
                .expect("state version resets with the incarnation");
        }

        #[test]
        fn the_property_generator_reaches_commits() {
            use proptest::strategy::ValueTree;
            use proptest::test_runner::TestRunner;
            let mut runner = TestRunner::deterministic();
            let strategy = events();
            let mut commits = 0usize;
            for _ in 0..1_000 {
                let events = strategy.new_tree(&mut runner).expect("strategy").current();
                let mut m = machine();
                for event in &events {
                    if matches!(m.apply(event), Reply::Committed { .. }) {
                        commits += 1;
                    }
                }
            }
            assert!(
                commits >= 200,
                "the generator must reach commits often: {commits} in 1000 cases"
            );
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(5_000))]

            #[test]
            fn reference_machine_invariants_hold(events in events()) {
                let mut m = machine();
                let mut terminal: HashMap<Handle, OperationStatus> = HashMap::new();
                let mut state_version: HashMap<u64, u64> = HashMap::new();
                let mut next_seq: HashMap<u64, u64> = HashMap::new();
                let mut last_started: HashMap<u64, u64> = HashMap::new();
                for (index, event) in events.iter().enumerate() {
                    let incarnation = m.incarnation();
                    let reply = m.apply(event);

                    // A handle from any other incarnation is refused.
                    if let Some(handle) = subject(event)
                        && Some(handle.incarnation) != incarnation
                    {
                        prop_assert_eq!(&reply, &Reply::Error(ErrorKind::AgentGone));
                    }

                    // One terminal result per input: once any reply shows a
                    // terminal status for a handle, every later one agrees.
                    if let (Some(handle), Some(status)) = (subject(event), terminal_of(&reply)) {
                        let first = *terminal.entry(handle).or_insert(status);
                        prop_assert_eq!(first, status, "event {} {:?}", index, event);
                    }

                    // The state version moves only on a completed commit,
                    // by exactly one, and only within an incarnation.
                    match &reply {
                        Reply::State { incarnation, state_version: sv } => {
                            prop_assert_eq!(*state_version.entry(*incarnation).or_insert(0), *sv);
                        }
                        Reply::Committed { status, state_version: sv } => {
                            let inc = incarnation.expect("a commit needs an agent");
                            let known = state_version.entry(inc).or_insert(0);
                            if *status == OperationStatus::Completed {
                                *known += 1;
                            }
                            prop_assert_eq!(*known, *sv, "event {} {:?}", index, event);
                        }
                        _ => {}
                    }

                    // Accept order is dense per incarnation.
                    let fresh = match &reply {
                        Reply::Accepted { operation, duplicate: false } => Some(*operation),
                        Reply::TimerFired(Some(operation)) => Some(*operation),
                        _ => None,
                    };
                    if let Some(op) = fresh {
                        let next = next_seq.entry(op.incarnation).or_insert(FIRST_ACCEPT_SEQ);
                        prop_assert_eq!(op.item, HandleItem::Operation(*next));
                        *next += 1;
                    }

                    // Turns start in accept order.
                    if let (Reply::TurnStarted, Some(op)) = (&reply, subject(event)) {
                        let HandleItem::Operation(seq) = op.item else {
                            return Err(TestCaseError::fail("non-operation start"));
                        };
                        let last = last_started.entry(op.incarnation).or_insert(0);
                        prop_assert!(seq >= *last, "start of {} after {}", seq, *last);
                        *last = seq;
                    }
                }
                let log = m.terminal_log();
                let distinct: HashSet<&Handle> = log.iter().map(|(h, _)| h).collect();
                prop_assert_eq!(distinct.len(), log.len(), "terminal log {:?}", log);
                for (handle, status) in log {
                    if let Some(seen) = terminal.get(handle) {
                        prop_assert_eq!(seen, status);
                    }
                }
            }
        }
    }
}
