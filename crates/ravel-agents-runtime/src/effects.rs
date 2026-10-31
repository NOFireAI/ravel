//! The step and effect journal, coordinator side (ADR-3331 decision 7).
//!
//! A leased worker runs one tool call through [`Coordinator::run_step`]:
//! [`Coordinator::record_step`] journals the step started, or returns the
//! record HEAD already holds; [`Coordinator::commit_intent`] and
//! [`Coordinator::authorize_attempt`] are the two CASes that precede every
//! dispatch; and the attempt is then handed to the caller's
//! [`EffectDispatcher`]. No other path in this crate calls the dispatcher, so
//! every dispatch names an attempt HEAD records as authorized under a
//! committed intent
//! (`tests::provider_idempotency_redispatches_the_same_effect_at_every_crash_point`
//! counts both CASes ahead of the first dispatch).
//!
//! A step journaled with a different tool name, tool version or input hash
//! under a reused key blocks the turn `NonDeterministicReplay`, and nothing is
//! dispatched (`tests::drift_in_tool_version_or_input_hash_blocks_the_turn_before_dispatch`).
//! A completed step replays from its record with no write
//! (`tests::a_completed_step_replays_without_a_new_intent_or_attempt`).
//!
//! Steps are sequential: while an attempt this coordinator dispatched under
//! the caller's ownership epoch for the active turn's input has no result,
//! another `record_step` under that epoch is refused `IdempotencyConflict`
//! (`tests::a_second_step_while_an_attempt_is_open_is_refused_typed`). That
//! record is kept in memory only, and is dropped when HEAD shows its input
//! settled. A turn whose input was cancelled settles with its attempt still
//! open; HEAD keeps that attempt `Authorized`, as evidence that the tool was
//! dispatched, its late result is stored as evidence without changing HEAD,
//! and the next input's steps run under the same epoch
//! (`tests::a_cancelled_turn_with_an_open_attempt_does_not_wedge_the_next_turn`).
//! An authorized attempt this coordinator did not dispatch under the
//! caller's epoch (a takeover, or a coordinator restart) is a lost attempt,
//! and the step's recovery policy decides:
//!
//! | Policy | First dispatch | After a lost attempt | Result of the lost attempt |
//! |---|---|---|---|
//! | `provider_idempotency` | attempt 1 (`tests::provider_idempotency_redispatches_the_same_effect_at_every_crash_point`) | the same effect id and attempt again, no write (same test) | recorded once; the re-dispatch's equal result answers without a write (same test) |
//! | `at_least_once` | attempt 1 (`tests::at_least_once_authorizes_a_new_attempt_at_every_crash_point`) | a newly authorized attempt (same test) | evidence; only the newest attempt's result is the step's (same test) |
//! | `manual` | attempt 1 (`tests::manual_policy_attempt_with_lost_result_blocks_as_effect_in_doubt_and_is_never_redispatched`) | `EffectInDoubt`, never dispatched (same test) | settled only by reconciliation (same test) |
//!
//! [`Coordinator::complete_effect`] is the result-completion path: it takes
//! the attempt the dispatcher was handed, not a lease, so a result lands
//! after the worker that dispatched it died. It records only that effect's
//! result; the owner's next `run_step` journals the step completed from it.
//! A result for an attempt a retry superseded, or for an effect whose input
//! ended cancelled, is evidence: its bytes are stored and named in
//! [`EffectSettled::Evidence`], and HEAD does not change
//! (`tests::late_results_after_takeover_and_after_cancel_are_evidence`). A
//! second, different result for the same attempt is `IdempotencyConflict`
//! (same test). [`Coordinator::fail_effect`] records a failure as the
//! attempt's result, so the turn can then fail: the step stays started, and
//! its replay answers [`StepProgress::Failed`]
//! (`tests::fail_effect_records_the_failure_so_the_turn_can_fail`). A failure
//! is typed by its result object, which starts with
//! [`FAILURE_RESULT_PREFIX`]; a JSON output never does, so an output of any
//! shape replays as an output
//! (`tests::an_output_shaped_like_a_failure_replays_as_an_output`).
//!
//! While the input is being cancelled no attempt is dispatched, and a
//! completion settles the input cancelled
//! (`tests::a_new_attempt_for_a_cancelled_input_is_refused_and_the_completion_settles_cancelled`).
//!
//! Reconciliation of an effect in doubt is [`Coordinator::reconcile_attach`]
//! (attach a verified result) or [`Coordinator::reconcile_authorize`]
//! (authorize another attempt and hand it over). Neither dispatches: an
//! authorized attempt is dispatched once by the next `run_step` for the
//! effect's step through this coordinator, which is not refused for that
//! attempt being open
//! (`tests::a_reconciled_attempt_is_dispatched_by_the_owners_next_step`),
//! whichever owner runs it, so a takeover in between does not lose it
//! (`tests::a_reconciled_attempt_survives_a_takeover_before_the_next_step`).
//! Two steps racing for it dispatch it once
//! (`tests::concurrent_steps_dispatch_a_reconciled_attempt_once`,
//! `tests::a_step_that_passed_the_open_check_before_the_claim_is_refused_after_it`),
//! and a step refused after claiming it hands it back
//! (`tests::a_hand_over_refused_after_its_claim_is_released_and_never_dispatched_twice`,
//! `tests::a_hand_over_whose_fresh_read_fails_is_dispatched_by_the_next_step`),
//! including a step whose turn another owner took over in between
//! (`tests::a_hand_over_claimed_by_an_owner_that_lost_the_turn_goes_to_the_new_owner`).
//! The hand-over is held in this coordinator's memory, so a step that
//! reaches another coordinator, or one after a restart, finds the attempt
//! lost and the policy decides (a `manual` effect latches in doubt again).
//! Each is idempotent, and the kernel appends one audit event to the
//! effect's turn's event stream for each one that changes HEAD
//! (`tests::reconcile_is_idempotent_and_audited_once`).
//!
//! A reconcile hands an attempt over only when this call's own HEAD write
//! was acknowledged. Two reconciles starting from the same HEAD write the
//! same bytes, so after an unknown outcome HEAD cannot say whose write
//! landed: the call answers `OutcomeUnknown` and hands nothing over, and the
//! attempt is dispatched at most once across coordinators
//! (`tests::concurrent_reconciles_on_two_coordinators_dispatch_the_attempt_at_most_once`,
//! `tests::a_reconcile_authorization_with_a_lost_ack_is_not_handed_over_and_latches_in_doubt_again`).
//! A reconcile whose write lost the race with a known outcome answers
//! `false` and hands nothing over
//! (`tests::a_reconcile_another_admin_landed_first_is_not_dispatched_again`).
//! A claimed hand-over whose agent was destroyed in the meantime is refused
//! `AgentGone` and dropped, never dispatched
//! (`tests::a_hand_over_claimed_after_a_destroy_on_another_coordinator_is_not_dispatched`).
//! The `agents_admin` capability check belongs to the route that calls them.
//!
//! A command whose HEAD write outcome is unknown runs again, up to
//! [`UNKNOWN_OUTCOME_RETRIES`] times; every effect command is answered
//! without a write when it already applied
//! (`tests::an_effect_result_with_a_lost_ack_lands_once`).
//!
//! Reachability: `ravel-server`'s worker, effect and reconcile routes call
//! this module; so do this module's tests.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use ravel_agents_core::command::{
    Authority, AuthorizeAttempt, Command, CommitIntent, IncarnationFence, IntentSpec,
    RecordEffectResult, RecordStep, TurnFence,
};
use ravel_agents_core::error::{AgentError, ErrorKind};
use ravel_agents_core::head::{
    Cancellation, EffectRecord, EffectStatus, Head, Lifecycle, ObjectRef, RecoveryPolicy,
    StepRecord, StepStatus,
};
use ravel_agents_core::identity::{AgentId, Hash256, Id128, effect_id};
use ravel_agents_core::kernel::Reply;
use ravel_agents_core::wire::TurnLease;
use ravel_agents_store::Outcome;
use serde_json::Value;

use crate::{Coordinator, fence_from_lease, json_bytes, refused};

/// How many times an effect command whose outcome is unknown runs again.
pub const UNKNOWN_OUTCOME_RETRIES: u32 = 3;

/// One tool call a handler makes through `ctx.step`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepSpec {
    pub step_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub policy: RecoveryPolicy,
    /// `identity::step_input_hash` over the canonical input.
    pub input_hash: Hash256,
}

/// An effect of one agent incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectRef {
    pub agent: AgentId,
    pub incarnation: u64,
    pub effect_id: Id128,
}

/// One authorized attempt at an effect, as handed to the dispatcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dispatch {
    pub effect: EffectRef,
    pub attempt: u32,
    pub step_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub policy: RecoveryPolicy,
    pub input_hash: Hash256,
}

/// Receives each attempt the coordinator authorized to run. The tool's
/// result comes back through [`Coordinator::complete_effect`] or
/// [`Coordinator::fail_effect`].
pub trait EffectDispatcher: Send + Sync {
    fn dispatch(&self, dispatch: &Dispatch);
}

/// What HEAD journals for a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepJournal {
    pub step: StepRecord,
    pub effect: Option<EffectRecord>,
}

/// Where a step stands after [`Coordinator::run_step`].
#[derive(Debug, Clone, PartialEq)]
pub enum StepProgress {
    /// The step completed; replay its output.
    Completed { output: Value },
    /// The step's effect recorded a tool failure through
    /// [`Coordinator::fail_effect`]; replay the failure. The step stays
    /// journaled started, so a retried turn replays the failure again.
    Failed { error: String },
    /// An attempt was handed to the dispatcher by this call.
    Dispatched(Dispatch),
}

/// The bytes an effect result object starts with when it records a tool
/// failure, followed by the error as UTF-8. A result reported through
/// [`Coordinator::complete_effect`] is JSON text, which never contains a NUL
/// byte, so no output can be read back as a failure.
pub const FAILURE_RESULT_PREFIX: &[u8] = b"\0ravel-agents-tool-failure-v1\0";

/// An effect result object, decoded.
#[derive(Debug, Clone, PartialEq)]
pub enum EffectResult {
    /// What the tool returned, through [`Coordinator::complete_effect`] or a
    /// reconcile.
    Output(Value),
    /// The tool failure [`Coordinator::fail_effect`] recorded.
    Failure(String),
}

impl EffectResult {
    /// Decode a stored result; `None` for bytes that are neither form.
    pub fn decode(bytes: &[u8]) -> Option<EffectResult> {
        match bytes.strip_prefix(FAILURE_RESULT_PREFIX) {
            Some(error) => std::str::from_utf8(error)
                .ok()
                .map(|e| EffectResult::Failure(e.to_string())),
            None => serde_json::from_slice(bytes).ok().map(EffectResult::Output),
        }
    }

    fn failure_bytes(error: &str) -> Bytes {
        let mut bytes = Vec::with_capacity(FAILURE_RESULT_PREFIX.len() + error.len());
        bytes.extend_from_slice(FAILURE_RESULT_PREFIX);
        bytes.extend_from_slice(error.as_bytes());
        Bytes::from(bytes)
    }
}

/// The answer to a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectSettled {
    /// The result is the effect's (now, or already by an equal earlier call).
    Recorded,
    /// The attempt was superseded or its input ended cancelled: the stored
    /// result is kept as evidence and HEAD is unchanged.
    Evidence { result: ObjectRef },
}

/// The attempt this coordinator dispatched under one ownership epoch, for
/// the input `accept_seq` of `incarnation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenAttempt {
    owner_id: String,
    epoch: u64,
    incarnation: u64,
    accept_seq: u64,
    attempt: u32,
    /// Authorized by [`Coordinator::reconcile_authorize`] and not yet
    /// claimed: the next `run_step` for the effect's step under the same
    /// incarnation, whichever owner and epoch it runs under, dispatches it.
    reconciled: bool,
}

impl Coordinator {
    /// Journal the step started, or return what HEAD already journals for
    /// it with its effect, if any. Refused `IdempotencyConflict` while an
    /// attempt dispatched under the lease's epoch has no result, and
    /// `NonDeterministicReplay` (blocking the turn) when the key was
    /// journaled with another tool identity or input hash.
    pub async fn record_step(
        &self,
        lease: &TurnLease,
        spec: &StepSpec,
    ) -> Result<StepJournal, AgentError> {
        Ok(self.journal_step(lease, spec).await?.0)
    }

    /// [`Coordinator::record_step`], also answering the input the step
    /// belongs to.
    async fn journal_step(
        &self,
        lease: &TurnLease,
        spec: &StepSpec,
    ) -> Result<(StepJournal, u64), AgentError> {
        let (agent, fence) = fence_from_lease(lease)?;
        self.refuse_open_attempt(&agent, &fence, &spec.step_key)
            .await?;
        let command = Command::RecordStep(RecordStep {
            fence,
            step_key: spec.step_key.clone(),
            tool_name: spec.tool_name.clone(),
            tool_version: spec.tool_version.clone(),
            input_hash: spec.input_hash,
            status: StepStatus::Started,
            output: None,
        });
        let out = self.execute_effect(&agent, &command, &[]).await?;
        let step = match out.reply {
            Reply::StepRecorded(step) | Reply::StepReplayed(step) => step,
            _ => {
                return Err(refused(
                    &agent,
                    ErrorKind::OwnershipLost,
                    "step answered with something other than its record",
                ));
            }
        };
        let Some(accept_seq) = out.head.active_turn.as_ref().map(|t| t.input_accept_seq) else {
            return Err(refused(
                &agent,
                ErrorKind::OwnershipLost,
                "the step's turn is no longer active",
            ));
        };
        let effect = step_effect(&out.head, &spec.step_key).cloned();
        Ok((StepJournal { step, effect }, accept_seq))
    }

    /// Commit the step's intent; answers the effect id.
    pub async fn commit_intent(
        &self,
        lease: &TurnLease,
        spec: &StepSpec,
    ) -> Result<Id128, AgentError> {
        let (agent, fence) = fence_from_lease(lease)?;
        let command = Command::CommitIntent(CommitIntent {
            fence,
            intent: IntentSpec {
                intent_key: spec.step_key.clone(),
                tool_name: spec.tool_name.clone(),
                tool_version: spec.tool_version.clone(),
                input_hash: spec.input_hash,
                policy: spec.policy,
                input: None,
            },
        });
        let out = self.execute_effect(&agent, &command, &[]).await?;
        match out.reply {
            Reply::IntentCommitted { effect_id } => Ok(effect_id),
            _ => Err(refused(
                &agent,
                ErrorKind::OwnershipLost,
                "intent answered with something other than its effect id",
            )),
        }
    }

    /// Authorize `attempt` of the effect as the lease's owner. A `manual`
    /// effect whose authorized attempt is retried is latched in doubt and
    /// refused `EffectInDoubt`.
    pub async fn authorize_attempt(
        &self,
        lease: &TurnLease,
        effect_id: Id128,
        attempt: u32,
    ) -> Result<u32, AgentError> {
        let (agent, fence) = fence_from_lease(lease)?;
        let command = Command::AuthorizeAttempt(AuthorizeAttempt {
            authority: Authority::Owner(fence.owner),
            effect_id,
            attempt,
        });
        let out = self.execute_effect(&agent, &command, &[]).await?;
        match out.reply {
            Reply::AttemptAuthorized { attempt, .. } => Ok(attempt),
            _ => Err(refused(
                &agent,
                ErrorKind::OwnershipLost,
                "the effect has no attempt left to authorize",
            )),
        }
    }

    /// Run one step: replay it when it completed, otherwise journal and
    /// authorize an attempt as its state and recovery policy allow and hand
    /// it to `dispatcher`.
    pub async fn run_step(
        &self,
        lease: &TurnLease,
        spec: &StepSpec,
        dispatcher: &dyn EffectDispatcher,
    ) -> Result<StepProgress, AgentError> {
        let (agent, fence) = fence_from_lease(lease)?;
        let (journal, accept_seq) = self.journal_step(lease, spec).await?;
        if journal.step.status == StepStatus::Completed {
            let output = self.read_json(&agent, journal.step.output).await?;
            return Ok(StepProgress::Completed { output });
        }
        let handed = match &journal.effect {
            Some(effect) => self.claim_reconciled(&agent, &fence, effect)?,
            None => None,
        };
        let attempt = match (&journal.effect, handed) {
            (Some(effect), Some(handed)) => {
                if let Err(err) = self.refuse_stale_hand_over(&agent, &fence).await {
                    if err.kind == ErrorKind::AgentGone {
                        self.drop_reconciled(&agent, effect.effect_id, &handed);
                    } else {
                        self.release_reconciled(&agent, effect.effect_id, handed);
                    }
                    return Err(err);
                }
                (effect.effect_id, effect.attempt)
            }
            (None, _) => {
                self.refuse_cancelled(&agent).await?;
                let effect_id = self.commit_intent(lease, spec).await?;
                let attempt = self.authorize_attempt(lease, effect_id, 1).await?;
                (effect_id, attempt)
            }
            (Some(effect), None) => match (effect.status, effect.policy) {
                (EffectStatus::Completed, _) => {
                    if let EffectResult::Failure(error) =
                        self.read_result(&agent, effect.result).await?
                    {
                        return Ok(StepProgress::Failed { error });
                    }
                    return self.complete_step(lease, spec, effect).await;
                }
                (EffectStatus::InDoubt, _) => {
                    return Err(refused(
                        &agent,
                        ErrorKind::EffectInDoubt,
                        "the step's effect awaits reconciliation",
                    ));
                }
                (EffectStatus::Intent, _) => {
                    self.refuse_cancelled(&agent).await?;
                    let next = effect.attempt.saturating_add(1);
                    let attempt = self
                        .authorize_attempt(lease, effect.effect_id, next)
                        .await?;
                    (effect.effect_id, attempt)
                }
                (EffectStatus::Authorized, RecoveryPolicy::ProviderIdempotency) => {
                    self.refuse_cancelled(&agent).await?;
                    (effect.effect_id, effect.attempt)
                }
                (
                    EffectStatus::Authorized,
                    RecoveryPolicy::AtLeastOnce | RecoveryPolicy::Manual,
                ) => {
                    self.refuse_cancelled(&agent).await?;
                    let next = effect.attempt.saturating_add(1);
                    let attempt = self
                        .authorize_attempt(lease, effect.effect_id, next)
                        .await?;
                    (effect.effect_id, attempt)
                }
            },
        };
        let (effect_id, attempt) = attempt;
        let dispatch = Dispatch {
            effect: EffectRef {
                agent,
                incarnation: fence.owner.incarnation,
                effect_id,
            },
            attempt,
            step_key: spec.step_key.clone(),
            tool_name: spec.tool_name.clone(),
            tool_version: spec.tool_version.clone(),
            policy: spec.policy,
            input_hash: spec.input_hash,
        };
        self.open_attempts.lock().insert(
            (agent, effect_id),
            OpenAttempt {
                owner_id: fence.owner.owner_id.clone(),
                epoch: fence.owner.epoch,
                incarnation: fence.owner.incarnation,
                accept_seq,
                attempt,
                reconciled: false,
            },
        );
        dispatcher.dispatch(&dispatch);
        Ok(StepProgress::Dispatched(dispatch))
    }

    /// Record `result` for `attempt`, whoever reports it. Only the effect's
    /// journaled attempt settles it; anything else is evidence.
    pub async fn complete_effect(
        &self,
        effect: &EffectRef,
        attempt: u32,
        result: &Value,
    ) -> Result<EffectSettled, AgentError> {
        let bytes = json_bytes(&effect.agent, result)?;
        self.record_result(effect, attempt, bytes).await
    }

    /// Record a failed attempt: `error` becomes the attempt's result, stored
    /// under [`FAILURE_RESULT_PREFIX`], so the effect is settled and the turn
    /// may fail. The step replays it as [`StepProgress::Failed`], never as an
    /// output (`tests::fail_effect_records_the_failure_so_the_turn_can_fail`,
    /// `tests::an_output_shaped_like_a_failure_replays_as_an_output`).
    pub async fn fail_effect(
        &self,
        effect: &EffectRef,
        attempt: u32,
        error: &str,
    ) -> Result<EffectSettled, AgentError> {
        self.record_result(effect, attempt, EffectResult::failure_bytes(error))
            .await
    }

    /// The effect result object `dep` names, decoded; no object is a `null`
    /// output.
    pub async fn read_result(
        &self,
        agent: &AgentId,
        dep: Option<ObjectRef>,
    ) -> Result<EffectResult, AgentError> {
        let Some(dep) = dep else {
            return Ok(EffectResult::Output(Value::Null));
        };
        let bytes = self.store.read_object(agent, dep).await?;
        EffectResult::decode(&bytes).ok_or_else(|| {
            refused(
                agent,
                ErrorKind::UnsupportedFormat,
                "effect result is neither a JSON value nor a recorded failure",
            )
        })
    }

    /// Reconcile an effect in doubt by attaching a verified result to its
    /// latest attempt. Idempotent: attaching the same result again answers
    /// `Recorded` without a write.
    pub async fn reconcile_attach(
        &self,
        effect: &EffectRef,
        result: &Value,
    ) -> Result<EffectSettled, AgentError> {
        let head = self.reload(&effect.agent).await?;
        let record = journaled(&head, effect)?;
        if record.status != EffectStatus::InDoubt && record.status != EffectStatus::Completed {
            return Err(refused(
                &effect.agent,
                ErrorKind::OwnershipLost,
                "EffectStateAdmits: reconciliation needs an effect in doubt",
            ));
        }
        let attempt = match record.status {
            EffectStatus::Completed => record.result_attempt,
            _ => record.attempt,
        };
        self.complete_effect(effect, attempt, result).await
    }

    /// Reconcile an effect in doubt by authorizing `attempt`, the one after
    /// its latest, and handing it over. Nothing is dispatched here: the
    /// attempt is recorded open and unclaimed for the effect's incarnation,
    /// and the next [`Coordinator::run_step`] for the effect's step through
    /// this coordinator, under whichever owner holds the turn by then,
    /// dispatches it through its own dispatcher, once
    /// (`tests::reconcile_is_idempotent_and_audited_once`,
    /// `tests::a_reconciled_attempt_survives_a_takeover_before_the_next_step`). Answers `true`
    /// when this call's own HEAD write published the authorization; only
    /// then is the attempt handed over, and an existing hand-over record for
    /// the same incarnation and attempt is kept as it is. Idempotent: a
    /// repeat for an attempt already authorized answers `false` and hands
    /// nothing over.
    ///
    /// When this call's write came back with an unknown outcome and the
    /// retry finds the attempt authorized, the authorization cannot be
    /// attributed to this call: another reconcile from the same HEAD writes
    /// the same bytes. The call answers `OutcomeUnknown` and hands nothing
    /// over, whether its own write landed
    /// (`tests::a_reconcile_authorization_with_a_lost_ack_is_not_handed_over_and_latches_in_doubt_again`)
    /// or another coordinator's did, which then holds the hand-over
    /// (`tests::concurrent_reconciles_on_two_coordinators_dispatch_the_attempt_at_most_once`).
    /// When another reconcile of the same attempt landed first and this
    /// call's write lost the race with a known outcome, this call answers
    /// `false` and hands nothing over
    /// (`tests::a_reconcile_another_admin_landed_first_is_not_dispatched_again`).
    ///
    /// The hand-over is held by this coordinator in memory. A next step
    /// that reaches another coordinator, or a restart in between, finds the
    /// attempt authorized with no record, takes it as lost, and the step's
    /// policy decides (a `manual` effect latches in doubt again).
    ///
    /// When all [`UNKNOWN_OUTCOME_RETRIES`] retries also come back unknown,
    /// the error is returned and nothing is handed over, even if a write
    /// landed: HEAD may then hold the attempt authorized with no record, and
    /// the owner's next `run_step` finds it lost as above.
    pub async fn reconcile_authorize(
        &self,
        effect: &EffectRef,
        attempt: u32,
    ) -> Result<bool, AgentError> {
        let head = self.reload(&effect.agent).await?;
        let record = journaled(&head, effect)?.clone();
        let replay = record.status == EffectStatus::Authorized && record.attempt == attempt;
        if record.status != EffectStatus::InDoubt && !replay {
            return Err(refused(
                &effect.agent,
                ErrorKind::OwnershipLost,
                "EffectStateAdmits: reconciliation needs an effect in doubt",
            ));
        }
        let command = Command::AuthorizeAttempt(AuthorizeAttempt {
            authority: Authority::Reconcile(IncarnationFence {
                incarnation: effect.incarnation,
            }),
            effect_id: effect.effect_id,
            attempt,
        });
        let (out, retried) = self
            .execute_effect_counted(&effect.agent, &command, &[])
            .await?;
        let Reply::AttemptAuthorized { attempt, .. } = out.reply else {
            return Err(refused(
                &effect.agent,
                ErrorKind::OwnershipLost,
                "reconciliation answered with something other than an attempt",
            ));
        };
        if !out.published {
            // After an unknown outcome the retry finds the attempt authorized
            // whoever wrote it: another reconcile starting from the same HEAD
            // writes the same bytes, so nothing here says it was this call.
            let landed = retried
                && journaled(&out.head, effect).is_ok_and(|now| {
                    now.status == EffectStatus::Authorized && now.attempt == attempt
                });
            if landed {
                return Err(refused(
                    &effect.agent,
                    ErrorKind::OutcomeUnknown,
                    "the attempt's authorization landed but cannot be attributed to this call; it was not handed over here",
                ));
            }
            return Ok(false);
        }
        if let Some(accept_seq) = effect_input(&out.head, &record) {
            let mut attempts = self.open_attempts.lock();
            let key = (effect.agent, effect.effect_id);
            let held = attempts.get(&key).is_some_and(|open| {
                open.incarnation == out.head.incarnation && open.attempt == attempt
            });
            if !held {
                attempts.insert(
                    key,
                    OpenAttempt {
                        owner_id: out.head.owner_id.clone(),
                        epoch: out.head.epoch,
                        incarnation: out.head.incarnation,
                        accept_seq,
                        attempt,
                        reconciled: true,
                    },
                );
            }
        }
        Ok(true)
    }

    /// Claim the attempt of `effect` a reconcile handed over, if this
    /// coordinator holds it unclaimed: the unclaimed record once, for the one
    /// `run_step` that dispatches it. The hand-over is keyed on the
    /// incarnation and the attempt, not on the owner, since
    /// `Authority::Reconcile` is fenced by incarnation only and an unclaimed
    /// record proves no step in this process dispatched the attempt; a
    /// takeover between the reconcile and the next step does not lose it. The
    /// claim moves the record to the fence's owner and epoch, so a concurrent
    /// step of that owner is refused `IdempotencyConflict`, since the attempt
    /// is open (`tests::a_step_that_passed_the_open_check_before_the_claim_is_refused_after_it`).
    fn claim_reconciled(
        &self,
        agent: &AgentId,
        fence: &TurnFence,
        effect: &EffectRecord,
    ) -> Result<Option<OpenAttempt>, AgentError> {
        if effect.status != EffectStatus::Authorized {
            return Ok(None);
        }
        let mut attempts = self.open_attempts.lock();
        let Some(open) = attempts.get_mut(&(*agent, effect.effect_id)) else {
            return Ok(None);
        };
        if open.incarnation != fence.owner.incarnation || open.attempt != effect.attempt {
            return Ok(None);
        }
        if open.reconciled {
            let handed = open.clone();
            open.reconciled = false;
            open.owner_id = fence.owner.owner_id.clone();
            open.epoch = fence.owner.epoch;
            return Ok(Some(handed));
        }
        if open.owner_id == fence.owner.owner_id && open.epoch == fence.owner.epoch {
            return Err(refused(
                agent,
                ErrorKind::IdempotencyConflict,
                "a step's attempt is still open; steps are sequential",
            ));
        }
        Ok(None)
    }

    /// Whether this coordinator holds `attempt` of `effect` as a hand-over
    /// from [`Coordinator::reconcile_authorize`] that no step has claimed
    /// yet. An attempt authorized by an owner's own step, or a hand-over
    /// already dispatched, answers `false`.
    pub fn holds_hand_over(&self, effect: &EffectRef, attempt: u32) -> bool {
        self.open_attempts
            .lock()
            .get(&(effect.agent, effect.effect_id))
            .is_some_and(|open| {
                open.reconciled && open.incarnation == effect.incarnation && open.attempt == attempt
            })
    }

    /// Hand a claimed reconciled attempt back, unclaimed, when its step was
    /// refused before dispatching it. A takeover in between may have pruned
    /// the claimed record as its epoch's, so a missing record is put back;
    /// a record for another attempt is newer and kept.
    fn release_reconciled(&self, agent: &AgentId, effect_id: Id128, handed: OpenAttempt) {
        let mut attempts = self.open_attempts.lock();
        match attempts.get_mut(&(*agent, effect_id)) {
            Some(open) => {
                if open.incarnation == handed.incarnation && open.attempt == handed.attempt {
                    open.reconciled = true;
                }
            }
            None => {
                attempts.insert((*agent, effect_id), handed);
            }
        }
    }

    /// Forget a claimed reconciled attempt whose agent is gone: no step and
    /// no reconcile can reach its incarnation again. A record for another
    /// attempt is newer and kept.
    fn drop_reconciled(&self, agent: &AgentId, effect_id: Id128, handed: &OpenAttempt) {
        let mut attempts = self.open_attempts.lock();
        if attempts.get(&(*agent, effect_id)).is_some_and(|open| {
            open.incarnation == handed.incarnation && open.attempt == handed.attempt
        }) {
            attempts.remove(&(*agent, effect_id));
        }
    }

    async fn record_result(
        &self,
        effect: &EffectRef,
        attempt: u32,
        bytes: Bytes,
    ) -> Result<EffectSettled, AgentError> {
        let result = self.store.put_object(&effect.agent, bytes.clone()).await?;
        let command = Command::RecordEffectResult(RecordEffectResult {
            fence: IncarnationFence {
                incarnation: effect.incarnation,
            },
            effect_id: effect.effect_id,
            attempt,
            result,
        });
        let out = self
            .execute_effect(&effect.agent, &command, &[bytes])
            .await?;
        match out.reply {
            Reply::EffectResultRecorded { .. } => {
                self.open_attempts
                    .lock()
                    .remove(&(effect.agent, effect.effect_id));
                Ok(EffectSettled::Recorded)
            }
            Reply::LateResultIgnored { .. } => Ok(EffectSettled::Evidence { result }),
            _ => Err(refused(
                &effect.agent,
                ErrorKind::OwnershipLost,
                "result answered with something other than a verdict",
            )),
        }
    }

    /// Journal the step completed from its effect's recorded result.
    async fn complete_step(
        &self,
        lease: &TurnLease,
        spec: &StepSpec,
        effect: &EffectRecord,
    ) -> Result<StepProgress, AgentError> {
        let (agent, fence) = fence_from_lease(lease)?;
        let command = Command::RecordStep(RecordStep {
            fence,
            step_key: spec.step_key.clone(),
            tool_name: spec.tool_name.clone(),
            tool_version: spec.tool_version.clone(),
            input_hash: spec.input_hash,
            status: StepStatus::Completed,
            output: effect.result,
        });
        let out = self.execute_effect(&agent, &command, &[]).await?;
        let (Reply::StepRecorded(step) | Reply::StepReplayed(step)) = out.reply else {
            return Err(refused(
                &agent,
                ErrorKind::OwnershipLost,
                "step answered with something other than its record",
            ));
        };
        let output = self.read_json(&agent, step.output).await?;
        Ok(StepProgress::Completed { output })
    }

    /// Refuse a new step while an attempt this coordinator dispatched under
    /// the fence's epoch, for the active turn's input, has no result. An
    /// attempt of another input does not race this step: that input settled
    /// (a cancelled input settles with its attempt still authorized), and its
    /// record is dropped by [`Coordinator::prune_open_attempts`] once HEAD
    /// shows the input's result. The record is also dropped once HEAD no
    /// longer shows that attempt open. An attempt a reconcile handed over and
    /// not yet dispatched does not refuse `step_key`'s own step, which is the
    /// one that dispatches it; it refuses every other step.
    async fn refuse_open_attempt(
        &self,
        agent: &AgentId,
        fence: &TurnFence,
        step_key: &str,
    ) -> Result<(), AgentError> {
        let mine: Vec<(Id128, u64, u32, bool)> = self
            .open_attempts
            .lock()
            .iter()
            .filter(|((a, _), open)| {
                a == agent
                    && ((open.owner_id == fence.owner.owner_id && open.epoch == fence.owner.epoch)
                        || (open.reconciled && open.incarnation == fence.owner.incarnation))
            })
            .map(|((_, effect_id), open)| {
                (*effect_id, open.accept_seq, open.attempt, open.reconciled)
            })
            .collect();
        if mine.is_empty() {
            return Ok(());
        }
        let head = self.reload(agent).await?;
        let Some(active) = head.active_turn.as_ref().map(|t| t.input_accept_seq) else {
            return Ok(());
        };
        let own_effect = effect_id(fence.owner.incarnation, active, step_key);
        let mut still_open = false;
        let mut attempts = self.open_attempts.lock();
        for (id, accept_seq, attempt, reconciled) in mine {
            if accept_seq != active || (reconciled && id == own_effect) {
                continue;
            }
            let open = head.effects.iter().any(|e| {
                e.effect_id == id && e.status == EffectStatus::Authorized && e.attempt == attempt
            });
            if open {
                still_open = true;
            } else {
                attempts.remove(&(*agent, id));
            }
        }
        if still_open {
            return Err(refused(
                agent,
                ErrorKind::IdempotencyConflict,
                "a step's attempt is still open; steps are sequential",
            ));
        }
        Ok(())
    }

    /// Drop the attempts recorded for `agent` under an epoch older than
    /// `head`'s, since no lease of that epoch can journal another step, and
    /// those whose input `head` shows settled, since no step of that input
    /// follows. An unclaimed hand-over outlives its epoch: whichever owner
    /// runs the step next dispatches it.
    ///
    /// Reads no page: an input is settled when its result is in the open
    /// results page or below `results.tail_from`, since results seal only
    /// below the pending floor, which never falls.
    pub(crate) fn prune_open_attempts(&self, agent: &AgentId, head: &Head) {
        let mut attempts = self.open_attempts.lock();
        if attempts.is_empty() {
            return;
        }
        let settled = |seq: u64| {
            head.index.results.page_for(seq).is_some()
                || head.results.iter().any(|r| r.accept_seq == seq)
        };
        attempts.retain(|(a, _), open| {
            let current = open.epoch >= head.epoch
                || (open.reconciled && open.incarnation == head.incarnation);
            a != agent
                || (current && !(open.incarnation == head.incarnation && settled(open.accept_seq)))
        });
    }

    /// Refuse a dispatch while the loaded root shows the input being
    /// cancelled. The kernel refuses a new authorization then as well; this
    /// also covers a re-dispatch that needs none.
    async fn refuse_cancelled(&self, agent: &AgentId) -> Result<(), AgentError> {
        let head = match self.cached(agent) {
            Some(head) => head,
            None => self.reload(agent).await?,
        };
        refuse_cancelling(agent, &head)
    }

    /// Refuse dispatching a claimed hand-over unless HEAD read now still
    /// names the fence's owner and epoch and the input is not being
    /// cancelled. A hand-over can wait on an operator for longer than any
    /// lease, so it is checked against a fresh read rather than the loaded
    /// root, and a step whose turn was taken over after it journaled hands
    /// the attempt back rather than dispatching it to a worker that lost the
    /// turn (`tests::a_hand_over_claimed_by_an_owner_that_lost_the_turn_goes_to_the_new_owner`).
    /// A HEAD whose lifecycle is not `Active` answers `AgentGone`, and
    /// `run_step` drops the claim rather than handing it back
    /// (`tests::a_hand_over_claimed_after_a_destroy_on_another_coordinator_is_not_dispatched`).
    async fn refuse_stale_hand_over(
        &self,
        agent: &AgentId,
        fence: &TurnFence,
    ) -> Result<(), AgentError> {
        let head = self.reload(agent).await?;
        if head.lifecycle != Lifecycle::Active {
            return Err(refused(
                agent,
                ErrorKind::AgentGone,
                "the agent was destroyed before the handed-over attempt was dispatched",
            ));
        }
        if head.incarnation != fence.owner.incarnation
            || head.owner_id != fence.owner.owner_id
            || head.epoch != fence.owner.epoch
        {
            return Err(refused(
                agent,
                ErrorKind::OwnershipLost,
                "the turn changed owner before the handed-over attempt was dispatched",
            ));
        }
        refuse_cancelling(agent, &head)
    }

    /// Run an effect command, running it again while its outcome is unknown.
    async fn execute_effect(
        &self,
        agent: &AgentId,
        command: &Command,
        deps: &[Bytes],
    ) -> Result<Outcome, AgentError> {
        Ok(self.execute_effect_counted(agent, command, deps).await?.0)
    }

    /// [`Coordinator::execute_effect`], also answering whether an unknown
    /// outcome was run again.
    async fn execute_effect_counted(
        &self,
        agent: &AgentId,
        command: &Command,
        deps: &[Bytes],
    ) -> Result<(Outcome, bool), AgentError> {
        let mut retries = 0;
        loop {
            match self.execute_with(agent, command, deps).await {
                Ok(out) => {
                    if retries > 0 {
                        self.metrics
                            .outcome_unknown_resolved
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    self.after_outcome(&out);
                    return Ok((out, retries > 0));
                }
                Err(e)
                    if e.kind == ErrorKind::OutcomeUnknown && retries < UNKNOWN_OUTCOME_RETRIES =>
                {
                    retries += 1;
                }
                Err(e) => {
                    if e.kind == ErrorKind::OwnershipLost {
                        self.metrics
                            .ownership_lost_refusals
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    if matches!(
                        e.kind,
                        ErrorKind::OwnershipLost
                            | ErrorKind::NonDeterministicReplay
                            | ErrorKind::EffectInDoubt
                    ) && let Err(reload) = self.reload(agent).await
                    {
                        tracing::debug!(%reload, "agents reload after an effect refusal failed");
                    }
                    return Err(e);
                }
            }
        }
    }
}

/// Refuse a dispatch while `head` shows the active input being cancelled.
fn refuse_cancelling(agent: &AgentId, head: &Head) -> Result<(), AgentError> {
    if head
        .active_turn
        .as_ref()
        .is_some_and(|t| t.cancellation == Cancellation::Requested)
    {
        return Err(refused(
            agent,
            ErrorKind::OwnershipLost,
            "EffectStateAdmits: the effect's input is being cancelled",
        ));
    }
    Ok(())
}

/// The effect HEAD journals for `step_key` under the active turn's input.
fn step_effect<'a>(head: &'a Head, step_key: &str) -> Option<&'a EffectRecord> {
    let turn = head.active_turn.as_ref()?;
    let id = effect_id(head.incarnation, turn.input_accept_seq, step_key);
    head.effects.iter().find(|e| e.effect_id == id)
}

/// The input whose turn journaled `record`, among those HEAD still holds.
/// The active turn's input is read from the turn itself; any other input's
/// pages are in `head` because a reconciling `AuthorizeAttempt` loads every
/// inbox and results page for an effect outside the active turn
/// (`kernel::page_needs`).
fn effect_input(head: &Head, record: &EffectRecord) -> Option<u64> {
    head.active_turn
        .iter()
        .map(|t| t.input_accept_seq)
        .chain(head.inbox.iter().map(|i| i.accept_seq))
        .chain(head.results.iter().map(|r| r.accept_seq))
        .find(|seq| effect_id(head.incarnation, *seq, &record.step_key) == record.effect_id)
}

fn journaled<'a>(head: &'a Head, effect: &EffectRef) -> Result<&'a EffectRecord, AgentError> {
    if head.incarnation != effect.incarnation {
        return Err(refused(
            &effect.agent,
            ErrorKind::AgentGone,
            "the effect's incarnation is not the current one",
        ));
    }
    head.effects
        .iter()
        .find(|e| e.effect_id == effect.effect_id)
        .ok_or_else(|| {
            refused(
                &effect.agent,
                ErrorKind::AgentGone,
                "effect is not journaled",
            )
        })
}

#[cfg(test)]
mod tests;
