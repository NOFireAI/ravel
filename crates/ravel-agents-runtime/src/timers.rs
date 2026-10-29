//! Durable timers (ADR-3331 decision 8).
//!
//! [`Coordinator::schedule_timer`] and [`Coordinator::cancel_timer`] are one
//! kernel command each, published through the store. The `_wake` hint a
//! schedule needs is written by the store after the CAS, never here. Firing
//! is issued from one place, `fire_due`, which the sweep calls with a root
//! it has just read from HEAD (or the root the previous fire's CAS just
//! published), and each fire names that root's timer id and generation in a
//! `FireTimer`, so it is one CAS that the kernel refuses as `TimerStale` for
//! any generation the root no longer holds as scheduled
//! (`tests::fire_and_cancel_race_resolves_at_head_and_a_stale_hint_never_fires_a_cancelled_generation`).
//!
//! A cancel that lands after the firing CAS answers `AlreadyFired`, and a
//! fire after a cancel enqueues nothing: whichever CAS lands first wins, and
//! HEAD alone records which (same test).
//!
//! # Clock
//!
//! A timer's `due_unix_ns` is recorded time: the instant the scheduler asked
//! for, in unix nanoseconds, stored in HEAD unchanged. "Due" means
//! `due_unix_ns <= now`, where `now` is the injected [`ravel_agents_store::Clock`]
//! of the coordinator running the sweep; the store's kernel repeats the check
//! against its own clock at the CAS and answers `TimerNotDue` when it reads an
//! earlier time. No margin is added in either direction: a timer fires at its
//! recorded due time and not a nanosecond before
//! (`tests::a_timer_fires_at_its_recorded_due_time_and_not_a_nanosecond_before`).
//!
//! Safety does not depend on clocks: a fire is a fenced CAS on the generation
//! HEAD holds, so skew can make a timer fire early or late but never twice and
//! never after its cancel. Accuracy does: a sweep host whose clock runs ahead
//! by `s` fires up to `s` early, one that runs behind fires late by its lag
//! plus up to one sweep interval, and a fire refused `TimerNotDue` is retried
//! by the next pass. Hints are bucketed by minute
//! (`WAKE_BUCKET_WIDTH_NS`), so a hint in the current bucket can be early;
//! such a hint fires nothing and is kept
//! (`sweep::tests::a_premature_hint_fires_nothing_and_is_kept`).

use std::sync::atomic::Ordering;

use ravel_agents_core::command::{
    CancelTimer, Command, FireTimer, IncarnationFence, ScheduleTimer, TimerSpec,
};
use ravel_agents_core::error::{AgentError, ErrorKind};
use ravel_agents_core::head::{Head, Lifecycle, TimerEntry, TimerStatus};
use ravel_agents_core::identity::{AgentId, timer_id};
use ravel_agents_core::kernel::{Effect, Reply};
use ravel_agents_core::keys::DueBucket;
use ravel_agents_core::pages::{ListKind, Need, id_fingerprint};
use ravel_agents_core::wire::{
    CancelTimerRequest, CancelTimerResponse, TimerCancelOutcome, TimerRequest, TimerResponse,
};

use crate::{Coordinator, opt_dep, parse_agent, refused};

/// What firing the due timers of one root did.
pub(crate) struct Fired {
    /// The newest root seen: the one read, or the last one a fire published.
    pub(crate) head: Head,
    /// `TimerFired` replies this call's CASes published.
    pub(crate) fired: u64,
    /// Whether every due timer was fired or found settled. A fire that
    /// failed or was refused as not due leaves this false, and the hints of
    /// the agent are kept for the next pass.
    pub(crate) clean: bool,
}

impl Coordinator {
    /// Schedule (or reschedule) the timer `timer_key`: one `ScheduleTimer`
    /// through the store. The store writes the wake hint after its CAS. A
    /// lost acknowledgement is settled from HEAD: a scheduled entry with the
    /// same due time, handler and payload is the answer; otherwise the
    /// command runs once more
    /// (`tests::a_schedule_whose_hint_was_never_written_still_fires_when_due`).
    pub async fn schedule_timer(&self, req: &TimerRequest) -> Result<TimerResponse, AgentError> {
        let (kind, key) = parse_agent(&req.agent)?;
        let agent = ravel_agents_core::identity::agent_id(&kind, &key);
        let mut deps = Vec::new();
        let payload = opt_dep(&mut deps, &agent, &req.payload)?;
        let spec = TimerSpec {
            timer_key: req.timer_key.clone(),
            due_unix_ns: req.due_unix_ns,
            handler: req.handler.clone(),
            payload,
        };
        let mut incarnation = self.incarnation(&agent).await?;
        let mut reloaded = false;
        let mut retried = false;
        loop {
            let command = Command::ScheduleTimer(ScheduleTimer {
                fence: IncarnationFence { incarnation },
                timer: spec.clone(),
            });
            match self.store.execute_with(&agent, &command, &deps).await {
                Ok(out) => {
                    self.observe(&agent, &out.head);
                    let Reply::TimerScheduled {
                        timer_id,
                        generation,
                    } = out.reply
                    else {
                        return Err(refused(
                            &agent,
                            ErrorKind::OutcomeUnknown,
                            "schedule answered with something other than a timer",
                        ));
                    };
                    return Ok(TimerResponse {
                        timer: timer_id.to_hex(),
                        generation,
                    });
                }
                Err(e) if e.kind == ErrorKind::OutcomeUnknown => {
                    let head = self.reload(&agent).await?;
                    if let Some(entry) = scheduled_as(&head, &spec) {
                        self.metrics
                            .outcome_unknown_resolved
                            .fetch_add(1, Ordering::Relaxed);
                        return Ok(TimerResponse {
                            timer: entry.timer_id.to_hex(),
                            generation: entry.generation,
                        });
                    }
                    if retried {
                        return Err(e);
                    }
                    retried = true;
                    incarnation = head.incarnation;
                }
                Err(e) if e.kind == ErrorKind::AgentGone && !reloaded => {
                    reloaded = true;
                    incarnation = self.reload(&agent).await?.incarnation;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Cancel the timer `timer_key`: one `CancelTimer` through the store.
    /// `AlreadyFired` when HEAD shows the timer delivered, `Unknown` when it
    /// holds no entry for the key. A lost acknowledgement is settled from
    /// HEAD the same way.
    pub async fn cancel_timer(
        &self,
        req: &CancelTimerRequest,
    ) -> Result<CancelTimerResponse, AgentError> {
        let (kind, key) = parse_agent(&req.agent)?;
        let agent = ravel_agents_core::identity::agent_id(&kind, &key);
        let mut incarnation = self.incarnation(&agent).await?;
        let mut reloaded = false;
        let mut retried = false;
        loop {
            let command = Command::CancelTimer(CancelTimer {
                fence: IncarnationFence { incarnation },
                timer_key: req.timer_key.clone(),
            });
            match self.store.execute(&agent, &command).await {
                Ok(out) => {
                    self.observe(&agent, &out.head);
                    let outcome = match out.reply {
                        Reply::TimerCancelled => TimerCancelOutcome::Cancelled,
                        Reply::AlreadyFired => TimerCancelOutcome::AlreadyFired,
                        Reply::UnknownTimer => TimerCancelOutcome::Unknown,
                        _ => {
                            return Err(refused(
                                &agent,
                                ErrorKind::OutcomeUnknown,
                                "cancel answered with something other than a timer",
                            ));
                        }
                    };
                    return Ok(CancelTimerResponse { outcome });
                }
                Err(e) if e.kind == ErrorKind::OutcomeUnknown => {
                    let mut head = self.reload(&agent).await?;
                    let id = timer_id(head.incarnation, &req.timer_key);
                    // A cancelled or delivered entry may sit on a sealed page.
                    self.store
                        .attach(
                            &agent,
                            &mut head,
                            &[(ListKind::Timers, Need::Fingerprint(id_fingerprint(&id)))],
                        )
                        .await?;
                    let outcome = match head.timers.iter().find(|t| t.timer_id == id) {
                        None => Some(TimerCancelOutcome::Unknown),
                        Some(t) => match t.status {
                            TimerStatus::Cancelled => Some(TimerCancelOutcome::Cancelled),
                            TimerStatus::Delivered => Some(TimerCancelOutcome::AlreadyFired),
                            TimerStatus::Scheduled => None,
                        },
                    };
                    if let Some(outcome) = outcome {
                        self.metrics
                            .outcome_unknown_resolved
                            .fetch_add(1, Ordering::Relaxed);
                        return Ok(CancelTimerResponse { outcome });
                    }
                    if retried {
                        return Err(e);
                    }
                    retried = true;
                    incarnation = head.incarnation;
                }
                Err(e) if e.kind == ErrorKind::AgentGone && !reloaded => {
                    reloaded = true;
                    incarnation = self.reload(&agent).await?.incarnation;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Fire every timer `head` holds as scheduled and due at `now`, one
    /// `FireTimer` CAS each, taking the timer id and generation from `head`
    /// as read from HEAD by the caller, then from each fire's own published
    /// root. The only place this crate issues `FireTimer`.
    pub(crate) async fn fire_due(&self, agent: &AgentId, head: Head, now: i64) -> Fired {
        let mut fired = Fired {
            head,
            fired: 0,
            clean: true,
        };
        if fired.head.lifecycle != Lifecycle::Active {
            return fired;
        }
        loop {
            let Some(due) = due_timers(&fired.head, now).into_iter().next() else {
                return fired;
            };
            let command = Command::FireTimer(FireTimer {
                fence: IncarnationFence {
                    incarnation: fired.head.incarnation,
                },
                timer_id: due.timer_id,
                generation: due.generation,
            });
            match self.store.execute(agent, &command).await {
                Ok(out) => {
                    let wake = out.effects.contains(&Effect::WakeWorker);
                    let landed = matches!(out.reply, Reply::TimerFired { .. });
                    let still_due = out.head.timers.iter().any(|t| {
                        t.timer_id == due.timer_id
                            && t.generation == due.generation
                            && t.status == TimerStatus::Scheduled
                    });
                    fired.head = out.head;
                    if landed && out.published {
                        fired.fired += 1;
                        self.metrics.timers_fired.fetch_add(1, Ordering::Relaxed);
                    }
                    if wake {
                        self.wake.notify_waiters();
                    }
                    // Refused as not due by the store's clock: the next pass
                    // retries, and this one stops here rather than spin.
                    if still_due {
                        fired.clean = false;
                        return fired;
                    }
                }
                Err(error) => {
                    tracing::debug!(agent = %agent.to_hex(), %error, "agents timer fire not published");
                    fired.clean = false;
                    return fired;
                }
            }
        }
    }
}

/// The entry `head` holds as scheduled exactly as `spec` asks, if any.
fn scheduled_as<'a>(head: &'a Head, spec: &TimerSpec) -> Option<&'a TimerEntry> {
    let id = timer_id(head.incarnation, &spec.timer_key);
    head.timers.iter().find(|t| {
        t.timer_id == id
            && t.status == TimerStatus::Scheduled
            && t.due_unix_ns == spec.due_unix_ns
            && t.handler == spec.handler
            && t.payload == spec.payload
    })
}

/// The timers `head` holds as scheduled with `due_unix_ns <= now`, earliest
/// first.
pub(crate) fn due_timers(head: &Head, now: i64) -> Vec<TimerEntry> {
    let mut due: Vec<TimerEntry> = head
        .timers
        .iter()
        .filter(|t| t.status == TimerStatus::Scheduled && t.due_unix_ns <= now)
        .cloned()
        .collect();
    due.sort_by_key(|t| (t.due_unix_ns, t.generation));
    due
}

/// Whether `head` still holds a scheduled timer whose hint lives in
/// `bucket`.
pub(crate) fn armed_in(head: &Head, bucket: u64) -> bool {
    head.lifecycle == Lifecycle::Active
        && head.timers.iter().any(|t| {
            t.status == TimerStatus::Scheduled
                && DueBucket::for_due(t.due_unix_ns).map(DueBucket::index) == Ok(bucket)
        })
}

#[cfg(test)]
pub(crate) mod tests;
