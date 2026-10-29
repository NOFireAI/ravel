//! Activation, takeover and the lease table (ADR-3331 decision 6).
//!
//! Activation and takeover are each one kernel `Takeover` command through
//! the store: one HEAD CAS that advances the epoch and names the new owner
//! (`tests::worker_paused_past_lease_cannot_commit_after_takeover_epoch_advanced`
//! asserts the CAS count). The owner id is the worker id. A takeover whose
//! acknowledgement was lost and whose HEAD shows another owner past the
//! observed epoch is `OwnershipLost`, never the raw `OutcomeUnknown`
//! (`tests::a_lost_takeover_ack_with_another_owner_in_head_is_ownership_lost`).
//! The kernel accepts a takeover by the current owner too: it advances the
//! epoch and keeps the active turn, so the owner's own older lease is
//! refused (`tests::a_stale_lease_of_the_same_owner_cannot_commit_after_an_admin_takeover`).
//!
//! The lease table maps an agent to the owner, epoch and expiry this
//! coordinator last granted or renewed. Expiry plus `heartbeat_grace_ms`
//! decides only WHEN a takeover is attempted. It never decides whether a
//! commit is valid: a commit carries the lease's own fence and the kernel
//! checks that fence against HEAD. Renewals are in memory only; another
//! coordinator sees the lease hint the takeover wrote into HEAD, which no
//! heartbeat advances, so with two coordinators a turn that runs longer than
//! `lease_ms + heartbeat_grace_ms` is taken over by a worker polling the
//! other one and the first worker's commit is then refused. That bound
//! holds until a renewal reaches HEAD (a kernel command under the owner
//! fence; T11 #3345 and T15 #3348 own that design). The same table also
//! lets a takeover at every turn boundary: `takeover_due` fires whenever the
//! owner holds no active turn, so with several workers on one agent each
//! boundary costs one `Takeover` CAS.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use ravel_agents_core::command::{Command, OwnerFence, Takeover};
use ravel_agents_core::error::{AgentError, ErrorKind};
use ravel_agents_core::head::Head;
use ravel_agents_core::identity::{AgentId, agent_id};
use ravel_agents_core::kernel::Reply;
use ravel_agents_core::wire::{AgentRef, TurnLease};

use crate::{Coordinator, fence_from_lease, parse_agent, refused};

/// Why a takeover was attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakeoverReason {
    /// The agent had no owner.
    Activation,
    /// The previous owner's lease and grace both ran out.
    LeaseExpired,
    /// An operator moved the agent.
    Admin,
}

/// One lease-table entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Lease {
    pub(crate) owner_id: String,
    pub(crate) epoch: u64,
    pub(crate) expires_unix_ns: i64,
}

#[derive(Debug, Default)]
pub(crate) struct LeaseTable(Mutex<HashMap<AgentId, Lease>>);

impl LeaseTable {
    /// The entry for `head`'s current owner and epoch, or one rebuilt from
    /// HEAD's lease hint when the table holds none or an older epoch.
    pub(crate) fn current(&self, agent: &AgentId, head: &Head) -> Lease {
        let mut table = self.0.lock();
        match table.get(agent) {
            Some(l) if l.epoch == head.epoch && l.owner_id == head.owner_id => l.clone(),
            _ => {
                let lease = Lease {
                    owner_id: head.owner_id.clone(),
                    epoch: head.epoch,
                    expires_unix_ns: head.owner_lease_hint_unix_ns,
                };
                table.insert(*agent, lease.clone());
                lease
            }
        }
    }

    pub(crate) fn grant(&self, agent: &AgentId, owner_id: &str, epoch: u64, expires: i64) {
        self.0.lock().insert(
            *agent,
            Lease {
                owner_id: owner_id.to_string(),
                epoch,
                expires_unix_ns: expires,
            },
        );
    }

    pub(crate) fn forget(&self, agent: &AgentId) {
        self.0.lock().remove(agent);
    }
}

pub(crate) fn ms_to_ns(ms: u64) -> i64 {
    i64::try_from(ms.saturating_mul(1_000_000)).unwrap_or(i64::MAX)
}

impl Coordinator {
    pub(crate) fn lease_ns(&self) -> i64 {
        ms_to_ns(self.config.lease_ms)
    }

    /// Whether `worker` may take `head` over now: it has no owner, or its
    /// owner holds no active turn, or the owner's lease and grace ran out.
    pub(crate) fn takeover_due(
        &self,
        agent: &AgentId,
        head: &Head,
        worker: &str,
    ) -> Option<TakeoverReason> {
        if head.owner_id == worker {
            return None;
        }
        if head.owner_id.is_empty() {
            return Some(TakeoverReason::Activation);
        }
        let lease = self.leases.current(agent, head);
        let grace = ms_to_ns(self.config.heartbeat_grace_ms);
        if head.active_turn.is_none() || self.now() > lease.expires_unix_ns.saturating_add(grace) {
            return Some(TakeoverReason::LeaseExpired);
        }
        None
    }

    /// Run the kernel's `Takeover` for `owner_id` against the epoch observed
    /// in `head`: one CAS. Returns the new owner fence.
    pub(crate) async fn take_over(
        &self,
        agent: &AgentId,
        head: &Head,
        owner_id: &str,
        reason: TakeoverReason,
    ) -> Result<OwnerFence, AgentError> {
        let lease_hint = self.now().saturating_add(self.lease_ns());
        let command = Command::Takeover(Takeover {
            incarnation: head.incarnation,
            owner_id: owner_id.to_string(),
            observed_epoch: head.epoch,
            lease_hint_unix_ns: lease_hint,
        });
        let (epoch, incarnation, published) = match self.execute(agent, &command).await {
            Ok(out) => {
                let Reply::Owned { epoch } = out.reply else {
                    return Err(refused(
                        agent,
                        ErrorKind::OwnershipLost,
                        "takeover answered without an epoch",
                    ));
                };
                self.after_outcome(&out);
                (epoch, out.head.incarnation, out.published)
            }
            Err(e) if e.kind == ErrorKind::OwnershipLost => {
                self.reload(agent).await?;
                return Err(e);
            }
            // A takeover has no operation handle to resolve; HEAD settles it:
            // this owner at the next epoch means the lost write landed, the
            // observed epoch still standing means it did not, and anything
            // else means another writer moved the agent on.
            Err(e) if e.kind == ErrorKind::OutcomeUnknown => {
                let fresh = self.reload(agent).await?;
                let landed = fresh.owner_id == owner_id
                    && fresh.incarnation == head.incarnation
                    && fresh.epoch == head.epoch.saturating_add(1);
                if !landed {
                    let unchanged = fresh.incarnation == head.incarnation
                        && fresh.epoch == head.epoch
                        && fresh.owner_id == head.owner_id;
                    return Err(if unchanged {
                        refused(
                            agent,
                            ErrorKind::StoreUnavailable,
                            "takeover write did not apply",
                        )
                    } else {
                        refused(
                            agent,
                            ErrorKind::OwnershipLost,
                            "another writer advanced the epoch",
                        )
                    });
                }
                self.metrics
                    .outcome_unknown_resolved
                    .fetch_add(1, Ordering::Relaxed);
                (fresh.epoch, fresh.incarnation, true)
            }
            Err(e) => return Err(e),
        };
        if published {
            self.metrics.takeovers.fetch_add(1, Ordering::Relaxed);
        }
        tracing::debug!(agent = %agent.to_hex(), epoch, ?reason, "agents takeover");
        self.leases.grant(agent, owner_id, epoch, lease_hint);
        Ok(OwnerFence {
            incarnation,
            owner_id: owner_id.to_string(),
            epoch,
        })
    }

    /// Activate an agent for `owner_id`: a takeover when it has no owner,
    /// when its owner holds no active turn, or when the owner's lease and
    /// grace ran out; nothing when `owner_id` already owns it. Returns the
    /// owner's epoch.
    pub async fn activate(&self, agent: &AgentRef, owner_id: &str) -> Result<u64, AgentError> {
        let (kind, key) = parse_agent(agent)?;
        let agent = agent_id(&kind, &key);
        let head = self.reload(&agent).await?;
        if head.owner_id == owner_id {
            return Ok(head.epoch);
        }
        let Some(reason) = self.takeover_due(&agent, &head, owner_id) else {
            self.metrics
                .ownership_lost_refusals
                .fetch_add(1, Ordering::Relaxed);
            return Err(refused(
                &agent,
                ErrorKind::OwnershipLost,
                "another owner's lease is live",
            ));
        };
        Ok(self.take_over(&agent, &head, owner_id, reason).await?.epoch)
    }

    /// Move an agent to `owner_id` regardless of the current lease. Returns
    /// the new epoch.
    pub async fn takeover(
        &self,
        agent: &AgentRef,
        owner_id: &str,
        reason: TakeoverReason,
    ) -> Result<u64, AgentError> {
        let (kind, key) = parse_agent(agent)?;
        let agent = agent_id(&kind, &key);
        let head = self.reload(&agent).await?;
        Ok(self.take_over(&agent, &head, owner_id, reason).await?.epoch)
    }

    /// Renew a lease in this coordinator's table. Reads HEAD (one GET,
    /// never a write) and renews only when HEAD still names the lease's
    /// owner, epoch and turn; otherwise the worker has lost the turn.
    /// Returns the new expiry. The renewal is visible to this coordinator
    /// only: HEAD's lease hint is not advanced, so another coordinator still
    /// takes the agent over `lease_ms + heartbeat_grace_ms` after the
    /// takeover that granted the lease (see the module doc).
    pub async fn heartbeat(&self, lease: &TurnLease) -> Result<i64, AgentError> {
        let (agent, fence) = fence_from_lease(lease)?;
        let head = self.reload(&agent).await?;
        let holds = head.incarnation == fence.owner.incarnation
            && head.owner_id == fence.owner.owner_id
            && head.epoch == fence.owner.epoch
            && head
                .active_turn
                .as_ref()
                .is_some_and(|t| t.turn_id == fence.turn_id);
        if !holds {
            self.metrics
                .ownership_lost_refusals
                .fetch_add(1, Ordering::Relaxed);
            return Err(refused(
                &agent,
                ErrorKind::OwnershipLost,
                "lease no longer names HEAD's owner, epoch and turn",
            ));
        }
        let expires = self.now().saturating_add(self.lease_ns());
        self.leases
            .grant(&agent, &fence.owner.owner_id, fence.owner.epoch, expires);
        Ok(expires)
    }
}
