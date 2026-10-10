//! Per-(shard, tenant) flush shares (ADR-2708 D3), shared by the metrics, log,
//! and span shard actors.
//!
//! Each shard keeps exactly one FIFO flush semaphore of `max_inflight_flushes`
//! permits. On top of it, every tenant on the shard owns a share of
//! [`crate::IngestConfig::flush_share_per_tenant`] flushes, counted twice:
//!
//! - `spawned` counts the tenant's flush tasks spawned and not yet finished.
//!   The actor reads it at a trigger and defers an ordinary trigger once it
//!   reaches the share, and lets a tenant with nothing spawned past the
//!   queued-flush cap.
//! - `permits` is a semaphore of `share` permits that a flush task takes
//!   before the shard permit. Ordinary triggers never exceed the share, so it
//!   only parks the backstop-exempt flushes, which keeps a tenant whose
//!   prefix is held from occupying the last shard permit with them.
//!
//! With the default `share = max_inflight_flushes - 1`, one tenant with every
//! flush hung leaves at least one shard permit for its neighbours.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ravel_types::TenantHash;
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

/// One tenant's share on one shard.
#[derive(Debug)]
struct TenantShare {
    permits: Arc<Semaphore>,
    spawned: AtomicUsize,
}

/// The per-tenant shares of one shard actor. Lives on the actor; only the
/// [`ShareSlot`]s it hands out travel into flush tasks.
#[derive(Debug)]
pub(crate) struct TenantFlushShares {
    share: usize,
    tenants: HashMap<TenantHash, Arc<TenantShare>>,
}

impl TenantFlushShares {
    pub(crate) fn new(share: usize) -> Self {
        Self {
            share: share.max(1),
            tenants: HashMap::new(),
        }
    }

    /// Flush tasks `tenant` has spawned on this shard and not yet finished.
    pub(crate) fn in_flight(&self, tenant: TenantHash) -> usize {
        self.tenants
            .get(&tenant)
            .map_or(0, |s| s.spawned.load(Ordering::Acquire))
    }

    /// Whether `tenant` has reached its share, the point at which an ordinary
    /// trigger is deferred.
    pub(crate) fn at_share(&self, tenant: TenantHash) -> bool {
        self.in_flight(tenant) >= self.share
    }

    /// Counts a flush task for `tenant`, to be moved into the task it is
    /// spawned for. The count drops when the slot does.
    pub(crate) fn enter(&mut self, tenant: TenantHash) -> ShareSlot {
        let share = self.share;
        let entry = self.tenants.entry(tenant).or_insert_with(|| {
            Arc::new(TenantShare {
                permits: Arc::new(Semaphore::new(share)),
                spawned: AtomicUsize::new(0),
            })
        });
        entry.spawned.fetch_add(1, Ordering::AcqRel);
        ShareSlot {
            share: Arc::clone(entry),
        }
    }

    /// Forgets tenants with no flush task left, so the map tracks only the
    /// tenants with work in flight. Called after the actor reaps.
    pub(crate) fn prune(&mut self) {
        self.tenants
            .retain(|_, s| s.spawned.load(Ordering::Acquire) > 0);
    }
}

/// One spawned flush task's place in its tenant's share. Declare it before
/// the permits [`ShareSlot::acquire`] returns so it drops after them.
#[derive(Debug)]
pub(crate) struct ShareSlot {
    share: Arc<TenantShare>,
}

/// The two permits a flush task holds while it runs: its tenant's and the
/// shard's.
#[derive(Debug)]
pub(crate) struct FlushPermits {
    _tenant: OwnedSemaphorePermit,
    _shard: OwnedSemaphorePermit,
}

impl ShareSlot {
    /// Takes a tenant permit, then a permit from the shard's one FIFO
    /// semaphore. Waiting on the tenant permit first means a tenant over its
    /// share never queues on the shard semaphore ahead of a neighbour.
    pub(crate) async fn acquire(
        &self,
        shard: &Arc<Semaphore>,
    ) -> Result<FlushPermits, AcquireError> {
        let tenant = Arc::clone(&self.share.permits).acquire_owned().await?;
        let shard = Arc::clone(shard).acquire_owned().await?;
        Ok(FlushPermits {
            _tenant: tenant,
            _shard: shard,
        })
    }
}

impl Drop for ShareSlot {
    fn drop(&mut self) {
        self.share.spawned.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn hash(n: u8) -> TenantHash {
        TenantHash([n; 16])
    }

    #[tokio::test]
    async fn a_tenant_over_its_share_waits_on_its_own_permit_not_the_shards() {
        let shard = Arc::new(Semaphore::new(2));
        let mut shares = TenantFlushShares::new(1);
        let a1 = shares.enter(hash(1));
        let a2 = shares.enter(hash(1));
        assert_eq!(shares.in_flight(hash(1)), 2);
        assert!(shares.at_share(hash(1)));
        assert!(!shares.at_share(hash(2)));
        let held = a1.acquire(&shard).await;
        assert!(held.is_ok());
        // a2 parks on the tenant permit, leaving the second shard permit free.
        assert!(a2.acquire(&shard).now_or_never().is_none());
        assert_eq!(shard.available_permits(), 1);
        let b = shares.enter(hash(2));
        assert!(b.acquire(&shard).await.is_ok());
        drop(a1);
        drop(a2);
        drop(b);
        shares.prune();
        assert_eq!(shares.in_flight(hash(1)), 0);
        assert!(shares.tenants.is_empty());
    }
}
