//! Deterministic rendezvous (highest-random-weight, HRW) subset selection.
//!
//! Given a tenant key and a set of replicas, [`rank`] produces a total order
//! over the replicas that depends only on the `(tenant_key, replica_id)` pairs,
//! never on the order in which the replicas were supplied. [`subset`] takes the
//! top `size` of that order.
//!
//! This is the ravel-native subset-affinity primitive from ADR-0080 decision 3.
//! It has no Kubernetes dependency: a [`ReplicaId`] wraps whatever stable
//! identity bytes the caller provides (ADR-0080 uses the gateway pod's
//! EndpointSlice UID, but this crate does not assume that).
//!
//! # HRW guarantee
//!
//! Each replica's score for a given tenant key is independent of every other
//! replica in the set. Adding or removing a single replica therefore leaves the
//! relative order of all the others unchanged, so `subset(tenant_key, .., size)`
//! only changes membership for tenants whose ranking crosses position `size` —
//! the standard bounded-reassignment property. A scheme that instead hashed
//! `tenant mod N` (or otherwise mixed the whole set into each score) would
//! reshuffle every tenant on any membership change; this one does not.

/// A replica's stable identity, as an opaque byte string chosen by the caller.
///
/// The bytes are used both as hash input and as the deterministic tie-break, so
/// two replicas with identical bytes are indistinguishable to this crate.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ReplicaId(Vec<u8>);

impl ReplicaId {
    /// Wrap identity bytes.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// The wrapped identity bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for ReplicaId {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for ReplicaId {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

/// The HRW score of a `(tenant_key, replica)` pair.
///
/// The tenant key is length-prefixed before the replica bytes so the two inputs
/// are domain-separated (no concatenation can be reinterpreted as a different
/// split). Ordering compares the raw 32-byte digest lexicographically.
fn score(tenant_key: &[u8], replica: &ReplicaId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(tenant_key.len() as u64).to_le_bytes());
    hasher.update(tenant_key);
    hasher.update(replica.as_bytes());
    *hasher.finalize().as_bytes()
}

/// Rank every replica for `tenant_key`, highest score first.
///
/// The result is a permutation of `replicas` (as borrows) that is fully
/// determined by the `(tenant_key, replica_id)` pairs: the input slice order
/// does not affect it. Ties on score (only reachable via a blake3 collision or
/// two replicas sharing identity bytes) are broken by replica-id bytes
/// ascending, so the order is total and deterministic.
pub fn rank<'a>(tenant_key: &[u8], replicas: &'a [ReplicaId]) -> Vec<&'a ReplicaId> {
    let mut scored: Vec<([u8; 32], &'a ReplicaId)> = replicas
        .iter()
        .map(|replica| (score(tenant_key, replica), replica))
        .collect();
    // Score descending; replica bytes ascending as the deterministic tie-break.
    scored.sort_by(|(sa, ra), (sb, rb)| sb.cmp(sa).then_with(|| ra.0.cmp(&rb.0)));
    scored.into_iter().map(|(_, replica)| replica).collect()
}

/// The top-`size` replicas for `tenant_key` in HRW order.
///
/// `size == 0` returns an empty vec; `size >= replicas.len()` returns the whole
/// ranking (no truncation), since [`Iterator::take`] saturates.
pub fn subset<'a>(tenant_key: &[u8], replicas: &'a [ReplicaId], size: usize) -> Vec<&'a ReplicaId> {
    rank(tenant_key, replicas).into_iter().take(size).collect()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn replicas(n: usize) -> Vec<ReplicaId> {
        (0..n)
            .map(|i| ReplicaId::new(format!("replica-{i}").into_bytes()))
            .collect()
    }

    fn tenant_keys(n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| format!("tenant-{i}").into_bytes()).collect()
    }

    /// A deterministic, seed-free permutation of a slice (no `rand` dependency).
    /// Reverses, then rotates by `shift`, so different shifts give genuinely
    /// different input orders.
    fn permute(reps: &[ReplicaId], shift: usize) -> Vec<ReplicaId> {
        let mut v: Vec<ReplicaId> = reps.iter().rev().cloned().collect();
        let n = v.len().max(1);
        v.rotate_left(shift % n);
        v
    }

    /// Compare a `Vec<&ReplicaId>` to expected values by identity, not position
    /// in the original slice.
    fn values(order: &[&ReplicaId]) -> Vec<ReplicaId> {
        order.iter().map(|r| (*r).clone()).collect()
    }

    #[test]
    fn determinism_same_inputs_same_output() {
        let reps = replicas(7);
        let key = b"tenant-42";
        let first = values(&rank(key, &reps));
        for _ in 0..5 {
            assert_eq!(values(&rank(key, &reps)), first);
        }
    }

    #[test]
    fn ordering_independence_shuffled_input_same_result() {
        let reps = replicas(9);
        let key = b"tenant-xyz";
        let baseline = values(&rank(key, &reps));
        assert_eq!(baseline.len(), 9);
        // Every permutation of the input must produce the identical ranking.
        for shift in 0..reps.len() {
            let shuffled = permute(&reps, shift);
            assert_eq!(
                values(&rank(key, &shuffled)),
                baseline,
                "input order changed the ranking at shift {shift}"
            );
        }
    }

    #[test]
    fn size_one_returns_single_hrw_winner() {
        let reps = replicas(6);
        let key = b"tenant-1";
        let winner = subset(key, &reps, 1);
        assert_eq!(winner.len(), 1);
        // The single winner is the head of the full ranking.
        assert_eq!(winner[0], rank(key, &reps)[0]);
    }

    #[test]
    fn size_two_returns_top_two_in_hrw_order() {
        let reps = replicas(6);
        let key = b"tenant-2";
        let full = rank(key, &reps);
        let two = subset(key, &reps, 2);
        assert_eq!(two.len(), 2);
        assert_eq!(values(&two), values(&full[..2]));
    }

    #[test]
    fn size_at_or_above_len_returns_all_ranked_no_truncation() {
        let reps = replicas(5);
        let key = b"tenant-3";
        let full = values(&rank(key, &reps));
        for size in [reps.len(), reps.len() + 1, reps.len() + 100] {
            let s = subset(key, &reps, size);
            assert_eq!(s.len(), reps.len(), "size {size} truncated or padded");
            assert_eq!(values(&s), full, "size {size} changed the order");
        }
    }

    #[test]
    fn empty_replicas_and_size_zero_do_not_panic() {
        let empty: Vec<ReplicaId> = Vec::new();
        assert!(rank(b"t", &empty).is_empty());
        assert!(subset(b"t", &empty, 3).is_empty());

        let reps = replicas(4);
        assert!(subset(b"t", &reps, 0).is_empty());
        assert!(subset(b"t", &empty, 0).is_empty());
    }

    /// The core HRW invariant behind bounded reassignment: the ranking over a
    /// set with one member removed equals the full ranking with that member
    /// filtered out, relative order of everyone else untouched. A
    /// re-hash-everything scheme (e.g. `hash(tenant, i) where i = index mod N`)
    /// would violate this because each score depends on `N`.
    #[test]
    fn removing_one_replica_preserves_relative_order() {
        let reps = replicas(8);
        for key in tenant_keys(20) {
            let full = values(&rank(&key, &reps));
            for drop_idx in 0..reps.len() {
                let mut smaller = reps.clone();
                let removed = smaller.remove(drop_idx);
                let expected: Vec<ReplicaId> =
                    full.iter().filter(|r| **r != removed).cloned().collect();
                assert_eq!(
                    values(&rank(&key, &smaller)),
                    expected,
                    "removing {removed:?} reordered the survivors"
                );
            }
        }
    }

    /// Bounded reassignment on removal: for a fixed subset size `S`, removing
    /// one replica changes `subset(.., S)` membership for exactly the tenants
    /// whose old subset contained the removed replica, and for no others.
    ///
    /// This would FAIL against a naive re-hash-everything implementation
    /// (score depending on set size or on the replica's index in the slice):
    /// there, dropping any replica reshuffles scores globally and most tenants'
    /// subsets would change even though the removed replica was not in them.
    #[test]
    fn bounded_reassignment_on_removal() {
        let reps = replicas(8);
        let s = 3usize;
        let drop_idx = 4;
        let removed = reps[drop_idx].clone();
        let mut smaller = reps.clone();
        smaller.remove(drop_idx);

        let mut changed_and_contained = 0usize;
        let mut unchanged_and_absent = 0usize;

        for key in tenant_keys(60) {
            let before: Vec<ReplicaId> = values(&subset(&key, &reps, s));
            let after: Vec<ReplicaId> = values(&subset(&key, &smaller, s));
            let contained = before.contains(&removed);

            if contained {
                // Membership must change: the removed replica leaves and the
                // next-ranked survivor enters. Everyone else in the subset stays.
                assert_ne!(before, after, "subset unchanged despite dropping a member");
                assert!(
                    !after.contains(&removed),
                    "removed replica still present after removal"
                );
                let kept: Vec<&ReplicaId> = before.iter().filter(|r| **r != removed).collect();
                for k in &kept {
                    assert!(
                        after.iter().any(|r| &r == k),
                        "a non-removed member was evicted from the subset"
                    );
                }
                changed_and_contained += 1;
            } else {
                // Removed replica was not in this tenant's subset: it must be
                // byte-for-byte identical. This is the assertion a
                // re-hash-everything scheme fails.
                assert_eq!(
                    before, after,
                    "subset changed for a tenant whose subset did not contain the removed replica"
                );
                unchanged_and_absent += 1;
            }
        }

        // Non-vacuous: both branches must actually be exercised, otherwise the
        // test proves nothing about the stability/movement split.
        assert!(
            changed_and_contained > 0,
            "no tenant had the removed replica in its subset; test is vacuous"
        );
        assert!(
            unchanged_and_absent > 0,
            "no tenant kept its subset; test is vacuous"
        );
    }

    /// Bounded reassignment on addition: adding one replica changes
    /// `subset(.., S)` only for tenants whose subset the new replica ranks into.
    #[test]
    fn bounded_reassignment_on_addition() {
        let base = replicas(7);
        let added = ReplicaId::new(b"replica-new".to_vec());
        let mut grown = base.clone();
        grown.push(added.clone());
        let s = 3usize;

        let mut entered = 0usize;
        let mut untouched = 0usize;

        for key in tenant_keys(60) {
            let before: Vec<ReplicaId> = values(&subset(&key, &base, s));
            let after: Vec<ReplicaId> = values(&subset(&key, &grown, s));
            let new_in_after = after.contains(&added);

            if new_in_after {
                // New replica ranked into the top S: it displaces exactly the
                // old tail member; the higher-ranked members are unchanged.
                assert_ne!(before, after);
                let survivors: Vec<&ReplicaId> = after.iter().filter(|r| **r != added).collect();
                for sv in &survivors {
                    assert!(
                        before.iter().any(|r| &r == sv),
                        "addition introduced a member other than the new replica"
                    );
                }
                entered += 1;
            } else {
                assert_eq!(
                    before, after,
                    "subset changed for a tenant the new replica did not rank into"
                );
                untouched += 1;
            }
        }

        assert!(
            entered > 0,
            "new replica never entered any subset; test vacuous"
        );
        assert!(
            untouched > 0,
            "new replica entered every subset; test vacuous"
        );
    }
}
