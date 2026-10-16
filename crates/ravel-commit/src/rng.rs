//! Injected randomness so jitter and identity generation are deterministic
//! under the simulation harness (ADR-0068 decision 2), mirroring the injected
//! [`Clock`](../../ravel-ingest/src/clock.rs) pattern.
//!
//! Production code must never call `rand::rng()` / OS entropy directly for
//! backoff jitter or writer/process identity; every such draw goes through
//! this trait so the seeded simulation driver can pin, replay, and derive all
//! randomness from one master seed. The production default ([`SystemRng`])
//! still draws OS entropy, so behavior with the default source is unchanged.
//! `rand::rng()` and `Uuid::new_v4()` are therefore confined to [`SystemRng`],
//! the single blessed production entropy source.

use std::sync::Mutex;

use rand::rngs::StdRng;
use rand::{Rng as _, RngExt as _, SeedableRng as _};
use uuid::Uuid;

/// Source of the two kinds of randomness Ravel's hot paths draw: bounded
/// jitter for retry/loop backoff, and fresh v4 UUIDs for writer/process
/// identity. Stored, defaulted, and overridden exactly like `Clock`.
pub trait RngSource: Send + Sync + 'static {
    /// A uniformly random value in the inclusive range `0..=max_ms`.
    ///
    /// This is the exact shape every jitter site used before the seam
    /// (`rand::rng().random_range(0..=max_ms)`); the production
    /// [`SystemRng`] keeps that call verbatim, so the default distribution is
    /// unchanged.
    fn jitter_ms(&self, max_ms: u64) -> u64;

    /// A fresh random v4 UUID for writer/process identity.
    fn new_uuid(&self) -> Uuid;
}

impl<R: RngSource + ?Sized> RngSource for std::sync::Arc<R> {
    fn jitter_ms(&self, max_ms: u64) -> u64 {
        (**self).jitter_ms(max_ms)
    }

    fn new_uuid(&self) -> Uuid {
        (**self).new_uuid()
    }
}

/// Production source: OS entropy via the thread RNG and `Uuid::new_v4`. This
/// is the one place `rand::rng()` and `Uuid::new_v4()` may appear on a
/// production code path.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemRng;

impl RngSource for SystemRng {
    fn jitter_ms(&self, max_ms: u64) -> u64 {
        rand::rng().random_range(0..=max_ms)
    }

    fn new_uuid(&self) -> Uuid {
        Uuid::new_v4()
    }
}

/// Deterministic source constructed from a `u64` seed, for the simulation
/// harness. Every draw comes from a `StdRng` seeded once at construction, so
/// two `SeededRng`s built from the same seed emit identical jitter and UUID
/// sequences, and different seeds diverge.
///
/// The `StdRng` is behind a `Mutex` so the source is `Sync` and can be shared
/// (`Arc<dyn RngSource>`) across the shard actors and flush tasks that hold it,
/// matching how `Arc<dyn Clock>` is shared. The harness runs single-threaded
/// (ADR-0068 decision 1), so the lock is never contended; a poisoned lock is
/// recovered rather than unwrapped, since panicking here is forbidden on a
/// production code path.
#[derive(Debug)]
pub struct SeededRng {
    inner: Mutex<StdRng>,
}

impl SeededRng {
    /// Build a deterministic source from a `u64` seed.
    pub fn new(seed: u64) -> Self {
        SeededRng {
            inner: Mutex::new(StdRng::seed_from_u64(seed)),
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut StdRng) -> T) -> T {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut guard)
    }
}

impl RngSource for SeededRng {
    fn jitter_ms(&self, max_ms: u64) -> u64 {
        self.with(|rng| rng.random_range(0..=max_ms))
    }

    fn new_uuid(&self) -> Uuid {
        let mut bytes = [0u8; 16];
        self.with(|rng| rng.fill_bytes(&mut bytes));
        // `from_random_bytes` sets the v4 version and RFC-4122 variant bits,
        // yielding the same UUID shape `Uuid::new_v4` produces.
        uuid::Builder::from_random_bytes(bytes).into_uuid()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    const MAX_MS: u64 = 1_000;

    fn jitter_seq(rng: &dyn RngSource, n: usize) -> Vec<u64> {
        (0..n).map(|_| rng.jitter_ms(MAX_MS)).collect()
    }

    fn uuid_seq(rng: &dyn RngSource, n: usize) -> Vec<Uuid> {
        (0..n).map(|_| rng.new_uuid()).collect()
    }

    /// With a seeded source, the jitter sequence
    /// and the generated writer ids are identical across two runs with the
    /// same seed, and differ across two different seeds.
    #[test]
    fn seeded_rng_source_reproducible() {
        let seed = 0x5EED_0000_0000_0001_u64;
        let a = SeededRng::new(seed);
        let b = SeededRng::new(seed);

        let a_jitter = jitter_seq(&a, 64);
        let b_jitter = jitter_seq(&b, 64);
        assert_eq!(a_jitter, b_jitter, "same seed must reproduce jitter");

        let a_ids = uuid_seq(&a, 64);
        let b_ids = uuid_seq(&b, 64);
        assert_eq!(a_ids, b_ids, "same seed must reproduce writer ids");

        // A different seed must diverge (both jitter and ids).
        let c = SeededRng::new(seed ^ 0xFFFF_FFFF);
        assert_ne!(
            a_jitter,
            jitter_seq(&c, 64),
            "different seed must change jitter"
        );
        assert_ne!(a_ids, uuid_seq(&c, 64), "different seed must change ids");
    }

    /// Every seeded UUID must be a valid v4 (version/variant bits set), just
    /// like `Uuid::new_v4`, so identity keys are well-formed under the harness.
    #[test]
    fn seeded_uuid_is_v4() {
        let rng = SeededRng::new(7);
        for _ in 0..16 {
            assert_eq!(rng.new_uuid().get_version_num(), 4);
        }
    }

    /// The production source must not carry a fixed seed: distinct
    /// constructions and repeated draws produce distinct writer ids.
    #[test]
    fn system_rng_produces_distinct_ids() {
        let mut seen = HashSet::new();
        for _ in 0..64 {
            assert!(
                seen.insert(SystemRng.new_uuid()),
                "SystemRng repeated a UUID"
            );
        }
        // Two independent constructions also diverge.
        assert_ne!(SystemRng.new_uuid(), SystemRng.new_uuid());
    }
}
