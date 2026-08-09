//! Seeded foundation (ADR-0068 deliverable 1): every sub-system draws its
//! randomness from one master seed via domain-separated hashing into
//! `StdRng::seed_from_u64`, so a whole cycle is reproducible from a single
//! `u64` and a failure can be replayed with `RAVEL_SIM_SEED=<seed>`, for
//! every entry point that resolves its seed via [`MasterSeed::from_env_or`]
//! rather than [`MasterSeed::new`] directly -- see that method's doc for
//! which ones do.

use rand::SeedableRng;
use rand::rngs::StdRng;

/// Environment variable that overrides the seed a caller would otherwise
/// pick, for replaying a reported failure: `RAVEL_SIM_SEED=<n> cargo test -p
/// ravel-sim <test>`.
pub const RAVEL_SIM_SEED_ENV: &str = "RAVEL_SIM_SEED";

/// The one seed a whole simulated cycle is reproducible from. Every
/// sub-system (workload generation, clock-stepping jitter, and future
/// domains such as fault schedules) derives its own `StdRng` from this via
/// [`MasterSeed::sub_seed`], never from OS entropy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MasterSeed(pub u64);

impl MasterSeed {
    pub fn new(seed: u64) -> Self {
        MasterSeed(seed)
    }

    /// `RAVEL_SIM_SEED` overrides `default` when set and parses as a `u64`;
    /// an unset or unparseable value falls back to `default` rather than
    /// erroring, so a plain `cargo test` run never needs the variable set.
    ///
    /// Only a test that wants to be replayable this way should call this
    /// instead of [`MasterSeed::new`] directly: `tests/seed_batch.rs` and
    /// `tests/different_seed_different_workload.rs` deliberately don't, since
    /// each already names its seed(s) directly in its own assertion output
    /// (a batch of fixed seeds, and an explicit seed-vs-seed comparison)
    /// where an env override would either be redundant or would break the
    /// comparison itself.
    pub fn from_env_or(default: u64) -> Self {
        MasterSeed(resolve_seed(
            std::env::var(RAVEL_SIM_SEED_ENV).ok(),
            default,
        ))
    }

    /// Derives a domain's sub-seed by FNV-1a hashing this master seed with a
    /// domain tag (e.g. `"workload"`, `"clock"`).
    ///
    /// FNV-1a rather than `std::hash::DefaultHasher`: the standard library
    /// explicitly does not guarantee `DefaultHasher`'s algorithm is stable
    /// across toolchain versions, and a per-domain seed that silently
    /// changed on a compiler bump would break every previously-recorded
    /// failure seed's replay. FNV-1a is fully specified and never changes.
    pub fn sub_seed(&self, domain: &str) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut hash = FNV_OFFSET;
        for byte in self.0.to_le_bytes().into_iter().chain(domain.bytes()) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    /// A `StdRng` seeded from this master seed's sub-seed for `domain`.
    pub fn rng(&self, domain: &str) -> StdRng {
        StdRng::seed_from_u64(self.sub_seed(domain))
    }
}

/// Pure decision logic behind [`MasterSeed::from_env_or`], split out so it
/// is testable without mutating process environment state (`env::set_var`
/// is `unsafe` and this workspace forbids `unsafe_code` outright).
fn resolve_seed(raw: Option<String>, default: u64) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_seed_is_deterministic_and_domain_separated() {
        let seed = MasterSeed::new(42);
        assert_eq!(seed.sub_seed("workload"), seed.sub_seed("workload"));
        assert_ne!(seed.sub_seed("workload"), seed.sub_seed("clock"));
    }

    #[test]
    fn different_master_seeds_derive_different_sub_seeds() {
        assert_ne!(
            MasterSeed::new(1).sub_seed("workload"),
            MasterSeed::new(2).sub_seed("workload")
        );
    }

    #[test]
    fn resolve_seed_prefers_parseable_override() {
        assert_eq!(resolve_seed(Some(" 777 ".to_string()), 1), 777);
    }

    #[test]
    fn resolve_seed_falls_back_on_missing_or_unparseable() {
        assert_eq!(resolve_seed(None, 1), 1);
        assert_eq!(resolve_seed(Some("not-a-number".to_string()), 1), 1);
    }
}
