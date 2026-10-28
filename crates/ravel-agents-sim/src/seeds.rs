//! Seed domains for agent simulations.
//!
//! Every random choice a simulation makes comes from one [`MasterSeed`],
//! split by domain so that adding a draw in one domain leaves every other
//! domain's sequence unchanged. The domain names are distinct from the ones
//! `ravel-sim` already uses, so an agent run and a storage run under the
//! same master seed draw independent sequences.

use rand::rngs::StdRng;
use ravel_commit::rng::SeededRng;
use ravel_sim::MasterSeed;

/// Which events a client issues and in what order.
pub const WORKLOAD_DOMAIN: &str = "agents-workload";
/// Which store calls fail, and how.
pub const FAULTS_DOMAIN: &str = "agents-faults";
/// Ids the runtime draws through its `RngSource` seam.
pub const IDS_DOMAIN: &str = "agents-ids";
/// Release and crash order of held store calls.
pub const SCHEDULE_DOMAIN: &str = "agents-schedule";

/// The per-domain generators of one agent simulation run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentSeeds {
    pub master: MasterSeed,
}

impl AgentSeeds {
    pub fn new(seed: u64) -> AgentSeeds {
        AgentSeeds {
            master: MasterSeed::new(seed),
        }
    }

    /// `RAVEL_SIM_SEED` when set and parseable, else `default`.
    pub fn from_env_or(default: u64) -> AgentSeeds {
        AgentSeeds {
            master: MasterSeed::from_env_or(default),
        }
    }

    pub fn seed(&self) -> u64 {
        self.master.0
    }

    pub fn workload_rng(&self) -> StdRng {
        self.master.rng(WORKLOAD_DOMAIN)
    }

    pub fn faults_rng(&self) -> StdRng {
        self.master.rng(FAULTS_DOMAIN)
    }

    pub fn schedule_rng(&self) -> StdRng {
        self.master.rng(SCHEDULE_DOMAIN)
    }

    /// The `RngSource` to hand the runtime, so every id it draws replays
    /// from the seed.
    pub fn ids(&self) -> SeededRng {
        SeededRng::new(self.master.sub_seed(IDS_DOMAIN))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ravel_commit::rng::RngSource;

    const RAVEL_SIM_DOMAINS: [&str; 6] = [
        "workload",
        "clock",
        "ingest-rng",
        "fold-id",
        "compactor-id",
        "faults",
    ];
    const AGENT_DOMAINS: [&str; 4] = [WORKLOAD_DOMAIN, FAULTS_DOMAIN, IDS_DOMAIN, SCHEDULE_DOMAIN];

    #[test]
    fn agent_domains_draw_distinct_sub_seeds_from_each_other_and_from_ravel_sim() {
        for seed in [0, 1, 42, u64::MAX] {
            let master = MasterSeed::new(seed);
            let mut all: Vec<u64> = AGENT_DOMAINS
                .iter()
                .chain(RAVEL_SIM_DOMAINS.iter())
                .map(|d| master.sub_seed(d))
                .collect();
            all.sort_unstable();
            all.dedup();
            assert_eq!(all.len(), AGENT_DOMAINS.len() + RAVEL_SIM_DOMAINS.len());
        }
    }

    #[test]
    fn ids_replay_from_the_seed() {
        let a = AgentSeeds::new(7).ids();
        let b = AgentSeeds::new(7).ids();
        let c = AgentSeeds::new(8).ids();
        let draw = |r: &SeededRng| (0..8).map(|_| r.jitter_ms(1_000_000)).collect::<Vec<_>>();
        let first = draw(&a);
        assert_eq!(first, draw(&b));
        assert_ne!(first, draw(&c));
        assert_eq!(a.new_uuid(), b.new_uuid());
    }

    #[test]
    fn the_schedule_domain_sub_seed_is_pinned() {
        // FNV-1a over the seed's little-endian bytes and the domain name; a
        // change here invalidates every recorded failure seed.
        assert_eq!(
            AgentSeeds::new(42).master.sub_seed(SCHEDULE_DOMAIN),
            fnv1a(42, SCHEDULE_DOMAIN)
        );
    }

    fn fnv1a(seed: u64, domain: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in seed.to_le_bytes().into_iter().chain(domain.bytes()) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }
}
