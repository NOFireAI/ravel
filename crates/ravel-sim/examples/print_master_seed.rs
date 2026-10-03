//! Prints the master seed `MasterSeed::from_env_or(1)` resolves to, so
//! `tests/env_seed_override_changes_seed.rs` can assert the resolved value
//! actually changes under `RAVEL_SIM_SEED` from outside the test process
//! (`std::env::set_var` is `unsafe` and this workspace forbids
//! `unsafe_code`, so the override can't be exercised in-process).

use ravel_sim::MasterSeed;

fn main() {
    println!("{}", MasterSeed::from_env_or(1).0);
}
