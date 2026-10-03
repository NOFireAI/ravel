//! Crate-local glue for the shared test scaffolding in `ravel-test-support`.
//!
//! [`ParkOnFirstArmedCall`] lives in that crate so the health-listener and
//! CPU-gate tests can use it too (ADR-1702), and that crate depends on no
//! Ravel crate, so it cannot implement this crate's [`Clock`]. The forwarding
//! impl therefore sits here, on the side that owns the trait.

use ravel_test_support::ParkOnFirstArmedCall;

use crate::clock::Clock;

impl Clock for ParkOnFirstArmedCall {
    fn now_ns(&self) -> u64 {
        ParkOnFirstArmedCall::now_ns(self)
    }
}
