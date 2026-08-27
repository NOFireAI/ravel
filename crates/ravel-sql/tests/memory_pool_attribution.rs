//! Attribution of a disabled-disk-manager spill refusal (issue #740, finding
//! 1). A `RepartitionExec` output channel refused by the pool falls to
//! `create_in_progress_file("SpillPool")`, and with the disk manager disabled
//! (ADR-0102 decision 3) that raises
//! `"Memory Exhausted while SpillPool (DiskManager is disabled)"`. The message
//! names the exchange that HOLDS the reservation, not the aggregate tables
//! that FILLED the pool, and carries no byte figures.
//!
//! [`SqlError::resources_exhausted_reattributed`] rewrites it from the pool's
//! own `reserved()`/limit at the moment of refusal. This test pins the exact
//! figures from a real [`TenantDelegatingPool`] and shows the rewrite is red
//! against the pass-through message.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool};
use ravel_sql::{
    CeilingBreach, MSG_SPILL_DISABLED_MARKER, SqlError, TenantDelegatingPool,
    TenantMemoryAccountant,
};
use ravel_types::accounting::QueryAccounting;

/// The message DataFusion 54 raises from a `RepartitionExec` output channel
/// whose `try_grow` the pool refused, once the disk manager is disabled.
const SPILL_MSG: &str = "Memory Exhausted while SpillPool (DiskManager is disabled)";

/// A spill refusal surfaces as the typed `ResourcesExhausted` carrying the
/// pool's `used`/`limit` (exact figures read from `MemoryPool::reserved()` and
/// the configured limit), not the pass-through exchange message.
///
/// Prove-the-test: [`SqlError::resources_exhausted_reattributed`] returns the
/// raw message verbatim when it lacks [`MSG_SPILL_DISABLED_MARKER`], so a
/// build that skipped the marker branch (or one that reverted to
/// `ResourcesExhausted(raw)`) would carry `"SpillPool"` and neither figure --
/// every assertion below on the two figures and on the marker's absence goes
/// red. The `pass_through` binding at the end is exactly that reverted form,
/// asserted to be the message the fix must NOT produce.
#[test]
fn a_spill_refusal_reports_the_pools_used_and_limit_not_the_exchange() {
    const QUERY_LIMIT: usize = 8000;
    const RESERVED: usize = 6144;

    let tenant = TenantMemoryAccountant::new(1 << 30);
    let pool: Arc<dyn MemoryPool> = Arc::new(TenantDelegatingPool::new(
        QUERY_LIMIT,
        tenant,
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));

    // The aggregate hash tables are what filled the pool: reserve their bytes
    // through the pool so `reserved()` reads the real occupancy at the moment
    // the (separate) exchange's spill is refused.
    let aggregate = MemoryConsumer::new("GroupedHashAggregateStream[0]").register(&pool);
    aggregate.try_grow(RESERVED).expect("within the ceiling");
    assert_eq!(pool.reserved(), RESERVED, "the pool holds the aggregate bytes");

    let MemoryLimit::Finite(limit) = pool.memory_limit() else {
        panic!("the query pool declares a finite limit");
    };
    assert_eq!(limit, QUERY_LIMIT, "the pool's limit is the configured ceiling");
    let err = SqlError::resources_exhausted_reattributed(SPILL_MSG, pool.reserved(), limit);

    let SqlError::ResourcesExhausted(message) = &err else {
        panic!("a spill refusal must stay a typed ResourcesExhausted; got {err:?}");
    };
    // The exact figures from the fixture's pool, both present.
    assert!(
        message.contains("6144"),
        "message must name the reserved bytes (used); got {message:?}"
    );
    assert!(
        message.contains("8000"),
        "message must name the limit; got {message:?}"
    );
    // Attribution is by consumer, not by the spilling holder: the exchange's
    // own name is gone and the message says whose bytes these are.
    assert!(
        !message.contains("SpillPool") && !message.contains(MSG_SPILL_DISABLED_MARKER),
        "the reattributed message must drop the exchange/spill wording; got {message:?}"
    );
    assert!(
        message.contains("consumer"),
        "the message must state attribution is by consumer; got {message:?}"
    );

    // The figures survive the client boundary verbatim (a budget error).
    let client = err.client_message();
    assert!(client.contains("6144") && client.contains("8000"), "{client}");

    // Red against the pass-through: the message the fix must NOT produce is the
    // raw exchange text with no figures.
    let pass_through = SqlError::ResourcesExhausted(SPILL_MSG.to_string());
    assert_ne!(
        message,
        &SPILL_MSG.to_string(),
        "the fix must not echo the exchange message"
    );
    assert!(
        pass_through.to_string().contains("SpillPool")
            && !pass_through.to_string().contains("6144"),
        "the pass-through form names the exchange and carries no figure, which is the \
         regression this test guards"
    );
}

/// A native `ResourcesExhausted` that is NOT a disabled-disk spill (the pool's
/// own `try_grow` text, which already carries figures) passes through
/// unchanged, so the reattribution helper is safe to route every native
/// `ResourcesExhausted` through.
#[test]
fn a_non_spill_message_passes_through_unchanged() {
    let raw = "query memory pool exhausted: 4096 more bytes on top of 6144 already reserved \
               exceeds per-query limit 8000";
    let err = SqlError::resources_exhausted_reattributed(raw, 6144, 8000);
    let SqlError::ResourcesExhausted(message) = &err else {
        panic!("still a ResourcesExhausted; got {err:?}");
    };
    assert_eq!(message, raw, "a non-spill message is returned verbatim");
}
