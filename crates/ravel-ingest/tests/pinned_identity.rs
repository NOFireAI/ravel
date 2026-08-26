//! A flush's identity (seq, ingest_hour_bucket, serialized bytes, hash) is
//! pinned once at flush open and must survive a retry even across an hour
//! boundary (docs/catalog-and-mvcc.md "Pinned flush identity", ADR-0010 §1).
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::Signal;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

#[tokio::test]
async fn retry_across_hour_boundary_keeps_the_pinned_ingest_hour() {
    // Start just before an hour boundary so the clock advance triggered by
    // the retry crosses it. Anchored above the receiver-clock plausibility
    // floor (ADR-0051 amendment): `checked_ingest_hour_bucket` now
    // rejects a flush-open reading below `MIN_PLAUSIBLE_INGEST_CLOCK_NS`.
    let start_ns = ravel_ingest::MIN_PLAUSIBLE_INGEST_CLOCK_NS + 20 * NS_PER_HOUR - 10_000_000_000;
    let opened_hour = u32::try_from(start_ns.div_euclid(NS_PER_HOUR)).expect("fits u32");

    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
            .with_key_contains("/l0/")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(start_ns);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_max_attempts: 4,
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(20),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    // Advance the clock past the hour boundary while the retry is in
    // flight; the flush's identity was pinned before this and must not
    // move to the new hour.
    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        async {
            // Advance only once the first data PUT has actually faulted, so
            // the clock genuinely moves while the retry is in flight. A flat
            // 5 ms sleep here could land after the flush had already closed
            // on a fast host, leaving the hour assertion below trivially
            // true and this test proving nothing.
            let mut retried = false;
            for _ in 0..2_000 {
                if fault_store.fault_count(Op::Put, FaultKind::DuplicateDelivery) > 0 {
                    retried = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert!(
                retried,
                "the retry this test advances the clock across was never injected"
            );
            clock.advance_ns(20_000_000_000);
        },
    );
    let receipt = write_result.expect("retried flush still succeeds");
    assert_eq!(receipt.tokens.len(), 1);
    let token = &receipt.tokens[0];
    assert_eq!(
        token.ingest_hour_bucket, opened_hour,
        "commit token must keep the hour pinned at flush open, not the advanced clock's hour"
    );

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let commit_key = objects
        .iter()
        .find(|o| o.key.contains("/c/"))
        .expect("commit record exists")
        .key
        .clone();
    let opened_hour_text = ravel_commit::keys::ingest_hour_string(opened_hour);
    assert!(
        commit_key.contains(&opened_hour_text),
        "commit key {commit_key} must live under the pinned hour {opened_hour_text}"
    );

    router.shutdown().await;
}
