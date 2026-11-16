//! #1113 traceability (commit row 6, SupersedeRecord): a commit-token query
//! for an L0 flush a real compaction has superseded.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use common::*;
use ravel_maintain::{Clock, CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::{CommitToken, Signal, TimeRange};
use uuid::Uuid;

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

/// SupersedeRecord (commit traceability row 6): a commit-token query for an
/// L0 flush a real compaction has superseded must answer superseded, served
/// from the compaction record's parts, not a plain miss. The raw commit
/// record is removed the way the superseded-input sweep eventually leaves it
/// (that sweep is a separate traceability row; this row names
/// `compact_bucket`, not the sweep), so the resolver's exact-key GET
/// genuinely misses and the fallback's compaction-record branch is the one
/// that must serve the answer.
///
/// To watch this FAIL, make `resolve_min_token_fallback` treat every
/// compaction record's `covers` check as false (as if no compaction record
/// ever names the token's identity): the resolve call then returns
/// `CatalogError::UnsatisfiableToken` instead of a snapshot naming the L1
/// parts, and the `.expect(...)` below panics.
#[tokio::test]
async fn superseded_record_token_query_reports_superseded() {
    let store = Arc::new(MemoryStore::new());
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);

    let kept = InputSpec::new(
        Uuid::from_u128(0x81),
        1,
        1,
        vec![raw_series("m", &[("k", "a")], &[(1_000, 1.0)])],
    );
    let superseded = InputSpec::new(
        Uuid::from_u128(0x82),
        2,
        1,
        vec![raw_series("m", &[("k", "b")], &[(2_000, 2.0)])],
    );
    seed_input(store.as_ref(), &kept).await;
    let superseded_key = seed_input(store.as_ref(), &superseded).await;

    let config = cfg();
    let compacted = compact_bucket(store.as_ref(), &clock, &config, &bucket())
        .await
        .expect("compact");
    assert!(matches!(compacted, CompactionOutcome::Compacted { .. }));

    // The raw L0 input the superseded-input sweep would eventually delete:
    // remove it directly so the token query's exact-key GET genuinely misses,
    // isolating the fallback's compaction-record branch from the sweep that
    // (in production) produces the same precondition.
    store
        .delete(&superseded_key)
        .await
        .expect("delete raw L0 record");

    let token = CommitToken {
        shard: SHARD,
        writer_id: superseded.writer_id,
        epoch: superseded.epoch,
        seq: superseded.seq,
        ingest_hour_bucket: HOUR,
    };
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = ravel_catalog::Catalog::new(
        dyn_store,
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("catalog");
    let range = TimeRange {
        start_ns: i64::from(HOUR) * NS_PER_HOUR,
        end_ns: (i64::from(HOUR) + 1) * NS_PER_HOUR,
    };
    let snapshot = catalog
        .resolve(
            &tenant_hash(),
            Signal::Metrics,
            range,
            &[token],
            clock.now_ns(),
        )
        .await
        .expect(
            "a superseded token resolves through the compaction record, not UnsatisfiableToken",
        );
    assert!(
        !snapshot.segments.is_empty(),
        "the superseded flush is served from the compaction record's parts, not zero segments"
    );
    assert!(
        snapshot
            .segments
            .iter()
            .all(|s| s.ingest_hour_bucket == HOUR),
        "served segments belong to the compacted bucket's hour"
    );
}
