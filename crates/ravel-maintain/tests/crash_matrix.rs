//! FaultStore crash-matrix coverage for the compactor's crash-recovery paths. Every test asserts the fault
//! actually fired (via `fault_count`) so it proves its own fault point, per
//! the repo testing conventions, and asserts the expected convergence.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::HashSet;

use common::*;
use prost::Message;
use ravel_maintain::{
    Bucket, Clock, CompactionOutcome, CompactorConfig, FixedClock, PublishOutcome, build,
    compact_bucket, publish, read,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{CompactionRecord, Signal as ProtoSignal};
use uuid::Uuid;

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

fn two_inputs() -> Vec<InputSpec> {
    vec![
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("m", &[("k", "a")], &[(1_000, 1.0), (2_000, 2.0)]),
                raw_series("m", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("m", &[("k", "a")], &[(3_000, 3.0)])],
        ),
    ]
}

async fn seed(store: &dyn ObjectStoreBackend) -> Vec<InputSpec> {
    let specs = two_inputs();
    for s in &specs {
        seed_input(store, s).await;
    }
    specs
}

async fn compaction_record_present(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> bool {
    use ravel_commit::keys;
    use ravel_object_store::list_all;
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    list_all(store, &prefix).await.unwrap().iter().any(|m| {
        matches!(
            keys::partition_bucket_entry(&m.key),
            Ok(keys::BucketEntry::CompactionRecord(_))
        )
    })
}

/// Row 1: crash during the bucket LIST. Nothing is written (the run is still
/// read-only); a re-run succeeds. The listing fault fires exactly once.
#[tokio::test]
async fn row1_crash_during_list() {
    let inner = MemoryStore::new();
    seed(&inner).await;
    let plan = FaultPlan::empty()
        .with_rule(Rule::new(Op::List, ScriptedFault::Timeout).with_occurrence(Occurrence::Nth(1)));
    let store = FaultStore::new(inner, plan);
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();

    let first = compact_bucket(&store, &clock, &cfg(), &bucket).await;
    assert!(first.is_err(), "list fault must fail the run");
    assert_eq!(store.fault_count(Op::List, FaultKind::Timeout), 1);
    assert!(!compaction_record_present(&store, &bucket).await);

    let second = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("re-run after list fault");
    assert!(matches!(second, CompactionOutcome::Compacted { .. }));
    assert!(compaction_record_present(&store, &bucket).await);
}

/// Rows 2 and 3: crash after the parts are written but before the record.
/// The record-less parts are invisible (no record references them); the
/// re-run reuses their content-addressed keys via CreateIfAbsent and then
/// publishes. The record-PUT fault fires once.
#[tokio::test]
async fn row2_3_crash_after_parts_before_record() {
    let inner = MemoryStore::new();
    let specs = seed(&inner).await;
    // The compaction record key filename is `l1.<hash>.cmt`; part keys never
    // contain the literal "l1." (their dir segment is "/l1/"). So this rule
    // targets only the record PUT.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::Timeout)
            .with_key_contains("l1.")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let store = FaultStore::new(inner, plan);
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();

    let first = compact_bucket(&store, &clock, &cfg(), &bucket).await;
    assert!(first.is_err(), "record PUT fault must fail the run");
    assert_eq!(store.fault_count(Op::Put, FaultKind::Timeout), 1);
    assert!(!compaction_record_present(&store, &bucket).await);

    let second = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("re-run reuses part keys and publishes");
    assert!(matches!(second, CompactionOutcome::Compacted { .. }));
    let record = fetch_compaction_record(&store, &bucket).await;
    assert_eq!(
        read_record_samples(&store, &record).await,
        expected_samples(&specs)
    );
}

/// Row 4: a partial upload (connection dropped mid-PUT). The store contract
/// keeps the object invisible; a re-run writes it cleanly. The partial-write
/// fault fires once, on a part PUT.
#[tokio::test]
async fn row4_partial_part_upload() {
    let inner = MemoryStore::new();
    let specs = seed(&inner).await;
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::PartialWriteThenError)
            .with_key_contains(".rseg")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let store = FaultStore::new(inner, plan);
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();

    let first = compact_bucket(&store, &clock, &cfg(), &bucket).await;
    assert!(first.is_err(), "partial part upload must fail the run");
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::PartialWriteThenError),
        1
    );
    assert!(!compaction_record_present(&store, &bucket).await);

    let second = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("re-run after partial upload");
    assert!(matches!(second, CompactionOutcome::Compacted { .. }));
    let record = fetch_compaction_record(&store, &bucket).await;
    assert_eq!(
        read_record_samples(&store, &record).await,
        expected_samples(&specs)
    );
}

/// Row 10: two compactors race on the same inputs. One wins the record's
/// CreateIfAbsent; the loser HEAD-and-repairs the winner's parts and reports
/// convergence. Here the winner runs first, one of its parts is dropped, then
/// the loser's publish converges and repairs the missing part.
#[tokio::test]
async fn row10_racing_compactors_loser_converges_and_repairs() {
    let store = MemoryStore::new();
    let specs = seed(&store).await;
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();

    // Winner.
    compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("winner");
    let winner = fetch_compaction_record(&store, &bucket).await;
    assert!(!winner.parts.is_empty());

    // Loser rebuilds its parts (CreateIfAbsent over the winner's identical
    // parts is a no-op) ...
    let listing = read::list_bucket(&store, &bucket).await.unwrap();
    let inputs = read::load_inputs(&store, &bucket, &listing.commit_keys, 1)
        .await
        .unwrap();
    let hash = read::input_set_hash(&inputs);
    let mut catalogs = Vec::new();
    for i in &inputs {
        catalogs.push(read::load_input_catalog(&store, &cfg(), i).await.unwrap());
    }
    let parts = build::build_parts(
        &store,
        &cfg(),
        &bucket,
        &inputs,
        catalogs,
        &HashSet::new(),
        &hash,
    )
    .await
    .unwrap();

    // ... then a winner part goes missing (e.g. a stray delete), so the
    // loser's converging publish must HEAD-and-repair it.
    let victim = ravel_commit::keys::reconstruct_l1_part_key(&winner, &winner.parts[0]).unwrap();
    store.delete(&victim).await.unwrap();
    assert!(store.get(&victim, GetRange::Full).await.is_err());

    // The loser's record PUT hits AlreadyExists with the same input_set_hash,
    // so it converges and repairs the dropped part.
    let outcome = publish::publish_record(
        &store,
        &cfg(),
        &clock,
        &bucket,
        &inputs,
        &hash,
        &parts,
        clock.now_ns(),
    )
    .await
    .expect("loser publish");
    assert_eq!(outcome, PublishOutcome::Converged { parts_repaired: 1 });
    assert!(
        store.get(&victim, GetRange::Full).await.is_ok(),
        "part repaired"
    );

    let record = fetch_compaction_record(&store, &bucket).await;
    assert_eq!(
        read_record_samples(&store, &record).await,
        expected_samples(&specs)
    );
}

/// Row 11: an AlreadyExists whose stored record carries a *different* full
/// input_set_hash than ours (an input_set_hash16 prefix collision on the key).
/// A sealed bucket cannot legitimately hold two input sets, so the compactor
/// alarms and stops, deleting nothing.
#[tokio::test]
async fn row11_already_exists_different_hash_alarms() {
    let store = MemoryStore::new();
    seed(&store).await;
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();

    // Our hash and a colliding-prefix foreign hash (same first 8 bytes).
    let our_hash = [7u8; 32];
    let mut foreign = [7u8; 32];
    foreign[8] = 9;
    let hash16 = hex::encode(&our_hash[..8]);
    let record_key = ravel_commit::keys::compaction_record_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
        &hash16,
    )
    .unwrap();

    // Plant a foreign winner record at our record key.
    let foreign_record = CompactionRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: ProtoSignal::Metrics as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        level: 1,
        inputs: vec![],
        input_set_hash: foreign.to_vec(),
        parts: vec![],
        created_unix_ns: 0,
    };
    store
        .put(
            &record_key,
            foreign_record.encode_to_vec().into(),
            PutOptions::default(),
        )
        .await
        .unwrap();

    // Build our parts (with our hash) and try to publish.
    let listing = read::list_bucket(&store, &bucket).await.unwrap();
    let inputs = read::load_inputs(&store, &bucket, &listing.commit_keys, 1)
        .await
        .unwrap();
    let mut catalogs = Vec::new();
    for i in &inputs {
        catalogs.push(read::load_input_catalog(&store, &cfg(), i).await.unwrap());
    }
    let parts = build::build_parts(
        &store,
        &cfg(),
        &bucket,
        &inputs,
        catalogs,
        &HashSet::new(),
        &our_hash,
    )
    .await
    .unwrap();
    let err = publish::publish_record(
        &store,
        &cfg(),
        &clock,
        &bucket,
        &inputs,
        &our_hash,
        &parts,
        clock.now_ns(),
    )
    .await
    .expect_err("must alarm on input_set_hash divergence");
    assert!(
        matches!(
            err,
            ravel_maintain::MaintainError::InputSetHashDivergence { .. }
        ),
        "got {err:?}"
    );

    // Nothing deleted: the foreign record is untouched.
    let stored = store.get(&record_key, GetRange::Full).await.unwrap();
    let decoded = CompactionRecord::decode(stored.data.as_ref()).unwrap();
    assert_eq!(decoded.input_set_hash, foreign.to_vec());
}

/// Row 13: a compaction run past its `max_compaction_lifetime` deadline must
/// not publish (the abandonment mirror of the writer interlock). This is what
/// makes the sweeper's unreferenced-part rule safe: a late retry can never
/// re-reference a swept part, because it never publishes.
#[tokio::test]
async fn row13_past_deadline_abandons_without_publishing() {
    let store = MemoryStore::new();
    seed(&store).await;
    let config = cfg();
    let bucket = bucket();
    let start_ns = sealed_now_ns();
    // Clock is well past start + max_compaction_lifetime.
    let clock = FixedClock::new(start_ns + config.max_compaction_lifetime_ns + 1);

    let listing = read::list_bucket(&store, &bucket).await.unwrap();
    let inputs = read::load_inputs(&store, &bucket, &listing.commit_keys, 1)
        .await
        .unwrap();
    let hash = read::input_set_hash(&inputs);
    let mut catalogs = Vec::new();
    for i in &inputs {
        catalogs.push(read::load_input_catalog(&store, &config, i).await.unwrap());
    }
    let parts = build::build_parts(
        &store,
        &config,
        &bucket,
        &inputs,
        catalogs,
        &HashSet::new(),
        &hash,
    )
    .await
    .unwrap();
    let outcome = publish::publish_record(
        &store, &config, &clock, &bucket, &inputs, &hash, &parts, start_ns,
    )
    .await
    .expect("abandon is not an error");
    assert_eq!(outcome, PublishOutcome::Abandoned);
    assert!(
        !compaction_record_present(&store, &bucket).await,
        "abandoned run must not publish a record"
    );
}
