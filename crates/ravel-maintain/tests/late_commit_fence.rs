//! The late-commit fence (ADR-4561): maintenance plans a sealed bucket only
//! from the commit records the store stamped before the bucket's cutoff
//! `C(H) = end(H) + seal_margin`, truncated to a whole second, and leaves every
//! later one in place, served and kept.
//!
//! Stamps are set with `MemoryStore::set_clock_ms` before each seed, so every
//! fixture states which records are on time and which are late. Interleavings
//! are driven by `FaultStore` hold gates, and each test proves its hold fired
//! from the gate's held-call registry. No test sleeps.
//!
//! The fence's counters are process-wide, so every test here takes [`SERIAL`]
//! before its first pass: a test that asserts an exact counter delta then sees
//! only its own passes.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use common::*;
use prost::Message;
use ravel_commit::{keys, signal};
use ravel_maintain::migrate::{MigrationPath, NotMigratedBucket, NotMigratedReason, Verification};
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, Coordination, ErasureRequestSweepOutcome,
    ErasureRewriteOutcome, FixedClock, LATE_COMMIT_SETTLE_NS, LateCommitFence, MaintainError,
    MaintainMemo, MigrateBudget, MigrateOutcome, NoLeases, PendingErasureRequest, PublishOutcome,
    RequestLedger, bucket_erasure_completion, compact_bucket, erasure_rewrite_bucket,
    late_commit_records_total, migrate_bucket_format, migrate_family, read,
    store_clock_unobserved_total, sweep_erasure_requests, sweep_shard, sweep_superseded,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, GateHandle, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionRecord, ErasureCompletion,
    ErasurePredicateMatcher, ErasureRequest, RetentionTombstone, RewriteRecord,
};
use ravel_types::{CommitToken, Signal, TimeRange};
use uuid::Uuid;

/// The fence every planning pass here runs under, unless a test names `Off`.
const FENCE: LateCommitFence = LateCommitFence::Enforce;

const NS_PER_SECOND: i64 = 1_000_000_000;

/// The key fragment of the compaction record (`.../l1.<hash>.cmt`).
const COMPACTION_RECORD_KEYS: &str = "/l1.";

/// The key fragment of the rewrite record (`.../rw.<hash>.cmt`).
const REWRITE_RECORD_KEYS: &str = "/rw.";

/// The key fragment of every L1 part object.
const PART_KEYS: &str = "/l1/";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn base_ns() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn ms(ns: i64) -> u64 {
    u64::try_from(ns / 1_000_000).expect("positive instant")
}

/// `C(H)` for [`bucket`] under the default seal margin.
fn cutoff_ns() -> i64 {
    bucket().late_commit_cutoff_ns(&CompactorConfig::default())
}

/// A stamp inside the bucket's own hour: on time.
fn on_time_ms() -> u64 {
    ms(base_ns() + 30 * 60 * NS_PER_SECOND)
}

/// A stamp one second past the cutoff: late.
fn late_ms() -> u64 {
    ms(cutoff_ns() + NS_PER_SECOND)
}

fn fenced(fence: LateCommitFence, config: CompactorConfig) -> CompactorConfig {
    CompactorConfig {
        late_commit_fence: fence,
        ..config
    }
}

/// A single-pass config with no claims: the pre-publish re-list is the only
/// fence between two passes of one bucket.
fn unclaimed(fence: LateCommitFence, ledger: &RequestLedger) -> CompactorConfig {
    fenced(
        fence,
        CompactorConfig {
            coordination: Coordination::On,
            claim_participant: None,
            request_ledger: Some(ledger.clone()),
            ..CompactorConfig::default()
        },
    )
}

fn input(writer: u128, seq: u64, series: Vec<RawSeries>) -> InputSpec {
    InputSpec::new(Uuid::from_u128(writer), 10, seq, series)
}

fn identity(spec: &InputSpec) -> (String, u64, u64) {
    (spec.writer_id.to_string(), spec.epoch, spec.seq)
}

fn identities(inputs: &[CompactionInputIdentity]) -> Vec<(String, u64, u64)> {
    let mut out: Vec<(String, u64, u64)> = inputs
        .iter()
        .map(|i| (i.writer_id.clone(), i.writer_epoch, i.writer_seq))
        .collect();
    out.sort();
    out
}

/// Seed `spec` through `store` with every object stamped `stamp_ms` by
/// `stamps`, the memory store underneath. Returns the commit key.
async fn seed_at(
    stamps: &MemoryStore,
    store: &dyn ObjectStoreBackend,
    stamp_ms: u64,
    spec: &InputSpec,
) -> String {
    stamps.set_clock_ms(stamp_ms);
    seed_input(store, spec).await
}

/// The data object key `commit_key`'s record names.
async fn data_key_of(store: &dyn ObjectStoreBackend, commit_key: &str) -> String {
    let record = CommitRecord::decode(get_full(store, commit_key).await.as_ref())
        .expect("decode commit record");
    keys::reconstruct_data_key(&record).expect("data key")
}

async fn exists(store: &dyn ObjectStoreBackend, key: &str) -> bool {
    store.get(key, GetRange::Full).await.is_ok()
}

/// One erasure request for every series named `name`, optionally windowed.
fn request(name: &str, window: Option<(i64, i64)>, id: u128) -> Vec<PendingErasureRequest> {
    let request_id = Uuid::from_u128(id);
    let (window_start_ns, window_end_ns) = window.unwrap_or((0, 0));
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 0,
        predicate: vec![ErasurePredicateMatcher {
            key: "__name__".to_string(),
            value: name.to_string(),
        }],
        window_start_ns,
        window_end_ns,
        reason: String::new(),
    };
    vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request,
    }]
}

fn victim_request() -> Vec<PendingErasureRequest> {
    request("victim", None, 0x4561)
}

async fn erasure(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
    pending: &[PendingErasureRequest],
) -> ravel_maintain::Result<ErasureRewriteOutcome> {
    let mut memo = MaintainMemo::with_default_interval();
    erasure_rewrite_bucket(
        store,
        clock,
        config,
        &NoLeases,
        &bucket(),
        pending,
        &mut memo,
    )
    .await
}

/// Wait until the store's gates hold `held` calls in all, check exactly one of
/// them is a PUT on a key containing `fragment`, and return its id. The held
/// registry is shared by every gate of one store.
async fn parked(gate: &GateHandle, fragment: &str, held: usize) -> u64 {
    gate.wait_until_held(held).await;
    let calls = gate.held_details();
    assert_eq!(calls.len(), held, "the gates hold {held} calls: {calls:?}");
    let ours: Vec<_> = calls
        .iter()
        .filter(|(_, _, key)| key.contains(fragment))
        .collect();
    assert_eq!(
        ours.len(),
        1,
        "exactly one held call is on {fragment}: {calls:?}"
    );
    let (id, op, key) = ours[0];
    assert_eq!(*op, Op::Put, "the parked call is a PUT: {key}");
    *id
}

async fn compaction_records(store: &dyn ObjectStoreBackend) -> Vec<(String, CompactionRecord)> {
    let listing = read::list_bucket(store, &bucket()).await.expect("list");
    let mut out = Vec::new();
    for key in listing.compaction_record_keys {
        let record =
            CompactionRecord::decode(get_full(store, &key).await.as_ref()).expect("decode");
        out.push((key, record));
    }
    out
}

async fn rewrite_records(store: &dyn ObjectStoreBackend) -> Vec<(String, RewriteRecord)> {
    let listing = read::list_bucket(store, &bucket()).await.expect("list");
    let mut out = Vec::new();
    for key in listing.rewrite_record_keys {
        let record = RewriteRecord::decode(get_full(store, &key).await.as_ref()).expect("decode");
        out.push((key, record));
    }
    out
}

/// What a resolve serves for [`bucket`]'s hour: the data object keys, and how
/// many times each `(series, ts, value bits)` appears across them.
struct Served {
    keys: BTreeSet<String>,
    samples: BTreeMap<([u8; 16], i64, u64), usize>,
}

impl Served {
    /// The timestamps, relative to the hour, at which `name{k=label}` is
    /// served.
    fn times_of(&self, name: &str, label: &str) -> Vec<i64> {
        let id = series_id(name, &[("k", label)]).0;
        self.samples
            .keys()
            .filter(|(series, _, _)| *series == id)
            .map(|(_, ts, _)| ts - base_ns())
            .collect()
    }

    fn served_more_than_once(&self) -> usize {
        self.samples.values().filter(|n| **n > 1).count()
    }
}

async fn resolve(
    store: Arc<dyn ObjectStoreBackend>,
    tokens: &[CommitToken],
    now_ns: i64,
) -> Served {
    let catalog = ravel_catalog::Catalog::new(
        store.clone(),
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("catalog");
    let snapshot = catalog
        .resolve(
            &tenant_hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: base_ns(),
                end_ns: base_ns() + NS_PER_HOUR,
            },
            tokens,
            now_ns,
        )
        .await
        .expect("resolve");
    let keys: BTreeSet<String> = snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    let mut samples = BTreeMap::new();
    for key in &keys {
        for (id, series) in read_scalar_samples(&get_full(store.as_ref(), key).await) {
            for (ts, bits) in series {
                *samples.entry((id, ts, bits)).or_default() += 1;
            }
        }
    }
    Served { keys, samples }
}

/// The three inputs of the planning test, stamped `C(H) - 1 ms`, `C(H)` and
/// `C(H) + 1 h`. Only the first is on time.
fn stamped_three() -> Vec<(u64, InputSpec)> {
    let b = base_ns();
    let cutoff_ms = ms(cutoff_ns());
    vec![
        (
            cutoff_ms - 1,
            input(
                1,
                1,
                vec![
                    raw_series("keep", &[("k", "a")], &[(b + 1_000, 1.0)]),
                    raw_series("victim", &[("k", "b")], &[(b + 1_000, 5.0)]),
                ],
            ),
        ),
        (
            cutoff_ms,
            input(
                2,
                2,
                vec![
                    raw_series("keep", &[("k", "a")], &[(b + 2_000, 2.0)]),
                    raw_series("victim", &[("k", "b")], &[(b + 2_000, 6.0)]),
                ],
            ),
        ),
        (
            ms(cutoff_ns() + NS_PER_HOUR),
            input(
                3,
                3,
                vec![raw_series("keep", &[("k", "a")], &[(b + 3_000, 3.0)])],
            ),
        ),
    ]
}

async fn seeded_three() -> (MemoryStore, Vec<InputSpec>) {
    let store = MemoryStore::new();
    let mut specs = Vec::new();
    for (stamp, spec) in stamped_three() {
        seed_at(&store, &store, stamp, &spec).await;
        specs.push(spec);
    }
    (store, specs)
}

/// R1: the cutoff is the seal instant truncated down to a whole second, and a
/// stamp at it is late (R2).
#[test]
fn the_cutoff_is_the_seal_truncated_to_a_whole_second() {
    let config = CompactorConfig {
        clock_skew_allowance_ns: 5 * 60 * NS_PER_SECOND + 999_999_999,
        ..CompactorConfig::default()
    };
    let b = bucket();
    let seal = b.end_ns() + config.seal_margin_ns();
    assert_eq!(seal % NS_PER_SECOND, 999_999_999);
    assert_eq!(b.late_commit_cutoff_ns(&config), seal - 999_999_999);
    assert_eq!(
        cutoff_ns(),
        b.end_ns() + NS_PER_HOUR + 5 * 60 * NS_PER_SECOND,
        "the default margin is a whole second already"
    );
    let cutoff_ms = i64::try_from(ms(cutoff_ns())).expect("ms");
    assert!(!read::is_late_stamp(cutoff_ms - 1, cutoff_ns()));
    assert!(read::is_late_stamp(cutoff_ms, cutoff_ns()));
}

/// Test 1 (R4): a commit record stamped at or after the cutoff is not an input
/// of a compaction, of the migrate primitive or of a RawL0 erasure rewrite. Of
/// three inputs stamped `C(H) - 1 ms`, `C(H)` and `C(H) + 1 h`, each pass
/// publishes a record naming exactly the first.
///
/// With `FENCE` set to `Off` each record names all three identities.
#[tokio::test]
async fn a_commit_record_stamped_at_the_cutoff_is_left_out_of_every_plan() {
    let _serial = SERIAL.lock().await;
    let now_ns = sealed_now_ns();
    let clock = FixedClock::new(now_ns);
    let config = fenced(
        FENCE,
        CompactorConfig {
            min_compaction_inputs: 1,
            ..CompactorConfig::default()
        },
    );

    let (store, specs) = seeded_three().await;
    let first = vec![identity(&specs[0])];
    let late_before = late_commit_records_total(Signal::Metrics);
    let outcome = compact_bucket(&store, &clock, &config, &bucket())
        .await
        .expect("compaction");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    let record = fetch_compaction_record(&store, &bucket()).await;
    assert_eq!(identities(&record.inputs), first, "compaction inputs");
    assert_eq!(
        late_commit_records_total(Signal::Metrics) - late_before,
        2,
        "the plan counted both late records it left out"
    );

    let (store, _) = seeded_three().await;
    let target = ravel_maintain::build::OUTPUT_FORMAT_VERSION + 1;
    let outcome = migrate_bucket_format(&store, &clock, &config, &bucket(), target)
        .await
        .expect("migration");
    assert!(
        matches!(
            outcome,
            MigrateOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    let record = fetch_compaction_record(&store, &bucket()).await;
    assert_eq!(identities(&record.inputs), first, "migration inputs");

    let (store, _) = seeded_three().await;
    let outcome = erasure(&store, &clock, &config, &victim_request())
        .await
        .expect("erasure rewrite");
    assert!(
        matches!(
            outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    let rewrites = rewrite_records(&store).await;
    assert_eq!(rewrites.len(), 1);
    assert_eq!(
        identities(&rewrites[0].1.inputs),
        first,
        "erasure rewrite inputs"
    );
}

/// The final state of [`late_l0_between_the_listings`].
struct Race {
    compaction_key: String,
    rewrite_key: String,
    dominated: BTreeSet<String>,
    conflicts: Vec<(String, String)>,
    late_data_key: String,
    served: Served,
    rewrite_parts: BTreeSet<String>,
    rerun: ravel_maintain::Result<ErasureRewriteOutcome>,
}

/// The #4449 race with a late L0 between the two passes' listings. Erasure E
/// plans from the two on-time inputs, re-lists, and is parked inside its record
/// PUT. A late L0 then lands. Compaction A plans, re-lists (E has published
/// nothing yet) and is parked inside its record PUT. E is released and
/// publishes; then A is released and publishes beside it. Neither pass takes a
/// claim, so nothing but the planning listings decides what A names.
async fn late_l0_between_the_listings(fence: LateCommitFence) -> Race {
    let now_ns = sealed_now_ns();
    let clock = FixedClock::new(now_ns);
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let b = base_ns();
    for spec in [
        input(
            1,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(b + 1_000, 1.0), (b + 2_000, 2.0)]),
                raw_series("victim", &[("k", "b")], &[(b + 1_000, 5.0)]),
            ],
        ),
        input(
            2,
            2,
            vec![raw_series("victim", &[("k", "b")], &[(b + 3_000, 3.0)])],
        ),
    ] {
        seed_at(store.inner(), store.as_ref(), on_time_ms(), &spec).await;
    }
    let late = input(
        3,
        3,
        vec![raw_series("late", &[("k", "c")], &[(b + 4_000, 4.0)])],
    );

    let e_ledger = RequestLedger::new();
    let e_config = unclaimed(fence, &e_ledger);
    let a_ledger = RequestLedger::new();
    let a_config = unclaimed(fence, &a_ledger);
    let e_gate = store.hold(
        Op::Put,
        Some(REWRITE_RECORD_KEYS.to_string()),
        Occurrence::Nth(1),
    );
    let a_gate = store.hold(
        Op::Put,
        Some(COMPACTION_RECORD_KEYS.to_string()),
        Occurrence::Nth(1),
    );
    let (e_done, e_landed) = tokio::sync::oneshot::channel::<()>();

    let e = async {
        let outcome = erasure(store.as_ref(), &clock, &e_config, &victim_request())
            .await
            .expect("erasure rewrite");
        e_done.send(()).expect("the driver waits for E");
        outcome
    };
    let driver = async {
        let e_id = parked(&e_gate, REWRITE_RECORD_KEYS, 1).await;
        assert_eq!(
            e_ledger.report().list.requests,
            1,
            "E re-listed before its PUT"
        );
        let late_commit_key = seed_at(store.inner(), store.as_ref(), late_ms(), &late).await;
        let a_bucket = bucket();
        let a = compact_bucket(store.as_ref(), &clock, &a_config, &a_bucket);
        let release = async {
            let a_id = parked(&a_gate, COMPACTION_RECORD_KEYS, 2).await;
            assert_eq!(
                a_ledger.report().list.requests,
                2,
                "A planned and re-listed before its PUT"
            );
            assert!(e_gate.release(e_id), "E's record PUT was released");
            e_landed.await.expect("E finished");
            assert!(a_gate.release(a_id), "A's record PUT was released");
        };
        let (a_outcome, ()) = tokio::join!(a, release);
        (a_outcome.expect("compaction"), late_commit_key)
    };
    let (e_outcome, (a_outcome, late_commit_key)) = tokio::join!(e, driver);
    assert!(
        matches!(
            e_outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{e_outcome:?}"
    );
    assert!(
        matches!(
            a_outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{a_outcome:?}"
    );

    let compactions = compaction_records(store.as_ref()).await;
    let rewrites = rewrite_records(store.as_ref()).await;
    assert_eq!((compactions.len(), rewrites.len()), (1, 1));
    let dominance = ravel_catalog::erasure_dominance(&compactions, &rewrites, "bucket")
        .expect("dominance resolves");
    let dominated = dominance
        .dominated
        .iter()
        .map(|k| (*k).to_string())
        .collect();
    let conflicts = dominance
        .conflicts
        .iter()
        .map(|(c, r)| ((*c).to_string(), r.clone()))
        .collect();
    let rewrite_parts = rewrites[0]
        .1
        .parts
        .iter()
        .map(|p| keys::reconstruct_rewrite_part_key(&rewrites[0].1, p).expect("part key"))
        .collect();
    let late_data_key = data_key_of(store.as_ref(), &late_commit_key).await;
    let served = resolve(store.clone(), &[], now_ns).await;
    let rerun_clock = FixedClock::new(now_ns + NS_PER_HOUR);
    let rerun = erasure(
        store.as_ref(),
        &rerun_clock,
        &unclaimed(fence, &RequestLedger::new()),
        &victim_request(),
    )
    .await;
    Race {
        compaction_key: compactions[0].0.clone(),
        rewrite_key: rewrites[0].0.clone(),
        dominated,
        conflicts,
        late_data_key,
        served,
        rewrite_parts,
        rerun,
    }
}

/// Test 2: a late L0 that lands between an erasure rewrite's listing and a
/// compaction's no longer turns the #4449 race into a partial cover. Both
/// records name the same two on-time inputs, so the rewrite dominates the
/// compaction record; every surviving row is served once, the erased series
/// is not served, the late row is served from its own L0, and a later erasure
/// pass finds the rewrite the one live record.
///
/// With `FENCE` set to `Off` the compaction names the late L0 too: the two
/// records conflict and the rerun fails with `MultipleLiveRecords`, which
/// [`with_the_fence_off_a_late_l0_between_the_listings_is_a_partial_cover`]
/// pins.
#[tokio::test]
async fn a_late_l0_between_the_two_listings_is_a_full_cover() {
    let _serial = SERIAL.lock().await;
    let race = late_l0_between_the_listings(FENCE).await;

    assert_eq!(
        race.dominated,
        BTreeSet::from([race.compaction_key.clone()])
    );
    assert_eq!(race.conflicts, Vec::<(String, String)>::new());
    let mut expected = race.rewrite_parts.clone();
    expected.insert(race.late_data_key.clone());
    assert_eq!(
        race.served.keys, expected,
        "the rewrite's parts and the late L0"
    );
    assert_eq!(race.served.served_more_than_once(), 0);
    assert_eq!(race.served.times_of("victim", "b"), Vec::<i64>::new());
    assert_eq!(race.served.times_of("keep", "a"), vec![1_000, 2_000]);
    assert_eq!(race.served.times_of("late", "c"), vec![4_000]);
    assert!(
        matches!(race.rerun, Ok(ErasureRewriteOutcome::AlreadyApplied)),
        "{:?}",
        race.rerun
    );
}

/// The negative control of test 2: the same script with the fence off is the
/// partial cover the fence exists to prevent.
#[tokio::test]
async fn with_the_fence_off_a_late_l0_between_the_listings_is_a_partial_cover() {
    let _serial = SERIAL.lock().await;
    let race = late_l0_between_the_listings(LateCommitFence::Off).await;

    assert_eq!(race.dominated, BTreeSet::new());
    assert_eq!(
        race.conflicts,
        vec![(race.compaction_key.clone(), race.rewrite_key.clone())]
    );
    assert!(
        matches!(race.rerun, Err(MaintainError::MultipleLiveRecords { .. })),
        "{:?}",
        race.rerun
    );
}

/// Test 3 (R6): an acknowledged write whose commit record lands late is
/// served and never swept. After a compaction that leaves it out, an erasure
/// rewrite for an unrelated subject, and two sweeps aged past every grace
/// period, the late commit record and its data object are still stored; a
/// listing resolve, a token resolve and a resolve from a folded HEAD each
/// serve its row exactly once.
#[tokio::test]
async fn an_acknowledged_write_that_lands_late_is_served_and_never_swept() {
    let _serial = SERIAL.lock().await;
    let now_ns = sealed_now_ns();
    let store = Arc::new(MemoryStore::new());
    let b = base_ns();
    let on_time = [
        input(
            1,
            1,
            vec![raw_series("keep", &[("k", "a")], &[(b + 1_000, 1.0)])],
        ),
        input(
            2,
            2,
            vec![raw_series("keep", &[("k", "a")], &[(b + 2_000, 2.0)])],
        ),
    ];
    let mut on_time_keys = Vec::new();
    for spec in &on_time {
        on_time_keys.push(seed_at(&store, store.as_ref(), on_time_ms(), spec).await);
    }
    let late = input(
        3,
        3,
        vec![raw_series("late", &[("k", "c")], &[(b + 4_000, 4.0)])],
    );
    let late_key = seed_at(&store, store.as_ref(), late_ms(), &late).await;
    let late_data_key = data_key_of(store.as_ref(), &late_key).await;
    let config = fenced(FENCE, CompactorConfig::default());

    let clock = FixedClock::new(now_ns);
    store.set_clock_ms(ms(now_ns));
    let outcome = compact_bucket(store.as_ref(), &clock, &config, &bucket())
        .await
        .expect("compaction");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    let outcome = erasure(
        store.as_ref(),
        &clock,
        &config,
        &request("nobody", None, 0x4562),
    )
    .await
    .expect("erasure rewrite");
    assert!(
        matches!(
            outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );

    let day = 24 * NS_PER_HOUR;
    let mut swept_at = now_ns;
    let mut deleted = Vec::new();
    for _ in 0..2 {
        swept_at += 7 * day;
        store.set_clock_ms(ms(swept_at));
        let report = sweep_shard(
            store.as_ref(),
            &FixedClock::new(swept_at),
            &config,
            &NoLeases,
            &tenant_hash(),
            Signal::Metrics,
            SHARD,
        )
        .await
        .expect("sweep");
        deleted.push((
            report.superseded_records_deleted,
            report.superseded_data_deleted,
            report.superseded_deletes_refused,
        ));
    }
    assert_eq!(
        deleted,
        vec![(0, 0, 0), (3, 3, 0)],
        "the first sweep marks the pinned window, the second deletes both on-time \
         inputs and the compaction record the rewrite superseded"
    );

    for key in &on_time_keys {
        assert!(
            !exists(store.as_ref(), key).await,
            "the sweep reached deletion: the superseded on-time input {key} is gone"
        );
    }
    assert!(
        exists(store.as_ref(), &late_key).await,
        "late commit record kept"
    );
    assert!(
        exists(store.as_ref(), &late_data_key).await,
        "late data object kept"
    );

    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let by_listing = resolve(dyn_store.clone(), &[], swept_at).await;
    assert!(by_listing.keys.contains(&late_data_key));
    assert_eq!(by_listing.times_of("late", "c"), vec![4_000]);
    assert_eq!(by_listing.times_of("keep", "a"), vec![1_000, 2_000]);
    assert_eq!(by_listing.served_more_than_once(), 0);

    let token = CommitToken {
        shard: SHARD,
        writer_id: late.writer_id,
        epoch: late.epoch,
        seq: late.seq,
        ingest_hour_bucket: HOUR,
    };
    let by_token = resolve(dyn_store.clone(), &[token], swept_at).await;
    assert!(by_token.keys.contains(&late_data_key));
    assert_eq!(by_token.times_of("late", "c"), vec![4_000]);
    assert_eq!(by_token.served_more_than_once(), 0);

    let catalog = ravel_catalog::Catalog::new(
        dyn_store.clone(),
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("catalog");
    catalog
        .fold(
            &tenant_hash(),
            Signal::Metrics,
            Uuid::from_u128(0x4561),
            swept_at,
            &[],
            None,
        )
        .await
        .expect("fold publishes a HEAD");
    let by_head = resolve(dyn_store, &[], swept_at).await;
    assert!(by_head.keys.contains(&late_data_key));
    assert_eq!(by_head.times_of("late", "c"), vec![4_000]);
    assert_eq!(by_head.served_more_than_once(), 0);
}

/// Test 4 (R3): a pass whose own clock has sealed the bucket still waits until
/// the store's clock, read after the planning LIST, is at least
/// `C(H) + SETTLE`. A store that gives no observation is planned on the pass's
/// own clock and counted, exactly once per plan.
#[tokio::test]
async fn a_plan_waits_until_the_store_clock_passes_the_cutoff() {
    let _serial = SERIAL.lock().await;
    let settled = cutoff_ns() + LATE_COMMIT_SETTLE_NS;
    let config = fenced(FENCE, CompactorConfig::default());
    let clock = FixedClock::new(sealed_now_ns());
    assert!(
        sealed_now_ns() > settled,
        "the pass's own clock has settled"
    );

    async fn seeded() -> MemoryStore {
        let store = MemoryStore::new();
        for spec in [
            input(
                1,
                1,
                vec![raw_series("keep", &[("k", "a")], &[(1_000, 1.0)])],
            ),
            input(
                2,
                2,
                vec![raw_series("keep", &[("k", "a")], &[(2_000, 2.0)])],
            ),
        ] {
            seed_at(&store, &store, on_time_ms(), &spec).await;
        }
        store
    }

    let unobserved = store_clock_unobserved_total(Signal::Metrics);
    let store = seeded().await;
    store.set_observed_store_time_ns(Some(settled - NS_PER_SECOND));
    let outcome = compact_bucket(&store, &clock, &config, &bucket())
        .await
        .expect("compaction");
    assert_eq!(outcome, CompactionOutcome::NotSealed);

    store.set_observed_store_time_ns(Some(settled));
    let outcome = compact_bucket(&store, &clock, &config, &bucket())
        .await
        .expect("compaction");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(
        store_clock_unobserved_total(Signal::Metrics),
        unobserved,
        "an observation was given each time"
    );

    let store = seeded().await;
    let outcome = compact_bucket(&store, &FixedClock::new(settled - 1), &config, &bucket())
        .await
        .expect("compaction");
    assert_eq!(
        outcome,
        CompactionOutcome::NotSealed,
        "the pass's own clock is short of the settle allowance"
    );
    assert_eq!(
        store_clock_unobserved_total(Signal::Metrics),
        unobserved,
        "no LIST, so no observation was read"
    );

    let outcome = compact_bucket(&store, &FixedClock::new(settled), &config, &bucket())
        .await
        .expect("compaction");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(
        store_clock_unobserved_total(Signal::Metrics) - unobserved,
        1,
        "the one plan without an observation is counted once"
    );
}

/// Run an unclaimed compaction parked at its first part PUT, seed `arrival`
/// stamped `stamp_ms` while it is parked, then release it.
async fn compaction_with_an_arrival(
    stamp_ms: u64,
) -> (
    Arc<FaultStore<MemoryStore>>,
    CompactionOutcome,
    RequestLedger,
) {
    let now_ns = sealed_now_ns();
    let clock = FixedClock::new(now_ns);
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    for spec in [
        input(
            1,
            1,
            vec![raw_series("keep", &[("k", "a")], &[(1_000, 1.0)])],
        ),
        input(
            2,
            2,
            vec![raw_series("keep", &[("k", "a")], &[(2_000, 2.0)])],
        ),
    ] {
        seed_at(store.inner(), store.as_ref(), on_time_ms(), &spec).await;
    }
    let arrival = input(
        3,
        3,
        vec![raw_series("keep", &[("k", "a")], &[(3_000, 3.0)])],
    );
    let ledger = RequestLedger::new();
    let config = unclaimed(FENCE, &ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));
    let c = async {
        compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compaction")
    };
    let driver = async {
        let id = parked(&gate, PART_KEYS, 1).await;
        seed_at(store.inner(), store.as_ref(), stamp_ms, &arrival).await;
        assert!(gate.release(id), "the parked part PUT was released");
    };
    let (outcome, ()) = tokio::join!(c, driver);
    (store, outcome, ledger)
}

/// Test 5 (R5): a late record that arrives between a compaction's plan and its
/// pre-publish re-list does not abort the publish, and the record names the
/// planned on-time inputs only.
#[tokio::test]
async fn a_late_arrival_does_not_abort_a_publish() {
    let _serial = SERIAL.lock().await;
    let (store, outcome, ledger) = compaction_with_an_arrival(late_ms()).await;
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(ledger.report().list.requests, 2, "the plan and the re-list");
    let record = fetch_compaction_record(store.as_ref(), &bucket()).await;
    assert_eq!(
        identities(&record.inputs),
        vec![
            (Uuid::from_u128(1).to_string(), 10, 1),
            (Uuid::from_u128(2).to_string(), 10, 2)
        ]
    );
}

/// Test 5 (R5): an on-time record that arrives between the plan and the
/// re-list still aborts the publish, as before the fence.
#[tokio::test]
async fn an_on_time_arrival_still_aborts_a_publish() {
    let _serial = SERIAL.lock().await;
    let (store, outcome, ledger) = compaction_with_an_arrival(ms(cutoff_ns()) - 1).await;
    assert_eq!(
        outcome,
        CompactionOutcome::Compacted {
            parts: 0,
            publish: PublishOutcome::Abandoned,
        }
    );
    assert_eq!(ledger.report().list.requests, 2, "the plan and the re-list");
    assert_eq!(ledger.report().publish.requests, 0, "no record PUT");
    assert!(compaction_records(store.as_ref()).await.is_empty());
}

/// Two on-time inputs, `victim` in the first, and one late input holding
/// `late_series` at `late_ts`, relative to the hour.
async fn erasure_fixture(late_series: &str, late_ts: i64) -> (MemoryStore, Vec<InputSpec>) {
    let (store, on_time) = erasure_fixture_without_late().await;
    let b = base_ns();
    let late = input(
        3,
        3,
        vec![raw_series(
            late_series,
            &[("k", "c")],
            &[(b + late_ts, 4.0)],
        )],
    );
    seed_at(&store, &store, late_ms(), &late).await;
    (store, on_time)
}

/// The on-time inputs of [`erasure_fixture`] alone.
async fn erasure_fixture_without_late() -> (MemoryStore, Vec<InputSpec>) {
    let store = MemoryStore::new();
    let b = base_ns();
    let on_time = vec![
        input(
            1,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(b + 1_000, 1.0)]),
                raw_series("victim", &[("k", "b")], &[(b + 1_000, 5.0)]),
            ],
        ),
        input(
            2,
            2,
            vec![raw_series("keep", &[("k", "a")], &[(b + 2_000, 2.0)])],
        ),
    ];
    for spec in &on_time {
        seed_at(&store, &store, on_time_ms(), spec).await;
    }
    (store, on_time)
}

/// Erase `pending` from the fixture, check the rewrite names only the on-time
/// inputs, and return the completion gate's verdict.
async fn erase_then_complete(
    store: &MemoryStore,
    on_time: &[InputSpec],
    pending: &[PendingErasureRequest],
) -> ravel_maintain::BucketErasureCompletion {
    let clock = FixedClock::new(sealed_now_ns());
    let config = fenced(FENCE, CompactorConfig::default());
    let outcome = erasure(store, &clock, &config, pending)
        .await
        .expect("erasure rewrite");
    assert!(
        matches!(
            outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    let rewrites = rewrite_records(store).await;
    assert_eq!(rewrites.len(), 1);
    let mut expected: Vec<(String, u64, u64)> = on_time.iter().map(identity).collect();
    expected.sort();
    assert_eq!(identities(&rewrites[0].1.inputs), expected);
    bucket_erasure_completion(store, &clock, &config, &NoLeases, &bucket(), pending)
        .await
        .expect("completion")
}

/// Test 6 and site 9: the rewrite leaves a late L0 that holds the subject out,
/// and the completion gate, which reads every commit record, keeps the request
/// blocked, so no `.done` is written while that L0 is served.
#[tokio::test]
async fn a_late_l0_holding_the_subject_keeps_the_request_blocked() {
    let _serial = SERIAL.lock().await;
    let (store, on_time) = erasure_fixture("victim", 4_000).await;
    let pending = victim_request();
    let completion = erase_then_complete(&store, &on_time, &pending).await;
    assert_eq!(
        completion.blocked,
        [pending[0].request.request_id.clone()]
            .into_iter()
            .collect()
    );
    assert!(!completion.unresolved);
}

/// Test 6: a late L0 outside a windowed request's event-time window does not
/// block the request, which completes once the rewrite has published.
#[tokio::test]
async fn a_late_l0_outside_the_window_lets_the_request_complete() {
    let _serial = SERIAL.lock().await;
    let (store, on_time) = erasure_fixture("other", 10_000_000).await;
    let b = base_ns();
    let pending = request("victim", Some((b, b + 5_000)), 0x4563);
    let completion = erase_then_complete(&store, &on_time, &pending).await;
    assert!(completion.blocked.is_empty(), "{:?}", completion.blocked);
    assert!(!completion.unresolved);
}

/// Site 9: the completion gate decides on event-time overlap, not on the
/// subject. A windowless request overlaps every live record, so a late L0 that
/// holds only another series still keeps it blocked.
#[tokio::test]
async fn a_late_l0_keeps_a_windowless_request_blocked_whatever_it_holds() {
    let _serial = SERIAL.lock().await;
    let (store, on_time) = erasure_fixture("other", 4_000).await;
    let pending = victim_request();
    let completion = erase_then_complete(&store, &on_time, &pending).await;
    assert_eq!(
        completion.blocked,
        [pending[0].request.request_id.clone()]
            .into_iter()
            .collect()
    );
}

/// The completion gate judges a bucket under the test the raw-L0 erasure
/// rewrite plans under. Under the fence a bucket that is sealed but not yet
/// plannable, by the pass's clock or by the store's, keeps a request it held
/// at acknowledgement blocked and is not listed at all; once plannable it is
/// judged, and a request whose window holds nothing it serves completes. With
/// the fence off the gate judges the bucket as soon as it seals, as before.
#[tokio::test]
async fn completion_waits_until_the_bucket_is_plannable() {
    let _serial = SERIAL.lock().await;
    let (memory, _) = erasure_fixture_without_late().await;
    let b = base_ns();
    let mut pending = request("victim", Some((b + 10_000_000, b + 20_000_000)), 0x4566);
    pending[0].request.created_unix_ns = b + 30 * 60 * NS_PER_SECOND;
    let request_id = pending[0].request.request_id.clone();
    let config = fenced(FENCE, CompactorConfig::default());
    let sealed = bucket().end_ns() + config.seal_margin_ns();
    let settled = cutoff_ns() + LATE_COMMIT_SETTLE_NS;
    assert!(bucket().is_sealed(sealed, &config));
    assert!(
        sealed < settled,
        "the seal comes before the bucket is plannable"
    );

    let prefix = keys::commit_shard_hour_prefix(
        &bucket().tenant_hash,
        bucket().signal,
        bucket().shard,
        bucket().ingest_hour_bucket,
    )
    .expect("bucket prefix");
    let store = FaultStore::new(
        memory,
        FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Permanent("bucket listed".into()))
                .with_key_contains(prefix),
        ),
    );
    for (now_ns, observed) in [(sealed, None), (settled, Some(settled - NS_PER_SECOND))] {
        store.inner().set_observed_store_time_ns(observed);
        let completion = bucket_erasure_completion(
            &store,
            &FixedClock::new(now_ns),
            &config,
            &NoLeases,
            &bucket(),
            &pending,
        )
        .await
        .expect("a bucket that is not plannable is not listed");
        assert_eq!(
            completion.blocked,
            [request_id.clone()].into_iter().collect(),
            "at {now_ns} with the store clock at {observed:?}"
        );
        assert!(!completion.unresolved);
    }
    assert_eq!(store.fault_count(Op::List, FaultKind::Permanent), 0);

    let memory = store.inner();
    memory.set_observed_store_time_ns(Some(settled));
    let completion = bucket_erasure_completion(
        memory,
        &FixedClock::new(settled),
        &config,
        &NoLeases,
        &bucket(),
        &pending,
    )
    .await
    .expect("completion");
    assert!(completion.blocked.is_empty(), "{:?}", completion.blocked);
    assert!(!completion.unresolved);

    memory.set_observed_store_time_ns(None);
    let off = fenced(LateCommitFence::Off, CompactorConfig::default());
    let completion = bucket_erasure_completion(
        memory,
        &FixedClock::new(sealed),
        &off,
        &NoLeases,
        &bucket(),
        &pending,
    )
    .await
    .expect("completion");
    assert!(completion.blocked.is_empty(), "{:?}", completion.blocked);
}

/// The pinned-query window (ADR-1133) zeroed, so the sweep's observing rule-2
/// pass sees its other rules alone.
fn sweep_config(fence: LateCommitFence) -> CompactorConfig {
    CompactorConfig {
        max_query_duration_ns: 0,
        head_cache_ttl_ns: 0,
        clock_skew_allowance_ns: 0,
        ..fenced(fence, CompactorConfig::default())
    }
}

/// When the requests the hole fixture completes are marked complete.
fn completed_ns() -> i64 {
    sealed_now_ns()
}

/// A stamp one minute after the completion: late, and applied after the
/// completion gate ran.
fn after_completion_ms() -> u64 {
    ms(completed_ns() + 60 * NS_PER_SECOND)
}

/// The hole ordering: the request is erased from the on-time records, the
/// completion gate finds nothing blocking it, its `.dreq` and `.done` are
/// written, and only then does the store apply a late L0 holding `late_series`
/// at `late_ts`, relative to the hour. Returns the store and the late commit
/// key.
async fn completed_before_a_late_l0(
    pending: &[PendingErasureRequest],
    late_series: &str,
    late_ts: i64,
) -> (MemoryStore, String) {
    let (store, on_time) = erasure_fixture_without_late().await;
    let completion = erase_then_complete(&store, &on_time, pending).await;
    assert!(completion.blocked.is_empty(), "{:?}", completion.blocked);
    assert!(!completion.unresolved);
    for p in pending {
        let request_id = Uuid::parse_str(&p.request.request_id).expect("request id");
        store
            .put(
                &p.request_key,
                ravel_commit::erasure::encode_request(&p.request),
                PutOptions::default(),
            )
            .await
            .expect("put .dreq");
        let completion = ErasureCompletion {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            request_id: p.request.request_id.clone(),
            predicate_hash: vec![0x11; 32],
            bucket_drops: Vec::new(),
            requested_unix_ns: 0,
            completed_unix_ns: completed_ns(),
            deferral_cause: 0,
        };
        store
            .put(
                &keys::erasure_completion_key(&tenant_hash(), Signal::Metrics, request_id)
                    .expect("done key"),
                ravel_commit::erasure::encode_completion(&completion),
                PutOptions::default(),
            )
            .await
            .expect("put .done");
    }
    let late = input(
        3,
        3,
        vec![raw_series(
            late_series,
            &[("k", "c")],
            &[(base_ns() + late_ts, 4.0)],
        )],
    );
    let late_key = seed_at(&store, &store, after_completion_ms(), &late).await;
    let stamp = store
        .head(&late_key)
        .await
        .expect("late record")
        .last_modified_unix_ms;
    assert!(read::is_late_stamp(stamp, cutoff_ns()), "{stamp}");
    (store, late_key)
}

/// Rule 2's deleting pass, which removes the inputs the rewrite superseded and
/// so releases rule 6's hold on them, then rule 6, both past the completion's
/// protection horizon.
async fn sweep_requests(store: &dyn ObjectStoreBackend) -> ErasureRequestSweepOutcome {
    sweep_requests_under(FENCE, store).await
}

/// [`sweep_requests`] with the late-commit fence set to `fence`.
async fn sweep_requests_under(
    fence: LateCommitFence,
    store: &dyn ObjectStoreBackend,
) -> ErasureRequestSweepOutcome {
    let config = sweep_config(fence);
    let clock = FixedClock::new(completed_ns() + config.protection_horizon_ns + NS_PER_HOUR);
    let b = bucket();
    sweep_superseded(
        store,
        &clock,
        &config,
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("rule 2");
    sweep_erasure_requests(
        store,
        &clock,
        &config,
        &NoLeases,
        &tenant_hash(),
        Signal::Metrics,
    )
    .await
    .expect("rule 6")
}

fn windowed_victim() -> Vec<PendingErasureRequest> {
    let b = base_ns();
    request("victim", Some((b, b + 5_000)), 0x4564)
}

/// Commit 2, test (a): a late L0 the store applied after the request completed
/// holds the subject inside the window. No rewrite planned it and the
/// completion gate never saw it, so the `.dreq` stays past its horizon and the
/// keep is counted as held.
#[tokio::test]
async fn a_late_l0_landing_after_completion_keeps_the_dreq() {
    let _serial = SERIAL.lock().await;
    let pending = windowed_victim();
    let (store, _late) = completed_before_a_late_l0(&pending, "victim", 4_000).await;
    let outcome = sweep_requests(&store).await;
    assert_eq!(
        outcome,
        ErasureRequestSweepOutcome {
            deleted: 0,
            kept: 1,
            held_by_superseded_inputs: 1,
        }
    );
    assert!(exists(&store, &pending[0].request_key).await);
}

/// R9 does not depend on the fence: with the fence off the same late L0 keeps
/// the `.dreq`, because a stamp says what the store applied late whether or
/// not the planners leave it out.
#[tokio::test]
async fn with_the_fence_off_a_late_l0_landing_after_completion_keeps_the_dreq() {
    let _serial = SERIAL.lock().await;
    let pending = windowed_victim();
    let (store, _late) = completed_before_a_late_l0(&pending, "victim", 4_000).await;
    let outcome = sweep_requests_under(LateCommitFence::Off, &store).await;
    assert_eq!(
        outcome,
        ErasureRequestSweepOutcome {
            deleted: 0,
            kept: 1,
            held_by_superseded_inputs: 1,
        }
    );
    assert!(exists(&store, &pending[0].request_key).await);
}

/// Commit 2: a request with no window overlaps every late bucket, so a late L0
/// holding only another series still keeps its `.dreq`.
#[tokio::test]
async fn a_late_l0_keeps_a_windowless_dreq_whatever_it_holds() {
    let _serial = SERIAL.lock().await;
    let pending = request("victim", None, 0x4565);
    let (store, _late) = completed_before_a_late_l0(&pending, "other", 4_000).await;
    let outcome = sweep_requests(&store).await;
    assert_eq!((outcome.deleted, outcome.held_by_superseded_inputs), (0, 1));
    assert!(exists(&store, &pending[0].request_key).await);
}

/// Commit 2, test (b): a late L0 whose event times fall outside the request's
/// window cannot hold the subject, so the `.dreq` is deleted.
#[tokio::test]
async fn a_late_l0_outside_the_window_does_not_keep_the_dreq() {
    let _serial = SERIAL.lock().await;
    let pending = windowed_victim();
    let (store, _late) = completed_before_a_late_l0(&pending, "victim", 10_000_000).await;
    let outcome = sweep_requests(&store).await;
    assert_eq!(
        outcome,
        ErasureRequestSweepOutcome {
            deleted: 1,
            kept: 0,
            held_by_superseded_inputs: 0,
        }
    );
    assert!(!exists(&store, &pending[0].request_key).await);
}

/// Commit 2, test (c): nothing persists the keep. Once the late L0 is deleted
/// the next pass deletes the `.dreq`.
#[tokio::test]
async fn the_dreq_is_released_once_the_late_l0_is_gone() {
    let _serial = SERIAL.lock().await;
    let pending = windowed_victim();
    let (store, late) = completed_before_a_late_l0(&pending, "victim", 4_000).await;
    assert_eq!(sweep_requests(&store).await.held_by_superseded_inputs, 1);
    store.delete(&late).await.expect("delete late record");
    let outcome = sweep_requests(&store).await;
    assert_eq!((outcome.deleted, outcome.kept), (1, 0));
    assert!(!exists(&store, &pending[0].request_key).await);
}

/// R9: a retention tombstone does not release the keep. A HEAD that already
/// folded the bucket serves its late L0 until a fold observes the tombstone,
/// so the `.dreq` stays while the late record is stored, and is deleted on the
/// first pass after retention deletes it.
#[tokio::test]
async fn a_late_l0_in_a_tombstoned_bucket_keeps_the_dreq_until_it_is_deleted() {
    let _serial = SERIAL.lock().await;
    let pending = windowed_victim();
    let (store, late) = completed_before_a_late_l0(&pending, "victim", 4_000).await;
    let b = bucket();
    let tombstone = RetentionTombstone {
        format_version: 1,
        tenant_hash: b.tenant_hash.0.to_vec(),
        signal: signal::to_proto(b.signal) as i32,
        shard: b.shard,
        ingest_hour_bucket: b.ingest_hour_bucket,
        retired_at_ns: 0,
        retention_window_ns: 0,
        record_count_observed: 0,
    };
    store
        .put(
            &keys::retention_tombstone_key_for(&tombstone).expect("tombstone key"),
            tombstone.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put tombstone");
    let outcome = sweep_requests(&store).await;
    assert_eq!(
        outcome,
        ErasureRequestSweepOutcome {
            deleted: 0,
            kept: 1,
            held_by_superseded_inputs: 1,
        }
    );
    assert!(exists(&store, &pending[0].request_key).await);

    store
        .delete(&late)
        .await
        .expect("retention deletes the late record");
    let outcome = sweep_requests(&store).await;
    assert_eq!((outcome.deleted, outcome.kept), (1, 0));
    assert!(!exists(&store, &pending[0].request_key).await);
}

/// Test 7 (site 10): a bucket whose remaining below-target L0 is late is named
/// under `LateCommitRecords`, its on-time records are migrated, and the walk
/// goes on to the next bucket.
#[tokio::test]
async fn a_late_below_target_l0_is_named_and_the_walk_goes_on() {
    let _serial = SERIAL.lock().await;
    let store = MemoryStore::new();
    ravel_catalog::validate_or_adopt(
        &store,
        &tenant_hash(),
        Signal::Metrics,
        SHARD + 1,
        0,
        ravel_catalog::AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision tenant/signal");
    let earlier = HOUR - 1;
    let earlier_cutoff = bucket_at(earlier).late_commit_cutoff_ns(&CompactorConfig::default());
    let series = |ts: i64| vec![raw_series("keep", &[("k", "a")], &[(ts, 1.0)])];
    for (seq, stamp) in [
        (1, ms(i64::from(earlier) * NS_PER_HOUR)),
        (2, ms(i64::from(earlier) * NS_PER_HOUR)),
        (3, ms(earlier_cutoff + NS_PER_SECOND)),
    ] {
        let spec = InputSpec::new_at(
            earlier,
            Uuid::from_u128(u128::from(seq)),
            10,
            seq,
            series(i64::from(earlier) * NS_PER_HOUR + 1_000 * i64::try_from(seq).expect("seq")),
        );
        seed_at(&store, &store, stamp, &spec).await;
    }
    for seq in [4, 5] {
        let spec = input(
            u128::from(seq),
            seq,
            series(base_ns() + 1_000 * i64::try_from(seq).expect("seq")),
        );
        seed_at(&store, &store, on_time_ms(), &spec).await;
    }

    let report = migrate_family(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &fenced(FENCE, CompactorConfig::default()),
        tenant_hash(),
        Signal::Metrics,
        "rseg",
        ravel_maintain::build::OUTPUT_FORMAT_VERSION + 1,
        SHARD + 1,
        MigrateBudget::unlimited(),
        "late_commit_fence test",
    )
    .await
    .expect("migrate");
    assert_eq!(
        report.verification,
        Some(Verification::Stragglers {
            l0: 1,
            l1: 2,
            rewrite_parts: 0,
            blocked: vec![],
        }),
        "the late L0 is the one straggler, so the floor is not raised"
    );
    let floor =
        ravel_catalog::current_floor_from_store(&store, &tenant_hash(), Signal::Metrics, "rseg")
            .await
            .expect("read floor");
    assert!(
        floor.is_none_or(|f| f < ravel_maintain::build::OUTPUT_FORMAT_VERSION + 1),
        "{floor:?}"
    );

    assert_eq!(
        report.not_migrated,
        vec![NotMigratedBucket {
            shard: SHARD,
            ingest_hour: earlier,
            path: MigrationPath::L0Migration,
            reason: NotMigratedReason::LateCommitRecords { records: 1 },
        }]
    );
    assert_eq!(report.buckets_migrated, 2, "both buckets' on-time records");
    assert_eq!(report.records_migrated, 4, "the late record is not counted");
}

/// Test 9 (R7): with the fence off, the library default, a plan takes every
/// listed commit record whatever its stamp, and nothing is counted.
#[tokio::test]
async fn with_the_fence_off_every_commit_record_is_planned() {
    let _serial = SERIAL.lock().await;
    assert_eq!(
        CompactorConfig::default().late_commit_fence,
        LateCommitFence::Off
    );
    let (store, specs) = seeded_three().await;
    let late_before = late_commit_records_total(Signal::Metrics);
    let unobserved_before = store_clock_unobserved_total(Signal::Metrics);
    let config = CompactorConfig {
        min_compaction_inputs: 1,
        ..CompactorConfig::default()
    };
    let outcome = compact_bucket(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &config,
        &bucket(),
    )
    .await
    .expect("compaction");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    let record = fetch_compaction_record(&store, &bucket()).await;
    let mut all: Vec<(String, u64, u64)> = specs.iter().map(identity).collect();
    all.sort();
    assert_eq!(identities(&record.inputs), all);
    assert_eq!(late_commit_records_total(Signal::Metrics), late_before);
    assert_eq!(
        store_clock_unobserved_total(Signal::Metrics),
        unobserved_before
    );
}

/// True in a build where ravel-invariant records evaluations (the invariants
/// lane). This crate has no feature that forwards `capture`, so the gate is an
/// assertion macro whose condition asks `test_support::capturing`; the default
/// build expands it to nothing.
fn capturing() -> bool {
    let on = std::cell::Cell::new(false);
    ravel_invariant::inv_sometimes!(
        {
            on.set(ravel_invariant::test_support::capturing());
            true
        },
        "ravel-maintain test: evaluations are captured",
        ""
    );
    on.get()
}

/// `(condition, details)` of every captured evaluation of `message` whose
/// details mention `needle`; empty outside the invariants lane.
fn evaluations(message: &'static str, needle: &str) -> Vec<(bool, String)> {
    let found = std::cell::RefCell::new(Vec::new());
    let _ = (message, needle);
    ravel_invariant::inv_sometimes!(
        {
            *found.borrow_mut() =
                ravel_invariant::test_support::evaluations(message, needle).unwrap_or_default();
            true
        },
        "ravel-maintain test: evaluations are read",
        ""
    );
    found.into_inner()
}

/// Test 10: the two invariants are evaluated on a fenced compaction that
/// leaves two late records out: the plan reports them left out, and the
/// publish-side check finds no late record among the record's inputs.
#[tokio::test]
async fn a_fenced_compaction_evaluates_both_invariants() {
    let _serial = SERIAL.lock().await;
    if !capturing() {
        return;
    }
    const LEFT_OUT: &str = "maintain: a plan left a late commit record out";
    const NAMES_NONE: &str = "maintain: a published record names no late commit record";
    let left_out_before = evaluations(LEFT_OUT, "\"compaction\"").len();
    let names_none_before = evaluations(NAMES_NONE, "\"compaction\"").len();
    let (store, _) = seeded_three().await;
    let config = fenced(
        FENCE,
        CompactorConfig {
            min_compaction_inputs: 1,
            ..CompactorConfig::default()
        },
    );
    compact_bucket(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &config,
        &bucket(),
    )
    .await
    .expect("compaction");

    let left_out = evaluations(LEFT_OUT, "\"compaction\"");
    assert_eq!(left_out.len(), left_out_before + 1, "{left_out:?}");
    let (condition, details) = &left_out[left_out_before];
    assert!(*condition, "{details}");
    assert!(
        details.contains("\"late_commit_records\": \"2\""),
        "{details}"
    );
    let names_none = evaluations(NAMES_NONE, "\"compaction\"");
    assert_eq!(names_none.len(), names_none_before + 1, "{names_none:?}");
    assert!(names_none[names_none_before].0, "{names_none:?}");
}

/// Selects mutant `name` on this thread until the returned guard drops;
/// `None` outside the invariants lane, where no mutant is compiled in.
fn scoped_mutant(name: &'static str) -> Option<Box<dyn std::any::Any>> {
    let guard: std::cell::RefCell<Option<Box<dyn std::any::Any>>> = Default::default();
    let _ = name;
    ravel_invariant::inv_sometimes!(
        {
            *guard.borrow_mut() =
                ravel_invariant::test_support::scoped_mutant(name).expect("a registered mutant");
            true
        },
        "ravel-maintain test: a mutant is scoped",
        ""
    );
    guard.into_inner()
}

/// The `catalog/plan-takes-late-record` mutant on the compaction of test 10:
/// the plan still classifies the two late records, takes them anyway, and the
/// publish-side check evaluates false, naming one of them.
#[tokio::test]
async fn mutant_plan_takes_late_record_falsifies_the_publish_check() {
    let _serial = SERIAL.lock().await;
    if !capturing() {
        return;
    }
    let Some(_mutant) = scoped_mutant("catalog/plan-takes-late-record") else {
        return;
    };
    const NAMES_NONE: &str = "maintain: a published record names no late commit record";
    let names_none_before = evaluations(NAMES_NONE, "\"compaction\"").len();
    let store = MemoryStore::new();
    let mut commit_keys = Vec::new();
    for (stamp, spec) in stamped_three() {
        commit_keys.push(seed_at(&store, &store, stamp, &spec).await);
    }
    let config = fenced(
        FENCE,
        CompactorConfig {
            min_compaction_inputs: 1,
            ..CompactorConfig::default()
        },
    );
    compact_bucket(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &config,
        &bucket(),
    )
    .await
    .expect("compaction");

    let names_none = evaluations(NAMES_NONE, "\"compaction\"");
    assert_eq!(names_none.len(), names_none_before + 1, "{names_none:?}");
    let (condition, details) = &names_none[names_none_before];
    assert!(!*condition, "{details}");
    let late_keys = &commit_keys[1..];
    assert!(
        late_keys.iter().any(|key| details.contains(key.as_str())),
        "{details} names none of {late_keys:?}"
    );
}

mod determinism {
    //! Test 8: the on-time and late split depends on the stamps alone.

    use super::*;
    use proptest::prelude::*;

    /// A commit record key in [`bucket`] for writer `n`. The listing
    /// partitions by key shape and never decodes the body.
    fn commit_key(n: u64) -> String {
        keys::commit_key(
            &tenant_hash(),
            Signal::Metrics,
            SHARD,
            HOUR,
            Uuid::from_u128(u128::from(n)),
            1,
            n,
        )
        .expect("commit key")
    }

    fn stamp_offsets() -> impl Strategy<Value = Vec<i64>> {
        prop::collection::vec(prop_oneof![-2i64..=2, -5_000i64..5_000], 1..12)
    }

    proptest! {
        /// Stamps are offsets in milliseconds from `C(H)`, written in a
        /// shuffled order to a store that lists two keys per page. The split
        /// the listing classifies equals the one computed from the stamps.
        #[test]
        fn the_split_is_the_same_for_every_order_and_page_boundary(
            (offsets, order) in stamp_offsets().prop_flat_map(|offsets| {
                let order: Vec<usize> = (0..offsets.len()).collect();
                (Just(offsets), Just(order).prop_shuffle())
            })
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime");
            let cutoff = cutoff_ns();
            let cutoff_ms = i64::try_from(ms(cutoff)).expect("ms");
            let (late, listing) = runtime.block_on(async {
                let store = MemoryStore::with_page_size(2);
                let mut late = Vec::new();
                for &i in &order {
                    let n = u64::try_from(i).expect("index");
                    let stamp = cutoff_ms + offsets[i];
                    store.set_clock_ms(u64::try_from(stamp).expect("stamp"));
                    let key = commit_key(n);
                    store
                        .put(&key, bytes::Bytes::from_static(b"r"), PutOptions::default())
                        .await
                        .expect("put");
                    if offsets[i] >= 0 {
                        late.push(key);
                    }
                }
                let mut listing = read::list_bucket(&store, &bucket()).await.expect("list");
                listing.classify_late(cutoff);
                (late, listing)
            });
            let mut late = late;
            late.sort();
            let mut on_time: Vec<String> = listing
                .commit_keys
                .iter()
                .filter(|k| late.binary_search(k).is_err())
                .cloned()
                .collect();
            on_time.sort();
            let mut planned = listing.planning_commit_keys();
            planned.sort();
            prop_assert_eq!(listing.commit_keys.len(), offsets.len());
            prop_assert_eq!(&listing.late_commit_keys, &late);
            prop_assert_eq!(planned, on_time);
            prop_assert_eq!(listing.late_commit_cutoff_ns, Some(cutoff));
        }
    }
}
