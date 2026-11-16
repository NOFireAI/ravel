//! `ravel_maintain_compactions_in_flight` (issue #2666): the gauge reads 1
//! while a bucket merge is in progress and returns to 0 on every way a merge
//! ends: a publish, a failed input read, and the merge future being dropped
//! mid-flight. Each of those, an abandoned publish and a dry run, logs exactly
//! one merge-end line naming its outcome. The gauge stays at 0 through the
//! claim, the format-floor read, and a bucket the gates turn away, and the
//! merge-start and merge-end lines name the prefix the publish line's key
//! carries.
//!
//! The gauge is process-wide, so the tests in this binary run one at a time
//! under [`SERIAL`] and each asserts absolute values. A merge is parked with a
//! `FaultStore` hold gate on one request; no test sleeps.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::future::Future;
use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use common::{InputSpec, TENANT, bucket, raw_series, sealed_now_ns, seed_input};
use ravel_catalog::{
    AbsentPolicy, FloorBasis, provisioning_key, raise_format_floor, validate_or_adopt,
};
use ravel_fleet::claim::COMPACTION_CLAIMS_PREFIX;
use ravel_ingest::{FormatFamily, FormatFloorMetrics, FormatFloorPolicy};
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::{
    ClaimParticipant, ClaimedCompaction, Clock, CompactionOutcome, CompactorConfig, Coordination,
    FixedClock, MaintainError, PublishOutcome, compact_bucket, compact_bucket_claimed,
    compactions_in_flight,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, GateHandle, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_types::{Signal, TenantId};
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

/// L0 data object keys; commit records and the bucket listing never match.
const L0_KEYS: &str = "/l0/";

/// The message `MergeInFlight::start` logs.
const START_LINE: &str = "compaction merge started";

/// The message `MergeInFlight`'s `Drop` logs.
const END_LINE: &str = "compaction merge ended";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Everything the global subscriber has written.
static LOG: OnceLock<CapturedLog> = OnceLock::new();

#[derive(Clone)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Install the capturing global subscriber at INFO, once per test binary.
fn capture() -> &'static CapturedLog {
    LOG.get_or_init(|| {
        let log = CapturedLog(Arc::new(Mutex::new(Vec::new())));
        tracing_subscriber::fmt()
            .with_writer(log.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .init();
        log
    })
}

/// Captured lines containing `needle`.
fn lines_with(needle: &str) -> Vec<String> {
    let bytes = capture().0.lock().expect("log buffer lock").clone();
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter(|l| l.contains(needle))
        .map(str::to_string)
        .collect()
}

/// The value of `field=` on `line`, up to the next space.
fn field<'a>(line: &'a str, name: &str) -> &'a str {
    let at = line
        .find(&format!(" {name}="))
        .unwrap_or_else(|| panic!("no {name}= on {line}"));
    let rest = &line[at + name.len() + 2..];
    rest.split(' ').next().unwrap_or_default()
}

/// The `outcome=` of every merge-end line past the first `from`.
fn end_outcomes_since(from: usize) -> Vec<String> {
    lines_with(END_LINE)[from..]
        .iter()
        .map(|line| field(line, "outcome").to_string())
        .collect()
}

async fn seeded(plan: FaultPlan) -> FaultStore<MemoryStore> {
    let store = FaultStore::new(MemoryStore::new(), plan);
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![raw_series(
                "m",
                &[("k", "a")],
                &[(1_000, 1.0), (2_000, 2.0)],
            )],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("m", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ] {
        seed_input(store.inner(), &spec).await;
    }
    store
}

/// Wait until `gate` parks exactly one `op` on a key containing `key_part`
/// and return its id.
async fn parked_on(gate: &GateHandle, op: Op, key_part: &str) -> u64 {
    gate.wait_until_held(1).await;
    let held = gate.held_details();
    assert_eq!(held.len(), 1, "the gate parked exactly one call: {held:?}");
    let (id, held_op, key) = &held[0];
    assert_eq!(*held_op, op);
    assert!(
        key.contains(key_part),
        "the parked call is on {key_part}: {key}"
    );
    *id
}

/// Poll `merge` until `gate` parks its first input read, failing if the merge
/// finishes first.
async fn park<F: Future + Unpin>(merge: &mut F, gate: &GateHandle) -> u64 {
    park_on(merge, gate, Op::Get, L0_KEYS).await
}

/// Poll `merge` until `gate` parks an `op` on `key_part`, failing if the merge
/// finishes first.
async fn park_on<F: Future + Unpin>(
    merge: &mut F,
    gate: &GateHandle,
    op: Op,
    key_part: &str,
) -> u64 {
    tokio::select! {
        _ = merge => panic!("the merge finished before {op:?} on {key_part}"),
        id = parked_on(gate, op, key_part) => id,
    }
}

/// A [`ClaimSleeper`] that returns immediately, so acquisition jitter never
/// waits on the real timer.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// A compactor config that takes a claim on `clock`.
fn claiming_config(clock: &FixedClock) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_lease_duration: Duration::from_secs(3),
        claim_participant: Some(
            ClaimParticipant::new(
                Uuid::from_u128(9),
                Arc::new(clock.clone()) as Arc<dyn Clock>,
            )
            .with_sleeper(Arc::new(NoWait)),
        ),
        ..CompactorConfig::default()
    }
}

/// A compactor config that reads the bucket's format floor and refuses a run
/// below it.
fn floor_config() -> CompactorConfig {
    CompactorConfig {
        format_floor: Some(FormatFloorPolicy::new(
            false,
            Arc::new(FormatFloorMetrics::default()),
        )),
        ..CompactorConfig::default()
    }
}

/// The tenant's metrics provisioning key, which the format-floor read GETs.
fn floor_key() -> String {
    provisioning_key(&TenantId::new(TENANT).hash(), Signal::Metrics)
}

/// Provision the tenant and raise its RSEG floor one above the build's
/// version, so a compaction of the bucket is refused.
async fn raise_floor_above_writer(store: &MemoryStore) {
    let tenant = TenantId::new(TENANT).hash();
    validate_or_adopt(
        store,
        &tenant,
        Signal::Metrics,
        8,
        1,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    raise_format_floor(
        store,
        &tenant,
        Signal::Metrics,
        FormatFamily::Rseg.as_str(),
        FormatFamily::Rseg.writable_version() + 1,
        FloorBasis {
            observed_entries: 0,
            observed_newest_created_unix_ns: 0,
            observed_shards: 1,
        },
        "test",
        2,
    )
    .await
    .expect("raise floor");
}

#[tokio::test]
async fn gauge_reads_one_while_parked_and_zero_after_publish() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = seeded(FaultPlan::empty()).await;
    let gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();
    let bucket = bucket();
    assert_eq!(compactions_in_flight(), 0, "no merge before the run");
    let ends = lines_with(END_LINE).len();

    let mut merge = Box::pin(compact_bucket(&store, &clock, &config, &bucket));
    let id = park(&mut merge, &gate).await;
    assert_eq!(compactions_in_flight(), 1, "the parked merge is in flight");
    assert!(
        end_outcomes_since(ends).is_empty(),
        "a parked merge has not ended"
    );
    assert!(gate.release(id), "the parked GET was released");
    let outcome = merge.await.expect("compaction");

    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the merge published: {outcome:?}"
    );
    assert_eq!(
        compactions_in_flight(),
        0,
        "a published merge is not in flight"
    );
    assert_eq!(
        end_outcomes_since(ends),
        ["published"],
        "a published merge logs one end line"
    );
}

/// A merge whose `max_compaction_lifetime` passes before its record PUT
/// abandons without publishing and logs one end line saying so.
#[tokio::test]
async fn an_abandoned_merge_logs_one_abandoned_end_line() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = seeded(FaultPlan::empty()).await;
    let gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();
    let bucket = bucket();
    let ends = lines_with(END_LINE).len();

    let mut merge = Box::pin(compact_bucket(&store, &clock, &config, &bucket));
    let id = park(&mut merge, &gate).await;
    clock.set(clock.now_ns() + config.max_compaction_lifetime_ns + 1);
    assert!(gate.release(id), "the parked GET was released");
    let outcome = merge.await.expect("compaction");

    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Abandoned,
                ..
            }
        ),
        "the merge abandoned: {outcome:?}"
    );
    assert_eq!(
        compactions_in_flight(),
        0,
        "an abandoned merge is not in flight"
    );
    assert_eq!(
        end_outcomes_since(ends),
        ["abandoned"],
        "an abandoned merge logs one end line"
    );
}

/// A dry run reports a publish it never made; its end line names it a dry run.
#[tokio::test]
async fn a_dry_run_merge_logs_one_dry_run_end_line() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = seeded(FaultPlan::empty()).await;
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig {
        dry_run: true,
        ..CompactorConfig::default()
    };
    let ends = lines_with(END_LINE).len();

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
        "a dry run reports a publish: {outcome:?}"
    );
    assert_eq!(
        end_outcomes_since(ends),
        ["dry-run"],
        "a dry run logs one end line"
    );
}

#[tokio::test]
async fn gauge_returns_to_zero_after_an_input_read_fails() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = seeded(
        FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("injected".to_string()))
                .with_key_contains(L0_KEYS)
                .with_occurrence(Occurrence::Always),
        ),
    )
    .await;
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();
    let ends = lines_with(END_LINE).len();

    let result = compact_bucket(&store, &clock, &config, &bucket()).await;

    assert!(
        matches!(result, Err(MaintainError::Store(_))),
        "the injected input read error fails the merge: {result:?}"
    );
    assert!(
        store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
        "the input read fault fired"
    );
    assert_eq!(
        compactions_in_flight(),
        0,
        "a failed merge is not in flight"
    );
    assert_eq!(
        end_outcomes_since(ends),
        ["failed"],
        "a failed merge logs one end line"
    );
}

#[tokio::test]
async fn gauge_returns_to_zero_when_the_merge_future_is_dropped() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = seeded(FaultPlan::empty()).await;
    let gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();
    let bucket = bucket();
    let ends = lines_with(END_LINE).len();

    let mut merge = Box::pin(compact_bucket(&store, &clock, &config, &bucket));
    park(&mut merge, &gate).await;
    assert_eq!(compactions_in_flight(), 1, "the parked merge is in flight");
    drop(merge);

    assert_eq!(
        compactions_in_flight(),
        0,
        "a dropped merge is not in flight"
    );
    assert_eq!(
        end_outcomes_since(ends),
        ["dropped"],
        "a dropped merge logs one end line"
    );
}

/// A run that takes a claim starts its merge only once the claim is held: the
/// gauge reads 0 while the claim's create PUT is parked, and 1 once the merge
/// reaches its first input read.
#[tokio::test]
async fn gauge_reads_zero_while_the_claim_is_parked() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = seeded(FaultPlan::empty()).await;
    let claim_gate = store.hold(
        Op::Put,
        Some(COMPACTION_CLAIMS_PREFIX.to_string()),
        Occurrence::Nth(1),
    );
    let l0_gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));
    let clock = FixedClock::new(sealed_now_ns());
    let config = claiming_config(&clock);
    let bucket = bucket();

    let mut merge = Box::pin(compact_bucket_claimed(&store, &clock, &config, &bucket));
    let id = park_on(&mut merge, &claim_gate, Op::Put, COMPACTION_CLAIMS_PREFIX).await;
    assert_eq!(
        compactions_in_flight(),
        0,
        "a merge waiting on its claim is not in flight"
    );
    assert!(claim_gate.release(id), "the parked claim PUT was released");
    let id = park(&mut merge, &l0_gate).await;
    assert_eq!(compactions_in_flight(), 1, "the claimed merge is in flight");
    assert!(l0_gate.release(id), "the parked GET was released");
    let outcome = merge.await.expect("compaction");

    assert!(
        matches!(
            outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            })
        ),
        "the claimed merge published: {outcome:?}"
    );
    assert_eq!(
        compactions_in_flight(),
        0,
        "a published merge is not in flight"
    );
}

/// The format-floor read comes before the merge starts: the gauge reads 0
/// while it is parked, and 1 once the merge reaches its first input read.
#[tokio::test]
async fn gauge_reads_zero_while_the_format_floor_read_is_parked() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = seeded(FaultPlan::empty()).await;
    let floor_gate = store.hold(Op::Get, Some(floor_key()), Occurrence::Nth(1));
    let l0_gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));
    let clock = FixedClock::new(sealed_now_ns());
    let config = floor_config();
    let bucket = bucket();

    let mut merge = Box::pin(compact_bucket(&store, &clock, &config, &bucket));
    let id = park_on(&mut merge, &floor_gate, Op::Get, &floor_key()).await;
    assert_eq!(
        compactions_in_flight(),
        0,
        "a merge waiting on its format-floor read is not in flight"
    );
    assert!(floor_gate.release(id), "the parked floor read was released");
    let id = park(&mut merge, &l0_gate).await;
    assert_eq!(
        compactions_in_flight(),
        1,
        "the admitted merge is in flight"
    );
    assert!(l0_gate.release(id), "the parked GET was released");
    let outcome = merge.await.expect("compaction");

    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the merge published: {outcome:?}"
    );
    assert_eq!(
        compactions_in_flight(),
        0,
        "a published merge is not in flight"
    );
}

/// A bucket with fewer inputs than `min_compaction_inputs` is turned away by
/// its listing and never starts a merge: no merge-start or merge-end line is
/// logged and the gauge reads 0 after the run.
#[tokio::test]
async fn a_bucket_below_min_inputs_never_moves_the_gauge() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
    seed_input(
        store.inner(),
        &InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![raw_series("m", &[("k", "a")], &[(1_000, 1.0)])],
        ),
    )
    .await;
    let list_gate = store.hold(Op::List, None, Occurrence::Nth(1));
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();
    let bucket = bucket();
    let starts = lines_with(START_LINE).len();
    let ends = lines_with(END_LINE).len();

    // The LIST comes before every gate, so the gauge reads 0 while it is
    // parked whatever the gates decide. The hold is there to show the run got
    // past the seal gate and listed the bucket: the min-inputs gate, not the
    // seal gate, is what the start-line count below is about.
    let mut merge = Box::pin(compact_bucket(&store, &clock, &config, &bucket));
    let id = park_on(&mut merge, &list_gate, Op::List, "/m/c/").await;
    assert!(list_gate.release(id), "the parked LIST was released");
    let outcome = merge.await.expect("compaction");

    assert!(
        matches!(outcome, CompactionOutcome::BelowMinInputs { count: 1 }),
        "one input is below the minimum of two: {outcome:?}"
    );
    assert_eq!(
        lines_with(START_LINE).len(),
        starts,
        "a bucket below min-inputs logs no merge start"
    );
    assert!(
        end_outcomes_since(ends).is_empty(),
        "a bucket below min-inputs logs no merge end"
    );
    assert_eq!(
        compactions_in_flight(),
        0,
        "nothing is in flight after the run"
    );
}

/// A bucket whose format floor is above the build's output version is refused
/// and never starts a merge: the gauge reads 0 while the floor read is parked
/// and after the refusal, and no merge-start line is logged.
#[tokio::test]
async fn a_bucket_refused_by_its_format_floor_never_moves_the_gauge() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = seeded(FaultPlan::empty()).await;
    raise_floor_above_writer(store.inner()).await;
    let floor_gate = store.hold(Op::Get, Some(floor_key()), Occurrence::Nth(1));
    let clock = FixedClock::new(sealed_now_ns());
    let config = floor_config();
    let bucket = bucket();
    let starts = lines_with(START_LINE).len();

    let mut merge = Box::pin(compact_bucket(&store, &clock, &config, &bucket));
    let id = park_on(&mut merge, &floor_gate, Op::Get, &floor_key()).await;
    assert_eq!(
        compactions_in_flight(),
        0,
        "a bucket whose floor is being read is not merging"
    );
    assert!(floor_gate.release(id), "the parked floor read was released");
    let result = merge.await;

    assert!(
        matches!(result, Err(MaintainError::BelowFormatFloor(_))),
        "the floor refuses the run: {result:?}"
    );
    assert_eq!(
        lines_with(START_LINE).len(),
        starts,
        "a refused bucket logs no merge start"
    );
    assert_eq!(
        compactions_in_flight(),
        0,
        "nothing is in flight after the run"
    );
}

/// The merge-start line's `bucket=` is the bucket's commit prefix, the
/// merge-end line repeats it, and the publish line's `key=` is that prefix
/// plus a file name. The chaos oracle (scripts/chaos/lib.sh) pairs the lines
/// by exactly this relation.
#[tokio::test]
async fn merge_start_line_names_the_prefix_of_the_published_key() {
    let _serial = SERIAL.lock().await;
    capture();
    let store = seeded(FaultPlan::empty()).await;
    let clock = FixedClock::new(sealed_now_ns());
    let config = CompactorConfig::default();
    let bucket = bucket();
    let starts = lines_with(START_LINE).len();
    let ends = lines_with(END_LINE).len();
    let publishes = lines_with("compaction record published").len();

    let outcome = compact_bucket(&store, &clock, &config, &bucket)
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
        "the merge published: {outcome:?}"
    );

    let start_lines = lines_with(START_LINE);
    let publish_lines = lines_with("compaction record published");
    assert_eq!(start_lines.len(), starts + 1, "one merge-start line");
    assert_eq!(publish_lines.len(), publishes + 1, "one publish line");
    let prefix = field(&start_lines[starts], "bucket");
    let key = field(&publish_lines[publishes], "key");
    assert_eq!(
        prefix,
        ravel_commit::keys::commit_shard_hour_prefix(
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
            bucket.ingest_hour_bucket,
        )
        .expect("prefix"),
        "the start line names the bucket's commit prefix"
    );
    let end_lines = lines_with(END_LINE);
    assert_eq!(end_lines.len(), ends + 1, "one merge-end line");
    assert_eq!(
        field(&end_lines[ends], "bucket"),
        prefix,
        "the end line names the start line's prefix"
    );
    let file = key
        .strip_prefix(prefix)
        .unwrap_or_else(|| panic!("the published key {key} is under {prefix}"));
    assert!(
        !file.is_empty() && !file.contains('/'),
        "the published key is a file directly under the prefix: {key}"
    );
}
