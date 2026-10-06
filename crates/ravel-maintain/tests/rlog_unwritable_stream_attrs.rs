//! A log object already in storage whose `stream_attrs` blob the RLOG writer
//! refuses (issue #2554). Such an object predates the writer's validation
//! (issue #2548). Compaction leaves it out of the merge, counted and warned
//! once, and merges the rest of its bucket; the object stays in storage,
//! unnamed by the compaction record, and is not read again by this process.
//!
//! The skip counter and the set of skipped objects are process-wide, so every
//! test here seeds its own writer ids (distinct object keys) and holds
//! [`SERIAL`] while it reads counter deltas. The warn line is captured through
//! one global subscriber installed before any test reaches the callsite: a
//! per-thread subscriber would race `tracing`'s process-wide callsite interest
//! cache across the tests of this binary.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use common::*;
use ravel_commit::erasure::compute_compaction_input_set_hash;
use ravel_commit::{keys, record};
use ravel_fleet::claim::COMPACTION_CLAIMS_PREFIX;
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::{
    ClaimParticipant, Clock, CompactionInputSkipReason, CompactionOutcome, CompactorConfig,
    Coordination, FixedClock, MaintainError, MaintainMemo, MaintainReport, MigrateBudget,
    PublishOutcome, UnwritableBucket, Verification, compact_bucket,
    compaction_inputs_skipped_total, migrate_bucket_format, migrate_family,
};
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence, SequenceStep};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_types::Signal;
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

/// Serializes the tests that read the process-wide skip counter.
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

/// Install the capturing global subscriber, once per test binary. Every test
/// calls this before anything else.
fn capture() -> &'static CapturedLog {
    LOG.get_or_init(|| {
        let log = CapturedLog(Arc::new(Mutex::new(Vec::new())));
        tracing_subscriber::fmt()
            .with_writer(log.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .init();
        log
    })
}

/// Captured lines that are the skip warning and name `key`.
fn skip_warnings_naming(key: &str) -> Vec<String> {
    let bytes = capture().0.lock().expect("log buffer lock").clone();
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter(|l| l.contains("compaction skipped an input object") && l.contains(key))
        .map(str::to_string)
        .collect()
}

fn skipped_total() -> u64 {
    compaction_inputs_skipped_total(
        Signal::Logs,
        CompactionInputSkipReason::UnwritableStreamAttrs,
    )
}

/// Stream `n`'s record with its scope name's first byte replaced by `0xFF`:
/// well-formed framing whose scope name is not UTF-8, which the reader's
/// decoder refuses and so the writer refuses too. Its stream id is stream
/// `n`'s, so `n` must not be a stream any healthy input carries.
fn refused_record(stream_n: u32, ts: i64) -> ravel_logseg::LogRecord {
    let mut r = log_record(stream_n, ts, "refused");
    let at = r
        .stream_attrs
        .windows(5)
        .position(|w| w == b"scope")
        .expect("scope name in blob");
    r.stream_attrs[at] = 0xFF;
    assert!(ravel_logseg::stream_attr_pairs(&r.stream_attrs).is_err());
    r
}

/// The data-object key a commit record names.
async fn data_key_of(store: &dyn ObjectStoreBackend, commit_key: &str) -> String {
    let got = store
        .get(commit_key, GetRange::Full)
        .await
        .expect("get commit record");
    let rec = record::decode(&got.data).expect("decode commit record");
    keys::reconstruct_data_key(&rec).expect("data key")
}

/// Seeds healthy input `n` (writer `base + n`, sequence `n`) with two records.
async fn seed_healthy(store: &dyn ObjectStoreBackend, base: u128, n: u64) -> String {
    let ts = 10 * n as i64;
    seed_rlog_input(
        store,
        Uuid::from_u128(base + u128::from(n)),
        10,
        n,
        &[log_record(0, ts, "alpha"), log_record(1, ts + 1, "bravo")],
    )
    .await
}

/// Seeds refused input `n` (writer `base + n`, sequence `n`) and returns its
/// (commit key, data key).
async fn seed_refused(store: &dyn ObjectStoreBackend, base: u128, n: u64) -> (String, String) {
    let commit = seed_rlog_input_unchecked(
        store,
        Uuid::from_u128(base + u128::from(n)),
        10,
        n,
        &[refused_record(9, 10 * n as i64)],
    )
    .await;
    let data = data_key_of(store, &commit).await;
    (commit, data)
}

/// `mem` behind a [`FaultStore`] with one sequence per key in `keys`, in
/// order, each passing every GET of its key through and counting it:
/// `sequence_progress(i)` is the number of reads of `keys[i]`. One more
/// sequence, at index `keys.len()`, counts the compaction claim PUTs.
fn counting_reads_of(mem: &Arc<MemoryStore>, keys: &[&str]) -> FaultStore<Arc<MemoryStore>> {
    let mut plan = FaultPlan::empty();
    for key in keys {
        plan = plan.with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(*key)
                .with_steps(vec![SequenceStep::Passthrough; 1024]),
        );
    }
    plan = plan.with_sequence(
        Sequence::new(Op::Put)
            .with_key_contains(COMPACTION_CLAIMS_PREFIX)
            .with_steps(vec![SequenceStep::Passthrough; 1024]),
    );
    FaultStore::new(Arc::clone(mem), plan)
}

/// A [`ClaimSleeper`] that returns immediately.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// A config that claims every bucket as one process on `clock`, as the
/// background supervisor does.
fn claiming_config(clock: &FixedClock) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_participant: Some(
            ClaimParticipant::new(
                Uuid::from_u128(77),
                Arc::new(clock.clone()) as Arc<dyn Clock>,
            )
            .with_sleeper(Arc::new(NoWait)),
        ),
        ..CompactorConfig::default()
    }
}

/// One maintenance tick over the logs bucket's shard.
async fn tick(
    memo: &mut MaintainMemo,
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
) -> MaintainReport {
    let b = logs_bucket();
    ravel_maintain::scan::scan_and_maintain_with_memo(
        memo,
        store,
        clock,
        config,
        &ravel_maintain::RetentionConfig::default(),
        &ravel_maintain::NoLeases,
        b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("the tick succeeds")
}

async fn compaction_record_count(store: &dyn ObjectStoreBackend) -> usize {
    let b = logs_bucket();
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .expect("prefix");
    list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .filter(|m| {
            matches!(
                keys::partition_bucket_entry(&m.key),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .count()
}

/// The writer ids a compaction record names as its inputs.
fn input_writers(record: &ravel_proto::commit::v1::CompactionRecord) -> BTreeSet<String> {
    record.inputs.iter().map(|i| i.writer_id.clone()).collect()
}

fn writer(base: u128, n: u64) -> String {
    Uuid::from_u128(base + u128::from(n)).to_string()
}

/// A bucket of two healthy inputs and one refused input: the first tick merges
/// the two healthy inputs and publishes a record naming only them, leaves the
/// refused object and its commit record in storage byte for byte, counts the
/// skip once and warns once with the object key and the decoder's error. The
/// second tick finds the bucket compacted and reads the refused object zero
/// times, and the counter stays at 1.
///
/// Against the pre-change code the first tick fails with the writer's
/// `Corrupted` refusal (`expect("the tick succeeds")`); without the record's
/// input list being cut down, the published record names the refused writer;
/// without the hash recomputed after the skip (`hash = input_set_hash(&inputs)`
/// in the rewrite primitive), the record's hash covers three inputs while it
/// names two.
#[tokio::test]
async fn a_refused_object_is_skipped_and_the_rest_of_its_bucket_merged() {
    capture();
    let _serial = SERIAL.lock().await;
    const BASE: u128 = 0x100;

    let mem = Arc::new(MemoryStore::new());
    let now_ns = sealed_now_ns();
    mem.set_clock_ms((now_ns / 1_000_000) as u64);
    seed_healthy(&*mem, BASE, 1).await;
    seed_healthy(&*mem, BASE, 2).await;
    let (refused_commit, refused_data) = seed_refused(&*mem, BASE, 3).await;
    let refused_bytes = mem
        .get(&refused_data, GetRange::Full)
        .await
        .expect("refused object")
        .data;
    let store = counting_reads_of(&mem, &[&refused_data]);
    let clock = FixedClock::new(now_ns);
    let config = claiming_config(&clock);
    let mut memo = MaintainMemo::with_default_interval();
    let before = skipped_total();

    let first = tick(&mut memo, &store, &clock, &config).await;
    assert_eq!(first.compacted, 1, "the bucket compacts");
    let first_reads = store.sequence_progress(0);
    assert!(
        first_reads > 0,
        "the first tick reads the refused object's catalog"
    );
    assert_eq!(skipped_total() - before, 1, "the skip counts once");

    let record = fetch_compaction_record(&*mem, &logs_bucket()).await;
    assert_eq!(
        input_writers(&record),
        BTreeSet::from([writer(BASE, 1), writer(BASE, 2)]),
        "the record names the healthy inputs and not the skipped one"
    );
    assert_eq!(
        record.input_set_hash,
        compute_compaction_input_set_hash(&record.inputs).to_vec(),
        "the input set hash covers exactly the inputs the record names"
    );
    assert_eq!(
        record.parts.iter().map(|p| p.sample_count).sum::<u64>(),
        4,
        "both healthy inputs' records are merged"
    );

    assert_eq!(
        mem.get(&refused_data, GetRange::Full)
            .await
            .expect("the refused object is still stored")
            .data,
        refused_bytes,
        "the refused object is untouched"
    );
    mem.get(&refused_commit, GetRange::Full)
        .await
        .expect("the refused object's commit record is still stored");
    let listed: Vec<String> = list_all(&*mem, "")
        .await
        .expect("list store")
        .into_iter()
        .map(|m| m.key)
        .collect();
    assert!(listed.contains(&refused_data), "and still listed");
    assert!(listed.contains(&refused_commit));

    let warnings = skip_warnings_naming(&refused_data);
    assert_eq!(warnings.len(), 1, "one warning: {warnings:?}");
    assert!(
        warnings[0].contains("not utf-8") && warnings[0].contains("unwritable_stream_attrs"),
        "the warning carries the decoder's error and the reason: {}",
        warnings[0]
    );

    let second = tick(&mut memo, &store, &clock, &config).await;
    assert_eq!(second.compacted, 0);
    assert_eq!(second.already_done, 1, "the bucket is already compacted");
    assert_eq!(
        store.sequence_progress(0),
        first_reads,
        "the second tick does not read the refused object"
    );
    assert_eq!(skipped_total() - before, 1, "the counter stays at 1");
    assert_eq!(skip_warnings_naming(&refused_data).len(), 1);
    assert_eq!(compaction_record_count(&*mem).await, 1);
}

/// A bucket holding only refused objects has nothing left to merge: the first
/// tick reads both objects, counts and warns each once, publishes nothing and
/// reports the bucket below the input threshold with no pending L0 record. The
/// second tick reads neither object, because this process remembers both,
/// counts nothing more, and puts no claim.
///
/// Against the pre-change code the first tick fails with the writer's
/// refusal; with the remembered set unconsulted (`drop_skipped_inputs` in
/// `compact_bucket_scoped`), the second tick reads both catalogs again; with
/// that filter after the claim acquisition, the second tick puts a claim.
#[tokio::test]
async fn a_skipped_object_is_not_read_again_by_the_same_process() {
    capture();
    let _serial = SERIAL.lock().await;
    const BASE: u128 = 0x200;

    let mem = Arc::new(MemoryStore::new());
    let now_ns = sealed_now_ns();
    mem.set_clock_ms((now_ns / 1_000_000) as u64);
    let (_, first_data) = seed_refused(&*mem, BASE, 1).await;
    let (_, second_data) = seed_refused(&*mem, BASE, 2).await;
    let store = counting_reads_of(&mem, &[&first_data, &second_data]);
    let clock = FixedClock::new(now_ns);
    let config = claiming_config(&clock);
    let mut memo = MaintainMemo::with_default_interval();
    let before = skipped_total();

    let first = tick(&mut memo, &store, &clock, &config).await;
    assert_eq!(first.compacted, 0);
    assert_eq!(first.already_done, 1, "reported below the input threshold");
    assert_eq!(first.l0_records_pending, 0, "no writable input is pending");
    let reads = (store.sequence_progress(0), store.sequence_progress(1));
    assert!(reads.0 > 0 && reads.1 > 0, "both catalogs read: {reads:?}");
    let claim_puts = store.sequence_progress(2);
    assert!(claim_puts > 0, "the first tick claims the bucket");
    assert_eq!(skipped_total() - before, 2);
    assert_eq!(compaction_record_count(&*mem).await, 0, "nothing published");

    let second = tick(&mut memo, &store, &clock, &config).await;
    assert_eq!(second.already_done, 1);
    assert_eq!(second.l0_records_pending, 0);
    assert_eq!(
        (store.sequence_progress(0), store.sequence_progress(1)),
        reads,
        "the second tick reads neither refused object"
    );
    assert_eq!(
        store.sequence_progress(2),
        claim_puts,
        "the second tick puts no claim"
    );
    assert_eq!(skipped_total() - before, 2, "and counts nothing more");
    assert_eq!(skip_warnings_naming(&first_data).len(), 1);
    assert_eq!(skip_warnings_naming(&second_data).len(), 1);
}

/// A bucket of one healthy and one refused input passes the listing's input
/// count, but only one writable input remains once the refused one is dropped,
/// below the default minimum of 2. The first tick reads the refused object,
/// counts and warns it once, publishes no record and reports the bucket below
/// the minimum with the one healthy record pending. The second tick reads
/// neither the refused object nor publishes, and puts no claim.
///
/// With the minimum not re-checked after the skip (the
/// `inputs.len() < config.min_compaction_inputs` test after
/// `skip_unwritable_inputs` in the rewrite primitive), the first tick publishes
/// a record over the one healthy input.
#[tokio::test]
async fn a_skip_that_leaves_the_bucket_below_the_minimum_publishes_nothing() {
    capture();
    let _serial = SERIAL.lock().await;
    const BASE: u128 = 0x400;

    let mem = Arc::new(MemoryStore::new());
    let now_ns = sealed_now_ns();
    mem.set_clock_ms((now_ns / 1_000_000) as u64);
    seed_healthy(&*mem, BASE, 1).await;
    let (_, refused_data) = seed_refused(&*mem, BASE, 2).await;
    let store = counting_reads_of(&mem, &[&refused_data]);
    let clock = FixedClock::new(now_ns);
    let config = claiming_config(&clock);
    assert_eq!(config.min_compaction_inputs, 2);
    let mut memo = MaintainMemo::with_default_interval();
    let before = skipped_total();

    let first = tick(&mut memo, &store, &clock, &config).await;
    assert_eq!(first.compacted, 0, "nothing compacts");
    assert_eq!(first.already_done, 1, "reported below the input threshold");
    assert_eq!(
        first.l0_records_pending, 1,
        "the healthy input is the one record left pending"
    );
    let reads = store.sequence_progress(0);
    assert!(
        reads > 0,
        "the first tick reads the refused object's catalog"
    );
    let claim_puts = store.sequence_progress(1);
    assert_eq!(compaction_record_count(&*mem).await, 0, "nothing published");
    assert_eq!(skipped_total() - before, 1, "the skip counts once");
    assert_eq!(skip_warnings_naming(&refused_data).len(), 1);

    let second = tick(&mut memo, &store, &clock, &config).await;
    assert_eq!(second.compacted, 0);
    assert_eq!(second.already_done, 1);
    assert_eq!(second.l0_records_pending, 1);
    assert_eq!(
        store.sequence_progress(0),
        reads,
        "the second tick does not read the refused object"
    );
    assert_eq!(
        store.sequence_progress(1),
        claim_puts,
        "the second tick puts no claim"
    );
    assert_eq!(compaction_record_count(&*mem).await, 0, "nor publishes");
    assert_eq!(skipped_total() - before, 1, "the counter stays at 1");
    assert_eq!(skip_warnings_naming(&refused_data).len(), 1);
}

/// Format migration skips no input: over a bucket holding a refused object it
/// fails, before the merge, with `MaintainError::UnwritableInput` naming the
/// refused object's key (issue #2580), publishes nothing, and moves no skip
/// counter.
///
/// With the migration passing `UnwritableInputs::Skip` (in `load_then_rewrite`)
/// it skips the refused object, counts it, and publishes a record over the two
/// healthy inputs. With the `UnwritableInputs::Fail` arm's check removed from
/// `rewrite_and_publish_guarded`, the writer's `LogSeg(Corrupted)` surfaces
/// instead.
#[tokio::test]
async fn format_migration_still_fails_on_a_refused_object() {
    capture();
    let _serial = SERIAL.lock().await;
    const BASE: u128 = 0x500;

    let store = MemoryStore::new();
    seed_healthy(&store, BASE, 1).await;
    seed_healthy(&store, BASE, 2).await;
    let (_, refused_data) = seed_refused(&store, BASE, 3).await;
    let before = skipped_total();

    let err = migrate_bucket_format(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &logs_bucket(),
        u32::MAX,
    )
    .await
    .expect_err("the migration fails on the refused object");
    match &err {
        MaintainError::UnwritableInput {
            object_key,
            reason,
            detail,
        } => {
            assert_eq!(object_key, &refused_data);
            assert_eq!(*reason, CompactionInputSkipReason::UnwritableStreamAttrs);
            assert!(detail.contains("not utf-8"), "{detail}");
        }
        other => panic!("expected UnwritableInput, got {other:?}"),
    }
    assert_eq!(
        compaction_record_count(&store).await,
        0,
        "nothing published"
    );
    assert_eq!(skipped_total(), before, "the skip counter does not move");
    assert!(skip_warnings_naming(&refused_data).is_empty());
}

/// Issue #2580: a migrate walk over three logs buckets whose middle bucket
/// holds a refused object migrates the first and the third, names the middle
/// one in `unwritable_skipped` with the refused object's key, writes nothing
/// into it, and leaves the family floor where it was: the re-audit counts the
/// middle bucket's two L0 records as stragglers. The L0 records claim one
/// version below the target, so the migrated buckets' parts meet it and the
/// stragglers are the skipped bucket's alone.
///
/// Against the pre-change code the walk propagates the writer's refusal out of
/// `migrate_family` and the `expect` fails; with the walk's
/// `Err(MaintainError::UnwritableInput { .. })` arm removed it propagates the
/// typed error the same way.
#[tokio::test]
async fn migrate_skips_a_bucket_with_a_refused_object_and_walks_on() {
    capture();
    let _serial = SERIAL.lock().await;
    const BASE: u128 = 0x600;
    const FAMILY: &str = "rlog";
    let target = ravel_maintain::rlog::OUTPUT_FORMAT_VERSION;
    let below = target - 1;

    let store = MemoryStore::new();
    let hours = [HOUR - 2, HOUR - 1, HOUR];
    let mut seq = 0u64;
    let mut refused_data = String::new();
    for (i, hour) in hours.into_iter().enumerate() {
        for n in 0..2u64 {
            seq += 1;
            let ts = i64::from(hour) * 3_600_000_000_000 + n as i64;
            let refused = i == 1 && n == 1;
            let records = if refused {
                vec![refused_record(9, ts)]
            } else {
                vec![log_record(0, ts, "alpha")]
            };
            let commit = seed_rlog_input_at_hour(
                &store,
                Uuid::from_u128(BASE + u128::from(seq)),
                10,
                seq,
                hour,
                &records,
                refused,
                below,
            )
            .await;
            if refused {
                refused_data = data_key_of(&store, &commit).await;
            }
        }
    }
    let floor_before =
        ravel_catalog::current_floor_from_store(&store, &tenant_hash(), Signal::Logs, FAMILY)
            .await
            .expect("read floor");
    let skipped_before = skipped_total();

    let report = migrate_family(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        tenant_hash(),
        Signal::Logs,
        FAMILY,
        target,
        SHARD + 1,
        MigrateBudget::unlimited(),
        "rlog_unwritable_stream_attrs test",
    )
    .await
    .expect("the walk finishes past the refused bucket");

    assert_eq!(report.buckets_migrated, 2, "{report:?}");
    assert_eq!(report.records_migrated, 4, "{report:?}");
    assert_eq!(
        report.unwritable_skipped,
        vec![UnwritableBucket {
            shard: SHARD,
            ingest_hour: HOUR - 1,
            object_key: refused_data.clone(),
            reason: CompactionInputSkipReason::UnwritableStreamAttrs,
        }]
    );
    assert!(report.not_migrated.is_empty(), "{report:?}");
    assert!(report.walk_complete);
    assert_eq!(
        report.verification,
        Some(Verification::Stragglers {
            l0: 2,
            l1: 0,
            rewrite_parts: 0,
            blocked: Vec::new(),
        }),
        "the skipped bucket's two records hold the floor down, and nothing else does"
    );
    assert_eq!(
        ravel_catalog::current_floor_from_store(&store, &tenant_hash(), Signal::Logs, FAMILY)
            .await
            .expect("read floor"),
        floor_before,
        "the family floor does not move"
    );
    for (hour, expected) in [(HOUR - 2, 1), (HOUR - 1, 0), (HOUR, 1)] {
        let prefix = keys::commit_shard_hour_prefix(&tenant_hash(), Signal::Logs, SHARD, hour)
            .expect("prefix");
        let records = list_all(&store, &prefix)
            .await
            .expect("list")
            .into_iter()
            .filter(|m| {
                matches!(
                    keys::partition_bucket_entry(&m.key),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .count();
        assert_eq!(records, expected, "compaction records at hour {hour}");
    }
    assert_eq!(skipped_total(), skipped_before, "migration skips no input");
}

/// A bucket with no refused object compacts every input exactly as before:
/// the record names all three, every record is merged, and nothing is counted
/// or warned.
#[tokio::test]
async fn a_bucket_without_a_refused_object_compacts_every_input() {
    capture();
    let _serial = SERIAL.lock().await;
    const BASE: u128 = 0x300;

    let store = MemoryStore::new();
    for n in 1..=3 {
        seed_healthy(&store, BASE, n).await;
    }
    let before = skipped_total();

    let outcome = compact_bucket(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &logs_bucket(),
    )
    .await
    .expect("compaction succeeds");
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
    let record = fetch_compaction_record(&store, &logs_bucket()).await;
    assert_eq!(
        input_writers(&record),
        BTreeSet::from([writer(BASE, 1), writer(BASE, 2), writer(BASE, 3)])
    );
    assert_eq!(record.parts.iter().map(|p| p.sample_count).sum::<u64>(), 6);
    assert_eq!(skipped_total(), before, "nothing skipped");
}
