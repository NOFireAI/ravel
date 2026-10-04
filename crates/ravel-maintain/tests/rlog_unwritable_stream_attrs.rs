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
use ravel_commit::{keys, record};
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::{
    ClaimParticipant, Clock, CompactionInputSkipReason, CompactionOutcome, CompactorConfig,
    Coordination, FixedClock, MaintainMemo, MaintainReport, PublishOutcome, compact_bucket,
    compaction_inputs_skipped_total,
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
/// `sequence_progress(i)` is the number of reads of `keys[i]`.
fn counting_reads_of(mem: &Arc<MemoryStore>, keys: &[&str]) -> FaultStore<Arc<MemoryStore>> {
    let mut plan = FaultPlan::empty();
    for key in keys {
        plan = plan.with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(*key)
                .with_steps(vec![SequenceStep::Passthrough; 1024]),
        );
    }
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
/// input list being cut down, the published record names the refused writer.
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
/// reports the bucket below the input threshold. The second tick reads neither
/// object, because this process remembers both, and counts nothing more.
///
/// Against the pre-change code the first tick fails with the writer's
/// refusal; with the remembered set unconsulted (`is_skipped_input` in the
/// rewrite primitive), the second tick reads both catalogs again.
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
    let reads = (store.sequence_progress(0), store.sequence_progress(1));
    assert!(reads.0 > 0 && reads.1 > 0, "both catalogs read: {reads:?}");
    assert_eq!(skipped_total() - before, 2);
    assert_eq!(compaction_record_count(&*mem).await, 0, "nothing published");

    let second = tick(&mut memo, &store, &clock, &config).await;
    assert_eq!(second.already_done, 1);
    assert_eq!(
        (store.sequence_progress(0), store.sequence_progress(1)),
        reads,
        "the second tick reads neither refused object"
    );
    assert_eq!(skipped_total() - before, 2, "and counts nothing more");
    assert_eq!(skip_warnings_naming(&first_data).len(), 1);
    assert_eq!(skip_warnings_naming(&second_data).len(), 1);
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
