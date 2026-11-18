//! Concurrent migrate, ingest, compaction and a late N write inside H
//! (ADR-4233 decision 9). After activation of the drill version N+1, four
//! tasks run against one store, interleaved at store-call granularity by a
//! seeded scheduler: `migrate` of the family, a compaction of the hour, an
//! ingest publish at N+1, and a late publish at N from a writer whose permit
//! was granted before activation, so inside H of it. No task runs on a thread,
//! no runtime drives them and nothing waits on a clock: every store call
//! returns `Pending` once and the scheduler picks the next task to poll from
//! the seed.
//!
//! For every seed: the hour reads every published sample exactly once, before
//! and after the sweeps; no flush is the input of two live compaction records;
//! the sweeps delete a flush's object exactly when a compaction took it; and
//! certify's outcome is the one the late write requires. A compaction that
//! took the late write took the two seed flushes listed before it, so nothing
//! below N+1 is left and certify certifies; when none took it, certify reports
//! `Contradicted` naming the first N member the census meets.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use ravel_commit::{keys, record};
use ravel_maintain::{
    Bucket, CertifyReport, CompactionOutcome, FamilyMigrateReport, FixedClock, activate,
    compact_bucket,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_segment::SegmentVersion;
use ravel_types::Signal;
use uuid::Uuid;

use common::pipeline::{
    Flush, HOUR, ManualClock, activate_request, certify_at, compactor_config, compat_keys, drill,
    encode_flush, hash, init, migrate, provision, query_hour, sealed_now_ns, sweep, sweep_times,
    v7, writable, write_flush,
};
use common::tap::TapStore;

const SEEDS: u64 = 64;
const SHARD: u32 = 0;
/// Polls before a schedule counts as stuck; a full schedule takes a few
/// hundred.
const MAX_POLLS: usize = 200_000;
/// `Pending` returns in a row, from one task, without a store call between
/// them: past this the task waits on something no store call will release.
const MAX_IDLE_POLLS: usize = 1_000;

const SEED_A: (u128, u64) = (0xA0, 1);
const SEED_B: (u128, u64) = (0xA1, 2);
const LATE: (u128, u64) = (0xB0, 3);
const INGEST: (u128, u64) = (0xC0, 4);

/// SplitMix64: a fixed, dependency-free stream per seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % n as u64).expect("fits")
    }
}

type Task<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Poll `tasks` to completion in the order `seed` picks, and return each
/// task's output and the order the tasks finished in. `calls` counts the
/// store calls the tasks have entered.
///
/// Polling happens outside any tokio runtime on purpose: inside one, every
/// poll here runs within a single runtime task that never yields to the
/// runtime, so tokio's cooperative budget runs out and the build's fetch
/// semaphore returns `Pending` for good.
fn drive<T>(
    seed: u64,
    calls: &dyn Fn() -> usize,
    mut tasks: Vec<Task<'_, T>>,
) -> (Vec<T>, Vec<usize>) {
    let mut cx = Context::from_waker(Waker::noop());
    let mut rng = Rng(seed);
    let mut out: Vec<Option<T>> = tasks.iter().map(|_| None).collect();
    let mut live: Vec<usize> = (0..tasks.len()).collect();
    let mut finished = Vec::new();
    let mut idle = vec![0usize; tasks.len()];
    let mut polls = 0usize;
    while !live.is_empty() {
        polls += 1;
        assert!(
            polls < MAX_POLLS,
            "seed {seed}: no progress after {polls} polls, live tasks {live:?}"
        );
        let pick = rng.below(live.len());
        let task = live[pick];
        let before = calls();
        match tasks[task].as_mut().poll(&mut cx) {
            Poll::Ready(done) => {
                out[task] = Some(done);
                live.remove(pick);
                finished.push(task);
            }
            Poll::Pending if calls() > before => idle[task] = 0,
            Poll::Pending => {
                idle[task] += 1;
                assert!(
                    idle[task] < MAX_IDLE_POLLS,
                    "seed {seed}: task {task} returned Pending {MAX_IDLE_POLLS} times in a row \
                     without a store call, after {polls} polls"
                );
            }
        }
    }
    let out = out.into_iter().map(|d| d.expect("finished")).collect();
    (out, finished)
}

/// Run one future to completion with the same poller.
fn block_on<T>(seed: u64, future: impl Future<Output = T>) -> T {
    let (mut out, _) = drive(seed, &|| 0, vec![Box::pin(future) as Task<'_, T>]);
    out.pop().expect("one output")
}

fn flush(identity: (u128, u64), version: SegmentVersion, value: f64) -> Flush {
    Flush {
        version,
        ..Flush::v7(SHARD, identity.0, identity.1, value)
    }
}

/// The two seed flushes at N, the late flush at N, and the ingest flush at
/// N+1, all in [`HOUR`] on [`SHARD`].
fn flushes() -> [Flush; 4] {
    [
        flush(SEED_A, SegmentVersion::V7, 1.0),
        flush(SEED_B, SegmentVersion::V7, 2.0),
        flush(LATE, SegmentVersion::V7, 3.0),
        flush(INGEST, SegmentVersion::Drill, 4.0),
    ]
}

/// Every sample the hour reads, flattened over the resolved objects, sorted.
async fn read_samples(store: &Arc<MemoryStore>, now_ns: i64) -> Vec<(i64, u64)> {
    let mut samples: Vec<(i64, u64)> = query_hour(store.clone(), 1, now_ns)
        .await
        .into_iter()
        .flat_map(|(_, series)| series)
        .flat_map(|s| s.samples)
        .collect();
    samples.sort_unstable();
    samples
}

/// The `(writer, seq)` inputs of every compaction record in the store that no
/// version 2 record supersedes, by record key.
async fn compacted_inputs(store: &dyn ObjectStoreBackend) -> BTreeMap<String, Vec<(u128, u64)>> {
    let mut records = BTreeMap::new();
    for meta in list_all(store, "").await.expect("list") {
        if keys::parse_compaction_record_key(&meta.key).is_err() {
            continue;
        }
        let bytes = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get a listed compaction record")
            .data;
        records.insert(
            meta.key.clone(),
            record::decode_compaction(&bytes).expect("decode a compaction record"),
        );
    }
    let superseded: BTreeSet<String> = records
        .values()
        .map(|r| r.superseded_record_key.clone())
        .filter(|k| !k.is_empty())
        .collect();
    records
        .into_iter()
        .filter(|(key, _)| !superseded.contains(key))
        .map(|(key, r)| {
            let inputs = r
                .inputs
                .iter()
                .map(|i| {
                    let writer = Uuid::parse_str(&i.writer_id).expect("uuid").as_u128();
                    (writer, i.writer_seq)
                })
                .collect();
            (key, inputs)
        })
        .collect()
}

#[derive(Debug, Default)]
struct Tally {
    certified: usize,
    contradicted: usize,
}

async fn one_seed(seed: u64, tally: &mut Tally) {
    let store = Arc::new(MemoryStore::with_page_size(2));
    let clock = Arc::new(ManualClock::new(sealed_now_ns()));
    let [a, b, late, ingest] = flushes();
    write_flush(store.as_ref(), &a).await;
    write_flush(store.as_ref(), &b).await;
    provision(store.as_ref(), 1).await;
    init(store.as_ref(), &clock).await;
    let activated = activate(
        store.as_ref(),
        Some(&compat_keys()),
        clock.as_ref(),
        &writable(),
        &activate_request(None),
    )
    .await
    .expect("activate");
    assert!(activated.walk.complete, "seed {seed}: {:?}", activated.walk);
    let config = compactor_config(store.clone(), clock.clone()).await;

    #[derive(Debug)]
    enum Done {
        Migrate(ravel_maintain::Result<FamilyMigrateReport>),
        Compact(ravel_maintain::Result<CompactionOutcome>),
        Published,
    }
    let tap = TapStore::yielding(store.clone());
    let compact_clock = FixedClock::new(sealed_now_ns());
    let bucket = Bucket::new(hash(), Signal::Metrics, SHARD, HOUR);
    let tasks: Vec<Task<'_, Done>> = vec![
        Box::pin(async { Done::Migrate(migrate(&tap, &config, 1).await) }),
        Box::pin(async {
            Done::Compact(compact_bucket(&tap, &compact_clock, &config, &bucket).await)
        }),
        Box::pin(async {
            write_flush(&tap, &ingest).await;
            Done::Published
        }),
        Box::pin(async {
            write_flush(&tap, &late).await;
            Done::Published
        }),
    ];
    let (done, order) = drive(seed, &|| tap.calls(), tasks);
    let writes: Vec<String> = tap
        .take_writes()
        .into_iter()
        .map(|w| format!("{:?} {}", w.op, w.key))
        .collect();
    let context =
        format!("seed {seed}: finish order {order:?}, outcomes {done:#?}, writes {writes:#?}");
    for outcome in &done {
        match outcome {
            Done::Migrate(Err(e)) => panic!("{context}: migrate failed: {e}"),
            Done::Compact(Err(e)) => panic!("{context}: compaction failed: {e}"),
            _ => {}
        }
    }

    let mut all: Vec<(i64, u64)> = flushes()
        .iter()
        .map(|f| (f.ts_ns, f.value.to_bits()))
        .collect();
    all.sort_unstable();
    assert_eq!(
        read_samples(&store, sealed_now_ns()).await,
        all,
        "{context}: the hour reads every published sample exactly once"
    );

    let compacted = compacted_inputs(store.as_ref()).await;
    for f in flushes() {
        let holders: Vec<&String> = compacted
            .iter()
            .filter(|(_, inputs)| inputs.contains(&(f.writer, f.seq)))
            .map(|(key, _)| key)
            .collect();
        assert!(
            holders.len() <= 1,
            "{context}: flush {:#x}/{} is the input of {holders:?}",
            f.writer,
            f.seq
        );
    }
    let taken = |f: &Flush| compacted.values().any(|i| i.contains(&(f.writer, f.seq)));
    let late_taken = taken(&late);
    // The N members no compaction took, in key order: what the census meets.
    let uncompacted_n: BTreeSet<String> = [&a, &b, &late]
        .into_iter()
        .filter(|f| !taken(f))
        .map(|f| encode_flush(f).data_key)
        .collect();
    assert_eq!(
        uncompacted_n.is_empty(),
        late_taken,
        "{context}: a compaction that took the late write took the seeds listed before it"
    );

    let (mark, delete) = sweep_times(&config);
    for now in [mark, delete] {
        sweep(store.as_ref(), &config, now, SHARD)
            .await
            .unwrap_or_else(|e| panic!("{context}: sweep at {now}: {e}"));
    }
    assert_eq!(
        read_samples(&store, delete).await,
        all,
        "{context}: after the sweeps the hour still reads every sample exactly once"
    );
    for f in flushes() {
        let key = encode_flush(&f).data_key;
        assert_eq!(
            store.head(&key).await.is_ok(),
            !taken(&f),
            "{context}: {key} is swept exactly when a compaction took it"
        );
    }

    let report = certify_at(store.as_ref(), delete, None)
        .await
        .unwrap_or_else(|e| panic!("{context}: certify failed: {e}"));
    match uncompacted_n.first() {
        None => {
            tally.certified += 1;
            assert!(
                matches!(report, CertifyReport::Certified { version, .. } if version == drill()),
                "{context}: the late write was compacted, nothing below N+1 is left, so \
                 certify certifies; got {report:?}"
            );
        }
        Some(first) => {
            tally.contradicted += 1;
            assert_eq!(
                report,
                CertifyReport::Contradicted {
                    member_key: first.clone(),
                    found_version: v7(),
                },
                "{context}: the late write was not compacted, so certify is contradicted"
            );
        }
    }
}

#[test]
fn seeded_interleavings_of_migrate_ingest_compaction_and_a_late_write() {
    let mut tally = Tally::default();
    for seed in 0..SEEDS {
        block_on(seed, one_seed(seed, &mut tally));
    }
    assert!(
        tally.certified > 0 && tally.contradicted > 0,
        "the seeds cover both outcomes: {tally:?}"
    );
}
