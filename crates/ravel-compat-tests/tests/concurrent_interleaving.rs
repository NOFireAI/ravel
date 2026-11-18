//! Concurrent migrate, ingest, compaction and a late N write inside H
//! (ADR-4233 decision 9). After activation of the drill version N+1, four
//! tasks run against one store, interleaved at store-call granularity by a
//! seeded scheduler: `migrate` of the family, a compaction of the hour, an
//! ingest publish at N+1, and a late publish at N -- a raw N flush standing
//! in for a permit granted inside H of it. No task runs on a thread,
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
use std::sync::Arc;

use ravel_commit::{keys, record};
use ravel_maintain::{
    Bucket, CertifyReport, CompactionOutcome, FamilyMigrateReport, FixedClock, activate,
    compact_bucket,
};
use ravel_object_store::fault::Op;
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
use common::sched::{Task, block_on, drive};
use common::tap::{TapStore, Write};

const SEEDS: u64 = 64;
const SHARD: u32 = 0;
const SEED_A: (u128, u64) = (0xA0, 1);
const SEED_B: (u128, u64) = (0xA1, 2);
const LATE: (u128, u64) = (0xB0, 3);
const INGEST: (u128, u64) = (0xC0, 4);

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

/// Task indices as `one_seed` builds them, for `tasks_overlapped`.
const MIGRATE_TASK: usize = 0;
const COMPACT_TASK: usize = 1;

#[derive(Debug, Default)]
struct Tally {
    certified: usize,
    contradicted: usize,
    /// Seeds where `migrate` and the compaction both reached the publish
    /// step for this bucket's hour (`compaction_record_publish_attempts`
    /// sees 2 or more) AND their store calls actually overlapped in time
    /// (`tasks_overlapped`), rather than one task's calls running entirely
    /// before the other's. Attempt count alone cannot tell these apart: both
    /// tasks reach the publish step regardless of scheduling, so a seed
    /// where `migrate` runs to completion before `compact_bucket` starts
    /// still shows 2 attempts. Only the call-ordinal overlap proves the
    /// scheduler actually interleaved them.
    overlapping_publish: usize,
}

/// Attempted (not necessarily successful) PUTs toward publishing a level-1
/// compaction record for `bucket`'s hour in `writes`: the record's own
/// `create_if_absent` CAS (`keys::parse_compaction_record_key`), plus the
/// part object that precedes it (`keys::l1_part_key`'s directory), which a
/// publisher writes unconditionally before it re-lists and decides whether
/// to attempt the CAS at all. Neither `migrate_bucket_format` nor
/// `compact_bucket` retries a failed publish, and nothing else in this test
/// writes under either path, so two or more attempts, from the part prefix
/// alone or combined with the record CAS, means both tasks reached the
/// publish step for this bucket -- even when only one of them goes on to
/// win the record CAS.
fn compaction_record_publish_attempts(writes: &[Write], bucket: &Bucket) -> usize {
    let part_prefix = format!(
        "t/{}/{}/l1/{:04}/{}/",
        bucket.tenant_hash.to_hex(),
        bucket.signal.key_prefix(),
        bucket.shard,
        keys::ingest_hour_string(bucket.ingest_hour_bucket),
    );
    writes
        .iter()
        .filter(|w| w.op == Op::Put)
        .filter(|w| {
            w.key.starts_with(&part_prefix)
                || keys::parse_compaction_record_key(&w.key).is_ok_and(|parsed| {
                    parsed.tenant_hash == bucket.tenant_hash
                        && parsed.signal == bucket.signal
                        && parsed.shard == bucket.shard
                        && parsed.ingest_hour_bucket == bucket.ingest_hour_bucket
                })
        })
        .count()
}

/// Whether task `a`'s and task `b`'s store calls actually overlapped in
/// `call_log` (`TapStore::call_log`, one task tag per call in call order):
/// the ordinal range from each task's first call to its last intersects the
/// other's. A count of attempts from both tasks (as
/// `compaction_record_publish_attempts` gives) is true whether or not the
/// scheduler ever interleaved them -- a seed where `migrate` finishes every
/// one of its calls before `compact_bucket` makes its first still shows both
/// tasks reaching the publish step. Only this ordinal check tells that case
/// apart from a seed where the scheduler actually wove the two tasks'
/// calls together.
fn tasks_overlapped(call_log: &[Option<usize>], a: usize, b: usize) -> bool {
    let range = |task: usize| -> Option<(usize, usize)> {
        let positions: Vec<usize> = call_log
            .iter()
            .enumerate()
            .filter(|(_, tag)| **tag == Some(task))
            .map(|(i, _)| i)
            .collect();
        positions.first().copied().zip(positions.last().copied())
    };
    match (range(a), range(b)) {
        (Some((a_first, a_last)), Some((b_first, b_last))) => {
            a_first.max(b_first) <= a_last.min(b_last)
        }
        _ => false,
    }
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
    let raw_writes = tap.take_writes();
    if compaction_record_publish_attempts(&raw_writes, &bucket) >= 2
        && tasks_overlapped(&tap.call_log(), MIGRATE_TASK, COMPACT_TASK)
    {
        tally.overlapping_publish += 1;
    }
    let writes: Vec<String> = raw_writes
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
        common::with_seed_context(
            seed,
            std::panic::AssertUnwindSafe(|| {
                block_on(seed, one_seed(seed, &mut tally));
            }),
        );
    }
    assert!(
        tally.certified > 0 && tally.contradicted > 0,
        "the seeds cover both outcomes: {tally:?}"
    );
    assert!(
        tally.overlapping_publish > 0,
        "at least one seed must have migrate and the compaction both attempt a \
         compaction record publish to the hour, or the single-winner check below \
         never actually exercised the race: {tally:?}"
    );
}
