//! An erasure and a pinned query during an in-flight migrate (ADR-4233
//! decision 9, "retention, erasure, legal hold, pinned query and Flight ticket
//! during migrate": pinned inputs keep certify waiting; erasure completes at
//! the active version). After activation of N+1, a query resolves its snapshot
//! of the hour's three N objects, then `migrate` and an erasure rewrite of one
//! sample run against one store, interleaved at store-call granularity by a
//! seeded scheduler as in `concurrent_interleaving.rs`. Whichever publishes
//! first, the other either builds on it or stands down, and a rerun of each
//! converges.
//!
//! For every seed: every rewrite part is stamped and written at N+1; the hour
//! reads every sample but the erased one, exactly once; the pinned snapshot's
//! N objects still read as resolved, and certify refuses on them, naming the
//! first in key order, until the sweeps delete them; then certify certifies.
//!
//! This outcome holds only with claims on (`claiming` below puts both tasks
//! under `Coordination::On`) and with the store clock aligned to the claim
//! clock (the `store.set_clock_ms` call in `one_seed`). With claims off
//! (`--maintain-claims off`) or against the unclaimed background migrate, the
//! erased sample is served instead; that race is tracked separately as
//! issue #4449, with the unclaimed background migrate itself as issue #4448.
//! This test does not exercise either mode.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::compat::FAMILY_RSEG;
use ravel_catalog::read_floors_from_store;
use ravel_commit::{erasure, keys, signal};
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::{
    Bucket, CertifyReport, ClaimParticipant, Clock, CompactorConfig, Coordination,
    ErasureRewriteOutcome, FamilyMigrateReport, FixedClock, MaintainMemo, NoLeases,
    PendingErasureRequest, PublishOutcome, activate, erasure_rewrite_bucket,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_segment::{ReaderLimits, open_from_full};
use ravel_types::Signal;
use uuid::Uuid;

use common::pipeline::{
    Flush, HOUR, METRIC, ManualClock, SECOND_NS, activate_request, certify_at, compactor_config,
    compat_keys, drill, hash, init, migrate, provision, query_hour, sealed_now_ns, sweep,
    sweep_times, v7, writable, write_flush,
};
use common::sched::{Task, block_on, drive};
use common::tap::TapStore;

const SEEDS: u64 = 32;
const SHARD: u32 = 0;
/// Rounds of erasure and migrate after the interleaved run before both must
/// have converged.
const MAX_ROUNDS: i64 = 3;

fn flushes() -> [Flush; 3] {
    [
        Flush::v7(SHARD, 0xA0, 1, 1.5),
        Flush::v7(SHARD, 0xA1, 2, 2.5),
        Flush::v7(SHARD, 0xA2, 3, 3.5),
    ]
}

/// One request erasing the series' sample at `victim`'s timestamp only.
fn pending(victim: &Flush) -> Vec<PendingErasureRequest> {
    let request_id = Uuid::from_u128(0x4419);
    vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request: ErasureRequest {
            format_version: 1,
            tenant_hash: hash().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            request_id: request_id.to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: "__name__".to_string(),
                value: METRIC.to_string(),
            }],
            window_start_ns: victim.ts_ns,
            window_end_ns: victim.ts_ns + 1,
            reason: String::new(),
        },
    }]
}

/// The claim acquisition jitter, not waited out: no runtime drives these
/// passes.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(std::future::ready(()))
    }
}

/// `config` taking the bucket claim as process `process` at `now_ns`, as every
/// fleet maintenance worker does.
fn claiming(config: CompactorConfig, process: u128, now_ns: i64) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_participant: Some(
            ClaimParticipant::new(
                Uuid::from_u128(process),
                Arc::new(FixedClock::new(now_ns)) as Arc<dyn Clock>,
            )
            .with_sleeper(Arc::new(NoWait)),
        ),
        ..config
    }
}

async fn erase(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    pending: &[PendingErasureRequest],
    now_ns: i64,
) -> ravel_maintain::Result<ErasureRewriteOutcome> {
    erasure_rewrite_bucket(
        store,
        &FixedClock::new(now_ns),
        config,
        &NoLeases,
        &Bucket::new(hash(), Signal::Metrics, SHARD, HOUR),
        pending,
        &mut MaintainMemo::with_default_interval(),
    )
    .await
}

/// Every part of every rewrite record on the store: its key, the version its
/// record stamps it at, and its trailer version.
async fn rewrite_parts(store: &dyn ObjectStoreBackend) -> Vec<(String, u32, u32)> {
    let mut parts = Vec::new();
    for meta in list_all(store, "").await.expect("list") {
        if keys::parse_rewrite_record_key(&meta.key).is_err() {
            continue;
        }
        let bytes = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get a rewrite record")
            .data;
        let record = erasure::decode_rewrite(&bytes).expect("decode a rewrite record");
        for part in &record.parts {
            let key = keys::reconstruct_rewrite_part_key(&record, part).expect("part key");
            let data = store
                .get(&key, GetRange::Full)
                .await
                .expect("get a rewrite part")
                .data;
            let trailer = open_from_full(&data, ReaderLimits::default())
                .expect("open a rewrite part")
                .version;
            parts.push((key, part.segment_format_version, u32::from(trailer)));
        }
    }
    parts
}

/// Every sample in `objects`, sorted.
fn samples(objects: &[(String, Vec<common::DecodedSeries>)]) -> Vec<(i64, u64)> {
    let mut out: Vec<(i64, u64)> = objects
        .iter()
        .flat_map(|(_, series)| series)
        .flat_map(|s| s.samples.iter().copied())
        .collect();
    out.sort_unstable();
    out
}

fn sample(flush: &Flush) -> (i64, u64) {
    (flush.ts_ns, flush.value.to_bits())
}

async fn rseg_floor(store: &dyn ObjectStoreBackend) -> Option<u32> {
    read_floors_from_store(store, &hash(), Signal::Metrics)
        .await
        .expect("read floors")
        .expect("a provisioning record")
        .into_iter()
        .find(|f| f.family == FAMILY_RSEG)
        .map(|f| f.floor_version)
}

#[derive(Debug, Default)]
struct Tally {
    erasure_published_in_run: usize,
    erasure_stood_down_in_run: usize,
}

async fn one_seed(seed: u64, tally: &mut Tally) {
    let store = Arc::new(MemoryStore::with_page_size(2));
    let clock = Arc::new(ManualClock::new(sealed_now_ns()));
    let [a, b, victim] = flushes();
    for flush in [&a, &b, &victim] {
        write_flush(store.as_ref(), flush).await;
    }
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
    let base = compactor_config(store.clone(), clock.clone()).await;
    let config = claiming(base.clone(), 1, sealed_now_ns());
    let erase_config = claiming(base.clone(), 2, sealed_now_ns());
    let pending = pending(&victim);

    let pinned = query_hour(store.clone(), 1, sealed_now_ns()).await;
    assert_eq!(
        pinned.len(),
        3,
        "seed {seed}: the snapshot is the three N objects"
    );
    assert_eq!(samples(&pinned), [&a, &b, &victim].map(sample));

    #[derive(Debug)]
    enum Done {
        Migrate(ravel_maintain::Result<FamilyMigrateReport>),
        Erase(ravel_maintain::Result<ErasureRewriteOutcome>),
    }
    // Claim expiry is judged on the store's clock: the claims' time base.
    store.set_clock_ms(u64::try_from(sealed_now_ns() / 1_000_000).expect("ms"));
    let tap = TapStore::yielding(store.clone());
    let tasks: Vec<Task<'_, Done>> = vec![
        Box::pin(async { Done::Migrate(migrate(&tap, &config, 1).await) }),
        Box::pin(async {
            Done::Erase(erase(&tap, &erase_config, &pending, sealed_now_ns()).await)
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
            Done::Erase(Err(e)) => panic!("{context}: erasure failed: {e}"),
            Done::Erase(Ok(ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            })) => tally.erasure_published_in_run += 1,
            Done::Erase(Ok(_)) => tally.erasure_stood_down_in_run += 1,
            Done::Migrate(Ok(_)) => {}
        }
    }

    // Each rerun starts past the lease of every claim the run before it took,
    // so a claim the interleaving left held never stands a rerun down.
    let lease = i64::try_from(base.claim_lease_duration.as_nanos()).expect("lease fits");
    let mut rounds = 0;
    let settled = loop {
        let later = sealed_now_ns() + (rounds + 1) * (lease + SECOND_NS);
        store.set_clock_ms(u64::try_from(later / 1_000_000).expect("ms"));
        let erased = block_on(
            seed,
            erase(
                store.as_ref(),
                &claiming(base.clone(), 2, later),
                &pending,
                later,
            ),
        )
        .unwrap_or_else(|e| panic!("{context}: erasure rerun: {e}"));
        block_on(
            seed,
            migrate(store.as_ref(), &claiming(base.clone(), 1, later), 1),
        )
        .unwrap_or_else(|e| panic!("{context}: migrate rerun: {e}"));
        let floor = block_on(seed, rseg_floor(store.as_ref()));
        let nothing_to_erase = matches!(
            erased,
            ErasureRewriteOutcome::AlreadyApplied | ErasureRewriteOutcome::NoApplicableRequests
        );
        if nothing_to_erase && floor == Some(drill()) {
            break later;
        }
        rounds += 1;
        assert!(
            rounds < MAX_ROUNDS,
            "{context}: no convergence after {rounds} rounds: erasure {erased:?}, floor {floor:?}"
        );
    };

    let parts = block_on(seed, rewrite_parts(store.as_ref()));
    assert!(!parts.is_empty(), "{context}: the erasure published parts");
    for (key, stamp, trailer) in &parts {
        assert_eq!(
            (*stamp, *trailer),
            (drill(), drill()),
            "{context}: {key} is stamped and written at the active N+1"
        );
    }
    let survivors = [&a, &b].map(sample).to_vec();
    assert_eq!(
        samples(&block_on(
            seed,
            query_hour(store.clone(), 1, sealed_now_ns())
        )),
        survivors,
        "{context}: the hour reads every sample but the erased one, exactly once"
    );

    let mut pinned_keys: Vec<&String> = pinned.iter().map(|(key, _)| key).collect();
    pinned_keys.sort();
    let first_pinned = pinned_keys[0].clone();
    // The sweeps' protection horizon runs from the last record a rerun wrote.
    let (mark, delete) = sweep_times(&config);
    let shift = settled - sealed_now_ns();
    let (mark, delete) = (mark + shift, delete + shift);
    for now in [mark, delete] {
        for (key, series) in &pinned {
            let bytes = block_on(seed, store.get(key, GetRange::Full))
                .unwrap_or_else(|e| {
                    panic!("{context}: pinned {key} before the sweep at {now}: {e}")
                })
                .data;
            assert_eq!(
                &common::decode_rseg(&bytes).expect("decode a pinned object"),
                series,
                "{context}: the pinned query still reads {key} as resolved"
            );
        }
        assert_eq!(
            block_on(seed, certify_at(store.as_ref(), now, None)).expect("certify"),
            CertifyReport::Contradicted {
                member_key: first_pinned.clone(),
                found_version: v7(),
            },
            "{context}: the pinned N inputs keep certify waiting at {now}"
        );
        block_on(seed, sweep(store.as_ref(), &config, now, SHARD))
            .unwrap_or_else(|e| panic!("{context}: sweep at {now}: {e}"));
    }
    for key in &pinned_keys {
        assert!(
            block_on(seed, store.head(key)).is_err(),
            "{context}: {key} is swept"
        );
    }
    let report = block_on(seed, certify_at(store.as_ref(), delete, None)).expect("certify");
    assert!(
        matches!(report, CertifyReport::Certified { version, .. } if version == drill()),
        "{context}: with the pinned inputs swept, certify certifies: {report:?}"
    );
    assert_eq!(
        samples(&block_on(seed, query_hour(store.clone(), 1, delete))),
        survivors,
        "{context}: after the sweeps the hour still reads the survivors exactly once"
    );
}

#[test]
fn an_erasure_and_a_pinned_query_during_migrate_complete_at_n_plus_1() {
    let mut tally = Tally::default();
    for seed in 0..SEEDS {
        block_on(seed, one_seed(seed, &mut tally));
    }
    assert!(
        tally.erasure_published_in_run > 0 && tally.erasure_stood_down_in_run > 0,
        "the seeds cover an erasure that publishes during the migrate and one that stands \
         down for it: {tally:?}"
    );
}
