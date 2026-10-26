//! Timing harness for the memoised L0 derivation (issue #3231), over the
//! fixture `resolve_memo_tests` builds. Kept in its own file because its
//! subject is wall time: the test-hygiene guard tracks names bound from
//! `.elapsed()` file-wide, and this harness's phase names would otherwise taint
//! unrelated assertions in catalog.rs.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::time::Instant;

use ravel_commit::declared_stats::read_commit_record;

use super::resolve_memo_tests::{RECORDS, SIGNAL, commit_key_of, config, fixture, tenant, window};
use super::*;

fn per_resolve_us(elapsed: std::time::Duration, iters: u32) -> f64 {
    elapsed.as_secs_f64() * 1e6 / f64::from(iters)
}

/// Timing harness for one warm resolve's derived work over 408 cached L0
/// records, split by phase. Not a gate: run it on purpose with
/// `cargo test -p ravel-catalog --release --lib -- --ignored --nocapture
/// warm_resolve_derived_work_split`.
#[tokio::test]
#[ignore = "timing harness, run by hand in release"]
async fn warm_resolve_derived_work_split() {
    const ITERS: u32 = 300;
    let (store, records) = fixture(RECORDS).await;
    let catalog = Catalog::new(store, config()).expect("catalog");
    let (range, now) = window();
    let cold = catalog
        .resolve(&tenant(), SIGNAL, range, &[], now)
        .await
        .expect("cold resolve");
    assert_eq!(cold.segments.len(), RECORDS as usize);

    let keyed: Vec<(String, &CommitRecord)> =
        records.iter().map(|r| (commit_key_of(r), r)).collect();
    let acc = QueryAccounting::new();
    let entries: Vec<CachedCommit> = keyed
        .iter()
        .map(|(key, _)| {
            catalog
                .cache
                .get_cached(&tenant(), key, &acc)
                .expect("warm entry")
        })
        .collect();
    let cached: Vec<Arc<CommitRecord>> = entries.iter().map(|e| e.record.clone()).collect();
    let refs: Vec<SegmentRef> = keyed
        .iter()
        .zip(&cached)
        .map(|((key, _), record)| derive_l0(key, record).expect("ref").0)
        .collect();

    let started = Instant::now();
    for _ in 0..ITERS {
        black_box(
            catalog
                .resolve(&tenant(), SIGNAL, range, &[], now)
                .await
                .expect("warm resolve"),
        );
    }
    let full = per_resolve_us(started.elapsed(), ITERS);

    // Once per key: the listing parse is the only one a resolve runs.
    let started = Instant::now();
    for _ in 0..ITERS {
        for (key, _) in &keyed {
            black_box(keys::partition_bucket_entry(black_box(key)).expect("parse"));
        }
    }
    let key_parsing = per_resolve_us(started.elapsed(), ITERS);

    let started = Instant::now();
    for _ in 0..ITERS {
        for (key, _) in &keyed {
            black_box(catalog.cache.get_cached(&tenant(), black_box(key), &acc));
        }
    }
    let cache_touch = per_resolve_us(started.elapsed(), ITERS);

    let started = Instant::now();
    for _ in 0..ITERS {
        for ((key, _), entry) in keyed.iter().zip(&entries) {
            black_box(
                catalog
                    .l0_segment_ref(&tenant(), key, black_box(entry))
                    .expect("ref"),
            );
        }
    }
    let memo_hit = per_resolve_us(started.elapsed(), ITERS);

    let shard_of = |record: &CommitRecord| record.shard;
    let started = Instant::now();
    for _ in 0..ITERS {
        for ((key, _), record) in keyed.iter().zip(&cached) {
            validate_expected_fields(
                &catalog,
                black_box(record),
                &tenant(),
                SIGNAL,
                shard_of(record),
                key,
            )
            .expect("valid");
        }
    }
    let validation = per_resolve_us(started.elapsed(), ITERS);

    let started = Instant::now();
    for _ in 0..ITERS {
        for ((key, _), record) in keyed.iter().zip(&cached) {
            black_box(derive_l0(key, black_box(record)).expect("ref"));
        }
    }
    let build_ref = per_resolve_us(started.elapsed(), ITERS);

    let started = Instant::now();
    for _ in 0..ITERS {
        for record in &cached {
            black_box(keys::verify_object_key(black_box(record)).expect("verify"));
        }
    }
    let verify_key = per_resolve_us(started.elapsed(), ITERS);

    let started = Instant::now();
    for _ in 0..ITERS {
        for record in &cached {
            let record = black_box(record);
            let hash: Result<[u8; 32], _> = record.content_hash.clone().try_into();
            black_box(hash.expect("32 bytes"));
            black_box(Uuid::parse_str(&record.writer_id).expect("uuid"));
        }
    }
    let id_parsing = per_resolve_us(started.elapsed(), ITERS);

    let started = Instant::now();
    for _ in 0..ITERS {
        for record in &cached {
            black_box(DeclaredColumnStats::from_validated(&read_commit_record(
                black_box(record),
            )));
        }
    }
    let declared = per_resolve_us(started.elapsed(), ITERS);

    // The String-keyed map traffic a warm resolve performs per L0 record:
    // the bucket's and the window's `out` maps, and `resolve_impl`'s
    // `segments`/`origin_by_key` pair (lookup, insert, insert, remove). A hit
    // is looked up by its listed key, so no resolve-wide map of cached records
    // is built.
    let started = Instant::now();
    for _ in 0..ITERS {
        let mut bucket_out: HashMap<String, SegmentRef> = HashMap::new();
        for segment_ref in &refs {
            bucket_out
                .entry(segment_ref.data_object_key.clone())
                .or_insert_with(|| segment_ref.clone());
        }
        let mut window_out: HashMap<String, SegmentRef> = HashMap::new();
        for (key, segment_ref) in bucket_out {
            window_out.entry(key).or_insert(segment_ref);
        }
        let mut segments: HashMap<String, SegmentRef> = HashMap::new();
        let mut origin_by_key: HashMap<String, SegmentOrigin> = HashMap::new();
        for (key, segment_ref) in window_out {
            if !segments.contains_key(&key) {
                origin_by_key.insert(key.clone(), SegmentOrigin::Recent);
            }
            segments.entry(key).or_insert(segment_ref);
        }
        for segment in segments.values() {
            black_box(origin_by_key.remove(&segment.data_object_key));
        }
    }
    let hashing = per_resolve_us(started.elapsed(), ITERS);

    // What a warm resolve runs for a record whose derivation is stored: one
    // parse, the touch, validation, the memo hit, and the map traffic. The
    // derivation and its parts are listed for comparison; a resolve runs them
    // only for a record with no stored derivation.
    let derived = key_parsing + cache_touch + validation + memo_hit + hashing;
    let rows = [
        ("key parsing (1x partition_bucket_entry)", key_parsing),
        ("cache touch (RecordCache::get_cached)", cache_touch),
        ("validate_expected_fields", validation),
        ("memoised ref (l0_segment_ref, hit)", memo_hit),
        ("map hashing (6 String-keyed ops)", hashing),
    ];
    let reference = [
        ("derive_l0 (first resolve of a record)", build_ref),
        ("  verify_object_key", verify_key),
        ("  content_hash + writer_id parse", id_parsing),
        ("  declared stats read", declared),
    ];
    println!("warm resolve over {RECORDS} cached L0 records, {ITERS} iterations");
    println!("{:<44} {:>10} {:>9}", "phase", "us/resolve", "% derived");
    for (name, us) in rows {
        println!("{name:<44} {us:>10.1} {:>8.1}%", 100.0 * us / derived);
    }
    println!("{:<44} {derived:>10.1}", "derived total");
    println!("{:<44} {full:>10.1}", "full warm resolve (MemoryStore)");
    for (name, us) in reference {
        println!("{name:<44} {us:>10.1}");
    }
    assert_eq!(
        catalog.l0_derivations(),
        RECORDS,
        "the warm resolves above derived nothing"
    );
}
