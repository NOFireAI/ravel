//! `IngestConfig::segment_format_version`: which RSEG writer a shard calls,
//! and the invariant that the commit record's `segment_format_version`
//! field always agrees with the object's own trailer version because both
//! are stamped from one resolved value in `ShardActor::flush_tenant`
//! (docs/rseg-v2-plan.md P6). Also covers the first real (non-unit-test)
//! v2 object produced through the actual shard actor / router, and a
//! catalog snapshot mixing v1 and v2 objects for the same series, queried
//! through the real `ravel-query` engine end to end.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{
    TestClock, catalog, commit_and_trailer_versions, engine, make_point, publish_segment,
    series_input, tenant,
};
use ravel_ingest::{IngestConfig, IngestRouter, SEGMENT_FORMAT_V1, SEGMENT_FORMAT_V2, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::{CommitToken, Signal, TenantId};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Fast-flushing config shared by the single-flush tests below. Does NOT
/// set `segment_format_version`, so callers that also don't override it
/// exercise the real shipped default.
fn fast_flush_config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(50),
        ..IngestConfig::default()
    }
}

/// Drives one point through the real `IngestRouter` -> `ShardActor` path
/// under `config`, strict-acks it, and returns the store, the ack's
/// `CommitToken`, and the tenant used.
async fn ingest_one_point_and_flush(
    config: IngestConfig,
) -> (Arc<dyn ObjectStoreBackend>, CommitToken, TenantId) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tid = tenant("acme");
    let points = vec![make_point(&tid, "cpu_usage", &[("host", "a")], 1_000, 1.0)];

    let (write_result, ()) = tokio::join!(
        router.write(
            tid.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        router.flush_all(),
    );
    let receipt = write_result.expect("strict write succeeds");
    assert_eq!(receipt.tokens.len(), 1);
    router.shutdown().await;
    (store, receipt.tokens[0].clone(), tid)
}

#[tokio::test]
async fn default_config_produces_v1_commit_record_and_trailer() {
    // `fast_flush_config()` deliberately leaves `segment_format_version`
    // untouched, so this proves the *shipped default*, not a hand-set v1.
    let (store, token, tid) = ingest_one_point_and_flush(fast_flush_config()).await;
    let (record_version, trailer_version) =
        commit_and_trailer_versions(store.as_ref(), &tid.hash(), Signal::Metrics, &token).await;
    assert_eq!(record_version, u32::from(SEGMENT_FORMAT_V1));
    assert_eq!(trailer_version, SEGMENT_FORMAT_V1);
}

#[tokio::test]
async fn explicit_v2_config_produces_v2_commit_record_and_trailer() {
    let config = IngestConfig {
        segment_format_version: SEGMENT_FORMAT_V2,
        ..fast_flush_config()
    };
    let (store, token, tid) = ingest_one_point_and_flush(config).await;
    let (record_version, trailer_version) =
        commit_and_trailer_versions(store.as_ref(), &tid.hash(), Signal::Metrics, &token).await;
    assert_eq!(record_version, u32::from(SEGMENT_FORMAT_V2));
    assert_eq!(trailer_version, SEGMENT_FORMAT_V2);
}

/// An unrecognized `segment_format_version` must fall back to v1 in both
/// the writer call and the stamp together, never one without the other:
/// exactly the failure mode the single-resolved-value structure in
/// `flush_tenant` rules out.
#[tokio::test]
async fn unrecognized_config_value_falls_back_to_v1_consistently() {
    let config = IngestConfig {
        segment_format_version: 7,
        ..fast_flush_config()
    };
    let (store, token, tid) = ingest_one_point_and_flush(config).await;
    let (record_version, trailer_version) =
        commit_and_trailer_versions(store.as_ref(), &tid.hash(), Signal::Metrics, &token).await;
    assert_eq!(record_version, u32::from(SEGMENT_FORMAT_V1));
    assert_eq!(trailer_version, SEGMENT_FORMAT_V1);
}

/// The scenario that matters most: the same series has samples split
/// across one v1 object and one v2 object, both present in one catalog
/// snapshot, queried through the real `ravel-query` engine (catalog
/// resolve -> segment fetch, which self-detects v1 vs v2 per object from
/// its own trailer -> cross-segment k-way merge/dedup -> PromQL range
/// eval). This is the first test exercising v1 and v2 objects *interleaved*
/// in the merge path, not merely decoded in isolation (that was P3's
/// scope).
///
/// Both segments share one `writer_id`/`writer_epoch`/`created_unix_ns` and
/// differ only in `writer_seq`, mirroring
/// `ravel-query`'s own
/// `cross_segment_duplicate_sample_resolves_to_greatest_writer_seq`
/// (crates/ravel-query/tests/e2e.rs): the greatest `writer_seq` must win
/// the duplicate timestamp regardless of which segment (v1 or v2) carries
/// it, proving the total-order tiebreak treats the two RSEG versions
/// uniformly rather than silently favoring one.
///
/// The value bit-pattern tiebreak (the final rung of the total order) is
/// deliberately not exercised here: it only fires when
/// `created_unix_ns`/`writer_epoch`/`writer_seq` all tie across two
/// objects, and `ravel_commit::publish` rejects a second commit record at
/// the same (shard, writer_id, epoch, seq) as split-brain, so two distinct
/// published objects can never reach it.
async fn run_mixed_population_case(v1_writer_seq: u64, v2_writer_seq: u64, expected_t1: f64) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let writer_id = Uuid::new_v4();
    let hour_bucket = u32::try_from(BASE_NS / (3_600 * NS_PER_SEC)).expect("hour bucket");

    let t0 = BASE_NS;
    let t1 = BASE_NS + NS_PER_MIN;
    let t2 = BASE_NS + 2 * NS_PER_MIN;

    // v1 object: samples at t0 (unique) and t1 (contested).
    let v1_series = vec![series_input(
        &tid,
        "mixed_metric",
        &[],
        &[(t0, 10.0), (t1, 15.0)],
    )];
    publish_segment(
        store.as_ref(),
        SEGMENT_FORMAT_V1,
        tid.hash(),
        0,
        writer_id,
        1,
        v1_writer_seq,
        hour_bucket,
        BASE_NS,
        v1_series,
    )
    .await;

    // v2 object: samples at t1 (contested) and t2 (unique).
    let v2_series = vec![series_input(
        &tid,
        "mixed_metric",
        &[],
        &[(t1, 25.0), (t2, 30.0)],
    )];
    publish_segment(
        store.as_ref(),
        SEGMENT_FORMAT_V2,
        tid.hash(),
        0,
        writer_id,
        1,
        v2_writer_seq,
        hour_bucket,
        BASE_NS,
        v2_series,
    )
    .await;

    let cat = catalog(Arc::clone(&store), 1);
    let eng = engine(store, cat);
    let now_ns = t2 + NS_PER_MIN;
    let matrix = eng
        .range(
            tid.hash(),
            "mixed_metric",
            t0 / 1_000_000,
            t2 / 1_000_000,
            NS_PER_MIN / 1_000_000,
            &[],
            now_ns,
            Duration::from_secs(5),
        )
        .await
        .expect("range query");

    assert_eq!(matrix.len(), 1, "exactly the one merged series, no more");
    let (_labels, samples) = &matrix[0];
    let observed: Vec<(i64, f64)> = samples.iter().map(|s| (s.ts_ns, s.value)).collect();
    assert_eq!(
        observed,
        vec![(t0, 10.0), (t1, expected_t1), (t2, 30.0)],
        "right samples, right order: t0 from the v1-only sample, t1 resolved \
         by the total-order tiebreak, t2 from the v2-only sample"
    );
}

#[tokio::test]
async fn mixed_v1_v2_population_v2_wins_duplicate_by_greater_writer_seq() {
    run_mixed_population_case(1, 2, 25.0).await;
}

#[tokio::test]
async fn mixed_v1_v2_population_v1_wins_duplicate_by_greater_writer_seq() {
    // Mirror of the case above with the greater `writer_seq` on the v1
    // object instead: if a merge bug systematically preferred v2 objects
    // (e.g. because it happened to iterate the v2 fetch path last), this
    // is the case that would catch it. The other case alone cannot: it
    // stays green under "always prefer v2" too.
    run_mixed_population_case(2, 1, 15.0).await;
}
