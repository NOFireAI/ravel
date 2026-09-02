//! The compaction read ledger (ADR-0996 task 996-8): per-run store requests and
//! wire bytes, split by the phase that issued them.
//!
//! The headline test uses an [`InstrumentedStore`] as an ORACLE. It is the only
//! independent account of what a run actually spent, and it knows nothing about
//! phases, so two properties are checkable at once: each phase's count is what
//! that phase's call sites issued (pinned as exact integers for this fixture),
//! and the sum over phases equals the wrapper's grand total exactly -- no
//! request uncounted, none counted twice.
//!
//! Every request count asserted here is exact; ranged-read byte figures use
//! per-object proportional bands (backed by the oracle's exact grand-total
//! reconciliation), never flat floors. A suite that only checked `> 0` would
//! pass on a ledger that double-counts, which is the failure this seam
//! exists to make impossible.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::request_ledger::RunRequestReport;
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, FixedClock, MaintainError, PublishOutcome, RequestLedger,
    RlogCodec, compact_bucket, conserve_exact, read, rewrite_and_publish,
};
use ravel_object_store::instrument::{InstrumentedStore, StoreMetricsSnapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use uuid::Uuid;

/// A config with a ledger installed and nothing else changed from the shipped
/// defaults, so the figures pinned below are the default geometry's.
fn config_with(ledger: &RequestLedger) -> CompactorConfig {
    CompactorConfig {
        request_ledger: Some(ledger.clone()),
        ..CompactorConfig::default()
    }
}

/// Every store call the wrapper saw, as one figure: GET + PUT + HEAD + LIST.
/// This is what the ledger's `total_requests()` must equal.
fn oracle_requests(before: &StoreMetricsSnapshot, after: &StoreMetricsSnapshot) -> u64 {
    (after.get.calls - before.get.calls)
        + (after.put.calls - before.put.calls)
        + (after.head.calls - before.head.calls)
        + (after.list_calls() - before.list_calls())
}

/// Assert the ledger's totals reconcile against the wrapper's, per byte kind.
/// `wire_bytes_received` is checked against the wrapper's GET bytes (the only
/// op whose returned payload it counts) and `wire_bytes_sent` against its PUT
/// bytes; the two are never added together.
fn assert_reconciles(
    report: &RunRequestReport,
    before: &StoreMetricsSnapshot,
    after: &StoreMetricsSnapshot,
) {
    assert_eq!(
        report.total_requests(),
        oracle_requests(before, after),
        "every store call must be attributed to exactly one phase"
    );
    assert_eq!(
        report.total_wire_bytes_received(),
        after.get.bytes - before.get.bytes,
        "received bytes must equal the wrapper's GET payload total"
    );
    assert_eq!(
        report.total_wire_bytes_sent(),
        after.put.bytes - before.put.bytes,
        "sent bytes must equal the wrapper's PUT payload total"
    );
}

/// The byte size of every object under `prefix`, summed.
async fn bytes_under(store: &dyn ObjectStoreBackend, prefix: &str) -> u64 {
    list_all(store, prefix)
        .await
        .expect("list")
        .iter()
        .map(|m| m.size)
        .sum()
}

/// Headline: a real RLOG compaction over the two-input logs fixture, with the
/// instrumented wrapper as the oracle. Each phase's request count is pinned
/// exactly and the phases reconcile to the wrapper's grand total.
///
/// Where each expected integer comes from (two L0 `.rlog` inputs, one part):
///
/// - `list` 1: one listing page over the bucket's commit prefix (2 keys, well
///   under `MemoryStore`'s default page size).
/// - `record_read` 2: one whole-object GET per input commit record.
/// - `catalog_read` 10: per input, the footer suffix probe plus the four
///   whole-read directory sections the RLOG merge needs (STREAM_DIR, FIELD_DIR,
///   SKIP_IDX, PAGE_DIR). No tail range (a 64 KiB probe covers these small
///   objects' footers) and no POSTINGS GET (the fixture indexes no fields, so
///   the section is absent).
/// - `block_read` 4: one ranged block GET per (stream, input) cursor. The
///   fixture's streams are stream 0 in both inputs, stream 1 in the first and
///   stream 2 in the second, i.e. four cursors, each over a single block.
/// - `part_put` 1: the single L1 part's `CreateIfAbsent` PUT.
/// - `publish` 1: the compaction record's PUT. No post-publish HEAD: no part
///   answered `AlreadyExists` on a first run.
///
/// Flip proofs, each run against this test:
///
/// - dropping the `note_get` at `rlog.rs`'s `fetch_block`: `block_read` reads
///   0 against the expected 4.
/// - attributing the commit-record GET to `CatalogRead` instead of
///   `RecordRead` in `read.rs`'s `load_one_input`: `record_read` reads 0
///   against the expected 2, so the pinned split, not just the total, is what
///   holds attribution in place.
/// - making `rewrite_and_publish` reset unconditionally instead of
///   `reset_for_run_unless_open`: `list` reads 0 against the expected 1,
///   because the rewrite would wipe the LIST its driver had already counted.
///
/// The reconciliation assertion is proved non-vacuous separately, in
/// `the_rseg_and_rspan_paths_report_under_the_same_phases`.
#[tokio::test]
async fn per_phase_counts_match_the_instrumented_oracle_exactly() {
    let store = InstrumentedStore::new(MemoryStore::new());
    let bucket = seed_rlog_two_inputs(&store).await;
    let metrics = store.metrics();
    let before = metrics.snapshot();

    let ledger = RequestLedger::new();
    let clock = FixedClock::new(sealed_now_ns());
    let outcome = compact_bucket(&store, &clock, &config_with(&ledger), &bucket)
        .await
        .expect("compact");
    assert_eq!(
        outcome,
        CompactionOutcome::Compacted {
            parts: 1,
            publish: PublishOutcome::Published,
        }
    );
    let after = metrics.snapshot();
    let report = ledger.report();

    assert_eq!(report.list.requests, 1, "one listing page");
    assert_eq!(report.record_read.requests, 2, "one GET per commit record");
    assert_eq!(
        report.catalog_read.requests, 10,
        "probe + STREAM_DIR + FIELD_DIR + SKIP_IDX + PAGE_DIR, per input"
    );
    assert_eq!(
        report.block_read.requests, 4,
        "one block GET per (stream, input) cursor"
    );
    assert_eq!(report.part_put.requests, 1, "one L1 part PUT");
    assert_eq!(report.publish.requests, 1, "the compaction record PUT");
    assert_eq!(report.total_requests(), 19);

    assert_reconciles(&report, &before, &after);

    // LIST reports requests only: its response body is not visible at the store
    // seam, so neither byte figure may move for that phase.
    assert_eq!(report.list.wire_bytes_received, 0);
    assert_eq!(report.list.wire_bytes_sent, 0);
    // Read phases move no bytes upward and write phases move none downward, so
    // the two kinds stay separable per phase.
    assert_eq!(report.record_read.wire_bytes_sent, 0);
    assert_eq!(report.catalog_read.wire_bytes_sent, 0);
    assert_eq!(report.block_read.wire_bytes_sent, 0);
    assert_eq!(report.part_put.wire_bytes_received, 0);
}

/// Wire bytes are proportional to the fixture's ACTUAL object sizes, per
/// object, not to a flat floor:
///
/// - `record_read` received bytes equal the summed size of the two commit
///   record objects, exactly (each is read whole, once).
/// - `part_put` sent bytes equal the L1 part object's own size, exactly.
/// - `publish` sent bytes equal the compaction record object's own size,
///   exactly.
/// - the ranged read phases are banded against the inputs' OWN sizes, per
///   object: the catalog phase must cover at least every input's footer probe
///   (`min(footer_probe_bytes, that object's size)`, which on this fixture's
///   small objects is the whole object) and at most one further object's worth
///   of directory sections; the block phase must be positive and cannot exceed
///   the inputs' bytes, since every block is fetched by range exactly once.
///
/// Flip proof (run): charging `read.rs`'s `load_one_input` a constant 1 byte
/// instead of the response payload length reports 2 received bytes against the
/// fixture's real 582, so the byte figures track the objects rather than the
/// request count.
#[tokio::test]
async fn wire_bytes_track_the_fixtures_real_object_sizes() {
    let store = MemoryStore::new();
    let bucket = seed_rlog_two_inputs(&store).await;

    let commit_prefix = ravel_commit::keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .expect("prefix");
    let commit_record_bytes = bytes_under(&store, &commit_prefix).await;
    // The inputs' own object sizes, read per object from their commit records
    // rather than from a flat assumption about the fixture.
    let listing = read::list_bucket(&store, &bucket).await.expect("list");
    let input_object_bytes: u64 = read::load_inputs(&store, &bucket, &listing.commit_keys, 1)
        .await
        .expect("inputs")
        .iter()
        .map(|i| i.record.object_size)
        .sum();

    let ledger = RequestLedger::new();
    let clock = FixedClock::new(sealed_now_ns());
    compact_bucket(&store, &clock, &config_with(&ledger), &bucket)
        .await
        .expect("compact");
    let report = ledger.report();

    assert_eq!(
        report.record_read.wire_bytes_received, commit_record_bytes,
        "each input commit record is read whole, exactly once"
    );

    let record = fetch_compaction_record(&store, &bucket).await;
    let part_bytes: u64 = record.parts.iter().map(|p| p.object_size).sum();
    assert_eq!(
        report.part_put.wire_bytes_sent, part_bytes,
        "the part PUT offered exactly the part object's bytes"
    );

    let record_key =
        ravel_commit::keys::compaction_record_key_for(&record).expect("compaction record key");
    let published = store
        .get(&record_key, GetRange::Full)
        .await
        .expect("get record")
        .data
        .len() as u64;
    assert_eq!(
        report.publish.wire_bytes_sent, published,
        "the record PUT offered exactly the record object's bytes"
    );

    // Per-object band, not a flat floor: each input's probe window is capped by
    // that object's own size.
    let probe_floor: u64 = read::load_inputs(&store, &bucket, &listing.commit_keys, 1)
        .await
        .expect("inputs")
        .iter()
        .map(|i| {
            i.record
                .object_size
                .min(CompactorConfig::default().footer_probe_bytes)
        })
        .sum();
    let catalog = report.catalog_read.wire_bytes_received;
    assert!(
        catalog >= probe_floor,
        "the catalog phase covers every input's footer probe: {catalog} < {probe_floor}"
    );
    assert!(
        catalog <= probe_floor + input_object_bytes,
        "the directory sections add at most one further object's worth: \
         {catalog} > {probe_floor} + {input_object_bytes}"
    );
    let blocks = report.block_read.wire_bytes_received;
    assert!(blocks > 0, "the merge read its inputs' blocks");
    assert!(
        blocks <= input_object_bytes,
        "every block is fetched by range exactly once: {blocks} > {input_object_bytes}"
    );
}

/// One LIST request per listing PAGE, not per listing. Driving the same
/// compaction over a store paginating at one key per page turns the fixture's
/// single `list` request into three (a page per commit record, then the
/// terminating empty page), and every other phase is unchanged.
///
/// Flip proof (run): moving the `note_metadata` in `read.rs`'s
/// `list_all_counted` out of the loop to after it reports 1 here against the
/// expected 3.
#[tokio::test]
async fn list_counts_every_page_of_a_paginated_drain() {
    let store = InstrumentedStore::new(MemoryStore::with_page_size(1));
    let bucket = seed_rlog_two_inputs(&store).await;
    let metrics = store.metrics();
    let before = metrics.snapshot();

    let ledger = RequestLedger::new();
    let clock = FixedClock::new(sealed_now_ns());
    compact_bucket(&store, &clock, &config_with(&ledger), &bucket)
        .await
        .expect("compact");
    let after = metrics.snapshot();
    let report = ledger.report();

    assert_eq!(
        report.list.requests, 3,
        "two single-key pages plus the terminating page"
    );
    assert_eq!(report.record_read.requests, 2);
    assert_eq!(report.catalog_read.requests, 10);
    assert_eq!(report.block_read.requests, 4);
    assert_reconciles(&report, &before, &after);
}

/// A converging rerun still counts its part PUT: the `CreateIfAbsent` answered
/// `AlreadyExists`, but the request was issued and the body was sent, so it is
/// an attempt like any other. The rerun's publish phase is the record PUT that
/// lost, the winner-record GET, and one HEAD per referenced part.
///
/// Flip proof (run): moving `put_part_with_ledger`'s `note_put` onto the `Ok`
/// arm of the outcome match reports `part_put.requests == 0` here against the
/// expected 1.
#[tokio::test]
async fn an_already_exists_part_put_still_counts_as_an_attempt() {
    let store = InstrumentedStore::new(MemoryStore::new());
    let bucket = seed_rlog_two_inputs(&store).await;
    let listing = read::list_bucket(&store, &bucket).await.expect("list");
    let clock = FixedClock::new(sealed_now_ns());
    let ledger = RequestLedger::new();
    let config = config_with(&ledger);

    let first = rewrite_and_publish::<RlogCodec>(
        &store,
        &clock,
        &config,
        &bucket,
        &listing.commit_keys,
        conserve_exact(),
        sealed_now_ns(),
    )
    .await
    .expect("first run");
    assert_eq!(first.publish, PublishOutcome::Published);
    assert_eq!(first.parts, 1);
    assert_eq!(
        ledger.report().part_put.requests,
        1,
        "the first run created the part"
    );

    // Re-run from scratch, as a crashed run would: identical content-addressed
    // parts, so every part PUT answers AlreadyExists and the record PUT loses.
    let metrics = store.metrics();
    let before = metrics.snapshot();
    let second = rewrite_and_publish::<RlogCodec>(
        &store,
        &clock,
        &config,
        &bucket,
        &listing.commit_keys,
        conserve_exact(),
        sealed_now_ns(),
    )
    .await
    .expect("second run");
    assert_eq!(
        second.publish,
        PublishOutcome::Converged { parts_repaired: 0 }
    );
    let after = metrics.snapshot();
    let report = ledger.report();

    assert_eq!(
        report.part_put.requests, 1,
        "the AlreadyExists part PUT is still an attempt"
    );
    assert!(
        report.part_put.wire_bytes_sent > 0,
        "the rejected PUT still sent the part's body"
    );
    assert_eq!(
        report.publish.requests, 3,
        "the losing record PUT, the winner-record GET, and one part HEAD"
    );
    assert_eq!(
        report.list.requests, 0,
        "this run was driven straight into the rewrite; it listed nothing"
    );
    assert_reconciles(&report, &before, &after);
}

/// A publish that aborts on the conservation gate reports the phases that ran
/// and ZERO publish-phase requests: the gate fires before the record PUT, so no
/// publish request was ever issued, and the ledger must not imply one was.
///
/// Flip proof (run): accounting the record PUT before the conservation gate
/// rather than at the call reports `publish.requests == 1` here against the
/// expected 0.
#[tokio::test]
async fn a_conservation_abort_reports_zero_publish_requests() {
    let store = InstrumentedStore::new(MemoryStore::new());
    let bucket = seed_rlog_two_inputs(&store).await;
    let listing = read::list_bucket(&store, &bucket).await.expect("list");
    let metrics = store.metrics();
    let before = metrics.snapshot();

    let ledger = RequestLedger::new();
    let clock = FixedClock::new(sealed_now_ns());
    let err = rewrite_and_publish::<RlogCodec>(
        &store,
        &clock,
        &config_with(&ledger),
        &bucket,
        &listing.commit_keys,
        // Demand one dropped record where the merge drops none.
        |input: u64, output: u64| input == output + 1,
        sealed_now_ns(),
    )
    .await
    .expect_err("a rejected count must abort");
    assert!(
        matches!(err, MaintainError::ConservationViolation { .. }),
        "expected ConservationViolation, got {err:?}"
    );
    let after = metrics.snapshot();
    let report = ledger.report();

    assert_eq!(
        report.publish.requests, 0,
        "the gate fires before the record PUT; nothing was published"
    );
    assert_eq!(report.publish.wire_bytes_sent, 0);
    assert_eq!(report.publish.wire_bytes_received, 0);
    // The phases that did run are reported in full, including the part PUTs the
    // aborted run already spent.
    assert_eq!(report.record_read.requests, 2);
    assert_eq!(report.catalog_read.requests, 10);
    assert_eq!(report.block_read.requests, 4);
    assert_eq!(report.part_put.requests, 1);
    assert_reconciles(&report, &before, &after);
}

/// Run scoping: a second run reports only its own figures. The rerun here is
/// gated `AlreadyCompacted` after its LIST, so its report is exactly one
/// listing request and nothing else -- none of the first run's 19 requests
/// survive into it.
///
/// Flip proofs, both run against this test:
///
/// - making `RequestLedger::reset_for_run` clear nothing: the second run's
///   `list` reads 2 against the expected 1, the accumulation this scoping
///   exists to prevent.
/// - removing `compact_bucket`'s `reset_for_run` entirely: the FIRST run's
///   `list` reads 0 against the expected 1, because `rewrite_and_publish`
///   then finds no open scope and resets over the LIST its driver counted.
#[tokio::test]
async fn a_second_run_reports_only_its_own_figures() {
    let store = MemoryStore::new();
    let bucket = seed_rlog_two_inputs(&store).await;
    let ledger = RequestLedger::new();
    let config = config_with(&ledger);
    let clock = FixedClock::new(sealed_now_ns());

    compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("first compaction");
    let first = ledger.report();
    assert_eq!(
        first.list.requests, 1,
        "the driver's LIST survives the rewrite"
    );
    assert_eq!(first.total_requests(), 19);

    let outcome = compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("second compaction");
    assert_eq!(outcome, CompactionOutcome::AlreadyCompacted);
    let second = ledger.report();

    assert_eq!(second.list.requests, 1, "its own listing page");
    assert_eq!(
        second.total_requests(),
        1,
        "an already-compacted bucket spends one LIST and nothing else"
    );
    assert_eq!(second.record_read, Default::default());
    assert_eq!(second.catalog_read, Default::default());
    assert_eq!(second.block_read, Default::default());
    assert_eq!(second.part_put, Default::default());
    assert_eq!(second.publish, Default::default());
}

/// The RSEG (metrics) and RSPAN (spans) paths report under the same phases: a
/// compaction of either signal attributes every request it makes and reconciles
/// against the oracle, so the ledger is not RLOG-only.
///
/// This is also where the reconciliation assertion is shown non-vacuous. Flip
/// proof (run): dropping the `note_get` on RSPAN's SKIP_IDX section fetch
/// leaves every per-phase assertion here passing (the footer probe still puts
/// `catalog_read` above zero) and fails only the reconciliation, 9 attributed
/// against the wrapper's 11 -- exactly the "a request went uncounted" case a
/// per-phase floor cannot see.
#[tokio::test]
async fn the_rseg_and_rspan_paths_report_under_the_same_phases() {
    for signal in ["metrics", "spans"] {
        let store = InstrumentedStore::new(MemoryStore::new());
        let bucket = if signal == "metrics" {
            seed_input(
                &store,
                &InputSpec::new(
                    Uuid::from_u128(1),
                    10,
                    1,
                    vec![raw_series("m", &[], &[(10, 1.0)])],
                ),
            )
            .await;
            seed_input(
                &store,
                &InputSpec::new(
                    Uuid::from_u128(2),
                    10,
                    2,
                    vec![raw_series("m", &[], &[(20, 2.0)])],
                ),
            )
            .await;
            common::bucket()
        } else {
            seed_rspan_two_inputs(&store).await
        };
        let metrics = store.metrics();
        let before = metrics.snapshot();

        let ledger = RequestLedger::new();
        let clock = FixedClock::new(sealed_now_ns());
        let outcome = compact_bucket(&store, &clock, &config_with(&ledger), &bucket)
            .await
            .expect("compact");
        assert!(
            matches!(outcome, CompactionOutcome::Compacted { .. }),
            "{signal} fixture must compact, got {outcome:?}"
        );
        let after = metrics.snapshot();
        let report = ledger.report();

        assert_eq!(report.list.requests, 1, "{signal}: one listing page");
        assert_eq!(
            report.record_read.requests, 2,
            "{signal}: two commit records"
        );
        assert!(
            report.catalog_read.requests > 0,
            "{signal}: catalog reads attributed"
        );
        assert!(
            report.block_read.requests > 0,
            "{signal}: page/block reads attributed"
        );
        assert!(
            report.part_put.requests > 0,
            "{signal}: part PUTs attributed"
        );
        assert_eq!(report.publish.requests, 1, "{signal}: the record PUT");
        assert_reconciles(&report, &before, &after);
    }
}
