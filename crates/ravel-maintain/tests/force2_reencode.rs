//! ADR-0066 force 2: re-encoding a below-target compaction part set into a
//! version 2 compaction record (`rewrite::reencode_compaction_parts`).
//!
//! Every bucket here is a real compaction: L0 inputs seeded through the
//! production writers and compacted by `compact_bucket`, so the record's parts
//! are genuine L1 objects. The parts are then made below-target the way the
//! L0 migration tests make an input below-target: the bytes stay at the
//! current version and the record's `segment_format_version` says one less,
//! because only one RSEG, RLOG and RSPAN version is writable today. The record
//! is overwritten in place to do that, which only a fixture may do.
//!
//! Served rows are read the way a resolver picks them: the bucket's compaction
//! records go through the shared selector
//! (`ravel_catalog::select_authoritative_compaction_records`) and the parts of
//! the records it does not exclude are decoded. Interleavings are driven by
//! `FaultStore` hold gates and `FixedClock`s the test moves; nothing sleeps.
//! Each test's doc comment names the line whose removal fails it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use ravel_catalog::select_authoritative_compaction_records;
use ravel_commit::{erasure, keys, record, signal};
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::rewrite::{ReencodeOutcome, reencode_compaction_parts};
use ravel_maintain::{
    Checkpoint, ClaimParticipant, Clock, CompactionOutcome, CompactorConfig, Coordination,
    ErasureRewriteOutcome, FixedClock, MaintainMemo, NoLeases, PendingErasureRequest,
    PublishOutcome, RequestLedger, compact_bucket, erasure_rewrite_bucket, read,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, GateHandle, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{
    CompactionRecord, ErasurePredicateMatcher, ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_segment::{
    ReaderLimits, SeriesEntryV4, ValueKind, decode_catalog_v5, decode_run_pages_soa,
    open_from_full, plan_ranges_v4,
};
use ravel_types::Signal;
use uuid::Uuid;

/// A lease long enough that no run here comes due for a renewal unless the
/// test moves the clock by a third of it.
const LEASE: Duration = Duration::from_secs(3);

/// Past the lease, on every clock a test moves.
const PAST_LEASE_NS: i64 = 4 * 1_000_000_000;

/// The key fragment of every L1 and rewrite part object.
const PART_KEYS: &str = "/l1/";

/// A [`ClaimSleeper`] that returns at once, so no jitter wait touches the real
/// timer.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// The writer switch on, no claims.
fn reencode_cfg() -> CompactorConfig {
    CompactorConfig {
        reencode_writer_enabled: true,
        ..CompactorConfig::default()
    }
}

/// A config for one pass that takes claims as process `process` on `clock`,
/// counting its requests into `ledger`.
fn claiming_cfg(process: u128, clock: &FixedClock, ledger: &RequestLedger) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_lease_duration: LEASE,
        claim_participant: Some(
            ClaimParticipant::new(
                Uuid::from_u128(process),
                Arc::new(clock.clone()) as Arc<dyn Clock>,
            )
            .with_sleeper(Arc::new(NoWait)),
        ),
        request_ledger: Some(ledger.clone()),
        reencode_writer_enabled: true,
        ..CompactorConfig::default()
    }
}

/// The segment format version the current build writes `signal`'s compaction
/// parts at.
fn current_version(signal: Signal) -> u32 {
    match signal {
        Signal::Metrics => ravel_maintain::build::OUTPUT_FORMAT_VERSION,
        Signal::Logs => ravel_maintain::rlog::OUTPUT_FORMAT_VERSION,
        Signal::Spans => ravel_maintain::rspan_codec::OUTPUT_FORMAT_VERSION,
        other => panic!("no compaction parts for {other:?}"),
    }
}

/// Two metrics inputs that both carry the series `keep` and `victim`, so
/// compaction merges each series' runs into one run carrying the per-sample
/// provenance column. An erasure test drops `victim`. The duplicate
/// `(keep, 2000)` timestamp is the case the provenance column exists for.
async fn seed_metrics(store: &dyn ObjectStoreBackend) -> ravel_maintain::Bucket {
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(1_000, 1.0), (2_000, 2.0)]),
                raw_series("victim", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![
                raw_series("keep", &[("k", "a")], &[(1_500, 1.5), (2_000, -0.0)]),
                raw_series("victim", &[("k", "b")], &[(3_000, 3.0)]),
            ],
        ),
    ] {
        seed_input(store, &spec).await;
    }
    bucket()
}

/// Seed `signal`'s two L0 inputs into `store` and compact them.
async fn seed_compacted(store: &dyn ObjectStoreBackend, signal: Signal) -> ravel_maintain::Bucket {
    let b = match signal {
        Signal::Metrics => seed_metrics(store).await,
        Signal::Logs => seed_rlog_two_inputs(store).await,
        Signal::Spans => seed_rspan_two_inputs(store).await,
        other => panic!("no fixture for {other:?}"),
    };
    let outcome = compact_bucket(
        store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &b,
    )
    .await
    .expect("compact");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the fixture compacts: {outcome:?}"
    );
    b
}

/// The bucket's compaction records, decoded, by key.
async fn compaction_records(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
) -> Vec<(String, CompactionRecord)> {
    let listing = read::list_bucket(store, b).await.expect("list");
    let mut out = Vec::new();
    for key in listing.compaction_record_keys {
        let rec = record::decode_compaction(&get_full(store, &key).await).expect("decode");
        keys::verify_compaction_record_key(&rec, &key).expect("record key verifies");
        out.push((key, rec));
    }
    out
}

/// Overwrite the bucket's one compaction record with every part recorded one
/// version below the current one. Returns the record and its key.
async fn stamp_parts_below_target(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
) -> (String, CompactionRecord) {
    let mut records = compaction_records(store, b).await;
    assert_eq!(records.len(), 1, "one compaction record to stamp");
    let (key, mut rec) = records.remove(0);
    for part in &mut rec.parts {
        part.segment_format_version = current_version(b.signal) - 1;
    }
    store
        .put(&key, record::encode_compaction(&rec), PutOptions::default())
        .await
        .expect("overwrite the fixture record");
    (key, rec)
}

/// The records the shared selector serves: every compaction record it does not
/// exclude.
async fn served(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
) -> Vec<(String, CompactionRecord)> {
    let records = compaction_records(store, b).await;
    let selection = select_authoritative_compaction_records(&records).expect("selector");
    let excluded: Vec<String> = selection.excluded().map(str::to_string).collect();
    records
        .into_iter()
        .filter(|(key, _)| !excluded.contains(key))
        .collect()
}

/// The single record the selector serves.
async fn served_one(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
) -> (String, CompactionRecord) {
    let mut served = served(store, b).await;
    assert_eq!(served.len(), 1, "the selector serves one record");
    served.remove(0)
}

/// One served RSEG sample: series id, its run's run-wide provenance
/// `(created_unix_ns, writer_epoch, writer_seq)`, its position in the run, its
/// timestamp, its value's bit pattern, and its per-sample provenance when the
/// run carries the column.
type RsegRow = (
    [u8; 16],
    (i64, u64, u64),
    usize,
    i64,
    u64,
    Option<(i64, u64, u64, u32)>,
);

/// Every sample of every part of `rec`, in the order the parts store them.
async fn rseg_rows(store: &dyn ObjectStoreBackend, rec: &CompactionRecord) -> Vec<RsegRow> {
    let limits = ReaderLimits::default();
    let mut out = Vec::new();
    for part in &rec.parts {
        let obj = get_full(store, &keys::reconstruct_l1_part_key(rec, part).unwrap()).await;
        let loc = open_from_full(&obj, limits).expect("open part");
        let entries = decode_catalog_v5(&loc.footer, &obj, limits).expect("catalog");
        let refs: Vec<&SeriesEntryV4> = entries.iter().collect();
        let mut planned = plan_ranges_v4(&loc.footer, &refs)
            .expect("plan")
            .into_iter();
        for entry in &entries {
            assert_eq!(entry.entry.value_kind, ValueKind::Scalar);
            for (i, run) in entry.runs.iter().enumerate() {
                let range = planned.next().expect("a range per run");
                let slice = |(off, len): (u64, u64)| &obj[off as usize..(off + len) as usize];
                let (mut scratch, mut ts, mut vals) = (Vec::new(), Vec::new(), Vec::new());
                decode_run_pages_soa(
                    &entry.entry.series_id,
                    run,
                    slice(range.ts_range),
                    slice(range.val_range),
                    limits,
                    &mut scratch,
                    &mut ts,
                    &mut vals,
                )
                .expect("decode run");
                let provenance = entry.per_sample_provenance.get(i).cloned().flatten();
                for (j, (t, v)) in ts.into_iter().zip(vals).enumerate() {
                    out.push((
                        entry.entry.series_id.0,
                        (run.created_unix_ns, run.writer_epoch, run.writer_seq),
                        j,
                        t,
                        v.to_bits(),
                        provenance.as_ref().map(|p| {
                            let p = p[j];
                            (
                                p.created_unix_ns,
                                p.writer_epoch,
                                p.writer_seq,
                                p.in_page_index,
                            )
                        }),
                    ));
                }
            }
        }
    }
    out.sort();
    out
}

/// Every record of every part of `rec`, decoded and printed with every field,
/// sorted. Logs and spans records carry no float the printing could merge.
async fn record_rows(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    rec: &CompactionRecord,
) -> Vec<String> {
    let mut out = Vec::new();
    for part in &rec.parts {
        let obj = get_full(store, &keys::reconstruct_l1_part_key(rec, part).unwrap()).await;
        match signal {
            Signal::Logs => {
                let reader =
                    ravel_logseg::RlogReader::new(&obj, &ravel_logseg::RlogConfig::default())
                        .expect("open rlog part");
                let (rows, _) = reader
                    .scan(&ravel_logseg::Predicate::And(vec![]))
                    .expect("scan rlog part");
                out.extend(rows.iter().map(|r| format!("{r:?}")));
            }
            Signal::Spans => {
                let reader =
                    ravel_rspan::RspanReader::new(&obj, &ravel_rspan::RspanConfig::default())
                        .expect("open rspan part");
                let (rows, _) = reader
                    .scan(&ravel_rspan::SpanQuery::ts_range(i64::MIN, i64::MAX))
                    .expect("scan rspan part");
                out.extend(rows.iter().map(|r| format!("{r:?}")));
            }
            other => panic!("no record decoder for {other:?}"),
        }
    }
    out.sort();
    out
}

/// Re-encode `b` on `store` with the switch on and no claims, expecting a
/// published version 2 record over `predecessor_key`.
async fn reencode_published(
    store: &dyn ObjectStoreBackend,
    b: &ravel_maintain::Bucket,
    predecessor_key: &str,
) {
    let outcome = reencode_compaction_parts(
        store,
        &FixedClock::new(sealed_now_ns() + 1_000),
        &reencode_cfg(),
        b,
    )
    .await
    .expect("re-encode");
    match outcome {
        ReencodeOutcome::Reencoded {
            superseded_record_key,
            parts,
            publish,
        } => {
            assert_eq!(superseded_record_key, predecessor_key);
            assert!(parts >= 1);
            assert_eq!(publish, PublishOutcome::Published);
        }
        other => panic!("expected Reencoded, got {other:?}"),
    }
}

/// The served record after a re-encode is a version 2 record whose parts are
/// all at the current version, and which is not the predecessor.
fn assert_current_successor(
    signal: Signal,
    predecessor_key: &str,
    (key, rec): &(String, CompactionRecord),
) {
    assert_ne!(key, predecessor_key, "the selector serves the successor");
    assert_eq!(rec.format_version, 2);
    assert!(!rec.parts.is_empty());
    for part in &rec.parts {
        assert_eq!(
            part.segment_format_version,
            current_version(signal),
            "every new part carries the current version"
        );
    }
}

/// Metrics differential: the rows the selector serves after the re-encode are
/// the predecessor's, sample for sample, with every run's run-wide provenance
/// and every per-sample provenance entry, and the new parts are current.
///
/// The fixture's predecessor carries a provenance column (asserted), so the
/// comparison covers it. Removing the `provenance:` line in
/// `reencode_rseg_part` (so every run is written with `None`) fails the
/// row equality: all six samples lose their provenance entries.
#[tokio::test]
async fn metrics_reencode_serves_the_predecessors_exact_rows() {
    let store = MemoryStore::new();
    let b = seed_compacted(&store, Signal::Metrics).await;
    let (pred_key, pred) = stamp_parts_below_target(&store, &b).await;
    let before = served_one(&store, &b).await;
    assert_eq!(before.0, pred_key);
    let want = rseg_rows(&store, &pred).await;
    assert!(
        want.iter().any(|row| row.5.is_some()),
        "the predecessor carries per-sample provenance, so the test covers it"
    );
    assert_eq!(want.len(), 6, "every seeded sample is served");

    reencode_published(&store, &b, &pred_key).await;

    let after = served_one(&store, &b).await;
    assert_current_successor(Signal::Metrics, &pred_key, &after);
    assert_eq!(after.1.parts.len(), pred.parts.len(), "part for part");
    assert_eq!(rseg_rows(&store, &after.1).await, want);
}

/// Logs differential: the selector serves the predecessor's log records,
/// every field of each, from current-version parts.
///
/// No single line of the primitive decides RLOG record contents: the merge is
/// the compactor's. Replacing the keep-everything closure passed to
/// `rlog::merge_catalogs` with one that drops a record, and the
/// `conserve_exact()` gate with an always-true one, fails the row equality.
#[tokio::test]
async fn logs_reencode_serves_the_predecessors_exact_records() {
    differential_records(Signal::Logs).await;
}

/// Spans differential, as for logs. The same two-line change against the
/// closure passed to `rspan_codec::merge` and the conservation gate fails the
/// row equality.
#[tokio::test]
async fn spans_reencode_serves_the_predecessors_exact_records() {
    differential_records(Signal::Spans).await;
}

async fn differential_records(signal: Signal) {
    let store = MemoryStore::new();
    let b = seed_compacted(&store, signal).await;
    let (pred_key, pred) = stamp_parts_below_target(&store, &b).await;
    let want = record_rows(&store, signal, &pred).await;
    assert_eq!(want.len(), 4, "every seeded record is served");

    reencode_published(&store, &b, &pred_key).await;

    let after = served_one(&store, &b).await;
    assert_current_successor(signal, &pred_key, &after);
    assert_eq!(record_rows(&store, signal, &after.1).await, want);
}

/// The version 2 record copies the predecessor's inputs verbatim, names it in
/// `superseded_record_key`, carries the version 2 hash of both, and is stored
/// at the canonical key for that hash. The predecessor stays in place for the
/// sweep.
///
/// Removing `Some(predecessor_key)` from `publish_superseding_record`'s call
/// (passing `None`) writes a version 1 record with the version 2 hash: the
/// `format_version` and `superseded_record_key` assertions fail.
#[tokio::test]
async fn the_version_2_record_names_its_predecessor_under_its_canonical_key() {
    let store = MemoryStore::new();
    let b = seed_compacted(&store, Signal::Logs).await;
    let (pred_key, pred) = stamp_parts_below_target(&store, &b).await;

    reencode_published(&store, &b, &pred_key).await;

    let records = compaction_records(&store, &b).await;
    assert_eq!(records.len(), 2, "the predecessor and its successor");
    let (key, v2) = records
        .iter()
        .find(|(key, _)| *key != pred_key)
        .expect("a successor");
    assert_eq!(v2.format_version, 2);
    assert_eq!(v2.inputs, pred.inputs, "inputs copied verbatim");
    assert_eq!(v2.superseded_record_key, pred_key);
    assert_eq!(
        v2.input_set_hash,
        erasure::compute_superseding_compaction_input_set_hash(&pred.inputs, &pred_key).to_vec()
    );
    assert_eq!(
        *key,
        keys::compaction_record_key_for(v2).expect("canonical key"),
        "stored at the canonical key of its version 2 hash"
    );
    assert_eq!(v2.level, pred.level);
}

/// A store over `seed`'s bucket that fails every PUT made through it, so a
/// refusal test proves it wrote nothing by the fault counter staying at 0.
/// Seeding goes through the inner store, under the fault layer.
async fn put_refusing_store(signal: Signal) -> (FaultStore<MemoryStore>, ravel_maintain::Bucket) {
    let store = FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(Rule::new(
            Op::Put,
            ScriptedFault::Permanent("this run must not write".to_string()),
        )),
    );
    let b = seed_compacted(store.inner(), signal).await;
    (store, b)
}

fn puts_attempted(store: &FaultStore<MemoryStore>) -> u64 {
    store.fault_count(Op::Put, FaultKind::Permanent)
}

async fn all_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    list_all(store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect()
}

/// With the writer switch off (the default) the primitive refuses with
/// `WriterDisabled` and makes no PUT.
///
/// Removing the `if !config.reencode_writer_enabled` return in
/// `reencode_compaction_parts` lets the run reach its first part PUT, which
/// the store fails, so the `expect` fails and the PUT count is 1.
#[tokio::test]
async fn the_switch_off_refuses_and_writes_nothing() {
    let (store, b) = put_refusing_store(Signal::Metrics).await;
    stamp_parts_below_target(store.inner(), &b).await;
    assert!(!CompactorConfig::default().reencode_writer_enabled);
    let before = all_keys(store.inner()).await;

    let outcome = reencode_compaction_parts(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &b,
    )
    .await
    .expect("a refusal is not an error");

    assert_eq!(outcome, ReencodeOutcome::WriterDisabled);
    assert_eq!(puts_attempted(&store), 0, "no PUT was made");
    assert_eq!(all_keys(store.inner()).await, before);
}

/// A bucket whose overlap component holds two compaction records is refused
/// with `ContestedOverlap` and nothing is written (ADR-0066 force 2 item 4).
/// The second record names one of the first record's two inputs, so the two
/// share a component, and the selector picks one winner between them.
///
/// Removing the `selection.largest_component() > 1` return lets the winner be
/// re-encoded: the run reaches its first part PUT, which the store fails, so
/// the `expect` fails.
#[tokio::test]
async fn a_contested_overlap_component_is_refused_and_writes_nothing() {
    let (store, b) = put_refusing_store(Signal::Metrics).await;
    let (_, first) = stamp_parts_below_target(store.inner(), &b).await;
    let inputs = vec![first.inputs[0].clone()];
    let second = CompactionRecord {
        input_set_hash: erasure::compute_compaction_input_set_hash(&inputs).to_vec(),
        inputs,
        ..first.clone()
    };
    store
        .inner()
        .put(
            &keys::compaction_record_key_for(&second).expect("key"),
            record::encode_compaction(&second),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put the second record");
    let before = all_keys(store.inner()).await;

    let outcome = reencode_compaction_parts(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &reencode_cfg(),
        &b,
    )
    .await
    .expect("a refusal is not an error");

    assert_eq!(
        outcome,
        ReencodeOutcome::ContestedOverlap {
            largest_component: 2
        }
    );
    assert_eq!(puts_attempted(&store), 0, "no PUT was made");
    assert_eq!(all_keys(store.inner()).await, before);
}

/// A bucket holding a rewrite record is refused with `RewritePresent` and
/// nothing is written (ADR-1331).
///
/// Removing the `!listing.rewrite_record_keys.is_empty()` return lets the
/// compaction record be re-encoded: the run reaches its first part PUT, which
/// the store fails, so the `expect` fails.
#[tokio::test]
async fn a_bucket_with_a_rewrite_record_is_refused_and_writes_nothing() {
    let (store, b) = put_refusing_store(Signal::Metrics).await;
    let (pred_key, pred) = stamp_parts_below_target(store.inner(), &b).await;
    let request_id = Uuid::from_u128(0x2093).to_string();
    let rewrite = RewriteRecord {
        format_version: 1,
        tenant_hash: b.tenant_hash.0.to_vec(),
        signal: signal::to_proto(b.signal) as i32,
        shard: b.shard,
        ingest_hour_bucket: b.ingest_hour_bucket,
        inputs: Vec::new(),
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &[],
            Some(&pred_key),
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        parts: pred.parts.clone(),
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: pred.created_unix_ns + 1_000,
        superseded_record_key: pred_key.clone(),
    };
    store
        .inner()
        .put(
            &keys::rewrite_record_key_for(&rewrite).expect("key"),
            erasure::encode_rewrite(&rewrite),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put the rewrite record");
    let before = all_keys(store.inner()).await;

    let outcome = reencode_compaction_parts(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &reencode_cfg(),
        &b,
    )
    .await
    .expect("a refusal is not an error");

    assert_eq!(outcome, ReencodeOutcome::RewritePresent);
    assert_eq!(puts_attempted(&store), 0, "no PUT was made");
    assert_eq!(all_keys(store.inner()).await, before);
}

/// A bucket whose one record is already at the current version is a no-op.
///
/// Removing the `UpToDate` return (the `segment_format_version <
/// target_version` test) re-encodes the current parts anyway: the run reaches
/// its first part PUT, which the store fails, so the `expect` fails.
#[tokio::test]
async fn a_bucket_at_target_is_a_no_op() {
    for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
        let (store, b) = put_refusing_store(signal).await;
        let before = all_keys(store.inner()).await;

        let outcome = reencode_compaction_parts(
            &store,
            &FixedClock::new(sealed_now_ns()),
            &reencode_cfg(),
            &b,
        )
        .await
        .expect("a no-op is not an error");

        assert_eq!(outcome, ReencodeOutcome::UpToDate, "{signal:?}");
        assert_eq!(puts_attempted(&store), 0, "no PUT was made");
        assert_eq!(all_keys(store.inner()).await, before);
    }
}

fn ms(ns: i64) -> u64 {
    u64::try_from(ns / 1_000_000).expect("positive instant")
}

/// A fault store whose own clock (the base claim expiry is judged on) reads
/// `now_ns`, holding a compacted metrics bucket with below-target parts.
async fn fenced_store(now_ns: i64) -> (Arc<FaultStore<MemoryStore>>, String) {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    store.inner().set_clock_ms(ms(now_ns));
    let b = seed_compacted(store.inner(), Signal::Metrics).await;
    let (pred_key, _) = stamp_parts_below_target(store.inner(), &b).await;
    (store, pred_key)
}

/// A windowless erasure request for every series named `victim`.
fn pending() -> Vec<PendingErasureRequest> {
    let request_id = Uuid::from_u128(0x2093);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 0,
        predicate: vec![ErasurePredicateMatcher {
            key: "__name__".to_string(),
            value: "victim".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request,
    }]
}

async fn erasure(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
) -> ErasureRewriteOutcome {
    let mut memo = MaintainMemo::with_default_interval();
    erasure_rewrite_bucket(
        store,
        clock,
        config,
        &NoLeases,
        &bucket(),
        &pending(),
        &mut memo,
    )
    .await
    .expect("erasure rewrite")
}

/// Wait until `gate` parks exactly one call, check it is a PUT on a part key,
/// and return its id.
async fn parked_part_put(gate: &GateHandle) -> u64 {
    gate.wait_until_held(1).await;
    let held = gate.held_details();
    assert_eq!(held.len(), 1, "the gate parked exactly one call: {held:?}");
    let (id, op, key) = &held[0];
    assert_eq!(*op, Op::Put, "the parked call is a PUT: {key}");
    assert!(key.contains(PART_KEYS), "the parked call is a part: {key}");
    *id
}

/// `(compaction records, rewrite records)` in the bucket.
async fn record_sets(store: &dyn ObjectStoreBackend) -> (usize, usize) {
    let listing = read::list_bucket(store, &bucket()).await.expect("list");
    (
        listing.compaction_record_keys.len(),
        listing.rewrite_record_keys.len(),
    )
}

fn is_published(outcome: &ErasureRewriteOutcome) -> bool {
    matches!(
        outcome,
        ErasureRewriteOutcome::Rewritten {
            publish: PublishOutcome::Published,
            ..
        }
    )
}

/// The claim fence: re-encode R claims the bucket and is parked at its first
/// part PUT. The lease expires; erasure E, a second process, steals the claim
/// and publishes its rewrite record. R resumes, its part-boundary checkpoint
/// renews, the renewal finds the claim stolen, and R cancels there with
/// nothing published.
///
/// Removing the `claim_bucket(store, config, bucket, "reencode")` call (so R
/// runs unclaimed) lets E take the claim without a steal and R's checkpoints
/// pass; R is then stopped by its re-list instead, `RecordSetChanged`, which
/// fails the `Cancelled` assertion. The re-list test below pins the re-list.
#[tokio::test]
async fn an_erasure_rewrite_that_steals_the_claim_cancels_the_reencode() {
    let now_ns = sealed_now_ns();
    let (store, _) = fenced_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let r_ledger = RequestLedger::new();
    let r_config = claiming_cfg(1, &clock, &r_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = claiming_cfg(2, &clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let r = async {
        reencode_compaction_parts(store.as_ref(), &clock, &r_config, &bucket())
            .await
            .expect("re-encode")
    };
    let e = async {
        let id = parked_part_put(&gate).await;
        clock.set(now_ns + PAST_LEASE_NS);
        store.inner().set_clock_ms(ms(now_ns + PAST_LEASE_NS));
        let outcome = erasure(store.as_ref(), &clock, &e_config).await;
        assert!(gate.release(id), "the parked part PUT was released");
        outcome
    };
    let (r_outcome, e_outcome) = tokio::join!(r, e);

    assert!(
        is_published(&e_outcome),
        "E steals the expired claim and publishes: {e_outcome:?}"
    );
    assert_eq!(
        r_outcome,
        ReencodeOutcome::Cancelled {
            at: Checkpoint::PartBoundary
        }
    );
    assert_eq!(
        record_sets(store.as_ref()).await,
        (1, 1),
        "no version 2 record beside the predecessor and E's rewrite record"
    );
    assert_eq!(r_ledger.report().publish.requests, 0, "R PUT no record");
    assert_eq!(
        r_ledger.report().list.requests,
        1,
        "R stopped at its checkpoint, before its re-list"
    );
}

/// The re-list fence, with no claims anywhere: re-encode R is parked at its
/// first part PUT while erasure E publishes its rewrite record. R resumes,
/// builds, and its pre-publish re-list finds the rewrite record, so it
/// publishes nothing.
///
/// Removing the `relist_changed` check in `reencode_and_publish` lets R
/// publish a version 2 record beside E's rewrite record (`Reencoded`), which
/// fails the `RecordSetChanged` assertion and the record count.
#[tokio::test]
async fn an_erasure_rewrite_that_publishes_mid_reencode_stops_it_at_the_relist() {
    let now_ns = sealed_now_ns();
    let (store, _) = fenced_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let r_ledger = RequestLedger::new();
    let r_config = CompactorConfig {
        request_ledger: Some(r_ledger.clone()),
        ..reencode_cfg()
    };
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let r = async {
        reencode_compaction_parts(store.as_ref(), &clock, &r_config, &bucket())
            .await
            .expect("re-encode")
    };
    let e = async {
        let id = parked_part_put(&gate).await;
        let outcome = erasure(store.as_ref(), &clock, &CompactorConfig::default()).await;
        assert!(gate.release(id), "the parked part PUT was released");
        outcome
    };
    let (r_outcome, e_outcome) = tokio::join!(r, e);

    assert!(is_published(&e_outcome), "E publishes: {e_outcome:?}");
    assert_eq!(r_outcome, ReencodeOutcome::RecordSetChanged);
    assert_eq!(record_sets(store.as_ref()).await, (1, 1));
    assert_eq!(r_ledger.report().publish.requests, 0, "R PUT no record");
    assert_eq!(
        r_ledger.report().list.requests,
        2,
        "the plan and the re-list"
    );
}
