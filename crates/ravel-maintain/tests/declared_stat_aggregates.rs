//! ADR-4628 task T4 (issue #4671): the compaction recompute's sum and zero
//! count reach every logs output the one per-part accumulator writes, and the
//! `emit_declared_stat_aggregates` opt-out withholds them on each of those
//! paths.
//!
//! Compaction (`compact_bucket`), the migrate primitive
//! (`migrate_bucket_format`) and the force 2 re-encode
//! (`reencode_compaction_parts`) all build their logs parts through
//! `DeclaredStatAccum` in `rlog.rs`. Each test here writes the same four rows
//! and compares the output stamp with the figures computed by hand from them,
//! which a fresh compaction of the same rows also produces (asserted).
//!
//! Rows, on one stream with no stream-level declared values:
//!   ts 10  status 0         cache false
//!   ts 20  status i64::MAX  cache --
//!   ts 30  status i64::MAX  cache true
//!   ts 40  status -4        cache false
//! status: min -4, max i64::MAX, null 0, sum 2 * i64::MAX - 4, zero count 1.
//! cache: min false, max true, null 1, no sum, zero count 2.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_catalog::select_authoritative_compaction_records;
use ravel_commit::declared_stats::{
    StatCarrier, declared_stat_drops_observed, read_compaction_part, stamp_commit_record,
};
use ravel_commit::{keys, record};
use ravel_logseg::LogRecord;
use ravel_maintain::rewrite::{MigrateOutcome, ReencodeOutcome, reencode_compaction_parts};
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, FixedClock, PublishOutcome, compact_bucket,
    migrate_bucket_format, read,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{CompactionPart, CompactionRecord};
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
use ravel_types::logstream::AttrValue;
use uuid::Uuid;

const STATUS: &str = "http.status";
const CACHE: &str = "cache.hit";

/// The status sum the four rows hold, past `i64::MAX`.
fn status_sum() -> i128 {
    2 * i128::from(i64::MAX) - 4
}

fn row(ts: i64, status: Option<i64>, cache: Option<bool>) -> LogRecord {
    let mut r = log_record(0, ts, &format!("row{ts}"));
    if let Some(v) = status {
        r.attrs.push((STATUS.to_string(), AttrValue::I64(v)));
    }
    if let Some(b) = cache {
        r.attrs.push((CACHE.to_string(), AttrValue::Bool(b)));
    }
    r
}

fn input_1() -> Vec<LogRecord> {
    vec![
        row(10, Some(0), Some(false)),
        row(30, Some(i64::MAX), Some(true)),
    ]
}

fn input_2() -> Vec<LogRecord> {
    vec![
        row(20, Some(i64::MAX), None),
        row(40, Some(-4), Some(false)),
    ]
}

/// The flush stamp a pre-ADR-4628 writer puts on `records`: min, max and null
/// count, no sum and no zero count. The compactor reads only which columns are
/// declared from it.
fn l0_stamps(records: &[LogRecord]) -> Vec<DeclaredColumnStat> {
    let iv: Vec<i64> = records
        .iter()
        .flat_map(|r| &r.attrs)
        .filter_map(|(k, v)| match v {
            AttrValue::I64(x) if k == STATUS => Some(*x),
            _ => None,
        })
        .collect();
    let bv: Vec<bool> = records
        .iter()
        .flat_map(|r| &r.attrs)
        .filter_map(|(k, v)| match v {
            AttrValue::Bool(b) if k == CACHE => Some(*b),
            _ => None,
        })
        .collect();
    let rows = records.len() as u64;
    vec![
        DeclaredColumnStat::new(
            STATUS,
            DeclaredStatType::I64,
            iv.iter().min().map(|v| DeclaredStatValue::I64(*v)),
            iv.iter().max().map(|v| DeclaredStatValue::I64(*v)),
            rows - iv.len() as u64,
        )
        .unwrap(),
        DeclaredColumnStat::new(
            CACHE,
            DeclaredStatType::Bool,
            bv.iter().min().map(|v| DeclaredStatValue::Bool(*v)),
            bv.iter().max().map(|v| DeclaredStatValue::Bool(*v)),
            rows - bv.len() as u64,
        )
        .unwrap(),
    ]
}

/// Seed one L0 input whose commit record carries [`l0_stamps`] and claims
/// `segment_format_version`.
async fn seed_stamped(
    store: &dyn ObjectStoreBackend,
    writer: u128,
    seq: u64,
    records: &[LogRecord],
    segment_format_version: u32,
) {
    let key = seed_rlog_input_at_hour(
        store,
        Uuid::from_u128(writer),
        10,
        seq,
        HOUR,
        records,
        false,
        segment_format_version,
    )
    .await;
    let mut crec = record::decode(&get_full(store, &key).await).unwrap();
    stamp_commit_record(&mut crec, &l0_stamps(records));
    assert_eq!(keys::commit_key_for_record(&crec).unwrap(), key);
    store
        .put(&key, record::encode(&crec), PutOptions::default())
        .await
        .unwrap();
}

/// Both inputs at the current RLOG version.
async fn seed_both(store: &dyn ObjectStoreBackend) {
    let v = u32::from(ravel_logseg::footer::VERSION);
    seed_stamped(store, 1, 1, &input_1(), v).await;
    seed_stamped(store, 2, 2, &input_2(), v).await;
}

async fn compact(store: &dyn ObjectStoreBackend, config: &CompactorConfig) {
    let outcome = compact_bucket(
        store,
        &FixedClock::new(sealed_now_ns()),
        config,
        &logs_bucket(),
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
}

async fn compaction_records(store: &dyn ObjectStoreBackend) -> Vec<(String, CompactionRecord)> {
    let listing = read::list_bucket(store, &logs_bucket())
        .await
        .expect("list");
    let mut out = Vec::new();
    for key in listing.compaction_record_keys {
        let rec = record::decode_compaction(&get_full(store, &key).await).expect("decode");
        out.push((key, rec));
    }
    out
}

/// The one record the shared selector serves.
async fn served_one(store: &dyn ObjectStoreBackend) -> CompactionRecord {
    let records = compaction_records(store).await;
    let selection = select_authoritative_compaction_records(&records).expect("selector");
    let excluded: Vec<String> = selection.excluded().map(str::to_string).collect();
    let mut served: Vec<CompactionRecord> = records
        .into_iter()
        .filter(|(key, _)| !excluded.contains(key))
        .map(|(_, rec)| rec)
        .collect();
    assert_eq!(served.len(), 1, "the selector serves one record");
    served.remove(0)
}

/// The one part of `rec`, which every path here writes for four small rows.
fn only_part(rec: &CompactionRecord) -> &CompactionPart {
    assert_eq!(rec.parts.len(), 1, "four small rows -> one part");
    let p = &rec.parts[0];
    assert_eq!(p.sample_count, 4);
    p
}

fn stat<'a>(stats: &'a [DeclaredColumnStat], name: &str) -> &'a DeclaredColumnStat {
    stats
        .iter()
        .find(|s| s.name() == name)
        .unwrap_or_else(|| panic!("part uncovered for {name}"))
}

/// The validated `(min, max, null_count)` of both columns, which the opt-out
/// leaves unchanged.
type Triples = [(Option<DeclaredStatValue>, Option<DeclaredStatValue>, u64); 2];

fn triples(p: &CompactionPart) -> Triples {
    let v = read_compaction_part(p);
    [STATUS, CACHE].map(|name| {
        let s = stat(v.covered(), name);
        (s.min(), s.max(), s.null_count())
    })
}

/// The validated `(sum, zero_count)` of both columns.
fn aggregates(p: &CompactionPart) -> [(Option<i128>, Option<u64>); 2] {
    let v = read_compaction_part(p);
    [STATUS, CACHE].map(|name| {
        let s = stat(v.covered(), name);
        (s.sum(), s.zero_count())
    })
}

/// The figures computed by hand from the four rows (module doc).
fn expected_aggregates() -> [(Option<i128>, Option<u64>); 2] {
    [(Some(status_sum()), Some(1)), (None, Some(2))]
}

fn expected_triples() -> Triples {
    [
        (
            Some(DeclaredStatValue::I64(-4)),
            Some(DeclaredStatValue::I64(i64::MAX)),
            0,
        ),
        (
            Some(DeclaredStatValue::Bool(false)),
            Some(DeclaredStatValue::Bool(true)),
            1,
        ),
    ]
}

/// Every raw entry of `p` lacks fields 6 to 8.
fn assert_raw_without_aggregates(p: &CompactionPart, path: &str) {
    assert_eq!(
        p.declared_column_stats.len(),
        2,
        "{path}: both columns stamped"
    );
    for e in &p.declared_column_stats {
        assert_eq!(e.sum, None, "{path}: {} carries no field 6", e.name);
        assert!(
            e.sum_wide.is_empty(),
            "{path}: {} carries no field 7",
            e.name
        );
        assert_eq!(e.zero_count, None, "{path}: {} carries no field 8", e.name);
    }
}

/// The part a fresh compaction of the four rows writes under `config`.
async fn fresh_compaction_part(config: &CompactorConfig) -> CompactionPart {
    let store = MemoryStore::new();
    seed_both(&store).await;
    compact(&store, config).await;
    only_part(&served_one(&store).await).clone()
}

/// Rewrite the bucket's one compaction record as an older build would have
/// left it for a force 2 re-encode: every part one RLOG version below the
/// current one, and every stamp without fields 6 to 8.
async fn age_the_compaction_record(store: &dyn ObjectStoreBackend) -> String {
    let mut records = compaction_records(store).await;
    assert_eq!(records.len(), 1);
    let (key, mut rec) = records.remove(0);
    for part in &mut rec.parts {
        part.segment_format_version = ravel_maintain::rlog::OUTPUT_FORMAT_VERSION - 1;
        for e in &mut part.declared_column_stats {
            e.sum = None;
            e.sum_wide.clear();
            e.zero_count = None;
        }
    }
    store
        .put(&key, record::encode_compaction(&rec), PutOptions::default())
        .await
        .unwrap();
    key
}

/// Compact the four rows under the default config, age the record, and
/// re-encode it under `config`. Returns the served successor's one part.
async fn force2_part(config: &CompactorConfig) -> CompactionPart {
    let store = MemoryStore::new();
    seed_both(&store).await;
    compact(&store, &CompactorConfig::default()).await;
    let predecessor_key = age_the_compaction_record(&store).await;
    let config = CompactorConfig {
        reencode_writer_enabled: true,
        ..config.clone()
    };
    let outcome = reencode_compaction_parts(
        &store,
        &FixedClock::new(sealed_now_ns() + 1_000),
        &config,
        &logs_bucket(),
    )
    .await
    .expect("re-encode");
    match outcome {
        ReencodeOutcome::Reencoded {
            superseded_record_key,
            publish,
            ..
        } => {
            assert_eq!(superseded_record_key, predecessor_key);
            assert_eq!(publish, PublishOutcome::Published);
        }
        other => panic!("expected Reencoded, got {other:?}"),
    }
    let served = served_one(&store).await;
    assert_eq!(
        served.format_version, 2,
        "the successor is a force 2 record"
    );
    only_part(&served).clone()
}

/// Seed both inputs one version below the target and migrate them under
/// `config`. Returns the migrate output's one part.
async fn migrate_part(config: &CompactorConfig) -> CompactionPart {
    let store = MemoryStore::new();
    let below = u32::from(ravel_logseg::footer::VERSION) - 1;
    seed_stamped(&store, 1, 1, &input_1(), below).await;
    seed_stamped(&store, 2, 2, &input_2(), below).await;
    let outcome = migrate_bucket_format(
        &store,
        &FixedClock::new(sealed_now_ns()),
        config,
        &logs_bucket(),
        u32::from(ravel_logseg::footer::VERSION),
    )
    .await
    .expect("migrate");
    assert!(
        matches!(
            outcome,
            MigrateOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the fixture migrates: {outcome:?}"
    );
    only_part(&served_one(&store).await).clone()
}

fn opted_out() -> CompactorConfig {
    CompactorConfig {
        emit_declared_stat_aggregates: false,
        ..CompactorConfig::default()
    }
}

/// The default emits: the field is on unless an operator turns it off.
#[test]
fn emission_is_on_by_default() {
    assert!(CompactorConfig::default().emit_declared_stat_aggregates);
}

/// A fresh compaction of the four rows stamps the hand-computed figures, with
/// the I64 sum past `i64::MAX` written to field 7.
#[tokio::test]
async fn compaction_part_carries_the_rows_sum_and_zero_count() {
    let drops_before = declared_stat_drops_observed(StatCarrier::CompactionPart);
    let p = fresh_compaction_part(&CompactorConfig::default()).await;
    assert_eq!(triples(&p), expected_triples());
    assert_eq!(aggregates(&p), expected_aggregates());
    assert_eq!(
        declared_stat_drops_observed(StatCarrier::CompactionPart),
        drops_before,
        "no compaction stamp is dropped on read"
    );
}

/// The migrate primitive writes its logs part through the compactor's
/// accumulator, so the part carries the same exact sum and zero count as a
/// fresh compaction of the same rows. The inputs' stamps carry neither.
#[tokio::test]
async fn migrate_output_carries_the_sum_and_zero_count() {
    let p = migrate_part(&CompactorConfig::default()).await;
    let fresh = fresh_compaction_part(&CompactorConfig::default()).await;
    assert_eq!(aggregates(&p), expected_aggregates());
    assert_eq!(aggregates(&p), aggregates(&fresh));
    assert_eq!(triples(&p), triples(&fresh));
}

/// The force 2 re-encode of a predecessor whose stamps lack fields 6 to 8 (an
/// older compactor's output) recomputes them from the rows: the successor's
/// part carries the same exact sum and zero count as a fresh compaction.
#[tokio::test]
async fn force2_output_carries_the_sum_and_zero_count() {
    let p = force2_part(&CompactorConfig::default()).await;
    let fresh = fresh_compaction_part(&CompactorConfig::default()).await;
    assert_eq!(aggregates(&p), expected_aggregates());
    assert_eq!(aggregates(&p), aggregates(&fresh));
    assert_eq!(triples(&p), triples(&fresh));
}

/// Test 12, compactor half: with the opt-out set, compaction, migrate and
/// force 2 each write raw entries with no field 6, 7 or 8, while min, max and
/// null count equal an emitting run's.
#[tokio::test]
async fn opt_out_withholds_the_fields_on_every_path() {
    let drops_before = declared_stat_drops_observed(StatCarrier::CompactionPart);
    let emitting = fresh_compaction_part(&CompactorConfig::default()).await;
    let outputs = [
        ("compaction", fresh_compaction_part(&opted_out()).await),
        ("migrate", migrate_part(&opted_out()).await),
        ("force 2", force2_part(&opted_out()).await),
    ];
    for (path, p) in &outputs {
        assert_raw_without_aggregates(p, path);
        assert_eq!(triples(p), triples(&emitting), "{path}: triple unchanged");
        assert_eq!(
            aggregates(p),
            [(None, None), (None, None)],
            "{path}: the reader sees no sum and no zero count"
        );
    }
    assert_eq!(
        declared_stat_drops_observed(StatCarrier::CompactionPart),
        drops_before,
        "no compaction stamp is dropped on read"
    );
}
