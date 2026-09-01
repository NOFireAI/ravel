//! ADR-0873 wave 5b: compaction recomputes declared-column min/max/null-count
//! stamps over exactly the records that land in each output part (issue #1022).
//!
//! The L1 half of the wave-5 fold. Wave 5a stamps every L0 flush; here the
//! compactor learns WHICH columns are declared from its inputs' own stamps and
//! recomputes the extrema over the rows it writes into each part, never copying
//! an input's stamp. These tests pin, over known I64/BOOL extrema:
//!
//! 1. each output part's stamps are the min/max/null-count of the records in
//!    THAT part, across a multi-input bucket;
//! 2. at a mid-stream split, the record that opens the next part is folded into
//!    that next part, never the closing one (its extremum is constructed so a
//!    misattribution flips a pinned value);
//! 3. the stored L1 part object bytes are byte-identical whether or not the
//!    parts are stamped -- the stamp lives in the compaction record's metadata,
//!    never in the object (the #872 differential rests on that);
//! 4. after compaction the wave-4 catalog reader answers MIN/MAX/COUNT of a
//!    declared column from the resolved parts' stamps with zero GETs against any
//!    L1 data object;
//! 6. an erasure rewrite's parts stay unstamped even when the surviving records
//!    carry eligible declared columns (the wave-3 staleness rule).
//!
//! (Test 5, the conservation gate under stamping, is a unit test beside the
//! gate it exercises, in `publish.rs`.)

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use prost::Message;
use ravel_commit::declared_stats::{read_compaction_part, stamp_commit_record};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, writer::ObjectIdentity};
use ravel_maintain::{
    Bucket, CompactorConfig, ErasureRewriteOutcome, FixedClock, MaintainMemo, NoLeases,
    PendingErasureRequest, compact_bucket, erasure_rewrite_bucket,
};
use ravel_object_store::fault::{FaultKind, FaultPlan, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{
    CompactionPart, CompactionRecord, ErasurePredicateMatcher, ErasureRequest, RewriteRecord,
};
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
use ravel_types::logstream::{AttrValue, LogStreamId, log_stream_id};
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

const TENANT: &str = "acme";
const SHARD: u32 = 7;
const HOUR: u32 = 495_000;
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const EPOCH: u64 = 10;

/// The I64 declared column under test (ClickBench's `EventDate` shape).
const STATUS: &str = "http.status";
/// The BOOL declared column under test (ClickBench's `IsRefresh` shape).
const CACHE: &str = "cache.hit";

/// Each record's body is padded to this length. `estimate_record` (the memory
/// split target's trigger) is then dominated by the body, so a part splits as a
/// function of record COUNT, not of which declared values a record carries --
/// the two tests that force a split can place a known number of records per part
/// without depending on the exact per-record slot overhead.
const BODY_LEN: usize = 20_000;

/// Two records fit under this decoded-heap target and three do not: at
/// `BODY_LEN` = 20_000 each record's estimate is ~20_400, so one is ~20_400
/// (< 30_000) and two are ~40_800 (>= 30_000). `max_l1_part_bytes` stays at its
/// 256 MiB default so only the memory split target ever fires here.
const SPLIT_TARGET_BYTES: u64 = 30_000;

const OUTPUT_FORMAT_VERSION: u32 = ravel_maintain::rlog::OUTPUT_FORMAT_VERSION;

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn bucket() -> Bucket {
    Bucket::new(tenant_hash(), Signal::Logs, SHARD, HOUR)
}

/// Past the seal margin for [`HOUR`] under default config.
fn sealed_now_ns() -> i64 {
    (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
}

/// A `CompactorConfig` whose memory split target forces a part boundary every
/// two records (see [`SPLIT_TARGET_BYTES`]).
fn split_config() -> CompactorConfig {
    CompactorConfig {
        l1_part_memory_target_bytes: SPLIT_TARGET_BYTES,
        ..Default::default()
    }
}

/// A synthetic stream `n`'s id and canonical resource+scope blob, identical to
/// the crate's own RLOG test helper: the id is the true hash of the blob.
fn stream_ident(n: u32) -> (LogStreamId, Vec<u8>) {
    let res = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc{n}")),
    )];
    let id = log_stream_id(&res, "scope", "1", &[]);
    let blob = ravel_logseg::stream_attrs_bytes(&res, "scope", "1", &[]);
    (id, blob)
}

/// A body of exactly [`BODY_LEN`] bytes, tagged with `ts` so distinct records
/// stay distinct (logs never dedup, ADR-0032) while keeping a uniform length.
fn body_of(ts: i64) -> String {
    let mut b = format!("ts{ts}-");
    if b.len() < BODY_LEN {
        b.push_str(&"x".repeat(BODY_LEN - b.len()));
    }
    b
}

/// One record on stream `n` at `ts`, optionally carrying the I64 `STATUS` and
/// BOOL `CACHE` declared columns. A `None` means the attribute is absent, which
/// the fold reads as a NULL for that declaration.
fn rec(
    n: u32,
    ts: i64,
    status: Option<i64>,
    cache: Option<bool>,
    extra: &[(&str, &str)],
) -> LogRecord {
    let (stream_id, stream_attrs) = stream_ident(n);
    let mut attrs: Vec<(String, AttrValue)> = Vec::new();
    if let Some(v) = status {
        attrs.push((STATUS.to_string(), AttrValue::I64(v)));
    }
    if let Some(b) = cache {
        attrs.push((CACHE.to_string(), AttrValue::Bool(b)));
    }
    for (k, v) in extra {
        attrs.push(((*k).to_string(), AttrValue::Str((*v).to_string())));
    }
    LogRecord {
        stream_id,
        stream_attrs,
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body_of(ts),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn attr_i64(r: &LogRecord, name: &str) -> Option<i64> {
    r.attrs.iter().find_map(|(k, v)| match v {
        AttrValue::I64(x) if k == name => Some(*x),
        _ => None,
    })
}

fn attr_bool(r: &LogRecord, name: &str) -> Option<bool> {
    r.attrs.iter().find_map(|(k, v)| match v {
        AttrValue::Bool(b) if k == name => Some(*b),
        _ => None,
    })
}

/// The exact declared-column stamps a wave-5a L0 writer would carry for
/// `records`: `STATUS` as I64 and `CACHE` as BOOL, extrema over the non-null
/// values (absent when there are none), and the exact NULL count. The compactor
/// reads only the (name, type) pair from these to learn which columns are
/// declared; it recomputes the extrema itself over the merged rows.
fn l0_stamps(records: &[LogRecord]) -> Vec<DeclaredColumnStat> {
    let iv: Vec<i64> = records.iter().filter_map(|r| attr_i64(r, STATUS)).collect();
    let bv: Vec<bool> = records.iter().filter_map(|r| attr_bool(r, CACHE)).collect();
    let i_null = records.len() as u64 - iv.len() as u64;
    let b_null = records.len() as u64 - bv.len() as u64;
    vec![
        DeclaredColumnStat::new(
            STATUS,
            DeclaredStatType::I64,
            iv.iter().min().map(|v| DeclaredStatValue::I64(*v)),
            iv.iter().max().map(|v| DeclaredStatValue::I64(*v)),
            i_null,
        )
        .expect("valid I64 L0 stamp"),
        DeclaredColumnStat::new(
            CACHE,
            DeclaredStatType::Bool,
            bv.iter().min().map(|v| DeclaredStatValue::Bool(*v)),
            bv.iter().max().map(|v| DeclaredStatValue::Bool(*v)),
            b_null,
        )
        .expect("valid BOOL L0 stamp"),
    ]
}

/// Seed one L0 `.rlog` input (data object + commit record), as the ingest log
/// shard would. When `stamp` is set the commit record carries the wave-5a
/// declared-column stamps for `records`; otherwise it carries none (the
/// pre-declaration corpus), which leaves the compaction output unstamped.
async fn seed(
    store: &dyn ObjectStoreBackend,
    writer_id: Uuid,
    seq: u64,
    records: &[LogRecord],
    stamp: bool,
) {
    let th = tenant_hash();
    let identity = ObjectIdentity {
        tenant_hash: th.0,
        shard: SHARD,
        writer_id: writer_id.into_bytes(),
        writer_epoch: EPOCH,
        writer_seq: seq,
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity);
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = bytes::Bytes::from(w.finish().expect("finish L0"));
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let data_key = keys::data_key(
        &th,
        Signal::Logs,
        SHARD,
        writer_id,
        EPOCH,
        seq,
        &content_hash,
    )
    .expect("data key");
    store
        .put(&data_key, bytes.clone(), PutOptions::default())
        .await
        .expect("put data");

    let mut ids = std::collections::BTreeSet::new();
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for r in records {
        ids.insert(r.stream_id);
        min_ts = min_ts.min(r.ts_ns);
        max_ts = max_ts.max(r.ts_ns);
    }
    let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
    let mut crec = record::build(NewCommitRecord {
        tenant_hash: th,
        signal: Signal::Logs,
        shard: SHARD,
        writer_id,
        writer_epoch: EPOCH,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: records.len() as u64,
        series_count: ids.len() as u64,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: created,
        max_ingest_ts_ns: created,
        segment_format_version: OUTPUT_FORMAT_VERSION,
        created_unix_ns: created,
        ingest_hour_bucket: HOUR,
    })
    .expect("build commit record");
    if stamp {
        stamp_commit_record(&mut crec, &l0_stamps(records));
    }
    let commit_key = keys::commit_key_for_record(&crec).expect("commit key");
    store
        .put(&commit_key, record::encode(&crec), PutOptions::default())
        .await
        .expect("put commit");
}

/// Compact the seeded bucket and return the single compaction record, decoded.
async fn compact_and_record(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
) -> CompactionRecord {
    let clock = FixedClock::new(sealed_now_ns());
    compact_bucket(store, &clock, config, &bucket())
        .await
        .expect("compact");
    let prefix = keys::commit_shard_hour_prefix(&tenant_hash(), Signal::Logs, SHARD, HOUR).unwrap();
    let record_key = list_all(store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .find(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .expect("compaction record key");
    let bytes = store.get(&record_key, GetRange::Full).await.unwrap().data;
    CompactionRecord::decode(bytes.as_ref()).unwrap()
}

/// The validated I64 stamp `(min, max, null_count)` a part carries for `name`,
/// or a panic if the part is uncovered for it. Read through
/// [`read_compaction_part`], the same wave-2/3 predicate the query side uses, so
/// a stamp the reader would drop is never reported as coverage.
fn part_i64(p: &CompactionPart, name: &str) -> (Option<i64>, Option<i64>, u64) {
    let validated = read_compaction_part(p);
    let stat = validated
        .covered()
        .iter()
        .find(|s| s.name() == name)
        .unwrap_or_else(|| panic!("part {} uncovered for {name}", p.part_index));
    assert_eq!(stat.declared_type(), DeclaredStatType::I64);
    let mn = match stat.min() {
        Some(DeclaredStatValue::I64(x)) => Some(x),
        None => None,
        other => panic!("non-I64 min {other:?}"),
    };
    let mx = match stat.max() {
        Some(DeclaredStatValue::I64(x)) => Some(x),
        None => None,
        other => panic!("non-I64 max {other:?}"),
    };
    (mn, mx, stat.null_count())
}

/// The validated BOOL stamp `(min, max, null_count)` a part carries for `name`.
fn part_bool(p: &CompactionPart, name: &str) -> (Option<bool>, Option<bool>, u64) {
    let validated = read_compaction_part(p);
    let stat = validated
        .covered()
        .iter()
        .find(|s| s.name() == name)
        .unwrap_or_else(|| panic!("part {} uncovered for {name}", p.part_index));
    assert_eq!(stat.declared_type(), DeclaredStatType::Bool);
    let mn = match stat.min() {
        Some(DeclaredStatValue::Bool(b)) => Some(b),
        None => None,
        other => panic!("non-BOOL min {other:?}"),
    };
    let mx = match stat.max() {
        Some(DeclaredStatValue::Bool(b)) => Some(b),
        None => None,
        other => panic!("non-BOOL max {other:?}"),
    };
    (mn, mx, stat.null_count())
}

// ---------------------------------------------------------------------------
// Test 1: each output part's stamps are exactly its own records' extrema.
// ---------------------------------------------------------------------------

/// Two inputs merge into two output parts of two records each, and each part's
/// I64/BOOL stamps are the exact min/max/null-count of the records THAT part
/// holds -- never the whole bucket's, and never the other part's.
///
/// The four records span both extremes of each column and one NULL per column
/// per part, so a fold that leaked a value across the part boundary (or summed
/// the whole bucket) reports a different min, max, or null_count than the one
/// asserted here. `non_null + null_count == sample_count` is checked on every
/// part, the invariant `read_compaction_part` relies on.
///
/// Prove-the-test: delete the `self.declared_accum.observe_record(&r.attrs)`
/// call in `PartBuilder::push` (crates/ravel-maintain/src/rlog.rs) and every
/// part stamps all-NULL, so each `Some(..)` min/max assertion fails.
#[tokio::test]
async fn each_output_part_stamps_exactly_its_own_records() {
    let store = MemoryStore::new();
    // Merged (ts-ascending) order across the two inputs is A, B, C, D. The
    // memory split target closes a part every two records, so part 0 = {A, B}
    // and part 1 = {C, D}.
    //   A ts1000 status=50  cache=false
    //   B ts2000 status=10  cache=true    -> part0: status 10..50 null0, cache false..true null0
    //   C ts3000 status=--  cache=true     (status NULL)
    //   D ts4000 status=99  cache=--       (cache NULL) -> part1: status 99..99 null1, cache true..true null1
    let input_1 = vec![
        rec(0, 1000, Some(50), Some(false), &[]),
        rec(0, 3000, None, Some(true), &[]),
    ];
    let input_2 = vec![
        rec(0, 2000, Some(10), Some(true), &[]),
        rec(0, 4000, Some(99), None, &[]),
    ];
    seed(&store, Uuid::from_u128(1), 1, &input_1, true).await;
    seed(&store, Uuid::from_u128(2), 2, &input_2, true).await;

    let record = compact_and_record(&store, &split_config()).await;
    assert_eq!(record.parts.len(), 2, "two records per part -> two parts");

    for p in &record.parts {
        assert_eq!(p.sample_count, 2, "each part holds two records");
        for stat in read_compaction_part(p).covered() {
            assert!(
                stat.null_count() <= p.sample_count,
                "a stamp's null_count can never exceed its part's sample_count \
                 (null_count={} sample_count={})",
                stat.null_count(),
                p.sample_count
            );
        }
    }

    let p0 = &record.parts[0];
    assert_eq!(
        part_i64(p0, STATUS),
        (Some(10), Some(50), 0),
        "part0 status 10..50, no NULL"
    );
    assert_eq!(
        part_bool(p0, CACHE),
        (Some(false), Some(true), 0),
        "part0 cache false..true, no NULL"
    );

    let p1 = &record.parts[1];
    assert_eq!(
        part_i64(p1, STATUS),
        (Some(99), Some(99), 1),
        "part1 status 99, one NULL (C)"
    );
    assert_eq!(
        part_bool(p1, CACHE),
        (Some(true), Some(true), 1),
        "part1 cache true, one NULL (D)"
    );
}

// ---------------------------------------------------------------------------
// Test 2: the record that opens the next part is folded into that part.
// ---------------------------------------------------------------------------

/// At a mid-stream split the boundary record belongs to the part it OPENS, not
/// the part that just closed. Three records on one stream split after two, so
/// R3 opens part 1. R1 and R2 both carry `status = 100` and R3 carries
/// `status = 5`, so:
///   - if R3 were (mis)folded into part 0, part 0's min would flip 100 -> 5;
///   - part 1's stamp would then be all-NULL or empty.
/// Asserting `part0.min == 100` excludes both a misattribution (`5`) and a
/// dropped fold (`None`).
///
/// Prove-the-test: two independent flips fail it. (a) Delete the
/// `observe_record` call in `PartBuilder::push` (rlog.rs) -> part 0's min is
/// `None`, not `Some(100)`. (b) Raise this test's `l1_part_memory_target_bytes`
/// so the split falls after three records instead of two: R3 then lands in part
/// 0 and part 0's min flips to 5 -- the same misattribution the per-part
/// accumulator prevents, demonstrated by moving the boundary.
#[tokio::test]
async fn boundary_record_folds_into_the_part_it_opens() {
    let store = MemoryStore::new();
    // input_1 = {R1, R3}, input_2 = {R2}; merged ts-ascending: R1, R2, R3.
    let input_1 = vec![
        rec(0, 1000, Some(100), None, &[]),
        rec(0, 3000, Some(5), None, &[]),
    ];
    let input_2 = vec![rec(0, 2000, Some(100), None, &[])];
    seed(&store, Uuid::from_u128(10), 1, &input_1, true).await;
    seed(&store, Uuid::from_u128(11), 2, &input_2, true).await;

    let record = compact_and_record(&store, &split_config()).await;
    assert_eq!(
        record.parts.len(),
        2,
        "split after two records -> {{R1,R2}} and {{R3}}"
    );

    let p0 = &record.parts[0];
    assert_eq!(p0.sample_count, 2, "part0 holds R1 and R2");
    assert_eq!(
        part_i64(p0, STATUS),
        (Some(100), Some(100), 0),
        "part0 is R1,R2 only: min stays 100, NOT the boundary record's 5"
    );

    let p1 = &record.parts[1];
    assert_eq!(
        p1.sample_count, 1,
        "part1 holds only the boundary record R3"
    );
    assert_eq!(
        part_i64(p1, STATUS),
        (Some(5), Some(5), 0),
        "R3's 5 lands in the part it opens"
    );
}

// ---------------------------------------------------------------------------
// Test 3: stamping does not move the stored object bytes.
// ---------------------------------------------------------------------------

/// The declared-column stamp lives in the compaction record's part metadata,
/// never in the L1 data object. Compacting the same records with the inputs
/// stamped and unstamped yields byte-identical L1 part objects (identical
/// content-addressed keys AND identical bytes), while only the stamped run's
/// record carries non-empty `declared_column_stats`. This is what keeps the
/// #872 differential hashes untouched by wave 5b.
///
/// Prove-the-test: delete the `observe_record` call in `PartBuilder::push`
/// (rlog.rs) -> the stamped run's parts stamp all-NULL (absent extrema), so the
/// "part0 carries the real extrema" assertion below fails while the
/// byte-identity half still holds -- which is exactly why byte identity alone is
/// not enough and this test also pins the stamped values.
#[tokio::test]
async fn stamping_leaves_the_stored_object_bytes_identical() {
    // Merged ts order: 1000(50), 2000(10), 3000(7), 4000(99); parts split after
    // two, so part0 = {50, 10} and part1 = {7, 99}.
    let records_1 = vec![
        rec(0, 1000, Some(50), Some(false), &[]),
        rec(0, 3000, Some(7), Some(true), &[]),
    ];
    let records_2 = vec![
        rec(0, 2000, Some(10), Some(true), &[]),
        rec(0, 4000, Some(99), Some(false), &[]),
    ];

    // Same records, one run stamped and one not.
    let stamped_store = MemoryStore::new();
    seed(&stamped_store, Uuid::from_u128(1), 1, &records_1, true).await;
    seed(&stamped_store, Uuid::from_u128(2), 2, &records_2, true).await;
    let stamped = compact_and_record(&stamped_store, &split_config()).await;

    let plain_store = MemoryStore::new();
    seed(&plain_store, Uuid::from_u128(1), 1, &records_1, false).await;
    seed(&plain_store, Uuid::from_u128(2), 2, &records_2, false).await;
    let plain = compact_and_record(&plain_store, &split_config()).await;

    assert_eq!(stamped.parts.len(), plain.parts.len());

    // The stamped run must carry the REAL extrema (not just present, all-NULL
    // stamps) -- otherwise byte-identity is vacuous.
    assert_eq!(stamped.parts.len(), 2, "two records per part -> two parts");
    assert_eq!(
        part_i64(&stamped.parts[0], STATUS),
        (Some(10), Some(50), 0),
        "part0 status 10..50 over the records it holds"
    );
    assert_eq!(
        part_i64(&stamped.parts[1], STATUS),
        (Some(7), Some(99), 0),
        "part1 status 7..99 over the records it holds"
    );
    for p in &plain.parts {
        assert!(
            read_compaction_part(p).covered().is_empty(),
            "an unstamped input set produces unstamped parts"
        );
    }

    // The L1 data objects are byte-identical: same content-addressed key (which
    // hashes the whole object) and same bytes.
    for (sp, pp) in stamped.parts.iter().zip(plain.parts.iter()) {
        assert_eq!(
            sp.content_hash, pp.content_hash,
            "stamping must not change a part's content hash"
        );
        let sk = keys::reconstruct_l1_part_key(&stamped, sp).unwrap();
        let pk = keys::reconstruct_l1_part_key(&plain, pp).unwrap();
        assert_eq!(sk, pk, "content-addressed L1 part keys must match");
        let sb = stamped_store.get(&sk, GetRange::Full).await.unwrap().data;
        let pb = plain_store.get(&pk, GetRange::Full).await.unwrap().data;
        assert_eq!(sb, pb, "stored L1 part bytes must be byte-identical");
    }
}

// ---------------------------------------------------------------------------
// Test 4: the wave-4 reader answers from the stamps with zero data-object GETs.
// ---------------------------------------------------------------------------

/// After compaction, resolving the bucket's snapshot carries each compacted
/// part's declared-column stamps onto its `SegmentRef`, and MIN/MAX/COUNT of a
/// declared column fold out of those stamps without opening a single L1 data
/// object. The store is wrapped in a `FaultStore` that fails every GET against
/// an `/l1/` (data part) key, so any attempt to read a part body aborts the
/// resolve; the resolve succeeds and the fault never fires, which is the
/// zero-data-GET reachability proof (the maintain-side twin of
/// `ravel-sql/tests/logs_declared_minmax_from_stamps.rs`).
///
/// Prove-the-test: delete the `observe_record` call in `PartBuilder::push`
/// (rlog.rs) -> the resolved segments are uncovered for `STATUS` and the
/// per-segment coverage assertion panics.
#[tokio::test]
async fn resolve_answers_min_max_count_from_stamps_without_data_gets() {
    let inner: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    // Event timestamps inside HOUR's own ns window, so the resolve's time-range
    // filter (which is over event time) selects these segments.
    let base = i64::from(HOUR) * NS_PER_HOUR;
    let input_1 = vec![
        rec(0, base + 1000, Some(50), Some(false), &[]),
        rec(0, base + 3000, None, Some(true), &[]),
    ];
    let input_2 = vec![
        rec(0, base + 2000, Some(10), Some(true), &[]),
        rec(0, base + 4000, Some(99), None, &[]),
    ];
    seed(&*inner, Uuid::from_u128(1), 1, &input_1, true).await;
    seed(&*inner, Uuid::from_u128(2), 2, &input_2, true).await;

    // The true answer over all four records: status non-null values are 50, 10,
    // 99 with one NULL (base + 3000), so MIN 10, MAX 99, COUNT(status) 3.
    let all: Vec<&LogRecord> = input_1.iter().chain(input_2.iter()).collect();
    let sv: Vec<i64> = all.iter().filter_map(|r| attr_i64(r, STATUS)).collect();
    let true_min = *sv.iter().min().unwrap();
    let true_max = *sv.iter().max().unwrap();
    let true_rows = all.len() as u64;
    let true_status_nulls = true_rows - sv.len() as u64;

    // Compact on the raw store (this legitimately reads and PUTs data objects).
    let record = compact_and_record(&*inner, &CompactorConfig::default()).await;
    assert!(!record.parts.is_empty());

    // Now arm a permanent GET fault on every L1 data-part key and resolve. A
    // resolve that opened a part body would error; commit/compaction records
    // live under `/c/`, not `/l1/`, so a stats-only resolve never trips it.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Get,
            ScriptedFault::Permanent("resolve must not GET an L1 data object".into()),
        )
        .with_key_contains("/l1/"),
    );
    let faulted = Arc::new(ravel_object_store::fault::FaultStore::new(
        Arc::clone(&inner),
        plan,
    ));
    let catalog = ravel_catalog::Catalog::new(
        Arc::clone(&faulted) as Arc<dyn ObjectStoreBackend>,
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("catalog");
    let range = TimeRange {
        start_ns: i64::from(HOUR) * NS_PER_HOUR,
        end_ns: (i64::from(HOUR) + 1) * NS_PER_HOUR,
    };
    let snapshot = catalog
        .resolve(&tenant_hash(), Signal::Logs, range, &[], sealed_now_ns())
        .await
        .expect("resolve must not read any L1 data object");

    assert_eq!(
        faulted.fault_count(Op::Get, FaultKind::Permanent),
        0,
        "resolve must answer from record metadata, never a GET on an /l1/ data object"
    );
    // Negative control arming the rule itself: a direct GET of a real L1 part
    // key must trip the fault, proving the "/l1/" substring matches the keys
    // this tenant actually writes (a typo in the rule would make the zero
    // assertion above silently vacuous).
    let l1_key = &snapshot.segments[0].data_object_key;
    assert!(
        l1_key.contains("/l1/"),
        "resolved segment key {l1_key:?} must be an L1 part"
    );
    faulted
        .get(l1_key, ravel_object_store::GetRange::Full)
        .await
        .expect_err("a direct GET of a real L1 key must trip the armed fault rule");
    assert_eq!(
        faulted.fault_count(Op::Get, FaultKind::Permanent),
        1,
        "the negative control must fire the rule exactly once"
    );
    assert!(
        !snapshot.segments.is_empty(),
        "the compacted bucket resolves segments"
    );

    // Fold MIN/MAX/COUNT(status) out of the resolved parts' stamps.
    let mut min_status = i64::MAX;
    let mut max_status = i64::MIN;
    let mut rows = 0u64;
    let mut status_nulls = 0u64;
    for seg in &snapshot.segments {
        rows += seg.sample_count;
        let stat = seg
            .declared_column_stats
            .column(STATUS)
            .unwrap_or_else(|| panic!("resolved segment uncovered for {STATUS}"));
        if let Some(DeclaredStatValue::I64(v)) = stat.min() {
            min_status = min_status.min(v);
        }
        if let Some(DeclaredStatValue::I64(v)) = stat.max() {
            max_status = max_status.max(v);
        }
        status_nulls += stat.null_count();
    }

    assert_eq!(rows, true_rows, "the parts account for every record");
    assert_eq!(min_status, true_min, "MIN(status) from stamps");
    assert_eq!(max_status, true_max, "MAX(status) from stamps");
    // COUNT(status) = total rows - NULL rows, taken from the exact null counts.
    assert_eq!(
        rows - status_nulls,
        true_rows - true_status_nulls,
        "COUNT(status) = rows - NULLs, from the stamped null counts"
    );
}

// ---------------------------------------------------------------------------
// Test 6: an erasure rewrite's parts stay unstamped.
// ---------------------------------------------------------------------------

/// The erasure rewrite drops rows, so a stamp computed over the pre-drop set
/// would be stale for the survivors (ADR-0873 decision 3 staleness rule). Its
/// output parts therefore carry NO declared-column stamps, even when the
/// surviving records carry eligible declared columns and the L0 inputs were
/// stamped -- the rewrite passes an empty declared set through the shared merge
/// driver, so `finalize_part` stamps nothing.
///
/// Prove-the-test: in `erasure_rewrite::build_rewrite_logs` (rlog part of
/// crates/ravel-maintain/src/erasure_rewrite.rs) replace the empty `declared`
/// argument to `merge_catalogs` with a non-empty set (e.g. `vec![(STATUS,
/// DeclaredStatType::I64)]`) and the survivors' parts gain stamps, failing the
/// `is_empty` assertion here.
#[tokio::test]
async fn erasure_rewrite_parts_stay_unstamped() {
    let store = MemoryStore::new();
    // Two records carry the victim marker (dropped by the request); two do not
    // and carry the eligible declared columns (survivors).
    let victim = &[("subject", "victim")][..];
    let input_1 = vec![
        rec(0, 1000, Some(50), Some(false), &[]),
        rec(0, 2000, Some(10), Some(true), victim),
    ];
    let input_2 = vec![
        rec(0, 3000, Some(99), Some(true), victim),
        rec(0, 4000, Some(7), Some(false), &[]),
    ];
    seed(&store, Uuid::from_u128(1), 1, &input_1, true).await;
    seed(&store, Uuid::from_u128(2), 2, &input_2, true).await;

    let request_id = Uuid::from_u128(99);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: ravel_commit::signal::to_proto(Signal::Logs) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 0,
        predicate: vec![ErasurePredicateMatcher {
            key: "subject".to_string(),
            value: "victim".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    let pending = PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Logs, request_id)
            .expect("dreq key"),
        request,
    };

    let clock = FixedClock::new(sealed_now_ns());
    let mut memo = MaintainMemo::with_default_interval();
    let outcome = erasure_rewrite_bucket(
        &store,
        &clock,
        &CompactorConfig::default(),
        &NoLeases,
        &bucket(),
        std::slice::from_ref(&pending),
        &mut memo,
    )
    .await
    .expect("rewrite");
    assert!(
        matches!(outcome, ErasureRewriteOutcome::Rewritten { .. }),
        "the victim records must trigger a rewrite, got {outcome:?}"
    );

    // Decode the rewrite record and assert every part is unstamped.
    let prefix = keys::commit_shard_hour_prefix(&tenant_hash(), Signal::Logs, SHARD, HOUR).unwrap();
    let rewrite_key = list_all(&store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .find(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::RewriteRecord(_))
            )
        })
        .expect("rewrite record key");
    let bytes = store.get(&rewrite_key, GetRange::Full).await.unwrap().data;
    let rewrite = RewriteRecord::decode(bytes.as_ref()).unwrap();

    assert!(
        !rewrite.parts.is_empty(),
        "the survivors produce at least one part"
    );
    let survivor_rows: u64 = rewrite.parts.iter().map(|p| p.sample_count).sum();
    assert_eq!(survivor_rows, 2, "two survivors after the drop");
    for p in &rewrite.parts {
        assert!(
            p.declared_column_stats.is_empty(),
            "rewrite parts stay unstamped (wave-3 staleness rule), part {} had stamps",
            p.part_index
        );
        assert!(
            read_compaction_part(p).covered().is_empty(),
            "and the reader sees no coverage on a rewrite part"
        );
    }
}
