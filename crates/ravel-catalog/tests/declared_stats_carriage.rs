//! ADR-0873 wave 3: the declared-column stamps a writer put on a commit
//! record or a compaction part must arrive, unchanged, on the `SegmentRef` a
//! resolve returns -- through the fold, the `.csnap` part, and snapshot
//! resolution -- and must be identical whether the segment is resolved from a
//! live listing above the fold watermark or from a sealed snapshot entry.
//!
//! Every assertion here is on the exact stamp struct (name, declared type,
//! both extrema, null count), never on a length or a "some coverage exists"
//! predicate: a carriage bug that swaps two columns, drops an extremum, or
//! defaults a null count to zero passes every count-shaped assertion, and a
//! defaulted null count silently turns "every row is NULL" into "no NULLs"
//! with `Precision::Exact` attached.
//!
//! Resolution never fetches segment bytes, and the fold's postings build is
//! allowed to fail without failing the fold, so these tests publish commit and
//! compaction records only -- no data objects.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use prost::Message;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, PartLimits, SegmentLevel, SegmentRef, decode_part, encode_part,
    read_snapshot_entry,
};
use ravel_commit::declared_stats::{
    DeclaredStatDefect, stamp_commit_record, stamp_compaction_part,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::{
    DeclaredColumnMinMax, DeclaredColumnStatValue, SnapshotEntry, declared_column_stat_value::Kind,
};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord,
};
use ravel_segment::VERSION_V7;
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;
const SIGNAL: Signal = Signal::Logs;

fn tenant() -> TenantHash {
    TenantHash([0x7a; 16])
}

fn config() -> CatalogConfig {
    CatalogConfig {
        shard_count: 1,
        ..Default::default()
    }
}

/// `now_ns` at which ingest hour `hour` has just sealed under default margins.
fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn i64_stat(name: &str, min: i64, max: i64, null_count: u64) -> DeclaredColumnStat {
    DeclaredColumnStat::new(
        name,
        DeclaredStatType::I64,
        Some(DeclaredStatValue::I64(min)),
        Some(DeclaredStatValue::I64(max)),
        null_count,
    )
    .expect("valid i64 stat")
}

fn bool_stat(name: &str, min: bool, max: bool, null_count: u64) -> DeclaredColumnStat {
    DeclaredColumnStat::new(
        name,
        DeclaredStatType::Bool,
        Some(DeclaredStatValue::Bool(min)),
        Some(DeclaredStatValue::Bool(max)),
        null_count,
    )
    .expect("valid bool stat")
}

/// An I64 stat (ADR-4628) carrying the exact sum and zero count, built exactly
/// as the writer that computed them would stamp them.
fn i64_full(
    name: &str,
    min: i64,
    max: i64,
    null_count: u64,
    sum: i128,
    zero_count: u64,
) -> DeclaredColumnStat {
    i64_stat(name, min, max, null_count)
        .with_sum(sum)
        .expect("valid sum")
        .with_zero_count(zero_count)
        .expect("valid zero count")
}

/// A BOOL stat carrying its zero (`false`) count; BOOL columns carry no sum.
fn bool_zero(
    name: &str,
    min: bool,
    max: bool,
    null_count: u64,
    zero_count: u64,
) -> DeclaredColumnStat {
    bool_stat(name, min, max, null_count)
        .with_zero_count(zero_count)
        .expect("valid zero count")
}

/// A sum past `i64::MAX`, so the encoder writes field 7 (`sum_wide`, 16 bytes)
/// and leaves field 6 absent. For a column whose `100` non-null rows are each
/// `i64::MAX`, the only in-bounds sum is `100 * i64::MAX`, which is this value.
fn wide_sum() -> i128 {
    100i128 * i128::from(i64::MAX)
}

/// Content hash of the record `l0_record` builds for `seq`, so a test can find
/// one segment among several without depending on snapshot order.
fn l0_hash_byte(seq: u64) -> u8 {
    seq as u8 ^ 0x5a
}

/// A self-consistent L0 commit record of `sample_count` rows, stamped with
/// `stats` exactly as the writer that computed them would stamp it.
fn l0_record(seq: u64, hour: u32, sample_count: u64, stats: &[DeclaredColumnStat]) -> CommitRecord {
    let event = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
    let mut rec = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: SIGNAL,
        shard: 0,
        writer_id: Uuid::from_u128(u128::from(seq) + 1),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: 100,
        content_hash: [l0_hash_byte(seq); 32],
        sample_count,
        series_count: 1,
        min_event_ts_ns: event,
        max_event_ts_ns: event + 100,
        min_ingest_ts_ns: event,
        max_ingest_ts_ns: event + 100,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: event,
        ingest_hour_bucket: hour,
    })
    .expect("valid record");
    stamp_commit_record(&mut rec, stats);
    rec
}

async fn put_l0(store: &dyn ObjectStoreBackend, rec: &CommitRecord) {
    let key = keys::commit_key_for_record(rec).expect("commit key");
    store
        .put(&key, record::encode(rec), PutOptions::create_if_absent())
        .await
        .expect("put commit record");
}

fn part(
    part_index: u32,
    hour: u32,
    seed: u8,
    sample_count: u64,
    stats: &[DeclaredColumnStat],
) -> CompactionPart {
    let event = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
    let mut part = CompactionPart {
        part_index,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count,
        series_count: 2,
        run_count: 3,
        min_event_ts_ns: event,
        max_event_ts_ns: event + 100,
        segment_format_version: u32::from(VERSION_V7),
        declared_column_stats: Vec::new(),
    };
    stamp_compaction_part(&mut part, stats);
    part
}

async fn put_compaction_record(
    store: &dyn ObjectStoreBackend,
    hour: u32,
    inputs: &[&CommitRecord],
    parts: Vec<CompactionPart>,
) {
    let input_set_hash = *blake3::hash(b"declared-stats-carriage").as_bytes();
    let rec = CompactionRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(SIGNAL) as i32,
        shard: 0,
        ingest_hour_bucket: hour,
        level: 1,
        inputs: inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect(),
        input_set_hash: input_set_hash.to_vec(),
        parts,
        created_unix_ns: i64::from(hour) * NS_PER_HOUR + 120_000_000_000,
        superseded_record_key: String::new(),
    };
    let hash16: String = input_set_hash[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let key =
        keys::compaction_record_key(&tenant(), SIGNAL, 0, hour, &hash16).expect("compaction key");
    store
        .put(
            &key,
            rec.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put compaction record");
}

/// Fold, then return the sealed snapshot entries the fold wrote, decoded from
/// the `.csnap` part itself.
///
/// Reading the part is what makes the fold's copy observable on its own: a
/// resolve re-validates every entry it reads, so a `SegmentRef` alone cannot
/// distinguish "the fold refused to copy it" from "the resolve dropped it".
async fn fold_and_read_entries(
    store: &Arc<MemoryStore>,
    catalog: &Catalog,
    now_ns: i64,
) -> Vec<SnapshotEntry> {
    let report = catalog
        .fold(&tenant(), SIGNAL, Uuid::new_v4(), now_ns, &[], None)
        .await
        .expect("fold");
    assert!(!report.no_op, "the fold sealed at least one hour");

    let prefix = format!(
        "t/{}/catalog/{}/snap/",
        tenant().to_hex(),
        SIGNAL.key_prefix()
    );
    let page = store
        .list(&prefix, None)
        .await
        .expect("list snapshot parts");
    assert_eq!(page.objects.len(), 1, "one fold, one part");
    let bytes = store
        .get(&page.objects[0].key, GetRange::Full)
        .await
        .expect("get snapshot part")
        .data;
    decode_part(&bytes, &PartLimits::default())
        .expect("snapshot part decodes")
        .entries
}

async fn resolve_all(catalog: &Catalog, from_hour: u32, now_ns: i64) -> Vec<SegmentRef> {
    let range = TimeRange {
        start_ns: i64::from(from_hour) * NS_PER_HOUR,
        end_ns: now_ns,
    };
    catalog
        .resolve(&tenant(), SIGNAL, range, &[], now_ns)
        .await
        .expect("resolve")
        .segments
}

fn segment_with_hash(segments: &[SegmentRef], first_byte: u8) -> &SegmentRef {
    segments
        .iter()
        .find(|s| s.content_hash[0] == first_byte)
        .expect("segment present in the snapshot")
}

fn entry_with_hash(entries: &[SnapshotEntry], first_byte: u8) -> &SnapshotEntry {
    entries
        .iter()
        .find(|e| e.content_hash[0] == first_byte)
        .expect("entry present in the part")
}

/// Test 1, the round trip: two commit records, each stamped for two declared
/// columns of different eligible types, fold into `.csnap` entries and resolve
/// into `SegmentRef`s whose stamps equal the input stamps exactly -- and the
/// listing path above the watermark produces exactly the same stamps as the
/// sealed path below it.
#[tokio::test]
async fn folded_stamps_reach_the_resolved_segment_ref_unchanged() {
    let store = Arc::new(MemoryStore::new());
    let hour = 500_000u32;
    let now = now_at_seal(hour);

    let a_stats = vec![
        i64_stat("EventDate", -5, 19_000, 12),
        bool_stat("IsRefresh", false, true, 0),
    ];
    let b_stats = vec![
        // A degenerate span (min == max), and an all-NULL column: both extrema
        // absent with null_count equal to the row count is an exact statement,
        // and it is the shape a "fill in a default" bug destroys.
        i64_stat("EventDate", 7, 7, 3),
        DeclaredColumnStat::new("IsRefresh", DeclaredStatType::Bool, None, None, 100)
            .expect("all-null bool stat"),
    ];
    let a = l0_record(1, hour, 100, &a_stats);
    let b = l0_record(2, hour, 100, &b_stats);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;

    // Above the watermark: the live tail, resolved by listing the commit
    // records directly. This is the population no fold-built object ever
    // covers (ADR-0873 context), so it is asserted first.
    let listing_catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let listed = resolve_all(&listing_catalog, hour, now).await;
    assert_eq!(listed.len(), 2);
    assert_eq!(
        segment_with_hash(&listed, l0_hash_byte(1))
            .declared_column_stats
            .as_slice(),
        a_stats.as_slice()
    );
    assert_eq!(
        segment_with_hash(&listed, l0_hash_byte(2))
            .declared_column_stats
            .as_slice(),
        b_stats.as_slice()
    );

    // The fold copies each record's validated stamps onto its sealed entry.
    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let entries = fold_and_read_entries(&store, &catalog, now).await;
    assert_eq!(entries.len(), 2);
    for (seq, expected) in [(1u64, &a_stats), (2u64, &b_stats)] {
        let entry = entry_with_hash(&entries, l0_hash_byte(seq));
        assert_eq!(entry.declared_column_stats.len(), expected.len());
        let read = read_snapshot_entry(entry);
        assert!(read.dropped().is_empty(), "seq {seq}");
        assert_eq!(read.covered().to_vec(), *expected, "seq {seq}");
    }

    // Below the watermark: the same segments, now served from the snapshot
    // part, carry byte-identical stamps.
    let sealed = resolve_all(&catalog, hour, now).await;
    assert_eq!(sealed.len(), 2);
    for (seq, expected) in [(1u64, &a_stats), (2u64, &b_stats)] {
        let seg = segment_with_hash(&sealed, l0_hash_byte(seq));
        assert_eq!(seg.declared_column_stats.as_slice(), expected.as_slice());
        // Field by field on one column, so a bug that preserved the list but
        // corrupted a value cannot hide behind a whole-struct comparison of
        // two equally-wrong values.
        let event_date = seg
            .declared_column_stats
            .column("EventDate")
            .expect("EventDate covered");
        assert_eq!(event_date.declared_type(), DeclaredStatType::I64);
        assert_eq!(event_date.min(), expected[0].min());
        assert_eq!(event_date.max(), expected[0].max());
        assert_eq!(event_date.null_count(), expected[0].null_count());
    }
    let all_null = segment_with_hash(&sealed, l0_hash_byte(2))
        .declared_column_stats
        .column("IsRefresh")
        .expect("IsRefresh covered");
    assert_eq!(all_null.min(), None);
    assert_eq!(all_null.max(), None);
    assert_eq!(all_null.null_count(), 100);

    // The listing and sealed paths agree, which is the property that makes the
    // stamp a single carrier rather than two.
    for seq in [1u64, 2] {
        assert_eq!(
            segment_with_hash(&listed, l0_hash_byte(seq)).declared_column_stats,
            segment_with_hash(&sealed, l0_hash_byte(seq)).declared_column_stats
        );
    }
}

/// Test 2a, absence: an unstamped record folds to an entry and resolves to a
/// ref with no stamps at all -- not a placeholder entry, not an error.
#[tokio::test]
async fn an_unstamped_record_folds_and_resolves_to_no_coverage() {
    let store = Arc::new(MemoryStore::new());
    let hour = 500_100u32;
    let now = now_at_seal(hour);
    put_l0(store.as_ref(), &l0_record(1, hour, 100, &[])).await;

    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let listed = resolve_all(&catalog, hour, now).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].declared_column_stats.len(), 0);
    assert!(listed[0].declared_column_stats.is_empty());
    assert_eq!(listed[0].declared_column_stats.column("EventDate"), None);

    let entries = fold_and_read_entries(&store, &catalog, now).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].declared_column_stats.len(), 0);
    let read = read_snapshot_entry(&entries[0]);
    assert_eq!(read.covered().len(), 0);
    assert_eq!(read.dropped().len(), 0);

    let sealed = resolve_all(&catalog, hour, now).await;
    assert_eq!(sealed.len(), 1);
    assert!(sealed[0].declared_column_stats.is_empty());
    // Uncovered from either path is one value, so a reader cannot tell the two
    // apart and no code can come to depend on the difference.
    assert_eq!(
        listed[0].declared_column_stats,
        sealed[0].declared_column_stats
    );
}

/// A `SnapshotEntry` as a fold that predates ADR-0873 encodes it: fields 1-14
/// and no field 15 in the schema at all, so its bytes cannot carry the new
/// field even accidentally.
#[derive(Clone, PartialEq, prost::Message)]
struct LegacySnapshotEntry {
    #[prost(uint32, tag = "1")]
    level: u32,
    #[prost(uint32, tag = "2")]
    shard: u32,
    #[prost(uint32, tag = "3")]
    ingest_hour_bucket: u32,
    #[prost(bytes = "vec", tag = "4")]
    writer_id: Vec<u8>,
    #[prost(uint64, tag = "5")]
    writer_epoch: u64,
    #[prost(uint64, tag = "6")]
    writer_seq: u64,
    #[prost(bytes = "vec", tag = "7")]
    content_hash: Vec<u8>,
    #[prost(uint64, tag = "8")]
    object_size: u64,
    #[prost(sint64, tag = "9")]
    min_event_ts_ns: i64,
    #[prost(sint64, tag = "10")]
    max_event_ts_ns: i64,
    #[prost(uint64, tag = "11")]
    sample_count: u64,
    #[prost(uint64, tag = "12")]
    series_count: u64,
    #[prost(uint32, tag = "13")]
    segment_format_version: u32,
    #[prost(sint64, tag = "14")]
    created_unix_ns: i64,
}

fn stamp_free_entry() -> SnapshotEntry {
    SnapshotEntry {
        level: 0,
        shard: 0,
        ingest_hour_bucket: 500_200,
        writer_id: vec![0xAA; 16],
        writer_epoch: 3,
        writer_seq: 4,
        content_hash: vec![0xBB; 32],
        object_size: 4_096,
        min_event_ts_ns: 10,
        max_event_ts_ns: 20,
        sample_count: 7,
        series_count: 2,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: 1_000,
        declared_column_stats: Vec::new(),
    }
}

/// Test 2b, the old-reader path: a snapshot part written before field 15
/// existed decodes with empty stamps, never an error.
///
/// The part is the legacy part, not an imitation of one: a repeated field with
/// no elements occupies no bytes in proto3, so the entry a current encoder
/// writes with no stamps is byte-for-byte the entry the pre-ADR-0873 schema
/// wrote, and the envelope is a pure function of those bytes. The first
/// assertion is that byte identity; without it the rest of this test would only
/// prove that an empty list round-trips.
#[test]
fn a_snapshot_part_written_before_field_fifteen_decodes_with_no_stamps() {
    let entry = stamp_free_entry();
    let legacy = LegacySnapshotEntry {
        level: entry.level,
        shard: entry.shard,
        ingest_hour_bucket: entry.ingest_hour_bucket,
        writer_id: entry.writer_id.clone(),
        writer_epoch: entry.writer_epoch,
        writer_seq: entry.writer_seq,
        content_hash: entry.content_hash.clone(),
        object_size: entry.object_size,
        min_event_ts_ns: entry.min_event_ts_ns,
        max_event_ts_ns: entry.max_event_ts_ns,
        sample_count: entry.sample_count,
        series_count: entry.series_count,
        segment_format_version: entry.segment_format_version,
        created_unix_ns: entry.created_unix_ns,
    };
    assert_eq!(
        entry.encode_length_delimited_to_vec(),
        legacy.encode_length_delimited_to_vec(),
        "an unstamped entry is byte-identical to the pre-field-15 schema's"
    );

    let encoded = encode_part(
        [0x11; 16],
        1,
        8,
        entry.ingest_hour_bucket,
        std::slice::from_ref(&entry),
    )
    .expect("encode legacy-shaped part");
    let decoded = decode_part(&encoded, &PartLimits::default()).expect("legacy part decodes");
    assert_eq!(decoded.entries.len(), 1);
    assert_eq!(decoded.entries[0].declared_column_stats.len(), 0);
    assert_eq!(decoded.entries[0], entry);
    let read = read_snapshot_entry(&decoded.entries[0]);
    assert_eq!(read.covered().len(), 0);
    assert_eq!(read.dropped().len(), 0);
    assert_eq!(read.column("EventDate"), None);
}

/// Test 3, the validation binding: a stamp that fails a row-count clause of
/// the wave-2 predicate never reaches the `SnapshotEntry`, while its valid
/// sibling on the same record survives.
///
/// Per-entry, not all-or-nothing: ADR-0873 decision 2's granularity split
/// makes the stamp entry-granular precisely so one broken writer's entry
/// cannot uncover the columns its siblings describe.
#[tokio::test]
async fn a_stamp_failing_a_row_count_clause_never_reaches_the_snapshot_entry() {
    let store = Arc::new(MemoryStore::new());
    let hour = 500_300u32;
    let now = now_at_seal(hour);

    let valid = i64_stat("EventDate", 1, 2, 6);
    // Eight NULLs in a seven-row object: clause 4. The entry is well-formed on
    // its own, which is why the predicate has to bind where a row count is
    // known.
    let invalid = i64_stat("Status", 200, 500, 8);
    let record = l0_record(1, hour, 7, &[valid.clone(), invalid]);
    // The record itself carries both: the drop happens on the copy, not by
    // refusing to publish or to decode.
    assert_eq!(record.declared_column_stats.len(), 2);
    put_l0(store.as_ref(), &record).await;

    // The listing path drops it too, before any fold has run: the same
    // predicate against the same row count, whichever carrier a resolve reads.
    let listing_catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let listed = resolve_all(&listing_catalog, hour, now).await;

    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let entries = fold_and_read_entries(&store, &catalog, now).await;
    assert_eq!(entries.len(), 1);
    // The invalid entry is absent from the sealed part outright. Asserting on
    // the raw repeated field, not on a re-read of it: a copy that carried the
    // entry through and left it to the reader to drop would keep a convicted
    // stamp on an immutable object that outlives the record convicting it.
    assert_eq!(entries[0].declared_column_stats.len(), 1);
    assert_eq!(entries[0].declared_column_stats[0].name, "EventDate");
    let read = read_snapshot_entry(&entries[0]);
    assert_eq!(read.covered().to_vec(), vec![valid.clone()]);
    assert!(read.dropped().is_empty());
    assert_eq!(read.column("Status"), None);

    for segments in [listed, resolve_all(&catalog, hour, now).await] {
        assert_eq!(segments.len(), 1);
        let stats = &segments[0].declared_column_stats;
        assert_eq!(stats.as_slice(), [valid.clone()].as_slice());
        assert_eq!(stats.column("Status"), None);
        assert_eq!(
            stats.column("EventDate").map(|s| s.null_count()),
            Some(6),
            "the surviving column keeps its exact null count"
        );
    }
}

/// Test 4: a compaction record's parts carry their own stamps
/// (`CompactionPart` field 12) through the same fold and resolve, gated
/// against the part's own row count.
#[tokio::test]
async fn compaction_part_stamps_fold_exactly_as_commit_record_stamps_do() {
    let store = Arc::new(MemoryStore::new());
    let hour = 500_400u32;
    let now = now_at_seal(hour);

    let input_a = l0_record(1, hour, 100, &[i64_stat("EventDate", -5, 19_000, 12)]);
    let input_b = l0_record(2, hour, 100, &[i64_stat("EventDate", 0, 1, 0)]);
    put_l0(store.as_ref(), &input_a).await;
    put_l0(store.as_ref(), &input_b).await;

    // Recomputed over the rows each part holds, never merged from the inputs
    // (ADR-0873 decision 3), so the part stamps deliberately differ from both
    // input stamps.
    let p0_stats = vec![
        i64_stat("EventDate", -5, 19_000, 4),
        bool_stat("IsRefresh", true, true, 0),
    ];
    let p1_stats = vec![
        DeclaredColumnStat::new("EventDate", DeclaredStatType::I64, None, None, 10)
            .expect("all-null i64 stat"),
    ];
    put_compaction_record(
        store.as_ref(),
        hour,
        &[&input_a, &input_b],
        vec![
            part(0, hour, 0x11, 10, &p0_stats),
            part(1, hour, 0x22, 10, &p1_stats),
        ],
    )
    .await;

    // Listing path: the compaction record and its parts, read live.
    let listing_catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let listed = resolve_all(&listing_catalog, hour, now).await;
    assert_eq!(listed.len(), 2, "both inputs superseded by the two parts");
    for seg in &listed {
        assert!(matches!(seg.level, SegmentLevel::L1 { .. }));
    }
    assert_eq!(
        segment_with_hash(&listed, 0x11)
            .declared_column_stats
            .as_slice(),
        p0_stats.as_slice()
    );
    assert_eq!(
        segment_with_hash(&listed, 0x22)
            .declared_column_stats
            .as_slice(),
        p1_stats.as_slice()
    );

    // Sealed path: the fold copies the part's stamps onto the level-1 entry.
    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let entries = fold_and_read_entries(&store, &catalog, now).await;
    assert_eq!(entries.len(), 2);
    for (seed, expected) in [(0x11u8, &p0_stats), (0x22u8, &p1_stats)] {
        let entry = entry_with_hash(&entries, seed);
        assert_eq!(entry.level, 1);
        assert_eq!(entry.declared_column_stats.len(), expected.len());
        let read = read_snapshot_entry(entry);
        assert!(read.dropped().is_empty(), "part {seed:#x}");
        assert_eq!(read.covered().to_vec(), *expected, "part {seed:#x}");
    }

    let sealed = resolve_all(&catalog, hour, now).await;
    assert_eq!(sealed.len(), 2);
    for (seed, expected) in [(0x11u8, &p0_stats), (0x22u8, &p1_stats)] {
        assert_eq!(
            segment_with_hash(&sealed, seed)
                .declared_column_stats
                .as_slice(),
            expected.as_slice()
        );
        assert_eq!(
            segment_with_hash(&sealed, seed).declared_column_stats,
            segment_with_hash(&listed, seed).declared_column_stats
        );
    }
    // The all-NULL part keeps both extrema absent with the exact null count:
    // the statement "this part has ten rows and none of them has EventDate",
    // which is not the same statement as "no coverage".
    let all_null = segment_with_hash(&sealed, 0x22)
        .declared_column_stats
        .column("EventDate")
        .expect("EventDate covered");
    assert_eq!(all_null.min(), None);
    assert_eq!(all_null.max(), None);
    assert_eq!(all_null.null_count(), 10);
}

/// ADR-4628: the sum and zero count a stamp carries ride the same carriage as
/// min, max and null count. A narrow sum in field 6, a wide one in field 7, and
/// zero counts on an I64 and a BOOL column fold onto the sealed entry and read
/// back off it unchanged; the sealed `SegmentRef` equals the one the live
/// listing resolves; and the raw field-15 entry shows the wide sum as 16 bytes
/// in field 7 with field 6 absent.
#[tokio::test]
async fn folded_sum_and_zero_count_reach_the_resolved_segment_ref_unchanged() {
    let store = Arc::new(MemoryStore::new());
    let hour = 500_500u32;
    let now = now_at_seal(hour);

    let stats = vec![
        i64_full("Narrow", 0, 10, 0, 500, 7),
        i64_stat("Wide", i64::MAX, i64::MAX, 0)
            .with_sum(wide_sum())
            .expect("wide sum"),
        bool_zero("Flag", false, true, 0, 40),
    ];
    let rec = l0_record(1, hour, 100, &stats);
    put_l0(store.as_ref(), &rec).await;

    // Above the watermark, resolved by listing the commit record.
    let listing_catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let listed = resolve_all(&listing_catalog, hour, now).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        segment_with_hash(&listed, l0_hash_byte(1))
            .declared_column_stats
            .as_slice(),
        stats.as_slice()
    );

    // The fold copies the validated stamps, sum and zero count included.
    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let entries = fold_and_read_entries(&store, &catalog, now).await;
    assert_eq!(entries.len(), 1);
    let entry = entry_with_hash(&entries, l0_hash_byte(1));
    let read = read_snapshot_entry(entry);
    assert!(read.dropped().is_empty());
    assert_eq!(read.covered().to_vec(), stats);

    // The raw field-15 entries: a wide sum is 16 bytes in field 7 with field 6
    // absent, a narrow one is field 6 with no field 7, and the BOOL carries a
    // zero count and no sum of any width.
    let raw = |name: &str| {
        entry
            .declared_column_stats
            .iter()
            .find(|e| e.name == name)
            .expect("column present in the raw entry")
    };
    assert_eq!(raw("Narrow").sum, Some(500));
    assert!(raw("Narrow").sum_wide.is_empty());
    assert_eq!(raw("Narrow").zero_count, Some(7));
    assert_eq!(raw("Wide").sum, None);
    assert_eq!(raw("Wide").sum_wide.len(), 16);
    assert_eq!(raw("Wide").zero_count, None);
    assert_eq!(raw("Flag").sum, None);
    assert!(raw("Flag").sum_wide.is_empty());
    assert_eq!(raw("Flag").zero_count, Some(40));

    // Below the watermark: the sealed resolve equals the live listing.
    let sealed = resolve_all(&catalog, hour, now).await;
    assert_eq!(sealed.len(), 1);
    let seg = segment_with_hash(&sealed, l0_hash_byte(1));
    assert_eq!(seg.declared_column_stats.as_slice(), stats.as_slice());
    assert_eq!(
        seg.declared_column_stats,
        segment_with_hash(&listed, l0_hash_byte(1)).declared_column_stats
    );
    // Field by field on the two sums and a zero count, so a carriage that
    // dropped one field cannot hide behind the whole-struct comparison of two
    // equally-wrong values.
    let wide = seg
        .declared_column_stats
        .column("Wide")
        .expect("Wide covered");
    assert_eq!(wide.sum(), Some(wide_sum()));
    let narrow = seg
        .declared_column_stats
        .column("Narrow")
        .expect("Narrow covered");
    assert_eq!(narrow.sum(), Some(500));
    assert_eq!(narrow.zero_count(), Some(7));
}

/// The compaction-part carrier (`CompactionPart` field 12) carries the sum and
/// zero count exactly as a commit record does, gated against the part's own row
/// count, with the wide sum again landing in field 7.
#[tokio::test]
async fn compaction_part_sum_and_zero_count_fold_exactly_as_commit_record_stamps_do() {
    let store = Arc::new(MemoryStore::new());
    let hour = 500_600u32;
    let now = now_at_seal(hour);

    let input = l0_record(1, hour, 100, &[i64_stat("Narrow", 0, 10, 0)]);
    put_l0(store.as_ref(), &input).await;

    // Recomputed over the rows the part holds, never merged from the input.
    let p_stats = vec![
        i64_full("Narrow", 0, 10, 0, 300, 5),
        i64_stat("Wide", i64::MAX, i64::MAX, 0)
            .with_sum(wide_sum())
            .expect("wide sum"),
        bool_zero("Flag", false, true, 0, 12),
    ];
    put_compaction_record(
        store.as_ref(),
        hour,
        &[&input],
        vec![part(0, hour, 0x33, 100, &p_stats)],
    )
    .await;

    let listing_catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let listed = resolve_all(&listing_catalog, hour, now).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        segment_with_hash(&listed, 0x33)
            .declared_column_stats
            .as_slice(),
        p_stats.as_slice()
    );

    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let entries = fold_and_read_entries(&store, &catalog, now).await;
    assert_eq!(entries.len(), 1);
    let entry = entry_with_hash(&entries, 0x33);
    assert_eq!(entry.level, 1);
    let read = read_snapshot_entry(entry);
    assert!(read.dropped().is_empty());
    assert_eq!(read.covered().to_vec(), p_stats);
    let raw = |name: &str| {
        entry
            .declared_column_stats
            .iter()
            .find(|e| e.name == name)
            .expect("column present in the raw entry")
    };
    assert_eq!(raw("Wide").sum, None);
    assert_eq!(raw("Wide").sum_wide.len(), 16);
    assert_eq!(raw("Narrow").sum, Some(300));
    assert_eq!(raw("Narrow").zero_count, Some(5));
    assert_eq!(raw("Flag").zero_count, Some(12));

    let sealed = resolve_all(&catalog, hour, now).await;
    assert_eq!(sealed.len(), 1);
    assert_eq!(
        segment_with_hash(&sealed, 0x33)
            .declared_column_stats
            .as_slice(),
        p_stats.as_slice()
    );
    assert_eq!(
        segment_with_hash(&sealed, 0x33).declared_column_stats,
        segment_with_hash(&listed, 0x33).declared_column_stats
    );
}

/// A snapshot entry carrying a defective aggregate -- both sum fields present
/// at once (ADR-4628 decision 2, "one encoding per value") -- is dropped whole
/// on read. The mirror must copy BOTH fields through for the predicate to see
/// the conflict: a mirror that kept only one would let the entry decode.
#[test]
fn an_entry_with_both_sum_fields_is_dropped_whole_on_read() {
    let i64_val = |v: i64| {
        Some(DeclaredColumnStatValue {
            kind: Some(Kind::I64(v)),
        })
    };
    let defective = DeclaredColumnMinMax {
        name: "Both".to_string(),
        declared_type: DeclaredStatType::I64.tag(),
        min: i64_val(0),
        max: i64_val(10),
        null_count: 0,
        sum: Some(5),
        sum_wide: vec![0u8; 16],
        zero_count: None,
    };
    let valid = DeclaredColumnMinMax {
        name: "Fine".to_string(),
        declared_type: DeclaredStatType::I64.tag(),
        min: i64_val(0),
        max: i64_val(10),
        null_count: 0,
        sum: Some(5),
        sum_wide: Vec::new(),
        zero_count: None,
    };
    let mut entry = stamp_free_entry();
    entry.declared_column_stats = vec![defective, valid];
    let read = read_snapshot_entry(&entry);
    assert_eq!(read.dropped().len(), 1);
    assert_eq!(
        read.dropped()[0].reason,
        DeclaredStatDefect::BothSumFields {
            name: "Both".to_string(),
        }
    );
    // The sibling with one sum field is unaffected: a defect is per entry.
    assert_eq!(read.covered().len(), 1);
    assert_eq!(read.covered()[0].name(), "Fine");
    assert_eq!(read.column("Both"), None);
}

/// A `DeclaredColumnMinMax` as a writer that predates ADR-4628 encodes it:
/// fields 1-5 and no field 6, 7 or 8 in the schema at all. Its min/max are the
/// unchanged `DeclaredColumnStatValue` message, so this twin shares the real
/// one.
#[derive(Clone, PartialEq, prost::Message)]
struct LegacyDeclaredColumnMinMax {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(uint32, tag = "2")]
    declared_type: u32,
    #[prost(message, optional, tag = "3")]
    min: Option<DeclaredColumnStatValue>,
    #[prost(message, optional, tag = "4")]
    max: Option<DeclaredColumnStatValue>,
    #[prost(uint64, tag = "5")]
    null_count: u64,
}

/// A stamped entry whose stamp carries only fields 1-5 (no sum, no zero count)
/// is byte-identical to the pre-ADR-4628 schema's, and decodes to the same
/// stamp with the new fields absent. Pins the bytes the current encoder writes
/// for a fixed stamp: absent fields 6-8 occupy no bytes, so an older reader's
/// record and a newer one's agree exactly.
#[test]
fn a_stamp_written_before_fields_six_to_eight_decodes_unchanged() {
    let current = DeclaredColumnMinMax {
        name: "EventDate".to_string(),
        declared_type: DeclaredStatType::I64.tag(),
        min: Some(DeclaredColumnStatValue {
            kind: Some(Kind::I64(-5)),
        }),
        max: Some(DeclaredColumnStatValue {
            kind: Some(Kind::I64(19_000)),
        }),
        null_count: 12,
        sum: None,
        sum_wide: Vec::new(),
        zero_count: None,
    };
    let legacy = LegacyDeclaredColumnMinMax {
        name: current.name.clone(),
        declared_type: current.declared_type,
        min: current.min,
        max: current.max,
        null_count: current.null_count,
    };
    assert_eq!(
        current.encode_to_vec(),
        legacy.encode_to_vec(),
        "a stamp with fields 6-8 absent is byte-identical to the pre-ADR-4628 schema's"
    );

    // Build an entry carrying it, round-trip through the part codec, and read it
    // back: the stamp is covered with min/max/null intact and no sum or zero
    // count.
    let mut entry = stamp_free_entry();
    entry.sample_count = 100;
    entry.declared_column_stats = vec![current.clone()];
    let encoded = encode_part(
        [0x44; 16],
        1,
        9,
        entry.ingest_hour_bucket,
        std::slice::from_ref(&entry),
    )
    .expect("encode part");
    let decoded = decode_part(&encoded, &PartLimits::default()).expect("part decodes");
    assert_eq!(decoded.entries.len(), 1);
    let stamp = &decoded.entries[0].declared_column_stats[0];
    assert_eq!(stamp.sum, None);
    assert!(stamp.sum_wide.is_empty());
    assert_eq!(stamp.zero_count, None);
    let read = read_snapshot_entry(&decoded.entries[0]);
    let covered = read.column("EventDate").expect("EventDate covered");
    assert_eq!(covered.min(), Some(DeclaredStatValue::I64(-5)));
    assert_eq!(covered.max(), Some(DeclaredStatValue::I64(19_000)));
    assert_eq!(covered.null_count(), 12);
    assert_eq!(covered.sum(), None);
    assert_eq!(covered.zero_count(), None);
}
