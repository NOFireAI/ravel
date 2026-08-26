//! Integration tests for the RLOG version-4 column-chunk fetcher (ADR-0699
//! decision 5, `ravel_query::BlockRangeFetcher`).
//!
//! Version 4 stores a row group's pages column-major and lists every page in
//! PAGE_DIR, so a block is no longer a contiguous byte range and the ADR-0107
//! block-range protocol does not apply to it. The fetcher instead resolves each
//! surviving `(row group, projected column)` to the byte extents of that
//! column's pages for the surviving blocks, coalesces them under the same gap
//! policy, and fetches those. These tests pin what that buys and what it costs:
//!
//! 1. A projected read issues one probe plus one range per surviving `(group,
//!    column)` and moves exactly those columns' page bytes.
//! 2. A prune arm that leaves survivors in one group reads inside that group
//!    only.
//! 3. An all-columns read of every block still crosses over to one whole-object
//!    GET at the 75% coverage threshold.
//! 4. A page-crc corruption in a fetched chunk is a typed error; a corruption in
//!    a column the projection dropped does not affect the projected read, which
//!    verifies page crcs rather than the block crc it cannot compute.
//! 5. The plan phase fetches no page byte.
//! 6. Two partitions reading the same chunk collapse onto one store GET.
//! 7. The default suffix probe covers the plan sections (footer, SKIP_IDX,
//!    PAGE_DIR) of a wide, full-row-group object, so #766's second GET is gone.
//! 8. What a chunk read decodes is what a whole-object read decodes.
//! 9. The predicate-free full-window whole-segment entry
//!    (`scan_whole_accounted_with_tenant`) takes the same chunk read when the
//!    caller projects (#790), and keeps its single whole-object GET for
//!    `ColumnSelection::all`, for a wide projection over the coverage
//!    crossover, and for a version-3 object.
//!
//! The oracle for 1-3 is PAGE_DIR itself, walked here independently of the
//! fetcher's own walk, plus an exact literal so a silent change in either shows
//! up as a number rather than as two agreeing derivations.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, LogFooter, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::record::{COL_FLAGS, COL_STREAM_REF, COL_TS};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, ColumnSelection, FieldSel, FieldType, LogRecord, LogSegError, Predicate, RlogConfig,
    RlogWriter, read_section, stream_attrs_bytes,
};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{BlockRangeFetcher, CacheFetchError, LogFetchError, LogQuery, LogSegmentFetcher};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::logstream::LogStreamId;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const CONTENT_HASH: [u8; 32] = [9u8; 32];
const KEY: &str = "logs/v4.rlog";

/// Two blocks to a row group and two records to a block, so the fixture has
/// three row groups of two blocks each and a prune arm can leave survivors in
/// exactly one of them.
const GROUP_BLOCKS: usize = 2;
const BLOCK_RECORDS: usize = 2;
const GROUPS: usize = 3;
const BLOCKS: usize = GROUPS * GROUP_BLOCKS;
const RECORDS: usize = BLOCKS * BLOCK_RECORDS;

/// Body filler per record. Large relative to every other column, so a
/// projection that drops `body` is a large byte saving and the coverage
/// crossover does not fire on it.
const BODY_BYTES: usize = 8 * 1024;

/// The declared numeric attribute every record carries: `code = <block index>`,
/// so a `NumRange` arm on it selects an exact block subset the skip index can
/// prune.
const CODE_COL: &str = "code";

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        // Must match the fetch tenant: the RLOG read path enforces a footer
        // tenant_hash check.
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Pseudo-random printable filler, so the writer's body compression cannot
/// shrink the fixture back to a size where every chunk range coalesces into one.
fn filler(seed: u64, len: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = seed;
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        out.push(ALPHABET[(z & 63) as usize] as char);
    }
    out
}

fn record(i: usize) -> LogRecord {
    let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
    let block = i / BLOCK_RECORDS;
    let ts = i as i64;
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts + 1,
        severity_num: 9,
        severity_text: if i.is_multiple_of(2) { "INFO" } else { "WARN" }.into(),
        body: filler(i as u64, BODY_BYTES),
        trace_id: None,
        span_id: None,
        // Nonzero and varying, so `flags` really carries a page in every block
        // and the projection under test is not silently selecting an absent
        // column.
        flags: (i as u32 & 7) + 1,
        attrs: vec![(CODE_COL.to_string(), AttrValue::I64(block as i64))],
    }
}

fn records() -> Vec<LogRecord> {
    (0..RECORDS).map(record).collect()
}

fn build_object(records: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(fixture_config(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish v4")
}

/// The same records in the version-3 BLOCKS layout (`header || pages` per
/// block, no PAGE_DIR), the N-1 reader's format (ADR-0066 decision 1). Written
/// through the writer's test-only version-3 exit; production writes version 4.
fn build_v3_object(records: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(fixture_config(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish_v3_for_tests().expect("finish v3")
}

fn fixture_config() -> RlogConfig {
    RlogConfig {
        block_target_records: BLOCK_RECORDS,
        group_target_blocks: GROUP_BLOCKS,
        ..RlogConfig::default()
    }
}

fn seg_ref(size: u64, records: &[LogRecord]) -> SegmentRef {
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: KEY.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: CONTENT_HASH,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

// ---- directory oracles ---------------------------------------------------

fn footer_of(bytes: &[u8]) -> LogFooter {
    footer::open(bytes).expect("footer")
}

fn section_raw(bytes: &[u8], k: u32) -> Vec<u8> {
    let f = footer_of(bytes);
    let desc = f.section(k).expect("section present");
    read_section(bytes, desc, &RlogConfig::default()).expect("section decode")
}

fn page_dir_of(bytes: &[u8]) -> PageDir {
    PageDir::decode(&section_raw(bytes, kind::PAGE_DIR)).expect("PAGE_DIR")
}

fn field_dir_of(bytes: &[u8]) -> FieldDir {
    FieldDir::decode(&section_raw(bytes, kind::FIELD_DIR), 1 << 20).expect("FIELD_DIR")
}

fn blocks_extent(bytes: &[u8]) -> (u64, u64) {
    let f = footer_of(bytes);
    let b = f.section(kind::BLOCKS).expect("BLOCKS");
    (b.offset, b.len)
}

/// Bytes after the BLOCKS section: SKIP_IDX, PAGE_DIR, BLOOM, POSTINGS, then the
/// footer and trailer. A probe suffix of exactly this length covers the whole
/// tail and reaches no page.
fn tail_len(bytes: &[u8]) -> u64 {
    let (offset, len) = blocks_extent(bytes);
    bytes.len() as u64 - (offset + len)
}

/// The absolute page extents a version-4 read must resolve for the whole-object
/// block indices `blocks` and the column ids `selected` keeps (`None` keeps
/// every column), coalesced at `gap`.
///
/// Written as an independent walk of PAGE_DIR rather than by calling the
/// fetcher's own helper, so this is an oracle and not a restatement.
fn expected_runs(
    bytes: &[u8],
    blocks: &[usize],
    selected: Option<&HashSet<u32>>,
    gap: u64,
) -> Vec<(u64, u64)> {
    let dir = page_dir_of(bytes);
    let (blocks_offset, _) = blocks_extent(bytes);
    let mut raw: Vec<(u64, u64)> = Vec::new();
    for group in &dir.groups {
        for chunk in &group.chunks {
            let keep = selected.is_none_or(|s| s.contains(&chunk.column_id));
            let mut at = chunk.offset;
            for p in &chunk.pages {
                let start = at;
                at += p.len;
                let whole = group.first_block as usize + p.block as usize;
                if keep && blocks.contains(&whole) {
                    raw.push((blocks_offset + start, p.len));
                }
            }
        }
    }
    raw.sort_by_key(|r| r.0);
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (start, len) in raw {
        if let Some(last) = out.last_mut()
            && start <= last.0 + last.1 + gap
        {
            let end = (last.0 + last.1).max(start + len);
            last.1 = end - last.0;
            continue;
        }
        out.push((start, len));
    }
    out
}

/// The `(offset, length)` byte span of one row group in the object: from its
/// first column chunk to the end of its last.
fn group_span(bytes: &[u8], group: usize) -> (u64, u64) {
    let dir = page_dir_of(bytes);
    let (blocks_offset, _) = blocks_extent(bytes);
    let g = &dir.groups[group];
    let mut start = u64::MAX;
    let mut end = 0u64;
    for c in &g.chunks {
        let (offset, len) = c.extent().expect("chunk extent");
        start = start.min(offset);
        end = end.max(offset + len);
    }
    (blocks_offset + start, end - start)
}

/// The column ids a [`ColumnSelection`] resolves to against this object's
/// FIELD_DIR: exactly what the fetcher and the decode both use.
fn resolved(bytes: &[u8], sel: &ColumnSelection) -> Option<HashSet<u32>> {
    sel.resolve(&field_dir_of(bytes))
}

// ---- store doubles -------------------------------------------------------

/// Records every `get` range so a test can assert WHERE a read landed, not only
/// how much it moved.
struct RecordingStore {
    inner: Arc<MemoryStore>,
    full: AtomicU64,
    suffix: AtomicU64,
    ranges: std::sync::Mutex<Vec<(u64, u64)>>,
}

impl RecordingStore {
    fn new(inner: Arc<MemoryStore>) -> Arc<Self> {
        Arc::new(RecordingStore {
            inner,
            full: AtomicU64::new(0),
            suffix: AtomicU64::new(0),
            ranges: std::sync::Mutex::new(Vec::new()),
        })
    }
    fn full_gets(&self) -> u64 {
        self.full.load(Ordering::SeqCst)
    }
    fn suffix_gets(&self) -> u64 {
        self.suffix.load(Ordering::SeqCst)
    }
    /// Every `[start, end)` range GET, in issue order.
    fn ranges(&self) -> Vec<(u64, u64)> {
        self.ranges.lock().expect("ranges").clone()
    }
    fn gets(&self) -> u64 {
        self.full_gets() + self.suffix_gets() + self.ranges().len() as u64
    }
}

#[async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        match range {
            GetRange::Full => {
                self.full.fetch_add(1, Ordering::SeqCst);
            }
            GetRange::Suffix(_) => {
                self.suffix.fetch_add(1, Ordering::SeqCst);
            }
            GetRange::Range(a, b) => self.ranges.lock().expect("ranges").push((a, b)),
        }
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

async fn store_with(bytes: &[u8]) -> Arc<MemoryStore> {
    let mem = Arc::new(MemoryStore::new());
    mem.put(
        KEY,
        bytes::Bytes::copy_from_slice(bytes),
        PutOptions::default(),
    )
    .await
    .expect("put");
    mem
}

/// A block-range fetcher forced onto the ranged path on this small fixture,
/// with a probe sized to exactly the object tail (so it carries the footer,
/// SKIP_IDX and PAGE_DIR and reaches no page) and no coalescing slack (so each
/// column chunk is its own GET and the range count is the chunk count).
fn ranged(store: Arc<dyn ObjectStoreBackend>, bytes: &[u8]) -> BlockRangeFetcher {
    BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len(bytes))
        .with_coalesce_gap(0)
}

fn read_cache() -> Arc<Cache<CacheFetchError>> {
    Arc::new(Cache::new(CacheLimits::new(64 << 20, 4096, 64 << 20)))
}

/// `code` in `[min, max]`, the prune-only numeric arm the SQL layer produces
/// for a declared integer column (ADR-0095 decision 6).
fn code_between(min: i64, max: i64) -> Predicate {
    Predicate::NumRange {
        field: FieldSel::Attr(CODE_COL.to_string()),
        ty: FieldType::I64,
        min: Some(min as u64),
        max: Some(max as u64),
    }
}

// ---- 1. projected read: one range per (group, column) ---------------------

/// A projected read of `k` columns over `G` row groups issues one suffix probe,
/// one GET for each of the two front sections the probe structurally cannot
/// reach, and `G * k` chunk ranges -- and moves exactly those columns' page
/// bytes, not the object.
///
/// This is the test the interim whole-object guard's
/// `version_4_object_is_read_whole_until_the_page_dir_fetcher_lands` became.
///
/// Non-vacuity: restoring the guard (`fetch_object_with_footer`'s
/// `if footer.section(kind::PAGE_DIR).is_some()` arm returning one
/// `GetRange::Full`) makes this read 1 whole-object GET of the entire object,
/// so `full_gets == 0`, the range count, and the byte assertions all fail.
#[tokio::test]
async fn version_4_projected_read_fetches_one_range_per_group_and_column() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    // ts, stream_ref and flags: three columns with unselected columns between
    // them (observed_ts between 0 and 2; severity and body between 2 and 8), so
    // no two of their chunks are adjacent and none coalesces at gap 0.
    let sel = ColumnSelection::fixed_only().with_flags();
    let ids = resolved(&bytes, &sel).expect("a projection, not all columns");
    assert_eq!(
        ids,
        HashSet::from([COL_TS, COL_STREAM_REF, COL_FLAGS]),
        "the fixture's projection is exactly three fixed columns"
    );

    let dir = page_dir_of(&bytes);
    assert_eq!(dir.groups.len(), GROUPS, "fixture row-group count");
    assert!(
        dir.groups
            .iter()
            .all(|g| g.block_count as usize == GROUP_BLOCKS),
        "every row group is full, so every group holds surviving blocks"
    );

    let all_blocks: Vec<usize> = (0..BLOCKS).collect();
    let runs = expected_runs(&bytes, &all_blocks, Some(&ids), 0);
    assert_eq!(
        runs.len(),
        GROUPS * ids.len(),
        "one contiguous run per (row group, projected column)"
    );

    let seg = seg_ref(bytes.len() as u64, &recs);
    let acc = QueryAccounting::new();
    let (got, stats) = ranged(store, &bytes)
        .fetch_object_projected(&seg, TENANT, i64::MIN, i64::MAX, &sel, &acc)
        .await
        .expect("projected fetch");

    assert!(!stats.whole_object, "the object is not read whole");
    assert_eq!(stats.probe_gets, 1, "one etag-establishing suffix probe");
    assert_eq!(
        stats.probe_misses, 0,
        "the probe covers SKIP_IDX and PAGE_DIR"
    );
    assert_eq!(
        stats.candidate_blocks, BLOCKS as u64,
        "no predicate, so every block survives"
    );
    // 9 = 3 row groups x 3 projected columns. The exact literal alongside the
    // oracle: a change in either the fixture's layout or the fetcher's walk has
    // to move this number, not just keep two derivations agreeing.
    assert_eq!(stats.block_range_gets, 9, "G*k chunk ranges");
    assert_eq!(stats.block_range_gets, runs.len() as u64);
    // STREAM_DIR and FIELD_DIR sit at the object's front; no suffix probe of
    // any length reaches them, so they are one GET each on every ranged read.
    assert_eq!(stats.metadata_gets, 2, "the two front sections");
    assert_eq!(
        recording.gets(),
        12,
        "1 probe + 2 front sections + 9 chunk ranges"
    );
    assert_eq!(recording.full_gets(), 0, "no whole-object GET");

    // The chunk GETs are exactly the oracle's runs, so the fetch landed on the
    // projected columns' pages and nothing else.
    let mut issued: Vec<(u64, u64)> = recording
        .ranges()
        .into_iter()
        .filter(|(a, b)| runs.iter().any(|(s, l)| a == s && b == &(s + l)))
        .collect();
    issued.sort_unstable();
    let mut want: Vec<(u64, u64)> = runs.iter().map(|(s, l)| (*s, s + l)).collect();
    want.sort_unstable();
    assert_eq!(issued, want, "the chunk GETs are the resolved chunk ranges");

    let chunk_bytes: u64 = runs.iter().map(|(_, l)| l).sum();
    assert_eq!(
        stats.block_bytes_fetched, chunk_bytes,
        "block_bytes_fetched is exactly the projected columns' page bytes"
    );
    // The whole point: the wire bytes track the projection, not the object.
    // 3 of ~9 columns, and the object is dominated by the unprojected `body`.
    let object = bytes.len() as u64;
    assert!(
        chunk_bytes * 20 < object,
        "the projection must move under 5% of the object: {chunk_bytes} of {object}"
    );
    assert!(
        acc.snapshot().total_s3_bytes() < object,
        "including the probe and the front sections, still under one whole-object read"
    );
    assert_eq!(got.len(), bytes.len(), "an object-sized decode buffer");
}

// ---- 2. pruned: ranges land in the surviving group only -------------------

/// A numeric prune arm whose survivors all live in one row group makes the read
/// land inside that group's byte span and nowhere else.
///
/// Non-vacuity: with the interim whole-object guard restored, the single GET is
/// `GetRange::Full` over the object, so `full_gets == 0` fails and no range GET
/// exists to check against the group span.
#[tokio::test]
async fn version_4_prune_reads_ranges_in_the_surviving_group_only() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    // `code = 0` keeps block 0 only, which is the first block of row group 0.
    let prune = vec![code_between(0, 0)];
    let sel = ColumnSelection::all();
    let runs = expected_runs(&bytes, &[0], None, 0);

    let seg = seg_ref(bytes.len() as u64, &recs);
    let acc = QueryAccounting::new();
    let (_got, stats) = ranged(store, &bytes)
        .fetch_object_with_footer(&seg, TENANT, i64::MIN, i64::MAX, &prune, &sel, None, &acc)
        .await
        .expect("pruned fetch");

    assert!(!stats.whole_object, "one of six blocks is far under 75%");
    assert_eq!(
        stats.candidate_blocks, 1,
        "the numeric arm keeps exactly block 0"
    );
    // 8 = one range per column chunk block 0 carries a page for (ts,
    // observed_ts, stream_ref, severity_num, severity_text, body, flags, code),
    // none of them coalescing at gap 0 because block 1's page for the same
    // column sits between every consecutive pair.
    assert_eq!(stats.block_range_gets, 8, "one range per chunk of group 0");
    assert_eq!(stats.block_range_gets, runs.len() as u64);
    assert_eq!(recording.full_gets(), 0, "no whole-object GET");

    let (g0_start, g0_len) = group_span(&bytes, 0);
    let (g1_start, _) = group_span(&bytes, 1);
    assert!(g1_start >= g0_start + g0_len, "the groups are disjoint");
    for (start, end) in recording.ranges() {
        // The front sections sit before BLOCKS; only the page ranges are
        // checked against the group span.
        let (blocks_offset, _) = blocks_extent(&bytes);
        if start < blocks_offset {
            continue;
        }
        assert!(
            start >= g0_start && end <= g0_start + g0_len,
            "range [{start},{end}) escaped row group 0 [{g0_start},{})",
            g0_start + g0_len
        );
    }

    let group_bytes: u64 = g0_len;
    assert!(
        stats.block_bytes_fetched < group_bytes,
        "block 0's pages ({}) are a strict subset of its group ({group_bytes})",
        stats.block_bytes_fetched
    );
}

// ---- 3. the coverage crossover ------------------------------------------

/// Selecting every column with every block surviving covers essentially all of
/// BLOCKS, so the 75% coverage crossover fires and the object is read whole in
/// one GET -- the crossover ADR-0107 introduced, preserved by the chunk path.
///
/// Non-vacuity: `with_coverage_threshold(2.0)` on the same fixture (a threshold
/// coverage cannot reach) takes the ranged branch instead, and `full_gets == 1`
/// fails with 0.
#[tokio::test]
async fn version_4_all_columns_all_blocks_crosses_over_to_one_whole_object_get() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    let (_, blocks_len) = blocks_extent(&bytes);
    let runs = expected_runs(&bytes, &(0..BLOCKS).collect::<Vec<_>>(), None, 0);
    let wanted: u64 = runs.iter().map(|(_, l)| l).sum();
    assert!(
        wanted as f64 / blocks_len as f64 >= 0.75,
        "an all-columns, all-blocks read must clear the crossover: {wanted} of {blocks_len}"
    );

    let seg = seg_ref(bytes.len() as u64, &recs);
    let acc = QueryAccounting::new();
    let (got, stats) = ranged(store, &bytes)
        .fetch_object_projected(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &ColumnSelection::all(),
            &acc,
        )
        .await
        .expect("all-columns fetch");

    assert!(stats.whole_object, "the crossover fired");
    assert_eq!(recording.full_gets(), 1, "exactly one whole-object GET");
    assert_eq!(stats.block_range_gets, 1, "counted as the one read it is");
    assert_eq!(
        stats.block_bytes_fetched,
        bytes.len() as u64,
        "the whole object"
    );
    assert_eq!(got.as_ref(), bytes.as_slice(), "and it is the object");
    // The probe plus the crossover read, and nothing in between: no front
    // section GET is paid before the decision (an all-columns query with no
    // numeric arm needs no FIELD_DIR to resolve either channel).
    assert_eq!(recording.gets(), 2, "the probe and the whole-object GET");
}

// ---- 4. checksums on a projected read ------------------------------------

/// A flipped byte inside a chunk the projection FETCHES fails that page's
/// PAGE_DIR crc32c and surfaces as a typed error, never as a decoded row.
///
/// Non-vacuity: this is the corruption the block crc used to catch. Under a
/// projected version-4 read there is no block crc to check (the reader does not
/// hold the block's other pages), so the page crc is the only thing standing
/// between a flipped byte and a wrong value. Flipping the same byte with the
/// page-crc check removed in `decode_v4_block` returns rows instead of erroring.
#[tokio::test]
async fn version_4_page_crc_corruption_in_a_fetched_chunk_is_a_typed_error() {
    let recs = records();
    let clean = build_object(&recs);
    let sel = ColumnSelection::fixed_only().with_flags();
    let ids = resolved(&clean, &sel).expect("a projection");

    // A byte inside the first page of the `flags` chunk of row group 0, which
    // the projection fetches and decodes.
    let runs = expected_runs(&clean, &[0], Some(&ids), 0);
    let flags_run = flags_chunk_range(&clean, 0);
    let target = flags_run.0;
    assert!(
        runs.iter().any(|(s, l)| target >= *s && target < s + l),
        "the corrupted byte must be inside a fetched chunk range"
    );

    let mut corrupt = clean.clone();
    corrupt[target as usize] ^= 0xff;

    let err = fetch_and_decode(&corrupt, &recs, &sel)
        .await
        .expect_err("a corrupted fetched page must not decode");
    let LogFetchError::Corrupt { source, .. } = &err else {
        panic!("expected Corrupt, got {err:?}");
    };
    let LogSegError::Corrupted(msg) = source else {
        panic!("expected Corrupted, got {source:?}");
    };
    assert!(
        msg.contains("page crc mismatch"),
        "the page crc is what caught it, got {msg}"
    );
}

/// A flipped byte in a column the projection DROPS does not affect the
/// projected read: the page is never fetched, never decoded, and the block crc
/// -- which that byte does break -- is not verified by a read that does not hold
/// every one of the block's pages (docs/log-segment-format.md, "BLOCKS").
/// An all-columns read of the same object does fail, which is what shows the
/// corruption is real rather than the flip landing on padding.
#[tokio::test]
async fn version_4_corruption_outside_the_projection_leaves_it_intact() {
    let recs = records();
    let clean = build_object(&recs);
    let sel = ColumnSelection::fixed_only().with_flags();
    let ids = resolved(&clean, &sel).expect("a projection");

    // A byte inside `body`'s chunk in row group 0: part of block 0's block crc,
    // and outside every chunk the projection keeps.
    let target = body_chunk_range(&clean, 0).0;
    let kept = expected_runs(&clean, &(0..BLOCKS).collect::<Vec<_>>(), Some(&ids), 0);
    assert!(
        !kept.iter().any(|(s, l)| target >= *s && target < s + l),
        "the corrupted byte must be outside every fetched chunk range"
    );

    let mut corrupt = clean.clone();
    corrupt[target as usize] ^= 0xff;

    let projected = fetch_and_decode(&corrupt, &recs, &sel)
        .await
        .expect("a projected read is unaffected by a dropped column's bytes");
    let baseline = fetch_and_decode(&clean, &recs, &sel)
        .await
        .expect("clean projected read");
    assert_eq!(
        projected.len(),
        RECORDS,
        "every record still comes back through the projection"
    );
    assert_eq!(
        projected, baseline,
        "and byte-identically to the same read of the uncorrupted object"
    );

    // The corruption is genuine: a read that keeps every column does hold the
    // block's pages, verifies both the page crc and the block crc, and fails.
    let err = fetch_and_decode(&corrupt, &recs, &ColumnSelection::all())
        .await
        .expect_err("an all-columns read must catch it");
    assert!(
        matches!(err, LogFetchError::Corrupt { .. }),
        "expected Corrupt, got {err:?}"
    );
}

/// The absolute `(offset, len)` of one row group's `body` column chunk.
fn body_chunk_range(bytes: &[u8], group: usize) -> (u64, u64) {
    chunk_range(bytes, group, ravel_logseg::record::COL_BODY)
}

/// The absolute `(offset, len)` of one row group's `flags` column chunk.
fn flags_chunk_range(bytes: &[u8], group: usize) -> (u64, u64) {
    chunk_range(bytes, group, COL_FLAGS)
}

fn chunk_range(bytes: &[u8], group: usize, column_id: u32) -> (u64, u64) {
    let (blocks_offset, _) = blocks_extent(bytes);
    let (offset, len) = page_dir_of(bytes)
        .chunk_range(group, column_id)
        .expect("the fixture carries this column in this group");
    (blocks_offset + offset, len)
}

/// Fetch `bytes` (as the stored object) through the version-4 chunk path with
/// `sel`, then drain the scan, so a test can assert on what a projected read
/// decodes rather than on the buffer it assembled.
async fn fetch_and_decode(
    bytes: &[u8],
    recs: &[LogRecord],
    sel: &ColumnSelection,
) -> Result<Vec<LogRecord>, LogFetchError> {
    let mem = store_with(bytes).await;
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(ranged(store, bytes));
    let seg = seg_ref(bytes.len() as u64, recs);
    let query = LogQuery::new(i64::MIN, i64::MAX);
    let acc = QueryAccounting::new();
    let mut scan = fetcher
        .scan_accounted_with_tenant(&seg, TENANT, &query, sel, &acc)
        .await?
        .expect("in range");
    let mut out = Vec::new();
    while let Some(rows) = scan.next_block()? {
        out.extend(rows);
    }
    Ok(out)
}

// ---- 5. the plan phase reads no page -------------------------------------

/// `plan_segment` on a version-4 object counts survivors from SKIP_IDX (and
/// FIELD_DIR, to resolve the arm) and fetches no page byte: the probe plus at
/// most one section GET, `page_bytes_fetched == 0`, and no block decoded.
///
/// Non-vacuity: forcing `plan_skip_decidable` to return `false` drops this query
/// onto the plan fallback, which fetches the object through the scan path; the
/// BLOCKS-overlap assertion then fails on the chunk ranges it issues, and the
/// carried footer disappears. The decode-time `page_bytes_fetched == 0` is
/// corroboration, not the guard: it is zero for any plan branch that opens no
/// cursor, which is why the read-shape assertions are checked first.
#[tokio::test]
async fn plan_segment_on_a_version_4_object_fetches_no_page_bytes() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    let fetcher = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(ranged(store, &bytes));
    let seg = seg_ref(bytes.len() as u64, &recs);
    let query = LogQuery::new(i64::MIN, i64::MAX).with_prune(code_between(0, 0));
    let acc = QueryAccounting::new();

    let (survivors, stats, footer) = fetcher
        .plan_segment(&seg, TENANT, &query, &acc)
        .await
        .expect("plan_segment")
        .expect("relevant segment");

    assert_eq!(survivors, 1, "the numeric arm keeps one block");
    assert_eq!(stats.blocks_total, BLOCKS as u32);
    assert_eq!(stats.blocks_scanned, 0, "no block decoded");
    assert_eq!(
        stats.page_bytes_fetched, 0,
        "the plan phase touches no page"
    );
    assert_eq!(stats.page_bytes_decoded, 0);
    assert_eq!(recording.full_gets(), 0, "no whole-object plan read");
    assert_eq!(
        recording.suffix_gets(),
        1,
        "one etag-establishing suffix probe"
    );
    // The read shape is what actually carries the "no page byte" claim: the
    // decode-time counters above are zero for any plan branch that opens no
    // cursor, so they are corroboration rather than the guard. Every GET this
    // made must lie outside the BLOCKS section.
    let (blocks_offset, blocks_len) = blocks_extent(&bytes);
    for (start, end) in recording.ranges() {
        assert!(
            end <= blocks_offset || start >= blocks_offset + blocks_len,
            "range [{start},{end}) reached into BLOCKS [{blocks_offset},{})",
            blocks_offset + blocks_len
        );
    }
    // The probe covers the whole tail here, so the only range GET left is
    // FIELD_DIR at the object front, which no suffix can reach.
    assert_eq!(
        recording.ranges().len(),
        1,
        "at most one section GET beyond the probe: {:?}",
        recording.ranges()
    );
    assert!(
        footer.is_some(),
        "the footer is carried forward so the scan skips its own probe"
    );
}

// ---- 6. two partitions, one chunk, one GET --------------------------------

/// Two partitions resolving the same chunk range collapse onto one store GET.
///
/// The pair is made genuinely concurrent by a [`FaultStore`] hold gate: both
/// tasks are in flight when the leader's GET is held, and the held count is
/// read at that instant. A warm-up read over a different ts window leaves the
/// probe and every section cached, so the only cold extent left is the one
/// chunk range this asserts about.
///
/// Non-vacuity: dropping `.with_cache(..)` from `br` removes the single-flight
/// entirely, and both the held count (2) and the released-GET count (2) fail.
#[tokio::test]
async fn concurrent_partitions_reading_one_chunk_collapse_onto_one_get() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let faulty = Arc::new(FaultStore::new(mem, FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;

    // `ts` and `stream_ref` with the production coalescing gap: their two chunks
    // and the `observed_ts` chunk between them fuse into ONE range per row
    // group, so the cold stage below is a single GET and "held count 1" is a
    // statement about the pair rather than about one fetch's own concurrency.
    let sel = ColumnSelection::fixed_only();
    let ids = resolved(&bytes, &sel).expect("a projection");
    let gap = ravel_query::DEFAULT_LOG_COALESCE_GAP;
    let seg = seg_ref(bytes.len() as u64, &recs);
    let cache = read_cache();
    let br = ranged(Arc::clone(&store), &bytes)
        .with_coalesce_gap(gap)
        .with_cache(Arc::clone(&cache));

    // Warm-up over row group 0 only (ts 0..=1 is block 0): admits the probe and
    // both front sections, leaving the cold window below with nothing uncached
    // but its own chunk range.
    let warm = QueryAccounting::new();
    br.fetch_object_projected(&seg, TENANT, 0, 1, &sel, &warm)
        .await
        .expect("warm-up fetch");

    // The cold window: the last block, which lives in the last row group.
    let (cold_min, cold_max) = ((RECORDS - BLOCK_RECORDS) as i64, (RECORDS - 1) as i64);
    let cold_runs = expected_runs(&bytes, &[BLOCKS - 1], Some(&ids), gap);
    assert_eq!(
        cold_runs.len(),
        1,
        "the cold stage must be exactly one chunk range: {cold_runs:?}"
    );

    let gate = faulty.hold(Op::Get, Some(KEY.to_string()), Occurrence::Always);
    let acc = QueryAccounting::new();
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let br = br.clone();
        let seg = seg.clone();
        let sel = sel.clone();
        let acc = acc.clone();
        tasks.push(tokio::spawn(async move {
            br.fetch_object_projected(&seg, TENANT, cold_min, cold_max, &sel, &acc)
                .await
                .map(|(_, stats)| stats)
        }));
    }

    // Wait for the leader's GET to be held, then leave the follower ample time
    // to arrive. With single-flight it subscribes to the leader's in-flight
    // fetch and the held count stays 1; without it, it issues its own GET for
    // the same extent and the count goes to 2.
    // Bounded: a fetch shape that issues no GET at this stage at all must fail
    // the test, not hang it.
    let mut waited = 0u32;
    while gate.held_count() == 0 {
        assert!(
            waited < 5_000,
            "no store GET was ever held: the cold window issued none"
        );
        waited += 1;
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(
        gate.held_count(),
        1,
        "both partitions want the same chunk range; only one GET is in flight"
    );

    let mut released = 0u64;
    let mut spins = 0u32;
    loop {
        for (id, _, _) in gate.held_details() {
            if gate.release(id) {
                released += 1;
            }
        }
        if tasks.iter().all(|t| t.is_finished()) && gate.held_count() == 0 {
            break;
        }
        assert!(spins < 5_000, "the fetch pair never finished draining");
        spins += 1;
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    for t in tasks {
        let stats = t.await.expect("join").expect("fetch");
        assert!(!stats.whole_object);
    }

    assert_eq!(
        released, 1,
        "one store GET for the pair, not one per partition"
    );
    assert_eq!(
        acc.snapshot().s3_requests(AccountedOp::Get),
        1,
        "and the accounting agrees"
    );
}

// ---- 7. the probe covers the plan sections -------------------------------

/// The default suffix probe covers footer + SKIP_IDX + PAGE_DIR on a
/// full-row-group, wide object, so a predicated statement pays one probe per
/// object and no second GET for the plan sections (issue #766).
///
/// The measured section sizes are printed, because the probe length is chosen
/// from them: this test is the measurement that justifies
/// `DEFAULT_LOG_SUFFIX_LEN`, not only a guard on it.
///
/// Non-vacuity: reverting `DEFAULT_LOG_SUFFIX_LEN` to the 64 KiB it was before
/// #766 makes the tail exceed the probe and both the `tail <= suffix` assertion
/// and `probe_misses == 0` fail.
#[tokio::test]
async fn probe_covers_the_plan_sections_on_a_wide_row_group_object() {
    // A full default-sized row group (32 blocks) of a 105-column object: the
    // ClickBench tenant's width at the writer's default grouping. Small blocks
    // keep the fixture cheap; PAGE_DIR is sized by the PAGE COUNT (105 columns
    // x 32 blocks x 2 groups), which is what this measures, not by the records
    // behind them.
    const COLUMNS: usize = 105;
    const WIDE_GROUP_BLOCKS: usize = 32;
    const WIDE_GROUPS: usize = 2;
    const WIDE_BLOCK_RECORDS: usize = 2;

    let cfg = RlogConfig {
        block_target_records: WIDE_BLOCK_RECORDS,
        group_target_blocks: WIDE_GROUP_BLOCKS,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity());
    let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
    let total = WIDE_GROUPS * WIDE_GROUP_BLOCKS * WIDE_BLOCK_RECORDS;
    for i in 0..total {
        let attrs: Vec<(String, AttrValue)> = (0..COLUMNS)
            .map(|c| (format!("col{c:03}"), AttrValue::I64((i * c) as i64)))
            .collect();
        w.push(LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: i as i64,
            observed_ts_ns: i as i64,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: filler(i as u64, 512),
            trace_id: None,
            span_id: None,
            flags: 1,
            attrs,
        })
        .expect("push");
    }
    let bytes = w.finish().expect("finish");

    let f = footer_of(&bytes);
    let size_of = |k: u32| f.section(k).map(|d| d.len).unwrap_or(0);
    let skip = f.section(kind::SKIP_IDX).expect("SKIP_IDX");
    let tail = bytes.len() as u64 - skip.offset;
    let dir = page_dir_of(&bytes);
    let pages: usize = dir
        .groups
        .iter()
        .flat_map(|g| g.chunks.iter())
        .map(|c| c.pages.len())
        .sum();
    eprintln!(
        "[wide v4 object] total={} blocks={} groups={} pages={} | SKIP_IDX={} PAGE_DIR={} \
         BLOOM={} POSTINGS={} footer+trailer={} | tail(SKIP_IDX..end)={} suffix={}",
        bytes.len(),
        dir.block_count(),
        dir.groups.len(),
        pages,
        size_of(kind::SKIP_IDX),
        size_of(kind::PAGE_DIR),
        size_of(kind::BLOOM),
        size_of(kind::POSTINGS),
        bytes.len() as u64
            - f.sections
                .iter()
                .map(|s| s.offset + s.len)
                .max()
                .unwrap_or(0),
        tail,
        ravel_query::DEFAULT_LOG_SUFFIX_LEN,
    );

    assert_eq!(dir.groups.len(), WIDE_GROUPS, "two full row groups");
    assert!(
        dir.groups[0].chunks.len() >= COLUMNS,
        "the fixture must really be {COLUMNS} columns wide, got {}",
        dir.groups[0].chunks.len()
    );
    assert!(
        tail <= ravel_query::DEFAULT_LOG_SUFFIX_LEN,
        "one suffix probe of {} B must cover footer + SKIP_IDX + PAGE_DIR (+ the \
         BLOOM and POSTINGS sitting between them), which is {tail} B here",
        ravel_query::DEFAULT_LOG_SUFFIX_LEN,
    );

    // And end to end: a plan-phase read of this object at production defaults
    // reports no probe miss.
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;
    let recs: Vec<LogRecord> = Vec::new();
    let _ = recs;
    let seg = SegmentRef {
        object_size: bytes.len() as u64,
        min_event_ts_ns: 0,
        max_event_ts_ns: total as i64,
        sample_count: total as u64,
        ..seg_ref(bytes.len() as u64, &[record(0)])
    };
    let acc = QueryAccounting::new();
    let (_footer, _skip, _fd, stats) = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .fetch_plan_sections(&seg, TENANT, &acc)
        .await
        .expect("plan sections");
    assert_eq!(stats.probe_gets, 1, "one probe");
    assert_eq!(
        stats.probe_misses, 0,
        "the default probe covered SKIP_IDX and PAGE_DIR"
    );
    assert!(
        recording.ranges().len() <= 1,
        "at most one section GET beyond the probe, and only ever FIELD_DIR at \
         the object front: {:?}",
        recording.ranges()
    );
}

// ---- 8. the chunk read decodes what a whole-object read decodes ------------

/// The differential: for every projection, what the chunk path decodes equals
/// what a whole-object read of the same object decodes, record for record.
///
/// Non-vacuity: with the interim whole-object guard restored both sides are the
/// same whole-object read and the test cannot fail; it is meaningful only
/// because the left side now fetches a strict subset of the object's bytes,
/// which the byte assertion states.
#[tokio::test]
async fn version_4_chunk_read_decodes_what_a_whole_object_read_decodes() {
    let recs = records();
    let bytes = build_object(&recs);
    let store: Arc<dyn ObjectStoreBackend> = store_with(&bytes).await;
    let seg = seg_ref(bytes.len() as u64, &recs);
    let query = LogQuery::new(i64::MIN, i64::MAX);

    // `narrow` says whether the selection drops the object's dominant column
    // (`body`, which is 8 KiB per record against a few bytes for everything
    // else). Only a narrow selection is expected to move fewer bytes than the
    // object: a selection that keeps `body` covers most of BLOCKS, so the 75%
    // coverage crossover fires and the read is a probe plus one whole-object
    // GET, which is MORE bytes than a plain whole-object read.
    for (sel, narrow) in [
        (ColumnSelection::all(), false),
        (ColumnSelection::fixed_only(), true),
        (ColumnSelection::fixed_only().with_flags(), true),
        (
            ColumnSelection::fixed_only()
                .with_body()
                .with_severity_num(),
            false,
        ),
        (ColumnSelection::fixed_only().with_attr(CODE_COL), true),
        (ColumnSelection::fixed_only().with_all_attrs(), true),
    ] {
        // Whole object in one GET, the pre-ADR-0699 read shape.
        let whole = LogSegmentFetcher::new(Arc::clone(&store));
        let mut want = Vec::new();
        let mut scan = whole
            .scan_accounted_with_tenant(&seg, TENANT, &query, &sel, &QueryAccounting::new())
            .await
            .expect("whole scan")
            .expect("in range");
        while let Some(rows) = scan.next_block().expect("decode") {
            want.extend(rows);
        }

        let acc = QueryAccounting::new();
        let chunked = LogSegmentFetcher::new(Arc::clone(&store))
            .with_block_range_threshold(0)
            .with_block_range(ranged(Arc::clone(&store), &bytes));
        let mut got = Vec::new();
        let mut scan = chunked
            .scan_accounted_with_tenant(&seg, TENANT, &query, &sel, &acc)
            .await
            .expect("chunk scan")
            .expect("in range");
        while let Some(rows) = scan.next_block().expect("decode") {
            got.extend(rows);
        }

        assert_eq!(got.len(), RECORDS, "every record comes back");
        assert_eq!(got, want, "chunk read == whole-object read for {sel:?}");
        if narrow {
            let moved = acc.snapshot().total_s3_bytes();
            assert!(
                moved < bytes.len() as u64,
                "a projected chunk read moves fewer bytes than the object \
                 ({moved} vs {}) for {sel:?}",
                bytes.len()
            );
        }
    }
}

// ---- 9. the predicate-free whole-segment fast path projects too (#790) -----

/// The stored length of one front directory section.
fn section_len(bytes: &[u8], k: u32) -> u64 {
    footer_of(bytes).section(k).expect("section present").len
}

/// A fetcher whose whole-segment entry takes the ranged path on this small
/// fixture: the probe covers exactly the object tail and nothing coalesces, so
/// every GET count below is a statement about the read shape.
fn whole_segment_fetcher(store: Arc<dyn ObjectStoreBackend>, bytes: &[u8]) -> LogSegmentFetcher {
    LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(ranged(store, bytes))
}

/// Drains a scan opened by the whole-segment entry and returns `(ts, stream,
/// flags)` per row, the fields a `flags` projection can speak for.
fn projected_rows(rows: &[LogRecord]) -> Vec<(i64, LogStreamId, u32)> {
    rows.iter()
        .map(|r| (r.ts_ns, r.stream_id, r.flags))
        .collect()
}

/// The predicate-free full-window whole-segment path over a version-4 object
/// reads through PAGE_DIR when the caller projects: one suffix probe, one GET
/// for each of the two front sections no suffix reaches, and one range per
/// surviving `(row group, projected column)` -- and it moves those columns'
/// page bytes plus that fixed overhead, not the object.
///
/// Before #790 this entry called `whole_object_bytes` unconditionally, so it
/// was the one read shape ADR-0699's chunk fetcher never reached, on exactly
/// the statements with the widest byte-to-projection gap.
///
/// Non-vacuity: restoring the old body (`let bytes = self
/// .whole_object_bytes(seg_ref, tenant_hash, accounting).await?;` in
/// `LogSegmentFetcher::scan_whole_accounted_with_tenant`) makes this one
/// `GetRange::Full` of the whole object: `suffix_gets` is 0, `full_gets` is 1,
/// the range count is 0, and the byte total is the object.
#[tokio::test]
async fn whole_segment_fast_path_projects_a_version_4_object_through_page_dir() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    // One queried column (`flags`) plus the two the format always decodes.
    let sel = ColumnSelection::fixed_only().with_flags();
    let ids = resolved(&bytes, &sel).expect("a projection, not all columns");
    assert_eq!(ids, HashSet::from([COL_TS, COL_STREAM_REF, COL_FLAGS]));
    let all_blocks: Vec<usize> = (0..BLOCKS).collect();
    let runs = expected_runs(&bytes, &all_blocks, Some(&ids), 0);

    let fetcher = whole_segment_fetcher(store, &bytes);
    let seg = seg_ref(bytes.len() as u64, &recs);
    // The window contains the segment and carries no predicate: the shape
    // `ravel_sql::logs_scan` proves before it takes this entry.
    let query = LogQuery::new(i64::MIN, i64::MAX);
    let acc = QueryAccounting::new();
    let mut scan = fetcher
        .scan_whole_accounted_with_tenant(&seg, TENANT, &query, &sel, &acc)
        .await
        .expect("whole-segment scan")
        .expect("in range");
    let mut got = Vec::new();
    while let Some(rows) = scan.next_block().expect("decode") {
        got.extend(rows);
    }
    assert_eq!(got.len(), RECORDS, "every record of the segment comes back");

    assert_eq!(recording.full_gets(), 0, "no whole-object GET");
    assert_eq!(
        recording.suffix_gets(),
        1,
        "one etag-establishing suffix probe per segment"
    );
    // 9 = 3 row groups x 3 projected columns, plus STREAM_DIR and FIELD_DIR at
    // the object front. The exact literal beside the oracle: a change in the
    // fixture's layout or in the fetcher's walk has to move this number.
    assert_eq!(runs.len(), 9, "one run per (row group, projected column)");
    assert_eq!(
        recording.ranges().len(),
        11,
        "9 chunk ranges + 2 front sections: {:?}",
        recording.ranges()
    );
    assert_eq!(
        recording.gets(),
        12,
        "1 probe + 2 front sections + 9 ranges"
    );

    let mut issued: Vec<(u64, u64)> = recording
        .ranges()
        .into_iter()
        .filter(|(a, b)| runs.iter().any(|(s, l)| a == s && b == &(s + l)))
        .collect();
    issued.sort_unstable();
    let mut want: Vec<(u64, u64)> = runs.iter().map(|(s, l)| (*s, s + l)).collect();
    want.sort_unstable();
    assert_eq!(issued, want, "the chunk GETs are the resolved chunk ranges");

    // The exact byte figure: the projected columns' page bytes plus the fixed
    // probe and front-section overhead, and nothing else.
    let chunk_bytes: u64 = runs.iter().map(|(_, l)| l).sum();
    let overhead = tail_len(&bytes)
        + section_len(&bytes, kind::STREAM_DIR)
        + section_len(&bytes, kind::FIELD_DIR);
    let moved = acc.snapshot().total_s3_bytes();
    assert_eq!(
        moved,
        chunk_bytes + overhead,
        "the read moves the projected pages ({chunk_bytes}) plus the probe and front \
         sections ({overhead}) and nothing else"
    );
    let object = bytes.len() as u64;
    assert!(
        moved * 4 < object,
        "the whole-segment read must now move well under a quarter of the object: \
         {moved} of {object}"
    );
}

/// What the projected whole-segment read decodes is what the whole-object read
/// decodes: every row, and the projected column's value on each, by value.
///
/// Non-vacuity: with the pre-#790 body restored the two sides are the same
/// whole-object read and the comparison cannot fail; it is meaningful because
/// the left side now fetches a strict subset of the object's bytes, which the
/// byte assertion states.
#[tokio::test]
async fn whole_segment_projected_scan_decodes_what_the_whole_object_scan_decodes() {
    let recs = records();
    let bytes = build_object(&recs);
    let store: Arc<dyn ObjectStoreBackend> = store_with(&bytes).await;
    let seg = seg_ref(bytes.len() as u64, &recs);
    let query = LogQuery::new(i64::MIN, i64::MAX);
    let sel = ColumnSelection::fixed_only().with_flags();

    // The unchanged whole-object read of the same entry (production defaults:
    // the fixture sits below the block-range threshold).
    let whole = LogSegmentFetcher::new(Arc::clone(&store));
    let whole_acc = QueryAccounting::new();
    let mut want = Vec::new();
    let mut scan = whole
        .scan_whole_accounted_with_tenant(&seg, TENANT, &query, &sel, &whole_acc)
        .await
        .expect("whole scan")
        .expect("in range");
    while let Some(rows) = scan.next_block().expect("decode") {
        want.extend(rows);
    }

    let acc = QueryAccounting::new();
    let chunked = whole_segment_fetcher(Arc::clone(&store), &bytes);
    let mut got = Vec::new();
    let mut scan = chunked
        .scan_whole_accounted_with_tenant(&seg, TENANT, &query, &sel, &acc)
        .await
        .expect("chunk scan")
        .expect("in range");
    while let Some(rows) = scan.next_block().expect("decode") {
        got.extend(rows);
    }

    assert_eq!(got, want, "projected chunk read == whole-object read");
    // Pinned by value, not only against the other read: `flags` is
    // `(i & 7) + 1` and `ts` is `i`, for all 12 records in order.
    let expected: Vec<(i64, LogStreamId, u32)> = (0..RECORDS)
        .map(|i| {
            let r = record(i);
            (r.ts_ns, r.stream_id, r.flags)
        })
        .collect();
    assert_eq!(expected.len(), RECORDS);
    assert_eq!(projected_rows(&got), expected, "every row, by value");
    assert!(
        acc.snapshot().total_s3_bytes() < whole_acc.snapshot().total_s3_bytes(),
        "the projected read moved fewer bytes than the whole-object read: {} vs {}",
        acc.snapshot().total_s3_bytes(),
        whole_acc.snapshot().total_s3_bytes()
    );
}

/// `ColumnSelection::all` keeps the pre-#790 read exactly: one whole-object GET,
/// no probe, no range.
#[tokio::test]
async fn whole_segment_all_columns_reads_the_object_whole() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    let fetcher = whole_segment_fetcher(store, &bytes);
    let seg = seg_ref(bytes.len() as u64, &recs);
    let query = LogQuery::new(i64::MIN, i64::MAX);

    // Both spellings of "every column": `ColumnSelection::all`, and the one the
    // SQL surface actually produces for a full projection, which NAMES each
    // fixed column and takes the all-attributes channel
    // (`ravel_sql::logs_scan::resolve_columns`) and so answers `is_all() ==
    // false`. Routing on `is_all()` alone would put every SQL statement,
    // however wide, on the chunk read.
    let named_every_column = ColumnSelection::fixed_only()
        .with_observed_ts()
        .with_severity_num()
        .with_severity_text()
        .with_body()
        .with_trace_id()
        .with_span_id()
        .with_flags()
        .with_all_attrs();
    assert!(
        !named_every_column.is_all(),
        "the SQL-shaped full projection is not `all()`; that is the point"
    );

    for (label, sel) in [
        ("all()", ColumnSelection::all()),
        ("named every column", named_every_column),
    ] {
        let before = recording.gets();
        let acc = QueryAccounting::new();
        let mut scan = fetcher
            .scan_whole_accounted_with_tenant(&seg, TENANT, &query, &sel, &acc)
            .await
            .expect("whole-segment scan")
            .expect("in range");
        let mut rows = 0usize;
        while let Some(block) = scan.next_block().expect("decode") {
            rows += block.len();
        }

        assert_eq!(rows, RECORDS, "{label}");
        assert_eq!(recording.gets() - before, 1, "{label}: exactly one GET");
        assert_eq!(recording.suffix_gets(), 0, "{label}: no probe, ever");
        assert!(recording.ranges().is_empty(), "{label}: no range GET");
        assert_eq!(
            acc.snapshot().total_s3_bytes(),
            bytes.len() as u64,
            "{label}: exactly the object"
        );
    }
    assert_eq!(recording.full_gets(), 2, "one whole-object GET each");
}

/// A projection wide enough to cross the 75% coverage threshold reads the
/// object whole: the crossover applies to this entry exactly as it does to the
/// striped one, so `SELECT *`-shaped statements keep their single GET.
#[tokio::test]
async fn whole_segment_wide_projection_crosses_over_to_one_whole_object_get() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    // `body` is 8 KiB per record against a few bytes for every other column, so
    // keeping it covers well over 75% of the BLOCKS section.
    let sel = ColumnSelection::fixed_only().with_body();
    let ids = resolved(&bytes, &sel).expect("a projection, not all columns");
    let runs = expected_runs(&bytes, &(0..BLOCKS).collect::<Vec<_>>(), Some(&ids), 0);
    let wanted: u64 = runs.iter().map(|(_, l)| l).sum();
    let (_, blocks_len) = blocks_extent(&bytes);
    assert!(
        wanted as f64 / blocks_len as f64 >= 0.75,
        "the fixture must put this projection over the crossover: {wanted} of {blocks_len}"
    );

    let fetcher = whole_segment_fetcher(store, &bytes);
    let seg = seg_ref(bytes.len() as u64, &recs);
    let query = LogQuery::new(i64::MIN, i64::MAX);
    let acc = QueryAccounting::new();
    let mut scan = fetcher
        .scan_whole_accounted_with_tenant(&seg, TENANT, &query, &sel, &acc)
        .await
        .expect("whole-segment scan")
        .expect("in range");
    let mut rows = 0usize;
    while let Some(block) = scan.next_block().expect("decode") {
        rows += block.len();
    }

    assert_eq!(rows, RECORDS);
    assert_eq!(recording.full_gets(), 1, "the crossover reads the object");
    assert_eq!(
        recording.suffix_gets(),
        1,
        "the probe that resolved PAGE_DIR"
    );
    assert_eq!(
        recording.ranges().len(),
        1,
        "FIELD_DIR only, to resolve the projection: {:?}",
        recording.ranges()
    );
}

/// A version-3 object has no PAGE_DIR, so its blocks are contiguous and a
/// projection is a decode choice only: the candidate set covers the BLOCKS
/// section, the coverage crossover fires, and the object is read whole in one
/// `GetRange::Full` on the same `(0, object_size)` key as before.
///
/// The suffix probe in front of it is the version discovery: the trailer is the
/// only place the format version is written, so no caller can route on it
/// without reading the object's tail. That is one extra cache-routed GET per
/// object per query on a version-3 object, and no extra whole-object read.
#[tokio::test]
async fn whole_segment_on_a_version_3_object_reads_the_object_whole() {
    let recs = records();
    let bytes = build_v3_object(&recs);
    assert!(
        footer_of(&bytes).section(kind::PAGE_DIR).is_none(),
        "the fixture must be a version-3 object"
    );
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    let fetcher = whole_segment_fetcher(store, &bytes);
    let seg = seg_ref(bytes.len() as u64, &recs);
    let query = LogQuery::new(i64::MIN, i64::MAX);
    let acc = QueryAccounting::new();
    let mut scan = fetcher
        .scan_whole_accounted_with_tenant(
            &seg,
            TENANT,
            &query,
            &ColumnSelection::fixed_only().with_flags(),
            &acc,
        )
        .await
        .expect("whole-segment scan")
        .expect("in range");
    let mut got = Vec::new();
    while let Some(rows) = scan.next_block().expect("decode") {
        got.extend(rows);
    }

    assert_eq!(got.len(), RECORDS, "every record of the segment comes back");
    assert_eq!(
        recording.full_gets(),
        1,
        "the object is still read whole, exactly once"
    );
    assert_eq!(
        recording.suffix_gets(),
        1,
        "one probe, which is what reads the version"
    );
    assert!(
        recording.ranges().is_empty(),
        "no section GET: the probe covers the tail: {:?}",
        recording.ranges()
    );
    assert_eq!(
        acc.snapshot().total_s3_bytes(),
        bytes.len() as u64 + tail_len(&bytes),
        "the whole object plus the probe suffix"
    );
}
