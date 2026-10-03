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
//! 9. A tail-section probe miss is counted exactly once per object per read
//!    path, by whichever layer issued the probe (#883, issue #885 review).
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
use ravel_logseg::encoding::Enc;
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
use ravel_query::{
    BlockRangeFetcher, CacheFetchError, CarriedFooter, LogFetchError, LogQuery, LogSegmentFetcher,
    MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT, PhaseWireByteCounts, QueryPhase, ReadPhases,
};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting, QueryAccountingSnapshot};
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
    let cfg = RlogConfig {
        block_target_records: BLOCK_RECORDS,
        group_target_blocks: GROUP_BLOCKS,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish v4")
}

fn seg_ref(size: u64, records: &[LogRecord]) -> SegmentRef {
    seg_ref_at(size, records, SegmentLevel::L0)
}

/// The same object as [`seg_ref`] describes, catalogued as one part of a
/// compacted L1 bucket instead of an L0 flush.
fn l1_seg_ref(size: u64, records: &[LogRecord]) -> SegmentRef {
    seg_ref_at(
        size,
        records,
        SegmentLevel::L1 {
            input_set_hash: [3u8; 32],
            part_index: 0,
        },
    )
}

fn seg_ref_at(size: u64, records: &[LogRecord], level: SegmentLevel) -> SegmentRef {
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
        level,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
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
            let read = |p: &ravel_logseg::page_dir::PageEntry| {
                blocks.contains(&(group.first_block as usize + p.block as usize))
            };
            // A row-group dictionary page is fetched with any kept block of
            // its chunk (ADR-2135 decision 6).
            let chunk_read = chunk
                .pages
                .iter()
                .any(|p| p.enc != Enc::DictPage && read(p));
            let mut at = chunk.offset;
            for p in &chunk.pages {
                let start = at;
                at += p.len;
                let wanted = if p.enc == Enc::DictPage {
                    chunk_read
                } else {
                    read(p)
                };
                if keep && wanted {
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

/// Mirrors `fetcher::bound_runs` exactly (ADR-2066 decision 1): bridges the
/// `runs.len() - max_runs` smallest gaps between sorted, non-overlapping
/// `(start, end)` runs, ties bridging the earlier gap first. Written as an
/// independent copy of the algorithm rather than by calling the fetcher's own
/// `pub(crate)` helper (unreachable from an integration test), so this is an
/// oracle and not a restatement.
fn bound_runs_oracle(runs: &[(u64, u64)], max_runs: usize) -> Vec<(u64, u64)> {
    let max_runs = max_runs.max(1);
    if runs.len() <= max_runs {
        return runs.to_vec();
    }
    let mut gaps: Vec<(u64, usize)> = runs
        .windows(2)
        .enumerate()
        .map(|(i, pair)| (pair[1].0.saturating_sub(pair[0].1), i))
        .collect();
    gaps.sort_unstable();
    let mut bridged = vec![false; gaps.len()];
    for (_, i) in gaps.into_iter().take(runs.len() - max_runs) {
        bridged[i] = true;
    }
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(max_runs);
    let mut bridge_previous = false;
    for (run, bridge_next) in runs.iter().copied().zip(bridged.into_iter().chain([false])) {
        match out.last_mut() {
            Some(last) if bridge_previous => last.1 = last.1.max(run.1),
            _ => out.push(run),
        }
        bridge_previous = bridge_next;
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

// ---- 1a. one row group, narrow projection: 3 GETs total --------------------

/// ADR-2066 decision 1's headline number: a narrow projection of a
/// one-row-group object costs at most 3 GETs -- probe, front sections, one
/// run -- because `ts`/`stream_ref`'s chunks and the unselected `observed_ts`
/// page between them coalesce (at the production default gap, not the
/// zero-slack harness gap the other tests in this file use) into one run, and
/// STREAM_DIR+FIELD_DIR are one combined-span GET.
///
/// Non-vacuity: before `place_and_decode_field_dir` was changed to call
/// `place_front_sections` (this task's predecessor commit), FIELD_DIR alone
/// was fetched on the narrow-projection path and STREAM_DIR followed
/// separately once the coverage decision needed it, so `metadata_gets` read 2
/// and the total read 4, not 3.
#[tokio::test]
async fn version_4_narrow_projection_of_one_row_group_object_costs_three_gets() {
    // One row group of every block this fixture has: `group_target_blocks`
    // set to the block count folds every block into a single group.
    const ONE_GROUP_BLOCKS: usize = BLOCKS;
    let recs = records();
    let cfg = RlogConfig {
        block_target_records: BLOCK_RECORDS,
        group_target_blocks: ONE_GROUP_BLOCKS,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity());
    for r in &recs {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish v4");

    let dir = page_dir_of(&bytes);
    assert_eq!(dir.groups.len(), 1, "every block folds into one row group");

    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    // `ts` and `stream_ref` (the two always-decoded fixed columns) at the
    // production coalescing gap: their two chunks and the `observed_ts` chunk
    // between them fuse into one run per row group (see
    // `concurrent_partitions_reading_one_chunk_collapse_onto_one_get`'s
    // comment for the same fusion).
    let sel = ColumnSelection::fixed_only();
    let ids = resolved(&bytes, &sel).expect("a projection, not all columns");
    let gap = ravel_query::DEFAULT_LOG_COALESCE_GAP;
    let all_blocks: Vec<usize> = (0..BLOCKS).collect();
    let runs = expected_runs(&bytes, &all_blocks, Some(&ids), gap);
    assert_eq!(
        runs.len(),
        1,
        "ts, observed_ts and stream_ref's pages fuse into one run: {runs:?}"
    );

    let seg = seg_ref(bytes.len() as u64, &recs);
    let acc = QueryAccounting::new();
    let fetcher = BlockRangeFetcher::new(Arc::clone(&store))
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len(&bytes))
        .with_coalesce_gap(gap);
    let (_got, stats) = fetcher
        .fetch_object_projected(&seg, TENANT, i64::MIN, i64::MAX, &sel, &acc)
        .await
        .expect("projected fetch");

    assert!(!stats.whole_object, "the object is not read whole");
    assert_eq!(stats.probe_gets, 1, "one etag-establishing suffix probe");
    assert_eq!(
        stats.probe_misses, 0,
        "the probe covers SKIP_IDX and PAGE_DIR"
    );
    assert_eq!(stats.block_range_gets, 1, "the one fused chunk run");
    assert_eq!(
        stats.metadata_gets, 1,
        "STREAM_DIR and FIELD_DIR in one combined-span GET"
    );
    assert_eq!(
        recording.gets(),
        3,
        "1 probe + 1 front-section GET + 1 chunk run"
    );
    assert_eq!(recording.full_gets(), 0, "no whole-object GET");
}

// ---- 1b. multiple row groups: chunk runs cap at 4, bridging smallest gaps --

/// A projected read of `k` columns over `G` row groups raises `G * k`
/// candidate chunk runs, but ADR-2066 decision 1 caps the GETs issued for them
/// at `MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT` (4): the smallest gaps between
/// candidate runs are bridged first, exactly as the metrics path's
/// `bound_runs` already does for an L0 segment's page ranges (`fetcher.rs`).
///
/// This is the test the interim whole-object guard's
/// `version_4_object_is_read_whole_until_the_page_dir_fetcher_lands` became,
/// and the test that pinned "1 probe + 2 front sections + 9 chunk ranges"
/// before decision 1 landed.
///
/// Non-vacuity: restoring the guard (`fetch_object_with_footer`'s
/// `if footer.section(kind::PAGE_DIR).is_some()` arm returning one
/// `GetRange::Full`) makes this read 1 whole-object GET of the entire object,
/// so `full_gets == 0`, the range count, and the byte assertions all fail.
#[tokio::test]
async fn version_4_projected_read_bridges_chunk_runs_to_the_four_get_cap() {
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
    let raw_runs = expected_runs(&bytes, &all_blocks, Some(&ids), 0);
    assert_eq!(
        raw_runs.len(),
        GROUPS * ids.len(),
        "one contiguous run per (row group, projected column), before bridging"
    );
    // 9 = 3 row groups x 3 projected columns. The exact literal alongside the
    // oracle: a change in either the fixture's layout or the fetcher's walk has
    // to move this number, not just keep two derivations agreeing.
    assert_eq!(raw_runs.len(), 9, "9 candidate runs before bridging");
    assert!(
        raw_runs.len() > MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT,
        "the fixture must actually exceed the cap for this test to mean anything"
    );

    let raw_ends: Vec<(u64, u64)> = raw_runs.iter().map(|(s, l)| (*s, s + l)).collect();
    let bridged = bound_runs_oracle(&raw_ends, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT);
    assert_eq!(
        bridged.len(),
        MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT,
        "9 candidate runs bridge down to exactly the cap"
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
    assert_eq!(
        stats.block_range_gets, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT as u64,
        "9 candidate runs cap at 4 chunk-run GETs"
    );
    // STREAM_DIR and FIELD_DIR sit at the object's front; no suffix probe of any
    // length reaches them. On this narrow-projection path FIELD_DIR is needed
    // early to resolve the projection, and `place_front_sections` fetches every
    // not-yet-resident front section in one combined-span GET, so STREAM_DIR
    // rides along with it instead of following separately after the coverage
    // decision.
    assert_eq!(stats.metadata_gets, 1, "the two front sections, one GET");
    assert_eq!(
        recording.gets(),
        1 + 1 + MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT as u64,
        "1 probe + 1 front-section GET + 4 bridged chunk-run GETs"
    );
    assert_eq!(recording.full_gets(), 0, "no whole-object GET");

    // The chunk GETs are exactly the bridged runs, not the raw per-column runs:
    // bridging fetches some bytes the query does not need (the ADR's traded-off
    // cost) in exchange for capping the request count.
    let mut issued: Vec<(u64, u64)> = recording
        .ranges()
        .into_iter()
        .filter(|(a, b)| bridged.iter().any(|(s, e)| a == s && b == e))
        .collect();
    issued.sort_unstable();
    let mut want = bridged.clone();
    want.sort_unstable();
    assert_eq!(
        issued, want,
        "the chunk GETs are exactly the bridged runs, the smallest gaps merged in"
    );

    // Which gaps survive unbridged: the run count drops from 9 to 4, so 5 of the
    // 8 gaps between the 9 raw runs are bridged. Assert the SURVIVING gaps
    // (the 3 largest) are exactly the raw-run boundaries still present, byte for
    // byte, in `bridged`'s own internal joins -- i.e. the bridging kept the
    // largest gaps as real breaks and merged the smallest ones away.
    let raw_gaps: Vec<u64> = raw_ends
        .windows(2)
        .map(|w| w[1].0.saturating_sub(w[0].1))
        .collect();
    let mut sorted_gaps = raw_gaps.clone();
    sorted_gaps.sort_unstable();
    // 8 gaps, 5 bridged (9 runs -> 4), 3 survive as real breaks between the
    // bridged output's own runs.
    assert_eq!(raw_gaps.len(), 8, "8 gaps between 9 raw runs");
    let surviving_as_breaks = bridged.len() - 1;
    assert_eq!(surviving_as_breaks, 3, "3 breaks survive between 4 runs");
    let smallest_five: u64 = sorted_gaps[..5].iter().sum();
    let largest_three: u64 = sorted_gaps[5..].iter().sum();
    assert!(
        smallest_five <= largest_three || sorted_gaps[4] <= sorted_gaps[5],
        "the 5 bridged gaps must be no larger than the 3 surviving ones: {sorted_gaps:?}"
    );

    let chunk_bytes: u64 = bridged.iter().map(|(s, e)| e - s).sum();
    assert_eq!(
        stats.block_bytes_fetched, chunk_bytes,
        "block_bytes_fetched is exactly the bridged runs' bytes, including the \
         bridged slack"
    );
    let raw_chunk_bytes: u64 = raw_runs.iter().map(|(_, l)| l).sum();
    assert!(
        chunk_bytes >= raw_chunk_bytes,
        "bridging can only add bytes, never drop needed ones: {chunk_bytes} < {raw_chunk_bytes}"
    );
    // The whole point: even with bridging slack, the wire bytes still track the
    // projection far more than the object, which is dominated by `body`.
    let object = bytes.len() as u64;
    assert!(
        chunk_bytes * 10 < object,
        "the bridged projection must still move under 10% of the object: \
         {chunk_bytes} of {object}"
    );
    assert!(
        acc.snapshot().total_s3_bytes() < object,
        "including the probe and the front sections, still under one whole-object read"
    );
    assert_eq!(
        got.len(),
        bytes.len(),
        "the source spans the whole object, however few of its bytes are placed"
    );
}

// ---- 1c. the chunk-run cap binds L0 only -----------------------------------

/// What one narrow projected read of `bytes` at `seg`'s level issued: its
/// stats, every range GET that landed in BLOCKS (sorted), the whole-object GET
/// count and the total GET count.
async fn projected_read_at(
    bytes: &[u8],
    seg: &SegmentRef,
    coverage_threshold: Option<f64>,
) -> (ravel_query::BlockRangeStats, Vec<(u64, u64)>, u64, u64) {
    let mem = store_with(bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;
    let mut fetcher = ranged(store, bytes);
    if let Some(t) = coverage_threshold {
        fetcher = fetcher.with_coverage_threshold(t);
    }
    let sel = ColumnSelection::fixed_only().with_flags();
    let acc = QueryAccounting::new();
    let (_got, stats) = fetcher
        .fetch_object_projected(seg, TENANT, i64::MIN, i64::MAX, &sel, &acc)
        .await
        .expect("projected fetch");
    let (blocks_offset, _) = blocks_extent(bytes);
    let mut chunk_ranges: Vec<(u64, u64)> = recording
        .ranges()
        .into_iter()
        .filter(|(start, _)| *start >= blocks_offset)
        .collect();
    chunk_ranges.sort_unstable();
    (stats, chunk_ranges, recording.full_gets(), recording.gets())
}

/// The chunk-run cap is an L0 bound (ADR-2066 decision 1), as it is on the
/// metrics path (`fetch_pages` exempts L1): the object and projection of
/// `version_4_projected_read_bridges_chunk_runs_to_the_four_get_cap`,
/// catalogued as an L1 part, issue one GET per coalesced run with no gap
/// bridged, while the same bytes catalogued as an L0 flush issue exactly
/// `MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT`.
///
/// Non-vacuity: with the cap applied regardless of level, or gated on an
/// object-size threshold above this fixture's size instead of on the level,
/// the L1 read bridges to 4 runs and the L1 run-count assertion fails.
#[tokio::test]
async fn version_4_l1_part_issues_every_chunk_run_while_l0_caps_at_four() {
    let recs = records();
    let bytes = build_object(&recs);
    let sel = ColumnSelection::fixed_only().with_flags();
    let ids = resolved(&bytes, &sel).expect("a projection, not all columns");
    let all_blocks: Vec<usize> = (0..BLOCKS).collect();
    let raw_ends: Vec<(u64, u64)> = expected_runs(&bytes, &all_blocks, Some(&ids), 0)
        .into_iter()
        .map(|(s, l)| (s, s + l))
        .collect();
    assert_eq!(
        raw_ends.len(),
        9,
        "3 row groups x 3 projected columns, none adjacent at gap 0"
    );
    let bridged = bound_runs_oracle(&raw_ends, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT);
    assert_eq!(bridged.len(), 4, "the L0 cap bridges 9 runs to 4");

    let l1 = l1_seg_ref(bytes.len() as u64, &recs);
    let (stats, chunk_ranges, full_gets, gets) = projected_read_at(&bytes, &l1, None).await;
    assert!(!stats.whole_object, "L1: the projection stays ranged");
    assert_eq!(full_gets, 0, "L1: no whole-object GET");
    assert_eq!(
        stats.block_range_gets, 9,
        "L1: one chunk-run GET per coalesced run, none bridged"
    );
    assert_eq!(
        chunk_ranges, raw_ends,
        "L1: the chunk GETs are exactly the raw coalesced runs"
    );
    assert_eq!(gets, 1 + 1 + 9, "L1: probe + front sections + 9 runs");
    let raw_bytes: u64 = raw_ends.iter().map(|(s, e)| e - s).sum();
    assert_eq!(
        stats.block_bytes_fetched, raw_bytes,
        "L1: no gap byte is bridged"
    );

    let l0 = seg_ref(bytes.len() as u64, &recs);
    let (stats, chunk_ranges, full_gets, gets) = projected_read_at(&bytes, &l0, None).await;
    assert!(!stats.whole_object, "L0: the projection stays ranged");
    assert_eq!(full_gets, 0, "L0: no whole-object GET");
    assert_eq!(
        stats.block_range_gets, 4,
        "L0: 9 runs bridged to MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT"
    );
    assert_eq!(
        chunk_ranges, bridged,
        "L0: the chunk GETs are the bridged runs"
    );
    assert_eq!(gets, 1 + 1 + 4, "L0: probe + front sections + 4 runs");
}

/// The coverage crossover sizes an L1 read on the runs that read would
/// issue, unbridged, not on the L0-bridged set: with a threshold between the
/// two coverages, the L1 part stays ranged while the same bytes as an L0 flush
/// cross over to one whole-object GET.
///
/// Non-vacuity: exempting L1 in `bounded_chunk_runs` but still bridging to 4
/// in `bridged_run_bytes` makes the L1 read cross over, and its
/// `whole_object` assertion fails.
#[tokio::test]
async fn version_4_l1_crossover_is_sized_on_its_unbridged_runs() {
    let recs = records();
    let bytes = build_object(&recs);
    let sel = ColumnSelection::fixed_only().with_flags();
    let ids = resolved(&bytes, &sel).expect("a projection, not all columns");
    let all_blocks: Vec<usize> = (0..BLOCKS).collect();
    let raw_ends: Vec<(u64, u64)> = expected_runs(&bytes, &all_blocks, Some(&ids), 0)
        .into_iter()
        .map(|(s, l)| (s, s + l))
        .collect();
    let bridged = bound_runs_oracle(&raw_ends, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT);
    let raw_bytes: u64 = raw_ends.iter().map(|(s, e)| e - s).sum();
    let bridged_bytes: u64 = bridged.iter().map(|(s, e)| e - s).sum();
    assert!(
        raw_bytes < bridged_bytes,
        "bridging must add gap bytes for the threshold to fit between: \
         {raw_bytes} vs {bridged_bytes}"
    );
    let (_, blocks_len) = blocks_extent(&bytes);
    let threshold = (raw_bytes + bridged_bytes) as f64 / 2.0 / blocks_len as f64;

    let l1 = l1_seg_ref(bytes.len() as u64, &recs);
    let (stats, _, full_gets, _) = projected_read_at(&bytes, &l1, Some(threshold)).await;
    assert!(
        !stats.whole_object,
        "L1: the unbridged runs' coverage ({raw_bytes} of {blocks_len}) is under \
         the threshold, so the read stays ranged"
    );
    assert_eq!(full_gets, 0, "L1: no whole-object GET");
    assert_eq!(stats.block_range_gets, 9, "L1: the 9 unbridged runs");

    let l0 = seg_ref(bytes.len() as u64, &recs);
    let (stats, _, full_gets, _) = projected_read_at(&bytes, &l0, Some(threshold)).await;
    assert!(
        stats.whole_object,
        "L0: the bridged runs' coverage ({bridged_bytes} of {blocks_len}) clears \
         the threshold, so the read crosses over"
    );
    assert_eq!(full_gets, 1, "L0: one whole-object GET");
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
        .fetch_object_with_footer(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &prune,
            &sel,
            None,
            ReadPhases::SCAN,
            &acc,
        )
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
    // column sits between every consecutive pair. ADR-2066 decision 1 caps
    // the GETs issued for them at MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT (4),
    // bridging the smallest gaps first.
    assert_eq!(runs.len(), 8, "8 candidate runs before bridging");
    assert!(
        runs.len() > MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT,
        "the fixture must actually exceed the cap for this test to mean anything"
    );
    let raw_ends: Vec<(u64, u64)> = runs.iter().map(|(s, l)| (*s, s + l)).collect();
    let bridged = bound_runs_oracle(&raw_ends, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT);
    assert_eq!(
        bridged.len(),
        MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT,
        "8 candidate runs bridge down to exactly the cap"
    );
    assert_eq!(
        stats.block_range_gets, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT as u64,
        "8 candidate runs cap at 4 chunk-run GETs"
    );
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
    assert_eq!(
        got.as_whole().map(AsRef::as_ref),
        Some(bytes.as_slice()),
        "and it is the object, whole"
    );
    // The probe plus the crossover read, and nothing in between: no front
    // section GET is paid before the decision (an all-columns query with no
    // numeric arm needs no FIELD_DIR to resolve either channel).
    assert_eq!(recording.gets(), 2, "the probe and the whole-object GET");
}

/// A narrow projection whose OWN selected bytes stay far under the covering
/// threshold still crosses over to one whole-object GET once the coalescing
/// gap fuses its candidate runs into a span that does: ADR-2066 decision 1
/// computes the 75% crossover against the coalesced/bounded run set, not the
/// raw wanted extents, so bridging (or, as here, plain coalescing) can push a
/// narrow read over the line by itself.
///
/// Non-vacuity: computing the crossover against the raw per-chunk sum instead
/// (`stored_decoded`, asserted below to be far under the threshold on its
/// own) would keep this read on the ranged path, and `stats.whole_object`
/// would be `false`.
#[tokio::test]
async fn version_4_projection_whose_bridged_runs_cross_the_threshold_becomes_one_whole_object_get()
{
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    let sel = ColumnSelection::fixed_only().with_flags();
    let ids = resolved(&bytes, &sel).expect("a projection, not all columns");
    let all_blocks: Vec<usize> = (0..BLOCKS).collect();
    let (_, blocks_len) = blocks_extent(&bytes);

    let stored_decoded = expected_stored_page_bytes(&bytes, &all_blocks, Some(&ids));
    assert!(
        (stored_decoded as f64) < 0.75 * blocks_len as f64,
        "the projection's own selected bytes stay far under the covering \
         threshold on their own: {stored_decoded} of {blocks_len}"
    );

    let hole_runs = expected_runs(&bytes, &all_blocks, Some(&ids), HOLE_GAP);
    assert_eq!(
        hole_runs.len(),
        1,
        "HOLE_GAP fuses the projection into one run"
    );
    let fused: u64 = hole_runs.iter().map(|(_, l)| l).sum();
    assert!(
        fused as f64 / blocks_len as f64 >= 0.75,
        "the fused run must itself clear the crossover: {fused} of {blocks_len}"
    );

    let seg = seg_ref(bytes.len() as u64, &recs);
    let acc = QueryAccounting::new();
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len(&bytes))
        .with_coalesce_gap(HOLE_GAP);
    let (got, stats) = block_range
        .fetch_object_projected(&seg, TENANT, i64::MIN, i64::MAX, &sel, &acc)
        .await
        .expect("projected fetch");

    assert!(
        stats.whole_object,
        "the coalesced run's own coverage clears the threshold, so the \
         crossover fires despite the narrow projection"
    );
    assert_eq!(recording.full_gets(), 1, "exactly one whole-object GET");
    assert_eq!(stats.block_range_gets, 1, "counted as the one read it is");
    assert_eq!(
        stats.block_bytes_fetched,
        bytes.len() as u64,
        "the whole object"
    );
    assert_eq!(
        got.as_whole().map(AsRef::as_ref),
        Some(bytes.as_slice()),
        "and it is the object, whole"
    );
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

    let (survivors, _dirs, stats, footer, _whole_object) = fetcher
        .plan_segment(&seg, TENANT, &query, &acc)
        .await
        .expect("plan_segment")
        .expect("relevant segment");

    assert_eq!(survivors.len(), 1, "the numeric arm keeps one block");
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

/// The skip-decidable plan path's own directory decompression lands in the
/// PLAN phase, not phase-less or pooled away: `plan_segment` decodes the four
/// directories (STREAM_DIR, FIELD_DIR, SKIP_IDX, PAGE_DIR) once and carries
/// them to every later open of the segment (ADR-2414 decision A1), and all four
/// are always zstd (the writer stores every whole-read directory section
/// COMP_ZSTD unconditionally, docs/log-segment-format.md), so a caller
/// threading a [`PhaseAccounting`]'s `plan()` handle through must see the exact
/// sum of their `uncomp_len` show up under `snapshot().plan.decompressed_bytes`
/// (issue #1401 finding 2).
#[tokio::test]
async fn plan_segment_charges_directory_decompression_to_the_plan_phase() {
    let recs = records();
    let bytes = build_object(&recs);
    let store: Arc<dyn ObjectStoreBackend> = store_with(&bytes).await;

    let f = footer_of(&bytes);
    let mut expected = 0u64;
    for k in [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::SKIP_IDX,
        kind::PAGE_DIR,
    ] {
        let desc = *f.section(k).expect("section present");
        assert_eq!(
            desc.comp,
            footer::COMP_ZSTD,
            "fixture section {k} must be zstd for this test"
        );
        expected += desc.uncomp_len;
    }

    let fetcher = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(ranged(store, &bytes));
    let seg = seg_ref(bytes.len() as u64, &recs);
    let query = LogQuery::new(i64::MIN, i64::MAX).with_prune(code_between(0, 0));
    let phase = ravel_query::PhaseAccounting::new();

    let (survivors, _dirs, _stats, _footer, _whole_object) = fetcher
        .plan_segment(&seg, TENANT, &query, phase.plan())
        .await
        .expect("plan_segment")
        .expect("relevant segment");
    assert_eq!(survivors.len(), 1, "the numeric arm keeps one block");

    let snap = phase.snapshot();
    assert_eq!(
        snap.plan.decompressed_bytes, expected,
        "plan_segment's skip-decidable path charges exactly STREAM_DIR + \
         FIELD_DIR + SKIP_IDX + PAGE_DIR's zstd uncomp_len to the plan phase"
    );
}

/// The plan FALLBACK (a predicate SKIP_IDX cannot decide: here a `has_word`
/// content arm) opens the reader over the fetched object to count survivors.
/// That open decodes STREAM_DIR, FIELD_DIR, SKIP_IDX and PAGE_DIR, and those
/// bytes ride in the reader's own `ScanStats`, not on any handle, so
/// `plan_segment` must charge them to the plan phase itself: the exact sum of
/// the four sections' `uncomp_len` (each zstd on this fixture), and nothing on
/// the scan phase, since counting survivors decodes no block.
///
/// Read whole on purpose: with the block-range threshold at `u64::MAX` the
/// fallback's `tenant_bytes` is one whole-object GET with no fetcher-side
/// directory decode, so the reader's open is the only decompression there is
/// and the oracle is exactly those four sections.
#[tokio::test]
async fn plan_segment_fallback_charges_the_reader_open_to_the_plan_phase() {
    let recs = records();
    let bytes = build_object(&recs);
    let store: Arc<dyn ObjectStoreBackend> = store_with(&bytes).await;

    let f = footer_of(&bytes);
    let mut expected = 0u64;
    for k in [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::SKIP_IDX,
        kind::PAGE_DIR,
    ] {
        let desc = *f.section(k).expect("section present");
        assert_eq!(
            desc.comp,
            footer::COMP_ZSTD,
            "fixture section {k} must be zstd for this test"
        );
        expected += desc.uncomp_len;
    }

    let fetcher = LogSegmentFetcher::new(Arc::clone(&store)).with_block_range_threshold(u64::MAX);
    let seg = seg_ref(bytes.len() as u64, &recs);
    // A body word the filler alphabet cannot spell as a token: a bloom-only
    // arm SKIP_IDX cannot decide, which is what sends `plan_segment` down the
    // fallback.
    let query = LogQuery::new(i64::MIN, i64::MAX).with_content(Predicate::HasWord {
        field: FieldSel::Body,
        word: "no.such.word".into(),
    });
    let phase = ravel_query::PhaseAccounting::new();

    let (_survivors, _dirs, _stats, carried_footer, _whole_object) = fetcher
        .plan_segment(&seg, TENANT, &query, phase.plan())
        .await
        .expect("plan_segment")
        .expect("relevant segment");
    assert!(
        carried_footer.is_none(),
        "the fallback carries no footer forward"
    );

    let snap = phase.snapshot();
    assert_eq!(
        snap.plan.decompressed_bytes, expected,
        "the fallback plan read charges exactly the reader's four directory \
         decodes to the plan phase"
    );
    assert_eq!(
        snap.scan.decompressed_bytes, 0,
        "counting survivors decodes no block, so the scan phase stays at zero"
    );
}

/// The plan phase caches FIELD_DIR under its own per-section cache key
/// (`plan_section_raw` -> `cached_extent`, keyed on `(tenant, content_hash,
/// desc.offset, desc.len)`); a narrow-projection scan on the SAME fetcher and
/// cache resolves FIELD_DIR through `place_and_decode_field_dir` ->
/// `place_front_sections`, which must peek that exact key and find it
/// resident rather than folding FIELD_DIR into a fresh combined-span GET.
///
/// Non-vacuity: reverting `place_front_sections` to always fetch the
/// STREAM_DIR+FIELD_DIR span without first checking the per-section cache key
/// makes the scan phase issue a range GET that overlaps FIELD_DIR even though
/// the plan phase already cached it, and the range-disjointness assertion
/// below fails.
#[tokio::test]
async fn plan_phase_field_dir_cache_is_reused_by_a_narrow_scan() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;

    let f = footer_of(&bytes);
    let field_dir = *f.section(kind::FIELD_DIR).expect("FIELD_DIR section");

    let cache = read_cache();
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(ranged(Arc::clone(&store), &bytes))
        .with_cache(Arc::clone(&cache));
    let seg = seg_ref(bytes.len() as u64, &recs);

    // Plan phase: a skip-decidable numeric arm, which resolves through
    // FIELD_DIR and caches it under `plan_section_raw`'s own per-section key.
    let plan_query = LogQuery::new(i64::MIN, i64::MAX).with_prune(code_between(0, 0));
    let plan_acc = QueryAccounting::new();
    let (survivors, _dirs, _stats, _footer, _whole_object) = fetcher
        .plan_segment(&seg, TENANT, &plan_query, &plan_acc)
        .await
        .expect("plan_segment")
        .expect("relevant segment");
    assert_eq!(survivors.len(), 1, "the numeric arm keeps one block");

    let after_plan = recording.ranges().len();
    assert!(
        after_plan > 0,
        "the plan phase must issue at least the section GET that caches \
         FIELD_DIR: {:?}",
        recording.ranges()
    );

    // Scan phase: a narrow projection on the SAME fetcher/cache, which needs
    // FIELD_DIR early (through `place_and_decode_field_dir` ->
    // `place_front_sections`) to resolve the projected column ids.
    let sel = ColumnSelection::fixed_only().with_flags();
    assert!(!sel.is_all(), "a narrow projection, not all columns");
    let scan_query = LogQuery::new(i64::MIN, i64::MAX);
    let scan_acc = QueryAccounting::new();
    let mut scan = fetcher
        .scan_accounted_with_tenant(&seg, TENANT, &scan_query, &sel, &scan_acc)
        .await
        .expect("scan")
        .expect("in range");
    let mut rows = 0usize;
    while let Some(block) = scan.next_block().expect("decode") {
        rows += block.len();
    }
    assert_eq!(rows, RECORDS, "every record decoded");

    let scan_ranges: Vec<(u64, u64)> = recording.ranges()[after_plan..].to_vec();
    for (start, end) in &scan_ranges {
        assert!(
            *end <= field_dir.offset || *start >= field_dir.offset + field_dir.len,
            "scan-phase range [{start},{end}) re-fetches FIELD_DIR [{},{}) \
             even though the plan phase already cached it: \
             place_front_sections must check the per-section cache key \
             before folding FIELD_DIR into a combined-span GET",
            field_dir.offset,
            field_dir.offset + field_dir.len
        );
    }
    assert!(
        scan_acc.snapshot().cache_hits > 0,
        "the scan phase must record at least one cache hit (FIELD_DIR, \
         served from the plan phase's cache entry)"
    );
}

/// The combined STREAM_DIR+FIELD_DIR GET admits each section under its own
/// per-section cache key, the key `place_front_sections` peeks: a second read
/// of the same object through the same cached fetcher, with a fresh
/// assembler, serves both front sections from those peeks.
///
/// The first read is cold: it misses the probe, the front span and the 4
/// bridged chunk runs (6 read-through misses on the query's accounting), and
/// the cache's own counters also carry the two per-section peeks that found
/// nothing (8). The second read issues no GET and hits the probe, the two
/// sections and the 4 runs (7 hits, 0 misses on either counter).
///
/// Non-vacuity: admitting under the span's key only (the pre-change code)
/// leaves the second read at 6 hits (the span once, not the two sections) and
/// 2 cache-counter misses; admitting FIELD_DIR's key only makes the second
/// read re-fetch STREAM_DIR live.
#[tokio::test]
async fn combined_front_get_admits_each_section_under_its_own_key() {
    let recs = records();
    let bytes = build_object(&recs);
    let mem = store_with(&bytes).await;
    let recording = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;
    let cache = read_cache();
    let fetcher = ranged(store, &bytes).with_cache(Arc::clone(&cache));
    let seg = seg_ref(bytes.len() as u64, &recs);
    let sel = ColumnSelection::fixed_only().with_flags();

    let before = cache.metrics().snapshot();
    let first = QueryAccounting::new();
    let (_got, stats) = fetcher
        .fetch_object_projected(&seg, TENANT, i64::MIN, i64::MAX, &sel, &first)
        .await
        .expect("first fetch");
    let after_first = cache.metrics().snapshot();
    assert_eq!(
        stats.metadata_gets, 1,
        "first read: STREAM_DIR and FIELD_DIR in one combined GET"
    );
    assert_eq!(
        stats.block_range_gets, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT as u64,
        "first read: 4 bridged chunk runs"
    );
    assert_eq!(
        recording.gets(),
        1 + 1 + 4,
        "first read: probe + front + 4 runs"
    );
    let first_snap = first.snapshot();
    assert_eq!(
        first_snap.cache_misses, 6,
        "first read: probe + front span + 4 runs, each one read-through miss"
    );
    assert_eq!(first_snap.cache_hits, 0, "first read: nothing resident");
    assert_eq!(
        after_first.misses - before.misses,
        8,
        "first read, cache counters: the 6 read-through misses plus the two \
         per-section peeks"
    );
    assert_eq!(after_first.hits - before.hits, 0, "first read: no hit");

    let gets_before_second = recording.gets();
    let second = QueryAccounting::new();
    let (_got, stats) = fetcher
        .fetch_object_projected(&seg, TENANT, i64::MIN, i64::MAX, &sel, &second)
        .await
        .expect("second fetch");
    let after_second = cache.metrics().snapshot();
    assert_eq!(
        recording.gets(),
        gets_before_second,
        "second read: every extent is resident, no GET"
    );
    assert_eq!(stats.metadata_gets, 0, "second read: no front-section GET");
    assert_eq!(
        stats.block_cache_hits, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT as u64,
        "second read: the 4 runs hit"
    );
    let second_snap = second.snapshot();
    assert_eq!(
        second_snap.cache_hits,
        1 + 2 + 4,
        "second read: the probe, STREAM_DIR and FIELD_DIR under their own \
         keys, and the 4 runs"
    );
    assert_eq!(second_snap.cache_misses, 0, "second read: no miss recorded");
    assert_eq!(
        after_second.hits - after_first.hits,
        7,
        "second read, cache counters: the same 7 hits"
    );
    assert_eq!(
        after_second.misses - after_first.misses,
        0,
        "second read, cache counters: the section peeks no longer miss"
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

/// The ceiling suffix probe (`DEFAULT_LOG_SUFFIX_LEN`) covers footer + SKIP_IDX
/// + PAGE_DIR on a full-row-group, wide object, so a predicated statement over a
/// production-sized object of this width (where the #883 derivation reaches the
/// ceiling) pays one probe per object and no second GET for the plan sections
/// (issue #766).
///
/// The measured section sizes are printed, because the ceiling is chosen from
/// them: this test is the measurement that justifies `DEFAULT_LOG_SUFFIX_LEN` as
/// the derivation's ceiling, not only a guard on it. The end-to-end read pins
/// the ceiling because the cheap fixture is byte-small (its size-derived probe
/// would be the floor); see the comment at that call site.
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

    // And end to end: a plan-phase read of this object at the CEILING probe
    // reports no probe miss. Since #883 the default suffix is derived from the
    // object size (`derive_suffix_len`), and this fixture is deliberately
    // byte-small but section-wide (2-record blocks), so its size-derived probe
    // is the floor, not the ceiling this test is about. A production wide object
    // of this width carries far more records per block and so is many MB, where
    // the derivation reaches `DEFAULT_LOG_SUFFIX_LEN`; the pin below reproduces
    // that ceiling on the cheap fixture, which is what makes the `tail <=
    // DEFAULT_LOG_SUFFIX_LEN` measurement above an end-to-end no-miss guarantee
    // rather than a bare byte comparison.
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
    let (_footer, _dirs, stats) = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(ravel_query::DEFAULT_LOG_SUFFIX_LEN)
        .fetch_plan_directories(&seg, TENANT, &acc)
        .await
        .expect("plan directories");
    assert_eq!(stats.probe_gets, 1, "one probe");
    assert_eq!(
        stats.probe_misses, 0,
        "the ceiling probe covered SKIP_IDX and PAGE_DIR"
    );
    assert!(
        recording.ranges().len() <= 1,
        "at most one section GET beyond the probe, and only ever the front pair \
         (STREAM_DIR and FIELD_DIR) at the object front: {:?}",
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

// ---- 9. a tail miss is counted once, by whoever issued the probe ----------

/// A probe pinned to start exactly at PAGE_DIR's end: it covers the footer and
/// the trailer (so there is no footer chase) but neither of the two tail
/// sections a version-4 read locates pages through, so the derived window costs
/// this object one extra request and `probe_misses` must report exactly 2.
///
/// Returns the object bytes and that suffix.
fn probe_missing_both_tail_sections() -> (Vec<u8>, u64) {
    let bytes = build_object(&records());
    let total = bytes.len() as u64;
    let f = footer_of(&bytes);
    let skip = f.section(kind::SKIP_IDX).expect("SKIP_IDX");
    let page = f.section(kind::PAGE_DIR).expect("PAGE_DIR");
    let suffix = total - (page.offset + page.len);
    let probe_start = total - suffix;
    assert!(
        skip.offset < probe_start && page.offset < probe_start,
        "the pinned probe must reach neither SKIP_IDX ({}) nor PAGE_DIR ({}), \
         but starts at {probe_start}",
        skip.offset,
        page.offset
    );
    (bytes, suffix)
}

/// A scan that issued its own probe counts its own tail misses: no footer is
/// carried, so this read is the only layer that probed the object and the two
/// uncovered tail sections are its own.
///
/// This is case 1 of the three-way, and it is the case the gates never changed.
/// It is here as the control for the two below: if this ever stops reporting 2,
/// the fixture stopped reaching the miss-counting site and the other two tests
/// are vacuous rather than passing.
///
/// Prove-the-test: the count is exact, so it fails in both directions. Widening
/// the pinned suffix to `tail_len(&bytes)` (a probe that covers both sections)
/// drops it to 0; deleting either `stats.probe_misses += 1` in
/// `fetch_object_v4` drops it to 1.
#[tokio::test]
async fn a_scan_that_probed_counts_its_own_tail_misses() {
    let (bytes, suffix) = probe_missing_both_tail_sections();
    let mem = store_with(&bytes).await;
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let seg = seg_ref(bytes.len() as u64, &records());
    let acc = QueryAccounting::new();

    let (_got, stats) = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(suffix)
        .fetch_object_with_footer(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &[],
            &ColumnSelection::all(),
            None,
            ReadPhases::SCAN,
            &acc,
        )
        .await
        .expect("fetch");

    assert_eq!(
        stats.probe_gets, 1,
        "the probe covered the footer, no chase"
    );
    assert_eq!(
        stats.probe_misses, 2,
        "this read probed, so it counts SKIP_IDX and PAGE_DIR itself"
    );
}

/// A scan handed a footer whose plan read counted NOTHING about the tail
/// sections still counts them: `plan_segment_fast` calls `fetch_footer`, which
/// reads the footer alone, so the probe that was too short to reach SKIP_IDX and
/// PAGE_DIR costs THIS read the extra request and no other layer has counted it.
///
/// This is case 3, the one the previous commit's `plan_footer.is_none()` /
/// `probed` gates silently dropped.
///
/// Prove-the-test: demonstrated failing against the pre-fix code by restoring
/// the gate at `crates/ravel-query/src/log_fetcher.rs`'s `if count_tail_misses`
/// in `fetch_object_v4` to `if probed`, which is `plan_footer.is_none()` and so
/// false here: `probe_misses` then reads 0 against the expected 2.
#[tokio::test]
async fn a_carried_footer_that_counted_nothing_leaves_the_scan_to_count() {
    let (bytes, suffix) = probe_missing_both_tail_sections();
    let mem = store_with(&bytes).await;
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let seg = seg_ref(bytes.len() as u64, &records());
    let acc = QueryAccounting::new();

    // What `plan_segment_fast` does: read the footer and nothing else. It
    // reports no tail miss, which is the whole reason the scan must.
    let fetcher = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(suffix);
    let (footer, plan_stats) = fetcher
        .fetch_footer(&seg, TENANT, &acc)
        .await
        .expect("footer-only plan read");
    assert_eq!(
        plan_stats.probe_misses, 0,
        "fetch_footer reads no tail section, so it counts no tail miss"
    );

    let (_got, stats) = fetcher
        .fetch_object_with_footer(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &[],
            &ColumnSelection::all(),
            Some(CarriedFooter {
                footer: &footer,
                tail_misses_counted: false,
            }),
            ReadPhases::SCAN,
            &acc,
        )
        .await
        .expect("fetch");

    assert_eq!(
        stats.probe_misses, 2,
        "nobody counted these two sections yet, so this read does"
    );
    assert_eq!(
        plan_stats.probe_misses + stats.probe_misses,
        2,
        "exactly once across the plan and the scan"
    );
}

/// A scan handed a footer whose plan read ALREADY counted the tail sections
/// counts nothing: `fetch_plan_directories` runs `ensure_tail_plan_sections`,
/// which counted both, and counting them again here would report one object's
/// too-short probe as two.
///
/// This is case 2. The sum across the two phases is the assertion that matters:
/// it is 2 whether the counting happens in the plan or in the scan, and it is 4
/// if both count.
///
/// Prove-the-test: demonstrated failing by deleting the `count_tail_misses`
/// gate at `crates/ravel-query/src/log_fetcher.rs`'s `if count_tail_misses` in
/// `fetch_object_v4` (the double-count the previous commit fixed): the scan
/// then reports 2 and the sum reads 4.
#[tokio::test]
async fn a_carried_footer_that_already_counted_is_not_counted_again() {
    let (bytes, suffix) = probe_missing_both_tail_sections();
    let mem = store_with(&bytes).await;
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let seg = seg_ref(bytes.len() as u64, &records());
    let acc = QueryAccounting::new();

    let fetcher = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(suffix);
    let (footer, _dirs, plan_stats) = fetcher
        .fetch_plan_directories(&seg, TENANT, &acc)
        .await
        .expect("plan directories");
    assert_eq!(
        plan_stats.probe_misses, 2,
        "the plan read located both tail sections through a window that reached \
         neither, and counted them"
    );

    let (_got, stats) = fetcher
        .fetch_object_with_footer(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &[],
            &ColumnSelection::all(),
            Some(CarriedFooter {
                footer: &footer,
                tail_misses_counted: true,
            }),
            ReadPhases::SCAN,
            &acc,
        )
        .await
        .expect("fetch");

    assert_eq!(
        stats.probe_misses, 0,
        "the plan phase already counted this object's tail misses"
    );
    assert_eq!(
        plan_stats.probe_misses + stats.probe_misses,
        2,
        "exactly once across the plan and the scan, not twice"
    );
}

// ---- 5. fetch amplification: WIRE bytes over STORED decoded page bytes ----
//
// Issue #913 T1. Two quantities appear below and they are not comparable:
//
//   WIRE bytes    what a store GET transferred, from
//                 `PhaseWireByteCounts` (`QueryPhase::Scan` is the
//                 BLOCKS-section data ranges).
//   STORED bytes  post-compression page lengths from PAGE_DIR, from
//                 `QueryAccountingSnapshot::page_bytes_decoded`.
//
// Fetch amplification is the first divided by the second. Nothing here sums
// one with the other.

/// Coalescing gap wide enough to fuse every projected chunk of the fixture into
/// one run, so the run spans the unwanted columns' pages lying between them.
/// Those bytes crossed the wire and belong in the numerator.
const HOLE_GAP: u64 = 1 << 20;

/// Sum of the STORED page lengths a projection decodes: an independent walk of
/// PAGE_DIR, in the same units `DecodedBlock::page_bytes_decoded` reports.
/// This is the denominator of fetch amplification.
///
/// A row-group dictionary page (ADR-2135 decision 6) is read by every block of
/// its chunk, so it counts once per decoded block with a page in that chunk.
fn expected_stored_page_bytes(
    bytes: &[u8],
    blocks: &[usize],
    selected: Option<&HashSet<u32>>,
) -> u64 {
    let dir = page_dir_of(bytes);
    let mut total = 0u64;
    for group in &dir.groups {
        for chunk in &group.chunks {
            if !selected.is_none_or(|s| s.contains(&chunk.column_id)) {
                continue;
            }
            let mut readers = HashSet::new();
            for p in chunk.pages.iter().filter(|p| p.enc != Enc::DictPage) {
                let whole = group.first_block as usize + p.block as usize;
                if blocks.contains(&whole) {
                    total += p.len;
                    readers.insert(whole);
                }
            }
            if let Some(d) = chunk.dict_page() {
                total += d.len * readers.len() as u64;
            }
        }
    }
    total
}

/// Sum of the STORED page lengths of EVERY page present in the decoded blocks,
/// whatever the projection: the quantity `page_bytes_fetched` reports. On a
/// version-4 object no read moves these bytes, which is the whole point.
fn expected_present_page_bytes(bytes: &[u8], blocks: &[usize]) -> u64 {
    expected_stored_page_bytes(bytes, blocks, None)
}

/// One narrow-projection scan of the fixture, drained to exhaustion, with the
/// block-range fetcher's coalescing gap pinned to `gap`.
///
/// Returns this execution's per-phase WIRE bytes, its accounting snapshot
/// (whose `page_bytes_*` are STORED page bytes), and the rows it decoded.
async fn amplification_scan(
    bytes: &[u8],
    recs: &[LogRecord],
    sel: &ColumnSelection,
    gap: u64,
) -> (PhaseWireByteCounts, QueryAccountingSnapshot, usize) {
    let store: Arc<dyn ObjectStoreBackend> = store_with(bytes).await;
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len(bytes))
        .with_coalesce_gap(gap);
    let fetcher = LogSegmentFetcher::new(store)
        .with_block_range(block_range)
        .with_block_range_threshold(0);
    let wire = fetcher.phase_wire_byte_counter();
    let before = wire.snapshot();

    let seg = seg_ref(bytes.len() as u64, recs);
    let acc = QueryAccounting::new();
    let query = LogQuery::new(i64::MIN, i64::MAX);
    let mut scan = fetcher
        .scan_accounted_with_tenant(&seg, TENANT, &query, sel, &acc)
        .await
        .expect("scan")
        .expect("the segment overlaps the query window");
    let mut rows = 0usize;
    while let Some(block) = scan.next_block().expect("decode") {
        rows += block.len();
    }
    drop(scan);
    (
        wire.snapshot().saturating_sub(&before),
        acc.snapshot(),
        rows,
    )
}

/// The exact numerator, denominator and ratio of fetch amplification on a
/// fixture whose page geometry is known from PAGE_DIR.
///
/// Two gaps, because the numerator is a transfer count and not a page-length
/// sum. At gap 0 the fixture's 9 candidate runs (3 row groups x 3 projected
/// columns) exceed `MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT` (ADR-2066 decision
/// 1), so the fetcher bridges the smallest gaps down to 4 runs before this
/// read ever happens; the wire bytes are the bridged runs' bytes, not the raw
/// per-chunk sum, so the ratio is over 1.0 even at gap 0. At [`HOLE_GAP`] the
/// fixture's row groups sit close enough together (this is a small synthetic
/// object) that coalescing fuses the whole projection into one run spanning
/// almost the entire BLOCKS section, which crosses the covering-read
/// threshold (ADR-0107 decision 1) computed against that same coalesced run
/// (ADR-2066 decision 1's "computed against the bounded set, not the raw
/// wanted extents"), so the read converts to one whole-object GET and the
/// numerator becomes the object's total size.
///
/// Non-vacuity: computing the numerator as the sum of `PageDesc::len` over
/// every page present in the decoded blocks (the pre-version-4 counterfactual
/// `page_bytes_fetched` still reports) makes the gap-0 case read the
/// `page_bytes_fetched` total instead of the transferred total, and the exact
/// equality below fails.
#[tokio::test]
async fn fetch_amplification_pins_exact_wire_and_stored_page_bytes() {
    let recs = records();
    let bytes = build_object(&recs);
    let sel = ColumnSelection::fixed_only().with_flags();
    let ids = resolved(&bytes, &sel).expect("a projection, not all columns");
    let all_blocks: Vec<usize> = (0..BLOCKS).collect();

    // Denominator oracle: the stored page bytes the projection decodes. The
    // decode reads every block (the query carries no predicate).
    let stored_decoded = expected_stored_page_bytes(&bytes, &all_blocks, Some(&ids));

    // ---- gap 0: 9 candidate runs, capped and bridged to 4 -----------------
    let runs = expected_runs(&bytes, &all_blocks, Some(&ids), 0);
    assert_eq!(runs.len(), 9, "9 candidate runs before bridging");
    assert!(
        runs.len() > MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT,
        "the fixture must actually exceed the cap for this test to mean anything"
    );
    let raw_ends: Vec<(u64, u64)> = runs.iter().map(|(s, len)| (*s, s + len)).collect();
    let bridged = bound_runs_oracle(&raw_ends, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT);
    assert_eq!(
        bridged.len(),
        MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT,
        "9 candidate runs bridge down to exactly the cap"
    );
    let wire_oracle: u64 = bridged.iter().map(|(s, e)| e - s).sum();
    let (wire, acc, rows) = amplification_scan(&bytes, &recs, &sel, 0).await;
    assert_eq!(rows, RECORDS, "every record decoded");

    let numerator = wire.phase(QueryPhase::Scan);
    let denominator = acc.page_bytes_decoded;
    assert_eq!(
        numerator, wire_oracle,
        "scan-phase WIRE bytes are the coalesced projected page runs"
    );
    assert_eq!(
        numerator, 46,
        "exact BLOCKS-section wire bytes for this fixture and projection, once \
         the 9 candidate runs bridge down to the 4-run cap"
    );
    assert_eq!(
        denominator, stored_decoded,
        "the denominator is the STORED bytes of the decoded projected pages"
    );
    assert_eq!(
        denominator, 30,
        "exact stored decoded page bytes for this fixture and projection"
    );
    assert_eq!(
        numerator as f64 / denominator as f64,
        46.0 / 30.0,
        "bridging the 9 candidate runs down to the 4-run cap moves bytes the \
         projection did not ask for, so gap 0 no longer reads exactly what it decodes"
    );

    // ---- HOLE_GAP: coalescing crosses the covering-read threshold too ------
    let hole_runs = expected_runs(&bytes, &all_blocks, Some(&ids), HOLE_GAP);
    assert_eq!(
        hole_runs.len(),
        1,
        "HOLE_GAP fuses the whole projection into one contiguous run"
    );
    let (hole_wire, hole_acc, hole_rows) = amplification_scan(&bytes, &recs, &sel, HOLE_GAP).await;
    assert_eq!(hole_rows, RECORDS);

    // The fused run spans almost the entire BLOCKS section (this is a small
    // synthetic object, so its row groups sit close enough together that one
    // coalescing gap wide enough to fuse them crosses the covering-read
    // threshold too). ADR-2066 decision 1 computes that crossover against the
    // coalesced/bounded run set, not the raw wanted extents, so this read
    // converts to one whole-object GET: the numerator is the object's total
    // size, not just the fused run's own span.
    let hole_numerator = hole_wire.phase(QueryPhase::Scan);
    assert_eq!(
        hole_numerator,
        bytes.len() as u64,
        "the fused run crosses the 75% covering-read threshold, so the read \
         becomes one whole-object GET"
    );
    assert_eq!(
        hole_numerator, 78_899,
        "exact object size for this fixture, once coalescing crosses the \
         covering-read threshold"
    );
    assert_eq!(
        hole_acc.page_bytes_decoded, stored_decoded,
        "the coalescing gap moves the numerator only; the decode is unchanged"
    );
    assert_eq!(
        hole_numerator as f64 / hole_acc.page_bytes_decoded as f64,
        78_899.0 / 30.0,
        "exact amplification once coalescing crosses the covering-read threshold"
    );
}

/// Every WIRE byte an execution moved is charged to exactly one phase, and the
/// phases sum to the pooled `QueryAccounting` GET total. A call site that
/// records into the counter without recording into the accounting handle (or
/// twice into either) breaks this equality.
///
/// The metadata GETs of a data read are `Probe`, its BLOCKS-section ranges are
/// `Scan`, and neither `Resolve` (the catalog's, which this fetcher never
/// issues) nor `Plan` (no planning read here) is written at all.
#[tokio::test]
async fn per_phase_wire_bytes_sum_to_the_pooled_object_store_bytes() {
    let recs = records();
    let bytes = build_object(&recs);
    let sel = ColumnSelection::fixed_only().with_flags();
    let (wire, acc, _rows) = amplification_scan(&bytes, &recs, &sel, 0).await;

    assert_eq!(
        wire.total(),
        acc.s3_bytes(AccountedOp::Get),
        "the per-phase split neither drops nor double-counts a wire byte"
    );
    assert_eq!(
        wire.total(),
        acc.total_s3_bytes(),
        "this read issues GETs only, so the pooled total is the GET total"
    );

    assert_eq!(
        wire.phase(QueryPhase::Resolve),
        0,
        "the catalog resolve is not this fetcher's traffic"
    );
    assert_eq!(
        wire.phase(QueryPhase::Plan),
        0,
        "a scan funnel issues no planning read"
    );

    // The probe is a suffix of exactly the object tail, plus the two front
    // sections no suffix reaches.
    let footer = footer_of(&bytes);
    let front: u64 = [kind::STREAM_DIR, kind::FIELD_DIR]
        .iter()
        .map(|k| footer.section(*k).expect("front section").len)
        .sum();
    assert_eq!(
        wire.phase(QueryPhase::Probe),
        tail_len(&bytes) + front,
        "probe-phase wire bytes are the tail probe and the two front sections"
    );
    assert_eq!(
        wire.phase(QueryPhase::Probe) + wire.phase(QueryPhase::Scan),
        wire.total(),
        "no third phase carries any of this read's bytes"
    );
}

/// The property this metric exists to expose: on a narrow projection over a
/// version-4 object the new ratio is strictly below the legacy
/// `page_bytes_fetched / page_bytes_decoded` ratio, and the gap is the bytes
/// the mandatory PAGE_DIR lets the read never fetch, less the bridging slack
/// ADR-2066 decision 1's 4-run cap adds back on this fixture (9 candidate
/// runs bridge down to 4, so gap 0 is no longer a bytes-in-equals-bytes-out
/// read).
///
/// Equality (of the two ratios) would mean either the fetcher is not
/// narrowing as docs/log-segment-format.md claims, or the numerator is being
/// computed from page descriptors rather than from transfers.
#[tokio::test]
async fn narrow_projection_amplification_is_below_the_legacy_page_byte_ratio() {
    let recs = records();
    let bytes = build_object(&recs);
    let sel = ColumnSelection::fixed_only().with_flags();
    let all_blocks: Vec<usize> = (0..BLOCKS).collect();
    let (wire, acc, _rows) = amplification_scan(&bytes, &recs, &sel, 0).await;

    // Legacy pair, unchanged in meaning: both sides are STORED page bytes, and
    // `page_bytes_fetched` counts every page descriptor in a decoded block
    // whether or not the read retrieved it.
    assert_eq!(
        acc.page_bytes_fetched,
        expected_present_page_bytes(&bytes, &all_blocks),
        "the legacy numerator is every present page's stored length"
    );
    assert_eq!(acc.page_bytes_fetched, 74_321);
    assert_eq!(acc.page_bytes_decoded, 30);

    let legacy = acc.page_bytes_fetched as f64 / acc.page_bytes_decoded as f64;
    let measured = wire.phase(QueryPhase::Scan) as f64 / acc.page_bytes_decoded as f64;
    assert_eq!(legacy, 74_321.0 / 30.0, "exact legacy ratio");
    assert_eq!(
        measured,
        46.0 / 30.0,
        "exact measured ratio: 9 candidate runs bridge down to the 4-run cap, \
         so gap 0 moves 46 wire bytes for 30 decoded bytes, not 1:1"
    );
    assert!(
        measured < legacy,
        "measured amplification {measured} must be strictly below the legacy {legacy}"
    );

    // The gap is the stored bytes of the pages the projection dropped, which a
    // version-4 read never asks the store for, less the bridging slack the
    // 4-run cap adds back on this fixture (9 candidate runs bridge down to 4).
    let never_fetched = acc.page_bytes_fetched - acc.page_bytes_decoded;
    assert_eq!(never_fetched, 74_291);
    let bridging_slack = wire.phase(QueryPhase::Scan) - acc.page_bytes_decoded;
    assert_eq!(
        bridging_slack, 16,
        "the 4-run cap re-fetches 16 bytes of unwanted pages to stay under it"
    );
    assert_eq!(
        acc.page_bytes_fetched - wire.phase(QueryPhase::Scan),
        never_fetched - bridging_slack,
        "the legacy numerator overstates by exactly the bytes version 4 avoids, \
         less the bridging slack this read paid to stay under the cap"
    );
}

// ---- 10. sparse assembly: a ranged read holds what it placed (#2066) -------

/// The placed extents of the narrow projection
/// `version_4_projected_read_bridges_chunk_runs_to_the_four_get_cap` reads,
/// derived from the directories rather than from the fetcher: the tail probe
/// (`ranged` sizes it to exactly the tail), the one STREAM_DIR+FIELD_DIR span,
/// and the four bridged chunk runs. Returns `(placed, projection)`.
fn narrow_projection_placed_bytes(bytes: &[u8]) -> (u64, ColumnSelection) {
    let sel = ColumnSelection::fixed_only().with_flags();
    let ids = resolved(bytes, &sel).expect("a projection, not all columns");
    let f = footer_of(bytes);
    let stream = f.section(kind::STREAM_DIR).expect("STREAM_DIR");
    let field = f.section(kind::FIELD_DIR).expect("FIELD_DIR");
    assert_eq!(
        stream.offset + stream.len,
        field.offset,
        "the front sections are adjacent, so their span is their two lengths"
    );
    let front = stream.len + field.len;
    let all_blocks: Vec<usize> = (0..BLOCKS).collect();
    let raw: Vec<(u64, u64)> = expected_runs(bytes, &all_blocks, Some(&ids), 0)
        .into_iter()
        .map(|(s, l)| (s, s + l))
        .collect();
    let runs: u64 = bound_runs_oracle(&raw, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT)
        .iter()
        .map(|(s, e)| e - s)
        .sum();
    // Literals beside the derivation, so a change in the fixture's layout or
    // in the oracle moves a number rather than two agreeing sums. The 46 run
    // bytes are the wire figure the amplification test above pins.
    assert_eq!(
        (tail_len(bytes), front, runs),
        (4_494, 84, 46),
        "tail probe, front span, bridged runs"
    );
    (tail_len(bytes) + front + runs, sel)
}

/// Issue #2066: a narrow projection over a three-row-group version-4 object
/// holds exactly the bytes it placed, not the object. The assembly gauge and
/// the fetch budget's `fetch_reserved` both read the placed figure -- the tail
/// probe, the front-section span and the four bridged chunk runs, 4,494 + 84 +
/// 46 = 4,624 of a 78,899-byte object -- while the reader holds the bytes,
/// and both return to zero when it drops them.
///
/// With no cache wired nothing is offered to a second byte ledger, so the
/// placed regions are this read's alone and `handoff_overlap` stays 0.
///
/// Prove-the-test: against the object-sized pooled buffer this replaced, the
/// gauge and `fetch_reserved` both read 78,899 at the first figure asserted
/// after the fetch. A sparse assembler that still reserved the object size
/// fails the `fetch_reserved` assertion; one that kept a pooled object-sized
/// buffer beside its regions and charged it fails the gauge assertion. Making
/// `hold_placement` mark every reservation handed off unconditionally, instead
/// of only when a cache is wired, fails the `handoff_overlap` assertion below
/// with 4,624 against 0.
#[tokio::test]
async fn version_4_narrow_projection_holds_exactly_its_placed_bytes() {
    let recs = records();
    let bytes = build_object(&recs);
    let object_size = bytes.len() as u64;
    let (placed, sel) = narrow_projection_placed_bytes(&bytes);
    assert_eq!(object_size, 78_899, "fixture object size");
    assert_eq!(placed, 4_624, "placed bytes of the narrow projection");

    let recording = RecordingStore::new(store_with(&bytes).await);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;
    let budget = Arc::new(ravel_memory::MemoryBudget::new(4 * object_size));
    let br = ranged(store, &bytes).with_memory_budget(Arc::clone(&budget));
    let seg = seg_ref(object_size, &recs);
    let (got, stats) = br
        .fetch_object_projected(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &sel,
            &QueryAccounting::new(),
        )
        .await
        .expect("projected fetch");
    assert!(!stats.whole_object, "the object is not read whole");

    // What the store was asked for is what the oracle names: the suffix probe
    // of `tail_len` plus every range GET.
    assert_eq!(recording.suffix_gets(), 1, "one suffix probe");
    let ranged_bytes: u64 = recording.ranges().iter().map(|(a, b)| b - a).sum();
    assert_eq!(ranged_bytes + tail_len(&bytes), placed);

    let held = br.assembly_buffer_stats();
    assert_eq!(
        (held.live_bytes, held.peak_live_bytes),
        (placed, placed),
        "the read holds its placed bytes, not the {object_size}-byte object"
    );
    assert_eq!(
        budget.fetch_reserved(),
        placed,
        "the fetch reservation covers the placed bytes, not the object"
    );
    assert_eq!(
        budget.handoff_overlap(),
        0,
        "with no cache wired the regions are offered to no second ledger"
    );

    drop(got);
    assert_eq!(br.assembly_buffer_stats().live_bytes, 0);
    assert_eq!(
        budget.fetch_reserved(),
        0,
        "the reservation releases when the reader drops the bytes"
    );
}

/// Issue #2066, ADR-1170 decision 2: during the read of
/// `version_4_narrow_projection_holds_exactly_its_placed_bytes`, with the
/// first chunk-run GET held in flight, `fetch_reserved` already equals the
/// read's placed bytes: the probe and the front span are placed and reserved,
/// and the four runs were reserved together before any of them was issued.
/// Nothing else is reserved, and the figure returns to zero once the reader
/// drops the bytes. GET 1 is the probe, GET 2 the front span, GET 3 the first
/// chunk run.
///
/// Prove-the-test: against the object-sized reservation this replaced, the
/// in-flight figure read the object size plus the transient run reservation,
/// 78,899 + 46 = 78,945, not 4,624.
#[tokio::test]
async fn version_4_ranged_read_reserves_its_placed_bytes_while_a_get_is_in_flight() {
    let recs = records();
    let bytes = build_object(&recs);
    let object_size = bytes.len() as u64;
    let (placed, sel) = narrow_projection_placed_bytes(&bytes);

    let faulty = Arc::new(FaultStore::new(
        store_with(&bytes).await,
        FaultPlan::empty(),
    ));
    let gate = faulty.hold(Op::Get, Some(KEY.to_string()), Occurrence::Nth(3));
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
    let budget = Arc::new(ravel_memory::MemoryBudget::new(4 * object_size));
    let br = ranged(store, &bytes).with_memory_budget(Arc::clone(&budget));
    let seg = seg_ref(object_size, &recs);

    let task = tokio::spawn(async move {
        br.fetch_object_projected(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &sel,
            &QueryAccounting::new(),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
        .await
        .expect("the first chunk-run GET reaches the gate within 30 s");
    assert_eq!(
        budget.fetch_reserved(),
        placed,
        "mid-read, the reservation is the placed bytes, not the object"
    );
    for id in gate.held() {
        assert!(gate.release(id), "held id must release");
    }
    let (got, stats) = task.await.expect("join").expect("projected fetch");
    assert!(!stats.whole_object);
    assert_eq!(budget.fetch_reserved(), placed);
    drop(got);
    assert_eq!(budget.fetch_reserved(), 0);
}

/// Issue #2066, ADR-1170 decision 2: each placement reserves its own length
/// BEFORE the GET that fetches it, so a refused placement issues no request at
/// all. The budget here is exactly the suffix probe's length, which admits the
/// probe and nothing after it; the next placement on this read is the 84-byte
/// STREAM_DIR+FIELD_DIR front span, and it is refused.
///
/// Prove-the-test: moving `self.reserve_fetch(len)?` in `place_extent` below
/// the `cached_extent` call it precedes leaves the read failing with the same
/// `FetchMemoryExhausted`, because the reservation is refused either way --
/// but the front-span GET has gone out to the store by then, so `gets()` reads
/// 2 and the GET-count assertion below fails. That assertion, not the error
/// class, is what pins the ordering.
#[tokio::test]
async fn version_4_ranged_read_refuses_a_placement_before_issuing_its_get() {
    let recs = records();
    let bytes = build_object(&recs);
    let object_size = bytes.len() as u64;
    let (_, sel) = narrow_projection_placed_bytes(&bytes);
    let probe = tail_len(&bytes);

    let recording = RecordingStore::new(store_with(&bytes).await);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;
    let budget = Arc::new(ravel_memory::MemoryBudget::new(probe));
    let br = ranged(store, &bytes).with_memory_budget(Arc::clone(&budget));
    let seg = seg_ref(object_size, &recs);

    let err = br
        .fetch_object_projected(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &sel,
            &QueryAccounting::new(),
        )
        .await
        .expect_err("the placement after the probe must be refused");
    match err {
        LogFetchError::FetchMemoryExhausted {
            requested,
            reserved,
            limit,
        } => {
            assert_eq!(requested, 84, "the refused placement is the front span");
            assert_eq!(
                reserved, probe,
                "the probe's own reservation is the held one"
            );
            assert_eq!(limit, probe);
        }
        other => panic!("expected FetchMemoryExhausted, got {other:?}"),
    }
    assert_eq!(recording.suffix_gets(), 1, "the probe GET was issued");
    assert_eq!(
        recording.gets(),
        1,
        "the probe is the ONLY GET: the refused placement never reached the store"
    );
    assert_eq!(
        budget.fetch_reserved(),
        0,
        "the refusal drops the assembler, releasing what it had placed"
    );
}

/// Issue #2066, ADR-1170 decision 2's handoff rule: with a cache wired every
/// placed region is offered to the cache, so every assembler guard is marked
/// handed off and `handoff_overlap` equals the read's placed bytes -- on the
/// cold read that admits them and on the warm read that hits them. The same
/// read with no cache reports 0
/// (`version_4_narrow_projection_holds_exactly_its_placed_bytes`).
///
/// The figure bounds the overlap from above rather than counting it exactly:
/// the mark is made per placement, not per admission, so a value the cache
/// refused over its single-entry cap still counts. This fixture's cache admits
/// every region (64 MiB single-entry cap against a 4,624-byte read), so the
/// bound is tight here.
///
/// Prove-the-test: dropping the `reservation.mark_handed_off()` call from
/// `hold_placement` leaves `handoff_overlap()` at 0 on both reads below, so
/// the first cached assertion fails with 0 against 4,624.
#[tokio::test]
async fn version_4_cached_ranged_read_marks_its_placements_handed_off() {
    let recs = records();
    let bytes = build_object(&recs);
    let object_size = bytes.len() as u64;
    let (placed, sel) = narrow_projection_placed_bytes(&bytes);

    let store: Arc<dyn ObjectStoreBackend> = store_with(&bytes).await;
    let budget = Arc::new(ravel_memory::MemoryBudget::new(4 * object_size));
    let br = ranged(store, &bytes)
        .with_cache(read_cache())
        .with_memory_budget(Arc::clone(&budget));
    let seg = seg_ref(object_size, &recs);

    let (cold, stats) = br
        .fetch_object_projected(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &sel,
            &QueryAccounting::new(),
        )
        .await
        .expect("cold projected fetch");
    assert!(!stats.whole_object, "the object is not read whole");
    assert_eq!(
        budget.fetch_reserved(),
        placed,
        "the cold read reserves its placed bytes"
    );
    assert_eq!(
        budget.handoff_overlap(),
        placed,
        "every region the cold read admitted is under the cache's ledger too"
    );
    drop(cold);
    assert_eq!(
        budget.handoff_overlap(),
        0,
        "the cold read's overlap clears before the warm read is measured"
    );

    let accounting = QueryAccounting::new();
    let (warm, stats) = br
        .fetch_object_projected(&seg, TENANT, i64::MIN, i64::MAX, &sel, &accounting)
        .await
        .expect("warm projected fetch");
    assert!(!stats.whole_object);
    assert_eq!(
        (
            stats.probe_gets,
            stats.block_range_gets,
            stats.metadata_gets
        ),
        (0, 0, 0),
        "the warm read crosses no network: every extent is cache-resident"
    );
    assert_eq!(
        budget.handoff_overlap(),
        placed,
        "a hit holds the cache entry's own bytes under both ledgers"
    );
    drop(warm);
    assert_eq!(budget.handoff_overlap(), 0, "the overlap clears on drop");
    assert_eq!(
        budget.fetch_reserved(),
        0,
        "the reservations release with it"
    );
}
