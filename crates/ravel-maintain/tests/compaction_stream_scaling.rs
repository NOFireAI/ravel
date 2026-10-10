//! How RLOG compaction's block-read cost scales with the number of streams
//! sharing one row group (performance review experiment E2, issue #2378).
//!
//! The fixture is two L0 `.rlog` inputs, each written by the real `RlogWriter`
//! at its default geometry: 4 blocks of 8192 records, so one row group per
//! input (`RlogConfig::group_target_blocks` is 32). `S` streams are assigned
//! round-robin over the records. The writer sorts rows by
//! `(stream_ref, ts_ns)` before blocking (docs/log-segment-format.md,
//! "BLOCKS"), so the round-robin order does not survive into the object: each
//! stream is one contiguous run of `32768 / S` rows, and what `S` varies is how
//! many streams share the one row group.
//!
//! What the compactor does with that layout, each step at the line that makes
//! it true:
//!
//! - It merges one stream at a time over the sorted union of the inputs'
//!   STREAM_DIRs (`crates/ravel-maintain/src/rlog.rs:1117`, the
//!   `for stream_id in merged.keys()` loop calling `merge_stream_into_parts`).
//! - Per stream it opens one cursor per input carrying the stream
//!   (`rlog.rs:3148`, `StreamCursor::open` inside `open_cursor`), whose fetch
//!   plan is `RlogRangeReader::stream_blocks` (`rlog.rs:2155`).
//! - `stream_blocks` returns one location per row group the stream touches
//!   (`crates/ravel-logseg/src/ranged.rs:347-381`), and each location's range
//!   is the whole row group's extent, not the stream's blocks
//!   (`ranged.rs:200`, `extent_of` widening every block to `group_extent`).
//! - The cursor fetches each location with one ranged GET
//!   (`rlog.rs:2486` in `fetch_block`, called from `next_raw_block` at
//!   `rlog.rs:2420-2429`), and no state is shared between cursors, so two
//!   streams in the same group fetch it independently.
//!
//! So the prediction pinned here is that block-read GETs per input equal `S`,
//! every one of them for the input's whole row group, and block-read bytes
//! grow as `S` times the group's stored size.
//!
//! No decoded-block counter is exposed by `ravel-maintain`, so decode cost is
//! reported as elapsed wall time only, printed and never asserted.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use common::*;
use ravel_commit::keys;
use ravel_logseg::RlogConfig;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, RequestLedger, compact_bucket, read,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use uuid::Uuid;

const BLOCK_RECORDS: usize = 8192;
const BLOCKS_PER_INPUT: usize = 4;
const RECORDS_PER_INPUT: usize = BLOCK_RECORDS * BLOCKS_PER_INPUT;
const INPUTS: usize = 2;
const STREAM_COUNTS: [u32; 4] = [1, 4, 16, 64];

/// One ranged GET as the store saw it: key, `[start, end)`, payload bytes.
#[derive(Clone, Debug)]
struct RangedGet {
    key: String,
    start: u64,
    end: u64,
    bytes: u64,
}

/// A behavior-preserving decorator that records every ranged GET, plus a
/// count and byte total for every GET of any kind.
struct RecordingStore {
    inner: MemoryStore,
    ranged: Mutex<Vec<RangedGet>>,
    all_gets: Mutex<(u64, u64)>,
}

impl RecordingStore {
    fn new() -> Self {
        RecordingStore {
            inner: MemoryStore::new(),
            ranged: Mutex::new(Vec::new()),
            all_gets: Mutex::new((0, 0)),
        }
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
        let got = self.inner.get(key, range).await?;
        let bytes = got.data.len() as u64;
        {
            let mut all = self.all_gets.lock().unwrap();
            all.0 += 1;
            all.1 += bytes;
        }
        if let GetRange::Range(start, end) = range {
            self.ranged.lock().unwrap().push(RangedGet {
                key: key.to_string(),
                start,
                end,
                bytes,
            });
        }
        Ok(got)
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
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

/// The writer geometry every input is written at: the shipped default, with
/// the block target and group size spelled out because the fixture's shape
/// (4 blocks, 1 group) depends on them.
fn fixture_config() -> RlogConfig {
    RlogConfig {
        block_target_records: BLOCK_RECORDS,
        group_target_blocks: 32,
        ..RlogConfig::default()
    }
}

/// Seed `INPUTS` L0 inputs of `RECORDS_PER_INPUT` records each, `streams`
/// streams assigned round-robin. Timestamps interleave across inputs so every
/// stream's slices overlap in time and the merge opens both inputs' cursors.
async fn seed_inputs(store: &dyn ObjectStoreBackend, streams: u32) -> Bucket {
    for input in 0..INPUTS {
        let records: Vec<_> = (0..RECORDS_PER_INPUT)
            .map(|i| {
                let stream = i as u32 % streams;
                let ts = 1_000_000 + 2 * i as i64 + input as i64;
                log_record(stream, ts, &format!("line {i} of input {input}"))
            })
            .collect();
        seed_rlog_input_with_config(
            store,
            Uuid::from_u128(input as u128 + 1),
            10,
            input as u64 + 1,
            HOUR,
            &records,
            false,
            u32::from(ravel_logseg::footer::VERSION),
            fixture_config(),
        )
        .await;
    }
    logs_bucket()
}

/// Per input data key: the absolute `[start, end)` extent of each row group,
/// derived from the object's own PAGE_DIR (the union of the group's column
/// chunk extents, offset by the BLOCKS section), and its block count.
async fn group_extents(
    store: &MemoryStore,
    bucket: &Bucket,
) -> BTreeMap<String, (Vec<(u64, u64)>, u64)> {
    let listing = read::list_bucket(store, bucket).await.expect("list");
    let inputs = read::load_inputs(store, bucket, &listing.commit_keys, 1)
        .await
        .expect("inputs");
    let mut out = BTreeMap::new();
    for input in &inputs {
        let key = keys::reconstruct_data_key(&input.record).expect("data key");
        let object = store
            .get(&key, GetRange::Full)
            .await
            .expect("get input")
            .data;
        let ftr = footer::open(&object).expect("open footer");
        let blocks = ftr.section(kind::BLOCKS).expect("BLOCKS section");
        let raw = ravel_logseg::read_section(
            &object,
            ftr.section(kind::PAGE_DIR).expect("PAGE_DIR section"),
            &RlogConfig::default(),
        )
        .expect("PAGE_DIR");
        let page_dir = PageDir::decode(&raw).expect("decode PAGE_DIR");
        let extents = page_dir
            .groups
            .iter()
            .map(|g| {
                let (mut start, mut end) = (u64::MAX, 0u64);
                for chunk in &g.chunks {
                    let (offset, len) = chunk.extent().expect("chunk extent");
                    start = start.min(offset);
                    end = end.max(offset + len);
                }
                (blocks.offset + start, blocks.offset + end)
            })
            .collect();
        out.insert(key, (extents, page_dir.block_count()));
    }
    out
}

/// One row of the scaling table.
#[derive(Debug)]
struct Row {
    streams: u32,
    inputs: usize,
    groups_per_input: usize,
    blocks_per_input: u64,
    /// Every GET the run issued, of any kind, and its payload bytes.
    all_gets: u64,
    all_get_bytes: u64,
    /// The ledger's block-read phase: requests and wire bytes received.
    block_gets: u64,
    block_bytes: u64,
    /// Per input, ranged GETs whose range is exactly one of its row groups.
    group_fetches: Vec<u64>,
    /// Per input, the stored size of its row groups summed.
    group_bytes: Vec<u64>,
    parts: usize,
    part_bytes: u64,
    elapsed_ms: u128,
}

async fn measure(streams: u32) -> Row {
    let store = RecordingStore::new();
    let bucket = seed_inputs(&store, streams).await;
    let ledger = RequestLedger::new();
    let config = CompactorConfig {
        request_ledger: Some(ledger.clone()),
        ..CompactorConfig::default()
    };
    let clock = FixedClock::new(sealed_now_ns());
    let started = Instant::now();
    let outcome = compact_bucket(&store, &clock, &config, &bucket)
        .await
        .expect("compact");
    let elapsed_ms = started.elapsed().as_millis();
    let CompactionOutcome::Compacted { parts, .. } = outcome else {
        panic!("expected a compaction, got {outcome:?}");
    };
    let report = ledger.report();
    let ranged = store.ranged.lock().unwrap().clone();
    let (all_gets, all_get_bytes) = *store.all_gets.lock().unwrap();

    let layout = group_extents(&store.inner, &bucket).await;
    assert_eq!(layout.len(), INPUTS, "both inputs listed");
    let mut group_fetches = Vec::new();
    let mut group_bytes = Vec::new();
    let mut groups_per_input = None;
    let mut blocks_per_input = None;
    for (key, (extents, blocks)) in &layout {
        assert_eq!(
            *groups_per_input.get_or_insert(extents.len()),
            extents.len()
        );
        assert_eq!(*blocks_per_input.get_or_insert(*blocks), *blocks);
        group_fetches.push(
            ranged
                .iter()
                .filter(|g| &g.key == key && extents.contains(&(g.start, g.end)))
                .count() as u64,
        );
        group_bytes.push(extents.iter().map(|(s, e)| e - s).sum());
    }
    // Every block-read GET is accounted for as a whole-group fetch, so the
    // per-input figure above is the block-read phase, not a subset of it.
    assert_eq!(
        group_fetches.iter().sum::<u64>(),
        report.block_read.requests,
        "every block-read GET is a whole-row-group range"
    );
    let block_ranged_bytes: u64 = ranged
        .iter()
        .filter(|g| {
            layout
                .get(&g.key)
                .is_some_and(|(ext, _)| ext.contains(&(g.start, g.end)))
        })
        .map(|g| g.bytes)
        .sum();
    assert_eq!(block_ranged_bytes, report.block_read.wire_bytes_received);

    Row {
        streams,
        inputs: layout.len(),
        groups_per_input: groups_per_input.unwrap_or(0),
        blocks_per_input: blocks_per_input.unwrap_or(0),
        all_gets,
        all_get_bytes,
        block_gets: report.block_read.requests,
        block_bytes: report.block_read.wire_bytes_received,
        group_fetches,
        group_bytes,
        parts,
        part_bytes: report.part_put.wire_bytes_sent,
        elapsed_ms,
    }
}

/// Measures `S` in 1, 4, 16, 64, prints one table row per `S`, and pins the
/// scaling: the per-input whole-group fetch count at `S = 16` is at least 8x
/// the figure at `S = 1`, and exactly `S` at every point, with block-read bytes
/// exactly `S` times the input's group bytes.
///
/// Run with `--nocapture` to see the table.
#[tokio::test]
async fn group_fetches_per_input_scale_with_streams_per_group() {
    let mut rows = Vec::new();
    for s in STREAM_COUNTS {
        rows.push(measure(s).await);
    }

    println!(
        "{:>3} {:>6} {:>6} {:>6} {:>8} {:>12} {:>9} {:>12} {:>14} {:>12} {:>5} {:>10} {:>8}",
        "S",
        "inputs",
        "groups",
        "blocks",
        "all_gets",
        "all_bytes",
        "blk_gets",
        "blk_bytes",
        "grp_fetch/in",
        "grp_bytes/in",
        "parts",
        "part_bytes",
        "wall_ms"
    );
    for r in &rows {
        println!(
            "{:>3} {:>6} {:>6} {:>6} {:>8} {:>12} {:>9} {:>12} {:>14} {:>12} {:>5} {:>10} {:>8}",
            r.streams,
            r.inputs,
            r.groups_per_input,
            r.blocks_per_input,
            r.all_gets,
            r.all_get_bytes,
            r.block_gets,
            r.block_bytes,
            format!("{:?}", r.group_fetches),
            format!("{:?}", r.group_bytes),
            r.parts,
            r.part_bytes,
            r.elapsed_ms
        );
    }

    let at = |s: u32| rows.iter().find(|r| r.streams == s).expect("row for S");
    let per_input = |r: &Row| r.group_fetches.iter().copied().max().unwrap_or(0);
    let (one, sixteen) = (per_input(at(1)), per_input(at(16)));
    assert!(
        one > 0 && sixteen >= 8 * one,
        "group fetches per input at S=16 ({sixteen}) must be at least 8x S=1 ({one})"
    );

    for r in &rows {
        assert_eq!(r.inputs, INPUTS);
        assert_eq!(
            r.blocks_per_input, BLOCKS_PER_INPUT as u64,
            "S={}: fixture must write {BLOCKS_PER_INPUT} blocks per input",
            r.streams
        );
        assert_eq!(
            r.groups_per_input, 1,
            "S={}: fixture must write one row group per input",
            r.streams
        );
        for (fetches, bytes) in r.group_fetches.iter().zip(&r.group_bytes) {
            assert_eq!(
                *fetches,
                u64::from(r.streams),
                "S={}: one whole-group fetch per (stream, input) cursor",
                r.streams
            );
            assert!(*bytes > 0);
        }
        let expected_bytes: u64 = r.group_bytes.iter().map(|b| b * u64::from(r.streams)).sum();
        assert_eq!(
            r.block_bytes, expected_bytes,
            "S={}: block-read bytes are S x each input's group bytes",
            r.streams
        );
        assert!(r.parts >= 1 && r.part_bytes > 0);
    }
}
