//! The writer's working set, pinned (#682, ADR-0699 consequences).
//!
//! #682 made the pre-encoding working set one block of cells rather than the
//! whole object. ADR-0699 adds one row group of *encoded, already-compressed*
//! pages on top of that, because a row group's pages are placed column-major
//! and so cannot be written until the group's last block is encoded. The ADR
//! states that cost as bounded rather than unbounded; this file is what stops
//! it from growing silently.
//!
//! The quantity measured is the bytes the writer holds back before placing
//! them, which is exactly the largest row group's extent in the BLOCKS section
//! of the object it produced. That is a property of the bytes, not a sampled
//! allocator or RSS reading, so it is deterministic and it is the number the
//! ADR's consequences section names. `unsafe` is denied workspace-wide, so a
//! counting global allocator is not available and would be noisier anyway.
//!
//! The companion pin -- that the buffer never holds more than
//! `group_target_blocks` blocks at once -- is a unit test on `BlocksBuilder` in
//! `src/writer.rs`, which can see the buffer directly.
#![allow(clippy::expect_used)]

use ravel_logseg::footer::{self, kind, open};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter, read_section,
    stream_attrs_bytes,
};

const BLOCK_RECORDS: usize = 512;

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

/// A corpus wide enough that page bytes, not per-record overhead, dominate: 24
/// dynamic columns of mixed types, with bodies varied enough that zstd cannot
/// collapse them to nothing.
fn corpus(records: usize) -> Vec<LogRecord> {
    let blob = stream_attrs_bytes(
        &[("service.name".to_string(), AttrValue::Str("svc".into()))],
        "scope",
        "1",
        &[],
    );
    (0..records)
        .map(|i| {
            let n = i as i64;
            let mut attrs = Vec::with_capacity(24);
            for c in 0..8i64 {
                attrs.push((format!("i{c}"), AttrValue::I64(n * 31 + c)));
                attrs.push((
                    format!("s{c}"),
                    AttrValue::Str(format!("value-{c}-{}", i % 97)),
                ));
                attrs.push((format!("f{c}"), AttrValue::F64((n % 251) as f64 + 0.5)));
            }
            LogRecord {
                stream_id: sid(0),
                stream_attrs: blob.clone(),
                ts_ns: n,
                observed_ts_ns: n,
                severity_num: (i % 24) as u8,
                severity_text: "INFO".to_string(),
                body: format!(
                    "request {i} finished in {} ms with code {}",
                    i % 733,
                    i % 17
                ),
                trace_id: Some([(i % 251) as u8; 16]),
                span_id: Some([(i % 251) as u8; 8]),
                flags: (i as u32) & 0xf,
                attrs,
            }
        })
        .collect()
}

/// Writes `blocks` blocks at `group_target_blocks` and returns
/// `(largest row group extent, BLOCKS section length, row group count)`.
fn measure(blocks: usize, group_target_blocks: usize) -> (u64, u64, usize) {
    let cfg = RlogConfig {
        block_target_records: BLOCK_RECORDS,
        group_target_blocks,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: [1u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 0,
        writer_seq: 0,
    };
    let mut w = RlogWriter::new(cfg, identity);
    for r in corpus(blocks * BLOCK_RECORDS) {
        w.push(r).expect("push");
    }
    let object = w.finish().expect("finish");

    let ftr = open(&object).expect("open");
    let blocks_len = ftr.section(kind::BLOCKS).expect("BLOCKS").len;
    let raw = read_section(
        &object,
        ftr.section(kind::PAGE_DIR).expect("PAGE_DIR"),
        &cfg,
    )
    .expect("read page_dir");
    let dir = PageDir::decode(&raw).expect("decode page_dir");
    assert_eq!(
        dir.block_count(),
        blocks as u64,
        "block count as configured"
    );

    // A row group's extent is its first column chunk's offset to the end of its
    // last: the chunks of one group are contiguous, so this is exactly the
    // bytes the writer held back before placing that group.
    let largest = dir
        .groups
        .iter()
        .map(|g| {
            let start = g.chunks.iter().map(|c| c.offset).min().unwrap_or(0);
            let end = g
                .chunks
                .iter()
                .filter_map(|c| c.extent())
                .map(|(o, l)| o + l)
                .max()
                .unwrap_or(0);
            end - start
        })
        .max()
        .unwrap_or(0);
    (largest, blocks_len, dir.groups.len())
}

/// The row-group buffer costs one row group of encoded pages, and that cost
/// does not grow with the object.
///
/// Quadrupling the object at a fixed 32-block group leaves the largest group's
/// extent essentially unchanged, and the extent stays a small fraction of the
/// whole BLOCKS section. The bound is stated against measured quantities (the
/// object's own bytes per block) rather than a byte constant, so it does not
/// need re-tuning when the corpus or the codecs change; what it forbids is the
/// held-back bytes scaling with the object instead of with the group.
#[test]
fn row_group_buffer_is_one_row_group_and_does_not_grow_with_the_object() {
    const GROUP: usize = 32;

    let (small, small_blocks_len, small_groups) = measure(GROUP, GROUP);
    let (large, large_blocks_len, large_groups) = measure(4 * GROUP, GROUP);

    // Printed so a regression report carries the numbers, not just a verdict.
    println!(
        "group_target_blocks={GROUP}: {GROUP} blocks -> largest row group {small} B \
         of BLOCKS {small_blocks_len} B in {small_groups} group(s); \
         {} blocks -> largest row group {large} B of BLOCKS {large_blocks_len} B \
         in {large_groups} group(s)",
        4 * GROUP
    );

    assert_eq!(
        small_groups, 1,
        "32 blocks at a 32-block group is one group"
    );
    assert_eq!(large_groups, 4, "128 blocks at a 32-block group is four");

    // The held-back bytes track the group, not the object: the object grew 4x
    // and the buffer did not.
    let growth_permille = large.saturating_sub(small) * 1000 / small.max(1);
    assert!(
        growth_permille <= 50,
        "quadrupling the object grew the largest row group from {small} B to \
         {large} B ({}.{}%); the row group buffer must not scale with the object \
         (ADR-0699 consequences)",
        growth_permille / 10,
        growth_permille % 10,
    );

    // And it is a group's share of the section, not the section: four groups
    // means at most about a quarter, with slack for per-group variation.
    assert!(
        large * 3 <= large_blocks_len,
        "the largest of four row groups is {large} B of a {large_blocks_len} B \
         BLOCKS section, which is more than a third: the writer would be \
         holding back most of the object"
    );

    // The version bump did not change how many bytes the pages are, only where
    // they sit: the section is the same size whatever the group size.
    let (_, ungrouped_blocks_len, ungrouped_groups) = measure(4 * GROUP, 1);
    assert_eq!(
        ungrouped_blocks_len, large_blocks_len,
        "the row group size changes where pages sit, never how many bytes they are"
    );
    assert_eq!(ungrouped_groups, 4 * GROUP);
    assert_eq!(footer::VERSION, 4, "these are version-4 objects");
}
