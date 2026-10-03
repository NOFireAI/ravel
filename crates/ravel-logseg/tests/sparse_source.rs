//! Decoding through a [`SparseObject`] (issue #2066): a reader opened on only
//! the regions a ranged read would place -- the directories and the pages of
//! some blocks -- decodes those blocks to exactly the rows the whole object
//! decodes them to, and fails typed with `Unplaced` on anything else, never a
//! panic and never bytes it was not given.
#![allow(clippy::expect_used)]

use bytes::Bytes;
use proptest::prelude::*;
use ravel_logseg::footer::{kind, open};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    AttrValue, ByteSource, ColumnSelection, LogRecord, LogSegError, LogStreamId, ObjectIdentity,
    Predicate, RlogConfig, RlogReader, RlogWriter, SparseObject, read_section, stream_attrs_bytes,
};

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

fn write_object(cfg: RlogConfig, recs: &[LogRecord]) -> Vec<u8> {
    let identity = ObjectIdentity {
        tenant_hash: [3u8; 16],
        shard: 0,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: 2,
    };
    let mut w = RlogWriter::new(cfg, identity);
    for r in recs {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

fn arb_record() -> impl Strategy<Value = LogRecord> {
    (
        0u8..3,
        0i64..40,
        prop::sample::select(vec!["", "INFO", "ERROR"]),
        "[a-z ]{0,24}",
        any::<u32>(),
        proptest::collection::vec(
            (
                prop::sample::select(vec!["a", "b", "http.status"]),
                prop_oneof![
                    any::<i64>().prop_map(AttrValue::I64),
                    "[a-z]{0,4}".prop_map(AttrValue::Str),
                    any::<bool>().prop_map(AttrValue::Bool),
                ],
            ),
            0..4,
        ),
    )
        .prop_map(|(s, ts, sevt, body, flags, attrs)| LogRecord {
            stream_id: sid(s),
            stream_attrs: stream_attrs_bytes(
                &[("service.name".to_string(), AttrValue::Str(format!("s{s}")))],
                "scope",
                "1",
                &[],
            ),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: sevt.into(),
            body,
            trace_id: None,
            span_id: None,
            flags,
            attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        })
}

/// Each block's absolute page extents, in PAGE_DIR order, then the row-group
/// dictionary pages its string columns decode through.
fn block_pages(object: &[u8]) -> Vec<Vec<(u64, u64)>> {
    let footer = open(object).expect("footer");
    let blocks = footer.section(kind::BLOCKS).expect("BLOCKS");
    let raw = read_section(
        object,
        footer.section(kind::PAGE_DIR).expect("PAGE_DIR"),
        &RlogConfig::default(),
    )
    .expect("PAGE_DIR raw");
    let dir = PageDir::decode(&raw).expect("PAGE_DIR");
    let count: u32 = dir.groups.iter().map(|g| g.block_count).sum();
    (0..count)
        .map(|b| {
            let pages = dir.block_pages(b).expect("block pages");
            let dicts = dir.block_dict_pages(b).expect("block dictionary pages");
            pages
                .iter()
                .chain(&dicts)
                .map(|p| (blocks.offset + p.offset, p.desc.len))
                .collect()
        })
        .collect()
}

/// What a ranged read places for its directories: every section except BLOCKS,
/// and the footer and trailer after the last section.
fn directory_extents(object: &[u8]) -> Vec<(u64, u64)> {
    let footer = open(object).expect("footer");
    let mut out: Vec<(u64, u64)> = footer
        .sections
        .iter()
        .filter(|s| s.kind != kind::BLOCKS)
        .map(|s| (s.offset, s.len))
        .collect();
    let tail = footer
        .sections
        .iter()
        .map(|s| s.offset + s.len)
        .max()
        .expect("sections");
    out.push((tail, object.len() as u64 - tail));
    out
}

/// Places `(start, len)` from `object`, split in two adjacent placements at
/// `cut` (taken modulo the length) when `split` is set, so reads cross a
/// region seam.
fn place(sparse: &mut SparseObject, object: &[u8], start: u64, len: u64, split: bool, cut: u64) {
    let region = |s: u64, l: u64| Bytes::copy_from_slice(&object[s as usize..(s + l) as usize]);
    if split && len > 1 {
        let at = 1 + cut % (len - 1);
        sparse
            .place(start + at, region(start + at, len - at))
            .expect("place");
        sparse.place(start, region(start, at)).expect("place");
    } else {
        sparse.place(start, region(start, len)).expect("place");
    }
}

/// Every byte of `[start, end)` inside some placed extent.
fn covered(extents: &[(u64, u64)], start: u64, end: u64) -> bool {
    (start..end).all(|b| extents.iter().any(|(s, l)| *s <= b && b < s + l))
}

/// Rows of the blocks at `indices`, decoded through `source`.
fn decode<S: ByteSource + ?Sized>(
    source: &S,
    indices: &[usize],
) -> Result<Vec<String>, LogSegError> {
    let reader = RlogReader::from_source(source, &RlogConfig::default())?;
    let mut scan = reader.scan_blocks_subset(
        &Predicate::And(Vec::new()),
        &[],
        &ColumnSelection::all(),
        indices,
    )?;
    let mut rows = Vec::new();
    while let Some(block) = scan.next_block(source)? {
        rows.extend(block.iter().map(|r| format!("{r:?}")));
    }
    Ok(rows)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Random objects, random placements: the directories plus a random subset
    /// of blocks' pages, each region optionally split across a seam. Decoding
    /// the placed blocks through the sparse source equals decoding them from
    /// the whole object; decoding an unplaced block fails with `Unplaced`; and
    /// a random range reads the object's own bytes when every byte of it is
    /// placed and fails with `Unplaced` otherwise.
    ///
    /// Prove-the-test: a source that returns zeros for an unplaced range reads
    /// `Ok` there, so the random-range arm fails on the first unplaced range,
    /// and the unplaced-block arm fails with a page crc `Corrupted`, not
    /// `Unplaced`.
    #[test]
    fn sparse_decode_equals_whole_decode_and_unplaced_reads_refuse(
        records in proptest::collection::vec(arb_record(), 1..40),
        block in 1usize..6,
        group in 1usize..4,
        keep in proptest::collection::vec(any::<bool>(), 64),
        split in proptest::collection::vec(any::<bool>(), 256),
        cut in any::<u64>(),
        probe_start in any::<u64>(),
        probe_len in 0u64..512,
    ) {
        let cfg = RlogConfig {
            block_target_records: block,
            block_max_bytes: 8192,
            group_target_blocks: group,
            ..RlogConfig::default()
        };
        let object = write_object(cfg, &records);
        let pages = block_pages(&object);
        let placed_blocks: Vec<usize> = (0..pages.len()).filter(|b| keep[b % keep.len()]).collect();

        let mut sparse = SparseObject::new(object.len() as u64);
        let mut extents = directory_extents(&object);
        for &b in &placed_blocks {
            extents.extend(pages[b].iter().copied());
        }
        for (i, &(start, len)) in extents.iter().enumerate() {
            place(&mut sparse, &object, start, len, split[i % split.len()], cut);
        }

        let whole = decode(object.as_slice(), &placed_blocks).expect("whole decode");
        let through_sparse = decode(&sparse, &placed_blocks).expect("sparse decode");
        prop_assert_eq!(&through_sparse, &whole);

        if let Some(unplaced) = (0..pages.len()).find(|b| !placed_blocks.contains(b)) {
            let got = decode(&sparse, &[unplaced]);
            prop_assert!(
                matches!(got, Err(LogSegError::Unplaced { .. })),
                "an unplaced block must refuse typed, got {:?}",
                got.map(|rows| rows.len())
            );
        }

        let len = object.len() as u64;
        let start = probe_start % len;
        let n = probe_len.min(len - start);
        match sparse.read(start, n) {
            Ok(got) => {
                prop_assert!(covered(&extents, start, start + n), "read [{}, +{}) was not placed", start, n);
                prop_assert_eq!(&*got, &object[start as usize..(start + n) as usize]);
            }
            Err(LogSegError::Unplaced { .. }) => {
                prop_assert!(!covered(&extents, start, start + n), "placed range [{}, +{}) refused", start, n);
            }
            Err(other) => prop_assert!(false, "unexpected error {other:?}"),
        }
    }
}
