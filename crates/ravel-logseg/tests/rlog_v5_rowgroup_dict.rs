//! RLOG v5 row group string dictionaries (ADR-2135 decision 6,
//! docs/log-segment-format.md "Encodings (tag registry)" and "String codec
//! layouts"): tag 12, one sorted dictionary page per `(row group, string
//! column)` chunk, and tag 13, each block's bit-packed ids into it.
//!
//! Objects go through `RlogWriter` and both readers, `RlogReader` (the scan)
//! and `RlogRangeReader` (the ranged single-block read). The PAGE_DIR cases are
//! hand-built directories through `PageDir::decode`, and the codec cases enter
//! at `rlog_codec` and `block::read_block_pages_with_dicts`, the function the
//! reader hands a block's verified pages and its dictionaries to.
#![allow(clippy::expect_used)]

use std::collections::HashSet;

use bytes::Bytes;
use proptest::prelude::*;
use ravel_logseg::block::{PageCounters, read_block_pages_with_dicts};
use ravel_logseg::encoding::Enc;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{kind, open};
use ravel_logseg::page::{
    COMP_NONE, COMP_ZSTD, COMPRESSION_FLOOR, PageDesc, SealedPage, seal_page, smallest_stored,
};
use ravel_logseg::page_dir::{ChunkEntry, GroupEntry, PageDir, PageEntry};
use ravel_logseg::record::{COL_SEVERITY_TEXT, COL_STREAM_REF, COL_TS};
use ravel_logseg::rlog_codec::{
    MAX_DICT_ENTRIES, decode_dict_ids, decode_dict_page, encode_dict_ids, encode_dict_page,
    string_candidates,
};
use ravel_logseg::{
    AttrValue, ColumnSelection, ColumnarLogBatch, FieldType, LogRecord, LogSegError, LogStreamId,
    ObjectIdentity, Predicate, RlogConfig, RlogRangeReader, RlogReader, RlogWriter, SparseObject,
    read_section, stream_attrs_bytes,
};

const ZSTD_LEVEL: i32 = 3;
const STREAM: LogStreamId = LogStreamId([0x44; 16]);

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0x7C; 16],
        shard: 3,
        writer_id: [0xC7; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn record(ts: i64, attrs: Vec<(&str, &str)>) -> LogRecord {
    LogRecord {
        stream_id: STREAM,
        stream_attrs: stream_attrs_bytes(
            &[("service.name".to_string(), AttrValue::Str("svc".into()))],
            "scope",
            "1.0",
            &[],
        ),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "ok".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs
            .into_iter()
            .map(|(k, v)| (k.to_string(), AttrValue::Str(v.to_string())))
            .collect(),
    }
}

/// A deterministic lowercase value of `len` bytes, distinct per `i`, with no
/// run or repeat for a page codec to collapse.
fn word(i: usize, len: usize) -> String {
    let mut x = 0x9E37_79B9u64.wrapping_mul(i as u64 + 1);
    (0..len)
        .map(|_| {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            char::from(b'a' + ((x >> 33) % 26) as u8)
        })
        .collect()
}

fn blocks_cfg(block_target_records: usize) -> RlogConfig {
    RlogConfig {
        block_target_records,
        ..RlogConfig::default()
    }
}

fn write_rows(cfg: &RlogConfig, records: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(*cfg, identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

fn write_columnar(cfg: &RlogConfig, batch: ColumnarLogBatch) -> Vec<u8> {
    let mut w = RlogWriter::new(*cfg, identity());
    w.push_columnar(batch).expect("push columnar");
    w.finish().expect("finish")
}

fn page_dir(object: &[u8]) -> PageDir {
    let cfg = RlogConfig::default();
    let ftr = open(object).expect("open");
    let raw = read_section(object, ftr.section(kind::PAGE_DIR).expect("PAGE_DIR"), &cfg)
        .expect("read PAGE_DIR");
    PageDir::decode(&raw).expect("decode PAGE_DIR")
}

fn blocks_offset(object: &[u8]) -> u64 {
    open(object)
        .expect("open")
        .section(kind::BLOCKS)
        .expect("BLOCKS")
        .offset
}

fn dyn_column(object: &[u8], name: &str) -> u32 {
    let cfg = RlogConfig::default();
    let ftr = open(object).expect("open");
    let raw = read_section(
        object,
        ftr.section(kind::FIELD_DIR).expect("FIELD_DIR"),
        &cfg,
    )
    .expect("read FIELD_DIR");
    FieldDir::decode(&raw, u64::MAX)
        .expect("decode FIELD_DIR")
        .column(name, FieldType::Str)
        .expect("column")
        .column_id
}

fn chunk(dir: &PageDir, group: usize, column_id: u32) -> ChunkEntry {
    dir.groups[group]
        .chunks
        .iter()
        .find(|c| c.column_id == column_id)
        .expect("chunk")
        .clone()
}

fn encs(c: &ChunkEntry) -> Vec<Enc> {
    c.pages.iter().map(|p| p.enc).collect()
}

/// The stored bytes of `c`'s dictionary page, sliced out of the object.
fn dict_page_bytes(object: &[u8], c: &ChunkEntry) -> Vec<u8> {
    let d = c.dict_page().expect("dictionary page");
    let start = (blocks_offset(object) + c.offset) as usize;
    object[start..start + d.len as usize].to_vec()
}

fn scan_all(object: &[u8]) -> Vec<LogRecord> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg).expect("reader");
    reader.scan(&Predicate::And(vec![])).expect("scan").0
}

/// Whole-object read of exactly block `block` through the scan.
fn scan_block(
    object: &[u8],
    block: usize,
    columns: &ColumnSelection,
) -> Result<Vec<LogRecord>, LogSegError> {
    let reader = RlogReader::new(object, &RlogConfig::default())?;
    let mut scan = reader.scan_blocks_subset(&Predicate::And(vec![]), &[], columns, &[block])?;
    let rows = scan.next_block(object)?.unwrap_or_default();
    assert!(scan.next_block(object)?.is_none(), "one block only");
    Ok(rows)
}

fn range_reader(object: &[u8]) -> RlogRangeReader {
    let cfg = RlogConfig::default();
    let ftr = open(object).expect("open footer");
    let section = |k: u32| {
        read_section(object, ftr.section(k).expect("section present"), &cfg).expect("section")
    };
    let page_dir = ftr
        .section(kind::PAGE_DIR)
        .map(|d| read_section(object, d, &cfg).expect("PAGE_DIR"));
    RlogRangeReader::from_sections_with_page_dir(
        &ftr,
        &section(kind::STREAM_DIR),
        &section(kind::FIELD_DIR),
        &section(kind::SKIP_IDX),
        page_dir.as_deref(),
    )
    .expect("range reader")
}

/// Ranged read of exactly block `block` out of its row group's byte range.
fn ranged_block(object: &[u8], block: usize) -> Result<Vec<LogRecord>, LogSegError> {
    let rr = range_reader(object);
    let locs = rr.stream_blocks(&STREAM)?.expect("stream present");
    let loc = locs
        .iter()
        .find(|l| l.block_indices().contains(&block))
        .expect("loc holding the block");
    rr.decode_block_in_group(
        loc,
        block,
        &object[loc.start() as usize..loc.end() as usize],
    )
}

/// A sparse copy of `object` holding every section but BLOCKS, plus exactly
/// the BLOCKS extents `ranges` (relative to BLOCKS, as PAGE_DIR states them).
fn sparse_with(object: &[u8], ranges: &[(u64, u64)]) -> SparseObject {
    let footer = open(object).expect("footer");
    let mut sparse = SparseObject::new(object.len() as u64);
    let region = |s: u64, l: u64| Bytes::copy_from_slice(&object[s as usize..(s + l) as usize]);
    let mut tail = 0u64;
    for s in &footer.sections {
        tail = tail.max(s.offset + s.len);
        if s.kind != kind::BLOCKS {
            sparse
                .place(s.offset, region(s.offset, s.len))
                .expect("place");
        }
    }
    sparse
        .place(tail, region(tail, object.len() as u64 - tail))
        .expect("place");
    let base = blocks_offset(object);
    for &(start, len) in ranges {
        sparse
            .place(base + start, region(base + start, len))
            .expect("place");
    }
    sparse
}

fn attr<'r>(r: &'r LogRecord, key: &str) -> Option<&'r str> {
    r.attrs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| match v {
            AttrValue::Str(s) => s.as_str(),
            other => panic!("not a string: {other:?}"),
        })
}

fn is_corrupted<T>(r: &Result<T, LogSegError>) -> bool {
    matches!(r, Err(LogSegError::Corrupted(_)))
}

/// Twelve rows over three blocks of four; `svc` cycles through three long
/// values, so its one row group chunk is a dictionary and three id pages.
fn three_block_object() -> (Vec<LogRecord>, Vec<u8>) {
    let values: Vec<String> = (0..3).map(|i| word(i, 24)).collect();
    let records: Vec<LogRecord> = (0..12)
        .map(|i| record(1_000 + i as i64, vec![("svc", values[i % 3].as_str())]))
        .collect();
    let object = write_rows(&blocks_cfg(4), &records);
    (records, object)
}

// --- acceptance tests -------------------------------------------------------

/// One block of a three-block row group reads correctly through the chunk's
/// dictionary on both single-block paths: the ranged reader's
/// `decode_block_in_group`, and a scan over a sparse object holding only the
/// directories, the dictionary page and block 1's id page.
///
/// Wrong implementations this rules out, each shown failing: a ranged decode
/// that hands the block's pages over without resolving the chunk's dictionary
/// (tag 13 has nothing to index, `Corrupted`); a projected fetch without the
/// dictionary extent (the sparse scan reads bytes it was never given).
#[test]
fn single_block_read_resolves_through_the_row_group_dictionary() {
    let (records, object) = three_block_object();
    let dir = page_dir(&object);
    assert_eq!(dir.block_count(), 3);
    let svc = dyn_column(&object, "svc");
    let c = chunk(&dir, 0, svc);
    assert_eq!(
        encs(&c),
        [Enc::DictPage, Enc::DictIds, Enc::DictIds, Enc::DictIds]
    );

    assert_eq!(
        ranged_block(&object, 1).expect("ranged block 1"),
        records[4..8]
    );

    let ranges = dir
        .projected_page_ranges(0, &[1], Some(&HashSet::from([COL_TS, COL_STREAM_REF, svc])))
        .expect("ranges");
    let sparse = sparse_with(&object, &ranges);
    let reader = RlogReader::from_source(&sparse, &RlogConfig::default()).expect("reader");
    let mut scan = reader
        .scan_blocks_subset(
            &Predicate::And(vec![]),
            &[],
            &ColumnSelection::fixed_only().with_attr("svc"),
            &[1],
        )
        .expect("scan");
    let rows = scan
        .next_block(&sparse)
        .expect("sparse block 1")
        .expect("block 1");
    let got: Vec<Option<&str>> = rows.iter().map(|r| attr(r, "svc")).collect();
    let want: Vec<Option<&str>> = records[4..8].iter().map(|r| attr(r, "svc")).collect();
    assert_eq!(got, want);
}

/// The row path (`push`), the columnar path (`push_columnar`) and the columnar
/// path with its string columns already in dictionary shape write the same
/// bytes, and the object reads back exactly on the row and the columnar scan.
/// `flag` has two distinct values in 100 rows over three blocks, and block 0
/// holds only `zeta`, so a dictionary kept in first-seen order (by row or by
/// block) is `[zeta, alpha]` and a sorted one `[alpha, zeta]`; `opt` is absent
/// from every fifth row, so
/// its chunk carries presence pages beside its id pages.
///
/// Wrong implementations this rules out, each shown failing: a dictionary in
/// first-seen order (the page is not ascending, decode refuses it); a columnar
/// path that keeps per-block pages while the row path takes the dictionary
/// (the bytes differ).
#[test]
fn row_and_columnar_paths_identical_with_row_group_dictionaries() {
    let services: Vec<String> = (0..5).map(|i| word(10 + i, 16)).collect();
    let records: Vec<LogRecord> = (0..100)
        .map(|i| {
            let flag = if i < 34 || i % 3 == 0 {
                "zeta"
            } else {
                "alpha"
            };
            let mut attrs = vec![("flag", flag)];
            if i % 5 != 0 {
                attrs.push(("opt", if i % 2 == 0 { "even" } else { "odd" }));
            }
            attrs.push(("svc", services[i % 5].as_str()));
            record(1_000 + i as i64, attrs)
        })
        .collect();
    let cfg = blocks_cfg(34);
    let rows = write_rows(&cfg, &records);
    let columnar = write_columnar(&cfg, ColumnarLogBatch::from_records(&records));
    let dictionaries = write_columnar(
        &cfg,
        ColumnarLogBatch::from_records(&records).with_dictionaries(),
    );
    assert!(rows == columnar, "row and columnar objects differ");
    assert!(
        rows == dictionaries,
        "row and dictionary-shaped objects differ"
    );

    let dir = page_dir(&rows);
    assert_eq!(dir.block_count(), 3);
    let flag = dyn_column(&rows, "flag");
    let opt = dyn_column(&rows, "opt");
    let flag_chunk = chunk(&dir, 0, flag);
    assert_eq!(
        encs(&flag_chunk),
        [Enc::DictPage, Enc::DictIds, Enc::DictIds, Enc::DictIds]
    );
    assert_eq!(flag_chunk.dict_page().expect("dict").comp, COMP_NONE);
    let entries = decode_dict_page(&dict_page_bytes(&rows, &flag_chunk)).expect("dict decodes");
    assert_eq!(entries, [b"alpha".to_vec(), b"zeta".to_vec()]);
    assert_eq!(
        encs(&chunk(&dir, 0, opt)),
        [
            Enc::DictPage,
            Enc::Bitmap,
            Enc::DictIds,
            Enc::Bitmap,
            Enc::DictIds,
            Enc::Bitmap,
            Enc::DictIds
        ]
    );

    assert_eq!(scan_all(&rows), records);

    let reader = RlogReader::new(&rows, &RlogConfig::default()).expect("reader");
    let mut scan = reader
        .scan_blocks(&Predicate::And(vec![]), &[], &ColumnSelection::all())
        .expect("scan");
    // A block's view narrows the group dictionary to the entries the block
    // uses, the dictionary a tag 7 page over its values would have: block 0
    // holds only `zeta`.
    let mut at = 0usize;
    let mut block = 0usize;
    while let Some(view) = scan.next_block_columnar(rows.as_slice()).expect("block") {
        let dict = view.str_dict(flag).expect("flag dictionary");
        let want: &[&[u8]] = if block == 0 {
            &[b"zeta"]
        } else {
            &[b"alpha", b"zeta"]
        };
        assert_eq!(dict.dict(), want);
        block += 1;
        let opt_dict = view.str_dict(opt).expect("opt dictionary");
        for i in 0..view.surviving_count() {
            let r = &records[at + i];
            assert_eq!(dict.value_at(i), attr(r, "flag").map(str::as_bytes));
            assert_eq!(opt_dict.value_at(i), attr(r, "opt").map(str::as_bytes));
        }
        at += view.surviving_count();
    }
    assert_eq!(at, records.len());
}

/// A dictionary page whose stored bytes change after it was written fails a
/// read of every block of its row group, on both the scan and the ranged path,
/// before any tag 13 page is decoded. The change bumps the last byte of the
/// last entry, which keeps the entries ascending and the page decodable, so
/// only the page's own crc32c can see it; the block crc covers no dictionary
/// page and still matches. A projection that leaves the column out still reads
/// every block.
///
/// Wrong implementations this rules out, each shown failing: no dictionary crc
/// check (every read returns the altered value); a check made only for the
/// group's first block (blocks 1 and 2 read the altered value).
#[test]
fn corrupt_dictionary_page_fails_a_whole_block_read_of_every_other_block() {
    let (_, object) = three_block_object();
    let dir = page_dir(&object);
    let svc = dyn_column(&object, "svc");
    let c = chunk(&dir, 0, svc);
    let d = *c.dict_page().expect("dictionary page");
    assert_eq!(d.comp, COMP_NONE, "the page is stored raw");
    let last = (blocks_offset(&object) + c.offset + d.len - 1) as usize;
    let mut bad = object.clone();
    bad[last] += 1;
    let entries = decode_dict_page(&dict_page_bytes(&bad, &c)).expect("still a valid page");
    assert_eq!(entries.len(), 3);

    for block in 0..3 {
        let whole = scan_block(&bad, block, &ColumnSelection::all());
        assert!(is_corrupted(&whole), "scan block {block}: {whole:?}");
        let ranged = ranged_block(&bad, block);
        assert!(is_corrupted(&ranged), "ranged block {block}: {ranged:?}");
        let without = scan_block(&bad, block, &ColumnSelection::fixed_only());
        assert_eq!(without.expect("svc not read").len(), 4);
    }
}

/// A page's cost under the writer's size rule when placed at `block`: its
/// stored bytes plus its PAGE_DIR entry, then the same over its encoded
/// length, which is what a rule comparing sizes before the envelope counts.
fn page_cost(page: &SealedPage, block: usize) -> (u64, u64) {
    let entry = |comp: u8, len: u64| {
        PageEntry {
            block: block as u32,
            enc: page.enc,
            comp,
            len,
            uncomp_len: page.uncomp_len,
            crc32c: 0,
        }
        .encoded_len()
    };
    let stored = page.stored.len() as u64;
    (
        stored + entry(page.comp, stored),
        page.uncomp_len + entry(COMP_NONE, page.uncomp_len),
    )
}

/// A chunk's two layouts for `blocks` of values, costed page by page with the
/// codec the writer uses: per-block value pages, and one dictionary page plus
/// per-block id pages.
#[derive(Debug)]
struct ChunkCosts {
    /// Stored bytes plus PAGE_DIR entries: the size rule's two sides.
    per_block: u64,
    dict: u64,
    /// The same over encoded sizes.
    per_block_encoded: u64,
    dict_encoded: u64,
    /// Stored page bytes alone, the chunk's extent under each layout.
    per_block_pages: u64,
    dict_pages: u64,
}

fn chunk_costs(blocks: &[Vec<String>]) -> ChunkCosts {
    let mut sorted: Vec<&[u8]> = blocks.iter().flatten().map(|v| v.as_bytes()).collect();
    sorted.sort_unstable();
    sorted.dedup();
    let dict_page = seal_page(Enc::DictPage, encode_dict_page(&sorted), ZSTD_LEVEL);
    let (dict, dict_encoded) = page_cost(&dict_page, blocks.len());
    let mut c = ChunkCosts {
        per_block: 0,
        dict,
        per_block_encoded: 0,
        dict_encoded,
        per_block_pages: 0,
        dict_pages: dict_page.stored.len() as u64,
    };
    for (block, b) in blocks.iter().enumerate() {
        let values: Vec<&[u8]> = b.iter().map(|v| v.as_bytes()).collect();
        let own = smallest_stored(string_candidates(&values), ZSTD_LEVEL).expect("candidates");
        let (stored, encoded) = page_cost(&own, block);
        c.per_block += stored;
        c.per_block_encoded += encoded;
        c.per_block_pages += own.stored.len() as u64;
        let ids: Vec<u64> = values
            .iter()
            .map(|v| sorted.partition_point(|e| e < v) as u64)
            .collect();
        let id_page = seal_page(
            Enc::DictIds,
            encode_dict_ids(&ids, sorted.len()),
            ZSTD_LEVEL,
        );
        let (stored, encoded) = page_cost(&id_page, block);
        c.dict += stored;
        c.dict_encoded += encoded;
        c.dict_pages += id_page.stored.len() as u64;
    }
    c
}

/// Writes `blocks` (each but the last of the first's length) as one row group
/// of attribute `k`, checks it scans back, and returns the chunk and the
/// object's length.
fn write_k_blocks(blocks: &[Vec<String>]) -> (ChunkEntry, usize) {
    let per = blocks[0].len();
    let records: Vec<LogRecord> = blocks
        .iter()
        .flatten()
        .enumerate()
        .map(|(i, v)| record(1_000 + i as i64, vec![("k", v.as_str())]))
        .collect();
    let object = write_rows(&blocks_cfg(per), &records);
    assert_eq!(scan_all(&object), records);
    let dir = page_dir(&object);
    assert_eq!(dir.groups.len(), 1);
    assert_eq!(dir.block_count(), blocks.len() as u64);
    (chunk(&dir, 0, dyn_column(&object, "k")), object.len())
}

fn extent(c: &ChunkEntry) -> u64 {
    c.extent().expect("extent").1
}

/// The dictionary is taken only when the chunk's distinct values are at most
/// half its values and the dictionary page plus the id pages store strictly
/// smaller than the per-block value pages would.
///
/// Each case is costed with the writer's own codec and PAGE_DIR encoder, as
/// stored page bytes plus one 9-byte entry per page (every varint here is
/// under 128, except the 32-entry dictionary page's two lengths):
///
/// - one block `ABAB` of 8-byte words: a 21-byte tag 7 page, 30 with its
///   entry, against a 19-byte dictionary page and a 1-byte id page, 38.
///   Keeps its tag 7 page: a one-block group never profits from a
///   dictionary, since the ids and a second entry only add to it.
/// - two blocks `ABAB|ABAB`, the same column: 60 against 19 + 9 + 2 * 10,
///   48. Takes the dictionary.
/// - two blocks `VV|VV` of one 5-byte value: an 8-byte tag 7 page per block,
///   34, against a 7-byte dictionary page and two empty id pages, also 34. A
///   tie keeps the per-block pages. With a 6-byte value, 36 against 35,
///   takes the dictionary.
/// - `AB|AB`, exactly half: two 18-byte plain pages, 54, against 48. Takes
///   the dictionary.
/// - `ABC|AB`, three distinct in five, one over half: 63 against 57, and
///   keeps its per-block pages on the half rule alone.
/// - 32 blocks of one own value twice, exactly half: 640 against 652, where
///   32 entries need 5-bit ids. Keeps per-block pages.
/// - `ABCD|AB`, four distinct in six: 72 against 66, keeps per-block pages.
///
/// Wrong implementations this rules out, each shown failing: comparing page
/// bytes with no PAGE_DIR entries (the one-block case takes it at 21 against
/// 20); not charging the dictionary page's own entry (one block, 29 against
/// 30); `<=` in place of `<` (the 5-byte tie takes it); the half rule alone,
/// with no size comparison (the 32-block chunk takes it); a strict half
/// (`2 * distinct < present`, `AB|AB` keeps per-block pages); a loose half
/// by one (`2 * distinct > present + 1` or `distinct > present.div_ceil(2)`,
/// `ABC|AB` takes it); no half rule, size only (`ABCD|AB` takes it).
#[test]
fn dictionary_is_only_chosen_when_it_is_smaller() {
    let w = |i: usize| word(100 + i, 8);
    let takes = |blocks: &[Vec<String>], object_len: usize| {
        let c = chunk_costs(blocks);
        let (got, len) = write_k_blocks(blocks);
        let mut want = vec![Enc::DictPage];
        want.extend(vec![Enc::DictIds; blocks.len()]);
        assert_eq!(encs(&got), want, "{c:?}");
        assert_eq!(extent(&got), c.dict_pages);
        assert_eq!(len, object_len);
        c
    };
    let keeps = |blocks: &[Vec<String>], enc: Enc, object_len: usize| {
        let c = chunk_costs(blocks);
        let (got, len) = write_k_blocks(blocks);
        assert_eq!(encs(&got), vec![enc; blocks.len()], "{c:?}");
        assert!(got.dict_page().is_none());
        assert_eq!(extent(&got), c.per_block_pages);
        assert_eq!(len, object_len);
        c
    };

    let one = vec![vec![w(0), w(1), w(0), w(1)]];
    let c = keeps(&one, Enc::Dict, 540);
    assert_eq!((c.per_block, c.dict), (30, 38));
    assert_eq!((c.per_block_pages, c.dict_pages), (21, 20));
    let two = vec![one[0].clone(), one[0].clone()];
    let c = takes(&two, 701);
    assert_eq!((c.per_block, c.dict), (60, 48));

    let v = |n: usize| vec![vec![word(7, n); 2], vec![word(7, n); 2]];
    let c = keeps(&v(5), Enc::Dict, 682);
    assert_eq!((c.per_block, c.dict), (34, 34));
    let c = takes(&v(6), 681);
    assert_eq!((c.per_block, c.dict), (36, 35));

    let half = vec![vec![w(0), w(1)], vec![w(0), w(1)]];
    let c = takes(&half, 699);
    assert_eq!((c.per_block, c.dict), (54, 48));

    let half_larger: Vec<Vec<String>> = (0..32).map(|i| vec![w(i), w(i)]).collect();
    let c = keeps(&half_larger, Enc::Dict, 4503);
    assert_eq!((c.per_block, c.dict), (640, 652));

    for (blocks, costs, object_len) in [
        (
            vec![vec![w(0), w(1), w(2)], vec![w(0), w(1)]],
            (63, 57),
            717,
        ),
        (
            vec![vec![w(0), w(1), w(2), w(3)], vec![w(0), w(1)]],
            (72, 66),
            726,
        ),
    ] {
        let c = chunk_costs(&blocks);
        assert_eq!((c.per_block, c.dict), costs);
        let records: Vec<LogRecord> = blocks
            .iter()
            .flatten()
            .enumerate()
            .map(|(i, v)| record(1_000 + i as i64, vec![("k", v.as_str())]))
            .collect();
        let object = write_rows(&blocks_cfg(blocks[0].len()), &records);
        assert_eq!(scan_all(&object), records);
        let dir = page_dir(&object);
        assert_eq!(dir.block_count(), 2);
        let got = chunk(&dir, 0, dyn_column(&object, "k"));
        assert_eq!(encs(&got), [Enc::Plain, Enc::Plain]);
        assert_eq!(extent(&got), c.per_block_pages);
        assert_eq!(object.len(), object_len);
    }
}

/// The size rule compares stored bytes, after the page envelope decides on
/// zstd. Four blocks of 16 values over 8 own 23-byte strings each: every
/// per-block page stays under the 512-byte compression floor and is stored
/// raw, while the 32-entry dictionary page, 769 bytes encoded, is above it and
/// compresses to 134. With PAGE_DIR entries, on encoded sizes the dictionary
/// is the larger side (856 against 844) and the chunk would keep its per-block
/// pages; on stored sizes it is the smaller one (221 against 844) and the chunk
/// takes it.
///
/// Wrong implementations this rules out, each shown failing: a rule comparing
/// every page's encoded length, and one counting only the dictionary page at
/// its encoded length.
#[test]
fn dictionary_size_rule_compares_bytes_after_compression() {
    let blocks: Vec<Vec<String>> = (0..4)
        .map(|b| {
            (0..16)
                .map(|i| format!("region-cluster-node-{:03}", b * 8 + i % 8))
                .collect()
        })
        .collect();
    let c = chunk_costs(&blocks);
    assert!(c.per_block_encoded < c.dict_encoded, "{c:?}");
    assert!(c.dict < c.per_block, "{c:?}");
    assert_eq!(
        (c.per_block_encoded, c.dict_encoded, c.per_block, c.dict),
        (844, 856, 844, 221)
    );

    let (got, len) = write_k_blocks(&blocks);
    let mut want = vec![Enc::DictPage];
    want.extend([Enc::DictIds; 4]);
    assert_eq!(encs(&got), want);
    assert_eq!(extent(&got), c.dict_pages);
    let dict = got.dict_page().expect("dictionary page");
    assert_eq!(dict.comp, COMP_ZSTD);
    assert!(dict.uncomp_len >= COMPRESSION_FLOOR as u64);
    assert_eq!((dict.uncomp_len, dict.len), (769, 134));
    assert!(got.pages[1..].iter().all(|p| p.comp == COMP_NONE));
    assert_eq!(len, 1134);
}

/// A row-group dictionary holds at most `MAX_DICT_ENTRIES` (65,536) entries.
/// One full group, 32 blocks of 4,100 12-byte values, each value present about
/// twice: at 65,536 distinct values the chunk takes a dictionary, and at 65,537
/// it keeps its per-block pages though the dictionary would still store
/// smaller. The decoder refuses a tag 12 page counting 65,537 entries and
/// accepts one counting 65,536.
///
/// Wrong implementations this rules out, each shown failing: a writer with no
/// entry cap (it emits the 65,537-entry dictionary and its own reader refuses
/// the object); a cap checked with `>=` (65,536 falls back).
#[test]
fn dictionary_entry_cap_is_inclusive() {
    const ROWS_PER_BLOCK: usize = 4_100;
    let cap = MAX_DICT_ENTRIES as usize;
    for (distinct, takes) in [(cap, true), (cap + 1, false)] {
        let blocks: Vec<Vec<String>> = (0..32)
            .map(|b| {
                (0..ROWS_PER_BLOCK)
                    .map(|i| word((b * ROWS_PER_BLOCK + i) % distinct, 12))
                    .collect()
            })
            .collect();
        let unique: HashSet<&String> = blocks.iter().flatten().collect();
        assert_eq!(unique.len(), distinct);
        let c = chunk_costs(&blocks);
        assert!(c.dict < c.per_block, "{distinct}: {c:?}");
        let (got, _) = write_k_blocks(&blocks);
        if takes {
            let mut want = vec![Enc::DictPage];
            want.extend([Enc::DictIds; 32]);
            assert_eq!(encs(&got), want);
            assert_eq!(extent(&got), c.dict_pages);
        } else {
            assert!(got.dict_page().is_none());
            assert_eq!(got.pages.len(), 32);
            assert!(got.pages.iter().all(|p| p.enc != Enc::DictIds));
            assert_eq!(extent(&got), c.per_block_pages);
        }
    }

    let entries: Vec<String> = (0..=cap).map(|i| format!("{i:05}")).collect();
    let refs: Vec<&[u8]> = entries.iter().map(|e| e.as_bytes()).collect();
    let at_cap = decode_dict_page(&encode_dict_page(&refs[..cap])).expect("at the cap");
    assert_eq!(at_cap.len(), cap);
    match decode_dict_page(&encode_dict_page(&refs)) {
        Err(LogSegError::Corrupted(m)) => {
            assert!(m.contains("count 65537 outside 1..=65536"), "{m}")
        }
        other => panic!("65,537 entries: {:?}", other.map(|e| e.len())),
    }
}

fn page(block: u32, enc: Enc) -> PageEntry {
    PageEntry {
        block,
        enc,
        comp: COMP_NONE,
        len: 4,
        uncomp_len: 4,
        crc32c: 0,
    }
}

fn one_chunk(block_count: u32, pages: Vec<PageEntry>) -> PageDir {
    PageDir {
        groups: vec![GroupEntry {
            first_block: 0,
            block_count,
            chunks: vec![ChunkEntry {
                column_id: 10,
                offset: 0,
                pages,
            }],
        }],
    }
}

fn decodes(dir: &PageDir) -> Result<PageDir, LogSegError> {
    PageDir::decode(&dir.encode())
}

/// A tag 12 page may name the block one past the group's last, precede block
/// 0, and take the chunk one page past two per block, and only a tag 12 page,
/// only as the chunk's first page. The same three shapes on any other page are
/// `Corrupted`, as are a second dictionary page, a dictionary page naming any
/// other block, an id page with no dictionary page, and a dictionary page with
/// no id page. `block_pages` never returns the dictionary page;
/// `block_dict_pages` returns it for every block with an id page.
///
/// Wrong implementations this rules out, each shown failing: the
/// `block_count` index admitted for any encoding (the plain page naming block 2
/// decodes); no first-page rule (the dictionary page after block 0 decodes).
#[test]
fn page_dir_relaxations_apply_only_to_the_dictionary_page() {
    let good = one_chunk(
        2,
        vec![
            page(2, Enc::DictPage),
            page(0, Enc::Bitmap),
            page(0, Enc::DictIds),
            page(1, Enc::Bitmap),
            page(1, Enc::DictIds),
        ],
    );
    let dir = decodes(&good).expect("the dictionary page's three relaxations");
    assert_eq!(dir, good);
    for block in 0..2 {
        let pages = dir.block_pages(block).expect("block pages");
        assert_eq!(pages.len(), 2);
        assert!(pages.iter().all(|p| p.desc.enc != Enc::DictPage));
        let dicts = dir.block_dict_pages(block).expect("dict pages");
        assert_eq!(dicts.len(), 1);
        assert_eq!((dicts[0].desc.enc, dicts[0].offset), (Enc::DictPage, 0));
    }
    assert!(dir.block_pages(2).is_none());
    assert_eq!(
        dir.groups[0].chunks[0].dict_page().map(|p| p.block),
        Some(2)
    );

    let refused: Vec<(&str, Vec<PageEntry>)> = vec![
        (
            "plain page naming block_count",
            vec![page(0, Enc::Plain), page(2, Enc::Plain)],
        ),
        (
            "plain page preceding block 0",
            vec![page(1, Enc::Plain), page(0, Enc::Plain)],
        ),
        (
            "five pages over two blocks without a dictionary",
            vec![
                page(0, Enc::Bitmap),
                page(0, Enc::Plain),
                page(1, Enc::Bitmap),
                page(1, Enc::Plain),
                page(1, Enc::Plain),
            ],
        ),
        (
            "dictionary page after block 0",
            vec![
                page(0, Enc::DictIds),
                page(2, Enc::DictPage),
                page(1, Enc::DictIds),
            ],
        ),
        (
            "dictionary page after a presence page",
            vec![
                page(0, Enc::Bitmap),
                page(2, Enc::DictPage),
                page(0, Enc::DictIds),
                page(1, Enc::Bitmap),
                page(1, Enc::DictIds),
            ],
        ),
        (
            "dictionary page naming block 1",
            vec![
                page(1, Enc::DictPage),
                page(0, Enc::DictIds),
                page(1, Enc::DictIds),
            ],
        ),
        (
            "dictionary page naming block 0",
            vec![
                page(0, Enc::DictPage),
                page(0, Enc::DictIds),
                page(1, Enc::DictIds),
            ],
        ),
        (
            "two dictionary pages",
            vec![
                page(2, Enc::DictPage),
                page(2, Enc::DictPage),
                page(0, Enc::DictIds),
                page(1, Enc::DictIds),
            ],
        ),
        (
            "id page without a dictionary page",
            vec![page(0, Enc::DictIds), page(1, Enc::DictIds)],
        ),
        (
            "dictionary page without an id page",
            vec![page(2, Enc::DictPage)],
        ),
    ];
    for (name, pages) in refused {
        let got = decodes(&one_chunk(2, pages));
        assert!(is_corrupted(&got), "{name}: {got:?}");
    }
}

/// A tag 13 id at or past the dictionary's length is `Corrupted` in the codec
/// and through the block decode, for every width, including an id equal to the
/// length and a one-entry dictionary's zero-width page.
///
/// Wrong implementations this rules out, each shown failing: no range check
/// (the ids decode); an off-by-one range check, `id > dict_len` (an id equal to
/// the length decodes).
#[test]
fn ids_past_the_dictionary_are_refused() {
    let one = vec![b"a".to_vec()];
    let three = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
    let five: Vec<Vec<u8>> = (b'a'..=b'e').map(|c| vec![c]).collect();
    let cases: Vec<(&[Vec<u8>], Vec<u64>)> = vec![
        (&three, vec![0, 1, 3]),
        (&five, vec![4, 5]),
        (&five, vec![7, 0]),
    ];
    for (dict, ids) in cases {
        let body = encode_dict_ids(&ids, dict.len());
        let got = decode_dict_ids(&body, ids.len(), dict.len());
        assert!(is_corrupted(&got), "{ids:?} over {}: {got:?}", dict.len());
        let block = block_with_ids(ids.len(), body, dict);
        assert!(is_corrupted(&block), "block {ids:?} over {}", dict.len());
    }

    let good = encode_dict_ids(&[0, 4, 2], 5);
    assert_eq!(decode_dict_ids(&good, 3, 5).expect("in range"), [0, 4, 2]);
    let block = block_with_ids(3, good, &five).expect("block decodes");
    assert_eq!(block.record_count(), 3);

    assert!(encode_dict_ids(&[0, 0, 0], 1).is_empty());
    assert_eq!(decode_dict_ids(&[], 3, 1).expect("width 0"), [0, 0, 0]);
    assert!(is_corrupted(&decode_dict_ids(&[0], 3, 1)));
    assert!(block_with_ids(3, Vec::new(), &one).is_ok());
}

fn block_with_ids(
    count: usize,
    body: Vec<u8>,
    dict: &[Vec<u8>],
) -> Result<ravel_logseg::block::DecodedBlock, LogSegError> {
    let desc = PageDesc {
        column_id: COL_SEVERITY_TEXT,
        enc: Enc::DictIds,
        comp: COMP_NONE,
        len: body.len() as u64,
        uncomp_len: body.len() as u64,
    };
    read_block_pages_with_dicts(
        count,
        &[desc],
        &[Some(body)],
        &[(COL_SEVERITY_TEXT, dict)],
        &[],
        PageCounters::default(),
    )
}

/// A projection that keeps only block 1 of a three-block group fetches the
/// chunk's dictionary page exactly once, ahead of block 1's id page, and
/// nothing of blocks 0 and 2. Keeping all three blocks still fetches it once.
/// A column the projection leaves out contributes nothing, its dictionary
/// included.
///
/// Wrong implementations this rules out, each shown failing: the dictionary
/// omitted from the projection (the one-block list has one extent); the
/// dictionary emitted per kept id page (the three-block list holds it three
/// times).
#[test]
fn projected_fetch_includes_the_dictionary_page() {
    let (_, object) = three_block_object();
    let dir = page_dir(&object);
    let svc = dyn_column(&object, "svc");
    let c = chunk(&dir, 0, svc);
    let d = *c.dict_page().expect("dictionary page");
    let offsets = c.page_offsets().expect("offsets");
    let keep = HashSet::from([svc]);

    let one = dir
        .projected_page_ranges(0, &[1], Some(&keep))
        .expect("ranges");
    assert_eq!(one, [(c.offset, d.len), (offsets[2], c.pages[2].len)]);
    assert_eq!(one.iter().filter(|r| **r == (c.offset, d.len)).count(), 1);

    let all = dir
        .projected_page_ranges(0, &[0, 1, 2], Some(&keep))
        .expect("ranges");
    let want: Vec<(u64, u64)> = offsets
        .iter()
        .copied()
        .zip(c.pages.iter().map(|p| p.len))
        .collect();
    assert_eq!(all, want);
    assert_eq!(all.iter().filter(|r| **r == (c.offset, d.len)).count(), 1);

    let none = dir
        .projected_page_ranges(0, &[1], Some(&HashSet::from([COL_SEVERITY_TEXT + 1_000])))
        .expect("ranges");
    assert!(none.is_empty());
}

// --- corrupt input ----------------------------------------------------------

fn mutate(bytes: &[u8], kind: u8, at: usize, bit: u8, splice: &[u8]) -> Vec<u8> {
    let mut m = bytes.to_vec();
    match kind % 3 {
        0 if !m.is_empty() => {
            let i = at % m.len();
            m[i] ^= 1 << (bit % 8);
        }
        1 => m.truncate(at % (m.len() + 1)),
        _ => {
            let i = at % (m.len() + 1);
            m.splice(i..i, splice.iter().copied());
        }
    }
    m
}

fn arb_dict_and_ids() -> impl Strategy<Value = (Vec<Vec<u8>>, Vec<u64>)> {
    proptest::collection::btree_set(proptest::collection::vec(any::<u8>(), 0..10), 1..24)
        .prop_flat_map(|set| {
            let dict: Vec<Vec<u8>> = set.into_iter().collect();
            let n = dict.len() as u64;
            (Just(dict), proptest::collection::vec(0..n, 0..64))
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Tag 12 and tag 13 round-trip every dictionary and id list through the
    /// codec, and the id page decodes through the block decode. Any flip, truncation or splice of
    /// either page decodes to a well-formed result (a nonempty, strictly
    /// ascending dictionary; exactly `count` ids each inside the dictionary) or
    /// fails as `Corrupted`, never panics and never returns another error kind.
    #[test]
    fn dictionary_and_id_pages_round_trip_and_corruption_is_typed(
        (dict, ids) in arb_dict_and_ids(),
        kind in any::<u8>(),
        at in any::<usize>(),
        bit in any::<u8>(),
        splice in proptest::collection::vec(any::<u8>(), 1..6),
    ) {
        let sorted: Vec<&[u8]> = dict.iter().map(Vec::as_slice).collect();
        let page = encode_dict_page(&sorted);
        prop_assert_eq!(&decode_dict_page(&page).expect("dictionary decodes"), &dict);
        let body = encode_dict_ids(&ids, dict.len());
        let want: Vec<u32> = ids.iter().map(|&i| i as u32).collect();
        prop_assert_eq!(decode_dict_ids(&body, ids.len(), dict.len()).expect("ids decode"), want);
        let block = block_with_ids(ids.len(), body.clone(), &dict).expect("block decodes");
        prop_assert_eq!(block.record_count(), ids.len());

        match decode_dict_page(&mutate(&page, kind, at, bit, &splice)) {
            Ok(v) => {
                prop_assert!(!v.is_empty());
                prop_assert!(v.windows(2).all(|w| w[0] < w[1]));
            }
            Err(LogSegError::Corrupted(_)) => {}
            Err(other) => prop_assert!(false, "untyped error {other:?}"),
        }
        let bad = mutate(&body, kind, at, bit, &splice);
        match decode_dict_ids(&bad, ids.len(), dict.len()) {
            Ok(v) => {
                prop_assert_eq!(v.len(), ids.len());
                prop_assert!(v.iter().all(|&i| (i as usize) < dict.len()));
            }
            Err(LogSegError::Corrupted(_)) => {}
            Err(other) => prop_assert!(false, "untyped error {other:?}"),
        }
        let got = block_with_ids(ids.len(), bad, &dict);
        prop_assert!(got.is_ok() || is_corrupted(&got));
    }
}
