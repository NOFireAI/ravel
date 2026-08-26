//! RLOG version 4: row groups with column-major page placement and the
//! PAGE_DIR section (ADR-0699).
//!
//! Covers the acceptance anchors ADR-0699 names: a differential check that
//! a version-4 object decodes to the same records as the version-3 writer
//! produced for the same batch, projection pushdown through PAGE_DIR, the
//! row-group boundary cases, and the corrupt-input cases (a flipped page
//! byte, a flipped PAGE_DIR byte, a truncated last row group).
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_logseg::footer::{
    self, COMP_ZSTD, LogFooter, SectionDesc, kind, open, write_footer_and_trailer_versioned,
};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    AttrValue, ColumnSelection, LogRecord, LogSegError, LogStreamId, ObjectIdentity, Predicate,
    RlogConfig, RlogReader, RlogWriter, read_section, stream_attrs_bytes,
};

// --- fixtures ---------------------------------------------------------------

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

fn stream_blob(n: u8) -> Vec<u8> {
    stream_attrs_bytes(
        &[("service.name".to_string(), AttrValue::Str(format!("s{n}")))],
        "scope",
        "1",
        &[],
    )
}

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [3u8; 16],
        shard: 0,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: 2,
    }
}

fn writer(cfg: RlogConfig) -> RlogWriter {
    RlogWriter::new(cfg, identity())
}

fn write_v4(cfg: RlogConfig, recs: &[LogRecord]) -> Vec<u8> {
    let mut w = writer(cfg);
    for r in recs {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish v4")
}

fn write_v3(cfg: RlogConfig, recs: &[LogRecord]) -> Vec<u8> {
    let mut w = writer(cfg);
    for r in recs {
        w.push(r.clone()).expect("push");
    }
    w.finish_v3_for_tests().expect("finish v3")
}

fn scan_all(object: &[u8]) -> Vec<LogRecord> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg).expect("open");
    reader.scan(&Predicate::And(Vec::new())).expect("scan").0
}

fn page_dir_of(object: &[u8]) -> PageDir {
    let cfg = RlogConfig::default();
    let ftr = open(object).expect("open footer");
    let raw = read_section(
        object,
        ftr.section(kind::PAGE_DIR).expect("PAGE_DIR present"),
        &cfg,
    )
    .expect("read PAGE_DIR");
    PageDir::decode(&raw).expect("decode PAGE_DIR")
}

/// One block per `per_block` records, `blocks` blocks worth, over two streams
/// so the stream columns are not constant.
fn linear_corpus(records: usize) -> Vec<LogRecord> {
    (0..records)
        .map(|i| {
            let s = (i % 2) as u8;
            LogRecord {
                stream_id: sid(s),
                stream_attrs: stream_blob(s),
                ts_ns: i as i64,
                observed_ts_ns: i as i64 + 1,
                severity_num: (i % 24) as u8,
                severity_text: if i % 2 == 0 { "INFO" } else { "WARN" }.to_string(),
                body: format!("record {i} of the corpus"),
                trace_id: Some([(i % 251) as u8; 16]),
                span_id: Some([(i % 251) as u8; 8]),
                flags: (i as u32) & 7,
                attrs: vec![
                    ("k".to_string(), AttrValue::I64(i as i64 % 11)),
                    ("t".to_string(), AttrValue::Str(format!("v{}", i % 5))),
                ],
            }
        })
        .collect()
}

// --- object surgery ---------------------------------------------------------

/// One section's stored bytes plus the descriptor fields that travel with them.
struct Section {
    kind: u32,
    stored: Vec<u8>,
    comp: u8,
    uncomp_len: u64,
}

/// Splits an object into its footer and its sections' stored bytes.
fn explode(object: &[u8]) -> (LogFooter, Vec<Section>) {
    let ftr = open(object).expect("open footer");
    let sections = ftr
        .sections
        .iter()
        .map(|d| {
            let start = d.offset as usize;
            let end = start + d.len as usize;
            Section {
                kind: d.kind,
                stored: object[start..end].to_vec(),
                comp: d.comp,
                uncomp_len: d.uncomp_len,
            }
        })
        .collect();
    (ftr, sections)
}

/// Reassembles an object from sections, recomputing every offset, length, and
/// section crc, so a rewritten section yields a structurally valid object whose
/// only defect is the one the test introduced.
fn assemble(footer: LogFooter, sections: &[Section]) -> Vec<u8> {
    assemble_versioned(footer, sections, footer::VERSION)
}

/// [`assemble`] with the trailer version named, so a version-3 object stays
/// version 3 through a rewrite. The footer crc covers the version byte, so
/// patching it afterwards would fail the crc instead of whatever the test meant
/// to exercise.
fn assemble_versioned(mut footer: LogFooter, sections: &[Section], version: u16) -> Vec<u8> {
    let mut object = Vec::new();
    let mut descs = Vec::with_capacity(sections.len());
    for s in sections {
        let offset = object.len() as u64;
        object.extend_from_slice(&s.stored);
        descs.push(SectionDesc {
            kind: s.kind,
            offset,
            len: s.stored.len() as u64,
            crc32c: crc32c::crc32c(&s.stored),
            comp: s.comp,
            uncomp_len: s.uncomp_len,
        });
    }
    footer.sections = descs;
    write_footer_and_trailer_versioned(&mut object, &footer, version);
    object
}

/// Replaces the PAGE_DIR section's *uncompressed* content, re-compressing it
/// the way the writer does.
fn with_page_dir_bytes(object: &[u8], raw: Vec<u8>) -> Vec<u8> {
    let (footer, mut sections) = explode(object);
    let stored = zstd::bulk::compress(&raw, 3).expect("compress");
    let slot = sections
        .iter_mut()
        .find(|s| s.kind == kind::PAGE_DIR)
        .expect("PAGE_DIR present");
    slot.uncomp_len = raw.len() as u64;
    slot.stored = stored;
    slot.comp = COMP_ZSTD;
    assemble(footer, &sections)
}

/// Truncates the BLOCKS section by `n` bytes, which cuts the tail off the last
/// row group (its pages are placed last).
fn truncate_blocks(object: &[u8], n: usize) -> Vec<u8> {
    let (footer, mut sections) = explode(object);
    let slot = sections
        .iter_mut()
        .find(|s| s.kind == kind::BLOCKS)
        .expect("BLOCKS present");
    let keep = slot.stored.len().saturating_sub(n);
    slot.stored.truncate(keep);
    slot.uncomp_len = keep as u64;
    assemble(footer, &sections)
}

/// Flips one byte at an absolute offset in the object.
fn flip(object: &[u8], at: usize) -> Vec<u8> {
    let mut out = object.to_vec();
    out[at] ^= 0xff;
    out
}

fn is_corrupted(r: Result<impl Sized, LogSegError>) -> bool {
    matches!(r, Err(LogSegError::Corrupted(_)))
}

/// Asserts the error is `Corrupted` and that its message names `needle`.
///
/// Used where a weaker "some error happened" would pass on a different check
/// entirely: a version-4 object with its PAGE_DIR removed is *also* rejected
/// downstream, because its BLOCKS bytes fail the block crc when read as
/// version-3 blocks. Only the message proves the version/section agreement
/// check is what refused it.
fn corrupted_saying(r: Result<impl Sized, LogSegError>, needle: &str) {
    match r {
        Err(LogSegError::Corrupted(m)) if m.contains(needle) => {}
        Err(LogSegError::Corrupted(m)) => panic!("Corrupted but not about {needle}: {m}"),
        Err(other) => panic!("expected Corrupted about {needle}, got {other:?}"),
        Ok(_) => panic!("expected Corrupted about {needle}, got Ok"),
    }
}

/// Opens and drains an object, returning whatever error either step produced.
fn open_and_drain(object: &[u8]) -> Result<Vec<LogRecord>, LogSegError> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg)?;
    Ok(reader.scan(&Predicate::And(Vec::new()))?.0)
}

// --- differential: version 4 decodes to what version 3 decodes --------------

fn arb_attr_value() -> impl Strategy<Value = AttrValue> {
    let leaf = prop_oneof![
        any::<i64>().prop_map(AttrValue::I64),
        any::<f64>().prop_map(AttrValue::F64),
        Just(AttrValue::F64(-0.0)),
        Just(AttrValue::F64(f64::from_bits(f64::NAN.to_bits() | 0x7))),
        any::<bool>().prop_map(AttrValue::Bool),
        "[a-z]{0,4}".prop_map(AttrValue::Str),
        proptest::collection::vec(any::<u8>(), 0..4).prop_map(AttrValue::Bytes),
    ];
    leaf.prop_recursive(2, 6, 3, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..3).prop_map(AttrValue::List),
            proptest::collection::vec(("[a-z]{1,3}", inner), 0..3).prop_map(AttrValue::Map),
        ]
    })
}

/// The same small name pool the columnar-versus-row differential in
/// `src/writer.rs` uses, so duplicate keys within a record and one key at two
/// types across records both arise.
fn arb_name() -> impl Strategy<Value = String> {
    prop::sample::select(vec!["a", "b", "c", "dup", "http.status", "svc.k"]).prop_map(String::from)
}

fn arb_record() -> impl Strategy<Value = LogRecord> {
    (
        0u8..3,
        0i64..40,
        0u8..30,
        prop::sample::select(vec!["", "INFO", "ERROR"]),
        prop::sample::select(vec!["", "hello", "a b c"]),
        prop_oneof![Just(None), any::<[u8; 16]>().prop_map(Some)],
        prop_oneof![Just(None), any::<[u8; 8]>().prop_map(Some)],
        any::<u32>(),
        proptest::collection::vec((arb_name(), arb_attr_value()), 0..6),
    )
        .prop_map(
            |(s, ts, sev, sevt, body, trace, span, flags, attrs)| LogRecord {
                stream_id: sid(s),
                stream_attrs: stream_blob(s),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: sev,
                severity_text: sevt.into(),
                body: body.into(),
                trace_id: trace,
                span_id: span,
                flags,
                attrs,
            },
        )
}

/// A record key that distinguishes f64 by bits, so -0.0 and NaN payloads are
/// not conflated. Records are compared in scan order, not as a set: the two
/// layouts must agree on order too.
fn key(r: &LogRecord) -> String {
    format!("{r:?}")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// ADR-0699's acceptance anchor: for the generators the columnar-versus-row
    /// differential already covers, a version-4 object read through the
    /// version-4 reader yields the same records, in the same order, as the same
    /// batch written through the version-3 path and read through the version-3
    /// reader. Both objects come from one writer whose only difference is the
    /// BLOCKS layout, so a divergence is a layout defect and nothing else.
    #[test]
    fn version_4_decodes_to_what_version_3_decodes(
        records in proptest::collection::vec(arb_record(), 1..25),
        max_dyn in 1usize..8,
        group in 1usize..4,
    ) {
        let cfg = RlogConfig {
            max_dynamic_columns: max_dyn,
            block_target_records: 5,
            block_max_bytes: 8192,
            group_target_blocks: group,
            ..RlogConfig::default()
        };
        let v3 = write_v3(cfg, &records);
        let v4 = write_v4(cfg, &records);
        prop_assert_ne!(&v3, &v4, "the two layouts must not be the same bytes");

        let from_v3: Vec<String> = scan_all(&v3).iter().map(key).collect();
        let from_v4: Vec<String> = scan_all(&v4).iter().map(key).collect();
        prop_assert_eq!(from_v4, from_v3);
    }
}

/// The row-group boundary cases ADR-0699's consequences section names: one
/// block, `group_target_blocks - 1`, exactly one group, and one over.
#[test]
fn row_group_boundary_cases_decode_identically_to_version_3() {
    const GROUP: usize = 4;
    const PER_BLOCK: usize = 5;

    for blocks in [1usize, GROUP - 1, GROUP, GROUP + 1] {
        let cfg = RlogConfig {
            block_target_records: PER_BLOCK,
            group_target_blocks: GROUP,
            ..RlogConfig::default()
        };
        let records = linear_corpus(blocks * PER_BLOCK);
        let v3 = write_v3(cfg, &records);
        let v4 = write_v4(cfg, &records);

        let from_v3: Vec<String> = scan_all(&v3).iter().map(key).collect();
        let from_v4: Vec<String> = scan_all(&v4).iter().map(key).collect();
        assert_eq!(from_v4, from_v3, "{blocks} blocks at a {GROUP}-block group");

        let dir = page_dir_of(&v4);
        assert_eq!(dir.block_count(), blocks as u64, "{blocks} blocks");
        assert_eq!(
            dir.groups.len(),
            blocks.div_ceil(GROUP),
            "{blocks} blocks fill {} group(s)",
            blocks.div_ceil(GROUP)
        );
        // The groups partition the blocks into consecutive runs from 0, and
        // only the last may be short.
        let mut next = 0u32;
        for (i, g) in dir.groups.iter().enumerate() {
            assert_eq!(g.first_block, next, "group {i} continues the previous");
            assert!(g.block_count as usize <= GROUP);
            if i + 1 < dir.groups.len() {
                assert_eq!(g.block_count as usize, GROUP, "only the last is short");
            }
            next += g.block_count;
        }
    }
}

// --- projection through PAGE_DIR --------------------------------------------

/// A 105-column corpus: the nine always-present fixed columns plus 96 dynamic
/// attribute columns, every one present on every record, so each block carries
/// exactly one page per column and no presence pages.
fn wide_corpus(records: usize) -> Vec<LogRecord> {
    (0..records)
        .map(|i| {
            let attrs = (0..96)
                .map(|c| (format!("c{c:03}"), AttrValue::I64(i as i64 * 7 + c)))
                .collect();
            LogRecord {
                stream_id: sid(0),
                stream_attrs: stream_blob(0),
                ts_ns: i as i64,
                observed_ts_ns: i as i64 + 1,
                severity_num: (i % 24) as u8,
                severity_text: "INFO".to_string(),
                body: format!("wide record {i}"),
                trace_id: Some([(i % 251) as u8; 16]),
                span_id: Some([(i % 251) as u8; 8]),
                flags: 1,
                attrs,
            }
        })
        .collect()
}

/// With PAGE_DIR the reader skips an unwanted column's page without reading it
/// at all, and `pages_skipped` reflects exactly that: 105 columns, 2 decoded,
/// 103 skipped, per block.
///
/// The count is pinned exactly rather than as `> 0`. A layout that emitted
/// pages block-major would still let the reader skip *some* pages, so a loose
/// assertion would pass on the thing this test exists to forbid.
#[test]
fn two_column_projection_skips_the_other_103_columns_pages() {
    const PER_BLOCK: usize = 8;
    const BLOCKS: usize = 9;

    let cfg = RlogConfig {
        block_target_records: PER_BLOCK,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    let records = wide_corpus(BLOCKS * PER_BLOCK);
    let object = write_v4(cfg, &records);

    // The fixture really is 105 pages per block, with no presence pages.
    let dir = page_dir_of(&object);
    assert_eq!(dir.block_count(), BLOCKS as u64);
    for block in 0..BLOCKS as u32 {
        let pages = dir.block_pages(block).expect("block in page_dir");
        assert_eq!(
            pages.len(),
            105,
            "block {block} carries one page per column"
        );
    }

    let reader = RlogReader::new(&object, &cfg).expect("open");
    let pred = Predicate::And(Vec::new());

    // `fixed_only` resolves to exactly `ts` and `stream_ref`: the two columns
    // every scan needs, and nothing else.
    let mut cursor = reader
        .scan_blocks(&pred, &[], &ColumnSelection::fixed_only())
        .expect("cursor");
    let mut projected = Vec::new();
    while let Some(rows) = cursor.next_block(&object).expect("next") {
        projected.extend(rows);
    }
    let stats = cursor.stats();
    assert_eq!(stats.blocks_scanned as usize, BLOCKS);
    assert_eq!(
        stats.pages_decoded,
        (BLOCKS * 2) as u64,
        "exactly the two selected columns' pages are decoded"
    );
    assert_eq!(
        stats.pages_skipped,
        (BLOCKS * 103) as u64,
        "every other column's page is skipped"
    );

    // The decoded columns equal the whole-read's for the columns that were
    // selected: same records, same order, same ts and stream identity.
    let full = scan_all(&object);
    assert_eq!(projected.len(), full.len());
    for (p, f) in projected.iter().zip(&full) {
        assert_eq!(p.ts_ns, f.ts_ns);
        assert_eq!(p.stream_id, f.stream_id);
        assert_eq!(p.stream_attrs, f.stream_attrs);
    }
}

// --- the chunk-range seam ---------------------------------------------------

/// The seam ADR-0699 decision 5's fetcher calls: per `(row group, column)`, the
/// column chunk's `(offset, len)` covers exactly the pages PAGE_DIR lists for
/// it, contiguous and in order, and the chunks of one group tile the group's
/// bytes without gap or overlap.
#[test]
fn chunk_ranges_cover_exactly_their_pages_contiguously() {
    let cfg = RlogConfig {
        block_target_records: 8,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    let object = write_v4(cfg, &wide_corpus(9 * 8));
    let dir = page_dir_of(&object);
    let reader = RlogReader::new(&object, &cfg).expect("open");
    let blocks_offset = open(&object)
        .expect("footer")
        .section(kind::BLOCKS)
        .expect("BLOCKS")
        .offset;

    assert_eq!(reader.row_group_count(), dir.groups.len());
    let mut cursor = blocks_offset;
    for (gi, g) in dir.groups.iter().enumerate() {
        for c in &g.chunks {
            let (offset, len) = dir.chunk_range(gi, c.column_id).expect("chunk range");
            assert_eq!(offset, c.offset, "the range starts at the chunk's offset");
            assert_eq!(
                len,
                c.pages.iter().map(|p| p.len).sum::<u64>(),
                "the range is exactly the chunk's pages"
            );

            // The pages tile the range in listed order.
            let mut at = offset;
            for p in &c.pages {
                at += p.len;
            }
            assert_eq!(at, offset + len);

            // The reader's absolute form of the same seam.
            assert_eq!(
                reader.column_chunk_range(gi, c.column_id),
                Some((blocks_offset + offset, len)),
                "the reader returns the object-absolute range"
            );

            // Chunks follow each other with no gap: BLOCKS is exactly the
            // groups' chunks laid end to end, in column_id order per group.
            assert_eq!(
                blocks_offset + offset,
                cursor,
                "group {gi} column {} starts where the previous chunk ended",
                c.column_id
            );
            cursor = blocks_offset + offset + len;
        }
    }
    let blocks_len = open(&object)
        .expect("footer")
        .section(kind::BLOCKS)
        .expect("BLOCKS")
        .len;
    assert_eq!(
        cursor,
        blocks_offset + blocks_len,
        "the chunks tile the whole BLOCKS section"
    );

    // Absent group and absent column both read as None rather than panicking.
    assert_eq!(reader.column_chunk_range(dir.groups.len(), 0), None);
    assert_eq!(reader.column_chunk_range(0, 100_000), None);
}

// --- corruption -------------------------------------------------------------

/// A flipped byte inside a page fails that page's own crc32c, before it is
/// decompressed. BLOCKS has no whole-section checksum, so nothing else can
/// catch it.
#[test]
fn flipped_byte_inside_a_page_is_a_typed_error() {
    let cfg = RlogConfig {
        block_target_records: 8,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    let object = write_v4(cfg, &linear_corpus(9 * 8));
    let blocks_offset = open(&object)
        .expect("footer")
        .section(kind::BLOCKS)
        .expect("BLOCKS")
        .offset;
    let dir = page_dir_of(&object);

    // A page in the last row group, so the flip is not confined to group 0.
    let last = dir.groups.last().expect("a group");
    let chunk = last.chunks.last().expect("a chunk");
    let page = chunk.pages.last().expect("a page");
    let page_start: u64 = chunk.offset
        + chunk.pages[..chunk.pages.len() - 1]
            .iter()
            .map(|p| p.len)
            .sum::<u64>();
    assert!(page.len > 0);
    let at = (blocks_offset + page_start) as usize;

    let damaged = flip(&object, at);
    assert!(
        is_corrupted(open_and_drain(&damaged)),
        "a flipped page byte must be a typed Corrupted error"
    );
    // The undamaged object drains, so the assertion above is about the flip.
    assert!(open_and_drain(&object).is_ok());
}

/// A flipped byte anywhere in PAGE_DIR's stored bytes fails the section crc,
/// before the directory is decoded and before any offset in it is used.
#[test]
fn flipped_byte_in_page_dir_fails_the_section_crc() {
    let cfg = RlogConfig {
        block_target_records: 8,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    let object = write_v4(cfg, &linear_corpus(9 * 8));
    let desc = *open(&object)
        .expect("footer")
        .section(kind::PAGE_DIR)
        .expect("PAGE_DIR");

    for at in [
        desc.offset,
        desc.offset + desc.len / 2,
        desc.offset + desc.len - 1,
    ] {
        let damaged = flip(&object, at as usize);
        assert!(
            is_corrupted(open_and_drain(&damaged)),
            "a flipped PAGE_DIR byte at {at} must be a typed Corrupted error"
        );
    }
}

/// A truncated last row group leaves PAGE_DIR describing pages past the end of
/// BLOCKS, which `validate_extents` rejects at open: nothing is located through
/// an offset that was never checked against the section it indexes.
#[test]
fn truncated_last_row_group_is_a_typed_error() {
    let cfg = RlogConfig {
        block_target_records: 8,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    let object = write_v4(cfg, &linear_corpus(9 * 8));
    // The reassembled but untruncated object still reads, so the failures below
    // are about the truncation and not about the reassembly.
    let intact = truncate_blocks(&object, 0);
    assert!(open_and_drain(&intact).is_ok(), "reassembly is faithful");

    for cut in [1usize, 64, 512] {
        let damaged = truncate_blocks(&object, cut);
        assert!(
            is_corrupted(open_and_drain(&damaged)),
            "BLOCKS truncated by {cut} bytes must be a typed Corrupted error"
        );
    }
}

/// A PAGE_DIR whose chunk offsets point past the end of BLOCKS is refused at
/// open, not followed into another section's bytes.
#[test]
fn page_dir_offsets_past_blocks_are_refused() {
    let cfg = RlogConfig {
        block_target_records: 8,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    let object = write_v4(cfg, &linear_corpus(9 * 8));
    let mut dir = page_dir_of(&object);
    let blocks_len = open(&object)
        .expect("footer")
        .section(kind::BLOCKS)
        .expect("BLOCKS")
        .len;

    // The last chunk of the last group, pushed one byte past the section's end.
    let group = dir.groups.last_mut().expect("a group");
    let chunk = group.chunks.last_mut().expect("a chunk");
    let len: u64 = chunk.pages.iter().map(|p| p.len).sum();
    chunk.offset = blocks_len - len + 1;

    let damaged = with_page_dir_bytes(&object, dir.encode());
    assert!(
        is_corrupted(open_and_drain(&damaged)),
        "a chunk ending past BLOCKS must be a typed Corrupted error"
    );
}

/// A PAGE_DIR claiming more pages for one column of one block than the
/// presence-plus-value pair allows is refused by decode, before any allocation
/// sized by the count.
#[test]
fn page_dir_page_count_over_the_cap_is_refused() {
    let cfg = RlogConfig {
        block_target_records: 8,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    let object = write_v4(cfg, &linear_corpus(9 * 8));

    // A hand-built directory: one group of one block whose single column claims
    // a page count far above the cap.
    let mut raw = Vec::new();
    let mut put = |v: u64| {
        let mut x = v;
        loop {
            let b = (x & 0x7f) as u8;
            x >>= 7;
            if x == 0 {
                raw.push(b);
                break;
            }
            raw.push(b | 0x80);
        }
    };
    put(1); // group_count
    put(0); // first_block
    put(1); // block_count
    put(1); // chunk_count
    put(0); // column_id
    put(0); // offset
    put(u32::MAX as u64); // page_count, far over the 2-per-block cap

    let damaged = with_page_dir_bytes(&object, raw);
    assert!(
        is_corrupted(open_and_drain(&damaged)),
        "a page count over the cap must be a typed Corrupted error"
    );
}

// --- version gate -----------------------------------------------------------

/// A version-3 object still reads through the N-1 reader, and a version-5
/// trailer is refused with the existing typed unsupported-version error
/// (ADR-0699 decision 3, ADR-0066 decision 2).
#[test]
fn version_gate_accepts_3_and_4_and_refuses_5() {
    let cfg = RlogConfig {
        block_target_records: 5,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    let records = linear_corpus(20);
    let v3 = write_v3(cfg, &records);
    let v4 = write_v4(cfg, &records);

    let n3 = v3.len();
    assert_eq!(u16::from_le_bytes([v3[n3 - 8], v3[n3 - 7]]), 3);
    let n4 = v4.len();
    assert_eq!(
        u16::from_le_bytes([v4[n4 - 8], v4[n4 - 7]]),
        footer::VERSION
    );
    assert_eq!(footer::VERSION, 4);

    assert_eq!(
        scan_all(&v3).len(),
        records.len(),
        "a version-3 object reads"
    );
    assert_eq!(
        scan_all(&v4).len(),
        records.len(),
        "a version-4 object reads"
    );
    assert!(
        open(&v3)
            .expect("open v3")
            .section(kind::PAGE_DIR)
            .is_none(),
        "version 3 carries no PAGE_DIR"
    );

    // The version byte and PAGE_DIR's presence must agree: a version-4 object
    // whose PAGE_DIR was dropped, and a version-3 object that gained one, are
    // both refused rather than read under a guessed layout.
    let (footer, sections) = explode(&v4);
    let without: Vec<Section> = sections
        .into_iter()
        .filter(|s| s.kind != kind::PAGE_DIR)
        .collect();
    corrupted_saying(
        open_and_drain(&assemble(footer, &without)),
        "disagrees with the PAGE_DIR section",
    );

    let (v3_footer, mut v3_sections) = explode(&v3);
    let dir_stored = {
        let (_, v4_sections) = explode(&v4);
        v4_sections
            .into_iter()
            .find(|s| s.kind == kind::PAGE_DIR)
            .expect("PAGE_DIR in v4")
    };
    v3_sections.push(dir_stored);
    let v3_plus = assemble_versioned(v3_footer, &v3_sections, 3);
    corrupted_saying(
        open_and_drain(&v3_plus),
        "disagrees with the PAGE_DIR section",
    );
    // The same rewrite without the added section still reads at version 3, so
    // the refusal above is about PAGE_DIR and not about the rewrite.
    let (v3_footer, v3_sections) = explode(&v3);
    assert!(open_and_drain(&assemble_versioned(v3_footer, &v3_sections, 3)).is_ok());

    let mut future = v4.clone();
    future[n4 - 8..n4 - 6].copy_from_slice(&5u16.to_le_bytes());
    match RlogReader::new(&future, &cfg) {
        Err(LogSegError::UnsupportedVersion(5)) => {}
        other => panic!(
            "expected UnsupportedVersion(5), got {:?}",
            other.map(|_| ())
        ),
    }
}
