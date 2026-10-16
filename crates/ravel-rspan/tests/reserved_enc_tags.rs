//! Encoding tags 10-13 are reserved for the codecs of ADR-2135 (decisions 3
//! and 6). RSPAN never writes them, and a block whose page descriptor carries
//! one is refused as `Corrupted` rather than decoded under a guessed codec.
#![allow(clippy::expect_used)]

use ravel_rspan::block::{read_block, write_block};
use ravel_rspan::record::COL_START_TS;
use ravel_rspan::varint::get_uvarint;
use ravel_rspan::{ResolvedSpanRow, SpanSegError, StatusCode};

fn row() -> ResolvedSpanRow {
    ResolvedSpanRow {
        trace_id: [0x11; 16],
        span_id: [0x22; 8],
        parent_span_id: None,
        name: "GET /cart".to_string(),
        start_ts_ns: 1_700_000_000_000_000_000,
        end_ts_ns: 1_700_000_000_000_000_500,
        status_code: StatusCode::Ok,
        status_message: None,
        service_name: Some("checkout".to_string()),
        columns: Vec::new(),
        attrs_raw: None,
        events: Vec::new(),
    }
}

/// The byte offset of the `enc` tag in the header descriptor of `column_id`'s
/// page.
fn enc_offset(block: &[u8], column_id: u32) -> usize {
    let mut pos = 0usize;
    get_uvarint(block, &mut pos).expect("record_count");
    let pages = get_uvarint(block, &mut pos).expect("page_count");
    for _ in 0..pages {
        let cid = get_uvarint(block, &mut pos).expect("column_id");
        let at = pos;
        pos += 2;
        get_uvarint(block, &mut pos).expect("len");
        get_uvarint(block, &mut pos).expect("uncomp_len");
        if cid == u64::from(column_id) {
            return at;
        }
    }
    panic!("no page for column {column_id}");
}

#[test]
fn reserved_enc_tags_are_refused() {
    let out = write_block(&[row()], &[], 3).expect("write block");
    read_block(&out.bytes, out.crc32c, u64::MAX).expect("the unmodified block reads");
    let at = enc_offset(&out.bytes, COL_START_TS);

    for tag in 10u8..=13 {
        let mut bytes = out.bytes.clone();
        bytes[at] = tag;
        let crc = crc32c::crc32c(&bytes);
        match read_block(&bytes, crc, u64::MAX) {
            Err(SpanSegError::Corrupted(m)) => {
                assert!(m.contains("not an integer codec"), "tag {tag}: {m}")
            }
            Err(other) => panic!("tag {tag}: expected Corrupted, got {other:?}"),
            Ok(_) => panic!("tag {tag}: a reserved tag decoded"),
        }
    }
}
