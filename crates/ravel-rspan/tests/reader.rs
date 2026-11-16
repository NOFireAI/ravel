//! Reader-level acceptance for trailer v5 (ADR-2707 decision 2): the KEY_IDX
//! probe names exactly the blocks holding a trace, through both the
//! whole-object reader and the ranged reader, and a v4 object is refused.
#![allow(clippy::expect_used)]

use std::cell::RefCell;
use std::ops::Range;

use ravel_rspan::footer::{DEFAULT_MAX_SECTION_UNCOMP, kind};
use ravel_rspan::{
    ObjectIdentity, RspanConfig, RspanRangeReader, RspanReader, RspanWriter, SpanRecord,
    SpanSegError, StatusCode, TraceKeyProbe, open, read_section,
};

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0x51; 16],
        shard: 2,
        writer_id: [0x15; 16],
        writer_epoch: 1,
        writer_seq: 7,
    }
}

fn span(trace_id: [u8; 16], i: usize) -> SpanRecord {
    SpanRecord {
        trace_id,
        span_id: [i as u8; 8],
        parent_span_id: None,
        name: format!("op-{i}"),
        start_ts_ns: 1_000 + i as i64,
        end_ts_ns: 2_000 + i as i64,
        status_code: StatusCode::Ok,
        status_message: None,
        attrs: vec![("service.name".to_string(), "svc".to_string())],
    }
}

/// Traces whose first 8 bytes all differ, so each has its own `key8`.
const A: [u8; 16] = [0x10; 16];
const B: [u8; 16] = [0x20; 16];
const C: [u8; 16] = [0x30; 16];
const D: [u8; 16] = [0x40; 16];
const E: [u8; 16] = [0x50; 16];

fn cfg() -> RspanConfig {
    RspanConfig {
        block_target_records: 2,
        ..RspanConfig::default()
    }
}

/// Two records per block in `(trace_id, start_ts)` order: A A | B B | B B |
/// B B | C C | D E, so B spans the run 1..=3 and D and E share block 5.
fn write_object() -> Vec<u8> {
    let mut w = RspanWriter::new(cfg(), identity());
    let counts = [(A, 2), (B, 6), (C, 2), (D, 1), (E, 1)];
    let mut i = 0;
    // Pushed in reverse so the expected blocks rest on the writer's sort.
    let mut records = Vec::new();
    for (trace, n) in counts {
        for _ in 0..n {
            records.push(span(trace, i));
            i += 1;
        }
    }
    for rec in records.into_iter().rev() {
        w.push(rec);
    }
    w.finish().expect("finish")
}

fn range_reader(obj: &[u8]) -> RspanRangeReader {
    let footer = open(obj).expect("open");
    let skip_desc = footer.section(kind::SKIP_IDX).expect("skip section");
    let skip_raw = read_section(obj, skip_desc, DEFAULT_MAX_SECTION_UNCOMP).expect("skip");
    RspanRangeReader::from_sections(&footer, &skip_raw).expect("range reader")
}

/// Probes through the ranged reader, returning the answer and every absolute
/// range it asked for.
fn ranged_probe(
    obj: &[u8],
    reader: &RspanRangeReader,
    trace: &[u8; 16],
) -> (TraceKeyProbe, Vec<Range<u64>>) {
    let reads = RefCell::new(Vec::new());
    let got = reader
        .probe_trace(trace, |r: Range<u64>| {
            reads.borrow_mut().push(r.clone());
            Ok::<_, SpanSegError>(&obj[r.start as usize..r.end as usize])
        })
        .expect("ranged probe");
    (got, reads.into_inner())
}

#[test]
fn v5_object_probe_names_the_trace_blocks_and_v4_is_refused() {
    let obj = write_object();
    let reader = RspanReader::new(&obj, &cfg()).expect("reader");
    let ranged = range_reader(&obj);

    let footer = open(&obj).expect("open");
    let key_idx = *footer.section(kind::KEY_IDX).expect("KEY_IDX section");
    let prefix = key_idx.offset..key_idx.offset + u64::from(key_idx.prefix_len);
    let section = key_idx.offset..key_idx.offset + key_idx.len;

    let cases: [(&[u8; 16], Vec<u32>); 6] = [
        (&A, vec![0]),
        (&B, vec![1, 2, 3]),
        (&C, vec![4]),
        (&D, vec![5]),
        (&E, vec![5]),
        (&[0x99; 16], vec![]),
    ];
    for (trace, want) in cases {
        let answer = if want.is_empty() {
            TraceKeyProbe::Absent
        } else {
            TraceKeyProbe::Present(want.clone())
        };
        assert_eq!(
            reader.probe_trace(trace).expect("probe"),
            answer,
            "whole-object probe for {:02x}",
            trace[0]
        );
        let (got, reads) = ranged_probe(&obj, &ranged, trace);
        assert_eq!(got, answer, "ranged probe for {:02x}", trace[0]);
        // Exactly the prefix first, then at most one bucket inside the section
        // past the prefix; a present trace always needs its bucket.
        assert_eq!(reads[0], prefix, "first read is exactly [0, prefix_len)");
        assert!(reads.len() <= 2, "at most one bucket read: {reads:?}");
        if !want.is_empty() {
            assert_eq!(reads.len(), 2, "present trace reads its bucket");
        }
        if let Some(bucket) = reads.get(1) {
            assert!(bucket.start >= prefix.end && bucket.end <= section.end);
        }
    }

    // An absent trace whose key mixes to B's (non-empty) bucket still gets no
    // blocks: the bucket's entries are filtered by key8.
    let (_, b_reads) = ranged_probe(&obj, &ranged, &B);
    let shared = (0u16..=u16::MAX)
        .map(|n| {
            let mut t = [0xEE; 16];
            t[..2].copy_from_slice(&n.to_le_bytes());
            t
        })
        .find(|t| ranged_probe(&obj, &ranged, t).1.get(1) == b_reads.get(1))
        .expect("some absent trace shares B's bucket");
    assert_eq!(
        reader.probe_trace(&shared).expect("probe"),
        TraceKeyProbe::Absent
    );
    assert_eq!(
        ranged_probe(&obj, &ranged, &shared).0,
        TraceKeyProbe::Absent
    );

    let v4: &[u8] = include_bytes!("fixtures/golden_rspan_v4.bin");
    assert!(matches!(open(v4), Err(SpanSegError::UnsupportedVersion(4))));
    assert!(matches!(
        RspanReader::new(v4, &cfg()),
        Err(SpanSegError::UnsupportedVersion(4))
    ));
}
