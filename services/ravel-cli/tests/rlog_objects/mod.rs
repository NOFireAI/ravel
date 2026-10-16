//! The `.rlog` objects the `rlog footprint` tests measure.

use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

pub fn rec(stream: u8, ts: i64, body: String, attrs: Vec<(String, AttrValue)>) -> LogRecord {
    LogRecord {
        stream_id: sid(stream),
        stream_attrs: ravel_logseg::stream_attrs_bytes(
            &[("service.name".into(), AttrValue::Str(format!("s{stream}")))],
            "scope",
            "1",
            &[],
        ),
        ts_ns: ts,
        observed_ts_ns: ts + 5,
        severity_num: 9,
        severity_text: "INFO".into(),
        body,
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// Object A: 12 records, 4 per block (3 blocks), 2 blocks per row group (2
/// row groups). Every record carries `svc` (two values alternating, so each
/// block's page is dictionary-encoded) and `code` (i64). Bodies are long and
/// repetitive, so each body page crosses the compression floor and is stored
/// zstd-compressed, smaller than its uncompressed size.
pub fn object_a() -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: 4,
        group_target_blocks: 2,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity(1));
    for i in 0..12i64 {
        let svc = if i % 2 == 0 { "api" } else { "auth" };
        let body = format!("request {i} served ").repeat(20);
        w.push(rec(
            1,
            1_000 + i,
            body,
            vec![
                ("svc".into(), AttrValue::Str(svc.into())),
                ("code".into(), AttrValue::I64(200 + i * 37)),
            ],
        ))
        .expect("push");
    }
    w.finish().expect("finish")
}

/// Object B: 6 records, 3 per block (2 blocks, one row group), and a column set
/// disjoint from A's dynamic columns. `region` is on two of each block's three
/// rows, so each block carries a presence bitmap page and a value page for it.
pub fn object_b() -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity(2));
    for i in 0..6i64 {
        let mut attrs = vec![("latency_ms".into(), AttrValue::I64(10 + i))];
        if i % 3 != 2 {
            attrs.push(("region".into(), AttrValue::Str("eu-west".into())));
        }
        w.push(rec(2, 5_000 + i, format!("b{i}"), attrs))
            .expect("push");
    }
    w.finish().expect("finish")
}

pub fn identity(seq: u64) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [0x11u8; 16],
        shard: 0,
        writer_id: [0x22u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    }
}
