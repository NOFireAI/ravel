//! Hostile-input suite. One valid object is mutated every way a byte stream
//! can be damaged (truncate, single-byte flip, splice), and the reader must
//! either succeed with exactly the original data (the mutation hit unread or
//! unverified bytes) or return a typed error. It must never panic and never
//! return records that differ from the original without an error.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, ObjectIdentity, Predicate, RlogConfig, RlogReader,
    RlogWriter, stream_attrs_bytes,
};

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

/// The canonical resource+scope blob for synthetic stream `n`. Every record of
/// a stream must carry the same bytes, so it is derived from `n` alone.
fn stream_blob(n: u8) -> Vec<u8> {
    stream_attrs_bytes(
        &[("service.name".to_string(), AttrValue::Str(format!("s{n}")))],
        "scope",
        "1",
        &[],
    )
}

/// A varied fixture: several streams, attrs of every scalar type plus bytes,
/// NaN and -0.0 payloads, some trace/span ids, and multi-token bodies.
fn fixture() -> (Vec<LogRecord>, Vec<u8>) {
    let bodies = [
        "connection timeout",
        "all ok",
        "retry later",
        "cache miss",
        "",
    ];
    let mut corpus = Vec::new();
    for i in 0..200i64 {
        let stream = (i % 4) as u8;
        let mut attrs = vec![
            ("svc".to_string(), AttrValue::Str(format!("s{stream}"))),
            ("code".to_string(), AttrValue::I64(i % 7)),
        ];
        match i % 5 {
            0 => attrs.push(("dur".into(), AttrValue::F64(-0.0))),
            1 => attrs.push((
                "dur".into(),
                AttrValue::F64(f64::from_bits(f64::NAN.to_bits() | 0x3)),
            )),
            2 => attrs.push(("flag".into(), AttrValue::Bool(i % 2 == 0))),
            3 => attrs.push(("raw".into(), AttrValue::Bytes(vec![i as u8, 0, 255]))),
            _ => attrs.push(("empty".into(), AttrValue::Str(String::new()))),
        }
        corpus.push(LogRecord {
            stream_id: sid(stream),
            stream_attrs: stream_blob(stream),
            ts_ns: i,
            observed_ts_ns: i + 1,
            severity_num: (i % 24) as u8,
            severity_text: if i % 2 == 0 { "INFO" } else { "ERROR" }.to_string(),
            body: bodies[(i as usize) % bodies.len()].to_string(),
            trace_id: if i % 3 == 0 {
                Some([i as u8; 16])
            } else {
                None
            },
            span_id: if i % 4 == 0 { Some([i as u8; 8]) } else { None },
            flags: (i as u32) & 0xf,
            attrs,
        });
    }
    let cfg = RlogConfig {
        block_target_records: 16,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 1,
        writer_id: [9u8; 16],
        writer_epoch: 2,
        writer_seq: 3,
    };
    let mut w = RlogWriter::new(cfg, identity);
    for r in &corpus {
        w.push(r.clone()).expect("push");
    }
    let object = w.finish().expect("finish");
    (corpus, object)
}

// --- order-insensitive record keys (f64 by bits) --------------------------

fn put_uv(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            break;
        }
        out.push(b | 0x80);
    }
}

fn encode_value(out: &mut Vec<u8>, v: &AttrValue) {
    match v {
        AttrValue::Str(s) => {
            out.push(1);
            put_uv(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        AttrValue::I64(x) => {
            out.push(2);
            out.extend_from_slice(&x.to_le_bytes());
        }
        AttrValue::F64(f) => {
            out.push(3);
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        AttrValue::Bool(b) => {
            out.push(4);
            out.push(u8::from(*b));
        }
        AttrValue::Bytes(b) => {
            out.push(5);
            put_uv(out, b.len() as u64);
            out.extend_from_slice(b);
        }
        AttrValue::List(_) | AttrValue::Map(_) => unreachable!("fixture uses scalar/bytes only"),
    }
}

fn record_key(r: &LogRecord) -> Vec<u8> {
    let mut k = Vec::new();
    k.extend_from_slice(&r.stream_id.0);
    k.extend_from_slice(&r.ts_ns.to_le_bytes());
    k.extend_from_slice(&r.observed_ts_ns.to_le_bytes());
    k.push(r.severity_num);
    put_uv(&mut k, r.severity_text.len() as u64);
    k.extend_from_slice(r.severity_text.as_bytes());
    put_uv(&mut k, r.body.len() as u64);
    k.extend_from_slice(r.body.as_bytes());
    match r.trace_id {
        Some(t) => {
            k.push(1);
            k.extend_from_slice(&t);
        }
        None => k.push(0),
    }
    match r.span_id {
        Some(s) => {
            k.push(1);
            k.extend_from_slice(&s);
        }
        None => k.push(0),
    }
    k.extend_from_slice(&r.flags.to_le_bytes());
    let mut attrs = r.attrs.clone();
    attrs.sort_by(|a, b| a.0.cmp(&b.0));
    put_uv(&mut k, attrs.len() as u64);
    for (name, v) in &attrs {
        put_uv(&mut k, name.len() as u64);
        k.extend_from_slice(name.as_bytes());
        encode_value(&mut k, v);
    }
    k
}

fn normalize(v: &[LogRecord]) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = v.iter().map(record_key).collect();
    keys.sort();
    keys
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 600, ..ProptestConfig::default() })]

    #[test]
    fn mutation_never_panics_never_wrong(
        op in 0u8..3,
        idx in any::<usize>(),
        xor in any::<u8>(),
        splice in proptest::collection::vec(any::<u8>(), 1..8),
    ) {
        let (corpus, object) = fixture();
        let mutated: Vec<u8> = match op {
            // Truncate at an arbitrary offset.
            0 => {
                let cut = idx % (object.len() + 1);
                object[..cut].to_vec()
            }
            // Flip one byte (xor forced nonzero so it is a real change).
            1 => {
                let mut m = object.clone();
                if !m.is_empty() {
                    let i = idx % m.len();
                    m[i] ^= xor | 1;
                }
                m
            }
            // Splice random bytes in at an arbitrary offset.
            _ => {
                let at = idx % (object.len() + 1);
                let mut m = Vec::with_capacity(object.len() + splice.len());
                m.extend_from_slice(&object[..at]);
                m.extend_from_slice(&splice);
                m.extend_from_slice(&object[at..]);
                m
            }
        };

        let cfg = RlogConfig::default();
        let match_all = Predicate::And(Vec::new());
        if let Ok(reader) = RlogReader::new(&mutated, &cfg)
            && let Ok((got, _stats)) = reader.scan(&match_all)
        {
            // A successful scan of a mutated object must still yield exactly
            // the original data (the damage hit unread/unverified bytes).
            prop_assert_eq!(normalize(&got), normalize(&corpus));
        }
    }

    /// The unmutated object always scans back to the full corpus.
    #[test]
    fn identity_roundtrip(_n in 0u8..1) {
        let (corpus, object) = fixture();
        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&object, &cfg).expect("open");
        let (got, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        prop_assert_eq!(normalize(&got), normalize(&corpus));
    }
}
