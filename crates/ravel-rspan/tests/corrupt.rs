//! Hostile-input suite. One valid RSPAN object is mutated every way a byte
//! stream can be damaged (truncate, single-byte flip, splice), and the reader
//! must either succeed with exactly the original data (the mutation hit unread
//! or unverified bytes) or return a typed error. It must never panic and never
//! return records that differ from the original without an error.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_rspan::{
    ObjectIdentity, RspanConfig, RspanReader, RspanWriter, SpanQuery, SpanRecord, StatusCode,
};

fn tid(n: u8) -> [u8; 16] {
    let mut a = [0u8; 16];
    a[0] = n;
    a
}

/// One deterministic, validly-framed events blob (a single OTLP-shaped event),
/// hex-encoded exactly as the ingest path encodes `_events_raw`. Modelled on
/// the golden-bytes fixture so at least one block carries decoded event
/// columns, exercising the nested-column read path under mutation.
fn events_blob() -> String {
    let mut event = Vec::new();
    event.push(0x09u8); // field 1, wire type 1 (fixed64) time_unix_nano
    event.extend_from_slice(&1_650_000_000_000_000_100i64.to_le_bytes());
    event.push(0x12); // field 2, wire type 2 (string) name
    event.push(9);
    event.extend_from_slice(b"exception");
    event.push(0x1a); // field 3, wire type 2 (attributes stand-in)
    event.push(5);
    event.extend_from_slice(b"stack");
    let mut raw = Vec::new();
    raw.push(event.len() as u8); // canonical single-byte varint length
    raw.extend_from_slice(&event);
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

/// A varied fixture: several traces and services, every status code, spans with
/// and without a parent and a status message, dynamic attribute columns, empty
/// strings, non-ASCII values, and one span carrying an events blob. Records are
/// grouped into a handful of traces so a trace-restricted scan has a non-trivial
/// true subset. A small `block_target_records` forces many blocks so mutations
/// land across block framing, page bytes, and the skip index alike.
fn fixture() -> (Vec<SpanRecord>, Vec<u8>) {
    let services = ["checkout", "payments", "inventory", "checkout", "gateway"];
    let names = [
        "GET /cart",
        "charge card",
        "list items",
        "POST /order",
        "resolve route",
    ];
    let statuses = [StatusCode::Unset, StatusCode::Ok, StatusCode::Error];
    let mut corpus = Vec::new();
    for i in 0..80i64 {
        let trace = (i % 5) as u8;
        let service = services[(i as usize) % services.len()];
        let name = names[(i as usize) % names.len()];
        let mut attrs = vec![
            ("service.name".to_string(), service.to_string()),
            ("http.method".to_string(), "GET".to_string()),
            ("code".to_string(), format!("{}", i % 7)),
        ];
        match i % 4 {
            0 => attrs.push(("region".into(), "eu-west-\u{00e9}".into())),
            1 => attrs.push(("empty".into(), String::new())),
            2 => attrs.push(("http.target".into(), format!("/p/{i}"))),
            _ => attrs.push(("dropped".into(), "false".into())),
        }
        if i == 0 {
            attrs.push(("_events_raw".to_string(), events_blob()));
        }
        corpus.push(SpanRecord {
            trace_id: tid(trace),
            span_id: [i as u8; 8],
            parent_span_id: if i % 3 == 0 {
                None
            } else {
                Some([(i + 1) as u8; 8])
            },
            name: name.to_string(),
            start_ts_ns: 1_650_000_000_000_000_000 + i * 1000,
            end_ts_ns: 1_650_000_000_000_000_000 + i * 1000 + 500,
            status_code: statuses[(i as usize) % statuses.len()],
            status_message: if i % 2 == 0 {
                None
            } else {
                Some(format!("msg {i}"))
            },
            attrs,
        });
    }
    let cfg = RspanConfig {
        block_target_records: 4,
        ..RspanConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 1,
        writer_id: [9u8; 16],
        writer_epoch: 2,
        writer_seq: 3,
    };
    let mut w = RspanWriter::new(cfg, identity);
    for r in &corpus {
        w.push(r.clone());
    }
    let object = w.finish().expect("finish");
    (corpus, object)
}

/// The spans of one trace, for checking a trace-restricted scan against the
/// same ground truth the match-all scan uses.
fn trace_subset(corpus: &[SpanRecord], trace_id: [u8; 16]) -> Vec<SpanRecord> {
    corpus
        .iter()
        .filter(|r| r.trace_id == trace_id)
        .cloned()
        .collect()
}

// --- order-insensitive record keys ----------------------------------------
//
// A scan returns records in block/scan order, which a mutation can reorder
// relative to the corpus vector, so equality is checked over sorted canonical
// keys, not vector-position by vector-position.

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

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_uv(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

fn record_key(r: &SpanRecord) -> Vec<u8> {
    let mut k = Vec::new();
    k.extend_from_slice(&r.trace_id);
    k.extend_from_slice(&r.span_id);
    match r.parent_span_id {
        Some(p) => {
            k.push(1);
            k.extend_from_slice(&p);
        }
        None => k.push(0),
    }
    put_str(&mut k, &r.name);
    k.extend_from_slice(&r.start_ts_ns.to_le_bytes());
    k.extend_from_slice(&r.end_ts_ns.to_le_bytes());
    k.push(r.status_code as u8);
    match &r.status_message {
        Some(m) => {
            k.push(1);
            put_str(&mut k, m);
        }
        None => k.push(0),
    }
    // `attrs` is canonical sorted with unique keys on both the corpus side
    // (the writer canonicalizes) and the decoded side, but sort here anyway so
    // the key is independent of that guarantee.
    let mut attrs = r.attrs.clone();
    attrs.sort();
    put_uv(&mut k, attrs.len() as u64);
    for (name, value) in &attrs {
        put_str(&mut k, name);
        put_str(&mut k, value);
    }
    k
}

fn normalize(v: &[SpanRecord]) -> Vec<Vec<u8>> {
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

        let cfg = RspanConfig::default();
        let all = SpanQuery::ts_range(i64::MIN, i64::MAX);
        if let Ok(reader) = RspanReader::new(&mutated, &cfg) {
            if let Ok((got, _stats)) = reader.scan(&all) {
                // A successful scan of a mutated object must still yield exactly
                // the original data: the damage hit unread or unverified bytes,
                // never silently altered a returned span.
                prop_assert_eq!(normalize(&got), normalize(&corpus));
            }
            // Same invariant through the trace-restricted path, which prunes on
            // the skip index's per-block trace ranges and the BLOOM: an Equals
            // on trace 0 either returns exactly that trace's true subset or a
            // typed error, never a wrong or narrowed result.
            let one_trace = SpanQuery::trace(tid(0), i64::MIN, i64::MAX);
            if let Ok((got, _stats)) = reader.scan(&one_trace) {
                prop_assert_eq!(normalize(&got), normalize(&trace_subset(&corpus, tid(0))));
            }
        }
    }

    /// The unmutated object always scans back to the full corpus, and a
    /// trace-restricted scan back to exactly that trace's spans.
    #[test]
    fn identity_roundtrip(_n in 0u8..1) {
        let (corpus, object) = fixture();
        let cfg = RspanConfig::default();
        let reader = RspanReader::new(&object, &cfg).expect("open");
        let (got, _) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan");
        prop_assert_eq!(normalize(&got), normalize(&corpus));

        let (got, _) = reader
            .scan(&SpanQuery::trace(tid(0), i64::MIN, i64::MAX))
            .expect("scan");
        prop_assert_eq!(normalize(&got), normalize(&trace_subset(&corpus, tid(0))));
    }
}
