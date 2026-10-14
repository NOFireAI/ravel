//! Hostile-input suite. One valid RSPAN object is mutated every way a byte
//! stream can be damaged (truncate, single-byte flip, splice), and the reader
//! must either succeed with exactly the original data (the mutation hit unread
//! or unverified bytes) or return a typed error. It must never panic and never
//! return records that differ from the original without an error.
//!
//! The KEY_IDX probe gets the same treatment, through both the whole-object
//! and the ranged reader: a probe on a damaged object returns a superset of
//! the blocks truly holding the trace, or an error, and the rows read from the
//! blocks it names are exactly that trace's. Case count honours
//! `PROPTEST_CASES` (600 when unset); the CI fuzz job raises it.
#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::ops::Range;

use proptest::prelude::*;
use ravel_rspan::footer::{DEFAULT_MAX_SECTION_UNCOMP, kind, write_footer_and_trailer};
use ravel_rspan::{
    ObjectIdentity, RspanConfig, RspanRangeReader, RspanReader, RspanWriter, SpanQuery, SpanRecord,
    SpanSegError, StatusCode, open, read_section,
};

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600)
}

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

// --- KEY_IDX probe oracle ---------------------------------------------------

/// The traces probed on every case: the five fixture traces plus one absent.
fn probed_traces() -> Vec<[u8; 16]> {
    (0..5).map(tid).chain([tid(0xEE)]).collect()
}

/// A ranged reader over `object`, or `None` when its footer or SKIP_IDX does
/// not open (a damaged object the ranged path refuses before any probe).
fn range_reader(object: &[u8]) -> Option<RspanRangeReader> {
    let footer = open(object).ok()?;
    let skip_raw = read_section(
        object,
        footer.section(kind::SKIP_IDX)?,
        DEFAULT_MAX_SECTION_UNCOMP,
    )
    .ok()?;
    RspanRangeReader::from_sections(&footer, &skip_raw).ok()
}

/// The ordinals of the blocks of the unmutated fixture holding `trace`.
fn true_blocks(object: &[u8], trace: &[u8; 16]) -> BTreeSet<u32> {
    let ranged = range_reader(object).expect("fixture opens");
    (0..ranged.block_count())
        .filter(|&i| {
            let loc = ranged.block_loc(i).expect("loc");
            ranged
                .decode_block(&loc, &object[loc.start() as usize..loc.end() as usize])
                .expect("decode")
                .iter()
                .any(|r| r.trace_id == *trace)
        })
        .map(|i| i as u32)
        .collect()
}

/// A probe answer on a damaged object: `None` (the section declined, so every
/// block is a candidate) or a superset of the true blocks; and when every
/// named block decodes, the rows of `trace` among them are exactly its spans.
fn check_probe_answer(
    corpus: &[SpanRecord],
    original: &[u8],
    mutated: &[u8],
    trace: &[u8; 16],
    answer: Option<Vec<u32>>,
) -> Result<(), TestCaseError> {
    let Some(blocks) = answer else {
        return Ok(());
    };
    let named: BTreeSet<u32> = blocks.iter().copied().collect();
    prop_assert!(
        named.is_superset(&true_blocks(original, trace)),
        "probe for {:02x} named {:?}, missing a true block",
        trace[0],
        blocks
    );
    if let Some(ranged) = range_reader(mutated) {
        let mut rows = Vec::new();
        for b in &blocks {
            let Ok(loc) = ranged.block_loc(*b as usize) else {
                return Ok(());
            };
            let Some(bytes) = mutated.get(loc.start() as usize..loc.end() as usize) else {
                return Ok(());
            };
            let Ok(decoded) = ranged.decode_block(&loc, bytes) else {
                return Ok(());
            };
            rows.extend(decoded.into_iter().filter(|r| r.trace_id == *trace));
        }
        prop_assert_eq!(normalize(&rows), normalize(&trace_subset(corpus, *trace)));
    }
    Ok(())
}

/// Probes every traced id through the whole-object reader (when it opens) and
/// the ranged reader (when its footer and SKIP_IDX open), checking each answer.
fn check_probes(
    corpus: &[SpanRecord],
    original: &[u8],
    mutated: &[u8],
) -> Result<(), TestCaseError> {
    if let Ok(reader) = RspanReader::new(mutated, &RspanConfig::default()) {
        for trace in probed_traces() {
            if let Ok(answer) = reader.probe_trace(&trace) {
                check_probe_answer(corpus, original, mutated, &trace, answer)?;
            }
        }
    }
    if let Some(ranged) = range_reader(mutated) {
        for trace in probed_traces() {
            let read = |r: Range<u64>| {
                mutated
                    .get(r.start as usize..r.end as usize)
                    .ok_or_else(|| SpanSegError::Corrupted("range past object".into()))
            };
            if let Ok(answer) = ranged.probe_trace(&trace, read) {
                check_probe_answer(corpus, original, mutated, &trace, answer)?;
            }
        }
    }
    Ok(())
}

/// The object split into its section bytes and decoded footer.
fn split(object: &[u8]) -> (Vec<u8>, ravel_rspan::SpanFooter) {
    let footer = open(object).expect("open fixture");
    let n = object.len();
    let footer_len = u32::from_le_bytes(object[n - 16..n - 12].try_into().expect("4 bytes"));
    let body_len = n - 16 - footer_len as usize;
    (object[..body_len].to_vec(), footer)
}

fn reassemble(mut body: Vec<u8>, footer: &ravel_rspan::SpanFooter) -> Vec<u8> {
    write_footer_and_trailer(&mut body, footer);
    body
}

fn key_idx_entry(footer: &mut ravel_rspan::SpanFooter) -> &mut ravel_rspan::footer::SectionDesc {
    footer
        .sections
        .iter_mut()
        .find(|s| s.kind == kind::KEY_IDX)
        .expect("KEY_IDX entry")
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

    /// Damage confined to the KEY_IDX section bytes or its footer entry. The
    /// section crc rewritten to match (op 1) or the footer entry moved (op 2+)
    /// takes the whole-object reader past its crc check, so the grammar's own
    /// checks and the RSPAN parameter and ordinal rules are what stand.
    #[test]
    fn key_idx_mutation_probe_is_superset_or_error(
        op in 0u8..7,
        idx in any::<usize>(),
        xor in any::<u8>(),
        value in any::<u32>(),
    ) {
        let (corpus, object) = fixture();
        let (mut body, mut footer) = split(&object);
        let entry = *key_idx_entry(&mut footer);
        let section = entry.offset as usize..(entry.offset + entry.len) as usize;
        let flip = section.start + idx % section.len();
        let refuse_whole = match op {
            // A byte flip in the section: the stored crc refuses it at open.
            0 => {
                body[flip] ^= xor | 1;
                true
            }
            // The same flip with the footer crc rewritten to match.
            1 => {
                body[flip] ^= xor | 1;
                key_idx_entry(&mut footer).crc32c = crc32c::crc32c(&body[section.clone()]);
                false
            }
            // Footer-entry mutations: prefix_len, offset, len, comp,
            // uncomp_len, each to an arbitrary value.
            2 => {
                key_idx_entry(&mut footer).prefix_len = value;
                false
            }
            3 => {
                key_idx_entry(&mut footer).offset = u64::from(value) % body.len() as u64;
                false
            }
            4 => {
                key_idx_entry(&mut footer).len = u64::from(value) % (body.len() as u64 + 1);
                false
            }
            5 => {
                key_idx_entry(&mut footer).comp = value as u8;
                false
            }
            _ => {
                key_idx_entry(&mut footer).uncomp_len = u64::from(value);
                false
            }
        };
        let mutated = reassemble(body, &footer);
        let cfg = RspanConfig::default();
        let opened = RspanReader::new(&mutated, &cfg);
        if refuse_whole {
            prop_assert!(matches!(opened, Err(SpanSegError::Corrupted(_))));
        }
        if let Ok(reader) = &opened
            && let Ok((got, _)) = reader.scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
        {
            prop_assert_eq!(normalize(&got), normalize(&corpus));
        }
        check_probes(&corpus, &object, &mutated)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

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
        check_probes(&corpus, &object, &mutated)?;
    }
}

/// The unmutated object always scans back to the full corpus, a
/// trace-restricted scan back to exactly that trace's spans, and both probes
/// name exactly the true blocks. Deterministic, so one run.
#[test]
fn identity_roundtrip() {
    let (corpus, object) = fixture();
    let cfg = RspanConfig::default();
    let reader = RspanReader::new(&object, &cfg).expect("open");
    let (got, _) = reader
        .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
        .expect("scan");
    assert_eq!(normalize(&got), normalize(&corpus));

    let (got, _) = reader
        .scan(&SpanQuery::trace(tid(0), i64::MIN, i64::MAX))
        .expect("scan");
    assert_eq!(normalize(&got), normalize(&trace_subset(&corpus, tid(0))));

    let ranged = range_reader(&object).expect("ranged");
    for trace in probed_traces() {
        let want: Vec<u32> = true_blocks(&object, &trace).into_iter().collect();
        assert_eq!(
            reader.probe_trace(&trace).expect("probe"),
            Some(want.clone())
        );
        let read = |r: Range<u64>| Ok::<_, SpanSegError>(&object[r.start as usize..r.end as usize]);
        assert_eq!(ranged.probe_trace(&trace, read).expect("probe"), Some(want));
    }
}
