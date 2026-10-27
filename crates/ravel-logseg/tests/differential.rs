//! Keystone differential suite: a pruned scan returns exactly what an
//! independent naive in-memory filter returns, over arbitrary corpora and
//! predicates. `naive_eval` shares no storage code with the reader (only the
//! tokenizer, which defines word semantics), so agreement is real evidence the
//! pruning is both sound and precise-enough not to drop matches.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use proptest::sample::Index;
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::{
    AttrValue, ColumnSelection, ColumnarLogBatch, FieldSel, FieldType, LogRecord, LogStreamId,
    ObjectIdentity, Predicate, RlogConfig, RlogReader, RlogWriter, stream_attrs_bytes,
};

const STREAMS: u8 = 6;
const TS_MAX: i64 = 400;
const NAMES: &[&str] = &["svc", "env", "code", "user", "path", "dur", "flag", "host"];
const WORDS: &[&str] = &[
    "timeout",
    "connection",
    "error",
    "ok",
    "retry",
    "fail",
    "cache",
    "slow",
];

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

// --- generators -----------------------------------------------------------

fn arb_value() -> impl Strategy<Value = AttrValue> {
    prop_oneof![
        proptest::sample::select(WORDS).prop_map(|w| AttrValue::Str(w.to_string())),
        Just(AttrValue::Str(String::new())),
        any::<i64>().prop_map(AttrValue::I64),
        prop_oneof![
            any::<f64>().prop_map(AttrValue::F64),
            Just(AttrValue::F64(-0.0)),
            Just(AttrValue::F64(0.0)),
            Just(AttrValue::F64(f64::from_bits(f64::NAN.to_bits() | 0x7))),
        ],
        any::<bool>().prop_map(AttrValue::Bool),
        proptest::collection::vec(any::<u8>(), 0..6).prop_map(AttrValue::Bytes),
    ]
}

fn arb_body() -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::sample::select(WORDS), 0..4).prop_map(|ws| ws.join(" "))
}

fn arb_record() -> impl Strategy<Value = LogRecord> {
    (
        0u8..STREAMS,
        0i64..TS_MAX,
        arb_body(),
        0u8..30,
        // Up to four attributes with distinct names.
        proptest::collection::vec((proptest::sample::select(NAMES), arb_value()), 0..4),
        proptest::option::of(any::<[u8; 16]>()),
    )
        .prop_map(|(stream, ts, body, sev, raw_attrs, trace)| {
            let mut seen = std::collections::HashSet::new();
            let mut attrs = Vec::new();
            for (name, v) in raw_attrs {
                if seen.insert(name) {
                    attrs.push((name.to_string(), v));
                }
            }
            LogRecord {
                stream_id: sid(stream),
                stream_attrs: stream_blob(stream),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: sev,
                severity_text: if sev % 2 == 0 { "INFO" } else { "WARN" }.to_string(),
                body,
                trace_id: trace,
                span_id: None,
                flags: 0,
                attrs,
            }
        })
}

fn arb_corpus() -> impl Strategy<Value = Vec<LogRecord>> {
    proptest::collection::vec(arb_record(), 1..250)
}

fn arb_field() -> impl Strategy<Value = FieldSel> {
    prop_oneof![
        Just(FieldSel::Body),
        Just(FieldSel::SeverityText),
        proptest::sample::select(NAMES).prop_map(|n| FieldSel::Attr(n.to_string())),
    ]
}

fn arb_word() -> impl Strategy<Value = String> {
    // Words drawn both from the corpus vocabulary and from outside it.
    prop_oneof![
        proptest::sample::select(WORDS).prop_map(str::to_string),
        Just("absent".to_string()),
        Just("connection timeout".to_string()),
    ]
}

fn arb_arm() -> impl Strategy<Value = Predicate> {
    prop_oneof![
        (0i64..TS_MAX, 0i64..TS_MAX).prop_map(|(a, b)| Predicate::TsRange {
            min_ns: a.min(b),
            max_ns: a.max(b),
        }),
        proptest::collection::vec(0u8..(STREAMS + 1), 0..3)
            .prop_map(|v| Predicate::StreamIn(v.into_iter().map(sid).collect())),
        (arb_field(), arb_word()).prop_map(|(field, word)| Predicate::HasWord { field, word }),
        (arb_field(), arb_value()).prop_map(|(field, value)| Predicate::Equals { field, value }),
    ]
}

fn arb_predicate() -> impl Strategy<Value = Predicate> {
    proptest::collection::vec(arb_arm(), 0..4).prop_map(Predicate::And)
}

// --- independent naive evaluator ------------------------------------------

fn field_text<'a>(rec: &'a LogRecord, field: &FieldSel) -> Option<&'a str> {
    match field {
        FieldSel::Body => Some(&rec.body),
        FieldSel::SeverityText => Some(&rec.severity_text),
        FieldSel::Attr(name) => rec.attrs.iter().find_map(|(k, v)| match v {
            AttrValue::Str(s) if k == name => Some(s.as_str()),
            _ => None,
        }),
    }
}

fn phrase_match(text: &str, word: &str) -> bool {
    let query = tokens(word);
    if query.is_empty() {
        return true;
    }
    let toks = tokens(text);
    toks.windows(query.len()).any(|w| w == query.as_slice())
}

fn value_eq(a: &AttrValue, b: &AttrValue) -> bool {
    match (a, b) {
        (AttrValue::F64(x), AttrValue::F64(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

fn eval(rec: &LogRecord, pred: &Predicate) -> bool {
    match pred {
        Predicate::And(v) => v.iter().all(|p| eval(rec, p)),
        Predicate::TsRange { min_ns, max_ns } => rec.ts_ns >= *min_ns && rec.ts_ns <= *max_ns,
        Predicate::StreamIn(ids) => ids.contains(&rec.stream_id),
        Predicate::HasWord { field, word } => {
            field_text(rec, field).is_some_and(|t| phrase_match(t, word))
        }
        Predicate::Equals { field, value } => match field {
            FieldSel::Body => matches!(value, AttrValue::Str(s) if *s == rec.body),
            FieldSel::SeverityText => {
                matches!(value, AttrValue::Str(s) if *s == rec.severity_text)
            }
            FieldSel::Attr(name) => rec
                .attrs
                .iter()
                .any(|(k, v)| k == name && value_eq(v, value)),
        },
        // Prune-only (ADR-0095 decision 6): never an exact per-row filter, and
        // `arb_predicate` never generates it. Mirror the reader's exact channel
        // (match every row) so the differential stays correct if one appears.
        Predicate::NumRange { .. } | Predicate::NumNotEq { .. } => true,
        // Keeps rows a later exact filter decides, so it has no record-level
        // mirror; the reader's own row-by-row oracle covers it.
        Predicate::Contains { .. } => unreachable!("arb_predicate never generates Contains"),
    }
}

fn naive_eval(corpus: &[LogRecord], pred: &Predicate) -> Vec<LogRecord> {
    corpus.iter().filter(|r| eval(r, pred)).cloned().collect()
}

// --- normalization --------------------------------------------------------

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
        AttrValue::List(_) | AttrValue::Map(_) => unreachable!("corpus uses scalar/bytes only"),
    }
}

/// A deterministic, order-insensitive key for a record. f64 travels by bits so
/// -0.0 and NaN payloads are distinguished; attributes are sorted by key.
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

fn normalize(v: Vec<LogRecord>) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = v.iter().map(record_key).collect();
    keys.sort();
    keys
}

fn write_object(corpus: &[LogRecord]) -> Vec<u8> {
    write_with(
        RlogConfig {
            block_target_records: 32,
            ..RlogConfig::default()
        },
        &[],
        corpus,
        false,
    )
}

/// `corpus` written with `chunk_order` (ADR-3217 decision 8) and POSTINGS on
/// the `indexed` names into row groups of two blocks, through the columnar
/// path when `columnar` is set.
fn write_object_ordered(
    corpus: &[LogRecord],
    chunk_order: &'static [&'static str],
    indexed: &[&str],
    columnar: bool,
) -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: 32,
        group_target_blocks: 2,
        chunk_order,
        ..RlogConfig::default()
    };
    write_with(cfg, indexed, corpus, columnar)
}

fn write_with(cfg: RlogConfig, indexed: &[&str], corpus: &[LogRecord], columnar: bool) -> Vec<u8> {
    let identity = ObjectIdentity {
        tenant_hash: [0u8; 16],
        shard: 0,
        writer_id: [0u8; 16],
        writer_epoch: 0,
        writer_seq: 0,
    };
    let mut w = RlogWriter::new(cfg, identity)
        .with_indexed_fields(indexed.iter().map(|n| n.to_string()).collect());
    if columnar {
        w.push_columnar(ColumnarLogBatch::from_records(corpus))
            .expect("push_columnar");
    } else {
        for r in corpus {
            w.push(r.clone()).expect("push");
        }
    }
    w.finish().expect("finish")
}

/// A chunk order drawn from the corpus's attribute names, in any order; a
/// name no record of a given corpus carries is part of the case.
fn arb_chunk_order() -> impl Strategy<Value = Vec<&'static str>> {
    proptest::sample::subsequence(NAMES.to_vec(), 1..=NAMES.len()).prop_shuffle()
}

/// The attribute names an object indexes in POSTINGS: at least one, chosen
/// independently of the chunk order.
fn arb_indexed() -> impl Strategy<Value = Vec<&'static str>> {
    proptest::sample::subsequence(NAMES.to_vec(), 1..=NAMES.len())
}

/// How a corpus attribute becomes an arm (see [`picked_arms`]).
#[derive(Clone, Copy, Debug)]
enum PickShape {
    ContentEquals,
    PruneEquals,
    RangePoint,
    RangeFrom,
    RangeTo,
}

/// Attributes drawn from the corpus by (record, attribute) index, so the
/// arms built from them name terms and values the object holds.
fn arb_picks() -> impl Strategy<Value = Vec<(Index, Index, PickShape)>> {
    let shape = prop_oneof![
        Just(PickShape::ContentEquals),
        Just(PickShape::PruneEquals),
        Just(PickShape::RangePoint),
        Just(PickShape::RangeFrom),
        Just(PickShape::RangeTo),
    ];
    proptest::collection::vec((any::<Index>(), any::<Index>(), shape), 1..6)
}

/// A numeric attribute value as a `NumRange` column type and bound bits; none
/// for a non-numeric value or a NaN, which `NumRange` must not be given.
fn num_bound(value: &AttrValue) -> Option<(FieldType, u64)> {
    match value {
        AttrValue::I64(x) => Some((FieldType::I64, *x as u64)),
        AttrValue::F64(f) if !f.is_nan() => Some((FieldType::F64, f.to_bits())),
        AttrValue::Bool(b) => Some((FieldType::Bool, u64::from(*b))),
        _ => None,
    }
}

/// The picked attributes as `(content, prune)` arms. A content `Equals`
/// filters exactly and probes POSTINGS when its name is indexed; a prune
/// `Equals` probes POSTINGS only; a `NumRange`, placed in the prune channel
/// since that is the only one it prunes from, prunes on the attribute's
/// SKIP_IDX stats. A range pick on a non-numeric value becomes a prune
/// `Equals`.
fn picked_arms(
    corpus: &[LogRecord],
    picks: &[(Index, Index, PickShape)],
) -> (Vec<Predicate>, Vec<Predicate>) {
    let mut content = Vec::new();
    let mut prune = Vec::new();
    for (rec, attr, shape) in picks {
        let rec = &corpus[rec.index(corpus.len())];
        if rec.attrs.is_empty() {
            continue;
        }
        let (name, value) = &rec.attrs[attr.index(rec.attrs.len())];
        let field = FieldSel::Attr(name.clone());
        let equals = Predicate::Equals {
            field: field.clone(),
            value: value.clone(),
        };
        let range = |min, max| {
            num_bound(value).map(|(ty, _)| Predicate::NumRange {
                field: field.clone(),
                ty,
                min,
                max,
            })
        };
        let bits = num_bound(value).map(|(_, bits)| bits);
        let arm = match shape {
            PickShape::ContentEquals => {
                content.push(equals);
                continue;
            }
            PickShape::PruneEquals => None,
            PickShape::RangePoint => range(bits, bits),
            PickShape::RangeFrom => range(bits, None),
            PickShape::RangeTo => range(None, bits),
        };
        prune.push(arm.unwrap_or(equals));
    }
    (content, prune)
}

/// Every record the block scan returns under `columns`, block by block.
fn scan_projected(
    object: &[u8],
    pred: &Predicate,
    prune: &[Predicate],
    columns: &ColumnSelection,
) -> Vec<LogRecord> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg).expect("open");
    let mut cursor = reader
        .scan_blocks(pred, prune, columns)
        .expect("scan_blocks");
    let mut out = Vec::new();
    while let Some(rows) = cursor.next_block(object).expect("next_block") {
        out.extend(rows);
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

    #[test]
    fn pruned_scan_equals_naive_scan(corpus in arb_corpus(), pred in arb_predicate()) {
        let bytes = write_object(&corpus);
        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&bytes, &cfg).expect("open");
        let (got, _stats) = reader.scan(&pred).expect("scan");
        let want = naive_eval(&corpus, &pred);
        prop_assert_eq!(normalize(got), normalize(want));
    }

    /// A declared chunk order changes which name gets which column id and
    /// nothing a read returns: the pruned scan, the same scan with a prune
    /// channel, and the block scan projected onto the declared names agree
    /// with the default-order object on both writer paths.
    ///
    /// Both objects index a generated set of names in POSTINGS, and the arms
    /// drawn from the corpus reach the id-keyed indexes with terms the object
    /// holds: an `Equals` on an indexed name probes POSTINGS from either
    /// channel, and a prune `NumRange` on a numeric attribute prunes on its
    /// SKIP_IDX stat. Where the declared order moves an attribute's id, an
    /// index entry keyed by the wrong id prunes a different block set than
    /// the default object's, which the comparison sees as missing rows.
    #[test]
    fn chunk_order_reads_equal_the_default_order(
        corpus in arb_corpus(),
        pred in arb_predicate(),
        picks in arb_picks(),
        order in arb_chunk_order(),
        indexed in arb_indexed(),
        columnar in any::<bool>(),
    ) {
        let order: &'static [&'static str] = order.leak();
        let (content, prune) = picked_arms(&corpus, &picks);
        let pred = Predicate::And(vec![pred, Predicate::And(content)]);
        let default_object = write_object_ordered(&corpus, &[], &indexed, columnar);
        let ordered_object = write_object_ordered(&corpus, order, &indexed, columnar);
        let cfg = RlogConfig::default();
        let scan = |object: &[u8], prune: &[Predicate]| {
            RlogReader::new(object, &cfg)
                .expect("open")
                .scan_pruned(&pred, prune)
                .expect("scan")
                .0
        };
        let got = scan(&ordered_object, &[]);
        prop_assert_eq!(normalize(got.clone()), normalize(scan(&default_object, &[])));
        prop_assert_eq!(normalize(got), normalize(naive_eval(&corpus, &pred)));
        prop_assert_eq!(
            normalize(scan(&ordered_object, &prune)),
            normalize(scan(&default_object, &prune))
        );

        let mut columns = ColumnSelection::fixed_only().with_body().with_severity_text();
        for name in order.iter().take(2) {
            columns = columns.with_attr(*name);
        }
        prop_assert_eq!(
            normalize(scan_projected(&ordered_object, &pred, &prune, &columns)),
            normalize(scan_projected(&default_object, &pred, &prune, &columns))
        );
    }
}
