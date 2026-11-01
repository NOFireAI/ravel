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
    ObjectIdentity, PageChoice, Predicate, RlogConfig, RlogReader, RlogWriter, stream_attrs_bytes,
};

#[path = "../benches/common/ingest_shape.rs"]
mod ingest_shape;

const STREAMS: u8 = 6;
const TS_MAX: i64 = 400;
/// `arb_typed_attr`'s `I64` values lie in `-NARROW_I64..=NARROW_I64 + 2`:
/// `dur` in `-NARROW_I64..=NARROW_I64`, `code` up to `NARROW_I64 + 2` on stream 5.
const NARROW_I64: i64 = 8;
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
            make_record(stream, ts, body, sev, raw_attrs, trace)
        })
}

fn make_record(
    stream: u8,
    ts: i64,
    body: String,
    sev: u8,
    raw_attrs: Vec<(&'static str, AttrValue)>,
    trace: Option<[u8; 16]>,
) -> LogRecord {
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
        severity_text: if sev.is_multiple_of(2) {
            "INFO"
        } else {
            "WARN"
        }
        .to_string(),
        body,
        trace_id: trace,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn arb_corpus() -> impl Strategy<Value = Vec<LogRecord>> {
    proptest::collection::vec(arb_record(), 1..250)
}

/// The chunk-order test's attributes for a record of `stream` at `ts`, typed
/// by name and banded so the arms built from them prune part of an object.
/// The writer lays records out by stream and then timestamp, so a value banded
/// by either sits in some blocks and not others: per stream, `svc` draws from
/// two adjacent words, `code` from a four-wide `I64` band, `user` from two
/// adjacent members of an `F64` set holding -0.0, 0.0 and a NaN payload, and
/// `flag` leans true on even streams; `dur` draws a two-wide `I64` band that
/// rises with the timestamp. The `I64` bands lie in `-NARROW_I64..NARROW_I64`
/// plus their width, so they straddle zero. `env` and `path` draw any word or
/// the empty string in every block. `host` keeps [`arb_value`]'s mixed types
/// and full ranges, so a `Str` prune on a name that also has a non-`Str`
/// column, an open bound every block satisfies, and `Bytes` stay reachable.
fn arb_typed_attr(stream: u8, ts: i64) -> impl Strategy<Value = (&'static str, AttrValue)> {
    const FLOATS: [f64; 6] = [-2.5, -0.0, 0.0, 1.0, 3.5, f64::NAN];
    let s = usize::from(stream);
    let svc = vec![WORDS[s % WORDS.len()], WORDS[(s + 1) % WORDS.len()]];
    let user: Vec<f64> = [FLOATS[s % FLOATS.len()], FLOATS[(s + 1) % FLOATS.len()]]
        .iter()
        .map(|f| {
            if f.is_nan() {
                f64::from_bits(f.to_bits() | 0x7)
            } else {
                *f
            }
        })
        .collect();
    let code = i64::from(stream) * 3 - NARROW_I64;
    let dur = ts * 2 * NARROW_I64 / TS_MAX - NARROW_I64;
    let word = prop_oneof![
        4 => proptest::sample::select(WORDS).prop_map(|w| AttrValue::Str(w.to_string())),
        1 => Just(AttrValue::Str(String::new())),
    ];
    let even = stream.is_multiple_of(2);
    prop_oneof![
        1 => (
            Just("svc"),
            proptest::sample::select(svc).prop_map(|w| AttrValue::Str(w.to_string()))
        ),
        2 => (proptest::sample::select(&["env", "path"][..]), word),
        1 => (Just("code"), (code..code + 4).prop_map(AttrValue::I64)),
        1 => (Just("dur"), (dur..dur + 2).prop_map(AttrValue::I64)),
        1 => (
            Just("flag"),
            proptest::bool::weighted(if even { 0.9 } else { 0.1 }).prop_map(AttrValue::Bool)
        ),
        1 => (Just("user"), proptest::sample::select(user).prop_map(AttrValue::F64)),
        1 => (Just("host"), arb_value()),
    ]
}

/// A record for the chunk-order test, its attributes from [`arb_typed_attr`].
fn arb_typed_record() -> impl Strategy<Value = LogRecord> {
    (0u8..STREAMS, 0i64..TS_MAX)
        .prop_flat_map(|(stream, ts)| {
            (
                Just(stream),
                Just(ts),
                arb_body(),
                0u8..30,
                proptest::collection::vec(arb_typed_attr(stream, ts), 0..4),
                proptest::option::of(any::<[u8; 16]>()),
            )
        })
        .prop_map(|(stream, ts, body, sev, raw_attrs, trace)| {
            make_record(stream, ts, body, sev, raw_attrs, trace)
        })
}

fn arb_typed_corpus() -> impl Strategy<Value = Vec<LogRecord>> {
    proptest::collection::vec(arb_typed_record(), 1..250)
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
/// arms built from them name terms and values the object holds. Three in four
/// content picks share one anchor record, so the content arms match at least
/// that record and a prune arm drawn from any record has rows left to prune;
/// the rest draw their own record, which keeps content arms no single record
/// satisfies reachable.
fn arb_picks() -> impl Strategy<Value = Vec<(Index, Index, PickShape)>> {
    let shape = prop_oneof![
        Just(PickShape::ContentEquals),
        Just(PickShape::PruneEquals),
        Just(PickShape::RangePoint),
        Just(PickShape::RangeFrom),
        Just(PickShape::RangeTo),
    ];
    let pick = (
        any::<Index>(),
        proptest::bool::weighted(0.75),
        any::<Index>(),
        shape,
    );
    (any::<Index>(), proptest::collection::vec(pick, 1..6)).prop_map(|(anchor, picks)| {
        picks
            .into_iter()
            .map(|(rec, anchored, attr, shape)| {
                let content = matches!(shape, PickShape::ContentEquals);
                (if anchored && content { anchor } else { rec }, attr, shape)
            })
            .collect()
    })
}

/// One case of [`chunk_order_reads_equal_the_default_order`]: corpus, random
/// content predicate, picks, chunk order, indexed names, writer path.
type ChunkOrderCase = (
    Vec<LogRecord>,
    Predicate,
    Vec<(Index, Index, PickShape)>,
    Vec<&'static str>,
    Vec<&'static str>,
    bool,
);

/// The generators of [`chunk_order_reads_equal_the_default_order`], shared
/// with [`chunk_order_prune_channel_prunes_partially`]. The random content
/// predicate is empty three times in four: an [`arb_predicate`] conjunction
/// matches no row in most cases, and a case with no row left leaves the prune
/// channel nothing to remove.
fn arb_chunk_order_case() -> impl Strategy<Value = ChunkOrderCase> {
    (
        arb_typed_corpus(),
        prop_oneof![3 => Just(Predicate::And(Vec::new())), 1 => arb_predicate()],
        arb_picks(),
        arb_chunk_order(),
        arb_indexed(),
        any::<bool>(),
    )
}

/// A chunk-order case ready to read: the content predicate composed with the
/// content arms, the prune arms, and the corpus written in the default order
/// and in the declared one.
struct ChunkOrderReads {
    corpus: Vec<LogRecord>,
    pred: Predicate,
    prune: Vec<Predicate>,
    order: &'static [&'static str],
    default_object: Vec<u8>,
    ordered_object: Vec<u8>,
}

/// A generated case built for its reads, its arms from [`picked_arms`].
fn chunk_order_reads(case: ChunkOrderCase) -> ChunkOrderReads {
    let (corpus, pred, picks, order, indexed, columnar) = case;
    let (content, prune) = picked_arms(&corpus, &picks);
    chunk_order_reads_from_arms(
        corpus,
        pred,
        content,
        prune,
        order.leak(),
        &indexed,
        columnar,
    )
}

/// A case built for its reads from arms given directly, as a pinned case
/// gives them: `Index` has no public constructor, so picks cannot be written
/// as literals.
fn chunk_order_reads_from_arms(
    corpus: Vec<LogRecord>,
    pred: Predicate,
    content: Vec<Predicate>,
    prune: Vec<Predicate>,
    order: &'static [&'static str],
    indexed: &[&str],
    columnar: bool,
) -> ChunkOrderReads {
    let pred = Predicate::And(vec![pred, Predicate::And(content)]);
    let default_object = write_object_ordered(&corpus, &[], indexed, columnar);
    let ordered_object = write_object_ordered(&corpus, order, indexed, columnar);
    ChunkOrderReads {
        corpus,
        pred,
        prune,
        order,
        default_object,
        ordered_object,
    }
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
/// `Equals`. Picks index the records that carry an attribute.
fn picked_arms(
    corpus: &[LogRecord],
    picks: &[(Index, Index, PickShape)],
) -> (Vec<Predicate>, Vec<Predicate>) {
    let mut content = Vec::new();
    let mut prune = Vec::new();
    let carriers: Vec<&LogRecord> = corpus.iter().filter(|r| !r.attrs.is_empty()).collect();
    if carriers.is_empty() {
        return (content, prune);
    }
    for (rec, attr, shape) in picks {
        let rec = carriers[rec.index(carriers.len())];
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

/// Whether `rec` carries what a prune arm built by [`picked_arms`] keeps: the
/// `Equals` value, or a value of the `NumRange` type inside its bounds under
/// the order the skip index folds by (`i64` signed, `f64` by `total_cmp`,
/// `bool` as 0/1). A NaN value counts as outside every range, which can only
/// shrink the set the guard in [`chunk_order_reads_equal_the_default_order`]
/// requires.
fn satisfies_prune_arm(rec: &LogRecord, arm: &Predicate) -> bool {
    match arm {
        Predicate::Equals { .. } => eval(rec, arm),
        Predicate::NumRange {
            field: FieldSel::Attr(name),
            ty,
            min,
            max,
        } => rec.attrs.iter().any(|(k, v)| {
            k == name
                && num_bound(v).is_some_and(|(vty, bits)| {
                    vty == *ty
                        && min.is_none_or(|m| bits_cmp(*ty, m, bits).is_le())
                        && max.is_none_or(|m| bits_cmp(*ty, bits, m).is_le())
                })
        }),
        _ => unreachable!("picked_arms builds Equals and NumRange prune arms only"),
    }
}

/// The order `crates/ravel-logseg/src/skip_index.rs` folds `min_bits` and
/// `max_bits` under: `i64` signed, `f64` by `total_cmp`, anything else unsigned.
fn bits_cmp(ty: FieldType, a: u64, b: u64) -> std::cmp::Ordering {
    match ty {
        FieldType::F64 => f64::from_bits(a).total_cmp(&f64::from_bits(b)),
        FieldType::I64 => (a as i64).cmp(&(b as i64)),
        _ => a.cmp(&b),
    }
}

/// Whether every key of `sub` appears in `sup` at least as often; both sorted,
/// as [`normalize`] returns them.
fn is_sub_multiset(sub: &[Vec<u8>], sup: &[Vec<u8>]) -> bool {
    let mut rest = sup.iter();
    sub.iter().all(|k| rest.by_ref().any(|s| s == k))
}

/// What a case's prune channel does to the rows the default-order object's
/// content scan returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PruneEffect {
    /// The picks built no prune arm.
    NoArm,
    /// The same rows come back, including a content scan that returns none.
    Nothing,
    /// Some rows are removed and some kept: the only effect under which an
    /// index keyed by the wrong column id changes the compared row set.
    Partial,
    /// Every row is removed.
    Empty,
}

fn prune_effect(object: &[u8], content: &Predicate, prune: &[Predicate]) -> PruneEffect {
    if prune.is_empty() {
        return PruneEffect::NoArm;
    }
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg).expect("open");
    let rows = |prune: &[Predicate]| reader.scan_pruned(content, prune).expect("scan").0.len();
    let (all, kept) = (rows(&[]), rows(prune));
    if kept == all {
        PruneEffect::Nothing
    } else if kept == 0 {
        PruneEffect::Empty
    } else {
        PruneEffect::Partial
    }
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

/// The assertions of [`chunk_order_reads_equal_the_default_order`], for a
/// generated case and for a pinned one alike.
fn check_chunk_order_reads(case: &ChunkOrderReads) -> Result<(), TestCaseError> {
    let ChunkOrderReads {
        corpus,
        pred,
        prune,
        order,
        default_object,
        ordered_object,
    } = case;
    let cfg = RlogConfig::default();
    let scan = |object: &[u8], prune: &[Predicate]| {
        RlogReader::new(object, &cfg)
            .expect("open")
            .scan_pruned(pred, prune)
            .expect("scan")
            .0
    };
    let got = scan(ordered_object, &[]);
    let matches = naive_eval(corpus, pred);
    prop_assert_eq!(normalize(got.clone()), normalize(scan(default_object, &[])));
    prop_assert_eq!(normalize(got), normalize(matches.clone()));
    let pruned = normalize(scan(ordered_object, prune));
    prop_assert_eq!(&pruned, &normalize(scan(default_object, prune)));
    let must_keep = normalize(
        matches
            .iter()
            .filter(|r| prune.iter().all(|arm| satisfies_prune_arm(r, arm)))
            .cloned()
            .collect(),
    );
    prop_assert!(
        is_sub_multiset(&must_keep, &pruned),
        "the prune channel dropped a content match that satisfies every prune arm: \
         {} such rows, {} kept",
        must_keep.len(),
        pruned.len()
    );
    prop_assert!(
        is_sub_multiset(&pruned, &normalize(matches)),
        "the pruned read returned a row the content predicate does not match"
    );

    let mut columns = ColumnSelection::fixed_only()
        .with_body()
        .with_severity_text();
    for name in order.iter().take(2) {
        columns = columns.with_attr(*name);
    }
    prop_assert_eq!(
        normalize(scan_projected(ordered_object, pred, prune, &columns)),
        normalize(scan_projected(default_object, pred, prune, &columns))
    );
    Ok(())
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
    ///
    /// The pruned reads must also keep every content match that satisfies
    /// each prune arm, and nothing outside the content matches, so two reads
    /// that prune to the same wrong rows, or to nothing, do not pass as
    /// agreement.
    #[test]
    fn chunk_order_reads_equal_the_default_order(case in arb_chunk_order_case()) {
        check_chunk_order_reads(&chunk_order_reads(case))?;
    }
}

/// The stream attribute bytes both recorded shrinks of
/// [`chunk_order_reads_equal_the_default_order`] carry, copied from their
/// regressions file comments.
const RECORDED_STREAM_ATTRS: [u8; 27] = [
    1, 12, 115, 101, 114, 118, 105, 99, 101, 46, 110, 97, 109, 101, 1, 2, 115, 48, 5, 115, 99, 111,
    112, 101, 1, 49, 0,
];

/// The one record of a recorded shrink: stream 0 at timestamp 0, severity 0
/// ("INFO"), empty body, no trace or span id, flags 0, carrying `attrs`.
fn recorded_record(attrs: Vec<(&'static str, AttrValue)>) -> LogRecord {
    let rec = make_record(0, 0, String::new(), 0, attrs, None);
    assert_eq!(rec.stream_attrs, RECORDED_STREAM_ATTRS);
    rec
}

/// The shrink seed b662e1fd recorded for
/// [`chunk_order_reads_equal_the_default_order`] before the typed corpus,
/// the picks and the indexed names: `svc` and `code` hold the same word and
/// `svc` is declared first. No arms and no indexed names, as recorded, on
/// both writer paths.
#[test]
fn chunk_order_recorded_shrink_svc_and_code_same_word() {
    for columnar in [false, true] {
        let rec = recorded_record(vec![
            ("svc", AttrValue::Str("timeout".to_string())),
            ("code", AttrValue::Str("timeout".to_string())),
        ]);
        let case = chunk_order_reads_from_arms(
            vec![rec],
            Predicate::And(Vec::new()),
            Vec::new(),
            Vec::new(),
            &["svc"],
            &[],
            columnar,
        );
        if let Err(e) = check_chunk_order_reads(&case) {
            panic!("columnar {columnar}: {e}");
        }
    }
}

/// The shrink seed 9e19bfdc recorded for
/// [`chunk_order_reads_equal_the_default_order`] under `Str` values on names
/// the typed corpus no longer gives `Str`: the pick `(0, 0, ContentEquals)`
/// as the content arm it builds, `flag = "fail"`, probing POSTINGS on the
/// indexed `flag`, with a declared order naming no attribute the record
/// carries. Both writer paths.
#[test]
fn chunk_order_recorded_shrink_indexed_flag_content_equals() {
    for columnar in [false, true] {
        let rec = recorded_record(vec![
            ("flag", AttrValue::Str("fail".to_string())),
            ("dur", AttrValue::Str("timeout".to_string())),
        ]);
        let content = vec![Predicate::Equals {
            field: FieldSel::Attr("flag".to_string()),
            value: AttrValue::Str("fail".to_string()),
        }];
        let case = chunk_order_reads_from_arms(
            vec![rec],
            Predicate::And(Vec::new()),
            content,
            Vec::new(),
            &["host"],
            &["dur", "flag"],
            columnar,
        );
        if let Err(e) = check_chunk_order_reads(&case) {
            panic!("columnar {columnar}: {e}");
        }
    }
}

/// Seed and case count of [`chunk_order_prune_channel_prunes_partially`].
const COVERAGE_SEED: [u8; 32] = [7; 32];
const COVERAGE_CASES: usize = 200;
// measured 2026-10-28: 69 of 200 partial, 11 no arm and 24 empty with seed
// [7; 32]. The floor is two thirds of the partial count rounded down, each
// ceiling one and a half times its count rounded up. The counts follow
// proptest 1.11.0's RNG streams: re-measure them on a proptest bump.
const PARTIAL_FLOOR: usize = 46;
const NOARM_CEIL: usize = 17;
const EMPTY_CEIL: usize = 36;

/// Pins the coverage of [`chunk_order_reads_equal_the_default_order`]'s prune
/// comparison: run through the same generators on a fixed seed, enough cases
/// must remove part of the compared row set, and few may build no prune arm
/// or remove every row, since those compare nothing a wrong column id moves.
#[test]
fn chunk_order_prune_channel_prunes_partially() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &COVERAGE_SEED),
    );
    let strategy = arb_chunk_order_case();
    let mut effects = Vec::with_capacity(COVERAGE_CASES);
    for _ in 0..COVERAGE_CASES {
        let case = chunk_order_reads(
            strategy
                .new_tree(&mut runner)
                .expect("generate a case")
                .current(),
        );
        effects.push(prune_effect(&case.default_object, &case.pred, &case.prune));
    }
    let count = |e: PruneEffect| effects.iter().filter(|&&x| x == e).count();
    let summary = [
        PruneEffect::NoArm,
        PruneEffect::Nothing,
        PruneEffect::Partial,
        PruneEffect::Empty,
    ]
    .map(|e| (e, count(e)));
    assert!(
        count(PruneEffect::Partial) >= PARTIAL_FLOOR,
        "partial prunes below the floor of {PARTIAL_FLOOR}: {summary:?}"
    );
    assert!(
        count(PruneEffect::NoArm) <= NOARM_CEIL,
        "no-arm cases above the ceiling of {NOARM_CEIL}: {summary:?}"
    );
    assert!(
        count(PruneEffect::Empty) <= EMPTY_CEIL,
        "empty prunes above the ceiling of {EMPTY_CEIL}: {summary:?}"
    );
}

// --- page choice (ADR-3492 decision 1) -------------------------------------

/// `corpus` written under `cfg`, through the row path or through the columnar
/// path in ingest push batches of `ingest_shape::PUSH_BATCH`.
fn write_batched(cfg: RlogConfig, corpus: &[LogRecord], columnar: bool) -> Vec<u8> {
    let identity = ObjectIdentity {
        tenant_hash: [0u8; 16],
        shard: 0,
        writer_id: [0u8; 16],
        writer_epoch: 0,
        writer_seq: 0,
    };
    let mut w = RlogWriter::new(cfg, identity);
    if columnar {
        for batch in corpus.chunks(ingest_shape::PUSH_BATCH) {
            let batch = ColumnarLogBatch::try_from_records(batch).expect("batch");
            w.push_columnar(batch).expect("push_columnar");
        }
    } else {
        for r in corpus {
            w.push(r.clone()).expect("push");
        }
    }
    w.finish().expect("finish")
}

/// Every record an object holds, in the order it stores them, as comparable
/// keys.
fn decoded_keys(object: &[u8]) -> Vec<Vec<u8>> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(object, &cfg).expect("open");
    let all = Predicate::TsRange {
        min_ns: i64::MIN,
        max_ns: i64::MAX,
    };
    let (got, _stats) = reader.scan(&all).expect("scan");
    got.iter().map(record_key).collect()
}

/// One ingest flush of the benchmark shape, at the default layout and at
/// small blocks in row groups of four: under `Estimated` the same input
/// encodes to the same bytes twice, the row and columnar paths agree byte for
/// byte under each choice, and the `Estimated` object decodes to exactly the
/// records the `Exhaustive` one does.
#[test]
fn estimated_mode_is_deterministic() {
    let corpus = ingest_shape::ingest_records(7_500);
    let small = RlogConfig {
        block_target_records: 256,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    for base in [RlogConfig::default(), small] {
        let est = RlogConfig {
            page_choice: PageChoice::Estimated,
            ..base
        };
        let first = write_batched(est, &corpus, false);
        assert!(
            first == write_batched(est, &corpus, false),
            "Estimated is not repeatable"
        );
        assert!(
            first == write_batched(est, &corpus, true),
            "Estimated row and columnar differ"
        );
        let exh = write_batched(base, &corpus, false);
        assert!(
            exh == write_batched(base, &corpus, true),
            "Exhaustive row and columnar differ"
        );
        assert!(first != exh, "the writer ignored page_choice");
        assert_eq!(decoded_keys(&first), decoded_keys(&exh));
    }
}

/// One ingest flush of the benchmark shape through the columnar path, under
/// both page choices at the default layout and at small blocks, hashes to the
/// blake3 digests the writer produced before its integer-keyed maps moved to
/// foldhash (#3590). No map iteration order reaches the object bytes.
#[test]
fn ingest_shape_object_bytes_are_pinned() {
    let corpus = ingest_shape::ingest_records(7_500);
    let small = RlogConfig {
        block_target_records: 256,
        group_target_blocks: 4,
        ..RlogConfig::default()
    };
    let cases = [
        (
            "default/exhaustive",
            RlogConfig::default(),
            PageChoice::Exhaustive,
            GOLDEN_DEFAULT_EXHAUSTIVE,
        ),
        (
            "default/estimated",
            RlogConfig::default(),
            PageChoice::Estimated,
            GOLDEN_DEFAULT_ESTIMATED,
        ),
        (
            "small/exhaustive",
            small,
            PageChoice::Exhaustive,
            GOLDEN_SMALL_EXHAUSTIVE,
        ),
        (
            "small/estimated",
            small,
            PageChoice::Estimated,
            GOLDEN_SMALL_ESTIMATED,
        ),
    ];
    let mut wrong = Vec::new();
    for (name, base, choice, want) in cases {
        let cfg = RlogConfig {
            page_choice: choice,
            ..base
        };
        let got = blake3::hash(&write_batched(cfg, &corpus, true)).to_hex();
        if got.as_str() != want {
            wrong.push(format!("{name}: got {got}, want {want}"));
        }
    }
    assert!(wrong.is_empty(), "object digests moved: {wrong:#?}");
}

const GOLDEN_DEFAULT_EXHAUSTIVE: &str =
    "8690e1be20e17f2de12341b0d20cb99e80ba9199fa8033f12ba55bd2ba1011e9";
const GOLDEN_DEFAULT_ESTIMATED: &str =
    "e0b048f9442158d5e2aeada427fa8f8f8f69d3c9a10999ef1101e6d2b21de1a3";
const GOLDEN_SMALL_EXHAUSTIVE: &str =
    "d6522a9dd59c65ef4ccd298003fa660fb5570badf0bec4010a0093ead5f6854f";
const GOLDEN_SMALL_ESTIMATED: &str =
    "bad47f6c9f89a94aa1ed9f9ffbc03dcccbcff417efa55b8d924bd3caf28e5b31";

/// How one generated page column draws its values.
#[derive(Clone, Copy, Debug)]
enum PageKind {
    Constant,
    Monotonic,
    Random,
    SmallRange,
    LowCardStr,
    HighCardStr,
}

fn page_value(kind: PageKind, seed: u64, i: usize) -> AttrValue {
    let r = (seed ^ (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .wrapping_mul(0xbf58_476d_1ce4_e5b9)
        .rotate_left(29);
    match kind {
        PageKind::Constant => AttrValue::I64(seed as i64),
        PageKind::Monotonic => {
            AttrValue::I64((seed as i64 % 1_000_000) + i as i64 * 1_000 + (r % 3) as i64)
        }
        PageKind::Random => AttrValue::I64(r as i64),
        PageKind::SmallRange => AttrValue::I64((r % 16) as i64),
        PageKind::LowCardStr => AttrValue::Str(format!("v{}", r % 4)),
        PageKind::HighCardStr => AttrValue::Str(format!("{r:016x}")),
    }
}

/// A corpus whose attribute columns are each one [`PageKind`], present in
/// every record or in a seed-chosen share of them, with bodies of either
/// kind of string.
fn arb_page_corpus() -> impl Strategy<Value = (Vec<LogRecord>, usize)> {
    let kind = prop_oneof![
        Just(PageKind::Constant),
        Just(PageKind::Monotonic),
        Just(PageKind::Random),
        Just(PageKind::SmallRange),
        Just(PageKind::LowCardStr),
        Just(PageKind::HighCardStr),
    ];
    (
        1usize..400,
        proptest::collection::vec((kind, any::<u64>(), 0u8..4), 1..6),
        any::<bool>(),
        8usize..80,
    )
        .prop_map(|(n, columns, low_card_body, block)| {
            let corpus = (0..n)
                .map(|i| {
                    let attrs = columns
                        .iter()
                        .enumerate()
                        .filter(|(_, (_, _, sparse))| {
                            *sparse == 0 || i % (usize::from(*sparse) + 1) != 0
                        })
                        .map(|(c, &(kind, seed, _))| (format!("c{c}"), page_value(kind, seed, i)))
                        .collect();
                    let body = if low_card_body {
                        WORDS[i % 3].to_string()
                    } else {
                        format!("{} {i}", WORDS[i % WORDS.len()])
                    };
                    let stream = (i % usize::from(STREAMS)) as u8;
                    LogRecord {
                        attrs,
                        ..make_record(
                            stream,
                            (i / 3) as i64,
                            body,
                            (i % 30) as u8,
                            Vec::new(),
                            None,
                        )
                    }
                })
                .collect();
            (corpus, block)
        })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// Every `Estimated` object decodes to exactly the records the
    /// `Exhaustive` object of the same input decodes to, in the same order,
    /// and the row and columnar paths agree byte for byte under `Estimated`.
    #[test]
    fn estimated_objects_decode_identically((corpus, block) in arb_page_corpus()) {
        let exh = RlogConfig {
            block_target_records: block,
            group_target_blocks: 3,
            ..RlogConfig::default()
        };
        let est = RlogConfig { page_choice: PageChoice::Estimated, ..exh };
        let est_row = write_batched(est, &corpus, false);
        prop_assert!(est_row == write_batched(est, &corpus, true), "row and columnar differ");
        let want = decoded_keys(&write_batched(exh, &corpus, false));
        prop_assert_eq!(decoded_keys(&est_row), want.clone());
        let mut sorted = want;
        sorted.sort();
        prop_assert_eq!(sorted, normalize(corpus));
    }
}
