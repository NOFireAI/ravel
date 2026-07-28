//! Keystone differential suite: a pruned scan returns exactly what an
//! independent naive in-memory filter returns, over arbitrary corpora and
//! predicates. `naive_eval` shares no storage code with the reader (only the
//! tokenizer, which defines word semantics), so agreement is real evidence the
//! pruning is both sound and precise-enough not to drop matches.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::{
    AttrValue, FieldSel, LogRecord, LogStreamId, ObjectIdentity, Predicate, RlogConfig, RlogReader,
    RlogWriter,
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
    let cfg = RlogConfig {
        block_target_records: 32,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: [0u8; 16],
        shard: 0,
        writer_id: [0u8; 16],
        writer_epoch: 0,
        writer_seq: 0,
    };
    let mut w = RlogWriter::new(cfg, identity);
    for r in corpus {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
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
}
