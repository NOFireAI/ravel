//! Layer-1 scan-oracle differential gate for the `logs` SQL path (ADR-0033).
//!
//! This is the log-signal counterpart of the metrics layer-1 gate
//! (`tests/pipeline.rs`): an independent reference re-decodes the
//! same underlying RLOG object bytes and re-implements the relevant matching
//! logic completely separately from the code under test, then the gate asserts
//! the two agree by proptest over random corpora. Logs have no aggregation
//! semantics in v1 (ADR-0033, "scan-oracle only for v1"), so only layer 1 is
//! built; there is no layer-2 operator gate (`tests/differential.rs`) analogue.
//!
//! # What is independent, and what is deliberately shared
//!
//! The reference bypasses [`ravel_query::LogSegmentFetcher`] and
//! [`ravel_sql::LogsTableProvider`] entirely: it fetches every object straight
//! through [`ravel_logseg::RlogReader`] and iterates the decoded
//! [`ravel_logseg::LogRecord`]s directly.
//!
//! - **ts range** and **stream-attribute equality** are matched by *fresh* code
//!   in this file. The stream-attribute check independently decodes each
//!   record's canonical `stream_attrs` blob and merges it with the record's
//!   dynamic attributes under the record-wins precedence rule (ADR-0033
//!   amendment). It never calls `ravel_sql`'s private
//!   `decode_stream_attrs`/`merged_attrs`; sharing that code would defeat the
//!   oracle.
//! - **`has_word`** is *not* re-implemented independently. Its semantics ARE
//!   the thing the format layer's differential suite gates; here both
//!   the scan and the reference call the one genuinely shared semantic source,
//!   [`ravel_logseg::tokenizer::tokens`], per the ADR's `Inexact` discipline
//!   (reproducing tokenization by hand would drift from the frozen tokenizer
//!   and produce false gate failures unrelated to what this gate checks).
//!
//! # Attribute value scope
//!
//! Generated resource/scope/record attributes carry `Str` values only. The
//! nested-`Map`/`List` omission path in the merged `attrs` column is already
//! pinned by `logs_provider.rs`'s unit tests; this gate concentrates on the
//! merge-precedence rule and the stream-attribute/ts matching over scalar
//! attributes, where an independent oracle is faithful and unambiguous.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, FixedSizeBinaryArray, MapArray, StringArray, StructArray, TimestampNanosecondArray,
    UInt8Array, UInt32Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    Between, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, and,
};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use proptest::prelude::*;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::tokenizer::tokens;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, LogRecord, Predicate, RlogConfig, RlogReader, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{LogsTableProvider, has_word_udf};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

// Case count: matched to this crate's convention for differential suites, which
// both `tests/differential.rs` and `tests/pipeline.rs` fix at
// `ProptestConfig::with_cases(48)`. Each case writes several small RLOG objects,
// fetches them twice (reference reader and the real scan), and runs a full
// `SessionContext` query, so 48 keeps the suite in line with the existing
// differential gates' cost while covering a wide corpus/predicate space.
const CASES: u32 = 48;

// --- generation vocabularies ---

/// Resource attribute keys. Disjoint from `SCOPE_KEYS` so a merged map never has
/// a duplicate key from the resource/scope decode (keeping the reference's map
/// comparison unambiguous).
const RESOURCE_KEYS: &[&str] = &["service.name", "host", "region"];
/// Scope attribute keys, disjoint from `RESOURCE_KEYS`.
const SCOPE_KEYS: &[&str] = &["lib", "otel.scope"];
/// Record-only dynamic attribute keys (never resource/scope keys).
const RECORD_ONLY_KEYS: &[&str] = &["req.id", "user.id"];
/// Attribute values, a small pool so a stream-attribute equality drawn from the
/// corpus genuinely matches some streams and misses others.
const VALUE_VOCAB: &[&str] = &["api", "worker", "db", "edge"];
/// Body word vocabulary, small so `has_word` queries have real hits and misses.
const WORD_VOCAB: &[&str] = &[
    "timeout",
    "connection",
    "error",
    "request",
    "gateway",
    "retry",
    "ok",
    "fatal",
];
const SEVERITY_TEXT: &[&str] = &["INFO", "WARN", "ERROR", "DEBUG"];

/// ts band width per object: object `i`'s records live in `[i*BAND, i*BAND+SPREAD)`
/// so a ts-range predicate sometimes excludes a whole object before any GET.
const BAND: i64 = 1000;
const SPREAD: i64 = 500;

// ---------------------------------------------------------------------------
// Generated corpus
// ---------------------------------------------------------------------------

/// One stream's identifying resource + scope attributes (scope name/version are
/// fixed; streams differ by their attribute sets).
#[derive(Clone, Debug)]
struct StreamSpec {
    resource: Vec<(String, AttrValue)>,
    scope: Vec<(String, AttrValue)>,
}

/// One record's payload. `ts_off` is an offset within its object's band; the
/// absolute ts is `object_base + ts_off`.
#[derive(Clone, Debug)]
struct RecordSpec {
    ts_off: i64,
    obs_extra: i64,
    severity_num: u8,
    severity_text: String,
    body: String,
    trace_id: Option<[u8; 16]>,
    span_id: Option<[u8; 8]>,
    flags: u32,
    attrs: Vec<(String, AttrValue)>,
}

/// One RLOG object: several streams, each with several records.
#[derive(Clone, Debug)]
struct ObjSpec {
    streams: Vec<(StreamSpec, Vec<RecordSpec>)>,
}

/// A whole corpus plus the abstract predicate selectors resolved against it in
/// the test body (kept abstract so generation stays pure).
#[derive(Clone, Debug)]
struct Scenario {
    objects: Vec<ObjSpec>,
    selectors: Selectors,
}

/// Abstract predicate choices, resolved to a concrete query once the corpus is
/// materialized.
#[derive(Clone, Debug)]
struct Selectors {
    ts: TsChoice,
    use_stream_attr: bool,
    attr_pick: usize,
    use_word: bool,
    word_pick: usize,
}

/// A ts predicate shape over the raw domain. `lo`/`hi` are pre-sorted.
#[derive(Clone, Copy, Debug)]
enum TsChoice {
    None,
    Ge(i64),
    Lt(i64),
    GeLt(i64, i64),
    Between(i64, i64),
}

fn arb_value() -> impl Strategy<Value = AttrValue> {
    prop::sample::select(VALUE_VOCAB).prop_map(|s| AttrValue::Str(s.to_string()))
}

/// A set of `(key, Str)` attributes with distinct keys drawn from `keys`.
/// `subsequence` yields distinct elements, so no key repeats within a set.
fn arb_attr_set(keys: &'static [&'static str]) -> impl Strategy<Value = Vec<(String, AttrValue)>> {
    proptest::sample::subsequence(keys.to_vec(), 0..=keys.len())
        .prop_flat_map(|sel| {
            let n = sel.len();
            (Just(sel), prop::collection::vec(arb_value(), n))
        })
        .prop_map(|(sel, vals)| {
            sel.into_iter()
                .zip(vals)
                .map(|(k, v)| (k.to_string(), v))
                .collect()
        })
}

fn arb_stream() -> impl Strategy<Value = StreamSpec> {
    (arb_attr_set(RESOURCE_KEYS), arb_attr_set(SCOPE_KEYS))
        .prop_map(|(resource, scope)| StreamSpec { resource, scope })
}

fn arb_body() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(WORD_VOCAB), 1..4).prop_map(|words| words.join(" "))
}

fn arb_opt_bytes16() -> impl Strategy<Value = Option<[u8; 16]>> {
    prop::option::of(any::<u8>().prop_map(|b| [b; 16]))
}

fn arb_opt_bytes8() -> impl Strategy<Value = Option<[u8; 8]>> {
    prop::option::of(any::<u8>().prop_map(|b| [b; 8]))
}

/// Record dynamic attributes: keys drawn from resource + scope + record-only
/// vocabularies, so some collide with the stream's resource/scope attribute
/// names (exercising record-wins merge precedence) and some do not.
fn arb_record_attrs() -> impl Strategy<Value = Vec<(String, AttrValue)>> {
    let all: Vec<&'static str> = RESOURCE_KEYS
        .iter()
        .chain(SCOPE_KEYS)
        .chain(RECORD_ONLY_KEYS)
        .copied()
        .collect();
    proptest::sample::subsequence(
        all,
        0..=(RESOURCE_KEYS.len() + SCOPE_KEYS.len() + RECORD_ONLY_KEYS.len()),
    )
    .prop_flat_map(|sel| {
        let n = sel.len();
        (Just(sel), prop::collection::vec(arb_value(), n))
    })
    .prop_map(|(sel, vals)| {
        sel.into_iter()
            .zip(vals)
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    })
}

fn arb_record() -> impl Strategy<Value = RecordSpec> {
    (
        0i64..SPREAD,
        0i64..3,
        0u8..=24,
        prop::sample::select(SEVERITY_TEXT),
        arb_body(),
        arb_opt_bytes16(),
        arb_opt_bytes8(),
        any::<u32>(),
        arb_record_attrs(),
    )
        .prop_map(
            |(
                ts_off,
                obs_extra,
                severity_num,
                severity_text,
                body,
                trace_id,
                span_id,
                flags,
                attrs,
            )| RecordSpec {
                ts_off,
                obs_extra,
                severity_num,
                severity_text: severity_text.to_string(),
                body,
                trace_id,
                span_id,
                flags,
                attrs,
            },
        )
}

fn arb_object() -> impl Strategy<Value = ObjSpec> {
    prop::collection::vec(
        (arb_stream(), prop::collection::vec(arb_record(), 1..4)),
        1..3,
    )
    .prop_map(|streams| ObjSpec { streams })
}

fn arb_ts_choice() -> impl Strategy<Value = TsChoice> {
    let domain = 0i64..(3 * BAND + 100);
    (0u8..5, domain.clone(), domain).prop_map(|(shape, a, b)| {
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        match shape {
            0 => TsChoice::None,
            1 => TsChoice::Ge(lo),
            2 => TsChoice::Lt(hi),
            3 => TsChoice::GeLt(lo, hi),
            _ => TsChoice::Between(lo, hi),
        }
    })
}

fn arb_selectors() -> impl Strategy<Value = Selectors> {
    (
        arb_ts_choice(),
        any::<bool>(),
        0usize..64,
        any::<bool>(),
        0usize..WORD_VOCAB.len(),
    )
        .prop_map(
            |(ts, use_stream_attr, attr_pick, use_word, word_pick)| Selectors {
                ts,
                use_stream_attr,
                attr_pick,
                use_word,
                word_pick,
            },
        )
}

fn arb_scenario() -> impl Strategy<Value = Scenario> {
    (prop::collection::vec(arb_object(), 1..4), arb_selectors())
        .prop_map(|(objects, selectors)| Scenario { objects, selectors })
}

// ---------------------------------------------------------------------------
// Materializing the corpus into RLOG objects
// ---------------------------------------------------------------------------

fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

fn build_record(stream: &StreamSpec, rec: &RecordSpec, base: i64) -> LogRecord {
    let ts = base + rec.ts_off;
    LogRecord {
        stream_id: log_stream_id(&stream.resource, "scope", "1.0", &stream.scope),
        stream_attrs: stream_attrs_bytes(&stream.resource, "scope", "1.0", &stream.scope),
        ts_ns: ts,
        observed_ts_ns: ts + rec.obs_extra,
        severity_num: rec.severity_num,
        severity_text: rec.severity_text.clone(),
        body: rec.body.clone(),
        trace_id: rec.trace_id,
        span_id: rec.span_id,
        flags: rec.flags,
        attrs: rec.attrs.clone(),
    }
}

/// Write one object's records into `store` and return its `SegmentRef` carrying
/// the true ts span (the fields `LogsTableProvider` and the fetcher prune on).
async fn write_object(
    store: &MemoryStore,
    key: &str,
    records: &[LogRecord],
    seq: u64,
) -> SegmentRef {
    let identity = ObjectIdentity {
        // Matches the TenantHash([7u8; 16]) this test fetches as; the RLOG
        // read path enforces a footer tenant_hash check, so a footer naming a
        // different tenant than the fetch fails closed.
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    };
    let mut w = RlogWriter::new(small_blocks(), identity);
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(u128::from(seq)),
        writer_epoch: 1,
        writer_seq: seq,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// Materialize the whole corpus onto a fresh `MemoryStore`, returning the store
/// and the snapshot of object refs.
async fn build_corpus(objects: &[ObjSpec]) -> (Arc<MemoryStore>, Snapshot) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for (i, obj) in objects.iter().enumerate() {
        let base = i as i64 * BAND;
        let mut records = Vec::new();
        for (stream, recs) in &obj.streams {
            for rec in recs {
                records.push(build_record(stream, rec, base));
            }
        }
        let key = format!("logs/obj-{i}.rlog");
        segments.push(write_object(store.as_ref(), &key, &records, i as u64 + 1).await);
    }
    (
        store,
        Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
    )
}

// ---------------------------------------------------------------------------
// The concrete query, resolved from the abstract selectors + corpus
// ---------------------------------------------------------------------------

/// A resolved predicate combination (0-3 conjuncts, ANDed), the SQL subset this
/// epic supports (no OR/NOT over these predicate kinds).
#[derive(Clone, Debug)]
struct QuerySpec {
    ts: TsChoice,
    stream_attr: Option<(String, String)>,
    word: Option<String>,
}

impl QuerySpec {
    /// Resolve abstract selectors against the concrete corpus: the
    /// stream-attribute pick is drawn from the corpus's actual resource/scope
    /// pairs so it sometimes matches and sometimes does not.
    fn resolve(sel: &Selectors, objects: &[ObjSpec]) -> QuerySpec {
        let stream_attr = if sel.use_stream_attr {
            let pairs = corpus_stream_attr_pairs(objects);
            if pairs.is_empty() {
                None
            } else {
                Some(pairs[sel.attr_pick % pairs.len()].clone())
            }
        } else {
            None
        };
        let word = sel
            .use_word
            .then(|| WORD_VOCAB[sel.word_pick % WORD_VOCAB.len()].to_string());
        QuerySpec {
            ts: sel.ts,
            stream_attr,
            word,
        }
    }

    /// Build the combined filter expression driven through DataFusion, or `None`
    /// when no predicate was selected.
    fn to_filter(&self, get_field: &ScalarUDF, has_word: &ScalarUDF) -> Option<Expr> {
        let mut conjuncts: Vec<Expr> = Vec::new();
        match self.ts {
            TsChoice::None => {}
            TsChoice::Ge(lo) => conjuncts.push(col("ts").gt_eq(ts_lit(lo))),
            TsChoice::Lt(hi) => conjuncts.push(col("ts").lt(ts_lit(hi))),
            TsChoice::GeLt(lo, hi) => {
                conjuncts.push(col("ts").gt_eq(ts_lit(lo)));
                conjuncts.push(col("ts").lt(ts_lit(hi)));
            }
            TsChoice::Between(lo, hi) => conjuncts.push(Expr::Between(Between {
                expr: Box::new(col("ts")),
                negated: false,
                low: Box::new(ts_lit(lo)),
                high: Box::new(ts_lit(hi)),
            })),
        }
        if let Some((k, v)) = &self.stream_attr {
            conjuncts.push(
                get_field
                    .call(vec![col("attrs"), lit(k.clone())])
                    .eq(lit(v.clone())),
            );
        }
        if let Some(w) = &self.word {
            conjuncts.push(has_word.call(vec![col("body"), lit(w.clone())]));
        }
        conjuncts.into_iter().reduce(and)
    }

    /// The independent reference predicate for one decoded record.
    fn matches(&self, rec: &LogRecord) -> bool {
        let ts_ok = match self.ts {
            TsChoice::None => true,
            TsChoice::Ge(lo) => rec.ts_ns >= lo,
            TsChoice::Lt(hi) => rec.ts_ns < hi,
            TsChoice::GeLt(lo, hi) => rec.ts_ns >= lo && rec.ts_ns < hi,
            TsChoice::Between(lo, hi) => rec.ts_ns >= lo && rec.ts_ns <= hi,
        };
        if !ts_ok {
            return false;
        }
        if let Some((k, v)) = &self.stream_attr {
            // The scan emits every fetched record unconditionally; DataFusion's
            // `Inexact` residual re-applies the equality against the merged
            // `attrs` column (resource + scope + record attributes, record wins
            // on collision). That merged view alone decides the match -- the
            // table's true SQL semantics per the ADR-0033 amendment, not the raw
            // `stream_attrs` blob. A record matches iff its merged map resolves
            // k to v, whether the match comes from a resource/scope attribute or
            // from a per-record dynamic attribute.
            let merged = ref_merged_attrs(rec);
            if merged.get(k).map(String::as_str) != Some(v.as_str()) {
                return false;
            }
        }
        if let Some(w) = &self.word
            && !body_has_word(&rec.body, w)
        {
            return false;
        }
        true
    }
}

/// Every `(key, value)` resource/scope attribute pair present in the corpus, as
/// strings (values are always `Str`).
fn corpus_stream_attr_pairs(objects: &[ObjSpec]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for obj in objects {
        for (stream, _) in &obj.streams {
            for (k, v) in stream.resource.iter().chain(&stream.scope) {
                if let AttrValue::Str(s) = v {
                    let pair = (k.clone(), s.clone());
                    if !out.contains(&pair) {
                        out.push(pair);
                    }
                }
            }
        }
    }
    out
}

fn ts_lit(v: i64) -> Expr {
    lit(ScalarValue::TimestampNanosecond(Some(v), None))
}

// ---------------------------------------------------------------------------
// has_word: the shared semantic source (tokenizer), not re-implemented
// ---------------------------------------------------------------------------

/// `word` tokenizes to an in-order contiguous run present in the tokenized
/// `body`. Uses the frozen tokenizer both the scan (via the pushed `HasWord`
/// and the residual `has_word` UDF) and this reference share, so this is NOT an
/// independent re-implementation (ADR-0033: `has_word`'s own correctness is the
/// reader's job).
fn body_has_word(body: &str, word: &str) -> bool {
    let query = tokens(word);
    if query.is_empty() {
        return true;
    }
    let toks = tokens(body);
    toks.windows(query.len()).any(|w| w == query.as_slice())
}

// ---------------------------------------------------------------------------
// Independent canonical-attribute decoder (fresh code, no shared helper)
// ---------------------------------------------------------------------------
//
// Decodes the top-level scalar attributes of a `stream_attrs` blob and merges
// them with a record's dynamic attributes, record wins on collision. Written
// against the frozen canonical grammar (`ravel_types::logstream`,
// docs/log-segment-format.md); it never touches `ravel_sql`'s private
// `decode_stream_attrs`/`merged_attrs`.

/// The merged `attrs` map for a record: top-level resource/scope scalars overlaid
/// with dynamic per-record attributes (record wins on a key collision).
fn ref_merged_attrs(rec: &LogRecord) -> BTreeMap<String, String> {
    let mut map = ref_decode_stream_attrs(&rec.stream_attrs);
    for (k, v) in &rec.attrs {
        map.insert(k.clone(), ref_attr_to_string(v));
    }
    map
}

/// Top-level resource + scope scalar attributes of a blob, keyed by name. Nested
/// `Map`/`List` values are consumed but omitted (a documented v1 limitation of
/// the merged column). Panics (via `expect`, allowed in tests) on a malformed
/// blob, which would itself be a real defect worth surfacing loudly.
fn ref_decode_stream_attrs(blob: &[u8]) -> BTreeMap<String, String> {
    let mut pos = 0usize;
    let mut out = BTreeMap::new();
    ref_decode_attr_set(blob, &mut pos, &mut out); // resource set
    ref_skip_len_prefixed(blob, &mut pos); // scope name
    ref_skip_len_prefixed(blob, &mut pos); // scope version
    ref_decode_attr_set(blob, &mut pos, &mut out); // scope set
    out
}

fn ref_decode_attr_set(buf: &[u8], pos: &mut usize, out: &mut BTreeMap<String, String>) {
    let count = ref_uvarint(buf, pos);
    for _ in 0..count {
        let klen = ref_uvarint(buf, pos) as usize;
        let key = std::str::from_utf8(&buf[*pos..*pos + klen])
            .expect("key utf-8")
            .to_string();
        *pos += klen;
        if let Some(value) = ref_decode_value(buf, pos) {
            out.insert(key, value);
        }
    }
}

/// Decode one attribute value, returning its string form for a scalar or `None`
/// for a nested `List`/`Map` (consumed but omitted, mirroring the merged column).
fn ref_decode_value(buf: &[u8], pos: &mut usize) -> Option<String> {
    let tag = buf[*pos];
    *pos += 1;
    match tag {
        // Str
        1 => {
            let len = ref_uvarint(buf, pos) as usize;
            let s = std::str::from_utf8(&buf[*pos..*pos + len])
                .expect("str utf-8")
                .to_string();
            *pos += len;
            Some(s)
        }
        // I64 (zigzag varint)
        2 => Some(ref_unzigzag(ref_uvarint(buf, pos)).to_string()),
        // F64 (8 LE bytes of to_bits)
        3 => {
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[*pos..*pos + 8]);
            *pos += 8;
            Some(f64::from_bits(u64::from_le_bytes(b)).to_string())
        }
        // Bool
        4 => {
            let v = buf[*pos] != 0;
            *pos += 1;
            Some(v.to_string())
        }
        // Bytes
        5 => {
            let len = ref_uvarint(buf, pos) as usize;
            *pos += len;
            // Value form for a bytes attribute is not exercised by this gate's
            // Str-only corpus; consume and omit rather than guess its rendering.
            None
        }
        // List
        6 => {
            let n = ref_uvarint(buf, pos);
            for _ in 0..n {
                ref_skip_value(buf, pos);
            }
            None
        }
        // Map (nested attribute set)
        7 => {
            let n = ref_uvarint(buf, pos);
            for _ in 0..n {
                let klen = ref_uvarint(buf, pos) as usize;
                *pos += klen;
                ref_skip_value(buf, pos);
            }
            None
        }
        other => panic!("unexpected attr value tag {other}"),
    }
}

fn ref_skip_value(buf: &[u8], pos: &mut usize) {
    let tag = buf[*pos];
    *pos += 1;
    match tag {
        1 | 5 => {
            let len = ref_uvarint(buf, pos) as usize;
            *pos += len;
        }
        2 => {
            ref_uvarint(buf, pos);
        }
        3 => *pos += 8,
        4 => *pos += 1,
        6 => {
            let n = ref_uvarint(buf, pos);
            for _ in 0..n {
                ref_skip_value(buf, pos);
            }
        }
        7 => {
            let n = ref_uvarint(buf, pos);
            for _ in 0..n {
                let klen = ref_uvarint(buf, pos) as usize;
                *pos += klen;
                ref_skip_value(buf, pos);
            }
        }
        other => panic!("unexpected attr value tag {other}"),
    }
}

fn ref_skip_len_prefixed(buf: &[u8], pos: &mut usize) {
    let len = ref_uvarint(buf, pos) as usize;
    *pos += len;
}

fn ref_uvarint(buf: &[u8], pos: &mut usize) -> u64 {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = buf[*pos];
        *pos += 1;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

fn ref_unzigzag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// String form of a scalar attribute value for the merged map. The corpus uses
/// only `Str`; the other scalar arms exist so a generation change cannot make
/// the reference silently wrong.
fn ref_attr_to_string(v: &AttrValue) -> String {
    match v {
        AttrValue::Str(s) => s.clone(),
        AttrValue::I64(i) => i.to_string(),
        AttrValue::F64(f) => f.to_string(),
        AttrValue::Bool(b) => b.to_string(),
        other => panic!("unexpected record attr value {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The comparable row form
// ---------------------------------------------------------------------------

/// Every field of an emitted row, in a totally-ordered form so a multiset of
/// rows can be compared order-independently by sorting.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
    ts: i64,
    observed_ts: i64,
    severity_num: u8,
    severity_text: String,
    body: String,
    trace_id: Option<Vec<u8>>,
    span_id: Option<Vec<u8>>,
    flags: u32,
    attrs: Vec<(String, String)>,
}

fn row_from_record(rec: &LogRecord) -> Row {
    Row {
        ts: rec.ts_ns,
        observed_ts: rec.observed_ts_ns,
        severity_num: rec.severity_num,
        severity_text: rec.severity_text.clone(),
        body: rec.body.clone(),
        trace_id: rec.trace_id.map(|a| a.to_vec()),
        span_id: rec.span_id.map(|a| a.to_vec()),
        flags: rec.flags,
        attrs: ref_merged_attrs(rec).into_iter().collect(),
    }
}

/// Extract the `attrs` map contents of row `i` as sorted `(key, value)` pairs.
fn map_row(map: &MapArray, i: usize) -> Vec<(String, String)> {
    let entries = map.value(i);
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("map entries struct");
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("map keys utf8");
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("map values utf8");
    let mut out: Vec<(String, String)> = (0..keys.len())
        .map(|j| (keys.value(j).to_string(), vals.value(j).to_string()))
        .collect();
    out.sort();
    out
}

fn rows_from_batches(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<Row> {
    let mut out = Vec::new();
    for batch in batches {
        assert_eq!(
            batch.schema(),
            ravel_sql::logs_schema(),
            "public logs schema"
        );
        let ts = col_ts(batch, 0);
        let observed_ts = col_ts(batch, 1);
        let severity_num = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("severity_num");
        let severity_text = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("severity_text");
        let body = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body");
        let trace = batch
            .column(5)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("trace_id");
        let span = batch
            .column(6)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("span_id");
        let flags = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("flags");
        let attrs = batch
            .column(8)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("attrs map");
        for i in 0..batch.num_rows() {
            out.push(Row {
                ts: ts.value(i),
                observed_ts: observed_ts.value(i),
                severity_num: severity_num.value(i),
                severity_text: severity_text.value(i).to_string(),
                body: body.value(i).to_string(),
                trace_id: (!trace.is_null(i)).then(|| trace.value(i).to_vec()),
                span_id: (!span.is_null(i)).then(|| span.value(i).to_vec()),
                flags: flags.value(i),
                attrs: map_row(attrs, i),
            });
        }
    }
    out
}

fn col_ts(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    idx: usize,
) -> &TimestampNanosecondArray {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("timestamp column")
}

// ---------------------------------------------------------------------------
// The reference: fetch every object through RlogReader, apply the query
// ---------------------------------------------------------------------------

async fn reference_rows(store: &MemoryStore, snapshot: &Snapshot, query: &QuerySpec) -> Vec<Row> {
    let cfg = RlogConfig::default();
    let mut out = Vec::new();
    for seg in &snapshot.segments {
        let got = store
            .get(&seg.data_object_key, GetRange::Full)
            .await
            .expect("get object");
        let bytes = got.data;
        let reader = RlogReader::new(&bytes, &cfg).expect("open reader");
        // Match every record in the object, bypassing every prune the scan does.
        let (records, _stats) = reader
            .scan(&Predicate::TsRange {
                min_ns: i64::MIN,
                max_ns: i64::MAX,
            })
            .expect("scan");
        for rec in &records {
            if query.matches(rec) {
                out.push(row_from_record(rec));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The scan under test: a real SessionContext query
// ---------------------------------------------------------------------------

async fn scan_rows(
    store: Arc<dyn ObjectStoreBackend>,
    snapshot: Snapshot,
    query: &QuerySpec,
) -> Vec<Row> {
    let fetcher = LogSegmentFetcher::new(store);
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );

    let ctx = SessionContext::new();
    let get_field = ScalarUDF::from(GetField::new());
    let has_word = has_word_udf();
    ctx.register_udf(get_field.clone());
    ctx.register_udf(has_word.clone());
    ctx.register_table("logs", Arc::new(provider))
        .expect("register table");

    let df = ctx.table("logs").await.expect("table");
    let df = match query.to_filter(&get_field, &has_word) {
        Some(filter) => df.filter(filter).expect("filter"),
        None => df,
    };
    let plan = df.create_physical_plan().await.expect("physical plan");
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    rows_from_batches(&batches)
}

/// A row of the fixed-column projection (no `attrs`), so the columnar fast path
/// and the reference can be compared over the columns the fast path builds.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ProjRow {
    ts: i64,
    observed_ts: i64,
    severity_num: u8,
    severity_text: String,
    body: String,
    trace_id: Option<Vec<u8>>,
    span_id: Option<Vec<u8>>,
    flags: u32,
}

impl From<&Row> for ProjRow {
    fn from(r: &Row) -> Self {
        ProjRow {
            ts: r.ts,
            observed_ts: r.observed_ts,
            severity_num: r.severity_num,
            severity_text: r.severity_text.clone(),
            body: r.body.clone(),
            trace_id: r.trace_id.clone(),
            span_id: r.span_id.clone(),
            flags: r.flags,
        }
    }
}

/// The block counters a scan published, and its rows over the fixed-column
/// projection.
struct ProjRun {
    rows: Vec<ProjRow>,
    columnar_batches: usize,
    rowpath_batches: usize,
}

/// The `LogsScanExec` leaf of a physical plan, found by walking rather than by
/// shape (the optimizer inserts a residual `FilterExec` and possibly a
/// repartition above it).
fn find_logs_scan(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    if plan.name() == "LogsScanExec" {
        return Some(Arc::clone(plan));
    }
    plan.children().iter().find_map(|c| find_logs_scan(c))
}

/// Run the same query with a fixed-column projection (no `attrs`), so a query
/// with no attribute predicate takes the columnar fast path and one with an
/// attribute predicate (which folds `attrs` into the scan projection) takes the
/// row path. Returns the projected rows and both path metrics, so the gate can
/// assert which path ran as well as that the output is correct.
async fn scan_projected(
    store: Arc<dyn ObjectStoreBackend>,
    snapshot: Snapshot,
    query: &QuerySpec,
) -> ProjRun {
    let fetcher = LogSegmentFetcher::new(store);
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );

    let ctx = SessionContext::new();
    let get_field = ScalarUDF::from(GetField::new());
    let has_word = has_word_udf();
    ctx.register_udf(get_field.clone());
    ctx.register_udf(has_word.clone());
    ctx.register_table("logs", Arc::new(provider))
        .expect("register table");

    let df = ctx.table("logs").await.expect("table");
    let df = match query.to_filter(&get_field, &has_word) {
        Some(filter) => df.filter(filter).expect("filter"),
        None => df,
    };
    let df = df
        .select_columns(&[
            "ts",
            "observed_ts",
            "severity_num",
            "severity_text",
            "body",
            "trace_id",
            "span_id",
            "flags",
        ])
        .expect("project fixed columns");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let scan = find_logs_scan(&plan).expect("a LogsScanExec leaf");
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("collect");

    let metrics = scan.metrics().expect("scan metrics");
    let count = |name: &str| metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);

    let mut rows = Vec::new();
    for batch in &batches {
        let ts = col_ts(batch, 0);
        let observed_ts = col_ts(batch, 1);
        let severity_num = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("severity_num");
        let severity_text = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("severity_text");
        let body = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body");
        let trace = batch
            .column(5)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("trace_id");
        let span = batch
            .column(6)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("span_id");
        let flags = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("flags");
        for i in 0..batch.num_rows() {
            rows.push(ProjRow {
                ts: ts.value(i),
                observed_ts: observed_ts.value(i),
                severity_num: severity_num.value(i),
                severity_text: severity_text.value(i).to_string(),
                body: body.value(i).to_string(),
                trace_id: (!trace.is_null(i)).then(|| trace.value(i).to_vec()),
                span_id: (!span.is_null(i)).then(|| span.value(i).to_vec()),
                flags: flags.value(i),
            });
        }
    }
    ProjRun {
        rows,
        columnar_batches: count("columnar_batches"),
        rowpath_batches: count("rowpath_batches"),
    }
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// The epic's acceptance test: over random log corpora and
    /// random ts-range / stream-attribute / has_word predicate combinations, the
    /// `logs` scan's output equals an independent record-by-record reference by
    /// exact multiset equality, every field byte-for-byte.
    ///
    /// Both scan paths are exercised (ADR-0099). The full-schema query projects
    /// the `attrs` map, so it always takes the row path. A second run projects
    /// only the fixed columns: with no attribute predicate it takes the columnar
    /// fast path; with one, `attrs` folds back into the scan projection and it
    /// takes the row path. Either way its output must match the reference's fixed
    /// columns, and its published metric must name the path it took.
    #[test]
    fn scan_output_exactly_matches_reference_over_random_corpora_and_predicates(
        scenario in arb_scenario(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let (store, snapshot) = build_corpus(&scenario.objects).await;
            let query = QuerySpec::resolve(&scenario.selectors, &scenario.objects);

            let mut want = reference_rows(store.as_ref(), &snapshot, &query).await;
            let backend: Arc<dyn ObjectStoreBackend> = store;
            let mut got = scan_rows(Arc::clone(&backend), snapshot.clone(), &query).await;

            want.sort();
            got.sort();

            prop_assert_eq!(
                got.len(),
                want.len(),
                "row count differs for query {:?}: got {} want {}",
                query,
                got.len(),
                want.len()
            );
            prop_assert_eq!(&got, &want, "scan output must equal the reference exactly for query {:?}", query);

            // The fixed-column projection, exercising the columnar fast path
            // (and the row-path fallback when the query names an attribute).
            let proj = scan_projected(Arc::clone(&backend), snapshot.clone(), &query).await;
            let mut proj_got = proj.rows;
            let mut proj_want: Vec<ProjRow> = want.iter().map(ProjRow::from).collect();
            proj_got.sort();
            proj_want.sort();
            prop_assert_eq!(
                &proj_got,
                &proj_want,
                "projected scan output must equal the reference for query {:?}",
                query
            );

            // The projection carries no `attrs`, so the path is decided by
            // whether the query names an attribute (which folds `attrs` back into
            // the scan projection). An attribute predicate here is a stream-attr
            // equality, evaluated through the `get_field` UDF residual.
            if query.stream_attr.is_some() {
                prop_assert_eq!(
                    proj.columnar_batches, 0,
                    "an attribute predicate must fold attrs in and take the row path"
                );
            } else if !proj_got.is_empty() {
                prop_assert!(
                    proj.columnar_batches > 0,
                    "a fixed-only projection with no attribute predicate must take the fast path"
                );
                prop_assert_eq!(
                    proj.rowpath_batches, 0,
                    "the fast path must not fall back on this spill-free corpus"
                );
            }
            Ok(())
        })?;
    }
}

// ---------------------------------------------------------------------------
// A minimal `get_field(map, 'key') -> Utf8` UDF, so a stream-attribute equality
// drives the real supports_filters_pushdown -> scan -> residual FilterExec path
// (the subscript syntax `attrs['k']` does not plan on this crate's DataFusion
// build; see logs_provider.rs's identical helper and ADR-0033's subscript gap).
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct GetField {
    signature: Signature,
}

impl GetField {
    fn new() -> Self {
        GetField {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GetField {
    fn name(&self) -> &str {
        "get_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let key = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(k))) => k.clone(),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "get_field test udf: key must be a Utf8 literal, got {other:?}"
                )));
            }
        };
        let map_arr = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let map = map_arr
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("get_field test udf: first arg must be a Map column");
        let mut out = datafusion::arrow::array::StringBuilder::new();
        for i in 0..map.len() {
            if map.is_null(i) {
                out.append_null();
                continue;
            }
            let entries = map.value(i);
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("map entries struct");
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("map keys utf8");
            let vals = entries
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("map values utf8");
            let mut hit = None;
            for j in 0..keys.len() {
                if !keys.is_null(j) && keys.value(j) == key {
                    hit = Some(j);
                    break;
                }
            }
            match hit {
                Some(j) if !vals.is_null(j) => out.append_value(vals.value(j)),
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}
