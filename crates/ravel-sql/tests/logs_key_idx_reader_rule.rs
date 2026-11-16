//! Issue #3039: an RLOG object's KEY_IDX keys and SKIP_IDX numeric stats for a
//! declared field are the values the SQL reader materializes for that field,
//! row by row.
//!
//! The reader resolves a declared column over each record's merged view
//! (`rlog_attrs::find_attr`): resource and scope entries holding a `Map` or
//! `List` are dropped, the first remaining entry wins with resource ahead of
//! scope, a record-level value replaces it, and a first entry of the wrong
//! type reads NULL and still shadows every later one. Each case below puts one
//! record in its own block (one stream per case), so "the block holds the
//! value" is "the row materializes the value", and the test asserts, for every
//! candidate key, that the KEY_IDX probe names exactly the blocks whose row
//! materializes it through a real `SqlExecutor`. For the `I64` field it also
//! asserts that each block's numeric stat covers the value its row
//! materializes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, BinaryArray, DictionaryArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::Int32Type;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::key_index::{
    self, Key8, KeyType, RLOG_KEY_IDX_VERSION, key8_bytes, key8_i64, key8_str,
};
use ravel_logseg::record::canonical_value_bytes;
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, FieldType, LogRecord, LogSegError, Predicate, RlogConfig, RlogReader, RlogWriter,
    read_section, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    StaticDeclaredColumns,
};
use ravel_types::logstream::canonical_attr_bytes;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

const FIELD: &str = "k";

type Attrs = Vec<(String, AttrValue)>;

/// One case: the resource, scope and record-level entries for [`FIELD`].
struct Case {
    what: &'static str,
    resource: Attrs,
    scope: Attrs,
    record: Attrs,
    /// Store `scope` in the order given rather than the canonical order.
    scope_as_given: bool,
}

/// The cases of issue #3039 for one key type. `scalar(n)` is a value of the
/// declared type, `wrong` a scalar of another type.
fn cases(scalar: impl Fn(i64) -> AttrValue, wrong: AttrValue) -> Vec<Case> {
    let k = |v: AttrValue| vec![(FIELD.to_string(), v)];
    let nested_list = AttrValue::List(vec![scalar(90)]);
    let nested_map = AttrValue::Map(vec![("inner".to_string(), scalar(91))]);
    let case = |what, resource, scope, record| Case {
        what,
        resource,
        scope,
        record,
        scope_as_given: false,
    };
    vec![
        case(
            "nested resource, then scope scalar",
            k(nested_list.clone()),
            k(scalar(1)),
            vec![],
        ),
        case(
            "nested scope after a resource scalar",
            k(scalar(2)),
            k(nested_map.clone()),
            vec![],
        ),
        case(
            "nested only",
            k(nested_map.clone()),
            k(nested_list.clone()),
            vec![],
        ),
        case("scalar only", vec![], k(scalar(4)), vec![]),
        case(
            "record-level over nested and scalar stream entries",
            k(nested_list.clone()),
            k(scalar(5)),
            k(scalar(55)),
        ),
        case(
            "wrong-type scalar first",
            k(wrong.clone()),
            k(scalar(6)),
            vec![],
        ),
        case(
            "nested, then a wrong-type scalar",
            k(nested_list.clone()),
            k(wrong.clone()),
            vec![],
        ),
        case(
            "record-level over a nested-only stream layer",
            k(nested_map.clone()),
            vec![],
            k(scalar(8)),
        ),
        case(
            "record-level nested over a stream scalar",
            vec![],
            k(scalar(9)),
            k(nested_list.clone()),
        ),
        case(
            "duplicate names within the resource",
            vec![
                (FIELD.to_string(), nested_list),
                (FIELD.to_string(), scalar(11)),
                (FIELD.to_string(), scalar(10)),
            ],
            k(scalar(13)),
            vec![],
        ),
        // The canonical blob sorts a name's scalar values ahead of its nested
        // ones, so only a blob stored in another order puts a nested scope
        // entry in front of a scope scalar. The writer accepts such a blob and
        // the reader resolves it in stored order.
        Case {
            scope_as_given: true,
            ..case(
                "duplicate names within the scope, nested stored first",
                vec![],
                vec![
                    (FIELD.to_string(), nested_map),
                    (FIELD.to_string(), scalar(12)),
                ],
                vec![],
            )
        },
        case(
            "resource and scope scalars of the declared type",
            k(scalar(14)),
            k(scalar(15)),
            vec![],
        ),
        case(
            "record-level wrong type over a stream scalar",
            vec![],
            k(scalar(16)),
            k(wrong),
        ),
    ]
}

/// The STREAM_DIR blob of `c`: canonical, or with the scope attributes in the
/// order the case gives them.
fn stream_blob(c: &Case, scope_name: &str) -> Vec<u8> {
    if !c.scope_as_given {
        return stream_attrs_bytes(&c.resource, scope_name, "1", &c.scope);
    }
    let mut blob = stream_attrs_bytes(&c.resource, scope_name, "1", &[]);
    assert_eq!(blob.pop(), Some(0), "an empty scope set is one zero count");
    assert!(c.scope.len() < 0x80);
    blob.push(c.scope.len() as u8);
    for pair in &c.scope {
        // One entry's encoding, less its set's one-byte count.
        blob.extend_from_slice(&canonical_attr_bytes(std::slice::from_ref(pair))[1..]);
    }
    blob
}

fn tenant(tag: &str) -> TenantId {
    TenantId::new(format!("key-idx-reader-rule-{tag}"))
}

/// Writes one record per case, each its own stream and block, publishes the
/// object, and returns its bytes.
async fn publish_cases(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    key_type: KeyType,
    cases: &[Case],
) -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut w =
        RlogWriter::new(cfg, identity).with_key_index_fields(vec![(FIELD.to_string(), key_type)]);
    for (i, c) in cases.iter().enumerate() {
        let scope_name = format!("scope{i}");
        let ts = i as i64 + 1;
        w.push(LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(
                &c.resource,
                &scope_name,
                "1",
                &c.scope,
            ),
            stream_attrs: stream_blob(c, &scope_name),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: c.what.into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: c.record.clone(),
        })
        .expect("push");
    }
    let bytes = w.finish().expect("finish");
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(3_039),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: cases.len() as u64,
        series_count: cases.len() as u64,
        min_event_ts_ns: 1,
        max_event_ts_ns: cases.len() as i64,
        min_ingest_ts_ns: 1,
        max_ingest_ts_ns: cases.len() as i64,
        segment_format_version: u32::from(footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(
            &data_key,
            bytes::Bytes::from(bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
    bytes
}

/// The value SQL materializes for [`FIELD`] in each row, ordered by `ts`, as
/// the KEY_IDX key of that value (`None` for NULL), plus the `I64` value
/// itself for the numeric-stat check.
async fn materialized(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &TenantId,
    ty: DeclaredType,
    rows: usize,
) -> Vec<Option<(Key8, Option<i64>)>> {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(vec![
        DeclaredColumn::new(FIELD, ty),
    ])));
    let outcome = executor
        .execute(
            tenant.hash(),
            &SqlRequest {
                sql: format!("SELECT {FIELD} FROM logs ORDER BY ts"),
                window: TimeRange {
                    start_ns: 0,
                    end_ns: 1_000_000,
                },
                min_tokens: Vec::new(),
                now_ns: 1_000_000,
                deadline: Duration::from_secs(30),
                row_window: false,
                max_rows: None,
                budgets: None,
            },
        )
        .await
        .expect("query executes");
    assert_eq!(outcome.output.num_rows(), rows);
    let mut out = Vec::new();
    for batch in outcome.output.batches() {
        let col = batch.column(0);
        for i in 0..batch.num_rows() {
            if col.is_null(i) {
                out.push(None);
                continue;
            }
            out.push(Some(match ty {
                DeclaredType::I64 => {
                    let v = col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("Int64")
                        .value(i);
                    (key8_i64(v), Some(v))
                }
                DeclaredType::Str => {
                    let dict = col
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .expect("Dictionary(Int32, Utf8)");
                    let values = dict
                        .values()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("Utf8 values");
                    (key8_str(values.value(dict.keys().value(i) as usize)), None)
                }
                DeclaredType::Bytes => {
                    let v = col
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .expect("Binary")
                        .value(i);
                    (key8_bytes(v), None)
                }
                DeclaredType::Bool => unreachable!("no Bool key type"),
            }));
        }
    }
    assert_eq!(out.len(), rows);
    out
}

/// The block of each row, indexed by `ts - 1`.
fn block_of_ts(bytes: &[u8], rows: usize) -> Vec<u32> {
    let cfg = RlogConfig::default();
    let ftr = footer::open(bytes).expect("open");
    let raw = read_section(bytes, ftr.section(kind::SKIP_IDX).expect("SKIP_IDX"), &cfg)
        .expect("skip section");
    let skip = SkipIndex::decode(&raw, 1 << 24).expect("decode skip");
    assert_eq!(skip.l0.len(), rows, "one block per case");
    assert!(skip.l0.iter().all(|e| e.record_count == 1));
    let (scanned, _) = RlogReader::new(bytes, &cfg)
        .expect("open")
        .scan(&Predicate::And(Vec::new()))
        .expect("scan");
    let mut out = vec![u32::MAX; rows];
    for (block, r) in scanned.iter().enumerate() {
        out[(r.ts_ns - 1) as usize] = block as u32;
    }
    assert!(out.iter().all(|&b| b != u32::MAX));
    out
}

fn probe(bytes: &[u8], key_type: KeyType, key8: &Key8) -> Option<Vec<u32>> {
    let ftr = footer::open(bytes).expect("open");
    key_index::probe_ranged(&ftr, FIELD, key_type, key8, |r| {
        Ok::<_, LogSegError>(&bytes[r.start as usize..r.end as usize])
    })
    .expect("KEY_IDX probe")
}

/// Asserts that every block whose row materializes an `I64` value carries a
/// numeric stat for [`FIELD`] whose range covers it.
fn assert_stats_cover(bytes: &[u8], blocks: &[u32], got: &[Option<(Key8, Option<i64>)>]) {
    let cfg = RlogConfig::default();
    let ftr = footer::open(bytes).expect("open");
    let raw = read_section(bytes, ftr.section(kind::SKIP_IDX).expect("SKIP_IDX"), &cfg)
        .expect("skip section");
    let skip = SkipIndex::decode(&raw, 1 << 24).expect("decode skip");
    let raw = read_section(
        bytes,
        ftr.section(kind::FIELD_DIR).expect("FIELD_DIR"),
        &cfg,
    )
    .expect("field dir section");
    let dir = FieldDir::decode(&raw, 1 << 20).expect("decode field dir");
    let column = dir.column(FIELD, FieldType::I64).map(|e| e.column_id);
    for (row, value) in got.iter().enumerate() {
        let Some((_, Some(v))) = value else { continue };
        let entry = &skip.l0[blocks[row] as usize];
        let stat = column
            .and_then(|id| entry.stats.iter().find(|s| s.column_id == id))
            .unwrap_or_else(|| panic!("row {row} reads {v} and its block has no stat"));
        let (min, max) = (stat.min_bits as i64, stat.max_bits as i64);
        assert!(
            min <= *v && *v <= max,
            "row {row} reads {v}, its block's stat is [{min}, {max}]"
        );
    }
}

async fn assert_keys_follow_the_reader(
    tag: &str,
    key_type: KeyType,
    ty: DeclaredType,
    scalar: impl Fn(i64) -> AttrValue,
    key_of: impl Fn(&AttrValue) -> Key8,
    wrong: AttrValue,
    expected: &[Option<AttrValue>],
) {
    let cases = cases(&scalar, wrong.clone());
    assert_eq!(cases.len(), expected.len());
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let t = tenant(tag);
    let bytes = publish_cases(store.as_ref(), &t, key_type, &cases).await;
    {
        let ftr = footer::open(&bytes).expect("open");
        let version = key_index::key_index_version(&ftr, |r| {
            Ok::<_, LogSegError>(&bytes[r.start as usize..r.end as usize])
        })
        .expect("version");
        assert_eq!(version, Some(RLOG_KEY_IDX_VERSION), "{tag}");
        assert!(key_index::section_resolves_like_the_reader(
            RLOG_KEY_IDX_VERSION
        ));
    }
    let got = materialized(Arc::clone(&store), &t, ty, cases.len()).await;
    // The premise: SQL resolves each case the way the rule says it does.
    for (i, (c, want)) in cases.iter().zip(expected).enumerate() {
        assert_eq!(
            got[i].map(|(k, _)| k),
            want.as_ref().map(&key_of),
            "{tag}: case {i} ({}): SQL materialized another value",
            c.what
        );
    }
    let blocks = block_of_ts(&bytes, cases.len());
    if key_type == KeyType::I64 {
        assert_stats_cover(&bytes, &blocks, &got);
    }

    // Every value a case carries anywhere, so a key the writer stamps for a
    // shadowed or dropped entry is a candidate too.
    let mut candidates: BTreeSet<Key8> = got.iter().flatten().map(|(k, _)| *k).collect();
    for n in [1, 2, 4, 5, 55, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 90, 91] {
        candidates.insert(key_of(&scalar(n)));
    }
    candidates.insert(key_of(&wrong));
    candidates.insert(key_of(&AttrValue::List(vec![scalar(90)])));
    candidates.insert(key_of(&AttrValue::Map(vec![(
        "inner".to_string(),
        scalar(91),
    )])));
    for key8 in candidates {
        let want: BTreeSet<u32> = got
            .iter()
            .enumerate()
            .filter(|(_, v)| v.is_some_and(|(k, _)| k == key8))
            .map(|(row, _)| blocks[row])
            .collect();
        let probed: BTreeSet<u32> = probe(&bytes, key_type, &key8)
            .expect("the probe answers for a declared field")
            .into_iter()
            .collect();
        let rows_of = |set: &BTreeSet<u32>| -> Vec<&str> {
            set.iter()
                .map(|b| cases[blocks.iter().position(|x| x == b).unwrap()].what)
                .collect()
        };
        assert_eq!(
            probed,
            want,
            "{tag}: key {key8:02x?}: probe names {:?}, SQL materializes it in {:?}",
            rows_of(&probed),
            rows_of(&want)
        );
    }
}

/// The resolved value of each case in [`cases`], in order.
fn expected(
    scalar: impl Fn(i64) -> AttrValue,
    record_nested: Option<AttrValue>,
) -> Vec<Option<AttrValue>> {
    vec![
        Some(scalar(1)),
        Some(scalar(2)),
        None,
        Some(scalar(4)),
        Some(scalar(55)),
        None,
        None,
        Some(scalar(8)),
        record_nested,
        Some(scalar(10)),
        Some(scalar(12)),
        Some(scalar(14)),
        None,
    ]
}

#[tokio::test]
async fn i64_keys_and_stats_follow_the_readers_merged_view() {
    let scalar = AttrValue::I64;
    assert_keys_follow_the_reader(
        "i64",
        KeyType::I64,
        DeclaredType::I64,
        scalar,
        |v| match v {
            AttrValue::I64(n) => key8_i64(*n),
            other => key8_bytes(&canonical_value_bytes(other)),
        },
        AttrValue::Str("wrong".to_string()),
        &expected(scalar, None),
    )
    .await;
}

#[tokio::test]
async fn str_keys_follow_the_readers_merged_view() {
    let scalar = |n: i64| AttrValue::Str(format!("v{n}"));
    assert_keys_follow_the_reader(
        "str",
        KeyType::Str,
        DeclaredType::Str,
        scalar,
        |v| match v {
            AttrValue::Str(s) => key8_str(s),
            other => key8_bytes(&canonical_value_bytes(other)),
        },
        AttrValue::I64(-1),
        &expected(scalar, None),
    )
    .await;
}

/// A record-level `List` reads as its canonical bytes under a `Bytes` column,
/// so that case resolves to a value here and to NULL under the other types.
#[tokio::test]
async fn bytes_keys_follow_the_readers_merged_view() {
    let scalar = |n: i64| AttrValue::Bytes(vec![n as u8, 0xB0]);
    let nested = AttrValue::List(vec![scalar(90)]);
    assert_keys_follow_the_reader(
        "bytes",
        KeyType::Bytes,
        DeclaredType::Bytes,
        scalar,
        |v| match v {
            AttrValue::Bytes(b) => key8_bytes(b),
            other => key8_bytes(&canonical_value_bytes(other)),
        },
        AttrValue::Str("wrong".to_string()),
        &expected(scalar, Some(nested)),
    )
    .await;
}
