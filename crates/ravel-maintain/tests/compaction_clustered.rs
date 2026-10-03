//! RLOG compaction over clustered inputs (ADR-2135 decisions 2, 4 and 5,
//! docs/log-segment-format.md "Compaction (L0 → L1)"): the output descriptor,
//! generation and BLOOM scope come from the highest-generation input (the first
//! such input on a tie), the merge pushes each stream by
//! `(ts.div_euclid(W), input_index)` for the widest input bucket `W` across
//! part cuts as well as inside a part, a coarse bucket whose cursor
//! reservations pass the budget merges in input-order batches into the same
//! parts, and parts are written at `CompactorConfig::rlog_zstd_level`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;

use prost::Message;
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{
    self, SortBucketWidth, SortDescriptor, SortKeyColumn, SortKeyType, kind,
};
use ravel_logseg::record::{COL_BODY, COL_SEVERITY_TEXT};
use ravel_logseg::rlog_bloom::RlogBloomSection;
use ravel_logseg::{
    BloomScope, FieldType, LogRecord, ObjectIdentity, Predicate, RlogConfig, RlogReader,
    RlogWriter, read_section,
};
use ravel_maintain::config::AdmissionMode;
use ravel_maintain::{Bucket, CompactorConfig, FixedClock, MaintainError, compact_bucket};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_types::logstream::{AttrValue, LogStreamId, log_stream_id};
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

const TENANT: &str = "acme";
const SHARD: u32 = 7;
const INGEST_HOUR: u32 = 495_000;
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_DAY: i64 = 24 * NS_PER_HOUR;
/// The start of an event-time day, so `ts.div_euclid` of every width is aligned.
const T0: i64 = 20_000 * NS_PER_DAY;
const EPOCH: u64 = 10;
const OUTPUT_FORMAT_VERSION: u32 = ravel_maintain::rlog::OUTPUT_FORMAT_VERSION;

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn bucket() -> Bucket {
    Bucket::new(tenant_hash(), Signal::Logs, SHARD, INGEST_HOUR)
}

fn sealed_now_ns() -> i64 {
    (i64::from(INGEST_HOUR) + 3) * NS_PER_HOUR
}

fn stream_ident(n: u32) -> (LogStreamId, Vec<u8>) {
    let res = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc{n}")),
    )];
    let id = log_stream_id(&res, "scope", "1", &[]);
    let blob = ravel_logseg::stream_attrs_bytes(&res, "scope", "1", &[]);
    (id, blob)
}

fn s(v: &str) -> AttrValue {
    AttrValue::Str(v.to_string())
}

fn record(stream_n: u32, ts: i64, body: &str, attrs: Vec<(&str, AttrValue)>) -> LogRecord {
    let (stream_id, stream_attrs) = stream_ident(stream_n);
    LogRecord {
        stream_id,
        stream_attrs,
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    }
}

fn descriptor(width: SortBucketWidth, key: &str) -> SortDescriptor {
    SortDescriptor {
        bucket_width: width,
        key_columns: vec![SortKeyColumn {
            name: key.to_string(),
            ty: SortKeyType::Str,
        }],
    }
}

/// One L0 input. Its writer id is its position in canonical input order: the
/// data keys sort by writer id, so `Uuid::from_u128(1)` is input 0.
struct Input {
    records: Vec<LogRecord>,
    descriptor: Option<SortDescriptor>,
    generation: u64,
    scope: BloomScope,
}

impl Input {
    fn keyed(records: Vec<LogRecord>, d: SortDescriptor, generation: u64) -> Self {
        Input {
            records,
            descriptor: Some(d),
            generation,
            scope: BloomScope::All,
        }
    }

    fn unkeyed(records: Vec<LogRecord>) -> Self {
        Input {
            records,
            descriptor: None,
            generation: 0,
            scope: BloomScope::All,
        }
    }
}

/// Seeds `input` as L0 input `index` (data object plus commit record) and
/// returns the object bytes.
async fn seed(store: &dyn ObjectStoreBackend, index: usize, input: &Input) -> Vec<u8> {
    let th = tenant_hash();
    let writer_id = Uuid::from_u128(index as u128 + 1);
    let seq = index as u64 + 1;
    let identity = ObjectIdentity {
        tenant_hash: th.0,
        shard: SHARD,
        writer_id: writer_id.into_bytes(),
        writer_epoch: EPOCH,
        writer_seq: seq,
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity)
        .with_sort_descriptor(input.descriptor.clone(), input.generation)
        .with_bloom_scope(input.scope.clone());
    for r in &input.records {
        w.push(r.clone()).expect("push");
    }
    let bytes = bytes::Bytes::from(w.finish().expect("finish L0"));
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let data_key = keys::data_key(
        &th,
        Signal::Logs,
        SHARD,
        writer_id,
        EPOCH,
        seq,
        &content_hash,
    )
    .expect("data key");
    store
        .put(&data_key, bytes.clone(), PutOptions::default())
        .await
        .expect("put data");

    let ids: BTreeSet<LogStreamId> = input.records.iter().map(|r| r.stream_id).collect();
    let min_ts = input
        .records
        .iter()
        .map(|r| r.ts_ns)
        .min()
        .expect("records");
    let max_ts = input
        .records
        .iter()
        .map(|r| r.ts_ns)
        .max()
        .expect("records");
    let created = i64::from(INGEST_HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
    let rec = record::build(NewCommitRecord {
        tenant_hash: th,
        signal: Signal::Logs,
        shard: SHARD,
        writer_id,
        writer_epoch: EPOCH,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: input.records.len() as u64,
        series_count: ids.len() as u64,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: created,
        max_ingest_ts_ns: created,
        segment_format_version: OUTPUT_FORMAT_VERSION,
        created_unix_ns: created,
        ingest_hour_bucket: INGEST_HOUR,
    })
    .expect("build commit record");
    let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
    store
        .put(&commit_key, record::encode(&rec), PutOptions::default())
        .await
        .expect("put commit");
    bytes.to_vec()
}

async fn seed_all(store: &dyn ObjectStoreBackend, inputs: &[Input]) -> Vec<Vec<u8>> {
    let mut objects = Vec::new();
    for (i, input) in inputs.iter().enumerate() {
        objects.push(seed(store, i, input).await);
    }
    objects
}

/// The published compaction's parts, in part order.
async fn read_parts(store: &dyn ObjectStoreBackend) -> Vec<Vec<u8>> {
    let b = bucket();
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .unwrap();
    let record_key = list_all(store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .find(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .expect("compaction record");
    let record_bytes = store.get(&record_key, GetRange::Full).await.unwrap().data;
    let record = ravel_proto::commit::v1::CompactionRecord::decode(record_bytes.as_ref()).unwrap();
    let mut parts = Vec::new();
    for p in &record.parts {
        let key = keys::reconstruct_l1_part_key(&record, p).unwrap();
        parts.push(store.get(&key, GetRange::Full).await.unwrap().data.to_vec());
    }
    parts
}

/// Compacts `inputs` from a fresh store under `config` and returns the input
/// objects and the output parts.
async fn compact(
    inputs: &[Input],
    config: &CompactorConfig,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>), MaintainError> {
    let store = MemoryStore::new();
    let objects = seed_all(&store, inputs).await;
    compact_bucket(&store, &FixedClock::new(sealed_now_ns()), config, &bucket()).await?;
    let parts = read_parts(&store).await;
    Ok((objects, parts))
}

fn config_at(level: i32) -> CompactorConfig {
    CompactorConfig {
        rlog_zstd_level: level,
        ..CompactorConfig::default()
    }
}

fn scan(object: &[u8]) -> Vec<LogRecord> {
    let reader = RlogReader::new(object, &RlogConfig::default()).expect("reader");
    reader.scan(&Predicate::And(vec![])).expect("scan").0
}

fn hash_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// The descriptor and generation an object's footer records.
fn clustering(object: &[u8]) -> (Option<SortDescriptor>, u64) {
    let ftr = footer::open(object).expect("open footer");
    (ftr.sort_descriptor, ftr.clustering_generation)
}

/// The names of the columns an object's BLOOM covers, the two fixed text
/// columns spelled by their ids' roles.
fn covered_names(object: &[u8]) -> Vec<String> {
    let ftr = footer::open(object).expect("open footer");
    let cfg = RlogConfig::default();
    let dir_raw = read_section(
        object,
        ftr.section(kind::FIELD_DIR).expect("FIELD_DIR"),
        &cfg,
    )
    .expect("FIELD_DIR");
    let dir = FieldDir::decode(&dir_raw, u64::MAX).expect("decode FIELD_DIR");
    let bloom =
        read_section(object, ftr.section(kind::BLOOM).expect("BLOOM"), &cfg).expect("BLOOM");
    let section = RlogBloomSection::parse(&bloom, &dir).expect("parse BLOOM");
    section
        .covered()
        .iter()
        .map(|&id| match id {
            COL_SEVERITY_TEXT => "severity_text".to_string(),
            COL_BODY => "body".to_string(),
            _ => {
                let e = dir
                    .entries()
                    .iter()
                    .find(|e| e.column_id == id)
                    .expect("covered id names a column");
                assert_eq!(e.ty, FieldType::Str, "only string columns are covered");
                e.name.clone()
            }
        })
        .collect()
}

/// The reference part for one merge: per stream in id order, each input's
/// records for it in stored order, concatenated in input order and stably
/// sorted by `ts.div_euclid(width_ns)`, pushed through one writer with the
/// output's descriptor at the compactor's level and identity.
fn reference_part(
    objects: &[Vec<u8>],
    width_ns: i64,
    out: (Option<SortDescriptor>, u64),
    actual: &[u8],
    config: &CompactorConfig,
) -> Vec<u8> {
    let per_input: Vec<Vec<LogRecord>> = objects.iter().map(|o| scan(o)).collect();
    let ids: BTreeSet<LogStreamId> = per_input.iter().flatten().map(|r| r.stream_id).collect();
    let b = bucket();
    let identity = ObjectIdentity {
        tenant_hash: b.tenant_hash.0,
        shard: b.shard,
        writer_id: config.compactor_writer_id.into_bytes(),
        writer_epoch: 0,
        writer_seq: 0,
    };
    let writer_config = RlogConfig {
        zstd_level: config.rlog_zstd_level,
        ..RlogConfig::default()
    };
    let mut writer = RlogWriter::new(writer_config, identity).with_sort_descriptor(out.0, out.1);
    for id in &ids {
        let mut recs: Vec<LogRecord> = per_input
            .iter()
            .flat_map(|recs| recs.iter().filter(|r| &r.stream_id == id).cloned())
            .collect();
        recs.sort_by_key(|r| r.ts_ns.div_euclid(width_ns));
        for r in recs {
            writer.push(r).expect("push");
        }
    }
    let input_set_hash = footer::open(actual).expect("open").input_set_hash;
    writer
        .finish_compacted(1, input_set_hash, 0)
        .expect("finish reference")
}

// ---------------------------------------------------------------------------
// Output descriptor
// ---------------------------------------------------------------------------

fn few_records(tag: &str) -> Vec<LogRecord> {
    (0..6)
        .map(|i| {
            record(
                0,
                T0 + i * 5 * NS_PER_HOUR,
                &format!("{tag}-{i}"),
                vec![("k", s(["a", "b", "c"][(i % 3) as usize]))],
            )
        })
        .collect()
}

async fn output_clustering(inputs: Vec<Input>) -> (Option<SortDescriptor>, u64) {
    let (_objects, parts) = compact(&inputs, &CompactorConfig::default())
        .await
        .expect("compact");
    assert_eq!(parts.len(), 1);
    clustering(&parts[0])
}

/// The output takes the descriptor and generation of the highest-generation
/// input: not the first input's (SixHours on `c`), not the widest bucket's
/// (OneDay on `a`). With no keyed input it is no descriptor at the highest
/// generation, and a cleared key (no descriptor, generation 5) outranks a
/// key at generation 3.
#[tokio::test]
async fn compaction_keeps_the_highest_generation_descriptor() {
    let out = output_clustering(vec![
        Input::keyed(
            few_records("x"),
            descriptor(SortBucketWidth::SixHours, "c"),
            2,
        ),
        Input::keyed(
            few_records("y"),
            descriptor(SortBucketWidth::OneHour, "b"),
            3,
        ),
        Input::keyed(
            few_records("z"),
            descriptor(SortBucketWidth::OneDay, "a"),
            1,
        ),
    ])
    .await;
    assert_eq!(
        out,
        (Some(descriptor(SortBucketWidth::OneHour, "b")), 3),
        "the generation-3 input's descriptor"
    );

    let cleared = |records, generation| Input {
        records,
        descriptor: None,
        generation,
        scope: BloomScope::All,
    };
    let out = output_clustering(vec![
        Input::keyed(
            few_records("x"),
            descriptor(SortBucketWidth::OneDay, "a"),
            3,
        ),
        cleared(few_records("y"), 5),
    ])
    .await;
    assert_eq!(out, (None, 5), "a cleared key outranks an older key");

    let out = output_clustering(vec![
        cleared(few_records("x"), 0),
        cleared(few_records("y"), 4),
    ])
    .await;
    assert_eq!(
        out,
        (None, 4),
        "no keyed input: no descriptor, max generation"
    );

    let out = output_clustering(vec![
        Input::unkeyed(few_records("x")),
        Input::unkeyed(few_records("y")),
    ])
    .await;
    assert_eq!(
        out,
        (None, 0),
        "unkeyed inputs: no descriptor, generation 0"
    );
}

// ---------------------------------------------------------------------------
// Merge order
// ---------------------------------------------------------------------------

const KEYS: [&str; 3] = ["a", "b", "c"];

/// Two streams over two days, four hours a day, two records an hour. Every
/// input built from this shares each record's `(stream, ts, k)` and differs
/// only in the body's tag, so every record ties across inputs on the output's
/// `(bucket, k, ts)` and the order the merge pushes ties in is visible in the
/// bytes. The key falls as the hour rises (`c` at 01:00, `a` at 13:00), so a
/// day-bucket input's stored order runs against time inside a day.
fn interleaved_records(tag: &str) -> Vec<LogRecord> {
    let mut out = Vec::new();
    for day in 0..2i64 {
        for (slot, hour) in [1i64, 7, 13, 20].into_iter().enumerate() {
            for stream in 0..2u32 {
                for j in 0..2usize {
                    let ts = T0
                        + day * NS_PER_DAY
                        + hour * NS_PER_HOUR
                        + i64::from(stream) * 1_000
                        + j as i64;
                    let k = KEYS[(2 + 3 * 3 - slot + j + day as usize) % 3];
                    out.push(record(
                        stream,
                        ts,
                        &format!("{tag}-s{stream}-d{day}-h{hour}-{j}"),
                        vec![("k", s(k))],
                    ));
                }
            }
        }
    }
    out
}

fn order_inputs() -> Vec<Input> {
    vec![
        Input::keyed(
            interleaved_records("x"),
            descriptor(SortBucketWidth::OneDay, "k"),
            2,
        ),
        Input::keyed(
            interleaved_records("y"),
            descriptor(SortBucketWidth::SixHours, "k"),
            1,
        ),
    ]
}

/// The pinned hash of the merged part [`order_inputs`] compacts to.
const ORDER_PART_HASH: &str = "e178d48641c5087dc35f5ac745cd2fc6327170c7518d17df87d5510ce528d003";

/// Each stream merges by `(ts.div_euclid(1 day), input_index)`, the widest
/// input bucket, in both admission modes, and the part equals the reference
/// written through one writer. Pushing heads by raw ts, or by the narrowest
/// bucket (six hours), pushes input 1's `(c, 01:00)` before input 0's tie,
/// whose day-bucket stored order starts at 13:00.
#[tokio::test]
async fn merge_order_is_bucket_then_input_index() {
    let inputs = order_inputs();
    let mut outputs = Vec::new();
    for mode in [AdmissionMode::Overlap, AdmissionMode::EagerAll] {
        let config = CompactorConfig {
            merge_admission: mode,
            ..CompactorConfig::default()
        };
        let (objects, mut parts) = compact(&inputs, &config).await.expect("compact");
        assert_eq!(parts.len(), 1, "{mode:?}: one part");
        let out = (Some(descriptor(SortBucketWidth::OneDay, "k")), 2);
        assert_eq!(clustering(&parts[0]), out);
        let reference = reference_part(&objects, NS_PER_DAY, out, &parts[0], &config);
        assert!(
            reference == parts[0],
            "{mode:?}: the part is the (day bucket, input index) reference"
        );
        outputs.push(parts.remove(0));
    }
    assert!(
        outputs[0] == outputs[1],
        "both admission modes write the same bytes"
    );
    assert_eq!(hash_hex(&outputs[0]), ORDER_PART_HASH);

    // Ties really are pushed in input order: each `(stream, ts)` appears once
    // per input, input 0's (`x`) first.
    let bodies: Vec<String> = scan(&outputs[0]).into_iter().map(|r| r.body).collect();
    assert_eq!(bodies.len(), 64);
    for pair in bodies.chunks(2) {
        assert!(pair[0].starts_with("x-"), "{pair:?}");
        assert_eq!(pair[0][1..], pair[1][1..], "{pair:?}");
    }
}

/// The pinned part hashes [`order_inputs`] compacts to under a memory target
/// small enough to cut each stream into several parts.
const ORDER_MULTI_PART_HASHES: &[&str] = &[
    "2b27d5771d9e59c556ad175ce21360b838d8a7fa02c950b501060250a30bf4a2",
    "b3101deba268f08a8e8f2ed8a85fa936fe55a1eb3eafdf6a8a64d2daf2fff820",
    "cbf8ce717079e18d7ffa00e932a8999d65ab4f905d72ca15bd77db9f3ced0400",
    "4851b9df83d7f2dbf629c03804bf7e23ea6188ee2f8c9384372f65a6274857f3",
    "256ccff6107deabaefcf1fa29574909fcc61192848e1405d3a7f73de9ef3d63c",
    "31f761cc74fd46e0cd2744a817c193f185594c82fbdbf097cf4b9ced732594b4",
];

/// With the output cut into several parts, which records land in which part
/// follows the push order, so the parts' bytes pin `(day bucket,
/// input_index)` across cuts, not only inside one part.
///
/// Demonstrated red against: concatenating each stream's inputs in input
/// order (merge width `i64::MAX`), which the one-part test above passes,
/// since the writer re-sorts a part and ties keep input order either way;
/// pushing by raw ts (width 1, the order before ADR-2135); and pushing by the
/// narrowest input bucket (six hours).
#[tokio::test]
async fn merge_order_holds_across_part_cuts() {
    let config = CompactorConfig {
        l1_part_memory_target_bytes: 4 * 1024,
        ..CompactorConfig::default()
    };
    let (_objects, parts) = compact(&order_inputs(), &config).await.expect("compact");
    let hashes: Vec<String> = parts.iter().map(|p| hash_hex(p)).collect();
    assert_eq!(hashes, ORDER_MULTI_PART_HASHES);
    for p in &parts {
        assert_eq!(
            clustering(p),
            (Some(descriptor(SortBucketWidth::OneDay, "k")), 2)
        );
    }
    let records: usize = parts.iter().map(|p| scan(p).len()).sum();
    assert_eq!(records, 64);
}

// ---------------------------------------------------------------------------
// BLOOM coverage
// ---------------------------------------------------------------------------

/// Records with three string attributes, `k`, `note` and `region`.
fn string_records(tag: &str) -> Vec<LogRecord> {
    (0..4)
        .map(|i| {
            record(
                0,
                T0 + i * NS_PER_HOUR,
                &format!("{tag} request {i}"),
                vec![
                    ("k", s(KEYS[(i % 3) as usize])),
                    ("note", s(&format!("needle{i}"))),
                    ("region", s("westcoast")),
                ],
            )
        })
        .collect()
}

fn scoped(tag: &str, scope: BloomScope, generation: u64) -> Input {
    Input {
        records: string_records(tag),
        descriptor: Some(descriptor(SortBucketWidth::OneHour, "k")),
        generation,
        scope,
    }
}

async fn output_coverage(inputs: Vec<Input>) -> Vec<String> {
    let (_objects, parts) = compact(&inputs, &CompactorConfig::default())
        .await
        .expect("compact");
    assert_eq!(parts.len(), 1);
    covered_names(&parts[0])
}

/// The output covers what the highest-generation input covers, mapped by
/// column name: `text` when that input covers only body and severity text,
/// `undeclared` over the string columns it left uncovered, and `all` when it
/// covers every string column. Never the union of the inputs' coverages, and
/// never the first input's.
#[tokio::test]
async fn coverage_follows_the_highest_generation_input() {
    let all = ["severity_text", "body", "k", "note", "region"];

    // Text at generation 2 over All at generation 1: the union would be All,
    // and so would the first input's coverage.
    let covered = output_coverage(vec![
        scoped("x", BloomScope::All, 1),
        scoped("y", BloomScope::Text, 2),
    ])
    .await;
    assert_eq!(covered, ["severity_text", "body"]);

    // Undeclared over `region` at generation 3 among Text and All inputs: the
    // union is All, the first input's is Text, the intersection is Text.
    let covered = output_coverage(vec![
        scoped("x", BloomScope::Text, 1),
        scoped(
            "y",
            BloomScope::Undeclared {
                declared: vec!["region".to_string()],
            },
            3,
        ),
        scoped("z", BloomScope::All, 2),
    ])
    .await;
    assert_eq!(covered, ["severity_text", "body", "k", "note"]);

    // All at generation 2 over Text at generation 1: the intersection and the
    // first input's coverage would both be Text.
    let covered = output_coverage(vec![
        scoped("x", BloomScope::Text, 1),
        scoped("y", BloomScope::All, 2),
    ])
    .await;
    assert_eq!(covered, all);
}

/// Hour-keyed records carrying exactly the string attributes `names`.
fn carrying(tag: &str, names: &[&str], scope: BloomScope, generation: u64) -> Input {
    let records = (0..4)
        .map(|i| {
            record(
                0,
                T0 + i * NS_PER_HOUR,
                &format!("{tag} request {i}"),
                names
                    .iter()
                    .map(|&n| (n, s(&format!("{n}-{tag}-{i}"))))
                    .collect(),
            )
        })
        .collect();
    Input {
        records,
        descriptor: Some(descriptor(SortBucketWidth::OneHour, "k")),
        generation,
        scope,
    }
}

fn undeclared(names: &[&str]) -> BloomScope {
    BloomScope::Undeclared {
        declared: names.iter().map(|n| n.to_string()).collect(),
    }
}

/// When the inputs carry different string columns, the chosen input's
/// coverage maps to a scope, not to a name list: a column only another input
/// carries is covered, except when the chosen input covers none of the string
/// columns it carries (`text`). Output order is column-id order.
///
/// Demonstrated red, at the case named, against: covering exactly the chosen
/// input's covered names (case 1 drops `region`); the union of every input's
/// coverage (case 2 drops `region`); the intersection (case 1 drops
/// `region`); mapping a chosen input that covers none of its string columns
/// to `undeclared` over them rather than to `text` (case 3 adds `region`);
/// and an output that is always `all`, as before ADR-2135 (case 1 adds
/// `note`).
#[tokio::test]
async fn coverage_maps_the_chosen_scope_over_differing_columns() {
    // 1. The chosen input covers `k` and leaves `note` uncovered: undeclared
    //    over `note`, so `region`, which only input 0 carries, is covered.
    let covered = output_coverage(vec![
        carrying("x", &["k", "region"], BloomScope::All, 1),
        carrying("y", &["k", "note"], undeclared(&["note"]), 2),
    ])
    .await;
    assert_eq!(covered, ["severity_text", "body", "k", "region"]);

    // 2. The chosen input covers every string column it carries (its declared
    //    `region` is a column it does not carry): all.
    let covered = output_coverage(vec![
        carrying("x", &["k", "region"], BloomScope::Text, 1),
        carrying("y", &["k", "note"], undeclared(&["region"]), 2),
    ])
    .await;
    assert_eq!(covered, ["severity_text", "body", "k", "note", "region"]);

    // 3. The chosen input covers none of the string columns it carries: text.
    let covered = output_coverage(vec![
        carrying("x", &["k", "region"], BloomScope::All, 1),
        carrying("y", &["k", "note"], undeclared(&["k", "note"]), 2),
    ])
    .await;
    assert_eq!(covered, ["severity_text", "body"]);

    // 4. The chosen input carries no string column: all, over the columns the
    //    other input carries.
    let covered = output_coverage(vec![
        carrying("x", &["k", "region"], BloomScope::Text, 1),
        carrying("y", &[], BloomScope::Text, 2),
    ])
    .await;
    assert_eq!(covered, ["severity_text", "body", "k", "region"]);
}

/// At a generation tie the first input in canonical order is the chosen one:
/// two generation-0 unkeyed inputs, one `text` and one `all`, write the
/// coverage of whichever is input 0.
///
/// Demonstrated red against the last tied input winning (`>=` flipped to `>`
/// in `OutputClustering::from_inputs`), the union of the two (`all` in the
/// first case), the intersection (`text` in the second), and an output that
/// is always `all` (the first case).
#[tokio::test]
async fn a_generation_tie_takes_the_first_inputs_coverage() {
    let unkeyed = |tag: &str, scope: BloomScope| Input {
        descriptor: None,
        generation: 0,
        ..carrying(tag, &["k", "note", "region"], scope, 0)
    };
    let covered = output_coverage(vec![
        unkeyed("x", BloomScope::Text),
        unkeyed("y", BloomScope::All),
    ])
    .await;
    assert_eq!(covered, ["severity_text", "body"], "input 0 is text");

    let covered = output_coverage(vec![
        unkeyed("x", BloomScope::All),
        unkeyed("y", BloomScope::Text),
    ])
    .await;
    assert_eq!(
        covered,
        ["severity_text", "body", "k", "note", "region"],
        "input 0 is all"
    );
}

// ---------------------------------------------------------------------------
// Reservation batches
// ---------------------------------------------------------------------------

/// Three inputs of one stream inside one day, all keyed on that day, so their
/// envelopes share a coarse bucket and the bucket's reservation is the sum of
/// all three.
fn batch_inputs() -> Vec<Input> {
    (0..3)
        .map(|n| {
            let records = (0..48i64)
                .map(|i| {
                    record(
                        0,
                        T0 + i * 1_800_000_000_000 + n,
                        &format!("in{n} event {i:03} payload {}", "z".repeat(40)),
                        vec![("k", s(KEYS[((i + n) % 3) as usize]))],
                    )
                })
                .collect();
            Input::keyed(records, descriptor(SortBucketWidth::OneDay, "k"), 1)
        })
        .collect()
}

/// The largest single input's cursor reservation in [`batch_inputs`]: the
/// smallest budget a batched merge of them admits. Measured, and pinned by
/// the refusal one byte below it.
const LARGEST_INPUT_RESERVATION: u64 = 28_871;

/// A coarse bucket whose summed reservations pass the budget merges in
/// input-order batches into the same part: at a budget of the largest single
/// input's reservation, a third of the bucket's sum, the merge succeeds in
/// both admission modes and writes the bytes of the one-pass merge. One byte
/// lower, a single input is over the budget and the merge aborts with
/// `MergeCursorBudgetExceeded`, as an unbatched merge does.
#[tokio::test]
async fn batching_partitions_by_coarse_bucket_and_input_order() {
    let inputs = batch_inputs();
    let (_objects, one_pass) = compact(&inputs, &CompactorConfig::default())
        .await
        .expect("one-pass merge");
    assert_eq!(one_pass.len(), 1, "one part");

    for mode in [AdmissionMode::Overlap, AdmissionMode::EagerAll] {
        let config = CompactorConfig {
            merge_cursor_budget_bytes: LARGEST_INPUT_RESERVATION,
            merge_admission: mode,
            ..CompactorConfig::default()
        };
        let (_objects, batched) = compact(&inputs, &config)
            .await
            .unwrap_or_else(|e| panic!("{mode:?}: batched merge: {e}"));
        assert_eq!(batched.len(), 1, "{mode:?}: batches share one part");
        assert!(
            batched[0] == one_pass[0],
            "{mode:?}: batched bytes equal the one-pass merge"
        );

        let config = CompactorConfig {
            merge_cursor_budget_bytes: LARGEST_INPUT_RESERVATION - 1,
            merge_admission: mode,
            ..CompactorConfig::default()
        };
        let err = compact(&inputs, &config)
            .await
            .expect_err("one input over the budget aborts");
        match err {
            MaintainError::MergeCursorBudgetExceeded {
                open_cursors,
                budget_bytes,
                required_bytes,
                ..
            } => {
                assert_eq!(open_cursors, 0, "{mode:?}: refused at admission");
                assert_eq!(budget_bytes, LARGEST_INPUT_RESERVATION - 1);
                assert_eq!(required_bytes, LARGEST_INPUT_RESERVATION);
            }
            other => panic!("{mode:?}: expected MergeCursorBudgetExceeded, got {other}"),
        }
    }
}

// ---------------------------------------------------------------------------
// zstd level
// ---------------------------------------------------------------------------

/// Three unkeyed inputs of repetitive request lines, enough for the zstd level
/// to move the stored size.
fn level_inputs() -> Vec<Input> {
    (0..3)
        .map(|n| {
            let records = (0..1500i64)
                .map(|i| {
                    record(
                        (i % 2) as u32,
                        T0 + i * 1_000_000 + n,
                        &format!(
                            "GET /api/v1/items/{} status={} user=u{} took={}ms",
                            i % 97,
                            [200, 404, 500][(i % 3) as usize],
                            i % 13,
                            (i * 7) % 250
                        ),
                        vec![("route", s(&format!("/items/{}", i % 11)))],
                    )
                })
                .collect();
            Input::unkeyed(records)
        })
        .collect()
}

const LEVEL_3_PART_BYTES: usize = 17_701;
const LEVEL_9_PART_BYTES: usize = 13_972;

/// `CompactorConfig::rlog_zstd_level` reaches the part writer: level 9 stores
/// fewer bytes than level 3, both sizes pinned, and the two parts read back to
/// the same records.
#[tokio::test]
async fn compaction_writes_at_level_nine() {
    let inputs = level_inputs();
    let (_objects, at3) = compact(&inputs, &config_at(3)).await.expect("level 3");
    let (_objects, at9) = compact(&inputs, &CompactorConfig::default())
        .await
        .expect("default level");
    assert_eq!(CompactorConfig::default().rlog_zstd_level, 9);
    assert_eq!(at3.len(), 1);
    assert_eq!(at9.len(), 1);
    assert_eq!(scan(&at3[0]), scan(&at9[0]), "levels read back identically");
    assert_eq!(scan(&at9[0]).len(), 4500);
    assert_eq!(at3[0].len(), LEVEL_3_PART_BYTES);
    assert_eq!(at9[0].len(), LEVEL_9_PART_BYTES);
    assert!(at9[0].len() < at3[0].len());
}

// ---------------------------------------------------------------------------
// Unkeyed inputs
// ---------------------------------------------------------------------------

/// Three unkeyed inputs over three hours, interleaved a second apart so every
/// hour holds records of every input, sized to cut into several parts.
fn unkeyed_inputs() -> Vec<Input> {
    (0..3)
        .map(|n| {
            let records = (0..360i64)
                .map(|i| {
                    record(
                        (i % 3) as u32,
                        T0 + i * 30_000_000_000 + n * 1_000_000_000,
                        &format!("in{n} line {i} {}", "w".repeat((i % 17) as usize)),
                        vec![("route", s(&format!("/r/{}", i % 5)))],
                    )
                })
                .collect();
            Input::unkeyed(records)
        })
        .collect()
}

const UNKEYED_PART_HASHES: &[&str] = &[
    "69aaaad4a181f1cae281447ced464526246a570471ab82f2652502bf1f73b8d2",
    "29da13093b95ad99f18b9688fe25f21b58141523351c5d78690d53214b93092f",
    "c8f2b078e4f59e2a730bf06236fe918b465a1780d8e75fc69e4337c5b7a59911",
    "392aa6eec93cb02f614eab5a56aedcd83367fd9669907da3455d96ab4bfba3b7",
    "65d2104a71739f7a7fb1dd0c77a99ed539968bebf1bb001e9288b37c759ea475",
    "37e1b2f449b7cc1119c146cf3f2167a6ef1fbb6fcf79365aa289df006ef5a09d",
    "d3444e84dd571c810d7794d8f8e8d2f4d7592865e2e4dc3ca25f7e300b59f505",
];

/// Unkeyed inputs merge by `(ts, input_index)` exactly as before ADR-2135:
/// at level 3 every part's bytes are pinned by hash, and these are the hashes
/// the compactor at 655e56fc (fixed level 3, no clustered merge) writes for
/// this corpus, checked by running the same corpus there. The memory target
/// cuts the output mid-stream, so a merge pushing records in any other order
/// puts different records in each part.
#[tokio::test]
async fn unkeyed_inputs_merge_exactly_as_before() {
    let config = CompactorConfig {
        l1_part_memory_target_bytes: 64 * 1024,
        ..config_at(3)
    };
    let (_objects, parts) = compact(&unkeyed_inputs(), &config).await.expect("compact");
    let hashes: Vec<String> = parts.iter().map(|p| hash_hex(p)).collect();
    assert_eq!(hashes, UNKEYED_PART_HASHES);
    for p in &parts {
        assert_eq!(clustering(p), (None, 0));
    }
}
