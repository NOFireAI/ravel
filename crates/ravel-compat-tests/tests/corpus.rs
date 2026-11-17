//! The frozen baseline corpus (ADR-4233 decision 9, "Frozen corpus").
//!
//! Each directory `crates/ravel-compat-tests/corpus/<release>/` holds objects
//! written by the real write paths of the product code its `PROVENANCE` file
//! identifies, with the exact expected results recorded beside them. Corpora
//! are append-only (the `check-compat-corpus-append-only.sh` guard enforces
//! it); [`reads_every_frozen_corpus`] opens every directory under `corpus/`
//! and asserts exact results against the current build.
//!
//! `corpus/v0.34.0/` is the baseline candidate, written before the v0.34.0
//! tag exists. Its `PROVENANCE` records the git tree of each product crate
//! that wrote a fixture; the byte-identity check against the tagged build is
//! a release step, not this test, and its result goes in a new file.
//!
//! On-disk shape of one release directory:
//!   - `manifest.txt`, `objects/<NNNN>.bin`, `expected.txt`: metrics. Two RSEG
//!     L0 objects written by the segment codec the ingest flush uses, their
//!     commit records, and the L1 part and compaction record a real
//!     `compact_bucket` wrote from them. `expected.txt` holds object and record
//!     counts.
//!   - `samples.txt`: for every RSEG object, one line per series with its
//!     labels, `SeriesId` and every `(ts_ns, value.to_bits())`. The lines come
//!     from the generator's inputs, never from the reader under test.
//!   - `logs/` and `spans/`: one RLOG and one RSPAN L0 object with their
//!     commit records, and `expected.txt` holding every row. The RSPAN object
//!     is written as the ingest span flush writes it, with
//!     `RspanConfig::default()`. The RLOG object is written with the ingest
//!     flush's `RlogConfig` built from `IngestConfig::default()` (zstd level,
//!     group size, chunk order and the `Estimated` page choice), through the
//!     row `push` path and without the flush's indexed fields, key-index
//!     fields, sort descriptor or bloom scope.
//!   - `versions.txt` in each of the three object sets: every data object's
//!     key and trailer version, as the generating build stamped it in the
//!     object's commit record or compaction part. The read test asserts each
//!     trailer and each stamp against this file, never against the current
//!     build's write version.
//!   - `PROVENANCE`: the generator command, the git tree of each product
//!     crate whose writer produced a fixture, and the tree of ravel-ingest,
//!     whose `IngestConfig::default()` supplies the RLOG configuration.
//!
//! The read test loads each object set into a `MemoryStore` under its real keys.
//! (A disk `ScratchStore` is not used: it clears its directory on open, which
//! would wipe the fixtures.)
//!
//! `generate_corpus` (`#[ignore]`, run by hand) writes every file a release
//! directory lacks and never overwrites one; where the metrics objects already
//! exist it regenerates them in memory and asserts they are byte-identical.
//! It does not write `PROVENANCE`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ravel_catalog::{AbsentPolicy, validate_or_adopt};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_ingest::IngestConfig;
use ravel_logseg::PageChoice;
use ravel_logseg::reader::RlogReader;
use ravel_logseg::record::{LogRecord, Predicate, stream_attrs_bytes};
use ravel_logseg::writer::{RlogConfig, RlogWriter};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, compact_bucket, read,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_rspan::{RspanConfig, RspanReader, RspanWriter, SpanQuery, SpanRecord, StatusCode};
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7,
    open_from_full,
};
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

use common::{DecodedSeries, decode_rseg, hex};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 495_000;
const SHARD: u32 = 0;
/// The generator's target directory name, required so a run can never write
/// into a directory it was not pointed at.
const RELEASE_ENV: &str = "RAVEL_COMPAT_CORPUS_RELEASE";

fn corpus_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

/// Every release directory under `corpus/`, sorted.
fn release_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(corpus_root())
        .expect("read corpus/")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    dirs
}

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn hash() -> TenantHash {
    tenant().hash()
}

fn bucket() -> Bucket {
    Bucket::new(hash(), Signal::Metrics, SHARD, HOUR)
}

/// A time well past the seal margin for [`HOUR`], so the bucket is sealed.
fn sealed_now_ns() -> i64 {
    (i64::from(HOUR) + 1) * NS_PER_HOUR + 3 * NS_PER_HOUR
}

fn hour_start_ns() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn series(name: &str, value: f64, ts_ns: i64) -> SeriesInput {
    let labels = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: name.to_string(),
    }])
    .expect("labels");
    SeriesInput {
        series_id: SeriesId::compute(&tenant(), name, &labels).expect("series id"),
        labels,
        samples: vec![Sample { ts_ns, value }],
    }
}

/// The metrics inputs: L0 input 1 writes series a and b, input 2 writes another
/// sample of a. Compaction merges them into one L1 part.
fn metrics_inputs() -> Vec<(u64, Vec<SeriesInput>)> {
    vec![
        (
            1,
            vec![
                series("corpus_a", 1.0, hour_start_ns() + 1000),
                series("corpus_b", 2.0, hour_start_ns() + 1000),
            ],
        ),
        (2, vec![series("corpus_a", 3.0, hour_start_ns() + 2000)]),
    ]
}

/// The series a reader must decode from `inputs`: merged by `SeriesId`,
/// samples in timestamp order, series in `SeriesId` order (the catalog order).
fn model(inputs: &[&SeriesInput]) -> Vec<DecodedSeries> {
    let mut by_id: BTreeMap<[u8; 16], DecodedSeries> = BTreeMap::new();
    for input in inputs {
        let entry = by_id
            .entry(input.series_id.0)
            .or_insert_with(|| DecodedSeries {
                labels: input
                    .labels
                    .iter()
                    .map(|l| (l.name.clone(), l.value.clone()))
                    .collect(),
                series_id: input.series_id.0,
                samples: Vec::new(),
            });
        entry
            .samples
            .extend(input.samples.iter().map(|s| (s.ts_ns, s.value.to_bits())));
    }
    by_id
        .into_values()
        .map(|mut s| {
            s.samples.sort_unstable();
            s
        })
        .collect()
}

fn writer_identity(seq: u64) -> (Uuid, u64, i64) {
    let writer_id = Uuid::from_u128(0xC0_0000 + u128::from(seq));
    let event_ns = hour_start_ns() + (seq as i64) * 1_000_000;
    (writer_id, 1, event_ns)
}

#[allow(clippy::too_many_arguments)]
fn commit_record(
    signal: Signal,
    seq: u64,
    bytes: &[u8],
    content_hash: [u8; 32],
    sample_count: u64,
    series_count: u64,
    min_event_ts_ns: i64,
    max_event_ts_ns: i64,
    version: u16,
) -> (String, ravel_proto::commit::v1::CommitRecord) {
    let (writer_id, epoch, event_ns) = writer_identity(seq);
    let data_key = keys::data_key(&hash(), signal, SHARD, writer_id, epoch, seq, &content_hash)
        .expect("data key");
    let commit = record::build(NewCommitRecord {
        tenant_hash: hash(),
        signal,
        shard: SHARD,
        writer_id,
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count,
        series_count,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: event_ns,
        max_ingest_ts_ns: event_ns,
        segment_format_version: u32::from(version),
        created_unix_ns: event_ns,
        ingest_hour_bucket: u32::try_from(event_ns.div_euclid(NS_PER_HOUR)).expect("hour"),
    })
    .expect("build commit record");
    (data_key, commit)
}

async fn put_pair(
    store: &dyn ObjectStoreBackend,
    data_key: &str,
    bytes: Vec<u8>,
    commit: &ravel_proto::commit::v1::CommitRecord,
) {
    store
        .put(data_key, bytes.into(), PutOptions::default())
        .await
        .expect("put data object");
    store
        .put(
            &keys::commit_key_for_record(commit).expect("commit key"),
            record::encode(commit),
            PutOptions::default(),
        )
        .await
        .expect("put commit record");
}

/// Write one real RSEG v7 L0 object and its commit record at `seq`.
async fn write_l0(store: &dyn ObjectStoreBackend, seq: u64, inputs: Vec<SeriesInput>) {
    let (writer_id, epoch, event_ns) = writer_identity(seq);
    let written = SegmentWriter::write(
        inputs,
        SegmentIdentity {
            tenant_hash: hash().0,
            shard: SHARD,
            writer_id: writer_id.to_string(),
            writer_epoch: epoch,
            writer_seq: seq,
        },
        IngestBounds {
            min_ingest_ts_ns: event_ns,
            max_ingest_ts_ns: event_ns,
        },
    )
    .expect("write RSEG v7");
    let (data_key, commit) = commit_record(
        Signal::Metrics,
        seq,
        &written.bytes,
        written.summary.blake3,
        written.summary.sample_count,
        written.summary.series_count,
        written.summary.min_event_ts_ns,
        written.summary.max_event_ts_ns,
        VERSION_V7,
    );
    put_pair(store, &data_key, written.bytes.to_vec(), &commit).await;
}

/// Write the metrics corpus into a fresh store: provisioning, two L0 inputs and
/// a real compaction.
async fn build_metrics_store() -> MemoryStore {
    let store = MemoryStore::with_page_size(2);
    validate_or_adopt(
        &store,
        &hash(),
        Signal::Metrics,
        SHARD + 1,
        hour_start_ns(),
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    for (seq, inputs) in metrics_inputs() {
        write_l0(&store, seq, inputs).await;
    }
    let outcome = compact_bucket(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &bucket(),
    )
    .await
    .expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
    store
}

/// The expected `samples.txt` for the metrics store: every RSEG object's key
/// with the model series of its inputs. An L0 object holds its own input; the
/// L1 part holds every input merged.
async fn metrics_samples_from_model(store: &dyn ObjectStoreBackend) -> String {
    let inputs = metrics_inputs();
    let all: Vec<&SeriesInput> = inputs.iter().flat_map(|(_, s)| s.iter()).collect();
    let mut out = String::new();
    for key in common::data_keys(store).await {
        let series = if key.contains("/l1/") {
            model(&all)
        } else {
            let (_, input) = inputs
                .iter()
                .find(|(seq, _)| key.contains(&format!(".1.{seq:020}.")))
                .unwrap_or_else(|| panic!("no input for {key}"));
            model(&input.iter().collect::<Vec<_>>())
        };
        for s in series {
            out.push_str(&format!("{key}\t{s}\n"));
        }
    }
    out
}

/// Every RSEG object decoded through the segment reader, as `samples.txt`
/// lines in key order then catalog order.
async fn decoded_samples(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let mut lines = Vec::new();
    for key in common::data_keys(store).await {
        let bytes = store.get(&key, GetRange::Full).await.expect("get").data;
        for s in decode_rseg(&bytes).unwrap_or_else(|e| panic!("decode {key}: {e}")) {
            lines.push(format!("{key}\t{s}"));
        }
    }
    lines
}

/// Facts a reader recomputes from the metrics objects and checks against the
/// recorded `expected.txt`.
#[derive(Debug, PartialEq, Eq)]
struct Facts {
    rseg_data_objects: u64,
    l0_commit_records: u64,
    l0_sample_count: u64,
    l0_series_count: u64,
    compaction_records: u64,
    l1_sample_count: u64,
    l1_series_count: u64,
}

/// The RSEG data object count, the L0 commit records' summed counts, and the
/// compaction record's parts' summed counts. Versions are checked against
/// `versions.txt` by [`assert_versions`].
async fn compute_facts(store: &dyn ObjectStoreBackend) -> Facts {
    let rseg_data_objects = common::data_keys(store).await.len() as u64;

    let listing = read::list_bucket(store, &bucket())
        .await
        .expect("list bucket");
    let (mut l0_sample_count, mut l0_series_count) = (0u64, 0u64);
    for key in &listing.commit_keys {
        let bytes = store.get(key, GetRange::Full).await.expect("get").data;
        let rec = record::decode(&bytes).expect("decode commit");
        l0_sample_count += rec.sample_count;
        l0_series_count += rec.series_count;
    }
    let (mut l1_sample_count, mut l1_series_count) = (0u64, 0u64);
    for key in &listing.compaction_record_keys {
        let bytes = store.get(key, GetRange::Full).await.expect("get").data;
        let rec = record::decode_compaction(&bytes).expect("decode compaction");
        for part in &rec.parts {
            l1_sample_count += part.sample_count;
            l1_series_count += part.series_count;
        }
    }
    Facts {
        rseg_data_objects,
        l0_commit_records: listing.commit_keys.len() as u64,
        l0_sample_count,
        l0_series_count,
        compaction_records: listing.compaction_record_keys.len() as u64,
        l1_sample_count,
        l1_series_count,
    }
}

fn facts_to_lines(f: &Facts) -> String {
    format!(
        "rseg_data_objects={}\nl0_commit_records={}\nl0_sample_count={}\nl0_series_count={}\n\
         compaction_records={}\nl1_sample_count={}\nl1_series_count={}\n",
        f.rseg_data_objects,
        f.l0_commit_records,
        f.l0_sample_count,
        f.l0_series_count,
        f.compaction_records,
        f.l1_sample_count,
        f.l1_series_count,
    )
}

fn facts_from_lines(text: &str) -> Facts {
    let map: BTreeMap<&str, u64> = text
        .lines()
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.trim(), v.trim().parse().expect("number")))
        .collect();
    let get = |k: &str| *map.get(k).unwrap_or_else(|| panic!("missing {k}"));
    Facts {
        rseg_data_objects: get("rseg_data_objects"),
        l0_commit_records: get("l0_commit_records"),
        l0_sample_count: get("l0_sample_count"),
        l0_series_count: get("l0_series_count"),
        compaction_records: get("compaction_records"),
        l1_sample_count: get("l1_sample_count"),
        l1_series_count: get("l1_series_count"),
    }
}

// === Logs and spans ==========================================================

fn attr_text(value: &AttrValue) -> String {
    match value {
        AttrValue::Str(s) => format!("s:{s}"),
        AttrValue::I64(n) => format!("i:{n}"),
        AttrValue::F64(f) => format!("f:{:016x}", f.to_bits()),
        AttrValue::Bool(b) => format!("b:{b}"),
        other => panic!("the corpus fixtures write no {other:?} attribute"),
    }
}

fn opt_hex(bytes: Option<&[u8]>) -> String {
    bytes.map_or_else(|| "-".to_string(), hex)
}

/// One RLOG row as an `expected.txt` line: every field the record carries.
fn log_line(key: &str, r: &LogRecord) -> String {
    let attrs: Vec<String> = r
        .attrs
        .iter()
        .map(|(k, v)| format!("{k}={}", attr_text(v)))
        .collect();
    format!(
        "{key}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        r.stream_id.to_hex(),
        hex(&r.stream_attrs),
        r.ts_ns,
        r.observed_ts_ns,
        r.severity_num,
        r.severity_text,
        r.body,
        opt_hex(r.trace_id.as_ref().map(|t| &t[..])),
        opt_hex(r.span_id.as_ref().map(|s| &s[..])),
        r.flags,
        attrs.join(";"),
    )
}

/// One RSPAN row as an `expected.txt` line.
fn span_line(key: &str, s: &SpanRecord) -> String {
    let attrs: Vec<String> = s.attrs.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!(
        "{key}\t{}\t{}\t{}\t{}\t{}\t{}\t{:?}\t{}\t{}",
        hex(&s.trace_id),
        hex(&s.span_id),
        opt_hex(s.parent_span_id.as_ref().map(|p| &p[..])),
        s.name,
        s.start_ts_ns,
        s.end_ts_ns,
        s.status_code,
        s.status_message.as_deref().unwrap_or("-"),
        attrs.join(";"),
    )
}

fn log_records() -> Vec<LogRecord> {
    let stream = |svc: &str| {
        let res = vec![("service.name".to_string(), AttrValue::Str(svc.to_string()))];
        (
            log_stream_id(&res, "corpus-scope", "1", &[]),
            stream_attrs_bytes(&res, "corpus-scope", "1", &[]),
        )
    };
    let rec = |svc: &str,
               ts: i64,
               sev: (u8, &str),
               body: &str,
               trace: Option<([u8; 16], [u8; 8])>,
               attrs: Vec<(String, AttrValue)>| {
        let (stream_id, stream_attrs) = stream(svc);
        LogRecord {
            stream_id,
            stream_attrs,
            ts_ns: ts,
            observed_ts_ns: ts + 5,
            severity_num: sev.0,
            severity_text: sev.1.to_string(),
            body: body.to_string(),
            trace_id: trace.map(|t| t.0),
            span_id: trace.map(|t| t.1),
            flags: u32::from(trace.is_some()),
            attrs,
        }
    };
    let h = hour_start_ns();
    vec![
        rec(
            "checkout",
            h + 1_000,
            (9, "INFO"),
            "order placed",
            Some(([0x11; 16], [0x21; 8])),
            vec![
                ("amount".to_string(), AttrValue::F64(-0.0)),
                ("order.id".to_string(), AttrValue::I64(4249)),
            ],
        ),
        rec(
            "checkout",
            h + 2_000,
            (17, "ERROR"),
            "payment declined",
            None,
            vec![("retry".to_string(), AttrValue::Bool(true))],
        ),
        rec(
            "inventory",
            h + 1_500,
            (5, "DEBUG"),
            "stock reserved",
            None,
            vec![("sku".to_string(), AttrValue::Str("A-1".to_string()))],
        ),
    ]
}

fn span_records() -> Vec<SpanRecord> {
    let h = hour_start_ns();
    vec![
        SpanRecord {
            trace_id: [0x11; 16],
            span_id: [0x21; 8],
            parent_span_id: None,
            name: "POST /checkout".to_string(),
            start_ts_ns: h + 1_000,
            end_ts_ns: h + 9_000,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: vec![
                ("http.method".to_string(), "POST".to_string()),
                ("service.name".to_string(), "checkout".to_string()),
            ],
        },
        SpanRecord {
            trace_id: [0x11; 16],
            span_id: [0x22; 8],
            parent_span_id: Some([0x21; 8]),
            name: "charge card".to_string(),
            start_ts_ns: h + 2_000,
            end_ts_ns: h + 4_000,
            status_code: StatusCode::Error,
            status_message: Some("declined".to_string()),
            attrs: vec![("service.name".to_string(), "payments".to_string())],
        },
        SpanRecord {
            trace_id: [0x12; 16],
            span_id: [0x31; 8],
            parent_span_id: None,
            name: "reserve stock".to_string(),
            start_ts_ns: h + 1_500,
            end_ts_ns: h + 3_500,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: vec![("service.name".to_string(), "inventory".to_string())],
        },
    ]
}

const LOGS_SEQ: u64 = 3;
const SPANS_SEQ: u64 = 4;

/// The `RlogConfig` an ingest log flush writes with (`flush_rlog_config` in
/// ravel-ingest's `log_shard.rs`), from `IngestConfig::default()`: the zstd
/// level, group size, chunk order and `Estimated` page choice over
/// `RlogConfig::default()`.
fn flush_rlog_config() -> RlogConfig {
    let ingest = IngestConfig::default();
    RlogConfig {
        zstd_level: ingest.rlog_zstd_level.get(),
        group_target_blocks: ingest.rlog_group_target_blocks.get(),
        chunk_order: ingest.rlog_chunk_order.get(),
        page_choice: ingest.rlog_page_choice,
        ..RlogConfig::default()
    }
}

/// One RLOG L0 object and its commit record, written with
/// [`flush_rlog_config`]. The commit record carries what the flush records:
/// `series_count` the stream count and the event bounds the row timestamps.
/// Returns the store and the object's key.
async fn build_logs_store() -> (MemoryStore, String) {
    let store = MemoryStore::with_page_size(2);
    let (writer_id, epoch, _) = writer_identity(LOGS_SEQ);
    let records = log_records();
    let config = flush_rlog_config();
    assert_eq!(config.page_choice, PageChoice::Estimated);
    let mut writer = RlogWriter::new(
        config,
        ravel_logseg::writer::ObjectIdentity {
            tenant_hash: hash().0,
            shard: SHARD,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: epoch,
            writer_seq: LOGS_SEQ,
        },
    );
    for r in records.clone() {
        writer.push(r).expect("push log record");
    }
    let bytes = writer.finish().expect("finish RLOG");
    let streams: std::collections::BTreeSet<_> = records.iter().map(|r| r.stream_id).collect();
    let (data_key, commit) = commit_record(
        Signal::Logs,
        LOGS_SEQ,
        &bytes,
        blake3_hash(&bytes),
        records.len() as u64,
        streams.len() as u64,
        records.iter().map(|r| r.ts_ns).min().expect("rows"),
        records.iter().map(|r| r.ts_ns).max().expect("rows"),
        ravel_logseg::WriteVersion::CURRENT.number(),
    );
    put_pair(&store, &data_key, bytes, &commit).await;
    (store, data_key)
}

/// One RSPAN L0 object and its commit record, as the ingest span flush writes
/// them: `RspanConfig::default()`, `series_count` the distinct trace count and
/// the event bounds min start and max end.
async fn build_spans_store() -> (MemoryStore, String) {
    let store = MemoryStore::with_page_size(2);
    let (writer_id, epoch, _) = writer_identity(SPANS_SEQ);
    let spans = span_records();
    let mut writer = RspanWriter::new(
        RspanConfig::default(),
        ravel_rspan::ObjectIdentity {
            tenant_hash: hash().0,
            shard: SHARD,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: epoch,
            writer_seq: SPANS_SEQ,
        },
    );
    for s in spans.clone() {
        writer.push(s);
    }
    let bytes = writer.finish().expect("finish RSPAN");
    let traces: std::collections::BTreeSet<_> = spans.iter().map(|s| s.trace_id).collect();
    let (data_key, commit) = commit_record(
        Signal::Spans,
        SPANS_SEQ,
        &bytes,
        blake3_hash(&bytes),
        spans.len() as u64,
        traces.len() as u64,
        spans.iter().map(|s| s.start_ts_ns).min().expect("spans"),
        spans.iter().map(|s| s.end_ts_ns).max().expect("spans"),
        ravel_rspan::footer::WriteVersion::CURRENT.number(),
    );
    put_pair(&store, &data_key, bytes, &commit).await;
    (store, data_key)
}

fn blake3_hash(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

/// The RSPAN trailer's version field: the u16 eight bytes from the end
/// (docs/span-segment-format.md trailer: len, crc, version, signal, reserved,
/// magic).
fn rspan_trailer_version(bytes: &[u8]) -> u16 {
    let at = bytes.len() - 8;
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}

/// Every row of every RLOG object in `store`, decoded through `RlogReader`,
/// as sorted `expected.txt` lines.
async fn decoded_log_lines(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let mut lines = Vec::new();
    for key in common::data_keys(store).await {
        let bytes = store.get(&key, GetRange::Full).await.expect("get").data;
        let reader = RlogReader::new(&bytes, &RlogConfig::default()).expect("open RLOG");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        lines.extend(rows.iter().map(|r| log_line(&key, r)));
    }
    lines.sort();
    lines
}

/// Every span of every RSPAN object in `store`, decoded through `RspanReader`,
/// as sorted `expected.txt` lines.
async fn decoded_span_lines(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let mut lines = Vec::new();
    for key in common::data_keys(store).await {
        let bytes = store.get(&key, GetRange::Full).await.expect("get").data;
        let reader = RspanReader::new(&bytes, &RspanConfig::default()).expect("open RSPAN");
        let (rows, _) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan");
        lines.extend(rows.iter().map(|s| span_line(&key, s)));
    }
    lines.sort();
    lines
}

// === Recorded versions =======================================================

/// The format family of one object set's data objects.
#[derive(Clone, Copy, Debug)]
enum Family {
    Rseg,
    Rlog,
    Rspan,
}

impl Family {
    fn signal(self) -> Signal {
        match self {
            Family::Rseg => Signal::Metrics,
            Family::Rlog => Signal::Logs,
            Family::Rspan => Signal::Spans,
        }
    }

    /// The version field of the object's trailer.
    fn trailer_version(self, bytes: &[u8]) -> u32 {
        u32::from(match self {
            Family::Rseg => {
                open_from_full(bytes, ReaderLimits::default())
                    .expect("open RSEG")
                    .version
            }
            Family::Rlog => ravel_logseg::footer::trailer_version(bytes).expect("RLOG trailer"),
            Family::Rspan => rspan_trailer_version(bytes),
        })
    }
}

/// One `segment_format_version` stamp: the record that carries it, the
/// content hash of the object it describes, and the version.
struct Stamp {
    record_key: String,
    content_hash: Vec<u8>,
    version: u32,
}

/// Every version stamp in the family's bucket: each L0 commit record's, and
/// each compaction record part's.
async fn version_stamps(store: &dyn ObjectStoreBackend, family: Family) -> Vec<Stamp> {
    let listing = read::list_bucket(store, &Bucket::new(hash(), family.signal(), SHARD, HOUR))
        .await
        .expect("list bucket");
    let mut stamps = Vec::new();
    for key in &listing.commit_keys {
        let bytes = store.get(key, GetRange::Full).await.expect("get").data;
        let rec = record::decode(&bytes).expect("decode commit");
        stamps.push(Stamp {
            record_key: key.clone(),
            content_hash: rec.content_hash,
            version: rec.segment_format_version,
        });
    }
    for key in &listing.compaction_record_keys {
        let bytes = store.get(key, GetRange::Full).await.expect("get").data;
        let rec = record::decode_compaction(&bytes).expect("decode compaction");
        stamps.extend(rec.parts.into_iter().map(|part| Stamp {
            record_key: key.clone(),
            content_hash: part.content_hash,
            version: part.segment_format_version,
        }));
    }
    stamps
}

/// The one stamp describing the object `bytes`.
fn stamp_of<'a>(stamps: &'a [Stamp], key: &str, bytes: &[u8]) -> &'a Stamp {
    let content_hash = blake3_hash(bytes);
    let mine: Vec<&Stamp> = stamps
        .iter()
        .filter(|s| s.content_hash == content_hash)
        .collect();
    assert_eq!(mine.len(), 1, "{key}: one record stamps this object");
    mine[0]
}

/// The generator's `versions.txt`: each data object's key and the version
/// the generating build stamped in its record.
async fn versions_from_stamps(store: &dyn ObjectStoreBackend, family: Family) -> String {
    let stamps = version_stamps(store, family).await;
    let mut out = String::new();
    for key in common::data_keys(store).await {
        let bytes = store.get(&key, GetRange::Full).await.expect("get").data;
        let version = stamp_of(&stamps, &key, &bytes).version;
        out.push_str(&format!("{key}\t{version}\n"));
    }
    out
}

/// Each data object's trailer, and the one record stamp describing it, equal
/// the version `versions.txt` records for it; `versions.txt` names every data
/// object and every stamp describes one.
async fn assert_versions(store: &dyn ObjectStoreBackend, family: Family, dir: &Path) {
    let name = dir.display();
    let recorded: BTreeMap<String, u32> = lines_of(&dir.join("versions.txt"))
        .iter()
        .map(|line| {
            let (key, version) = line.split_once('\t').expect("versions.txt line");
            (key.to_string(), version.parse().expect("version number"))
        })
        .collect();
    let keys = common::data_keys(store).await;
    assert_eq!(
        keys.iter().collect::<Vec<_>>(),
        recorded.keys().collect::<Vec<_>>(),
        "{name}: versions.txt names every data object"
    );
    let stamps = version_stamps(store, family).await;
    for key in &keys {
        let bytes = store.get(key, GetRange::Full).await.expect("get").data;
        assert_eq!(
            family.trailer_version(&bytes),
            recorded[key],
            "{name}: {key} trailer version"
        );
        let stamp = stamp_of(&stamps, key, &bytes);
        assert_eq!(
            stamp.version, recorded[key],
            "{name}: {} stamps {key}",
            stamp.record_key
        );
    }
    assert_eq!(
        stamps.len(),
        keys.len(),
        "{name}: every record stamp describes a data object"
    );
}

// === On-disk object sets =====================================================

/// Load an object set (`manifest.txt` plus `objects/`) into a `MemoryStore`
/// under its real keys.
async fn load_set(dir: &Path) -> MemoryStore {
    let store = MemoryStore::with_page_size(2);
    let manifest = read_file(&dir.join("manifest.txt"));
    for line in manifest.lines() {
        let (index, key) = line.split_once('\t').expect("manifest line");
        let bytes =
            std::fs::read(dir.join("objects").join(format!("{index}.bin"))).expect("read object");
        store
            .put(key, bytes.into(), PutOptions::default())
            .await
            .expect("load object");
    }
    store
}

fn read_file(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn lines_of(path: &Path) -> Vec<String> {
    read_file(path).lines().map(str::to_string).collect()
}

/// Write `contents` to `path` only if it does not exist yet; never overwrite.
fn create_new(path: &Path, contents: &[u8]) {
    use std::io::Write;
    std::fs::create_dir_all(path.parent().expect("parent")).expect("create dir");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap_or_else(|e| panic!("create {} (never overwritten): {e}", path.display()));
    file.write_all(contents).expect("write");
}

/// Dump every object in `store` (sorted by key) under `dir`.
async fn freeze_set(store: &dyn ObjectStoreBackend, dir: &Path) {
    let mut manifest = String::new();
    for (index, meta) in list_all(store, "")
        .await
        .expect("list")
        .into_iter()
        .enumerate()
    {
        let bytes = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get")
            .data;
        create_new(&dir.join("objects").join(format!("{index:04}.bin")), &bytes);
        manifest.push_str(&format!("{index:04}\t{}\n", meta.key));
    }
    create_new(&dir.join("manifest.txt"), manifest.as_bytes());
}

/// A release directory's three object sets and the family of each.
fn object_sets(dir: &Path) -> [(PathBuf, Family); 3] {
    [
        (dir.to_path_buf(), Family::Rseg),
        (dir.join("logs"), Family::Rlog),
        (dir.join("spans"), Family::Rspan),
    ]
}

/// Assert one release directory reads back exactly: every object's recorded
/// version, the metrics counts and every decoded sample, every log row and
/// every span.
async fn assert_release_reads_exactly(dir: &Path) {
    let name = dir.display();
    assert!(
        dir.join("PROVENANCE").is_file(),
        "{name}: a corpus records where it came from in PROVENANCE"
    );
    for (set, family) in object_sets(dir) {
        assert_versions(&load_set(&set).await, family, &set).await;
    }

    let metrics = load_set(dir).await;
    let facts = compute_facts(&metrics).await;
    assert_eq!(
        facts,
        facts_from_lines(&read_file(&dir.join("expected.txt"))),
        "{name}: metrics object and record counts"
    );
    assert_eq!(
        facts.l1_sample_count, facts.l0_sample_count,
        "{name}: compaction conserves samples"
    );
    let samples = decoded_samples(&metrics).await;
    let expected_samples = lines_of(&dir.join("samples.txt"));
    assert_eq!(
        samples, expected_samples,
        "{name}: every RSEG series, SeriesId and (ts_ns, value bits) decodes exactly"
    );

    let logs = load_set(&dir.join("logs")).await;
    assert_eq!(
        decoded_log_lines(&logs).await,
        lines_of(&dir.join("logs").join("expected.txt")),
        "{name}: every RLOG row decodes exactly"
    );

    let spans = load_set(&dir.join("spans")).await;
    assert_eq!(
        decoded_span_lines(&spans).await,
        lines_of(&dir.join("spans").join("expected.txt")),
        "{name}: every RSPAN span decodes exactly"
    );
}

/// The real test: every release directory under `corpus/` reads back exactly
/// with the current build.
#[tokio::test]
async fn reads_every_frozen_corpus() {
    let dirs = release_dirs();
    assert!(
        !dirs.is_empty(),
        "no corpus under {}; run generate_corpus",
        corpus_root().display()
    );
    for dir in &dirs {
        assert_release_reads_exactly(dir).await;
    }
}

/// The sample comparison is exact: one flipped value bit or one shifted
/// timestamp in the decoded series makes it unequal to the recorded line.
#[tokio::test]
async fn a_one_bit_sample_difference_fails_the_comparison() {
    for dir in release_dirs() {
        let metrics = load_set(&dir).await;
        let expected = lines_of(&dir.join("samples.txt"));
        let key = common::data_keys(&metrics).await.remove(0);
        let bytes = metrics.get(&key, GetRange::Full).await.expect("get").data;
        let decoded = decode_rseg(&bytes).expect("decode");

        let mutate = |f: &dyn Fn(&mut DecodedSeries)| {
            let mut wrong = decoded.clone();
            f(&mut wrong[0]);
            wrong
                .iter()
                .map(|s| format!("{key}\t{s}"))
                .collect::<Vec<_>>()
        };
        let flipped = mutate(&|s| s.samples[0].1 ^= 1);
        let shifted = mutate(&|s| s.samples[0].0 += 1);
        let relabeled = mutate(&|s| s.series_id[15] ^= 1);
        for wrong in [flipped, shifted, relabeled] {
            assert!(
                !expected.iter().any(|l| l == &wrong[0]),
                "{}: a wrong series line must not match the record: {}",
                dir.display(),
                wrong[0]
            );
        }
    }
}

/// Run by hand to FREEZE a corpus (`#[ignore]`), with
/// `RAVEL_COMPAT_CORPUS_RELEASE=<dir name>`: writes every file the directory
/// lacks (never overwriting one) and asserts that metrics objects already
/// frozen there are byte-identical to what this build writes. Then reads the
/// directory back exactly.
#[tokio::test]
#[ignore = "writes the frozen corpus fixtures; run by hand to create or extend a release corpus"]
async fn generate_corpus() {
    let release = std::env::var(RELEASE_ENV)
        .unwrap_or_else(|_| panic!("set {RELEASE_ENV} to the release directory name"));
    let dir = corpus_root().join(release);

    let metrics = build_metrics_store().await;
    if dir.join("manifest.txt").exists() {
        let frozen = load_set(&dir).await;
        assert_eq!(
            common::snapshot(&frozen).await,
            common::snapshot(&metrics).await,
            "the frozen metrics objects are byte-identical to what this build writes"
        );
    } else {
        freeze_set(&metrics, &dir).await;
        create_new(
            &dir.join("expected.txt"),
            facts_to_lines(&compute_facts(&metrics).await).as_bytes(),
        );
    }
    if !dir.join("samples.txt").exists() {
        create_new(
            &dir.join("samples.txt"),
            metrics_samples_from_model(&metrics).await.as_bytes(),
        );
    }

    if !dir.join("logs").exists() {
        let (logs, key) = build_logs_store().await;
        freeze_set(&logs, &dir.join("logs")).await;
        let mut lines: Vec<String> = log_records().iter().map(|r| log_line(&key, r)).collect();
        lines.sort();
        create_new(
            &dir.join("logs").join("expected.txt"),
            format!("{}\n", lines.join("\n")).as_bytes(),
        );
    }
    if !dir.join("spans").exists() {
        let (spans, key) = build_spans_store().await;
        freeze_set(&spans, &dir.join("spans")).await;
        let mut lines: Vec<String> = span_records().iter().map(|s| span_line(&key, s)).collect();
        lines.sort();
        create_new(
            &dir.join("spans").join("expected.txt"),
            format!("{}\n", lines.join("\n")).as_bytes(),
        );
    }

    for (set, family) in object_sets(&dir) {
        if !set.join("versions.txt").exists() {
            let frozen = load_set(&set).await;
            create_new(
                &set.join("versions.txt"),
                versions_from_stamps(&frozen, family).await.as_bytes(),
            );
        }
    }

    assert_release_reads_exactly(&dir).await;
}
