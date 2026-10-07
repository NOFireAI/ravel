//! `ravel-cli clustering-key set`/`clear` and `bloom-scope set` reaching the
//! log objects a load writes (ADR-2135 decision 7, issue #2146).
//!
//! Each test drives the functions `main.rs` dispatches the commands to,
//! in-process against one `MemoryStore`, then loads a Parquet file through
//! `load::run` (what `main.rs` dispatches `load` to) into the same store and
//! reads the written RLOG object back. A subprocess against `--store memory`
//! could not hand its store to the next command. The rollout-flag test also
//! runs the built binary, because exit status 2 is clap's and only the binary
//! has it.
//!
//! `load::run` flushes on the system clock, so the fixture's timestamps sit on
//! the UTC day before the test runs and the SQL window reaches the present:
//! the commit is filed under the flush time and the rows under their own.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use ravel_catalog::{Catalog, CatalogConfig, StorageLayoutWrite};
use ravel_cli::load::{self, DEFAULT_DECODE_QUEUE_BATCHES, DEFAULT_TARGET_BYTES};
use ravel_cli::maintain::SignalArg;
use ravel_cli::storage_layout::{self, BloomScopeArg, BucketWidthArg};
use ravel_cli::typed_attr_column;
use ravel_ingest::RlogZstdLevel;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{
    self, LogFooter, SortBucketWidth, SortDescriptor, SortKeyColumn, SortKeyType, kind,
};
use ravel_logseg::record::{COL_BODY, COL_SEVERITY_TEXT};
use ravel_logseg::rlog_bloom::RlogBloomSection;
use ravel_logseg::{FieldType, LogRecord, Predicate, RlogConfig, RlogReader, read_section};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, InstrumentedStore, ListPage, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError, StoreMetrics,
    StoreMetricsSnapshot, list_all,
};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::logstream::AttrValue;
use ravel_types::{TenantId, TimeRange};

const TENANT: &str = "acme";
const HOUR: i64 = 3_600_000_000_000;
const DAY: i64 = 24 * HOUR;
const ROWS: usize = 12;

const MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "region"
column = "region"
type = "str"

[[attribute]]
key = "code"
column = "code"
type = "i64"

[[attribute]]
key = "user"
column = "user"
type = "str"
"#;

/// (body, service, ts offset from the fixture's day start, region, code,
/// user), pushed in this order. Two streams, each across two six-hour
/// buckets.
const FIXTURE: [(&str, &str, i64, &str, i64, &str); ROWS] = [
    ("a", "api", HOUR, "west", 200, "u-0"),
    ("b", "api", 2 * HOUR, "east", 500, "u-1"),
    ("c", "api", 3 * HOUR, "east", 200, "u-2"),
    ("d", "api", 7 * HOUR, "east", 500, "u-3"),
    ("e", "api", 8 * HOUR, "west", 200, "u-0"),
    ("f", "api", 9 * HOUR, "east", 200, "u-1"),
    ("g", "web", 3 * HOUR / 2, "east", 500, "u-2"),
    ("h", "web", 4 * HOUR, "west", 500, "u-3"),
    ("i", "web", 5 * HOUR / 2, "east", 500, "u-0"),
    ("j", "web", 13 * HOUR / 2, "west", 200, "u-1"),
    ("k", "web", 10 * HOUR, "east", 200, "u-2"),
    ("l", "web", 11 * HOUR, "east", 200, "u-3"),
];

/// Under the `[region, code]` key at six hours, each stream stores as
/// (bucket, region, code, ts): api c, b, a | f, d, e and web g, i, h | k, l, j.
const API_KEYED: [&str; 6] = ["c", "b", "a", "f", "d", "e"];
const WEB_KEYED: [&str; 6] = ["g", "i", "h", "k", "l", "j"];
/// Without a key each stream stores in ts order.
const API_UNKEYED: [&str; 6] = ["a", "b", "c", "d", "e", "f"];
const WEB_UNKEYED: [&str; 6] = ["g", "i", "h", "j", "k", "l"];

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos() as i64
}

/// The start of the UTC day before today, a six-hour bucket boundary.
fn fixture_day(now_ns: i64) -> i64 {
    (now_ns / DAY - 1) * DAY
}

struct Fixture {
    _dir: tempfile::TempDir,
    parquet: std::path::PathBuf,
    mapping: std::path::PathBuf,
    day: i64,
    now_ns: i64,
}

fn fixture() -> Fixture {
    let now_ns = now_ns();
    let day = fixture_day(now_ns);
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = dir.path().join("logs.parquet");
    let mapping = dir.path().join("mapping.toml");
    std::fs::write(&mapping, MAPPING).expect("write mapping");
    type Row = (
        &'static str,
        &'static str,
        i64,
        &'static str,
        i64,
        &'static str,
    );
    let col = |f: fn(&Row) -> &'static str| -> ArrayRef {
        Arc::new(StringArray::from(
            FIXTURE.iter().map(f).collect::<Vec<&str>>(),
        ))
    };
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from(
                FIXTURE.iter().map(|r| day + r.2).collect::<Vec<i64>>(),
            )) as ArrayRef,
        ),
        ("body", col(|r| r.0)),
        ("svc", col(|r| r.1)),
        ("region", col(|r| r.3)),
        (
            "code",
            Arc::new(Int64Array::from(
                FIXTURE.iter().map(|r| r.4).collect::<Vec<i64>>(),
            )) as ArrayRef,
        ),
        ("user", col(|r| r.5)),
    ])
    .expect("batch");
    write_parquet(&parquet, &batch);
    Fixture {
        _dir: dir,
        parquet,
        mapping,
        day,
        now_ns,
    }
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

/// A memory store behind a counting decorator, so a test can prove a refused
/// command issued no write.
fn counted_store() -> (Arc<dyn ObjectStoreBackend>, Arc<StoreMetrics>) {
    let metrics = Arc::new(StoreMetrics::default());
    let store = InstrumentedStore::with_metrics(MemoryStore::new(), Arc::clone(&metrics));
    (Arc::new(store), metrics)
}

/// Every object in the store, key and bytes.
async fn contents(store: &dyn ObjectStoreBackend) -> Vec<(String, bytes::Bytes)> {
    let mut out = Vec::new();
    for meta in list_all(store, "").await.expect("list") {
        let data = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get")
            .data;
        out.push((meta.key, data));
    }
    out
}

fn writes(snapshot: &StoreMetricsSnapshot) -> (u64, u64) {
    (snapshot.put.calls, snapshot.delete.calls)
}

async fn declare_region_and_code(store: &Arc<dyn ObjectStoreBackend>, now_ns: i64) {
    typed_attr_column::set(
        Arc::clone(store),
        TENANT,
        &["region:str".to_string(), "code:i64".to_string()],
        now_ns,
    )
    .await
    .expect("declare typed columns");
}

async fn set_key(
    store: &Arc<dyn ObjectStoreBackend>,
    columns: &[&str],
    now_ns: i64,
) -> anyhow::Result<String> {
    let mut out = Vec::new();
    storage_layout::clustering_key_set_to(
        Arc::clone(store),
        TENANT,
        columns.iter().map(|c| c.to_string()).collect(),
        BucketWidthArg::SixHours,
        StorageLayoutWrite::ReadersRolledOut,
        now_ns,
        &mut out,
    )
    .await?;
    Ok(String::from_utf8(out).expect("utf8"))
}

async fn clear_key(store: &Arc<dyn ObjectStoreBackend>, now_ns: i64) -> anyhow::Result<String> {
    let mut out = Vec::new();
    storage_layout::clustering_key_clear_to(
        Arc::clone(store),
        TENANT,
        StorageLayoutWrite::ReadersRolledOut,
        now_ns,
        &mut out,
    )
    .await?;
    Ok(String::from_utf8(out).expect("utf8"))
}

async fn set_scope(
    store: &Arc<dyn ObjectStoreBackend>,
    scope: BloomScopeArg,
    now_ns: i64,
) -> anyhow::Result<String> {
    let mut out = Vec::new();
    storage_layout::bloom_scope_set_to(
        Arc::clone(store),
        TENANT,
        scope,
        StorageLayoutWrite::ReadersRolledOut,
        now_ns,
        &mut out,
    )
    .await?;
    Ok(String::from_utf8(out).expect("utf8"))
}

/// Load the fixture and return the one RLOG object it wrote.
async fn load_one(store: &Arc<dyn ObjectStoreBackend>, fx: &Fixture) -> bytes::Bytes {
    load::run(
        Arc::clone(store),
        &fx.parquet,
        TENANT,
        &fx.mapping,
        SignalArg::Logs,
        1,
        ROWS,
        0,
        None,
        1,
        1,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        RlogZstdLevel::new(3).expect("in range"),
        None,
        fx.now_ns,
    )
    .await
    .expect("load succeeds");
    let objects: Vec<bytes::Bytes> = contents(store.as_ref())
        .await
        .into_iter()
        .filter(|(_, data)| footer::open(data).is_ok())
        .map(|(_, data)| data)
        .collect();
    assert_eq!(objects.len(), 1, "one shard, one flush, one RLOG object");
    objects.into_iter().next().expect("one object")
}

/// The trailer's version field, read from the bytes rather than the decoder.
fn trailer_version(object: &[u8]) -> u16 {
    let n = object.len();
    u16::from_le_bytes([object[n - 8], object[n - 7]])
}

fn records(object: &[u8]) -> Vec<LogRecord> {
    let reader = RlogReader::new(object, &RlogConfig::default()).expect("open rlog");
    reader.scan(&Predicate::And(Vec::new())).expect("scan").0
}

/// Bodies in stored order, split at the stream boundary. Panics unless the
/// object holds exactly two streams, each one contiguous run.
fn bodies_by_stream(object: &[u8]) -> Vec<Vec<String>> {
    let mut runs: Vec<(ravel_types::logstream::LogStreamId, Vec<String>)> = Vec::new();
    for r in records(object) {
        match runs.last_mut() {
            Some((id, bodies)) if *id == r.stream_id => bodies.push(r.body),
            _ => {
                assert!(
                    runs.iter().all(|(id, _)| *id != r.stream_id),
                    "a stream's rows are not contiguous"
                );
                runs.push((r.stream_id, vec![r.body]));
            }
        }
    }
    assert_eq!(runs.len(), 2, "two streams");
    assert!(runs[0].0 < runs[1].0, "streams in stream id order");
    runs.into_iter().map(|(_, b)| b).collect()
}

/// The two streams' expected orders, in the stream order the object uses:
/// stream id order, which a hash fixes.
fn by_stream_id(object: &[u8], api: [&str; 6], web: [&str; 6]) -> Vec<Vec<String>> {
    let first = records(object).into_iter().next().expect("a record").body;
    let first_is_api = FIXTURE.iter().any(|f| f.0 == first && f.1 == "api");
    let own = |v: [&str; 6]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    if first_is_api {
        vec![own(api), own(web)]
    } else {
        vec![own(web), own(api)]
    }
}

fn str_attr<'a>(r: &'a LogRecord, key: &str) -> &'a str {
    match r.attrs.iter().find(|(k, _)| k == key) {
        Some((_, AttrValue::Str(s))) => s,
        other => panic!("{key} is not a string attribute: {other:?}"),
    }
}

fn i64_attr(r: &LogRecord, key: &str) -> i64 {
    match r.attrs.iter().find(|(k, _)| k == key) {
        Some((_, AttrValue::I64(v))) => *v,
        other => panic!("{key} is not an i64 attribute: {other:?}"),
    }
}

/// The object's FIELD_DIR and parsed BLOOM section.
fn bloom(object: &[u8]) -> (FieldDir, Vec<u32>) {
    let ftr = footer::open(object).expect("footer");
    let cfg = RlogConfig::default();
    let dir_raw = read_section(
        object,
        ftr.section(kind::FIELD_DIR).expect("FIELD_DIR"),
        &cfg,
    )
    .expect("read FIELD_DIR");
    let dir = FieldDir::decode(&dir_raw, u64::MAX).expect("decode FIELD_DIR");
    let raw =
        read_section(object, ftr.section(kind::BLOOM).expect("BLOOM"), &cfg).expect("read BLOOM");
    let covered = RlogBloomSection::parse(&raw, &dir)
        .expect("parse BLOOM")
        .covered()
        .to_vec();
    (dir, covered)
}

fn str_col(dir: &FieldDir, name: &str) -> u32 {
    dir.column(name, FieldType::Str).expect(name).column_id
}

fn sorted(mut ids: Vec<u32>) -> Vec<u32> {
    ids.sort_unstable();
    ids
}

/// Rows a logs SQL statement returns over the fixture day through the
/// present, with no read-your-write token: the load has returned, so its
/// commits are listed.
async fn sql_rows(store: &Arc<dyn ObjectStoreBackend>, fx: &Fixture, sql: &str) -> usize {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig::default(),
        1 << 30,
    );
    let req = SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: fx.day,
            end_ns: fx.now_ns + HOUR,
        },
        min_tokens: Vec::new(),
        now_ns: fx.now_ns + 1_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    };
    executor
        .execute(TenantId::new(TENANT).hash(), &req)
        .await
        .expect("logs query executes")
        .output
        .num_rows()
}

fn region_code_six_hours() -> SortDescriptor {
    SortDescriptor {
        bucket_width: SortBucketWidth::SixHours,
        key_columns: vec![
            SortKeyColumn {
                name: "region".to_string(),
                ty: SortKeyType::Str,
            },
            SortKeyColumn {
                name: "code".to_string(),
                ty: SortKeyType::I64,
            },
        ],
    }
}

fn open_footer(object: &[u8]) -> LogFooter {
    footer::open(object).expect("footer")
}

/// The staleness note every write command prints after its outcome line.
macro_rules! note {
    () => {
        "note: a server's log ingest flush can keep the layout it read before this write for up \
         to 60s (its tenant config staleness horizon), and longer while its config reads fail, \
         when it keeps serving the layout it last read; a key it cannot resolve writes no \
         clustering descriptor, counted on ingest_clustering_key_unresolved_total\n"
    };
}

const UPDATED: &str = concat!(
    "updated tenant acme's config record (swapped in place with CasVersion against the version \
     this command read); every other field carried through unchanged\n",
    note!()
);

/// Declare two typed columns, key on both at six hours, narrow the bloom scope
/// to text, load, and read the one object back.
#[tokio::test]
async fn set_key_then_load_writes_sorted_v5_objects() {
    let fx = fixture();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    declare_region_and_code(&store, fx.now_ns).await;

    let printed = set_key(&store, &["region", "code"], fx.now_ns)
        .await
        .expect("set key");
    assert_eq!(
        printed,
        format!(
            "{UPDATED}tenant acme clustering key at generation 1, bucket width 6h, 2 column(s) \
             in key order:\n  region:str\n  code:i64\n"
        )
    );
    let printed = set_scope(&store, BloomScopeArg::Text, fx.now_ns)
        .await
        .expect("set scope");
    assert_eq!(
        printed,
        format!(
            "{UPDATED}tenant acme bloom scope: text\ntenant acme clustering key at generation 2, \
             bucket width 6h, 2 column(s) in key order:\n  region:str\n  code:i64\n"
        )
    );

    let object = load_one(&store, &fx).await;
    assert_eq!(trailer_version(&object), 5);
    let ftr = open_footer(&object);
    assert_eq!(ftr.sort_descriptor, Some(region_code_six_hours()));
    assert_eq!(ftr.clustering_generation, 2);
    assert_eq!(ftr.record_count, ROWS as u64);

    // (stream, bucket, key, ts) order, pinned, and re-derived from the
    // stored rows so the pin is checked against the rule it names.
    assert_eq!(
        bodies_by_stream(&object),
        by_stream_id(&object, API_KEYED, WEB_KEYED)
    );
    let stored = records(&object);
    let key = |r: &LogRecord| {
        (
            r.stream_id,
            r.ts_ns.div_euclid(6 * HOUR),
            str_attr(r, "region").to_string(),
            i64_attr(r, "code"),
            r.ts_ns,
        )
    };
    let mut expected: Vec<_> = stored.iter().map(key).collect();
    expected.sort();
    assert_eq!(stored.iter().map(key).collect::<Vec<_>>(), expected);

    // Text scope: the body and severity text, and no attribute column.
    let (dir, covered) = bloom(&object);
    assert_eq!(covered, sorted(vec![COL_SEVERITY_TEXT, COL_BODY]));
    assert!(!covered.contains(&str_col(&dir, "user")));
    assert!(!covered.contains(&str_col(&dir, "region")));

    // An uncovered string column still answers an equality exactly.
    assert_eq!(
        sql_rows(
            &store,
            &fx,
            "SELECT ts FROM logs WHERE attrs['user'] = 'u-1'"
        )
        .await,
        3
    );
    assert_eq!(
        sql_rows(
            &store,
            &fx,
            "SELECT ts FROM logs WHERE attrs['region'] = 'west'"
        )
        .await,
        4
    );
    assert_eq!(sql_rows(&store, &fx, "SELECT ts FROM logs").await, ROWS);
}

/// The text the catalog refuses a storage-layout write with when the opt-in
/// is absent.
fn writer_cannot_emit(field: &str, setter: &str) -> String {
    format!(
        "cannot set {field} without the storage-layout write opt-in: this build's config record \
         writer stamps format_version 2 by default, and {field} needs a version-3 record, which a \
         reader from a release that predates version 3 refuses. Produce {field} with {setter} \
         given StorageLayoutWrite::ReadersRolledOut, and only once every process reading this \
         bucket runs a release whose reader accepts version 3 (ADR-0066 R1)"
    )
}

/// Accept connections on a loopback port, answering each request 403 so the
/// client stops at once, and count them.
fn counting_endpoint() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            counter.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    (port, seen)
}

fn cli_against(port: u16, args: &[&str]) -> std::process::Output {
    let endpoint = format!("http://127.0.0.1:{port}");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("RAVEL_") {
            cmd.env_remove(key);
        }
    }
    cmd.args([
        "--store",
        "s3",
        "--s3-endpoint",
        &endpoint,
        "--s3-bucket",
        "bucket",
        "--s3-region",
        "us-east-1",
        "--s3-access-key",
        "test",
        "--s3-secret-key",
        "test",
    ])
    .args(args)
    .output()
    .expect("ravel-cli runs")
}

/// Without `--readers-rolled-out` each write command exits 2 and sends the
/// store nothing; with it, the same command line reaches the store.
#[tokio::test]
async fn set_without_the_rollout_flag_writes_nothing() {
    let commands: [&[&str]; 3] = [
        &[
            "clustering-key",
            "set",
            "--tenant",
            TENANT,
            "--column",
            "region",
            "--bucket-width",
            "6h",
        ],
        &["clustering-key", "clear", "--tenant", TENANT],
        &["bloom-scope", "set", "--tenant", TENANT, "--scope", "text"],
    ];
    for args in commands {
        let (port, seen) = counting_endpoint();
        let refused = cli_against(port, args);
        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert_eq!(refused.status.code(), Some(2), "{args:?}: {stderr}");
        assert!(
            stderr.contains("the following required arguments were not provided")
                && stderr.contains("--readers-rolled-out"),
            "{args:?}: {stderr}"
        );
        assert_eq!(seen.load(Ordering::SeqCst), 0, "{args:?} reached the store");

        // Positive control: the endpoint does see a command that has the flag.
        let mut with_flag = args.to_vec();
        with_flag.push("--readers-rolled-out");
        let reached = cli_against(port, &with_flag);
        assert_ne!(reached.status.code(), Some(0), "the endpoint refuses");
        assert_ne!(reached.status.code(), Some(2), "clap accepted the line");
        assert!(seen.load(Ordering::SeqCst) >= 1, "{args:?} never connected");
    }

    // In process, the refusal is the catalog's and comes before any request.
    let (store, metrics) = counted_store();
    let mut out = Vec::new();
    let err = storage_layout::clustering_key_set_to(
        Arc::clone(&store),
        TENANT,
        vec!["region".to_string()],
        BucketWidthArg::SixHours,
        StorageLayoutWrite::Disabled,
        0,
        &mut out,
    )
    .await
    .expect_err("refused");
    assert_eq!(
        err.to_string(),
        writer_cannot_emit(
            "clustering_key",
            "TenantConfig::set_clustering_key or TenantConfig::clear_clustering_key"
        )
    );
    let err = storage_layout::clustering_key_clear_to(
        Arc::clone(&store),
        TENANT,
        StorageLayoutWrite::Disabled,
        0,
        &mut out,
    )
    .await
    .expect_err("refused");
    assert_eq!(
        err.to_string(),
        writer_cannot_emit(
            "clustering_key",
            "TenantConfig::set_clustering_key or TenantConfig::clear_clustering_key"
        )
    );
    let err = storage_layout::bloom_scope_set_to(
        Arc::clone(&store),
        TENANT,
        BloomScopeArg::Text,
        StorageLayoutWrite::Disabled,
        0,
        &mut out,
    )
    .await
    .expect_err("refused");
    assert_eq!(
        err.to_string(),
        writer_cannot_emit("bloom_scope", "TenantConfig::set_bloom_scope")
    );
    assert!(out.is_empty());
    assert_eq!(metrics.snapshot(), StoreMetricsSnapshot::default());
}

/// Run `f` against the store and assert it fails with exactly `expected` and
/// leaves every object and the write counters as they were.
async fn refused_writing_nothing<F, Fut>(
    store: &Arc<dyn ObjectStoreBackend>,
    metrics: &StoreMetrics,
    expected: &str,
    f: F,
) where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<String>>,
{
    let before = contents(store.as_ref()).await;
    let writes_before = writes(&metrics.snapshot());
    let err = f().await.expect_err("refused");
    assert_eq!(err.to_string(), expected);
    assert_eq!(writes(&metrics.snapshot()), writes_before, "{expected}");
    assert_eq!(contents(store.as_ref()).await, before, "{expected}");
}

#[tokio::test]
async fn set_refuses_an_undeclared_column_and_writes_nothing() {
    let (store, metrics) = counted_store();
    declare_region_and_code(&store, 1).await;
    assert_eq!(
        writes(&metrics.snapshot()),
        (1, 0),
        "the declaration's write"
    );

    refused_writing_nothing(
        &store,
        &metrics,
        "clustering key column \"zone\" is not a declared typed attribute column in this \
         tenant's config record: declare it in the record's typed_attr_columns first",
        || set_key(&store, &["region", "zone"], 2),
    )
    .await;
    refused_writing_nothing(
        &store,
        &metrics,
        "clustering key column \"\" is not a declared typed attribute column in this tenant's \
         config record: declare it in the record's typed_attr_columns first",
        || set_key(&store, &[""], 2),
    )
    .await;
    refused_writing_nothing(
        &store,
        &metrics,
        "the clustering key names column \"region\" more than once",
        || set_key(&store, &["region", "code", "region"], 2),
    )
    .await;
    refused_writing_nothing(
        &store,
        &metrics,
        "the clustering key names 5 columns, more than the maximum of 4",
        || set_key(&store, &["region", "code", "a", "b", "c"], 2),
    )
    .await;
    refused_writing_nothing(
        &store,
        &metrics,
        "there is no clustering key to clear: this tenant never set one",
        || clear_key(&store, 2),
    )
    .await;
    assert_eq!(writes(&metrics.snapshot()), (1, 0));

    // The same store takes a declared key, so each refusal above is the
    // key's and not the store's.
    set_key(&store, &["region", "code"], 3)
        .await
        .expect("a declared key is set");
    assert_eq!(writes(&metrics.snapshot()), (2, 0));
}

#[tokio::test]
async fn clear_then_load_writes_no_descriptor_and_the_generation() {
    let fx = fixture();
    let (store, metrics) = counted_store();
    declare_region_and_code(&store, fx.now_ns).await;
    set_key(&store, &["region", "code"], fx.now_ns)
        .await
        .expect("set key");
    let printed = clear_key(&store, fx.now_ns).await.expect("clear key");
    assert_eq!(
        printed,
        format!(
            "{UPDATED}tenant acme has no clustering key at generation 2 (cleared, or never set \
             and given a generation by a bloom scope change or by a typed attribute column change \
         under the undeclared scope)\n"
        )
    );
    assert_eq!(
        writes(&metrics.snapshot()),
        (3, 0),
        "declare, set and clear, one PUT each"
    );

    let object = load_one(&store, &fx).await;
    assert_eq!(trailer_version(&object), 5);
    let ftr = open_footer(&object);
    assert_eq!(ftr.sort_descriptor, None);
    assert_eq!(ftr.clustering_generation, 2);
    assert_eq!(
        bodies_by_stream(&object),
        by_stream_id(&object, API_UNKEYED, WEB_UNKEYED)
    );

    // A second clear has nothing to clear and writes nothing: no PUT or
    // DELETE, the record's bytes unchanged, and it still shows cleared at
    // generation 2.
    let (_, record_before) = config_record(store.as_ref()).await;
    refused_writing_nothing(
        &store,
        &metrics,
        "there is no clustering key to clear: this tenant's clustering key is already absent at \
         generation 2",
        || clear_key(&store, fx.now_ns),
    )
    .await;
    let (_, record_after) = config_record(store.as_ref()).await;
    assert_eq!(record_after, record_before);
    let mut shown = Vec::new();
    storage_layout::clustering_key_show_to(Arc::clone(&store), TENANT, &mut shown)
        .await
        .expect("show");
    assert_eq!(
        String::from_utf8(shown).expect("utf8"),
        "tenant acme has no clustering key at generation 2 (cleared, or never set and given a \
         generation by a bloom scope change or by a typed attribute column change \
         under the undeclared scope)\n"
    );
}

#[tokio::test]
async fn bloom_scope_set_reaches_the_object() {
    let fx = fixture();
    let (store, metrics) = counted_store();
    declare_region_and_code(&store, fx.now_ns).await;
    let printed = set_scope(&store, BloomScopeArg::Undeclared, fx.now_ns)
        .await
        .expect("set scope");
    assert_eq!(
        printed,
        format!(
            "{UPDATED}tenant acme bloom scope: undeclared\ntenant acme has no clustering key at \
             generation 1 (cleared, or never set and given a generation by a bloom scope \
             change or by a typed attribute column change under the undeclared scope)\n"
        )
    );
    // The stored scope again writes nothing and says so.
    let writes_before = writes(&metrics.snapshot());
    let contents_before = contents(store.as_ref()).await;
    let printed = set_scope(&store, BloomScopeArg::Undeclared, fx.now_ns)
        .await
        .expect("same scope");
    assert_eq!(writes(&metrics.snapshot()), writes_before);
    assert_eq!(contents(store.as_ref()).await, contents_before);
    assert_eq!(
        printed,
        "tenant acme bloom scope is already undeclared; nothing written\ntenant acme bloom \
         scope: undeclared\ntenant acme has no clustering key at generation 1 (cleared, or \
         never set and given a generation by a bloom scope change or by a typed attribute column change \
         under the undeclared scope)\n"
    );

    let object = load_one(&store, &fx).await;
    assert_eq!(trailer_version(&object), 5);
    let ftr = open_footer(&object);
    assert_eq!(ftr.sort_descriptor, None);
    assert_eq!(ftr.clustering_generation, 1);
    // Undeclared: `user` keeps its filter; the declared `region` loses its.
    let (dir, covered) = bloom(&object);
    assert_eq!(
        covered,
        sorted(vec![COL_SEVERITY_TEXT, COL_BODY, str_col(&dir, "user")])
    );
    assert!(!covered.contains(&str_col(&dir, "region")));
    assert_eq!(
        sql_rows(
            &store,
            &fx,
            "SELECT ts FROM logs WHERE attrs['region'] = 'east'"
        )
        .await,
        8
    );

    // A tenant with no record gets one carrying only the scope.
    let fresh: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let printed = set_scope(&fresh, BloomScopeArg::Text, fx.now_ns)
        .await
        .expect("set scope");
    assert_eq!(
        printed,
        concat!(
            "created the config record for tenant acme (it had none), with lifecycle_state=active \
             and no override but this command's\n",
            note!(),
            "tenant acme bloom scope: text\ntenant acme has no clustering key at generation 1 \
             (cleared, or never set and given a generation by a bloom scope change or by a typed attribute column change \
         under the undeclared scope)\n"
        )
    );
}

/// Under the undeclared scope, declaring a column changes which string
/// columns the filter covers, so the declaration takes a clustering
/// generation and the next object names it. The declared column here is an
/// i64 while the loaded `user` values are strings: the writer matches by name,
/// so `user` loses its filter all the same.
#[tokio::test]
async fn an_undeclared_scope_declaration_takes_a_generation_the_object_names() {
    let fx = fixture();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    declare_region_and_code(&store, fx.now_ns).await;
    set_scope(&store, BloomScopeArg::Undeclared, fx.now_ns)
        .await
        .expect("set scope");
    typed_attr_column::set(
        Arc::clone(&store),
        TENANT,
        &[
            "region:str".to_string(),
            "code:i64".to_string(),
            "user:i64".to_string(),
        ],
        fx.now_ns,
    )
    .await
    .expect("declare user");
    let shown = {
        let mut out = Vec::new();
        storage_layout::clustering_key_show_to(Arc::clone(&store), TENANT, &mut out)
            .await
            .expect("show");
        String::from_utf8(out).expect("utf8")
    };
    assert_eq!(
        shown,
        "tenant acme has no clustering key at generation 2 (cleared, or never set and given a \
         generation by a bloom scope change or by a typed attribute column change \
         under the undeclared scope)\n"
    );

    let object = load_one(&store, &fx).await;
    let ftr = open_footer(&object);
    assert_eq!(ftr.sort_descriptor, None);
    assert_eq!(ftr.clustering_generation, 2);
    let (dir, covered) = bloom(&object);
    assert_eq!(covered, sorted(vec![COL_SEVERITY_TEXT, COL_BODY]));
    assert!(!covered.contains(&str_col(&dir, "user")));
    assert_eq!(
        sql_rows(
            &store,
            &fx,
            "SELECT ts FROM logs WHERE attrs['user'] = 'u-1'"
        )
        .await,
        3
    );
}

/// A store that, on the `nth` GET of a tenant config record, first overwrites
/// that record with `moved` (another writer's record), so a command's read
/// and its write see different records.
struct MovingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    nth: usize,
    gets: AtomicUsize,
    moved: bytes::Bytes,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for MovingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if key.ends_with("/config") && self.gets.fetch_add(1, Ordering::SeqCst) + 1 == self.nth {
            self.inner
                .put(key, self.moved.clone(), PutOptions::default())
                .await?;
        }
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// The one config record in `store`.
async fn config_record(store: &dyn ObjectStoreBackend) -> (String, bytes::Bytes) {
    let mut records: Vec<_> = contents(store)
        .await
        .into_iter()
        .filter(|(key, _)| key.ends_with("/config"))
        .collect();
    assert_eq!(records.len(), 1);
    records.pop().expect("one record")
}

/// A record another writer changes between a write command's read and its
/// write refuses the command with the catalog's re-read-and-retry error, and
/// the other writer's record stands. Each command reads the record once and
/// writes against that read's version.
#[tokio::test]
async fn a_record_moved_after_the_read_refuses_the_write() {
    // The other writer's record: region, code and user declared.
    let theirs: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    declare_region_and_code(&theirs, 1).await;
    typed_attr_column::set(
        Arc::clone(&theirs),
        TENANT,
        &[
            "region:str".to_string(),
            "code:i64".to_string(),
            "user:str".to_string(),
        ],
        2,
    )
    .await
    .expect("declare user");
    let (_, moved) = config_record(theirs.as_ref()).await;

    for command in [
        "clustering-key set",
        "clustering-key clear",
        "bloom-scope set",
    ] {
        let inner: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        declare_region_and_code(&inner, 1).await;
        if command == "clustering-key clear" {
            set_key(&inner, &["region"], 1).await.expect("set key");
        }
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MovingStore {
            inner: Arc::clone(&inner),
            nth: 2,
            gets: AtomicUsize::new(0),
            moved: moved.clone(),
        });
        let result = match command {
            "clustering-key set" => set_key(&store, &["region"], 3).await,
            "clustering-key clear" => clear_key(&store, 3).await,
            _ => set_scope(&store, BloomScopeArg::Text, 3).await,
        };
        let (key, stored) = config_record(inner.as_ref()).await;
        let err = result.expect_err(command);
        assert_eq!(
            err.to_string(),
            format!(
                "a concurrent write changed config record {key:?} since this one read it (CAS \
                 precondition failed): re-read and retry rather than overwrite the other write"
            ),
            "{command}"
        );
        assert_eq!(stored, moved, "{command}: the other writer's record stands");
    }
}
