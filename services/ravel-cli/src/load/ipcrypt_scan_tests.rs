//! ADR-2993 acceptance for `ravel-cli load`: distinct addresses loaded under
//! an IPCrypt policy through every label-building point (a metric label, log
//! resource and record attributes on both the columnar and the row path, and
//! a span's resource attribute, attribute and attributes-map entry) never
//! reach the bucket in any form, and each one's ciphertext does. A control
//! run with no policy finds every raw address, so the scan can see them.
//!
//! The marker refusals run here too: they are checked before the first write.

use std::io::Read;
use std::net::Ipv4Addr;

use arrow::array::{Float64Array, MapBuilder, StringBuilder};
use ravel_ipcrypt::marker::{IPCRYPT_POLICY_KEY, check_marker};
use ravel_ipcrypt::policy::{Variant, parse_policy_file};
use ravel_ipcrypt::render::RenderOutcome;
use ravel_ipcrypt::rules::EpochKey;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Op, Rule as FaultRule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, list_all};

use super::test_support::*;
use super::*;
use crate::ipcrypt::sanitize_rule_key;

const TENANT: &str = "acme";
const DEPLOYMENT_KEY: [u8; 32] = [0x5a; 32];

const POLICY: &str = r#"
[[defaults.rules]]
key = "client.ip"
variant = "deterministic"

[[defaults.rules]]
key = "host.ip"
variant = "prefix"

[[defaults.rules]]
key = "peer.ip"
variant = "deterministic"
"#;

/// Last octet of each case's address in 198.51.100.0/24 (RFC 5737
/// TEST-NET-2). Every octet has three digits, so no case's text form is a
/// substring of another's.
mod octet {
    pub const METRIC_LABEL: u8 = 101;
    pub const COLUMNAR_LOG_RESOURCE: u8 = 102;
    pub const COLUMNAR_LOG_RECORD: u8 = 103;
    pub const ROW_LOG_RESOURCE: u8 = 104;
    pub const ROW_LOG_RECORD: u8 = 105;
    pub const SPAN_RESOURCE: u8 = 106;
    pub const SPAN_ATTR: u8 = 107;
    pub const SPAN_ATTRS_MAP: u8 = 108;
}

/// Every case: its name, the rule key that covers it, and its octet.
const CASES: [(&str, &str, u8); 8] = [
    ("metric label", "client.ip", octet::METRIC_LABEL),
    (
        "columnar log resource",
        "host.ip",
        octet::COLUMNAR_LOG_RESOURCE,
    ),
    (
        "columnar log record",
        "client.ip",
        octet::COLUMNAR_LOG_RECORD,
    ),
    ("row log resource", "host.ip", octet::ROW_LOG_RESOURCE),
    ("row log record", "client.ip", octet::ROW_LOG_RECORD),
    ("span resource", "host.ip", octet::SPAN_RESOURCE),
    ("span attribute", "client.ip", octet::SPAN_ATTR),
    ("span attributes map", "peer.ip", octet::SPAN_ATTRS_MAP),
];

fn addr(octet: u8) -> String {
    Ipv4Addr::new(198, 51, 100, octet).to_string()
}

fn policy() -> IpcryptPolicy {
    let file = parse_policy_file(POLICY, sanitize_rule_key).expect("policy parses");
    IpcryptPolicy::new(file, Some(&DEPLOYMENT_KEY)).expect("policy loads")
}

/// What the rule on `key` stores for the case at `octet`, derived from the
/// deployment key directly rather than through the rules the load ran.
fn ciphertext(key: &str, octet: u8) -> String {
    let variant = if key == "host.ip" {
        Variant::Prefix
    } else {
        Variant::Deterministic
    };
    let epoch_key = EpochKey::new(&DEPLOYMENT_KEY, &TenantId::new(TENANT).hash(), variant, 1)
        .expect("key derives");
    let (stored, outcome) = epoch_key.encrypt(&addr(octet));
    assert_eq!(outcome, RenderOutcome::Encrypted);
    assert_ne!(stored, addr(octet));
    stored
}

fn write_batch(dir: &Path, name: &str, batch: &RecordBatch) -> std::path::PathBuf {
    use parquet::arrow::ArrowWriter;
    let pq = dir.join(name);
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
    pq
}

fn write_mapping(dir: &Path, name: &str, toml: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, toml).expect("write mapping");
    path
}

const LOGS_MAPPING: &str = "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n\n\
     [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
     [[resource_attribute]]\nkey = \"host.ip\"\ncolumn = \"host_ip\"\ntype = \"str\"\n\n\
     [[attribute]]\nkey = \"client.ip\"\ncolumn = \"client_ip\"\ntype = \"str\"\n";

fn logs_batch(resource: u8, record: u8) -> RecordBatch {
    batch(vec![
        ("ts", i64_col(vec![NOW_NS])),
        ("body", str_col(vec!["request served"])),
        ("svc", str_col(vec!["api"])),
        ("host_ip", str_col(vec![addr(resource).as_str()])),
        ("client_ip", str_col(vec![addr(record).as_str()])),
    ])
}

/// Drive one CLI load (the production columnar path for logs) through the
/// entry point that reads the marker and builds the rules.
async fn cli_load(
    store: Arc<dyn ObjectStoreBackend>,
    pq: &Path,
    mapping: &Path,
    signal: SignalArg,
    policy: &IpcryptPolicy,
) -> anyhow::Result<()> {
    run_fold_warning_to(
        store,
        pq,
        TENANT,
        mapping,
        signal,
        1,
        1_000,
        0,
        Some(1),
        Some(1),
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        None,
        None,
        RlogZstdLevel::DEFAULT,
        LoadMemoryRequest::Unbudgeted,
        false,
        policy,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
        &mut Vec::new(),
    )
    .await
}

/// Load one row per case through every label-building point into `store`.
/// `policy` drives the CLI entry point; the row path, which only tests can
/// select, takes the same policy's rules for the tenant.
async fn load_every_path(store: &Arc<dyn ObjectStoreBackend>, policy: &IpcryptPolicy) {
    let dir = tempfile::tempdir().expect("tempdir");

    let metrics = batch(vec![
        ("ts", i64_col(vec![NOW_NS])),
        ("value", Arc::new(Float64Array::from(vec![1.5])) as ArrayRef),
        ("cip", str_col(vec![addr(octet::METRIC_LABEL).as_str()])),
    ]);
    let pq = write_batch(dir.path(), "metrics.parquet", &metrics);
    let mapping = write_mapping(
        dir.path(),
        "metrics.toml",
        "[metrics]\nname = \"requests\"\nvalue_column = \"value\"\nts_column = \"ts\"\n\
         ts_unit = \"nanos\"\nkind = \"gauge\"\n\n\
         [[metrics.label]]\nname = \"client_ip\"\ncolumn = \"cip\"\n",
    );
    cli_load(Arc::clone(store), &pq, &mapping, SignalArg::Metrics, policy)
        .await
        .expect("metrics load");

    let pq = write_batch(
        dir.path(),
        "columnar_logs.parquet",
        &logs_batch(octet::COLUMNAR_LOG_RESOURCE, octet::COLUMNAR_LOG_RECORD),
    );
    let mapping = write_mapping(dir.path(), "logs.toml", LOGS_MAPPING);
    cli_load(Arc::clone(store), &pq, &mapping, SignalArg::Logs, policy)
        .await
        .expect("columnar logs load");

    let pq = write_batch(
        dir.path(),
        "row_logs.parquet",
        &logs_batch(octet::ROW_LOG_RESOURCE, octet::ROW_LOG_RECORD),
    );
    let logs_mapping = parse_mapping(LOGS_MAPPING).expect("logs mapping");
    load_instrumented_at(
        Arc::clone(store),
        &pq,
        TENANT,
        &logs_mapping,
        1,
        1_000,
        0,
        Some(1),
        Some(1),
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        None,
        Some(DEFAULT_MAX_FLUSH_DELAY),
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)) as Arc<dyn Clock>,
        LoadPath::Row,
        None,
        None,
        RlogZstdLevel::DEFAULT,
        None,
        false,
        Arc::new(policy.rules_for(TENANT).expect("rules resolve")),
    )
    .await
    .expect("row logs load");

    let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    attrs.keys().append_value("peer.ip");
    attrs.values().append_value(addr(octet::SPAN_ATTRS_MAP));
    attrs.append(true).expect("map entry");
    let spans = batch(vec![
        (
            "trace_id",
            Arc::new(arrow::array::BinaryArray::from(vec![[1u8; 16].as_slice()])) as ArrayRef,
        ),
        (
            "span_id",
            Arc::new(arrow::array::BinaryArray::from(vec![[2u8; 8].as_slice()])) as ArrayRef,
        ),
        ("name", str_col(vec!["op"])),
        ("start_ns", i64_col(vec![NOW_NS])),
        ("end_ns", i64_col(vec![NOW_NS])),
        ("svc", str_col(vec!["api"])),
        (
            "host_ip",
            str_col(vec![addr(octet::SPAN_RESOURCE).as_str()]),
        ),
        ("client_ip", str_col(vec![addr(octet::SPAN_ATTR).as_str()])),
        ("attrs", Arc::new(attrs.finish()) as ArrayRef),
    ]);
    let pq = write_batch(dir.path(), "spans.parquet", &spans);
    let mapping = write_mapping(
        dir.path(),
        "spans.toml",
        "[spans]\ntrace_id_column = \"trace_id\"\nspan_id_column = \"span_id\"\n\
         name_column = \"name\"\nstart_ts_column = \"start_ns\"\nstart_ts_unit = \"nanos\"\n\
         end_ts_column = \"end_ns\"\nend_ts_unit = \"nanos\"\nattrs_map_column = \"attrs\"\n\n\
         [[spans.resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
         [[spans.resource_attribute]]\nkey = \"host.ip\"\ncolumn = \"host_ip\"\ntype = \"str\"\n\n\
         [[spans.attribute]]\nkey = \"client.ip\"\ncolumn = \"client_ip\"\ntype = \"str\"\n",
    );
    cli_load(Arc::clone(store), &pq, &mapping, SignalArg::Spans, policy)
        .await
        .expect("spans load");
}

/// One stored object: its key and its bytes, plus every zstd frame inside
/// it, decompressed recursively.
struct Stored {
    key: String,
    blobs: Vec<Vec<u8>>,
}

impl Stored {
    fn contains(&self, needle: &[u8]) -> bool {
        self.blobs.iter().any(|blob| find(blob, needle).is_some())
    }
}

async fn stored_objects(store: &Arc<dyn ObjectStoreBackend>) -> Vec<Stored> {
    let objects = list_all(store.as_ref(), "")
        .await
        .expect("list every object");
    let mut stored = Vec::new();
    for object in objects {
        let data = store
            .get(&object.key, GetRange::Full)
            .await
            .expect("read object")
            .data
            .to_vec();
        let mut blobs = Vec::new();
        expand_zstd(&data, 0, &mut blobs);
        blobs.push(data);
        stored.push(Stored {
            key: object.key,
            blobs,
        });
    }
    stored
}

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
const MAX_FRAME_BYTES: u64 = 64 * 1024 * 1024;

/// Decodes one zstd frame at every offset carrying the frame magic, so a
/// frame followed by other bytes (a footer, the next section) still decodes.
fn expand_zstd(data: &[u8], depth: usize, out: &mut Vec<Vec<u8>>) {
    if depth > 3 {
        return;
    }
    let mut at = 0;
    while let Some(offset) = find(&data[at..], &ZSTD_MAGIC) {
        let start = at + offset;
        if let Some(decoded) = decode_frame(&data[start..]) {
            expand_zstd(&decoded, depth + 1, out);
            out.push(decoded);
        }
        at = start + 1;
    }
}

fn decode_frame(bytes: &[u8]) -> Option<Vec<u8>> {
    let decoder = zstd::stream::read::Decoder::new(bytes).ok()?.single_frame();
    let mut decoded = Vec::new();
    decoder
        .take(MAX_FRAME_BYTES)
        .read_to_end(&mut decoded)
        .ok()?;
    Some(decoded)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The forms a stored address could take: text, 4-byte binary, 16-byte
/// v4-mapped binary, and the lowercase hex of each of those three.
fn address_forms(octet: u8) -> Vec<(&'static str, Vec<u8>)> {
    let text = addr(octet);
    let v4 = [198, 51, 100, octet];
    let mut mapped = [0u8; 16];
    mapped[10] = 0xff;
    mapped[11] = 0xff;
    mapped[12..].copy_from_slice(&v4);
    vec![
        ("text", text.as_bytes().to_vec()),
        ("ipv4 binary", v4.to_vec()),
        ("v4-mapped binary", mapped.to_vec()),
        ("text hex", hex(text.as_bytes()).into_bytes()),
        ("ipv4 hex", hex(&v4).into_bytes()),
        ("v4-mapped hex", hex(&mapped).into_bytes()),
    ]
}

/// The forms a stored ciphertext could take: its text and the hex of its
/// text.
fn ciphertext_forms(ciphertext: &str) -> [Vec<u8>; 2] {
    [
        ciphertext.as_bytes().to_vec(),
        hex(ciphertext.as_bytes()).into_bytes(),
    ]
}

fn holding<'a>(stored: &'a [Stored], needle: &[u8]) -> Vec<&'a str> {
    stored
        .iter()
        .filter(|object| object.contains(needle))
        .map(|object| object.key.as_str())
        .collect()
}

#[tokio::test]
async fn no_address_reaches_the_bucket_under_the_policy_and_each_ciphertext_does() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let policy = policy();
    load_every_path(&store, &policy).await;
    let stored = stored_objects(&store).await;
    assert!(
        stored.iter().any(|object| object.key.contains("/l0/")),
        "the loads wrote no data object: {:?}",
        stored.iter().map(|o| o.key.as_str()).collect::<Vec<_>>()
    );

    let mut leaks = Vec::new();
    let mut missing = Vec::new();
    for (case, key, octet) in CASES {
        for (form, needle) in address_forms(octet) {
            let objects = holding(&stored, &needle);
            if !objects.is_empty() {
                leaks.push(format!("{case}: {} as {form} in {objects:?}", addr(octet)));
            }
        }
        let expected = ciphertext(key, octet);
        if !ciphertext_forms(&expected)
            .iter()
            .any(|form| !holding(&stored, form).is_empty())
        {
            missing.push(format!("{case}: ciphertext {expected} of {}", addr(octet)));
        }
    }
    assert!(leaks.is_empty(), "raw addresses stored: {leaks:#?}");
    assert!(missing.is_empty(), "ciphertexts not stored: {missing:#?}");
}

/// The control: the same loads with no policy store every raw address in
/// text form, so the scan above can see each case it says is absent.
#[tokio::test]
async fn with_no_policy_every_address_is_stored() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load_every_path(&store, &IpcryptPolicy::empty()).await;
    let stored = stored_objects(&store).await;
    let unseen: Vec<String> = CASES
        .iter()
        .filter(|(_, _, octet)| {
            address_forms(*octet)
                .iter()
                .all(|(_, needle)| holding(&stored, needle).is_empty())
        })
        .map(|(case, _, octet)| format!("{case}: {}", addr(*octet)))
        .collect();
    assert!(unseen.is_empty(), "the control did not store: {unseen:?}");
    assert!(
        stored.iter().all(|o| o.key != IPCRYPT_POLICY_KEY),
        "an empty policy on an unmarked bucket writes no marker"
    );
}

/// A logs fixture on disk for the refusal tests.
fn refusal_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = write_batch(
        dir.path(),
        "logs.parquet",
        &logs_batch(octet::COLUMNAR_LOG_RESOURCE, octet::COLUMNAR_LOG_RECORD),
    );
    let mapping = write_mapping(dir.path(), "logs.toml", LOGS_MAPPING);
    (dir, pq, mapping)
}

#[tokio::test]
async fn a_policy_creates_the_marker_on_an_unmarked_bucket() {
    let (_dir, pq, mapping) = refusal_fixture();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let policy = policy();
    cli_load(Arc::clone(&store), &pq, &mapping, SignalArg::Logs, &policy)
        .await
        .expect("load");
    let (marker, _) = ravel_ipcrypt::marker::read_marker(store.as_ref())
        .await
        .expect("read marker")
        .expect("the load created the marker");
    assert_eq!(marker.fingerprint, policy.fingerprint());
}

#[tokio::test]
async fn a_marker_mismatch_refuses_before_any_write_and_names_both_fingerprints() {
    let (_dir, pq, mapping) = refusal_fixture();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let other = [7u8; 32];
    check_marker(store.as_ref(), other)
        .await
        .expect("seed a marker for another policy");
    let policy = policy();
    let err = cli_load(Arc::clone(&store), &pq, &mapping, SignalArg::Logs, &policy)
        .await
        .expect_err("a different policy must be refused");
    let message = format!("{err:#}");
    assert!(message.contains(&hex(&other)), "{message}");
    assert!(message.contains(&hex(&policy.fingerprint())), "{message}");
    assert!(list_data_objects(store.as_ref()).await.is_empty());
}

#[tokio::test]
async fn no_flag_on_a_bucket_with_a_policy_marker_refuses() {
    let (_dir, pq, mapping) = refusal_fixture();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let policy = policy();
    check_marker(store.as_ref(), policy.fingerprint())
        .await
        .expect("seed the policy's marker");
    let err = cli_load(
        Arc::clone(&store),
        &pq,
        &mapping,
        SignalArg::Logs,
        &IpcryptPolicy::empty(),
    )
    .await
    .expect_err("a load with no policy on a marked bucket must be refused");
    let message = format!("{err:#}");
    assert!(message.contains(&hex(&policy.fingerprint())), "{message}");
    assert!(list_data_objects(store.as_ref()).await.is_empty());
}

/// A marker read that fails is not an absent marker: the load refuses, and
/// neither the marker nor any data object is written.
#[tokio::test]
async fn a_marker_read_failure_refuses_and_is_not_taken_for_absent() {
    let (_dir, pq, mapping) = refusal_fixture();
    let faulty = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(
            FaultRule::new(Op::Get, ScriptedFault::Transient("marker read".into()))
                .with_key_contains(IPCRYPT_POLICY_KEY),
        ),
    ));
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
    for policy in [policy(), IpcryptPolicy::empty()] {
        let err = cli_load(Arc::clone(&store), &pq, &mapping, SignalArg::Logs, &policy)
            .await
            .expect_err("a failed marker read must refuse");
        assert!(format!("{err:#}").contains("refusing to load"), "{err:#}");
    }
    assert_eq!(faulty.fault_count(Op::Get, FaultKind::Transient), 2);
    let inner = faulty.inner();
    let keys: Vec<String> = list_all(inner, "")
        .await
        .expect("list")
        .into_iter()
        .map(|o| o.key)
        .collect();
    assert!(
        !keys
            .iter()
            .any(|k| k == IPCRYPT_POLICY_KEY || k.contains("/l0/")),
        "{keys:?}"
    );
}
