//! ADR-2677 decision 1, issue #2679: `ravel-cli load --fold-after-load` seals
//! the hours the load wrote, so the first query after the load resolves them
//! from the folded snapshot instead of listing them.
//!
//! The load runs through `ravel_cli::load::load_with_fold_after_load` (the
//! logs entry point `--fold-after-load` dispatches to) with an injected clock
//! fixed inside hour `H`, ten-odd minutes in, where the seal margin alone seals
//! only `H - 3`. The query runs afterwards through the real `/api/v1/sql`
//! handler with its own clock in hour `H + 1`, behind a store that records
//! every LIST, and is compared against a control load of the same file without
//! the flag.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::load::{self, LoadReport, Mapping};
use ravel_commit::keys;
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_types::TenantId;
use serde_json::Value;
use tower::ServiceExt;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// The loader's clock: 2023-11-14T22:13:20Z, thirteen minutes into its hour.
const CLOCK_NS: i64 = 1_700_000_000_000_000_000;
const HOUR: u32 = (CLOCK_NS / NS_PER_HOUR) as u32;

/// The load's start-of-run `now_ns`, one nanosecond before the clock's
/// reading, so a fold that used it instead of a fresh clock reading is told
/// apart by HEAD's `created_unix_ns`.
const LOAD_NOW_NS: i64 = CLOCK_NS - 1;

/// The query's clock: a minute into hour `H + 1`, so the default window (the
/// hour ending at now) covers the loaded rows and the resolve's listing runs
/// to `H + 1`.
const QUERY_NOW_NS: i64 = (HOUR as i64 + 1) * NS_PER_HOUR + 60_000_000_000;

/// How long every PUT of the logs HEAD is held. Only a fold writes it, so the
/// fold takes at least this long and an `elapsed` that left the fold out
/// would come in under it.
const HEAD_PUT_DELAY: Duration = Duration::from_millis(500);

const TENANT: &str = "acme";
const TOKEN: &str = "acme-token";
const SHARDS: u32 = 2;
const ROWS: usize = 40;

struct FixedClock(Arc<AtomicI64>);

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Records every LIST prefix and every key a LIST returned, and holds each
/// PUT of a key ending in `head_suffix` for [`HEAD_PUT_DELAY`].
struct RecordingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    head_suffix: String,
    prefixes: Mutex<Vec<String>>,
    listed_keys: Mutex<Vec<String>>,
}

impl RecordingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
        let head_suffix = format!("/catalog/{}/HEAD", ravel_types::Signal::Logs.key_prefix());
        Arc::new(Self {
            inner,
            head_suffix,
            prefixes: Mutex::new(Vec::new()),
            listed_keys: Mutex::new(Vec::new()),
        })
    }

    fn reset(&self) {
        self.prefixes.lock().unwrap().clear();
        self.listed_keys.lock().unwrap().clear();
    }

    fn list_count(&self) -> usize {
        self.prefixes.lock().unwrap().len()
    }

    fn listed_keys(&self) -> Vec<String> {
        self.listed_keys.lock().unwrap().clone()
    }

    fn note(&self, prefix: &str, page: &ListPage) {
        self.prefixes.lock().unwrap().push(prefix.to_string());
        let mut keys = self.listed_keys.lock().unwrap();
        keys.extend(page.objects.iter().map(|meta| meta.key.clone()));
    }
}

#[async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(&self, k: &str, d: Bytes, o: PutOptions) -> Result<PutOutcome, StoreError> {
        if k.ends_with(&self.head_suffix) {
            tokio::time::sleep(HEAD_PUT_DELAY).await;
        }
        self.inner.put(k, d, o).await
    }
    async fn get(&self, k: &str, r: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(k, r).await
    }
    async fn head(&self, k: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(k).await
    }
    async fn list(&self, p: &str, t: Option<PageToken>) -> Result<ListPage, StoreError> {
        let page = self.inner.list(p, t).await?;
        self.note(p, &page);
        Ok(page)
    }
    async fn list_after(
        &self,
        p: &str,
        start_after: Option<&str>,
        t: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        let page = self.inner.list_after(p, start_after, t).await?;
        self.note(p, &page);
        Ok(page)
    }
    async fn list_delimited(&self, p: &str) -> Result<DelimitedList, StoreError> {
        self.prefixes.lock().unwrap().push(p.to_string());
        self.inner.list_delimited(p).await
    }
    async fn delete(&self, k: &str) -> Result<(), StoreError> {
        self.inner.delete(k).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

fn write_fixture() -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("logs.parquet");
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "ts",
            Arc::new(Int64Array::from(
                (0..ROWS).map(|r| CLOCK_NS + r as i64).collect::<Vec<_>>(),
            )),
        ),
        (
            "body",
            Arc::new(StringArray::from(
                (0..ROWS).map(|r| format!("row{r}")).collect::<Vec<_>>(),
            )),
        ),
        (
            "svc",
            Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|r| format!("svc{}", r % 4))
                    .collect::<Vec<_>>(),
            )),
        ),
    ];
    let batch = RecordBatch::try_from_iter(columns).expect("record batch");
    let file = std::fs::File::create(&parquet_path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    let mapping = load::parse_mapping(
        "ts_column = \"ts\"\n\
         ts_unit = \"nanos\"\n\
         body_column = \"body\"\n\
         \n\
         [[resource_attribute]]\n\
         key = \"service.name\"\n\
         column = \"svc\"\n\
         type = \"str\"\n",
    )
    .expect("valid mapping");
    (dir, parquet_path, mapping)
}

async fn run_load(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    mapping: &Mapping,
    fold_after_load: bool,
) -> LoadReport {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock(Arc::new(AtomicI64::new(CLOCK_NS))));
    let result = if fold_after_load {
        load::load_with_fold_after_load(
            store,
            parquet_path,
            TENANT,
            mapping,
            SHARDS,
            10,
            None,
            1,
            LOAD_NOW_NS,
            clock,
        )
        .await
    } else {
        load::load(
            store,
            parquet_path,
            TENANT,
            mapping,
            SHARDS,
            10,
            None,
            1,
            LOAD_NOW_NS,
            clock,
        )
        .await
    };
    result.expect("load succeeds")
}

fn build_app(store: Arc<dyn ObjectStoreBackend>) -> Router {
    let config = CatalogConfig {
        shard_count: SHARDS,
        ..CatalogConfig::default()
    };
    let catalog = Arc::new(Catalog::new(Arc::clone(&store), config).expect("catalog"));
    let tokens = HashMap::from([(TOKEN.to_string(), TenantId::new(TENANT))]);
    let mut state = ravel_server::query::build_sql_state(
        catalog,
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        None,
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    )
    .expect("build_sql_state");
    state.clock = Arc::new(FixedClock(Arc::new(AtomicI64::new(QUERY_NOW_NS))));
    ravel_server::sql::router(state)
}

async fn count_rows(app: &Router) -> Value {
    let payload =
        serde_json::json!({ "query": "SELECT COUNT(*) FROM logs", "timeout": 60.0 }).to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::from(payload))
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&bytes).expect("JSON body");
    assert_eq!(status, StatusCode::OK, "query failed: {value}");
    value
}

fn count_of(value: &Value) -> i64 {
    value["data"]["rows"][0][0]
        .as_i64()
        .unwrap_or_else(|| panic!("no count in {value}"))
}

fn resolve_lists(value: &Value) -> u64 {
    value["stats"]["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .find(|phase| phase["phase"] == "resolve")
        .unwrap_or_else(|| panic!("no resolve phase in {value}"))["s3ListRequests"]
        .as_u64()
        .unwrap_or_else(|| panic!("no resolve s3ListRequests in {value}"))
}

fn unfolded_segments(value: &Value) -> u64 {
    value["stats"]["io"]["unfoldedSegmentsResolved"]
        .as_u64()
        .unwrap_or_else(|| panic!("no io.unfoldedSegmentsResolved in {value}"))
}

/// Listed keys that carry the loaded hour `H` in their path: commit records of
/// an hour the fold sealed, which a resolve from the snapshot never lists.
fn sealed_hour_keys(store: &RecordingStore) -> Vec<String> {
    let marker = format!("/{}/", keys::ingest_hour_string(HOUR));
    store
        .listed_keys()
        .into_iter()
        .filter(|key| key.contains(&marker))
        .collect()
}

#[tokio::test]
async fn load_with_fold_after_load_resolves_from_the_snapshot_through_sql() {
    let (_dir, parquet_path, mapping) = write_fixture();

    // The control: the same file without the flag, queried at the same now.
    let control_store = RecordingStore::new(Arc::new(MemoryStore::new()));
    let control_report = run_load(
        Arc::clone(&control_store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        false,
    )
    .await;
    assert_eq!(control_report.rows_processed, ROWS as u64);
    assert_eq!(control_report.fold, None, "no flag, no fold figures");
    control_store.reset();
    let control = count_rows(&build_app(
        Arc::clone(&control_store) as Arc<dyn ObjectStoreBackend>
    ))
    .await;
    assert_eq!(count_of(&control), ROWS as i64, "control count: {control}");
    assert!(
        unfolded_segments(&control) > 0,
        "the control resolves its segments from the listing: {control}"
    );
    assert!(
        !sealed_hour_keys(&control_store).is_empty(),
        "the control lists hour {HOUR}'s commit records"
    );

    // The load under test.
    let store = RecordingStore::new(Arc::new(MemoryStore::new()));
    let report = run_load(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        &parquet_path,
        &mapping,
        true,
    )
    .await;
    assert_eq!(report.rows_processed, ROWS as u64);
    assert!(
        report.tokens.iter().all(|t| t.ingest_hour_bucket == HOUR),
        "every object lands in the clock's hour {HOUR}"
    );
    let fold = report
        .fold
        .clone()
        .expect("--fold-after-load reports a fold");
    assert!(!fold.no_op, "the fold sealed the loaded hour: {fold:?}");
    assert_eq!(fold.seal_through_hour, Some(HOUR), "{fold:?}");
    assert_eq!(fold.watermark_hour, Some(HOUR), "{fold:?}");
    assert_eq!(
        fold.entry_count,
        report.objects_written() as u64,
        "every loaded object is a snapshot entry: {fold:?}"
    );
    assert!(
        fold.elapsed >= HEAD_PUT_DELAY,
        "the fold's time covers its HEAD write: {fold:?}"
    );
    assert!(
        report.elapsed >= fold.elapsed,
        "the load's elapsed {:?} includes the fold's {:?}",
        report.elapsed,
        fold.elapsed
    );

    // HEAD was written at the clock's fresh reading: the fold did not move
    // the clock to make the margin seal hour H, and did not reuse the
    // load's start-of-run now.
    let tenant_hash = TenantId::new(TENANT).hash();
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash.to_hex(),
        ravel_types::Signal::Logs.key_prefix()
    );
    let head_bytes = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("the fold wrote HEAD")
        .data;
    let head = ravel_catalog::decode_head(&head_bytes).expect("HEAD decodes");
    assert_eq!(head.watermark_hour, HOUR);
    assert_eq!(head.created_unix_ns, CLOCK_NS);

    // Only now query, and only the query's LISTs are counted.
    store.reset();
    let folded = count_rows(&build_app(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)).await;
    assert_eq!(count_of(&folded), ROWS as i64, "folded count: {folded}");
    assert_eq!(count_of(&folded), count_of(&control));
    assert_eq!(
        unfolded_segments(&folded),
        0,
        "every segment resolves from the snapshot: {folded}"
    );
    assert_eq!(
        resolve_lists(&folded),
        u64::from(SHARDS) + 1,
        "one bounded LIST per shard plus the erasure LIST: {folded}"
    );
    assert_eq!(
        store.list_count(),
        SHARDS as usize + 1,
        "the store saw exactly the resolve's LISTs"
    );
    assert_eq!(
        sealed_hour_keys(&store),
        Vec::<String>::new(),
        "no commit record of the sealed hour {HOUR} is listed"
    );
}
