//! Per-GET first-byte and total latency through [`InstrumentedStore`]
//! (issue #2976): which instant each duration ends at, which size bucket and
//! which phase a GET lands in, and how a failed-then-retried GET is counted.
//!
//! The memory oracle answers with the complete body, so a test that needs a
//! body arriving after its headers uses a backend that marks the first byte
//! itself, or the S3 adapter against a local endpoint that holds the body back.
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use axum::response::Response;
use axum::routing::any;
use bytes::Bytes;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::get_latency::{
    GetLatencySnapshot, GetPhase, InGetPhase, SIZE_BUCKET_COUNT, mark_first_byte, size_bucket,
};
use ravel_object_store::instrument::MonotonicClock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, InstrumentedStore, ListPage, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

/// A clock that moves only when a test advances it, and counts its readings.
#[derive(Default)]
struct ManualClock {
    now: AtomicU64,
    reads: AtomicUsize,
}

impl ManualClock {
    fn advance(&self, nanos: u64) {
        self.now.fetch_add(nanos, Ordering::SeqCst);
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl MonotonicClock for ManualClock {
    fn now_nanos(&self) -> u64 {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.now.load(Ordering::SeqCst)
    }
}

/// A backend whose GET takes `header_nanos` to its response headers and then
/// `body_nanos` more to its last byte, on the shared clock, marking the first
/// byte in between the way the S3 adapter does.
struct SlowBodyStore {
    inner: MemoryStore,
    clock: Arc<ManualClock>,
    header_nanos: u64,
    body_nanos: u64,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for SlowBodyStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.clock.advance(self.header_nanos);
        mark_first_byte();
        self.clock.advance(self.body_nanos);
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

async fn seeded_memory(objects: &[(&str, u64)]) -> MemoryStore {
    let store = MemoryStore::new();
    for (key, len) in objects {
        store
            .put(
                key,
                Bytes::from(vec![7u8; *len as usize]),
                PutOptions::default(),
            )
            .await
            .expect("seed");
    }
    store
}

fn snapshot<S: ObjectStoreBackend>(store: &InstrumentedStore<S>) -> GetLatencySnapshot {
    store.metrics().get_latency().snapshot()
}

/// Successful GETs per size bucket in `phase`.
fn bucket_counts(snap: &GetLatencySnapshot, phase: GetPhase) -> [u64; SIZE_BUCKET_COUNT] {
    snap.phase(phase).buckets.map(|bucket| bucket.count)
}

/// Successful GETs per phase, in `GetPhase::ALL` order.
fn phase_counts(snap: &GetLatencySnapshot) -> [u64; 5] {
    snap.phases.map(|phase| phase.ok_count())
}

/// First byte ends at the backend's mark, total at the call's return: one
/// GET whose headers take 1 ms and whose body takes 5 ms more records exactly
/// 1000 us and 6000 us. An implementation that ended both at the same instant
/// would record 6000 us twice.
#[tokio::test]
async fn a_delayed_body_records_first_byte_strictly_before_total() {
    let clock = Arc::new(ManualClock::default());
    let backend = SlowBodyStore {
        inner: seeded_memory(&[("seg", 10 * KIB)]).await,
        clock: Arc::clone(&clock),
        header_nanos: 1_000_000,
        body_nanos: 5_000_000,
    };
    let store = InstrumentedStore::with_clock(backend, clock.clone() as Arc<dyn MonotonicClock>);

    let got = store
        .get("seg", GetRange::Range(0, 4 * KIB))
        .await
        .expect("get");
    assert_eq!(got.data.len() as u64, 4 * KIB);

    let snap = snapshot(&store);
    let bucket = snap.phase(GetPhase::Unattributed).buckets[size_bucket(4 * KIB)];
    assert_eq!(bucket.count, 1);
    assert_eq!(bucket.first_byte.max_micros, 1_000);
    assert_eq!(bucket.total.max_micros, 6_000);
    assert!(bucket.first_byte.max_micros < bucket.total.max_micros);
    // Issue, first byte, return: one reading each.
    assert_eq!(clock.reads(), 3);
}

/// A backend that marks no first byte (the memory oracle hands back the whole
/// body at once) records first byte equal to total, and reads the clock twice.
#[tokio::test]
async fn an_unmarked_get_records_first_byte_equal_to_total() {
    let clock = Arc::new(ManualClock::default());
    let store = InstrumentedStore::with_clock(
        seeded_memory(&[("seg", 100)]).await,
        clock.clone() as Arc<dyn MonotonicClock>,
    );
    store.get("seg", GetRange::Full).await.expect("get");
    let bucket = snapshot(&store).phase(GetPhase::Unattributed).buckets[0];
    assert_eq!(bucket.count, 1);
    assert_eq!(bucket.first_byte, bucket.total);
    assert_eq!(clock.reads(), 2);
}

/// GETs of 3 KiB, 4 KiB, 5 KiB, 64 MiB and 65 MiB out of one 70 MiB object
/// land in the buckets of their requested lengths, exactly. Bucketing by object
/// size would put all five in the overflow bucket. A whole-object GET of a
/// 3 KiB object is bucketed by the bytes it returned.
#[tokio::test]
async fn gets_are_bucketed_by_requested_length_not_object_size() {
    let store =
        InstrumentedStore::new(seeded_memory(&[("big", 70 * MIB), ("small", 3 * KIB)]).await);
    for len in [3 * KIB, 4 * KIB, 5 * KIB, 64 * MIB, 65 * MIB] {
        let got = store
            .get("big", GetRange::Range(0, len))
            .await
            .expect("ranged get");
        assert_eq!(got.data.len() as u64, len);
    }
    store.get("small", GetRange::Full).await.expect("full get");

    let mut want = [0u64; SIZE_BUCKET_COUNT];
    want[0] = 2; // 3 KiB ranged, 3 KiB whole object
    want[1] = 2; // 4 KiB, 5 KiB: [4 KiB, 8 KiB)
    want[14] = 1; // 64 MiB: [32 MiB, 64 MiB]
    want[15] = 1; // 65 MiB: above 64 MiB
    let snap = snapshot(&store);
    assert_eq!(bucket_counts(&snap, GetPhase::Unattributed), want);
}

/// Two GETs issued in one phase and one in another count exactly 2 and 1, and
/// nothing lands anywhere else. `get_pinned` and `get_with_pin` are GETs too.
#[tokio::test]
async fn each_get_counts_in_the_phase_that_issued_it() {
    let store = InstrumentedStore::new(seeded_memory(&[("seg", 10 * KIB)]).await);
    async {
        store
            .get("seg", GetRange::Range(0, 100))
            .await
            .expect("plan get 1");
        store
            .get_with_pin("seg", GetRange::Suffix(100))
            .await
            .expect("plan get 2");
    }
    .in_get_phase(GetPhase::Plan)
    .await;
    store
        .get("seg", GetRange::Full)
        .in_get_phase(GetPhase::Scan)
        .await
        .expect("scan get");

    let snap = snapshot(&store);
    assert_eq!(phase_counts(&snap), [0, 2, 0, 1, 0]);
    assert_eq!(snap.phase(GetPhase::Plan).buckets[0].count, 2);
    assert_eq!(
        snap.phase(GetPhase::Scan).buckets[size_bucket(10 * KIB)].count,
        1
    );
}

/// A GET that fails with a transient error and is retried inside the same
/// phase scope counts in that phase twice: once under `failed`, once in the
/// size bucket of the retry that succeeded.
#[tokio::test]
async fn a_retried_get_counts_both_requests_in_the_original_phase() {
    let faulty = FaultStore::new(
        seeded_memory(&[("seg", 6 * KIB)]).await,
        FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Transient("blip".into()))
                .with_occurrence(Occurrence::Nth(1)),
        ),
    );
    let store = InstrumentedStore::new(faulty);

    let attempts = async {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match store.get("seg", GetRange::Full).await {
                Ok(got) => break (attempts, got),
                Err(err) if err.is_retryable() && attempts < 2 => continue,
                Err(err) => panic!("unexpected error {err:?}"),
            }
        }
    }
    .in_get_phase(GetPhase::Probe)
    .await;
    assert_eq!(attempts.0, 2);
    assert_eq!(attempts.1.data.len() as u64, 6 * KIB);
    assert_eq!(
        store.inner().fault_count(Op::Get, FaultKind::Transient),
        1,
        "the transient fault fired once"
    );

    let snap = snapshot(&store);
    let probe = snap.phase(GetPhase::Probe);
    assert_eq!(probe.failed_count, 1);
    assert_eq!(probe.ok_count(), 1);
    assert_eq!(probe.buckets[size_bucket(6 * KIB)].count, 1);
    for phase in GetPhase::ALL {
        if phase != GetPhase::Probe {
            assert_eq!(snap.phase(phase).ok_count(), 0, "{}", phase.name());
            assert_eq!(snap.phase(phase).failed_count, 0, "{}", phase.name());
        }
    }
}

/// A GET issued with no phase in scope is counted under `unattributed`.
#[tokio::test]
async fn a_get_with_no_phase_is_counted_as_unattributed() {
    let store = InstrumentedStore::new(seeded_memory(&[("seg", 10)]).await);
    store
        .get("seg", GetRange::Full)
        .await
        .expect("unscoped get");
    let snap = snapshot(&store);
    assert_eq!(phase_counts(&snap), [0, 0, 0, 0, 1]);
    assert_eq!(snap.phase(GetPhase::Unattributed).buckets[0].count, 1);
}

// ---------------------------------------------------------------------------
// The S3 adapter's first-byte mark, over HTTP
// ---------------------------------------------------------------------------

const BUCKET: &str = "ravel-latency-bucket";
const OBJECT_LEN: usize = 4096;
const BODY_DELAY_NANOS: u64 = 7_000_000;

/// Serves every GET with headers first and the body only after the client has
/// read the clock twice (the issue reading and the first-byte mark), advancing
/// the clock by [`BODY_DELAY_NANOS`] before releasing it. A client that marks
/// the first byte only after reading the body never reaches the second reading,
/// so the body is never sent.
async fn spawn_held_body_endpoint(clock: Arc<ManualClock>) -> String {
    let app = Router::new().route(
        "/{*rest}",
        any(move |method: Method| {
            let clock = Arc::clone(&clock);
            async move {
                if method != Method::GET {
                    return Response::builder()
                        .status(StatusCode::NOT_IMPLEMENTED)
                        .body(Body::empty())
                        .expect("response");
                }
                let body = futures::stream::once(async move {
                    while clock.reads() < 2 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    clock.advance(BODY_DELAY_NANOS);
                    Ok::<_, std::io::Error>(Bytes::from(vec![3u8; OBJECT_LEN]))
                });
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::ETAG, "\"held-body\"")
                    .header(header::LAST_MODIFIED, "Wed, 21 Oct 2015 07:28:00 GMT")
                    .header(header::CONTENT_LENGTH, OBJECT_LEN.to_string())
                    .body(Body::from_stream(body))
                    .expect("response")
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    endpoint
}

/// The S3 adapter marks the first byte when the response headers arrive,
/// before it reads the body: first byte is 0 us on the clock and total is the
/// 7 ms the endpoint held the body for. Marking after the body would leave the
/// endpoint waiting for a second clock reading that never comes, and the GET
/// would not finish.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_s3_adapter_marks_first_byte_before_reading_the_body() {
    let clock = Arc::new(ManualClock::default());
    let endpoint = spawn_held_body_endpoint(Arc::clone(&clock)).await;
    let s3 = S3Store::new(S3Config {
        bucket: BUCKET.to_string(),
        region: "us-east-1".to_string(),
        endpoint: Some(endpoint),
        access_key_id: "fake-access-key".to_string(),
        secret_access_key: "fake-secret-key".to_string(),
        allow_http: true,
        force_path_style: true,
        kms_key_id: None,
        session_token: None,
        credentials_file: None,
        auth: Default::default(),
        instance_metadata_endpoint: None,
    })
    .expect("store builds");
    let store = InstrumentedStore::with_clock(s3, clock.clone() as Arc<dyn MonotonicClock>);

    let got = tokio::time::timeout(
        Duration::from_secs(10),
        store
            .get("seg", GetRange::Full)
            .in_get_phase(GetPhase::Scan),
    )
    .await
    .expect("the first byte was marked before the body arrived, so the body was sent")
    .expect("get");
    assert_eq!(got.data.len(), OBJECT_LEN);

    let bucket = snapshot(&store).phase(GetPhase::Scan).buckets[size_bucket(OBJECT_LEN as u64)];
    assert_eq!(bucket.count, 1);
    assert_eq!(bucket.first_byte.max_micros, 0);
    assert_eq!(bucket.total.max_micros, BODY_DELAY_NANOS / 1_000);
}
