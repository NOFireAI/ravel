//! The `sys/compat` activation policy on the log and span flush paths
//! (ADR-4233 decisions 2 to 4), driven through each router's write API with
//! the policy's family-version seam: no policy publishes byte for byte what
//! the parent of the policy work published, the bucket is fenced before the
//! first PUT of a version above the baseline, a view that ages past the
//! horizon after grant holds the flush before its next attempt until a
//! refresh or the end of its lifetime, a `sys/compat` outage after an absent
//! read refuses admission and holds an already acknowledged flush at grant
//! the same way, a view no read has succeeded for publishes only while its
//! latest refresh was refused with `AccessDenied`, and a teardown flush is
//! refused under a stale view after an activation and publishes at the
//! baseline on a store that never ran compat init.
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use prost::Message;
use ravel_catalog::compat::{
    COMPAT_VIEW_HORIZON_NS, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, create_compat,
};
use ravel_catalog::{AbsentPolicy, CompatClock, CompatKeys, CompatRefusal, CompatView};
use ravel_commit::rng::SeededRng;
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_proto::sys::v1 as sysproto;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};

use crate::clock::Clock;
use crate::compat::{CompatPublishRefusal, IngestCompatPolicy};
use crate::compat_get_store::{CompatGet, CompatGetStore};
use crate::config::IngestConfig;
use crate::format_floor::FormatFamily;
use crate::indexed_fields::IndexedFieldsOverlay;
use crate::log_error::LogWriteError;
use crate::log_router::{LogIngestRouter, NoIndexedFields};
use crate::pending_ack::PendingAckBudget;
use crate::router::WriteMode;
use crate::span_error::SpanWriteError;
use crate::span_router::SpanIngestRouter;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

const ACK_DEADLINE: Duration = Duration::from_secs(60);

/// What `acme`'s one-record log write and one-span span write published on
/// the parent of the activation policy work (b6646b020), under
/// [`seeded_log_router`] and [`seeded_span_router`].
const PARENT_LOGS_DIGEST: &str = "98ecb0f9875f6c059ae729a5ecc6b0a33ca3c18497eb6de738d366a136d7c8d7";
const PARENT_SPANS_DIGEST: &str =
    "254f5ddc969569986bc590ff5928c7871de5656ec720067e98b17e5a8cb4d629";

/// Pins every clock-derived identity field. Flushes run on arrival, never
/// on the age tick.
struct FixedClock(i64);

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

struct CompatTestClock(AtomicI64);

impl CompatClock for CompatTestClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(10),
        ..IngestConfig::default()
    }
}

fn log_record() -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&res, "scope", "", &scope_attrs).into(),
        ts_ns: 1_000,
        observed_ts_ns: 1_000,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "line".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn span() -> NormalizedSpan {
    NormalizedSpan {
        trace_id: [1; 16],
        span_id: [1; 8],
        parent_span_id: None,
        name: "handle".to_string(),
        start_ts_ns: 1_000,
        end_ts_ns: 1_100,
        status_code: StatusCode::Unset,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}

/// Every key under `tenant`'s `signal` prefix, sorted.
async fn keys(store: &dyn ObjectStoreBackend, tenant: &TenantId, signal: Signal) -> Vec<String> {
    let prefix = format!("t/{}/{}/", tenant.hash().to_hex(), signal.key_prefix());
    let mut keys: Vec<String> = list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    keys.sort();
    keys
}

fn objects(keys: &[String]) -> usize {
    keys.iter()
        .filter(|k| k.contains("/l0/") || k.contains("/c/"))
        .count()
}

/// blake3 over every key and object under `tenant`'s `signal` prefix, in
/// key order.
async fn digest(store: &dyn ObjectStoreBackend, tenant: &TenantId, signal: Signal) -> String {
    let keys = keys(store, tenant, signal).await;
    assert!(keys.iter().any(|key| key.contains("/l0/")), "{keys:?}");
    assert!(keys.iter().any(|key| key.contains("/c/")), "{keys:?}");
    let mut hasher = blake3::Hasher::new();
    for key in &keys {
        let bytes = store.get(key, GetRange::Full).await.expect("get").data;
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    hasher.finalize().to_hex().to_string()
}

fn seeded_log_router(store: Arc<dyn ObjectStoreBackend>) -> LogIngestRouter {
    LogIngestRouter::with_rng(
        config(),
        store,
        Arc::new(FixedClock(BASE_NS)),
        Arc::new(IndexedFieldsOverlay::new(Arc::new(NoIndexedFields))),
        Arc::new(SeededRng::new(4244)),
        Arc::new(PendingAckBudget::new(0)),
    )
}

fn seeded_span_router(store: Arc<dyn ObjectStoreBackend>) -> SpanIngestRouter {
    SpanIngestRouter::with_rng(
        config(),
        store,
        Arc::new(FixedClock(BASE_NS)),
        Arc::new(SeededRng::new(4244)),
        Arc::new(PendingAckBudget::new(0)),
    )
}

async fn log_digest(policy: Option<Option<Arc<IngestCompatPolicy>>>) -> String {
    let acme = TenantId::new("acme");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut router = seeded_log_router(Arc::clone(&store));
    if let Some(policy) = policy {
        router = router.with_compat_policy(policy).expect("no publish bound");
    }
    router
        .write(
            acme.clone(),
            vec![log_record()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect("written");
    digest(store.as_ref(), &acme, Signal::Logs).await
}

async fn span_digest(policy: Option<Option<Arc<IngestCompatPolicy>>>) -> String {
    let acme = TenantId::new("acme");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut router = seeded_span_router(Arc::clone(&store));
    if let Some(policy) = policy {
        router = router.with_compat_policy(policy).expect("no publish bound");
    }
    router
        .write(
            acme.clone(),
            vec![span()],
            WriteMode::StrictArrival,
            ACK_DEADLINE,
        )
        .await
        .expect("written");
    digest(store.as_ref(), &acme, Signal::Spans).await
}

/// ADR-4233 decision 8: with no policy installed, never set or set to
/// `None`, a log and a span write store the same keys and bytes as before
/// the policy existed.
#[tokio::test]
async fn no_policy_publishes_the_parents_log_and_span_bytes() {
    assert_eq!(log_digest(None).await, PARENT_LOGS_DIGEST);
    assert_eq!(log_digest(Some(None)).await, PARENT_LOGS_DIGEST);
    assert_eq!(span_digest(None).await, PARENT_SPANS_DIGEST);
    assert_eq!(span_digest(Some(None)).await, PARENT_SPANS_DIGEST);
}

/// A store holding `tenant`'s `signal` provisioning record and a
/// `sys/compat` at epoch 1 naming each family's `CURRENT`; a policy over a
/// view that read it, with `family`'s baseline lowered one below `CURRENT`
/// so `CURRENT` is above the baseline and needs the bucket fence.
async fn above_baseline(
    tenant: &TenantId,
    signal: Signal,
    family: FormatFamily,
) -> (
    Arc<FaultStore<MemoryStore>>,
    Arc<CompatTestClock>,
    Arc<IngestCompatPolicy>,
) {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    ravel_catalog::validate_or_adopt(
        store.as_ref(),
        &tenant.hash(),
        signal,
        1,
        BASE_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    let keys = CompatKeys::from_file_keys(&[[4; 32]]).expect("one key");
    let families = [
        (FAMILY_RSEG, FormatFamily::Rseg),
        (FAMILY_RLOG, FormatFamily::Rlog),
        (FAMILY_RSPAN, FormatFamily::Rspan),
    ]
    .into_iter()
    .map(|(name, family)| sysproto::CompatFamily {
        family: name.to_string(),
        active_write_version: family.writable_version(),
        ..Default::default()
    })
    .collect();
    create_compat(
        store.as_ref(),
        &keys,
        sysproto::CompatRecord {
            compat_epoch: 1,
            families,
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
    let clock = Arc::new(CompatTestClock(AtomicI64::new(BASE_NS)));
    let view = CompatView::new(Arc::clone(&store), keys, clock.clone());
    view.refresh().await.expect("read sys/compat");
    let current = u16::try_from(family.writable_version()).expect("fits");
    let policy = Arc::new(
        IngestCompatPolicy::new(view, clock.clone()).with_family_versions(
            family,
            vec![current],
            current - 1,
        ),
    );
    (fault_store, clock, policy)
}

/// ADR-4233 decision 3 on the log path: a view that no refresh has
/// succeeded for is blind only while its latest refresh was refused with
/// `AccessDenied`, a credential without the `sys/compat` grant. Blind, a log
/// write to a bucket at `compat_epoch` 0 publishes at the baseline. Behind a
/// 503 the view cannot tell whether an activation happened, and the same
/// write is refused under `view_stale` at once, inside the horizon, and
/// stores nothing.
#[tokio::test]
async fn a_never_read_log_view_publishes_only_behind_access_denied() {
    let acme = TenantId::new("acme");
    for get in [CompatGet::AccessDenied, CompatGet::ServerError] {
        let compat_store = Arc::new(CompatGetStore::new(get));
        let store: Arc<dyn ObjectStoreBackend> = compat_store.clone();
        ravel_catalog::validate_or_adopt(
            store.as_ref(),
            &acme.hash(),
            Signal::Logs,
            1,
            BASE_NS,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
        let compat_keys = CompatKeys::from_file_keys(&[[4; 32]]).expect("one key");
        let clock = Arc::new(CompatTestClock(AtomicI64::new(BASE_NS)));
        let view = CompatView::new(Arc::clone(&store), compat_keys, clock.clone());
        let err = view.refresh().await.expect_err("the sys/compat GET fails");
        assert_eq!(
            err.is_access_denied(),
            get == CompatGet::AccessDenied,
            "{get:?}"
        );
        assert_eq!(compat_store.failed(), 1, "{get:?}");
        let router = seeded_log_router(Arc::clone(&store))
            .with_compat_policy(Some(Arc::new(IngestCompatPolicy::new(view, clock))))
            .expect("no publish bound");

        let written = router
            .write(
                acme.clone(),
                vec![log_record()],
                WriteMode::StrictArrival,
                ACK_DEADLINE,
            )
            .await;
        let stored = objects(&keys(store.as_ref(), &acme, Signal::Logs).await);
        let refused = router.metrics_snapshot().compat_publish_refused;
        match get {
            CompatGet::AccessDenied => {
                written.expect("a blind view publishes at the baseline");
                assert_eq!(stored, 2);
                assert_eq!(refused.total(), 0);
            }
            CompatGet::ServerError | CompatGet::Passthrough => {
                let err = written.expect_err("a never-read view behind a 5xx is not blind");
                assert!(
                    matches!(&err, LogWriteError::CompatRefused(r)
                    if policy_refusal(r, FormatFamily::Rlog, |r| matches!(
                        r,
                        CompatRefusal::ViewStale {
                            last_read_unix_ns: None,
                            now_unix_ns: BASE_NS,
                        }
                    ))),
                    "{err:?}"
                );
                assert_eq!(stored, 0);
                assert_eq!(refused.view_stale, 1);
                assert_eq!(refused.total(), 1);
            }
        }
    }
}

/// The `compat_epoch` of `tenant`'s `signal` provisioning record.
async fn bucket_compat_epoch(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
) -> u64 {
    let key = ravel_catalog::provisioning_key(&tenant.hash(), signal);
    let bytes = store
        .get(&key, GetRange::Full)
        .await
        .expect("provisioning record")
        .data;
    sysproto::ProvisioningRecord::decode(bytes.as_ref())
        .expect("decode provisioning record")
        .compat_epoch
}

/// Waits for one PUT to park at `gate`, failing rather than hanging when the
/// flush never sends it.
async fn held(gate: &GateHandle) {
    tokio::time::timeout(Duration::from_secs(30), gate.wait_until_held(1))
        .await
        .expect("a PUT parked at the gate");
}

fn policy_refusal(
    refused: &CompatPublishRefusal,
    family: FormatFamily,
    want: impl Fn(&CompatRefusal) -> bool,
) -> bool {
    matches!(
        refused,
        CompatPublishRefusal::Policy { family: f, refusal } if *f == family && want(refusal)
    )
}

/// ADR-4233 decision 4 on the log path: the bucket carries the view's epoch
/// by the time the first data PUT is sent. A fence raised after the PUT, or
/// never, leaves it at 0 there.
#[tokio::test]
async fn a_log_flush_above_the_baseline_fences_the_bucket_before_its_first_put() {
    let acme = TenantId::new("acme");
    let (fault_store, _, policy) = above_baseline(&acme, Signal::Logs, FormatFamily::Rlog).await;
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let router = Arc::new(
        seeded_log_router(Arc::clone(&store))
            .with_compat_policy(Some(policy))
            .expect("no publish bound"),
    );
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

    let write = tokio::spawn({
        let (router, acme) = (Arc::clone(&router), acme.clone());
        async move {
            router
                .write(
                    acme,
                    vec![log_record()],
                    WriteMode::StrictArrival,
                    ACK_DEADLINE,
                )
                .await
        }
    });
    held(&gate).await;
    assert_eq!(
        bucket_compat_epoch(store.as_ref(), &acme, Signal::Logs).await,
        1,
        "the bucket is fenced to the view's epoch before the data PUT"
    );
    for id in gate.held() {
        assert!(gate.release(id));
    }
    write.await.expect("write task").expect("written");
    assert_eq!(objects(&keys(store.as_ref(), &acme, Signal::Logs).await), 2);
    assert_eq!(router.metrics_snapshot().compat_publish_refused.total(), 0);
}

#[tokio::test]
async fn a_span_flush_above_the_baseline_fences_the_bucket_before_its_first_put() {
    let acme = TenantId::new("acme");
    let (fault_store, _, policy) = above_baseline(&acme, Signal::Spans, FormatFamily::Rspan).await;
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let router = Arc::new(
        seeded_span_router(Arc::clone(&store))
            .with_compat_policy(Some(policy))
            .expect("no publish bound"),
    );
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

    let write = tokio::spawn({
        let (router, acme) = (Arc::clone(&router), acme.clone());
        async move {
            router
                .write(acme, vec![span()], WriteMode::StrictArrival, ACK_DEADLINE)
                .await
        }
    });
    held(&gate).await;
    assert_eq!(
        bucket_compat_epoch(store.as_ref(), &acme, Signal::Spans).await,
        1,
        "the bucket is fenced to the view's epoch before the data PUT"
    );
    for id in gate.held() {
        assert!(gate.release(id));
    }
    write.await.expect("write task").expect("written");
    assert_eq!(
        objects(&keys(store.as_ref(), &acme, Signal::Spans).await),
        2
    );
    assert_eq!(router.metrics_snapshot().compat_publish_refused.total(), 0);
}

/// A clock the test moves by hand; a `sleep` on it completes once the test
/// advances it past the sleep's end.
pub(crate) struct ManualClock {
    now_ns: AtomicI64,
    wake: tokio::sync::watch::Sender<()>,
}

impl ManualClock {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(ManualClock {
            now_ns: AtomicI64::new(BASE_NS),
            wake: tokio::sync::watch::channel(()).0,
        })
    }

    /// Moves the clock one [`crate::compat::STALE_VIEW_RETRY`] at a time,
    /// `steps` times, letting a flush waiting out a stale view ask again
    /// after each.
    pub(crate) async fn step(&self, steps: usize) {
        let step = i64::try_from(crate::compat::STALE_VIEW_RETRY.as_nanos()).expect("fits");
        for _ in 0..steps {
            self.advance_ns(step).await;
        }
    }

    /// Moves the clock `delta_ns` forward, then yields so every sleep it
    /// ended runs.
    pub(crate) async fn advance_ns(&self, delta_ns: i64) {
        self.now_ns.fetch_add(delta_ns, Ordering::SeqCst);
        let _ = self.wake.send(());
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }
}

impl Clock for ManualClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.load(Ordering::SeqCst)
    }

    fn sleep(
        &self,
        dur: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let end = self
            .now_ns()
            .saturating_add(i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX));
        let mut rx = self.wake.subscribe();
        Box::pin(async move {
            while self.now_ns() < end {
                if rx.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}

const STALE_LIFETIME: Duration = Duration::from_secs(30);

/// One shard, flushes on arrival, and a lifetime below [`ACK_DEADLINE`] so
/// a flush refused at its end reaches the writer.
fn stale_config() -> IngestConfig {
    IngestConfig {
        max_flush_delay: Duration::from_secs(10),
        max_flush_delay_idle: Duration::from_secs(10),
        max_flush_lifetime: STALE_LIFETIME,
        ..config()
    }
}

/// ADR-4233 decision 3 on `signal`'s path: the view ages to the horizon
/// while the granted flush's fence PUT is in flight. The flush sends no PUT
/// while the view is stale and keeps its acknowledged rows: with `recover`
/// a refresh inside its lifetime lets it publish, and without one it is
/// refused `view_stale` once the lifetime elapses. A check made only at
/// grant lets the PUT through stale; one that refuses the attempt at once
/// drops the rows even when the view recovers.
async fn stale_after_grant<T: std::fmt::Debug, E: std::fmt::Debug>(
    signal: Signal,
    family: FormatFamily,
    recover: bool,
    write: impl FnOnce(
        Arc<dyn ObjectStoreBackend>,
        Arc<ManualClock>,
        Arc<IngestCompatPolicy>,
        TenantId,
    ) -> (
        tokio::task::JoinHandle<Result<T, E>>,
        Box<dyn Fn() -> crate::compat::CompatRefusalSnapshot>,
    ),
    refused_view_stale: impl Fn(&E) -> bool,
) {
    let acme = TenantId::new("acme");
    let (fault_store, compat_clock, policy) = above_baseline(&acme, signal, family).await;
    let view = policy.view().clone();
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let gate = fault_store.hold(Op::Put, Some("/prov".to_string()), Occurrence::Nth(1));
    let clock = ManualClock::new();
    let (written, refused) = write(Arc::clone(&store), Arc::clone(&clock), policy, acme.clone());
    held(&gate).await;
    compat_clock
        .0
        .fetch_add(COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);
    for id in gate.held() {
        assert!(gate.release(id));
    }

    clock.step(15).await;
    assert!(
        !written.is_finished(),
        "inside the lifetime the flush waits"
    );
    assert_eq!(
        objects(&keys(store.as_ref(), &acme, signal).await),
        0,
        "no PUT while the view is stale"
    );
    assert_eq!(refused().total(), 0, "the flush is not dropped");

    if recover {
        view.refresh()
            .await
            .expect("the view reads sys/compat again");
        for _ in 0..5 {
            if written.is_finished() {
                break;
            }
            clock.step(1).await;
        }
        assert!(
            written.is_finished(),
            "published within a retry of the refresh"
        );
        written
            .await
            .expect("write task")
            .expect("published once the view is fresh");
        assert_eq!(objects(&keys(store.as_ref(), &acme, signal).await), 2);
        assert_eq!(refused().total(), 0);
    } else {
        clock.step(16).await;
        assert!(written.is_finished(), "refused once the lifetime elapsed");
        let err = written
            .await
            .expect("write task")
            .expect_err("the lifetime elapsed with the view still stale");
        assert!(refused_view_stale(&err), "{err:?}");
        assert_eq!(objects(&keys(store.as_ref(), &acme, signal).await), 0);
        assert_eq!(refused().view_stale, 1);
        assert_eq!(refused().total(), 1);
    }
}

async fn log_stale_after_grant(recover: bool) {
    stale_after_grant(
        Signal::Logs,
        FormatFamily::Rlog,
        recover,
        |store, clock, policy, acme| {
            let router = Arc::new(
                LogIngestRouter::with_rng(
                    stale_config(),
                    store,
                    clock,
                    Arc::new(IndexedFieldsOverlay::new(Arc::new(NoIndexedFields))),
                    Arc::new(SeededRng::new(4244)),
                    Arc::new(PendingAckBudget::new(0)),
                )
                .with_compat_policy(Some(policy))
                .expect("no publish bound"),
            );
            let written = tokio::spawn({
                let router = Arc::clone(&router);
                async move {
                    router
                        .write(
                            acme,
                            vec![log_record()],
                            WriteMode::StrictArrival,
                            ACK_DEADLINE,
                        )
                        .await
                }
            });
            (
                written,
                Box::new(move || router.metrics_snapshot().compat_publish_refused),
            )
        },
        |err| {
            matches!(err, LogWriteError::CompatRefused(r)
                if policy_refusal(r, FormatFamily::Rlog, |r| matches!(r, CompatRefusal::ViewStale { .. })))
        },
    )
    .await;
}

async fn span_stale_after_grant(recover: bool) {
    stale_after_grant(
        Signal::Spans,
        FormatFamily::Rspan,
        recover,
        |store, clock, policy, acme| {
            let router = Arc::new(
                SpanIngestRouter::with_rng(
                    stale_config(),
                    store,
                    clock,
                    Arc::new(SeededRng::new(4244)),
                    Arc::new(PendingAckBudget::new(0)),
                )
                .with_compat_policy(Some(policy))
                .expect("no publish bound"),
            );
            let written = tokio::spawn({
                let router = Arc::clone(&router);
                async move {
                    router
                        .write(acme, vec![span()], WriteMode::StrictArrival, ACK_DEADLINE)
                        .await
                }
            });
            (
                written,
                Box::new(move || router.metrics_snapshot().compat_publish_refused),
            )
        },
        |err| {
            matches!(err, SpanWriteError::CompatRefused(r)
                if policy_refusal(r, FormatFamily::Rspan, |r| matches!(r, CompatRefusal::ViewStale { .. })))
        },
    )
    .await;
}

#[tokio::test]
async fn a_log_view_stale_after_grant_waits_for_a_refresh_then_publishes() {
    log_stale_after_grant(true).await;
}

#[tokio::test]
async fn a_log_view_stale_after_grant_is_refused_only_past_the_lifetime() {
    log_stale_after_grant(false).await;
}

#[tokio::test]
async fn a_span_view_stale_after_grant_waits_for_a_refresh_then_publishes() {
    span_stale_after_grant(true).await;
}

#[tokio::test]
async fn a_span_view_stale_after_grant_is_refused_only_past_the_lifetime() {
    span_stale_after_grant(false).await;
}

/// One write of `acme` through a router under test, and its router's
/// refusal counters.
type OutageWrite<E> =
    Box<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), E>> + Send>>>;
type OutageRefused = Box<dyn Fn() -> crate::compat::CompatRefusalSnapshot>;

/// ADR-4233 decisions 1 and 3 on `signal`'s grant path: on a store with no
/// `sys/compat`, a read finds it absent, a buffered write is acknowledged,
/// and then every `sys/compat` GET fails with a 503 past the horizon.
/// Admission refuses the next write `view_stale`. The acknowledged rows'
/// flush is not dropped at grant: it waits out the stale view and, with
/// `recover`, publishes once a refresh succeeds inside its
/// `max_flush_lifetime`; without one it is refused `view_stale`, counted and
/// with nothing stored, only once that lifetime from its grant elapses. A
/// grant that refuses a stale view at once drops the rows either way; an
/// admission that lets a stale view through acknowledges the second write.
async fn outage_after_an_absent_read<E: std::fmt::Debug>(
    signal: Signal,
    recover: bool,
    build: impl FnOnce(
        Arc<dyn ObjectStoreBackend>,
        Arc<ManualClock>,
        Arc<IngestCompatPolicy>,
    ) -> (OutageWrite<E>, OutageRefused),
    refused_view_stale: impl Fn(&E) -> bool,
) {
    let acme = TenantId::new("acme");
    let compat_store = Arc::new(CompatGetStore::new(CompatGet::Passthrough));
    let store: Arc<dyn ObjectStoreBackend> = compat_store.clone();
    ravel_catalog::validate_or_adopt(
        store.as_ref(),
        &acme.hash(),
        signal,
        1,
        BASE_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    let compat_keys = CompatKeys::from_file_keys(&[[4; 32]]).expect("one key");
    let compat_clock = Arc::new(CompatTestClock(AtomicI64::new(BASE_NS)));
    let view = CompatView::new(Arc::clone(&store), compat_keys, compat_clock.clone());
    assert!(!view.refresh().await.expect("absent reads").present);
    let clock = ManualClock::new();
    let policy = Arc::new(IngestCompatPolicy::new(view.clone(), compat_clock.clone()));
    let (write, refused) = build(Arc::clone(&store), Arc::clone(&clock), policy);

    write().await.expect("acknowledged while the view is fresh");
    compat_store.set_get(CompatGet::ServerError);
    compat_clock
        .0
        .fetch_add(COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);
    assert!(view.refresh().await.is_err(), "the outage fails the read");
    assert_eq!(compat_store.failed(), 1);

    let err = write()
        .await
        .expect_err("admission refuses past the horizon");
    assert!(refused_view_stale(&err), "{err:?}");
    assert_eq!(refused().view_stale, 1);

    clock.step(15).await;
    assert_eq!(
        objects(&keys(store.as_ref(), &acme, signal).await),
        0,
        "no PUT while the view is stale"
    );
    assert_eq!(refused().view_stale, 1, "the flush is not dropped");

    if recover {
        compat_store.set_get(CompatGet::Passthrough);
        assert!(!view.refresh().await.expect("the outage ends").present);
        for _ in 0..5 {
            if objects(&keys(store.as_ref(), &acme, signal).await) == 2 {
                break;
            }
            clock.step(1).await;
        }
        assert_eq!(objects(&keys(store.as_ref(), &acme, signal).await), 2);
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme, signal).await, 0);
        assert_eq!(refused().total(), 1);
    } else {
        clock.step(STALE_LIFETIME.as_secs() as usize + 1).await;
        assert_eq!(refused().view_stale, 2, "abandoned past the lifetime");
        assert_eq!(refused().total(), 2);
        assert_eq!(objects(&keys(store.as_ref(), &acme, signal).await), 0);
    }
}

async fn log_outage_after_an_absent_read(recover: bool) {
    outage_after_an_absent_read(
        Signal::Logs,
        recover,
        |store, clock, policy| {
            let router = Arc::new(
                LogIngestRouter::with_rng(
                    stale_config(),
                    store,
                    clock,
                    Arc::new(IndexedFieldsOverlay::new(Arc::new(NoIndexedFields))),
                    Arc::new(SeededRng::new(4244)),
                    Arc::new(PendingAckBudget::new(0)),
                )
                .with_compat_policy(Some(policy))
                .expect("no publish bound"),
            );
            let writer = Arc::clone(&router);
            (
                Box::new(move || {
                    let router = Arc::clone(&writer);
                    Box::pin(async move {
                        router
                            .write(
                                TenantId::new("acme"),
                                vec![log_record()],
                                WriteMode::Buffered,
                                ACK_DEADLINE,
                            )
                            .await
                            .map(drop)
                    })
                }),
                Box::new(move || router.metrics_snapshot().compat_publish_refused),
            )
        },
        |err| {
            matches!(err, LogWriteError::CompatRefused(r)
                if policy_refusal(r, FormatFamily::Rlog, |r| matches!(r, CompatRefusal::ViewStale { .. })))
        },
    )
    .await;
}

async fn span_outage_after_an_absent_read(recover: bool) {
    outage_after_an_absent_read(
        Signal::Spans,
        recover,
        |store, clock, policy| {
            let router = Arc::new(
                SpanIngestRouter::with_rng(
                    stale_config(),
                    store,
                    clock,
                    Arc::new(SeededRng::new(4244)),
                    Arc::new(PendingAckBudget::new(0)),
                )
                .with_compat_policy(Some(policy))
                .expect("no publish bound"),
            );
            let writer = Arc::clone(&router);
            (
                Box::new(move || {
                    let router = Arc::clone(&writer);
                    Box::pin(async move {
                        router
                            .write(
                                TenantId::new("acme"),
                                vec![span()],
                                WriteMode::Buffered,
                                ACK_DEADLINE,
                            )
                            .await
                            .map(drop)
                    })
                }),
                Box::new(move || router.metrics_snapshot().compat_publish_refused),
            )
        },
        |err| {
            matches!(err, SpanWriteError::CompatRefused(r)
                if policy_refusal(r, FormatFamily::Rspan, |r| matches!(r, CompatRefusal::ViewStale { .. })))
        },
    )
    .await;
}

#[tokio::test]
async fn a_log_outage_after_an_absent_read_publishes_acknowledged_rows_once_the_view_recovers() {
    log_outage_after_an_absent_read(true).await;
}

#[tokio::test]
async fn a_log_outage_after_an_absent_read_abandons_acknowledged_rows_only_past_the_lifetime() {
    log_outage_after_an_absent_read(false).await;
}

#[tokio::test]
async fn a_span_outage_after_an_absent_read_publishes_acknowledged_rows_once_the_view_recovers() {
    span_outage_after_an_absent_read(true).await;
}

#[tokio::test]
async fn a_span_outage_after_an_absent_read_abandons_acknowledged_rows_only_past_the_lifetime() {
    span_outage_after_an_absent_read(false).await;
}

/// How a log or span router reaches its teardown flush, as in the metrics
/// shard's twin (`shard::tests`): `shutdown()` with the switch live, or a
/// drop, where the switch is gone and only the drain's bypass passes open
/// the flush.
#[derive(Debug, Clone, Copy)]
enum Teardown {
    Shutdown,
    Drop,
}

/// What a teardown test reads off a router's metrics.
#[derive(Debug)]
struct TeardownCounts {
    refused: crate::compat::CompatRefusalSnapshot,
    flushes_manual: u64,
    buffered: u64,
}

type CountsFn = Box<dyn Fn() -> TeardownCounts + Send + Sync>;

/// A log or span router under a teardown test, with `config()`'s hour-long
/// flush delays so the only flush of a buffered write is the teardown's, and
/// a [`STALE_LIFETIME`] the test's clock steps cross.
trait TeardownRouter: Send + Sync + Sized + 'static {
    const SIGNAL: Signal;
    const FAMILY: FormatFamily;

    fn build(
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<ManualClock>,
        policy: Arc<IngestCompatPolicy>,
    ) -> Self;

    /// One buffered write of `tenant`, acknowledged.
    fn write_one(&self, tenant: TenantId) -> impl std::future::Future<Output = ()> + Send + '_;

    fn teardown_shutdown(self) -> impl std::future::Future<Output = ()> + Send;

    fn counts(&self) -> CountsFn;
}

fn teardown_config() -> IngestConfig {
    IngestConfig {
        max_flush_lifetime: STALE_LIFETIME,
        ..config()
    }
}

impl TeardownRouter for LogIngestRouter {
    const SIGNAL: Signal = Signal::Logs;
    const FAMILY: FormatFamily = FormatFamily::Rlog;

    fn build(
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<ManualClock>,
        policy: Arc<IngestCompatPolicy>,
    ) -> Self {
        LogIngestRouter::with_rng(
            teardown_config(),
            store,
            clock,
            Arc::new(IndexedFieldsOverlay::new(Arc::new(NoIndexedFields))),
            Arc::new(SeededRng::new(4244)),
            Arc::new(PendingAckBudget::new(0)),
        )
        .with_compat_policy(Some(policy))
        .expect("no publish bound")
    }

    async fn write_one(&self, tenant: TenantId) {
        self.write(
            tenant,
            vec![log_record()],
            WriteMode::Buffered,
            ACK_DEADLINE,
        )
        .await
        .expect("acknowledged");
    }

    fn teardown_shutdown(self) -> impl std::future::Future<Output = ()> + Send {
        self.shutdown()
    }

    fn counts(&self) -> CountsFn {
        let metrics = self.metrics_handle();
        Box::new(move || {
            let snapshot = metrics.snapshot();
            TeardownCounts {
                refused: snapshot.compat_publish_refused,
                flushes_manual: snapshot.flushes_manual,
                buffered: snapshot.buffered_records_total,
            }
        })
    }
}

impl TeardownRouter for SpanIngestRouter {
    const SIGNAL: Signal = Signal::Spans;
    const FAMILY: FormatFamily = FormatFamily::Rspan;

    fn build(
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<ManualClock>,
        policy: Arc<IngestCompatPolicy>,
    ) -> Self {
        SpanIngestRouter::with_rng(
            teardown_config(),
            store,
            clock,
            Arc::new(SeededRng::new(4244)),
            Arc::new(PendingAckBudget::new(0)),
        )
        .with_compat_policy(Some(policy))
        .expect("no publish bound")
    }

    async fn write_one(&self, tenant: TenantId) {
        self.write(tenant, vec![span()], WriteMode::Buffered, ACK_DEADLINE)
            .await
            .expect("acknowledged");
    }

    fn teardown_shutdown(self) -> impl std::future::Future<Output = ()> + Send {
        self.shutdown()
    }

    fn counts(&self) -> CountsFn {
        let metrics = self.metrics_handle();
        Box::new(move || {
            let snapshot = metrics.snapshot();
            TeardownCounts {
                refused: snapshot.compat_publish_refused,
                flushes_manual: snapshot.flushes_manual,
                buffered: snapshot.buffered_spans_total,
            }
        })
    }
}

/// Tears `router` down as `teardown` says, in a task that ends once the
/// teardown call returns.
fn spawn_teardown<R: TeardownRouter>(router: R, teardown: Teardown) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match teardown {
            Teardown::Shutdown => router.teardown_shutdown().await,
            Teardown::Drop => drop(router),
        }
    })
}

/// Yields until `probe` is true, failing after a bounded number of turns.
async fn eventually(mut probe: impl FnMut() -> bool, what: &str) {
    for _ in 0..20_000 {
        if probe() {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert!(probe(), "{what}");
}

/// Every key under `tenant`'s `signal` prefix with its bytes, in key order.
async fn signal_objects(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
) -> Vec<(String, bytes::Bytes)> {
    let mut objects = Vec::new();
    for key in keys(store, tenant, signal).await {
        let bytes = store.get(&key, GetRange::Full).await.expect("get").data;
        objects.push((key, bytes));
    }
    objects
}

/// The `segment_format_version` of every commit record under `tenant`'s
/// `signal` prefix.
async fn committed_versions(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
) -> Vec<u32> {
    let mut versions = Vec::new();
    for key in keys(store, tenant, signal).await {
        if key.contains("/c/") {
            let bytes = store.get(&key, GetRange::Full).await.expect("get").data;
            versions.push(
                ravel_commit::record::decode(&bytes)
                    .expect("decode commit record")
                    .segment_format_version,
            );
        }
    }
    versions
}

/// #4362 on `R`'s path (the metrics twin is
/// `shard::tests::a_teardown_flush_under_a_stale_view_after_an_activation_writes_nothing`):
/// after an activation, with the bucket fenced, a buffered row acknowledged
/// while the view was fresh is still buffered when the view ages past the
/// horizon and the router is torn down. The teardown flush waits inside its
/// `max_flush_lifetime`, is then refused `view_stale` and counted, and
/// writes nothing: every key under the tenant, the fenced provisioning
/// record included, is byte for byte what it was.
async fn teardown_under_a_stale_view<R: TeardownRouter>(teardown: Teardown) {
    let acme = TenantId::new("acme");
    let (fault_store, compat_clock, policy) = above_baseline(&acme, R::SIGNAL, R::FAMILY).await;
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    ravel_catalog::raise_compat_fence(store.as_ref(), &acme.hash(), R::SIGNAL, 1, BASE_NS)
        .await
        .expect("fence acme at the activation's epoch");
    let clock = ManualClock::new();
    let router = R::build(Arc::clone(&store), Arc::clone(&clock), policy);
    let counts = router.counts();

    router.write_one(acme.clone()).await;
    eventually(|| counts().buffered >= 1, "the row is buffered").await;
    let before = signal_objects(store.as_ref(), &acme, R::SIGNAL).await;
    assert_eq!(objects(&keys(store.as_ref(), &acme, R::SIGNAL).await), 0);
    compat_clock
        .0
        .fetch_add(COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);

    let torn_down = spawn_teardown(router, teardown);
    eventually(
        || counts().flushes_manual >= 1,
        "the teardown drain opened the flush",
    )
    .await;
    clock.step(15).await;
    assert_eq!(
        counts().refused.total(),
        0,
        "{teardown:?}: inside its lifetime the teardown flush waits"
    );
    assert_eq!(
        objects(&keys(store.as_ref(), &acme, R::SIGNAL).await),
        0,
        "{teardown:?}: no PUT while the view is stale"
    );

    clock.step(16).await;
    eventually(
        || counts().refused.view_stale == 1,
        "refused view_stale past the lifetime",
    )
    .await;
    tokio::time::timeout(Duration::from_secs(30), torn_down)
        .await
        .expect("the teardown ends")
        .expect("teardown task");
    let after = counts();
    assert_eq!(after.refused.total(), 1, "{teardown:?}: {after:?}");
    assert_eq!(after.flushes_manual, 1, "{teardown:?}: {after:?}");
    assert_eq!(
        signal_objects(store.as_ref(), &acme, R::SIGNAL).await,
        before,
        "{teardown:?}: the teardown wrote nothing"
    );
    assert_eq!(
        bucket_compat_epoch(store.as_ref(), &acme, R::SIGNAL).await,
        1
    );
}

/// #4362, ADR-4233 decision 1, on `R`'s path: on a store that never ran
/// compat init the teardown flush publishes the acknowledged buffered row at
/// the baseline, as v0.33.0 did, whether the view read `sys/compat` absent or
/// is blind (its only read refused `AccessDenied`) and past the horizon.
/// Nothing is refused, the bucket stays unfenced, and no `sys/compat` object
/// appears.
async fn teardown_on_a_never_activated_store<R: TeardownRouter>(
    teardown: Teardown,
    get: CompatGet,
) {
    let acme = TenantId::new("acme");
    let compat_store = Arc::new(CompatGetStore::new(get));
    let store: Arc<dyn ObjectStoreBackend> = compat_store.clone();
    ravel_catalog::validate_or_adopt(
        store.as_ref(),
        &acme.hash(),
        R::SIGNAL,
        1,
        BASE_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    let compat_keys = CompatKeys::from_file_keys(&[[4; 32]]).expect("one key");
    let compat_clock = Arc::new(CompatTestClock(AtomicI64::new(BASE_NS)));
    let view = CompatView::new(Arc::clone(&store), compat_keys, compat_clock.clone());
    match get {
        CompatGet::AccessDenied => {
            let err = view.refresh().await.expect_err("denied");
            assert!(err.is_access_denied());
            compat_clock
                .0
                .fetch_add(COMPAT_VIEW_HORIZON_NS, Ordering::SeqCst);
        }
        CompatGet::Passthrough | CompatGet::ServerError => {
            assert!(!view.refresh().await.expect("absent reads").present);
        }
    }
    let policy = Arc::new(IngestCompatPolicy::new(view, compat_clock));
    let clock = ManualClock::new();
    let router = R::build(Arc::clone(&store), Arc::clone(&clock), policy);
    let counts = router.counts();

    router.write_one(acme.clone()).await;
    eventually(|| counts().buffered >= 1, "the row is buffered").await;
    assert_eq!(objects(&keys(store.as_ref(), &acme, R::SIGNAL).await), 0);

    let torn_down = spawn_teardown(router, teardown);
    tokio::time::timeout(Duration::from_secs(30), torn_down)
        .await
        .expect("the teardown ends")
        .expect("teardown task");
    let mut stored = 0;
    for _ in 0..20_000 {
        stored = objects(&keys(store.as_ref(), &acme, R::SIGNAL).await);
        if stored == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let case = format!("{teardown:?}, {get:?}");
    assert_eq!(stored, 2, "{case}: the teardown flush published");
    assert_eq!(
        committed_versions(store.as_ref(), &acme, R::SIGNAL).await,
        vec![R::FAMILY.writable_version()],
        "{case}"
    );
    let after = counts();
    assert_eq!(after.refused.total(), 0, "{case}: {after:?}");
    assert_eq!(after.flushes_manual, 1, "{case}: {after:?}");
    assert_eq!(
        bucket_compat_epoch(store.as_ref(), &acme, R::SIGNAL).await,
        0,
        "{case}"
    );
    assert!(
        list_all(store.as_ref(), "sys/")
            .await
            .expect("list sys/")
            .is_empty(),
        "{case}: no sys/compat object"
    );
}

#[tokio::test]
async fn a_log_teardown_flush_under_a_stale_view_after_an_activation_writes_nothing() {
    teardown_under_a_stale_view::<LogIngestRouter>(Teardown::Shutdown).await;
    teardown_under_a_stale_view::<LogIngestRouter>(Teardown::Drop).await;
}

#[tokio::test]
async fn a_span_teardown_flush_under_a_stale_view_after_an_activation_writes_nothing() {
    teardown_under_a_stale_view::<SpanIngestRouter>(Teardown::Shutdown).await;
    teardown_under_a_stale_view::<SpanIngestRouter>(Teardown::Drop).await;
}

#[tokio::test]
async fn a_log_teardown_flush_on_a_never_activated_store_publishes_at_the_baseline() {
    for teardown in [Teardown::Shutdown, Teardown::Drop] {
        for get in [CompatGet::Passthrough, CompatGet::AccessDenied] {
            teardown_on_a_never_activated_store::<LogIngestRouter>(teardown, get).await;
        }
    }
}

#[tokio::test]
async fn a_span_teardown_flush_on_a_never_activated_store_publishes_at_the_baseline() {
    for teardown in [Teardown::Shutdown, Teardown::Drop] {
        for get in [CompatGet::Passthrough, CompatGet::AccessDenied] {
            teardown_on_a_never_activated_store::<SpanIngestRouter>(teardown, get).await;
        }
    }
}
