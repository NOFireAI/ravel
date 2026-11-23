//! `stats.timings` splits `resolveMs` into `resolveGenerationsMs`,
//! `resolveListWaveMs`, `resolveRecordsMs` and `resolveOtherMs` (issue #4634).
//!
//! Each test holds one store request of the resolve for [`DELAY`] with a
//! `FaultStore` gate and checks that the delay lands in the part that owns the
//! request and in no other. The tenant is a small unfolded logs tenant with
//! provisioning enforcement on, so every resolve reads the provisioning record
//! once and lists `del/` once; a warm-up query before the gate is armed fills
//! the once-per-pair `shard_count` check and the HEAD cache.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ravel_catalog::{AbsentPolicy, Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::stats_json::timings_json;
use ravel_sql::{SqlConfig, SqlExecutor};
use ravel_types::Signal;
use serde_json::Value as Json;

use crate::util::{self, NOW_NS, request, tenant_id};

/// How long the gated request is held.
const DELAY: Duration = Duration::from_millis(200);

/// The ceiling for a part the delay must not reach.
const UNDELAYED_MAX_MS: f64 = 100.0;

/// How far the four parts' sum may sit from `resolveMs`.
const SUM_TOLERANCE_MS: f64 = 5.0;

async fn fixture() -> (Arc<FaultStore<MemoryStore>>, SqlExecutor) {
    let faulty = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
    let tenant = tenant_id("acme");
    util::publish_logs(store.as_ref(), &tenant, 0, &[100, 200, 300]).await;
    util::publish_logs(store.as_ref(), &tenant, 1, &[400, 500]).await;
    let config = CatalogConfig::default();
    ravel_catalog::validate_or_adopt(
        store.as_ref(),
        &tenant.hash(),
        Signal::Logs,
        config.shard_count,
        NOW_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("write the provisioning record");
    let catalog = Arc::new(
        Catalog::new(Arc::clone(&store), config)
            .expect("catalog")
            .with_provisioning_enforcement(),
    );
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        ravel_sql::SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    );
    (faulty, executor)
}

/// Releases each call `gate` holds once it has been held for [`DELAY`], and
/// counts the releases.
fn delay_each(gate: GateHandle) -> (tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
    let released = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&released);
    let task = tokio::spawn(async move {
        loop {
            gate.wait_until_held(1).await;
            tokio::time::sleep(DELAY).await;
            for id in gate.held() {
                if gate.release(id) {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    });
    (task, released)
}

/// Runs the logs query once unheld, then arms the gate `arm` registers and
/// runs it again, returning the second run's `stats.timings`.
async fn timings_with_delay(arm: impl FnOnce(&FaultStore<MemoryStore>) -> GateHandle) -> Json {
    let (store, executor) = fixture().await;
    let tenant = tenant_id("acme");
    let sql = request("SELECT count(*) AS n FROM logs");
    executor
        .execute(tenant.hash(), &sql)
        .await
        .expect("warm-up query");

    let (releaser, released) = delay_each(arm(&store));
    let outcome = executor
        .execute(tenant.hash(), &sql)
        .await
        .expect("delayed query");
    releaser.abort();

    assert_eq!(outcome.stats.resolves, 1, "one resolve per statement");
    assert_eq!(
        released.load(Ordering::SeqCst),
        1,
        "the delayed request is issued exactly once per resolve"
    );
    timings_json(&outcome.stats, Duration::ZERO)
}

fn part(timings: &Json, key: &str) -> f64 {
    timings[key]
        .as_f64()
        .unwrap_or_else(|| panic!("{key} missing from {timings}"))
}

fn assert_parts_sum_to_resolve(timings: &Json) {
    let sum = part(timings, "resolveGenerationsMs")
        + part(timings, "resolveListWaveMs")
        + part(timings, "resolveRecordsMs")
        + part(timings, "resolveOtherMs");
    let resolve = part(timings, "resolveMs");
    assert!(
        (sum - resolve).abs() <= SUM_TOLERANCE_MS,
        "the parts sum to {sum} ms, resolveMs is {resolve} ms: {timings}"
    );
}

/// A delay on the `del/` LIST shows up in `resolveListWaveMs` and in neither
/// `resolveRecordsMs` nor `resolveGenerationsMs`.
#[tokio::test]
async fn a_slow_del_list_lands_in_the_list_wave() {
    let timings = timings_with_delay(|store| {
        store.hold(Op::List, Some("/del/".to_string()), Occurrence::Always)
    })
    .await;

    // hygiene-allow: wall-clock -- the parts are real monotonic durations by
    // design; the held part has a lower bound only, and the others an upper
    // bound half the delay wide.
    assert!(
        part(&timings, "resolveListWaveMs") >= DELAY.as_secs_f64() * 1_000.0,
        "{timings}"
    );
    assert!(
        part(&timings, "resolveRecordsMs") < UNDELAYED_MAX_MS,
        "{timings}"
    );
    assert!(
        part(&timings, "resolveGenerationsMs") < UNDELAYED_MAX_MS,
        "{timings}"
    );
    assert_parts_sum_to_resolve(&timings);
}

/// A delay on the provisioning record GET shows up in `resolveGenerationsMs`
/// alone.
#[tokio::test]
async fn a_slow_provisioning_get_lands_in_generations() {
    let key = ravel_catalog::provisioning_key(&tenant_id("acme").hash(), Signal::Logs);
    let timings =
        timings_with_delay(|store| store.hold(Op::Get, Some(key), Occurrence::Always)).await;

    // hygiene-allow: wall-clock -- as above.
    assert!(
        part(&timings, "resolveGenerationsMs") >= DELAY.as_secs_f64() * 1_000.0,
        "{timings}"
    );
    assert!(
        part(&timings, "resolveListWaveMs") < UNDELAYED_MAX_MS,
        "{timings}"
    );
    assert!(
        part(&timings, "resolveRecordsMs") < UNDELAYED_MAX_MS,
        "{timings}"
    );
    assert_parts_sum_to_resolve(&timings);
}
