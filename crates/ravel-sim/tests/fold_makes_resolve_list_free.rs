//! Proves `Catalog::fold` is load-bearing for a no-pinned-tokens `resolve`
//! (the exact call `check_visible`'s strict-ack-implies-durable check makes
//! through `QueryEngine::instant` -- see `src/driver.rs`): before the fold,
//! resolving the whole ingested window depends entirely on the per-bucket
//! commit-record LIST fallback (`Catalog::resolve`'s per-(shard, hour) listing
//! pass); after the fold, a snapshot
//! watermark covering the whole window serves it from the folded snapshot's
//! GET-based part index and never lists the commit-record prefix at all.
//!
//! `tests/ack_implies_durable_and_token_resolves.rs` used to carry a
//! narrower version of this claim, backed only by "comment out the fold call
//! and watch the test fail". That narrative was false: commenting out the
//! fold there left the test passing, because that test's `resolve` calls are
//! all per-token (`catalog.resolve(..., min_tokens=[token], ...)`), and
//! `Catalog::resolve_min_token` satisfies a per-token resolve with a direct
//! GET on that token's own commit-record key (`commit_key_for_token`) --
//! satisfied whether or not any fold ever ran, since fold does not delete or
//! move the L0 commit record it read. The fold's only observable effect is
//! on a resolve with **no** pinned tokens, which is exactly what
//! `check_visible`'s `AckDurable` check exercises indirectly, through
//! `QueryEngine::instant`'s internal empty-`min_tokens` resolve. This test
//! calls that same no-pinned-tokens `Catalog::resolve` directly so the claim
//! can be checked and fault-injected without going through the query engine.
//!
//! Technique: ingest through a real `IngestRouter`/`Catalog` pair over a
//! plain `MemoryStore`, then copy the store's contents into two fresh
//! `MemoryStore`s -- one snapshotted right after ingest (before the fold),
//! one snapshotted right after the fold -- and wrap each copy in a
//! `FaultStore` that turns every `list()`/`list_delimited()` call whose key
//! contains `/c/` (the commit-record prefix; data objects live under `/l0/`
//! and never match) into a `Permanent` error. A fresh `Catalog` is then
//! built over each faulted copy and asked to `resolve` the whole window with
//! `min_tokens: &[]`.
//!
//! "prove-the-test", both directions actually run (not just asserted):
//! - Before-fold copy: `resolve` returns `Err` (verified below), and
//!   `fault_store.fault_count(Op::List, FaultKind::Permanent)` is asserted
//!   `> 0` -- the commit-bucket LIST fallback was attempted and hit the
//!   fault, proving it was the only path that could have satisfied this
//!   resolve.
//! - After-fold copy: `resolve` returns `Ok` (verified below) and
//!   `fault_count(Op::List, FaultKind::Permanent)` is asserted `== 0` -- the
//!   folded snapshot's part index satisfied the whole window without ever
//!   listing the commit-record prefix, so the fold is genuinely load-bearing
//!   here, not just "also present".

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_ingest::{Clock, IngestConfig, IngestPoint, IngestRouter, IngestValue, WriteMode};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
// Aligned to an hour boundary so the whole sample window, plus a
// `resolve_now_ns` a few seconds and one `clock_skew_allowance_ns` past it,
// stays inside one ingest hour -- see `resolve_now_ns` below.
const START_TS_NS: i64 = (1_700_000_000_000_000_000 / NS_PER_HOUR) * NS_PER_HOUR;

/// Test-local stand-in for `ravel-sim`'s `SimClock`: `now_ns` must actually
/// advance, unlike a truly fixed clock, because the ingest shard actor's
/// age-based strict-mode flush (`shard.rs`'s `flush_aged`, driven by its
/// periodic `clock.sleep(until)` tick) computes a buffered point's age as
/// `now_ns() - arrival_ns`. A clock whose `now_ns()` never changes leaves
/// that age at zero forever, so a small write (well under
/// `target_bytes`, the only other flush trigger) would never flush and its
/// strict-mode ack would time out. `sleep` advances the counter by the
/// slept duration once the tokio (paused, auto-advancing) timer completes,
/// exactly mirroring `src/driver.rs`'s `SimClock`.
struct AdvancingClock {
    now_ns: AtomicI64,
}

impl AdvancingClock {
    fn new(start_ns: i64) -> Self {
        AdvancingClock {
            now_ns: AtomicI64::new(start_ns),
        }
    }
}

impl Clock for AdvancingClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.load(Ordering::SeqCst)
    }

    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            tokio::time::sleep(dur).await;
            self.now_ns
                .fetch_add(dur.as_nanos() as i64, Ordering::SeqCst);
        })
    }
}

/// The `now_ns` a fold must be given for `end_ts_ns`'s hour to be sealed.
/// Mirrors `src/driver.rs`'s `seal_now_ns`, duplicated here (three lines) so
/// this test does not need `driver.rs` to widen its public surface.
fn seal_now_ns(end_ts_ns: i64) -> i64 {
    let margin_ns = DEFAULT_MAX_FLUSH_LIFETIME_NS
        + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
        + DEFAULT_FOLD_SAFETY_MARGIN_NS;
    let hour = end_ts_ns.div_euclid(NS_PER_HOUR);
    (hour + 1) * NS_PER_HOUR + margin_ns
}

/// Copies every object currently in `from` into a fresh `MemoryStore`.
async fn snapshot_store(from: &dyn ObjectStoreBackend) -> MemoryStore {
    let copy = MemoryStore::new();
    for meta in list_all(from, "").await.expect("list_all on source store") {
        let got = from
            .get(&meta.key, GetRange::Full)
            .await
            .unwrap_or_else(|e| panic!("get {} from source store: {e}", meta.key));
        copy.put(&meta.key, got.data, PutOptions::default())
            .await
            .unwrap_or_else(|e| panic!("put {} into snapshot store: {e}", meta.key));
    }
    copy
}

fn commit_bucket_fault_plan() -> FaultPlan {
    FaultPlan::empty().with_rule(
        Rule::new(
            Op::List,
            ScriptedFault::Permanent("F1 test: commit-bucket LIST disabled".to_string()),
        )
        .with_key_contains("/c/")
        .with_occurrence(Occurrence::Always),
    )
}

#[test]
fn fold_makes_resolve_list_free() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("build paused runtime");
    rt.block_on(async {
        let store = Arc::new(MemoryStore::new());
        store.set_clock_ms((START_TS_NS / 1_000_000).max(0) as u64);
        let backend: Arc<dyn ObjectStoreBackend> = store;

        let clock: Arc<dyn Clock> = Arc::new(AdvancingClock::new(START_TS_NS));
        let catalog_config = CatalogConfig {
            shard_count: 2,
            ..CatalogConfig::default()
        };
        let catalog = Catalog::new(Arc::clone(&backend), catalog_config).expect("catalog config");

        let tenant = TenantId::new("fold-load-bearing-tenant");
        let tenant_hash = tenant.hash();
        let labels = LabelSet::new(vec![
            Label {
                name: "__name__".to_string(),
                value: "sim_probe".to_string(),
            },
            Label {
                name: "shape".to_string(),
                value: "f1".to_string(),
            },
        ])
        .expect("valid label set");
        let series_id = SeriesId::compute(&tenant, "sim_probe", &labels).expect("series id");

        let ingest_config = IngestConfig {
            shard_count: 2,
            ..IngestConfig::default()
        };
        let router = IngestRouter::new(
            ingest_config,
            Arc::clone(&backend),
            Signal::Metrics,
            Arc::clone(&clock),
        );

        let samples_end_ts_ns = START_TS_NS + 3 * 1_000_000_000;
        let points: Vec<IngestPoint> = (0..4)
            .map(|i| IngestPoint {
                series_id,
                labels: labels.clone(),
                value: IngestValue::Scalar(Sample {
                    ts_ns: START_TS_NS + i * 1_000_000_000,
                    value: i as f64,
                }),
            })
            .collect();
        router
            .write_values(
                tenant.clone(),
                points,
                WriteMode::Strict,
                Duration::from_secs(5),
            )
            .await
            .expect("strict ingest");

        let full_range = TimeRange {
            start_ns: START_TS_NS,
            end_ns: samples_end_ts_ns,
        };
        let fold_now_ns = seal_now_ns(samples_end_ts_ns);
        // Deliberately NOT `fold_now_ns`: that value is `seal_now_ns`'s
        // one-hour-plus-margins-past-`end_ts_ns` figure, needed only so the
        // fold call itself trusts the sample hour as sealed. Handing that
        // same inflated value to `resolve` as `now_ns` would widen its
        // window past the watermark hour (`window_hour_bounds`: `now_ns +
        // clock_skew_allowance_ns`), forcing a real per-bucket LIST of the
        // still-open trailing hour(s) regardless of the fold -- an
        // unavoidable, correct cost of scanning past what any snapshot can
        // cover, not something F1 claims the fold eliminates. A caller
        // resolving shortly after the data landed uses a `now_ns` near the
        // data's own event time instead, which is what this is.
        let resolve_now_ns = samples_end_ts_ns + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;

        // Snapshot before the fold.
        let before_copy = snapshot_store(backend.as_ref()).await;
        let before_fault = Arc::new(FaultStore::new(before_copy, commit_bucket_fault_plan()));
        let before_backend: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&before_fault) as Arc<dyn ObjectStoreBackend>;
        let before_catalog = Catalog::new(before_backend, catalog_config).expect("catalog config");

        // Run the real fold against the real (unfaulted) store.
        catalog
            .fold(
                &tenant_hash,
                Signal::Metrics,
                Uuid::new_v4(),
                fold_now_ns,
                &[],
            )
            .await
            .expect("fold");

        // Snapshot after the fold.
        let after_copy = snapshot_store(backend.as_ref()).await;
        let after_fault = Arc::new(FaultStore::new(after_copy, commit_bucket_fault_plan()));
        let after_backend: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&after_fault) as Arc<dyn ObjectStoreBackend>;
        let after_catalog = Catalog::new(after_backend, catalog_config).expect("catalog config");

        // Before-fold: no snapshot exists yet, so a no-pinned-tokens resolve
        // has only the per-bucket commit-record LIST fallback to satisfy it,
        // and that fallback is faulted. Expect failure, and prove the fault
        // actually fired rather than the call succeeding some other way.
        let before_result = before_catalog
            .resolve(
                &tenant_hash,
                Signal::Metrics,
                full_range,
                &[],
                resolve_now_ns,
            )
            .await;
        assert!(
            before_result.is_err(),
            "expected the before-fold copy's resolve to fail with the commit-bucket LIST \
             faulted, got {before_result:?}"
        );
        let before_list_faults = before_fault.fault_count(Op::List, FaultKind::Permanent);
        assert!(
            before_list_faults > 0,
            "expected the before-fold resolve to hit the faulted commit-bucket LIST at least \
             once (it did not, so something other than the LIST fallback must have satisfied \
             or failed this resolve)"
        );

        // After-fold: the folded snapshot's watermark covers the whole
        // window, so resolve must be served entirely from its GET-based part
        // index. Expect success, with the commit-bucket LIST fault never
        // firing.
        let after_result = after_catalog
            .resolve(
                &tenant_hash,
                Signal::Metrics,
                full_range,
                &[],
                resolve_now_ns,
            )
            .await;
        assert!(
            after_result.is_ok(),
            "expected the after-fold copy's resolve to succeed from the folded snapshot alone, \
             got {after_result:?}"
        );
        let after_list_faults = after_fault.fault_count(Op::List, FaultKind::Permanent);
        assert_eq!(
            after_list_faults, 0,
            "expected the after-fold resolve to never list the commit-bucket prefix (it should \
             be served entirely from the folded snapshot's GET-based part index), but the \
             commit-bucket LIST fault fired {after_list_faults} time(s)"
        );
    });
}
