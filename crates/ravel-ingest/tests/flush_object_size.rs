//! The size trigger measures the bytes that reach the object, not the bytes
//! the buffer holds (docs/ingest.md "Flush triggers").
//!
//! Two figures are derived from the same buffered points and they are not
//! interchangeable. The memory ceiling (ADR-0069) charges
//! `IngestPoint::est_charge_bytes`, which counts the in-memory `Label` struct
//! headers so the process-wide gauge never undercounts what a buffer holds.
//! The size trigger charges the object-bytes estimate, which counts only the
//! payload a flush writes. Driving the trigger from the memory figure fires a
//! flush at a fraction of `target_bytes`, which is what these tests pin.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestMetricsSnapshot, IngestRouter,
    WriteMode,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectMeta, ObjectStoreBackend, list_all};
use ravel_otlp::NormalizedPoint;
use ravel_types::{Signal, TenantId};

/// `target_bytes` for every test here. Small enough that a few thousand
/// samples reach it, large enough that one batch is a fine-grained step.
const TARGET_BYTES: usize = 256 * 1024;

/// Distinct series per write, each carrying `SAMPLES_PER_SERIES` samples: the
/// shape of one buffer window on a metrics tenant, where a scrape contributes
/// a handful of samples to every series it covers.
const SERIES_PER_BATCH: usize = 100;
const SAMPLES_PER_SERIES: usize = 4;
const POINTS_PER_BATCH: usize = SERIES_PER_BATCH * SAMPLES_PER_SERIES;

/// Ten labels per series: `__name__="http_requests_total"` (8 + 19 bytes) plus
/// `k0..k8` (2 bytes) each carrying a 13-byte value, so every series holds
/// 27 + 9 * 15 = 162 bytes of label text.
const LABELS_PER_SERIES: usize = 10;
const LABEL_TEXT_BYTES: usize = 162;

/// One point's charge against the memory ceiling, per
/// `IngestPoint::est_charge_bytes`: 16 bytes for the sample, plus the 48-byte
/// `Label` struct header for each of the ten labels, plus the label text.
/// Labels are charged per point there because the estimate bounds what an
/// admitted request holds before anything is deduplicated.
const PER_POINT_CHARGE: u64 = 16 + 48 * LABELS_PER_SERIES as u64 + LABEL_TEXT_BYTES as u64;

/// Batches that buffer without reaching `target_bytes`, and the one that
/// reaches it. The object-bytes estimate charges 32 bytes plus the label text
/// once per series (194) and 16 bytes per scalar sample, so a batch adds
/// 100 * (194 + 4 * 16) = 25_800 bytes: ten batches reach 258_000, under the
/// 262_144 target, and the eleventh crosses it. Hard-coded rather than
/// computed so a change to either estimator breaks these tests instead of
/// silently moving the flush point.
const BATCHES_BELOW_TARGET: usize = 10;
const TRIGGERING_BATCH: usize = BATCHES_BELOW_TARGET + 1;

/// Pre-registered band for the flushed object, as a fraction of
/// `target_bytes`. This corpus flushes 64_627 bytes, 0.247 of the 262_144
/// target, measured over `MemoryStore`, whose output is deterministic for a
/// fixed corpus. The band brackets that measurement rather than bounding
/// payload sizes in general: a band spanning an order of magnitude accepts a
/// 2.5x shrink, and the shrink is exactly what a regression here looks like.
///
/// What it has to exclude: reintroducing the `size_of::<Label>()` per-label
/// term into the object-bytes estimator (the defect issue #1305 fixed) takes a
/// batch's estimate from 25_800 to 73_800, so the trigger fires on batch 4
/// instead of batch 11 and the object lands near 0.09 of target. Every
/// batch-count assertion in this file still passes under that defect, because
/// those are driven by the estimator on both sides; this band is what goes red.
/// The measurement is recorded rather than asserted to the byte because the
/// object passes through a compressor, so an exact pin would couple this test
/// to a zstd version bump rather than to either estimator.
const BAND_LOW: f64 = 0.20;
const BAND_HIGH: f64 = 0.30;

/// Memory one batch adds to the buffer figure the backstop reads. Unlike
/// `PER_POINT_CHARGE`, which the ceiling charges per admitted point, the buffer
/// counts a series' labels once, the first time it sees that series: 100 *
/// (480 + 162) label bytes plus 400 * 16 sample bytes.
const PER_BATCH_BUFFERED: u64 = 70_600;

/// A process-wide ceiling small enough to expose a backstop calibrated against
/// the 512 MiB default. An eighth of it is 275_000, which is above
/// `TARGET_BYTES`, so the backstop and not the `target_bytes` floor under it is
/// what fires, and which falls between the third and fourth batch.
const SMALL_CEILING: u64 = 2_200_000;

/// Batches whose buffered memory reaches an eighth of `SMALL_CEILING`: three
/// hold 211_800 and four hold 282_400 against the 275_000 backstop.
const BATCHES_BELOW_BACKSTOP: usize = 3;
const BATCHES_TO_BACKSTOP: usize = BATCHES_BELOW_BACKSTOP + 1;

const BASE_TS_NS: i64 = 1_700_000_000_000_000_000;

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: TARGET_BYTES,
        // Age can never fire: the injected clock never advances and both age
        // thresholds sit an hour out, so every flush here is size-triggered.
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

/// One batch of `SERIES_PER_BATCH` fresh series, `SAMPLES_PER_SERIES` samples
/// each. Series are numbered globally so no two batches share one, matching a
/// tenant whose series set turns over across buffer windows.
fn batch(tenant: &TenantId, batch_index: usize) -> Vec<NormalizedPoint> {
    let mut points = Vec::with_capacity(POINTS_PER_BATCH);
    for series in 0..SERIES_PER_BATCH {
        let series_index = batch_index * SERIES_PER_BATCH + series;
        let pairs: Vec<(String, String)> = (0..LABELS_PER_SERIES - 1)
            .map(|i| (format!("k{i}"), format!("instance-{series_index:04}")))
            .collect();
        let labels: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        for sample in 0..SAMPLES_PER_SERIES {
            // Millisecond jitter on a 15 s scrape interval: the ordinary
            // shape, rather than the exactly-periodic best case.
            let jitter_ms = ((series_index * 7 + sample * 13) % 200) as i64;
            let ts_ns = BASE_TS_NS + sample as i64 * 15_000_000_000 + jitter_ms * 1_000_000;
            let value = (series_index * 1_000 + sample * 7) as f64;
            points.push(make_point(
                tenant,
                "http_requests_total",
                &labels,
                ts_ns,
                value,
            ));
        }
    }
    points
}

async fn write_batch(router: &IngestRouter, tenant: &TenantId, index: usize) {
    router
        .write(
            tenant.clone(),
            batch(tenant, index),
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write");
}

/// Writes `batches` batches into a fresh router, then shuts it down. Buffered
/// writes return before the shard has merged them, so the counters are read
/// after shutdown, which drains the shard's queue in order: a flush the
/// corpus triggered is counted under its own trigger, and whatever is left
/// buffered at teardown is counted as `flushes_manual`.
async fn run_batches(batches: usize) -> (IngestMetricsSnapshot, Vec<ObjectMeta>) {
    run_batches_under_ceiling(batches, IngestByteBudgetLimit::Unlimited).await
}

/// [`run_batches`] against a router carrying a configured process-wide byte
/// budget, so the per-buffer memory backstop is a share of `ceiling`.
async fn run_batches_under_ceiling(
    batches: usize,
    ceiling: IngestByteBudgetLimit,
) -> (IngestMetricsSnapshot, Vec<ObjectMeta>) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_TS_NS);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock)
        .with_budget(IngestByteBudget::shared(ceiling));
    let tenant = tenant("acme");

    for index in 0..batches {
        write_batch(&router, &tenant, index).await;
    }
    // `shutdown` consumes the router, so the counters are read through a
    // handle cloned out of it beforehand.
    let metrics = router.metrics_handle();
    router.shutdown().await;
    let snapshot = metrics.snapshot();
    let objects = list_all(store.as_ref(), "t/")
        .await
        .expect("list")
        .into_iter()
        .filter(|o| o.key.contains("/l0/"))
        .collect();
    (snapshot, objects)
}

/// The trigger fires on the batch that takes the object-bytes estimate past
/// `target_bytes`, not on the far earlier batch that takes the memory estimate
/// there. The same corpus charged the memory figure crosses 262_144 during
/// batch 4 (each batch adds 100 * (480 + 162) label bytes and 400 * 16 sample
/// bytes, so 70_600 per batch), which is the defect this pins.
#[tokio::test]
async fn size_trigger_fires_on_object_bytes_not_buffered_bytes() {
    let (below, _) = run_batches(BATCHES_BELOW_TARGET).await;
    assert_eq!(
        below.flushes_by_size, 0,
        "{BATCHES_BELOW_TARGET} batches hold the object-bytes estimate under target_bytes"
    );
    assert_eq!(
        below.flushes_manual, 1,
        "the whole corpus was still buffered when teardown flushed it"
    );

    let (crossing, _) = run_batches(TRIGGERING_BATCH).await;
    assert_eq!(
        crossing.flushes_by_size, 1,
        "batch {TRIGGERING_BATCH} takes the object-bytes estimate past target_bytes"
    );
    assert_eq!(
        crossing.flushes_manual, 0,
        "that flush drained the buffer, so teardown had nothing left to write"
    );
    assert_eq!(crossing.flushes_by_age, 0);
    assert_eq!(crossing.flushes_by_age_adaptive, 0);
}

/// The object that flush writes lands inside the pre-registered band of
/// `target_bytes`, and it is the only data object the corpus produces.
#[tokio::test]
async fn size_flush_fills_the_object_toward_target_bytes() {
    let (_, objects) = run_batches(TRIGGERING_BATCH).await;
    assert_eq!(
        objects.len(),
        1,
        "one size-triggered flush over {TRIGGERING_BATCH} batches wrote one object"
    );

    let size = objects[0].size;
    let low = (TARGET_BYTES as f64 * BAND_LOW) as u64;
    let high = (TARGET_BYTES as f64 * BAND_HIGH) as u64;
    assert!(
        size >= low && size <= high,
        "flushed object is {size} bytes, outside the pre-registered band \
         [{low}, {high}] ({BAND_LOW} to {BAND_HIGH} of target_bytes {TARGET_BYTES})"
    );
}

/// The memory backstop is a share of the ceiling the operator configured, not
/// of the 512 MiB default. On a replica sized with `--max-ingest-buffer-bytes
/// 8000000` one label-heavy buffer flushes at an eighth of that budget instead
/// of filling past the whole of it and shedding every other tenant's write.
#[tokio::test]
async fn memory_backstop_tracks_the_configured_ceiling() {
    // The corpus never approaches target_bytes on the object estimate here:
    // four batches estimate 103_200 against the 262_144 target, so every flush
    // this test sees is the backstop's.
    assert_eq!(SMALL_CEILING / 8, 275_000);
    assert!(
        SMALL_CEILING / 8 > TARGET_BYTES as u64,
        "an eighth of the ceiling, not the target_bytes floor, is the backstop here"
    );
    assert_eq!(
        BATCHES_BELOW_BACKSTOP as u64 * PER_BATCH_BUFFERED,
        211_800,
        "three batches stay under the 275_000 backstop"
    );
    let held = BATCHES_TO_BACKSTOP as u64 * PER_BATCH_BUFFERED;
    assert_eq!(held, 282_400, "the fourth batch crosses it");
    assert!(
        held < SMALL_CEILING,
        "the flushing buffer holds {held} bytes, not the whole {SMALL_CEILING}-byte budget"
    );
    assert_eq!(
        held - PER_BATCH_BUFFERED,
        211_800,
        "it crossed from within one write of an eighth of the budget"
    );

    let ceiling = IngestByteBudgetLimit::Bounded(SMALL_CEILING);
    let (below, _) = run_batches_under_ceiling(BATCHES_BELOW_BACKSTOP, ceiling).await;
    assert_eq!(below.flushes_by_size, 0);
    assert_eq!(
        below.flushes_manual, 1,
        "three batches were still buffered when teardown flushed them"
    );

    let (crossing, _) = run_batches_under_ceiling(BATCHES_TO_BACKSTOP, ceiling).await;
    assert_eq!(
        crossing.flushes_by_size, 1,
        "batch {BATCHES_TO_BACKSTOP} takes the buffer past an eighth of the configured ceiling"
    );
    assert_eq!(crossing.flushes_manual, 0);

    // The same corpus against a ceiling large enough to earn the 64 MiB cap
    // keeps buffering: the ceiling, not the corpus, is what moved the trigger.
    let (roomy, _) = run_batches_under_ceiling(
        BATCHES_TO_BACKSTOP,
        IngestByteBudgetLimit::Bounded(512 * 1024 * 1024),
    )
    .await;
    assert_eq!(
        roomy.flushes_by_size, 0,
        "an eighth of 512 MiB is 64 MiB, far above the {held} bytes this corpus holds"
    );
    assert_eq!(roomy.flushes_manual, 1);
}

/// The memory ceiling keeps charging `est_charge_bytes` while the trigger
/// reads the object-bytes estimate: the gauge holds the conservative figure to
/// the byte across the ten batches that stay under `target_bytes`, then
/// refunds it when the shutdown flush completes.
///
/// That the trigger stays silent over these batches is pinned by
/// `size_trigger_fires_on_object_bytes_not_buffered_bytes`, which reads its
/// counters after a shutdown drain. Asserting it here would read the snapshot
/// while the shard may not have drained its channel, so it would hold under
/// the defect too.
#[tokio::test]
async fn memory_gauge_still_charges_the_conservative_estimate() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_TS_NS);
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
    let router = IngestRouter::new(config(), Arc::clone(&store), Signal::Metrics, clock)
        .with_budget(Arc::clone(&budget));
    let tenant = tenant("acme");

    for index in 0..BATCHES_BELOW_TARGET {
        write_batch(&router, &tenant, index).await;
    }

    assert_eq!(
        budget.in_flight_bytes(),
        BATCHES_BELOW_TARGET as u64 * POINTS_PER_BATCH as u64 * PER_POINT_CHARGE,
        "the ceiling charges est_charge_bytes per point, unchanged by the trigger's estimate"
    );

    router.shutdown().await;
    assert_eq!(
        budget.in_flight_bytes(),
        0,
        "the flush that drained the buffer refunded every charged byte"
    );
}
