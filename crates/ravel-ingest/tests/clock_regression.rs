//! Per-writer monotonic flush clock (ADR-1307, issue #1307).
//!
//! `created_unix_ns` is the primary key of the query-time duplicate-resolution
//! order (docs/catalog-and-mvcc.md "Cross-segment duplicate samples"): among
//! duplicates of one (series, ts), the greatest `(created_unix_ns,
//! writer_epoch, writer_seq, in-page index)` wins. `writer_id` is not part of
//! that comparator; it is only a final tiebreak in the catalog's segment sort.
//! The key is stamped from the flush clock, which carries no ordering guarantee
//! on its own: a backwards wall-clock step (an NTP correction, a manual set)
//! between two flushes of the same writer used to stamp the later flush below
//! the earlier one, so a stale value outranked its own correction. Each shard
//! actor now validates the raw reading, then raises it to a per-writer
//! monotonic floor, absorbing a backwards step within `MAX_FLUSH_CLOCK_HOLD_NS`
//! (counted as `clock_regressions`) and refusing one beyond it (counted as
//! `clock_regressions_refused`, floor re-anchored). The floor is in-process
//! state only, reset to 0 on restart; ADR-1307 records the cross-restart
//! limitation, which `writer_id` does not close.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{RecedingClock, TestClock, build_labels, make_point, tenant};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_ingest::{
    IngestConfig, IngestExemplar, IngestPoint, IngestRouter, LogIngestRouter, LogWriteError,
    MAX_FLUSH_ALL_PASSES, MAX_FLUSH_CLOCK_HOLD_NS, SpanIngestRouter, SpanWriteError, WriteError,
    WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Exemplar, METRIC_NAME_LABEL, SeriesId, Signal, TenantId};

const T0: i64 = 1_700_000_000_000_000_000;
const TEN_MINUTES_NS: i64 = 600_000_000_000;
const ONE_MINUTE_NS: i64 = 60_000_000_000;
/// A backwards step small enough to sit within the production hold bound
/// [`MAX_FLUSH_CLOCK_HOLD_NS`], so the floor absorbs it (counted as
/// `clock_regressions`) instead of refusing. Derived from the production bound,
/// not a literal, so it stays within the bound if the bound moves in either
/// direction (F2: the test must import the production constant, never copy it).
const ABSORBED_STEP_NS: i64 = MAX_FLUSH_CLOCK_HOLD_NS / 2;

/// Reads back the flush-open stamp (`created_unix_ns`) and `writer_seq` a
/// commit token addresses, so a test can compare the exact query-time
/// duplicate-resolution key of two flushes.
async fn created_and_seq(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
    token: &CommitToken,
) -> (i64, u64) {
    let commit_key = keys::commit_key_for_token(&tenant.hash(), signal, token).expect("commit key");
    let bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let rec = record::decode(&bytes).expect("decode commit record");
    (rec.created_unix_ns, rec.writer_seq)
}

fn flush_per_write_config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        // A tiny target forces a size-triggered flush on the first buffered
        // input, so each write reads the flush clock once, deterministically.
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

/// A config where a write buffers rather than flushing: the byte target and
/// both age delays are large and the age tick is long, so nothing flushes until
/// `flush_all`/`shutdown` drains it. Lets a test hold a buffered-mode row across
/// a graceful teardown, which is where the F1 drain-time refusal lives.
fn buffered_config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 64 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_secs(3600),
        ..IngestConfig::default()
    }
}

/// One exemplar for `metric`'s series (no extra labels, so its `series_id`
/// matches `make_point(tenant, metric, &[], ..)`), for the InvalidReading-arm
/// exemplar-drop test.
fn metrics_exemplar(tenant: &TenantId, metric: &str, ts_ns: i64, tag: u8) -> IngestExemplar {
    let labels = build_labels(&[(METRIC_NAME_LABEL, metric)]);
    let series_id = SeriesId::compute(tenant, metric, &labels).expect("series id");
    IngestExemplar {
        series_id,
        exemplar: Exemplar {
            ts_ns,
            value_bits: 1.0f64.to_bits(),
            trace_id: [tag; 16],
            span_id: [tag; 8],
            filtered_attributes: Vec::new(),
        },
    }
}

fn norm_log_record(ts_ns: i64, body: &str) -> NormalizedLogRecord {
    let resource: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    let stream_id = log_stream_id(&resource, "scope", "", &scope_attrs);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "", &scope_attrs);
    NormalizedLogRecord {
        stream_id,
        stream_attrs,
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn norm_span(start_ns: i64, status: StatusCode) -> NormalizedSpan {
    NormalizedSpan {
        trace_id: [7u8; 16],
        span_id: [1u8; 8],
        parent_span_id: None,
        name: "handle".to_string(),
        start_ts_ns: start_ns,
        end_ts_ns: start_ns + 100,
        status_code: status,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}

/// Metrics: a backwards clock step between the write of a sample and the write
/// of its correction must not let the stale sample outrank the correction. The
/// correction's flush-open stamp is held to the earlier flush's stamp (equal,
/// not below), so its full duplicate-resolution key `(created_unix_ns,
/// writer_seq)` is strictly greater via the seq tiebreak, and the step is
/// counted exactly once. Flipping `monotonic_flush_open_ns` back to a raw
/// `now_ns()` read stamps the correction below the original, which inverts both
/// the created-stamp equality and the counted regression.
#[tokio::test]
async fn metrics_backward_step_holds_correction_above_stale_sample() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    let original = router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("original sample flushes");
    let original_token = original.tokens.first().expect("one token").clone();

    // NTP steps the host back, within the hold bound, then the correction for
    // the same (series, ts) is written and flushed.
    clock.set_ns(T0 - ABSORBED_STEP_NS);
    let correction = router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                2.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("correction sample flushes");
    let correction_token = correction.tokens.first().expect("one token").clone();

    let (orig_created, orig_seq) =
        created_and_seq(store.as_ref(), &tenant, Signal::Metrics, &original_token).await;
    let (corr_created, corr_seq) =
        created_and_seq(store.as_ref(), &tenant, Signal::Metrics, &correction_token).await;

    assert_eq!(
        orig_created, T0,
        "the first flush stamps the raw clock, unaltered"
    );
    assert_eq!(
        corr_created, orig_created,
        "the backwards step is absorbed: the correction is stamped at the floor, not below the original"
    );
    assert!(
        corr_seq > orig_seq,
        "the correction has the later writer_seq ({corr_seq} > {orig_seq})"
    );
    assert!(
        (corr_created, corr_seq) > (orig_created, orig_seq),
        "the correction's duplicate-resolution key must outrank the stale sample's"
    );
    assert_eq!(
        router.metrics().snapshot().clock_regressions,
        1,
        "the backwards step is counted exactly once"
    );
    assert_eq!(
        router.metrics().snapshot().clock_regressions_refused,
        0,
        "a step within the hold bound is absorbed, not refused"
    );

    router.shutdown().await;
}

/// Metrics: a forward clock step is not a regression. The later flush stamps
/// the raw reading verbatim and the counter stays at zero.
#[tokio::test]
async fn metrics_forward_step_stamps_raw_and_counts_nothing() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    let first = router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first sample flushes");
    let first_token = first.tokens.first().expect("one token").clone();

    clock.set_ns(T0 + TEN_MINUTES_NS);
    let second = router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                2_000,
                2.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("second sample flushes");
    let second_token = second.tokens.first().expect("one token").clone();

    let (first_created, _) =
        created_and_seq(store.as_ref(), &tenant, Signal::Metrics, &first_token).await;
    let (second_created, _) =
        created_and_seq(store.as_ref(), &tenant, Signal::Metrics, &second_token).await;

    assert_eq!(first_created, T0);
    assert_eq!(
        second_created,
        T0 + TEN_MINUTES_NS,
        "a forward step stamps the raw reading, not the floor"
    );
    assert!(second_created > first_created);
    assert_eq!(
        router.metrics().snapshot().clock_regressions,
        0,
        "a forward step is not a regression"
    );

    router.shutdown().await;
}

/// Logs: mirror of `metrics_forward_step_stamps_raw_and_counts_nothing` in the
/// log shard actor. A forward step stamps the raw reading verbatim and moves
/// neither counter. The log actor's forward-step behaviour otherwise appears
/// only as an unasserted intermediate step inside the refuse tests (ADR-1307
/// finding 4). Flipping `monotonic_flush_open_ns` back to a raw `now_ns()` read
/// leaves the stamp unchanged here (a forward step already stamps raw), so this
/// test guards the counter contract; the backward-step test guards the stamp.
#[tokio::test]
async fn logs_forward_step_stamps_raw_and_counts_nothing() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = LogIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    let first = router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "boot")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first record flushes");
    let first_token = first.tokens.first().expect("one token").clone();

    clock.set_ns(T0 + TEN_MINUTES_NS);
    let second = router
        .write(
            tenant.clone(),
            vec![norm_log_record(2_000, "later")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("second record flushes");
    let second_token = second.tokens.first().expect("one token").clone();

    let (first_created, _) =
        created_and_seq(store.as_ref(), &tenant, Signal::Logs, &first_token).await;
    let (second_created, _) =
        created_and_seq(store.as_ref(), &tenant, Signal::Logs, &second_token).await;

    assert_eq!(first_created, T0);
    assert_eq!(
        second_created,
        T0 + TEN_MINUTES_NS,
        "a forward step stamps the raw reading, not the floor"
    );
    assert!(second_created > first_created);
    assert_eq!(
        router.metrics().snapshot().clock_regressions,
        0,
        "a forward step is not a regression"
    );
    assert_eq!(
        router.metrics().snapshot().clock_regressions_refused,
        0,
        "a forward step is not refused"
    );

    router.shutdown().await;
}

/// Spans: mirror of `metrics_forward_step_stamps_raw_and_counts_nothing` in the
/// span shard actor (ADR-1307 finding 4).
#[tokio::test]
async fn spans_forward_step_stamps_raw_and_counts_nothing() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = SpanIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    let first = router
        .write(
            tenant.clone(),
            vec![norm_span(1_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first span flushes");
    let first_token = first.tokens.first().expect("one token").clone();

    clock.set_ns(T0 + TEN_MINUTES_NS);
    let second = router
        .write(
            tenant.clone(),
            vec![norm_span(2_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("second span flushes");
    let second_token = second.tokens.first().expect("one token").clone();

    let (first_created, _) =
        created_and_seq(store.as_ref(), &tenant, Signal::Spans, &first_token).await;
    let (second_created, _) =
        created_and_seq(store.as_ref(), &tenant, Signal::Spans, &second_token).await;

    assert_eq!(first_created, T0);
    assert_eq!(
        second_created,
        T0 + TEN_MINUTES_NS,
        "a forward step stamps the raw reading, not the floor"
    );
    assert!(second_created > first_created);
    assert_eq!(
        router.metrics().snapshot().clock_regressions,
        0,
        "a forward step is not a regression"
    );
    assert_eq!(
        router.metrics().snapshot().clock_regressions_refused,
        0,
        "a forward step is not refused"
    );

    router.shutdown().await;
}

/// Metrics: the floor is in-process state, never read back after a restart, so
/// it cannot order a restarted process's stamps against the process before it.
/// This is ADR-1307's stated Known limitation, pinned here: after the host
/// clock steps back across a restart, the fresh process stamps the lower raw
/// reading, and a duplicate written post-restart is stamped strictly below the
/// value it supersedes, inverting query-time resolution exactly as the
/// in-process defect would. `writer_id` does not rescue this: it is not part of
/// the duplicate-resolution comparator (`(created_unix_ns, writer_epoch,
/// writer_seq, in-page index)`), only a final tiebreak in the catalog segment
/// sort, so a fresh `writer_id` after the restart decides nothing about which
/// duplicate wins. Closing the limitation needs a persisted floor or a
/// comparator change (ADR-1307, out of scope).
#[tokio::test]
async fn restart_resets_the_floor_and_a_spanning_step_still_inverts() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let tenant = tenant("acme");

    let router1 = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let first = router1
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first process flushes");
    let first_token = first.tokens.first().expect("one token").clone();
    router1.shutdown().await;

    // The process restarts after the host's clock was stepped back.
    clock.set_ns(T0 - TEN_MINUTES_NS);
    let router2 = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let second = router2
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                2.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("second process flushes");
    let second_token = second.tokens.first().expect("one token").clone();

    let (first_created, _) =
        created_and_seq(store.as_ref(), &tenant, Signal::Metrics, &first_token).await;
    let (second_created, _) =
        created_and_seq(store.as_ref(), &tenant, Signal::Metrics, &second_token).await;

    assert_eq!(
        second_created,
        T0 - TEN_MINUTES_NS,
        "the fresh process stamps the raw clock; no floor survives the restart"
    );
    assert!(
        second_created < first_created,
        "the post-restart correction ({second_created}) is stamped below the value it \
         supersedes ({first_created}): the cross-restart step still inverts resolution \
         (ADR-1307 Known limitation)"
    );
    assert_eq!(
        router2.metrics().snapshot().clock_regressions,
        0,
        "the fresh process's counter starts at zero: it absorbed no step, because the \
         floor did not survive the restart to catch this one"
    );

    router2.shutdown().await;
}

/// Logs: same defect, same fix, in the log shard actor. A backwards step
/// between a log record and its correction holds the correction's stamp at the
/// floor and counts the step once.
#[tokio::test]
async fn logs_backward_step_holds_correction_above_stale_record() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = LogIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    let original = router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "boot")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("original record flushes");
    let original_token = original.tokens.first().expect("one token").clone();

    clock.set_ns(T0 - ABSORBED_STEP_NS);
    let correction = router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "boot-corrected")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("correction record flushes");
    let correction_token = correction.tokens.first().expect("one token").clone();

    let (orig_created, orig_seq) =
        created_and_seq(store.as_ref(), &tenant, Signal::Logs, &original_token).await;
    let (corr_created, corr_seq) =
        created_and_seq(store.as_ref(), &tenant, Signal::Logs, &correction_token).await;

    assert_eq!(orig_created, T0);
    assert_eq!(
        corr_created, orig_created,
        "the correction is stamped at the floor, not below the original"
    );
    assert!((corr_created, corr_seq) > (orig_created, orig_seq));
    assert_eq!(router.metrics().snapshot().clock_regressions, 1);
    assert_eq!(router.metrics().snapshot().clock_regressions_refused, 0);

    router.shutdown().await;
}

/// Spans: same defect, same fix, in the span shard actor.
#[tokio::test]
async fn spans_backward_step_holds_correction_above_stale_span() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = SpanIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    let original = router
        .write(
            tenant.clone(),
            vec![norm_span(1_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("original span flushes");
    let original_token = original.tokens.first().expect("one token").clone();

    clock.set_ns(T0 - ABSORBED_STEP_NS);
    let correction = router
        .write(
            tenant.clone(),
            vec![norm_span(1_000, StatusCode::Error)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("correction span flushes");
    let correction_token = correction.tokens.first().expect("one token").clone();

    let (orig_created, orig_seq) =
        created_and_seq(store.as_ref(), &tenant, Signal::Spans, &original_token).await;
    let (corr_created, corr_seq) =
        created_and_seq(store.as_ref(), &tenant, Signal::Spans, &correction_token).await;

    assert_eq!(orig_created, T0);
    assert_eq!(
        corr_created, orig_created,
        "the correction is stamped at the floor, not below the original"
    );
    assert!((corr_created, corr_seq) > (orig_created, orig_seq));
    assert_eq!(router.metrics().snapshot().clock_regressions, 1);
    assert_eq!(router.metrics().snapshot().clock_regressions_refused, 0);

    router.shutdown().await;
}

/// Metrics: the raw reading is plausibility-checked before the floor is
/// consulted. With the floor already armed at `T0`, a sub-floor raw reading
/// (1000 ns, positive but below the 2020 floor) fails the flush with the exact
/// sub-floor error rather than being silently raised to the floor and stamped
/// as a valid-looking `created_unix_ns`. Neither regression counter moves,
/// which is what distinguishes the fix from a floor-first order: raising 1000
/// to `T0` first would take the backwards-step path (a 1000-vs-`T0` gap far
/// beyond the hold bound) and increment `clock_regressions_refused`. Flipping
/// the guard by dropping the raw `checked_ingest_hour_bucket(raw_ns)` in
/// `monotonic_flush_open_ns` makes this fail with the hold-bound error and a
/// nonzero `clock_regressions_refused`.
#[tokio::test]
async fn sub_floor_reading_after_floor_armed_fails_before_the_floor() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first sample arms the floor at T0");

    clock.set_ns(1_000);
    let sub_floor = router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                2_000,
                2.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;

    let err = sub_floor.expect_err("a sub-floor flush reading must fail the flush");
    let msg = err.to_string();
    assert!(
        msg.contains("below the plausibility floor"),
        "expected the sub-floor error, got: {msg}"
    );
    assert!(
        msg.contains("flush_open_ns=1000"),
        "the error names the exact sub-floor reading, got: {msg}"
    );
    assert!(
        !err.is_retryable(),
        "a grossly broken (sub-floor) raw reading is fail-loud, not a transient \
         regression: it surfaces as the non-retryable SegmentBuild, unlike the \
         over-bound refusal which is a retryable Abandoned (ADR-1307); got: {err:?}"
    );

    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.clock_regressions, 0,
        "the raw check fires before the floor logic, so no step is absorbed"
    );
    assert_eq!(
        snap.clock_regressions_refused, 0,
        "the raw check fires before the floor logic, so no step is refused"
    );

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        2,
        "only the first flush published (one data object, one commit record); \
         the sub-floor flush published nothing: {objects:?}"
    );

    router.shutdown().await;
}

/// Metrics: a forward clock glitch beyond the hold bound ratchets the floor,
/// then the correction back toward wall time is a backwards step larger than
/// `MAX_FLUSH_CLOCK_HOLD_NS`. That step is refused (not absorbed) and the floor
/// re-anchors to the raw reading, so the very next normal reading proceeds
/// rather than being pinned forever behind the glitch. Flipping the guard from
/// `held_ns > MAX_FLUSH_CLOCK_HOLD_NS` to always-absorb makes the refused write
/// succeed and leaves `clock_regressions_refused` at zero.
#[tokio::test]
async fn forward_glitch_beyond_bound_is_refused_and_the_floor_re_anchors() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("baseline flush arms the floor at T0");

    // A forward glitch of twice the hold bound ratchets the floor far ahead.
    let glitch_ns = T0 + 2 * MAX_FLUSH_CLOCK_HOLD_NS;
    clock.set_ns(glitch_ns);
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                2_000,
                2.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("a forward step stamps raw and ratchets the floor");

    // The clock corrects back to T0 + 1m: a backwards step from the ratcheted
    // floor of nearly twice the hold bound, so it is refused.
    clock.set_ns(T0 + ONE_MINUTE_NS);
    // F3: `record_flush` runs only after the stamp is decided, so a refused
    // flush does not move the flush counter. Capture it across the refusal.
    let flushes_before_refuse = router.metrics().snapshot().flushes_by_size;
    let refused = router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                3_000,
                3.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    let err = refused.expect_err("a backwards step beyond the bound must fail the flush");
    assert!(
        err.to_string().contains("beyond the monotonic hold bound"),
        "expected the hold-bound error, got: {err}"
    );
    assert!(
        err.is_retryable(),
        "a refused clock regression is a transient server condition: it must be \
         retryable (Abandoned, 503), not a client SegmentBuild (400) that drops \
         the buffered rows on a conformant exporter (ADR-1307 finding 1); got: {err:?}"
    );
    // F5: retryability alone would pass for a wrong retryable variant; pin the
    // variant itself to the refuse path's `Abandoned`.
    assert!(
        matches!(err, WriteError::Abandoned(_)),
        "a refused clock regression must be the Abandoned variant, not merely \
         some retryable error; got: {err:?}"
    );
    assert_eq!(
        router.metrics().snapshot().flushes_by_size,
        flushes_before_refuse,
        "a refused flush is not counted as a flush: the flush counter is flat \
         across it (ADR-1307 finding 1)"
    );

    // The floor re-anchored to T0 + 1m on refusal, so a normal reading above it
    // (T0 + 2m, still far below the glitched floor) proceeds. Absent the
    // re-anchor this would itself be an over-bound backwards step and refuse.
    clock.set_ns(T0 + 2 * ONE_MINUTE_NS);
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                4_000,
                4.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the next reading proceeds: the floor re-anchored, it is not pinned");

    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.clock_regressions_refused, 1,
        "exactly the one over-bound step is refused"
    );
    assert_eq!(
        snap.clock_regressions, 0,
        "no step is within the bound, so nothing is absorbed"
    );

    router.shutdown().await;
}

/// Metrics: a refused flush re-buffers its rows rather than dropping them, so
/// they reach the store on the next flush (ADR-1307 finding 1). A forward
/// glitch ratchets the floor; the correction back toward wall time is refused
/// (a backwards step beyond the bound) and re-anchors the floor to that raw
/// reading. The refused write's rows are re-inserted into the tenant buffer, so
/// the drain flush at `shutdown` -- reading the clock at the re-anchored floor,
/// which now passes -- publishes them. The whole-tenant buffer for that shard is
/// preserved, not just the triggering request. Flipping the refuse arm from
/// re-inserting the buffer to dropping it (the pre-fix behaviour) leaves the
/// tenant empty at shutdown: only the first two flushes' four objects exist and
/// this asserts six.
#[tokio::test]
async fn metrics_refused_flush_re_buffers_rows_for_the_next_flush() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("baseline flush arms the floor at T0");

    // A forward glitch of twice the hold bound ratchets the floor far ahead.
    clock.set_ns(T0 + 2 * MAX_FLUSH_CLOCK_HOLD_NS);
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                2_000,
                2.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("a forward step stamps raw and ratchets the floor");

    let before = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        before.len(),
        4,
        "two successful flushes: two data objects and two commit records: {before:?}"
    );

    // The clock corrects back to T0 + 1m: a backwards step beyond the bound, so
    // it is refused. Its rows are re-buffered, the floor re-anchors to T0 + 1m.
    clock.set_ns(T0 + ONE_MINUTE_NS);
    let refused = router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                3_000,
                3.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    let err = refused.expect_err("a backwards step beyond the bound must fail the flush");
    assert!(
        err.is_retryable(),
        "the refusal is retryable so the caller re-drives the write; got: {err:?}"
    );

    let after_refuse = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_refuse.len(),
        4,
        "the refused flush published nothing itself: {after_refuse:?}"
    );
    assert_eq!(
        router.metrics().snapshot().clock_regressions_refused,
        1,
        "exactly the one over-bound step is refused"
    );

    // The drain flush reads the clock at T0 + 1m, equal to the re-anchored
    // floor, so it proceeds and publishes the re-buffered rows.
    router.shutdown().await;

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        6,
        "the re-buffered rows reached the store on the drain flush (one more data \
         object and commit record); had the refuse arm dropped the buffer, the \
         tenant would be empty at shutdown and only four objects would exist: {objects:?}"
    );
}

/// Metrics: the abandonment deadline derives from the raw reading, not the
/// floor-raised stamp. With `max_flush_lifetime` zero, every flush's deadline
/// equals its raw open reading, so `bound_to_deadline` abandons at once. Under
/// a backwards step the floor raises the stamp to `T0` while the raw reading is
/// `T0 - ABSORBED_STEP_NS`; if the deadline used the raised stamp its budget
/// would be that whole step and the flush would publish. It must not: the
/// deadline uses raw, so the flush abandons and nothing is published. Flipping
/// the deadline source from `raw_ns` to `flush_open_ns` makes the regressed
/// flush publish, so the object listing is no longer empty.
#[tokio::test]
async fn deadline_derives_from_raw_reading_not_the_floor() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let config = IngestConfig {
        max_flush_lifetime: Duration::ZERO,
        ..flush_per_write_config()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());
    let tenant = tenant("acme");

    // Deadline == raw == now at open, so this abandons at once; it still arms
    // the floor to T0 (the floor is set before the put runs).
    let first = router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    assert!(
        first.is_err(),
        "a zero lifetime abandons the first flush at its deadline too"
    );

    clock.set_ns(T0 - ABSORBED_STEP_NS);
    let regressed = router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                2_000,
                2.0,
            )],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    assert!(
        regressed.is_err(),
        "the regressed flush's deadline is raw + 0 = now, so it abandons; a deadline \
         from the floor-raised stamp would give it the whole step of budget and publish"
    );

    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.clock_regressions, 1,
        "the backwards step is still absorbed for the stamp itself"
    );
    assert_eq!(snap.clock_regressions_refused, 0);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.is_empty(),
        "both flushes abandoned at their raw-derived deadlines; nothing is published: \
         {objects:?}"
    );

    router.shutdown().await;
}

/// Logs: mirror of `forward_glitch_beyond_bound_is_refused_and_the_floor_re_anchors`
/// in the log shard actor. A forward glitch ratchets the floor; the correction
/// back toward wall time is a backwards step beyond `MAX_FLUSH_CLOCK_HOLD_NS`,
/// so it is refused, re-anchors the floor, and the refusal surfaces as a
/// retryable error (ADR-1307 finding 1), not a client `SegmentBuild`.
#[tokio::test]
async fn logs_forward_glitch_beyond_bound_is_refused_and_retryable() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = LogIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "boot")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("baseline flush arms the floor at T0");

    let glitch_ns = T0 + 2 * MAX_FLUSH_CLOCK_HOLD_NS;
    clock.set_ns(glitch_ns);
    router
        .write(
            tenant.clone(),
            vec![norm_log_record(2_000, "glitch")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("a forward step stamps raw and ratchets the floor");

    clock.set_ns(T0 + ONE_MINUTE_NS);
    let refused = router
        .write(
            tenant.clone(),
            vec![norm_log_record(3_000, "correction")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    let err = refused.expect_err("a backwards step beyond the bound must fail the flush");
    assert!(
        err.to_string().contains("beyond the monotonic hold bound"),
        "expected the hold-bound error, got: {err}"
    );
    assert!(
        err.is_retryable(),
        "a refused clock regression is retryable (Abandoned, 503), not a client \
         SegmentBuild (400); got: {err:?}"
    );
    // F5: pin the variant, not just retryability.
    assert!(
        matches!(err, LogWriteError::Abandoned(_)),
        "a refused clock regression must be the Abandoned variant; got: {err:?}"
    );

    // Re-anchored to T0 + 1m, so a normal reading above it proceeds.
    clock.set_ns(T0 + 2 * ONE_MINUTE_NS);
    router
        .write(
            tenant.clone(),
            vec![norm_log_record(4_000, "next")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the next reading proceeds: the floor re-anchored, it is not pinned");

    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.clock_regressions_refused, 1,
        "exactly the one over-bound step is refused"
    );
    assert_eq!(
        snap.clock_regressions, 0,
        "no step is within the bound, so nothing is absorbed"
    );

    router.shutdown().await;
}

/// Logs: mirror of `sub_floor_reading_after_floor_armed_fails_before_the_floor`
/// in the log shard actor. With the floor armed at `T0`, a sub-floor raw reading
/// fails the flush with the exact sub-floor error before the floor is consulted,
/// moves neither regression counter, and is fail-loud (non-retryable), so the
/// sub-floor flush publishes nothing.
#[tokio::test]
async fn logs_sub_floor_reading_after_floor_armed_fails_before_the_floor() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = LogIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "boot")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first record arms the floor at T0");

    clock.set_ns(1_000);
    let sub_floor = router
        .write(
            tenant.clone(),
            vec![norm_log_record(2_000, "sub-floor")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    let err = sub_floor.expect_err("a sub-floor flush reading must fail the flush");
    let msg = err.to_string();
    assert!(
        msg.contains("below the plausibility floor"),
        "expected the sub-floor error, got: {msg}"
    );
    assert!(
        msg.contains("flush_open_ns=1000"),
        "the error names the exact sub-floor reading, got: {msg}"
    );
    assert!(
        !err.is_retryable(),
        "a sub-floor reading is fail-loud (non-retryable SegmentBuild), not a \
         transient regression; got: {err:?}"
    );

    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_regressions, 0);
    assert_eq!(snap.clock_regressions_refused, 0);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        2,
        "only the first flush published; the sub-floor flush published nothing: {objects:?}"
    );

    router.shutdown().await;
}

/// Spans: mirror of the refuse-path test in the span shard actor.
#[tokio::test]
async fn spans_forward_glitch_beyond_bound_is_refused_and_retryable() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = SpanIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("baseline flush arms the floor at T0");

    let glitch_ns = T0 + 2 * MAX_FLUSH_CLOCK_HOLD_NS;
    clock.set_ns(glitch_ns);
    router
        .write(
            tenant.clone(),
            vec![norm_span(2_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("a forward step stamps raw and ratchets the floor");

    clock.set_ns(T0 + ONE_MINUTE_NS);
    let refused = router
        .write(
            tenant.clone(),
            vec![norm_span(3_000, StatusCode::Error)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    let err = refused.expect_err("a backwards step beyond the bound must fail the flush");
    assert!(
        err.to_string().contains("beyond the monotonic hold bound"),
        "expected the hold-bound error, got: {err}"
    );
    assert!(
        err.is_retryable(),
        "a refused clock regression is retryable (Abandoned, 503), not a client \
         SegmentBuild (400); got: {err:?}"
    );
    // F5: pin the variant, not just retryability.
    assert!(
        matches!(err, SpanWriteError::Abandoned(_)),
        "a refused clock regression must be the Abandoned variant; got: {err:?}"
    );

    clock.set_ns(T0 + 2 * ONE_MINUTE_NS);
    router
        .write(
            tenant.clone(),
            vec![norm_span(4_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the next reading proceeds: the floor re-anchored, it is not pinned");

    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.clock_regressions_refused, 1,
        "exactly the one over-bound step is refused"
    );
    assert_eq!(
        snap.clock_regressions, 0,
        "no step is within the bound, so nothing is absorbed"
    );

    router.shutdown().await;
}

/// Spans: mirror of the raw-first sub-floor test in the span shard actor.
#[tokio::test]
async fn spans_sub_floor_reading_after_floor_armed_fails_before_the_floor() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = SpanIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first span arms the floor at T0");

    clock.set_ns(1_000);
    let sub_floor = router
        .write(
            tenant.clone(),
            vec![norm_span(2_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    let err = sub_floor.expect_err("a sub-floor flush reading must fail the flush");
    let msg = err.to_string();
    assert!(
        msg.contains("below the plausibility floor"),
        "expected the sub-floor error, got: {msg}"
    );
    assert!(
        msg.contains("flush_open_ns=1000"),
        "the error names the exact sub-floor reading, got: {msg}"
    );
    assert!(
        !err.is_retryable(),
        "a sub-floor reading is fail-loud (non-retryable SegmentBuild), not a \
         transient regression; got: {err:?}"
    );

    let snap = router.metrics().snapshot();
    assert_eq!(snap.clock_regressions, 0);
    assert_eq!(snap.clock_regressions_refused, 0);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        2,
        "only the first flush published; the sub-floor flush published nothing: {objects:?}"
    );

    router.shutdown().await;
}

/// Logs: mirror of `metrics_refused_flush_re_buffers_rows_for_the_next_flush`.
/// A refused flush re-buffers its records rather than dropping them, so the
/// drain flush at `shutdown` publishes them (ADR-1307 finding 1). Flipping the
/// log refuse arm from `self.tenants.insert(tenant, buf)` to `drop(buf)` leaves
/// the tenant empty at shutdown: only the first two flushes' four objects exist,
/// so this assertion of six fails.
#[tokio::test]
async fn logs_refused_flush_re_buffers_records_for_the_next_flush() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = LogIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "boot")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("baseline flush arms the floor at T0");

    clock.set_ns(T0 + 2 * MAX_FLUSH_CLOCK_HOLD_NS);
    router
        .write(
            tenant.clone(),
            vec![norm_log_record(2_000, "glitch")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("a forward step stamps raw and ratchets the floor");

    let before = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        before.len(),
        4,
        "two successful flushes: two data objects and two commit records: {before:?}"
    );

    clock.set_ns(T0 + ONE_MINUTE_NS);
    let refused = router
        .write(
            tenant.clone(),
            vec![norm_log_record(3_000, "correction")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    let err = refused.expect_err("a backwards step beyond the bound must fail the flush");
    assert!(
        err.is_retryable(),
        "the refusal is retryable so the caller re-drives the write; got: {err:?}"
    );

    let after_refuse = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_refuse.len(),
        4,
        "the refused flush published nothing itself: {after_refuse:?}"
    );
    assert_eq!(router.metrics().snapshot().clock_regressions_refused, 1);

    // The drain flush reads the clock at T0 + 1m, equal to the re-anchored
    // floor, so it proceeds and publishes the re-buffered records.
    router.shutdown().await;

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        6,
        "the re-buffered records reached the store on the drain flush (one more \
         data object and commit record); had the refuse arm dropped the buffer, \
         only four objects would exist: {objects:?}"
    );
}

/// Spans: mirror of `metrics_refused_flush_re_buffers_rows_for_the_next_flush`.
/// A refused flush re-buffers its spans rather than dropping them, so the drain
/// flush at `shutdown` publishes them (ADR-1307 finding 1). Flipping the span
/// refuse arm from `self.tenants.insert(tenant, buf)` to `drop(buf)` leaves the
/// tenant empty at shutdown: only the first two flushes' four objects exist, so
/// this assertion of six fails.
#[tokio::test]
async fn spans_refused_flush_re_buffers_spans_for_the_next_flush() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(T0);
    let router = SpanIngestRouter::new(flush_per_write_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");

    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("baseline flush arms the floor at T0");

    clock.set_ns(T0 + 2 * MAX_FLUSH_CLOCK_HOLD_NS);
    router
        .write(
            tenant.clone(),
            vec![norm_span(2_000, StatusCode::Unset)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("a forward step stamps raw and ratchets the floor");

    let before = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        before.len(),
        4,
        "two successful flushes: two data objects and two commit records: {before:?}"
    );

    clock.set_ns(T0 + ONE_MINUTE_NS);
    let refused = router
        .write(
            tenant.clone(),
            vec![norm_span(3_000, StatusCode::Error)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    let err = refused.expect_err("a backwards step beyond the bound must fail the flush");
    assert!(
        err.is_retryable(),
        "the refusal is retryable so the caller re-drives the write; got: {err:?}"
    );

    let after_refuse = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_refuse.len(),
        4,
        "the refused flush published nothing itself: {after_refuse:?}"
    );
    assert_eq!(router.metrics().snapshot().clock_regressions_refused, 1);

    router.shutdown().await;

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        6,
        "the re-buffered spans reached the store on the drain flush (one more \
         data object and commit record); had the refuse arm dropped the buffer, \
         only four objects would exist: {objects:?}"
    );
}

/// F1 (metrics): a refusal that happens DURING the graceful drain, not on a
/// write. A buffered-mode row sits unflushed; `shutdown`'s own drain flush is
/// the one refused, because a forward glitch armed the floor far above the
/// drain's clock reading. `flush_all` must retry over a fresh snapshot -- the
/// refusal re-anchored the floor to the drain reading, so the second pass stamps
/// it and proceeds -- so the buffered row still reaches the store. Reverting
/// `flush_all` to a single snapshot drops the re-buffered tenant on teardown:
/// the row never publishes and only the floor-arming flush's two objects exist,
/// so this assertion of four fails.
#[tokio::test]
async fn metrics_flush_all_retries_a_refusal_during_the_drain() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    // Arm the floor far ahead so the later drain reading is a backwards step
    // beyond the hold bound.
    let armed_ns = T0 + 2 * MAX_FLUSH_CLOCK_HOLD_NS;
    let clock = TestClock::new(armed_ns);
    let router = IngestRouter::new(
        buffered_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");
    // The refusal is counted during `shutdown`, which consumes the router, so
    // read the counter through a shared handle taken beforehand.
    let metrics = router.metrics_handle();

    // Buffered write A, then flush it: this arms the floor at `armed_ns`.
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write A accepted");
    router.flush_all().await;

    let after_arm = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_arm.len(),
        2,
        "flushing A arms the floor and publishes one data object and commit record: {after_arm:?}"
    );

    // Buffered write B stays in the shard buffer (large byte target, long
    // delays): only the drain will flush it.
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                2_000,
                2.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write B accepted");

    // The clock corrects far back: the drain's first flush attempt for B is a
    // backwards step beyond the bound, so it is refused inside `flush_all`.
    clock.set_ns(T0);
    router.shutdown().await;

    assert_eq!(
        metrics.snapshot().clock_regressions_refused,
        1,
        "the drain's first flush attempt for B is refused exactly once"
    );
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        4,
        "the drain retried the refusal and published B (one more data object and \
         commit record); a single-snapshot flush_all would have dropped B and \
         left only two objects: {objects:?}"
    );
}

/// F1 (logs): the log drain retries a refusal that happens during it. Same shape
/// as the metrics F1 test; reverting the log `flush_all` to a single snapshot
/// drops the re-buffered tenant and this assertion of four fails.
#[tokio::test]
async fn logs_flush_all_retries_a_refusal_during_the_drain() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let armed_ns = T0 + 2 * MAX_FLUSH_CLOCK_HOLD_NS;
    let clock = TestClock::new(armed_ns);
    let router = LogIngestRouter::new(buffered_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    let metrics = router.metrics_handle();

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "a")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write A accepted");
    router.flush_all().await;

    let after_arm = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_arm.len(),
        2,
        "flushing A arms the floor: {after_arm:?}"
    );

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(2_000, "b")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write B accepted");

    clock.set_ns(T0);
    router.shutdown().await;

    assert_eq!(
        metrics.snapshot().clock_regressions_refused,
        1,
        "the drain's first flush attempt for B is refused exactly once"
    );
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        4,
        "the drain retried the refusal and published B; a single-snapshot \
         flush_all would have left only two objects: {objects:?}"
    );
}

/// F1 (spans): the span drain retries a refusal that happens during it. Same
/// shape as the metrics F1 test; reverting the span `flush_all` to a single
/// snapshot drops the re-buffered tenant and this assertion of four fails.
#[tokio::test]
async fn spans_flush_all_retries_a_refusal_during_the_drain() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let armed_ns = T0 + 2 * MAX_FLUSH_CLOCK_HOLD_NS;
    let clock = TestClock::new(armed_ns);
    let router = SpanIngestRouter::new(buffered_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    let metrics = router.metrics_handle();

    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000, StatusCode::Unset)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write A accepted");
    router.flush_all().await;

    let after_arm = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_arm.len(),
        2,
        "flushing A arms the floor: {after_arm:?}"
    );

    router
        .write(
            tenant.clone(),
            vec![norm_span(2_000, StatusCode::Unset)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write B accepted");

    clock.set_ns(T0);
    router.shutdown().await;

    assert_eq!(
        metrics.snapshot().clock_regressions_refused,
        1,
        "the drain's first flush attempt for B is refused exactly once"
    );
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        4,
        "the drain retried the refusal and published B; a single-snapshot \
         flush_all would have left only two objects: {objects:?}"
    );
}

/// F2 (metrics): the `InvalidReading` refuse arm counts every buffered exemplar
/// as dropped, so the drop stays visible when a grossly broken clock reading
/// abandons the flush. A sub-floor reading fails `checked_ingest_hour_bucket` at
/// the helper entry, so the flush is abandoned before the buffer is built and
/// its two buffered exemplars never reach `admit_exemplars`; the arm counts them.
/// Removing `self.metrics.record_exemplars(0, buf.exemplars.len())` from the
/// `InvalidReading` arm drops the count to zero and this assertion fails.
#[tokio::test]
async fn metrics_invalid_reading_arm_counts_dropped_exemplars() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    // A sub-floor reading: below MIN_PLAUSIBLE_INGEST_CLOCK_NS, so the flush's
    // clock read fails at the helper entry (InvalidReading), not the floor.
    let clock = TestClock::new(1_000);
    let router = IngestRouter::new(
        flush_per_write_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");

    // One point plus two exemplars for its series, in windows far enough apart
    // that both would be admitted if the flush ever built: it does not, so both
    // are counted dropped by the refuse arm.
    let point = IngestPoint::from(make_point(&tenant, "cpu_usage", &[], 1_000, 1.0));
    let exemplars = vec![
        metrics_exemplar(&tenant, "cpu_usage", 1_000, 1),
        metrics_exemplar(&tenant, "cpu_usage", 20_000_000_000, 2),
    ];
    let refused = router
        .write_values_with_exemplars(
            tenant.clone(),
            vec![point],
            exemplars,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await;
    let err = refused.expect_err("a sub-floor flush reading must fail the flush");
    assert!(
        matches!(err, WriteError::SegmentBuild(_)),
        "a sub-floor reading is fail-loud SegmentBuild; got: {err:?}"
    );

    let snap = router.metrics().snapshot();
    assert_eq!(
        snap.exemplars_dropped_total, 2,
        "both buffered exemplars are counted dropped by the InvalidReading arm"
    );
    assert_eq!(
        snap.exemplars_written_total, 0,
        "the abandoned flush wrote no exemplars"
    );
    assert_eq!(
        snap.abandoned_input_rejected, 1,
        "the sub-floor flush is abandoned as input-rejected"
    );

    router.shutdown().await;
}

/// A backwards step per reading large enough that the floor cannot absorb it,
/// so a [`RecedingClock`] armed with it refuses every drain pass instead of
/// letting the second one proceed. Derived from the production bound, never a
/// literal.
const RECEDING_STEP_NS: i64 = 2 * MAX_FLUSH_CLOCK_HOLD_NS;

/// F1 (metrics, teardown): a drain whose refusals outlive the pass cap. The
/// clock steps back beyond the hold bound on *every* reading, so each of the
/// `MAX_FLUSH_ALL_PASSES` passes is refused and the tenant is still buffered
/// when the drain gives up. On `Shutdown` the actor breaks straight after the
/// drain, so nothing retries that residue: it is an acknowledged buffered-mode
/// row lost on a graceful teardown, which is what the ERROR log and
/// `flush_all_residue_tenants` report. Flipping the `Shutdown` arm's
/// `DrainIntent::Teardown` to `DrainIntent::Retryable` leaves the counter at
/// zero and this assertion of one fails.
#[tokio::test]
async fn metrics_teardown_drain_reports_residue_after_the_pass_cap() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let armed_ns = T0 + RECEDING_STEP_NS;
    let clock = RecedingClock::new(armed_ns);
    let router = IngestRouter::new(
        buffered_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");
    let metrics = router.metrics_handle();

    // Write A and flush it while the clock is still frozen: this arms the floor
    // at `armed_ns`.
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write A accepted");
    router.flush_all().await;
    let after_arm = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_arm.len(),
        2,
        "flushing A arms the floor and publishes one data object and commit record: {after_arm:?}"
    );

    // Write B stays buffered: only a drain will flush it.
    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                2_000,
                2.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write B accepted");

    // Arm the recession: the drain's first reading is `T0` (a backwards step of
    // `RECEDING_STEP_NS` below the armed floor) and every later reading is
    // another `RECEDING_STEP_NS` lower, so re-anchoring the floor never lets a
    // pass through.
    clock.recede_from(T0, RECEDING_STEP_NS);
    router.shutdown().await;

    let snap = metrics.snapshot();
    assert_eq!(
        snap.clock_regressions_refused, MAX_FLUSH_ALL_PASSES as u64,
        "every drain pass is refused, once per pass, so the cap is what ends the drain"
    );
    assert_eq!(
        snap.clock_regressions, 0,
        "every step is beyond the hold bound, so none is absorbed"
    );
    assert_eq!(
        snap.flush_all_residue_tenants, 1,
        "the one tenant still buffered when a teardown drain gave up is reported as residue"
    );
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        2,
        "B never published: only A's data object and commit record exist: {objects:?}"
    );
}

/// F1 (metrics, `FlushNow`): the same residue on the drain the actor OUTLIVES
/// is not a lost row and must not be reported as one. The `FlushNow` arm does
/// not break: the tenant stays in the buffer map with its arrival bookkeeping,
/// so a later trigger flushes it. This asserts the counter whose own doc calls
/// a nonzero value a durability defect stays at zero across such a drain, and
/// then that the row does publish once the clock stops receding. Flipping the
/// `FlushNow` arm's `DrainIntent::Retryable` to `DrainIntent::Teardown` bumps
/// the counter to one and the first assertion of zero fails.
#[tokio::test]
async fn metrics_flush_now_drain_does_not_report_residue_and_retries_it() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let armed_ns = T0 + RECEDING_STEP_NS;
    let clock = RecedingClock::new(armed_ns);
    let router = IngestRouter::new(
        buffered_config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );
    let tenant = tenant("acme");
    let metrics = router.metrics_handle();

    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                1_000,
                1.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write A accepted");
    router.flush_all().await;
    let after_arm = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_arm.len(),
        2,
        "flushing A arms the floor: {after_arm:?}"
    );

    router
        .write(
            tenant.clone(),
            vec![make_point(
                &tenant,
                "cpu_usage",
                &[("host", "a")],
                2_000,
                2.0,
            )],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write B accepted");

    clock.recede_from(T0, RECEDING_STEP_NS);
    router.flush_all().await;

    let snap = metrics.snapshot();
    assert_eq!(
        snap.clock_regressions_refused, MAX_FLUSH_ALL_PASSES as u64,
        "the FlushNow drain is refused on every pass too, so it hits the same cap"
    );
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "the actor keeps running after FlushNow, so its residue is not a lost write"
    );
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        2,
        "B has not published yet; it is still buffered: {objects:?}"
    );

    // The clock stops receding: the next reading is forward of the re-anchored
    // floor, so the retained buffer flushes on the next trigger.
    clock.freeze_at(armed_ns);
    router.flush_all().await;
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        4,
        "the retained B published on the next flush: one more data object and \
         commit record: {objects:?}"
    );

    router.shutdown().await;
    assert_eq!(
        metrics.snapshot().flush_all_residue_tenants,
        0,
        "nothing was buffered by the time of the teardown drain, so no residue there either"
    );
}

/// F1 (logs, teardown): mirror of
/// `metrics_teardown_drain_reports_residue_after_the_pass_cap` in the log shard
/// actor. Flipping the log `Shutdown` arm's `DrainIntent::Teardown` to
/// `DrainIntent::Retryable` leaves the counter at zero and this fails.
#[tokio::test]
async fn logs_teardown_drain_reports_residue_after_the_pass_cap() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let armed_ns = T0 + RECEDING_STEP_NS;
    let clock = RecedingClock::new(armed_ns);
    let router = LogIngestRouter::new(buffered_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    let metrics = router.metrics_handle();

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "a")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write A accepted");
    router.flush_all().await;
    let after_arm = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_arm.len(),
        2,
        "flushing A arms the floor: {after_arm:?}"
    );

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(2_000, "b")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write B accepted");

    clock.recede_from(T0, RECEDING_STEP_NS);
    router.shutdown().await;

    let snap = metrics.snapshot();
    assert_eq!(
        snap.clock_regressions_refused, MAX_FLUSH_ALL_PASSES as u64,
        "every drain pass is refused, once per pass"
    );
    assert_eq!(
        snap.flush_all_residue_tenants, 1,
        "the tenant still buffered when the teardown drain gave up is reported as residue"
    );
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(objects.len(), 2, "B never published: {objects:?}");
}

/// F1 (logs, `FlushNow`): mirror of
/// `metrics_flush_now_drain_does_not_report_residue_and_retries_it` in the log
/// shard actor. Flipping the log `FlushNow` arm's `DrainIntent::Retryable` to
/// `DrainIntent::Teardown` bumps the counter to one and this fails.
#[tokio::test]
async fn logs_flush_now_drain_does_not_report_residue_and_retries_it() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let armed_ns = T0 + RECEDING_STEP_NS;
    let clock = RecedingClock::new(armed_ns);
    let router = LogIngestRouter::new(buffered_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    let metrics = router.metrics_handle();

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(1_000, "a")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write A accepted");
    router.flush_all().await;

    router
        .write(
            tenant.clone(),
            vec![norm_log_record(2_000, "b")],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write B accepted");

    clock.recede_from(T0, RECEDING_STEP_NS);
    router.flush_all().await;

    let snap = metrics.snapshot();
    assert_eq!(
        snap.clock_regressions_refused, MAX_FLUSH_ALL_PASSES as u64,
        "the FlushNow drain is refused on every pass too"
    );
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "the actor keeps running after FlushNow, so its residue is not a lost write"
    );

    clock.freeze_at(armed_ns);
    router.flush_all().await;
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        4,
        "the retained B published on the next flush: {objects:?}"
    );

    router.shutdown().await;
    assert_eq!(
        metrics.snapshot().flush_all_residue_tenants,
        0,
        "nothing was buffered by the time of the teardown drain"
    );
}

/// F1 (spans, teardown): mirror of
/// `metrics_teardown_drain_reports_residue_after_the_pass_cap` in the span
/// shard actor. Flipping the span `Shutdown` arm's `DrainIntent::Teardown` to
/// `DrainIntent::Retryable` leaves the counter at zero and this fails.
#[tokio::test]
async fn spans_teardown_drain_reports_residue_after_the_pass_cap() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let armed_ns = T0 + RECEDING_STEP_NS;
    let clock = RecedingClock::new(armed_ns);
    let router = SpanIngestRouter::new(buffered_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    let metrics = router.metrics_handle();

    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000, StatusCode::Unset)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write A accepted");
    router.flush_all().await;
    let after_arm = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        after_arm.len(),
        2,
        "flushing A arms the floor: {after_arm:?}"
    );

    router
        .write(
            tenant.clone(),
            vec![norm_span(2_000, StatusCode::Unset)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write B accepted");

    clock.recede_from(T0, RECEDING_STEP_NS);
    router.shutdown().await;

    let snap = metrics.snapshot();
    assert_eq!(
        snap.clock_regressions_refused, MAX_FLUSH_ALL_PASSES as u64,
        "every drain pass is refused, once per pass"
    );
    assert_eq!(
        snap.flush_all_residue_tenants, 1,
        "the tenant still buffered when the teardown drain gave up is reported as residue"
    );
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(objects.len(), 2, "B never published: {objects:?}");
}

/// F1 (spans, `FlushNow`): mirror of
/// `metrics_flush_now_drain_does_not_report_residue_and_retries_it` in the span
/// shard actor. Flipping the span `FlushNow` arm's `DrainIntent::Retryable` to
/// `DrainIntent::Teardown` bumps the counter to one and this fails.
#[tokio::test]
async fn spans_flush_now_drain_does_not_report_residue_and_retries_it() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let armed_ns = T0 + RECEDING_STEP_NS;
    let clock = RecedingClock::new(armed_ns);
    let router = SpanIngestRouter::new(buffered_config(), Arc::clone(&store), clock.clone());
    let tenant = tenant("acme");
    let metrics = router.metrics_handle();

    router
        .write(
            tenant.clone(),
            vec![norm_span(1_000, StatusCode::Unset)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write A accepted");
    router.flush_all().await;

    router
        .write(
            tenant.clone(),
            vec![norm_span(2_000, StatusCode::Unset)],
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write B accepted");

    clock.recede_from(T0, RECEDING_STEP_NS);
    router.flush_all().await;

    let snap = metrics.snapshot();
    assert_eq!(
        snap.clock_regressions_refused, MAX_FLUSH_ALL_PASSES as u64,
        "the FlushNow drain is refused on every pass too"
    );
    assert_eq!(
        snap.flush_all_residue_tenants, 0,
        "the actor keeps running after FlushNow, so its residue is not a lost write"
    );

    clock.freeze_at(armed_ns);
    router.flush_all().await;
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert_eq!(
        objects.len(),
        4,
        "the retained B published on the next flush: {objects:?}"
    );

    router.shutdown().await;
    assert_eq!(
        metrics.snapshot().flush_all_residue_tenants,
        0,
        "nothing was buffered by the time of the teardown drain"
    );
}
