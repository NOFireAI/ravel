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

use common::{TestClock, make_point, tenant};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_ingest::{IngestConfig, IngestRouter, LogIngestRouter, SpanIngestRouter, WriteMode};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantId};

const T0: i64 = 1_700_000_000_000_000_000;
const TEN_MINUTES_NS: i64 = 600_000_000_000;
const ONE_MINUTE_NS: i64 = 60_000_000_000;
/// Mirrors `config::MAX_FLUSH_CLOCK_HOLD_NS` (20 minutes), the largest
/// backwards hold the floor absorbs before refusing. A test-local copy so a
/// change to the production bound surfaces here as a failure rather than a
/// silently tracking constant.
const MAX_FLUSH_CLOCK_HOLD_NS: i64 = 20 * 60 * 1_000_000_000;

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
/// `now_ns()` read stamps the correction 10 minutes below the original, which
/// inverts both the created-stamp equality and the counted regression.
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

    // NTP steps the host back ten minutes, then the correction for the same
    // (series, ts) is written and flushed.
    clock.set_ns(T0 - TEN_MINUTES_NS);
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
        "the backwards step is absorbed: the correction is stamped at the floor, not 10 minutes below"
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

    clock.set_ns(T0 - TEN_MINUTES_NS);
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
        "the correction is stamped at the floor, not 10 minutes below"
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

    clock.set_ns(T0 - TEN_MINUTES_NS);
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
        "the correction is stamped at the floor, not 10 minutes below"
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

    // A forward glitch of 40 minutes ratchets the floor to T0 + 40m.
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

    // The clock corrects back to T0 + 1m: a 39-minute backwards step from the
    // ratcheted floor, beyond the 20-minute bound, so it is refused.
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
        err.to_string().contains("beyond the monotonic hold bound"),
        "expected the hold-bound error, got: {err}"
    );

    // The floor re-anchored to T0 + 1m on refusal, so a normal reading above it
    // (T0 + 2m, still far below the glitched floor) proceeds. Absent the
    // re-anchor this would itself be a 38-minute backwards step and refuse.
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

/// Metrics: the abandonment deadline derives from the raw reading, not the
/// floor-raised stamp. With `max_flush_lifetime` zero, every flush's deadline
/// equals its raw open reading, so `bound_to_deadline` abandons at once. Under
/// a backwards step the floor raises the stamp to `T0` while the raw reading is
/// `T0 - 10m`; if the deadline used the raised stamp its budget would be a full
/// 10 minutes and the flush would publish. It must not: the deadline uses raw,
/// so the flush abandons and nothing is published. Flipping the deadline source
/// from `raw_ns` to `flush_open_ns` makes the regressed flush publish, so the
/// object listing is no longer empty.
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

    clock.set_ns(T0 - TEN_MINUTES_NS);
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
         from the floor-raised stamp would give it 10 minutes of budget and publish"
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
