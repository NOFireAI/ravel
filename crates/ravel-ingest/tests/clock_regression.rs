//! Per-writer monotonic flush clock (ADR-1307, issue #1307).
//!
//! `created_unix_ns` is the primary key of the query-time duplicate-resolution
//! order (docs/catalog-and-mvcc.md "Cross-segment duplicate samples"): among
//! duplicates of one (series, ts), the greatest `(created_unix_ns,
//! writer_epoch, writer_seq, in-page index)` wins. That key is stamped from the
//! flush clock, which carries no ordering guarantee on its own: a backwards
//! wall-clock step (an NTP correction, a manual set) between two flushes of the
//! same writer used to stamp the later flush below the earlier one, so a stale
//! value outranked its own correction. Each shard actor now raises every
//! flush-open reading to a per-writer monotonic floor and counts each step it
//! absorbs (`clock_regressions`). The floor is in-process state only: a restart
//! mints a fresh `writer_id`, and cross-process order rests on that identity
//! rule, not on the floor.
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
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantId};

const T0: i64 = 1_700_000_000_000_000_000;
const TEN_MINUTES_NS: i64 = 600_000_000_000;

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

/// Metrics: the floor is in-process state, never read back after a restart. A
/// fresh router mints a new `writer_id`, so its first flush stamps the raw
/// clock even when that reading is below a stamp the previous process
/// committed, and its own regression counter starts at zero. Cross-process
/// order rests on the distinct `writer_id`, not on any inherited floor.
#[tokio::test]
async fn restart_resets_the_floor_with_a_fresh_writer() {
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

    assert_ne!(
        first_token.writer_id, second_token.writer_id,
        "a restart mints a fresh writer identity"
    );
    assert_eq!(
        second_created,
        T0 - TEN_MINUTES_NS,
        "the fresh writer stamps the raw clock; no floor survives the restart"
    );
    assert!(second_created < first_created);
    assert_eq!(
        router2.metrics().snapshot().clock_regressions,
        0,
        "the fresh writer's counter starts at zero: it absorbed no step"
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

    router.shutdown().await;
}
