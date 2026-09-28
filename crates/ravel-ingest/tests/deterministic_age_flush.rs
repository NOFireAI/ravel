//! Regression test: the shard actor's
//! flush tick must run on the injected `Clock`, so advancing that clock past
//! `max_flush_delay` drives an age-based flush deterministically, with no
//! wall-clock sleep and no flakiness.
//!
//! With the tick on the one injected clock, the two-clock interleaving
//! (the injected `Clock` for the age check, the tokio timer for the tick)
//! cannot occur: buffer the point first, then advance the clock, and the age
//! flush lands every time. The ordering is established with a cooperative poll on
//! the buffered-points counter rather than a real `tokio::time::sleep`, so
//! the test cannot race.
//!
//! The second half pins the flush cadence of the three age tiers (ADR-1737)
//! over one injected-clock hour on each of the metrics, log, and span
//! pipelines: the idle clock with `idle_flush_byte_floor` off, the sub-floor
//! hold below a non-zero floor, and the fast clock.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, span_on_shard, tenant};
use ravel_commit::record;
use ravel_ingest::{IngestConfig, IngestRouter, LogIngestRouter, SpanIngestRouter, WriteMode};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Buffers a point below the size threshold, then advances the injected clock
/// past `max_flush_delay`. The flush tick, now on that same clock, wakes and
/// fires the age flush; the strict write's ack proves it committed.
#[tokio::test]
async fn advancing_the_injected_clock_drives_an_age_flush() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let config = IngestConfig {
        shard_count: 1,
        // Only the age trigger can fire: the buffer never reaches target_bytes.
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    // A strict write blocks until its flush acks, so drive the age flush from
    // the joined arm. Wait (cooperatively, no real sleep) until the point is
    // buffered in the actor so `note_arrival` has stamped `oldest_arrival_ns`
    // with the pre-advance time, then push the clock past `max_flush_delay`.
    // Because the tick shares this clock, the advance deterministically wakes
    // it and the age check sees the buffer as due.
    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        async {
            while router.metrics().snapshot().buffered_points_total < 1 {
                tokio::task::yield_now().await;
            }
            clock.advance_ns(100_000_000);
        },
    );

    let receipt =
        write_result.expect("age flush fires once the injected clock passes max_flush_delay");
    assert_eq!(receipt.tokens.len(), 1);

    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, 1,
        "the advance must drive exactly one age-triggered flush"
    );
    assert_eq!(
        snapshot.flushes_by_size, 0,
        "the buffer never reached target_bytes, so no size flush may fire"
    );

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        objects.iter().any(|o| o.key.contains("/l0/")),
        "the age flush must have stored a data object"
    );
    assert!(
        objects.iter().any(|o| o.key.contains("/c/")),
        "the age flush must have stored a commit record"
    );

    router.shutdown().await;
}

/// A buffer whose age has not reached `max_flush_delay` must not be flushed by
/// a tick, no matter how many ticks fire. This pins the other side of the age
/// threshold so the flush above is attributable to the elapsed age, not to the
/// tick firing at all. The write is buffered (not strict) so nothing blocks on
/// an ack that will never come.
#[tokio::test]
async fn a_tick_below_max_flush_delay_does_not_flush() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];
    router
        .write(
            tenant.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write is acknowledged at enqueue");

    while router.metrics().snapshot().buffered_points_total < 1 {
        tokio::task::yield_now().await;
    }

    // Advance by less than `max_flush_delay` and fire several ticks. The age
    // check must find the buffer not yet due, so no flush happens.
    for _ in 0..5 {
        clock.advance_ns(5_000_000); // 5 ms, total 25 ms < 50 ms
        tokio::task::yield_now().await;
    }

    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, 0,
        "a buffer younger than max_flush_delay must not age-flush"
    );

    router.shutdown().await;
}

/// ADR-0051 section 7: a buffered-mode buffer with no strict-mode
/// waiter and fewer than `min_flush_bytes` is idle, so the fast
/// `max_flush_delay` age trigger must not fire for it -- only the slower
/// `max_flush_delay_idle` does. This pins the idle side of the predicate;
/// `advancing_the_injected_clock_drives_an_age_flush` above already pins the
/// non-idle (strict-waiter) side.
#[tokio::test]
async fn idle_buffer_defers_age_trigger() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let config = IngestConfig {
        shard_count: 1,
        // Only the age trigger can fire: the buffer never reaches target_bytes.
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        max_flush_delay_idle: Duration::from_millis(300),
        // Comfortably above one point's estimated buffered size, so the
        // buffer stays "idle" for the whole test.
        min_flush_bytes: 1_000_000,
        flush_tick: Duration::from_millis(10),
        ..IngestConfig::default()
    };
    let router = IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone());

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    // Buffered mode: no strict waiter, so `waiters` stays empty and the
    // buffer's idleness is decided purely by `min_flush_bytes`.
    router
        .write(
            tenant.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write is acknowledged at enqueue");

    while router.metrics().snapshot().buffered_points_total < 1 {
        tokio::task::yield_now().await;
    }

    // Past max_flush_delay (50ms) but below max_flush_delay_idle (300ms): an
    // idle buffer must not flush yet.
    for _ in 0..10 {
        clock.advance_ns(10_000_000); // 10ms, total 100ms
        tokio::task::yield_now().await;
    }
    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, 0,
        "an idle buffer must defer past max_flush_delay to max_flush_delay_idle"
    );

    // Past max_flush_delay_idle (300ms total): the deferred flush must fire.
    for _ in 0..25 {
        clock.advance_ns(10_000_000); // 10ms, total 350ms
        tokio::task::yield_now().await;
    }
    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, 1,
        "the deferred age flush must fire once max_flush_delay_idle elapses"
    );

    router.shutdown().await;
}

/// One injected-clock hour at the default 200 ms `flush_tick`, in ticks.
const TICKS_PER_HOUR: u64 = 18_000;

/// Below the 256 KiB default `min_flush_bytes`, and above the object bytes
/// any pipeline's near-idle case buffers in an hour: 360 units, each under 128
/// estimated object bytes (a scalar sample is 16, the log record and span
/// below are under 100).
const FLOOR_ABOVE_NEAR_IDLE: usize = 128 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pipeline {
    Metrics,
    Logs,
    Spans,
}

const PIPELINES: [Pipeline; 3] = [Pipeline::Metrics, Pipeline::Logs, Pipeline::Spans];

/// The flush counters every pipeline's snapshot carries that an age case can
/// move, plus the size counter so a case proves the size trigger stayed out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FlushCounts {
    by_size: u64,
    by_age: u64,
    by_age_floor: u64,
}

/// What one pipeline did over one injected-clock hour.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HourResult {
    pipeline: Pipeline,
    flushes: FlushCounts,
    data_objects: usize,
    commit_records: usize,
    /// Rows across every data object written in the hour, read from the
    /// commit records (`sample_count` is points, log records, or spans
    /// depending on the pipeline). Object counts alone cannot tell a flush
    /// that carried the hour's rows from one that carried a prefix and
    /// dropped the rest.
    rows: u64,
}

enum AnyRouter {
    Metrics(IngestRouter),
    Logs(LogIngestRouter),
    Spans(SpanIngestRouter),
}

fn unit_ts(seq: u64) -> i64 {
    1_000 + i64::try_from(seq).expect("seq fits i64")
}

/// A log record on one fixed stream.
fn log_record(seq: u64) -> NormalizedLogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&resource, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "", &scope_attrs),
        ts_ns: unit_ts(seq),
        observed_ts_ns: unit_ts(seq),
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "x".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// A span on one fixed trace, with a span id unique to `seq`.
fn span(seq: u64) -> NormalizedSpan {
    let mut span = span_on_shard(0, 1, unit_ts(seq));
    span.span_id = seq.to_le_bytes();
    span
}

impl AnyRouter {
    fn new(
        pipeline: Pipeline,
        config: IngestConfig,
        store: &Arc<dyn ObjectStoreBackend>,
        clock: &Arc<TestClock>,
    ) -> Self {
        let store = Arc::clone(store);
        match pipeline {
            Pipeline::Metrics => AnyRouter::Metrics(IngestRouter::new(
                config,
                store,
                Signal::Metrics,
                clock.clone(),
            )),
            Pipeline::Logs => AnyRouter::Logs(LogIngestRouter::new(config, store, clock.clone())),
            Pipeline::Spans => {
                AnyRouter::Spans(SpanIngestRouter::new(config, store, clock.clone()))
            }
        }
    }

    /// Writes `count` units numbered from `first_seq` to one series, stream,
    /// or trace, and returns the number of commit tokens the receipt carries.
    async fn write(&self, tenant: &TenantId, first_seq: u64, count: u64, mode: WriteMode) -> usize {
        let deadline = Duration::from_secs(5);
        let seqs = first_seq..first_seq + count;
        match self {
            AnyRouter::Metrics(router) => {
                let points = seqs
                    .map(|s| make_point(tenant, "cpu_usage", &[("host", "a")], unit_ts(s), 1.0))
                    .collect();
                router
                    .write(tenant.clone(), points, mode, deadline)
                    .await
                    .expect("metrics write")
                    .tokens
                    .len()
            }
            AnyRouter::Logs(router) => router
                .write(
                    tenant.clone(),
                    seqs.map(log_record).collect(),
                    mode,
                    deadline,
                )
                .await
                .expect("log write")
                .tokens
                .len(),
            AnyRouter::Spans(router) => router
                .write(tenant.clone(), seqs.map(span).collect(), mode, deadline)
                .await
                .expect("span write")
                .tokens
                .len(),
        }
    }

    /// Units the shard actors have buffered since the router started.
    fn buffered_units(&self) -> u64 {
        match self {
            AnyRouter::Metrics(router) => router.metrics().snapshot().buffered_points_total,
            AnyRouter::Logs(router) => router.metrics().snapshot().buffered_records_total,
            AnyRouter::Spans(router) => router.metrics().snapshot().buffered_spans_total,
        }
    }

    fn flushes(&self) -> FlushCounts {
        match self {
            AnyRouter::Metrics(router) => {
                let s = router.metrics().snapshot();
                assert_eq!(
                    s.flushes_by_age_adaptive, 0,
                    "adaptive delay is off in every case here"
                );
                FlushCounts {
                    by_size: s.flushes_by_size,
                    by_age: s.flushes_by_age,
                    by_age_floor: s.flushes_by_age_floor,
                }
            }
            AnyRouter::Logs(router) => {
                let s = router.metrics().snapshot();
                FlushCounts {
                    by_size: s.flushes_by_size,
                    by_age: s.flushes_by_age,
                    by_age_floor: s.flushes_by_age_floor,
                }
            }
            AnyRouter::Spans(router) => {
                let s = router.metrics().snapshot();
                FlushCounts {
                    by_size: s.flushes_by_size,
                    by_age: s.flushes_by_age,
                    by_age_floor: s.flushes_by_age_floor,
                }
            }
        }
    }

    async fn shutdown(self) {
        match self {
            AnyRouter::Metrics(router) => router.shutdown().await,
            AnyRouter::Logs(router) => router.shutdown().await,
            AnyRouter::Spans(router) => router.shutdown().await,
        }
    }
}

async fn yield_n(n: usize) {
    for _ in 0..n {
        tokio::task::yield_now().await;
    }
}

/// Drives one tenant through one injected-clock hour, one `flush_tick` per
/// step: every `write_every_ticks` ticks it buffers `units_per_write` units,
/// waits for the actor to hold them, then advances the clock by one tick and
/// yields so the actor's age check and any flush it opens run before the next
/// write. The objects are counted before shutdown, whose drain would add more.
async fn one_hour_at_rate(
    pipeline: Pipeline,
    config: IngestConfig,
    units_per_write: u64,
    write_every_ticks: u64,
) -> HourResult {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let tick_ns = i64::try_from(config.flush_tick.as_nanos()).expect("tick fits i64");
    let router = AnyRouter::new(pipeline, config, &store, &clock);
    let tenant = tenant("acme");
    let mut written = 0u64;
    for tick in 0..TICKS_PER_HOUR {
        if tick % write_every_ticks == 0 {
            router
                .write(&tenant, written, units_per_write, WriteMode::Buffered)
                .await;
            written += units_per_write;
            while router.buffered_units() < written {
                tokio::task::yield_now().await;
            }
        }
        clock.advance_ns(tick_ns);
        yield_n(16).await;
    }
    yield_n(64).await;
    let flushes = router.flushes();
    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let data_objects = objects.iter().filter(|o| o.key.contains("/l0/")).count();
    let commit_records = objects.iter().filter(|o| o.key.contains("/c/")).count();
    let mut rows = 0u64;
    for object in objects.iter().filter(|o| o.key.contains("/c/")) {
        let bytes = store
            .get(&object.key, GetRange::Full)
            .await
            .expect("get commit record")
            .data;
        rows += record::decode(&bytes)
            .expect("decode commit record")
            .sample_count;
    }
    router.shutdown().await;
    HourResult {
        pipeline,
        flushes,
        data_objects,
        commit_records,
        rows,
    }
}

async fn one_hour_on_every_pipeline(
    config: IngestConfig,
    units_per_write: u64,
    write_every_ticks: u64,
) -> Vec<HourResult> {
    let mut results = Vec::new();
    for pipeline in PIPELINES {
        results.push(one_hour_at_rate(pipeline, config, units_per_write, write_every_ticks).await);
    }
    results
}

/// The result every pipeline must produce: `flushes` flushes opened, each
/// writing one data object and one commit record, carrying `rows` rows
/// between them.
fn on_every_pipeline(flushes: FlushCounts, objects: usize, rows: u64) -> Vec<HourResult> {
    PIPELINES
        .iter()
        .map(|&pipeline| HourResult {
            pipeline,
            flushes,
            data_objects: objects,
            commit_records: objects,
            rows,
        })
        .collect()
}

/// Units `one_hour_at_rate` writes over the hour at this rate: the total every
/// case's objects must carry between them, since the hour's last tick is also
/// a flush for every case here.
fn units_written(units_per_write: u64, write_every_ticks: u64) -> u64 {
    units_per_write * TICKS_PER_HOUR.div_ceil(write_every_ticks)
}

/// Near idle, floor off (the default): one unit every 10 s never reaches
/// `min_flush_bytes`, so every pipeline flushes on the 40 s idle clock, 90
/// times in the hour, exactly as before ADR-1737.
#[tokio::test]
async fn near_idle_with_the_floor_off_flushes_on_the_idle_clock() {
    let config = IngestConfig::default();
    assert_eq!(config.idle_flush_byte_floor, 0);
    let results = one_hour_on_every_pipeline(config, 1, 50).await;
    let expected = FlushCounts {
        by_size: 0,
        by_age: 90,
        by_age_floor: 0,
    };
    assert_eq!(
        results,
        on_every_pipeline(expected, 90, units_written(1, 50))
    );
}

/// Near idle, floor on: the same trickle stays below the floor all hour, so
/// every pipeline holds its buffer for the sub-floor hold from the first unit,
/// which the hour's second-to-last tick reaches (the hold is one `flush_tick`
/// short of `max_flush_lifetime`, and the write that would start a second
/// buffer never comes: writes land every 50th tick and the last is at tick
/// 17,950). One flush, counted on `flushes_by_age_floor` and not on
/// `flushes_by_age`, and it carries every row the hour wrote: one object
/// is the whole hour's data, so an object count alone would pass on a flush
/// that dropped most of it.
#[tokio::test]
async fn near_idle_below_the_floor_holds_for_the_flush_lifetime() {
    let config = IngestConfig {
        idle_flush_byte_floor: FLOOR_ABOVE_NEAR_IDLE,
        ..IngestConfig::default()
    };
    assert_eq!(config.validate(), Ok(()));
    assert_eq!(config.max_flush_lifetime, Duration::from_secs(3600));
    assert_eq!(config.flush_tick, Duration::from_millis(200));
    let results = one_hour_on_every_pipeline(config, 1, 50).await;
    let expected = FlushCounts {
        by_size: 0,
        by_age: 0,
        by_age_floor: 1,
    };
    assert_eq!(
        results,
        on_every_pipeline(expected, 1, units_written(1, 50))
    );
}

/// Middle band, floor on: 25 units a second, written once a second, cross a
/// 4 KiB floor inside every 40 s window without reaching `min_flush_bytes`,
/// so every pipeline keeps the idle clock: 90 flushes in the hour.
#[tokio::test]
async fn middle_band_above_the_floor_keeps_the_idle_clock() {
    let config = IngestConfig {
        idle_flush_byte_floor: 4 * 1024,
        ..IngestConfig::default()
    };
    assert_eq!(config.validate(), Ok(()));
    let results = one_hour_on_every_pipeline(config, 25, 5).await;
    let expected = FlushCounts {
        by_size: 0,
        by_age: 90,
        by_age_floor: 0,
    };
    assert_eq!(
        results,
        on_every_pipeline(expected, 90, units_written(25, 5))
    );
}

/// Fast band, floor on: 160 units a second, written once a second, reach a
/// 4 KiB `min_flush_bytes` inside every 2 s `max_flush_delay` window, so every
/// pipeline flushes on the fast clock: 1800 flushes in the hour.
#[tokio::test]
async fn fast_band_keeps_the_fast_clock() {
    let config = IngestConfig {
        min_flush_bytes: 4 * 1024,
        idle_flush_byte_floor: 1024,
        ..IngestConfig::default()
    };
    assert_eq!(config.validate(), Ok(()));
    let results = one_hour_on_every_pipeline(config, 160, 5).await;
    let expected = FlushCounts {
        by_size: 0,
        by_age: 1800,
        by_age_floor: 0,
    };
    assert_eq!(
        results,
        on_every_pipeline(expected, 1800, units_written(160, 5))
    );
}

/// A strict-mode waiter on a one-unit buffer, far below the floor, still
/// flushes on the 2 s fast clock on every pipeline: the floor holds only
/// buffers nobody is blocked on (ADR-1737 decision 4).
#[tokio::test]
async fn strict_waiter_below_the_floor_flushes_on_the_fast_clock() {
    let config = IngestConfig {
        idle_flush_byte_floor: FLOOR_ABOVE_NEAR_IDLE,
        ..IngestConfig::default()
    };
    let tick_ns = i64::try_from(config.flush_tick.as_nanos()).expect("tick fits i64");
    let fast_ns = i64::try_from(config.max_flush_delay.as_nanos()).expect("delay fits i64");
    for pipeline in PIPELINES {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(BASE_NS);
        let router = AnyRouter::new(pipeline, config, &store, &clock);
        let tenant = tenant("acme");

        let (tokens, ()) = tokio::join!(router.write(&tenant, 0, 1, WriteMode::Strict), async {
            while router.buffered_units() < 1 {
                tokio::task::yield_now().await;
            }
            // Exactly `max_flush_delay` in ticks, then stop: the ack must
            // arrive without the clock reaching the idle clock or the hold.
            for _ in 0..fast_ns / tick_ns {
                clock.advance_ns(tick_ns);
                yield_n(16).await;
            }
        });

        assert_eq!(tokens, 1, "{pipeline:?}");
        assert_eq!(clock.now(), BASE_NS + fast_ns, "{pipeline:?}");
        assert_eq!(
            router.flushes(),
            FlushCounts {
                by_size: 0,
                by_age: 1,
                by_age_floor: 0,
            },
            "{pipeline:?}"
        );
        router.shutdown().await;
    }
}

/// Writes one metrics point every `write_every_ticks` ticks and returns the
/// injected-clock time, from the first point, at which the first age flush
/// opened, with the counters at that moment.
async fn first_age_flush(config: IngestConfig, write_every_ticks: u64) -> (i64, FlushCounts) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let tick_ns = i64::try_from(config.flush_tick.as_nanos()).expect("tick fits i64");
    let router = AnyRouter::new(Pipeline::Metrics, config, &store, &clock);
    let tenant = tenant("acme");
    let mut written = 0u64;
    for tick in 0..TICKS_PER_HOUR {
        if tick % write_every_ticks == 0 {
            router.write(&tenant, written, 1, WriteMode::Buffered).await;
            written += 1;
            while router.buffered_units() < written {
                tokio::task::yield_now().await;
            }
        }
        clock.advance_ns(tick_ns);
        yield_n(16).await;
        let flushes = router.flushes();
        if flushes.by_age + flushes.by_age_floor > 0 {
            router.shutdown().await;
            return (clock.now() - BASE_NS, flushes);
        }
    }
    panic!("no age flush in the hour");
}

/// A trickle that starts below the floor moves up to the idle tier as its
/// bytes reach the floor, and the idle clock runs from its oldest point.
///
/// One point every 2 s on one series: the first point is 70 estimated object
/// bytes (32 series overhead, 22 label bytes, one 16-byte sample) and each
/// later one adds 16, so the buffer holds `54 + 16 * n` bytes after `n` points.
/// With a 300-byte floor the 16th point, at 30 s, crosses it, and the flush
/// opens at 40 s. With a 375-byte floor the buffer holds 374 bytes when it
/// turns 40 s old, so it is still held; the 21st point, written at 40 s,
/// crosses the floor and the next tick, at 40.2 s, flushes it.
#[tokio::test]
async fn a_trickle_that_reaches_the_floor_flushes_on_the_idle_clock() {
    let on_idle_clock = FlushCounts {
        by_size: 0,
        by_age: 1,
        by_age_floor: 0,
    };
    let crossed_at_30s = IngestConfig {
        idle_flush_byte_floor: 300,
        ..IngestConfig::default()
    };
    assert_eq!(
        first_age_flush(crossed_at_30s, 10).await,
        (40_000_000_000, on_idle_clock)
    );
    let crossed_at_40s = IngestConfig {
        idle_flush_byte_floor: 375,
        ..IngestConfig::default()
    };
    assert_eq!(
        first_age_flush(crossed_at_40s, 10).await,
        (40_200_000_000, on_idle_clock)
    );
}
