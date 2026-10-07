use arrow::array::DictionaryArray;
use arrow::datatypes::Int32Type;

use super::*;
use crate::load::test_support::*;

/// A deterministic clock the test advances by hand, whose `sleep` the shard
/// actor's flush tick waits on (mirrors `ravel-ingest`'s own unit-test
/// `TestClock`, restated here because that clock is private to that crate's
/// test module). Advancing it past `max_flush_delay` is what drives an age
/// flush with no real-time sleep, so the age trigger can be exercised
/// through the loader deterministically.
struct TestClock {
    now_ns: std::sync::atomic::AtomicI64,
    /// Clock reads and sleep registrations since construction. Every shard
    /// actor loop iteration, every write the actor handles, and every flush
    /// task reads this clock, so the counter standing still means no task
    /// in the router is runnable: the progress signal
    /// [`yield_until_router_is_quiet`] samples.
    reads: std::sync::atomic::AtomicU64,
    wake_tx: tokio::sync::watch::Sender<()>,
}

impl TestClock {
    fn new(start_ns: i64) -> std::sync::Arc<Self> {
        let (wake_tx, _rx) = tokio::sync::watch::channel(());
        std::sync::Arc::new(TestClock {
            now_ns: std::sync::atomic::AtomicI64::new(start_ns),
            reads: std::sync::atomic::AtomicU64::new(0),
            wake_tx,
        })
    }

    fn advance_ns(&self, delta_ns: i64) {
        self.now_ns
            .fetch_add(delta_ns, std::sync::atomic::Ordering::SeqCst);
        let _ = self.wake_tx.send(());
    }

    fn reads(&self) -> u64 {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Clock for TestClock {
    fn now_ns(&self) -> i64 {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.now_ns.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn sleep(
        &self,
        dur: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let deadline = self
            .now_ns()
            .saturating_add(i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX));
        let mut rx = self.wake_tx.subscribe();
        Box::pin(async move {
            loop {
                if self.now_ns() >= deadline {
                    return;
                }
                if rx.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}

/// Consecutive scheduler rounds [`yield_until_router_is_quiet`] needs to see
/// the clock unread before it calls the router quiet. A routed write reaches
/// its shard buffer within a handful of rounds, so this is generous rather
/// than tuned; it costs nothing, because a round is one `yield_now` and no
/// wall-clock wait.
const QUIET_ROUNDS: usize = 64;

/// Nanoseconds [`load_with_released_tail`] advances the injected clock by to
/// release a load's tail. Above the router's 2s default `max_flush_delay`,
/// so a buffer no size trigger can reach ages out; far below the 60s Strict
/// ack deadline ([`WRITE_ACK_DEADLINE_FLOOR`]), so no in-flight write's
/// deadline can fire on the same advance.
const TAIL_RELEASE_ADVANCE_NS: i64 = 5 * 1_000_000_000;

/// Yields to the runtime until the router has stopped making progress, so
/// the caller can advance the clock knowing every dispatched write is
/// already in its shard buffer.
///
/// Quiescence is read off `clock.reads()`: with the clock frozen no timer
/// can fire, so once nothing reads it for [`QUIET_ROUNDS`] consecutive
/// rounds, no task in the router is runnable. Nothing here waits on wall
/// time.
async fn yield_until_router_is_quiet(clock: &TestClock) {
    let mut last = clock.reads();
    let mut still = 0usize;
    while still < QUIET_ROUNDS {
        tokio::task::yield_now().await;
        let reads = clock.reads();
        if reads == last {
            still += 1;
        } else {
            last = reads;
            still = 0;
        }
    }
}

/// Run one load whose object count is a function of `target_bytes` and the
/// input geometry alone, with no wall-clock input at all.
///
/// The router gets a frozen [`TestClock`], so the age trigger cannot fire
/// on its own; the drain-time re-flush runs on wall time and only matters
/// once the drain begins. Two things then have to be
/// arranged by hand, and both are what makes the count exact:
///
/// - The end-of-input `flush_all` must not run while a write is still on its
///   way to a shard, or that write lands in a fresh buffer afterwards and
///   gets an object of its own. The last batch's `on_batch_queued` hook
///   blocks the decoder thread, so no `Done` can reach the loader until this
///   driver releases it.
/// - The tail still has to be published. Once the router is quiet -- every
///   batch routed and every size-triggered flush finished -- the clock is
///   advanced past `max_flush_delay` exactly once, which ages out whatever
///   no size trigger could reach, one flush per shard.
///
/// `pipeline_depth` is one wider than the batch count so the loader never
/// parks mid-input on an ack: an ack the size trigger cannot answer would
/// otherwise need the age trigger to fire before the input is exhausted,
/// which is the layout decision the driver is here to make.
#[allow(clippy::too_many_arguments)]
async fn load_with_released_tail(
    store: Arc<dyn ObjectStoreBackend>,
    pq: &Path,
    m: &Mapping,
    shards: u32,
    batch_rows: usize,
    read_cursors: usize,
    batches: usize,
    target_bytes: usize,
) -> LoadReport {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let clock = TestClock::new(NOW_NS);
    let queued = Arc::new(AtomicUsize::new(0));
    let (gate_tx, mut gate_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let on_batch_queued: BuildStartHook = Arc::new(move || {
        if queued.fetch_add(1, Ordering::SeqCst) + 1 == batches {
            let _ = gate_tx.send(());
            let guard = release_rx
                .lock()
                .expect("the release channel is not poisoned");
            let _ = guard.recv();
        }
    });

    let load_fut = load_instrumented(
        store,
        pq,
        "acme",
        m,
        shards,
        batch_rows,
        0,
        Some(read_cursors),
        batches + 1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        target_bytes,
        None,
        NOW_NS,
        Arc::clone(&clock) as Arc<dyn Clock>,
        LoadPath::Columnar,
        None,
        Some(on_batch_queued),
    );

    // The driver never resolves, so the load future is what ends the
    // `select!`; both are polled on every round, which is what lets the
    // driver's yields hand the runtime to the writes still in flight.
    let driver = async {
        let () = gate_rx.recv().await.expect("the last batch is queued");
        yield_until_router_is_quiet(&clock).await;
        clock.advance_ns(TAIL_RELEASE_ADVANCE_NS);
        yield_until_router_is_quiet(&clock).await;
        // Releasing the decoder lets `Done` through, and with it the
        // end-of-input flush and the drain; both find empty buffers.
        drop(release_tx);
        std::future::pending::<()>().await
    };

    tokio::select! {
        report = load_fut => report.expect("load succeeds"),
        () = driver => unreachable!("the driver parks once the tail is released"),
    }
}

/// Reachability (issue #983): a real `load` run carries the per-shard flush
/// trigger mix in the same report the objects-written figure lives in, and
/// the mix sums to the object count. One row routes to one shard and flushes
/// by size at the default target, so this asserts an exact (size 1, age 0,
/// final 0) on a single shard rather than `> 0`.
#[tokio::test]
async fn load_report_carries_the_flush_trigger_mix() {
    let report = run_wide_load(4).await;
    assert!(
        !report.flush_trigger_mix.is_empty(),
        "a real load records at least one shard's flushes"
    );
    let mix = report.flush_mix_report();
    assert_eq!(mix.shards.len(), 1, "one row routes to exactly one shard");
    assert_eq!(
        mix.shards[0].counts,
        FlushMixCounts {
            size: 1,
            age: 0,
            final_drain: 0,
        },
        "the single row flushes by size, nothing ages or drains"
    );
    assert_eq!(
        mix.totals,
        FlushMixCounts {
            size: 1,
            age: 0,
            final_drain: 0,
        }
    );
    assert_eq!(
        mix.totals.total(),
        report.objects_written() as u64,
        "the mix sums to the objects the same report reports"
    );
}

/// A real `load --parquet` run wires and records every stage of the logs
/// pipeline, not a subset: admit, route, merge, encode, and bloom all
/// recorded at least one sample. Drop `#[cfg(feature = "stage-timing")]`
/// from `LogIngestRouter::stage_timings` (or from any one stage boundary,
/// including the `LogStage::Bloom` recording added for issue #1516) and
/// this fails, either at compile time or on an empty/partial stage set.
#[cfg(feature = "stage-timing")]
#[tokio::test]
async fn load_populates_the_stage_timing_breakdown() {
    let report = run_wide_load(4).await;
    let stages: Vec<_> = report.stage_timings.stages().collect();
    assert_eq!(
        stages,
        vec![
            ravel_ingest::LogStage::Admit,
            ravel_ingest::LogStage::Route,
            ravel_ingest::LogStage::Merge,
            ravel_ingest::LogStage::Encode,
            ravel_ingest::LogStage::Bloom,
        ],
        "a real load must wire and record every stage, not a subset"
    );
    for stage in stages {
        let totals = report
            .stage_timings
            .get(stage)
            .expect("a stage in `stages()` has totals");
        assert!(totals.samples > 0, "{stage:?} recorded zero samples");
    }
}

/// `--batch-rows 0` is rejected with a typed [`LoadError::Setup`] before any
/// work, rather than silently clamped to 1.
#[tokio::test]
async fn batch_rows_zero_is_rejected() {
    use ravel_object_store::memory::MemoryStore;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let m = base_mapping();
    let err = load(
        store,
        Path::new("/nonexistent.parquet"),
        "acme",
        &m,
        4,
        0,
        None,
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect_err("batch_rows of 0 is rejected");
    assert!(
        matches!(err, LoadError::Setup(_)),
        "a typed setup error, got: {err}"
    );
    assert!(
        err.to_string().contains("--batch-rows must be at least 1"),
        "the error names the lever: {err}"
    );
}

/// `--read-cursors 0` is rejected with a typed [`LoadError::Setup`] before
/// any work, rather than silently clamped to 1 (issue #560), mirroring
/// `batch_rows_zero_is_rejected` above.
#[tokio::test]
async fn read_cursors_zero_is_rejected() {
    use ravel_object_store::memory::MemoryStore;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let m = base_mapping();
    let err = load(
        store,
        Path::new("/nonexistent.parquet"),
        "acme",
        &m,
        4,
        10,
        Some(0),
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect_err("read_cursors of 0 is rejected");
    assert!(
        matches!(err, LoadError::Setup(_)),
        "a typed setup error, got: {err}"
    );
    assert!(
        err.to_string()
            .contains("--read-cursors must be at least 1"),
        "the error names the lever: {err}"
    );
}

/// `--pipeline-depth 0` is rejected with a typed [`LoadError::Setup`]
/// before any work, rather than silently clamped to 1, mirroring
/// `batch_rows_zero_is_rejected` above. A depth of 0 would also make the
/// main loop's `while inflight.len() >= pipeline_depth` true before any
/// write is ever spawned; the guard makes that unreachable rather than
/// relying on the `let`-else in the pop to save it.
#[tokio::test]
async fn pipeline_depth_zero_is_rejected() {
    use ravel_object_store::memory::MemoryStore;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let m = base_mapping();
    let err = load(
        store,
        Path::new("/nonexistent.parquet"),
        "acme",
        &m,
        4,
        10,
        None,
        0,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect_err("pipeline_depth of 0 is rejected");
    assert!(
        matches!(err, LoadError::Setup(_)),
        "a typed setup error, got: {err}"
    );
    assert!(
        err.to_string()
            .contains("--pipeline-depth must be at least 1"),
        "the error names the lever: {err}"
    );
}

/// `--decode-queue-batches 0` is rejected with a typed [`LoadError::Setup`]
/// before any work, mirroring `pipeline_depth_zero_is_rejected` (issue
/// #680). A depth of 0 is a channel that can hold no batch, so the guard
/// makes it unreachable rather than letting `mpsc::channel(0)` be hit.
#[tokio::test]
async fn decode_queue_batches_zero_is_rejected() {
    use ravel_object_store::memory::MemoryStore;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let m = base_mapping();
    let err = load_instrumented(
        store,
        Path::new("/nonexistent.parquet"),
        "acme",
        &m,
        4,
        10,
        0,
        None,
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        0,
        DEFAULT_TARGET_BYTES,
        None,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
        LoadPath::Columnar,
        None,
        None,
    )
    .await
    .expect_err("decode_queue_batches of 0 is rejected");
    assert!(
        matches!(err, LoadError::Setup(_)),
        "a typed setup error, got: {err}"
    );
    assert!(
        err.to_string()
            .contains("--decode-queue-batches must be at least 1"),
        "the error names the lever: {err}"
    );
}

/// `--max-inflight-flushes 0` is rejected with a typed [`LoadError::Setup`]
/// before any work, mirroring `pipeline_depth_zero_is_rejected` (issue
/// #807). A bound of 0 builds `Semaphore::new(0)` in every shard actor, a
/// permit no flush can ever acquire, so the guard makes it unreachable
/// rather than letting the first flush trigger park forever.
///
/// Non-vacuity (prove-the-test): the message assertion is what carries the
/// weight. With the guard deleted, this fixture still fails, but on the
/// missing-file open further down, so the `LoadError::Setup(_)` shape alone
/// would pass while the flag went unvalidated; only the
/// `--max-inflight-flushes must be at least 1` text distinguishes the two.
#[tokio::test]
async fn max_inflight_flushes_zero_is_rejected() {
    use ravel_object_store::memory::MemoryStore;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let m = base_mapping();
    let err = load_instrumented(
        store,
        Path::new("/nonexistent.parquet"),
        "acme",
        &m,
        4,
        10,
        0,
        None,
        1,
        0,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
        LoadPath::Columnar,
        None,
        None,
    )
    .await
    .expect_err("max_inflight_flushes of 0 is rejected");
    assert!(
        matches!(err, LoadError::Setup(_)),
        "a typed setup error, got: {err}"
    );
    assert!(
        err.to_string()
            .contains("--max-inflight-flushes must be at least 1"),
        "the error names the lever: {err}"
    );
}

/// A multi-batch Parquet fixture with a dictionary-encoded string column, a
/// plain string column, a resource column, and a ts column, so the decode
/// stage does real `build_columnar_batch` work (dictionary interning
/// included) rather than trivial passthrough.
fn multi_batch_dict_fixture() -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
    use parquet::arrow::ArrowWriter;

    let n = 8usize;
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("overlap.parquet");
    let ts: Vec<i64> = (0..n).map(|k| NOW_NS - (k as i64) * 1_000).collect();
    let dictvals: Vec<Option<&str>> = (0..n)
        .map(|k| {
            if k % 3 == 0 {
                None
            } else {
                Some(["a", "b", "c"][k % 3])
            }
        })
        .collect();
    let dictcol =
        Arc::new(dictvals.into_iter().collect::<DictionaryArray<Int32Type>>()) as ArrayRef;
    let plain = Arc::new(StringArray::from(
        (0..n).map(|k| format!("p{}", k % 4)).collect::<Vec<_>>(),
    )) as ArrayRef;
    let svc = Arc::new(StringArray::from_iter_values(
        (0..n).map(|k| format!("svc{}", k % 2)),
    )) as ArrayRef;
    let b = batch(vec![
        ("ts", Arc::new(Int64Array::from(ts)) as ArrayRef),
        ("svc", svc),
        ("dictcol", dictcol),
        ("plaincol", plain),
    ]);
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut w = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
    w.write(&b).expect("write batch");
    w.close().expect("close writer");

    let mut m = base_mapping();
    m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
    m.attributes = vec![
        attr("dictkey", "dictcol", ColType::Str),
        attr("plainkey", "plaincol", ColType::Str),
    ];
    (dir, pq, m)
}

/// Drive the decode stage at a given queue depth and return the BLAKE3 of
/// each built columnar batch's RLOG encoding, in build order. The writer is
/// pinned to a fixed identity so the encoding depends only on batch content,
/// not on the router's random per-run `writer_id`.
async fn decode_object_hashes(
    pq: &Path,
    mapping: &Mapping,
    shards: u32,
    batch_rows: usize,
    read_cursors: Option<usize>,
    queue_depth: usize,
    reader_batch_rows_override: Option<usize>,
) -> Vec<[u8; 32]> {
    let input = FileInput { path: pq };
    let metadata = read_input_metadata(&input).expect("read metadata");
    let row_group_lens = row_group_row_counts(&metadata);
    let cursor_count = resolve_read_cursors(read_cursors, shards, row_group_lens.len());
    // `None` is what `load` passes; `Some(batch_rows)` reproduces the
    // pre-#2613 reader (one `batch_rows`-row Arrow batch per block).
    let reader_rows =
        reader_batch_rows_override.unwrap_or_else(|| reader_batch_rows(batch_rows, cursor_count));
    let cursors = open_stride_cursors(
        &input,
        &metadata,
        &row_group_lens,
        cursor_count,
        batch_rows,
        reader_rows,
    )
    .expect("cursors");
    let state = StrideCursors {
        cursors,
        deal_offset: 0,
        skip_rows: 0,
    };
    let (mut rx, handle) = spawn_decode_pipeline(
        state,
        Arc::new(mapping.clone()),
        LogIngestLimits::default(),
        NOW_NS,
        batch_rows,
        LoadPath::Columnar,
        None,
        None,
        queue_depth,
        None,
    );
    let mut hashes = Vec::new();
    while let Some(p) = rx.recv().await {
        match p {
            Prefetched::Batch(Built::Columnar(b, _)) => {
                if b.num_rows > 0 {
                    hashes.push(*blake3::hash(&columnar_object(*b)).as_bytes());
                }
            }
            Prefetched::Batch(Built::Row(_)) => panic!("columnar path only"),
            Prefetched::Done => {}
            Prefetched::BatchFailed { reason } => panic!("batch failed: {reason}"),
            Prefetched::RowRejected { row, reason } => {
                panic!("row {row} rejected: {reason}")
            }
        }
    }
    handle.await.expect("decoder task joins");
    hashes
}

/// Byte-identity across decode-queue depths (issue #680): moving scheduling
/// (the depth of the decode->encode channel) must not change the bytes of
/// any RLOG object. The same fixture decoded at depth 1 (today's near-
/// lockstep) and depth 4 must produce the identical ordered sequence of
/// per-batch RLOG encodings. The comparison is at the batch encoding rather
/// than at the stored object because the production `LogIngestRouter` draws a
/// random `writer_id` per construction (crates/ravel-ingest, `SystemRng`),
/// so two full loads never share stored bytes regardless of this change; the
/// batch content the writer consumes is exactly what scheduling could
/// perturb, and it is what this pins.
///
/// Prove-the-test: make `spawn_decode_pipeline` reorder or drop a batch (or
/// have `build_columnar_batch` depend on queue depth) and the two hash lists
/// diverge. Confirmed conceptually by the depth-1-vs-4 equality: the only
/// difference between the runs is the channel capacity.
#[tokio::test]
async fn rlog_objects_are_byte_identical_across_decode_queue_depths() {
    let (_dir, pq, m) = multi_batch_dict_fixture();
    // batch_rows = 2 over 8 rows -> four batches.
    let lockstep = decode_object_hashes(&pq, &m, 4, 2, None, 1, None).await;
    let deep = decode_object_hashes(&pq, &m, 4, 2, None, 4, None).await;
    assert!(
        lockstep.len() >= 3,
        "the fixture splits into several batches: {}",
        lockstep.len()
    );
    assert_eq!(
        lockstep,
        deep,
        "RLOG object bytes must not depend on the decode-queue depth ({} objects)",
        lockstep.len()
    );
}

/// A file of five row groups of uneven sizes (700, 300, 500, 900, 200 rows);
/// see [`seq_row_group_fixture`].
fn uneven_row_group_fixture() -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
    seq_row_group_fixture(&[700, 300, 500, 900, 200])
}

/// A file with one row group per entry of `group_rows` (none at all for an
/// empty slice) whose every row is distinguishable: `seq` is the
/// file-absolute row index as an i64 attribute and `tag` a per-row string, so
/// a batch that gained, lost or reordered a single row encodes to different
/// bytes. Resource identity rotates over four hosts so rows spread across
/// shards.
fn seq_row_group_fixture(group_rows: &[i64]) -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
    use parquet::arrow::ArrowWriter;

    let seq_batch = |first: i64, rows: i64| {
        let seq: Vec<i64> = (first..first + rows).collect();
        let ts: Vec<i64> = seq.iter().map(|r| NOW_NS - r * 1_000).collect();
        let tag: Vec<String> = seq.iter().map(|r| format!("tag-{r}")).collect();
        let host: Vec<String> = seq.iter().map(|r| format!("host{}", r % 4)).collect();
        batch(vec![
            ("ts", Arc::new(Int64Array::from(ts)) as ArrayRef),
            ("seq", Arc::new(Int64Array::from(seq)) as ArrayRef),
            (
                "tag",
                Arc::new(StringArray::from_iter_values(tag)) as ArrayRef,
            ),
            (
                "host",
                Arc::new(StringArray::from_iter_values(host)) as ArrayRef,
            ),
        ])
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("seq.parquet");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer =
        ArrowWriter::try_new(file, seq_batch(0, 0).schema(), None).expect("arrow writer");
    let mut file_row = 0i64;
    for &rows in group_rows {
        writer
            .write(&seq_batch(file_row, rows))
            .expect("write row group");
        writer.flush().expect("flush row group");
        file_row += rows;
    }
    writer.close().expect("close writer");

    let mut m = base_mapping();
    m.resource_attributes = vec![attr("host", "host", ColType::Str)];
    m.attributes = vec![
        attr("seq", "seq", ColType::I64),
        attr("tag", "tag", ColType::Str),
    ];
    (dir, pq, m)
}

/// The production dealer's RLOG bytes do not depend on the cursors' Arrow
/// batch size (issue #2613): the same dealer over readers decoding
/// `batch_rows`-row batches and over readers decoding `ceil(batch_rows / K)`
/// rows encodes the same per-batch RLOG bytes. Both arms run today's dealer,
/// so this says nothing about the pre-#2613 dealer; that comparison is
/// `pre_2613::batch_composition_matches_the_pre_2613_dealer`.
///
/// Prove-the-test: have `cursor_take_spans` make a single `cursor_take` call
/// per round, so a share is capped at one reader batch, and it fails at
/// (450, 3). A dealer change that applies to both arms, such as ignoring
/// `block_rows`, passes here; the `pre_2613` tests catch that.
#[tokio::test]
async fn production_dealer_rlog_bytes_do_not_depend_on_reader_batch_size() {
    let (_dir, pq, m) = uneven_row_group_fixture();
    for (batch_rows, cursors) in [(450usize, 3usize), (1000, 4), (333, 5), (2600, 2), (100, 1)] {
        let reference =
            decode_object_hashes(&pq, &m, 4, batch_rows, Some(cursors), 2, Some(batch_rows)).await;
        let bounded = decode_object_hashes(&pq, &m, 4, batch_rows, Some(cursors), 2, None).await;
        assert!(
            reference.len() >= 2,
            "batch_rows {batch_rows} x cursors {cursors}: fixture splits into several batches, \
             got {}",
            reference.len()
        );
        assert_eq!(
            reference,
            bounded,
            "batch_rows {batch_rows} x cursors {cursors} (reader batch {}): RLOG bytes must not \
             depend on the reader's Arrow batch size",
            reader_batch_rows(batch_rows, cursors)
        );
    }
}

mod pre_2613;

/// Deal `k` cursors over `groups` row groups of `group_rows` rows each at
/// `batch_rows` until they are exhausted, with the readers `load` opens, and
/// return the rows each cursor holds decoded but undealt after every round.
fn undealt_rows_per_round(
    groups: u32,
    group_rows: usize,
    batch_rows: usize,
    k: usize,
) -> Vec<Vec<usize>> {
    let (_dir, pq, _m) = sorted_by_shard_fixture(groups, group_rows);
    let input = FileInput { path: &pq };
    let metadata = read_input_metadata(&input).expect("read metadata");
    let row_group_lens = row_group_row_counts(&metadata);
    assert_eq!(row_group_lens, vec![group_rows as u64; groups as usize]);
    let cursors = open_stride_cursors(
        &input,
        &metadata,
        &row_group_lens,
        k,
        batch_rows,
        reader_batch_rows(batch_rows, k),
    )
    .expect("cursors");
    let mut state = StrideCursors {
        cursors,
        deal_offset: 0,
        skip_rows: 0,
    };
    let total_rows = groups as usize * group_rows;
    let full_rounds = total_rows / batch_rows;
    let mut per_round = Vec::new();
    for round in 1.. {
        let dealt = match collect_spans(&mut state, batch_rows, None) {
            SpanOutcome::Spans(spans) => spans.iter().map(|(b, _)| b.num_rows()).sum::<usize>(),
            SpanOutcome::Done => break,
            SpanOutcome::Failed(reason) => panic!("round {round}: {reason}"),
        };
        if round <= full_rounds {
            assert_eq!(dealt, batch_rows, "round {round} deals one full batch");
        }
        assert!(
            round <= total_rows + k + 1,
            "still dealing at round {round}"
        );
        per_round.push(
            state
                .cursors
                .iter()
                .map(|c| c.buffered.as_ref().map_or(0, |b| b.num_rows()))
                .collect(),
        );
    }
    assert!(per_round.len() > full_rounds, "every full round was dealt");
    per_round
}

/// The decoded Arrow rows the stride cursors hold between rounds (issue
/// #2613). Before this change each of K cursors decoded a full
/// `batch_rows`-row Arrow batch of every column and kept the undealt
/// remainder, so K whole batches were alive after every round. Now a cursor
/// decodes `ceil(batch_rows / K)` rows at a time and only when its buffer is
/// empty and its share needs more, so it keeps fewer than that many rows
/// undealt, and the total no longer scales with K.
///
/// Four 1,000-row groups, four cursors, 1,000-row batches: every share is 250
/// rows, a whole reader batch, so nothing is left undealt. The pre-#2613
/// reader left 750 per cursor after round one, and a reader decoding
/// `2 * batch_rows / K` rows leaves 250.
///
/// Three 1,000-row groups, three cursors, 1,000-row batches: shares of 333
/// and 334 rows against 334-row reader batches, so a cursor may keep a row or
/// two, never 334. A `2 * batch_rows / K` (667-row) reader leaves 334 on the
/// cursors that drew a 333-row share in round one.
#[test]
fn stride_cursors_hold_under_one_reader_batch_of_undealt_rows() {
    for (round, undealt) in undealt_rows_per_round(4, 1000, 1000, 4).iter().enumerate() {
        assert_eq!(
            undealt,
            &vec![0; 4],
            "round {}: shares of a whole reader batch leave nothing undealt",
            round + 1
        );
    }

    // ceil(1000 / 3), written out so a change to `reader_batch_rows` cannot
    // move the bound with it.
    let reader_rows = 334;
    for (round, undealt) in undealt_rows_per_round(3, 1000, 1000, 3).iter().enumerate() {
        assert!(
            undealt.iter().all(|&n| n < reader_rows),
            "round {}: undealt rows per cursor {undealt:?}, each must be under one {reader_rows}-row \
             reader batch",
            round + 1
        );
    }
}

/// End-to-end structural invariance across decode-queue depths (issue #680):
/// a real `load_instrumented` at depth 1 and at depth 4, each into its own
/// `MemoryStore`, writes the same number of data objects with the same
/// multiset of sizes. (Object bytes themselves differ only by the router's
/// random `writer_id`; size is invariant to it, so equal sorted sizes plus
/// equal counts is the strongest depth-independent store-level check. The
/// content-level guarantee is `rlog_objects_are_byte_identical_...` above.)
#[tokio::test]
async fn load_writes_the_same_objects_across_decode_queue_depths() {
    use ravel_object_store::memory::MemoryStore;

    let (_dir, pq, m) = multi_batch_dict_fixture();

    let run = |depth: usize| {
        let pq = pq.clone();
        let m = m.clone();
        async move {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            load_instrumented(
                Arc::clone(&store),
                &pq,
                "acme",
                &m,
                4,
                2,
                0,
                None,
                2,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                depth,
                DEFAULT_TARGET_BYTES,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
                LoadPath::Columnar,
                None,
                None,
            )
            .await
            .expect("load succeeds");
            let mut objs = list_data_objects(store.as_ref()).await;
            objs.sort_by_key(|(_, size)| *size);
            objs.into_iter().map(|(_, size)| size).collect::<Vec<u64>>()
        }
    };

    let sizes_1 = run(1).await;
    let sizes_4 = run(4).await;
    assert!(!sizes_1.is_empty(), "the load wrote data objects");
    assert_eq!(
        sizes_1, sizes_4,
        "object count and sizes must not depend on the decode-queue depth"
    );
}

/// A store that holds only the FIRST data-object PUT for a fixed duration,
/// snapshotting a shared counter at the moment that PUT completes. Every
/// other PUT and every non-PUT call passes straight through.
struct FirstPutHoldStore {
    inner: Arc<dyn ObjectStoreBackend>,
    /// Hold the first data PUT until the decoder has queued this many
    /// batches, then snapshot the count. The bounded decode channel is
    /// what stops the decoder, so this target is reached (and not
    /// exceeded) as a property of the code rather than of how many
    /// batches the host managed inside a fixed hold.
    hold_until_queued: usize,
    queued: Arc<std::sync::atomic::AtomicUsize>,
    first_seen: Arc<std::sync::atomic::AtomicBool>,
    snapshot: Arc<std::sync::Mutex<Option<usize>>>,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for FirstPutHoldStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: ravel_object_store::PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        use std::sync::atomic::Ordering;
        if key.contains("/l0/") && !self.first_seen.swap(true, Ordering::SeqCst) {
            // Bounded so a regression that stops the decoder short fails
            // the assertion on the snapshot rather than hanging the suite.
            for _ in 0..10_000 {
                if self.queued.load(Ordering::SeqCst) >= self.hold_until_queued {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            *self.snapshot.lock().expect("snapshot lock") =
                Some(self.queued.load(Ordering::SeqCst));
        }
        self.inner.put(key, data, opts).await
    }

    async fn get(
        &self,
        key: &str,
        range: ravel_object_store::GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, ravel_object_store::StoreError>
    {
        self.inner.put_multipart(key).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        page: Option<ravel_object_store::PageToken>,
    ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(
        &self,
        prefix: &str,
    ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> ravel_object_store::Capabilities {
        self.inner.capabilities()
    }
}

/// The decoder runs more than one batch ahead of the encoders, and no
/// further than the queue depth (issue #680). With `--pipeline-depth 1` the
/// loader consumes exactly one batch, spawns its write, and blocks awaiting
/// that write's ack. Holding that first data PUT lets the decoder fill the
/// decode->encode channel and block on back-pressure: it will have queued
/// exactly `decode_queue_batches + 1` batches (one consumed by the loop plus
/// `decode_queue_batches` buffered in the full channel), no matter how many
/// batches remain in the file.
///
/// Non-vacuity (prove-the-test): forcing lockstep by changing
/// `QUEUE_DEPTH` to 1 leaves only 2 batches queued (1 consumed + 1
/// buffered), which fails the `> 2` "more than one ahead" assertion; and if
/// the channel were unbounded the decoder would race to queue all ~20
/// batches, failing the `== QUEUE_DEPTH + 1` bound.
#[tokio::test]
async fn decoder_runs_ahead_bounded_by_the_queue_depth() {
    use ravel_object_store::memory::MemoryStore;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const QUEUE_DEPTH: usize = 3;
    // Many small batches so the bound (not the file's end) is what stops the
    // decoder: batch_rows = 2 over 40 rows -> 20 batches.
    let n_rows = 40usize;
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("many.parquet");
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS; n_rows])),
        ("svc", str_col(vec!["api"; n_rows])),
    ]);
    {
        use parquet::arrow::ArrowWriter;
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut w = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        w.write(&b).expect("write batch");
        w.close().expect("close writer");
    }
    let m = parse_mapping(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n",
    )
    .expect("valid mapping");

    let queued = Arc::new(AtomicUsize::new(0));
    let snapshot = Arc::new(std::sync::Mutex::new(None));
    let store = Arc::new(FirstPutHoldStore {
        inner: Arc::new(MemoryStore::new()),
        // The channel bound stops the decoder at exactly this many
        // (1 consumed by the loop + QUEUE_DEPTH buffered), which is also
        // what the assertions below pin. Holding until the decoder gets
        // there replaces a fixed 300 ms hold whose outcome was however
        // many batches the host happened to decode in that window.
        hold_until_queued: QUEUE_DEPTH + 1,
        queued: Arc::clone(&queued),
        first_seen: Arc::new(AtomicBool::new(false)),
        snapshot: Arc::clone(&snapshot),
    });

    let queued_hook = Arc::clone(&queued);
    let on_queued: BuildStartHook = Arc::new(move || {
        queued_hook.fetch_add(1, Ordering::SeqCst);
    });

    let report = load_instrumented(
        store as Arc<dyn ObjectStoreBackend>,
        &pq,
        "acme",
        &m,
        1,
        2,
        0,
        None,
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        QUEUE_DEPTH,
        DEFAULT_TARGET_BYTES,
        None,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
        LoadPath::Columnar,
        None,
        Some(on_queued),
    )
    .await
    .expect("the pipelined load succeeds");
    assert_eq!(report.rows_processed, n_rows as u64, "every row is written");

    let ahead = snapshot
        .lock()
        .expect("snapshot lock")
        .expect("the first data PUT was held and snapshotted");
    assert!(
        ahead > 2,
        "the decoder ran more than one batch ahead while the encoder was blocked \
             (a lockstep depth-1 loop leaves it at 2): queued = {ahead}"
    );
    assert_eq!(
        ahead,
        QUEUE_DEPTH + 1,
        "and no further: 1 consumed by the loop plus {QUEUE_DEPTH} buffered in the full channel"
    );
}

/// Issue #296 reachability, end to end through the loader: a multi-shard
/// batch where one shard's data-object PUT fails permanently while a
/// sibling shard commits durably. `load` must return `LoadError::Flush`
/// whose durable-token list -- the exact list `print_durable_tokens` prints
/// -- includes the surviving shard's token, where before the fix that token
/// was structurally unreportable and the list undercounted.
///
/// Non-vacuity (prove-the-test): the failing shard (shard 0) sorts first in
/// the router's ack loop, so the pre-fix early return dropped shard 1's
/// token; against that code this fails at `durable.len() == 1` (the list is
/// empty). The `FaultStore` counter is asserted so the abandonment is
/// proven to have fired.
#[tokio::test]
async fn flush_failure_reports_the_surviving_shards_durable_token() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;

    let shards = 4;
    let h_victim = host_for_shard(0, shards);
    let h_survivor = host_for_shard(1, shards);

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("two_shards.parquet");
    let cols: Vec<(String, ArrayRef)> = vec![
        ("ts".to_string(), i64_col(vec![NOW_NS, NOW_NS])),
        ("svc".to_string(), str_col(vec!["api", "api"])),
        (
            "host".to_string(),
            str_col(vec![h_victim.as_str(), h_survivor.as_str()]),
        ),
    ];
    let batch = RecordBatch::try_from_iter(cols).expect("two-row batch");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    let m = parse_mapping(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
    )
    .expect("valid mapping");

    // Fail every data-object PUT for shard 0 (`/l0/0000/`) permanently: a
    // non-retryable error abandons that flush at once, deterministically,
    // while shard 1 commits normally in the same Strict write.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
        )
        .with_key_contains("/l0/0000/")
        .with_occurrence(Occurrence::Always),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));

    let err = load(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        &pq,
        "acme",
        &m,
        shards,
        // Both rows in one batch, so one Strict write spans both shards.
        10,
        None,
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect_err("one shard's flush was abandoned, so the load fails");

    let durable = match &err {
        LoadError::Flush { durable, cause, .. } => {
            assert!(
                cause.contains("flush abandoned"),
                "the flush failure classifies as the underlying abandonment: {cause}"
            );
            durable.clone()
        }
        other => panic!("expected LoadError::Flush, got {other:?}"),
    };

    // The exact list `print_durable_tokens` iterates: the surviving shard's
    // token, recovered from the write error (issue #296).
    assert_eq!(
        err.durable_tokens().len(),
        1,
        "the surviving shard's token reaches the printed durable list, got {durable:?}"
    );
    assert_eq!(
        durable[0].shard, 1,
        "the recovered token is the surviving shard's (shard 1), not the abandoned shard 0"
    );

    assert_eq!(
        store.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the permanent data-object PUT fault fired exactly once (shard 0, no retry)"
    );
}

/// A store wrapper whose data-object (`/l0/`) PUT sleeps a fixed duration
/// before completing, and which snapshots a shared "builds started" counter
/// at the moment each such PUT finishes. Non-data PUTs (provisioning record,
/// commit records) pass straight through, so only a batch's real RSEG write
/// is timed. Every other method delegates unchanged.
struct SlowPutStore {
    inner: Arc<dyn ObjectStoreBackend>,
    put_delay: Duration,
    builds_started: Arc<std::sync::atomic::AtomicUsize>,
    /// `builds_started` observed at completion of each data-object PUT.
    snapshots: Arc<std::sync::Mutex<Vec<usize>>>,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for SlowPutStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: ravel_object_store::PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        let is_data_object = key.contains("/l0/");
        if is_data_object {
            tokio::time::sleep(self.put_delay).await;
        }
        let result = self.inner.put(key, data, opts).await;
        if is_data_object {
            self.snapshots.lock().expect("snapshots lock").push(
                self.builds_started
                    .load(std::sync::atomic::Ordering::SeqCst),
            );
        }
        result
    }

    async fn get(
        &self,
        key: &str,
        range: ravel_object_store::GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, ravel_object_store::StoreError>
    {
        self.inner.put_multipart(key).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        page: Option<ravel_object_store::PageToken>,
    ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(
        &self,
        prefix: &str,
    ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> ravel_object_store::Capabilities {
        self.inner.capabilities()
    }
}

/// The pipeline actually overlaps (issue #541): batch N+1's decode/build
/// begins while batch N's slow object-store PUT is still in flight, not
/// after it returns. A single stream over `batch_rows = 2` splits into three
/// batches; each batch's RSEG PUT sleeps 50ms, and the decode/build start
/// hook bumps a shared counter. At the completion of every data-object PUT
/// the counter is snapshotted; a value >= 2 means the *next* batch's build
/// had already started before this batch's PUT returned.
///
/// Non-vacuity (prove-the-test): against the former fully-serial loop
/// (revert the `spawn_build` lookahead so batch N is decoded, written, and
/// awaited before batch N+1 is even read) the first data PUT completes with
/// only batch 0 built, so the snapshot is 1 and the `min >= 2` assertion
/// fails.
#[tokio::test]
async fn next_batch_decode_overlaps_current_batch_write() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let n_rows = 6;
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("multi.parquet");
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS; n_rows])),
        ("svc", str_col(vec!["api"; n_rows])),
    ]);
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
    writer.write(&b).expect("write batch");
    writer.close().expect("close writer");

    let m = parse_mapping(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n",
    )
    .expect("valid mapping");

    let builds_started = Arc::new(AtomicUsize::new(0));
    let snapshots = Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
    let store = Arc::new(SlowPutStore {
        inner: Arc::new(MemoryStore::new()),
        put_delay: Duration::from_millis(50),
        builds_started: Arc::clone(&builds_started),
        snapshots: Arc::clone(&snapshots),
    });

    let hook_counter = Arc::clone(&builds_started);
    let hook: BuildStartHook = Arc::new(move || {
        hook_counter.fetch_add(1, Ordering::SeqCst);
    });

    // `batch_rows = 2` over 6 rows yields three batches through one shard,
    // so three RSEG PUTs happen in sequence.
    let report = load_instrumented(
        store as Arc<dyn ObjectStoreBackend>,
        &pq,
        "acme",
        &m,
        1,
        2,
        0,
        None,
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
        LoadPath::Columnar,
        Some(hook),
        None,
    )
    .await
    .expect("the pipelined load succeeds");

    assert_eq!(report.rows_processed, n_rows as u64, "every row is written");

    let snaps = snapshots.lock().expect("snapshots lock").clone();
    assert!(
        snaps.len() >= 2,
        "at least two data-object PUTs happened (three batches, one shard): {snaps:?}"
    );
    let min = *snaps.iter().min().expect("non-empty snapshots");
    assert!(
        min >= 2,
        "batch N+1's decode/build must start before batch N's slow PUT returns \
             (a serial loop leaves the counter at 1 when the first PUT completes); \
             builds-started-at-PUT-completion snapshots = {snaps:?}"
    );
    assert!(
        builds_started.load(Ordering::SeqCst) >= 3,
        "all three batches were decoded/built, got {}",
        builds_started.load(Ordering::SeqCst)
    );
}

/// A rejected row in the *second* batch must report its absolute index
/// into the whole file, not an index relative to that batch or to its own
/// stride cursor's partition. `file_base` is threaded through
/// `decode_and_build_stride`, a free function outside the loop the
/// prefetch refactor introduced, and easy to drop by accident (no
/// existing test drove a real multi-batch `load()` far enough to catch
/// it: `future_skew_beyond_the_bound_is_rejected` calls `build_record`
/// directly on row 0, and `batch_failed_reports_durable_tokens_not_empty`
/// hand-constructs a `LoadError` rather than running a load). Change
/// `file_base + row as u64` to `row as u64` in `decode_and_build_stride`'s
/// `RowRejected` arm and the first half of this test (the `read-cursors`
/// auto-resolved to `1` case below) fails at `assert_eq!(row, 2, ..)` with
/// `left: 0, right: 2` -- confirmed by performing the flip. The test
/// panics there, before ever reaching the second half, because both
/// halves exercise the same shared line: `decode_and_build_stride` is now
/// the only row-decode path, used for every `--read-cursors` value
/// including `1`, so this one flip is non-vacuous for both.
///
/// Extended for issue #560: under `--read-cursors 4`, the same guarantee
/// must hold when the rejected row sits in a stride cursor whose own
/// partition starts partway through the file (row 5, at local index 1
/// within its own stride cursor's 2-row span starting at file row 4), so
/// the translation is via that span's own `file_base`, not a single
/// file-wide accumulator.
#[tokio::test]
async fn a_rejected_row_in_a_later_batch_reports_its_absolute_index() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;

    let limits = LogIngestLimits::default();
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("two_batches.parquet");

    // Rows 0-1 are batch 1 (batch_rows: 2) and valid. Row 2, the first row
    // of batch 2, is far enough in the future to be rejected.
    let ts = vec![
        NOW_NS,
        NOW_NS,
        NOW_NS + limits.max_future_skew_ns + 1,
        NOW_NS,
    ];
    let batch_data = batch(vec![("ts", i64_col(ts))]);
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch_data.schema(), None).expect("arrow writer");
    writer.write(&batch_data).expect("write batch");
    writer.close().expect("close writer");

    let m = base_mapping();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let err = load(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        4,
        2,
        None,
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect_err("the far-future row must be rejected");

    match err {
        LoadError::RowRejected { row, durable, .. } => {
            assert_eq!(
                row, 2,
                "row index must be absolute across batches, not relative to its own batch"
            );
            assert!(
                !durable.is_empty(),
                "batch 1's write must already be durable: it was fully awaited before \
                     batch 2 was even decoded"
            );
        }
        other => panic!("expected RowRejected, got {other:?}"),
    }

    // Same guarantee under `--read-cursors 4`: a 4-row-group file, one row
    // group per stride cursor, with the future-skew violator at
    // file-absolute row 5 (local index 1 within row group 2's 2-row
    // span). Its reported index must still be 5, translated via that
    // span's own `file_base` rather than a shared `row_base`.
    let pq4 = dir.path().join("four_row_groups.parquet");
    let row_groups: Vec<Vec<i64>> = vec![
        vec![NOW_NS, NOW_NS],
        vec![NOW_NS, NOW_NS],
        vec![NOW_NS, NOW_NS + limits.max_future_skew_ns + 1],
        vec![NOW_NS, NOW_NS],
    ];
    let file4 = std::fs::File::create(&pq4).expect("create parquet");
    let mut writer4 = ArrowWriter::try_new(file4, batch_data.schema(), None).expect("arrow writer");
    for rg in &row_groups {
        let rg_batch = batch(vec![("ts", i64_col(rg.clone()))]);
        writer4.write(&rg_batch).expect("write row group");
        writer4.flush().expect("flush row group");
    }
    writer4.close().expect("close writer");

    let err4 = load(
        Arc::clone(&store),
        &pq4,
        "acme",
        &m,
        4,
        8,
        Some(4),
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect_err("the far-future row must be rejected under read-cursors=4");

    match err4 {
        LoadError::RowRejected { row, .. } => {
            assert_eq!(
                row, 5,
                "row index must be FILE-absolute even when a stride cursor's own \
                     span starts partway through the file"
            );
        }
        other => panic!("expected RowRejected, got {other:?}"),
    }
}

#[tokio::test]
async fn stride_reading_spreads_a_sorted_batch_across_all_shards() {
    use ravel_object_store::memory::MemoryStore;

    let shards = 4u32;
    let rows_per_group = 4usize;
    let (_dir, pq, m) = sorted_by_shard_fixture(shards, rows_per_group);

    let n_rows = rows_per_group * shards as usize;
    let batches = n_rows / rows_per_group;

    let store4: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report4 = load(
        store4,
        &pq,
        "acme",
        &m,
        shards,
        rows_per_group,
        Some(shards as usize),
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect("stride-read load succeeds");
    assert_eq!(report4.rows_processed, n_rows as u64);
    assert_eq!(
        report4.objects_written(),
        batches * shards as usize,
        "read-cursors=4: every batch draws one row from each row group/shard"
    );

    let store1: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report1 = load(
        store1,
        &pq,
        "acme",
        &m,
        shards,
        rows_per_group,
        Some(1),
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect("sequential-read load succeeds");
    assert_eq!(report1.rows_processed, n_rows as u64);
    assert_eq!(
        report1.objects_written(),
        batches,
        "read-cursors=1: each batch is one whole row group, one shard"
    );
}

/// A flush target above one batch's encoded size is the object-count lever
/// `--batch-rows` cannot be without a linear memory cost (issue #801).
///
/// Geometry, pinned on both sides: the 4-row-group fixture holds 4 rows per
/// group, one shard's `host` value per group, so `--batch-rows 4` with
/// `--read-cursors 4` yields exactly 4 batches, each drawing one row from
/// every group and therefore touching all 4 shards -- 16 (batch, shard)
/// writes over 16 rows.
///
/// At the default `--target-bytes 1` each of those 16 writes flushes inside
/// its own `handle_write`: 16 objects. At 8 MiB none of them can, because a
/// shard's whole share of the file is 4 one-row writes, so each shard
/// flushes exactly once (released by [`load_with_released_tail`], which
/// advances the injected clock past the age trigger once every batch has
/// been routed): 4 objects, one per shard. Same 16 rows, same decoded
/// records.
///
/// Prove-the-test: hardcode `target_bytes: 1` back into the `IngestConfig`
/// in `load_instrumented` and the large-target side writes 16 objects, not
/// 4 (`left: 16, right: 4`). Reverting `objects_written` to `tokens.len()`
/// fails the same side at `left: 16, right: 4`, because one flush answers
/// four batches' acks with the same token.
#[tokio::test]
async fn a_larger_target_bytes_writes_strictly_fewer_objects_for_the_same_rows() {
    use ravel_object_store::memory::MemoryStore;

    let shards = 4u32;
    let rows_per_group = 4usize;
    let (_dir, pq, m) = sorted_by_shard_fixture(shards, rows_per_group);
    let n_rows = (rows_per_group * shards as usize) as u64;

    let run = |target_bytes: usize| {
        let pq = pq.clone();
        let m = m.clone();
        async move {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let report = load_with_released_tail(
                Arc::clone(&store),
                &pq,
                &m,
                shards,
                rows_per_group,
                shards as usize,
                rows_per_group,
                target_bytes,
            )
            .await;
            let stored = list_data_objects(store.as_ref()).await.len();
            (report, stored, decoded_records(store.as_ref()).await)
        }
    };

    let (small, stored_small, records_small) = run(DEFAULT_TARGET_BYTES).await;
    let (large, stored_large, records_large) = run(8 * 1024 * 1024).await;

    assert_eq!(small.rows_processed, n_rows);
    assert_eq!(large.rows_processed, n_rows);
    assert_eq!(
        stored_small, 16,
        "target 1: each of the 4 batches flushes on all 4 shards at once"
    );
    assert_eq!(
        stored_large, 4,
        "target 8 MiB: each shard accumulates all 4 batches into one object"
    );
    assert_eq!(
        small.objects_written(),
        stored_small,
        "the reported object count must equal what the store actually holds"
    );
    assert_eq!(
        large.objects_written(),
        stored_large,
        "the reported object count must equal what the store actually holds; \
             an ack that was answered by an earlier batch's flush repeats that \
             flush's token rather than naming a new object"
    );
    assert_eq!(
        records_small, records_large,
        "the same rows, decoded, regardless of how many objects hold them"
    );
}

/// Issue #971: what `--target-bytes` regime a value falls into is decided by
/// one batch's PER-SHARD SLICE footprint, not by how large the byte figure
/// looks next to the objects the load writes. Four targets over one input,
/// each with a derivable object count.
///
/// Geometry: 4 row groups of 16 rows, one shard's `host` value per group,
/// read with `--read-cursors 4 --batch-rows 16`, so there are 4 batches and
/// each batch puts 4 rows on every one of the 4 shards: 16 (batch, shard)
/// writes over 64 rows. Every row carries a 4000-byte record attribute, so
/// one row's estimated footprint is about 4.1 KB (the 4000 value bytes, a
/// 56-byte pair header, the 7-byte key, the stream-attribute blob and 32
/// fixed bytes), one slice's about 16.6 KB, and a shard's whole share of the
/// file about 66 KB.
///
/// - `1`: every write flushes itself. 4 x 4 = 16 objects.
/// - `4096`: a quarter of one slice, so every write still reaches the target
///   on its own and flushes. 16 objects, IDENTICAL to `1`. This is the
///   reported defect in miniature: a byte figure that looks generous beside
///   the objects it produces (4 rows each) but sits far below the footprint
///   estimate it is actually compared against.
/// - `24576`: above one slice and below two, so each shard flushes on every
///   second batch. 4 shards x 2 = 8 objects, exactly half of 16.
/// - `1 MiB`: above a shard's whole 66 KB share, so each shard flushes once,
///   released by the tail advance. 4 objects, one per shard.
///
/// Every regime runs through [`load_with_released_tail`], so the four counts
/// are a function of `target_bytes` and the geometry alone: the router's
/// clock is frozen, and the test advances it past `max_flush_delay` once,
/// after every batch has been routed, to release the tail (issue #1111).
///
/// Prove-the-test: hardcode `target_bytes: 1` into the `IngestConfig` in
/// `load_instrumented` and both effective regimes fail (`left: 16, right:
/// 8`), while the two no-effect regimes stay green, which is exactly the
/// asymmetry the issue reported. Under-counting the magnitudes fails too:
/// asserting 4 objects for the `24576` regime (a floor a fraction of the
/// truth would clear) fails at `left: 8, right: 4`.
#[tokio::test]
async fn target_bytes_regimes_are_set_by_one_batchs_per_shard_slice() {
    use ravel_object_store::memory::MemoryStore;

    let shards = 4u32;
    let rows_per_group = 16usize;
    let batch_rows = 16usize;
    let (_dir, pq, _mapping_path, m) =
        fat_attr_sorted_by_shard_fixture(shards, rows_per_group, 4000);
    let n_rows = (rows_per_group * shards as usize) as u64;
    let batches = rows_per_group / (batch_rows / shards as usize);
    assert_eq!(batches, 4, "the geometry must yield 4 batches");

    let run = |target_bytes: usize| {
        let pq = pq.clone();
        let m = m.clone();
        async move {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let report = load_with_released_tail(
                Arc::clone(&store),
                &pq,
                &m,
                shards,
                batch_rows,
                shards as usize,
                batches,
                target_bytes,
            )
            .await;
            let stored = list_data_objects(store.as_ref()).await.len();
            (report, stored, decoded_records(store.as_ref()).await)
        }
    };

    let (default, objects_default, records_default) = run(DEFAULT_TARGET_BYTES).await;
    let (below, objects_below, records_below) = run(4096).await;
    let (mid, objects_mid, _) = run(24_576).await;
    let (whole, objects_whole, records_whole) = run(1024 * 1024).await;

    for report in [&default, &below, &mid, &whole] {
        assert_eq!(report.rows_processed, n_rows, "every run loads every row");
    }
    assert_eq!(
        objects_default, 16,
        "target 1: each of the 4 batches flushes on all 4 shards at once"
    );
    assert_eq!(
        objects_below, objects_default,
        "target 4096 is a quarter of one (batch, shard) slice's estimated footprint, so every \
             write still flushes itself: the same layout target 1 produces"
    );
    assert_eq!(
        objects_below, 16,
        "and that layout is 4 batches x 4 shards, pinned rather than compared"
    );
    assert_eq!(
        objects_mid, 8,
        "target 24576 sits above one slice and below two, so each shard flushes every second \
             batch: 4 shards x 2 flushes"
    );
    assert_eq!(
        objects_whole, 4,
        "target 1 MiB is above a shard's whole share of the file, so each shard flushes once"
    );
    // Which trigger opened each object, not only how many there were: the
    // flake this test used to carry (issue #1111) showed up here first, as
    // size 4 / final 8 on the mid regime, before it showed up as 12 objects.
    for (target, report, expected) in [
        (
            DEFAULT_TARGET_BYTES,
            &default,
            FlushMixCounts {
                size: 16,
                age: 0,
                final_drain: 0,
            },
        ),
        (
            4096,
            &below,
            FlushMixCounts {
                size: 16,
                age: 0,
                final_drain: 0,
            },
        ),
        (
            24_576,
            &mid,
            FlushMixCounts {
                size: 8,
                age: 0,
                final_drain: 0,
            },
        ),
        (
            1024 * 1024,
            &whole,
            FlushMixCounts {
                size: 0,
                age: 4,
                final_drain: 0,
            },
        ),
    ] {
        assert_eq!(
            report.flush_mix_report().totals,
            expected,
            "target {target}: every object below the target's reach is opened by the tail \
                 advance and every one at or above it by the size trigger; nothing is left for \
                 the end-of-input drain to sweep"
        );
    }
    for report in [&default, &below, &mid, &whole] {
        assert_eq!(
            report.tokens.len(),
            16,
            "every run acks the same 16 (batch, shard) writes whatever the target; only how \
                 many distinct objects answer them changes"
        );
    }
    assert_eq!(
        below.objects_written(),
        objects_below,
        "the reported object count must equal what the store holds"
    );
    assert_eq!(
        mid.objects_written(),
        objects_mid,
        "the reported object count must equal what the store holds"
    );
    assert_eq!(
        whole.objects_written(),
        objects_whole,
        "the reported object count must equal what the store holds"
    );
    assert_eq!(
        records_below, records_default,
        "the same rows, decoded, regardless of how many objects hold them"
    );
    assert_eq!(
        records_whole, records_default,
        "the same rows, decoded, regardless of how many objects hold them"
    );
}

/// The ack semantics a larger `--target-bytes` changes (issue #801,
/// deliverable 3). A Strict write's ack is answered from `ack_waiters` in
/// `crates/ravel-ingest/src/log_shard.rs`, which only runs once that
/// buffer's flush has published its object and commit record. So an ack
/// still means durable -- but above target 1 the flush that answers it is
/// triggered by a LATER batch (or by the age trigger), not by the write
/// itself, so the ack now waits for one.
///
/// Pinned without a timing band: under a [`TestClock`] this test never
/// advances, the age trigger can never fire (the shard actor's flush tick
/// waits on that clock's own `sleep`, which only returns on an advance),
/// and at an 8 MiB target no write in this 16-row fixture can reach the
/// size trigger either. With no trigger reachable, the load cannot finish
/// and -- the part that would be false under the old semantics -- the
/// store holds zero data objects while it is suspended. The store is read
/// with the load future still alive and pinned, so no shutdown flush from
/// dropping the router can race the observation.
///
/// The suspension is observed off the pipeline's own progress, not off a
/// wall-clock window and not off a yield count: the decoder's
/// `on_batch_queued` hook gates on every batch having been queued (that
/// hook runs on the `spawn_blocking` decode thread, so no filesystem read
/// of the fixture can still be outstanding), and
/// [`yield_until_router_is_quiet`] then waits for `clock.reads()` to
/// settle (so no routed write can still be moving toward its shard
/// buffer). A yield count would bound neither, since yields on this task
/// do not wait on the blocking decode thread.
///
/// Prove-the-test: pass `1` as `target_bytes` to the
/// `build_ingest_config` call in `load_instrumented` (that function has
/// other callers, so change the call, not the function) and every write
/// triggers its own flush, so the
/// acks are answered, the load runs to completion while the driver is
/// still waiting for the router to go quiet, and the `biased` select hits
/// the `panic!` arm ("the load must not complete...").
#[tokio::test]
async fn a_strict_ack_above_target_one_waits_for_a_later_batchs_flush() {
    use ravel_object_store::memory::MemoryStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let shards = 4u32;
    let rows_per_group = 4usize;
    // The fixture holds one row group of `rows_per_group` rows per shard and
    // `--batch-rows` is `rows_per_group`, so the decoder queues exactly
    // `shards` batches of `rows_per_group` rows each.
    let batches = shards as usize;
    let (_dir, pq, m) = sorted_by_shard_fixture(shards, rows_per_group);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(NOW_NS);

    let queued = Arc::new(AtomicUsize::new(0));
    let (gate_tx, mut gate_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let on_batch_queued: BuildStartHook = Arc::new(move || {
        if queued.fetch_add(1, Ordering::SeqCst) + 1 == batches {
            let _ = gate_tx.send(());
        }
    });

    let load_fut = load_instrumented(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        shards,
        rows_per_group,
        0,
        Some(shards as usize),
        rows_per_group,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        8 * 1024 * 1024,
        None,
        NOW_NS,
        Arc::clone(&clock) as Arc<dyn Clock>,
        LoadPath::Columnar,
        None,
        Some(on_batch_queued),
    );
    tokio::pin!(load_fut);

    // Wait for the pipeline to run out of work, then confirm the load is
    // still parked and nothing was made durable. `biased` polls the load
    // first on every round, so a load that does complete trips the panic
    // rather than losing the race to the driver.
    let stored = tokio::select! {
        biased;
        _ = &mut load_fut => panic!(
            "the load must not complete: at an 8 MiB target no write reaches the size \
                 trigger, and a TestClock this test never advances cannot fire the age \
                 trigger, so no ack can be answered"
        ),
        () = async {
            let () = gate_rx.recv().await.expect("every batch is queued");
            yield_until_router_is_quiet(&clock).await;
        } => {
            list_data_objects(store.as_ref()).await
        }
    };
    assert_eq!(
        stored.len(),
        0,
        "no flush has been triggered, so nothing is durable yet: {stored:?}"
    );
}

/// `--target-bytes 0` is rejected rather than silently behaving as `1`
/// (`est_bytes >= 0` holds for an empty buffer, so `0` is not a smaller
/// target than `1`), matching the other operator-facing lever guards.
///
/// Prove-the-test: delete the `target_bytes == 0` guard in
/// `load_instrumented` and the load runs to completion instead, failing
/// `expect_err`.
#[tokio::test]
async fn target_bytes_of_zero_is_rejected() {
    use ravel_object_store::memory::MemoryStore;

    let (_dir, pq, m) = sorted_by_shard_fixture(4, 4);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let err = load_instrumented(
        store,
        &pq,
        "acme",
        &m,
        4,
        4,
        0,
        Some(4),
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        0,
        None,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
        LoadPath::Columnar,
        None,
        None,
    )
    .await
    .expect_err("target_bytes of 0 is rejected");
    assert!(
        matches!(err, LoadError::Setup(_)),
        "a typed setup error, got: {err}"
    );
    assert!(
        err.to_string()
            .contains("--target-bytes must be at least 1"),
        "the error names the lever: {err}"
    );
}

/// One side of the [`max_flush_delay_decides_whether_two_writes_coalesce`]
/// pair. Every argument of the load is fixed here, so the only thing the
/// two calls differ in is `max_flush_delay`.
///
/// Pacing comes only from the injected [`TestClock`], modelled on
/// [`load_with_released_tail`]: the decoder is held on a two-phase gate
/// (`on_batch_queued` reports each batch's ordinal on `gate_tx`, then
/// blocks on `release_rx.recv()`) so batch 1's write reaches the shard
/// buffer alone, the driver advances the clock by a single `ADVANCE_NS`
/// (5s) once the router has gone quiet, then releases the decoder for
/// batch 2. Quiescence is read off [`TestClock::reads`] via
/// [`yield_until_router_is_quiet`]; there is no wall-clock sleep, poll, or
/// timeout anywhere in this path.
///
/// 5s clears `SHORT`'s 1s delay (so the first buffer always ages out on
/// that side) while staying far under `LONG`'s 3600s delay, under
/// `max_flush_lifetime`'s 3600s default, and unreachable by the real-time
/// 60s Strict ack deadline, so exactly one deadline can come due on the
/// jump.
///
/// The bounded wait for a published object before the second quiesce
/// proves different things on each side, which is why both sides share
/// this one helper instead of diverging: on `LONG` the only object that
/// can exist there is write 2's size flush, so the wait pins that write 2
/// reached the shard buffer before `Done` and the end-of-input
/// `flush_all` could split it. On `SHORT` the aged object from the
/// advance already satisfies it; a `SHORT`-side write 2 published as a
/// straggler and one published as an ordered tail both count as the same
/// `final_drain` flush, so that side's expected layout does not depend on
/// the ordering.
async fn load_two_writes_across_one_clock_advance(
    store: Arc<dyn ObjectStoreBackend>,
    pq: &Path,
    m: &Mapping,
    max_flush_delay: Duration,
) -> LoadReport {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // TARGET between one 4.1 KB slice and two, with margin on both sides,
    // so one write never reaches it and two always do.
    const TARGET: usize = 6_000;
    const ADVANCE_NS: i64 = 5 * 1_000_000_000;

    let clock = TestClock::new(NOW_NS);
    let probe: Arc<dyn ObjectStoreBackend> = Arc::clone(&store);

    let ordinal = Arc::new(AtomicUsize::new(0));
    let (gate_tx, mut gate_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let on_batch_queued: BuildStartHook = Arc::new(move || {
        let n = ordinal.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = gate_tx.send(n);
        let guard = release_rx
            .lock()
            .expect("the release channel is not poisoned");
        let _ = guard.recv();
    });

    let load_fut = load_instrumented(
        store,
        pq,
        "acme",
        m,
        1, // one shard: both writes land in the same buffer
        1, // one row per batch
        0,
        Some(1), // one read cursor: batches are strictly sequential
        4,       // pipeline depth above the write count: no mid-loop ack wait
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        1, // one queued batch: the loader parks on `recv` between writes
        TARGET,
        Some(max_flush_delay),
        NOW_NS,
        Arc::clone(&clock) as Arc<dyn Clock>,
        LoadPath::Columnar,
        None,
        Some(on_batch_queued),
    );

    let driver = async {
        assert_eq!(
            gate_rx.recv().await,
            Some(1),
            "the first batch reaches the decoder before anything else runs"
        );
        yield_until_router_is_quiet(&clock).await;
        assert_eq!(
            list_data_objects(probe.as_ref()).await.len(),
            0,
            "one write alone is under TARGET and the clock has not moved: nothing can \
                 have flushed yet"
        );
        clock.advance_ns(ADVANCE_NS);
        yield_until_router_is_quiet(&clock).await;
        release_tx
            .send(())
            .expect("the hook is still waiting on its first release");
        assert_eq!(
            gate_rx.recv().await,
            Some(2),
            "the second batch reaches the decoder once released"
        );

        const MAX_SETTLE_ROUNDS: usize = 10_000;
        let mut rounds = 0;
        while list_data_objects(probe.as_ref()).await.is_empty() {
            rounds += 1;
            assert!(
                rounds < MAX_SETTLE_ROUNDS,
                "issue #1235: settle loop exceeded {MAX_SETTLE_ROUNDS} rounds \
                     without a published object"
            );
            tokio::task::yield_now().await;
        }
        yield_until_router_is_quiet(&clock).await;
        drop(release_tx);
        std::future::pending::<()>().await
    };

    let report = tokio::select! {
        report = load_fut => report.expect("the load completes"),
        () = driver => unreachable!("the driver parks once the decoder is released"),
    };

    assert_eq!(
        clock.now_ns(),
        NOW_NS + ADVANCE_NS,
        "the driver advances the injected clock exactly once, by exactly ADVANCE_NS"
    );

    report
}

/// The `--max-flush-delay` lever decides an object layout, not just a config
/// field (issue #801, deliverable 3). Both sides run
/// [`load_two_writes_across_one_clock_advance`]: same two rows, same
/// `--target-bytes`, same `--pipeline-depth`, same injected clock advanced
/// by the same pattern, only the delay flipped.
///
/// - `LONG` (1h) outlasts the single 5s advance the driver ever makes, so
///   nothing can age out. The second write merges into the first's buffer,
///   pushes it past the target and flushes both as ONE object by size.
/// - `SHORT` (1s) is shorter than the single 5s advance, so the first
///   write's buffer ages out while the decoder is gated: one object by age.
///   The second write then lands in a fresh buffer with the clock already
///   stopped, so nothing can age it either, and the loader's end-of-input
///   flush publishes it. TWO objects, exactly one of them aged.
///
/// The counts come from the real router's issue #983 trigger mix, so each
/// side pins which trigger opened each object, not just how many there were.
///
/// Prove-the-test, flipping only the delay: passing `SHORT` to the coalesced
/// side fails its object count at `left: 2, right: 1` (the first write ages
/// out during the gate instead of waiting for the second), and passing
/// `LONG` to the split side fails at `left: 1, right: 2`, with its mix at
/// `size: 1, age: 0, final: 0` where `size: 0, age: 1, final: 1` is
/// required.
///
/// Inside the helper itself: removing its single `clock.advance_ns` call
/// fails the helper's own post-select clock assertion on the first
/// (coalesced) call, at `left: 1700000000000000000, right:
/// 1700000005000000000`, before the split side ever runs; calling it
/// twice fails the same assertion at
/// `left: 1700000010000000000, right: 1700000005000000000`; and lowering
/// the helper's `TARGET` to `3_000` (so write 1 flushes by size alone)
/// fails the pre-advance zero-object assertion at `left: 1, right: 0`.
#[tokio::test]
async fn max_flush_delay_decides_whether_two_writes_coalesce() {
    use ravel_object_store::memory::MemoryStore;

    const LONG: Duration = Duration::from_secs(3600);
    const SHORT: Duration = Duration::from_secs(1);

    let (_dir, pq, _mapping_path, m) = fat_attr_sorted_by_shard_fixture(1, 2, 4000);

    let coalesced_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let coalesced =
        load_two_writes_across_one_clock_advance(Arc::clone(&coalesced_store), &pq, &m, LONG).await;

    assert_eq!(
        coalesced.objects_written(),
        1,
        "a delay longer than the advance keeps the first buffer alive for the second write: \
             one object"
    );
    assert_eq!(
        coalesced.flush_mix_report().totals,
        FlushMixCounts {
            size: 1,
            age: 0,
            final_drain: 0,
        },
        "the single object is a size flush; nothing ages under a delay longer than the advance"
    );

    let split_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let split =
        load_two_writes_across_one_clock_advance(Arc::clone(&split_store), &pq, &m, SHORT).await;

    assert_eq!(
        split.objects_written(),
        2,
        "a delay shorter than the advance ages the first write out before the second arrives: \
             two objects"
    );
    assert_eq!(
        split.flush_mix_report().totals,
        FlushMixCounts {
            size: 0,
            age: 1,
            final_drain: 1,
        },
        "the first object is the aged-out buffer, the second is the tail the end-of-input \
             flush published"
    );

    // Same rows either way, only their object layout differs.
    assert_eq!(coalesced.rows_processed, 2);
    assert_eq!(split.rows_processed, 2);
    assert_eq!(
        decoded_records(coalesced_store.as_ref()).await,
        decoded_records(split_store.as_ref()).await,
        "the same two rows, decoded, regardless of how many objects hold them"
    );
}

/// A drain-time re-flush period no test run reaches: an hour, past the ack
/// deadline ([`write_ack_deadline`]) of the test that uses it.
const UNREACHED_REFLUSH_PERIOD: Duration = Duration::from_secs(3600);

/// A load whose last slice stays under `--target-bytes` completes under a
/// raised `--max-flush-delay`, with that tail published by the loader's
/// end-of-input flush (issue #801). This is the run-burner the flag shipped
/// with: the tail's Strict ack has nothing left to release it by size, so
/// when the force-flush ran only after the in-flight window was drained,
/// the ack waited on the age trigger, blew the deadline, and returned an
/// error from a load whose every object had already landed.
///
/// Geometry: one shard, four 4 KB-payload rows read one per batch, so each
/// write's estimated per-shard slice is about 4.1 KB. `TARGET = 10_000`
/// sits above two slices and below three, so writes 1-3 flush as one object
/// by size and write 4 is left alone under the target. `--pipeline-depth 5`
/// is above the batch count, so the loader never waits on an ack mid-loop.
/// The injected [`TestClock`] is never advanced, which makes the assertion
/// sharp: the age trigger cannot fire at all here, so the tail's object
/// exists only because the manual flush published it.
///
/// Which flush publishes what also depends on scheduling: a write task that
/// has not reached its shard channel when the end-of-input `flush_all` runs
/// lands in a fresh buffer behind it, so without the gate below writes 1-3
/// would never share a buffer and, at the default ticker period, the
/// drain-time re-flush would publish the stragglers (`size: 0,
/// final_drain: 2`). The last batch's `on_batch_queued` hook
/// therefore holds the decoder, so no `Done` reaches the loader, until the
/// router is quiet: every write routed and the size flush finished.
///
/// The drain-time re-flush ticker also publishes with the manual trigger, so
/// its period is pushed to [`UNREACHED_REFLUSH_PERIOD`], past the 120 s ack
/// deadline the 60 s delay scales to ([`write_ack_deadline`]). The tail can
/// therefore be published only by the end-of-input `flush_all`.
///
/// Prove-the-test: move `router.flush_all()` after `drain_inflight`, or
/// delete it, and the drain waits on an ack nothing will answer before the
/// ticker's first tick. The router's ack deadline fires after 120 s, the load
/// returns a flush failure (`LoadError::Flush`, an ack timeout), and the
/// `expect` on the report fails.
#[tokio::test]
async fn a_tail_below_target_is_published_by_the_end_of_input_flush() {
    use ravel_object_store::memory::MemoryStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const TARGET: usize = 10_000;
    const BATCHES: usize = 4;
    let (_dir, pq, _mapping_path, m) = fat_attr_sorted_by_shard_fixture(1, BATCHES, 4000);

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let probe: Arc<dyn ObjectStoreBackend> = Arc::clone(&store);
    let clock = TestClock::new(NOW_NS);

    let queued = Arc::new(AtomicUsize::new(0));
    let (gate_tx, mut gate_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let on_batch_queued: BuildStartHook = Arc::new(move || {
        if queued.fetch_add(1, Ordering::SeqCst) + 1 == BATCHES {
            let _ = gate_tx.send(());
            let guard = release_rx
                .lock()
                .expect("the release channel is not poisoned");
            let _ = guard.recv();
        }
    });

    let load_fut = load_with_drain_reflush_period(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        1,
        1,
        0,
        Some(1),
        5, // pipeline depth above the batch count: no mid-loop ack wait
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        1,
        TARGET,
        Some(Duration::from_secs(60)),
        NOW_NS,
        Arc::clone(&clock) as Arc<dyn Clock>,
        LoadPath::Columnar,
        None,
        Some(on_batch_queued),
        RlogZstdLevel::DEFAULT,
        UNREACHED_REFLUSH_PERIOD,
        None,
    );

    let driver = async {
        let () = gate_rx.recv().await.expect("the last batch is queued");
        yield_until_router_is_quiet(&clock).await;
        assert_eq!(
            list_data_objects(probe.as_ref()).await.len(),
            1,
            "before the end-of-input flush, writes 1-3 are one size-flushed object and \
                 nothing else is published yet"
        );
        // Releasing the decoder lets `Done` through, and with it the
        // end-of-input flush that publishes the tail.
        drop(release_tx);
        std::future::pending::<()>().await
    };

    let report = tokio::select! {
        report = load_fut => report.expect("a raised delay must not strand the tail buffer's ack"),
        () = driver => unreachable!("the driver parks once the decoder is released"),
    };

    assert_eq!(
        clock.now_ns(),
        NOW_NS,
        "test setup: this test never advances the injected clock, so no buffer can \
         age out"
    );

    assert_eq!(report.rows_processed, 4, "every row is durable");
    assert_eq!(
        report.objects_written(),
        2,
        "three slices reach the target as one object, the fourth is the tail"
    );
    assert_eq!(
        report.flush_mix_report().totals,
        FlushMixCounts {
            size: 1,
            age: 0,
            final_drain: 1,
        },
        "the tail is published by the manual end-of-input flush, not by the age trigger"
    );
    assert_eq!(
        decoded_records(store.as_ref()).await.len(),
        4,
        "the two objects hold all four rows"
    );
}

/// Every row is delivered exactly once regardless of `--read-cursors`,
/// including the exhaustion/redistribution path: a 14-row, 4-row-group
/// file with deliberately unequal group sizes (4/3/5/2, no two equal),
/// loaded with `batch_rows=5` under `read-cursors=1` (sequential),
/// `read-cursors=4` (one cursor per row group, all exhaust together),
/// and `read-cursors=3` (partitions of uneven length `[7, 5, 2]` rows,
/// so partitions exhaust at different rounds -- `3` divides neither
/// `batch_rows` (5) nor the row-group count (4)) -- reports exactly 14
/// durable rows every time.
#[tokio::test]
async fn every_read_cursors_setting_delivers_the_exact_row_count() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("uneven_row_groups.parquet");
    let group_sizes = [4usize, 3, 5, 2];
    let total: usize = group_sizes.iter().sum();

    let first = batch(vec![("ts", i64_col(vec![NOW_NS; group_sizes[0]]))]);
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
    writer.write(&first).expect("write row group");
    writer.flush().expect("flush row group");
    for &size in &group_sizes[1..] {
        let rg = batch(vec![("ts", i64_col(vec![NOW_NS; size]))]);
        writer.write(&rg).expect("write row group");
        writer.flush().expect("flush row group");
    }
    writer.close().expect("close writer");

    let m = base_mapping();
    for read_cursors in [Some(1), Some(4), Some(3)] {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report = load(
            store,
            &pq,
            "acme",
            &m,
            4,
            5,
            read_cursors,
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect("load succeeds");
        assert_eq!(
            report.rows_processed, total as u64,
            "read_cursors={read_cursors:?}: every row must be loaded exactly once"
        );
    }
}

/// The early shard-skew warning (issue #560) fires exactly once when the
/// observed spread stays at or below `shards / SKEW_WARN_DENOMINATOR`
/// through the `SKEW_CHECK_AFTER_BATCHES`-batch checkpoint, and stays
/// silent whenever stride reading (or an already-interleaved input)
/// keeps the spread above it.
///
/// Non-vacuity (prove-the-test): change `distinct_shards as u32 >
/// threshold` to `>=` in `shard_skew_warning` and the first case below
/// (distinct=1, threshold=1, an exact-boundary case) stops warning:
/// `1 >= 1` incorrectly early-returns `None`.
#[tokio::test]
async fn early_skew_warning_fires_once_and_only_when_the_spread_stays_narrow() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;

    let shards = 4u32;
    let group_len = 20usize;
    let batch_rows = 2usize;
    let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();

    let mapping_dir = tempfile::tempdir().expect("tempdir");
    let mapping_path = mapping_dir.path().join("mapping.toml");
    std::fs::write(
        &mapping_path,
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
    )
    .expect("write mapping");

    // (a)/(b): a sorted file -- 4 row groups, one per shard's host value,
    // `group_len` rows each.
    let dir = tempfile::tempdir().expect("tempdir");
    let sorted_pq = dir.path().join("sorted.parquet");
    let first = batch(vec![
        ("ts", i64_col(vec![NOW_NS; group_len])),
        ("svc", str_col(vec!["api"; group_len])),
        ("host", str_col(vec![hosts[0].as_str(); group_len])),
    ]);
    let file = std::fs::File::create(&sorted_pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
    writer.write(&first).expect("write row group");
    writer.flush().expect("flush row group");
    for host in &hosts[1..] {
        let rg = batch(vec![
            ("ts", i64_col(vec![NOW_NS; group_len])),
            ("svc", str_col(vec!["api"; group_len])),
            ("host", str_col(vec![host.as_str(); group_len])),
        ]);
        writer.write(&rg).expect("write row group");
        writer.flush().expect("flush row group");
    }
    writer.close().expect("close writer");

    // (a) sorted + read-cursors=1: the first `SKEW_CHECK_AFTER_BATCHES`
    // batches (16 rows) stay inside row group 0's single host/shard, so
    // the warning fires -- exactly once.
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut sink = Vec::new();
    run_warning_to(
        store,
        &sorted_pq,
        "acme",
        &mapping_path,
        SignalArg::Logs,
        shards,
        batch_rows,
        0,
        Some(1),
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        RlogZstdLevel::DEFAULT,
        None,
        NOW_NS,
        &mut sink,
    )
    .await
    .expect("load succeeds");
    let emitted = String::from_utf8(sink).expect("utf8");
    assert_eq!(
        emitted.matches("shard spread is at or below").count(),
        1,
        "sorted input read sequentially must warn exactly once: {emitted}"
    );

    // (b) sorted + read-cursors=4: one stride cursor per row group mixes
    // all 4 shards into the first couple of batches, so the spread never
    // narrows and the warning stays silent.
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut sink = Vec::new();
    run_warning_to(
        store,
        &sorted_pq,
        "acme",
        &mapping_path,
        SignalArg::Logs,
        shards,
        batch_rows,
        0,
        Some(4),
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        RlogZstdLevel::DEFAULT,
        None,
        NOW_NS,
        &mut sink,
    )
    .await
    .expect("load succeeds");
    let emitted = String::from_utf8(sink).expect("utf8");
    assert!(
        !emitted.contains("shard spread is at or below"),
        "stride reading the same sorted input must not warn: {emitted}"
    );

    // (c) an already-interleaved input, read-cursors=1: even a
    // sequential reader sees all 4 shards from row 0, so the warning
    // stays silent.
    let interleaved_pq = dir.path().join("interleaved.parquet");
    let n_rows = 32usize;
    let host_seq: Vec<&str> = (0..n_rows)
        .map(|i| hosts[i % shards as usize].as_str())
        .collect();
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS; n_rows])),
        ("svc", str_col(vec!["api"; n_rows])),
        ("host", str_col(host_seq)),
    ]);
    let file = std::fs::File::create(&interleaved_pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
    writer.write(&b).expect("write batch");
    writer.close().expect("close writer");

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut sink = Vec::new();
    run_warning_to(
        store,
        &interleaved_pq,
        "acme",
        &mapping_path,
        SignalArg::Logs,
        shards,
        batch_rows,
        0,
        Some(1),
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        RlogZstdLevel::DEFAULT,
        None,
        NOW_NS,
        &mut sink,
    )
    .await
    .expect("load succeeds");
    let emitted = String::from_utf8(sink).expect("utf8");
    assert!(
        !emitted.contains("shard spread is at or below"),
        "an already-interleaved input must not warn: {emitted}"
    );
}

/// Reachability (the point of ADR-0109): the real `load` entry point drives
/// the columnar path, not merely a builder that compiles. A load of a
/// multi-batch file reports `columnar_batches_built > 0`, and the row
/// differential path over the same file reports 0 -- an observation the row
/// path cannot satisfy, evaluated at the point of reliance (each batch that
/// was handed to `write_columnar`).
///
/// Prove-the-test: change `load`'s `LoadPath::Columnar` argument to
/// `LoadPath::Row`, and this fails at `columnar_batches_built > 0` (it reads
/// 0). Confirmed by performing the flip.
#[tokio::test]
async fn load_drives_the_columnar_path_end_to_end() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;

    let n_rows = 6usize;
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("reach.parquet");
    let b = batch(vec![
        ("ts", i64_col(vec![NOW_NS; n_rows])),
        ("svc", str_col(vec!["api"; n_rows])),
    ]);
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
    writer.write(&b).expect("write batch");
    writer.close().expect("close writer");

    let m = parse_mapping(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n",
    )
    .expect("valid mapping");

    // One shard, batch_rows=2 over 6 rows: three columnar batches, three
    // write_columnar calls.
    let store_col: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report_col = load(
        Arc::clone(&store_col),
        &pq,
        "acme",
        &m,
        1,
        2,
        None,
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect("columnar load succeeds");
    assert_eq!(report_col.rows_processed, n_rows as u64);
    assert!(
        report_col.columnar_batches_built > 0,
        "the real load entry point must drive the columnar path"
    );
    assert_eq!(
        report_col.columnar_batches_built, 3,
        "each of the three batches was built and driven through write_columnar"
    );

    // The row differential path over the same file never touches the
    // columnar builder, so the counter stays 0 -- proof the signal is
    // specific to the columnar path and not incremented incidentally.
    let store_row: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report_row = load_row(
        Arc::clone(&store_row),
        &pq,
        "acme",
        &m,
        1,
        2,
        None,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect("row load succeeds");
    assert_eq!(report_row.rows_processed, n_rows as u64);
    assert_eq!(
        report_row.columnar_batches_built, 0,
        "the row path builds no columnar batch"
    );
}

/// A store wrapper that instruments data-object (`/l0/`) PUTs for the
/// `--pipeline-depth` tests: it can sleep a per-key-substring delay before a
/// PUT, track the maximum number of data-object PUTs concurrently in flight,
/// and count how many PUTs matching a watch prefix started versus completed.
/// Non-data PUTs (provisioning record, commit records) pass straight
/// through. Every other method delegates unchanged.
struct InstrumentedPutStore {
    inner: Arc<dyn ObjectStoreBackend>,
    /// `(key substring, delay)`; the first matching entry's delay is applied
    /// before the PUT reaches `inner`.
    delays: Vec<(&'static str, Duration)>,
    /// Data-object PUTs currently sleeping-or-in-`inner`, and the running max.
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    /// PUTs whose key contains any of these are counted as started (before
    /// the delay) and, separately, completed (only on a successful `inner`
    /// PUT). A `.abort()`ed task never reaches the completion increment.
    watch_prefixes: Vec<&'static str>,
    watch_started: Arc<std::sync::atomic::AtomicUsize>,
    watch_completed: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for InstrumentedPutStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: ravel_object_store::PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        use std::sync::atomic::Ordering::SeqCst;
        let is_data_object = key.contains("/l0/");
        let watched = self.watch_prefixes.iter().any(|p| key.contains(p));
        let delay = self
            .delays
            .iter()
            .find(|(p, _)| key.contains(p))
            .map(|(_, d)| *d);
        if is_data_object {
            let now = self.in_flight.fetch_add(1, SeqCst) + 1;
            self.max_in_flight.fetch_max(now, SeqCst);
        }
        if watched {
            self.watch_started.fetch_add(1, SeqCst);
        }
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let result = self.inner.put(key, data, opts).await;
        if is_data_object {
            self.in_flight.fetch_sub(1, SeqCst);
        }
        if watched && result.is_ok() {
            self.watch_completed.fetch_add(1, SeqCst);
        }
        result
    }

    async fn get(
        &self,
        key: &str,
        range: ravel_object_store::GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, ravel_object_store::StoreError>
    {
        self.inner.put_multipart(key).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        page: Option<ravel_object_store::PageToken>,
    ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(
        &self,
        prefix: &str,
    ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> ravel_object_store::Capabilities {
        self.inner.capabilities()
    }
}

/// `--pipeline-depth` is load-bearing, not accepted-and-ignored: the same
/// stream run at depth 1 keeps at most one write outstanding, while at depth
/// 3 up to three writes are genuinely in flight at once. Each batch is a
/// single row routed to its own shard (so its flush runs on its own shard
/// actor, and distinct batches' data-object PUTs can overlap); every
/// data-object PUT sleeps 50ms while the store tracks the running maximum of
/// concurrently outstanding data-object PUTs. Four rows over four shards
/// split into four single-shard batches, submitted in turn.
///
/// Non-vacuity (prove-the-test): the depth-1 arm asserts the observed max is
/// exactly 1 and the depth-3 arm asserts it reaches 3. An implementation
/// that accepted `--pipeline-depth` but still awaited each write inline
/// before starting the next (today's behavior) would leave the max at 1 for
/// both depths, failing the depth-3 assertion; confirmed by running the same
/// body at both depths — `max_d1` is 1 and `max_d3` reaches 3 only because
/// the write window is real.
#[tokio::test]
async fn pipeline_depth_bounds_concurrent_writes() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn max_concurrent_at_depth(depth: usize) -> usize {
        let shards = 4u32;
        // Each batch is one row on its own shard, so its flush runs on a
        // distinct shard actor and can overlap the others.
        let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("depth.parquet");
        let cols: Vec<(String, ArrayRef)> = vec![
            ("ts".to_string(), i64_col(vec![NOW_NS; shards as usize])),
            ("svc".to_string(), str_col(vec!["api"; shards as usize])),
            (
                "host".to_string(),
                str_col(hosts.iter().map(|h| h.as_str()).collect()),
            ),
        ];
        let b = RecordBatch::try_from_iter(cols).expect("record batch");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");

        let m = parse_mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
                 [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
        )
        .expect("valid mapping");

        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(InstrumentedPutStore {
            inner: Arc::new(MemoryStore::new()),
            delays: vec![("/l0/", Duration::from_millis(50))],
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: Arc::clone(&max_in_flight),
            watch_prefixes: Vec::new(),
            watch_started: Arc::new(AtomicUsize::new(0)),
            watch_completed: Arc::new(AtomicUsize::new(0)),
        });

        let report = load(
            store as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &m,
            shards,
            1,
            None,
            depth,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect("the load succeeds");
        assert_eq!(report.rows_processed, shards as u64, "every row is written");
        max_in_flight.load(Ordering::SeqCst)
    }

    let max_d1 = max_concurrent_at_depth(1).await;
    assert_eq!(
        max_d1, 1,
        "at --pipeline-depth 1 exactly one write is ever outstanding (today's behavior), \
             got {max_d1}"
    );

    let max_d3 = max_concurrent_at_depth(3).await;
    assert!(
        max_d3 >= 3,
        "at --pipeline-depth 3 the write window reaches three concurrently outstanding PUTs, \
             got {max_d3}"
    );
}

/// `--max-inflight-flushes` is load-bearing, not accepted-and-ignored, and
/// it is a genuinely different window from `--pipeline-depth`: it bounds the
/// flushes ONE SHARD may run at once, where `--pipeline-depth` bounds the
/// writes the loader keeps outstanding across all shards. Every batch here
/// lands on the same shard (`--shards 1`), so the shard's flush semaphore is
/// the only thing that can serialize them, and `--pipeline-depth 4` is held
/// above every flush setting under test so the loader's own window is never
/// the binding constraint.
///
/// Four rows, one row per batch, each data-object PUT held 50ms while the
/// store tracks the running maximum of concurrently outstanding data-object
/// PUTs. Both arms assert an exact figure, not a floor: the semaphore is a
/// hard ceiling (a shard may not exceed its permit count) and, with four
/// batches queued behind a four-deep loader window, it is also reached.
///
/// Non-vacuity (prove-the-test): confirmed failing against the pre-change
/// code by deleting the `max_inflight_flushes,` field from the
/// `IngestConfig` literal in `load_instrumented`, which is exactly the state
/// before this ticket -- the flag parsed and threaded but never reaching the
/// router. The window then falls back to `IngestConfig::default()`'s 1 and
/// the `flushes = 3` arm observes a high-water mark of 1 instead of 3. The
/// `flushes = 1` arm is the control: it reads 1 either way, so on its own it
/// proves nothing, which is why the higher setting is asserted exactly.
#[tokio::test]
async fn max_inflight_flushes_bounds_concurrent_flushes_per_shard() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Rows, and therefore single-row batches, in the fixture. One more
    /// than the highest flush window under test, so the window (not the
    /// supply of batches) is what the high-water mark measures.
    const BATCHES: usize = 4;
    /// Held above every flush setting under test: the loader's own window
    /// must never be the binding constraint on this fixture.
    const PIPELINE_DEPTH: usize = 4;

    async fn max_concurrent_at_flush_window(flushes: u32) -> usize {
        // One shard, so every batch's flush contends for the same shard
        // actor's semaphore. Distinct hosts keep the objects distinct
        // without changing routing.
        let shards = 1u32;
        let hosts: Vec<String> = (0..BATCHES).map(|i| format!("h{i}")).collect();

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("flush_window.parquet");
        let cols: Vec<(String, ArrayRef)> = vec![
            ("ts".to_string(), i64_col(vec![NOW_NS; BATCHES])),
            ("svc".to_string(), str_col(vec!["api"; BATCHES])),
            (
                "host".to_string(),
                str_col(hosts.iter().map(|h| h.as_str()).collect()),
            ),
        ];
        let b = RecordBatch::try_from_iter(cols).expect("record batch");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");

        let m = parse_mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
                 [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
        )
        .expect("valid mapping");

        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(InstrumentedPutStore {
            inner: Arc::new(MemoryStore::new()),
            delays: vec![("/l0/", Duration::from_millis(50))],
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: Arc::clone(&max_in_flight),
            watch_prefixes: Vec::new(),
            watch_started: Arc::new(AtomicUsize::new(0)),
            watch_completed: Arc::new(AtomicUsize::new(0)),
        });

        let report = load_instrumented(
            store as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &m,
            shards,
            1,
            0,
            None,
            PIPELINE_DEPTH,
            flushes,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            None,
            None,
        )
        .await
        .expect("the load succeeds");
        assert_eq!(
            report.rows_processed, BATCHES as u64,
            "every row is written"
        );
        max_in_flight.load(Ordering::SeqCst)
    }

    let max_w1 = max_concurrent_at_flush_window(1).await;
    assert_eq!(
        max_w1, 1,
        "at --max-inflight-flushes 1 the shard runs exactly one flush at a time even with \
             --pipeline-depth {PIPELINE_DEPTH} handing it {BATCHES} writes, got {max_w1}"
    );

    let max_w3 = max_concurrent_at_flush_window(3).await;
    assert_eq!(
        max_w3, 3,
        "at --max-inflight-flushes 3 the shard runs exactly three flushes at once: the \
             semaphore is the ceiling and {BATCHES} queued batches reach it, got {max_w3}"
    );
}

/// The loader-side counterpart of `ravel-ingest`'s
/// `neither_write_window_alone_moves_the_wall_and_the_counters_say_which`,
/// measured end to end through `load_instrumented` with a real per-PUT
/// latency injected (issue #800). ADR-0807 measured `4`/`4` against `1`/`1`
/// on the ClickBench corpus but never isolated the two windows from each
/// other, so the 2.94x it reports is not apportioned. This runs the full
/// 2x2 on one fixture.
///
/// Sixteen single-row batches, one shard, and a 40ms delay on every
/// data-object PUT, so the whole load is round-trip bound by construction
/// and the arms differ only in the two windows.
///
/// Pre-registered before running (the arithmetic, not a post-hoc fit):
/// `16 * 40ms = 640ms` for every arm that leaves either window at 1, and
/// `4 * 40ms = 160ms` for the arm that raises both, a 4x ratio. Peak
/// concurrent data-object PUTs: exactly 1, 1, 1, and 4.
///
/// Asserted: the peak concurrency exactly, which is a count and cannot
/// drift with machine load; and, on the wall, only the two conclusions the
/// counts alone cannot give -- that raising both windows is at least 2x
/// (against 4x predicted, so a loaded box has 2x of headroom before this
/// misfires) and that neither window alone gets within 30% of that. Both
/// wall bounds are ratios against this run's own `1`/`1` arm, so a uniformly
/// slow machine moves numerator and denominator together.
///
/// Non-vacuity (prove-the-test): the shipped-defaults arm's peak of 4 fails
/// against the pre-change defaults. Confirmed by setting
/// `DEFAULT_PIPELINE_DEPTH` back to 1: that arm reads `left: 1, right: 4`
/// and its wall goes to 665.99ms, indistinguishable from the fully serial
/// arm's 673.05ms in the same run. The `4`/`1` and `1`/`4` arms are what make
/// the claim "both windows, not either" falsifiable: a change that only
/// raised one would still pass a bare `1`/`1`-against-`4`/`4` comparison.
#[tokio::test]
async fn both_write_windows_are_needed_to_overlap_put_round_trips() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Single-row batches, so each is one flush on the single shard.
    const BATCHES: usize = 16;
    /// Injected cost of one data-object PUT.
    const PUT_DELAY: Duration = Duration::from_millis(40);

    /// One arm's outcome: the wall, the peak concurrent object PUTs, and the
    /// submit loop's own wall split into the only two things it can block on.
    struct Arm {
        wall: Duration,
        peak: usize,
        write_wait: Duration,
        decode_wait: Duration,
    }

    async fn arm(depth: usize, flushes: u32) -> Arm {
        let shards = 1u32;
        let hosts: Vec<String> = (0..BATCHES).map(|i| format!("h{i}")).collect();

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("windows.parquet");
        let cols: Vec<(String, ArrayRef)> = vec![
            ("ts".to_string(), i64_col(vec![NOW_NS; BATCHES])),
            ("svc".to_string(), str_col(vec!["api"; BATCHES])),
            (
                "host".to_string(),
                str_col(hosts.iter().map(|h| h.as_str()).collect()),
            ),
        ];
        let b = RecordBatch::try_from_iter(cols).expect("record batch");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");

        let m = parse_mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
                 [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
        )
        .expect("valid mapping");

        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(InstrumentedPutStore {
            inner: Arc::new(MemoryStore::new()),
            delays: vec![("/l0/", PUT_DELAY)],
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: Arc::clone(&max_in_flight),
            watch_prefixes: Vec::new(),
            watch_started: Arc::new(AtomicUsize::new(0)),
            watch_completed: Arc::new(AtomicUsize::new(0)),
        });

        let started = Instant::now(); // allow-wall-clock: measures real wall for the diagnostic printed below, never asserted on; the test's claim is the exact peak-concurrency count, not any elapsed time
        let report = load_instrumented(
            store as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &m,
            shards,
            1,
            0,
            None,
            depth,
            flushes,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            None,
            None,
        )
        .await
        .expect("the load succeeds");
        let wall = started.elapsed(); // allow-wall-clock: real elapsed for the printed diagnostic only; the peak-concurrency assertions below carry the whole claim
        assert_eq!(
            report.rows_processed, BATCHES as u64,
            "every row is written at depth {depth} / flushes {flushes}"
        );
        assert_eq!(
            report.objects_written(),
            BATCHES,
            "object layout is identical across arms: {BATCHES} objects however \
                 the windows are set, so the peak-concurrency comparison is not \
                 confounded by a different number of PUTs"
        );
        // The loop can only block on receiving a decoded batch or on
        // resolving a write, so these two partition its wall.
        //
        // hygiene-allow: wall-clock -- the gap here is manufactured by the
        // fixture's injected per-PUT delay, not by how fast the machine is:
        // 671.3 ms against 0.85 ms, about 790x. A slow or loaded runner
        // moves both sides together and cannot invert it. There is no
        // deterministic restatement of this claim, and the claim is the
        // whole point of the fixture: the submit loop blocks on writes, not
        // on the decoder.
        assert!(
            report.write_wait > report.decode_wait,
            "this fixture is round-trip bound by construction, so the submit \
                 loop must spend more time resolving writes ({:?}) than waiting \
                 for decoded batches ({:?}) at depth {depth} / flushes {flushes}",
            report.write_wait,
            report.decode_wait
        );
        Arm {
            wall,
            peak: max_in_flight.load(Ordering::SeqCst),
            write_wait: report.write_wait,
            decode_wait: report.decode_wait,
        }
    }

    let a_1_1 = arm(1, 1).await;
    let a_4_1 = arm(4, 1).await;
    let a_1_4 = arm(1, 4).await;
    let a_4_4 = arm(4, 4).await;
    let peak_1_1 = a_1_1.peak;
    let peak_4_1 = a_4_1.peak;
    let peak_1_4 = a_1_4.peak;
    let peak_4_4 = a_4_4.peak;

    println!("write-window 2x2 ({BATCHES} batches, {PUT_DELAY:?} per data PUT, 1 shard):");
    for (label, a) in [
        ("depth 1 / flushes 1", &a_1_1),
        ("depth 4 / flushes 1", &a_4_1),
        ("depth 1 / flushes 4", &a_1_4),
        ("depth 4 / flushes 4", &a_4_4),
    ] {
        println!(
            "  {label}: wall {:?} peak {} (submit loop: write_wait {:?}, decode_wait {:?})",
            a.wall, a.peak, a.write_wait, a.decode_wait
        );
    }

    assert_eq!(
        peak_1_1, 1,
        "at depth 1 / flushes 1 exactly one object PUT is ever outstanding"
    );
    assert_eq!(
        peak_4_1, 1,
        "raising only the loader's window still leaves the shard's flush \
             semaphore at one permit, so exactly one PUT is outstanding"
    );
    assert_eq!(
        peak_1_4, 1,
        "raising only the shard's flush window leaves the loader awaiting each \
             batch before submitting the next, so the extra permits are never asked \
             for and exactly one PUT is outstanding"
    );
    assert_eq!(
        peak_4_4, 4,
        "both windows raised: exactly four object PUTs overlap, the loader's \
             four outstanding batches each holding one of the shard's four permits"
    );

    // The walls are printed above, not asserted on. The four peak
    // assertions already carry this test's whole claim: 1, 1, 1 and 4
    // outstanding PUTs is the concurrency the windows are supposed to
    // produce, stated exactly and observed directly. A wall-clock band on
    // top of that adds no proof and does add a failure mode, since the
    // ratio between two elapsed times on a shared runner is not a property
    // of the code. The end-to-end wall figure that justifies the default
    // lives in ADR-0807, measured on a real corpus.

    // The shipped defaults, run through the same fixture: what an operator
    // gets with neither flag given must be the overlapped arm, not the
    // serial one. This is the assertion the default change is accountable
    // to; against the pre-change defaults of 1 and 1 it reads a peak of 1.
    let a_default = arm(DEFAULT_PIPELINE_DEPTH, DEFAULT_MAX_INFLIGHT_FLUSHES).await;
    let (wall_default, peak_default) = (a_default.wall, a_default.peak);
    println!(
        "  shipped defaults:    wall {wall_default:?} peak {peak_default} \
             (submit loop: write_wait {:?}, decode_wait {:?})",
        a_default.write_wait, a_default.decode_wait
    );
    assert_eq!(
        peak_default, 4,
        "the shipped defaults must overlap four object PUTs; a peak of 1 means \
             the loader ships serial"
    );
}

/// Durable-token correctness under a partial-window failure: the reported
/// list equals what actually committed, at `--pipeline-depth` above 1
/// (issue #800). With depth 4 and a stream of five single-shard batches, the
/// data PUT for the *middle* batch (index 2, shard 2) is held ~300ms before
/// failing permanently. The batches strictly after it (indices 3 and 4,
/// shards 3 and 4) have no artificial delay at all, so their shard actors
/// race far ahead of shard 2's held PUT and commit their objects
/// *independently*, well before shard 2's failure is even detected. The
/// loader cannot prevent that: it routes each batch to its own shard actor,
/// and a shard actor's flush is downstream of the write future the loader
/// holds, so aborting that future's `JoinHandle` does not stop an actor
/// already mid-PUT (see `LogIngestRouter::write` in
/// `crates/ravel-ingest/src/log_router.rs`: the actor has no join handle of
/// its own, only a channel).
///
/// Since it cannot stop them, it waits for them ([`harvest_after_failure`]).
/// Asserted: the reported durable list is exactly shards `[0, 1, 3, 4]`, in
/// submission order -- the two batches before the failure, then the two
/// after it that committed anyway -- and carries no token for the failing
/// batch itself (a full single-shard PUT failure has no partial survivor).
/// `watch_completed` reading 2 is the direct, non-inferred proof that
/// batches 3 and 4's objects genuinely landed in the underlying store, so
/// the list equality is a claim about what happened, not about what the
/// loader chose to look at.
///
/// Non-vacuity (prove-the-test): this exact assertion fails against the
/// pre-change code, which aborted the post-failure handles
/// (`for (_, handle) in inflight.drain(..) { handle.abort(); }` at the two
/// error sites). Confirmed by running it against that code: it panicked with
/// `durable.len() == 2`, the two pre-failure shards only, while
/// `watch_completed` still read 2 -- the gap this closes, stated as its own
/// failure. The ordering is deterministic because a zero-delay in-memory PUT
/// completes in microseconds while shard 2 is held 300ms, a 6000x margin
/// that no scheduling jitter inverts; and the harvest pops the window
/// front-to-back, so 3 precedes 4. A resolver that recorded whichever write
/// finished first would report the post-failure shards ahead of the
/// pre-failure ones and fail the exact-sequence assertion.
#[tokio::test]
async fn partial_window_failure_reports_every_batch_that_committed() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let shards = 5;
    // One row per batch (batch_rows = 1), each routed to its own shard, so
    // batch k is the sole write to shard k's `/l0/000k/` object.
    let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("five_batches.parquet");
    let cols: Vec<(String, ArrayRef)> = vec![
        ("ts".to_string(), i64_col(vec![NOW_NS; shards as usize])),
        ("svc".to_string(), str_col(vec!["api"; shards as usize])),
        (
            "host".to_string(),
            str_col(hosts.iter().map(|h| h.as_str()).collect()),
        ),
    ];
    let b = RecordBatch::try_from_iter(cols).expect("five-row batch");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
    writer.write(&b).expect("write batch");
    writer.close().expect("close writer");

    let m = parse_mapping(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
    )
    .expect("valid mapping");

    // Fail the middle batch's (shard 2) data PUT permanently; no retry, no
    // sibling shard, so no partial survivor.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
        )
        .with_key_contains("/l0/0002/")
        .with_occurrence(Occurrence::Always),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));

    let watch_started = Arc::new(AtomicUsize::new(0));
    let watch_completed = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(InstrumentedPutStore {
        inner: fault.clone() as Arc<dyn ObjectStoreBackend>,
        // Shard 2 (the failing batch) is held ~300ms before its permanent
        // fault fires. Shards 3 and 4 (after the failure) get no artificial
        // delay at all, so their zero-delay PUTs commit within microseconds
        // of being spawned -- long before shard 2's held failure surfaces.
        // This is the shape that discriminates FIFO from whichever-finishes-
        // first: delaying the *post-failure* batches instead would stop them
        // finishing early under any resolver and prove nothing (see the test
        // doc comment).
        delays: vec![("/l0/0002/", Duration::from_millis(300))],
        in_flight: Arc::new(AtomicUsize::new(0)),
        max_in_flight: Arc::new(AtomicUsize::new(0)),
        watch_prefixes: vec!["/l0/0003/", "/l0/0004/"],
        watch_started: Arc::clone(&watch_started),
        watch_completed: Arc::clone(&watch_completed),
    });

    let err = load(
        store as Arc<dyn ObjectStoreBackend>,
        &pq,
        "acme",
        &m,
        shards,
        1,
        None,
        4,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect_err("the middle batch's permanent PUT failure fails the load");

    // Shards 3 and 4 have zero artificial delay, so by the time shard 2's
    // ~300ms-held failure surfaces (and `load` returns it) both have already
    // started AND completed their PUT -- a 6000x margin over a zero-delay
    // in-memory write, leaving no scheduling-jitter window that could catch
    // them mid-flight instead. This is the direct, non-inferred proof the
    // spec requires: the objects genuinely landed in the underlying store,
    // not merely "present in the returned list" (which a wrong
    // implementation could also produce, by reporting a token for a batch
    // that never committed).
    let durable = match &err {
        LoadError::Flush { durable, .. } => durable.clone(),
        other => panic!("expected LoadError::Flush, got {other:?}"),
    };
    let shard_sequence: Vec<u32> = durable.iter().map(|t| t.shard).collect();
    assert_eq!(
        shard_sequence,
        vec![0, 1, 3, 4],
        "the durable list is exactly the batches that committed, in submission order: the two \
             before the failure, then the two after it whose independent writes landed anyway. It \
             carries no token for the failing batch (shard 2), which had no partial survivor. Got \
             {durable:?}"
    );

    assert_eq!(
        fault.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the permanent data-object PUT fault fired exactly once (shard 2, no retry)"
    );

    assert_eq!(
        watch_started.load(Ordering::SeqCst),
        2,
        "both after-failure writes (batches 3 and 4) reached their PUT"
    );
    assert_eq!(
        watch_completed.load(Ordering::SeqCst),
        2,
        "batches 3 and 4's writes did in fact succeed and commit. The loader cannot stop a \
             shard actor already mid-PUT, so it waits for the outcome instead of abandoning it, \
             and both appear in the durable list above. That is what makes the report equal to \
             what landed at any --pipeline-depth: a resume from it re-ingests neither rows that \
             committed nor rows that did not."
    );
}

/// `--skip-rows` (issue #1713): a positional resume with no idempotency
/// marker. These tests exercise `load_instrumented` directly (rather than
/// the public [`load`] wrapper, which hardcodes `skip_rows: 0` to avoid
/// changing its own signature) since that is the only entry point that
/// carries the parameter.
mod load_skip_rows {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    /// A 10-row single-row-group fixture with a distinguishing `idx`
    /// attribute (0..n), so a landed record's identity -- not just its
    /// count -- can be checked against the source file.
    fn skip_rows_fixture(n: i64) -> (tempfile::TempDir, std::path::PathBuf, Mapping, RecordBatch) {
        let mut m = base_mapping();
        m.attributes = vec![attr("idx", "idx", ColType::I64)];
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS; n as usize])),
            ("idx", i64_col((0..n).collect())),
        ]);
        let (dir, pq) = write_parquet(&b);
        (dir, pq, m, b)
    }

    /// The same fixture written as `rows / group_rows` row groups, so a load
    /// over it can open one stride cursor per row group. Row identity is the
    /// same `idx` attribute, so the landed set is still checkable against
    /// file-absolute positions.
    fn skip_rows_row_group_fixture(
        rows: i64,
        group_rows: usize,
    ) -> (tempfile::TempDir, std::path::PathBuf, Mapping, RecordBatch) {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let mut m = base_mapping();
        m.attributes = vec![attr("idx", "idx", ColType::I64)];
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS; rows as usize])),
            ("idx", i64_col((0..rows).collect())),
        ]);
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("skip_groups.parquet");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(group_rows))
            .build();
        let mut w = ArrowWriter::try_new(file, b.schema(), Some(props)).expect("arrow writer");
        w.write(&b).expect("write batch");
        w.close().expect("close writer");
        (dir, pq, m, b)
    }

    /// The `idx`-attributed records for `rows` of `full`, in the form
    /// [`decoded_records`] returns, so a landed set can be compared by
    /// identity rather than by count.
    fn expected_records(
        full: &RecordBatch,
        m: &Mapping,
        rows: std::ops::Range<usize>,
    ) -> Vec<String> {
        let mut expected: Vec<String> = row_records(&full.slice(rows.start, rows.len()), m)
            .iter()
            .map(|r| format!("{:?}", to_logrecord(r)))
            .collect();
        expected.sort();
        expected
    }

    async fn run_skip_rows(
        store: Arc<dyn ObjectStoreBackend>,
        pq: &Path,
        m: &Mapping,
        skip_rows: u64,
    ) -> LoadReport {
        run_skip_rows_with(store, pq, m, skip_rows, 10, Some(1)).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_skip_rows_with(
        store: Arc<dyn ObjectStoreBackend>,
        pq: &Path,
        m: &Mapping,
        skip_rows: u64,
        batch_rows: usize,
        read_cursors: Option<usize>,
    ) -> LoadReport {
        load_instrumented(
            store,
            pq,
            "acme",
            m,
            1,
            batch_rows,
            skip_rows,
            read_cursors,
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            None,
            None,
        )
        .await
        .expect("load succeeds")
    }

    /// A 10-row file loaded with `skip_rows=7` lands exactly the 3 rows at
    /// file-absolute positions 7, 8, 9 -- verified by reading the RLOG
    /// objects the router actually wrote back (via [`decoded_records`]),
    /// not merely by a row count, against an independently computed
    /// differential reference ([`row_records`] over the same rows sliced
    /// straight out of the source batch).
    ///
    /// Non-vacuity (prove-the-test): change the `cut` computation in
    /// `collect_spans` from `state.skip_rows - *file_base` to
    /// `state.skip_rows - *file_base + 1` (an off-by-one that slices one
    /// row too few off the straddling batch) and this test's exact
    /// `rows_processed == 3` assertion fails: `left: 2, right: 3`, since
    /// row 7 (file-absolute) is wrongly dropped along with 0..6.
    #[tokio::test]
    async fn skip_rows_lands_exactly_the_rows_after_the_offset() {
        let (_dir, pq, m, full) = skip_rows_fixture(10);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

        let report = run_skip_rows(Arc::clone(&store), &pq, &m, 7).await;

        assert_eq!(
            report.rows_skipped, 7,
            "skip_rows=7 against a 10-row file must report exactly 7 skipped"
        );
        assert_eq!(
            report.rows_processed, 3,
            "skip_rows=7 against a 10-row file must land exactly 3 rows"
        );

        assert_eq!(
            decoded_records(store.as_ref()).await,
            expected_records(&full, &m, 7..10),
            "the landed records must be exactly file rows 7..10, not merely 3 of them"
        );
    }

    /// A `skip_rows` at or beyond the file's total row count lands 0
    /// records and the load still exits `Ok` -- there is no error case
    /// for "skip past the end", since a positional resume of an
    /// already-fully-loaded file is exactly this shape.
    #[tokio::test]
    async fn skip_rows_beyond_the_file_lands_nothing_and_succeeds() {
        let (_dir, pq, m, _full) = skip_rows_fixture(10);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

        let report = run_skip_rows(Arc::clone(&store), &pq, &m, 1_000).await;

        assert_eq!(
            report.rows_skipped, 10,
            "rows_skipped caps at the file's total row count (10), not the requested 1000"
        );
        assert_eq!(
            report.rows_processed, 0,
            "no row survives a skip past the file's end"
        );
        assert!(
            decoded_records(store.as_ref()).await.is_empty(),
            "no RLOG object holds any record when every row is skipped"
        );

        // The clamped figure above cannot show that the REQUEST missed the
        // file, so the report carries the requested value beside the
        // file's own total and the warning is built from those two.
        assert_eq!(
            (report.skip_rows_requested, report.file_total_rows),
            (1_000, 10),
            "the unclamped request and the file's row count are both carried"
        );
        let warning =
            skip_rows_past_end_warning(report.skip_rows_requested, report.file_total_rows)
                .expect("a request past the end of the file must warn");
        assert!(
            warning.contains("--skip-rows 1000") && warning.contains("10 rows"),
            "the warning names the requested offset and the file's row count: {warning}"
        );
    }

    /// `--skip-rows` EQUAL to the file's row count is the legitimate resume
    /// of an already-complete file, so it stays silent while a strictly
    /// larger value warns. Without this case the warning could be written
    /// as `>=` and nothing would fail, which would make every completed
    /// resume print an error-shaped line.
    ///
    /// Non-vacuity (prove-the-test): changing the predicate in
    /// `skip_rows_past_end_warning` from `>` to `>=` fails this test on the
    /// `is_none` assertion while leaving the case above passing.
    #[tokio::test]
    async fn skip_rows_exactly_at_the_end_lands_nothing_and_stays_silent() {
        let (_dir, pq, m, _full) = skip_rows_fixture(10);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

        let report = run_skip_rows(Arc::clone(&store), &pq, &m, 10).await;

        assert_eq!(
            (report.rows_skipped, report.rows_processed),
            (10, 0),
            "a skip of exactly the row count drops every row and writes none"
        );
        assert!(
            skip_rows_past_end_warning(report.skip_rows_requested, report.file_total_rows)
                .is_none(),
            "a completed resume is not an operator error and must not warn"
        );
    }

    /// The drop `--skip-rows` performs is FILE-absolute, not per-cursor:
    /// over a 12-row file in 4 row groups read by 4 stride cursors,
    /// `skip_rows=5` lands exactly file rows 5..12 -- the same set a single
    /// sequential cursor would land -- even though the cursor whose
    /// partition straddles the offset (rows 3..6) is not the one the offset
    /// counted through. The row-group count is asserted first: with one row
    /// group `resolve_read_cursors` clamps the requested 4 cursors to 1 and
    /// the test would prove nothing about cursor count.
    ///
    /// This is the flag's own positional guarantee. It is NOT the resume
    /// guarantee: what a FAILED multi-cursor run left behind is a different
    /// question, pinned by
    /// `a_failed_load_prints_the_resume_figures_and_the_settings_precondition`.
    ///
    /// Non-vacuity (prove-the-test), both demonstrated failing:
    /// drop the `*file_base +` term from `collect_spans`'s `end` (the skip
    /// read per-span instead of file-absolute, which is what a per-cursor
    /// offset would be) and every one-row span is dropped: `and must land
    /// exactly the remaining 7 rows ... left: 0, right: 7`. Asserting
    /// `4..11` instead of `5..12` keeps the count at 7 and still fails, on
    /// `idx` 4 against 11, so the landed-set assertion is pinned by row
    /// identity and not by how many rows arrived.
    #[tokio::test]
    async fn skip_rows_is_file_absolute_across_multiple_cursors_and_row_groups() {
        let (_dir, pq, m, full) = skip_rows_row_group_fixture(12, 3);
        let metadata = read_input_metadata(&FileInput { path: &pq }).expect("read metadata");
        let groups = row_group_row_counts(&metadata).len();
        assert_eq!(
            groups, 4,
            "the fixture must hold 4 row groups, or the 4 requested cursors clamp to the row \
                 group count and the load is single-cursor after all"
        );
        assert_eq!(
            resolve_read_cursors(Some(4), 1, groups),
            4,
            "and the load must therefore open 4 stride cursors, one per row group"
        );

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        // 4 rows per batch over 4 live cursors: each batch takes one row
        // from each partition, so every batch spans the whole file and the
        // offset is crossed by a cursor that is not reading from row 0.
        let report = run_skip_rows_with(Arc::clone(&store), &pq, &m, 5, 4, Some(4)).await;

        assert_eq!(
            report.rows_skipped, 5,
            "skip_rows=5 against a 12-row file must report exactly 5 skipped, whatever the \
                 cursor count"
        );
        assert_eq!(
            report.rows_processed, 7,
            "and must land exactly the remaining 7 rows"
        );
        assert_eq!(
            decoded_records(store.as_ref()).await,
            expected_records(&full, &m, 5..12),
            "the landed records must be exactly file rows 5..12: the cursor holding rows 3..6 \
                 keeps only row 5, and the cursors at rows 6..12 keep everything"
        );
    }

    /// The past-end warning reaches the operator, not just the function
    /// that builds it. The two cases above call
    /// `skip_rows_past_end_warning` directly, so deleting the `if let`
    /// that writes it in `run_warning_to` leaves them green while the
    /// clamped summary silently returns to reporting success. This file
    /// already states that rule at the admission-bypass warning: with the
    /// write inlined there, deleting the emit left every test green.
    ///
    /// Non-vacuity (prove-the-test), demonstrated failing: deleting the
    /// `skip_rows_past_end_warning` emit block in `run_warning_to` fails
    /// this test on the first assertion, against a sink carrying only the
    /// admission-bypass warning.
    #[tokio::test]
    async fn a_skip_past_the_end_warns_through_the_cli_entry_point() {
        let (dir, pq, _m, _full) = skip_rows_fixture(6);
        let mapping_path = dir.path().join("mapping.toml");
        std::fs::write(
            &mapping_path,
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[attribute]]\nkey = \"idx\"\ncolumn = \"idx\"\ntype = \"i64\"\n",
        )
        .expect("write mapping");

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut sink: Vec<u8> = Vec::new();
        run_warning_to(
            Arc::clone(&store),
            &pq,
            "acme",
            &mapping_path,
            SignalArg::Logs,
            1,
            2,
            99,
            Some(1),
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
            RlogZstdLevel::DEFAULT,
            None,
            NOW_NS,
            &mut sink,
        )
        .await
        .expect("a skip past the end writes nothing and still succeeds");

        let emitted = String::from_utf8(sink).expect("warnings are utf-8");
        assert!(
            emitted.contains("--skip-rows 99") && emitted.contains("6 rows"),
            "the operator is told the requested offset and the file's row count: {emitted}"
        );
        assert!(
            decoded_records(store.as_ref()).await.is_empty(),
            "and nothing was loaded"
        );
    }

    /// A load that fails mid-file prints the two figures a resume needs
    /// (`rows_skipped` and `rows_written`), their sum as the next
    /// `--skip-rows`, and whether this run's settings make that sum mean
    /// anything -- end to end through [`run_warning_to`], the CLI's own
    /// entry point, so the mapping file, the error path and the output
    /// stream are the operator's.
    ///
    /// The run is `--read-cursors 1 --pipeline-depth 1` so the failure point
    /// is deterministic: batches are submitted and resolved one at a time,
    /// the first batch (file rows 2, 3) commits, and the scripted fault
    /// fails the second batch's data-object PUT. The same error is then
    /// asked for the multi-cursor verdict, which is the case the figures
    /// must NOT be pasted into a resume.
    ///
    /// Non-vacuity (prove-the-test), both demonstrated failing: delete the
    /// `resume_hint` emit block in `run_warning_to` and the first
    /// assertion fails against a stream carrying only the admission-bypass
    /// warning; force `sequential` in `resume_hint` to `true` (one verdict
    /// for every geometry) and the multi-cursor assertion fails against the
    /// prefix verdict.
    #[tokio::test]
    async fn a_failed_load_prints_the_resume_figures_and_the_settings_precondition() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };

        let (dir, pq, _m, _full) = skip_rows_fixture(6);
        let mapping_path = dir.path().join("mapping.toml");
        std::fs::write(
            &mapping_path,
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[attribute]]\nkey = \"idx\"\ncolumn = \"idx\"\ntype = \"i64\"\n",
        )
        .expect("write mapping");

        // The second data-object PUT into shard 0 fails permanently, so the
        // first batch is durable and the second is not.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
            )
            .with_key_contains("/l0/0000/")
            .with_occurrence(Occurrence::Nth(2)),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));

        let mut sink: Vec<u8> = Vec::new();
        let err = run_warning_to(
            store.clone() as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &mapping_path,
            SignalArg::Logs,
            1,
            2,
            2,
            Some(1),
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
            RlogZstdLevel::DEFAULT,
            None,
            NOW_NS,
            &mut sink,
        )
        .await
        .expect_err("the second batch's PUT fails permanently, so the load fails");
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Permanent),
            1,
            "the scripted fault must have fired, or the failure under test never happened"
        );

        let emitted = String::from_utf8(sink).expect("warnings are utf-8");
        assert!(
            emitted.contains("rows_skipped     : 2"),
            "the offset this run started from reaches the operator: {emitted}"
        );
        assert!(
            emitted.contains("rows_written     : 2"),
            "so do the rows it acked durable before failing: {emitted}"
        );
        assert!(
            emitted.contains("next --skip-rows : 4 (rows_skipped + rows_written)"),
            "and the sum, named as the flag it goes into: {emitted}"
        );
        assert!(
            emitted.contains(
                "this run used --read-cursors 1 --pipeline-depth 1, so the rows that landed \
                     are a contiguous prefix of the file"
            ),
            "with the settings precondition stated as met for this run: {emitted}"
        );

        let load_err = err
            .downcast::<LoadError>()
            .expect("the CLI error wraps the typed load error");
        assert_eq!(
            load_err.resume_figures(),
            Some(ResumeFigures {
                rows_skipped: 2,
                rows_written: 2,
            }),
            "the figures are carried on the error itself, not only printed"
        );

        // The same failure under the default geometry: the figures are the
        // same numbers and are NOT an offset.
        let multi = resume_hint(&load_err, Some(4), 4).expect("a non-setup error has figures");
        assert!(
            multi.contains("next --skip-rows : 4 (rows_skipped + rows_written)")
                && multi.contains("NOT a contiguous prefix of the file")
                && multi.contains("--read-cursors 4 and --pipeline-depth 4"),
            "a multi-cursor, pipelined run must be told the sum is not a resume offset: \
                 {multi}"
        );
    }
}
