use super::*;
use crate::load::test_support::*;

/// `bloom` is nested inside `encode`'s timing window, not a disjoint fifth
/// stage (ADR-0104 decision 1): its printed row must say so, or an
/// operator summing the `stage timings` table's `total_ms` column
/// double-counts it against `encode`. This pins the exact rendered name
/// for every stage, not a substring, so a later rename of the marker text
/// cannot quietly drop it.
#[cfg(feature = "stage-timing")]
#[test]
fn stage_display_name_marks_bloom_as_nested_in_encode() {
    assert_eq!(stage_display_name(ravel_ingest::LogStage::Admit), "admit");
    assert_eq!(stage_display_name(ravel_ingest::LogStage::Route), "route");
    assert_eq!(stage_display_name(ravel_ingest::LogStage::Merge), "merge");
    assert_eq!(stage_display_name(ravel_ingest::LogStage::Encode), "encode");
    assert_eq!(
        stage_display_name(ravel_ingest::LogStage::Bloom),
        "bloom (in encode)"
    );
}

/// A load whose object crosses the 1000-column dynamic budget produces the
/// overflow warning, driven from real router metrics rather than a
/// hand-built snapshot. Flip the `dynamic_columns_overflowed_total > 0`
/// guard in `dynamic_column_warnings` to `false` and this fails: no warning
/// is emitted for a load that genuinely overflowed.
#[tokio::test]
async fn warns_when_dynamic_columns_overflow() {
    let report = run_wide_load(1001).await;
    assert!(
        report.metrics.dynamic_columns_overflowed_total > 0,
        "1001 distinct attribute columns overflow the 1000-column per-object budget"
    );
    let warnings = dynamic_column_warnings(
        &report.metrics,
        ravel_logseg::RlogConfig::default().max_dynamic_columns,
    );
    assert_eq!(warnings.len(), 1, "exactly the overflow warning fires");
    assert!(
        warnings[0].contains("overflowed the per-object dynamic-column budget")
            && warnings[0].contains("attrs_raw"),
        "the overflow warning states the count and the attrs_raw consequence: {}",
        warnings[0]
    );
}

/// The overflow warning reaches a caller of the real entry point, not just
/// [`dynamic_column_warnings`].
///
/// The sibling tests call that helper directly, so all of them stayed green
/// when the emit loop was deleted from the entry point: they prove the text,
/// not the wiring. This one drives [`run_warning_to`] end to end -- mapping
/// file on disk, Parquet fixture, real router, real write -- and asserts the
/// warning came out of the stream the CLI hands it.
#[tokio::test]
async fn the_entry_point_emits_the_overflow_warning() {
    use parquet::arrow::ArrowWriter;
    use ravel_object_store::memory::MemoryStore;

    let n_attrs = 1001;
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("wide.parquet");
    let mapping_path = dir.path().join("mapping.toml");

    let mut cols: Vec<(String, ArrayRef)> = vec![
        ("ts".to_string(), i64_col(vec![NOW_NS])),
        ("svc".to_string(), str_col(vec!["api"])),
    ];
    let mut attr_toml = String::new();
    for i in 0..n_attrs {
        let name = format!("a{i}");
        cols.push((name.clone(), i64_col(vec![i as i64])));
        attr_toml.push_str(&format!(
            "\n[[attribute]]\nkey = \"{name}\"\ncolumn = \"{name}\"\ntype = \"i64\"\n"
        ));
    }
    let batch = RecordBatch::try_from_iter(cols).expect("wide batch");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    std::fs::write(
        &mapping_path,
        format!(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n{attr_toml}"
        ),
    )
    .expect("write mapping");

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut sink: Vec<u8> = Vec::new();
    run_warning_to(
        store,
        &pq,
        "acme",
        &mapping_path,
        SignalArg::Logs,
        4,
        10_000,
        0,
        None,
        Some(1),
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        Some(PER_BATCH_TARGET_BYTES),
        None,
        RlogZstdLevel::DEFAULT,
        None,
        NOW_NS,
        &mut sink,
    )
    .await
    .expect("the load itself succeeds; overflow is a warning, not a failure");

    let emitted = String::from_utf8(sink).expect("warnings are utf-8");
    assert!(
        emitted.contains("overflowed the per-object dynamic-column budget"),
        "the entry point must emit the overflow warning it computed: {emitted}"
    );
    assert!(
        emitted.contains(ADMISSION_BYPASS_WARNING),
        "and the pre-existing admission warning still goes to the same stream: {emitted}"
    );
}

/// A load that reaches >= 90% of the budget without overflowing produces the
/// distinct near-cap warning, again from real metrics.
#[tokio::test]
async fn warns_near_cap_without_overflow() {
    let report = run_wide_load(950).await;
    assert_eq!(
        report.metrics.dynamic_columns_overflowed_total, 0,
        "950 distinct columns stay under the 1000 budget, so nothing overflows"
    );
    assert!(
        report.metrics.dynamic_columns_used_max >= 900,
        "the widest object should sit near the cap: used_max = {}",
        report.metrics.dynamic_columns_used_max
    );
    let warnings = dynamic_column_warnings(&report.metrics, 1000);
    assert_eq!(warnings.len(), 1, "exactly the near-cap warning fires");
    assert!(
        warnings[0].contains("at or above") && warnings[0].contains("attrs_raw"),
        "the near-cap warning states the pressure and the attrs_raw consequence: {}",
        warnings[0]
    );
}

/// The near-cap boundary is exact: 90% warns, 89% does not, and an overflow
/// takes precedence over the near-cap message. Flip the `>=` in
/// `dynamic_column_warnings` to `>` and the exactly-90% case fails.
#[test]
fn near_cap_threshold_is_at_ninety_percent() {
    let snap = |used: u64, overflowed: u64| LogIngestMetricsSnapshot {
        dynamic_columns_used_max: used,
        dynamic_columns_overflowed_total: overflowed,
        ..Default::default()
    };
    assert_eq!(
        dynamic_column_warnings(&snap(900, 0), 1000).len(),
        1,
        "900 / 1000 = exactly 90% warns"
    );
    assert!(
        dynamic_column_warnings(&snap(899, 0), 1000).is_empty(),
        "899 / 1000 is just under 90% and does not warn"
    );
    assert!(
        dynamic_column_warnings(&snap(890, 0), 1000).is_empty(),
        "890 / 1000 = 89% does not warn"
    );
    let overflow = dynamic_column_warnings(&snap(900, 3), 1000);
    assert_eq!(overflow.len(), 1, "overflow still yields one message");
    assert!(
        overflow[0].contains("overflowed the per-object dynamic-column budget"),
        "overflow takes precedence over the near-cap message: {}",
        overflow[0]
    );
}

/// The no-effect case reaches the operator through the real entry point
/// (issue #971): a target that reproduced the `1` layout is reported on the
/// warning stream, and one that changed the layout is not.
///
/// Same fixture and geometry as
/// `target_bytes_regimes_are_set_by_one_batchs_per_shard_slice`, driven
/// through [`run_warning_to`] so the mapping file, the router, and the
/// warning stream are the CLI's own.
///
/// Prove-the-test: delete the `target_bytes_no_effect_warning` emit block in
/// `run_warning_to` and the first assertion fails; make the helper return
/// its message unconditionally and the 1 MiB case fails instead.
#[tokio::test]
async fn the_entry_point_reports_a_target_bytes_that_changed_nothing() {
    use ravel_object_store::memory::MemoryStore;

    let shards = 4u32;
    let (_dir, pq, mapping_path, _m) = fat_attr_sorted_by_shard_fixture(shards, 16, 4000);

    let run = |target_bytes: usize| {
        let pq = pq.clone();
        let mapping_path = mapping_path.clone();
        async move {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let mut sink: Vec<u8> = Vec::new();
            run_warning_to(
                store,
                &pq,
                "acme",
                &mapping_path,
                SignalArg::Logs,
                shards,
                16,
                0,
                Some(shards as usize),
                Some(4),
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                DEFAULT_DECODE_QUEUE_BATCHES,
                Some(target_bytes),
                // The 1 MiB case parks on an ack that only the age trigger
                // answers, on the wall clock; 2 s keeps that wait short.
                Some(Duration::from_secs(2)),
                RlogZstdLevel::DEFAULT,
                None,
                NOW_NS,
                &mut sink,
            )
            .await
            .expect("the load itself succeeds; an ineffective target is a warning");
            String::from_utf8(sink).expect("warnings are utf-8")
        }
    };

    let emitted = run(4096).await;
    assert!(
        emitted.contains("--target-bytes 4096 did not change this load's object layout"),
        "the ineffective target is named with its value: {emitted}"
    );
    assert!(
        emitted.contains("estimated UNCOMPRESSED object content")
            && emitted.contains("about 4 rows here"),
        "and the message states the unit and the slice it had to clear: {emitted}"
    );

    let effective = run(1024 * 1024).await;
    assert!(
        !effective.contains("did not change this load's object layout"),
        "a target that collapsed 16 writes into 4 objects must not be reported as inert: \
             {effective}"
    );
}

/// [`target_bytes_no_effect_warning`]'s three preconditions, each pinned by
/// the case that would misfire without it. `writes` is
/// `LoadReport::tokens`, one entry per (batch, shard) ack, so a flush that
/// answered several batches shows up as a repeated token.
///
/// Prove-the-test: delete the `objects < writes` early return and the
/// accumulating case starts warning (its `is_none` assertion fails at
/// "a repeated token is a flush that answered two batches"); delete the
/// `< 2` writes-per-shard guard and the one-write-per-shard case starts
/// warning; change the `target_bytes <= PER_BATCH_TARGET_BYTES` guard to `<`
/// and the default-target case starts warning.
#[test]
fn the_no_effect_warning_fires_only_when_the_target_is_what_did_nothing() {
    let token = |shard: u32, seq: u64| CommitToken {
        shard,
        writer_id: uuid::Uuid::nil(),
        epoch: 1,
        seq,
        ingest_hour_bucket: 7,
    };
    let report = |tokens: Vec<CommitToken>| LoadReport {
        tokens,
        ..LoadReport::default()
    };

    // Two writes on one shard, two distinct objects: nothing accumulated.
    let inert = report(vec![token(0, 1), token(0, 2)]);
    let warning = target_bytes_no_effect_warning(4096, &inert, 16, 4)
        .expect("two writes, two objects, one shard: the target did nothing");
    assert!(
        warning.contains("All 2 (batch, shard) writes flushed as their own object (2 objects)"),
        "the message reports the observed counts: {warning}"
    );
    assert!(
        warning.contains("about 4 rows here, at --batch-rows 16 over 4 shards"),
        "and the slice threshold it derives from the geometry: {warning}"
    );

    assert!(
        target_bytes_no_effect_warning(PER_BATCH_TARGET_BYTES, &inert, 16, 4).is_none(),
        "the default target is not a no-op, it is the documented per-batch layout"
    );

    // Two writes answered by one flush: the same token repeats, so the
    // target held a buffer open.
    let accumulating = report(vec![token(0, 1), token(0, 1)]);
    assert!(
        target_bytes_no_effect_warning(4096, &accumulating, 16, 4).is_none(),
        "a repeated token is a flush that answered two batches: the target worked"
    );

    // One write per shard: no buffer could have spanned two writes at any
    // target, so the target is not what to blame.
    let single = report(vec![token(0, 1), token(1, 1), token(2, 1), token(3, 1)]);
    assert!(
        target_bytes_no_effect_warning(4096, &single, 16, 4).is_none(),
        "no shard was written twice, so nothing could have accumulated"
    );
}

/// A derived budget below one batch is reported even when the load then
/// fails. The loader admits the batches one at a time and records the
/// warning on the report, which a failed load drops; the entry point prints
/// it beside the error from the copy the memory options kept.
#[tokio::test]
async fn a_failed_load_still_prints_the_one_batch_warning() {
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;

    let (dir, pq) = write_parquet(&batch(vec![
        ("ts", i64_col(vec![NOW_NS; 4])),
        ("idx", i64_col((0..4).collect())),
    ]));
    let mapping_path = dir.path().join("mapping.toml");
    std::fs::write(
        &mapping_path,
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[attribute]]\nkey = \"idx\"\ncolumn = \"idx\"\ntype = \"i64\"\n",
    )
    .expect("write mapping");

    // The second batch's data-object PUT fails, after the first batch has
    // already been admitted alone and the warning recorded.
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
    run_memory_warning_to(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        &pq,
        "acme",
        &mapping_path,
        SignalArg::Logs,
        1,
        2,
        0,
        Some(1),
        Some(1),
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        Some(PER_BATCH_TARGET_BYTES),
        None,
        RlogZstdLevel::DEFAULT,
        LoadMemoryRequest::Derived {
            host_total_bytes: Some(1024),
        },
        NOW_NS,
        &mut sink,
    )
    .await
    .expect_err("the second batch's PUT fails permanently, so the load fails");
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the scripted PUT fault fired"
    );

    let emitted = String::from_utf8(sink).expect("warnings are utf-8");
    assert_eq!(
        emitted
            .matches("more than the loader's derived memory budget of 0 bytes")
            .count(),
        1,
        "the one-batch warning is printed exactly once on the failure path: {emitted}"
    );
    assert!(
        emitted.contains("host memory 1024 bytes"),
        "naming the budget's source: {emitted}"
    );
}

/// The `--fold-after-load` summary lines for each outcome: a seal, a load that
/// wrote nothing (no fold ran), a no-op, and either fold beside a
/// `FoldLeftCommitsUncovered` error, which never reads as a plain seal.
#[test]
fn fold_summary_names_the_outcome() {
    let folded = LoadFold {
        elapsed: Duration::from_millis(1_250),
        entry_count: 7,
        watermark_hour: Some(472_222),
        seal_through_hour: Some(472_222),
        no_op: false,
        parts_read: 2,
        buckets_listed: 1,
        records_read: 3,
    };
    assert_eq!(
        fold_summary(&folded, true),
        "  fold after load  : sealed, seal_through_hour 472222, watermark_hour 472222, \
         entries 7, parts read 2, buckets listed 1, records read 3, elapsed 1.250s (included \
         in elapsed)\n"
    );
    assert_eq!(
        fold_summary(&folded, false),
        "  fold after load  : folded, commits not confirmed in the snapshot (see the error \
         below), seal_through_hour 472222, watermark_hour 472222, entries 7, parts read 2, \
         buckets listed 1, records read 3, elapsed 1.250s (included in elapsed)\n"
    );

    let empty = fold_summary(&LoadFold::default(), true);
    assert_eq!(empty, "  fold after load  : no fold: nothing was written\n");

    let no_op = LoadFold {
        elapsed: Duration::ZERO,
        entry_count: 0,
        watermark_hour: Some(472_222),
        seal_through_hour: Some(472_222),
        no_op: true,
        parts_read: 1,
        buckets_listed: 0,
        records_read: 0,
    };
    assert_eq!(
        fold_summary(&no_op, true),
        "  fold after load  : no-op, HEAD already sealed, seal_through_hour 472222, \
         watermark_hour 472222, entries 0, parts read 1, buckets listed 0, records read 0, \
         elapsed 0.000s (included in elapsed)\n"
    );
    assert_eq!(
        fold_summary(&no_op, false),
        "  fold after load  : no-op, HEAD already sealed, commits not confirmed in the snapshot \
         (see the error below), seal_through_hour 472222, watermark_hour 472222, entries 0, \
         parts read 1, buckets listed 0, records read 0, elapsed 0.000s (included in \
         elapsed)\n"
    );
}

/// A warnings sink the test can read while the load still holds it.
#[derive(Clone, Default)]
struct SharedSink(Arc<std::sync::Mutex<Vec<u8>>>);

impl SharedSink {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("sink not poisoned").clone())
            .expect("warnings are utf-8")
    }
}

impl std::io::Write for SharedSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("sink not poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A one-column logs fixture of `ts` values and its mapping file.
fn ts_only_fixture(ts: Vec<i64>) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let (dir, pq) = write_parquet(&batch(vec![("ts", i64_col(ts))]));
    let mapping_path = dir.path().join("mapping.toml");
    std::fs::write(&mapping_path, "ts_column = \"ts\"\nts_unit = \"nanos\"\n")
        .expect("write mapping");
    (dir, pq, mapping_path)
}

/// Router-quiet periods on the injected clock [`yield_until_geometry_printed`]
/// sits through before it calls the line missing. Decoding the first batch
/// runs on a blocking thread that reads no clock, so the router reads as quiet
/// while it runs; this many quiet periods is far longer than that decode and
/// still fails a load that never prints in well under a second.
const GEOMETRY_QUIET_PERIODS: usize = 10_000;

/// Yields until `sink` holds the load's geometry line. The loader prints it
/// once the first batch is decoded, which happens on a blocking thread this
/// task cannot otherwise wait on. Panics once the router has been quiet on
/// `clock` [`GEOMETRY_QUIET_PERIODS`] times with no line, so a load that never
/// calls the geometry hook fails the test instead of hanging it.
async fn yield_until_geometry_printed(sink: &SharedSink, clock: &TestClock) {
    for _ in 0..GEOMETRY_QUIET_PERIODS {
        if sink.text().contains(GEOMETRY_LINE_PREFIX) {
            return;
        }
        yield_until_router_is_quiet(clock).await;
    }
    panic!(
        "no geometry line after {GEOMETRY_QUIET_PERIODS} quiet periods on the injected clock: {}",
        sink.text()
    );
}

/// A store that records, at its first data-object PUT, whether `sink` already
/// held the geometry line.
struct GeometryBeforePutStore {
    inner: Arc<dyn ObjectStoreBackend>,
    sink: SharedSink,
    printed_at_first_put: std::sync::Mutex<Option<bool>>,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for GeometryBeforePutStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: ravel_object_store::PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        if key.contains("/l0/") {
            let printed = self.sink.text().contains(GEOMETRY_LINE_PREFIX);
            self.printed_at_first_put
                .lock()
                .expect("first-put lock")
                .get_or_insert(printed);
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

/// Issue #2753: with `--max-flush-delay` unset the CLI hands the router 60 s,
/// not the library's 2 s default. One 2-row batch at a 1 GiB target and
/// `--pipeline-depth 1` parks the loader on that batch's ack, which only the
/// age trigger can answer. 59 s on the injected clock leaves the store empty;
/// 2 s more ages the buffer out and the load finishes with one object.
///
/// Prove-the-test: delete the `max_flush_delay.unwrap_or(
/// DEFAULT_MAX_FLUSH_DELAY)` line in `run_fold_warning_to` and the router
/// keeps its 2 s default, so the 59 s advance publishes the object and the
/// load completes into the `panic!` arm.
#[tokio::test]
async fn an_unset_max_flush_delay_reaches_the_router_as_sixty_seconds() {
    use ravel_object_store::memory::MemoryStore;

    const SECOND_NS: i64 = 1_000_000_000;
    assert_eq!(DEFAULT_MAX_FLUSH_DELAY, Duration::from_secs(60));
    let (_dir, pq, mapping_path) = ts_only_fixture(vec![NOW_NS; 2]);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(NOW_NS);
    let sink = SharedSink::default();
    let mut warnings = sink.clone();
    let load = run_fold_warning_to(
        Arc::clone(&store),
        &pq,
        "acme",
        &mapping_path,
        SignalArg::Logs,
        1,
        2,
        0,
        Some(1),
        Some(1),
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        Some(1 << 30),
        None,
        RlogZstdLevel::DEFAULT,
        LoadMemoryRequest::Derived {
            host_total_bytes: Some(64 << 30),
        },
        false,
        NOW_NS,
        Arc::clone(&clock) as Arc<dyn Clock>,
        &mut warnings,
    );
    tokio::pin!(load);

    tokio::select! {
        biased;
        result = &mut load => panic!(
            "the load must not finish before 60 s on the injected clock: {result:?}"
        ),
        () = async {
            yield_until_geometry_printed(&sink, &clock).await;
            yield_until_router_is_quiet(&clock).await;
            clock.advance_ns(59 * SECOND_NS);
            yield_until_router_is_quiet(&clock).await;
        } => {}
    }
    assert_eq!(
        list_data_objects(store.as_ref()).await.len(),
        0,
        "a 59 s old buffer is inside the 60 s delay"
    );
    let emitted = sink.text();
    assert!(
        emitted.contains(
            "load geometry: logs: flush target 1073741824 bytes (--target-bytes), pipeline \
             depth 1 (--pipeline-depth)"
        ),
        "{emitted}"
    );

    clock.advance_ns(2 * SECOND_NS);
    load.await
        .expect("the buffer ages out at 60 s and the load finishes");
    assert_eq!(list_data_objects(store.as_ref()).await.len(), 1);
}

/// Issue #2753: a load at the default geometry that fails on a later batch
/// reports at once. The derived target is far above the two buffered
/// batches and the injected [`TestClock`] never advances, so neither the
/// size nor the age trigger can answer their acks: only the failure path's
/// own `flush_all` (`drain_inflight`) can. The load returns the row
/// rejection with the first two batches durable as one object, and the
/// geometry line was already in the warnings sink at the first data-object
/// PUT.
///
/// Prove-the-test: delete the `router.flush_all()` in `drain_inflight` and
/// the buffered acks are answered by nothing, so the load fails only once
/// the Strict ack deadline (120 s of wall time at the 60 s default delay)
/// expires, with a write error instead of the row rejection. Drop the
/// `rx.recv()` arm in `printing_geometry` and the line is written only once
/// the load returns, after that PUT.
#[tokio::test]
async fn a_failed_default_load_flushes_before_it_reports() {
    use ravel_object_store::memory::MemoryStore;

    let limits = LogIngestLimits::default();
    let mut ts = vec![NOW_NS; 6];
    // Row 4 is the first row of the third 2-row batch.
    ts[4] = NOW_NS + limits.max_future_skew_ns + 1;
    let (_dir, pq, mapping_path) = ts_only_fixture(ts);
    let clock = TestClock::new(NOW_NS);
    let sink = SharedSink::default();
    let probe = Arc::new(GeometryBeforePutStore {
        inner: Arc::new(MemoryStore::new()),
        sink: sink.clone(),
        printed_at_first_put: std::sync::Mutex::new(None),
    });
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&probe) as Arc<dyn ObjectStoreBackend>;
    let mut warnings = sink.clone();
    let err = run_fold_warning_to(
        Arc::clone(&store),
        &pq,
        "acme",
        &mapping_path,
        SignalArg::Logs,
        1,
        2,
        0,
        Some(1),
        None,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        None,
        None,
        RlogZstdLevel::DEFAULT,
        LoadMemoryRequest::Derived {
            host_total_bytes: Some(64 << 30),
        },
        false,
        NOW_NS,
        Arc::clone(&clock) as Arc<dyn Clock>,
        &mut warnings,
    )
    .await
    .expect_err("row 4 is past the future-skew limit");

    match err.downcast_ref::<LoadError>() {
        Some(LoadError::RowRejected { row, durable, .. }) => {
            assert_eq!(*row, 4);
            assert_eq!(durable.len(), 2, "both earlier batches acked: {durable:?}");
        }
        other => panic!("expected the row rejection, got {other:?}: {err:#}"),
    }
    assert_eq!(
        list_data_objects(store.as_ref()).await.len(),
        1,
        "the failure flush published the two buffered batches as one object"
    );
    let emitted = sink.text();
    assert!(
        emitted.contains("load geometry: logs: flush target "),
        "{emitted}"
    );
    assert_eq!(
        *probe.printed_at_first_put.lock().expect("first-put lock"),
        Some(true),
        "the geometry line was in the sink at the first data-object PUT"
    );
}
