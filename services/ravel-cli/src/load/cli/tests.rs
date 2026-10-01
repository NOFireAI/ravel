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
        1,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        RlogZstdLevel::DEFAULT,
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
                4,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                DEFAULT_DECODE_QUEUE_BATCHES,
                target_bytes,
                None,
                RlogZstdLevel::DEFAULT,
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
        emitted.contains("ESTIMATED in-memory footprint") && emitted.contains("about 4 rows here"),
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
/// warning; change the `target_bytes <= DEFAULT_TARGET_BYTES` guard to `<`
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
        target_bytes_no_effect_warning(DEFAULT_TARGET_BYTES, &inert, 16, 4).is_none(),
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
