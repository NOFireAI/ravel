use super::*;

// A batch that fails mid-loop (a later Parquet batch fails to decode, or
// its columns fail to resolve against the mapping) must report whatever
// was durable before it, not the empty slice `Setup` reports. Before this
// fix, both in-loop failure sites used `LoadError::Setup`, so a load that
// durably flushed earlier batches and then hit this error told the
// operator "nothing landed" while `report.tokens` already held commit
// tokens for those earlier batches -- confirmed by temporarily reverting
// this test's expectation to `&[]` and observing it match `Setup`'s
// behavior, which is the exact silent-loss shape the fix removes.
#[test]
fn batch_failed_reports_durable_tokens_not_empty() {
    let durable = vec![CommitToken {
        shard: 0,
        writer_id: uuid::Uuid::nil(),
        epoch: 0,
        seq: 1,
        ingest_hour_bucket: 0,
    }];
    let err = LoadError::BatchFailed {
        reason: "failed to read Parquet batch: corrupt page".into(),
        durable: durable.clone(),
        resume: ResumeFigures::default(),
    };
    assert_eq!(err.durable_tokens(), durable.as_slice());
    assert_ne!(
        err.durable_tokens(),
        LoadError::Setup("x".into()).durable_tokens()
    );
}

/// Round-trip (issues #983, #2753): the machine-readable flush mix serializes
/// and deserializes without changing the counts. The router's loader-requested
/// flushes split into `depth` (the loader's depth flushes on that shard,
/// capped at the router's count) and `manual` (the rest), with `arrival`
/// kept apart from `age`.
#[test]
fn flush_mix_report_round_trips_through_json() {
    let report = LoadReport {
        flush_trigger_mix: vec![
            (
                0,
                FlushTriggerMix {
                    size: 5,
                    arrival: 1,
                    age: 2,
                    final_drain: 3,
                },
            ),
            (
                2,
                FlushTriggerMix {
                    size: 0,
                    arrival: 0,
                    age: 0,
                    final_drain: 3,
                },
            ),
        ],
        depth_flushes_by_shard: vec![(0, 2), (2, 9)],
        ..LoadReport::default()
    };
    let mix = report.flush_mix_report();
    assert_eq!(
        mix.shards[0].counts,
        FlushMixCounts {
            size: 5,
            age: 2,
            arrival: 1,
            depth: 2,
            manual: 1,
        },
        "shard 0: arrival stays apart from age, two of three requested flushes were depth flushes"
    );
    assert_eq!(
        mix.shards[1].counts.depth, 3,
        "a depth count above the router's requested flushes is capped at them"
    );
    assert_eq!(
        mix.totals,
        FlushMixCounts {
            size: 5,
            age: 2,
            arrival: 1,
            depth: 5,
            manual: 1,
        },
        "totals sum each cause across shards"
    );

    let json = serde_json::to_string(&mix).expect("serialize");
    for key in [
        "\"size\":",
        "\"age\":",
        "\"arrival\":",
        "\"depth\":",
        "\"manual\":",
    ] {
        assert!(json.contains(key), "{key} missing from {json}");
    }
    let back: FlushMixReport = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(
        back, mix,
        "the round trip preserves the exact per-shard counts and totals"
    );
    // The per-shard rows survive keyed by shard, not reordered or merged.
    assert_eq!(back.shards[0].shard, 0);
    assert_eq!(back.shards[0].counts.size, 5);
    assert_eq!(back.shards[1].shard, 2);
    assert_eq!(back.shards[1].counts.depth, 3);
}

/// The loader's two write windows compose as
/// `shards * min(pipeline_depth, max_inflight_flushes)` (ADR-0807), so the
/// inner default must equal the outer one, not be an independent literal
/// that can drift below it. Below it, each shard's excess batches re-queue
/// on the flush semaphore and the outer window buys nothing; above it, the
/// value is unreachable because the loader never hands a shard more
/// concurrent work than `--pipeline-depth` batches.
#[test]
fn default_max_inflight_flushes_matches_pipeline_depth() {
    assert_eq!(
        DEFAULT_MAX_INFLIGHT_FLUSHES as usize, MEASURED_PIPELINE_DEPTH,
        "the inner flush window's default must track the outer pipeline depth's"
    );
}

/// The loader's `--max-inflight-flushes` default deliberately does NOT track
/// `IngestConfig::max_inflight_flushes` (issue #800). That field's default
/// governs the client-facing serving path, whose memory has no outer window
/// capping it; the bulk loader's tracks `--pipeline-depth`. ADR-2708 D3 moved
/// the serving default from 1 to 4, so the two now coincide by value only.
/// This pins the serving default so that moving it again is a deliberate edit
/// here too, and pins that a load gives its one tenant every permit rather
/// than the serving path's N-1 share.
#[test]
fn loader_flush_window_default_diverges_from_the_serving_default() {
    assert_eq!(
        IngestConfig::default().max_inflight_flushes,
        4,
        "the serving default is 4 (ADR-2708 D3)"
    );
    assert_eq!(
        IngestConfig::default().max_inflight_flushes_per_tenant,
        None,
        "the serving path leaves the share at max(1, N-1)"
    );
    let cfg = build_ingest_config(
        4,
        PER_BATCH_TARGET_BYTES,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        None,
    );
    assert_eq!(
        cfg.max_inflight_flushes_per_tenant,
        Some(DEFAULT_MAX_INFLIGHT_FLUSHES as usize),
        "a load's one tenant may use every --max-inflight-flushes permit"
    );
}

/// A `None` max flush delay leaves the router's age trigger at its
/// `IngestConfig::default()` value, so a library caller that passes none
/// builds a byte-for-byte default config (issue #801, deliverable 1). The
/// CLI resolves an omitted `--max-flush-delay` to `DEFAULT_MAX_FLUSH_DELAY`
/// before it gets here (issue #2753). The config
/// field is the thing that flows to the router, so it is what this asserts.
///
/// Prove-the-test: change `build_ingest_config` to substitute any other
/// duration when `max_flush_delay` is `None` (e.g.
/// `Duration::from_secs(1)`) and this fails at
/// `left: 1s, right: 2s`.
#[test]
fn max_flush_delay_unset_keeps_the_default_age_trigger() {
    let cfg = build_ingest_config(
        4,
        PER_BATCH_TARGET_BYTES,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        None,
    );
    assert_eq!(
        cfg.max_flush_delay,
        IngestConfig::default().max_flush_delay,
        "an unset --max-flush-delay must not change the router's age trigger"
    );
}

/// `--max-flush-delay 10m` reaches `IngestConfig::max_flush_delay` as
/// exactly 600s (issue #801, deliverable 2). The humantime parse lives in
/// `parse_max_flush_delay`; this pins that a `Some(_)` overrides the field
/// exactly, with no scaling or rounding.
///
/// Prove-the-test: change the `Some` arm of `build_ingest_config` to ignore
/// its argument (fall through to the default) and this fails at
/// `left: 2s, right: 600s`.
#[test]
fn max_flush_delay_set_reaches_the_config_exactly() {
    let cfg = build_ingest_config(
        4,
        PER_BATCH_TARGET_BYTES,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        Some(Duration::from_secs(600)),
    );
    assert_eq!(
        cfg.max_flush_delay,
        Duration::from_secs(600),
        "--max-flush-delay 10m must arrive as exactly 600s"
    );
}

/// The Strict ack deadline scales with the configured age trigger (issue
/// #801). A tail or under-target buffer is answered by the age trigger, so a
/// deadline that does not outlast the configured delay times out on exactly
/// the buffers the raised delay was set to let accumulate. An unset flag
/// keeps the deadline it always had, to the second.
///
/// Prove-the-test: return `WRITE_ACK_DEADLINE_FLOOR` unconditionally from
/// `write_ack_deadline` and the 10m case fails at `left: 60s, right: 660s`.
#[test]
fn write_ack_deadline_scales_with_the_configured_flush_delay() {
    let ack = |delay| write_ack_deadline(delay, None, PER_BATCH_TARGET_BYTES);
    assert_eq!(
        ack(None),
        Duration::from_secs(60),
        "an unset --max-flush-delay leaves the deadline at exactly 60s"
    );
    assert_eq!(
        ack(Some(Duration::from_secs(600))),
        Duration::from_secs(660),
        "--max-flush-delay 10m gives an 11m deadline: the delay plus one minute of margin"
    );
    assert_eq!(
        ack(Some(Duration::ZERO)),
        Duration::from_secs(60),
        "a delay under the floor cannot shorten the deadline"
    );
    let long = Duration::from_secs(3600);
    assert!(
        ack(Some(long)) > long,
        "the deadline always outlasts the age trigger it has to wait for"
    );
}

/// The ack deadline's margin carries the encode time of a target-sized flush
/// (issue #3273): `max(60s, delay + 60s + target / 100,000,000 B/s)`. A flush
/// released by the age trigger encodes up to `--target-bytes` of rows before
/// its PUT starts, so the margin scales with the target, not with the delay.
///
/// Prove-the-test: drop the `encode_allowance` term from `write_ack_deadline`
/// and the 7.4e9 case fails at `left: 150s, right: 224s`; scale the margin
/// with the delay instead (`delay + delay`) and it fails at `180s`.
#[test]
fn write_ack_deadline_adds_the_encode_time_of_a_target_sized_flush() {
    assert_eq!(ENCODE_BYTES_PER_SECOND, 100_000_000);
    assert_eq!(
        write_ack_deadline(None, None, 1_850_000_000),
        Duration::from_millis(78_500),
        "an unset delay at the stock 1.85e9 target: the 60s margin plus 18.5s of encode"
    );
    assert_eq!(
        write_ack_deadline(Some(Duration::from_secs(30)), None, 1_850_000_000),
        Duration::from_millis(108_500),
        "a 30s delay at the stock target: 30 + 60 + 18.5"
    );
    assert_eq!(
        write_ack_deadline(Some(Duration::from_secs(60)), None, 1_850_000_000),
        Duration::from_millis(138_500),
        "the CLI's 60s default delay at the stock target: 60 + 60 + 18.5"
    );
    assert_eq!(
        write_ack_deadline(Some(Duration::from_secs(90)), None, 7_400_000_000),
        Duration::from_secs(224),
        "a 90s delay at a 7.4e9 target: 90 + 60 + 74, not the 150s a flat margin gives"
    );
    assert_eq!(
        write_ack_deadline(Some(Duration::from_secs(90)), None, 1),
        Duration::from_secs(150),
        "--target-bytes 1 flushes per batch and adds nothing"
    );
    assert_eq!(
        write_ack_deadline(Some(Duration::from_secs(90)), None, 3_000_000),
        Duration::from_millis(150_030),
        "a target that may sit below one batch's per-shard slice still gets its 30ms"
    );
}

/// The encode allowance's only zero case is the per-batch target (issue
/// #3289): `PER_BATCH_TARGET_BYTES` adds nothing and one byte more, a
/// two-byte target, adds 20 ns. No slice estimate exempts a larger target,
/// since a skewed shard's real slice can be below the estimate and its buffer
/// accumulate to the target across batches.
///
/// Prove-the-test: restore a `target <= slice_est_bytes` exemption (with any
/// non-zero estimate) and the `PER_BATCH_TARGET_BYTES + 1` case fails at
/// `left: 0ns, right: 20ns`.
#[test]
fn encode_allowance_is_zero_only_at_the_per_batch_target() {
    assert_eq!(encode_allowance(PER_BATCH_TARGET_BYTES), Duration::ZERO);
    assert_eq!(
        encode_allowance(PER_BATCH_TARGET_BYTES + 1),
        Duration::from_nanos(20),
        "two bytes at 100,000,000 bytes per second"
    );
    assert_eq!(encode_allowance(3_700_000_000), Duration::from_secs(37));
}

/// `--target-bytes` is bounded by the target whose encode allowance is one
/// hour, derived from `ENCODE_BYTES_PER_SECOND` (issue #3289): 360,000,000,000
/// is accepted and one byte more is refused with a message naming the flag,
/// the bound and the value, with `--ack-margin` given as well as unset. Zero
/// is still refused.
///
/// Prove-the-test: write the bound as `> MAX_TARGET_BYTES + 1` (or as a
/// literal off by any amount) and the 360,000,000,001 case is accepted;
/// compare with `>=` and the 360,000,000,000 case is refused.
#[test]
fn validate_target_bytes_refuses_a_target_whose_encode_allowance_exceeds_an_hour() {
    assert_eq!(MAX_TARGET_BYTES, 360_000_000_000);
    assert_eq!(
        encode_allowance(360_000_000_000),
        Duration::from_secs(3_600),
        "the bound is the target with exactly one hour of encode allowance"
    );
    assert!(validate_target_bytes(None).is_ok());
    assert!(validate_target_bytes(Some(1)).is_ok());
    assert!(validate_target_bytes(Some(360_000_000_000)).is_ok());
    let refusal = "--target-bytes must be at most 360000000000 (one hour of encode at 100000000 \
                   bytes per second, the allowance the ack deadline adds when --ack-margin is \
                   unset); 360000000001 was given";
    match validate_target_bytes(Some(360_000_000_001)) {
        Err(LoadError::Setup(msg)) => assert_eq!(msg, refusal),
        other => panic!("expected a setup error: {other:?}"),
    }
    // A given --ack-margin replaces the allowance but not the bound.
    let with_margin = GeometryRequest {
        ack_margin: Some(Duration::from_secs(60)),
        ..GeometryRequest::explicit(360_000_000_001, 1)
    };
    match with_margin.validate() {
        Err(LoadError::Setup(msg)) => assert_eq!(msg, refusal),
        other => panic!("expected a setup error with --ack-margin given: {other:?}"),
    }
    match validate_target_bytes(Some(0)) {
        Err(LoadError::Setup(msg)) => assert!(msg.contains("at least 1"), "{msg}"),
        other => panic!("expected a setup error: {other:?}"),
    }
}

/// `--ack-margin` replaces the whole margin, the encode allowance included,
/// and still never takes the deadline under the 60s floor.
#[test]
fn ack_margin_replaces_the_whole_margin() {
    let margin = Some(Duration::from_secs(60));
    assert_eq!(
        write_ack_deadline(Some(Duration::from_secs(90)), margin, 7_400_000_000),
        Duration::from_secs(150),
        "--ack-margin 60 drops the 74s encode allowance"
    );
    assert_eq!(
        write_ack_deadline(
            Some(Duration::from_secs(90)),
            Some(Duration::from_secs(600)),
            1
        ),
        Duration::from_secs(690),
        "--ack-margin 600 widens the margin at a per-batch target"
    );
    assert_eq!(
        write_ack_deadline(None, Some(Duration::from_secs(1)), 7_400_000_000),
        Duration::from_secs(60),
        "a 1s margin with no delay still meets the floor"
    );
    assert_eq!(
        write_ack_deadline(
            Some(Duration::from_secs(90)),
            Some(Duration::from_secs(1)),
            7_400_000_000
        ),
        Duration::from_secs(91),
        "the floor applies to the whole deadline, not to the margin: 90 + 1, not 90 + 60"
    );
}

/// `--ack-margin 0` is refused at parse, with a message that names the flag,
/// and so is a fractional or non-numeric value.
#[test]
fn parse_ack_margin_refuses_zero_and_names_the_flag() {
    assert_eq!(parse_ack_margin("90"), Ok(Duration::from_secs(90)));
    let zero = parse_ack_margin("0").expect_err("0 must be refused");
    assert!(zero.contains("--ack-margin"), "{zero}");
    assert!(zero.contains("at least 1 second"), "{zero}");
    let fractional = parse_ack_margin("1.5").expect_err("whole seconds only");
    assert!(fractional.contains("--ack-margin"), "{fractional}");
    assert!(
        validate_ack_margin(Some(Duration::ZERO)).is_err(),
        "the loader refuses a zero margin a library caller hands it"
    );
    assert!(validate_ack_margin(None).is_ok());
    assert!(validate_ack_margin(Some(Duration::from_secs(1))).is_ok());
}

/// `strict_visibility_budget_ns` follows the configured `max_flush_delay`
/// (ADR-0076 decision 4), exactly as ravel-server's own router construction
/// derives it. The field is metrics-only on this path, but the coupling is
/// documented on `IngestConfig` and a config that raises the delay while
/// leaving the budget at the 2s-based default contradicts it.
///
/// Prove-the-test: drop the `strict_visibility_budget_ns` field from
/// `build_ingest_config` (falling through to `IngestConfig::default()`) and
/// the raised-delay case fails at `left: 2500000000, right: 600500000000`.
#[test]
fn strict_visibility_budget_follows_the_configured_flush_delay() {
    let raised = build_ingest_config(
        4,
        PER_BATCH_TARGET_BYTES,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        Some(Duration::from_secs(600)),
    );
    assert_eq!(
        raised.strict_visibility_budget_ns,
        600_000_000_000 + STRICT_VISIBILITY_RESERVE_NS,
        "the budget is the configured delay plus the reserve, never the default's"
    );
    let delay_ns = i64::try_from(raised.max_flush_delay.as_nanos()).expect("delay fits i64");
    assert!(
        raised.strict_visibility_budget_ns > delay_ns,
        "the budget must exceed the delay, not equal it: equal collapses the corridor"
    );

    let unset = build_ingest_config(
        4,
        PER_BATCH_TARGET_BYTES,
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        None,
    );
    assert_eq!(
        unset.strict_visibility_budget_ns,
        IngestConfig::default().strict_visibility_budget_ns,
        "an unset flag still builds a byte-for-byte default config"
    );
}
