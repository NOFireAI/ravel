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

/// Round-trip (issue #983): the machine-readable flush mix serializes and
/// deserializes without changing the counts, and the final drain is keyed
/// `final` in the serialized form (the Rust field is `final_drain`, since
/// `final` is a keyword). Test 3's serialize/deserialize half.
#[test]
fn flush_mix_report_round_trips_through_json() {
    let report = LoadReport {
        flush_trigger_mix: vec![
            (
                0,
                FlushTriggerMix {
                    size: 5,
                    age: 2,
                    final_drain: 1,
                },
            ),
            (
                2,
                FlushTriggerMix {
                    size: 0,
                    age: 0,
                    final_drain: 3,
                },
            ),
        ],
        ..LoadReport::default()
    };
    let mix = report.flush_mix_report();
    assert_eq!(
        mix.totals,
        FlushMixCounts {
            size: 5,
            age: 2,
            final_drain: 4,
        },
        "totals sum each cause across shards"
    );

    let json = serde_json::to_string(&mix).expect("serialize");
    assert!(
        json.contains("\"final\":"),
        "the drain is keyed `final` in the serialized form: {json}"
    );
    let back: FlushMixReport = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(
        back, mix,
        "the round trip preserves the exact per-shard counts and totals"
    );
    // The per-shard rows survive keyed by shard, not reordered or merged.
    assert_eq!(back.shards[0].shard, 0);
    assert_eq!(back.shards[0].counts.size, 5);
    assert_eq!(back.shards[1].shard, 2);
    assert_eq!(back.shards[1].counts.final_drain, 3);
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
        DEFAULT_MAX_INFLIGHT_FLUSHES as usize, DEFAULT_PIPELINE_DEPTH,
        "the inner flush window's default must track the outer pipeline depth's"
    );
}

/// The loader's `--max-inflight-flushes` default deliberately does NOT track
/// `IngestConfig::max_inflight_flushes` (issue #800). That field's default of
/// 1 governs the client-facing serving path, whose Strict ack contract
/// ADR-0067 decision 2 froze and whose memory has no outer window capping
/// it; the bulk loader is a different workload. This pins the divergence so
/// that raising the serving default later is a deliberate edit here too,
/// rather than something that silently re-couples the two.
#[test]
fn loader_flush_window_default_diverges_from_the_serving_default() {
    assert_eq!(
        IngestConfig::default().max_inflight_flushes,
        1,
        "the serving default is unchanged at 1 (ADR-0067 decision 2)"
    );
    assert!(
        DEFAULT_MAX_INFLIGHT_FLUSHES > IngestConfig::default().max_inflight_flushes,
        "the bulk loader pipelines flushes where the serving path does not"
    );
}

/// `--max-flush-delay` unset (`None`) leaves the router's age trigger at
/// its `IngestConfig::default()` value, so an omitted flag builds a
/// byte-for-byte default config (issue #801, deliverable 1). The config
/// field is the thing that flows to the router, so it is what this asserts.
///
/// Prove-the-test: change `build_ingest_config` to substitute any other
/// duration when `max_flush_delay` is `None` (e.g.
/// `Duration::from_secs(1)`) and this fails at
/// `left: 1s, right: 2s`.
#[test]
fn max_flush_delay_unset_keeps_the_default_age_trigger() {
    let cfg = build_ingest_config(4, DEFAULT_TARGET_BYTES, DEFAULT_MAX_INFLIGHT_FLUSHES, None);
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
        DEFAULT_TARGET_BYTES,
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
    assert_eq!(
        write_ack_deadline(None),
        Duration::from_secs(60),
        "an unset --max-flush-delay leaves the deadline at exactly 60s"
    );
    assert_eq!(
        write_ack_deadline(Some(Duration::from_secs(600))),
        Duration::from_secs(660),
        "--max-flush-delay 10m gives an 11m deadline: the delay plus one minute of margin"
    );
    assert_eq!(
        write_ack_deadline(Some(Duration::ZERO)),
        Duration::from_secs(60),
        "a delay under the floor cannot shorten the deadline"
    );
    let long = Duration::from_secs(3600);
    assert!(
        write_ack_deadline(Some(long)) > long,
        "the deadline always outlasts the age trigger it has to wait for"
    );
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
        DEFAULT_TARGET_BYTES,
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

    let unset = build_ingest_config(4, DEFAULT_TARGET_BYTES, DEFAULT_MAX_INFLIGHT_FLUSHES, None);
    assert_eq!(
        unset.strict_visibility_budget_ns,
        IngestConfig::default().strict_visibility_budget_ns,
        "an unset flag still builds a byte-for-byte default config"
    );
}
