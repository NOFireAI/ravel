//! Smoke test for the `l1_listing_charge` bench (issue #4393): the library
//! entry at two small caps on a reduced series count, its bands, and the
//! binary search's answer re-checked against fresh listings.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::cell::Cell;

use ravel_bench::l1_listing_charge::{
    CAP_FIGURES, ListingChargeError, RunConfig, build_bucket, check_report, compact_at,
    engine_listing_succeeds, held_fetch_bound, listing_bound, listing_succeeds, minimal_budget,
    part_shape, report_json, run, upper_bound,
};
use ravel_query::{DEFAULT_SUFFIX_LEN, DEFAULT_WHOLE_OBJECT_THRESHOLD};

const CAPS: [u64; 2] = [64 << 10, 256 << 10];

fn small_config(caps: Vec<u64>) -> RunConfig {
    RunConfig {
        series: 120,
        scrape_secs: 15,
        history_hours: 8,
        flush_hours: 2,
        caps,
    }
}

/// 99 % of `limit`, rounded down.
fn one_percent_below(limit: u64) -> u64 {
    limit - limit.div_ceil(100)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_caps_report_every_figure_once_inside_its_band() {
    let config = small_config(CAPS.to_vec());
    let report = run(config.clone()).await.expect("run");
    let text = serde_json::to_string_pretty(&report_json(&report)).expect("render");

    for figure in CAP_FIGURES {
        assert_eq!(
            text.matches(&format!("\"{figure}\":")).count(),
            CAPS.len(),
            "{figure} must appear once per cap"
        );
    }
    check_report(&text, &CAPS).expect("every band holds");
    assert_eq!(
        report.caps.iter().map(|c| c.cap_bytes).collect::<Vec<_>>(),
        CAPS
    );

    let bucket = build_bucket(&config).expect("bucket");
    for figures in &report.caps {
        assert!(figures.full_parts >= 2, "cap {}", figures.cap_bytes);
        let peak = figures.search.succeeds_at;
        // 64 B of decoded provenance columns and a 32 B `SampleProvenance`
        // for every sample: the part carries per-sample provenance.
        assert!(
            figures.search.refused_at >= 96 * figures.shape.samples,
            "cap {}: the largest refused budget, {} B, is below 96 B for each of {} samples",
            figures.cap_bytes,
            figures.search.refused_at,
            figures.shape.samples
        );
        assert!(
            figures.search.refused_at >= one_percent_below(peak),
            "cap {}: the search stopped {} B below {peak} B, coarser than 1 %",
            figures.cap_bytes,
            peak - figures.search.refused_at
        );

        // A fresh compaction of the same inputs gives the same part; list it
        // again at the reported limit and 1 % below it.
        let compacted = compact_at(&bucket, &config, figures.cap_bytes)
            .await
            .expect("compact");
        let part = &compacted.parts[figures.part_index];
        let shape = part_shape(&compacted.store, part).await.expect("shape");
        assert_eq!(shape.samples, figures.shape.samples);
        assert_eq!(shape.series, figures.shape.series);
        let store = &compacted.store;
        assert!(
            listing_succeeds(store, part, shape.series, peak)
                .await
                .expect("list"),
            "cap {}: the listing must succeed at the reported peak {peak} B",
            figures.cap_bytes
        );
        assert!(
            !listing_succeeds(store, part, shape.series, one_percent_below(peak))
                .await
                .expect("list"),
            "cap {}: the listing must be refused 1 % below the reported peak {peak} B",
            figures.cap_bytes
        );
        // The issue's formula leaves out the bytes the GETs hold, so a budget
        // of exactly that figure is refused, while the listing bound is not.
        assert!(
            !listing_succeeds(store, part, shape.series, upper_bound(&shape))
                .await
                .expect("list"),
            "cap {}: a budget of the (64 + 32) * samples + catalog formula must be refused",
            figures.cap_bytes
        );
        assert!(
            listing_succeeds(store, part, shape.series, listing_bound(&shape))
                .await
                .expect("list")
        );
    }
}

/// The figure the bench measures through `SegmentFetcher::fetch_series` is the
/// one the engine's listing (`resolve_series`, which reads each object with
/// `list_series_phase_accounted`) needs, on a bucket compacted into one part.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetcher_figure_matches_the_engine_listing_on_one_part() {
    let one_part = u64::MAX >> 1;
    let config = small_config(vec![one_part]);
    let bucket = build_bucket(&config).expect("bucket");
    let compacted = compact_at(&bucket, &config, one_part)
        .await
        .expect("compact");
    assert_eq!(compacted.parts.len(), 1);
    let part = &compacted.parts[0];
    let shape = part_shape(&compacted.store, part).await.expect("shape");
    let store = &compacted.store;
    let search = minimal_budget(upper_bound(&shape), |limit| {
        listing_succeeds(store, part, shape.series, limit)
    })
    .await
    .expect("search");

    let listed = engine_listing_succeeds(store, &config, search.succeeds_at)
        .await
        .expect("engine listing");
    assert_eq!(listed, Some(shape.series as usize));
    let refused = engine_listing_succeeds(store, &config, one_percent_below(search.succeeds_at))
        .await
        .expect("engine listing");
    assert_eq!(refused, None);
}

/// The search against a known threshold: it returns a succeeding limit within
/// 1 % above the threshold, and probes more than the first limit.
#[tokio::test]
async fn minimal_budget_finds_the_threshold_to_one_percent() {
    for (start, threshold) in [(1_000, 123_457), (500_000, 123_457), (123_457, 123_457)] {
        let probes = Cell::new(0u32);
        let probes = &probes;
        let outcome = minimal_budget(start, |limit| async move {
            probes.set(probes.get() + 1);
            Ok(limit >= threshold)
        })
        .await
        .expect("search");
        assert!(outcome.succeeds_at >= threshold, "{outcome:?}");
        assert!(
            outcome.succeeds_at - threshold <= outcome.succeeds_at / 100,
            "{outcome:?} is more than 1 % above {threshold}"
        );
        assert!(outcome.refused_at < threshold, "{outcome:?}");
        assert!(outcome.refused_at >= one_percent_below(outcome.succeeds_at));
        assert_eq!(outcome.probes, probes.get());
        assert!(outcome.probes > 1, "{outcome:?}");
    }
}

/// A figure missing, emitted twice, or outside its band fails the check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_report_rejects_missing_duplicate_and_out_of_band_figures() {
    let caps = [CAPS[0]];
    let report = run(small_config(caps.to_vec())).await.expect("run");
    let json = report_json(&report);
    let text = serde_json::to_string_pretty(&json).expect("render");
    check_report(&text, &caps).expect("the real report passes");

    let edit = |f: &dyn Fn(&mut serde_json::Map<String, serde_json::Value>)| {
        let mut doc = json.clone();
        let entry = doc["caps"][0].as_object_mut().expect("cap entry");
        f(entry);
        serde_json::to_string_pretty(&doc).expect("render")
    };
    let missing = edit(&|e| {
        e.remove("peak_charge_bytes");
    });
    assert!(check_report(&missing, &caps).is_err());
    let out_of_band = edit(&|e| {
        e.insert("peak_per_sample_bytes".into(), 250.0.into());
    });
    assert!(check_report(&out_of_band, &caps).is_err());
    let refused_above_bound = edit(&|e| {
        let bound = e["listing_bound_bytes"].as_u64().expect("bound");
        e.insert("refused_at_bytes".into(), bound.into());
    });
    assert!(check_report(&refused_above_bound, &caps).is_err());
    let bound_refused = edit(&|e| {
        e.insert("listing_bound_succeeds".into(), false.into());
    });
    assert!(check_report(&bound_refused, &caps).is_err());
    let coarse = edit(&|e| {
        let peak = e["peak_charge_bytes"].as_u64().expect("peak");
        e.insert("refused_at_bytes".into(), (peak - peak / 50).into());
    });
    assert!(check_report(&coarse, &caps).is_err());

    // A key written twice in one entry parses as its last value, so only the
    // raw-text count sees it.
    let duplicated = text.replacen("\"samples\":", "\"samples\": 1,\n      \"samples\":", 1);
    assert!(serde_json::from_str::<serde_json::Value>(&duplicated).is_ok());
    assert!(check_report(&duplicated, &caps).is_err());

    // Two caps whose peak falls as samples grow.
    let mut doc = json.clone();
    let mut second = doc["caps"][0].clone();
    let samples = second["samples"].as_u64().expect("samples");
    let peak = second["peak_charge_bytes"].as_u64().expect("peak");
    second["cap_bytes"] = (CAPS[1]).into();
    second["samples"] = (samples * 2).into();
    second["peak_charge_bytes"] = (peak / 2).into();
    doc["caps"].as_array_mut().expect("caps").push(second);
    let falling = serde_json::to_string_pretty(&doc).expect("render");
    let failures = check_report(&falling, &CAPS).expect_err("peak fell");
    assert!(
        failures.iter().any(|f| f.contains("peak fell")),
        "{failures:?}"
    );
}

/// The budget limit at which listing `part` starts to succeed, to the byte:
/// refused at `refused`, succeeding at `succeeds`.
async fn exact_threshold(
    store: &std::sync::Arc<ravel_object_store::memory::MemoryStore>,
    part: &ravel_catalog::SegmentRef,
    series: u64,
    mut refused: u64,
    mut succeeds: u64,
) -> u64 {
    assert!(
        !listing_succeeds(store, part, series, refused)
            .await
            .expect("list")
    );
    assert!(
        listing_succeeds(store, part, series, succeeds)
            .await
            .expect("list")
    );
    while succeeds - refused > 1 {
        let mid = refused + (succeeds - refused) / 2;
        if listing_succeeds(store, part, series, mid)
            .await
            .expect("list")
        {
            succeeds = mid;
        } else {
            refused = mid;
        }
    }
    succeeds
}

/// `held_fetch_bound` equals, to the byte, what the fetcher holds on a real
/// part on each side of the whole-object threshold: the listing's exact
/// threshold minus the `(64 + 32) * samples + catalog` formula. At 72 h of
/// history the compactor's one-series overshoot closes the first part of a
/// 256 KiB cap above 512 KiB (853,172 B, measured 2026-11-17), where the open
/// GET holds only the 64 KiB footer tail plus the catalog run; the last part
/// stays below the threshold and is held whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_fetch_bound_matches_the_bytes_a_real_part_holds() {
    let config = RunConfig {
        series: 24,
        scrape_secs: 15,
        history_hours: 72,
        flush_hours: 2,
        caps: vec![256 << 10],
    };
    let bucket = build_bucket(&config).expect("bucket");
    let compacted = compact_at(&bucket, &config, 256 << 10)
        .await
        .expect("compact");
    let store = &compacted.store;
    let mut large = 0;
    let mut small = 0;
    for part in [&compacted.parts[0], compacted.parts.last().expect("a part")] {
        let shape = part_shape(store, part).await.expect("shape");
        if shape.wire_bytes > DEFAULT_WHOLE_OBJECT_THRESHOLD {
            large += 1;
            assert!(
                shape.held_fetch_bytes < DEFAULT_SUFFIX_LEN + (4 << 10),
                "a {} B part holds the footer tail and a small catalog run, not {} B",
                shape.wire_bytes,
                shape.held_fetch_bytes
            );
        } else {
            small += 1;
            assert_eq!(shape.held_fetch_bytes, shape.wire_bytes);
        }
        let threshold = exact_threshold(
            store,
            part,
            shape.series,
            upper_bound(&shape),
            listing_bound(&shape),
        )
        .await;
        assert_eq!(
            threshold - upper_bound(&shape),
            shape.held_fetch_bytes,
            "a {} B part: the fetcher holds {} B, held_fetch_bound says {} B",
            shape.wire_bytes,
            threshold - upper_bound(&shape),
            shape.held_fetch_bytes
        );
    }
    assert_eq!(
        (large, small),
        (1, 1),
        "one part on each side of the threshold"
    );
}

/// `held_fetch_bound` term by term on synthetic layouts, including the two the
/// bench's parts do not reach: a footer that starts before the 64 KiB tail
/// (the fetcher's second GET runs from the footer's start to the object's
/// end) and catalog sections the tail already covers (not fetched again).
#[test]
fn held_fetch_bound_charges_the_tail_the_footer_chase_and_the_uncovered_catalog() {
    let threshold = DEFAULT_WHOLE_OBJECT_THRESHOLD;
    let tail = DEFAULT_SUFFIX_LEN;
    // At or below the threshold the whole object is held, whatever the layout.
    assert_eq!(
        held_fetch_bound(threshold, threshold - 4_096, &[(1_000, 5_000)]),
        threshold
    );

    let object_size = 614_400u64;
    assert!(object_size > threshold);
    let sections = [(2_048u64, 6_144u64), (6_144u64, 70_000u64)];
    // Footer inside the tail: the tail and the catalog run, nothing more.
    assert_eq!(
        held_fetch_bound(object_size, object_size - 1_000, &sections),
        tail + (70_000 - 2_048)
    );
    assert_eq!(
        held_fetch_bound(object_size, object_size - 1_000, &sections),
        133_488
    );
    // Footer at 500_000, before the tail at 548_864: the chase GET holds
    // 500_000..614_400 on top of the tail.
    assert_eq!(
        held_fetch_bound(object_size, 500_000, &sections),
        tail + (object_size - 500_000) + (70_000 - 2_048)
    );
    assert_eq!(held_fetch_bound(object_size, 500_000, &sections), 247_888);
    // A section inside the tail is already held; only the other is fetched.
    let tail_start = object_size - tail;
    assert_eq!(
        held_fetch_bound(
            object_size,
            object_size - 1_000,
            &[(2_048, 6_144), (tail_start + 10, tail_start + 500)]
        ),
        tail + 4_096
    );
    // A section the chase covers is held too, and none left means no run.
    assert_eq!(
        held_fetch_bound(object_size, 500_000, &[(500_100, 500_200)]),
        tail + (object_size - 500_000)
    );
}

/// The peak-per-sample band fails when the full 96 B of per-sample provenance
/// is not charged, or is charged twice. The old [1, 200] B band admitted both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peak_per_sample_band_rejects_missing_and_doubled_provenance() {
    let caps = [CAPS[0]];
    let report = run(small_config(caps.to_vec())).await.expect("run");
    let json = report_json(&report);
    let text = serde_json::to_string_pretty(&json).expect("render");
    check_report(&text, &caps).expect("the real per-sample charge is inside the band");

    let with_per_sample = |value: f64| {
        let mut doc = json.clone();
        doc["caps"][0]
            .as_object_mut()
            .expect("cap entry")
            .insert("peak_per_sample_bytes".into(), value.into());
        serde_json::to_string_pretty(&doc).expect("render")
    };
    // Only the 32 B record charged, the 64 B provenance columns dropped: about
    // 34 B a sample (the measured 97.8 B minus 64 B), below the 96 B floor.
    assert!(
        check_report(&with_per_sample(33.8), &caps).is_err(),
        "a 34 B/sample charge (record only, columns dropped) must fail the floor"
    );
    // Provenance charged twice: about 194 B a sample, above the 130 B ceiling.
    assert!(
        check_report(&with_per_sample(193.8), &caps).is_err(),
        "a 194 B/sample charge (provenance doubled) must fail the ceiling"
    );
    // The real figure restamped still holds.
    check_report(&with_per_sample(97.8), &caps).expect("97.8 B a sample is inside the band");
}

/// The reported peak sits at or below the listing bound on a real run, and
/// `check_report` compares the two directly: a peak one byte above the bound
/// fails even when the refused limit stays below the bound, the 1 % bracket
/// holds and the listing under the bound succeeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peak_above_the_listing_bound_fails_the_direct_check() {
    let report = run(small_config(CAPS.to_vec())).await.expect("run");
    for entry in &report.caps {
        assert!(
            entry.search.succeeds_at <= entry.listing_bound_bytes,
            "cap {}: the reported peak {} B is above the listing bound {} B",
            entry.cap_bytes,
            entry.search.succeeds_at,
            entry.listing_bound_bytes
        );
    }

    let caps = [CAPS[0]];
    let report = run(small_config(caps.to_vec())).await.expect("run");
    let json = report_json(&report);
    let text = serde_json::to_string_pretty(&json).expect("render");
    check_report(&text, &caps).expect("the real report holds every band");

    let bound = json["caps"][0]["listing_bound_bytes"]
        .as_u64()
        .expect("bound");
    let mut doc = json.clone();
    let entry = doc["caps"][0].as_object_mut().expect("cap entry");
    entry.insert("peak_charge_bytes".into(), (bound + 1).into());
    entry.insert("refused_at_bytes".into(), (bound - 1).into());
    entry.insert("listing_bound_succeeds".into(), true.into());
    let violated = serde_json::to_string_pretty(&doc).expect("render");
    assert_eq!(
        check_report(&violated, &caps),
        Err(vec![format!(
            "cap {}: the peak {} B is above the listing bound {bound} B",
            caps[0],
            bound + 1
        )])
    );
}

/// `minimal_budget` terminates when the threshold is below 100 bytes, where the
/// 1 % step floors to zero, and lands on it to the byte. A ready future never
/// yields, so a hang is caught by capping the probes rather than by a timeout.
#[tokio::test]
async fn minimal_budget_terminates_below_a_hundred_byte_threshold() {
    let probes = Cell::new(0u32);
    let outcome = minimal_budget(1, |limit| {
        probes.set(probes.get() + 1);
        let calls = probes.get();
        async move {
            if calls > 1_000 {
                return Err(ListingChargeError::Setup(
                    "minimal_budget spun past 1,000 probes".to_string(),
                ));
            }
            Ok(limit >= 50)
        }
    })
    .await
    .expect("search");
    assert_eq!((outcome.succeeds_at, outcome.refused_at), (50, 49));
    assert_eq!(outcome.probes, probes.get());
}

/// A flush shorter than one scrape step holds no step, so `validate` returns
/// the typed setup error instead of letting `build_bucket` panic on a zero
/// `step_by`.
#[test]
fn build_bucket_rejects_a_flush_shorter_than_one_scrape_step() {
    let config = RunConfig {
        series: 120,
        scrape_secs: 7_200,
        history_hours: 2,
        flush_hours: 1,
        caps: vec![1 << 20],
    };
    match build_bucket(&config) {
        Err(ListingChargeError::Setup(msg)) => {
            assert!(msg.contains("holds no scrape"), "{msg}")
        }
        Err(other) => panic!("expected a setup error for a zero-step flush, got {other}"),
        Ok(_) => panic!("expected a setup error for a zero-step flush, got a bucket"),
    }
}
