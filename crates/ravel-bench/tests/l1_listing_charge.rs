//! Smoke test for the `l1_listing_charge` bench (issue #4393): the library
//! entry at two small caps on a reduced series count, its bands, and the
//! binary search's answer re-checked against fresh listings.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::cell::Cell;
use std::time::Duration;

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

/// `held_fetch_bound`'s large-object branch: above the whole-object threshold
/// the open GET holds only the suffix tail (plus the footer chase above it),
/// not the whole object, and the catalog run adds the span from the first
/// section's start to the last one's end. Pinned exactly so a bound that
/// overestimates (for example one that charges the whole object) is caught.
#[test]
fn held_fetch_bound_large_object_holds_tail_chase_and_catalog_span() {
    // At or below the threshold the whole object is held.
    assert_eq!(
        held_fetch_bound(
            DEFAULT_WHOLE_OBJECT_THRESHOLD,
            DEFAULT_WHOLE_OBJECT_THRESHOLD - 4_096,
            &[(1_000, 5_000)],
        ),
        DEFAULT_WHOLE_OBJECT_THRESHOLD,
    );

    // Above it: 614_400 B object, footer at 500_000 so the chase runs from the
    // footer up to the 64 KiB tail, two catalog sections spanning 2_048..70_000.
    let object_size = DEFAULT_WHOLE_OBJECT_THRESHOLD + 90_112; // 614_400 B
    let footer_offset = 500_000u64;
    let sections = [(2_048u64, 6_144u64), (6_144u64, 70_000u64)];
    let tail = DEFAULT_SUFFIX_LEN; // 65_536, smaller than the object
    let chase = object_size - footer_offset - tail; // 48_864
    let span = 70_000 - 2_048; // 67_952
    let expected = tail + chase + span; // 182_352
    assert_eq!(
        held_fetch_bound(object_size, footer_offset, &sections),
        expected,
    );
    assert_eq!(
        held_fetch_bound(object_size, footer_offset, &sections),
        182_352,
        "the large-object branch must hold the tail, chase and catalog span exactly"
    );
    assert!(
        expected < object_size,
        "the large-object branch must hold less than the whole {object_size} B object"
    );

    // Footer inside the tail: the chase saturates to zero, only tail and span.
    let small_chase = DEFAULT_WHOLE_OBJECT_THRESHOLD + 1;
    assert_eq!(
        held_fetch_bound(small_chase, small_chase - 1_000, &[(1_000, 2_000)]),
        DEFAULT_SUFFIX_LEN + 1_000,
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

/// The "peak at or below the listing bound" band admits the reported peak
/// sitting just above the bound, because the 1 %-resolution search reports the
/// smallest probed succeeding budget rather than the true threshold; it still
/// catches a budget at or above the bound that the search refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn band_admits_peak_above_bound_within_the_search_bracket() {
    let caps = [CAPS[0]];
    let report = run(small_config(caps.to_vec())).await.expect("run");
    let entry = &report.caps[0];
    assert!(
        entry.search.succeeds_at > entry.listing_bound_bytes,
        "expected the reported peak {} B above the listing bound {} B",
        entry.search.succeeds_at,
        entry.listing_bound_bytes
    );
    let json = report_json(&report);
    let text = serde_json::to_string_pretty(&json).expect("render");
    check_report(&text, &caps).expect("the band admits a peak above the bound within the bracket");

    // A budget at the bound that the search refused means the true peak is
    // above the bound: a real violation, rejected directly.
    let mut doc = json.clone();
    let bound = doc["caps"][0]["listing_bound_bytes"]
        .as_u64()
        .expect("bound");
    doc["caps"][0]
        .as_object_mut()
        .expect("cap entry")
        .insert("refused_at_bytes".into(), bound.into());
    let violated = serde_json::to_string_pretty(&doc).expect("render");
    assert!(check_report(&violated, &caps).is_err());
}

/// `minimal_budget` terminates when the threshold is below 100 bytes, where the
/// 1 % step would floor to zero and spin forever.
#[tokio::test]
async fn minimal_budget_terminates_below_a_hundred_byte_threshold() {
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        minimal_budget(1, |limit| async move { Ok(limit >= 50) }),
    )
    .await
    .expect("minimal_budget hung on a sub-100-byte threshold")
    .expect("search");
    assert!(outcome.succeeds_at >= 50, "{outcome:?}");
    assert!(outcome.refused_at < 50, "{outcome:?}");
    assert!(
        outcome.refused_at >= outcome.succeeds_at.saturating_sub(1),
        "{outcome:?}"
    );
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
