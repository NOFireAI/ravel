//! Smoke test for the `l1_listing_charge` bench (issues #4393 and #4433): the
//! library entry at two small caps on a reduced series count, its bands for the
//! listing and the sample read, and each binary search's answer re-checked
//! against fresh reads.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::cell::Cell;

use ravel_bench::l1_listing_charge::{
    CAP_FIGURES, CAP_FLAGS, Compacted, ListingChargeError, PartShape, RunConfig, build_bucket,
    check_report, compact_at, engine_listing_succeeds, fetcher_sample_read, held_fetch_bound,
    isolate_part, listing_charge, listing_succeeds, minimal_budget, part_shape, report_json, run,
    sample_read_bound, sample_read_succeeds, upper_bound,
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

    for figure in CAP_FIGURES.iter().chain(CAP_FLAGS.iter()) {
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
        let cap = figures.cap_bytes;
        assert!(figures.full_parts >= 2, "cap {cap}");
        let sample_peak = figures.sample_search.succeeds_at;
        let listing_peak = figures.listing_search.succeeds_at;
        // 64 B of decoded provenance columns and a 32 B `SampleProvenance`
        // for every sample: the part carries per-sample provenance.
        assert!(
            figures.sample_search.refused_at >= 96 * figures.shape.samples,
            "cap {cap}: the largest refused sample-read budget, {} B, is below 96 B for each \
             of {} samples",
            figures.sample_search.refused_at,
            figures.shape.samples
        );
        for (path, search) in [
            ("sample read", figures.sample_search),
            ("listing", figures.listing_search),
        ] {
            assert!(
                search.refused_at >= one_percent_below(search.succeeds_at),
                "cap {cap}: the {path} search stopped {} B below {} B, coarser than 1 %",
                search.succeeds_at - search.refused_at,
                search.succeeds_at
            );
        }
        // The listing's peak is its charge: the search starts there and every
        // lower limit it tries is refused.
        assert_eq!(listing_peak, figures.listing_charge_bytes, "cap {cap}");

        // A fresh compaction of the same inputs gives the same part; read it
        // again on each path at the reported limit and 1 % below it.
        let compacted = compact_at(&bucket, &config, cap).await.expect("compact");
        let part = &compacted.parts[figures.part_index];
        let shape = part_shape(&compacted.store, part).await.expect("shape");
        assert_eq!(shape.samples, figures.shape.samples);
        assert_eq!(shape.series, figures.shape.series);
        let store = &compacted.store;
        let series = shape.series;
        assert!(
            sample_read_succeeds(store, part, series, sample_peak)
                .await
                .expect("read"),
            "cap {cap}: the sample read must succeed at the reported peak {sample_peak} B"
        );
        assert!(
            !sample_read_succeeds(store, part, series, one_percent_below(sample_peak))
                .await
                .expect("read"),
            "cap {cap}: the sample read must be refused 1 % below the reported peak \
             {sample_peak} B"
        );
        // The issue's formula leaves out the bytes the GETs hold, so a budget
        // of exactly that figure is refused, while the sample-read bound is not.
        assert!(
            !sample_read_succeeds(store, part, series, upper_bound(&shape))
                .await
                .expect("read"),
            "cap {cap}: a budget of the (64 + 32) * samples + catalog formula must be refused"
        );
        assert!(
            sample_read_succeeds(store, part, series, sample_read_bound(&shape))
                .await
                .expect("read")
        );

        let isolated = isolate_part(&config, cap, &compacted, figures.part_index)
            .await
            .expect("isolate");
        let only = &isolated.store;
        assert!(
            listing_succeeds(only, &config, series, listing_peak)
                .await
                .expect("list"),
            "cap {cap}: the listing must succeed at the reported peak {listing_peak} B"
        );
        assert!(
            !listing_succeeds(only, &config, series, one_percent_below(listing_peak))
                .await
                .expect("list"),
            "cap {cap}: the listing must be refused 1 % below the reported peak {listing_peak} B"
        );
    }
}

/// A bucket compacted into one part, and that part's shape.
async fn one_part() -> (RunConfig, Compacted, PartShape) {
    let one_part = u64::MAX >> 1;
    let config = small_config(vec![one_part]);
    let bucket = build_bucket(&config).expect("bucket");
    let compacted = compact_at(&bucket, &config, one_part)
        .await
        .expect("compact");
    assert_eq!(compacted.parts.len(), 1);
    let shape = part_shape(&compacted.store, &compacted.parts[0])
        .await
        .expect("shape");
    // Above the whole-object threshold: the open GET holds the 64 KiB tail and
    // the catalog run, not the part's pages.
    assert_eq!(
        (shape.wire_bytes, shape.held_fetch_bytes, shape.catalog_decode_bytes),
        (1_202_303, 69_014, 242_523)
    );
    assert_eq!((shape.samples, shape.series), (230_400, 120));
    (config, compacted, shape)
}

/// The listing path's peak, to the byte, on a one-part bucket: the bench's
/// listing read (`listing_succeeds` over the store `isolate_part` returns)
/// first succeeds at exactly [`listing_charge`], the held fetch bytes plus the
/// inflated catalog sections, and `QueryEngine::resolve_series` called
/// directly succeeds there and is refused one byte below.
///
/// FLIPS: a `listing_succeeds` that reads through
/// `SegmentFetcher::fetch_series` moves the threshold to the sample-read bound,
/// 22,429,937 B; `CatalogUse::Samples` in `list_series_phase_accounted` does
/// the same through the engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_peak_is_the_held_fetch_and_catalog_bytes_through_resolve_series() {
    let (config, compacted, shape) = one_part().await;
    let isolated = isolate_part(&config, u64::MAX >> 1, &compacted, 0)
        .await
        .expect("isolate");
    let store = &isolated.store;
    let series = shape.series;
    let config = &config;
    // The upper end is the sample read's bound, so a listing that charged the
    // provenance would still be bracketed and fail the pin below.
    let peak = exact_threshold(1, sample_read_bound(&shape), |limit| async move {
        listing_succeeds(store, config, series, limit)
            .await
            .expect("list")
    })
    .await;
    assert_eq!(peak, listing_charge(&shape));
    assert_eq!(peak, 69_014 + 242_523);
    assert_eq!(peak, 311_537);
    for (limit, expected) in [(peak, Some(series as usize)), (peak - 1, None)] {
        assert_eq!(
            engine_listing_succeeds(store, config, limit)
                .await
                .expect("engine listing"),
            expected,
            "resolve_series under {limit} B"
        );
    }
}

/// The sample-read path's peak, to the byte, on the same one-part bucket: the
/// bench's sample read (`sample_read_succeeds`) first succeeds at exactly
/// [`sample_read_bound`], the listing's charge plus 96 B of provenance for
/// every sample, and `SegmentFetcher::fetch_series` called directly succeeds
/// there and is refused one byte below.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sample_read_peak_adds_the_provenance_through_fetch_series() {
    let (_config, compacted, shape) = one_part().await;
    let store = &compacted.store;
    let part = &compacted.parts[0];
    let series = shape.series;
    let peak = exact_threshold(
        upper_bound(&shape),
        sample_read_bound(&shape),
        |limit| async move {
            sample_read_succeeds(store, part, series, limit)
                .await
                .expect("read")
        },
    )
    .await;
    assert_eq!(peak, sample_read_bound(&shape));
    assert_eq!(peak, listing_charge(&shape) + 96 * 230_400);
    assert_eq!(peak, 22_429_937);
    for (limit, expected) in [(peak, Some(series as usize)), (peak - 1, None)] {
        assert_eq!(
            fetcher_sample_read(store, part, limit)
                .await
                .expect("fetch_series"),
            expected,
            "fetch_series under {limit} B"
        );
    }
}

/// On one part, the listing's peak is at most a twentieth of the sample
/// read's, each found by the bench's own search. Measured: 311,537 B against
/// 22,429,937 B, a 72x gap.
///
/// FLIPS: reverting #4433 (`CatalogUse::Samples` in
/// `list_series_phase_accounted`, or the cherry-picked commit as a whole)
/// makes the listing charge the provenance too, and the two peaks equal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_peak_is_at_most_a_twentieth_of_the_sample_read_peak_on_one_part() {
    let (config, compacted, shape) = one_part().await;
    let part = &compacted.parts[0];
    let store = &compacted.store;
    let series = shape.series;
    let sample = minimal_budget(sample_read_bound(&shape), |limit| {
        sample_read_succeeds(store, part, series, limit)
    })
    .await
    .expect("sample-read search");
    let isolated = isolate_part(&config, u64::MAX >> 1, &compacted, 0)
        .await
        .expect("isolate");
    let only = &isolated.store;
    let config = &config;
    let listing = minimal_budget(listing_charge(&shape), |limit| {
        listing_succeeds(only, config, series, limit)
    })
    .await
    .expect("listing search");
    eprintln!(
        "one part: listing peak {} B, sample-read peak {} B",
        listing.succeeds_at, sample.succeeds_at
    );
    assert!(
        listing.succeeds_at.saturating_mul(20) <= sample.succeeds_at,
        "the listing peak {} B is more than 1/20 of the sample-read peak {} B",
        listing.succeeds_at,
        sample.succeeds_at
    );
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
    let fails_alone = |doc: &str, prefix: &str| {
        let failures = check_report(doc, &caps).expect_err(prefix);
        assert!(
            matches!(failures.as_slice(), [only] if only.starts_with(prefix)),
            "expected one failure starting {prefix:?}: {failures:?}"
        );
    };
    let cap = caps[0];
    // (peak, refused, per sample, bound, bound succeeds) for each path.
    for (path, peak, refused, per_sample, bound, succeeds) in [
        (
            "listing",
            "listing_peak_bytes",
            "listing_refused_at_bytes",
            "listing_peak_per_sample_bytes",
            "listing_charge_bytes",
            "listing_charge_succeeds",
        ),
        (
            "sample-read",
            "sample_peak_bytes",
            "sample_refused_at_bytes",
            "sample_peak_per_sample_bytes",
            "sample_read_bound_bytes",
            "sample_read_bound_succeeds",
        ),
    ] {
        let missing = edit(&|e| {
            e.remove(peak);
        });
        let failures = check_report(&missing, &caps).expect_err("missing");
        assert!(
            failures.contains(&format!("cap {cap}: {peak} missing"))
                && failures.contains(&format!("{peak}: emitted 0 times for 1 caps")),
            "{failures:?}"
        );
        let out_of_band = edit(&|e| {
            e.insert(per_sample.into(), 250.0.into());
        });
        fails_alone(&out_of_band, &format!("cap {cap}: {path} peak per sample 250"));
        let refused_above_bound = edit(&|e| {
            let at = e[bound].as_u64().expect("bound");
            let top = e[peak].as_u64().expect("peak").max(at + 1);
            e.insert(peak.into(), top.into());
            e.insert(refused.into(), at.into());
        });
        let failures = check_report(&refused_above_bound, &caps).expect_err("refused");
        assert!(
            failures
                .iter()
                .any(|f| f.starts_with(&format!("cap {cap}: the {path} was refused a "))),
            "{failures:?}"
        );
        let bound_refused = edit(&|e| {
            e.insert(succeeds.into(), false.into());
        });
        fails_alone(
            &bound_refused,
            &format!("cap {cap}: the {path} does not succeed under its bound, {bound}"),
        );
        let coarse = edit(&|e| {
            let top = e[peak].as_u64().expect("peak");
            e.insert(refused.into(), (top - top / 50).into());
        });
        fails_alone(&coarse, &format!("cap {cap}: the {path} refused at "));
    }

    // A key written twice in one entry parses as its last value, so only the
    // raw-text count sees it.
    let duplicated = text.replacen("\"samples\":", "\"samples\": 1,\n      \"samples\":", 1);
    assert!(serde_json::from_str::<serde_json::Value>(&duplicated).is_ok());
    assert!(check_report(&duplicated, &caps).is_err());

    // Two caps whose peak falls on one path as samples grow.
    for (path, peak) in [
        ("listing", "listing_peak_bytes"),
        ("sample-read", "sample_peak_bytes"),
    ] {
        let mut doc = json.clone();
        let mut second = doc["caps"][0].clone();
        let samples = second["samples"].as_u64().expect("samples");
        let top = second[peak].as_u64().expect("peak");
        second["cap_bytes"] = (CAPS[1]).into();
        second["samples"] = (samples * 2).into();
        second[peak] = (top - 1).into();
        doc["caps"].as_array_mut().expect("caps").push(second);
        let falling = serde_json::to_string_pretty(&doc).expect("render");
        let failures = check_report(&falling, &CAPS).expect_err("peak fell");
        assert!(
            failures
                .iter()
                .any(|f| f.starts_with(&format!("{path} peak fell"))),
            "{failures:?}"
        );
    }
}

/// The budget limit at which `read` starts to succeed, to the byte: refused at
/// `refused`, succeeding at `succeeds`.
async fn exact_threshold<F, Fut>(mut refused: u64, mut succeeds: u64, mut read: F) -> u64
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    assert!(!read(refused).await, "refused at {refused} B");
    assert!(read(succeeds).await, "succeeds at {succeeds} B");
    while succeeds - refused > 1 {
        let mid = refused + (succeeds - refused) / 2;
        if read(mid).await {
            succeeds = mid;
        } else {
            refused = mid;
        }
    }
    succeeds
}

/// `held_fetch_bound` equals, to the byte, what the fetcher holds on a real
/// part on each side of the whole-object threshold: the sample read's exact
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
        let series = shape.series;
        let threshold = exact_threshold(
            upper_bound(&shape),
            sample_read_bound(&shape),
            |limit| async move {
                sample_read_succeeds(store, part, series, limit)
                    .await
                    .expect("read")
            },
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

/// Sets `key` of the one cap entry of `json` to `value` and checks the result.
fn check_with(
    json: &serde_json::Value,
    caps: &[u64],
    key: &str,
    value: serde_json::Value,
) -> Result<(), Vec<String>> {
    let mut doc = json.clone();
    doc["caps"][0]
        .as_object_mut()
        .expect("cap entry")
        .insert(key.into(), value);
    check_report(&serde_json::to_string_pretty(&doc).expect("render"), caps)
}

/// Asserts `failures` is one per-sample band failure of `path` for `wrong`.
fn only_band_failure(failures: &[String], cap: u64, path: &str, wrong: f64, band: &str) {
    assert!(
        matches!(failures, [only] if only.starts_with(&format!(
            "cap {cap}: {path} peak per sample {}",
            wrong.trunc()
        )) && only.ends_with(&format!("B outside {band}"))),
        "{wrong} B a sample must fail the {path} band alone: {failures:?}"
    );
}

/// The sample-read peak-per-sample band fails when the full 96 B of per-sample
/// provenance is not charged, or is charged twice. The old [1, 200] B band
/// admitted both. The wrong figures are the real run's own figure, and the
/// default caps' measured 97.03 and 98.30 B, with the 64 B of columns dropped
/// (the 32 B record only) or with another 96 B on top.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sample_peak_per_sample_band_rejects_missing_and_doubled_provenance() {
    let caps = [CAPS[0]];
    let report = run(small_config(caps.to_vec())).await.expect("run");
    let json = report_json(&report);
    let text = serde_json::to_string_pretty(&json).expect("render");
    check_report(&text, &caps).expect("the real per-sample charge is inside the band");
    let real = report.caps[0].sample_peak_per_sample();

    let key = "sample_peak_per_sample_bytes";
    for measured in [real, 97.03, 98.30] {
        check_with(&json, &caps, key, measured.into())
            .expect("a measured figure is inside the band");
        for wrong in [measured - 64.0, measured + 96.0] {
            let failures = check_with(&json, &caps, key, wrong.into()).expect_err("outside");
            only_band_failure(&failures, caps[0], "sample-read", wrong, "[96, 130]");
        }
    }
}

/// The listing peak-per-sample band fails when the listing charges provenance
/// again, the 32 B records alone or the full 96 B, and when it charges only
/// the held fetch bytes without the catalog decode. The wrong figures are the
/// real run's own figure and the default caps' measured 1.03 and 2.30 B with
/// 32 or 96 B on top, and the 64 MiB cap's held fetch bytes alone, 78,022 B
/// over 7,447,680 samples.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_peak_per_sample_band_rejects_a_provenance_charge() {
    let caps = [CAPS[0]];
    let report = run(small_config(caps.to_vec())).await.expect("run");
    let json = report_json(&report);
    let text = serde_json::to_string_pretty(&json).expect("render");
    check_report(&text, &caps).expect("the real per-sample charge is inside the band");
    let real = report.caps[0].listing_peak_per_sample();

    let key = "listing_peak_per_sample_bytes";
    for measured in [real, 1.03, 2.30] {
        check_with(&json, &caps, key, measured.into())
            .expect("a measured figure is inside the band");
        for wrong in [measured + 32.0, measured + 96.0] {
            let failures = check_with(&json, &caps, key, wrong.into()).expect_err("outside");
            only_band_failure(&failures, caps[0], "listing", wrong, "[1, 16]");
        }
    }
    let fetch_only = 78_022.0 / 7_447_680.0;
    let failures = check_with(&json, &caps, key, fetch_only.into()).expect_err("below");
    only_band_failure(&failures, caps[0], "listing", fetch_only, "[1, 16]");
}

/// Each path's reported peak sits at or below its bound on a real run, and
/// `check_report` compares the two directly: a peak one byte above the bound
/// fails even when the refused limit stays below the bound, the 1 % bracket
/// holds and the read under the bound succeeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peak_above_its_bound_fails_the_direct_check() {
    let report = run(small_config(CAPS.to_vec())).await.expect("run");
    for entry in &report.caps {
        assert!(
            entry.sample_search.succeeds_at <= entry.sample_read_bound_bytes,
            "cap {}: the reported sample-read peak {} B is above its bound {} B",
            entry.cap_bytes,
            entry.sample_search.succeeds_at,
            entry.sample_read_bound_bytes
        );
        assert!(
            entry.listing_search.succeeds_at <= entry.listing_charge_bytes,
            "cap {}: the reported listing peak {} B is above its bound {} B",
            entry.cap_bytes,
            entry.listing_search.succeeds_at,
            entry.listing_charge_bytes
        );
    }

    let caps = [CAPS[0]];
    let report = run(small_config(caps.to_vec())).await.expect("run");
    let json = report_json(&report);
    let text = serde_json::to_string_pretty(&json).expect("render");
    check_report(&text, &caps).expect("the real report holds every band");

    for (path, peak, refused, bound, succeeds) in [
        (
            "listing",
            "listing_peak_bytes",
            "listing_refused_at_bytes",
            "listing_charge_bytes",
            "listing_charge_succeeds",
        ),
        (
            "sample-read",
            "sample_peak_bytes",
            "sample_refused_at_bytes",
            "sample_read_bound_bytes",
            "sample_read_bound_succeeds",
        ),
    ] {
        let at = json["caps"][0][bound].as_u64().expect("bound");
        let mut doc = json.clone();
        let entry = doc["caps"][0].as_object_mut().expect("cap entry");
        entry.insert(peak.into(), (at + 1).into());
        entry.insert(refused.into(), (at - 1).into());
        entry.insert(succeeds.into(), true.into());
        let violated = serde_json::to_string_pretty(&doc).expect("render");
        assert_eq!(
            check_report(&violated, &caps),
            Err(vec![format!(
                "cap {}: the {path} peak {} B is above its bound {at} B",
                caps[0],
                at + 1
            )])
        );
    }
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
