#![allow(clippy::expect_used)]

use super::figures::{
    self, Bands, Figure, Key, OBJECT_BODY, Prices, Scope, check, parse_report, write_report,
};
use super::scenarios::{POSITIONS, percentile, run_scenario, worst_case_passes};
use super::{DISCOVERY, SCENARIOS, Size, load_bands, load_prices};

fn small(name: &str) -> Vec<Figure> {
    run_scenario(name, &Size::Small.config()).expect("scenario runs")
}

fn bands() -> Bands {
    load_bands().expect("prereg.md parses")
}

/// One scenario's figures against prereg.md: every figure inside its band,
/// every exact row for the scenario matched once.
fn assert_scenario(name: &str, figures: &[Figure]) {
    let checked = check(figures, &bands(), Size::Small, false, Scope::Scenario(name));
    assert!(
        checked.failures.is_empty(),
        "{name}: {:#?}",
        checked.failures
    );
    assert!(!figures.is_empty(), "{name} printed no figure");
}

fn value(figures: &[Figure], key: &str) -> f64 {
    let wanted = Key::parse(key).expect("key parses");
    let found: Vec<&Figure> = figures.iter().filter(|f| f.key == wanted).collect();
    assert_eq!(found.len(), 1, "{key} printed {} times", found.len());
    found[0].value
}

fn assert_value(figures: &[Figure], key: &str, expected: f64) {
    let got = value(figures, key);
    assert_eq!(
        got.to_bits(),
        expected.to_bits(),
        "{key}: {got} != {expected}"
    );
}

#[test]
fn hot_scenario_inside_its_bands() {
    let figures = small("hot");
    assert_scenario("hot", &figures);
    // 3 concurrent sends a round: 3 conflicts per 4 messages.
    assert_value(&figures, "hot.warm.message.publish.conflict", 0.75);
    assert_value(&figures, "hot.cold.message.publish.cas", 1.75);
    assert_value(&figures, "hot.warm.step.store.conflict", 1.0);
}

#[test]
fn slow_scenario_inside_its_bands() {
    let figures = small("slow");
    assert_scenario("slow", &figures);
    assert!(value(&figures, "slow.warm.message.latency.p50_ms") >= 500.0);
}

#[test]
fn many_scenario_inside_its_bands() {
    let figures = small("many");
    assert_scenario("many", &figures);
    assert_value(&figures, "many.warm.activation.ops.count", 6.0);
    assert_value(&figures, "many.cold.activation.ops.count", 12.0);
}

#[test]
fn dormant_agents_see_no_request() {
    let figures = small("dormant");
    assert_scenario("dormant", &figures);
    assert_value(&figures, "dormant.warm.run.isolation.dormant_requests", 0.0);
    assert_value(&figures, "dormant.cold.run.isolation.dormant_requests", 0.0);
}

#[test]
fn index_scenarios_inside_their_bands() {
    let p64 = small("index-p64");
    assert_scenario("index-p64", &p64);
    let p2 = small("index-p2");
    assert_scenario("index-p2", &p2);
    // At page size 2 a full page seals when the next entry arrives: 5 seals
    // over 12 messages. None at 64.
    assert_value(&p2, "index-p2.warm.message.store.put", 1.417);
    assert_value(&p64, "index-p64.warm.message.store.put", 1.0);
    assert_value(&p2, "index-p2.warm.message.index.page_gets", 0.0);
    assert_value(&p2, "index-p2.cold.turn.index.page_gets", 0.0);
}

#[test]
fn storm_scenario_inside_its_bands() {
    let figures = small("storm");
    assert_scenario("storm", &figures);
    assert_value(&figures, "storm.cold.pass.resolve.get", 8.0);
    assert_value(&figures, "storm.cold.activation.ops.count", 8.0);
}

#[test]
fn baseline_issues_no_agents_request() {
    let figures = small("baseline");
    assert_scenario("baseline", &figures);
    assert_value(&figures, "baseline.na.run.isolation.agents_requests", 0.0);
    assert_value(&figures, "baseline.na.ingest.telemetry.put", 1.0);
}

#[test]
fn discovery_stays_within_the_worst_case() {
    let figures = small(DISCOVERY);
    for position in POSITIONS {
        let name = format!("{DISCOVERY}-n100-{}", position.name());
        assert_scenario(&name, &figures);
        let passes = value(&figures, &format!("{name}.cold.assign.sweep.passes"));
        let delay = value(&figures, &format!("{name}.cold.assign.latency.delay_ms"));
        let worst = worst_case_passes(100) as f64;
        assert!(passes <= worst, "{name}: {passes} passes");
        assert!(delay <= worst * 5000.0, "{name}: {delay} ms");
    }
    assert_eq!(worst_case_passes(100), 2);
    assert_eq!(worst_case_passes(10_000), 157);
}

/// The acceptance check: a whole small run, every figure inside its band
/// and printed once, every exact row matched, every wildcard row deciding a
/// figure; and per operation, the request counts each phase issued.
#[test]
fn small_run_meets_the_preregistered_bands() {
    let outcome = super::run(Size::Small).expect("run");
    assert!(
        outcome.checked.failures.is_empty(),
        "{:#?}",
        outcome.checked.failures
    );
    let f = &outcome.figures;
    for (key, expected) in [
        // message: HEAD read, payload PUT, HEAD CAS (warm, uncontended).
        ("many.warm.message.resolve.get", 1.5),
        ("many.warm.message.store.put", 1.0),
        ("many.warm.message.publish.cas", 1.0),
        ("many.cold.message.resolve.get", 2.0),
        // turn: poll and commit, a takeover on every cold turn.
        ("many.warm.turn.resolve.get", 2.5),
        ("many.cold.turn.resolve.get", 3.0),
        ("many.cold.turn.publish.cas", 3.0),
        ("many.warm.turn.store.put", 2.0),
        ("many.warm.turn.store.get", 1.5),
        // step.
        ("many.warm.step.publish.cas", 5.0),
        ("many.warm.step.store.conflict", 1.0),
        // activation.
        ("many.cold.activation.resolve.get", 2.0),
        ("many.cold.activation.publish.cas", 1.0),
        // no LIST on any hot path.
        ("many.warm.message.resolve.list", 0.0),
        ("many.warm.turn.resolve.list", 0.0),
        ("many.warm.step.resolve.list", 0.0),
        // storm pass and discovery.
        ("storm.cold.pass.resolve.get", 8.0),
        ("discovery-n100-end.cold.assign.sweep.passes", 2.0),
        ("discovery-n100-end.cold.assign.resolve.list", 4.0),
        ("discovery-n100-end.cold.assign.resolve.get", 102.0),
    ] {
        assert_value(f, key, expected);
    }
    for verdict in &outcome.checked.verdicts {
        assert!(verdict.inside, "{:?}", verdict.figure.key);
        assert!(verdict.row.is_some(), "{:?}", verdict.figure.key);
    }
}

#[test]
fn the_report_round_trips() {
    let figures = small("baseline");
    let report = write_report(&figures);
    assert_eq!(report.lines().count(), figures.len());
    assert_eq!(parse_report(&report), figures);
    // Dollar lines, the stamp and unrelated output are not figures.
    let prices = load_prices().expect("prices");
    let mixed = format!(
        "agents-bench-stamp revision=x\n{report}{}noise\n",
        figures::dollar_lines(&figures, &prices)
    );
    assert_eq!(parse_report(&mixed), figures);
}

/// Pooling the store phases into one counter must fail the check: the
/// bands are per phase, and a pooled figure has no band but the zero row.
#[test]
fn pooled_phases_fail_the_check() {
    let figures = small("hot");
    let bands = bands();
    let scope = Scope::Scenario("hot");
    assert!(
        check(&figures, &bands, Size::Small, false, scope)
            .failures
            .is_empty()
    );

    let mut pooled: Vec<Figure> = Vec::new();
    for figure in &figures {
        if ["resolve", "store", "publish"].contains(&figure.key.phase.as_str()) {
            let mut key = figure.key.clone();
            key.phase = "all".to_string();
            match pooled.iter_mut().find(|p| p.key == key) {
                Some(p) => p.value += figure.value,
                None => pooled.push(Figure {
                    key,
                    ..figure.clone()
                }),
            }
        } else {
            pooled.push(figure.clone());
        }
    }
    let checked = check(&pooled, &bands, Size::Small, false, scope);
    assert!(
        checked
            .failures
            .iter()
            .any(|f| f.starts_with("hot.warm.message.all.get:")),
        "{:#?}",
        checked.failures
    );
}

/// A byte figure must name which bytes it counts.
#[test]
fn a_byte_figure_without_a_kind_fails_the_check() {
    let mut figures = small("baseline");
    let bands = bands();
    let scope = Scope::Scenario("baseline");
    assert!(
        check(&figures, &bands, Size::Small, false, scope)
            .failures
            .is_empty()
    );

    let target = Key::parse("baseline.na.ingest.telemetry.body_out_bytes").expect("key");
    let figure = figures
        .iter_mut()
        .find(|f| f.key == target)
        .expect("byte figure");
    assert_eq!(figure.kind.as_deref(), Some(OBJECT_BODY));
    figure.kind = None;
    let checked = check(&figures, &bands, Size::Small, false, scope);
    assert_eq!(
        checked.failures,
        vec!["baseline.na.ingest.telemetry.body_out_bytes: byte figure without kind".to_string()]
    );
}

#[test]
fn a_missing_or_doubled_figure_fails_the_check() {
    let figures = small("baseline");
    let bands = bands();
    let scope = Scope::Scenario("baseline");
    let count = Key::parse("baseline.na.ingest.ops.count").expect("key");

    let missing: Vec<Figure> = figures.iter().filter(|f| f.key != count).cloned().collect();
    let checked = check(&missing, &bands, Size::Small, false, scope);
    assert!(
        checked
            .failures
            .iter()
            .any(|f| f.contains("baseline.na.ingest.ops.count matched 0 figures")),
        "{:#?}",
        checked.failures
    );

    let mut doubled = figures.clone();
    doubled.extend(figures.iter().filter(|f| f.key == count).cloned());
    let checked = check(&doubled, &bands, Size::Small, false, scope);
    assert!(
        checked
            .failures
            .iter()
            .any(|f| f == "baseline.na.ingest.ops.count: printed 2 times, expected once"),
        "{:#?}",
        checked.failures
    );
}

#[test]
fn every_scenario_has_a_runner() {
    let config = Size::Small.config();
    assert!(run_scenario("no-such", &config).is_err());
    assert_eq!(SCENARIOS.len(), 8);
}

#[test]
fn percentiles_are_nearest_rank() {
    let sorted = [10.0, 20.0, 30.0, 40.0];
    assert_eq!(percentile(&sorted, 0.5).to_bits(), 20.0f64.to_bits());
    assert_eq!(percentile(&sorted, 0.99).to_bits(), 40.0f64.to_bits());
    assert_eq!(percentile(&[], 0.5).to_bits(), 0.0f64.to_bits());
}

#[test]
fn prices_are_assumptions_with_a_date_and_source() {
    let prices = load_prices().expect("prices.toml parses");
    assert_eq!(prices.label, "assumption");
    assert_eq!(prices.date, "2026-10-29");
    assert!(prices.source.contains("aws.amazon.com/s3/pricing"));
    let measured = "label = \"measured\"\ndate = \"2026-10-29\"\nsource = \"x\"\n";
    assert!(Prices::parse(measured).is_err());
}
