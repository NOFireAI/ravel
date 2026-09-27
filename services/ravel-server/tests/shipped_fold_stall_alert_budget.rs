//! ADR-1306 follow-up task 4: the shipped `RavelCatalogFoldStalled` alert and
//! the derived per-query S3 request budget are one coupled pair, and this is
//! the test that keeps them coupled.
//!
//! Decision 2's ordering guarantee ("a stalled fold pages before it refuses
//! any query") rests on two numbers agreeing. The budget is sized from
//! `covered_span`, whose lag half is
//! `seal_margin + FOLD_STALL_ALERT_FOR + ALERT_DELIVERY_SLACK` (ADR-1306
//! decision 1). The alert is what actually delivers the page, and its own
//! threshold and `for:` live in a YAML file an operator loads verbatim. Raise
//! either of those in the file without raising `lag_allowance` and the budget
//! starts refusing queries before the page goes out, with nothing in the Rust
//! tree changed and no test to notice; the Consequences section names exactly
//! that as what this test must catch.
//!
//! So the assertions read the shipped file, not a constant restating it. The
//! rule is located and its `expr` and `for:` are read off the parsed block,
//! the way `shipped_rules_name_emitted_metrics.rs` reads that file: at
//! runtime through `env!("CARGO_MANIFEST_DIR")`, never `include_str!` across
//! the crate boundary, which would bake a copy into this binary and keep
//! passing against a file that had since changed.
//!
//! This reads ONE rule, so it parses only the block that rule occupies rather
//! than the whole document. The shape checks over the whole file (every group,
//! every rule, every `for:` against Prometheus's duration grammar, every
//! `expr` against Ravel's PromQL parser) are
//! `shipped_rules_name_emitted_metrics.rs`'s job and are not repeated here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

/// The shipped rule file, relative to this crate's manifest directory.
const RULES_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/prometheus/ravel.rules.yaml"
);

/// The alert this test is about.
const ALERT: &str = "RavelCatalogFoldStalled";

/// The gauge the alert reads. Its staleness is what the threshold compares.
const GAUGE: &str = "ravel_catalog_fold_last_success_timestamp_seconds";

/// The two fields of the shipped rule this test holds to the budget.
#[derive(Debug)]
struct FoldStallRule {
    expr: String,
    fires_after: String,
}

fn indent_of(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

/// Reads the `expr:` and `for:` of the one `- alert: <ALERT>` rule in `text`.
///
/// Every failure is an error rather than a default: a rule that is absent,
/// that appears twice, or that carries neither field must fail this test, not
/// leave it asserting over an empty string. That is the same reason the
/// counts in `shipped_rules_name_emitted_metrics.rs` are pinned literals.
fn parse_fold_stall_rule(text: &str) -> Result<FoldStallRule, String> {
    if text.contains('\t') {
        return Err("a tab appears in the file; YAML forbids tabs in indentation".to_string());
    }
    let lines: Vec<&str> = text.lines().collect();
    let marker = format!("- alert: {ALERT}");
    let starts: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.trim() == marker)
        .map(|(at, _)| at)
        .collect();
    if starts.len() != 1 {
        return Err(format!(
            "expected exactly one `{marker}` line, found {}",
            starts.len()
        ));
    }
    let start = starts[0];
    // A sequence item's own fields sit two columns in from its `- ` marker.
    let field_indent = indent_of(lines[start]) + 2;

    let mut expr: Option<String> = None;
    let mut fires_after: Option<String> = None;
    let mut at = start + 1;
    while at < lines.len() {
        let line = lines[at];
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            at += 1;
            continue;
        }
        if indent_of(line) < field_indent || trimmed.starts_with("- ") {
            break;
        }
        if indent_of(line) > field_indent {
            at += 1;
            continue;
        }
        let Some((key, after)) = trimmed.split_once(':') else {
            return Err(format!("line {}: not a mapping key: {trimmed:?}", at + 1));
        };
        let after = after.trim();
        at += 1;
        match key {
            "expr" => {
                if after != "|" {
                    return Err(format!(
                        "`expr:` opens {after:?}; this reader takes the literal block scalar the \
                         shipped rule is written as"
                    ));
                }
                let mut body: Vec<&str> = Vec::new();
                while at < lines.len() {
                    if lines[at].trim().is_empty() {
                        body.push("");
                        at += 1;
                        continue;
                    }
                    if indent_of(lines[at]) <= field_indent {
                        break;
                    }
                    body.push(lines[at].trim_end());
                    at += 1;
                }
                while body.last().is_some_and(|line| line.is_empty()) {
                    body.pop();
                }
                if body.is_empty() {
                    return Err("`expr:` opened an empty block scalar".to_string());
                }
                let base = body
                    .iter()
                    .filter(|line| !line.is_empty())
                    .map(|line| indent_of(line))
                    .min()
                    .unwrap_or(0);
                let dedented: Vec<String> = body
                    .iter()
                    .map(|line| {
                        if line.is_empty() {
                            String::new()
                        } else {
                            line[base..].to_string()
                        }
                    })
                    .collect();
                expr = Some(dedented.join("\n"));
            }
            "for" => {
                if after.is_empty() {
                    return Err("`for:` carries no value".to_string());
                }
                fires_after = Some(after.to_string());
            }
            _ => {}
        }
    }

    Ok(FoldStallRule {
        expr: expr.ok_or_else(|| format!("alert {ALERT} carries no `expr`"))?,
        fires_after: fires_after.ok_or_else(|| format!("alert {ALERT} carries no `for:`"))?,
    })
}

/// The right-hand side of the one `>` comparison in `expr`, in seconds.
///
/// Exactly one, and it is what makes the alert's threshold a number this test
/// can compare: a second comparison would mean the rule fires on a condition
/// this test does not read, and zero would mean the threshold moved somewhere
/// the scan does not see. `>=`, `<=` and `!=` are not matched, so a rewrite
/// into one of those forms fails here rather than being read as the same
/// threshold.
fn threshold_seconds(expr: &str) -> Result<u64, String> {
    let bytes = expr.as_bytes();
    let mut found: Vec<u64> = Vec::new();
    for (at, byte) in bytes.iter().enumerate() {
        if *byte != b'>' {
            continue;
        }
        if bytes.get(at + 1) == Some(&b'=') {
            return Err("the expression holds a `>=` comparison this reader does not read".into());
        }
        if at > 0 && matches!(bytes[at - 1], b'=' | b'<' | b'!') {
            return Err(
                "the expression holds a compound comparison this reader does not read".to_string(),
            );
        }
        let rest = expr[at + 1..].trim_start();
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        let after = &rest[digits.len()..];
        // A `> 1.5` would otherwise read as `1`: the threshold has to be a
        // whole number of seconds with nothing continuing it.
        if digits.is_empty()
            || after
                .chars()
                .next()
                .is_some_and(|c| c == '.' || c.is_alphanumeric())
        {
            return Err(format!(
                "the `>` comparison is against {rest:?}, not an integer number of seconds"
            ));
        }
        found.push(digits.parse().map_err(|e| format!("{digits:?}: {e}"))?);
    }
    match found.len() {
        1 => Ok(found[0]),
        n => Err(format!("expected exactly one `>` comparison, found {n}")),
    }
}

/// A Prometheus `for:` duration as a `Duration`.
///
/// One or more `<number><unit>` pairs, the grammar
/// `shipped_rules_name_emitted_metrics.rs` validates the whole file against.
/// This one also has to produce the VALUE, which is what the budget
/// comparison needs.
fn parse_duration(text: &str) -> Result<Duration, String> {
    const UNITS: [(&str, u64); 6] = [
        ("ms", 0),
        ("s", 1),
        ("m", 60),
        ("h", 3_600),
        ("d", 86_400),
        ("w", 604_800),
    ];
    if text.is_empty() {
        return Err("a duration must not be empty".to_string());
    }
    let mut total = Duration::ZERO;
    let mut rest = text;
    while !rest.is_empty() {
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() {
            return Err(format!("{text:?}: expected a number at {rest:?}"));
        }
        rest = &rest[digits.len()..];
        let unit: String = rest.chars().take_while(char::is_ascii_alphabetic).collect();
        rest = &rest[unit.len()..];
        let (_, seconds) = UNITS
            .iter()
            .find(|(name, _)| *name == unit)
            .ok_or_else(|| format!("{text:?}: {unit:?} is not a duration unit"))?;
        if *seconds == 0 {
            return Err(format!(
                "{text:?}: a sub-second `for:` is not a value this comparison is written for"
            ));
        }
        let count: u64 = digits.parse().map_err(|e| format!("{digits:?}: {e}"))?;
        total += Duration::from_secs(count * seconds);
    }
    Ok(total)
}

fn shipped_fold_stall_rule() -> FoldStallRule {
    let text = std::fs::read_to_string(RULES_FILE)
        .unwrap_or_else(|e| panic!("shipped rule file {RULES_FILE} must be readable: {e}"));
    parse_fold_stall_rule(&text)
        .unwrap_or_else(|e| panic!("alert {ALERT} must be readable from {RULES_FILE}: {e}"))
}

/// The shipped alert's threshold and `for:` must both fit the lag half of the
/// span the derived request budget covers.
///
/// ADR-1306 decision 1 defines that half as
///
/// ```text
/// lag_allowance = seal_margin + FOLD_STALL_ALERT_FOR + ALERT_DELIVERY_SLACK
/// covered_span  = healthy_tail_max + lag_allowance
/// ```
///
/// so `covered_span - healthy_tail_max` is the lag allowance the derivation
/// really covers, taken off `ravel-query`'s own functions rather than
/// recomputed here from the three terms.
///
/// The three assertions, and what each one is for:
///
/// * The threshold equals the seal margin of the catalog this server folds
///   with. Decision 1 rests on that ("the alert's threshold is already the
///   seal margin, so those two terms are the time from the last successful
///   fold to the rule's condition holding for its `for:`"). A threshold above
///   the margin delays the page past what `lag_allowance` budgets for; one
///   below it pages on a fold that is running normally.
/// * The `for:` equals `FOLD_STALL_ALERT_FOR`, the term the derivation
///   budgets for the rule's own hold time. The constant is named for this
///   file's value; if the file moves and the constant does not, the budget is
///   sized for a page that arrives later than it assumes.
/// * Threshold plus `for:` plus `ALERT_DELIVERY_SLACK` is at most
///   `lag_allowance`. This is the ordering itself: the whole path from the
///   last successful fold to a page in an operator's hand has to fit inside
///   the lag the budget covers. It holds with equality today, which is the
///   tightest it can hold, so any rise in either shipped value fails here.
///
/// RED: raise the `> 4800` in the shipped file, or its `for: 10m`. Either
/// breaks its own equality, and the ordering assertion with it.
#[test]
fn shipped_fold_stall_alert_fits_the_budget_lag_allowance() {
    let rule = shipped_fold_stall_rule();
    assert!(
        rule.expr.contains(GAUGE),
        "alert {ALERT} must read {GAUGE}; the parsed expression is {:?}",
        rule.expr
    );

    let threshold = Duration::from_secs(
        threshold_seconds(&rule.expr)
            .unwrap_or_else(|e| panic!("alert {ALERT}'s threshold must be readable: {e}")),
    );
    let fires_after = parse_duration(&rule.fires_after)
        .unwrap_or_else(|e| panic!("alert {ALERT}'s `for:` must be a duration: {e}"));

    // The seal margin the server's own catalog folds and resolves with
    // (ADR-1306 decision 3), which is `CatalogConfig::default`'s 1h + 5m + 15m
    // today. Read off the server's catalog rather than restated, so the alert
    // is compared against the margin a running process really seals on.
    let seal_margin = ravel_server::query::server_seal_margin();
    assert_eq!(
        threshold,
        seal_margin.total(),
        "alert {ALERT} fires at a {threshold:?} staleness, but the catalog this server folds \
         with seals an ingest hour {:?} after it ends. ADR-1306 decision 1 makes the \
         threshold the seal margin: above it the page is later than the request budget \
         assumes, below it a healthy fold pages",
        seal_margin.total()
    );

    assert_eq!(
        fires_after,
        ravel_query::FOLD_STALL_ALERT_FOR,
        "alert {ALERT} holds its condition for {fires_after:?}, but the derived request \
         budget sizes the page's arrival with FOLD_STALL_ALERT_FOR = {:?}",
        ravel_query::FOLD_STALL_ALERT_FOR
    );

    // The lag half of the span the derivation covers, off ravel-query's own
    // functions: `covered_span = healthy_tail_max + lag_allowance`.
    let lag_allowance =
        ravel_query::covered_span(seal_margin) - ravel_query::healthy_tail_max(seal_margin);
    assert_eq!(
        lag_allowance,
        seal_margin.total() + ravel_query::FOLD_STALL_ALERT_FOR + ravel_query::ALERT_DELIVERY_SLACK,
        "sanity: `covered_span - healthy_tail_max` must be ADR-1306 decision 1's \
         `lag_allowance`, or the comparison below is against the wrong span"
    );

    let to_the_page = threshold + fires_after + ravel_query::ALERT_DELIVERY_SLACK;
    assert!(
        to_the_page <= lag_allowance,
        "the shipped alert takes {to_the_page:?} to reach an operator (threshold \
         {threshold:?} + for: {fires_after:?} + ALERT_DELIVERY_SLACK {:?}), past the \
         {lag_allowance:?} of fold lag the derived request budget covers. ADR-1306 \
         decision 2's ordering is broken: a query would be refused before the page",
        ravel_query::ALERT_DELIVERY_SLACK
    );
}

/// The parser has to report a missing or duplicated rule rather than return
/// something empty, since every assertion above runs over what it returns.
#[test]
fn the_rule_reader_refuses_an_absent_or_duplicated_rule() {
    const FIXTURE: &str = "\
groups:
  - name: ravel-catalog-fold
    rules:
      - alert: RavelCatalogFoldStalled
        expr: |
          time() - max by (signal) (
            ravel_catalog_fold_last_success_timestamp_seconds
          ) > 4800
        for: 10m
        labels:
          severity: critical
";
    let parsed = parse_fold_stall_rule(FIXTURE).expect("the fixture rule parses");
    assert_eq!(parsed.fires_after, "10m");
    assert_eq!(
        threshold_seconds(&parsed.expr).expect("the fixture threshold reads"),
        4_800
    );
    assert!(
        parsed.expr.starts_with("time() - max by (signal) ("),
        "the block scalar must come back dedented, got {:?}",
        parsed.expr
    );

    assert!(
        parse_fold_stall_rule(&FIXTURE.replace("RavelCatalogFoldStalled", "RavelSomethingElse"))
            .is_err(),
        "a file with no such alert must be an error, not an empty rule"
    );
    let doubled = format!("{FIXTURE}{}", FIXTURE.replace("groups:\n", ""));
    assert!(
        parse_fold_stall_rule(&doubled).is_err(),
        "two rules of the same name must be an error: this test would otherwise assert \
         over whichever one the scan reached first"
    );
}

/// The threshold reader must refuse the shapes it cannot compare, rather than
/// read a number out of one and pass.
#[test]
fn the_threshold_reader_refuses_shapes_it_cannot_compare() {
    assert_eq!(threshold_seconds("time() - gauge > 4800").unwrap(), 4_800);
    assert!(
        threshold_seconds("time() - gauge >= 4800").is_err(),
        "`>=` is a different condition and must not be read as the same threshold"
    );
    assert!(
        threshold_seconds("gauge > 4800 and other > 10").is_err(),
        "two comparisons mean the rule fires on a condition this reader does not read"
    );
    assert!(
        threshold_seconds("absent(gauge)").is_err(),
        "an expression with no threshold must be an error, not a zero"
    );
    assert!(
        threshold_seconds("time() - gauge > 1.5").is_err(),
        "a fractional threshold is not an integer number of seconds"
    );
}

/// The `for:` reader must produce the value, and refuse a unit it cannot
/// convert.
#[test]
fn the_duration_reader_converts_and_refuses() {
    assert_eq!(parse_duration("10m").unwrap(), Duration::from_secs(600));
    assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5_400));
    assert!(parse_duration("").is_err(), "an empty duration is an error");
    assert!(
        parse_duration("10 minutes").is_err(),
        "Prometheus has no `minutes` unit and no space before the unit"
    );
    assert!(
        parse_duration("500ms").is_err(),
        "a sub-second hold time is not a value this comparison is written for"
    );
}
