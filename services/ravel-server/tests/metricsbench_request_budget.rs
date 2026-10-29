//! `deploy/metricsbench/ravel.yml` states `--max-s3-requests` as an exact
//! count (ADR-0927's frozen limits: the derived default, stated, not raised),
//! while the server would derive that count from `--shards` and
//! `--max-flush-delay`. Nothing else ties the two: change the flush delay or
//! the shard count and leave the count alone, and the MetricsBench Ravel row
//! runs a budget the server would not derive, with every other check green.
//!
//! This reads the ravel-server command out of the compose file, parses it
//! with the server's own `Cli`, and requires the stated count to equal what
//! the same command derives with the count left out. It also requires the
//! README's frozen-limits row to state the same count. The files are read at
//! runtime through `env!("CARGO_MANIFEST_DIR")`, as
//! `shipped_fold_stall_alert_budget.rs` reads its rule file, so an edit to
//! either file is checked without rebuilding a baked-in copy.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_query::RequestLimit;
use ravel_server::Cli;

const COMPOSE_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/metricsbench/ravel.yml"
);

const README_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/metricsbench/README.md"
);

/// The README's frozen-limits row for the flag; its third cell is the value.
const README_ROW_PREFIX: &str = "| Ravel | `--max-s3-requests` | `";

/// The quoted list items under the `ravel-server` service's `command:`.
fn server_command(compose: &str) -> Result<Vec<String>, String> {
    let lines: Vec<&str> = compose.lines().collect();
    let services: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.trim_end() == "  ravel-server:")
        .map(|(at, _)| at)
        .collect();
    if services.len() != 1 {
        return Err(format!(
            "expected exactly one `  ravel-server:` service, found {}",
            services.len()
        ));
    }
    let service = services[0];
    let mut command = None;
    for (at, line) in lines.iter().enumerate().skip(service + 1) {
        if line.starts_with("  ") && !line.starts_with("   ") && !line.trim().is_empty() {
            break;
        }
        if line.trim_end() == "    command:" {
            command = Some(at);
            break;
        }
    }
    let command = command.ok_or("the ravel-server service has no `    command:` list")?;
    let mut args = Vec::new();
    for line in &lines[command + 1..] {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some(item) = line.strip_prefix("      - ") else {
            break;
        };
        let value = item
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
            .ok_or_else(|| format!("command item is not one quoted string: {line}"))?;
        args.push(value.to_string());
    }
    if args.is_empty() {
        return Err("the ravel-server command list is empty".to_string());
    }
    Ok(args)
}

/// The value cell of the README's one `--max-s3-requests` row.
fn readme_value(readme: &str) -> Result<u64, String> {
    let rows: Vec<&str> = readme
        .lines()
        .filter(|line| line.starts_with(README_ROW_PREFIX))
        .collect();
    if rows.len() != 1 {
        return Err(format!(
            "expected exactly one README row starting {README_ROW_PREFIX:?}, found {}",
            rows.len()
        ));
    }
    let rest = &rows[0][README_ROW_PREFIX.len()..];
    let end = rest
        .find('`')
        .ok_or("the README row's value cell is not closed")?;
    rest[..end].parse().map_err(|e| {
        format!(
            "the README row's value {:?} is not a count: {e}",
            &rest[..end]
        )
    })
}

/// Returns the stated count when the compose file states it, the server
/// derives the same count from the rest of that command, and the README
/// states it too.
fn check(compose: &str, readme: &str) -> Result<u64, String> {
    let args = server_command(compose)?;
    let mut cli = Cli::try_parse_from(std::iter::once("ravel-server".to_string()).chain(args))
        .map_err(|e| format!("the compose command does not parse as ravel-server's CLI: {e}"))?;
    let stated = cli
        .max_s3_requests
        .ok_or("the compose command does not state --max-s3-requests")?;
    cli.max_s3_requests = None;
    let flush = cli
        .resolve_flush_cadence()
        .map_err(|e| format!("the compose command's flush cadence does not resolve: {e}"))?
        .max_flush_delay;
    let derived = cli
        .resolve_max_s3_requests()
        .map_err(|e| format!("the derived budget does not resolve: {e}"))?;
    if derived != RequestLimit::Bounded(stated) {
        return Err(format!(
            "ravel.yml states --max-s3-requests {stated}, but the same command derives \
             {derived:?} at --shards {} and --max-flush-delay {flush:?}",
            cli.shards
        ));
    }
    let documented = readme_value(readme)?;
    if documented != stated {
        return Err(format!(
            "the README's frozen-limits row states {documented}, ravel.yml states {stated}"
        ));
    }
    Ok(stated)
}

fn read(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

/// Replaces exactly one occurrence of `from`, so a mutation that matched
/// nothing fails here instead of leaving the file unchanged and the
/// negative case passing for the wrong reason.
fn mutate(text: &str, from: &str, to: &str) -> String {
    assert_eq!(
        text.matches(from).count(),
        1,
        "mutation target {from:?} must occur exactly once"
    );
    text.replacen(from, to, 1)
}

#[test]
fn stated_request_budget_is_the_derivation_of_the_stated_command() {
    let compose = read(COMPOSE_FILE);
    let readme = read(README_FILE);
    let stated = check(&compose, &readme).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        stated, 1_358_600,
        "the count ADR-0927's flush-setting amendment restated for 4 shards and 500ms"
    );
}

#[test]
fn a_count_left_behind_by_a_cadence_or_shard_change_is_refused() {
    let compose = read(COMPOSE_FILE);
    let readme = read(README_FILE);

    // The 2 s figure kept after the flush delay moved to 500 ms.
    let old_count = mutate(&compose, "      - \"1358600\"", "      - \"343400\"");
    let old_readme = mutate(
        &readme,
        &format!("{README_ROW_PREFIX}1358600`"),
        &format!("{README_ROW_PREFIX}343400`"),
    );
    let err = check(&old_count, &old_readme).expect_err("343400 at 500ms must be refused");
    assert!(err.contains("derives"), "{err}");

    // The flush delay moved and the count left alone.
    let flush_moved = mutate(
        &compose,
        "      - \"--max-flush-delay\"\n      - \"500ms\"",
        "      - \"--max-flush-delay\"\n      - \"1s\"",
    );
    let err = check(&flush_moved, &readme).expect_err("a 1s delay with the 500ms count");
    assert!(err.contains("derives"), "{err}");

    // The shard count moved and the count left alone.
    let shards_moved = mutate(
        &compose,
        "      - \"--shards\"\n      - \"4\"",
        "      - \"--shards\"\n      - \"8\"",
    );
    let err = check(&shards_moved, &readme).expect_err("8 shards with the 4-shard count");
    assert!(err.contains("derives"), "{err}");

    // The compose file right and the README row stale.
    let err = check(&compose, &old_readme).expect_err("a README row that disagrees");
    assert!(err.contains("README"), "{err}");
}
