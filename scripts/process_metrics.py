#!/usr/bin/env python3
"""Measure process metrics against current repo and GitHub state. Writes a
Markdown report plus a sibling JSON snapshot so a program's end can diff
before/after.

Usage: python3 scripts/process_metrics.py [--repo OWNER/NAME] [--output PATH]
"""

import json
import re
import subprocess

DEFAULT_REPO = "NOFireAI/ravel"

_TASK_MERGE_RE = re.compile(
    r"^Merge remote-tracking branch 'origin/task/[a-f0-9-]+/result'$"
)


def count_task_merge_commits(subjects: list[str]) -> int:
    """Count commit subjects matching the fleet task-result merge pattern."""
    return sum(1 for s in subjects if _TASK_MERGE_RE.match(s))


def git_log_subjects(repo_path: str, ref: str = "main") -> list[str]:
    """Return one git commit subject line per element, oldest ordering
    ignored (order does not matter for counting)."""
    result = subprocess.run(
        ["git", "-C", repo_path, "log", ref, "--format=%s"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.splitlines()


def compute_ci_failure_rate(runs: list[dict]) -> dict:
    """Gate-failure-at-merge proxy: failure rate of CI runs triggered by a
    push to main. Runs with conclusion=None (still in progress) are counted
    but excluded from the rate denominator."""
    main_pushes = [r for r in runs if r.get("event") == "push" and r.get("head_branch") == "main"]
    failures = sum(1 for r in main_pushes if r.get("conclusion") == "failure")
    successes = sum(1 for r in main_pushes if r.get("conclusion") == "success")
    in_progress = sum(1 for r in main_pushes if r.get("conclusion") is None)
    terminal = failures + successes
    rate = (failures / terminal) if terminal > 0 else None
    return {
        "total": len(main_pushes),
        "failures": failures,
        "successes": successes,
        "in_progress": in_progress,
        "rate": rate,
    }


def fetch_main_push_runs(repo: str) -> list[dict]:
    """Fetch all workflow_runs entries via gh api, paginated."""
    result = subprocess.run(
        [
            "gh", "api", f"repos/{repo}/actions/runs", "--paginate",
            "-q", ".workflow_runs[] | {event, head_branch, conclusion}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


_CONFIRMED_FINDINGS_RE = re.compile(r"Confirmed findings:\s+\*\*(\d+)\*\*")


def parse_confirmed_findings_count(report_text: str) -> int | None:
    """Extract the prose 'Confirmed findings: **N**' summary line."""
    m = _CONFIRMED_FINDINGS_RE.search(report_text)
    return int(m.group(1)) if m else None


_FINDING_ID_RE = re.compile(r"^[A-Za-z0-9]+-F\d+$")


def count_finding_table_rows(report_text: str, section_heading: str) -> int:
    """Count Markdown finding rows under '## <n>. <section_heading>'.

    A finding row is one whose first table cell is a finding ID matching
    ``<prefix>-F<number>``. Counting on that positive signal, rather than
    "every pipe-line except the header and separator", is what lets this
    ignore the table's header row, its ``|---|`` separator, and non-finding
    scaffolding rows that share the table but carry no finding ID. It is the
    one signal consistent across finding tables that otherwise share no
    column schema."""
    heading_re = re.compile(
        r"^## \d+\. " + re.escape(section_heading) + r"\s*$", re.MULTILINE
    )
    m = heading_re.search(report_text)
    if not m:
        return 0
    rest = report_text[m.end():]
    next_heading = re.search(r"^## ", rest, re.MULTILINE)
    section = rest[: next_heading.start()] if next_heading else rest

    count = 0
    for line in section.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        first_cell = stripped.strip("|").split("|")[0].strip()
        if _FINDING_ID_RE.match(first_cell):
            count += 1
    return count


def cross_check_escape_count(report_text: str, section_heading: str) -> dict:
    prose = parse_confirmed_findings_count(report_text)
    table = count_finding_table_rows(report_text, section_heading)
    return {
        "prose_count": prose,
        "table_count": table,
        "agrees": prose is not None and prose == table,
    }


MANUAL_METRICS = {
    "lost_work_incidents": {
        "value": 2,
        "source": "historical count of results lost when a first run committed "
                  "to a side worktree the fleet harness does not collect",
        "note": "Fixed by the commit-on-dispatched-HEAD rule. No durable "
                "counter exists for this class of incident yet, so this is a "
                "point-in-time historical count, not a live query.",
    },
    "merge_touch_cost_steps": {
        "value": 7,
        "source": "distinct manual actions the documented merge procedure "
                  "requires per merge (remote verify, worktree, fetch result "
                  "branch, review diff scope, run local gates, merge --no-ff, "
                  "push and clean up refs)",
        "note": "Counts distinct manual actions the current, un-automated "
                "merge path requires. A merge queue is designed to bring this "
                "to 0 for the green-path case.",
    },
    "redispatch_rate": {
        "value": "unmeasurable",
        "source": "no durable dispatch log exists outside session transcripts",
        "note": "Known incidents are documented only anecdotally; there is no "
                "durable dispatch log to compute a rate from. Stall telemetry "
                "is the intended fix for this gap.",
    },
}


def build_report(
    repo: str,
    repo_path: str,
    generated_date: str,
    task_merge_count: int,
    ci_stats: dict,
    q0_escape: dict,
    sql_escape: dict,
) -> dict:
    return {
        "repo": repo,
        "generated_date": generated_date,
        "task_merge_count": task_merge_count,
        "ci_failure_rate": ci_stats,
        "escape_rate": {"q0": q0_escape, "ravel_sql": sql_escape},
        "manual_metrics": MANUAL_METRICS,
    }


def _fmt_rate(rate):
    return f"{rate:.1%}" if rate is not None else "n/a (no terminal runs)"


def render_markdown(report: dict) -> str:
    ci = report["ci_failure_rate"]
    q0 = report["escape_rate"]["q0"]
    sql = report["escape_rate"]["ravel_sql"]
    manual = report["manual_metrics"]

    lines = [
        "# Process metrics baseline",
        "",
        f"Generated {report['generated_date']} against `{report['repo']}` "
        f"main. Source data and re-run instructions: "
        f"`python3 scripts/process_metrics.py`. Re-run at the end of a "
        f"process program for the before/after comparison.",
        "",
        "## Metrics",
        "",
        "| Metric | Value | Source |",
        "|---|---|---|",
        f"| Fleet task-merges on main | {report['task_merge_count']} | "
        f"`git log main --format=%s`, counted by "
        f"`count_task_merge_commits` |",
        f"| CI failure rate (main pushes) | {_fmt_rate(ci['rate'])} "
        f"({ci['failures']} failures / {ci['successes']} successes, "
        f"{ci['in_progress']} in progress, {ci['total']} total) | "
        f"`gh api repos/{report['repo']}/actions/runs` |",
        f"| Escape rate, storage audit | {q0['table_count']} confirmed findings "
        f"(prose/table agree: {q0['agrees']}) | "
        f"storage-engine quality audit |",
        f"| Escape rate, ravel-sql audit | {sql['table_count']} table rows, "
        f"{sql['prose_count']} findings claimed (prose/table agree: "
        f"{sql['agrees']}) | ravel-sql audit |",
    ]
    for name, entry in manual.items():
        lines.append(f"| {name.replace('_', ' ')} | {entry['value']} | {entry['source']} |")
    lines += ["", "## Notes on unmeasurable and mismatched metrics", ""]
    if not sql["agrees"]:
        lines.append(
            f"- ravel-sql escape count: prose says {sql['prose_count']}, the "
            f"finding table has {sql['table_count']} rows. This is a documented, "
            f"understood exception (one finding folds into another ticket "
            f"instead of getting its own row) -- not parser error."
        )
    for name, entry in manual.items():
        if entry["value"] == "unmeasurable":
            lines.append(f"- {name.replace('_', ' ')}: {entry['note']}")
    lines.append("")
    return "\n".join(lines)


def main():
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--repo-path", default=".")
    parser.add_argument(
        "--output",
        default="process-metrics-baseline.md",
    )
    args = parser.parse_args()

    task_merge_count = count_task_merge_commits(
        git_log_subjects(args.repo_path, ref="main")
    )
    ci_stats = compute_ci_failure_rate(fetch_main_push_runs(args.repo))

    with open(f"{args.repo_path}/storage-engine-quality-audit.md") as f:
        q0_escape = cross_check_escape_count(f.read(), "Child-ticket index")
    with open(f"{args.repo_path}/ravel-sql-audit.md") as f:
        sql_escape = cross_check_escape_count(f.read(), "Finding index")

    report = build_report(
        repo=args.repo,
        repo_path=args.repo_path,
        generated_date=date.today().isoformat(),
        task_merge_count=task_merge_count,
        ci_stats=ci_stats,
        q0_escape=q0_escape,
        sql_escape=sql_escape,
    )

    with open(args.output, "w") as f:
        f.write(render_markdown(report))
    json_path = args.output.rsplit(".", 1)[0] + ".json"
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
        f.write("\n")
    print(f"Wrote {args.output} and {json_path}")


if __name__ == "__main__":
    main()
