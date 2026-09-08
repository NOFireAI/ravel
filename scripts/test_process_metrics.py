#!/usr/bin/env python3
"""Unit tests for scripts/process_metrics.py. Run: python3 -m unittest scripts.test_process_metrics -v"""

import unittest

from process_metrics import (
    count_task_merge_commits,
    compute_ci_failure_rate,
    parse_confirmed_findings_count,
    count_finding_table_rows,
    cross_check_escape_count,
    MANUAL_METRICS,
    build_report,
    render_markdown,
)


Q0_FIXTURE = """## 1. Executive summary

- Confirmed findings: **43** (S0: 0, S1: 1, S2: 21, S3: 21). 17 have
  committed reproducers.

## 7. Child-ticket index

| Finding | Issue | Severity | Confidence | Crate | Category | Dependencies | Size | Wave |
|---|---|---|---|---|---|---|---|---|
| c1-F01 | #101 | S1 | Confirmed | ravel-catalog | MVCC | -> #105, #120 | M | 0 |
| c2-F01 | #102 | S2 | Confirmed | ravel-segment | DEC | none | S | 1 |
| c1-F02 | #103 | S2 | High | ravel-catalog | MVCC | blocked by #130 | M | 1 |

## 8. Dependency graph and dispatch order

Some other content.
"""

SQL_FIXTURE = """## 3. Summary

- Confirmed findings: **8** (S0: 0, S1: 1, S2: 6, S3: 1).

## 4. Finding index

| ID | Area | Sev | Cat | Ticket | Reproducer |
|---|---|---|---|---|---|
| q1-F01 | SQL | S1 | MEM | #201 | audit_sql_exec::q1_f01 |
| q1-F02 | SQL | S2 | MEM | #202 | audit_sql_exec::q1_f02 |
"""

Q0_FIXTURE_ESCAPE = {"prose_count": 43, "table_count": 43, "agrees": True}
SQL_FIXTURE_ESCAPE = {"prose_count": 8, "table_count": 7, "agrees": False}


class TestCountTaskMergeCommits(unittest.TestCase):
    def test_counts_matching_merge_subjects(self):
        subjects = [
            "Merge remote-tracking branch 'origin/task/bc08408e-441a-478d-95d9-a89e52fcb59a/result'",
            "feat(promql): implement P6 elementwise, clamp, sort, label, time functions",
            "Merge remote-tracking branch 'origin/task/4cbd60db-7cc0-4c90-a679-9b69a25dbe71/result'",
            "docs: add process program design spec",
        ]
        self.assertEqual(count_task_merge_commits(subjects), 2)

    def test_ignores_non_task_merges(self):
        subjects = [
            "Merge remote-tracking branch 'origin/main'",
            "Merge pull request #12 from feature/x",
        ]
        self.assertEqual(count_task_merge_commits(subjects), 0)

    def test_empty_input(self):
        self.assertEqual(count_task_merge_commits([]), 0)


class TestComputeCiFailureRate(unittest.TestCase):
    def test_computes_rate_over_terminal_runs(self):
        runs = [
            {"event": "push", "head_branch": "main", "conclusion": "success"},
            {"event": "push", "head_branch": "main", "conclusion": "failure"},
            {"event": "push", "head_branch": "main", "conclusion": "failure"},
            {"event": "push", "head_branch": "main", "conclusion": "success"},
        ]
        result = compute_ci_failure_rate(runs)
        self.assertEqual(result["total"], 4)
        self.assertEqual(result["failures"], 2)
        self.assertEqual(result["successes"], 2)
        self.assertEqual(result["in_progress"], 0)
        self.assertAlmostEqual(result["rate"], 0.5)

    def test_excludes_non_main_and_non_push(self):
        runs = [
            {"event": "push", "head_branch": "main", "conclusion": "success"},
            {"event": "pull_request", "head_branch": "main", "conclusion": "failure"},
            {"event": "push", "head_branch": "feature/x", "conclusion": "failure"},
        ]
        result = compute_ci_failure_rate(runs)
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["rate"], 0.0)

    def test_in_progress_runs_excluded_from_rate_but_counted(self):
        runs = [
            {"event": "push", "head_branch": "main", "conclusion": None},
            {"event": "push", "head_branch": "main", "conclusion": "success"},
        ]
        result = compute_ci_failure_rate(runs)
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["in_progress"], 1)
        self.assertEqual(result["rate"], 0.0)

    def test_no_terminal_runs_yields_none_rate(self):
        result = compute_ci_failure_rate([])
        self.assertEqual(result["total"], 0)
        self.assertIsNone(result["rate"])


class TestParseConfirmedFindingsCount(unittest.TestCase):
    def test_extracts_bold_prose_count(self):
        self.assertEqual(parse_confirmed_findings_count(Q0_FIXTURE), 43)
        self.assertEqual(parse_confirmed_findings_count(SQL_FIXTURE), 8)

    def test_returns_none_when_absent(self):
        self.assertIsNone(parse_confirmed_findings_count("no such line here"))


class TestCountFindingTableRows(unittest.TestCase):
    def test_counts_data_rows_under_heading(self):
        self.assertEqual(
            count_finding_table_rows(Q0_FIXTURE, "Child-ticket index"), 3
        )
        self.assertEqual(
            count_finding_table_rows(SQL_FIXTURE, "Finding index"), 2
        )

    def test_missing_heading_returns_zero(self):
        self.assertEqual(count_finding_table_rows(Q0_FIXTURE, "Nonexistent"), 0)


class TestCrossCheckEscapeCount(unittest.TestCase):
    def test_flags_agreement(self):
        # table has 3 rows but prose says 43 in the fixture (truncated
        # fixture, not the real report) -- this must NOT agree.
        result = cross_check_escape_count(Q0_FIXTURE, "Child-ticket index")
        self.assertEqual(result["prose_count"], 43)
        self.assertEqual(result["table_count"], 3)
        self.assertFalse(result["agrees"])

    def test_flags_true_agreement(self):
        matching = Q0_FIXTURE.replace("**43**", "**3**")
        result = cross_check_escape_count(matching, "Child-ticket index")
        self.assertTrue(result["agrees"])


class TestManualMetrics(unittest.TestCase):
    def test_has_required_keys(self):
        expected = {"lost_work_incidents", "merge_touch_cost_steps", "redispatch_rate"}
        self.assertEqual(set(MANUAL_METRICS.keys()), expected)
        for name, entry in MANUAL_METRICS.items():
            with self.subTest(metric=name):
                self.assertIn("value", entry)
                self.assertIn("source", entry)
                self.assertIn("note", entry)

    def test_lost_work_incidents_is_two(self):
        self.assertEqual(MANUAL_METRICS["lost_work_incidents"]["value"], 2)

    def test_merge_touch_cost_is_seven_steps(self):
        self.assertEqual(MANUAL_METRICS["merge_touch_cost_steps"]["value"], 7)

    def test_redispatch_rate_is_unmeasurable(self):
        self.assertEqual(MANUAL_METRICS["redispatch_rate"]["value"], "unmeasurable")


class TestBuildReport(unittest.TestCase):
    def test_assembles_all_five_metrics(self):
        report = build_report(
            repo="NOFireAI/ravel",
            repo_path=".",
            generated_date="2026-07-28",
            task_merge_count=9,
            ci_stats={"total": 139, "failures": 40, "successes": 98, "in_progress": 1, "rate": 40 / 138},
            q0_escape=Q0_FIXTURE_ESCAPE,
            sql_escape=SQL_FIXTURE_ESCAPE,
        )
        self.assertEqual(report["generated_date"], "2026-07-28")
        self.assertEqual(report["task_merge_count"], 9)
        self.assertIn("ci_failure_rate", report)
        self.assertIn("escape_rate", report)
        self.assertIn("manual_metrics", report)
        self.assertEqual(report["manual_metrics"], MANUAL_METRICS)


class TestRenderMarkdown(unittest.TestCase):
    def test_renders_a_metrics_table(self):
        report = build_report(
            repo="NOFireAI/ravel",
            repo_path=".",
            generated_date="2026-07-28",
            task_merge_count=9,
            ci_stats={"total": 139, "failures": 40, "successes": 98, "in_progress": 1, "rate": 40 / 138},
            q0_escape=Q0_FIXTURE_ESCAPE,
            sql_escape=SQL_FIXTURE_ESCAPE,
        )
        md = render_markdown(report)
        self.assertIn("# Process metrics baseline", md)
        self.assertIn("9", md)
        self.assertIn("unmeasurable", md)


if __name__ == "__main__":
    unittest.main()
