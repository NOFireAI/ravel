"""Unit tests for bin/agent-cli's parsing helpers (no network, no gh)."""

import importlib.machinery
import importlib.util
import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

_PATH = pathlib.Path(__file__).resolve().parent.parent / "bin" / "agent-cli"
_LOADER = importlib.machinery.SourceFileLoader("agent_cli", str(_PATH))
_SPEC = importlib.util.spec_from_loader("agent_cli", _LOADER)
cli = importlib.util.module_from_spec(_SPEC)
_LOADER.exec_module(cli)

BOT = {"login": "claude-fleet[bot]"}
HUMAN = {"login": "pmoust"}


class DecodeConcat(unittest.TestCase):
    def test_paginated_arrays_are_joined_in_order(self):
        self.assertEqual(cli.decode_concat('[1,2]\n[3]\n[]'), [1, 2, 3])

    def test_a_single_object_is_returned_as_is(self):
        self.assertEqual(cli.decode_concat(' {"a": 1} '), {"a": 1})

    def test_empty_output_is_none(self):
        self.assertIsNone(cli.decode_concat("  \n"))


class CiCounts(unittest.TestCase):
    def test_each_bucket_and_the_failing_names(self):
        rollup = [
            {"__typename": "CheckRun", "name": "a", "status": "COMPLETED", "conclusion": "SUCCESS"},
            {"__typename": "CheckRun", "name": "b", "status": "IN_PROGRESS", "conclusion": ""},
            {"__typename": "CheckRun", "name": "c", "status": "COMPLETED", "conclusion": "SKIPPED"},
            {"__typename": "CheckRun", "name": "d", "status": "COMPLETED", "conclusion": "FAILURE"},
            {"__typename": "CheckRun", "name": "e", "status": "COMPLETED", "conclusion": "CANCELLED"},
            {"__typename": "StatusContext", "context": "f", "state": "PENDING"},
            {"__typename": "StatusContext", "context": "g", "state": "SUCCESS"},
        ]
        counts, failing = cli.ci_counts(rollup)
        self.assertEqual(counts, {"pass": 2, "pending": 2, "fail": 2, "skip": 1})
        self.assertEqual(failing, ["d:failure", "e:cancelled"])


class Findings(unittest.TestCase):
    def comments(self):
        return [
            {"id": 1, "user": BOT, "commit_id": "H", "path": "a.rs", "line": 3,
             "body": "should_fix: the thing is wrong.\nFailure: ..."},
            {"id": 2, "user": BOT, "commit_id": "OLD", "path": "b.rs", "line": 9, "body": "nit: old"},
            {"id": 3, "user": HUMAN, "commit_id": "H", "in_reply_to_id": 1, "body": "fixed"},
            {"id": 4, "user": BOT, "commit_id": "H", "path": "c.rs", "line": 1, "body": "nit: tiny"},
            {"id": 5, "user": BOT, "commit_id": "H", "in_reply_to_id": 4, "body": "bot follow-up"},
            {"id": 6, "user": HUMAN, "commit_id": "H", "path": "d.rs", "line": 2, "body": "own note"},
        ]

    def test_head_only_bot_findings_with_replied_from_a_non_bot(self):
        rows = cli.classify_findings(self.comments(), "H")
        self.assertEqual([(r["id"], r["severity"], r["replied"]) for r in rows],
                         [(1, "should_fix", True), (4, "nit", False)])
        self.assertEqual(rows[0]["summary"], "the thing is wrong.")

    def test_a_finding_re_anchored_to_head_is_still_open_at_head(self):
        # GitHub moves commit_id to the new head when the commented line did
        # not change, which means the finding was not fixed by the push.
        moved = [{"id": 7, "user": BOT, "commit_id": "H", "original_commit_id": "OLD",
                  "path": "a.rs", "line": 1, "body": "should_fix: from the last round"},
                 {"id": 8, "user": BOT, "commit_id": "OLD", "original_commit_id": "OLD",
                  "path": "a.rs", "line": 2, "body": "should_fix: line changed since"}]
        self.assertEqual([(r["id"], r["commit"]) for r in cli.classify_findings(moved, "H")],
                         [(7, "OLD")])
        self.assertEqual([r["id"] for r in cli.classify_findings(moved, "H", all_commits=True)],
                         [7, 8])

    def test_all_commits_includes_older_findings(self):
        rows = cli.classify_findings(self.comments(), "H", all_commits=True)
        self.assertEqual([r["id"] for r in rows], [1, 2, 4])

    def test_review_body_verdict_and_bullets(self):
        body = ("Request changes: one should_fix.\n\nFindings:\n"
                "- should_fix, docs/x.md:12 — stale claim\n- nit, a.rs:3 - small\n")
        verdict, rows = cli.review_body_findings(body)
        self.assertEqual(verdict, "Request changes")
        self.assertEqual([(r["severity"], r["where"], r["summary"]) for r in rows],
                         [("should_fix", "docs/x.md:12", "stale claim"), ("nit", "a.rs:3", "small")])


class Severity(unittest.TestCase):
    def test_every_bot_and_skill_prefix_is_classified_and_anything_else_is_actionable(self):
        for word in ("blocking", "blocker", "major", "should_fix", "minor", "nit"):
            self.assertEqual(cli.severity(f"{word}: x"), word)
        self.assertEqual(cli.severity("looks fine to me"), "unclassified")
        self.assertEqual(cli.first_sentence("should_fix: should_fix: doubled prefix"), "doubled prefix")


OUTSIDE_BODY = """Request changes: two findings outside the diff.

Not checked:
- the TLA rows

Findings outside the diff:
- crates/ravel-sql/src/executor.rs:120 (major): the retry drops the error
  and the continuation line stays inside the block

- docs/ingest.md: the flush cadence paragraph contradicts the code

Findings outside the diff:
- services/x.rs:9 (blocking): a second block after a 422
Task 1234 on executor, model claude-opus-5.
- not a finding: this bullet follows a closing line
"""


class OutsideDiff(unittest.TestCase):
    def test_bullets_inside_every_block_and_none_outside(self):
        rows = cli.outside_diff_findings(OUTSIDE_BODY)
        self.assertEqual([(r["where"], r["severity"]) for r in rows],
                         [("crates/ravel-sql/src/executor.rs:120", "major"),
                          ("docs/ingest.md", "unclassified"),
                          ("services/x.rs:9", "blocking")])
        verdict, all_rows = cli.review_body_findings(OUTSIDE_BODY)
        self.assertEqual(verdict, "Request changes")
        self.assertEqual(sum(1 for r in all_rows if r["outside_diff"]), 3)

    def test_outside_diff_findings_are_summed_over_every_review_at_head(self):
        first = {"body": "Findings outside the diff:\n- a.rs:1 (major): one\n"}
        latest = {"body": "Approve: fine.\n\nFindings:\n- nit, b.rs:2 - small\n"}
        verdict, rows = cli.head_review_findings([first, latest])
        self.assertEqual(verdict, "Approve")
        self.assertEqual([(r["where"], r["outside_diff"]) for r in rows],
                         [("b.rs:2", False), ("a.rs:1", True)])

    def test_the_count_matches_the_shared_jq_filter(self):
        jq = shutil.which("jq")
        if not jq:
            self.skipTest("jq not installed")
        filt = pathlib.Path(__file__).resolve().parent / "lib" / "fleet-review-outside-diff.jq"
        bot = "claude-fleet[bot]"
        reviews = json.dumps([{"commit_id": "H", "user": {"login": bot}, "body": OUTSIDE_BODY}])
        out = subprocess.run([jq, "--arg", "sha", "H", "--arg", "bot", bot, "-f", str(filt)],
                             input=reviews, capture_output=True, text=True, check=True)
        self.assertEqual(int(out.stdout.strip()), len(cli.outside_diff_findings(OUTSIDE_BODY)))


class Checklist(unittest.TestCase):
    BODY = "- [ ] #1 T1: a\n- [x] #2 T2: b\n  - [ ] #3 T3: c\ntext\n"

    def test_counts(self):
        self.assertEqual(cli.checklist(self.BODY), (1, 2))

    def test_tick_only_matching_unticked_lines_and_keep_the_rest(self):
        body, changed = cli.tick_lines(self.BODY, ["#1 ", "#2 "])
        self.assertEqual(changed, 1)
        self.assertEqual(body, "- [x] #1 T1: a\n- [x] #2 T2: b\n  - [ ] #3 T3: c\ntext\n")

    def test_untick(self):
        body, changed = cli.tick_lines(self.BODY, ["#2 "], untick=True)
        self.assertEqual(changed, 1)
        self.assertIn("- [ ] #2 T2: b", body)


class EpicStatus(unittest.TestCase):
    def test_rows_are_parsed_and_the_header_ignored(self):
        text = ("== Epic #1: x [OPEN]\ntask  start  result  merge PR\n"
                "07502591-10b9-43ab-86f3-28801d32b0c2   yes    no      -\n"
                "226fb03d-7f85-443f-91ae-71bbbecb4f8c   yes    yes     #3538 MERGED\n")
        rows = cli.parse_epic_status(text)
        self.assertEqual([(r["task"][:8], r["result"], r["pr"]) for r in rows],
                         [("07502591", "no", "-"), ("226fb03d", "yes", "#3538 MERGED")])


class SpecLint(unittest.TestCase):
    def test_each_forbidden_character_is_located(self):
        hits = cli.lint_spec_text("ok\nrun `x` and $(y)\nend — here\n")
        self.assertEqual([(h["line"], h["col"], h["kind"], h["blocking"]) for h in hits],
                         [(2, 5, "backtick", True), (2, 7, "backtick", True),
                          (2, 13, "dollar-paren", True), (3, 5, "em-dash", False)])

    def test_a_clean_spec_has_no_hits(self):
        self.assertEqual(cli.lint_spec_text("plain text with $HOME and 'quotes'\n"), [])


class AdrSections(unittest.TestCase):
    TEXT = ("# ADR-1\n\n## Decision\nbody\n```\n## not a heading\n```\n### Sub\nx\n"
            "## Amendment (2026-01-01): more\ny\n")

    def test_headings_skip_fenced_code_and_mark_amendments(self):
        heads = cli.adr_headings(self.TEXT)
        self.assertEqual([(h["line"], h["level"], h["amendment"]) for h in heads],
                         [(1, 1, False), (3, 2, False), (8, 3, False), (10, 2, True)])

    def test_status_needs_a_colon_and_a_bare_status_word_is_not_a_crash(self):
        self.assertEqual(cli.adr_status("# A\n\n- Status: Accepted\n"), "Accepted")
        self.assertEqual(cli.adr_status("# A\n\nstatus pending owner sign-off\n"), "")


    def test_a_section_runs_to_the_next_heading_of_its_level(self):
        hit, body = cli.section_text(self.TEXT, "decision")
        self.assertEqual(hit["line"], 3)
        self.assertTrue(body.startswith("## Decision"))
        self.assertIn("### Sub", body)
        self.assertNotIn("Amendment", body)


class FleetConfig(unittest.TestCase):
    def test_cp_env_accepts_the_export_form_like_the_orchestrator(self):
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "cp.env"
            p.write_text('export FLEET_PUBLIC_URL="https://cp.example/"\n  export FLEET_ENQUEUE_TOKEN=t\n')
            keep = {k: os.environ.pop(k, None) for k in ("FLEET_CP_URL", "FLEET_PUBLIC_URL",
                                                          "FLEET_ENQUEUE_TOKEN")}
            os.environ["RAVEL_FLEET_CP_ENV"] = str(p)
            try:
                self.assertEqual(cli.cp_config(), ("https://cp.example", "t"))
            finally:
                os.environ.pop("RAVEL_FLEET_CP_ENV", None)
                for k, v in keep.items():
                    if v is not None:
                        os.environ[k] = v

    def test_expired_is_terminal(self):
        self.assertIn("expired", cli.TERMINAL_TASK)


class Usage(unittest.TestCase):
    TABLE = [{"pattern": "\\bgh\\s+pr\\s+(view|checks|status)\\b", "subcommand": "pr N", "answers": ""},
             {"pattern": "(^|[;&|]\\s*)df\\s+-h\\b", "subcommand": "box", "answers": ""}]

    def record(self, ts, cmd):
        return json.dumps({"timestamp": ts, "message": {"content": [
            {"type": "tool_use", "name": "Bash", "input": {"command": cmd}}]}})

    def test_every_shared_pattern_compiles_in_python(self):
        table = json.loads((pathlib.Path(__file__).resolve().parents[1] / ".claude" / "guards"
                            / "agent-cli-nudges.json").read_text())["nudges"]
        for n in table:
            re.compile(n["pattern"])
        self.assertGreaterEqual(len(table), 10)

    def test_counts_agent_cli_and_raw_calls_after_since_per_session(self):
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "s1.jsonl"
            p.write_text("\n".join([
                self.record("2026-10-30T00:00:00Z", "gh pr view 1"),        # before since
                self.record("2026-11-02T00:00:00Z", "gh pr view 2 --json state"),
                self.record("2026-11-02T00:00:01Z", "cd /x && df -h ."),
                self.record("2026-11-02T00:00:02Z", "bin/agent-cli --json pr 2"),
                self.record("2026-11-02T00:00:03Z", "grep -rn 'df -h' docs/"),  # neither
                self.record("2026-11-02T00:00:04Z", "git commit -m 'use agent-cli first'"),  # a mention
                self.record("2026-11-02T00:00:05Z", "git commit -m 'run agent-cli pr first'"),  # quoted
                self.record("2026-11-02T00:00:06Z", "timeout 60 bin/agent-cli pr 3"),
                self.record("2026-11-02T00:00:07Z", "gh pr view 4; df -h ."),  # two raw lookups
                self.record("2026-11-02T00:00:08Z", "cd /x\n  S=1 bin/agent-cli pr 5\nbin/agent-cli box"),
                self.record("2026-11-02T00:00:09Z", "bin/agent-cli pr 6 && gh pr view 6"),  # both sides
                "not json",
            ]))
            (pathlib.Path(d) / "s2.jsonl").write_text(self.record("2026-11-03T00:00:00Z", "ls"))
            t = cli.count_usage(sorted(pathlib.Path(d).glob("*.jsonl")), "2026-11-01", self.TABLE,
                                cli.subcommand_names())
        self.assertIn("usage", cli.subcommand_names())
        self.assertEqual((t["sessions"], t["agent_cli"], t["raw"]), (2, 5, 5))
        self.assertEqual(t["raw_by_subcommand"], {"pr": 3, "box": 2})
        self.assertEqual(t["agent_cli_by_subcommand"], {"pr": 4, "box": 1})

    def test_transcript_dirs_include_worktree_sessions(self):
        dirs = cli.transcript_dirs()
        if not dirs:
            self.skipTest("no Claude Code transcripts for this repository on this machine")
        root = dirs[0].name.split("-wt-")[0]
        self.assertTrue(all(d.name.startswith(root) for d in dirs))


class Parser(unittest.TestCase):
    def test_json_before_or_after_the_subcommand_is_honoured(self):
        parser = cli.build_parser()
        self.assertTrue(parser.parse_args(["--json", "box"]).json)
        self.assertTrue(parser.parse_args(["box", "--json"]).json)
        self.assertFalse(parser.parse_args(["box"]).json)

    def test_a_missing_file_argument_is_a_usage_error_not_a_finding(self):
        self.assertEqual(cli.main(["spec-lint", "/nonexistent/agent-cli-spec.txt"]), 64)

    def test_every_subcommand_has_its_own_help(self):
        parser = cli.build_parser()
        sub = next(a for a in parser._actions if a.dest == "cmd")
        self.assertGreaterEqual(len(sub.choices), 20)
        for name, sp in sub.choices.items():
            self.assertTrue(sp.description, name)
            self.assertIn("--json", sp.format_help(), name)


if __name__ == "__main__":
    unittest.main()
