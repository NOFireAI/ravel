"""Unit tests for bin/agent-cli's parsing helpers (no network, no gh)."""

import importlib.machinery
import importlib.util
import pathlib
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
        self.assertEqual([(h["line"], h["col"], h["kind"]) for h in hits],
                         [(2, 5, "backtick"), (2, 7, "backtick"), (2, 13, "dollar-paren"),
                          (3, 5, "em-dash")])

    def test_a_clean_spec_has_no_hits(self):
        self.assertEqual(cli.lint_spec_text("plain text with $HOME and 'quotes'\n"), [])


class AdrSections(unittest.TestCase):
    TEXT = ("# ADR-1\n\n## Decision\nbody\n```\n## not a heading\n```\n### Sub\nx\n"
            "## Amendment (2026-01-01): more\ny\n")

    def test_headings_skip_fenced_code_and_mark_amendments(self):
        heads = cli.adr_headings(self.TEXT)
        self.assertEqual([(h["line"], h["level"], h["amendment"]) for h in heads],
                         [(1, 1, False), (3, 2, False), (8, 3, False), (10, 2, True)])

    def test_a_section_runs_to_the_next_heading_of_its_level(self):
        hit, body = cli.section_text(self.TEXT, "decision")
        self.assertEqual(hit["line"], 3)
        self.assertTrue(body.startswith("## Decision"))
        self.assertIn("### Sub", body)
        self.assertNotIn("Amendment", body)


class Parser(unittest.TestCase):
    def test_every_subcommand_has_its_own_help(self):
        parser = cli.build_parser()
        sub = next(a for a in parser._actions if a.dest == "cmd")
        self.assertGreaterEqual(len(sub.choices), 20)
        for name, sp in sub.choices.items():
            self.assertTrue(sp.description, name)
            self.assertIn("--json", sp.format_help(), name)


if __name__ == "__main__":
    unittest.main()
