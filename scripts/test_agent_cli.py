"""Unit tests for bin/agent-cli's parsing helpers (no network, no gh)."""

import contextlib
import importlib.machinery
import importlib.util
import io
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

    def _spec_lint(self, text):
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "spec.txt"
            p.write_text(text)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = cli.main(["spec-lint", str(p)])
        return rc, buf.getvalue()

    def test_spec_lint_refuses_what_dispatch_refuses(self):
        # FLIP: spec-lint checking characters only reports this spec clean
        # (rc 0), while dispatch refuses it for the missing head lines.
        rc, out = self._spec_lint("plain spec\ndisk_floor_gb: 10\n")
        self.assertEqual(rc, 1)
        self.assertIn("no gate_tier line", out)

    def test_spec_lint_passes_a_spec_with_both_head_lines(self):
        rc, _ = self._spec_lint("gate_tier: script-only\ndisk_floor_gb: 10\nplain spec\n")
        self.assertEqual(rc, 0)

    def test_spec_lint_reports_the_placeholder(self):
        # FLIP: a spec-lint that only checks the two head lines' own presence
        # and syntax reports this spec clean (rc 0), since gate_tier and
        # disk_floor_gb are both present and valid; only scanning the body for
        # the unfilled headroom placeholder catches the stale template line.
        rc, out = self._spec_lint(
            "gate_tier: script-only\ndisk_floor_gb: 10\n"
            "    scripts/guards/check-disk-headroom.sh .gate-logs <disk_floor_gb>\n")
        self.assertEqual(rc, 1)
        self.assertIn("line 3", out)
        self.assertIn("<disk_floor_gb>", out)

    def test_the_real_template_filled_in_passes(self):
        # FLIP: scanning every line for the placeholder refuses this, since
        # the template names it in prose and in the executor's below-floor
        # report format, which a filled spec keeps.
        skill = pathlib.Path(__file__).resolve().parent.parent / ".claude/skills/fleet-task-spec/SKILL.md"
        text = skill.read_text()
        start = text.index("## Template")
        body = text[text.index("```\n", start) + 4:]
        body = body[:body.index("\n```\n")]
        body = body.replace("gate_tier: <executor|merge|script-only>", "gate_tier: executor")
        body = body.replace("disk_floor_gb: <N>", "disk_floor_gb: 60")
        body = body.replace(".gate-logs <disk_floor_gb>", ".gate-logs 60")
        self.assertIn("<disk_floor_gb>", body, "the template no longer mentions the placeholder; "
                      "this case would no longer test the prose exemption")
        self.assertEqual(cli.spec_head_values(body), ("executor", 60))

    def test_headroom_floor_parsing(self):
        head = "gate_tier: executor\ndisk_floor_gb: 60\n"
        run = "    mkdir -p .gate-logs && scripts/guards/check-disk-headroom.sh .gate-logs {} && df -h\n"
        refused = {
            # FLIP: a leading-digit match accepts 60x and 60.5, which the
            # guard's integer test rejects on every host.
            "60x": "is not an integer", "60.5": "is not an integer",
            # FLIP: a digit-first match skips a quoted floor, which the shell
            # unquotes and runs as 5.
            '"5"': "floor 5 differs",
            "<N>": "is not an integer", "sixty": "is not an integer",
        }
        for arg, why in refused.items():
            with self.assertRaises(cli.CliError, msg=arg) as ctx:
                cli.spec_head_values(head + run.format(arg))
            self.assertIn(why, str(ctx.exception), arg)
        # FLIP: a per-line scan misses a floor moved to the next line.
        with self.assertRaises(cli.CliError) as ctx:
            cli.spec_head_values(head + "    scripts/guards/check-disk-headroom.sh .gate-logs \\\n      5\n")
        self.assertIn("floor 5 differs", str(ctx.exception))
        # FLIP: treating the guard's default floor as acceptable passes this.
        with self.assertRaises(cli.CliError) as ctx:
            cli.spec_head_values(head + "    scripts/guards/check-disk-headroom.sh .gate-logs\n")
        self.assertIn("without a floor argument", str(ctx.exception))
        # Prose and comments that name the script are not invocations.
        # FLIP: matching any line holding the script name refuses both.
        ok = head + ("If scripts/guards/check-disk-headroom.sh exits 1, stop.\n"
                     "# scripts/guards/check-disk-headroom.sh .gate-logs 5  (old floor)\n")
        self.assertEqual(cli.spec_head_values(ok), ("executor", 60))
        self.assertEqual(cli.spec_head_values(head + run.format("60")), ("executor", 60))
        self.assertEqual(cli.spec_head_values(head + run.format("060")), ("executor", 60))


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


class CiBudgets(unittest.TestCase):
    def _fixture(self, d, exit_code, body='echo "args: $*"'):
        # A stub check-ci-job-budgets.sh standing in for the real guard
        # (issue #4037): echoes the argv it was called with, so the
        # assertions below can pin exactly what cmd_ci_budgets forwarded.
        root = pathlib.Path(d)
        guard_dir = root / "scripts" / "guards"
        guard_dir.mkdir(parents=True)
        guard = guard_dir / "check-ci-job-budgets.sh"
        guard.write_text(f"#!/usr/bin/env bash\n{body}\nexit {exit_code}\n")
        guard.chmod(0o755)
        env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
                "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid"}
        subprocess.run(["git", "init", "-q"], cwd=root, check=True)
        subprocess.run(["git", "add", "-A"], cwd=root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=root, check=True,
                        env={**os.environ, **env})
        return root

    def _run_ci_budgets(self, root, argv):
        cwd_before = os.getcwd()
        os.chdir(root)
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                rc = cli.main(argv)
        finally:
            os.chdir(cwd_before)
        return rc, buf.getvalue()

    def test_passes_through_exit_code_and_forwards_repo_runs_flags(self):
        with tempfile.TemporaryDirectory() as d:
            root = self._fixture(d, 1)
            rc, out = self._run_ci_budgets(
                root, ["--json", "ci-budgets", "--repo", "acme/widgets", "--runs", "10"])
        # FLIP: if cmd_ci_budgets dropped the --repo/--runs forwarding, or
        # swallowed the guard's exit code behind a fixed return of its own,
        # this would either miss the flags in detail below or report rc=0.
        self.assertEqual(rc, 1)
        detail = json.loads(out)["detail"]
        self.assertIn("--repo", detail)
        self.assertIn("acme/widgets", detail)
        self.assertIn("--runs", detail)
        self.assertIn("10", detail)

    def test_omits_repo_and_runs_flags_when_neither_is_given(self):
        with tempfile.TemporaryDirectory() as d:
            root = self._fixture(d, 0)
            rc, out = self._run_ci_budgets(root, ["--json", "ci-budgets"])
        # FLIP: a cmd_ci_budgets that always passed a default --repo/--runs
        # through to the script would fail this, pinning that the flags are
        # genuinely optional.
        self.assertEqual(rc, 0)
        detail = json.loads(out)["detail"]
        self.assertNotIn("--repo", detail)
        self.assertNotIn("--runs", detail)

    def test_reports_the_finding_lines_not_the_last_job_row(self):
        body = ("printf 'doc-scripts\\tp90=8m\\tcap=10\\n'\n"
                # The real guard's spelling (check-ci-job-budgets.sh, the cap
                # rule's echo), prefix included.
                "printf 'check-ci-job-budgets.sh: FINDING cap job=sql p90=40m cap=45m ratio=0.89 (threshold 0.7)\\n'\n"
                "printf 'lint\\tp90=3m\\tcap=20\\n'")
        with tempfile.TemporaryDirectory() as d:
            root = self._fixture(d, 1, body)
            rc, out = self._run_ci_budgets(root, ["--json", "ci-budgets"])
        # FLIP: reporting the guard's last output line, as the command first
        # did, would surface the clean lint row here and drop the finding.
        self.assertEqual(rc, 1)
        data = json.loads(out)
        self.assertIn("FINDING cap job=sql", data["detail"])
        self.assertNotIn("lint", data["detail"])
        self.assertEqual(len(data["findings"]), 1)


TASK_ID = "0123abcd-4567-89ab-cdef-0123456789ab"
SPEC_HEAD = "gate_tier: script-only (no cargo build)\ndisk_floor_gb: 10\n"


def capture_main(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = cli.main(argv)
    return rc, out.getvalue(), err.getvalue()


class Dispatch(unittest.TestCase):
    """cmd_dispatch with every remote step stubbed: the MCP call and fresh_main
    in-process, the intent script through run, and gh as a stub on PATH that
    serves the epic body from a file, so the real epic-orchestrator.sh writes
    the ledger line and nothing reaches GitHub or the fleet."""

    GH_STUB = """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "${STUB_DIR}/gh.log"
case "$1 $2" in
  "issue view") cat "${STUB_DIR}/body.txt" ;;
  "issue edit")
    while [[ $# -gt 0 ]]; do
      [[ "$1" == "--body-file" ]] && cp "$2" "${STUB_DIR}/body.txt"
      shift
    done ;;
  *) echo "gh stub: unexpected call $*" >&2; exit 1 ;;
esac
"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = pathlib.Path(self.tmp.name)
        (self.dir / "bin").mkdir()
        gh = self.dir / "bin" / "gh"
        gh.write_text(self.GH_STUB)
        gh.chmod(0o755)
        (self.dir / "body.txt").write_text("Epic body\n")
        self.calls, self.mcp_calls = [], []
        real_run = cli.run

        def fake_run(cmd, cwd=None, env=None, stdin=None, check=False):
            self.calls.append(list(cmd))
            if cmd[0] == "scripts/epic-orchestrator.sh":
                return real_run(cmd, cwd=cwd, env=env, stdin=stdin, check=check)
            if cmd[:2] == ["scripts/fleet-dispatch-intent.sh", "intent"]:
                return 0, "nonce-1\n", ""
            return 0, "", ""

        def fake_mcp(tool, arguments, timeout=300):
            self.mcp_calls.append((tool, arguments))
            return f"Dispatched task {TASK_ID}"

        saved = {"run": cli.run, "mcp_call": cli.mcp_call, "fresh_main": cli.fresh_main}
        cli.run, cli.mcp_call, cli.fresh_main = fake_run, fake_mcp, lambda: "a" * 40
        env = {"PATH": f"{self.dir / 'bin'}{os.pathsep}{os.environ['PATH']}",
               "STUB_DIR": str(self.dir), "RAVEL_EPIC_STATE_DIR": str(self.dir / "state")}
        saved_env = {k: os.environ.get(k) for k in list(env) + ["RAVEL_EPIC_NO_REMOTE"]}
        os.environ.update(env)
        os.environ.pop("RAVEL_EPIC_NO_REMOTE", None)

        def restore():
            for k, v in saved.items():
                setattr(cli, k, v)
            for k, v in saved_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            self.tmp.cleanup()

        self.addCleanup(restore)

    def dispatch(self, spec):
        path = self.dir / "spec.txt"
        path.write_text(spec)
        return capture_main(["dispatch", "--epic", "4034", "--ticket", "4043", "--spec", str(path)])

    def test_dispatch_refuses_spec_without_gate_tier(self):
        rc, _, err = self.dispatch("Do the thing.\ndisk_floor_gb: 10\n")
        # FLIP: a cmd_dispatch that lints and dispatches without reading the
        # head (the check removed, or run after the intent step) exits 0 here,
        # or 2 only after the intent script and the MCP stub were called.
        self.assertEqual(rc, 2)
        self.assertIn("no gate_tier line", err)
        self.assertNotIn("disk_floor_gb", err.split("(each")[0])
        self.assertEqual((self.calls, self.mcp_calls), ([], []))

    def test_dispatch_refuses_spec_without_disk_floor(self):
        rc, _, err = self.dispatch("Do the thing.\ngate_tier: executor\n")
        # FLIP: a check that requires gate_tier alone dispatches this spec.
        self.assertEqual(rc, 2)
        self.assertIn("no disk_floor_gb line", err)
        self.assertEqual((self.calls, self.mcp_calls), ([], []))

    def test_dispatch_refuses_a_value_outside_the_forms(self):
        for head, what in (("gate_tier: fast\ndisk_floor_gb: 10\n", "gate_tier fast"),
                           ("gate_tier: merge\ndisk_floor_gb: 0\n", "disk_floor_gb 0"),
                           ("gate_tier: merge\ndisk_floor_gb: 40GB\n", "disk_floor_gb 40GB"),
                           ("gate_tier: merge\ngate_tier: executor\ndisk_floor_gb: 4\n",
                            "gate_tier given twice")):
            rc, _, err = self.dispatch("Do the thing.\n" + head)
            # FLIP: a check for the key's presence alone accepts each of these.
            self.assertEqual(rc, 2, head)
            self.assertIn(what, err, head)
        self.assertEqual((self.calls, self.mcp_calls), ([], []))

    def test_dispatch_records_tier_and_floor_in_the_ledger_line(self):
        rc, out, err = self.dispatch("Do the thing.\n" + SPEC_HEAD)
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(len(self.mcp_calls), 1)
        body = (self.dir / "body.txt").read_text()
        # FLIP: the record call without the tier and floor in its status value
        # writes "- #4043 task=<uuid> dispatched" and nothing after it.
        self.assertIn(f"- #4043 task={TASK_ID} dispatched tier=script-only floor=10\n", body)
        state = json.loads((self.dir / "state" / "4034.json").read_text())
        self.assertEqual((state["tasks"][TASK_ID]["tier"], state["tasks"][TASK_ID]["floor"]),
                         ("script-only", "10"))

    def test_dispatch_refuses_spec_with_unfilled_floor_placeholder(self):
        spec = ("Do the thing.\n" + SPEC_HEAD
                + "    mkdir -p .gate-logs && git check-ignore -q .gate-logs "
                  "&& scripts/guards/check-disk-headroom.sh .gate-logs <disk_floor_gb>\n")
        rc, _, err = self.dispatch(spec)
        # FLIP: a check that only reads the two head lines' own presence and
        # syntax dispatches this spec unchanged, since gate_tier and
        # disk_floor_gb are both present and valid; on the executor the
        # placeholder is shell redirection and the first command is a parse
        # error, which reads as below-floor instead of as this template bug.
        self.assertEqual(rc, 2)
        self.assertIn("<disk_floor_gb>", err)
        self.assertIn("placeholder", err)
        self.assertEqual((self.calls, self.mcp_calls), ([], []))

    def test_dispatch_refuses_headroom_floor_that_differs_from_declared(self):
        spec = ("Do the thing.\n" + SPEC_HEAD
                + "    mkdir -p .gate-logs && git check-ignore -q .gate-logs "
                  "&& scripts/guards/check-disk-headroom.sh .gate-logs 5\n")
        rc, _, err = self.dispatch(spec)
        # FLIP: a check that validates the headroom argument only as "a
        # positive integer" (the same rule disk_floor_gb's own line gets)
        # accepts this, since 5 passes that rule on its own; only comparing
        # it against the spec's declared disk_floor_gb (10) catches the
        # mismatch an author who edited one line and not the other leaves.
        self.assertEqual(rc, 2)
        self.assertIn("floor 5", err)
        self.assertIn("declared disk_floor_gb 10", err)
        self.assertEqual((self.calls, self.mcp_calls), ([], []))

    def test_dispatch_passes_headroom_floor_that_matches_declared(self):
        spec = ("Do the thing.\n" + SPEC_HEAD
                + "    mkdir -p .gate-logs && git check-ignore -q .gate-logs "
                  "&& scripts/guards/check-disk-headroom.sh .gate-logs 10\n")
        rc, out, err = self.dispatch(spec)
        # FLIP: a check that refuses any check-disk-headroom.sh invocation
        # outright, rather than only one whose floor argument differs from
        # the declared disk_floor_gb, would also refuse this filled-in,
        # matching spec.
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(len(self.mcp_calls), 1)


class Task(unittest.TestCase):
    """cmd_task against a stubbed cp_task: no control plane is reached."""

    def task(self, status, report, spec=None):
        t = {"status": status, "result": report, "result_ref": "", "results": {}}
        if spec is not None:
            t["spec"] = spec
        saved = cli.cp_task
        cli.cp_task = lambda task: t
        try:
            rc, out, _ = capture_main(["--json", "task", TASK_ID, "--full"])
            _, text, _ = capture_main(["task", TASK_ID])
        finally:
            cli.cp_task = saved
        return rc, json.loads(out), text

    def test_task_classifies_below_floor_report(self):
        report = ("Stopped at the headroom check.\n"
                  "EXECUTOR BELOW FLOOR: 12 GB of 40\nLinux x 7.0 x86_64\n16\n")
        for status in ("done", "failed"):
            rc, data, text = self.task(status, report)
            # FLIP: classifying by the control-plane status alone reports done
            # (and exit 0) or failed here.
            self.assertEqual(data["class"], "below-floor", status)
            self.assertIn("class=below-floor", text.splitlines()[0])
            self.assertEqual(rc, 1)
            self.assertFalse(data["claims_audit_missing"])
        rc, data, _ = self.task("done", "Quoted: the fixed EXECUTOR BELOW FLOOR: report.\n"
                                        "CLAIMS AUDIT: none\n")
        # FLIP: a substring match anywhere in the report, not at a line start,
        # calls this done task below-floor.
        self.assertEqual((data["class"], rc), ("done", 0))

    def test_task_flags_missing_claims_audit(self):
        report = "Implemented it. I ran the claims audit in my head.\nEXIT=0\n"
        rc, data, text = self.task("done", report)
        # FLIP: never flagging, or a case-insensitive substring search that
        # finds "claims audit" mid-line, leaves this false.
        self.assertTrue(data["claims_audit_missing"])
        self.assertIn("claims-audit-missing", text.splitlines()[0])
        self.assertEqual((data["class"], rc), ("done", 0))
        _, data, _ = self.task("done", report, spec="Steps.\nCLAIMS AUDIT, before the commit...\n")
        self.assertTrue(data["claims_audit_missing"])
        _, data, text = self.task("done", report, spec="Steps with no audit asked for.\n")
        # FLIP: ignoring a spec that is present flags this task, whose spec
        # never asked for an audit.
        self.assertFalse(data["claims_audit_missing"])
        self.assertNotIn("claims-audit-missing", text)
        _, data, _ = self.task("failed", report)
        self.assertFalse(data["claims_audit_missing"])

    def test_task_does_not_flag_a_report_with_claims_audit(self):
        for section in ("CLAIMS AUDIT: none written.", "## Claims audit", "claims audit:",
                        "### CLAIMS AUDIT"):
            _, data, text = self.task("done", f"Implemented it.\n{section}\n- a claim: a test\n")
            # FLIP: a match on the exact upper-case spelling, or one that
            # rejects a markdown heading, flags these.
            self.assertFalse(data["claims_audit_missing"], section)
            self.assertNotIn("claims-audit-missing", text)
            self.assertEqual(data["class"], "done")


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
