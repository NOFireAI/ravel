#!/usr/bin/env python3
"""Hermetic unit tests for scripts/check_readme_commands.py.

No network, no docker, no MinIO: the extractor is tested against the fixture
markdown, and the evaluator is fed canned command results. The live command
runner lives in check-readme-commands.sh and is deliberately not exercised here.

Run: cd scripts && python3 -m unittest test_check_readme_commands -v
"""

import os
import unittest

from check_readme_commands import (
    MarkerError,
    extract_blocks,
    parse_expectations,
    evaluate,
    evaluate_ok,
)

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "readme-commands.md")


def load_fixture():
    with open(FIXTURE, "r", encoding="utf-8") as handle:
        return handle.read()


class TestExtractor(unittest.TestCase):
    def setUp(self):
        self.blocks = extract_blocks(load_fixture())

    def test_extracts_only_marked_blocks(self):
        # Three marked blocks; the unmarked `echo` block is ignored.
        self.assertEqual(len(self.blocks), 3)

    def test_unmarked_block_never_extracted(self):
        for block in self.blocks:
            self.assertNotIn("THIS_MUST_NEVER_RUN", block.command)

    def test_multiline_command_survives_intact(self):
        multiline = [b for b in self.blocks if "\n" in b.command]
        self.assertEqual(len(multiline), 1)
        cmd = multiline[0].command
        # Every source line of the block is preserved, backslash continuations
        # and all.
        self.assertIn("curl -s \\", cmd)
        self.assertIn('-H "Authorization: Bearer demo-token"', cmd)
        self.assertIn('"http://127.0.0.1:4318/api/v1/query?query=up"', cmd)
        self.assertEqual(cmd.count("\n"), 2)

    def test_reports_source_line_numbers(self):
        for block in self.blocks:
            self.assertGreater(block.line, 0)
        # Blocks are returned in document order.
        lines = [b.line for b in self.blocks]
        self.assertEqual(lines, sorted(lines))

    def test_error_envelope_block_asserts_success(self):
        # The last marked block is the error-envelope case.
        block = self.blocks[-1]
        self.assertEqual(block.raw_expectation, "json:.status=success")


class TestExtractorFailsClosed(unittest.TestCase):
    def test_marker_without_expectation_is_error(self):
        text = "<!-- ravel:run -->\n```\ncurl x\n```\n"
        with self.assertRaises(MarkerError):
            extract_blocks(text)

    def test_unknown_keyword_is_error(self):
        text = "<!-- ravel:run teapot=418brew -->\n```\ncurl x\n```\n"
        with self.assertRaises(MarkerError):
            extract_blocks(text)

    def test_marker_not_before_fence_is_error(self):
        text = "<!-- ravel:run status=200 -->\n\nprose, not a fence\n"
        with self.assertRaises(MarkerError):
            extract_blocks(text)

    def test_unterminated_fence_is_error(self):
        text = "<!-- ravel:run status=200 -->\n```\ncurl x\n"
        with self.assertRaises(MarkerError):
            extract_blocks(text)

    def test_ravel_run_inside_unmarked_fence_is_not_a_marker(self):
        # A `ravel:run` string that lives inside an unmarked code sample must
        # not be picked up: it is body, not a marker.
        text = "```\n<!-- ravel:run status=200 -->\necho hi\n```\n"
        self.assertEqual(extract_blocks(text), [])


class TestParseExpectations(unittest.TestCase):
    def test_empty_raises(self):
        with self.assertRaises(MarkerError):
            parse_expectations("")
        with self.assertRaises(MarkerError):
            parse_expectations("   ")

    def test_unknown_keyword_raises(self):
        with self.assertRaises(MarkerError):
            parse_expectations("definitely-not-a-keyword")

    def test_status_requires_integer(self):
        with self.assertRaises(MarkerError):
            parse_expectations("status=twohundred")
        self.assertEqual(parse_expectations("status=200"), [{"kind": "status", "code": 200}])

    def test_json_requires_path_and_value(self):
        with self.assertRaises(MarkerError):
            parse_expectations("json:.status")  # no =value
        self.assertEqual(
            parse_expectations("json:.data.resultType=vector"),
            [{"kind": "json", "path": ["data", "resultType"], "value": "vector"}],
        )

    def test_nonempty_requires_path(self):
        with self.assertRaises(MarkerError):
            parse_expectations("nonempty:")
        self.assertEqual(
            parse_expectations("nonempty:.data.result"),
            [{"kind": "nonempty", "path": ["data", "result"]}],
        )

    def test_multiple_expectations_semicolon_separated(self):
        parsed = parse_expectations("status=200; json:.status=success")
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0]["kind"], "status")
        self.assertEqual(parsed[1]["kind"], "json")


def result(body="", http_status=200, exit_code=0):
    return {"exit": exit_code, "http_status": http_status, "body": body}


class TestEvaluateStatus(unittest.TestCase):
    def test_matching_status_passes(self):
        exp = parse_expectations("status=200")
        self.assertTrue(evaluate_ok(exp, result(http_status=200)))

    def test_wrong_status_fails(self):
        exp = parse_expectations("status=200")
        # curl exits 0 on a 401; the exit code would pass, the status must not.
        self.assertFalse(evaluate_ok(exp, result(http_status=401, exit_code=0)))

    def test_missing_status_fails(self):
        exp = parse_expectations("status=200")
        self.assertFalse(evaluate_ok(exp, result(http_status=None)))


class TestEvaluateJson(unittest.TestCase):
    def test_field_equals_passes(self):
        exp = parse_expectations("json:.status=success")
        self.assertTrue(evaluate_ok(exp, result(body='{"status":"success"}')))

    def test_error_envelope_fails_despite_exit_zero(self):
        # THE assertion the whole epic depends on: exit 0, HTTP 200, but the
        # body is an error envelope. An exit-code-only check would pass this.
        exp = parse_expectations("json:.status=success")
        res = result(body='{"status":"error","error":"boom"}', http_status=200, exit_code=0)
        self.assertFalse(evaluate_ok(exp, res))
        # And the failure detail names what went wrong.
        outcomes = evaluate(exp, res)
        self.assertEqual(len(outcomes), 1)
        ok, detail = outcomes[0]
        self.assertFalse(ok)
        self.assertIn("status", detail)

    def test_error_envelope_from_fixture_block(self):
        # Drive it through the actual fixture block, not a hand-written body,
        # so the marker the fixture declares is the marker under test.
        block = extract_blocks(load_fixture())[-1]
        res = result(body='{"status":"error","error":"bad query"}')
        self.assertFalse(evaluate_ok(block.expectations, res))

    def test_absent_path_fails(self):
        exp = parse_expectations("json:.status=success")
        self.assertFalse(evaluate_ok(exp, result(body='{"other":"success"}')))

    def test_non_json_body_fails(self):
        exp = parse_expectations("json:.status=success")
        self.assertFalse(evaluate_ok(exp, result(body="<html>401 Unauthorized</html>")))

    def test_array_index_path(self):
        exp = parse_expectations("json:.data.0=first")
        self.assertTrue(evaluate_ok(exp, result(body='{"data":["first","second"]}')))


class TestEvaluateNonEmpty(unittest.TestCase):
    def test_non_empty_array_passes(self):
        exp = parse_expectations("nonempty:.data.result")
        self.assertTrue(evaluate_ok(exp, result(body='{"data":{"result":[1,2]}}')))

    def test_empty_array_fails(self):
        exp = parse_expectations("nonempty:.data.result")
        self.assertFalse(evaluate_ok(exp, result(body='{"data":{"result":[]}}')))

    def test_absent_path_fails(self):
        exp = parse_expectations("nonempty:.data.result")
        self.assertFalse(evaluate_ok(exp, result(body='{"data":{}}')))


class TestEvaluateCombined(unittest.TestCase):
    def test_all_must_hold(self):
        exp = parse_expectations("status=200; json:.status=success")
        good = result(body='{"status":"success"}', http_status=200)
        self.assertTrue(evaluate_ok(exp, good))
        # Right body, wrong status: still a failure.
        bad = result(body='{"status":"success"}', http_status=500)
        self.assertFalse(evaluate_ok(exp, bad))


if __name__ == "__main__":
    unittest.main()
