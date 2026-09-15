"""Unit tests for bench-compare.py (ADR-0070 tier B).

Discovered by `make test-python` (python3 -m unittest discover -p 'test_*.py'
from scripts/). bench-compare.py has a dash in its name and so cannot be
imported normally; load it by path.
"""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(_HERE, "bench-compare.py")

_spec = importlib.util.spec_from_file_location("bench_compare", _TOOL)
bench_compare = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_compare)


def _estimates(dirpath, median, mean):
    os.makedirs(dirpath, exist_ok=True)
    with open(os.path.join(dirpath, "estimates.json"), "w", encoding="utf-8") as fh:
        json.dump(
            {"median": {"point_estimate": median}, "mean": {"point_estimate": mean}},
            fh,
        )


def _run(*args):
    return subprocess.run(
        [sys.executable, _TOOL, *args],
        capture_output=True,
        text=True,
        check=False,
    )


class PctTest(unittest.TestCase):
    def test_basic(self):
        self.assertAlmostEqual(bench_compare._pct(100, 130), 30.0)
        self.assertAlmostEqual(bench_compare._pct(100, 80), -20.0)
        self.assertEqual(bench_compare._pct(0, 0), 0.0)
        self.assertEqual(bench_compare._pct(0, 5), float("inf"))


class _CriterionCase(unittest.TestCase):
    """Shared fixture: a synthetic criterion tree and a collect helper."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _make_criterion(self, name, values):
        root = os.path.join(self.tmp, name)
        for bench_id, (median, mean) in values.items():
            _estimates(os.path.join(root, bench_id, "new"), median, mean)
        return root

    def _collect(self, name, values, label, knobs=None):
        root = self._make_criterion(name, values)
        out = os.path.join(self.tmp, f"{name}.json")
        extra = []
        for pair in knobs or []:
            extra += ["--knob", pair]
        res = _run("collect", "--criterion-dir", root, "--out", out,
                   "--label", label, *extra)
        self.assertEqual(res.returncode, 0, res.stderr)
        return out


class EndToEndTest(_CriterionCase):
    def test_collect_ignores_base_dir(self):
        # A saved base/ copy must not be picked up; only new/ counts.
        root = os.path.join(self.tmp, "c")
        _estimates(os.path.join(root, "g/b/new"), 100, 110)
        _estimates(os.path.join(root, "g/b/base"), 999, 999)
        out = os.path.join(self.tmp, "c.json")
        res = _run("collect", "--criterion-dir", root, "--out", out, "--label", "x")
        self.assertEqual(res.returncode, 0, res.stderr)
        with open(out, encoding="utf-8") as fh:
            doc = json.load(fh)
        self.assertEqual(list(doc["benchmarks"]), ["g/b"])
        self.assertEqual(doc["benchmarks"]["g/b"]["median_ns"], 100.0)

    def test_collect_empty_fails(self):
        empty = os.path.join(self.tmp, "empty")
        os.makedirs(empty)
        res = _run("collect", "--criterion-dir", empty, "--out", "-", "--label", "x")
        self.assertEqual(res.returncode, 2)

    def test_regression_advisory_is_green_enforce_is_red(self):
        base = self._collect("base", {"g/a": (100.0, 100.0)}, "base")
        cur = self._collect("cur", {"g/a": (130.0, 130.0)}, "cur")
        adv = _run("compare", "--baseline", base, "--current", cur, "--threshold", "15")
        self.assertEqual(adv.returncode, 0, adv.stdout)
        self.assertIn("regression", adv.stdout)
        enf = _run("compare", "--baseline", base, "--current", cur,
                   "--threshold", "15", "--enforce")
        self.assertEqual(enf.returncode, 1, enf.stdout)

    def test_within_threshold_is_green_even_enforcing(self):
        base = self._collect("base", {"g/a": (100.0, 100.0)}, "base")
        cur = self._collect("cur", {"g/a": (114.0, 114.0)}, "cur")
        enf = _run("compare", "--baseline", base, "--current", cur,
                   "--threshold", "15", "--enforce")
        self.assertEqual(enf.returncode, 0, enf.stdout)
        self.assertIn("No regression", enf.stdout)

    def test_missing_bench_fails_closed_under_enforce(self):
        base = self._collect("base", {"g/a": (100.0, 100.0), "g/b": (50.0, 50.0)}, "base")
        cur = self._collect("cur", {"g/a": (101.0, 101.0)}, "cur")
        enf = _run("compare", "--baseline", base, "--current", cur,
                   "--threshold", "15", "--enforce")
        self.assertEqual(enf.returncode, 1, enf.stdout)
        self.assertIn("missing", enf.stdout)
        # Advisory still exits 0 even with the gap.
        adv = _run("compare", "--baseline", base, "--current", cur, "--threshold", "15")
        self.assertEqual(adv.returncode, 0, adv.stdout)


class KnobDriftTest(_CriterionCase):
    """A knob that appears in a bench id renames its arm, so a mismatched pair
    drops the arm from the comparison instead of comparing it. The compare must
    say so, and must not call a pair comparable when it cannot tell."""

    def test_knobs_are_stamped_and_matching_knobs_report_nothing(self):
        base = self._collect("base", {"g/a_2000": (100.0, 100.0)}, "base",
                             knobs=["RAVEL_BENCH_MAX_SERIES=2000", "BENCH_WARMUP=1"])
        with open(base, encoding="utf-8") as fh:
            doc = json.load(fh)
        self.assertEqual(
            doc["_meta"]["knobs"],
            {"RAVEL_BENCH_MAX_SERIES": "2000", "BENCH_WARMUP": "1"},
        )
        cur = self._collect("cur", {"g/a_2000": (101.0, 101.0)}, "cur",
                            knobs=["RAVEL_BENCH_MAX_SERIES=2000", "BENCH_WARMUP=1"])
        res = _run("compare", "--baseline", base, "--current", cur,
                   "--threshold", "15", "--enforce")
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertNotIn("sampling knobs", res.stdout)

    def test_mismatched_knobs_are_named_and_fail_closed_under_enforce(self):
        # The arms line up by name here, so nothing else in the report objects:
        # without the knob check this pair reads as a clean pass.
        base = self._collect("base", {"g/a": (100.0, 100.0)}, "base",
                             knobs=["RAVEL_BENCH_MAX_SERIES=2000"])
        cur = self._collect("cur", {"g/a": (100.0, 100.0)}, "cur",
                            knobs=["RAVEL_BENCH_MAX_SERIES=100000"])
        enf = _run("compare", "--baseline", base, "--current", cur,
                   "--threshold", "15", "--enforce")
        self.assertEqual(enf.returncode, 1, enf.stdout)
        self.assertIn("MISMATCH", enf.stdout)
        self.assertIn("RAVEL_BENCH_MAX_SERIES", enf.stdout)
        self.assertIn("baseline `2000`", enf.stdout)
        self.assertIn("current `100000`", enf.stdout)
        # Advisory says the same thing and still exits 0, per ADR-0070 D3.
        adv = _run("compare", "--baseline", base, "--current", cur, "--threshold", "15")
        self.assertEqual(adv.returncode, 0, adv.stdout)
        self.assertIn("MISMATCH", adv.stdout)

    def test_unrecorded_knobs_report_unknown_not_agreement(self):
        base = self._collect("base", {"g/a": (100.0, 100.0)}, "base")
        cur = self._collect("cur", {"g/a": (100.0, 100.0)}, "cur",
                            knobs=["RAVEL_BENCH_MAX_SERIES=2000"])
        res = _run("compare", "--baseline", base, "--current", cur,
                   "--threshold", "15", "--enforce")
        # Cannot check is not the same as checked and agreed: it says so, and
        # it does not fail a run whose baseline predates the stamping.
        self.assertEqual(res.returncode, 0, res.stdout)
        self.assertIn("NOT RECORDED on the baseline file", res.stdout)
        self.assertNotIn("MISMATCH", res.stdout)

    def test_malformed_knob_is_refused(self):
        root = self._make_criterion("m", {"g/a": (100.0, 100.0)})
        out = os.path.join(self.tmp, "m.json")
        res = _run("collect", "--criterion-dir", root, "--out", out,
                   "--label", "m", "--knob", "no-equals-sign")
        self.assertEqual(res.returncode, 2, res.stdout)
        self.assertIn("KEY=VALUE", res.stderr)


if __name__ == "__main__":
    unittest.main()
