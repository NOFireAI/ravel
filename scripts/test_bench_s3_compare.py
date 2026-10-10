"""Unit tests for bench-s3-compare.py, scripts/bench/s3-envelope.py and the
committed bench/baselines/s3-envelope.json.

Discovered by `make test-python` (python3 -m unittest discover -p 'test_*.py'
from scripts/). Both tools have a dash in their name, so they are loaded by
path.
"""

import copy
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_TOOL = os.path.join(_HERE, "bench-s3-compare.py")
_SUMMARISER = os.path.join(_HERE, "bench", "s3-envelope.py")
_ENVELOPE = os.path.join(_ROOT, "bench", "baselines", "s3-envelope.json")
_GUIDE = os.path.join(_ROOT, "docs", "guides", "cost-model.md")
_WORKFLOW = os.path.join(_ROOT, ".github", "workflows", "bench-s3.yml")


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


bench_s3_compare = _load_module("bench_s3_compare", _TOOL)
s3_envelope = _load_module("s3_envelope", _SUMMARISER)


def _envelope_doc():
    with open(_ENVELOPE, encoding="utf-8") as fh:
        return json.load(fh)


def _report_from_run(doc, run):
    """A bench_report-shaped document carrying one envelope run's figures."""
    env = copy.deepcopy(doc["environment"])
    env["git_commit"] = run["git_commit"]
    env["toolchain"] = run["toolchain"]
    lat = run["strict_ack_latency_ms"]
    warm = run["warm_latency_ms"]
    req = run["s3_requests"]
    return {
        "environment": env,
        "ingest": {
            "strict_ack_latency_ms": dict(lat, max=lat["p99"], count=2500),
            "accepted_points": run["accepted_points"],
            "accepted_points_per_sec": 50000.0,
            "write_amplification": 1.0,
        },
        "query": {
            "cold_latency_ms": {"p50": run["cold_latency_ms_p50"], "p95": 0.0, "p99": 0.0,
                                "max": 0.0, "count": 1},
            "warm_latency_ms": dict(warm, max=warm["p99"], count=20),
            "matched_series": run["matched_series"],
        },
        "s3_requests": {
            "backend_bills_requests": True,
            "put": req["put"], "get": req["get"], "list": req["list"],
            "put_attempts": req["put_attempts"], "get_attempts": req["get_attempts"],
            "list_attempts": req["list_attempts"],
        },
        "bytes": dict(run["bytes"]),
    }


def _latest_report():
    doc = _envelope_doc()
    return _report_from_run(doc, doc["runs"][-1])


def _run(report, *extra):
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "report-s3.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(report, fh)
        return subprocess.run(
            [sys.executable, _TOOL, "--envelope", _ENVELOPE, "--report", path, *extra],
            capture_output=True, text=True, check=False,
        )


def _warned(stdout):
    return [line for line in stdout.splitlines() if line.startswith("::warning::")]


def _warned_figures(stdout):
    return [line[len("::warning::"):].split(" ", 1)[0] for line in _warned(stdout)]


class CommittedEnvelope(unittest.TestCase):
    def test_stored_envelope_matches_a_recompute_from_runs(self):
        doc = _envelope_doc()
        recomputed = {}
        for name, _rpath, run_path, _kind in bench_s3_compare.FIGURES:
            values = []
            for run in doc["runs"]:
                v = run
                for key in run_path:
                    v = v[key]
                values.append(v)
            recomputed[name] = {"min": min(values), "max": max(values)}
        self.assertEqual(doc["envelope"], recomputed)
        self.assertEqual(len(doc["envelope"]), 17)

    def test_summariser_check_agrees_and_catches_an_edit(self):
        ok = subprocess.run([sys.executable, _SUMMARISER, "check", _ENVELOPE],
                            capture_output=True, text=True, check=False)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        doc = _envelope_doc()
        doc["envelope"]["s3_requests.get"]["max"] += 1
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "e.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(doc, fh)
            bad = subprocess.run([sys.executable, _SUMMARISER, "check", path],
                                 capture_output=True, text=True, check=False)
        self.assertEqual(bad.returncode, 1)
        self.assertIn("s3_requests.get", bad.stderr)

    def test_summariser_refuses_an_exact_figure_that_varies(self):
        doc = _envelope_doc()
        runs = copy.deepcopy(doc["runs"])
        runs[-1]["s3_requests"]["put"] += 1
        with self.assertRaises(s3_envelope.InputError) as caught:
            s3_envelope.build_document(doc["environment"], runs, doc["_meta"]["runs_on"], [])
        self.assertIn("s3_requests.put", str(caught.exception))
        built = s3_envelope.build_document(doc["environment"], doc["runs"], doc["_meta"]["runs_on"], [])
        self.assertEqual(built["envelope"], doc["envelope"])

    def test_exact_figures_are_identical_across_every_run(self):
        doc = _envelope_doc()
        for name, _rpath, _run_path, kind in bench_s3_compare.FIGURES:
            if kind == "exact":
                self.assertEqual(doc["envelope"][name]["min"], doc["envelope"][name]["max"], name)
        self.assertEqual(
            sorted(n for n, _r, _p, k in bench_s3_compare.FIGURES if k == "exact"),
            ["ingest.accepted_points", "query.matched_series", "s3_requests.put"],
        )

    def test_meta_stamps(self):
        doc = _envelope_doc()
        meta = doc["_meta"]
        self.assertEqual(meta["workflow"], "bench-s3")
        self.assertEqual([r["run_id"] for r in meta["runs"]], [r["run_id"] for r in doc["runs"]])
        self.assertEqual([r["scheduled"] for r in meta["runs"]], [r["scheduled"] for r in doc["runs"]])
        self.assertEqual(len(doc["runs"]), 5)
        with open(_WORKFLOW, encoding="utf-8") as fh:
            workflow = fh.read()
        runs_on = re.findall(r"^\s+runs-on:\s*(\S+)\s*$", workflow, re.MULTILINE)
        self.assertEqual(runs_on, [meta["runs_on"]])
        # The load point the guide publishes is the lane's own invocation,
        # less the binary path and the output file.
        invocations = re.findall(r"\./target/release/(bench_report(?:[^\n]*\\\n)*[^\n]*)", workflow)
        self.assertEqual(len(invocations), 1)
        args = re.sub(r"\s*\\\n\s*", " ", invocations[0]).split()
        if "--out" in args:
            i = args.index("--out")
            del args[i:i + 2]
        self.assertEqual(" ".join(args), meta["load_point"])
        self.assertEqual(meta["region"], doc["environment"]["region"])
        self.assertEqual(meta["max_flush_delay_ms"], doc["environment"]["max_flush_delay_ms"])
        self.assertEqual(len(meta["unexplained"]), 3)
        for item in meta["unexplained"]:
            self.assertIn("issue #2686", item)

    def test_summarise_round_trips_reports_into_the_committed_runs(self):
        doc = _envelope_doc()
        with tempfile.TemporaryDirectory() as tmp:
            args = []
            for run in reversed(doc["runs"]):
                path = os.path.join(tmp, f"{run['run_id']}.json")
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(_report_from_run(doc, run), fh)
                args += ["--report", f"{run['run_id']}:{run['scheduled']}:{path}"]
            out = os.path.join(tmp, "out.json")
            unexplained = []
            for item in doc["_meta"]["unexplained"]:
                unexplained += ["--unexplained", item]
            res = subprocess.run(
                [sys.executable, _SUMMARISER, "summarise", "--out", out,
                 "--runs-on", doc["_meta"]["runs_on"], *args, *unexplained],
                capture_output=True, text=True, check=False)
            self.assertEqual(res.returncode, 0, res.stderr)
            with open(out, encoding="utf-8") as fh:
                rebuilt = json.load(fh)
        self.assertEqual(rebuilt, doc)

    def test_summarise_refuses_reports_from_different_environments(self):
        doc = _envelope_doc()
        with tempfile.TemporaryDirectory() as tmp:
            args = []
            for i, run in enumerate(doc["runs"][:2]):
                report = _report_from_run(doc, run)
                if i == 1:
                    report["environment"]["shard_count"] = 8
                path = os.path.join(tmp, f"{i}.json")
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(report, fh)
                args += ["--report", f"{run['run_id']}:{run['scheduled']}:{path}"]
            res = subprocess.run(
                [sys.executable, _SUMMARISER, "summarise", "--out", os.path.join(tmp, "o.json"),
                 "--runs-on", "ubuntu-latest", *args],
                capture_output=True, text=True, check=False)
        self.assertEqual(res.returncode, 2)
        self.assertIn("different environment", res.stderr)


class Compare(unittest.TestCase):
    def _assert_each_figure_printed_once(self, stdout):
        for name, _r, _p, _k in bench_s3_compare.FIGURES:
            lines = [l for l in stdout.splitlines() if l.startswith(name + ":")]
            self.assertEqual(len(lines), 1, name)
            self.assertIn("band", lines[0])
            self.assertIn("envelope min", lines[0])
            self.assertIn("max", lines[0])

    def test_latest_run_warns_nothing(self):
        res = _run(_latest_report())
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(_warned(res.stdout), [])
        self._assert_each_figure_printed_once(res.stdout)

    def test_strict_ack_p99_past_max_plus_26_percent_warns_on_exactly_it(self):
        report = _latest_report()
        hi = _envelope_doc()["envelope"]["ingest.strict_ack_latency_ms.p99"]["max"]
        report["ingest"]["strict_ack_latency_ms"]["p99"] = hi * 1.26
        res = _run(report)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(_warned_figures(res.stdout), ["ingest.strict_ack_latency_ms.p99"])
        self._assert_each_figure_printed_once(res.stdout)

    def test_enforce_turns_the_p99_case_into_exit_1(self):
        report = _latest_report()
        hi = _envelope_doc()["envelope"]["ingest.strict_ack_latency_ms.p99"]["max"]
        report["ingest"]["strict_ack_latency_ms"]["p99"] = hi * 1.26
        res = _run(report, "--enforce")
        self.assertEqual(res.returncode, 1, res.stdout)
        self.assertEqual(_warned_figures(res.stdout), ["ingest.strict_ack_latency_ms.p99"])

    def test_get_past_max_plus_11_percent_warns_on_exactly_it(self):
        report = _latest_report()
        hi = _envelope_doc()["envelope"]["s3_requests.get"]["max"]
        # get_attempts keeps the in-band value: the compare must read `get`.
        report["s3_requests"]["get"] = int(hi * 1.11)
        res = _run(report)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(_warned_figures(res.stdout), ["s3_requests.get"])

    def test_a_retry_regression_warns_on_the_billed_attempts(self):
        report = _latest_report()
        hi = _envelope_doc()["envelope"]["s3_requests.get_attempts"]["max"]
        # Every GET retried: the call count stays in band, the billed count does not.
        report["s3_requests"]["get_attempts"] = int(hi * 1.11)
        res = _run(report)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(_warned_figures(res.stdout), ["s3_requests.get_attempts"])

    def test_every_run_records_its_toolchain_and_attempts(self):
        for run in _envelope_doc()["runs"]:
            self.assertTrue(run["toolchain"].startswith("rustc "), run["run_id"])
            for k in ("put_attempts", "get_attempts", "list_attempts"):
                self.assertGreaterEqual(run["s3_requests"][k], run["s3_requests"][k[: -len("_attempts")]])

    def _assert_exact_off_by_one(self, section, key, name):
        report = _latest_report()
        report[section][key] += 1
        res = _run(report)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(_warned_figures(res.stdout), [name])
        line = [l for l in res.stdout.splitlines() if l.startswith(name + ":")][0]
        self.assertIn("OUTSIDE", line)
        self.assertIn("==", line)
        self.assertEqual(_run(report, "--enforce").returncode, 1)

    def test_put_off_by_one_fails_its_exact_check(self):
        self._assert_exact_off_by_one("s3_requests", "put", "s3_requests.put")

    def test_accepted_points_off_by_one_fails_its_exact_check(self):
        self._assert_exact_off_by_one("ingest", "accepted_points", "ingest.accepted_points")

    def test_matched_series_off_by_one_fails_its_exact_check(self):
        self._assert_exact_off_by_one("query", "matched_series", "query.matched_series")

    def test_a_different_region_refuses_naming_region(self):
        report = _latest_report()
        report["environment"]["region"] = "us-east-1"
        res = _run(report)
        self.assertEqual(res.returncode, 2)
        self.assertIn("not a comparison", res.stderr)
        self.assertIn("`region`", res.stderr)

    def test_a_different_workload_field_refuses_naming_it(self):
        report = _latest_report()
        report["environment"]["workload"]["batch_size"] = 100
        res = _run(report)
        self.assertEqual(res.returncode, 2)
        self.assertIn("`workload.batch_size`", res.stderr)

    def test_missing_cold_latency_fails_naming_it(self):
        report = _latest_report()
        del report["query"]["cold_latency_ms"]
        res = _run(report)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(len(_warned(res.stdout)), 1)
        self.assertIn("query.cold_latency_ms", _warned(res.stdout)[0])
        self._assert_each_figure_printed_once(res.stdout)
        self.assertEqual(_run(report, "--enforce").returncode, 1)


class GuideQuotesTheEnvelope(unittest.TestCase):
    """Every figure the cost-model guide's Measured envelope section quotes is
    read from the committed envelope, so the two cannot drift apart."""

    QUOTED = (
        "ingest.strict_ack_latency_ms.p50", "ingest.strict_ack_latency_ms.p95",
        "ingest.strict_ack_latency_ms.p99", "query.warm_latency_ms.p50",
        "query.warm_latency_ms.p95", "query.warm_latency_ms.p99",
        "query.cold_latency_ms.p50", "s3_requests.put", "s3_requests.get",
        "s3_requests.list",
    )

    @staticmethod
    def _section():
        with open(_GUIDE, encoding="utf-8") as fh:
            text = fh.read()
        start = text.index("## Measured envelope")
        end = text.index("\n## ", start + 1)
        return text[start:end]

    @staticmethod
    def _fmt(v):
        if isinstance(v, int):
            return f"{v:,}"
        return f"{v:,.0f}"

    def test_each_quoted_figure_appears_in_the_guide_row(self):
        section = self._section()
        env = _envelope_doc()["envelope"]
        for name in self.QUOTED:
            rows = [l for l in section.splitlines() if l.startswith(f"| `{name}` |")]
            self.assertEqual(len(rows), 1, name)
            cells = [c.strip() for c in rows[0].strip("|").split("|")]
            self.assertEqual(cells[1], self._fmt(env[name]["min"]), name)
            self.assertEqual(cells[2], self._fmt(env[name]["max"]), name)

    def test_section_names_provenance_issue_and_file(self):
        section = self._section()
        doc = _envelope_doc()
        with open(_GUIDE, encoding="utf-8") as fh:
            background = fh.read().split("\n## Background\n", 1)[1]
        # The docs check allows an issue number in a guide only under Background.
        self.assertIn("[Background](#background)", section)
        self.assertIn("unexplained items in the measured envelope: issue #2686",
                      " ".join(background.split()))
        self.assertIn("bench/baselines/s3-envelope.json", section)
        self.assertIn(doc["_meta"]["runs_on"], section)
        self.assertIn(doc["environment"]["region"], section)
        for run in doc["runs"]:
            self.assertIn(run["scheduled"], section)
        flat = " ".join(section.split())
        self.assertIn(f"`{doc['_meta']['load_point']}`", flat)
        self.assertIn(f"{doc['environment']['max_flush_delay_ms']} ms flush delay", flat)
        self.assertIn(f"{len(doc['runs'])} scheduled runs", flat.replace("five", "5"))
        p50 = doc["envelope"]["ingest.strict_ack_latency_ms.p50"]
        spread = round((p50["max"] / p50["min"] - 1.0) * 100)
        self.assertIn(f"up to {spread} percent on strict-ack p50", " ".join(section.split()))


def _spread_pct(env, name):
    """How far the envelope max sits above its min, in whole percent."""
    band = env[name]
    return round((band["max"] / band["min"] - 1.0) * 100)


class PercentagesQuoteTheEnvelope(unittest.TestCase):
    """Every percentage the compare tool's docstring and the baselines README
    state is either a band constant or a spread computed here from the
    committed envelope."""

    # Docstring phrase -> envelope figure whose spread it quotes.
    DOCSTRING_SPREADS = (
        ("strict-ack p50 by {} percent", "ingest.strict_ack_latency_ms.p50"),
        ("strict-ack p99 by {} percent", "ingest.strict_ack_latency_ms.p99"),
        ("warm p50 by {} percent", "query.warm_latency_ms.p50"),
        ("cold p50 by {} percent", "query.cold_latency_ms.p50"),
        ("by {} percent, stepping up", "s3_requests.get"),
        ("bytes read by {} percent", "bytes.read"),
    )

    @staticmethod
    def _bands():
        return [f"plus {pct:g} percent" for pct in
                (bench_s3_compare.LATENCY_BAND_PCT, bench_s3_compare.COUNT_BAND_PCT)]

    def _assert_only_known_percentages(self, flat, known):
        stated = re.findall(r"\S+ \d+ percent", flat)
        self.assertTrue(stated)
        for phrase in stated:
            self.assertTrue(any(phrase in k for k in known), phrase)

    def test_docstring_spreads_are_computed_from_the_envelope(self):
        env = _envelope_doc()["envelope"]
        flat = " ".join(bench_s3_compare.__doc__.split())
        known = list(self._bands())
        for template, name in self.DOCSTRING_SPREADS:
            phrase = template.format(_spread_pct(env, name))
            self.assertIn(phrase, flat, name)
            known.append(phrase)
        get = env["s3_requests.get"]
        self.assertIn(f"GET spans {get['min']:,} to {get['max']:,}", flat)
        self._assert_only_known_percentages(flat, known)

    def test_docstring_step_dates_are_where_get_steps(self):
        doc = _envelope_doc()
        flat = " ".join(bench_s3_compare.__doc__.split())
        runs = doc["runs"]
        jumps = [(runs[i + 1]["s3_requests"]["get"] - runs[i]["s3_requests"]["get"], i)
                 for i in range(len(runs) - 1)]
        _, i = max(jumps)
        self.assertIn(f"between the {runs[i]['scheduled']} and {runs[i + 1]['scheduled']} runs",
                      flat)

    def test_readme_percentages_are_the_band_constants(self):
        with open(os.path.join(_ROOT, "bench", "baselines", "README.md"),
                  encoding="utf-8") as fh:
            flat = " ".join(fh.read().split())
        bands = list(self._bands())
        for band in bands:
            self.assertIn(band, flat)
        self._assert_only_known_percentages(flat, bands)


if __name__ == "__main__":
    unittest.main()
