#!/usr/bin/env python3
"""Cases of the selective-investigations harness (issue #3699), run by
investigations.test.sh. No AWS, no server, no network.

    investigations_cases.py [unittest args]
    investigations_cases.py --refresh-local-smoke

--refresh-local-smoke re-evaluates fixtures/local-smoke/ and rewrites its
results.local.json.gz and summary.local.txt: run it after a deliberate change
to evaluate.py, bands.toml or statements.toml, and review the diff of
summary.local.txt before committing it.
"""

import copy
import gzip
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
EVALUATE = os.path.join(HERE, "evaluate.py")
GEN = os.path.join(HERE, "gen_corpus.py")
STATEMENTS = os.path.join(HERE, "statements.toml")
BANDS = os.path.join(HERE, "bands.toml")
HITS_TRUTH = os.path.join(HERE, "hits.truth.json")
SMOKE = os.path.join(HERE, "fixtures", "local-smoke")
BOX = os.path.join(HERE, "box")
PY = sys.executable
HOUR_NS = 3600 * 1_000_000_000
STATES = ("unfolded", "folded_no_leaves", "folded_leaves", "mixed", "many_parts")
UNSUPPORTED = "unsupported-on-this-build"


def gen(out, traces=1000, hours=2, seed=7, *extra):
    subprocess.run([PY, GEN, "--traces", str(traces), "--hours", str(hours), "--seed", str(seed),
                    "--spans-per-trace", "10", "--logs-per-trace", "5", "--format", "jsonl",
                    "--out", out, *extra], check=True, capture_output=True)
    with open(os.path.join(out, "truth.json")) as f:
        truth = json.load(f)
    rows = {}
    for sig in ("spans", "logs"):
        with open(os.path.join(out, f"{sig}.jsonl")) as f:
            rows[sig] = [json.loads(line) for line in f]
    return truth, rows


def read_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def write_jsonl(path, lines):
    with open(path, "w") as f:
        for line in lines:
            f.write(json.dumps(line, sort_keys=True) + "\n")


def gunzip_to(src, dst):
    with gzip.open(src, "rb") as f, open(dst, "wb") as g:
        g.write(f.read())


def smoke_run(dst):
    """The committed local-smoke run's inputs, unpacked into dst."""
    os.makedirs(dst)
    shutil.copy(os.path.join(SMOKE, "run.toml"), dst)
    shutil.copytree(os.path.join(SMOKE, "stamps"), os.path.join(dst, "stamps"))
    gunzip_to(os.path.join(SMOKE, "calls.jsonl.gz"), os.path.join(dst, "calls.jsonl"))
    gunzip_to(os.path.join(SMOKE, "truth.json.gz"), os.path.join(dst, "truth.json"))
    return dst


def evaluate(run, out, bands=BANDS, evaluate_py=EVALUATE):
    """(exit code, results, summary text) of one evaluate.py run."""
    p = subprocess.run([PY, evaluate_py, "--run", run, "--out-dir", out, "--statements", STATEMENTS,
                        "--bands", bands, "--hits-truth", HITS_TRUTH], capture_output=True, text=True)
    res = text = None
    for name in os.listdir(out) if os.path.isdir(out) else []:
        if name.startswith("results."):
            with open(os.path.join(out, name)) as f:
                res = json.load(f)
        elif name.startswith("summary."):
            with open(os.path.join(out, name)) as f:
                text = f.read()
    return p.returncode, res, text or p.stdout + p.stderr


def figure(res, fid):
    hits = [f for f in res["figures"] if f["id"] == fid]
    assert len(hits) == 1, f"{fid} appears {len(hits)} times"
    return hits[0]


# A bands file every figure of which the local smoke run passes: one counter
# figure per kind (statement answer, fold report, cost) and one timing figure
# whose target no run meets, so only the lane decides its verdict.
PASS_BANDS = """
[[row]]
key = "row_t1"
adr_row = 1
check = "case"
target = "case"
miss = "case"
source = "predicted"
requires_tenant = "inv"
requires_states = ["folded_leaves"]

[[row.figure]]
name = "spans_1_rows_exact"
statement = "spans_in_1"
state = "folded_leaves"
metric = "cold.rows_exact"
lane = "counter"
target_if = "v == 1"

[[row.figure]]
name = "present_rows_exact"
statement = "logs_request_id_present"
state = "folded_leaves"
metric = "cold.rows_exact"
lane = "counter"
target_if = "v == 1"

[[row.figure]]
name = "spans_leaves"
statement = "fold"
state = "folded_leaves"
transition = "leaf_rebuild"
metric = "fold.spans.leaves_written"
lane = "counter"
target_if = "v >= 1"

[[row.figure]]
name = "cost_figures"
statement = "all"
state = "folded_leaves"
metric = "cost.figures_without_basis"
lane = "counter"
target_if = "v == 0"

[[row.figure]]
name = "spans_1_wall"
statement = "spans_in_1"
state = "folded_leaves"
metric = "cold.wall_p50_s"
lane = "timing"
target_if = "v < 0"
"""


class Workdir(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def path(self, *parts):
        return os.path.join(self.tmp, *parts)

    def pass_bands(self, text=PASS_BANDS):
        p = self.path("bands.toml")
        with open(p, "w") as f:
            f.write(text)
        return p


# --- gen_corpus.py --------------------------------------------------------------


class Corpus(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.truth, cls.rows = gen(os.path.join(cls._tmp.name, "s1"))
        cls.truth10, cls.rows10 = gen(os.path.join(cls._tmp.name, "s10"), 1000, 2, 7, "--scale", "10")

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def hour(self, truth, ts):
        return ts // HOUR_NS - truth["generator"]["start_hour"]

    def roots(self, rows):
        return {r["trace_id"]: r for r in rows["spans"] if r["parent_span_id"] is None}

    def test_rows_recounted_from_the_data(self):
        for truth, rows, traces in ((self.truth, self.rows, 1000), (self.truth10, self.rows10, 10000)):
            self.assertEqual(len(rows["spans"]), traces * 10)
            self.assertEqual(len(rows["logs"]), traces * 5)
            self.assertEqual(truth["rows_written"], {"spans": traces * 10, "logs": traces * 5})

    def test_scale_10_adds_exactly_nine_times_the_unplanted_rows(self):
        self.assertEqual(self.truth["unplanted_rows"], {"spans": 0, "logs": 0, "noise_traces": 0})
        self.assertEqual(self.truth10["unplanted_rows"], {"spans": 90000, "logs": 45000, "noise_traces": 9000})
        base = {json.dumps(r, sort_keys=True) for r in self.rows["spans"]}
        self.assertTrue(base <= {json.dumps(r, sort_keys=True) for r in self.rows10["spans"]})
        for key in ("failing_cell", "needles", "in_lists", "saved_traces", "request_id"):
            self.assertEqual(self.truth[key], self.truth10[key], key)

    def test_hour_is_a_function_of_the_timestamp_and_rows_share_their_trace_hour(self):
        for truth, rows in ((self.truth, self.rows), (self.truth10, self.rows10)):
            roots = self.roots(rows)
            hours = truth["generator"]["hours"]
            for r in rows["spans"]:
                h = self.hour(truth, r["start_ts"])
                self.assertTrue(0 <= h < hours)
                self.assertEqual(h, self.hour(truth, roots[r["trace_id"]]["start_ts"]))
                self.assertEqual(self.hour(truth, r["end_ts"]), h)
            for r in rows["logs"]:
                self.assertEqual(self.hour(truth, r["ts"]), self.hour(truth, roots[r["trace_id"]]["start_ts"]))
            for s in truth["saved_traces"]:
                self.assertEqual(s["hour"], self.hour(truth, s["start_ns"]))

    def test_traces_are_written_sorted_by_start(self):
        for rows in (self.rows, self.rows10):
            starts = [r["start_ts"] for r in rows["spans"] if r["parent_span_id"] is None]
            self.assertEqual(starts, sorted(starts))

    def test_failing_cell_is_exactly_the_planted_traces(self):
        for truth, rows in ((self.truth, self.rows), (self.truth10, self.rows10)):
            cell = truth["failing_cell"]
            got = sorted(r["trace_id"] for r in rows["spans"]
                         if r["service"] == cell["service"] and r["status_code"] == 2
                         and cell["hour_start_ns"] <= r["start_ts"] < cell["hour_end_ns"])
            self.assertEqual(got, cell["trace_ids"])
            self.assertEqual(cell["count"], 50)
            self.assertFalse(cell["capped"])
            self.assertEqual(truth["answers"]["failing_cell.trace_ids"]["row_set"], got)
            n = sum(1 for r in rows["logs"] if r["trace_id"] in set(got))
            self.assertEqual(truth["answers"]["failing_cell.logs_count"]["value"], n)

    def test_status_is_an_integer_otlp_code(self):
        codes = {r["status_code"] for r in self.rows10["spans"]}
        self.assertEqual(codes, {0, 1, 2})
        self.assertTrue(all(type(c) is int for c in codes))

    def test_request_id_answers_recount(self):
        t, logs = self.truth, self.rows["logs"]
        present = [r for r in logs if r["request_id"] == t["request_id"]["present"]]
        self.assertEqual(len(present), 1)
        self.assertEqual(t["answers"]["request_id.present"], {"rows": 1, "row_set": [present[0]["trace_id"]]})
        self.assertFalse([r for r in logs if r["request_id"] == t["request_id"]["absent"]])
        counts = {}
        for r in logs:
            counts[r["request_id"]] = counts.get(r["request_id"], 0) + 1
        for size, ids in t["in_lists"]["request_id"].items():
            self.assertEqual(len(ids), int(size))
            self.assertEqual(len(set(ids)), int(size))
            self.assertNotIn(t["request_id"]["present"], ids)
            self.assertEqual(sum(counts.get(i, 0) for i in ids), int(size))
            self.assertEqual(t["answers"][f"in_request_id.{size}"]["rows"], int(size))

    def test_trace_id_in_lists_recount(self):
        t = self.truth
        for size, ids in t["in_lists"]["trace_id"].items():
            ids = set(ids)
            spans = sum(1 for r in self.rows["spans"] if r["trace_id"] in ids)
            logs = sum(1 for r in self.rows["logs"] if r["trace_id"] in ids)
            self.assertEqual(t["answers"][f"in_trace_id.{size}.spans"]["rows"], spans)
            self.assertEqual(t["answers"][f"in_trace_id.{size}.logs"]["rows"], logs)
            self.assertEqual(spans, int(size) * 10)

    def test_needle_tiers_recount_and_are_distinguishable(self):
        t = self.truth
        counts = {v: sum(1 for r in self.rows["logs"] if r["tag"] == v) for v in t["needles"]}
        self.assertEqual(counts, t["needles"])
        for v, n in counts.items():
            self.assertEqual(n, sum(1 for r in self.rows["logs"] if r["body"].endswith(" " + v)))
            self.assertEqual(t["answers"][f"needle.{v}"]["value"], n)
        self.assertEqual(counts["needlerare"], 1)
        self.assertLess(counts["needlerare"], counts["needlesparse"])
        self.assertLess(counts["needlesparse"], counts["needlecommon"])

    def test_needle_blocks_recount_within_20_percent(self):
        # Item 7: tiers are ordered by blocks touched, and a recount over the
        # written file in BLOCK_ROWS-row blocks agrees with truth within 20 %.
        for truth, rows in ((self.truth, self.rows), (self.truth10, self.rows10)):
            br = truth["generator"]["block_rows"]
            nb = truth["needle_blocks"]
            for v, want in nb.items():
                got = len({i // br for i, r in enumerate(rows["logs"]) if r["tag"] == v})
                self.assertLessEqual(abs(got - want), 0.2 * want, (v, got, want))
            self.assertEqual(nb["needlerare"], 1)
            self.assertLess(nb["needlerare"], nb["needlesparse"])
            self.assertLess(nb["needlesparse"], nb["needlecommon"])
            # One row per block: the planted row count equals the block count.
            self.assertEqual(nb, truth["needles"])

    def test_needle_tiers_are_one_tenth_percent_and_ten_percent_of_blocks(self):
        spec = importlib.util.spec_from_file_location("gen_corpus", GEN)
        g = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(g)
        n_blocks = -(-100_000_000 // 256)
        got = g.needle_block_counts(100_000_000)
        self.assertEqual(got["needlerare"], 1)
        self.assertEqual(got["needlesparse"], round(0.001 * n_blocks))
        self.assertEqual(got["needlecommon"], round(0.10 * n_blocks))

    def test_tail_corpus_plants_its_own_request_id(self):
        with tempfile.TemporaryDirectory() as d:
            truth, rows = gen(d, 200, 1, 8, "--tail")
        self.assertTrue(truth["generator"]["tail"])
        self.assertEqual(truth["request_id"]["present"], "req-planted-tail-0001")
        hit = [r for r in rows["logs"] if r["request_id"] == "req-planted-tail-0001"]
        self.assertEqual(len(hit), 1)
        self.assertFalse(any(r["request_id"] == "req-planted-present-0001" for r in rows["logs"]))
        self.assertEqual(truth["answers"]["request_id.present"]["row_set"], [hit[0]["trace_id"]])

    def test_erasure_victim_is_read_by_no_statement(self):
        with tempfile.TemporaryDirectory() as d:
            truth, rows = gen(d, 3000, 4, 9)
        v = truth["erasure_victim"]
        self.assertIsNotNone(v)
        row = [r for r in rows["logs"] if r["request_id"] == v["request_id"]]
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["trace_id"], v["trace_id"])
        self.assertIsNone(row[0]["tag"])
        busy = {s["trace_id"] for s in truth["saved_traces"]} | set(truth["failing_cell"]["trace_ids"])
        busy |= {t for ids in truth["in_lists"]["trace_id"].values() for t in ids}
        self.assertNotIn(v["trace_id"], busy)
        self.assertNotIn(v["request_id"], {r for ids in truth["in_lists"]["request_id"].values() for r in ids})
        self.assertNotEqual(self.hour(truth, row[0]["ts"]), truth["failing_cell"]["hour"])

    def test_saved_trace_answers_recount(self):
        for n, s in enumerate(self.truth["saved_traces"]):
            ids = sorted(r["span_id"] for r in self.rows["spans"] if r["trace_id"] == s["trace_id"])
            self.assertEqual(self.truth["answers"][f"saved_trace.{n}.spans"], {"rows": 10, "row_set": ids})

    def test_journey_c_build_sides_recount(self):
        for truth, rows in ((self.truth, self.rows), (self.truth10, self.rows10)):
            roots = self.roots(rows)
            for k, jc in truth["journey_c"].items():
                build = {tid for tid, r in roots.items() if jc["start_ns"] <= r["start_ts"] < jc["cut_ns"]}
                self.assertEqual(len(build), jc["build"])
                self.assertEqual(jc["capped"], jc["build"] < int(k))
                n = sum(1 for r in rows["logs"] if r["trace_id"] in build)
                self.assertEqual(truth["answers"][f"journey_c.{k}"]["value"], n)
                self.assertEqual(truth["answers"][f"journey_c.{k}"]["capped"], jc["capped"])
        self.assertTrue(self.truth["journey_c"]["10240"]["capped"])
        self.assertFalse(self.truth["journey_c"]["1"]["capped"])

    def test_capped_is_real(self):
        with tempfile.TemporaryDirectory() as d:
            truth, _ = gen(d, 60, 2, 7)
        cell = truth["failing_cell"]
        self.assertTrue(cell["capped"])
        self.assertEqual(cell["count"], len(cell["trace_ids"]))
        self.assertLess(cell["count"], 50)
        self.assertTrue(truth["answers"]["in_trace_id.100.spans"]["capped"])
        self.assertEqual(truth["answers"]["in_trace_id.100.spans"]["rows"], 600)
        self.assertFalse(self.truth["answers"]["in_trace_id.1000.spans"]["capped"])

    def test_seeded_and_deterministic(self):
        with tempfile.TemporaryDirectory() as d:
            gen(d)
            for name in ("truth.json", "spans.jsonl", "logs.jsonl"):
                with open(os.path.join(d, name), "rb") as a, open(os.path.join(self._tmp.name, "s1", name), "rb") as b:
                    self.assertEqual(a.read(), b.read(), name)

    def test_mappings_parse_and_name_real_columns(self):
        for sig in ("spans", "logs"):
            with open(os.path.join(self._tmp.name, "s1", f"{sig}.mapping.toml"), "rb") as f:
                m = tomllib.load(f)[sig]
            fields = set(self.rows[sig][0])
            cols = [v for k, v in m.items() if k.endswith("_column")]
            cols += [a["column"] for a in m.get("resource_attribute", []) + m.get("attribute", [])]
            self.assertTrue(cols)
            self.assertTrue(set(cols) <= fields, set(cols) - fields)
            self.assertEqual([a["key"] for a in m["resource_attribute"]], ["service.name"])
        with open(os.path.join(self._tmp.name, "s1", "spans.mapping.toml"), "rb") as f:
            self.assertEqual(tomllib.load(f)["spans"]["status_code_column"], "status_code")


# --- statements.toml and bands.toml ---------------------------------------------


class Statements(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(STATEMENTS, "rb") as f:
            cls.st = tomllib.load(f)["statement"]
        with open(BANDS, "rb") as f:
            cls.bands = tomllib.load(f)
        cls._tmp = tempfile.TemporaryDirectory()
        cls.truth, _ = gen(cls._tmp.name)
        spec = importlib.util.spec_from_file_location("inv", os.path.join(BOX, "lib", "inv.py"))
        cls.inv = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.inv)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_fields(self):
        ids = [s["id"] for s in self.st]
        self.assertEqual(len(ids), len(set(ids)))
        for s in self.st:
            self.assertIn(s["tenant"], ("inv", "hits"), s["id"])
            self.assertIn(s["kind"], ("trace_get", "sql"), s["id"])
            self.assertTrue(set(s["states"]) <= set(STATES), s["id"])
            self.assertIn(s.get("window", "none"), ("none", "1h", "narrow", "wide"), s["id"])
            if s["kind"] == "sql" and not s.get("corpus_id") and not s.get("control"):
                self.assertTrue(s.get("sql"), s["id"])

    def test_every_truth_key_exists(self):
        with open(HITS_TRUTH) as f:
            hits = json.load(f)
        for s in self.st:
            if s["tenant"] == "hits":
                self.assertIn(s["truth_key"], hits, s["id"])
                continue
            runs = range(len(self.truth["saved_traces"])) if s.get("per_run") == "saved_trace" else [0]
            for run in runs:
                self.assertIn(s["truth_key"].replace("{run}", str(run)), self.truth["answers"], s["id"])

    def test_every_placeholder_fills(self):
        p = self.inv.placeholders(self.truth, 0)
        for s in self.st:
            if s.get("sql"):
                self.inv.fill(s["sql"], p)

    def test_real_column_names(self):
        for s in self.st:
            sql = s.get("sql", "")
            self.assertIsNone(re.search(r"\bservice\s*=", sql), s["id"])
            self.assertIsNone(re.search(r"\bstatus\s*=", sql), s["id"])

    def test_required_statements(self):
        by = {s["id"]: s for s in self.st}
        for size in ("1", "10", "100", "1000"):
            self.assertIn("IN (", by[f"spans_in_{size}"]["sql"])
            self.assertIn("IN (", by[f"logs_request_in_{size}"]["sql"])
        for k in ("1", "102", "1024", "10240"):
            self.assertEqual(by[f"journey_c_{k}"]["truth_key"], f"journey_c.{k}")
        self.assertEqual(by["logs_trace_norange"]["window"], "none")
        self.assertEqual(by["q20_hits"]["tenant"], "hits")
        self.assertTrue(by["spans_in_1000"].get("unsupported_reason"))
        self.assertTrue(by["logs_request_in_1000"].get("unsupported_reason"))

    def test_bands_rows_1_to_17(self):
        rows = self.bands["row"]
        self.assertEqual([r["adr_row"] for r in rows], list(range(1, 18)))
        self.assertEqual([r["key"] for r in rows], [f"row_{n:02d}" for n in range(1, 18)])
        ids = {s["id"] for s in self.st} | {"fold", "all", "control"}
        for r in rows:
            for k in ("check", "target", "miss", "source"):
                self.assertTrue(r.get(k), f"{r['key']}.{k}")
            self.assertNotEqual(r["target"].strip().lower(), "n/a", r["key"])
            names = [f["name"] for f in r["figure"]]
            self.assertEqual(len(names), len(set(names)), r["key"])
            for f in r["figure"]:
                self.assertIn(f["statement"], ids, f["name"])
                self.assertIn(f["lane"], ("counter", "timing"), f["name"])
                for e in ("target_if", "miss_if"):
                    if e in f:
                        compile(f[e], f["name"], "eval")
        for key in ("row_05", "row_06"):
            r = next(r for r in rows if r["key"] == key)
            self.assertEqual(r["requires_tenant"], "hits")
            self.assertEqual({f["statement"] for f in r["figure"]}, {"q20_hits"})
        timed = sorted({r["key"] for r in rows for f in r["figure"]
                        if f["lane"] == "timing" and f["metric"] == "cold.wall_p50_s"
                        and f["statement"] != "q20_hits" and r["key"] not in ("row_15",)})
        self.assertEqual(timed, ["row_02"])


# --- evaluate.py ------------------------------------------------------------------


class LocalSmoke(Workdir):
    """The committed local-lane run, re-evaluated."""

    def test_byte_identical_reevaluation(self):
        code, _, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        self.assertEqual(code, 1)
        with gzip.open(os.path.join(SMOKE, "results.local.json.gz"), "rb") as f:
            want = f.read()
        with open(self.path("out", "results.local.json"), "rb") as f:
            self.assertEqual(f.read(), want)
        for name in ("summary.local.txt",):
            with open(os.path.join(SMOKE, name), "rb") as a, open(self.path("out", name), "rb") as b:
                self.assertEqual(a.read(), b.read(), name)

    def test_problems_are_the_two_counter_misses(self):
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        self.assertEqual(res["verdict"], "fail")
        self.assertEqual(res["problems"], ["miss row_07.spans_section_gets", "miss row_08.logs_leaf_pct"])

    def test_every_band_figure_present_exactly_once(self):
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        with open(BANDS, "rb") as f:
            bands = tomllib.load(f)
        want = [f"{r['key']}.{f['name']}" for r in bands["row"] for f in r["figure"]]
        self.assertEqual(sorted(f["id"] for f in res["figures"]), sorted(want))
        covered = {"unfolded", "folded_no_leaves", "folded_leaves", "mixed"}
        for f in res["figures"]:
            self.assertNotIn(f["status"], ("missing", "duplicate"), f["id"])
            if f["status"] == "not-run":
                self.assertTrue(f["reason"], f["id"])
                continue
            if f["state"] in covered:
                self.assertIn(f["status"], ("value", UNSUPPORTED), f["id"])
            if f["status"] == UNSUPPORTED:
                self.assertTrue(f["reason"], f["id"])

    def test_many_parts_and_in_1000_are_unsupported_on_this_build(self):
        run = smoke_run(self.path("run"))
        _, res, _ = evaluate(run, self.path("out"))
        with open(os.path.join(run, "run.toml"), "rb") as f:
            reason = tomllib.load(f)["unsupported_states"]["many_parts"]
        many = [c for c in read_jsonl(os.path.join(run, "calls.jsonl")) if c["state"] == "many_parts"]
        self.assertTrue(many)
        self.assertTrue(all(c["outcome"] == UNSUPPORTED and c["reason"] == reason for c in many))
        rows = [s for s in res["statements"] if s["state"] == "many_parts"]
        self.assertEqual(len(rows), len(many))
        self.assertTrue(all(s["status"] == UNSUPPORTED and s["reason"] == reason for s in rows))
        for sid in ("spans_in_1000", "logs_request_in_1000"):
            for state in ("unfolded", "folded_leaves"):
                s = next(s for s in res["statements"] if s["statement"] == sid and s["state"] == state)
                self.assertEqual(s["status"], UNSUPPORTED)
                self.assertIn("complexity", s["reason"])
        self.assertEqual(figure(res, "row_11.spans_1000_rows_exact")["status"], UNSUPPORTED)

    def test_flow_stamps(self):
        run = smoke_run(self.path("run"))
        folds = read_jsonl(os.path.join(run, "stamps", "folds.jsonl"))
        self.assertEqual([(f["transition"], f["signal"]) for f in folds], [
            ("folded_no_leaves", "spans"), ("folded_no_leaves", "logs"),
            ("folded_leaves", "spans"), ("folded_leaves", "logs"),
            ("leaf_rebuild", "spans"), ("leaf_rebuild", "logs"),
            ("fold_advance", "spans"), ("fold_advance", "logs")])
        self.assertTrue(all(f["exit"] == 0 for f in folds))
        rebuilt = [f for f in folds if f["transition"] == "leaf_rebuild"]
        self.assertTrue(all(f["report"]["leaves_written"] >= 1 for f in rebuilt))
        trans = read_jsonl(os.path.join(run, "stamps", "transitions.jsonl"))
        self.assertEqual([t["state"] for t in trans], [
            "unfolded", "folded_no_leaves", "folded_leaves", "mixed", "fold_advance", "many_parts"])
        mixed = next(t for t in trans if t["state"] == "mixed")
        self.assertGreater(mixed["ingest_hour"], trans[0]["ingest_hour"])
        calls = read_jsonl(os.path.join(run, "calls.jsonl"))
        restarts = read_jsonl(os.path.join(run, "stamps", "restarts.jsonl"))
        cold = {(c["statement"], c["state"], c["try"]) for c in calls
                if c.get("temp") == "cold" and c["outcome"] == "ok"}
        self.assertEqual(cold, {(r["statement"], r["state"], r["try"]) for r in restarts})
        self.assertEqual(len({r["pid"] for r in restarts}), len(restarts))
        self.assertTrue(all(isinstance(c.get("stats"), dict) for c in calls if c["outcome"] == "ok"))
        with open(os.path.join(run, "stamps", "host.json")) as f:
            self.assertEqual(json.load(f)["rq_stats"], "1")

    def test_truth_regenerates_from_the_stamped_corpus(self):
        with open(os.path.join(SMOKE, "stamps", "corpus.json")) as f:
            c = json.load(f)
        with open(os.path.join(SMOKE, "run.toml"), "rb") as f:
            corpus = tomllib.load(f)["corpus"]
        self.assertEqual((c["traces"], c["hours"], c["seed"]), (corpus["traces"], corpus["hours"], corpus["seed"]))
        truth, _ = gen(self.path("g"), c["traces"], c["hours"], c["seed"], "--start-hour", str(c["start_hour"]))
        with gzip.open(os.path.join(SMOKE, "truth.json.gz")) as f:
            committed = json.load(f)
        self.assertEqual(committed["generator"].pop("format"), "parquet")
        truth["generator"].pop("format")
        self.assertEqual(truth, committed)


class Evaluate(Workdir):
    """evaluate.py cases on mutations of the local smoke run."""

    def setUp(self):
        super().setUp()
        self.run_dir = smoke_run(self.path("run"))
        self.calls_path = os.path.join(self.run_dir, "calls.jsonl")
        self.calls = read_jsonl(self.calls_path)

    def save_calls(self):
        write_jsonl(self.calls_path, self.calls)

    def go(self, bands=None, evaluate_py=EVALUATE):
        out = self.path("out")
        shutil.rmtree(out, ignore_errors=True)
        return evaluate(self.run_dir, out, bands or self.pass_bands(), evaluate_py)

    def select(self, statement, state="folded_leaves", temp="cold"):
        return [c for c in self.calls if c["statement"] == statement and c["state"] == state
                and c.get("temp") == temp]

    def test_pass(self):
        code, res, _ = self.go()
        self.assertEqual((code, res["verdict"], res["problems"]), (0, "pass", []))
        self.assertEqual(figure(res, "row_t1.spans_1_rows_exact")["verdict"], "pass")
        self.assertGreaterEqual(figure(res, "row_t1.spans_leaves")["value"], 1)

    def test_timing_is_judged_on_the_s3_lane_only(self):
        code, res, _ = self.go()
        self.assertEqual(code, 0)
        self.assertEqual(figure(res, "row_t1.spans_1_wall")["verdict"], "reported (timing is judged on the s3 lane only)")
        toml = os.path.join(self.run_dir, "run.toml")
        with open(toml) as f:
            text = f.read()
        with open(toml, "w") as f:
            f.write(text.replace('lane = "local"', 'lane = "s3"'))
        code, res, _ = self.go()
        self.assertEqual((code, res["lane"]), (1, "s3"))
        self.assertEqual(figure(res, "row_t1.spans_1_wall")["verdict"], "miss")

    def check_missing(self, evaluate_py=EVALUATE):
        """Failures of the missing-call case; empty when evaluate is right."""
        self.calls = [c for c in self.calls if not (c["statement"] == "spans_in_1" and c["state"] == "folded_leaves")]
        self.save_calls()
        code, res, _ = self.go(evaluate_py=evaluate_py)
        out = []
        f = figure(res, "row_t1.spans_1_rows_exact")
        if code != 1:
            out.append(f"exit {code}")
        if f["verdict"] != "missing":
            out.append(f"verdict {f['verdict']}")
        if not any(p.startswith("missing row_t1.spans_1_rows_exact") for p in res["problems"]):
            out.append(f"problems {res['problems']}")
        return out

    def test_missing_call(self):
        self.assertEqual(self.check_missing(), [])

    def test_missing_try(self):
        self.calls = [c for c in self.calls if not (c["statement"] == "spans_in_1" and c["state"] == "folded_leaves"
                                                    and c.get("temp") == "cold" and c["try"] == 1)]
        self.save_calls()
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        f = figure(res, "row_t1.spans_1_rows_exact")
        self.assertEqual((f["status"], f["reason"]), ("missing", "cold tries [1] of spans_in_1 in folded_leaves"))

    def test_duplicate_call(self):
        self.calls.append(copy.deepcopy(self.select("spans_in_1")[0]))
        self.save_calls()
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        self.assertEqual(figure(res, "row_t1.spans_1_rows_exact")["verdict"], "duplicate")
        self.assertIn("duplicate call spans_in_1/folded_leaves/cold try 0", res["problems"])

    def check_duplicate_stamp(self, evaluate_py=EVALUATE):
        """Failures of the duplicated-fold-stamp case; empty when evaluate is right."""
        path = os.path.join(self.run_dir, "stamps", "folds.jsonl")
        folds = read_jsonl(path)
        extra = copy.deepcopy(next(f for f in folds if f["transition"] == "leaf_rebuild" and f["signal"] == "spans"))
        extra["report"]["leaves_written"] += 5
        write_jsonl(path, folds + [extra])
        code, res, _ = self.go(evaluate_py=evaluate_py)
        out = []
        f = figure(res, "row_t1.spans_leaves")
        if code != 1:
            out.append(f"exit {code}")
        if f["verdict"] != "duplicate":
            out.append(f"verdict {f['verdict']} value {f['value']}")
        if "duplicate stamps/folds.jsonl leaf_rebuild/spans" not in res["problems"]:
            out.append(f"problems {res['problems']}")
        return out

    def test_duplicate_fold_stamp(self):
        self.assertEqual(self.check_duplicate_stamp(), [])

    def test_band_miss(self):
        code, res, _ = self.go(self.pass_bands(PASS_BANDS.replace('target_if = "v >= 1"', 'target_if = "v >= 10000"')))
        self.assertEqual(code, 1)
        self.assertEqual(figure(res, "row_t1.spans_leaves")["verdict"], "miss")
        self.assertEqual(res["problems"], ["miss row_t1.spans_leaves"])

    def test_wrong_answer_zero_rows_against_one(self):
        for c in self.select("logs_request_id_present"):
            if c["try"] == 0:
                c["rows"], c["values"] = 0, []
        self.save_calls()
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        self.assertIn("wrong_answer logs_request_id_present/folded_leaves/cold try 0: rows 0 expected 1", res["problems"])
        f = figure(res, "row_t1.present_rows_exact")
        self.assertEqual((f["value"], f["verdict"]), (0, "miss"))

    def test_wrong_answer_row_set(self):
        c = self.select("logs_request_id_present")[0]
        c["values"] = ["0" * 32]
        self.save_calls()
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        self.assertIn("wrong_answer logs_request_id_present/folded_leaves/cold try 0: row set differs from the oracle",
                      res["problems"])

    def test_wrong_answer_truth_key_from_statements(self):
        c = self.select("logs_request_id_present")[0]
        c["truth_key"] = "request_id.absent"
        c["rows"], c["values"] = 0, []
        self.save_calls()
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        self.assertTrue(any(p.startswith("wrong_answer logs_request_id_present/folded_leaves/cold try 0: call names truth key")
                            for p in res["problems"]), res["problems"])

    def test_empty_stamps_directory(self):
        stamps = os.path.join(self.run_dir, "stamps")
        shutil.rmtree(stamps)
        os.makedirs(stamps)
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        for name in ("build.json", "host.json", "declare.json", "loads.jsonl", "folds.jsonl", "census.jsonl",
                     "restarts.jsonl", "transitions.jsonl"):
            self.assertIn(f"missing stamps/{name}", res["problems"])
        self.assertEqual(figure(res, "row_t1.spans_leaves")["verdict"], "missing")

    def test_no_calls(self):
        os.remove(self.calls_path)
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        self.assertIn("missing calls.jsonl", res["problems"])
        self.assertEqual(figure(res, "row_t1.spans_1_rows_exact")["verdict"], "missing")

    def test_unsupported_statement_is_carried_with_its_reason(self):
        self.calls = [c for c in self.calls if not (c["statement"] == "spans_in_1" and c["state"] == "folded_leaves")]
        self.calls.append({"statement": "spans_in_1", "state": "folded_leaves", "temp": "cold", "try": 0,
                           "tenant": "inv", "outcome": UNSUPPORTED, "reason": "case reason"})
        self.save_calls()
        code, res, _ = self.go()
        f = figure(res, "row_t1.spans_1_rows_exact")
        self.assertEqual((f["status"], f["reason"]), (UNSUPPORTED, "case reason"))
        s = next(s for s in res["statements"] if s["statement"] == "spans_in_1" and s["state"] == "folded_leaves")
        self.assertEqual((s["status"], s["reason"]), (UNSUPPORTED, "case reason"))
        self.assertEqual(code, 0)

    def test_cost_exact_with_nonzero_cpu(self):
        for c, cpu in zip(self.select("spans_in_1"), (1.8, 3.6)):
            c["server_cpu_s"] = cpu
            c["stats"]["phases"] = [
                {"phase": "resolve", "s3GetRequests": 0, "s3ListRequests": 200, "s3GetWireBytes": 0},
                {"phase": "probe", "s3GetRequests": 1000, "s3ListRequests": 0, "s3GetWireBytes": 0},
            ]
        self.save_calls()
        _, res, _ = self.go()
        s = next(s for s in res["cost"]["statements"] if (s["statement"], s["state"]) == ("spans_in_1", "folded_leaves"))
        # 1000 GET x $0.0004/1000 + 200 LIST x $0.005/1000 + 2.7 cpu_s x $0.612/3600
        self.assertEqual(s["dollars_per_call"], 0.001859)
        self.assertEqual((s["mean_gets"], s["mean_list_requests"], s["mean_server_cpu_s"]), (1000, 200, 2.7))
        m = next(m for m in res["cost"]["maintenance"] if (m["transition"], m["signal"]) == ("folded_no_leaves", "spans"))
        r = next(f for f in read_jsonl(os.path.join(self.run_dir, "stamps", "folds.jsonl"))
                 if (f["transition"], f["signal"]) == ("folded_no_leaves", "spans"))
        want = (r["report"]["get_requests"] * 0.0004 / 1000
                + (r["report"]["put_requests"] + r["report"]["list_requests"]) * 0.005 / 1000
                + r["measure"]["cpu_s"] * 0.612 / 3600)
        self.assertGreater(r["measure"]["cpu_s"], 0)
        self.assertEqual(m["dollars"], round(want, 9))
        for k, b in res["cost"]["prices"].items():
            self.assertTrue(b["source"], k)

    def test_cost_figures_come_from_data(self):
        toml = os.path.join(self.run_dir, "run.toml")
        with open(toml) as f:
            text = f.read()
        with open(toml, "w") as f:
            f.write(re.sub(r"(?m)^get_per_1000 = .*\n", "", text))
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        self.assertIn("missing run.toml prices.get_per_1000", res["problems"])


# --- wrong evaluators the cases rule out --------------------------------------------

FLIPS = {
    "missing treated as pass": (
        '        if entry["status"] != "value":\n            entry["verdict"] = entry["status"]\n',
        '        if entry["status"] != "value":\n'
        '            entry["verdict"] = "pass" if entry["status"] == "missing" else entry["status"]\n',
        "check_missing",
    ),
    "last of the duplicates taken": (
        "            problems.add(\"duplicate\", f\"{name} {'/'.join(str(x) for x in k)}\")\n            out[k] = None\n",
        "            out[k] = line\n",
        "check_duplicate_stamp",
    ),
}


class Flips(Workdir):
    def test_each_wrong_evaluator_fails_its_case(self):
        with open(EVALUATE) as f:
            src = f.read()
        for name, (old, new, case) in FLIPS.items():
            with self.subTest(flip=name):
                self.assertEqual(src.count(old), 1, f"{name}: anchor gone from evaluate.py; update the flip")
                line = src[: src.index(old)].count("\n") + 2
                flipped = self.path(f"evaluate_{case}.py")
                with open(flipped, "w") as f:
                    f.write(src.replace(old, new))
                t = Evaluate(case)
                t.setUp()
                try:
                    self.assertEqual(getattr(t, case)(), [], f"{name}: the real evaluator fails its own case")
                    t.tearDown()
                    t.setUp()
                    failures = getattr(t, case)(flipped)
                finally:
                    t.tearDown()
                self.assertTrue(failures, f"{name}: the flipped evaluator passed {case}")
                print(f"flip '{name}' at evaluate.py:{line} fails {case}: {failures}", file=sys.stderr)


# --- box/ ---------------------------------------------------------------------------


def run_sh(lane, toml, out, *extra):
    p = subprocess.run(["bash", os.path.join(BOX, "run.sh"), "--dry-run", "--lane", lane, "--run-toml", toml,
                        "--out", out, *extra], capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr


class Box(Workdir):
    def steps(self, text):
        return [line.split(" ", 1)[1] for line in text.splitlines() if line.startswith("STEP ")]

    def blocks(self, text):
        """{step: [lines]} of a dry-run's output."""
        out, cur = {}, None
        for line in text.splitlines():
            if line.startswith("STEP "):
                cur = line.split(" ", 1)[1]
                out[cur] = []
            elif cur:
                out[cur].append(line)
        return out

    def assert_flow(self, text):
        self.assertEqual(self.steps(text), [
            "bootstrap", "store", "host", "corpus", "declare", "load-base", "state:unfolded",
            "state:folded_no_leaves", "state:folded_leaves", "state:mixed", "state:many_parts", "evaluate", "finish"])
        b = self.blocks(text)
        for state in STATES:
            self.assertIn("STAMP stamps/transitions.jsonl", b[f"state:{state}"], state)
        for state in STATES[:4]:
            lines = b[f"state:{state}"]
            self.assertIn("STAMP stamps/census.jsonl", lines, state)
            self.assertIn("STAMP stamps/restarts.jsonl", lines, state)
            j = "\n".join(lines)
            self.assertIn("stop server, drop page cache, start server", j)
            self.assertIn("RQ_STATS=1", j)
            self.assertIn("plan --statements " + STATEMENTS, j)
        fl = b["state:folded_no_leaves"]
        self.assertEqual(sum("catalog fold" in x and "--writers-stopped" in x and "--key-index-leaves" not in x
                             for x in fl), 2)
        lv = "\n".join(b["state:folded_leaves"])
        self.assertEqual(lv.count("--writers-stopped --json --key-index-leaves"), 2)
        self.assertEqual(lv.count("--rebuild-key-index --json"), 2)
        self.assertLess(lv.index("--key-index-leaves"), lv.index("--rebuild-key-index"))
        self.assertLess(lv.rindex("--rebuild-key-index"), lv.index("STAMP stamps/transitions.jsonl"))
        self.assertEqual(b["state:folded_leaves"].count("STAMP stamps/folds.jsonl"), 4)
        mx = b["state:mixed"]
        mj = "\n".join(mx)
        self.assertLess(mj.index("wait until the ingest hour"), mj.index("load-extra-spans"))
        self.assertEqual(mx.count("STAMP stamps/transitions.jsonl"), 2)
        self.assertLess(mj.index("RQ_STATS=1"), mj.index("fold-fold_advance"))
        self.assertIn("unsupported-on-this-build", "\n".join(b["state:many_parts"]))
        self.assertIn("evaluate.py --run", "\n".join(b["evaluate"]))
        return b

    def test_s3_dry_run(self):
        code, text = run_sh("s3", os.path.join(BOX, "run.example.toml"), self.path("out"))
        self.assertEqual(code, 0, text)
        b = self.assert_flow(text)
        boot = "\n".join(b["bootstrap"])
        self.assertIn("checkout -q --detach", boot)
        self.assertIn("force-frame-pointers=yes", boot)
        self.assertIn("-p ravel-server --features sql", boot)
        self.assertIn("pyarrow", boot)
        self.assertIn("store qualify", "\n".join(b["store"]))
        self.assertIn("--index-trace-id true", "\n".join(b["declare"]))
        self.assertIn("upload out/ to s3://", "\n".join(b["finish"]))
        self.assertIn("then terminate the instance", "\n".join(b["finish"]))

    def test_local_dry_run(self):
        code, text = run_sh("local", os.path.join(BOX, "run.local-smoke.toml"), self.path("out"))
        self.assertEqual(code, 0, text)
        b = self.assert_flow(text)
        self.assertIn("--address 127.0.0.1:", "\n".join(b["store"]))
        self.assertNotIn("aws ", text)
        self.assertNotIn("terminate", text)
        self.assertNotIn("shutdown", text)

    def test_s3_lane_refuses_off_the_box(self):
        p = subprocess.run(["bash", os.path.join(BOX, "run.sh"), "--lane", "s3", "--run-toml",
                            os.path.join(BOX, "run.example.toml"), "--out", self.path("out")],
                           capture_output=True, text=True)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("runs only on a box launch.sh started", p.stderr)

    def test_run_sh_failure_and_terminate_guards(self):
        with open(os.path.join(BOX, "run.sh")) as f:
            src = f.read()
        self.assertIn("trap on_exit EXIT", src)
        self.assertIn(">\"$OUT/FAILED\"", src)
        self.assertIn('[ "$LANE" = s3 ] && [ "$DRY_RUN" = 0 ] && [ -f "$BOX_MARKER" ] || return 0', src)
        self.assertEqual(src.count("shutdown -h now"), 1)
        self.assertIn("--mode query", src)

    def filled_toml(self):
        with open(os.path.join(BOX, "run.example.toml")) as f:
            text = f.read()
        text = (text.replace("FILL-IN-COMMIT", "0123456789abcdef0123456789abcdef01234567")
                .replace("FILL-IN-BUCKET-NAME", "case-bucket").replace("FILL-IN-EMPTY-STORE-BUCKET", "case-store"))
        p = self.path("run.toml")
        with open(p, "w") as f:
            f.write(text)
        return p

    def stub_path(self):
        """A PATH whose aws records any call and fails, so a dry run that runs one shows."""
        d = self.path("stub")
        os.makedirs(d)
        with open(os.path.join(d, "aws"), "w") as f:
            f.write(f"#!/bin/sh\necho \"$*\" >> {self.path('aws-called')}\nexit 1\n")
        os.chmod(os.path.join(d, "aws"), 0o755)
        return d + os.pathsep + os.environ["PATH"]

    def test_launch_dry_run(self):
        env = dict(os.environ, PATH=self.stub_path())
        p = subprocess.run(["bash", os.path.join(BOX, "launch.sh"), "--dry-run", self.filled_toml()],
                           capture_output=True, text=True, env=env, cwd=self.tmp)
        text = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, text)
        self.assertFalse(os.path.exists(self.path("aws-called")))
        self.assertIn("--instance-initiated-shutdown-behavior terminate", text)
        self.assertIn("shutdown -h +600", text)
        self.assertIn("c6a.4xlarge", text)
        self.assertNotIn("spot", text.lower().replace("no spot", ""))
        user = text[text.index("DRY-RUN user-data:"): text.index("DRY-RUN end user-data")]
        self.assertLess(user.index("shutdown -h +600"), user.index("run.sh"))
        self.assertIn("RUN_ID", user)

    def test_launch_refuses_an_unfilled_toml(self):
        p = subprocess.run(["bash", os.path.join(BOX, "launch.sh"), "--dry-run", os.path.join(BOX, "run.example.toml")],
                           capture_output=True, text=True, env=dict(os.environ, PATH=self.stub_path()))
        self.assertEqual(p.returncode, 64)

    def test_fetch_dry_run(self):
        env = dict(os.environ, PATH=self.stub_path())
        run_id = "investigations-20261103T000000Z-1-1"
        p = subprocess.run(["bash", os.path.join(BOX, "fetch.sh"), "--dry-run", self.filled_toml(), run_id],
                           capture_output=True, text=True, env=env, cwd=self.tmp)
        text = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, text)
        self.assertFalse(os.path.exists(self.path("aws-called")))
        self.assertIn(f"s3://case-bucket/investigations/runs/{run_id}/out", text)
        p = subprocess.run(["bash", os.path.join(BOX, "fetch.sh"), "--dry-run", self.filled_toml(), "case-run"],
                           capture_output=True, text=True, env=env, cwd=self.tmp)
        self.assertEqual(p.returncode, 64)


def refresh_local_smoke():
    with tempfile.TemporaryDirectory() as d:
        code, _, text = evaluate(smoke_run(os.path.join(d, "run")), os.path.join(d, "out"))
        with open(os.path.join(d, "out", "results.local.json"), "rb") as f:
            data = f.read()
        with open(os.path.join(SMOKE, "results.local.json.gz"), "wb") as f:
            f.write(gzip.compress(data, compresslevel=9, mtime=0))
        shutil.copy(os.path.join(d, "out", "summary.local.txt"), SMOKE)
    print(f"refreshed {SMOKE} (evaluate.py exit {code})")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--refresh-local-smoke"]:
        sys.exit(refresh_local_smoke())
    unittest.main()
