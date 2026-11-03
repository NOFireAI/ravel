#!/usr/bin/env python3
"""Cases of the selective-investigations harness (issue #3699), run by
investigations.test.sh. No AWS, no server, no network.

    investigations_cases.py [unittest args]
    investigations_cases.py --refresh-local-smoke
    investigations_cases.py --import-local-smoke RUN_DIR

--refresh-local-smoke re-evaluates fixtures/local-smoke/ and rewrites its
results.local.json.gz and summary.local.txt: run it after a deliberate change
to evaluate.py, bands.toml or statements.toml, and review the diff of
summary.local.txt before committing it.

--import-local-smoke replaces fixtures/local-smoke/ with the inputs of the
local run box/run.sh wrote to RUN_DIR (run.toml, and calls.jsonl, truth.json
and every stamp gzipped with mtime 0), then refreshes it. Nothing in the
fixture is written by hand.
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
import time
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
# investigations.test.sh passes the bash running it, so a run under bash 3.2
# runs every box/ script under 3.2 too.
BASH = os.environ.get("INV_BASH", "bash")
HOUR_NS = 3600 * 1_000_000_000
STATES = ("unfolded", "folded_no_leaves", "folded_leaves", "mixed", "compaction_in_flight", "compaction_done",
          "erasure_in_flight", "many_parts_24", "many_parts_744", "many_parts_2160")
SMOKE_STATES = ("unfolded", "folded_no_leaves", "folded_leaves", "mixed", "compaction_in_flight", "many_parts_24")
UNSUPPORTED = "unsupported-on-this-build"
REFUSAL_1000 = ("server refused the statement (HTTP 400): statement complexity 1001 exceeds the maximum of "
                "1000 tokens; simplify the statement")


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


SMALL_CORPUS = {"traces": 1000, "hours": 2, "seed": 7, "start_hour": 498248, "tail_traces": 500, "tail_seed": 8,
                "tail_start_hour": 498250,
                "many_parts": {"24": {"traces": 240, "hours": 24, "seed": 31, "start_hour": 498222}}}


def merged_truth(out, c=SMALL_CORPUS):
    """truth.json as run.sh builds it from a corpus.json stamp: the base corpus,
    the tail hour and each many_parts_N corpus, folded by inv.py merge-truth."""
    gen(os.path.join(out, "base"), c["traces"], c["hours"], c["seed"], "--start-hour", str(c["start_hour"]))
    gen(os.path.join(out, "tail"), c["tail_traces"], 1, c["tail_seed"], "--start-hour", str(c["tail_start_hour"]),
        "--tail")
    subs = ["--sub", f"tail={os.path.join(out, 'tail', 'truth.json')}"]
    for n, mp in sorted(c["many_parts"].items()):
        d = os.path.join(out, f"mp{n}")
        gen(d, mp["traces"], mp["hours"], mp["seed"], "--start-hour", str(mp["start_hour"]))
        subs += ["--sub", f"many_parts_{n}={os.path.join(d, 'truth.json')}"]
    path = os.path.join(out, "truth.json")
    subprocess.run([PY, os.path.join(BOX, "lib", "inv.py"), "merge-truth", "--base",
                    os.path.join(out, "base", "truth.json"), *subs, "--out", path], check=True, capture_output=True)
    with open(path) as f:
        return json.load(f)


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
    os.makedirs(os.path.join(dst, "stamps"))
    shutil.copy(os.path.join(SMOKE, "run.toml"), dst)
    for name in sorted(os.listdir(os.path.join(SMOKE, "stamps"))):
        if not name.endswith(".gz"):
            raise AssertionError(f"fixture stamp {name} is not gzipped; re-import the run")
        gunzip_to(os.path.join(SMOKE, "stamps", name), os.path.join(dst, "stamps", name[:-3]))
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


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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
metric = "fold.inv.spans.leaves_written"
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
        # 25,000 log rows span four BLOCK_ROWS blocks, the fewest that give
        # every needle tier its own block count.
        cls.truthb, cls.rowsb = gen(os.path.join(cls._tmp.name, "b1"), 5000, 2, 7)
        cls.truthb10, cls.rowsb10 = gen(os.path.join(cls._tmp.name, "b10"), 5000, 2, 7, "--scale", "10")

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
        t, rows = self.truthb, self.rowsb
        counts = {v: sum(1 for r in rows["logs"] if r["tag"] == v) for v in t["needles"]}
        self.assertEqual(counts, t["needles"])
        for v, n in counts.items():
            self.assertEqual(n, sum(1 for r in rows["logs"] if r["body"].endswith(" " + v)))
            self.assertEqual(t["answers"][f"needle.{v}"]["value"], n)
        self.assertEqual(counts["needlerare"], 1)
        self.assertLess(counts["needlerare"], counts["needlesparse"])
        self.assertLess(counts["needlesparse"], counts["needlecommon"])

    def test_needle_blocks_recount_within_20_percent(self):
        # Item 7: tiers are ordered by blocks touched, and a recount over the
        # written file in BLOCK_ROWS-row blocks agrees with truth within 20 %.
        for truth, rows in ((self.truthb, self.rowsb), (self.truthb10, self.rowsb10)):
            br = truth["generator"]["block_rows"]
            self.assertEqual(br, 8192)
            nb = dict(truth["needle_blocks"])
            want = nb.pop("any")
            got = len({i // br for i, r in enumerate(rows["logs"]) if r["tag"] in nb})
            self.assertLessEqual(abs(got - want), 0.2 * want, ("any", got, want))
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
        self.assertEqual(g.BLOCK_ROWS, 8192)
        n_blocks = -(-100_000_000 // g.BLOCK_ROWS)
        got = g.needle_block_counts(100_000_000)
        self.assertEqual(got["needlerare"], 1)
        self.assertEqual(got["needlesparse"], round(0.001 * n_blocks))
        self.assertEqual(got["needlecommon"], round(0.10 * n_blocks))

    def test_under_one_block_plants_no_needle(self):
        # 5,000 log rows fill less than one block, so no needle row exists
        # and every needle answer is 0.
        for truth in (self.truth, self.truth10):
            self.assertEqual(truth["needles"], {"needlecommon": 0, "needlerare": 0, "needlesparse": 0})
            for v in truth["needles"]:
                self.assertEqual(truth["answers"][f"needle.{v}"]["value"], 0)
        self.assertFalse([r for r in self.rows["logs"] if r["tag"] is not None])

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
        cls.truth = merged_truth(cls._tmp.name)
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
            t, prefix = self.inv.sub_truth(self.truth, s.get("truth_sub"))
            if s.get("requires") and not t.get(s["requires"]):
                continue
            runs = range(len(t["saved_traces"])) if s.get("per_run") == "saved_trace" else [0]
            for run in runs:
                self.assertIn(prefix + s["truth_key"].replace("{run}", str(run)), self.truth["answers"], s["id"])

    def test_every_placeholder_fills(self):
        for s in self.st:
            t, _ = self.inv.sub_truth(self.truth, s.get("truth_sub"))
            if s.get("sql") and not (s.get("requires") and not t.get(s["requires"])):
                self.inv.fill(s["sql"], self.inv.placeholders(t, 0))

    def test_erasure_victim_needs_a_corpus_with_an_unread_row(self):
        # The small corpus's 1000-key lists name every trace, so it has no
        # victim and logs_erasure_victim plans as not-run; the box's corpus has one.
        self.assertIsNone(self.truth["erasure_victim"])
        truth, _ = gen(os.path.join(self._tmp.name, "victim"), 3000, 2, 7)
        self.assertEqual(truth["answers"]["erasure_victim.present"]["rows"], 1)

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
        # Item 11: the 1000-key lists are sent; the server's refusal is the
        # measured outcome, not a static skip.
        for sid in ("spans_in_1000", "logs_request_in_1000", "logs_trace_in_1000"):
            self.assertNotIn("unsupported_reason", by[sid], sid)

    def test_q20_oracle_is_the_unfolded_answer(self):
        # Item 19: q20's rows are judged against its own unfolded call; the
        # pre-registered row count is a separate, reported figure.
        by = {s["id"]: s for s in self.st}
        self.assertEqual(by["q20_hits"]["oracle"], "unfolded")
        row5 = {f["name"]: f for f in next(r for r in self.bands["row"] if r["key"] == "row_05")["figure"]}
        self.assertEqual(row5["rows_exact"]["metric"], "cold.rows_exact")
        self.assertEqual(row5["rows_match_unfolded"]["metric"], "cold.rows_match_state.unfolded")
        self.assertEqual((row5["rows_predicted"]["metric"], row5["rows_predicted"]["target_if"]),
                         ("cold.max.rows", "v == 4"))

    def test_bands_rows_1_to_17(self):
        rows = self.bands["row"]
        self.assertEqual([r["adr_row"] for r in rows], list(range(1, 18)))
        self.assertEqual([r["key"] for r in rows], [f"row_{n:02d}" for n in range(1, 18)])
        ids = {s["id"] for s in self.st} | {"fold", "all", "control", "stamp", "derived", "truth"}
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
            self.assertEqual({f["statement"] for f in r["figure"]}, {"q20_hits", "stamp"})
        timed = sorted({r["key"] for r in rows for f in r["figure"] if f["lane"] == "timing"})
        # Rows 2, 5 and 15 time statements; row 16's slowdown is a timing
        # figure of a control run this harness does not perform.
        self.assertEqual(timed, ["row_02", "row_05", "row_15", "row_16"])
        r16 = next(r for r in rows if r["key"] == "row_16")
        self.assertEqual({f["statement"] for f in r16["figure"] if f["lane"] == "timing"}, {"control"})
        for r in rows:
            for f in r["figure"]:
                if f["metric"] == "derived":
                    self.assertTrue(f.get("expr"), f["name"])


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

    def test_fixture_is_under_300_kb(self):
        total = sum(os.path.getsize(os.path.join(d, n)) for d, _, names in os.walk(SMOKE) for n in names)
        self.assertLess(total, 300_000)

    def test_problems_are_exactly_these(self):
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        self.assertEqual(res["verdict"], "fail")
        self.assertEqual(res["problems"], [
            "miss row_01.wire_bytes",
            "miss row_03.probe_wire_per_object",
            "miss row_07.spans_section_gets: from-scratch rebuild: section GETs = 2 x entry_count",
            "miss row_07.logs_section_gets: from-scratch rebuild: section GETs = 2 x entry_count",
            "miss row_08.logs_trace_id_leaf_pct",
            "miss row_08.logs_request_id_leaf_pct: key cardinality equals the row count",
            "dead_counter segmentsOpened journey_c_1 folded_leaves",
            "dead_counter segmentsOpened journey_c_10240 folded_leaves",
        ])

    def test_every_band_figure_present_exactly_once(self):
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        with open(BANDS, "rb") as f:
            bands = tomllib.load(f)
        want = [f"{r['key']}.{f['name']}" for r in bands["row"] for f in r["figure"]]
        self.assertEqual(sorted(f["id"] for f in res["figures"]), sorted(want))
        covered = set(SMOKE_STATES) | {"fold_advance", "compaction_done"}
        for f in res["figures"]:
            self.assertNotIn(f["status"], ("missing", "duplicate"), f["id"])
            if f["status"] == "not-run":
                self.assertTrue(f["reason"], f["id"])
                continue
            if f["state"] in covered:
                self.assertIn(f["status"], ("value", UNSUPPORTED, "dead_counter"), f["id"])
            if f["status"] == UNSUPPORTED:
                self.assertTrue(f["reason"], f["id"])

    def test_states_section(self):
        # Item 15: every state is listed, run or not, with the reason it was not.
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        got = {s["state"]: (s["status"], s.get("reason")) for s in res["states"]}
        want = {s: ("run", None) for s in SMOKE_STATES + ("fold_advance", "compaction_done")}
        want.update({s: ("not-run", "not in run.toml states")
                     for s in ("erasure_in_flight", "many_parts_744", "many_parts_2160")})
        self.assertEqual(got, want)

    def test_in_1000_is_sent_and_refused_by_the_complexity_guard(self):
        # Item 11: the refusal is the server's, observed live, with its text.
        run = smoke_run(self.path("run"))
        _, res, _ = evaluate(run, self.path("out"))
        calls = read_jsonl(os.path.join(run, "calls.jsonl"))
        for sid in ("spans_in_1000", "logs_trace_in_1000", "logs_request_in_1000"):
            for state in ("unfolded", "folded_leaves"):
                lines = [c for c in calls if (c["statement"], c["state"]) == (sid, state)]
                self.assertEqual(len(lines), 2, (sid, state))
                for c in lines:
                    self.assertEqual((c["outcome"], c["http_status"], c["reason"]), (UNSUPPORTED, 400, REFUSAL_1000))
                    self.assertIn("wall_s", c)
        f = figure(res, "row_11.control_1001_not_served")
        self.assertEqual((f["value"], f["verdict"]), (1, "pass"))
        self.assertEqual(figure(res, "row_11.spans_1000_rows_exact")["reason"], REFUSAL_1000)

    def test_many_parts_24(self):
        # Item 9: the smoke's many-parts state is a real 24-part catalog.
        run = smoke_run(self.path("run"))
        _, res, _ = evaluate(run, self.path("out"))
        with open(os.path.join(run, "stamps", "many_parts_24.spans.catalog.json")) as f:
            doc = json.load(f)
        self.assertEqual((doc["tenant"], doc["part_count"], len(doc["parts"]), doc["parts_with_leaves"]),
                         ("mp24", 24, 24, 24))
        for name, value in (("covering_parts_24", 24), ("rows_exact_24", 1)):
            f = figure(res, f"row_09.{name}")
            self.assertEqual((f["value"], f["verdict"]), (value, "pass"), name)
        self.assertLessEqual(figure(res, "row_09.resolve_gets_24")["value"], 24 + 3)
        for name in ("covering_parts", "rows_exact"):
            self.assertEqual(figure(res, f"row_09.{name}")["status"], "not-run")

    def test_section_gets_are_a_from_scratch_rebuild(self):
        # Item 3: the advancing fold re-reads every entry's sections, old and
        # new, not only the tail's.
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        for sig in ("spans", "logs"):
            f = figure(res, f"row_07.{sig}_section_gets")
            tail = figure(res, f"row_07.{sig}_tail_entries")["value"]
            self.assertEqual((f["value"], f["verdict"]), (2 * tail, "miss"), sig)
            self.assertEqual(f["reason"], "from-scratch rebuild: section GETs = 2 x entry_count")

    def test_dead_counter_on_row_14(self):
        # Item 5: segmentsOpened reads 0 on calls that issued scan GETs.
        run = smoke_run(self.path("run"))
        _, res, _ = evaluate(run, self.path("out"))
        for k in ("1", "10240"):
            f = figure(res, f"row_14.objects_opened_{k}")
            self.assertEqual((f["status"], f["value"], f["verdict"]), ("dead_counter", 0, "dead_counter"))
            self.assertIn(f"segmentsOpened 0 while journey_c_{k}/folded_leaves/cold try", f["reason"])
        calls = [c for c in read_jsonl(os.path.join(run, "calls.jsonl"))
                 if (c["statement"], c["state"], c["temp"]) == ("journey_c_1", "folded_leaves", "cold")]
        self.assertTrue(all(sum(p["segmentsOpened"] for p in c["stats"]["phases"]) == 0 for c in calls))
        self.assertTrue(any(p["phase"] == "scan" and p["s3GetRequests"] >= 1
                            for c in calls for p in c["stats"]["phases"]))

    def test_probe_wire_per_object_records_a_miss(self):
        # Item 6: one 64 KiB probe read per object, against <= 8192.
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        f = figure(res, "row_03.probe_wire_per_object")
        self.assertEqual((f["value"], f["verdict"]), (65536.0, "miss"))

    def test_leaf_share_is_per_field_over_the_census_bytes(self):
        # Item 14, recomputed by hand from the stamps.
        run = smoke_run(self.path("run"))
        _, res, _ = evaluate(run, self.path("out"))
        census = next(c for c in read_jsonl(os.path.join(run, "stamps", "census.jsonl"))
                      if c["state"] == "folded_leaves")
        for label, field in (("spans", "trace_id"), ("logs", "trace_id"), ("logs", "request_id")):
            with open(os.path.join(run, "stamps", f"folded_leaves.{label}.catalog.json")) as f:
                doc = json.load(f)
            leaf = sum(lf["bytes"] for p in doc["parts"] for lf in p["leaves"] if lf["field"] == field)
            data = census["census"]["tenants"]["inv"]["data"][label]["bytes"]
            got = figure(res, f"row_08.{label}_{field}_leaf_pct")["value"]
            self.assertEqual(got, round(100.0 * leaf / data, 9), (label, field))
        self.assertEqual(figure(res, "row_08.logs_request_id_leaf_pct")["value"], 25.174788981)

    def test_maintenance_per_part_month(self):
        # Item 13: spans leaf rebuild, 8 section GET x $0.0004/1000 + 1 leaf
        # PUT x $0.005/1000 = $8.2e-06 over 1 month, plus 12712 leaf bytes
        # x $0.023/GB-month = $2.92376e-07, over 1 part = $8.492376e-06,
        # $8.492e-06 at evaluate.py's nine decimal places.
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        m = next(m for m in res["cost"]["maintenance"] if m["signal"] == "spans")
        self.assertEqual((m["section_gets"], m["leaf_puts"], m["leaf_bytes"], m["parts"]), (8, 1, 12712, 1))
        self.assertEqual(m["maintenance_per_part_month"], round(8 * 0.0004 / 1000 + 1 * 0.005 / 1000
                                                                + 12712 / 1e9 * 0.023, 9))
        self.assertEqual(m["maintenance_per_part_month"], 8.492e-06)

    def test_warm_served_counts_cold_calls_the_cache_served(self):
        # Item 18: a cold call after a restart and page-cache drop can still
        # be served from the server's own warmed cache; each is listed.
        run = smoke_run(self.path("run"))
        _, res, text = evaluate(run, self.path("out"))
        want = sorted((c["statement"], c["state"], c["try"]) for c in read_jsonl(os.path.join(run, "calls.jsonl"))
                      if c.get("temp") == "cold" and c["outcome"] == "ok"
                      and sum(p.get("cacheServedBytes", 0) for p in c["stats"]["phases"]) > 0)
        self.assertEqual(sorted((w["statement"], w["state"], w["try"]) for w in res["warm_served"]), want)
        self.assertEqual(len(want), 118)
        self.assertIn("warm_served", text)

    def test_flow_stamps(self):
        run = smoke_run(self.path("run"))
        folds = read_jsonl(os.path.join(run, "stamps", "folds.jsonl"))
        self.assertEqual([(f["transition"], f["tenant"], f["signal"]) for f in folds], [
            ("folded_no_leaves", "inv", "spans"), ("folded_no_leaves", "inv", "logs"),
            ("folded_leaves", "inv", "spans"), ("folded_leaves", "inv", "logs"),
            ("leaf_rebuild", "inv", "spans"), ("leaf_rebuild", "inv", "logs"),
            ("fold_advance", "inv", "spans"), ("fold_advance", "inv", "logs"),
            ("many_parts_24", "mp24", "spans"), ("many_parts_24_leaf_rebuild", "mp24", "spans")])
        self.assertTrue(all(f["exit"] == 0 for f in folds))
        rebuilt = [f for f in folds if f["transition"] == "leaf_rebuild"]
        self.assertTrue(all(f["report"]["leaves_written"] >= 1 for f in rebuilt))
        trans = read_jsonl(os.path.join(run, "stamps", "transitions.jsonl"))
        self.assertEqual([t["state"] for t in trans], [
            "unfolded", "folded_no_leaves", "folded_leaves", "mixed", "fold_advance", "compaction_in_flight",
            "compaction_done", "many_parts_24"])
        mixed = next(t for t in trans if t["state"] == "mixed")
        self.assertGreater(mixed["ingest_hour"], trans[0]["ingest_hour"])
        compaction = read_jsonl(os.path.join(run, "stamps", "compaction.jsonl"))
        self.assertEqual([c["signal"] for c in compaction], ["spans", "logs"])
        self.assertTrue(all(c["exit"] == 0 and c["report"]["fields"]["compacted"] >= 1 for c in compaction))
        for name in ("store.json", "corpus.json", "tenants.json", "needle_blocks.json"):
            self.assertTrue(os.path.exists(os.path.join(run, "stamps", name)), name)
        calls = read_jsonl(os.path.join(run, "calls.jsonl"))
        restarts = read_jsonl(os.path.join(run, "stamps", "restarts.jsonl"))
        cold = {(c["statement"], c["state"], c["try"]) for c in calls if c.get("temp") == "cold"
                and (c["outcome"] == "ok" or (c["outcome"] == UNSUPPORTED and "http_status" in c))}
        # One more restart than cold calls: the server restarts once before
        # the compaction starts, stamped under the state's own name.
        self.assertEqual(cold | {("compaction_in_flight", "compaction_in_flight", 0)},
                         {(r["statement"], r["state"], r["try"]) for r in restarts})
        self.assertEqual(len({r["pid"] for r in restarts}), len(restarts))
        self.assertTrue(all(isinstance(c.get("stats"), dict) for c in calls if c["outcome"] == "ok"))
        with open(os.path.join(run, "stamps", "host.json")) as f:
            self.assertEqual(json.load(f)["rq_stats"], "1")

    def test_truth_regenerates_from_the_stamped_corpus(self):
        run = smoke_run(self.path("run"))
        with open(os.path.join(run, "stamps", "corpus.json")) as f:
            c = json.load(f)
        with open(os.path.join(SMOKE, "run.toml"), "rb") as f:
            corpus = tomllib.load(f)["corpus"]
        self.assertEqual((c["traces"], c["hours"], c["seed"]), (corpus["traces"], corpus["hours"], corpus["seed"]))
        self.assertEqual(c, {**SMALL_CORPUS, "many_parts": {"24": {**SMALL_CORPUS["many_parts"]["24"],
                                                                   "ingest_hour_start": 498223}},
                             "truth_sha256": c["truth_sha256"]})
        truth = merged_truth(self.path("g"), c)
        with gzip.open(os.path.join(SMOKE, "truth.json.gz")) as f:
            committed = json.load(f)
        for t in (committed, truth):
            for doc in (t, *t["sub"].values()):
                self.assertEqual(doc["generator"].pop("format"), "parquet" if t is committed else "jsonl")
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
        if "duplicate stamps/folds.jsonl leaf_rebuild/inv/spans" not in res["problems"]:
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
        for name in ("build.json", "host.json", "declare.json", "store.json", "corpus.json", "tenants.json",
                     "needle_blocks.json", "loads.jsonl", "folds.jsonl", "census.jsonl", "restarts.jsonl",
                     "transitions.jsonl", "compaction.jsonl"):
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
            for p in c["stats"]["phases"]:
                p["s3GetRequests"] = 1000 if p["phase"] == "probe" else 0
                p["s3ListRequests"] = 200 if p["phase"] == "resolve" else 0
        self.save_calls()
        _, res, _ = self.go()
        s = next(s for s in res["cost"]["statements"] if (s["statement"], s["state"]) == ("spans_in_1", "folded_leaves"))
        # 1000 GET x $0.0004/1000 + 200 LIST x $0.005/1000 + 2.7 cpu_s x $0.612/3600
        self.assertEqual(s["dollars_per_call"], 0.001859)
        self.assertEqual((s["mean_gets"], s["mean_list_requests"], s["mean_server_cpu_s"]), (1000, 200, 2.7))
        for sig in ("spans", "logs"):
            m = next(m for m in res["cost"]["maintenance"] if m["signal"] == sig)
            r = next(f for f in read_jsonl(os.path.join(self.run_dir, "stamps", "folds.jsonl"))
                     if (f["transition"], f["tenant"], f["signal"]) == ("leaf_rebuild", "inv", sig))
            with open(os.path.join(self.run_dir, "stamps", f"folded_leaves.{sig}.catalog.json")) as f:
                doc = json.load(f)
            leaf = sum(lf["bytes"] for p in doc["parts"] for lf in p["leaves"])
            puts = sum(a["leaf_puts"] for a in r["report"]["attempts"])
            want = (r["report"]["ranged_gets"] * 0.0004 / 1000 + puts * 0.005 / 1000 + leaf / 1e9 * 0.023)
            self.assertEqual(m["maintenance_per_part_month"], round(want / doc["part_count"], 9), sig)
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

    def stamp_path(self, name):
        return os.path.join(self.run_dir, "stamps", name)

    def check_missing_catalog_stamp(self, evaluate_py=EVALUATE):
        """Item 1: a state's catalog stamp absent is a problem, not a skip."""
        os.remove(self.stamp_path("folded_leaves.spans.catalog.json"))
        code, res, _ = self.go(evaluate_py=evaluate_py)
        out = []
        if code != 1:
            out.append(f"exit {code}")
        if "missing covering_parts.folded_leaves.spans" not in res["problems"]:
            out.append(f"problems {res['problems']}")
        return out

    def test_missing_catalog_stamp(self):
        self.assertEqual(self.check_missing_catalog_stamp(), [])

    def check_absent_phases(self, evaluate_py=EVALUATE):
        """Item 2: an ok call without stats.phases is named, and its figures are missing."""
        c = next(c for c in self.select("spans_lookup_norange") if c["try"] == 0)
        del c["stats"]["phases"]
        self.save_calls()
        code, res, _ = self.go(BANDS, evaluate_py)
        out = []
        if code != 1:
            out.append(f"exit {code}")
        if "missing stats.phases in spans_lookup_norange/folded_leaves/cold try 0" not in res["problems"]:
            out.append(f"problems {res['problems']}")
        return out

    def test_absent_phases(self):
        self.assertEqual(self.check_absent_phases(), [])

    def test_absent_cpu_on_every_call(self):
        n = 0
        for c in self.calls:
            if c["outcome"] == "ok":
                del c["server_cpu_s"]
                n += 1
        self.save_calls()
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        lacking = [p for p in res["problems"] if p.startswith("missing server_cpu_s in ")]
        self.assertEqual(len(lacking), n)
        self.assertIn("missing server_cpu_s in spans_in_1/folded_leaves/cold try 0", lacking)
        self.assertEqual(res["cost"]["statements"], [])
        self.assertEqual(figure(res, "row_t1.spans_1_rows_exact")["verdict"], "missing")

    def check_dead_counter(self, evaluate_py=EVALUATE):
        """Item 5: segmentsOpened 0 beside scan GETs is a dead counter, not a value."""
        code, res, _ = self.go(BANDS, evaluate_py)
        out = []
        f = figure(res, "row_14.objects_opened_1")
        if (f["status"], f["verdict"]) != ("dead_counter", "dead_counter"):
            out.append(f"row_14.objects_opened_1 {f['status']} {f['verdict']}")
        if "dead_counter segmentsOpened journey_c_1 folded_leaves" not in res["problems"]:
            out.append(f"problems {res['problems']}")
        return out

    def test_dead_counter(self):
        self.assertEqual(self.check_dead_counter(), [])

    def test_zero_opened_without_scan_gets_is_a_miss(self):
        for c in self.select("journey_c_1"):
            for p in c["stats"]["phases"]:
                if p["phase"] == "scan":
                    p["s3GetRequests"] = 0
        self.save_calls()
        _, res, _ = self.go(BANDS)
        f = figure(res, "row_14.objects_opened_1")
        self.assertEqual((f["status"], f["value"], f["verdict"]), ("value", 0, "miss"))
        self.assertIn("miss row_14.objects_opened_1", res["problems"])
        self.assertNotIn("dead_counter segmentsOpened journey_c_1 folded_leaves", res["problems"])

    def check_probe_per_object(self, evaluate_py=EVALUATE):
        """Item 6: row 3's probe band is per object read, not the call's total."""
        for c in self.select("spans_lookup_1h", "unfolded"):
            for p in c["stats"]["phases"]:
                if p["phase"] == "probe":
                    p["s3GetRequests"], p["s3GetWireBytes"] = 4, 16388
        self.save_calls()
        _, res, _ = self.go(BANDS, evaluate_py)
        f = figure(res, "row_03.probe_wire_per_object")
        return [] if (f["value"], f["verdict"]) == (4097.0, "pass") else [f"{f['value']} {f['verdict']}"]

    def test_probe_per_object(self):
        self.assertEqual(self.check_probe_per_object(), [])

    def check_miss_reason(self, evaluate_py=EVALUATE):
        """Item 3: row 7's miss carries the from-scratch reason bands.toml gives it."""
        _, res, _ = self.go(BANDS, evaluate_py)
        want = "miss row_07.spans_section_gets: from-scratch rebuild: section GETs = 2 x entry_count"
        return [] if want in res["problems"] else [f"problems {res['problems']}"]

    def test_miss_reason(self):
        self.assertEqual(self.check_miss_reason(), [])

    def check_leaf_share(self, evaluate_py=EVALUATE):
        """Item 14: the share is the named field's leaf bytes, not every leaf's."""
        _, res, _ = self.go(BANDS, evaluate_py)
        census = next(c for c in read_jsonl(self.stamp_path("census.jsonl")) if c["state"] == "folded_leaves")
        data = census["census"]["tenants"]["inv"]["data"]["logs"]["bytes"]
        with open(self.stamp_path("folded_leaves.logs.catalog.json")) as f:
            doc = json.load(f)
        out = []
        for field in ("trace_id", "request_id"):
            leaf = sum(lf["bytes"] for p in doc["parts"] for lf in p["leaves"] if lf["field"] == field)
            got = figure(res, f"row_08.logs_{field}_leaf_pct")["value"]
            if got != round(100.0 * leaf / data, 9):
                out.append(f"{field} {got}")
        return out

    def test_leaf_share(self):
        self.assertEqual(self.check_leaf_share(), [])

    def check_part_count(self, evaluate_py=EVALUATE):
        """Item 9: the part count is what the catalog holds, not what the load declared."""
        path = self.stamp_path("many_parts_24.spans.catalog.json")
        with open(path) as f:
            doc = json.load(f)
        doc["parts"] = doc["parts"][:-1]
        doc["part_count"] = len(doc["parts"])
        with open(path, "w") as f:
            json.dump(doc, f)
        _, res, _ = self.go(BANDS, evaluate_py)
        f = figure(res, "row_09.covering_parts_24")
        out = [] if (f["value"], f["verdict"]) == (23, "miss") else [f"{f['value']} {f['verdict']}"]
        if "miss row_09.covering_parts_24" not in res["problems"]:
            out.append(f"problems {res['problems']}")
        return out

    def test_part_count(self):
        self.assertEqual(self.check_part_count(), [])

    def check_in_refusal(self, evaluate_py=EVALUATE):
        """Item 11: a 1000-key list counts as refused only when the server said so."""
        for c in self.select("spans_in_1000"):
            del c["http_status"]
        self.save_calls()
        _, res, _ = self.go(BANDS, evaluate_py)
        f = figure(res, "row_11.control_1001_not_served")
        out = [] if f["verdict"] == "missing" else [f"{f['value']} {f['verdict']}"]
        if not any(p.startswith("missing row_11.control_1001_not_served") for p in res["problems"]):
            out.append(f"problems {res['problems']}")
        return out

    def test_in_refusal(self):
        self.assertEqual(self.check_in_refusal(), [])

    def check_state_absent(self, evaluate_py=EVALUATE):
        """Item 15: a state run.toml lists and calls.jsonl lacks is missing."""
        self.calls = [c for c in self.calls if c["state"] != "mixed"]
        self.save_calls()
        code, res, _ = self.go(evaluate_py=evaluate_py)
        out = []
        if code != 1:
            out.append(f"exit {code}")
        if "missing state mixed: no calls in calls.jsonl" not in res["problems"]:
            out.append(f"problems {res['problems']}")
        st = next(s for s in res["states"] if s["state"] == "mixed")
        if st["status"] != "missing":
            out.append(f"states {st}")
        return out

    def test_state_absent(self):
        self.assertEqual(self.check_state_absent(), [])

    def check_census_per_tenant(self, evaluate_py=EVALUATE):
        """Item 4: census figures read their own tenant's objects."""
        mod = load_module(evaluate_py, "evaluate_case")
        tenants = {"inv": {"data": {"logs": {"objects": 9}}}, "hits": {"data": {"logs": {"objects": 398}}}}
        census = {("unfolded",): {"state": "unfolded", "census": {"tenants": tenants}}}
        cfg = {"tenant": "inv", "hits": True}
        out = []
        for tenant, want in (("hits", 398), ("inv", 9)):
            got = mod.stamp_figure(None, census, cfg, "unfolded", f"census.{tenant}.logs.objects")
            if got != ("value", want, ""):
                out.append(f"{tenant} {got}")
        return out

    def test_census_per_tenant(self):
        self.assertEqual(self.check_census_per_tenant(), [])

    def to_s3_lane(self):
        toml = os.path.join(self.run_dir, "run.toml")
        with open(toml) as f:
            text = f.read()
        with open(toml, "w") as f:
            f.write(text.replace('lane = "local"', 'lane = "s3"'))

    def test_journey_wall_is_the_sum_of_its_steps(self):
        # Item 16: three 2 s steps make a 6 s journey, which misses < 5 s.
        self.to_s3_lane()
        for sid in ("failing_cell", "journey_b", "spans_lookup_1h"):
            for c in self.select(sid):
                c["wall_s"] = 2.0
        self.save_calls()
        _, res, _ = self.go(BANDS)
        self.assertEqual(figure(res, "row_15.lookup_wall_p50_s")["value"], 2.0)
        f = figure(res, "row_15.journey_wall_p50_s")
        self.assertEqual((f["value"], f["verdict"]), (6.0, "miss"))
        self.assertIn("miss row_15.journey_wall_p50_s", res["problems"])

    def test_journey_gets_over_budget(self):
        for c in self.select("journey_b"):
            for p in c["stats"]["phases"]:
                if p["phase"] == "scan":
                    p["s3GetRequests"] = 10_000
        self.save_calls()
        _, res, _ = self.go(BANDS)
        budget = figure(res, "row_15.journey_gets_budget")["value"]
        f = figure(res, "row_15.journey_total_gets")
        self.assertGreater(f["value"], budget)
        self.assertEqual(f["verdict"], "miss")
        self.assertIn("miss row_15.journey_total_gets", res["problems"])

    def test_unsealed_tail_wrong_answer(self):
        # Item 17: the tail's planted row missing from the mixed state's
        # answer is a wrong answer.
        c = next(c for c in self.select("logs_tail_request_id", "mixed") if c["try"] == 0)
        c["rows"], c["values"] = 0, []
        self.save_calls()
        code, res, _ = self.go(BANDS)
        self.assertEqual(code, 1)
        self.assertIn("wrong_answer logs_tail_request_id/mixed/cold try 0: rows 0 expected 1", res["problems"])
        self.assertEqual(figure(res, "row_13.wrong_answers")["value"], 1)


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
    "absent catalog stamp skipped": (
        '                    problems.add("missing", f"covering_parts.{s}.{lab}")',
        "                    pass",
        "check_missing_catalog_stamp",
    ),
    "absent field not named": (
        '    for name in lacking:\n        problems.add("missing", f"{name} in {at}")\n',
        '    for name in lacking[:0]:\n        problems.add("missing", f"{name} in {at}")\n',
        "check_absent_phases",
    ),
    "zero segmentsOpened taken as a value": (
        '    if name == "segments_opened" and value == 0:',
        "    if False:",
        "check_dead_counter",
    ),
    "probe wire not divided per object": (
        '            v = rnd(m["probe_wire_bytes"] / m["probe_gets"])',
        '            v = m["probe_wire_bytes"]',
        "check_probe_per_object",
    ),
    "miss reason dropped": (
        '        if miss and f.get("miss_reason"):',
        "        if False:",
        "check_miss_reason",
    ),
    "leaf share over every field": (
        '        return "value", rnd(100.0 * leaf_bytes(doc, field) / data["bytes"]), ""',
        '        return "value", rnd(100.0 * leaf_bytes(doc) / data["bytes"]), ""',
        "check_leaf_share",
    ),
    "part count read from the declaration": (
        '        return "value", doc.get("part_count", len(doc.get("parts", []))), ""',
        '        return "value", doc.get("parts_declared", 24), ""',
        "check_part_count",
    ),
    "any unsupported 1000-key call taken as refused": (
        '        refused = all(c.get("outcome") == UNSUPPORTED and COMPLEXITY_REFUSAL in (c.get("reason") or "")\n'
        '                      and "http_status" in c for c in lines)',
        '        refused = all(c.get("outcome") == UNSUPPORTED for c in lines)',
        "check_in_refusal",
    ),
    "state without calls passed over": (
        '            problems.add("missing", f"state {state}: no calls in calls.jsonl")\n',
        "            pass\n",
        "check_state_absent",
    ),
    "census read from the first tenant": (
        '        data = ((line.get("census") or {}).get("tenants") or {}).get(tenant, {}).get("data", {}).get(signal)',
        '        data = ((line.get("census") or {}).get("tenants") or {}).get("inv", {}).get("data", {}).get(signal)',
        "check_census_per_tenant",
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
    p = subprocess.run([BASH, os.path.join(BOX, "run.sh"), "--dry-run", "--lane", lane, "--run-toml", toml,
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

    LANE_STATES = {"local": ("compaction_in_flight", "erasure_in_flight", "many_parts_24"),
                   "s3": ("compaction_in_flight", "erasure_in_flight", "many_parts_744", "many_parts_2160")}

    def assert_flow(self, text, lane, hits=False):
        states = ("unfolded", "folded_no_leaves", "folded_leaves", "mixed") + self.LANE_STATES[lane]
        self.assertEqual(self.steps(text), [
            "bootstrap", "store", "host", "corpus", "declare", "load-base",
            *(f"state:{s}" for s in states), "evaluate", "finish"])
        b = self.blocks(text)
        for state in states:
            lines = b[f"state:{state}"]
            self.assertIn("STAMP stamps/transitions.jsonl", lines, state)
            self.assertIn("STAMP stamps/census.jsonl", lines, state)
            self.assertIn("STAMP stamps/restarts.jsonl", lines, state)
            if state != "compaction_in_flight":
                self.assertIn(f"STAMP stamps/{state}.server.json", lines, state)
        for state in states[:4]:
            lines = b[f"state:{state}"]
            self.assertIn("STAMP stamps/census.jsonl", lines, state)
            self.assertIn("STAMP stamps/restarts.jsonl", lines, state)
            j = "\n".join(lines)
            self.assertIn("stop server, drop page cache, start server", j)
            self.assertIn("RQ_STATS=1", j)
            self.assertIn("plan --statements " + STATEMENTS, j)
        folds = 3 if hits else 2
        fl = b["state:folded_no_leaves"]
        self.assertEqual(sum("catalog fold" in x and "--writers-stopped" in x and "--key-index-leaves" not in x
                             for x in fl), folds)
        lv = "\n".join(b["state:folded_leaves"])
        self.assertEqual(lv.count("--writers-stopped --json --key-index-leaves"), folds)
        self.assertEqual(lv.count("--rebuild-key-index --json"), folds)
        self.assertLess(lv.index("--key-index-leaves"), lv.index("--rebuild-key-index"))
        self.assertLess(lv.rindex("--rebuild-key-index"), lv.index("STAMP stamps/transitions.jsonl"))
        self.assertEqual(b["state:folded_leaves"].count("STAMP stamps/folds.jsonl"), 2 * folds)
        mx = b["state:mixed"]
        mj = "\n".join(mx)
        self.assertLess(mj.index("wait until the ingest hour"), mj.index("load-tail-spans"))
        self.assertLess(mj.index("load-tail-spans"), mj.index("load-tail-logs"))
        self.assertEqual(mx.count("STAMP stamps/transitions.jsonl"), 2)
        self.assertLess(mj.index("RQ_STATS=1"), mj.index("fold-fold_advance-spans"))
        self.assertIn("fold-fold_advance-logs", mj)
        cx = b["state:compaction_in_flight"]
        cj = "\n".join(cx)
        self.assertLess(cj.index("wait until 310 s past the current hour start"), cj.index("compact-spans"))
        self.assertEqual(cj.count("the compactions above run as background children, both signals at once"), 1)
        self.assertLess(cj.index("background children"), cj.index("--state compaction_in_flight"))
        self.assertLess(cj.index("RQ_STATS=1"), cj.index("kv-report"))
        self.assertEqual(cx.count("STAMP stamps/compaction.jsonl"), 2)
        self.assertEqual(cx.count("STAMP stamps/transitions.jsonl"), 2)
        self.assertEqual(cx.count("STAMP stamps/compaction_done.server.json"), 1)
        self.assertIn("--state compaction_done", cj)
        for state in self.LANE_STATES[lane]:
            if not state.startswith("many_parts_"):
                continue
            tenant = "mp" + state.rsplit("_", 1)[1]
            pj = "\n".join(b[f"state:{state}"])
            self.assertIn(f"load-{state}-spans", pj)
            self.assertLess(pj.index(f"fold-{state}-spans"), pj.index(f"fold-{state}_leaf_rebuild-spans"))
            self.assertIn(f"--tenant {tenant}", pj)
            self.assertIn(f"--state {state} ", pj)
            self.assertIn(f"--tenant {tenant} --sub {state}", pj)
        self.assertIn("evaluate.py --run", "\n".join(b["evaluate"]))
        return b

    def test_s3_dry_run(self):
        code, text = run_sh("s3", os.path.join(BOX, "run.example.toml"), self.path("out"))
        self.assertEqual(code, 0, text)
        b = self.assert_flow(text, "s3")
        boot = "\n".join(b["bootstrap"])
        self.assertIn("checkout -q --detach", boot)
        self.assertIn("force-frame-pointers=yes", boot)
        self.assertIn("-p ravel-server --features sql", boot)
        self.assertIn("pyarrow", boot)
        self.assertIn("store qualify", "\n".join(b["store"]))
        self.assertIn("--index-trace-id true", "\n".join(b["declare"]))
        self.assertIn("upload out/ (no DONE, FAILED or work/) to s3://", "\n".join(b["finish"]))
        self.assertIn("then terminate the instance", "\n".join(b["finish"]))
        self.assertNotIn("--tenant hits", text)
        self.assertNotIn("NOT-RUN", text)
        # Item 9: one part per ingest hour, N hours ending three hours before
        # the run's first hour, so both states' parts are sealed when they run.
        h0 = int(time.time()) // 3600
        for n in (744, 2160):
            pj = "\n".join(b[f"state:many_parts_{n}"])
            m = re.findall(r"--ingest-hour-stride 1 --ingest-hour-start (\d+)", pj)
            self.assertEqual(len(m), 1, n)
            self.assertIn(int(m[0]) + n + 3 - h0, (0, -1), (n, m[0], h0))
            fold = next(x for x in b[f"state:many_parts_{n}"] if f"fold-many_parts_{n}-spans" in x)
            self.assertIn("--snapshot-part-max-entries 1", fold)
            self.assertEqual(pj.count("--snapshot-part-max-entries 1"), 1, n)

    def example_toml(self, *swaps):
        with open(os.path.join(BOX, "run.example.toml")) as f:
            text = f.read()
        for old, new in swaps:
            self.assertEqual(text.count(old), 1, old)
            text = text.replace(old, new)
        p = self.path("run.toml")
        with open(p, "w") as f:
            f.write(text)
        return p

    def test_s3_dry_run_with_hits(self):
        # Item 4: the hits tenant is declared, loaded, folded, rebuilt and
        # stamped on its own, keyed on UserID.
        code, text = run_sh("s3", self.example_toml(("hits = false", "hits = true")), self.path("out"))
        self.assertEqual(code, 0, text)
        b = self.assert_flow(text, "s3", hits=True)
        self.assertIn("--key-index UserID", "\n".join(b["declare"]))
        self.assertIn("--batch-rows 100000 --target-bytes 1850000000", text)
        self.assertIn("load-hits.measure.json", text)
        lv = "\n".join(b["state:folded_leaves"])
        self.assertIn("fold-folded_leaves-hits", lv)
        self.assertIn("fold-leaf_rebuild-hits", lv)
        self.assertIn("catalog fold --tenant hits --shards 8 --signal logs --rebuild-key-index --json", lv)
        stamped = [s for s in b if "--tenant hits --signal logs --label hits" in "\n".join(b[s])]
        self.assertTrue({"state:unfolded", "state:folded_no_leaves", "state:folded_leaves"} <= set(stamped), stamped)

    def test_s3_dry_run_past_the_deadline(self):
        # Item 20: with the deadline already passed every state is NOT-RUN
        # with its reason, and the run still evaluates and finishes.
        code, text = run_sh("s3", self.example_toml(("max_minutes = 540", "max_minutes = 0")), self.path("out"))
        self.assertEqual(code, 0, text)
        states = ("unfolded", "folded_no_leaves", "folded_leaves", "mixed") + self.LANE_STATES["s3"]
        lines = [x for x in text.splitlines() if x.startswith("NOT-RUN ")]
        self.assertEqual(lines, [f"NOT-RUN {s}: deadline: run.max_minutes 0 passed (0 minutes elapsed)"
                                 for s in states])
        self.assertEqual(self.steps(text)[-2:], ["evaluate", "finish"])
        self.assertNotIn("RQ_STATS=1", text)

    def test_local_dry_run(self):
        code, text = run_sh("local", os.path.join(BOX, "run.local-smoke.toml"), self.path("out"))
        self.assertEqual(code, 0, text)
        b = self.assert_flow(text, "local")
        self.assertIn("--address 127.0.0.1:", "\n".join(b["store"]))
        self.assertNotIn("--tenant hits", text)
        self.assertNotIn("aws ", text)
        self.assertNotIn("terminate", text)
        self.assertNotIn("shutdown", text)

    def test_s3_lane_refuses_off_the_box(self):
        p = subprocess.run([BASH, os.path.join(BOX, "run.sh"), "--lane", "s3", "--run-toml",
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
        p = subprocess.run([BASH, os.path.join(BOX, "launch.sh"), "--dry-run", self.filled_toml()],
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
        p = subprocess.run([BASH, os.path.join(BOX, "launch.sh"), "--dry-run", os.path.join(BOX, "run.example.toml")],
                           capture_output=True, text=True, env=dict(os.environ, PATH=self.stub_path()))
        self.assertEqual(p.returncode, 64)

    def test_fetch_dry_run(self):
        env = dict(os.environ, PATH=self.stub_path())
        run_id = "investigations-20261103T000000Z-1-1"
        p = subprocess.run([BASH, os.path.join(BOX, "fetch.sh"), "--dry-run", self.filled_toml(), run_id],
                           capture_output=True, text=True, env=env, cwd=self.tmp)
        text = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, text)
        self.assertFalse(os.path.exists(self.path("aws-called")))
        self.assertIn(f"s3://case-bucket/investigations/runs/{run_id}/out", text)
        p = subprocess.run([BASH, os.path.join(BOX, "fetch.sh"), "--dry-run", self.filled_toml(), "case-run"],
                           capture_output=True, text=True, env=env, cwd=self.tmp)
        self.assertEqual(p.returncode, 64)

    def fetch_with_aws(self, ls_exit, *flags):
        d = self.path(f"stub-{ls_exit}")
        os.makedirs(d)
        with open(os.path.join(d, "aws"), "w") as f:
            f.write(f"#!/bin/sh\ncase \"$*\" in *' s3 ls '*) echo denied >&2; exit {ls_exit} ;; esac\nexit 0\n")
        os.chmod(os.path.join(d, "aws"), 0o755)
        env = dict(os.environ, PATH=d + os.pathsep + os.environ["PATH"])
        p = subprocess.run([BASH, os.path.join(BOX, "fetch.sh"), *flags, "--interval", "1", self.filled_toml(),
                            "investigations-20261103T000000Z-1-1"], capture_output=True, text=True, env=env,
                           cwd=self.tmp, timeout=60)
        return p.returncode, p.stdout + p.stderr

    def test_fetch_exits_69_when_a_marker_check_cannot_be_answered(self):
        # Kills fetch.sh's has_marker `*) ... exit 69` arm: an aws error
        # (255) read as "no marker yet" polls until the bound and exits 3.
        code, text = self.fetch_with_aws(255, "--partial")
        self.assertEqual(code, 69, text)
        self.assertIn("aws exit 255", text)
        self.assertIn("denied", text)
        # aws's own "not there" (exit 1) is an answer: --partial downloads.
        code, text = self.fetch_with_aws(1, "--partial")
        self.assertEqual(code, 3, text)
        self.assertIn("PARTIAL", text)

    # Arrays box/ scripts expand bare: each is assigned non-empty before use.
    NON_EMPTY_ARRAYS = {"CLI", "tenants", "args", "pids"}

    def test_possibly_empty_arrays_are_guarded_for_bash_3_2(self):
        # bash before 4.4 treats "${x[@]}" of an empty x as unbound under
        # set -u; any array outside NON_EMPTY_ARRAYS must use the
        # ${x[@]+"${x[@]}"} form.
        bare = re.compile(r'(?<!\+)"\$\{([A-Za-z_][A-Za-z_0-9]*)\[@\]\}"')
        found = []
        for name in sorted(os.listdir(BOX)) + ["../investigations.test.sh"]:
            if not name.endswith(".sh"):
                continue
            with open(os.path.join(BOX, name)) as f:
                for n, line in enumerate(f, 1):
                    for m in bare.finditer(line):
                        if m.group(1) not in self.NON_EMPTY_ARRAYS:
                            found.append(f"{name}:{n}: {m.group(1)}")
        self.assertEqual(found, [])
        self.assertTrue(bare.search('run "${CLI[@]}" x'))
        self.assertFalse(bare.search('${flags[@]+"${flags[@]}"}'))

    def test_empty_flags_dry_run_under_bash_3_2(self):
        # Kills an unguarded "${flags[@]}" at run.sh's load-base step:
        # run.example.toml has split_base_load = false, so flags is empty.
        ver = subprocess.run([BASH, "-c", "echo ${BASH_VERSINFO[0]}.${BASH_VERSINFO[1]}"], capture_output=True,
                             text=True).stdout.strip()
        if tuple(int(x) for x in ver.split(".")) >= (4, 4):
            self.skipTest(f"bash {ver} accepts an empty array under set -u; run under bash 3.2 to check")
        code, text = run_sh("s3", os.path.join(BOX, "run.example.toml"), self.path("out"))
        self.assertEqual(code, 0, text)
        copy = self.path("inv")
        shutil.copytree(HERE, copy, ignore=shutil.ignore_patterns("fixtures", "__pycache__"))
        p = os.path.join(copy, "box", "run.sh")
        with open(p) as f:
            src = f.read()
        guarded = 'load_signal base "$sig" "$WORK/corpus" "$TENANT" ${flags[@]+"${flags[@]}"}'
        self.assertEqual(src.count(guarded), 1)
        with open(p, "w") as f:
            f.write(src.replace(guarded, 'load_signal base "$sig" "$WORK/corpus" "$TENANT" "${flags[@]}"'))
        m = subprocess.run([BASH, p, "--dry-run", "--lane", "s3", "--run-toml", os.path.join(BOX, "run.example.toml"),
                            "--out", self.path("out2")], capture_output=True, text=True)
        self.assertNotEqual(m.returncode, 0, m.stdout + m.stderr)
        self.assertIn("unbound variable", m.stderr)


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


def gzip_to(src, dst):
    with open(src, "rb") as f, open(dst, "wb") as g:
        g.write(gzip.compress(f.read(), compresslevel=9, mtime=0))


def import_local_smoke(run_dir):
    with open(os.path.join(run_dir, "run.toml"), "rb") as f:
        lane = tomllib.load(f)["run"]["lane"]
    if lane != "local":
        print(f"{run_dir} is a {lane} run, not the local smoke", file=sys.stderr)
        return 64
    if not os.path.exists(os.path.join(run_dir, "DONE")):
        print(f"{run_dir} has no DONE marker: run.sh did not finish", file=sys.stderr)
        return 64
    shutil.rmtree(SMOKE, ignore_errors=True)
    os.makedirs(os.path.join(SMOKE, "stamps"))
    shutil.copy(os.path.join(run_dir, "run.toml"), SMOKE)
    gzip_to(os.path.join(run_dir, "calls.jsonl"), os.path.join(SMOKE, "calls.jsonl.gz"))
    gzip_to(os.path.join(run_dir, "truth.json"), os.path.join(SMOKE, "truth.json.gz"))
    for name in sorted(os.listdir(os.path.join(run_dir, "stamps"))):
        gzip_to(os.path.join(run_dir, "stamps", name), os.path.join(SMOKE, "stamps", name + ".gz"))
    return refresh_local_smoke()


if __name__ == "__main__":
    if sys.argv[1:] == ["--refresh-local-smoke"]:
        sys.exit(refresh_local_smoke())
    if len(sys.argv) == 3 and sys.argv[1] == "--import-local-smoke":
        sys.exit(import_local_smoke(sys.argv[2]))
    unittest.main()
