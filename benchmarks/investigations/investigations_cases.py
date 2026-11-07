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
README = os.path.join(HERE, "README.md")
# complexity_guard.rs flat_literal_list: a bare '..' string, an X'..' hex
# string, or a number, comma separated inside parentheses.
IN_LIST_LITERAL = r"'(?:[^']|'')*'|[Xx]'[0-9A-Fa-f]*'|[-+]?[0-9]+(?:\.[0-9]+)?"
FLAT_IN_LIST = re.compile(r"[ \t\n\r]*\((?:[ \t\n\r]*(?:%s)[ \t\n\r]*,)*[ \t\n\r]*(?:%s)[ \t\n\r]*\)"
                          % (IN_LIST_LITERAL, IN_LIST_LITERAL))
ADR = os.path.join(HERE, "..", "..", "docs", "adrs", "3692-selective-investigations-logs-traces.md")
HITS_TRUTH = os.path.join(HERE, "hits.truth.json")
SMOKE = os.path.join(HERE, "fixtures", "local-smoke")
BOX = os.path.join(HERE, "box")
PY = sys.executable
# investigations.test.sh passes the bash running it, so a run under bash 3.2
# runs every box/ script under 3.2 too.
BASH = os.environ.get("INV_BASH", "bash")
HOUR_NS = 3600 * 1_000_000_000
STATES = ("unfolded", "folded_no_leaves", "folded_leaves", "mixed", "advanced_leaves", "compaction_in_flight",
          "compaction_done", "erasure_in_flight", "many_parts_24", "many_parts_744", "many_parts_2160")
SMOKE_STATES = ("unfolded", "folded_no_leaves", "folded_leaves", "mixed", "compaction_in_flight",
                "erasure_in_flight", "many_parts_24")
UNSUPPORTED = "unsupported-on-this-build"
GUARD = "refused-by-guard"
REFUSAL_1000 = ("server refused the statement (HTTP 400): statement complexity 1001 exceeds the maximum of "
                "1000 tokens; simplify the statement")

# A ravel-server stand-in for the server-death tests (issue #3813): answers
# /readyz always, answers its main port with a minimal trace_get body, and
# dies by SIGKILL on itself without responding once STUB_DIE_AT says so.
# STUB_DIE_AT is "N" (die on request N only) or "N+" (die on N and every
# later request); STUB_COUNT_FILE persists the request count across restarts
# (a fresh process each time, same file).
STUB_SERVER_PY = '''#!/usr/bin/env python3
import http.server
import json
import os
import signal
import socketserver
import sys
import threading

socketserver.TCPServer.allow_reuse_address = True


def addr(flag):
    i = sys.argv.index(flag)
    host, port = sys.argv[i + 1].rsplit(":", 1)
    return host, int(port)


def bump_count():
    path = os.environ["STUB_COUNT_FILE"]
    try:
        with open(path) as f:
            n = int(f.read().strip() or "0")
    except FileNotFoundError:
        n = 0
    n += 1
    with open(path, "w") as f:
        f.write(str(n))
    return n


def should_die(n):
    # STUB_DIE_AT: comma-separated request numbers (1-based, counted across
    # restarts), each "N" (that request) or "N+" (that one and every later).
    for item in filter(None, os.environ.get("STUB_DIE_AT", "").split(",")):
        if item.endswith("+") and n >= int(item[:-1]):
            return True
        if not item.endswith("+") and n == int(item):
            return True
    return False


class Health(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


class Main(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def _handle(self):
        n = bump_count()
        if should_die(n):
            os.kill(os.getpid(), signal.SIGKILL)
            return
        body = json.dumps({"data": {"rows": [["s1"]], "columns": [{"name": "span_id"}]}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main():
    health = socketserver.TCPServer(addr("--listen-health"), Health)
    main_srv = socketserver.TCPServer(addr("--listen-http"), Main)
    threading.Thread(target=health.serve_forever, daemon=True).start()
    if os.environ.get("STUB_ARGV_FILE"):
        with open(os.environ["STUB_ARGV_FILE"], "a") as f:
            f.write(json.dumps(sys.argv[1:]) + "\\n")
    print("cache warmup: pass complete parts_warmed=0 elapsed_ms=1", flush=True)
    main_srv.serve_forever()


if __name__ == "__main__":
    main()
'''


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
# The corpus.json the committed smoke run stamped.
SMOKE_CORPUS = {"traces": 20000, "hours": 2, "seed": 7, "start_hour": 498257, "tail_traces": 10000, "tail_seed": 8,
                "tail_start_hour": 498259,
                "many_parts": {"24": {"traces": 240, "hours": 24, "seed": 31, "start_hour": 498231,
                                      "ingest_hour_start": 498232}}}


def merged_truth(out, c=SMALL_CORPUS):
    """truth.json as run.sh builds it from a corpus.json stamp: the base corpus,
    the tail hour and each many_parts_N corpus, folded by inv.py merge-truth."""
    gen(os.path.join(out, "base"), c["traces"], c["hours"], c["seed"], "--start-hour", str(c["start_hour"]))
    gen(os.path.join(out, "tail"), c["tail_traces"], 1, c["tail_seed"], "--start-hour", str(c["tail_start_hour"]),
        "--tail")
    subs = ["--sub", f"tail={os.path.join(out, 'tail', 'truth.json')}"]
    for n, mp in sorted(c["many_parts"].items()):
        d = os.path.join(out, f"mp{n}")
        gen(d, mp["traces"], mp["hours"], mp["seed"], "--start-hour", str(mp["start_hour"]),
            *(["--even-hours"] if mp.get("even_hours") else []))
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


BLAKE3_IV = (0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19)
BLAKE3_PERM = (2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8)


def blake3_compress(cv, m, block_len, flags):
    s = list(cv) + list(BLAKE3_IV[:4]) + [0, 0, block_len, flags]
    rot = lambda x, n: ((x >> n) | (x << (32 - n))) & 0xFFFFFFFF

    def g(a, b, c, d, x, y):
        s[a] = (s[a] + s[b] + x) & 0xFFFFFFFF
        s[d] = rot(s[d] ^ s[a], 16)
        s[c] = (s[c] + s[d]) & 0xFFFFFFFF
        s[b] = rot(s[b] ^ s[c], 12)
        s[a] = (s[a] + s[b] + y) & 0xFFFFFFFF
        s[d] = rot(s[d] ^ s[a], 8)
        s[c] = (s[c] + s[d]) & 0xFFFFFFFF
        s[b] = rot(s[b] ^ s[c], 7)
    for _ in range(7):
        for i, (a, b, c, d) in enumerate(((0, 4, 8, 12), (1, 5, 9, 13), (2, 6, 10, 14), (3, 7, 11, 15),
                                          (0, 5, 10, 15), (1, 6, 11, 12), (2, 7, 8, 13), (3, 4, 9, 14))):
            g(a, b, c, d, m[2 * i], m[2 * i + 1])
        m = [m[p] for p in BLAKE3_PERM]
    return [s[i] ^ s[i + 8] for i in range(8)]


def blake3_single_chunk(data):
    """BLAKE3 of an input of at most one 1,024-byte chunk, unkeyed: the
    blake3 crate's hash() for the short keys key8_str and key8_bytes take."""
    assert len(data) <= 1024
    blocks = [data[i:i + 64] for i in range(0, len(data), 64)] or [b""]
    cv = BLAKE3_IV
    for n, block in enumerate(blocks):
        flags = (1 if n == 0 else 0) | (2 | 8 if n == len(blocks) - 1 else 0)
        words = [int.from_bytes(block.ljust(64, b"\0")[i:i + 4], "little") for i in range(0, 64, 4)]
        cv = blake3_compress(cv, words, len(block), flags)
    return b"".join(w.to_bytes(4, "little") for w in cv)


def key8(kind, v):
    """The 8-byte key a KEY_IDX probe sorts and dedups on
    (crates/ravel-codec/src/key_idx.rs): an Id16's high 8 bytes, a string's
    or bytes' blake3[..8], an i64's big-endian v ^ 1<<63."""
    if kind == "id16":
        return bytes.fromhex(v)[:8]
    if kind == "str":
        return blake3_single_chunk(v.encode())[:8]
    if kind == "bytes":
        return blake3_single_chunk(v)[:8]
    if kind == "i64":
        return ((v & 0xFFFFFFFFFFFFFFFF) ^ (1 << 63)).to_bytes(8, "big")
    raise ValueError(kind)


MATRIX_BANDS = PASS_BANDS + """
[[row]]
key = "row_t2"
adr_row = 13
check = "case"
target = "case"
miss = "case"
source = "predicted"
requires_tenant = "inv"

[[row.figure]]
name = "mixed_rows_exact"
statement = "all"
state = "mixed"
metric = "matrix.state_rows_exact"
lane = "counter"
target_if = "v == 1"
"""


class Reports(unittest.TestCase):
    """inv.py extract_report over what `measure` writes: stdout and stderr in
    one file, so a ravel-cli `--json` report shares it with tracing lines."""

    TRACE = ("\x1b[2m2026-11-06T21:52:22.654860Z\x1b[0m \x1b[33m WARN\x1b[0m "
             "\x1b[2mravel_catalog::fold\x1b[0m: shard 3 hour 498333 deferred\n")
    # "frontier" last: its indented `"frontier": {}` line parses on its own.
    REPORT = {"signal": "logs", "key_index_ranged_gets": 1140, "key_index_build_passes": 1,
              "attempts": [{"parts": 1}], "nested": {"a": {"b": 2}}, "frontier": {}}

    @classmethod
    def setUpClass(cls):
        cls.inv = load_module(os.path.join(BOX, "lib", "inv.py"), "inv_reports")

    def pretty(self):
        return json.dumps(self.REPORT, indent=2) + "\n"

    def test_whole_document(self):
        self.assertEqual(self.inv.extract_report(self.pretty()), self.REPORT)

    def test_pretty_report_beside_tracing_lines(self):
        # Box C (#3918): every fold that did real work stamped report null,
        # because a tracing line shared the file with the pretty-printed
        # report and neither the whole-file nor the one-line parse took it.
        for text in (self.TRACE + self.pretty(), self.pretty() + self.TRACE,
                     self.TRACE + self.pretty() + self.TRACE + self.TRACE):
            self.assertEqual(self.inv.extract_report(text), self.REPORT)

    def test_nested_object_is_never_taken_for_the_report(self):
        # A parser taking the last line whose `{...}` tail parses would
        # return the indented `"frontier": {}` as the report.
        doc = self.inv.extract_report(self.TRACE + self.pretty())
        self.assertEqual(doc["key_index_ranged_gets"], 1140)

    def test_last_document_wins(self):
        first = json.dumps({"signal": "spans"}, indent=2) + "\n"
        self.assertEqual(self.inv.extract_report(first + self.TRACE + self.pretty()), self.REPORT)

    def test_load_report_line(self):
        text = self.TRACE + "rows: 10\nload report json : " + json.dumps({"rows": 10}) + "\n"
        self.assertEqual(self.inv.extract_report(text), {"rows": 10})

    def test_no_report(self):
        self.assertIsNone(self.inv.extract_report(self.TRACE + "compacted: 8\n"))


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

    def test_request_id_lists_exclude_the_planted_row_in_a_tiny_corpus(self):
        # One trace, five log rows, so every list above size 4 must take all
        # four unplanted rows and the planted row (drawn with probability
        # 4 in 5 by a plain sample of the range) must be redrawn, not kept.
        # FLIP: dropping the pool_row mapping (sampling range(n_log_rows)
        # directly) puts the present id in the 1,000 list about four runs in
        # five; building the pool as a list instead would hold 100,000,000
        # ints at the real lane's scale.
        with tempfile.TemporaryDirectory() as d:
            t, rows = gen(d, traces=1, hours=1, seed=11)
        present = t["request_id"]["present"]
        self.assertEqual(len(rows["logs"]), 5)
        self.assertEqual(sum(1 for r in rows["logs"] if r["request_id"] == present), 1)
        # The planted row carries the planted id in the data, so its drawn
        # form (request_id_of its trace and position) exists in no log row:
        # a list that holds every unplanted row's id, and only ids the data
        # holds, has excluded it. assertNotIn(present, ids) alone cannot
        # fail, because the lists never spell the planted constant.
        unplanted = {r["request_id"] for r in rows["logs"] if r["request_id"] != present}
        self.assertEqual(len(unplanted), 4)
        for size, ids in t["in_lists"]["request_id"].items():
            self.assertEqual(len(ids), min(int(size), 4), size)
            self.assertEqual(len(set(ids)), len(ids), size)
            self.assertLessEqual(set(ids), unplanted, size)
        self.assertEqual(set(t["in_lists"]["request_id"]["1000"]), unplanted)

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

    def test_object_ranges_reads_every_data_object(self):
        # Item 3: the ranges stamp lists every .rseg of the tenant and signal,
        # not commit records, provenance or the other signal's objects.
        tenant = "a" * 32
        keys = os.path.join(self._tmp.name, "keys.tsv")
        with open(keys, "w") as f:
            for size, key in ((10, f"t/{tenant}/s/l0/0000/b.rseg"), (20, f"t/{tenant}/s/l0/0001/a.rseg"),
                              (5, f"t/{tenant}/s/c/0000/x.commit"), (3, f"t/{tenant}/s/prov"),
                              (7, f"t/{tenant}/l/l0/0000/c.rseg"), (9, f"t/{'b' * 32}/s/l0/0000/d.rseg")):
                f.write(f"{size}\t{key}\n")
        seen = []

        def run_cli(argv, stop_at=None):
            seen.append(argv[-1])
            lo = 100 if argv[-1].endswith("a.rseg") else 200
            return [f"min_start_ts_ns: {lo}", f"max_end_ts_ns: {lo + 50}"] + (
                [] if argv[-1].endswith("a.rseg") else ["min_trace_id: 00"])

        old, self.inv.run_cli = self.inv.run_cli, run_cli
        try:
            got = self.inv.object_ranges(["ravel-cli"], keys, tenant, "spans")
        finally:
            self.inv.run_cli = old
        self.assertEqual(got, [
            {"key": f"t/{tenant}/s/l0/0000/b.rseg", "bytes": 10, "min_ns": 200, "max_ns": 250},
            {"key": f"t/{tenant}/s/l0/0001/a.rseg", "bytes": 20, "min_ns": 100, "max_ns": 150},
        ])
        self.assertEqual(seen, [f"t/{tenant}/s/l0/0000/b.rseg", f"t/{tenant}/s/l0/0001/a.rseg"])

    def test_needle_totals_null_when_unreadable(self):
        # Item 5: a sum inv.py cannot vouch for is written null, never 0.
        def obj(*cols, blocks=4):
            return {"block_count": blocks, "record_count": 100,
                    "field_dir": [{"name": "body", "present_blocks": 4, "null_count": 0}, *cols]}

        tag = {"name": "tag", "present_blocks": 2, "null_count": 90}
        rare = {"name": "needle_rare", "present_blocks": 1, "null_count": 99}
        got = self.inv.needle_totals([obj(tag, rare), obj(tag)])
        self.assertEqual((got["block_count"], got["tag_present_blocks"], got["needle_rare_present_blocks"],
                          got["needle_rare_null_count"]), (8, 4, 1, 99))
        # A column no object lists is unknown, not absent.
        self.assertEqual((got["needle_sparse_present_blocks"], got["needle_common_present_blocks"]), (None, None))
        got = self.inv.needle_totals([obj(tag, {"name": "needle_rare", "null_count": 99}), obj(tag, rare)])
        self.assertEqual((got["needle_rare_present_blocks"], got["needle_rare_null_count"]), (None, 198))
        self.assertEqual(got["tag_present_blocks"], 4)
        for bad in ({"block_count": 4, "record_count": 100, "field_dir": []}, obj(tag, blocks=None)):
            got = self.inv.needle_totals([obj(tag, rare), bad])
            self.assertEqual(set(got.values()), {None}, bad)

    def test_evaluator_window_matches_the_plan(self):
        # Item 3: the evaluator's window for a candidate count is the one
        # inv.py's plan sent.
        ev = load_module(EVALUATE, "evaluate_window")
        st = next(s for s in self.st if s["id"] == "spans_lookup_1h")
        for run in range(len(self.truth["saved_traces"])):
            start_s, end_s = self.inv.window_bounds(st, self.truth, run, 0)
            start, end = ev.call_window_ns(st, self.truth, {"try": run})
            self.assertEqual((round(start / 1e9, 6), round(end / 1e9, 6)), (round(start_s, 6), round(end_s, 6)))

    def test_every_placeholder_fills(self):
        for s in self.st:
            t, _ = self.inv.sub_truth(self.truth, s.get("truth_sub"))
            if s.get("sql") and not (s.get("requires") and not t.get(s["requires"])):
                self.inv.fill(s["sql"], self.inv.placeholders(t, 0))

    def test_over_cap_control_lists(self):
        # Row 11's control is 1,025 keys, one over KEY_SET_MAX_KEYS: the
        # 1,000-key list, control_extra and padding the corpus cannot hold,
        # with the generator's 1,000-plus-extra answer as its oracle. Kills a
        # control still built at 1,001 keys (the 1,001-key list is a key set
        # under the cap) and padding that names a real key.
        for s in ("spans_in_1025", "logs_trace_in_1025", "logs_request_in_1025"):
            sql = next(x for x in self.st if x["id"] == s)["sql"]
            filled = self.inv.fill(sql, self.inv.placeholders(self.truth, 0))
            keys = re.findall(r"X'([0-9a-f]{32})'|'(req-[^']*)'", filled)
            self.assertEqual(len(keys), 1025, s)
            self.assertEqual(len(set(keys)), 1025, s)
        for field, answers in (("trace_id", ("in_trace_id.{}.spans", "in_trace_id.{}.logs")),
                               ("request_id", ("in_request_id.{}",))):
            ids = self.truth["over_cap"][field]
            top = self.truth["in_lists"][field]["1000"]
            extra = self.truth["control_extra"][field]
            self.assertEqual(ids[:len(top)], top, field)
            pad = ids[len(top) + (extra is not None):]
            self.assertEqual(len(ids), 1025, field)
            if extra is not None:
                self.assertEqual(ids[len(top)], extra, field)
            if field == "request_id":
                self.assertTrue(all(p.startswith("req-over-cap-absent-") for p in pad), pad[:3])
            for key in answers:
                over, base = self.truth["answers"][key.format(1025)], self.truth["answers"][key.format(1001)]
                self.assertEqual(over["rows"], base["rows"], key)
                self.assertEqual(over.get("row_set"), base.get("row_set"), key)
                self.assertEqual(over["requested"] * 1001, base["requested"] * 1025, key)
        # The small corpus's 1,000 traces are all in the 1,000-key list, so
        # 25 trace ids are padding and the request id list gains its extra.
        self.assertIsNone(self.truth["control_extra"]["trace_id"])
        self.assertIsNotNone(self.truth["control_extra"]["request_id"])

    def test_blake3_matches_the_reference_vectors(self):
        # key_idx.rs pins blake3("")[..8]; the rest are the BLAKE3 test
        # vectors (input bytes i % 251) at one block, two blocks and a chunk
        # less one byte.
        for data, want in ((b"", "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"),
                           (b"abc", "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"),
                           (bytes(i % 251 for i in range(1023)),
                            "10108970eeda3eb932baac1428c7a2163b0e924c9a9e25b35bba72b28f70bd11")):
            self.assertEqual(blake3_single_chunk(data).hex(), want, len(data))
        self.assertEqual(key8("i64", 0), bytes([0x80] + [0] * 7))
        self.assertEqual(key8("i64", -1), bytes([0x7f] + [0xff] * 7))
        self.assertEqual(key8("id16", "0102030405060708090a0b0c0d0e0f10"), bytes(range(1, 9)))

    def test_over_cap_lists_hold_over_1024_distinct_key8(self):
        # KeySet::new (key_index_route.rs) sorts and dedups the IN list's
        # key8s and refuses only above KEY_SET_MAX_KEYS, so a 1,025-key list
        # whose padding shares key8s is a key set under the cap and row 11's
        # over-cap control never runs. Kills trace padding that differs only
        # in the low 8 bytes (every one key8 0) and padding whose key8 is a
        # listed id's.
        kinds = {"trace_id": "id16", "request_id": "str"}
        for field, ids in self.truth["over_cap"].items():
            distinct = len({key8(kinds[field], v) for v in ids})
            self.assertGreater(distinct, self.inv.KEY_SET_MAX_KEYS, field)
            self.assertEqual(distinct, len(ids), field)
        self.assertEqual(set(self.truth["over_cap"]), set(kinds))
        # A corpus whose largest list is smaller takes more padding: the
        # whole 1,025 of either field is key8-distinct on its own.
        for field, kind in kinds.items():
            pad = self.inv.over_cap_padding(field, self.inv.OVER_CAP_KEYS, [])
            self.assertEqual(len({key8(kind, v) for v in pad}), self.inv.OVER_CAP_KEYS, field)
        # The small corpus has no trace outside the list (control_extra is a
        # held id the list lacks), so padding key8-distinct from the list is
        # padding the corpus does not hold.
        self.assertIsNone(self.truth["control_extra"]["trace_id"])

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

    def test_three_statements_run_warm(self):
        """docs/internal/bench-reports.md: 43 of the 46 statements are cold_only."""
        self.assertEqual(len(self.st), 46)
        self.assertEqual([s["id"] for s in self.st if not s.get("cold_only")],
                         ["spans_lookup_norange", "spans_lookup_1h", "logs_trace_1h"])

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

    @staticmethod
    def guard_units(sql, key_sets=True):
        """complexity_guard's scan for the statements here: one unit per token
        start, a word run or a quoted literal one token, any other character
        one token. An X'..' literal is the X run plus the quoted literal.
        With key_sets (ADR-3692's key set amendment), an IN followed by a flat
        list of literals costs the IN plus in_list_units: one unit, and one
        per 64 literals rounded up. Without it, main before the amendment,
        the list is counted token by token."""
        n = i = 0
        while i < len(sql):
            c = sql[i]
            if c.isspace():
                i += 1
                continue
            n += 1
            if c == "'":
                i = sql.index("'", i + 1) + 1
            elif c.isalnum() or c in "_$":
                start = i
                while i < len(sql) and (sql[i].isalnum() or sql[i] in "_$"):
                    i += 1
                flat = FLAT_IN_LIST.match(sql, i) if key_sets and sql[start:i].lower() == "in" else None
                if flat:
                    n += 1 + -(-len(re.findall(IN_LIST_LITERAL, flat.group(0))) // 64)
                    i = flat.end()
            else:
                i += 1
        return n

    def test_in_list_caps_from_the_complexity_guard(self):
        # Item 9. Before ADR-3692's key set amendment an X list held at most
        # 330 keys (3n + 8) and a quoted list at most 496 (2n + 8) under
        # MAX_STATEMENT_COMPLEXITY = 1,000, so the 1,000- and 1,025-key lists
        # carry expect_refusal_on_main and fixtures/local-smoke reads them
        # refused. Kills a cap stated from 2 units per X element (495) or from
        # 1,000 / 2. Since the amendment a flat literal list costs one unit
        # plus one per 64 literals, so every one of these statements costs 24
        # units at 1,000 keys and 25 at the 1,025-key control, which is under
        # MAX_IN_LIST_LITERALS and so admitted, not refused: kills a harness
        # or README still reading the old caps as the current guard's, and a
        # control judged as a refusal the current guard never makes.
        with open(os.path.join(HERE, "..", "..", "crates", "ravel-sql", "src", "complexity_guard.rs")) as f:
            guard = f.read()
        lines = guard.splitlines()
        for anchor in ("pub const MAX_STATEMENT_COMPLEXITY: usize = 1_000;",
                       "pub const MAX_IN_LIST_LITERALS: usize = 4_096;",
                       "pub const IN_LIST_LITERALS_PER_UNIT: usize = 64;",
                       "    1 + literals.div_ceil(IN_LIST_LITERALS_PER_UNIT)",
                       "    let count = scan(sql, MAX_STATEMENT_COMPLEXITY);",
                       "    if count > MAX_STATEMENT_COMPLEXITY {"):
            self.assertEqual(lines.count(anchor), 1, anchor)
        self.assertIn('if word.eq_ignore_ascii_case("in")', guard)
        self.assertIn("count += in_list_units(literals);", guard)
        by = {x["id"]: x for x in self.st}
        caps = {}
        for sid, elem in (("spans_in_1000", "X'{:032x}'"), ("logs_trace_in_1000", "X'{:032x}'"),
                          ("logs_request_in_1000", "'req-{:016x}-000'")):
            sql = by[sid]["sql"]
            fill = lambda n: re.sub(r"\{[a-z_0-9]+\}", ", ".join(elem.format(k) for k in range(n)), sql)
            per = 3 if elem.startswith("X") else 2
            for n in (1, 2, 330):
                self.assertEqual(self.guard_units(fill(n), key_sets=False), per * n + 8, (sid, n))
            caps[sid] = max(n for n in range(1, 1001) if self.guard_units(fill(n), key_sets=False) <= 1000)
            self.assertGreater(self.guard_units(fill(1000), key_sets=False), 1000, sid)
            self.assertGreater(self.guard_units(fill(1025), key_sets=False), 1000, sid)
            for n, units in ((1, 9), (64, 9), (65, 10), (1000, 24), (1025, 25), (4096, 72)):
                self.assertEqual(self.guard_units(fill(n)), units, (sid, n))
            self.assertTrue(by[sid].get("expect_refusal_on_main"), sid)
            over = by[sid.replace("1000", "1025")]
            self.assertTrue(over.get("expect_refusal_on_main"), sid)
            self.assertIn("_1025}", over["sql"], sid)
            self.assertTrue(over["truth_key"].startswith(("in_trace_id.1025.", "in_request_id.1025")), sid)
        # The control is one key over KEY_SET_MAX_KEYS and under the guard's
        # literal cap: admitted, and carried as no key set.
        with open(os.path.join(HERE, "..", "..", "crates", "ravel-query", "src", "key_index_route.rs")) as f:
            self.assertIn("pub const KEY_SET_MAX_KEYS: usize = 1_024;", f.read())
        inv = load_module(os.path.join(BOX, "lib", "inv.py"), "inv_caps")
        self.assertEqual(inv.OVER_CAP_KEYS, 1025)
        self.assertLessEqual(inv.OVER_CAP_KEYS, 4096)
        self.assertEqual(caps, {"spans_in_1000": 330, "logs_trace_in_1000": 330, "logs_request_in_1000": 496})
        for doc in (README, STATEMENTS):
            with open(doc) as f:
                text = " ".join(f.read().split())
            self.assertIn("330", text, doc)
            self.assertIn("496", text, doc)
            self.assertIn("3n + 8", text, doc)
            self.assertIn("2n + 8", text, doc)
            self.assertIn("one per 64 literals", text, doc)
            self.assertIn("24 units", text, doc)
            self.assertNotRegex(text, r"\b999 keys\b", doc)
            self.assertNotRegex(text, r"complexity_guard\.rs:\d", doc)
        # Box A's refusal of a 1,000-key list read "statement complexity 1001
        # exceeds the maximum of 1000 tokens": check stops scanning at the cap
        # plus one, so 1001 is the stop point, not one unit per key. Kills a
        # README that reads the cap off the refusal text (999 keys).
        self.assertIn("The returned count is then `stop_above + 1`, never the full", guard)
        self.assertIn("if count > stop_above {", guard)
        with open(README) as f:
            text = " ".join(f.read().split())
        for claim in ("an X list of 330 keys is admitted and one of 331 refused",
                      "a quoted list of 496 admitted and one of 497 refused",
                      "a 999-key list of either kind refused with the same `statement complexity 1001`",
                      "investigations-20261104T001915Z-46224-17727"):
            self.assertIn(claim, text)

    def adr_band_rows(self):
        with open(ADR) as f:
            lines = f.read().splitlines()
        header = "| check | target | miss | source |"
        self.assertEqual(lines.count(header), 1)
        head = lines.index(header)
        self.assertEqual(lines[head + 1], "|---|---|---|---|")
        # The table is the one under the Acceptance heading, wherever the ADR
        # puts it: the nearest heading above it.
        above = next(x for x in reversed(lines[:head]) if x.startswith("#"))
        self.assertTrue(above.startswith("## Acceptance"), above)
        rows = []
        for line in lines[head + 2:]:
            if not line.startswith("|"):
                break
            cells = [c.strip() for c in line.strip().strip("|").split(" | ")]
            self.assertEqual(len(cells), 4, line[:60])
            rows.append(cells)
        return head + 1, rows

    def test_band_text_is_the_adr_table_verbatim(self):
        # Item 17: each row's check, target, miss and source text is the ADR
        # Acceptance table's cell, the check without its "N. " number.
        _, adr = self.adr_band_rows()
        self.assertEqual(len(adr), 18)
        rows = self.bands["row"]
        for n, (r, cells) in enumerate(zip(rows, adr), 1):
            num, _, check = cells[0].partition(". ")
            self.assertEqual(num, str(n))
            self.assertEqual(r["check"].strip(), check, f"{r['key']}.check")
            for k, cell in zip(("target", "miss", "source"), cells[1:]):
                self.assertEqual(r[k].strip(), cell, f"{r['key']}.{k}")

    def test_bands_rows_1_to_18(self):
        rows = self.bands["row"]
        self.assertEqual([r["adr_row"] for r in rows], list(range(1, 19)))
        self.assertEqual([r["key"] for r in rows], [f"row_{n:02d}" for n in range(1, 19)])
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
        # Rows 2, 5, 15 and 18 time statements; row 16's slowdown is a timing
        # figure of a control run this harness does not perform.
        self.assertEqual(timed, ["row_02", "row_05", "row_15", "row_16", "row_18"])
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

    def test_control_1025_refused_by_guard_on_main(self):
        """bands.toml row 11: the fixture's build refuses the 1,000-key lists."""
        with gzip.open(os.path.join(SMOKE, "results.local.json.gz"), "rt") as f:
            res = json.load(f)
        for name in ("spans_control_1025", "logs_trace_control_1025", "logs_request_control_1025"):
            f = figure(res, f"row_11.{name}")
            self.assertEqual((f["status"], f["verdict"]), (GUARD, GUARD), name)

    def test_fixture_is_under_300_kb(self):
        total = sum(os.path.getsize(os.path.join(d, n)) for d, _, names in os.walk(SMOKE) for n in names)
        self.assertLess(total, 300_000)

    def test_problems_are_exactly_these(self):
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        self.assertEqual(res["verdict"], "fail")
        self.assertEqual(res["problems"], [
            "miss row_01.wire_bytes",
            "miss row_02.wire_bytes",
            "miss row_03.probe_wire_per_object",
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
                self.assertIn(f["status"], ("value", UNSUPPORTED, GUARD, "dead_counter"), f["id"])
            if f["status"] in (UNSUPPORTED, GUARD):
                self.assertTrue(f["reason"], f["id"])

    def test_states_section(self):
        # Item 15: every state is listed, run or not, with the reason it was not.
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        got = {s["state"]: (s["status"], s.get("reason")) for s in res["states"]}
        want = {s: ("run", None) for s in SMOKE_STATES + ("fold_advance", "compaction_done")}
        want.update({s: ("not-run", "not in run.toml states") for s in ("advanced_leaves", "many_parts_744", "many_parts_2160")})
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
        # The over-cap control has no 1,000-key answer to compare against.
        for prefix in ("spans", "logs_trace", "logs_request"):
            f = figure(res, f"row_11.{prefix}_control_1025")
            self.assertEqual((f["status"], f["value"], f["verdict"], f["reason"]),
                             (GUARD, None, GUARD, f"refused by the SQL complexity guard: {REFUSAL_1000}"))
        self.assertEqual(figure(res, "row_11.spans_1000_rows_exact")["reason"], REFUSAL_1000)
        # Item 8: the guard's refusals are counted apart from unsupported figures.
        self.assertEqual(res["refusal_classes"], {GUARD: {"figures": 15, "calls": 24},
                                                  UNSUPPORTED: {"figures": 29, "calls": 0}})
        guarded = sorted(f["id"] for f in res["figures"] if f["status"] == GUARD)
        self.assertEqual(len(guarded), 15)
        self.assertTrue(all(i.startswith("row_11.") for i in guarded), guarded)

    def test_not_requested_rows_listed_beside_the_verdict(self):
        # Item 9: gated rows run.toml never asked for, in results and summary.
        _, res, text = evaluate(smoke_run(self.path("run")), self.path("out"))
        self.assertEqual(res["not_requested"], [
            {"row": 5, "key": "row_05", "reason": "run.toml hits = false"},
            {"row": 6, "key": "row_06", "reason": "run.toml hits = false"},
            {"row": 9, "key": "row_09",
             "figures": ["covering_parts", "resolve_gets", "leaf_gets", "parts_beyond_ceiling", "rows_exact"],
             "reason": "run.toml states lacks ['many_parts_744']"},
            {"row": 10, "key": "row_10", "reason": "run.toml states lacks ['many_parts_2160']"},
            {"row": 12, "key": "row_12", "reason": "run.toml scales lacks [10, 100]"},
            {"row": 16, "key": "row_16", "reason": "run.toml hits = false, baseline = false"},
            {"row": 18, "key": "row_18", "reason": "run.toml states lacks ['advanced_leaves']"},
        ])
        self.assertEqual(text.splitlines()[:3], [
            "selective investigations, lane local: fail",
            "not requested: row 5 (run.toml hits = false); row 6 (run.toml hits = false); row 9 figures "
            "covering_parts, resolve_gets, leaf_gets, parts_beyond_ceiling, rows_exact (run.toml states lacks "
            "['many_parts_744']); row 10 (run.toml "
            "states lacks ['many_parts_2160']); row 12 (run.toml scales lacks [10, 100]); row 16 (run.toml "
            "hits = false, baseline = false); row 18 (run.toml states lacks ['advanced_leaves'])",
            "refusal classes: refused-by-guard 15 figure(s), 24 call(s); unsupported-on-this-build 29 figure(s), "
            "0 call(s)",
        ])

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

    def test_section_gets_need_the_pass_count(self):
        # Item 3: the fixture's advancing fold read every entry's sections,
        # 2 x its tail entries, but its build reports no
        # key_index_build_passes, and row 7 is judged per pass, so the figure
        # is unsupported, not a miss and not a pass (the per-pass cases are
        # check_row_7_box_d and check_row_7_rebuild).
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        for sig in ("spans", "logs"):
            f = figure(res, f"row_07.{sig}_section_gets")
            tail = figure(res, f"row_07.{sig}_tail_entries")["value"]
            self.assertEqual((f["value"], f["verdict"]), (2 * tail, UNSUPPORTED), sig)
            self.assertIn(f"band refers to figure row_07.{sig}_build_passes", f["reason"])
            for name in ("sections_read", "sections_per_pass", "leaf_gets", "build_passes"):
                self.assertEqual(figure(res, f"row_07.{sig}_{name}")["status"], UNSUPPORTED, (sig, name))

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

    def test_probe_wire_per_object_reads_the_ranges_stamp(self):
        # The smoke stamps unfolded's per-object event ranges, the only state
        # row 3 reads; check_probe_per_object covers the figure's arithmetic.
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        f = figure(res, "row_03.probe_wire_per_object")
        self.assertEqual((f["status"], f["value"], f["verdict"]), ("value", 65536.0, "miss"))

    def test_probe_wire_per_object_reads_missing_without_ranges(self):
        # Without the ranges stamp the per-object denominator is missing
        # rather than guessed.
        run = smoke_run(self.path("run"))
        os.remove(os.path.join(run, "stamps", "unfolded.spans.ranges.json"))
        _, res, _ = evaluate(run, self.path("out"))
        f = figure(res, "row_03.probe_wire_per_object")
        self.assertEqual((f["status"], f["value"], f["reason"]), (
            "missing", None, "stamps/unfolded.spans.ranges.json absent: no per-object event ranges to count the "
            "window's candidate objects"))

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
        self.assertEqual(figure(res, "row_08.logs_request_id_leaf_pct")["value"], 27.532655509)

    def test_maintenance_per_part_month(self):
        # Item 13: spans leaf rebuild, 8 section GET x $0.0004/1000 + 1 leaf
        # PUT x $0.005/1000 = $8.2e-06 over 1 month, plus 219887 leaf bytes
        # x $0.023/GB-month = $5.057401e-06, over 1 part = $1.3257401e-05,
        # $1.3257e-05 at evaluate.py's nine decimal places.
        _, res, _ = evaluate(smoke_run(self.path("run")), self.path("out"))
        m = next(m for m in res["cost"]["maintenance"] if m["signal"] == "spans")
        self.assertEqual((m["section_gets"], m["leaf_puts"], m["leaf_bytes"], m["parts"]), (8, 1, 219887, 1))
        self.assertEqual(m["maintenance_per_part_month"], round(8 * 0.0004 / 1000 + 1 * 0.005 / 1000
                                                                + 219887 / 1e9 * 0.023, 9))
        self.assertEqual(m["maintenance_per_part_month"], 1.3257e-05)

    def test_warm_served_counts_cold_calls_the_cache_served(self):
        # Item 18: a cold call after a restart and page-cache drop can still
        # be served from the server's own warmed cache; each is listed.
        run = smoke_run(self.path("run"))
        _, res, text = evaluate(run, self.path("out"))
        want = sorted((c["statement"], c["state"], c["try"]) for c in read_jsonl(os.path.join(run, "calls.jsonl"))
                      if c.get("temp") == "cold" and c["outcome"] == "ok"
                      and sum(p.get("cacheServedBytes", 0) for p in c["stats"]["phases"]) > 0)
        self.assertEqual(sorted((w["statement"], w["state"], w["try"]) for w in res["warm_served"]), want)
        self.assertEqual(len(want), 126)
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
            "compaction_done", "erasure_in_flight", "many_parts_24"])
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

    def test_needle_blocks_over_ten_written_blocks(self):
        # Item 10: every logs object inspected; 100,000 rows in 8,192-row
        # blocks; the rare needle sits in exactly the blocks truth predicts.
        with open(os.path.join(smoke_run(self.path("run")), "stamps", "needle_blocks.json")) as f:
            nb = json.load(f)
        self.assertEqual((nb["objects_inspected"], nb["record_count"], nb["truth_block_rows"]),
                         (nb["objects_total"], 100_000, 8192))
        self.assertEqual(nb["block_count"], 14)
        self.assertGreaterEqual(nb["block_count"], 10)
        self.assertEqual(nb["needle_rare_present_blocks"], nb["truth_needle_blocks"]["needlerare"])
        self.assertEqual(nb["needle_rare_present_blocks"], 1)
        self.assertEqual((nb["needle_sparse_present_blocks"], nb["truth_needle_blocks"]["needlesparse"]), (2, 2))
        self.assertEqual((nb["needle_common_present_blocks"], nb["truth_needle_blocks"]["needlecommon"]), (3, 3))
        self.assertEqual((nb["tag_present_blocks"], nb["truth_needle_blocks"]["any"]), (4, 5))

    def test_erasure_in_flight_reads_pending_and_filters_the_victim(self):
        # Item 12: run.sh's PENDING check saw the request, and the victim's
        # lookup returned no row under it while the unfolded state returned one.
        run = smoke_run(self.path("run"))
        _, res, _ = evaluate(run, self.path("out"))
        er = read_jsonl(os.path.join(run, "stamps", "erasure.jsonl"))
        self.assertEqual(len(er), 1)
        victim = er[0]["victim_request_id"]
        self.assertEqual(er[0]["status_after"]["fields"]["predicate"], f"request_id={victim}")
        self.assertEqual(sum(line.endswith(": PENDING") for line in er[0]["status_after"]["lines"]), 1)
        rows = {(c["state"], c["temp"], c["try"]): c["rows"] for c in read_jsonl(os.path.join(run, "calls.jsonl"))
                if c["statement"] == "logs_erasure_victim" and c["outcome"] == "ok"}
        self.assertEqual({k: v for k, v in rows.items() if k[0] == "erasure_in_flight"},
                         {("erasure_in_flight", "cold", 0): 0, ("erasure_in_flight", "cold", 1): 0})
        self.assertEqual(rows[("unfolded", "cold", 0)], 1)
        self.assertEqual(figure(res, "row_13.erasure_in_flight_rows_exact")["value"], 1)

    def test_truth_regenerates_from_the_stamped_corpus(self):
        run = smoke_run(self.path("run"))
        with open(os.path.join(run, "stamps", "corpus.json")) as f:
            c = json.load(f)
        with open(os.path.join(SMOKE, "run.toml"), "rb") as f:
            corpus = tomllib.load(f)["corpus"]
        self.assertEqual((c["traces"], c["hours"], c["seed"]), (corpus["traces"], corpus["hours"], corpus["seed"]))
        self.assertEqual(c, {**SMOKE_CORPUS, "truth_sha256": c["truth_sha256"]})
        truth = merged_truth(self.path("g"), c)
        with gzip.open(os.path.join(SMOKE, "truth.json.gz")) as f:
            committed = json.load(f)
        for t in (committed, truth):
            for doc in (t, *t["sub"].values()):
                self.assertEqual(doc["generator"].pop("format"), "parquet" if t is committed else "jsonl")
        # The fixture's run predates the over-cap lists merge-truth now adds;
        # they are the only difference.
        self.assertNotIn("over_cap", committed)
        load_module(os.path.join(BOX, "lib", "inv.py"), "inv_over_cap").add_over_cap(committed)
        self.assertEqual([len(v) for v in truth["over_cap"].values()], [1025, 1025])
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

    def test_server_died_call_is_missing_with_its_reason(self):
        reason = "server died during spans_in_1/folded_leaves try 0, signal 9"
        for c in self.select("spans_in_1"):
            if c["try"] == 0:
                c["outcome"] = "server_died"
                c["reason"] = reason
                for k in ("rows", "columns", "values", "values_sha256"):
                    c.pop(k, None)
        self.save_calls()
        code, res, _ = self.go()
        self.assertEqual(code, 1)
        f = figure(res, "row_t1.spans_1_rows_exact")
        self.assertEqual((f["status"], f["verdict"], f["reason"]), ("missing", "missing", reason))
        self.assertEqual(res["verdict"], "fail")
        self.assertIn(f"missing row_t1.spans_1_rows_exact: {reason}", res["problems"])

    def check_server_died_everywhere(self, evaluate_py=EVALUATE):
        """A server_died call is a problem of the run whichever group it is
        in, read by a band figure or not: with try 0 of every call group of
        the fixture replaced by one, each group's death is a named problem
        and the verdict is fail."""
        groups = {}
        for c in self.calls:
            if c.get("try", 0) == 0 and c.get("outcome") == "ok":
                groups.setdefault((c["statement"], c["state"], c.get("temp", "cold")), c)
        for (statement, state, temp), c in groups.items():
            c["outcome"] = "server_died"
            c["reason"] = f"server died during {statement}/{state} try 0, signal 9"
            for k in ("rows", "columns", "values", "values_sha256", "stats"):
                c.pop(k, None)
        self.save_calls()
        _, res, _ = self.go(evaluate_py=evaluate_py)
        out = []
        if res["verdict"] != "fail":
            out.append(f"verdict {res['verdict']}")
        for statement, state, temp in groups:
            want = (f"server_died {statement}/{state}/{temp} try 0: "
                    f"server died during {statement}/{state} try 0, signal 9")
            if want not in res["problems"]:
                out.append(f"no problem for {statement}/{state}/{temp}")
        if ("spans_in_10", "unfolded", "cold") not in groups:
            out.append("the fixture has no spans_in_10/unfolded cold call (box B's death)")
        return out

    def test_server_died_everywhere(self):
        self.assertEqual(self.check_server_died_everywhere(), [])

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
        self.assertEqual(s["server_cpu_s_cold_sum"], 5.4)
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

    def check_amortise_months_required(self, evaluate_py=EVALUATE):
        """Item 5: run.toml without cost.amortise_months is a problem, never a one-month default."""
        toml = os.path.join(self.run_dir, "run.toml")
        with open(toml) as f:
            text = f.read()
        with open(toml, "w") as f:
            f.write(re.sub(r"(?m)^amortise_months = .*\n", "", text))
        code, res, summary = self.go(evaluate_py=evaluate_py)
        out = []
        want = "missing run.toml cost.amortise_months (a whole number of months, at least 1)"
        if want not in res["problems"]:
            out.append(f"no '{want}' in {res['problems']}")
        if res["cost"]["amortise_months"] is not None:
            out.append(f"results amortise_months {res['cost']['amortise_months']}")
        got = [m["status"] for m in res["cost"]["maintenance"]]
        if got != ["missing", "missing"]:
            out.append(f"maintenance statuses {got}")
        if "leaf build amortised over missing (run.toml cost.amortise_months absent)" not in summary:
            out.append("summary does not say amortise_months is missing")
        return out

    def test_amortise_months_required(self):
        self.assertEqual(self.check_amortise_months_required(), [])

    def check_storage_bytes_missing(self, evaluate_py=EVALUATE):
        """Item 5: a census line without bytes is a missing storage cost, never $0."""
        path = self.stamp_path("census.jsonl")
        lines = read_jsonl(path)
        for line in lines:
            if line["state"] == "unfolded":
                del line["census"]["bytes"]
        with open(path, "w") as f:
            f.writelines(json.dumps(line, sort_keys=True) + "\n" for line in lines)
        code, res, _ = self.go(evaluate_py=evaluate_py)
        out = []
        want = "missing storage cost for unfolded: census line carries no census.bytes"
        if want not in res["problems"]:
            out.append(f"no '{want}' in {res['problems']}")
        got = next(s for s in res["cost"]["storage"] if s["state"] == "unfolded")
        if (got.get("status"), got["bytes"], got["dollars_per_month"]) != ("missing", None, None):
            out.append(f"unfolded storage {got}")
        return out

    def test_storage_bytes_missing(self):
        self.assertEqual(self.check_storage_bytes_missing(), [])

    def check_row_6_floor(self, evaluate_py=EVALUATE):
        """Item 4: row 6's keyIndex GETs below the census's objects over 512 KiB read miss, with the reason."""
        mod = load_module(evaluate_py, "evaluate_row6")
        with open(BANDS, "rb") as f:
            bands = {"row": [r for r in tomllib.load(f)["row"] if r["key"] == "row_06"]}
        census = {("unfolded",): {"state": "unfolded", "census": {"tenants": {
            "hits": {"data": {"logs": {"objects": 398, "objects_over_512k": 3}}}}}}}
        cfg = {"lane": "local", "hits": True, "tenant": "inv", "scales": [1], "baseline": False,
               "states": ["unfolded"]}
        out = []
        for gets, want in ((2, "miss"), (3, "pass"), (796, "pass"), (797, "miss")):
            def fixed(calls, cfg, statement, state, metric, candidates=None, gets=gets):
                return ("value", gets, "") if metric == "cold.max.keyindex_gets" else ("missing", None, "not set")
            mod.statement_figure = fixed
            figs = mod.run_rows(bands, cfg, {"unfolded": None}, None, None, {}, census, None, {}, {})
            got = next(f for f in figs if f["id"] == "row_06.keyindex_gets")
            if got["verdict"] != want:
                out.append(f"{gets} keyIndex GETs read {got['verdict']}, want {want}")
            if gets == 2 and got["reason"] != ("fewer keyIndex GETs than objects above 512 KiB: one was read "
                                               "without its probe"):
                out.append(f"2 keyIndex GETs reason {got['reason']!r}")
        return out

    def test_row_6_floor(self):
        self.assertEqual(self.check_row_6_floor(), [])

    @staticmethod
    def box_c_q20_call():
        """One cold q20 call on unfolded with box C's per-phase stats, as its calls.jsonl records try 0 (and
        try 1 the same): run investigations-20261106T154511Z-47463-23195, build b54ecc52a. The plan phase
        opened 493 objects with 493 GETs (the KEY_IDX probe's footer reads), the key index pruned 492 of them,
        and the scan phase opened 4 with 6 GETs and 989,537 B; 497 over every phase."""
        def phase(name, gets=0, wire=0, opened=0, cached=0):
            return {"phase": name, "s3GetRequests": gets, "s3GetWireBytes": wire, "s3ListRequests": 0,
                    "segmentsOpened": opened, "decompressedOutputBytes": 0, "cacheServedBytes": cached}
        return {"statement": "q20_hits", "state": "unfolded", "temp": "cold", "try": 0, "outcome": "ok",
                "truth_key": "hits.q20", "rows": 4, "wall_s": 3.0, "server_cpu_s": 1.0, "server_hwm_bytes": 1,
                "stats": {"phases": [phase("resolve", 1, 33, 0, 1_287_727), phase("plan", 493, 121_939_878, 493),
                                     phase("probe"), phase("scan", 6, 989_537, 4, 262_144),
                                     phase("keyIndex", 970, 1_322_842)],
                          "pruning": {"blocksScanned": 5, "segments": 523, "segmentsPrunedByKeyIndex": 492,
                                      "segmentsPrunedByStats": 26},
                          "io": {"dependencyDepth": 1}, "signals": [{}]}}

    def check_row_6_objects_past_probe(self, evaluate_py=EVALUATE, bands=BANDS):
        """Item 4: row 6's objects past the probe are the scan phase's segmentsOpened: 4 on box C's call,
        where segmentsOpened summed over the phases reads 497. 4 still misses the band's holders (1), which
        this does not change: the scan opened 4 objects, and the census holds 4 at or under 512 KiB, which
        the keyed plan reads without a probe."""
        mod = load_module(evaluate_py, "evaluate_row6_past_probe")
        with open(bands, "rb") as f:
            rows = {"row": [r for r in tomllib.load(f)["row"] if r["key"] == "row_06"]}
        c = self.box_c_q20_call()
        problems = mod.Problems()
        calls = mod.Calls([c], problems, {"q20_hits": {"id": "q20_hits", "tenant": "hits"}})
        c["_judged"] = "reference"
        census = {("unfolded",): {"state": "unfolded", "census": {"tenants": {
            "hits": {"data": {"logs": {"objects": 523, "objects_over_512k": 519}}}}}}}
        cfg = {"lane": "local", "hits": True, "tenant": "inv", "scales": [1], "baseline": False,
               "states": ["unfolded"], "cold_runs": 1, "warm_runs": 0}
        figs = {f["id"]: f for f in mod.run_rows(rows, cfg, {"unfolded": None}, calls, None, {}, census, None,
                                                   {}, {})}
        out = [] if not problems.lines else [f"problems {problems.lines}"]
        got = figs["row_06.objects_past_probe"]
        if (got["value"], got["verdict"]) != (4, "miss"):
            out.append(f"objects_past_probe {got['value']} {got['verdict']}")
        for name, value in (("gets_beyond_probe", 6), ("scan_wire_bytes", 989_537)):
            if (figs[f"row_06.{name}"]["value"], figs[f"row_06.{name}"]["verdict"]) != (value, "pass"):
                out.append(f"{name} {figs[f'row_06.{name}']}")
        if mod.call_metrics(c)["segments_opened"] != 497:
            out.append(f"segmentsOpened summed {mod.call_metrics(c)['segments_opened']}")
        return out

    def test_row_6_objects_past_probe(self):
        self.assertEqual(self.check_row_6_objects_past_probe(), [])

    def needle_stamp(self, **fields):
        path = self.stamp_path("needle_blocks.json")
        with open(path) as f:
            doc = json.load(f)
        for k, v in fields.items():
            if k.startswith("truth_"):
                doc["truth_needle_blocks"][k[len("truth_"):]] = v
            else:
                doc[k] = v
        with open(path, "w") as f:
            json.dump(doc, f)

    NEEDLE_IDS = [f"needle_blocks.{v}.{k}" for v in ("needlerare", "needlesparse", "needlecommon", "any")
                  for k in ("planted", "storage")]

    def check_needle_geometry(self, evaluate_py=EVALUATE):
        """Item 7: planted and storage blocks per needle, each figure once; sparse and common inside 25 %."""
        self.needle_stamp(needle_sparse_present_blocks=2, truth_needlecommon=4, needle_common_present_blocks=5)
        code, res, summary = self.go(evaluate_py=evaluate_py)
        nb = res["needle_blocks"]
        out = []
        ids = [f["id"] for f in nb["figures"]]
        for i in self.NEEDLE_IDS:
            if ids.count(i) != 1:
                out.append(f"{i} present {ids.count(i)} time(s)")
        if sorted(ids) != sorted(self.NEEDLE_IDS):
            out.append(f"figure ids {ids}")
        got = {v: (e["planted"], e["storage"], e["status"]) for v, e in nb["needles"].items()}
        want = {"needlerare": (1, 1, "match"), "needlesparse": (2, 2, "within"), "needlecommon": (4, 5, "within"),
                "any": (5, 4, "reported")}
        if got != want:
            out.append(f"needles {got}")
        if [p for p in res["problems"] if "needle" in p]:
            out.append(f"needle problems {res['problems']}")
        if "  needlecommon   needle_common      4 / 5     within 25%  within" not in summary:
            out.append("summary lacks the needlecommon line")
        return out

    def test_needle_geometry(self):
        self.assertEqual(self.check_needle_geometry(), [])

    def check_needle_partial(self, evaluate_py=EVALUATE):
        """Item 3 (round 5): a partial needle inspection leaves the exact and
        banded needles unjudged, and says so under incomplete with the counts
        inspected."""
        self.needle_stamp(objects_inspected=1, objects_total=4)
        code, res, _ = self.go(evaluate_py=evaluate_py)
        out = [] if (code, res["verdict"], res["problems"]) == (1, "incomplete", []) else [
            f"exit {code} {res['verdict']} {res['problems']}"]
        got = {v: e["status"] for v, e in res["needle_blocks"]["needles"].items()}
        if got != {"needlerare": "incomplete", "needlesparse": "incomplete", "needlecommon": "incomplete",
                   "any": "reported"}:
            out.append(f"needles {got}")
        want = ("needle_blocks needlerare: needle_rare counted over 1 of 4 logs objects (run.toml "
                "needle_blocks_max_objects), 1 storage blocks against 1 planted, not judged")
        if want not in res["incomplete"]:
            out.append(f"incomplete {res['incomplete']}")
        return out

    def test_needle_partial(self):
        self.assertEqual(self.check_needle_partial(), [])

    def check_needle_band_miss(self, evaluate_py=EVALUATE):
        """Item 7: a common needle in 6 storage blocks against 4 planted is outside 25 %, a miss."""
        self.needle_stamp(needle_sparse_present_blocks=2, truth_needlecommon=4, needle_common_present_blocks=6)
        code, res, _ = self.go(evaluate_py=evaluate_py)
        want = "miss needle_blocks needlecommon: needle_common in 6 storage blocks, 4 planted, outside 25%"
        out = [] if want in res["problems"] else [f"no '{want}' in {res['problems']}"]
        if res["needle_blocks"]["needles"]["needlecommon"]["status"] != "miss":
            out.append(f"status {res['needle_blocks']['needles']['needlecommon']}")
        return out

    def test_needle_band_miss(self):
        self.assertEqual(self.check_needle_band_miss(), [])

    def check_needle_storage_null(self, evaluate_py=EVALUATE):
        """Item 5: a null storage block count (inv.py could not sum it) is missing, never 0."""
        self.needle_stamp(needle_sparse_present_blocks=None, needle_common_present_blocks=3)
        code, res, _ = self.go(evaluate_py=evaluate_py)
        want = ("missing needle_blocks needlesparse: stamps/needle_blocks.json needle_sparse_present_blocks "
                "absent or null")
        out = [] if want in res["problems"] else [f"no '{want}' in {res['problems']}"]
        got = res["needle_blocks"]["needles"]["needlesparse"]
        if (got["storage"], got["status"]) != (None, "missing"):
            out.append(f"needlesparse {got}")
        return out

    def test_needle_storage_null(self):
        self.assertEqual(self.check_needle_storage_null(), [])

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

    def check_dirty_paths_null(self, evaluate_py=EVALUATE):
        """Round 5 item 5: a local build stamp whose git status failed (dirty_paths null) is missing."""
        path = self.stamp_path("build.json")
        with open(path) as f:
            doc = json.load(f)
        doc["lane"], doc["dirty_paths"] = "local", None
        with open(path, "w") as f:
            json.dump(doc, f)
        code, res, _ = self.go(evaluate_py=evaluate_py)
        out = []
        if code != 1:
            out.append(f"exit {code}")
        want = "missing stamps/build.json dirty_paths: no count of the harness's uncommitted paths"
        if want not in res["problems"]:
            out.append(f"problems {res['problems']}")
        return out

    def test_dirty_paths_null(self):
        self.assertEqual(self.check_dirty_paths_null(), [])

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

    def write_ranges(self, inside=4, outside=2):
        """stamps/unfolded.spans.ranges.json: inside objects overlapping every saved-trace window the cold
        tries read, outside objects two hours before them."""
        with open(os.path.join(self.run_dir, "truth.json")) as f:
            saved = json.load(f)["saved_traces"]
        starts = [saved[c["try"] % len(saved)]["start_ns"] for c in self.select("spans_lookup_1h", "unfolded")]
        lo, hi = min(starts), max(starts)
        objects = [{"key": f"in{i}.rseg", "bytes": 1, "min_ns": lo - 60 * 10**9, "max_ns": hi + 60 * 10**9}
                   for i in range(inside)]
        objects += [{"key": f"out{i}.rseg", "bytes": 1, "min_ns": lo - 3 * HOUR_NS, "max_ns": lo - 2 * HOUR_NS}
                    for i in range(outside)]
        with open(self.stamp_path("unfolded.spans.ranges.json"), "w") as f:
            json.dump({"state": "unfolded", "label": "spans", "objects": objects}, f)

    def check_probe_per_object(self, evaluate_py=EVALUATE):
        """Row 3's probe band is per candidate object: the unfolded objects whose event range overlaps the
        call's window, from the ranges stamp; not per GET, not the call's own pruning.segments, not every
        object in the state."""
        self.write_ranges(inside=4, outside=2)
        for c in self.select("spans_lookup_1h", "unfolded"):
            c["stats"].pop("perObject", None)
            c["stats"]["pruning"]["segments"] = 9
            for p in c["stats"]["phases"]:
                if p["phase"] == "probe":
                    p["s3GetRequests"], p["s3GetWireBytes"] = 8, 16388
        self.save_calls()
        _, res, _ = self.go(BANDS, evaluate_py)
        f = figure(res, "row_03.probe_wire_per_object")
        return [] if (f["value"], f["verdict"]) == (4097.0, "pass") else [f"{f['value']} {f['verdict']}"]

    def check_probe_per_object_cross_check(self, evaluate_py=EVALUATE):
        """A per-object breakdown whose entry count differs from the stamp's candidate count is a problem."""
        self.write_ranges(inside=4, outside=2)
        for c in self.select("spans_lookup_1h", "unfolded"):
            c["stats"]["perObject"] = [{"probeGets": 1}] * 3
        self.save_calls()
        _, res, _ = self.go(BANDS, evaluate_py)
        f = figure(res, "row_03.probe_wire_per_object")
        want = ("missing row_03.probe_wire_per_object: spans_lookup_1h/unfolded/cold try 0 perObject has 3 "
                "entries, the ranges stamp 4 candidate objects")
        return [] if want in res["problems"] else [f"{f['status']} {f['reason']}"]

    def test_probe_per_object_cross_check(self):
        self.assertEqual(self.check_probe_per_object_cross_check(), [])

    def test_probe_per_object(self):
        self.assertEqual(self.check_probe_per_object(), [])

    # Box D's fold_advance (#3692 comment 6700944105, the Stage 1 amendment's
    # row 7): spans 40 new entries over 3 passes, 240 ranged GETs, 1 previous
    # leaf; logs 22 new entries of a 570-entry tail over 27 passes, 1,188
    # ranged GETs (594 sections, 22 a pass), 6 previous leaves (5 request_id
    # slices and 1 trace_id); no entry uncovered.
    BOX_D_ROW_7 = {
        "spans": dict(mixed=800, tail=840, leaves=1, ranged=240, sections=120, passes=3, leaf_gets=1, uncovered=0),
        "logs": dict(mixed=548, tail=570, leaves=6, ranged=1188, sections=594, passes=27, leaf_gets=6, uncovered=0),
    }

    def set_row_7(self, figures):
        """Write {signal: dict(mixed, tail, leaves, ranged, sections, passes, leaf_gets, uncovered)} into the
        mixed and fold_advance catalog stamps and the fold_advance fold reports."""
        for sig, f in figures.items():
            for state, entries in (("mixed", f["mixed"]), ("fold_advance", f["tail"])):
                path = self.stamp_path(f"{state}.{sig}.catalog.json")
                with open(path) as fh:
                    doc = json.load(fh)
                doc["entries"] = entries
                if state == "mixed":
                    leaf = doc["parts"][0]["leaves"][0]
                    doc["parts"] = doc["parts"][:1]
                    doc["parts"][0]["leaves"] = [dict(leaf) for _ in range(f["leaves"])]
                with open(path, "w") as fh:
                    json.dump(doc, fh)
        folds = read_jsonl(self.stamp_path("folds.jsonl"))
        for line in folds:
            f = figures.get(line["signal"]) if line["transition"] == "fold_advance" else None
            if f:
                line["report"].update(key_index_ranged_gets=f["ranged"], key_index_sections_read=f["sections"],
                                      key_index_build_passes=f["passes"], key_index_leaf_gets=f["leaf_gets"],
                                      key_index_uncovered_entries=f["uncovered"])
        write_jsonl(self.stamp_path("folds.jsonl"), folds)

    def check_row_7_box_d(self, evaluate_py=EVALUATE, bands=BANDS):
        """Item 3: box D's advance holds on section GETs per pass, section reads per pass and leaf GETs, where
        a total against 2 x tail entries (1,188 against 1,140) called it a rebuild."""
        self.set_row_7(self.BOX_D_ROW_7)
        _, res, _ = self.go(bands, evaluate_py)
        out = []
        for sig, f in self.BOX_D_ROW_7.items():
            for name, value in (("section_gets", f["ranged"]), ("sections_per_pass", f["sections"] / f["passes"]),
                                ("leaf_gets", f["leaf_gets"]), ("new_entries", f["tail"] - f["mixed"])):
                got = figure(res, f"row_07.{sig}_{name}")
                if (got["value"], got["verdict"]) != (value, "pass"):
                    out.append(f"{sig} {name} {got['value']} {got['verdict']} {got['reason']}")
        return out

    def test_row_7_box_d(self):
        self.assertEqual(self.check_row_7_box_d(), [])
        # The arithmetic the case rests on, and the total the old miss line
        # compared: 2 x 22 x 27 = 1,188 >= 2 x 570 = 1,140.
        logs = self.BOX_D_ROW_7["logs"]
        self.assertEqual(logs["ranged"], 2 * (logs["tail"] - logs["mixed"]) * logs["passes"])
        self.assertEqual(logs["sections"], (logs["tail"] - logs["mixed"]) * logs["passes"])
        self.assertGreaterEqual(logs["ranged"], 2 * logs["tail"])
        self.assertEqual(2 * logs["tail"], 1140)

    def check_row_7_rebuild(self, evaluate_py=EVALUATE, bands=BANDS):
        """Item 3: a from-scratch rebuild, every tail entry's sections read in one pass (2 x 570 = 1,140 ranged
        GETs, 570 sections), still misses on both signals, with the rebuild reasons."""
        rebuild = {sig: dict(f, ranged=2 * f["tail"], sections=f["tail"], passes=1)
                   for sig, f in self.BOX_D_ROW_7.items()}
        self.set_row_7(rebuild)
        _, res, _ = self.go(bands, evaluate_py)
        out = []
        for sig in rebuild:
            for name, reason in (("section_gets", "from-scratch rebuild: section GETs = 2 x entry_count per pass"),
                                 ("sections_per_pass", "from-scratch rebuild: every tail entry's section read on "
                                                       "a pass")):
                want = f"miss row_07.{sig}_{name}: {reason}"
                if want not in res["problems"]:
                    out.append(f"no '{want}'")
        return out

    def test_row_7_rebuild(self):
        self.assertEqual(self.check_row_7_rebuild(), [])

    def check_row_7_extra_leaf_gets(self, evaluate_py=EVALUATE, bands=BANDS):
        """Item 3: an advance that reads the previous leaves more than once each misses on leaf GETs."""
        self.set_row_7({"logs": dict(self.BOX_D_ROW_7["logs"], leaf_gets=12)})
        _, res, _ = self.go(bands, evaluate_py)
        f = figure(res, "row_07.logs_leaf_gets")
        return [] if (f["value"], f["verdict"]) == (12, "miss") else [f"{f['value']} {f['verdict']}"]

    def test_row_7_extra_leaf_gets(self):
        self.assertEqual(self.check_row_7_extra_leaf_gets(), [])

    def check_miss_reason(self, evaluate_py=EVALUATE):
        """Item 3: row 7's miss carries the from-scratch reason bands.toml gives it."""
        self.set_row_7({"spans": dict(self.BOX_D_ROW_7["spans"], ranged=2 * 840, sections=840, passes=1)})
        _, res, _ = self.go(BANDS, evaluate_py)
        want = "miss row_07.spans_section_gets: from-scratch rebuild: section GETs = 2 x entry_count per pass"
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
        self.assertEqual(figure(res, "row_13.errors")["value"], 0)

    def check_typed_refusal_is_an_error_not_a_wrong_answer(self, evaluate_py=EVALUATE):
        """Box C (#3918): eight HTTP 422 budget refusals read as eight wrong answers with no row differing
        from the oracle. A refusal still fails the run, as an error, and leaves wrong_answers alone."""
        c = next(c for c in self.select("logs_tail_request_id", "mixed") if c["try"] == 0)
        c["outcome"], c["http_status"] = "error", 422
        c["error"] = "query memory budget exhausted"
        self.save_calls()
        code, res, _ = self.go(BANDS, evaluate_py)
        out = [] if code == 1 else [f"exit {code}"]
        if "error logs_tail_request_id/mixed/cold try 0: outcome 'error' HTTP 422" not in res["problems"]:
            out.append(f"problems {res['problems']}")
        f = figure(res, "row_13.wrong_answers")
        if (f["value"], f["verdict"]) != (0, "pass"):
            out.append(f"wrong_answers {f['value']} {f['verdict']}")
        f = figure(res, "row_13.errors")
        if (f["value"], f["verdict"]) != (1, "miss"):
            out.append(f"errors {f['value']} {f['verdict']}")
        return out

    def test_typed_refusal_is_an_error_not_a_wrong_answer(self):
        self.assertEqual(self.check_typed_refusal_is_an_error_not_a_wrong_answer(), [])

    # --- row 18, key-set lookup time ------------------------------------------

    ROW_18_CELLS = {"logs_request_in_10": "logs_request_id_10", "logs_request_in_100": "logs_request_id_100",
                    "logs_trace_in_10": "logs_trace_id_10", "logs_trace_in_100": "logs_trace_id_100",
                    "spans_in_10": "spans_trace_id_10", "spans_in_100": "spans_trace_id_100"}
    COVERED = {"partsCovered": 1, "partsUncovered": 0, "skippedByBudget": 0, "partsBeyondCeiling": 0,
               "leavesRejected": 0, "reservationsRefused": 0, "leafGetRequests": 11, "keysProbed": 10}

    def set_advanced_leaves(self, cells=None):
        """An s3-lane run with advanced_leaves listed after mixed and run: its six statements' cold calls copied
        from folded_leaves, its stamps from folded_leaves's (logs request_id at 3 slices a part against the
        rebuild's 1). cells: {statement: (unfolded walls, advanced walls, keyIndex)}; a statement not named
        reads unfolded (2.8, 3.2), so U 3.0 and IQR 0.2, and advanced (3.1, 3.1), covered: a hold inside the
        noise, 3.15."""
        cells = cells or {}
        self.to_s3_lane()
        toml = os.path.join(self.run_dir, "run.toml")
        with open(toml) as f:
            text = f.read()
        with open(toml, "w") as f:
            f.write(text.replace('"mixed", "compaction_in_flight"', '"mixed", "advanced_leaves", "compaction_in_flight"'))
        restarts = read_jsonl(self.stamp_path("restarts.jsonl"))
        for sid in self.ROW_18_CELLS:
            unfolded, advanced, ki = cells.get(sid, ((2.8, 3.2), (3.1, 3.1), self.COVERED))
            for c in self.calls:
                if (c["statement"], c["state"], c.get("temp")) == (sid, "unfolded", "cold"):
                    c["wall_s"] = unfolded[c["try"]]
            for c in self.select(sid):
                new = copy.deepcopy(c)
                new.update(state="advanced_leaves", wall_s=advanced[c["try"]])
                k = ki[c["try"]] if isinstance(ki, list) else ki
                new["stats"]["keyIndex"] = dict(k)
                self.calls.append(new)
                restarts.append(dict(next(r for r in restarts if (r["statement"], r["state"], r["try"]) ==
                                          (sid, "folded_leaves", c["try"])), state="advanced_leaves"))
        self.save_calls()
        write_jsonl(self.stamp_path("restarts.jsonl"), restarts)
        for name in ("census.jsonl", "transitions.jsonl"):
            lines = read_jsonl(self.stamp_path(name))
            lines.append(dict(next(x for x in lines if x["state"] == "folded_leaves"), state="advanced_leaves"))
            write_jsonl(self.stamp_path(name), lines)
        for label in ("spans", "logs"):
            with open(self.stamp_path(f"folded_leaves.{label}.catalog.json")) as f:
                doc = json.load(f)
            if label == "logs":
                leaf = next(lf for lf in doc["parts"][0]["leaves"] if lf["field"] == "request_id")
                doc["parts"][0]["leaves"] += [dict(leaf), dict(leaf)]
            with open(self.stamp_path(f"advanced_leaves.{label}.catalog.json"), "w") as f:
                json.dump(dict(doc, state="advanced_leaves"), f)
        shutil.copy(self.stamp_path("folded_leaves.server.json"), self.stamp_path("advanced_leaves.server.json"))

    def row_18(self, res):
        return {f["id"][len("row_18."):]: f for f in res["figures"] if f["id"].startswith("row_18.")}

    def check_row_18_hold(self, evaluate_py=EVALUATE, bands=BANDS):
        """Item 5: every cell 0.1 s over its unfolded p50 of 3.0 s, inside the noise (5 % of 3.0 = 0.15 s
        beats half the 0.2 s range): each holds, and the slices are reported beside the row."""
        self.set_advanced_leaves()
        _, res, _ = self.go(bands, evaluate_py)
        got = self.row_18(res)
        out = [p for p in res["problems"] if "row_18" in p]
        for cell in self.ROW_18_CELLS.values():
            for name, value, verdict in ((f"{cell}_p50_s", 3.1, "pass"), (f"{cell}_unfolded_p50_s", 3.0, "pass"),
                                         (f"{cell}_unfolded_iqr_s", 0.2, "pass")):
                if (got[name]["value"], got[name]["verdict"]) != (value, verdict):
                    out.append(f"{name} {got[name]['value']} {got[name]['verdict']} {got[name]['reason']}")
        for name, value in (("logs_request_id_slices", 3), ("logs_request_id_slices_rebuild", 1),
                            ("logs_trace_id_slices", 1), ("spans_trace_id_slices", 1)):
            if (got[name]["value"], got[name]["verdict"]) != (value, "pass"):
                out.append(f"{name} {got[name]['value']} {got[name]['verdict']}")
        if any("row_18" in r for r in res["incomplete"]):
            out.append(f"incomplete {res['incomplete']}")
        return out

    def test_row_18_hold(self):
        self.assertEqual(self.check_row_18_hold(), [])

    def check_row_18_box_c_miss(self, evaluate_py=EVALUATE, bands=BANDS):
        """Item 5: box C's logs request_id at 100 keys, 7.64 s through the leaves against an unfolded p50 of
        3.01 s (0.18 s of interquartile range here), misses."""
        self.set_advanced_leaves({"logs_request_in_100": ((2.83, 3.19), (7.64, 7.64), self.COVERED)})
        _, res, _ = self.go(bands, evaluate_py)
        got = self.row_18(res)["logs_request_id_100_p50_s"]
        out = [] if (got["value"], got["verdict"]) == (7.64, "miss") else [f"{got['value']} {got['verdict']}"]
        if "miss row_18.logs_request_id_100_p50_s" not in res["problems"]:
            out.append(f"problems {res['problems']}")
        if self.row_18(res)["logs_request_id_100_unfolded_p50_s"]["value"] != 3.01:
            out.append(f"unfolded p50 {self.row_18(res)['logs_request_id_100_unfolded_p50_s']['value']}")
        return out

    def test_row_18_box_c_miss(self):
        self.assertEqual(self.check_row_18_box_c_miss(), [])

    # Each noise term on both sides of its edge, one cell each. Two cold calls
    # a and b make U = (a + b) / 2 and IQR = (b - a) / 2. (2.8, 3.2): U 3.0,
    # IQR 0.2, 5 % = 0.15 beats 0.1, so the bound is 3.15. (2.5, 3.5): U 3.0,
    # IQR 0.5, half = 0.25 beats 0.15, so the bound is 3.25.
    NOISE_EDGES = {
        "logs_request_in_10": ((2.8, 3.2), 3.1499, "pass"),
        "logs_request_in_100": ((2.8, 3.2), 3.1501, "miss"),
        "logs_trace_in_10": ((2.5, 3.5), 3.2499, "pass"),
        "logs_trace_in_100": ((2.5, 3.5), 3.2501, "miss"),
    }

    def check_row_18_noise_edges(self, evaluate_py=EVALUATE, bands=BANDS):
        """Item 5: the bound is U + max(5 % of U, half the IQR): inside and outside each term's edge."""
        self.set_advanced_leaves({sid: (u, (v, v), self.COVERED) for sid, (u, v, _) in self.NOISE_EDGES.items()})
        _, res, _ = self.go(bands, evaluate_py)
        got = self.row_18(res)
        out = []
        for sid, (_, v, verdict) in self.NOISE_EDGES.items():
            f = got[f"{self.ROW_18_CELLS[sid]}_p50_s"]
            if (f["value"], f["verdict"]) != (v, verdict):
                out.append(f"{sid} {f['value']} {f['verdict']}, want {verdict}")
        return out

    def test_row_18_noise_edges(self):
        self.assertEqual(self.check_row_18_noise_edges(), [])

    def check_row_18_uncovered(self, evaluate_py=EVALUATE, bands=BANDS):
        """Item 5: a cell whose part went Uncovered on one cold call (skippedByBudget 1, as box C's 100-key
        trace_id lists) is not measured, even at 1.0 s, under its 3.15 s bound, and the run is incomplete."""
        skipped = dict(self.COVERED, partsCovered=0, skippedByBudget=1, leafGetRequests=1)
        self.set_advanced_leaves({"logs_trace_in_100": ((2.8, 3.2), (1.0, 1.0), [self.COVERED, skipped])})
        _, res, _ = self.go(bands, evaluate_py)
        f = self.row_18(res)["logs_trace_id_100_p50_s"]
        out = []
        if (f["status"], f["value"], f["verdict"]) != ("not-measured", None, "not-measured"):
            out.append(f"{f['status']} {f['value']} {f['verdict']}")
        if "cold try 1: a covering part went Uncovered (skippedByBudget 1)" not in (f["reason"] or ""):
            out.append(f"reason {f['reason']}")
        if not any(r.startswith("figure row_18.logs_trace_id_100_p50_s not measured: ") for r in res["incomplete"]):
            out.append(f"incomplete {res['incomplete']}")
        if any("row_18" in p for p in res["problems"]):
            out.append(f"problems {res['problems']}")
        return out

    def test_row_18_uncovered(self):
        self.assertEqual(self.check_row_18_uncovered(), [])

    def test_row_18_not_run_without_its_state(self):
        # The committed fixture does not list advanced_leaves: the row is
        # not-run and listed as not requested.
        _, res, _ = self.go(BANDS)
        for f in self.row_18(res).values():
            self.assertEqual(f["status"], "not-run", f["id"])
            self.assertIn("advanced_leaves: not in run.toml states", f["reason"])
        self.assertIn({"row": 18, "key": "row_18", "reason": "run.toml states lacks ['advanced_leaves']"},
                      res["not_requested"])
        # Listed, then failed by its own precondition: not-run with the step,
        # and the run incomplete.
        self.set_advanced_leaves()
        with open(self.stamp_path("failed_states.jsonl"), "w") as f:
            f.write(json.dumps({"state": "advanced_leaves", "step": "advance-check advanced_leaves", "exit": 1,
                                "log": "out/logs/advance-check.out"}) + "\n")
        _, res, _ = self.go(BANDS)
        why = "state failed at advance-check advanced_leaves (exit 1)"
        for f in self.row_18(res).values():
            if f["state"] == "folded_leaves":
                continue
            self.assertEqual(f["status"], "not-run", f["id"])
            self.assertIn(why, f["reason"], f["id"])
        self.assertIn(f"state advanced_leaves not run: {why}", res["incomplete"])

    # --- in-flight overlap, refusals, the 1,025-key control -----------------

    def statement_tables(self):
        with open(STATEMENTS, "rb") as f:
            return {s["id"]: s.get("table", "spans") for s in tomllib.load(f)["statement"]}

    def inflight_calls(self, sig):
        table = self.statement_tables()
        return [c for c in self.calls if c["state"] == "compaction_in_flight" and c.get("temp") == "inflight"
                and c["outcome"] == "ok" and table[c["statement"]] == sig]

    def set_compaction(self, windows):
        """windows: {signal: f(calls) -> (started, ended)} over that signal's in-flight calls."""
        path = self.stamp_path("compaction.jsonl")
        lines = read_jsonl(path)
        for line in lines:
            window = windows.get(line["signal"])
            if window:
                line["measure"]["started_unix_s"], line["measure"]["ended_unix_s"] = window(
                    self.inflight_calls(line["signal"]))
        write_jsonl(path, lines)

    @staticmethod
    def covering(calls):
        return min(c["started_unix_s"] for c in calls) - 1, max(c["started_unix_s"] + c["wall_s"] for c in calls) + 1

    @staticmethod
    def after(calls, seconds):
        end = max(c["started_unix_s"] + c["wall_s"] for c in calls) + 1
        return end, end + seconds

    def check_overlap_full(self, evaluate_py=EVALUATE):
        """Every in-flight try inside its compaction's window: no not_in_flight, the state's figure passes."""
        self.set_compaction({"spans": self.covering, "logs": self.covering})
        _, res, _ = self.go(BANDS, evaluate_py)
        out = [p for p in res["problems"] if p.startswith("not_in_flight")]
        f = figure(res, "row_13.compaction_in_flight_rows_exact")
        if (f["status"], f["value"], f["verdict"]) != ("value", 1, "pass"):
            out.append(f"figure {f['status']} {f['value']} {f['verdict']}")
        return out

    def test_overlap_full(self):
        self.assertEqual(self.check_overlap_full(), [])

    def check_overlap_partial(self, evaluate_py=EVALUATE):
        """A logs compaction that covers only the first logs try: that group is not_in_flight."""
        calls = sorted(self.inflight_calls("logs"), key=lambda c: c["started_unix_s"])
        first, second = calls[0], calls[1]
        group = [c for c in calls if c["statement"] == first["statement"]]
        self.set_compaction({"spans": self.covering,
                             "logs": lambda _: (first["started_unix_s"] - 10,
                                                (first["started_unix_s"] + second["started_unix_s"]) / 2)})
        _, res, _ = self.go(BANDS, evaluate_py)
        want = (f"not_in_flight {first['statement']}/compaction_in_flight: 1 of {len(group)} inflight tries "
                f"overlapped the logs compaction")
        return [] if len(group) > 1 and want in res["problems"] else [f"problems {res['problems']}"]

    def test_overlap_partial(self):
        self.assertEqual(self.check_overlap_partial(), [])

    def check_overlap_zero(self, evaluate_py=EVALUATE):
        """A long logs compaction that started after every logs try ended: not_in_flight, not not-run."""
        calls = self.inflight_calls("logs")
        self.set_compaction({"spans": self.covering, "logs": lambda c: self.after(c, 100)})
        _, res, _ = self.go(BANDS, evaluate_py)
        out = []
        for statement in sorted({c["statement"] for c in calls}):
            n = sum(1 for c in calls if c["statement"] == statement)
            want = (f"not_in_flight {statement}/compaction_in_flight: 0 of {n} inflight tries "
                    f"overlapped the logs compaction")
            if want not in res["problems"]:
                out.append(f"no '{want}'")
        return out

    def test_overlap_zero(self):
        self.assertEqual(self.check_overlap_zero(), [])

    def check_overlap_too_short(self, evaluate_py=EVALUATE):
        """Compactions shorter than one call that cover no try: not-run, so incomplete, never a problem."""
        shortest = min(c["wall_s"] for sig in ("spans", "logs") for c in self.inflight_calls(sig))
        self.set_compaction({"spans": lambda c: self.after(c, shortest / 2),
                             "logs": lambda c: self.after(c, shortest / 2)})
        code, res, _ = self.go(BANDS, evaluate_py)
        out = [p for p in res["problems"] if p.startswith("not_in_flight") or "compaction_in_flight" in p]
        f = figure(res, "row_13.compaction_in_flight_rows_exact")
        if f["status"] != "not-run" or "shorter than one call" not in (f["reason"] or ""):
            out.append(f"figure {f['status']} {f['reason']}")
        if not any("compaction_in_flight not run: compaction ran " in r for r in res["incomplete"]):
            out.append(f"incomplete {res['incomplete']}")
        return out

    def test_overlap_too_short(self):
        self.assertEqual(self.check_overlap_too_short(), [])

    def test_compaction_done_before_the_first_call_is_not_run(self):
        calls = {sig: self.inflight_calls(sig) for sig in ("spans", "logs")}
        first = min(c["started_unix_s"] for v in calls.values() for c in v)
        self.set_compaction({"spans": lambda _: (first - 10, first - 9), "logs": lambda _: (first - 10, first - 9)})
        _, res, _ = self.go(BANDS)
        self.assertEqual([p for p in res["problems"] if "compaction_in_flight" in p], [])
        f = figure(res, "row_13.compaction_in_flight_rows_exact")
        self.assertEqual((f["status"], f["reason"]), ("not-run", "every call group in compaction_in_flight not run: "
                                                       "compaction finished in 1000 ms before the first call"))

    def check_refused(self, evaluate_py=EVALUATE):
        """A refusal of a statement not planned as a cap probe is the problem refused; a cap probe's reads
        unsupported."""
        for c in self.select("spans_in_100"):
            for k in ("stats", "rows", "values", "server_cpu_s"):
                c.pop(k, None)
            c.update(outcome=UNSUPPORTED, http_status=400, reason=REFUSAL_1000)
        self.save_calls()
        code, res, _ = self.go(BANDS, evaluate_py)
        want = f"refused spans_in_100/folded_leaves/cold try 0: HTTP 400 {REFUSAL_1000}"
        out = [] if want in res["problems"] else [f"no '{want}'"]
        out += [p for p in res["problems"] if p.startswith("refused spans_in_1000")]
        return out

    def test_refused(self):
        self.assertEqual(self.check_refused(), [])

    def served_1000(self, statement, keys=None, parts=1, scan=True, leaf=None):
        """Turn statement's folded_leaves cold calls into ok calls copied from spans_in_100, which scanned 4
        objects. keys None drops the keyIndex section; leaf sets its leafGetRequests; scan False zeros the
        probe and scan GETs and blocks."""
        src = {c["try"]: c for c in self.select("spans_in_100")}
        for i, c in enumerate(self.calls):
            if c["statement"] == statement and c["state"] == "folded_leaves" and c.get("temp") == "cold":
                new = copy.deepcopy(src[c["try"]])
                new.update(statement=statement, truth_key=c.get("truth_key", new.get("truth_key")))
                if keys is None:
                    new["stats"].pop("keyIndex", None)
                else:
                    new["stats"]["keyIndex"] = dict(new["stats"].get("keyIndex") or {}, keysProbed=keys,
                                                    partsCovered=parts)
                    if leaf is not None:
                        new["stats"]["keyIndex"]["leafGetRequests"] = leaf
                if not scan:
                    for p in new["stats"]["phases"]:
                        if p["phase"] in ("probe", "scan"):
                            p["s3GetRequests"] = 0
                    new["stats"]["pruning"]["blocksScanned"] = 0
                self.calls[i] = new

    CONTROL = {
        # mode: (1,000-key arm, 1,025-key arm, (status, value, verdict), reason fragment). An arm left None
        # keeps the fixture's calls, which the complexity guard of its build refused.
        "guard": (None, None, (GUARD, None, GUARD), "refused by the SQL complexity guard: " + REFUSAL_1000),
        "unsupported": (dict(), None, (UNSUPPORTED, None, UNSUPPORTED), "carries no keyIndex.keysProbed"),
        # What the current build does with 1,025 keys: admitted, no key set, a scan with keysProbed 0,
        # partsCovered 0 and no leaf GET (sql_key_in.rs in_list_rows_equal_the_union_of_equalities).
        "pass": (dict(keys=1000), dict(keys=0, parts=0, leaf=0), ("value", 1, "pass"), ""),
        "over-refused": (dict(keys=1000), None, ("value", 0, "miss"),
                         "was refused (" + REFUSAL_1000 + ") while the 1,000-key list was admitted"),
        "miss": (dict(keys=1000), dict(keys=1025), ("value", 0, "miss"), "was index-served"),
        "leaf-gets": (dict(keys=1000), dict(keys=0, parts=0, leaf=1), ("value", 0, "miss"),
                      "was index-served: keysProbed 0 partsCovered 0 leafGetRequests 1"),
        "served-with-scan": (dict(keys=1000), dict(), ("value", 1, "pass"), ""),
        "served-with-keyindex": (dict(keys=1000), dict(keys=1025, scan=False), ("value", 0, "miss"),
                                 "was index-served: keysProbed 1025 partsCovered 1"),
        "served-without-scan": (dict(keys=1000), dict(scan=False), ("value", 0, "miss"),
                                "served without a visible path"),
        "zeros-without-scan": (dict(keys=1000), dict(keys=0, parts=0, scan=False), ("value", 0, "miss"),
                               "served without a visible path"),
        "zero-parts-covered": (dict(keys=1000, parts=0), dict(keys=0, parts=0), ("value", 0, "miss"),
                               "probed 1000 keys with partsCovered 0"),
    }

    def check_control(self, mode, evaluate_py=EVALUATE):
        arm_1000, arm_1025, want, reason = self.CONTROL[mode]
        if arm_1000 is not None:
            self.served_1000("spans_in_1000", **arm_1000)
        if arm_1025 is not None:
            self.served_1000("spans_in_1025", **arm_1025)
        self.save_calls()
        _, res, _ = self.go(BANDS, evaluate_py)
        f = figure(res, "row_11.spans_control_1025")
        out = [] if (f["status"], f["value"], f["verdict"]) == want else [f"{f['status']} {f['value']} {f['verdict']}"]
        if reason not in (f["reason"] or ""):
            out.append(f"reason {f['reason']}")
        return out

    def check_control_guard(self, evaluate_py=EVALUATE):
        """Row 11 on main: the 1,000-key list refused by the complexity guard reads refused-by-guard."""
        return self.check_control("guard", evaluate_py)

    def check_control_unsupported(self, evaluate_py=EVALUATE):
        """A 1,000-key list served with no tier-2 figures: unsupported."""
        return self.check_control("unsupported", evaluate_py)

    def check_control_pass(self, evaluate_py=EVALUATE):
        """1,000 keys probed and the 1,025-key list scanned with no route: the control passes."""
        return self.check_control("pass", evaluate_py)

    def check_control_over_refused(self, evaluate_py=EVALUATE):
        """A 1,025-key list refused while the 1,000-key list was admitted: the guard admits 1,025
        literals, so a refusal is not the control's answer: miss."""
        return self.check_control("over-refused", evaluate_py)

    def check_control_miss(self, evaluate_py=EVALUATE):
        """A 1,025-key list the index served: the control misses."""
        return self.check_control("miss", evaluate_py)

    def check_control_leaf_gets(self, evaluate_py=EVALUATE):
        """A 1,025-key list with keysProbed and partsCovered 0 but a leaf GET: the index read it: miss."""
        return self.check_control("leaf-gets", evaluate_py)

    def check_control_served_with_scan(self, evaluate_py=EVALUATE):
        """A 1,025-key list served by a scan with no keyIndex figure: the control passes."""
        return self.check_control("served-with-scan", evaluate_py)

    def check_control_served_with_keyindex(self, evaluate_py=EVALUATE):
        """A 1,025-key list served with keyIndex figures and no scan: the control misses."""
        return self.check_control("served-with-keyindex", evaluate_py)

    def check_control_served_without_scan(self, evaluate_py=EVALUATE):
        """Reviewer mutation (a): a 1,025-key list served with no keyIndex figure and zero GETs: miss."""
        return self.check_control("served-without-scan", evaluate_py)

    def check_control_zeros_without_scan(self, evaluate_py=EVALUATE):
        """Reviewer mutation (b): a 1,025-key list whose keyIndex figures are zeros, with no scan: miss."""
        return self.check_control("zeros-without-scan", evaluate_py)

    def check_control_zero_parts_covered(self, evaluate_py=EVALUATE):
        """A 1,000-key list that probed 1,000 keys but covered no part: miss."""
        return self.check_control("zero-parts-covered", evaluate_py)

    def test_control_guard(self):
        self.assertEqual(self.check_control_guard(), [])

    def test_control_unsupported(self):
        self.assertEqual(self.check_control_unsupported(), [])

    def test_control_pass(self):
        self.assertEqual(self.check_control_pass(), [])

    def test_control_miss(self):
        self.assertEqual(self.check_control_miss(), [])

    def test_control_over_refused(self):
        self.assertEqual(self.check_control_over_refused(), [])

    def test_control_leaf_gets(self):
        self.assertEqual(self.check_control_leaf_gets(), [])

    def test_control_served_with_scan(self):
        self.assertEqual(self.check_control_served_with_scan(), [])

    def test_control_served_with_keyindex(self):
        self.assertEqual(self.check_control_served_with_keyindex(), [])

    def test_control_served_without_scan(self):
        self.assertEqual(self.check_control_served_without_scan(), [])

    def test_control_zeros_without_scan(self):
        self.assertEqual(self.check_control_zeros_without_scan(), [])

    def test_control_zero_parts_covered(self):
        self.assertEqual(self.check_control_zero_parts_covered(), [])

    # --- incomplete, truth-derived budgets, absent fields -------------------

    def check_skipped_state(self, evaluate_py=EVALUATE):
        """A state run.toml asks for that the run skipped: incomplete, not pass."""
        self.calls = [c for c in self.calls if c["state"] != "mixed"]
        self.save_calls()
        write_jsonl(self.stamp_path("not_run.jsonl"), [{"state": "mixed", "reason": "case: run deadline"}])
        code, res, _ = self.go(evaluate_py=evaluate_py)
        out = [] if (code, res["verdict"], res["problems"]) == (1, "incomplete", []) else [
            f"exit {code} {res['verdict']} {res['problems']}"]
        if "state mixed not run: case: run deadline" not in res["incomplete"]:
            out.append(f"incomplete {res['incomplete']}")
        return out

    def test_skipped_state(self):
        self.assertEqual(self.check_skipped_state(), [])

    MP24_FAILED = "state failed at load many_parts_24/spans (exit 1)"
    MIXED_FAILED = "state failed at fold fold_advance/logs (exit 101)"

    def fail_states(self):
        """The fixture as run.sh leaves it when many_parts_24's load exits 1
        (no calls, census, transition, folds or stamps of it) and mixed's
        fold_advance logs fold exits 101 after its statements ran."""
        self.calls = [c for c in self.calls if c["state"] != "many_parts_24"]
        self.save_calls()
        stamps = os.path.join(self.run_dir, "stamps")
        for name in os.listdir(stamps):
            if name.startswith(("many_parts_24.", "fold_advance.")):
                os.remove(os.path.join(stamps, name))
        for name in ("census.jsonl", "transitions.jsonl"):
            lines = read_jsonl(self.stamp_path(name))
            write_jsonl(self.stamp_path(name), [x for x in lines if x["state"] not in ("many_parts_24", "fold_advance")])
        folds = [x for x in read_jsonl(self.stamp_path("folds.jsonl"))
                 if not x["transition"].startswith("many_parts_24")]
        for x in folds:
            if (x["transition"], x["signal"]) == ("fold_advance", "logs"):
                x["exit"] = 101
        write_jsonl(self.stamp_path("folds.jsonl"), folds)
        loads = read_jsonl(self.stamp_path("loads.jsonl"))
        for x in loads:
            if x["hour"] == "many_parts_24":
                x["exit"] = 1
        write_jsonl(self.stamp_path("loads.jsonl"), loads)
        write_jsonl(self.stamp_path("failed_states.jsonl"), [
            {"state": "many_parts_24", "step": "load many_parts_24/spans", "exit": 1,
             "log": "out/logs/load-many_parts_24-spans.out", "at": "2026-11-04T00:00:00Z"},
            {"state": "mixed", "step": "fold fold_advance/logs", "exit": 101,
             "log": "out/logs/fold-fold_advance-logs.out", "at": "2026-11-04T00:00:00Z"}])

    def check_failed_state(self, evaluate_py=EVALUATE):
        """Item 1: a state run.sh recorded failed is not-run with "state
        failed at <step> (exit N)" in every figure of it and of its derived
        state, never pass and never missing, and the verdict is incomplete
        naming it."""
        _, base, _ = self.go(BANDS, evaluate_py)
        self.fail_states()
        out = []
        code, res, _ = self.go(evaluate_py=evaluate_py)
        if (code, res["verdict"], res["problems"]) != (1, "incomplete", []):
            out.append(f"exit {code} {res['verdict']} {res['problems']}")
        for s, why in (("many_parts_24", self.MP24_FAILED), ("mixed", self.MIXED_FAILED)):
            if f"state {s} not run: {why}" not in res["incomplete"]:
                out.append(f"incomplete {res['incomplete']}")
        code, res, text = self.go(BANDS, evaluate_py)
        new = sorted(set(res["problems"]) - set(base["problems"]))
        if new:
            out.append(f"new problems {new}")
        for f in res["figures"]:
            if f["id"].startswith("row_09.") and f["id"].endswith("_24"):
                want = f"many_parts_24: {self.MP24_FAILED}"
            elif f["id"].startswith("row_07."):
                want = self.MIXED_FAILED
            else:
                continue
            if f["status"] != "not-run" or f["verdict"] != "not-run" or want not in (f["reason"] or ""):
                out.append(f"{f['id']} {f['status']} {f['verdict']} {f['reason']}")
        states = {x["state"]: x for x in res["states"]}
        mp = states["many_parts_24"]
        if (mp["status"], mp.get("step"), mp.get("exit"), mp.get("log")) != (
                "failed", "load many_parts_24/spans", 1, "out/logs/load-many_parts_24-spans.out"):
            out.append(f"states many_parts_24 {mp}")
        if (states["fold_advance"]["status"], states["fold_advance"].get("reason")) != ("not-run", self.MIXED_FAILED):
            out.append(f"states fold_advance {states['fold_advance']}")
        if f"  many_parts_24            failed  ({self.MP24_FAILED})" not in text:
            out.append("summary carries no failed line for many_parts_24")
        return out

    def test_failed_state(self):
        self.assertEqual(self.check_failed_state(), [])

    FOLD_ADVANCE_FAILED = "state failed at fold fold_advance/spans (exit 3)"

    def fail_fold_advance(self):
        """The fixture as run.sh leaves it when mixed's fold_advance spans
        fold exits 3 after mixed's statements ran: the failure is recorded
        against fold_advance, its spans fold stamp carries exit 3, its logs
        fold never ran, and no fold_advance census, transition or inspect
        stamps exist; mixed's own stamps are untouched."""
        stamps = os.path.join(self.run_dir, "stamps")
        for name in os.listdir(stamps):
            if name.startswith("fold_advance."):
                os.remove(os.path.join(stamps, name))
        for name in ("census.jsonl", "transitions.jsonl"):
            lines = read_jsonl(self.stamp_path(name))
            write_jsonl(self.stamp_path(name), [x for x in lines if x["state"] != "fold_advance"])
        folds = []
        for x in read_jsonl(self.stamp_path("folds.jsonl")):
            if x["transition"] == "fold_advance":
                if x["signal"] == "logs":
                    continue
                x["exit"] = 3
            folds.append(x)
        write_jsonl(self.stamp_path("folds.jsonl"), folds)
        write_jsonl(self.stamp_path("failed_states.jsonl"), [
            {"state": "fold_advance", "step": "fold fold_advance/spans", "exit": 3,
             "log": "out/logs/fold-fold_advance-spans.out", "at": "2026-11-04T00:00:00Z"}])

    def check_failed_fold_advance(self, evaluate_py=EVALUATE):
        """A fold_advance failure recorded against fold_advance keeps mixed
        live: its fold stamps are not wanted (no new problem, so the verdict
        is incomplete, not fail), fold_advance's figures are not-run with the
        reason, mixed's figures stay live, and incomplete names fold_advance
        and not mixed."""
        _, base, _ = self.go(BANDS, evaluate_py)
        self.fail_fold_advance()
        out = []
        code, res, _ = self.go(BANDS, evaluate_py)
        new = sorted(set(res["problems"]) - set(base["problems"]))
        if new:
            out.append(f"new problems {new}")
        if res["verdict"] == "pass":
            out.append(f"verdict pass with a failed state: {res['incomplete']}")
        if not any(self.FOLD_ADVANCE_FAILED in e for e in res["incomplete"]):
            out.append(f"incomplete {res['incomplete']}")
        if any(e.startswith("state mixed not run") for e in res["incomplete"]):
            out.append(f"mixed not run {res['incomplete']}")
        for f in res["figures"]:
            if f["id"] == "row_07.spans_tail_entries":
                if f["status"] != "not-run" or self.FOLD_ADVANCE_FAILED not in (f["reason"] or ""):
                    out.append(f"{f['id']} {f['status']} {f['reason']}")
            elif f["id"] == "row_07.spans_mixed_entries" and f["status"] == "not-run":
                out.append(f"{f['id']} not-run {f['reason']}")
        states = {x["state"]: x for x in res["states"]}
        fa = states["fold_advance"]
        if (fa["status"], fa.get("step"), fa.get("exit")) != ("failed", "fold fold_advance/spans", 3):
            out.append(f"states fold_advance {fa}")
        if states["mixed"]["status"] == "failed":
            out.append(f"states mixed {states['mixed']}")
        return out

    def test_failed_fold_advance(self):
        self.assertEqual(self.check_failed_fold_advance(), [])

    def test_failed_state_line_without_exit_is_missing(self):
        write_jsonl(self.stamp_path("failed_states.jsonl"), [{"state": "mixed", "step": "fold fold_advance/logs"}])
        code, res, _ = self.go()
        self.assertEqual((code, res["verdict"]), (1, "fail"))
        self.assertEqual([p for p in res["problems"] if "failed_states" in p], [
            'missing stamps/failed_states.jsonl line without a state, step or integer exit: '
            '{"state": "mixed", "step": "fold fold_advance/logs"}'])

    def check_deadline_mid_state(self, evaluate_py=EVALUATE):
        """A deadline that stopped a state part way: its unsent calls make the run incomplete."""
        for i, c in enumerate(self.calls):
            if (c["statement"], c["state"], c.get("temp"), c["try"]) == ("spans_in_10", "folded_leaves", "cold", 1):
                self.calls[i] = {k: c[k] for k in ("statement", "state", "temp", "try", "tenant")}
                self.calls[i].update(outcome="not-run", reason="case: run deadline")
        self.save_calls()
        code, res, _ = self.go(evaluate_py=evaluate_py)
        out = [] if (code, res["verdict"], res["problems"]) == (1, "incomplete", []) else [
            f"exit {code} {res['verdict']} {res['problems']}"]
        if "1 cold call(s) of spans_in_10 in folded_leaves not run: case: run deadline" not in res["incomplete"]:
            out.append(f"incomplete {res['incomplete']}")
        return out

    def test_deadline_mid_state(self):
        self.assertEqual(self.check_deadline_mid_state(), [])

    def check_not_requested(self, evaluate_py=EVALUATE):
        """Gated rows run.toml never asked for are listed, each with the setting that leaves it out."""
        _, res, text = self.go(BANDS, evaluate_py)
        got = [(r["row"], r["reason"]) for r in res["not_requested"]]
        want = [(5, "run.toml hits = false"), (6, "run.toml hits = false"),
                (9, "run.toml states lacks ['many_parts_744']"), (10, "run.toml states lacks ['many_parts_2160']"), (12, "run.toml scales lacks [10, 100]"),
                (16, "run.toml hits = false, baseline = false"), (18, "run.toml states lacks ['advanced_leaves']")]
        out = [] if got == want else [f"not_requested {got}"]
        if "not requested: row 5 (run.toml hits = false); row 6 " not in text:
            out.append("summary carries no not-requested line")
        return out

    def test_not_requested(self):
        self.assertEqual(self.check_not_requested(), [])

    @staticmethod
    def gated_placement(res):
        """Each not-run figure not placed exactly once among not_requested
        (its whole row, or the figure by name) and incomplete."""
        rows = {r["key"] for r in res["not_requested"] if not r.get("figures")}
        named = {f"{r['key']}.{n}" for r in res["not_requested"] for n in r.get("figures", [])}
        out = []
        for f in res["figures"]:
            places = (f["id"].split(".", 1)[0] in rows) + (f["id"] in named) + sum(
                line.startswith(f"figure {f['id']} not run: ") for line in res["incomplete"])
            if places != (f["status"] == "not-run"):
                out.append(f"{f['id']} ({f['status']}) placed {places} time(s)")
        return out

    def check_gated_unrequested(self, evaluate_py=EVALUATE):
        """Item 2 (round 5): a gated figure whose state run.toml does not list
        (row 9's many_parts_744 arm) is listed under not_requested by name."""
        _, res, text = self.go(BANDS, evaluate_py)
        out = self.gated_placement(res)
        want = {"row": 9, "key": "row_09", "figures": ["covering_parts", "resolve_gets", "leaf_gets",
                                                       "parts_beyond_ceiling", "rows_exact"],
                "reason": "run.toml states lacks ['many_parts_744']"}
        if want not in res["not_requested"]:
            out.append(f"not_requested {res['not_requested']}")
        if ("row 9 figures covering_parts, resolve_gets, leaf_gets, parts_beyond_ceiling, rows_exact "
                "(run.toml states lacks ['many_parts_744'])") not in text:
            out.append("summary names no row 9 figures")
        return out

    def test_gated_unrequested(self):
        self.assertEqual(self.check_gated_unrequested(), [])

    def check_gated_asked_not_run(self, evaluate_py=EVALUATE):
        """Item 2 (round 5): a gated figure whose state run.toml asks for and
        the run did not reach is incomplete with the reason."""
        toml = os.path.join(self.run_dir, "run.toml")
        with open(toml) as f:
            text = f.read()
        self.assertEqual(text.count('"many_parts_24"]'), 1)
        with open(toml, "w") as f:
            f.write(text.replace('"many_parts_24"]', '"many_parts_24", "many_parts_744"]'))
        write_jsonl(self.stamp_path("not_run.jsonl"), [{"state": "many_parts_744", "reason": "case: run deadline"}])
        code, res, _ = self.go(BANDS, evaluate_py)
        out = self.gated_placement(res)
        if any(r["key"] == "row_09" for r in res["not_requested"]):
            out.append(f"not_requested {res['not_requested']}")
        want = "figure row_09.covering_parts not run: many_parts_744: case: run deadline"
        if want not in res["incomplete"]:
            out.append(f"incomplete {res['incomplete']}")
        return out

    def test_gated_asked_not_run(self):
        self.assertEqual(self.check_gated_asked_not_run(), [])

    def test_gated_measured(self):
        # Item 2 (round 5): the 24-part arm ran, so its figures are measured
        # and placed nowhere else.
        _, res, _ = self.go(BANDS)
        self.assertEqual(self.gated_placement(res), [])
        for name in ("covering_parts_24", "resolve_gets_24", "rows_exact_24"):
            self.assertEqual(figure(res, f"row_09.{name}")["status"], "value")

    def check_refusal_classes(self, evaluate_py=EVALUATE):
        """Guard refusals and unsupported figures are counted apart."""
        _, res, text = self.go(BANDS, evaluate_py)
        want = {GUARD: {"figures": 15, "calls": 24}, UNSUPPORTED: {"figures": 29, "calls": 0}}
        out = [] if res["refusal_classes"] == want else [f"classes {res['refusal_classes']}"]
        if "refusal classes: refused-by-guard 15 figure(s), 24 call(s); unsupported-on-this-build 29" not in text:
            out.append("summary carries no refusal-class line")
        return out

    def test_refusal_classes(self):
        self.assertEqual(self.check_refusal_classes(), [])

    def unsend(self, keep):
        """Turn every mixed call whose (statement, temp) keep rejects into a not-run line."""
        for i, c in enumerate(self.calls):
            if c["state"] == "mixed" and not keep(c["statement"], c.get("temp")):
                self.calls[i] = {k: c[k] for k in ("statement", "state", "temp", "try", "tenant")}
                self.calls[i].update(outcome="not-run", reason="case: run deadline")
        self.save_calls()

    def check_matrix_all_not_run(self, evaluate_py=EVALUATE):
        """Every call group of a state not run: its rows_exact figure is not-run and the run incomplete."""
        self.unsend(lambda statement, temp: False)
        code, res, _ = self.go(self.pass_bands(MATRIX_BANDS), evaluate_py)
        f = figure(res, "row_t2.mixed_rows_exact")
        out = [] if (f["status"], f["value"], f["verdict"]) == ("not-run", None, "not-run") else [
            f"figure {f['status']} {f['value']} {f['verdict']}"]
        if (code, res["verdict"]) != (1, "incomplete"):
            out.append(f"exit {code} {res['verdict']} {res['problems']}")
        if ("figure row_t2.mixed_rows_exact not run: every call group in mixed not run: case: run deadline"
                not in res["incomplete"]):
            out.append(f"incomplete {res['incomplete']}")
        return out

    def test_matrix_all_not_run(self):
        self.assertEqual(self.check_matrix_all_not_run(), [])

    def check_matrix_some_not_run(self, evaluate_py=EVALUATE):
        """Some call groups of a state not run: the figure passes over the groups that ran and names the rest."""
        self.unsend(lambda statement, temp: (statement, temp) not in (("journey_b", "cold"),
                                                                      ("spans_lookup_1h", "warm")))
        code, res, _ = self.go(self.pass_bands(MATRIX_BANDS), evaluate_py)
        f = figure(res, "row_t2.mixed_rows_exact")
        out = [] if (f["status"], f["value"], f["verdict"]) == ("value", 1, "pass") else [
            f"figure {f['status']} {f['value']} {f['verdict']}"]
        if f["reason"] != ("over the 23 call group(s) that ran; 2 not run: journey_b/cold, "
                           "spans_lookup_1h/warm"):
            out.append(f"reason {f['reason']}")
        if res["verdict"] != "incomplete":
            out.append(f"verdict {res['verdict']} {res['problems']}")
        return out

    def test_matrix_some_not_run(self):
        self.assertEqual(self.check_matrix_some_not_run(), [])

    def check_budget_from_truth(self, evaluate_py=EVALUATE):
        """Row 15's GET budget charges truth.json's holders times its rows per holder."""
        path = os.path.join(self.run_dir, "truth.json")
        with open(path) as f:
            truth = json.load(f)
        truth["failing_cell"]["count"] += 1
        truth["generator"]["logs_per_trace"] += 2
        with open(path, "w") as f:
            json.dump(truth, f)
        _, res, _ = self.go(BANDS, evaluate_py)
        m = {f["id"]: f["value"] for f in res["figures"]}
        out = []
        if (m["row_15.truth_holders"], m["row_15.truth_rows_per_holder"]) != (
                truth["failing_cell"]["count"], truth["generator"]["logs_per_trace"]):
            out.append(f"holders {m['row_15.truth_holders']} rows {m['row_15.truth_rows_per_holder']}")
        with open(BANDS, "rb") as f:
            row = next(r for r in tomllib.load(f)["row"] if r["key"] == "row_15")
        expr = next(f["expr"] for f in row["figure"] if f["name"] == "journey_gets_budget")
        if m["row_15.journey_gets_budget"] != eval(expr, {"min": min}, {"m": m}):
            out.append(f"budget {m['row_15.journey_gets_budget']}")
        return out

    def test_budget_from_truth(self):
        self.assertEqual(self.check_budget_from_truth(), [])

    def check_absent_leaf_size(self, evaluate_py=EVALUATE):
        """A leaf the inspector printed no size for makes the share missing, never a sum that reads it as 0."""
        path = self.stamp_path("folded_leaves.logs.catalog.json")
        with open(path) as f:
            doc = json.load(f)
        leaf = next(lf for p in doc["parts"] for lf in p["leaves"] if lf["field"] == "trace_id")
        del leaf["bytes"]
        with open(path, "w") as f:
            json.dump(doc, f)
        _, res, _ = self.go(BANDS, evaluate_py)
        f = figure(res, "row_08.logs_trace_id_leaf_pct")
        want = ("missing", "a trace_id leaf in stamps/folded_leaves.logs.catalog.json has no size")
        return [] if (f["status"], f["reason"]) == want else [f"{f['status']} {f['value']} {f['reason']}"]

    def test_absent_leaf_size(self):
        self.assertEqual(self.check_absent_leaf_size(), [])

    def check_negative_controls(self, evaluate_py=EVALUATE):
        """Row 13's mutant controls are not built: not-run with that reason, so the run is at best incomplete."""
        _, res, _ = self.go(BANDS, evaluate_py)
        f = figure(res, "row_13.negative_controls_failing_on_mutant")
        out = [] if (f["status"], f["reason"]) == ("not-run", "mutant controls not implemented") else [
            f"{f['status']} {f['value']} {f['reason']}"]
        want = "figure row_13.negative_controls_failing_on_mutant not run: mutant controls not implemented"
        if want not in res["incomplete"]:
            out.append(f"incomplete {res['incomplete']}")
        return out

    def test_negative_controls(self):
        self.assertEqual(self.check_negative_controls(), [])

    def check_maintenance_per_lookup(self, evaluate_py=EVALUATE):
        """The folded cost with maintenance adds maintenance per part-month over lookups per part-month."""
        _, res, _ = self.go(BANDS, evaluate_py)
        freq = res["cost"]["lookups_per_part_month_assumed"]
        rows = [b for b in res["cost"]["break_even"] if "maintenance_per_lookup" in b]
        out = [] if freq == 1000 and rows else [f"freq {freq} rows {len(rows)}"]
        for b in rows:
            per = round(b["maintenance_per_part_month"] / freq, 9)
            if (b["maintenance_per_lookup"], b["dollars_folded_leaves_with_maintenance"]) != (
                    per, round(b["dollars_folded_leaves"] + per, 9)):
                out.append(f"{b['statement']} {b['maintenance_per_lookup']} {b['dollars_folded_leaves_with_maintenance']}")
        return out

    def test_maintenance_per_lookup(self):
        self.assertEqual(self.check_maintenance_per_lookup(), [])

    def check_erasure_victim(self, evaluate_py=EVALUATE):
        """Under a pending erasure the victim's lookup must return no row (truth_key_in), not the unfolded
        answer."""
        _, res, _ = self.go(BANDS, evaluate_py)
        out = [p for p in res["problems"] if "logs_erasure_victim" in p]
        f = figure(res, "row_13.erasure_in_flight_rows_exact")
        if (f["status"], f["value"]) != ("value", 1):
            out.append(f"figure {f['status']} {f['value']}")
        return out

    def test_erasure_victim(self):
        self.assertEqual(self.check_erasure_victim(), [])

    def test_erasure_victim_row_returned_is_wrong(self):
        unfolded = {c["try"]: c for c in self.select("logs_erasure_victim", "unfolded")}
        for c in self.select("logs_erasure_victim", "erasure_in_flight"):
            c["rows"], c["values"] = unfolded[c["try"]]["rows"], unfolded[c["try"]]["values"]
        self.save_calls()
        _, res, _ = self.go(BANDS)
        self.assertIn("wrong_answer logs_erasure_victim/erasure_in_flight/cold try 0: rows 1 expected 0",
                      res["problems"])


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
    "a local build stamp with dirty_paths null taken as clean": (
        '        problems.add("missing", "stamps/build.json dirty_paths: no count of the harness\'s uncommitted paths")\n',
        "        pass\n",
        "check_dirty_paths_null",
    ),
    "absent field not named": (
        '    for name in lacking:\n        problems.add("missing", f"{name} in {at}")\n',
        '    for name in lacking[:0]:\n        problems.add("missing", f"{name} in {at}")\n',
        "check_absent_phases",
    ),
    "zero segmentsOpened taken as a value": (
        '    if name in ("segments_opened", "scan_segments_opened") and value == 0:',
        "    if False:",
        "check_dead_counter",
    ),
    "probe wire divided by the call's own segment count": (
        '            probed, why = candidates(c) if candidates else (None, "no ranges stamp reader")',
        '            probed, why = m["segments"], None',
        "check_probe_per_object",
    ),
    "probe wire divided per GET": (
        '            probed, why = candidates(c) if candidates else (None, "no ranges stamp reader")',
        '            probed, why = m["probe_gets"], None',
        "check_probe_per_object",
    ),
    "every object in the state taken as a candidate": (
        '    return sum(1 for o in objects if o["min_ns"] < end and o["max_ns"] >= start), None',
        '    return len(objects), None',
        "check_probe_per_object",
    ),
    "a per-object breakdown not cross-checked against the stamp": (
        '            if isinstance(per_object, list) and len(per_object) != probed:',
        '            if False:',
        "check_probe_per_object_cross_check",
    ),
    "miss reason dropped": (
        '        if miss and f.get("miss_reason"):',
        "        if False:",
        "check_miss_reason",
    ),
    "leaf share over every field": (
        "        lb = leaf_bytes(doc, field)",
        "        lb = leaf_bytes(doc)",
        "check_leaf_share",
    ),
    "part count read from the declaration": (
        '    v = doc.get("part_count")\n',
        '    v = doc.get("parts_declared", 24)\n',
        "check_part_count",
    ),
    "every server refusal read refused-by-guard": (
        '                        outcome = c["outcome"] = GUARD if probe else "refused"',
        '                        outcome = c["outcome"] = GUARD',
        "check_refused",
    ),
    "only cap-probe refusals taken as refused": (
        '                        outcome = c["outcome"] = GUARD if probe else "refused"',
        '                        outcome = c["outcome"] = "refused" if probe else GUARD',
        "check_refused",
    ),
    "a guard-refused 1,000-key list read as missing": (
        "    if guarded:\n",
        "    if False:\n",
        "check_control_guard",
    ),
    "a 1,000-key list without tier-2 figures read as a miss": (
        '            return UNSUPPORTED, None, f"{where(c)} carries no keyIndex.keysProbed and partsCovered',
        '            return "value", 0, f"{where(c)} carries no keyIndex.keysProbed and partsCovered',
        "check_control_unsupported",
    ),
    "1,000 keys probed read as a miss": (
        '        if ki["keysProbed"] != 1000:',
        '        if ki["keysProbed"] != 999:',
        "check_control_pass",
    ),
    "a 1,000-key list covering no part passed": (
        '        if not ki["partsCovered"]:',
        "        if False:",
        "check_control_zero_parts_covered",
    ),
    "an index-served 1,025-key list not seen": (
        "        if key_index_nonzero(c):\n",
        "        if False:\n",
        "check_control_miss",
    ),
    "a 1,025-key list refused while the 1,000-key list was admitted passed": (
        "    if refused_over:\n",
        "    if False:\n",
        "check_control_over_refused",
    ),
    "a 1,025-key list's leaf GETs not read as index-served": (
        ' or ki.get("partsCovered") or ki.get("leafGetRequests"))',
        ' or ki.get("partsCovered"))',
        "check_control_leaf_gets",
    ),
    "a 1,025-key list served with keyIndex figures and no scan passed": (
        "        if key_index_nonzero(c):\n",
        "        if c.get(\"_never\"):\n",
        "check_control_served_with_keyindex",
    ),
    "scan GETs not counted as a visible path": (
        '        if m["probe_gets"] == 0 and m["scan_gets"] == 0 and m["blocks_scanned"] == 0:',
        '        if m["probe_gets"] == 0:',
        "check_control_served_with_scan",
    ),
    "a 1,025-key list served with no visible path passed": (
        '        if m["probe_gets"] == 0 and m["scan_gets"] == 0 and m["blocks_scanned"] == 0:',
        "        if False:",
        "check_control_served_without_scan",
    ),
    "keyIndex zeros read as index-served or passed": (
        '    return isinstance(ki, dict) and bool(ki.get("keysProbed") or ki.get("partsCovered") or ki.get("leafGetRequests"))',
        '    return isinstance(ki, dict)',
        "check_control_zeros_without_scan",
    ),
    # The #3922 review: a refusal is an error, not a wrong answer, so an
    # evaluator summing the two into matrix.wrong_answers fails the case.
    "matrix.wrong_answers counted as wrong rows plus errors": (
        '            return "value", len(calls.wrong), ""\n',
        '            return "value", len(calls.wrong) + len(calls.errors), ""\n',
        "check_typed_refusal_is_an_error_not_a_wrong_answer",
    ),
    "an Uncovered row 18 cell timed as if the leaves answered it": (
        '    if went or not ki["partsCovered"]:\n',
        "    if False:\n",
        "check_row_18_uncovered",
    ),
    "a not-measured row 18 cell left out of incomplete": (
        "        elif f[\"status\"] == NOT_MEASURED:\n",
        "        elif False:\n",
        "check_row_18_uncovered",
    ),
    "the interquartile range read as the 75th percentile": (
        "        return \"value\", rnd(percentile(walls, 0.75) - percentile(walls, 0.25)), \"\"\n",
        "        return \"value\", rnd(percentile(walls, 0.75)), \"\"\n",
        "check_row_18_noise_edges",
    ),
    "row 18's p50 taken without the Uncovered check": (
        "    if rest == \"leaf_wall_p50_s\":\n        for c in lines:\n",
        "    if rest == \"leaf_wall_p50_s\":\n        for c in []:\n",
        "check_row_18_uncovered",
    ),
    "a phase's segmentsOpened read as every phase's": (
        '        v[f"{p.lower()}_segments_opened"] = ph[p]["segmentsOpened"]\n',
        '        v[f"{p.lower()}_segments_opened"] = sum(ph[q]["segmentsOpened"] for q in PHASES)\n',
        "check_row_6_objects_past_probe",
    ),
    "a state with every call group unsent read as missing": (
        "        if groups and len(unsent) == len(groups):\n",
        "        if False:\n",
        "check_matrix_all_not_run",
    ),
    "unsent call groups not named beside the figure": (
        "        if unsent:\n            names = ",
        "        if False:\n            names = ",
        "check_matrix_some_not_run",
    ),
    "a hits-gated row not listed as not requested": (
        '    if tenant == "hits" and not cfg["hits"]:\n        why.append',
        '    if False:\n        why.append',
        "check_not_requested",
    ),
    "a gated figure of an unlisted state left out of not_requested": (
        "        out.append({\"row\": row, \"key\": key, \"figures\": names, \"reason\": why})\n",
        "        pass\n",
        "check_gated_unrequested",
    ),
    "a gated figure of an asked state left out of incomplete": (
        '        if f["status"] == "not-run" and not f.get("not_requested"):\n',
        '        if f["status"] == "not-run" and not f.get("gated"):\n',
        "check_gated_asked_not_run",
    ),
    "a partial needle inspection left out of incomplete": (
        '            out["incomplete"].append(entry["reason"])\n',
        "",
        "check_needle_partial",
    ),
    "guard refusals counted with unsupported figures": (
        '                                             if c.get("outcome") == k)}',
        '                                             if c.get("outcome") in (GUARD, UNSUPPORTED))}',
        "check_refusal_classes",
    ),
    "full overlap not counted": (
        'overlapped = sum(1 for c in lines if c["started_unix_s"] < ended and',
        'overlapped = sum(1 for c in lines if c["started_unix_s"] > ended and',
        "check_overlap_full",
    ),
    "partial overlap taken as in flight": (
        "        if overlapped == len(lines):\n",
        "        if overlapped:\n",
        "check_overlap_partial",
    ),
    "zero overlap read as a short window": (
        "        if ended <= first:\n",
        "        if ended <= first or not overlapped:\n",
        "check_overlap_zero",
    ),
    "a short window taken as not_in_flight": (
        "        elif ended - started < shortest:\n",
        "        elif False:\n",
        "check_overlap_too_short",
    ),
    "an unsent state read as missing": (
        "        if groups and len(unsent) == len(groups):\n",
        "        if False:\n",
        "check_overlap_too_short",
    ),
    "a skipped state passed over": (
        "        if live.get(s) is not None:\n            out.append(",
        "        if False:\n            out.append(",
        "check_skipped_state",
    ),
    "unsent calls of a live state passed over": (
        "        if lines:\n            out.append(",
        "        if False:\n            out.append(",
        "check_deadline_mid_state",
    ),
    "a failed state read as live": (
        "    for state, line in failed.items():\n        not_run[state] = failed_reason(line)\n",
        "    for state, line in failed.items():\n        pass\n",
        "check_failed_state",
    ),
    "truth figures not read from truth.json": (
        "    return \"value\", v, \"\"\n\n\ndef run_rows",
        "    return \"value\", 1, \"\"\n\n\ndef run_rows",
        "check_budget_from_truth",
    ),
    "an absent leaf size summed as 0": (
        "    return sum(values) if all(isinstance(v, int) for v in values) else None",
        "    return sum(v for v in values if isinstance(v, int))",
        "check_absent_leaf_size",
    ),
    "negative controls reported as run": (
        '            return "not-run", None, "mutant controls not implemented"',
        '            return "value", 5, ""',
        "check_negative_controls",
    ),
    "maintenance charged whole to every lookup": (
        '                per = m["maintenance_per_part_month"] / freq',
        '                per = m["maintenance_per_part_month"]',
        "check_maintenance_per_lookup",
    ),
    "pending erasure judged against the unfolded answer": (
        'unfolded_oracle = st is not None and state not in (st.get("truth_key_in") or {}) and (',
        "unfolded_oracle = st is not None and (",
        "check_erasure_victim",
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
    "amortise_months defaulted to one month": (
        '        problems.add("missing", "run.toml cost.amortise_months (a whole number of months, at least 1)")\n'
        '        cfg["cost"]["amortise_months"] = None\n',
        '        cfg["cost"]["amortise_months"] = 1\n',
        "check_amortise_months_required",
    ),
    "census bytes defaulted to 0": (
        '        b = (line.get("census") or {}).get("bytes")\n',
        '        b = (line.get("census") or {}).get("bytes", 0)\n',
        "check_storage_bytes_missing",
    ),
    "a band's lower bound not applied": (
        '            ok = evaluate_expr(f.get("target_if", "True"), entry["value"], values)\n'
        '            miss = evaluate_expr(f.get("miss_if", "False"), entry["value"], values)\n',
        '            ok = evaluate_expr(f.get("target_if", "True").split(" <= ", 1)[-1], entry["value"], values)\n'
        '            miss = False\n',
        "check_row_6_floor",
    ),
    "a needle outside 25 % of planted passed": (
        "        elif abs(got - want) <= NEEDLE_BLOCK_BAND * want:\n",
        "        elif True:\n",
        "check_needle_band_miss",
    ),
    "a sparse or common needle compared exactly": (
        "        elif band == \"exact\":\n",
        "        elif band in (\"exact\", \"band\"):\n",
        "check_needle_geometry",
    ),
    "a null storage block count read as 0": (
        '        got = doc.get(f"{column}_present_blocks")\n',
        '        got = doc.get(f"{column}_present_blocks") or 0\n',
        "check_needle_storage_null",
    ),
    "a needle's planted count not reported": (
        '        out["figures"].append({"id": f"needle_blocks.{value}.planted", "value": want})\n',
        "",
        "check_needle_geometry",
    ),
    "fold_advance's fold stamps gated on mixed": (
        '                             ("leaf_rebuild", "folded_leaves"), ("fold_advance", "fold_advance")):\n',
        '                             ("leaf_rebuild", "folded_leaves"), ("fold_advance", "mixed")):\n',
        "check_failed_fold_advance",
    ),
    "a server_died call a problem only where a band figure reads it": (
        '                        if outcome == "server_died":\n',
        '                        if False:\n',
        "check_server_died_everywhere",
    ),
}


class Flips(Workdir):
    def test_each_wrong_evaluator_fails_its_case(self):
        with open(EVALUATE) as f:
            src = f.read()
        for name, (old, new, case) in FLIPS.items():
            with self.subTest(flip=name):
                self.assertEqual(src.count(old), 1, f"{name}: anchor gone from evaluate.py; update the flip")
                differs = next((i for i, (a, b) in enumerate(zip(old, new)) if a != b), min(len(old), len(new)))
                line = src[: src.index(old) + differs].count("\n") + 1
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


# Wrong bands the cases rule out: the same shape as FLIPS, with the anchor in
# bands.toml and the case run against the real evaluate.py and the flipped
# bands.
BAND_FLIPS = {
    "row 7's logs section GETs missed against 2 x tail entries, not per pass": (
        "miss_if = \"v >= 2 * m['row_07.logs_tail_entries'] * m['row_07.logs_build_passes']\"\n",
        "miss_if = \"v >= 2 * m['row_07.logs_tail_entries']\"\n",
        "check_row_7_box_d",
    ),
    "row 7's spans section GETs target without the pass factor": (
        "+ m['row_07.spans_uncovered_entries']) if m['row_07.spans_footer_len'] > 8192 else 0)) "
        "* m['row_07.spans_build_passes']\"\n",
        "+ m['row_07.spans_uncovered_entries']) if m['row_07.spans_footer_len'] > 8192 else 0))\"\n",
        "check_row_7_box_d",
    ),
    "row 7's sections read per pass not judged": (
        "target_if = \"v <= m['row_07.logs_new_entries'] + m['row_07.logs_uncovered_entries']\"\n"
        "miss_if = \"v >= m['row_07.logs_tail_entries']\"\n",
        "target_if = \"True\"\n"
        "miss_if = \"False\"\n",
        "check_row_7_rebuild",
    ),
    "row 6's objects past the probe read from segmentsOpened over every phase": (
        'metric = "cold.max.scan_segments_opened"\n',
        'metric = "cold.max.segments_opened"\n',
        "check_row_6_objects_past_probe",
    ),
    "row 18's bound without its noise": (
        "target_if = \"v <= m['row_18.logs_request_id_100_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_request_id_100_unfolded_p50_s'], 0.5 * m['row_18.logs_request_id_100_unfolded_iqr_s'])\"\n",
        "target_if = \"v <= m['row_18.logs_request_id_100_unfolded_p50_s']\"\n",
        "check_row_18_hold",
    ),
    "row 18's noise without its 5 % term": (
        "target_if = \"v <= m['row_18.logs_request_id_10_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_request_id_10_unfolded_p50_s'], 0.5 * m['row_18.logs_request_id_10_unfolded_iqr_s'])\"\n"
        "miss_if = \"v > m['row_18.logs_request_id_10_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_request_id_10_unfolded_p50_s'], 0.5 * m['row_18.logs_request_id_10_unfolded_iqr_s'])\"\n",
        "target_if = \"v <= m['row_18.logs_request_id_10_unfolded_p50_s'] + 0.5 * "
        "m['row_18.logs_request_id_10_unfolded_iqr_s']\"\n"
        "miss_if = \"v > m['row_18.logs_request_id_10_unfolded_p50_s'] + 0.5 * "
        "m['row_18.logs_request_id_10_unfolded_iqr_s']\"\n",
        "check_row_18_noise_edges",
    ),
    "row 18's noise without its interquartile term": (
        "target_if = \"v <= m['row_18.logs_trace_id_10_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_trace_id_10_unfolded_p50_s'], 0.5 * m['row_18.logs_trace_id_10_unfolded_iqr_s'])\"\n"
        "miss_if = \"v > m['row_18.logs_trace_id_10_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_trace_id_10_unfolded_p50_s'], 0.5 * m['row_18.logs_trace_id_10_unfolded_iqr_s'])\"\n",
        "target_if = \"v <= m['row_18.logs_trace_id_10_unfolded_p50_s'] * 1.05\"\n"
        "miss_if = \"v > m['row_18.logs_trace_id_10_unfolded_p50_s'] * 1.05\"\n",
        "check_row_18_noise_edges",
    ),
    "row 18's noise as the whole interquartile range": (
        "target_if = \"v <= m['row_18.logs_trace_id_100_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_trace_id_100_unfolded_p50_s'], 0.5 * m['row_18.logs_trace_id_100_unfolded_iqr_s'])\"\n"
        "miss_if = \"v > m['row_18.logs_trace_id_100_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_trace_id_100_unfolded_p50_s'], 0.5 * m['row_18.logs_trace_id_100_unfolded_iqr_s'])\"\n",
        "target_if = \"v <= m['row_18.logs_trace_id_100_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_trace_id_100_unfolded_p50_s'], m['row_18.logs_trace_id_100_unfolded_iqr_s'])\"\n"
        "miss_if = \"v > m['row_18.logs_trace_id_100_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_trace_id_100_unfolded_p50_s'], m['row_18.logs_trace_id_100_unfolded_iqr_s'])\"\n",
        "check_row_18_noise_edges",
    ),
    "row 18 judged against three times the unfolded p50": (
        "target_if = \"v <= m['row_18.logs_request_id_100_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_request_id_100_unfolded_p50_s'], 0.5 * m['row_18.logs_request_id_100_unfolded_iqr_s'])\"\n"
        "miss_if = \"v > m['row_18.logs_request_id_100_unfolded_p50_s'] + max(0.05 * "
        "m['row_18.logs_request_id_100_unfolded_p50_s'], 0.5 * m['row_18.logs_request_id_100_unfolded_iqr_s'])\"\n",
        "target_if = \"v <= 3 * m['row_18.logs_request_id_100_unfolded_p50_s']\"\n"
        "miss_if = \"v > 3 * m['row_18.logs_request_id_100_unfolded_p50_s']\"\n",
        "check_row_18_box_c_miss",
    ),
    "row 7's previous-leaf GETs not compared with the previous leaves": (
        "target_if = \"v == m['row_07.logs_previous_leaves']\"\n",
        "target_if = \"v >= 0\"\n",
        "check_row_7_extra_leaf_gets",
    ),
}


class BandFlips(Workdir):
    def test_each_wrong_band_fails_its_case(self):
        with open(BANDS) as f:
            src = f.read()
        for name, (old, new, case) in BAND_FLIPS.items():
            with self.subTest(flip=name):
                self.assertEqual(src.count(old), 1, f"{name}: anchor gone from bands.toml; update the flip")
                line = src[: src.index(old)].count("\n") + 1
                flipped = self.path(f"bands_{case}.toml")
                with open(flipped, "w") as f:
                    f.write(src.replace(old, new))
                t = Evaluate(case)
                t.setUp()
                try:
                    self.assertEqual(getattr(t, case)(), [], f"{name}: the real bands fail their own case")
                    t.tearDown()
                    t.setUp()
                    failures = getattr(t, case)(EVALUATE, flipped)
                finally:
                    t.tearDown()
                self.assertTrue(failures, f"{name}: the flipped bands passed {case}")
                print(f"flip '{name}' at bands.toml:{line} fails {case}: {failures}", file=sys.stderr)


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
        states = ("unfolded", "folded_no_leaves", "folded_leaves", "mixed", "advanced_leaves") + self.LANE_STATES[lane]
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
            # Item 3: the even-hours corpus, and the loader's per-batch check
            # over the whole file before the load.
            self.assertIn(f"--even-hours --spans-per-trace 10 --logs-per-trace 5 --start-hour", "\n".join(
                x for x in b["corpus"] if f"corpus-{tenant}" in x))
            self.assertLess(pj.index(f"stride-check --parquet "), pj.index(f"load-{state}-spans"))
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
        # The builds run inside the clone: rustup reads rust-toolchain.toml
        # from the current directory, and the installer sets no default
        # toolchain. A build run from the box root fails at once.
        self.assertIn("env -C /root/inv/ravel", boot)
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
        states = ("unfolded", "folded_no_leaves", "folded_leaves", "mixed", "advanced_leaves") + self.LANE_STATES["s3"]
        lines = [x for x in text.splitlines() if x.startswith("NOT-RUN ")]
        self.assertEqual(lines, [f"NOT-RUN {s}: deadline: run.max_minutes 0 passed (0 minutes elapsed)"
                                 for s in states])
        self.assertEqual(self.steps(text)[-2:], ["evaluate", "finish"])
        self.assertNotIn("RQ_STATS=1", text)

    def deadline_lines(self, text):
        # The seconds left move with the clock; everything else is pinned.
        return [re.sub(r", \d+ s left of", ", N s left of", x) for x in text.splitlines()
                if x.startswith("NOT-RUN ")]

    def test_s3_dry_run_skips_steps_the_deadline_cannot_hold(self):
        # Item 6: with 20 minutes of deadline, every state whose next long
        # step is bounded past what is left is NOT-RUN before that step starts:
        # the 30-minute default estimate for loads, folds, leaf rebuilds and
        # compaction, the 70-minute wait for the mixed state's hour. States
        # with no long step run, and evaluate and finish still follow.
        code, text = run_sh("s3", self.example_toml(("max_minutes = 540", "max_minutes = 20")), self.path("out"))
        self.assertEqual(code, 0, text)
        left = "N s left of run.max_minutes 20"
        self.assertEqual(self.deadline_lines(text), [
            f"NOT-RUN folded_no_leaves: deadline: fold needs up to 1800 s, {left}",
            f"NOT-RUN folded_leaves: deadline: fold needs up to 1800 s, {left}",
            f"NOT-RUN mixed: deadline: the wait for the next ingest hour needs up to 4200 s, {left}",
            f"NOT-RUN compaction_in_flight: deadline: wait_sealed and compaction needs up to 2110 s, {left}",
            f"NOT-RUN many_parts_744: deadline: load needs up to 1800 s, {left}",
            f"NOT-RUN many_parts_2160: deadline: load needs up to 1800 s, {left}",
        ])
        b = self.blocks(text)
        self.assertNotIn("catalog fold", "\n".join(b["state:folded_no_leaves"] + b["state:folded_leaves"]))
        self.assertNotIn("wait until the ingest hour", text)
        self.assertNotIn("maintain compact-tenant", text)
        self.assertIn("needle-blocks", "\n".join(b["state:unfolded"]))
        self.assertIn("erase submit", "\n".join(b["state:erasure_in_flight"]))
        self.assertEqual(self.steps(text)[-2:], ["evaluate", "finish"])

    def test_s3_dry_run_records_mixed_not_run_when_no_fold_ran(self):
        # run.toml may list mixed without its fold states (load_config checks
        # names only), and the fold states can also be skipped by their own
        # deadline checks while the mixed wait still fits. Either way no fold
        # ran, so FOLD_HOUR is empty and the mixed state is recorded not-run
        # before the wait; evaluate and finish still follow.
        # FLIP: without the check, wait_next_hour reads the unset FOLD_HOUR,
        # and under set -u the run ends there with status 1, so on_exit writes
        # FAILED and neither evaluate nor the upload runs.
        code, text = run_sh("s3", self.example_toml(
            ("states = [\"unfolded\", \"folded_no_leaves\", \"folded_leaves\", \"mixed\", \"advanced_leaves\", "
             "\"compaction_in_flight\",\n"
             "          \"erasure_in_flight\", \"many_parts_744\", \"many_parts_2160\"]",
             "states = [\"unfolded\", \"mixed\"]")), self.path("out"))
        self.assertEqual(code, 0, text)
        self.assertEqual(self.deadline_lines(text), [
            "NOT-RUN mixed: no fold ran before the mixed state (its fold states were skipped or not listed), "
            "so the tail hour has no sealed hour to follow",
        ])
        self.assertNotIn("wait until the ingest hour", text)
        self.assertEqual(self.steps(text)[-2:], ["evaluate", "finish"])

    def test_s3_dry_run_per_state_estimate(self):
        # Item 6: a per-state estimate in run.toml replaces the default for
        # that state alone; the 310 s sealed-hour wait is bounded on its own.
        code, text = run_sh("s3", self.example_toml(
            ("max_minutes = 540", "max_minutes = 20"),
            ("[step_estimate_minutes]\ndefault = 30\n",
             "[step_estimate_minutes]\ndefault = 30\nfolded_no_leaves = 5\ncompaction_in_flight = 13\n")),
            self.path("out"))
        self.assertEqual(code, 0, text)
        lines = self.deadline_lines(text)
        self.assertNotIn("folded_no_leaves", "\n".join(lines))
        self.assertNotIn("compaction_in_flight", "\n".join(lines))
        self.assertIn("NOT-RUN folded_leaves: deadline: fold needs up to 1800 s, N s left of run.max_minutes 20",
                      lines)
        self.assertIn("catalog fold", "\n".join(self.blocks(text)["state:folded_no_leaves"]))
        self.assertIn("maintain compact-tenant", text)
        code, text = run_sh("s3", self.example_toml(
            ("max_minutes = 540", "max_minutes = 5"), ("states = [\"unfolded\"", "states = [\"compaction_in_flight\""),
            ("[step_estimate_minutes]\ndefault = 30\n", "[step_estimate_minutes]\ndefault = 0\n")),
            self.path("out"))
        self.assertEqual(code, 0, text)
        self.assertEqual(self.deadline_lines(text)[0], "NOT-RUN compaction_in_flight: deadline: wait_sealed and "
                                                       "compaction needs up to 310 s, N s left of run.max_minutes 5")
        code, text = run_sh("s3", self.example_toml(
            ("[step_estimate_minutes]\ndefault = 30\n", "[step_estimate_minutes]\ndefault = \"half\"\n")),
            self.path("out"))
        self.assertEqual(code, 1, text)
        self.assertIn("run.toml step_estimate_minutes.unfolded", text)

    def compaction_deadline_lines(self, run_sh_path):
        # 16 minutes of compaction fits 20 minutes of run, and so does the
        # 310 s sealed-hour wait, but not the two back to back.
        toml = self.example_toml(
            ("max_minutes = 540", "max_minutes = 20"), ("states = [\"unfolded\"", "states = [\"compaction_in_flight\""),
            ("[step_estimate_minutes]\ndefault = 30\n", "[step_estimate_minutes]\ndefault = 0\ncompaction_in_flight = 16\n"))
        p = subprocess.run([BASH, run_sh_path, "--dry-run", "--lane", "s3", "--run-toml", toml, "--out",
                            self.path("out")], capture_output=True, text=True)
        lines = [x for x in self.deadline_lines(p.stdout + p.stderr) if x.startswith("NOT-RUN compaction_in_flight:")]
        return p.returncode, sorted(set(lines)), p.stdout + p.stderr

    def test_compaction_deadline_checks_the_sum(self):
        # Item 4 (round 5): wait_sealed and the compaction are checked
        # against the time left together.
        code, lines, text = self.compaction_deadline_lines(os.path.join(BOX, "run.sh"))
        self.assertEqual(code, 0, text)
        self.assertEqual(lines, ["NOT-RUN compaction_in_flight: deadline: wait_sealed and compaction needs up to "
                                 "1270 s, N s left of run.max_minutes 20"])
        self.assertNotIn("maintain compact-tenant", text)
        with open(os.path.join(BOX, "run.sh")) as f:
            src = f.read()
        old = ('  fits compaction_in_flight "wait_sealed and compaction" "$((WAIT_SEALED_MAX_S + compact_s))" '
               '|| return 0\n')
        self.assertEqual(src.count(old), 1)
        inv = self.path("inv")
        os.mkdir(inv)
        for name in os.listdir(HERE):
            if name != "box":
                os.symlink(os.path.join(HERE, name), os.path.join(inv, name))
        shutil.copytree(BOX, os.path.join(inv, "box"))
        with open(os.path.join(inv, "box", "run.sh"), "w") as f:
            f.write(src.replace(old, '  fits compaction_in_flight wait_sealed "$WAIT_SEALED_MAX_S" || return 0\n'
                                     '  fits compaction_in_flight compaction "$compact_s" || return 0\n'))
        _, lines, text = self.compaction_deadline_lines(os.path.join(inv, "box", "run.sh"))
        self.assertEqual(lines, [])
        self.assertIn("maintain compact-tenant", text)
        line = src[: src.index(old)].count("\n") + 1
        print(f"flip 'wait_sealed and compaction checked apart' at box/run.sh:{line} fails "
              f"test_compaction_deadline_checks_the_sum: NOT-RUN lines {lines}", file=sys.stderr)

    def test_deadline_check_flip_fails_its_case(self):
        # Item 6: a run.sh whose step check never refuses starts the fold
        # the deadline cannot hold.
        with open(os.path.join(BOX, "run.sh")) as f:
            src = f.read()
        old = '  [ "$left" -lt "$3" ] || return 0\n'
        self.assertEqual(src.count(old), 1)
        inv = self.path("inv")
        os.mkdir(inv)
        for name in os.listdir(HERE):
            if name != "box":
                os.symlink(os.path.join(HERE, name), os.path.join(inv, name))
        shutil.copytree(BOX, os.path.join(inv, "box"))
        with open(os.path.join(inv, "box", "run.sh"), "w") as f:
            f.write(src.replace(old, "  return 0\n"))
        toml = self.example_toml(("max_minutes = 540", "max_minutes = 20"))
        p = subprocess.run([BASH, os.path.join(inv, "box", "run.sh"), "--dry-run", "--lane", "s3", "--run-toml",
                            toml, "--out", self.path("out")], capture_output=True, text=True)
        lines = self.deadline_lines(p.stdout + p.stderr)
        self.assertNotIn("NOT-RUN folded_no_leaves: deadline: fold needs up to 1800 s, N s left of "
                         "run.max_minutes 20", lines)
        line = src[: src.index(old)].count("\n") + 1
        print(f"flip 'a step the deadline cannot hold started' at box/run.sh:{line} fails "
              f"test_s3_dry_run_skips_steps_the_deadline_cannot_hold: NOT-RUN lines {lines}", file=sys.stderr)

    def test_local_dry_run(self):
        code, text = run_sh("local", os.path.join(BOX, "run.local-smoke.toml"), self.path("out"))
        self.assertEqual(code, 0, text)
        b = self.assert_flow(text, "local")
        self.assertIn("--address 127.0.0.1:", "\n".join(b["store"]))
        self.assertNotIn("--tenant hits", text)
        self.assertNotIn("aws ", text)
        self.assertNotIn("terminate", text)
        self.assertNotIn("shutdown", text)

    def dirty_paths_line(self, run_sh_src=None):
        """The local dry run's dirty_paths with a git whose status fails."""
        d = self.path("gitstub")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "git"), "w") as f:
            f.write('#!/bin/sh\ncase " $* " in *" status "*) exit 128 ;; esac\necho 0123456789abcdef\n')
        os.chmod(os.path.join(d, "git"), 0o755)
        script = os.path.join(BOX, "run.sh")
        if run_sh_src is not None:
            script = os.path.join(BOX, "run.flipped.sh")
            with open(script, "w") as f:
                f.write(run_sh_src)
        try:
            p = subprocess.run([BASH, script, "--dry-run", "--lane", "local", "--run-toml",
                                os.path.join(BOX, "run.local-smoke.toml"), "--out", self.path("out")],
                               capture_output=True, text=True, env=dict(os.environ, PATH=d + os.pathsep + os.environ["PATH"]))
        finally:
            if run_sh_src is not None:
                os.remove(script)
        return [x for x in p.stdout.splitlines() if "build.json dirty_paths" in x]

    def test_dirty_paths_null_when_git_status_fails(self):
        self.assertEqual(self.dirty_paths_line(), ["DRY-RUN: build.json dirty_paths null"])
        with open(os.path.join(BOX, "run.sh")) as f:
            src = f.read()
        old = "      dirty=null\n"
        self.assertEqual(src.count(old), 1)
        # Flipped back to a count of zero, the failed status reads as a clean tree.
        self.assertEqual(self.dirty_paths_line(src.replace(old, "      dirty=0\n")),
                         ["DRY-RUN: build.json dirty_paths 0"])

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

    @staticmethod
    def objects_over_512k(inv_py):
        mod = load_module(inv_py, "inv_case")
        keys = [("t/h1/l/l0/0000/a.1.1.0000000000000000.rseg", 512 * 1024),
                ("t/h1/l/l0/0000/b.1.1.0000000000000000.rseg", 512 * 1024 + 1),
                ("t/h1/l/l0/0001/c.1.1.0000000000000000.rseg", 10),
                ("t/h2/l/l0/0000/d.1.1.0000000000000000.rseg", 1 << 30)]
        got = mod.tenant_census(keys, "h1")["data"]["logs"]
        return [] if (got["objects"], got["objects_over_512k"]) == (3, 1) else [f"census {got}"]

    def test_row_6_floor_counts_objects_over_512_kib(self):
        # Row 6's keyIndex GET floor is the tenant's data objects strictly
        # over 512 KiB; one at exactly 512 KiB is not probed.
        inv = os.path.join(BOX, "lib", "inv.py")
        self.assertEqual(self.objects_over_512k(inv), [])
        with open(inv) as f:
            src = f.read()
        old = 'd["objects_over_512k"] += size > LARGE_OBJECT_BYTES'
        self.assertEqual(src.count(old), 1)
        flipped = self.path("inv_flipped.py")
        with open(flipped, "w") as f:
            f.write(src.replace(old, old.replace(" > ", " >= ")))
        failures = self.objects_over_512k(flipped)
        self.assertTrue(failures)
        line = src[: src.index(old)].count("\n") + 1
        print(f"flip 'objects at 512 KiB counted' at box/lib/inv.py:{line} fails the row 6 floor: {failures}",
              file=sys.stderr)

    # A listing of one hour: 4 L0 objects, their 4 commit records, the
    # provenance object and one L1 part. One commit
    # record is given a data-object size so a count over the signal prefix
    # shows in objects_over_512k too.
    CENSUS_KEYS = [
        *((f"t/h1/l/l0/000{i}/w.1.{i}.000000000000000{i}.rseg", 600 * 1024 if i < 2 else 1000) for i in range(4)),
        *((f"t/h1/l/c/000{i}/2026110300/w.1.{i}.cmt", 700 * 1024 if i == 0 else 300) for i in range(4)),
        ("t/h1/l/prov", 33),
        ("t/h1/l/l1/0000/2026110300/abcdabcdabcdabcd.0000.0123456701234567.rseg", 900 * 1024),
    ]

    @classmethod
    def census_of_hits(cls, inv_py):
        """(problems, census figures) of CENSUS_KEYS as the hits tenant, and
        row 6's keyIndex GET verdicts at the 2 x objects ceiling and one past
        it."""
        inv = load_module(inv_py, "inv_census")
        tc = inv.tenant_census(cls.CENSUS_KEYS, "h1")
        got = tc["data"]["logs"]
        out = []
        want = {"objects": 5, "l0_objects": 4, "l1_objects": 1, "objects_over_512k": 3,
                "bytes": 2 * 600 * 1024 + 2 * 1000 + 900 * 1024}
        for k, v in want.items():
            if got.get(k) != v:
                out.append(f"census {k} {got.get(k)}, want {v}")
        for cls_name, n in (("l/l0", 4), ("l/c", 4), ("l/prov", 1), ("l/l1", 1)):
            if tc["classes"].get(cls_name, {}).get("objects") != n:
                out.append(f"class {cls_name} {tc['classes'].get(cls_name)}, want {n} objects")
        for key, want_cls in (("t/h1/l/l0/0000/x.rseg", "l/l0"), ("t/h1/s/c/0000/h/x.cmt", "s/c"),
                              ("t/h1/l/prov", "l/prov"), ("t/h1/catalog/s/HEAD", "catalog/s")):
            if inv.object_class(key) != want_cls:
                out.append(f"object_class({key}) {inv.object_class(key)!r}, want {want_cls!r}")
        mod = load_module(EVALUATE, "evaluate_census_row6")
        with open(BANDS, "rb") as f:
            bands = {"row": [r for r in tomllib.load(f)["row"] if r["key"] == "row_06"]}
        census = {("unfolded",): {"state": "unfolded", "census": {"tenants": {"hits": tc}}}}
        cfg = {"lane": "local", "hits": True, "tenant": "inv", "scales": [1], "baseline": False,
               "states": ["unfolded"]}
        for gets, verdict in ((10, "pass"), (11, "miss")):
            def fixed(calls, cfg, statement, state, metric, candidates=None, gets=gets):
                return ("value", gets, "") if metric == "cold.max.keyindex_gets" else ("missing", None, "not set")
            mod.statement_figure = fixed
            figs = mod.run_rows(bands, cfg, {"unfolded": None}, None, None, {}, census, None, {}, {})
            fig = next(f for f in figs if f["id"] == "row_06.keyindex_gets")
            if fig["verdict"] != verdict:
                out.append(f"{gets} keyIndex GETs over {got.get('objects')} objects read {fig['verdict']}, "
                           f"want {verdict}")
        return out

    def test_census_counts_data_objects_only(self):
        # Item 1 (round 5): data objects are the .rseg keys under l0/ and l1/;
        # commit records, provenance and compaction records are classes.
        inv = os.path.join(BOX, "lib", "inv.py")
        self.assertEqual(self.census_of_hits(inv), [])
        with open(inv) as f:
            src = f.read()
        old = "            if is_data_key(rest):\n"
        self.assertEqual(src.count(old), 1)
        flipped = self.path("inv_census_flipped.py")
        with open(flipped, "w") as f:
            f.write(src.replace(old, "            if True:\n"))
        failures = self.census_of_hits(flipped)
        self.assertIn("census objects 10, want 5", failures)
        self.assertIn("11 keyIndex GETs over 10 objects read pass, want miss", failures)
        line = src[: src.index(old)].count("\n") + 1
        print(f"flip 'every signal key counted as a data object' at box/lib/inv.py:{line} fails the census: "
              f"{failures}", file=sys.stderr)
        old = "    if head in SIGNAL_DIR and len(rest) > 1:\n"
        self.assertEqual(src.count(old), 1)
        with open(flipped, "w") as f:
            f.write(src.replace(old, '    if head in ("logs", "spans", "metrics") and len(rest) > 1:\n'))
        failures = self.census_of_hits(flipped)
        self.assertIn("object_class(t/h1/l/l0/0000/x.rseg) 'l', want 'l/l0'", failures)
        line = src[: src.index(old)].count("\n") + 1
        print(f"flip 'object_class by long signal names' at box/lib/inv.py:{line} fails the census: {failures}",
              file=sys.stderr)

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

    def advance_stamps(self, name, kind="fold --key-index-leaves", exit_code=0, rebuilt=5, advanced=27,
                       labels=("spans", "logs")):
        """A stamps directory as the run leaves it before advanced_leaves's check: fold_advance's fold lines
        for LABELS and the folded_leaves and advanced_leaves catalog stamps, logs request_id at REBUILT and
        ADVANCED slices a part (box D's 5 and 27), trace_id at 1 and 6, spans trace_id at 1 and 3."""
        d = self.path(name)
        os.makedirs(d)
        write_jsonl(os.path.join(d, "folds.jsonl"), [
            {"transition": "leaf_rebuild", "signal": s, "kind": "rebuild-key-index", "exit": 0} for s in labels] + [
            {"transition": "fold_advance", "signal": s, "kind": kind, "exit": exit_code} for s in labels])
        for state, logs, trace, spans in (("folded_leaves", rebuilt, 1, 1), ("advanced_leaves", advanced, 6, 3)):
            for label, fields in (("logs", {"request_id": logs, "trace_id": trace}), ("spans", {"trace_id": spans})):
                leaves = [{"field": f, "key": f"{f}-{i}"} for f, n in fields.items() for i in range(n)]
                with open(os.path.join(d, f"{state}.{label}.catalog.json"), "w") as f:
                    json.dump({"parts": [{"index": 0, "leaves": leaves}]}, f)
        return d

    def advance_check(self, stamps, inv_py=os.path.join(BOX, "lib", "inv.py")):
        p = subprocess.run([PY, inv_py, "advance-check", "--stamps", stamps], capture_output=True, text=True)
        return p.returncode, p.stdout, p.stderr

    def test_advance_check(self):
        # Item 5: advanced_leaves's two preconditions. Box D's stamps pass and
        # print the slices per (part, field) of both stamps.
        code, out, err = self.advance_check(self.advance_stamps("ok"))
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["slices_per_part_field"], {
            "folded_leaves": {"logs": {"request_id": 5, "trace_id": 1}, "spans": {"trace_id": 1}},
            "advanced_leaves": {"logs": {"request_id": 27, "trace_id": 6}, "spans": {"trace_id": 3}}})
        for name, kw, want in (
                ("switch-off", dict(kind="fold"), "fold_advance on spans ran without --key-index-leaves"),
                ("fold-failed", dict(exit_code=101), "fold_advance on spans exited 101"),
                ("no-advance", dict(labels=()), "no fold_advance fold stamp for spans"),
                ("no-new-slice", dict(advanced=5), "logs request_id has 5 slices per part in advanced_leaves "
                                                   "against 5 after the leaf rebuild")):
            code, _, err = self.advance_check(self.advance_stamps(name, **kw))
            self.assertEqual(code, 1, name)
            self.assertIn(want, err, name)

    def test_advance_check_flips(self):
        # The wrong checks the cases above rule out, each printed with the
        # inv.py line it flips.
        for old, new, stamps, label in (
                ("    if not rebuilt or not advanced or advanced <= rebuilt:\n",
                 "    if not rebuilt or not advanced or advanced < rebuilt:\n", dict(advanced=5),
                 "equal slices read as an advance"),
                ('        if "--key-index-leaves" not in str(line.get("kind") or "").split():\n',
                 "        if False:\n", dict(kind="fold"), "a fold with the switch off passed")):
            flipped, line = self.flipped_box(old, new, "lib/inv.py")
            code, _, _ = self.advance_check(self.advance_stamps(f"flip-{line}", **stamps), flipped)
            self.assertEqual(code, 0, label)
            shutil.rmtree(self.path("inv-flipped"))
            print(f"flip '{label}' at box/lib/inv.py:{line} fails test_advance_check", file=sys.stderr)

    def test_advanced_leaves_runs_the_check_after_mixed(self):
        # Item 5: the state folds nothing, stamps itself, checks, then sends
        # its calls; fold_advance before it folds with the switch on.
        code, text = run_sh("s3", os.path.join(BOX, "run.example.toml"), self.path("out"))
        self.assertEqual(code, 0, text)
        steps = self.steps(text)
        self.assertEqual(steps[steps.index("state:mixed") + 1], "state:advanced_leaves")
        b = self.blocks(text)
        adv = "\n".join(b["state:advanced_leaves"])
        self.assertNotIn("catalog fold", adv)
        self.assertIn("STAMP stamps/advanced_leaves.server.json", adv)
        self.assertIn("advance-check --stamps", adv)
        self.assertLess(adv.index("inspect-stamp"), adv.index("advance-check"))
        self.assertLess(adv.index("advance-check"), adv.index("plan --statements"))
        mixed = [x for x in b["state:mixed"] if "catalog fold" in x]
        self.assertTrue(mixed and all("--key-index-leaves" in x for x in mixed), mixed)
        code, text = run_sh("local", os.path.join(BOX, "run.local-smoke.toml"), self.path("out-local"))
        self.assertEqual(code, 0, text)
        self.assertIn("state:advanced_leaves", self.steps(text))

    def test_advanced_leaves_failed_check_fails_the_state(self):
        # A failed advance-check is the state's STATE-FAILED, and the run
        # goes on with the next state.
        out = self.path("out-fail")
        p = subprocess.run([BASH, os.path.join(BOX, "run.sh"), "--dry-run", "--lane", "s3", "--run-toml",
                            os.path.join(BOX, "run.example.toml"), "--out", out], capture_output=True, text=True,
                           env=dict(os.environ, INV_DRY_RUN_FAIL="advance-check"))
        text = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, text)
        self.assertIn("STATE-FAILED advanced_leaves: advance-check advanced_leaves exit 3", text)
        steps = self.steps(text)
        self.assertEqual(steps[steps.index("state:advanced_leaves") + 1], "state:compaction_in_flight")

    def flipped_box(self, old, new, script="run.sh"):
        """A copy of this directory whose box/SCRIPT has OLD (present once)
        replaced by NEW; returns (path of the copy's script, the 1-based line
        of OLD in the real one)."""
        with open(os.path.join(BOX, script)) as f:
            src = f.read()
        self.assertEqual(src.count(old), 1, old)
        inv = self.path("inv-flipped")
        os.mkdir(inv)
        for name in os.listdir(HERE):
            if name != "box":
                os.symlink(os.path.join(HERE, name), os.path.join(inv, name))
        shutil.copytree(BOX, os.path.join(inv, "box"))
        dst = os.path.join(inv, "box", script)
        with open(dst, "w") as f:
            f.write(src.replace(old, new))
        return dst, src[: src.index(old)].count("\n") + 1

    def failed_state_dry_run(self, script=None):
        p = subprocess.run([BASH, script or os.path.join(BOX, "run.sh"), "--dry-run", "--lane", "s3", "--run-toml",
                            os.path.join(BOX, "run.example.toml"), "--out", self.path("out")],
                           capture_output=True, text=True, env=dict(os.environ, INV_DRY_RUN_FAIL="--tenant mp744"))
        return p.returncode, p.stdout + p.stderr

    # The line in run_state that records a failed state and returns, so the
    # run goes on; flipped to fail_logged it ends the run at the state.
    STATE_FAILED_LINE = '  [ -n "$FAIL_STEP" ] || fail "state $state returned $code with no failed command named"\n  state_failed\n'

    def test_failed_state_continues_dry_run(self):
        # Item 1: many_parts_744's load exits 3; the state is recorded failed
        # with its log's out/ path, and the run goes on to many_parts_2160,
        # evaluate and the upload.
        code, text = self.failed_state_dry_run()
        self.assertEqual(code, 0, text)
        steps = self.steps(text)
        self.assertEqual(steps[-4:], ["state:many_parts_744", "state:many_parts_2160", "evaluate", "finish"])
        b = self.blocks(text)
        mp = b["state:many_parts_744"]
        self.assertIn("STATE-FAILED many_parts_744: load many_parts_744/spans exit 3 "
                      "(log out/logs/load-many_parts_744-spans.out); the run goes on", mp)
        self.assertIn("STAMP stamps/failed_states.jsonl", mp)
        self.assertLess(mp.index("STAMP stamps/loads.jsonl"), mp.index("STAMP stamps/failed_states.jsonl"))
        self.assertTrue(any(x.startswith("DRY-RUN: copy ") and x.endswith(
            "/work/load-many_parts_744-spans.out to out/logs/load-many_parts_744-spans.out") for x in mp), mp)
        self.assertNotIn("catalog fold", "\n".join(mp))
        self.assertIn("catalog fold --tenant mp2160", "\n".join(b["state:many_parts_2160"]))
        self.assertIn("upload out/ (no DONE, FAILED or work/)", "\n".join(b["finish"]))
        flipped, line = self.flipped_box(self.STATE_FAILED_LINE, self.STATE_FAILED_LINE.replace(
            "  state_failed\n", "  fail_logged\n"))
        code, text = self.failed_state_dry_run(flipped)
        self.assertEqual(code, 1, text)
        self.assertNotIn("evaluate", self.steps(text))
        print(f"flip 'a failed state ends the run' at box/run.sh:{line + 1} fails "
              f"test_failed_state_continues_dry_run: exit {code}, steps end {self.steps(text)[-1:]}",
              file=sys.stderr)

    def test_failed_base_load_ends_the_run_dry_run(self):
        # Item 1: load-base is not a state; nothing can be measured without
        # it, so its failure still ends the run, naming the kept log.
        p = subprocess.run([BASH, os.path.join(BOX, "run.sh"), "--dry-run", "--lane", "s3", "--run-toml",
                            os.path.join(BOX, "run.example.toml"), "--out", self.path("out")],
                           capture_output=True, text=True, env=dict(os.environ, INV_DRY_RUN_FAIL="load-base-logs"))
        text = p.stdout + p.stderr
        self.assertEqual(p.returncode, 1, text)
        self.assertIn("run.sh: FAILED step=load-base: load base/logs exit 3 (log out/logs/load-base-logs.out)", text)
        self.assertNotIn("STEP state:", text)

    def fold_advance_failed_dry_run(self, script=None):
        p = subprocess.run([BASH, script or os.path.join(BOX, "run.sh"), "--dry-run", "--lane", "s3", "--run-toml",
                            os.path.join(BOX, "run.example.toml"), "--out", self.path("out")],
                           capture_output=True, text=True,
                           env=dict(os.environ, INV_DRY_RUN_FAIL="fold-fold_advance-spans"))
        return p.returncode, p.stdout + p.stderr

    def test_failed_fold_advance_is_recorded_against_fold_advance(self):
        # The fold that seals the mixed state's tail hour is fold_advance's,
        # the derived state, as its deadline check records it. Recorded
        # against mixed it would discard mixed's measured figures and every
        # row that requires mixed live (row 13 among them), although mixed
        # ran fine; recorded against fold_advance it loses only the figures
        # that depend on the fold. The run still goes on with the next state.
        code, text = self.fold_advance_failed_dry_run()
        self.assertEqual(code, 0, text)
        self.assertIn("STATE-FAILED fold_advance: fold fold_advance/spans exit 3 "
                      "(log out/logs/fold-fold_advance-spans.out); the run goes on", text)
        self.assertNotIn("STATE-FAILED mixed", text)
        steps = self.steps(text)
        self.assertIn("state:compaction_in_flight", steps[steps.index("state:mixed"):])
        self.assertEqual(steps[-2:], ["evaluate", "finish"])
        flipped, line = self.flipped_box(
            "  fold fold_advance --key-index-leaves || { STATE=fold_advance; return 1; }\n",
            "  fold fold_advance --key-index-leaves || return 1\n")
        code, text = self.fold_advance_failed_dry_run(flipped)
        self.assertEqual(code, 0, text)
        self.assertIn("STATE-FAILED mixed: fold fold_advance/spans exit 3", text)
        print(f"flip 'a failed fold_advance is recorded against mixed' at box/run.sh:{line + 1} fails "
              f"test_failed_fold_advance_is_recorded_against_fold_advance", file=sys.stderr)

    def sourced_states(self, script=None):
        """run.sh sourced, outside a dry run, with a ravel-cli stub that
        prints its arguments and exits 7: the folded_no_leaves and
        folded_leaves states' folds fail for real (inv.py measure runs the
        stub), and run_states runs both. Returns (exit, output, out dir)."""
        stub = self.path("stub-cli")
        with open(stub, "w") as f:
            f.write('#!/bin/sh\necho "stub ravel-cli refused: $*"\nexit 7\n')
        os.chmod(stub, 0o755)
        out = self.path("out")
        driver = self.path("driver.sh")
        with open(driver, "w") as f:
            f.write(f"""set -u
source {script or os.path.join(BOX, "run.sh")} --dry-run --lane local --run-toml {os.path.join(BOX, "run.local-smoke.toml")} --out {out}
DRY_RUN=0
load_config
CLI=({stub})
STATES="folded_no_leaves folded_leaves"
run_states
echo RUN-STATES-RETURNED
FINISHED=1
""")
        p = subprocess.run([BASH, driver], capture_output=True, text=True, timeout=120)
        return p.returncode, p.stdout + p.stderr, out

    def test_failed_state_keeps_its_log_and_the_run_goes_on(self):
        # Items 1 and 2: each failed fold's log is copied to out/logs/ and
        # named by its failed_states line, and run_states returns after the
        # second state instead of the run ending at the first.
        code, text, out = self.sourced_states()
        self.assertEqual(code, 0, text)
        self.assertIn("RUN-STATES-RETURNED", text)
        self.assertFalse(os.path.exists(os.path.join(out, "FAILED")))
        lines = read_jsonl(os.path.join(out, "stamps", "failed_states.jsonl"))
        self.assertEqual([(x["state"], x["step"], x["exit"], x["log"]) for x in lines], [
            ("folded_no_leaves", "fold folded_no_leaves/spans", 7, "out/logs/fold-folded_no_leaves-spans.out"),
            ("folded_leaves", "fold folded_leaves/spans", 7, "out/logs/fold-folded_leaves-spans.out")])
        with open(os.path.join(out, "logs", "fold-folded_no_leaves-spans.out")) as f:
            self.assertEqual(f.read(), "stub ravel-cli refused: catalog fold --tenant inv --shards 2 --signal spans "
                                       "--writers-stopped --json\n")
        folds = read_jsonl(os.path.join(out, "stamps", "folds.jsonl"))
        self.assertEqual([(x["transition"], x["exit"]) for x in folds], [("folded_no_leaves", 7), ("folded_leaves", 7)])
        old = '  elif ! { mkdir -p "$OUT/logs" && cp "$1" "$OUT/logs/"; } 2>/dev/null; then\n'
        flipped, line = self.flipped_box(old, "  elif false; then\n")
        shutil.rmtree(out)
        code, text, out = self.sourced_states(flipped)
        self.assertEqual(code, 0, text)
        self.assertFalse(os.path.exists(os.path.join(out, "logs")))
        print(f"flip 'the failed command's log not copied' at box/run.sh:{line} fails "
              f"test_failed_state_keeps_its_log_and_the_run_goes_on: out/logs/ absent", file=sys.stderr)

    @staticmethod
    def stride_check_744(gen_corpus, even, inv_py=os.path.join(BOX, "lib", "inv.py"), out=None):
        """The strided-load check over a many_parts_744 spans corpus at
        traces_per_hour 1, as run.sh deals it (batch rows = rows / 744,
        ingest hour start one past the first event hour)."""
        start = 497516
        subprocess.run([PY, gen_corpus, "--traces", "744", "--hours", "744", "--seed", "751", "--spans-per-trace",
                        "10", "--logs-per-trace", "5", "--format", "jsonl", "--start-hour", str(start), "--out", out,
                        *(["--even-hours"] if even else [])], check=True, capture_output=True)
        with open(os.path.join(out, "spans.jsonl")) as f:
            rows = [json.loads(line) for line in f]
        inv = load_module(inv_py, "inv_stride")
        return inv.stride_refusal([r["end_ts"] for r in rows], len(rows) // 744, start + 1, 1,
                                  inv.MAX_INGEST_LAG_NS, 744), rows

    def test_many_parts_744_strided_load_check(self):
        # Item 3: box A's many_parts_744 load exited 1 because a random hour
        # per trace leaves the hours unequal, so a fixed --batch-rows drifts
        # off its hours and the loader refuses the first batch whose events
        # pass its ingest hour's end plus the 2 h lag (EventTimePastLag).
        # The even-hours corpus holds one trace per hour and passes; the
        # random one is refused, as the loader refused it on the local lane.
        with open(os.path.join(HERE, "..", "..", "services", "ravel-cli", "src", "load", "hour_stride.rs")) as f:
            stride = f.read().splitlines()
        self.assertEqual(stride[310].strip(), "if max_event_ns >= bound_ns {")
        with open(os.path.join(HERE, "..", "..", "crates", "ravel-catalog", "src", "config.rs")) as f:
            self.assertEqual(f.read().splitlines()[7],
                             "pub const DEFAULT_MAX_INGEST_LAG_NS: i64 = 2 * 60 * 60 * 1_000_000_000;")
        why, rows = self.stride_check_744(GEN, True, out=self.path("even"))
        self.assertIsNone(why)
        hours = {}
        for r in rows:
            hours.setdefault(r["start_ts"] // HOUR_NS, set()).add(r["trace_id"])
        self.assertEqual(sorted(len(v) for v in hours.values()), [1] * 744)
        why, _ = self.stride_check_744(GEN, False, out=self.path("random"))
        self.assertRegex(why or "", r"^--ingest-hour-stride would refuse batch \d+ before submitting it: its latest "
                                    r"event time \d+ ns is not before \d+ ns, the end of its ingest hour \d+ plus "
                                    r"max_ingest_lag \(7200000000000 ns\) \(EventTimePastLag, "
                                    r"services/ravel-cli/src/load/hour_stride.rs:311\)$")
        inv = load_module(os.path.join(BOX, "lib", "inv.py"), "inv_stride_bound")
        bound = 11 * HOUR_NS + inv.MAX_INGEST_LAG_NS
        self.assertIsNone(inv.stride_refusal([bound - 1], 1, 10, 1, inv.MAX_INGEST_LAG_NS, 1))
        self.assertIn("batch 0 ", inv.stride_refusal([bound], 1, 10, 1, inv.MAX_INGEST_LAG_NS, 1))
        self.assertIn("deals 2 batches", inv.stride_refusal([0, 0, 0], 2, 10, 1, inv.MAX_INGEST_LAG_NS, 1))
        with open(GEN) as f:
            src = f.read()
        old = "        hour = i % hours if even else rng.randrange(hours)\n"
        self.assertEqual(src.count(old), 1)
        flipped = self.path("gen_corpus_flipped.py")
        with open(flipped, "w") as f:
            f.write(src.replace(old, "        hour = rng.randrange(hours)\n"))
        why, _ = self.stride_check_744(flipped, True, out=self.path("flipped"))
        self.assertIsNotNone(why)
        print(f"flip 'a random hour per trace' at gen_corpus.py:{src[: src.index(old)].count(chr(10)) + 1} fails "
              f"test_many_parts_744_strided_load_check: {why}", file=sys.stderr)

    def test_stride_check_that_cannot_run_is_not_a_refusal(self):
        # run.sh records exit 1 as the state not-run with the refusal on
        # stdout, and any other non-zero exit as a failed state with the
        # stderr log. A check that could not read its file must therefore
        # not exit 1: a missing Parquet file (or no pyarrow at all) exits 70
        # with its error on stderr and nothing on stdout, and a bad
        # --batch-rows exits 64 before any read.
        missing = self.path("no-such.parquet")
        p = subprocess.run([PY, os.path.join(BOX, "lib", "inv.py"), "stride-check", "--parquet", missing,
                            "--batch-rows", "10", "--batches", "3", "--ingest-hour-start", "497517"],
                           capture_output=True, text=True)
        self.assertEqual(p.returncode, 70, p.stderr)
        self.assertEqual(p.stdout, "")
        self.assertTrue(p.stderr.startswith("inv.py: stride-check: "), p.stderr)
        p = subprocess.run([PY, os.path.join(BOX, "lib", "inv.py"), "stride-check", "--parquet", missing,
                            "--batch-rows", "0", "--batches", "3", "--ingest-hour-start", "497517"],
                           capture_output=True, text=True)
        self.assertIn(p.returncode, (64, 70), p.stderr)
        self.assertEqual(p.stdout, "")
        with open(os.path.join(BOX, "run.sh")) as f:
            run_sh = f.read()
        # The mapping in run.sh: 1 is not-run with stdout, anything else fails
        # the state with the stderr log.
        self.assertIn('    1) not_run "many_parts_$n" "$why"; return 0 ;;\n'
                      '    *) failed_at "stride-check many_parts_$n" "$code" "$stride_log"; return 1 ;;\n', run_sh)

    def local_smoke_toml_with(self, extra_lines, suffix="run"):
        """A copy of run.local-smoke.toml with EXTRA_LINES appended to its
        [server] section."""
        with open(os.path.join(BOX, "run.local-smoke.toml")) as f:
            text = f.read()
        self.assertEqual(text.count("[server]\n"), 1)
        text = text.replace("[server]\n", "[server]\n" + extra_lines)
        p = self.path(f"{suffix}.toml")
        with open(p, "w") as f:
            f.write(text)
        return p

    def sourced_load_config(self, toml):
        """Sources run.sh under --dry-run, then calls load_config for real
        (DRY_RUN=0) with no server or store, and prints what it built."""
        out = self.path(f"out-{os.path.basename(toml)}")
        driver = self.path(f"driver-{os.path.basename(toml)}.sh")
        with open(driver, "w") as f:
            f.write(f"""set -u
source {os.path.join(BOX, "run.sh")} --dry-run --lane local --run-toml {toml} --out {out}
DRY_RUN=0
load_config
printf 'SERVER_FLAGS=%s\\n' "$SERVER_FLAGS"
printf 'SERVER_EXTRA=%s\\n' "${{SERVER_EXTRA[*]+${{SERVER_EXTRA[*]}}}}"
FINISHED=1
""")
        p = subprocess.run([BASH, driver], capture_output=True, text=True, timeout=60)
        return p.returncode, p.stdout + p.stderr

    def test_extra_flags_appear_on_server_command_line_and_stamp(self):
        toml = self.local_smoke_toml_with(
            'extra_flags = ["--sql-max-query-bytes=1048576", "--audit-log-sample-rate=1"]\n')
        code, text = self.sourced_load_config(toml)
        self.assertEqual(code, 0, text)
        self.assertIn("SERVER_EXTRA=--memory-budget-bytes 1000000000 --sql-max-query-bytes=1048576 "
                      "--audit-log-sample-rate=1", text)
        # SERVER_FLAGS is what start_server passes on the command line and
        # the only site (run.sh "server_flags=\"$SERVER_FLAGS\"") that stamps
        # it into <state>.server.json, so this one string proves both.
        self.assertIn(
            "SERVER_FLAGS=--mode query --store s3 --shards 2 --tenant-hash-unkeyed --audit-mode off "
            "--memory-budget-bytes 1000000000 --sql-max-query-bytes=1048576 --audit-log-sample-rate=1", text)

    def test_process_readers_return_none_for_a_gone_process(self):
        # A server that died mid-call leaves nothing for the "after" reading;
        # both readers return None on either platform, so cmd_call still
        # exits 75 rather than with a traceback. The darwin branch is forced
        # here so a Linux run checks it too.
        inv = load_module(os.path.join(BOX, "lib", "inv.py"), "inv_readers")
        gone = 2 ** 22 + 12345
        self.assertIsNone(inv.proc_cpu_s(gone))
        self.assertIsNone(inv.proc_hwm_bytes(gone))

        def refuse(pid):
            raise OSError(f"proc_pid_rusage({pid}) failed")

        inv.darwin_rusage = refuse
        platform = inv.sys.platform
        inv.sys.platform = "darwin"
        try:
            self.assertIsNone(inv.proc_cpu_s(gone))
            self.assertIsNone(inv.proc_hwm_bytes(gone))
        finally:
            inv.sys.platform = platform

    def test_a_body_cut_short_is_a_connection_failure(self):
        # A server killed after its headers (Content-Length promised, body
        # cut short) is an IncompleteRead, not an OSError: inv.py call still
        # exits 75 and writes no calls.jsonl line, so run.sh decides from the
        # process whether the server died.
        import socket
        import threading
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]

        def answer():
            conn, _ = srv.accept()
            conn.recv(65536)
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"da")
            conn.close()

        t = threading.Thread(target=answer, daemon=True)
        t.start()
        auth = self.path("auth")
        os.makedirs(auth, exist_ok=True)
        with open(os.path.join(auth, "inv.auth"), "w") as f:
            f.write("Authorization: Bearer x\n")
        calls = self.path("calls.jsonl")
        line = json.dumps({"statement": "s", "state": "unfolded", "temp": "cold", "try": 0, "tenant": "inv",
                           "kind": "trace_get", "path": "/api/traces/x"})
        p = subprocess.run([PY, os.path.join(BOX, "lib", "inv.py"), "call", "--line", line,
                            "--server", f"http://127.0.0.1:{port}", "--auth-dir", auth, "--pid", "0",
                            "--calls", calls], capture_output=True, text=True, timeout=60)
        t.join(5)
        srv.close()
        self.assertEqual(p.returncode, 75, p.stderr)
        self.assertIn("IncompleteRead", p.stderr)
        self.assertFalse(os.path.exists(calls) and os.path.getsize(calls))

    def test_extra_flags_bad_element_refused_before_anything_runs(self):
        # inv.py server-extra-flags refuses with 64; run.sh turns that into
        # FAILED naming the key, exit 1, before any step runs.
        toml = self.local_smoke_toml_with('extra_flags = ["sql-max-query-bytes=1048576"]\n')
        code, text = run_sh("local", toml, self.path("out"))
        self.assertEqual(code, 1, text)
        self.assertNotIn("STEP ", text)
        self.assertIn("run.toml server.extra_flags", text)
        self.assertIn("'sql-max-query-bytes=1048576' is not a single --flag token", text)

        toml2 = self.local_smoke_toml_with('extra_flags = ["--has space"]\n', suffix="run2")
        code2, text2 = run_sh("local", toml2, self.path("out2"))
        self.assertNotEqual(code2, 0, text2)
        self.assertNotIn("STEP ", text2)
        self.assertIn("'--has space' is not a single --flag token", text2)

    # Two statements for the death cases: a_fetch with a warm repeat after its
    # cold tries, b_fetch cold only, so a death can land on the last cold try,
    # on the warm call, or before another statement's cold calls.
    DEATH_STATEMENTS = ('[[statement]]\n'
                        'id = "a_fetch"\n'
                        'kind = "trace_get"\n'
                        'states = ["unfolded"]\n'
                        'truth_key = "ok"\n'
                        'cold_only = false\n'
                        '\n'
                        '[[statement]]\n'
                        'id = "b_fetch"\n'
                        'kind = "trace_get"\n'
                        'states = ["unfolded"]\n'
                        'truth_key = "ok"\n'
                        'cold_only = true\n')

    def died_run(self, cold_runs, die_at, script=None, tag="", warm_runs=1, inflight=False, toml=None,
                 then_empty_state=False):
        """run.sh's run_state for the unfolded state, with state_unfolded
        replaced by the statements() loop alone (no census or needle_blocks,
        which need a real store and CLI), against a stub server that answers
        normally except on the requests STUB_DIE_AT names, where it dies by
        SIGKILL with no response. INFLIGHT starts one server and runs every
        call --temp inflight (no restarts), as the compaction state does.
        THEN_EMPTY_STATE runs a second state afterwards whose function does
        nothing. TAG disambiguates a second run in the same test. Returns
        (exit, output, out dir, argv file of every server the stub started)."""
        stub = self.path(f"stub-server{tag}")
        with open(stub, "w") as f:
            f.write(STUB_SERVER_PY)
        os.chmod(stub, 0o755)
        statements_toml = self.path(f"statements{tag}.toml")
        with open(statements_toml, "w") as f:
            f.write(self.DEATH_STATEMENTS)
        truth = self.path(f"truth{tag}.json")
        with open(truth, "w") as f:
            json.dump({
                "saved_traces": [{"trace_id": "deadbeef"}],
                "failing_cell": {"service": "svc", "hour_start_ns": 0, "hour_end_ns": 1},
                "request_id": {"present": "r1", "absent": "r2"},
                "in_lists": {"trace_id": {}, "request_id": {}},
                "journey_c": {},
            }, f)
        out = self.path(f"out{tag}")
        count_file = self.path(f"stub-count{tag}")
        argv_file = self.path(f"stub-argv{tag}")
        driver = self.path(f"driver-died{tag}.sh")
        body = ("state_unfolded() { start_server || return 1; statements unfolded --temp inflight; }"
                if inflight else "state_unfolded() { statements unfolded; }")
        tail = ("state_erasure() { return 0; }\nrun_state erasure_in_flight\n" if then_empty_state else "")
        with open(driver, "w") as f:
            f.write(f"""set -u
source {script or os.path.join(BOX, "run.sh")} --dry-run --lane local --run-toml {toml or os.path.join(BOX, "run.local-smoke.toml")} --out {out}
DRY_RUN=0
load_config
STATEMENTS={statements_toml}
COLD_RUNS={cold_runs}
WARM_RUNS={warm_runs}
SERVER_BIN={stub}
write_secrets
HTTP_PORT="$(free_port)"
HEALTH_PORT="$(free_port)"
GRPC_PORT="$(free_port)"
mkdir -p "$OUT"
cp {truth} "$OUT/truth.json"
: >"$CALLS"
export STUB_DIE_AT="{die_at}"
export STUB_COUNT_FILE={count_file}
export STUB_ARGV_FILE={argv_file}
{body}
run_state unfolded
{tail}stop_server
printf 'STATES-DONE\\n'
FINISHED=1
""")
        p = subprocess.run([BASH, driver], capture_output=True, text=True, timeout=120)
        return p.returncode, p.stdout + p.stderr, out, argv_file

    def death_calls(self, out):
        return [(c["statement"], c["temp"], c["try"], c["outcome"])
                for c in read_jsonl(os.path.join(out, "calls.jsonl"))]

    def restart_keys(self, out):
        path = os.path.join(out, "stamps", "restarts.jsonl")
        lines = read_jsonl(path) if os.path.exists(path) else []
        for r in lines:
            # Every restart a cold call is measured after dropped the cache
            # (the local lane says why when it cannot).
            self.assertTrue(r.get("cache_drop"), r)
        return [(r["statement"], r["try"]) for r in lines]

    def test_server_death_is_recorded_and_the_run_moves_to_the_next_call(self):
        # A died try is not retried. a_fetch's 2nd of 3 cold tries dies (the
        # 2nd request); its 3rd cold try restarts the server for itself and
        # runs; nothing is restarted for the dead try, so restarts.jsonl holds
        # exactly one stamp per cold try, under that try's own key.
        code, text, out, _ = self.died_run(3, "2", warm_runs=0)
        self.assertEqual(code, 0, text)
        self.assertIn("STATES-DONE", text)
        self.assertFalse(os.path.exists(os.path.join(out, "FAILED")))
        self.assertEqual(self.death_calls(out), [
            ("a_fetch", "cold", 0, "ok"), ("a_fetch", "cold", 1, "server_died"), ("a_fetch", "cold", 2, "ok"),
            ("b_fetch", "cold", 0, "ok"), ("b_fetch", "cold", 1, "ok"), ("b_fetch", "cold", 2, "ok")])
        died = read_jsonl(os.path.join(out, "calls.jsonl"))[1]
        self.assertEqual(died["signal"], 9)
        self.assertEqual(died["reason"], "server died during a_fetch/unfolded try 1, signal 9")
        self.assertTrue(os.path.exists(os.path.join(out, died["log"][len("out/"):])), died["log"])
        self.assertEqual(self.restart_keys(out), [("a_fetch", 0), ("a_fetch", 1), ("a_fetch", 2),
                                                  ("b_fetch", 0), ("b_fetch", 1), ("b_fetch", 2)])
        old = '    [ "$code" = 75 ] && ! kill -0 "${SERVER_PID:-0}" 2>/dev/null || fail "call against an ' \
              'unreachable server"\n'
        flipped, line = self.flipped_box(old, '    fail "call against an unreachable server"\n')
        code2, text2, _, _ = self.died_run(3, "2", flipped, tag="-flip", warm_runs=0)
        self.assertNotEqual(code2, 0, text2)
        print(f"flip 'a died try fails the run instead of being recorded' at box/run.sh:{line} fails "
              f"test_server_death_is_recorded_and_the_run_moves_to_the_next_call: exit {code2}", file=sys.stderr)

    def test_death_on_the_last_cold_try_leaves_the_warm_call_not_run(self):
        # a_fetch's last cold try dies (request 2 of cold_runs 2). Its warm
        # repeat, defined by the server it follows, is recorded not-run with
        # the reason, and b_fetch's first cold call restarts the server for
        # itself: a restart for the dead try under its key, then no restart
        # for b_fetch's cold 0 (a warm server measured as cold), is the shape
        # this rules out.
        code, text, out, _ = self.died_run(2, "2")
        self.assertEqual(code, 0, text)
        self.assertEqual(self.death_calls(out), [
            ("a_fetch", "cold", 0, "ok"), ("a_fetch", "cold", 1, "server_died"), ("a_fetch", "warm", 0, "not-run"),
            ("b_fetch", "cold", 0, "ok"), ("b_fetch", "cold", 1, "ok")])
        warm = read_jsonl(os.path.join(out, "calls.jsonl"))[2]
        self.assertEqual(warm["reason"], "the server died earlier in unfolded and no cold call has restarted it")
        self.assertEqual(self.restart_keys(out), [("a_fetch", 0), ("a_fetch", 1), ("b_fetch", 0), ("b_fetch", 1)])
        flipped, line = self.flipped_box("    server_down=1\n", '    restart_for "$line"\n    server_down=0\n')
        code2, text2, out2, _ = self.died_run(2, "2", flipped, tag="-flip")
        self.assertEqual(code2, 0, text2)
        self.assertNotEqual(self.restart_keys(out2),
                            [("a_fetch", 0), ("a_fetch", 1), ("b_fetch", 0), ("b_fetch", 1)])
        print(f"flip 'restart for the dead try' at box/run.sh:{line} fails "
              f"test_death_on_the_last_cold_try_leaves_the_warm_call_not_run: restarts {self.restart_keys(out2)}",
              file=sys.stderr)

    def test_death_on_a_warm_call_stamps_no_restart(self):
        # The warm repeat dies (request 3 of cold_runs 2): it is recorded
        # server_died, and the next statement's cold calls restart for
        # themselves, one stamp each, none duplicating a cold key.
        code, text, out, _ = self.died_run(2, "3")
        self.assertEqual(code, 0, text)
        self.assertEqual(self.death_calls(out), [
            ("a_fetch", "cold", 0, "ok"), ("a_fetch", "cold", 1, "ok"), ("a_fetch", "warm", 0, "server_died"),
            ("b_fetch", "cold", 0, "ok"), ("b_fetch", "cold", 1, "ok")])
        self.assertEqual(self.restart_keys(out), [("a_fetch", 0), ("a_fetch", 1), ("b_fetch", 0), ("b_fetch", 1)])

    def test_death_on_an_inflight_call_leaves_the_rest_of_the_state_not_run(self):
        # Inflight calls run against the state's one server with no restart.
        # After that server dies (request 2), every later inflight call is
        # recorded not-run with the reason; nothing is restarted or stamped.
        code, text, out, _ = self.died_run(2, "2", inflight=True)
        self.assertEqual(code, 0, text)
        calls = self.death_calls(out)
        self.assertEqual(calls[:2], [("a_fetch", "inflight", 0, "ok"), ("a_fetch", "inflight", 1, "server_died")])
        self.assertTrue(calls[2:], calls)
        self.assertTrue(all(c[1] == "inflight" and c[3] == "not-run" for c in calls[2:]), calls)
        self.assertEqual(self.restart_keys(out), [])

    def test_three_deaths_stop_the_statement_not_the_state(self):
        # Three deaths across a_fetch's own tries (requests 1, 2 and 3; every
        # cold try restarts for itself in between) stop a_fetch: its remaining
        # tries are recorded not-run with the reason, and b_fetch still runs.
        # The state is not failed: the deaths reach the verdict through
        # evaluate.py's server_died problems instead.
        code, text, out, _ = self.died_run(5, "1,2,3", warm_runs=0)
        self.assertEqual(code, 0, text)
        self.assertEqual(self.death_calls(out), [
            ("a_fetch", "cold", 0, "server_died"), ("a_fetch", "cold", 1, "server_died"),
            ("a_fetch", "cold", 2, "server_died"), ("a_fetch", "cold", 3, "not-run"),
            ("a_fetch", "cold", 4, "not-run"),
            ("b_fetch", "cold", 0, "ok"), ("b_fetch", "cold", 1, "ok"), ("b_fetch", "cold", 2, "ok"),
            ("b_fetch", "cold", 3, "ok"), ("b_fetch", "cold", 4, "ok")])
        stopped = read_jsonl(os.path.join(out, "calls.jsonl"))[3]
        self.assertEqual(stopped["reason"],
                         "the server died 3 times during a_fetch in unfolded; its remaining tries were not sent")
        self.assertFalse(os.path.exists(os.path.join(out, "stamps", "failed_states.jsonl")))
        flipped, line = self.flipped_box('    [ "$deaths" -lt 3 ] || stopped="$stmt"\n', "")
        code2, text2, out2, _ = self.died_run(5, "1,2,3", flipped, tag="-flip", warm_runs=0)
        self.assertEqual(code2, 0, text2)
        self.assertNotIn(("a_fetch", "cold", 3, "not-run"), self.death_calls(out2))
        print(f"flip 'three deaths do not stop the statement' at box/run.sh:{line} fails "
              f"test_three_deaths_stop_the_statement_not_the_state", file=sys.stderr)

    def test_server_death_keeps_the_dying_log_and_the_states_last_log(self):
        # The dying server's own log lands under out/logs/ (the server_died
        # line names it), and run_state copies the state's last server log,
        # the one of the server that answered b_fetch's last call.
        code, text, out, argv_file = self.died_run(2, "2")
        self.assertEqual(code, 0, text)
        died_log = read_jsonl(os.path.join(out, "calls.jsonl"))[1]["log"]
        self.assertEqual(died_log, "out/logs/server-2.log")
        self.assertTrue(os.path.exists(os.path.join(out, "logs", "server-2.log")))
        last_log = os.path.join(out, "logs", "server-unfolded-last.log")
        self.assertTrue(os.path.exists(last_log))
        with open(argv_file) as f:
            servers = len(f.read().splitlines())
        self.assertEqual(servers, 4)
        with open(last_log, "rb") as f, open(os.path.join(out, "work", "server-4.log"), "rb") as g:
            self.assertEqual(f.read(), g.read())
        flipped, line = self.flipped_box('  keep_state_log "$state"\n', "")
        code2, text2, out2, _ = self.died_run(2, "2", flipped, tag="-flip")
        self.assertFalse(os.path.exists(os.path.join(out2, "logs", "server-unfolded-last.log")))
        print(f"flip 'run_state keeps no state log' at box/run.sh:{line} fails "
              f"test_server_death_keeps_the_dying_log_and_the_states_last_log", file=sys.stderr)

    def test_a_state_with_no_server_copies_no_earlier_log(self):
        # A state that neither starts a server nor sends a call (here a state
        # function that does nothing, after unfolded ran) copies no log under
        # its own name: the previous state's server log is not its log.
        code, text, out, _ = self.died_run(1, "", warm_runs=0, then_empty_state=True)
        self.assertEqual(code, 0, text)
        self.assertTrue(os.path.exists(os.path.join(out, "logs", "server-unfolded-last.log")))
        self.assertFalse(os.path.exists(os.path.join(out, "logs", "server-erasure_in_flight-last.log")))
        flipped, line = self.flipped_box('  STATE_SERVER_LOG=""\n', "")
        code2, text2, out2, _ = self.died_run(1, "", flipped, tag="-flip", warm_runs=0, then_empty_state=True)
        self.assertTrue(os.path.exists(os.path.join(out2, "logs", "server-erasure_in_flight-last.log")))
        print(f"flip 'the state log is not reset per state' at box/run.sh:{line} fails "
              f"test_a_state_with_no_server_copies_no_earlier_log", file=sys.stderr)

    def test_extra_flags_reach_the_server_command_line(self):
        # The flags run.toml [server] extra_flags names are on the argv of
        # every server start_server launches, after --memory-budget-bytes, as
        # written: "--x=*" is not glob-expanded against a file named
        # "--x=GLOBBED" in the working directory.
        toml = self.local_smoke_toml_with('extra_flags = ["--sql-max-query-bytes=1048576", "--x=*"]\n')
        open(os.path.join(self.path(""), "--x=GLOBBED"), "w").close()
        cwd = os.getcwd()
        os.chdir(self.path(""))
        try:
            code, text, out, argv_file = self.died_run(1, "", warm_runs=0, toml=toml)
        finally:
            os.chdir(cwd)
        self.assertEqual(code, 0, text)
        with open(argv_file) as f:
            argvs = [json.loads(line) for line in f]
        self.assertTrue(argvs)
        for argv in argvs:
            i = argv.index("--memory-budget-bytes")
            self.assertEqual(argv[i + 2:i + 4], ["--sql-max-query-bytes=1048576", "--x=*"], argv)
        flipped, line = self.flipped_box(' ${SERVER_EXTRA[@]+"${SERVER_EXTRA[@]}"} </dev/null', " </dev/null")
        code2, text2, _, argv2 = self.died_run(1, "", flipped, tag="-flip", warm_runs=0, toml=toml)
        with open(argv2) as f:
            self.assertNotIn("--sql-max-query-bytes=1048576", f.read())
        print(f"flip 'extra flags left off the server command' at box/run.sh:{line} fails "
              f"test_extra_flags_reach_the_server_command_line", file=sys.stderr)

    def test_server_last_log_is_truncated_past_2mb(self):
        # Deliverable 2: keep_state_log truncates to the last 2000 lines once
        # the server log passes 2 MB, and says so on its first line.
        out = self.path("out")
        work = self.path("work")
        os.makedirs(work, exist_ok=True)
        server_log = os.path.join(work, "server-1.log")
        with open(server_log, "w") as f:
            for i in range(60000):
                f.write(f"line {i} padding padding padding padding padding padding padding\n")
        self.assertGreater(os.path.getsize(server_log), 2 * 1024 * 1024)
        driver = self.path("driver-trunc.sh")
        with open(driver, "w") as f:
            f.write(f"""set -u
source {os.path.join(BOX, "run.sh")} --dry-run --lane local --run-toml {os.path.join(BOX, "run.local-smoke.toml")} --out {out}
DRY_RUN=0
mkdir -p "$OUT"
STATE_SERVER_LOG={server_log}
keep_state_log truncstate
FINISHED=1
""")
        p = subprocess.run([BASH, driver], capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        kept = os.path.join(out, "logs", "server-truncstate-last.log")
        with open(kept) as f:
            lines = f.read().splitlines()
        self.assertEqual(lines[0], f"... truncated to the last 2000 lines (original "
                                   f"{os.path.getsize(server_log)} bytes) ...")
        self.assertEqual(len(lines), 2001)
        self.assertEqual(lines[-1], "line 59999 padding padding padding padding padding padding padding")


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
