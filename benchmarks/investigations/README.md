# Selective investigations

Issue #3699, ADR-3692 decision 8 (T1). An in-tree measurement harness for
"selective" read paths: a point lookup by trace id or request id, an
IN-list, a sparse tag/body needle, a narrow-join, against a corpus that
mimics 20M-trace-scale spans+logs data, across the key-index state matrix
(unfolded, folded without leaves, folded with leaves, mixed, many-parts).
Every figure it reports is checked against a pre-registered band in
`bands.toml`, which encodes the 17 rows of ADR-3692's Acceptance table.

This is not a `ravel-bench` report (see `docs/internal/bench-reports.md`
for those): it is a standalone box flow, because it needs a real S3-backed
`ravel-server` and `ravel-cli load`/`catalog fold` runs, not an in-process
benchmark.

## Files

- `gen_corpus.py` -- seeded corpus generator. Writes `spans.jsonl` /
  `logs.jsonl` always, `spans.parquet` / `logs.parquet` when `pyarrow` is
  importable (see "Parquet availability" below), mapping TOMLs for
  `ravel-cli load`, and `truth.json` (the planted answers every statement
  in `statements.toml` is checked against).
- `statements.toml` -- the statement set: id, table, SQL (or the
  trace-by-id GET), which states it runs in, its `truth.json` key, whether
  it only runs cold, whether current `main` supports it, and which
  Acceptance-table row(s) it feeds.
- `bands.toml` -- the 17 Acceptance-table rows as named figures with
  `target`/`miss` expressions, evaluated over a `value` variable.
- `box/run.sh` -- the one-run box flow: bootstrap, store, corpus, declare
  tenants, load, state matrix (stamp before/after every transition),
  statement execution, upload, done/failed marker. `--dry-run` prints
  every step without touching a store, a server, or AWS.
- `box/launch.sh` / `box/fetch.sh` -- thin EC2 launch/poll wrappers for the
  real-S3 lane, matching `benchmarks/clickbench/kit/launch.sh` /
  `fetch.sh`'s `--dry-run` shape.
- `evaluate.py` -- reads `calls.jsonl` + `truth.json` + the state-matrix
  stamps + load/fold reports, computes every figure `bands.toml` names
  exactly once, and writes `results.json` + `summary.txt`. Fails the run
  (nonzero exit, `missing <figure>` / `duplicate <figure>` in `errors`) if
  a figure is absent or reported more than once -- see "Class closure"
  below.
- `kit.test.sh` -- the harness's own test suite (gen_corpus correctness,
  evaluate.py fixture-based correctness, `run.sh --dry-run` step ordering,
  `statements.toml` validity). Runs in under two minutes with no AWS
  access and no running server; wired into CI's `doc-scripts` job.

## Launching a run

### Local / RustFS lane

For iterating on the harness itself, not for a reportable figure (no real
S3 request/latency behavior):

```sh
bash benchmarks/investigations/box/run.sh run.local.toml
```

### Real-S3 lane

```sh
bash benchmarks/investigations/box/launch.sh run.s3.toml
# box/run.sh runs on the launched instance (via its user-data), then:
bash benchmarks/investigations/box/fetch.sh run.s3.toml ./out
```

Both `launch.sh` and `run.sh` accept `--dry-run`, which prints every `aws`
or `ravel-cli` command they would run (via the same `run()`/`capture()`
wrapper shape as `benchmarks/clickbench/kit/launch.sh`) and performs no
network or filesystem action beyond reading `run.toml`.

## run.toml keys

| Key | Meaning | Default |
|---|---|---|
| `sha` | git sha to build `ravel-cli`/`ravel-server` at | required |
| `lane` | `"local"` or `"s3"` | `"local"` |
| `bucket`, `region`, `prefix`, `run_id` | s3 lane only | required for `lane = "s3"` |
| `states` | comma-separated state-matrix entries to run | `"unfolded,folded_no_leaves,folded_leaves"` |
| `control` | run the ClickBench `hits` control corpus too | `false` |
| `tenant_spans`, `tenant_logs` | tenant names to load into | `"inv-spans"`, `"inv-logs"` |
| `shards` | `ravel-cli load --shards` | `4` |
| `corpus.traces`, `corpus.hours`, `corpus.logs_per_trace`, `corpus.seed`, `corpus.scale` | passed through to `gen_corpus.py` | see `gen_corpus.py --help` |

`many_parts` in `states` is accepted but always recorded as
`unsupported-on-this-build`: it needs T2's `--ingest-hour-stride`, which is
not on `main` yet.

## Pre-registration

Before a reportable run (one whose figures will be cited on the epic or an
Acceptance-table row), write the expected figures and the band that would
count as a miss as a comment on issue #3699 first, per the repository's
measurement-discipline rule (see the root `CLAUDE.md`, "Measurement and
cost discipline"). `bands.toml` is the pre-registration for the
Acceptance-table rows themselves; a run against a different corpus size or
tenant shape needs its own pre-registration comment, since the bands
assume the `corpus.*` defaults above.

## Stamps

`box/run.sh` writes `out/stamps/<state>.before.json` and
`out/stamps/<state>.after.json` around every state-matrix transition
(wall-clock seconds since epoch, plus the state name), and the state
transition itself writes its own report under `out/` (`load_spans.json`,
`load_logs.json`, `fold_no_leaves.json`, `fold_leaves.json` -- produced by
`ravel-cli load` / `ravel-cli catalog fold`, not by this harness).
`evaluate.py` reads these to compute the fold-duration and leaf-overhead
figures (Acceptance rows 9-10) and to confirm a load or fold happened
before the state whose statements depend on it.

## Lane separation

A result file is always named `results.<lane>.json` /
`summary.<lane>.txt`, never a bare `results.json`, so a local-lane
iteration run can never be mistaken for (or silently overwrite) a real-S3
reportable run sitting in the same `out/` directory.

## Unsupported statements

`statements.toml` marks a statement `supported = false` when current
`main` cannot run it; `run.sh` records
`<id>/<state>: unsupported-on-this-build` and continues rather than
failing or silently skipping it, and `evaluate.py` reports its figures
with the string value `"unsupported-on-this-build"` (verdict
`UNSUPPORTED` in `summary.txt`) instead of a number. As of this writing
that is every `*_in_trace_id_*`, `*_in_request_id_*`, and
`journey_b_logs_by_trace_ids` statement: `main` has no IN-list equality
pushdown for `trace_id`/`request_id` yet.

## Block-size assumption

`gen_corpus.py` plants needles (request id, tag/body frequency tiers) at
block granularity using its own `BLOCK_ROWS = 256` constant. This is the
generator's own assumption, not read from `ravel-logseg`'s real writer
block size, and is deliberately disjoint from production segment geometry;
a needle's planted block count is exact by construction (the generator
chooses disjoint block indices directly), so the 20% tolerance ADR-3692
allows for needle-frequency figures is headroom, not something the
generator relies on to hit its target.

## Parquet availability

`gen_corpus.py` always writes `spans.jsonl` / `logs.jsonl` (pure stdlib).
It additionally writes `spans.parquet` / `logs.parquet` when `pyarrow` is
importable, and records whether it did so in `truth.json`
(`spans_parquet_written` / `logs_parquet_written`). A real box run is
expected to have `pyarrow` available (bootstrap installs it) so
`ravel-cli load --parquet` always gets genuine Parquet; `kit.test.sh` runs
the generator's correctness checks against the JSONL output so the test
suite does not require `pyarrow` to be installed.

## Class closure

Every figure named in `bands.toml` must appear in `results.json`'s
`figures` list exactly once. `evaluate.py`'s `check_bands()` is the single
function that reads that list and decides presence/duplication; every
number `evaluate.py` computes is appended to that one list and reaches
`results.json`/`summary.txt` only through it, so a figure the code forgets
to compute fails as `missing <name>` and a figure computed twice (by two
code paths, or a fixed and a per-state path both wired to the same name)
fails as `duplicate <name>`, rather than one silently overwriting the
other. `kit.test.sh` demonstrates both failure shapes directly against
`check_bands()`, and demonstrates two specific wrong evaluators the real
check rules out (see that file's comments for the exact flips).
