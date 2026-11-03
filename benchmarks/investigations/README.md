# Selective investigations

Issue #3699, ADR-3692 decision 8 (T1 of epic #3692). A measurement harness
for the selective read paths of NOFire AI's telemetry store: a lookup by
trace id or request id, an IN list, a rare, sparse or common needle, and the
narrow joins of the investigation journeys. It runs them against a seeded
spans and logs corpus in each state of the key-index matrix (unfolded,
folded without leaves, folded with leaves, mixed, many parts), judges every
answer against the generator's oracle, and judges every figure against
`bands.toml`, which transcribes the 17 rows of ADR-3692's Acceptance table.

This is not a `ravel-bench` report: it drives the real binaries, `ravel-cli
load`, `ravel-cli catalog fold` and a restarted `ravel-server`, against an
S3-compatible store, from an empty bucket to `results.<lane>.json`.

## Files

- `gen_corpus.py`: seeded corpus generator. Writes `spans.jsonl` and
  `logs.jsonl` (`--format jsonl`), `spans.parquet` and `logs.parquet`
  (`--format parquet`, needs pyarrow), the two `ravel-cli load` mapping files,
  and `truth.json`, the exact answer of every planted key. Its module
  docstring states the time and determinism rules.
- `statements.toml`: the statements, one `[[statement]]` each: tenant, table,
  SQL or trace GET, the states it runs in, its `truth.json` key, whether it
  runs cold only, and `unsupported_reason` when this build cannot run it.
- `bands.toml`: ADR-3692 Acceptance rows 1 to 17, the ADR's cells verbatim,
  each with the figures evaluate.py extracts and the `target_if` and
  `miss_if` expressions it judges them with. Its header documents the fields.
- `hits.truth.json`: the oracle of the two statements on the ClickBench hits
  tenant (q20 and the control set).
- `box/run.sh`: one run, from an empty store to `results.<lane>.json`.
  `--dry-run` prints every step, command and stamp in order and runs nothing.
- `box/launch.sh`, `box/fetch.sh`: start the s3 lane's EC2 instance, and
  wait for and download its `out/`. Both take `--dry-run`.
- `box/lib/inv.py`: run.sh's helpers (TOML reads, the call plan, one call,
  stamps, the restart warmup wait).
- `box/run.example.toml`: the s3 lane's run file, every key commented.
  `box/run.local-smoke.toml` is the local lane's.
- `evaluate.py`: reads a run directory and writes `results.<lane>.json` and
  `summary.<lane>.txt`.
- `investigations.test.sh`, `investigations_cases.py`: the harness's cases.
- `fixtures/local-smoke/`: a committed local-lane run (see below).

## Lanes

Results are always `results.<lane>.json` and `summary.<lane>.txt`, so a local
run never overwrites or reads as an s3 one.

### s3 lane (the only lane timing is judged on)

Fill in a copy of `box/run.example.toml` (`box.sha`, `box.bucket`,
`box.store_bucket`), then:

```sh
bash benchmarks/investigations/box/launch.sh run.toml     # prints the run id
bash benchmarks/investigations/box/fetch.sh run.toml RUN_ID
```

`launch.sh` uploads this directory and `run.toml` to
`s3://<bucket>/<prefix>/runs/<run-id>/in/`, checks the instance role's
permissions, and starts one on-demand instance (default `c6a.4xlarge`; no
spot) with `--instance-initiated-shutdown-behavior terminate`. Its user-data
arms `shutdown -h +<box.max_minutes>` (default 600, at most 600) before
anything else, writes `/root/inv/RUN_ID`, and runs `run.sh --lane s3`.
`run.sh` checks out `box.sha` detached, builds `ravel-cli` and `ravel-server
--features sql` in separate `cargo build --release --locked` runs with frame
pointers on, installs pyarrow, runs the flow against `box.store_bucket`
through the instance role, uploads `out/`, and terminates the instance, on
success and failure alike: an error trap writes `out/FAILED` with the step
that failed. The shutdown timer terminates the instance whatever run.sh is
doing. `fetch.sh` polls for `out/DONE` or `out/FAILED` and gives up
`box.max_minutes + 30` minutes after it starts.

At the ADR's 20,000,000 traces and 20 cold runs, each cold call costing a
server restart, a run is not known to fit inside 600 minutes; the first
s3 run should be pre-registered with that as an open question.

### local lane (counters only)

A loopback RustFS started by run.sh itself, and release binaries built
beforehand:

```sh
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release --locked -p ravel-cli
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release --locked -p ravel-server --features sql
bash benchmarks/investigations/box/run.sh --lane local \
  --run-toml benchmarks/investigations/box/run.local-smoke.toml \
  --out /tmp/inv-local --bin-dir target/release --rustfs /path/to/rustfs \
  [--pylib DIR-with-pyarrow]
```

Build the two packages in separate commands: one `cargo build -p ravel-cli
-p ravel-server --features ravel-server/sql` does not build ravel-server
with its SQL surface. The local lane drops the page cache only when run as
root; otherwise each restart stamp says `"cache_drop": "unavailable (not
root)"`. It reports timing figures but never judges them.

## The flow

`run.sh` steps, in order: `bootstrap` (build stamp), `store` (`store qualify`; the
local lane creates a fresh bucket in its own RustFS, the s3 lane refuses a
`store_bucket` that holds any object), `host`, `corpus` (gen_corpus.py at
`run.toml [corpus]`), `declare` (the tenant's typed attribute columns from
the mappings, `request_id` as key index, `--index-trace-id true`),
`load-base`, one step per state, `evaluate`, `finish` (`out/DONE` with the
verdict). The states:

- `unfolded`: the loads only.
- `folded_no_leaves`: `catalog fold --writers-stopped` per signal.
- `folded_leaves`: `catalog fold --key-index-leaves` per signal, then
  `catalog fold --rebuild-key-index`. The leaves fold right after a plain
  fold has nothing left to seal and writes no leaf; the rebuild writes them
  (stamped as transition `leaf_rebuild`).
- `mixed`: waits for the ingest hour after the last fold's (at most
  `run.mixed_wait_max_minutes`), loads an extra hour of new traces, runs the
  statements over folded and unfolded data together, then folds that hour
  (`fold_advance`).
- `many_parts`: unsupported on this build (see below).

In each state, every statement planned for it runs `run.cold_runs` times
cold, each call after a server stop, a page-cache drop and a server start
that waits for `/readyz` and for the server's startup cache warmup to end,
then `run.warm_runs` times warm, without a restart, unless the statement is
`cold_only`. Every call runs with `RQ_STATS=1` and writes one line to
`calls.jsonl`. The server runs `--mode query`: `--mode all` runs scheduled
folds and compaction that would move the state under the measurement, so
run.sh stops the server before every fold.

The startup warmup loads recent parts into the cache before the first call,
so a "cold" call means a fresh process after a page-cache drop, not an empty
query cache. Each restart stamp records what the warmup loaded.

## run.toml

`box/run.example.toml` comments every key. In short: `[run]` (lane, states,
`cold_runs`, `warm_runs`, `hits`, `scales`, `baseline`,
`mixed_wait_max_minutes`), `[box]` (s3 lane only: `sha`, `repo_url`,
`region`, `bucket`, `prefix`, `store_bucket`, `instance_profile`,
`instance_type`, `volume_gb`, `max_minutes`), `[corpus]` (gen_corpus.py's
`traces`, `hours`, `seed`, `spans_per_trace`, `logs_per_trace`),
`[server]` (`tenant`, `shards`, `memory_budget_bytes`), `[hits]`,
`[unsupported_states]` (state = reason), `[prices]` and `[cost]`.

`[prices]` holds `get_per_1000`, `put_per_1000` (PUT and LIST),
`storage_gb_month` and `instance_per_hour`, each beside a `<key>_source`
naming where the number was read. A price without a number is `missing`; a
price without a source makes `cost.figures_without_basis` non-zero, which
misses row 17. `[cost]` holds `lookups_per_part_month`, the assumed query
frequency, with its `_source`, and `amortise_months`.

## Stamps

What the run proves about its own state, under `out/stamps/`. evaluate.py
reports `missing stamps/<name>` for any it needs and does not find.

| Stamp | Proves |
|---|---|
| `build.json` | the commit and how the binaries were built, with their sha256 |
| `store.json` | the store bucket was qualified and empty at the start |
| `host.json` | the machine (arch, nproc, memory, disk), `RQ_STATS=1`, and whether the page cache can be dropped |
| `corpus.json` | the generator's parameters and the sha256 of `truth.json` |
| `declare.json` | the tenant's declared columns and key index, as `tenant show` printed them |
| `loads.jsonl` | one line per load (hour, signal): exit code, ingest hour and the load report |
| `folds.jsonl` | one line per fold (transition, signal): exit code, the `--json` report, wall time, CPU seconds and peak RSS |
| `census.jsonl` | one line per state: object count and bytes per key class, listed from the store |
| `transitions.jsonl` | one line per state entered: when, and in which ingest hour |
| `restarts.jsonl` | one line per cold call: the server pid, the cache-drop result and the warmup |

## Evaluation

```sh
python3 benchmarks/investigations/evaluate.py --run OUT [--out-dir DIR]
```

Each answer is judged against `truth.json` under the truth key
`statements.toml` gives the call: a row count or row set that differs, or a
call naming another key, is `wrong_answer`. Every figure `bands.toml` names
goes through one list and is present exactly once: absent is `missing`, two
inputs for it (calls or stamps) are `duplicate` and neither is used, a
figure this build cannot produce is `unsupported-on-this-build` with the
reason, and the figures of a row whose tenant, states, scales or baseline
the run lacks are `not-run` with the reason. `timing` figures are judged on
the s3 lane and only reported on the local one. Exit 0 only when nothing is
missing, duplicated, wrong or missed; the output is a pure function of the
run directory, so re-evaluating it is byte-identical.

Metrics a band can name: `cold.max.<m>` and `cold.min.<m>` (over the cold
tries; `<m>` from the call's stats phases and members, such as `total_gets`,
`data_gets`, `wire_bytes`, `leaf_gets`), `warm.max.<m>`, `cold.rows_exact`
(1 when every try's answer was correct), `cold.rows_match_state.<state>`,
`cold.wall_p50_s`, `fold.<signal>.<field>` (from the fold report;
`leaf_bytes_pct` is leaf bytes over the signal's own object bytes in the
state's census, `new_entries` the difference from the previous fold),
`matrix.*`, `cost.*`, `scale.*` and `control.*`.

Cost: per (statement, state), the mean cold call's GETs, LISTs and server
CPU seconds times the named prices; per fold, its report's requests and its
measured CPU; per state, storage from the census. Break-even compares each
statement folded with leaves against unfolded at the assumed
`lookups_per_part_month`, which the summary prints beside every figure.

## Unsupported on this build

- `many_parts`: needs `ravel-cli load --ingest-hour-stride`, which this build
  lacks. Every call planned for it is a `calls.jsonl` line with outcome
  `unsupported-on-this-build` and the reason from `[unsupported_states]`.
- `spans_in_1000`, `logs_request_in_1000`: the SQL surface refuses a
  statement over 1,000 complexity tokens, and a 1,000-literal IN list counts
  1,001. `statements.toml` carries the reason; run.sh records the call as
  unsupported instead of sending it.
- Key-index stats (`keyIndex.leafGets`, `partsCovered`, `keysProbed`),
  `pruning.skipIdxDropped`, a per-object breakdown, and the fold report's
  `key_index_build_passes` and `head_bytes_per_part_field`: absent from this
  build's output, so their figures are unsupported with that reason.
- Row 13's negative controls: decision 9's mutants are not built here.

Rows 5, 6 and 16 need `hits = true`; row 12 needs scales 10 and 100, and row
16 a baseline build. This flow loads scale 1 and builds one commit, so those
rows report `not-run`.

## Pre-registration

Before a reportable run, write the expected figures and the band that would
count as a miss as a comment on epic #3692, per the root `CLAUDE.md`
"Measurement and cost discipline". `bands.toml` is the pre-registration of
the Acceptance rows at the ADR's corpus; a run at another corpus size needs
its own comment.

## Corpus

Sizes, hours and the needle tiers are `gen_corpus.py` parameters; the ADR's
corpus is `--traces 20000000 --hours 24`. Needles are planted by row index
against the generator's own 256-row blocks: the common needle fills whole
blocks covering 10 % of them, the sparse one 0.1 % of rows outside those,
the rare one a single row. Those block frequencies are approximate with
respect to the store's real segment geometry, which the generator does not
read. Issue #3699's spec, not the ADR, allows them 20 % of slack;
`truth.json` records the exact counts every answer is judged against.

## fixtures/local-smoke

A local-lane run of `box/run.local-smoke.toml` on an aarch64 machine (4
cores, 8 GB): 1,000 traces over 2 hours, 2 cold runs, every state, against a
loopback RustFS. Its `calls.jsonl`, `truth.json` and `results.local.json`
are gzipped to keep the fixture under 200 KB; `summary.local.txt` and the
stamps are plain. Its verdict is `fail`, from two counter misses at this
scale: `row_07.spans_section_gets` (8 ranged GETs against a target of
3 x 2 new entries + 1 previous leaf = 7) and `row_08.logs_leaf_pct` (leaf
bytes 33.7 % of the logs objects against a 2.5 % target stated for the ADR's
corpus, measured here over 5,000 log rows). They are the run's real figures.

`investigations.test.sh` re-evaluates it and requires byte-identical output.
After a deliberate change to evaluate.py, `bands.toml` or `statements.toml`,
regenerate the expected output and review the summary diff:

```sh
python3 benchmarks/investigations/investigations_cases.py --refresh-local-smoke
```

## Tests

`bash benchmarks/investigations/investigations.test.sh`, in CI's
`doc-scripts` job: no AWS, no server, under two minutes. It runs `bash -n`
over every script, generator recounts at 1,000 traces (and at scale 10),
the shape of `statements.toml` and `bands.toml`, evaluate.py on the fixture
and on mutations of it (missing, duplicate, miss, wrong answer, exact cost),
two wrong evaluators that must fail those cases, and the dry runs of
`run.sh`, `launch.sh` (with an `aws` stub that fails if called) and
`fetch.sh`.
