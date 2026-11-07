# Selective investigations

Issue #3699, ADR-3692 decision 8 (T1 of epic #3692). A measurement harness
for the selective read paths of NOFire AI's telemetry store: a lookup by
trace id or request id, an IN list, a rare, sparse or common needle, and the
narrow joins of the investigation journeys. It runs them against a seeded
spans and logs corpus in each state of the key-index matrix (see "States"),
judges every answer against the generator's oracle, and judges every figure
against `bands.toml`, which transcribes the 18 rows of ADR-3692's Acceptance
table.

This is not a `ravel-bench` report: it drives the real binaries, `ravel-cli
load`, `ravel-cli catalog fold`, `ravel-cli maintain compact-tenant`,
`ravel-cli erase` and a restarted `ravel-server`, against an S3-compatible
store, from an empty bucket to `results.<lane>.json`.

## Files

- `gen_corpus.py`: seeded corpus generator. Writes `spans.jsonl` and
  `logs.jsonl` (`--format jsonl`), `spans.parquet` and `logs.parquet`
  (`--format parquet`, needs pyarrow), the two `ravel-cli load` mapping files,
  and `truth.json`, the exact answer of every planted key. `--tail` writes
  the mixed state's extra hour, with planted answers of its own.
  `--even-hours` puts exactly traces / hours traces in every hour (the
  many_parts_N corpora). Its module docstring states the time and
  determinism rules.
- `statements.toml`: the statements, one `[[statement]]` each: tenant, table,
  SQL or trace GET, the states it runs in, its `truth.json` key, whether it
  runs cold only, and `requires` when it needs a planted key the corpus may
  not hold.
- `bands.toml`: ADR-3692 Acceptance rows 1 to 18. The text of each row's
  `check`, `target`, `miss` and `source` cells is the ADR's, verbatim; the
  `target_if` and `miss_if` expressions are transcriptions of that text into
  figures this harness extracts, and where a transcription had to choose, a
  comment above the figure says what it chose. Its header documents the
  fields.
- `hits.truth.json`: the pre-registered figures of the two statements on the
  ClickBench hits tenant (q20's predicted row count and the control set).
- `box/run.sh`: one run, from an empty store to `results.<lane>.json`.
  `--dry-run` prints every step, command and stamp in order and runs nothing.
- `box/launch.sh`, `box/fetch.sh`: start the s3 lane's EC2 instance, and
  wait for and download its `out/`. Both take `--dry-run`.
- `box/lib/inv.py`: run.sh's helpers (TOML reads, the call plan, one call,
  stamps, inspect stamps, the census, the truth merge, the restart warmup
  wait, and `stride-check`, a strided load's per-batch event-time check run
  over the whole corpus before the load).
- `box/run.example.toml`: the s3 lane's run file, every key commented.
  `box/run.local-smoke.toml` is the local lane's.
- `evaluate.py`: reads a run directory and writes `results.<lane>.json` and
  `summary.<lane>.txt`.
- `investigations.test.sh`, `investigations_cases.py`: the harness's cases.
- `fixtures/local-smoke/`: a committed local-lane run (see below).

## Lanes

Results are always `results.<lane>.json` and `summary.<lane>.txt`, so a local
run never overwrites or reads as an s3 one.

### S3 lane

The only lane timing is judged on.

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
pointers on, installs pyarrow 17.0.0 (T12's pin; the generator uses only
`pyarrow.Table` and `pyarrow.parquet.write_table`, and `inv.py
stride-check` only `pyarrow.parquet.read_table` and an int64 cast, which
17.0.0 has), runs
the flow against `box.store_bucket` through the instance role, and
terminates the instance, on success and failure alike: an error trap writes
`out/FAILED` with the step that failed (a failed command inside a state
fails that state only; see "The flow").

`run.toml [run] max_minutes` (default 540) is the run's own deadline, an
hour inside the shutdown timer: a state that would start after it is
recorded `not-run` with reason `deadline`, a state already running when it
passes records each of its remaining calls `not-run` ("run deadline
(max_minutes) passed before this call") without restarting the server for
them, and evaluate and the upload still happen. Before each long step (a
fold, a leaf rebuild, a load, a compaction, and the waits for the next
ingest hour and for 310 s into an hour) run.sh checks that the time left
holds the step's bound: the waits are bounded by their maxima, the other
steps by `[step_estimate_minutes]` (per state, else `default`, 30 minutes
when the table is absent). A step the time left cannot hold records its
state `not-run` with reason `deadline: <step> needs up to N s, M s left of
run.max_minutes` before it starts. Each of these makes the verdict
`incomplete`. The upload copies `out/` without the marker first, then writes
`DONE` (or `FAILED`) as its own final PUT, only after that copy exited 0.
`fetch.sh` polls for `out/DONE` or `out/FAILED`, gives up `box.max_minutes +
30` minutes after it starts, and refuses a prefix with neither marker unless
`--partial` is given.

#### Runtime estimate

From the smoke's own figures (`fixtures/local-smoke/`): a cold-call restart
(server stop, start, `/readyz`, warmup) took 0.875 s on average over its 221
restarts, and a call 0.032 s on average over its 244 timed calls. The s3
lane's default states plan, at 20 cold runs and 1 warm run per statement
that is not `cold_only` (statement counts from `statements.toml`):

| state | statements | cold | warm |
|---|---|---|---|
| unfolded | 36 | 720 | 3 |
| folded_no_leaves | 5 | 100 | 3 |
| folded_leaves | 35 | 700 | 3 |
| mixed | 22 | 440 | 3 |
| advanced_leaves | 6 | 120 | 0 |
| compaction_in_flight | 4 | 80 (one restart) | 1 |
| compaction_done | 4 | 80 | 1 |
| erasure_in_flight | 4 | 80 | 1 |
| many_parts_744 | 2 | 40 | 2 |
| many_parts_2160 | 2 | 40 | 2 |

That is 2,419 calls and 2,321 restarts: 2,321 x 0.875 s = 2,031 s, plus
2,419 x 0.032 s = 77 s, so 2,108 s, about 35 minutes of calls. The fixed
waits add at most 70 minutes for mixed (the next ingest hour;
`run.mixed_wait_max_minutes`, which `box/run.sh` defaults to 70) and at most
310 s, about 5 minutes, for compaction_in_flight (until 310 s into an hour;
in the smoke they took 13 and 4 minutes). The estimate is 2,108 + 4,200 +
310 = 6,618 s, about 110 minutes, against the 540-minute
deadline, and it covers calls and waits only. It omits corpus generation,
every load (the `hits` tenant's included) and every fold at 20,000,000
traces: the smoke's figures for them (0.30 to 1.03 s per load, at most
0.27 s per fold) are fixed-cost dominated and do not scale, and neither
ADR-3692 nor ADR-2707's T12 amendment records a duration for any of them
(T12 records GETs, bytes and lookup times). The nearest recorded figures
are ADR-2707's Stage 0, on 200 M spans: a load of 110 s with 1 M-row
batches and 597 s at the old default, and a fold of 2,411 s after
compacting that default's 80,000 objects, a geometry the default load no
longer produces
(`docs/adrs/2707-exact-key-index-for-spans-and-logs.md:39-41`). A call on
S3 at the ADR's corpus is slower than a call on loopback at 20,000 traces;
at ten times the smoke's restart and call time the calls alone take 446
minutes. Measured on box A (run `investigations-20261104T001915Z-46224-17727`,
main 3465163df, 2,000,000 traces, real S3; from its `progress.log`): the
two release builds took 13 minutes, corpus generation 4 minutes, the
`unfolded` state 57 minutes, `folded_leaves` 53, `mixed` 33 including the
wait for the next hour, the compaction state 9 and the erasure state 6. If
a run reaches the deadline, drop these
states from `run.toml` first, in this order: `many_parts_2160` (2,160
strided ingest hours loaded and folded one part per hour), `many_parts_744`,
then `erasure_in_flight`; rows 10, 9 and 13's erasure figure then report
`not-run`.

### Local lane

Counters only. A loopback RustFS started by run.sh itself, and release
binaries built beforehand:

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
with its SQL surface. On macOS, where there is no `/proc`, `inv.py call`
reads the server's CPU seconds and its lifetime peak physical footprint (in
place of VmHWM) from libproc's `proc_pid_rusage`, and `host.json` takes
memory from `hw.memsize`; give run.sh a `python3` with pyarrow on `PATH`
(or `--pylib`). The local lane drops the page cache only when run as
root; otherwise each restart stamp says `"cache_drop": "unavailable (not
root)"`. It reports timing figures but never judges them.

## The flow

`run.sh` steps, in order: `bootstrap` (build stamp), `store` (`store
qualify`; the local lane creates a fresh bucket in its own RustFS, the s3
lane refuses a `store_bucket` that holds any object), `host`, `corpus`
(gen_corpus.py at `run.toml [corpus]`, the mixed state's tail hour and one
corpus per many_parts state, merged into one `truth.json`), `declare` (per
tenant: the typed attribute columns from the mappings, `request_id` as key
index on inv with `--index-trace-id true`, `UserID` on hits, each with
`--readers-rolled-out`), `load-base`, one step per state, `evaluate`,
`finish` (`out/DONE` with the verdict).

With `run.hits = true` the hits tenant is loaded with T12's recipe
(`--batch-rows 100000 --target-bytes 1850000000 --max-flush-delay 30s
--pipeline-depth 32 --load-memory-bytes 6500000000`), and every fold, leaf
rebuild, inspect stamp and census line covers each loaded tenant and signal,
hits included. The census's `objects` for a tenant and signal counts its data
objects only, the `.rseg` keys under `<signal>/l0/` and `<signal>/l1/`
(`is_data_key` in `box/lib/inv.py`); commit records, the provenance object and
erasure markers are classes of their own. Row 6 bands the hits tenant's logs
count in the unfolded state at exactly 398 (`row_06.hits_objects`), and caps
its keyIndex GETs at 2 x that count.

### States

- `unfolded`: the base loads only.
- `folded_no_leaves`: `catalog fold --writers-stopped` per tenant and signal.
- `folded_leaves`: `catalog fold --key-index-leaves`, then `catalog fold
  --rebuild-key-index` (stamped as transition `leaf_rebuild`). The leaves
  fold right after a plain fold has nothing left to seal and writes no leaf;
  the rebuild writes them.
- `mixed`: waits for the ingest hour after the last fold's (at most
  `run.mixed_wait_max_minutes`), loads the tail hour (traces with planted
  answers of their own: saved trace ids, a request id, needle rows, failing
  cell traces), and runs the statements over the folded hours and the
  unsealed tail together, with ids from both. Its oracle is the base answer
  plus the tail's. Then it folds that hour (`fold_advance`, `catalog fold
  --writers-stopped --json --key-index-leaves`, the leaf switch on).
- `advanced_leaves` (ADR-3692 row 18): runs after `mixed`, against the
  catalog `fold_advance` left, and folds nothing itself, so the leaves have
  been through one advance and carry its extra slices. It stamps the state
  as every leaf state is stamped (census, catalog, object, kidx), then runs
  `inv.py advance-check`, which fails the state (`STATE-FAILED`, step
  `advance-check advanced_leaves`) unless two things hold: `fold_advance`
  folded spans and logs with `--key-index-leaves` and exited 0 (its
  `folds.jsonl` lines), since a fold with the switch off re-encodes the
  parts and drops their leaf refs; and the logs `request_id` field carries
  more slices per part in the state's catalog stamp than in the
  `folded_leaves` one, stamped after the leaf rebuild, so the state really
  measures an advanced tail. It prints the slices per (part, field) of both
  stamps. Then it sends the `IN` lists of 10 and 100 keys on logs
  `request_id`, logs `trace_id` and spans `trace_id` (`logs_request_in_10`,
  `logs_request_in_100`, `logs_trace_in_10`, `logs_trace_in_100`,
  `spans_in_10`, `spans_in_100`). Their oracle is the base answer under the
  same truth key: the state holds the base hours plus the tail hour, and the
  tail is a separate generator run (seed + 1) whose planted ids are its own
  (`req-planted-tail-0001`, its saved traces and failing cell), so a listed
  key gains rows from the tail only if a tail trace id equals a base one (a
  128-bit draw) or, for a request id, shares its first 16 hex digits and row
  index (`request_id_of`). A key that did would return more rows than the
  oracle and read as a wrong answer, not a pass. The local smoke lists the
  state so its flow and its check run there, and it is expected to fail
  the second check: the smoke's 150,000 logs rows, base and tail, are
  150,000 `request_id` pairs, under the 4,000,000-pair share each of the
  two logs fields gets of the default 8,000,000-pair budget
  (`DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS`), so the advance takes one pass and
  adds no slice (the committed fixture's `folded_leaves` and `fold_advance`
  stamps both carry one `request_id` leaf).
- `compaction_in_flight`: waits until every earlier hour is sealed (310 s
  into the hour), starts `maintain compact-tenant` on both signals at once as
  background children, runs the statement set while they run (temp `inflight`,
  against one restarted server), waits for the child, and stamps
  `compaction.jsonl`. `compaction_done` runs the same set after it. The
  oracle of both passes is the unfolded state's own answer.
- `erasure_in_flight`: `erase submit` on the erasure victim's request id,
  `erase status` before and after (the run fails unless it reads pending),
  then the statement set against the pre-erasure answer, except
  `logs_erasure_victim`, the victim's own lookup, which must return no row:
  a snapshot resolved after the request's ack filters its records while it
  is pending (`docs/catalog-and-mvcc.md`, "Snapshot resolution by listing",
  step 6; `truth_key_in` in statements.toml). That step backs 0 rows under
  PENDING for a resolve by listing only: the tenant here is folded, and
  "Snapshot resolution from a folded snapshot" in the same document does not
  say it attaches pending erasures. That gap in the document is filed
  separately; the smoke observed 0 rows on its folded tenant, so the
  expectation stands on the run's own reading. The pass after the
  erasure is applied is not run: nothing in this flow applies a pending
  request. A corpus with no victim row (every row read by some other
  statement) records the state `not-run` with that reason; the smoke's
  20,000 traces have one.
- `many_parts_N` (744 and 2,160 on the s3 lane, 24 in the smoke): a fresh
  spans tenant `mpN` loaded with `--ingest-hour-stride 1` and an
  `--ingest-hour-start` that puts the last hour below the current one, one
  batch per hour, with no fold during the loads (the load refuses once a HEAD
  appears), then folded with `--writers-stopped --snapshot-part-max-entries
  1` so each hour is its own part, then leaves rebuilt. The stamped part
  count is banded (rows 9 and 10).

  The corpus is generated with `gen_corpus.py --even-hours`: exactly
  `many_parts.traces_per_hour` traces in every event hour, written in hour
  order, so the load's fixed `--batch-rows` (rows / N) makes batch k exactly
  event hour k, written into ingest hour k + 1. Before each batch the
  loader refuses one whose latest event time (a span's `end_ts`) is not
  before its ingest hour's end plus `max_ingest_lag`, 2 hours
  (`HourStrideBatchError::EventTimePastLag`,
  `services/ravel-cli/src/load/hour_stride.rs:311`; the lag is
  `DEFAULT_MAX_INGEST_LAG_NS`, `crates/ravel-catalog/src/config.rs:8`).
  Without `--even-hours` every trace takes a random hour, the hours hold
  unequal row counts, and the batches drift off their hours by about the
  square root of the trace count: box A's many_parts_744 (7,440 traces)
  exited 1 six seconds into its load, and a local reproduction at 744
  traces refused batch 23 with `--ingest-hour-stride refused batch 23
  before submitting it: its latest event time 1791182427140145961 ns is not
  before 1791180000000000000 ns, the end of its ingest hour 497547 plus
  max_ingest_lag (7200000000000 ns)`: its latest row is from event hour
  497550, four hours after hour 497546, the one batch 23 should hold.
  At 24 hours the drift stays inside the lag, which is why many_parts_24
  passed. The cause was the harness's corpus, not the loader or the write
  path. Before the load `run.sh` runs the loader's check over the whole
  file (`box/lib/inv.py stride-check`, the same bound per batch); a corpus
  it would refuse records the state `not-run` with that refusal, instead
  of loading a partial tenant; a check that could not run at all (no
  pyarrow, an unreadable file: exit 70, its error on stderr) fails the
  state with that log, since an unchecked corpus is not a refused one.
  With the fix, the local lane with `states =
  ["unfolded", "many_parts_744"]` at 1,000 traces and `traces_per_hour = 1`
  loaded 744 objects into ingest hours 497525 to 498268 and measured row
  9 (744 covering parts, 747 resolve GETs, exact rows).

`mixed` is followed by `fold_advance` and `compaction_in_flight` by
`compaction_done`; each derived state is live exactly when its parent is. A
state the run lists and does not reach is `not-run` with the reason, and a
state absent from both the results and the not-run list is a missing figure.

A load, fold, leaf rebuild, compaction or erasure command that exits
non-zero inside a state fails that state, not the run: run.sh copies the
command's log to `out/logs/`, appends a `stamps/failed_states.jsonl` line
(the state, the step, the command's exit code, the log's `out/logs/` path),
prints `STATE-FAILED`, stops the server and goes on with the next state,
then evaluates and uploads as usual. evaluate.py reports every figure of
that state, and of the state derived from it, `not-run` with the reason
`state failed at <step> (exit N)`, lists the state as `failed` with its
step, exit code and log, and the verdict reads `incomplete` naming it. A
failed state's store is left as the command left it (a half-loaded tenant,
one signal folded), and later states measure what is there, as their own
census and catalog stamps record. A failure in bootstrap, store, host,
corpus, declare or load-base, where nothing can be measured without the
step, still ends the run with `out/FAILED`, which names the failed
command's log under `out/logs/` when it has one.

A server that dies mid-call (the connection is refused, reset or cut
short rather than answered with an HTTP status, and the process is gone)
loses that one try, not the run: run.sh keeps the dying server's log under
`out/logs/server-<N>.log` and appends a `calls.jsonl` line with outcome
`server_died` (the statement, state, try, temp, the signal the server died
on, and that log's path), then moves on to the next planned call; the dead
try is not repeated (`test_server_death_is_recorded_and_the_run_moves_to_the_next_call`).
Nothing is restarted for the dead try: the next cold call restarts the
server for itself, as every cold call does, so `restarts.jsonl` holds one
stamp per cold try under that try's own key, and a warm or inflight call
reached while the server is down is recorded `not-run` (`the server died
earlier in <state> and no cold call has restarted it`), since it is defined
by the server it follows (`test_death_on_the_last_cold_try_leaves_the_warm_call_not_run`,
`test_death_on_a_warm_call_stamps_no_restart`,
`test_death_on_an_inflight_call_leaves_the_rest_of_the_state_not_run`).
Three deaths across one statement's own tries stop that statement: its
remaining tries are recorded `not-run` with the reason and the state goes
on with its next statement (`test_three_deaths_stop_the_statement_not_the_state`).
evaluate.py reports every `server_died` call as a `server_died` problem
naming the statement, state, temp and try, whether or not a band figure
reads that call and whether or not its state is live, so the verdict is
`fail` (`test_server_died_everywhere`); a figure the call would have fed
reads `missing` with reason `server died during <statement>/<state> try N,
signal S`. At the end of every state, whether it failed or not, the log of
the last server it started or sent a call to is copied to
`out/logs/server-<state>-last.log` (past 2 MB, only its last 2,000 lines,
with a first line noting the truncation and the original size); a state
that started no server and sent no call copies none
(`test_server_death_keeps_the_dying_log_and_the_states_last_log`,
`test_a_state_with_no_server_copies_no_earlier_log`,
`test_server_last_log_is_truncated_past_2mb`).

In each state, every statement planned for it runs `run.cold_runs` times
cold, each call after a server stop, a page-cache drop and a server start
that waits for `/readyz` and for the server's startup cache warmup to end,
then `run.warm_runs` times warm, without a restart, unless the statement is
`cold_only`. Every call runs with `RQ_STATS=1` and writes one line to
`calls.jsonl`. The server runs `--mode query`, which runs no scheduled fold
or compaction (`--mode all` would move the state under the measurement, and
ravel-server refuses `--disable-fold` in query mode, so the flag is not
passed); run.sh stops the server before every transition.

### Placeholders

A statement's SQL names planted keys as `{placeholders}`, filled from
`truth.json` by `box/lib/inv.py plan`: `{trace}` (saved trace `run`),
`{cell_service}`, `{cell_start_ns}`, `{cell_end_ns}`, `{request_present}`,
`{request_absent}`, `{in_trace_ids_x_N}` (the trace id IN lists, as binary
literals, used on spans and on logs) and `{in_request_ids_N}` (the
request id IN lists, as string literals),
`{erasure_victim_rid}`, and `{jc_start_ns_K}` and `{jc_cut_ns_K}` (journey
C's build windows). A placeholder left unfilled stops the run.

### Cold and warm

The ADR-0046 startup warmup loads recent parts into the cache before the
first call, and the server skips it only with the cache disabled, which
this harness does not do. So a "cold" call means a fresh process after a
page-cache drop, not an empty query cache, and some cold calls are served
partly from that cache: in the smoke the logs journeys read about every
logs byte from it and issue no scan GET. Every cold call whose
`cacheServedBytes` is above 0 is flagged `warm_served` in the results and
counted per statement and state in the summary: in the smoke, 126 of the 196
cold calls that returned an answer (of 220 cold calls; the other 24, 12 on the 1,000-key lists
and 12 on their over-cap controls, were refused by the SQL complexity guard
and carry no stats). The flag is a reported figure, not a problem. Each restart stamp
records what the warmup loaded.

## run.toml

`box/run.example.toml` comments every key. In short: `[run]` (lane, states,
`max_minutes`, `cold_runs`, `warm_runs`, `hits`, `scales`, `baseline`,
`mixed_wait_max_minutes`, `needle_blocks_max_objects`, `split_base_load`),
`[box]` (s3 lane only: `sha`, `repo_url`, `region`, `bucket`, `prefix`,
`store_bucket`, `instance_profile`, `instance_type`, `volume_gb`,
`max_minutes`), `[corpus]` (gen_corpus.py's `traces`, `hours`, `seed`,
`spans_per_trace`, `logs_per_trace`), `[server]` (`tenant`, `shards`,
`memory_budget_bytes`, `extra_flags`), `[hits]`, `[many_parts]`
(`traces_per_hour`, `snapshot_part_max_entries`), `[prices]` and `[cost]`.

`[server] extra_flags` is a list of extra `ravel-server` command-line
flags, appended verbatim after `--memory-budget-bytes` on every server
command line and stamped into `<state>.server.json`'s `server_flags` as
given (`test_extra_flags_reach_the_server_command_line`,
`test_extra_flags_appear_on_server_command_line_and_stamp`). Each element
is one argv token, passed with no shell evaluation, no word splitting and
no globbing: a flag that takes a value is one `--flag=value` element, not
two. An element that contains whitespace or does not start with `--` is
refused before any step runs: `inv.py server-extra-flags` exits 64 naming
the element, and run.sh ends with `FAILED` naming `run.toml
server.extra_flags`, exit 1
(`test_extra_flags_bad_element_refused_before_anything_runs`).

`[prices]` holds `get_per_1000`, `put_per_1000` (PUT and LIST),
`storage_gb_month` and `instance_per_hour`, each beside a `<key>_source`
naming where the number was read. A price without a number is `missing`; a
price without a source makes `cost.figures_without_basis` non-zero, which
misses row 17. `[cost]` holds `lookups_per_part_month`, the assumed query
frequency, with its `_source`, and `amortise_months`, the months a leaf
build is amortised over. It has no default: absent or not a whole number of
at least 1, it is the problem `missing` and the maintenance figures read
`missing`.

## Stamps

What the run proves about its own state, under `out/stamps/`. evaluate.py
reports `missing stamps/<name>` for any it needs and does not find;
`store.json` and `corpus.json` are always required.

| Stamp | Proves |
|---|---|
| `build.json` | the commit and how the binaries were built, with their sha256 |
| `store.json` | the store bucket was qualified and empty at the start |
| `host.json` | the machine (arch, nproc, memory, disk), `RQ_STATS=1`, and whether the page cache can be dropped |
| `corpus.json` | the generator's parameters for every sub-corpus and the sha256 of `truth.json` |
| `declare.json` | each tenant's declared columns and key index, as `typed-attr-column show` printed them |
| `tenants.json` | the tenants the run loaded and their store prefixes |
| `loads.jsonl` | one line per load (hour, tenant, signal): exit code, ingest hour and the load report |
| `folds.jsonl` | one line per fold (transition, tenant, signal): exit code, the `--json` report, wall time, CPU seconds and peak RSS |
| `census.jsonl` | one line per state: object count and bytes per tenant, signal and key class, listed from the store |
| `transitions.jsonl` | one line per state entered: when, and in which ingest hour |
| `restarts.jsonl` | one line per cold call: the server pid, the cache-drop result and the warmup |
| `STATE.SIG.catalog.json` | `catalog inspect` before the state's first statement: HEAD id, part count, entries per part, the leaves each part carries |
| `STATE.SIG.object.json` | `rspan inspect` or `rlog inspect` of one object named from the census: format version, sections, KEY_IDX presence, footer length |
| `STATE.SIG.kidx.json` | `inspect kidx` of one leaf, when the state has leaves |
| `STATE.SIG.tenant.txt` | `typed-attr-column show` at that state |
| `STATE.server.json` | the server binary's sha256 and its flags |
| `needle_blocks.json` | the logs needles' block counts as `rlog inspect` reads them, beside the generator's |
| `compaction.jsonl`, `erasure.jsonl`, `not_run.jsonl` | the compaction child's reports, the erase submit and status, and each state recorded not-run |
| `failed_states.jsonl` | each state a failed command stopped: the step, the command's exit code and the `out/logs/` path of its log |

Covering parts, entry counts and leaf presence come from the catalog stamps,
never from a fold report. A state whose catalog stamp is absent is the
missing figure `covering_parts.STATE.SIG`.

## Evaluation

```sh
python3 benchmarks/investigations/evaluate.py --run OUT [--out-dir DIR]
```

Each answer is judged against `truth.json` under the truth key
`statements.toml` gives the call, or, in the compaction and erasure states,
against the unfolded state's answer to the same statement and try: a row
count or row set that differs, or a call naming another key, is
`wrong_answer`. q20's oracle is likewise the unfolded run's own rows;
`hits.truth.json`'s predicted 4 rows is reported beside it as the
pre-registered figure. Every figure `bands.toml` names goes through one
list and is present exactly once: absent is `missing`, two inputs for it
(calls or stamps) are `duplicate` and neither is used, and the figures of a
row whose tenant, states, scales or baseline the run lacks are `not-run`
with the reason.

Only an allowlist of fields this build does not report may read as
`unsupported-on-this-build`, and only when the call itself succeeded:
`stats.keyIndex` and its members (`partsCovered`, `partsUncovered`,
`partsBeyondCeiling`, `leavesRejected`, `reservationsRefused`,
`leafGetRequests`, `keysProbed`), `pruning.skipIdxDropped`, the per-object
and per-part breakdowns, and the fold report's `key_index_build_passes`,
`key_index_sections_read`, `key_index_leaf_gets` and
`head_bytes_per_part_field`. Any other absent field (`phases`, `pruning`,
`signals`, `io`, `server_cpu_s`, `server_rss_peak`, `rows`, the duration)
is `missing`, and no figure defaults to 0. A figure read from
`segmentsOpened` (rows 5, 6 and 14, "objects opened") whose value is 0
while the same call's scan phase issued a GET is `dead_counter
segmentsOpened STATEMENT STATE`, a problem: on this build the rlog and rspan
scan paths never increment that counter.

`timing` figures (rows 2, 5, 15, 16 and 18) are judged on the s3 lane and only
reported on the local one. The output is a pure function of the run
directory, so re-evaluating it is byte-identical.

The verdict is `fail` when anything is missing, duplicated, wrong, refused,
dead, not in flight or missed. Otherwise it is `incomplete` when the run
proved less than it was asked: a state `run.toml` lists that did not run, a
planned call that was not sent in a state that did (the run deadline, an
in-flight window too short), a figure `not-run` for a reason other than
its row or figure needing a tenant, state, scale or baseline the run never
asked for, or a row 18 cell `not-measured` (below). Each reason is listed under `incomplete` in the results and the
summary. The rows left out that way are not silent either: each is listed
with the setting that leaves it out (`hits = false`, `baseline = false`, a
state or scale `run.toml` does not list) under `not_requested` in the results and on the
summary's `not requested:` line. A gated figure whose row is listed for
another reason goes there too, by name (`row 9 figures covering_parts, ...`
when `many_parts_744` is not in `run.toml`'s states), so every gated figure is
measured, `not_requested` or `incomplete`. Otherwise it is `pass`. Exit 0 only on `pass`; `DONE` carries the
verdict.

Every in-flight call must overlap the compaction of the signal its
statement reads: the call's `[started_unix_s, started_unix_s + wall_s]`
against the compaction measure's `[started_unix_s, ended_unix_s]`. When
the compaction finished before the first call ("compaction finished in N ms
before the first call") or ran shorter than one call, no call could overlap
it, and the statement's in-flight calls are `not-run` with that reason. Any
other partial overlap is the problem `not_in_flight`.

Metrics a band can name: `cold.max.<m>` and `cold.min.<m>` (over the cold
tries; `<m>` from the call's stats phases and members, such as `total_gets`,
`data_gets`, `wire_bytes`, `leaf_gets`, `probe_wire_per_object`),
`warm.max.<m>`, `cold.rows_exact` (1 when every try's answer was correct),
`cold.rows_match_state.<state>`, `cold.wall_p50_s`, `cold.wall_iqr_s` (the
75th less the 25th percentile of the cold tries' wall times),
`cold.leaf_wall_p50_s` (row 18, below),
`cold.control_1025` (row 11's control, below), `stamp.<what>.<label>`
(covering parts, entry counts, previous leaves, footer length, leaf
buckets), `stamp.slices.<label>.<field>` (the most leaves one part carries
for the field), `leaf_share.<label>.<field>`,
`census.<tenant>.<signal>.objects|bytes|objects_over_512k`,
`fold.<tenant>.<label>.<field>`, `truth.<path>` (an integer `truth.json`
holds), and `derived` figures over those. A stamp field the inspector did
not print is `missing`, never 0.

Some bands, as transcribed:

- Row 3: probe wire per object at most 8,192 B, miss over 16,384 B. The
  denominator comes from the stamps, never from the call: the unfolded
  spans data objects whose event range overlaps the call's window, read
  from `stamps/unfolded.spans.ranges.json` (written by `inv.py
  inspect-stamp --ranges-keys`, one `{key, bytes, min_ns, max_ns}` per
  object). A call that reports a `perObject` breakdown must list exactly
  that many entries, or the figure is `missing`; an absent ranges stamp is
  `missing` too.
- Row 6: keyIndex GETs at most 2 x the census's hits objects and at least
  one per hits object above 512 KiB (the census's `objects_over_512k`): the
  logs keyed plan probes every object past that size (#3117). Objects read
  past the probe are the scan phase's `segmentsOpened`
  (`cold.max.scan_segments_opened`), not the sum over the phases: the keyed
  plan counts each object whose footer its KEY_IDX probe decoded as opened
  in the plan phase, absent key or not, so the sum counts the probed
  objects too. Box C's q20 (run
  `investigations-20261106T154511Z-47463-23195`, its `calls.jsonl`) read
  497 as that sum: 493 opened in the plan phase with 493 GETs, 492 of them
  pruned by the key index, and 4 opened in the scan phase with 6 GETs and
  989,537 B. The figure is 4 (`check_row_6_objects_past_probe`), which
  still misses the band's holders (1); that tenant's census holds 4 objects
  at or under 512 KiB, which the keyed plan reads without a probe.
- Row 7, judged per build pass (the #3704 amendment's form, with the pass
  factor of the pass-end slice amendment): ranged section GETs
  (`key_index_ranged_gets`) at most 2 x (new + uncovered entries) x
  `key_index_build_passes`, plus one per such entry and pass whose footer
  exceeds 8 KiB, miss at 2 x tail entries x passes, the from-scratch
  rebuild; the previous leaves' GETs (`key_index_leaf_gets`, read once per
  fold) equal to the previous leaves; and, as a second signal, the fold's
  section reads per pass (`key_index_sections_read` over the passes) at most
  new + uncovered, miss at the tail's entry count. A total against 2 x tail
  entries cannot tell a rebuild from extra passes: box D's logs advance read
  22 new entries a pass over 27 passes, 1,188 ranged GETs, over 2 x 570 =
  1,140 with no rebuild (`check_row_7_box_d`), while one pass reading all
  570 entries still misses on both signals (`check_row_7_rebuild`).
  Uncovered is the fold report's `key_index_uncovered_entries`. New
  entries, previous leaves and footer lengths come from the stamps; `rlog
  inspect` and `rspan inspect` print a footer length, so the footer term is
  real (0 at the smoke's 199 and 248 bytes). The miss reads "over" as at
  least: a from-scratch rebuild is exactly 2 x tail entries per pass, so
  equality is its signature. A build whose fold report carries no
  `key_index_build_passes` cannot be judged per pass, so its section GETs
  read `unsupported-on-this-build`.
- Row 8: leaf bytes per field over the label's census data bytes, banded at
  2.5 % on logs and 6 % on spans, with the field named in the figure. The
  ADR's band was measured on one repeated-key column (hits `UserID`, 2.06
  %); the `request_id` leaf holds one key per row and is over that band on
  any build, so its miss carries the reason "key cardinality equals the row
  count".
- Row 11: rows exact and `keysProbed` equal to the list size at 1, 10, 100
  and 1,000 keys, leaf GETs at most 1 + min(distinct buckets, 64) and leaf
  bytes at most entry_count x 11 KiB per covered part, and an over-cap
  control on each of `spans_in_1000`, `logs_trace_in_1000` and
  `logs_request_in_1000`: a list of 1,025 keys, one over decision 6's
  k = 1,024 (`spans_in_1025`, `logs_trace_in_1025`, `logs_request_in_1025`).
  `inv.py merge-truth` builds each list as the 1,000-key list, the
  generator's `control_extra` key (one the list does not hold) and ids the
  corpus does not hold up to 1,025 (`truth.json` `over_cap`): request ids
  `req-over-cap-absent-NNNN`, which the generator's `req-<16 hex>-<row>`
  and `req-planted-*` forms never produce, and trace ids whose high 8
  bytes are 1, 2, 3 and on and whose low 8 bytes are zero, as many as the
  list needs (24 beside a `control_extra` key, 25 when the 1,000-key list
  already names every trace), skipping a high half a listed id already
  holds. The planner dedups a list on each key's key8 before it counts the
  keys (`KeySet::new`), and an Id16's key8 is its high 8 bytes, so padding
  that differed only in the low half would be one key and leave the list
  under the cap; every over-cap list holds 1,025 distinct key8s
  (`test_over_cap_lists_hold_over_1024_distinct_key8`). A random 128-bit
  draw lands on a padding id with negligible odds and, if one did, would
  add rows and read as a wrong answer, not a pass. The padding adds no row,
  so the oracle (`in_trace_id.1025.*`,
  `in_request_id.1025`) is the generator's answer for the 1,000-key list
  plus `control_extra`. The current build admits the list and does not
  route it: the SQL complexity guard charges a flat literal list one unit
  plus one per 64 literals and refuses only past 4,096 literals
  (`MAX_IN_LIST_LITERALS`), so the statement costs 25 units, and the
  planner carries no key set past 1,024 keys (`KEY_SET_MAX_KEYS`; the
  spans `TRACE_ID_SET_MAX` and logs `KeyIn::new` both drop it), so the scan
  has no key arm and the residual filter answers it with `keysProbed` 0 and
  0 leaf GETs (`services/ravel-server/tests/sql_key_in.rs`,
  `in_list_rows_equal_the_union_of_equalities`). The control is 1 (pass)
  when every 1,000-key call reports `keysProbed` 1000 and `partsCovered`
  and every 1,025-key call is served by a visible scan with no keyIndex
  figure above zero; 0 (miss) when a 1,025-key call is index-served
  (`keysProbed`, `partsCovered` or `leafGetRequests` non-zero), is served
  with no visible path, or is refused while the 1,000-key list was
  admitted, or when the 1,000-key list probed another count;
  `refused-by-guard` when the 1,000-key list was refused by the SQL
  complexity guard, and `unsupported-on-this-build` when it carries no
  tier-2 figures.
- Row 15: the sum of the three steps' p50 under 5 s, miss over 20 s, and the
  journey's GET total within resolve + 2 x covering parts on spans + (1 +
  min(distinct buckets, 64)) per covering part on logs + 50 x (footer +
  SKIP_IDX + block runs) on spans + the logs holders' block reads. The last
  term comes from `truth.json`, not from the measured call: the failing
  cell's traces (`failing_cell.count`, the holders) x the logs rows planted
  per trace (`generator.logs_per_trace`), each row at most one block read.
- Row 18, key-set lookup time, one cell per field and list size (logs
  `request_id`, logs `trace_id` and spans `trace_id`, at 10 and 100 keys):
  the cell's `advanced_leaves` p50 over its cold calls
  (`cold.leaf_wall_p50_s`) at most U + max(0.05 x U, 0.5 x IQR), U the
  `unfolded` p50 of the same statement (`cold.wall_p50_s`) and IQR its
  unfolded interquartile range (`cold.wall_iqr_s`), both from the same
  run; over that bound is a miss. A cell any of whose cold calls left a
  covering part `Uncovered` (`partsCovered` 0, or `partsUncovered`,
  `skippedByBudget`, `partsBeyondCeiling`, `leavesRejected` or
  `reservationsRefused` above 0) reads `not-measured` whatever its time:
  its time is the tier-1 probe's, so it is neither a hold nor a miss, and
  it makes the verdict at best `incomplete`. With either state absent the
  row reads `not-run` with the reason, as the committed fixture's does. The
  row also reports the slices per (part, field) the lookups touched
  (`stamp.slices`), and the logs `request_id` count after the leaf rebuild
  beside it. Box C, before #3917, measured logs `request_id` at 100 keys at
  7.64 s through the leaves against 3.01 s unfolded, a miss
  (`check_row_18` cases).

Cost: per (statement, state), the mean cold call's GETs, LISTs and server
CPU seconds times the named prices. Server CPU is `/proc/<pid>/stat` utime
plus stime, which advance in 10 ms ticks, so a single cheap call can read
0 s; each cost line also carries `server_cpu_s_cold_sum`, the sum over the
cold tries, the per-statement figure the tick distorts least. Amortised maintenance per signal is
decision 8's: the leaf rebuild report's `ranged_gets` (section GETs) at the
GET price plus its `attempts[].leaf_puts` at the PUT price, over
`amortise_months`, plus the folded_leaves catalog stamp's leaf bytes in GB
at the storage price, per stamped part and month. Break-even per statement
and signal is that maintenance over (unfolded cost per lookup minus folded
cost per lookup), "not applicable" when the folded cost is not below the
unfolded one, at the assumed `lookups_per_part_month`, which the summary
prints once, above the break-even lines. Each break-even line also carries
the folded arm's cost per lookup with maintenance amortised in
(`dollars_folded_leaves_with_maintenance`: the folded cost per call plus
maintenance per part-month over `lookups_per_part_month`, the frequency
named in `maintenance_per_lookup_basis`); the break-even itself is
unchanged.

## Unsupported on this build

- Key-index stats (`stats.keyIndex` and its members),
  `pruning.skipIdxDropped`, the per-object and per-part breakdowns, and the
  fold report's `key_index_build_passes`, `key_index_sections_read`,
  `key_index_leaf_gets` and `head_bytes_per_part_field`:
  absent from this build's output, so their figures are unsupported with
  that reason (the allowlist above). The many_parts leaf-GET figures are
  among them: main has no tier-2 reader.
- The 1,000- and 1,025-key IN lists (`spans_in_1000`, `logs_trace_in_1000`,
  `logs_request_in_1000` and their `_1025` controls) are sent. A build from
  before ADR-3692's key set amendment refuses them with HTTP 400 and
  `statement complexity 1001 exceeds the maximum of 1000 tokens; simplify the
  statement`, and `fixtures/local-smoke` is a run of such a build. That run
  sent the harness's earlier 1,001-key controls; its six control call
  groups were renamed to the `_1025` statement ids and truth keys when the
  control moved, and their `restarts.jsonl` stamps to the same statement
  ids, and nothing else in them changed: the guard refused them before
  their length mattered, with the same text it gives every list past its
  cap (below). These
  statements carry `expect_refusal_on_main = true`, and evaluate.py records
  each such refusal as `refused-by-guard` ("refused by the SQL complexity
  guard") with that text, a class of its own: the summary's `refusal classes`
  line counts its figures and calls apart from `unsupported-on-this-build`,
  and neither is a problem. A refusal of any other statement is the problem
  `refused` with its status and text. The flag is read only when the server
  refuses, so on a build that admits the lists it changes nothing.
  Before the amendment the SQL complexity guard charged one unit per token
  start: an identifier or number run is one unit, an opening quote is one,
  and an `X'..'` literal is an X run plus a quote, so an element costs 3
  units with its comma in an X list and 2 in a quoted one. Against
  `MAX_STATEMENT_COMPLEXITY = 1_000` and the 8 units of the rest of the
  statement, an X list (3n + 8) held at most 330 keys and a quoted list
  (2n + 8) at most 496. The guard stops counting once the count passes the
  cap (`scan` returns `stop_above + 1`), so for these lists, which add one
  unit at a time, it reported 1001 whatever the list's length (a hint
  comment adds 2 or 3 units in one step, so a refusal can also read 1002 or
  1003): the figure in a refusal is where the scan stopped, not the
  statement's count. Box A's s3 run
  (`investigations-20261104T001915Z-46224-17727`, main 3465163df) refused
  its 1,000-key lists with exactly that text, which says nothing of one unit
  per key or of a cap one key below 1,000. Measured 2026-11-04 against a
  release `ravel-server --features sql` built from main c322a283c on the
  local lane: an X list of 330 keys is admitted and one of 331 refused, a
  quoted list of 496 admitted and one of 497 refused, and a 999-key list of
  either kind refused with the same `statement complexity 1001`. Since the
  amendment a flat list of literals costs one unit plus one per 64 literals,
  rounded up (`in_list_units` in `crates/ravel-sql/src/complexity_guard.rs`),
  up to 4,096 literals, so each of these statements costs 24 units at 1,000
  keys and 25 at 1,025, and is admitted.
- Row 13's negative controls: decision 9's mutants are not built here, so
  the figure is `not-run` ("mutant controls not implemented") and the
  verdict is at best `incomplete`.

Rows 5, 6 and 16 need `hits = true`; row 12 needs scales 10 and 100, and row
16 a baseline build. This flow loads scale 1 and builds one commit, so rows
12 and 16 report `not-run`, and rows 5 and 6 do too unless `hits = true`.

## Pre-registration

Before a reportable run, write the expected figures and the band that would
count as a miss as a comment on epic #3692, per the root `CLAUDE.md`
"Measurement and cost discipline". `bands.toml` is the pre-registration of
the Acceptance rows at the ADR's corpus; a run at another corpus size needs
its own comment.

## Corpus

Sizes, hours and the needle tiers are `gen_corpus.py` parameters; the ADR's
corpus is `--traces 20000000 --hours 24`. Needles are planted per block of
the store's logs block geometry, 8,192 rows per block
(`docs/log-segment-format.md:438`, "Target 8192 records per block";
`block_target_records: 8192` at `crates/ravel-logseg/src/writer.rs:99`), in
the generator's row order: the common needle in one row of 10 % of block indices, the
sparse one in one row of 0.1 % of block indices (at least 2, and the common
one at least one more), the rare one in a single row, so the tiers order rare < sparse < common by blocks touched. The suite
recounts blocks from the data rows under that geometry and requires each
tier within 20 % of its target (issue #3699's spec, not the ADR, allows the
20 %). `truth.json` records the exact counts every answer is judged
against.

The plant counts are blocks of the generator's row order, not of storage:
the writer sorts each object's records by `(stream_ref, ts_ns)`
(`docs/log-segment-format.md`, "BLOCKS"), per shard and ingest hour, so a
needle row lands in whichever storage block the sort puts it in.
`stamps/needle_blocks.json` records the storage counts per needle column
(`<column>_present_blocks`, null when an inspected object's block or column
counts are unreadable, never 0), and evaluate.py compares each against the
planted count by tier: the rare needle exactly (one row is one block in any
order), the sparse and common needles within 25 % of planted, and the tag
column, which every tier sets, reported only. Each needle's planted and
storage figures are listed in the results (`needle_blocks.<value>.planted`
and `.storage`) and in the summary's needle block lines. In the smoke the
generator planted needles in 5 of its 8,192-row blocks over 100,000 log rows
(common 3, sparse 2, rare 1; one block holds two tiers), and `rlog inspect`
over all 4 logs objects reports 14 blocks for 100,000 records, with the
needle tag present in 4 of them against 5 planted and the rare needle in
exactly 1, the sparse needle in 2 against 2 planted and the common needle in
3 against 3, both inside 25 %. The suite pins those figures.

## fixtures/local-smoke

A local-lane run of `box/run.local-smoke.toml` on an aarch64 machine (4
cores, 8 GB, `CARGO_BUILD_JOBS=2`): 20,000 traces over 2 hours plus a
10,000-trace tail hour, 2 cold runs, hits = false, over unfolded,
folded_no_leaves, folded_leaves, mixed, compaction_in_flight,
erasure_in_flight and many_parts_24, against a loopback RustFS. Its 100,000
log rows fill 14 store blocks. It took 23 minutes, 13 of them the mixed
state's wait for the next ingest hour. Imported
with `investigations_cases.py --import-local-smoke DIR`. Its `calls.jsonl`,
`truth.json`, `results.local.json` and every stamp are gzipped, which keeps
the fixture under 300 KB; `summary.local.txt` and `run.toml` are plain.

Its verdict is `fail`. The problem list, pinned by the suite:

```
miss row_01.wire_bytes
miss row_02.wire_bytes
miss row_03.probe_wire_per_object
miss row_08.logs_trace_id_leaf_pct
miss row_08.logs_request_id_leaf_pct: key cardinality equals the row count
dead_counter segmentsOpened journey_c_1 folded_leaves
dead_counter segmentsOpened journey_c_10240 folded_leaves
```

The only incomplete line is
`figure row_13.negative_controls_failing_on_mutant not run: mutant controls
not implemented`.

- Row 3: the call moved 131,072 probe wire bytes in 2 probe GETs over the
  window's 2 candidate objects (from `stamps/unfolded.spans.ranges.json`),
  65,536 per object against a 16 KiB miss: the old 64 KiB read (#3118,
  fixed by T4, not on main). The fixture carries the ranges stamps of
  unfolded, the only state row 3 reads and the only one the run stamps.
- Row 7: the fold_advance rebuild issued 12 section GETs on each signal,
  2 x the 6 entries of the whole tail, where the 2 new entries would cost 4
  a pass. The fixture's build rebuilt the leaf from scratch on an advancing
  fold, which T6 of the epic changed; this is not a scale effect. Its fold
  report carries no `key_index_build_passes`, `key_index_sections_read` or
  `key_index_leaf_gets`, so row 7, judged per pass, reads those figures and
  the section GETs `unsupported-on-this-build`, not a miss
  (`test_section_gets_need_the_pass_count`).
- Row 8: the logs `request_id` leaf is 27.5 % of the logs data bytes (one
  key per row) and the logs trace id leaf 5.9 %, against 2.5 %; the spans
  trace id leaf is 4.3 %, inside 6 %.
- Rows 1 and 2: 464,912 wire bytes for the no-range lookup and 333,840 for
  the 1 h lookup, against 300 KB; the run's real figures.
- Row 11: the 1,000-key IN lists are refused by the complexity guard
  (1,001 units), so their figures and the over-cap controls read
  `refused-by-guard` with the server's text (15 figures over 24 calls on
  the summary's `refusal classes` line). Row 13: every in-flight try of
  compaction_in_flight's four statements overlapped its own signal's
  compaction (the two signals compact concurrently, 1.26 s and 1.98 s), and
  under the pending erasure the victim's lookup returned no row on
  both cold tries while `erase status` read PENDING.
- `dead_counter`: segmentsOpened is 0 on every SQL call while scan GETs move
  bytes; journey C's objects-opened figures (row 14) report it.

`investigations.test.sh` re-evaluates it and requires byte-identical output
and that exact problem list. After a deliberate change to evaluate.py,
`bands.toml` or `statements.toml`, regenerate the expected output and
review the summary diff:

```sh
python3 benchmarks/investigations/investigations_cases.py --refresh-local-smoke
```

## Tests

`bash benchmarks/investigations/investigations.test.sh`, in CI's
`doc-scripts` job: no AWS, no server, under two minutes. It runs `bash -n`
over every script, generator recounts at 1,000 traces and at 5,000 (where
the logs reach 4 blocks of 8,192 rows and the needle-block recount applies,
also at scale 10), the shape of `statements.toml` and `bands.toml` (the IN
list caps recomputed from the complexity guard, and every band's text
against the ADR's Acceptance table), evaluate.py on the fixture and on
mutations of it (missing, duplicate, miss, wrong answer, a deleted catalog
stamp, absent phases, CPU and leaf sizes, dead counters, full, partial, zero
and too-short compaction overlap, a refusal outside the cap probes, the
1,025-key control's pass, miss, refused and unsupported readings, a skipped state
and a mid-state deadline, the unsealed tail, the pending-erasure victim, a
state absent from the results, row 3's per-object denominator, row 6's
objects past the probe on box C's stats, row 7 per pass on box D's figures
and on a one-pass rebuild, row 15's budget from `truth.json`, row 18's
hold, box C miss, noise edges, `Uncovered` cell and absent state, a typed
refusal counted as an error, exact cost and maintenance dollars), the wrong
evaluators each of those cases must reject (each printed with the
evaluate.py line it flips) and the wrong bands (each printed with the
bands.toml line it flips), `inv.py advance-check` on box D's slice counts
and on each precondition failing, a failed state (its figures `not-run` with
`state failed at <step> (exit N)`, never pass or missing), the even-hours
corpus and the strided-load check over it at 744 hours, and the dry runs of
`run.sh` (local, s3, s3 with hits, past the deadline, a failed state that
continues and a failed base load that ends the run), `run.sh` sourced with
a stub `ravel-cli` that fails (its log copied to `out/logs/`, the next
state still run), `launch.sh` (with an `aws` stub that
fails if called) and `fetch.sh` (including an `aws` stub that exits 255,
which must exit 69).

The box scripts target bash 3.2 as well as 5: run
`/path/to/bash-3.2 benchmarks/investigations/investigations.test.sh` and
every script is syntax-checked and dry-run under that bash. bash before 4.4
treats `"${a[@]}"` of an empty array as unbound under `set -u`, so every
array that can be empty expands as `${a[@]+"${a[@]}"}`; a lint case
enforces it, and under 3.2 a case shows the unguarded form failing.
