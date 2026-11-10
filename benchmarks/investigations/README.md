# Selective investigations

Issue #3699, ADR-3692 decision 8 (T1 of epic #3692). A measurement harness
for the selective read paths of NOFire AI's telemetry store: a lookup by
trace id or request id, an IN list, a rare, sparse or common needle, and the
narrow joins of the investigation journeys. It runs them against a seeded
spans and logs corpus in each state of the key-index matrix (see "States"),
judges every answer against the generator's oracle, and judges every figure
against `bands.toml`, which transcribes the 18 rows of ADR-3692's Acceptance
table and ADR-3939's rows (see "ADR-3939's rows").

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
  oracle and read as a wrong answer, not a pass. The local smoke does not
  list the state, because at its scale the second check would always fail
  it: the smoke's 150,000 logs rows, base and tail, are 150,000
  `request_id` pairs, under the 4,000,000-pair share each of the two logs
  fields gets of the default 8,000,000-pair budget
  (`DEFAULT_KEY_INDEX_BUILD_MAX_PAIRS`), so the advance takes one pass and
  adds no slice (the committed fixture's `folded_leaves` and `fold_advance`
  stamps both carry one `request_id` leaf). The state's flow and its check
  are pinned on the s3 lane's dry run instead.
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
- `day_parts_7`, `day_parts_30` and `retired_hour`: ADR-3939's multi-day
  states, listed only by a run that measures that ADR ("day_parts_N and
  retired_hour" below).

`mixed` is followed by `fold_advance`, `compaction_in_flight` by
`compaction_done`, `day_parts_N` by `day_parts_N_advance` and
`retired_hour` by `retired_hour_drop`; each derived state is live exactly
when its parent is. A
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

### day_parts_N and retired_hour

ADR-3939's harness-state amendment ("Two states",
`docs/adrs/3939-key-index-day-seal.md`) adds these states. `box/run.sh`
builds them in `state_day_parts` and `state_retired_hour`. Each multi-day
state loads its own tenant, so the base tenant's corpus is not changed:

- `dp7` holds spans and logs. `day_parts_7` and `retired_hour` run on it.
- `dp30` holds logs only. `day_parts_30` runs on it.

The loads are per tenant; the maintain pass `retired_hour` runs is not. A
maintain-mode server compacts every tenant's buckets it lists, whatever
token file it is given, so run.sh refuses a `run.toml` that lists any state
after `retired_hour` (nothing measured later can be moved under it), and
`retired_hour` itself fails if the maintain pass wrote a compaction or
rewrite record into a `dp7` logs bucket ("How the tombstone is produced",
step 5).

`retired_hour` runs on `dp7` because ADR-3939 measures the retirement of a
day part's hour 0 in a sealed day. `dp7`'s oldest day is such a day, and it
is already folded with leaves. A fresh tenant would need a second week-long
load. `bands.toml` therefore reads every ADR-3939 fold figure as
`fold.dp7.*`.

**The window.** `inv.py day-parts-window` picks N whole UTC days whose last
hour is before the current ingest hour. It refuses a window that would
reach the current hour, since the strided load refuses one too
(`day_parts_window`, `box/lib/inv.py`). The window is computed once, at
corpus time, so it stays in the past.

**The corpus.** `gen_corpus.py --even-hours` generates the corpus:

- `--hours 24N` and `--start-hour` at the window's first hour.
- `--traces` is `day_parts.traces_per_day_N` times N.
- `--seed` is `corpus.seed + 100 + N`.

For `dp7`, `inv.py midnight-truth` then adds the midnight lookup's answer
(`midnight.request_id.1`) to `truth.json`. That answer is the request id of
the row nearest the first UTC midnight of the window, within 30 minutes of
it. It must occur once and be no planted id. With no such row the
statement is recorded `not-run`.

**Corpus time.** run.sh generates the corpora one after another, before the
first state, and the day-parts corpora are the largest a run makes. The
amendment prices 30 days at box D's rate at about 20 hours of generation,
so about 40 minutes a day at 20,000,000 traces. `dp7` (7 days at
19,999,992 traces) takes about 7 x 40 = 280 minutes, about 4.7 hours, and
`dp30` (30 days at 4,000,008 traces, 120,000,240 traces, six box D days)
about 6 x 40 = 240 minutes, on top of the base corpus (one box D day, about
40 minutes). A run listing both states spends about 560 minutes generating
before it loads anything, which is past the example's `run.max_minutes`
(540) and leaves nothing of `box.max_minutes` (at most 600). A Stage 2 run
therefore splits them: one run lists `day_parts_7` and `retired_hour`
(about 320 minutes of corpus, then the loads, folds and statements, inside
`run.max_minutes = 540`), and a second run lists `day_parts_30` (about 280
minutes of corpus). A run that keeps both must raise `run.max_minutes` and
`box.max_minutes` past the corpus time plus the states' own steps, and 600
does not hold that.

**`day_parts_N`** runs these steps in order:

1. `inv.py stride-check` checks each signal over all 24N batches. A
   refusal records the state `not-run`; a check that cannot run fails it.
2. One `ravel-cli load` per signal, with these flags:
   - `--ingest-hour-stride 1`
   - `--ingest-hour-start` at the window's first hour
   - `--batch-rows` = rows / 24N, so batch k is event hour k, in ingest
     hour start + k
   - `--target-bytes 1`
3. The statement set under temp `prefold`, before any fold.
4. One `catalog fold --writers-stopped --json --key-index-leaves` per
   signal (transition `day_parts_N`). It builds every day's leaves in one
   pass (ADR-3939 decision 4, corrected). For N = 30 both folds also pass
   `--key-index-build-max-pairs day_parts.key_index_build_max_pairs_30`
   (default 1,600,000), stamped in
   `stamps/day_parts_30.logs.build_budget.json`. At a fifth of box D's rate
   the default 8,000,000 would split a day's 20,000,040 request_id pairs
   into about 6 passes (a share of 4,000,000 per indexed field), so S would
   be about 5; 1,600,000 gives a share of 800,000 and 26 passes, A1's 26,
   so S is about 26 and the state reads at box D's T(26) + 100 = 126
   (`run.example.toml` carries the arithmetic).
5. For N = 7 only, a second fold with the same flags
   (`day_parts_7_refold`, A4's second HEAD).
6. The transition, then the census, catalog, object, kidx and tenant stamps.
7. `inv.py day-parts-stamp` writes `stamps/day_parts_N.SIG.day_parts.json`.
   It splits the catalog stamp per part: `min_hour`, `max_hour`, UTC `day`,
   `one_day` (false for a part that spans two days), `entry_count`, and
   `slices` per field. It also records the HEAD `watermark_hour`, the
   `days` held and `part_count`.
8. The statement set, cold.

**`day_parts_N_advance`** follows:

1. Wait for the next ingest hour, at most `run.mixed_wait_max_minutes`.
2. Load the mixed state's tail corpus into the tenant with `ravel-cli load`
   on the real clock, as `mixed` does. The amendment says the advance tail
   is loaded "through the server"; the harness does not send it to a
   running server. Without `--ingest-hour-stride`, `ravel-cli load` writes
   through the same `LogIngestRouter` (`crates/ravel-ingest/src/log_router.rs`)
   the server's `logs_ingest.rs` uses, and the span router likewise, on the
   wall clock, so the tail lands in the current ingest hour as a server
   ingest at that moment would. For the rows this is equivalent: the same
   tail rows land in the same ingest hour under the same router. What
   differs is the transport (no OTLP request, no HTTP acknowledgement), and
   no advance figure reads it.
3. Fold with `--key-index-leaves` (transition `day_parts_N_advance`).
4. Take the census and the day-parts stamps again.

**`retired_hour`** requires `day_parts_7` listed before it, no state listed
after it, and `[retired_hour] c1_sha` set. run.sh refuses a run.toml that
breaks any of these before any step. The state is recorded `not-run` when
`day_parts_7` did not build `dp7`, when `ravel-cli catalog fold --help`
lists no `--retention-default` (that fold could not reconcile the
tombstone), or when `box.sha` is empty and the checkout has modified
tracked files (on the local lane the binary's SHA is read from the
checkout's HEAD, which then does not name what was built).

1. `inv.py fold-binary` writes `stamps/retired_hour.logs.fold_binary.json`.
   See "W2's fold binary".
2. `retire_through retired_hour 0` tombstones ingest hour D0, the first
   hour of `dp7`'s oldest day, and nothing later. "How the tombstone is
   produced" below explains how.
3. It stamps `stamps/retired_hour.logs.tombstone.json`. `hour`,
   `first_hour` and `last_hour` are in unix hours, the unit catalog part
   ranges count in.
4. Two `ravel-cli catalog fold --writers-stopped --json --key-index-leaves
   --retention-default Rs` follow. They are stamped in `folds.jsonl` as
   `retired_hour` and `retired_hour_refold`.
5. The transition, the census and the catalog stamp.

The state sends no statement: its rows read fold and stamp figures only.

**`retired_hour_drop`** (W3) repeats those steps with
`retire_through retired_hour_drop 23`, which tombstones every hour of that
day, D0 to D0 + 23. Its catalog stamp is
`stamps/retired_hour_drop.logs.catalog.json`.

**How the tombstone is produced.**

1. ravel-maintain's retention pass writes a bucket's tombstone,
   `t/<hash>/<signal>/c/<shard>/<YYYYMMDDTHH>/retire.tmb`
   (`retention_tombstone_key`, `crates/ravel-commit/src/keys.rs`). It does
   this once the bucket's latest event time is before now minus the
   tenant's retention window R (`is_expired` and `write_tombstone`,
   `crates/ravel-maintain/src/retention.rs`).
2. `inv.py retention-cutoff` chooses R from the corpus. It reads the
   latest event time in batch K and in batch K + 1 of the strided load. The
   cutoff is the first whole second past batch K's latest event, and R is
   now minus that cutoff. It refuses when batch K + 1's latest event is
   before that cutoff, since the same R would expire it too. It also
   refuses a cutoff that is not in the past.
3. run.sh lists the store (`keys-STATE-before.tsv`), then starts
   `ravel-server --mode maintain --maintain-key-file F --disable-fold
   --retention-tenant dp7=Rs --maintain-interval-secs
   retired_hour.maintain_interval_secs`. Maintain mode requires the key
   file and every other mode refuses it (`services/ravel-server/src/config.rs`).
   Its format is what `parse_fragment_keys` reads: one 64-hex-digit key per
   line, `#` comments and blank lines ignored, an empty file refused. run.sh
   writes `work/auth/maintain.key` once per run, mode 0600, from Python's
   `secrets.token_hex(32)`. The token file names `dp7` alone, but that
   bounds only which tenant the retention flag names: the pass's
   compaction lists every tenant.
4. Every interval, run.sh lists the store and runs `inv.py
   tombstone-check`, per bucket: it continues once every `(shard, hour)`
   bucket of `dp7`'s logs in hours D0 to D0 + K that holds a commit record
   also holds `retire.tmb`, and no bucket outside that range does. One
   shard's tombstone does not stand for the hour. An hour in the range
   with no commit-holding bucket at all fails the state at once (exit 65).
   If the check has not passed after `retired_hour.wait_minutes`, the state
   fails.
5. run.sh stops the server, lists the store again
   (`keys-STATE-after.tsv`), and `inv.py maint-records` compares the
   compaction (`l1.*.cmt`) and rewrite (`rw.*.cmt`) records in `dp7`'s logs
   buckets before and after, writing
   `stamps/STATE.logs.maint_records.json`. Any new one fails the state:
   W2 and W3 read fold-wide counters, and a bucket the maintain pass
   compacted would move them.

**How the fold reconciles it.** The fold passes `--retention-default Rs`,
which gives a tenant with no durable retention override a window R. The
fold's retention-frontier reconcile (`retirement_frontier_hour`,
`crates/ravel-catalog/src/fold.rs` about line 1218, and the pass at about
lines 4294 to 4318) then re-lists the snapshot-named hours at or below
now - R + the protection horizon (25 h 5 min by default), oldest first, at
most `frontier_reconcile_max_hours` (168) of them, each strictly below the
fold's fixed reconcile window. A fold with no window lists none of them,
which is why run.sh checks the fold help first. The on-demand server fold
(`POST /api/v1/admin/fold`) also carries a window, but its response has no
key-index field, no `head_digest` and no `retired_hours`, so the harness
does not use it; nothing else in it needs it.

**Why the local smoke lists none of them.** These states are pinned on the
s3 lane's dry run instead (`test_new_states_dry_run_plan` and the four
cases under "Tests"), for three reasons:

- The committed fixture is a real run, and adding a state to it means
  re-running the smoke with release binaries and pyarrow. This change was
  made on a machine without either. `--refresh-local-smoke` re-evaluates the
  existing run; it does not re-run it.
- Each advance waits up to `run.mixed_wait_max_minutes` (70) for the next
  ingest hour, once per state, on top of the smoke's own wait.
- `retired_hour` waits for maintain retention: up to
  `retired_hour.wait_minutes` (20) per step, twice.

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
(`traces_per_hour`, `snapshot_part_max_entries`), `[day_parts]`
(`traces_per_day_7`, `traces_per_day_30`), `[retired_hour]` (`c1_sha`,
`r2_sha`, `maintain_interval_secs`, `wait_minutes`), `[prices]` and
`[cost]`.

`[day_parts]` is read only when `run.states` lists a `day_parts_N` state.
The 7-day rate defaults to 19,999,992 traces a day, which is 24 x 833,333:
box D's 20,000,000 a day rounded to a multiple of 24 so every hour holds
the same count. The 30-day rate is 4,000,008, which is 24 x 166,667: a
fifth of that rate, rounded the same way. The amendment calls for a reduced
rate there but names no figure, so the fifth is this harness's choice. Its
reason for reducing: 30 days at box D's rate cost about 4 hours of fold and
about 20 hours of corpus generation.
`[retired_hour]` is read only when `run.states` lists `retired_hour`.
`c1_sha` is required. An empty `r2_sha` means R2 has not landed.

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
| `folds.jsonl` | one line per fold (transition, tenant, signal): exit code, the `--json` report, wall time, CPU seconds and peak RSS; the kind names the fold's flags (`--key-index-leaves`, `--key-index-build-max-pairs N`, `--retention-default Rs`) |
| `census.jsonl` | one line per state: object count and bytes per tenant, signal and key class, listed from the store |
| `transitions.jsonl` | one line per state entered: when, and in which ingest hour |
| `restarts.jsonl` | one line per cold call: the server pid, the cache-drop result and the warmup |
| `STATE.SIG.catalog.json` | `catalog inspect` before the state's first statement: HEAD id, part count, entries per part, the leaves each part carries |
| `STATE.SIG.object.json` | `rspan inspect` or `rlog inspect` of one object named from the census: format version, sections, KEY_IDX presence, footer length |
| `STATE.SIG.kidx.json` | `inspect kidx` of one leaf, when the state has leaves |
| `STATE.SIG.tenant.txt` | `typed-attr-column show` at that state |
| `STATE.server.json` | the server binary's sha256 and its flags |
| `STATE.SIG.day_parts.json` | `day_parts_N` and its advance: the catalog stamp per part (`min_hour`, `max_hour`, UTC `day`, `one_day`, `entry_count`, `slices` per field), the HEAD `watermark_hour`, the `days` held and `part_count` |
| `retired_hour.logs.fold_binary.json` | the fold binary's `sha`, run.toml's `c1_sha` and `r2_sha` resolved, and `carries_c1` and `carries_r2` by git ancestry |
| `STATE.logs.tombstone.json` | `retired_hour` and `retired_hour_drop`: the tombstoned `hour` (D0) and `first_hour` to `last_hour` in unix hours, the retention window and the tombstone-check output |
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
with the reason. A figure whose inputs no build's report carries is
`not-computable`, with the inputs it lacks as the reason (row 7's G,
below), and a figure whose precondition does not hold on the measured
state is `incomplete`, with the precondition as the reason (row 6's probe
survivors, below). A figure computed from, or judged against, a
`not-computable` or `incomplete` figure takes that status and its reason.

Only an allowlist of fields a build may not report may read as
`unsupported-on-this-build`, and only when the call itself succeeded; a
field read from the call is unsupported only when it is absent, while the
per-object and per-part breakdowns are unsupported on every run, since
evaluate.py does not read them from the stats at all:
`stats.keyIndex` and its members (`partsCovered`, `partsUncovered`,
`partsBeyondCeiling`, `leavesRejected`, `reservationsRefused`,
`leafGetRequests`, `keysProbed`), `pruning.skipIdxDropped`, the per-object
and per-part breakdowns, and the fold report's `key_index_build_passes`,
`key_index_sections_read`, `key_index_leaf_gets` and
`head_bytes_per_part_field` (main's `FoldReport`,
`crates/ravel-catalog/src/fold.rs`, writes the first three, so they read
unsupported only from a report that lacks them), plus, for ADR-3939's rows,
`stats.keyIndex.leafRoundTrips`, `io.s3SlowDownResponses`, the fold
report's fields and the H1b stamps listed under "ADR-3939's rows". Any
other absent field (`phases`, `pruning`, `signals`, `io`, `server_cpu_s`, `server_rss_peak`, `rows`, the duration)
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
asked for, a row 18 cell `not-measured` (below), a figure that reads
`not-computable` or `incomplete` or is judged against one, or a row 18
figure that reads `unsupported-on-this-build`, `refused-by-guard`, refused
or missing (missing also fails the run). Each reason is listed under `incomplete` in the results and the
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
  past the probe are the probed objects the key index did not prune: the
  plan phase's `segmentsOpened` less `pruning.segmentsPrunedByKeyIndex`
  (`cold.max.probe_survivors`), `unsupported-on-this-build` when the stats
  carry no `segmentsPrunedByKeyIndex`. The keyed plan counts each object
  whose KEY_IDX footer it probed as opened in the plan phase, absent key or
  not, so `segmentsOpened` summed over the phases counts the probed objects
  too, and the scan phase's `segmentsOpened` is not the figure either: the
  plan does not probe an object at or under 512 KiB, so the scan can open
  objects no probe saw. Box C's q20 (run
  `investigations-20261106T154511Z-47463-23195`, its `calls.jsonl`) read
  497 as the sum: of 523 objects, 26 pruned by stats and 4 at or under 512
  KiB (the census's 523 less its 519 above), 493 opened in the plan phase
  with 493 GETs, 492 of them pruned by the key index, and 4 opened in the
  scan phase with 6 GETs and 989,537 B. The figure is 493 - 492 = 1, the
  band's holders, a pass (`check_row_6_objects_past_probe`). That leaves 5
  objects past every pruning step, the 1 probe survivor and the 4 unprobed
  small objects, while the scan opened 4; the stats count objects and do
  not name them, so they cannot tell which of the 5 the 4 are. The figure
  is judged only when the plan opened exactly the probed objects: plan
  `segmentsOpened` equal to the census's `objects_over_512k` less
  `pruning.segmentsPrunedByStats` (box C: 519 - 26 = 493). The plan keeps
  objects at or under 512 KiB out of its phase only when it skips sub-bound
  plans (`skips_sub_bound_plans` in `crates/ravel-sql/src/logs_scan/plan.rs`);
  otherwise it opens them with no probe, and they would count as
  survivors. When the equality fails, or the census carries no
  `objects_over_512k` for the statement's tenant and signal, the figure
  reads `incomplete` with the precondition as its reason, never a hold or a
  miss, and the run is incomplete. A small object pruned by stats also
  breaks the equality (it is in `segmentsPrunedByStats` but not in
  `objects_over_512k`), which reads `incomplete` too: the safe direction.
  `segmentsPrunedByStats` does not count objects the time window drops or
  `segmentsSkippedByThreshold`, so the equality could still hold by
  coincidence on another statement; the precondition also asks
  `pruning.segments` equal to the census's `objects` (box C: 523) and
  `pruning.segmentsSkippedByThreshold` 0, and reads `incomplete` otherwise,
  or when the census carries no `objects`. The same stats with the plan
  opening 497, with `pruning.segments` 522, or with one segment skipped by
  threshold read `incomplete`; with the precondition holding, a probe that
  let 2 objects through misses (`check_row_6_objects_past_probe`).
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
  Uncovered is the fold report's `key_index_uncovered_entries`, which the
  fold sums over fields (an entry uncovered for two fields counts twice);
  the report has no per-field count, so the row judges the sum. That can
  only widen the targets, and neither miss line reads it, so a rebuild
  still misses (`check_row_7_uncovered` pins each signal holding only with
  the term). The passes target is G, the pass-end slice amendment's greedy
  partition of each field's key8 runs into groups of at most the pair
  share s = max(1, floor(budget / F)) for F fields, a single run over the
  share its own group and the slowest field deciding. evaluate.py bounds G
  from the two fold report fields issue #3948 specifies:
  `key_index_pair_budget` (the build's pair budget) and
  `key_index_field_pairs` (a map from every field the build indexed,
  zero-pair fields included, to its total pairs, so F is the map's size).
  A field of P pairs takes between ceil(P / s) and 2 x ceil(P / s) - 1
  groups, at least 1, because no group holds more than s pairs while no
  key8 run (one key's entry ordinals) exceeds the share and greedy groups
  are maximal, so any two adjacent groups exceed s; `build_passes_low` and
  `build_passes_high` are the largest low and the largest high bound over
  fields, and `build_passes` holds inside that range and misses outside it
  (`g_bounds`, `check_g_bounds`). The fold reports totals rather than each
  field's key8 runs, which would give G exactly, because the runs are one
  integer per distinct key8 per field, about 20 million at the T12 corpus,
  and holding them would hold every run in memory, which the pair budget
  exists to prevent. No fold report carries either field yet. Until one
  does, both bounds read `not-computable` naming the missing inputs,
  `build_passes`, judged against them, reads `not-computable` too, and
  each adds an incomplete reason, so the verdict is at best `incomplete`
  and the exit 1: row 7 cannot hold until the fold report carries G's
  inputs, which issue #3948 adds. Box D's 3 and 27 passes read that way,
  not as holds or misses, and still scale the section GETs
  (`check_row_7_passes_not_computable`). A fold report carrying box D's
  inputs (spans `trace_id` 20,833,333 pairs at budget 8,000,000; logs
  `request_id` 104,166,665 and `trace_id` 20,833,333 at the same budget)
  bounds G in [3, 5] on spans and [27, 53] on logs (request_id [27, 53],
  trace_id [6, 11]), so 3 and 27 hold, and on logs 53 holds while 2 and
  54 miss (`check_row_7_g`); a third, zero-pair field still counts in F,
  putting logs in [40, 79], where 27 misses
  (`check_row_7_g_zero_pair_field`). `build_passes` has no miss line of its
  own, so a rebuild whose passes fall between the bounds reads within
  range; the per-pass section figures are what catch a rebuild. On the
  committed local smoke both bounds read `not-computable` for that reason,
  but `build_passes` reads `unsupported-on-this-build` before it is judged:
  that build's fold report carries no `key_index_build_passes` at all. New
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
  holds. The logs planner dedups a list on each key's key8 before it counts
  the keys (`KeyIn::new`, through `KeySet::new`), which is exact for logs,
  and an Id16's key8 is its high 8 bytes, so padding that differed only in
  the low half would be one key and leave the list under the cap. The spans
  `trace_id IN` arm sorts and dedups the full 16-byte ids and drops the
  list when more than `TRACE_ID_SET_MAX` (1,024) distinct ids remain,
  before any key8 is taken (`crates/ravel-sql/src/spans_pushdown.rs`, the
  `Expr::InList` arm); only a list that passes that cap is deduped on key8,
  by the router's key set. Every over-cap list holds 1,025 distinct key8s,
  so it is over the cap on both paths
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
  when every 1,000-key call reports `keysProbed` 1000 and either
  `partsCovered` or a part left to tier 1: any of `partsUncovered`,
  `skippedByBudget`, `partsBeyondCeiling`, `leavesRejected`,
  `reservationsRefused` and `partsOverTierOne`, every outcome the ADR's row
  11 lets the report name (box C's went Uncovered by budget, `partsCovered`
  0 and `skippedByBudget` 1, which still carried the key set:
  `check_control_uncovered_by_budget`; each outcome alone passes in
  `check_control_uncovered_outcomes`, which also pins that its cases cover
  every outcome evaluate.py accepts), and
  every 1,025-key call is served by a visible scan with no keyIndex figure
  above zero; 0 (miss) when a 1,025-key call is index-served
  (`keysProbed`, `partsCovered` or `leafGetRequests` non-zero), is served
  with no visible path, or is refused while the 1,000-key list was
  admitted, or when the 1,000-key list probed another count or reported
  `partsCovered` 0 with no part left to tier 1;
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
  covering part to tier 1 (`partsCovered` 0, or `partsUncovered`,
  `skippedByBudget`, `partsBeyondCeiling`, `leavesRejected`,
  `reservationsRefused` or `partsOverTierOne` above 0) reads `not-measured`
  whatever its time (`check_row_18_over_tier_one` for the last):
  its time is the tier-1 probe's, so it is neither a hold nor a miss, and
  it makes the verdict at best `incomplete`. `partsCovered` 0 with every
  outcome counter 0 counts too: the leaves answered no part
  (`check_row_18_covered_nothing`). A cell whose build reports no
  keyIndex outcome counters reads `unsupported-on-this-build`, which also
  makes the verdict `incomplete` and the exit 1, not a pass
  (`check_row_18_unsupported_is_incomplete`). `partsOverTierOne` alone may
  be absent: a build that predates it left no part to tier 1 by cost, so
  it reads as 0 and the other five decide
  (`check_row_18_predates_over_tier_one`). With either state absent the
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

## ADR-3939's rows

`bands.toml` also transcribes ADR-3939's two Acceptance tables
(`docs/adrs/3939-key-index-day-seal.md`): rows A1 to A7 of the
pre-registered table and W1 to W8 and F1 to F3 of the added rows, keyed
`adr3939_a1` to `adr3939_f3`. Each row carries `adr = "ADR-3939"` and its
ADR label in `adr_row`, and its `check`, `target`, `miss` and `source`
cells are the ADR's cells verbatim. The summary names these rows by their
ADR label (`ADR-3939 row W5`), and `not requested` lists them after
ADR-3692's rows. Each row runs on one of the three states the
harness-state amendment adds, and on that state's tenant (`dp7` or `dp30`,
"day_parts_N and retired_hour"):

| State | Rows |
|---|---|
| `day_parts_7` | A1 to A6, W1, W4, W6, W7, F1, and F3's W4 figures |
| `day_parts_30` | A7, W5, F2, and F3's W5 figures |
| `retired_hour` | W2, W3, W8 |

The amendment names A5, W4, W6, W7, F1 and F3 for the 7-day state and A7,
W5, F2 and F3 for the 30-day one. A1 to A4, A6 and W1 measure an advance
or a fold at a full-day tail, which the 7-day state is the first to hold,
so they run there too.

`box/run.sh` builds the three states only when `run.states` lists them,
and `run.example.toml` lists none of them. A run without them reports every
figure of these rows `not-run` with `states ['day_parts_7'] not run:
day_parts_7: not in run.toml states` (or the state it lacks), and the
states section lists each as `not-run (not in run.toml states)`. The rows
are not requested, so they leave no run incomplete.

Each `day_parts_N` state sends its calls under temp `prefold` before its
fold (the unfolded baselines of A6, W4 and F2), and cold after it. A
`prefold` call is a cold call in every respect but its group. It follows a
server restart, which `restarts.jsonl` stamps with `temp = "prefold"`. It
is held to `cold_runs` tries. Like the cold calls, it is judged against
`truth.json`'s answers for the state's own corpus (planned with `--sub
day_parts_N`). A `prefold.*` figure reads the `prefold` group only.

New statements, cold only, on the states their rows need:
`logs_request_in_100_7d` (A5, W4) and `logs_request_in_100_30d` (A7, W5),
the 100-key `request_id` list over the 7 and 30 days; `logs_request_in_1_7d`
(W6), one key; and `logs_request_in_1_midnight` (W7), one key from the hour
that spans a UTC midnight (23:30 to 00:30, so it reads the two day parts
either side of it), with window `midnight` and truth key
`midnight.request_id.1`. `inv.py midnight-truth` writes that key for
`dp7`'s corpus ("day_parts_N and retired_hour"). When no row qualifies, the
key is left out, and the run records the midnight call `not-run`: `box/lib/inv.py
plan` checks `requires = "midnight"` before it computes a window. 4 leaf GETs alone
would also hold for a window that read one day part, so W7 also pins the
calls' `keyIndex.partsCovered` to exactly 2 (lowest and highest across the
tries) and `partsUncovered` to 0. The six row-18 lookups also run on
`day_parts_7`, for A6.

### The reduced-rate rule

The 30-day state is loaded at a reduced rate: S slices per day part, read
off the catalog stamp (`stamp.slices.logs.request_id`) as figure `slices`.
A day part's bound for 100 keys is T(S) + 100, with T(S) = S × (1 − (1 −
1/S)^100), computed by `day_part_bound(S, factor=1)` in a band expression:
126 at S = 26, 176 at S = 175, 194 at S = 750. W5 and A7 check the same
two leaf GET figures of `logs_request_in_100_30d`: `leaf_gets_per_part`,
the largest per-part leaf GET count of any cold call, at most T(S) + 100,
and `leaf_gets`, the largest total of any cold call, at most 30 × (T(S) +
100). The stats carry no per-part breakdown. `key_index_json`
(`crates/ravel-sql/src/stats_json.rs`) writes `keyIndex` totals and no
`keyIndex.perPart`. So `leaf_gets_per_part` reads
`unsupported-on-this-build` ("stats carry no keyIndex.perPart breakdown")
in both rows on every run. Judging it needs a server that writes the field
and an evaluate.py that reads it. A7's total is 30 times
the bound, not the parts held times it: the 30-day state holds 30 day
parts, and a state holding any other number is not that state, so A7's
`leaf_gets` is `not-run` there (it carries `parts_held == 30` in its
`not_run_unless`) rather than judged against a rescaled total. W5's 30
days covered is its own figure, and a state short of 30 parts misses it.
A7's ceiling figure also checks 30 × (T(S) + 100) ≤ 66,560, the clause
under which every day held is covered. F3's W5 figures use 1.1 × (T(S) + 100),
`day_part_bound(S, 1.1)` (139 at S = 26). No bound is hardcoded: each
follows the stamped S. Below S = 3 the multi-slice term is not exercised,
so every figure that reads S carries `not_run_unless = "m['...slices'] >=
3"` and reports `not-run` with that reason, never a pass. A figure with
`not_run_unless` whose expression is false is `not-run`, not judged, and
leaves the run incomplete.

### W2's fold binary

After R2 a tombstoned hour in a sealed day takes the `retired_hours` path
and is not re-encoded, so W2 means something only on a fold binary that
carries C1 and not R2. Its first figure, `fold_binary_pre_r2`, reads
`stamps/retired_hour.logs.fold_binary.json` (`sha`, `carries_c1`,
`carries_r2`) and is 1 for C1 without R2; a stamp missing any of the
three fields is `missing`. Its other figures are `not-run` unless that
figure is 1. The harness-state amendment runs every other row on the R2
build, so a binary carrying R2 (or no C1) is one W2 is not measured on, not
a miss: the binary figure is `not-run` with the binary's SHA and what it
carries, the other W2 figures are `not-run` naming it, and all of them are
listed under `not requested` with that reason, so a run on the R2 build
holding `retired_hour` for W3 and W8 is neither failed nor left incomplete
by W2.

`state_retired_hour` writes the stamp with `inv.py fold-binary` before its
first tombstone, and fails the state when the stamp cannot be written. The
stamp records the binary's SHA (run.toml `box.sha`, or the checkout's
HEAD), with `c1_sha` and `r2_sha` resolved in the checkout that built it.
`carries_c1` is true when `git merge-base --is-ancestor c1_sha sha` holds,
and `carries_r2` the same for `r2_sha`. An empty `r2_sha` gives `false`. A
SHA that names no commit (the binary's, `c1_sha`, or a non-empty `r2_sha`)
fails the state. In a run where `retired_hour` is live, an absent stamp is
`missing`.

W3's tombstoned hour reads `stamps/retired_hour.logs.tombstone.json` the
same way. W3's retired-hour resolve (`retired_hour_segments`) reads
`unsupported-on-this-build` on every run: no harness command lists the
segments one ingest hour resolves to after a fold. Neither ravel-cli nor
the server has one.

### W3's drop and W8's retired hours

W3's "the part leaves HEAD with 0 PUTs at the day's last tombstone" is
three figures, not two: 0 part PUTs and 0 leaf PUTs in the drop fold's
report (transition `retired_hour_drop`), and `drop_parts_holding_hour`, the
number of parts in the catalog stamp taken after that fold
(`stamps/retired_hour_drop.logs.catalog.json`) whose hour range holds the
tombstoned hour, which must be 0. A drop fold that drops nothing writes no
PUT either, and misses on that count. `state_retired_hour` writes both
stamps, so an absent one is `missing`, naming it. So is a stamp with no
HEAD, a part count that disagrees with the parts listed, or a part
without a `LO..=HI` range is `missing`.

W8's two folds each report `retired_hours`, and each must equal `[h]` for
W3's tombstoned hour `h`, besides being identical: two folds that observed
no tombstone report `[]` alike and miss. A4's and W8's HEAD digests must be
non-empty strings, so two empty digests do not read as identical HEADs.

### Unsupported figures in these rows

These figures read `unsupported-on-this-build` when the field they name is
absent from the report (the allowlist applies only then): the fold
report's `key_index_build_passes` and `key_index_leaf_gets`, which main's
`FoldReport` writes, so they read unsupported only from a build whose
report lacks them, and `sealed_day_leaf_puts`,
`sealed_day_previous_leaf_gets`, `key_index_unchanged_section_gets`,
`key_index_part_puts`, `head_digest`, `switch_off_partition_changed` and
`retired_hours` (A1 to A4, W1 to W3, W8), which no build writes yet; the per-part leaf GETs (A7, W5, F3);
`stats.keyIndex.leafRoundTrips` (W6); `io.s3SlowDownResponses` (F3); and
W3's `retired_hour_segments` (above).

The `retired_hour` and `retired_hour_drop` folds are `ravel-cli catalog
fold --json` runs, like every other fold, so W2, W3 and W8 read their
figures from the same `--json` report under the same allowlist: a field
the report lacks reads unsupported on that build, as above. W2's binary
figure, W3's tombstoned hour and W3's `drop_parts_holding_hour` come from
stamps.

Every ADR-3939 figure, counter or timing, leaves the run incomplete when
it has no value to judge (`unsupported-on-this-build`, `refused-by-guard`,
`refused` or `missing`), as row 18's do, rather than letting the run pass
without it: a fold report that lacks a field these rows read, or a
per-part figure evaluate.py does not read, shows as incomplete, never as
a pass. A run holding a multi-day state is therefore incomplete until the
server and the fold report carry those fields.

## Unsupported on this build

- Row 7's G bounds (`build_passes_low`, `build_passes_high`) are not in
  this list: they are `not-computable`, not unsupported, because no
  build's fold report carries their inputs (`key_index_pair_budget`,
  `key_index_field_pairs`) yet, and they leave the run incomplete (row 7,
  above).
- Key-index stats (`stats.keyIndex` and its members),
  `pruning.skipIdxDropped`, the per-object and per-part breakdowns, and the
  fold report's `key_index_build_passes`, `key_index_sections_read`,
  `key_index_leaf_gets` and `head_bytes_per_part_field` (ADR-3939's rows
  add the fields listed under "ADR-3939's rows"): when one is absent from
  a build's output its figures are unsupported with that reason (the
  allowlist above). Main's fold report writes the first three; a build
  that predates them reads unsupported. The many_parts leaf-GET figures are
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
the fixture under 320 KB; `summary.local.txt` and `run.toml` are plain.

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

The incomplete lines are row 7's four G bound figures,
`figure row_07.spans_build_passes_low not-computable: fold report
fold_advance/inv/spans carries no key_index_pair_budget and no
key_index_field_pairs, ...`, its `build_passes_high` twin and the logs
pair, and
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
1,025-key control's pass, miss, refused and unsupported readings and each
outcome of its 1,000-key list that leaves a part to tier 1, a skipped state
and a mid-state deadline, the unsealed tail, the pending-erasure victim, a
state absent from the results, row 3's per-object denominator, row 6's
objects past the probe on box C's stats, with its plan-phase precondition
failing (incomplete) and a second survivor (miss), row 7 per pass on box
D's figures and on a one-pass rebuild, row 7's G `not-computable` on box
D's figures (incomplete) and computed from a fold report that carries its
inputs (hold at box D's passes, miss one over), row 15's budget from `truth.json`, row 18's
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

ADR-3939's rows have these cases of their own, each with the wrong versions
it must reject: `test_adr3939_band_text_is_the_adr_tables_verbatim` (every
row's cells against both of the ADR's tables, exact to the character; a
one-character drift in W4's target or A1's check fails it),
`test_multi_day_rows_report_not_run_without_their_state` (every row
`not-run` naming its missing state; fails with the three states
unregistered), `test_w5_a7_targets_follow_stamped_slices` (W5's and A7's
bounds at S = 26, 175 and 2: 126 and 3,780, 176 and 5,280, and `not-run`
at 2; fails with the bound hardcoded at 126 in evaluate.py or in either
row, with no S floor in W5, or with `not_run_unless` never consulted),
`test_w5_a7_per_part_and_thirty_parts` (a 127-GET part misses in W5 and A7
at S 26 while the total holds, a missing breakdown is unsupported, and A7's
total is `not-run` at 29 parts; fails with W5's per-part figure held to
the 30-day total or A7 judged on 29 parts),
`test_w2_requires_pre_r2_binary_stamp` (no stamp or no SHA: the binary
figure missing and the rest `not-run`; C1 with R2 and no C1: `not-run`, not
requested, with no W2 miss and nothing incomplete; C1 without R2 passes;
fails with W2 judged on any binary, the binary read off C1 alone, an R2
binary scored a miss, or a stamp figure's `not-run` left off
`not_requested`),
`test_w3_drop_leaves_head` (a part `96..=119` left after the drop misses
for tombstoned hour 100, its removal passes, an absent drop stamp is
missing naming it, a stamp with no HEAD or a short part list missing;
fails with the count fixed at 0 or the band reading `v >= 0`),
`test_w8_retired_hours_are_the_tombstoned_hour` (both folds `[]` miss, `[h]`
passes, empty A4 and W8 digests miss; fails with the refold's band reading
`v == v` or A4's refold digest `v is not None`),
`test_w7_reads_two_day_parts` (1 covered part, a third part or an uncovered
one each miss at 4 leaf GETs; fails with `parts_covered_max` reading
`v >= 2`) and `test_row_18_timing_rows_leave_run_incomplete` (A6's six p50
cells, W4's, F1's and F2's unsupported, and W4's unsupported leaf GETs, a
counter, each leave the run incomplete; fails with ADR-3939 figures left off
the incomplete list). `test_adr3939_rows_run_on_their_states`
pins each row's state (fails with A5 moved off `day_parts_7`).

The three harness states have box cases of their own, each run against the
wrong version named after it and shown failing (the flip is printed with
the file and line it changes):

- `test_day_parts_state_ends_before_the_current_hour`: `inv.py
  day-parts-window` returns N whole UTC days ending before the current hour
  at hours 0, 1 and 23 of a day and at the next midnight, for N 7 and 30,
  and refuses an end day that reaches the current hour. Wrong
  versions: the refusal reading `end > now_hour` ("a window ending at the
  current hour accepted"), and the default last day computed as
  `now_hour // 24 - 1` ("the default last day reaching the current hour at
  midnight").
- `test_day_parts_fold_builds_every_day_once`: one stride check over all
  24N batches per signal, prefold calls before the fold, one `catalog fold --writers-stopped --key-index-leaves` per
  signal, a second fold for day_parts_7 only, one fold per advance, and
  `inv.py day-parts` splitting the catalog stamp one entry per day. Wrong
  version: the state's fold without `--key-index-leaves` ("the state's fold
  without the leaf switch").
- `test_retired_hour_tombstones_hour_zero_only`: retired_hour retires
  ingest hour D0 and nothing later, `retention-cutoff` expires batch 0 and
  not batch 1, and `tombstone-check` accepts exactly the hours asked. Wrong
  versions: `retire_through retired_hour 1` ("hours 0 and 1 retired"),
  `tombstone-check` failing only on missing buckets ("more tombstoned hours
  than asked accepted"), and `retention_window`'s refusal of a cutoff past
  batch 1's latest event removed ("a cutoff that expires the next batch
  too").
- `test_tombstone_check_is_per_shard`: `tombstone-check` reads each
  `(shard, hour)` bucket on its own, so an hour whose shard 0 carries
  `retire.tmb` and whose shard 1 does not fails. Wrong version: failing
  only on extra buckets ("one shard's tombstone read as the hour's").
- `test_maintain_server_gets_a_key_file`: both maintain starts carry
  `--maintain-key-file`, and `maintain_key_file` writes one 64-hex-digit
  key (the `parse_fragment_keys` format), mode 0600, once per run. Wrong
  versions: the start without the flag ("the maintain server started with
  no key file") and `token_hex(16)` ("a 32-hex-digit key").
- `test_no_state_runs_after_retired_hour`: a run.toml listing day_parts_30
  after retired_hour is refused. Wrong version: the refusal turned into
  `|| true` ("a state listed after retired_hour accepted").
- `test_maintain_compaction_fails_retired_hour`: `maint_records` fails the
  state on a new `l1.*.cmt` or `rw.*.cmt` in a dp7 logs bucket and passes
  one in another tenant, in dp7's spans, or a new plain commit record.
  Wrong version: `maint-records` never failing ("a compaction record the
  maintain pass wrote accepted").
- `test_retired_hour_needs_a_fold_with_retention_default`: against a stub
  ravel-cli, a `catalog fold --help` without `--retention-default` records
  the state not-run and one with it goes on to the tombstone pass. Wrong
  version: the help check removed ("a fold binary without
  --retention-default accepted").
- `test_retired_hour_refuses_a_dirty_local_checkout`: on the local lane with
  no `box.sha`, a modified tracked file in the checkout records the state
  not-run. Wrong version: the check removed ("a modified checkout's HEAD
  stamped as the fold binary").
- `test_day_parts_30_folds_at_a_reduced_build_budget`: every day_parts_30
  fold carries `--key-index-build-max-pairs 1600000`, no day_parts_7 fold
  does, the budget is stamped, the arithmetic gives 26 passes (6 at the
  default), and a budget of 0 is refused. Wrong version: no budget ("the
  30-day folds at the default budget").
- `test_retired_hour_stamps_fold_binary_sha`: `inv.py fold-binary` writes
  `{sha, carries_c1, carries_r2}` by git ancestry on a scratch repository,
  an empty `r2_sha` reads false, an unresolvable SHA writes no stamp, the
  stamp precedes the first tombstone in the dry run, and run.sh refuses a
  retired_hour with no `c1_sha` or no day_parts_7 before it. Wrong version:
  `carries_r2` falling back to C1 when `r2_sha` is empty ("an empty r2_sha
  read as carried").
- `test_new_states_dry_run_plan`: the s3 lane's dry run with all three
  states listed, every step pinned in order. Wrong version: the drop
  retiring through hour 22 ("the drop leaving the day's last hour").

The box scripts target bash 3.2 as well as 5: run
`/path/to/bash-3.2 benchmarks/investigations/investigations.test.sh` and
every script is syntax-checked and dry-run under that bash. bash before 4.4
treats `"${a[@]}"` of an empty array as unbound under `set -u`, so every
array that can be empty expands as `${a[@]+"${a[@]}"}`; a lint case
enforces it, and under 3.2 a case shows the unguarded form failing.
