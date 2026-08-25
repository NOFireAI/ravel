# Running ClickBench against Ravel

ClickBench is an external, public analytical benchmark: a flat 105-column,
~100M-row `hits` table and a fixed suite of 43 statements, reported as the
minimum / median / maximum of three consecutive runs per query alongside the
load time and the stored size (ADR-0100). This guide makes that workload
runnable and repeatable against Ravel. It never fetches the dataset for you and
never reports a number: it is the procedure, not a measurement.

The checked-in artifacts live under `benchmarks/clickbench/`:

| File | What it is |
|---|---|
| `hits.mapping.toml` | the `--mapping` for `ravel-cli load` and `typed-attr-column set` |
| `hits.corpus.json` | the ClickBench suite rewritten for Ravel's `logs` table, in `sql_corpus.rs`'s external-file format |

The upstream suite is maintained at
<https://github.com/ClickHouse/ClickBench>; `queries.sql` (Q1..Q43) and
`postgresql/create.sql` are the sources the corpus and mapping were built from.
Each corpus entry carries its upstream `Q<n>` id so it can be diffed against the
original.

## What this measures, and what it does not

The millisecond figures the harness prints **do not reproduce across hosts**:
they depend on CPU, memory, and — by an order of magnitude — on the object-store
backend (local filesystem vs MinIO vs S3). They do not reproduce across two
instances of the same type either: issue #680 measured a 1.6x to 2x gap between
two c6a.4xlarge boxes at identical settings. So the safe default is that only
**ratios within one run** are comparable: query A versus query B, cold versus
warm, pre- versus post-compaction, one `--batch-rows` layout versus another.
Two reports go side by side only when they tick the comparability checklist in
"Reading a report against ClickBench" below. That is why every report carries
its own provenance (backend, region/endpoint, host logical cores, dataset id,
runs, `cache_bytes`, `deadline_secs`, `fetch_concurrency`). A latency table
without its backend named will mislead the first person who compares two runs.

Prior to #677 the `--tenant` lane resolved shard 0 only, so any earlier
multi-shard report understates the dataset (it measured, and reported an object
count for, one shard's slice of the data rather than the whole tenant); treat
pre-#677 multi-shard tenant numbers as a lower bound, not the dataset.

## 0. Prerequisites

- An object store both `ravel-cli` and `sql_latency_bench` can reach. The bench
  reads the `RAVEL_S3_*` env vars (`RAVEL_S3_BUCKET`, `RAVEL_S3_REGION`,
  `RAVEL_S3_ENDPOINT`, `RAVEL_S3_ACCESS_KEY_ID`, `RAVEL_S3_SECRET_ACCESS_KEY`,
  `RAVEL_S3_ALLOW_HTTP`, `RAVEL_S3_FORCE_PATH_STYLE`); point `ravel-cli` at the
  same store with its global `--store` flag. A ~100M-row load needs durable
  storage, so this is the `s3` backend (real S3 or MinIO), not `memory`.
- The `sql-latency` cargo feature builds the harness; it is off by default.

## 1. Fetch the dataset (you run this, not this guide)

`hits.parquet` is on the order of tens of gigabytes. Do not fetch it on a small
host. From the ClickBench repository:

```sh
# One partitioned file (recommended for loading in bounded memory):
#   https://github.com/ClickHouse/ClickBench#data-loading
# Single-file Parquet export:
wget 'https://datasets.clickhouse.com/hits_compatible/hits.parquet'
```

Confirm how your copy encodes the datetime columns before loading. `EventTime`
(the primary event-time column) is handled either way; the mapping's caveats for
the secondary time columns are in `hits.mapping.toml` and in step 3 below.

## 2. Load it

```sh
ravel-cli --store <your-store-flags> load \
  --parquet /path/to/hits.parquet \
  --tenant clickbench \
  --mapping benchmarks/clickbench/hits.mapping.toml \
  --shards 4 \
  --batch-rows 40000 \
  --read-cursors 4 \
  --pipeline-depth 1
```

`--batch-rows` is the object-count lever. One batch is one Strict flush, which is
one RLOG object per shard the batch's rows land on. Per-object cost (LIST,
footer read, decode setup) is paid thousands of times per query and can dominate
everything the columnar and pushdown work saves, so object count is a
first-class variable: raise `--batch-rows` for fewer, larger objects; lower it
for the opposite. Measure both if you care where the time goes.

`--read-cursors` matters because `hits.parquet` is globally sorted by
`CounterID` (issue #560), and `CounterID` is `hits.mapping.toml`'s sole
`resource_attribute`. A single sequential reader therefore fills every
`--batch-rows` batch with one contiguous run of one `CounterID` value: one
`shard_for_log` hash, one shard, and one core doing all of that batch's encode
work while the other `--shards - 1` sit idle. `--read-cursors 4` opens 4 stride
cursors over disjoint, near-even, far-apart row-group partitions and assembles
each batch from a contiguous run out of every live cursor, so one batch spans 4
different regions of the file (and, on this entity-sorted input, 4 different
`CounterID`s) instead of one. Match it to `--shards` so a batch can spread
across every shard; `1` reproduces today's sequential read exactly. A load that
still reports a narrow shard spread prints a stderr warning naming the observed
spread and this flag as one of the levers to raise it.

`hits.mapping.toml` declares `CounterID` as a `resource_attribute` (issue
#519), so it is part of stream identity and `shard_for_log` hashes it to pick
a shard: rows spread across all `--shards` instead of every row landing on
shard 0. That changes the batch-rows arithmetic. A batch's rows now split
across up to `--shards` shards, and each shard's slice flushes as its own
object *immediately* (the loader's `target_bytes` is fixed at `1`) — before it
can reach the `block_target_records` block target (8192 rows). With the
default `--batch-rows 10000` and `--shards 4`, a shard's slice averages ~2500
rows, well under one full block, and object count would inflate well past
`--shards`x rather than by a clean multiple. Raise `--batch-rows` to keep each
shard's average slice at or above 8192: `40000` keeps that margin (`40000 / 4
= 10000` rows/shard) while landing on roughly the same total object count as
the pre-#519 single-shard `10000` batch size at ~100M rows. Lower `--shards`
or raise `--batch-rows` further if a load still reports objects smaller than
expected.

This `--batch-rows`-scales-with-`--shards` sizing floor still applies once
`--read-cursors` is in play: stride reading changes which rows a batch is
assembled *from*, not how many rows in total each shard receives per batch, so
the ~8192-rows-per-shard floor above is unchanged. Raising `--shards` still
means raising `--batch-rows` to match, regardless of `--read-cursors`.

`--pipeline-depth` is the write-latency lever, and it trades memory for it. Each
batch's write is one S3 PUT round trip per involved shard; at the default depth
`1` the loader submits one batch's write and waits for its ack before building or
submitting the next, so on a fast encoder that PUT round trip is the serial term
that dominates wall time. Raising the depth lets up to that many writes overlap:
the loader keeps submitting later batches while earlier writes are still awaiting
their acks, hiding the round-trip latency behind subsequent encode and I/O. The
cost is memory. Each in-flight write keeps its built batch resident until its ack
returns, so raising `--pipeline-depth` above `1` multiplies the live
decoded-batch-plus-pending-write working set by roughly the depth. This cost is
*in addition to* the `--batch-rows` x `--shards` product above, not a
replacement for it: the per-batch resident size is still set by that product, and
`--pipeline-depth` keeps that many built batches alive at once.

As a concrete anchor (issue #682, measured at 8 shards, 80k rows, depth 1): live
heap is about 6GB under a memory-returning allocator (tcmalloc) and scales close
to linearly with `--batch-rows`; under the default glibc allocator the same
geometry plateaus around 20GB because glibc's arenas retain freed blocks rather
than returning them to the OS (arena retention, not a leak). Raising the depth
scales that live working set by roughly the depth on top of whichever allocator
you run. Because the real per-row cost is allocator-dependent by more than 3x,
the loader does not compute or enforce a safe ceiling for you: size
`--pipeline-depth` against your host's memory the same way you size the
`--batch-rows` x `--shards` product, measuring on your own allocator rather than
trusting a single baked-in per-row estimate. `1` reproduces today's
one-write-at-a-time behavior exactly.

The loader prints a completion summary to stdout — `rows processed`,
`objects written`, and `elapsed` (the load wall-time ClickBench reports). It also
prints a stderr warning if any object crossed, or came within 90% of, the
per-object dynamic-column budget of 1000. The `hits` schema is 104 attribute
columns (see step 3), far under that budget, so a clean load prints no such
warning; one appearing means a per-object attribute set is wider than the schema
suggests (stray per-record keys), which is worth investigating before trusting
the numbers.

## 3. Declare the typed columns

The load writes the data; it does not tell the SQL layer which attributes to
treat as typed columns. Derive that declaration from the same mapping:

```sh
ravel-cli --store <your-store-flags> typed-attr-column set clickbench \
  --from-mapping benchmarks/clickbench/hits.mapping.toml
```

This writes ~104 declared columns (every `[[attribute]]` and
`[[resource_attribute]]` entry) through the config CAS path. Notes specific to
`hits`:

- **`EventTime` is not declared.** It is the mapping's `ts_column`, so it rides
  the native typed `ts` path; range predicates over it need no declared column.
  Corpus statements point `EventTime` references at `ts`.
- **Secondary time columns are declared `i64`.** `EventDate`, `ClientEventTime`,
  `LocalEventTime` get the full typed treatment (NumStat pruning, typed
  comparison and pushdown), but as integers: `DeclaredType` has no date/time
  variant. `EventDate` is stored in its native epoch-DAY unit, so a query filters
  it against an epoch-day integer, not a `DATE` literal. The corpus flags those
  statements `modified` with the conversion stated (see the gap/modification
  notes below). `#432` tracks the date/time ergonomic gap.
- **No column is skipped for `f64`.** `ColType::F64` attributes cannot be
  declared (`DeclaredType` has no F64 variant; ADR-0101 / `#431`), so a float
  column would be skipped here with a stderr warning and stay queryable only
  through `attrs['<key>']`. The `hits` schema has **zero** float columns (77
  integer/date/time, 28 text), so this affects nothing here.

## 4. Wait out the declaration staleness horizon

A declaration written by the CLI becomes visible to queries only after the
server's declared-column cache staleness horizon. Do not query immediately after
step 3, or the first queries will run against the old (empty) declaration and
project NULL for every declared column. Wait past the horizon before measuring.
(The `sql_latency_bench --tenant` lane reads the tenant's durable declaration
directly, so it sees a freshly written declaration without the server cache in
the path; the horizon still applies to anything querying through
`ravel-server`.)

## 5. Run the harness

```sh
cargo run -p ravel-bench --features sql-latency --bin sql_latency_bench -- \
  --tenant clickbench \
  --store s3 \
  --corpus benchmarks/clickbench/hits.corpus.json \
  --runs 3 \
  --compaction pre \
  --window-hours 200000
```

- `--tenant clickbench` measures the loaded tenant (not an in-process generated
  dataset). It resolves the tenant's real durable declaration and **skips** any
  statement whose required declared column is absent or the wrong type, rather
  than running it and reporting a plausible-but-wrong latency.
- `--corpus benchmarks/clickbench/hits.corpus.json` runs the checked-in
  ClickBench suite instead of the default Ravel corpus. It is parse- and
  construct-gated before the first query, exactly like the checked-in set.
- `--runs 3` is ClickBench's convention: three runs, first flagged cold. How
  those three map onto ClickBench's published cold and hot columns is in
  "Reading a report against ClickBench" below.
- `--compaction pre|post` labels which layout you measured (freshly loaded, or
  after the maintenance machinery compacted it). Both are legitimate; the delta
  between them is itself a finding, so the report states which one it is.
- `--window-hours` must reach back far enough to cover the data's event-time
  span. ClickBench's `EventTime` values are from 2013, so widen the window well
  past the default 24 hours (relative to `--now-secs`, default the wall clock),
  or the catalog resolve will not see the segments.
- `--sql-max-query-bytes` raises the per-query DataFusion memory-pool ceiling,
  mirroring `ravel-server`'s flag of the same name (ADR-0088). A heavy statement
  (a wide `SELECT *` with a row-wise filter and a pre-`LIMIT` `ORDER BY`) can
  abort with `query memory budget exhausted`; that aborts the whole run with no
  number for it. Pass a larger byte budget (for example `--sql-max-query-bytes
  1073741824` for 1 GiB) to measure it instead. Omitted, it defaults to
  ravel-sql's compiled-in 256 MiB, leaving the measured budget unchanged.
- `--shards <N>` sets how many shards the resolve scans. Omitted, it reads the
  tenant's durable provisioning record (the one `ravel-cli load` writes) and uses
  that record's shard ceiling, so a tenant loaded with `--shards 4` is measured
  over all four. Pass `--shards` explicitly only for a tenant loaded before
  provisioning records existed (that tenant has no record; without the flag the
  run refuses rather than guess). If both a `--shards` value and a record are
  present and they disagree, the run errors rather than silently preferring one.
- `--cache-bytes <N>` attaches an ADR-0046 read cache of `N` bytes to the query
  fetcher, so a statement's repeat runs can serve from cache and the report's
  `cache_hits`/`cache_misses`/`cache_bytes` become meaningful. Omitted (default
  `0`), no cache is attached and the fetcher is byte-for-byte as before. The
  configured budget is recorded in the report's provenance so a table states
  whether a cache was on.
- `--deadline-secs <N>` is the per-statement wall deadline (default `30`, the
  budget every run used before the flag existed). At 100M rows the cold
  `count(*)` alone exceeds 30 s, so a full-size run needs a larger budget; the
  value is recorded in the report's provenance as `deadline_secs`.
- `--continue-on-error` records a statement that fails to execute (deadline
  expiry, `query memory budget exhausted`, a planning error) in the report's
  `failed` list, with the run index and the error verbatim, and moves on to
  the next statement. Omitted, the first failure aborts the run with no report
  at all, which is the right default for a small corpus and the wrong one for
  a discovery pass over a corpus whose magnitudes are unknown. The process
  still exits non-zero when `failed` is non-empty, after writing the report,
  so a partial table cannot pass for a complete one. Pair it with `--runs 1`
  and a generous `--deadline-secs` for the first pass over a new dataset, then
  choose the deadline for the `--runs 3` table from what that pass measured.
- `--fetch-concurrency <N>` is the executor's `fetch_concurrency` (ADR-0088),
  the same knob as `ravel-server --fetch-concurrency`: the logs scan's
  partition count and the bound on in-flight segment fetches per query. Default
  8, ravel-query's compiled-in value and what every earlier run used. A
  full-scan statement's cold time is latency-bound at the object store (a few
  hundred KB per GET, a few MB/s per connection), so it moves nearly linearly
  with this up to the host's cores; the value is recorded in the report's
  provenance as `fetch_concurrency`, and two tables at different values are
  not comparable without it.
- `--progress-jsonl <PATH>` appends one JSON line per finished statement to
  `PATH` as the run goes (`{"outcome":"measured",...}`, `"skipped"`,
  `"failed"`), flushed per line. The full report still goes to stdout at the
  end. Use it on every long run: a 43-statement pass over 100M rows takes
  hours, and without it a kill or a crash at statement 40 leaves nothing.
- `--sql-tenant-max-bytes <N>` is the per-tenant SQL memory ceiling, the same
  knob as `ravel-server --sql-tenant-max-bytes`, and it is a SECOND limit under
  `--sql-max-query-bytes`: the per-query pool bounds one statement, this bounds
  a tenant across its concurrent queries. A statement refused by it reports
  `tenant memory budget exhausted`, not `query memory pool exhausted`, so a
  heavy aggregate needs both raised together. Default 1 GiB, what every earlier
  run used.
- `--sql-parallel-final-aggregation` lets an exact-typed query repartition its
  final aggregation (ADR-0094), the same knob as the server flag of that name.
  Off by default; a `GROUP BY` over a high-cardinality key is where it shows.
- A resolve that finds **0 objects** is now an error naming the tenant, the
  resolved shard count, the window, and `now_ns`, rather than a silent report
  over an empty dataset. A wrong `--window-hours` (the event-time span the
  data does not fall in) or a wrong tenant is therefore loud, not a table of
  `0 objects, 0 rows` statements that all "passed" in a few milliseconds.

#### CPU flamegraph pass (use `--runs 1`)

To capture a CPU flamegraph of the corpus, build with `--features
sql-latency,profiling` and set `RAVEL_BENCH_PROFILE_SVG` to the output path.
For a profiled pass, run it with **`--runs 1`**, not `--runs 3`:

```sh
RAVEL_BENCH_PROFILE_SVG=/tmp/sql_latency.svg \
cargo run -p ravel-bench --features sql-latency,profiling --bin sql_latency_bench -- \
  --tenant clickbench --store s3 \
  --corpus benchmarks/clickbench/hits.corpus.json \
  --runs 1 --compaction pre --window-hours 200000
```

The profiler is a signal sampler, and running each statement more than once
under a live sampler has been observed to segfault the process (issue #616);
`--runs 1` is stable. This costs nothing for a profile: one execution already
yields a dense flamegraph, and profiled latency numbers are inflated by the
sampler and not usable anyway. Take latency from a separate unprofiled `--runs
3` pass, and read the flamegraph for CPU attribution only. See
`crates/ravel-bench/src/profiling.rs` for the mechanism.

The same signal-safety hazard also fired once the corpus's logs scan lane ran
its segment prunes and scan partitions concurrently, segfaulting even at
`--runs 1` (issue #680). To keep the exposure down, the query lanes
(`sql_latency_bench`, `query_latency_bench`) now sample at 199 Hz rather than
the ingest lane's 997 Hz; this is the configuration the in-process sampler is
known to survive on the ClickBench corpus. It is still a probabilistic hazard,
not a proof of safety: if a profiled query run faults on your host, do not raise
the rate. Fall back to `perf record --call-graph dwarf` on the box, which
unwinds out of process instead of inside the target's signal handler; the load
and query flamegraphs on issue #680 were produced that way.

### How to read the report

The bench prints the full report as JSON, then a human table. Per statement:

- **min / median / max ms** over the `--runs` executions, and **cold ms** (the
  first run, against a fresh `Catalog` + `SqlExecutor` per statement so it is
  genuinely cold). Ratios only, unless both runs tick the comparability
  checklist below; see the reproducibility note above.
- **rows returned.**
- **scan diagnostics**, which say *where* the time went rather than only that it
  was slow: `segments`, `blocks_total`, `blocks_scanned`,
  `blocks_pruned_by_postings` (POSTINGS pruning selectivity), plus the cold run's
  object-store GET/LIST request counts, bytes transferred, and fetch-cache
  hits/misses/bytes.

Dataset-level, independent of any one query:

- **load wall-time**: `0` in the `--tenant` lane (the load ran out of process;
  read its time from the `ravel-cli load` summary's `elapsed`), measured in the
  `--generate` lane.
- **stored bytes** and **object count**: summed over the resolved snapshot's
  segments. Object count is the `--batch-rows` consequence from step 2 made
  visible.
- **rows** and the **pre-/post-compaction layout label**.

Provenance (backend, region, endpoint, host logical cores, source, dataset id,
runs, `cache_bytes`, `deadline_secs`, `fetch_concurrency`) is recorded beside
the numbers so two runs are comparable or provably not.

## Reading a report against ClickBench

ClickBench's published tables come from running each query three times on one
node with a warm local disk, reporting all three. The table's "cold" column is
run 1 and its "hot" column is the best of runs 2 and 3. Ravel's analogue is
`--runs 3`: `cold_ms` is run 1 against a fresh executor, catalog, and fetcher
cache, and the hot figure is `min_ms` taken over runs 2 and 3, which are served
from the ADR-0046 read cache attached with `--cache-bytes`. State it plainly in
the report: a run without `--cache-bytes` has no hot column at all, because
every run re-reads the object store and the three numbers measure the cold path
three times. A `--runs 1` report is a discovery pass, not a table.

### Comparability checklist

Tick every line before putting two reports side by side.

- Same instance type and the same instance. Issue #680 measured a 1.6x to 2x
  gap between two c6a.4xlarge boxes at identical settings, so a cross-box
  comparison carries the box id or it carries nothing.
- Same `fetch_concurrency`, `cache_bytes`, `deadline_secs`, and
  `sql_max_query_bytes`. The first three are in the provenance; the last is the
  DataFusion pool ceiling the run was given.
- Same allocator. An `LD_PRELOAD` of tcmalloc against the default glibc changes
  peak RSS by about 2x, and the allocator is not in the provenance, so it goes
  in the report's caption.
- Same dataset stanza: object count, rows, and the layout label from
  `--compaction`.

### Reading `failed`

A `query memory budget exhausted` failure is a statement the DataFusion pool
could not hold, not a scan failure, and that pool is `--sql-max-query-bytes`. A
`wall deadline` failure is `--deadline-secs`. Neither is a number, and neither
is omitted from the table: they stay as rows that say why there is no number.

### The per-statement `scan` block

`object_store_bytes` and `object_store_get_requests` are the cost the cold
column paid. A full-window statement over an N-object tenant reads every
object, because the plan phase reads the whole dataset; issues #693 and #699
are the two open changes to that. So the figure to compare across runs is bytes
per second at the reported `host_logical_cores`, not the statement's row count.

## Gap list: ClickBench statements the construct gate rejects

Running a 43-query suite against a supported-construct gate means some queries
fail the gate rather than return a number. That is the intended outcome: an
unsupported construct becomes a **named capability gap with a failing query
attached**, not an omission from a results table. The checked-in corpus now
holds **all 43** statements; the gap list is empty (enforced by
`crates/ravel-bench/tests/clickbench_corpus.rs`, which fails if any of the 43 is
neither in the corpus nor listed as a gap).

There are currently no known gaps.

**`LIKE` / SQL pattern matching** used to block Q21-Q24 and is now supported
(issue #479): `col LIKE 'pattern'` / `NOT LIKE` with `%`/`_` wildcards is
evaluated by the Ravel `like` UDF (`crates/ravel-sql/src/like_udf.rs`), which
matches a declared `Str` column's dictionary once per distinct value and leaves
`body` (plain `Utf8`) on a row-wise path. It is case-sensitive and pushes down
nothing: substring `LIKE` is not a sound superset of the RLOG reader's exact
`HasWord`/`Equals` predicates, so it is evaluated exactly over the scanned rows.
Ravel also offers token search (`has_word`) and `regexp_replace`.

Q28/Q29 (`AVG(length(...))`) were also blocked, but that gap was bookkeeping,
not capability: `length` was already admitted by the SQL engine, just not
enumerated as a named construct in `ravel_sql::conformance::registry()` (the
registry attests scalar functions by family representative and did not yet
have an individual row for `length`). Issue #480 added that row; Q28 and Q29
now run as ordinary corpus entries.

## Modified statements

Any rewrite that changes what a statement *computes* is flagged `modified` in the
corpus with a stated reason; pure renames (`hits`→`logs`, identifier quoting for
`hits`'s CamelCase column names, `EventTime`→`ts`, and the `extract(minute FROM
EventTime)`→`date_part('minute', ts)` spelling swap, which computes the same
minute) are not. The flagged statements are exactly those touching the
secondary time column `EventDate`: because it is declared `i64` in epoch-days,
`DATE` range literals become epoch-day integers (e.g. `'2013-07-01'` → `15887`,
`'2013-07-31'` → `15917`, `'2013-07-14'` → `15900`, `'2013-07-15'` → `15901`),
and `MIN`/`MAX` over it return epoch-day integers rather than `DATE` values. This
is the epoch-integer-comparison consequence ADR-0100 decision 3 requires be
flagged. The affected statements are Q7 and Q37–Q43.
