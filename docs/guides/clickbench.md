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
backend (local filesystem vs MinIO vs S3). Only **ratios within one run** are
comparable: query A versus query B, cold versus warm, pre- versus
post-compaction, one `--batch-rows` layout versus another. That is why every
report carries its own provenance (backend, region/endpoint, host logical cores,
dataset id, runs). A latency table without its backend named will mislead the
first person who compares two runs.

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
  --read-cursors 4
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
- `--runs 3` is ClickBench's convention: three runs, first flagged cold.
- `--compaction pre|post` labels which layout you measured (freshly loaded, or
  after the maintenance machinery compacted it). Both are legitimate; the delta
  between them is itself a finding, so the report states which one it is.
- `--window-hours` must reach back far enough to cover the data's event-time
  span. ClickBench's `EventTime` values are from 2013, so widen the window well
  past the default 24 hours (relative to `--now-secs`, default the wall clock),
  or the catalog resolve will not see the segments.

### How to read the report

The bench prints the full report as JSON, then a human table. Per statement:

- **min / median / max ms** over the `--runs` executions, and **cold ms** (the
  first run, against a fresh `Catalog` + `SqlExecutor` per statement so it is
  genuinely cold). Ratios only — see the reproducibility note above.
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
runs) is recorded beside the numbers so two runs are comparable or provably not.

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
