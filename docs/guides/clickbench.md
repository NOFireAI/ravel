# Running ClickBench against Ravel

ClickBench is an external, public analytical benchmark: a flat 105-column,
~100M-row `hits` table and a fixed suite of 43 statements, reported as the
minimum / median / maximum of three consecutive runs per query alongside the
load time and the stored size (ADR-0100). This guide makes that workload
runnable and repeatable against Ravel. It never fetches the dataset for you and
never reports a number: it is the procedure, not a measurement.

For running this on AWS from nothing -- bucket, credentials, IAM role, EC2 box,
then the same load and measure steps as copy-pasteable commands -- see
[clickbench-aws-runbook.md](clickbench-aws-runbook.md). This guide remains the
reference for what the harness measures and how to read its report.

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
runs, `cache_bytes`, `deadline_secs`, `max_concurrent_gets`, `scan_partitions`).
A latency table without its backend named will mislead the first person who
compares two runs.

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
  --pipeline-depth 4 \
  --max-inflight-flushes 4 \
  --decode-queue-batches 2 \
  --target-bytes 1
```

The two write-concurrency flags are spelled out here only because this is a
worked example; both already default to `4`, so omitting them loads the same
way. Set them to `1` to reproduce the pre-issue-#800 serial behaviour.

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
object *immediately* (at the default `--target-bytes 1`) — before it
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

`--target-bytes` is the other object-size lever, and unlike `--batch-rows` it
does not multiply Arrow batch memory. It is not free, though: a larger target
means each active shard holds a larger *encoded* buffer until it flushes, so
total encoded-buffer memory scales with the target times the shard count. What
stays bounded is the Arrow side, which is what made `--batch-rows` expensive. At the default `1` a shard's slice of a batch flushes as
its own object the moment it is written: one object per involved shard per
batch, `--batch-rows` sets its size, no buffer lingers. Any larger value is a
byte target the shard accumulates *encoded* records toward across several
batches before it flushes, so objects grow without any more Arrow batches being
held resident. That is the difference that matters at 100M rows: raising
`--batch-rows` for bigger objects costs memory linearly, because each Arrow
batch is buffered whole (measured on issue #801: 6x the batch rows reached
27.4GB RSS on a 30GB box in 2.5 minutes), while raising `--target-bytes` costs
one partially-filled shard buffer per shard. For reference, the default load
geometry at `--batch-rows 65536` over 100M rows wrote 8424 objects of 1.32MB
each: 16,850 PUTs during the load and an 8424-GET floor on every query
afterward. `IngestConfig`'s own default target is 8MiB.

The trade is ack timing, not durability. A Strict write's ack is still sent
only after that flush's data object and commit record are published, so an ack
always means the records are durable. But above `1` the flush that answers a
batch's ack may be triggered by a *later* batch reaching the target, so that
ack now waits for one; a buffer that never reaches the target is released by
the ingest router's wall-clock age trigger instead (`max_flush_delay`, 2s),
which is also what releases the tail of every load. Two consequences for a bulk
load: set `--pipeline-depth` to at least the number of batches that accumulate
into one flush, or the loader blocks on an ack no submitted batch can release
and every flush waits out that 2s timer; and objects bucket by their flush-open
wall-clock reading, which above `1` is later than the write that filled them.
`0` is rejected.

`--pipeline-depth` and `--max-inflight-flushes` are the two write-concurrency
levers, and they compose:

```text
concurrent_object_writes = active_shards * min(pipeline_depth, max_inflight_flushes)
```

`active_shards` is the number of shards a batch actually routes rows to, which
equals the configured `--shards` only when every batch touches every shard. With
narrower fan-out the configured value is a ceiling, not the multiplier.

`--pipeline-depth` bounds the batch writes the loader keeps outstanding;
`--max-inflight-flushes` bounds the flushes any one shard actor will run at
once. Because the term is a `min`, **neither one alone changes anything**. At
depth `1` the loader awaits each batch's every-shard ack before submitting the
next, so a shard is never asked for a second concurrent flush and its extra
permits go unused; at one permit the shard serialises its PUT round trips
however many batches the loader hands it. Measured on a 16-batch fixture with a
40ms injected data-object PUT and one shard (issue #800): depth 1 / flushes 1
took 673.9ms, depth 4 / flushes 1 took 671.9ms, depth 1 / flushes 4 took
673.3ms, and depth 4 / flushes 4 took 169.3ms. The first three are the same
number.

Both therefore default to `4`, and the recipe for tuning further is to raise
them together (and to raise `--shards` toward core count, which is a
provisioning decision made when the signal is provisioned, not a per-load
knob).

The cost is memory, and it belongs to `--pipeline-depth` alone. Each in-flight
write keeps its built batch resident until its ack returns, so the depth
multiplies the live decoded-batch-plus-pending-write working set. This cost is
*in addition to* the `--batch-rows` x `--shards` product above, not a
replacement for it: the per-batch resident size is still set by that product,
and `--pipeline-depth` keeps that many built batches alive at once.
`--max-inflight-flushes` adds nothing further on this path, because the
outstanding batches it flushes concurrently are already capped by the depth; it
only decides whether that same bounded set of objects is encoded and PUT
concurrently or one at a time.

Setting `--max-inflight-flushes` *below* `--pipeline-depth` is the shape to
avoid: each shard's excess batches queue on the flush semaphore, and they still
have to clear it inside the 60s Strict ack deadline. A large depth against one
permit is how a load fails with `timed out waiting for shard ack`.

The reported durable-token list is correct at any depth. On a partial-load
failure the loader resolves every outstanding write before returning, rather
than abandoning it, so the list is exactly the batches that committed: those
before the failure, then any submitted after it whose own write landed anyway
(the loader cannot stop a shard actor already mid-PUT, so it waits for the
outcome instead of guessing). The list neither omits a batch that committed nor
names one that did not.

That is a statement about the report, not a resume mechanism. The loader has no
resume mode: re-running re-ingests the whole file and there is no dedup, so a
re-run duplicates every row the failed attempt did commit. The list tells an
operator which rows are already durable; acting on it is their decision.

`--decode-queue-batches` is the decode/encode overlap lever (issue #680). A
bounded channel sits between the Parquet reader plus `build_columnar_batch`
stage and the shard writers, so the reader decodes batch N+1 (and, with
`--read-cursors > 1`, stride-reads several row-group regions in parallel) while
the writers are still flushing batch N, instead of the two stages alternating in
lockstep with each waiting on the other. Its default of `2` lets the reader run
up to two batches ahead; raise it if the decode stage is starving the writers,
lower it to `1` to bound the queue tightest. The reader blocks when the channel
is full, so the queue holds at most `--decode-queue-batches` built batches — that
much memory again on top of both the `--batch-rows` x `--shards` per-batch size
and the `--pipeline-depth` in-flight-write working set. Size the peak working set
as roughly `(--pipeline-depth + --decode-queue-batches + 2)` built batches (the
in-flight writes, the queued batches, plus one in each stage's hand). The RLOG
objects a load writes are byte-identical regardless of this flag: only the
scheduling of the unchanged decode and write work moves.

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
trusting a single baked-in per-row estimate. `1` reproduces the
one-write-at-a-time behaviour the loader had before issue #800; the shipped
default of `4` raises the in-flight batches 4x, which takes that anchor's live
working set from 3 to 6 batches, roughly `2x` on the same geometry. That is the
memory the default spends.

The loader prints a completion summary to stdout — `rows processed`,
`objects written`, `elapsed` (the load wall-time ClickBench reports), and a
`flush triggers` breakdown. It also
prints a stderr warning if any object crossed, or came within 90% of, the
per-object dynamic-column budget of 1000.

`objects written` is not reproducible across runs of the same command, and is
not a function of the command line alone (issue #983): input order concentrates
consecutive rows on one shard, and the 2-second `max_flush_delay` age trigger
means a slower host ages more buffers out before they reach `--target-bytes`,
changing the layout. Two loads of the same file can therefore write different
object counts and still be correct. The comparison basis that *is* stable is the
`flush triggers` line, which states how many flushes each cause opened: `size`
(a buffer reached `--target-bytes`), `age` (a buffer aged past `max_flush_delay`),
and `final` (the drain at load close), per shard and as load totals. The three
causes are disjoint and sum to the objects written, so a mix that shifts from
`size` toward `age` between two runs explains an object-count difference that the
count alone cannot. Compare the mix, not the raw count. The `hits` schema is 104 attribute
columns (see step 3), far under that budget, so a clean load prints no such
warning; one appearing means a per-object attribute set is wider than the schema
suggests (stray per-record keys), which is worth investigating before trusting
the numbers.

### Fold the catalog before measuring anything

The load's writer process has exited by this point, so nothing can publish
another commit record into the hours it wrote. Fold them immediately instead of
waiting out the seal margin:

```sh
ravel-cli --store <your-store-flags> catalog fold \
  --tenant clickbench \
  --shards 4 \
  --signal logs \
  --max-flush-lifetime 0s
```

`--shards` must be the value the load ran with, and `--signal logs` is
mandatory: the fold defaults to metrics, and folding metrics on this
logs-only tenant seals nothing and publishes an empty metrics HEAD.

Check the report before going further. `seal_margin: 20m` confirms the
override took effect (the default is `1h 20m`), and that residual margin is
also why `entry_count` may still be below the `objects written` figure the
load printed: `0s` removes only the flush-lifetime term, so an ingest hour
seals 20 minutes after it ends, and the hour the load finished in (plus the
previous one, for 20 minutes) stays hot. The equality `entry_count == objects
written` is the bench precondition, not the load's exit check: re-run the fold
once 20 minutes have passed since the last loaded hour ended, and only then
measure. Anything still lower at that point means part of the load is outside
the snapshot.

This is not optional tuning. Without a snapshot covering these hours, every
query resolves by listing and reading commit records directly: one
commit-record GET per segment, per statement. On a 100M-row `hits` load that
is 8,424 GETs for a single statement, and a 43-statement pass measured that
way reads as a format or engine regression when it is only an unfolded
catalog. A tenant loaded from 14:54Z to 16:08Z cannot be folded by the default
margin until 18:21Z, which is exactly the window this flag removes.

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
  ravel-sql's compiled-in 256 MiB, leaving the measured budget unchanged. The
  report's provenance records it twice: `sql_max_query_bytes_requested` is what
  the run asked for, and `sql_max_query_bytes_effective` is what governed. On
  the in-process lanes they are the same. Under `--flight` the flag is not sent
  to the server (it is not a Flight header), so the server's own ceiling
  governed and `effective` is null: two Flight tables at different
  `--sql-max-query-bytes` values are NOT comparable on that basis, and the null
  is what stops them being mistaken for it.
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
- `--max-concurrent-gets <N>` is the executor's `EngineConfig::fetch_concurrency`
  (issue #846), the same knob as `ravel-server --max-concurrent-gets`: the bound
  on concurrent in-flight object-store GETs per query. Default 8, ravel-query's
  compiled-in value and what every earlier run used. A full-scan statement's
  cold time is latency-bound at the object store (a few hundred KB per GET, a
  few MB/s per connection), so it moves nearly linearly with this; the value is
  recorded in the report's provenance as `max_concurrent_gets`.
- `--scan-partitions <N>` is the executor's `EngineConfig::scan_partitions`, the
  same knob as `ravel-server --scan-partitions`: the SQL scan partition count
  (`target_partitions`). This is the CPU-side lever, so it moves a hot run that
  reads zero bytes, which the GET bound cannot. Unset, it couples to
  `--max-concurrent-gets` (the pre-split behavior) and the report says so.
  Recorded in the provenance as `scan_partitions`, absent when unset.
- Before issue #846 one `--fetch-concurrency` set both of the above, so no sweep
  taken with it was attributable to one or the other. It is kept as a deprecated
  alias of `--max-concurrent-gets`, warns on stderr, and conflicts with it. Two
  tables at different values of either knob are not comparable without both
  values, which is why the report stamps both.
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
  final aggregation (ADR-0094, amended by issue #741), the same knob as the
  server flag of that name. On by default; a `GROUP BY` or `COUNT(DISTINCT)`
  over a high-cardinality key is where it shows. With the flag off, nine such
  statements failed with a pool-exhausted error; with it on, **five of those
  nine** (`COUNT(DISTINCT UserID)` and four more) moved to 44-50 s, while the
  other **four (q29, q33, q34, q35) still exhaust the pool** for a separate
  reason (see the ADR-0094 2026-08-26 amendment and its "still excluded" note).
  Pass `--sql-parallel-final-aggregation=false` to measure the pre-amendment
  single-partition final; the bare flag stays accepted and still means on. This
  local value is recorded in the report's provenance as
  `parallel_final_aggregation_requested`; for an in-process (`--tenant`) run it
  is also the effective value (`parallel_final_aggregation_effective`), but a
  `--flight` run does not send the setting to the server, so its effective value
  is the server's own default (on) unless the server was started otherwise, and
  the report records `parallel_final_aggregation_effective` as null.
- `--sql-max-segments <N>` is the engine's `max_segments` ceiling, the same knob
  as `ravel-server --max-segments`: the number of sealed, below-watermark
  segments a statement may fan out over before it is refused with `query fans
  out over too many segments` (ADR-0073 decision 2). Default 1024, ravel-query's
  compiled-in value. Only sealed, below-watermark segments count, so a freshly
  loaded ClickBench tenant (8,424 objects) sits far above the ceiling yet never
  trips it until a fold seals its hours; once it gets a logs snapshot every
  statement fails in a fraction of a second with `8424 exceeds max 1024`, so a
  folded tenant needs this raised past its sealed-segment count. The value is
  recorded in the report's provenance as `sql_max_segments`.
- `--explain` writes each statement's physical plan to
  `--explain-dir <dir>/<id>.txt` (one file per statement, `--explain-dir`
  required when `--explain` is set) before measuring it, so the DataFusion
  optimizer rules that fired (`AggregateStatistics`,
  `single_distinct_to_groupby`, projection/filter pushdown) are readable per
  statement without a debugger. The plans are a side artifact: not timed, never
  part of the report's numbers; the provenance records `explain: true`. The
  `--flight` lane has no in-process plan to display and ignores it.
- A resolve that finds **0 objects** is now an error naming the tenant, the
  resolved shard count, the window, and `now_ns`, rather than a silent report
  over an empty dataset. A wrong `--window-hours` (the event-time span the
  data does not fall in) or a wrong tenant is therefore loud, not a table of
  `0 objects, 0 rows` statements that all "passed" in a few milliseconds.

#### CPU flamegraph pass (use `--runs 1`)

To capture a CPU flamegraph of the corpus, build with `--features
sql-latency,profiling` and set `RAVEL_BENCH_PROFILE_SVG` to the output path.
For a profiled pass, run it with **`--runs 1`**, not `--runs 3`: with
`RAVEL_BENCH_PROFILE_SVG` set, any `--runs` above 1 is REFUSED with an error
rather than run (issue #616), so this is a rule the binary enforces, not advice.

```sh
RAVEL_BENCH_PROFILE_SVG=/tmp/sql_latency.svg \
cargo run -p ravel-bench --features sql-latency,profiling --bin sql_latency_bench -- \
  --tenant clickbench --store s3 \
  --corpus benchmarks/clickbench/hits.corpus.json \
  --runs 1 --compaction pre --window-hours 200000
```

The profiler is a signal sampler, and running each statement more than once
under a live sampler has been observed to segfault the process (issue #616), so
the binary refuses that combination outright; `--runs 1` is stable. This costs nothing for a profile: one execution already
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
  hits/misses/bytes. Present for the in-process lanes only; the `--flight` lane
  of step 6 omits the whole block, for the reason given there.

Dataset-level, independent of any one query:

- **load wall-time**: `0` in the `--tenant` lane (the load ran out of process;
  read its time from the `ravel-cli load` summary's `elapsed`), measured in the
  `--generate` lane.
- **stored bytes** and **object count**: summed over the resolved snapshot's
  segments. Object count is the `--batch-rows` consequence from step 2 made
  visible.
- **rows** and the **pre-/post-compaction layout label**.

Provenance (backend, region, endpoint, host logical cores, source, dataset id,
runs, `cache_bytes`, `deadline_secs`, `max_concurrent_gets`, `scan_partitions`,
`parallel_final_aggregation_requested` (the local CLI value) and
`parallel_final_aggregation_effective` (the value that governed execution:
equal to the request for an in-process lane, null for a `--flight` run whose
effective setting is the server's), and `flight_endpoint` when step 6's lane
ran) is recorded beside the numbers so two runs are comparable or provably not.

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
- Same `max_concurrent_gets`, `scan_partitions`, `cache_bytes`, `deadline_secs`,
  and per-query pool ceiling. All five are in the provenance for an in-process
  run; on a `--flight` run the two knobs are stamped null because they never
  left the process (the remote server's own config governed), so two Flight
  reports are comparable on them only via the server's flags, not the bench's.
  Compare on
  `sql_max_query_bytes_effective`, not on what was requested. Where that field
  is null (a `--flight` run), the ceiling that governed is the server's and is
  not recorded here, so the two runs are comparable on it only if you know both
  servers were configured the same.
- Same allocator. An `LD_PRELOAD` of tcmalloc against the default glibc changes
  peak RSS by about 2x. The allocator is in the provenance (the `allocator`
  field, resolved at runtime by reading the process's mapped libraries from
  `/proc/self/maps`, so an `LD_PRELOAD` shows up as `tcmalloc`/`jemalloc`/
  `mimalloc` and a plain run as `system`), so compare on that field, not on a
  caption. A run whose allocator could not be probed records `unknown`; two
  reports are comparable on peak RSS only if both name the same allocator and
  neither is `unknown`.
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

### The per-statement `per_run_accounting` block

One entry per run, in run order, so index 0 is the cold run and index 1 the
warm one. It carries the same object-store and cache figures as `scan` plus
`probe_misses_plan` and `probe_misses_scan`: tail sections (SKIP_IDX, and
PAGE_DIR on a version-4 object) that the run's suffix probe did not reach,
split by the phase that issued the probe.

Read them against `object_store_get_requests`, and read them as uncovered tail
SECTIONS rather than as GETs. A short version-4 probe can miss SKIP_IDX and
PAGE_DIR both, incrementing twice, while the fetcher coalesces their adjacent
ranges into a single GET -- so the count bounds the extra requests from above
and never maps one-to-one. A run whose GETs rose alongside its probe misses
paid for the probe length; one whose GETs rose with probe misses flat did not. This is the number that
gates any tightening of the probe floor (`LOG_SUFFIX_FLOOR_BYTES`): a change
that trades probe bytes for requests is a win only if these stay where they
were. They are measured against the probe window rather than against the read
cache, so the warm run reports the same counts as the cold one; a difference
between the two runs means the plan shape changed, not that the cache helped.
The `pmiss` column in the bench's text table is the cold run's two phases
summed.

## 6. Through the server (Flight SQL)

Everything above measures the SQL executor as a library, in the bench's own
process. A number a user would see goes through `ravel-server` over Flight SQL:
server-side planning and admission, gRPC, Arrow IPC encode on the server and
decode on the client. Those are different numbers, and the second one is what a
published result claims. The `--flight` lane runs the same corpus, over the same
tenant, into the same report, through a running server.

Start the server against the same bucket, built with its `flight-sql` feature.
Flight SQL is served on the gRPC listener:

```sh
cargo run -p ravel-server --features flight-sql --bin ravel-server -- \
  --mode query \
  --store s3 \
  --listen-grpc 127.0.0.1:4317 \
  --tenant-token "$RAVEL_FLIGHT_TOKEN=clickbench" \
  --shards 4 \
  --max-concurrent-gets 8 \
  --sql-max-query-bytes 1073741824 \
  --sql-tenant-max-bytes 2147483648 \
  --max-segments 1024 \
  --cache-max-bytes 268435456
```

The flags that must mirror the bench's, or the two tables are not comparable:

- `--max-concurrent-gets` and `--scan-partitions` are the same issue #846 knobs
  as the bench's flags of the same names (the in-flight GET bound and the SQL
  scan partition count). The GET bound is the one that moves a cold full scan the
  most; set both sides to the same values and record them.
- `--sql-max-query-bytes` is the per-query DataFusion memory-pool ceiling. A
  statement that fits the bench's budget and not the server's aborts on the
  server with `query memory budget exhausted` and lands in `failed`.
- `--sql-tenant-max-bytes` has no bench counterpart: it is the server's
  per-tenant ceiling across concurrent queries, which an in-process lane running
  one statement at a time never reaches. Set it above
  `--sql-max-query-bytes` so it is not the binding limit for a serial run.
- Cache flags: the bench's `--cache-bytes` attaches an ADR-0046 read cache to
  its own fetcher; the server's equivalent is `--cache-max-bytes` (plus
  `--cache-dir` for the disk tier, and `--disable-cache` to turn it off). To
  compare against a `--cache-bytes 0` bench run, start the server with
  `--disable-cache`; otherwise match the byte budgets. The server's cache is
  process-wide and survives between statements, so a warm server does not
  reproduce the bench's per-statement cold run.

Then point the bench at it. Two lines:

```sh
export RAVEL_FLIGHT_TOKEN=<the token side of --tenant-token>
cargo run -p ravel-bench --features sql-latency,flight-lane --bin sql_latency_bench -- \
  --tenant clickbench --store s3 --flight 127.0.0.1:4317 \
  --corpus benchmarks/clickbench/hits.corpus.json \
  --runs 3 --compaction pre --window-hours 200000 \
  --max-concurrent-gets 8 --sql-max-query-bytes 1073741824
```

- `--flight <host:port>` is the server's `--listen-grpc` address. It needs the
  `flight-lane` build feature; without it the run fails with an error naming the
  feature rather than quietly measuring in process.
- `--flight-token <TOKEN>` passes the credential on the command line;
  `RAVEL_FLIGHT_TOKEN` is the better place for it, since a token in the argument
  vector lands in the shell history and in `ps`. It is sent as `authorization:
  Bearer <TOKEN>` and must be the token side of the server's `--tenant-token
  <TOKEN>=<TENANT>` pair.
- `--store` and `--tenant` are still required and still used. The dataset stanza
  (objects, bytes, rows, layout) and the tenant's declared columns are resolved
  from the object store **directly**, not through the server: a Flight client
  cannot read the tenant's catalog, and the declared-column skip check needs the
  declarations. So this lane needs object-store credentials as well as a server.
- `--window-hours` / `--now-secs` reach the server in the request metadata, as
  `x-ravel-start` and `x-ravel-end` in Unix float seconds. The Flight SQL
  command carries no window of its own, so this is how ravel-sql's Flight
  service reads it, exactly as the HTTP endpoint reads `start`/`end` from the
  JSON body. `--deadline-secs` travels the same way as `x-ravel-timeout` and is
  clamped by the server's own maximum, which a client can shorten but never
  extend.
- `--runs`, `--corpus`, `--continue-on-error`, and `--progress-jsonl` behave
  exactly as in step 5.

**The Flight lane's report has no `scan` block.** `segments`, `blocks_total`,
`blocks_scanned`, `blocks_pruned_by_postings`, the object-store GET/LIST counts,
the bytes, and the cache hits and misses are all read off the executor's own
counters inside the process that ran the query. A Flight SQL response carries
result batches, not the server's internal accounting, so the bench has no way to
observe them from the client side. Rather than report zeros, which would read as
"this statement scanned nothing", the `scan` field is omitted from the JSON
entirely and the human table prints `-` in those columns. `provenance.source` is
`"flight"` and `provenance.flight_endpoint` names the address, so a report
cannot be mistaken for an in-process one. Everything else -- `cold_ms`,
`min_ms`, `median_ms`, `max_ms`, `rows_returned`, `skipped`, `failed`, and the
progress stream -- is identical. When you need the attribution, run step 5's
in-process lane over the same tenant and read the two tables together: the
in-process one says where the time went, this one says what the user waits.
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
