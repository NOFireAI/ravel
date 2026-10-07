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
backend (local filesystem vs RustFS vs S3). They do not reproduce across two
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
  storage, so this is the `s3` backend (real S3 or RustFS), not `memory`.
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

That is a statement about the report, not a resume mechanism. Re-running from
the start re-ingests the whole file and there is no dedup, so it duplicates
every row the failed attempt did commit. `--skip-rows` is the positional resume,
and it only means anything for a load that was started with `--read-cursors 1
--pipeline-depth 1`; at the settings this runbook otherwise recommends, the
rows that landed are not a contiguous prefix and no single offset describes
them. A failed load prints the figures and says which case it is. See the
`--skip-rows` section of docs/guides/ingest.md before resuming a ClickBench
load.

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

The loader prints a completion summary to stdout — `rows_written`
(`rows processed` before the resume work), `rows_skipped`, `objects written`,
`elapsed` (the load wall-time ClickBench reports), and a `flush triggers`
breakdown. It also
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
- **`f64` columns are skipped, and `hits` has none.** `ColType::F64` attributes
  cannot be declared (`DeclaredType` has no F64 variant; ADR-0101 / `#431`), so a
  float column would be skipped here with a stderr warning and stay queryable only
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
  --window-hours 200000 \
  --sql-max-segments 1000000 \
  --cache-bytes 25769803776
```

- `--sql-max-segments 1000000` lifts the engine's sealed-segment ceiling from
  its default of 1024; a folded ClickBench tenant sits far above that and
  every statement would otherwise fail with `8424 exceeds max 1024`.
- `--cache-bytes 25769803776` (24 GiB) exceeds the 9,844,635,064-byte corpus,
  so the second and third runs have a hot column to report. Leave it off for
  a cold-only pass.
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
- `--fetch-concurrency <N>` is the executor's `fetch_concurrency` (ADR-0088),
  the same knob as `ravel-server --fetch-concurrency`: the logs scan's
  partition count and the bound on in-flight segment fetches per query. Default
  8, ravel-query's compiled-in value and what every earlier run used. A
  full-scan statement's cold time is latency-bound at the object store (a few
  hundred KB per GET, a few MB/s per connection), so it moves nearly linearly
  with this up to the host's cores; the value is recorded in the report's
  provenance as `fetch_concurrency`, and two tables at different values are
  not comparable without it.
- `--logs-fetch-policy <policy>` is the logs read shape, the same knob and the
  same value names as `ravel-server --logs-fetch-policy`, resolved by the bench
  through the same resolution the server runs at startup. `request-minimal`
  reads every log object whole in one covering GET, with no tail probe and no
  ranged read; `byte-minimal` uses ranged reads wherever they save more bytes
  than a request costs; `cost-based` (the bench's default) derives the
  choice from the pass's store cost profile, which at the shipped reference
  intra-region profile no longer resolves to request-minimal behaviour: the
  rate is the profile's time term, 6,300,000 bytes per request, and a narrow
  projection reads an object ranged only where it skips more than the
  18,900,000-byte projection break-even, so objects of that size or less are
  still read whole; `latency-first`
  resolves the byte-minimizing quantities as an intent rather than from prices,
  and pays off only at the concurrency its trade was measured at, which a pass
  sets with `--fetch-concurrency`. So a bench run at
  default flags measures the shape a stock server produces, loopback
  `--s3-endpoint` or not (ADR-2023 decision 1): roughly one GET per object
  for a full-scan statement, not one per block, including against the
  ClickBench entry's local RustFS. Before the flag existed
  the bench routed at a fixed 512 KiB threshold, which range-read every larger
  object per block and made an in-process table incomparable with the server's;
  pass `byte-minimal` to reach that older shape deliberately. The requested
  policy is recorded in the report's provenance as `logs_fetch_policy`.
- `--logs-block-range-threshold <BYTES>` is the object size above which a logs
  read takes the ranged per-block path, the same knob as the server flag of that
  name. Unset, the resolution's input is ravel-query's compiled-in 512 KiB. A
  policy that resolves to whole-object reads OVERRIDES it, exactly as on the
  server, so read what actually governed off the report's
  `logs_block_range_threshold_effective` rather than off the flag. Both figures
  are null on a `--flight` run, whose routing the server's own config decided.
- `--logs-request-cost-bytes <BYTES>` is the expert escape hatch under the
  policy, the same knob as the server flag of that name: set, it wins over the
  policy's derived rate; unset, the policy derives it. The report records what
  was asked for as `logs_request_cost_bytes_requested` and what governed as
  `logs_request_cost_bytes_effective`, and at the default policy those two
  differ, because the resolution saturates the rate.
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
  other **four (q29, q33, q34, q35) still exhausted the pool** at that time
  for a separate reason (see the ADR-0094 2026-08-26 amendment and its "still
  excluded" note); since the per-query pool derives at the tenant's share
  (ADR-2414 B1) all four run in the stock configuration on the reference box.
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
sql-latency,profiling` **and with frame pointers**, then set
`RAVEL_BENCH_PROFILE_SVG` to the output path. For a profiled pass, run it with
**`--runs 1`**, not `--runs 3`: with `RAVEL_BENCH_PROFILE_SVG` set, any `--runs`
above 1 is REFUSED with an error rather than run (issue #616), so this is a rule
the binary enforces, not advice.

```sh
RUSTFLAGS="-C force-frame-pointers=yes" \
RAVEL_BENCH_PROFILE_SVG=/tmp/sql_latency.svg \
cargo run --release -p ravel-bench --features sql-latency,profiling --bin sql_latency_bench -- \
  --tenant clickbench --store s3 \
  --corpus benchmarks/clickbench/hits.corpus.json \
  --runs 1 --compaction pre --window-hours 200000
```

The `RUSTFLAGS` is not optional decoration. A release build omits frame
pointers, and without them the unwinder cannot walk out of inlined generic code:
Ravel's own hot path lands in `[unknown]`. One such profile put 33.95% of
samples there and sent a merge after the wrong call site; the same workload
rebuilt with the flag measured 0.00% and named the real hot frame (issue #884).
From `crates/ravel-bench/` the cargo alias `cargo profile-build` sets the flag
for you; from the workspace root, as above, pass it yourself.

Two checks enforce this rather than trusting the operator to remember it, and
both fail the run with a non-zero exit:

- **Before sampling starts**, the binary counts x86-64 frame-pointer prologues
  (`push %rbp; mov %rsp, %rbp`) in its own executable segments and refuses below
  a measured floor (4,800; a build with the flag measures around 10,000, one
  without it around 1,500). It reads the produced binary, not the build
  configuration, because a flag that silently did nothing looks identical to one
  that worked. On an architecture or executable format where that scan cannot
  run it says so on stderr and continues, rather than skipping quietly.
- **When the profile is summarised**, the share of samples that resolved to no
  symbol (`[unknown]`) is printed and, above 2%, refused: no SVG is written and
  the message names the measured share, the threshold, and the likely cause. A
  healthy frame-pointer profile of this corpus measures 0.00%.

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
runs, `cache_bytes`, `deadline_secs`, `fetch_concurrency`, `logs_fetch_policy`
and `logs_block_range_threshold_effective` (the resolved read shape: two runs at
different policies are not comparable, and the effective threshold is the one
the resolution produced, not the one the flag asked for),
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
- Same `fetch_concurrency`, `cache_bytes`, `deadline_secs`, and per-query pool
  ceiling. All four are in the provenance; compare on
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
  --shards 4
```

No performance flags. Since #1141 the server derives all six of them from the
host at startup. Two are host-independent and match a published entry exactly:
a 1,000,000 sealed-segment cap and an 11-minute engine deadline. Fetch
concurrency is CPU-derived and also matches on this box: 32, two per core on
16 cores.

### Deriving the reference sizes

The memory-derived settings are computed from this box's own memory profile:
`MemTotal` (capped by the cgroup limit when the server runs in a container,
which the reference box does not), which Linux reports as 32,903,794,688
bytes here (MemTotal 32132612 kB), and -- since ADR-1170, amended 2026-10-03
by issue #2367 -- `MemAvailable`, which this box reports as 29,922,488,320
bytes, with this process's own resident set at effectively `0` just after
startup. With no cgroup limit and a readable `MemAvailable`, the budget is
`min(MemTotal - RESERVE, max(FLOOR, MemAvailable + own RSS - RESERVE))`:
`27,775,004,672` here, resolved with `source="derived-available"`, against
32,903,794,688 - 2,147,483,648 = 30,756,311,040 if the derivation still used
raw `MemTotal` as it did before the amendment. The two caches carve this
budget, not `MemTotal`: a 25% read cache of 6,943,751,168 bytes and a 5%
catalog byte cache of 1,388,750,233 bytes against an `s3` store. Against a
loopback store the read cache instead carves a 40% share, 11,110,001,868
bytes.

The 50% per-query and per-tenant SQL pools still carve raw `MemTotal` first
(16,451,897,344 each, unchanged by the amendment), but a DERIVED (not
explicit-flag) pool is then held at or below 90% of
`memory_remainder_bytes` -- what the budget above leaves once the two
caches are carved out of it. Against the `s3` store, the remainder is
19,442,503,271 and its 90% cap (17,498,252,943) does not bind, so both SQL
pools stay at 16,451,897,344 with `remainder_capped=false`. Against the
loopback store, the larger 40% read-cache carve leaves a remainder of
15,276,252,571, whose 90% cap (13,748,627,313) does bind: both SQL pools
resolve to that same 13,748,627,313 (the exact `bytes_as_usize`-truncated
90% figure) with `remainder_capped=true`. This box's exact numbers depend on
`MemAvailable` at the moment the server starts, not only on its fixed
`MemTotal`: a co-resident process on the same host at that moment would
derive a smaller budget and smaller caches, so two stock passes on the same
host can resolve different ceilings (ADR-1170's 2026-10-03 available-memory
amendment). Do not assume the resolved values: record the server's own
startup log lines (below) with the entry, alongside the `MemAvailable`
reading those lines were derived from. When comparing two passes against
each other (an A/B, a regression check), pin `--memory-budget-bytes` to one
value on both runs, so the budget is not the variable that differs between
them. Both of the two settings that used to be mandatory here are among the
derived six: a folded ClickBench tenant sits far above the old 1024
sealed-segment ceiling, so an un-derived server failed every statement with
`8424 exceeds max 1024`, and the cache must exceed the 9,844,635,064-byte
corpus or every run is cold and there is no hot column to compare.

Read the resolved values off the server's own startup log rather than
assuming them, and record them with the entry (the lines below are this
real box, `s3` store, at the `MemAvailable` reading above; a loopback store
or a different `MemAvailable` reading changes the memory-derived lines as
described above; the server prefixes each line with a timestamp, left out
here):

```
INFO ravel_server::config: performance default resolved setting="fetch_concurrency" value=32 source="derived"
INFO ravel_server::config: performance default resolved setting="cache_max_bytes" value=6943751168 source="budget-carve"
INFO ravel_server::config: performance default resolved setting="catalog_cache_max_bytes" value=1388750233 source="budget-carve"
INFO ravel_server::config: performance default resolved setting="memory_budget_bytes" value=27775004672 source="derived-available"
INFO ravel_server::config: performance default resolved setting="sql_max_query_bytes" value=16451897344 source="derived" clamped=false remainder_capped=false
INFO ravel_server::config: performance default resolved setting="sql_tenant_max_bytes" value=16451897344 source="derived" raised=false remainder_capped=false
```

Pass a flag only to measure a setting other than the derived one; a flag logs
`source="flag"`, which is what an entry's record must show if it was not run at
the defaults. The bench's `--cache-bytes` and `--sql-max-segments` configure the
in-process fetcher and engine, which the Flight lane never builds, so neither
reaches the server; leave them off the bench command, or the report header
claims a client-side cache that took part in nothing.

The bench-side flags must mirror what the server resolved, or the two tables are
not comparable:

- `--fetch-concurrency` is the same ADR-0088 knob as the bench's flag of the
  same name (logs scan partitions and in-flight segment fetches per query). This
  is the one that moves a cold full scan the most; set the bench's to the value
  the server logged, and record it.
- `--sql-max-query-bytes` is the per-query DataFusion memory-pool ceiling. A
  statement that fits the bench's budget and not the server's aborts on the
  server with `query memory budget exhausted` and lands in `failed`.
- `--sql-tenant-max-bytes`: the bench exposes a flag of that name, but it
  configures only the in-process executor; on the Flight lane the client sends
  the SQL statement alone, so the flag never reaches the server, and the
  server's own `--sql-tenant-max-bytes` (or its derived value) governs. It is
  the server's per-tenant ceiling across concurrent queries, which a lane
  running one statement at a time never reaches. The derived value is twice
  the derived per-query pool, so it is not the binding limit for a serial run;
  an explicit value below the per-query pool clamps the per-query pool down to
  it, and the server warns when it does.
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
  --fetch-concurrency 32 --sql-max-query-bytes 16106127360
```

The two bench-side values are the reference host's resolved defaults, so the
report's provenance matches the server's startup log; substitute the values
your server logged. On the Flight lane they are recorded, not enforced: the
server's own resolution governs every statement.

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

## ClickBench Parquet lane

Everything above loads `hits` into Ravel's own log format. This lane queries
the upstream Parquet files in place instead (ADR-2040 D7, issue #2055): the
upstream `create.sql` and the 43 statements of `queries.sql`, unrewritten,
through `ravel-server`, every answer compared with datafusion-cli 54.1.0's.
It lives under `benchmarks/clickbench/parquet/`:

| File | What it is |
|---|---|
| `create.sql`, `queries.sql` | upstream text, never edited |
| `suite.toml` | Ravel's `CREATE EXTERNAL TABLE` template and the per-statement comparator overrides |
| `prereg.toml` | the pre-registered bar every report is judged against |
| `make-reference.sh` | writes datafusion-cli's answers to `ref/` |
| `ref/` | those answers, checked in once generated |

The table has two arms. Arm A mounts a prefix holding the 100
`hits_N.parquet` files; arm B mounts the single-file `hits.parquet`. The
bench drops and creates `hits` itself (`DROP TABLE IF EXISTS hits`, then the
`suite.toml` template), and refuses to go on unless the server reports 100
mounted files for arm A and 1 for arm B.

### What CI covers and what it does not

CI runs `cargo test -p ravel-bench --features sql-latency`, which includes
`parquet_lane_runs_the_upstream_suite_verbatim` in
`crates/ravel-bench/tests/clickbench_corpus.rs`: the 43 statements over a
synthetic fixture of about 22,000 rows in four parts, both arms,
in process, compared with an in-process DataFusion reference. The same run
covers the report's D7 check against hand-built reports, the concurrency
phase against scripted clocks, and the binary's own refusals.
`make-reference.test.sh` runs `make-reference.sh` against a stub
datafusion-cli.

Only the reference machine produces anything D7 judges: the real
datafusion-cli reference, the real 100-file and single-file layouts, the
timings, the stamped server settings and the concurrency figures. No CI job
runs `clickbench_parquet_bench` against a server.

### 1. The box

Stand the box up with
[clickbench-aws-runbook.md](clickbench-aws-runbook.md) sections 1 to 6. D7
measures against loopback RustFS on that box, not against S3: run RustFS
1.0.0 there, serving `http://127.0.0.1:9000`, holding Ravel's own bucket and
a second bucket for the Parquet files, here `clickbench-parquet`. Ravel never
reads a Parquet table from its own bucket (ADR-2040 D4), so the two must
differ. Both startup preconditions in that runbook's section 7 apply to the
server here too.

Put the files in the second bucket, with RustFS's keys in
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`:

```sh
mkdir -p /root/hits && cd /root/hits
for i in $(seq 0 99); do
  wget -q "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_$i.parquet"
done
aws s3 cp --endpoint-url http://127.0.0.1:9000 --recursive /root/hits/ \
  s3://clickbench-parquet/hits/
aws s3 cp --endpoint-url http://127.0.0.1:9000 /root/hits.parquet \
  s3://clickbench-parquet/hits.parquet
```

### 2. The reference and the DESCRIBE check

Install datafusion-cli 54.1.0 and generate the reference from the local
single-file copy, which is arm B's definition of `hits`:

```sh
cargo install --locked datafusion-cli --version 54.1.0
benchmarks/clickbench/parquet/make-reference.sh /root/hits.parquet
```

It refuses any other datafusion-cli version before writing anything. It
writes `ref/q01.json` to `ref/q43.json`, `ref/describe.json` (the output of
`DESCRIBE hits` over the same `create.sql`), and `ref/VERSION` last, only once
every statement succeeded; it exits 1 naming each statement that failed. The
bench refuses a reference directory whose `VERSION` is not
`datafusion-cli 54.1.0` or that lacks any `qNN.json`.

Before trusting the reference, read `ref/describe.json`. `EventDate` must be
a `Date32`, which is what both upstream's view and Ravel's
`ravel.cast.EventDate` `date-from-days` option produce, and the text columns
(`URL`, `Title`, `Referer`, `SearchPhrase`) must be string types, not
`Binary`, which is what `binary_as_string` produces on both sides. A
reference that differs here compares Ravel with a different table. Commit
`ref/` once it passes.

### 3. Fill prereg.toml before any Ravel run

`prereg.toml` ships with `memory_cap_bytes`, `arm_b_hot_s` and `arm_b_cold_s`
set to a placeholder string, and the bench refuses the file, naming each key,
until all three are numbers. Fill them before the first suite run and commit
the filled file with the reports:

- `memory_cap_bytes` is the per-query memory cap the pre-registered failures
  are stated against. Start the server once (step 5) and copy the
  `sql_max_query_bytes` value its startup log names; every report's server
  log must show exactly this value.
- `arm_b_hot_s` and `arm_b_cold_s` are your predictions of arm B's hot and
  cold sums. Every report, arm B's own included, fails if its sum exceeds
  1.25x the prediction.

`failures`, `rlog_hot_ceiling_s` and `concurrency_qps_floor` already hold
D7's figures; change them only with a reason recorded on #2055. There is no
error-ratio key: `concurrency_error_ratio_ceiling` was removed (issue
#2055, step 7), and the bench refuses a `prereg.toml` that still carries it.

### 4. Build at one SHA

Build every binary from the commit the runbook's bootstrap pinned in
`/root/CLONE_SHA`, and run the bench from that checkout: the report's
`provenance.git_sha` is the `HEAD` of the directory the bench runs in.

```sh
cd /root/ravel
git checkout -q --detach "$(cat /root/CLONE_SHA)"
cargo build --release -p ravel-server --features sql
cargo build --release -p ravel-cli
cargo build --release -p ravel-bench --features sql-latency --bin clickbench_parquet_bench
```

### 5. Grant the locations and start the server

The tenant here is `clickbench`. Write a credential profile file,
`profiles.json`, with one S3 profile named `rustfs` reaching
`http://127.0.0.1:9000` (the
shape is in [ravel-server-flags.md](../reference/ravel-server-flags.md),
`--parquet-profiles`, and ADR-2040 D1; it names where the secret is, never
the secret), then grant both arms' locations:

```sh
for loc in s3://clickbench-parquet/hits/ s3://clickbench-parquet/hits.parquet; do
  target/release/ravel-cli --store <your-store-flags> --parquet-profiles profiles.json \
    tenant parquet-grant add --tenant clickbench --location "$loc" --profile rustfs
done
```

Generate the bearer token into an environment variable and start the server
with a fresh log file. The `;ddl` suffix gives the token the right to create
and drop tables:

```sh
export TOKEN="$(openssl rand -hex 16)"
target/release/ravel-server --store s3 <the --s3-* flags for Ravel's bucket> \
  --parquet-profiles profiles.json \
  --tenant-token "$TOKEN=clickbench;ddl" > server.log 2>&1 &
```

Pass no performance flags except `--memory-budget-bytes`, set to the same
value for both arms. Arms A and B are compared against each other, and since
ADR-1170's available-memory amendment a derived budget follows the
`MemAvailable` reading at each start, so an unpinned budget could differ
between the arms. The bench reads five startup lines from `server.log`, each
like

```
INFO ravel_server::config: performance default resolved setting="cache_max_bytes" value=8053063680 source="budget-carve"
```

for `cache_max_bytes`, `catalog_cache_max_bytes`, `fetch_concurrency`,
`sql_max_query_bytes` and `sql_tenant_max_bytes`, and stamps the report with
them. Each must appear exactly once, so give every server start its own log
file rather than appending.

### 6. Arms A and B

For each arm, restart the server with a new log and drop the page cache
(`sync; echo 3 > /proc/sys/vm/drop_caches`) first, then:

```sh
target/release/clickbench_parquet_bench \
  --server http://127.0.0.1:4318 --token-env TOKEN \
  --arm b --location s3://clickbench-parquet/hits.parquet \
  --reference benchmarks/clickbench/parquet/ref \
  --prereg benchmarks/clickbench/parquet/prereg.toml \
  --server-log server.log --out arm-b.json \
  --sql-max-query-bytes <from server.log> --sql-tenant-max-bytes <from server.log>
```

and the same with `--arm a --location s3://clickbench-parquet/hits/ --out
arm-a.json`. `--token-env` takes the variable's name; there is no flag that
takes the token itself.

Each statement runs three times. Try 1 is the cold figure and the faster of
tries 2 and 3 the hot one; the sums leave out failed statements. The binary
runs the statements back to back and drops no cache, so only q01's first try
is cold in ClickBench's sense; every later statement's first try starts with
what the statements before it left cached. Try 1's answer is compared with
the reference.

The bench exits 0 when the report meets the bar, 1 when it does not, after
printing each violation, and 2 on a setup error (an unfilled prereg, a bad
token variable, a location that does not fit the arm, a refused reference,
an unreadable log, a log missing a stamp or carrying one twice, a failed
`CREATE`, concurrency arguments the phase refuses, an unwritable `--out`).
Every one of these but the `CREATE` is checked before anything reaches the
server, so a refused argument runs nothing. That check creates `--out`'s
directory if it is missing and opens `--out` without truncating it, so a
report from an earlier run survives until a new one replaces it.
Past setup it writes the report before judging it: a concurrency phase that
fails once started, or that the engine becoming unreachable ends early (step
7), is recorded in the report's `concurrency_error`, the report is written
with every statement's figures, and the bench exits 1. A violation is any
of:

- a statement missing from the report, repeated, or not in the suite;
- a stamp differing from `--sql-max-query-bytes` or `--sql-tenant-max-bytes`
  (a stamp missing from the log or present more than once is refused at
  setup, so a run never reaches the report check with one);
- `sql_max_query_bytes` differing from `memory_cap_bytes`;
- a statement that failed and is not in `failures`;
- an answer that could not be compared with the reference, or whose verdict
  is neither a pass nor the verdict `suite.toml` declares for it, or
  `suite.toml`'s declared verdicts not loading;
- hot or cold sum above 1.25x the arm B prediction, or a hot sum not under
  `rlog_hot_ceiling_s`;
- in the concurrency phase, queries per second under
  `concurrency_qps_floor`, or any statement error from a statement not in
  `failures`;
- a concurrency phase that failed after it started, including one the engine
  becoming unreachable ended (`concurrency_error`).

A pre-registered failure that answered is printed as a finding, not a
violation. The 1.25x bar against the *measured* arm B is not mechanical:
compare `totals.hot_sum_s` and `totals.cold_sum_s` in `arm-a.json` with
`arm-b.json` by hand, and explain any arm A figure above arm B's on #2055.

### 7. The concurrency phase

Add `--concurrency-seconds 600` to an arm's command for D7's phase: ten
connections (`--concurrency-tasks`, default 10), each its own HTTP client,
cycling the 43 statements, task `i` starting `4i` statements into the suite.
The report's
`concurrency` block holds the configured `duration_s` and the measured
`elapsed_s`, queries per second, the error ratio, and per-statement counts,
nearest-rank p50 and p95, and the first error. No task starts a statement
after `duration_s`, but one already running finishes and counts, so
`elapsed_s` runs from the phase's start to the last statement returning and
is at least `duration_s` unless the phase ended early (below); `qps` is
completed queries over `elapsed_s`.
`error_ratio` is every error over completed plus errors.

An error the server answers with, any HTTP status including 422 and 503,
is a statement error: it counts against that statement and the phase goes
on. A connection-level failure, where the request could not be sent or the
server refused or dropped the connection (the error text starts with
`engine unreachable:`), ends the phase early. A single one is enough,
including a reused keep-alive connection the server drops while a request
is on it: `HttpEngine` makes no retry of its own, so a phase that ended
early costs a rerun. No task starts a statement
after the first one; a statement already in flight finishes, answered or
failed the same way, so at most one connection-level failure per task is
counted. The bench prints `concurrency: the engine became unreachable <N> s
into the phase; task <T> saw it first, on q<NN>: <error>` once, and the
report keeps the `concurrency` block with the figures up to that point:
`elapsed_s` then ends at the last return after the failure and can be below
`duration_s`, `engine_unreachable` holds the time, task, statement and
error, and each statement's `unreachable` counts its connection-level
failures, which its `errors` includes. The same line is the report's
`concurrency_error`, which the D7 check reports as exactly one violation,
`ConcurrencyPhaseFailed`. The check skips the `qps` rule for a phase the
engine ended, since its `qps` measures the outage. A connection-level
failure is not a statement error, so it adds no per-statement violation and
leaves `errored_statements` and `first_error` alone.

D7's concurrency bar is two rules (issue #2055): `qps` at least
`concurrency_qps_floor` (0.400), and no statement error from any statement
outside `failures`. The error ratio is not judged: the five registered failures
alone put it at 5/43, about 0.116, above the RLOG entry's 0.101 on every
run. ADR-2040's concurrency bar amendment gives the full reason. The bench
prints `error_ratio` beside the RLOG entry's 0.101; explain any gap beyond
what the registered failures account for on #2055.
Without the flag the phase does not run and none of D7's concurrency bar is
checked.

### 8. Post the reports

Post each arm's report on #2055 with the filled `prereg.toml`, the server
log, the `CLONE_SHA` and the bench's exit code. A report is stamped when its
`provenance` carries the five server settings, the reference's `VERSION`,
and the mounted file count; one that exited 2 has no report to post.
