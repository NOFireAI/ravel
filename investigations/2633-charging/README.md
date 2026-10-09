# Issue #2633: what the SQL memory pool does not charge

Measurement only. The question: for single ClickBench statements on
`ravel-server`, how far does jemalloc's `allocated` rise above the SQL
reservation DataFusion reports to Ravel's pool, and which operators allocate
the difference.

**The instrumentation patch (commit c1f00f49, `crates/ravel-sql/src/memory.rs`)
lives on this task branch only and must never be merged.** It logs one
`pool consumer peaks` line per query and was written for this measurement,
not for production use.

## Short answer

There are two distinct shortfalls. The band's figure (allocation peak minus
SQL reservation peak) sees only the first.

1. **Mid-statement, while the aggregation is still growing: 0.12 to 0.33 GB
   per statement** (median, 3 runs). At the q35 profile moment, the
   difference between allocated bytes and the SQL reservation was
   0.41 GB. It splits as follows:
   - 0.07 GB was Ravel's own fetch reservation, which is charged, just to a
     different budget component.
   - 0.11 GB was the RepartitionExec output coalescer's in-progress buffers.
   - About 0.06 GB was `ArrowBytesViewMap::size()` undercounting its views
     vector and hash table.
   - About 0.17 GB, by subtraction, was partial-aggregate state that had
     already been emitted and was still being drained.
2. **At the final aggregate's emit, which the peak-minus-peak figure hides:
   up to 1.2 GB.** `GroupedHashAggregateStream::emit` moves the whole group
   state into an output batch and immediately shrinks the reservation to the
   emptied state. The batch stays alive while it is sliced out downstream:
   - q33: the SQL reservation fell from 2.22 GB to 0.47 GB while allocation
     stayed at 1.68 GB, leaving 1.20 GB uncharged for 60 to 80 ms (two runs,
     both 1.205 GB).
   - q19: 0.78 GB uncharged.

   The peak figure misses this because allocation also drops at that moment,
   just by less. For a single statement this is harmless. Under concurrency,
   it is memory the pool has already handed back to other statements while
   it is still live.

The fixes for all three operator findings are in upstream DataFusion. No
Ravel frame appears in the uncharged allocations. Ravel's own AVG
accumulators contribute only through the emit path (finding 1).

## Bands

| Row | Figure | Expectation | Miss | Measured | Verdict |
|---|---|---|---|---|---|
| 1 | Allocated delta minus SQL reservation peak | >=0.5 GB for q33, q34, q35, q18, q19 | <0.2 GB on all of them | q33 0.169, q34 0.314, q35 0.327, q18 0.196, q19 0.246 GB | **Neither.** No statement reaches 0.5 GB. Three of the five are above 0.2 GB, so it is not a miss either. The emit-phase figure (shortfall 2), which is not this row's figure, is 1.205 GB for q33 and 0.765 GB for q19. |
| 2 | Same, controls q1 and q2 | <0.1 GB | >=0.3 GB | q1 0.001, q2 0.009 GB | **Met** |
| 3 | Per-consumer peaks vs profile attribution | name the operators that fall short | n/a | see "Operators to charge" | Named: GroupedHashAggregateStream emit path, RepartitionExec output coalescer, ArrowBytesViewMap size accounting |

## Setup

| Item | Value |
|---|---|
| Host | ip-172-31-18-6, Linux 7.0.0-1011-aws x86_64, `nproc` 16, MemTotal 32,132,612 kB, cgroup cap near 29 GB |
| Base commit | 0dab65b6; both server binaries built from c1f00f49 (base plus instrumentation) |
| Toolchain | rustc 1.97.1 (8bab26f4f 2026-07-14) |
| DataFusion | 54.1.0 (Cargo.lock); arrow-select 58.4.0 on the DataFusion path |
| Object store | RustFS 1.0.0 on 127.0.0.1:39000, local data directory; binary sha256 222eedc3d9baabf6516702d9fbf230270c3ca49b50f562d3461c96e2cc6ae6ad (commit unknown) |
| Data | ClickBench `hits_0..19.parquet` from datasets.clickhouse.com, 20 files, 2,711,471,866 B, 20,000,000 rows, in bucket `clickbench-parquet` under `hits/` |
| Budget | `--memory-budget-bytes 8000000000`. This resolved to: SQL per-query and per-tenant limits of 3,960,000,000 B, cache 3.2e9 B, catalog cache 4e8 B, and fetch concurrency 32. |

Binaries:

| Binary | Build | sha256 |
|---|---|---|
| ravel-server (sql) | `cargo build --release --locked -p ravel-server --features sql` | 73406616567aea80745e421893222af1e6cb6c42b6f83dfe83c84c6e4727e3d2 |
| ravel-server (heap) | `cargo build --release --locked -p ravel-server --features sql,heap-profiling` | c996fc7dbb6d1a51a6cd6004a794925b98e29dd0742b4dd5f9f57a633d485e52 |
| ravel-cli | `cargo build --release --locked -p ravel-cli --bin ravel-cli` | 29d35ec838efb7b5082925e563f8af9a043bdca8b779c6f9a386aea001dfe729 |

Server command line (credentials are loopback throwaways; shown as
placeholders):

```sh
export AWS_ACCESS_KEY_ID=<access-key> AWS_SECRET_ACCESS_KEY=<secret-key>
export RAVEL_AUDIT_TOKEN_KEY=<random 32 bytes hex>
ravel-server --store s3 --s3-endpoint http://127.0.0.1:39000 --s3-bucket ravel \
  --s3-region us-east-1 --s3-access-key <access-key> --s3-secret-key <secret-key> \
  --tenant-hash-unkeyed --parquet-profiles profiles.json \
  --tenant-token "<token>=clickbench;ddl" --memory-budget-bytes 8000000000
```

`profiles.json` holds one S3 profile, `rustfs`, with `allow_http`,
`force_path_style`, and static credentials taken from the two environment
variables. The table comes from the ClickBench suite's template:

```sql
CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION 's3://clickbench-parquet/hits/'
OPTIONS ('binary_as_string' 'true', 'ravel.cast.EventDate' 'date-from-days');
```

The heap-profiling server additionally ran with
`_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:19,lg_prof_interval:30`.

### Instrumentation

In `TenantDelegatingPool` (`crates/ravel-sql/src/memory.rs`, task branch only):

- `grow`, `try_grow` (on success) and `shrink` update a
  `Mutex<HashMap<String, (cur, peak)>>`.
- Each consumer is tracked under two keys:
  - `P <consumer name>`: one entry per partition.
  - `B <base name>`: partition indices folded to `[*]`, so the entry is the
    sum across partitions.
- The query-wide peak is tracked separately.
- The pool is per query. Its `Drop` logs the table once, so "dump and reset
  per statement" is that drop.
- Partial and final aggregates share the consumer name
  `GroupedHashAggregateStream[N] (aggs)`, so the `B` row for an aggregate is
  partial plus final.

### Method

`charge.py` runs one statement at a time against `/api/v1/sql`:

- A 20 ms thread scrapes `/metrics` for these figures:
  - jemalloc `allocated` and `resident`;
  - `ravel_memory_reserved_bytes` for components `sql` and `fetch`;
  - the sum of `ravel_cache_resident_bytes`.
- Each run waits 1 s and takes the median of the last 10 samples as its
  baseline.
- The window is from the POST to the response plus 100 ms.
- The run records these figures:
  - allocation peak delta: largest `allocated` in the window minus the
    baseline;
  - SQL reservation peak;
  - gap: the allocation peak delta minus the SQL reservation peak;
  - fetch reservation peak;
  - max simultaneous uncharged: the largest per-sample value of
    `allocated - baseline - sql - fetch`;
  - the pool dump.
- Each statement gets one warm-up (rep 0) and 3 measured runs.
- Statements run in this order: q13, q14, q17, q18, q19, q31, q32, q33, q34,
  q35, q1, q2.
- 48 runs, all HTTP 200, each with exactly one dump, 0 sampler errors.

## Per-statement results

GB, median (min-max) over the 3 measured runs (`runs.jsonl`, `analysis.txt`).

| q | alloc peak delta | SQL reservation peak | **gap** | fetch reservation peak | max simultaneous alloc - SQL - fetch | pool peak (dump) | secs |
|---|---|---|---|---|---|---|---|
| q01 | 0.001 (0.001-0.001) | 0.000 | **0.001 (0.001-0.001)** | 0.000 | 0.001 | 0.000 | 0.023 |
| q02 | 0.009 (0.008-0.011) | 0.000 | **0.009 (0.008-0.011)** | 0.000 | 0.009 | 0.000 | 0.028 |
| q13 | 0.365 (0.352-0.370) | 0.224 (0.222-0.226) | **0.139 (0.129-0.146)** | 0.013 | 0.139 (0.125-0.174) | 0.264 | 0.167 |
| q14 | 0.378 (0.377-0.389) | 0.260 (0.257-0.262) | **0.120 (0.116-0.129)** | 0.040 | 0.139 (0.131-0.151) | 0.286 | 0.280 |
| q17 | 0.651 (0.642-0.677) | 0.511 (0.493-0.520) | **0.157 (0.131-0.158)** | 0.028 | 0.385 (0.215-0.394) | 0.560 | 0.324 |
| q18 | 0.669 (0.658-0.743) | 0.476 (0.462-0.522) | **0.196 (0.193-0.221)** | 0.039 | 0.240 (0.206-0.265) | 0.560 | 0.268 |
| q19 | 1.264 (1.210-1.280) | 1.032 (1.001-1.034) | **0.246 (0.178-0.263)** | 0.118 | 0.765 (0.756-0.788) | 1.070 | 0.527 |
| q31 | 0.151 (0.151-0.160) | 0.111 (0.111-0.112) | **0.040 (0.039-0.050)** | 0.011 | 0.041 (0.037-0.041) | 0.122 | 0.208 |
| q32 | 0.227 (0.216-0.250) | 0.172 (0.158-0.180) | **0.058 (0.055-0.070)** | 0.044 | 0.070 (0.054-0.123) | 0.224 | 0.185 |
| q33 | 2.394 (2.383-2.400) | 2.227 (2.226-2.231) | **0.169 (0.152-0.173)** | 0.147 | 1.205 (1.166-1.205) | 2.245 | 0.634 |
| q34 | 1.130 (1.090-1.165) | 0.816 (0.814-0.823) | **0.314 (0.267-0.351)** | 0.181 | 0.414 (0.410-0.498) | 0.855 | 0.554 |
| q35 | 1.093 (1.084-1.187) | 0.815 (0.758-0.825) | **0.327 (0.278-0.362)** | 0.118 | 0.330 (0.321-0.384) | 0.852 | 0.548 |

Reading the table:

- The gap column is the band's figure. It includes Ravel's fetch
  reservation, which is charged against the memory budget, but to the
  `fetch` component rather than to `sql`.
- The max-simultaneous column subtracts fetch per sample. Where it far
  exceeds the gap (q17, q19, q33), the excess is the emit phase:
  - `series-check.txt` shows the time series for q33 and q19. It comes from
    2 extra measured runs each on the heap-profiling binary.
  - The SQL reservation collapses to about 20% of its peak while allocated
    bytes fall by only a third.
  - That state lasts until the response is sent.

## Per-consumer peaks, worst three statements

The worst three by median gap are q35, q34 and q19. Figures are in GB, the
sum across partitions (`B` rows), with the median in the last column. Each
run registered 65 per-partition consumers: 32 aggregate (partial plus
final), 32 RepartitionExec and 1 TopK.

q35, `SELECT 1, "URL", COUNT(*) AS c FROM hits GROUP BY 1, "URL" ORDER BY c DESC LIMIT 10;`

| consumer | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| `GroupedHashAggregateStream[*] (count(1))` | 0.814 | 0.819 | 0.833 | 0.819 |
| `RepartitionExec[*]` | 0.110 | 0.146 | 0.117 | 0.117 |
| `TopK[*]` | 0.032 | 0.030 | 0.029 | 0.030 |

q34, `SELECT "URL", COUNT(*) AS c FROM hits GROUP BY "URL" ORDER BY c DESC LIMIT 10;`

| consumer | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| `GroupedHashAggregateStream[*] (count(1))` | 0.830 | 0.859 | 0.813 | 0.830 |
| `RepartitionExec[*]` | 0.077 | 0.118 | 0.187 | 0.118 |
| `TopK[*]` | 0.030 | 0.031 | 0.031 | 0.031 |

q19, `SELECT "UserID", extract(minute FROM to_timestamp_seconds("EventTime")) AS m, "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", m, "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10;`

| consumer | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| `GroupedHashAggregateStream[*] (count(1))` | 0.994 | 1.049 | 1.026 | 1.026 |
| `RepartitionExec[*]` | 0.352 | 0.324 | 0.313 | 0.324 |
| `TopK[*]` | 0.023 | 0.024 | 0.024 | 0.024 |

For reference, q33 (largest emit-phase shortfall):

| consumer | median |
|---|---|
| `GroupedHashAggregateStream[*] (count(1), sum(hits.IsRefresh), avg(hits.ResolutionWidth))` | 2.222 |
| `RepartitionExec[*]` | 0.138 |
| `TopK[*]` | 0.035 |

In these four statements no consumer other than these three kinds
registered a reservation, so every uncharged byte belongs to one of these
operators or to an operator that holds no reservation. Across the whole
matrix the only other consumer is `AggregateStream`, from the ungrouped
controls.

## Heap profile attribution (q35)

- Run: the heap-profiling server, q35, two warm runs with gaps of 0.301 and
  0.280 GB (`profile-run.jsonl`). The measured run started at t0 =
  1791505821.362 with a pre-statement `allocated` of 0.401 GB.
- Peak sample at t0 + 0.309 s:
  - allocation delta 1.128 GB;
  - SQL reservation 0.716 GB;
  - fetch reservation 0.070 GB.
- Profile used: the interval dump nearest that sample.
  - i20 was written 8 ms after the peak sample. Its sampled total is
    1,525,547,926 B, against `allocated` of about 1.529 GB.
  - The base is i16, written 9 ms after t0, with a total of 478,784,688 B.
    About 78 MB of query-owned allocations predate i16 and are therefore
    absent from the diff.
- The diff i20 minus i16 is 1,046,763,219 B. It is split per operator by
  `mode.py`, which classifies each stack by the frame that drives the
  aggregate's task:
  - `RepartitionExec::pull_from_input`: partial aggregate.
  - `RecordBatchReceiverStreamBuilder::run_input`: final aggregate.

| operator | site | bytes | share |
|---|---|---|---|
| partial aggregate | `ArrowBytesViewMap::append_value` (string blocks and views) | 333,071,013 | 31.8 % |
| final aggregate | `ArrowBytesViewMap::append_value` | 311,989,548 | 29.8 % |
| final aggregate | hashbrown table of `ArrowBytesViewMap` (`insert_accounted`) | 141,796,266 | 13.5 % |
| partial aggregate | hashbrown table of `ArrowBytesViewMap` | 99,584,699 | 9.5 % |
| RepartitionExec output coalescer | arrow-select `ByteViewArray` coalescer `next_buffer` | 95,991,704 | 9.2 % |
| partial aggregate | `CountGroupsAccumulator` | 30,933,185 | 3.0 % |
| final aggregate | `CountGroupsAccumulator` | 21,620,792 | 2.1 % |
| RepartitionExec output coalescer | `ensure_capacity` (primitive and view builders) | 10,320,500 | 1.0 % |
| final aggregate | `GroupValuesBytesView` | 5,019,997 | 0.5 % |

`jeprof --text --lines --cum` over the same diff (`jp-diff-lines-cum.txt`)
places the string bytes at these lines of
`datafusion-physical-expr-common-54.1.0/src/binary_view_map.rs`:

| line | allocation | bytes |
|---|---|---|
| :439 | new 2 MiB blocks | 408 MB |
| :446 | first in-progress block growing from `Vec::new()` | 148 MB |
| :451 | views vector `grow_one` | 89 MB |
| :371 (`insert_accounted`) | hash table | 244 MB |

The coalescer bytes come from `datafusion-physical-plan-54.1.0`
`src/repartition/mod.rs:170`, via `coalesce/mod.rs:118`, to
`arrow-select-58.4.0` `src/coalesce/byte_view.rs:384`. No frame from a Ravel
crate appears anywhere in the diff.

Comparison with the pool at that moment:

| figure | GB | source |
|---|---|---|
| Query-owned heap | 1.125 | 1.047 in the diff plus 0.078 before i16; matches the 1.128 sample |
| SQL reservation | 0.716 | sample |
| Fetch reservation | 0.070 | sample |
| Uncharged | 0.34 | remainder |
| Aggregation heap (partial plus final) | 0.945 | profile |
| Aggregate consumer peak, whole run | 0.82 to 0.86 | dumps |
| Coalescer heap | 0.106 | profile |
| RepartitionExec consumer peak | 0.117 | dumps |

The coalescer bytes and the RepartitionExec reservation are different memory.
The reservation charges completed batches waiting in the channels. The
coalescer's in-progress buffers come before that charge (finding 2).

## Operators to charge

Ranked by the memory they leave uncharged. Line numbers refer to DataFusion
54.1.0 and arrow-select 58.4.0 as resolved by Cargo.lock, and to this tree
for Ravel files.

### 1. `GroupedHashAggregateStream` emit: output batch held uncharged

**Evidence** (`datafusion-physical-plan-54.1.0/src/aggregates/row_hash.rs`):

1. `emit` (:1103) moves the group state out with `group_values.emit` (:1114).
   For string keys, `GroupValuesBytesView::emit`
   (`group_values/single_group_by/bytes_view.rs:89-91`) takes the whole
   `ArrowBytesViewMap` and converts it zero-copy with `into_state`
   (`binary_view_map.rs:384`).
2. `emit` builds the aggregate output arrays with `evaluate` or `state`
   (:1120-1127). These are new allocations.
3. Then `update_memory_reservation` (:1133) resizes the reservation to the
   now-empty group state.
4. `set_input_done_and_produce_output` (:1329-1347) parks the batch in
   `ExecutionState::ProducingOutput`. `poll_next` hands it out in
   `batch_size` slices (:854-880). The slices share the batch's buffers, so
   all of it stays live, uncharged, until the last slice is dropped
   downstream.

Ravel's own AVG accumulators build their output during this step:

- `ExactIntegerAvgGroupsAccumulator::evaluate`, `crates/ravel-sql/src/avg.rs:783`;
- `SequentialAvgGroupsAccumulator::evaluate`, `crates/ravel-sql/src/avg.rs:524`.

Their `size()` (:856, :546) is correct before emit. The output array is
uncharged for the same reason as everything else in the batch.

**Measured:**

- Final aggregate:
  - q33: 1.205 GB uncharged in every measured run (matrix 1.166-1.205 GB),
    lasting 60 to 80 ms (`series-check.txt`);
  - q19: 0.765 GB (0.756-0.788);
  - q17: 0.385 GB.
- Partial aggregate: the same mechanism runs while the final aggregate is
  still accumulating. At the q35 profile moment it accounts for about
  0.17 GB.
  - This is by subtraction:
    0.34 uncharged − 0.106 coalescer − about 0.06 size undercount.
  - The profile cannot separate emitted bytes from bytes still in a map,
    because both were allocated at `append_value`.

**Fix: upstream DataFusion.** While the stream is in `ProducingOutput`, it
should keep its reservation at the emitted batch's size and release it as
slices drain. Ravel cannot do this itself: the pool sees only `shrink`, not
that the bytes are still live. No DataFusion config knob covers it.

**Share:**

- Emit phase: essentially all of the 0.4-1.2 GB emit-phase shortfall (q17,
  q19, q33).
- Mid-statement gap: about 40% for q35 at the profile moment.

### 2. `RepartitionExec` output coalescer: in-progress buffers uncharged

**Evidence** (`datafusion-physical-plan-54.1.0/src/repartition/mod.rs`):

1. `OutputChannel::coalesce` (:168-172) pushes every hash-partitioned batch
   into a `SharedCoalescer` (`push_and_drain`, :244-252).
2. The coalescer copies string views into new buffers:
   - `arrow-select-58.4.0/src/coalesce/byte_view.rs:384` (`next_buffer`);
   - `:102` (`ensure_capacity`);
   - `coalesce/primitive.rs:59`.
3. Only a completed batch is charged, in `send`:
   `get_array_memory_size` at :181, then `try_grow` at :186.
4. The receiver releases that charge when it takes the batch (:1873).
5. Nothing charges the coalescer's in-progress buffers.

**Measured:** 106 MB at the q35 profile moment, which is 26% of
allocated-minus-SQL (0.41 GB) there.

**Fix:**

- Upstream DataFusion: charge `LimitedBatchCoalescer`'s buffered bytes to
  the output channel's reservation.
- Config knob: `datafusion.execution.batch_size` and the target partition
  count bound the per-partition in-progress size and multiply it. Lowering
  either trades throughput for headroom and does not make the bytes visible
  to the pool.

**Share:** about 25-30% of the mid-statement gap for q34/q35, less for
statements with narrow keys.

### 3. `ArrowBytesViewMap::size()` undercounts its own allocations

**Evidence** (`datafusion-physical-expr-common-54.1.0/src/binary_view_map.rs`):

- `size()` (:473-486) counts the views as `views.len() * 16` (:474).
  - The vector grows by doubling (`views.push` at :451), so its capacity is
    between 1x and 2x its length.
- The hash table is counted through `map_size`.
  - `HashTableAllocExt::insert_accounted`
    (`datafusion-common-54.1.0/src/utils/proxy.rs:158-184`) adds
    `capacity * size_of::<Entry>()` on each growth.
  - `Entry` is a u128 view, a u64 hash and a payload: 32 B.
  - hashbrown actually allocates `buckets * (32 + 1)`, with buckets of
    capacity × 8/7. That is about 18% more, before jemalloc size-class
    rounding.
- The reservation is updated only after each batch has been interned
  (`row_hash.rs:1067-1090`). A table doubling is therefore allocated before
  it is charged, while the old table is still live during the rehash.

**Measured:** estimated 0.06 GB at the q35 profile moment:

- views: 89 MB allocated, so 0-45 MB uncounted, about 22 MB expected;
- hash tables: 242 MB allocated, about 36 MB uncounted.

**Fix: upstream DataFusion.** Count `views.capacity()`, and compute the hash
table's size from `allocation_size()` instead of from bumps.

**Share:** about 15% of the mid-statement gap for string-keyed aggregations
(q13, q14, q34, q35).

### Not an operator: Ravel's fetch reservation

The band's gap figure includes Ravel's fetch reservation:

- peaks of 0.04-0.18 GB;
- 0.070 GB, or 17%, of allocated-minus-SQL at the q35 profile moment.

That memory is charged, to the `fetch` component of the same budget. Nothing
needs fixing, but the Row 1 figure overstates the SQL pool's shortfall by
this much. The max-simultaneous column removes it.

### Concurrency

These shortfalls add up across statements that run at the same time:

- Shortfall 1 is 0.12-0.33 GB per statement, while aggregation is still
  growing.
- Shortfall 2 is up to 1.2 GB per statement, at emit. That memory has
  already been returned to the pool, so another statement can be granted
  it.

Concurrent overshoot is therefore expected to scale with statement count,
and it is largest when several high-cardinality aggregations emit together.
This run measured single statements only and did not re-measure
concurrency.

## Deviations

- The instrumentation patch was first applied with a Python script. That
  breaks the edit-tools-only rule. It was reverted with `git checkout` and
  redone with the Edit tool; the committed patch is the second version.
- The first matrix parse used `str.splitlines()`, which also splits on the
  `\x1e` row separator, so each stored dump kept only its top row.
  - `redump.py` re-extracted every dump from the server log.
  - The `query_peak` of every re-extracted dump matches the original: 48/48
    matrix runs and 4/4 profile runs, with 0 mismatches.
  - `charge.py` now splits on `\n` only.
- The 20 ms sampling cadence was not held during heavy statements:
  - `/metrics` scrapes took about 2 ms when idle but stalled up to 260 ms
    under load.
  - A sample's timestamp is the request start, so a stalled sample reports
    a later moment.
  - Peaks can be underestimated, and each statement has fewer samples than
    its duration implies (as few as 15 for a 0.47 s statement).
- "Dump and reset per statement" is implemented as one dump when the
  per-query pool drops. The pool is created per query, so no explicit reset
  is needed.
- Partial and final aggregates share a consumer name, so the aggregate rows
  in the per-consumer tables are partial plus final.
- Seven smoke statements ran on the sql server before the matrix:
  COUNT(*) once, and q1, q2 and q33 twice each. The server was not restarted between them,
  and the OS page cache was not dropped. The warm-up run absorbed cache
  fill: `cache_delta` was 0 on every measured run.
- The first profile invocation lacked per-sample series. q35 was rerun with
  the current `charge.py` on the same heap-profiling server, and the dumps
  used are from that second invocation. Its warm-up run was itself warm.
- The emit-phase time series for q33 and q19 (`series-check.*`) is an
  addition not in the task spec. It is 1 warm-up and 2 measured runs each,
  on the heap-profiling binary, because that server was the one running.
  Those runs' gaps (q33 0.155/0.151, q19 0.228/0.261 GB) agree with the
  matrix.
- The profile diff's base (i16) is 9 ms after t0, so about 78 MB of
  query-owned memory predates it. The comparison table adds it back.
- The 0.17 GB attributed to emitted partial-aggregate state is inferred by
  subtraction, not measured directly.
- The RustFS build commit is unknown; only its binary sha256 is recorded.
- The commit trailer `Co-Authored-By` was omitted, per the repository's
  no-AI-footer rule.

## Files

| File | What |
|---|---|
| `charge.py` | runner and 20 ms sampler; writes one JSON line per run |
| `runs.jsonl`, `runs.log` | the matrix: 12 statements × (1 warm-up + 3 runs) |
| `analyze.py`, `analysis.txt` | per-statement and per-consumer tables (`analyze.py runs.jsonl 35 34 19`) |
| `redump.py` | re-attaches full pool dumps from the server log (see Deviations) |
| `server-stamps-sql.txt` | server log lines for the sql server's startup, tenant hash redacted |
| `profile-run.jsonl`, `profile-run.log`, `profile-run2.log` | q35 runs on the heap-profiling server (first and second invocation) |
| `heap/jeprof.4005296.16.i16.heap`, `heap/jeprof.4005296.20.i20.heap` | base and peak interval dumps |
| `attr.py` | site attribution from `jeprof --collapsed` (copied from the baseline-2633 method) |
| `sites-peak.txt`, `sites-diff.txt` | `attr.py` over i20, and over i20 minus i16 |
| `stacks.py`, `sites-diff-stacks.txt` | caller chains per site |
| `mode.py`, `sites-diff-mode.txt` | per-operator split (partial and final aggregate, coalescer) |
| `jp-diff-lines-cum.txt` | `jeprof --text --lines --show_bytes --cum --base=i16` on i20 |
| `series-check.jsonl`, `series-check.log`, `series-check.txt` | q33 and q19 emit-phase time series |
