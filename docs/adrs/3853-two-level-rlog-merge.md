# ADR-3853: Two-level RLOG merge for many-input buckets

- Status: Accepted
- Date: 2026-11-05
- Refs: #3853, #3819, #3854, #3855, #3856, #3492, ADR-0018, ADR-0032, ADR-0046, ADR-0065, ADR-0699, ADR-0979, ADR-1029, ADR-1702, ADR-2135, ADR-3492, ADR-3819

## Context

ADR-3819 made the RLOG L0 to L1 merge read each input once by holding the
inputs' row groups resident and emitting the streams in ranges, each range a
pass that decodes every block once. On the bucket that motivated it, the
merge never takes that path. Code lines below are on `origin/main`
9b61011bf.

### Stage 0 numbers

Measured 2026-11-05 on GKE n2d-standard-32 against the logs-benchmark bucket
`nofire-production-logsbench-ravel-buffered-r2`, image `manual-ad0ab23` (main
at the #3836 head). Full write-ups: #3819 comments 6660371946 and
6660709107; the epic body of #3853 repeats them.

| unit | inputs | raw | mode | result | source |
|---|---|---|---|---|---|
| hour 498271, shard 0 | 17,066 | 3.9 GB | cursor (`largest_stream_pass_bytes=171164619526`, `hot_streams=2477`) | not finished | comment 6660371946 |
| hour 498273, shard 0 | 19,177 | 16.7 GiB | cursor | stopped after 64 min at 8 reads in flight and after 34 min at 64, both unfinished | comment 6660371946 |
| hour 498274, both shards | about 230 per shard | 0.39 GiB per shard | resident | 520 s, CPU-bound on one core, about 23k records/s (derived) | comment 6660709107 |

Other facts from the same runs:

- Catalog load costs about 6.6 GETs per input: 17 min at 8 in flight, 4.5
  min at 64 in flight, pinned at one core (comment 6660371946). #3855 serves
  the sections from the suffix probe, and #3856 raises the default read
  concurrency from 8 to 32; #3854 gives ravel-cli a tracing subscriber so
  the merge's mode line is visible from `compact-bucket`.
- GCS from the node is latency-bound: 16, 72 and 108 MB/s at 8, 32 and 64
  reads in flight (comment 6660371946).
- Each L0 object holds about 0.7 s of data, about 10.9k records in 1 to 4
  blocks, and about 2,440 of the 2,477 streams. A block spans about 1,800
  streams (comment 6660709107).
- The process working set sits at 5.9 to 6.6 GiB throughout hour 498274
  (about 230 inputs per shard) as well as on the 17k and 19k-input hours
  (measured 2026-11-05). That term is unexplained. Nothing in this ADR is
  sized on an explanation of it; it is an open item for the profile.

### Why the plan is cursor

`plan_merge` (`crates/ravel-maintain/src/rlog.rs:1440`) prices a stream as
the sum, over every input carrying it, of `pass_block_cost` of each
candidate block (`rlog.rs:1491-1537`). Candidate blocks come from SKIP_IDX's
per-block `min_stream_ref` and `max_stream_ref`
(`crates/ravel-logseg/src/ranged.rs:334-345`). On an L0 whose blocks are cut
every 8,192 records of `(stream_ref, ts)`-sorted rows, every block carries
about 1,800 streams, so every stream's candidate set is one block per input
and its estimate is `N x ceiling(one block)`: 171 GB against 3.9 GB raw on
hour 498271, 10 MB per input. No budget holds one stream, `hot_split`
(`rlog.rs:1635-1645`) marks all 2,477 hot, the hot cursor charge leaves no
pass budget, and the plan is cursor (`rlog.rs:1587-1605`).

The charge is not a planning artefact. A pass holds, per input, the whole
decoded block its streams fall in: `block_rows_for_refs`
(`ranged.rs:556-600`) decodes the entire block and the view owns it
(`StreamBlockRows.block`, `ranged.rs:683-700`), and `run_stream_pass`
pushes an unexhausted view into `held` with its heap charge
(`rlog.rs:1989-1996`) and keeps it until the pass pops its last wanted row
(the drained assertion at `rlog.rs:2058-2065`). So the memory floor of a
stream-major pass on this geometry is `N x h`, `h` the decoded heap of one
block. `h` is not measured on a real block. The ADR-3819 fixture figure of
108 bytes per record gives 0.9 MB. The ceiling `block_decode_ceiling_bytes`
(`rlog.rs:820-845`) prices a block by its two per-row terms, `16 B x rows x
total_cols` (`DECODED_CELL_BYTES`, `rlog.rs:704`, `831-833`) and `40 B x rows
x string_cols` (`DECODED_STRING_SLOT_BYTES`, `rlog.rs:711`, `834-836`), plus
the string pages' `uncomp_len` and a slot-spine term that does not scale with
rows (`48 B x total_cols x 5 x 2`, `rlog.rs:838-842`, 32.6 KB at 68 column
ids). On this schema (8,192 rows, 68 column ids, 35 string columns, about
2 MB of strings) the ceiling is `8.9 MB + 11.5 MB + 2 MB`, about 22 MB, and
`h` lies between the two figures. At N = 19,177 the floor is between 17 GB
and 422 GB. No ceiling admits it, no admission rule lowers
it, and a per-stream row count in the format (which the format does not
carry: STREAM_DIR has `first_blk` and `last_blk`,
`docs/log-segment-format.md:264-265`) would not lower it either.

Cursor mode on this geometry costs `N x S` block GETs and `N x S` whole-block
decodes: 47.5 million on hour 498273, each decoding 8,192 rows to emit about
4, a decode overhead of about 1,600x over the records (derived). Holding the
raw bytes resident removes the GETs and keeps the decodes.

### The objective

Compaction throughput at or above ingest throughput: about 134k records/s
per tenant on the benchmark (two shards of about 242M records per hour,
derived at 74 stored bytes per record). Plus a per-unit band for a
240M-record shard-hour. Five minutes per unit on 32 cores is out of reach:
`5 min x 32 cores` is 9,600 core-seconds, or 40 us per record for the whole
merge, and the single-level resident merge already costs 43.5 us per record
on hour 498274 (1 / 23k). A two-level merge costs more than one level.
Reaching five minutes per unit needs either about 50 cores or a decode and
merge cost well under 10 us per record, which only the profile can show.

## Decision

The merge of a many-input bucket runs in two levels. Level 1 merges
contiguous groups of a few hundred inputs into sorted runs on local scratch,
each group through the unchanged `merge_catalogs`, several groups in
parallel. Level 2 merges the runs into the final L1 parts in today's exact
record order, with part encodes pipelined behind the merge. Range workers at
level 2 are a second phase, gated on the first phase's measurement.

### D0. Refuse or route

Before this ADR's merge ships, a bucket whose plan is cursor and whose
predicted decoded rows exceed 16 times its record count is refused with a
typed error, `MergeGeometryUnsupported`, naming the inputs, streams, blocks,
records and predicted decoded rows. The prediction is the sum over every
stream and every input of the candidate blocks' `record_count`, which
`plan_merge` already walks. The refusal happens after the catalog phase and
before the first block GET; the claim is released; the server's maintain
memo records the outcome as terminal for the bucket under the running
binary version so the tick does not rebuild it, and a counter
`ravel_maintain_compaction_refused_total{reason="geometry"}` moves. A
bucket whose cursor plan is under that ratio (few streams per block, a hot
stream) keeps running cursor mode as today. Today's behaviour on the
refused buckets is a merge that holds a claim for days and issues tens of
millions of GETs; a loud refusal is better than that until the fix lands.

After this ADR's merge ships, the same plan routes instead: a plan that is
resident over all inputs runs the single-level merge as today; any other
plan (fetch-per-pass or cursor over all inputs) runs the two-level merge.
The refusal remains for a two-level plan that cannot run: a group size
under 2, a level-2 plan that is neither resident nor fetch-per-pass at one
level-1 worker (D4's constraint (b) fails at `W1 = 1`), or scratch headroom
below the unit's need (D2). The error names which of the three.

### D1. Group merges

Inputs in canonical order are cut into contiguous groups of `G`. `G` is the
largest value for which the shipped planner takes the resident path with one
pass for the group, constraint (a):

```
G x (raw per input + price(the input's blocks)) + run target <= C
```

where `C = U / W1` is the per-worker ceiling, `U` the unit's
`merge_resident_ceiling_bytes` (the 20 GiB `merge_cursor_budget_bytes`
default, `crates/ravel-maintain/src/config.rs:1573`, divided by
`--maintain-unit-concurrency` in ravel-server,
`services/ravel-server/src/config.rs:8715-8716`: 5 GiB at the default
concurrency 4, 20 GiB at 1 and for `compact-bucket`), `W1` the number of
level-1 workers (D4), the run target the fixed 256 MiB of D4, and `price` the
sum of `pass_block_cost` over the input's blocks, since one pass over the
whole group holds every block of every input. Each group is merged by
`merge_catalogs` exactly as a bucket is merged today, into a run (D2), with
`input_set_hash` of the whole bucket and the group index in the run's name.
The group's record sequence is today's sequence restricted to the group,
since the planner and the passes are the same code.

`price` is the ADR-0979 ceiling today: about 29 MB per hour-498273 input
(22 MB for its 8,192-row block and 7 MB for its 2,696-row block, Context).
When the profile reports `decoded_heap_bytes` of a real block, the plan
gains a measured-ratio term and prices a block at that figure, `h`, instead.
The sizing table is in D4, because `G`, the run count `R = N / G` and `W1`
constrain each other through level 2 (D3).

### D2. Scratch runs

A run is an RLOG object written by the frozen writer with transient
settings: `zstd_level 1`, `PageChoice::Estimated`, `group_target_blocks 1`,
all existing `RlogConfig` fields (`crates/ravel-logseg/src/writer.rs:92-109`).
One block per row group matters: a loc names a whole row group (ADR-0699)
and a fetch-per-pass pass at level 2 refetches the group per pass, 32x the
bytes it uses at the default. A group's run is one or more run parts, cut by
the run memory target the same way L1 parts are cut.

Runs live in a per-process scratch directory behind a scratch
`ObjectStoreBackend` over that directory, so `load_catalog_from_object` and
`merge_catalogs` run over runs unmodified. The directory is a cache and
nothing else:

- it is wiped when the process starts and when the unit ends, whatever the
  outcome;
- no run is published, listed by the catalog, or read by another process;
- a crash loses the runs and the unit restarts from the L0 objects, which
  remain the only source of truth. No durability and no recovery path
  depends on the directory, which is the posture of the query-node disk
  cache (`docs/consistency-model.md`, "Physical bytes gone from query-node
  disk caches", ADR-0046);
- before the first block GET the merge computes the unit's scratch need as
  1.3 times the inputs' object sizes, which the catalog probes return, and
  refuses with a typed error naming the need and the free space when the
  directory cannot hold it. It never falls back to cursor mode.

The scratch directory is a new config input: `--scratch-dir` on
`ravel-cli maintain compact-bucket` and `compact-tenant`, and a field on
the operator's maintain Deployment that mounts an ephemeral volume
(Consequences). With no directory configured, the two-level route refuses
(D0); the single-level merge is unaffected.

Why local scratch rather than transient objects or memory is in Rejected
alternatives.

### D3. Level 2 over runs

Level 2 loads the run parts' catalogs from the scratch store and merges them
with the same planner and passes under the unit ceiling `U`. Every run
carries every stream, so a stream's level-2 estimate is `R x price(one run
block)`, and level 2 plans fetch-per-pass only under constraint (b):

```
R x price(one run block) + reads in flight + final part target <= U
```

At ceiling pricing a run block of 8,192 rows costs the same 22 MB as an L0
block, so (b) bounds `R` at about 113 under a 5 GiB `U` and about 770 under
20 GiB; at `h` pricing the bound is `h / 22 MB` times looser. A run of
1.2M records has about 148 blocks and about 17 streams per block, so a pass
covers about 17 streams, decodes each run block once, and reads about 1.6
times the run bytes from local disk across the unit (block-aligned groups,
boundary blocks twice). A level-2 plan that fails (b) is refused (D0), never
run as a cursor merge over the runs. All predicted.

The emitted sequence is today's. Groups are contiguous in canonical input
order, a run stores equal-key ties in push order, and the level-2 tie-break
`(ts bucket, run index)` therefore orders two records the way
`(ts bucket, input index)` orders them today. Part cuts run on that sequence
unchanged, so every L1 part keeps its bytes and its content-addressed key.
One exception is stated rather than hidden: a keyed bucket whose per-bucket
reservation sum exceeds `merge_cursor_budget_bytes` is emitted batch-major
today (ADR-2135 decision 2, `reservation_batches`,
`rlog.rs:3561-3611`), and the batches over runs differ from the batches over
inputs. An unkeyed bucket never batches (`rlog.rs:3566-3568`, one batch at
`width_ns <= 1`), and the logs benchmark is unkeyed. A keyed bucket with
`R` near 770 runs at 37 MB per cursor reaches 28 GB per stream, above the
20 GiB budget, so the exception is live for a keyed many-input bucket at
ceiling pricing; the phase A fixture includes one such bucket and pins its
output as the two-level sequence, not today's.

Part encodes stop blocking the merge. Today a closed part's encode runs on
the read gate (`MaintainReadGate::run`,
`crates/ravel-maintain/src/read_gate.rs:31-45`, a capped `spawn_blocking` in
ravel-server and inline in ravel-cli) and the merge awaits it
(`rlog.rs:2289-2296`), so decode and encode never overlap. Level 2 hands a
closed builder to the gate and continues merging into the next builder,
with at most `E` builders in flight (`E` is 2 by default), each charged to
the tracker's writer term, and awaits the oldest only when the next close
would exceed `E`. Parts are still PUT in `part_index` order. The sequence,
the cuts and the bytes do not change.

### D4. Parallel groups

Level-1 groups run `W1` at a time as tokio tasks. `W1` is at most the
unit's core share (the usable cores divided by `--maintain-unit-concurrency`
in ravel-server, all usable cores for `compact-bucket`: 8 at the server
defaults on the 32-core reference box, 32 at concurrency 1), and the plan
takes the largest `W1` at or under that share for which D1's constraint (a)
and D3's constraint (b) both hold, since a larger `W1` lowers `C`, which
lowers `G`, which raises `R` until level 2 no longer fits. The per-merge
budgets are divided per worker: `merge_cursor_budget_bytes` and
`merge_resident_ceiling_bytes` over `W1`, and the run memory target is a
fixed 256 MiB per builder, not derived from host memory, so the number of
level-1 builders does not move the final parts. The final part memory target
is unchanged and still derived as ADR-2135's 2026-10-02 amendment states.
`MergeCursorBudgetExceeded` stays fail-closed per worker. The
`MergeMemoryTracker` becomes one instance per worker, and the unit's report
sums their high-water marks.

Predicted sizing on hour 498273 (N = 19,177, 0.9 MB raw per input, 2
blocks per input, run blocks of 8,192 rows), by unit ceiling and by block
pricing:

| unit ceiling U, cores per unit | pricing | W1 | C = U / W1 | G | R | level-2 estimate R x price | source |
|---|---|---|---|---|---|---|---|
| 5 GiB, 8 (server defaults) | ceiling, 29 MB per input, 22 MB per run block | 1 | 5 GiB | 170 | 113 | 2.5 GB | predicted |
| 5 GiB, 8 | h = 10 MB (21 MB per input) | 4 | 1.25 GiB | 49 | 392 | 3.9 GB | predicted |
| 5 GiB, 8 | h = 3 MB (6.9 MB per input) | 8 | 640 MiB | 55 | 349 | 1.0 GB | predicted |
| 5 GiB, 8 | h = 0.9 MB (2.7 MB per input) | 8 | 640 MiB | 142 | 135 | 0.1 GB | predicted |
| 20 GiB, 32 (concurrency 1, compact-bucket) | ceiling | 20 | 1 GiB | 25 | 767 | 16.9 GB | predicted |
| 20 GiB, 32 | h = 10 MB | 32 | 640 MiB | 18 | 1,066 | 10.7 GB | predicted |
| 20 GiB, 32 | h = 3 MB | 32 | 640 MiB | 55 | 349 | 1.0 GB | predicted |

At ceiling pricing and the server defaults, level 1 runs on one core per
unit, and parallelism comes from the four units the server merges at once.
The measured-ratio term is what buys level-1 parallelism inside a unit, and
the profile's `h` decides which row applies. Every row keeps the actual
pass memory per worker under `C` by construction; the actual figure is
`G x (raw per input + 2h)` and is reported, not planned.

Every worker ticks the claim checkpoints at the merge loop head and at each
part boundary (`rlog.rs:1773-1791`, `2312-2320`). The `ClaimGuard` is
shared and serializes renewals behind its own lock
(`crates/ravel-maintain/src/claim_guard.rs:294-309`). A checkpoint that
cancels sets a token every worker polls at its next checkpoint, so one
stolen claim stops the unit; the lease still only has to exceed one part
encode plus PUT, as ADR-1029 decision 3 states.

### D5. Range workers (gated)

Phase B splits level 2 into stream ranges, each merged by its own worker
with its own passes and its own builder. It ships only when phase A's
measurement shows the serial level-2 merge below the objective:

| figure | phase B ships if | source |
|---|---|---|
| level-2 records/s with pipelined encode, one unit, hour 498273 | below 134k | predicted; measured after phase A |

Ranges are cut by a data-derived quantum from the run catalogs (the sum of
the candidate blocks' `record_count` over the range), never by the worker
count, and consumed from a queue by `W2` workers, so two nodes with
different core counts produce the same parts. Each range starts a fresh
part and owns `K` part-index slots: range `r` writes parts
`r x K .. r x K + K - 1`, `K` 16 by default, and a range that would need a
seventeenth part fails with a typed error before encoding it. The key
format caps `part_index` at 9,999 (`crates/ravel-commit/src/keys.rs:108-112`),
so a unit of more than 625 ranges fails the same way; hour 498273 at today's
part size needs about 100 ranges. Whether anything reads part indexes as
contiguous (the record, the fold, the sweep) is checked in the phase B
ticket before the rule is fixed.

The final part memory target does not depend on `W2`: it is a part-close
trigger (`rlog_memory_target_bytes`, `rlog.rs:2201`), so dividing it by a
worker count would cut parts at different records on nodes with different
core counts. `W2` is bounded by memory instead: the largest value such that
`W2 x final part target` is at most half of `merge_memory_budget_bytes`
(`crates/ravel-maintain/src/config.rs:1115`), and at most the unit's cores.
On the 30 GB reference box at concurrency 1 the derived target is 1 GiB and
the budget 8 GiB, so `W2 = 4`, which merges no faster than phase A's
pipelined level 2 at `f = 0.75`; phase B pays when the operator sets the
target to 256 MiB (`W2 = 16`, parts four times smaller). Both the target and
`W2`'s bound are part-split settings in the sense of ADR-0979's 2026-10-02
amendment, and the phase B amendment records the bound; the target's
derivation is untouched.

Range-aligned cuts change part boundaries, so phase B re-pins the golden
content-hash vectors. Convergence does not depend on them: a racing
compactor on another version publishes under `CreateIfAbsent`,
`resolve_already_exists` compares only `input_set_hash`
(`crates/ravel-maintain/src/publish.rs:393-420`), and the loser's parts age
out under the unreferenced-part sweep.

### D6. Counters and bands

The merge's per-unit report gains counters, each named by what it counts:
`level1_block_decodes` and `level2_block_decodes` (whole blocks decoded),
`scratch_write_bytes` and `scratch_read_bytes` (bytes the scratch backend
wrote and read, uncompressed on neither side, as stored), `level1_passes`
and `level2_passes`, `level1_groups`, and GETs per input in the catalog and
block phases from the request ledger (`RequestPhase`,
`crates/ravel-maintain/src/request_ledger.rs:81-121`). Scratch traffic is
not a store request and is never pooled into the ledger's phases.

Bands for hour 498273, shard 0 (N = 19,177, about 242M records, 16.7 GiB).
Every figure is predicted; the profile task 108ffe3b (bin
`rlog_compaction_profile` in ravel-bench) and the first phase A run on this
bucket replace the predictions.

| figure | band | miss if | source |
|---|---|---|---|
| GETs, catalog plus block, per input | 1 with #3855 landed, at most 2 | above 2 | predicted |
| bytes read from GCS | 16.7 GiB, within 2% | outside | predicted |
| `level1_block_decodes` | the inputs' block count, within 5% | outside | predicted |
| `level2_block_decodes` | the run parts' block count, within 10% (boundary blocks twice) | above 1.3x | predicted |
| `scratch_write_bytes` | 1.0 to 1.3 x raw | outside | predicted |
| `scratch_read_bytes` | 1.2 to 2.0 x `scratch_write_bytes` | outside | predicted |
| peak working set, one unit, W1 level-1 workers | W1 x (G x (raw per input + 2h) + 256 MiB) plus the unexplained 6 GiB | above U plus 8 GiB | predicted |
| phase A wall, one unit on 32 cores (concurrency 1) | 47 to 95 min | above 2 h | predicted |
| phase B wall, one unit on 32 cores, 256 MiB part target | 14 to 17 min | above 25 min | predicted |
| throughput, server defaults (4 units in flight on 32 cores) | at or above 134k records/s | below 134k | predicted |

The wall figures come from this arithmetic. Today's single-level cost is
43.5 us per record (measured 2026-11-05, hour 498274). Let `f` be the share
of that in page trials and zstd, which the profile measures. Level 1 encodes
runs at zstd 1 and `Estimated`, taken as a quarter of today's encode
(predicted). Level 2 encodes final parts at today's settings. The worker
counts are D4's 20 GiB rows: `W1` between 20 (ceiling pricing) and 32, `W2`
from D5.

| | f = 0.75 | f = 0.5 | source |
|---|---|---|---|
| level 1 per record | 10.9 + 8.2 = 19 us | 21.8 + 5.4 = 27 us | predicted |
| level 2 per record | 43.5 us | 43.5 us | predicted |
| core-seconds for 242M records | 15,100 | 17,100 | predicted |
| I/O floor, 16.7 GiB at 64 in flight at 108 MB/s | 166 s, overlapped with level 1 | 166 s | predicted |
| level 1 wall at W1 = 32 (W1 = 20) | 166 s, I/O bound (230 s) | 204 s (327 s) | predicted |
| level 2 wall, serial merge with pipelined encode (phase A) | 242M x 10.9 us = 44 min | 88 min | predicted |
| level 2 wall at W2 = 4 (derived 1 GiB target) | 44 min | 44 min | predicted |
| level 2 wall at W2 = 16 (256 MiB target) | 11 min | 11 min | predicted |
| cores to sustain 134k records/s | 8.4 | 9.4 | predicted |

The unit-level figures are what fails the five-minute target: at 62.5 to
70.5 us per record the merge needs 15,100 to 17,100 core-seconds per
240M-record unit, which 32 cores deliver in 7.9 to 8.9 minutes only with
no I/O wait, no memory bound on `W2`, and perfect scaling; with `W2`
bounded as D5 states, one unit takes 14 to 17 minutes at a 256 MiB part
target and 47 to 95 minutes at the derived target. The throughput objective
holds at the server defaults because units overlap: four units in flight
each run a level-2 merge thread at `1 / (decode + merge)` records/s, 92k at
`f = 0.75` and 46k at `f = 0.5`, so 368k or 184k records/s for the node, with
level-1 work (7 or 5 cores) and part encodes (12 or 4 cores) on the other
cores; about nine cores sustain the benchmark's ingest. A five-hour backlog
of 2.4G records drains at the node's rate net of the 134k records/s that
keep arriving: about 2.9 h at 368k, about 13 h at 184k.

What the profile must show for each part of the decision to stand:

| profile figure | decides | source |
|---|---|---|
| `decoded_heap_bytes` of a real 8,192-row block | which D4 row applies: G, R and W1, and whether the plan gains a measured-ratio term | predicted 0.9 to 22 MB |
| decode, merge, page-trial and zstd shares of 43.5 us | whether phase A alone meets the objective (it delivers `1 / (decode + merge)` records/s at level 2) or D5 ships | predicted |
| zstd 9 against 3 and 1, `Exhaustive` against `Estimated`, per record | whether level-1 runs cost a quarter of today's encode, and the deferred phase C decision on final-part settings | predicted |
| catalog decode CPU per input | #3855's expected catalog phase | predicted |
| GET throughput past 64 in flight | the I/O floor | predicted |

Phase C, final-part encode settings (`Estimated`, which ADR-3492 measured
at most 10% larger at ingest, and the zstd level ADR-2135 decision 4 sets to
9), is deferred to the profile and is not decided here.

## Rejected alternatives

- **Adaptive admission inside the shipped modes.** The floor of a stream-major
  pass is `N x h` whatever the estimate says; a runtime measure would find
  it after allocating it. Lost on arithmetic: 17 to 422 GB at N = 19,177.
- **A tighter per-block estimate alone.** Even at the optimistic 0.9 MB per
  block the pass needs 17 GB for one unit. Lost on the same arithmetic.
- **Per-stream row counts in the format.** An ADR and a trailer version bump
  that leave the floor where it is: the pass still holds one block per
  input. Lost because it does not move the number.
- **Stream-range workers over all N inputs, no fan-in.** Every worker
  decodes every input's block for its range, so decode work is `W x` and
  memory is `W x N x h`. At 32 workers and 10.9 us decode plus merge per
  record every worker spends 44 min decoding, so the wall is about 48 min
  whatever `W` is, slower than two levels. Lost on decode multiplicity.
- **Runs as transient objects in the bucket.** About 2,000 PUTs and GETs per
  unit, cheap, but a new object class under the frozen key layout
  (`docs/catalog-and-mvcc.md`), an orphan sweep rule and a lifecycle entry in
  the bucket contract: an ADR on the key layout for no functional gain. It
  is the fallback if a maintain pod cannot be given scratch disk.
- **Runs in memory.** 17 to 20 GiB of runs per unit does not fit beside the
  20 GiB cursor budget on the 30 GB reference box, and grows with the hour.
- **Time-sliced final parts, one level.** Publishing each group's output as
  final parts halves the CPU and needs no scratch, but every part then spans
  all 2,477 streams: a stream-filtered query opens every part of the hour,
  about 800 at today's 17 to 25 MB parts, and each part's STREAM_DIR is 180
  KB instead of a few entries. The query path owns that decision and it
  waits for larger parts, which the row-form part builder caps at roughly 17
  MB stored per GiB of memory target on this schema (ADR-3819, decoded over
  stored 55 to 67).
- **Partial compaction records, multi-level records.** The resolver already
  serves two records with disjoint input sets, with an alarm
  (`docs/catalog-and-mvcc.md`, "Racing compactors"), but the driver refuses a
  bucket with any record (`crates/ravel-maintain/src/compact.rs:681-682`),
  the record has no level above 1, and the rewrite fence and the
  superseded-input sweep rest on one record set per bucket. A catalog-model
  ADR with new convergence states, for compaction before the hour seals,
  which nothing asked for. Kept as the follow-up.
- **Bigger L0 objects at ingest, alone.** At 8 MiB objects N falls about 10x,
  and streams per block fall with it, but the planner still sees `N x one
  block` per stream (about 44 GB at 2,000 inputs) and fetch-per-pass on
  one-row-group objects refetches the whole object per pass. Worth doing
  under an ingest ADR (why the objects hold 0.7 s of data when
  `max_flush_delay` is 2 s and `target_bytes` is 8 MiB is that ADR's first
  question); not a fix for this one.
- **Phase B first.** Range workers without fan-in are the third bullet;
  range workers over runs change part boundaries, so they re-pin the golden
  vectors, and the measurement that says whether they are needed does not
  exist until phase A runs.

## Consequences

- A many-input unit reads each input once (one GET per input with #3855),
  decodes each L0 block once and each run block about once, and runs on
  `W1` cores at level 1. The merge CPU is about 1.5x the single-level cost
  (predicted) because the runs are decoded and merged a second time; the run
  encode is cheap by construction.
- Final L1 parts keep their bytes and keys in phase A. The byte-identity
  tests keep their pins: `streaming_merge_output_is_byte_identical_to_stable_sort_order`
  (`rlog.rs:8638`), `differential_part_hashes_match_the_pre_columnar_cursor`
  (`rlog.rs:8920`), `differential_check` (`rlog.rs:11554`),
  `same_inputs_same_bytes_and_keys` in `tests/rlog_determinism.rs` and
  `tests/determinism.rs`. A new fixture of many-stream inputs (N above G,
  so the two-level route runs) asserts the two-level output equals the
  single-level output under a ceiling large enough to hold it. Phase B
  re-pins the vectors and keeps the equality test only for N at or below G.
- A maintain pod needs an ephemeral volume. The operator's maintain
  Deployment (`deploy/k8s/operator/`) mounts none today; it gains a scratch
  volume field, sized at 1.3 times the largest unit's raw bytes times
  `--maintain-unit-concurrency`, about 90 GB for four units of this
  bucket's largest hour. Without it, those units are refused (D0) rather
  than run as cursor merges.
- The merge's memory terms change: per-worker catalogs (G of them, not N),
  per-worker resident raw and pass rows under a per-worker ceiling, `E`
  encoding builders at level 2. ADR-3819's amendment, in this branch, names
  the `N x h` floor and the measured cursor plan. ADR-0065 decision 4 gains
  an amendment with phase A's tracker changes that names the floor and the
  per-worker division of the budgets; that division is documented nowhere
  until then.
- The 6 GiB working set is unexplained and stays in the report as a term of
  its own until the profile names it.
- The ADR-3819 commitments row for hours 08 and 09 is a measured miss; its
  replacement is D6's band table.
- `changelog.d/3819.fixed.md` is rewritten in this branch to claim what
  shipped: three read modes, taken when a block carries few streams, with
  the per-stream cursor merge kept otherwise. The two-level merge gets its
  own fragment when it lands.

### Not in this ADR

- Phase C, final-part encode settings, deferred to the profile.
- Cross-merge admission, a shared budget concurrent units wait on (#3825).
- The ingest-side L0 geometry (why 0.7 s per object), a separate ADR.
- Partial and multi-level compaction records.

```mermaid
flowchart LR
  subgraph l0[L0 inputs in canonical order]
    I1["inputs 0 to G-1"] --> W1a
    I2["inputs G to 2G-1"] --> W1b
    I3["inputs ... to N-1"] --> W1c
  end
  subgraph level1[level 1: W1 group merges in parallel]
    W1a["one GET per input, resident plan, one pass, merge_catalogs unchanged"] --> R1[("run 0 parts, scratch dir, zstd 1")]
    W1b["same"] --> R2[("run 1 parts")]
    W1c["same"] --> R3[("run R-1 parts")]
  end
  subgraph level2[level 2: merge over the runs]
    R1 --> P["fetch-per-pass planner over run parts: about 17 streams per pass, each run block decoded once"]
    R2 --> P
    R3 --> P
    P --> E["stream-major sequence, today's order and cuts"]
    E --> B1["builder"] --> X1["encode on the read gate, E in flight, PUT in part order"]
    E -. "phase B: range r, parts r*K.." .-> B2["builder per range"] --> X2["encode, PUT"]
  end
  X1 --> C["compaction record, CreateIfAbsent"]
  X2 --> C
```
