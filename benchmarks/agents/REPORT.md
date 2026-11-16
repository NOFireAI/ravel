# Agents cost benchmarks: report

Issue #3361 (T27 of epic #3331), ADR-3331 decision 15. Bands:
[prereg.md](prereg.md). Scenarios and report format:
[docs/agents-benchmarks.md](../../docs/agents-benchmarks.md).

## Stamp

| field | value |
|---|---|
| revision | the commit that adds this file |
| commands | `cargo bench -p ravel-agents-sim --bench agents_cost` (small) and `cargo bench -p ravel-agents-sim --bench agents_cost -- full` |
| bands | prereg.md, the original table and both 2026-10-29 revisions |
| store | memory+fault (`MemoryStore` in `FaultStore`, latency layer on top) |
| seed | 3361 |
| sizes | small and full, as prereg.md "Scenario sizes"; discovery N = 100 on small, 100, 1,000 and 10,000 on full |
| clock | tokio-paused (latency is injected latency on the paused clock) |
| heap | stats_alloc global allocator, sampled live-heap rise (`heap-live-delta`) |
| host | aarch64 Raspberry Pi, 4 cores, 8 GB, `CARGO_BUILD_JOBS=2`; no figure depends on it |
| date | 2026-10-29 |

Verified before the run: the band table parsed, every scenario built its
agents before metering (the build phase is checked to have issued no metered
request), and each scenario ran on a fresh store.

## Regenerating

Every figure is deterministic: the store is in memory, the clock is paused
and the seed is fixed, so the same commit prints the same figures on any
host (the heap figure aside, which only has a bound). To reproduce this
report, run the two commands in the stamp at the commit that adds this file.
Each prints a stamp line, one `agents-bench` line per figure,
`agents-bench-usd` lines and an `agents-bench-summary` line, and exits
non-zero on any failure. The tables below are copied from that output; the
full per-figure output is not kept here.

## Result

| size | figures | failures | exit |
|---|---|---|---|
| small | 1462 | 0 | 0 |
| full | 1688 | 0 | 0 |

Every figure was inside its band and printed once, every exact band row
matched one figure, and every operation printed each figure its kind of
operation emits.

## Misses

The first small run failed 124 checks. Each miss, with the layer it pointed
at, is recorded in the first "Revision 2026-10-29" section of prereg.md:

1. State: identical message payloads deduplicated under content addressing,
   so later sends issued a conflict and a read-back GET instead of a PUT.
   The harness was fixed to send a unique payload per request; no band
   moved for this.
2. Table: rows the revision restates were reported as dead rows. The check
   now counts a restated row as superseded.
3. Phase split, turn: every cold turn, and each agent's first warm turn,
   takes the agent over (`TakeoverReason::Activation`), one HEAD GET and one
   CAS more; and the poll reads the previous state object once the agent has
   finished a turn.
4. Phase split, storm: the takeover poll reads HEAD twice and the input
   once, and the commit reads HEAD once.
5. Phase split, discovery: the claim reads the input object and costs two
   CAS (takeover and begin-turn), which puts every delay 40 ms above its
   original prediction, inside its band. The second revision restates the
   delay rows with it.
6. Latency: index seal PUTs on the message path, seen at page size 2 on the
   small run and at page size 64 on the full run.

The saturated-wake discovery rows were pre-registered in the second
revision before that scenario first ran, and every one of its figures
landed on its predicted value.

## Headline figures

Per operation, full size. Request counts are requests per operation.

| figure | warm | cold |
|---|---|---|
| many message: resolve GET, store PUT, publish CAS | 1.5, 1, 1 | 2, 1, 1 |
| many turn: resolve GET, store GET, store PUT, publish CAS | 2.5, 1.5, 2, 2.5 | 3, 1.5, 2, 3 |
| many step: resolve GET, store GET, publish CAS, store conflict | 6, 3, 5, 1 | 6, 3, 5, 1 |
| hot message: publish CAS, publish conflict | 1.75, 0.75 | 1.75, 0.75 |
| hot message p99 latency | 170 ms | 170 ms |

## Discovery

Cold, sweep interval 5 s, default sweep (64 HEAD reads a pass, half of them
reserved for the wake half). Two cases:

- Quiet: no wake hint is due, so the directory half reads all 64 entries a
  pass. This is the best case, `ceil(N / 64)` passes.
- Saturated: every agent but the target has a due timer, so the wake half
  spends its 32 reads every pass and the directory half keeps its own 32.
  This is the guarantee, `ceil(N / 32)` passes, whatever else is due.

| case | N | position | passes | bound | delay ms | resolve GET | LIST | CAS | PUT | DELETE |
|---|---|---|---|---|---|---|---|---|---|---|
| quiet | 100 | start | 1 | 2 | 740 | 66 | 2 | 2 | 0 | 0 |
| quiet | 100 | middle | 1 | 2 | 740 | 66 | 2 | 2 | 0 | 0 |
| quiet | 100 | end | 2 | 2 | 5460 | 102 | 4 | 2 | 0 | 0 |
| quiet | 1000 | start | 1 | 16 | 740 | 66 | 2 | 2 | 0 | 0 |
| quiet | 1000 | middle | 8 | 16 | 35740 | 514 | 16 | 2 | 0 | 0 |
| quiet | 1000 | end | 16 | 16 | 75500 | 1002 | 32 | 2 | 0 | 0 |
| quiet | 10000 | start | 1 | 157 | 740 | 66 | 2 | 2 | 0 | 0 |
| quiet | 10000 | middle | 79 | 157 | 390740 | 5058 | 158 | 2 | 0 | 0 |
| quiet | 10000 | end | 157 | 157 | 780260 | 10002 | 314 | 2 | 0 | 0 |
| saturated | 100 | end | 3 | 4 | 11510 | 265 | 6 | 101 | 64 | 96 |
| saturated | 1000 | end | 31 | 32 | 151670 | 2961 | 62 | 1001 | 960 | 992 |
| saturated | 10000 | end | 312 | 313 | 1556990 | 29953 | 624 | 10001 | 9952 | 9984 |

CAS, PUT and DELETE are the publish CASes, the publish-phase PUTs (the
`_dir` entries the saturated sweep writes back) and the hint DELETEs. In the
saturated case most of them are the sweep firing and settling the other
agents' timers, work it would do anyway, not the cost of finding the target.

A target at the end of a 10,000-agent directory waits 157 passes, about 13
minutes, when nothing else is due, and 312 passes, about 26 minutes, when
the wake half is saturated; the guaranteed bound is 313 passes, 1,565 s.

Dollar figures are derived from `prices.toml`, which is labelled as an
assumption; no band is on a dollar figure.
