# Agents cost benchmarks: report

Issue #3361 (T27 of epic #3331), ADR-3331 decision 15. Bands:
[prereg.md](prereg.md). Scenarios and report format:
[docs/agents-benchmarks.md](../../docs/agents-benchmarks.md).

## Stamp

| field | value |
|---|---|
| revision | 3799d65dd70682d6290ada4a6a810c66ac2a9ac3 |
| store | memory+fault (`MemoryStore` in `FaultStore`, latency layer on top) |
| seed | 3361 |
| clock | tokio-paused (latency is injected latency on the paused clock) |
| heap | stats_alloc global allocator, sampled live-heap rise (`heap-live-delta`) |
| host | aarch64 Raspberry Pi, 4 cores, 8 GB, `CARGO_BUILD_JOBS=2`; no figure depends on it |
| date | 2026-10-29 |

Verified before the run: the band table parsed (prereg.md with its
2026-10-29 revision), every scenario built its agents before metering (the
build phase is checked to have issued no metered request), and each
scenario ran on a fresh store.

## Result

| size | figures | failures | exit |
|---|---|---|---|
| small | 1433 | 0 | 0 |
| full | 1601 | 0 | 0 |

Every figure was inside its band, printed once, and every exact band row
matched one figure.

## First run and its misses

The first small run (revision 32258b5, before the harness fix) failed 124
checks. Each miss, with the layer it pointed at, is recorded in the
"Revision 2026-10-29" section of prereg.md:

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
   CAS (takeover and begin-turn).
6. Latency: index seal PUTs on the message path, seen at page size 2 on the
   small run and at page size 64 on the full run.

## Full run headline figures

Per operation, full size. Request counts are requests per operation.

| figure | warm | cold |
|---|---|---|
| many message: resolve GET, store PUT, publish CAS | 1.5, 1, 1 | 2, 1, 1 |
| many turn: resolve GET, store GET, store PUT, publish CAS | 2.5, 1.5, 2, 2.5 | 3, 1.5, 2, 3 |
| many step: resolve GET, store GET, publish CAS, store conflict | 6, 3, 5, 1 | 6, 3, 5, 1 |
| hot message: publish CAS, publish conflict | 1.75, 0.75 | 1.75, 0.75 |
| hot message p99 latency | 170 ms | 170 ms |

Discovery, cold, sweep interval 5 s, worst case ceil(N/64) passes:

| N | position | passes | worst case | delay ms | resolve GET | LIST |
|---|---|---|---|---|---|---|
| 100 | start | 1 | 2 | 740 | 66 | 2 |
| 100 | middle | 1 | 2 | 740 | 66 | 2 |
| 100 | end | 2 | 2 | 5460 | 102 | 4 |
| 1000 | start | 1 | 16 | 740 | 66 | 2 |
| 1000 | middle | 8 | 16 | 35740 | 514 | 16 |
| 1000 | end | 16 | 16 | 75500 | 1002 | 32 |
| 10000 | start | 1 | 157 | 740 | 66 | 2 |
| 10000 | middle | 79 | 157 | 390740 | 5058 | 158 |
| 10000 | end | 157 | 157 | 780260 | 10002 | 314 |

A target at the end of a 10,000-agent directory waits 157 passes, about 13
minutes, and costs about 10,000 GETs and 314 LISTs to find.

Dollar figures are derived from `prices.toml`, which is labelled as an
assumption; no band is on a dollar figure.

## Small run, verbatim

The output of `cargo bench -p ravel-agents-sim --bench agents_cost` at the
revision above.

```text
agents-bench-stamp revision=3799d65dd70682d6290ada4a6a810c66ac2a9ac3 store=memory+fault seed=3361 clock=tokio-paused size=small heap=stats_alloc
agents-bench hot.warm.activation.ops.count value=1.000 unit=ops
agents-bench hot.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench hot.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench hot.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench hot.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench hot.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.warm.activation.resolve.body_in_bytes value=132.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.store.get value=0.000 unit=requests/op
agents-bench hot.warm.activation.store.put value=1.000 unit=requests/op
agents-bench hot.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench hot.warm.activation.store.list value=0.000 unit=requests/op
agents-bench hot.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench hot.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.store.body_out_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench hot.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench hot.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench hot.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench hot.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench hot.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.publish.body_out_bytes value=232.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench hot.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench hot.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench hot.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench hot.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench hot.warm.message.ops.count value=8.000 unit=ops
agents-bench hot.warm.message.resolve.get value=1.875 unit=requests/op
agents-bench hot.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench hot.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench hot.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench hot.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.warm.message.resolve.body_in_bytes value=1501.375 unit=bytes/op kind=object-body
agents-bench hot.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.message.store.get value=0.750 unit=requests/op
agents-bench hot.warm.message.store.put value=1.750 unit=requests/op
agents-bench hot.warm.message.store.cas value=0.000 unit=requests/op
agents-bench hot.warm.message.store.list value=0.000 unit=requests/op
agents-bench hot.warm.message.store.conflict value=0.750 unit=requests/op
agents-bench hot.warm.message.store.body_in_bytes value=14.250 unit=bytes/op kind=object-body
agents-bench hot.warm.message.store.body_out_bytes value=33.750 unit=bytes/op kind=object-body
agents-bench hot.warm.message.publish.get value=0.000 unit=requests/op
agents-bench hot.warm.message.publish.put value=0.000 unit=requests/op
agents-bench hot.warm.message.publish.cas value=1.750 unit=requests/op
agents-bench hot.warm.message.publish.list value=0.000 unit=requests/op
agents-bench hot.warm.message.publish.conflict value=0.750 unit=requests/op
agents-bench hot.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.message.publish.body_out_bytes value=1768.125 unit=bytes/op kind=object-body
agents-bench hot.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench hot.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.warm.message.other.delete value=0.000 unit=requests/op
agents-bench hot.warm.message.latency.p50_ms value=60.000 unit=ms
agents-bench hot.warm.message.latency.p99_ms value=170.000 unit=ms
agents-bench hot.warm.message.latency.max_ms value=170.000 unit=ms
agents-bench hot.warm.step.ops.count value=8.000 unit=ops
agents-bench hot.warm.step.resolve.get value=6.000 unit=requests/op
agents-bench hot.warm.step.resolve.put value=0.000 unit=requests/op
agents-bench hot.warm.step.resolve.cas value=0.000 unit=requests/op
agents-bench hot.warm.step.resolve.list value=0.000 unit=requests/op
agents-bench hot.warm.step.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.warm.step.resolve.body_in_bytes value=9939.000 unit=bytes/op kind=object-body
agents-bench hot.warm.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.step.store.get value=3.000 unit=requests/op
agents-bench hot.warm.step.store.put value=1.125 unit=requests/op
agents-bench hot.warm.step.store.cas value=0.000 unit=requests/op
agents-bench hot.warm.step.store.list value=0.000 unit=requests/op
agents-bench hot.warm.step.store.conflict value=1.000 unit=requests/op
agents-bench hot.warm.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench hot.warm.step.store.body_out_bytes value=11.250 unit=bytes/op kind=object-body
agents-bench hot.warm.step.publish.get value=0.000 unit=requests/op
agents-bench hot.warm.step.publish.put value=0.000 unit=requests/op
agents-bench hot.warm.step.publish.cas value=5.000 unit=requests/op
agents-bench hot.warm.step.publish.list value=0.000 unit=requests/op
agents-bench hot.warm.step.publish.conflict value=0.000 unit=requests/op
agents-bench hot.warm.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.step.publish.body_out_bytes value=8453.625 unit=bytes/op kind=object-body
agents-bench hot.warm.step.index.page_gets value=0.000 unit=requests/op
agents-bench hot.warm.step.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.warm.step.other.delete value=0.000 unit=requests/op
agents-bench hot.warm.step.latency.p50_ms value=210.000 unit=ms
agents-bench hot.warm.step.latency.p99_ms value=230.000 unit=ms
agents-bench hot.warm.step.latency.max_ms value=230.000 unit=ms
agents-bench hot.warm.turn.ops.count value=8.000 unit=ops
agents-bench hot.warm.turn.resolve.get value=2.125 unit=requests/op
agents-bench hot.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench hot.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench hot.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench hot.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.warm.turn.resolve.body_in_bytes value=3291.875 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.store.get value=1.875 unit=requests/op
agents-bench hot.warm.turn.store.put value=2.000 unit=requests/op
agents-bench hot.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench hot.warm.turn.store.list value=0.000 unit=requests/op
agents-bench hot.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench hot.warm.turn.store.body_in_bytes value=28.250 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.store.body_out_bytes value=46.500 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench hot.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench hot.warm.turn.publish.cas value=2.125 unit=requests/op
agents-bench hot.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench hot.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench hot.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.publish.body_out_bytes value=3161.875 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench hot.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench hot.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench hot.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench hot.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench hot.warm.run.heap.peak_bytes value=406502.000 unit=bytes kind=heap-live-delta
agents-bench hot.cold.activation.ops.count value=2.000 unit=ops
agents-bench hot.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench hot.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench hot.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench hot.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench hot.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.cold.activation.resolve.body_in_bytes value=1247.000 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.store.get value=0.000 unit=requests/op
agents-bench hot.cold.activation.store.put value=1.000 unit=requests/op
agents-bench hot.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench hot.cold.activation.store.list value=0.000 unit=requests/op
agents-bench hot.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench hot.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.store.body_out_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench hot.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench hot.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench hot.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench hot.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench hot.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.publish.body_out_bytes value=787.500 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench hot.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench hot.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench hot.cold.activation.latency.p99_ms value=60.000 unit=ms
agents-bench hot.cold.activation.latency.max_ms value=60.000 unit=ms
agents-bench hot.cold.message.ops.count value=8.000 unit=ops
agents-bench hot.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench hot.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench hot.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench hot.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench hot.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.cold.message.resolve.body_in_bytes value=1649.000 unit=bytes/op kind=object-body
agents-bench hot.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.message.store.get value=0.750 unit=requests/op
agents-bench hot.cold.message.store.put value=1.750 unit=requests/op
agents-bench hot.cold.message.store.cas value=0.000 unit=requests/op
agents-bench hot.cold.message.store.list value=0.000 unit=requests/op
agents-bench hot.cold.message.store.conflict value=0.750 unit=requests/op
agents-bench hot.cold.message.store.body_in_bytes value=14.250 unit=bytes/op kind=object-body
agents-bench hot.cold.message.store.body_out_bytes value=33.750 unit=bytes/op kind=object-body
agents-bench hot.cold.message.publish.get value=0.000 unit=requests/op
agents-bench hot.cold.message.publish.put value=0.000 unit=requests/op
agents-bench hot.cold.message.publish.cas value=1.750 unit=requests/op
agents-bench hot.cold.message.publish.list value=0.000 unit=requests/op
agents-bench hot.cold.message.publish.conflict value=0.750 unit=requests/op
agents-bench hot.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.message.publish.body_out_bytes value=1768.125 unit=bytes/op kind=object-body
agents-bench hot.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench hot.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.cold.message.other.delete value=0.000 unit=requests/op
agents-bench hot.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench hot.cold.message.latency.p99_ms value=170.000 unit=ms
agents-bench hot.cold.message.latency.max_ms value=170.000 unit=ms
agents-bench hot.cold.step.ops.count value=8.000 unit=ops
agents-bench hot.cold.step.resolve.get value=6.000 unit=requests/op
agents-bench hot.cold.step.resolve.put value=0.000 unit=requests/op
agents-bench hot.cold.step.resolve.cas value=0.000 unit=requests/op
agents-bench hot.cold.step.resolve.list value=0.000 unit=requests/op
agents-bench hot.cold.step.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.cold.step.resolve.body_in_bytes value=9939.000 unit=bytes/op kind=object-body
agents-bench hot.cold.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.step.store.get value=3.000 unit=requests/op
agents-bench hot.cold.step.store.put value=1.125 unit=requests/op
agents-bench hot.cold.step.store.cas value=0.000 unit=requests/op
agents-bench hot.cold.step.store.list value=0.000 unit=requests/op
agents-bench hot.cold.step.store.conflict value=1.000 unit=requests/op
agents-bench hot.cold.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench hot.cold.step.store.body_out_bytes value=11.250 unit=bytes/op kind=object-body
agents-bench hot.cold.step.publish.get value=0.000 unit=requests/op
agents-bench hot.cold.step.publish.put value=0.000 unit=requests/op
agents-bench hot.cold.step.publish.cas value=5.000 unit=requests/op
agents-bench hot.cold.step.publish.list value=0.000 unit=requests/op
agents-bench hot.cold.step.publish.conflict value=0.000 unit=requests/op
agents-bench hot.cold.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.step.publish.body_out_bytes value=8453.625 unit=bytes/op kind=object-body
agents-bench hot.cold.step.index.page_gets value=0.000 unit=requests/op
agents-bench hot.cold.step.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.cold.step.other.delete value=0.000 unit=requests/op
agents-bench hot.cold.step.latency.p50_ms value=210.000 unit=ms
agents-bench hot.cold.step.latency.p99_ms value=230.000 unit=ms
agents-bench hot.cold.step.latency.max_ms value=230.000 unit=ms
agents-bench hot.cold.turn.ops.count value=8.000 unit=ops
agents-bench hot.cold.turn.resolve.get value=2.250 unit=requests/op
agents-bench hot.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench hot.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench hot.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench hot.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.cold.turn.resolve.body_in_bytes value=3518.250 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.store.get value=1.875 unit=requests/op
agents-bench hot.cold.turn.store.put value=2.000 unit=requests/op
agents-bench hot.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench hot.cold.turn.store.list value=0.000 unit=requests/op
agents-bench hot.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench hot.cold.turn.store.body_in_bytes value=28.250 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.store.body_out_bytes value=46.500 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench hot.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench hot.cold.turn.publish.cas value=2.250 unit=requests/op
agents-bench hot.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench hot.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench hot.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.publish.body_out_bytes value=3388.250 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench hot.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench hot.cold.turn.latency.p50_ms value=120.000 unit=ms
agents-bench hot.cold.turn.latency.p99_ms value=150.000 unit=ms
agents-bench hot.cold.turn.latency.max_ms value=150.000 unit=ms
agents-bench hot.cold.run.heap.peak_bytes value=410962.000 unit=bytes kind=heap-live-delta
agents-bench many.warm.activation.ops.count value=6.000 unit=ops
agents-bench many.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench many.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench many.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench many.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench many.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench many.warm.activation.resolve.body_in_bytes value=134.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.store.get value=0.000 unit=requests/op
agents-bench many.warm.activation.store.put value=1.000 unit=requests/op
agents-bench many.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench many.warm.activation.store.list value=0.000 unit=requests/op
agents-bench many.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench many.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench many.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench many.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench many.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench many.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench many.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.publish.body_out_bytes value=223.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench many.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench many.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench many.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench many.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench many.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench many.warm.message.ops.count value=12.000 unit=ops
agents-bench many.warm.message.resolve.get value=1.500 unit=requests/op
agents-bench many.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench many.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench many.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench many.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench many.warm.message.resolve.body_in_bytes value=266.500 unit=bytes/op kind=object-body
agents-bench many.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.store.get value=0.000 unit=requests/op
agents-bench many.warm.message.store.put value=1.000 unit=requests/op
agents-bench many.warm.message.store.cas value=0.000 unit=requests/op
agents-bench many.warm.message.store.list value=0.000 unit=requests/op
agents-bench many.warm.message.store.conflict value=0.000 unit=requests/op
agents-bench many.warm.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.publish.get value=0.000 unit=requests/op
agents-bench many.warm.message.publish.put value=0.000 unit=requests/op
agents-bench many.warm.message.publish.cas value=1.000 unit=requests/op
agents-bench many.warm.message.publish.list value=0.000 unit=requests/op
agents-bench many.warm.message.publish.conflict value=0.000 unit=requests/op
agents-bench many.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.publish.body_out_bytes value=387.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench many.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench many.warm.message.other.delete value=0.000 unit=requests/op
agents-bench many.warm.message.latency.p50_ms value=50.000 unit=ms
agents-bench many.warm.message.latency.p99_ms value=60.000 unit=ms
agents-bench many.warm.message.latency.max_ms value=60.000 unit=ms
agents-bench many.warm.step.ops.count value=12.000 unit=ops
agents-bench many.warm.step.resolve.get value=6.000 unit=requests/op
agents-bench many.warm.step.resolve.put value=0.000 unit=requests/op
agents-bench many.warm.step.resolve.cas value=0.000 unit=requests/op
agents-bench many.warm.step.resolve.list value=0.000 unit=requests/op
agents-bench many.warm.step.resolve.conflict value=0.000 unit=requests/op
agents-bench many.warm.step.resolve.body_in_bytes value=3714.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.store.get value=3.000 unit=requests/op
agents-bench many.warm.step.store.put value=1.500 unit=requests/op
agents-bench many.warm.step.store.cas value=0.000 unit=requests/op
agents-bench many.warm.step.store.list value=0.000 unit=requests/op
agents-bench many.warm.step.store.conflict value=1.000 unit=requests/op
agents-bench many.warm.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.store.body_out_bytes value=15.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.publish.get value=0.000 unit=requests/op
agents-bench many.warm.step.publish.put value=0.000 unit=requests/op
agents-bench many.warm.step.publish.cas value=5.000 unit=requests/op
agents-bench many.warm.step.publish.list value=0.000 unit=requests/op
agents-bench many.warm.step.publish.conflict value=0.000 unit=requests/op
agents-bench many.warm.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.publish.body_out_bytes value=3266.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.index.page_gets value=0.000 unit=requests/op
agents-bench many.warm.step.index.planner_misses value=0.000 unit=misses/op
agents-bench many.warm.step.other.delete value=0.000 unit=requests/op
agents-bench many.warm.step.latency.p50_ms value=210.000 unit=ms
agents-bench many.warm.step.latency.p99_ms value=230.000 unit=ms
agents-bench many.warm.step.latency.max_ms value=230.000 unit=ms
agents-bench many.warm.turn.ops.count value=12.000 unit=ops
agents-bench many.warm.turn.resolve.get value=2.500 unit=requests/op
agents-bench many.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench many.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench many.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench many.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench many.warm.turn.resolve.body_in_bytes value=1241.500 unit=bytes/op kind=object-body
agents-bench many.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.turn.store.get value=1.500 unit=requests/op
agents-bench many.warm.turn.store.put value=2.000 unit=requests/op
agents-bench many.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench many.warm.turn.store.list value=0.000 unit=requests/op
agents-bench many.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench many.warm.turn.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench many.warm.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench many.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench many.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench many.warm.turn.publish.cas value=2.500 unit=requests/op
agents-bench many.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench many.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench many.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.turn.publish.body_out_bytes value=1140.500 unit=bytes/op kind=object-body
agents-bench many.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench many.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench many.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench many.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench many.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench many.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench many.warm.run.heap.peak_bytes value=274401.000 unit=bytes kind=heap-live-delta
agents-bench many.cold.activation.ops.count value=12.000 unit=ops
agents-bench many.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench many.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench many.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench many.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench many.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench many.cold.activation.resolve.body_in_bytes value=466.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.store.get value=0.000 unit=requests/op
agents-bench many.cold.activation.store.put value=1.000 unit=requests/op
agents-bench many.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench many.cold.activation.store.list value=0.000 unit=requests/op
agents-bench many.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench many.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench many.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench many.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench many.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench many.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench many.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.publish.body_out_bytes value=387.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench many.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench many.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench many.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench many.cold.activation.latency.p99_ms value=60.000 unit=ms
agents-bench many.cold.activation.latency.max_ms value=60.000 unit=ms
agents-bench many.cold.message.ops.count value=12.000 unit=ops
agents-bench many.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench many.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench many.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench many.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench many.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench many.cold.message.resolve.body_in_bytes value=466.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.store.get value=0.000 unit=requests/op
agents-bench many.cold.message.store.put value=1.000 unit=requests/op
agents-bench many.cold.message.store.cas value=0.000 unit=requests/op
agents-bench many.cold.message.store.list value=0.000 unit=requests/op
agents-bench many.cold.message.store.conflict value=0.000 unit=requests/op
agents-bench many.cold.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.publish.get value=0.000 unit=requests/op
agents-bench many.cold.message.publish.put value=0.000 unit=requests/op
agents-bench many.cold.message.publish.cas value=1.000 unit=requests/op
agents-bench many.cold.message.publish.list value=0.000 unit=requests/op
agents-bench many.cold.message.publish.conflict value=0.000 unit=requests/op
agents-bench many.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.publish.body_out_bytes value=387.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench many.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench many.cold.message.other.delete value=0.000 unit=requests/op
agents-bench many.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench many.cold.message.latency.p99_ms value=60.000 unit=ms
agents-bench many.cold.message.latency.max_ms value=60.000 unit=ms
agents-bench many.cold.step.ops.count value=12.000 unit=ops
agents-bench many.cold.step.resolve.get value=6.000 unit=requests/op
agents-bench many.cold.step.resolve.put value=0.000 unit=requests/op
agents-bench many.cold.step.resolve.cas value=0.000 unit=requests/op
agents-bench many.cold.step.resolve.list value=0.000 unit=requests/op
agents-bench many.cold.step.resolve.conflict value=0.000 unit=requests/op
agents-bench many.cold.step.resolve.body_in_bytes value=3714.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.store.get value=3.000 unit=requests/op
agents-bench many.cold.step.store.put value=1.500 unit=requests/op
agents-bench many.cold.step.store.cas value=0.000 unit=requests/op
agents-bench many.cold.step.store.list value=0.000 unit=requests/op
agents-bench many.cold.step.store.conflict value=1.000 unit=requests/op
agents-bench many.cold.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.store.body_out_bytes value=15.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.publish.get value=0.000 unit=requests/op
agents-bench many.cold.step.publish.put value=0.000 unit=requests/op
agents-bench many.cold.step.publish.cas value=5.000 unit=requests/op
agents-bench many.cold.step.publish.list value=0.000 unit=requests/op
agents-bench many.cold.step.publish.conflict value=0.000 unit=requests/op
agents-bench many.cold.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.publish.body_out_bytes value=3266.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.index.page_gets value=0.000 unit=requests/op
agents-bench many.cold.step.index.planner_misses value=0.000 unit=misses/op
agents-bench many.cold.step.other.delete value=0.000 unit=requests/op
agents-bench many.cold.step.latency.p50_ms value=210.000 unit=ms
agents-bench many.cold.step.latency.p99_ms value=230.000 unit=ms
agents-bench many.cold.step.latency.max_ms value=230.000 unit=ms
agents-bench many.cold.turn.ops.count value=12.000 unit=ops
agents-bench many.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench many.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench many.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench many.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench many.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench many.cold.turn.resolve.body_in_bytes value=1517.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.store.get value=1.500 unit=requests/op
agents-bench many.cold.turn.store.put value=2.000 unit=requests/op
agents-bench many.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench many.cold.turn.store.list value=0.000 unit=requests/op
agents-bench many.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench many.cold.turn.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench many.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench many.cold.turn.publish.cas value=3.000 unit=requests/op
agents-bench many.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench many.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench many.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.publish.body_out_bytes value=1416.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench many.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench many.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench many.cold.turn.latency.p50_ms value=140.000 unit=ms
agents-bench many.cold.turn.latency.p99_ms value=150.000 unit=ms
agents-bench many.cold.turn.latency.max_ms value=150.000 unit=ms
agents-bench many.cold.run.heap.peak_bytes value=273339.000 unit=bytes kind=heap-live-delta
agents-bench dormant.warm.activation.ops.count value=3.000 unit=ops
agents-bench dormant.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.body_in_bytes value=140.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.store.get value=0.000 unit=requests/op
agents-bench dormant.warm.activation.store.put value=1.000 unit=requests/op
agents-bench dormant.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench dormant.warm.activation.store.list value=0.000 unit=requests/op
agents-bench dormant.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench dormant.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench dormant.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench dormant.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench dormant.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.publish.body_out_bytes value=226.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench dormant.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench dormant.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench dormant.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench dormant.warm.message.ops.count value=6.000 unit=ops
agents-bench dormant.warm.message.resolve.get value=1.500 unit=requests/op
agents-bench dormant.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench dormant.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench dormant.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.message.resolve.body_in_bytes value=271.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.store.get value=0.000 unit=requests/op
agents-bench dormant.warm.message.store.put value=1.000 unit=requests/op
agents-bench dormant.warm.message.store.cas value=0.000 unit=requests/op
agents-bench dormant.warm.message.store.list value=0.000 unit=requests/op
agents-bench dormant.warm.message.store.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.publish.get value=0.000 unit=requests/op
agents-bench dormant.warm.message.publish.put value=0.000 unit=requests/op
agents-bench dormant.warm.message.publish.cas value=1.000 unit=requests/op
agents-bench dormant.warm.message.publish.list value=0.000 unit=requests/op
agents-bench dormant.warm.message.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.publish.body_out_bytes value=390.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.warm.message.other.delete value=0.000 unit=requests/op
agents-bench dormant.warm.message.latency.p50_ms value=50.000 unit=ms
agents-bench dormant.warm.message.latency.p99_ms value=60.000 unit=ms
agents-bench dormant.warm.message.latency.max_ms value=60.000 unit=ms
agents-bench dormant.warm.step.ops.count value=6.000 unit=ops
agents-bench dormant.warm.step.resolve.get value=6.000 unit=requests/op
agents-bench dormant.warm.step.resolve.put value=0.000 unit=requests/op
agents-bench dormant.warm.step.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.warm.step.resolve.list value=0.000 unit=requests/op
agents-bench dormant.warm.step.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.step.resolve.body_in_bytes value=3732.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.store.get value=3.000 unit=requests/op
agents-bench dormant.warm.step.store.put value=1.500 unit=requests/op
agents-bench dormant.warm.step.store.cas value=0.000 unit=requests/op
agents-bench dormant.warm.step.store.list value=0.000 unit=requests/op
agents-bench dormant.warm.step.store.conflict value=1.000 unit=requests/op
agents-bench dormant.warm.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.store.body_out_bytes value=15.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.publish.get value=0.000 unit=requests/op
agents-bench dormant.warm.step.publish.put value=0.000 unit=requests/op
agents-bench dormant.warm.step.publish.cas value=5.000 unit=requests/op
agents-bench dormant.warm.step.publish.list value=0.000 unit=requests/op
agents-bench dormant.warm.step.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.publish.body_out_bytes value=3281.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.warm.step.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.warm.step.other.delete value=0.000 unit=requests/op
agents-bench dormant.warm.step.latency.p50_ms value=210.000 unit=ms
agents-bench dormant.warm.step.latency.p99_ms value=230.000 unit=ms
agents-bench dormant.warm.step.latency.max_ms value=230.000 unit=ms
agents-bench dormant.warm.turn.ops.count value=6.000 unit=ops
agents-bench dormant.warm.turn.resolve.get value=2.500 unit=requests/op
agents-bench dormant.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench dormant.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench dormant.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.turn.resolve.body_in_bytes value=1249.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.store.get value=1.500 unit=requests/op
agents-bench dormant.warm.turn.store.put value=2.000 unit=requests/op
agents-bench dormant.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench dormant.warm.turn.store.list value=0.000 unit=requests/op
agents-bench dormant.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.turn.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench dormant.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench dormant.warm.turn.publish.cas value=2.500 unit=requests/op
agents-bench dormant.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench dormant.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.publish.body_out_bytes value=1148.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench dormant.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench dormant.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench dormant.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench dormant.warm.run.isolation.dormant_requests value=0.000 unit=requests
agents-bench dormant.warm.run.heap.peak_bytes value=520070.000 unit=bytes kind=heap-live-delta
agents-bench dormant.cold.activation.ops.count value=6.000 unit=ops
agents-bench dormant.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.body_in_bytes value=472.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.store.get value=0.000 unit=requests/op
agents-bench dormant.cold.activation.store.put value=1.000 unit=requests/op
agents-bench dormant.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench dormant.cold.activation.store.list value=0.000 unit=requests/op
agents-bench dormant.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench dormant.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench dormant.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench dormant.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench dormant.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.publish.body_out_bytes value=390.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench dormant.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench dormant.cold.activation.latency.p99_ms value=60.000 unit=ms
agents-bench dormant.cold.activation.latency.max_ms value=60.000 unit=ms
agents-bench dormant.cold.message.ops.count value=6.000 unit=ops
agents-bench dormant.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench dormant.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench dormant.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench dormant.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.message.resolve.body_in_bytes value=472.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.store.get value=0.000 unit=requests/op
agents-bench dormant.cold.message.store.put value=1.000 unit=requests/op
agents-bench dormant.cold.message.store.cas value=0.000 unit=requests/op
agents-bench dormant.cold.message.store.list value=0.000 unit=requests/op
agents-bench dormant.cold.message.store.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.publish.get value=0.000 unit=requests/op
agents-bench dormant.cold.message.publish.put value=0.000 unit=requests/op
agents-bench dormant.cold.message.publish.cas value=1.000 unit=requests/op
agents-bench dormant.cold.message.publish.list value=0.000 unit=requests/op
agents-bench dormant.cold.message.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.publish.body_out_bytes value=390.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.cold.message.other.delete value=0.000 unit=requests/op
agents-bench dormant.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench dormant.cold.message.latency.p99_ms value=60.000 unit=ms
agents-bench dormant.cold.message.latency.max_ms value=60.000 unit=ms
agents-bench dormant.cold.step.ops.count value=6.000 unit=ops
agents-bench dormant.cold.step.resolve.get value=6.000 unit=requests/op
agents-bench dormant.cold.step.resolve.put value=0.000 unit=requests/op
agents-bench dormant.cold.step.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.cold.step.resolve.list value=0.000 unit=requests/op
agents-bench dormant.cold.step.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.step.resolve.body_in_bytes value=3732.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.store.get value=3.000 unit=requests/op
agents-bench dormant.cold.step.store.put value=1.500 unit=requests/op
agents-bench dormant.cold.step.store.cas value=0.000 unit=requests/op
agents-bench dormant.cold.step.store.list value=0.000 unit=requests/op
agents-bench dormant.cold.step.store.conflict value=1.000 unit=requests/op
agents-bench dormant.cold.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.store.body_out_bytes value=15.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.publish.get value=0.000 unit=requests/op
agents-bench dormant.cold.step.publish.put value=0.000 unit=requests/op
agents-bench dormant.cold.step.publish.cas value=5.000 unit=requests/op
agents-bench dormant.cold.step.publish.list value=0.000 unit=requests/op
agents-bench dormant.cold.step.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.publish.body_out_bytes value=3281.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.cold.step.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.cold.step.other.delete value=0.000 unit=requests/op
agents-bench dormant.cold.step.latency.p50_ms value=210.000 unit=ms
agents-bench dormant.cold.step.latency.p99_ms value=230.000 unit=ms
agents-bench dormant.cold.step.latency.max_ms value=230.000 unit=ms
agents-bench dormant.cold.turn.ops.count value=6.000 unit=ops
agents-bench dormant.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.body_in_bytes value=1526.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.store.get value=1.500 unit=requests/op
agents-bench dormant.cold.turn.store.put value=2.000 unit=requests/op
agents-bench dormant.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench dormant.cold.turn.store.list value=0.000 unit=requests/op
agents-bench dormant.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.turn.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench dormant.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench dormant.cold.turn.publish.cas value=3.000 unit=requests/op
agents-bench dormant.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench dormant.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.publish.body_out_bytes value=1425.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench dormant.cold.turn.latency.p50_ms value=140.000 unit=ms
agents-bench dormant.cold.turn.latency.p99_ms value=150.000 unit=ms
agents-bench dormant.cold.turn.latency.max_ms value=150.000 unit=ms
agents-bench dormant.cold.run.isolation.dormant_requests value=0.000 unit=requests
agents-bench dormant.cold.run.heap.peak_bytes value=522404.000 unit=bytes kind=heap-live-delta
agents-bench index-p64.warm.activation.ops.count value=1.000 unit=ops
agents-bench index-p64.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.body_in_bytes value=144.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.store.get value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.store.put value=1.000 unit=requests/op
agents-bench index-p64.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.store.list value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.publish.body_out_bytes value=228.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench index-p64.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench index-p64.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench index-p64.warm.message.ops.count value=12.000 unit=ops
agents-bench index-p64.warm.message.resolve.get value=1.083 unit=requests/op
agents-bench index-p64.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.message.resolve.body_in_bytes value=824.917 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.store.get value=0.000 unit=requests/op
agents-bench index-p64.warm.message.store.put value=1.000 unit=requests/op
agents-bench index-p64.warm.message.store.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.message.store.list value=0.000 unit=requests/op
agents-bench index-p64.warm.message.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.store.body_out_bytes value=16.167 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.publish.get value=0.000 unit=requests/op
agents-bench index-p64.warm.message.publish.put value=0.000 unit=requests/op
agents-bench index-p64.warm.message.publish.cas value=1.000 unit=requests/op
agents-bench index-p64.warm.message.publish.list value=0.000 unit=requests/op
agents-bench index-p64.warm.message.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.publish.body_out_bytes value=971.583 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.warm.message.other.delete value=0.000 unit=requests/op
agents-bench index-p64.warm.message.latency.p50_ms value=50.000 unit=ms
agents-bench index-p64.warm.message.latency.p99_ms value=60.000 unit=ms
agents-bench index-p64.warm.message.latency.max_ms value=60.000 unit=ms
agents-bench index-p64.warm.turn.ops.count value=12.000 unit=ops
agents-bench index-p64.warm.turn.resolve.get value=2.083 unit=requests/op
agents-bench index-p64.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.resolve.body_in_bytes value=2051.333 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.store.get value=1.917 unit=requests/op
agents-bench index-p64.warm.turn.store.put value=2.000 unit=requests/op
agents-bench index-p64.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.store.list value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.store.body_in_bytes value=25.500 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.store.body_out_bytes value=43.667 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.publish.cas value=2.083 unit=requests/op
agents-bench index-p64.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.publish.body_out_bytes value=2028.250 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench index-p64.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench index-p64.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench index-p64.warm.run.heap.peak_bytes value=371623.000 unit=bytes kind=heap-live-delta
agents-bench index-p64.cold.activation.ops.count value=12.000 unit=ops
agents-bench index-p64.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.body_in_bytes value=1637.833 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.store.get value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.store.put value=1.000 unit=requests/op
agents-bench index-p64.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.store.list value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.store.body_out_bytes value=16.167 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.publish.body_out_bytes value=971.583 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench index-p64.cold.activation.latency.p99_ms value=60.000 unit=ms
agents-bench index-p64.cold.activation.latency.max_ms value=60.000 unit=ms
agents-bench index-p64.cold.message.ops.count value=12.000 unit=ops
agents-bench index-p64.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.body_in_bytes value=1637.833 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.store.get value=0.000 unit=requests/op
agents-bench index-p64.cold.message.store.put value=1.000 unit=requests/op
agents-bench index-p64.cold.message.store.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.message.store.list value=0.000 unit=requests/op
agents-bench index-p64.cold.message.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.store.body_out_bytes value=16.167 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.publish.get value=0.000 unit=requests/op
agents-bench index-p64.cold.message.publish.put value=0.000 unit=requests/op
agents-bench index-p64.cold.message.publish.cas value=1.000 unit=requests/op
agents-bench index-p64.cold.message.publish.list value=0.000 unit=requests/op
agents-bench index-p64.cold.message.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.publish.body_out_bytes value=971.583 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.cold.message.other.delete value=0.000 unit=requests/op
agents-bench index-p64.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench index-p64.cold.message.latency.p99_ms value=60.000 unit=ms
agents-bench index-p64.cold.message.latency.max_ms value=60.000 unit=ms
agents-bench index-p64.cold.turn.ops.count value=12.000 unit=ops
agents-bench index-p64.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.body_in_bytes value=3003.917 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.store.get value=1.917 unit=requests/op
agents-bench index-p64.cold.turn.store.put value=2.000 unit=requests/op
agents-bench index-p64.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.store.list value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.store.body_in_bytes value=25.500 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.store.body_out_bytes value=43.667 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.cas value=3.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.publish.body_out_bytes value=2980.833 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.latency.p50_ms value=150.000 unit=ms
agents-bench index-p64.cold.turn.latency.p99_ms value=150.000 unit=ms
agents-bench index-p64.cold.turn.latency.max_ms value=150.000 unit=ms
agents-bench index-p64.cold.run.heap.peak_bytes value=461035.000 unit=bytes kind=heap-live-delta
agents-bench index-p2.warm.activation.ops.count value=1.000 unit=ops
agents-bench index-p2.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.body_in_bytes value=142.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.store.get value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.store.put value=1.000 unit=requests/op
agents-bench index-p2.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.store.list value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.publish.body_out_bytes value=227.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench index-p2.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench index-p2.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench index-p2.warm.message.ops.count value=12.000 unit=ops
agents-bench index-p2.warm.message.resolve.get value=1.083 unit=requests/op
agents-bench index-p2.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.message.resolve.body_in_bytes value=553.917 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.store.get value=0.000 unit=requests/op
agents-bench index-p2.warm.message.store.put value=1.417 unit=requests/op
agents-bench index-p2.warm.message.store.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.message.store.list value=0.000 unit=requests/op
agents-bench index-p2.warm.message.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.store.body_out_bytes value=60.333 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.publish.get value=0.000 unit=requests/op
agents-bench index-p2.warm.message.publish.put value=0.000 unit=requests/op
agents-bench index-p2.warm.message.publish.cas value=1.000 unit=requests/op
agents-bench index-p2.warm.message.publish.list value=0.000 unit=requests/op
agents-bench index-p2.warm.message.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.publish.body_out_bytes value=682.167 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.warm.message.other.delete value=0.000 unit=requests/op
agents-bench index-p2.warm.message.latency.p50_ms value=50.000 unit=ms
agents-bench index-p2.warm.message.latency.p99_ms value=70.000 unit=ms
agents-bench index-p2.warm.message.latency.max_ms value=70.000 unit=ms
agents-bench index-p2.warm.turn.ops.count value=12.000 unit=ops
agents-bench index-p2.warm.turn.resolve.get value=2.083 unit=requests/op
agents-bench index-p2.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.resolve.body_in_bytes value=1472.417 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.store.get value=1.917 unit=requests/op
agents-bench index-p2.warm.turn.store.put value=2.417 unit=requests/op
agents-bench index-p2.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.store.list value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.store.body_in_bytes value=25.500 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.store.body_out_bytes value=107.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.publish.cas value=2.083 unit=requests/op
agents-bench index-p2.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.publish.body_out_bytes value=1412.917 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench index-p2.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench index-p2.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench index-p2.warm.run.heap.peak_bytes value=194190.000 unit=bytes kind=heap-live-delta
agents-bench index-p2.cold.activation.ops.count value=12.000 unit=ops
agents-bench index-p2.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.body_in_bytes value=1096.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.store.get value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.store.put value=1.417 unit=requests/op
agents-bench index-p2.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.store.list value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.store.body_out_bytes value=60.333 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.publish.body_out_bytes value=682.167 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench index-p2.cold.activation.latency.p99_ms value=80.000 unit=ms
agents-bench index-p2.cold.activation.latency.max_ms value=80.000 unit=ms
agents-bench index-p2.cold.message.ops.count value=12.000 unit=ops
agents-bench index-p2.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.body_in_bytes value=1096.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.store.get value=0.000 unit=requests/op
agents-bench index-p2.cold.message.store.put value=1.417 unit=requests/op
agents-bench index-p2.cold.message.store.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.message.store.list value=0.000 unit=requests/op
agents-bench index-p2.cold.message.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.store.body_out_bytes value=60.333 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.publish.get value=0.000 unit=requests/op
agents-bench index-p2.cold.message.publish.put value=0.000 unit=requests/op
agents-bench index-p2.cold.message.publish.cas value=1.000 unit=requests/op
agents-bench index-p2.cold.message.publish.list value=0.000 unit=requests/op
agents-bench index-p2.cold.message.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.publish.body_out_bytes value=682.167 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.cold.message.other.delete value=0.000 unit=requests/op
agents-bench index-p2.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench index-p2.cold.message.latency.p99_ms value=80.000 unit=ms
agents-bench index-p2.cold.message.latency.max_ms value=80.000 unit=ms
agents-bench index-p2.cold.turn.ops.count value=12.000 unit=ops
agents-bench index-p2.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.body_in_bytes value=2135.667 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.store.get value=1.917 unit=requests/op
agents-bench index-p2.cold.turn.store.put value=2.417 unit=requests/op
agents-bench index-p2.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.store.list value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.store.body_in_bytes value=25.500 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.store.body_out_bytes value=107.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.cas value=3.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.publish.body_out_bytes value=2076.167 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.latency.p50_ms value=150.000 unit=ms
agents-bench index-p2.cold.turn.latency.p99_ms value=170.000 unit=ms
agents-bench index-p2.cold.turn.latency.max_ms value=170.000 unit=ms
agents-bench index-p2.cold.run.heap.peak_bytes value=213026.000 unit=bytes kind=heap-live-delta
agents-bench storm.cold.activation.ops.count value=8.000 unit=ops
agents-bench storm.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench storm.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench storm.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench storm.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench storm.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench storm.cold.activation.resolve.body_in_bytes value=676.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.store.get value=1.000 unit=requests/op
agents-bench storm.cold.activation.store.put value=0.000 unit=requests/op
agents-bench storm.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench storm.cold.activation.store.list value=0.000 unit=requests/op
agents-bench storm.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench storm.cold.activation.store.body_in_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench storm.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench storm.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench storm.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench storm.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench storm.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.publish.body_out_bytes value=338.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench storm.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench storm.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench storm.cold.activation.latency.p50_ms value=50.000 unit=ms
agents-bench storm.cold.activation.latency.p99_ms value=50.000 unit=ms
agents-bench storm.cold.activation.latency.max_ms value=50.000 unit=ms
agents-bench storm.cold.ingest.ops.count value=10.000 unit=ops
agents-bench storm.cold.ingest.telemetry.put value=1.000 unit=requests/op
agents-bench storm.cold.ingest.telemetry.get value=1.000 unit=requests/op
agents-bench storm.cold.ingest.telemetry.body_in_bytes value=7.000 unit=bytes/op kind=object-body
agents-bench storm.cold.ingest.telemetry.body_out_bytes value=7.000 unit=bytes/op kind=object-body
agents-bench storm.cold.ingest.latency.p50_ms value=30.000 unit=ms
agents-bench storm.cold.ingest.latency.p99_ms value=30.000 unit=ms
agents-bench storm.cold.ingest.latency.max_ms value=30.000 unit=ms
agents-bench storm.cold.pass.ops.count value=1.000 unit=ops
agents-bench storm.cold.pass.resolve.get value=8.000 unit=requests/op
agents-bench storm.cold.pass.resolve.put value=0.000 unit=requests/op
agents-bench storm.cold.pass.resolve.cas value=0.000 unit=requests/op
agents-bench storm.cold.pass.resolve.list value=4.000 unit=requests/op
agents-bench storm.cold.pass.resolve.conflict value=0.000 unit=requests/op
agents-bench storm.cold.pass.resolve.body_in_bytes value=2704.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.store.get value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.put value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.cas value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.list value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.conflict value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.publish.get value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.put value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.cas value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.list value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.conflict value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.publish.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.index.page_gets value=0.000 unit=requests/op
agents-bench storm.cold.pass.index.planner_misses value=0.000 unit=misses/op
agents-bench storm.cold.pass.other.delete value=0.000 unit=requests/op
agents-bench storm.cold.pass.latency.p50_ms value=140.000 unit=ms
agents-bench storm.cold.pass.latency.p99_ms value=140.000 unit=ms
agents-bench storm.cold.pass.latency.max_ms value=140.000 unit=ms
agents-bench storm.cold.turn.ops.count value=8.000 unit=ops
agents-bench storm.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench storm.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench storm.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench storm.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench storm.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench storm.cold.turn.resolve.body_in_bytes value=1014.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.store.get value=1.000 unit=requests/op
agents-bench storm.cold.turn.store.put value=2.000 unit=requests/op
agents-bench storm.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench storm.cold.turn.store.list value=0.000 unit=requests/op
agents-bench storm.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench storm.cold.turn.store.body_in_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench storm.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench storm.cold.turn.publish.cas value=2.000 unit=requests/op
agents-bench storm.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench storm.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench storm.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.publish.body_out_bytes value=605.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench storm.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench storm.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench storm.cold.turn.latency.p50_ms value=120.000 unit=ms
agents-bench storm.cold.turn.latency.p99_ms value=120.000 unit=ms
agents-bench storm.cold.turn.latency.max_ms value=120.000 unit=ms
agents-bench storm.cold.run.heap.peak_bytes value=203167.000 unit=bytes kind=heap-live-delta
agents-bench slow.warm.activation.ops.count value=1.000 unit=ops
agents-bench slow.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench slow.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench slow.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench slow.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench slow.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.warm.activation.resolve.body_in_bytes value=134.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.store.get value=0.000 unit=requests/op
agents-bench slow.warm.activation.store.put value=1.000 unit=requests/op
agents-bench slow.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench slow.warm.activation.store.list value=0.000 unit=requests/op
agents-bench slow.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench slow.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.store.body_out_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench slow.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench slow.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench slow.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench slow.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench slow.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.publish.body_out_bytes value=233.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench slow.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench slow.warm.activation.latency.p50_ms value=802.000 unit=ms
agents-bench slow.warm.activation.latency.p99_ms value=802.000 unit=ms
agents-bench slow.warm.activation.latency.max_ms value=802.000 unit=ms
agents-bench slow.warm.message.ops.count value=8.000 unit=ops
agents-bench slow.warm.message.resolve.get value=1.875 unit=requests/op
agents-bench slow.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench slow.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench slow.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench slow.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.warm.message.resolve.body_in_bytes value=1503.250 unit=bytes/op kind=object-body
agents-bench slow.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.message.store.get value=0.750 unit=requests/op
agents-bench slow.warm.message.store.put value=1.750 unit=requests/op
agents-bench slow.warm.message.store.cas value=0.000 unit=requests/op
agents-bench slow.warm.message.store.list value=0.000 unit=requests/op
agents-bench slow.warm.message.store.conflict value=0.750 unit=requests/op
agents-bench slow.warm.message.store.body_in_bytes value=14.250 unit=bytes/op kind=object-body
agents-bench slow.warm.message.store.body_out_bytes value=33.750 unit=bytes/op kind=object-body
agents-bench slow.warm.message.publish.get value=0.000 unit=requests/op
agents-bench slow.warm.message.publish.put value=0.000 unit=requests/op
agents-bench slow.warm.message.publish.cas value=1.750 unit=requests/op
agents-bench slow.warm.message.publish.list value=0.000 unit=requests/op
agents-bench slow.warm.message.publish.conflict value=0.750 unit=requests/op
agents-bench slow.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.message.publish.body_out_bytes value=1769.875 unit=bytes/op kind=object-body
agents-bench slow.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench slow.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.warm.message.other.delete value=0.000 unit=requests/op
agents-bench slow.warm.message.latency.p50_ms value=802.000 unit=ms
agents-bench slow.warm.message.latency.p99_ms value=2839.000 unit=ms
agents-bench slow.warm.message.latency.max_ms value=2839.000 unit=ms
agents-bench slow.warm.step.ops.count value=8.000 unit=ops
agents-bench slow.warm.step.resolve.get value=6.000 unit=requests/op
agents-bench slow.warm.step.resolve.put value=0.000 unit=requests/op
agents-bench slow.warm.step.resolve.cas value=0.000 unit=requests/op
agents-bench slow.warm.step.resolve.list value=0.000 unit=requests/op
agents-bench slow.warm.step.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.warm.step.resolve.body_in_bytes value=9945.000 unit=bytes/op kind=object-body
agents-bench slow.warm.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.step.store.get value=3.000 unit=requests/op
agents-bench slow.warm.step.store.put value=1.125 unit=requests/op
agents-bench slow.warm.step.store.cas value=0.000 unit=requests/op
agents-bench slow.warm.step.store.list value=0.000 unit=requests/op
agents-bench slow.warm.step.store.conflict value=1.000 unit=requests/op
agents-bench slow.warm.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench slow.warm.step.store.body_out_bytes value=11.250 unit=bytes/op kind=object-body
agents-bench slow.warm.step.publish.get value=0.000 unit=requests/op
agents-bench slow.warm.step.publish.put value=0.000 unit=requests/op
agents-bench slow.warm.step.publish.cas value=5.000 unit=requests/op
agents-bench slow.warm.step.publish.list value=0.000 unit=requests/op
agents-bench slow.warm.step.publish.conflict value=0.000 unit=requests/op
agents-bench slow.warm.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.step.publish.body_out_bytes value=8458.625 unit=bytes/op kind=object-body
agents-bench slow.warm.step.index.page_gets value=0.000 unit=requests/op
agents-bench slow.warm.step.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.warm.step.other.delete value=0.000 unit=requests/op
agents-bench slow.warm.step.latency.p50_ms value=3160.000 unit=ms
agents-bench slow.warm.step.latency.p99_ms value=3659.000 unit=ms
agents-bench slow.warm.step.latency.max_ms value=3659.000 unit=ms
agents-bench slow.warm.turn.ops.count value=8.000 unit=ops
agents-bench slow.warm.turn.resolve.get value=2.125 unit=requests/op
agents-bench slow.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench slow.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench slow.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench slow.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.warm.turn.resolve.body_in_bytes value=3294.000 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.store.get value=1.875 unit=requests/op
agents-bench slow.warm.turn.store.put value=2.000 unit=requests/op
agents-bench slow.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench slow.warm.turn.store.list value=0.000 unit=requests/op
agents-bench slow.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench slow.warm.turn.store.body_in_bytes value=28.250 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.store.body_out_bytes value=46.500 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench slow.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench slow.warm.turn.publish.cas value=2.125 unit=requests/op
agents-bench slow.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench slow.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench slow.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.publish.body_out_bytes value=3164.000 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench slow.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench slow.warm.turn.latency.p50_ms value=1716.000 unit=ms
agents-bench slow.warm.turn.latency.p99_ms value=1965.000 unit=ms
agents-bench slow.warm.turn.latency.max_ms value=1965.000 unit=ms
agents-bench slow.warm.run.heap.peak_bytes value=407953.000 unit=bytes kind=heap-live-delta
agents-bench slow.cold.activation.ops.count value=2.000 unit=ops
agents-bench slow.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench slow.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench slow.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench slow.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench slow.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.cold.activation.resolve.body_in_bytes value=1249.000 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.store.get value=0.000 unit=requests/op
agents-bench slow.cold.activation.store.put value=1.000 unit=requests/op
agents-bench slow.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench slow.cold.activation.store.list value=0.000 unit=requests/op
agents-bench slow.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench slow.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.store.body_out_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench slow.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench slow.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench slow.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench slow.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench slow.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.publish.body_out_bytes value=788.500 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench slow.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench slow.cold.activation.latency.p50_ms value=802.000 unit=ms
agents-bench slow.cold.activation.latency.p99_ms value=1039.000 unit=ms
agents-bench slow.cold.activation.latency.max_ms value=1039.000 unit=ms
agents-bench slow.cold.message.ops.count value=8.000 unit=ops
agents-bench slow.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench slow.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench slow.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench slow.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench slow.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.cold.message.resolve.body_in_bytes value=1651.000 unit=bytes/op kind=object-body
agents-bench slow.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.message.store.get value=0.750 unit=requests/op
agents-bench slow.cold.message.store.put value=1.750 unit=requests/op
agents-bench slow.cold.message.store.cas value=0.000 unit=requests/op
agents-bench slow.cold.message.store.list value=0.000 unit=requests/op
agents-bench slow.cold.message.store.conflict value=0.750 unit=requests/op
agents-bench slow.cold.message.store.body_in_bytes value=14.250 unit=bytes/op kind=object-body
agents-bench slow.cold.message.store.body_out_bytes value=33.750 unit=bytes/op kind=object-body
agents-bench slow.cold.message.publish.get value=0.000 unit=requests/op
agents-bench slow.cold.message.publish.put value=0.000 unit=requests/op
agents-bench slow.cold.message.publish.cas value=1.750 unit=requests/op
agents-bench slow.cold.message.publish.list value=0.000 unit=requests/op
agents-bench slow.cold.message.publish.conflict value=0.750 unit=requests/op
agents-bench slow.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.message.publish.body_out_bytes value=1769.875 unit=bytes/op kind=object-body
agents-bench slow.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench slow.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.cold.message.other.delete value=0.000 unit=requests/op
agents-bench slow.cold.message.latency.p50_ms value=1039.000 unit=ms
agents-bench slow.cold.message.latency.p99_ms value=2817.000 unit=ms
agents-bench slow.cold.message.latency.max_ms value=2817.000 unit=ms
agents-bench slow.cold.step.ops.count value=8.000 unit=ops
agents-bench slow.cold.step.resolve.get value=6.000 unit=requests/op
agents-bench slow.cold.step.resolve.put value=0.000 unit=requests/op
agents-bench slow.cold.step.resolve.cas value=0.000 unit=requests/op
agents-bench slow.cold.step.resolve.list value=0.000 unit=requests/op
agents-bench slow.cold.step.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.cold.step.resolve.body_in_bytes value=9945.000 unit=bytes/op kind=object-body
agents-bench slow.cold.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.step.store.get value=3.000 unit=requests/op
agents-bench slow.cold.step.store.put value=1.125 unit=requests/op
agents-bench slow.cold.step.store.cas value=0.000 unit=requests/op
agents-bench slow.cold.step.store.list value=0.000 unit=requests/op
agents-bench slow.cold.step.store.conflict value=1.000 unit=requests/op
agents-bench slow.cold.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench slow.cold.step.store.body_out_bytes value=11.250 unit=bytes/op kind=object-body
agents-bench slow.cold.step.publish.get value=0.000 unit=requests/op
agents-bench slow.cold.step.publish.put value=0.000 unit=requests/op
agents-bench slow.cold.step.publish.cas value=5.000 unit=requests/op
agents-bench slow.cold.step.publish.list value=0.000 unit=requests/op
agents-bench slow.cold.step.publish.conflict value=0.000 unit=requests/op
agents-bench slow.cold.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.step.publish.body_out_bytes value=8458.625 unit=bytes/op kind=object-body
agents-bench slow.cold.step.index.page_gets value=0.000 unit=requests/op
agents-bench slow.cold.step.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.cold.step.other.delete value=0.000 unit=requests/op
agents-bench slow.cold.step.latency.p50_ms value=3109.000 unit=ms
agents-bench slow.cold.step.latency.p99_ms value=3659.000 unit=ms
agents-bench slow.cold.step.latency.max_ms value=3659.000 unit=ms
agents-bench slow.cold.turn.ops.count value=8.000 unit=ops
agents-bench slow.cold.turn.resolve.get value=2.250 unit=requests/op
agents-bench slow.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench slow.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench slow.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench slow.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.cold.turn.resolve.body_in_bytes value=3520.500 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.store.get value=1.875 unit=requests/op
agents-bench slow.cold.turn.store.put value=2.000 unit=requests/op
agents-bench slow.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench slow.cold.turn.store.list value=0.000 unit=requests/op
agents-bench slow.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench slow.cold.turn.store.body_in_bytes value=28.250 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.store.body_out_bytes value=46.500 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench slow.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench slow.cold.turn.publish.cas value=2.250 unit=requests/op
agents-bench slow.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench slow.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench slow.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.publish.body_out_bytes value=3390.500 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench slow.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench slow.cold.turn.latency.p50_ms value=1755.000 unit=ms
agents-bench slow.cold.turn.latency.p99_ms value=2271.000 unit=ms
agents-bench slow.cold.turn.latency.max_ms value=2271.000 unit=ms
agents-bench slow.cold.run.heap.peak_bytes value=412413.000 unit=bytes kind=heap-live-delta
agents-bench baseline.na.ingest.ops.count value=8.000 unit=ops
agents-bench baseline.na.ingest.telemetry.put value=1.000 unit=requests/op
agents-bench baseline.na.ingest.telemetry.get value=1.000 unit=requests/op
agents-bench baseline.na.ingest.telemetry.body_in_bytes value=7.000 unit=bytes/op kind=object-body
agents-bench baseline.na.ingest.telemetry.body_out_bytes value=7.000 unit=bytes/op kind=object-body
agents-bench baseline.na.ingest.latency.p50_ms value=30.000 unit=ms
agents-bench baseline.na.ingest.latency.p99_ms value=30.000 unit=ms
agents-bench baseline.na.ingest.latency.max_ms value=30.000 unit=ms
agents-bench baseline.na.run.isolation.agents_requests value=0.000 unit=requests
agents-bench baseline.na.run.heap.peak_bytes value=21393.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n100-start.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n100-start.cold.assign.resolve.get value=66.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.list value=2.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.body_in_bytes value=5282.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n100-start.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.latency.delay_ms value=740.000 unit=ms
agents-bench discovery-n100-start.cold.assign.sweep.passes value=1.000 unit=passes
agents-bench discovery-n100-start.cold.run.heap.peak_bytes value=527091.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n100-middle.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n100-middle.cold.assign.resolve.get value=66.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.list value=2.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.body_in_bytes value=5282.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n100-middle.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.latency.delay_ms value=740.000 unit=ms
agents-bench discovery-n100-middle.cold.assign.sweep.passes value=1.000 unit=passes
agents-bench discovery-n100-middle.cold.run.heap.peak_bytes value=525755.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n100-end.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n100-end.cold.assign.resolve.get value=102.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.list value=4.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.body_in_bytes value=7874.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n100-end.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.latency.delay_ms value=5460.000 unit=ms
agents-bench discovery-n100-end.cold.assign.sweep.passes value=2.000 unit=passes
agents-bench discovery-n100-end.cold.run.heap.peak_bytes value=520702.000 unit=bytes kind=heap-live-delta
agents-bench-usd baseline.na.ingest usd_per_million_ops=5.4000 label=assumption date=2026-10-29
agents-bench-usd discovery-n100-end.cold.assign usd_per_million_ops=71.2000 label=assumption date=2026-10-29
agents-bench-usd discovery-n100-middle.cold.assign usd_per_million_ops=46.8000 label=assumption date=2026-10-29
agents-bench-usd discovery-n100-start.cold.assign usd_per_million_ops=46.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.cold.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.cold.message usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.cold.step usd_per_million_ops=36.1000 label=assumption date=2026-10-29
agents-bench-usd dormant.cold.turn usd_per_million_ops=26.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.warm.message usd_per_million_ops=10.6000 label=assumption date=2026-10-29
agents-bench-usd dormant.warm.step usd_per_million_ops=36.1000 label=assumption date=2026-10-29
agents-bench-usd dormant.warm.turn usd_per_million_ops=24.1000 label=assumption date=2026-10-29
agents-bench-usd hot.cold.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd hot.cold.message usd_per_million_ops=18.6000 label=assumption date=2026-10-29
agents-bench-usd hot.cold.step usd_per_million_ops=34.2250 label=assumption date=2026-10-29
agents-bench-usd hot.cold.turn usd_per_million_ops=22.9000 label=assumption date=2026-10-29
agents-bench-usd hot.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd hot.warm.message usd_per_million_ops=18.5500 label=assumption date=2026-10-29
agents-bench-usd hot.warm.step usd_per_million_ops=34.2250 label=assumption date=2026-10-29
agents-bench-usd hot.warm.turn usd_per_million_ops=22.2250 label=assumption date=2026-10-29
agents-bench-usd index-p2.cold.activation usd_per_million_ops=12.8850 label=assumption date=2026-10-29
agents-bench-usd index-p2.cold.message usd_per_million_ops=12.8850 label=assumption date=2026-10-29
agents-bench-usd index-p2.cold.turn usd_per_million_ops=29.0518 label=assumption date=2026-10-29
agents-bench-usd index-p2.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd index-p2.warm.message usd_per_million_ops=12.5182 label=assumption date=2026-10-29
agents-bench-usd index-p2.warm.turn usd_per_million_ops=24.1000 label=assumption date=2026-10-29
agents-bench-usd index-p64.cold.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd index-p64.cold.message usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd index-p64.cold.turn usd_per_million_ops=26.9668 label=assumption date=2026-10-29
agents-bench-usd index-p64.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd index-p64.warm.message usd_per_million_ops=10.4332 label=assumption date=2026-10-29
agents-bench-usd index-p64.warm.turn usd_per_million_ops=22.0150 label=assumption date=2026-10-29
agents-bench-usd many.cold.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd many.cold.message usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd many.cold.step usd_per_million_ops=36.1000 label=assumption date=2026-10-29
agents-bench-usd many.cold.turn usd_per_million_ops=26.8000 label=assumption date=2026-10-29
agents-bench-usd many.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd many.warm.message usd_per_million_ops=10.6000 label=assumption date=2026-10-29
agents-bench-usd many.warm.step usd_per_million_ops=36.1000 label=assumption date=2026-10-29
agents-bench-usd many.warm.turn usd_per_million_ops=24.1000 label=assumption date=2026-10-29
agents-bench-usd slow.cold.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd slow.cold.message usd_per_million_ops=18.6000 label=assumption date=2026-10-29
agents-bench-usd slow.cold.step usd_per_million_ops=34.2250 label=assumption date=2026-10-29
agents-bench-usd slow.cold.turn usd_per_million_ops=22.9000 label=assumption date=2026-10-29
agents-bench-usd slow.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd slow.warm.message usd_per_million_ops=18.5500 label=assumption date=2026-10-29
agents-bench-usd slow.warm.step usd_per_million_ops=34.2250 label=assumption date=2026-10-29
agents-bench-usd slow.warm.turn usd_per_million_ops=22.2250 label=assumption date=2026-10-29
agents-bench-usd storm.cold.activation usd_per_million_ops=6.2000 label=assumption date=2026-10-29
agents-bench-usd storm.cold.ingest usd_per_million_ops=5.4000 label=assumption date=2026-10-29
agents-bench-usd storm.cold.pass usd_per_million_ops=23.2000 label=assumption date=2026-10-29
agents-bench-usd storm.cold.turn usd_per_million_ops=21.6000 label=assumption date=2026-10-29
agents-bench-summary figures=1433 failures=0
```

## Full run, verbatim

The output of `cargo bench -p ravel-agents-sim --bench agents_cost -- full`
at the revision above.

```text
agents-bench-stamp revision=3799d65dd70682d6290ada4a6a810c66ac2a9ac3 store=memory+fault seed=3361 clock=tokio-paused size=full heap=stats_alloc
agents-bench hot.warm.activation.ops.count value=1.000 unit=ops
agents-bench hot.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench hot.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench hot.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench hot.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench hot.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.warm.activation.resolve.body_in_bytes value=132.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.store.get value=0.000 unit=requests/op
agents-bench hot.warm.activation.store.put value=1.000 unit=requests/op
agents-bench hot.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench hot.warm.activation.store.list value=0.000 unit=requests/op
agents-bench hot.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench hot.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.store.body_out_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench hot.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench hot.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench hot.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench hot.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench hot.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.publish.body_out_bytes value=232.000 unit=bytes/op kind=object-body
agents-bench hot.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench hot.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench hot.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench hot.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench hot.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench hot.warm.message.ops.count value=64.000 unit=ops
agents-bench hot.warm.message.resolve.get value=1.766 unit=requests/op
agents-bench hot.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench hot.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench hot.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench hot.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.warm.message.resolve.body_in_bytes value=14275.734 unit=bytes/op kind=object-body
agents-bench hot.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.message.store.get value=0.750 unit=requests/op
agents-bench hot.warm.message.store.put value=1.750 unit=requests/op
agents-bench hot.warm.message.store.cas value=0.000 unit=requests/op
agents-bench hot.warm.message.store.list value=0.000 unit=requests/op
agents-bench hot.warm.message.store.conflict value=0.750 unit=requests/op
agents-bench hot.warm.message.store.body_in_bytes value=14.531 unit=bytes/op kind=object-body
agents-bench hot.warm.message.store.body_out_bytes value=34.406 unit=bytes/op kind=object-body
agents-bench hot.warm.message.publish.get value=0.000 unit=requests/op
agents-bench hot.warm.message.publish.put value=0.000 unit=requests/op
agents-bench hot.warm.message.publish.cas value=1.750 unit=requests/op
agents-bench hot.warm.message.publish.list value=0.000 unit=requests/op
agents-bench hot.warm.message.publish.conflict value=0.750 unit=requests/op
agents-bench hot.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.message.publish.body_out_bytes value=14550.578 unit=bytes/op kind=object-body
agents-bench hot.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench hot.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.warm.message.other.delete value=0.000 unit=requests/op
agents-bench hot.warm.message.latency.p50_ms value=60.000 unit=ms
agents-bench hot.warm.message.latency.p99_ms value=170.000 unit=ms
agents-bench hot.warm.message.latency.max_ms value=170.000 unit=ms
agents-bench hot.warm.step.ops.count value=64.000 unit=ops
agents-bench hot.warm.step.resolve.get value=6.000 unit=requests/op
agents-bench hot.warm.step.resolve.put value=0.000 unit=requests/op
agents-bench hot.warm.step.resolve.cas value=0.000 unit=requests/op
agents-bench hot.warm.step.resolve.list value=0.000 unit=requests/op
agents-bench hot.warm.step.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.warm.step.resolve.body_in_bytes value=53605.531 unit=bytes/op kind=object-body
agents-bench hot.warm.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.step.store.get value=3.000 unit=requests/op
agents-bench hot.warm.step.store.put value=1.016 unit=requests/op
agents-bench hot.warm.step.store.cas value=0.000 unit=requests/op
agents-bench hot.warm.step.store.list value=0.000 unit=requests/op
agents-bench hot.warm.step.store.conflict value=1.000 unit=requests/op
agents-bench hot.warm.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench hot.warm.step.store.body_out_bytes value=10.156 unit=bytes/op kind=object-body
agents-bench hot.warm.step.publish.get value=0.000 unit=requests/op
agents-bench hot.warm.step.publish.put value=0.000 unit=requests/op
agents-bench hot.warm.step.publish.cas value=5.000 unit=requests/op
agents-bench hot.warm.step.publish.list value=0.000 unit=requests/op
agents-bench hot.warm.step.publish.conflict value=0.000 unit=requests/op
agents-bench hot.warm.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.step.publish.body_out_bytes value=44842.297 unit=bytes/op kind=object-body
agents-bench hot.warm.step.index.page_gets value=0.000 unit=requests/op
agents-bench hot.warm.step.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.warm.step.other.delete value=0.000 unit=requests/op
agents-bench hot.warm.step.latency.p50_ms value=210.000 unit=ms
agents-bench hot.warm.step.latency.p99_ms value=230.000 unit=ms
agents-bench hot.warm.step.latency.max_ms value=230.000 unit=ms
agents-bench hot.warm.turn.ops.count value=64.000 unit=ops
agents-bench hot.warm.turn.resolve.get value=2.016 unit=requests/op
agents-bench hot.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench hot.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench hot.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench hot.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.warm.turn.resolve.body_in_bytes value=17770.500 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.store.get value=1.984 unit=requests/op
agents-bench hot.warm.turn.store.put value=2.000 unit=requests/op
agents-bench hot.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench hot.warm.turn.store.list value=0.000 unit=requests/op
agents-bench hot.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench hot.warm.turn.store.body_in_bytes value=30.563 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.store.body_out_bytes value=48.594 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench hot.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench hot.warm.turn.publish.cas value=2.016 unit=requests/op
agents-bench hot.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench hot.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench hot.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.publish.body_out_bytes value=17632.250 unit=bytes/op kind=object-body
agents-bench hot.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench hot.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench hot.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench hot.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench hot.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench hot.warm.run.heap.peak_bytes value=37298463.000 unit=bytes kind=heap-live-delta
agents-bench hot.cold.activation.ops.count value=16.000 unit=ops
agents-bench hot.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench hot.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench hot.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench hot.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench hot.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.cold.activation.resolve.body_in_bytes value=15855.375 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.store.get value=0.000 unit=requests/op
agents-bench hot.cold.activation.store.put value=1.000 unit=requests/op
agents-bench hot.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench hot.cold.activation.store.list value=0.000 unit=requests/op
agents-bench hot.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench hot.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.store.body_out_bytes value=21.375 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench hot.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench hot.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench hot.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench hot.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench hot.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.publish.body_out_bytes value=8090.688 unit=bytes/op kind=object-body
agents-bench hot.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench hot.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench hot.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench hot.cold.activation.latency.p99_ms value=60.000 unit=ms
agents-bench hot.cold.activation.latency.max_ms value=60.000 unit=ms
agents-bench hot.cold.message.ops.count value=64.000 unit=ops
agents-bench hot.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench hot.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench hot.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench hot.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench hot.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.cold.message.resolve.body_in_bytes value=16256.625 unit=bytes/op kind=object-body
agents-bench hot.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.message.store.get value=0.750 unit=requests/op
agents-bench hot.cold.message.store.put value=1.750 unit=requests/op
agents-bench hot.cold.message.store.cas value=0.000 unit=requests/op
agents-bench hot.cold.message.store.list value=0.000 unit=requests/op
agents-bench hot.cold.message.store.conflict value=0.750 unit=requests/op
agents-bench hot.cold.message.store.body_in_bytes value=14.531 unit=bytes/op kind=object-body
agents-bench hot.cold.message.store.body_out_bytes value=34.406 unit=bytes/op kind=object-body
agents-bench hot.cold.message.publish.get value=0.000 unit=requests/op
agents-bench hot.cold.message.publish.put value=0.000 unit=requests/op
agents-bench hot.cold.message.publish.cas value=1.750 unit=requests/op
agents-bench hot.cold.message.publish.list value=0.000 unit=requests/op
agents-bench hot.cold.message.publish.conflict value=0.750 unit=requests/op
agents-bench hot.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.message.publish.body_out_bytes value=14550.578 unit=bytes/op kind=object-body
agents-bench hot.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench hot.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.cold.message.other.delete value=0.000 unit=requests/op
agents-bench hot.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench hot.cold.message.latency.p99_ms value=170.000 unit=ms
agents-bench hot.cold.message.latency.max_ms value=170.000 unit=ms
agents-bench hot.cold.step.ops.count value=64.000 unit=ops
agents-bench hot.cold.step.resolve.get value=6.000 unit=requests/op
agents-bench hot.cold.step.resolve.put value=0.000 unit=requests/op
agents-bench hot.cold.step.resolve.cas value=0.000 unit=requests/op
agents-bench hot.cold.step.resolve.list value=0.000 unit=requests/op
agents-bench hot.cold.step.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.cold.step.resolve.body_in_bytes value=53605.578 unit=bytes/op kind=object-body
agents-bench hot.cold.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.step.store.get value=3.000 unit=requests/op
agents-bench hot.cold.step.store.put value=1.016 unit=requests/op
agents-bench hot.cold.step.store.cas value=0.000 unit=requests/op
agents-bench hot.cold.step.store.list value=0.000 unit=requests/op
agents-bench hot.cold.step.store.conflict value=1.000 unit=requests/op
agents-bench hot.cold.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench hot.cold.step.store.body_out_bytes value=10.156 unit=bytes/op kind=object-body
agents-bench hot.cold.step.publish.get value=0.000 unit=requests/op
agents-bench hot.cold.step.publish.put value=0.000 unit=requests/op
agents-bench hot.cold.step.publish.cas value=5.000 unit=requests/op
agents-bench hot.cold.step.publish.list value=0.000 unit=requests/op
agents-bench hot.cold.step.publish.conflict value=0.000 unit=requests/op
agents-bench hot.cold.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.step.publish.body_out_bytes value=44842.344 unit=bytes/op kind=object-body
agents-bench hot.cold.step.index.page_gets value=0.000 unit=requests/op
agents-bench hot.cold.step.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.cold.step.other.delete value=0.000 unit=requests/op
agents-bench hot.cold.step.latency.p50_ms value=210.000 unit=ms
agents-bench hot.cold.step.latency.p99_ms value=230.000 unit=ms
agents-bench hot.cold.step.latency.max_ms value=230.000 unit=ms
agents-bench hot.cold.turn.ops.count value=64.000 unit=ops
agents-bench hot.cold.turn.resolve.get value=2.250 unit=requests/op
agents-bench hot.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench hot.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench hot.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench hot.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench hot.cold.turn.resolve.body_in_bytes value=19899.797 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.store.get value=1.984 unit=requests/op
agents-bench hot.cold.turn.store.put value=2.000 unit=requests/op
agents-bench hot.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench hot.cold.turn.store.list value=0.000 unit=requests/op
agents-bench hot.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench hot.cold.turn.store.body_in_bytes value=30.563 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.store.body_out_bytes value=48.594 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench hot.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench hot.cold.turn.publish.cas value=2.250 unit=requests/op
agents-bench hot.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench hot.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench hot.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.publish.body_out_bytes value=19761.547 unit=bytes/op kind=object-body
agents-bench hot.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench hot.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench hot.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench hot.cold.turn.latency.p50_ms value=120.000 unit=ms
agents-bench hot.cold.turn.latency.p99_ms value=150.000 unit=ms
agents-bench hot.cold.turn.latency.max_ms value=150.000 unit=ms
agents-bench hot.cold.run.heap.peak_bytes value=38578291.000 unit=bytes kind=heap-live-delta
agents-bench many.warm.activation.ops.count value=128.000 unit=ops
agents-bench many.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench many.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench many.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench many.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench many.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench many.warm.activation.resolve.body_in_bytes value=134.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.store.get value=0.000 unit=requests/op
agents-bench many.warm.activation.store.put value=1.000 unit=requests/op
agents-bench many.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench many.warm.activation.store.list value=0.000 unit=requests/op
agents-bench many.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench many.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench many.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench many.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench many.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench many.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench many.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.publish.body_out_bytes value=223.000 unit=bytes/op kind=object-body
agents-bench many.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench many.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench many.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench many.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench many.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench many.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench many.warm.message.ops.count value=256.000 unit=ops
agents-bench many.warm.message.resolve.get value=1.500 unit=requests/op
agents-bench many.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench many.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench many.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench many.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench many.warm.message.resolve.body_in_bytes value=266.500 unit=bytes/op kind=object-body
agents-bench many.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.store.get value=0.000 unit=requests/op
agents-bench many.warm.message.store.put value=1.000 unit=requests/op
agents-bench many.warm.message.store.cas value=0.000 unit=requests/op
agents-bench many.warm.message.store.list value=0.000 unit=requests/op
agents-bench many.warm.message.store.conflict value=0.000 unit=requests/op
agents-bench many.warm.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.publish.get value=0.000 unit=requests/op
agents-bench many.warm.message.publish.put value=0.000 unit=requests/op
agents-bench many.warm.message.publish.cas value=1.000 unit=requests/op
agents-bench many.warm.message.publish.list value=0.000 unit=requests/op
agents-bench many.warm.message.publish.conflict value=0.000 unit=requests/op
agents-bench many.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.publish.body_out_bytes value=387.000 unit=bytes/op kind=object-body
agents-bench many.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench many.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench many.warm.message.other.delete value=0.000 unit=requests/op
agents-bench many.warm.message.latency.p50_ms value=50.000 unit=ms
agents-bench many.warm.message.latency.p99_ms value=60.000 unit=ms
agents-bench many.warm.message.latency.max_ms value=60.000 unit=ms
agents-bench many.warm.step.ops.count value=256.000 unit=ops
agents-bench many.warm.step.resolve.get value=6.000 unit=requests/op
agents-bench many.warm.step.resolve.put value=0.000 unit=requests/op
agents-bench many.warm.step.resolve.cas value=0.000 unit=requests/op
agents-bench many.warm.step.resolve.list value=0.000 unit=requests/op
agents-bench many.warm.step.resolve.conflict value=0.000 unit=requests/op
agents-bench many.warm.step.resolve.body_in_bytes value=3714.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.store.get value=3.000 unit=requests/op
agents-bench many.warm.step.store.put value=1.500 unit=requests/op
agents-bench many.warm.step.store.cas value=0.000 unit=requests/op
agents-bench many.warm.step.store.list value=0.000 unit=requests/op
agents-bench many.warm.step.store.conflict value=1.000 unit=requests/op
agents-bench many.warm.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.store.body_out_bytes value=15.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.publish.get value=0.000 unit=requests/op
agents-bench many.warm.step.publish.put value=0.000 unit=requests/op
agents-bench many.warm.step.publish.cas value=5.000 unit=requests/op
agents-bench many.warm.step.publish.list value=0.000 unit=requests/op
agents-bench many.warm.step.publish.conflict value=0.000 unit=requests/op
agents-bench many.warm.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.publish.body_out_bytes value=3266.000 unit=bytes/op kind=object-body
agents-bench many.warm.step.index.page_gets value=0.000 unit=requests/op
agents-bench many.warm.step.index.planner_misses value=0.000 unit=misses/op
agents-bench many.warm.step.other.delete value=0.000 unit=requests/op
agents-bench many.warm.step.latency.p50_ms value=210.000 unit=ms
agents-bench many.warm.step.latency.p99_ms value=230.000 unit=ms
agents-bench many.warm.step.latency.max_ms value=230.000 unit=ms
agents-bench many.warm.turn.ops.count value=256.000 unit=ops
agents-bench many.warm.turn.resolve.get value=2.500 unit=requests/op
agents-bench many.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench many.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench many.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench many.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench many.warm.turn.resolve.body_in_bytes value=1241.500 unit=bytes/op kind=object-body
agents-bench many.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.turn.store.get value=1.500 unit=requests/op
agents-bench many.warm.turn.store.put value=2.000 unit=requests/op
agents-bench many.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench many.warm.turn.store.list value=0.000 unit=requests/op
agents-bench many.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench many.warm.turn.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench many.warm.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench many.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench many.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench many.warm.turn.publish.cas value=2.500 unit=requests/op
agents-bench many.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench many.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench many.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.warm.turn.publish.body_out_bytes value=1140.500 unit=bytes/op kind=object-body
agents-bench many.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench many.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench many.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench many.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench many.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench many.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench many.warm.run.heap.peak_bytes value=4653623.000 unit=bytes kind=heap-live-delta
agents-bench many.cold.activation.ops.count value=256.000 unit=ops
agents-bench many.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench many.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench many.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench many.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench many.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench many.cold.activation.resolve.body_in_bytes value=466.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.store.get value=0.000 unit=requests/op
agents-bench many.cold.activation.store.put value=1.000 unit=requests/op
agents-bench many.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench many.cold.activation.store.list value=0.000 unit=requests/op
agents-bench many.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench many.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench many.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench many.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench many.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench many.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench many.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.publish.body_out_bytes value=387.000 unit=bytes/op kind=object-body
agents-bench many.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench many.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench many.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench many.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench many.cold.activation.latency.p99_ms value=60.000 unit=ms
agents-bench many.cold.activation.latency.max_ms value=60.000 unit=ms
agents-bench many.cold.message.ops.count value=256.000 unit=ops
agents-bench many.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench many.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench many.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench many.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench many.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench many.cold.message.resolve.body_in_bytes value=466.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.store.get value=0.000 unit=requests/op
agents-bench many.cold.message.store.put value=1.000 unit=requests/op
agents-bench many.cold.message.store.cas value=0.000 unit=requests/op
agents-bench many.cold.message.store.list value=0.000 unit=requests/op
agents-bench many.cold.message.store.conflict value=0.000 unit=requests/op
agents-bench many.cold.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.publish.get value=0.000 unit=requests/op
agents-bench many.cold.message.publish.put value=0.000 unit=requests/op
agents-bench many.cold.message.publish.cas value=1.000 unit=requests/op
agents-bench many.cold.message.publish.list value=0.000 unit=requests/op
agents-bench many.cold.message.publish.conflict value=0.000 unit=requests/op
agents-bench many.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.publish.body_out_bytes value=387.000 unit=bytes/op kind=object-body
agents-bench many.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench many.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench many.cold.message.other.delete value=0.000 unit=requests/op
agents-bench many.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench many.cold.message.latency.p99_ms value=60.000 unit=ms
agents-bench many.cold.message.latency.max_ms value=60.000 unit=ms
agents-bench many.cold.step.ops.count value=256.000 unit=ops
agents-bench many.cold.step.resolve.get value=6.000 unit=requests/op
agents-bench many.cold.step.resolve.put value=0.000 unit=requests/op
agents-bench many.cold.step.resolve.cas value=0.000 unit=requests/op
agents-bench many.cold.step.resolve.list value=0.000 unit=requests/op
agents-bench many.cold.step.resolve.conflict value=0.000 unit=requests/op
agents-bench many.cold.step.resolve.body_in_bytes value=3714.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.store.get value=3.000 unit=requests/op
agents-bench many.cold.step.store.put value=1.500 unit=requests/op
agents-bench many.cold.step.store.cas value=0.000 unit=requests/op
agents-bench many.cold.step.store.list value=0.000 unit=requests/op
agents-bench many.cold.step.store.conflict value=1.000 unit=requests/op
agents-bench many.cold.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.store.body_out_bytes value=15.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.publish.get value=0.000 unit=requests/op
agents-bench many.cold.step.publish.put value=0.000 unit=requests/op
agents-bench many.cold.step.publish.cas value=5.000 unit=requests/op
agents-bench many.cold.step.publish.list value=0.000 unit=requests/op
agents-bench many.cold.step.publish.conflict value=0.000 unit=requests/op
agents-bench many.cold.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.publish.body_out_bytes value=3266.000 unit=bytes/op kind=object-body
agents-bench many.cold.step.index.page_gets value=0.000 unit=requests/op
agents-bench many.cold.step.index.planner_misses value=0.000 unit=misses/op
agents-bench many.cold.step.other.delete value=0.000 unit=requests/op
agents-bench many.cold.step.latency.p50_ms value=210.000 unit=ms
agents-bench many.cold.step.latency.p99_ms value=230.000 unit=ms
agents-bench many.cold.step.latency.max_ms value=230.000 unit=ms
agents-bench many.cold.turn.ops.count value=256.000 unit=ops
agents-bench many.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench many.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench many.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench many.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench many.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench many.cold.turn.resolve.body_in_bytes value=1517.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.store.get value=1.500 unit=requests/op
agents-bench many.cold.turn.store.put value=2.000 unit=requests/op
agents-bench many.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench many.cold.turn.store.list value=0.000 unit=requests/op
agents-bench many.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench many.cold.turn.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench many.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench many.cold.turn.publish.cas value=3.000 unit=requests/op
agents-bench many.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench many.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench many.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.publish.body_out_bytes value=1416.000 unit=bytes/op kind=object-body
agents-bench many.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench many.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench many.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench many.cold.turn.latency.p50_ms value=140.000 unit=ms
agents-bench many.cold.turn.latency.p99_ms value=150.000 unit=ms
agents-bench many.cold.turn.latency.max_ms value=150.000 unit=ms
agents-bench many.cold.run.heap.peak_bytes value=4419437.000 unit=bytes kind=heap-live-delta
agents-bench dormant.warm.activation.ops.count value=16.000 unit=ops
agents-bench dormant.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.activation.resolve.body_in_bytes value=140.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.store.get value=0.000 unit=requests/op
agents-bench dormant.warm.activation.store.put value=1.000 unit=requests/op
agents-bench dormant.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench dormant.warm.activation.store.list value=0.000 unit=requests/op
agents-bench dormant.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench dormant.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench dormant.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench dormant.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench dormant.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.publish.body_out_bytes value=226.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench dormant.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench dormant.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench dormant.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench dormant.warm.message.ops.count value=32.000 unit=ops
agents-bench dormant.warm.message.resolve.get value=1.500 unit=requests/op
agents-bench dormant.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench dormant.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench dormant.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.message.resolve.body_in_bytes value=271.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.store.get value=0.000 unit=requests/op
agents-bench dormant.warm.message.store.put value=1.000 unit=requests/op
agents-bench dormant.warm.message.store.cas value=0.000 unit=requests/op
agents-bench dormant.warm.message.store.list value=0.000 unit=requests/op
agents-bench dormant.warm.message.store.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.publish.get value=0.000 unit=requests/op
agents-bench dormant.warm.message.publish.put value=0.000 unit=requests/op
agents-bench dormant.warm.message.publish.cas value=1.000 unit=requests/op
agents-bench dormant.warm.message.publish.list value=0.000 unit=requests/op
agents-bench dormant.warm.message.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.publish.body_out_bytes value=390.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.warm.message.other.delete value=0.000 unit=requests/op
agents-bench dormant.warm.message.latency.p50_ms value=50.000 unit=ms
agents-bench dormant.warm.message.latency.p99_ms value=60.000 unit=ms
agents-bench dormant.warm.message.latency.max_ms value=60.000 unit=ms
agents-bench dormant.warm.step.ops.count value=32.000 unit=ops
agents-bench dormant.warm.step.resolve.get value=6.000 unit=requests/op
agents-bench dormant.warm.step.resolve.put value=0.000 unit=requests/op
agents-bench dormant.warm.step.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.warm.step.resolve.list value=0.000 unit=requests/op
agents-bench dormant.warm.step.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.step.resolve.body_in_bytes value=3732.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.store.get value=3.000 unit=requests/op
agents-bench dormant.warm.step.store.put value=1.500 unit=requests/op
agents-bench dormant.warm.step.store.cas value=0.000 unit=requests/op
agents-bench dormant.warm.step.store.list value=0.000 unit=requests/op
agents-bench dormant.warm.step.store.conflict value=1.000 unit=requests/op
agents-bench dormant.warm.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.store.body_out_bytes value=15.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.publish.get value=0.000 unit=requests/op
agents-bench dormant.warm.step.publish.put value=0.000 unit=requests/op
agents-bench dormant.warm.step.publish.cas value=5.000 unit=requests/op
agents-bench dormant.warm.step.publish.list value=0.000 unit=requests/op
agents-bench dormant.warm.step.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.publish.body_out_bytes value=3281.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.step.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.warm.step.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.warm.step.other.delete value=0.000 unit=requests/op
agents-bench dormant.warm.step.latency.p50_ms value=210.000 unit=ms
agents-bench dormant.warm.step.latency.p99_ms value=230.000 unit=ms
agents-bench dormant.warm.step.latency.max_ms value=230.000 unit=ms
agents-bench dormant.warm.turn.ops.count value=32.000 unit=ops
agents-bench dormant.warm.turn.resolve.get value=2.500 unit=requests/op
agents-bench dormant.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench dormant.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench dormant.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.turn.resolve.body_in_bytes value=1249.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.store.get value=1.500 unit=requests/op
agents-bench dormant.warm.turn.store.put value=2.000 unit=requests/op
agents-bench dormant.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench dormant.warm.turn.store.list value=0.000 unit=requests/op
agents-bench dormant.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.turn.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench dormant.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench dormant.warm.turn.publish.cas value=2.500 unit=requests/op
agents-bench dormant.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench dormant.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.publish.body_out_bytes value=1148.000 unit=bytes/op kind=object-body
agents-bench dormant.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench dormant.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench dormant.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench dormant.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench dormant.warm.run.isolation.dormant_requests value=0.000 unit=requests
agents-bench dormant.warm.run.heap.peak_bytes value=22671974.000 unit=bytes kind=heap-live-delta
agents-bench dormant.cold.activation.ops.count value=32.000 unit=ops
agents-bench dormant.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.activation.resolve.body_in_bytes value=472.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.store.get value=0.000 unit=requests/op
agents-bench dormant.cold.activation.store.put value=1.000 unit=requests/op
agents-bench dormant.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench dormant.cold.activation.store.list value=0.000 unit=requests/op
agents-bench dormant.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench dormant.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench dormant.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench dormant.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench dormant.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.publish.body_out_bytes value=390.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench dormant.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench dormant.cold.activation.latency.p99_ms value=60.000 unit=ms
agents-bench dormant.cold.activation.latency.max_ms value=60.000 unit=ms
agents-bench dormant.cold.message.ops.count value=32.000 unit=ops
agents-bench dormant.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench dormant.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench dormant.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench dormant.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.message.resolve.body_in_bytes value=472.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.store.get value=0.000 unit=requests/op
agents-bench dormant.cold.message.store.put value=1.000 unit=requests/op
agents-bench dormant.cold.message.store.cas value=0.000 unit=requests/op
agents-bench dormant.cold.message.store.list value=0.000 unit=requests/op
agents-bench dormant.cold.message.store.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.publish.get value=0.000 unit=requests/op
agents-bench dormant.cold.message.publish.put value=0.000 unit=requests/op
agents-bench dormant.cold.message.publish.cas value=1.000 unit=requests/op
agents-bench dormant.cold.message.publish.list value=0.000 unit=requests/op
agents-bench dormant.cold.message.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.publish.body_out_bytes value=390.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.cold.message.other.delete value=0.000 unit=requests/op
agents-bench dormant.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench dormant.cold.message.latency.p99_ms value=60.000 unit=ms
agents-bench dormant.cold.message.latency.max_ms value=60.000 unit=ms
agents-bench dormant.cold.step.ops.count value=32.000 unit=ops
agents-bench dormant.cold.step.resolve.get value=6.000 unit=requests/op
agents-bench dormant.cold.step.resolve.put value=0.000 unit=requests/op
agents-bench dormant.cold.step.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.cold.step.resolve.list value=0.000 unit=requests/op
agents-bench dormant.cold.step.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.step.resolve.body_in_bytes value=3732.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.store.get value=3.000 unit=requests/op
agents-bench dormant.cold.step.store.put value=1.500 unit=requests/op
agents-bench dormant.cold.step.store.cas value=0.000 unit=requests/op
agents-bench dormant.cold.step.store.list value=0.000 unit=requests/op
agents-bench dormant.cold.step.store.conflict value=1.000 unit=requests/op
agents-bench dormant.cold.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.store.body_out_bytes value=15.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.publish.get value=0.000 unit=requests/op
agents-bench dormant.cold.step.publish.put value=0.000 unit=requests/op
agents-bench dormant.cold.step.publish.cas value=5.000 unit=requests/op
agents-bench dormant.cold.step.publish.list value=0.000 unit=requests/op
agents-bench dormant.cold.step.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.publish.body_out_bytes value=3281.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.step.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.cold.step.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.cold.step.other.delete value=0.000 unit=requests/op
agents-bench dormant.cold.step.latency.p50_ms value=210.000 unit=ms
agents-bench dormant.cold.step.latency.p99_ms value=230.000 unit=ms
agents-bench dormant.cold.step.latency.max_ms value=230.000 unit=ms
agents-bench dormant.cold.turn.ops.count value=32.000 unit=ops
agents-bench dormant.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.turn.resolve.body_in_bytes value=1526.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.store.get value=1.500 unit=requests/op
agents-bench dormant.cold.turn.store.put value=2.000 unit=requests/op
agents-bench dormant.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench dormant.cold.turn.store.list value=0.000 unit=requests/op
agents-bench dormant.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.turn.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench dormant.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench dormant.cold.turn.publish.cas value=3.000 unit=requests/op
agents-bench dormant.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench dormant.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench dormant.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.publish.body_out_bytes value=1425.000 unit=bytes/op kind=object-body
agents-bench dormant.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench dormant.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench dormant.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench dormant.cold.turn.latency.p50_ms value=140.000 unit=ms
agents-bench dormant.cold.turn.latency.p99_ms value=150.000 unit=ms
agents-bench dormant.cold.turn.latency.max_ms value=150.000 unit=ms
agents-bench dormant.cold.run.isolation.dormant_requests value=0.000 unit=requests
agents-bench dormant.cold.run.heap.peak_bytes value=22648079.000 unit=bytes kind=heap-live-delta
agents-bench index-p64.warm.activation.ops.count value=1.000 unit=ops
agents-bench index-p64.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.resolve.body_in_bytes value=144.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.store.get value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.store.put value=1.000 unit=requests/op
agents-bench index-p64.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.store.list value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.publish.body_out_bytes value=228.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench index-p64.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench index-p64.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench index-p64.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench index-p64.warm.message.ops.count value=256.000 unit=ops
agents-bench index-p64.warm.message.resolve.get value=1.004 unit=requests/op
agents-bench index-p64.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.message.resolve.body_in_bytes value=5123.316 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.store.get value=0.000 unit=requests/op
agents-bench index-p64.warm.message.store.put value=1.012 unit=requests/op
agents-bench index-p64.warm.message.store.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.message.store.list value=0.000 unit=requests/op
agents-bench index-p64.warm.message.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.store.body_out_bytes value=55.738 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.publish.get value=0.000 unit=requests/op
agents-bench index-p64.warm.message.publish.put value=0.000 unit=requests/op
agents-bench index-p64.warm.message.publish.cas value=1.000 unit=requests/op
agents-bench index-p64.warm.message.publish.list value=0.000 unit=requests/op
agents-bench index-p64.warm.message.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.publish.body_out_bytes value=5243.996 unit=bytes/op kind=object-body
agents-bench index-p64.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.warm.message.other.delete value=0.000 unit=requests/op
agents-bench index-p64.warm.message.latency.p50_ms value=50.000 unit=ms
agents-bench index-p64.warm.message.latency.p99_ms value=70.000 unit=ms
agents-bench index-p64.warm.message.latency.max_ms value=70.000 unit=ms
agents-bench index-p64.warm.turn.ops.count value=256.000 unit=ops
agents-bench index-p64.warm.turn.resolve.get value=2.004 unit=requests/op
agents-bench index-p64.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.resolve.body_in_bytes value=10574.129 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.store.get value=1.996 unit=requests/op
agents-bench index-p64.warm.turn.store.put value=2.012 unit=requests/op
agents-bench index-p64.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.store.list value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.store.body_in_bytes value=29.102 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.store.body_out_bytes value=102.074 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.publish.cas value=2.004 unit=requests/op
agents-bench index-p64.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.publish.body_out_bytes value=10492.496 unit=bytes/op kind=object-body
agents-bench index-p64.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench index-p64.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench index-p64.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench index-p64.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench index-p64.warm.run.heap.peak_bytes value=36999927.000 unit=bytes kind=heap-live-delta
agents-bench index-p64.cold.activation.ops.count value=256.000 unit=ops
agents-bench index-p64.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.resolve.body_in_bytes value=10247.148 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.store.get value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.store.put value=1.012 unit=requests/op
agents-bench index-p64.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.store.list value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.store.body_out_bytes value=55.738 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.publish.body_out_bytes value=5244.535 unit=bytes/op kind=object-body
agents-bench index-p64.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench index-p64.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench index-p64.cold.activation.latency.p99_ms value=80.000 unit=ms
agents-bench index-p64.cold.activation.latency.max_ms value=80.000 unit=ms
agents-bench index-p64.cold.message.ops.count value=256.000 unit=ops
agents-bench index-p64.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.message.resolve.body_in_bytes value=10247.148 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.store.get value=0.000 unit=requests/op
agents-bench index-p64.cold.message.store.put value=1.012 unit=requests/op
agents-bench index-p64.cold.message.store.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.message.store.list value=0.000 unit=requests/op
agents-bench index-p64.cold.message.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.store.body_out_bytes value=55.738 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.publish.get value=0.000 unit=requests/op
agents-bench index-p64.cold.message.publish.put value=0.000 unit=requests/op
agents-bench index-p64.cold.message.publish.cas value=1.000 unit=requests/op
agents-bench index-p64.cold.message.publish.list value=0.000 unit=requests/op
agents-bench index-p64.cold.message.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.publish.body_out_bytes value=5244.535 unit=bytes/op kind=object-body
agents-bench index-p64.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.cold.message.other.delete value=0.000 unit=requests/op
agents-bench index-p64.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench index-p64.cold.message.latency.p99_ms value=80.000 unit=ms
agents-bench index-p64.cold.message.latency.max_ms value=80.000 unit=ms
agents-bench index-p64.cold.turn.ops.count value=256.000 unit=ops
agents-bench index-p64.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.resolve.body_in_bytes value=15818.863 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.store.get value=1.996 unit=requests/op
agents-bench index-p64.cold.turn.store.put value=2.012 unit=requests/op
agents-bench index-p64.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.store.list value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.store.body_in_bytes value=29.102 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.store.body_out_bytes value=102.074 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.cas value=3.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.publish.body_out_bytes value=15737.234 unit=bytes/op kind=object-body
agents-bench index-p64.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p64.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench index-p64.cold.turn.latency.p50_ms value=150.000 unit=ms
agents-bench index-p64.cold.turn.latency.p99_ms value=170.000 unit=ms
agents-bench index-p64.cold.turn.latency.max_ms value=170.000 unit=ms
agents-bench index-p64.cold.run.heap.peak_bytes value=50933037.000 unit=bytes kind=heap-live-delta
agents-bench index-p2.warm.activation.ops.count value=1.000 unit=ops
agents-bench index-p2.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.resolve.body_in_bytes value=142.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.store.get value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.store.put value=1.000 unit=requests/op
agents-bench index-p2.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.store.list value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.store.body_out_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.publish.body_out_bytes value=227.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench index-p2.warm.activation.latency.p50_ms value=60.000 unit=ms
agents-bench index-p2.warm.activation.latency.p99_ms value=60.000 unit=ms
agents-bench index-p2.warm.activation.latency.max_ms value=60.000 unit=ms
agents-bench index-p2.warm.message.ops.count value=256.000 unit=ops
agents-bench index-p2.warm.message.resolve.get value=1.004 unit=requests/op
agents-bench index-p2.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.message.resolve.body_in_bytes value=7378.891 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.store.get value=0.000 unit=requests/op
agents-bench index-p2.warm.message.store.put value=1.496 unit=requests/op
agents-bench index-p2.warm.message.store.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.message.store.list value=0.000 unit=requests/op
agents-bench index-p2.warm.message.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.store.body_out_bytes value=72.207 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.publish.get value=0.000 unit=requests/op
agents-bench index-p2.warm.message.publish.put value=0.000 unit=requests/op
agents-bench index-p2.warm.message.publish.cas value=1.000 unit=requests/op
agents-bench index-p2.warm.message.publish.list value=0.000 unit=requests/op
agents-bench index-p2.warm.message.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.publish.body_out_bytes value=7510.195 unit=bytes/op kind=object-body
agents-bench index-p2.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.warm.message.other.delete value=0.000 unit=requests/op
agents-bench index-p2.warm.message.latency.p50_ms value=50.000 unit=ms
agents-bench index-p2.warm.message.latency.p99_ms value=70.000 unit=ms
agents-bench index-p2.warm.message.latency.max_ms value=70.000 unit=ms
agents-bench index-p2.warm.turn.ops.count value=256.000 unit=ops
agents-bench index-p2.warm.turn.resolve.get value=2.004 unit=requests/op
agents-bench index-p2.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.resolve.body_in_bytes value=15106.523 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.store.get value=1.996 unit=requests/op
agents-bench index-p2.warm.turn.store.put value=2.496 unit=requests/op
agents-bench index-p2.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.store.list value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.store.body_in_bytes value=29.102 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.store.body_out_bytes value=123.629 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.publish.cas value=2.004 unit=requests/op
agents-bench index-p2.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.publish.body_out_bytes value=15031.879 unit=bytes/op kind=object-body
agents-bench index-p2.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench index-p2.warm.turn.latency.p50_ms value=120.000 unit=ms
agents-bench index-p2.warm.turn.latency.p99_ms value=140.000 unit=ms
agents-bench index-p2.warm.turn.latency.max_ms value=140.000 unit=ms
agents-bench index-p2.warm.run.heap.peak_bytes value=32677171.000 unit=bytes kind=heap-live-delta
agents-bench index-p2.cold.activation.ops.count value=256.000 unit=ops
agents-bench index-p2.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.resolve.body_in_bytes value=14758.305 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.store.get value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.store.put value=1.496 unit=requests/op
agents-bench index-p2.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.store.list value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.store.body_out_bytes value=72.207 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.publish.body_out_bytes value=7510.734 unit=bytes/op kind=object-body
agents-bench index-p2.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench index-p2.cold.activation.latency.p50_ms value=60.000 unit=ms
agents-bench index-p2.cold.activation.latency.p99_ms value=80.000 unit=ms
agents-bench index-p2.cold.activation.latency.max_ms value=80.000 unit=ms
agents-bench index-p2.cold.message.ops.count value=256.000 unit=ops
agents-bench index-p2.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.message.resolve.body_in_bytes value=14758.305 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.store.get value=0.000 unit=requests/op
agents-bench index-p2.cold.message.store.put value=1.496 unit=requests/op
agents-bench index-p2.cold.message.store.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.message.store.list value=0.000 unit=requests/op
agents-bench index-p2.cold.message.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.message.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.store.body_out_bytes value=72.207 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.publish.get value=0.000 unit=requests/op
agents-bench index-p2.cold.message.publish.put value=0.000 unit=requests/op
agents-bench index-p2.cold.message.publish.cas value=1.000 unit=requests/op
agents-bench index-p2.cold.message.publish.list value=0.000 unit=requests/op
agents-bench index-p2.cold.message.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.publish.body_out_bytes value=7510.734 unit=bytes/op kind=object-body
agents-bench index-p2.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.cold.message.other.delete value=0.000 unit=requests/op
agents-bench index-p2.cold.message.latency.p50_ms value=60.000 unit=ms
agents-bench index-p2.cold.message.latency.p99_ms value=80.000 unit=ms
agents-bench index-p2.cold.message.latency.max_ms value=80.000 unit=ms
agents-bench index-p2.cold.turn.ops.count value=256.000 unit=ops
agents-bench index-p2.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.resolve.body_in_bytes value=22617.461 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.store.get value=1.996 unit=requests/op
agents-bench index-p2.cold.turn.store.put value=2.496 unit=requests/op
agents-bench index-p2.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.store.list value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.store.body_in_bytes value=29.102 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.store.body_out_bytes value=123.629 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.cas value=3.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.publish.body_out_bytes value=22542.820 unit=bytes/op kind=object-body
agents-bench index-p2.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench index-p2.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench index-p2.cold.turn.latency.p50_ms value=150.000 unit=ms
agents-bench index-p2.cold.turn.latency.p99_ms value=170.000 unit=ms
agents-bench index-p2.cold.turn.latency.max_ms value=170.000 unit=ms
agents-bench index-p2.cold.run.heap.peak_bytes value=46925137.000 unit=bytes kind=heap-live-delta
agents-bench storm.cold.activation.ops.count value=24.000 unit=ops
agents-bench storm.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench storm.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench storm.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench storm.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench storm.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench storm.cold.activation.resolve.body_in_bytes value=676.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.store.get value=1.000 unit=requests/op
agents-bench storm.cold.activation.store.put value=0.000 unit=requests/op
agents-bench storm.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench storm.cold.activation.store.list value=0.000 unit=requests/op
agents-bench storm.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench storm.cold.activation.store.body_in_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench storm.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench storm.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench storm.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench storm.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench storm.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.publish.body_out_bytes value=338.000 unit=bytes/op kind=object-body
agents-bench storm.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench storm.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench storm.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench storm.cold.activation.latency.p50_ms value=50.000 unit=ms
agents-bench storm.cold.activation.latency.p99_ms value=50.000 unit=ms
agents-bench storm.cold.activation.latency.max_ms value=50.000 unit=ms
agents-bench storm.cold.ingest.ops.count value=28.000 unit=ops
agents-bench storm.cold.ingest.telemetry.put value=1.000 unit=requests/op
agents-bench storm.cold.ingest.telemetry.get value=1.000 unit=requests/op
agents-bench storm.cold.ingest.telemetry.body_in_bytes value=7.643 unit=bytes/op kind=object-body
agents-bench storm.cold.ingest.telemetry.body_out_bytes value=7.643 unit=bytes/op kind=object-body
agents-bench storm.cold.ingest.latency.p50_ms value=30.000 unit=ms
agents-bench storm.cold.ingest.latency.p99_ms value=30.000 unit=ms
agents-bench storm.cold.ingest.latency.max_ms value=30.000 unit=ms
agents-bench storm.cold.pass.ops.count value=3.000 unit=ops
agents-bench storm.cold.pass.resolve.get value=8.000 unit=requests/op
agents-bench storm.cold.pass.resolve.put value=0.000 unit=requests/op
agents-bench storm.cold.pass.resolve.cas value=0.000 unit=requests/op
agents-bench storm.cold.pass.resolve.list value=4.000 unit=requests/op
agents-bench storm.cold.pass.resolve.conflict value=0.000 unit=requests/op
agents-bench storm.cold.pass.resolve.body_in_bytes value=2704.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.store.get value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.put value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.cas value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.list value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.conflict value=0.000 unit=requests/op
agents-bench storm.cold.pass.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.publish.get value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.put value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.cas value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.list value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.conflict value=0.000 unit=requests/op
agents-bench storm.cold.pass.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.publish.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.pass.index.page_gets value=0.000 unit=requests/op
agents-bench storm.cold.pass.index.planner_misses value=0.000 unit=misses/op
agents-bench storm.cold.pass.other.delete value=0.000 unit=requests/op
agents-bench storm.cold.pass.latency.p50_ms value=140.000 unit=ms
agents-bench storm.cold.pass.latency.p99_ms value=140.000 unit=ms
agents-bench storm.cold.pass.latency.max_ms value=140.000 unit=ms
agents-bench storm.cold.turn.ops.count value=24.000 unit=ops
agents-bench storm.cold.turn.resolve.get value=3.000 unit=requests/op
agents-bench storm.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench storm.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench storm.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench storm.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench storm.cold.turn.resolve.body_in_bytes value=1014.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.store.get value=1.000 unit=requests/op
agents-bench storm.cold.turn.store.put value=2.000 unit=requests/op
agents-bench storm.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench storm.cold.turn.store.list value=0.000 unit=requests/op
agents-bench storm.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench storm.cold.turn.store.body_in_bytes value=16.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.store.body_out_bytes value=43.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench storm.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench storm.cold.turn.publish.cas value=2.000 unit=requests/op
agents-bench storm.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench storm.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench storm.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.publish.body_out_bytes value=605.000 unit=bytes/op kind=object-body
agents-bench storm.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench storm.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench storm.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench storm.cold.turn.latency.p50_ms value=120.000 unit=ms
agents-bench storm.cold.turn.latency.p99_ms value=120.000 unit=ms
agents-bench storm.cold.turn.latency.max_ms value=120.000 unit=ms
agents-bench storm.cold.run.heap.peak_bytes value=484701.000 unit=bytes kind=heap-live-delta
agents-bench slow.warm.activation.ops.count value=1.000 unit=ops
agents-bench slow.warm.activation.resolve.get value=2.000 unit=requests/op
agents-bench slow.warm.activation.resolve.put value=0.000 unit=requests/op
agents-bench slow.warm.activation.resolve.cas value=0.000 unit=requests/op
agents-bench slow.warm.activation.resolve.list value=0.000 unit=requests/op
agents-bench slow.warm.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.warm.activation.resolve.body_in_bytes value=134.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.store.get value=0.000 unit=requests/op
agents-bench slow.warm.activation.store.put value=1.000 unit=requests/op
agents-bench slow.warm.activation.store.cas value=0.000 unit=requests/op
agents-bench slow.warm.activation.store.list value=0.000 unit=requests/op
agents-bench slow.warm.activation.store.conflict value=0.000 unit=requests/op
agents-bench slow.warm.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.store.body_out_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.publish.get value=0.000 unit=requests/op
agents-bench slow.warm.activation.publish.put value=0.000 unit=requests/op
agents-bench slow.warm.activation.publish.cas value=1.000 unit=requests/op
agents-bench slow.warm.activation.publish.list value=0.000 unit=requests/op
agents-bench slow.warm.activation.publish.conflict value=0.000 unit=requests/op
agents-bench slow.warm.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.publish.body_out_bytes value=233.000 unit=bytes/op kind=object-body
agents-bench slow.warm.activation.index.page_gets value=0.000 unit=requests/op
agents-bench slow.warm.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.warm.activation.other.delete value=0.000 unit=requests/op
agents-bench slow.warm.activation.latency.p50_ms value=802.000 unit=ms
agents-bench slow.warm.activation.latency.p99_ms value=802.000 unit=ms
agents-bench slow.warm.activation.latency.max_ms value=802.000 unit=ms
agents-bench slow.warm.message.ops.count value=64.000 unit=ops
agents-bench slow.warm.message.resolve.get value=1.766 unit=requests/op
agents-bench slow.warm.message.resolve.put value=0.000 unit=requests/op
agents-bench slow.warm.message.resolve.cas value=0.000 unit=requests/op
agents-bench slow.warm.message.resolve.list value=0.000 unit=requests/op
agents-bench slow.warm.message.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.warm.message.resolve.body_in_bytes value=14277.500 unit=bytes/op kind=object-body
agents-bench slow.warm.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.message.store.get value=0.750 unit=requests/op
agents-bench slow.warm.message.store.put value=1.750 unit=requests/op
agents-bench slow.warm.message.store.cas value=0.000 unit=requests/op
agents-bench slow.warm.message.store.list value=0.000 unit=requests/op
agents-bench slow.warm.message.store.conflict value=0.750 unit=requests/op
agents-bench slow.warm.message.store.body_in_bytes value=14.531 unit=bytes/op kind=object-body
agents-bench slow.warm.message.store.body_out_bytes value=34.406 unit=bytes/op kind=object-body
agents-bench slow.warm.message.publish.get value=0.000 unit=requests/op
agents-bench slow.warm.message.publish.put value=0.000 unit=requests/op
agents-bench slow.warm.message.publish.cas value=1.750 unit=requests/op
agents-bench slow.warm.message.publish.list value=0.000 unit=requests/op
agents-bench slow.warm.message.publish.conflict value=0.750 unit=requests/op
agents-bench slow.warm.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.message.publish.body_out_bytes value=14552.328 unit=bytes/op kind=object-body
agents-bench slow.warm.message.index.page_gets value=0.000 unit=requests/op
agents-bench slow.warm.message.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.warm.message.other.delete value=0.000 unit=requests/op
agents-bench slow.warm.message.latency.p50_ms value=908.000 unit=ms
agents-bench slow.warm.message.latency.p99_ms value=2839.000 unit=ms
agents-bench slow.warm.message.latency.max_ms value=2839.000 unit=ms
agents-bench slow.warm.step.ops.count value=64.000 unit=ops
agents-bench slow.warm.step.resolve.get value=6.000 unit=requests/op
agents-bench slow.warm.step.resolve.put value=0.000 unit=requests/op
agents-bench slow.warm.step.resolve.cas value=0.000 unit=requests/op
agents-bench slow.warm.step.resolve.list value=0.000 unit=requests/op
agents-bench slow.warm.step.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.warm.step.resolve.body_in_bytes value=53611.531 unit=bytes/op kind=object-body
agents-bench slow.warm.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.step.store.get value=3.000 unit=requests/op
agents-bench slow.warm.step.store.put value=1.016 unit=requests/op
agents-bench slow.warm.step.store.cas value=0.000 unit=requests/op
agents-bench slow.warm.step.store.list value=0.000 unit=requests/op
agents-bench slow.warm.step.store.conflict value=1.000 unit=requests/op
agents-bench slow.warm.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench slow.warm.step.store.body_out_bytes value=10.156 unit=bytes/op kind=object-body
agents-bench slow.warm.step.publish.get value=0.000 unit=requests/op
agents-bench slow.warm.step.publish.put value=0.000 unit=requests/op
agents-bench slow.warm.step.publish.cas value=5.000 unit=requests/op
agents-bench slow.warm.step.publish.list value=0.000 unit=requests/op
agents-bench slow.warm.step.publish.conflict value=0.000 unit=requests/op
agents-bench slow.warm.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.step.publish.body_out_bytes value=44847.297 unit=bytes/op kind=object-body
agents-bench slow.warm.step.index.page_gets value=0.000 unit=requests/op
agents-bench slow.warm.step.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.warm.step.other.delete value=0.000 unit=requests/op
agents-bench slow.warm.step.latency.p50_ms value=3160.000 unit=ms
agents-bench slow.warm.step.latency.p99_ms value=3659.000 unit=ms
agents-bench slow.warm.step.latency.max_ms value=3659.000 unit=ms
agents-bench slow.warm.turn.ops.count value=64.000 unit=ops
agents-bench slow.warm.turn.resolve.get value=2.016 unit=requests/op
agents-bench slow.warm.turn.resolve.put value=0.000 unit=requests/op
agents-bench slow.warm.turn.resolve.cas value=0.000 unit=requests/op
agents-bench slow.warm.turn.resolve.list value=0.000 unit=requests/op
agents-bench slow.warm.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.warm.turn.resolve.body_in_bytes value=17772.516 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.store.get value=1.984 unit=requests/op
agents-bench slow.warm.turn.store.put value=2.000 unit=requests/op
agents-bench slow.warm.turn.store.cas value=0.000 unit=requests/op
agents-bench slow.warm.turn.store.list value=0.000 unit=requests/op
agents-bench slow.warm.turn.store.conflict value=0.000 unit=requests/op
agents-bench slow.warm.turn.store.body_in_bytes value=30.563 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.store.body_out_bytes value=48.594 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.publish.get value=0.000 unit=requests/op
agents-bench slow.warm.turn.publish.put value=0.000 unit=requests/op
agents-bench slow.warm.turn.publish.cas value=2.016 unit=requests/op
agents-bench slow.warm.turn.publish.list value=0.000 unit=requests/op
agents-bench slow.warm.turn.publish.conflict value=0.000 unit=requests/op
agents-bench slow.warm.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.publish.body_out_bytes value=17634.266 unit=bytes/op kind=object-body
agents-bench slow.warm.turn.index.page_gets value=0.000 unit=requests/op
agents-bench slow.warm.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.warm.turn.other.delete value=0.000 unit=requests/op
agents-bench slow.warm.turn.latency.p50_ms value=1817.000 unit=ms
agents-bench slow.warm.turn.latency.p99_ms value=2085.000 unit=ms
agents-bench slow.warm.turn.latency.max_ms value=2085.000 unit=ms
agents-bench slow.warm.run.heap.peak_bytes value=37299806.000 unit=bytes kind=heap-live-delta
agents-bench slow.cold.activation.ops.count value=16.000 unit=ops
agents-bench slow.cold.activation.resolve.get value=2.000 unit=requests/op
agents-bench slow.cold.activation.resolve.put value=0.000 unit=requests/op
agents-bench slow.cold.activation.resolve.cas value=0.000 unit=requests/op
agents-bench slow.cold.activation.resolve.list value=0.000 unit=requests/op
agents-bench slow.cold.activation.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.cold.activation.resolve.body_in_bytes value=15857.375 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.store.get value=0.000 unit=requests/op
agents-bench slow.cold.activation.store.put value=1.000 unit=requests/op
agents-bench slow.cold.activation.store.cas value=0.000 unit=requests/op
agents-bench slow.cold.activation.store.list value=0.000 unit=requests/op
agents-bench slow.cold.activation.store.conflict value=0.000 unit=requests/op
agents-bench slow.cold.activation.store.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.store.body_out_bytes value=21.375 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.publish.get value=0.000 unit=requests/op
agents-bench slow.cold.activation.publish.put value=0.000 unit=requests/op
agents-bench slow.cold.activation.publish.cas value=1.000 unit=requests/op
agents-bench slow.cold.activation.publish.list value=0.000 unit=requests/op
agents-bench slow.cold.activation.publish.conflict value=0.000 unit=requests/op
agents-bench slow.cold.activation.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.publish.body_out_bytes value=8091.688 unit=bytes/op kind=object-body
agents-bench slow.cold.activation.index.page_gets value=0.000 unit=requests/op
agents-bench slow.cold.activation.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.cold.activation.other.delete value=0.000 unit=requests/op
agents-bench slow.cold.activation.latency.p50_ms value=904.000 unit=ms
agents-bench slow.cold.activation.latency.p99_ms value=1039.000 unit=ms
agents-bench slow.cold.activation.latency.max_ms value=1039.000 unit=ms
agents-bench slow.cold.message.ops.count value=64.000 unit=ops
agents-bench slow.cold.message.resolve.get value=2.000 unit=requests/op
agents-bench slow.cold.message.resolve.put value=0.000 unit=requests/op
agents-bench slow.cold.message.resolve.cas value=0.000 unit=requests/op
agents-bench slow.cold.message.resolve.list value=0.000 unit=requests/op
agents-bench slow.cold.message.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.cold.message.resolve.body_in_bytes value=16258.625 unit=bytes/op kind=object-body
agents-bench slow.cold.message.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.message.store.get value=0.750 unit=requests/op
agents-bench slow.cold.message.store.put value=1.750 unit=requests/op
agents-bench slow.cold.message.store.cas value=0.000 unit=requests/op
agents-bench slow.cold.message.store.list value=0.000 unit=requests/op
agents-bench slow.cold.message.store.conflict value=0.750 unit=requests/op
agents-bench slow.cold.message.store.body_in_bytes value=14.531 unit=bytes/op kind=object-body
agents-bench slow.cold.message.store.body_out_bytes value=34.406 unit=bytes/op kind=object-body
agents-bench slow.cold.message.publish.get value=0.000 unit=requests/op
agents-bench slow.cold.message.publish.put value=0.000 unit=requests/op
agents-bench slow.cold.message.publish.cas value=1.750 unit=requests/op
agents-bench slow.cold.message.publish.list value=0.000 unit=requests/op
agents-bench slow.cold.message.publish.conflict value=0.750 unit=requests/op
agents-bench slow.cold.message.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.message.publish.body_out_bytes value=14552.328 unit=bytes/op kind=object-body
agents-bench slow.cold.message.index.page_gets value=0.000 unit=requests/op
agents-bench slow.cold.message.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.cold.message.other.delete value=0.000 unit=requests/op
agents-bench slow.cold.message.latency.p50_ms value=1039.000 unit=ms
agents-bench slow.cold.message.latency.p99_ms value=2920.000 unit=ms
agents-bench slow.cold.message.latency.max_ms value=2920.000 unit=ms
agents-bench slow.cold.step.ops.count value=64.000 unit=ops
agents-bench slow.cold.step.resolve.get value=6.000 unit=requests/op
agents-bench slow.cold.step.resolve.put value=0.000 unit=requests/op
agents-bench slow.cold.step.resolve.cas value=0.000 unit=requests/op
agents-bench slow.cold.step.resolve.list value=0.000 unit=requests/op
agents-bench slow.cold.step.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.cold.step.resolve.body_in_bytes value=53611.578 unit=bytes/op kind=object-body
agents-bench slow.cold.step.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.step.store.get value=3.000 unit=requests/op
agents-bench slow.cold.step.store.put value=1.016 unit=requests/op
agents-bench slow.cold.step.store.cas value=0.000 unit=requests/op
agents-bench slow.cold.step.store.list value=0.000 unit=requests/op
agents-bench slow.cold.step.store.conflict value=1.000 unit=requests/op
agents-bench slow.cold.step.store.body_in_bytes value=30.000 unit=bytes/op kind=object-body
agents-bench slow.cold.step.store.body_out_bytes value=10.156 unit=bytes/op kind=object-body
agents-bench slow.cold.step.publish.get value=0.000 unit=requests/op
agents-bench slow.cold.step.publish.put value=0.000 unit=requests/op
agents-bench slow.cold.step.publish.cas value=5.000 unit=requests/op
agents-bench slow.cold.step.publish.list value=0.000 unit=requests/op
agents-bench slow.cold.step.publish.conflict value=0.000 unit=requests/op
agents-bench slow.cold.step.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.step.publish.body_out_bytes value=44847.344 unit=bytes/op kind=object-body
agents-bench slow.cold.step.index.page_gets value=0.000 unit=requests/op
agents-bench slow.cold.step.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.cold.step.other.delete value=0.000 unit=requests/op
agents-bench slow.cold.step.latency.p50_ms value=3129.000 unit=ms
agents-bench slow.cold.step.latency.p99_ms value=3659.000 unit=ms
agents-bench slow.cold.step.latency.max_ms value=3659.000 unit=ms
agents-bench slow.cold.turn.ops.count value=64.000 unit=ops
agents-bench slow.cold.turn.resolve.get value=2.250 unit=requests/op
agents-bench slow.cold.turn.resolve.put value=0.000 unit=requests/op
agents-bench slow.cold.turn.resolve.cas value=0.000 unit=requests/op
agents-bench slow.cold.turn.resolve.list value=0.000 unit=requests/op
agents-bench slow.cold.turn.resolve.conflict value=0.000 unit=requests/op
agents-bench slow.cold.turn.resolve.body_in_bytes value=19902.047 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.store.get value=1.984 unit=requests/op
agents-bench slow.cold.turn.store.put value=2.000 unit=requests/op
agents-bench slow.cold.turn.store.cas value=0.000 unit=requests/op
agents-bench slow.cold.turn.store.list value=0.000 unit=requests/op
agents-bench slow.cold.turn.store.conflict value=0.000 unit=requests/op
agents-bench slow.cold.turn.store.body_in_bytes value=30.563 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.store.body_out_bytes value=48.594 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.publish.get value=0.000 unit=requests/op
agents-bench slow.cold.turn.publish.put value=0.000 unit=requests/op
agents-bench slow.cold.turn.publish.cas value=2.250 unit=requests/op
agents-bench slow.cold.turn.publish.list value=0.000 unit=requests/op
agents-bench slow.cold.turn.publish.conflict value=0.000 unit=requests/op
agents-bench slow.cold.turn.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.publish.body_out_bytes value=19763.797 unit=bytes/op kind=object-body
agents-bench slow.cold.turn.index.page_gets value=0.000 unit=requests/op
agents-bench slow.cold.turn.index.planner_misses value=0.000 unit=misses/op
agents-bench slow.cold.turn.other.delete value=0.000 unit=requests/op
agents-bench slow.cold.turn.latency.p50_ms value=1811.000 unit=ms
agents-bench slow.cold.turn.latency.p99_ms value=2448.000 unit=ms
agents-bench slow.cold.turn.latency.max_ms value=2448.000 unit=ms
agents-bench slow.cold.run.heap.peak_bytes value=38578394.000 unit=bytes kind=heap-live-delta
agents-bench baseline.na.ingest.ops.count value=256.000 unit=ops
agents-bench baseline.na.ingest.telemetry.put value=1.000 unit=requests/op
agents-bench baseline.na.ingest.telemetry.get value=1.000 unit=requests/op
agents-bench baseline.na.ingest.telemetry.body_in_bytes value=8.570 unit=bytes/op kind=object-body
agents-bench baseline.na.ingest.telemetry.body_out_bytes value=8.570 unit=bytes/op kind=object-body
agents-bench baseline.na.ingest.latency.p50_ms value=30.000 unit=ms
agents-bench baseline.na.ingest.latency.p99_ms value=30.000 unit=ms
agents-bench baseline.na.ingest.latency.max_ms value=30.000 unit=ms
agents-bench baseline.na.run.isolation.agents_requests value=0.000 unit=requests
agents-bench baseline.na.run.heap.peak_bytes value=123617.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n100-start.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n100-start.cold.assign.resolve.get value=66.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.list value=2.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.resolve.body_in_bytes value=5282.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-start.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n100-start.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n100-start.cold.assign.latency.delay_ms value=740.000 unit=ms
agents-bench discovery-n100-start.cold.assign.sweep.passes value=1.000 unit=passes
agents-bench discovery-n100-start.cold.run.heap.peak_bytes value=527091.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n100-middle.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n100-middle.cold.assign.resolve.get value=66.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.list value=2.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.resolve.body_in_bytes value=5282.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-middle.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n100-middle.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n100-middle.cold.assign.latency.delay_ms value=740.000 unit=ms
agents-bench discovery-n100-middle.cold.assign.sweep.passes value=1.000 unit=passes
agents-bench discovery-n100-middle.cold.run.heap.peak_bytes value=525755.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n100-end.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n100-end.cold.assign.resolve.get value=102.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.list value=4.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.resolve.body_in_bytes value=7874.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n100-end.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n100-end.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n100-end.cold.assign.latency.delay_ms value=5460.000 unit=ms
agents-bench discovery-n100-end.cold.assign.sweep.passes value=2.000 unit=passes
agents-bench discovery-n100-end.cold.run.heap.peak_bytes value=520702.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n1000-start.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n1000-start.cold.assign.resolve.get value=66.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.resolve.list value=2.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.resolve.body_in_bytes value=5282.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-start.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-start.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-start.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-start.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-start.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-start.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n1000-start.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n1000-start.cold.assign.latency.delay_ms value=740.000 unit=ms
agents-bench discovery-n1000-start.cold.assign.sweep.passes value=1.000 unit=passes
agents-bench discovery-n1000-start.cold.run.heap.peak_bytes value=4434611.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n1000-middle.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n1000-middle.cold.assign.resolve.get value=514.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.resolve.list value=16.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.resolve.body_in_bytes value=37538.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-middle.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-middle.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-middle.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-middle.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-middle.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-middle.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n1000-middle.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n1000-middle.cold.assign.latency.delay_ms value=35740.000 unit=ms
agents-bench discovery-n1000-middle.cold.assign.sweep.passes value=8.000 unit=passes
agents-bench discovery-n1000-middle.cold.run.heap.peak_bytes value=6449877.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n1000-end.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n1000-end.cold.assign.resolve.get value=1002.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.resolve.list value=32.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.resolve.body_in_bytes value=72674.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-end.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-end.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-end.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-end.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-end.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n1000-end.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n1000-end.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n1000-end.cold.assign.latency.delay_ms value=75500.000 unit=ms
agents-bench discovery-n1000-end.cold.assign.sweep.passes value=16.000 unit=passes
agents-bench discovery-n1000-end.cold.run.heap.peak_bytes value=7247330.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n10000-start.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n10000-start.cold.assign.resolve.get value=66.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.resolve.list value=2.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.resolve.body_in_bytes value=5282.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-start.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-start.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-start.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-start.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-start.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-start.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n10000-start.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n10000-start.cold.assign.latency.delay_ms value=740.000 unit=ms
agents-bench discovery-n10000-start.cold.assign.sweep.passes value=1.000 unit=passes
agents-bench discovery-n10000-start.cold.run.heap.peak_bytes value=35672331.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n10000-middle.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n10000-middle.cold.assign.resolve.get value=5058.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.resolve.list value=158.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.resolve.body_in_bytes value=364706.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-middle.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-middle.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-middle.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-middle.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-middle.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-middle.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n10000-middle.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n10000-middle.cold.assign.latency.delay_ms value=390740.000 unit=ms
agents-bench discovery-n10000-middle.cold.assign.sweep.passes value=79.000 unit=passes
agents-bench discovery-n10000-middle.cold.run.heap.peak_bytes value=61611855.000 unit=bytes kind=heap-live-delta
agents-bench discovery-n10000-end.cold.assign.ops.count value=1.000 unit=ops
agents-bench discovery-n10000-end.cold.assign.resolve.get value=10002.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.resolve.put value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.resolve.cas value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.resolve.list value=314.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.resolve.conflict value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.resolve.body_in_bytes value=720674.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-end.cold.assign.resolve.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-end.cold.assign.store.get value=1.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.store.put value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.store.cas value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.store.list value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.store.conflict value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.store.body_in_bytes value=21.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-end.cold.assign.store.body_out_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-end.cold.assign.publish.get value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.publish.put value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.publish.cas value=2.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.publish.list value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.publish.conflict value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.publish.body_in_bytes value=0.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-end.cold.assign.publish.body_out_bytes value=622.000 unit=bytes/op kind=object-body
agents-bench discovery-n10000-end.cold.assign.index.page_gets value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.index.planner_misses value=0.000 unit=misses/op
agents-bench discovery-n10000-end.cold.assign.other.delete value=0.000 unit=requests/op
agents-bench discovery-n10000-end.cold.assign.latency.delay_ms value=780260.000 unit=ms
agents-bench discovery-n10000-end.cold.assign.sweep.passes value=157.000 unit=passes
agents-bench discovery-n10000-end.cold.run.heap.peak_bytes value=85324008.000 unit=bytes kind=heap-live-delta
agents-bench-usd baseline.na.ingest usd_per_million_ops=5.4000 label=assumption date=2026-10-29
agents-bench-usd discovery-n100-end.cold.assign usd_per_million_ops=71.2000 label=assumption date=2026-10-29
agents-bench-usd discovery-n100-middle.cold.assign usd_per_million_ops=46.8000 label=assumption date=2026-10-29
agents-bench-usd discovery-n100-start.cold.assign usd_per_million_ops=46.8000 label=assumption date=2026-10-29
agents-bench-usd discovery-n1000-end.cold.assign usd_per_million_ops=571.2000 label=assumption date=2026-10-29
agents-bench-usd discovery-n1000-middle.cold.assign usd_per_million_ops=296.0000 label=assumption date=2026-10-29
agents-bench-usd discovery-n1000-start.cold.assign usd_per_million_ops=46.8000 label=assumption date=2026-10-29
agents-bench-usd discovery-n10000-end.cold.assign usd_per_million_ops=5581.2000 label=assumption date=2026-10-29
agents-bench-usd discovery-n10000-middle.cold.assign usd_per_million_ops=2823.6000 label=assumption date=2026-10-29
agents-bench-usd discovery-n10000-start.cold.assign usd_per_million_ops=46.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.cold.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.cold.message usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.cold.step usd_per_million_ops=36.1000 label=assumption date=2026-10-29
agents-bench-usd dormant.cold.turn usd_per_million_ops=26.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd dormant.warm.message usd_per_million_ops=10.6000 label=assumption date=2026-10-29
agents-bench-usd dormant.warm.step usd_per_million_ops=36.1000 label=assumption date=2026-10-29
agents-bench-usd dormant.warm.turn usd_per_million_ops=24.1000 label=assumption date=2026-10-29
agents-bench-usd hot.cold.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd hot.cold.message usd_per_million_ops=18.6000 label=assumption date=2026-10-29
agents-bench-usd hot.cold.step usd_per_million_ops=33.6800 label=assumption date=2026-10-29
agents-bench-usd hot.cold.turn usd_per_million_ops=22.9436 label=assumption date=2026-10-29
agents-bench-usd hot.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd hot.warm.message usd_per_million_ops=18.5064 label=assumption date=2026-10-29
agents-bench-usd hot.warm.step usd_per_million_ops=33.6800 label=assumption date=2026-10-29
agents-bench-usd hot.warm.turn usd_per_million_ops=21.6800 label=assumption date=2026-10-29
agents-bench-usd index-p2.cold.activation usd_per_million_ops=13.2800 label=assumption date=2026-10-29
agents-bench-usd index-p2.cold.message usd_per_million_ops=13.2800 label=assumption date=2026-10-29
agents-bench-usd index-p2.cold.turn usd_per_million_ops=29.4784 label=assumption date=2026-10-29
agents-bench-usd index-p2.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd index-p2.warm.message usd_per_million_ops=12.8816 label=assumption date=2026-10-29
agents-bench-usd index-p2.warm.turn usd_per_million_ops=24.1000 label=assumption date=2026-10-29
agents-bench-usd index-p64.cold.activation usd_per_million_ops=10.8600 label=assumption date=2026-10-29
agents-bench-usd index-p64.cold.message usd_per_million_ops=10.8600 label=assumption date=2026-10-29
agents-bench-usd index-p64.cold.turn usd_per_million_ops=27.0584 label=assumption date=2026-10-29
agents-bench-usd index-p64.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd index-p64.warm.message usd_per_million_ops=10.4616 label=assumption date=2026-10-29
agents-bench-usd index-p64.warm.turn usd_per_million_ops=21.6800 label=assumption date=2026-10-29
agents-bench-usd many.cold.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd many.cold.message usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd many.cold.step usd_per_million_ops=36.1000 label=assumption date=2026-10-29
agents-bench-usd many.cold.turn usd_per_million_ops=26.8000 label=assumption date=2026-10-29
agents-bench-usd many.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd many.warm.message usd_per_million_ops=10.6000 label=assumption date=2026-10-29
agents-bench-usd many.warm.step usd_per_million_ops=36.1000 label=assumption date=2026-10-29
agents-bench-usd many.warm.turn usd_per_million_ops=24.1000 label=assumption date=2026-10-29
agents-bench-usd slow.cold.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd slow.cold.message usd_per_million_ops=18.6000 label=assumption date=2026-10-29
agents-bench-usd slow.cold.step usd_per_million_ops=33.6800 label=assumption date=2026-10-29
agents-bench-usd slow.cold.turn usd_per_million_ops=22.9436 label=assumption date=2026-10-29
agents-bench-usd slow.warm.activation usd_per_million_ops=10.8000 label=assumption date=2026-10-29
agents-bench-usd slow.warm.message usd_per_million_ops=18.5064 label=assumption date=2026-10-29
agents-bench-usd slow.warm.step usd_per_million_ops=33.6800 label=assumption date=2026-10-29
agents-bench-usd slow.warm.turn usd_per_million_ops=21.6800 label=assumption date=2026-10-29
agents-bench-usd storm.cold.activation usd_per_million_ops=6.2000 label=assumption date=2026-10-29
agents-bench-usd storm.cold.ingest usd_per_million_ops=5.4000 label=assumption date=2026-10-29
agents-bench-usd storm.cold.pass usd_per_million_ops=23.2000 label=assumption date=2026-10-29
agents-bench-usd storm.cold.turn usd_per_million_ops=21.6000 label=assumption date=2026-10-29
agents-bench-summary figures=1601 failures=0
```
