# S5: bounded read-ahead, checklist against the code and the verdict

Hypothesis under test: starting future scan work before the current segment
finishes reduces exposed open stalls at a fixed configured resource budget.

## Verdict first (measured on the injected-stall backend, inferred for S3)

Not supported as a distinct mechanism at this revision. The evidence:

- Measured (backend `memory+get-delay-20ms`, LEDGER runs 5 and 6): the
  partition's critical path is the serialized opens, 5 x 21 ms = 107 ms at
  8 partitions, and decode is 2 to 5 ms of it. Depth-1 read-ahead can hide
  at most the decode of the current segment behind the open of the next,
  so on this workload its ceiling is 4 x (0.5 to 1 ms) = under 5 ms of a
  110 ms scan, under 5 percent, inside the 15 percent noise floor. A
  prototype could not be shown to win here.
- Measured (run 6): the same stall is cut to 65 ms and 43 ms by raising the
  partition count to 16 and 32 with identical I/O (40 GETs, 1,074,826
  bytes). Read-ahead depth `d` on `P` partitions holds `P x (d + 1)` objects
  in flight; `P x (d + 1)` partitions at depth 0 hold the same, and the
  partition count is already a knob (`--sql-partition-count`, ADR-1195). At
  a fixed in-flight object budget, read-ahead and partition count buy the
  same overlap; read-ahead adds an accounting seam the partition count does
  not need.
- Inferred, not measured (no S3 access): on the reference corpus the
  per-object decode is comparable to the per-object GET time (ADR-1196's
  11.24 GB in 486 s at 256 concurrency is 23 MB/s per process, and this
  box decodes an aggregation at 60 to 70 MB/s per core, run 3b
  `agg_group_severity_dur` 16.6 ms for 1.07 MB), so the overlap read-ahead
  offers is real there. It is still the same overlap more partitions offer.
  The case where read-ahead beats partitions is one where the partition
  count is capped by something other than in-flight memory (DataFusion's
  `target_partitions` coupling to CPU parallelism of the operators above the
  scan). That is a configuration question (ADR-1195 unbundles it), not a
  scheduling gap, and it is the one thing an S3 pass should measure before
  any prototype: `--sql-partition-count` sweep at fixed
  `--store-get-concurrency` on the reference host.

What the study did NOT do: no read-ahead prototype was built. The checklist
below is the design the ceiling would have truncated; it is delivered as the
design of record for a future attempt, with the conditions that would have
to change first.

## Checklist against the code

| Question | Answer at `ee2070a0` |
|---|---|
| Who polls or schedules future opens while the current segment decodes? | Nobody. `LogScanStream` has one `state` slot; `Opening` is constructed only in `NextSegment` after the previous segment is exhausted (`crates/ravel-sql/src/logs_scan.rs:3451-3529`). Storing a second `OpenFuture` would not start it: a future does nothing until polled, and `poll_next` polls one state. A prototype would need either a spawned task per read-ahead open (`tokio::spawn` with a `JoinHandle` held in the stream, cancelled on drop) or a second future polled in the same `poll_next` before the decode branch. The spawned form is the only one that makes progress while decode holds the runtime thread, because decode is synchronous inside `poll_next` (S1 finding 5). |
| Bounds on queued work and live bytes, across partitions and queries? | None exist for fetch bytes. `GetLimiter` bounds requests in flight (per leaf GET, permit released before the object is consumed, `log_fetcher.rs:3796-3805`). `AssemblyBufferPool` bounds idle buffers only (S1 finding 7). `RequestBudgets` bounds requests and bytes scanned per query (`request_budgets.rs:36-46`), not residency. The whole-object `Bytes` held by `LogSegmentScan` is unreserved (S1 finding 9). A read-ahead depth `d` would multiply peak residency by `d + 1` with no accountant refusing it. |
| Admission before allocation; reservation held through in-flight and ready-but-unconsumed; release at true end of life? | Not available. ADR-1170 decision 2 specifies exactly this (`FetchMemoryExhausted`, reservations around `whole_object_bytes`, `ObjectAssembler`, `fetch_blocks`, `fetch_chunk_ranges`) and PR #1284 implements it; neither is in this tree. A read-ahead prototype must land after #1284, reserve before constructing the open future, keep the reservation inside the `LogSegmentScan` (whose `Drop` already folds stats, `log_fetcher.rs:466-472`) and release it there. |
| Interaction with query, tenant and process budgets; reuse of the authoritative accountant? | The request accountant is `GetLimiter` (shared `Arc`, server-wide, `services/ravel-server/src/lib.rs:1833-1836`). `RequestScheduler`/`ClassedStore` has a `Background` class with a floor (`scheduling.rs:110-129`) but is off by default and not on the query path (S1 server wiring). A speculative open should acquire `GetLimiter` as today and additionally be admitted as `RequestClass::Background` when `--store-scheduling` is on, so a foreground demand read is never queued behind speculation. The memory side is ravel-memory's `MemoryBudget` via #1284/#1436. Do not add a third allowance. |
| Progress under tight memory: speculation never consumes what current work needs? | Requires a reservation that can be refused: on `MemoryExhausted` the read-ahead is skipped and the stream falls back to the demand open it does today. With no reservation (this tree) a prototype cannot satisfy this. |
| Cancellation, error propagation, early LIMIT completion, cleanup? | A spawned read-ahead task must be aborted in `LogScanStream::drop`; DataFusion drops the stream on LIMIT completion (`EmissionType::Incremental`, `logs_scan.rs:2029`) and on error. Errors from a speculative open must be deferred to the moment the segment is actually needed, not surfaced early: a LIMIT may finish before that segment, and surfacing the error would fail a query whose result was complete. The `attrs_raw` reopen (`ReopenRows`) re-fetches the same segment and must not consume a speculative open of the next one. |
| Pruning before speculative payload reads; late materialisation? | On the planning path the block-index list per segment exists before any scan open (`PlanCounts`), so speculation can be restricted to surviving blocks. On the fast path there is no prune. Late materialisation (`LogsRowFetchExec`, ADR-0774) re-reads blocks by row ref through `RowFetchSource`; speculative bytes from phase 1 would have to be kept resident to help phase 2, which is a cache question, not a read-ahead one. |
| Tenant fairness through the shared limiter? | `GetLimiter` is process-wide and tenant-blind; a tenant's speculation competes equally with another tenant's demand reads. `RequestScheduler`'s `Background` class is the existing seam for a lower class, still tenant-blind. Read-ahead worsens fairness unless it is admitted at the lower class. |
| Ordering and deterministic aggregation despite out-of-order I/O? | The scan declares no output ordering (`logs_scan.rs:1508-1510`); aggregates above are order-independent; `ORDER BY` sorts above the scan. Out-of-order completion of speculative opens is safe as long as each partition still drains its `VecDeque<OwnedSeg>` in order (only the open starts early, consumption order is unchanged). |

## Granularity, if a prototype is ever built

- Segment granularity, depth 1, on the fast path only (whole-object opens),
  after #1284: the smallest change is a second `Option<OpenFuture>` slot
  polled at the top of `poll_inner` while the state is `Columnar`/`Rows`.
  It only helps if the runtime gets to poll it, which it does not while
  decode runs synchronously on the same task; so the practical form is a
  `tokio::spawn`ed open whose `JoinHandle` is the slot. That is the point
  where this stops being "small".
- Row-group or column-range granularity would change `LogSegmentScan`'s
  contract (whole object resident, absolute offsets) and is a separate
  experiment, as the task said.

## Whole-object assembly and progressive decoding

Code-observed: the open completes only when the whole object (or all
selected ranges assembled into an object-sized buffer) is resident
(`log_fetcher.rs:1232`, `:1287`, `:1888`, and `ObjectAssembler::into_bytes`
`:3304-3306`); decode then proceeds block by block from the resident bytes,
and the first batch is emitted after the first block (`logs_scan.rs:3571`,
`:4536-4555`). There is no decode of block `i` while block `i+1` is still
in flight within one open. Measured (run 5, `first_batch_elapsed_min` 22 ms
under a 20 ms stall): time to first batch is one open plus one block decode,
as the code predicts. Streaming decoded blocks after a complete open does
not prove fetch/decode overlap within an open; it is the larger separate
experiment and was not attempted.

## Synchronous decode pressure

Measured (run 3a, backend `memory`): the attrs-map statements hold the
runtime threads for 1.1 to 2.2 s of partition-summed decode plus 1.7 to 2.2
s of row-batch build per statement, at 3.4 cores of process CPU during a
0.4 to 1.1 s query, which is decode work, not scheduler starvation: with
`fetch_concurrency 8` there are 8 partitions and 16 cores. On the declared
form the same statements spend 10 to 12 ms of decode. No additional thread
pool is indicated by these measurements; the pressure is the row path's
merged-map construction, which S4 addresses at its source.
