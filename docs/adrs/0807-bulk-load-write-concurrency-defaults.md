# ADR-0807: Bulk-load write concurrency ceilings and defaults

Status: Proposed. Decisions 3 and 5 are superseded by the amendment at the end
of this file (issue #800); decisions 1, 2 and 4 stand as written.

## Context

The `ravel-cli load` bulk path decodes a Parquet file and writes RLOG objects
through the in-process `LogIngestRouter` (ADR-0089, ADR-0109). On a 100M-row
ClickBench load on a 16-core box the loader left the box almost idle: `mpstat`
showed ~12% user, ~88% idle, and **0.06% iowait** across the whole load, and
`/usr/bin/time` reported 10,396.90 s user CPU over 4,466.76 s wall, i.e. 2.33
of 16 cores busy. Near-zero iowait rules out disk, so the box was waiting on
serialized round trips to object storage. Doubling `--batch-rows` (doubling
object size) did not move utilization, which rules out object size as the
limiter. The limiter is concurrency: the write path has fixed bounds that
default to serial, and the primary one is not reachable from the loader at all.

### The write path, walked end to end

The audit below is of the log write path, which is the path `ravel-cli load`
uses (ADR-0089, ADR-0109). The metrics path (`crates/ravel-ingest/src/router.rs`,
`shard.rs`) and the span path (`span_router.rs`, `span_shard.rs`) carry the same
`max_inflight_flushes` semaphore and `channel_depth` mpsc shape; bulk load does
not exercise them, so they are named but not re-audited. The stages walked, from
Parquet decode to the object-storage acknowledgement:

1. **Decode/build.** A single blocking decoder task owns the `--read-cursors`
   stride cursors and drives `collect_spans` + `build_columnar_batch` in
   row-group order, pushing each built batch into a bounded channel
   (`spawn_decode_pipeline`, `services/ravel-cli/src/load.rs:776`).
2. **Loader submit loop** (`load.rs:793-916`): pulls built batches from that
   channel and keeps a window of `tokio::spawn`ed write tasks in flight, bounded
   by `--pipeline-depth`.
3. **Router** `write` / `write_columnar` (`crates/ravel-ingest/src/log_router.rs:346`,
   `:488`): charges the ADR-0069 byte budget, resolves the generation view,
   partitions the batch by shard, dispatches one message per involved shard, and
   awaits every shard's Strict ack together via `join_all`
   (`await_strict_acks`, `log_router.rs:410-474`).
4. **Shard actor flush** (`log_shard.rs`, mirrored in `shard.rs`,
   `span_shard.rs`): a per-shard `tokio::sync::Semaphore` sized to
   `max_inflight_flushes` gates each spawned flush task (`log_shard.rs:834`,
   `shard.rs:925`, `span_shard.rs:547`; the acquire that blocks is
   `shard.rs:1271`). Each flush encodes the object, issues the data PUT and the
   commit-record PUT, and acks.

### Audit of every fixed concurrency or queue-depth bound on the write path

| Bound | file:line | Default | Operator-changeable today | What it bounds | Role in the composed ceiling |
|---|---|---|---|---|---|
| `--batch-rows` (`DEFAULT_BATCH_ROWS`) | `load.rs:67`, `main.rs:238` | 10000 | Yes, `--batch-rows` | Rows per Strict flush; one RLOG object per involved shard | Sets object size and object count, not write concurrency; doubling it did not move utilization |
| `--shards` | `main.rs:231` | 4 | Yes, `--shards` (validated against / written to the durable provisioning record) | Shard fan-out width; one batch write dispatches to up to this many shards concurrently | The multiplier `S` in the formula below; on the reference box it defaults far below core count |
| `--read-cursors` (`resolve_read_cursors`) | `main.rs:254`, `load.rs:1480` | `None` -> `min(shards, row_groups)`, floored at 1; explicit value clamped to `[1, row_groups]` | Yes, `--read-cursors` | Parallel stride cursors over the Parquet row groups; sets how many distinct shards each batch actually touches | Collapses `S`: at `1` on a file sorted by a resource attribute (ClickBench's `hits.parquet`, sorted by `CounterID`) a batch lands on ~1 shard regardless of `--shards` |
| `--decode-queue-batches` (`DEFAULT_DECODE_QUEUE_BATCHES`) | `load.rs:75`, `main.rs:290` | 2 | Yes, `--decode-queue-batches` | Decoded batches queued between the single decoder task and the writers | Decouples decode from encode look-ahead; not a write-concurrency term |
| `--pipeline-depth` | `main.rs:278` | 1 | Yes, `--pipeline-depth` | Strict batch writes the loader keeps outstanding | One term of the per-shard `min()`; at `1` also forces an await-all-shards barrier between batches |
| `IngestConfig::max_inflight_flushes` | `config.rs:185`, `config.rs:228` | 1 | **No on the loader** (fixed at the default: `load.rs:714-722` builds `IngestConfig { .. , ..IngestConfig::default() }`, exposing no flag). Yes on `ravel-server` via `--max-inflight-flushes` (`services/ravel-server/src/config.rs:589`) | Concurrent flush tasks per shard, via the per-shard semaphore | The other term of the per-shard `min()`; the only cap on overlapping one shard's PUT round trips |
| `IngestConfig::channel_depth` | `config.rs:132`, `config.rs:217` | 256 | No on the loader (fixed at the default) | mpsc depth router -> shard actor (`log_router.rs:174`) | With `target_bytes: 1` each message flushes; never the binding term because `--pipeline-depth` keeps far fewer than 256 batches outstanding |
| `target_bytes` (loader override) | `load.rs:717` | 1 (loader forces it) | No (loader-forced) | Size trigger; makes every batch flush immediately as one object | Guarantees one object per batch; interacts with `--batch-rows`, not with concurrency |
| `WRITE_ACK_DEADLINE` | `load.rs:79` | 60 s | No (const) | Per-batch Strict ack timeout | A timeout, not a concurrency bound; on elapse `AckTimeout` carries no recovered tokens |
| `put_retry_max_attempts` / base / max delay | `config.rs:223-225` | 4 / 100 ms / 2 s | No on the loader | PUT retry attempts inside a flush | Tail latency, not concurrency |
| Byte budget (`est_record_bytes`, ADR-0069) | `log_router.rs:330-336` | process-wide ceiling | Server-side only | Global buffered-bytes admission | A refusal bound (`BufferBudgetExceeded`), not a per-write concurrency term |

### The composed ceiling

Within one `write` / `write_columnar` call every involved shard is dispatched
and awaited concurrently (`await_strict_acks` folds `join_all` over the per-shard
ack receivers, `log_router.rs:422`), so one batch = up to `S` concurrent shard
flushes, where `S` is the number of distinct shards the batch touches (up to
`--shards`, and equal to it only when `--read-cursors` spreads the batch across
all shards). The loader keeps up to `--pipeline-depth` such batches outstanding.
Per shard, each outstanding batch contributes at most one flush, and the
per-shard semaphore caps concurrent flushes at `max_inflight_flushes`. So the
number of object writes in flight is:

```text
concurrent_object_writes = shards * min(pipeline_depth, max_inflight_flushes)
```

as the minimum of the loader-side demand `pipeline_depth * S` and the shard-tier
supply `shards * max_inflight_flushes`, which reduces to the form above when
`S = shards`. The terms:

- `shards` = `--shards` (loader default 4), the fan-out width.
- `pipeline_depth` = `--pipeline-depth` (default 1), batches outstanding.
- `max_inflight_flushes` = `IngestConfig::max_inflight_flushes` (default 1),
  flushes per shard, **not reachable from the loader**.
- `S` = distinct shards per batch, driven by `--read-cursors`; `S = shards` only
  with cursor spread, else `S` collapses toward 1.

### What binds first (verified from the code, not assumed)

At loader defaults the formula gives `4 * min(1, 1) = 4`. Two distinct effects
sit under that number, and they are not the same knob:

- **The cross-batch barrier is `--pipeline-depth`.** At depth 1 the submit loop
  awaits the current batch's every-shard ack before it spawns the next batch's
  write (`load.rs:897`, `while inflight.len() >= pipeline_depth`). Every shard
  that finishes early then idles until the slowest shard of the same batch acks
  and the loop comes around. This is the serialization the 88%-idle evidence
  shows, and raising `--pipeline-depth` removes it: with a full pipe the shard
  actors pull their next message the moment the semaphore frees, keeping up to
  `shards` writes continuously in flight.

- **The per-shard ceiling is `max_inflight_flushes`, and it is pinned.** Because
  the loader builds its own `IngestConfig` from `..IngestConfig::default()` and
  exposes no flag for it, `max_inflight_flushes` is fixed at 1 on the bulk path.
  So the per-shard term `min(pipeline_depth, max_inflight_flushes)` is `min(p, 1)
  = 1` for every `p`. Raising `--pipeline-depth` alone therefore lifts the
  barrier but never puts a second flush on any one shard: a shard's own PUT round
  trip is never overlapped with its next encode. Exceeding `shards` concurrent
  writes requires raising `max_inflight_flushes`, which `ravel-cli load` cannot
  do today.

So `max_inflight_flushes` binds first for per-shard overlap and is not
operator-reachable, while `--pipeline-depth` binds the cross-batch barrier and is
reachable. And `--shards` itself defaults to 4: on the 16-core reference box the
sustainable ceiling with `--pipeline-depth` raised is still `shards = 4` writes,
well under core count, which is the residual idle. Three knobs interact; only two
of them can be turned on the bulk path, and one of those (`--shards`) is a
provisioning decision, not a per-load tuning knob.

### The acknowledgement consequence of `--pipeline-depth` > 1

The `--pipeline-depth` help text (`main.rs:255-277`) documents a failure-reporting
change at depth > 1. Read against the loop (`load.rs:876-915`) and the router, it
is exact:

- The reported durable-token list is always exactly the batches strictly before
  the failing one, in submission order (the loop records a token only by draining
  `inflight` oldest-first at `load.rs:901`, so a later batch that finished first
  can never record its token ahead of an earlier batch or an earlier failure).
- But on a write error the loop aborts the still-queued writes' `JoinHandle`s
  (`load.rs:910-912`), and abort cancels only the loader's *wait* for the ack,
  not the underlying flush: the shard actor holds only a channel `tx`, no join
  handle of the spawned flush (comment at `load.rs:881-892`). A batch after the
  failing one can therefore still complete its data PUT and commit-record PUT in
  the background after the loader has returned an error.
- That later batch's rows then become query-visible (its commit record exists;
  visibility is atomic per object, docs/consistency-model.md) **without being
  reported durable**. A resume from the reported token list re-ingests them, and
  because logs have no query-time dedup (docs/consistency-model.md, "Duplicates
  and idempotency": a re-ingest of logs is *user-visible* duplication), those
  rows are duplicated in query results.

This is not a violation of the Strict ack contract. The router still returns a
token only for a shard whose flush durably committed (`await_strict_acks`,
`log_router.rs:432-473`); the gap is between the router's per-shard ack and the
*loader's* durable-token report, which is a resume aid, not the ack. At
`--pipeline-depth 1` the gap cannot occur: only one batch is ever outstanding, so
there is no later batch to leak. The gap is a known limitation pending a
`ravel-ingest` flush-cancellation mechanism (comment at `load.rs:891-892`): if a
cancelled batch provably did not commit, the report would again equal what
landed.

## Alternatives considered

**Raise the bulk-loader `--pipeline-depth` default (to 2, 4, or `--shards`).**
This is the change that most directly moves the 88%-idle number, and for a
restartable bulk load the throughput is the point. It is rejected as a *default*
because it silently converts the durable-token report from "reported == committed"
to "reported is a subset of committed" for every operator, including one who does
not read the help and who resumes from the reported tokens after a failure. A
throughput default must not weaken a durability report while the underlying
cancellation gap is unfixed. It stays an opt-in flag with the consequence stated
at the point of use.

**Raise the bulk-loader `max_inflight_flushes` default above 1.** Pointless while
`--pipeline-depth` defaults to 1: at depth 1 a shard receives one message per
batch and one batch at a time, so a second per-shard permit is never demanded.
The two knobs are multiplicatively coupled (`min(pipeline_depth,
max_inflight_flushes)`); raising one default without the other buys nothing.

**Change the `ravel-server` (serving) `max_inflight_flushes` default.** Out of
scope and rejected. The serving Strict ack is client-facing and its contract is
frozen in docs/consistency-model.md; ADR-0067 decision 2 already set that default
to 1 and made raising it "a measured decision, not a routine tuning change." This
ADR governs the bulk path and leaves the serving default untouched.

**Bypass the concurrency windows: have the loader build and commit objects
directly.** Rejected for the same reason ADR-0109 rejected it: it forks the commit
protocol and the ack semantics ADR-0089 deliberately reused, for no bound this
ADR cannot lift within the existing router.

**Do nothing (leave both at 1, document only).** Rejected as insufficient: the
audit found a bound (`max_inflight_flushes`) that no operator can change from the
bulk path at all. An unreachable bound is itself the defect. Exposing it, even at
an unchanged default, is the minimum honest fix; leaving it invisible would mean
the audit changed nothing.

## Decision

1. **Serving-ingest defaults are unchanged.** `ravel-server`'s
   `--max-inflight-flushes` stays at 1 (ADR-0067 decision 2). The client-facing
   Strict ack contract in docs/consistency-model.md is not touched.

2. **Expose `max_inflight_flushes` on the bulk loader.** `ravel-cli load` gains a
   `--max-inflight-flushes` flag threaded into the `IngestConfig` it builds
   (`load.rs:714-722`), so the second concurrency window becomes reachable on the
   bulk path. Default 1, and 0 rejected at the edge exactly as the server rejects
   it (`config.rs:2065`). Exposing the knob is not changing its default.

3. **Both bulk-loader defaults stay at 1.** `--pipeline-depth` and
   `--max-inflight-flushes` both default to 1, preserving today's
   one-batch-at-a-time behavior and today's exact durable-token report.
   Note the formula above: at the defaults the ceiling is still `shards`
   concurrent object writes, because a batch fans out to every shard its rows
   touch. What depth 1 enforces is the barrier BETWEEN batches, not one write
   at a time. The
   speed-up is opt-in because its cost is a durability-report weakening (the
   acknowledgement consequence above), and that must be a conscious operator
   choice, not a default. A bulk load is restartable and idempotency is the
   operator's concern, but the operator who resumes is exactly the one the report
   gap harms, so the safe default is the one whose report equals what landed.

4. **Document the tuning recipe at the point of use.** The bulk-load guide
   (docs/guides/clickbench.md) and the flag help state the recipe and the ceiling
   formula: raise `--shards` toward core count (a provisioning decision, made when
   the signal is provisioned), raise `--pipeline-depth` to remove the cross-batch
   barrier, and raise `--max-inflight-flushes` to overlap each shard's PUT round
   trip, with `concurrent_object_writes = shards * min(pipeline_depth,
   max_inflight_flushes)`. Any operator raising `--pipeline-depth` above 1 is told,
   at the flag, that a resume after a partial-load failure may re-ingest
   duplicate rows.

5. **Revisit the `--pipeline-depth` default after flush cancellation lands.** The
   report gap exists only because aborting the loader's ack wait does not stop the
   underlying flush (`load.rs:891-892`). Once `ravel-ingest` gains a mechanism that
   provably prevents a cancelled batch from committing, `--pipeline-depth` > 1 no
   longer weakens the durable-token report, and a higher default becomes a plain
   throughput decision to reopen here.

## Consequences

- **Measured since this ADR was written: raising both windows to 4 loads the
  100M-row ClickBench corpus in 1,519.75 s against 4,466.76 s at the defaults,
  a 2.94x reduction.** This measures the **combined effect of raising both
  windows**; it is not a single-variable result. The unchanged object count of
  8,424 controls for object layout, which is what makes the comparison against
  arm A meaningful, but it does not apportion the gain between
  `--pipeline-depth` and `--max-inflight-flushes`. What is known about the split
  is one-sided: raising the depth ALONE aborts, so the second window is
  necessary; no arm has isolated the flush window on its own. Cores busy rise
  from 2.33 to 8.58 of 16.
  Two failed arms bracket it: doubling `--batch-rows` instead (bigger objects,
  both windows at default) was 11% SLOWER, and raising `--pipeline-depth` to 16
  alone aborted with `flush failed: timed out waiting for shard ack`, because
  depth over-subscribes a pipeline bounded by the formula above. This is what
  the opt-in buys, and it raises the value of decision 5: once flush
  cancellation exists, the default becomes a plain throughput decision worth
  2.94x.

- **Every published ClickBench load figure produced by this repo's load scripts
  was measured at the defaults and is no longer representative.** The scope is
  what the scripts prove: `run_load_v4.sh` and `run_load_v4big.sh` pass neither
  write-window flag, so they ran at the effective defaults. A figure whose recipe
  has not been inspected is not covered by this claim. The 4,466.76 s reference load, the comparison claiming
  a load 2.1x faster than Elasticsearch, and the 15x deficit against ClickHouse's
  290 s were all measured with `--pipeline-depth 1`, `--max-inflight-flushes 1`,
  and the default `--shards 4` on a 16-core box, i.e. `shards * min(1, 1) = 4`
  concurrent writes with a cross-batch barrier on top. They are not wrong, but
  they describe a configuration nobody tuning for throughput would deliberately
  choose. Any republished load-time comparison must be re-measured with the recipe
  in decision 4 (shards near core count, `--pipeline-depth` and
  `--max-inflight-flushes` raised), and the old figures must not be cited as
  Ravel's load throughput without that caveat. This invalidates the load-side half
  of every ClickBench comparison table; the query-side figures are unaffected.

- The bulk path gains a durability-affecting knob (`--pipeline-depth` > 1) whose
  correct use depends on the operator's downstream tolerating duplicate log rows
  on resume. This is the at-least-once model docs/consistency-model.md already
  states for logs, now reachable per-batch on the loader; it is documented at the
  flag, not hidden.

- No persistent format, schema, series or stream identity, commit token, or object
  key layout changes. No frozen contract is touched and no version is bumped. This
  ADR changes CLI surface and defaults only.

- `--max-inflight-flushes` now means the same knob in two places with two scopes:
  process-wide per-shard on `ravel-server`, and per-load on `ravel-cli load`. The
  docs name both so the shared name does not read as a single global.

- The residual ceiling after tuning is `--shards`, which is a provisioning
  decision validated against the durable record, not a per-load knob. A load on a
  signal provisioned at 4 shards cannot exceed 4 concurrent writes however the
  other knobs are set; reaching core-count parallelism requires provisioning shard
  count near core count, exactly the condition ADR-0109 decision 8 already stated.

Refs: #807

## Amendment (issue #800): both bulk-loader defaults move to 4

### What the audit above got wrong

The audit concluded that "`max_inflight_flushes` binds first for per-shard
overlap". Measured, that is not what happens at the shipped defaults, and the
distinction matters because it changes which knob to reach for.

The logs pipeline had no per-shard skew accounting: issue #865's
`on_actor_ns` / `flush_permit_wait_ns` / `off_actor_ns` split was wired into
`router.rs` and `shard.rs` only, and `ravel-cli load` drives `log_router.rs` and
`log_shard.rs`. So the counter that says whether the flush semaphore is refusing
work did not exist for the one workload this ADR is about, and the audit reasoned
from the code instead. That accounting is now wired into the logs pipeline
(`crates/ravel-ingest/tests/log_shard_skew.rs` pins it), and it says:

- At `--pipeline-depth 1`, `flush_permit_wait_ns` is exactly **0**, however slow
  the flushes are. Nothing ever parks on the per-shard semaphore, because the
  loader never asks a shard for a second concurrent flush. The inner window is
  not binding; it is not being consulted.
- Submitting concurrently against one permit gives a `flush_permit_wait_ns` of
  `2 * SLOW` on a three-flush fixture -- and **the same wall** as the serial arm.
  A binding inner window looks different from an unconsulted one on the counter,
  and identical on the clock.

So the two windows are symmetric, not ordered: `min(p, m)` has no "first". The
composed-ceiling formula in the audit was right; the sentence about which term
binds first was not.

The end-to-end 2x2, on a 16-batch single-shard fixture with a 40ms injected
data-object PUT (`both_write_windows_are_needed_to_overlap_put_round_trips` in
`services/ravel-cli/src/load.rs`), pre-registered at `16 * 40ms = 640ms` for any
arm leaving either window at 1 and `4 * 40ms = 160ms` for the arm raising both:

| depth | flushes | wall | peak concurrent object PUTs |
|---|---|---|---|
| 1 | 1 | 673.9 ms | 1 |
| 4 | 1 | 671.9 ms | 1 |
| 1 | 4 | 673.3 ms | 1 |
| 4 | 4 | 169.3 ms | 4 |

This is the apportionment the Consequences section above says was never done:
neither window alone moves the wall at all (within 0.3%), and together they give
3.97x. It also confirms that section's one-sided claim -- raising depth alone is
not merely insufficient, it is worth nothing.

### Why the report gap no longer blocks a higher default

Decision 3 kept both defaults at 1 because `--pipeline-depth > 1` weakened the
durable-token report, and decision 5 deferred revisiting it until `ravel-ingest`
gained "a mechanism that provably prevents a cancelled batch from committing".

That framing had one option too few. The gap did not come from batches
committing; it came from the loader *aborting* the join handles of later writes
and therefore never learning what they committed. The loader can simply wait
instead: once every outstanding write has reached a terminal outcome, nothing is
left that can commit, so the reported list is exactly what landed, at any depth.
`harvest_after_failure` in `services/ravel-cli/src/load.rs` does this, and the
test that used to pin the gap
(`partial_window_failure_reports_only_earlier_batches_never_later`) now pins its
closure as `partial_window_failure_reports_every_batch_that_committed`: on the
same fixture the report goes from `[0, 1]` to `[0, 1, 3, 4]`, which is precisely
the set of batches whose objects the store holds.

The cost is on the failure path only and is bounded: the remaining writes were
submitted before the failing one and run concurrently, so the wait is at most one
`WRITE_ACK_DEADLINE` (60s). A cancellation mechanism in `ravel-ingest` would
still be an improvement -- it would avoid the orphaned objects a doomed batch
writes -- but it is no longer a precondition for the default, and this ADR should
not have made it one.

### Amended decisions

**3 (amended). Both bulk-loader defaults are 4.** `DEFAULT_PIPELINE_DEPTH = 4`
and `DEFAULT_MAX_INFLIGHT_FLUSHES = DEFAULT_PIPELINE_DEPTH`, pinned to each other
by test, because an inner window below the outer one re-serialises each shard's
PUT round trips and an inner window above it is unreachable. `4` and not higher:
it is the largest depth with a measured result behind it, a depth of 16 against
one permit aborted with `timed out waiting for shard ack`, and it bounds the
memory at a stated multiple rather than at `--shards`, which an operator may
provision far above 4.

The memory cost is the outer window's alone. The loader holds at most
`--pipeline-depth` built batches for their in-flight writes plus
`--decode-queue-batches` queued ahead, so the resident batch working set goes
from `1 + 2` to `4 + 2` batches of `--batch-rows` rows; against the #682 anchor
(8 shards, 80k rows, ~6GB live under tcmalloc at depth 1) that is roughly 4x on
the same geometry. `--max-inflight-flushes` adds nothing further here, because
the outstanding batches are already capped by the depth -- unlike on
`ravel-server`, where the same field has no outer window above it and ADR-0067
decision 2's default of 1 stands untouched.

**5 (amended). Reopened and closed.** The report gap is fixed by waiting rather
than by cancelling, so there is nothing left to revisit here. A `ravel-ingest`
flush-cancellation mechanism remains worth having for the orphaned data objects a
doomed batch leaves behind, which is an object-count and storage-cost question,
not a durability-report one.

**Unchanged.** Decision 1 (serving defaults), decision 2 (the flag exists), and
decision 4 (document the recipe at the point of use) stand. The Consequences
section's statement that every published ClickBench load figure was measured at
the old defaults also stands, and now has a second reason to be re-measured: the
defaults themselves have moved.

Refs: #800
