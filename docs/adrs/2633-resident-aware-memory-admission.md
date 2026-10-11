# ADR-2633: Resident-aware memory admission

Status: Accepted (2026-10-09).

Revised (2026-10-10): the high-water mark is re-derived from the uncharged heap
measured after the aggregate hold (PR #2685); the default moves from 75 % to 70 %.

Revised (2026-10-11): the gate closes on host available memory below a floor,
not on the process's resident size against a mark (the #2806 amendment below);
the mark and its 70 % default are superseded, and the derived floors are
clamped to an eighth and a quarter of host memory so a small host starts.

Issue #2633, epic #2040. Builds on ADR-1170 (one
process-wide memory budget) and, once accepted, changes the ADR-1170 sentences
listed in section 6. No persistent format changes: no object Ravel writes, no
key it builds and no protobuf schema changes. This decision adds a gate in
front of the process `MemoryBudget` that reads the host's available memory,
forces a jemalloc purge while it is below a purge floor, and refuses growth
with the typed budget error (the `HostMemory` cause) while it is below a close
floor. It also moves every memory-budget refusal on the fetch path, and the
catalog decode refusal (`CatalogError::MemoryExhausted`), from 503 to the
budget's 422.

## Context

ADR-1170 bounds tracked allocations: the two caches are hard eviction caps
carved from `memory_budget_bytes`, and SQL and fetch draw from
`memory_remainder_bytes` through one `Arc<MemoryBudget>`
(`crates/ravel-memory/src/lib.rs`). Its constraint 4 says the ceiling cannot
be RSS, and its Consequences name the residual it leaves: process survival is
guaranteed for tracked allocations only. The measurements below show that the
residual is not small, and that most of it is not live memory at all.

### What was measured

Scaled ClickBench run: `--memory-budget-bytes` 8 GB, 20 ClickBench files, 10
connections, 32 GB host. "Accounted" is what the ADR-1170 ledgers had
reserved. "Retention" is jemalloc `stats.resident` minus `stats.allocated`:
pages the process freed and the allocator has not yet returned to the kernel.

| Run | Server | Peak gap (jemalloc `resident` − accounted) | Of which retention (`resident` − `allocated`) | Peak VmRSS | Idle retention |
|---|---|---|---|---|---|
| baseline | main before fixes | 4.22 GB | 2.54 GB | 9.01 GB | 2.0 GB |
| rerun | + background purge thread (#2652) + cache owned copy (#2653) | 6.30 GB | 5.43 GB | 10.44 GB | 0.08 GB |
| decay A, x2 | same, default decay | 6.69 / 7.06 GB | 6.54 / 6.11 GB | 9.89 / 10.19 GB | 0.09 GB |
| decay B, x2 | same, `dirty_decay_ms:1000,muzzy_decay_ms:0` | 4.91 / 5.00 GB | 4.33 / 4.65 GB | 8.83 / 8.82 GB | 0.09 GB |

What the table and the runs behind it establish:

- Every run's peak VmRSS is above the 8 GB budget, by 0.82 to 2.44 GB.
- Retained-but-freed pages dominate the gap at peak: 2.54 of 4.22 GB on the
  baseline, and 4.33 to 6.54 GB of 4.91 to 7.06 GB on every later run.
- The background purge thread (#2652) removed idle retention (2.0 GB to
  0.08 GB) but not peak retention, which grew. Idle is not where the process
  dies.
- The 1 s dirty decay (decay B) cuts median retention from about 3.7 to about
  1.3 GB but leaves 4 to 5 GB bursts. One run's 5 s samples of retention read
  0.9, 2.3, 4.3, 2.1, 0.9 GB: a burst of 2 GB per 5 s sample, about
  0.4 GB/s, that a decay timer of any length lags.
- The 1 s decay costs 10.9 % qps, which cannot be separated from noise: the
  two default-decay runs alone differ by 9.2 %.
- The uncharged live heap is mostly DataFusion aggregation state. It is
  `allocated` − accounted + handoff overlap: the overlap is counted twice
  inside "accounted", so it is added back, not subtracted. In nearly every
  sample the overlap equals `fetch_reserved`, so in practice the figure is
  `allocated` − `sql_reserved` − cache resident. Its per-sample maximum,
  over every 5 s sample in each run's `samples.tsv`, is 4.46 GB (baseline),
  3.15 GB (rerun), 3.43 and 1.91 GB (decay A) and 3.03 and 2.98 GB
  (decay B). A gate must hold at the worst sample, not at the peak-gap one,
  so the derivation in section 2 uses a per-sample maximum, taken after the
  aggregate hold below.
- `memory_overhead_reserve_bytes` is a fixed 2.1 GB at the 27 GB budget (it
  is `MEMORY_OVERHEAD_RESERVE_BYTES`, 2 GiB, in
  `crates/ravel-maintain/src/config.rs`).
- The M1 run (30 GB host, 27 GB budget) was OOM-killed in 3 of 3 attempts.

The gap is therefore mostly memory nobody is using, which the allocator would
return if asked. ADR-1170 cannot see it, because the budget counts
reservations, and the allocator's decay cannot be relied on to return it
before the kernel acts.

### The aggregate hold, and what it leaves uncharged

DataFusion 54.1 shrinks a `GroupedHashAggregateStream`'s reservation at emit
while the emitted batch is still live, which left up to 1.2 GB per statement
uncharged. PR #2685 (main `7b5c56134`) adds a hold: `TenantDelegatingPool`
keeps a non-spillable `GroupedHashAggregateStream`'s shrunk bytes charged
until the stream unregisters. On DataFusion `main` (55.1) the default
aggregation path already keeps the reservation sized to the emitted batch
(found while preparing apache/datafusion#26167), so the hold becomes
removable when Ravel upgrades. The gate does not depend on it.

The same scaled workload (8 GB budget, 20 files, 10 connections, 360 s), two
runs at main `6b794db90` with the hold:

| Run | Max uncharged heap per sample | Median uncharged heap | Typed 422 refusals | Peak retention (`resident` − `allocated`) | Peak VmRSS |
|---|---|---|---|---|---|
| hold R1 | 2.48 GB (a single-statement serial-pass sample; concurrency-phase maximum 1.33 GB) | 0.60 GB | 53 | 6.63 GB | 9.58 GB |
| hold R2 | 1.82 GB (concurrency phase) | 0.54 GB | 87 | 6.00 GB | 9.77 GB |

Against two runs without the hold, the median uncharged heap falls from 0.82
and 0.73 GB, qps rises from 7.63 to 8.03 (mean), and typed 422 refusals rise
from 37 and 42. The per-sample maximum falls from 1.91 to 4.46 GB across the
six earlier runs to 1.82 and 2.48 GB. Charging did not touch retention: peak
`resident` − `allocated` is 6.0 to 6.6 GB, and peak VmRSS is still 1.58 and
1.77 GB above the budget. Retention is now the remaining overshoot the gate
exists for.

### What the allocator exposes through the safe API

The server links the jemalloc 5.3.1 that `tikv-jemalloc-sys` 0.7.1 vendors and
reads it through `tikv-jemalloc-ctl` 0.7.0. The workspace forbids `unsafe_code`
(root `Cargo.toml`, `[workspace.lints.rust]`), so only the safe surface of
`tikv-jemalloc-ctl` is usable. What that surface can and cannot do decides
the mechanism:

- Readable, safely: `epoch::advance()`, `stats::resident`, `stats::allocated`,
  `stats::active`, `background_thread`, `arenas::narenas`. The server already
  reads the first four in `services/ravel-server/src/mem_stats.rs` (`read`).
- Not reachable: `arena.<i>.purge` and `arena.<i>.decay`. jemalloc registers
  both as `NEITHER_READ_NOR_WRITE` controls, which return `EPERM` when the
  caller passes a non-null new value. Every write `tikv-jemalloc-ctl` offers
  passes a non-null new value of `size_of::<T>()`: the safe `Access<T>` impls,
  and `raw::write` and `raw::write_mib`, which are `unsafe fn` besides. A null
  new value needs the `mallctl` binding in `tikv-jemalloc-sys`, an `unsafe`
  extern call. The typed modules expose no purge or decay key at all.
- Reachable, safely, and sufficient: `arena.<i>.dirty_decay_ms` and
  `arena.<i>.muzzy_decay_ms`, `ssize_t` controls written through
  `Access<isize>` on a `Name`. jemalloc's setter (`pac_decay_ms_set` in the
  vendored `src/pac.c`) reinitialises the decay state and then calls
  `pac_maybe_decay_purge`, which for a decay of 0 purges the arena's whole
  dirty (or muzzy) cache synchronously, whatever the background-thread
  eagerness. Writing 0 and then writing the previous value back is a forced
  purge of that arena. `MALLCTL_ARENAS_ALL` is not accepted for this key
  (`arena_get` finds no arena, `EFAULT`), so a purge walks arenas
  `0..arenas::narenas` and skips the indexes that return `EFAULT`, which are
  arenas never initialised.
- With jemalloc's default `muzzy_decay_ms` of 0, purged dirty pages are
  returned with a forced purge, so VmRSS falls when the write returns. When an
  operator's `_RJEM_MALLOC_CONF` sets a nonzero muzzy decay, dirty pages would
  be purged lazily into the muzzy state and stay counted in VmRSS until the
  kernel reclaims them; the purge below handles that case by zeroing the
  muzzy decay as well.

## Decision

```mermaid
flowchart TD
    S["Sampler thread wakes every --memory-gate-interval-ms"] --> E["epoch::advance, read stats.resident"]
    E --> C{"resident at or above the mark?"}
    C -- no --> G["Set ravel_memory_gate_resident_bytes to the reading"]
    G --> O["Gate OPEN"]
    C -- yes --> P["Forced purge: each arena's decay_ms set to 0, then restored"]
    P --> PC["ravel_memory_gate_purges_total plus 1"]
    PC --> R["epoch::advance, re-read stats.resident"]
    R --> G2["Set ravel_memory_gate_resident_bytes to the re-read"]
    G2 --> C2{"still at or above the mark?"}
    C2 -- no --> O
    C2 -- yes --> X["Gate CLOSED"]
    O --> S
    X --> S
```

```mermaid
flowchart TD
    A["New statement at admission"] --> A1{"gate OPEN?"}
    A1 -- yes --> A2["take the concurrency permit, run"]
    A1 -- no --> A3["wait, checking each interval, up to --memory-gate-wait-ms and the deadline"]
    A3 -- opened --> A2
    A3 -- timed out --> AR["422 typed budget error, site admission"]
    B["MemoryBudget::try_reserve of n above 0: SQL try_grow, fetch, catalog decode"] --> B1{"gate OPEN?"}
    B1 -- no --> BR["MemoryExhausted, resident cause, site reserve: a spillable operator spills, else 422"]
    B1 -- yes --> B2{"accounted total fits?"}
    B2 -- yes --> B3["reserve"]
    B2 -- no --> BA["MemoryExhausted, accounted cause, ravel_memory_budget_refusals_total"]
    D["TenantDelegatingPool::grow, infallible"] --> D1{"gate OPEN?"}
    D1 -- no --> DB["trip CeilingBreach, site grow: the query fails with 422 on its next poll"]
    D1 -- yes --> D2["existing ADR-1170 breach checks"]
```

### 1. A gate that reads `stats.resident` at a bounded rate

A dedicated std thread (`ravel-memory-gate`) runs in `ravel-server` for the
process's life. Every `--memory-gate-interval-ms` (default 100, accepted range
10 to 1000) it calls `epoch::advance()`, reads `stats::resident`, stores the
reading in the gauge, and decides the gate state as the first diagram shows.
The epoch is refreshed by this thread only, at most once per interval;
nothing on a query path calls into the allocator's control interface.
The reading that decides the gate is no longer `stats.resident` but the
host's available memory, read on the same sample (see the host-memory
amendment below); the resident reading stays a gauge. The gate closes only
after consecutive readings below the floor, and reopens on the first reading
at or above it (see the consecutive-samples amendment below).

The state is a small padded group of atomics owned by `MemoryBudget` in
`ravel-memory`, which stays a std-only crate with no allocator dependency:
an open-or-closed flag, the last reading and the mark (the available reading
and the floor, since the host-memory amendment below). `ravel-server` writes
them, and a check site loads only the flag; the reading and mark are for
refusal messages and metrics. The group sits on its own cache line, apart
from `reserved`, so a reservation's CAS and a gate check never contend; it is
written at most once per interval. Admission refusals do not pass through
`MemoryBudget`: the admission call in `ravel-query` reads the same flag,
returns its own typed refusal, and owns the count behind
`ravel_memory_gate_refusals_total{site="admission"}`.

The 100 ms default is set by the burst slope: at the measured 0.4 GB/s, one
interval lets about 40 MB of growth go unseen, small against the slack in
section 2. An epoch refresh merges per-arena statistics and costs tens of
microseconds on this host's arena count, so the epoch refresh costs well
under 0.1 % of one core. That figure excludes the purge, which runs on the
same thread and is bounded separately (section 2 and the acceptance test);
the implementation measures and stamps both (task 2).

### 2. A forced purge at the high-water mark, and the default mark

The mark, its derivation and the startup check in this section are
superseded: see the host-memory amendment below. The purge mechanism stands.
The mark is `--memory-gate-high-water-percent` (default 70, accepted 1 to 100)
of the resolved `memory_budget_bytes`, the whole budget including the cache
caps. At or above it, the sampler forces a purge through the safe decay write
from the Context: for each initialised arena, set `muzzy_decay_ms` to 0 when
it is nonzero, set `dirty_decay_ms` to 0, then restore `dirty_decay_ms` and
`muzzy_decay_ms` to the values `update` returned. It counts one purge per
pass over the arenas, re-reads `resident`, and closes the gate only if the
reading is still at or above the mark. The gate reopens at the first later
sample below the mark; a purge runs on every sample that reads at or above
the mark, so the purge rate is at most one per interval.

The purge runs on the sampler thread, never on an allocating thread. It holds
each arena's decay mutex while that arena purges, so allocations that need
new extents from that arena wait for it; thread-cache fast paths do not take
that lock. Between the two writes, frees on that arena purge eagerly. The
restore resets the arena's decay backlog. jemalloc's own comment on the
setter says decay changes are intended to be infrequent; this use writes at
most four times per arena per interval (set and restore, dirty and muzzy), and only while the mark is exceeded. The
reset costs at most an earlier-than-scheduled purge, and task 2's purge
duration measurement is where a cost from it would show. Nothing else in the
server writes decay settings; an operator's `_RJEM_MALLOC_CONF` values are
read back by `update` and restored, not overwritten.

**Default derivation.** With budget B, mark M, and acceptance slack S, the
peak the gate permits is bounded by:

```text
peak VmRSS <= M + D + G + O
  D  growth before the sampler sees it: 0.4 GB/s x (0.1 s interval + 0.05 s purge) = 0.06 GB
  G  live growth a closed gate does not stop: the uncharged heap, at most 2.48 GB measured
     (per-sample maximum of allocated - accounted + overlap, two runs with the
     aggregate hold; see Context)
  O  VmRSS outside jemalloc resident (stacks, binary, kernel-side buffers): taken at
     NON_BUDGET_BASELINE_BYTES, 256 MiB = 0.27 GB, until task 2 measures it
```

Accounted growth stops when the gate closes: `try_reserve` refuses, and an
infallible `grow` trips a breach that ends its query on the next poll. G is
the part no ledger sees. The acceptance band is B + S with S = 512 MiB
(0.54 GB), the `NON_BUDGET_BASELINE_BYTES` baseline plus as much again for
sampling and measurement error. O and S both include that baseline, so the
bound counts it twice; that lowers the mark, which is the safe direction.
The bound also assumes no reservation is charged ahead of the pages that back
it; in every measured run `allocated` stays above the accounted total. For
the 8 GB run:

```text
M <= B + S - D - G - O = 8.00 + 0.54 - 0.06 - 2.48 - 0.27 = 5.73 GB = 0.716 B
```

G = 2.48 GB is R1's worst sample, taken while one statement ran alone in the
serial pass. With the concurrency-phase maximum, G = 1.82 GB (R2), the same
bound gives 8.00 + 0.54 − 0.06 − 1.82 − 0.27 = 6.39 GB = 0.80 B. The default
takes the conservative figure, because the serial pass is a state the
process really reaches: the largest whole-five-percent fraction under 0.716
is 70 %, a mark of 5.6 GB, which leaves 0.13 GB for an O or G larger than
estimated. 75 % (6.0 GB) overshoots the band by 0.27 GB in the same bound.
The derivation assumes the hold is in the binary, or a DataFusion whose
default aggregation keeps the emitted batch reserved (55.x does; see
apache/datafusion#26165): with the baseline's 4.46 GB maximum from before
the hold, the same bound gives 3.75 GB.

The mark costs little concurrency on the measured workload. Taking O at
0.27 GB, the peak allocated figure (VmRSS − O − retention) is at most about
2.7 and 3.5 GB in the two hold runs, 4.7 GB on the rerun before the hold, and
6.2 GB on the pre-fix baseline. That assumes the peaks of the three figures
coincide, which the run summaries do not say. A purge brings resident down to
about the allocated figure, so on the current code the gate is expected to
purge in every run and to refuse in none; on the baseline it would have
refused. At the 27 GB budget the mark is 18.9 GB, and the same bound gives a
peak of about 21.7 GB on a 30 GB host.

The following paragraph is superseded by the host-memory amendment below.
The percentage applies to the budget, not the host, because the budget is the
figure the operator chose and the acceptance test is stated against. A
startup check refuses a configuration where the cache hard caps plus, in
`all` mode, the ingest buffer ceiling reach the mark (the startup-check
amendment below drops the ingest term): those sit inside `resident`, and
with them full the gate would never open. At the defaults the caps are 30 %
of the budget (45 % on a loopback store), well under 70 %.

The gate is off, and the stamp says why, when there is no budget to take a
fraction of (`--mode gateway`, or a budget that resolved from the fallback),
when `--disable-memory-gate` is passed, and on a build without jemalloc.

### 3. Where the check sits, and what it costs

Three sites, all reading the open-or-closed flag of the same padded group of
atomics:

1. **Admission.** Every query admission that today calls
   `QueryAdmissionController::try_admit` (`QueryControls::admit` in
   `crates/ravel-query/src/http/service.rs`, and the three independent
   `try_admit` calls in `crates/ravel-sql/src/flight/service.rs`, at about
   lines 310, 431 and 648, each of which the implementing task replaces and
   tests) goes through one async admission
   call that first checks the gate. When the gate is closed it waits, checking
   once per gate interval, up to `--memory-gate-wait-ms` (default 2000, 0
   refuses at once) or the statement's remaining deadline, whichever is
   shorter, and then refuses with the typed budget error. This is the bounded
   wait the decision asks for. A waiting statement holds no permit, no
   reservation and no thread.
2. **Every `MemoryBudget::try_reserve` with n > 0.** That one function is the
   funnel for SQL `TenantDelegatingPool::try_grow` (through
   `TenantMemoryAccountant::try_grow`), the RSEG, RLOG and RSPAN fetch
   reservations (`reserve_fetch` in `crates/ravel-query/src/fetcher.rs` and
   `span_fetcher.rs`, through `MemoryBudget::reserve`), and the catalog
   decode charge (`crates/ravel-catalog/src/charged.rs`). A closed gate
   refuses at once with `MemoryExhausted`, which gains a cause:
   accounted, or resident with the reading and the mark. The existing
   rollbacks run unchanged. A zero-byte reservation is always admitted.
   The catalog decode charge answers an accounted refusal today with an
   eviction pass and one retry; a resident-cause refusal skips both, since
   eviction frees no `resident` before the next sample and the retry would
   meet the same closed gate, and `decode_reserve_retries` keeps counting
   eviction passes only.
3. **`TenantDelegatingPool::grow`.** The infallible path cannot refuse, so a
   closed gate trips its `CeilingBreach`, and the query fails with the 422 on
   its next poll and releases what it holds. A gate-caused breach carries the
   resident cause (the reading and the mark), not the "bytes reserved exceeds
   limit" wording of the ledger breaches, since the ledger is usually far
   from its limit when the gate closes.

The growth sites refuse rather than wait. `try_grow` is synchronous and runs
on a tokio worker: blocking there parks the threads whose progress and drops
are what lower `resident`, and a query blocked while holding reservations is
hold-and-wait. Refusing lets a spill-capable operator spill (ADR-0954), which
is the purpose of `try_grow` returning an error. The wait therefore applies to
growth that has not started, which is admission. This narrows "a growth waits
a bounded time and is then refused" to admission growth; reservation growth
inside a running statement is refused immediately.

Cost on the hot path: one `Relaxed` load of a read-mostly cache line per
`try_reserve`, next to the CAS loop the function already runs on a contended
atomic, and one per `grow`. The pre-registered expectation is no qps change
measurable against the 9.2 % run-to-run spread; task 1 adds a
microbenchmark of `try_reserve` with the gate open, which must stay within
5 % of the current figure.

### 4. Observability

The gauges and the stamp fields in this section are as the host-memory
amendment below renames them: `ravel_memory_gate_high_water_bytes` became
`ravel_memory_gate_available_floor_bytes`, `ravel_memory_gate_available_bytes`
and `ravel_memory_gate_purge_floor_bytes` were added, and the stamp carries
the floors instead of the mark. `ravel_memory_gate_samples_below_floor_total`
and the stamp field `close_after_samples` were added after that (see the
consecutive-samples amendment below).

- `ravel_memory_gate_resident_bytes`, gauge: the sampler's last `resident`
  reading, after the purge when one ran.
- `ravel_memory_gate_high_water_bytes`, gauge: the mark, so a dashboard can
  draw it beside the reading.
- `ravel_memory_gate_purges_total`, counter: forced purges.
- `ravel_memory_gate_refusals_total{site="admission"|"reserve"|"grow"}`,
  counter: refusals caused by the gate. The `admission` count lives in
  `ravel-query`'s admission call (section 1); the server reads it for export.
- `ravel_memory_budget_refusals_total{site="reserve"|"grow"}`, counter: the
  existing accounted refusals, which no metric counts today. The two families
  are separate so a reader can tell "the ledger is full" from "the process is
  full".
- A startup INFO line, `memory gate resolved`, beside the existing
  `performance default resolved` lines (one carries `setting =
  "memory_budget_bytes"`) and the `allocator background purge thread
  resolved` line, with `enabled`, `high_water_bytes`, `high_water_percent`,
  `memory_budget_bytes`, `interval_ms`, `wait_ms` and `source` (`default`,
  `flag`, or `disabled-gateway`, `disabled-fallback-budget`, `disabled-flag`,
  `not-jemalloc`).

Every family is unlabelled except `site`, a closed three-value set.

### 5. The fetch-memory and catalog decode 503s are folded in

ADR-1170 answers a fetch the budget cannot admit with 503 and the message
"upstream storage temporarily unavailable" (`MSG_UNAVAILABLE`), on the SQL
surface (`SqlError::Fetch`, `LogFetch` and `SpanFetch` take
`ErrorClass::Unavailable` in `crates/ravel-sql/src/error.rs`) and on PromQL
(`FetchError::FetchMemoryExhausted` redacts to `MSG_UNAVAILABLE` in
`crates/ravel-query/src/http/error.rs`). The SQL pool's own refusal answers
422.

This ADR folds the change in rather than naming a separate issue, because the
gate refuses through the same `FetchMemoryExhausted`. Left alone, the
decision's "refused with the typed budget error (HTTP 422)" would hold on the
SQL pool and not on the fetch pool, and every gate closure under a burst would
tell clients that storage is down. Decision: every `FetchMemoryExhausted`,
accounted or gate, answers 422 with a typed memory-budget message on the SQL
and PromQL HTTP surfaces, and on Flight SQL takes the same status the SQL
pool's refusal takes today. The refusal still carries only byte figures,
never a key or a tenant value.

The catalog decode charge is the third refusal path with the same defect.
`crates/ravel-catalog/src/charged.rs` returns `MemoryExhausted` from
`reserve_decoded`. That surfaces as `CatalogError::MemoryExhausted`, and
`crates/ravel-query/src/http/error.rs` redacts it to `MSG_UNAVAILABLE`, so
`ApiError::Unavailable` answers 503. A gate closure during a burst would
then tell a PromQL client that storage is down. It moves with the fetch
refusal: `CatalogError::MemoryExhausted`, accounted or gate, answers 422
with the typed memory-budget message on every surface that answers it today.
Task 4 covers both.

### 6. ADR-1170: the sentences that change

ADR-1170 is Proposed, so it is not edited by this draft. The implementing
change (task 5) adds the amendment below at the end of ADR-1170 and the
inline pointer "see the host-memory gate amendment below" to each named
section, and beside each of the four retired 503 sentences (items 4, 5, 7
and 8) on the line before or after it, since `check-amendment-integrity.sh`
accepts a retired phrase only with the pointer in that window. This follows
the "Amending an ADR" convention in the decision-record index. The block
and items 1 and 2 below are as the host-memory amendment (2026-10-11,
#2806) rewrote them: the gate they describe closes on host available
memory, not on `stats.resident` at a mark. The sentences it changes, quoted
from ADR-1170:

1. Constraints, constraint 4: "The ceiling cannot be RSS. MemTotal and
   `memory.max` are kill boundaries; DataFusion's `grow` allocates before a
   breach is detectable. The honest claim is "tracked allocations are bounded,
   with a measured overhead reserve"." Still true of the ceiling. It changes
   in that the host's available memory, sampled at a bounded rate, becomes
   a second, admission-side bound under the ceiling: the gate closes before
   the kill boundary is reached, not at it.
2. Rejected alternatives: "**Use RSS or `memory.max` as the ceiling.** Kill
   boundaries, not budgets; `grow` has already allocated by the time a breach
   is visible (constraint 4). The budget bounds tracked allocations and the
   acceptance test measures the residual against a stated reserve." The
   rejection of RSS as the ceiling stands. The acceptance test becomes the
   host-memory amendment's criteria (a) to (d); peak VmRSS is a reported
   figure beside them, not a pass criterion.
3. Consequences: "process survival is guaranteed for tracked allocations and
   bounded, not guaranteed, for that path, by the overhead reserve exceeding
   `partitions x max batch bytes`." The gate closes that path too, by tripping
   the breach while closed.
4. Decision 2 server amendment: "a fetch that needs more than the remaining
   budget fails with `FetchMemoryExhausted`, returned as 503." Becomes 422.
5. SQL fetcher amendment: "A SQL fetch the budget cannot admit fails with
   `FetchMemoryExhausted` and answers 503 (`unavailable`) over HTTP, distinct
   from the SQL memory pool's own refusal, which answers 422." Becomes: both
   answer 422.
6. Available-memory amendment, "What this does not fix": "It does not bound
   memory the ledgers never see." Partly superseded: the gate bounds the
   retained part of that memory and stops admitting while the unseen live
   part is high. Attributing and charging the live part stays open.
7. Available-memory amendment, decision 3: "a fetch the remainder cannot
   admit answers 503 (the SQL fetcher amendment above)." Becomes 422.
8. Available-memory amendment, decision 3: "A fetch that arrives while a
   tenant sits at its ceiling and the remaining 10% is already reserved
   still answers 503." Becomes 422. The cap's point, that 10% stays outside
   the SQL ceiling, is unchanged.

The amendment text task 5 appends, with its markers:

```markdown
## Amendment (ADR-2633): a host-memory gate in front of the budget

<!-- amendment-applies: sections="Constraints a governor has to satisfy|Rejected alternatives|Consequences|Amendment 2026-09-26 (issue #1255): decision 2 reaches the server|Amendment (2026-09-29, issue #2086): the SQL path's fetchers reserve against the budget|Amendment (2026-10-03, issue #2367): the budget starts from available memory" pointer="host-memory gate amendment" -->
<!-- amendment-supersedes: phrase="returned as 503" pointer="host-memory gate amendment" -->
<!-- amendment-supersedes: phrase="answers 503 (`unavailable`) over HTTP" pointer="host-memory gate amendment" -->
<!-- amendment-supersedes: phrase="a fetch the remainder cannot admit answers 503" pointer="host-memory gate amendment" -->
<!-- amendment-supersedes: phrase="still answers 503" pointer="host-memory gate amendment" -->

ADR-2633 (as amended 2026-10-11, #2806) adds a gate that samples the host's
available memory (`MemAvailable`, and under a finite cgroup memory limit the
smaller of that and the cgroup's headroom) every 100 ms, forces a jemalloc
purge while the reading is below a purge floor (default the smaller of twice
the close floor and a quarter of host memory, at most once per 5 s), and
refuses admission and reservation growth with the budget's 422 and the
`HostMemory` cause while the reading is below a close floor (default
`min(max(512 MiB, 2 % of host memory), host memory / 8)`). Constraint 4 and
the rejected "RSS as the ceiling" alternative stand for the ceiling itself:
the gate is a bound under the kill boundary, not a ceiling. The acceptance
is ADR-2633's criteria (a) to (d): the ClickBench concurrent error ratio at
most 0.10 on a stock c6a.4xlarge, q33 to q35 answering on a warm server, no
process killed at a 27 GB budget on a 32 GB host with the gate's refusals
carrying the host-memory cause, and purges bounded by the minimum interval;
peak VmRSS is a reported figure beside them, not a band. The infallible
`grow` path trips its breach while the gate is closed. Every
`FetchMemoryExhausted` and every `CatalogError::MemoryExhausted` answers
422, not 503.
```

## Rejected alternatives

**A fixed derate of the budget.** Setting the budget to, say, 60 % of what
the host allows would have kept every measured run inside its host, but it
costs concurrency at all times to cover a burst that happens some of the
time, and the factor that is enough depends on the workload: retention at
peak ranged from 2.54 to 6.63 GB on one workload at one budget. The gate pays
only while resident is actually high, and a purge recovers most of it.

**The shorter decay alone.** The 1 s dirty decay cuts median retention to
about 1.3 GB, but bursts of 4 to 5 GB remain, and the peak is what kills the
process. Its 10.9 % qps cost cannot be told from noise in the runs so far.

**Exact DataFusion charging.** Charging DataFusion's aggregation state
addresses the uncharged live heap, not retention. The aggregate emit part is
done in Ravel as an interim hold (PR #2685, see Context), and the upstream
issues are filed: apache/datafusion#26165, #26166, and a comment on #23393.
With the hold the per-sample maximum fell from up to 4.46 GB to 2.48 GB,
which is the G section 2 now uses. Charging did not remove retention: peak
`resident` − `allocated` stayed at 6.0 to 6.6 GB with the hold on, freed
memory no charging can see. That is why the gate remains.

**A cgroup memory limit.** `memory.max` makes the kernel enforce the bound by
reclaiming and then killing. That is the outcome this decision exists to
replace with a refusal the client can act on.

**A purge through the unsafe `mallctl` binding.** `arena.<all>.purge` is one
call where the decay write walks every arena. Calling it needs `unsafe`, which
the workspace forbids, or a wrapper crate outside the workspace, which adds a
supply surface and a second place for allocator code to live. The safe decay
write reaches the same jemalloc purge routine.

**Blocking `try_grow` until the gate opens.** Rejected in section 3: it parks
runtime workers inside queries that hold reservations, which is hold-and-wait
on the threads whose drops would open the gate.

## Consequences

The gate now closes on host available memory below a floor, and purges at
most once per minimum interval (see the host-memory amendment below); the
consequences that rest on the mark and on a purge per interval read with
that change. It closes only once several consecutive samples are below the
floor, so a dip shorter than that refuses nothing (see the
consecutive-samples amendment below).

More 422s under bursts: a statement that arrives while the gate is closed
waits up to 2 s and is refused, and a running statement whose reservation
lands while it is closed spills or fails. On the measured workload the
current code is expected to purge and not refuse (section 2); a workload whose
live heap really reaches the mark gets refusals instead of an OOM kill, which
is the point.

The fetch path's memory refusals and the catalog decode refusal
(`CatalogError::MemoryExhausted`) move from 503 to 422. A client that retried
on 503 for these stops retrying; the message says the memory budget is
exhausted rather than that storage is unavailable. This is a client-visible
status change and goes in the changelog.

Costs: one relaxed load per reservation and per `grow`; one sampler thread
with an epoch refresh per 100 ms; a purge that holds each arena's decay lock
for the duration of its own purge, at most once per interval while the mark
is exceeded. The purge duration is not measured yet and is pre-registered
below.

An `all`-mode process counts its ingest buffers inside `resident`, so heavy
ingest can close the gate on queries. The startup check keeps the ingest
ceiling under the mark (no longer: see the startup-check amendment below,
which stamps the ceiling and does not count it); it does not reserve room
for queries beside it.

### Pre-registered acceptance test

The host-memory amendment below replaces this test: the scaled run's VmRSS
band becomes a reported figure, not a pass criterion, and the acceptance is
the four criteria stated there, which task 6 (#2734) runs.

Scaled run: `--memory-budget-bytes` 8 GB, 20 ClickBench files, 10
connections, 32 GB host, gate at its defaults, three runs.

- Pass: in every run, peak VmRSS is at or below the budget plus 512 MiB
  (536,870,912 bytes), and no process is OOM-killed. Every run in the table
  above fails this band (the lowest peak is 8.82 GB), so the test
  discriminates.
- Expected, and a miss to investigate if not met: `ravel_memory_gate_purges_total`
  above 0 in every run; `ravel_memory_gate_refusals_total` at 0, with at most
  1 % of statements refused before the miss counts; p99 purge duration at or
  below 50 ms; epoch-refresh CPU below 0.1 % of one core; purge CPU (p99
  purge duration times the observed purge rate) at or below 25 % of one
  core; O (peak VmRSS minus peak `resident`) at or below 0.27 GB. The stamp
  reports the two CPU figures separately.
- Throughput, pre-registered against a gate-off arm: on one box with one
  binary and `--memory-budget-bytes` pinned, three interleaved pairs (ABABAB)
  of `--disable-memory-gate` against the defaults. The gate-on median qps
  must be within 5 % of the gate-off median; a larger loss is a miss even
  when every other criterion passes. The purge is expected to fire on most
  samples (section 2), and it is more aggressive than the 1 s decay, whose
  measured 10.9 % qps loss could not be told from noise, so this figure is
  asserted, not assumed.
- On a miss, check in this order: the run used the stated budget and the gate
  stamp read enabled at 70 %; O and G as measured against the 0.27 and
  2.48 GB used to derive the mark; for a throughput miss, the purge rate and
  duration; only then the mark itself.

M1-shaped run: 30 GB host, 27 GB budget, the workload that was OOM-killed in
3 of 3 attempts.

- Pass: 3 of 3 runs complete with no OOM kill, and peak VmRSS is at or below
  the budget plus 512 MiB.

Each report stamps the binary's main SHA, the `memory gate resolved` line,
the `memory_budget_bytes` resolution line, and the peaks of VmRSS, `resident`,
`allocated` and the accounted total, sampled at the same instant.

### The 1 s decay

It does not ship as a default. With the gate purging at the mark, the decay
setting changes median retention, which no longer decides survival. The
measurement that would settle it: on one box with the gate on in both arms
and `--memory-budget-bytes` pinned, five interleaved pairs (ABABABABAB) of
default decay against `dirty_decay_ms:1000`, comparing the median qps of each
arm against the spread between runs of the same arm. Ship the shorter decay
only if its median qps cost is at or below 3 % and the gate's purge or
refusal counts fall with it.

## Implementation tasks

Tasks 1 to 3 are written as the host-memory amendment below reshaped them:
the gate decides on host available memory against a close floor, purges
below a purge floor, and refuses with the `HostMemory` cause. Task 2 landed
first under the original mark (#2764) and was reworked under #2806.

**Task 1: the gate state and the refusal cause in `MemoryBudget`** (crates:
ravel-memory)

- The padded group of gate atomics (an open-or-closed flag, the last
  available reading and the floor) on `MemoryBudget`, written through one
  setter and read with relaxed loads.
- `try_reserve(n)` for n > 0 refuses while closed; `MemoryExhausted` gains a
  cause (accounted, or `HostMemory` with the available reading and the
  floor), and its `Display` names it. Accounted and gate refusal counts for
  the `reserve` site as atomics on the budget, read by the server's
  metrics. The `admission` count is not here: task 3 puts it in
  `ravel-query`.
- Acceptance: a closed gate refuses `try_reserve(1)` with the `HostMemory`
  cause and admits `try_reserve(0)`; an open gate behaves exactly as today
  across the existing tests; the counts move by one per refusal of each
  kind; `reserve_unchecked` is unaffected; a microbenchmark of
  `try_reserve` with the gate open stays within 5 % of the current figure.
  The crate stays std-only.

**Task 2: the sampler, the purge, flags, stamp and metrics** (crates:
ravel-server)

- The `ravel-memory-gate` thread and the forced purge in `mem_stats.rs`,
  through the safe `Access<isize>` decay write per arena, skipping `EFAULT`.
- Flags `--memory-gate-min-available-bytes` (the close floor),
  `--memory-gate-purge-available-bytes` (the purge floor),
  `--memory-gate-purge-min-interval-ms`, `--memory-gate-interval-ms`,
  `--memory-gate-wait-ms`, `--disable-memory-gate`; the startup check that
  the purge floor is above the close floor and both floors are below host
  memory; the `memory gate resolved` stamp with its `available_source`; the
  metric families of section 4 as the host-memory amendment names them.
  `--memory-gate-close-after-samples`, its stamp field and its counter came
  after (see the consecutive-samples amendment below).
- The available reading through an injected file reader: the smaller of
  `MemAvailable` and, under a finite cgroup limit, the cgroup's headroom
  (`limit - current + inactive_file`), decided again on every sample.
- Acceptance: in the binary's own test module, where jemalloc is the global
  allocator (beside `binary_runs_under_jemalloc`), freeing a 256 MiB
  allocation and running the purge lowers `stats::resident` by at least
  200 MiB and restores every arena's decay values to what they were; an
  available reading below the close floor closes the gate and one at or
  above it opens it; the purge counter counts, at most once per minimum
  interval while below the purge floor; the stamp reports each `source`;
  a purge floor not above the close floor, or either floor at or above
  host memory, is refused with a message naming both figures; the gate is
  off in gateway mode, on a fallback budget, and when `MemAvailable`
  cannot be read; the resident gauge after a purging sample holds the
  post-purge re-read; the reading takes whichever term is smaller, drops
  the cgroup term when its limit reads `max` or its usage files do not
  parse, and is unread only when neither term can be read.

**Task 3: the check sites** (crates: ravel-sql, ravel-query, ravel-server)

- `TenantDelegatingPool::grow` trips `CeilingBreach` while the gate is
  closed; `try_grow` surfaces the `HostMemory` cause, the host's available
  reading and the floor, in its `ResourcesExhausted` message.
- One async admission call in ravel-query that checks the gate, waits up to
  the wait and the deadline, then takes the concurrency permit; used by
  `QueryControls::admit` and the Flight SQL service. It owns the
  `site="admission"` count of `ravel_memory_gate_refusals_total`.
- Acceptance: with the gate held closed by a test setter, a SQL statement is
  refused at admission with 422 after the configured wait and not before; a
  statement whose gate opens during the wait runs; over Flight SQL, a request
  reaching each of the three former `try_admit` call sites in
  `crates/ravel-sql/src/flight/service.rs` is refused with the SQL pool's
  refusal status after the wait, and runs once the gate opens; a
  spill-capable query that meets a closed gate mid-run spills and returns
  the exact result; an infallible `grow` while closed fails the query with
  422, its message names the available reading and the floor, and it
  releases its reservations; each site's refusal counter moves by one.

**Task 4: the fetch and catalog-decode refusals answer 422** (crates:
ravel-sql, ravel-query, ravel-catalog, ravel-server)

- `FetchMemoryExhausted` and `CatalogError::MemoryExhausted` take the budget
  class and a memory-budget message on SQL HTTP, PromQL HTTP and Flight SQL.
- Acceptance: the existing
  `a_sql_{metrics,logs,spans}_fetch_over_the_process_budget_is_refused_and_the_process_keeps_serving`
  tests assert 422 and the new message; a PromQL fetch over the budget
  answers 422; a PromQL query whose catalog decode charge is refused answers
  422; a resident-cause decode refusal runs no eviction pass and leaves
  `decode_reserve_retries` unchanged; the server's status-mapping test pins
  the change; the changelog records the status change.

**Task 5: documentation** (docs only)

- The ADR-1170 amendment, the inline pointer in each named section and beside
  each retired 503 sentence (section 6); the five metrics
  in the metrics reference; the four flags in the generated configuration
  page.
- Acceptance: `scripts/guards/check-amendment-integrity.sh` passes with the
  new markers, and `python3 scripts/check_docs.py` passes.

**Task 6: the acceptance runs** (no crate)

- The scaled and M1-shaped runs above, pre-registered on the issue before the
  first run, with the stamps listed.
- Acceptance: the pass criteria above, quoted with their measured figures.

## Amendment (2026-10-10, #2730): the startup check compares the cache caps alone

<!-- amendment-applies: sections="2. A forced purge at the high-water mark, and the default mark|Consequences" pointer="startup-check amendment" -->
<!-- amendment-supersedes: phrase="the ingest buffer ceiling reach the mark" pointer="startup-check amendment" -->
<!-- amendment-supersedes: phrase="The startup check keeps the ingest ceiling under the mark" pointer="startup-check amendment" -->

Section 2 had the startup check add the `all`-mode ingest buffer ceiling to
the cache hard caps, and justified the check against the caps alone ("30 %
of the budget, well under 70 %"). The ingest term breaks that justification:
`--max-ingest-buffer-bytes` defaults to a fixed 512 MiB while the budget has
a 1 GiB floor, so at the defaults the check fires whenever
`0.30 B + 512 MiB >= 0.70 B`, which is every budget of 1.28 GiB or less
(2 GiB on a loopback store, where the caps are 45 %). A 2 GiB host, or any
host whose available memory resolves to the floor, would refuse to start a
default `all` process.

Decision: the check compares `memory_hard_caps_bytes` alone against the
mark. The ingest buffer ceiling is stamped on the `memory gate resolved`
line (`ingest_buffer_bytes`, with `ingest_buffer_in_mark_check=false`) and
not counted (gone, check and stamp fields, see the host-memory amendment
below). The ingest buffer has its own ledger in `ravel-ingest` and does
not pass through `MemoryBudget::try_reserve`, so a closed gate never
throttles it: in `all` mode heavy ingest can hold the gate closed, which
section 2's Consequences already state. The refusal message names the caps,
the mark and the budget.

## Amendment (2026-10-11, #2806): the gate closes on host available memory

<!-- amendment-applies: sections="1. A gate that reads `stats.resident` at a bounded rate|2. A forced purge at the high-water mark, and the default mark|4. Observability|6. ADR-1170: the sentences that change|Consequences|Pre-registered acceptance test|Implementation tasks" pointer="host-memory amendment" -->
<!-- amendment-supersedes: phrase="The percentage applies to the budget, not the host" pointer="host-memory amendment" -->
<!-- amendment-supersedes: phrase="default 70, accepted 1 to 100" pointer="host-memory amendment" -->
<!-- amendment-supersedes: phrase="ingest_buffer_in_mark_check=false" pointer="host-memory amendment" -->

### What was measured

Stock c6a.4xlarge hosts (16 vCPU, 32 GB), stock flags, ClickBench through
the fork's `ravel-v0.23.0` driver (#2730 comment 6098619649, issue #2806):

- Concurrent phase, 10 connections x 600 s, three phases: main 5652e791a
  pooled an error ratio of 0.787 (0.774 / 0.807 / 0.777) against 0.071
  (0.088 / 0.055 / 0.069) on v0.23.0, with server RSS about 20 GB and
  `MemAvailable` 9 to 10 GB. `ravel_memory_budget_refusals_total` stayed at
  0, so every refusal came from the gate. `ravel_memory_gate_high_water_bytes`
  read 20,144,038,297, and 148 of 174 purges took 0.25 to 1 s.
- Single statements on a warm server (#2795 comment 6099730067): from
  69ec39ba8 on, q33 and q34 (`GROUP BY "URL"`) are refused in 4 of 4 runs
  and q35 in 2 to 3 of 4, run alone with no concurrency, after the same
  process has run q19, q21 and q22; the parent commit answers all three.
  The driver's full sweep answers 43 of 43 only because it restarts the
  server before each statement.

The arithmetic, from the figures above and the tree. The mark and the
process limit in that comment come from two process starts, and the budget
is derived from available memory at each start, so they imply two budgets:
at the 70 % default a mark of 20,144,038,297 bytes is a budget of about
28.8 GB (two cache caps of 45 % on a loopback store, about 12.9 GB; a
remainder of about 15.8 GB), and the refusal's process limit of
16,049,231,054 bytes, which is `memory_remainder_bytes`, is 55 % of a
budget of about 29.2 GB (caps of about 13.1 GB, a mark of about 20.4 GB).
Taking the refusal's start throughout: the derived SQL tenant pool, capped
at 90 % of the remainder (`services/ravel-server/src/config.rs`), is about
14.4 GB, and the caches plus one tenant pool, about 27.6 GB, exceed the
20.4 GB mark by 7 GB. The other start's figures differ by a few hundred
megabytes and reach the same conclusion. A warm server running one heavy
statement therefore sits above the mark with 9 to 10 GB of host memory
free; the purge reclaims nothing, because those bytes are live, and the
gate stays closed in steady state.

The two 27 GB data points on this issue (#2633 comments 6099757615 and
6099845644), same host shape, `--memory-budget-bytes 27000000000`:

- Gate on at the defaults (stamp `high_water_bytes=18900000000`): the
  concurrent phase scored an error ratio of 0.880 (1,105 completed, 8,123
  errors); 7,819 of the 8,126 server-side refusals were fetch-memory
  refusals; peak VmRSS was 19.31 GB against the 18.9 GB mark, so the gate
  was closed for most of the phase, reached by accounted memory alone.
- Gate off (`--disable-memory-gate`): the kernel OOM-killed the server 34.4 s
  into the concurrent phase.

### Why not a mark on the process's resident size against host memory

A mark on RSS against a fraction of host memory does not separate the two
cases either (#2806 comment 6103639401): on the same host shape the
pre-gate server peaks at 28.3 to 29.3 GB RSS of 31.4 GB `MemTotal`, with
0.8 to 1.6 GB `MemAvailable`, in every concurrent phase, and is not killed;
the one run that was killed (the 27 GB budget with the gate off) is the one
that drove available memory to zero. The quantity that separates a server
the kernel kills from one it does not is host available memory.

### Why not resident minus accounted

Subtracting the bytes the ledgers account for from `resident` does not give
the uncharged figure: cache-hit bytes sit in both the cache ledger and the
fetch reservations, and `ravel_memory_handoff_overlap_bytes`, which counts
exactly those, reached 2.3 to 3.8 GB during the concurrent phase (#2806
comment 6100322587), so the subtraction double-counts in either direction
depending on which side is taken. The band such a figure would serve, the
budget plus 512 MiB, is not reachable on current code without refusing:
every run in the Context tables peaks above it (8.82 to 10.44 GB against an
8.54 GB band). Host pressure needs no subtraction.

### Decision

1. **The gate closes on host available memory**, not on `stats.resident`
   and not on the budget. Every `--memory-gate-interval-ms` the sampler
   reads the host's available memory and `stats.resident` (one epoch
   advance). Available memory is the smaller of two terms, over whichever
   of them can be read on that sample: `MemAvailable` from
   `/proc/meminfo`, and, when a finite cgroup memory limit is in effect,
   the cgroup's headroom, `limit - current + inactive_file` saturating at
   zero (v2 `memory.max`, `memory.current` and `memory.stat`'s
   `inactive_file`; v1 `memory.limit_in_bytes`, `memory.usage_in_bytes`
   and `total_inactive_file`). That is the kubelet's working-set figure:
   `memory.current` charges page cache, which the kernel reclaims under
   pressure, so the limit minus it alone reads toward zero on a healthy
   container whose cache or spill directory has filled the page cache,
   and a limit above physical memory or an overcommitted node reads high
   while the host runs out, which the `min` with `MemAvailable` catches.
   The limit is read on every sample, not once at startup: `memory.max`
   reading `max`, or a usage or stat file that does not parse, drops the
   cgroup term for that sample and `MemAvailable` stands alone. The
   `/proc/meminfo` parser is the one `config.rs` uses for the budget
   derivation (ADR-1170, available-memory amendment); the `memory.current`
   and `memory.stat` readers are new, behind an injected file reader so
   the reading is tested without `/proc`. The gate is closed when the
   sample's final available reading is below the close floor (see the
   consecutive-samples amendment below: one sample no longer closes it),
   `--memory-gate-min-available-bytes`, and open otherwise; the budget's
   gate setter is written on every successful sample. The default close
   floor is `min(max(512 MiB, 2 % of host memory), host memory / 8)`,
   where host memory is `MemTotal` capped by the cgroup limit, the figure
   the performance defaults resolve. From 4 GiB up the clamp does not
   bind, since an eighth of host memory is at or above 512 MiB there; on a
   32 GiB host the floor is 687,194,767 bytes. Below 4 GiB the floor is an
   eighth of host memory: 134,217,728 bytes on 1 GiB, 67,108,864 on
   512 MiB, 33,554,432 on 256 MiB. The flag accepts 64 MiB and up; that
   minimum applies to a flagged value only, and a derived floor below it
   is accepted, so a host ADR-1170's small-host amendment (#2607) keeps
   bootable is not refused by the gate.
2. **The purge runs at a higher floor, rate-limited.** When the available
   reading is below `--memory-gate-purge-available-bytes` (default
   `min(2 x close floor, host memory / 4)`: 1,374,389,534 bytes on a
   32 GiB host, 268,435,456 on 1 GiB, 134,217,728 on 512 MiB, 67,108,864
   on 256 MiB; a flagged value must be above the close floor) and at least
   `--memory-gate-purge-min-interval-ms` (default
   5000, accepted 100 to 600000) has passed since the previous purge
   started, the sampler purges as section 2 describes, per arena, and
   re-reads both figures before deciding the gate. A sample below the close
   floor whose purge is rate-limited closes on the reading it has. This
   replaces section 2's purge on every sample at or above the mark: the
   purge rate is at most one per minimum interval. `now` for the spacing is
   an injected clock parameter of the sample function, so the rate is
   tested without sleeping.
3. **`stats.resident` decides nothing.** It is still read on every sample,
   stays the `ravel_memory_gate_resident_bytes` gauge, and is what the
   purge still acts on. If neither available-memory term can be read on a
   sample the gate keeps its last state and the sampler warns once, as it
   does for an unreadable resident. A sample whose terms differ from the
   `available_source` resolved at startup (a cgroup limit that has gone
   to `max`) is a reading, not an unread sample; the sampler warns once
   about the change, and `ravel_memory_gate_available_bytes` carries what
   was read.
4. **Flags.** `--memory-gate-high-water-percent` is removed; it landed in
   #2764 the day before this amendment and no release carries it.
   `--memory-gate-min-available-bytes`, `--memory-gate-purge-available-bytes`
   and `--memory-gate-purge-min-interval-ms` are added as above.
   `--memory-gate-interval-ms` and `--memory-gate-wait-ms` keep their
   meaning.
5. **`ravel-memory`.** The gate state keeps the open flag plus two figures,
   now `available` and `floor`; the setter is `set_host_memory_gate`;
   `ExhaustionCause::Resident { resident, high_water }` becomes
   `HostMemory { available, floor }`, with a `Display` of the form "memory
   exhausted: host available memory N bytes is below the M byte floor
   (requested n)". The cause stays `Copy` and string-free. The two relaxed
   stores stay consistent because the floor is fixed for the process's
   life. The Flight SQL wait and the admission site (task 3, #2731) read
   the same open flag unchanged.
6. **Startup.** The off modes and their stamp sources stay, plus
   `disabled-no-host-memory` when `/proc/meminfo`'s `MemAvailable` or
   `MemTotal` cannot be read: without the first the sampler has nothing
   to decide on, without the second there is no host memory to take the
   default floor of. The resolve checks flagged floors only: it refuses a
   flagged close floor at or above host memory, a flagged purge floor not
   above the close floor, and a flagged purge floor at or above host
   memory (every reading would be below it and the purge would run at
   every minimum interval for the process's life). A derived purge floor
   is above a derived close floor on every host size, since the close
   floor is at most an eighth of host memory and the purge floor is twice
   it up to a quarter; with a close floor flagged at or above a quarter of
   host memory the derived purge floor is not above it, and the resolve
   refuses with `PurgeFloorNotDerivable`, whose message says to pass
   `--memory-gate-purge-available-bytes` explicitly. That is the only
   refusal a derived purge floor can produce, and it needs a flagged close
   floor; a derived configuration never refuses. The cache-caps check of section 2
   and the startup-check amendment is dropped: it compared the caps with a
   mark on the process's own memory, which no longer exists, and the
   stamp fields it left (`ingest_buffer_bytes`,
   `ingest_buffer_in_mark_check`) go with it. The `memory gate resolved`
   stamp carries `enabled`, `host_memory_bytes`, `min_available_bytes`,
   `purge_available_bytes`, `purge_min_interval_ms`, `interval_ms`,
   `wait_ms`, `memory_budget_bytes`, `available_source` (`meminfo+cgroup`
   when the host profile carries a finite cgroup limit, else `meminfo`)
   and `source` (and `close_after_samples`, since the consecutive-samples
   amendment below).
7. **Metrics.** `ravel_memory_gate_high_water_bytes` is renamed
   `ravel_memory_gate_available_floor_bytes` (the close floor);
   `ravel_memory_gate_available_bytes` (the last available reading) and
   `ravel_memory_gate_purge_floor_bytes` are added;
   `ravel_memory_gate_resident_bytes` stays the last resident reading. The
   refusal counters and the samples, epoch-refresh and purge families are
   unchanged.

### What this supersedes

Section 2's default derivation and its 70 % figure, its mark on the budget
rather than the host, and its startup check; section 1's reading of
`stats.resident` as the figure that decides the gate; section 4's gauge
names and stamp fields; and the Consequences' purge cost of at most once per
interval, which becomes at most once per minimum interval while below the
purge floor. An `all`-mode process's ingest buffers still count, now in the
host's used memory, so heavy ingest can still close the gate on queries.
The pre-registered acceptance test below replaces the one above: the 8 GB
scaled run's VmRSS band becomes a reported figure, not a pass criterion.
Section 6's ADR-1170 amendment text (the block task 5 appends) is rewritten
to this gate and this acceptance: it no longer describes a gate on
`stats.resident` at a mark, and its acceptance is criteria (a) to (d) below
rather than a VmRSS band; section 6's items 1 and 2 carry the same change.

### Pre-registered acceptance (task 6, #2734)

- (a) ClickBench concurrent on a stock c6a.4xlarge pools at most 0.10 error
  ratio (v0.23.0: 0.071).
- (b) q33, q34 and q35 answer on a warm server.
- (c) At a 27 GB budget on a 32 GB host no process is killed, and the
  gate's refusals carry the host-memory cause.
- (d) Purges during a 600 s concurrent phase number at most
  600 / purge-min-interval-seconds + 1 (121 at the 5 s default).

Each report stamps the binary's main SHA, the `memory gate resolved` line
and the peaks of VmRSS and `resident`, with the minimum of
`ravel_memory_gate_available_bytes`, sampled at the same instant.

## Amendment (2026-10-11, #2838): the gate closes after consecutive samples below the floor

<!-- amendment-applies: sections="1. A gate that reads `stats.resident` at a bounded rate|4. Observability|Consequences|Implementation tasks" pointer="consecutive-samples amendment" -->
<!-- amendment-supersedes: phrase="closed when the sample's final available reading is below the close floor" pointer="consecutive-samples amendment" -->

### What was measured

The task 6 acceptance run on main 1590f945f (#2806 comment 6105829771:
paired fresh c6a.4xlarge boxes, fork `ravel-v0.23.0`, three 600 s
concurrent phases on each): criteria (b), (c) and (d) passed, and criterion
(a) missed on one phase. The stock box pooled an error ratio of 0.207
(0.070 / 0.074 / 0.379) against the 0.10 bar; the 27 GB-budget box pooled
0.092. In stock phase 3 the gate's own available gauge dipped to 0.53 GB,
below the 0.66 GB floor, while the 2 s host samples of `MemAvailable` never
went under 1.22 GB: the dips lasted well under 2 s. While the gate was
closed, the phase's server log holds 47 fetch refusals with 0 bytes already
reserved (the gate's signature) and 132 "upstream storage temporarily
unavailable" errors. v0.23.0, with no gate, dipped to 0.83 GB available at
an error ratio of about 0.07 and was not killed, so the 0.66 GB floor sits
below the healthy operating point and the gate was closing on transients.

### Decision

1. **The gate closes only after consecutive samples below the floor.**
   `--memory-gate-close-after-samples` (default 3, accepted 1 to 100) is
   how many consecutive samples must read below the close floor,
   `--memory-gate-min-available-bytes`, before the sampler closes the gate.
   A sample at or above the floor resets the run to 0 and, if the gate is
   closed, reopens it on that sample: the close is debounced, the reopen is
   not. An unread sample neither counts nor resets, as it leaves the gate's
   state alone. With the value 1 the behaviour is the one the host-memory
   amendment's decision 1 describes, exactly. The purge decision is
   unchanged; it has its own spacing.
2. **Why 3 at 100 ms.** At the default interval a dip must last at least
   300 ms to close the gate. The dips measured above were shorter than the
   host sampler's own 2 s period and closed the gate for a whole phase,
   while v0.23.0 ran at 0.83 GB available with an error ratio of about
   0.07. Three samples is the shortest run that outlasts a dip of one or
   two readings, and at section 1's measured 0.4 GB/s burst slope it lets
   about 120 MB of growth go unseen before the close, small against the
   687 MB floor on the 32 GiB host. The count is a flag, so a host whose
   dips are longer can raise it, up to 100 (10 s at the default interval).
3. **The setter is written on every readable sample**, with the gate's
   decision and the sample's reading, so `ravel_memory_gate_available_bytes`
   shows a reading below the floor while the gate is still open. For that,
   `MemoryBudget::set_host_memory_gate` takes the open flag from the
   sampler instead of deriving it from the reading and the floor. A closed
   gate still always comes with a reading below the floor, which the
   refusal path's re-check rests on.
4. **Observability.** `ravel_memory_gate_samples_below_floor_total`,
   counter: every sample whose final reading was below the close floor,
   whether or not it closed the gate; this moving while
   `ravel_memory_gate_refusals_total` does not is a dip too short to close
   the gate. The `memory gate resolved` stamp gains `close_after_samples`.

### What this supersedes

The host-memory amendment's decision 1, where one sample below the close
floor closes the gate; with `--memory-gate-close-after-samples 1` that is
still the behaviour. Nothing else moves: the floors, their defaults, the
purge and the refusal cause stand.

### Pre-registered acceptance (criterion (a) re-run)

Criterion (a) of the task 6 acceptance (the host-memory amendment above) is
re-run on this change, by ravel-perf: paired fresh c6a.4xlarge boxes, this
change's head against a main control, three concurrent phases each plus the
stock run. The pooled error ratio is at most 0.058, no phase is over 0.08,
and QPS is not below the control. Criteria (b), (c) and (d) are unchanged.
