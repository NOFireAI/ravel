# ADR-0065: leased distributed maintenance

Status: Accepted

## Context

This ADR covers three gaps: maintenance does not distribute (no lease, no
durable incremental cursor, full-rescan cost linear in the retention
window, and two replicas double-pay rather than partition), and log
compaction memory is unbounded in input size. The acceptance is two
maintain replicas partitioning the work rather than both paying for it,
plus a bounded-memory assertion on a synthetic large-input RLOG
compaction.

Every gap was re-verified against current `main` before this design.
Earlier notes were stale on two points and current on the core one; the
code shows:

**Current: no distribution, no lease.** ADR-0048 rebuilt the maintain
driver as a single supervisor loop
(`services/ravel-server/src/maintain.rs`): every tick (default 5 min) it
re-discovers tenants from storage (`discover_tenants`, one delimited LIST of
`t/`), then sequentially, per tenant, refreshes legal holds and walks every
`(signal, shard)` through `scan_and_maintain_with_memo` plus `sweep_shard`.
Nothing anywhere partitions this: a second replica runs the identical loop
over the identical world. The only cross-process coordination is convergence
— compaction serializes at the record's `CreateIfAbsent` (the loser's parts
age out), and the advisory CAS cursor (`t/<hash>/<sig>/maint/<shard>/cursor`,
ADR-0018, used only by the CLI-facing `scan_and_compact` path, deliberately
not by `scan_and_maintain`) dedups nothing but cursor writes. Write races are
resolved; read cost is not: each extra worker pays the full listing and
evaluation cost again. The single supervisor is also an availability
floor — the operator deploys maintain as a single-replica Recreate Deployment
(ADR-0034), and when it is down nothing compacts fleet-wide.

**Partially stale: "no incremental cursor".** Earlier changes added
`MaintainMemo` (`crates/ravel-maintain/src/scan.rs`): an in-memory, per-
process, never-correctness-bearing memo of terminal buckets (compacted-and-
not-expired, below-threshold, swept-empty), skipped without per-bucket
LIST/GETs until an hourly re-verify. This cut the steady-state tick cost
materially, but three costs remain linear in the retention window or reset to
linear on restart:

- The memo is ephemeral. Every worker restart is a cold full rescan, and in a
  k8s world restarts are routine. Ownership movement between replicas (which
  this ADR introduces) would likewise start cold without durable state.
- The hourly re-verify re-pays the full per-bucket read cost for every
  terminal bucket in the retention window, every hour, even though an
  interior bucket's only possible future transition (retention expiry) is
  computable in advance from its hour and the policy.
- `sweep_shard` is deliberately cursorless and full-scan
  (`crates/ravel-maintain/src/sweep.rs`): all three rules list the shard's
  whole keyspace every tick. After ADR-0048 the orphan re-verify is batched to one
  LIST per pass, but the pass itself still walks every retained hour, every
  five minutes.

**Partially stale: "RLOG compaction is not streamed".** An earlier change landed
`RlogRangeReader` and rebuilt the merge (`crates/ravel-maintain/src/rlog.rs`)
to retain only per-input directory sections and fetch block bytes by range,
one stream at a time. The residual is now RSPAN, not RLOG
(`rspan_codec.rs:161` still does `GetRange::Full` per input).
But RLOG's bound is not yet a real bound, and one high-volume log stream
in one sealed hour still defeats it:

- `gather_stream` materializes one stream's records *from every input at
  once* into a `Vec<LogRecord>` before sorting. One hot stream (a single
  busy service — the common case for logs) can carry most of the hour;
  peak memory is unbounded in stream size, which is unbounded in input size.
- `build_parts` then accumulates up to a full part of *decoded* records in a
  second `Vec` before pushing them into the writer, roughly doubling
  residency (decoded records are larger than their encoded estimate).

So the deliverable's real shape today is: distribution and leasing from zero;
the incremental cursor from a good in-memory prototype to a durable,
schedule-aware design; RLOG streaming from "streamed per stream" to "bounded
regardless of stream shape", with a test that proves it.

**The established coordination pattern.** ADR-0057 (fleet-global admission)
introduced this codebase's cross-process coordination primitive: each process
periodically writes a snapshot to a key it alone ever writes
(`PutMode::Overwrite`, no CAS, no contention), reads non-stale siblings
(staleness = 2x the write interval, judged by the reader's clock), and
derives a local decision from the merged view. ADR-0061 reuses it for
query concurrency. Its properties fit maintenance even
better than admission: maintenance work is idempotent and convergent (every
pass is safe to run twice; `CreateIfAbsent` picks winners; deletes are
horizon-gated), so a bounded coordination-staleness window costs duplicated
*work*, never correctness — exactly the tradeoff the pattern makes.

**Naming hazard.** `ravel-maintain` already has a `LeaseCheck` trait
(`sweep.rs:88`): it is the GC *reader-lease* protection gate (is this key
protected from deletion), wired to legal holds by ADR-0048. It is unrelated to
worker coordination. This ADR deliberately does not name its mechanism
"lease" in code to avoid colliding with it (see Decision 1).

No frozen persistent *data* format is touched. The additions are two new
mutable control-plane key prefixes under `sys/maintain/` — additive key-
layout changes made by this ADR exactly as ADR-0057 added
`t/<hash>/<sig>/admission/`; docs/catalog-and-mvcc.md's key-layout section is
updated in the same commit that lands them. Payloads are versioned-tag
protobuf, advisory, and reconstructible; losing them costs a rescan, never
correctness (the ADR-0003 HEAD-pointer precedent, same as the existing
cursor).

## Decision

### 1. Worker membership via self-owned heartbeat keys

Each maintain-role process writes, every heartbeat interval `H` (default
60 s), a snapshot to a key it alone ever writes:

```
sys/maintain/workers/<process_id>
```

`process_id` is a UUID generated at startup (the ADR-0057 convention).
Payload: a versioned-tag protobuf `{process_id, started_unix_ns,
heartbeat_unix_ns}`, written `PutMode::Overwrite` — self-owned, no CAS, no
contention. On the same cadence each process lists `sys/maintain/workers/`
and GETs siblings; the **live set** is itself plus every sibling whose
`heartbeat_unix_ns` is within `3 * H` of the reader's own clock (one factor
wider than ADR-0057's `2 * R` to absorb skew plus write jitter; the exact
factor is a config knob, not a contract). A stale worker is treated as gone —
its work is taken over, and if it was merely slow, the overlap is idempotent
(the same fail-open direction ADR-0057 chose, for the same reason: a slow
heartbeat anywhere must not freeze maintenance everywhere).

This is deliberately not called a lease in code (`WorkerSet`,
`sys/maintain/workers/`): the existing `LeaseCheck` trait is the GC
reader-protection gate and the two concepts must not blur. What a
single-writer lease asks for — one owner per unit of work in steady state,
automatic takeover on death, no double-pay — is delivered by Decision 2 on
top of this membership view.

### 2. Work partitioning: rendezvous hash over live workers

The unit of ownership is `(tenant_hash, signal, shard)` — the exact unit
`run_tick` already iterates, and the granularity at which listing cost is
paid. For each unit, every worker independently computes

```
owner(unit) = argmax over w in live_set of blake3(unit_key || w.process_id)
```

and the supervisor's discovery cycle simply skips units it does not own.
Rendezvous (highest-random-weight) hashing needs no coordination state, no
assignment object, and no leader; every worker with the same live set
computes the same owner, and a membership change moves only the departed or
arrived worker's units. With one replica the live set is `{self}` and
behavior is byte-for-byte today's.

Ownership gates *all* per-unit maintenance work in the maintain role: the
retention-and-compaction tick, `sweep_shard`, the idempotency-marker sweep
(per `(tenant, signal)`, owned by the owner of shard 0 of that pair), and the
ADR-0059 scrub rotation (`services/ravel-server/src/scrub.rs`), which today
also double-pays across replicas. Tenant discovery (one delimited LIST) and
the legal-hold refresh stay per-process for the tenants a worker owns any
unit of: the hold refresh is small (a control-plane shard LIST plus GETs) and
duplicating it at most `min(replicas, signals x shards)` times per tenant is
cheaper and simpler than a hold-snapshot handoff protocol.

**Why correctness survives overlap.** During a membership transition (at most
`3 * H` plus one heartbeat), two workers may both believe they own a unit.
Every operation they can both run is already concurrency-safe by prior
design: compaction converges at `CreateIfAbsent` (ADR-0018), sweeps are
idempotent and horizon-gated, retention tombstones are `CreateIfAbsent`,
scrub is read-only, and the conservation gate and orphan breaker (ADR-0048)
are per-pass. The transition window costs bounded duplicate reads — the
steady state costs none, which is what the partition acceptance measures.

**Stuck-owner hazard and mitigation.** Membership is process-level liveness:
a live-but-wedged worker starves its own units. Mitigations, in this ADR's
scope: (a) the supervisor runs its owned units with bounded intra-process
concurrency (default 4 units in flight) instead of today's strictly
sequential walk, so one pathological unit cannot starve the rest of the
process's ownership; (b) a stalled-units gauge
(`ravel_maintain_units_stalled`, counting owned units with no successful tick
in `k` intervals — a per-unit gauge would exceed the metrics label
allowlist) with a shipped alert rule. A per-unit work-level lease with
takeover is rejected below.

### 3. Durable incremental cursor: per-worker maintain-state snapshot

Each worker persists, every tick (debounced: skipped when unchanged), a
compact summary of what it has verified, to a second self-owned key:

```
sys/maintain/memo/<process_id>
```

Only terminal buckets (compacted / below-threshold / swept-empty) are
recorded; a bucket still doing work carries no entry. Adjacent hours in the
same terminal state collapse to one run, whose `verified_unix_ns` is the
minimum over the run (conservative for freshness). Payload (versioned-tag
protobuf), per owned `(tenant, signal, shard)`:

- a `frontier` run: the **longest contiguous same-state terminal run** in the
  unit (ties broken toward the highest end hour), encoded as
  `(start_hour, end_hour, state, verified_unix_ns)`. The interior of a
  retention window is one such run, which is where the compression comes from.
- an exception list of the unit's **other terminal runs** — the terminal
  spans that fall outside the frontier run — each encoded as
  `(start_hour, length, state, verified_unix_ns)`.

Run-length encoding against the frontier keeps this KBs per unit (the
interior of a retention window is one run), so the snapshot stays a small
single-PUT object even at large retention.

**Warm start and handoff.** On startup — and whenever ownership of a unit
arrives via a membership change — a worker reads all non-stale memo
snapshots (its own previous one and siblings'), and seeds its in-memory
`MaintainMemo` from the freshest entries for the units it now owns. The memo
stays exactly as advisory as today: entries only suppress re-reads inside the
freshness rules below, are re-verified on schedule, and a lost, stale, or
corrupted snapshot degrades to the cold rescan we do unconditionally today.

**Zone scheduling replaces the flat hourly re-verify.** A unit's hours fall
into three zones with different change dynamics:

- **Head** (hours newer than the seal margin plus one hour of slack): new
  L0 objects, sealing, first compaction. Evaluated every tick, as today.
- **Tail** (hours inside the retention-expiry window widened by the
  protection horizon): tombstoning and physical sweep. Evaluated every tick.
- **Interior** (below the frontier, outside the tail): nothing can change a
  terminal interior bucket except a future retention expiry (computable:
  `hour + retention_window`), an operator action (tombstone, hold), or a
  future erasure order (ADR-0064). Terminal interior buckets are re-verified at
  their computed expiry time when a retention policy exists, and otherwise
  on a slow full re-verify cadence, default 6 h (config
  `maintain_interior_reverify`, replacing the flat 1 h memo interval for
  this zone; head/tail keep tick-cadence evaluation).

`sweep_shard` is split on the same zones: the per-tick sweep lists only head
and tail hour prefixes (the keys are hour-bucketed, so
`commit_shard_hour_prefix` scopes each LIST), which is where orphans (age
gate = grace + max flush lifetime), superseded records (post-compaction), and
unreferenced parts (raced/abandoned runs) actually appear; a full-keyspace
sweep pass — today's exact behavior — runs on the slow cadence as the safety
net for interior stragglers and operator actions. All deletion remains
horizon-gated, so this changes promptness (documented in
docs/consistency-model.md's "Deletion and GC" and the operations guide),
never safety. The breaker and batched re-verify (ADR-0048) apply unchanged to
both sweep shapes.

**Invalidation hook.** `MaintainMemo` gains a public
`invalidate(tenant, signal, shard, hours)` seam that forces named buckets out
of terminal state for immediate re-evaluation. Nothing in this ADR calls it
except tests; it exists because selective-deletion (ADR-0064) work orders will
rewrite interior buckets and must not wait out the slow cadence (see
Consequences).

**Cost shape after this decision.** Steady-state per tick per unit: one
delimited hour LIST plus O(head + tail) bucket evaluations, instead of
O(retention-window) sweep listings; the interior is paid once per slow
cadence instead of hourly; the whole fleet divides the unit set by replica
count instead of multiplying cost by it; and a restart or handoff warm-starts
instead of rescanning. Requests per day stop growing linearly with the
retention window at tick cadence — the linear term survives only at the
slow-cadence full pass, which is the safety net, not the steady state.

### 4. RLOG compaction: a real memory bound

The merge in `crates/ravel-maintain/src/rlog.rs` is restructured from
"materialize one stream, sort, batch a part" to a k-way block-streaming
merge:

- `ravel-logseg`'s `RlogRangeReader` gains block-granular iteration within a
  stream's span: records inside one stream are already ts-ascending in the
  format (objects are sorted `(stream_ref, ts)`), so each input yields its
  stream records as an ordered sequence with exactly one decoded block
  resident per input.
- `gather_stream`'s whole-stream `Vec` is replaced by a k-way merge across
  the inputs carrying the stream, ordered by `ts_ns` with ties broken by
  canonical input order — bit-for-bit the ordering the current stable sort
  produces, so output objects are unchanged.
- Merged records are pushed directly into the `RlogWriter` (the intermediate
  per-part `Vec<LogRecord>` batch is deleted); the part still splits on a
  stream boundary at the size cap, and the writer's own block encoding keeps
  decoded residency to one in-progress block.

Peak resident memory becomes: per-input retained directories (STREAM_DIR /
FIELD_DIR / SKIP_IDX — KBs per input, the already-shipped ranged-reader footprint),
plus one decoded block per input carrying the current stream, plus the
writer's one in-progress part (bounded by `max_l1_part_bytes`, needed anyway
because the L1 key is content-addressed, so the object must be complete
before its key exists). The bound is independent of stream size and record
count; the surviving input-count term is directory metadata only, and is
stated, not hidden.

**Acceptance test.** A `MergeMemoryTracker` seam (test-injectable, default
no-op) accounts fetched block bytes, decoded-block residency, and the
writer's buffer estimate at each merge step and records the high-water mark.
The synthetic test builds one bucket whose single hot stream grows 10x across
runs and asserts the tracked peak stays under a fixed bound derived from
block size, input count, and `max_l1_part_bytes` — failing loudly if anyone
reintroduces whole-stream materialization. This is deterministic (no
allocator hooks, no flaky RSS reads) and runs against `MemoryStore`.

**Partial failure and resume.** Unchanged, deliberately: parts are
content-addressed and `CreateIfAbsent`; a crashed run restarts the bucket
from scratch and converges (plan §3.6). Streaming adds no durable mid-part
state — a resume cursor for a half-built part would introduce a new mutable
state class to save re-reads that occur only on crash.

**RSPAN residual, explicitly out of scope.** `rspan_codec.rs` still fetches
each input whole (one at a time); this is the remaining unbounded-memory
residual. It is not covered by this ADR's acceptance (which names RLOG)
and needs RSPAN ranged-reader format work plus trace-boundary-aware
splitting; filed as a named follow-up, not silently absorbed here.

## Rejected alternatives

1. **A bucket-keyed work queue with per-unit CAS TTL leases** (the issue
   deliverable's literal shape: a lease object per unit, acquired
   `CreateIfAbsent`, renewed by `CasVersion`, expired by TTL). Lost on four
   concrete points. (a) It does not remove enumeration: workers must still
   discover the unit set to know which leases to attempt, so a partitioning
   function is needed anyway — and once you have one, the lease object is
   redundant in steady state. (b) Its request cost is a GET (and periodic
   PUT) per unit per tick, a new per-tick cost linear in the unit count —
   the exact cost class this ADR exists to remove. (c) TTL expiry makes a
   coordination decision out of comparing another process's clock against
   the reader's on a per-object basis, a clock assumption
   multiplied across every unit; the membership design confines that
   comparison to one heartbeat object per worker. (d) It adds a new
   contended mutable-key class (every worker CASing the same lease keys),
   which ADR-0051 and ADR-0057 both deliberately rejected for coordination.
   What the lease buys over rendezvous — per-unit takeover from a
   live-but-wedged worker — is mitigated more cheaply by bounded
   intra-process concurrency plus the stalled-units alarm (Decision 2), and
   correctness never depended on it.

2. **Leader-elects-and-assigns** (one leader lease; the leader writes an
   assignment object mapping units to workers). Lost because it reintroduces
   the exact availability floor this ADR removes — no leader, no
   assignment, no maintenance — behind one more election protocol and one
   more mutable object to fight over. Rendezvous hashing computes the same
   assignment with zero shared state and no privileged process.

3. **One shared durable memo, CAS-updated by all workers**, instead of
   per-worker self-owned snapshots. Lost because it is a contended mutable
   object updated every tick by every worker — the write pattern ADR-0057
   was specifically designed to avoid — and a lost CAS race either drops
   memo updates (wasted work) or retries (contention scaling with fleet
   size). Self-owned snapshots merged at read time give the same warm-start
   information with zero contention, at the cost of reading N small objects
   on startup and handoff, which is when you are cold anyway.

4. **Keep the memo ephemeral and accept cold rescans** (status quo plus
   partitioning only). Lost because restart cost stays linear in the
   retention window at the worst moment (a rolling deploy restarts every
   worker), and because ownership handoff — which partitioning makes routine
   — would go cold on every membership change, turning each scale event into
   a fleet-wide rescan. The durable snapshot is small, advisory, and
   reuses an existing pattern; the cost asymmetry is decisive.

## Consequences

- **On ADR-0048:** builds directly on its supervisor: discovery,
  restriction flags, hold refresh, breaker, and conservation gate are
  unchanged in logic; the discovery cycle gains an ownership filter, the
  sweep gains zone-scoped scheduling around the same rules, and the hold
  refresh is duplicated per owning replica (bounded, documented). The
  `LeaseCheck`/worker-coordination naming split is documented in both
  modules.
- **The partition acceptance becomes a test**, not an experiment: two supervisors over one
  counting `MemoryStore` partition the unit set disjointly, cover it
  completely, and the per-unit request counters show no steady-state
  double-pay; kill one and its units are taken over within the staleness
  window. The bounded-memory RLOG assertion is Decision 4's test.
- **Deployment model changes:** the maintain role may now run N replicas.
  ADR-0034's single-replica Recreate guidance is superseded; the k8s
  operator needs a follow-up to expose maintain replicas, and
  the operations guide documents the new scaling knob and alert rules.
- **New control-plane keys** `sys/maintain/workers/<process_id>` and
  `sys/maintain/memo/<process_id>`: mutable, self-owned, versioned-tag,
  advisory. docs/catalog-and-mvcc.md's key layout and ADR-0055's role/grant
  templates are updated in the landing commits (maintain role: write on
  `sys/maintain/`). Snapshot lifecycle follows ADR-0057 §5: stale objects
  are excluded by staleness, cleanup is a future bounded sweep if it ever
  matters.
- **Promptness bounds change and are documented:** interior-zone re-verify
  moves from 1 h to a 6 h default (configurable); operator tombstones and
  holds on interior buckets take effect within the slow cadence rather than
  one hour. Head and tail behavior — everything time-critical — keeps tick
  cadence. docs/consistency-model.md "Deletion and GC" and the operations
  guide are updated in the same commit as the scheduling change.
- **On multi-part fold (ADR-0063, ravel-catalog):** no file or premise
  overlap — fold's CAS pointer and snapshot layout are untouched here, and
  this ADR's crates (`ravel-maintain`, `ravel-logseg`, `ravel-server`
  driver files) are disjoint from it. The fold loop remains undistributed;
  it can adopt the same `WorkerSet` ownership filter later (follow-up, not
  this ADR). No ordering dependency either way.
- **On selective deletion (ADR-0064):** a real ordering interaction. Its
  erasure work will rewrite interior buckets, breaking Decision 3's
  "interior is inert" scheduling assumption; the `invalidate` hook is the
  seam it must call, and its erasure jobs should run *on* this ADR's
  ownership partition rather than growing their own coordination.
  Recommendation recorded here: ADR-0064 lands after this and its ADR names this
  hook.
- **On WORM (ADR-0055):** the new keys (and the existing advisory cursor)
  are mutable and must sit outside any WORM-protected prefix; whichever ADR
  lands second addresses the other.
- **On the two-writer race** (two workers observing different input sets producing two
  compaction records): steady-state single ownership makes this
  near-impossible rather than merely convergent — a side benefit, not a
  correctness change; the resolver's widening behavior stays.
- **Reconciliation, reported:** the "RLOG compaction is not streamed"
  framing predates the streaming change; the shipped gap is the
  hot-stream materialization (fixed here) and the RSPAN whole-object
  residual (follow-up at landing).
- **New metrics** on the existing `/metrics` endpoint:
  `ravel_maintain_workers_live`, `ravel_maintain_units_owned`,
  `ravel_maintain_units_stalled`, `ravel_maintain_memo_warm_start_units`,
  `ravel_maintain_full_sweep_passes_total`, plus the RLOG merge peak-bytes
  gauge from the tracker seam. Shipped alert rules: `workers_live == 0` in
  a maintaining mode, `units_stalled > 0` sustained.
