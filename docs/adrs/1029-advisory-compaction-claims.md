# ADR-1029: advisory compaction claims over object-store CAS

Status: Accepted (2026-09-01, PR #1031; see the 2026-09-28 amendment)

## Context

Compaction is coordinated for correctness but not for cost. The publish
path serializes racing runs at the compaction record's `CreateIfAbsent`
(`crates/ravel-maintain/src/publish.rs:209`), so two processes that
compact the same sealed bucket always converge on one record; the loser
pays the full merge and discards it. One full-tenant logs compaction on
the 100M-row ClickBench corpus measured 16,224 s of wall clock and 3,459
part PUTs (issue #968 ledger); a duplicated run pays all of it again for
zero durable effect.

What already prevents duplicates, and what does not:

- ADR-0065 decisions 1 and 2 are landed. The background supervisor gates
  every `(tenant, signal, shard)` unit on rendezvous ownership over the
  heartbeat live set (`services/ravel-server/src/maintain.rs:1116-1123`,
  `crates/ravel-fleet/src/worker_set.rs`). In steady state, background
  replicas do not duplicate discovery or merges. This ADR does not touch
  that mechanism.
- Three windows remain where duplicate merges happen anyway:
  1. `ravel-cli maintain compact-tenant` calls `compact_bucket` directly
     (`services/ravel-cli/src/maintain.rs:425`) and is invisible to the
     rendezvous hash. A CLI run racing the supervisor on the same bucket
     duplicates the whole merge until the record PUT.
  2. Membership transitions: ADR-0065 accepts a bounded double-ownership
     window (`3 x H` plus one heartbeat) during which two workers both
     believe they own a unit. A merge in flight across that window is
     duplicated.
  3. A live-but-wedged owner starves its units; ADR-0065's mitigation is
     an alarm plus bounded intra-process concurrency, not takeover. The
     operator's remedy is a manual CLI run, which is window 1.
- ADR-0979 made the duplicate strictly more expensive to interrupt badly:
  the bounded compactor releases part bytes at PUT
  (`crates/ravel-maintain/src/rlog.rs:1976`), so a converging loser can no
  longer repair a missing winner part from RAM and fails closed
  (`ConvergedWinnerPartMissing`), forcing a full re-run.

The object store already carries the required primitive. The contract
makes both conditional modes mandatory (`PutMode::CreateIfAbsent`,
`PutMode::CasVersion(Version)`; `crates/ravel-object-store/src/lib.rs:57`,
capabilities enforced at startup), the S3 adapter maps them to
`If-None-Match: *` and `If-Match`, and the deployment qualification suite
(`ravel-cli store qualify`, `crates/ravel-object-store/src/conformance.rs`)
already falsifies both probes per bucket before a server will start. No
new store capability is needed.

One genuine gap: no durable expiry exists anywhere in the codebase, and
`last_modified` is returned only by `head()`/`list()` (`ObjectMeta`), at
1-second granularity, with the contract scoping it to "GC age checks
only; never order commits by it". Lease expiry is an age check with
advisory consequences, but the contract wording must be widened
deliberately, not assumed.

Naming is constrained by ADR-0065 decision 1: `LeaseCheck` is the GC
reader-protection gate and `WorkerSet` is membership; both module docs
forbid blurring. The word "claim" has zero coordination uses repo-wide
and is this ADR's vocabulary.

A lease is logically a lock. After this ADR, Ravel's architectural
statement is: no coordinator, no leader election, and no
correctness-critical distributed locks. Claims suppress redundant
maintenance work; immutable content-addressed parts and CAS record
publication remain the sole correctness mechanism. (Between a compaction and
an erasure rewrite of one bucket the claim is now also a fence; see the
2026-10-03 amendment.)

## Decision

### 1. A reusable claim primitive in ravel-fleet

New module `crates/ravel-fleet/src/claim.rs`. A claim is one small
mutable advisory object per unit of expensive work:

```
sys/maintain/claims/compaction/<work_id_hex>
```

`work_id` is a blake3 hash under a versioned domain tag:

```
work_id = blake3("ravel-compaction-claim-v1",
                 tenant_hash, signal, shard, ingest_hour_bucket)
```

The identity is the `Bucket` struct's four fields
(`crates/ravel-maintain/src/bucket.rs:11-15`), exactly the granularity at
which merges are paid for. Deliberately excluded from the identity:

- `input_set_hash`. Two nodes whose listings diverge on a sealed bucket
  must collide on one claim, run once, and surface the divergence through
  the existing `InputSetHashDivergence` machinery, not run twice under
  two claims. (The surfacing mechanism moved; see the 2026-09-28
  amendment.)
- A compaction policy version. None exists in the repo; the record key
  itself embeds only `input_set_hash16`, and geometry knobs
  (`max_l1_part_bytes`, `l1_part_memory_target_bytes`) already change
  part sets without changing record identity. The claim mirrors record
  identity. If a policy version is ever introduced, the domain tag
  versions the claim key space.

Payload is a versioned-tag protobuf:

```
{ owner_process_id, attempt_id, input_set_hash, state,
  renewed_count, lease_duration_ns, owner_clock_ns }
```

`owner_process_id` is the ADR-0057/0065 startup UUID (or another id the
process draws for a concurrent claiming task, see the 2026-11-18
amendment); `attempt_id` is fresh per acquisition. `input_set_hash` and `owner_clock_ns` are
informational (operator forensics), never inputs to any decision.
`state` is `running` or `completed`.

Protocol, using only contract-mandatory operations:

1. Check the terminal marker first: if the bucket already has a
   compaction record, there is nothing to claim (`compact.rs:64` already
   short-circuits; the claim check sits behind it).
2. Acquire with `PutMode::CreateIfAbsent`. `AlreadyExists` means another
   attempt holds the claim: read it once (one GET), `head()` it for
   `last_modified`, compute expiry as
   `last_modified + lease_duration`, and reschedule this bucket to after
   expiry plus deterministic jitter. Never poll an active claim. A claim
   whose payload cannot be read is given the 24 h lease ceiling as its
   lease (the 2026-11-09 amendment).
3. Renew with `PutMode::CasVersion(v)` where `v` is the `Version` from
   the owner's last successful PUT (returned in `PutOutcome`; no extra
   read). Renewal cadence is one-third of the lease duration, evaluated
   at cancellation checkpoints (Decision 3), not on a timer task.
4. Steal an expired claim with `PutMode::CasVersion(old_v)`: the old
   version token guarantees exactly one thief wins and that a
   concurrent renewal by a not-actually-dead owner defeats the steal.
   `PreconditionFailed` on a steal means someone else moved first; back
   off to step 2's reschedule.
5. On losing a renewal (`PreconditionFailed`), the owner stops at the
   next cancellation checkpoint. It publishes nothing from that run.
6. On success, mark the claim `state=completed` with `CasVersion`, or
   leave it to age out. Never delete unconditionally: a stale worker's
   DELETE could destroy a newer owner's claim. The published compaction
   record remains the only completion marker that means anything.

Expiry is judged from the store's server-generated `last_modified`, read
by `head()` on the contention path only. Node clocks never enter the
decision; 1-second granularity is noise against the 300 s default lease.
Early stealing caused by any residual skew is safe (advisory), merely
wasteful. `docs/object-store-contract.md`'s `last_modified` wording is
widened in the same commit from "GC age checks only" to "advisory age
decisions (GC age checks, claim expiry) only", keeping the ban on
ordering commits by it.

The primitive is generic over the key prefix and payload so retention,
sweep, folds, and erasure can adopt it later as an optimization; only
compaction adopts it in this ADR (the erasure rewrite adopts it too, as a
fence, since the 2026-10-03 amendment).

### 2. Claims are advisory: the correctness layer is untouched

A claim confers zero publication rights and its absence removes none
(between two compactions; the 2026-10-03 amendment makes holding it a
condition of a participating compaction or erasure rewrite publish).
The publish path (`publish.rs`) does not read claims. A paused owner
that loses its claim, wakes, and finishes anyway still collides at
content-addressed part keys and at the record's `CreateIfAbsent`, and
either converges or receives the existing typed
mismatch/missing-part errors. A claim bug can waste work; it cannot
corrupt data (between two compactions; between a compaction and an erasure
rewrite the claim is the fence, see the 2026-10-03 amendment). This
property is load-bearing: it is what keeps the
"no correctness-critical locks" statement true, and every test in the
Consequences section that kills owners at arbitrary points exists to
defend it.

### 3. Cancellation checkpoints in the merge pipeline

The merge gains a `ClaimGuard` consulted at the five natural quiescent
points the pipeline already has (the claim is acquired at the first and
consulted at the other four; see the amendment on jitter on the contended
path):

1. after the seal/tombstone/already-compacted/min-input gates, before
   any read (`compact.rs:60-71`); the claim is in fact taken after the input
   commit-record reads, see the amendment on where the claim is taken;
2. after input listing and `input_set_hash`, before catalog fan-out
   (`rewrite.rs:112-134`);
3. at the per-stream merge loop head (`rlog.rs:777-788`);
4. at each part boundary, after `put_part` returns (`rlog.rs:1960`);
5. immediately before record publish (`rewrite.rs:138`).

At each checkpoint the guard renews if a third of the lease has elapsed,
and cancels the run if renewal fails or the claim was observed stolen. A
renewal that fails with a store error other than a lost race, leaving the
claim in place under this process, is reclaimed by the same process at its
next attempt without waiting for the lease to expire; see the 2026-09-30
amendment. Cancellation returns the existing `PublishOutcome::Abandoned`
shape and inherits its safety argument verbatim (`publish.rs:31-45`): parts are
content-addressed and deterministic over the frozen input set, a later
run republishes byte-identical keys, and orphaned parts age out under
sweep rule 3. No new durable mid-run state exists.

The lease duration must exceed the longest non-cancellable stage (one
stream's cursor drain or one part encode+PUT), not the whole job.
Default 300 s, config `claim_lease_duration`; a warning at startup if
configured below 2x the largest of `max_l1_part_bytes` at a conservative
encode rate (the largest part is the larger of the two stored-size caps, see
the 2026-10-02 amendment).

### 4. Cost gating: claims only where duplication is expensive

Claim traffic is PUT-class, the expensive request class ($5/M reference
profile vs $0.40/M GET). The claim decision is explicit:

```
claim when  P(duplicate) x expected merge cost  >  claim PUT + renewals
```

Mechanically: a bucket is claimed only when its listed input bytes are
at or above `claim_min_input_bytes` (default 64 MiB stored). Below it,
duplicated work is cheaper than coordination and the bucket runs
unclaimed, exactly as today (retired by the 2026-10-03 amendment: a
participating run now claims every bucket). Deterministic jitter (derived from
`blake3(work_id || process_id)`, so it is stable per contender and free
of a shared clock) precedes every acquisition attempt, spreading
simultaneous starts (see the amendment on jitter on the contended path:
it now precedes only the contended ones). In steady state the supervisor's rendezvous gate
already ensures a single contender, so the added cost is one PUT per
claimed bucket plus renewals for long merges; the claim earns its PUT
whenever it prevents even one duplicated merge per ~thousands of claims.

Every claim/renew/steal request is counted under a new `coordinate`
phase in the compaction request ledger (996-8), never pooled into the
merge's own counters.

### 5. Participation and the escape hatch

Both callers of `compact_bucket` participate:

- the background supervisor's per-unit tick, inside its existing
  ownership gate;
- `ravel-cli maintain compact-tenant`, which is exactly the actor the
  rendezvous hash cannot see. The bucket walk (sequential when written;
  optionally concurrent since #1028's stage 1, see the 2026-09-28 amendment)
  claims each
  bucket before merging it and reports skipped-because-claimed buckets
  in the walk summary, per the no-silent-defaults rule.

`--no-claim` on the CLI bypasses claiming for repair work (documented
as: safe for correctness, may duplicate work). `coordination = off` in
the compactor config disables claims fleet-wide as the fallback for a
store whose qualification record predates the CAS probes or an
emergency; the code path is the same as the tiny-bucket skip (which the
2026-10-03 amendment removed; the unclaimed path stays), so it is
exercised by default tests, not a dead branch.

### 6. Discovery deduplication stays on rendezvous ownership

The intent's fixed virtual maintenance partitions (`H(unit) mod 256`
with renewable partition claims) are not adopted. The property they
target, at most one background maintainer discovering/scanning each
slice with automatic takeover on death and no registry, leader, stable
identity, or reassignment service, is already delivered by the landed
rendezvous ownership over heartbeat membership, as a pure function with
zero contended objects and zero renewal traffic. Superseding it with 256
CAS-renewed partition claims would add a contended mutable-key class and
a steady-state PUT budget to re-deliver an existing capability, and
ADR-0065's acceptance test for it (two supervisors, disjoint coverage,
no steady-state double-pay) is already the regression net. Claims layer
exactly where ownership cannot reach: cross-fleet actors, handoff
windows, wedged owners.

```mermaid
sequenceDiagram
    participant S as supervisor (owner by rendezvous)
    participant C as ravel-cli compact-tenant
    participant O as object store

    C->>O: PUT claim (If-None-Match: *)
    O-->>C: 200, version v1
    S->>O: PUT claim (If-None-Match: *)
    O-->>S: AlreadyExists
    S->>O: GET claim + HEAD (last_modified)
    S-->>S: reschedule bucket to expiry + jitter (no polling)
    C->>O: merge: input GETs, part PUTs (content-addressed)
    C->>O: renew claim (If-Match: v1) at checkpoints
    O-->>C: 200, version v2
    C->>O: PUT compaction record (If-None-Match: *)
    O-->>C: 200  (correctness decided HERE, claim irrelevant)
    C->>O: mark claim completed (If-Match: v2)
```

## Rejected alternatives

1. **Fixed maintenance partitions with renewable partition claims** (the
   intent's section 3). Lost to the landed ADR-0065 rendezvous
   mechanism: same four negative-space properties, zero contention, zero
   steady-state request cost, already gating production
   (`maintain.rs:1116`). The partitions would re-deliver discovery dedup
   at the cost of 256 contended claim objects and their renewal PUTs.
   Rendezvous does not cover the CLI; the bucket claim does.
2. **Per-unit CAS TTL leases for all maintenance work every tick**
   (ADR-0065 rejected alternative 1). Still rejected on its cost shape:
   a GET-and-PUT per unit per tick, linear in the unit count. This ADR's
   claims differ in every term of that objection: per bucket rather than
   per unit, taken only at merge start, only above a cost threshold, and
   renewed only while a merge runs, so steady-state claim traffic is
   proportional to compactions actually executed, which is the work
   being protected.
3. **`input_set_hash` in the work id.** Divergent input views must
   collide on one claim so one run executes and the divergence surfaces
   as the existing typed invariant breach (that mechanism moved; see the
   2026-09-28 amendment); hashing the view into the key
   would let both run and publish two records under two claims.
4. **Unconditional claim deletion on completion.** A stale worker's
   DELETE after a steal would destroy the newer owner's claim; `If-Match`
   completion or lifecycle aging cannot.
5. **Expiry from a payload-embedded node clock.** Reintroduces the clock
   skew ADR-0065 confined to one heartbeat object; the store's
   `last_modified` is one server-assigned time base shared by all
   contenders.
6. **Making the claim a publication precondition.** A lease bug would
   become a data-corruption bug. The claim stays invisible to
   `publish.rs` by construction, and the crash-kill test battery pins
   that. The 2026-10-03 amendment adopts it for the compaction and erasure
   rewrite publishes of one bucket, and says why the objection no longer
   holds there.

## Consequences

- **New advisory mutable key prefix** `sys/maintain/claims/` beside
  `workers/` and `memo/`. `docs/catalog-and-mvcc.md`'s key-layout
  section and ADR-0055's role templates (maintain role writes
  `sys/maintain/`, already granted) are updated in the landing commit.
  Like its siblings it must sit outside any WORM-protected prefix.
- **Contract doc widening** for `last_modified` as stated in Decision 1;
  no store capability or version changes. Claims use only mandatory,
  already-qualified operations, and the existing qualification suite's
  CAS probes are the conformance gate the `coordination = off` fallback
  is documented against.
- **`ravel-fleet` grows `claim.rs`**; `LeaseCheck` and `WorkerSet`
  vocabularies untouched. The claim module doc carries the same
  "this is not the GC lease" paragraph both siblings carry.
- **Request-cost accounting**: claim traffic reports under a
  `coordinate` phase; 996-8's compaction ledger counts it separately.
  Expected steady-state figures: +1 PUT and +0 GET per claimed bucket
  (owner path), +1 GET +1 HEAD per contender observation (contention
  path only).
- **New metrics** `ravel_maintain_claims_acquired_total`,
  `claims_lost_total`, `claims_stolen_total`,
  `claim_renew_failures_total`, `claimed_buckets_skipped` on the walk
  report. Alert rule: sustained steal rate above zero (a steal storm
  means lease duration is below a non-cancellable stage). These shipped
  with #1035; see item 2 of the implementation facts at the end for the
  shipped names and why the operations guide alerts on lost claims rather
  than steals, and the amendment on jitter on the contended path for the
  tracing fields claim outcomes also surface as.
- **Interaction with #1028 bucket concurrency**: claims are per bucket;
  a concurrent walk holds N claims with independent renewal state.
  Jitter is per work id, so N parallel acquisitions do not stampede.
- **Test battery** (FaultStore-scripted, MemoryStore-verified): crashes
  before and after each renewal, part upload, and record publication;
  lost renewal responses (renew returns `PreconditionFailed`, run
  cancels at next checkpoint, publishes nothing); a paused stale owner
  finishing after a steal (converges or takes the typed error, never
  corrupts); steal races (two thieves, one `CasVersion` winner); 404 on
  claim HEAD after DELETE-by-sweep; divergent input hashes colliding on
  one claim; and a two-supervisor MemoryStore test asserting via
  request counters that exactly one merge runs where today's test
  observes two (corrected in the 2026-09-28 amendment). Every count is an
  exact figure, not `> 0`.
- **Wave preview** (Stage 2 decomposes properly): W1 claim primitive +
  contract/key-layout docs (ravel-fleet, docs); W2 checkpoints + claim
  participation (ravel-maintain, serialized behind the in-flight #872
  chain on rlog.rs); W3 CLI participation + flags (ravel-cli, behind
  #1028 stage 1); W4 metrics + operations guide. Reuse for
  retention/sweep/fold/erasure is explicitly follow-up work (erasure
  adopted it with the 2026-10-03 amendment).

## Amendment (2026-09-28): status, and four facts that moved

<!-- amendment-applies: sections="Decision|Rejected alternatives|Consequences" pointer="2026-09-28 amendment" -->

This ADR passed its approval gate on 2026-09-01 (epic #1029 ledger; PR #1031
merged that day) and the claim primitive landed with #1032 (PR #1062), but the
status line still read Proposed. It now reads Accepted. The decisions stand;
four statements they rest on no longer describe main, and are corrected here.

1. **How a divergent listing surfaces.** Since #1070, two runs over the same
   bucket with different input sets publish two separate compaction records,
   and readers pick one with `select_authoritative_compaction_records`
   (`crates/ravel-catalog/src/catalog.rs`). `InputSetHashDivergence` now fires
   only when two different input-set hashes share the record key's 16-hex
   prefix. Leaving `input_set_hash` out of the work id still makes divergent
   views collide on one claim, so one merge is paid, which is the purpose of
   this ADR. The claim path must not turn a divergent listing into a silent
   success: the run that is refused the claim reports the bucket as skipped,
   never as compacted.
2. **The CLI bucket walk can run buckets concurrently.** Since #1028's stage 1
   `--bucket-concurrency` (default 1, `services/ravel-cli/src/maintain.rs`)
   lets an operator run several buckets at once, so decision 5 applies per
   bucket per concurrency slot. A test that never raises the flag covers only
   the one-bucket-at-a-time walk.
3. **Today's two-replica test runs no merge at all.** With a shared live set,
   `two_replicas_partition_units_without_double_pay`
   (`services/ravel-server/src/maintain.rs`) seeds one below-threshold bucket
   per shard and asserts that each unit is evaluated exactly once and
   classified `already_done`. It never reads `MaintainReport::compacted`, so it
   cannot show a duplicate merge either way. In the steady state rendezvous
   ownership already gives each unit one evaluator. The duplicate this ADR
   removes happens where ownership overlaps: an operator's
   `ravel-cli maintain compact-*` run, which the rendezvous hash cannot see,
   and a membership change during a merge, since the ownership check is taken
   at discovery and a running merge continues after its shard moves. The
   acceptance test therefore needs a bucket at or above the merge threshold,
   has to force the overlap (two replicas with solo live sets, or the owner
   changing while a merge runs), and asserts on `compacted` or the request
   counters: two merges without claims, exactly one with them.
4. **Code references drifted.** The seams this ADR names are on main at
   different lines than it cites: the supervisor ownership gate at
   `services/ravel-server/src/maintain.rs` around line 1737, the CLI merge
   calls at `services/ravel-cli/src/maintain.rs` around lines 161 and 781,
   the discovery check at `crates/ravel-maintain/src/compact.rs` around line
   102, and the part PUT at `crates/ravel-maintain/src/rlog.rs` around line
   2742. Every other line number in this document also predates this
   amendment and may no longer point at the code it describes. The
   implementing task locates each seam by name, not by line.

A duplicate compaction is a cost, not a correctness, problem: converging and
authoritative-record selection keep reads correct either way. The leak of a
losing record's parts (#1155) is separate work that claims make rarer but do
not fix.


## Amendment (2026-09-28): where the claim is taken

<!-- amendment-applies: sections="Decision" pointer="amendment on where the claim is taken" -->
<!-- amendment-supersedes: phrase="before any read" pointer="amendment on where the claim is taken" -->

Decision 3 item 1 places the claim acquisition "before any read". The
implementation (`compact_bucket_scoped` in
`crates/ravel-maintain/src/compact.rs`) takes it later: after the bucket
LIST and the gates it feeds, and after the input commit records are read, one
GET per L0 input. Decision 4 forces that order. The cost gate compares the
inputs' stored bytes with `claim_min_input_bytes`, and those bytes are the
`object_size` each commit record carries; the bucket listing names the
records, not their sizes. The claim still precedes every catalog and block
read and every PUT.

This is safe because the reads before the claim are read-only: a run refused
the claim has written nothing, so it has nothing to abandon, and the claim
stays advisory (decision 2) whenever it is taken.

The cost falls on a contender that is refused the claim. Before its claim
request it pays the bucket LIST plus one commit-record GET per input, on top
of the rejected PUT, the GET and the HEAD that Consequences lists for the
contention path. Those commit-record GETs are counted under the record-read
phase of the request ledger, not under `coordinate`. The background
supervisor then holds the bucket until the holder's expiry
(`MaintainMemo::claim_deferred`, checked before the bucket is listed; see
the amendment on jitter on the contended path), so it pays them once per
observed claim rather than once per tick.
`a_held_claim_skips_the_bucket_without_merging_it`
(`crates/ravel-maintain/tests/compaction_claims.rs`) pins the figures for a
two-input bucket: two record reads, zero catalog and block reads, three
coordinate requests.

## Amendment (2026-09-28): jitter on the contended path only

<!-- amendment-applies: sections="3. Cancellation checkpoints in the merge pipeline|4. Cost gating: claims only where duplication is expensive|Consequences" pointer="amendment on jitter on the contended path" -->
<!-- amendment-supersedes: phrase="precedes every acquisition attempt" pointer="amendment on jitter on the contended path" -->
<!-- amendment-supersedes: phrase="checked before the bucket is listed" pointer="amendment on jitter on the contended path" -->

Decision 4 says the deterministic jitter "precedes every acquisition
attempt". It now precedes only the contended ones. `ClaimGuard::acquire`
(`crates/ravel-maintain/src/claim_guard.rs`) issues its first
`CreateIfAbsent` with no wait, and waits out the jitter, through the
participant's `ClaimSleeper`, once before each of the two writes that can
follow a refused create: the steal of an expired claim, and the second
`CreateIfAbsent` after a claim vanished between the refusal and its read
(the same-process reclaim of the 2026-09-30 amendment pays none).

The reason is cost with nothing bought. The first `CreateIfAbsent` resolves
the race on its own, so a wait before it decorrelates nobody; jitter only
spreads contenders that retry or steal. Paid before every first attempt, it
charged every claimed bucket up to 10% of the lease at the default
`jitter_span_fraction` (30 s at the 300 s default lease, about 15 s on
average), and `scan_and_maintain_with_memo` awaits a shard's buckets one
after another, so a shard with ten claimed buckets spent about half of a
300 s maintain tick asleep. A single-replica deployment with no CLI run
beside it has no contender, and paid it on every claimed bucket.

The moment a steal lands shifts only by the latency of the refused create
and its two reads. Decision 1 step 2 reschedules a contender to after expiry
plus its jitter; the skip now reports one millisecond past expiry, and the
retry waits out the jitter before its steal instead of before its create. A
contender that loses a steal reschedules one full lease (capped at the 24 h
ceiling by the lease-ceiling guard amendment below) from the moment it
lost, because the winner has just written a claim with a fresh lease. An
unreadable claim's skip reschedules to one millisecond past one lease plus
the contender's jitter (the 2026-11-09 amendment moves it past the lease
ceiling plus the jitter), the instant the claim stops holding the bucket back,
because that retry steals nothing and so waits nothing.
The claim guard's tests `an_uncontended_claim_requests_no_jitter_wait`,
`a_refused_then_retried_create_requests_the_jitter_once` and
`a_steal_requests_the_jitter_once_and_the_reschedule_carries_none` pin the
waits.

Three further facts about the landed implementation:

1. **Four consulted checkpoints.** Of decision 3's five points, the first is
   where the claim is acquired (moved after the input record reads by the
   amendment on where the claim is taken), and the guard is consulted at the
   other four. The `Checkpoint` enum names only those four.
2. **The Consequences metrics shipped with #1035 (dated note, 2026-09-29).**
   `ravel_maintain_claims_acquired_total`, `ravel_maintain_claims_stolen_total`,
   `ravel_maintain_claims_lost_total`, `ravel_maintain_claim_renew_failures_total`
   and `ravel_maintain_claims_skipped_total` render on `/metrics`, one family
   per signal, accumulated by the supervisor from each pass's `MaintainReport`
   (`services/ravel-server/src/maintain.rs`, `crates/ravel-maintain/src/scan.rs`).
   `ravel_maintain_claims_skipped_total` is this Consequences list's
   `claimed_buckets_skipped`, under its shipped name. Before #1035 claim
   outcomes surfaced only as tracing fields (the skip and completion-failure
   events in `crates/ravel-maintain/src/compact.rs`, the lost-claim warning in
   `claim_guard.rs`, and the supervisor's per-shard pass summary) and as the
   `MaintainReport` counters `claim_skipped` and `claim_cancelled`; those
   fields and counters still exist and now feed the metrics above rather
   than being the only surface. Renewal store errors are the exception: the
   supervisor counts them where the error surfaces, because a pass that ends
   in an error returns no report, and so drops the other counts it had
   gathered. The operations guide alerts on
   `ravel_maintain_claims_lost_total` rather than on the steal rate the
   Consequences name: an expired claim is left by any crash, restart or
   failed run, so steals follow those too, while a lost claim is a run that
   was still working when another process took the bucket, which is the
   too-short lease that rule was after. `contend` does not tell this
   process's own expired claim from another's, so after a renewal store
   error the process skips its own bucket until the lease expires and then
   steals it back; that is reported on #1029 rather than changed here. The
   2026-09-30 amendment closes this: a renewal failure that leaves the
   claim in place is now reclaimed by the same process immediately,
   without waiting out the lease, while another process's claim still
   waits the full lease as before.
3. **A held bucket is still retention-evaluated.** The supervisor's claim
   hold (`MaintainMemo::claim_deferred`) skips only the held bucket's
   compaction call. Retention and zone classification run for it as for any
   other bucket, so a held head or tail hour still reaches
   `MaintainReport::head_tail_hours`, which scopes the supervisor's zoned
   sweep. The amendment on where the claim is taken describes the hold as
   checked before the bucket is listed; it is still read at the top of the
   bucket loop, but it now gates only the compaction call, and retention
   lists the bucket whenever the tenant has a retention policy and the
   bucket is sealed. The compaction path's own
   listing, its commit-record reads and its claim requests are still not
   issued while the hold lasts. `a_held_bucket_still_reaches_the_zone_split`
   (`crates/ravel-maintain/tests/compaction_claims.rs`) pins the zone split,
   zero coordinate requests and zero compactions for a held bucket.

## Amendment (2026-09-30): reclaim a same-process leftover claim without waiting out the lease (issue #2156)

<!-- amendment-applies: sections="3. Cancellation checkpoints in the merge pipeline|Amendment (2026-09-28): jitter on the contended path only" pointer="2026-09-30 amendment" -->

The 2026-09-28 amendment (fact 2 of "jitter on the contended path only")
named a gap: `contend` does not tell this process's own expired claim from
another's, so after a renewal store error (`MaintainError::ClaimRenewFailed`)
the process skipped its own bucket every pass until the lease expired, then
stole it back, and the metrics counted that as a steal rather than as the
bucket having stayed held throughout.

`ClaimGuard::contend` (`crates/ravel-maintain/src/claim_guard.rs`) now checks
the observed claim's holder process id before the expiry check. When it
equals the guard's own `owner.process_id`, the guard calls a new `reclaim`
primitive (`crates/ravel-fleet/src/claim.rs`) instead of waiting: `reclaim`
refuses locally, with no store request, unless the observed holder's
process id matches the caller's, and otherwise CAS-writes a fresh claim (new
attempt id, fresh lease) under the observed version, exactly as `steal`
does, but without requiring the claim to be expired and never by DELETE. A
same-process reclaim is reported as an acquisition (`claims_acquired`), not
a steal, since there was never another contender; a lost CAS race, or a
claim deleted before the CAS (`NotFound`), still counts as
`ClaimSkipReason::StealLost`; a different process's claim is unaffected and
still waits out the full lease as before. No jitter is paid, since there is
no contention with another process to decorrelate from.

The renewal store error is the case that motivated this, but a matching
process id covers every claim this process left behind: one from a run that
failed with any other error after taking it, and this process's own
completed claim from an earlier run. Process ids are fresh per process
start and per CLI invocation, so none of these is another process's claim
(a process with two concurrent claiming tasks gives each its own id, see
the 2026-11-18 amendment).
A concurrent sibling run of the same bucket in one process, which the
supervisor and the CLI walk do not produce, would cancel at its next
renewal if that fell before it published, and otherwise duplicate the
merge; between two compactions that is a cost, not a correctness problem
(between a compaction and an erasure rewrite, see the 2026-10-03
amendment).

`crates/ravel-sim/src/driver.rs`'s `is_recoverable_maintain_error` now
classifies `MaintainError::ClaimRenewFailed` carrying a retryable store
error the same way as a retryable `MaintainError::Store`. The simulator
installs no claim participant today, so this arm cannot fire there yet; a
unit case pins the classification.

Tests: in `crates/ravel-fleet/src/claim.rs`,
`reclaim_succeeds_on_own_unexpired_claim`,
`reclaim_refuses_locally_for_different_process` and
`reclaim_cas_race_returns_lost` pin the primitive. In
`crates/ravel-maintain/src/claim_guard.rs`,
`renewal_store_error_is_claim_renew_failed_not_lost` pins that a renewal
failing with a transient store error returns `ClaimRenewFailed` while
leaving the claim held;
`same_process_reclaims_leftover_claim_before_lease_expiry`,
`different_process_still_skips_held_by_another_after_renew_failure` and
`same_process_race_reclaim_cancels_the_original_guard` pin the guard
branch. `services/ravel-server/src/maintain.rs`'s extended
`claim_renewal_store_error_counts_as_a_renew_failure_not_a_loss` pins the
supervisor's next tick compacting the bucket, with `claims_acquired` rising
by one and `claims_skipped`/`claims_stolen` staying at zero.

## Amendment (2026-10-02): the startup check sizes the largest part as the larger of two caps (issue #2351)

<!-- amendment-applies: sections="3. Cancellation checkpoints in the merge pipeline" pointer="2026-10-02 amendment" -->

Decision 3 warns at startup when the lease is under twice the time to encode
and upload "the largest of `max_l1_part_bytes`". RLOG compaction now has a
stored-size cap of its own, `rlog_max_l1_part_bytes`, which the binaries set
equal to the derived memory split target (ADR-2135, #2351 amendment), and
that can be larger than the shared cap RSEG reads. The check therefore sizes
the largest part as the larger of the two (`CompactorConfig::
largest_stored_target_bytes`, called through `claim_lease_below_warn_threshold`
on the config), and `ravel-server` logs the larger figure.

The derived target is also capped at the part the lease supports,
`lease * 10 MiB/s / 2`, so the check is quiet at the defaults (1500 MiB at the
300 s default). The 256 MiB floor wins over that cap: a lease under about 51 s
cannot carry a floor-sized part, and the check warns, as decision 3 intends.

Tests: `lease_term_bounds_the_target_and_the_display_says_so`,
`a_lease_too_short_for_the_floor_yields_the_floor_and_the_check_warns` and
`the_startup_lease_check_sizes_against_the_larger_cap` in
`crates/ravel-maintain/src/config.rs`, and
`the_lease_check_is_quiet_at_the_derived_defaults_and_warns_at_the_floor` in
`services/ravel-server/src/config.rs`.

## Amendment (2026-10-03): the claim fences the compaction and erasure rewrite publishes (issue #2199)

<!-- amendment-applies: sections="Context|1. A reusable claim primitive in ravel-fleet|2. Claims are advisory: the correctness layer is untouched|4. Cost gating: claims only where duplication is expensive|5. Participation and the escape hatch|Rejected alternatives|Consequences" pointer="2026-10-03 amendment" -->
<!-- amendment-supersedes: phrase="its absence removes none" pointer="2026-10-03 amendment" -->
<!-- amendment-supersedes: phrase="unclaimed, exactly as today" pointer="2026-10-03 amendment" -->
<!-- amendment-supersedes: phrase="the same as the tiny-bucket skip" pointer="2026-10-03 amendment" -->
<!-- amendment-supersedes: phrase="compaction adopts it in this ADR" pointer="2026-10-03 amendment" -->

**The race.** A compaction record (`l1.<hash>.cmt`) and an erasure rewrite
record (`rw.<hash>.cmt`, ADR-0064 decision 3) have different keys, so neither's
`CreateIfAbsent` refuses the other. The compactor's refusal of a bucket that
holds a rewrite record (`RewritePresent`) was a listing-time check only; its
publish checkpoint consulted the claim and did not re-list; the claim was
skipped below the cost gate; and the erasure rewrite took no claim at all. A
compaction that listed a bucket before an erasure rewrite of it published could
therefore publish afterwards, from inputs that still held the erased rows, and
once the request's `.dreq` was retired those rows were served again. The
mirror order (an erasure rewrite planned from raw L0 inputs, publishing after a
compaction of them) leaves the same two record sets. Issue #1420 is the same
race class, found by the TLA+ lifecycle model.

**Decision (owner decision on #2199).** One per-bucket claim fences both
publishes, and neither pass may skip it. Concretely:

1. **The claim is mandatory for both publishes.** `compact_bucket_claimed` and
   `erasure_rewrite_bucket` (`crates/ravel-maintain/src/compact.rs`,
   `crates/ravel-maintain/src/erasure_rewrite.rs`) take the bucket's claim
   through one function, `claim_guard::claim_bucket`, under the same work id,
   so they contend for one claim object. Compaction takes it where it did,
   after the input commit-record reads and before any catalog read; the
   erasure rewrite takes it after its live-record resolution and before it
   builds. Both hold it through their record PUT: the claim rides on the
   run's config, so the logs and spans erasure builds renew it at the same
   merge checkpoints compaction does, and both consult it at the publish
   checkpoint.
2. **The paths that skipped it, and why they are no longer allowed.** The
   cost gate (decision 4) let a bucket below `claim_min_input_bytes` run
   unclaimed. It priced duplicate work against claim traffic, which is the
   right trade for a cost measure and the wrong one for a fence: a small
   bucket's erased rows are served exactly like a large one's. It is removed
   from `claims_bucket`, so a participating run claims every bucket. It was
   never separable into a decision about whether to compact at all (that is
   `min_compaction_inputs`), so nothing of it remains; the field and the
   `--maintain-claim-min-input-bytes` flag still parse and decide nothing. The
   erasure rewrite, which took no claim, now takes it. A stale unreadable
   claim, which `Acquire::Unclaimed` let the compaction run past, now holds
   the bucket for both passes (`ClaimSkipReason::UnreadableClaim`, retried one
   lease later) until an operator removes the object (until it is older than
   the lease ceiling, since the 2026-11-09 amendment): running past it would
   publish unfenced. A claim written in a newer claim format reads as
   unreadable too (no longer: since the 2026-11-15 amendment it is
   version-protected and never stolen), and claims are never deleted, so a future claim format
   bump followed by a rollback would hold every bucket the newer build
   claimed, erasures included, until each claim key is removed by hand (for
   at most the lease ceiling, since the 2026-11-09 amendment); a
   claim format bump must say how a rollback clears them.
3. **A pass that cannot take the claim backs off.** It builds nothing,
   publishes nothing, and a later pass retries; it never publishes unfenced. A
   compaction reports `ClaimedCompaction::SkippedClaimed` as before. An
   erasure rewrite reports `Rewritten { parts: 0, publish: Abandoned, .. }`,
   which the supervisor already treats as deferred (the `.dreq` stays pending
   and no `.done` is written); its outcome enum gained no variant, and
   `Rewritten` instead carries why it abandoned (`ErasureAbandon`: the claim
   held by another process, the claim lost, the re-list finding the record set
   changed, or the deadline) and its claim acquisition, so `ravel-server`
   counts a refused claim on `ravel_maintain_claims_skipped_total`, a lost one
   on `ravel_maintain_claims_lost_total`, and the other two on no claim
   counter. A claim lost mid-run cancels at the next checkpoint as decision 3
   describes (`Cancelled` for a compaction, `Abandoned` for an erasure
   rewrite).
4. **The pre-publish re-list is a second check.** After its last claim
   checkpoint and immediately before its record PUT, each pass lists the
   bucket again (`rewrite::relist_changed`) and compares its record set (L0
   commit records, compaction records, rewrite records, tombstone) with the
   listing it planned from. Any difference publishes nothing: a compaction
   reports the gate the new listing fails (`RewritePresent`,
   `AlreadyCompacted`, `Tombstoned`) or an abandoned publish, an erasure
   rewrite reports `Abandoned`. This covers what the claim cannot: an owner
   paused past its lease whose renewal cadence had not come due on its own
   clock when it resumed, and a caller that takes no claim. It costs one LIST
   per run that reaches its publish, counted under the request ledger's list
   phase.
5. **Callers that take no claim keep only the re-list.** Each of these
   publishes after the pre-publish re-list and holds no claim, so the window
   between its re-list and its PUT stays open against an erasure rewrite of
   the same bucket:
   - the unclaimed `compact_bucket` entry point, which `scan_and_compact`
     drives for the server's query-audit shard (a signal the erasure rewrite
     never touches, so no erasure rewrite can race it there), and which
     benchmarks and tests drive directly;
   - `ravel-cli maintain compact-bucket`, `compact-tenant` and `migrate` run
     with `--no-claim`, which install no `ClaimParticipant` (`--dry-run`
     installs none either, and publishes nothing);
   - `erasure_rewrite_bucket` and `migrate_bucket_format` under a caller that
     installs no participant;
   - every pass on a process with `coordination = off`
     (`--maintain-claims off`).

   The public `rewrite_and_publish` primitive keeps neither: it is given no
   planned listing, so it neither claims nor re-lists, and only tests drive
   it. `coordination = off` is decision 5's escape hatch for a store without
   the CAS probes and stays one, with this cost now stated.

   The format migration is fenced like a compaction, since it publishes a
   compaction record. `migrate_bucket_format` takes the bucket's claim through
   `claim_guard::claim_bucket` when its config carries a participant, after
   its eligibility reads and before it rewrites, and holds it through its
   record PUT; it reports `MigrateOutcome::SkippedClaimed` when refused and
   `MigrateOutcome::Cancelled` when it loses the claim, publishing nothing in
   either case. It passes its listing to the primitive, so its re-list runs
   with or without a claim. `ravel-cli maintain migrate` installs a
   participant unless `--no-claim` is given.
6. **Why no key-layout or proto change was needed.** The claim object already
   lives at `sys/maintain/claims/compaction/<work_id_hex>`, keyed by the
   bucket's four fields, which identify the erasure rewrite's bucket exactly as
   they identify the compaction's; "compaction" in the prefix names the key
   space and constrains nothing. The payload, the CAS protocol and the maintain
   role's grant on `sys/maintain/` are unchanged, and the re-list reads the
   existing commit prefix. The alternative, making the two record kinds collide
   at one `CreateIfAbsent`, would have changed the frozen record key layout.
7. **Decision 2 and rejected alternative 6.** Between two compactions decision
   2 stands: they still converge at the record's `CreateIfAbsent`, and
   `publish.rs` still does not read claims (the checks run before it is
   called). Between a compaction and an erasure rewrite, holding the claim is
   now a condition of a participating pass's publish, which is the
   publication precondition alternative 6 rejected. Its objection, that a
   lease bug would become a data-corruption bug, does not hold for this pair:
   without the fence the race already served erased rows. A claim held too
   long delays the other pass, which the erasure deadline surfaces; a claim
   stolen from a live owner is caught by its renewal at the next checkpoint or
   by its re-list. An owner that stalls longer than the lease between its
   re-list and its record PUT is caught by neither: both ran before the steal,
   so it publishes, and the pass that stole the claim can publish too. The
   outcome of that window is stated by the 2026-11-20 outcome amendment
   below. That is the same short window the unclaimed callers of point 5
   have. Between two compactions the re-list also changes how decision 2's
   convergence plays
   out: a losing compaction now usually finds the winner's record at its
   re-list and stops with `AlreadyCompacted`, so it rarely reaches
   `publish.rs`'s `resolve_already_exists`, and that path's re-PUT of a
   winner's missing parts no longer repairs them in practice. The Context's
   statement that claims are not correctness-critical is narrowed accordingly:
   for this one pair of passes the claim is part of the correctness argument,
   with the re-list behind it.

Tests, each shown failing against the pre-fix code, in
`crates/ravel-maintain/tests/erasure_compaction_fence.rs`:
`an_erasure_rewrite_that_publishes_mid_compaction_cancels_the_compaction` and
`a_compaction_that_publishes_mid_erasure_cancels_the_erasure_rewrite` (the two
interleavings, driven by `FaultStore` hold gates and test clocks),
`a_claim_held_by_one_pass_makes_the_other_back_off`,
`the_pre_publish_relist_aborts_when_the_record_set_changed` and
`a_stale_unreadable_claim_holds_the_bucket_against_both_passes`; for the
migration, `an_erasure_rewrite_that_publishes_mid_migration_cancels_the_migration`
and `the_migration_relist_aborts_when_an_erasure_rewrite_published`. In
`crates/ravel-maintain/tests/compaction_claims.rs`,
`a_bucket_below_the_retired_cost_gate_is_claimed` replaces the cost-gate test
and `a_stale_unreadable_claim_holds_the_bucket` replaces the test that ran such
a bucket unclaimed.

## Amendment (2026-11-09): an unreadable claim is stolen once older than the lease ceiling (issue #3209)

<!-- amendment-applies: sections="1. A reusable claim primitive in ravel-fleet|Amendment (2026-09-28): jitter on the contended path only|Amendment (2026-10-03): the claim fences the compaction and erasure rewrite publishes (issue #2199)" pointer="2026-11-09 amendment" -->
<!-- amendment-supersedes: phrase="until an operator removes the object" pointer="2026-11-09 amendment" -->
<!-- amendment-supersedes: phrase="until each claim key is removed by hand" pointer="2026-11-09 amendment" -->
<!-- amendment-supersedes: phrase="skip reschedules to one millisecond past one lease plus" pointer="2026-11-09 amendment" -->

**The problem.** Since the 2026-10-03 amendment a claim whose payload does not
decode holds its bucket against both the compaction and the erasure rewrite,
and nothing in Ravel ever removed it. Any principal with write access to
`sys/maintain/claims/` could therefore stall an erasure indefinitely with one
PUT of garbage, and a claim format bump followed by a rollback did the same to
every bucket the newer build had claimed. An erasure is a deadline obligation;
a hold with no bound is a liveness failure of it.

**Decision.** An unreadable claim (bytes that do not decode, a
`format_version` above this build's, an unspecified state, or an id that is
not 16 bytes -- narrowed by the 2026-11-15 amendment, which carves the
version cases out into their own never-stolen class) is given
`MAX_OBSERVED_LEASE_MS` (24 h,
`crates/ravel-fleet/src/claim.rs`) as its lease. Its expiry is the store's
`last_modified` plus 24 h, and past that it is stolen under
`PutMode::CasVersion` on the version the contender observed, exactly as any
expired claim is (decision 1 step 4). The steal is never an unconditional PUT
and never a DELETE: a write to the object between the observation and the
steal moves its version and defeats the steal, which reports `Steal::Lost`.
`StealRefused::UnreadableClaim` and the claim guard's refusal to steal are
removed.

**The invariant.** No claim object holds its bucket past its `last_modified`
(except a version-protected claim, 2026-11-15 amendment) plus 24 h plus one
contender's jitter without a write to the object. A
readable claim's lease is already capped at the same ceiling; an unreadable
one now gets exactly the ceiling. The jitter for an unreadable claim is drawn
from the contender's configured lease (capped at the ceiling), not from the
ceiling, so it is at most `jitter_span_fraction` of that lease: 30 s at the
300 s default.

**Why 24 h and not the configured lease.** The claim's own lease is unknown
because its payload is. A live writer of any lease up to the ceiling renews
well inside it, so judging the claim by the ceiling never steals from a live
owner whose lease is legal, whatever build wrote it; judging it by the
observer's lease would steal from a newer build running a longer lease. The
ceiling is also the bound a readable claim's declared lease is already held
to, so one number bounds both.

**No second jitter.** The claim guard's skip for an unreadable claim already
reschedules to one millisecond past the ceiling plus the contender's jitter,
so contenders' thresholds differ by that jitter. A retry at or after that
point steals without waiting it out again, which keeps the stall at the
ceiling plus one jitter rather than two. Below that point the guard skips with
`ClaimSkipReason::UnreadableClaim` and writes nothing; the steal is logged at
warn with the claim's `last_modified`.

**The fence still holds.** A pass that steals the claim holds it, so it
publishes fenced, as the 2026-10-03 amendment requires. Nothing runs
unclaimed past an unreadable claim.

**Residual.** A writer that re-PUTs the claim object more often than every
24 h moves its `last_modified` each time and keeps the bucket held. Closing
that needs authenticated claims (a keyed MAC over the payload, as ADR-3130
does for its own objects), which is a proto field and a claim format version
bump. It is not decided here; the write grant on `sys/maintain/` remains the
control on it.

**The recheck is once per lease, not once per ceiling.** The skip carries two
instants. `reschedule_after_unix_ms` is the steal threshold above.
`recheck_after_unix_ms` is when the caller looks at the bucket again: for an
unreadable claim, the earlier of that threshold and one configured lease plus
the contender's jitter plus one millisecond past the observation; for every
other skip reason, the reschedule point itself. The compaction scan's
in-memory hold reads the recheck point, so a worker re-observes an unreadable
claim about once per lease: if the key is gone it claims the bucket normally,
if it is still there and young it skips again, and past the threshold it
steals. Each look costs the rejected `CreateIfAbsent` plus one GET and one
HEAD of the claim key, the same as any contended observation.

**Operator removal.** Deleting a stuck claim key by hand is still allowed and
ends the hold within one lease plus jitter (the next recheck), with the
caution the operations guide gives. It is no longer the only way out.

Tests, each shown failing against the code before this amendment:
`an_unreadable_claim_is_stolen_once_older_than_the_lease_ceiling` in
`crates/ravel-fleet/src/claim.rs` (garbage and future-format payloads --
the future-format case is carved out by the 2026-11-15 amendment and no
longer stolen -- the CAS steal, and a rewrite between observation and steal
reporting `Lost`);
`an_unreadable_claim_defers_until_the_lease_ceiling_plus_jitter_then_steals`
in `crates/ravel-maintain/src/claim_guard.rs`;
`a_stale_unreadable_claim_holds_both_passes_until_the_lease_ceiling` in
`crates/ravel-maintain/tests/erasure_compaction_fence.rs` and
`a_stale_unreadable_claim_holds_the_bucket_until_the_lease_ceiling` in
`crates/ravel-maintain/tests/compaction_claims.rs`, which replace the two
tests of the same prefix the 2026-10-03 amendment names; and (superseded by
the 2026-11-15 amendment)
`a_future_format_claim_stalls_an_erasure_for_at_most_the_lease_ceiling` in
`services/ravel-server/src/maintain.rs`. The recheck point is pinned by
`an_unreadable_claim_is_rechecked_once_per_lease_before_its_steal` in
`crates/ravel-maintain/src/claim_guard.rs`, and through the compaction scan by
`a_hand_deleted_unreadable_claim_releases_its_bucket_within_one_lease` and
`a_present_unreadable_claim_is_rechecked_once_per_lease_and_skipped` in
`services/ravel-server/src/maintain.rs`.

The TLA+ model checks the same rule: `Steal` in
`formal/tla/maintenance/CompactionClaims.tla` is gated on expiry alone, and an
unreadable claim's expiry uses the ceiling (`ObservedLease`), so the publish
fence and the other claim safety invariants are checked over behaviours that
steal a corrupt claim (`formal/tla/maintenance/results.md`, "Unreadable-claim
steal").

## Amendment (2026-11-10): the claim guard caps its own lease at the ceiling (issue #4065)

<!-- amendment-applies: sections="Amendment (2026-09-28): jitter on the contended path only" pointer="lease-ceiling guard amendment" -->

This is the lease-ceiling guard amendment. The claim guard read its
configured lease raw in two places, while the claim observation caps a lease
at `MAX_OBSERVED_LEASE_MS` (24 h) before it uses it. With a lease configured
above 24 h, the jitter the guard waited out before a steal was priced from
the raw lease and could reach `jitter_span_fraction` of it, and a contender
that lost a steal rescheduled one raw lease from the moment it lost, which
could be longer than any lease an observer trusts.

`ClaimGuard::lease_ms` now floors the configured lease at 1 ms and caps it at
the ceiling, and the guard's jitter, its unreadable-claim recheck cap and its
lost-steal reschedule all read that value. A lost steal therefore waits at
most 24 h before the contender looks again. At the 300 s default lease none
of these figures changes. The holder still renews its own claim against the
lease it wrote, uncapped, since renewal is not an observation.

Tests, each shown failing against the code before this amendment:
`a_lease_above_the_ceiling_prices_the_jitter_from_the_clamped_lease` and
`a_lost_steal_reschedules_within_the_lease_ceiling` in
`crates/ravel-maintain/src/claim_guard.rs`.

## Amendment (2026-11-15): a claim at an unknown format version is never stolen (ADR-4233 decision 4, H8)

<!-- amendment-applies: sections="Amendment (2026-11-09): an unreadable claim is stolen once older than the lease ceiling (issue #3209)" pointer="2026-11-15 amendment" -->
<!-- amendment-supersedes: phrase="a `format_version` above this build's, an unspecified state, or an id that is" pointer="2026-11-15 amendment" -->
<!-- amendment-supersedes: phrase="garbage and future-format payloads" pointer="2026-11-15 amendment" -->
<!-- amendment-supersedes: phrase="a_future_format_claim_stalls_an_erasure_for_at_most_the_lease_ceiling" pointer="2026-11-15 amendment" -->
<!-- amendment-supersedes: phrase="A claim written in a newer claim format reads as" pointer="2026-11-15 amendment" -->
<!-- amendment-supersedes: phrase="No claim object holds its bucket past its `last_modified`" pointer="2026-11-15 amendment" -->

**The problem.** The 2026-11-09 amendment gave every unreadable claim,
including one whose `format_version` is 0 (never written by any build) or
above this build's `CLAIM_FORMAT_VERSION` (written by a newer build), the
same 24 h-plus-jitter steal. A build that does not know a stored claim's
version cannot know what invariant that version's writer relied on; stealing
it under CAS and letting an older build's compactor or erasure pass run
unfenced past it risks exactly the correctness failure claims exist to
prevent, for a case the ceiling was never meant to cover (a format bump
followed by a rollback, not a crashed writer).

**Decision.** A claim is now classified before the 24 h ceiling applies at
all, not folded into one unreadable case. A claim whose `format_version` is 0
or above `CLAIM_FORMAT_VERSION` is version-protected: it is never stolen and
never overwritten, at any age, and judged by no lease at all. A claim whose
`format_version` is a version this build knows, but whose payload otherwise
does not decode (bytes that do not parse, an unspecified state, or an id that
is not 16 bytes), is unchanged: it is still given `MAX_OBSERVED_LEASE_MS` as
its lease and stolen past that ceiling exactly as the 2026-11-09 amendment
decided.

**The invariant, restated.** No claim object at a known `format_version`
holds its bucket past its `last_modified` plus 24 h plus one contender's
jitter without a write to the object. A claim at an unknown `format_version`
holds its bucket until an operator deletes the object by hand or a build
that knows the version restarts and reclaims it; no ceiling bounds it.

**The claim guard.** `ClaimGuard::acquire` reports the new
`ClaimSkipReason::VersionProtectedClaim`, distinct from
`ClaimSkipReason::UnreadableClaim`, on the existing
`ravel_maintain_claims_skipped_total` skip counter, not a new metric family.
Both `reschedule_after_unix_ms` and `recheck_after_unix_ms` are pinned to
`i64::MAX` for this reason, since there is no threshold to recheck against
short of the two remedies above; the in-memory deferral this pins
(`MaintainMemo::claim_deferred_until_ns`) is process-local and resets on
restart, so pinning it costs nothing durable. This process logs the hazard
once per claim at `warn`, not on every tick, naming the claim object's key
and the operator remedy (`docs/guides/operations/maintenance.md`).

**Why not fold this into the ceiling.** Judging an unknown version by a
lease computed for known ones assumes the unknown version's writer obeys the
same renewal and CAS discipline this build's does; a newer build's claim
format might not. Holding forever is the conservative failure mode a bounded
steal cannot offer a version it cannot read, and it is a hazard the operator
can see (the warn log, the skip reason, the operations-guide remedy) and
resolve directly, rather than a silent window where a stale build's compactor
races one it cannot understand.

Tests, each shown failing against the code before this amendment:
`claim_above_known_version_or_zero_is_never_stolen` in
`crates/ravel-fleet/src/claim.rs`; `a_version_protected_claim_is_never_stolen`
and `version_protected_warn_fires_once_per_work_id` in
`crates/ravel-maintain/src/claim_guard.rs`;
`claims_skipped_help_text_names_the_version_protected_reason` in
`services/ravel-server/src/metrics.rs`;
`a_version_protected_claim_skip_renders_its_own_reason` in
`services/ravel-cli/src/maintain.rs`; and
`a_future_format_claim_is_never_stolen_at_any_age` in
`services/ravel-server/src/maintain.rs`, which asserts the rendered
`/metrics` skip count and replaces
`a_future_format_claim_stalls_an_erasure_for_at_most_the_lease_ceiling` named
by the 2026-11-09 amendment: that test is no longer accurate, since a
future-format claim is no longer stolen at any age.

## Amendment (2026-11-18): a process may claim under more than one owner id (issue #4448)

<!-- amendment-applies: sections="1. A reusable claim primitive in ravel-fleet|Amendment (2026-09-30): reclaim a same-process leftover claim without waiting out the lease (issue #2156)" pointer="2026-11-18 amendment" -->
<!-- amendment-supersedes: phrase="`owner_process_id` is the ADR-0057/0065 startup UUID;" pointer="2026-11-18 amendment" -->

Decision 1 said `owner_process_id` is the ADR-0057/0065 startup UUID, the
id the process's `sys/maintain/workers/<process_id>` heartbeat is keyed
under. That held while each process had one claiming task. A maintain
process run with `--background-migrate` has two that run concurrently: the
maintenance tick (compaction and erasure rewrite) and the background
migrate walk. The 2026-09-30 amendment makes a claim guard reclaim a claim
held under its own owner id at once, without waiting for the lease, so two
concurrent tasks under one id would each take a bucket the other holds.

A process may therefore claim under more than one owner id, one per
concurrent claiming task:

- The maintenance tick claims under the startup UUID, as before.
- Background migrate claims under its own id, drawn from the process RNG
  once per process start, when the server builds the compat maintenance
  context. It always installs its own participant under that id, replacing
  any participant already in the configuration, so the two tasks never
  share one. At startup the server logs that id next to the worker id, the
  one record that maps it back to the process.
- A claim's owner id therefore need not match any heartbeat key. No
  decision reads that link: the claim guard compares an observed owner id
  only with its own, for the same-process reclaim. An operator following a
  claim holder to a heartbeat finds none for a background migrate claim and
  uses the startup log line instead.

The 2026-09-30 amendment's reasoning holds per owner id: an owner id is
fresh per process start and per claiming task, so a claim under the
guard's own owner id is still one this task left behind.

Tests, each shown failing against a mutation of the code it pins:
`the_spawned_context_draws_a_migrate_claim_id_apart_from_the_worker_id` in
`services/ravel-server/src/compat_maintain.rs` fails when the context takes
the worker id; `background_migrate_stands_down_for_a_claim_the_ticks_process_id_holds`
in the same file fails when the walk claims under the worker id, and, for
a configuration that already carries a participant under the worker id,
when the walk reuses that participant.

## Amendment (2026-11-20, #4449): what the residual publish window does, and how it closes

<!-- amendment-applies: sections="Amendment (2026-10-03): the claim fences the compaction and erasure rewrite publishes (issue #2199)" pointer="2026-11-20 outcome amendment" -->

Point 7 of the 2026-10-03 amendment names the window the claim and the
re-list leave open: an owner that stalls longer than the lease between its
re-list and its record PUT publishes, and so does the pass that stole its
claim. It did not say what that produces. Reproduced with claims on, with
injected clocks and a store gate parking the compaction inside its record
PUT (`crates/ravel-maintain/tests/erasure_claim_pause.rs`, PR #4507): both
records land, every reader serves the compaction's parts beside the
rewrite's, so the erased series is served again and the bucket's surviving
rows are returned twice, and every later erasure pass over the bucket fails
with `MultipleLiveRecords`, so the request never completes and its `.dreq`
is never retired. The window is reached by any stall of about 200 to 300 s
at the default lease (the claim is at most a third of the lease old at the
publish checkpoint): a process freeze, a VM or container pause, or a PUT
stalled inside the client's timeout. A claim renewal immediately before the
PUT narrows it and cannot close it, because the stall can land after any
client-side check and the store conditions a write on its own key only.

The closure decided here (it has landed, as the 2026-11-21 amendment below
records) is on the read side, applied identically by every bucket resolution: a version 1
compaction record that is not in a live rewrite's chain and whose every
input is among that rewrite's effective inputs is dominated by the rewrite.
Readers will drop its parts, the superseded-input sweep will reclaim it and
its parts in the rewrite's chain group, as for a dominated version 2
record, and the erasure pass will treat it as superseded. In the race above
both passes planned from one listing, so the compaction's inputs are
exactly the rewrite's, and the rewrite's parts hold every row of those
inputs minus the erased ones; nothing is lost. A compaction record whose
inputs are not all covered is a genuine conflict and stays served as
today, counted and warned. Decision 2's statement that a claim bug cannot
corrupt data is then restored in this form: between a compaction and an
erasure rewrite a lost or late claim costs a wasted compaction, never a
served erased row (narrowed to records over the same inputs by the #4560
amendment below). Making the two record kinds collide at one create-only
key per bucket, which point 6 rejected as a key-layout change, remains the
store-side closure; the owner declined an ADR for it on 2026-11-20.

## Amendment (2026-11-21, #4449): erasure dominance of version 1 records has landed

<!-- amendment-applies: sections="Amendment (2026-11-20, #4449): what the residual publish window does, and how it closes" pointer="2026-11-21 amendment" -->
<!-- amendment-supersedes: phrase="planned for 0.35.0 and not yet implemented" pointer="2026-11-21 amendment" -->

The read-side closure the 2026-11-20 amendment decided is implemented. A
version 1 compaction record that no live rewrite's chain names is dominated
when the effective inputs of one live rewrite contain every input it names
(`ravel_catalog::erasure_dominance`). Coverage is per rewrite: a record two
rewrites cover only between them is not dominated. A version 2 record whose
predecessor is a dominated version 1 record is dominated too. Every bucket
resolution, the token fallback, the index fold, scrub, the erasure
completion gate and `migrate` drop a dominated record's parts; the erasure
pass treats it as superseded when it picks the bucket's live record
(`select_live_record`); and the superseded-input sweep reclaims it with its
parts in the covering rewrite's group, under that group's horizon,
reachability and legal-hold gates. The cycle and depth errors and the
version 2 input-equality check are unchanged, and nothing in the formats or
the key layout changed. The rule is stated normatively in
docs/catalog-and-mvcc.md, step 3.

A record whose inputs a live rewrite covers only in part stays a conflict
for the overlap selection. Such a record is counted in
`ravel_catalog_erasure_overlap_conflicts_total` and each such record and
rewrite pair is logged at warn; each dominated version 1 record is counted
in `ravel_catalog_dominated_compaction_records_total`. Both counters are on
`/metrics` and count once per resolution that finds the record, so a record
still present counts again on every later resolution; the superseded-input
sweep's evaluation does not count.

A bucket already in this state before the upgrade is read correctly wherever
it is resolved from its commit records. An hour folded into the catalog
snapshot while in this state keeps serving both records' parts until a fold
reconciles that hour again, as for any record that lands in an already
folded hour (ADR-0063 section 4).

With this, between a compaction and an erasure rewrite a lost or late claim
costs a wasted compaction, never a served erased row (true only for records
over the same inputs; the #4560 amendment below corrects it). The race the
2026-11-20 amendment reproduced still publishes both records, since the
store-side closure stays declined, but readers serve only the rewrite's
parts, the erasure rerun reports `AlreadyApplied` rather than
`MultipleLiveRecords`, and the sweep reclaims the compaction's record and
parts.

Tests, each shown failing against a mutation of the code it pins:
`a_compaction_paused_inside_its_record_put_publishes_beside_the_erasure_rewrite`
in `crates/ravel-maintain/tests/erasure_claim_pause.rs`, no longer ignored,
fails when no rewrite covers the record (the erased series is served, the
kept rows are served twice, and the rerun fails with `MultipleLiveRecords`);
in `crates/ravel-catalog/src/catalog.rs`,
`coverage_spread_across_two_rewrites_is_not_dominance` fails when coverage
is taken as any overlap or as the union of rewrites, and
`partial_coverage_is_a_counted_conflict` fails when it is taken as any
overlap;
`a_version_1_record_a_rewrite_covers_is_not_live` in
`crates/ravel-maintain/src/erasure_rewrite.rs` fails when
`select_live_record` ignores dominance; and
`sweep_reclaims_a_dominated_version_1_record_with_its_raw_rewrite`,
`sweep_reclaims_a_dominated_version_1_record_with_its_predecessor_rewrite`,
`sweep_holds_a_dominated_version_1_group_under_a_legal_hold` and
`sweep_keeps_a_dominated_version_1_group_inside_its_horizon` in
`crates/ravel-maintain/tests/superseding_compaction_record.rs` fail when the
sweep leaves the record out of the rewrite's group or times the group's
horizon from the rewrite alone.

## Amendment (2026-11-21, #4560): a conflicting compaction record keeps the erasure request's `.dreq`

<!-- amendment-applies: sections="Amendment (2026-11-20, #4449): what the residual publish window does, and how it closes|Amendment (2026-11-21, #4449): erasure dominance of version 1 records has landed" pointer="#4560 amendment" -->
<!-- amendment-supersedes: phrase="never a served erased row" pointer="#4560 amendment" -->

The two #4449 amendments above end on "a lost or late claim costs a wasted
compaction, never a served erased row". That holds only when the compaction
and the erasure rewrite plan the same inputs, the race both reproduced: one
live rewrite then covers every input of the compaction's version 1 record,
the record is dominated, and readers serve the rewrite's parts alone.

It does not hold when a late L0 lands after the maintain seal and the
compaction's listing takes it while the rewrite's did not (the third-stall
ordering in #4542). The compaction record then names more inputs than any
one live rewrite covers, so it is not dominated: it is a conflict
(`ErasureDominance::conflicts`), served beside the rewrite. The bucket can
return the surviving rows of the shared inputs twice, an erasure pass over
the bucket fails with `MultipleLiveRecords`, so later erasures for that
tenant and signal whose window reaches the bucket stop completing, and the
catalog counts the record in `ravel_catalog_erasure_overlap_conflicts_total`
and logs each record and rewrite pair at warn. The record's parts can carry
the rows the rewrite erased. While the request's `.dreq` exists, the
query-time filter hides them; if the compaction's record PUT lands after
the erasure's completion gate, the request gets its `.done`, and before
this amendment the erasure-request sweep (rule 6) deleted the `.dreq` once
the protection horizon passed, after which the erased rows were served from
the compaction's parts.

Decided and implemented: rule 6 keeps a request's `.dreq` while a
compaction record that conflicts with a live rewrite applying the request
is live. The superseded-input sweep, which already calls
`erasure_dominance` per bucket in its deleting and its observing mode,
reports every request applied by a live rewrite that has a conflicting
record in its bucket: the rewrite's own drops and those of every rewrite
record still present on its supersession chain
(`SupersededSweepOutcome::conflict_request_ids`). Rule 6 reads them off
its own observing pass, keeps each such candidate, counts it in
`ravel_maintain_dreq_held_by_superseded_inputs_total`, and logs one warn
line naming the tenant, signal and request id. Nothing persists the keep:
the set is recomputed from the records present on every pass, so the
`.dreq` is released by the ordinary rule once the conflicting record is
deleted or a later live rewrite dominates it.

A chain that names a record no longer present is not walked to the end.
Rule 2 reclaims a predecessor rewrite under its live successor once the
predecessor's horizon passes; the successor then resolves to fewer inputs
than it superseded, so a compaction record over the missing inputs is no
longer reported as a conflict, and the requests the missing generation
applied are named by no surviving record. The same holds for a version 2
link whose predecessor is gone. The sweep reports such a bucket fail-safe
(`SupersededSweepOutcome::conflict_truncated_buckets`) when a rewrite whose
chain is cut either has a conflict or shares the bucket with a served
compaction record, one neither dominated nor named by another record, and
rule 6 keeps every candidate `.dreq` for the tenant and signal while any
bucket is reported, with the same count and warn line. The records present
cannot tell that case from a served record that shares nothing with the
rewrite, so the keep covers both and lasts until the served record is
deleted or dominated, or retention removes the bucket. The sweep also
records a bucket's conflicts before it selects the bucket's authoritative
compaction records, so a bucket whose selection fails keeps them.

`ErasureDominance::conflicts` now also reports a version 2 record (a
`migrate` re-encode) that no rewrite's chain names, that is not dominated
and that shares an input with a live rewrite. A version 2 record carries
its predecessor's inputs, so a re-encode of a conflicting version 1 record
conflicts the same way, and before this it dropped out of the conflict set
as soon as the sweep removed the predecessor. The dominated set and the
version 1 attribution are unchanged; the conflict counter and warn line
now include such records.

What remains: the erased subject stays filtered while the conflict is
live, at the liveness cost above (rows served twice, erasures over the
bucket not completing) until an operator removes the conflicting record or
a later rewrite dominates it. An hour folded into the catalog snapshot
while the conflict was live behaves as the 2026-11-21 #4449 amendment
describes. The fold-side fence against a late commit landing after the
seal is #4561.

Tests, each shown failing against a mutation of the code it pins, in
`crates/ravel-maintain/tests/erasure_conflict_dreq.rs`:
`rule_6_keeps_a_dreq_while_a_conflicting_compaction_record_is_live` fails
when rule 6 ignores the conflict set (the `.dreq` is deleted);
`a_kept_dreq_is_released_once_the_conflicting_record_is_deleted` and
`a_kept_dreq_is_released_once_a_later_rewrite_dominates_the_record` fail
when the keep follows every live rewrite in the bucket instead of the
conflicts (the `.dreq` is kept after the conflict is gone);
`rule_6_keeps_every_dreq_while_a_conflicting_rewrite_chain_is_truncated`
fails without the cut-chain report (the `.dreq` is deleted once the
predecessor rewrite is reclaimed);
`rule_6_keeps_a_conflict_dreq_when_the_bucket_selection_fails` fails when
the conflicts are recorded after the selection's error arm; and
`rule_6_keeps_a_dreq_while_a_conflicting_version_2_record_is_live`, with
`a_version_2_record_over_a_conflicting_record_is_a_conflict` in
`crates/ravel-catalog/src/catalog.rs`, fails when version 2 records are
left out of the conflicts.
