# ADR-0057: fleet-global admission via periodic self-owned-key reconciliation

Status: Accepted (2026-08-06)

Issue #656, program #450. New finding from adversarial review v2
(post-remediation delta, 2026-08-05), NF-4: epic EB (#452, ADR-0051, closed)
shipped `AdmissionController` -- active-series, active-stream, byte-rate, and
series-creation-rate caps -- enforced entirely per process. A tenant routed
across N ingest replicas gets N times the configured budget, with no
coordination required to hit it: normal load-balanced traffic, not an
attack.

## Context

`AdmissionController` (`crates/ravel-ingest/src/admission.rs`) is one
`Arc<AdmissionController>` per process, holding a `Mutex<HashMap<TenantId,
TenantState>>`. Three of its four caps are genuinely stateful:
`max_active_series`/`max_active_streams` (`EpochIdSet`, an exact two-generation
rotating-set membership tracker over one-hour epochs) and
`ingest_byte_rate`/`series_creation_rate` (classic token buckets over an
injected clock). The fourth, body size, is a stateless per-connection
transport limit (axum/tonic) and is not affected by this ADR at all -- it is
already exact per request regardless of replica count.

Every check is synchronous, in-process, and precedes all I/O: one mutex
lock, a hashmap lookup, and O(1) hashset/token-bucket arithmetic, all
sub-microsecond, with no `.await` under the lock
(`admission.rs:405-406`'s own comment states this explicitly). It runs
before the OTLP protobuf decode (byte rate) and before the write is handed
to the ingest router's shard actor (series/stream admission) --
`services/ravel-server/src/ingest.rs:118-156`,
`logs_ingest.rs:210-241`. The object-store `PUT` for the actual data
happens later, asynchronously, decoupled from the request by the shard
actor's own flush trigger. This is the load-bearing fact the whole design
turns on: admission today costs nothing on the hot path, and any design
that inserts a network round-trip at the exact point the check runs turns
a sub-microsecond gate into a multi-millisecond one on every ingest
request.

ADR-0051 already considered this problem and made a deliberate choice.
Rejected alternative 1, verbatim: "Globally consistent (S3-coordinated)
quota enforcement. Shared counters CAS'd in object storage, so the cap
holds fleet-wide regardless of replica count. Rejected: it puts an S3
round-trip into every admission decision on the hottest path in the
system, adds a new mutable-object churn class, and defends against a
multiplier (replica count) the operator already controls. Per-process
enforcement with the N× bound documented is honest and has zero hot-path
cost." This ADR does not reopen that specific proposal -- a shared object
mutated on the request path, CAS-retried under contention -- and agrees
with its reasoning: `ObjectStoreBackend` (`crates/ravel-object-store/src/
lib.rs`) has no atomic-increment primitive, only whole-object `put` with
an optional CAS precondition, so a request-path shared counter really
would mean GET-decode-increment-PUT-retry-on-conflict on every admission
decision, worse under contention from a busy tenant precisely when the
cap matters most.

What this ADR adds is a mechanism ADR-0051 did not consider: reconciliation
off the hot path, on a bounded interval, using keys each process owns
exclusively -- so there is no CAS, no shared mutable object, and no
contention, at the cost of a bounded (not eliminated) overshoot window
matching the shape issue #656 itself invited ("a hybrid, e.g. per-process
soft caps with periodic reconciliation against a shared hard cap"). The
closest existing precedent for this shape, though for a different problem,
is ADR-0052's router live-switch (`crates/ravel-ingest/src/generation.rs`,
`GenerationSwitch`): a cached view refreshed on a bounded interval,
fail-closed once the cache exceeds that interval's age. That precedent
does not transfer directly -- ADR-0052's staleness is absorbed by a wider
read-side scan window (the routing decision is never wrong, only
sometimes deferred), while admission staleness has no compensating
mechanism: a stale local view simply admits more than the cap intends,
and an over-admitted write cannot be un-admitted after the fact. The
design below is shaped specifically around that difference.

**Cardinality caps do not sum the way rate caps do.** A byte-rate or
series-creation-rate is a flow: bytes-per-second or new-identities-per-
second genuinely add across processes. Active-series/stream counts are
set membership: if the same series is (or has been) routed to two
different replicas over its lifetime, summing each process's own
`EpochIdSet` size counts it twice, overestimating the true fleet-wide
distinct count. This ADR treats that overestimate as acceptable, not as a
bug to solve: the cap's job is to bound worst-case resource consumption
per tenant, not to compute exact fleet-wide cardinality (that is the
catalog's job at query time, over durable data, not admission's job at
write time, over in-memory state). Overestimating usage against a cap can
only make the system reject sooner than the letter of the configured
limit requires -- never admit more than it allows. That is the safe
direction for a limit.

## Decision

Each process periodically writes its own current usage to a key **it alone
ever writes**, and periodically reads every other process's key to compute
the fleet-wide picture. No key is ever contended: every writer owns its
key exclusively, so there is no CAS, no retry-on-conflict, and no hot
single object for a busy tenant to pile onto.

### 1. Per-process usage snapshot

Every reconciliation interval `R` (see §4 for the value), each process
writes one object per (tenant, signal) it has active state for:

```
t/<tenant_hash>/<sig>/admission/<process_id>.snapshot
```

`process_id` is a value stable for the process's lifetime and unique
fleet-wide (a UUID generated at startup is sufficient; it need not be
meaningful outside this mechanism). The write is `PutMode::Overwrite` --
not `CreateIfAbsent`, not `CasVersion` -- because only the owning process
ever writes this key, so there is no concurrent writer to race against,
ADR-0051's rejected-alternative concern does not apply, and a failed or
stale snapshot write is self-correcting on the next interval rather than
requiring conflict resolution.

The snapshot body is a small protobuf message carrying, per cap:
- `active_series_count`, `active_streams_count`: current `EpochIdSet`
  sizes (an upper bound on this process's contribution to the fleet-wide
  distinct count, per the Context section's reasoning).
- `byte_rate_consumed_since_last_snapshot`, `series_creation_consumed_
  since_last_snapshot`: bytes and new-identities admitted since this
  process's own previous snapshot -- a delta, not a running total, so
  reconciliation sums genuinely fresh flow rather than re-summing history.
- `snapshot_unix_ns`: this process's own clock at write time, for staleness
  detection on the reading side (§3).

A process with no active state for a (tenant, signal) writes nothing (or
lets a prior snapshot expire, see §5) rather than writing an empty
snapshot for every tenant it has never seen -- most processes see most
tenants never, and this avoids an all-processes-times-all-tenants object
count.

### 2. Reconciliation read and the local soft threshold

On the same interval `R`, each process lists the
`t/<tenant_hash>/<sig>/admission/` prefix for every (tenant, signal) it is
itself currently tracking usage for, GETs every sibling snapshot found,
and computes a threshold that replaces the raw configured cap as the
value `AdmissionController`'s existing hot-path check compares against,
for this (tenant, signal), until the next reconciliation. The hot-path
check itself is **unchanged**: same mutex, same hashmap, same token
bucket and `EpochIdSet` logic, same sub-microsecond cost -- only the
number it compares against now comes from the last reconciliation instead
of being the static configured limit. `configured_fleet_cap` is the value
an operator sets once, meaning the fleet-wide total, not per-process --
closing the exact gap issue #656 names: sizing a cap no longer requires
dividing by replica count.

Count and rate caps need different formulas, because one is a stock and
the other is a flow.

**Count caps** (`active_series`, `active_streams`) use additive headroom:

```
fleet_used(cap) = own_current_usage(cap) + sum(sibling_snapshot.usage(cap) for each non-stale sibling)
fleet_remaining(cap) = configured_fleet_cap(cap) - fleet_used(cap)
local_soft_threshold(cap) = own_current_usage(cap) + max(0, fleet_remaining(cap))
```

This is correct for a stock: `own_current_usage` only grows through
admissions this same threshold gates, so once the fleet is at or over
cap, the threshold collapses to `own_current_usage` -- this process
admits nothing further, and the value doesn't move again until a
tenant's usage actually drops. A stable fixed point.

**Rate caps** (`ingest_byte_rate`, `series_creation_rate`) do not have
that property, and using the same formula for them was a bug the
checkpoint reviewing EF-T1 caught before it landed (2026-08-06):
`own_current_usage` for a rate cap is a *measured* flow rate, not a
stock the threshold controls. Once the fleet crosses the configured cap,
every process's `own` reading is "whatever I'm already sending," so the
additive-headroom formula pins every process's threshold to its current
rate and never reduces it -- the fleet sustains the sum of everyone's
uncapped rate indefinitely, not for one interval.

Rate caps instead use an equal fleet-share of the configured cap:

```
N(cap) = 1 (self) + count of non-stale siblings that reported a snapshot for this (tenant, signal), regardless of whether their reported delta for this cap was itself zero
local_soft_rate_threshold(cap) = configured_fleet_cap(cap) / N(cap)      // integer division, floor
```

Every process computes `N` from the same non-stale sibling set it already
reads for the count formula, so once every process's view of `N` agrees
-- which happens within one interval `R` of the last change to who's
live -- the sum of every process's `local_soft_rate_threshold` is at most
`configured_fleet_cap` (floor-rounding can only lose up to `N - 1` total
from the cap, never exceed it). This is deliberately the boring choice:
it is stable under reconciliation lag, since a process's threshold at a
given `N` is a fixed value rather than something recomputed against
siblings' possibly-stale demand every interval -- an oscillation risk a
demand-proportional split (e.g. weighting each process's share by its own
measured rate) would carry, since every process would be scaling itself
down against a denominator that is itself up to `2R` stale, all at once,
every interval. The cost is that an idle process's fair share sits unused
rather than being reallocated to a busy sibling; making the split
demand-aware is left to a future ADR if that waste is ever shown to
matter in practice, and nothing here blocks it -- this is a private
helper internal to reconciliation, not a wire-format or hot-path
commitment.

### 3. Staleness: fail closed on caps, not on flow

If a sibling snapshot's `snapshot_unix_ns` is older than `2 * R` when
read, exclude it from `fleet_used` and treat that sibling as having
reported zero -- the reader assumes a silent/dead process is not
consuming budget rather than assuming it still is. This is the opposite
fail-closed direction from ADR-0052's routing staleness (which refuses to
route rather than guess): here, guessing "stale sibling = zero" is the
conservative choice for a process that might have simply been slow to
write its own snapshot (its own hot-path enforcement is unaffected either
way, since it always includes `own_current_usage` un-staled), while
treating it as unknown-and-blocking would make one slow reconciliation
write anywhere in the fleet freeze every other process's admission
decisions -- an availability cliff this ADR explicitly rejects (see
Rejected Alternatives).

If a process cannot complete its own reconciliation read at all (the
LIST or a GET fails), it keeps using its last-computed
`local_soft_threshold` until the next interval succeeds, and increments a
new counter (`ravel_admission_reconciliation_failures_total`, per-tenant)
so a sustained failure is observable rather than silently degrading
accuracy indefinitely. It does not fail closed to zero admission on a
single failed reconciliation -- a transient store hiccup must not turn
into an ingest outage for every tenant on that process, which would be a
strictly worse failure mode than the N-times-quota gap this ADR exists to
narrow.

### 4. The reconciliation interval `R` and the overshoot bound

`R` defaults to 10 seconds. The bound this ADR gives up in exchange for
zero hot-path cost differs in shape between count caps and rate caps,
because their formulas differ (section 2).

**Count caps:** in the worst case, every process's local view is exactly
`R` stale relative to its siblings, so the true fleet-wide total can
exceed the configured cap by at most the sum, across all processes, of
what each could admit in one interval `R` under its own
`local_soft_threshold` at the moment reconciliation last ran. This is
bounded, not unbounded, and shrinks to the configured cap within one
further interval once any process observes the overage. It is the same
shape of tradeoff ADR-0052 section 3 makes for its slack window `S`:
a stated, finite window instead of either "perfectly exact" or
"unbounded," chosen because the alternative (a hot-path round-trip) has
already been rejected for good reason.

**Rate caps:** the overshoot here is driven by disagreement over `N`
rather than by any one process's own admitted delta. A process that just
joined the fleet contributes no snapshot until its own first
reconciliation write, so existing siblings continue enforcing their
share of the *old*, smaller `N` until their own next tick observes the
newcomer -- for at most one interval `R` after a join, the fleet's
combined enforced rate can be as high as `N_old * (cap / N_old) + cap /
N_new` (the existing siblings' unchanged shares, plus the newcomer's own,
already-correct share), which is at most one extra full `N`-th share
above the steady-state `configured_fleet_cap`. It converges to at most
`configured_fleet_cap` once every process's next tick lands, within a
further `R`. A process leaving or going stale has the opposite,
safe-direction transient: the remaining processes briefly under-use their
true fair share until their own next tick raises `N`'s denominator back
down, never an overage.

`R = 10s` is chosen to keep the LIST-then-N-GETs reconciliation read cheap
(a realistic gateway replica count is single digits to low tens per the
operator's own default of 1 replica and no documented guidance suggesting
more; N processes reading N-1 siblings' objects every 10 seconds is a
request rate the object store already sustains for far hotter paths in
this system) while keeping the overshoot window short enough that an
operator sizing a hard business cap does not need to pad it materially to
absorb reconciliation lag. An operator who needs a tighter bound can
configure a shorter `R`; this trades reconciliation request volume for a
smaller overshoot window, a knob this ADR exposes rather than hard-codes.

### 5. Snapshot lifecycle: no new sweep needed

A process's own snapshot for a (tenant, signal) it stops tracking (the
tenant goes idle, or the process is draining) is left in place rather
than actively deleted -- deleting it would require the same delete-grant
reasoning ADR-0055 just narrowed, for no real benefit, since a stale
snapshot is already excluded by staleness (§3) after `2 * R`, at most 20
seconds by default. `crates/ravel-maintain`'s existing sweep machinery
(the same one that already horizon-gates `idem/`) is the natural home for
an eventual bounded cleanup of long-abandoned snapshot keys if their
accumulation ever matters in practice; this ADR does not add that sweep
now, since an unbounded number of small, cheap-to-ignore stale objects
under one more `admission/` prefix is a materially smaller concern than
the `idem/` keyspace growth ADR-0055 and issue #656's sibling findings
already track, and speculative cleanup work is exactly the kind of
premature scope this program's own discipline argues against.

## Rejected alternatives

**Re-litigate ADR-0051's rejected S3-CAS shared counter.** Rejected again,
for the same reason ADR-0051 gave: a request-path round-trip against one
mutable, CAS-contended object is a real latency and correctness cost this
ADR's whole design exists to avoid. Nothing in review v2 or issue #656
presents new information that changes that calculus -- issue #656 itself
explicitly invites the hybrid this ADR delivers instead.

**A small coordinator service.** Issue #656 named this as a candidate.
Rejected: it is a new stateful, network-reachable process with its own
availability and durability story, and CLAUDE.md's invariant that "no
durability may depend on local disk, and no recovery path may read state
another process wrote locally" argues against introducing a new
non-object-storage durability dependency for a control this ADR can
deliver entirely on the storage substrate every process already trusts.
The self-owned-key design gets the same fleet-wide visibility with no new
component, no new failure mode class, and no new thing to operate.

**Treat a stale sibling snapshot as unknown-and-block, rather than
zero-and-continue.** Rejected: this would make one process's slow or
failed reconciliation write propagate into every other process refusing
to admit for that tenant, fleet-wide -- trading a bounded overshoot for
an availability cliff triggered by a single slow write. The chosen
direction (assume a stale sibling is contributing zero) accepts a
marginally wider overshoot bound in the rare case a genuinely-still-busy
process's write is merely delayed, in exchange for never letting one
process's hiccup take down admission for the whole fleet.

**Exact fleet-wide cardinality for the count caps (a CRDT-style merged
set, or a per-series ownership handoff so each series is tracked by
exactly one process).** Rejected as unjustified complexity for this
epic: the safe-overestimate property established in Context already
gives a correct (if conservative) cap enforcement without ever tracking
which process a given series is "really" owned by, which would require
either a coordination protocol on the write path (exactly the cost this
ADR avoids) or an eventually-consistent merge with its own staleness
story layered on top of the one this ADR already introduces. Revisit only
if the overestimate is measured to matter in practice (a tenant's traffic
genuinely alternating across replicas per-series at a rate that makes the
double-count material), which is not a reported problem today.

**Sticky/consistent-hash routing of ingest traffic per tenant, to reduce
the actual cross-replica overlap this ADR's overestimate is a hedge
against.** Rejected as out of this ADR's scope: it is a load-balancing
change to how traffic reaches gateway replicas at all (likely outside
Ravel's own code, at whatever ingress/load-balancer sits in front of it),
orthogonal to admission enforcement, and would only reduce -- not
eliminate -- the need for the reconciliation mechanism this ADR builds
regardless (multiple replicas for HA means some series will always cross
replicas on failover even under sticky routing). Worth an operator-facing
note in the eventual docs, not a redesign of this ADR.

## Consequences

- **The hot path is unchanged.** Same mutex, same hashmap, same token
  bucket, same sub-microsecond cost. The only new cost is a background
  task per process, off the request path, on interval `R`.
- **The configured cap becomes genuinely fleet-wide.** Count caps are
  bounded by at most one reconciliation interval's worth of admission per
  process (section 4); rate caps converge to the configured cap within
  one interval of the fleet's live-process set settling (also section
  4) -- both a stated, finite overshoot window, not an operator-computed
  per-process fraction. Operators no longer divide a business cap by
  replica count (`docs/guides/admission-limits.md`'s existing
  manual-division guidance, from ADR-0051, is superseded and corrected in
  this same commit).
- **A new object-store keyspace**, `t/<hash>/<sig>/admission/<process_id>.
  snapshot`, small, per-process, self-owned, no CAS. Falls under
  Maintain's existing write/delete grant in ADR-0055's role model for any
  future cleanup sweep, and Gateway's existing write grant for the
  snapshot writes themselves (an additive grant ADR-0055's landed policy
  templates will need updating to include -- filed as a follow-up on
  landing, not blocking this ADR's approval).
- **Count-cap enforcement becomes a safe overestimate, stated as such**,
  rather than an exact fleet-wide cardinality -- consistent with this
  program's "exact semantics by default, approximation opt-in and
  visible" invariant, since the approximation direction (over-counts,
  never under-counts) and its cause are both documented here rather than
  silently accepted.
- **Availability under a reconciliation failure degrades to today's
  per-process behavior** (the last-known soft threshold, itself no worse
  than the configured cap), not to zero admission -- a sustained failure
  is observable via the new counter rather than silent, but never becomes
  an outage on its own.
- **Does not close #491** (the ravel-ingest/ravel-server default
  discrepancy) -- unrelated, already tracked separately.

## Correction (2026-08-06)

The rate-cap formula in section 2 and the rate-cap overshoot bound in
section 4 were corrected before this ADR's first implementation (issue
#677) landed. The originally accepted text applied the count-cap
additive-headroom formula to rate caps unmodified; a checkpoint review of
the implementation caught that this formula does not converge for a flow
quantity (see section 2's "Rate caps" subsection for the failure mode and
the corrected equal-fleet-share formula). No deployment ever ran the
original formula -- issue #677's implementation was blocked on this
finding before landing.
