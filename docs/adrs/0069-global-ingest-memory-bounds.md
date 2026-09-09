# ADR-0069: Global ingest memory bounds and idle-tenant state eviction

Status: Accepted

## Context

Ravel's memory model is configuration-bounded per tenant and per query,
with no unbounded channels anywhere. But nothing bounds the *sum*:

- Ingest buffers are capped per (tenant, shard, signal) at ~8 MiB, so the
  worst case is roughly `tenants x shards x signals x 8 MiB` — unbounded
  in active-tenant count. On the 8 GB hosts the fleet actually runs, a few
  dozen active tenants can theoretically exceed RAM before any per-tenant
  limit trips.
- Every parked strict request holds its decoded, normalized points for up
  to the 10 s ack deadline. A separate transport concurrency limit bounds
  the number of parked requests; it does not bound their bytes.
- Per-tenant map entries are never evicted for idle tenants: admission
  controller state, generation-switch views (old shard sets are never
  removed), the catalog's per-tenant cache outer maps, and SQL memory
  accountants all grow monotonically over process lifetime.

TigerBeetle answers this with fully static allocation. That model does not
fit a multi-tenant elastic system: tenants appear and disappear, and
cardinality swings by orders of magnitude. But the underlying principle —
know the bound, enforce it explicitly, fail with backpressure instead of
OOM — applies directly.

## Decision

1. **A process-wide ingest byte budget.** One atomic gauge charges the
   estimated bytes of buffered ingest state at admission (after decode,
   before buffering) and refunds on flush completion or error. A new flag
   (`--max-ingest-buffer-bytes`, default 512 MiB, 0 = unlimited) sets the
   ceiling. At the ceiling, new writes are shed exactly like any admission
   failure — HTTP 429 with Retry-After, gRPC RESOURCE_EXHAUSTED — before
   any buffering, so strict-mode semantics are untouched. The gauge and a
   shed counter render on /metrics inside the existing label allowlist.
   Interacts with ADR-0067: in-flight pipelined flushes stay charged until
   their PUTs complete, so pipelining depth is automatically accounted.
2. **Idle-tenant eviction, only for re-derivable state.** A background
   sweep (jittered interval, same worker shape as every other loop) evicts
   per-tenant entries idle past a threshold (default 1 h) from: generation
   views (re-read from the provisioning record on next touch), catalog
   per-tenant cache maps (already reconstructible by definition), and SQL
   memory accountants with zero outstanding reservations. Admission
   controller state is explicitly **excluded** in this ADR: its
   active-series/stream counts are correctness-bearing caps, and evicting
   them silently resets a tenant's cap consumption. Whether ADR-0057's
   fleet reconciliation records make admission-state eviction safe is a
   separate follow-up decision; until then that map grows with tenant
   count and is documented as doing so.
3. **A documented boundedness statement** in docs/ingest.md: worst-case
   process RSS as the sum of the ingest budget ceiling, cache byte caps,
   per-query budgets x concurrency ceiling, and fixed overhead — every
   term a named config knob.

```mermaid
flowchart LR
    REQ[decoded request] --> ADM{admission:\nlayers 1-4\n+ global byte gauge}
    ADM -->|over ceiling| SHED[429 / RESOURCE_EXHAUSTED\nshed counter++]
    ADM -->|charged| BUF[shard buffers]
    BUF --> FL[flush + PUTs]
    FL -->|complete or error| REFUND[gauge refund]
    SWEEP[idle-tenant sweep] -. evicts re-derivable state only .-> MAPS[(generation views,\ncatalog outer maps,\nSQL accountants)]
```

## Rejected alternatives

- **Full static allocation** (TigerBeetle's model): requires fixing tenant
  count and per-tenant shape at startup; wrong for an elastic multi-tenant
  service and would waste most of an 8 GB host or hard-cap tenancy.
- **Per-tenant fixed reservations**: same waste problem in different
  clothes; the global gauge admits bursty tenants opportunistically while
  keeping the sum bounded.
- **LRU caps on the admission maps**: silently discards correctness-bearing
  cap state; a tenant's active-series count must never reset as a side
  effect of memory pressure.
- **Relying on the transport concurrency limit alone**: bounds request *count*,
  not buffered *bytes*; a small number of maximal 16 MiB requests across
  many tenants still needs the byte gauge.
- **cgroup/OOM-based limits**: an OOM kill loses every in-flight strict
  request and looks like a crash to clients; explicit shed is strictly
  better and is the existing admission idiom.

## Consequences

- A new global admission check on the hot path: one atomic add/sub per
  request — negligible next to existing per-request work.
- Under sustained overload the system now sheds with 429 instead of
  growing RSS; clients with retry/backoff (the OTLP norm) experience
  backpressure, not data loss.
- Eviction introduces a re-read cost on first touch after idleness
  (one provisioning-record GET for generation views); bounded and rare.
- The admission-state exclusion is an honest gap, documented, with a named
  follow-up rather than an unsafe eviction.

## Implementation notes (idle-tenant eviction)

Decision 2 is implemented as one background sweep in `services/ravel-server`
(`idle_tenant_state.rs`), spawned alongside the other worker loops with the
same jittered-interval shutdown shape and driven by the injected `SystemClock`.
The flag is `--idle-tenant-state-ttl` (default `1h`, `0` disables the sweep).
Each owning crate exposes a deterministic, clock-free eviction entry point that
the sweep drives with the injected `now_ns` and the TTL:

- `ravel-ingest`: `GenerationSwitch::evict_idle` (via each router's
  `evict_idle_generation_views`). Last touch advances on every cache-hit route
  and every refresh; an evicted view re-reads the provisioning record on the
  next write and succeeds. Only the per-tenant `views` map is swept — the
  shared-by-count `sets` map is topology, not per-tenant idle state.
- `ravel-catalog`: `Catalog::evict_idle_tenants`. Last touch is stamped per
  tenant at the head of `resolve_impl`; a swept tenant loses its whole
  outer-map entry in every per-tenant decoded cache and re-reads on the next
  resolve. The process-wide content-hash-keyed byte cache is not partitioned
  per tenant and is reclaimed by its own capacity bound instead.
- `ravel-sql`: `SqlExecutor::evict_idle_accountants`, guarded on zero
  outstanding reservations so an accountant backing a live query is never
  evicted (that would stop the tenant ceiling being shared across its
  concurrent queries). Last touch is stamped in the shared `resolve` funnel, so
  both the HTTP and Flight SQL paths keep an active tenant out of the sweep.

The admission-controller maps (active-series/stream sets, token buckets,
usage counters) are the one long-lived per-tenant map family that is
deliberately *not* an evictor and never registered with the sweep, so its
exclusion from decision 2 is structural, not merely a convention.

The OTLP HTTP handler's exemplar admission (`services/ravel-server/src/ingest.rs`,
wired through `normalize_metrics_with_exemplars`) does not add a second one:
its `ExemplarCap` (ADR-0047 decision 2) is built fresh per request and
dropped at the end of the call, the same shape every other OTLP normalize
entry point already uses. The real per-series-per-window budget is enforced
per shard actor with no cross-shard coordination (`crates/ravel-ingest/src/shard.rs`),
which already builds and drops its own cap per flush -- a shard-lived (or
longer) cap was considered and rejected there as an unbounded memory-growth
vector, since `ExemplarCap`'s per-series map has no eviction. Nothing at the
transport layer needs to, or should, outlive one request.

## Amendment (2026-09-07): the OTLP HTTP gzip inflate is charged pre-inflate

This amendment appends to decision 1; it moves the charge point for one
transient and, in doing so, redefines what the gauge measures. Everything
else above stands unchanged.

### Context

Decision 1 charges "at admission (after decode, before buffering)". That
placement left the OTLP HTTP gzip decompression buffer outside the ceiling.
`services/ravel-server/src/otlp_http.rs` takes the in-flight-request permit,
then decompresses the body up to `MAX_DECOMPRESSED_OTLP_BODY_BYTES` (64 MiB),
and only afterward does the router take the decision-1 charge. So
`--max-inflight-ingest-requests` copies of a 64 MiB inflate -- 64 GiB at the
default 1024 -- could exist at once before a single byte was charged. The
`--max-ingest-buffer-bytes` flag and docs/ingest.md both claimed a bound that
this transient escaped: the flag overclaimed (issue #1297).

### Decision

The gzip inflate path charges the process-wide `IngestByteBudget` for the
bytes it decompresses, **before it finishes inflating**, incrementally as each
chunk is produced. A decompression whose running total would cross the ceiling
is shed mid-inflate (OTLP HTTP 429 with `Retry-After`, the existing shed
counter) rather than being allocated in full and charged afterward. This
amendment covers OTLP HTTP gzip only; OTLP gRPC gzip decompression is unchanged
and tracked separately (#1419), so no gRPC status is described here. The charge
is held as an RAII guard through protobuf decode and released once decode has
consumed and freed the inflate chunks -- prost copies them into owned structs --
before the router takes its own decision-1 buffered charge. A single request's
inflate charge and buffered charge therefore never coexist: the peak that one
request contributes to the gauge is the larger of the two, not their sum.

The decompressed body is retained as the list of exactly-sized chunks that were
charged for, and decoded through that list as a non-contiguous `Buf`, rather than
appended into one growing `Vec<u8>`. This is what makes the charge equal the
retained bytes at every instant: an amortized-growth buffer holds spare capacity
past its length, and holds the old and the new allocation simultaneously while it
reallocates, so a charge taken on the appended length would undercount the very
buffer this amendment exists to bound (`reserve_exact` does not close the gap,
because an allocator may return more than was requested).

**What the gauge now means.** Before this amendment `ravel_ingest_buffer_bytes`
measured buffered ingest state only. It now also counts the transient OTLP HTTP
gzip decode state currently in flight: a request that is mid-inflate holds a
decode charge on the same gauge, alongside every other request's buffered
charge, so the gauge reflects the concurrent inflate buffers that used to be
invisible. For that decode term the gauge counts the bytes the request has
actually retained -- the summed length of its inflate chunks, which because each
chunk is allocated once at its final size is also the number of bytes those
allocations occupy -- not an estimate and not a length that a spare-capacity or
mid-reallocation buffer would exceed. A single request does not hold both charges
at once -- it releases the decode charge once decode has consumed and freed those
chunks, before the router takes its buffered charge -- so that one request's
contribution is the larger of the two, never their sum. This is deliberate: the ceiling bounds peak
resident ingest memory, and the concurrent inflate buffers are part of that
peak. The identity (uncompressed) path allocates no inflate buffer and takes no
gateway charge; the OTLP gRPC and Remote Write decode paths are out of scope
here and remain bounded by `--max-inflight-ingest-requests` alone.

**Why the actual inflated length, charged incrementally.** Charging the 64 MiB
cap up front would over-charge every well-compressing request and shed real
traffic under a tight budget. Charging a compressed-size estimate would either
over- or under-charge depending on the ratio. Charging each produced chunk
makes the summed charge equal the actual decompressed length exactly: the
over-charge bound for an admitted request is **zero**, and the uncharged
allocations are a fixed staging-and-decoder cost plus per-chunk bookkeeping
that scales with chunk count: one staging chunk (64 KiB) per in-flight inflate,
which transiently holds one chunk of decompressed bytes; per-chunk bookkeeping
(a `Bytes` handle and a charge guard for each retained chunk, about 48 bytes
per 64 KiB chunk, held in two vectors that grow by doubling); and flate2's own
decoder state (tens of KiB). The compressed request body itself also stays
resident for the whole inflate, but it is bounded by the wire-body cap
(`MAX_REQUEST_BODY_BYTES`, 16 MiB, ADR-0051 section 1 and section 3) and
already counted against `--max-inflight-ingest-requests`, not left uncharged
here. No uncharged allocation holds a copy of the full decompressed body; the
staging chunk holds only one chunk at a time, and it and the decoder state are
a fixed cost that alone can exceed the charge itself on a small decompressed
body. A shed request refunds every partial chunk on the spot.

### Rejected alternatives

- **Documentation only** (correct the flag docs, leave the resource
  unbounded): rejected. It leaves 64 GiB of possible inflate outside the
  ceiling and keeps the flags claiming a bound they do not enforce.
- **Lower the `--max-inflight-ingest-requests` default**: rejected. It caps
  concurrency for every caller to paper over an accounting gap, and the gap
  (uncharged inflate) would still exist at the lower concurrency.
- **Charge after inflate** (keep decision 1's point, add the decompressed size
  once decode finishes): rejected. The peak *is* the inflate; charging after
  it has already been allocated does not bound the transient this amendment
  exists to bound.
- **Reserve `max_inflight_requests x cap` statically**: rejected for the same
  reason decision 1's own "Full static allocation" bullet rejects it -- it
  wastes most of an 8 GB host or hard-caps tenancy, and the point of the global
  gauge is to admit bursty requests opportunistically while keeping the sum
  bounded.

### Model (RUST_ONLY)

The formal model is unaffected. Admission shedding happens strictly before
`PinFlush`, and the model has no admission action, so a shed request mints no
flush: there is no new transition, no new commit-protocol interleaving, and
nothing for the TLA+ model to cover. This amendment changes only where in the
Rust ingest path a byte is charged, not the durability or visibility state
machine.
