# ADR-2793: Strict flush-on-arrival ack policy and admission permit release

Status: Accepted. The 2026-10-14 amendments at the end were approved by
the maintainer in their revised text on 2026-10-14.
Date: 2026-10-10
Epic: #2793
Amends: ADR-0076 decision 4 (adds a second axis; the aged flush stays the
default)

## Context

A strict-mode write is acknowledged after the flush carrying its rows has
stored its data object and published its commit record. Today that flush
opens when the shard's age tick finds the buffer older than
`max_flush_delay` (2 s), checked once per `flush_tick` (200 ms). ADR-0076
decision 4 chose the 2 s delay as a PUT-cost lever: a buffer that waits
longer coalesces more writes into one object, and it named the cost: "this
decision costs strict-mode acknowledgement latency, and that is its real
price."

Measured on a fresh c6a.4xlarge against real S3 (main e20cd8316, DE review
2026-10-10, figures in epic #2793):

| Figure | Value |
|---|---|
| Strict OTLP ack p50, one record | 2.207 s |
| Strict OTLP ack p50, 1,000 records | 2.205 s |
| Strict OTLP ack p50, 8 concurrent clients | 2.202 s |
| Buffered ack | 0-1 ms |
| Data PUT, commit PUT | about 31 ms each |

Storage is not the cost. The wait is the age tick. Two consequences follow
from where that wait sits:

1. **A serial client is latency-bound.** A client that sends one request at
   a time, which is how an OTLP exporter behaves on a single queue, gets
   0.45 requests per second whatever the server's CPU.
2. **The admission permit is held across the wait.** The process-wide
   `--max-inflight-ingest-requests` permit (default 1,024) is taken by the
   admission middleware at the request head and released when the handler
   returns, after the strict ack (`services/ravel-server/src/ingest_admission.rs:246`,
   held across `next.run(request).await`). Its stated purpose is transient
   decode memory: "it bounds request COUNT, so the transient decode memory
   it caps is this ceiling times the largest per-request decoded body"
   (`config.rs:1555-1559`). Held for 2.2 s per strict request, it caps the
   process at about 465 strict requests per second regardless of CPU.

The shard actor already has a flush path that runs with no tick: the size
trigger. `handle_write` merges the rows, pushes the strict waiter, and if
`flush_est_bytes >= target_bytes` calls `flush_tenant(.., FlushTrigger::Size, ..)`
in the same call (`crates/ravel-ingest/src/shard.rs:1391-1405`, log and span
actors likewise). The bulk loader sets `target_bytes = 1` and gets exactly
that: every strict write flushes inside its own handler and acks at two PUT
round trips. Fleet experiment E1 (task 8314ff33) prototyped the other shape,
a zero age threshold for a buffer with a strict waiter: 2,200 ms to 200 ms
in-process, still one tick, two PUTs per write on both arms.

Selection today is per request: `x-ravel-ingest-mode: buffered` on the OTLP
HTTP, gRPC and OTAP surfaces (`otlp_http.rs:685-694`), honored for any
tenant. Remote Write ignores the header and is strict-only, because a
Remote Write sender drops its WAL entry on any 2xx. `docs/ingest.md:1486`
claims a per-tenant gate on buffered mode; no such gate exists in the tree,
and `docs/guides/ingest.md:139` says so. That stale sentence is reported
with this ADR, not fixed by it.

## Decision

**1. A strict write may select flush-on-arrival; the aged flush stays the
default.**

`WriteMode` gains a third variant, `StrictArrival`. Its acknowledgement
contract is identical to `Strict`: the ack follows the commit-record PUT,
carries one commit token per shard flushed, and is never answered from a
flush the deferral cap abandoned. What changes is when the flush opens.

Selection:

- `--strict-flush-policy aged|arrival` (default `aged`) on `ravel-server`
  is the process default: what a strict request gets when it does not
  name a policy. It applies to every strict surface, Remote Write
  included, since Remote Write reads no header and its ack contract is
  unchanged by the policy.
- The header `x-ravel-ingest-mode` on the OTLP HTTP, gRPC and OTAP
  surfaces, read by the same parser that reads `buffered` today, gains two
  explicit values that override the process default for one request:
  `strict-arrival` and `strict-aged`. `buffered` keeps its meaning. The
  value `strict`, an absent header, and any unknown value all mean "strict
  under the process default", so `strict` is not an opt-out of
  `--strict-flush-policy arrival`; `strict-aged` is.

A per-tenant setting is deliberately not part of this ADR. `TenantConfig`
is a protobuf record whose field set is versioned (`proto/ravel/sys.proto`
version history at the `TenantConfig` message), so a tenant field is a
format change under the format-change procedure. The per-request header
plus the process default cover the two callers this ADR is for: a client
that wants low-latency acks on its own exports, and an operator running a
deployment where every tenant wants them. A follow-up may add the field
once a deployment needs a per-tenant default that differs from the process
default; the policy value and the actor mechanism below are the same
either way.

**2. The arrival flush runs inside the write handler, bounded to one
in-flight flush per (shard, tenant).**

In each shard actor (`shard.rs`, `log_shard.rs`, `span_shard.rs`) the
message carries the policy alongside the ack. `handle_write` keeps its
order: deferral-cap refusal, merge, charge, push waiter, size check. After
the size check, if the write is strict-arrival and the size trigger did not
already fire:

- If this tenant has no flush in flight on this shard, the actor calls
  `flush_tenant(.., FlushTrigger::Arrival, LagCheck::Enforced)` now, the same
  call the size trigger makes. No tick is involved.
- If this tenant has a flush in flight on this shard, the buffer is marked
  `arrival_pending` and left in place. When the actor reaps that flush
  (the `flushes.join_next()` arm of `run`), it flushes the pending buffer
  if it still holds waiters. Writes that arrive meanwhile merge into the
  same buffer and ride that one flush.

The in-flight count the actor reads is the one ADR-2708 decision 3
introduces (accepted 2026-10-09, landing under #1921): spawned, unreaped
flushes counted per (shard, tenant), with the tenant identity returned from
the flush task so the reap arm can decrement it. This ADR adds no second
count. The arrival rule reads that count as "zero means flush now"; the
reap arm, which decision 3 already makes tenant-aware, additionally checks
the reaped tenant's buffer for `arrival_pending`. The shard-actor work here
is sequenced after #1921 lands, on top of its count, never beside it.

Every other rule of the flush pipeline applies unchanged. The arrival flush
acquires the shard's `max_inflight_flushes` permit inside the spawned task
(ADR-1642) and waits behind another tenant's flush if the permits are
held; past the deferral cap it is abandoned with the same `Abandoned`
answer. It is exempt from the queued-flush cap and from decision 3's
per-tenant share by construction, not by a new rule: it opens only when
the tenant has zero flushes in flight on the shard, and decision 3 states
that "a tenant with no flush in flight is never refused by the queued-flush
cap", with a share of at least one. So no queued-flush-cap or
per-tenant-share deferral path exists for an arrival flush, and an
implementer must not add one; the flush deferral cap and
`LagCheck::Enforced` are a different mechanism and stay exactly as the
size path has them. The memory argument is
decision 3's: the exemption adds at most one window per active tenant on
the shard, charged to the byte budget. A buffer whose arrival flush is
pending behind an in-flight one still holds a strict waiter, so it stays
in the priority age tier: the `max_flush_delay` tick is its fallback
floor, and the worst case for that buffer is today's behavior.

This is the PUT-rate bound. Per (shard, tenant) the arrival policy issues
at most one flush per flush duration, two PUT round trips, about 60-70 ms
on S3, so at most about 16 flushes per second per shard per tenant at
saturation, against one per 2 s under the aged policy (`max_flush_delay`
2 s). A tenant sending one strict request per 2 s costs the same under
both. A tenant sending ten per second on one shard costs up to 20x the
PUTs it would under the aged policy (each request's own flush is its
upper bound, 10 against 0.5 per second); a tenant at or past 16 per
second reaches the saturation ratio of 32x. That is why the policy is
opt-in and not the default. Per shard,
the `max_inflight_flushes` permits (1 in the tree today, 4 with a
per-tenant share of 3 under ADR-2708 decision 3) bound the shard's total
flushes in flight across tenants, so no tenant's choice can push a shard
past its permit count.

**3. `FlushTrigger::Arrival` is counted on its own, and so is coalescing.**

A new trigger variant `Arrival` with counter `flushes_by_arrival` in all
three pipelines' metrics, exported beside `flushes_by_size` and
`flushes_by_age`, never folded into `flushes_by_age`. A second counter,
`arrival_coalesced`, increments once per strict-arrival write that found a
flush in flight and waited for the reap. Both are what a test asserts to
prove the bound fired: with every PUT held by a store gate so no flush can
be reaped until all eight writes have landed, a run of eight concurrent
arrival writes on one shard asserts `flushes_by_arrival <= 2` and
`arrival_coalesced >= 6`, not only an ack latency. The held store is the
band's precondition, stated in the test: against an ungated store a reap
between two writes opens a third flush and the band does not hold.

**4. The admission permit is released at enqueue; pending strict acks get
their own bound.**

The router's write splits into two phases: `submit`, which takes a
pending-ack permit for a strict write, then charges the ADR-0069 byte
budget, sends to every involved shard and returns a `Submitted` handle,
and `Submitted::acks()`, which awaits the strict acks under the ack
deadline and returns the receipt. The permit comes before the charge so a
pending-ack refusal leaves nothing charged and nothing to release. `write`
remains as the composition of the two for callers that do not care.

The admission layers stop holding the permit for the handler's lifetime,
with one mechanism per transport, because the two transports drop request
extensions at different times:

One ownership model on every transport: a release handle, an
`Arc<Mutex<Option<IngestPermit>>>`, owns the permit; the admission layer
holds one clone of the handle for the request's lifetime and the handler
takes another. Emptying the slot releases the permit; dropping a clone
never does. A handler that never reaches `submit` leaves the slot full,
and the layer's clone releases the permit when the request ends, as today.

- HTTP: the middleware (`ingest_admission.rs:246`) moves the permit into a
  handle, keeps a clone across `next.run(request)`, and puts another in
  the request extensions. The OTLP metrics, logs and traces handlers and
  the Remote Write handler (`remote_write.rs:334`, behind the same
  middleware, strict-only by construction and so the surface that holds
  its permit across every ack today) clone the handle out, call `submit`,
  empty the slot, then await `acks()`.
- gRPC: the layer (`ingest_admission.rs:449-455`) stops moving the bare
  permit into its own future (`let _permit = permit;`) and holds a clone
  of the handle there instead, with another clone in the extensions. The
  handler reads it from the extensions before `request.into_inner()`, the
  way it reads the admission marker today, so tonic dropping the
  extensions mid-handler drops a clone, never the permit. On the
  direct-call path, where no layer ran and `admit_grpc_request` took the
  permit itself (`otlp_grpc.rs:78-84`), the handler owns that permit and
  drops it after `submit`. The existing marker is not precedent for any
  of this: its comment (`ingest_admission.rs:84-96`) exists to forbid a
  bare permit in extensions, and the handle design honors that reason
  because the layer always holds a clone of its own.
- OTAP: the batch handler already holds its permit locally and drops it
  after `submit`.

Why releasing at enqueue keeps the permit's stated purpose, decode
memory, covered: on the HTTP and gRPC paths the decoded request is moved
into normalization (`ingest.rs:149-205` and the log and span
equivalents), which consumes it before the router is called, and the
rows a strict request then holds in a shard buffer are charged to the
byte budget. OTAP is the exception: `process_batch` keeps the decoded
batch alive across `write_batch` by reference (`otap_grpc.rs:220-223`,
`306-312`), so it would stay resident through the ack wait. The OTAP
handler therefore narrows the batch's scope so the decoded batch is
dropped once `submit` has returned, then drops its permit, then awaits
`acks()`; until the batch is dropped the permit is held. A test pins
that order on OTAP.

What the permit covered by accident, the count of held response contexts,
gets an explicit bound: `--max-pending-strict-acks` (default 8,192, 0 =
unlimited). `submit` takes a pending-ack permit for a strict write before
sending (see the head pending-ack amendment below: a write admitted at the
head arrives holding one), and `Submitted` holds it until the acks resolve or the deadline
passes. A strict request refused here gets the same 429 with `Retry-After`
and gRPC `RESOURCE_EXHAUSTED` as the in-flight ceiling, with its own body
text and counter `ravel_ingest_pending_ack_shed_total`, and the gauge
`ravel_ingest_pending_strict_acks` shows the held count. The two refusals
differ in where they shed: the in-flight ceiling decides at the request
head and a shed request never buffers a body (narrowed by the head-refusal
drain amendment below: it never decodes one), while the pending-ack bound
is taken inside `submit`, after decode and before the byte charge, so a
refusal there has paid a full decode (see the head pending-ack amendment
below, which moves it to the request head) and holds no charge. It is a
bound on held contexts, not a replacement for the head shed. Buffered writes
return at enqueue and take none. The default is the in-flight ceiling
times eight: at the aged policy's 2.2 s that is about 3,700 strict requests
per second before refusal, at the arrival policy's 0.1 s about 80,000, and
a held context is a response future plus one oneshot receiver per shard,
tens of megabytes at the default, not gigabytes.

```mermaid
flowchart LR
    subgraph gateway
        A[admission middleware<br/>take in-flight permit] --> B[decode body<br/>charge decode bytes]
        B --> C[router.submit<br/>take pending-ack permit<br/>charge byte budget<br/>send to shards]
        C --> D[drop in-flight permit]
        D --> E[Submitted.acks<br/>await under deadline]
    end
    subgraph shard actor
        F[handle_write<br/>merge, charge, push waiter] --> G{size trigger?}
        G -- yes --> H[flush_tenant Size]
        G -- no --> I{strict-arrival?}
        I -- no --> J[wait for age tick]
        I -- yes --> K{tenant flush<br/>in flight here?}
        K -- no --> L[flush_tenant Arrival]
        K -- yes --> M[mark arrival_pending<br/>count coalesced]
        N[join_next reaps flush] --> O{pending with waiters?}
        O -- yes --> L
    end
    C -. ShardMsg::Write .-> F
    L --> P[(data PUT, commit PUT)]
    P --> E
```

## Rejected alternatives

- **Lower `--max-flush-delay` for everyone.** It is the only existing lever
  that shortens a strict ack, and it is process-wide: every tenant's PUT
  rate rises linearly with it. ADR-0076 calls it the lever of last resort.
  The arrival policy moves only the writes that ask for it.
- **The E1 shape: a zero age threshold for a buffer with a strict waiter.**
  Ten lines, measured, and still tick-bound: the ack floor is one
  `flush_tick` plus two PUTs, 200 ms plus 60 ms, against 60-70 ms for the
  in-handler flush the size trigger already performs. The tick adds nothing
  once the in-flight count bounds the rate; the handler path is the one
  the loader has run in production.
- **Make arrival the default.** At ten strict requests per second per shard
  it costs up to 20x the PUTs of the aged policy, and 32x at saturation.
  ADR-0076's whole point is the request bill, and its decision 4 stands as
  the default.
- **A per-tenant policy field in this ADR.** `TenantConfig` is a versioned
  protobuf; adding a field is a format change with a reader-first rollout
  (sys.proto version 3 took one). The two callers this ADR serves are
  covered by the header and the process default. Deferred, not rejected.
- **A per-tenant flush semaphore.** ADR-1642 rejected it and ADR-2708
  decision 3 keeps one FIFO semaphore per shard with a per-tenant share.
  The bound here is a read of decision 3's in-flight count before opening
  a flush, not a permit a task waits on; it does not change who holds the
  shard permit or in what order.
- **A second in-flight count owned by this ADR.** Two counts of the same
  thing in one actor drift. Decision 3 lands first and this ADR reads it.
- **Flush on arrival through `FlushNow`.** The router's manual trigger
  flushes every tenant on the shard and takes no bound. The size path is
  per tenant, in the handler, and already proven.
- **Raise `--max-inflight-ingest-requests` instead of releasing at
  enqueue.** The flag's memory term is count times the largest decoded
  body (1,024 times 16 MiB today); raising it to lift the ack-bound ceiling
  raises the decode memory bound in lockstep. Decoupling the two lifetimes
  lifts the ceiling at no memory cost.
- **Release at enqueue with no pending-ack bound.** ADR-0076 named held
  request contexts as a cost "ADR-0069's global ingest budget... does not
  cover". The in-flight permit covered it only by holding across the ack.
  A bound that disappears silently is a regression of a stated property.

## Consequences

- A strict-arrival write acks at the shard permit wait plus two PUT round
  trips. Pre-registered on the epic: p50 60-120 ms on real S3 for a serial
  one-record client, exactly two PUTs per write, at most one flush per
  shard per in-flight window under eight concurrent clients. The aged
  policy's figures are unchanged, and a test pins that a default-policy
  write does not flush before its age threshold.
- The durability and visibility contracts in `docs/consistency-model.md`
  are unchanged. The strict ack still means data object plus commit record
  durable. `docs/consistency-model.md:165`'s visibility formula gains the
  arrival case: flush delay is zero under this policy.
- `flush_trigger_age_bound_ns` and the deferral cap are unchanged: an
  arrival flush opens no later than an aged one would.
- `docs/ingest.md` changes: the Modes section gains the third value and
  the process default; the shard actor section gains the arrival branch
  and the in-flight count; the pipelined-flushes section states that the
  arrival flush takes the same permit and queue cap; the sizing table and
  the metrics section gain the new flag, counters and gauge; the admission
  text at lines 56-61 says the in-flight permit covers decode through
  enqueue and names the pending-ack bound. Four in-tree sentences become
  false the moment the permit is released at enqueue and change in the
  same commit as that behavior: the middleware comment at
  `services/ravel-server/src/ingest_admission.rs:235-241`, which loses
  "and the durable write" from what the permit covers; the marker comment
  at `ingest_admission.rs:89-95`, whose "the permit instead lives in the
  layer's own future, whose lifetime is exactly the request's" becomes
  "the layer holds a clone of the release handle"; the gRPC layer comment
  at `ingest_admission.rs:450-452`, "held for the whole inner call: the
  body read, tonic's decode, and the handler's durable write", which the
  early release contradicts; and the `--max-inflight-ingest-requests`
  help at `services/ravel-server/src/config.rs:1550-1565`, which keeps
  its head-shed sentence (changed later by the head-refusal drain
  amendment below) and gains one saying the permit is released at
  enqueue, so the count bounds decodes in flight and no longer covers the
  ack wait. `docs/reference/ravel-server-flags.md`,
  `docs/reference/http-api.md`, `docs/guides/ingest.md` and `docs/concepts.md`
  gain the header value and flags. The flag doc test
  `ingest_flag_docs_name_the_inflate_term` keeps pinning the in-flight
  flag's terms.
- ADR-0076 decision 4 carries an inline pointer to this ADR's amendment
  section. Decision 4 itself stands.
- `ravel-cli load` is untouched: it already flushes on arrival through the
  size trigger and keeps `WriteMode::Strict`.
- The split router API is additive. `write` keeps its signature; every
  existing caller compiles unchanged.
- The ack latency under the arrival policy is bounded below by the shard
  permit wait. With the single permit in the tree today a co-resident
  tenant's slow flush delays an arrival ack by its own duration; ADR-2708
  decision 3's four permits with a per-tenant share leave one permit free
  of any single hung tenant. The shard-actor task of this epic depends on
  #1921 landing, and its acceptance test runs against that tree.

## Amendment (2026-10-14): a head refusal drains the request body before it answers (issue #2926)

<!-- amendment-applies: sections="Decision" pointer="head-refusal drain amendment" -->
<!-- amendment-supersedes: phrase="a shed request never buffers a body" pointer="head-refusal drain amendment" -->

Decision 4 says a request shed at the head never buffers a body. Taken
literally, that loses the response it was meant to make cheap. The
admission middleware answers the in-flight ceiling's 429 before the body
extractor runs. hyper's HTTP/1 connection then polls the unread body once
and closes the read side. A socket closed with unread bytes in its receive
queue is reset by the kernel, and the reset can discard the 429 before the
client reads it. The client sees a broken pipe or a connection reset, and
gets no `Retry-After`.

Measured on a pooled HTTP/1.1 client with 8 requests in flight, sending
Remote Write over loopback:

| Body | Head refusals | Arrived as a transport error | Source |
|---|---|---|---|
| 130 B, 14,962 B | 2,000 x 3 runs each | 0 | issue #2926 body |
| 64,351 B | 2,000 x 3 runs | 262, 254, 260 (about 13%) | issue #2926 body |
| 6,000 series, x86_64 | 2,000 | 169 | comment 6156727395 |

**Decision.** When the in-flight ceiling sheds an HTTP/1 ingest request at
its head and the request's tenant credential resolves, the admission layer
reads and discards the request body before it sends the 429. The pending-ack
429 joins this once issue #2805 moves it to the head. Three bounds apply,
each a constant in `ingest_admission.rs`, not a flag:

- `HEAD_REFUSAL_DRAIN_MAX_BYTES`: 1 MiB of body as sent, before any
  inflate.
- `HEAD_REFUSAL_DRAIN_TIMEOUT`: 1 s.
- `HEAD_REFUSAL_DRAIN_MAX_CONCURRENT`: 64 drains at once, process-wide.

Changing any of the three takes a later amendment with its own evidence.

A drained body is never inflated, decoded or charged to any budget. The
code that calls the drain holds no admission permit while it runs: a shed
request was never given one, and a later head refusal that holds the
in-flight permit releases it first. The concurrency bound is taken with
`try_acquire` and never awaited. It is what limits drains under the
overload that produces them, since no admission permit does.

**Only an authenticated caller is drained.** The admission layer checks the
credential right after the permit, and that ordering exists so that an
unauthenticated caller cannot make this process buffer a body (ADR-0051).
Draining for an unauthenticated caller would undo that guarantee. It would
also let such a caller hold all 64 drain slots, which pushes every
authenticated refusal onto the fallback below. So:

- A shed request whose credential does not resolve gets the 429 with no
  drain. The check reads only the request head, and on this path it has no
  side effect: it does not call the surface's unauthorized hook or move its
  rejected-request counter, since the answer is the 429, not a 401.
- The credential check's own 401 is not drained either.

The cost is diagnosability. A client sending a large body with a bad
credential can see a connection reset where it would have seen a 401.
That client fails either way, and neither response tells it to retry.

**The fallback.** On HTTP/1, a shed refusal that is not drained is answered
as it is today: the same status, `Retry-After` and body text, with
`Connection: close` added. That happens when:

- the credential does not resolve;
- the body is past the byte cap (a `Content-Length` above it is refused
  without reading a byte);
- all 64 drain slots are taken;
- the body is not complete within the deadline.

The conditions are checked in that order, and the first that applies
names the refusal: an unauthenticated shed declaring a body
over the cap is counted as `unauthenticated`. A `Content-Length` over the
cap is caught before a drain slot is taken, and a chunked body that passes
the cap during the drain is counted as `over_cap`, not `timeout`.

`Connection: close` tells a client that does read the response not to reuse
the connection. It does not prevent the reset, so an undrained refusal can
still reach the client as a transport error. A deployment sees how often
this happens through `ravel_ingest_head_refusal_undrained_total`, with a
`reason` label of `over_cap`, `timeout`, `busy` or `unauthenticated`. It
counts shed refusals only. A 401 at a ceiling with room is not a shed: it
is not counted and gets no `Connection: close`, so it is answered exactly
as today.

**The bounds.**
- The byte cap is 1 MiB, not the routes' 16 MiB body limit. The reset
  starts at about one socket buffer, near 64 KB on loopback, and 1 MiB
  covers bodies 16 times that.
- The time bound is what limits a slow sender.
- 64 concurrent drains is chosen, not measured. It caps the body bytes
  being read for refusals at 64 MiB at any instant. At the default
  in-flight ceiling of 1,024 it is one sixteenth of the admitted requests,
  so drains do not rival the admitted work they protect. A deployment
  that sets a much lower ceiling gets a drain pool close to its size.
- Draining 16 MiB per refusal would hold a task for the transfer time of
  the largest legal body.

**Excluded.**

- The body-read timeout's 503. That sender is already past a 30 s
  deadline, and 503 is a status both exporters retry. A sender still
  trickling bytes can be reset there, as before.
- The route body limit's 413. It is layered outside the admission layer,
  and a body over 16 MiB is past the drain cap by construction.
- The gRPC surfaces. HTTP/2 refuses a stream, not the connection, and
  still delivers the response headers. That is inferred from the protocol,
  not measured, and the acceptance tests pin it.
- OTAP, whose refusals answer inside an open stream.
- An HTTP/2 request on the same routes. The listeners serve HTTP/2 when
  the build enables it, and there a refusal ends a stream, not the
  connection. It is answered without a drain, is not counted, and carries
  no `Connection` header, which HTTP/2 forbids. So it cannot take the
  drain slots the HTTP/1 path depends on.

**Acceptance.**
- A pooled-client test, run while the ceiling is held, sends to each of
  the three OTLP HTTP routes and to Remote Write. Its bodies are larger
  than the loopback socket buffer and below the cap. Every refusal must
  arrive as a readable 429 with `Retry-After`, with zero client transport
  errors. The test must rule out three wrong implementations:
  - `Connection: close` alone: transport errors stay.
  - A drain that decodes or charges: across the drained refusals the
    counters that move only on a decode or a charge stay at zero:
    `ravel_ingest_buffered_items_total` and the ingest byte-budget
    charge. The existing over-ceiling test uses this same form.
  - A drain with no cap: a request whose `Content-Length` is above the cap
    must get `Connection: close` and move the `over_cap` series by exactly
    one.
- A unit test fills the drain slots and asserts that the next refusal
  takes the `busy` path.
- A shed request whose chunked body stalls below the cap gets a response
  with `Connection: close` within about the deadline, and moves the
  `timeout` series by exactly one. This rules out a deadline that is never
  armed, which would let a stalled sender hold a drain slot.
- A refusal without a resolvable credential must reach its response
  without reading the body:
  - a shed request takes the `unauthenticated` path;
  - a 401 is sent while the client has sent only the request head.
- A gRPC case with a large message asserts `RESOURCE_EXHAUSTED`.

**Follow-through in the code change.** Some text in the tree states the old
property and must change in the same commit as the code:

- the `--max-inflight-ingest-requests` help in `config.rs`, and the
  generated flag reference rebuilt from it;
- the admission section of `docs/ingest.md`, whose sentence "a shed request
  costs this process the bytes of one request head" holds afterwards only
  for an unauthenticated caller;
- the doc comment on `try_admit` in
  `services/ravel-server/src/ingest_concurrency.rs`. It says a shed request
  does none of the body read or the tenant resolution, and for an
  authenticated shed both now happen;
- any test on the HTTP routes that pins that a shed body is never read,
  or that a shed reaches no credential check. That includes the doc of the
  over-ceiling case in
  `services/ravel-server/tests/ingest_admission_before_body.rs`, whose
  request is unauthenticated and so keeps passing. The test is changed to
  pin never decoded and never charged, and the credential check is
  described as side-effect free.

The gRPC `PoisonBody` tests stay as they are.

## Amendment (2026-10-14): the pending strict-ack permit is taken at the request head (issue #2805)

<!-- amendment-applies: sections="Decision" pointer="head pending-ack amendment" -->
<!-- amendment-supersedes: phrase="is taken inside `submit`, after decode and before the byte charge" pointer="head pending-ack amendment" -->

Decision 4 places the pending-ack bound inside the router's `submit`. A
strict request refused there has therefore already been read, inflated,
decoded, normalized and admitted. Issue #2805 proposed moving the refusal
to the request head, gated on a measurement of what the late refusal
costs. That measurement is #2902. It ran on an 8 vCPU x86_64 executor,
with a fresh connection per request in both arms and every count band
exact:

| Body (series x samples) | Extra CPU per post-decode refusal over a head refusal | Source |
|---|---|---|
| 1 x 1 | 30 us (noise-limited) | comment 6156727395 |
| 1,000 x 1 | 1.25 ms | comment 6156727395 |
| 1,000 x 10 | 2.84 ms | comment 6156727395 |
| 10,000 x 1 | 12.97 ms | comment 6156727395 |

At 10,000 x 1 the server refused 466 requests a second after decode on
about 6.2 busy cores, against 12,859 a second at the head on about 3.8. The
rule pre-registered on #2805 was to implement at 1 ms or more on the large
body; the outcome is recorded in comment 6119242471.

**Decision.** On the OTLP HTTP routes, on Remote Write, and on the OTLP gRPC
services (not OTAP; see below), the admission layer takes the pending-ack
permit at the request head. It does so right after the in-flight permit and
the credential check, and only for a strict write.

Whether a write is strict is a property of the surface, not of the header.
Each surface's admission layer is given what it needs to decide it: the
HTTP middleware through the admission state each surface instantiates it
with, and the gRPC admission layer through the strict flush policy, which
it does not carry today and gains in this change:

- On OTLP HTTP and gRPC the state answers with `resolve_write_mode` over
  the header or the gRPC metadata, the same function the handler uses, so
  the head and the handler cannot disagree.
- On Remote Write the state answers strict unconditionally, whatever the
  request carries, because the handler writes every Remote Write request
  strictly.
- A buffered request takes no permit.

The gRPC rule covers the layer and also the direct-call path, where
`admit_grpc_request` takes the in-flight permit itself.

A refusal answers like the refusal it replaces:

- the same 429 with `Retry-After`, or gRPC `RESOURCE_EXHAUSTED`;
- the same body text;
- the same counter, `ravel_ingest_pending_ack_shed_total`.

The in-flight permit is released before anything else happens. On HTTP/1
the body is then drained under the head-refusal drain amendment above. The
caller is authenticated by then, so the drain applies. That is why the drain
lands first.

**One budget, one holder.**
- The layer takes the permit from the same `Arc<PendingAckBudget>` the
  routers hold, so the bound, the gauge and the shed counter keep one
  source.
- The permit travels to the handler the way the in-flight release handle
  does. The handler passes it to the router through a submit entry point
  that accepts an already-held permit.
- `Submitted` holds it through `acks()` and the deadline, as today.
- A pre-admitted write never takes a second permit in the router.
- Callers that do not pass through the admission layer (`ravel-cli load`,
  OTAP, tests) keep acquiring inside `submit`.

**OTAP keeps the post-decode refusal, as a trade-off.** OTAP already refuses
some batches before decode: on the in-flight ceiling and on the byte rate.
Each such refusal ends the stream, because a batch the decoder never sees
would desynchronise its schema and dictionary state for every later batch.
Taking the pending-ack permit there would turn today's per-batch pending-ack
refusal, which keeps the stream open, into a stream teardown and a fresh
decoder state for every refused batch. A stream's batches share one
connection and one decoder, so wasting the decode is the cheaper of the two
costs. The OTAP stream head takes no pending-ack permit either. Its mode
does cover every batch on the stream, but one stream serves an unbounded
number of writes, so a single permit taken at the head would be held for
the stream's whole lifetime while bounding none of them.

**What the bound now counts.** A pre-admitted permit is held from the head,
through the body read (up to the 30 s body-read timeout) and the decode, and
then through the ack wait. While the in-flight ceiling is set, it bounds how
many requests hold a pending-ack permit before `submit`. The gauge
`ravel_ingest_pending_strict_acks` and its HELP text change to count strict
requests from admission to ack.

A pending-ack bound set below the in-flight ceiling, or a finite one while
the ceiling is 0 (unlimited), now also bounds strict requests that are still
being read or decoded. Strict writes can then be shed while fewer acks than
the bound are pending. The server accepts this configuration, and the
flag's help states the interaction. The defaults are 8,192 and 1,024.

**Acceptance.** Each test below rules out the wrong implementations named
beside it.

| Setup | Expected | Rules out |
|---|---|---|
| Budget 1, no ack pending. One strict request is parked between the head and `submit`: its client has sent the head and holds the body. A second strict request follows. | The second is refused at the head with the pending-ack 429. | A head that only checks and acquires in `submit`: the second request is admitted and decoded. An implementation that acquires only in `submit`. |
| Budget 1, one strict ack held by a gated data PUT. A second strict request with an undecodable body. | The pending-ack 429 text, not a 400. | A refusal still taken in `submit`: the 400 comes first. |
| Budget 2, PUT held. Two concurrent strict writes. | Both admitted, gauge 2, and a third refused. | Acquiring at the head and again in `submit`: the second is refused. Releasing at `submit`: the third is admitted. |
| Budget 1, one ack held. A `buffered` request on HTTP and on gRPC. | 200 and OK. | Taking a permit for every mode. Reading the mode from the wrong source on one transport. |
| Budget 1, one ack held. A Remote Write request carrying `x-ravel-ingest-mode: buffered`. | The pending-ack 429. | Reading the header on Remote Write, which would let any client opt out of the bound. |
| Budget 1, PUT held. A direct-call gRPC request, with no layer, invoking the service method in-process. | `RESOURCE_EXHAUSTED`, with normalization and `submit` never reached: the router and ingest counters do not move. | A no-layer path that takes no permit at the head: it refuses only after decode. A bare permit dropped with tonic's extensions: the second request is admitted. |
| In-flight ceiling 1, budget 1, one ack held. A strict request is refused by the pending-ack bound, and its body below the drain cap is still draining. A second request, buffered, arrives during that drain. | The second is admitted: the refused request's in-flight slot was returned before its drain. | A drain that runs while the in-flight permit is still held: the second is shed. |

**Follow-through in the code change.** Some text in the tree stops being true
for admission-layer writes and must change in the same commit as the code:

- the `docs/ingest.md` sentence saying that a strict `submit` takes a
  pending-ack permit before it charges the byte budget;
- the `docs/ingest.md` metric descriptions of the pending-ack gauge and
  shed counter, which describe `submit`;
- the gauge's HELP text;
- the `--max-pending-strict-acks` help, and the flag reference generated
  from it;
- the module doc of `crates/ravel-ingest/src/pending_ack.rs` and the doc
  comment on the strict submit in `log_router.rs` (and its metrics and span
  equivalents), which say a strict `submit` takes the permit. They stay
  true for callers outside the admission layer and gain that a
  pre-admitted write carries one in.

```mermaid
flowchart LR
    A[admission layer<br/>in-flight permit, credential] --> M{strict for<br/>this surface?}
    M -- yes --> P[take pending-ack permit<br/>or release in-flight, drain, 429]
    M -- no --> B
    P --> B[decode body]
    B --> C[router submit, with the held permit if strict<br/>charge byte budget, send to shards]
    C --> D[drop in-flight permit]
    D --> E[Submitted.acks holds the<br/>pending-ack permit to the ack]
```
