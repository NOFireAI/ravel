# ADR-3718: Refuse ingest before the node's cores saturate

- Status: Accepted (2026-11-03)
- Date: 2026-11-03
- Refs: #3718, #3645, #3492, ADR-0051, ADR-0069, ADR-1702, ADR-2708

## Context

Logs ingest throughput on one `ravel-server` follows its cores: about 18k
OTLP log lines/s per vCPU, about 46 to 47 us of CPU per stored line, on 8 and
on 16 vCPU alike (#3492 comments 6579509683 and 6589850073). Below that
ceiling every offered line is stored. Above it the server does not hold the
ceiling. It falls into an overloaded state and stores less than its peak, and
it stays there for as long as the excess load keeps arriving.

Peak stored is the highest stored rate the topology reached at any step.
Stored above the ceiling is the range over the overloaded steps. The loss is
peak minus stored, over peak.

| Node | Peak stored | Offered above the ceiling | Stored there | Loss | Source |
|---|---|---|---|---|---|
| 8 vCPU | 152k lines/s (fixed 150k) | 160k (fixed) | 115k to 123k | 19% to 24% | #3645 comments 6574471828 and 6584038929 |
| 16 vCPU | 283k lines/s (280k step) | 300k to 340k | 248k to 260k | 8% to 12% | #3492 comment 6589850073 |
| 2 x 8 vCPU, one Service | 261k lines/s (280k step, 9% dropped) | 300k to 340k | 227k to 248k | 5% to 13% | #3492 comment 6589850073 |

The two-replica row is on a different basis from the other two.
- **Clean step:** it held 243k with no drops at the 240k step. Measured from
  there, its 300k to 340k steps stored between 7% less and 2% more, so at the
  topology level there is no clear collapse.
- **Peak:** it peaked at 261k at the 280k step, with drops, because one
  replica saturated at 220k and the other never did.

The collapse is per replica: the busier one was in the overloaded signature
from 220k on (#3492 comment 6589850073). The topology-level figure
understates it, because the idle replica's spare cores absorb part of the
loss.

The overloaded state has one signature on every topology:
- every core is busy;
- flushes are size-triggered at the 32 MiB object backstop instead of age-triggered at about 9k lines;
- flushes wait seconds for a flush permit;
- ordinary flush triggers are deferred at about 90 per second (zero on every clean step);
- CPU per stored line is 59 to 76 us instead of 47 us, uniformly across decode, normalize and encode, with no lock or spin hotspot (#3645 comment 6584038929).

The measurements on #3645 changed one variable at a time against that state:

| Variable | Result | Source |
|---|---|---|
| Write-gate permits 4, 6, 8 | no change | #3645 comment 6573395997 |
| Flush slots 4 or 8 per shard | 8 delays the tip; it still happens | #3645 comment 6575073635 |
| Object backstop multiple 1 or 4 | 1 is 4% to 15% worse | #3645 comment 6584038929 |
| Ingest byte budget 1 GiB or 4 GiB | no change | #3645 comment 6584038929 |
| At most 8 requests decoding at once, against 1024 | 6% to 13% better, still overloaded; almost no 429s | #3645 comment 6585932504 |

None of these is a CPU limit. `--max-inflight-ingest-requests` bounds decode
memory and releases its permit at enqueue. The ADR-0069 byte budget bounds
buffered bytes. Rate admission (ADR-0051, ADR-0084) bounds a tenant's
configured rate. Flush permits (ADR-2708 D3) bound concurrent PUTs. A server
accepts any load these bounds allow, whether or not its cores can encode it,
and the backlog that builds then keeps it overloaded. ADR-1702 already
decided where the answer belongs: a busy node stays in its Service, and
"overload is handled by admission control, not by the probe" (ADR-1702,
lines 225 to 226). No admission control looks at CPU today.

## Decision

### D1. A saturation detector reads two signals the process already has

A background task samples every 100 ms:

- **CPU use:** the process's own CPU time (user plus system, fields 14 and
  15 of `/proc/self/stat` at Linux's fixed 100 ticks per second, a plain
  file read, as `crates/ravel-bench/src/columnar_load.rs` already does) over
  wall time, divided by the core count `HostProfile` already resolves
  through `available_parallelism`. That count follows a cgroup CPU quota.
  Off Linux there is no `/proc/self/stat`, so the detector never reports
  saturation and logs once that shedding is off.
- **Flush backlog:** the sum over shards of `flush_trigger_deferred` (an
  existing counter, `crates/ravel-ingest/src/metrics.rs`), read as a rate
  over the last second through a new cheap accessor on the ingest routers:
  one atomic load per shard, not the full metrics snapshot.

The node is **saturated** while CPU use is at or above
`--ingest-cpu-shed-threshold` (default 0.95) and deferred triggers are above
zero, both averaged over 1 s. CPU use alone does not trip it: a clean step at
the ceiling runs at about 91% of the node (#3492 comment 6579509683), close
enough that CPU alone would shed good load. Deferred triggers alone do not
trip it either: a multi-tenant node can defer one stalled tenant's trigger
with cores to spare (ADR-2708 D3). The two together mark the state measured
above and no clean step. The CPU level at which `p` rises moved below the
threshold in the deadband integrator amendment below.

### D2. While saturated, refuse a growing fraction of new ingest requests at the head

OTAP batches draw at a second point, after their stream's tenant is
resolved; see the OTAP batch amendment below.

The detector keeps a shed fraction `p` between 0 and `--ingest-cpu-shed-max`
(default 0.9, 0.5 since the deadband integrator amendment). Each sample it moves `p` by one of three rules, with CPU use
and deferred triggers averaged over 1 s as in D1. This table and the three
paragraphs after it are replaced by the deadband integrator amendment below:

| CPU use | Deferred triggers | `p` |
|---|---|---|
| at or above the threshold | above zero (saturated) | rises by 0.05 |
| at or above the release level (threshold minus 0.05, so 0.90 by default) | any, while not saturated | holds |
| below the release level | any | falls by 10% of itself, and snaps to 0 below 0.01 |

The three rows partition every input: saturated, not saturated but near the
ceiling, and clear of it. The snap to 0 means a node that has left the ceiling
refuses nothing, rather than a vanishing fraction for ever.

The middle row is what lets `p` settle. Once shedding brings the admitted
load just under the ceiling, deferrals stop but CPU use stays near the
ceiling: a clean step at the ceiling runs at about 91% of the node (#3492
comment 6579509683). So `p` holds instead of collapsing back into the
overload.

The rule that lowers `p` needs CPU to fall clear of the ceiling, and it falls
slower than it rises: from 0.5 to under 0.05 takes about 2.2 s, against
1 s to climb from 0 to 0.5. A drop in offered load releases the shed within
seconds, and a server sitting at its ceiling does not cycle.

Each new ingest request draws once against `p`. Refused requests get 429
with `Retry-After: 1` on OTLP HTTP and Remote Write, and
`RESOURCE_EXHAUSTED` on gRPC.

The check runs at the request head, before the body is read or decoded, and
before the tenant is resolved: right after the in-flight permit, in the order
that permit already keeps (a shed is answered before a credential check, so
a saturated node does no tenant resolution for a request it will refuse):
- in `admit_ingest_request` (`services/ravel-server/src/ingest_admission.rs`),
  answered through `refuse_from_head`, which drains the body without decoding
  it. The in-flight permit is released before the refusal, as the
  pending-ack shed already does, so a drain does not hold an in-flight slot;
- in `GrpcIngestAdmissionLayer::call` at the same point for gRPC.

So an unauthenticated request that arrives while shedding gets 429, not 401,
as it already does from the in-flight permit.

A refusal costs the server a header parse and, when it can, a body drain. It
never costs a decode. The drain is bounded: at most 1 MiB, 1 s, and 64
concurrent drains per process. A refusal that finds no free drain slot, whose
body exceeds 1 MiB or whose drain times out is answered with
`Connection: close` and counted on `ravel_ingest_head_refusal_undrained_total`
(`services/ravel-server/src/ingest_admission.rs`, `refuse_from_head`). See
Consequences for what that costs at a high shed fraction.

Refusals count on a new `ravel_ingest_cpu_shed_total{mode}`, beside the
existing `ravel_ingest_concurrency_shed_total`, `ravel_ingest_pending_ack_shed_total`
and `ravel_ingest_buffer_shed_total`. The current `p` exports as the gauge
`ravel_ingest_cpu_shed_fraction`.

A probabilistic fraction, rather than refusing everything while saturated,
admits a steady share of the load. With the hold band above, that share
settles just under the ceiling instead of oscillating between full refusal
and overload.

Whether it settles is a measured question, not an assumption. D4 reports `p`
over time, and a cycle that drops back into overload fails D4's first bar.

### D3. Defaults scale with the node; one flag turns it off

The threshold is a fraction of the resolved core count, so the same default
holds on 8, 16 or more vCPU without tuning. `--ingest-cpu-shed-threshold 0`
disables shedding, which is today's behaviour. The resolved threshold and the
core count are logged at startup with the other derived defaults. The
detector box in the diagram below shows D2's original step, nonzero
thresholds must now be above 0.07, and `--ingest-cpu-shed-max` now defaults
to 0.5; see the deadband integrator amendment below.

It is on by default: a server that saturates loses 5% to 24% of its peak
stored rate (Context), which no operator wants. Shedding is process-wide and does not
pick tenants; refusals fall on tenants in proportion to their requests. Per
tenant fairness stays with rate admission (ADR-0051).

```mermaid
flowchart LR
  subgraph head["request head, before the body is read"]
    P["in-flight permit\n(--max-inflight-ingest-requests)"] --> C{"CPU shed draw\n(D2: refuse with probability p)"}
    C -- refused --> R["429 Retry-After: 1\nravel_ingest_cpu_shed_total"]
    C -- admitted --> T[tenant]
    T --> A["pending strict ack\n(strict writes)"]
  end
  A --> D["read, rate check, inflate,\nbyte budget, decode"]
  D --> B["shard buffers"] --> F["flush: write gate encode, PUT"]
  subgraph det["saturation detector, every 100 ms (D1)"]
    U["process CPU / cores"] --> S{"saturated?\nCPU at least 0.95\nand deferred triggers above 0"}
    Q["deferred flush triggers\n(per-shard counters)"] --> S
    S --> PF["p: +0.05 while saturated,\nelse held while CPU at least 0.90,\nelse -10% of p, 0 below 0.01"]
  end
  F -. defers triggers .-> Q
  PF -. sets p .-> C
```

### D4. Acceptance on the benchmark

Each figure below is pre-registered as predicted. The epic exits when all
three hold on the logs benchmark (GKE, GCS store, buffered acks), with the
detector on its defaults:

| Figure | Bar | Basis |
|---|---|---|
| 8 vCPU, fixed 160k offered: stored | at least 145k, with refusals in place of the collapse | predicted |
| 16 vCPU, fixed 320k offered: stored | at least 270k | predicted |
| 8 vCPU, exit ramp to 150k: best 30-min and the 150k step | within 1% of the same build with `--ingest-cpu-shed-threshold 0`, and no refusals below the 150k step | predicted |

Each run also reports, without a bar:
- `ravel_ingest_head_refusal_undrained_total` by reason, against the CPU
  sheds;
- the client's connection count;
- `ravel_ingest_cpu_shed_fraction` over time, with the share of each window
  spent in the overloaded signature (size-triggered flushes at the backstop,
  deferred triggers above zero).

The first two show how many refusals forced a reconnect (Consequences). The
third shows whether `p` settled or cycled (D2).

## Rejected alternatives

- **A lower fixed `--max-inflight-ingest-requests`.** It bounds a count, not
  CPU, so no single default fits 8 and 16 vCPU. Its permit is released at
  enqueue, before the encode that saturates the node. Measured at 8 against
  1024: 6% to 13% better but still overloaded, with almost no 429s (#3645
  comment 6585932504).
- **Tuning flush slots, the backstop multiple or the byte budget.** Each was
  measured against the overloaded state and none prevented it (#3645
  comments 6575073635 and 6584038929). The backstop multiple at 1 made it
  worse.
- **Queue requests at the head instead of refusing them.** A queue in front
  of a saturated node grows without bound under sustained excess load. It
  holds client connections and memory, and pushes the overload into the
  client's timeouts. A 429 with `Retry-After` lets an OTLP exporter back off
  and retry, which it already does.
- **Shed on CPU use alone.** The clean ceiling runs at about 91% of the node,
  so a CPU-only threshold either sheds good load or fires too late.
- **Leave it to horizontal scaling.** More replicas raise the ceiling, but each
  replica still collapses when its own share exceeds its cores. Connection
  balancing made one of two replicas saturate first (#3492 comment
  6589850073).
- **Shrink CPU per line further.** Two cuts aimed at the remaining hot spots
  measured no cheaper (#3654, #3655). A cheaper line moves the ceiling up, but
  the collapse above it remains.

## Consequences

- Above its ceiling a server refuses the excess instead of losing 5% to 24%
  of its peak stored rate. Clients see 429s where they saw latency and
  dropped lines.
- At a high shed fraction most refusals go through `refuse_from_head`.
  - **Undrained refusals reconnect.** Its 64 drain slots, 1 MiB cap and 1 s
    timeout were sized for the rare in-flight and pending-ack sheds. A
    refusal that misses a slot, or carries a body over 1 MiB, is answered
    with `Connection: close`, so an HTTP/1 exporter reconnects, and with TLS
    re-handshakes, for each one. That spends CPU on connection setup while
    the node is shedding for CPU.
  - **`Connection: close` stays mandatory.** Every refusal that leaves the
    body unread keeps the header. A reused HTTP/1.1 connection would
    otherwise parse the unread body bytes as the next request.
  - **The caps are the lever.** If D4's undrained count shows reconnects
    dominate, the change is to raise the drain caps
    (`HEAD_REFUSAL_DRAIN_MAX_CONCURRENT` and `HEAD_REFUSAL_DRAIN_MAX_BYTES`
    in `services/ravel-server/src/ingest_admission.rs`), measured, not to
    drop the header.
  - **gRPC is cheaper.** gRPC is refused per stream on a kept connection, so
    it pays no reconnect.
- On a node shared with other CPU-heavy work, the process's own CPU use
  under-states the node's, so shedding starts later. The threshold flag is the
  lever there.
- Strict acks are refused at the same head, so a strict client sees a 429
  instead of an ack that arrives late. The pending-ack budget is unchanged.
- Under heavy deferral with spare cores (one stalled tenant on a busy
  multi-tenant node), nothing is shed, because D1 needs both signals. That
  case stays with ADR-2708 D3's share.
- The detector adds one background task and two atomic reads per sample. A
  shed draw is one atomic load and one random number per request.
- `docs/ingest.md` and `docs/guides/ingest-affinity.md` ("Admission
  rejections") gain the new refusal, and `docs/reference/ravel-server-flags.md`
  the two flags. The guide pointer is corrected by the OTAP batch amendment
  below.

## Amendment (2026-11-03): OTAP batches, a full window, and the guide pointer (#3720)

<!-- amendment-applies: sections="D2. While saturated, refuse a growing fraction of new ingest requests at the head|Consequences" pointer="OTAP batch amendment" -->

Three changes from implementing D1 and D2 (#3720).

- **OTAP batches draw per batch.** An OTAP Arrow stream takes no in-flight
  permit at the gRPC layer, so the draw D2 places there never runs for it.
  The implementation draws once per `BatchArrowRecords` batch in
  `process_batch` (`services/ravel-server/src/otap_grpc.rs`), right after
  that batch's own in-flight permit and before its byte-rate check and
  decode.
  - The stream's tenant was resolved when it opened, so this draw runs after
    the tenant, unlike D2's head refusal.
  - A refused batch ends the stream with `RESOURCE_EXHAUSTED`, as the
    in-flight ceiling already does: skipping one batch would leave the
    stream's decoder state out of step with the sender.
- **Saturation needs a full window.** Until the detector holds 1 s of
  samples, the deferred average it feeds to D2's rules reads 0. A single
  100 ms sample at startup therefore cannot raise `p`, while the hold and
  decay rows apply from the first sample.
- **The refusal is documented in `docs/ingest.md`.** It is not in the
  "Admission rejections" section of `docs/guides/ingest-affinity.md`, which
  is about Kubernetes CRD validation, not runtime ingest admission.

## Amendment (2026-11-03): the shed step settles on a deadband integrator (#3741)

<!-- amendment-applies: sections="D1. A saturation detector reads two signals the process already has|D2. While saturated, refuse a growing fraction of new ingest requests at the head|D3. Defaults scale with the node; one flag turns it off" pointer="deadband integrator amendment" -->

D2's three-row step cycled instead of settling. At a fixed 160k lines/s on
8 vCPU, `p` swung from 0.10 to 0.90 about every 15 s, and acceptance part 1
stored 133.2k and 133.4k lines/s against D4's 145k bar (comment 6596936397).
Two things combined:

- **The release level sat above the bar's operating point.** 145k lines/s
  is about 0.89 of 8 cores at the measured 49 us per line, below D2's 0.90
  release level, so at every rate the bar accepts the old rule was in its
  decay row and released the shed.
- **Clients return together.** The benchmark's 32 writers back off on a 429
  (retry after 100 ms, doubling to 5 s). Once `p` fell, they all came back
  at once, deferrals fired, and `p` climbed back to the cap.

A first version of this amendment let `p` rise only with CPU more than 0.02
above the target and fall by up to 0.027 per sample, symmetric about the
band, and checked it on a model that counted deferrals whenever the flush
backlog was above zero. In that model deferrals showed on 100% of samples at
160k offered: the band spans 143.7k to 150.2k admitted lines/s, across the
149k flush capacity, and that law held `p` anywhere inside it, so the
backlog never cleared for a full second. #3741's review replaced both
the law and the model with the ones below.

D2's step table is replaced by a deadband integrator. Each 100 ms sample,
with `target = threshold - TARGET_OFFSET` (0.90 at the default threshold),
`e = cpu_avg - target` and `d` the 1 s deferred-trigger average of D1:

| `d` | `e` | `p` |
|---|---|---|
| above 0 | at least `-DEADBAND` | rises by `GAIN * max(e, DEADBAND)`, capped at `--ingest-cpu-shed-max` |
| above 0 | below `-DEADBAND` | holds |
| 0 | within `DEADBAND` of 0, either side | holds |
| 0 | below `-DEADBAND` | falls by `min(GAIN * -e, MAX_FALL_PER_TICK)`, and snaps to 0 below 0.01 |
| 0 | above `DEADBAND` | holds |

`TARGET_OFFSET` is 0.05, `DEADBAND` is 0.02, `GAIN` is 0.03 and
`MAX_FALL_PER_TICK` is 0.005 (constants in
`services/ravel-server/src/cpu_shed.rs`, `step`; its unit tests pin every
row and each row's edges). While triggers are deferred `p` rises by at least
0.0006 per sample even with CPU inside the band, since a backlog is the
overload D1 measures whatever CPU reads, and it stops rising only once CPU
is more than 0.02 below the target. Without deferrals it falls only from
below the band, by at most 0.005 per sample, so a release from 0.5 to 0
takes at least 98 samples (9.8 s). D1's two-signal rule still governs the
rise: CPU alone never raises `p`. At the default threshold CPU cannot exceed
1.0, so `p` gains at most 0.003 per sample, and 0.5 takes about 17 s from 0.

With this law the fall row is reachable only while `target - DEADBAND` is
above 0, so `--ingest-cpu-shed-threshold` now refuses any nonzero value at
or below 0.07 (it refused at or below 0.05 before). 0 still disables
shedding.

**The cap default moves from 0.9 to 0.5** (D3). A batch refused with
probability `p` and retried after a doubling backoff waits, on average, the
sum over `k` of `p^(k+1) * min(100 ms * 2^k, 5 s)`, whose terms grow by `2p`
each step until the 5 s ceiling. At `p` 0.5 a writer of 2,000-line batches
with 50 ms sends spends 0.79 of its time backed off and the benchmark's 32
writers deliver about 134k lines/s; at 0.6 that is 0.88 and 60k lines/s,
and at 0.9 it is 0.98 and 2.3k lines/s. Above 0.5 such a client parks every
worker, so a higher cap only stalls it.

The law is checked against a deterministic closed-loop model of the 8 vCPU
run (`cpu_shed.rs`, module `tests::plant`, seed `0x3741`, 1 ms steps):

- **Generator.** One shared queue, filled at the offered rate, 400,000 lines
  deep; lines offered to a full queue are dropped and counted as writer
  drops.
- **Writers.** 32 workers each take 2,000 lines from the queue when free. A
  send takes 50 ms and draws against the `p` the previous sample published.
  A refused send backs off `min(100 ms * 2^k, 5 s)`, `k` the batch's earlier
  refusals, then retries the same batch.
- **CPU.** 49 us per admitted line while the flush backlog is under 75,000
  lines and 65 us above it (the Context's 59 to 76 us), on 8 cores. Work
  above the cores in one sample carries into the next, so a sample reads at
  most 1.0.
- **Flush.** 149k lines/s of capacity. Deferral is a hysteresis: on once the
  backlog passes 75,000 lines, off once it returns to 0, and the per-sample
  deferral reading is 1 while on, else 0, averaged over D1's 10 samples.

Over the 60 s after a 20 s warm-up the model measured (2026-11-03, the
`plant` module's output):

| Law | Offered | Mean `p` | `p` range | Largest distance from mean | Admitted / offered | Admitted / 149k | Samples deferring | Writer drops |
|---|---|---|---|---|---|---|---|---|
| deadband integrator | 160k | 0.4969 | 0.3957 to 0.5 | 0.1012 | 0.8713 | 0.9356 | 43.8% | 800,080 |
| deadband integrator | 320k | 0.5 | 0.5 throughout | 0 | 0.4373 | 0.9391 | 100% | 10,798,000 |
| D2's three-row step | 160k | 0.4945 | 0.1743 to 0.5 | 0.3202 | 0.9060 | 0.9729 | 71.3% | 608,080 |
| D2's three-row step | 320k | 0.5 | 0.5 throughout | 0 | 0.4729 | 1.0157 | 100% | 10,128,000 |
| same target, no deadband, gain 0.3 | 160k | 0.5 | 0.5 throughout | 0 | 0.9629 | 1.0340 | 100% | 80,080 |
| same target, no deadband, gain 0.3 | 320k | 0.5 | 0.5 throughout | 0 | 0.4367 | 0.9378 | 56.2% | 10,822,000 |

These are model outputs, not benchmark measurements, and all three laws run
at the 0.5 cap. #3741 set seven bars for the model, and the integrator meets
two: at 320k it admits 0.94 of capacity (at least 0.85) and `p` stays within
0.15 of its mean. It misses `p` within 0.05 of its mean at 160k (0.1012),
admitting at least 0.90 of offered at 160k (0.8713), deferring on fewer than
20% of samples at 160k (43.8%), `p` below the cap at 320k (it sits at 0.5),
and a release to exactly 0 within 5 s of the offered load dropping from 160k
to 100k at 40 s (`p` is 0.5 at the drop and still above 0 10 s later). Those
five tests are committed ignored with their measured values. The model
shows why. Once the backlog passes 75,000 lines a line costs 65 us, and
149k lines/s at 65 us is 1.21 of 8 cores, so the carried CPU grows while
the backlog lasts and the CPU average reads 1.0 across the span at 160k.
`p` rises at 0.003 per sample until it reaches the cap 23.5 s in, so its
160k range is the end of that rise. Once deferrals stop, `e` is far above
the band and the deferral-free rows hold `p` at the cap instead of releasing
it, and at `p` 0.5 the writers' backoff, not the shed, limits admission to
about 140k lines/s. The release bar also cannot be met from the cap by
construction: the fall bound alone takes at least 9.8 s from 0.5, and 5 s
reaches 0 only from below 0.26, even with the fall row active on the first
sample after the drop.

No assertion the integrator passes separates it from either wrong law: both
also admit at least 0.85 of capacity at 320k and hold `p` within 0.15 of its
mean there. Each wrong law's test asserts the bar it fails instead: D2's
step strays 0.3202 from its mean at 160k, against 0.05, and gain 0.3 defers
on every span sample at 160k, against 20%. The integrator fails both of
those bars as well (0.1012 and 43.8%), so neither test is a discriminator.
D4's bars are unchanged.
