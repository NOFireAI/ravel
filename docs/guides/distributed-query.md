# Distributed query and cross-cluster federation

Ravel can serve one read with more than one process, and it can serve one read
from more than one cluster. Both capabilities come from
[ADR-0071](../adrs/0071-distributed-read-fanout.md) and both are off by
default. This guide is the operator and user view: when distribution engages,
how to turn it on, what a client sees, and what happens when something fails.

For the engine-internal specification (slice partitioning, the merge order,
the budget re-enforcement rules, the credential model) read
[query-engine.md](../query-engine.md#intra-cluster-read-fan-out-adr-0071).
For the as-built status of the epic that delivered it, including the two
places the shipped code deviates from the original design, read
[the status addendum](../reviews/2026-08-adversarial-program/RAVEL-DISTRIBUTED-SEARCH-STATUS.md).

Contents:

- [What distribution does, and what it does not do](#what-distribution-does-and-what-it-does-not-do)
- [When it engages: the cost gate](#when-it-engages-the-cost-gate)
- [The lifecycle of a distributed query](#the-lifecycle-of-a-distributed-query)
- [Turning it on](#turning-it-on)
- [The worker registry and heartbeat](#the-worker-registry-and-heartbeat)
- [Cross-cluster federation](#cross-cluster-federation)
- [Reading `stats.fragments[]`](#reading-statsfragments)
- [Metrics](#metrics)
- [Failure behavior](#failure-behavior)
- [What is not distributed](#what-is-not-distributed)

## What distribution does, and what it does not do

Distribution changes **where bytes are fetched and decoded**, never **what a
query computes**. A distributed result is bit-for-bit identical to the result
the same query would produce on one process, and a differential test
(`distributed_merge_equals_local_bitwise`) enforces that over arbitrary
corpora and slice partitions.

Concretely:

- The query node that receives a request is that query's **coordinator**. This
  is a per-query role, not a process type: every node is a coordinator for the
  requests it receives and a worker for its peers' slices. There is no
  scheduler tier, no leader, and no assignment object.
- The coordinator resolves **one** pinned snapshot, exactly as a single-process
  query does, and ships explicit segment identities to workers. Workers never
  resolve their own snapshot for an intra-cluster slice, so a distributed query
  reads exactly one consistent view of the data.
- Workers fetch, decode, matcher-prune, apply erasure predicates, and pre-merge
  their slice. They do not aggregate and they do not evaluate.
- The coordinator k-way merges every slice under the existing total order and
  then runs the unchanged PromQL evaluator, or the unchanged single-partition
  SQL aggregation.

What does not move: aggregation, evaluation, and the authoritative
cross-segment deduplication. Those stay on the coordinator, which means the
coordinator is still the ceiling for a query whose cost is dominated by final
aggregation over very high cardinality. Distribution buys you more NICs, CPUs,
and page cache for the fetch and decode phase, which is where a large query
spends its time.

## When it engages: the cost gate

Distribution is cost-gated, so a cheap query never pays for the machinery. The
coordinator computes the same pre-execution `CostEstimate` the accounting layer
already produces (ADR-0044) over the resolved snapshot, and distributes only
when the estimate reaches **either** threshold:

| Axis | Flag | Default |
|---|---|---|
| Estimated store bytes | `--distribute-bytes-threshold` | 256 MiB (268435456) |
| Segment count | `--distribute-segments-threshold` | 256 |

Either axis alone trips the gate; a query below both runs the fully local path,
byte-identical to a build without the flag. A third flag,
`--max-parallel-slices` (default 8), caps how many slices one query fans out
into, and therefore how many concurrent remote fetches it can start.

The defaults are ADR-0071's initial gate, set before the crossover benchmark
had a store-latency-bearing environment to run in. On a zero-latency store the
fan-out is pure overhead (see the `distrib_crossover` panel in
[BENCHMARKS.md](../../BENCHMARKS.md)); it pays off when object-store latency,
not CPU, is the bound. Tune both thresholds against your own store before
leaving the defaults in place.

## The lifecycle of a distributed query

<svg viewBox="0 0 980 720" width="980" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Lifecycle of a distributed query: request, resolve, cost gate, slice dispatch, merge, evaluate, respond">
  <style>
    .b{fill:#ffffff;stroke:#333;stroke-width:1.2;}
    .g{fill:#f2f2f2;stroke:#333;stroke-width:1.2;}
    .s{fill:#fff7e0;stroke:#8a6d00;stroke-width:1.2;}
    .r{fill:#eef4ff;stroke:#1f4e9c;stroke-width:1.2;}
    .t{font:12px monospace;fill:#111;}
    .h{font:bold 12px monospace;fill:#111;}
    .a{stroke:#333;stroke-width:1.2;fill:none;marker-end:url(#arL);}
    .d{stroke:#1f4e9c;stroke-width:1.2;fill:none;marker-end:url(#arLd);stroke-dasharray:5 3;}
  </style>
  <defs>
    <marker id="arL" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 z" fill="#333"/></marker>
    <marker id="arLd" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 z" fill="#1f4e9c"/></marker>
  </defs>
  <rect class="b" x="330" y="8" width="220" height="30"/><text class="h" x="384" y="28">Client request</text>
  <path class="a" d="M440,38 L440,60"/>
  <rect class="b" x="140" y="60" width="600" height="46"/>
  <text class="h" x="156" y="78">ravel-server (mode all|query)</text>
  <text class="t" x="156" y="94">auth, tenant resolve, client-query admission permit, deadline</text>
  <path class="a" d="M440,106 L440,128"/>
  <rect class="s" x="140" y="128" width="600" height="62"/>
  <text class="h" x="156" y="146">Plan, then Catalog::resolve -- ONE pinned snapshot</text>
  <text class="t" x="156" y="162">PromQL: plan_selectors; SQL: DataFusion plan, widen-only pushdown</text>
  <text class="t" x="156" y="178">output: Vec&lt;SegmentRef&gt; + pending erasure predicates</text>
  <path class="a" d="M440,190 L440,212"/>
  <rect class="s" x="280" y="212" width="320" height="46"/>
  <text class="h" x="296" y="230">cost gate: should_distribute</text>
  <text class="t" x="296" y="246">bytes &gt;= 256 MiB OR segments &gt;= 256</text>
  <path class="a" d="M280,235 L234,235"/>
  <rect class="b" x="30" y="212" width="200" height="46"/>
  <text class="h" x="42" y="230">below gate: local path</text>
  <text class="t" x="42" y="246">byte-identical fetch</text>
  <path class="a" d="M130,258 L130,493 L176,493"/>
  <path class="a" d="M440,258 L440,282"/>
  <rect class="b" x="140" y="282" width="600" height="32"/>
  <text class="t" x="156" y="303">partition_snapshot: shard-major slices, rendezvous-mapped to workers</text>
  <path class="a" d="M300,314 L300,344"/>
  <path class="a" d="M470,314 L470,344"/>
  <path class="a" d="M660,314 L660,344"/>
  <rect class="b" x="210" y="346" width="180" height="88"/>
  <text class="h" x="222" y="364">worker (slice 1)</text>
  <text class="t" x="222" y="380">auth: bearer token</text>
  <text class="t" x="222" y="396">FragmentAdmission</text>
  <text class="t" x="222" y="412">windowed resolve</text>
  <text class="t" x="222" y="428">fetch/decode/prune</text>
  <rect class="b" x="380" y="346" width="180" height="88"/>
  <text class="h" x="392" y="364">worker (slice 2)</text>
  <text class="t" x="392" y="380">erasure applied</text>
  <text class="t" x="392" y="396">pre-merge slice</text>
  <text class="t" x="392" y="412">stream series runs</text>
  <text class="t" x="392" y="428">+ summary frame</text>
  <rect class="b" x="570" y="346" width="180" height="88"/>
  <text class="h" x="582" y="364">coordinator-local</text>
  <text class="t" x="582" y="380">slice it owns by</text>
  <text class="t" x="582" y="396">rendezvous: no hop</text>
  <text class="t" x="582" y="412">same code path</text>
  <text class="t" x="582" y="428">no token, no admit</text>
  <path class="a" d="M300,434 L300,468"/>
  <path class="a" d="M470,434 L470,468"/>
  <path class="a" d="M660,434 L660,468"/>
  <rect class="b" x="180" y="470" width="520" height="46"/>
  <text class="h" x="196" y="488">k-way merge over the flat run pool (one total order)</text>
  <text class="t" x="196" y="504">coordinator re-enforces max_series and max_bytes_scanned</text>
  <path class="a" d="M440,516 L440,538"/>
  <rect class="b" x="180" y="538" width="520" height="46"/>
  <text class="h" x="196" y="556">unchanged evaluator / single-partition SQL aggregation</text>
  <text class="t" x="196" y="572">the distributed result is bit-for-bit equal to local execution</text>
  <path class="a" d="M440,584 L440,606"/>
  <rect class="b" x="280" y="606" width="320" height="46"/>
  <text class="h" x="296" y="624">JSON / Arrow response</text>
  <text class="t" x="296" y="640">stats.fragments[]: one entry per slice</text>
  <path class="d" d="M740,159 L865,159 L865,342"/>
  <rect class="r" x="770" y="346" width="190" height="88"/>
  <text class="h" x="782" y="364">remote cluster</text>
  <text class="t" x="782" y="380">Resolve scope:</text>
  <text class="t" x="782" y="396">matchers + window</text>
  <text class="t" x="782" y="412">own snapshot, own</text>
  <text class="t" x="782" y="428">auth/erasure/limits</text>
  <path class="d" d="M865,434 L865,493 L704,493"/>
  <text class="t" x="20" y="678">Federation runs whether or not the intra-cluster cost gate trips; a skipped remote sets partial: true plus one warning.</text>
  <text class="t" x="20" y="696">Below the gate, or on an Unsupported worker answer, the whole query runs the local path with no partial result.</text>
</svg>

## Turning it on

Distribution is enabled per query node, and the two flags below are a pair:
either without the other fails startup rather than exposing an
unauthenticated fetch surface or leaving a configured secret inert.

```sh
# On every query-serving node in the cluster (same token file everywhere).
ravel-server --mode all \
  --listen-http 0.0.0.0:4318 \
  --listen-grpc 10.0.0.11:4317 \
  --distributed-query \
  --fragment-auth-token-file /etc/ravel/fragment.token
```

- `--distributed-query` opts this process in. In `--mode all` or
  `--mode query` it does two things at once: it registers the internal
  `SeriesFetch` fragment service on the cluster-internal gRPC listener, and it
  makes the process a coordinator that may fan a large query out. Other modes
  ignore it (no query surface, nothing to distribute).
- `--fragment-auth-token-file` names a file holding the shared
  cluster-internal bearer token. A file, never an inline value or an env var,
  so the secret never appears in a process listing. **Every node in one
  cluster must read the same token**: a coordinator presents exactly this
  token on each dispatch, and a worker refuses any fragment request whose
  bearer token is missing or unequal (compared in constant time). An
  unreadable or empty file fails startup.
- `--listen-grpc` is required in practice. The fragment surface is bound only
  on the cluster-internal gRPC listener, never on the client HTTP listener and
  never on the mTLS listener. A node with no gRPC listener never registers
  itself as a worker, so every slice of every query runs coordinator-local.
  That is correct, just not distributed.
- `--max-inflight-fragments` (default 32) caps how many inbound slice fetches
  this process serves concurrently for other coordinators. This is a **distinct
  admission class** from `--max-concurrent-queries`: a coordinator holding a
  client-query permit while it waits on its own dispatched fragments can never
  deadlock behind client queries queued on the client cap. Over the cap a
  fragment request queues; it is not rejected.

The operator contract for the fragment token is the same one that governs your
object-store credentials: the token conveys no privilege that a process
holding the bucket's S3 credentials does not already have, so treat it as a
cluster-internal secret and keep the gRPC listener off any network a client
can reach. Rotating it means updating the file on every node; during a rolling
restart, a node still holding the old token is rejected by nodes holding the
new one, and the coordinator falls back to local execution for those slices.

Adding capacity is adding processes. A new node with the same flags and the
same bucket appears in the live worker set within one heartbeat interval and
starts receiving slices; a removed node ages out of it. There is nothing to
rebalance and no state to drain.

## The worker registry and heartbeat

Membership needs no new durable state and no consensus. Each distributed query
node writes one object it alone ever writes:

```
sys/query/workers/<process_id>
```

The record is a small JSON control-plane payload carrying the process id, the
`fragment_endpoint` (`host:port` of its cluster-internal gRPC listener), the
`queryfrag` protocol version it speaks, and a liveness timestamp re-stamped on
every beat. The write is an unconditional overwrite: one writer per key, no
compare-and-swap, no contention. This is the same pattern the maintenance role
already uses for its own heartbeats.

On the same cadence (`H` = 60 s by default) every node lists the prefix and
refreshes its view. The **live set** is itself plus every sibling whose stamp
is within `3 * H` of the reader's own clock, in either direction: a stuck
future-dated record drops out just like a stale past-dated one. Worker
identity comes from the key, not the record body, so a record whose body
disagrees with its key cannot smuggle a false identity into the live set.

To place a slice, the coordinator rendezvous-hashes the slice's
`(tenant_hash, signal, shard)` unit over the live set and takes the top owner,
then the next, and so on, giving a deterministic failover order. Two
consequences an operator should expect:

- **Cache affinity for free.** The same shard of the same tenant lands on the
  same worker as long as membership is stable, so the per-process
  content-addressed read caches behave as one aggregate cache. Segments are
  immutable, so there is no invalidation protocol.
- **Version skew costs nothing.** Workers whose advertised protocol version
  differs from the coordinator's are dropped at routing time, before any
  dispatch. During a rolling upgrade a coordinator simply sees fewer eligible
  workers, and in the limit runs everything locally.

Until the first heartbeat cycle completes after startup, a node sees an empty
live set and runs every slice locally. Expect a distributed cluster to take up
to one heartbeat interval after a restart to start fanning out again.

## Cross-cluster federation

Federation is the other half of ADR-0071: a coordinator asks **independent
Ravel clusters** — separate buckets, separate trust domains — to each resolve
their own snapshot, and merges what they return into the same pool its own
selectors feed. It is configured per remote, is independent of the
intra-cluster cost gate (a federated query federates whether or not it also
fans out locally), and is entirely absent from a deployment that configures no
remotes.

```sh
ravel-server --mode query \
  --listen-http 0.0.0.0:4318 \
  --listen-grpc 10.0.0.11:4317 \
  --distributed-query \
  --fragment-auth-token-file /etc/ravel/fragment.token \
  --remote-cluster name=eu,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu.token,tls=on,tls-ca-file=/etc/ravel/eu-ca.pem,skip-unavailable=true \
  --remote-cluster name=apac,endpoint=apac.internal:9443,credential-file=/etc/ravel/apac.token,tls=on,soft-timeout=15s \
  --remote-cluster-soft-timeout 10s
```

`--remote-cluster` is repeatable, once per remote, and its value is a
comma-separated `key=value` spec:

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | The cluster's stable operator-facing label. This is the only identity a client ever sees for the remote (in `warnings`). |
| `endpoint` | yes | `host:port` of the remote's fragment surface. |
| `credential-file` | yes | File holding the bearer token this coordinator presents to that remote. |
| `tls` | no | `on` or `off`, default `off`. |
| `tls-ca-file` | no | CA bundle for the remote's server certificate. Meaningful only with `tls=on`; setting it with TLS off fails startup. |
| `skip-unavailable` | no | `true` or `false`, default `false`. |
| `soft-timeout` | no | Per-remote override of `--remote-cluster-soft-timeout`. |

`--remote-cluster-soft-timeout` sets the default bound for every remote
(default 10 s) as a humantime duration (`10s`, `500ms`). A remote that has not
answered within its bound is treated as unavailable.

Every one of these is validated at startup, not at the first federated query:
a malformed spec, an unknown key, a duplicate cluster name, `tls-ca-file`
without `tls=on`, a zero soft timeout, or an unreadable or empty credential
file all fail the process before it binds a listener.

### What crosses the boundary, and what does not

A federated request carries **matchers, a time window, and budgets** — never
segment references and never object-store credentials. The remote resolves its
own snapshot over that window and runs it through its ordinary query path, so
it enforces its own admission limits, its own tenancy hashing, its own
selective-erasure predicates, and its own budgets.

The credential is an **operator** secret, and the tenant the remote serves is
derived from that credential by the remote's own resolver chain. A coordinator
cannot name a tenant on a remote: whatever `tenant_hash` sits on the wire is
overwritten with the locally resolved value, never read. The calling client's
own credential is never forwarded across a cluster boundary. A remote also
rejects the intra-cluster fragment token outright — that token is a slice
credential, never a federation credential.

`RemoteClusterConfig`'s debug formatting prints `credential: <redacted>`, so a
config dump or a panic message never leaks the operator secret.

Both the value-bearing endpoints (`/api/v1/query`, `/api/v1/query_range`) and
the discovery endpoints (`/api/v1/series`, `/api/v1/labels`,
`/api/v1/label/<name>/values`) federate, through the same coordinator and with
the same semantics.

### What a client sees when a remote is degraded

With `skip-unavailable=false` (the default), a remote that fails or times out
fails the whole request with a typed error. With `skip-unavailable=true` the
query continues without that cluster and says so, in two places at once:

```json
{
  "status": "success",
  "data": { "resultType": "vector", "result": [] },
  "warnings": [
    "remote cluster eu unavailable; results are partial"
  ]
}
```

and, in the query's stats block, `partial: true`. Warnings name only the
operator-facing cluster name; the remote's IP:port and errno are redacted,
because a client reading the envelope is not entitled to the coordinator's
internal topology. Warnings are deduplicated, so a multi-selector request that
federates once per selector still reports one warning per skipped cluster.

Two behaviors worth knowing:

- **A budget overrun is never skippable.** The coordinator re-enforces
  `max_bytes_scanned` over the folded remote spend and fails typed regardless
  of `skip-unavailable`. A budget cap is a correctness bound, not an
  availability property.
- **A data kind this build cannot decode across the boundary** (native
  histogram frames) is a coverage gap, not a hard fault: skippable under
  `skip-unavailable` with a truthful warning naming the reason, and a typed
  error with that same reason without it.

Federation assumes each cluster owns a **disjoint** slice of series identity —
the intended deployment is region- or tenant-sharded, so one series lives in
exactly one cluster. If the same series and timestamp arrive from two clusters
with different values, the merge still emits exactly one sample per timestamp,
but which cluster wins is unspecified, because the provenance fields the total
order tie-breaks on are only comparable within one cluster. The discovery
endpoints carry no such ambiguity (a series id is a canonical function of its
labels, so the cross-cluster union is a plain set union). See
[query-engine.md](../query-engine.md#cross-cluster-federation-adr-0071) for
the full statement.

## Reading `stats.fragments[]`

A distributed query's stats block gains one `fragments` array, with one object
per dispatched slice. The field is absent entirely on a query that did not
distribute, so its presence is itself the signal that fan-out happened.

```json
"stats": {
  "fragments": [
    { "workerEndpoint": "10.0.0.12:4317", "segmentCount": 41, "bytesReported": 189743104, "status": "ok" },
    { "workerEndpoint": "10.0.0.13:4317", "segmentCount": 38, "bytesReported": 174260224, "status": "ok" },
    { "workerEndpoint": "local", "segmentCount": 40, "bytesReported": 181403648, "status": "fallback" }
  ]
}
```

- `workerEndpoint` — where the slice actually ran. A slice the coordinator
  owned by rendezvous, or one that fell back, reports local execution rather
  than a peer's address.
- `segmentCount` — how many pinned segments the slice carried. Badly skewed
  counts across entries mean your ingest shards are unevenly sized; slices are
  cut shard-major and a shard is never split, so shard skew becomes slice skew.
- `bytesReported` — the store bytes that worker reported scanning, already
  folded into the query's own accounting total.
- `status` — `ok`, `fallback` (the slice ran on the coordinator after a remote
  attempt failed), or `error`.

A `fallback` entry is the single most useful diagnostic here: it means a peer
was unreachable or reported itself unavailable, the query still returned a
complete and correct result, and it cost more than it should have. Correlate
with `ravel_distrib_slices_fallback_total` and the worker's own logs.

Per-slice cardinality lives only in this response body, never as a metric
label.

## Metrics

`GET /metrics` renders the `ravel_distrib_*` family on any process with
distribution enabled, under the closed `mode` label alone (no per-shard,
per-worker, or per-tenant label). The family is absent entirely when
distribution is off.

| Metric | Type | What it tells you |
|---|---|---|
| `ravel_distrib_fragment_requests_total` | counter | Inbound slice fetches this process served for other coordinators. |
| `ravel_distrib_fragment_auth_failures_total` | counter | Rejected fragment requests. Anything but zero after a token rotation means a node is still on the old token. |
| `ravel_distrib_fragment_inflight` | gauge | Fragments in flight now. Riding at `--max-inflight-fragments` means inbound slices are queueing. |
| `ravel_distrib_slices_local_total` | counter | Slices this coordinator ran itself with no hop. |
| `ravel_distrib_slices_remote_total` | counter | Slices dispatched to a peer. |
| `ravel_distrib_slices_redispatched_total` | counter | Slices re-dispatched after a failed first attempt. |
| `ravel_distrib_slices_fallback_total` | counter | Slices that ended up running coordinator-local after remote attempts failed. |
| `ravel_distrib_slice_fetch_seconds` | histogram | Per-slice fetch latency, sharing the object-store histogram's bucket layout. |

`slices_local_total` staying high while `slices_remote_total` stays at zero on
a multi-node cluster is the signature of a membership problem: no gRPC
listener, a clock skew wider than `3 * H`, or a protocol-version mismatch
during a partial upgrade. See [observability.md](observability.md) for how to
read the rest of the query cost families alongside these.

## Failure behavior

The rule behind every case below: **intra-cluster execution is
all-or-nothing.** A slice failure is retried, then absorbed locally, then
raised as a typed error. It is never turned into a partial merge. Only
cross-cluster federation can return partial coverage, and only when an
operator opted that remote into it.

<svg viewBox="0 0 940 580" width="940" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Failure flow: intra-cluster slice re-dispatch and local fallback, and the cross-cluster skip path">
  <style>
    .b{fill:#ffffff;stroke:#333;stroke-width:1.2;}
    .g{fill:#f2f2f2;stroke:#333;stroke-width:1.2;}
    .s{fill:#fff7e0;stroke:#8a6d00;stroke-width:1.2;}
    .r{fill:#eef4ff;stroke:#1f4e9c;stroke-width:1.2;}
    .t{font:12px monospace;fill:#111;}
    .h{font:bold 12px monospace;fill:#111;}
    .a{stroke:#333;stroke-width:1.2;fill:none;marker-end:url(#arX);}
    .d{stroke:#1f4e9c;stroke-width:1.2;fill:none;marker-end:url(#arXd);}
  </style>
  <defs>
    <marker id="arX" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 z" fill="#333"/></marker>
    <marker id="arXd" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 z" fill="#1f4e9c"/></marker>
  </defs>
  <text class="h" x="20" y="26">Intra-cluster slice failure (never partial)</text>
  <rect class="b" x="20" y="40" width="190" height="44"/>
  <text class="h" x="32" y="58">slice dispatched</text>
  <text class="t" x="32" y="74">to rendezvous owner</text>
  <path class="a" d="M210,62 L244,62"/>
  <rect class="s" x="248" y="40" width="200" height="44"/>
  <text class="h" x="260" y="58">transport loss or</text>
  <text class="t" x="260" y="74">Unavailable summary</text>
  <path class="a" d="M448,62 L482,62"/>
  <rect class="b" x="486" y="40" width="210" height="44"/>
  <text class="h" x="498" y="58">re-dispatch ONCE to</text>
  <text class="t" x="498" y="74">next rendezvous worker</text>
  <path class="a" d="M696,62 L730,62"/>
  <rect class="b" x="734" y="40" width="186" height="44"/>
  <text class="h" x="746" y="58">run slice on the</text>
  <text class="t" x="746" y="74">coordinator (local)</text>
  <path class="a" d="M827,84 L827,118"/>
  <rect class="b" x="700" y="120" width="220" height="44"/>
  <text class="h" x="712" y="138">still failing:</text>
  <text class="t" x="712" y="154">typed error, never partial</text>
  <path class="a" d="M348,84 L348,118"/>
  <rect class="b" x="248" y="120" width="200" height="44"/>
  <text class="h" x="260" y="138">Corrupt / decode:</text>
  <text class="t" x="260" y="154">terminal, no retry</text>
  <path class="a" d="M115,84 L115,118"/>
  <rect class="b" x="20" y="120" width="210" height="60"/>
  <text class="h" x="32" y="138">SnapshotInvalidated:</text>
  <text class="t" x="32" y="154">one re-resolve, whole</text>
  <text class="t" x="32" y="170">query re-dispatched</text>
  <text class="t" x="20" y="202">Reached at the same point in the fan-out, without a re-dispatch:</text>
  <rect class="b" x="20" y="210" width="430" height="78"/>
  <text class="h" x="32" y="228">Unsupported: version skew,</text>
  <text class="t" x="32" y="244">a non-metrics signal, or a</text>
  <text class="t" x="32" y="260">histogram-bearing slice</text>
  <text class="t" x="32" y="276">=&gt; whole query runs fully local</text>
  <rect class="b" x="480" y="210" width="440" height="78"/>
  <text class="h" x="492" y="228">deadline reached</text>
  <text class="t" x="492" y="244">coordinator cancels the fan-out; stream</text>
  <text class="t" x="492" y="260">teardown reaches workers, freeing their</text>
  <text class="t" x="492" y="276">in-flight GETs and fragment permits</text>
  <text class="h" x="20" y="322">Cross-cluster path (the only source of partial coverage)</text>
  <rect class="r" x="20" y="338" width="190" height="44"/>
  <text class="h" x="32" y="356">federated fetch to</text>
  <text class="t" x="32" y="372">one remote cluster</text>
  <path class="d" d="M210,360 L244,360"/>
  <rect class="r" x="248" y="338" width="200" height="44"/>
  <text class="h" x="260" y="356">soft timeout, or</text>
  <text class="t" x="260" y="372">remote unavailable</text>
  <path class="d" d="M448,360 L482,360"/>
  <rect class="r" x="486" y="338" width="270" height="44"/>
  <text class="h" x="498" y="356">skip-unavailable=false:</text>
  <text class="t" x="498" y="372">query fails typed (the default)</text>
  <path class="d" d="M348,382 L348,416"/>
  <rect class="r" x="248" y="418" width="300" height="78"/>
  <text class="h" x="260" y="436">skip-unavailable=true:</text>
  <text class="t" x="260" y="452">continue without that cluster,</text>
  <text class="t" x="260" y="468">stats partial: true, one warning</text>
  <text class="t" x="260" y="484">per cluster, deduplicated</text>
  <path class="d" d="M548,457 L582,457"/>
  <rect class="r" x="586" y="418" width="334" height="78"/>
  <text class="h" x="598" y="436">client sees a complete envelope</text>
  <text class="t" x="598" y="452">data + warnings[] naming only the</text>
  <text class="t" x="598" y="468">operator-facing cluster name;</text>
  <text class="t" x="598" y="484">IP:port and errno are redacted</text>
  <text class="t" x="20" y="530">A budget overrun is never skippable: the coordinator fails typed regardless of skip-unavailable.</text>
  <text class="t" x="20" y="548">Slice atomicity: a slice joins the merge only after its summary frame; partial frames are discarded whole.</text>
</svg>

What an operator will actually observe, case by case:

| Condition | Behavior | Visible as |
|---|---|---|
| A worker is unreachable, or the stream dies mid-slice | Re-dispatch once to the next rendezvous worker, then run the slice on the coordinator, then fail typed | `slices_redispatched_total`, `slices_fallback_total`, a `fallback` entry in `stats.fragments[]`, a `warn` log naming the endpoint |
| A worker answers `Unavailable` | Same sequence as unreachable | Same |
| A pinned segment vanished (concurrent GC or compaction) | The coordinator re-resolves the snapshot once and re-dispatches the whole query, not one slice; a second occurrence fails | The same single-retry behavior a local query already has |
| A worker reports a corrupt segment, or a frame fails to decode | Terminal immediately: no retry, no local fallback | Typed error; a retry would mask real corruption behind a clean local read |
| A budget trips on a slice, or on the folded total | The same typed `TooManySeries` / `TooManyBytesScanned` a local query raises | HTTP 4xx with the usual budget error |
| The query deadline is reached | The coordinator cancels the fan-out; stream teardown reaches the workers and drop-based cancellation frees their in-flight GETs and fragment permits | Normal deadline error; no leaked permits |
| Protocol version skew during a rolling deploy | Skewed workers are dropped at routing time, so a mismatch costs no round trip; if none are eligible, the query runs fully local | `slices_local_total` rising, `slices_remote_total` flat |
| A non-metrics signal or a histogram-bearing slice | The worker answers `Unsupported` and the coordinator silently re-runs the whole query locally | Nothing to the client; the already-paid remote fetch is still folded into the reported cost, so such a query reports both fetches |
| A remote cluster is slow or down | Fails typed by default; with `skip-unavailable=true`, continues with `partial: true` and one warning | `warnings[]` in the response envelope |

Two invariants that make the retry logic safe, and that are worth knowing when
you read a trace: a slice contributes to the merge only after its terminal
summary frame arrives, so partial frames from a failed attempt are discarded
whole and re-dispatch needs no deduplication bookkeeping; and every slice's
real spend is folded into the query's accounting before any failure or
fallback, so the reported cost never under-counts work already paid for.

## What is not distributed

Deliberately, in this generation:

- **Aggregation and evaluation.** Both stay on the coordinator. The SQL engine
  forces single-partition aggregation for bit-stable float accumulation, and
  that reasoning applies unchanged to distributed partials. Order-insensitive
  pushdown, and the float-tolerance policy that order-sensitive pushdown would
  need, are separately tracked.
- **Logs and traces.** Only the metrics signal distributes. A slice for any
  other signal is answered `Unsupported`, and the query runs locally.
- **Native histograms across the slice boundary.** A histogram-bearing slice
  falls back to local execution, and a remote that streams histogram frames is
  a coverage gap rather than a hard fault.
- **Straggler hedging and slice rebalancing.** A slow-but-alive worker is
  waited on; only a failed or unavailable one is re-dispatched. An oversized
  ingest shard makes an oversized slice, because a shard is never split.
- **Client-visible multi-endpoint Flight SQL.** The Flight SQL surface returns
  exactly one endpoint to a client, whatever the fan-out does behind it. Slice
  tickets are an internal coordinator-to-worker contract; see
  [the status addendum](../reviews/2026-08-adversarial-program/RAVEL-DISTRIBUTED-SEARCH-STATUS.md)
  for why.

## See also

- [ADR-0071](../adrs/0071-distributed-read-fanout.md): the decision, the
  rejected alternatives, and the security model.
- [query-engine.md](../query-engine.md#intra-cluster-read-fan-out-adr-0071):
  the engine-internal specification of slicing, merging, and budget
  re-enforcement.
- [architecture.md](../architecture.md#distributed-reads-and-cross-cluster-federation-adr-0071):
  where the distributed role sits among the services.
- [RAVEL-DISTRIBUTED-SEARCH-STATUS.md](../reviews/2026-08-adversarial-program/RAVEL-DISTRIBUTED-SEARCH-STATUS.md):
  what shipped, the two as-built deviations, and what remains.
- [operations.md](operations.md): the full flag and environment reference.
- [observability.md](observability.md): reading `/metrics` and per-query cost.
- [consistency-model.md](../consistency-model.md): the snapshot, deadline, and
  GC-horizon guarantees distribution inherits unchanged.
</content>
</invoke>
