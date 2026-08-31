# Architecture

Ravel is an OpenTelemetry-native telemetry database for metrics, logs, and
traces. One rule shapes every part of it: the object store is the only
durable component. There is no write-ahead log, no ingester quorum, and no
local disk that matters. Any process can die at any instant, and every
acknowledged write survives.

This file is the map. Each section names the ADR or spec that holds the
reasoning and the exact contracts. The doc index in
[docs/README.md](README.md) lists the operator guides.

## The write path

<svg viewBox="0 0 900 470" width="900" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Write path: clients through gateway, router, and shard actors to object storage">
  <style>
    .b{fill:#ffffff;stroke:#333;stroke-width:1.2;}
    .g{fill:#f2f2f2;stroke:#333;stroke-width:1.2;}
    .s{fill:#fff7e0;stroke:#8a6d00;stroke-width:1.2;}
    .t{font:12px monospace;fill:#111;}
    .h{font:bold 12px monospace;fill:#111;}
    .a{stroke:#333;stroke-width:1.2;fill:none;marker-end:url(#awT);}
  </style>
  <defs>
    <marker id="awT" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 z" fill="#333"/></marker>
  </defs>
  <rect class="b" x="240" y="8" width="420" height="30"/>
  <text class="h" x="252" y="28">OTLP gRPC / OTLP HTTP / Remote Write 1.0 + 2.0</text>
  <path class="a" d="M450,38 L450,64"/>
  <rect class="b" x="240" y="66" width="420" height="46"/>
  <text class="h" x="252" y="84">gateway: auth, tenant resolution, admission limits</text>
  <text class="t" x="252" y="100">per-tenant body size, byte rate, series and stream caps</text>
  <path class="a" d="M450,112 L450,138"/>
  <rect class="b" x="240" y="140" width="420" height="46"/>
  <text class="h" x="252" y="158">ingest router</text>
  <text class="t" x="252" y="174">shard = hash(tenant, series or stream identity) % shards</text>
  <path class="a" d="M450,186 L450,212"/>
  <rect class="b" x="160" y="214" width="580" height="62"/>
  <text class="h" x="172" y="232">shard actors, one single-threaded task per shard</text>
  <text class="t" x="172" y="248">buffer records, then build one immutable columnar object in memory:</text>
  <text class="t" x="172" y="264">RSEG (metrics), RLOG (logs), RSPAN (spans); blake3 over the bytes</text>
  <path class="a" d="M450,276 L450,302"/>
  <rect class="g" x="160" y="304" width="580" height="62"/>
  <text class="h" x="172" y="322">object store (S3 / MinIO; memory and fault-injecting in tests)</text>
  <text class="t" x="172" y="338">1. data PUT (create-if-absent, upload checksum)</text>
  <text class="t" x="172" y="354">2. commit-record PUT (create-if-absent) -- the atomic publish</text>
  <path class="a" d="M450,366 L450,392"/>
  <rect class="s" x="160" y="394" width="580" height="62"/>
  <text class="h" x="172" y="412">acknowledgement</text>
  <text class="t" x="172" y="428">strict: the client is answered only after both PUTs are durable;</text>
  <text class="t" x="172" y="444">the response carries a commit token that makes the write readable</text>
</svg>

A batch enters through the gateway, which resolves the tenant and applies
that tenant's admission limits. The router hashes each record's identity to
a shard. Each shard is one single-threaded actor with a bounded queue, so
ordering inside a shard needs no locks. The actor buffers records, builds
one immutable columnar object in memory, and writes it with two PUTs: the
data object first, then a commit record. The commit record is the publish:
readers see only data that a commit record names.

Acknowledgement has two modes (docs/consistency-model.md is normative).
Strict mode answers the client only after both PUTs are durable, and the
response carries a commit token. A query that presents that token reads the
write with no listing race. Buffered mode answers after admission and
enqueue, and the crash window this opens is documented, bounded, and
chosen per request, never hidden.

All three signals share this pipeline. Remote Write payloads normalize to
the same shape OTLP produces and enter the same router call, so there is no
second flush or commit path to hold correct. Every persistent layout the
path writes is a frozen contract: the RSEG layout (docs/segment-format.md),
the RLOG layout (docs/log-segment-format.md), the protobuf schemas under
proto/, and the object key layout (docs/catalog-and-mvcc.md).

## The catalog and the read path

<svg viewBox="0 0 900 430" width="900" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Read path: commit records fold into a snapshot; queries resolve the snapshot and read segments">
  <style>
    .b{fill:#ffffff;stroke:#333;stroke-width:1.2;}
    .g{fill:#f2f2f2;stroke:#333;stroke-width:1.2;}
    .s{fill:#fff7e0;stroke:#8a6d00;stroke-width:1.2;}
    .t{font:12px monospace;fill:#111;}
    .h{font:bold 12px monospace;fill:#111;}
    .a{stroke:#333;stroke-width:1.2;fill:none;marker-end:url(#arP);}
  </style>
  <defs>
    <marker id="arP" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 z" fill="#333"/></marker>
  </defs>
  <rect class="g" x="60" y="8" width="780" height="58"/>
  <text class="h" x="76" y="26">object store</text>
  <text class="t" x="76" y="42">commit records (one per data object) | snapshot HEAD + parts | derived</text>
  <text class="t" x="76" y="58">catalog objects: column statistics (.cstat), name postings (.npost)</text>
  <path class="a" d="M250,66 L250,96"/>
  <rect class="b" x="80" y="98" width="340" height="76"/>
  <text class="h" x="92" y="116">catalog fold (background, per tenant and signal)</text>
  <text class="t" x="92" y="132">lists sealed commit records, folds them into</text>
  <text class="t" x="92" y="148">snapshot parts, publishes HEAD by CAS;</text>
  <text class="t" x="92" y="164">losing the CAS is ordinary, never corruption</text>
  <path class="a" d="M600,66 L600,96"/>
  <rect class="b" x="480" y="98" width="340" height="76"/>
  <text class="h" x="492" y="116">query resolve</text>
  <text class="t" x="492" y="132">one GET of HEAD pins one immutable snapshot;</text>
  <text class="t" x="492" y="148">the whole query runs against that snapshot,</text>
  <text class="t" x="492" y="164">so a concurrent fold never changes its answer</text>
  <path class="a" d="M600,174 L600,204"/>
  <rect class="b" x="160" y="206" width="580" height="78"/>
  <text class="h" x="172" y="224">plan and prune</text>
  <text class="t" x="172" y="240">time window and shard bounds from the snapshot; skip indexes, bloom</text>
  <text class="t" x="172" y="256">filters, and exact column statistics prune objects and blocks; a prune</text>
  <text class="t" x="172" y="272">may only ever widen the read set, never narrow it below correctness</text>
  <path class="a" d="M450,284 L450,314"/>
  <rect class="b" x="160" y="316" width="580" height="52"/>
  <text class="h" x="172" y="334">fetch and decode</text>
  <text class="t" x="172" y="350">footer probe, then ranged or whole-object GETs; a read cache holds hot chunks</text>
  <path class="a" d="M320,368 L320,398"/>
  <path class="a" d="M580,368 L580,398"/>
  <rect class="s" x="160" y="400" width="300" height="26"/>
  <text class="h" x="172" y="418">PromQL evaluator (/api/v1/*)</text>
  <rect class="s" x="480" y="400" width="340" height="26"/>
  <text class="h" x="492" y="418">SQL: samples, logs, spans (/api/v1/sql, Flight SQL)</text>
</svg>

Commit records are the write-side truth, and the catalog fold turns them
into a read-side index. The fold runs in the background per tenant and
signal. It lists commit records whose ingest hour has sealed, folds them
into content-addressed snapshot parts, and publishes a new HEAD with a
compare-and-swap. Two folders can race safely: the loser's CAS fails and
nothing is corrupted. An hour seals only after
`max_flush_lifetime + clock_skew_allowance + fold_safety_margin` has passed,
so a fold that runs right after a write finds nothing eligible. That is the
expected answer, not a failure. docs/catalog-and-mvcc.md holds the exact
protocol.

A query starts with one GET of HEAD, which pins one immutable snapshot.
Everything the query reads comes from that snapshot, so results are stable
under concurrent folds and compactions. Planning prunes with the snapshot's
time and shard bounds, then with skip indexes, bloom filters, and exact
per-column statistics. Pruning obeys one invariant: it may widen the read
set, never narrow it below what correctness requires. Statements that exact
catalog statistics can answer in full, such as a predicate-free count,
issue zero data reads.

The fetch layer probes an object's footer, then issues ranged GETs for the
blocks the plan kept, or a whole-object GET when that is cheaper. The
request-cost/latency trade is an operator knob, not a constant. A read
cache holds hot chunks in memory; a cold and a warm pass over the same data
differ only in requests issued, never in rows returned.

Two query engines sit on top of one fetch layer. The PromQL evaluator
serves the Prometheus-compatible `/api/v1/*` routes. The SQL engine
(ravel-sql, on DataFusion) serves the `samples`, `logs`, and `spans` tables
over `POST /api/v1/sql` and Flight SQL. Both engines deduplicate
cross-segment duplicates with the same bit-exact total order, and Arrow and
DataFusion stay isolated behind the `sql` and `flight-sql` cargo features.

## Maintenance

Compaction, retention, and garbage collection run as background work over
the same store, and every one of their outputs is published through the
same commit or CAS protocols the write path uses.

- Compaction merges many small L0 objects into few larger L1 parts per
  `(tenant, signal, shard, ingest hour)` bucket, streaming blocks rather
  than materializing inputs. Output parts are content addressed, and a
  compaction record publishes them atomically; two compactors racing on
  one bucket converge on one winner (ADR-0979 bounds the merge's memory).
- Age-based retention and the GC sweeper delete only what nothing
  references, behind grace periods and a mass-orphan circuit breaker, so a
  listing anomaly withholds deletions instead of amplifying them.
- The fold (above) is the third background loop, and an on-demand form of
  it exists per tenant (below).

The maintenance and fold supervisors derive their tenant set from storage,
not from flags: each cycle lists the tenant prefixes under `t/` and runs
every discovered tenant's tick. Once a tenant carries a durable
`t/<hash>/config` lifecycle record, that record is the sole authority, and
no flag can silently exclude the tenant from retention. A discovery failure
skips the whole cycle and is retried; it never falls back to an empty
tenant set, because an empty set would look exactly like a healthy idle
cycle on the dashboard meant to catch it. The
`ravel_maintain_tenants_discovered` and `..._maintained` gauges expose the
gap a restriction or a bug would open.

## One binary, four modes

`ravel-server` runs every role as a mode of one binary: `all`, `gateway`,
`query`, and `maintain`. Crate boundaries keep the roles separable, and
`maintain` is a disposable worker that serves no ingest or query surface.
Every mode binds `--listen-http` and serves three routes there
unconditionally:

- `/healthz` answers 200 whenever the HTTP loop is serving. It never
  depends on store reachability, so a store outage cannot get healthy
  processes killed.
- `/readyz` gates on completed startup, then follows a background store
  probe with asymmetric hysteresis: four consecutive probe failures flip it
  to 503, one success recovers it. The kubelet path reads an in-memory
  atomic and never touches the store.
- `/metrics` renders hand-written Prometheus text from counters the process
  already holds. The label set is fixed and small on purpose; per-shard and
  free-form labels are excluded so Ravel's own telemetry cannot explode.

Startup on any non-memory store is gated on a durable `sys/qualification`
record: a deployment must run `ravel-cli store qualify` once before the
first server starts. This proves the backend honors the semantics the
commit protocol depends on, before any data rides on them.

Three listeners exist. `--listen-http` carries OTLP HTTP, Remote Write, the
query and analytics routes, and SQL. `--listen-grpc` carries OTLP gRPC,
Flight SQL, and the cluster-internal fragment service. `--mtls-listener` is
a third listener for mTLS-terminated traffic; the resolver that trusts the
forwarded client-certificate header exists only in that listener's router
chain, so the public listeners are safe against header forgery by
construction. Startup validation refuses any configuration that would break
this isolation.

## Distributed reads and cross-cluster federation

<svg viewBox="0 0 900 540" width="900" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Cluster topology: symmetric query nodes over one shared bucket, with remote clusters reached only through their API">
  <style>
    .b{fill:#ffffff;stroke:#333;stroke-width:1.2;}
    .g{fill:#f2f2f2;stroke:#333;stroke-width:1.2;}
    .s{fill:#fff7e0;stroke:#8a6d00;stroke-width:1.2;}
    .r{fill:#eef4ff;stroke:#1f4e9c;stroke-width:1.2;}
    .t{font:12px monospace;fill:#111;}
    .h{font:bold 12px monospace;fill:#111;}
    .a{stroke:#333;stroke-width:1.2;fill:none;marker-end:url(#arT);}
    .d{stroke:#1f4e9c;stroke-width:1.2;fill:none;marker-end:url(#arTd);stroke-dasharray:5 3;}
  </style>
  <defs>
    <marker id="arT" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 z" fill="#333"/></marker>
    <marker id="arTd" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L7,3 L0,6 z" fill="#1f4e9c"/></marker>
  </defs>
  <rect class="b" x="280" y="8" width="280" height="30"/>
  <text class="h" x="292" y="28">Clients (PromQL / SQL / Flight SQL)</text>
  <path class="a" d="M380,38 L220,82"/>
  <path class="a" d="M460,38 L620,82"/>
  <rect class="b" x="60" y="84" width="320" height="100"/>
  <text class="h" x="72" y="102">query node A -- coordinator</text>
  <text class="t" x="72" y="118">mode all|query, --distributed-query</text>
  <text class="t" x="72" y="134">resolves ONE pinned snapshot</text>
  <text class="t" x="72" y="150">partitions, dispatches, merges</text>
  <text class="t" x="72" y="166">serves SeriesFetch + Flight SQL</text>
  <rect class="b" x="460" y="84" width="380" height="100"/>
  <text class="h" x="472" y="102">query node B -- peer worker</text>
  <text class="t" x="472" y="118">same binary, same flags: any node is a</text>
  <text class="t" x="472" y="134">coordinator for the requests it receives</text>
  <text class="t" x="472" y="150">and a worker for its peers' slices</text>
  <text class="t" x="472" y="166">fragment surface on --listen-grpc only</text>
  <path class="a" d="M380,118 L456,118"/>
  <path class="a" d="M460,150 L384,150"/>
  <text class="t" x="20" y="206">A and B exchange slices in either direction over the cluster-internal gRPC listener, bearer-token authed.</text>
  <path class="a" d="M200,184 L200,226"/>
  <path class="a" d="M640,184 L640,226"/>
  <rect class="g" x="60" y="228" width="780" height="76"/>
  <text class="h" x="76" y="246">S3 bucket = one cluster (the only durable state)</text>
  <text class="t" x="76" y="262">immutable segments, commit records, manifests, snapshot HEAD/parts</text>
  <text class="t" x="76" y="278">sys/gc, sys/tenancy, sys/query/workers/&lt;process_id&gt; heartbeats</text>
  <text class="t" x="20" y="326">Membership: every distributed query node PUTs its own sys/query/workers/&lt;process_id&gt; record every</text>
  <text class="t" x="20" y="342">60 s; readers take the live set as records within 3 x H and rendezvous-hash (tenant_hash, signal, shard).</text>
  <path class="d" d="M450,356 L450,390"/>
  <text class="t" x="470" y="378">reached only through their own API</text>
  <rect class="r" x="60" y="392" width="780" height="96"/>
  <text class="h" x="76" y="410">Remote Ravel clusters (--remote-cluster): separate buckets, separate trust domains</text>
  <text class="t" x="76" y="426">Reached only through the remote's own API endpoint: the coordinator sends matchers and a window,</text>
  <text class="t" x="76" y="442">the remote resolves its own snapshot under its own operator credential and its own erasure.</text>
  <text class="t" x="76" y="458">No segment references, no S3 credentials, and no client credential ever cross this boundary.</text>
  <text class="t" x="20" y="514">Compute stays disposable: a node can be added or removed at any time, and membership reconverges.</text>
</svg>

A read can span more than one process. This is off by default and
cost-gated: the node that receives a request coordinates it, resolves one
pinned snapshot, and fans out only when a pre-execution estimate clears the
gate. Cheap queries run the untouched local path. Worker membership needs
no new durable state; nodes self-register with a heartbeat key, and
rendezvous hashing assigns slices. The fragment service admits inbound
slices from a pool separate from client-query admission, so a coordinator
holding a client permit can never deadlock on fragments that need the same
pool. A slice the coordinator owns runs through the same service in
process, so local and remote slices cannot diverge in behavior.

Federation treats every remote cluster as a separate trust domain. The
coordinator sends matchers and a time window to the remote's public API
under an ordinary per-remote credential. The remote resolves its own
snapshot under its own erasure state. No segment reference, S3 credential,
or client credential ever crosses the boundary. A slow or unreachable
remote degrades to partial coverage, and that state is always visible in
the query's stats and warnings. docs/query-engine.md and
docs/guides/distributed-query.md hold the full contract.

## Analytics and alerting

`ravel-analytics` is a post-evaluation stage of pure per-series functions:
change point detection and robust summary statistics over
`(timestamp_ns, f64)` slices. `POST /api/v1/analytics` runs the same range
evaluation as `/api/v1/query_range`, then applies the requested operation
per series. It links no Arrow and needs no cargo feature. Approximation
exists only as an explicit, visible `downsample` option.

Alert rules are queries. One background task per tenant evaluates the
tenant's rules against the same query engines the API serves from, on a
jittered interval. State transitions, and only transitions, are written as
immutable records under the alerts keyspace, through the same
data-PUT-then-commit-PUT protocol as any signal. No alert state lives in
process memory, so a restarted evaluator resumes from the records. Webhook
and Alertmanager sinks fire only after the record is durable; delivery is
at-least-once and never blocks the write. A firing rule re-notifies on its
`repeat_interval`, anchored to the durable record's timestamp, so the
schedule survives restarts.

## On-demand catalog fold

`POST /api/v1/admin/fold` runs the same fold the background loop runs, for
one tenant and one signal, under the same credential the query surfaces
require. Concurrent calls for one `(tenant, signal)` coalesce into a single
fold, and a rate gate declines to fold when HEAD is younger than the fold
interval. Together the gates bound one caller to one fold per signal per
interval. Every completed call reports one of four statuses, because these
are different facts an operator has to tell apart:

| `status` | Meaning |
|---|---|
| `published` | A new snapshot was published; HEAD names different content than before. |
| `nothing_eligible` | No commit was eligible. `head_advanced` says whether the sealing watermark still moved. |
| `lost_cas` | A concurrent fold won the HEAD CAS; that fold's snapshot is current. |
| `throttled` | HEAD is younger than the fold interval; nothing was listed, so no eligibility claim is made. |

Authentication and validation errors (401, 400, 403) precede any store
work. A 503 does not carry that guarantee: the fold may have written parts
before failing, so a 503 means "outcome unknown, retry", never "nothing was
written".

## Crate map

Dependency order, no cycles:

```
ravel-types
  <- ravel-proto        (prost-generated footer/commit messages)
  <- ravel-object-store (S3/MinIO, memory, fault-injecting backends)
  <- ravel-segment      (RSEG metrics format; types, proto)
  <- ravel-logseg       (RLOG logs format)
  <- ravel-commit       (commit records; types, proto, object-store)
  <- ravel-catalog      (fold, snapshots, column statistics; commit)
  <- ravel-otlp         (types; opentelemetry-proto)
  <- ravel-remote-write (Remote Write 1.0/2.0 decode + normalize)
  <- ravel-otap         (OTel Arrow ingest; arrow isolated here)
  <- ravel-ingest       (router, shard actors, admission)
  <- ravel-promql       (parser + evaluator; types)
  <- ravel-query        (fetch layer, engines' shared read path)
  <- ravel-maintain     (L0->L1 compactor, GC sweeper, retention)
  <- ravel-analytics    (pure post-evaluation compute; types only)
  <- ravel-alerting     (rule/condition/state/record logic, no I/O)
  <- ravel-sql          (DataFusion engine; arrow + datafusion)
  <- services/ravel-server, services/ravel-cli
ravel-test-util (types, object-store) used by all dev-deps
```

Each crate's normative doc is listed in the doc map in CLAUDE.md and in
[docs/README.md](README.md). docs/consistency-model.md governs
acknowledgement, visibility, and crash behavior everywhere.
