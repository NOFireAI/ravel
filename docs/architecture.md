# Architecture

This is the one-page overview: what exists, what is durable, what is
disposable, and how the pieces meet. Every answer here is short on purpose
and links to the document that holds the detail. Read
[docs/concepts.md](concepts.md) first if the vocabulary is new; that page
defines every term used below and expands every acronym.

One rule shapes everything: the object store is the only durable component.
There is no write-ahead log, no replicated block device, and no local disk
whose loss loses data. Any process can die at any instant, and every
strictly acknowledged write survives.

## Which components exist

`ravel-server` is one binary that runs four modes: `all`, `gateway`,
`query`, and `maintain`, selected with `--mode`. A deployment is some number
of processes of that binary over one bucket. `ravel-cli` is the operator and
inspection tool. `ravel-operator` reconciles the `RavelCluster` custom
resource on Kubernetes ([guides/kubernetes.md](guides/kubernetes.md)).
`ravel-ingest-router` is an optional front door that pins each tenant to a
stable subset of gateway replicas.

Inside a server process, five subsystems do the work: ingest (gateway
admission, the router, and the shard actors), the catalog (commit records,
folds, snapshots), the read path (snapshot resolution, pruning, segment
fetch, caching), the two query engines (a PromQL evaluator and a
DataFusion-based SQL engine), and maintenance (compaction, retention, the
garbage-collection sweep, the integrity scrubber). Each mode enables a
subset. `gateway` runs ingest and the catalog fold. `query` runs the read
path, both engines, the fold, and alert-rule evaluation. `maintain` runs
maintenance and nothing else. `all` is `gateway` plus `query` in one
process; it does not run maintenance, so a deployment with no `maintain`
process never compacts or deletes anything.

The ingest surfaces are OTLP over HTTP and gRPC, Prometheus Remote Write,
and OTAP behind a cargo feature. The query surfaces are the
Prometheus-compatible `/api/v1/*` routes, `POST /api/v1/sql`, Flight SQL
behind a cargo feature, and `POST /api/v1/analytics`. The ingest protocols
share one pipeline: Remote Write payloads normalise to the shape OTLP
produces and enter the same router call, so there is no second flush or
commit path to hold correct.

Two capabilities that the decision records discuss do not exist in the
shipped system: RavelQL, and Sigma or OCSF rule ingestion. Logs, spans,
compaction, and catalog snapshots all ship.

## Which state is durable

Objects in the bucket, and nothing else. There are five kinds:

- Data objects: immutable columnar segments, one per flush at L0 and one per
  compaction output at L1. Their layouts are frozen contracts, one per
  signal: [RSEG](segment-format.md), [RLOG](log-segment-format.md), and
  [RSPAN](span-segment-format.md).
- Commit records: immutable objects that publish a data object. Also the
  compaction records, rewrite records, retention tombstones, and idempotency
  markers that publish the other durable transactions.
- Catalog objects: immutable snapshot parts and column-statistics objects,
  plus the HEAD pointer, the catalog's one mutable object, written only
  under compare-and-swap.
- Per-tenant records: the shard-count provisioning record, the append-only
  encryption key-epoch record, the tenant configuration, admission usage
  snapshots, alert state, and metric family metadata. These are the records
  that change after they are written, each under its own rule, and a
  snapshot names none of them.
- Deployment records under the bucket root: the store qualification record,
  the tenancy scheme marker, the authentication map, per-process liveness
  heartbeats, and advisory work claims.

The exact key layout and the mutability rule for every one of them is in
[docs/catalog-and-mvcc.md](catalog-and-mvcc.md).

## Which processes are disposable

All of them. Disposable is a stronger claim than stateless: a process holds
buffered records, a resolved snapshot, a read cache, and a membership view,
but never state that another process needs in order to recover. No restart
path reads a file another process wrote locally, and no correctness argument
depends on a particular process returning.

Concretely, that means five things. A process can be replaced by a fresh one
with no handoff, so rolling restarts, autoscaling, and spot interruption are
ordinary events rather than incidents. Local disk is a cache only: the read
cache's disk tier and any scratch directory can be deleted between runs with
no effect except a colder first pass ([guides/caching.md](guides/caching.md)).
There is no leader to elect, no quorum to lose, and no membership state to
repair, because the store's own conditional writes are the only arbiter.
Alert-rule state lives in durable records rather than process memory, so a
restarted evaluator resumes from them. And a process's identity is
short-lived: a restarted writer takes a new epoch rather than reusing a
sequence number, so nothing depends on a process remembering what it did
before it died.

What disposability costs is that every durable step is a network round trip.
The floor on visibility latency is object-store latency, and request count is
a real bill ([guides/cost-model.md](guides/cost-model.md)).

Two HTTP routes exist on every mode so an orchestrator can act on this
model. `/healthz` answers 200 whenever the HTTP loop is serving and never
depends on store reachability, so a store outage cannot get healthy processes
killed. `/readyz` gates on completed startup and then follows a background
store probe with asymmetric hysteresis: four consecutive probe failures flip
it to 503 and one success recovers it, and the kubelet path reads an
in-memory atomic rather than touching the store. `/metrics` is the third
unconditional route, rendering a fixed and deliberately small label set so
Ravel's own telemetry cannot explode
([guides/observability.md](guides/observability.md)).

## How ingest, query, and maintenance interact

![Write path: clients through gateway, router, and shard actors to object storage, then acknowledgement](diagrams/architecture-write-path.svg)

Ingest writes two objects per flush. A batch enters through the gateway,
which resolves the tenant and applies that tenant's admission limits. The
router hashes each record's identity to a shard, and each shard is one
single-threaded actor with a bounded queue. The actor buffers records, builds
one immutable columnar segment in memory, and writes it with two PUTs: the
data object first, then the commit record that publishes it. Strict
acknowledgement answers the client only after both PUTs are durable and
returns one commit token per shard the request's points flushed through;
buffered acknowledgement answers after admission and enqueue, returns no
token, and carries a bounded loss window on an abrupt crash.
[docs/consistency-model.md](consistency-model.md) is normative for both, and
[docs/ingest.md](ingest.md) holds the pipeline's internals.

![Read path: commit records fold into a snapshot; queries pin the snapshot, prune, fetch, and evaluate](diagrams/architecture-read-path.svg)

Query reads from a pinned snapshot. It begins with one GET of HEAD, which
pins one immutable snapshot for the whole execution, so results are stable
under concurrent folds and compactions. Planning prunes with the snapshot's
time and shard bounds, then with skip indexes, bloom filters, and exact
per-column statistics, under one invariant: pruning may widen the read set,
never narrow it below what correctness requires. The fetch layer probes an
object's footer and then issues ranged GETs for the blocks the plan kept, or
a whole-object GET where that is cheaper. A read cache holds hot chunks, and
a cold pass and a warm pass over the same data differ only in requests
issued, never in rows returned. [docs/query-engine.md](query-engine.md) holds
the engine contract, and [guides/query.md](guides/query.md) the endpoints.

Maintenance reshapes the bucket without changing any answer. Compaction
merges many L0 segments into fewer L1 segments per (tenant, signal, shard,
ingest hour) bucket, publishing them with one compaction record after
checking that input and output record counts are exactly equal. Retention
expires data by age behind a durable tombstone. The sweep physically removes
what nothing references, behind grace periods and a mass-orphan circuit
breaker, so a listing anomaly withholds deletions instead of amplifying them.
The catalog fold is the fourth loop: it turns sealed commit records into
snapshot parts and publishes a new HEAD by compare-and-swap.

The three meet at the commit record and at HEAD, and nowhere else. Ingest
only creates commit records; the fold only reads them and writes snapshot
parts; query only reads HEAD and the objects it names. Maintenance
supervisors derive their tenant set by listing tenant prefixes rather than
from a flag, so no configuration can silently exclude a tenant from
retention, and a discovery failure skips and retries the cycle rather than
falling back to an empty set.

<a id="on-demand-catalog-fold"></a>
One maintenance operation is also an API. `POST /api/v1/admin/fold` runs the
same fold the background loop runs, for one tenant and one signal, under the
credential the query surfaces take. Concurrent calls for one pair coalesce
into a single fold, and a rate gate declines when HEAD is younger than the
fold interval. Its response distinguishes four outcomes, because they are
different facts for an operator: `published`, `nothing_eligible`, `lost_cas`,
and `throttled`. Right after a load the honest answer is `nothing_eligible`,
because an ingest hour is not foldable until its sealing window has elapsed.

## What Ravel assumes about the object store

These are requirements, not preferences. Production startup fails if the
configured backend under-reports any of them:

- Read-after-write consistency on create, so a commit record is readable the
  instant it exists.
- List-after-write consistency, so a commit record is discoverable by
  listing the instant it exists.
- Conditional create-if-absent put, which is what makes commit records and
  data objects safe to write from many uncoordinated processes.
- Compare-and-swap on a version, which is what makes the HEAD pointer safe to
  publish.
- Ranged reads including suffix ranges, which is what makes footer-first
  segment reads possible.
- Paginated prefix listing, used by discovery and by garbage collection.

`--mode maintain` additionally requires multipart upload. Optional and not
required by any mode: batch delete, lifecycle expiration, and server-side
encryption headers.

Startup on any non-memory store is also gated on a durable qualification
record: a deployment runs `ravel-cli store qualify` once before its first
server starts, which proves the backend honours the semantics the commit
protocol depends on before any data rides on them. The capability table, the
qualification suite, and the retry and timeout contract are in
[docs/object-store-contract.md](object-store-contract.md).

## Where the trust and failure boundaries are

The tenant is the isolation boundary. A tenant never appears in an object
key, only a hash of it does, and no query resolves across two tenants. Every
per-tenant limit, retention policy, encryption key epoch, and shard count
hangs off that boundary.

The listener is the authentication boundary. `--listen-http` carries OTLP
HTTP, Remote Write, the query and analytics routes, and SQL. `--listen-grpc`
carries OTLP gRPC, Flight SQL, and the cluster-internal fragment service.
`--mtls-listener` is a separate listener for mTLS-terminated traffic, and the
resolver that trusts a forwarded client-certificate header exists only in
that listener's router chain, so the public listeners are safe against header
forgery by construction. Startup validation refuses any configuration that
would break the isolation.

A remote cluster is a separate trust domain. Federation sends matchers and a
time window to the remote's public API under an ordinary per-remote
credential, and the remote resolves its own snapshot under its own state. No
segment reference, storage credential, or client credential crosses the
boundary. A slow or unreachable remote degrades to partial coverage, and that
state is always visible in the query's stats and warnings
([guides/distributed-query.md](guides/distributed-query.md)).

The failure boundaries are the two PUTs of a write and the compare-and-swap
of a fold. A failure on either side of them has a defined outcome and never
an ambiguous one for Ravel, only sometimes for the client. Storage
credentials are scoped so that the grant set a role carries matches the
objects its mode writes; the store itself is the last boundary, and its
durability is the floor under every guarantee on this page.

## What happens when processes run concurrently

![Cluster topology: symmetric query nodes over one shared bucket, with remote clusters reached only through their API](diagrams/architecture-cluster-topology.svg)

Processes are symmetric and uncoordinated. Any number of them may serve any
mode over one bucket, and none of them exchanges state with another except
through objects. Every race is resolved by a conditional write rather than by
a lock: two writers on one commit key are separated by create-if-absent, two
folders by the HEAD compare-and-swap, two compactors on one bucket by the
create-if-absent on the compaction record. The loser of any of these
re-reads, and its already-written objects become unreferenced and age out.

Work is divided without a coordinator. Maintain workers and query workers
each write a liveness heartbeat under their own key, list their siblings to
compute a live set, and partition the work unit space over it by rendezvous
hashing, so N replicas divide the work rather than each paying for all of it.
Membership needs no new durable state and no lease, and a stale heartbeat is
self-correcting on the next interval.

A single read can also span processes. This is off by default and cost-gated:
the receiving node coordinates the request, resolves one pinned snapshot, and
fans out only when a pre-execution estimate clears the gate, so cheap queries
run the untouched local path. The fragment service admits inbound slices from
a pool separate from client-query admission, so a coordinator holding a
client permit cannot deadlock on fragments needing the same pool, and a slice
the coordinator owns runs through the same service in process, so local and
remote slices cannot diverge in behaviour.

Concurrency never changes an answer. A query pins its snapshot at the first
GET, and commits, folds, compactions, and deletions that land mid-query are
outside it.

## How the system behaves after an interruption or a retry

Nothing is repaired on startup, because nothing needs repairing. A restarted
process reads the store and continues; there is no recovery log to replay and
no local state to reconcile.

An interruption between a write's two PUTs leaves a data object that no
commit record names. It is invisible to every query, and the sweep deletes it
after the grace period. An interruption after the commit PUT but before the
response leaves data that is durable and visible while the client believes
the write failed. That is the one genuinely ambiguous case, and it is
ambiguous for the client: an unkeyed retry stores a duplicate, and a keyed
retry replays its idempotency marker and stores nothing new, because the
marker PUT precedes the acknowledgement.

Retries are safe everywhere because no persisted step is a
read-modify-write. Data objects are content-addressed, commit records and
markers are created with create-if-absent, and HEAD moves only under
compare-and-swap. Delivery is at-least-once: for metrics a duplicate
collapses at query time by series and timestamp, and for logs and spans the
opt-in idempotency key is what makes a retry a no-op.

Where an API cannot tell which side of a boundary a failure fell on, it says
so. Authentication and validation errors precede any store work, so a 401,
400, or 403 guarantees nothing was written; a 503 from the on-demand fold
means the outcome is unknown and the call should be retried. The full crash
table is [docs/consistency-model.md](consistency-model.md#crash-matrix-strict-mode),
and recovering a whole deployment from the bucket alone is
[guides/disaster-recovery.md](guides/disaster-recovery.md).

## Crate map

The workspace has 32 members: 28 crates under `crates/` and four binaries
under `services/`. Grouped by dependency layer, so a crate depends only on
crates in its own group or above:

```text
foundations       ravel-types, ravel-proto, ravel-codec, ravel-object-store,
                  ravel-cache, ravel-affinity, ravel-analytics,
                  ravel-tracing-export
formats, identity ravel-segment, ravel-logseg, ravel-rspan,
                  ravel-tenant-resolve, ravel-promql
commit, members   ravel-commit, ravel-fleet
catalog           ravel-catalog
wire decode       ravel-otlp, ravel-remote-write, ravel-otap, ravel-alerting
paths             ravel-ingest, ravel-maintain, ravel-query, ravel-sql
binaries          ravel-server, ravel-cli, ravel-ingest-router,
                  ravel-operator
test and bench    ravel-bench, ravel-failure-tests, ravel-promql-difftest,
                  ravel-sim
```

The last group is development-only and no shipping crate depends on it.
Arrow and DataFusion are isolated behind the `sql`, `flight-sql`, and `otap`
cargo features, so a default build links neither.

The documentation index in [docs/README.md](README.md) lists the normative
document for each crate, and [docs/concepts.md](concepts.md) carries the
[glossary](concepts.md#glossary) for every term on this page.
