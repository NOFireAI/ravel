# Architecture (Phase 1 scope)

Read the ADRs for the reasoning behind each choice. This file is the map.

```
OTLP gRPC / OTLP HTTP-protobuf / Remote Write 1.0+2.0 (/api/v1/write)
        |
        v
  gateway (auth, tenant, limits)          services/ravel-server --mode all
        |
        v
  ingest router: hash(tenant, series_id) % shards
        |
        v
  shard actors (single-threaded, bounded mpsc)
        |  buffer -> RSEG L0 build -> blake3
        v
  ObjectStoreBackend  (memory | fault-injecting | S3/MinIO)
        |   data PUT + commit PUT (create-if-absent)
        v
  commit records  <---- catalog resolve (LIST per shard/hour) ----+
                                                                  |
  query frontend: /api/v1/query, /query_range, /labels, /series  -+
        |
        v
  segment reader: suffix GET footer -> prune series via SERIES_META
                  -> ranged GETs for needed pages -> sample iterators
        |
        v
  PromQL evaluator (selectors + lookback, Phase 1)
```

Crate dependency order (no cycles):

```
ravel-types
  <- ravel-proto (prost-generated footer/commit messages)
  <- ravel-object-store
  <- ravel-segment      (types, proto)
  <- ravel-commit       (types, proto, object-store)
  <- ravel-catalog      (commit)
  <- ravel-otlp         (types; opentelemetry-proto)
  <- ravel-otap         (types, segment; arrow, isolated to this crate)
  <- ravel-ingest       (segment, commit, object-store, otlp)
  <- ravel-promql       (types)
  <- ravel-query        (catalog, segment, promql)
  <- ravel-maintain     (commit, object-store, segment, logseg; the L0->L1
                          compactor, GC sweeper, and age-based retention)
  <- ravel-analytics    (types only; pure post-evaluation compute stage)
  <- ravel-alerting     (types, logseg; pure rule/condition/state/record
                          logic, no I/O and no scheduler)
  <- ravel-sql          (query, catalog, types; arrow + datafusion, in
                          progress -- see below)
  <- services/ravel-server, services/ravel-cli
ravel-test-util (types, object-store) used by all dev-deps
```

Phase 1 runs every service as modes of one binary (`ravel-server --mode
all|gateway|query`). Crate boundaries keep the split honest so later phases
can deploy them separately. A fourth mode, `--mode maintain`, runs no
ingest or query surface: it is a disposable background worker that drives
`ravel-maintain`'s compaction, age-based retention, and GC sweeper per
tenant over every shard. It requires
a `multipart`-capable object store (compaction is the only writer of
multipart objects) and still binds `--listen-http`, serving the `/healthz`,
`/readyz`, and `/metrics` routes only (no ingest or query surface).

The maintenance driver and the catalog fold task (below) both derive their
tenant set from storage, not from CLI flags (ADR-0048 decision 3): each
cycle, one supervisor re-enumerates every tenant prefix storage
reports under `t/` (`ravel_maintain::discover_tenants`, one `list_delimited`
call) and runs the existing per-tenant maintenance tick for each. This is
what makes a server authenticating tenants through OIDC or mTLS -- which
populates neither `--tenant-token` nor `--maintain-tenant` -- still compact,
retain, and GC every tenant it holds data for; previously the tenant set came
only from those flags, so such a deployment silently maintained nothing.
`--tenant-token`/`--maintain-tenant` are now an
optional *restriction* on the discovered set, not its source, and it governs
only tenants with no durable config record: for such a tenant, unset means it
is maintained, and set narrows the no-config discovered set to exactly the
named tenants, with an excluded no-config tenant counted, not silently dropped.
Under ADR-0066 decision 6 the maintenance and fold supervisors consult each
tenant's durable `t/<hash>/config` lifecycle record, and once a tenant carries
one that record is the sole authority: the tenant stays maintained
unconditionally, and no CLI flag -- not `--tenant-token`, not
`--maintain-tenant` -- can exclude it from fold or maintenance today. This is
what makes removing a token (or dropping a `--tenant-token`/`--maintain-tenant`
entry on restart) never silently disable a config-recorded tenant's retention.
The `discovered`/`maintained`/`excluded` counting still applies to the
no-config-record case: an excluded no-config tenant shows up as the
`tenants_discovered` minus `tenants_maintained` gap on `/metrics` and in a
logged line, never a silent drop. A discovery failure (the LIST itself erroring) skips the
whole cycle -- no tenant's tick runs -- and is retried next cycle; it never
falls back to an empty tenant set, since that would look identical to a
healthy "nothing to do" on the very dashboard meant to catch this failure
mode (see the `/metrics` gauges below). ADR-0048 deliberately rejected a
durable tenant-registry object as an alternative: a second source of truth
for the tenant set could itself drift from the prefixes actually holding
data, the same config-asserts-reality bug class this derivation removes.

Every mode, maintain included, serves two health routes on `--listen-http`
(ADR-0034 decision 4). `/healthz` (liveness) returns 200 whenever the HTTP
listener is serving, so a routed 200 proves the axum event loop is alive; it is
deliberately independent of store reachability, so a store outage never makes
liveness fail and get healthy processes killed and restarted. `/readyz`
(readiness) returns 503 until startup has fully completed (config parsed, the
object-store capability gate passed, both listeners bound), and thereafter is
the AND of that startup latch and a background store-reachability probe
(ADR-0050 section 7): one probe per process GETs the fixed `sys/tenancy` object
every `--store-probe-interval` (default 30s, jittered), and after four
consecutive failures readiness flips to 503, recovering on the first success
(asymmetric hysteresis). `/readyz` still performs no object-store call on the
probe path itself -- the kubelet reads an in-memory atomic the probe maintains
-- which keeps the two objections the original design documented (kubelet-
frequency S3 cost, single-blip mass ejection) answered rather than overridden.
The probe also exports `ravel_store_reachable` (gauge) and
`ravel_store_probe_failures_total` (counter) at `/metrics`, with a default
alert rule (docs/guides/operations.md). In maintain mode these three routes are
the entire HTTP surface: liveness there means the routes answer, not merely that
a TCP connection is accepted.

Startup is also gated on the store backend being qualified (ADR-0050 section 6):
on any non-`memory` store, every mode reads the durable `sys/qualification`
record before binding a listener and refuses to start if it is absent or its
suite version is below the binary's floor. A fresh production deployment must run
`ravel-cli store qualify` first; this is deliberate, not a bootstrap-and-continue
path (docs/guides/operations.md).

Every mode also serves `GET /metrics` on `--listen-http` (ADR-0044 section 4):
a hand-written Prometheus text exposition of counters Ravel
already computes (object-store calls/errors/bytes/latency, ingest flush and
ack counters by signal, catalog anomaly counters), rendered by
`services/ravel-server/src/metrics.rs` rather than pulled from the
`prometheus` crate, so the label set stays exactly what Ravel decides. Labels
are restricted to a fixed, exhaustively-matched set (`tenant_hash`, `signal`,
`mode`, `op`, `error_kind`, `workload_class`, `level`); `shard` is deliberately
excluded because Ravel's own telemetry must not be able to explode. Like
`/readyz`, this endpoint performs no object-store call: every sample comes
from an in-memory counter already held by a running process.

`--mode maintain` additionally renders three tenant-discovery samples
(ADR-0048 decision 3) through this same renderer, no second
registry: the gauges `ravel_maintain_tenants_discovered` and
`ravel_maintain_tenants_maintained` (updated once per discovery cycle,
holding their last known-good value across a failed one), and the counter
`ravel_maintain_tenant_discovery_failures_total`. The condition these exist to
alarm on is a prefix that holds data but receives no maintenance:
`tenants_maintained` staying below `tenants_discovered` with no corresponding
flag restriction configured, or `tenants_maintained` at zero while
`tenants_discovered` is not, in a mode that should be maintaining.

`--mode maintain` also renders four maintenance-safety samples through
the same renderer (ADR-0048 decisions 4 and 6):
`ravel_maintain_legal_hold_refresh_failures_total`,
`ravel_maintain_conservation_aborts_total` and
`ravel_maintain_orphan_breaker_tripped_total` (both labeled by
`signal`), and the gauge `ravel_maintain_orphans_withheld` (also
labeled by `signal`). These use only the existing `mode` and `signal`
labels; ADR-0048 names `tenant_hash` on the orphan-breaker-trip counter,
but ADR-0044's per-tenant label ban on this unauthenticated route holds
for these families regardless: `--metrics-tenant-labels` (ADR-0051
section 6) only affects the admission usage family, not the
maintenance-safety family here. See docs/guides/operations.md for the
default alert rules and the breaker runbook.

Remote Write (ADR-0015) reuses this same gateway/router/shard pipeline: RW1
and RW2 payloads decode and normalize to the same `NormalizedPoint` shape
OTLP produces, in `ravel-remote-write`, then flow through the identical
`IngestRouter::write` call OTLP uses, strict-mode only. No new crash-matrix
failure point, since no new flush/commit path was added.

Signals other than metrics, compaction, catalog snapshots, RavelQL,
Sigma/OCSF: later phases. See the spec docs as they land.

## Listener topology

`ravel-server` binds up to three listeners:

- `--listen-http`: OTLP HTTP-protobuf, Remote Write, `/api/v1/*` query and
  analytics routes, `/api/v1/sql` (feature `sql`), and the mode-independent
  `/healthz`, `/readyz`, `/metrics` routes.
- `--listen-grpc`: OTLP gRPC and, under the `flight-sql` feature, Flight SQL.
  Bound only in the modes and feature combinations that serve one of those.
  Under `--distributed-query` it also carries the cluster-internal
  `SeriesFetch` fragment service (ADR-0071, below). That service is bound
  here and nowhere else: never on `--listen-http`, never on the mTLS
  listener. It is also where a worker advertises itself, so a query node with
  no gRPC listener never joins the worker set and runs every slice itself.
- `--mtls-listener` (ADR-0050 section 1): a third listener,
  required by and only meaningful together with `--mtls-enabled`. It serves
  the same ingest and query surface as `--listen-http`, but its router chain
  is built with the `MtlsResolver` in place of whatever resolver backs the
  other two. The chains built for `--listen-http` and `--listen-grpc` never
  contain the `MtlsResolver` at all -- structurally, not just inertly -- so
  the `x-ravel-client-cert-cn` header it trusts has no effect there
  regardless of what a proxy in front of them does or does not strip.

`Cli::validate` refuses to start (typed error, not a warning) on any
configuration where this isolation would not hold: `--mtls-enabled` without
`--mtls-listener`, `--mtls-listener` without `--mtls-enabled`, or
`--mtls-listener` colliding with `--listen-http`/`--listen-grpc` (including
when `--dev-insecure-tenant-header` is also on `--listen-http`). The operator
contract is that only a TLS-terminating, header-stripping proxy is network-
reachable on the mTLS listener's address; the public listeners are safe
against header forgery by construction, independent of that proxy hygiene.

## SQL query path (in progress)

`ravel-sql` (ADR-0013) adds a second query
path alongside PromQL: `RsegScanExec -> SortPreservingMergeExec ->
RsegDedupExec`, a DataFusion physical pipeline over the same segments
PromQL reads, deduplicating cross-segment duplicate samples with the exact
same total order as `is_greater` in `ravel-query` (bit-for-bit, including
the `value.to_bits()` tiebreak). Arrow and DataFusion stay isolated to this
crate; PromQL numerics never lower to SQL, and vice versa.

Status: the scan/merge/dedup pipeline and predicate/projection pushdown
under a pruning-soundness invariant (pruning may only ever widen the read
set, never narrow it) are implemented and tested against an independent
oracle. Not yet built: the HTTP endpoint (`POST /api/v1/sql`, feature
`sql`) and Flight SQL (feature `flight-sql`) -- both follow-up work, gated
behind cargo features so the default build stays free of Arrow and DataFusion
outside `ravel-otap`.

## Distributed reads and cross-cluster federation (ADR-0071)

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

A read can span more than one process. This is off by default and cost-gated:
the query node that receives a request coordinates it, resolves one pinned
snapshot, and only fans out when a pre-execution estimate clears the gate
(256 MiB of estimated store bytes or 256 segments), so a cheap query runs the
untouched local path. Intra-cluster worker membership needs no new durable
state — workers self-register with a heartbeat key and are selected by
rendezvous hashing — and the full slice-partition, budget-re-enforcement, and
fault matrix live in docs/query-engine.md. For the operator view (flags, the
token file, the registry, what a client sees on partial coverage) see
docs/guides/distributed-query.md.

The distributed role is not a new process type or a new mode. A query-serving
process (`--mode all` or `--mode query`) started with `--distributed-query`
takes on two jobs at once, and every such process in a cluster is identical:

- **Coordinator**, for the requests it receives. It resolves the snapshot,
  applies the cost gate, partitions, dispatches, re-enforces the query's
  budgets over the folded per-slice accounting, k-way merges, evaluates, and
  renders `stats.fragments[]`.
- **Worker**, for its peers. The `FragmentService` on its cluster-internal
  gRPC listener serves inbound slices, guarding each with a constant-time
  check of the shared `--fragment-auth-token-file` bearer token and admitting
  it against `FragmentAdmission`, a workload class distinct from client-query
  admission (`--max-inflight-fragments`, default 32). That separation is what
  makes it impossible for a coordinator holding a client-query permit to
  deadlock waiting on fragments that would need the same pool. A slice a
  coordinator owns by rendezvous runs through this same service in-process,
  with no network hop, so local and remote slices cannot diverge in behavior.

Two topologies share the one gRPC
`SeriesFetch` service and one `SliceFetcher` seam the merge layer holds, so
the coordinator's k-way merge cannot tell a remote slice from a local one:

- **Intra-cluster slices** (`Scope::Pinned`): the coordinator hands a worker
  in its own cluster a short-lived fragment token and the already-resolved
  `tenant_hash`, which the worker trusts and uses directly.
- **Cross-cluster federation** (`Scope::Resolve`): each remote is a separate
  trust domain configured with one repeatable `--remote-cluster` spec
  (`name`, `endpoint`, and `credential-file` required; `tls`, `tls-ca-file`,
  `skip-unavailable`, and a per-remote `soft-timeout` optional), with
  `--remote-cluster-soft-timeout` setting the default bound. Every spec is
  validated at startup, so a malformed field, a duplicate name, or an
  unreadable credential file fails the process rather than the first
  federated query. The coordinator presents an
  ordinary per-remote tenant credential; the remote resolves the tenant from
  its own `TenantResolver` and ignores the wire `tenant_hash`, so a federated
  request can reach exactly the tenants that credential authorizes there and
  no more. A remote rejects the intra-cluster fragment token outright.

A slow or unreachable remote degrades to partial coverage rather than
failing the whole query (`skip_unavailable`, per-remote soft timeout). That
partial state is always surfaced in the query's stats (`partial: true`) and
warnings, naming only the operator-facing cluster; internal endpoints and
errnos are redacted. Federation assumes disjoint series identity across
clusters (region- or tenant-sharded); the design and its cross-cluster
duplicate tie-break limitation are specified in docs/query-engine.md.

The Flight SQL distributed scan (feature `flight-sql`) derives its ticket
MAC key from the shared cluster secret via a domain-separated BLAKE3 KDF, so
coordinator and worker validate tickets under the same key without a
separate key-distribution channel.

## Analytics stage

`ravel-analytics` (ADR-0028, docs/analytics.md) is a post-evaluation stage of
pure per-series functions over `(timestamp_ns, f64)` slices: change point
detection (PELT with a BIC penalty) and robust summary statistics. It touches
no frozen contract -- no parser fork, no proto or format change -- and depends
on `ravel-types` only, keeping change-point detection out of the aggregation
layer. `ravel-server` exposes it at `POST /api/v1/analytics`:
the endpoint runs the same range evaluation `/api/v1/query_range` runs, then
applies the requested op to each series of the matrix, capping a call at 1000
series and each `change_point` series at 2000 points (approximation via
`downsample` is opt-in and visible). Unlike the SQL path it needs no cargo
feature, since it links no Arrow or DataFusion.

## Alert evaluation

`ravel-alerting` (ADR-0043) holds the rule shape, the condition test, the alert
state machine, and the `Signal::Alerts` record encoding as pure logic. The
driver lives in `ravel-server` (`alerting.rs`, `alert_sink.rs`): one background
tokio task per tenant, shaped exactly like the maintenance task
(`spawn`/`run_loop`, a jittered `--alert-eval-interval-secs`, a `oneshot`
shutdown per task). It runs in the modes that build a query engine, `--mode
all` and `--mode query`, because a rule is a query and it evaluates rules
against the very `QueryEngine` and `SqlExecutor` instances `/api/v1/query` and
`/api/v1/sql` serve from, in process.

Rules are static per-tenant config loaded once at startup from the JSON file
`--alert-rules-file` names; a rules-management API is deferred (ADR-0043
decision 2). Each tick folds the tenant's durable alert history to the latest
record per `alert_id`, evaluates every rule, and writes a record only on a
state transition -- pending, firing, resolved -- never per tick. No alert state
is held in process memory, so a restarted evaluator resumes from the records.

An alert record is an ordinary RLOG object under the `a` keyspace, published
with the same data-PUT-then-commit-PUT protocol as any other signal
(create-if-absent, CRC32C upload checksum), which is what makes an abandoned
write invisible: the fold reads commit records, never data objects directly.

Two notification sinks ship: a webhook (the transition as JSON) and an
Alertmanager-compatible sink (`POST /api/v2/alerts` in Alertmanager's own
payload shape). Both fire only after the record is durable, and a sink failure
is logged and retried on a later tick from the latest record -- delivery is
at-least-once and never blocks, delays, or alters the write (ADR-0043 decision
6).
