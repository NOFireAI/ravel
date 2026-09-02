# Ravel diagrams

Every diagram under `docs/diagrams/` is a hand-authored SVG, embedded in one
page with an image link so it renders wherever the markdown does. This page
says what each one shows and which page it illustrates, so a diagram is
never reachable from nowhere and a page that changes can find the drawing
that has to change with it.

## The visual language

The diagrams share one style, so a reader who has learned one can read the
rest:

- Mid blue (`#a9c9e6` fill, `#1f4e79` stroke) for services, actors, and
  anything that runs as a process.
- Amber (`#f4c869` fill, `#8a5a10` stroke) for immutable objects in the
  object store and for the steps that write them.
- Green (`#a9dcae` fill, `#2f6b34` stroke) for checksum-verified regions.
- Lavender for `tracing` spans.
- Neutral grey (`#eceef1` or `#dde1e7` fill, `#383e46` stroke) for
  everything else, and white for plain boxes.
- Dashed strokes for optional or conditional paths; solid strokes for the
  paths every request takes.
- Text in the page's own sans-serif at 12 to 13 px, in near-black ink
  (`#17191c`), with a muted grey (`#4a4f57`) for captions.

Every file declares a `viewBox` so it scales, `role="img"` with an
`aria-label` that states what the drawing shows, embeds no raster image, and
references nothing outside itself. The documentation gate checks all four,
and that at least one page links each file. A diagram a user page embeds is
read as part of that page, so its text and its label carry no
decision-record citation or issue number; the gate checks that too, and a
diagram only a decision record embeds may name the record.

## architecture.svg

The whole system on one page: OTLP and OTAP clients ingest through the
gateway, the ingest router, and the shard actors down to immutable segments
and commit records in object storage, and Prometheus API consumers read back
through the PromQL evaluator, query workers, and catalog resolution. The
object store sits in the middle as the single durable center; every box above
it is disposable.

Illustrates: README.md "How it fits together".

## architecture-write-path.svg

The write path: clients through the gateway (authentication, tenant
resolution, admission), the ingest router's shard hash, the single-threaded
shard actors building one immutable columnar segment per flush, the two-PUT
publish (data object, then commit record), and the strict acknowledgement
that answers only after both are durable.

Illustrates: docs/architecture.md "How ingest, query, and maintenance
interact".

## architecture-read-path.svg

Commit records fold into snapshot parts and a HEAD published by
compare-and-swap; a query pins one snapshot with a single GET, prunes with the
snapshot's bounds, skip indexes, bloom filters, and exact column statistics,
fetches by ranged or whole-object GETs through the read cache, and evaluates
through PromQL or SQL.

Illustrates: docs/architecture.md "How ingest, query, and maintenance
interact".

## architecture-cluster-topology.svg

Symmetric query nodes over one shared bucket, heartbeat-registered workers
placed by rendezvous hashing, and remote clusters as separate trust domains
reached only through their own API.

Illustrates: docs/architecture.md "What happens when processes run
concurrently".

## ingest-commit-sequence.svg

A vertical sequence diagram of one strict-mode flush: pin the flush identity,
PUT the data object create-if-absent with a checksum, PUT the commit record
create-if-absent with an idempotency check, then acknowledge with a commit
token. The three crash points from the crash matrix are marked with what each
one leaves behind: nothing stored, an invisible orphan, or a visible segment
where a client retry stores a duplicate.

Illustrates: docs/guides/ingest.md "Commit tokens and read-your-write";
the crash matrix is in docs/consistency-model.md.

## rseg-layout.svg

The byte layout of an RSEG v7 object: the label dictionary and series ids,
the series-metadata catalog (or its sparse index and chunked form above the
4096-series threshold), the timestamp, value, and histogram page containers
with per-page headers, the protobuf footer, and the 16-byte trailer, with
brackets showing what each checksum covers.

Illustrates: docs/segment-format.md; docs/guides/inspecting-data.md
"segment inspect".

## query-path.svg

The query path from an incoming PromQL request to the JSON response: catalog
snapshot resolution (the listing window plus exact-key token reads), a suffix
GET of each segment's footer, series pruning through the series metadata and
label dictionary, range coalescing into a few GETs, page decode,
cross-segment deduplication, the PromQL evaluator, and the Prometheus JSON
envelope.

Illustrates: docs/guides/query.md.

## tenancy-key-layout.svg

The object-store key tree under `t/<tenant_hash>/`: the metrics, logs, and
spans signal prefixes with their L0 data keys and commit keys, and the catalog
snapshot and HEAD objects the fold writes. Each key's components are broken
down: writer id, epoch, sequence, and the hash for data keys; the ingest-hour
bucket for commit keys.

Illustrates: docs/guides/inspecting-data.md "Key layout"; the normative
layout is docs/catalog-and-mvcc.md.

## tracing-export.svg

Two panels. The first nests the six query-path phase spans inside the
request-level span, with each span's recorded fields and level, including the
field set the logs-signal `page_fetch` and `decode` spans carry instead of the
metric path's. The second shows the subscriber: one `EnvFilter` gating both
the always-on `fmt` layer and the off-by-default OpenTelemetry layer through
a batch span processor to an OTLP gRPC collector, plus the two export-failure
modes.

Illustrates: docs/guides/tracing.md.

## erasure-lifecycle.svg

Selective subject erasure end to end: an erase request excludes matching
records from queries immediately, a maintain rewrite drops the subject and
publishes a `.done` record, then a horizon- and hold-gated sweep physically
deletes the superseded objects, reaching absence in under four days on the
defaults.

Illustrates: docs/deletion-and-gc.md "Selective subject erasure".

## ingest-plausibility-window.svg

The ingest-timestamp plausibility window: the accept region between
`now - max_ingest_lag` and `now + max_future_skew` on the event-time axis,
the `TooOld` and `FutureSkew` reject zones, the per-signal bounded timestamps
(metric sample time, log record time, span end), and a long-running span
admitted because only its end is bounded.

Illustrates: docs/adrs/0051-tenant-admission-control.md; the rule it draws
is stated in docs/consistency-model.md "Late and skewed data" and
docs/guides/admission-limits.md "Event-time skew".

## query-request-budget.svg

Why a flat per-query request cap is dimensionally wrong: one cold query over
a tenant's open hour fetches every shard's segments, so cost scales with
shard count while a flat cap does not, and the default is derived from the
shard count instead.

Illustrates: docs/adrs/0075-shard-aware-query-request-budget.md.

## request-cost-levers.svg

Where the object-store request bill comes from and which levers reach it:
PUTs per day scale with tenants, signals, shards, replicas, and flush cadence;
replica affinity, shard count, log and span pipelining, and flush cadence are
the levers, and the commit protocol stays.

Illustrates: docs/adrs/0076-reducing-s3-request-cost.md; the operator's
view of the same levers is docs/guides/cost-model.md.

## distributed-query-lifecycle.svg

The lifecycle of one distributed query: the request, the single pinned
snapshot, the cost gate, shard-major slices dispatched to rendezvous-mapped
workers or run coordinator-local, the k-way merge under one total order, the
unchanged evaluator, and the response with one `stats.fragments[]` entry per
slice, with the cross-cluster federation path alongside.

Illustrates: docs/guides/distributed-query.md "The lifecycle of a
distributed query".

## distributed-query-failure.svg

The failure flow: an intra-cluster slice is re-dispatched once, then run on
the coordinator, then failed typed, never merged partially; corruption is
terminal; and the cross-cluster path is the only source of partial coverage,
under `skip-unavailable`.

Illustrates: docs/guides/distributed-query.md "Failure behavior".

## k8s-operator-reconcile.svg

The operator reconcile loop: one `RavelCluster` resource reconciles into the
gateway, query, and maintain Deployments and their Services.

Illustrates: docs/guides/kubernetes.md; docs/adrs/0034-k8s-operator.md.

## k8s-dev-environment.svg

The kind development environment: the ordered steps `scripts/kind-up.sh`
runs to create a cluster, build and load the container images, and bring up
Ravel.

Illustrates: docs/guides/kubernetes.md "The kind development environment".

## k8s-ci-integration.svg

The `k8s-integration` CI job against the local environment: the two paths
build container images differently and converge on identical `kind-up.sh`,
`kind-demo.sh`, and `kind-down.sh` calls.

Illustrates: docs/guides/kubernetes.md "The same environment in CI".
