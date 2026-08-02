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
tenant over every shard (docs/compaction-retention-plan.md P8). It requires
a `multipart`-capable object store (compaction is the only writer of
multipart objects) and still binds `--listen-http`, serving the `/healthz`,
`/readyz`, and `/metrics` routes only (no ingest or query surface).

Every mode, maintain included, serves two health routes on `--listen-http`
(ADR-0034 decision 4). `/healthz` (liveness) returns 200 whenever the HTTP
listener is serving, so a routed 200 proves the axum event loop is alive.
`/readyz` (readiness) returns 503 until startup has fully completed (config
parsed, the object-store capability gate passed, both listeners bound) and
200 thereafter; it performs no object-store call per probe, deliberately, to
avoid adding S3 cost on every kubelet probe and to avoid a transient S3 blip
ejecting every pod from its Service at once. In maintain mode these three
routes are the entire HTTP surface: liveness there means the routes answer,
not merely that a TCP connection is accepted.

Every mode also serves `GET /metrics` on `--listen-http` (ADR-0044 section 4,
issue #423): a hand-written Prometheus text exposition of counters Ravel
already computes (object-store calls/errors/bytes/latency, ingest flush and
ack counters by signal, catalog anomaly counters), rendered by
`services/ravel-server/src/metrics.rs` rather than pulled from the
`prometheus` crate, so the label set stays exactly what Ravel decides. Labels
are restricted to a fixed, exhaustively-matched set (`tenant_hash`, `signal`,
`mode`, `op`, `error_kind`, `workload_class`, `level`); `shard` is deliberately
excluded because Ravel's own telemetry must not be able to explode. Like
`/readyz`, this endpoint performs no object-store call: every sample comes
from an in-memory counter already held by a running process.

Remote Write (ADR-0015) reuses this same gateway/router/shard pipeline: RW1
and RW2 payloads decode and normalize to the same `NormalizedPoint` shape
OTLP produces, in `ravel-remote-write`, then flow through the identical
`IngestRouter::write` call OTLP uses, strict-mode only. No new crash-matrix
failure point, since no new flush/commit path was added.

Signals other than metrics, compaction, catalog snapshots, RavelQL,
Sigma/OCSF: later phases. See the spec docs as they land.

## SQL query path (in progress)

`ravel-sql` (ADR-0013, docs/arrow-datafusion-plan.md) adds a second query
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
`sql`) and Flight SQL (feature `flight-sql`) -- both later tickets in the
same epic, gated behind cargo features so the default build stays free of
Arrow and DataFusion outside `ravel-otap`.

## Analytics stage

`ravel-analytics` (ADR-0028, docs/analytics.md) is a post-evaluation stage of
pure per-series functions over `(timestamp_ns, f64)` slices: change point
detection (PELT with a BIC penalty) and robust summary statistics. It touches
no frozen contract -- no parser fork, no proto or format change -- and depends
on `ravel-types` only, mirroring Elastic's placement of `CHANGE_POINT` outside
the aggregation layer. `ravel-server` exposes it at `POST /api/v1/analytics`:
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
