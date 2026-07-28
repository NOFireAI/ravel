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
  <- ravel-sql          (query, catalog, types; arrow + datafusion, in
                          progress -- see below)
  <- services/ravel-server, services/ravel-cli
ravel-test-util (types, object-store) used by all dev-deps
```

Phase 1 runs every service as modes of one binary (`ravel-server --mode
all|gateway|query`). Crate boundaries keep the split honest so later phases
can deploy them separately.

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
