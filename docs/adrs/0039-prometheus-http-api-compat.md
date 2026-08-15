# ADR-0039: Prometheus HTTP API compatibility surface for Grafana

Status: Accepted

## Context

Ravel's PromQL query surface is further along than README.md
currently claims. A codebase survey found:

- Aggregation operators (`sum`, `avg`, `min`, `max`, `count`, `group`,
  `stddev`, `stdvar`, `topk`, `bottomk`, `quantile`, `count_values`) are
  fully implemented in `crates/ravel-promql/src/aggregate.rs` and scored
  as supported in `docs/query-engine.md`.
- Subqueries work for float series (`crates/ravel-promql/src/eval.rs`,
  `plan.rs`, `functions/mod.rs`). The one gap is a subquery whose matched
  series carry native histogram data, which is rejected with a typed
  `Error::Unsupported` (422) rather than silently dropping data
  (`eval.rs:939-960`) - a real but narrow gap.
- Remote Write 1.0/2.0 ingest is implemented (`crates/ravel-remote-write`)
  and mounted at `POST /api/v1/write` (`services/ravel-server/src/
  remote_write.rs`), with admission limits at parity with the OTLP path.

None of this is a decision this ADR needs to make; it is a documentation
currency bug (README/PROGRESS say "planned" or "does not work yet" for
behavior that has shipped), fixed per CLAUDE.md's doc-currency rule.

What remains a genuine gap, and what this ADR is actually about: Grafana's
built-in Prometheus datasource, on "Save & Test" and periodically after,
calls `GET /api/v1/status/buildinfo` to detect which Prometheus-API
flavor it is talking to (vanilla Prometheus vs Mimir/Thanos/Cortex, which
answer differently or not at all). Ravel has no such route today
(confirmed by grep: `services/ravel-server/src/health.rs` implements only
the Kubernetes-convention `/healthz`/`/readyz`, nothing under `/api/v1/
status/*` or Prometheus's own `/-/healthy`/`/-/ready` paths). Some
dashboards and the Explore UI's metric-type hints also call
`/api/v1/metadata`, which does not exist either.

## Decision

Add a small, honestly-labeled Prometheus HTTP API compatibility surface,
additive only, no existing route changed:

1. `GET /api/v1/status/buildinfo`: Prometheus's JSON response shape
   (`{"status":"success","data":{"version","revision","branch",
   "buildUser","buildDate","goVersion"}}`) populated with Ravel's own
   build metadata (crate version, git SHA if available at build time).
   This is a wire-compatibility shim so Grafana's flavor probe succeeds,
   not a claim that Ravel is Prometheus; the version string is Ravel's
   own, never a spoofed Prometheus version number.
2. `GET /api/v1/metadata`: Prometheus's response shape
   (`{"status":"success","data":{<metric_name>: [{"type","help","unit"}]}}`).
   Ravel does not track OTLP metric type/help/unit metadata anywhere
   today (no storage for it, no admission-time capture). Returning an
   empty `data` object is a valid, honest answer under Prometheus's own
   contract (an empty result is not an error) rather than inventing
   metadata Ravel doesn't have. A future epic can populate it for real if
   OTLP metric descriptors get captured; that is out of scope here.
3. `GET /-/healthy` and `GET /-/ready`: thin aliases over the existing
   `/healthz`/`/readyz` handlers (`services/ravel-server/src/health.rs`).
   No new health logic. These are Prometheus's own path convention,
   distinct from but not a replacement for the Kubernetes-convention
   routes the operator already depends on (docs/guides/kubernetes.md);
   both stay mounted.
4. Real-Grafana verification: point an actual Grafana instance's
   Prometheus datasource at a running `ravel-server`, confirm "Save &
   Test" succeeds, and that Explore / a basic dashboard panel render a
   query. This is acceptance evidence, not a new code surface.
5. Fix README.md to state the true current status of aggregations,
   subqueries, and Remote Write (done, not planned).

## Rejected alternatives

- **Do nothing; document that Grafana needs a generic (non-Prometheus)
  datasource plugin instead.** Rejected: it silently breaks the exact
  onboarding path ("point your existing Grafana at Ravel") that is this
  epic's whole point, for the cost of four small additive routes.
- **Fully spoof `buildinfo`'s `goVersion` and `version` fields to look
  like a real Prometheus release**, so version-gated Grafana features
  assume full parity. Rejected: dishonest, and brittle the moment a
  Grafana feature checks a real Prometheus version constraint Ravel
  doesn't actually meet (e.g. a PromQL construct still gated behind a
  current or future gap). Honest version string, real supported
  surface.
- **Populate `/api/v1/metadata` with inferred placeholder type/help
  strings** (e.g. guessing `counter` vs `gauge` from naming convention).
  Rejected by the "exact semantics by default, approximation is opt-in
  and visible" invariant: a guessed type presented as real metadata is
  worse than an honest empty response.

## Consequences

- No frozen format touched (no RSEG/proto/key-layout change); this ADR
  exists to record the compatibility-shim boundary (what Ravel claims to
  be vs what it wire-compatibly answers), not because a persistent format
  changed.
- `/api/v1/metadata` returning empty is a known, visible limitation until
  a later epic captures real OTLP metric descriptors; not silently
  degraded, documented in docs/query-engine.md.
- The native-histogram subquery gap stays open, tracked separately, and
  does not block the "Grafana works day one" acceptance bar.
