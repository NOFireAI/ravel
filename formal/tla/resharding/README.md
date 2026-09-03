# Generation-versioned online resharding (TLA+ model)

This directory holds a TLA+ model of Ravel's online resharding protocol: the
append-only generation history in the provisioning object, wall-clock routing
with lazy refresh, flush-pinned ingest hours, the read-side scan slack, the
folder HEAD ceiling, the reader validation fence, and commit-token resolution
across a reshard. It exists to check that the margins these pieces carry
compose into the safety properties ADR-1113 relies on, and to pin each margin
with a negative control that removes it and shows the corresponding property
break.

`OnlineResharding.tla` is the protocol. `MCOnlineResharding.tla` is the
model-checking harness: it fixes the symmetry set and names the spec TLC runs.
`RavelObjectStore` (under `formal/tla/common`) supplies the CAS-append and
list semantics the protocol builds on.

## What the model is, and is not

The model is an abstraction over shard counts and hours. A "write" is a record
carrying the shard index it routed to, the ingest hour its flush pinned, and
the route hour it was admitted at; it is not bytes, a segment, or a protobuf.
Time is discrete hours: the shipped 60-second refresh interval rounds up to
`C = 1` hour, which is the single most consequential modeling choice here (see
Assumptions). Clocks are per-actor and advance together for routers; skew is a
bounded offset on the appender's clock relative to the routers'.

The model mirrors the pure, deterministic functions of the protocol: the
active-count and scan-count arithmetic, the lead and grace horizons, the HEAD
acceptability predicate, and exact-key token resolution. It does not model the
object-store wire format, ret/backoff timing, segment contents, or the query
engine below the scan set. The traceability table
(`traceability.md`) ties each modeled action and property to the Rust symbol
that implements the same rule.

## Variable-to-Ravel mapping

| Model variable | Ravel counterpart |
|---|---|
| `store`, `versionCounter`, `listState` (from `RavelObjectStore`) | the S3-compatible object store: the provisioning object, HEAD, and per-writer commit keys, with CAS on version |
| `clocks` | per-actor wall clocks (writers, the appender/operator, the reader/folder), in hours |
| `views` | each writer's cached generation view and the hour it was last refreshed (`route_cached`) |
| `flushes` | each writer's open flush: the ingest hour pinned at open and its admit counter |
| `admitted` | records that reached durable storage, each with shard index, ingest hour, route hour |
| `reqs` | in-flight reshard requests at the operator (CAS-append progress) |
| `rview` | the reader's cached generation view |
| `casWins` | successful appends, keyed by base version, to check a single CAS winner |
| `lastOp` | the caller-visible outcome of the last step (admit/reject/resolve), the witness the invariants read |

## Constants and their shipped counterparts

| Constant | Shipped counterpart |
|---|---|
| `C` | refresh interval (`DEFAULT_REFRESH_INTERVAL_NS`), rounded up to whole hours |
| `MinLeadHours` | `min_lead_hours(C) = ceil(C) + 1`, the `MIN_LEAD_HOURS` the CLI enforces |
| `L` | the activation lead a reshard request asks for |
| `S` | read-side scan slack (`DEFAULT_SCAN_SLACK_HOURS`), which already folds in the tolerated clock skew |
| `FlushBound` | trailing-admission window (`FLUSH_BOUND_SLACK_HOURS`) |
| `AppenderSkew` | appender-to-router clock offset, checked against `TOLERATED_CLOCK_SKEW_HOURS` |
| `CasAttempts` | `RESHARD_CAS_ATTEMPTS` |
| `InitialShardCount`, `TargetCounts` | generation 0's count and the counts a reshard may target |
| `WriterFenceEnabled` | negative switch (correct value TRUE): fail closed on a view past grace |
| `TokenValidatedAgainstCount` | negative switch (correct value FALSE): token resolution is an exact-key GET, not a count check |

## Assumptions

- **The `C = 1` rounding.** The model's time unit is an hour, so the 60-second
  refresh interval becomes `C = 1` hour, inflating cached-view staleness about
  sixtyfold. `MinLeadHours = C + 1 = 2` then equals `MIN_LEAD_HOURS`, but at
  this rounding the lead, the tolerated skew, and the staleness collapse onto
  the same scale, where the shipped system has an hour of margin between them.
  Consequences run through the whole model: safety at the tolerated skew is a
  boundary case here rather than a comfortable one (see results.md), and the
  smoke run keeps `AppenderSkew = 0` because carrying both the refresh rounding
  and a skew rounding at once would model a system that does not exist.
- Clocks advance monotonically; router clocks advance together (`WriterSkew`
  bounds any residual difference, set to 0 in the runnable configs).
- The store is linearizable with CAS on version, as `RavelObjectStore` models.
- Bounds (`MaxHour`, `MaxGenerations`, `MaxAdmitsPerWriter`, `CasAttempts`) are
  finite model bounds, not protocol limits.

## Out of scope

- Object-store wire format, segment/RSEG contents, and encode/decode paths.
- Retry/backoff wall-clock timing (modeled as bounded attempt counts).
- The query engine below the scan set, and fold internals beyond the HEAD
  ceiling stamp.
- Multi-tenant interference: the model is single-tenant.

## Running the checks

```sh
scripts/check-tla.sh smoke        -a resharding
scripts/check-tla.sh negative     -a resharding
scripts/check-tla.sh traceability -a resharding
```

The smoke run is a symmetry-reduced safety check sized to finish in well under
a minute. The negative run confirms each control in `negative/` breaks the
property its `.expect` names. `exhaustive.cfg` is written for the orchestrator
and is deliberately not run by the executor; see results.md.

## What a green run does and does not claim

A passing smoke or exhaustive run means TLC checked this finite model, under
the bounds and assumptions above, and found no reachable state violating the
listed invariants. It is a check of a finite model, not a proof of the running
system: it does not verify the Rust implementation, and it does not establish
safety outside the modeled bounds. Statements derived from it, including the
ADR-1113 D12 claim, should be phrased as "TLC checked this finite model under
these bounds and assumptions," never as "verified" or "proven."
