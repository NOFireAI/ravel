# Traceability index

One row per area, indexing into that area's own `traceability.md`. Row
counts are exact data-row counts (header and separator excluded).
`scripts/check-tla.sh traceability` resolves every Rust path and symbol
named in every area table and fails when one no longer exists.

| Area | Specification | Rows | Rust crates covered | Table |
|---|---|---|---|---|
| common | `common/RavelObjectStore.tla` | 9 | ravel-object-store, ravel-commit, ravel-ingest | [common/traceability.md](common/traceability.md) |
| commit | `commit/CommitProtocol.tla` | 21 | ravel-ingest, ravel-commit, ravel-catalog, ravel-maintain, ravel-types | [commit/traceability.md](commit/traceability.md) |
| catalog | `catalog/CatalogMVCC.tla` | 25 | ravel-catalog, ravel-commit, ravel-maintain, ravel-query | [catalog/traceability.md](catalog/traceability.md) |
| lifecycle | `lifecycle/LifecycleGC.tla` | 18 | ravel-maintain, ravel-commit, ravel-query, ravel-catalog | [lifecycle/traceability.md](lifecycle/traceability.md) |
| resharding | `resharding/OnlineResharding.tla` | 19 | ravel-ingest, ravel-catalog, ravel-commit, ravel-types, services/ravel-cli | [resharding/traceability.md](resharding/traceability.md) |
| maintenance | `maintenance/MaintenanceOwnership.tla`, `maintenance/CompactionClaims.tla` | 45 | ravel-fleet, ravel-maintain, services/ravel-cli, services/ravel-server | [maintenance/traceability.md](maintenance/traceability.md) |

Total: 137 traceability rows across the suite.

## Test debt

Every row still lacking a test, gathered from each area's `traceability.md`
and tagged by area, the Rust path the row cites, and the reason no test
covers it. Wave R6 (crates/ravel-object-store, ravel-failure-tests,
ravel-catalog, ravel-maintain, ravel-ingest, ravel-fleet, services/ravel-cli
regression tests) closed every other row that previously appeared here.

### lifecycle (2 rows, production gaps)

- `RequestErasure` — `crates/ravel-commit/src/keys.rs::erasure_request_key::DREQ_SUFFIX`
  — no production symbol PUTs the request marker; only the key builder and
  tests exist
- `CompleteErasure / CompletionImpliesNoPreRewriteExposure` —
  `crates/ravel-maintain/src/erasure_rewrite.rs::bucket_erasure_completion::bucket_serves_subject`
  — the gate is computed but no production symbol writes the completion
  object

### maintenance (3 rows, proposal with no shipped caller)

- `GuardedPublish` — `crates/ravel-fleet/src/claim.rs::renew` — no
  `ClaimGuard` type exists; the guard is a caller convention over `renew`'s
  outcome, and no code outside ravel-fleet calls the claim primitive
  today (ADR-1029 is a proposed design). Pinned at the primitive by
  `claim_guard_abandons_publish_when_the_claim_is_lost`; a production test
  follows the first shipped caller
- `AbandonPublish` — `crates/ravel-fleet/src/claim.rs::renew` — same
  reason as `GuardedPublish`
- `LostClaimNeverPublishesThroughGuardedPath` —
  `crates/ravel-fleet/src/claim.rs::renew` — same reason as `GuardedPublish`

Total test-debt rows: 5 (lifecycle 2, maintenance 3).
