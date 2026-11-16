# Antithesis workloads

Each directory here is one Antithesis workload: its compose config, driver,
images and launch scripts, with a README that states what it tests and how to
build, run and launch it.

| Workload | Tests |
|---|---|
| [ingest-durability](ingest-durability/README.md) | the ingest acknowledgement and visibility contract (docs/consistency-model.md) under faults |

Ravel's own binaries report Antithesis assertions through `crates/ravel-invariant`
(ADR-4326, docs/adrs/4326-tla-invariants-as-antithesis-properties.md). Its
`antithesis` feature links the SDK and its `antithesis-mutants` feature compiles
the mutant switches in; ravel-server and ravel-cli forward both. No published
build enables either (scripts/guards/check-release-profile.sh). A binary built
with `antithesis-mutants` reads `RAVEL_ANTITHESIS_MUTANT` once at start and
refuses to start on a name outside `ravel_invariant::mutant::MUTANTS`.
