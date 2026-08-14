# ADR-0006: Custom signal-aware engine first; Arrow/DataFusion evaluated at Phase 3

Status: Accepted

## Context

PromQL range-vector semantics (lookback, staleness, counter resets, native
histograms) do not lower cleanly onto generic relational operators. The spec
mandates a shared typed IR eventually, plus Arrow/DataFusion evaluation where
useful.

## Decision

Phase 1–2 implement a compact custom engine: snapshot resolution → segment
pruning (time + matchers) → page reads → per-series sample iterators →
PromQL evaluator operating on (series, timestamp, value) streams. No Arrow on
this path yet; batches are plain columnar Rust structs.

DataFusion/Arrow are evaluated at Phase 3 (logs), where relational operators,
predicate pushdown, and SQL/Flight SQL interop pay off. The shared logical IR
is introduced with RavelQL; PromQL keeps its dedicated evaluator regardless
(ADR-0007); the IR handles scans/filters/correlation, not PromQL numerics.

## Consequences

- Fast path to a correct vertical slice without carrying Arrow's dependency
  weight into ingest-critical crates.
- Risk: rework when the IR lands. Contained by keeping the evaluator's input
  a narrow trait (`SeriesStream`) that any storage or IR backend can produce.
