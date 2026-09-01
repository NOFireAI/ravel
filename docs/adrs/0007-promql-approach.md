# ADR-0007: promql-parser crate for parsing, own evaluator, differential testing gate

Status: Accepted

## Context

PromQL compatibility must match evaluation semantics, not just syntax. Writing
a parser is mechanical; writing a semantically exact evaluator is the hard,
valuable part.

## Alternatives

1. Port Prometheus' Go parser+engine: maximal fidelity, enormous effort,
   license-workable (Apache-2.0) but a moving target.
2. `promql-parser` crate (Apache-2.0, GreptimeDB-maintained, tracks upstream
   grammar): parse only, we own evaluation.
3. Full custom parser + evaluator.

## Decision

Option 2. The evaluator is ours, written against the Prometheus documented
semantics and source behavior: 5m default lookback, staleness markers,
@/offset, subqueries, vector matching, counter-reset handling in
rate/increase, native histogram arithmetic (Phase 2+).

Gate: no PromQL feature is "done" until it passes differential tests against a
pinned Prometheus binary on generated datasets (values, labels, timestamps,
NaN/Inf, warnings, errors). Phase 1 scope: instant/range vector selectors with
all matcher types and offset.

## Consequences

- Parser bugs upstream become our bugs; the differential suite catches
  semantic drift regardless of which layer causes it.
- AST types from the crate leak into ravel-promql only; the public API of the
  crate exposes Ravel types.
