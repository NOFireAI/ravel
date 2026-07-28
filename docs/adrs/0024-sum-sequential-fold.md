# ADR-0024: Replace the built-in `sum` aggregate with a sequential-fold UDAF

Status: Proposed, not decided (2026-07-28). Split out of ADR-0022 (floating
aggregate exactness) at the orchestrating session's request: ADR-0022
admits `avg` via a custom UDAF with its own internal summation, but does
not touch the already-shipping, already-gated `sum` aggregate. This ADR
takes up, as its own decision, whether `sum` should change too. Unlike
ADR-0022 and ADR-0023, this document is not yet a chosen direction; it
exists so the argument the ADR-0022 drafting agent made is preserved and
reviewable on its own, separately from the admission policy ADR-0022
sets. Do not file an implementation ticket from this ADR until its
status changes to Accepted.

## Context

`sum` is already in the v1 SQL subset (docs/arrow-datafusion-plan.md
section 2), gated by the two-layer differential gate
(crates/ravel-sql/tests/differential.rs). Today it uses DataFusion's
built-in `sum` accumulator directly. That accumulator computes a batch's
partial sum with arrow's `compute::sum` kernel, which reduces internal
lanes in parallel; the lane count and reduction order are
architecture-dependent, so the bit pattern of a sum over values whose
partial sums are not exactly representable in f64 is not portable across
hosts. The existing gate documents this as a recorded deviation: ungrouped
`sum` is proptested only over a restricted pool of values whose partial
sums stay exactly representable, not the full adversarial pool (NaN
payloads, +/-Inf, -0.0, denormals, large-magnitude and
cancellation-prone values) the rest of the v1 subset is held to.

ADR-0022 admits `avg` as a new aggregate via a custom UDAF, because
DataFusion's built-in `avg` accumulator has the same lane-parallel-sum
property and so cannot meet ADR-0022's admission rule (full-pool,
bit-exact, portable). That UDAF's internal numerator is a plain
sequential left fold of f64 addition, independent of the public `sum`
aggregate. This means `avg(x)` and `sum(x)/count(x)`, computed via
today's built-in `sum`, are not guaranteed to agree bit-for-bit: two
different summation algorithms compute what is conceptually the same
quantity, on the same engine, for two different SQL expressions a user
might reasonably expect to match.

## The question

Should `sum` also move to a sequential-fold UDAF (the same algorithm
ADR-0022's `avg` uses internally), so that:

1. `sum` itself meets ADR-0022's admission rule on the full adversarial
   pool, rather than carrying the restricted-pool exception it has today,
   and
2. `avg(x)` and `sum(x)/count(x)` agree bit-for-bit on the engine surface?

## Alternatives (as argued when this was still part of ADR-0022; not
independently re-weighed by a dedicated review)

1. Keep `sum` as DataFusion's built-in, permanently. The restricted
   proptest pool stays a documented, accepted deviation. No behavior
   change to a shipping surface, ever, from this question. This is the
   status quo as of ADR-0022 merging.
2. Replace `sum` with a sequential-fold UDAF matching `avg`'s internal
   algorithm (first-value-seeded left fold of plain f64 addition in
   deterministic (series_id, ts) order). Lifts the restricted-pool
   deviation to full-pool coverage and establishes the `avg`/`sum`/`count`
   coherence identity. Costs: a behavior change to a shipping, gated
   aggregate. Two edges were identified as changing: ungrouped sums over
   values with inexact partial sums move from architecture-dependent
   lane-order bits to the sequential fold's bits, and an all-(-0.0) group
   changes from +0.0 (the built-in grouped accumulator's zero seed) to
   -0.0 (the IEEE-consistent answer under a first-value seed). Golden
   pins in the differential gate would need to update in the same
   commit as the change.
3. Some future ADR could also consider a higher-accuracy summation
   (Kahan or another compensated scheme) instead of naive sequential
   addition, if that is ever wanted; ADR-0022 rejected this for `avg`'s
   internal fold on the grounds that compensation buys no additional
   exactness against a reference running the identical algorithm. The
   same reasoning would apply here, but is a distinct question from
   whether to replace `sum`'s algorithm at all, and is not decided by
   this document either.

## What is not decided here

This document does not choose between alternatives 1 and 2. It exists so
the coherence argument is on record and reviewable, and so a future
decision (by a human, or by whatever process this repo uses to accept an
ADR) has the context already gathered rather than needing to be
re-derived. Accepting alternative 2 requires, at minimum: an explicit
sign-off that changing `sum`'s output bits on a shipping surface is
acceptable, an inventory of any downstream consumer that might depend on
the current bit pattern, and the golden-pin update landing in the same
commit as the accumulator change, per the pattern ADR-0023 established
for its own accumulator replacement.

## Consequences (conditional on eventual acceptance of alternative 2)

- `sum`'s bits change in the two edges named under alternative 2. The
  prior ungrouped behavior was architecture-dependent and never a stable
  contract, so this is a correction, not a regression, but any consumer
  bit-comparing results across the change would observe it.
- The differential gate's one documented deviation (the restricted
  ungrouped-sum proptest pool) is retired.
- `avg(x) == sum(x)/count(x)` becomes a bit-exact identity on the engine
  surface, matching ordinary user expectation.
