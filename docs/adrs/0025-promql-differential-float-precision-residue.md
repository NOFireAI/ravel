# ADR-0025: PromQL differential float-precision residue (rate/deriv/predict_linear vs. atanh)

Status: Proposed, not decided (2026-07-28). Written after fixing three of the
four failure classes issue #170 found once `promql-difftest` first ran
against a real, correctly-checksummed pinned Prometheus binary (error
misclassification, JSON timestamp rendering, and several stale corpus
assertions; see #170 for the full history). This ADR takes up the one
class left deliberately unfixed: five corpus cases where both engines
agree on everything except the last few bits of an f64 result. Per
ADR-0021 decision 3, any tolerance beyond bit-exact comparison requires a
per-function allowlist entry in the differential harness with a written
justification; this document is that justification exercise, not yet a
decision. Do not add an allowlist entry or file an implementation ticket
from this ADR until its status changes to Accepted.

## Context

ADR-0021 decision 3 commits Ravel to mirroring Prometheus' floating-point
algorithms operation-for-operation, gated bit-exact by the differential
comparator (`f64::to_bits`, with only NaN-as-a-class and order-normalization
as principled relaxations), with the allowlist "expected to stay empty."
`crates/ravel-promql/src/functions/rate.rs`'s module doc makes the same
claim for its own family: "a direct, bit-exact-oriented port of
Prometheus' own `promql/functions.go` ... down to its operation order."

With a real Prometheus binary now in the loop (issue #170), five corpus
cases fail on value, not shape or class:

| Case | Function(s) | Prometheus | Ravel | Relative gap |
|---|---|---|---|---|
| `range_rate_across_reset_at_boundary` | `rate` | `...33333333333337` | `...3333333333333` | ~1e-16 |
| `instant_deriv_gauge_walk` | `deriv` | `-0.00007863573362837782` | `-0.00007863573362839007` | ~2e-13 |
| `range_predict_linear_over_a_grid` | `predict_linear` | `-6.405717689226038` | `-6.4057176892260355` | ~5e-16 |
| `instant_rate_over_irregularly_spaced_samples` | `rate` | `0.0023756459269379904` | `0.00237564592693799` | ~2e-15 |
| `instant_atanh_domain_clamped` | `atanh` (+ `clamp`) | `-3.8002011672501994` | `-3.8002011672501244` | ~2e-14 |

These five do not share one cause. Reading the implementations they
exercise splits them into two structurally different problems:

**`rate`/`deriv`/`predict_linear` (four of the five cases) use only IEEE
754 `+`, `-`, `*`, `/`.** `linear_regression` (backing `deriv` and
`predict_linear`) accumulates `sum_x`/`sum_y`/`sum_xy`/`sum_x2` in a single
forward loop and combines them with the same `covXY`/`varX`/`slope`/
`intercept` formula, in the same order, as Prometheus' `linearRegression`.
`extrapolated_rate` (backing `rate`) mirrors `extrapolatedRate`'s
counter-reset compensation and 1.1x boundary-extrapolation logic the same
way, including the module doc's explicit ms-vs-ns rounding-domain choices
for each individual division. IEEE 754's basic arithmetic operations are
exactly specified and language-agnostic: given the same operation sequence
over the same inputs, Rust and Go must produce the same bits. A residual
difference here means either the operation order is not actually identical
somewhere this reading missed, or the inputs reaching the function (sample
values or timestamps) already differ by the time they arrive, not that
exact arithmetic is unattainable. **This is very likely a findable bug in
the port or the harness, not a policy question**, and no one has yet done
the dedicated, per-case investigation to find it. It was set aside during
the #170 cleanup only because that work was scoped to
wiring/classification/corpus fixes, not evaluator numerics.

**`atanh` (the fifth case) is a single call to Rust's `f64::atanh`.**
Transcendental functions are not exactly specified by IEEE 754 beyond
correct rounding of the four basic operations and square root; `atanh`,
like `sin`, `cos`, `ln`, `exp`, `log2`, `log10`, `sinh`, `cosh`, `tanh`,
`asin`, `acos`, `atan`, `asinh`, `acosh` (the rest of
`crates/ravel-promql/src/functions/transform.rs`'s math family), is
whatever polynomial/rational approximation the underlying math library
implements. Rust's `f64::atanh` delegates to the platform's C library or
compiler-rt; Go's `math.Atanh` is a distinct, hand-written pure-Go
implementation. Two different approximation algorithms for an irrational
function generically disagree in the last one or two bits. Matching this
bit-for-bit would mean porting Go's exact `math` package algorithm for
every affected function, not fixing a bug in Ravel's own logic.

## The question

Two independent questions, deliberately not conflated:

1. Do the four pure-arithmetic cases get their own investigation (as an
   ordinary bug hunt, no ADR needed) rather than being bundled into this
   document as if they were the same kind of problem as `atanh`?
2. For `atanh` and the rest of the transcendental math family: does
   ADR-0021's "allowlist starts empty and is expected to stay empty" hold
   here, or does this family get a documented, scoped exception?

## Alternatives

**For the four arithmetic cases:**

1. File a dedicated investigation ticket (not this ADR) to find the actual
   divergence: instrument `linear_regression`/`extrapolated_rate` to log
   every intermediate sum/product for one failing case on both engines and
   diff them term-by-term, and check whether the seeded dataset generator
   produces bit-identical sample values on both ingestion paths (Prometheus
   via remote-write protobuf, Ravel via the in-process path) rather than
   assuming it does. This is the recommended path; it is consistent with
   ADR-0021's existing, already-accepted policy and requires no new
   decision.
2. Treat these four as unfixable and allowlist them anyway. Rejected out of
   hand: nothing here establishes that exact arithmetic is unattainable,
   only that nobody has yet looked closely enough to find why it currently
   isn't exact.

**For `atanh` and the transcendental math family:**

1. **Allowlist a scoped, relative-epsilon tolerance for this specific
   function family**, with the written justification that transcendental
   approximation-algorithm choice is a library implementation detail, not
   an evaluator semantics choice, and Prometheus itself does not guarantee
   or document its own `math.Atanh` bit pattern as a stable contract either.
   Lowest cost; matches ADR-0021's own anticipated escape hatch. Downside:
   introduces the harness's first non-empty allowlist entry, and needs a
   defensible epsilon (e.g., a fixed ULP count, not an arbitrary decimal
   tolerance) chosen so it cannot silently mask a real logic bug in
   `elementwise`/`clamp`/argument handling riding along with the expected
   library noise.
2. **Port Go's exact `math` package algorithms** for `atanh` and every
   sibling function in `transform.rs`'s math family. Achieves true bit-exact
   parity. Cost: reimplementing and maintaining roughly a dozen numerical
   algorithms most of this codebase has no other reason to touch, with its
   own correctness risk (a hand-ported transcendental function is easier to
   get subtly wrong than the standard library call it replaces), and no
   clear owner or precedent elsewhere in this repo for maintaining ported
   numerical-library internals.
3. **Narrow ADR-0021's scope explicitly**: state that transcendental math
   functions were never a realistic target for the bit-exact policy (unlike
   `rate`/`deriv`/aggregation, which this repo has already shown are
   achievable, per ADR-0022's Kahan-compensated `avg`), and amend ADR-0021
   itself rather than layering a separate allowlist exception on top.
   Functionally similar to alternative 1 but changes the normative document
   instead of adding to the harness's exception list.

## What is not decided here

This document does not choose between the transcendental-family
alternatives, and explicitly separates the arithmetic cases out so they are
not decided by default inaction on this ADR. Accepting alternative 1 for the
transcendental family requires, at minimum: enumerating every function in
`transform.rs`'s math family the same reasoning applies to (not just
`atanh`), picking a concrete tolerance mechanism and magnitude the harness
can enforce mechanically, and confirming no other corpus case for that
family already passes by coincidence at a tolerance this loose. Accepting
alternative 2 requires a scoping ticket sized like any other numerical-port
effort, not a quick patch.

## Consequences (conditional on eventual acceptance)

- If the arithmetic-case investigation (alternative 1 for that class)
  finds a real bug: a normal bug-fix commit, golden/corpus values updated
  in the same commit, no ADR status change needed since ADR-0021's policy
  was never in question there.
- If the transcendental family gets an allowlist entry: `promql-difftest`
  goes fully green for the first time since checksums were populated, and
  ADR-0021's "allowlist starts empty" sentence needs a follow-up note
  recording the one accepted exception and why.
- If instead ADR-0021 is amended (alternative 3): the same outcome, but the
  exception lives in the policy document itself rather than as a harness
  entry, which may be clearer for the next function added to that family.
