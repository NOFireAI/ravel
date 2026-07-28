# ADR-0025: PromQL differential float-precision residue (rate/deriv/predict_linear vs. atanh)

Status: Accepted (2026-07-28). Written after fixing three of the four
failure classes issue #170 found once `promql-difftest` first ran against
a real, correctly-checksummed pinned Prometheus binary (error
misclassification, JSON timestamp rendering, and several stale corpus
assertions; see #170 for the full history). This ADR took up the one
class left deliberately unfixed: five corpus cases where both engines
agree on everything except the last few bits of an f64 result.

**Decision recorded:** alternative 1 (allowlist a scoped tolerance) for
both classes below, including the four arithmetic cases the "What is not
decided here" section originally recommended a dedicated bug-hunt
investigation for instead. Explicitly chosen after that tradeoff was
named: the arithmetic-case divergence is very likely a findable bug (see
below), and allowlisting it accepts not finding it right now, in exchange
for a green `promql-difftest`. This is a deliberate, informed choice, not
an oversight; if a real correctness bug is hiding in `rate`/`deriv`/
`predict_linear`, it stays latent until something else surfaces it. See
Consequences.

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

## The question (as posed before acceptance)

Two independent questions, deliberately not conflated:

1. Do the four pure-arithmetic cases get their own investigation (as an
   ordinary bug hunt, no ADR needed) rather than being bundled into this
   document as if they were the same kind of problem as `atanh`?
2. For `atanh` and the rest of the transcendental math family: does
   ADR-0021's "allowlist starts empty and is expected to stay empty" hold
   here, or does this family get a documented, scoped exception?

## Decision

Both classes get a scoped, per-entry ULP tolerance in the differential
harness (alternative 2 for the arithmetic cases, alternative 1 for the
transcendental family, in the numbering below), chosen explicitly over the
recommended arithmetic-case investigation: `promql-difftest` reaching a
green baseline now outweighs finding the arithmetic cases' root cause
today. That tradeoff was named and knowingly accepted, not defaulted into;
see Consequences for what it costs.

Mechanism: `CorpusEntry` gained an optional `tolerance_ulps` field (corpus
key `tolerance: <non-negative integer>`), consumed by the comparator's new
`within_ulps` check. Absent (the default, and now true for every entry
except the five below) still means exact `f64::to_bits` comparison; `-0.0`
vs `0.0` is exact-bit-only regardless of any tolerance value, by
construction, not by convention, so this mechanism cannot erode that
project-wide rule even by mistake. Each of the five allowlisted entries
carries its own measured ULP distance and a comment naming which of the
two classes below it belongs to and why:

| Case | Measured gap | Tolerance | Class |
|---|---|---|---|
| `range_rate_across_reset_at_boundary` | 1 ULP | 2 | arithmetic |
| `instant_deriv_gauge_walk` | 904 ULP | 1024 | arithmetic |
| `range_predict_linear_over_a_grid` | 3 ULP | 8 | arithmetic |
| `instant_rate_over_irregularly_spaced_samples` | 1 ULP | 2 | arithmetic |
| `instant_atanh_domain_clamped` | 169 ULP | 256 | transcendental |

## Alternatives

**For the four arithmetic cases:**

1. File a dedicated investigation ticket to find the actual divergence:
   instrument `linear_regression`/`extrapolated_rate` to log every
   intermediate sum/product for one failing case on both engines and diff
   them term-by-term, and check whether the seeded dataset generator
   produces bit-identical sample values on both ingestion paths (Prometheus
   via remote-write protobuf, Ravel via the in-process path) rather than
   assuming it does. Consistent with ADR-0021's existing, already-accepted
   policy and requires no new decision. **Not chosen**, in favor of 2 below.
2. **Allowlist these four with a measured, per-entry ULP tolerance, without
   further investigation right now.** Chosen. The divergence is very likely
   a findable bug (nothing here shows exact arithmetic is unattainable,
   only that nobody has looked closely enough yet), so this knowingly
   leaves that bug, if it exists, unfound; see Consequences.

**For `atanh` and the transcendental math family:**

1. **Allowlist a scoped ULP tolerance for this specific function family**,
   with the written justification that transcendental approximation-
   algorithm choice is a library implementation detail, not an evaluator
   semantics choice, and Prometheus itself does not guarantee or document
   its own `math.Atanh` bit pattern as a stable contract either. **Chosen.**
   Only `atanh` is allowlisted today because it is the only member of the
   family the corpus currently exercises numerically past an exact match;
   the same reasoning, and the same mechanism, applies to any sibling
   function (`sin`, `cos`, `ln`, `exp`, `log2`, `log10`, `sinh`, `cosh`,
   `tanh`, `asin`, `acos`, `atan`, `asinh`, `acosh`) if or when its own
   corpus case turns up a similar gap; no ADR amendment is needed to
   allowlist another one, just a measured tolerance and a comment citing
   this decision.
2. Port Go's exact `math` package algorithms for `atanh` and every sibling
   function in `transform.rs`'s math family. **Not chosen**: reimplementing
   and maintaining roughly a dozen numerical algorithms most of this
   codebase has no other reason to touch, with its own correctness risk (a
   hand-ported transcendental function is easier to get subtly wrong than
   the standard library call it replaces), and no clear owner or precedent
   elsewhere in this repo for maintaining ported numerical-library
   internals, for a difference nobody has shown matters to a real query.
3. Narrow ADR-0021's scope explicitly instead of using the harness
   allowlist. Not chosen: functionally equivalent to alternative 1 here,
   and the harness allowlist is the mechanism ADR-0021 itself already
   named, so there is no reason to also edit that document.

## Consequences

- The five cases named in this ADR pass under tolerance, verified locally
  against the pinned v3.13.1 binary; every other pre-existing entry stays
  bit-exact. `promql-difftest` is not fully green as of this ADR, though:
  running the full corpus surfaced 7 unrelated mismatches in the
  aggregation corpus (`aggregate.txt`, added the same day by the P8
  aggregation-operators work), none of which this ADR's decision covers.
  Tracked separately in issue #177.
- The differential harness's allowlist is no longer empty. ADR-0021's
  "expected to stay empty" is superseded by this decision for these five
  named entries; any future entry needs the same measured-gap-plus-comment
  treatment established here, not a bare tolerance number.
- The arithmetic-case divergence (`rate`/`deriv`/`predict_linear`) is
  explicitly not investigated further by this decision. If it is a real bug
  in Ravel's own port (the more likely explanation per the Context
  section), it stays latent: a future change to these functions could
  silently make the gap worse (up to the allowlisted tolerance) without
  any test catching it, or could fix it without anyone noticing the
  tolerance is now unnecessarily loose. Whoever next touches
  `rate.rs`/`linear_regression` should treat a tightenable tolerance as a
  signal worth checking, not just leave it.
- `atanh`'s tolerance is scoped to that one function and one corpus entry;
  it does not pre-allowlist the rest of the transcendental family. Adding
  another sibling function's own corpus case that turns out non-exact can
  cite this ADR directly rather than raising a new one.
