# ADR-0023: Grouped MIN/MAX restored via a total-order min/max UDAF replacing the built-ins

Status: Accepted

Builds on ADR-0013 and its exactness regime. Sibling decision: ADR-0022
covers the `avg`/`stddev` exactness exclusion; this ADR decides grouped
MIN/MAX total-order semantics only.

## Context

The v1 SQL subset was specified as `count`, `sum`, `min`, `max` under
`GROUP BY`. A review found that DataFusion
54.1.0 ships two disagreeing MIN/MAX implementations. The ungrouped path
(`AggregateStream` with `MinAccumulator`/`MaxAccumulator` over arrow's
min/max kernels) compares with `f64::total_cmp`, a true total order:
negative NaN is the minimum, positive NaN the maximum, -0.0 < 0.0. The
grouped path (`GroupedHashAggregateStream` with
`PrimitiveGroupsAccumulator`) instead folds `partial_cmp` in row order
from an `f64::MAX`/`f64::MIN` seed, which is not a total order and is
wrong three ways: a NaN in a group makes every later comparison return
`None`, so the accumulator keeps the newest row instead of the true
extreme; `-0.0` and `0.0` compare `Equal`, so the first seen wins and the
result depends on arrival order; and a group whose only values are
`+Inf`/`-Inf` never displaces the seed, so MIN/MAX returns
`f64::MAX`/`f64::MIN` instead of the actual value. These are silently
wrong answers, not approximations ("Exact semantics by default").

The interim fix rejects MIN/MAX combined with
`GROUP BY` at validation: `reject_grouped_min_max` and its
`GroupedMinMaxFinder` walk in crates/ravel-sql/src/validate.rs. That
interim fix left the long-term direction to this ADR.

Two facts from the interim period weigh on the decision:

- The rejection walk has already been wrong once: a
  grouped `min`/`max` appearing only in a query-level `ORDER BY` escaped
  the walk, because `ORDER BY` is a field of `Query`, not `Select`, and
  the grouped-scope stack was popped before it was visited. A legal
  single SELECT reached exactly the accumulator the rejection exists to
  avoid. The hole is fixed and regression-tested, but the structural
  weakness stands: unlike `avg`, which the session builder also
  deregisters as a backstop (crates/ravel-sql/src/session.rs), `min` and
  `max` must stay registered for the ungrouped case, so the walk is the
  sole guard and must stay airtight against every sqlparser AST shape on
  every version bump.
- The differential gate cannot cover the current grouped accumulator.
  With semantics defined only by the accumulator's fold order, an
  independent reference is a second copy of the same seed-and-fold
  algorithm; both sides agree on the wrong answer. The gap is
  untestable as long as the semantics are accidental.

## Alternatives

1. Total-order min/max UDAF registered over the built-ins (chosen).
   Replicates the `f64::total_cmp` order the ungrouped path already uses,
   registered under the names `min`/`max` so it replaces DataFusion's
   built-ins for grouped and ungrouped execution alike.
2. Wait for an upstream DataFusion fix; re-enable grouped MIN/MAX when a
   version bump's differential gate proves it correct. This would keep
   the rejection walk unchanged, note the dependency on the DataFusion
   changelog in the version-pinning policy, and land the grouped golden
   cases `#[ignore]`d with a comment so re-enablement is trivial.
   Rejected: the timeline is open-ended for a live gap in the v1
   surface; upstream's eventual semantics are their choice, and nothing
   guarantees they converge the grouped path onto `total_cmp` rather
   than, say, NaN-skipping semantics, so re-enablement is conditional on
   their pick matching our ungrouped pin; and until then the fragile
   walk remains the sole guard, with no backstop possible.
3. Keep the validation rejection permanently; grouped MIN/MAX is simply
   not in Ravel's SQL surface. This deserves real weight: the hole
   is fixed and regression-tested, the walk's cost looks one-time, and a
   UDAF is an ongoing maintenance burden. Rejected: the extreme per
   series (`SELECT series_id, max(value) ... GROUP BY series_id`) is the
   single most idiomatic query a telemetry database serves, so the hole
   is a permanent product defect, not a semantics simplification; and
   the walk's cost is not in fact one-time, because with no
   deregistration backstop available it must be re-proven against every
   sqlparser AST change on every version bump, which is exactly the
   fragility class that hole demonstrated.
4. Hybrid: ship the UDAF now, delete it once upstream fixes the grouped
   accumulator. Rejected: deletion would hand result semantics back to
   upstream, letting a later version bump silently change query results
   if their ordering ever diverges from `total_cmp`; the gate would
   catch the divergence, and the remedy would be reinstating the UDAF.
   Once gated, the UDAF's retention cost is near zero; keep it as the
   permanent owner of float extreme semantics.

## Decision

1. **Semantics.** MIN and MAX over floating-point values use the
   `f64::total_cmp` total order, grouped and ungrouped alike: negative
   NaN below `-Inf`, `-0.0 < 0.0`, positive NaN above `+Inf`; the NaN
   payload bits of the winning extreme are preserved; the result is a
   function of the input multiset, never of arrival order. Grouped and
   ungrouped MIN/MAX over the same multiset must agree bit for bit. This
   is the normative statement; DataFusion's behavior is no longer the
   definition.
2. **Mechanism.** One `AggregateUDFImpl` pair, registered in
   `build_session` (crates/ravel-sql/src/session.rs) under the names
   `min` and `max` via `register_udaf`, which inserts by name into the
   session's aggregate registry and displaces the built-in entry
   (verified in datafusion 54.1.0, session_state.rs `register_udaf`).
   DataFusion does not dispatch grouped versus ungrouped by function
   name: the one resolved UDAF serves both modes. Ungrouped plans call
   its `accumulator()`; `GroupedHashAggregateStream` calls
   `create_groups_accumulator()` when `groups_accumulator_supported()`
   answers true and otherwise wraps `accumulator()`'s output in
   `GroupsAccumulatorAdapter` (verified in datafusion 54.1.0,
   physical-plan row_hash.rs). Replacing the name therefore replaces
   both paths at once; no syntactic grouped/ungrouped distinction
   remains anywhere.
3. **Accumulator shape.** A plain `Accumulator` implementation, not a
   custom `GroupsAccumulator`: `groups_accumulator_supported()` returns
   false for float inputs, so grouped execution runs one accumulator per
   group behind `GroupsAccumulatorAdapter`. State is `Option<f64>`;
   absence means "no rows seen", so there is no seed value to leak and
   the all-infinite-group bug is impossible by construction.
   `update_batch` folds `total_cmp`; `state()` round-trips the extreme
   as one nullable Float64 `ScalarValue` and `merge_batch` folds the
   same order, which is associative and commutative, so partial/final
   aggregation splits stay exact. Performance is acceptable for v1:
   aggregation is pinned single-partition
   and group count is bounded by the matched-series budget (10k). A
   vectorized `GroupsAccumulator` is a later, benchmark-gated change
   under ADR-0012 discipline and must pass the same gate; it is not part
   of this decision.
4. **Type coverage.** Floating-point input types (Float64, Float32,
   Float16) take the total-order accumulator. Every other input type
   delegates to a wrapped built-in (`min_udaf()`/`max_udaf()` held
   inside the replacement, with signature, return type, state fields,
   and accumulator construction forwarded), because `partial_cmp` is
   already total for non-float primitives and the seed cannot surface a
   wrong extreme there; non-float grouped MIN/MAX keeps upstream's
   vectorized path. `min(ts)` and friends keep working unchanged.
5. **Placement.** New module `crates/ravel-sql/src/minmax.rs` exporting
   `total_order_min_udaf()` and `total_order_max_udaf()`; registration
   sits in `build_session` beside the existing `avg`/`mean`
   deregistration backstop.
6. **Deletions.** The same change that registers the UDAF and lands the
   evidence in point 7 deletes `reject_grouped_min_max`,
   `GroupedMinMaxFinder`, `select_is_grouped`,
   `ValidationError::GroupedMinMaxUnsupported`, and the rejection tests
   in validate.rs, and retires the audit reproducer
   `audit_sql4_validate.rs::grouped_min_max_in_order_by_must_be_rejected`
   (its shape becomes an acceptance-plus-correct-result differential
   case). The `avg` walk stays; it is ADR-0022's concern. At no point is
   there a window with neither the rejection nor the UDAF in place.
7. **Required evidence.** The decision counts as executed only when all
   of the following are green in crates/ravel-sql:
   - Golden grouped cases pinned by bits against datafusion 54.1.0: a
     NaN payload preserved as a group's extreme; a group holding NaN
     plus a smaller later value returns the smaller value for MIN (no
     poisoning); `-0.0`/`0.0` groups in both arrival orders return
     `-0.0` for MIN and `0.0` for MAX; all-`+Inf` and all-`-Inf` groups
     return the infinity, never `f64::MAX`/`f64::MIN`.
   - A proptest asserting grouped and ungrouped MIN/MAX agree bit for
     bit on single-group datasets drawn from the float edge-case pool.
   - The layer-2 differential gate extended: grouped query shapes in
     tests/differential.rs gain min/max columns, evaluated per group by
     the existing `min_total_order`/`max_total_order` reference folds,
     asserted f64-bit-identical over the edge-case pool. This is the
     gate previously called impossible; defining the semantics as a total
     order is what makes the scalar reference independent.
   - The previously-escaping shapes execute correctly: `GROUP BY ... ORDER
     BY max(value)` and `HAVING min(value)` covered by golden or
     differential cases.
   - A delegation case: grouped MIN/MAX over `ts` (Timestamp) plans and
     matches an integer reference fold.
   - All of the above join the pinned surface re-run on every
     arrow/datafusion version bump.

## Consequences

- Grouped MIN/MAX returns to the v1 SQL subset; the rejection error,
  its message, and the documented subset description are updated.
- ravel-sql owns floating-point extreme semantics. Upstream fixes or
  regressions in DataFusion's grouped accumulator no longer affect
  results; a version bump cannot silently change MIN/MAX answers.
- The validation walk shrinks and with it that fragility class: the
  guard for min/max moves from a syntactic walk that must be airtight to
  a registry replacement that is structurally total, the same shift the
  `avg` deregistration backstop already made for avg.
- Grouped execution pays one boxed accumulator per group through
  `GroupsAccumulatorAdapter`. Bounded by the 10k matched-series budget
  and the single-partition aggregation rule; a vectorized
  `GroupsAccumulator` is a measured follow-up, not assumed.
- Optimizer special-cases keyed to the built-in Min/Max types (for
  example statistics-based shortcut evaluation) do not fire for the
  replacement. Irrelevant today: `RsegScanExec` supplies no statistics,
  and exactness prefers the executed path regardless.
- `ValidationError` loses a variant; workspace-internal API change only.
- The DataFusion seed-leak (behavior 3 above) should be reported
  upstream as a courtesy; nothing here depends on its resolution.
- The `stddev`/`var` family gap remains open and belongs to ADR-0022's
  scope, not this ADR.
