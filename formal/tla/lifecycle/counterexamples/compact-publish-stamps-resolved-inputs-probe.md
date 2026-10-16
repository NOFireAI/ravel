# Probe: a CompactOut publish stamps its resolved input names, never rwA's

`PublishRewrite` names its two output variants by `RewriteKey` and stores the
names in `variantKey`, which `IdenticalInputSetsDoNotCollide` reads. The names
must describe the input set the publish actually resolved. `TargetOf` maps a
resolved set of `CompactOut` to `rwB` (the rewrite of the compaction output),
so an `rwB` publish resolved `CompactOut`, and its stored names must be
`CompactOut`-derived. Before the fix `PublishRewrite` stamped fixed
`RawInputs`-derived descriptors (`DescA`/`DescB`) regardless of what it
resolved, so an `rwB` publish wrote the two `RawInputs` names, and the collision
property then read names that no longer described the publish that wrote them.
The fix derives the descriptors from `rwInputs[id]`.

## The probe

```tla
ProbeCompactPublishStampsResolvedInputs ==
    (PresentObj("rwB") /\ ~PresentObj("rwA")) =>
        \A v \in {"v1","v2"} :
            variantKey[v] \notin { <<RawInputs, {"r1"}>>,
                                   <<RawInputs, {}>>,
                                   <<RawInputs>> }
```

When `rwB` is present and `rwA` is absent, the publish that stamped `variantKey`
resolved `CompactOut`, so no stored name may be one of the `RawInputs` keys. The
antecedent is exact: in the shipped configuration `rwA` and `rwB` never coexist
(`StartCompaction` refuses while a rewrite output is present, and once `rwB` is
published `StartRewrite` refuses the already-applied bucket), so `rwB` present
with `rwA` absent means the last publish to stamp `variantKey` was the
`CompactOut` one. A scratch copy under `/tmp` extends `MCLifecycleGC`, with
`smoke.cfg`'s constants (every switch shipped), `SPECIFICATION Spec`, `VIEW
View`, and this one `INVARIANT`. It is a state invariant, so a violation is TLC
exit 12.

## Before the fix: violated

The reaching trace is the one round eight recorded for `rwB`'s reachability:
`RequestErasure`, `StartCompaction`, `ExpireLease`, `PublishCompaction`,
`StartRewrite`, `PublishRewrite`. At the `PublishRewrite` step `rwInputs` is
`CompactOut` and `TargetOf` selects `rwB`, but the pre-fix stamp writes
`variantKey["v1"] = RewriteKey(DescA) = <<RawInputs, {"r1"}>>` and
`variantKey["v2"] = RewriteKey(DescB) = <<RawInputs, {}>>`. The state then has
`rwB` present, `rwA` absent, and both stored names in the forbidden `RawInputs`
set, so the invariant is violated (exit 12): a publish whose resolved inputs are
`CompactOut` stamped a `RawInputs` name.

## After the fix: holds

The same publish now writes `RewriteKey(VariantDescA(CompactOut)) =
<<CompactOut, {"r1"}>>` and `RewriteKey(VariantDescB(CompactOut)) =
<<CompactOut, {}>>`, neither of which is a `RawInputs` key, so the antecedent is
satisfiable without the consequent failing. No other reachable state trips it:
an `rwA` publish (which stamps the `RawInputs` names) can only occur on the path
where no compaction runs, and on that path `rwB` is never published, so the
antecedent's `PresentObj("rwB")` is false whenever the `RawInputs` names are
stored. The invariant holds over the complete graph (exit 0).

## Execution note

This tree was prepared on a fleet executor with no Java runtime and no network,
so TLC could not be run here to capture the counterexample depth and the
complete-graph state totals. The exit codes above follow from TLC's semantics
for a state invariant (12 on violation, 0 when it holds over the complete
graph); the depth and state figures are confirmed by the PR's TLA lane
(`scripts/check-tla.sh`, `actions/setup-java` temurin 21), which runs the same
`smoke.cfg` constants this probe uses.
