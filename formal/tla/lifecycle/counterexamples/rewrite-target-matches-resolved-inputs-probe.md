# Probe: a rewrite target's predecessors equal the input set the pass resolved

`PublishRewrite` writes `objContent'[tgt] = RecordSetContent(tgt)`, and
`RecordSetContent` reads the static `Predecessors(tgt)` map to decide which raw
inputs the published object materialises. The target is chosen by
`TargetOf(rwInputs[id])`. For the published content to describe the pass that
produced it, the predecessors of the chosen target must be exactly the input
set the pass resolved and recorded in `rwInputs[id]`. The invariant states that
directly:

```tla
RewriteTargetMatchesResolvedInputs ==
    \A id \in RewriteIds :
        rwPhase[id] = "done" =>
            Predecessors(TargetOf(rwInputs[id])) = rwInputs[id]
```

In the shipped model this holds trivially, and that is the risk: `TargetOf`
maps every resolved set other than `CompactOut` to `"rwA"`, and `Predecessors`
reads `Predecessors("rwA") = RawInputs`. They agree only because the shipped
`RawInputs` and `CompactOut` are both singletons, so the resolved set is always
the whole predecessor set. An invariant that can only be satisfied one way, on a
model that cannot express a proper subset, proves nothing. The probe makes the
model express one.

## The probe: two raw inputs, a resolvable proper subset

A scratch copy under `/tmp` widens `RawInputs` to `{"raw1", "raw2"}` and starts
from a store where `raw2` is absent at `Init` (present = FALSE), so
`resolve_live_inputs` (`LiveInputs`) resolves the proper subset `{"raw1"}` while
`Predecessors("rwA")` still names both. The rest of the constants are
`smoke.cfg`'s, at `MaxClock = 1` (the violation is reachable in three
transitions, so the tighter bound suffices and keeps the graph small),
`SPECIFICATION Spec`, `VIEW View`, and this one `INVARIANT`. It is a state
invariant, so a violation is TLC exit 12.

## Before the widening: violated

`TargetOf` is the shipped `IF ins = CompactOut THEN "rwB" ELSE "rwA"`, so a pass
that resolves `{"raw1"}` targets `"rwA"`, whose predecessors are
`{"raw1", "raw2"}`. TLC reaches the violation at depth 3:

```
State 1: <Initial predicate>
State 2: <RequestErasure ...>
State 3: <StartRewrite ...>      rwInputs[A] resolves to {"raw1"}
State 4: <PublishRewrite ...>    rwPhase[A] = "done", target "rwA"
```

At State 4 `rwPhase[A] = "done"` and `rwInputs[A] = {"raw1"}`, but
`Predecessors(TargetOf({"raw1"})) = Predecessors("rwA") = {"raw1", "raw2"}`,
which is not `{"raw1"}`. TLC:

```
Error: Invariant RewriteTargetMatchesResolvedInputs is violated.
...
401 states generated, 206 distinct states found
```

Exit 12, counterexample four states (three transitions). The published `rwA`
would claim `raw2` as a predecessor the pass never resolved.

## After the widening: holds

The fix gives each resolvable input set its own target whose predecessors equal
it. `TargetOf` gains a case for the subset and `Predecessors` gains the matching
target:

```tla
TargetOf(ins) == CASE ins = CompactOut -> "rwB"
                   [] ins = {"raw1"}    -> "rwA2"
                   [] OTHER             -> "rwA"

Predecessors(o) == CASE o = "rwA"  -> RawInputs
                     [] o = "rwA2" -> {"raw1"}
                     ...
```

Now `Predecessors(TargetOf({"raw1"})) = Predecessors("rwA2") = {"raw1"}`, and
`TargetOf(RawInputs) = "rwA"` with `Predecessors("rwA") = RawInputs`. Every
resolvable set maps to a target whose predecessors are that set, so the
consequent holds wherever `rwPhase[id] = "done"`. TLC over the complete graph:

```
Model checking completed. No error has been found.
2002442 states generated, 322428 distinct states found
The depth of the complete state graph search is 27.
```

Exit 0, complete graph, no violation.

## What this pins for the shipped model

The shipped model keeps the singleton `RawInputs` and `CompactOut`, where the
invariant is satisfied because `TargetOf`/`Predecessors` agree on those two
singletons. The probe shows the invariant is not vacuous: it distinguishes a
target mapping that respects resolved inputs from one that does not, and the
current single-`"rwA"` mapping only passes because no proper subset of
`RawInputs` is reachable at the shipped bound. If a future change lets a pass
resolve a proper subset without widening the target map to match, this
invariant fails rather than silently publishing content that overclaims its
predecessors.

## Execution note

Run on a fleet executor with no system Java. A Temurin JRE 21 was fetched into
`/tmp` and TLC (`tla2tools-1.7.4`, TLC2 v2.19) was run with `-workers 2
-Xmx2g`; the exit codes, trace, and state totals above are that run's output.
The scratch model lives under `/tmp` and is not committed. The gate configs
(`smoke.cfg`, `exhaustive.cfg`, and every `negative/*.cfg`) carry this
invariant against the shipped singleton model, where it holds over the complete
graph.
