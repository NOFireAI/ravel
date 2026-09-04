# Probe: rewrite never enabled with a swept predecessor (finding 1b)

Verified against the shipped code first: `erasure_rewrite_bucket` in
`crates/ravel-maintain/src/erasure_rewrite.rs` calls `resolve_live_inputs`,
which does a fresh `list_bucket` read at rewrite time and derives the output
only from inputs still present, so it can never rewrite from an input the
retention sweep already deleted. The model's `RewriteOutputContent` derived
from `InitContent`, a static function of the object's initial content, and
`PerformRewrite` never checked that a predecessor was still present: `Tick`
followed by `RetentionSweep(raw1)` can delete `raw1` while `dreqR1` is
present and `superseded = {}` still hold, letting the old guard set fire and
derive `rwA`'s content from an input no longer in the store.

## Probe module

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, adding:

```
ProbeNoRewriteWithMissingPredecessor ==
    (\E i \in Predecessors("rwA") : ~PresentObj(i)) => ~(ENABLED PerformRewrite)
```

checked as the sole non-`TypeOK` invariant, `SPECIFICATION Spec`, `VIEW
View`, every switch at `smoke.cfg`'s shipped value, `MaxClock = 2`.

## Result against the model without the new guard (insufficient)

`PerformRewrite` with `~PresentObj("doneR1")` present but no predecessor
presence conjunct. TLC exit 12:

```
Error: Invariant ProbeNoRewriteWithMissingPredecessor is violated.
```

8-state trace ending at `RetentionSweep`: `raw1` swept (absent), `dreqR1`
present, `doneR1` absent, `superseded = {}`, `clock = 2` --
`PerformRewrite`'s guard set without the predecessor-presence conjunct is
fully satisfied there (4407 states generated, 1645 distinct, run stopped at
the first violation).

## Result against the shipped fix

`PerformRewrite` gated additionally on `\A i \in Predecessors("rwA") :
PresentObj(i)`. TLC exit 0. Exact line:

```
Model checking completed. No error has been found.
```

393481 states generated, 71018 distinct states found, 0 left on queue, depth
21 (full state graph for this probe's cfg, not a bound run).
