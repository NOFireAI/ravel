# Probe: rewrite never enabled once its erasure has completed (finding 1a)

Verified against the shipped code first: `pending_erasure_requests` in
`crates/ravel-maintain/src/erasure_rewrite.rs` filters out any `.dreq` whose
`request_id` has a matching `.done`, so a completed erasure is never surfaced
as still pending and `erasure_rewrite_bucket` never rewrites for it again.
The model's prior `PerformRewrite` guard (`~PresentObj("rwA") /\
PresentObj("dreqR1") /\ superseded = {}`) had no equivalent: `RetireBucket`
followed by `DropRetiredBucketFromHead` can make `CompleteErasure` fire and
write `doneR1` while `superseded` is still `{}` and no rewrite has ever run,
after which the old guard set still permitted `PerformRewrite`.

## Probe module

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, adding:

```
ProbeNoRewriteAfterCompletion ==
    PresentObj("doneR1") => ~(ENABLED PerformRewrite)
```

checked as the sole non-`TypeOK` invariant, `SPECIFICATION Spec`, `VIEW
View`, every switch at `smoke.cfg`'s shipped value, `MaxClock = 2`.

## Result against the model without the new guard (insufficient)

`PerformRewrite` with only its pre-existing three conjuncts (no
`~PresentObj("doneR1")`). TLC exit 12:

```
Error: Invariant ProbeNoRewriteAfterCompletion is violated.
```

7-state trace ending at `CompleteErasure`: `doneR1` present, `dreqR1`
present, `raw1` still present, `superseded = {}`, `clock = 1` --
`PerformRewrite`'s old guard set is fully satisfied there (2656 states
generated, 1052 distinct, run stopped at the first violation).

## Result against the shipped fix

`PerformRewrite` gated additionally on `~PresentObj("doneR1")`. TLC exit 0.
Exact line:

```
Model checking completed. No error has been found.
```

393481 states generated, 71018 distinct states found, 0 left on queue, depth
21 (full state graph for this probe's cfg, not a bound run).
