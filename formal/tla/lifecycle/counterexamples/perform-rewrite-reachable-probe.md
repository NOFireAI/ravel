# Probe: PerformRewrite still reaches its output after the new ordering gate

Finding 2 (round four, issue #1122) gated `PerformRewrite` on
`PresentObj("dreqR1")` and `superseded = {}`: without those, the model could
materialise the rewrite output and supersede the raw inputs before any
erasure request existed, an ordering `erasure_rewrite.rs` never produces
(`pending_erasure_requests` only rewrites a bucket with a `.dreq` and no
matching `.done`, and `ErasureRewriteOutcome::AlreadyApplied` skips a bucket
already rewritten for every pending request). A guard change that makes an
action dead would make every invariant about it pass for the wrong reason,
so the fix needs its own reachability check, not just the gated behaviour's.

## Probe module

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, checked with
`SPECIFICATION Spec`, `VIEW View`, `INVARIANT TypeOK` plus the probe,
`smoke.cfg`'s constants.

```tla
ProbeReachesPerformRewrite ==
    ~(PresentObj("rwA") /\ RawInputs \subseteq superseded)
```

(Same target as `ProbeReachesRewriteSupersession` in
`reachability-probes.md`, which was checked at `exhaustive.cfg`'s
`MaxClock = 3` for a different question, the shrunk exhaustive bound. This
run is at `smoke.cfg`'s `MaxClock = 2`, against the new gate.)

## Result

Violates (TLC exit 12): 267 states generated, 144 distinct, before the first
violation. The trace is `RequestErasure` (writes `.dreq`) immediately
followed by `PerformRewrite`, i.e. the model now reaches the rewrite output
only through the same ordering the implementation requires.
