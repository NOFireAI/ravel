# Probe: the rewrite is still reachable after both new guards (finding 1)

A guard that makes an action dead would pass every invariant for the wrong
reason. This confirms `PerformRewrite` still fires at all once both new
conjuncts (`~PresentObj("doneR1")`, predecessor presence) are in place.

## Probe module

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, adding the same
formula as `reachability-probes.md`'s `ProbeReachesRewriteSupersession`:

```
ProbeReachesPerformRewrite ==
    ~(PresentObj("rwA") /\ RawInputs \subseteq superseded)
```

checked as the sole non-`TypeOK` invariant against the fully fixed model,
`SPECIFICATION Spec`, `VIEW View`, every switch at `smoke.cfg`'s shipped
value, `MaxClock = 2`.

## Result

TLC exit 12:

```
Error: Invariant ProbeReachesPerformRewrite is violated.
```

5-state trace ending at `PerformRewrite`: `rwA` present with
`objContent["rwA"] = {"rec2"}`, `raw1` still present, `dreqR1` present,
`doneR1` absent, `superseded = {"raw1"}`, `head = {"raw1"}` -- the ordinary
`RequestErasure`-then-rewrite path, unchanged by the new guards (252 states
generated, 146 distinct).
