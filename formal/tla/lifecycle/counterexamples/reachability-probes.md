# Probes: exhaustive's shrunk bound still reaches the four load-bearing behaviours

`exhaustive.cfg` shrank from `MaxClock = 4` (does not complete) to `MaxClock =
3` (completes; see results.md, "Exhaustive"). Shrinking a clock bound can
make a behaviour unreachable without any invariant saying so, since an
invariant that is never challenged passes vacuously. Each probe below asserts
one target behaviour never happens; TLC violating it proves the shrunk bound
still reaches that behaviour.

## Probe module

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, checked with
`SPECIFICATION Spec`, `VIEW View`, `INVARIANT TypeOK` plus one probe at a
time, `exhaustive.cfg`'s constants (`MaxClock = 3`, every switch at its
shipped value). Each probe is checked alone so its own violation is
unambiguous.

```tla
ProbeReachesRewriteSupersession ==
    ~(PresentObj("rwA") /\ RawInputs \subseteq superseded)

ProbeReachesLegalHoldBlocksCompletion ==
    ~( ~PresentObj("doneR1") /\ PresentObj("dreqR1") /\ HeadDeletable
       /\ (CompleteIgnoresServedSet \/ ~ServesNow("s1"))
       /\ clock > 0
       /\ HeldInputServes("s1") )

ProbeReachesHorizonGatesDelete ==
    ~( \E o \in DataObjects :
         /\ HeadDeletable
         /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)
         /\ Bucket(o) = "b1"
         /\ PresentObj(o)
         /\ PresentObj("tombB1")
         /\ clock >= tombRetiredAt["b1"] + SweepRetentionWindow
         /\ (\A x \in head : Bucket(x) # "b1")
         /\ ~HeldObject(o, heldBuckets)
         /\ QueryPermits(o)
         /\ clock < tombRetiredAt["b1"] + sysgc.ph )

ProbeReachesDreqHoldBlocksSweep ==
    ~( (RefreshFailureSweepsAnyway \/ ~refreshFailed)
       /\ HeadReadable
       /\ PresentObj("dreqR1")
       /\ PresentObj("doneR1")
       /\ doneAt > 0
       /\ clock >= dreqHorizon
       /\ ~ServesAny("s1")
       /\ HeldInputServes("s1") )
```

`ProbeReachesLegalHoldBlocksCompletion` and `ProbeReachesHorizonGatesDelete`
each restate one real action's guard with every clause held except the one
under test negated, so a violation means a state exists where that clause is
the only thing stopping the action: `CompleteErasure` otherwise enabled but
for the legal-hold gate, and `RetentionSweep` otherwise enabled but for the
horizon not yet having passed.

## Results

All four violate (TLC exit 12), so all four behaviours are reachable at
`MaxClock = 3`:

| probe | behaviour | result |
|---|---|---|
| `ProbeReachesRewriteSupersession` | a rewrite and its supersession | violated, 62 states generated, 59 distinct |
| `ProbeReachesLegalHoldBlocksCompletion` | a legal hold that actually blocks a completion | violated, 3302 states generated, 1355 distinct |
| `ProbeReachesHorizonGatesDelete` | the protection horizon gating a delete | violated, 630 states generated, 334 distinct |
| `ProbeReachesDreqHoldBlocksSweep` | the .dreq hold (a legal hold blocking the request-marker sweep) | violated, 30041 states generated, 9798 distinct |

Each is `Error: Invariant <name> is violated.` with a counter-example trace;
state counts are TLC's own totals at the point it stopped on the first
violation, not a full state graph for the probe's cfg.
