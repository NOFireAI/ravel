# Probe: completion never enabled under a bucket hold (finding 2)

Proves the finding 2 gap is closed: `CompleteErasure` is never `ENABLED` while
bucket `b1` is under legal hold and some live `DataObjects` member is present
in it, regardless of whether that member's content still serves the erased
subject.

## Probe module

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, adding:

```tla
ProbeNoCompletionUnderBucketHold ==
    ("b1" \in heldBuckets /\ (\E o \in DataObjects : Bucket(o) = "b1" /\ PresentObj(o)))
        => ~(ENABLED CompleteErasure)
```

checked as the sole non-`TypeOK` invariant, `SPECIFICATION Spec`, `VIEW View`,
every switch at `smoke.cfg`'s shipped value, `MaxClock = 2`.

## Result against a scope-only widening (insufficient)

Widening `HeldInputServes` from `RawInputs` to `DataObjects` while keeping its
`ServesSubject(o, s)` conjunct does not close the gap: `RewriteOutputContent`
computes `rwA`'s served set from a request set (`AppliedReqs("rwA") ==
Requests`) that is constant regardless of whether `RequestErasure` ever ran, so
a correctly computed `rwA` never serves `"s1"` and `ServesSubject("rwA",
"s1")` is always `FALSE`. The raw input's own content never changes, so
`ServesSubject("raw1", "s1")` is always `TRUE`; the old, `RawInputs`-only
predicate was therefore already equivalent to a content-blind "held and
present" check for `raw1`, by coincidence, while the same requirement made a
held `rwA` invisible to the gate. TLC exit 12 with this scope-only widening:

```text
Error: Invariant ProbeNoCompletionUnderBucketHold is violated.
```

8-state trace ending: `raw1` absent (swept), `rwA` present with
`objContent["rwA"] = {"rec2"}` (correctly not serving `"s1"`), `heldBuckets =
{"b1"}`, `dreqR1` present, `doneR1` absent, `clock = 2`, `headState =
"present"` -- `CompleteErasure`'s guard is fully satisfied there (464571
states not reached; run stopped at the first violation, 24872 states
generated, 7234 distinct states found).

## Result against the shipped fix (content-blind, `DataObjects`)

`HeldInputServes` drops the `ServesSubject` conjunct, matching
`bucket_is_held`'s and `chain_groups_held_by_legal_hold`'s own content-blind,
per-live-key gating. TLC exit 0. Exact line:

```text
Model checking completed. No error has been found.
```

464571 states generated, 79358 distinct states found, 0 left on queue, depth
19 (full state graph for this probe's cfg, not a bound run).
