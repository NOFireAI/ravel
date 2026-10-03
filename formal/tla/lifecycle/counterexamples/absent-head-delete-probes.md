# Probes: retention and superseded sweeps now delete on an absent HEAD

Finding 1 (round four, issue #1122) changed `RetentionSweep` and
`SupersededSweep` from gating on `HeadDeletable` (`headState = "present"`) to
gating on `HeadReadable` (`headState # "unreadable"`), and changed their
head-empty checks from raw `head` to `EffectiveHead`, to match
`reachability.rs`'s `bucket_gate`/`object_gate`: both return `Covering::Clear`
(permit) on `HeadStatus::Absent` and block only on `HeadStatus::Unreadable`.
A guard relaxation can be vacuous if the newly-permitted branch is never
actually reached; each probe below restates "this sweep never deletes while
headState is absent" and TLC violating it proves the branch fires for real.

## Probe module

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, checked with
`SPECIFICATION Spec`, `VIEW View`, `INVARIANT TypeOK` plus one probe at a
time, `smoke.cfg`'s constants.

```tla
ProbeReachesRetentionDeleteUnderAbsentHead ==
    ~(headState = "absent" /\ lastGc.rule = "retention")

ProbeReachesSupersededDeleteUnderAbsentHead ==
    ~(headState = "absent" /\ lastGc.rule = "superseded")
```

`lastGc.rule` is set to `"retention"`/`"superseded"` only in the state
immediately produced by that sweep's own `GcWitness` call, and `headState` is
`UNCHANGED` across that step, so `headState = "absent"` in that state means
the sweep read HEAD as absent at the moment it deleted.

## Results

Both violate (TLC exit 12):

| probe | behaviour | result |
|---|---|---|
| `ProbeReachesRetentionDeleteUnderAbsentHead` | `RetentionSweep` deletes with `headState = "absent"` | violated, 11552 states generated, 3991 distinct |
| `ProbeReachesSupersededDeleteUnderAbsentHead` | `SupersededSweep` deletes with `headState = "absent"` | violated, 17438 states generated, 5782 distinct |

The retention trace: `RetireBucket`, a tick, `DropRetiredBucketFromHead`
(`head` becomes `{}`), `SetHeadState("absent")`, then `RetentionSweep` fires
and deletes `raw1` while `headState = "absent"`. The superseded trace reaches
the same condition through `PerformRewrite` and `SupersededSweep` instead.

## A companion constraint on `SetHeadState`

Relaxing the sweeps to trust an absent read exposed that `SetHeadState` let
`headState` diverge from `head` in a way no real read can: on this store, a
GET of an existing key never returns absent (`docs/object-store-contract.md`'s
consistency guarantee), so a truthful absent read is only possible when `head`
itself already names nothing. Before this was constrained,
`HeadNamedObjectNeverDeletedBySupersededSweep` (restated against
`EffectiveHead`, see LifecycleGC.tla) was reachably violated by: `head =
{"raw1"}`, `SetHeadState("absent")` with `head` UNCHANGED, `SupersededSweep`
deleting `raw1` under the absent read, then `SetHeadState("present")` flipping
back to the same unchanged `head = {"raw1"}` — asserting `raw1` was reachable,
deleted, and reachable again with nothing having actually deleted it in the
world `head` describes. `SetHeadState(s)` now requires `s = "absent" => head =
{}`, which matches the real read semantics and removes that false counter-
example without narrowing anything Finding 1 needs: both probes above still
reach their target sweep with the constraint in place.
