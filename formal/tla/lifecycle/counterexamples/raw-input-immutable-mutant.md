# Non-vacuity mutant: RawInputContentAssumedImmutable

Proves `RawInputContentAssumedImmutable` is not vacuously true: a behaviour
that changes a raw input's stored content exists and the invariant catches
it, even though no such behaviour is part of the shipped `Next`.

## Mutation

Behaviour edit (not a switch), applied to a scratch copy under `/tmp`. A new
action is added and disjuncted into `Next`:

```tla
MutateRawInput ==
    /\ PresentObj("raw1")
    /\ objContent["raw1"] # {}
    /\ objContent' = [objContent EXCEPT !["raw1"] = {}]
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, variantKey>>
    /\ NoGc
```

Nothing else changed; the run used the base `smoke.cfg` with every switch at
its shipped value and the full smoke invariant list, including
`RawInputContentAssumedImmutable`.

## Result

TLC exit 12 at depth 4. Exact line:

```text
Error: Invariant RawInputContentAssumedImmutable is violated.
```

`MutateRawInput` clears `objContent["raw1"]` from `Init`'s `{"rec1","rec2"}`
to `{}`, so `objContent["raw1"] = InitContent("raw1")` fails immediately.

## Clean pass

The unmutated model (no `MutateRawInput` action, same `smoke.cfg`) passes
with `RawInputContentAssumedImmutable` in the invariant list:

```text
check-tla: lifecycle/MCLifecycleGC smoke: PASS  states=276015 distinct=50102 depth=21 3s
```

Restoring the shipped `Next` (no raw-input mutation reachable) makes the
invariant hold again, which is the non-vacuity argument: the invariant fails
exactly when the assumption it pins is false, and holds whenever the model
respects it.
