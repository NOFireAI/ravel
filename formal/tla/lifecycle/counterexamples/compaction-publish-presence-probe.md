# Probe: a compaction publish stamps only present inputs

`StartCompaction` moves the pass to `"listed"` but recorded nothing about which
inputs it saw. `PublishCompaction` then superseded a static `RawInputs` and
stamped `supersededAt` for it, with no check that the input was still there. So
a `RetentionSweep("raw1")` firing while the pass sat in `"listed"` (the sweep
consults the tombstone and the HEAD, never `cmpPhase`) deleted the raw input,
and the later publish superseded and stamped an object no longer present. The
shipped compactor cannot do that: `compact_bucket_scoped` reads every input to
build its output, so an input swept between listing and publish fails the pass
rather than publishing over the gap.

The fix snapshots the resolved inputs in `cmpInputs` at `StartCompaction` and
guards `PublishCompaction` with `\A o \in cmpInputs : PresentObj(o)`, applying
`superseded'` and `supersededAt'` over `cmpInputs` only after the check.
`CancelCompaction` returns a pass whose recorded input vanished to idle so the
guard does not strand it in `"listed"`.

## The probe

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, with `smoke.cfg`'s
constants, `SPECIFICATION Spec`, `VIEW View`, and exactly one `PROPERTY` so a
violation can only be the named property. TLC 1.7.4, `-workers 2 -Xmx2g`.
`cmpPhase` moving `"listed"` to `"done"` is `PublishCompaction` and nothing
else, so the action property attributes the stamp to that one action.

```tla
ProbeCompactionStampsPresentInput ==
    [][ (cmpPhase = "listed" /\ cmpPhase' = "done")
          => (\A o \in RawInputs : PresentObj(o)) ]_vars
```

`RawInputs` is the set the pre-fix publish superseded and stamped, so a
VIOLATED result means the publish stamped an absent input.

## Before the fix: reachable in the shipped configuration

```text
Error: Action property ProbeCompactionStampsPresentInput is violated.
```

TLC exit 13, `18989 states generated, 6862 distinct states found`, depth 8.
Trace (8 states):

1. `<Initial predicate>`
2. `Tick`
3. `StartCompaction`
4. `RetireBucket`
5. `Tick`
6. `DropRetiredBucketFromHead`
7. `RetentionSweep` (deletes `raw1`: tombstone written, HEAD no longer names the
   bucket, protection horizon elapsed at clock 1)
8. `PublishCompaction`

At the last step `superseded` becomes `{"raw1"}` and `supersededAt["raw1"]`
moves from 0 to 1 while `raw1` is already absent. This is the shipped
configuration (every switch at its base value), not a switch-off branch: the
retention delete of a raw input the compactor listed is reachable without any
control flipped.

## After the fix: holds over the complete graph

Under `smoke.cfg`'s constants `ProbeCompactionStampsPresentInput` HOLDS: TLC
exit 0 over the complete state graph, `24004521 states generated, 3587643
distinct states found, 0 states left on queue`, depth 30. `PublishCompaction`
now requires `raw1` present, so the step that produced the trace above is
disabled; `CancelCompaction` fires from that state instead and the pass returns
to idle.

| probe | constants | exit | states generated | distinct | depth |
|---|---|---|---|---|---|
| ProbeCompactionStampsPresentInput | shipped | 13 (before) | 18989 | 6862 | 8 |
| ProbeCompactionStampsPresentInput | shipped | 0 (after) | 24004521 | 3587643 | 30 |
