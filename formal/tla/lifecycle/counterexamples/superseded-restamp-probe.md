# Probes: a publish must not re-stamp an already superseded input

`supersededAt[i]` is the horizon `SupersededSweep` and
`NoDeleteInsideProtectionWindow` both read for object `i`. Both publishing
actions used to write it for every input they resolved, whether or not the
object was already in `superseded`, so a second publish over an input another
publish had already superseded moved that input's protection horizon forward.
At `MaxClock = 2` with `ProtectionHorizon = 1` a re-stamp at clock 2 makes
`EventuallySwept`'s antecedent permanently false for the re-stamped object, and
`NoDeleteInsideProtectionWindow` then checks the later timestamp instead of the
supersession the catalog actually recorded.

The fix keeps the first timestamp in both actions: `IF i \in <resolved inputs>
/\ i \notin superseded THEN clock ELSE supersededAt[i]`.

## The probes

A scratch module under `/tmp`, `EXTENDS MCLifecycleGC`, with `smoke.cfg`'s
constants, `SPECIFICATION Spec`, `VIEW View`, and exactly ONE `PROPERTY` per
run so a violation can only be the named property. TLC 1.7.4, `-workers 2
-Xmx2g`. Each is an action property asserting the re-stamp is impossible, so a
VIOLATED result means the re-stamp is reachable.

```tla
NoRestampOf(i) == (i \in superseded) => supersededAt'[i] = supersededAt[i]
AllKeepTheirFirstStamp == \A i \in SupersededCandidates : NoRestampOf(i)

ProbeNoRewriteRestamp ==
    [][ (\E id \in RewriteIds : rwPhase[id] = "listed" /\ rwPhase'[id] = "done")
            => AllKeepTheirFirstStamp ]_vars

ProbeNoCompactionRestamp ==
    [][ (cmpPhase = "listed" /\ cmpPhase' = "done")
            => AllKeepTheirFirstStamp ]_vars
```

`rwPhase[id]` moving from `listed` to `done` is `PublishRewrite(id)` and
nothing else; `cmpPhase` moving from `listed` to `done` is `PublishCompaction`
and nothing else, so each probe attributes the re-stamp to one action.

## `PublishRewrite`: reachable in the shipped configuration

```text
Error: Action property ProbeNoRewriteRestamp is violated.
```

TLC exit 13, trace 11 states: `RequestErasure`, `StartRewrite("A")`,
`ExpireLease`, `StartRewrite("B")`, `PublishRewrite("A")`, `RetireBucket`,
`Tick`, `DropRetiredBucketFromHead`, `RetentionSweep`, `PublishRewrite("B")`.

Both identities resolve `{raw1}` at their listing step, so both aim at `rwA`.
A publishes it and stamps `supersededAt["raw1"] = 0`. Retention then deletes
`rwA` (the tombstone was written before A published, HEAD no longer names the
bucket, and the protection horizon has passed at clock 1). B's publish now
finds its target absent, so its `CreateIfAbsent` succeeds a second time rather
than converging, and the old expression re-stamped `raw1` to clock 1.

This contradicts the expectation that `LiveInputs` excluding superseded objects
makes the path unreachable: that exclusion holds at the LISTING step, and the
re-stamp is written at the PUBLISH step, after `superseded` has moved. So
`PublishRewrite` needs the guard, not only `PublishCompaction`.

## `PublishCompaction`: unreachable in the shipped configuration, reachable with either guard off

Under `smoke.cfg`'s constants `ProbeNoCompactionRestamp` HOLDS: TLC exit 0 over
the complete graph, 25339943 states generated, 3773271 distinct, depth 31 (the
figures for the model as it stood before the fix). The scenario needs the
compactor to list a bucket whose raw input a rewrite has already superseded,
and the shipped model has no such state: `StartCompaction` refuses while a
rewrite output is present, the only action that removes a rewrite output is
`RetentionSweep`, that needs the tombstone, and `StartCompaction` refuses a
tombstoned bucket. The tombstone is never removed while the raw input is still
there, because `SweepTombstone` requires the bucket's data objects to be gone.

Turning off either of the two things that close it makes the re-stamp
reachable, so the guard is doing real work rather than covering a dead branch:

- `CompactionIgnoresRewrite = TRUE` (the negative control for
  `AtMostOneLiveRecordSetServed`): `Error: Action property
  ProbeNoCompactionRestamp is violated.`, exit 13, trace 8 states:
  `RequestErasure`, `StartRewrite`, `PublishRewrite`, `Tick`, `ExpireLease`,
  `StartCompaction`, `PublishCompaction`. `supersededAt["raw1"]` moves from 0
  to 1 on the last step.
- `SerializeCompactionAndRewrite = FALSE` (the residual window `cdce1722`
  documents): same property violated, exit 13, trace 9 states, the compactor
  listing while the rewrite is in flight.

## After the fix

Every probe holds, each over the complete state graph with `0 states left on
queue`:

| probe | constants | exit | states generated | distinct | depth |
|---|---|---|---|---|---|
| ProbeNoRewriteRestamp | shipped | 0 | 25425839 | 3786411 | 30 |
| ProbeNoCompactionRestamp | shipped | 0 | 25425839 | 3786411 | 30 |
| ProbeNoCompactionRestamp | CompactionIgnoresRewrite = TRUE | 0 | 29810787 | 4471337 | 32 |
| ProbeNoCompactionRestamp | SerializeCompactionAndRewrite = FALSE | 0 | 70309590 | 10207853 | 32 |

The two switch-off runs are the interesting ones: the interleavings that
produced the re-stamp are still reachable there (that is what makes the
negative control fire its own invariant), and the stamp now survives them.
