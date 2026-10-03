# Counterexample: the list action never progresses

Config: `negative/list-never-progresses.cfg` (`ListStalls = TRUE`,
`SPECIFICATION FairSpec`, `PROPERTY ListEventuallyComplete`). Expected:
`ListEventuallyComplete` violated, TLC exit 13 (temporal).

Observed TLC line:

    Error: Temporal properties were violated.

TLC 1.7.4 prints no property name on a temporal violation, so the harness runs
a generated config declaring only `ListEventuallyComplete`. A violation of that
generated config can only be that property, and a wrong `property=` name would
fail to resolve the operator instead of reporting exit 13.

## What the switch breaks

`ListEventuallyComplete` (`listState.active ~> (snapshot \subseteq Delivered)`)
holds under one fairness assumption: weak fairness on `MCListProgress`, the
list-delivery action. The switch adds `~ListStalls` as a guard on
`DoListProgress`, so with `ListStalls = TRUE` the fair action is never enabled.
Weak fairness on an action that is never enabled imposes nothing, so a started
listing may stutter forever without delivering the keys it snapshotted.

## Why it is a real bug if it ever held

A store whose listing can begin and then never make progress never satisfies a
consumer waiting to see every key present at snapshot time; a paginated scan
that stalls is a liveness failure, not merely slow. TLC reaches a state with a
present key, begins a listing, and finds an infinite stuttering suffix that is
fair yet never completes delivery.

The correct model (switch FALSE) passes under `exhaustive.cfg`: with the fair
`MCListProgress` action enabled, every started listing eventually delivers its
snapshot.
