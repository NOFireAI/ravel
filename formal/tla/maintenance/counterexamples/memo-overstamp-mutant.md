# Mutant: seed clamps each entry against the global max snapshot (F8)

Reviewer mutant applied to a scratch copy of the ownership model (`/tmp`,
never the repo): in `SeedMemo` the per-entry clamp
`ev == IF s.verU < s.snapNs THEN s.verU ELSE s.snapNs` (each entry clamped to
its OWN snapshot) becomes a clamp to the largest snapshot across all valid
entries:

```
gmax == Max({ memoSnap[y].snapNs : y \in valid })
ev   == IF s.verU < gmax THEN s.verU ELSE gmax
```

This is the aggregate reading the review flagged: a Max over all snapshots
rather than a per-entry bound. An entry whose own snapshot is smaller than
`gmax` can then read fresher than its own source, which is exactly the gap
the per-entry clamp closes.

Run: `MCMaintenanceOwnership.smoke.cfg` in the scratch tree (all negative
switches FALSE, so the mutated correct `SeedMemo` is the only seed action).

```
Error: Invariant MemoNeverExtendsFreshnessPastSnapshot is violated.
State 3: <FutureEntry ... of module MaintenanceOwnership>
...
/\ lastMaint = [class |-> "seed", verBefore |-> 0, verAfter |-> 0, maxExcess |-> 1]
```

`maxExcess |-> 1` is a seeded entry reading one tick past its own snapshot,
because a sibling with a larger snapshot raised `gmax`. The committed
per-entry clamp keeps `maxExcess =< 0` for every reachable seed. This is a
distinct weakening from the committed `memo-overstamp` control, which skips
the clamp entirely; this one keeps a clamp but against the wrong bound, and
the invariant still catches it because it is stated per entry, not as an
aggregate.
