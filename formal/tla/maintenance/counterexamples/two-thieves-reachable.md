# Two thieves winning one version is reachable (F5)

`AtMostOneThiefWinsAVersion` asserts that at most one steal ever wins a given
observed claim version. The review found it vacuous at the shipped smoke bounds:
with `DeclaredLease = 1`, once a thief wins a version the claim cannot re-expire
within the `MaxV` write budget, so a second steal of that same observed version
is never enabled and the invariant guards a state the model cannot reach.

The fix re-bounds the smoke model to `DeclaredLease = 0` (a claim re-expires
after each steal), which brings the race into reach without growing the state
space enough to matter (smoke still finishes in about 11s).

## Probe

To prove the re-bound is what makes the invariant non-vacuous, a scratch-only
probe invariant was added to a `/tmp` copy of `MCCompactionClaims.tla` (never
committed) that asserts the race precondition never occurs: a worker still
observes a nonzero version that is already in `stealWonVers[u]`, while the claim
is present and expired and write budget remains, so a second winning steal of
that version is enabled.

```
NoTwoThievesRace ==
    ~(\E u \in Units : \E w \in Workers :
        /\ obsVer[w][u] # 0
        /\ obsVer[w][u] \in stealWonVers[u]
        /\ ClaimPresent(u)
        /\ Expired(u)
        /\ CanWrite)
```

Run against the correct model (every negative switch FALSE), varying only
`DeclaredLease`:

```
DeclaredLease = 0:  Error: Invariant NoTwoThievesRace is violated.   (exit 12)
DeclaredLease = 1:  Model checking completed. No error has been found. (exit 0)
```

The violating trace at `DeclaredLease = 0`:

```
State 3: Observe   obsVer = <<<<0>>, <<1>>>>   (worker 2 observes version 1)
State 4: Observe   obsVer = <<<<1>>, <<1>>>>   (worker 1 also observes version 1)
State 5: TimePass                              (claim expires)
State 6: Steal     stealWonVers = <<{1}>>      (a thief wins version 1)
State 7: TimePass                              (claim re-expires: only at lease 0)
```

At State 7 the claim is expired again while a worker still observes version 1,
which is now in `stealWonVers`, so a second steal of version 1 is enabled. At
`DeclaredLease = 1` the claim cannot re-expire after State 6 within budget, so
State 7's precondition is unreachable and the invariant is vacuous. The probe is
a one-shot reachability demonstration, not a committed control; the permanent
control is `negative/steal-ignores-cas.cfg`.
