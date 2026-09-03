# Mutant: silent unconditional claim delete (F3)

Reviewer mutant applied to a scratch copy of `MCCompactionClaims.tla`
(`/tmp`, never the repo): a new action is added to `MCNext` that deletes a
present claim key and touches no witness.

```
SilentDeleteClaim(u) ==
    /\ ClaimPresent(u)
    /\ Delete(ClaimKey(u))
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, firstRecord, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, lastGuarded, stolen>>
```

`NoUnconditionalClaimDelete` now reads the store: the claim key drops from
present to absent while the `claimBorn` latch (set by the CreateIfAbsent
that first created it) stays set, so `claimBorn[u] => ClaimPresent(u)`
fails.

Run: `MCCompactionClaims.smoke.cfg` (all negative switches FALSE).

```
Error: Invariant NoUnconditionalClaimDelete is violated.
```

Before F3, the invariant was `~claimDeleted` over a ghost that this mutant
never sets, so it stayed green.
