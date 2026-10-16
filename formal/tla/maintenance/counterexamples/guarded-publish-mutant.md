# Mutant: guarded publish drops its HoldsClaim check (F4)

Reviewer mutant applied to a scratch copy of `CompactionClaims.tla`
(`/tmp`, never the repo): the `HoldsClaim(w, u)` conjunct is removed from
`GuardedPublish`, so it can fire when the worker no longer holds the claim.

`LostClaimNeverPublishesThroughGuardedPath` now bites because the witness
derives `held` from the store rather than a literal: `held |-> HoldsClaim(w, u)`
reads the stored claim owner and version, so a publish by a non-holder
records `fired = TRUE` with `held = FALSE`.

Run: `MCCompactionClaims.smoke.cfg` (all negative switches FALSE).

```
Error: Invariant LostClaimNeverPublishesThroughGuardedPath is violated.
```

The same behaviour is available as the permanent negative control
`negative/guarded-publish-ignores-claim.cfg` (switch `GuardIgnoresClaim`).
Before F4, `held` was the literal `TRUE`, so the invariant read
`TRUE => TRUE` and stayed green.
