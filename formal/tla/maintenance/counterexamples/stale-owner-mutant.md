# Mutant: MarkCompleted overwrites instead of CAS (F1)

Reviewer mutant applied to a scratch copy of `CompactionClaims.tla`
(`/tmp`, never the repo): in `MarkCompleted` the store call
`PutCasVersion(ClaimKey(u), v, <<"c", w, "done">>)` becomes
`PutOverwrite(ClaimKey(u), <<"c", w, "done">>)`, with the witness
expression untouched.

Because the witness now reads the store after the operator runs
(`afterVer |-> store'[ClaimKey(u)].version`,
`afterContent |-> store'[ClaimKey(u)].content`), a stale-token completion
reports `ok = FALSE` yet the stored version moves, so
`~ok => afterVer = beforeVer` fails.

Run: `MCCompactionClaims.smoke.cfg` (all negative switches FALSE).

```
Error: Invariant StaleOwnerCannotOverwriteNewerClaim is violated.
```

Before F1, the same mutant left the model green: the witness self-reported
`afterVer |-> IF ok THEN versionCounter + 1 ELSE ClaimVer(u)`, which never
observed the illegitimate overwrite.
