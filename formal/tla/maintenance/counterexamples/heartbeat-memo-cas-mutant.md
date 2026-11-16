# Mutant: memo persistence self-reports Overwrite but performs a CAS (F2)

Reviewer mutant applied to a scratch copy of the ownership model (`/tmp`,
never the repo): `lastMaint` is given an extra self-reported `mode` field,
and `BrokenMemoCas` sets `mode |-> "Overwrite"` while still calling
`PutCasVersion(MemoKey(w), 0, MemoContent(w))`. The point is to check that
the invariant observes the store operation, not the write's own claim about
what mode it used.

`HeartbeatAndMemoNeverCas` reads only the store witness
(`verAfter > verBefore`, both read from the store around the call), never
`mode`. A CasVersion against a stale token is a no-op under the contract
(no-op on an absent key and on a version mismatch alike), so the stored
version does not advance and the witness catches it regardless of the
self-report.

Run: `negative/heartbeat-memo-cas.cfg` in the scratch tree (with the
`mode` field added).

```
Error: Invariant HeartbeatAndMemoNeverCas is violated.
State 2: <BrokenMemoCas ... of module MCMaintenanceOwnership>
/\ lastMaint = [ class |-> "memo",
  verBefore |-> 0,
  verAfter |-> 0,
  maxExcess |-> 0,
  mode |-> "Overwrite" ]
```

The violating state self-reports `mode |-> "Overwrite"` yet
`verAfter = verBefore = 0`: the store shows no write happened. A model that
trusted the self-report would read `mode |-> "Overwrite"` and pass, which is
exactly the tautology the F2 fix removes. The committed model carries no
`mode` field at all; the invariant depends only on the stored version delta.
