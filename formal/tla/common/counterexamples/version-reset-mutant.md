# Mutant: Delete resets the version counter

What was broken: in a scratch copy of `RavelObjectStore.tla`, `Delete(k)`
was changed to set `versionCounter' = 0` instead of leaving the counter
unchanged. This is the shape of a backend whose version tokens restart
after a delete, so a create after a delete can hand out a version the
deleted key already carried, and a client holding the pre-delete token
could then CAS successfully against a different object.

Which invariant catches it: `VersionsNeverReused`. The model keeps two
ghosts, the set of versions the counter ever minted and the number of
mints. A counter that goes backwards mints a value already in the set, so
the set falls behind the count. The other invariants stay green on this
mutant, including `CasOutcomeMatchesEffect`, because a stale token that
happens to equal the re-minted version is, by the model's own bookkeeping,
the current version.

Trace, in prose: client 1 creates `k1` (version 1). Client 1 deletes `k1`;
the mutant resets the counter to 0. Client 2 creates `k2`; the counter
mints version 1 again. The minted set is `{1}` while the mint count is 2.

Observed with the harness's TLC command line on `smoke.cfg` against the
mutated copy:

```
Error: Invariant VersionsNeverReused is violated.
```

TLC exit 12 (invariant failure). The correct module is unchanged by this
experiment; the copy was discarded.
