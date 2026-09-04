# Equivalence check: store-grounded vs witness-derived wv (F2)

`DoPublish`'s `wv` (the divergence-branch decision) read the `firstRecord`
witness instead of the store's actual record content, unlike
`resolve_already_exists` in Rust, which resolves from the object it just
read. The fix grounds `wv` in `ContentOf(rk)` in both `CompactionClaims.tla`
and `MaintenanceOwnership.tla`. Two scratch copies (`/tmp`, never the repo)
established whether this fix changes any TLC-observable behavior.

Old formula:

```
wv == IF rp /\ firstRecord[u] # NoRec THEN firstRecord[u][2] ELSE v
```

New formula:

```
wv == IF rp THEN ContentOf(rk)[2] ELSE v
```

`ClaimGrantsNoPublicationAuthority` (`Present(RecordKey(u)) =>
ContentOf(RecordKey(u)) = firstRecord[u][2]`) already forces the two
formulas' inputs to agree in every state that invariant passes, so under
the shipped invariant suite no TLC run can tell them apart. To test the
formulas outside that guard, a probe config
(`negative/claim-as-publication-authority.cfg` with
`ClaimGrantsNoPublicationAuthority` dropped from `INVARIANT`) was run
exhaustively against both scratch copies, with `ClaimIsPublicationAuthority
= TRUE` enabling `BrokenClaimPublish`: the one MC mutant that can write the
record without updating `firstRecord'` (an overwrite of an already-present
record), the exact desync the store-grounded fix is meant to defend
against.

Old formula, probe config:

```
87866482 states generated, 15743105 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 16.
```

New formula, probe config:

```
87866482 states generated, 15743105 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 16.
```

Both runs completed with no invariant violation and identical generated,
distinct, and depth figures. The fix does not change reachable behavior
even under the one mutant designed to desync `firstRecord` from the store
and with the guarding invariant removed: `BrokenClaimPublish` only updates
`firstRecord'` on the record's absent-to-present transition, and every
later action that could exploit a stale `firstRecord` runs behind a claim
or ownership guard this small model's write budget does not let both
mutate the record twice and still exercise a further publish call within
the same run.

The change is correctness-model hygiene, not a reachable-behavior fix: it
makes `wv` match `resolve_already_exists`'s store-grounded read instead of
relying on the pre-existing invariant to keep the two readings in sync.
