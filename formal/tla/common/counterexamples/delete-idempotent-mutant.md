# Mutant: delete of an absent key changes observable state

An adversarial mutant of `RavelObjectStore.tla`, reverted after the run.

## The edit

Make `Delete` stamp a tombstone version onto the key instead of leaving an
absent key untouched, so a delete of an absent key changes the store record:

    Delete(k) ==
        /\ store' = [store EXCEPT ![k] =
                        [present |-> FALSE, content |-> NoContent, version |-> versionCounter + 1]]
        /\ versionCounter' = versionCounter + 1
        /\ UNCHANGED <<lastModified, uploads, listState>>

## The run and the TLC line

`scripts/check-tla.sh smoke -a common`, TLC exit 1 (invariant failure), with:

    Error: Invariant DeleteIdempotent is violated.

`DeleteIdempotent` requires that a delete whose pre-state key was absent leaves
`lastOp.after = lastOp.before`. Deleting an absent key at init moves its
version from 0 to `versionCounter + 1`, so `after # before` and the invariant
fails at depth 1. This pins the contract clause that a delete of an absent key
is a total no-op, and that the monotonic version counter, being global, is what
makes create/delete/create mint a fresh version rather than reusing one.

The correct model sets the key to `EmptyRec` and leaves `versionCounter`
unchanged, so an absent-key delete is observably a no-op.
