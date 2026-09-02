# Mutant: CAS succeeds on an absent key

This is an adversarial mutant of `RavelObjectStore.tla`, not a negative-control
switch: it edits the module text to delete a precondition, to prove the
invariant catches it. It is reverted after the run; the module in the tree is
the correct one.

## The edit

In `PutCasVersion`, delete the absent-key disjunct `(~store[k].present) \/`:

    PutCasVersion(k, v, c) ==
        IF (store[k].version # v)          \* was: (~store[k].present) \/ (store[k].version # v)
            THEN UNCHANGED storeVars
            ELSE PutOverwrite(k, c)

An absent key has version 0, so a CAS quoting version 0 now writes into a key
that was never created, while `CasResult` still classifies the absent key as
`PreconditionFailed`. This is the reviewer's original mutant: it used to stay
green because the MC client pre-classified freshness and only called the
operator on the branch it had already decided.

## The run and the TLC line

`scripts/check-tla.sh smoke -a common`, TLC exit 12 (invariant failure), with:

    Error: Invariant CasOutcomeMatchesEffect is violated.

The client now calls `PutCasVersion` unconditionally and records
`lastOp.outcome = CasResult(k, v)`. On an absent key the outcome is
`PreconditionFailed` but the store gained a record, so
`CasOutcomeMatchesEffect` (a non-Ok CAS leaves `after = before`) fails. The
invariant reads the store and the outcome, so the mutant can no longer hide.
