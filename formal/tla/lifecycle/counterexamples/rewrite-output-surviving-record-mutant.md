# Non-vacuity mutant: RewriteOutputsAreInputsMinusErased (kept direction)

Proves the "kept" half of `RewriteOutputsAreInputsMinusErased` is not vacuously
true (finding 3). Before this fix the model had one record, `rec1`, of one
subject, `s1`, and `r1` erased `s1`: every record in scope was erased, so the
right-hand side of the `<=>` was a state-independent FALSE for every reachable
state, and no behaviour could ever violate the "a non-erased record's subject
stays served" direction. The fix adds a second record `rec2` of a second
subject `s2` that `r1` never erases, so a rewrite must keep it.

## Mutation

Behaviour edit (not a switch), applied to a scratch copy under `/tmp`. The
rewrite output drops the surviving record outright, regardless of the
`RewriteKeepsErasedRecords` switch:

```tla
-    IN IF RewriteKeepsErasedRecords
-           THEN inRecs
-           ELSE { r \in inRecs : RecordSubject(r) \notin ErasedBy(AppliedReqs("rwA")) }
+    IN IF RewriteKeepsErasedRecords
+           THEN inRecs
+           ELSE { r \in inRecs : RecordSubject(r) \notin ErasedBy(AppliedReqs("rwA")) } \ {"rec2"}
```

Nothing else changed; the run used the base `smoke.cfg` with every switch at
its shipped value.

## Result

TLC exit 12. Exact line:

```text
Error: Invariant RewriteOutputsAreInputsMinusErased is violated.
```

Two states: `Init`, then `PerformRewrite`. The rewrite materializes `rwA`
without `rec2`, so `ServesSubject("rwA", "s2")` is FALSE while `raw1` (a
predecessor) still serves `s2` and `s2` was never erased -- the right-hand
side of the `<=>` is TRUE and the left-hand side is FALSE. Restoring the
dropped record makes the invariant hold again (smoke passes), which is the
non-vacuity argument for the previously-unfalsifiable direction.
