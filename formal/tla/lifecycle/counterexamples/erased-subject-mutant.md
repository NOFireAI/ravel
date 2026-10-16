# Non-vacuity mutant: ErasedSubjectNeverServedAfterRequest

Proves `ErasedSubjectNeverServedAfterRequest` is not vacuously true: a behaviour
that leaves the subject servable after an erasure request is reachable and the
invariant catches it.

## Mutation

Behaviour edit (not a switch), applied to a scratch copy under `/tmp`. The
erasure request stops writing its `.dreq` marker, so the read-time erasure
filter `~PresentObj("dreqR1")` never engages:

```tla
-    /\ S!PutCreateIfAbsent("dreqR1", "dat")
+    /\ UNCHANGED storeVars
```

in `RequestErasure`; `erasureRequested' = erasureRequested \cup {"s1"}` still
fires. The run used the base `smoke.cfg`.

## Result

TLC exit 12. Exact line:

```text
Error: Invariant ErasedSubjectNeverServedAfterRequest is violated.
```

After the request, `s1 \in erasureRequested` while `dreqR1` is absent, so
`ServedRead("s1") = ServesAny("s1") /\ ~PresentObj("dreqR1")` stays true as long
as the head still names a raw input serving `s1`. The invariant reads the store
and the head (`ServedRead`), not a ghost field, so it fires. Restoring the
`.dreq` write makes smoke pass.
