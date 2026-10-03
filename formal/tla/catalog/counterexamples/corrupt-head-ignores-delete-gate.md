# corrupt-head-ignores-delete-gate

Negative control. Switch `DeletePathIgnoresUnreadableHead = TRUE` drops the
`head.status` guard from both HEAD-gated delete passes, letting a delete
proceed while HEAD is corrupt or unsupported.

Violated invariant: `CorruptHeadFailsClosedOnDeletePaths` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. State 2: ingest publishes an L0 commit (`rA` lands in hour 0).
2. State 3: the clock advances.
3. State 4: a compaction over hour 0 publishes, superseding `rA`.
4. State 5: `DoUnsupportedHead` fires (one-shot), writing an unsupported HEAD
   (`head.status' = "unsupported"`, `unsupportedUsed' = TRUE`).
5. State 6: the clock advances past the protection horizon.
6. State 7: `DoSweepSuperseded` runs. With the guard dropped, it deletes `rA`
   from `l0[0]` even though HEAD is unsupported.

At State 7, `lastDelete.happened = TRUE` and `lastDelete.headStatus =
"unsupported"`, so `CorruptHeadFailsClosedOnDeletePaths` is false.

## Why it is the right control, and the reconnaissance correction

The real gate, `crates/ravel-maintain/src/reachability.rs::SnapshotReachability::ensure_head`,
fails the whole superseded-input sweep closed when HEAD cannot be read as
valid or absent; `crates/ravel-maintain/src/sweep.rs::sweep_unreferenced_catalog_objects::read_head_reference`
does the same for the unreferenced-part sweep, which has no covering-part or
entry-identity concept of its own. A prior round of this note cited
`SnapshotReachability::bucket_gate`, which is the retention path's gate, not
either delete pass this control exercises; the citation above corrects that.
This control drops the guard on the corrupt/unsupported-HEAD trigger shared
by both sweeps to show it is load-bearing rather than vacuous.
