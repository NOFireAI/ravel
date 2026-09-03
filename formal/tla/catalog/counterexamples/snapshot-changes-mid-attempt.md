# snapshot-changes-mid-attempt

Negative control. Switch `SnapshotChangesMidAttempt = TRUE` enables
`DoQueryTamper`, which removes an entry from the query's pinned set in the
middle of one attempt.

Violated invariant: `PinnedSnapshotStableWithinAttempt` (safety, TLC exit 12).

## Trace shape

1. A fold commits a valid HEAD with a non-empty entry set.
2. `DoQueryResolve` pins the snapshot: `qy.phase = "pinned"`, and both
   `qy.pinned` and `qy.pinnedAtAttempt` hold the resolved view.
3. `DoQueryTamper` fires, dropping one entry from `qy.pinned` while the attempt
   is still open, so `qy.pinned # qy.pinnedAtAttempt`.

At that state `PinnedSnapshotStableWithinAttempt` is false.

## Why it is the right control

A query pins its snapshot once per attempt and reads it to completion
(`crates/ravel-query/src/engine.rs::QueryEngine::resolve_snapshot_with_retry`).
The immutable-segment MVCC rule means a pinned set never changes underneath a
running attempt; a re-resolve starts a new attempt with a fresh pin. The switch
models the forbidden case of the pinned set mutating in place.
