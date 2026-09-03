# commit-before-data

Negative control. `CommitBeforeData = TRUE` lets the commit record be
published while the flush is still in the `pinned` phase, so the record is
written before its data object exists.

Violated invariant: `NoCommitWithoutData` (safety, TLC exit 12).

Trace, in prose: a writer pins a flush and, without the data PUT, publishes
its commit record. The record key is present while the data key is absent,
which is the state the invariant forbids and the state a reader would
resolve into a fetch of an object that is not there.

The correct model orders the two PUTs, which is what
`crates/ravel-commit/src/publish.rs::publish` does: the data object first,
the record only after it returns.
