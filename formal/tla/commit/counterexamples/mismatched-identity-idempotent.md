# mismatched-identity-idempotent

Negative control. `SkipHashCompare = TRUE` makes the commit PUT's
`AlreadyExists` path skip the content-hash comparison, so a second flush that
reuses a commit identity with different content is accepted as an idempotent
success instead of being detected.

Violated invariant: `OneIdentityOneContent` (safety, TLC exit 12).

Trace, in prose: a flush commits content c1 and crashes, retiring its
identity. The identity is reused with content c2. The commit PUT finds the
key present; with the comparison skipped the writer treats that as its own
record. The witness then holds a stored content that differs from the content
the caller pinned, which is exactly one identity bound to two contents.

The correct model reads the winner back and compares
(`crates/ravel-commit/src/publish.rs::resolve_already_exists`); a mismatch is
`PublishError::SplitBrain` and the shard stops rather than proceeding.
