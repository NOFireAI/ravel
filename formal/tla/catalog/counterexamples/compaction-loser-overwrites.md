# compaction-loser-overwrites

Negative control. Switch `CompactionLoserOverwrites = TRUE` makes a compactor
that loses the `CreateIfAbsent` race overwrite the winner's already-published
record with its own, later recomputation instead of reading the winner back and
adopting it. This breaks the `resolve_already_exists` contract
(`crates/ravel-maintain/src/publish.rs::resolve_already_exists`): the immutable
L1 record is mutated after publish.

Violated invariant: `CompactionRecordImmutable` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. State 2: `DoCommit` ingests an L0 input into the hour.
2. State 3: `DoTick` advances the clock past the maintenance seal, so the hour
   is compactable.
3. State 4: `DoCompact` publishes an L1 compaction record over the hour and
   wins the `CreateIfAbsent`, so `crec[H][g]` is now `used` and immutable.
4. State 5: `DoCommit` ingests a further input.
5. State 6: `DoCompactLoser` runs. With the switch set, the loser does not read
   the winner's record back; it overwrites `crec[H][g]` with its own
   recomputation, and the witness records `lastCompact.loserFired = TRUE` and
   `lastCompact.mutated = TRUE`.

At State 6 `lastCompact.loserFired` holds while `lastCompact.mutated` is also
true, so `CompactionRecordImmutable` (`loserFired => ~mutated`) is false: a loser
mutated a published immutable record.

## Why it is the right control

The crate resolves an already-exists on the losing side by reading the winner's
record back and adopting it, or failing closed, never by overwriting
(`crates/ravel-maintain/src/publish.rs::resolve_already_exists`, pinned by
`crates/ravel-maintain/tests/tombstone_race.rs::all_parts_already_exists_retry_retains_nothing_and_publishes`).
The default model gives the loser that outcome alphabet (adopt or fail closed);
this control forces the forbidden third outcome to show the immutability
invariant is load-bearing rather than vacuous.
