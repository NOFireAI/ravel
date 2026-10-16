# compaction-loser-overwrites

Negative control. Switch `CompactionLoserOverwrites = TRUE` makes a compactor
that loses the `CreateIfAbsent` race overwrite the winner's already-published
record with its own, later recomputation instead of reading the winner back and
adopting it. This breaks the `resolve_already_exists` contract
(`crates/ravel-maintain/src/publish.rs::resolve_already_exists`): the immutable
L1 record is mutated after publish.

Violated invariant: `CompactionRecordImmutable` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. State 2: `DoCommit` ingests `rA` into hour 0's L0 set.
2. State 3: `DoCommit` ingests `rA` into hour 1's L0 set.
3. State 4: `DoTick` advances the clock past the maintenance seal, so hour 0 is
   compactable.
4. State 5: `DoCompact` publishes an L1 compaction record over hour 0 and wins
   the `CreateIfAbsent`, so `crec[0][g1] = [used |-> TRUE, in |-> {rA},
   out |-> {rA}, at |-> 1]`.
5. State 6: `DoCompactLoser` runs on hour 0. With the switch set, the loser
   does not read the winner's record back; it recomputes from `l0[0]` and
   overwrites `crec[0][g1]` with its own output, and the witness records
   `lastCompact = [loserFired |-> TRUE, outcome |-> "overwrite"]`.

At State 6 `lastCompact.loserFired` holds while `lastCompact.outcome =
"overwrite"`, so `CompactionRecordImmutable` (`loserFired => outcome #
"overwrite"`) is false: a loser mutated a published immutable record.

Recorded run: 971 states generated, 532 distinct states found, depth 7 of
search, exit 12.

## Why it is the right control

The crate resolves an already-exists on the losing side by reading the winner's
record back and adopting it, or failing closed, never by overwriting
(`crates/ravel-maintain/src/publish.rs::resolve_already_exists`, pinned by
`crates/ravel-maintain/tests/tombstone_race.rs::all_parts_already_exists_retry_retains_nothing_and_publishes`).
The default model gives the loser that outcome alphabet (adopt or fail closed);
this control forces the forbidden third outcome to show the immutability
invariant is load-bearing rather than vacuous.
