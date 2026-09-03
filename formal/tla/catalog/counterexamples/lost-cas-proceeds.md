# lost-cas-proceeds

Negative control. Switch `LostCasProceedsOnStaleRead = TRUE` makes a folder that
loses the version-matched HEAD CAS proceed on its stale read: it overwrites HEAD
with its own staged snapshot instead of re-reading HEAD and retrying. A
concurrent rival (`DoRivalFoldWin`) has already advanced HEAD to a higher
watermark, so the loser's write regresses the watermark below a commit the
winner published.

Violated invariant: `NoLiveCommitOmittedByLostCas` (safety, TLC exit 12).

## Trace shape (from the recorded run)

1. States 2 to 3: `DoTick` advances the clock so early hours fold-seal.
2. States 4 to 6: a folder folds to watermark 1 and its CAS wins, so a valid
   HEAD names the hour-1 snapshot.
3. State 7: `DoTick` seals the next hour, raising the reachable fold watermark.
4. States 8 to 9: the folder starts a second fold and writes its part, holding
   the base HEAD version it read at start.
5. State 10: `DoTick` advances the clock.
6. State 11: `DoRivalFoldWin` folds current state to the higher watermark, wins
   the version-matched CAS, and bumps the HEAD object version. The modeled
   folder's base version is now stale.
7. State 12: `DoFoldCas` runs. Its `won` test is false (the store version no
   longer matches its base), but with the switch set `staleWrite` is true, so it
   writes its stale snapshot to HEAD anyway, regressing the watermark below the
   rival's published commit.

At State 12 `head.wm` is below `maxValidWm` and a live commit at or below the
rival's watermark is missing from `head.entries`, so
`NoLiveCommitOmittedByLostCas` is false.

## Why it is the right control

The crate's HEAD CAS serializes racing folders: a loser's version-matched CAS
no-ops and it re-reads HEAD to retry, never overwriting with a stale staged set
(`crates/ravel-catalog/src/fold.rs::get_head` with `MAX_HEAD_CAS_ATTEMPTS`,
pinned by
`crates/ravel-catalog/src/fold.rs::two_concurrent_first_folds_race_head_cas_and_only_one_advances`).
`DoRivalFoldWin` is what makes the race reachable with a single modeled folder:
without a concurrent winner that advanced the watermark, a loser's staged set can
never be staler than HEAD. This control lets the loser take the forbidden write
to show the invariant is load-bearing.
