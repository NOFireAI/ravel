# guarded-publish-ignores-claim

Switch: `GuardIgnoresClaim = TRUE`. Expected: `LostClaimNeverPublishesThroughGuardedPath` violated (exit 12).

Trace: worker 1 acquires the claim of unit 1 and a part is put. Time passes so
the claim expires, and worker 2 steals it, moving the stored owner and version
away from worker 1. The broken `BrokenGuardedPublish` step then lets worker 1
publish through the checkpoint path even though it no longer holds the claim.
The witness records `held |-> HoldsClaim(1, 1)`, read from the store, which is
now FALSE, so `lastGuarded.fired => lastGuarded.held` fails.

Why it matters: the cancellation checkpoint exists so a worker that has lost its
claim abandons the run instead of publishing. If the guard is skipped, a paused
stale worker resumes and publishes after another worker took over. The published
record is still correct only because it is CreateIfAbsent; the guard is the
belt-and-suspenders the design promises, and this control proves the model
observes it rather than a self-reported literal.
