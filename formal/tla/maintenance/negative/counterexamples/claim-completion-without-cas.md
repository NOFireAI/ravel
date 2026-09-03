# claim-completion-without-cas

Switch: `CompletionOverwrite = TRUE`. Expected: `StaleOwnerCannotOverwriteNewerClaim` violated (exit 12).

Trace: worker 1 acquires the claim of unit 1 (token version `v1`). Time passes,
the claim expires, and worker 2 observes and steals it, bumping the claim to a
new version and leaving worker 1's token `v1` stale. Worker 1 then takes the
broken `BrokenComplete` step: instead of a CasVersion on `v1` (which would fail),
it Overwrites the claim with `state = done`. The witness records
`ok = FALSE` (its token no longer matches) yet `afterVer != beforeVer` (the store
changed), so the clause `~ok => afterVer = beforeVer` fails.

Why it matters: a stale owner completing with Overwrite destroys the newer
owner's claim. The proposed design marks completion with CasVersion, so a stale
owner's completion is a no-op.
