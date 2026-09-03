# claim-delete-unconditional

Switch: `AllowClaimDelete = TRUE`. Expected: `NoUnconditionalClaimDelete` violated (exit 12).

Trace: worker 1 acquires the claim of unit 1. The broken `DeleteClaim` step then
issues an unconditional DELETE of the claim key and sets the `claimDeleted`
witness. The invariant `~claimDeleted` fails immediately.

Why it matters: ADR-1029 step 6 forbids an unconditional delete precisely
because a stale worker's DELETE could destroy a newer owner's claim; the
published compaction record is the only meaningful completion marker. The
proposed design never deletes a claim -- it marks `state = done` with CasVersion
or lets the claim age out.
