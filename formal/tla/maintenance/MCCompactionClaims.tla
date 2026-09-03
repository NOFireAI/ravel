------------------------- MODULE MCCompactionClaims -------------------------
(*****************************************************************************)
(* Model-check entry for CompactionClaims.tla. Carries the negative-control     *)
(* switches (ADR-1113 D6): each defaults to the correct value and a              *)
(* negative/<name>.cfg flips exactly one. A broken disjunct is enabled only      *)
(* when its switch is TRUE, so with every switch FALSE MCNext equals Next.       *)
(*                                                                           *)
(* No symmetry: the rendezvous-free claim payload distinguishes owners and the   *)
(* firstRecord witness distinguishes variants.                                  *)
(*****************************************************************************)
EXTENDS CompactionClaims

CONSTANTS
    CompletionOverwrite,         \* negative: mark_completed uses Overwrite not CAS
    AllowClaimDelete,            \* negative: a path deletes the claim unconditionally
    ClaimIsPublicationAuthority, \* negative: the publish path skips CreateIfAbsent
                                 \* when the claim is held
    GuardIgnoresClaim            \* negative: the guarded publish drops its
                                 \* HoldsClaim check and publishes anyway

ASSUME CompletionOverwrite \in BOOLEAN
ASSUME AllowClaimDelete \in BOOLEAN
ASSUME ClaimIsPublicationAuthority \in BOOLEAN
ASSUME GuardIgnoresClaim \in BOOLEAN

\* Broken: mark_completed overwrites the claim regardless of version. A stale
\* owner (its token no longer current) still overwrites a newer claim. The witness
\* reads the store before and after exactly as the correct MarkCompleted does, so
\* it captures the real Overwrite: on a stale token the reported outcome is not-Ok
\* yet the stored version moved. Caught by StaleOwnerCannotOverwriteNewerClaim.
BrokenComplete(w, u) ==
    /\ CompletionOverwrite
    /\ CanWrite
    /\ ClaimPresent(u)
    /\ ClaimReadable(u)
    /\ heldVer[w][u] # 0
    /\ LET v == heldVer[w][u] ok == (ClaimVer(u) = v) IN
        /\ PutOverwrite(ClaimKey(u), <<"c", w, "done">>)
        /\ heldVer' = [heldVer EXCEPT ![w][u] = versionCounter + 1]
        /\ lastClaimOp' = [kind |-> "complete", unit |-> u, usedVer |-> v, ok |-> ok,
                           beforeVer |-> ClaimVer(u),
                           afterVer |-> store'[ClaimKey(u)].version,
                           beforeContent |-> ClaimContentOf(u),
                           afterContent |-> store'[ClaimKey(u)].content]
    /\ UNCHANGED <<timeUsed, obsVer, firstRecord, claimBorn, dupThiefWin,
                   stealWonVers, lastGuarded, stolen>>

\* Broken: an unconditional DELETE of the claim key, touching no witness. The
\* store-level NoUnconditionalClaimDelete catches it: the claim key drops from
\* present to absent while the claimBorn latch (set when it was created) stays
\* set. Caught by NoUnconditionalClaimDelete.
DeleteClaim(u) ==
    /\ AllowClaimDelete
    /\ ClaimPresent(u)
    /\ Delete(ClaimKey(u))
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, firstRecord, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, lastGuarded, stolen>>

\* Broken: when the claim is held, the publish path overwrites the terminal
\* record instead of CreateIfAbsent, so a holder can mutate the record to a
\* divergent variant. Caught by ClaimGrantsNoPublicationAuthority.
BrokenClaimPublish(w, u, v) ==
    /\ ClaimIsPublicationAuthority
    /\ CanWrite
    /\ HoldsClaim(w, u)
    /\ Present(PartKey(u, v))
    /\ LET rk == RecordKey(u) IN
        /\ PutOverwrite(rk, <<u, v>>)
        /\ firstRecord' = IF ~Present(rk)
                            THEN [firstRecord EXCEPT ![u] = <<u, v>>]
                            ELSE firstRecord
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, lastGuarded, stolen>>

\* Broken: the guarded (checkpoint) publish drops its HoldsClaim check and
\* publishes regardless. The witness still reads the store (held |-> HoldsClaim),
\* so a publish by a non-holder records held = FALSE with fired = TRUE. Caught by
\* LostClaimNeverPublishesThroughGuardedPath.
BrokenGuardedPublish(w, u, v) ==
    /\ GuardIgnoresClaim
    /\ CanWrite
    /\ DoPublish(u, v)
    /\ lastGuarded' = [fired |-> TRUE, held |-> HoldsClaim(w, u)]
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, stolen>>

MCNext ==
    \/ Next
    \/ \E w \in Workers, u \in Units : BrokenComplete(w, u)
    \/ \E u \in Units : DeleteClaim(u)
    \/ \E w \in Workers, u \in Units, v \in Variants : BrokenClaimPublish(w, u, v)
    \/ \E w \in Workers, u \in Units, v \in Variants : BrokenGuardedPublish(w, u, v)

MCSpec == Init /\ [][MCNext]_vars
MCFairSpec == MCSpec /\ Fairness

=============================================================================
