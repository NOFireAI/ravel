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
    CompletionOverwrite,        \* negative: mark_completed uses Overwrite not CAS
    AllowClaimDelete,           \* negative: a path deletes the claim unconditionally
    ClaimIsPublicationAuthority \* negative: the publish path skips CreateIfAbsent
                                \* when the claim is held

ASSUME CompletionOverwrite \in BOOLEAN
ASSUME AllowClaimDelete \in BOOLEAN
ASSUME ClaimIsPublicationAuthority \in BOOLEAN

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
    /\ UNCHANGED <<timeUsed, obsVer, firstRecord, claimDeleted, dupThiefWin,
                   stealWonVers, lastGuarded, stolen>>

\* Broken: an unconditional DELETE of the claim key. Caught by
\* NoUnconditionalClaimDelete.
DeleteClaim(u) ==
    /\ AllowClaimDelete
    /\ ClaimPresent(u)
    /\ Delete(ClaimKey(u))
    /\ claimDeleted' = TRUE
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, firstRecord, dupThiefWin,
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
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, claimDeleted, dupThiefWin,
                   stealWonVers, lastClaimOp, lastGuarded, stolen>>

MCNext ==
    \/ Next
    \/ \E w \in Workers, u \in Units : BrokenComplete(w, u)
    \/ \E u \in Units : DeleteClaim(u)
    \/ \E w \in Workers, u \in Units, v \in Variants : BrokenClaimPublish(w, u, v)

MCSpec == Init /\ [][MCNext]_vars
MCFairSpec == MCSpec /\ Fairness

=============================================================================
