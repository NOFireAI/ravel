------------------------ MODULE MCMaintenanceOwnership ------------------------
(*****************************************************************************)
(* Model-check entry for MaintenanceOwnership.tla. Fixes the small constant    *)
(* sets and carries the negative-control switches (ADR-1113 D6): each defaults  *)
(* to the correct value and a negative/<name>.cfg flips exactly one. The        *)
(* correct model and the broken model are the same text under different         *)
(* constants; a broken disjunct is only enabled when its switch is TRUE, so     *)
(* with every switch FALSE MCNext equals the correct Next.                      *)
(*                                                                           *)
(* No symmetry: the rendezvous weight table distinguishes workers and the       *)
(* firstRecord witness distinguishes variants, so neither Workers nor Variants  *)
(* is a sound symmetry set.                                                    *)
(*****************************************************************************)
EXTENDS MaintenanceOwnership

CONSTANTS
    OwnerPublishOverwrite,  \* negative: an owner publishes the record with
                            \* Overwrite instead of CreateIfAbsent
    HeartbeatMemoUsesCas,   \* negative: a memo write uses CasVersion
    MemoOverstamp           \* negative: seeding skips the per-entry clamp

ASSUME OwnerPublishOverwrite \in BOOLEAN
ASSUME HeartbeatMemoUsesCas \in BOOLEAN
ASSUME MemoOverstamp \in BOOLEAN

\* Broken: an in-view owner overwrites the terminal record with its own variant.
\* A second overwrite with a different variant leaves the record diverging from
\* the winner firstRecord latched -- caught by
\* QueryVisibleDataCorrectUnderDuplicateOwnership.
BrokenOwnerPublish(w, u, v) ==
    /\ OwnerPublishOverwrite
    /\ ~crashed[w]
    /\ Owns(w, u)
    /\ Present(PartKey(u, v))
    /\ LET rk == RecordKey(u) IN
        /\ PutOverwrite(rk, <<u, v>>)
        /\ firstRecord' = IF ~Present(rk)
                            THEN [firstRecord EXCEPT ![u] = <<u, v>>]
                            ELSE firstRecord
    /\ attemptedByOwner' = [attemptedByOwner EXCEPT ![u] = TRUE]
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   cliCorrect, lastMaint>>

\* Broken: a memo persistence write uses CasVersion. Caught by
\* HeartbeatAndMemoNeverCas.
BrokenMemoCas(w) ==
    /\ HeartbeatMemoUsesCas
    /\ ~crashed[w]
    /\ memoSnap' = [memoSnap EXCEPT ![w] = [snapNs |-> now, verU |-> now]]
    /\ lastMaint' = [class |-> "memo", mode |-> "CasVersion",
                     val |-> now, bound |-> now]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive,
                   firstRecord, attemptedByOwner, cliCorrect>>

\* Broken: seeding takes the raw verified stamp without clamping it to the
\* source snapshot time, so an in-memory entry can read fresher than its
\* snapshot. Caught by MemoNeverExtendsFreshnessPastSnapshot.
BrokenSeed(w) ==
    /\ MemoOverstamp
    /\ ~crashed[w]
    /\ LET valid == ValidSnaps(w)
           raw == { memoSnap[x].verU : x \in valid }
           value == IF raw = {} THEN 0 ELSE Max(raw)
           bnd == IF valid = {} THEN 0 ELSE Max({ memoSnap[x].snapNs : x \in valid })
       IN lastMaint' = [class |-> "seed", mode |-> "none", val |-> value, bound |-> bnd]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect>>

MCNext ==
    \/ Next
    \/ \E w \in Workers, u \in Units, v \in Variants : BrokenOwnerPublish(w, u, v)
    \/ \E w \in Workers : BrokenMemoCas(w)
    \/ \E w \in Workers : BrokenSeed(w)

MCSpec == Init /\ [][MCNext]_vars
MCFairSpec == MCSpec /\ Fairness

=============================================================================
