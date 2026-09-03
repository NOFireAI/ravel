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
    OwnerPublishOverwrite,       \* negative: an owner publishes the record with
                                 \* Overwrite instead of CreateIfAbsent
    HeartbeatMemoUsesCas,        \* negative: a memo write uses CasVersion
    MemoOverstamp,               \* negative: seeding skips the per-entry clamp
    DivergeOverwritesRecord,     \* negative: a divergent-input loser overwrites
                                 \* the terminal record instead of alarming
    MissingPartReportsConverged  \* negative: a resolution whose winner part
                                 \* vanished and is tombstoned reports Converged

ASSUME OwnerPublishOverwrite \in BOOLEAN
ASSUME HeartbeatMemoUsesCas \in BOOLEAN
ASSUME MemoOverstamp \in BOOLEAN
ASSUME DivergeOverwritesRecord \in BOOLEAN
ASSUME MissingPartReportsConverged \in BOOLEAN

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
                   cliCorrect, lastMaint, partTomb, lastPub>>

\* Broken: a memo persistence write uses CasVersion against a stale version token
\* (0) instead of Overwrite. The contract makes CasVersion a no-op both on an
\* absent key and on a version mismatch, so a self-owned key never satisfies the
\* precondition here: the memo is not refreshed and the store version does not
\* advance. The witness reads the store, so this is caught by
\* HeartbeatAndMemoNeverCas even though nothing self-reports "CasVersion".
BrokenMemoCas(w) ==
    /\ HeartbeatMemoUsesCas
    /\ ~crashed[w]
    /\ PutCasVersion(MemoKey(w), 0, MemoContent(w))
    /\ lastMaint' = [class |-> "memo",
                     verBefore |-> VersionOf(MemoKey(w)),
                     verAfter |-> store'[MemoKey(w)].version,
                     maxExcess |-> 0]
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect,
                   partTomb, lastPub>>

\* Broken: seeding takes the raw verified stamp without clamping it to the
\* source snapshot time, so an in-memory entry can read fresher than its own
\* snapshot. The per-entry excess for a future/skewed entry (verU > snapNs) is
\* then positive even when another entry has a larger snapshot. Caught by
\* MemoNeverExtendsFreshnessPastSnapshot.
BrokenSeed(w) ==
    /\ MemoOverstamp
    /\ ~crashed[w]
    /\ LET valid == ValidSnaps(w)
           excess == { memoSnap[x].verU - memoSnap[x].snapNs : x \in valid }
           mx == IF excess = {} THEN 0 ELSE Max(excess)
       IN lastMaint' = [class |-> "seed", verBefore |-> 0, verAfter |-> 0,
                        maxExcess |-> mx]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect,
                   partTomb, lastPub>>

\* Broken: a loser whose input set diverges from the winner overwrites the
\* terminal record instead of alarming. The overwrite keeps the same content, so
\* the record-immutability invariant still holds; recOverwritten (the store
\* version delta) moves regardless, so the alarm-mutates-nothing property is what
\* catches it. Caught by DivergentInputSetNeverMutates.
BrokenDivergePublish(u, v) ==
    /\ DivergeOverwritesRecord
    /\ Present(RecordKey(u))
    /\ firstRecord[u] # NoRec
    /\ v # firstRecord[u][2]
    /\ LET rk == RecordKey(u) IN
        /\ PutOverwrite(rk, ContentOf(rk))
        /\ lastPub' = [outcome |-> "InputSetHashDivergence",
                       winnerPartPresent |-> Present(PartKey(u, firstRecord[u][2])),
                       recOverwritten |-> store'[rk].version # VersionOf(rk)]
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint, partTomb>>

\* Broken: a resolution whose winner part vanished and is tombstoned (not
\* re-PUTtable) reports Converged. It self-reports the "Converged" label while the
\* store witness (winnerPartPresent, read from the store) shows the part absent, so
\* a model that trusted the label would pass. Caught by MergeAttemptsConverge.
BrokenMissingPartConverge(u, v) ==
    /\ MissingPartReportsConverged
    /\ Present(RecordKey(u))
    /\ firstRecord[u] # NoRec
    /\ v = firstRecord[u][2]
    /\ ~Present(PartKey(u, v))
    /\ partTomb[u][v]
    /\ lastPub' = [outcome |-> "Converged",
                   winnerPartPresent |-> Present(PartKey(u, v)),
                   recOverwritten |-> FALSE]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint, partTomb>>

MCNext ==
    \/ Next
    \/ \E w \in Workers, u \in Units, v \in Variants : BrokenOwnerPublish(w, u, v)
    \/ \E w \in Workers : BrokenMemoCas(w)
    \/ \E w \in Workers : BrokenSeed(w)
    \/ \E u \in Units, v \in Variants : BrokenDivergePublish(u, v)
    \/ \E u \in Units, v \in Variants : BrokenMissingPartConverge(u, v)

MCSpec == Init /\ [][MCNext]_vars
MCFairSpec == MCSpec /\ Fairness

\* Safety-check state projection (smoke and the invariant negatives). The store's
\* absolute versions, the global monotone versionCounter, and lastModified are
\* dead state once a write has happened: no action or safety invariant reads them.
\* The only version-derived fact any invariant reads is the sign of the last
\* heartbeat/memo write's version delta (HeartbeatAndMemoNeverCas), which the
\* witness has already captured into lastMaint. Projecting the raw versions away
\* while keeping present/content and that delta sign collapses states that differ
\* only in global write order, which is the whole source of the version-counter
\* blow-up. Sound for safety only: it is NOT applied to the temporal cfgs, where a
\* VIEW that merges stutter-equivalent states can mask a liveness counterexample.
StoreView == [k \in OKeys |-> <<store[k].present, store[k].content>>]
LastMaintView == <<lastMaint.class,
                   lastMaint.verAfter > lastMaint.verBefore,
                   lastMaint.maxExcess>>
MCView == <<StoreView, now, hbStamp, crashed, cachedLive, memoSnap,
            firstRecord, attemptedByOwner, cliCorrect, LastMaintView,
            partTomb, lastPub>>

=============================================================================
