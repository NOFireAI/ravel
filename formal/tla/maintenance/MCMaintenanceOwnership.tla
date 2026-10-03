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
\* Restricted to units no legitimate publish has sealed (recVer = 0) so the ONLY
\* invariant it can trip is the record-divergence one: a first overwrite creates
\* the record and latches firstRecord, a second overwrite with a different variant
\* leaves the record diverging from that latched winner. Caught by
\* QueryVisibleDataCorrectUnderDuplicateOwnership.
BrokenOwnerPublish(w, u, v) ==
    /\ OwnerPublishOverwrite
    /\ ~crashed[w]
    /\ Owns(w, u)
    /\ recVer[u] = 0
    /\ Present(PartKey(u, v))
    /\ LET rk == RecordKey(u) IN
        /\ PutOverwrite(rk, <<u, v>>)
        /\ firstRecord' = IF ~Present(rk)
                            THEN [firstRecord EXCEPT ![u] = <<u, v>>]
                            ELSE firstRecord
    /\ attemptedByOwner' = [attemptedByOwner EXCEPT ![u] = TRUE]
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   cliCorrect, lastMaint, partTomb, lastPub, recVer,
                   seedFresh, vanishedOnce, hbWriteCount, memoWriteCount>>

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
                     verAfter |-> store'[MemoKey(w)].version]
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect,
                   partTomb, lastPub, recVer, seedFresh, vanishedOnce,
                   hbWriteCount, memoWriteCount>>

\* Broken: seeding takes the raw verified stamp without clamping it to the source
\* snapshot time and STORES that, so seedFresh records an in-memory entry that
\* reads fresher than its own snapshot. For a future/skewed worst entry
\* (verU > snapNs) the stored fresh exceeds the stored snap. Caught by
\* MemoNeverExtendsFreshnessPastSnapshot reading the stored pair.
BrokenSeed(w) ==
    /\ MemoOverstamp
    /\ ~crashed[w]
    /\ LET valid == ValidSnaps(w) IN
       IF valid = {}
         THEN seedFresh' = [seedFresh EXCEPT ![w] = [fresh |-> 0, snap |-> 0]]
         ELSE LET worst == CHOOSE x \in valid :
                             \A y \in valid :
                               memoSnap[y].verU - memoSnap[y].snapNs
                                 =< memoSnap[x].verU - memoSnap[x].snapNs
                  s == memoSnap[worst]
              IN seedFresh' = [seedFresh EXCEPT ![w] =
                                 [fresh |-> s.verU, snap |-> s.snapNs]]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub, recVer, vanishedOnce,
                   hbWriteCount, memoWriteCount>>

\* Broken: a loser whose input set diverges from the winner overwrites the
\* terminal record instead of alarming. The overwrite re-writes identical content,
\* so a content-only check would miss it; but the PUT mints a fresh store version,
\* moving VersionOf(RecordKey(u)) off the latched recVer (left UNCHANGED here).
\* Caught by DivergentInputSetNeverMutates as a pure store observation.
BrokenDivergePublish(u, v) ==
    /\ DivergeOverwritesRecord
    /\ Present(RecordKey(u))
    /\ firstRecord[u] # NoRec
    /\ v # firstRecord[u][2]
    /\ LET rk == RecordKey(u) IN
        /\ PutOverwrite(rk, ContentOf(rk))
        /\ lastPub' = [outcome |-> "InputSetHashDivergence",
                       winnerPartPresent |-> Present(PartKey(u, firstRecord[u][2]))]
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, recVer, seedFresh, vanishedOnce,
                   hbWriteCount, memoWriteCount>>

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
                   winnerPartPresent |-> Present(PartKey(u, v))]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, recVer, seedFresh, vanishedOnce,
                   hbWriteCount, memoWriteCount>>

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
\* dead state once a write has happened: no action reads them. The only version-
\* derived facts any invariant reads are the sign of the last heartbeat/memo
\* write's version delta (HeartbeatAndMemoNeverCas), captured into lastMaint, and
\* whether the terminal record's version still equals the one it was minted at
\* (DivergentInputSetNeverMutates); the latter is projected here as the two
\* booleans that invariant reads (recVer = 0, recVer = the record's current
\* version), never the raw version, so states differing only in global write order
\* still merge. recVer and seedFresh are otherwise read by no action, so projecting
\* them to those invariant-relevant booleans is sound for safety. NOT applied to
\* the temporal cfgs, where a VIEW merging stutter-equivalent states can mask a
\* liveness counterexample.
StoreView == [k \in OKeys |-> <<store[k].present, store[k].content>>]
LastMaintView == <<lastMaint.class, lastMaint.verAfter > lastMaint.verBefore>>
RecSealView == [u \in Units |-> <<recVer[u] = 0,
                                  recVer[u] = VersionOf(RecordKey(u))>>]
SeedFreshView == [w \in Workers |-> seedFresh[w].fresh =< seedFresh[w].snap]
MCView == <<StoreView, now, hbStamp, crashed, cachedLive, memoSnap,
            firstRecord, attemptedByOwner, cliCorrect, LastMaintView,
            partTomb, lastPub, RecSealView, SeedFreshView, vanishedOnce>>

=============================================================================
