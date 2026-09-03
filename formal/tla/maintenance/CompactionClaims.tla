--------------------------- MODULE CompactionClaims ---------------------------
(*****************************************************************************)
(* PROPOSED advisory compaction claims (ADR-1029) over the LANDED claim         *)
(* primitive: this model checks a proposed design layered on a shipped          *)
(* CreateIfAbsent/CasVersion primitive that nothing in the repository calls      *)
(* yet. It verifies the protocol design; implementation conformance is argued    *)
(* in the traceability table and asserted by the named Rust tests, not proved.   *)
(*                                                                           *)
(* --- Abstraction boundary --------------------------------------------------*)
(* MODELED, over the shared object-store contract (RavelObjectStore.tla):       *)
(*  * one claim key per unit routed through the store, so acquire is             *)
(*    CreateIfAbsent and renew/steal/mark_completed are CasVersion on the        *)
(*    held or observed version (claim.rs::acquire, renew, steal,                 *)
(*    mark_completed); PreconditionFailed is a no-op the caller reads as         *)
(*    ClaimLost/NotOwner/StealRefused;                                          *)
(*  * expiry read from the store's advisory last_modified via                    *)
(*    ClaimExpiryReadLastModifiedAdvisory, plus the holder's declared lease      *)
(*    capped at MAX_OBSERVED_LEASE (claim.rs::observe, MAX_OBSERVED_LEASE_MS);   *)
(*  * an unreadable (corrupt) or future-version claim is never stolen            *)
(*    (StealRefused::UnreadableClaim, NotExpired);                              *)
(*  * the cancellation-checkpoint guarded publish, which abandons when the       *)
(*    claim is lost, AND the ungated publish (the --no-claim CLI path and a      *)
(*    paused stale worker that ignores checkpoints) which publishes anyway;      *)
(*  * the compaction publication plane: content-addressed parts and one          *)
(*    terminal record per unit, CreateIfAbsent, whose correctness never reads    *)
(*    the claim (publish_record_with_conservation).                            *)
(*                                                                           *)
(* TIME is modeled as the store's own monotonic version domain (the same         *)
(* source as last_modified), advanced by a TimePass action. This is the          *)
(* natural common domain for last_modified-based advisory expiry, and it makes   *)
(* explicit the ADR-1029 property that node clocks never enter the correctness   *)
(* decision: every safety invariant here is independent of the expiry clock,     *)
(* which only gates WHEN a steal is permitted (early or late is advisory and     *)
(* safe, merely wasteful).                                                      *)
(*                                                                           *)
(* ASSUMED: blake3 work_id identity, the part encoder's content addressing,      *)
(* and the object store's own contract. OUT OF SCOPE: the cost gate arithmetic   *)
(* (claim_min_input_bytes), renewal cadence timing, and jitter scheduling; the   *)
(* model treats a claimed and an unclaimed bucket alike since a claim confers     *)
(* no publication authority.                                                    *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets, Integers

CONSTANTS
    Workers,          \* worker ids
    Units,            \* compaction units
    Variants,         \* input-set variants (a divergent listing -> divergent record)
    NoC,              \* NoContent sentinel
    Corrupt,          \* an unreadable claim payload
    Scr,              \* scratch content advancing logical time
    NoRec,            \* firstRecord sentinel
    DeclaredLease,    \* the holder's declared lease
    MaxObservedLease, \* MAX_OBSERVED_LEASE cap
    MaxV,             \* bound on the store version counter (total writes)
    MaxTime,          \* bound on TimePass steps
    LivenessMode,     \* TRUE: restrict to the paused-holder / thief lifecycle
    LHolder,          \* the only acquirer under LivenessMode
    LThief            \* the only thief under LivenessMode

ASSUME LHolder \in Workers /\ LThief \in Workers
ASSUME DeclaredLease \in Nat /\ MaxObservedLease \in Nat
ASSUME MaxV \in Nat /\ MaxTime \in Nat

EffLease == IF DeclaredLease < MaxObservedLease THEN DeclaredLease ELSE MaxObservedLease

\* --- store keys and contents ------------------------------------------------
ClaimKey(u)   == <<"claim", u>>
RecordKey(u)  == <<"rec", u>>
PartKey(u, v) == <<"part", u, v>>
ScratchKey    == <<"scratch">>
ClaimContentSet == { <<"c", o, s>> : o \in Workers, s \in {"run", "done"} }
OKeys == {ClaimKey(u) : u \in Units}
           \cup {RecordKey(u) : u \in Units}
           \cup {PartKey(u, v) : u \in Units, v \in Variants}
           \cup {ScratchKey}
OContent == {NoC, Corrupt, Scr} \cup ClaimContentSet
              \cup {<<u, v>> : u \in Units, v \in Variants}

VARIABLES
    store, lastModified, versionCounter, uploads, listState,
    timeUsed,        \* [0..MaxTime] TimePass budget
    heldVer,         \* [Workers -> [Units -> 0..MaxV]] 0 = holds no token
    obsVer,          \* [Workers -> [Units -> 0..MaxV]] last observed claim version
    firstRecord,     \* [Units -> OContent \cup {NoRec}]
    claimBorn,       \* [Units -> BOOLEAN] a claim key has ever been created
    dupThiefWin,     \* BOOLEAN two thieves won the same observed version
    stealWonVers,    \* [Units -> SUBSET (0..MaxV)] observed versions a steal won on
    lastClaimOp,     \* the most recent claim CAS (kind, version used, outcome)
    lastGuarded,     \* [fired: BOOL, held: BOOL] the last guarded publish
    stolen,          \* BOOLEAN a steal has succeeded (liveness witness)
    partTomb,        \* [Units -> [Variants -> BOOLEAN]] a part key deleted and
                     \* not re-PUTtable (a tombstone/GC the rerun cannot recreate)
    lastPub          \* the outcome of the most recent publish resolution, with
                     \* store-observed witnesses (never a self-reported label)

sVars == <<store, lastModified, versionCounter, uploads, listState>>
vars == <<store, lastModified, versionCounter, uploads, listState,
          timeUsed, heldVer, obsVer, firstRecord, claimBorn, dupThiefWin,
          stealWonVers, lastClaimOp, lastGuarded, stolen, partTomb, lastPub>>

\* The publication-resolution outcome alphabet (ADR-1113 D3), mirroring
\* publish.rs::resolve_already_exists: the CreateIfAbsent winner is Published;
\* a later attempt that finds the record present converges on it (Converged), or
\* re-PUTs a transiently vanished winner part and converges, or fails closed when
\* the winner part vanished and is not re-PUTtable (ConvergedWinnerPartMissing),
\* or refuses to touch a record whose input set diverges from its own
\* (InputSetHashDivergence); a checkpoint that loses the claim before publishing
\* Abandoned.
PubOutcomes == {"none", "Published", "Converged", "Abandoned",
                "ConvergedWinnerPartMissing", "InputSetHashDivergence"}

INSTANCE RavelObjectStore
    WITH Keys <- OKeys, Content <- OContent, NoContent <- NoC, Clients <- {}

\* --- claim accessors --------------------------------------------------------
ClaimPresent(u)   == Present(ClaimKey(u))
ClaimVer(u)       == VersionOf(ClaimKey(u))
ClaimContentOf(u) == ContentOf(ClaimKey(u))
ClaimReadable(u)  == ClaimContentOf(u) # Corrupt

\* Expiry from the advisory last_modified plus the effective lease, compared to
\* the current logical time (the version counter). ADR-1029 decision 1.
Expired(u) ==
    /\ ClaimPresent(u)
    /\ ClaimExpiryReadLastModifiedAdvisory(ClaimKey(u)) + EffLease < versionCounter

HoldsClaim(w, u) ==
    /\ ClaimPresent(u)
    /\ ClaimContentOf(u) = <<"c", w, "run">>
    /\ ClaimVer(u) = heldVer[w][u]

CanWrite == versionCounter < MaxV

VerRange == 0..MaxV
RecType == [present: BOOLEAN, content: OContent, version: Nat]

CTypeOK ==
    /\ StoreTypeOK
    /\ versionCounter \in 0..MaxV
    /\ timeUsed \in 0..MaxTime
    /\ heldVer \in [Workers -> [Units -> VerRange]]
    /\ obsVer \in [Workers -> [Units -> VerRange]]
    /\ firstRecord \in [Units -> OContent \cup {NoRec}]
    /\ claimBorn \in [Units -> BOOLEAN]
    /\ dupThiefWin \in BOOLEAN
    /\ stealWonVers \in [Units -> SUBSET VerRange]
    /\ lastClaimOp \in [kind: {"none", "acquire", "renew", "steal", "complete"},
                        unit: Units, usedVer: VerRange, ok: BOOLEAN,
                        beforeVer: VerRange, afterVer: VerRange,
                        beforeContent: OContent, afterContent: OContent]
    /\ lastGuarded \in [fired: BOOLEAN, held: BOOLEAN]
    /\ stolen \in BOOLEAN
    /\ partTomb \in [Units -> [Variants -> BOOLEAN]]
    /\ lastPub \in [outcome: PubOutcomes,
                    winnerPartPresent: BOOLEAN, recOverwritten: BOOLEAN]

Init ==
    /\ StoreInit
    /\ timeUsed = 0
    /\ heldVer = [w \in Workers |-> [u \in Units |-> 0]]
    /\ obsVer = [w \in Workers |-> [u \in Units |-> 0]]
    /\ firstRecord = [u \in Units |-> NoRec]
    /\ claimBorn = [u \in Units |-> FALSE]
    /\ dupThiefWin = FALSE
    /\ stealWonVers = [u \in Units |-> {}]
    /\ lastClaimOp = [kind |-> "none", unit |-> CHOOSE u \in Units : TRUE,
                      usedVer |-> 0, ok |-> TRUE, beforeVer |-> 0, afterVer |-> 0,
                      beforeContent |-> NoC, afterContent |-> NoC]
    /\ lastGuarded = [fired |-> FALSE, held |-> FALSE]
    /\ stolen = FALSE
    /\ partTomb = [u \in Units |-> [v \in Variants |-> FALSE]]
    /\ lastPub = [outcome |-> "none", winnerPartPresent |-> FALSE,
                  recOverwritten |-> FALSE]

\* --- claim lifecycle --------------------------------------------------------

\* claim.rs::acquire -- CreateIfAbsent. The Acquisition returns the version token.
Acquire(w, u) ==
    /\ CanWrite
    /\ (~LivenessMode \/ w = LHolder)
    /\ ~ClaimPresent(u)
    /\ PutCreateIfAbsent(ClaimKey(u), <<"c", w, "run">>)
    /\ heldVer' = [heldVer EXCEPT ![w][u] = versionCounter + 1]
    /\ lastClaimOp' = [kind |-> "acquire", unit |-> u, usedVer |-> 0, ok |-> TRUE,
                       beforeVer |-> ClaimVer(u), afterVer |-> store'[ClaimKey(u)].version,
                       beforeContent |-> ClaimContentOf(u),
                       afterContent |-> store'[ClaimKey(u)].content]
    /\ claimBorn' = [claimBorn EXCEPT ![u] = TRUE]
    /\ UNCHANGED <<timeUsed, obsVer, firstRecord, dupThiefWin,
                   stealWonVers, lastGuarded, stolen, partTomb, lastPub>>

\* claim.rs::observe -- one GET plus one HEAD; records the observed version.
Observe(w, u) ==
    /\ ClaimPresent(u)
    /\ obsVer' = [obsVer EXCEPT ![w][u] = ClaimVer(u)]
    /\ UNCHANGED <<sVars, timeUsed, heldVer, firstRecord, claimBorn,
                   dupThiefWin, stealWonVers, lastClaimOp, lastGuarded, stolen,
                   partTomb, lastPub>>

\* claim.rs::renew -- CasVersion on the held token; PreconditionFailed is
\* ClaimLost (the token is dropped). Only the holder attempts it (its token).
Renew(w, u) ==
    /\ CanWrite
    /\ ~LivenessMode
    /\ ClaimPresent(u)
    /\ ClaimReadable(u)
    /\ heldVer[w][u] # 0
    /\ LET v == heldVer[w][u] ok == (ClaimVer(u) = v) IN
        /\ PutCasVersion(ClaimKey(u), v, <<"c", w, "run">>)
        /\ heldVer' = [heldVer EXCEPT ![w][u] = IF ok THEN versionCounter + 1 ELSE 0]
        /\ lastClaimOp' = [kind |-> "renew", unit |-> u, usedVer |-> v, ok |-> ok,
                           beforeVer |-> ClaimVer(u),
                           afterVer |-> store'[ClaimKey(u)].version,
                           beforeContent |-> ClaimContentOf(u),
                           afterContent |-> store'[ClaimKey(u)].content]
    /\ UNCHANGED <<timeUsed, obsVer, firstRecord, claimBorn, dupThiefWin,
                   stealWonVers, lastGuarded, stolen, partTomb, lastPub>>

\* claim.rs::steal -- CasVersion on the observed version, gated on expiry and a
\* readable payload. StealRefused (NotExpired/UnreadableClaim) issues no store
\* request; a losing CAS (someone moved first) changes nothing.
Steal(w, u) ==
    /\ CanWrite
    /\ (~LivenessMode \/ w = LThief)
    /\ ClaimPresent(u)
    /\ Expired(u)
    /\ ClaimReadable(u)
    /\ obsVer[w][u] # 0
    /\ LET v == obsVer[w][u] ok == (ClaimVer(u) = v) IN
        /\ PutCasVersion(ClaimKey(u), v, <<"c", w, "run">>)
        /\ heldVer' = [heldVer EXCEPT ![w][u] = IF ok THEN versionCounter + 1 ELSE @]
        /\ dupThiefWin' = IF ok /\ v \in stealWonVers[u] THEN TRUE ELSE dupThiefWin
        /\ stealWonVers' = IF ok THEN [stealWonVers EXCEPT ![u] = @ \cup {v}] ELSE stealWonVers
        /\ stolen' = IF ok THEN TRUE ELSE stolen
        /\ lastClaimOp' = [kind |-> "steal", unit |-> u, usedVer |-> v, ok |-> ok,
                           beforeVer |-> ClaimVer(u),
                           afterVer |-> store'[ClaimKey(u)].version,
                           beforeContent |-> ClaimContentOf(u),
                           afterContent |-> store'[ClaimKey(u)].content]
    /\ UNCHANGED <<timeUsed, obsVer, firstRecord, claimBorn, lastGuarded,
                   partTomb, lastPub>>

\* claim.rs::mark_completed -- CasVersion; PreconditionFailed is NotOwner.
MarkCompleted(w, u) ==
    /\ CanWrite
    /\ ~LivenessMode
    /\ ClaimPresent(u)
    /\ ClaimReadable(u)
    /\ heldVer[w][u] # 0
    /\ LET v == heldVer[w][u] ok == (ClaimVer(u) = v) IN
        /\ PutCasVersion(ClaimKey(u), v, <<"c", w, "done">>)
        /\ heldVer' = [heldVer EXCEPT ![w][u] = IF ok THEN versionCounter + 1 ELSE 0]
        /\ lastClaimOp' = [kind |-> "complete", unit |-> u, usedVer |-> v, ok |-> ok,
                           beforeVer |-> ClaimVer(u),
                           afterVer |-> store'[ClaimKey(u)].version,
                           beforeContent |-> ClaimContentOf(u),
                           afterContent |-> store'[ClaimKey(u)].content]
    /\ UNCHANGED <<timeUsed, obsVer, firstRecord, claimBorn, dupThiefWin,
                   stealWonVers, lastGuarded, stolen, partTomb, lastPub>>

\* A claim payload becomes unreadable (corruption). Treated as absent by readers
\* and never stolen. No metadata changes (raw corruption, not a store write).
CorruptClaim(u) ==
    /\ ~LivenessMode
    /\ ClaimPresent(u)
    /\ ClaimReadable(u)
    /\ store' = [store EXCEPT ![ClaimKey(u)] =
                   [present |-> TRUE, content |-> Corrupt, version |-> ClaimVer(u)]]
    /\ UNCHANGED <<lastModified, versionCounter, uploads, listState, timeUsed,
                   heldVer, obsVer, firstRecord, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, lastGuarded, stolen,
                   partTomb, lastPub>>

\* Logical time advances (an unrelated store write bumps the version domain that
\* last_modified lives in). Bounded by MaxTime, separately from the write bound,
\* so time cannot starve the steal that expiry enables.
TimePass ==
    /\ CanWrite
    /\ timeUsed < MaxTime
    /\ (~LivenessMode \/ \A u \in Units : ClaimPresent(u))
    /\ PutOverwrite(ScratchKey, Scr)
    /\ timeUsed' = timeUsed + 1
    /\ UNCHANGED <<heldVer, obsVer, firstRecord, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, lastGuarded, stolen,
                   partTomb, lastPub>>

\* --- compaction publication (never reads the claim) -------------------------

\* A part is content-addressed CreateIfAbsent. A tombstoned key (VanishPart set
\* partTomb) cannot be recreated, so a rerun that needs it must fail closed
\* rather than silently re-PUT it.
PutPart(u, v) ==
    /\ CanWrite
    /\ ~partTomb[u][v]
    /\ (~LivenessMode \/ \A x \in Units : ClaimPresent(x))
    /\ PutCreateIfAbsent(PartKey(u, v), <<u, v>>)
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, firstRecord, claimBorn,
                   dupThiefWin, stealWonVers, lastClaimOp, lastGuarded, stolen,
                   partTomb, lastPub>>

\* The winner's terminal record PUT (CreateIfAbsent) and the loser's convergence
\* over the shared object store, resolving exactly as publish.rs and
\* resolve_already_exists do. The outcome witness (lastPub) reads the store
\* around the resolution (winnerPartPresent from store', recOverwritten from the
\* record version delta), never a self-reported label:
\*  * record absent: this attempter is the CreateIfAbsent winner -> Published;
\*  * record present, a DIFFERENT input set: alarm, delete and overwrite
\*    nothing -> InputSetHashDivergence;
\*  * record present, same input set, winner part intact: Converged;
\*  * record present, same input set, winner part transiently vanished but
\*    re-PUTtable: re-PUT identical content-addressed bytes -> Converged;
\*  * record present, same input set, winner part vanished and tombstoned
\*    (not re-PUTtable): fail closed -> ConvergedWinnerPartMissing.
DoPublish(u, v) ==
    LET rk  == RecordKey(u)
        rp  == Present(rk)
        wv  == IF rp /\ firstRecord[u] # NoRec THEN firstRecord[u][2] ELSE v
        wpk == PartKey(u, wv)
    IN
    IF ~rp
      THEN /\ Present(PartKey(u, v))
           /\ PutCreateIfAbsent(rk, <<u, v>>)
           /\ firstRecord' = [firstRecord EXCEPT ![u] = <<u, v>>]
           /\ lastPub' = [outcome |-> "Published",
                          winnerPartPresent |-> store'[PartKey(u, v)].present,
                          recOverwritten |-> FALSE]
      ELSE IF v # wv
        THEN /\ UNCHANGED <<sVars, firstRecord>>
             /\ lastPub' = [outcome |-> "InputSetHashDivergence",
                            winnerPartPresent |-> Present(wpk),
                            recOverwritten |-> FALSE]
        ELSE IF Present(wpk)
          THEN /\ UNCHANGED <<sVars, firstRecord>>
               /\ lastPub' = [outcome |-> "Converged",
                              winnerPartPresent |-> TRUE,
                              recOverwritten |-> FALSE]
          ELSE IF ~partTomb[u][wv]
            THEN /\ PutCreateIfAbsent(wpk, <<u, wv>>)
                 /\ UNCHANGED firstRecord
                 /\ lastPub' = [outcome |-> "Converged",
                                winnerPartPresent |-> store'[wpk].present,
                                recOverwritten |-> FALSE]
            ELSE /\ UNCHANGED <<sVars, firstRecord>>
                 /\ lastPub' = [outcome |-> "ConvergedWinnerPartMissing",
                                winnerPartPresent |-> FALSE,
                                recOverwritten |-> FALSE]

\* The guarded (cancellation-checkpoint) publish: publishes only while still
\* holding the claim, so a lost claim abandons the run (no publication). The
\* witness records whether the worker actually held the claim in the store at the
\* moment of publication (HoldsClaim reads the stored owner and version), NOT a
\* literal, so an unguarded publish is caught.
GuardedPublish(w, u, v) ==
    /\ CanWrite
    /\ ~LivenessMode
    /\ HoldsClaim(w, u)
    /\ DoPublish(u, v)
    /\ lastGuarded' = [fired |-> TRUE, held |-> HoldsClaim(w, u)]
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, stolen, partTomb>>

\* The ungated publish: the --no-claim CLI path and a paused stale worker that
\* ignores its checkpoints and reaches the publication path anyway. Publishes
\* regardless of any claim; correctness holds because the record is
\* CreateIfAbsent and parts are content addressed.
UngatedPublish(u, v) ==
    /\ CanWrite
    /\ ~LivenessMode
    /\ DoPublish(u, v)
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, lastGuarded, stolen, partTomb>>

\* The cancellation checkpoint that finds the claim already lost abandons the
\* run: it publishes nothing (the store is unchanged) and records the Abandoned
\* outcome. HoldsClaim is read from the store, so a genuine loss is the only way
\* to reach this.
AbandonPublish(w, u, v) ==
    /\ ~LivenessMode
    /\ ~HoldsClaim(w, u)
    /\ Present(PartKey(u, v))
    /\ lastPub' = [outcome |-> "Abandoned",
                   winnerPartPresent |-> IF firstRecord[u] = NoRec
                                           THEN Present(PartKey(u, v))
                                           ELSE Present(PartKey(u, firstRecord[u][2])),
                   recOverwritten |-> FALSE]
    /\ UNCHANGED <<sVars, timeUsed, heldVer, obsVer, firstRecord, claimBorn,
                   dupThiefWin, stealWonVers, lastClaimOp, lastGuarded, stolen,
                   partTomb>>

\* A published winner part transiently disappears (a tombstone race, GC, or a
\* delayed listing), and is re-PUTtable: a later convergence re-creates the
\* identical content-addressed bytes. Models tombstone_race.rs's revanish path
\* up to the point the part can still be recreated.
VanishPart(u) ==
    /\ firstRecord[u] # NoRec
    /\ Present(PartKey(u, firstRecord[u][2]))
    /\ ~partTomb[u][firstRecord[u][2]]
    /\ Delete(PartKey(u, firstRecord[u][2]))
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, firstRecord, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, lastGuarded, stolen, partTomb, lastPub>>

\* The winner part vanishes and is tombstoned: it cannot be re-PUT (the rerun in
\* tombstone_race.rs::rerun_with_revanished_part_fails_typed_not_converged sees a
\* delete tombstone the content-addressed create cannot overwrite). One-shot per
\* key. A later convergence on this unit must fail closed, never report Converged.
TombstonePart(u) ==
    /\ firstRecord[u] # NoRec
    /\ ~partTomb[u][firstRecord[u][2]]
    /\ partTomb' = [partTomb EXCEPT ![u][firstRecord[u][2]] = TRUE]
    /\ IF Present(PartKey(u, firstRecord[u][2]))
         THEN Delete(PartKey(u, firstRecord[u][2]))
         ELSE UNCHANGED sVars
    /\ UNCHANGED <<timeUsed, heldVer, obsVer, firstRecord, claimBorn, dupThiefWin,
                   stealWonVers, lastClaimOp, lastGuarded, stolen, lastPub>>

Next ==
    \/ \E w \in Workers, u \in Units : Acquire(w, u)
    \/ \E w \in Workers, u \in Units : Observe(w, u)
    \/ \E w \in Workers, u \in Units : Renew(w, u)
    \/ \E w \in Workers, u \in Units : Steal(w, u)
    \/ \E w \in Workers, u \in Units : MarkCompleted(w, u)
    \/ \E u \in Units : CorruptClaim(u)
    \/ TimePass
    \/ \E u \in Units, v \in Variants : PutPart(u, v)
    \/ \E w \in Workers, u \in Units, v \in Variants : GuardedPublish(w, u, v)
    \/ \E u \in Units, v \in Variants : UngatedPublish(u, v)
    \/ \E w \in Workers, u \in Units, v \in Variants : AbandonPublish(w, u, v)
    \/ \E u \in Units : VanishPart(u)
    \/ \E u \in Units : TombstonePart(u)

Spec == Init /\ [][Next]_vars

\* --- named safety invariants ------------------------------------------------

\* Publication correctness never reads the claim: whatever the claim state (held,
\* lost, expired early, corrupt, duplicated), the terminal record is the single
\* CreateIfAbsent winner and every part key carries its content-addressed bytes.
\* (ADR-1029 decision 2; broken by the claim-as-publication-authority switch.)
ClaimGrantsNoPublicationAuthority ==
    /\ \A u \in Units :
         Present(RecordKey(u)) => ContentOf(RecordKey(u)) = firstRecord[u]
    /\ \A u \in Units, v \in Variants :
         Present(PartKey(u, v)) => ContentOf(PartKey(u, v)) = <<u, v>>

\* A stale-version claim write never succeeds. The witness records what the store
\* held before the operator ran and what it held after (beforeVer/afterVer and
\* beforeContent/afterContent are read from the store, not self-reported), so the
\* invariant relates the reported outcome to the durable delta: a successful CAS
\* used the current version and bumped the stored version, and a failed CAS left
\* the stored record byte-for-byte unchanged. (Broken by
\* claim-completion-without-cas, which turns the CAS into an Overwrite.)
StaleOwnerCannotOverwriteNewerClaim ==
    lastClaimOp.kind \in {"renew", "steal", "complete"} =>
        /\ (lastClaimOp.ok =>
              /\ lastClaimOp.usedVer = lastClaimOp.beforeVer
              /\ lastClaimOp.afterVer # lastClaimOp.beforeVer)
        /\ (~lastClaimOp.ok =>
              /\ lastClaimOp.afterVer = lastClaimOp.beforeVer
              /\ lastClaimOp.afterContent = lastClaimOp.beforeContent)

\* No path deletes a claim unconditionally (a stale worker's DELETE could destroy
\* a newer owner's claim). Stated over the store: a claim key that was ever
\* created is still present, since no modeled action legitimately removes one.
\* claimBorn latches on the CreateIfAbsent that births the key, so a DELETE of a
\* present claim drops ClaimPresent while the latch stays set. (Broken by
\* claim-delete-unconditional.)
NoUnconditionalClaimDelete ==
    \A u \in Units : claimBorn[u] => ClaimPresent(u)

\* At most one thief wins a given observed version: the version token is consumed
\* by the first successful steal, so a second steal on the same observed version
\* fails its CAS. (Broken by the steal-ignores-cas control, which counts a win
\* from a steal whose CAS did not match.)
AtMostOneThiefWinsAVersion == ~dupThiefWin

\* The guarded (checkpoint) path never publishes once the claim is lost.
LostClaimNeverPublishesThroughGuardedPath ==
    lastGuarded.fired => lastGuarded.held

\* Merge attempts converge, restated for a world where a part can vanish (F7).
\* The old form asserted a present record's winner part is present; that held
\* ONLY because nothing ever deleted a part. Once VanishPart/TombstonePart exist
\* the honest guarantee is fail-closed convergence: a resolution reports Converged
\* only when the winner part is actually present, and reports
\* ConvergedWinnerPartMissing exactly when the winner part has vanished and is not
\* re-PUTtable (tombstoned) -- it never silently claims convergence over a missing
\* part. winnerPartPresent is read from the store at the moment of resolution
\* (store' in DoPublish), never a self-reported label, so a mutant that labels a
\* missing-part resolution "Converged" (its witness still reads the absent store)
\* is caught. A transiently vanished, re-PUTtable part is not a violation: the
\* resolution re-PUTs identical content-addressed bytes and reports Converged with
\* the part present again. See README (F7). (Broken by the
\* missing-part-reports-converged switch.)
MergeAttemptsConverge ==
    /\ (lastPub.outcome = "Converged" => lastPub.winnerPartPresent)
    /\ (lastPub.outcome = "ConvergedWinnerPartMissing" => ~lastPub.winnerPartPresent)

\* A divergent input set alarms and mutates nothing: the loser neither overwrites
\* the terminal record nor deletes any part. recOverwritten is the store-observed
\* record version delta across the resolution (FALSE means the record version did
\* not move), so a mutant that overwrites the record on divergence -- even to
\* identical content, which still mints a new version -- is caught. The divergent
\* branch of DoPublish is UNCHANGED store, so no part is deleted either.
\* (Broken by the diverge-overwrites-record switch.)
DivergentInputSetNeverMutates ==
    lastPub.outcome = "InputSetHashDivergence" => ~lastPub.recOverwritten

\* --- fairness and liveness --------------------------------------------------
\* Under LivenessMode the lifecycle is acquire (paused holder) -> time passes ->
\* observe -> steal, with no renewal or completion to resurrect the holder. With
\* weak fairness on the thief's observe and steal, a fair store, and advancing
\* time, an expired claim is eventually stolen. Environment: paused holder, fair
\* thief, fair store, advancing time (ADR-1029 decision 1 step 4).
Fairness ==
    /\ WF_vars(TimePass)
    /\ \A u \in Units : WF_vars(Acquire(LHolder, u))
    /\ \A u \in Units : WF_vars(Observe(LThief, u))
    /\ \A u \in Units : WF_vars(Steal(LThief, u))

FairSpec == Spec /\ Fairness

ExpiredClaimEventuallyStolen == <>(stolen)

=============================================================================
