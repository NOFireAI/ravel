---------------------------- MODULE CatalogMVCC ----------------------------
(*****************************************************************************)
(* Catalog model: fold, snapshots, compaction, and MVCC (ADR-1113 D3,       *)
(* task T3, issue #1121).                                                    *)
(*                                                                           *)
(* Scope. One (tenant, signal) catalog plane over a single shard, a small    *)
(* set of ingest hours, and a small pool of abstract commit-record           *)
(* identities. The HEAD register is the single CAS object; it is driven      *)
(* through an instance of RavelObjectStore so its create/CAS/lost-response   *)
(* semantics are exactly the shared object-store contract, not a re-modeled  *)
(* copy. The immutable planes (L0 commits, L1 compaction records, snapshot   *)
(* parts, tombstones) are append-only model variables: a CreateIfAbsent      *)
(* publish is an idempotent set insertion, a sweep is a removal.             *)
(*                                                                           *)
(* What the model drives, by actor:                                          *)
(*   ingest      publishes L0 commit records into not-yet-fold-sealed hours  *)
(*   compactor   publishes an L1 record over a maintenance-sealed hour, with *)
(*               a counts-only conservation gate                             *)
(*   folder      reads HEAD, stages a snapshot part, publishes the part,     *)
(*               then CAS-swaps HEAD; may crash before the CAS; racing       *)
(*               folders serialize on the HEAD version                       *)
(*   sweeper     sweep_superseded (no HEAD gate, the #1134 shape) and the    *)
(*               HEAD-gated catalog-object sweep (fail-closed on a corrupt   *)
(*               or unsupported HEAD)                                        *)
(*   retention   marks a bucket tombstoned                                   *)
(*   query       resolves a snapshot (or degrades to listing when HEAD is    *)
(*               not readable), pins it, and retries once on invalidation    *)
(*   environment ticks the clock, corrupts HEAD, or writes an unsupported    *)
(*               HEAD version                                                 *)
(*                                                                           *)
(* Two seal predicates are modeled distinctly (RECONNAISSANCE F1): the       *)
(* folder's fold watermark adds FoldSealDelay (fold_safety_margin folded in) *)
(* on top of the maintenance seal delay, so a bucket is maintenance-sealed   *)
(* (compactable) strictly before it is fold-sealed (foldable). Reconcile     *)
(* runs only inside a watermark-advancing fold (F16/F17), never on a plain   *)
(* clock tick.                                                               *)
(*                                                                           *)
(* Negative-control switches (default FALSE = the correct protocol) each     *)
(* break exactly one clause; a negative/<name>.cfg flips one. They are       *)
(* declared here, not in the MC module, because Next branches on them.       *)
(*                                                                           *)
(* Assumptions discharged elsewhere, NOT proved here (ADR-1113 D12):         *)
(*   - segment encode/decode, content hashing, and the multiset-level        *)
(*     equality of a compaction's output are asserted by                     *)
(*     crates/ravel-query/tests/differential_compaction.rs; the runtime gate *)
(*     modeled here is counts-only (sum of sample_count).                    *)
(*   - object-store durability and contract conformance are the RavelObject  *)
(*     Store module's assumptions.                                           *)
(* This model checks a finite configuration; it is not a proof for all       *)
(* shard, hour, and record sizes.                                            *)
(*****************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    \* --- object-store instance (HEAD register) ---
    Keys, Content, NoContent, Clients,
    \* --- catalog bounds ---
    Hours,              \* small set of ingest-hour indices (consecutive Nats)
    Records,            \* abstract L0 commit-record identities
    CompIds,            \* abstract compaction-record identities per hour
    MaxClock,           \* clock horizon
    MaxOps,             \* budget on ingest + compaction publishes
    \* --- protocol constants ---
    FoldSealDelay,      \* fold_safety_margin on top of the maintenance seal
    MaintSealDelay,     \* maintenance seal delay (Bucket::is_sealed)
    ProtectionHorizon,  \* age before sweep_superseded may delete L0 inputs
    RetentionHorizon,   \* age before a bucket may be tombstoned
    LagBound,           \* max compaction publish lag past the maintenance seal
    DedupBySignal,      \* TRUE = metrics (query-time dedup); FALSE = logs/spans
    \* --- negative-control switches (default FALSE) ---
    HeadNamesUnwrittenPart,    \* folder CAS-swaps HEAD naming a part never PUT
    CompactionSwapsRecord,     \* compaction output swaps an identity (counts kept)
    ReconcileOnTick,           \* reconcile runs off a plain tick, no wm advance
    SnapshotChangesMidAttempt, \* a pinned query snapshot mutates within an attempt
    DropMetricsDedup           \* metrics query stops deduping by identity at read

ASSUME NoContent \in Content
ASSUME Keys # {}
ASSUME Hours # {} /\ Records # {} /\ CompIds # {}
ASSUME MaxClock \in Nat /\ MaxOps \in Nat

VARIABLES
    store, lastModified, versionCounter, uploads, listState,  \* RavelObjectStore
    clock,            \* current tick, 0..MaxClock
    budget,           \* ingest + compaction publishes charged so far
    l0,               \* [Hours -> SUBSET Records]  present L0 commit identities
    crec,             \* [Hours -> [CompIds -> CrecRec]]  compaction records
    tomb,             \* [Hours -> BOOLEAN]  bucket tombstone marker
    head,             \* HeadRec: status/wm/entries payload of the HEAD register
    snapParts,        \* SUBSET SnapPart: content-addressed snapshot parts present
    foldStage,        \* [Clients -> StageRec]  per-folder in-flight fold
    qy,               \* QyRec: the single modeled query
    lastHead,         \* witness: the most recent HEAD-writing transition
    lastGatedSweep,   \* witness: the most recent HEAD-gated delete-path sweep
    corruptionUsed,   \* ghost: HEAD corruption fired (bounds state)
    unsupportedUsed   \* ghost: unsupported-HEAD write fired (bounds state)

INSTANCE RavelObjectStore

\* The object-store variables, named locally so UNCHANGED does not depend on the
\* instanced tuple operator.
svTuple == <<store, lastModified, versionCounter, uploads, listState>>

Folders == Clients
HK == CHOOSE k \in Keys : TRUE
HeadContentVal == CHOOSE c \in RealContent : TRUE

\* Tagged catalog entries: an L0 commit identity, or an L1 compaction part.
L0Entries  == { <<"l0", H, r>> : H \in Hours, r \in Records }
L1Entries  == { <<"l1", H, g>> : H \in Hours, g \in CompIds }
AllEntries == L0Entries \cup L1Entries

CrecRec    == [used: BOOLEAN, in: SUBSET Records, out: SUBSET Records, at: Int]
ClearedCrec == [used |-> FALSE, in |-> {}, out |-> {}, at |-> -1]
StageRec   == [active: BOOLEAN, wm: Int, entries: SUBSET AllEntries,
               partWritten: BOOLEAN, baseVer: Nat, baseAbsent: BOOLEAN]
ClearedStage == [active |-> FALSE, wm |-> -1, entries |-> {},
                 partWritten |-> FALSE, baseVer |-> 0, baseAbsent |-> FALSE]
Statuses   == {"absent", "valid", "corrupt", "unsupported"}
SnapPart   == [wm: Int, entries: SUBSET AllEntries]

allModel == <<clock, budget, l0, crec, tomb, head, snapParts, foldStage, qy,
              lastHead, lastGatedSweep, corruptionUsed, unsupportedUsed>>

\* --- seal predicates (RECONNAISSANCE F1) ------------------------------------
\* HourEnd(H) is the boundary tick of hour H. A bucket is maintenance-sealed
\* MaintSealDelay after its end (Bucket::is_sealed), and fold-sealed a further
\* FoldSealDelay later (sealed_watermark_hour adds fold_safety_margin).
HourEnd(H)      == H + 1
MaintSealTick(H) == HourEnd(H) + MaintSealDelay
FoldSealTick(H)  == HourEnd(H) + MaintSealDelay + FoldSealDelay
MaintSealed(H)  == clock >= MaintSealTick(H)
FoldSealed(H)   == clock >= FoldSealTick(H)

SealedHours == { H \in Hours : FoldSealed(H) }
FoldWatermark ==
    IF SealedHours = {} THEN -1
    ELSE CHOOSE H \in SealedHours : \A j \in SealedHours : j <= H

\* L0 identities named as inputs of some published compaction record for H.
SupersededInputs(H) ==
    LET Used == { g \in CompIds : crec[H][g].used }
    IN UNION { crec[H][g].in : g \in Used }

\* The fold's classify + reconcile output at watermark w: live L0 (present, not
\* superseded, not tombstoned) below the watermark, plus published L1 parts.
FoldEntriesFor(w) ==
    { e \in AllEntries :
        /\ e[2] <= w
        /\ ~tomb[e[2]]
        /\ ( \/ (e[1] = "l0" /\ e[3] \in l0[e[2]] /\ e[3] \notin SupersededInputs(e[2]))
             \/ (e[1] = "l1" /\ crec[e[2]][e[3]].used) ) }

\* A query that cannot read HEAD lists the commit plane directly (fail-open):
\* every live entry regardless of watermark.
ListingView ==
    { e \in AllEntries :
        /\ ~tomb[e[2]]
        /\ ( \/ (e[1] = "l0" /\ e[3] \in l0[e[2]] /\ e[3] \notin SupersededInputs(e[2]))
             \/ (e[1] = "l1" /\ crec[e[2]][e[3]].used) ) }

CurrentView == IF head.status = "valid" THEN head.entries ELSE ListingView

\* A snapshot part is named by HEAD iff HEAD is valid and equals it exactly.
PartNamedByHead(p) ==
    head.status = "valid" /\ p.wm = head.wm /\ p.entries = head.entries

\* A part an in-flight fold has staged and will name at its CAS.
StagedByActiveFold(p) ==
    \E f \in Folders :
        foldStage[f].active /\ foldStage[f].wm = p.wm /\ foldStage[f].entries = p.entries

\* The catalog-object sweep may reclaim a part only when no HEAD names it and no
\* in-flight fold is about to. This models the sweep's minimum-age gate, which
\* keeps it from racing a fold that has published its part but not yet CAS'd.
SweepablePart(p) == ~PartNamedByHead(p) /\ ~StagedByActiveFold(p)

\* The object backing a pinned entry has been swept away.
ObjectDeleted(e) ==
    IF e[1] = "l0" THEN e[3] \notin l0[e[2]] ELSE ~crec[e[2]][e[3]].used

TypeOK ==
    /\ StoreTypeOK
    /\ clock \in 0..MaxClock
    /\ budget \in 0..MaxOps
    /\ l0 \in [Hours -> SUBSET Records]
    /\ crec \in [Hours -> [CompIds -> CrecRec]]
    /\ tomb \in [Hours -> BOOLEAN]
    /\ head \in [status: Statuses, wm: Int, entries: SUBSET AllEntries]
    /\ snapParts \subseteq SnapPart
    /\ foldStage \in [Folders -> StageRec]
    /\ qy \in [phase: {"idle", "pinned", "done", "invalid"}, attempt: 0..2,
               source: {"none", "snapshot", "listing"}, pinned: SUBSET AllEntries,
               pinnedAtAttempt: SUBSET AllEntries,
               headStatusAtResolve: {"none"} \cup Statuses]
    /\ lastHead \in [kind: {"none", "fold", "recTick", "corrupt", "unsupported"},
                     wmBefore: Int, wmAfter: Int, entriesChanged: BOOLEAN,
                     entries: SUBSET AllEntries]
    /\ lastGatedSweep \in [ran: BOOLEAN, headStatus: {"none"} \cup Statuses,
                           deletedAny: BOOLEAN]
    /\ corruptionUsed \in BOOLEAN
    /\ unsupportedUsed \in BOOLEAN

Init ==
    /\ StoreInit
    /\ clock = 0
    /\ budget = 0
    /\ l0 = [H \in Hours |-> {}]
    /\ crec = [H \in Hours |-> [g \in CompIds |-> ClearedCrec]]
    /\ tomb = [H \in Hours |-> FALSE]
    /\ head = [status |-> "absent", wm |-> -1, entries |-> {}]
    /\ snapParts = {}
    /\ foldStage = [f \in Folders |-> ClearedStage]
    /\ qy = [phase |-> "idle", attempt |-> 0, source |-> "none", pinned |-> {},
             pinnedAtAttempt |-> {}, headStatusAtResolve |-> "none"]
    /\ lastHead = [kind |-> "none", wmBefore |-> -1, wmAfter |-> -1,
                   entriesChanged |-> FALSE, entries |-> {}]
    /\ lastGatedSweep = [ran |-> FALSE, headStatus |-> "none", deletedAny |-> FALSE]
    /\ corruptionUsed = FALSE
    /\ unsupportedUsed = FALSE

\* --- ingest: publish an L0 commit record ------------------------------------
\* crates/ravel-commit/src/publish.rs::publish::put_data_object. Commits land
\* only in hours not yet fold-sealed, so a fold at a watermark sees a frozen
\* prefix (the wrongly-sealed-bucket clock-skew case is out of scope).
DoCommit(H, r) ==
    /\ budget < MaxOps
    /\ ~FoldSealed(H)
    /\ ~tomb[H]
    /\ r \notin l0[H]
    /\ l0' = [l0 EXCEPT ![H] = @ \cup {r}]
    /\ budget' = budget + 1
    /\ UNCHANGED <<clock, crec, tomb, head, snapParts, foldStage, qy, lastHead,
                   lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* --- compactor: publish an L1 compaction record -----------------------------
\* crates/ravel-maintain/src/publish.rs::publish_record_with_conservation. The
\* runtime gate is counts-only (conserve_exact); the swap switch keeps the count
\* and changes one identity, which the counts gate cannot catch and the offline
\* multiset test must.
DoCompact(H, g) ==
    /\ budget < MaxOps
    /\ MaintSealed(H)
    /\ clock - MaintSealTick(H) <= LagBound
    /\ ~tomb[H]
    /\ ~crec[H][g].used
    /\ l0[H] # {}
    /\ LET inputs == l0[H]
           spare  == Records \ inputs
           out == IF CompactionSwapsRecord /\ spare # {} /\ inputs # {}
                  THEN (inputs \ {CHOOSE x \in inputs : TRUE})
                         \cup {CHOOSE y \in spare : TRUE}
                  ELSE inputs
       IN crec' = [crec EXCEPT ![H][g] =
                     [used |-> TRUE, in |-> inputs, out |-> out, at |-> clock]]
    /\ budget' = budget + 1
    /\ UNCHANGED <<clock, l0, tomb, head, snapParts, foldStage, qy, lastHead,
                   lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* --- folder: read HEAD and stage a fold -------------------------------------
\* crates/ravel-catalog/src/fold.rs::sealed_watermark_hour and
\* Catalog::reconcile_one_bucket. A fold starts only to advance the watermark on
\* a valid HEAD, or to rebuild an absent/corrupt HEAD (fold clobbers a corrupt
\* HEAD). An unsupported HEAD is terminal: the folder never starts.
DoFoldStart(f) ==
    /\ ~foldStage[f].active
    /\ head.status # "unsupported"
    /\ LET w == FoldWatermark IN
        /\ w >= 0
        /\ ( \/ head.status \in {"absent", "corrupt"}
             \/ (head.status = "valid" /\ w > head.wm) )
        /\ foldStage' = [foldStage EXCEPT ![f] =
              [active |-> TRUE, wm |-> w, entries |-> FoldEntriesFor(w),
               partWritten |-> FALSE, baseVer |-> store[HK].version,
               baseAbsent |-> (head.status = "absent")]]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, head, snapParts, qy, lastHead,
                   lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* crates/ravel-catalog/src/fold.rs::part_object_key: the snapshot part is
\* content-addressed and published before HEAD names it.
DoFoldPutPart(f) ==
    /\ foldStage[f].active
    /\ ~foldStage[f].partWritten
    /\ snapParts' = snapParts \cup
         { [wm |-> foldStage[f].wm, entries |-> foldStage[f].entries] }
    /\ foldStage' = [foldStage EXCEPT ![f].partWritten = TRUE]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, head, qy, lastHead,
                   lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* crates/ravel-catalog/src/fold.rs::Catalog::get_head with MAX_HEAD_CAS_ATTEMPTS:
\* the version-matched CAS serializes racing folders. A win publishes the new
\* snapshot atomically; a loser's CAS no-ops and it rebases (clears its stage).
\* Correct: the CAS requires the part be written. HeadNamesUnwrittenPart waives
\* that, so HEAD names a part absent from snapParts.
DoFoldCas(f) ==
    /\ foldStage[f].active
    /\ (HeadNamesUnwrittenPart \/ foldStage[f].partWritten)
    /\ LET st == foldStage[f]
           won == IF st.baseAbsent THEN ~store[HK].present
                  ELSE (store[HK].present /\ store[HK].version = st.baseVer)
       IN
        /\ IF st.baseAbsent THEN PutCreateIfAbsent(HK, HeadContentVal)
                            ELSE PutCasVersion(HK, st.baseVer, HeadContentVal)
        /\ head' = IF won
                   THEN [status |-> "valid", wm |-> st.wm, entries |-> st.entries]
                   ELSE head
        /\ lastHead' = IF won
                       THEN [kind |-> "fold", wmBefore |-> head.wm, wmAfter |-> st.wm,
                             entriesChanged |-> (st.entries # head.entries),
                             entries |-> st.entries]
                       ELSE lastHead
        /\ foldStage' = [foldStage EXCEPT ![f] = ClearedStage]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, snapParts, qy,
                   lastGatedSweep, corruptionUsed, unsupportedUsed>>

\* A folder crash between staging and the CAS: HEAD is untouched, and the
\* published part is left orphaned in snapParts for a later catalog-object sweep.
DoFoldCrash(f) ==
    /\ foldStage[f].active
    /\ foldStage' = [foldStage EXCEPT ![f] = ClearedStage]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, head, snapParts, qy, lastHead,
                   lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* --- reconcile off a plain tick (negative control only) ---------------------
\* crates/ravel-catalog/src/fold.rs::Catalog::reconcile_one_bucket runs inside a
\* watermark-advancing fold. Enabled only under ReconcileOnTick: it re-reconciles
\* HEAD without advancing the watermark, which the witness records as a reconcile
\* whose step did not raise the watermark.
DoReconcileTick(f) ==
    /\ ReconcileOnTick
    /\ head.status = "valid"
    /\ LET e2 == FoldEntriesFor(head.wm) IN
        /\ head' = [head EXCEPT !.entries = e2]
        /\ lastHead' = [kind |-> "recTick", wmBefore |-> head.wm, wmAfter |-> head.wm,
                        entriesChanged |-> (e2 # head.entries), entries |-> e2]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, snapParts, foldStage, qy,
                   lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* --- sweeper: sweep_superseded (no HEAD gate; the #1134 shape) ---------------
\* crates/ravel-maintain/src/sweep.rs::sweep_superseded_impl deletes an L1
\* record's L0 inputs once past the protection horizon, with NO check that a
\* current HEAD snapshot part still names them.
DoSweepSuperseded(H, g) ==
    /\ crec[H][g].used
    /\ crec[H][g].in \cap l0[H] # {}
    /\ clock - crec[H][g].at >= ProtectionHorizon
    /\ l0' = [l0 EXCEPT ![H] = @ \ crec[H][g].in]
    /\ UNCHANGED <<clock, budget, crec, tomb, head, snapParts, foldStage, qy,
                   lastHead, lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* crates/ravel-maintain/src/sweep.rs::sweep_unreferenced_catalog_objects reads
\* the HEAD reference (read_head_reference) and deletes an orphan snapshot part.
\* It runs only when HEAD is readable.
DoSweepCatalogObjects ==
    /\ head.status \in {"valid", "absent"}
    /\ \E p \in snapParts : SweepablePart(p)
    /\ LET orphan == CHOOSE p \in snapParts : SweepablePart(p) IN
         snapParts' = snapParts \ {orphan}
    /\ lastGatedSweep' = [ran |-> TRUE, headStatus |-> head.status, deletedAny |-> TRUE]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, head, foldStage, qy, lastHead,
                   corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* crates/ravel-maintain/src/reachability.rs::SnapshotReachability::bucket_gate:
\* a corrupt or unsupported HEAD fails the reachability read, so the whole
\* delete pass is blocked and nothing is deleted (fail-closed).
DoSweepBlockedOnCorruptHead ==
    /\ head.status \in {"corrupt", "unsupported"}
    /\ ~(lastGatedSweep.ran /\ lastGatedSweep.headStatus = head.status
         /\ ~lastGatedSweep.deletedAny)
    /\ lastGatedSweep' = [ran |-> TRUE, headStatus |-> head.status, deletedAny |-> FALSE]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, head, snapParts, foldStage, qy,
                   lastHead, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* --- retention: tombstone a bucket ------------------------------------------
\* crates/ravel-maintain/src/retention.rs::write_tombstone. The marker retires
\* the bucket; a later fold contributes nothing for it. Physical GC of the
\* objects is a separate pass, out of scope here (they stay present, excluded).
DoTombstone(H) ==
    /\ ~tomb[H]
    /\ MaintSealed(H)
    /\ clock - HourEnd(H) >= RetentionHorizon
    /\ tomb' = [tomb EXCEPT ![H] = TRUE]
    /\ UNCHANGED <<clock, budget, l0, crec, head, snapParts, foldStage, qy,
                   lastHead, lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* --- query: resolve, pin, retry once ----------------------------------------
\* crates/ravel-query/src/engine.rs::QueryEngine::resolve_snapshot_with_retry.
\* A valid HEAD pins the snapshot; any other HEAD state degrades to a listing
\* (fail-open), never an error.
DoQueryResolve ==
    /\ qy.phase = "idle"
    /\ qy' = [phase |-> "pinned", attempt |-> 1,
              source |-> IF head.status = "valid" THEN "snapshot" ELSE "listing",
              pinned |-> CurrentView, pinnedAtAttempt |-> CurrentView,
              headStatusAtResolve |-> head.status]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, head, snapParts, foldStage,
                   lastHead, lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* A pinned entry whose object was swept forces one re-resolve; a second miss is
\* terminal (crates/ravel-query/src/error.rs::QueryError::SnapshotInvalidated).
DoQueryRun ==
    /\ qy.phase = "pinned"
    /\ IF \A e \in qy.pinned : ~ObjectDeleted(e)
         THEN qy' = [qy EXCEPT !.phase = "done"]
         ELSE IF qy.attempt < 2
                THEN qy' = [phase |-> "pinned", attempt |-> 2,
                            source |-> IF head.status = "valid" THEN "snapshot" ELSE "listing",
                            pinned |-> CurrentView, pinnedAtAttempt |-> CurrentView,
                            headStatusAtResolve |-> head.status]
                ELSE qy' = [qy EXCEPT !.phase = "invalid"]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, head, snapParts, foldStage,
                   lastHead, lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* A pinned snapshot must not change under a query mid-attempt. Enabled only
\* under SnapshotChangesMidAttempt, which mutates the pinned set in place.
DoQueryTamper ==
    /\ SnapshotChangesMidAttempt
    /\ qy.phase = "pinned"
    /\ qy.pinned # {}
    /\ qy' = [qy EXCEPT !.pinned = qy.pinned \ {CHOOSE e \in qy.pinned : TRUE}]
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, head, snapParts, foldStage,
                   lastHead, lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* --- environment ------------------------------------------------------------
DoTick ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<budget, l0, crec, tomb, head, snapParts, foldStage, qy,
                   lastHead, lastGatedSweep, corruptionUsed, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* HEAD bytes become undecodable; the object stays present (same version). The
\* folder will clobber and rebuild (wm reset to unknown).
DoCorruptHead ==
    /\ ~corruptionUsed
    /\ head.status = "valid"
    /\ head' = [status |-> "corrupt", wm |-> -1, entries |-> {}]
    /\ lastHead' = [kind |-> "corrupt", wmBefore |-> head.wm, wmAfter |-> -1,
                    entriesChanged |-> (head.entries # {}), entries |-> {}]
    /\ corruptionUsed' = TRUE
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, snapParts, foldStage, qy,
                   lastGatedSweep, unsupportedUsed>>
    /\ UNCHANGED svTuple

\* A newer-format writer publishes an unsupported HEAD version (object present,
\* version bumped). This is terminal for the folder.
DoUnsupportedHead ==
    /\ ~unsupportedUsed
    /\ head.status \in {"absent", "valid"}
    /\ PutOverwrite(HK, HeadContentVal)
    /\ head' = [status |-> "unsupported", wm |-> -1, entries |-> {}]
    /\ lastHead' = [kind |-> "unsupported", wmBefore |-> head.wm, wmAfter |-> -1,
                    entriesChanged |-> (head.entries # {}), entries |-> {}]
    /\ unsupportedUsed' = TRUE
    /\ UNCHANGED <<clock, budget, l0, crec, tomb, snapParts, foldStage, qy,
                   lastGatedSweep, corruptionUsed>>

Next ==
    \/ \E H \in Hours, r \in Records : DoCommit(H, r)
    \/ \E H \in Hours, g \in CompIds : DoCompact(H, g)
    \/ \E f \in Folders : DoFoldStart(f)
    \/ \E f \in Folders : DoFoldPutPart(f)
    \/ \E f \in Folders : DoFoldCas(f)
    \/ \E f \in Folders : DoFoldCrash(f)
    \/ \E f \in Folders : DoReconcileTick(f)
    \/ \E H \in Hours, g \in CompIds : DoSweepSuperseded(H, g)
    \/ DoSweepCatalogObjects
    \/ DoSweepBlockedOnCorruptHead
    \/ \E H \in Hours : DoTombstone(H)
    \/ DoQueryResolve
    \/ DoQueryRun
    \/ DoQueryTamper
    \/ DoTick
    \/ DoCorruptHead
    \/ DoUnsupportedHead

vars == <<store, lastModified, versionCounter, uploads, listState,
          clock, budget, l0, crec, tomb, head, snapParts, foldStage, qy,
          lastHead, lastGatedSweep, corruptionUsed, unsupportedUsed>>

Spec == Init /\ [][Next]_vars

\* --- fairness (liveness only) -----------------------------------------------
\* Weak fairness on watermark advance (the folder makes progress: start, put
\* part, and CAS), on the clock, and on the query, so a late supersession is
\* eventually reflected and a started query eventually finishes.
FoldProgress == \E f \in Folders : (DoFoldStart(f) \/ DoFoldPutPart(f) \/ DoFoldCas(f))
QueryProgress == DoQueryResolve \/ DoQueryRun

FairSpec ==
    /\ Spec
    /\ WF_vars(DoTick)
    /\ WF_vars(FoldProgress)
    /\ WF_vars(QueryProgress)

----------------------------------------------------------------------------
\* Named safety invariants.

\* HEAD only ever names a snapshot part that was fully published first.
\* (Broken by HeadNamesUnwrittenPart.)
HeadNamesOnlyCompleteParts ==
    head.status = "valid" =>
        [wm |-> head.wm, entries |-> head.entries] \in snapParts

\* Every published compaction record preserves its input multiset exactly; the
\* counts-only gate permits the swap that this catches.
\* (Broken by CompactionSwapsRecord.)
CompactionPreservesMultiset ==
    \A H \in Hours : \A g \in CompIds :
        crec[H][g].used => crec[H][g].out = crec[H][g].in

\* A reconcile step that changed HEAD's entries also raised the watermark, so
\* reconcile never runs off a plain tick. The witness records the real wm delta
\* of the last fold/reconcile step. (Broken by ReconcileOnTick.)
ReconcileOnlyOnWatermarkAdvance ==
    (lastHead.kind \in {"fold", "recTick"} /\ lastHead.entriesChanged)
        => lastHead.wmAfter > lastHead.wmBefore

\* No HEAD snapshot entry sits above the watermark it was folded at.
SnapshotEntriesBelowWatermark ==
    head.status = "valid" => \A e \in head.entries : e[2] <= head.wm

\* Within one query attempt the pinned snapshot is frozen.
\* (Broken by SnapshotChangesMidAttempt.)
PinnedSnapshotStableWithinAttempt ==
    qy.phase = "pinned" => qy.pinned = qy.pinnedAtAttempt

\* The winning fold omits no live commit: every present, non-superseded,
\* non-tombstoned L0 record at or below the watermark is named by HEAD.
NoLiveCommitOmittedByLostCas ==
    head.status = "valid" =>
        \A H \in Hours :
            (H <= head.wm /\ ~tomb[H]) =>
                \A r \in l0[H] :
                    (r \notin SupersededInputs(H)) => <<"l0", H, r>> \in head.entries

\* A query that could not read a valid HEAD degraded to a listing rather than
\* erroring. (Load-bearing against the mutant that errors instead; see
\* counterexamples/missing-index-degrades-to-listing.md.)
MissingIndexDegradesToListing ==
    (qy.phase \in {"pinned", "done", "invalid"} /\ qy.headStatusAtResolve # "valid"
        /\ qy.headStatusAtResolve # "none")
        => qy.source = "listing"

\* A HEAD-gated delete pass that saw a corrupt or unsupported HEAD deleted
\* nothing (fail-closed).
CorruptHeadFailsClosedOnDeletePaths ==
    (lastGatedSweep.ran /\ lastGatedSweep.headStatus \in {"corrupt", "unsupported"})
        => ~lastGatedSweep.deletedAny

\* Every object a valid HEAD names is still present. This is NOT asserted in the
\* smoke or exhaustive configs: sweep_superseded carries no HEAD gate (issue
\* #1134), so under free supersession lag it can delete an L0 input that a still
\* current HEAD names. It is asserted only in negative/free-lag-head-dangling.cfg,
\* which is an EXPECTED failure recording the #1134 design flaw, not a protocol
\* the model claims to hold. See counterexamples/free-lag-head-dangling.md.
HeadNamedObjectNeverDeleted ==
    head.status = "valid" => \A e \in head.entries : ~ObjectDeleted(e)

\* A fold contributes nothing for a tombstoned bucket: the fold function excludes
\* every tombstoned hour at every watermark.
TombstonedBucketContributesNothing ==
    \A H \in Hours :
        tomb[H] => \A w \in Hours : \A e \in FoldEntriesFor(w) : e[2] # H

\* Signal semantics: under metrics (query-time dedup) a two-input-set compaction
\* conflict serves each identity at most once; under logs/spans (no dedup) the
\* same conflict is a duplicate-serving state and this constraint does not apply.
\* (Load-bearing against the mutant that drops the metrics dedup; see
\* counterexamples/signal-dedup-contract.md.)
SourcesServing(H, r) ==
    (IF ~tomb[H] /\ r \in l0[H] /\ r \notin SupersededInputs(H) THEN 1 ELSE 0)
      + Cardinality({ g \in CompIds :
            ~tomb[H] /\ crec[H][g].used /\ r \in crec[H][g].out })
\* The multiplicity a query actually serves for r: the raw source count when
\* dedup is off (logs/spans, or the DropMetricsDedup mutant), collapsed to at
\* most one when the metrics dedup runs (crates/ravel-query/src/engine.rs
\* is_greater over identity). The contract below is falsifiable exactly because
\* this collapse can be turned off; a two-source conflict (SourcesServing > 1) is
\* reachable with two compaction records, so the collapse does real work.
MetricsMult(H, r) ==
    IF DedupBySignal /\ ~DropMetricsDedup
        THEN (IF SourcesServing(H, r) > 0 THEN 1 ELSE 0)
        ELSE SourcesServing(H, r)
SignalDedupContract ==
    DedupBySignal =>
        \A H \in Hours : \A r \in Records : MetricsMult(H, r) <= 1

----------------------------------------------------------------------------
\* Named temporal properties (checked against FairSpec only).

\* A published compaction record is eventually reflected in a valid HEAD: its
\* superseded L0 inputs leave the snapshot and its L1 part enters it. Fairness:
\* WF on fold progress and the clock.
LateSupersessionEventuallyReflected ==
    \A H \in Hours : \A g \in CompIds :
        (crec[H][g].used /\ ~tomb[H])
            ~> ( head.status = "valid" /\ H <= head.wm
                 => ( <<"l1", H, g>> \in head.entries
                      /\ \A r \in crec[H][g].in : <<"l0", H, r>> \notin head.entries ) )

=============================================================================
