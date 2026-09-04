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
(* Invariant standard (ADR-1113 review, issue #1121). Every named safety     *)
(* invariant observes either the modelled STORE (objects present, their      *)
(* content, the HEAD register, a commit or compaction record) or a WITNESS   *)
(* variable that records what a store operation RETURNED (the HEAD status a   *)
(* read observed, the entry set a fold wrote, the view a query served). No    *)
(* invariant reads a bookkeeping flag an action sets to assert its own        *)
(* compliance. Each named invariant is proven non-vacuous by mutating the     *)
(* BEHAVIOUR (breaking an action's guard or its store effect) and observing   *)
(* the violation, never by flipping a control switch that also drives the     *)
(* thing the invariant reads; results.md records the TLC line for each.       *)
(*                                                                           *)
(* What the model drives, by actor:                                          *)
(*   ingest      publishes L0 commit records into not-yet-fold-sealed hours  *)
(*   compactor   publishes an L1 record over a maintenance-sealed hour, with *)
(*               a counts-only conservation gate                             *)
(*   folder      reads HEAD, stages a snapshot part, publishes the part,     *)
(*               then CAS-swaps HEAD; may crash before the CAS; rebases if    *)
(*               it outran the protection horizon; a losing CAS re-reads HEAD *)
(*               and retries, never overwriting (DoFoldCas loser branch)      *)
(*   rival       a concurrent catalog process that wins a CAS and advances    *)
(*               HEAD out from under an in-flight fold, so the modelled       *)
(*               folder reaches its CAS on a stale base and loses             *)
(*   sweeper     the HEAD-gated superseded-input sweep (ADR-0020 delete       *)
(*               blocker) and the HEAD-gated catalog-object sweep; both       *)
(*               delete only on a readable (valid/absent) HEAD, so a corrupt  *)
(*               or unsupported HEAD fails the pass closed                    *)
(*   retention   marks a bucket tombstoned                                   *)
(*   query       resolves a snapshot (or degrades to listing when HEAD is    *)
(*               not readable), pins it, dedups by identity under metrics,    *)
(*               and retries once on invalidation                             *)
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
(* break exactly one clause of a store or query effect; a negative/<name>.cfg *)
(* flips one. They are declared here, not in the MC module, because Next      *)
(* branches on them.                                                         *)
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
    HeadNamesUnwrittenPart,     \* folder CAS-swaps HEAD naming a part never PUT
    CompactionSwapsRecord,      \* compaction output swaps an identity (counts kept)
    ReconcileOnTick,            \* reconcile runs off a plain tick, no wm advance
    SnapshotChangesMidAttempt,  \* a pinned query snapshot mutates within an attempt
    DropMetricsDedup,           \* metrics query stops deduping by identity at read
    SweepSupersededNoHeadGate,  \* superseded sweep skips its HEAD-reachability gate
    LostCasProceedsOnStaleRead, \* a losing fold CAS overwrites HEAD on its stale read
    CompactionLoserOverwrites,  \* a losing compaction publish overwrites the winner
    QueryFailsClosedOnMissingIndex, \* a query with no readable index serves empty, not listing
    DeletePathIgnoresUnreadableHead, \* a delete pass proceeds despite a corrupt/unsupported HEAD
    DeletePathIgnoresUnreadablePart, \* superseded-input sweep proceeds despite an unreadable covering part
    DeletePathIgnoresUndecodableEntry, \* superseded-input sweep proceeds despite an undecodable entry identity
    FoldNamesEntryAboveWatermark, \* a fold's scan admits an entry above its own watermark
    FoldIncludesTombstonedEntries, \* a fold's scan re-includes a tombstoned hour
    \* --- state-space budget switch (default TRUE; FALSE only where a config
    \* needs the smaller graph) ---
    EnableDeletePathCorruption \* DoCorruptPart/DoPoisonEntry may fire at all;
                                \* see the comment beside them

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
    lastDelete,       \* witness: the HEAD status observed at the last store deletion
    lastCompact,      \* witness: whether the last losing compaction publish mutated
    maxValidWm,       \* witness: highest watermark ever published to a valid HEAD
    corruptionUsed,   \* ghost: HEAD corruption fired (bounds state)
    unsupportedUsed,  \* ghost: unsupported-HEAD write fired (bounds state)
    partUnreadable,   \* [Hours -> BOOLEAN]: H's covering snapshot part cannot be read
    entryUndecodable, \* [Hours -> BOOLEAN]: H's covering part carries an entry whose identity cannot be decoded
    partCorruptionUsed,  \* ghost: a part was marked unreadable (bounds state, one-shot)
    entryCorruptionUsed  \* ghost: an entry was marked undecodable (bounds state, one-shot)

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
               partWritten: BOOLEAN, baseVer: Nat, baseAbsent: BOOLEAN,
               startClock: Int, tombAtStage: [Hours -> BOOLEAN],
               reconcileLo: Int, frontierReconciled: SUBSET Hours]
ClearedStage == [active |-> FALSE, wm |-> -1, entries |-> {},
                 partWritten |-> FALSE, baseVer |-> 0, baseAbsent |-> FALSE,
                 startClock |-> -1, tombAtStage |-> [H \in Hours |-> FALSE],
                 reconcileLo |-> -1, frontierReconciled |-> {}]
Statuses   == {"absent", "valid", "corrupt", "unsupported"}
SnapPart   == [wm: Int, entries: SUBSET AllEntries, at: Int]

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

\* The fold's classify + reconcile output at watermark w, computed from a full
\* rescan of every hour <= w: live L0 (present, not superseded, not tombstoned)
\* plus published L1 parts. This is the OMNISCIENT view; the fold does NOT use it
\* directly (that would be the oracle the review flagged). It is the fresh-scan
\* component of the bounded incremental fold below, and the definition of "fully
\* reconciled" that the bounded fold is deliberately weaker than.
\* FoldNamesEntryAboveWatermark drops the e[2] <= w bound, letting a fold's scan
\* admit an entry from an hour it has not sealed through, so a published HEAD
\* can name a commit above its own watermark: the mutant
\* SnapshotEntriesBelowWatermark pins. FoldIncludesTombstonedEntries drops the
\* ~tomb[e[2]] filter, letting a retired bucket reappear in a fresh scan: the
\* mutant TombstonedBucketContributesNothing pins.
FoldEntriesFor(w) ==
    { e \in AllEntries :
        /\ (e[2] <= w \/ FoldNamesEntryAboveWatermark)
        /\ (~tomb[e[2]] \/ FoldIncludesTombstonedEntries)
        /\ ( \/ (e[1] = "l0" /\ e[3] \in l0[e[2]] /\ e[3] \notin SupersededInputs(e[2]))
             \/ (e[1] = "l1" /\ crec[e[2]][e[3]].used) ) }

\* The fold reconcile window (docs/catalog-and-mvcc.md: fold_reconcile_window_
\* hours, 26h in production). The model fixes it at its TIGHTEST value, 0, so
\* only the boundary hour and newly sealed hours are re-reconciled and every hour
\* strictly below the previous watermark is carried forward unchanged. A larger
\* window re-reconciles strictly MORE, so any safety property that holds at W = 0
\* holds at every real window; W = 0 is the worst case for carry-forward
\* staleness and the one the model checks.
ReconcileWindow == 0

\* ADR-0020 retention-frontier reconcile (docs/catalog-and-mvcc.md,
\* "Retention-frontier reconcile"): independent of the fixed window below, a
\* fold may also re-diff one carried-forward hour, applying the identical
\* tombstone filter the fresh scan uses. Unlike a superseded L0 input (safe to
\* carry forever, held by the sweep gate), a tombstoned bucket left below the
\* floor blocks retention permanently without this pass: nothing else ever
\* drops it from HEAD, so the sweep's HEAD-reachability gate never clears and
\* the objects are never deleted. The real fold bounds this to
\* frontier_reconcile_max_hours (168 in production) and drains a backlog
\* oldest-first over many folds; the model checks the tightest case, one hour
\* per fold, since each hour's tombstone filtering is independent of every
\* other hour's: what a multi-hour batch proves reconciled in a single fold, a
\* run of single-hour folds proves the same way across several, the same
\* worst-case-stands-in-for-every-case argument ReconcileWindow = 0 uses below.
FrontierAdmits(FH, e) == e[2] \in FH => (~tomb[e[2]] \/ FoldIncludesTombstonedEntries)

\* The bounded incremental fold (crates/ravel-catalog/src/fold.rs::reconcile_one_
\* bucket over the incremental range, plus the carried-forward prefix of the
\* prior snapshot). Advancing a valid HEAD from wmOld to wmNew:
\*   - hours < wmOld - ReconcileWindow are CARRIED FORWARD verbatim from the
\*     prior snapshot (head.entries), NOT rescanned, UNLESS the frontier pass
\*     (FH, at most one hour) picked one of them: that hour still gets the
\*     tombstone filter. A compaction that supersedes one of their L0 inputs
\*     after this hour left the window is NOT reflected by either pass: the
\*     snapshot keeps naming the pre-compaction L0 input. This is the bound
\*     the review required in place of the oracle.
\*   - hours >= wmOld - ReconcileWindow (the window plus every newly sealed hour
\*     up to wmNew) are freshly reconciled from the store.
\* A rebuild from an absent or corrupt HEAD has no prior snapshot to carry, so it
\* falls back to a full scan (FoldEntriesFor). The safety of the carried-forward
\* stale naming rests on the object-granular HEAD-gated sweep: a superseded input
\* the snapshot still names is held in the store (HeadNamedObjectNeverDeleted),
\* and a query serves it (correct data, merely uncompacted) with query-time dedup
\* collapsing any duplicate identity (SignalDedupContract). See #7 in results.md.
IncrementalFoldEntries(wmNew, FH) ==
    IF head.status = "valid"
    THEN LET reconcileLo == head.wm - ReconcileWindow IN
           { e \in head.entries : e[2] < reconcileLo /\ FrontierAdmits(FH, e) }
             \cup { e \in FoldEntriesFor(wmNew) : e[2] >= reconcileLo }
    ELSE FoldEntriesFor(wmNew)

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

\* The catalog-object sweep reclaims a part only when no HEAD names it AND the
\* part has aged past the fold-lifetime bound. The age gate is store-derived
\* (the part's own write clock, read back from the object), NOT a peek at any
\* in-flight fold's private state: the sweep cannot observe another process's
\* uncommitted fold. Because a fold must CAS or rebase within ProtectionHorizon
\* of its start (DoFoldCas / DoFoldRebase) and its part is written no earlier
\* than that start, a part a live fold may still name is younger than the gate,
\* so it is never swept out from under an in-flight CAS. See #9 in results.md.
SweepablePart(p) ==
    /\ ~PartNamedByHead(p)
    /\ clock - p.at >= ProtectionHorizon

\* The object backing a pinned entry has been swept away.
ObjectDeleted(e) ==
    IF e[1] = "l0" THEN e[3] \notin l0[e[2]] ELSE ~crec[e[2]][e[3]].used

\* --- query dedup semantics (crates/ravel-query/src/engine.rs::is_greater) ----
\* Which identity r a served entry contributes: an L0 record serves its own
\* identity; an L1 part serves each identity in its output set.
Serves(e, r) ==
    \/ (e[1] = "l0" /\ e[3] = r)
    \/ (e[1] = "l1" /\ r \in crec[e[2]][e[3]].out)
\* The identities a served entry set serves more than once (a two-source
\* conflict): recomputed on whatever set was actually served, so a broken
\* dedup is visible on the served entries themselves, never on a control flag.
RawDupIdentities(P) == { r \in Records : Cardinality({ e \in P : Serves(e, r) }) > 1 }

\* The metrics query collapses a two-source conflict to a single served entry
\* per identity; logs/spans (or the DropMetricsDedup mutant) serve every
\* source untouched. Dedup performs that collapse on the served set itself, so
\* RawDupIdentities recomputed on its output is a store-derived witness, not a
\* restatement of DedupApplies.
DedupApplies == DedupBySignal /\ ~DropMetricsDedup
Sources(r, P) == { e \in P : Serves(e, r) }

\* Survivor selection, most-constrained identity first: an identity with fewer
\* sources is processed before one with more, so a forced survivor (an
\* identity with exactly one source) is protected before a flexible identity
\* picks an independent alternative that would otherwise starve it of that
\* same shared source. An identity whose source set already overlaps
\* Protected reuses the existing survivor instead of adding one, which is what
\* keeps a shared L1 part serving two identities at once from also counting as
\* two of Sources(r, P) for either. Picking per identity independently (the
\* prior definition) had no such ordering: two identities sharing one L1
\* source could choose different survivors, and the shared source then fell
\* out of both removal sets' complement and was dropped, leaving the identity
\* whose only source that was with none (issue #1121 Finding 1).
RECURSIVE DedupSurvivors(_, _, _)
DedupSurvivors(P, RS, Protected) ==
    IF RS = {} THEN Protected
    ELSE LET r == CHOOSE x \in RS :
                 \A y \in RS : Cardinality(Sources(x, P)) <= Cardinality(Sources(y, P))
             Src == Sources(r, P)
         IN IF \/ Src = {}
               \/ Src \cap Protected # {}
            THEN DedupSurvivors(P, RS \ {r}, Protected)
            ELSE DedupSurvivors(P, RS \ {r}, Protected \cup {CHOOSE e \in Src : TRUE})

Dedup(P) ==
    IF ~DedupApplies THEN P
    ELSE LET Protected == DedupSurvivors(P, Records, {})
         IN { e \in P : \/ e \in Protected
                        \/ ~ \E r \in Records : Serves(e, r) }

\* Whether the reader can resolve a snapshot from HEAD. HeadNamesOnlyComplete
\* Parts already proves a valid HEAD always names an existing snapshot part
\* (the sweep guard never reclaims a part a live HEAD names), so this reduces
\* to HEAD validity; any other status (absent/corrupt/unsupported) leaves no
\* index to read. Store-derived: reads head.status, not a flag.
IndexReadable == head.status = "valid"

\* What the store says a reader with a readable index would see: the pinned
\* snapshot when the index is readable, the direct listing otherwise. Free of
\* QueryFailsClosedOnMissingIndex, so the fail-open contract has a witness the
\* switch cannot corrupt.
FallbackView == IF IndexReadable THEN head.entries ELSE ListingView

\* The view a query actually serves: the listing fallback, or nothing at all
\* when QueryFailsClosedOnMissingIndex wrongly serves empty instead of
\* degrading; then deduped the way a real reader dedups what it served.
QueryServedView ==
    Dedup(IF QueryFailsClosedOnMissingIndex /\ ~IndexReadable THEN {} ELSE FallbackView)

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
               pinned: SUBSET AllEntries, pinnedAtAttempt: SUBSET AllEntries,
               resolvedView: SUBSET AllEntries, dupServed: SUBSET Records,
               headStatusAtResolve: {"none"} \cup Statuses]
    /\ lastHead \in [kind: {"none", "fold", "recTick", "corrupt", "unsupported"},
                     wmBefore: Int, wmAfter: Int, entriesChanged: BOOLEAN,
                     entries: SUBSET AllEntries, tombAtWrite: [Hours -> BOOLEAN],
                     reconcileLo: Int, frontierReconciled: SUBSET Hours]
    /\ lastDelete \in [happened: BOOLEAN, headStatus: {"none"} \cup Statuses,
                       partUnreadable: BOOLEAN, entryUndecodable: BOOLEAN]
    /\ lastCompact \in [loserFired: BOOLEAN,
                        outcome: {"none", "converged", "diverged", "overwrite"}]
    /\ maxValidWm \in Int
    /\ corruptionUsed \in BOOLEAN
    /\ unsupportedUsed \in BOOLEAN
    /\ partUnreadable \in [Hours -> BOOLEAN]
    /\ entryUndecodable \in [Hours -> BOOLEAN]
    /\ partCorruptionUsed \in BOOLEAN
    /\ entryCorruptionUsed \in BOOLEAN

NoTomb == [H \in Hours |-> FALSE]

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
    /\ qy = [phase |-> "idle", attempt |-> 0, pinned |-> {},
             pinnedAtAttempt |-> {}, resolvedView |-> {}, dupServed |-> {},
             headStatusAtResolve |-> "none"]
    /\ lastHead = [kind |-> "none", wmBefore |-> -1, wmAfter |-> -1,
                   entriesChanged |-> FALSE, entries |-> {}, tombAtWrite |-> NoTomb,
                   reconcileLo |-> -1, frontierReconciled |-> {}]
    /\ lastDelete = [happened |-> FALSE, headStatus |-> "none",
                     partUnreadable |-> FALSE, entryUndecodable |-> FALSE]
    /\ lastCompact = [loserFired |-> FALSE, outcome |-> "none"]
    /\ maxValidWm = -1
    /\ corruptionUsed = FALSE
    /\ unsupportedUsed = FALSE
    /\ partUnreadable = [H \in Hours |-> FALSE]
    /\ entryUndecodable = [H \in Hours |-> FALSE]
    /\ partCorruptionUsed = FALSE
    /\ entryCorruptionUsed = FALSE

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
    /\ UNCHANGED <<clock, crec, lastCompact, tomb, head, snapParts, foldStage, qy, lastHead,
                   lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
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
    /\ UNCHANGED <<clock, l0, lastCompact, tomb, head, snapParts, foldStage, qy,
                   lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* A second compactor reaches PUT after the winner already published this record
\* (crates/ravel-maintain/src/publish.rs::resolve_already_exists). The publish is
\* CreateIfAbsent, so the loser's PUT returns AlreadyExists and the loser reads
\* the winner's record back. Real resolve_already_exists then takes one of two
\* non-error-free-of-mutation outcomes, neither of which ever overwrites the
\* immutable record: Converged when the loser's own recomputed input set
\* matches the winner's (adopts the winner's record as-is; a real repair re-PUT
\* of a missing but reproducible L1 part is not itself a crec mutation, and this
\* model has no separate part-presence store state to represent the distinct
\* ConvergedWinnerPartMissing fail-closed case), or the typed error
\* InputSetHashDivergence when it does not (the loser classified a later view of
\* l0[H] -- a commit landed after the winner published, inside the
\* maintenance-seal window -- so its recomputed record differs; the mismatch is
\* reported, not silently resolved, and the winner's record is still left
\* untouched). CompactionLoserOverwrites breaks both real outcomes at once: the
\* loser overwrites the published record with its own output regardless of
\* whether the input sets matched. The witness lastCompact.outcome records
\* which of the three the transition took and whether the store was mutated
\* (only "overwrite" ever mutates it), so CompactionRecordImmutable observes
\* the store effect, not a flag the action sets about itself.
DoCompactLoser(H, g) ==
    /\ crec[H][g].used
    /\ l0[H] # {}
    /\ ~tomb[H]
    /\ LET existing == crec[H][g]
           inputs   == l0[H]
           outcome  == IF CompactionLoserOverwrites THEN "overwrite"
                       ELSE IF inputs = existing.in THEN "converged"
                       ELSE "diverged"
           result   == IF outcome = "overwrite"
                       THEN [used |-> TRUE, in |-> inputs, out |-> inputs, at |-> clock]
                       ELSE existing
       IN
        /\ crec' = [crec EXCEPT ![H][g] = result]
        /\ lastCompact' = [loserFired |-> TRUE, outcome |-> outcome]
    /\ UNCHANGED <<clock, budget, l0, tomb, head, snapParts, foldStage, qy,
                   lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* --- folder: read HEAD and stage a fold -------------------------------------
\* crates/ravel-catalog/src/fold.rs::sealed_watermark_hour and
\* Catalog::reconcile_one_bucket. A fold starts only to advance the watermark on
\* a valid HEAD, or to rebuild an absent/corrupt HEAD (fold clobbers a corrupt
\* HEAD). An unsupported HEAD is terminal: the folder never starts.
DoFoldStart(f) ==
    /\ ~foldStage[f].active
    /\ head.status # "unsupported"
    /\ \E oh \in Hours \cup {-1} :
        LET w == FoldWatermark
            FH == IF oh = -1 THEN {} ELSE {oh}
        IN
            /\ w >= 0
            /\ ( \/ head.status \in {"absent", "corrupt"}
                 \/ (head.status = "valid" /\ w > head.wm) )
            /\ foldStage' = [foldStage EXCEPT ![f] =
                  [active |-> TRUE, wm |-> w, entries |-> IncrementalFoldEntries(w, FH),
                   partWritten |-> FALSE, baseVer |-> store[HK].version,
                   baseAbsent |-> (head.status = "absent"), startClock |-> clock,
                   tombAtStage |-> tomb, frontierReconciled |-> FH,
                   reconcileLo |-> IF head.status = "valid"
                                   THEN head.wm - ReconcileWindow ELSE -1]]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, snapParts, qy, lastHead,
                   lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* crates/ravel-catalog/src/fold.rs::part_object_key: the snapshot part is
\* content-addressed and published before HEAD names it.
DoFoldPutPart(f) ==
    /\ foldStage[f].active
    /\ ~foldStage[f].partWritten
    /\ snapParts' = snapParts \cup
         { [wm |-> foldStage[f].wm, entries |-> foldStage[f].entries, at |-> clock] }
    /\ foldStage' = [foldStage EXCEPT ![f].partWritten = TRUE]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, qy, lastHead,
                   lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* crates/ravel-catalog/src/fold.rs::Catalog::get_head with MAX_HEAD_CAS_ATTEMPTS:
\* the version-matched CAS serializes racing folders. A win publishes the new
\* snapshot atomically; a loser's CAS no-ops and it re-reads HEAD to retry
\* (clears its stage and rebases). Correct: a loser NEVER overwrites HEAD with
\* its stale staged set. LostCasProceedsOnStaleRead breaks exactly that store
\* effect: the loser writes its stale snapshot, regressing the watermark below
\* a live commit the winner already published.
\*
\* Fold-lifetime bound (docs/catalog-and-mvcc.md, MVCC rules): a fold commits
\* within the protection horizon of the state it listed at. The horizon
\* (max_query_duration + grace) is sized to outlast any in-flight reader or
\* fold, so a compaction published after this fold's listing cannot have its
\* superseded L0 inputs swept before this fold's CAS lands. A fold that outran
\* the horizon must rebase (DoFoldRebase) rather than publish a stale snapshot.
DoFoldCas(f) ==
    /\ foldStage[f].active
    /\ clock - foldStage[f].startClock < ProtectionHorizon
    /\ (HeadNamesUnwrittenPart \/ foldStage[f].partWritten)
    /\ LET st == foldStage[f]
           won == IF st.baseAbsent THEN ~store[HK].present
                  ELSE (store[HK].present /\ store[HK].version = st.baseVer)
           staleWrite == LostCasProceedsOnStaleRead
       IN
        /\ IF st.baseAbsent THEN PutCreateIfAbsent(HK, HeadContentVal)
                            ELSE PutCasVersion(HK, st.baseVer, HeadContentVal)
        /\ head' = IF won \/ staleWrite
                   THEN [status |-> "valid", wm |-> st.wm, entries |-> st.entries]
                   ELSE head
        /\ lastHead' = IF won \/ staleWrite
                       THEN [kind |-> "fold", wmBefore |-> head.wm, wmAfter |-> st.wm,
                             entriesChanged |-> (st.entries # head.entries),
                             entries |-> st.entries, tombAtWrite |-> st.tombAtStage,
                             reconcileLo |-> st.reconcileLo,
                             frontierReconciled |-> st.frontierReconciled]
                       ELSE lastHead
        /\ maxValidWm' = IF won /\ st.wm > maxValidWm THEN st.wm ELSE maxValidWm
        /\ foldStage' = [foldStage EXCEPT ![f] = ClearedStage]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, snapParts, qy,
                   lastDelete, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>

\* --- rival folder: a concurrent process wins the HEAD CAS -------------------
\* crates/ravel-catalog/src/fold.rs::get_head::MAX_HEAD_CAS_ATTEMPTS. A second
\* catalog process folds current state and wins the version-matched CAS,
\* advancing HEAD and bumping the object version. Any in-flight fold of the
\* modelled folder now holds a stale base version and will lose its own CAS.
\* This is what makes the lost-CAS race reachable with a single modelled folder
\* and NoLiveCommitOmittedByLostCas load-bearing: without a concurrent winner
\* that advanced the watermark, a loser's staged set can never be staler than
\* HEAD.
DoRivalFoldWin ==
    /\ head.status = "valid"
    /\ FoldWatermark > head.wm
    /\ \E oh \in Hours \cup {-1} :
        LET w == FoldWatermark
            FH == IF oh = -1 THEN {} ELSE {oh}
            es == IncrementalFoldEntries(FoldWatermark, FH)
        IN
            /\ PutOverwrite(HK, HeadContentVal)
            /\ head' = [status |-> "valid", wm |-> w, entries |-> es]
            /\ snapParts' = snapParts \cup { [wm |-> w, entries |-> es, at |-> clock] }
            /\ lastHead' = [kind |-> "fold", wmBefore |-> head.wm, wmAfter |-> w,
                            entriesChanged |-> (es # head.entries), entries |-> es,
                            tombAtWrite |-> tomb, reconcileLo |-> head.wm - ReconcileWindow,
                            frontierReconciled |-> FH]
            /\ maxValidWm' = IF w > maxValidWm THEN w ELSE maxValidWm
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, foldStage, qy,
                   lastDelete, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>

\* A folder crash between staging and the CAS: HEAD is untouched, and the
\* published part is left orphaned in snapParts for a later catalog-object sweep.
DoFoldCrash(f) ==
    /\ foldStage[f].active
    /\ foldStage' = [foldStage EXCEPT ![f] = ClearedStage]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, snapParts, qy, lastHead,
                   lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* A fold that outran the protection horizon (its listing is older than the
\* window that keeps its inputs alive) abandons its staged snapshot and rebases,
\* rather than CAS-publishing a HEAD that names a since-swept input. The next
\* fold re-lists current state. This is the release valve that keeps the
\* fold-lifetime bound in DoFoldCas from stranding a folder.
DoFoldRebase(f) ==
    /\ foldStage[f].active
    /\ clock - foldStage[f].startClock >= ProtectionHorizon
    /\ foldStage' = [foldStage EXCEPT ![f] = ClearedStage]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, snapParts, qy, lastHead,
                   lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* --- reconcile off a plain tick (negative control only) ---------------------
\* crates/ravel-catalog/src/fold.rs::Catalog::reconcile_one_bucket runs inside a
\* watermark-advancing fold. Enabled only under ReconcileOnTick: it re-reconciles
\* HEAD without advancing the watermark, which the witness records as a reconcile
\* whose step did not raise the watermark.
DoReconcileTick ==
    /\ ReconcileOnTick
    /\ head.status = "valid"
    /\ LET e2 == FoldEntriesFor(head.wm) IN
        /\ head' = [head EXCEPT !.entries = e2]
        /\ lastHead' = [kind |-> "recTick", wmBefore |-> head.wm, wmAfter |-> head.wm,
                        entriesChanged |-> (e2 # head.entries), entries |-> e2,
                        tombAtWrite |-> tomb, reconcileLo |-> -1, frontierReconciled |-> {}]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, snapParts, foldStage, qy,
                   lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* A valid HEAD names the L0 identity r in hour H.
HeadNamesL0(H, r) == head.status = "valid" /\ <<"l0", H, r>> \in head.entries

\* --- sweeper: sweep_superseded (HEAD-gated; ADR-0020 delete blocker) ----------
\* crates/ravel-maintain/src/sweep.rs::sweep_superseded_impl deletes an L1
\* record's L0 inputs once past the protection horizon, but object-granularly
\* HOLDS any input a valid HEAD snapshot still names, and holds everything when
\* the reachability gate cannot prove otherwise. The real gate
\* (crates/ravel-maintain/src/reachability.rs::SnapshotReachability::object_gate)
\* fails closed on three distinct triggers, each with its own code symbol: HEAD
\* itself unreadable (covering_parts / ensure_head), a covering snapshot part
\* present but undecodable (ensure_part, checked directly in object_gate and
\* again in clear_or_block_on_skipped for a part the hour-range filter would
\* otherwise skip), and a decoded entry whose identity fields do not fit the
\* shape a fold writes (snapshot_object returning None inside object_gate). All
\* three are modeled here: head.status covers the first, partUnreadable[H] the
\* second, entryUndecodable[H] the third. The witness lastDelete records all
\* three as observed at this real deletion, so a mutant that removes any one
\* guard is caught by CorruptHeadFailsClosedOnDeletePaths. The
\* SweepSupersededNoHeadGate switch drops the object-granular hold, reproducing
\* the pre-ADR-0020 (issue #1134) shape. DeletePathIgnoresUnreadableHead,
\* DeletePathIgnoresUnreadablePart, and DeletePathIgnoresUndecodableEntry each
\* drop exactly one of the three guards, so the pass can fire on that one
\* unreadable trigger alone and lastDelete records it, the mutant
\* CorruptHeadFailsClosedOnDeletePaths pins.
DoSweepSuperseded(H, g) ==
    /\ crec[H][g].used
    /\ (head.status \in {"valid", "absent"} \/ DeletePathIgnoresUnreadableHead)
    /\ (~partUnreadable[H] \/ DeletePathIgnoresUnreadablePart)
    /\ (~entryUndecodable[H] \/ DeletePathIgnoresUndecodableEntry)
    /\ clock - crec[H][g].at >= ProtectionHorizon
    /\ LET inputs == crec[H][g].in \cap l0[H]
           deletable == IF SweepSupersededNoHeadGate
                        THEN inputs
                        ELSE { r \in inputs : ~HeadNamesL0(H, r) }
       IN
        /\ deletable # {}
        /\ l0' = [l0 EXCEPT ![H] = @ \ deletable]
    /\ lastDelete' = [happened |-> TRUE, headStatus |-> head.status,
                      partUnreadable |-> partUnreadable[H], entryUndecodable |-> entryUndecodable[H]]
    /\ UNCHANGED <<clock, budget, crec, lastCompact, tomb, head, snapParts, foldStage, qy,
                   lastHead, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* crates/ravel-maintain/src/sweep.rs::sweep_unreferenced_catalog_objects reads
\* the HEAD reference (read_head_reference) and deletes an orphan snapshot part.
\* Unlike object_gate above, this function decodes only HEAD itself (the part
\* refs it names live inside the HEAD object, not in a separately fetched
\* covering part), so it has exactly one fail-closed trigger: it removes an
\* object only on a readable HEAD, and a corrupt or unsupported HEAD fails the
\* whole pass closed; lastDelete records the observed status.
\* DeletePathIgnoresUnreadableHead drops that guard, the same as in
\* DoSweepSuperseded above. partUnreadable/entryUndecodable do not apply to
\* this action; lastDelete always records them FALSE here.
DoSweepCatalogObjects ==
    /\ (head.status \in {"valid", "absent"} \/ DeletePathIgnoresUnreadableHead)
    /\ \E p \in snapParts : SweepablePart(p)
    /\ LET orphan == CHOOSE p \in snapParts : SweepablePart(p) IN
         snapParts' = snapParts \ {orphan}
    /\ lastDelete' = [happened |-> TRUE, headStatus |-> head.status,
                      partUnreadable |-> FALSE, entryUndecodable |-> FALSE]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, foldStage, qy, lastHead,
                   maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* --- environment: a covering snapshot part becomes unreadable ---------------
\* crates/ravel-maintain/src/reachability.rs::SnapshotReachability::ensure_part:
\* a snapshot part HEAD names as covering some hour is present but cannot be
\* decoded (checksum/hash mismatch, unsupported version, or missing). One-shot
\* (bounded by partCorruptionUsed) and fixed to a single canonical hour, not
\* quantified over Hours in Next, so it adds exactly one branch at every
\* reachable state, the same shape as DoCorruptHead (which also acts on the
\* whole HEAD register with no Hours parameter) rather than |Hours| branches.
\* That one branch at every reachable state is still enough to roughly double
\* a large graph, and paired with DoPoisonEntry below it grew exhaustive's
\* state count 4x and its wall clock 19x (issue #1121 round three finding 1;
\* see results.md). EnableDeletePathCorruption gates both actions out of Next
\* entirely in configs that do not need the covering-part/entry triggers on
\* top of the HEAD-status one; results.md says which configs keep it TRUE.
DoCorruptPart ==
    LET H == CHOOSE h \in Hours : TRUE IN
        /\ EnableDeletePathCorruption
        /\ ~partCorruptionUsed
        /\ ~partUnreadable[H]
        /\ partUnreadable' = [partUnreadable EXCEPT ![H] = TRUE]
        /\ partCorruptionUsed' = TRUE
        /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, snapParts, foldStage, qy,
                       lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed,
                       entryUndecodable, entryCorruptionUsed>>
        /\ UNCHANGED svTuple

\* --- environment: a covering part's entry identity becomes undecodable ------
\* crates/ravel-maintain/src/reachability.rs::snapshot_object: a decoded
\* snapshot entry covering some hour has identity fields that do not fit the
\* shape a fold writes (a malformed writer_id/input_set_hash/part_index), so
\* object_gate cannot decide whether it names a delete candidate. One-shot
\* (bounded by entryCorruptionUsed), independent of whether the part itself is
\* also unreadable, and fixed to a single canonical hour for the same
\* branching-factor reason as DoCorruptPart above, including the same
\* EnableDeletePathCorruption gate.
DoPoisonEntry ==
    LET H == CHOOSE h \in Hours : TRUE IN
        /\ EnableDeletePathCorruption
        /\ ~entryCorruptionUsed
        /\ ~entryUndecodable[H]
        /\ entryUndecodable' = [entryUndecodable EXCEPT ![H] = TRUE]
        /\ entryCorruptionUsed' = TRUE
        /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, snapParts, foldStage, qy,
                       lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed,
                       partUnreadable, partCorruptionUsed>>
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
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, head, snapParts, foldStage, qy,
                   lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* --- query: resolve, pin, retry once ----------------------------------------
\* crates/ravel-query/src/engine.rs::QueryEngine::resolve_snapshot_with_retry.
\* A valid HEAD with a readable index part pins the snapshot; any other state
\* degrades to a listing (fail-open), never an error. pinned/pinnedAtAttempt
\* record what the resolve SERVED (via QueryServedView); resolvedView records
\* what the store SAID should be served (the fail-open witness, computed
\* without QueryFailsClosedOnMissingIndex), so a mutant that serves an empty
\* result on an unreadable index diverges the two. dupServed records the
\* identities the query returned more than once.
DoQueryResolve ==
    /\ qy.phase = "idle"
    /\ LET v == QueryServedView IN
        qy' = [phase |-> "pinned", attempt |-> 1,
               pinned |-> v, pinnedAtAttempt |-> v,
               resolvedView |-> Dedup(FallbackView), dupServed |-> RawDupIdentities(v),
               headStatusAtResolve |-> head.status]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, snapParts, foldStage,
                   lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* A pinned entry whose object was swept forces one re-resolve; a second miss is
\* terminal (crates/ravel-query/src/error.rs::QueryError::SnapshotInvalidated).
DoQueryRun ==
    /\ qy.phase = "pinned"
    /\ IF \A e \in qy.pinned : ~ObjectDeleted(e)
         THEN qy' = [qy EXCEPT !.phase = "done"]
         ELSE IF qy.attempt < 2
                THEN LET v == QueryServedView IN
                     qy' = [phase |-> "pinned", attempt |-> 2,
                            pinned |-> v, pinnedAtAttempt |-> v,
                            resolvedView |-> Dedup(FallbackView), dupServed |-> RawDupIdentities(v),
                            headStatusAtResolve |-> head.status]
                ELSE qy' = [qy EXCEPT !.phase = "invalid"]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, snapParts, foldStage,
                   lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* A pinned snapshot must not change under a query mid-attempt. Enabled only
\* under SnapshotChangesMidAttempt, which mutates the served (pinned) set in
\* place without a re-resolve.
DoQueryTamper ==
    /\ SnapshotChangesMidAttempt
    /\ qy.phase = "pinned"
    /\ qy.pinned # {}
    /\ qy' = [qy EXCEPT !.pinned = qy.pinned \ {CHOOSE e \in qy.pinned : TRUE}]
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, head, snapParts, foldStage,
                   lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* --- environment ------------------------------------------------------------
DoTick ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<budget, l0, crec, lastCompact, tomb, head, snapParts, foldStage, qy,
                   lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* HEAD bytes become undecodable; the object stays present (same version). The
\* folder will clobber and rebuild (wm reset to unknown).
DoCorruptHead ==
    /\ ~corruptionUsed
    /\ head.status = "valid"
    /\ head' = [status |-> "corrupt", wm |-> -1, entries |-> {}]
    /\ lastHead' = [kind |-> "corrupt", wmBefore |-> head.wm, wmAfter |-> -1,
                    entriesChanged |-> (head.entries # {}), entries |-> {},
                    tombAtWrite |-> tomb, reconcileLo |-> -1, frontierReconciled |-> {}]
    /\ corruptionUsed' = TRUE
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, snapParts, foldStage, qy,
                   lastDelete, maxValidWm, unsupportedUsed,
                   partUnreadable, entryUndecodable, partCorruptionUsed, entryCorruptionUsed>>
    /\ UNCHANGED svTuple

\* A newer-format writer publishes an unsupported HEAD version (object present,
\* version bumped). This is terminal for the folder.
DoUnsupportedHead ==
    /\ ~unsupportedUsed
    /\ head.status \in {"absent", "valid"}
    /\ PutOverwrite(HK, HeadContentVal)
    /\ head' = [status |-> "unsupported", wm |-> -1, entries |-> {}]
    /\ lastHead' = [kind |-> "unsupported", wmBefore |-> head.wm, wmAfter |-> -1,
                    entriesChanged |-> (head.entries # {}), entries |-> {},
                    tombAtWrite |-> tomb, reconcileLo |-> -1, frontierReconciled |-> {}]
    /\ unsupportedUsed' = TRUE
    /\ UNCHANGED <<clock, budget, l0, crec, lastCompact, tomb, snapParts, foldStage, qy,
                   lastDelete, maxValidWm, corruptionUsed,
                   partUnreadable, entryUndecodable, partCorruptionUsed, entryCorruptionUsed>>

Next ==
    \/ \E H \in Hours, r \in Records : DoCommit(H, r)
    \/ \E H \in Hours, g \in CompIds : DoCompact(H, g)
    \/ \E H \in Hours, g \in CompIds : DoCompactLoser(H, g)
    \/ \E f \in Folders : DoFoldStart(f)
    \/ \E f \in Folders : DoFoldPutPart(f)
    \/ \E f \in Folders : DoFoldCas(f)
    \/ DoRivalFoldWin
    \/ \E f \in Folders : DoFoldCrash(f)
    \/ \E f \in Folders : DoFoldRebase(f)
    \/ DoReconcileTick
    \/ \E H \in Hours, g \in CompIds : DoSweepSuperseded(H, g)
    \/ DoSweepCatalogObjects
    \/ \E H \in Hours : DoTombstone(H)
    \/ DoQueryResolve
    \/ DoQueryRun
    \/ DoQueryTamper
    \/ DoTick
    \/ DoCorruptHead
    \/ DoUnsupportedHead
    \/ DoCorruptPart
    \/ DoPoisonEntry

vars == <<store, lastModified, versionCounter, uploads, listState,
          clock, budget, l0, crec, lastCompact, tomb, head, snapParts, foldStage, qy,
          lastHead, lastDelete, maxValidWm, corruptionUsed, unsupportedUsed, partUnreadable, entryUndecodable,
                   partCorruptionUsed, entryCorruptionUsed>>

Spec == Init /\ [][Next]_vars

\* --- fairness (liveness only) -----------------------------------------------
\* Weak fairness on watermark advance (the folder makes progress: start, put
\* part, and CAS), on the clock, and on the query, so a started query eventually
\* finishes (QueryTerminates). Fairness is on the folder's own progress actions
\* the implementation justifies (a fold that starts eventually CASes or rebases,
\* a background loop the runtime always drives), never over the whole next-state
\* relation, and no safety property depends on it.
FoldProgress == \E f \in Folders :
    (DoFoldStart(f) \/ DoFoldPutPart(f) \/ DoFoldCas(f) \/ DoFoldRebase(f))
QueryProgress == DoQueryResolve \/ DoQueryRun

FairSpec ==
    /\ Spec
    /\ WF_vars(DoTick)
    /\ WF_vars(FoldProgress)
    /\ WF_vars(QueryProgress)

----------------------------------------------------------------------------
\* Named safety invariants.

\* HEAD only ever names a snapshot part that was fully published first.
\* Store-derived: reads the HEAD register and the set of present parts.
\* (Broken by HeadNamesUnwrittenPart, which CASes HEAD naming a never-PUT part.)
HeadNamesOnlyCompleteParts ==
    head.status = "valid" =>
        \E p \in snapParts : p.wm = head.wm /\ p.entries = head.entries

\* Every published compaction record preserves its input multiset exactly.
\* Store-derived: reads the L1 compaction-record plane. The counts-only runtime
\* gate permits the identity swap this catches.
\* (Broken by CompactionSwapsRecord.)
CompactionPreservesMultiset ==
    \A H \in Hours : \A g \in CompIds :
        crec[H][g].used => crec[H][g].out = crec[H][g].in

\* A losing compaction publish never mutates the record the winner already
\* published, regardless of which of the real resolve_already_exists outcomes
\* it took: on AlreadyExists it reads the winner back and adopts it whether its
\* own recomputed input set converged or diverged from the winner's, so the
\* immutable L1 object is untouched either way. Witness-derived:
\* lastCompact.outcome is set from the loser's transition; only "overwrite"
\* ever changes the stored record. (Broken by CompactionLoserOverwrites, which
\* re-PUTs the loser's own output over the winner regardless of outcome.)
CompactionRecordImmutable ==
    lastCompact.loserFired => lastCompact.outcome # "overwrite"

\* A reconcile step that changed HEAD's entries also raised the watermark, so
\* reconcile never runs off a plain tick. Witness-derived: lastHead records the
\* real wm delta and entry-change of the last HEAD write. (Broken by
\* ReconcileOnTick.)
ReconcileOnlyOnWatermarkAdvance ==
    (lastHead.kind \in {"fold", "recTick"} /\ lastHead.entriesChanged)
        => lastHead.wmAfter > lastHead.wmBefore

\* No HEAD snapshot entry sits above the watermark it was folded at.
\* Store-derived: reads the HEAD register.
SnapshotEntriesBelowWatermark ==
    head.status = "valid" => \A e \in head.entries : e[2] <= head.wm

\* Within one query attempt the served snapshot is frozen. Witness-derived:
\* pinnedAtAttempt is the served view the resolve returned and never changes
\* within an attempt; pinned is the set the query serves now. They diverge only
\* if an action mutates the served set without a re-resolve.
\* (Broken by SnapshotChangesMidAttempt.)
PinnedSnapshotStableWithinAttempt ==
    qy.phase = "pinned" => qy.pinned = qy.pinnedAtAttempt

\* The winning fold omits no live commit, and a valid HEAD never regresses its
\* watermark below one already published. Store/witness-derived: reads the HEAD
\* register (head.wm, head.entries), the L0 plane, and maxValidWm (the highest
\* watermark any valid HEAD has held). The watermark-monotonicity clause is what
\* the lost-CAS case falsifies: a loser that overwrites HEAD on its stale read
\* rolls the watermark back below a commit the concurrent winner published.
\* (Broken by LostCasProceedsOnStaleRead.)
NoLiveCommitOmittedByLostCas ==
    head.status = "valid" =>
        /\ head.wm >= maxValidWm
        /\ \A H \in Hours :
            (H <= head.wm /\ ~tomb[H]) =>
                \A r \in l0[H] :
                    (r \notin SupersededInputs(H)) => <<"l0", H, r>> \in head.entries

\* A query that could not read a valid HEAD served the store listing rather
\* than erroring. Witness-derived: pinnedAtAttempt is what the resolve served;
\* resolvedView is what the store listing said should be served at that same
\* resolve, computed without QueryFailsClosedOnMissingIndex. The two coincide
\* by construction whenever the index was readable, so the invariant carries
\* no separate readable/unreadable antecedent: a fail-closed mutant (serve an
\* empty result on an unreadable HEAD) is the only way to diverge them, and it
\* does so unconditionally. Compared against pinnedAtAttempt rather than
\* pinned: both are written together by DoQueryResolve/DoQueryRun and never
\* by DoQueryTamper, so a mid-attempt tamper of pinned alone (the
\* SnapshotChangesMidAttempt mutant) cannot also trip this invariant.
MissingIndexDegradesToListing ==
    qy.pinnedAtAttempt = qy.resolvedView

\* A store deletion only ever ran while all three reachability-gate triggers
\* cleared: HEAD readable, its covering snapshot part readable, and the
\* covering entry's identity decodable. Witness-derived: lastDelete records the
\* HEAD status, partUnreadable, and entryUndecodable observed at the last real
\* object removal (DoSweepSuperseded or DoSweepCatalogObjects; the latter has
\* no part/entry trigger of its own, so it always records both FALSE).
\* Falsified by breaking any one of DoSweepSuperseded's three guards
\* (head.status, partUnreadable[H], entryUndecodable[H]) or DoSweepCatalogObjects's
\* head.status guard, so a deletion runs under that one unreadable trigger.
CorruptHeadFailsClosedOnDeletePaths ==
    lastDelete.happened => /\ lastDelete.headStatus \in {"valid", "absent"}
                           /\ ~lastDelete.partUnreadable
                           /\ ~lastDelete.entryUndecodable

\* Every object a valid HEAD names is still present. Store-derived: reads the
\* HEAD register, the L0 plane, and the L1 compaction-record plane. The
\* superseded sweep's object-granular HEAD-reachability gate provides this.
\* Broken by SweepSupersededNoHeadGate; see
\* counterexamples/sweep-superseded-no-head-gate.md.
HeadNamedObjectNeverDeleted ==
    head.status = "valid" => \A e \in head.entries : ~ObjectDeleted(e)

\* A fold contributes nothing for a bucket tombstoned at the time it reconciled
\* that bucket. Witness-derived: lastHead.entries is the entry set the last fold
\* wrote to HEAD, lastHead.tombAtWrite is the tombstone map that fold read, and
\* lastHead.frontierReconciled is the (at most one) below-floor hour the fold's
\* ADR-0020 retention-frontier pass separately re-diffed this cycle. The
\* guarantee covers the freshly reconciled range (hours at or above the fold's
\* reconcile floor, lastHead.reconcileLo, recorded at fold start from the
\* watermark the fold actually read, so a HEAD change between start and CAS, a
\* rival win or a corruption-then-heal, cannot shift it) AND any hour the
\* frontier pass separately picked, since both apply the identical tombstone
\* filter (FrontierAdmits); a rebuild records a floor of -1, below every hour,
\* so the whole snapshot is fresh regardless of the frontier pass. A
\* carried-forward hour below the floor that the frontier pass did NOT pick
\* this cycle is still exempt: a bucket tombstoned there is reflected once the
\* frontier pass reaches it (this fold or a later one) or the window returns to
\* it, exactly like a late supersession, and is safe meanwhile by the same
\* argument (the tombstoned bucket's objects stay held while HEAD names them).
\* Unlike a late supersession, this exemption is not permanent: the frontier
\* pass is what lets retention actually complete, since nothing else ever
\* drops a below-floor tombstoned hour from HEAD. An hour tombstoned AFTER a
\* fold reconciled it is legitimate and does not falsify this, because
\* tombAtWrite is frozen at the write. Falsified by dropping the ~tomb filter
\* (FoldIncludesTombstonedEntries), which defeats both the fresh scan and the
\* frontier pass since they share one check.
TombstonedBucketContributesNothing ==
    lastHead.kind = "fold" =>
        \A e \in lastHead.entries :
            (e[2] >= lastHead.reconcileLo \/ e[2] \in lastHead.frontierReconciled)
                => ~lastHead.tombAtWrite[e[2]]

\* Signal semantics: under metrics (query-time dedup) a query serves each
\* identity at most once even when two sources (an L0 record plus an L1 part, or
\* two L1 parts) name it. Witness-derived: dupServed records the identities the
\* query actually served more than once. A two-source conflict is reachable with
\* two compaction records, so the dedup does real work; dropping it (logs/spans,
\* or the DropMetricsDedup mutant on the query's dedup effect) makes dupServed
\* non-empty under metrics.
SignalDedupContract ==
    DedupBySignal => qy.dupServed = {}

\* Dedup must never remove the only source an identity had. Store-derived:
\* recomputes Dedup fresh against the current FallbackView rather than reading
\* any witness, so it checks the operator itself at every reachable state, not
\* just at a query resolve. Complements SignalDedupContract, which catches a
\* surviving duplicate but not a survivor dropped outright (issue #1121
\* Finding 1).
DedupPreservesCoverage ==
    \A r \in Records : Sources(r, FallbackView) # {} => Sources(r, Dedup(FallbackView)) # {}

\* Non-vacuity probe for the bounded incremental fold. True once a fold has
\* carried forward at least one entry from a hour below its reconcile floor
\* (i.e. the carry-forward branch of IncrementalFoldEntries did real work rather
\* than collapsing to a full rescan). Derived from lastHead, no extra state. Used
\* only as a refuted control: NoCarryForward is checked as an INVARIANT in a
\* dedicated config whose bounds make a second, watermark-advancing fold
\* reachable; TLC reporting NoCarryForward violated proves the incremental path is
\* exercised, so the safety pass over the same bounds is not vacuous.
CarryForwardExercised ==
    /\ lastHead.kind = "fold"
    /\ \E e \in lastHead.entries : e[2] < lastHead.reconcileLo

NoCarryForward == ~CarryForwardExercised

\* Non-vacuity probe for the ADR-0020 retention-frontier reconcile. True once
\* a fold picked a below-floor hour into its frontier set while that hour was
\* already tombstoned, i.e. the nondeterministic frontier choice in
\* DoFoldStart / DoRivalFoldWin fired on an hour where it does real work (the
\* same reachability argument CarryForwardExercised uses for the carry-forward
\* branch itself). Used only as a refuted control: NoFrontierReconcile is
\* checked as an INVARIANT in negative/frontier-reconcile-nonvacuity.cfg,
\* whose bounds make a second, watermark-advancing fold reachable after the
\* first hour is tombstoned; TLC reporting NoFrontierReconcile violated proves
\* the frontier pass is exercised, so TombstonedBucketContributesNothing's
\* frontierReconciled disjunct is not dead code.
FrontierReconcileExercised ==
    /\ lastHead.kind = "fold"
    /\ \E h \in lastHead.frontierReconciled :
        /\ h < lastHead.reconcileLo
        /\ lastHead.tombAtWrite[h]

NoFrontierReconcile == ~FrontierReconcileExercised

\* Non-vacuity probe for the widened compaction-loser outcome alphabet
\* (finding 6, ADR-1113 round two). True once a losing compaction publish
\* actually took the "diverged" branch: its own recomputed input set differed
\* from the already-published winner's, from a commit landing in l0[H] between
\* the winner's publish and the loser's retry. Derived from lastCompact, no
\* extra state. Used only as a refuted control: NoCompactionLoserDivergence is
\* checked as an INVARIANT in negative/compaction-loser-diverged-nonvacuity.cfg,
\* whose bounds make that commit-between-publish-and-retry interleaving
\* reachable; TLC reporting NoCompactionLoserDivergence violated proves the
\* "diverged" outcome is a real, reachable member of the alphabet, not a value
\* CompactionRecordImmutable's proof happens to pass through without ever
\* hitting.
CompactionLoserDiverged == lastCompact.outcome = "diverged"

NoCompactionLoserDivergence == ~CompactionLoserDiverged

\* Non-vacuity probes for CorruptHeadFailsClosedOnDeletePaths's three
\* fail-closed triggers (issue #1121 round three finding 1, and the budget
\* fix in round four that gated two of them behind EnableDeletePathCorruption
\* -- see the comment beside DoCorruptPart). Each is checked as an INVARIANT
\* in its own dedicated negative/*-nonvacuity.cfg; TLC reporting the negated
\* form violated proves the trigger itself is reachable, independent of
\* whether any config also exercises the DeletePathIgnoresX mutants against
\* it. HeadCorruptedExercised needs no EnableDeletePathCorruption gate (it
\* reads DoCorruptHead, which this round left ungated); the other two need
\* their probe config to set EnableDeletePathCorruption = TRUE, since
\* DoCorruptPart/DoPoisonEntry cannot fire otherwise.
HeadCorruptedExercised == head.status = "corrupt"

NoHeadCorrupted == ~HeadCorruptedExercised

PartUnreadableExercised == \E H \in Hours : partUnreadable[H]

NoPartUnreadable == ~PartUnreadableExercised

EntryUndecodableExercised == \E H \in Hours : entryUndecodable[H]

NoEntryUndecodable == ~EntryUndecodableExercised

----------------------------------------------------------------------------
\* Named temporal properties (checked against FairSpec only).

\* A started query always reaches a terminal phase: a pinned query either
\* completes or is invalidated, never spinning forever. One swept-object miss
\* forces exactly one re-resolve, a second miss is terminal
\* (crates/ravel-query/src/error.rs::QueryError::SnapshotInvalidated), so the
\* retry ladder is bounded. Fairness: WF on query progress. Checked in
\* exhaustive.cfg.
QueryTerminates ==
    (qy.phase = "pinned") ~> (qy.phase \in {"done", "invalid"})

\* A published compaction record is eventually reflected in a valid HEAD: its
\* superseded L0 inputs leave the snapshot and its L1 part enters it.
\*
\* NOT CHECKED in any config: recorded as a shrink. Under the F16/F17 design
\* (reconcile runs only on a watermark-advancing fold, RECONNAISSANCE.md), a
\* compaction that lands in an already-folded hour is reflected only by a later
\* fold whose watermark advances past that hour. Real deployments satisfy this
\* because the watermark advances without bound as wall-clock time moves and new
\* hours seal. A bounded model clock cannot: a compaction published after the
\* final watermark advance (Hours is finite, so the watermark saturates) is
\* never re-reconciled, and TLC reports a stuttering counter-example. The stale
\* window it exposes is safe by design (query-time dedup serves each identity
\* once, pinned by SignalDedupContract), so this is a finite-model liveness
\* limitation, not a defect. See counterexamples/late-supersession-shrink.md.
LateSupersessionEventuallyReflected ==
    \A H \in Hours : \A g \in CompIds :
        (crec[H][g].used /\ ~tomb[H])
            ~> ( head.status = "valid" /\ H <= head.wm
                 => ( <<"l1", H, g>> \in head.entries
                      /\ \A r \in crec[H][g].in : <<"l0", H, r>> \notin head.entries ) )

=============================================================================
