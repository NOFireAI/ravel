----------------------------- MODULE LifecycleGC -----------------------------
(*****************************************************************************)
(* Retention, selective erasure, legal holds and physical GC (ADR-1113 D5,   *)
(* task T4). This is the SINGLE spec module for the lifecycle area; the entry *)
(* module MCLifecycleGC.tla pins the finite instance and the negative-control *)
(* switches. Object presence is the shared object-store contract instantiated *)
(* from RavelObjectStore.tla (never re-implemented): a lifecycle object is a   *)
(* store key, deletion is `MemoryStore::delete`, tombstone creation is         *)
(* CreateIfAbsent. The protocol state that the store does not carry -- the      *)
(* HEAD's named parts, the fold watermark, legal holds, pinned queries, the     *)
(* served-record content of each object, the rewrite identity, the sys/gc       *)
(* config -- lives in dedicated variables and is documented in README.md as an  *)
(* abstraction boundary. There is no cache tier in this model; the erasure      *)
(* invariants make no claim about one (finding 11).                             *)
(*                                                                             *)
(* Abstraction boundary (see README for the full mapping):                      *)
(*  * `store[o].present` is whether object o exists in object storage.          *)
(*  * `head` is the set of data objects the current HEAD names as live parts.   *)
(*    `headState` models the object read of the HEAD itself: present (gate      *)
(*    reads membership), absent (gate clears), unreadable (whole pass blocked). *)
(*  * `superseded` is the set of inputs a published record set has superseded   *)
(*    (crates/ravel-catalog resolve_rewrite_supersession); a superseded input   *)
(*    is a physical-GC candidate but is HELD while HEAD still names it.          *)
(*  * `leaseOwner`, `rwPhase`, `rwInputs` and `cmpPhase` are the maintenance     *)
(*    passes' own bookkeeping: who holds the bucket's advisory lease, and how    *)
(*    far each pass has got between listing the bucket and publishing its        *)
(*    record. Every pass is two steps because the shipped passes are two object  *)
(*    store round trips with no compare-and-swap between them; that is what      *)
(*    makes a decision taken against a listing observable as stale by the time   *)
(*    it is acted on (issues #1289 and #1221).                                   *)
(*  * `heldBuckets` are the shards under a legal hold; a hold covers the         *)
(*    l0/commit/l1 prefixes and NOT the del prefix (shard_hold_scopes), so only *)
(*    data objects, never .dreq/.done/tombstone, are held.                      *)
(*  * `query` is one pinned in-flight query with a deadline = pin + mqd.         *)
(*  * `erasureRequested` is monotone: once an erasure is requested for a         *)
(*    subject it must never be served again by any modelled read.              *)
(*                                                                             *)
(* Every deletion goes through the store operator S!Delete and records what it  *)
(* observed (the held set, the head it read, the horizon, the refresh state) in *)
(* the single witness `lastGc`. The invariants read the store, the head and     *)
(* that witness -- never a switch, and never a ghost the action writes to       *)
(* certify itself.                                                              *)
(*                                                                             *)
(* The claim this model supports (ADR-1113 D12): TLC checked this finite model  *)
(* under the bounds and assumptions in results.md and README.md. It is a        *)
(* bounded model check, not a proof for all shard, bucket and clock sizes.      *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    (* Horizon parameters read from the sys/gc record (gc_config.rs). *)
    ProtectionHorizon,   \* protection_horizon
    Grace,               \* grace
    MaxQueryDuration,    \* max_query_duration
    ClockSkew,           \* clock_skew_allowance
    (* The fold overlays a durable per-tenant retention window; the sweep reads
       its own from CLI flags. Modelled as two constants that may differ (#1131):
       the fold seals a retired bucket out of HEAD only after FoldRetentionWindow,
       the sweep attempts only after SweepRetentionWindow. When the fold window is
       larger the head-empty gate blocks the sweep, so EventuallySwept holds only
       when the two agree. *)
    FoldRetentionWindow,
    SweepRetentionWindow,
    DreqHorizonDelta,    \* .dreq horizon offset from its request time
    MaxClock,            \* bound on the modelled clock
    FullEnv,             \* TRUE opens the adversarial head-read churn (absent /
                         \* unreadable HEAD) and the failed-refresh churn. Smoke and
                         \* exhaustive set it TRUE; a control may pin it FALSE to keep
                         \* headState present and refreshFailed FALSE when that churn
                         \* is irrelevant to the switch under test.
    (* Negative-control switches: all default FALSE (correct model) except
       HorizonGuardsPinnedQueries which defaults TRUE. A negative cfg flips
       exactly one; each breaks a BEHAVIOUR, never a value an invariant reads. *)
    DeleteBeforeHorizon,        \* retention deletes without the horizon gate
    RefreshFailureSweepsAnyway, \* a failed hold refresh does not skip the tick
    SupersededSweepUngated,     \* drop the object-granular HEAD gate
    DreqIgnoresHeldInputs,      \* delete the .dreq while an input serving the erased
                                \* subject is still present in the store
    RewriteIdentityOmitsRequests, \* the rewrite key hash ignores the applied ids
    RewriteKeepsErasedRecords,  \* the rewrite output keeps the erased records
                                \* instead of dropping them (breaks the multiset rule)
    CompleteIgnoresServedSet,   \* completion skips the served-set check, marking
                                \* .done while the current HEAD still serves the subject
    HorizonGuardsPinnedQueries, \* base: a horizon-gated delete also respects an
                                \* in-window pinned query. Candidate #1133 sets it
                                \* FALSE to model the shipped delete, which gates on
                                \* horizon AND head-empty but NOT on pinned queries.
    CompactionIgnoresRewrite,   \* the compactor publishes a record set without
                                \* checking the listing for a live rewrite record
                                \* (drops the RewritePresent refusal, issue #1289)
    SerializeCompactionAndRewrite \* base TRUE: the maintenance driver runs at most
                                \* one of compaction and erasure rewrite over a
                                \* bucket at a time, so neither pass can be between
                                \* its listing and its publish while the other lists.
                                \* FALSE models two workers that both believe they
                                \* own the bucket (ADR-0065 membership overlap), the
                                \* residual window cdce1722 documents as open.

ASSUME ProtectionHorizon \in Nat /\ Grace \in Nat
ASSUME MaxQueryDuration \in Nat /\ ClockSkew \in Nat
ASSUME MaxClock \in Nat
\* The GC startup inequality (gc_config.rs::satisfies_constraint) is a precondition
\* on the configuration the maintainer runs with, enforced at startup and never
\* re-checked per state. It is an ASSUME here, not a state invariant (finding 9):
\* as an invariant it reduced to a constant comparison the model never varies.
ASSUME ProtectionHorizon >= MaxQueryDuration + Grace + ClockSkew

\* --- The finite instance (a fixed bounded model; README documents the bounds) -
\* One retention bucket b1 with a single L0 raw input is the minimal slice that
\* exercises supersession, the held-input gate, retention delete and the pinned
\* query race. README records why a second raw input and a second bucket add no
\* new reachable invariant behaviour, only state.
Buckets   == {"b1"}
Subjects  == {"s1", "s2"}
Requests  == {"r1"}

\* Object identities (store keys).
\* A record set is any object a reader can be resolved onto for the bucket: a
\* rewrite output or a compaction output. Both are published under their own key
\* class (erasure rewrite record vs compaction record), so the store cannot make
\* one exclude the other; only a guard in the publishing pass can (issue #1289).
RawInputs     == {"raw1"}                   \* L0 raw input in b1, serves subject s1
RewriteOut    == {"rwA", "rwB"}             \* rwA rewrites {raw1}, rwB rewrites {cmpA}
CompactOut    == {"cmpA"}                   \* compaction of the live L0 inputs
RecordSets    == RewriteOut \cup CompactOut
DataObjects   == RawInputs \cup RecordSets
ControlObjects== {"tombB1", "dreqR1", "doneR1", "sysgc"}
Objects       == DataObjects \cup ControlObjects

\* The objects a later record set can supersede. A rewrite output is never itself
\* superseded in this instance (rwB is the terminal set), so the superseded set
\* and the horizon stamp range over the raw inputs and the compaction output.
SupersededCandidates == RawInputs \cup CompactOut

\* The two rewrite identities. Each is an independent worker attempt at the
\* bucket's erasure rewrite; ADR-0065 grants ownership by rendezvous hash with no
\* per-unit CAS lease and no fencing token, so a second identity can start after
\* the first identity's lease expires and before the first identity's own publish
\* (issue #1221). The pair is what makes that interleaving expressible at all.
RewriteIds    == {"A", "B"}

InitPresent   == RawInputs \cup {"sysgc"}

\* --- Static object metadata --------------------------------------------------
Bucket(o) == CASE o \in {"raw1","rwA","rwB","cmpA","tombB1","dreqR1","doneR1"} -> "b1"
               [] OTHER -> "sys"

\* The served records are modelled by identity, not by count: raw1 carries two
\* records of two distinct subjects. `objContent` (a state variable) holds the
\* set of record identities each object serves, so "serves subject s" is a fact
\* about the stored content, and the rewrite multiset rule (finding 3) is
\* checked over record identities rather than reduced to a constant.
\* RecordSubject maps a record to its subject. rec1/s1 is the erased side of the
\* claim; rec2/s2 is the surviving side -- r1 erases only s1, so a correct
\* rewrite must drop rec1 and keep rec2. Without rec2 the "kept" direction of
\* RewriteOutputsAreInputsMinusErased has no witness: every record in scope is
\* erased, so the right-hand side of the <=> is a state-independent FALSE.
AllRecords     == {"rec1", "rec2"}
RecordSubject(r) == IF r = "rec1" THEN "s1" ELSE "s2"

\* rwA rewrites the raw L0 inputs; cmpA compacts the same raw L0 inputs; rwB
\* rewrites the compaction output, which is the rewrite-of-a-derived-set case the
\* single-identity model could not reach (REPORT.md section 10, issue #1221).
Predecessors(o) == CASE o = "rwA"  -> RawInputs
                     [] o = "rwB"  -> CompactOut
                     [] o = "cmpA" -> RawInputs
                     [] OTHER      -> {}

\* A compaction applies no erasure request: it merges its inputs and drops
\* nothing. That is the whole mechanism issue #1289 asks about -- a compaction
\* record over inputs a rewrite deliberately emptied of a subject brings that
\* subject back for any reader resolved onto the compaction output.
AppliedReqs(o)  == IF o \in RewriteOut THEN Requests ELSE {}

\* Subjects erased by a set of request ids (r1 erases s1, never s2).
ErasedBy(reqs) == IF "r1" \in reqs THEN {"s1"} ELSE {}

\* Content of a raw input at Init: raw1 carries rec1 (s1, erasable) and rec2
\* (s2, must survive any rewrite applying r1); every other object carries no
\* records until an action writes it.
InitContent(o) == IF o \in RawInputs THEN {"rec1", "rec2"} ELSE {}

\* Rewrite descriptors for the identity-collision property: one resolved input
\* set, a different applied-request set. The shipped key binds the sorted applied
\* ids (compute_rewrite_input_set_hash); the switch drops them so the two collide.
\* The input set is the one the publishing identity resolved at its listing step,
\* not a fixed RawInputs: a publish that targets rwB resolved CompactOut, so its
\* stored names must be CompactOut-derived. Pinning both variants to RawInputs
\* would let IdenticalInputSetsDoNotCollide read names that no longer describe the
\* publish that wrote them (finding, #1289). PublishRewrite names its two output
\* variants by RewriteKey over these descriptors and stores the names in
\* `variantKey`; the invariant reads the stored names, not this operator
\* (finding 4). RewriteKey itself is what the action USES to name an object.
VariantDescA(ins) == [inputs |-> ins, reqs |-> {"r1"}]
VariantDescB(ins) == [inputs |-> ins, reqs |-> {}]
RewriteKey(d) == IF RewriteIdentityOmitsRequests
                     THEN <<d.inputs>>
                     ELSE <<d.inputs, d.reqs>>
\* The sentinel a variant name holds before PublishRewrite has assigned it.
UnnamedKey == <<>>
\* The names variantKey can hold: the sentinel, plus a key generated from either
\* resolvable input set (RawInputs for rwA, CompactOut for rwB) under either
\* applied-request set. TypeOK ranges over all of them so a CompactOut-derived
\* name is well typed.
VariantKeyRange ==
    {UnnamedKey}
      \cup { RewriteKey(VariantDescA(ins)) : ins \in {RawInputs, CompactOut} }
      \cup { RewriteKey(VariantDescB(ins)) : ins \in {RawInputs, CompactOut} }

\* Legal-hold coverage: a hold on a bucket covers its data objects (l0/commit/l1)
\* but never the del prefix (.dreq/.done/tombstone) or sys.
HeldObject(o, heldB) == (o \in DataObjects) /\ (Bucket(o) \in heldB)

\* Rules whose deletion is gated on a time horizon (as opposed to only a HEAD
\* gate). The superseded-input sweep is horizon-gated too (sweep.rs skips a record
\* younger than the protection horizon in both the compaction and the rewrite
\* branch), so it is included here and the pinned-query clause of
\* NoDeleteInsideProtectionWindow covers it (finding 6).
HorizonGatedRules == {"retention", "dreq", "superseded"}

\* --- Store instance ----------------------------------------------------------
VARIABLES
    store, lastModified, versionCounter, uploads, listState,  \* RavelObjectStore
    head,             \* SUBSET DataObjects: the HEAD's named live parts
    headState,        \* "present" | "absent" | "unreadable"  (the HEAD object read)
    clock,            \* Nat
    superseded,       \* SUBSET SupersededCandidates: inputs a published record set
                      \* has superseded
    heldBuckets,      \* SUBSET Buckets under a legal hold
    refreshFailed,    \* BOOLEAN: this tick's legal-hold refresh failed
    query,            \* [active, needs: SUBSET DataObjects, deadline: Nat]
    erasureRequested, \* SUBSET Subjects (monotone)
    tombRetiredAt,    \* [Buckets -> Nat]: retired_at, 0 when no tombstone
    dreqHorizon,      \* Nat: the .dreq horizon
    doneAt,           \* Nat: completion timestamp (0 when no .done)
    supersededAt,     \* [SupersededCandidates -> Nat]: per-object clock at which a
                      \* publish FIRST superseded it (0 = not superseded). Per
                      \* object, not one shared stamp, and written once: with two
                      \* publishing passes a later supersession would otherwise
                      \* retroactively re-open the protection window of an earlier,
                      \* already legitimate delete, and a re-stamp of an object
                      \* already in `superseded` would push its own horizon forward.
    objContent,       \* [Objects -> SUBSET AllRecords]: served record identities
    variantKey,       \* [{"v1","v2"} -> key]: the names PublishRewrite assigned
    leaseOwner,       \* "none" | "A" | "B" | "C": who currently holds the bucket's
                      \* maintenance lease ("C" is the compactor). Ownership is
                      \* advisory: ADR-0065 has no fencing token, so a pass that
                      \* already listed keeps running after the lease moves.
    rwPhase,          \* [RewriteIds -> {"idle","listed","done"}]: how far each
                      \* rewrite identity got. "listed" is the list-then-act window.
    rwInputs,         \* [RewriteIds -> SUBSET SupersededCandidates]: the live input
                      \* set each identity resolved at its listing step
    cmpPhase,         \* "idle" | "listed" | "done": the compaction pass's own window
    cmpInputs,        \* SUBSET RawInputs: the live L0 inputs the compaction pass
                      \* resolved at its listing step, the listing-time snapshot the
                      \* publish is checked against
    sysgc,            \* [ph, mqd, grace, skew]
    lastGc            \* witness of the last GC deletion step

storeVars == <<store, lastModified, versionCounter, uploads, listState>>
protoVars == <<head, headState, clock, superseded, heldBuckets,
               refreshFailed, query, erasureRequested, tombRetiredAt,
               dreqHorizon, doneAt, supersededAt, objContent, variantKey,
               leaseOwner, rwPhase, rwInputs, cmpPhase, cmpInputs,
               sysgc, lastGc>>
vars == <<store, lastModified, versionCounter, uploads, listState,
          head, headState, clock, superseded, heldBuckets,
          refreshFailed, query, erasureRequested, tombRetiredAt,
          dreqHorizon, doneAt, supersededAt, objContent, variantKey,
          leaseOwner, rwPhase, rwInputs, cmpPhase, cmpInputs,
          sysgc, lastGc>>

\* The maintenance-pass bookkeeping (who holds the lease, how far each pass got,
\* and the input sets each pass resolved). Every action that is not a maintenance
\* pass leaves all five alone and names this tuple in its UNCHANGED list, rather
\* than spelling out five entries on each of a dozen lists. The passes that assign
\* a subset of the five (StartRewrite, PublishRewrite, ExpireLease, and the three
\* compaction actions) keep an explicit list of the ones they leave alone: naming
\* maintVars there would double-constrain a variable the action also sets. An
\* omission from an UNCHANGED list does not fail to parse; it lets the variable
\* take any value in the step, so the tuple has to be spelled out somewhere for
\* every action, and using the name is what keeps the two forms from drifting.
maintVars == <<leaseOwner, rwPhase, rwInputs, cmpPhase, cmpInputs>>

S == INSTANCE RavelObjectStore
       WITH Keys <- Objects, Content <- {"dat", "nc"}, NoContent <- "nc",
            Clients <- {"mnt"}

PresentObj(o) == store[o].present

\* A subject is served by object o iff a record of that subject is in o's stored
\* content (finding 3: serving is a fact about stored content, not a static CASE).
ServesSubject(o, s) == \E r \in objContent[o] : RecordSubject(r) = s

\* The record set a published output should serve: its predecessors' records minus
\* the records whose subject its applied requests erased. A compaction applies no
\* request, so its content is exactly the union of its inputs' records; a rewrite
\* drops the erased ones. RewriteKeepsErasedRecords drops the minus for a rewrite
\* output (finding 3 behaviour mutant) and leaves a compaction alone, because a
\* compaction has no minus to drop.
\*
\* Reads objContent, not InitContent, because resolve_live_inputs re-lists the
\* bucket and reads current object bodies at publish time (issue #1122, finding 1).
\* For a raw-input predecessor this read is not exercised: raw inputs are immutable
\* by system invariant (RawInputContentAssumedImmutable, README.md), so objContent[i]
\* for i \in RawInputs always equals InitContent(i). The current-state read is what
\* matters for a predecessor that is itself a published record set, whose content an
\* earlier publish wrote. That case is now reachable: Predecessors("rwB") is the
\* compaction output, so a StartCompaction/PublishCompaction pass followed by a
\* rewrite over the resulting set exercises the read on a body no Init wrote. Before
\* the compaction action existed, RewriteOut held one object whose predecessors were
\* fixed to RawInputs and this read had no reachable witness (issue #1221).
RecordSetContent(o) ==
    LET inRecs == UNION { objContent[i] : i \in Predecessors(o) }
    IN IF RewriteKeepsErasedRecords \/ AppliedReqs(o) = {}
           THEN inRecs
           ELSE { r \in inRecs : RecordSubject(r) \notin ErasedBy(AppliedReqs(o)) }

\* State-space view: the invariants and every gate read object PRESENCE, never the
\* store's version, content, upload or listing bookkeeping. Projecting those away
\* collapses the states that differ only in the version integer a particular write
\* ordering assigned, which is the dominant source of otherwise-equivalent states.
StoreView == [o \in Objects |-> store[o].present]
View ==
    <<StoreView, head, headState, clock, superseded, heldBuckets, refreshFailed,
      query, erasureRequested, tombRetiredAt, dreqHorizon, doneAt, supersededAt,
      objContent, variantKey, leaseOwner, rwPhase, rwInputs, cmpPhase, cmpInputs,
      sysgc, lastGc>>

\* A delete decision needs a readable HEAD, present or absent: an absent HEAD
\* names nothing, so the delete may proceed exactly as if EffectiveHead were
\* empty (ADR-0020: the catalog index is a pure optimization; a missing HEAD
\* degrades to listing, it does not block). Only an unreadable HEAD fails
\* closed, because a HEAD that exists but cannot be decoded may still name the
\* object (reachability.rs bucket_gate/object_gate: `HeadStatus::Absent =>
\* Covering::Clear`, `HeadStatus::Unreadable => Covering::Blocked`; finding 1,
\* round four). HeadDeletable is the completion gate only (CompleteErasure):
\* completion still needs a present HEAD to read the served set from, a
\* narrower requirement the physical sweeps do not share.
HeadDeletable == headState = "present"
HeadReadable == headState # "unreadable"
EffectiveHead == IF headState = "absent" THEN {} ELSE head

\* --- Read-time serving (the erasure predicate is applied after fetch/cache) ---
\* A subject is served now iff a readable HEAD names some present object that
\* serves it. A read reads the HEAD object: unreadable fails closed (serves
\* nothing) and absent names nothing, so serving uses the same EffectiveHead the
\* GC gate reads.
ServesNow(s) == HeadReadable /\ \E o \in EffectiveHead : PresentObj(o) /\ ServesSubject(o, s)

\* A permitted in-flight query reads the HEAD snapshot it pinned, even after a fold
\* has advanced the current HEAD past it. That reader also serves the subject if a
\* still-present object it pinned serves it, which is why the .dreq (the read-time
\* erasure filter) must outlive such a query.
PinnedServes(s) ==
    /\ query.active
    /\ clock <= query.deadline
    /\ \E o \in query.needs : PresentObj(o) /\ ServesSubject(o, s)

ServesAny(s) == ServesNow(s) \/ PinnedServes(s)

\* A live data object (raw input OR rewrite output) under legal hold (finding 1;
\* scope widened to DataObjects and off content, finding 2). Independent of
\* head/pinned reachability: a superseded input can be off HEAD and unpinned
\* yet still legally held, which is exactly why the code gates on it separately
\* (bucket_is_held in erasure_rewrite.rs, chain_groups_held_by_legal_hold in
\* sweep.rs) rather than folding it into the served-set check. Both gate on
\* live key presence in the bucket listing, never on whether that key's stored
\* content still serves the subject being erased ("a hold over any single key
\* in [a chain group] stops all of it", sweep.rs); a raw input always still
\* carries the erased subject's record in this finite model, so an earlier,
\* narrower version of this predicate that also required ServesSubject(o, s)
\* passed for RawInputs by coincidence, while the same requirement made a held
\* rewrite output invisible once its raw input was swept, since a correctly
\* computed rewrite output never serves an already-erased subject -- exactly
\* the gap finding 2 found reachable. `s` stays in the signature for the call
\* sites' shape; the subject no longer changes the result, matching the
\* shipped gate.
HeldInputServes(s) == \E o \in DataObjects : HeldObject(o, heldBuckets) /\ PresentObj(o)

\* The .dreq presence filters the subject at read time for BOTH the current-HEAD
\* read and a pinned read; an erased subject stays filtered until the .dreq is gone
\* and nothing any reader can still reach serves it.
ServedRead(s) == ServesAny(s) /\ ~PresentObj("dreqR1")

--------------------------------------------------------------------------------
\* TypeOK
RecT == [present: BOOLEAN, content: {"dat","nc"}, version: Nat]

TypeOK ==
    /\ S!StoreTypeOK
    /\ store \in [Objects -> RecT]
    /\ versionCounter \in Nat
    /\ head \subseteq DataObjects
    /\ headState \in {"present","absent","unreadable"}
    /\ clock \in 0..MaxClock
    /\ superseded \subseteq SupersededCandidates
    /\ heldBuckets \subseteq Buckets
    /\ refreshFailed \in BOOLEAN
    /\ query \in [active: BOOLEAN, needs: SUBSET DataObjects, deadline: 0..(MaxClock + MaxQueryDuration)]
    /\ erasureRequested \subseteq Subjects
    /\ tombRetiredAt \in [Buckets -> 0..MaxClock]
    /\ dreqHorizon \in Nat
    /\ doneAt \in 0..MaxClock
    /\ supersededAt \in [SupersededCandidates -> 0..MaxClock]
    /\ objContent \in [Objects -> SUBSET AllRecords]
    /\ variantKey \in [{"v1","v2"} -> VariantKeyRange]
    /\ leaseOwner \in {"none"} \cup RewriteIds \cup {"C"}
    /\ rwPhase \in [RewriteIds -> {"idle","listed","done"}]
    /\ rwInputs \in [RewriteIds -> SUBSET SupersededCandidates]
    /\ cmpPhase \in {"idle","listed","done"}
    /\ cmpInputs \subseteq RawInputs
    /\ sysgc \in [ph: Nat, mqd: Nat, grace: Nat, skew: Nat]
    /\ lastGc.rule \in {"none","superseded","retention","dreq","complete","tombstone"}
    /\ lastGc.deleted \subseteq Objects
    /\ lastGc.atClock \in 0..MaxClock
    /\ lastGc.held \in BOOLEAN
    /\ lastGc.refreshWasFailed \in BOOLEAN
    /\ lastGc.permittedNeeds \subseteq DataObjects
    /\ lastGc.heldInputServed \in BOOLEAN

--------------------------------------------------------------------------------
\* Init: a populated store (raw1, raw2, d2, sysgc present), HEAD naming the data,
\* no tombstone/.dreq/.done, no holds, valid (or, under the switch, invalid) config.
InitStoreRec(o) ==
    IF o \in InitPresent
        THEN [present |-> TRUE, content |-> "dat", version |-> 1]
        ELSE [present |-> FALSE, content |-> "nc", version |-> 0]

Init ==
    /\ store = [o \in Objects |-> InitStoreRec(o)]
    /\ lastModified = [o \in Objects |-> IF o \in InitPresent THEN 1 ELSE 0]
    /\ versionCounter = 1
    /\ uploads = [u \in {"mnt"} |-> [active |-> FALSE, key |-> "raw1", content |-> "nc"]]
    /\ listState = [active |-> FALSE, snapshot |-> {}, delivered |-> [o \in Objects |-> 0]]
    /\ head = RawInputs
    /\ headState = "present"
    /\ clock = 0
    /\ superseded = {}
    /\ heldBuckets = {}
    /\ refreshFailed = FALSE
    /\ query = [active |-> FALSE, needs |-> {}, deadline |-> 0]
    /\ erasureRequested = {}
    /\ tombRetiredAt = [b \in Buckets |-> 0]
    /\ dreqHorizon = 0
    /\ doneAt = 0
    /\ supersededAt = [i \in SupersededCandidates |-> 0]
    /\ objContent = [o \in Objects |-> InitContent(o)]
    /\ variantKey = [v \in {"v1","v2"} |-> UnnamedKey]
    /\ leaseOwner = "none"
    /\ rwPhase = [id \in RewriteIds |-> "idle"]
    /\ rwInputs = [id \in RewriteIds |-> {}]
    /\ cmpPhase = "idle"
    /\ cmpInputs = {}
    /\ sysgc = [ph |-> ProtectionHorizon,
                mqd |-> MaxQueryDuration, grace |-> Grace, skew |-> ClockSkew]
    /\ lastGc = [rule |-> "none", deleted |-> {}, atClock |-> 0,
                 held |-> FALSE, refreshWasFailed |-> FALSE,
                 permittedNeeds |-> {}, heldInputServed |-> FALSE]

\* A GC witness records what the deleting store operation OBSERVED at its own
\* step: the TRUE legal-hold state (over heldBuckets, not the sweep's known set),
\* the refresh state, and the permitted-query needs. Invariants read this captured
\* state (never the live variable), so a hold or refresh flipped AFTER a legitimate
\* delete cannot retroactively make it look unsafe. `held` uses the true hold set
\* so a sweep that ran with degraded hold knowledge (finding 2) is caught.
\* `heldInputServed` is the same kind of per-step witness for finding 1: whether a
\* held raw input served the erased subject at the moment this action ran, not
\* whatever a hold placed or released afterward makes true. It is only ever
\* meaningful when paired with the rule/step that set it deliberately
\* (CompleteErasure via CompletionWitness, DreqSweep via GcWitness "dreq");
\* every other action resets it to FALSE the same way it resets `held`.
PermittedNeeds ==
    IF query.active /\ clock <= query.deadline THEN query.needs ELSE {}

GcWitness(r, dels) ==
    lastGc' = [rule |-> r, deleted |-> dels, atClock |-> clock,
               held |-> \E o \in dels : HeldObject(o, heldBuckets),
               refreshWasFailed |-> refreshFailed,
               permittedNeeds |-> PermittedNeeds,
               heldInputServed |-> HeldInputServes("s1")]

NoGc == lastGc' = [rule |-> "none", deleted |-> {}, atClock |-> clock,
                   held |-> FALSE, refreshWasFailed |-> FALSE,
                   permittedNeeds |-> {}, heldInputServed |-> FALSE]

\* CompleteErasure is not a delete, so it does not fit the GcWitness shape (no
\* object is deleted), but it needs the same per-step held-input witness as the
\* GC actions: whether HeldInputServes("s1") was true at the moment it wrote
\* .done, tagged with its own rule so CompletionRespectsLegalHold can find it.
CompletionWitness ==
    lastGc' = [rule |-> "complete", deleted |-> {}, atClock |-> clock,
               held |-> FALSE, refreshWasFailed |-> refreshFailed,
               permittedNeeds |-> {}, heldInputServed |-> HeldInputServes("s1")]

--------------------------------------------------------------------------------
\* Environment actor
--------------------------------------------------------------------------------

\* Advance the wall clock (bounded).
Tick ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

\* Pin an in-flight query at the current HEAD; its deadline is pin + mqd. It is
\* permitted (may still read the objects it named) until the clock passes the
\* deadline (max_query_duration).
PinQuery ==
    /\ ~query.active
    /\ query' = [active |-> TRUE,
                 needs |-> {o \in head : PresentObj(o)},
                 deadline |-> clock + MaxQueryDuration]
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

ExpireQuery ==
    /\ query.active
    /\ clock > query.deadline
    /\ query' = [active |-> FALSE, needs |-> {}, deadline |-> 0]
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

\* Place / release a legal hold on bucket b (its data prefixes).
PlaceHold(b) ==
    /\ b \notin heldBuckets
    /\ heldBuckets' = heldBuckets \cup {b}
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

ReleaseHold(b) ==
    /\ b \in heldBuckets
    /\ heldBuckets' = heldBuckets \ {b}
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

\* The HEAD object read can fail (unreadable) or find the HEAD gone (absent).
\* Unreadable can diverge from head at any content (an existing catalog object
\* can fail to decode regardless of what it names). Absent cannot: under the
\* object store's strong-consistency read of a single key, a GET on a HEAD that
\* really names something never comes back 404 (finding 1, round four). Absent
\* is only a truthful read outcome when head itself is already empty, so this
\* action requires that rather than letting a "present, nonempty head" world
\* report absent and then flip back to "present" with the same unchanged head,
\* which would assert a real object was reachable, deleted, and reachable again.
SetHeadState(s) ==
    /\ FullEnv
    /\ s # headState
    /\ s = "absent" => head = {}
    /\ headState' = s
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

\* Toggle this tick's legal-hold refresh outcome.
SetRefresh(f) ==
    /\ FullEnv
    /\ f # refreshFailed
    /\ refreshFailed' = f
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

--------------------------------------------------------------------------------
\* Erasure / rewrite actor (maintainer)
--------------------------------------------------------------------------------

\* Write the .dreq for the erasure request (CreateIfAbsent, irreversible key).
\* crates/ravel-commit erasure request; the subject is marked erasure-requested
\* forever (it must never be served again by any modelled read).
RequestErasure ==
    /\ ~PresentObj("dreqR1")
    /\ S!PutCreateIfAbsent("dreqR1", "dat")
    /\ erasureRequested' = erasureRequested \cup {"s1"}
    /\ dreqHorizon' = clock + DreqHorizonDelta
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, tombRetiredAt, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

\* --- Live input resolution (resolve_live_inputs) ------------------------------
\* The set a maintenance pass resolves for the bucket when it lists it: the live
\* record sets if any have been published, otherwise the live raw L0 inputs. A
\* superseded object is not live even when the object that superseded it has since
\* been swept: supersession is recorded in the catalog, not implied by presence.
LiveRecordSets == { o \in RecordSets : PresentObj(o) /\ o \notin superseded }

LiveInputs ==
    IF LiveRecordSets # {}
        THEN LiveRecordSets
        ELSE { o \in RawInputs : PresentObj(o) /\ o \notin superseded }

\* The rewrite record key is content-addressed over the resolved input set and the
\* sorted applied request ids (compute_rewrite_input_set_hash), so the key follows
\* from the inputs, not from which worker resolved them. Two identities that
\* resolved the same live input set therefore aim at the same key.
TargetOf(ins) == IF ins = CompactOut THEN "rwB" ELSE "rwA"

\* Materialise a rewrite output = inputs minus the erased subject and mark the
\* inputs superseded (resolve_rewrite_supersession). The HEAD is NOT switched
\* here; a later HeadAdvance drops the superseded inputs, so between the two the
\* inputs are still HEAD-named and the superseded sweep must hold them.
\*
\* Split into two steps (issue #1221). erasure_rewrite_bucket lists the bucket,
\* decides against that listing, then publishes; the two are separate object-store
\* round trips with no compare-and-swap between them, and ADR-0065 grants bucket
\* ownership by rendezvous hash with no per-unit lease and no fencing token. So a
\* second identity can list and publish inside the first identity's window, and the
\* first identity's publish is NOT re-checked against the lease it no longer holds.
\* The split is load-bearing: a single atomic action cannot express that
\* interleaving, so the invariants below would hold over it vacuously.
\*
\* StartRewrite carries erasure_rewrite_bucket's front gates:
\*  * PresentObj("dreqR1") /\ ~PresentObj("doneR1"): pending_erasure_requests
\*    filters out any .dreq with a matching .done, so a completed erasure is never
\*    seen as still pending (issue #1122, finding 1).
\*  * ~PresentObj("tombB1"): ErasureRewriteOutcome::Tombstoned, read against a
\*    fresh listing. RetireBucket has no dependency on dreqR1/doneR1/superseded, so
\*    this gate is the only thing excluding a rewrite of a retired bucket.
\*  * ~HeldInputServes: ErasureRewriteOutcome::Held (bucket_is_held).
\*  * LiveInputs # {}: resolve_live_inputs found something to rewrite.
\*  * no live input is itself a rewrite output: ErasureRewriteOutcome::AlreadyApplied,
\*    which skips a bucket already rewritten for every applicable pending request.
\*    This replaces the old `superseded = {}` conjunct, which also excluded a
\*    rewrite of a compaction output that no rewrite had yet touched.
StartRewrite(id) ==
    /\ leaseOwner \in {"none", id}
    /\ rwPhase[id] = "idle"
    /\ (SerializeCompactionAndRewrite => cmpPhase # "listed")
    /\ PresentObj("dreqR1")
    /\ ~PresentObj("doneR1")
    /\ ~PresentObj("tombB1")
    /\ ~HeldInputServes("s1")
    /\ LiveInputs # {}
    /\ ~(\E o \in LiveInputs : o \in RewriteOut)
    /\ leaseOwner' = id
    /\ rwPhase' = [rwPhase EXCEPT ![id] = "listed"]
    /\ rwInputs' = [rwInputs EXCEPT ![id] = LiveInputs]
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets, refreshFailed,
                   query, erasureRequested, tombRetiredAt, dreqHorizon, doneAt,
                   sysgc, supersededAt, objContent, variantKey, cmpPhase, cmpInputs>>
    /\ NoGc

\* The publish. Deliberately unguarded by the lease: nothing between the listing
\* and this write re-reads ownership, so this fires whatever leaseOwner now says.
\* The one thing that does protect it is the store: the record is published
\* CreateIfAbsent under a key derived from the resolved input set, so a sibling
\* identity that resolved the SAME inputs converges onto the one object instead of
\* publishing a second record set. That convergence is the reason the two-identity
\* race is safe; it is a property of the content-addressed key and CreateIfAbsent,
\* not of the lease.
PublishRewrite(id) ==
    /\ rwPhase[id] = "listed"
    /\ rwPhase' = [rwPhase EXCEPT ![id] = "done"]
    /\ LET tgt == TargetOf(rwInputs[id]) IN
         IF PresentObj(tgt)
             THEN \* Lost the CreateIfAbsent to a sibling that resolved the same
                  \* inputs first. Its record already covers this pass's work.
                  /\ UNCHANGED storeVars
                  /\ UNCHANGED <<superseded, supersededAt, objContent, variantKey>>
             ELSE
                  /\ S!PutCreateIfAbsent(tgt, "dat")
                  /\ superseded' = superseded \cup rwInputs[id]
                  \* The stamp is the FIRST supersession's clock. An input this
                  \* publish resolved may already be superseded (a sibling
                  \* identity published the same target and that target was then
                  \* swept, so this pass's CreateIfAbsent succeeds a second
                  \* time), and re-stamping it would move the protection horizon
                  \* of an object whose supersession the catalog already
                  \* recorded.
                  /\ supersededAt' = [i \in SupersededCandidates |->
                                        IF i \in rwInputs[id] /\ i \notin superseded
                                            THEN clock
                                            ELSE supersededAt[i]]
                  /\ objContent' = [objContent EXCEPT ![tgt] = RecordSetContent(tgt)]
                  \* Name the two variants from the input set THIS publish
                  \* resolved (rwInputs[id]), so a publish that resolved
                  \* CompactOut stamps CompactOut-derived names, never rwA's
                  \* RawInputs names (finding, #1289).
                  /\ variantKey' = [variantKey EXCEPT
                                        !["v1"] = RewriteKey(VariantDescA(rwInputs[id])),
                                        !["v2"] = RewriteKey(VariantDescB(rwInputs[id]))]
    /\ UNCHANGED <<head, headState, clock, heldBuckets, refreshFailed, query,
                   erasureRequested, tombRetiredAt, dreqHorizon, doneAt, sysgc,
                   leaseOwner, rwInputs, cmpPhase, cmpInputs>>
    /\ NoGc

\* The lease expires under a pass that has already listed. ADR-0065 decision 2:
\* during a membership transition (bounded by 3*H plus one heartbeat) two workers
\* may both believe they own a unit, and the enumerated concurrency-safe operations
\* do not include the erasure rewrite, which landed with ADR-0064 afterwards. This
\* action is what lets rwB start after rwA's lease has gone and before rwA acks.
ExpireLease ==
    /\ leaseOwner # "none"
    /\ leaseOwner' = "none"
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets, refreshFailed,
                   query, erasureRequested, tombRetiredAt, dreqHorizon, doneAt,
                   sysgc, supersededAt, objContent, variantKey, rwPhase, rwInputs,
                   cmpPhase, cmpInputs>>
    /\ NoGc

\* --- Compaction actor (maintainer) --------------------------------------------
\* Compaction publishes a new record set over a chosen subset of the bucket's live
\* objects. Split into a listing step and a publish step for the same reason the
\* rewrite is: compact_bucket_scoped lists the bucket, decides, then publishes,
\* with no compare-and-swap in between.
\*
\* THE GUARD (issue #1289), from crates/ravel-maintain/src/compact.rs,
\* fn compact_bucket_scoped, commit b997b7e1 "fix(maintain): refuse compaction of
\* a bucket with a live rewrite":
\*
\*     if !listing.rewrite_record_keys.is_empty() {
\*         return Ok(CompactionOutcome::RewritePresent);
\*     }
\*
\* One bucket serves one record set. A live rewrite record already covers these
\* inputs with records deliberately removed from its outputs, and a compaction
\* record over the same inputs is not overlap-harmless against it: a snapshot
\* including both resurrects the erased records (ADR-0064 decision 3 point 5).
\* CompactionIgnoresRewrite drops the refusal; that is the negative control for
\* AtMostOneLiveRecordSetServed.
\*
\* The scope of the guard is exactly what commit cdce1722 records: it is a
\* list-time observation with no compare-and-swap, so it closes only the case
\* where the rewrite record is already durable when the compactor lists the
\* bucket. The concurrent case -- a rewrite that has listed but not yet published
\* when the compactor lists -- is covered today only by the maintenance driver
\* serialising the two passes per bucket, which is SerializeCompactionAndRewrite
\* here. Setting that constant FALSE opens the residual window cdce1722 names as
\* an open gap closable only by a compare-and-swap or a claim on the bucket.
\*
\* The other gates are compact_bucket_scoped's own, in its order: the tombstone
\* gate, the already-compacted gate, then the minimum-inputs gate (there is one
\* raw input in this instance, so "at least min_compaction_inputs live L0 commits"
\* is "the raw input is present"). Note what the compactor CANNOT see: whether an
\* input is superseded is a catalog fact, not a listing fact, so this action reads
\* PresentObj, not membership of `superseded`. The rewrite record's presence is
\* the compactor's only evidence that a supersession happened, which is why the
\* guard above is load-bearing rather than redundant.
StartCompaction ==
    /\ leaseOwner \in {"none", "C"}
    /\ cmpPhase = "idle"
    /\ (SerializeCompactionAndRewrite => \A id \in RewriteIds : rwPhase[id] # "listed")
    /\ ~PresentObj("tombB1")
    /\ ~PresentObj("cmpA")
    /\ (CompactionIgnoresRewrite \/ ~(\E w \in RewriteOut : PresentObj(w)))
    /\ \E o \in RawInputs : PresentObj(o)
    /\ leaseOwner' = "C"
    /\ cmpPhase' = "listed"
    \* Snapshot the live L0 inputs this pass resolved at its listing step, so the
    \* publish can be held to the set that was actually present when it listed
    \* rather than to a static RawInputs (finding, #1289). compact_bucket_scoped
    \* reads every one of these to build the compaction output.
    /\ cmpInputs' = { o \in RawInputs : PresentObj(o) }
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets, refreshFailed,
                   query, erasureRequested, tombRetiredAt, dreqHorizon, doneAt,
                   sysgc, supersededAt, objContent, variantKey, rwPhase, rwInputs>>
    /\ NoGc

\* The compaction publish, unguarded for the same reason PublishRewrite is: the
\* decision was made against the listing and is not re-read. The record is written
\* CreateIfAbsent (ADR-0018: compaction converges at CreateIfAbsent), but under the
\* compaction key class, so it converges only with another compaction -- never with
\* a rewrite record, which lands under its own key. Nothing in the store makes the
\* two exclude each other.
PublishCompaction ==
    /\ cmpPhase = "listed"
    /\ ~PresentObj("cmpA")
    \* Every input the listing snapshot recorded must still be present. The
    \* shipped compactor reads each input to build its output, so an input swept
    \* between listing and publish fails the pass rather than letting it stamp
    \* and supersede an object that is no longer there (finding, #1289). The
    \* supersession and its stamp are applied only after this check passes.
    /\ \A o \in cmpInputs : PresentObj(o)
    /\ S!PutCreateIfAbsent("cmpA", "dat")
    /\ cmpPhase' = "done"
    /\ superseded' = superseded \cup cmpInputs
    \* Same first-supersession rule as PublishRewrite: an input a rewrite
    \* already superseded keeps that rewrite's clock, so this publish cannot
    \* push the input's protection horizon forward.
    /\ supersededAt' = [i \in SupersededCandidates |->
                          IF i \in cmpInputs /\ i \notin superseded
                              THEN clock
                              ELSE supersededAt[i]]
    /\ objContent' = [objContent EXCEPT !["cmpA"] = RecordSetContent("cmpA")]
    /\ UNCHANGED <<head, headState, clock, heldBuckets, refreshFailed, query,
                   erasureRequested, tombRetiredAt, dreqHorizon, doneAt, sysgc,
                   variantKey, leaseOwner, rwPhase, rwInputs, cmpInputs>>
    /\ NoGc

\* A listed compaction whose recorded input vanished between listing and publish
\* returns to idle. compact_bucket_scoped aborts the pass when an input it listed
\* is gone at read time; the model mirrors that by dropping the pass rather than
\* publishing over a missing object. Without this the presence guard on
\* PublishCompaction would strand the pass in "listed" forever and, under
\* SerializeCompactionAndRewrite, block every rewrite behind a stall the shipped
\* driver does not have. A fresh StartCompaction may re-list afterwards if a raw
\* input is still present.
CancelCompaction ==
    /\ cmpPhase = "listed"
    /\ ~PresentObj("cmpA")
    /\ \E o \in cmpInputs : ~PresentObj(o)
    /\ cmpPhase' = "idle"
    /\ cmpInputs' = {}
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets, refreshFailed,
                   query, erasureRequested, tombRetiredAt, dreqHorizon, doneAt,
                   sysgc, supersededAt, objContent, variantKey, leaseOwner,
                   rwPhase, rwInputs>>
    /\ NoGc

\* Switch the HEAD onto the live record sets, dropping the superseded objects (a
\* fold advancing). It may lag arbitrarily behind the publish that superseded them.
HeadAdvanceRewrite ==
    /\ LiveRecordSets # {}
    /\ head \cap superseded # {}
    /\ head' = (head \ superseded) \cup LiveRecordSets
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

\* Complete the erasure: write .done only when the served set no longer serves the
\* subject (bucket_erasure_completion over bucket_serves_subject). completed is
\* the current, non-zero clock. A legal hold on a still-present superseded input
\* that serves the subject blocks completion unconditionally (finding 1):
\* bucket_is_held is checked before the served-set read and has no switch of its
\* own in the code, so the model gates on it the same way, with no bypass.
CompleteErasure ==
    /\ ~PresentObj("doneR1")
    /\ PresentObj("dreqR1")
    /\ HeadDeletable   \* completion needs a real served-set read of HEAD
    /\ (CompleteIgnoresServedSet \/ ~ServesNow("s1"))
    /\ ~HeldInputServes("s1")
    /\ clock > 0
    /\ S!PutOverwrite("doneR1", "dat")
    /\ doneAt' = clock
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ CompletionWitness

--------------------------------------------------------------------------------
\* Retention actor (maintainer)
--------------------------------------------------------------------------------

\* Write the retention tombstone for b1 (CreateIfAbsent, irreversible).
\* retired_at is the current clock. crates/ravel-maintain retention write_tombstone.
RetireBucket ==
    /\ ~PresentObj("tombB1")
    /\ S!PutCreateIfAbsent("tombB1", "dat")
    /\ tombRetiredAt' = [tombRetiredAt EXCEPT !["b1"] = clock]
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

\* Fold reconciles a retired bucket out of the HEAD; it may lag (a late fold) and
\* seals the bucket only after its own retention window (FoldRetentionWindow). When
\* that window exceeds the sweep's, the head-empty gate below keeps the sweep
\* waiting on the fold (#1131).
DropRetiredBucketFromHead ==
    /\ PresentObj("tombB1")
    /\ clock >= tombRetiredAt["b1"] + FoldRetentionWindow
    /\ \E o \in head : Bucket(o) = "b1"
    /\ head' = {o \in head : Bucket(o) # "b1"}
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>
    /\ NoGc

\* Retention physical sweep of one b1 data object. Gates on now >= retired_at +
\* protection_horizon (DeleteBeforeHorizon drops this) AND the current HEAD naming
\* nothing in the bucket. The head-empty check reads EffectiveHead (an absent
\* HEAD names nothing, so it is vacuously empty for this bucket), and the pass
\* runs on any readable HEAD (HeadReadable): only an unreadable read fails
\* closed, because a present-but-undecodable HEAD may still name the object
\* (reachability.rs bucket_gate; finding 1, round four -- an absent HEAD used to
\* block here too, stricter than the shipped gate). A failed hold refresh skips
\* the whole tick. A held object is never swept. Base additionally respects an
\* in-window pinned query (HorizonGuardsPinnedQueries); candidate #1133 sets
\* that FALSE.
QueryPermits(o) ==
    HorizonGuardsPinnedQueries =>
        ~(query.active /\ clock <= query.deadline /\ o \in query.needs)

RetentionSweep(o) ==
    /\ HeadReadable
    /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)
    /\ o \in DataObjects
    /\ Bucket(o) = "b1"
    /\ PresentObj(o)
    /\ PresentObj("tombB1")
    /\ (DeleteBeforeHorizon \/ clock >= tombRetiredAt["b1"] + sysgc.ph)
    /\ clock >= tombRetiredAt["b1"] + SweepRetentionWindow
    /\ \A x \in EffectiveHead : Bucket(x) # "b1"
    /\ ~HeldObject(o, heldBuckets)
    /\ QueryPermits(o)
    /\ S!Delete(o)
    /\ GcWitness("retention", {o})
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>

\* Final tombstone delete (finding 3, round four): physical_sweep deletes the
\* bucket's data, verifies via bucket_is_empty_but_tombstone that only the
\* tombstone remains, then deletes the tombstone itself and reports the
\* bucket swept. The model stopped at the data delete; this adds the missing
\* last step, gated the same as the code: the same bucket_gate read
\* RetentionSweep uses (HeadReadable, EffectiveHead not naming the bucket),
\* the same LeaseCheck instance the data deletes used (is_protected on the
\* tombstone key, so a failed refresh fails closed here too), and the bucket
\* holding nothing but the tombstone.
SweepTombstone ==
    /\ HeadReadable
    /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)
    /\ \A x \in EffectiveHead : Bucket(x) # "b1"
    /\ PresentObj("tombB1")
    /\ \A o \in DataObjects : Bucket(o) = "b1" => ~PresentObj(o)
    /\ S!Delete("tombB1")
    /\ GcWitness("tombstone", {"tombB1"})
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>

--------------------------------------------------------------------------------
\* Physical GC actor (maintainer): superseded-input sweep and .dreq sweep
--------------------------------------------------------------------------------

\* Superseded-input sweep of one raw input. Object-granular HEAD gate
\* (reachability object_gate): an input EffectiveHead still names is HELD (an
\* absent HEAD names nothing, so it never holds one). The pass runs on any
\* readable HEAD (HeadReadable); only an unreadable read fails closed (finding
\* 1, round four -- an absent HEAD used to block here too, stricter than the
\* shipped gate). The delete is horizon-gated (sweep.rs skips a record younger
\* than the protection horizon) and respects an in-window pinned query.
\* SupersededSweepUngated drops the head-membership check.
SupersededGatePasses(o) ==
    IF SupersededSweepUngated THEN TRUE ELSE o \notin EffectiveHead

SupersededSweep(o) ==
    /\ HeadReadable
    /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)
    /\ o \in superseded
    /\ PresentObj(o)
    /\ ~HeldObject(o, heldBuckets)
    /\ (DeleteBeforeHorizon \/ clock >= supersededAt[o] + sysgc.ph)
    /\ QueryPermits(o)
    /\ SupersededGatePasses(o)
    /\ S!Delete(o)
    /\ GcWitness("superseded", {o})
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>

\* .dreq sweep: delete the .dreq when a matching .done exists, its completed
\* timestamp is non-zero, the horizon has passed, no reader (the current HEAD or
\* a permitted pinned query) still reaches a present object serving the subject
\* (once the .dreq read-time filter is gone, such a reader would serve the erased
\* subject), AND no held superseded input still serves the subject (finding 1:
\* sweep.rs folds chain_groups_held_by_legal_hold into held_request_ids alongside
\* the HEAD-named and unreadable-HEAD hold reasons, so a legally held input blocks
\* the .dreq the same way a HEAD-named one does, regardless of live reachability).
\* The ~ServesAny clause is unconditional; DreqIgnoresHeldInputs drops only the
\* held-input clause, so its name and its behaviour agree.
DreqSweep ==
    /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)
    /\ HeadReadable
    /\ PresentObj("dreqR1")
    /\ PresentObj("doneR1")
    /\ doneAt > 0
    /\ clock >= dreqHorizon
    /\ ~ServesAny("s1")
    /\ (DreqIgnoresHeldInputs \/ ~HeldInputServes("s1"))
    /\ S!Delete("dreqR1")
    /\ GcWitness("dreq", {"dreqR1"})
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey,
                   maintVars>>

--------------------------------------------------------------------------------
Next ==
    \/ Tick
    \/ PinQuery \/ ExpireQuery
    \/ PlaceHold("b1")
    \/ ReleaseHold("b1")
    \/ \E s \in {"present","absent","unreadable"} : SetHeadState(s)
    \/ \E f \in BOOLEAN : SetRefresh(f)
    \/ RequestErasure
    \/ \E id \in RewriteIds : StartRewrite(id)
    \/ \E id \in RewriteIds : PublishRewrite(id)
    \/ ExpireLease
    \/ StartCompaction
    \/ PublishCompaction
    \/ CancelCompaction
    \/ HeadAdvanceRewrite
    \/ CompleteErasure
    \/ RetireBucket
    \/ DropRetiredBucketFromHead
    \/ \E o \in DataObjects : RetentionSweep(o)
    \/ SweepTombstone
    \/ \E o \in SupersededCandidates : SupersededSweep(o)
    \/ DreqSweep

Spec == Init /\ [][Next]_vars

\* A terminal state (all writes done, clock exhausted) is legitimate; the cfgs set
\* CHECK_DEADLOCK FALSE. Terminal is documented, not enforced.
Terminal ==
    /\ clock = MaxClock
    /\ ~(ENABLED PinQuery) /\ ~(ENABLED ExpireQuery)

--------------------------------------------------------------------------------
\* Named safety invariants
--------------------------------------------------------------------------------

\* Four clauses, all read from the witness of the delete that actually happened
\* (its observed clock, its deleted set) against recorded state, never a switch:
\*  1. A retention delete happened no earlier than retired_at + protection_horizon
\*     (delete-before-horizon drops that gate, so this clause fires).
\*  2. A superseded-input delete happened no earlier than THAT OBJECT's own
\*     supersededAt plus protection_horizon. Per object, not against one shared
\*     stamp: two publishing passes (a compaction and a rewrite over its output)
\*     supersede different objects at different clocks, and a single stamp would
\*     let the later supersession retroactively re-open the protection window of
\*     an earlier delete that was legitimate when it ran.
\*  3. A .dreq delete happened no earlier than its own horizon (dreqHorizon,
\*     frozen once RequestErasure sets it, so reading it live is the same
\*     per-step witness reasoning as tombRetiredAt/supersededAt above). Unlike
\*     the other two rules, .dreq has no dedicated switch that drops this gate
\*     in the shipped model; a scratch removal of the gate from DreqSweep is
\*     what proves this clause can fire (results.md).
\*  4. No horizon-gated delete removed an object a permitted in-flight query still
\*     needs (a query is permitted while within max_query_duration of its pin).
\*     Candidate #1133 (HorizonGuardsPinnedQueries FALSE, the shipped delete that
\*     gates on horizon AND head-empty but not on pinned queries) makes a query
\*     pinned on a stale HEAD that a late fold then drops fire this clause. This
\*     clause is structurally unable to fire for lastGc.rule = "dreq": .dreq is
\*     a control object, lastGc.permittedNeeds is always a subset of
\*     DataObjects (PermittedNeeds reads query.needs, which PinQuery draws only
\*     from head \subseteq DataObjects), so the intersection is empty in every
\*     state regardless of any guard. That is why clause 3 above, not this one,
\*     is the .dreq horizon check.
NoDeleteInsideProtectionWindow ==
    /\ ( lastGc.rule = "retention" =>
             lastGc.atClock >= tombRetiredAt["b1"] + sysgc.ph )
    /\ ( lastGc.rule = "superseded" =>
             \A o \in lastGc.deleted :
                 lastGc.atClock >= supersededAt[o] + sysgc.ph )
    /\ ( lastGc.rule = "dreq" =>
             lastGc.atClock >= dreqHorizon )
    /\ ( lastGc.rule \in HorizonGatedRules =>
             (lastGc.deleted \cap lastGc.permittedNeeds) = {} )

\* No object under a legal hold is ever deleted by any sweep (the hold state the
\* delete observed at its own step).
HeldObjectNeverDeleted ==
    ~lastGc.held

\* A failed hold refresh skips the whole tick: no deletion happens while the
\* refresh failed (the refresh state the delete observed at its own step).
RefreshFailureNeverSweeps ==
    (lastGc.deleted # {}) => (lastGc.refreshWasFailed = FALSE)

\* The retention tombstone excludes the bucket before any of its objects is
\* deleted: a retention delete implies the tombstone was written (retired_at
\* recorded, non-zero-window) no later than the delete.
TombstoneExcludesBeforeDelete ==
    lastGc.rule = "retention" =>
        /\ PresentObj("tombB1")
        /\ \A o \in lastGc.deleted :
              /\ Bucket(o) = "b1"
              /\ tombRetiredAt["b1"] <= lastGc.atClock

\* The tombstone itself is never deleted while any of its bucket's data
\* objects are still present: physical_sweep only deletes the tombstone
\* after bucket_is_empty_but_tombstone confirms nothing else remains. Kept
\* separate from TombstoneExcludesBeforeDelete (finding 3, round four) so
\* each rule's own claim, the tombstone existing before a data delete versus
\* the tombstone outliving every data delete, stays independently falsifiable.
TombstoneNotDeletedBeforeBucketEmpty ==
    lastGc.rule = "tombstone" =>
        \A o \in DataObjects : Bucket(o) = "b1" => ~PresentObj(o)

\* Once an erasure is requested for a subject, that subject is never served
\* again: the modeled read (ServedRead) applies the erasure predicate after the
\* store fetch. The model has no cache tier; the production read applies the same
\* predicate after its cache, but that ordering is not something this model shows.
ErasedSubjectNeverServedAfterRequest ==
    \A s \in erasureRequested : ~ServedRead(s)

\* A rewrite output serves exactly its inputs' subjects minus the erased ones.
\* Quantified over both rewrite identities' outputs (issue #1221): rwA rewrites the
\* raw L0 inputs, rwB rewrites the compaction output, so the second conjunct is a
\* real claim about a rewrite whose predecessor is itself a published record set.
RewriteOutputsAreInputsMinusErased ==
    \A w \in RewriteOut :
        PresentObj(w) =>
            \A s \in Subjects :
                ServesSubject(w, s) <=>
                    ( (\E i \in Predecessors(w) : ServesSubject(i, s))
                      /\ s \notin ErasedBy(AppliedReqs(w)) )

\* The target a resolved input set selects is the record set whose static
\* predecessors ARE that resolved set. TargetOf maps every resolved set other than
\* CompactOut to rwA, and PublishRewrite writes objContent'[tgt] from
\* RecordSetContent(tgt), which reads the static Predecessors(tgt); the published
\* content and the variant naming are only sound when the target's predecessors
\* equal the set the pass actually resolved. In this instance RawInputs and
\* CompactOut are the only resolvable input sets and each maps to a target whose
\* predecessors match it, so the link holds -- but it holds because those two sets
\* are singletons, not because the mapping checks anything. In a larger instance a
\* pass that resolved a proper subset of RawInputs would still target rwA (the
\* catch-all branch of TargetOf) and publish content derived from the full
\* RawInputs, and RewriteOutputsAreInputsMinusErased, which reads the same static
\* predecessors, would then pass for a rewrite that never happened. This invariant
\* makes that agreement a checked precondition rather than a coincidence: for every
\* identity whose publish has run, the target's predecessors equal the resolved
\* input set the pass recorded. counterexamples/rewrite-target-matches-resolved-inputs-probe.md
\* resolves a proper subset in a two-raw-input scratch, shows this VIOLATED on the
\* current mapping, then holding once the target names the resolved set.
RewriteTargetMatchesResolvedInputs ==
    \A id \in RewriteIds :
        rwPhase[id] = "done" =>
            Predecessors(TargetOf(rwInputs[id])) = rwInputs[id]

\* At most one record set a reader can be served from references objects that a
\* completed rewrite has already superseded (issue #1289).
\*
\* "A reader can be served from o" is o being a live record set: present in the
\* store and not itself superseded, so a HEAD advance or a listing fallback can
\* resolve onto it. "References objects a completed rewrite has superseded" is
\* o's predecessors intersecting `superseded`, which is exactly the state a
\* published record set leaves behind.
\*
\* Two such sets at once is the #1289 hazard: the erasure rewrite published rwA
\* over raw1 with the erased records dropped and marked raw1 superseded, and a
\* compaction that listed the bucket before that record was durable published cmpA
\* over the same raw1 with nothing dropped. Both are live, both cover the same
\* inputs, and a snapshot that resolves onto cmpA serves the records rwA removed.
\* Neither the store nor the catalog excludes the pair: they land under different
\* key classes, so each publish's CreateIfAbsent succeeds.
\*
\* The invariant is stated over the store and the supersession set, never over a
\* switch and never over a flag an action sets to certify itself, so
\* CompactionIgnoresRewrite falsifies it by changing the reachable behaviour.
\* negative/compaction-ignores-rewrite.cfg is the proof that it can fire.
AtMostOneLiveRecordSetServed ==
    Cardinality({ o \in RecordSets :
                    /\ PresentObj(o)
                    /\ o \notin superseded
                    /\ Predecessors(o) \cap superseded # {} }) <= 1

\* Completion implies no pre-rewrite exposure: once .done exists, the current HEAD
\* no longer serves the erased subject (the rewrite advanced HEAD off it). A pinned
\* reader holding an older snapshot is handled separately by the .dreq read-time
\* filter (DreqRemovalCannotResurrect), not by completion.
CompletionImpliesNoPreRewriteExposure ==
    PresentObj("doneR1") => ~ServesNow("s1")

\* Legal hold wins over erasure completion (finding 1, ADR-0064 section 6): a
\* still-present, legally held superseded input that served the erased subject
\* at the moment CompleteErasure ran means that step should not have happened.
\* Reads the CompletionWitness lastGc set at that step, not the live HeldInputServes:
\* a hold placed or released AFTER a legitimate completion is a different bucket
\* state, not evidence the completion itself was wrong, so the check is scoped to
\* CompleteErasure's own transition the same way NoDeleteInsideProtectionWindow is
\* scoped to a delete's own transition via `held`/`atClock`.
CompletionRespectsLegalHold ==
    (lastGc.rule = "complete") => ~lastGc.heldInputServed

\* Removing the .dreq cannot resurrect the subject: if the .dreq is gone after a
\* request, no reader (current HEAD or a permitted pinned query) still serves it.
\* The pinned-reader case is handled here; a held-but-unreachable input is a
\* separate concern covered by DreqSweepRespectsLegalHold below, scoped to the
\* sweep's own step for the same reason CompletionRespectsLegalHold is scoped to
\* CompleteErasure's: a hold placed after this sweep already ran does not mean
\* the sweep resurrected anything.
DreqRemovalCannotResurrect ==
    ("s1" \in erasureRequested /\ ~PresentObj("dreqR1")) => ~ServesAny("s1")

\* Legal hold wins over the .dreq sweep (finding 1): a legally held superseded
\* input that served the erased subject at the moment DreqSweep ran means that
\* step should not have happened (sweep.rs folds chain_groups_held_by_legal_hold
\* into held_request_ids, gating the sweep the same way bucket_is_held gates
\* completion). Reads the GcWitness lastGc set at DreqSweep's own step ("dreq"),
\* not the live HeldInputServes, for the same retroactivity reason as
\* CompletionRespectsLegalHold.
DreqSweepRespectsLegalHold ==
    (lastGc.rule = "dreq") => ~lastGc.heldInputServed

\* Two rewrites over the same input set with different applied requests get
\* different keys (the hash binds the sorted applied ids). Reads the names
\* PublishRewrite actually stored (variantKey), not the RewriteKey operator, so the
\* property observes what the write produced (finding 4). RewriteIdentityOmitsRequests
\* drops the applied ids from the key, collapsing the two names.
IdenticalInputSetsDoNotCollide ==
    (\E w \in RewriteOut : PresentObj(w)) => variantKey["v1"] # variantKey["v2"]

\* An object a real HEAD read still names must be present: no sweep may delete a
\* HEAD-named raw input. Reads the store presence against EffectiveHead, the same
\* view SupersededGatePasses gates on, not the raw `head` variable: `head` and
\* `headState` are independent (SetHeadState churns the read outcome without
\* touching `head`), so once finding 1 (round four) let the sweep proceed on an
\* absent read, `head` can still name an input that no real reader can observe,
\* and holding the sweep to that unobservable truth would demand more than the
\* shipped gate (or any reader) can ever know. SupersededSweepUngated still
\* deletes an EffectiveHead-named input and fires this.
HeadNamedObjectNeverDeletedBySupersededSweep ==
    \A o \in SupersededCandidates : o \in EffectiveHead => PresentObj(o)

\* Environmental assumption, not a protocol property: a raw input's content
\* never changes across a reachable behaviour. Data objects are immutable in
\* Ravel (docs/object-store-contract.md, `put_data_object`'s CreateIfAbsent
\* path), so no Next action models a raw-input replacement (issue #1122,
\* finding 1). This pins that the model actually respects the assumption it is
\* built on: the only writer of `objContent` for a raw input is `Init`, and no
\* other action's UNCHANGED list omits it. If a future edit added a raw-input
\* mutation, this is the invariant that would catch it, not
\* `RewriteOutputsAreInputsMinusErased`, which reads `objContent` for both
\* rewrite outputs, `rwA` and `rwB`, and for their predecessors: the raw inputs
\* under `rwA` and the compaction output under `rwB`. It constrains those
\* outputs against whatever their predecessors currently hold, so it cannot
\* pin that a raw input's own content never moved. README.md's assumptions
\* section explains why a replacement transition is out of scope rather than
\* added.
RawInputContentAssumedImmutable ==
    \A o \in RawInputs : objContent[o] = InitContent(o)

--------------------------------------------------------------------------------
\* Liveness (checked against FairSpec only; see README and #1131, and the
\* checkpoint-finding-1 diagnosis in results.md). Weak fairness on the actions
\* the implementation justifies: the maintainer tick (sweeps), the folder's HEAD
\* advance, store completion, the clock itself, pinned-query expiry, and the
\* first superseding rewrite. PlaceHold, ReleaseHold, SetHeadState, and
\* SetRefresh stay unfair on purpose: nothing in the implementation guarantees
\* a legal hold is released, a HEAD read recovers, or a refresh eventually
\* succeeds, so a spec that assumed fairness there would assert a guarantee the
\* implementation doesn't make. StartRewrite's fairness is restricted to its
\* first firing (superseded = {}): the implementation runs one rewrite per
\* erasure request, not a loop that keeps re-deriving an already-produced
\* rewrite output every time ordinary retention ages it out, so granting it
\* unconditional fairness would force a publish loop the implementation
\* doesn't have: RetentionSweep deletes the rewrite output and a fair
\* StartRewrite recreates it, without end. That loop no longer moves the
\* inputs' protection horizon with it, since supersededAt keeps the first
\* supersession's clock, but the loop itself is still not what the pass does.
\* PublishRewrite and ExpireLease are treated differently: the publish IS fair
\* (a pass that already listed does eventually ack, which is the whole reason
\* the ack can land after the lease moved), while ExpireLease stays unfair
\* because nothing requires a lease to lapse.
\* StartCompaction stays unfair for the same reason PlaceHold does: nothing
\* guarantees a compaction ever runs on a bucket. PublishCompaction is fair
\* because a pass that has already listed does finish; leaving it unfair would let
\* a compaction sit in "listed" forever and, under
\* SerializeCompactionAndRewrite, block every rewrite behind a stall the
\* implementation does not have. CancelCompaction is fair for the same reason:
\* PublishCompaction is disabled once a recorded input is gone, so the abort is
\* the action that finishes a pass whose input vanished, and leaving it unfair
\* would reopen the same "listed" stall the publish fairness closes.
FairSpec ==
    /\ Spec
    /\ WF_vars(\E o \in SupersededCandidates : SupersededSweep(o)) \* maintainer sweep tick
    /\ WF_vars(HeadAdvanceRewrite)                       \* folder watermark advance
    /\ WF_vars(\E o \in DataObjects : RetentionSweep(o)) \* maintainer retention tick
    /\ WF_vars(CompleteErasure)                          \* store completion
    /\ WF_vars(Tick)                                     \* clock advances
    /\ WF_vars(ExpireQuery)                              \* pinned queries expire
    /\ WF_vars(\E id \in RewriteIds :
                 StartRewrite(id) /\ superseded = {})    \* the first rewrite lists
    /\ WF_vars(\E id \in RewriteIds : PublishRewrite(id)) \* a listed rewrite acks
    /\ WF_vars(PublishCompaction)                        \* a listed compaction acks
    /\ WF_vars(CancelCompaction)                         \* a stale compaction aborts

\* Every superseded input that becomes deletable is eventually swept, once its
\* own SupersededSweep guard (legal hold clear, horizon elapsed, no blocking
\* pinned query, HEAD readable, no failed refresh) holds permanently. Stated as
\* an explicit antecedent, not as "the environment eventually goes quiet" on
\* PlaceHold/ReleaseHold/SetHeadState/SetRefresh: those four stay unfair (see
\* FairSpec), and reviewers found real counterexamples (a hold that never
\* releases, a HEAD read that never recovers, a refresh that never succeeds)
\* where they never fire yet the old hypothesis's properties still failed.
\* This form is checkable at any MaxClock: confirmed at MaxClock=2 and
\* MaxClock=4 against the reduced per-property configuration (results.md).
\* PresentObj(o) is deliberately absent from this antecedent: SupersededSweep
\* is the action the antecedent describes, and its own effect is to remove o,
\* so an antecedent that also requires o present can never hold permanently
\* once the action is enabled -- the leads-to would be trivially true no
\* matter what the protocol did (finding, issue #1122). Dropping it leaves
\* the antecedent stateable independently of whether o already happens to be
\* gone, which is what makes the consequent a real claim.
EventuallySwept ==
    \A o \in SupersededCandidates :
        <>[](o \in superseded /\ ~HeldObject(o, heldBuckets)
             /\ (DeleteBeforeHorizon \/ clock >= supersededAt[o] + sysgc.ph)
             /\ QueryPermits(o) /\ SupersededGatePasses(o) /\ HeadReadable
             /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)) ~>
            ~PresentObj(o)

\* Every requested erasure is eventually completed, once CompleteErasure's own
\* guard holds permanently. Same rationale as EventuallySwept: an explicit
\* antecedent grounded in the action's real enabling condition, not a
\* quiescence hypothesis the four unfair environment actions can falsify.
\* ~PresentObj("doneR1") is deliberately absent from this antecedent for the
\* same reason PresentObj(o) is absent from EventuallySwept's: CompleteErasure
\* is the action being described, and its own effect is to write .done, so an
\* antecedent that also requires .done absent can never hold permanently once
\* the action is enabled (finding, issue #1122).
EventuallyCompleted ==
    <>[](PresentObj("dreqR1") /\ HeadDeletable
         /\ (CompleteIgnoresServedSet \/ ~ServesNow("s1"))
         /\ ~HeldInputServes("s1") /\ clock > 0) ~>
        PresentObj("doneR1")

===============================================================================
