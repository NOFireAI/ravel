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
(*  * `superseded` is the set of inputs a rewrite record has superseded         *)
(*    (crates/ravel-catalog resolve_rewrite_supersession); a superseded input   *)
(*    is a physical-GC candidate but is HELD while HEAD still names it.          *)
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
    HorizonGuardsPinnedQueries  \* base: a horizon-gated delete also respects an
                                \* in-window pinned query. Candidate #1133 sets it
                                \* FALSE to model the shipped delete, which gates on
                                \* horizon AND head-empty but NOT on pinned queries.

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
RawInputs     == {"raw1"}                   \* L0 raw input in b1, serves subject s1
RewriteOut    == {"rwA"}                    \* rewrite of {raw1} applying {r1}
DataObjects   == RawInputs \cup RewriteOut
ControlObjects== {"tombB1", "dreqR1", "doneR1", "sysgc"}
Objects       == DataObjects \cup ControlObjects

InitPresent   == RawInputs \cup {"sysgc"}

\* --- Static object metadata --------------------------------------------------
Bucket(o) == CASE o \in {"raw1","rwA","tombB1","dreqR1","doneR1"} -> "b1"
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

Predecessors(o) == IF o = "rwA" THEN RawInputs ELSE {}
AppliedReqs(o)  == IF o = "rwA" THEN Requests ELSE {}

\* Subjects erased by a set of request ids (r1 erases s1, never s2).
ErasedBy(reqs) == IF "r1" \in reqs THEN {"s1"} ELSE {}

\* Content of a raw input at Init: raw1 carries rec1 (s1, erasable) and rec2
\* (s2, must survive any rewrite applying r1); every other object carries no
\* records until an action writes it.
InitContent(o) == IF o \in RawInputs THEN {"rec1", "rec2"} ELSE {}

\* The record set the rewrite output should serve: its predecessors' records minus
\* the records whose subject the applied requests erased. RewriteKeepsErasedRecords
\* drops the minus (finding 3 behaviour mutant).
RewriteOutputContent ==
    LET inRecs == UNION { InitContent(i) : i \in Predecessors("rwA") }
    IN IF RewriteKeepsErasedRecords
           THEN inRecs
           ELSE { r \in inRecs : RecordSubject(r) \notin ErasedBy(AppliedReqs("rwA")) }

\* Rewrite descriptors for the identity-collision property: same input set, a
\* different applied-request set. The shipped key binds the sorted applied ids
\* (compute_rewrite_input_set_hash); the switch drops them so the two collide.
\* PerformRewrite names its two output variants by RewriteKey and stores those
\* names in `variantKey`; the invariant reads the stored names, not this operator
\* (finding 4). RewriteKey itself is what the action USES to name an object.
DescA == [inputs |-> RawInputs, reqs |-> {"r1"}]
DescB == [inputs |-> RawInputs, reqs |-> {}]
RewriteKey(d) == IF RewriteIdentityOmitsRequests
                     THEN <<d.inputs>>
                     ELSE <<d.inputs, d.reqs>>
\* The sentinel a variant name holds before PerformRewrite has assigned it.
UnnamedKey == <<>>

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
    superseded,       \* SUBSET RawInputs: inputs a rewrite has superseded
    heldBuckets,      \* SUBSET Buckets under a legal hold
    refreshFailed,    \* BOOLEAN: this tick's legal-hold refresh failed
    query,            \* [active, needs: SUBSET DataObjects, deadline: Nat]
    erasureRequested, \* SUBSET Subjects (monotone)
    tombRetiredAt,    \* [Buckets -> Nat]: retired_at, 0 when no tombstone
    dreqHorizon,      \* Nat: the .dreq horizon
    doneAt,           \* Nat: completion timestamp (0 when no .done)
    supersededAt,     \* Nat: clock at which the rewrite superseded its inputs (0 = none)
    objContent,       \* [Objects -> SUBSET AllRecords]: served record identities
    variantKey,       \* [{"v1","v2"} -> key]: the names PerformRewrite assigned
    sysgc,            \* [ph, mqd, grace, skew]
    lastGc            \* witness of the last GC deletion step

storeVars == <<store, lastModified, versionCounter, uploads, listState>>
protoVars == <<head, headState, clock, superseded, heldBuckets,
               refreshFailed, query, erasureRequested, tombRetiredAt,
               dreqHorizon, doneAt, supersededAt, objContent, variantKey,
               sysgc, lastGc>>
vars == <<store, lastModified, versionCounter, uploads, listState,
          head, headState, clock, superseded, heldBuckets,
          refreshFailed, query, erasureRequested, tombRetiredAt,
          dreqHorizon, doneAt, supersededAt, objContent, variantKey,
          sysgc, lastGc>>

S == INSTANCE RavelObjectStore
       WITH Keys <- Objects, Content <- {"dat", "nc"}, NoContent <- "nc",
            Clients <- {"mnt"}

PresentObj(o) == store[o].present

\* A subject is served by object o iff a record of that subject is in o's stored
\* content (finding 3: serving is a fact about stored content, not a static CASE).
ServesSubject(o, s) == \E r \in objContent[o] : RecordSubject(r) = s

\* State-space view: the invariants and every gate read object PRESENCE, never the
\* store's version, content, upload or listing bookkeeping. Projecting those away
\* collapses the states that differ only in the version integer a particular write
\* ordering assigned, which is the dominant source of otherwise-equivalent states.
StoreView == [o \in Objects |-> store[o].present]
View ==
    <<StoreView, head, headState, clock, superseded, heldBuckets, refreshFailed,
      query, erasureRequested, tombRetiredAt, dreqHorizon, doneAt, supersededAt,
      objContent, variantKey, sysgc, lastGc>>

\* A delete decision needs a real read of the current HEAD object. Only a present
\* read authorises a decision about whether an object is still referenced; an
\* absent or unreadable read fails closed (the sweep does not delete), because the
\* true HEAD may still name the object (finding 5). HeadReadable is kept for the
\* serving predicate, which treats a missing HEAD as naming nothing (fail-safe).
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
    /\ store \in [Objects -> RecT]
    /\ versionCounter \in Nat
    /\ head \subseteq DataObjects
    /\ headState \in {"present","absent","unreadable"}
    /\ clock \in 0..MaxClock
    /\ superseded \subseteq RawInputs
    /\ heldBuckets \subseteq Buckets
    /\ refreshFailed \in BOOLEAN
    /\ query \in [active: BOOLEAN, needs: SUBSET DataObjects, deadline: 0..(MaxClock + MaxQueryDuration)]
    /\ erasureRequested \subseteq Subjects
    /\ tombRetiredAt \in [Buckets -> 0..MaxClock]
    /\ dreqHorizon \in Nat
    /\ doneAt \in 0..MaxClock
    /\ supersededAt \in 0..MaxClock
    /\ objContent \in [Objects -> SUBSET AllRecords]
    /\ variantKey \in [{"v1","v2"} -> {UnnamedKey, RewriteKey(DescA), RewriteKey(DescB)}]
    /\ sysgc \in [ph: Nat, mqd: Nat, grace: Nat, skew: Nat]
    /\ lastGc.rule \in {"none","superseded","retention","dreq","complete"}
    /\ lastGc.deleted \subseteq Objects
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
    /\ supersededAt = 0
    /\ objContent = [o \in Objects |-> InitContent(o)]
    /\ variantKey = [v \in {"v1","v2"} |-> UnnamedKey]
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
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
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
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
    /\ NoGc

ExpireQuery ==
    /\ query.active
    /\ clock > query.deadline
    /\ query' = [active |-> FALSE, needs |-> {}, deadline |-> 0]
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
    /\ NoGc

\* Place / release a legal hold on bucket b (its data prefixes).
PlaceHold(b) ==
    /\ b \notin heldBuckets
    /\ heldBuckets' = heldBuckets \cup {b}
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
    /\ NoGc

ReleaseHold(b) ==
    /\ b \in heldBuckets
    /\ heldBuckets' = heldBuckets \ {b}
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
    /\ NoGc

\* The HEAD object read can fail (unreadable) or find the HEAD gone (absent).
SetHeadState(s) ==
    /\ FullEnv
    /\ s # headState
    /\ headState' = s
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
    /\ NoGc

\* Toggle this tick's legal-hold refresh outcome.
SetRefresh(f) ==
    /\ FullEnv
    /\ f # refreshFailed
    /\ refreshFailed' = f
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
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
                   refreshFailed, query, tombRetiredAt, doneAt, sysgc, supersededAt, objContent, variantKey>>
    /\ NoGc

\* Materialise the rewrite output rwA = inputs minus the erased subject and mark
\* the inputs superseded (resolve_rewrite_supersession). The HEAD is NOT switched
\* here; a later HeadAdvance drops the superseded inputs, so between the two the
\* inputs are still HEAD-named and the superseded sweep must hold them.
PerformRewrite ==
    /\ ~PresentObj("rwA")
    /\ S!PutOverwrite("rwA", "dat")
    /\ superseded' = superseded \cup RawInputs
    /\ supersededAt' = clock
    /\ objContent' = [objContent EXCEPT !["rwA"] = RewriteOutputContent]
    /\ variantKey' = [variantKey EXCEPT !["v1"] = RewriteKey(DescA),
                                        !["v2"] = RewriteKey(DescB)]
    /\ UNCHANGED <<head, headState, clock, heldBuckets, refreshFailed, query,
                   erasureRequested, tombRetiredAt, dreqHorizon, doneAt, sysgc>>
    /\ NoGc

\* Switch the HEAD onto the rewrite output, dropping the superseded raw inputs
\* (a fold advancing). It may lag arbitrarily behind PerformRewrite.
HeadAdvanceRewrite ==
    /\ PresentObj("rwA")
    /\ RawInputs \cap head # {}
    /\ head' = (head \ RawInputs) \cup {"rwA"}
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
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
                   dreqHorizon, sysgc, supersededAt, objContent, variantKey>>
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
                   refreshFailed, query, erasureRequested, dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
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
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>
    /\ NoGc

\* Retention physical sweep of one b1 data object. Gates on now >= retired_at +
\* protection_horizon (DeleteBeforeHorizon drops this) AND the current HEAD naming
\* nothing in the bucket. The head-empty check reads the real HEAD, and the pass
\* runs only on a present HEAD read (HeadDeletable): an absent or unreadable read
\* fails closed, because the true HEAD may still name the object (finding 5). A
\* failed hold refresh skips the whole tick. A held object is never swept. Base
\* additionally respects an in-window pinned query (HorizonGuardsPinnedQueries);
\* candidate #1133 sets that FALSE.
QueryPermits(o) ==
    HorizonGuardsPinnedQueries =>
        ~(query.active /\ clock <= query.deadline /\ o \in query.needs)

RetentionSweep(o) ==
    /\ HeadDeletable
    /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)
    /\ o \in DataObjects
    /\ Bucket(o) = "b1"
    /\ PresentObj(o)
    /\ PresentObj("tombB1")
    /\ (DeleteBeforeHorizon \/ clock >= tombRetiredAt["b1"] + sysgc.ph)
    /\ clock >= tombRetiredAt["b1"] + SweepRetentionWindow
    /\ \A x \in head : Bucket(x) # "b1"
    /\ ~HeldObject(o, heldBuckets)
    /\ QueryPermits(o)
    /\ S!Delete(o)
    /\ GcWitness("retention", {o})
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>

--------------------------------------------------------------------------------
\* Physical GC actor (maintainer): superseded-input sweep and .dreq sweep
--------------------------------------------------------------------------------

\* Superseded-input sweep of one raw input. Object-granular HEAD gate
\* (reachability object_gate): an input the current HEAD still names is HELD. The
\* pass runs only on a present HEAD read (HeadDeletable); an absent or unreadable
\* read fails closed. The delete is horizon-gated (sweep.rs skips a record younger
\* than the protection horizon) and respects an in-window pinned query.
\* SupersededSweepUngated drops the head-membership check.
SupersededGatePasses(o) ==
    IF SupersededSweepUngated THEN TRUE ELSE o \notin head

SupersededSweep(o) ==
    /\ HeadDeletable
    /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)
    /\ o \in superseded
    /\ PresentObj(o)
    /\ ~HeldObject(o, heldBuckets)
    /\ (DeleteBeforeHorizon \/ clock >= supersededAt + sysgc.ph)
    /\ QueryPermits(o)
    /\ SupersededGatePasses(o)
    /\ S!Delete(o)
    /\ GcWitness("superseded", {o})
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>

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
                   dreqHorizon, doneAt, sysgc, supersededAt, objContent, variantKey>>

--------------------------------------------------------------------------------
Next ==
    \/ Tick
    \/ PinQuery \/ ExpireQuery
    \/ PlaceHold("b1")
    \/ ReleaseHold("b1")
    \/ \E s \in {"present","absent","unreadable"} : SetHeadState(s)
    \/ \E f \in BOOLEAN : SetRefresh(f)
    \/ RequestErasure
    \/ PerformRewrite
    \/ HeadAdvanceRewrite
    \/ CompleteErasure
    \/ RetireBucket
    \/ DropRetiredBucketFromHead
    \/ \E o \in DataObjects : RetentionSweep(o)
    \/ \E o \in RawInputs : SupersededSweep(o)
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

\* Two clauses, both read from the witness of the delete that actually happened
\* (its observed clock, its deleted set) against recorded state, never a switch:
\*  1. A retention delete happened no earlier than retired_at + protection_horizon
\*     (delete-before-horizon drops that gate, so this clause fires).
\*  2. No horizon-gated delete removed an object a permitted in-flight query still
\*     needs (a query is permitted while within max_query_duration of its pin).
\*     Candidate #1133 (HorizonGuardsPinnedQueries FALSE, the shipped delete that
\*     gates on horizon AND head-empty but not on pinned queries) makes a query
\*     pinned on a stale HEAD that a late fold then drops fire this clause.
NoDeleteInsideProtectionWindow ==
    /\ ( lastGc.rule = "retention" =>
             lastGc.atClock >= tombRetiredAt["b1"] + sysgc.ph )
    /\ ( lastGc.rule = "superseded" =>
             lastGc.atClock >= supersededAt + sysgc.ph )
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

\* Once an erasure is requested for a subject, that subject is never served
\* again: the modeled read (ServedRead) applies the erasure predicate after the
\* store fetch. The model has no cache tier; the production read applies the same
\* predicate after its cache, but that ordering is not something this model shows.
ErasedSubjectNeverServedAfterRequest ==
    \A s \in erasureRequested : ~ServedRead(s)

\* A rewrite output serves exactly its inputs' subjects minus the erased ones.
RewriteOutputsAreInputsMinusErased ==
    PresentObj("rwA") =>
        \A s \in Subjects :
            ServesSubject("rwA", s) <=>
                ( (\E i \in Predecessors("rwA") : ServesSubject(i, s))
                  /\ s \notin ErasedBy(AppliedReqs("rwA")) )

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
\* PerformRewrite actually stored (variantKey), not the RewriteKey operator, so the
\* property observes what the write produced (finding 4). RewriteIdentityOmitsRequests
\* drops the applied ids from the key, collapsing the two names.
IdenticalInputSetsDoNotCollide ==
    PresentObj("rwA") => variantKey["v1"] # variantKey["v2"]

\* An object the current HEAD still names must be present: no sweep may delete a
\* HEAD-named raw input. Reads the store presence against the live HEAD set, so the
\* object-granular gate is observed on the store, not a witness field.
\* SupersededSweepUngated deletes a HEAD-named input and fires this.
HeadNamedObjectNeverDeletedBySupersededSweep ==
    \A o \in RawInputs : o \in head => PresentObj(o)

--------------------------------------------------------------------------------
\* Liveness (checked against FairSpec only; see README and #1131, and the
\* checkpoint-finding-1 diagnosis in results.md). Weak fairness on the actions
\* the implementation justifies: the maintainer tick (sweeps), the folder's HEAD
\* advance, store completion, the clock itself, pinned-query expiry, and the
\* first superseding rewrite. PlaceHold, ReleaseHold, SetHeadState, and
\* SetRefresh stay unfair on purpose: nothing in the implementation guarantees
\* a legal hold is released, a HEAD read recovers, or a refresh eventually
\* succeeds, so a spec that assumed fairness there would assert a guarantee the
\* implementation doesn't make. PerformRewrite's fairness is restricted to its
\* first firing (superseded = {}): the implementation runs one rewrite per
\* erasure request, not a loop that keeps re-deriving an already-produced
\* rewrite output every time ordinary retention ages it out, so granting it
\* unconditional fairness would force a livelock the implementation doesn't
\* have (RetentionSweep deleting the rewrite output, PerformRewrite recreating
\* it and re-stamping the shared supersededAt, forever deferring the raw
\* inputs' own sweep).
FairSpec ==
    /\ Spec
    /\ WF_vars(\E o \in RawInputs : SupersededSweep(o))  \* maintainer sweep tick
    /\ WF_vars(HeadAdvanceRewrite)                       \* folder watermark advance
    /\ WF_vars(\E o \in DataObjects : RetentionSweep(o)) \* maintainer retention tick
    /\ WF_vars(CompleteErasure)                          \* store completion
    /\ WF_vars(Tick)                                     \* clock advances
    /\ WF_vars(ExpireQuery)                              \* pinned queries expire
    /\ WF_vars(PerformRewrite /\ superseded = {})        \* the first rewrite fires

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
EventuallySwept ==
    \A o \in RawInputs :
        <>[](o \in superseded /\ PresentObj(o) /\ ~HeldObject(o, heldBuckets)
             /\ (DeleteBeforeHorizon \/ clock >= supersededAt + sysgc.ph)
             /\ QueryPermits(o) /\ SupersededGatePasses(o) /\ HeadDeletable
             /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)) ~>
            ~PresentObj(o)

\* Every requested erasure is eventually completed, once CompleteErasure's own
\* guard holds permanently. Same rationale as EventuallySwept: an explicit
\* antecedent grounded in the action's real enabling condition, not a
\* quiescence hypothesis the four unfair environment actions can falsify.
EventuallyCompleted ==
    <>[](~PresentObj("doneR1") /\ PresentObj("dreqR1") /\ HeadDeletable
         /\ (CompleteIgnoresServedSet \/ ~ServesNow("s1"))
         /\ ~HeldInputServes("s1") /\ clock > 0) ~>
        PresentObj("doneR1")

===============================================================================
