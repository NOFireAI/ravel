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
(* abstract cache, the sys/gc config -- lives in dedicated variables and is     *)
(* documented in README.md as an abstraction boundary.                          *)
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
(*    subject it must never be served again, even through the cache.            *)
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
    BadProtectionHorizon,\* a horizon that violates the startup inequality
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
    DreqIgnoresHeldInputs,      \* delete the .dreq while a superseded input is held
    RewriteIdentityOmitsRequests, \* the rewrite key hash ignores the applied ids
    GcConfigViolatesInequality, \* the sys/gc record fails the startup inequality
    HorizonGuardsPinnedQueries  \* base: a horizon-gated delete also respects an
                                \* in-window pinned query. Candidate #1133 sets it
                                \* FALSE to model the shipped delete, which gates on
                                \* horizon AND head-empty but NOT on pinned queries.

ASSUME ProtectionHorizon \in Nat /\ Grace \in Nat
ASSUME MaxQueryDuration \in Nat /\ ClockSkew \in Nat
ASSUME MaxClock \in Nat

\* --- The finite instance (a fixed bounded model; README documents the bounds) -
\* One retention bucket b1 with a single L0 raw input is the minimal slice that
\* exercises supersession, the held-input gate, retention delete and the pinned
\* query race. README records why a second raw input and a second bucket add no
\* new reachable invariant behaviour, only state.
Buckets   == {"b1"}
Subjects  == {"s1"}
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

\* Does o serve records of subject s? The raw input carries s1; the rewrite output
\* has s1 removed.
ServesSubject(o, s) == (o \in RawInputs) /\ (s = "s1")

Predecessors(o) == IF o = "rwA" THEN RawInputs ELSE {}
AppliedReqs(o)  == IF o = "rwA" THEN Requests ELSE {}

\* Subjects erased by a set of request ids (r1 erases s1).
ErasedBy(reqs) == IF "r1" \in reqs THEN {"s1"} ELSE {}

\* Rewrite descriptors for the identity-collision property: same input set, a
\* different applied-request set. The shipped key binds the sorted applied ids
\* (compute_rewrite_input_set_hash); the switch drops them so the two collide.
Descriptors == { [inputs |-> RawInputs, reqs |-> {"r1"}],
                 [inputs |-> RawInputs, reqs |-> {}] }
RewriteKey(d) == IF RewriteIdentityOmitsRequests
                     THEN <<d.inputs>>
                     ELSE <<d.inputs, d.reqs>>

\* Both descriptors are always materialised (a rewrite output really keyed by its
\* input-set hash exists for each). Kept as a fixed set, not a variable, so the
\* identity-collision property is checked without enlarging the state space.
Materialized == Descriptors

\* Legal-hold coverage: a hold on a bucket covers its data objects (l0/commit/l1)
\* but never the del prefix (.dreq/.done/tombstone) or sys.
HeldObject(o, heldB) == (o \in DataObjects) /\ (Bucket(o) \in heldB)

\* Rules whose deletion is gated on a time horizon (as opposed to the HEAD gate).
HorizonGatedRules == {"retention", "dreq"}

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
    sysgc,            \* [ph, mqd, grace, skew]
    lastGc            \* witness of the last GC deletion step

storeVars == <<store, lastModified, versionCounter, uploads, listState>>
protoVars == <<head, headState, clock, superseded, heldBuckets,
               refreshFailed, query, erasureRequested, tombRetiredAt,
               dreqHorizon, doneAt, sysgc, lastGc>>
vars == <<store, lastModified, versionCounter, uploads, listState,
          head, headState, clock, superseded, heldBuckets,
          refreshFailed, query, erasureRequested, tombRetiredAt,
          dreqHorizon, doneAt, sysgc, lastGc>>

S == INSTANCE RavelObjectStore
       WITH Keys <- Objects, Content <- {"dat", "nc"}, NoContent <- "nc",
            Clients <- {"mnt"}

PresentObj(o) == store[o].present

\* State-space view: the invariants and every gate read object PRESENCE, never the
\* store's version, content, upload or listing bookkeeping. Projecting those away
\* collapses the states that differ only in the version integer a particular write
\* ordering assigned, which is the dominant source of otherwise-equivalent states.
StoreView == [o \in Objects |-> store[o].present]
View ==
    <<StoreView, head, headState, clock, superseded, heldBuckets, refreshFailed,
      query, erasureRequested, tombRetiredAt, dreqHorizon, doneAt, sysgc, lastGc>>

\* Effective HEAD for a GC gate read: absent clears (empty), present reads the
\* set, unreadable is handled by HeadReadable (the whole pass is blocked).
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
    /\ sysgc \in [ph: Nat, mqd: Nat, grace: Nat, skew: Nat]
    /\ lastGc.rule \in {"none","superseded","retention","dreq"}
    /\ lastGc.deleted \subseteq Objects
    /\ lastGc.held \in BOOLEAN
    /\ lastGc.refreshWasFailed \in BOOLEAN
    /\ lastGc.permittedNeeds \subseteq DataObjects
    /\ lastGc.headNamed \subseteq Objects

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
    /\ sysgc = [ph |-> IF GcConfigViolatesInequality THEN BadProtectionHorizon ELSE ProtectionHorizon,
                mqd |-> MaxQueryDuration, grace |-> Grace, skew |-> ClockSkew]
    /\ lastGc = [rule |-> "none", deleted |-> {}, atClock |-> 0,
                 held |-> FALSE, refreshWasFailed |-> FALSE,
                 permittedNeeds |-> {}, headNamed |-> {}]

\* A GC witness records what the deleting store operation OBSERVED at its own
\* step: the legal-hold state, the refresh state, the permitted-query needs, and
\* the HEAD-named subset it deleted. Invariants read this captured state (never
\* the live variable), so a hold or refresh flipped AFTER a legitimate delete
\* cannot retroactively make it look unsafe.
PermittedNeeds ==
    IF query.active /\ clock <= query.deadline THEN query.needs ELSE {}

GcWitness(r, dels) ==
    lastGc' = [rule |-> r, deleted |-> dels, atClock |-> clock,
               held |-> \E o \in dels : HeldObject(o, heldBuckets),
               refreshWasFailed |-> refreshFailed,
               permittedNeeds |-> PermittedNeeds,
               headNamed |-> dels \cap EffectiveHead]

NoGc == lastGc' = [rule |-> "none", deleted |-> {}, atClock |-> clock,
                   held |-> FALSE, refreshWasFailed |-> FALSE,
                   permittedNeeds |-> {}, headNamed |-> {}]

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
                   dreqHorizon, doneAt, sysgc>>
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
                   dreqHorizon, doneAt, sysgc>>
    /\ NoGc

ExpireQuery ==
    /\ query.active
    /\ clock > query.deadline
    /\ query' = [active |-> FALSE, needs |-> {}, deadline |-> 0]
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc>>
    /\ NoGc

\* Place / release a legal hold on bucket b (its data prefixes).
PlaceHold(b) ==
    /\ b \notin heldBuckets
    /\ heldBuckets' = heldBuckets \cup {b}
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc>>
    /\ NoGc

ReleaseHold(b) ==
    /\ b \in heldBuckets
    /\ heldBuckets' = heldBuckets \ {b}
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc>>
    /\ NoGc

\* The HEAD object read can fail (unreadable) or find the HEAD gone (absent).
SetHeadState(s) ==
    /\ FullEnv
    /\ s # headState
    /\ headState' = s
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc>>
    /\ NoGc

\* Toggle this tick's legal-hold refresh outcome.
SetRefresh(f) ==
    /\ FullEnv
    /\ f # refreshFailed
    /\ refreshFailed' = f
    /\ UNCHANGED storeVars
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc>>
    /\ NoGc

--------------------------------------------------------------------------------
\* Erasure / rewrite actor (maintainer)
--------------------------------------------------------------------------------

\* Write the .dreq for the erasure request (CreateIfAbsent, irreversible key).
\* crates/ravel-commit erasure request; the subject is marked erasure-requested
\* forever (it must never be served again, even through the cache).
RequestErasure ==
    /\ ~PresentObj("dreqR1")
    /\ S!PutCreateIfAbsent("dreqR1", "dat")
    /\ erasureRequested' = erasureRequested \cup {"s1"}
    /\ dreqHorizon' = clock + DreqHorizonDelta
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, tombRetiredAt, doneAt, sysgc>>
    /\ NoGc

\* Materialise the rewrite output rwA = inputs minus the erased subject and mark
\* the inputs superseded (resolve_rewrite_supersession). The HEAD is NOT switched
\* here; a later HeadAdvance drops the superseded inputs, so between the two the
\* inputs are still HEAD-named and the superseded sweep must hold them.
PerformRewrite ==
    /\ ~PresentObj("rwA")
    /\ S!PutOverwrite("rwA", "dat")
    /\ superseded' = superseded \cup RawInputs
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
                   dreqHorizon, doneAt, sysgc>>
    /\ NoGc

\* Complete the erasure: write .done only when the served set no longer serves the
\* subject (bucket_erasure_completion over bucket_serves_subject). completed is
\* the current, non-zero clock.
CompleteErasure ==
    /\ ~PresentObj("doneR1")
    /\ PresentObj("dreqR1")
    /\ headState = "present"   \* completion needs a real served-set read of HEAD
    /\ ~ServesNow("s1")
    /\ clock > 0
    /\ S!PutOverwrite("doneR1", "dat")
    /\ doneAt' = clock
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, sysgc>>
    /\ NoGc

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
                   refreshFailed, query, erasureRequested, dreqHorizon, doneAt, sysgc>>
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
                   dreqHorizon, doneAt, sysgc>>
    /\ NoGc

\* Retention physical sweep of one b1 data object. Gates on now >= retired_at +
\* protection_horizon (DeleteBeforeHorizon drops this) AND the current HEAD naming
\* nothing in the bucket, with absent HEAD clearing and unreadable HEAD blocking
\* the whole pass. A failed hold refresh skips the whole tick. A held object is
\* never swept. Base additionally respects an in-window pinned query
\* (HorizonGuardsPinnedQueries); candidate #1133 sets that FALSE.
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
                   dreqHorizon, doneAt, sysgc>>

--------------------------------------------------------------------------------
\* Physical GC actor (maintainer): superseded-input sweep and .dreq sweep
--------------------------------------------------------------------------------

\* Superseded-input sweep of one raw input. Object-granular HEAD gate
\* (reachability object_gate): an input the current HEAD still names is HELD, an
\* unreadable HEAD blocks the whole pass fail-closed, an absent HEAD clears the
\* gate. SupersededSweepUngated drops the head-membership check.
SupersededGatePasses(o) ==
    IF SupersededSweepUngated THEN TRUE ELSE o \notin EffectiveHead

SupersededSweep(o) ==
    /\ HeadReadable
    /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)
    /\ o \in superseded
    /\ PresentObj(o)
    /\ ~HeldObject(o, heldBuckets)
    /\ SupersededGatePasses(o)
    /\ S!Delete(o)
    /\ GcWitness("superseded", {o})
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc>>

\* .dreq sweep: delete the .dreq when a matching .done exists, its completed
\* timestamp is non-zero, the horizon has passed AND no permitted pinned query can
\* still reach a present object serving the subject (once the .dreq read-time filter
\* is gone, such a reader would serve the erased subject). DreqIgnoresHeldInputs
\* drops the last clause, so the .dreq can be swept out from under such a query.
NoPinnedReaderServes ==
    ~\E o \in DataObjects :
        /\ PresentObj(o) /\ ServesSubject(o, "s1")
        /\ query.active /\ clock <= query.deadline /\ o \in query.needs

DreqSweep ==
    /\ (RefreshFailureSweepsAnyway \/ ~refreshFailed)
    /\ HeadReadable
    /\ PresentObj("dreqR1")
    /\ PresentObj("doneR1")
    /\ doneAt > 0
    /\ clock >= dreqHorizon
    /\ (DreqIgnoresHeldInputs \/ NoPinnedReaderServes)
    /\ S!Delete("dreqR1")
    /\ GcWitness("dreq", {"dreqR1"})
    /\ UNCHANGED <<head, headState, clock, superseded, heldBuckets,
                   refreshFailed, query, erasureRequested, tombRetiredAt,
                   dreqHorizon, doneAt, sysgc>>

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
\* again, including through the cache (the read applies the erasure predicate
\* after fetch and after the cache).
ErasedSubjectNeverServedAfterRequest ==
    \A s \in erasureRequested : ~ServedRead(s)

\* A rewrite output serves exactly its inputs' subjects minus the erased ones.
RewriteOutputsAreInputsMinusErased ==
    PresentObj("rwA") =>
        \A s \in Subjects :
            ServesSubject("rwA", s) <=>
                ( (\E i \in Predecessors("rwA") : ServesSubject(i, s))
                  /\ s \notin ErasedBy(AppliedReqs("rwA")) )

\* Completion implies no pre-rewrite exposure: once .done exists, no present
\* head-named object still serves the erased subject.
CompletionImpliesNoPreRewriteExposure ==
    PresentObj("doneR1") => ~ServesNow("s1")

\* Removing the .dreq cannot resurrect the subject: if the .dreq is gone after a
\* request, no reader (current HEAD or a permitted pinned query) still serves it.
DreqRemovalCannotResurrect ==
    ("s1" \in erasureRequested /\ ~PresentObj("dreqR1")) => ~ServesAny("s1")

\* Two rewrites over the same input set with different applied requests get
\* different keys (the hash binds the sorted applied ids). Anchored to a
\* materialised rewrite output so TLC evaluates it as a state invariant rather than
\* a constant.
IdenticalInputSetsDoNotCollide ==
    PresentObj("rwA") =>
        Cardinality({RewriteKey(d) : d \in Materialized}) = Cardinality(Materialized)

\* The predecessor chain is representable: acyclic, inputs are real data objects,
\* bounded depth.
PredecessorChainRepresentable ==
    \A o \in Objects :
        /\ o \notin Predecessors(o)
        /\ Predecessors(o) \subseteq DataObjects
        /\ (\A p \in Predecessors(o) : Predecessors(p) = {})

\* An object the live HEAD still names is held, never deleted, by the superseded
\* sweep (the object-granular HEAD gate).
HeadNamedObjectNeverDeletedBySupersededSweep ==
    lastGc.rule = "superseded" => lastGc.headNamed = {}

\* The sys/gc record satisfies the startup inequality
\* (protection_horizon >= max_query_duration + grace + clock_skew_allowance).
GcConfigSatisfiesHorizon ==
    sysgc.ph >= sysgc.mqd + sysgc.grace + sysgc.skew

--------------------------------------------------------------------------------
\* Liveness (checked against FairSpec only; see README and #1131). Weak fairness
\* on exactly the actions the implementation justifies: the maintainer tick
\* (sweeps), the folder's HEAD advance, and store completion. A legal hold, a
\* stopped maintainer, or a fold/sweep retention-window disagreement make these
\* intentionally false, as README records.
FairSpec ==
    /\ Spec
    /\ WF_vars(\E o \in RawInputs : SupersededSweep(o))  \* maintainer sweep tick
    /\ WF_vars(HeadAdvanceRewrite)                       \* folder watermark advance
    /\ WF_vars(\E o \in DataObjects : RetentionSweep(o)) \* maintainer retention tick
    /\ WF_vars(CompleteErasure)                          \* store completion

\* Every superseded input that becomes deletable is eventually swept.
EventuallySwept ==
    (superseded # {} /\ RawInputs \cap head = {}) ~>
        (\A o \in RawInputs : ~PresentObj(o))

\* Every requested erasure is eventually completed.
EventuallyCompleted ==
    (PresentObj("dreqR1")) ~> (PresentObj("doneR1"))

===============================================================================
