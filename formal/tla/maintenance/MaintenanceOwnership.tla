------------------------- MODULE MaintenanceOwnership -------------------------
(*****************************************************************************)
(* The SHIPPED maintenance ownership protocol (ADR-0065 decisions 1 to 3,     *)
(* ADR-0048 ownership and coverage), model-checked over the shared object     *)
(* store contract (RavelObjectStore.tla, ADR-1113 D2).                        *)
(*                                                                           *)
(* --- Abstraction boundary --------------------------------------------------*)
(* MODELED:                                                                   *)
(*  * worker membership by self-owned heartbeat keys, written Overwrite and    *)
(*    stamped with the writer's clock (WorkerSet::write_heartbeat);            *)
(*  * the live set as self plus every sibling whose stamp is within a          *)
(*    bidirectional staleness window of the reader's clock, self always        *)
(*    included, fail-open to the previous set on a read error                  *)
(*    (WorkerSet::live_set, is_stale, DEFAULT_LIVENESS_FACTOR);                *)
(*  * rendezvous ownership as a deterministic argmax of an injected weight     *)
(*    table over the live set (owner, owns, owns_unit); the table STANDS IN    *)
(*    for blake3(unit_key || process_id), which the model does not compute;    *)
(*  * a discovery cycle that snapshots the live set once (cachedLive) and      *)
(*    then attempts every owned unit over an unbounded number of ticks         *)
(*    (run_discovery_cycle, run_tick_with_clock);                             *)
(*  * the per-worker maintain memo: Overwrite persistence, seed-from-all-      *)
(*    snapshots under a bidirectional staleness gate and a per-entry clamp,    *)
(*    corruption treated as absent (MaintainMemo, seed_from_snapshot,          *)
(*    write_memo_snapshot, read_all_memo_snapshots);                          *)
(*  * the compaction publication plane reduced to content-addressed parts      *)
(*    and one terminal record per unit, published CreateIfAbsent; the loser    *)
(*    reads the winner and converges or fails closed, and a divergent input    *)
(*    set alarms and deletes nothing (publish_record_with_conservation,        *)
(*    resolve_already_exists);                                                *)
(*  * the ungated CLI path: an actor that publishes any unit with no           *)
(*    heartbeat and no live set (services/ravel-cli/src/maintain.rs).          *)
(*                                                                           *)
(* ASSUMED (stated, not proved here):                                         *)
(*  * blake3 is a deterministic total order over (unit, worker); the weight    *)
(*    table is an injected stand-in and its exact values are not a contract;   *)
(*  * the segment/part encoder is content addressed: a part key determines     *)
(*    its bytes, so any writer of the same logical part writes identical       *)
(*    bytes (crates/ravel-maintain/src/build.rs::put_part);                    *)
(*  * the object store honours its own contract (RavelObjectStore.tla).        *)
(*                                                                           *)
(* OUT OF SCOPE:                                                              *)
(*  * UUID churn on restart: a fresh process id leaves the old heartbeat key    *)
(*    in place. The old key, being stale, is excluded from every live set, so  *)
(*    it changes no owner computation and adds no reachable compaction-plane    *)
(*    state (publication never reads worker identity). Its ONE consequence     *)
(*    that IS modeled -- a lingering key that is within the window and         *)
(*    OUTRANKS the live workers -- is the Phantom member switch in the MC       *)
(*    module, which produces the zero-ownership limitation directly;           *)
(*  * the RLOG k-way merge memory bound (ADR-0065 decision 4, ADR-0979),        *)
(*    the orphan breaker and conservation-count arithmetic (ADR-0048           *)
(*    decisions 4 and 5): the model treats a part as present or absent, not     *)
(*    as a counted multiset.                                                  *)
(*                                                                           *)
(* Time is a single bounded logical clock `now`; heartbeat and memo stamps      *)
(* are values of it. Clock skew between workers is not modeled here (the        *)
(* checked safety properties are view-independent and the checked liveness      *)
(* property assumes stable membership); the one adversarial view the           *)
(* protocol permits -- a phantom owner no live process embodies -- is the       *)
(* Phantom switch, not a skew parameter.                                       *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets, Integers

CONSTANTS
    Workers,        \* worker ids (naturals); rendezvous ranks over these
    Units,          \* compaction units (naturals)
    Variants,       \* input-set variants for a unit (a divergent listing sees a
                    \* different variant, hence a different input_set_hash)
    Canon,          \* the variant a background owner attempts (in Variants)
    NoC,            \* NoContent sentinel for the store instance
    NoRec,          \* firstRecord sentinel: no record has been published yet
    H,              \* heartbeat interval (DEFAULT_HEARTBEAT_INTERVAL)
    Factor,         \* liveness factor (DEFAULT_LIVENESS_FACTOR); window = Factor*H
    MaxT,           \* clock bound
    Phantom,        \* TRUE: inject a phantom live member that outranks every worker
    PH,             \* the phantom member id (distinct from every worker id)
    PhantomWeight,  \* the phantom's weight; must exceed every real weight
    AllowCrash      \* TRUE: workers may crash and revive (membership churn)

ASSUME Canon \in Variants
ASSUME PH \notin Workers
ASSUME H \in Nat /\ Factor \in Nat /\ MaxT \in Nat
ASSUME Phantom \in BOOLEAN /\ AllowCrash \in BOOLEAN

Window == Factor * H

\* Rendezvous weight: a deterministic total order over (unit, worker) standing
\* in for blake3(unit_key || process_id). Monotone in the worker id: this is a
\* valid deterministic assignment; the checked properties do not depend on the
\* table's shape, only on its determinism and totality.
RealWeight(u, w) == u * 100 + w
Weight(u, m) == IF m = PH THEN PhantomWeight ELSE RealWeight(u, m)

\* --- compaction plane keys and contents (drive the store instance) ----------
RecordKey(u)  == <<"rec", u>>
PartKey(u, v) == <<"part", u, v>>
OKeys    == {RecordKey(u) : u \in Units}
              \cup {PartKey(u, v) : u \in Units, v \in Variants}
OContent == {NoC} \cup {<<u, v>> : u \in Units, v \in Variants}

VARIABLES
    \* --- the object store instance (compaction plane) ---
    store, lastModified, versionCounter, uploads, listState,
    \* --- worker membership / scheduling ---
    now,             \* [Nat] the shared logical clock
    hbStamp,         \* [Workers -> Nat] last heartbeat stamp each worker wrote
    crashed,         \* [Workers -> BOOLEAN]
    cachedLive,      \* [Workers -> SUBSET Members] live set frozen per cycle
    \* --- durable incremental memo (self-owned snapshot) ---
    memoSnap,        \* [Workers -> [snapNs: Int, verU: Int]]  (-1 snapNs = corrupt)
    \* --- witnesses (not durable state) ---
    firstRecord,     \* [Units -> OContent \cup {NoRec}] content of the winning record
    attemptedByOwner,\* [Units -> BOOLEAN] some in-view owner attempted the unit
    cliCorrect,      \* BOOLEAN a non-owner (CLI) published a winning record
    lastMaint        \* the most recent heartbeat/memo/seed step (put mode, clamp)

sVars == <<store, lastModified, versionCounter, uploads, listState>>
vars == <<store, lastModified, versionCounter, uploads, listState,
          now, hbStamp, crashed, cachedLive, memoSnap,
          firstRecord, attemptedByOwner, cliCorrect, lastMaint>>

INSTANCE RavelObjectStore
    WITH Keys <- OKeys, Content <- OContent, NoContent <- NoC, Clients <- {}

Members == Workers \cup (IF Phantom THEN {PH} ELSE {})

Max(S) == CHOOSE m \in S : \A x \in S : x =< m

\* A sibling is stale (excluded) when its stamp is more than the window into
\* the reader's past OR its future (is_stale is bidirectional).
Stale(s) == \/ now - hbStamp[s] > Window
            \/ hbStamp[s] - now > Window

\* The live set: self always included, every fresh sibling, plus the phantom
\* when injected. Fail-open to the previous cachedLive on a read error is
\* modeled by the ABSENCE of a ComputeLive step (the worker keeps its frozen set).
LiveView(w) == { s \in Workers : s = w \/ ~Stale(s) }
                 \cup (IF Phantom THEN {PH} ELSE {})

\* Rendezvous owner in a view: the argmax of the weight table.
Owner(u, S) == CHOOSE m \in S : \A x \in S : Weight(u, x) =< Weight(u, m)
Owns(w, u)  == Owner(u, cachedLive[w]) = w

RecType == [present: BOOLEAN, content: OContent, version: Nat]

OTypeOK ==
    /\ StoreTypeOK
    /\ now \in 0..MaxT
    /\ hbStamp \in [Workers -> 0..MaxT]
    /\ crashed \in [Workers -> BOOLEAN]
    /\ cachedLive \in [Workers -> SUBSET Members]
    /\ memoSnap \in [Workers -> [snapNs: {-1} \cup (0..MaxT), verU: 0..MaxT]]
    /\ firstRecord \in [Units -> OContent \cup {NoRec}]
    /\ attemptedByOwner \in [Units -> BOOLEAN]
    /\ cliCorrect \in BOOLEAN
    /\ lastMaint \in [class: {"none", "heartbeat", "memo", "seed"},
                      mode: {"none", "Overwrite", "CasVersion"},
                      val: Int, bound: Int]

Init ==
    /\ StoreInit
    /\ now = 0
    /\ hbStamp = [w \in Workers |-> 0]
    /\ crashed = [w \in Workers |-> FALSE]
    /\ cachedLive = [w \in Workers |-> {w}]
    /\ memoSnap = [w \in Workers |-> [snapNs |-> 0, verU |-> 0]]
    /\ firstRecord = [u \in Units |-> NoRec]
    /\ attemptedByOwner = [u \in Units |-> FALSE]
    /\ cliCorrect = FALSE
    /\ lastMaint = [class |-> "none", mode |-> "none", val |-> 0, bound |-> 0]

\* --- Membership actions -----------------------------------------------------

\* WorkerSet::write_heartbeat: Overwrite of sys/maintain/workers/<id>, stamped
\* with the writer's own clock. Self-owned; never a CAS.
WriteHeartbeat(w) ==
    /\ ~crashed[w]
    /\ hbStamp' = [hbStamp EXCEPT ![w] = now]
    /\ lastMaint' = [class |-> "heartbeat", mode |-> "Overwrite",
                     val |-> now, bound |-> now]
    /\ UNCHANGED <<sVars, now, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect>>

Tick ==
    /\ now < MaxT
    /\ now' = now + 1
    /\ UNCHANGED <<sVars, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint>>

Crash(w) ==
    /\ AllowCrash
    /\ ~crashed[w]
    /\ crashed' = [crashed EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<sVars, now, hbStamp, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint>>

\* A restart takes a fresh incarnation heartbeat (the old key lingers, abstracted
\* per the header): re-heartbeat at the current clock.
Revive(w) ==
    /\ crashed[w]
    /\ crashed' = [crashed EXCEPT ![w] = FALSE]
    /\ hbStamp' = [hbStamp EXCEPT ![w] = now]
    /\ UNCHANGED <<sVars, now, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint>>

\* WorkerSet::live_set snapshotted once at the head of a discovery cycle
\* (run_discovery_cycle threads one live set through every unit).
ComputeLive(w) ==
    /\ ~crashed[w]
    /\ cachedLive' = [cachedLive EXCEPT ![w] = LiveView(w)]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint>>

\* --- Compaction publication -------------------------------------------------

\* A content-addressed part PUT (CreateIfAbsent): the key determines the bytes,
\* so a second PUT of the same logical part is AlreadyExists and identical
\* (crates/ravel-maintain/src/build.rs::put_part).
PutPart(u, v) ==
    /\ PutCreateIfAbsent(PartKey(u, v), <<u, v>>)
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint>>

\* The winner's terminal record PUT (CreateIfAbsent) and the loser's convergence
\* (publish_record_with_conservation, resolve_already_exists): the loser's
\* CreateIfAbsent is a no-op (AlreadyExists); it reads the winner and either
\* converges (same variant) or alarms on divergence, deleting nothing.
DoPublish(u, v) ==
    LET rk == RecordKey(u) IN
    /\ Present(PartKey(u, v))
    /\ PutCreateIfAbsent(rk, <<u, v>>)
    /\ firstRecord' = IF ~Present(rk)
                        THEN [firstRecord EXCEPT ![u] = <<u, v>>]
                        ELSE firstRecord

\* run_tick_with_clock: an in-view owner attempts its unit with the canonical
\* variant. Ownership gates which unit a worker attempts; it does NOT gate the
\* publish, which is CreateIfAbsent (ADR-0048: ownership is not publication
\* authority).
WorkerRecord(w, u) ==
    /\ ~crashed[w]
    /\ Owns(w, u)
    /\ DoPublish(u, Canon)
    /\ attemptedByOwner' = [attemptedByOwner EXCEPT ![u] = TRUE]
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   cliCorrect, lastMaint>>

\* The ungated CLI path (services/ravel-cli/src/maintain.rs): publishes any unit
\* and any variant with no heartbeat and no ownership. A CLI actor is always a
\* non-owner. cliCorrect latches whenever the CLI executes the publication path
\* (winning or converging on an existing winner); because
\* QueryVisibleDataCorrectUnderDuplicateOwnership is a conjoined invariant, the
\* witnessed state always has correct data, so the latch records "a non-owner
\* published and the data is still correct".
CliRecord(u, v) ==
    /\ DoPublish(u, v)
    /\ cliCorrect' = TRUE
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   attemptedByOwner, lastMaint>>

\* --- Maintain memo ----------------------------------------------------------

\* write_memo_snapshot: Overwrite of sys/maintain/memo/<id>. verU is an entry's
\* verified stamp; it MAY exceed the snapshot time (a future/skewed entry), which
\* the seed path clamps.
WriteMemo(w) ==
    /\ ~crashed[w]
    /\ memoSnap' = [memoSnap EXCEPT ![w] = [snapNs |-> now, verU |-> now]]
    /\ lastMaint' = [class |-> "memo", mode |-> "Overwrite",
                     val |-> now, bound |-> now]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive,
                   firstRecord, attemptedByOwner, cliCorrect>>

\* A future/skewed entry: the snapshot records an entry verified AFTER its own
\* snapshot_unix_ns (a clock ahead of the snapshot-writing clock). This is the
\* exact case the seed clamp defends against; the correct SeedMemo clamps it.
FutureEntry(w) ==
    /\ ~crashed[w]
    /\ now < MaxT
    /\ memoSnap' = [memoSnap EXCEPT ![w] = [snapNs |-> now, verU |-> now + 1]]
    /\ lastMaint' = [class |-> "memo", mode |-> "Overwrite",
                     val |-> now, bound |-> now]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive,
                   firstRecord, attemptedByOwner, cliCorrect>>

\* Corruption of a snapshot is treated as absent: snapNs = -1 removes it from the
\* seed set (MemoSnapshotError, corruption treated as absent).
CorruptMemo(w) ==
    /\ memoSnap' = [memoSnap EXCEPT ![w] = [snapNs |-> -1, verU |-> memoSnap[w].verU]]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint>>

\* Valid snapshots for w to seed from: not corrupt, and within the bidirectional
\* staleness gate of w's clock.
ValidSnaps(w) == { x \in Workers :
                     /\ memoSnap[x].snapNs # -1
                     /\ ~(now - memoSnap[x].snapNs > Window)
                     /\ ~(memoSnap[x].snapNs - now > Window) }

\* MaintainMemo::seed_from_snapshot: seed in-memory freshness from all valid
\* snapshots, clamping each entry to that snapshot's snapshot_unix_ns
\* (verified_ns = min(verified_ns, snapshot_unix_ns)). Modeled as a witness of
\* the clamped value against its bound (the largest source snapshot time).
SeedMemo(w) ==
    /\ ~crashed[w]
    /\ LET valid == ValidSnaps(w)
           clamped == { LET s == memoSnap[x]
                        IN IF s.verU < s.snapNs THEN s.verU ELSE s.snapNs
                        : x \in valid }
           value == IF clamped = {} THEN 0 ELSE Max(clamped)
           bnd == IF valid = {} THEN 0 ELSE Max({ memoSnap[x].snapNs : x \in valid })
       IN lastMaint' = [class |-> "seed", mode |-> "none", val |-> value, bound |-> bnd]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect>>

Next ==
    \/ \E w \in Workers : WriteHeartbeat(w)
    \/ Tick
    \/ \E w \in Workers : Crash(w)
    \/ \E w \in Workers : Revive(w)
    \/ \E w \in Workers : ComputeLive(w)
    \/ \E u \in Units, v \in Variants : PutPart(u, v)
    \/ \E w \in Workers, u \in Units : WorkerRecord(w, u)
    \/ \E u \in Units, v \in Variants : CliRecord(u, v)
    \/ \E w \in Workers : WriteMemo(w)
    \/ \E w \in Workers : FutureEntry(w)
    \/ \E w \in Workers : CorruptMemo(w)
    \/ \E w \in Workers : SeedMemo(w)

Spec == Init /\ [][Next]_vars

\* An illustrative terminal state (used to justify CHECK_DEADLOCK FALSE): the
\* clock is maxed, every unit is published with its parts, and every live worker
\* has heartbeated at the current clock. Nothing further changes observable state.
Terminal ==
    /\ now = MaxT
    /\ \A u \in Units : Present(RecordKey(u))
    /\ \A u \in Units, v \in Variants : Present(PartKey(u, v))
    /\ \A w \in Workers : crashed[w] \/ hbStamp[w] = now

\* --- Named safety invariants ------------------------------------------------

\* Whoever publishes a unit's record, query-visible data stays correct: the
\* record is immutable (equals the single CreateIfAbsent winner) and every part
\* key present carries exactly its content-addressed bytes. Duplicate publishers
\* (duplicate ownership, the CLI, a paused stale worker) cannot diverge it.
\* (ADR-0048; broken by the ownership-as-publication-authority Overwrite switch.)
QueryVisibleDataCorrectUnderDuplicateOwnership ==
    /\ \A u \in Units :
         Present(RecordKey(u)) => ContentOf(RecordKey(u)) = firstRecord[u]
    /\ \A u \in Units, v \in Variants :
         Present(PartKey(u, v)) => ContentOf(PartKey(u, v)) = <<u, v>>

\* Heartbeat and memo writes are Overwrite, never CAS (self-owned keys).
HeartbeatAndMemoNeverCas ==
    lastMaint.class \in {"heartbeat", "memo"} => lastMaint.mode = "Overwrite"

\* Seeding never lets an in-memory entry read fresher than the snapshot it came
\* from: the clamped value never exceeds its source snapshot's time.
MemoNeverExtendsFreshnessPastSnapshot ==
    lastMaint.class = "seed" => lastMaint.val =< lastMaint.bound

\* --- Fairness and liveness --------------------------------------------------
\* Weak fairness only on the actions the implementation justifies: a maintainer
\* that eventually recomputes its live set and ticks its owned units, a store
\* that eventually completes a part and a record PUT. No fairness over Next.

Fairness ==
    /\ \A w \in Workers : WF_vars(ComputeLive(w))
    /\ \A u \in Units, v \in Variants : WF_vars(PutPart(u, v))
    /\ \A u \in Units : WF_vars(\E w \in Workers : WorkerRecord(w, u))
    /\ \A u \in Units : WF_vars(\E v \in Variants : CliRecord(u, v))

FairSpec == Spec /\ Fairness

\* Under stable membership (no crash, no phantom) and the fairness above, every
\* unit is eventually attempted by its in-view owner. FALSE under a phantom owner
\* that no live process embodies -- the zero-ownership limitation (see README).
EveryEligibleUnitEventuallyAttempted ==
    \A u \in Units : <>(attemptedByOwner[u])

\* Reachability, encoded as an eventuality witness under fairness on the CLI
\* path: a non-owner (the CLI, which has no heartbeat and no live set) eventually
\* publishes a winning record while data stays correct. Ownership is therefore
\* not publication authority (ADR-0048, ADR-1029 decision 2).
OwnershipIsNotPublicationAuthority == <>(cliCorrect)

=============================================================================
