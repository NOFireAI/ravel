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
(*    included (WorkerSet::live_set, is_stale, DEFAULT_LIVENESS_FACTOR); the   *)
(*    fail-open on a read error is the CALLER's, not this helper: run_loop's   *)
(*    discovery arm reuses the last live set the heartbeat watch published     *)
(*    (services/ravel-server maintain.rs run_loop, live_rx.borrow().clone()),  *)
(*    so a failed refresh keeps the previous set; the scrub loop's own         *)
(*    read-fault fallback is out of scope (see README);                        *)
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
    MaintTok,       \* content marker for the self-owned heartbeat/memo keys
    NoRec,          \* firstRecord sentinel: no record has been published yet
    H,              \* heartbeat interval (DEFAULT_HEARTBEAT_INTERVAL)
    Factor,         \* liveness factor (DEFAULT_LIVENESS_FACTOR); window = Factor*H
    MaxT,           \* clock bound
    Phantom,        \* TRUE: inject a phantom live member that outranks every worker
    PH,             \* the phantom member id (distinct from every worker id)
    PhantomWeight,  \* the phantom's weight; must exceed every real weight
    AllowCrash      \* TRUE: workers may crash and revive (membership churn)

ASSUME Canon \in Variants
ASSUME MaintTok \notin ({NoC} \cup {<<u, v>> : u \in Units, v \in Variants})
ASSUME PH \notin Workers
ASSUME H \in Nat /\ Factor \in Nat /\ MaxT \in Nat
ASSUME Phantom \in BOOLEAN /\ AllowCrash \in BOOLEAN

Window == Factor * H

\* One representative worker performs the durable heartbeat/memo OBJECT writes.
\* The put-mode property (HeartbeatAndMemoNeverCas) is per-write and independent
\* of which worker issues it, so a single writer observes it faithfully; letting
\* every worker mint its own monotone store version multiplies the write-ordering
\* combinatorics against the compaction plane's versions for no added coverage.
\* Every worker still refreshes its membership STAMP logically (WriteHeartbeat).
PersistWorker == CHOOSE w \in Workers : \A x \in Workers : w =< x

\* Rendezvous weight: a deterministic total order over (unit, worker) standing
\* in for blake3(unit_key || process_id). Monotone in the worker id: this is a
\* valid deterministic assignment; the checked properties do not depend on the
\* table's shape, only on its determinism and totality.
RealWeight(u, w) == u * 100 + w
Weight(u, m) == IF m = PH THEN PhantomWeight ELSE RealWeight(u, m)

\* --- compaction plane keys and contents (drive the store instance) ----------
RecordKey(u)  == <<"rec", u>>
PartKey(u, v) == <<"part", u, v>>
\* Self-owned membership/memo keys, actually written to the store (F2): the
\* heartbeat key sys/maintain/workers/<id> and the memo key sys/maintain/memo/<id>.
HbKey(w)      == <<"hb", w>>
MemoKey(w)    == <<"memo", w>>
OKeys    == {RecordKey(u) : u \in Units}
              \cup {PartKey(u, v) : u \in Units, v \in Variants}
              \cup {HbKey(w) : w \in Workers}
              \cup {MemoKey(w) : w \in Workers}
\* A payload marker for the self-owned keys. Content is irrelevant to the checked
\* property: the store mints a fresh version on every Overwrite regardless of
\* content, so repeated self-owned writes advance the key's version and the
\* witness reads that version delta. MaintTok is a distinguished model value (like
\* NoC), which keeps these markers safely outside the <<unit, variant>> tuple
\* space; TLC compares model values against tuples without a type error.
HbContent(w)   == MaintTok
MemoContent(w) == MaintTok
OContent == {NoC, MaintTok}
              \cup {<<u, v>> : u \in Units, v \in Variants}

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
    lastMaint,       \* the most recent heartbeat/memo/seed step (put mode, clamp)
    partTomb,        \* [Units -> [Variants -> BOOLEAN]] a part key deleted and
                     \* not re-PUTtable (a tombstone/GC the rerun cannot recreate)
    lastPub          \* the outcome of the most recent publish resolution, with
                     \* store-observed witnesses (never a self-reported label)

sVars == <<store, lastModified, versionCounter, uploads, listState>>
vars == <<store, lastModified, versionCounter, uploads, listState,
          now, hbStamp, crashed, cachedLive, memoSnap,
          firstRecord, attemptedByOwner, cliCorrect, lastMaint,
          partTomb, lastPub>>

\* The publication-resolution outcome alphabet (ADR-1113 D3), mirroring
\* publish.rs::resolve_already_exists: the CreateIfAbsent winner is Published; a
\* later attempt that finds the record present converges on it (Converged), or
\* re-PUTs a transiently vanished winner part and converges, or fails closed when
\* the winner part vanished and is not re-PUTtable (ConvergedWinnerPartMissing),
\* or refuses to touch a record whose input set diverges from its own
\* (InputSetHashDivergence). Abandoned is carried for alphabet parity with the
\* claim model; this ownership model has no cancellation checkpoint, so it is not
\* reached here.
PubOutcomes == {"none", "Published", "Converged", "Abandoned",
                "ConvergedWinnerPartMissing", "InputSetHashDivergence"}

INSTANCE RavelObjectStore
    WITH Keys <- OKeys, Content <- OContent, NoContent <- NoC, Clients <- {}

Members == Workers \cup (IF Phantom THEN {PH} ELSE {})

Max(S) == CHOOSE m \in S : \A x \in S : x =< m

\* A sibling is stale (excluded) when its stamp is more than the window into
\* the reader's past OR its future (is_stale is bidirectional).
Stale(s) == \/ now - hbStamp[s] > Window
            \/ hbStamp[s] - now > Window

\* The live set: self always included, every fresh sibling, plus the phantom
\* when injected. The fail-open on a read error is the CALLER's, not this
\* computation: run_loop's discovery arm reuses the last set the heartbeat watch
\* channel published (live_rx.borrow().clone()), so a failed refresh keeps the
\* previous set. The model captures that as the ABSENCE of a ComputeLive step
\* (the worker keeps its frozen cachedLive).
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
                      verBefore: Nat, verAfter: Nat,
                      maxExcess: Int]
    /\ partTomb \in [Units -> [Variants -> BOOLEAN]]
    /\ lastPub \in [outcome: PubOutcomes,
                    winnerPartPresent: BOOLEAN, recOverwritten: BOOLEAN]

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
    /\ lastMaint = [class |-> "none", verBefore |-> 0, verAfter |-> 0, maxExcess |-> 0]
    /\ partTomb = [u \in Units |-> [v \in Variants |-> FALSE]]
    /\ lastPub = [outcome |-> "none", winnerPartPresent |-> FALSE,
                  recOverwritten |-> FALSE]

\* --- Membership actions -----------------------------------------------------

\* WorkerSet::write_heartbeat has two views the model separates so the store
\* version does not churn once per clock (the put MODE is frequency-invariant, so
\* re-writing the same mode adds property coverage of nothing and only multiplies
\* version orderings). WriteHeartbeat refreshes the advisory STAMP that live_set
\* reads for staleness (logical, per clock, no store write); PersistHeartbeat
\* below is the durable OBJECT write whose put mode the invariant observes.
WriteHeartbeat(w) ==
    /\ ~crashed[w]
    /\ hbStamp[w] < now
    /\ hbStamp' = [hbStamp EXCEPT ![w] = now]
    /\ UNCHANGED <<sVars, now, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

\* The durable heartbeat OBJECT write (Overwrite of sys/maintain/workers/<id>),
\* self-owned and never a CAS. Bounded to once per key: the put mode does not vary
\* with frequency, so a single write suffices to observe it. The witness records
\* the key's stored version before and after, so the invariant observes that the
\* write landed unconditionally (a fresh version) rather than a self-report.
PersistHeartbeat(w) ==
    /\ w = PersistWorker
    /\ ~crashed[w]
    /\ ~Present(HbKey(w))
    /\ PutOverwrite(HbKey(w), HbContent(w))
    /\ lastMaint' = [class |-> "heartbeat",
                     verBefore |-> VersionOf(HbKey(w)),
                     verAfter |-> store'[HbKey(w)].version,
                     maxExcess |-> 0]
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect,
                   partTomb, lastPub>>

Tick ==
    /\ now < MaxT
    /\ now' = now + 1
    /\ UNCHANGED <<sVars, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

Crash(w) ==
    /\ AllowCrash
    /\ ~crashed[w]
    /\ crashed' = [crashed EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<sVars, now, hbStamp, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

\* A restart takes a fresh incarnation heartbeat (the old key lingers, abstracted
\* per the header): re-heartbeat at the current clock.
Revive(w) ==
    /\ crashed[w]
    /\ crashed' = [crashed EXCEPT ![w] = FALSE]
    /\ hbStamp' = [hbStamp EXCEPT ![w] = now]
    /\ UNCHANGED <<sVars, now, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

\* WorkerSet::live_set snapshotted once at the head of a discovery cycle
\* (run_discovery_cycle threads one live set through every unit).
ComputeLive(w) ==
    /\ ~crashed[w]
    /\ cachedLive' = [cachedLive EXCEPT ![w] = LiveView(w)]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

\* --- Compaction publication -------------------------------------------------

\* A content-addressed part PUT (CreateIfAbsent): the key determines the bytes,
\* so a second PUT of the same logical part is AlreadyExists and identical
\* (crates/ravel-maintain/src/build.rs::put_part).
PutPart(u, v) ==
    /\ ~partTomb[u][v]
    /\ PutCreateIfAbsent(PartKey(u, v), <<u, v>>)
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

\* The winner's terminal record PUT (CreateIfAbsent) and the loser's convergence
\* over the shared object store, resolving exactly as
\* publish_record_with_conservation and resolve_already_exists do. The outcome
\* witness (lastPub) reads the store around the resolution (winnerPartPresent from
\* store', recOverwritten from the record version delta), never a self-reported
\* label:
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
                   cliCorrect, lastMaint, partTomb>>

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
                   attemptedByOwner, lastMaint, partTomb>>

\* --- Maintain memo ----------------------------------------------------------

\* The memo snapshot's freshness DATA, refreshed logically (per clock, no store
\* write) exactly as WriteHeartbeat refreshes the heartbeat stamp. verU is an
\* entry's verified stamp; it MAY exceed the snapshot time (a future/skewed
\* entry), which the seed path clamps. PersistMemo below is the durable OBJECT
\* write whose put mode the invariant observes.
WriteMemo(w) ==
    /\ ~crashed[w]
    /\ memoSnap[w].snapNs < now
    /\ memoSnap' = [memoSnap EXCEPT ![w] = [snapNs |-> now, verU |-> now]]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

\* A future/skewed entry: the snapshot records an entry verified AFTER its own
\* snapshot_unix_ns (a clock ahead of the snapshot-writing clock). This is the
\* exact case the seed clamp defends against; the correct SeedMemo clamps it.
FutureEntry(w) ==
    /\ ~crashed[w]
    /\ now < MaxT
    /\ memoSnap[w].snapNs < now
    /\ memoSnap' = [memoSnap EXCEPT ![w] = [snapNs |-> now, verU |-> now + 1]]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

\* write_memo_snapshot: the durable memo OBJECT write (Overwrite of
\* sys/maintain/memo/<id>), self-owned and never a CAS. Bounded to once per key
\* for the same reason as PersistHeartbeat: the put mode is frequency-invariant.
\* The witness reads the stored version before and after.
PersistMemo(w) ==
    /\ w = PersistWorker
    /\ ~crashed[w]
    /\ ~Present(MemoKey(w))
    /\ PutOverwrite(MemoKey(w), MemoContent(w))
    /\ lastMaint' = [class |-> "memo",
                     verBefore |-> VersionOf(MemoKey(w)),
                     verAfter |-> store'[MemoKey(w)].version,
                     maxExcess |-> 0]
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect,
                   partTomb, lastPub>>

\* Corruption of a snapshot is treated as absent: snapNs = -1 removes it from the
\* seed set (MemoSnapshotError, corruption treated as absent).
CorruptMemo(w) ==
    /\ memoSnap' = [memoSnap EXCEPT ![w] = [snapNs |-> -1, verU |-> memoSnap[w].verU]]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

\* Valid snapshots for w to seed from: not corrupt, and within the bidirectional
\* staleness gate of w's clock.
ValidSnaps(w) == { x \in Workers :
                     /\ memoSnap[x].snapNs # -1
                     /\ ~(now - memoSnap[x].snapNs > Window)
                     /\ ~(memoSnap[x].snapNs - now > Window) }

\* MaintainMemo::seed_from_snapshot: seed in-memory freshness from all valid
\* snapshots, clamping each entry to that snapshot's snapshot_unix_ns
\* (verified_ns = min(verified_ns, snapshot_unix_ns)). The witness is per entry:
\* maxExcess is the largest amount by which any seeded entry's clamped value
\* exceeds its OWN source snapshot time. The correct clamp never lets an entry
\* read past its own snapshot, so maxExcess stays <= 0 regardless of any other
\* entry with a larger snapshot.
SeedMemo(w) ==
    /\ ~crashed[w]
    /\ LET valid == ValidSnaps(w)
           excess == { LET s  == memoSnap[x]
                           ev == IF s.verU < s.snapNs THEN s.verU ELSE s.snapNs
                       IN ev - s.snapNs
                       : x \in valid }
           mx == IF excess = {} THEN 0 ELSE Max(excess)
       IN lastMaint' = [class |-> "seed", verBefore |-> 0, verAfter |-> 0,
                        maxExcess |-> mx]
    /\ UNCHANGED <<sVars, now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect,
                   partTomb, lastPub>>

\* A published winner part transiently disappears (a tombstone race, GC, or a
\* delayed listing), and is re-PUTtable: a later convergence re-creates the
\* identical content-addressed bytes. Models tombstone_race.rs's revanish path up
\* to the point the part can still be recreated.
VanishPart(u) ==
    /\ firstRecord[u] # NoRec
    /\ Present(PartKey(u, firstRecord[u][2]))
    /\ ~partTomb[u][firstRecord[u][2]]
    /\ Delete(PartKey(u, firstRecord[u][2]))
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint,
                   partTomb, lastPub>>

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
    /\ UNCHANGED <<now, hbStamp, crashed, cachedLive, memoSnap,
                   firstRecord, attemptedByOwner, cliCorrect, lastMaint, lastPub>>

Next ==
    \/ \E w \in Workers : WriteHeartbeat(w)
    \/ \E w \in Workers : PersistHeartbeat(w)
    \/ Tick
    \/ \E w \in Workers : Crash(w)
    \/ \E w \in Workers : Revive(w)
    \/ \E w \in Workers : ComputeLive(w)
    \/ \E u \in Units, v \in Variants : PutPart(u, v)
    \/ \E w \in Workers, u \in Units : WorkerRecord(w, u)
    \/ \E u \in Units, v \in Variants : CliRecord(u, v)
    \/ \E w \in Workers : WriteMemo(w)
    \/ \E w \in Workers : FutureEntry(w)
    \/ \E w \in Workers : PersistMemo(w)
    \/ \E w \in Workers : CorruptMemo(w)
    \/ \E w \in Workers : SeedMemo(w)
    \/ \E u \in Units : VanishPart(u)
    \/ \E u \in Units : TombstonePart(u)

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

\* Heartbeat and memo writes are Overwrite, never CAS (self-owned keys). Observed
\* through the store: an Overwrite always mints a fresh version, so the written
\* key's stored version strictly advances. A CAS gated on a version token can
\* fail its precondition and leave the key unchanged, which this catches: for the
\* last heartbeat/memo write, the stored version after exceeds the version before.
\* (Broken by the heartbeat-memo-cas control, which performs a real CAS.)
HeartbeatAndMemoNeverCas ==
    lastMaint.class \in {"heartbeat", "memo"} =>
        lastMaint.verAfter > lastMaint.verBefore

\* Seeding never lets an in-memory entry read fresher than the snapshot it came
\* from: for EVERY seeded entry, its clamped value never exceeds its own source
\* snapshot's time. Stated per entry, not as an aggregate Max over all snapshots:
\* an aggregate bound would pass whenever some other entry has a larger snapshot,
\* which is the gap the review found. maxExcess is the worst per-entry overshoot.
\* (Broken by the memo-overstamp control, which skips the per-entry clamp.)
MemoNeverExtendsFreshnessPastSnapshot ==
    lastMaint.class = "seed" => lastMaint.maxExcess =< 0

\* Merge attempts converge or fail closed (ADR-1113 D3), stated for a world where
\* a part can vanish. A resolution reports Converged only when the winner part is
\* actually present, and reports ConvergedWinnerPartMissing exactly when the winner
\* part has vanished and is not re-PUTtable (tombstoned) -- it never silently
\* claims convergence over a missing part. winnerPartPresent is read from the store
\* at the moment of resolution (store' in DoPublish), never a self-reported label,
\* so a mutant that labels a missing-part resolution "Converged" is caught. A
\* transiently vanished, re-PUTtable part is not a violation: the resolution re-PUTs
\* identical content-addressed bytes and reports Converged with the part present
\* again. See README (F6/F7). (Broken by the missing-part-reports-converged switch.)
MergeAttemptsConverge ==
    /\ (lastPub.outcome = "Converged" => lastPub.winnerPartPresent)
    /\ (lastPub.outcome = "ConvergedWinnerPartMissing" => ~lastPub.winnerPartPresent)

\* A divergent input set alarms and mutates nothing: the loser neither overwrites
\* the terminal record nor deletes any part. recOverwritten is the store-observed
\* record version delta across the resolution (FALSE means the record version did
\* not move), so a mutant that overwrites the record on divergence -- even to
\* identical content, which still mints a new version -- is caught. The divergent
\* branch of DoPublish is UNCHANGED store, so no part is deleted either. (Broken by
\* the diverge-overwrites-record switch.)
DivergentInputSetNeverMutates ==
    lastPub.outcome = "InputSetHashDivergence" => ~lastPub.recOverwritten

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
