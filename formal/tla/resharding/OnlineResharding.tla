---------------------------- MODULE OnlineResharding ----------------------------
(***************************************************************************)
(* Generation-versioned online resharding (ADR-0052 and its in-file        *)
(* amendment, ADR-1113 task T5).                                          *)
(*                                                                        *)
(* Abstraction boundary. Inside the model: the provisioning record as an   *)
(* append-only generation history held in one object-store key, the        *)
(* compare-and-swap append that extends it, per-writer cached views of     *)
(* that history with a refresh interval and a bounded degraded-grace       *)
(* window, wall-clock routing of an admitted record to a shard index, the  *)
(* ingest-hour pin taken when a flush opens, the read-side scan set, the   *)
(* folder's HEAD ceiling stamp with the reader's validation fence, and     *)
(* commit-token resolution. Clocks are per actor, in whole hours, and      *)
(* differ only within stated bounds.                                       *)
(*                                                                        *)
(* Outside the model: series identity and hashing (a record routes to any  *)
(* index the divisor admits), object payloads, segment and manifest        *)
(* formats, the commit protocol itself, retention, compaction, and         *)
(* everything about a signal other than that all state here is one         *)
(* tenant's one signal.                                                    *)
(*                                                                        *)
(* The store is not re-modeled here: the object-store contract comes from  *)
(* common/RavelObjectStore.tla, instantiated below, so put modes, version  *)
(* semantics, and lost responses are the shared ones.                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Writers,                 \* router-side writers, each with a cached view
    Requesters,              \* concurrent reshard requests at the operator
    TargetCounts,            \* shard counts a request may ask for
    InitialShardCount,       \* generation 0's shard count
    MaxHour,                 \* model horizon, in model time-units
    MaxGenerations,          \* appends allowed past generation 0
    MaxAdmitsPerWriter,      \* admitted records per writer (model bound)
    CasAttempts,             \* RESHARD_CAS_ATTEMPTS
    C,                       \* refresh interval, in model time-units
    MinLeadHours,            \* min_lead_hours(C) = HourCeil(C) + HourUnits
    L,                       \* activation lead the caller asks for
    S,                       \* read-side scan slack, in model time-units
    FlushBound,              \* max_flush_delay_idle + max_flush_lifetime, in model time-units
    WriterSkew,              \* bound on router-to-router clock difference
    AppenderSkew,            \* bound on appender-to-router clock difference
    WriterFenceEnabled,      \* negative switch: fail closed on a view past grace
    TokenValidatedAgainstCount, \* negative switch: validate token.shard vs a count
    HourUnits                \* model time-units per wall-clock hour (see below)

\* HourUnits lets a config choose how finely a model time-unit divides the
\* wall-clock hour that min_lead_hours(C) rounds up to. Every cfg shipped
\* before this constant existed models a time-unit of one hour and sets
\* HourUnits = 1, under which HourCeil(C) = C and the formula below collapses
\* to the original MinLeadHours = C + 1. A cfg that instead models minutes
\* sets HourUnits = 60 and C = 1, representing the real 60 s refresh interval
\* exactly instead of rounding it up to a whole hour first; see README.md's
\* "Time-unit granularity" section for why that rounding otherwise inflates
\* cached-view staleness by the same factor it discards.
HourCeil(c) == ((c + HourUnits - 1) \div HourUnits) * HourUnits

ASSUME Writers # {}
ASSUME Requesters # {}
ASSUME TargetCounts # {} /\ \A c \in TargetCounts : c >= 1
ASSUME InitialShardCount >= 1
ASSUME HourUnits >= 1
ASSUME MinLeadHours = HourCeil(C) + HourUnits
ASSUME L >= 1 /\ S >= 0 /\ FlushBound >= 0
ASSUME WriterSkew >= 0 /\ AppenderSkew >= 0
ASSUME MaxHour >= 1 /\ MaxGenerations >= 1 /\ MaxAdmitsPerWriter >= 1
ASSUME CasAttempts >= 1
ASSUME WriterFenceEnabled \in BOOLEAN
ASSUME TokenValidatedAgainstCount \in BOOLEAN

Appender == "appender"       \* the reshard operator or CLI that appends generations
Reader   == "reader"         \* the query side; the folder shares this clock

ASSUME Appender \notin Writers /\ Reader \notin Writers

RouterActors == Writers \cup {Reader}
Actors == RouterActors \cup {Appender}
Hours == 0..MaxHour
AdmitIds == 1..MaxAdmitsPerWriter

ShardCounts == {InitialShardCount} \cup TargetCounts
MaxOf(Sset) == CHOOSE x \in Sset : \A y \in Sset : y <= x
MaxIdx(Sset) == CHOOSE i \in Sset : \A j \in Sset : j <= i
MaxShards == MaxOf(ShardCounts)
Shards == 0..(MaxShards - 1)

\* --- Object keys and the values that live under them ------------------------

\* Keys are tuples so that every key in the key set has the same value
\* shape: a per-writer commit key carries the writer, and TLC cannot
\* compare a plain string with a tuple.
ProvKey == <<"provisioning">>
HeadKey == <<"head">>
TokenKey(w, i) == <<"commit", w, i>>
TokenKeys == { TokenKey(w, i) : w \in Writers, i \in AdmitIds }
AllKeys == {ProvKey, HeadKey} \cup TokenKeys
\* The absent-content value is a record, not a string: stored contents are
\* histories (sequences) and head and commit records, and TLC cannot compare
\* a string with either.
Nil == [kind |-> "absent"]

GenRecs == [gen: 0..MaxGenerations, count: ShardCounts, act: Hours]
Histories == UNION { [1..n -> GenRecs] : n \in 1..(MaxGenerations + 1) }
HeadRecs == [ceiling: ShardCounts, genCount: 1..(MaxGenerations + 1), watermark: Hours]
WriteRecs == [writer: Writers, shard: Shards, ingestHour: Hours,
              routeHour: Hours, id: AdmitIds]

Gen0 == [gen |-> 0, count |-> InitialShardCount, act |-> 0]
InitialHistory == <<Gen0>>

VARIABLES
    store, lastModified, versionCounter, uploads, listState,  \* the shared store
    clocks,      \* [Actors -> Hours], per-actor wall clock in hours
    views,       \* [Writers -> cached provisioning view] (generations, refreshed_at)
    flushes,     \* [Writers -> open flush: its pinned ingest hour and admit counter]
    admitted,    \* set of admitted records: (shard index, ingest hour, token)
    reqs,        \* [Requesters -> reshard request progress]
    rview,       \* the reader's cached generation view
    lastOp,      \* witness for the caller-visible outcome of the last step
    casWins      \* set of successful appends as <<base version, requester>>

storeStateVars == <<store, lastModified, versionCounter, uploads, listState>>
localVars == <<clocks, views, flushes, admitted, reqs, rview, casWins>>
vars == <<store, lastModified, versionCounter, uploads, listState,
          clocks, views, flushes, admitted, reqs, rview, lastOp, casWins>>

\* The store is instantiated with the absent value as its whole content set.
\* One key here holds a sequence and the others hold records of different
\* shapes, and TLC cannot decide membership in a set that mixes the two, so
\* StoreTypeOK's blanket `content \in Content` is replaced by StoreShapeOK
\* below, which types each key's content against its own uniform set. Nothing
\* in this model uploads real content through the multipart path, so the
\* placeholder in `uploads` stays the absent value.
INSTANCE RavelObjectStore
    WITH Keys <- AllKeys, Content <- {Nil}, NoContent <- Nil, Clients <- Writers

\* --- Pure functions mirrored from crates/ravel-catalog/src/provisioning.rs --

\* active_shard_count: the count of the latest generation activated by `h`.
ActiveCount(g, h) ==
    LET A == { i \in 1..Len(g) : g[i].act <= h }
    IN IF A = {} THEN g[1].count ELSE g[MaxIdx(A)].count

\* scan_count: activated generations, each held for `slack` hours past its
\* successor's activation.
Contributes(g, i, h, slack) ==
    /\ g[i].act <= h
    /\ (i = Len(g) \/ h < g[i + 1].act + slack)

ScanCount(g, h, slack) ==
    LET Cs == { g[i].count : i \in { i \in 1..Len(g) : Contributes(g, i, h, slack) } }
    IN IF Cs = {} THEN g[1].count ELSE MaxOf(Cs)

\* max_scan_count_over_range: one shard bound covering every hour in [f, t].
RangeScanCount(g, f, t, slack) ==
    LET Cs == { g[i].count :
                  i \in { i \in 1..Len(g) :
                            /\ g[i].act <= t
                            /\ (i = Len(g) \/ f < g[i + 1].act + slack) } }
    IN IF Cs = {} THEN g[1].count ELSE MaxOf(Cs)

\* shard_ceiling: monotone max count over generations activated by `wm`.
ShardCeiling(g, wm) ==
    LET Cs == { g[i].count : i \in { i \in 1..Len(g) : g[i].act <= wm } }
    IN IF Cs = {} THEN g[1].count ELSE MaxOf(Cs)

\* head_generations_acceptable: ceiling match, or a safely-old HEAD whose
\* watermark predates the first generation it does not know.
Acceptable(hd, g) ==
    IF hd.ceiling = ShardCeiling(g, hd.watermark)
        THEN TRUE
        ELSE IF hd.genCount >= Len(g)
            THEN FALSE
            ELSE LET fu == (IF hd.genCount > 1 THEN hd.genCount ELSE 1) + 1
                 IN IF fu <= Len(g) THEN hd.watermark < g[fu].act ELSE FALSE

IsPrefixOf(a, b) == Len(a) <= Len(b) /\ SubSeq(b, 1, Len(a)) = a

\* --- Model helpers ----------------------------------------------------------

Gens == ContentOf(ProvKey)          \* the durable history, read through the store
HeadOf == ContentOf(HeadKey)
Fresh(w) == views[w].has /\ clocks[w] - views[w].at <= C
GraceOpen(w) == views[w].has /\ clocks[w] < views[w].at + MinLeadHours
AdmitsLeft(w) == MaxAdmitsPerWriter - flushes[w].next + 1

NoOp == [kind |-> "none", actor |-> Nil, outcome |-> "none", hour |-> 0,
         count |-> 0, viewAt |-> 0, before |-> InitialHistory,
         after |-> InitialHistory, underScan |-> FALSE, headOld |-> FALSE]

SkewOK(cl) ==
    /\ \A a, b \in RouterActors : cl[a] <= cl[b] + WriterSkew
    /\ \A a \in RouterActors : /\ cl[Appender] <= cl[a] + AppenderSkew
                               /\ cl[a] <= cl[Appender] + AppenderSkew

\* StoreTypeOK, with the content of each key typed against its own set.
StoreShapeOK ==
    /\ DOMAIN store = AllKeys
    /\ \A k \in AllKeys : store[k].present \in BOOLEAN /\ store[k].version \in Nat
    /\ lastModified \in [AllKeys -> Nat]
    /\ versionCounter \in Nat
    /\ uploads \in [Writers -> [active: BOOLEAN, key: AllKeys, content: {Nil}]]
    /\ listState.active \in BOOLEAN
    /\ listState.snapshot \subseteq AllKeys
    /\ listState.delivered \in [AllKeys -> Nat]
    /\ (Present(ProvKey) => ContentOf(ProvKey) \in Histories)
    /\ (Present(HeadKey) => ContentOf(HeadKey) \in HeadRecs)
    /\ \A k \in TokenKeys : Present(k) => ContentOf(k) \in WriteRecs

TypeOK ==
    /\ StoreShapeOK
    /\ clocks \in [Actors -> Hours]
    /\ SkewOK(clocks)
    /\ views \in [Writers -> [has: BOOLEAN, gens: Histories, at: Hours]]
    /\ flushes \in [Writers -> [open: BOOLEAN, hour: Hours,
                                next: 1..(MaxAdmitsPerWriter + 1)]]
    /\ admitted \subseteq WriteRecs
    /\ reqs \in [Requesters -> [phase: {"idle", "read", "done"},
                                base: Nat, gens: Histories,
                                target: TargetCounts, tries: 0..CasAttempts]]
    /\ rview \in [has: BOOLEAN, gens: Histories, at: Hours]
    /\ lastOp.kind \in {"none", "bootstrap", "tick", "reqread", "append",
                        "admit", "flush", "crash", "fold", "read", "resolve"}
    /\ lastOp.before \in Histories /\ lastOp.after \in Histories
    /\ lastOp.underScan \in BOOLEAN /\ lastOp.headOld \in BOOLEAN
    /\ casWins \subseteq [base: Nat, req: Requesters]

\* StoreInit, with the unused multipart placeholder key pinned to a
\* writer-independent key. StoreInit picks it with CHOOSE over the whole key
\* set, and this key set contains per-writer commit keys, so the CHOOSE would
\* leave a writer name in the initial state and a writer permutation would no
\* longer be a state symmetry.
Init ==
    /\ store = [k \in AllKeys |-> EmptyRec]
    /\ lastModified = [k \in AllKeys |-> 0]
    /\ versionCounter = 0
    /\ uploads = [u \in Writers |-> [active |-> FALSE, key |-> ProvKey,
                                     content |-> Nil]]
    /\ listState = [active |-> FALSE, snapshot |-> {},
                    delivered |-> [k \in AllKeys |-> 0]]
    /\ clocks = [a \in Actors |-> 0]
    /\ views = [w \in Writers |-> [has |-> FALSE, gens |-> InitialHistory, at |-> 0]]
    /\ flushes = [w \in Writers |-> [open |-> FALSE, hour |-> 0, next |-> 1]]
    /\ admitted = {}
    /\ reqs = [r \in Requesters |-> [phase |-> "idle", base |-> 0,
                                     gens |-> InitialHistory,
                                     target |-> MaxOf(TargetCounts),
                                     tries |-> 0]]
    /\ rview = [has |-> FALSE, gens |-> InitialHistory, at |-> 0]
    /\ lastOp = NoOp
    /\ casWins = {}

\* Provisioning creation (services/ravel-cli provision) publishes generation 0
\* under PutMode::CreateIfAbsent. Every other action requires the record.
Bootstrap ==
    /\ ~Present(ProvKey)
    /\ PutCreateIfAbsent(ProvKey, InitialHistory)
    /\ lastOp' = [NoOp EXCEPT !.kind = "bootstrap", !.after = InitialHistory]
    /\ UNCHANGED localVars

\* --- Clocks -----------------------------------------------------------------

\* Router clocks advance together; any single actor may drift within its bound.
\* No action here refreshes a cached view: refresh is lazy at write
\* (ravel-ingest has no periodic refresher), so time passing is what makes a
\* view stale, and only an admission reads the record again.
TickRouters ==
    /\ \A a \in RouterActors : clocks[a] < MaxHour
    /\ clocks' = [a \in Actors |-> IF a \in RouterActors THEN clocks[a] + 1
                                                         ELSE clocks[a]]
    /\ SkewOK(clocks')
    /\ lastOp' = [NoOp EXCEPT !.kind = "tick"]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads, listState,
                   views, flushes, admitted, reqs, rview, casWins>>

TickActor(a) ==
    /\ clocks[a] < MaxHour
    /\ clocks' = [clocks EXCEPT ![a] = @ + 1]
    /\ SkewOK(clocks')
    /\ lastOp' = [NoOp EXCEPT !.kind = "tick", !.actor = a]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads, listState,
                   views, flushes, admitted, reqs, rview, casWins>>

\* --- Reshard operator (crates/ravel-catalog/src/provisioning.rs) ------------

\* append_generation's read half: GET the record, keep its version for the CAS.
ReqRead(r) ==
    /\ Present(ProvKey)
    /\ reqs[r].phase = "idle"
    /\ reqs[r].tries < CasAttempts
    /\ \E tc \in TargetCounts :
          reqs' = [reqs EXCEPT ![r] = [phase |-> "read",
                                       base |-> VersionOf(ProvKey),
                                       gens |-> Gens, target |-> tc,
                                       tries |-> reqs[r].tries]]
    /\ lastOp' = [NoOp EXCEPT !.kind = "reqread", !.actor = r,
                              !.before = Gens, !.after = Gens]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads, listState,
                   clocks, views, flushes, admitted, rview, casWins>>

Proposed(r) ==
    LET g == reqs[r].gens
        last == g[Len(g)]
    IN [gen |-> last.gen + 1, count |-> reqs[r].target,
        act |-> clocks[Appender] + L]

\* append_generation's validation chain. `activation_hour = now_hour + L` is
\* the caller's arithmetic (services/ravel-cli, services/ravel-operator); the
\* append itself only rejects an activation at or before the last generation's
\* or at or before the appender's own hour (ActivationInPast).
ReqReject(r) ==
    /\ reqs[r].phase = "read"
    /\ LET g == reqs[r].gens
           last == g[Len(g)]
           new == Proposed(r)
       IN \/ new.count = last.count                 \* ReshardSameCount
          \/ new.act <= last.act                    \* ActivationInPast
          \/ new.act <= clocks[Appender]            \* ActivationInPast
          \/ new.act > MaxHour                      \* outside the model horizon
          \/ Len(g) > MaxGenerations                \* model bound on appends
    /\ reqs' = [reqs EXCEPT ![r].phase = "done"]
    /\ lastOp' = [NoOp EXCEPT !.kind = "append", !.actor = r,
                              !.outcome = "rejected"]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads, listState,
                   clocks, views, flushes, admitted, rview, casWins>>

CasAppendable(r) ==
    /\ reqs[r].phase = "read"
    /\ LET g == reqs[r].gens
           last == g[Len(g)]
           new == Proposed(r)
       IN /\ new.count # last.count
          /\ new.act > last.act
          /\ new.act > clocks[Appender]
          /\ new.act <= MaxHour
          /\ Len(g) <= MaxGenerations

\* The winning CAS: PutMode::CasVersion against the version the read returned.
\* A successful append only ever extends the history (never rewrites it).
ReqAppendOk(r) ==
    /\ CasAppendable(r)
    /\ VersionOf(ProvKey) = reqs[r].base
    /\ PutCasVersion(ProvKey, reqs[r].base, Append(reqs[r].gens, Proposed(r)))
    /\ reqs' = [reqs EXCEPT ![r].phase = "done"]
    /\ casWins' = casWins \cup {[base |-> reqs[r].base, req |-> r]}
    /\ lastOp' = [NoOp EXCEPT !.kind = "append", !.actor = r, !.outcome = "ok",
                              !.before = reqs[r].gens,
                              !.after = Append(reqs[r].gens, Proposed(r))]
    /\ UNCHANGED <<clocks, views, flushes, admitted, rview>>

\* Same effect, response lost (PutCasVersionLostResponse): the caller sees a
\* failure and retries, so its retry must find its own generation already there.
ReqAppendLost(r) ==
    /\ CasAppendable(r)
    /\ VersionOf(ProvKey) = reqs[r].base
    /\ PutCasVersionLostResponse(ProvKey, reqs[r].base,
                                 Append(reqs[r].gens, Proposed(r)))
    /\ reqs' = [reqs EXCEPT ![r].phase = IF reqs[r].tries + 1 < CasAttempts
                                             THEN "idle" ELSE "done",
                            ![r].tries = reqs[r].tries + 1]
    /\ casWins' = casWins \cup {[base |-> reqs[r].base, req |-> r]}
    /\ lastOp' = [NoOp EXCEPT !.kind = "append", !.actor = r, !.outcome = "lost",
                              !.before = reqs[r].gens,
                              !.after = Append(reqs[r].gens, Proposed(r))]
    /\ UNCHANGED <<clocks, views, flushes, admitted, rview>>

\* ReshardCasConflict: the loser re-reads and retries, or gives up after
\* RESHARD_CAS_ATTEMPTS. The store put is a no-op, so nothing is discarded.
ReqAppendConflict(r) ==
    /\ CasAppendable(r)
    /\ VersionOf(ProvKey) # reqs[r].base
    /\ PutCasVersion(ProvKey, reqs[r].base, Append(reqs[r].gens, Proposed(r)))
    /\ reqs' = [reqs EXCEPT ![r].phase = IF reqs[r].tries + 1 < CasAttempts
                                             THEN "idle" ELSE "done",
                            ![r].tries = reqs[r].tries + 1]
    /\ lastOp' = [NoOp EXCEPT !.kind = "append", !.actor = r,
                              !.outcome = "cas-conflict",
                              !.before = Gens, !.after = Gens]
    /\ UNCHANGED <<clocks, views, flushes, admitted, rview, casWins>>

\* --- Writers (crates/ravel-ingest/src/generation.rs, router.rs) -------------

\* checked_ingest_hour_bucket: the ingest hour is pinned when the flush opens,
\* and admission may trail that pin by up to FlushBound hours.
FlushOpen(w) ==
    /\ ~flushes[w].open
    /\ flushes' = [flushes EXCEPT ![w] = [open |-> TRUE, hour |-> clocks[w],
                                          next |-> flushes[w].next]]
    /\ lastOp' = [NoOp EXCEPT !.kind = "flush", !.actor = w, !.outcome = "open",
                              !.hour = clocks[w]]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads, listState,
                   clocks, views, admitted, reqs, rview, casWins>>

FlushClose(w) ==
    /\ flushes[w].open
    /\ flushes' = [flushes EXCEPT ![w].open = FALSE]
    /\ lastOp' = [NoOp EXCEPT !.kind = "flush", !.actor = w, !.outcome = "close",
                              !.hour = flushes[w].hour]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads, listState,
                   clocks, views, admitted, reqs, rview, casWins>>

CanAdmit(w) ==
    /\ Present(ProvKey)
    /\ flushes[w].open
    /\ flushes[w].next <= MaxAdmitsPerWriter
    /\ clocks[w] - flushes[w].hour <= FlushBound

\* shard_for(series, count) is abstracted to any index the divisor admits.
\* The commit object lands under its own exact key (commit_key_for_token).
DoAdmit(w, cnt, oc, vat) ==
    \E sh \in 0..(cnt - 1) :
        LET rec == [writer |-> w, shard |-> sh, ingestHour |-> flushes[w].hour,
                    routeHour |-> clocks[w], id |-> flushes[w].next]
        IN /\ PutCreateIfAbsent(TokenKey(w, flushes[w].next), rec)
           /\ admitted' = admitted \cup {rec}
           /\ flushes' = [flushes EXCEPT ![w].next = @ + 1]
           /\ lastOp' = [NoOp EXCEPT !.kind = "admit", !.actor = w,
                                     !.outcome = oc, !.hour = clocks[w],
                                     !.count = cnt, !.viewAt = vat]

\* route_cached returning Fresh: the cached view is within C, so no read.
AdmitCached(w) ==
    /\ CanAdmit(w)
    /\ Fresh(w)
    /\ DoAdmit(w, ActiveCount(views[w].gens, clocks[w]), "cached", views[w].at)
    /\ UNCHANGED <<clocks, views, reqs, rview, casWins>>

\* route_cached returning Routed::Stale, then a successful refresh GET.
AdmitAfterRefresh(w) ==
    /\ CanAdmit(w)
    /\ ~Fresh(w)
    /\ views' = [views EXCEPT ![w] = [has |-> TRUE, gens |-> Gens,
                                      at |-> clocks[w]]]
    /\ DoAdmit(w, ActiveCount(Gens, clocks[w]), "refreshed", clocks[w])
    /\ UNCHANGED <<clocks, reqs, rview, casWins>>

\* try_grace_extend: the refresh GET failed (a failed read changes no store
\* state), so route on the last-known-good view while
\* hour_of(now) < hour_of(refreshed_at) + min_lead_hours(C).
AdmitGrace(w) ==
    /\ CanAdmit(w)
    /\ ~Fresh(w)
    /\ GraceOpen(w)
    /\ DoAdmit(w, ActiveCount(views[w].gens, clocks[w]), "grace", views[w].at)
    /\ UNCHANGED <<clocks, views, reqs, rview, casWins>>

\* StaleProvisioningView: the refresh GET failed and the grace horizon has
\* passed, so admission fails closed. With WriterFenceEnabled FALSE the writer
\* routes on the expired view instead, which is the fence's negative control.
AdmitFailClosed(w) ==
    /\ CanAdmit(w)
    /\ ~Fresh(w)
    /\ ~GraceOpen(w)
    /\ IF WriterFenceEnabled \/ ~views[w].has
         THEN /\ lastOp' = [NoOp EXCEPT !.kind = "admit", !.actor = w,
                                        !.outcome = "failclosed",
                                        !.hour = clocks[w],
                                        !.viewAt = views[w].at]
              /\ UNCHANGED <<store, lastModified, versionCounter, uploads,
                             listState, admitted, flushes>>
         ELSE DoAdmit(w, ActiveCount(views[w].gens, clocks[w]), "cached",
                      views[w].at)
    /\ UNCHANGED <<clocks, views, reqs, rview, casWins>>

\* A crash loses the open flush and the cached view; admitted records are
\* already durable objects and stay.
WriterCrash(w) ==
    /\ flushes' = [flushes EXCEPT ![w].open = FALSE]
    /\ views' = [views EXCEPT ![w] = [has |-> FALSE, gens |-> InitialHistory,
                                      at |-> 0]]
    /\ lastOp' = [NoOp EXCEPT !.kind = "crash", !.actor = w]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads, listState,
                   clocks, admitted, reqs, rview, casWins>>

\* --- Folder (crates/ravel-catalog/src/fold.rs) ------------------------------

\* The fold stamps HEAD with the shard ceiling at its watermark plus the number
\* of generations it read (SnapshotHead::shard_generation_count).
\* The watermark only advances (a fold seals hours in order), and a fold that
\* would stamp the record HEAD already carries is not a distinct behavior.
Fold ==
    /\ Present(ProvKey)
    /\ \E wm \in 0..clocks[Reader] :
          LET nh == [ceiling |-> ShardCeiling(Gens, wm),
                     genCount |-> Len(Gens), watermark |-> wm]
          IN /\ Present(HeadKey) => (wm >= HeadOf.watermark /\ nh # HeadOf)
             /\ PutOverwrite(HeadKey, nh)
    /\ lastOp' = [NoOp EXCEPT !.kind = "fold", !.actor = Reader]
    /\ UNCHANGED localVars

\* --- Reader (crates/ravel-catalog/src/catalog.rs) ---------------------------

\* The scan set for a window is one shard bound over the whole window
\* (max_scan_count_over_range), applied to every ingest-hour bucket in it. A
\* record is expected to be found once the reader's window end has reached the
\* hour the record was routed at.
UnderScan(g, f, t) ==
    \E v \in admitted : /\ v.ingestHour >= f
                        /\ v.ingestHour <= t
                        /\ v.routeHour <= t
                        /\ v.shard >= RangeScanCount(g, f, t, S)

\* read_scan_generations serves the record from a cache with the same interval
\* C, so a query's generation view can be one interval behind the record.
\* validate_head_against_generations then accepts on ceiling match or the
\* safely-old rule, else takes exactly one uncached re-read, else fails closed.
\* There is no listing fallback: a HEAD with the reader's generation count but
\* a disagreeing ceiling fails without a re-read.
ReaderQuery ==
    /\ Present(ProvKey) /\ Present(HeadKey)
    /\ \E f \in 0..clocks[Reader] :
         LET cached == rview.has /\ clocks[Reader] - rview.at <= C
             g0 == IF cached THEN rview.gens ELSE Gens
             at0 == IF cached THEN rview.at ELSE clocks[Reader]
             hd == HeadOf
             t == clocks[Reader]
         IN IF Acceptable(hd, g0)
              THEN /\ rview' = [has |-> TRUE, gens |-> g0, at |-> at0]
                   /\ lastOp' = [NoOp EXCEPT !.kind = "read", !.actor = Reader,
                                     !.outcome = "ok", !.hour = f,
                                     !.count = RangeScanCount(g0, f, t, S),
                                     !.before = g0, !.after = g0,
                                     !.underScan = UnderScan(g0, f, t),
                                     !.headOld = hd.ceiling #
                                                 ShardCeiling(g0, hd.watermark)]
              ELSE IF hd.genCount = Len(g0)
                THEN /\ rview' = [has |-> TRUE, gens |-> g0, at |-> at0]
                     /\ lastOp' = [NoOp EXCEPT !.kind = "read",
                                       !.actor = Reader,
                                       !.outcome = "failclosed", !.hour = f]
                ELSE /\ rview' = [has |-> TRUE, gens |-> Gens, at |-> clocks[Reader]]
                     /\ lastOp' = IF Acceptable(hd, Gens)
                          THEN [NoOp EXCEPT !.kind = "read", !.actor = Reader,
                                    !.outcome = "reread-ok", !.hour = f,
                                    !.count = RangeScanCount(Gens, f, t, S),
                                    !.before = Gens, !.after = Gens,
                                    !.underScan = UnderScan(Gens, f, t),
                                    !.headOld = hd.ceiling #
                                                ShardCeiling(Gens, hd.watermark)]
                          ELSE [NoOp EXCEPT !.kind = "read", !.actor = Reader,
                                    !.outcome = "failclosed", !.hour = f]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads, listState,
                   clocks, views, flushes, admitted, reqs, casWins>>

\* commit_key_for_token: resolution is an exact key GET, independent of any
\* shard count. With TokenValidatedAgainstCount TRUE the resolver compares
\* token.shard against the active count first, which is the negative control
\* for the "no code validates token.shard against a count" MUST.
ResolveToken ==
    /\ Present(ProvKey)
    /\ \E v \in admitted :
         LET k == TokenKey(v.writer, v.id)
             found == /\ Present(k)
                      /\ (TokenValidatedAgainstCount =>
                            v.shard < ActiveCount(Gens, clocks[Reader]))
         IN lastOp' = [NoOp EXCEPT !.kind = "resolve", !.actor = Reader,
                           !.outcome = IF found THEN "ok" ELSE "notfound",
                           !.hour = v.ingestHour, !.count = v.shard]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads, listState,
                   clocks, views, flushes, admitted, reqs, rview, casWins>>

\* --- Next -------------------------------------------------------------------

Next ==
    \/ Bootstrap
    \/ TickRouters
    \/ \E a \in Actors : TickActor(a)
    \/ \E r \in Requesters :
          ReqRead(r) \/ ReqReject(r) \/ ReqAppendOk(r) \/ ReqAppendLost(r)
             \/ ReqAppendConflict(r)
    \/ \E w \in Writers :
          FlushOpen(w) \/ FlushClose(w) \/ AdmitCached(w)
             \/ AdmitAfterRefresh(w) \/ AdmitGrace(w) \/ AdmitFailClosed(w)
             \/ WriterCrash(w)
    \/ Fold
    \/ ReaderQuery
    \/ ResolveToken

Spec == Init /\ [][Next]_vars

\* --- Safety invariants ------------------------------------------------------

\* The record stays dense, append-only, and strictly increasing in activation
\* hour, with adjacent counts differing (ADR-0052 section 1).
HistoryDenseAppendOnlyIncreasing ==
    Present(ProvKey) =>
        LET g == Gens
        IN /\ Len(g) >= 1
           /\ g[1] = Gen0
           /\ \A i \in 1..(Len(g) - 1) :
                 /\ g[i + 1].gen = g[i].gen + 1
                 /\ g[i + 1].act > g[i].act
                 /\ g[i + 1].count # g[i].count

\* A successful (or lost-response) CAS append extends the durable history by
\* exactly one generation and discards nothing already stored. Grounded on the
\* store's current content (Gens), not on the witness's own before/after
\* fields, so a lost-response append that in fact clobbered the record would
\* be caught here even though the witness recorded its own intended effect.
CasAppendNeverDiscards ==
    (lastOp.kind = "append" /\ lastOp.outcome \in {"ok", "lost"}
        /\ Present(ProvKey)) =>
            /\ IsPrefixOf(lastOp.before, Gens)
            /\ Len(Gens) = Len(lastOp.before) + 1

\* No admitted record sits at a shard index outside the scan set for the
\* window that reaches from its ingest hour to the hour it was routed at.
EveryAdmittedWriteInScanSet ==
    Present(ProvKey) =>
        \A v \in admitted :
            v.shard < RangeScanCount(Gens, v.ingestHour, v.routeHour, S)

\* Coverage probe, not a safety property: witnesses that a real admitted
\* record can trail its ingest hour by 2 or more, so a config actually
\* reaches the FlushBound=2 boundary rather than passing it vacuously.
\* Grounded on admitted (the store), not on the FlushBound constant itself.
\* See flush-bound-trailing.cfg.
FlushBoundNeverBites ==
    \A v \in admitted : (v.routeHour - v.ingestHour) < 2

\* The straggler case of the above: a record routed on a count larger than the
\* one active at its route hour (a decrease's retiring count) stays in scope.
DecreaseKeepsStraggler ==
    Present(ProvKey) =>
        \A v \in admitted :
            v.shard >= ActiveCount(Gens, v.routeHour) =>
                v.shard < RangeScanCount(Gens, v.ingestHour, v.routeHour, S)

\* Coverage probe, not a safety property: witnesses that two distinct
\* writers can hold an open flush at the same time. exhaustive.cfg drops to
\* a single writer for tractability; this probe (see
\* two-writer-concurrency-probe.cfg) shows the concurrent-writer interleaving
\* it gives up is still reachable, at smoke's own dimensions.
TwoWritersNeverConcurrentlyOpen ==
    \A wa, wb \in Writers : (wa # wb) => ~(flushes[wa].open /\ flushes[wb].open)

\* No record is admitted on a view older than the degraded-grace horizon.
StaleWriterFailsClosed ==
    (lastOp.kind = "admit" /\ lastOp.outcome \in {"cached", "grace"}) =>
        lastOp.hour < lastOp.viewAt + MinLeadHours

\* A read that returned data did not under-scan its window.
StaleReaderFailsClosed ==
    (lastOp.kind = "read" /\ lastOp.outcome \in {"ok", "reread-ok"}) =>
        ~lastOp.underScan

\* Accepting an older HEAD is safe: its stamped ceiling still equals the true
\* ceiling for every hour up to its watermark.
SafelyOldHeadRule ==
    (lastOp.kind = "read" /\ lastOp.outcome \in {"ok", "reread-ok"}
        /\ Present(HeadKey)) =>
            HeadOf.ceiling >= ShardCeiling(lastOp.after, HeadOf.watermark)

\* Token resolution never depends on a shard count.
TokenResolvesAcrossReshards ==
    lastOp.kind = "resolve" => lastOp.outcome = "ok"

\* Two concurrent requests reading the same version cannot both win.
OneCasWinner ==
    \A a, b \in casWins : a.base = b.base => a.req = b.req

\* ADR-0052 section 3's inequality against live state: a generation no live
\* view knows about activates at least MinLeadHours past that view's refresh
\* time, allowing for the appender's clock skew.
LeadCoversRefreshHorizon ==
    Present(ProvKey) =>
        \A w \in Writers :
            \A i \in 2..Len(Gens) :
                (views[w].has /\ Len(views[w].gens) < i) =>
                    Gens[i].act + AppenderSkew >= views[w].at + MinLeadHours

\* --- Liveness ---------------------------------------------------------------

\* Weak fairness on the refresh-and-admit step (the store read succeeding) and
\* on the flush actions that keep it enabled. Nothing else is fair: crashes,
\* failed reads, grace admissions, folds, and reads may or may not happen, and
\* the appender is never required to act.
FairSpec ==
    /\ Spec
    \* The writer's refresh loop retries on its own; nothing external is
    \* required to make a pending refresh-and-admit succeed.
    /\ \A w \in Writers : WF_vars(AdmitAfterRefresh(w))
    \* A writer with no open flush always opens one on its own next tick.
    /\ \A w \in Writers : WF_vars(FlushOpen(w))
    \* An open flush is always closed once its window elapses.
    /\ \A w \in Writers : WF_vars(FlushClose(w))

RoutedOnLatest(w) == views[w].has /\ Len(views[w].gens) = Len(Gens)

\* A writer whose view has gone stale while the record holds a generation it
\* may not know either refreshes onto the current history (so its next record
\* routes on the new generation) or runs out of admissions. The escape clause
\* is the model's admission bound, not a property of the system.
EventuallyRoutedOnNewGeneration ==
    \A w \in Writers :
        (Present(ProvKey) /\ Len(Gens) > 1 /\ ~Fresh(w) /\ AdmitsLeft(w) > 0)
            ~> (RoutedOnLatest(w) \/ AdmitsLeft(w) = 0)

=============================================================================
