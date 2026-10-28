--------------------------- MODULE AgentIntegrated ---------------------------
(***************************************************************************)
(* One agent end to end (ADR-3331 decisions 4 to 8 and 13): a client       *)
(* enqueue, takeover, one tool step under the tool's recovery policy, the  *)
(* final turn completion, and a timer generation racing the client's       *)
(* cancel and the turn commit on the same HEAD.                            *)
(*                                                                         *)
(* ABSTRACTION BOUNDARY                                                    *)
(*                                                                         *)
(* Modelled: one agent HEAD written CreateIfAbsent once and CasVersion     *)
(* afterwards; one client that enqueues one request (payload object PUT    *)
(* before the enqueue CAS), may lose the response of either write,         *)
(* resolves an unknown outcome by re-reading HEAD, is answered, and may    *)
(* cancel; workers that take the agent over, bind each accepted input to a *)
(* turn, run the client input's handler (one durable step and one effect   *)
(* intent, attempt authorization, the provider's execution with response   *)
(* loss, result record), write the turn output and commit; worker crash    *)
(* and restart; a timer scheduled, cancelled or rescheduled, its wake hint *)
(* written by the scheduler, and the sweep that reads a hint and the HEAD  *)
(* it names and then fires the generation or deletes the hint.             *)
(*                                                                         *)
(* Every read, immutable write, HEAD CAS, acknowledge, crash, recovery,    *)
(* discovery, external execution, response loss and result record is its  *)
(* own action. AgentProtocol folds five of these for its smoke budget and  *)
(* they are separate here: the client's answer (Ack), the payload PUT's    *)
(* lost response, the re-read that resolves an unknown enqueue outcome     *)
(* (Resolve), a crashed worker's restart (Recover), and the sweep's read   *)
(* before its fire (SweepRead). Three callers still fold a read into their *)
(* CAS, each on the current version with nothing written in between: the  *)
(* client's cancel, the timer schedule and cancel, and the result-         *)
(* completion endpoint. The README states how this module refines the     *)
(* other two.                                                              *)
(*                                                                         *)
(* Assumed, not checked:                                                   *)
(*   - The store honours its contract (common/RavelObjectStore.tla).      *)
(*   - Handler code is a black box. The client input's handler runs one   *)
(*     durable step and one effect; a timer input's handler runs none.    *)
(*     Replay drift, retryable failures and admin reconciliation are      *)
(*     AgentEffects's; here an effect in doubt stays in doubt.            *)
(*   - Under provider_idempotency the provider deduplicates every         *)
(*     dispatch of the effect id.                                         *)
(*   - One tenant, one agent, one incarnation: tombstone, recreate and    *)
(*     tenant isolation are AgentProtocol's.                              *)
(*   - No real time. A written hint may be served by the sweep at any    *)
(*     later step, which covers a hint served early and one served late;  *)
(*     a hint served before it is due and kept is AgentProtocol's.        *)
(*                                                                         *)
(* Witnesses: `lastCas` holds the HEAD before and after the HEAD write the *)
(* step made (NoCas otherwise), derived in Next from store and store'.    *)
(* `acked` holds the handle the client was answered with; `calls` is the  *)
(* provider's log. Every invariant reads the store, lastCas, acked or     *)
(* calls.                                                                  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Workers,      \* runtime workers
    Client,       \* the one sender
    Policies,     \* recovery policies the tool may declare (one per run)
    MaxEpoch,     \* bound on takeovers
    MaxAttempt,   \* bound on attempts; a response is lost only below it
    MaxTimerGen,  \* bound on timer generations
    Payload,      \* the client payload's hash (content address)
    TimerMsg,     \* the payload hash of a timer message
    NoC,          \* sentinels, each a model value: absent content,
    NoWorker,     \* no owner,
    NoHandle,     \* no handle returned yet,
    NoCas         \* no HEAD write in this step

AllPolicies == {"provider_idempotency", "at_least_once", "manual"}

ASSUME Workers # {} /\ Client \notin Workers
ASSUME Policies \subseteq AllPolicies /\ Policies # {}
ASSUME MaxEpoch \in Nat /\ MaxAttempt \in Nat \ {0} /\ MaxTimerGen \in Nat

\* One client input and one input per timer generation, each bound to one
\* turn: no input is retried here.
MaxTurns == 1 + MaxTimerGen
MaxCalls == 2 * MaxAttempt

HeadKey    == <<"head">>
PayKey     == <<"obj", Payload>>
OutKey(t)  == <<"out", t>>
HintKey(g) == <<"wake", g>>

AllKeys ==
    {HeadKey, PayKey} \cup {OutKey(t) : t \in 1..MaxTurns}
    \cup {HintKey(g) : g \in 1..MaxTimerGen}

\* --- HEAD content ---------------------------------------------------------

NoTurn == [turn |-> 0, seq |-> 0, base |-> 0, creq |-> FALSE]
NoEff  == [st |-> "none", pol |-> NoC, att |-> 0, ratt |-> 0, rval |-> 0]

TimerStates == {"none", "armed", "delivered", "cancelled"}

InputRecs   == [seq: Nat, src: {"client", "timer"}, gen: Nat, hash: {Payload, TimerMsg}]
ReceiptRecs == [rid: {Client}, hash: {Payload}, seq: Nat]
ResultRecs  == [seq: Nat, kind: {"completed", "failed", "cancelled"}, turn: Nat,
                epoch: Nat, owner: Workers \cup {NoWorker}, sv: Nat]

\* step and eff: the transcript record of the client input's step key and
\* the journal record of its intent key. Both follow the input, not the
\* turn; the step is cleared with the input, the journal stays.
HeadRecs ==
    [rev: Nat, owner: Workers \cup {NoWorker}, epoch: Nat, sv: Nat,
     stateRef: {NoC} \cup {OutKey(t) : t \in Nat}, inputs: SUBSET InputRecs,
     inbox: SUBSET Nat, receipts: SUBSET ReceiptRecs, results: SUBSET ResultRecs,
     nextSeq: Nat, active: [turn: Nat, seq: Nat, base: Nat, creq: BOOLEAN],
     turns: Nat, cancels: SUBSET Nat, timer: [gen: Nat, st: TimerStates],
     step: {"none", "completed"},
     eff: [st: {"none", "intent", "authorized", "indoubt", "completed"},
           pol: AllPolicies \cup {NoC}, att: Nat, ratt: Nat, rval: Nat]]

Head0 ==
    [rev |-> 1, owner |-> NoWorker, epoch |-> 0, sv |-> 0, stateRef |-> NoC,
     inputs |-> {}, inbox |-> {}, receipts |-> {}, results |-> {}, nextSeq |-> 0,
     active |-> NoTurn, turns |-> 0, cancels |-> {},
     timer |-> [gen |-> 0, st |-> "none"], step |-> "none", eff |-> NoEff]

IntContent ==
    {NoC, Payload} \cup {OutKey(t) : t \in 1..MaxTurns}
    \cup {<<"hint", g>> : g \in 1..MaxTimerGen} \cup HeadRecs

VARIABLES
    store, lastModified, versionCounter, uploads, listState,  \* the store
    cl,       \* the client's operation in flight
    wk,       \* [Workers -> worker operation in flight]
    sw,       \* the sweep's operation in flight
    calls,    \* provider log: Seq of [att, phys]
    crashed,  \* SUBSET Workers: model bound, each worker crashes once
    pol,      \* the recovery policy the tool declares (fixed per run)
    acked,    \* the handle the client was answered with, or NoHandle
    lastCas   \* witness: HEAD before and after this step's HEAD write

Store == INSTANCE RavelObjectStore
             WITH Keys <- AllKeys, Content <- IntContent,
                  NoContent <- NoC, Clients <- Workers \cup {Client}

sVars == <<store, lastModified, versionCounter, uploads, listState>>
vars  == <<sVars, cl, wk, sw, calls, crashed, pol, acked, lastCas>>

Min(S) == CHOOSE x \in S : \A y \in S : x =< y

RootUp == Store!Present(HeadKey)
Root   == Store!ContentOf(HeadKey)
Ver    == Store!VersionOf(HeadKey)

ChangedHead == store'[HeadKey] # store[HeadKey]

CasWitness ==
    lastCas' =
        IF ~ChangedHead THEN NoCas
        ELSE [before |-> IF RootUp THEN Root ELSE NoC,
              vb |-> Ver,
              after |-> store'[HeadKey].content,
              va |-> store'[HeadKey].version]

\* The CAS on the current version, read and written in one step.
CasNow(new) == Store!PutCasVersion(HeadKey, Ver, new)

\* --- Root transitions (pure functions of an observed HEAD) ---------------

Bump(h) == [h EXCEPT !.rev = @ + 1]

NewReceipt(h) == [rid |-> Client, hash |-> Payload, seq |-> h.nextSeq]

EnqueueHead(h) ==
    [Bump(h) EXCEPT !.inputs = @ \cup {[seq |-> h.nextSeq, src |-> "client", gen |-> 0,
                                       hash |-> Payload]},
                    !.inbox = @ \cup {h.nextSeq},
                    !.receipts = @ \cup {NewReceipt(h)},
                    !.nextSeq = @ + 1]

FireHead(h, g) ==
    [Bump(h) EXCEPT !.timer = [gen |-> g, st |-> "delivered"],
                    !.inputs = @ \cup {[seq |-> h.nextSeq, src |-> "timer", gen |-> g,
                                        hash |-> TimerMsg]},
                    !.inbox = @ \cup {h.nextSeq},
                    !.nextSeq = @ + 1]

\* The active turn is the client input's: its handler runs the tool step.
ClientTurn(h) ==
    h.active.turn # 0 /\ \E x \in h.inputs : x.seq = h.active.seq /\ x.src = "client"

TurnResult(h, kind, w) ==
    [seq |-> h.active.seq, kind |-> kind, turn |-> h.active.turn, epoch |-> h.epoch,
     owner |-> w, sv |-> IF kind = "completed" THEN h.sv + 1 ELSE h.sv]

CommitKind(h, kind) == IF h.active.creq THEN "cancelled" ELSE kind

\* The final turn CAS: new state, consumed input and its result together.
EndTurn(h, kind, w) ==
    LET k == CommitKind(h, kind)
    IN [Bump(h) EXCEPT !.inbox = @ \ {h.active.seq},
                       !.active = NoTurn,
                       !.results = @ \cup {TurnResult(h, k, w)},
                       !.sv = IF k = "completed" THEN @ + 1 ELSE @,
                       !.stateRef = IF k = "completed" THEN OutKey(h.active.turn) ELSE @,
                       !.step = IF ClientTurn(h) THEN "none" ELSE @]

\* An active input is only marked; a pending one is settled at once.
CancelHead(h, s) ==
    IF h.active.turn # 0 /\ h.active.seq = s
      THEN [Bump(h) EXCEPT !.cancels = @ \cup {s}, !.active.creq = TRUE]
      ELSE [Bump(h) EXCEPT !.inbox = @ \ {s},
                           !.cancels = @ \cup {s},
                           !.results = @ \cup {[seq |-> s, kind |-> "cancelled", turn |-> 0,
                                                epoch |-> 0, owner |-> NoWorker,
                                                sv |-> h.sv]}]

AuthorizeHead(h, a) == [Bump(h) EXCEPT !.eff.st = "authorized", !.eff.att = a]

RecordResultHead(h, att, val) ==
    [Bump(h) EXCEPT !.eff.st = "completed", !.eff.ratt = att, !.eff.rval = val]

\* The owner may act on its turn only while the turn is uncancelled.
Running(h) == h.active.turn # 0 /\ ~h.active.creq

\* --- Initial state --------------------------------------------------------

ClientIdle == [pc |-> "start", ver |-> 0, obs |-> NoC, handle |-> NoHandle]
WorkerIdle == [pc |-> "idle", ver |-> 0, obs |-> NoC, held |-> "none", att |-> 0, val |-> 0]
SweepIdle  == [pc |-> "idle", ver |-> 0, obs |-> NoC, g |-> 0]

Init ==
    /\ Store!StoreInit
    /\ cl = ClientIdle
    /\ wk = [w \in Workers |-> WorkerIdle]
    /\ sw = SweepIdle
    /\ calls = <<>>
    /\ crashed = {}
    /\ pol \in Policies
    /\ acked = NoHandle
    /\ lastCas = NoCas

CreateHead ==
    /\ ~RootUp
    /\ Store!PutCreateIfAbsent(HeadKey, Head0)
    /\ UNCHANGED <<cl, wk, sw, calls, crashed, pol, acked>>

\* --- Client: enqueue ------------------------------------------------------

SetCl(r) == cl' = r
Mine(h) == {r \in h.receipts : r.rid = Client}

\* GET HEAD, first or after an unknown outcome.
ClientRead ==
    /\ cl.pc \in {"start", "unknown"}
    /\ RootUp
    /\ SetCl([cl EXCEPT !.pc = "read", !.ver = Ver, !.obs = Root])
    /\ UNCHANGED <<sVars, wk, sw, calls, crashed, pol, acked>>

\* The root read holds a receipt for the request id (after a lost CAS
\* response, or a retry of an accepted request): the request is accepted
\* and the client is answered with the recorded handle. The hash always
\* matches here; IdempotencyConflict is AgentProtocol's.
Resolve ==
    /\ cl.pc = "read"
    /\ Mine(cl.obs) # {}
    /\ SetCl([cl EXCEPT !.pc = "resp", !.handle = CHOOSE r \in Mine(cl.obs) : TRUE])
    /\ UNCHANGED <<sVars, wk, sw, calls, crashed, pol, acked>>

\* PUT the payload object with CreateIfAbsent; AlreadyExists is a success,
\* since the key is the content hash. The version read is the CAS token.
PutPayload ==
    /\ cl.pc = "read"
    /\ Mine(cl.obs) = {}
    /\ Store!PutCreateIfAbsent(PayKey, Payload)
    /\ SetCl([cl EXCEPT !.pc = "dep"])
    /\ UNCHANGED <<wk, sw, calls, crashed, pol, acked>>

\* The payload PUT applied and its response was lost: the client starts
\* over from the read, and its retried PUT answers AlreadyExists.
PutPayloadLostResponse ==
    /\ cl.pc = "read"
    /\ Mine(cl.obs) = {}
    /\ Store!PutCreateIfAbsentLostResponse(PayKey, Payload)
    /\ SetCl(ClientIdle)
    /\ UNCHANGED <<wk, sw, calls, crashed, pol, acked>>

\* The enqueue CAS links the payload, allocates accept_seq and records the
\* receipt. On success the response is held for the answer; on
\* PreconditionFailed the client re-reads.
EnqueueCas ==
    /\ cl.pc = "dep"
    /\ Store!PutCasVersion(HeadKey, cl.ver, EnqueueHead(cl.obs))
    /\ IF Store!CasResult(HeadKey, cl.ver) = "Ok"
         THEN SetCl([ClientIdle EXCEPT !.pc = "resp", !.handle = NewReceipt(cl.obs)])
         ELSE SetCl(ClientIdle)
    /\ UNCHANGED <<wk, sw, calls, crashed, pol, acked>>

EnqueueCasLostResponse ==
    /\ cl.pc = "dep"
    /\ Store!PutCasVersionLostResponse(HeadKey, cl.ver, EnqueueHead(cl.obs))
    /\ SetCl([cl EXCEPT !.pc = "unknown"])
    /\ UNCHANGED <<wk, sw, calls, crashed, pol, acked>>

\* The client is answered with the handle.
Ack ==
    /\ cl.pc = "resp"
    /\ acked' = cl.handle
    /\ SetCl([cl EXCEPT !.pc = "acked"])
    /\ UNCHANGED <<sVars, wk, sw, calls, crashed, pol>>

\* Cancellation linearizes at HEAD (read and CAS in one step). An input that
\* already has a result answers AlreadyTerminal and writes nothing.
Cancel ==
    /\ cl.pc = "acked"
    /\ RootUp
    /\ ~\E r \in Root.results : r.seq = acked.seq
    /\ acked.seq \notin Root.cancels
    /\ CasNow(CancelHead(Root, acked.seq))
    /\ SetCl([cl EXCEPT !.pc = "done"])
    /\ UNCHANGED <<wk, sw, calls, crashed, pol, acked>>

\* --- Workers --------------------------------------------------------------

SetWk(w, r) == wk' = [wk EXCEPT ![w] = r]
O(w) == wk[w].obs
Fresh(w) == RootUp /\ wk[w].ver = Ver
Ready(w) == wk[w].pc = "ready" /\ wk[w].held = "none" /\ Fresh(w)
Own(w) == O(w).owner = w

CasThen(w, new, okRec) ==
    /\ Store!PutCasVersion(HeadKey, wk[w].ver, new)
    /\ IF Store!CasResult(HeadKey, wk[w].ver) = "Ok" THEN SetWk(w, okRec)
       ELSE IF wk[w].held = "none" THEN SetWk(w, WorkerIdle)
       ELSE SetWk(w, [wk[w] EXCEPT !.pc = "idle", !.ver = 0, !.obs = NoC])

WUnchanged == UNCHANGED <<cl, sw, calls, crashed, pol, acked>>

\* GET HEAD, when idle or when the root held is no longer current.
Read(w) ==
    /\ RootUp
    /\ \/ wk[w].pc = "idle"
       \/ wk[w].pc = "ready" /\ wk[w].ver # Ver
    /\ SetWk(w, [wk[w] EXCEPT !.pc = "ready", !.ver = Ver, !.obs = Root])
    /\ UNCHANGED sVars
    /\ WUnchanged

\* Claim an agent with work (pending input or an armed timer).
Takeover(w) ==
    /\ Ready(w)
    /\ ~Own(w)
    /\ O(w).epoch < MaxEpoch
    /\ (O(w).inbox # {} \/ O(w).timer.st = "armed")
    /\ CasThen(w, [Bump(O(w)) EXCEPT !.epoch = @ + 1, !.owner = w], WorkerIdle)
    /\ WUnchanged

BeginTurn(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ O(w).active.turn = 0
    /\ O(w).inbox # {}
    /\ LET h == O(w)
       IN CasThen(w, [Bump(h) EXCEPT !.turns = @ + 1,
                         !.active = [turn |-> h.turns + 1, seq |-> Min(h.inbox),
                                     base |-> h.sv, creq |-> FALSE]], WorkerIdle)
    /\ WUnchanged

RecordStep(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ ClientTurn(O(w))
    /\ O(w).step = "none"
    /\ CasThen(w, [Bump(O(w)) EXCEPT !.step = "completed"], WorkerIdle)
    /\ WUnchanged

CommitIntent(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ ClientTurn(O(w))
    /\ O(w).step = "completed"
    /\ O(w).eff.st = "none"
    /\ CasThen(w, [Bump(O(w)) EXCEPT !.eff = [NoEff EXCEPT !.st = "intent", !.pol = pol]],
               WorkerIdle)
    /\ WUnchanged

AuthorizeFirst(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ ClientTurn(O(w))
    /\ O(w).eff.st = "intent"
    /\ CasThen(w, AuthorizeHead(O(w), 1), [WorkerIdle EXCEPT !.pc = "exec", !.att = 1])
    /\ WUnchanged

\* An attempt read authorized without a result, not held from this worker's
\* own dispatch, is ambiguous. Under manual the next authorization publishes
\* InDoubt and dispatches nothing; otherwise it authorizes attempt + 1.
Reauthorize(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ ClientTurn(O(w))
    /\ O(w).eff.st = "authorized"
    /\ O(w).eff.att < MaxAttempt
    /\ LET h == O(w)
       IN IF h.eff.pol = "manual"
            THEN CasThen(w, [Bump(h) EXCEPT !.eff.st = "indoubt"], WorkerIdle)
            ELSE CasThen(w, AuthorizeHead(h, h.eff.att + 1),
                         [WorkerIdle EXCEPT !.pc = "exec", !.att = h.eff.att + 1])
    /\ WUnchanged

\* The handler finishes (a client turn replays its effect result from the
\* journal) and PUTs the turn output with CreateIfAbsent; the output is a
\* function of the turn, so a resumed turn writes the same key.
PutTurnOutput(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ ClientTurn(O(w)) => O(w).eff.st = "completed"
    /\ Store!PutCreateIfAbsent(OutKey(O(w).active.turn), OutKey(O(w).active.turn))
    /\ SetWk(w, [wk[w] EXCEPT !.pc = "out"])
    /\ UNCHANGED <<cl, sw, calls, crashed, pol, acked>>

\* The completion CAS on the version read before the output PUT. A failed
\* CAS re-reads: a root still on the same turn is completed again (its PUT
\* answers AlreadyExists), and a cancel request that landed meanwhile is
\* committed by CommitCancelled.
CompleteTurn(w) ==
    /\ wk[w].pc = "out"
    /\ CasThen(w, EndTurn(O(w), "completed", w), WorkerIdle)
    /\ WUnchanged

CompleteTurnLostResponse(w) ==
    /\ wk[w].pc = "out"
    /\ Store!PutCasVersionLostResponse(HeadKey, wk[w].ver, EndTurn(O(w), "completed", w))
    /\ SetWk(w, WorkerIdle)
    /\ WUnchanged

\* A cancel request read on the turn: the owner stops the handler and
\* commits, which publishes the input cancelled. Refused while the effect
\* is in doubt (NoEffectInDoubt).
CommitCancelled(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ O(w).active.turn # 0
    /\ O(w).active.creq
    /\ ~(ClientTurn(O(w)) /\ O(w).eff.st = "indoubt")
    /\ CasThen(w, EndTurn(O(w), "completed", w), WorkerIdle)
    /\ WUnchanged

\* --- External execution ---------------------------------------------------

PhysCalls == {i \in 1..Len(calls) : calls[i].phys}

Dispatch(w) ==
    /\ wk[w].pc = "exec"
    /\ Len(calls) < MaxCalls
    /\ LET dedup == pol = "provider_idempotency" /\ PhysCalls # {}
       IN /\ calls' = Append(calls, [att |-> wk[w].att, phys |-> ~dedup])
          /\ SetWk(w, [wk[w] EXCEPT !.pc = "await",
                         !.val = IF dedup THEN CHOOSE i \in PhysCalls : TRUE
                                 ELSE Len(calls) + 1])
    /\ UNCHANGED <<sVars, cl, sw, crashed, pol, acked>>

Respond(w) ==
    /\ wk[w].pc = "await"
    /\ SetWk(w, [wk[w] EXCEPT !.pc = "idle", !.held = "result", !.ver = 0, !.obs = NoC])
    /\ UNCHANGED sVars
    /\ WUnchanged

LoseResponse(w) ==
    /\ wk[w].pc = "await"
    /\ wk[w].att < MaxAttempt
    /\ SetWk(w, WorkerIdle)
    /\ UNCHANGED sVars
    /\ WUnchanged

\* Record a held provider result (fenced by incarnation only). A completed
\* effect answers without a write.
RecordHeldResult(w) ==
    /\ wk[w].pc = "ready"
    /\ Fresh(w)
    /\ wk[w].held = "result"
    /\ IF O(w).eff.st = "completed"
         THEN /\ SetWk(w, WorkerIdle)
              /\ UNCHANGED sVars
         ELSE CasThen(w, RecordResultHead(O(w), wk[w].att, wk[w].val), WorkerIdle)
    /\ WUnchanged

\* The result-completion endpoint, for an attempt whose dispatcher lost the
\* response or died.
ResultCallback(i) ==
    /\ RootUp
    /\ i \in PhysCalls
    /\ Root.eff.st \in {"authorized", "indoubt"}
    /\ calls[i].att =< Root.eff.att
    /\ CasNow(RecordResultHead(Root, calls[i].att, i))
    /\ UNCHANGED <<cl, wk, sw, calls, crashed, pol, acked>>

\* --- Crash and recovery ---------------------------------------------------

WorkerCrash(w) ==
    /\ wk[w].pc \notin {"idle", "down"} \/ wk[w].held # "none"
    /\ w \notin crashed
    /\ crashed' = crashed \cup {w}
    /\ SetWk(w, [WorkerIdle EXCEPT !.pc = "down"])
    /\ UNCHANGED <<sVars, cl, sw, calls, pol, acked>>

Recover(w) ==
    /\ wk[w].pc = "down"
    /\ SetWk(w, WorkerIdle)
    /\ UNCHANGED sVars
    /\ WUnchanged

\* --- Timer, hint and sweep ------------------------------------------------

TUnchanged == UNCHANGED <<cl, wk, sw, calls, crashed, pol, acked>>

\* Fenced by incarnation, not owner: any caller may schedule or cancel.
\* Scheduling an armed timer supersedes it under the next generation.
ScheduleTimer ==
    /\ RootUp
    /\ Root.timer.gen < MaxTimerGen
    /\ CasNow([Bump(Root) EXCEPT !.timer = [gen |-> Root.timer.gen + 1, st |-> "armed"]])
    /\ TUnchanged

CancelTimer ==
    /\ RootUp
    /\ Root.timer.st = "armed"
    /\ CasNow([Bump(Root) EXCEPT !.timer.st = "cancelled"])
    /\ TUnchanged

\* The scheduler writes the armed generation's wake hint after the schedule
\* CAS. The sweep deletes a hint only once its generation is no longer
\* armed, and generations never re-arm, so an absent hint means unwritten.
WriteHint ==
    /\ RootUp
    /\ Root.timer.st = "armed"
    /\ ~Store!Present(HintKey(Root.timer.gen))
    /\ Store!PutOverwrite(HintKey(Root.timer.gen), <<"hint", Root.timer.gen>>)
    /\ UNCHANGED <<cl, wk, sw, calls, crashed, pol, acked>>

\* The sweep lists a hint and GETs the HEAD it names.
SweepRead(g) ==
    /\ sw.pc = "idle"
    /\ Store!Present(HintKey(g))
    /\ RootUp
    /\ sw' = [pc |-> "read", ver |-> Ver, obs |-> Root, g |-> g]
    /\ UNCHANGED <<sVars, cl, wk, calls, crashed, pol, acked>>

\* Fire the generation from the root read, with a CAS on its version; a
\* failed CAS goes back to the listing. A root that no longer has the
\* generation armed fires nothing and the hint is deleted: generations
\* never re-arm.
SweepFire ==
    /\ sw.pc = "read"
    /\ IF sw.obs.timer = [gen |-> sw.g, st |-> "armed"]
         THEN Store!PutCasVersion(HeadKey, sw.ver, FireHead(sw.obs, sw.g))
         ELSE Store!Delete(HintKey(sw.g))
    /\ sw' = SweepIdle
    /\ UNCHANGED <<cl, wk, calls, crashed, pol, acked>>

\* --- Specification --------------------------------------------------------

Step ==
    \/ CreateHead
    \/ ClientRead \/ Resolve \/ PutPayload \/ PutPayloadLostResponse
    \/ EnqueueCas \/ EnqueueCasLostResponse \/ Ack \/ Cancel
    \/ \E w \in Workers :
         \/ Read(w) \/ Takeover(w) \/ BeginTurn(w) \/ RecordStep(w) \/ CommitIntent(w)
         \/ AuthorizeFirst(w) \/ Reauthorize(w) \/ PutTurnOutput(w) \/ CompleteTurn(w)
         \/ CompleteTurnLostResponse(w) \/ CommitCancelled(w)
         \/ Dispatch(w) \/ Respond(w) \/ LoseResponse(w) \/ RecordHeldResult(w)
         \/ WorkerCrash(w) \/ Recover(w)
    \/ \E i \in 1..MaxCalls : ResultCallback(i)
    \/ ScheduleTimer \/ CancelTimer \/ WriteHint
    \/ \E g \in 1..MaxTimerGen : SweepRead(g)
    \/ SweepFire

Next == Step /\ CasWitness

Spec == Init /\ [][Next]_vars

\* Liveness assumptions: the store stays available, the client finishes the
\* enqueue it started (strong fairness, since a lost response sends it back
\* to the read), a crashed worker restarts, workers keep acting on
\* accepted work and dispatch what they authorized, the provider eventually
\* answers, the scheduler writes its hint and the sweep keeps serving
\* hints. The client's cancel, timer scheduling and cancelling, and the
\* result endpoint are not fair.
Fairness ==
    /\ WF_vars(CreateHead /\ CasWitness)
    /\ WF_vars(ClientRead /\ CasWitness)
    /\ WF_vars(Resolve /\ CasWitness)
    /\ SF_vars(PutPayload /\ CasWitness)
    /\ SF_vars(EnqueueCas /\ CasWitness)
    /\ WF_vars(Ack /\ CasWitness)
    /\ WF_vars(WriteHint /\ CasWitness)
    /\ WF_vars((\E g \in 1..MaxTimerGen : SweepRead(g)) /\ CasWitness)
    /\ WF_vars(SweepFire /\ CasWitness)
    /\ \A w \in Workers :
         /\ WF_vars(Read(w) /\ CasWitness)
         /\ WF_vars(Takeover(w) /\ CasWitness)
         /\ WF_vars(BeginTurn(w) /\ CasWitness)
         /\ WF_vars(RecordStep(w) /\ CasWitness)
         /\ WF_vars(CommitIntent(w) /\ CasWitness)
         /\ WF_vars(AuthorizeFirst(w) /\ CasWitness)
         /\ WF_vars(Reauthorize(w) /\ CasWitness)
         /\ WF_vars(PutTurnOutput(w) /\ CasWitness)
         /\ WF_vars(CompleteTurn(w) /\ CasWitness)
         /\ WF_vars(CommitCancelled(w) /\ CasWitness)
         /\ WF_vars(Dispatch(w) /\ CasWitness)
         /\ WF_vars(Respond(w) /\ CasWitness)
         /\ WF_vars(RecordHeldResult(w) /\ CasWitness)
         /\ WF_vars(Recover(w) /\ CasWitness)

FairSpec == Spec /\ Fairness

\* --- Safety properties ----------------------------------------------------
\* The protocol properties take AgentProtocol's form for one agent and one
\* incarnation; the effect properties take AgentEffects's form for the
\* client input's journal.

LC == lastCas
Wrote == LC # NoCas /\ LC.before # NoC
NewResults == LC.after.results \ LC.before.results

\* The input of the effect journal: the client's.
ClientSeq(h) == {x.seq : x \in {y \in h.inputs : y.src = "client"}}

\* A request the client was answered for stays resolvable from the store.
AcceptedImpliesRecoverable ==
    acked # NoHandle =>
        /\ RootUp
        /\ acked \in Root.receipts
        /\ Store!Present(PayKey)

PublishedReferencesComplete ==
    RootUp =>
        LET h == Root
        IN /\ \A x \in h.inputs : x.src = "client" => Store!Present(PayKey)
           /\ \A r \in h.results : r.kind # "cancelled" => Store!Present(OutKey(r.turn))
           /\ h.stateRef # NoC => Store!Present(h.stateRef)
           /\ \A s \in h.inbox : \E x \in h.inputs : x.seq = s

RequestIdentityStable ==
    /\ RootUp =>
         LET h == Root
         IN /\ Cardinality(h.receipts) =< 1
            /\ \A r \in h.receipts : \E x \in h.inputs :
                  x.seq = r.seq /\ x.src = "client" /\ x.hash = r.hash
    /\ Wrote => LC.before.receipts \subseteq LC.after.receipts

OneLogicalCompletion ==
    RootUp =>
        LET h == Root
        IN /\ \A r1, r2 \in h.results : r1.seq = r2.seq => r1 = r2
           /\ \A r \in h.results : r.seq \notin h.inbox
           /\ h.active.turn # 0 => h.active.seq \in h.inbox

TurnCommitAtomic ==
    Wrote =>
        LET b == LC.before
            a == LC.after
        IN /\ {r.seq : r \in NewResults} = b.inbox \ a.inbox
           /\ Cardinality(NewResults) =< 1
           /\ \A r \in NewResults : r.kind = "completed" =>
                 a.sv = b.sv + 1 /\ r.sv = a.sv /\ a.stateRef = OutKey(r.turn)
           /\ (\A r \in NewResults : r.kind # "completed") =>
                 a.sv = b.sv /\ a.stateRef = b.stateRef
           /\ \A r \in NewResults : r.turn # 0 => a.active = NoTurn

NoStaleOwnerCommit ==
    Wrote =>
        \A r \in NewResults : r.turn # 0 =>
            /\ LC.before.owner = r.owner
            /\ LC.before.epoch = r.epoch
            /\ LC.before.active.turn = r.turn
            /\ LC.before.active.seq = r.seq

NoLostConcurrentEnqueue ==
    /\ RootUp =>
         \A s \in 0..(Root.nextSeq - 1) :
            s \in Root.inbox \/ \E r \in Root.results : r.seq = s
    /\ Wrote =>
         /\ LC.before.inputs \subseteq LC.after.inputs
         /\ LC.after.nextSeq >= LC.before.nextSeq
         /\ \A s \in LC.before.inbox :
               s \in LC.after.inbox \/ \E r \in LC.after.results : r.seq = s

MonotonicIdentity ==
    LC # NoCas =>
        /\ LC.va > LC.vb
        /\ LC.before = NoC => LC.after.rev = 1
        /\ LC.before # NoC =>
             /\ LC.after.rev = LC.before.rev + 1
             /\ LC.after.epoch >= LC.before.epoch
             /\ LC.after.timer.gen >= LC.before.timer.gen

TimerGenerationDeliveredOnce ==
    /\ RootUp =>
         \A g \in 1..MaxTimerGen :
            Cardinality({x \in Root.inputs : x.src = "timer" /\ x.gen = g}) =< 1
    /\ Wrote =>
         /\ \A x \in LC.after.inputs \ LC.before.inputs : x.src = "timer" =>
               /\ LC.before.timer = [gen |-> x.gen, st |-> "armed"]
               /\ LC.after.timer = [gen |-> x.gen, st |-> "delivered"]
         /\ (LC.before.timer.st = "delivered" /\ LC.after.timer.gen = LC.before.timer.gen)
               => LC.after.timer.st = "delivered"

\* The step record is written once, a recorded effect result is never
\* rewritten, and the journal never goes back to a lower attempt or another
\* policy.
StepTranscriptCompatible ==
    Wrote =>
        LET b == LC.before
            a == LC.after
        IN /\ b.eff.st # "none" => a.eff.pol = b.eff.pol
           /\ b.eff.st = "completed" => a.eff = b.eff
           /\ a.eff.att >= b.eff.att
           /\ (b.step = "completed" /\ a.step = "none") =>
                 ClientSeq(b) \cap a.inbox = {}

DispatchRequiresCommittedAuthorization ==
    \A i \in 1..Len(calls) :
        /\ RootUp
        /\ Root.eff.st \notin {"none", "intent"}
        /\ calls[i].att =< Root.eff.att

\* Each attempt is dispatched at most once; under manual only the first
\* attempt exists (no admin here), and no result lands for the client input
\* while its effect is in doubt; under provider_idempotency the provider
\* executes at most once.
PolicyControlsRedispatch ==
    /\ \A i, j \in 1..Len(calls) : i # j => calls[i].att # calls[j].att
    /\ (RootUp /\ Root.eff.pol = "manual") =>
         /\ \A i \in 1..Len(calls) : calls[i].att = 1
         /\ Root.eff.att =< 1
    /\ (RootUp /\ Root.eff.pol = "provider_idempotency") => Cardinality(PhysCalls) =< 1
    /\ (Wrote /\ LC.before.eff.st = "indoubt") =>
         \A r \in NewResults : r.seq \notin ClientSeq(LC.before)

EffectResultFromExecution ==
    (RootUp /\ Root.eff.st = "completed") =>
        /\ Root.eff.rval \in PhysCalls
        /\ calls[Root.eff.rval].att =< Root.eff.att
        /\ 1 =< Root.eff.ratt /\ Root.eff.ratt =< Root.eff.att
        /\ Root.eff.pol # "provider_idempotency" =>
             calls[Root.eff.rval].att = Root.eff.ratt

\* Both forms: a cancelled input ends cancelled; until it settles it is the
\* active turn's input with the request recorded; once the request is on
\* the root the owner journals no new step or intent and authorizes no new
\* attempt; no write drops a cancel or rewrites a published result.
CancellationRespected ==
    /\ RootUp =>
         LET h == Root
         IN /\ \A r \in h.results : r.seq \in h.cancels => r.kind = "cancelled"
            /\ \A s \in h.cancels \cap h.inbox :
                  h.active.turn # 0 /\ h.active.seq = s /\ h.active.creq
    /\ Wrote =>
         LET b == LC.before
             a == LC.after
         IN /\ b.cancels \subseteq a.cancels
            /\ b.results \subseteq a.results
            /\ \A s \in b.cancels \cap b.inbox :
                  s \in a.inbox => a.active.turn = b.active.turn
            /\ b.active.creq =>
                 /\ \A r \in NewResults : r.kind = "cancelled"
                 /\ b.step = "none" => a.step = "none"
                 /\ b.eff.st = "none" => a.eff.st = "none"
                 /\ a.eff.att =< b.eff.att

\* --- Liveness (conditional on Fairness) ------------------------------------

\* Blocked terminals: the client effect is in doubt (no admin here), or the
\* run spent the model's attempt bound on an ambiguous attempt.
EffectBlocked(h) ==
    \/ h.eff.st = "indoubt"
    \/ h.eff.st = "authorized" /\ h.eff.att = MaxAttempt

Settled(s) == RootUp /\ \E r \in Root.results : r.seq = s

\* Every accepted input is eventually settled, or the agent waits on a
\* blocked effect.
InputSettledOrEffectBlocked ==
    \A s \in 0..MaxTurns - 1 :
        (RootUp /\ s \in Root.inbox) ~> (Settled(s) \/ (RootUp /\ EffectBlocked(Root)))

\* The client is eventually answered.
EnqueueEventuallyAnswered == <>(acked # NoHandle)

=============================================================================
