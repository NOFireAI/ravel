----------------------------- MODULE AgentEffects -----------------------------
(***************************************************************************)
(* Durable steps and external effects of one active turn (ADR-3331         *)
(* decision 7 and its step identity amendment).                            *)
(*                                                                         *)
(* ABSTRACTION BOUNDARY                                                    *)
(*                                                                         *)
(* Modelled: one agent HEAD holding one accepted input; turns bound to it  *)
(* by its owner, retried after a retryable failure under a new turn id     *)
(* that keeps the step transcript and the effect journal; one durable step *)
(* and one effect intent per turn, each journaled under a key that follows *)
(* the input, not the turn; attempt authorization by the owner under the   *)
(* tool's recovery policy (provider_idempotency, at_least_once, manual);   *)
(* the external provider, which executes a dispatched attempt and whose    *)
(* response can be lost; result publication into the journal by the       *)
(* dispatcher or by the result-completion endpoint; replay of a completed  *)
(* step or effect from its record; admin reconciliation of a manual effect *)
(* (attach a verified result, or authorize and dispatch another attempt),  *)
(* each behind an immutable audit object; takeover; worker crash and       *)
(* recovery; cancellation of the running turn.                             *)
(*                                                                         *)
(* Every read, immutable write, HEAD CAS, crash, recovery, dispatch,       *)
(* provider response, response loss and result record is its own action.  *)
(* Two callers fold a read into their CAS, as AgentProtocol's CasNow does: *)
(* the client's cancel and the result-completion endpoint. Each acts on    *)
(* the root it reads with nothing written in between, and a CAS on a stale *)
(* read fails and is retried, so the failed attempt is a stutter.          *)
(*                                                                         *)
(* Assumed, not checked:                                                   *)
(*   - The store honours its contract (common/RavelObjectStore.tla).      *)
(*   - Handler code is a black box that may compute a different input    *)
(*     hash on any pass (that is the drift the kernel must catch). A      *)
(*     step is recorded Completed in one CAS; the Started record the      *)
(*     kernel also accepts is checked against the same identity and adds  *)
(*     no case.                                                           *)
(*   - Under provider_idempotency the provider deduplicates every         *)
(*     dispatch of one effect id inside its documented scope, so a        *)
(*     second dispatch answers the first execution's result and executes  *)
(*     nothing.                                                           *)
(*   - An admin's verified result is the provider's actual result for     *)
(*     that attempt; the admin attaches one only for an attempt the       *)
(*     provider executed.                                                 *)
(*   - Enqueue, timers, tombstone and recreate are AgentProtocol's; the   *)
(*     single input is accepted before the first state.                   *)
(*                                                                         *)
(* An owner that reads a cancel request on its turn authorizes no further *)
(* attempt: `Kernel::authorize_attempt` refuses an owner authorization    *)
(* while cancellation is requested, and the negative                      *)
(* authorize-ignores-cancel-request shows the property that rests on it.  *)
(* Rules this module states that HEAD does not carry (see the README,     *)
(* "Differences from the kernel"):                                        *)
(*   - An owner leaves alone a manual attempt whose reconcile audit       *)
(*     object exists: that attempt is the reconciler's to dispatch. The   *)
(*     runtime keeps that only in memory (its open attempts); the root    *)
(*     carries no field that says so, and the model reads the audit       *)
(*     object.                                                            *)
(*                                                                         *)
(* Witnesses: `lastCas` holds the HEAD before and after the HEAD write the *)
(* step made (NoCas otherwise), derived in Next from store and store'.    *)
(* `calls` is the provider's own log of dispatches, one entry per call,   *)
(* recording the attempt and whether it executed physically. Every        *)
(* invariant reads the store, lastCas or calls.                           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Workers,      \* runtime workers that own the agent and run its turns
    Admin,        \* the reconciler holding agents_admin
    Policies,     \* recovery policies the tool may declare (one per run)
    Hashes,       \* input hashes a handler pass may compute
    MaxEpoch,     \* bound on takeovers
    MaxTurns,     \* bound on turns: a failure at the last one is terminal
    MaxAttempt,   \* bound on attempts; a response is lost only below it
    NoC,          \* sentinels, each a model value: absent content,
    NoWorker,     \* no owner,
    NoCas         \* no HEAD write in this step

AllPolicies == {"provider_idempotency", "at_least_once", "manual"}

ASSUME Workers # {} /\ Admin \notin Workers
ASSUME Policies \subseteq AllPolicies /\ Policies # {}
ASSUME Hashes # {}
ASSUME MaxEpoch \in Nat /\ MaxTurns \in Nat \ {0} /\ MaxAttempt \in Nat \ {0}

Actors == Workers \cup {Admin}

\* Each attempt is dispatched once by a correct runner, except that under
\* provider_idempotency attempt 1 is dispatched again, at most MaxAttempt
\* times in all; the bound only stops a broken runner from dispatching
\* forever.
MaxCalls == 2 * MaxAttempt

HeadKey == <<"head">>
OutKey(t) == <<"out", t>>
AuditKey(kind, a) == <<"audit", kind, a>>
AuditKinds == {"result", "attempt"}

AllKeys ==
    {HeadKey} \cup {OutKey(t) : t \in 1..MaxTurns}
    \cup {AuditKey(k, a) : k \in AuditKinds, a \in 1..MaxAttempt}

\* --- HEAD content ---------------------------------------------------------

\* The one input is accept_seq 0. `failed` counts its retryable failures.
\* step: the transcript record of the turn's step key; eff: the journal
\* record of its intent key (att: last authorized attempt; ratt, rval: the
\* attempt a result was recorded for and the provider call that produced
\* it).
NoTurn == [turn |-> 0, base |-> 0, creq |-> FALSE, block |-> FALSE]
NoStep == [st |-> "none", hash |-> NoC]
NoEff  == [st |-> "none", hash |-> NoC, pol |-> NoC, att |-> 0, ratt |-> 0, rval |-> 0]

ResultRecs == [kind: {"completed", "failed", "cancelled"}, turn: Nat, epoch: Nat,
               owner: Workers \cup {NoWorker}, sv: Nat]

HeadRecs ==
    [rev: Nat, owner: Workers \cup {NoWorker}, epoch: Nat, sv: Nat,
     stateRef: {NoC} \cup {OutKey(t) : t \in Nat}, inbox: SUBSET {0}, failed: Nat,
     results: SUBSET ResultRecs, active: [turn: Nat, base: Nat, creq: BOOLEAN, block: BOOLEAN],
     turns: Nat,
     step: [st: {"none", "completed"}, hash: Hashes \cup {NoC}],
     eff: [st: {"none", "intent", "authorized", "indoubt", "completed"},
           hash: Hashes \cup {NoC}, pol: AllPolicies \cup {NoC}, att: Nat, ratt: Nat,
           rval: Nat]]

Head0 ==
    [rev |-> 1, owner |-> NoWorker, epoch |-> 0, sv |-> 0, stateRef |-> NoC,
     inbox |-> {0}, failed |-> 0, results |-> {}, active |-> NoTurn, turns |-> 0,
     step |-> NoStep, eff |-> NoEff]

EffContent == {NoC, "audit"} \cup {OutKey(t) : t \in 1..MaxTurns} \cup HeadRecs

VARIABLES
    store, lastModified, versionCounter, uploads, listState,  \* the store
    ac,       \* [Actors -> operation in flight]
    calls,    \* provider log: Seq of [att, phys]
    crashed,  \* SUBSET Workers: model bound, each worker crashes once
    cl,       \* the client's cancel: "idle" or "done"
    pol,      \* the recovery policy the tool declares (fixed per run)
    lastCas   \* witness: HEAD before and after this step's HEAD write

Store == INSTANCE RavelObjectStore
             WITH Keys <- AllKeys, Content <- EffContent,
                  NoContent <- NoC, Clients <- Actors

sVars == <<store, lastModified, versionCounter, uploads, listState>>
vars  == <<sVars, ac, calls, crashed, cl, pol, lastCas>>

RootUp == Store!Present(HeadKey)
Root   == Store!ContentOf(HeadKey)
Ver    == Store!VersionOf(HeadKey)
AuditUp(kind, a) == Store!Present(AuditKey(kind, a))

ChangedHead == store'[HeadKey] # store[HeadKey]

CasWitness ==
    lastCas' =
        IF ~ChangedHead THEN NoCas
        ELSE [before |-> IF RootUp THEN Root ELSE NoC,
              vb |-> Ver,
              after |-> store'[HeadKey].content,
              va |-> store'[HeadKey].version]

\* --- Actors ---------------------------------------------------------------

\* pc: idle, ready (holds the root read and its version), exec (holds an
\* authorized attempt to dispatch), await (dispatched, response in flight),
\* out (turn output written), auditR / auditA (admin audit object written),
\* down (crashed). held: what survives a re-read in memory: "authz" for an
\* authorization whose CAS response was lost, "result" for a provider
\* response not yet recorded.
ActorIdle == [pc |-> "idle", ver |-> 0, obs |-> NoC, held |-> "none", att |-> 0, val |-> 0]

SetAc(a, r) == ac' = [ac EXCEPT ![a] = r]
O(a) == ac[a].obs

\* The CAS on the version the actor read.
Cas(a, new) == Store!PutCasVersion(HeadKey, ac[a].ver, new)
CasOk(a) == Store!CasResult(HeadKey, ac[a].ver) = "Ok"

\* Success goes to okRec, PreconditionFailed back to a re-read, keeping a
\* held authorization or provider result.
CasThen(a, new, okRec) ==
    /\ Cas(a, new)
    /\ IF CasOk(a) THEN SetAc(a, okRec)
       ELSE IF ac[a].held = "none" THEN SetAc(a, ActorIdle)
       ELSE SetAc(a, [ac[a] EXCEPT !.pc = "idle", !.ver = 0, !.obs = NoC])

\* --- Root transitions (pure functions of an observed HEAD) ---------------

Bump(h) == [h EXCEPT !.rev = @ + 1]

TurnResult(h, kind, w) ==
    [kind |-> kind, turn |-> h.active.turn, epoch |-> h.epoch, owner |-> w,
     sv |-> IF kind = "completed" THEN h.sv + 1 ELSE h.sv]

\* A commit on a turn with a cancel request publishes it cancelled. The
\* transcript is cleared with the input; the journal stays.
CommitKind(h, kind) == IF h.active.creq THEN "cancelled" ELSE kind

EndTurn(h, kind, w) ==
    LET k == CommitKind(h, kind)
    IN [h EXCEPT !.rev = @ + 1,
                 !.inbox = {},
                 !.active = NoTurn,
                 !.results = @ \cup {TurnResult(h, k, w)},
                 !.sv = IF k = "completed" THEN @ + 1 ELSE @,
                 !.stateRef = IF k = "completed" THEN OutKey(h.active.turn) ELSE @,
                 !.step = NoStep]

BlockHead(h) == [h EXCEPT !.rev = @ + 1, !.active.block = TRUE]

RecordResultHead(h, att, val) ==
    [h EXCEPT !.rev = @ + 1, !.eff.st = "completed", !.eff.ratt = att, !.eff.rval = val]

\* The input ended cancelled. Its effect's results are evidence from then on.
InputCancelled(h) == \E r \in h.results : r.kind = "cancelled"

\* Only the journaled attempt's result settles an unsettled effect of an input
\* that did not end cancelled. Anything else is LateResultIgnored: the caller
\* keeps it as evidence and HEAD does not change.
ResultAdmitted(h, att) ==
    /\ h.eff.st \in {"authorized", "indoubt"}
    /\ att = h.eff.att
    /\ ~InputCancelled(h)

\* The owner may act on its turn only while the turn is unblocked and
\* uncancelled.
Running(h) == h.active.turn # 0 /\ ~h.active.block /\ ~h.active.creq

\* A manual attempt authorized by reconciliation belongs to the reconciler.
ReconcilerOwns(h) ==
    h.eff.pol = "manual" /\ h.eff.st = "authorized" /\ AuditUp("attempt", h.eff.att)

\* Reconciliation acts only on an effect in doubt: the runtime's
\* reconcile_attach and reconcile_authorize refuse any other state.
Reconcilable(h) == h.eff.pol = "manual" /\ h.eff.st = "indoubt"

\* --- Initial state --------------------------------------------------------

Init ==
    /\ Store!StoreInit
    /\ ac = [a \in Actors |-> ActorIdle]
    /\ calls = <<>>
    /\ crashed = {}
    /\ cl = "idle"
    /\ pol \in Policies
    /\ lastCas = NoCas

\* The agent with its one accepted input (AgentProtocol checks enqueue).
CreateHead ==
    /\ ~RootUp
    /\ Store!PutCreateIfAbsent(HeadKey, Head0)
    /\ UNCHANGED <<ac, calls, crashed, cl, pol>>

\* --- Reads ----------------------------------------------------------------

\* GET HEAD. An actor re-reads when idle, or when the root it holds is no
\* longer current (an actor waiting on a root it cannot act on re-reads only
\* once that root moves). An authorization held from a lost response is
\* forgotten once the root no longer shows it. The admin reads only a root it
\* can reconcile, or to record a result it holds.
Read(a) ==
    /\ RootUp
    /\ \/ ac[a].pc = "idle"
       \/ ac[a].pc = "ready" /\ ac[a].ver # Ver
    /\ a = Admin => (ac[a].held = "result" \/ Reconcilable(Root))
    /\ SetAc(a, [ac[a] EXCEPT
                   !.pc = "ready", !.ver = Ver, !.obs = Root,
                   !.held = IF @ = "authz"
                              /\ ~(/\ Root.eff.st = "authorized" /\ Root.eff.att = ac[a].att
                                   /\ Root.owner = a /\ Running(Root))
                            THEN "none" ELSE @])
    /\ UNCHANGED <<sVars, calls, crashed, cl, pol>>

\* --- Owner commands -----------------------------------------------------

\* Every command acts on a current read. A command on a stale read is a
\* CAS that fails and sends the actor back to read, which is the stale
\* actor's Read step itself; the commands whose CAS follows another write
\* (CompleteTurn, AdminRecordResult, AdminAuthorize) do fail and re-read.
Fresh(a) == RootUp /\ ac[a].ver = Ver
Ready(w) == ac[w].pc = "ready" /\ ac[w].held = "none" /\ Fresh(w)
Own(w) == O(w).owner = w

\* Claim an agent with a pending input by advancing its epoch. The active
\* turn, its transcript and journal stay; the new owner resumes them.
Takeover(w) ==
    /\ Ready(w)
    /\ ~Own(w)
    /\ O(w).epoch < MaxEpoch
    /\ O(w).inbox # {}
    /\ CasThen(w, [Bump(O(w)) EXCEPT !.epoch = @ + 1, !.owner = w], ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

BeginTurn(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ O(w).active.turn = 0
    /\ O(w).inbox # {}
    /\ LET h == O(w)
       IN CasThen(w, [Bump(h) EXCEPT !.turns = @ + 1,
                         !.active = [turn |-> h.turns + 1, base |-> h.sv,
                                     creq |-> FALSE, block |-> FALSE]], ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* Run the step on a fresh transcript and record it with the input hash the
\* handler computed.
RecordStep(w, x) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).step.st = "none"
    /\ CasThen(w, [Bump(O(w)) EXCEPT !.step = [st |-> "completed", hash |-> x]], ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* A completed step replays from its record: the same identity writes
\* nothing. A replay that computes another input hash blocks the turn
\* (NonDeterministicReplay); the record is never rewritten.
ReplayStepDrift(w, x) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).step.st = "completed"
    /\ x # O(w).step.hash
    /\ CasThen(w, BlockHead(O(w)), ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* Journal the effect intent with the tool's policy.
CommitIntent(w, y) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).step.st = "completed"
    /\ O(w).eff.st = "none"
    /\ CasThen(w, [Bump(O(w)) EXCEPT !.eff = [NoEff EXCEPT !.st = "intent", !.hash = y,
                                                        !.pol = pol]], ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

ReplayIntentDrift(w, y) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).eff.st # "none"
    /\ y # O(w).eff.hash
    /\ CasThen(w, BlockHead(O(w)), ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

AuthorizeHead(h, a) == [Bump(h) EXCEPT !.eff.st = "authorized", !.eff.att = a]

\* The first attempt of a journaled intent. On success the owner holds the
\* attempt and dispatches it next.
AuthorizeFirst(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).eff.st = "intent"
    /\ CasThen(w, AuthorizeHead(O(w), 1),
               [ActorIdle EXCEPT !.pc = "exec", !.att = 1])
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* Dispatches made so far count against the attempt bound under
\* provider_idempotency, where every dispatch carries the same attempt.
DispatchBudgetLeft(h) ==
    IF h.eff.pol = "provider_idempotency" THEN Len(calls) < MaxAttempt
    ELSE h.eff.att < MaxAttempt

\* An attempt is ambiguous when the owner reads it authorized without a
\* result and does not hold it from its own lost response: it may have been
\* dispatched. Only the policy decides what happens next. Under manual the
\* owner's next authorization publishes InDoubt (Blocked) and nothing is
\* dispatched; under provider_idempotency the owner dispatches the same
\* effect id and attempt again with no write; under at_least_once it
\* authorizes attempt + 1 and dispatches that.
Reauthorize(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).eff.st = "authorized"
    /\ ~ReconcilerOwns(O(w))
    /\ DispatchBudgetLeft(O(w))
    /\ LET h == O(w)
       IN CASE h.eff.pol = "manual" ->
                 /\ CasThen(w, [Bump(h) EXCEPT !.eff.st = "indoubt"], ActorIdle)
            [] h.eff.pol = "provider_idempotency" ->
                 /\ SetAc(w, [ActorIdle EXCEPT !.pc = "exec", !.att = h.eff.att])
                 /\ UNCHANGED sVars
            [] OTHER ->
                 /\ CasThen(w, AuthorizeHead(h, h.eff.att + 1),
                            [ActorIdle EXCEPT !.pc = "exec", !.att = h.eff.att + 1])
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* The authorization CAS applied and its response was lost. The owner keeps
\* the attempt in memory and re-reads; finding it authorized, the retried
\* command answers AttemptAuthorized and the owner dispatches it. Only the
\* first attempt and an at_least_once retry are authorized by a write.
AuthorizeLostResponse(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ \/ O(w).eff.st = "intent"
       \/ /\ O(w).eff.st = "authorized" /\ O(w).eff.pol = "at_least_once"
          /\ O(w).eff.att < MaxAttempt
    /\ LET h == O(w)
           n == IF h.eff.st = "intent" THEN 1 ELSE h.eff.att + 1
       IN /\ Store!PutCasVersionLostResponse(HeadKey, ac[w].ver, AuthorizeHead(h, n))
          /\ SetAc(w, [ActorIdle EXCEPT !.held = "authz", !.att = n])
    /\ UNCHANGED <<calls, crashed, cl, pol>>

ResumeAuthorized(w) ==
    /\ ac[w].pc = "ready"
    /\ Fresh(w)
    /\ ac[w].held = "authz"
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).eff.st = "authorized" /\ O(w).eff.att = ac[w].att
    /\ SetAc(w, [ActorIdle EXCEPT !.pc = "exec", !.att = ac[w].att])
    /\ UNCHANGED <<sVars, calls, crashed, cl, pol>>

\* The effect's result replays from the journal; the handler finishes and
\* writes the turn output (state snapshot and result) with CreateIfAbsent.
PutTurnOutput(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).eff.st = "completed"
    /\ Store!PutCreateIfAbsent(OutKey(O(w).active.turn), OutKey(O(w).active.turn))
    /\ SetAc(w, [ac[w] EXCEPT !.pc = "out"])
    /\ UNCHANGED <<calls, crashed, cl, pol>>

CompleteTurn(w) ==
    /\ ac[w].pc = "out"
    /\ CasThen(w, EndTurn(O(w), "completed", w), ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* A cancel request read on the turn: the owner stops the handler and
\* commits; the commit publishes the input cancelled. Refused while the
\* effect is in doubt (NoEffectInDoubt); an attempt authorized without a
\* result does not hold it, and its late result is still recorded.
CommitCancelled(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ O(w).active.turn # 0
    /\ O(w).active.creq
    /\ ~O(w).active.block
    /\ O(w).eff.st # "indoubt"
    /\ CasThen(w, EndTurn(O(w), "completed", w), ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* A handler failure. Refused while the effect is in doubt or authorized
\* without a result (NoEffectInDoubt, NoAuthorizedAttemptPending), so a
\* retry never re-dispatches an attempt the failed turn authorized. With a
\* cancel request it consumes the input cancelled; below MaxTurns it is
\* retryable and keeps transcript and journal for the next turn; at the last
\* turn it is terminal. A blocked turn can always fail this way.
FailTurn(w) ==
    /\ Ready(w)
    /\ Own(w)
    /\ O(w).active.turn # 0
    /\ O(w).eff.st \notin {"authorized", "indoubt"}
    /\ LET h == O(w)
       IN CasThen(w,
            IF h.active.creq \/ h.failed + 1 >= MaxTurns
              THEN [EndTurn(h, "failed", w) EXCEPT !.failed = @ + 1]
              ELSE [Bump(h) EXCEPT !.active = NoTurn, !.failed = @ + 1],
            ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* --- External execution ---------------------------------------------------

PhysCalls == {i \in 1..Len(calls) : calls[i].phys}

\* Dispatch the held attempt under the effect id. The provider executes it,
\* except that under provider_idempotency a repeat of the effect id executes
\* nothing and answers the first execution's result.
Dispatch(a) ==
    /\ ac[a].pc = "exec"
    /\ Len(calls) < MaxCalls
    /\ LET dedup == pol = "provider_idempotency" /\ PhysCalls # {}
       IN /\ calls' = Append(calls, [att |-> ac[a].att, phys |-> ~dedup])
          /\ SetAc(a, [ac[a] EXCEPT !.pc = "await",
                         !.val = IF dedup THEN CHOOSE i \in PhysCalls : TRUE
                                 ELSE Len(calls) + 1])
    /\ UNCHANGED <<sVars, crashed, cl, pol>>

\* The provider's response arrives; the dispatcher holds the result to
\* record it.
Respond(a) ==
    /\ ac[a].pc = "await"
    /\ SetAc(a, [ac[a] EXCEPT !.pc = "idle", !.held = "result", !.ver = 0, !.obs = NoC])
    /\ UNCHANGED <<sVars, calls, crashed, cl, pol>>

\* The response is lost: the attempt may or may not have executed, and the
\* dispatcher forgets it. Only while the attempt bound (DispatchBudgetLeft)
\* is not spent, so that the provider eventually answers (the liveness
\* assumption).
LoseResponse(a) ==
    /\ ac[a].pc = "await"
    /\ IF pol = "provider_idempotency" THEN Len(calls) < MaxAttempt
       ELSE ac[a].att < MaxAttempt
    /\ SetAc(a, ActorIdle)
    /\ UNCHANGED <<sVars, calls, crashed, cl, pol>>

\* Record a held provider result in the journal (RecordEffectResult; fenced
\* by incarnation only, so a displaced owner records it too). A result the
\* kernel does not admit answers without a write: on a completed effect the
\* same result is idempotent and another attempt's is LateResultIgnored, and
\* an older attempt's result or one for a cancelled input is
\* LateResultIgnored too, kept as evidence outside HEAD.
RecordHeldResult(a) ==
    /\ ac[a].pc = "ready"
    /\ Fresh(a)
    /\ ac[a].held = "result"
    /\ IF ResultAdmitted(O(a), ac[a].att)
         THEN CasThen(a, RecordResultHead(O(a), ac[a].att, ac[a].val), ActorIdle)
         ELSE /\ SetAc(a, ActorIdle)
              /\ UNCHANGED sVars
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* The result-completion endpoint: an authenticated result for a dispatched
\* attempt, posted after its dispatcher lost the response or died. It writes
\* HEAD only when the kernel admits it; any other result is evidence stored
\* outside HEAD, which is a stutter here.
ResultCallback(i) ==
    /\ RootUp
    /\ i \in PhysCalls
    /\ ResultAdmitted(Root, calls[i].att)
    /\ Store!PutCasVersion(HeadKey, Ver, RecordResultHead(Root, calls[i].att, i))
    /\ UNCHANGED <<ac, calls, crashed, cl, pol>>

\* --- Crash and recovery ---------------------------------------------------

\* A crash loses the worker's memory (the attempt it held, a response it had
\* not recorded) and nothing durable. Each worker crashes at most once.
WorkerCrash(w) ==
    /\ ac[w].pc \notin {"idle", "down"} \/ ac[w].held # "none"
    /\ w \notin crashed
    /\ crashed' = crashed \cup {w}
    /\ SetAc(w, [ActorIdle EXCEPT !.pc = "down"])
    /\ UNCHANGED <<sVars, calls, cl, pol>>

Recover(w) ==
    /\ ac[w].pc = "down"
    /\ SetAc(w, ActorIdle)
    /\ UNCHANGED <<sVars, calls, crashed, cl, pol>>

\* --- Client cancel --------------------------------------------------------

\* A running turn's input is marked; a waiting one (between a retryable
\* failure and the retry) is consumed cancelled at once.
Cancel ==
    /\ cl = "idle"
    /\ RootUp
    /\ Root.inbox # {}
    /\ LET h == Root
       IN Store!PutCasVersion(HeadKey, Ver,
            IF h.active.turn # 0
              THEN [Bump(h) EXCEPT !.active.creq = TRUE]
              ELSE [Bump(h) EXCEPT !.inbox = {}, !.step = NoStep,
                      !.results = @ \cup {[kind |-> "cancelled", turn |-> 0, epoch |-> 0,
                                           owner |-> NoWorker, sv |-> h.sv]}])
    /\ cl' = "done"
    /\ UNCHANGED <<ac, calls, crashed, pol>>

\* --- Admin reconciliation -------------------------------------------------

\* Attach a verified result: the admin looked up what the provider did for
\* the current attempt. The audit object is written before the CAS.
AdminAuditResult(i) ==
    /\ Ready(Admin)
    /\ Reconcilable(O(Admin))
    /\ i \in PhysCalls
    /\ calls[i].att = O(Admin).eff.att
    /\ Store!PutCreateIfAbsent(AuditKey("result", calls[i].att), "audit")
    /\ SetAc(Admin, [ac[Admin] EXCEPT !.pc = "auditR", !.att = calls[i].att, !.val = i])
    /\ UNCHANGED <<calls, crashed, cl, pol>>

AdminRecordResult ==
    /\ ac[Admin].pc = "auditR"
    /\ CasThen(Admin, RecordResultHead(O(Admin), ac[Admin].att, ac[Admin].val), ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* Authorize another attempt, which the reconciler then dispatches itself.
AdminAuditAttempt ==
    /\ Ready(Admin)
    /\ Reconcilable(O(Admin))
    /\ O(Admin).eff.att < MaxAttempt
    /\ Store!PutCreateIfAbsent(AuditKey("attempt", O(Admin).eff.att + 1), "audit")
    /\ SetAc(Admin, [ac[Admin] EXCEPT !.pc = "auditA", !.att = O(Admin).eff.att + 1])
    /\ UNCHANGED <<calls, crashed, cl, pol>>

AdminAuthorize ==
    /\ ac[Admin].pc = "auditA"
    /\ CasThen(Admin, AuthorizeHead(O(Admin), ac[Admin].att),
               [ActorIdle EXCEPT !.pc = "exec", !.att = ac[Admin].att])
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* --- Specification --------------------------------------------------------

Step ==
    \/ CreateHead
    \/ Cancel
    \/ \E a \in Actors :
         \/ Read(a) \/ Dispatch(a) \/ Respond(a) \/ LoseResponse(a) \/ RecordHeldResult(a)
    \/ \E w \in Workers :
         \/ Takeover(w) \/ BeginTurn(w) \/ AuthorizeFirst(w) \/ Reauthorize(w)
         \/ AuthorizeLostResponse(w) \/ ResumeAuthorized(w) \/ PutTurnOutput(w)
         \/ CompleteTurn(w) \/ CommitCancelled(w) \/ FailTurn(w)
         \/ WorkerCrash(w) \/ Recover(w)
         \/ \E x \in Hashes :
              \/ RecordStep(w, x) \/ ReplayStepDrift(w, x)
              \/ CommitIntent(w, x) \/ ReplayIntentDrift(w, x)
    \/ \E i \in 1..MaxCalls : ResultCallback(i) \/ AdminAuditResult(i)
    \/ AdminRecordResult \/ AdminAuditAttempt \/ AdminAuthorize

Next == Step /\ CasWitness

Spec == Init /\ [][Next]_vars

\* Liveness assumptions: the store stays available (a transient failure is a
\* stutter), a crashed worker restarts, workers keep acting on the turn and
\* dispatch what they authorized, and the provider eventually answers (a
\* response is lost only below MaxAttempt). A handler that drifts on replay
\* is not forced to (drift is not fair), but a blocked turn is eventually
\* failed. The admin, the client's cancel and the result endpoint are not
\* fair: nothing obliges a person to reconcile.
Fairness ==
    /\ WF_vars(CreateHead /\ CasWitness)
    /\ \A w \in Workers :
         /\ WF_vars(Read(w) /\ CasWitness)
         /\ WF_vars(Takeover(w) /\ CasWitness)
         /\ WF_vars(BeginTurn(w) /\ CasWitness)
         /\ WF_vars((\E x \in Hashes : RecordStep(w, x)) /\ CasWitness)
         /\ WF_vars((\E x \in Hashes : CommitIntent(w, x)) /\ CasWitness)
         /\ WF_vars(AuthorizeFirst(w) /\ CasWitness)
         /\ WF_vars(Reauthorize(w) /\ CasWitness)
         /\ WF_vars(ResumeAuthorized(w) /\ CasWitness)
         /\ WF_vars(PutTurnOutput(w) /\ CasWitness)
         /\ WF_vars(CompleteTurn(w) /\ CasWitness)
         /\ WF_vars(CommitCancelled(w) /\ CasWitness)
         /\ WF_vars((FailTurn(w) /\ O(w).active.block) /\ CasWitness)
         /\ WF_vars(Dispatch(w) /\ CasWitness)
         /\ WF_vars(Respond(w) /\ CasWitness)
         /\ WF_vars(RecordHeldResult(w) /\ CasWitness)
         /\ WF_vars(Recover(w) /\ CasWitness)

FairSpec == Spec /\ Fairness

\* --- Safety properties ----------------------------------------------------

LC == lastCas
Wrote == LC # NoCas /\ LC.before # NoC
NewResults == LC.after.results \ LC.before.results

\* A step or intent key keeps the tool identity and input hash it was first
\* journaled with, and a recorded effect result is never rewritten: a
\* replay that disagrees blocks the turn instead. The journal never goes
\* back to a lower attempt.
StepTranscriptCompatible ==
    Wrote =>
        LET b == LC.before
            a == LC.after
        IN /\ (b.step.st = "completed" /\ a.step.st = "completed") => a.step.hash = b.step.hash
           /\ b.eff.st # "none" => a.eff.hash = b.eff.hash /\ a.eff.pol = b.eff.pol
           /\ b.eff.st = "completed" => a.eff = b.eff
           /\ a.eff.att >= b.eff.att

\* Every dispatch the provider saw was of an attempt the journal holds an
\* authorization for: the intent and the authorization committed first.
DispatchRequiresCommittedAuthorization ==
    \A i \in 1..Len(calls) :
        /\ RootUp
        /\ Root.eff.st \notin {"none", "intent"}
        /\ calls[i].att =< Root.eff.att

\* Under manual and at_least_once each authorized attempt is dispatched at
\* most once, and under manual no attempt after the first is authorized or
\* dispatched without a reconcile audit object, and no turn result lands
\* while the effect is in doubt. Under provider_idempotency the one attempt
\* is the only one ever authorized: a lost attempt is dispatched again
\* under the same effect id and attempt, and the provider executes at most
\* once. at_least_once executions are unconstrained
\* (DuplicateExecutionUnreachable is its probe).
PolicyControlsRedispatch ==
    /\ (RootUp /\ Root.eff.pol # "provider_idempotency") =>
         \A i, j \in 1..Len(calls) : i # j => calls[i].att # calls[j].att
    /\ (RootUp /\ Root.eff.pol = "manual") =>
         /\ \A i \in 1..Len(calls) : calls[i].att = 1 \/ AuditUp("attempt", calls[i].att)
         /\ Root.eff.att > 1 => AuditUp("attempt", Root.eff.att)
    /\ (RootUp /\ Root.eff.pol = "provider_idempotency") =>
         /\ Root.eff.att =< 1
         /\ \A i \in 1..Len(calls) : calls[i].att = 1
         \* True by construction: Dispatch marks a provider re-dispatch
         \* phys = FALSE whenever PhysCalls is non-empty, and no switch
         \* alters Dispatch, so the guarantee lives there and this conjunct
         \* restates it with no negative control of its own.
         /\ Cardinality(PhysCalls) =< 1
    /\ (Wrote /\ LC.before.eff.st = "indoubt") => NewResults = {}

\* A recorded result came from a physical execution of the attempt it is
\* recorded for, and that attempt is the journaled one: an older attempt's
\* result never becomes the effect's.
EffectResultFromExecution ==
    (RootUp /\ Root.eff.st = "completed") =>
        /\ Root.eff.rval \in PhysCalls
        /\ Root.eff.ratt = Root.eff.att
        /\ calls[Root.eff.rval].att = Root.eff.ratt

\* Effects form: a commit on a turn with a cancel request publishes it
\* cancelled; once the request is on the root the owner journals no new
\* step or intent and authorizes no new attempt (only a reconcile audit
\* object admits one); nothing rewrites a published result; and once the
\* input ended cancelled no write changes its effect, so a late effect
\* result after cancellation is evidence only.
CancellationRespected ==
    Wrote =>
        LET b == LC.before
            a == LC.after
        IN /\ b.results \subseteq a.results
           /\ InputCancelled(b) => a.eff = b.eff
           /\ b.active.creq =>
                /\ \A r \in NewResults : r.kind = "cancelled"
                /\ b.step.st = "none" => a.step.st = "none"
                /\ b.eff.st = "none" => a.eff.st = "none"
                /\ a.eff.att > b.eff.att => AuditUp("attempt", a.eff.att)

\* --- Liveness (conditional on Fairness) ------------------------------------

\* Blocked terminals: the effect awaits reconciliation (in doubt, or a
\* reconcile attempt whose outcome the admin has not settled), or the run
\* spent the model's attempt bound on an ambiguous attempt.
AwaitsReconciliation(h) == Reconcilable(h) \/ ReconcilerOwns(h)
AttemptBoundReached(h) == h.eff.st = "authorized" /\ ~DispatchBudgetLeft(h)

\* A turn blocked by replay drift while its attempt is authorized without a
\* result: fail_turn is refused (NoAuthorizedAttemptPending), and the
\* runtime authorizes and dispatches nothing on a blocked turn (its run_step
\* journals the step first, which require_running refuses on a blocked
\* turn; `authorize_attempt` itself does not read the block), so only the
\* result endpoint or the dispatcher's own response moves it. A design gap
\* the README reports.
BlockedWithPendingAttempt(h) == h.active.block /\ h.eff.st = "authorized"

\* The input is eventually settled, or the turn waits on a blocked effect.
InputSettledOrEffectBlocked ==
    (RootUp /\ Root.inbox # {}) ~>
        (RootUp /\ (Root.inbox = {} \/ AwaitsReconciliation(Root) \/ AttemptBoundReached(Root)
                                     \/ BlockedWithPendingAttempt(Root)))

=============================================================================
