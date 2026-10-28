---------------------------- MODULE AgentProtocol ----------------------------
(***************************************************************************)
(* Agent HEAD publication, enqueue, ownership, turns, cancellation and     *)
(* timers (ADR-3331 decisions 3 to 8).                                     *)
(*                                                                         *)
(* ABSTRACTION BOUNDARY                                                    *)
(*                                                                         *)
(* Modelled: one mutable HEAD per agent key, written CreateIfAbsent once   *)
(* and CasVersion afterwards; immutable dependency objects (request        *)
(* payloads, turn output objects) written CreateIfAbsent before the CAS    *)
(* that links them; client enqueue with a request id and payload hash and  *)
(* its receipt; the unknown outcome after a lost CAS response and its      *)
(* resolution by re-reading HEAD; takeover as an epoch-advancing CAS;      *)
(* turns bound to an accepted input, completed or failed in one CAS, and   *)
(* rebased across a concurrent write only when epoch, active turn and base *)
(* state version still match; worker crash and recovery; cancellation as  *)
(* a HEAD command; timer generations, wake hints and the reconciliation    *)
(* sweep; tombstone and recreate with an incarnation bump.                 *)
(*                                                                         *)
(* Every dependency write, HEAD CAS and crash is its own action. A read   *)
(* is folded into the step that acts on it only where no other write sits *)
(* between them (CasNow, ReadHead explain why that loses no behaviour).   *)
(* Every store call goes through RavelObjectStore, instantiated unchanged.*)
(*                                                                         *)
(* Assumed, not checked:                                                   *)
(*   - The store honours its contract (common/RavelObjectStore.tla).      *)
(*   - A transient store failure applies nothing and leaves the caller    *)
(*     free to retry the same step; it is a stuttering step here. A lost  *)
(*     response on a HEAD CAS is modelled explicitly, since it applies    *)
(*     the effect and leaves the caller an unknown outcome.               *)
(*   - Handler code is a black box: a turn either completes (one new      *)
(*     state version) or fails (state unchanged). Nothing here claims     *)
(*     that worker code is deterministic.                                 *)
(*   - No real time. Takeover timing, lease expiry and timer due-ness are *)
(*     nondeterministic environment steps; safety never reads a clock.    *)
(*                                                                         *)
(* Out of scope (AgentEffects and AgentIntegrated, task T17): step        *)
(* intents, attempt authorization, external execution, replay and manual  *)
(* resolution; history paging; quotas.                                    *)
(*                                                                         *)
(* Witnesses: `lastCas` holds the HEAD content and version read from the  *)
(* store immediately before and after the HEAD write the step just taken  *)
(* made, and NoCas after a step that wrote no HEAD (the write was checked *)
(* in the state right after it, and forgetting it merges states). Next    *)
(* derives it from store and store' (CasWitness); no action sets it;      *)
(* `acked` holds the handle each client was returned. Every invariant     *)
(* reads the store, lastCas or acked, never a flag the action under check *)
(* sets about itself.                                                     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Tenants,      \* tenant hashes
    Agents,       \* agent ids; an agent key is a (tenant, agent) pair
    Clients,      \* senders; each sends one request and may cancel it
    Workers,      \* runtime workers that take over agents and run turns
    Inputs,       \* request payloads
    RequestIds,   \* client request ids
    Hashes,       \* payload hashes (content addresses of payload objects)
    MaxRevision,  \* bound on optional root writes (cancel, destroy,
                  \* recreate): each is enabled only below this revision
    MaxEpoch,     \* bound on takeovers
    MaxTimerGen,  \* bound on timer generations
    NoC,          \* sentinels, each a model value: absent content,
    NoWorker,     \* no owner,
    NoHandle,     \* no handle returned yet,
    NoCas,        \* no HEAD write yet,
    TimerMsg      \* and the payload hash of a timer message

ASSUME Tenants # {} /\ Agents # {} /\ Clients # {} /\ Workers # {}
ASSUME Inputs # {} /\ RequestIds # {}
ASSUME Cardinality(Hashes) >= Cardinality(Inputs)
ASSUME MaxRevision \in Nat /\ MaxEpoch \in Nat /\ MaxTimerGen \in Nat

AKs == Tenants \X Agents

\* Content addressing: each payload has its own hash.
PayloadHash ==
    CHOOSE f \in [Inputs -> Hashes] : \A x, y \in Inputs : x # y => f[x] # f[y]

\* Each client is accepted at most once per incarnation and each timer
\* generation enqueues at most once, so no incarnation accepts more inputs
\* than MaxInputs. Every turn binds a distinct input and incarnations are
\* bounded by MaxRevision, so turn ids never exceed MaxTurns.
MaxInputs == Cardinality(Clients) + MaxTimerGen
MaxTurns  == Cardinality(Clients) * MaxRevision + MaxTimerGen
TurnIds   == 1..MaxTurns
OutId(n)  == <<"out", n>>
ObjIds    == Hashes \cup {OutId(n) : n \in TurnIds}

HeadKey(ak)   == <<"head", ak[1], ak[2]>>
ObjKey(ak, o) == <<"obj", ak[1], ak[2], o>>
HintKey(ak, g) == <<"wake", ak[1], ak[2], g>>

AllKeys ==
    {HeadKey(ak) : ak \in AKs}
    \cup {ObjKey(ak, o) : ak \in AKs, o \in ObjIds}
    \cup {HintKey(ak, g) : ak \in AKs, g \in 1..MaxTimerGen}

\* --- HEAD content ---------------------------------------------------------

NoTurn == [turn |-> 0, seq |-> 0, base |-> 0, inc |-> 0]

TimerStates == {"none", "armed", "delivered", "cancelled"}

InputRecs   == [seq: Nat, src: ({"client"} \X RequestIds) \cup ({"timer"} \X Nat),
                hash: Hashes \cup {TimerMsg}, inc: Nat]
ReceiptRecs == [rid: RequestIds, hash: Hashes, seq: Nat, inc: Nat]
ResultRecs  == [seq: Nat, kind: {"completed", "failed", "cancelled"}, turn: Nat,
                epoch: Nat, owner: Workers \cup {NoWorker}, sv: Nat, inc: Nat]

HeadRecs ==
    [tenant: Tenants, agent: Agents, inc: Nat, tomb: BOOLEAN, rev: Nat,
     owner: Workers \cup {NoWorker}, epoch: Nat, sv: Nat,
     stateRef: ObjIds \cup {NoC}, inputs: SUBSET InputRecs, inbox: SUBSET Nat,
     receipts: SUBSET ReceiptRecs, results: SUBSET ResultRecs, nextSeq: Nat,
     active: [turn: Nat, seq: Nat, base: Nat, inc: Nat], turns: Nat,
     cancels: SUBSET Nat, timer: [gen: Nat, st: TimerStates]]

\* A hint names the incarnation and generation it was written for; the key
\* names only the generation, so a later incarnation's hint overwrites it.
HintContents == {<<"hint", i, g>> : i \in Nat, g \in 1..MaxTimerGen}

AgentContent == {NoC} \cup ObjIds \cup HintContents \cup HeadRecs

VARIABLES
    store, lastModified, versionCounter, uploads, listState,  \* the store
    cl,           \* [Clients -> client operation in flight]
    wk,           \* [Workers -> worker operation in flight]
    co,           \* [AKs -> highest timer generation the scheduler hinted
                  \*         in the current incarnation]
    crashedOnce,  \* SUBSET Workers: model bound, each worker crashes once
    due,          \* [AKs -> highest due timer generation] (environment)
    acked,        \* [Clients -> handle returned to the client, or NoHandle]
    lastCas       \* witness: HEAD before and after this step's HEAD write

Store == INSTANCE RavelObjectStore
             WITH Keys <- AllKeys, Content <- AgentContent,
                  NoContent <- NoC, Clients <- Clients

sVars == <<store, lastModified, versionCounter, uploads, listState>>
aVars == <<cl, wk, co, crashedOnce, due, acked, lastCas>>
vars  == <<sVars, aVars>>

Min(S) == CHOOSE x \in S : \A y \in S : x =< y

Head(ak)  == Store!ContentOf(HeadKey(ak))
HeadUp(ak) == Store!Present(HeadKey(ak))
HintInc(ak, g) == Store!ContentOf(HintKey(ak, g))[2]

FreshHead(ak, inc, rev, owner, epoch, turns, gen) ==
    [tenant |-> ak[1], agent |-> ak[2], inc |-> inc, tomb |-> FALSE,
     rev |-> rev, owner |-> owner, epoch |-> epoch, sv |-> 0,
     stateRef |-> NoC, inputs |-> {}, inbox |-> {}, receipts |-> {},
     results |-> {}, nextSeq |-> 0, active |-> NoTurn, turns |-> turns,
     cancels |-> {}, timer |-> [gen |-> gen, st |-> "none"]]

\* The witness is derived once per step, in Next, from store and store': no
\* action sets it, so no action can describe its own write. Every action
\* writes at most one HEAD; the witness records that HEAD's content and
\* version on both sides of the step, or NoCas when no HEAD changed.
ChangedHeads == {ak \in AKs : store'[HeadKey(ak)] # store[HeadKey(ak)]}

CasWitness ==
    lastCas' =
        IF ChangedHeads = {} THEN NoCas
        ELSE LET ak == CHOOSE x \in ChangedHeads : TRUE
             IN [ak |-> ak,
                 before |-> IF HeadUp(ak) THEN Head(ak) ELSE NoC,
                 vb |-> Store!VersionOf(HeadKey(ak)),
                 after |-> store'[HeadKey(ak)].content,
                 va |-> store'[HeadKey(ak)].version]

CasOk(ak, v) == Store!CasResult(HeadKey(ak), v) = "Ok"

\* A step that reads HEAD and CASes the version it read, with no other write
\* in between, is one action here. A CAS on a stale read fails with
\* PreconditionFailed and changes nothing, and the caller re-reads and
\* retries, so the failed attempt is a stutter and folding the read into the
\* CAS loses no behaviour. CasNow is that folded CAS on the current version.
\* Where a dependency PUT sits between the read and the CAS (enqueue, turn
\* completion) the two stay separate actions.
CasNow(ak, new) == Store!PutCasVersion(HeadKey(ak), Store!VersionOf(HeadKey(ak)), new)

\* --- Root transitions (pure functions of an observed HEAD) ---------------

NewReceipt(h, rid, hh) == [rid |-> rid, hash |-> hh, seq |-> h.nextSeq, inc |-> h.inc]

EnqueueHead(h, rid, hh) ==
    [h EXCEPT !.rev = @ + 1,
              !.inputs = @ \cup {[seq |-> h.nextSeq, src |-> <<"client", rid>>,
                                  hash |-> hh, inc |-> h.inc]},
              !.inbox = @ \cup {h.nextSeq},
              !.receipts = @ \cup {NewReceipt(h, rid, hh)},
              !.nextSeq = @ + 1]

\* ctx: the turn the command is bound to (turn, seq, base, epoch, inc).
TurnResult(h, ctx, kind, w) ==
    [seq |-> ctx.seq, kind |-> kind, turn |-> ctx.turn, epoch |-> ctx.epoch,
     owner |-> w, sv |-> IF kind = "completed" THEN h.sv + 1 ELSE h.sv,
     inc |-> ctx.inc]

CompleteHead(h, ctx, kind, w) ==
    [h EXCEPT !.rev = @ + 1,
              !.inbox = @ \ {ctx.seq},
              !.active = NoTurn,
              !.results = @ \cup {TurnResult(h, ctx, kind, w)},
              !.sv = IF kind = "completed" THEN @ + 1 ELSE @,
              !.stateRef = IF kind = "completed" THEN OutId(ctx.turn) ELSE @]

CancelHead(h, s, inc) ==
    [h EXCEPT !.rev = @ + 1,
              !.inbox = @ \ {s},
              !.cancels = @ \cup {s},
              !.results = @ \cup {[seq |-> s, kind |-> "cancelled", turn |-> 0,
                                   epoch |-> 0, owner |-> NoWorker, sv |-> h.sv,
                                   inc |-> inc]},
              !.active = IF h.active.turn # 0 /\ h.active.seq = s THEN NoTurn ELSE @]

TimerInput(h, g) ==
    [seq |-> h.nextSeq, src |-> <<"timer", g>>, hash |-> TimerMsg, inc |-> h.inc]

FireHead(h, g) ==
    [h EXCEPT !.rev = @ + 1,
              !.timer = [gen |-> g, st |-> "delivered"],
              !.inputs = @ \cup {TimerInput(h, g)},
              !.inbox = @ \cup {h.nextSeq},
              !.nextSeq = @ + 1]

\* --- Initial state --------------------------------------------------------

ClientIdle == [pc |-> "start", ver |-> 0, obs |-> NoC, handle |-> NoHandle]

WorkerIdle ==
    [pc |-> "idle", ak |-> CHOOSE ak \in AKs : TRUE, ver |-> 0, obs |-> NoC,
     turn |-> 0, seq |-> 0, base |-> 0, epoch |-> 0, inc |-> 0]


\* Each client picks its agent key, request id and payload. The first client's
\* choice is pinned: every other choice is the same up to renaming, so this
\* loses no behaviour and divides the initial states by |AKs x RequestIds x
\* Inputs|.
FirstClient == CHOOSE c \in Clients : TRUE
FirstChoice ==
    [ak |-> CHOOSE ak \in AKs : TRUE, rid |-> CHOOSE r \in RequestIds : TRUE,
     inp |-> CHOOSE i \in Inputs : TRUE]

Init ==
    /\ Store!StoreInit
    /\ cl \in [Clients -> [pc: {"start"}, ak: AKs, rid: RequestIds, inp: Inputs,
                           ver: {0}, obs: {NoC}, handle: {NoHandle}]]
    /\ cl[FirstClient].ak = FirstChoice.ak
    /\ cl[FirstClient].rid = FirstChoice.rid
    /\ cl[FirstClient].inp = FirstChoice.inp
    /\ wk \in [Workers -> {WorkerIdle}]
    /\ co \in [AKs -> {0}]
    /\ crashedOnce = {}
    /\ due \in [AKs -> {0}]
    /\ acked \in [Clients -> {NoHandle}]
    /\ lastCas = NoCas

\* --- Agent creation, destroy and recreate ---------------------------------

CreateHead(ak) ==
    /\ ~HeadUp(ak)
    /\ Store!PutCreateIfAbsent(HeadKey(ak), FreshHead(ak, 1, 1, NoWorker, 0, 0, 0))
    /\ UNCHANGED <<cl, wk, co, crashedOnce, due, acked>>

\* Administrative writes are one read and one CAS on the version read.
AdminCas(ak, new) ==
    /\ CasNow(ak, new)
    /\ UNCHANGED <<cl, wk, co, crashedOnce, due, acked>>

\* Destroy sets the tombstone and leaves an armed timer armed, as
\* `Kernel::tombstone` does; FireTimer refuses to fire on a tombstoned root.
Destroy(ak) ==
    /\ HeadUp(ak)
    /\ Head(ak).rev < MaxRevision
    /\ ~Head(ak).tomb
    /\ AdminCas(ak, [Head(ak) EXCEPT !.rev = @ + 1, !.tomb = TRUE])

\* Recreate on a tombstoned HEAD bumps the incarnation in the same HEAD,
\* clears the owner and starts an empty mailbox and timer generation 0, as
\* `Kernel::create` does. The epoch is kept, so an owner of the old
\* incarnation stays fenced. Timer generations restart because a timer's
\* identity carries its incarnation; the scheduler's and the environment's
\* per-agent generation counters restart with them. `turns` only names
\* output objects and is kept so a new incarnation never reuses one.
Recreate(ak) ==
    /\ HeadUp(ak)
    /\ Head(ak).rev < MaxRevision
    /\ Head(ak).tomb
    /\ LET h == Head(ak)
       IN /\ CasNow(ak, FreshHead(ak, h.inc + 1, h.rev + 1, NoWorker, h.epoch, h.turns, 0))
          /\ co' = [co EXCEPT ![ak] = 0]
          /\ due' = [due EXCEPT ![ak] = 0]
    /\ UNCHANGED <<cl, wk, crashedOnce, acked>>

\* --- Client: enqueue ------------------------------------------------------

CK(c) == HeadKey(cl[c].ak)
Hc(c) == PayloadHash[cl[c].inp]
SameRid(c, h) == {r \in h.receipts : r.rid = cl[c].rid}

SetCl(c, f) == cl' = [cl EXCEPT ![c] = f]

\* Respond to the client with a handle and forget the in-flight operation.
AckWith(c, handle) ==
    /\ acked' = [acked EXCEPT ![c] = handle]
    /\ SetCl(c, [cl[c] EXCEPT !.pc = "acked", !.ver = 0, !.obs = NoC, !.handle = handle])

\* GET HEAD (first attempt, or the re-read after an unknown outcome) and act
\* on it. A tombstoned HEAD answers AgentGone. A receipt for the request id
\* answers the recorded handle when the hash matches (idempotent retry) and
\* IdempotencyConflict when it does not. Otherwise PUT the payload with
\* CreateIfAbsent and keep the version read as the CAS token.
\*
\* Folding: the read answers from its own result, and a write landing
\* between the read and the payload PUT is the same as one landing between
\* the PUT and the CAS (the CAS fails either way), so read, response and
\* PUT are one step. AlreadyExists on the PUT is a success, since the key is
\* the payload's content hash; for the same reason a lost response on it is
\* not its own action: the retry answers AlreadyExists.
ReadHead(c) ==
    /\ cl[c].pc \in {"start", "unknown"}
    /\ HeadUp(cl[c].ak)
    /\ LET ak == cl[c].ak
           h == Head(ak)
       IN IF h.tomb
            THEN /\ SetCl(c, [cl[c] EXCEPT !.pc = "gone", !.ver = 0, !.obs = NoC])
                 /\ UNCHANGED <<sVars, acked>>
          ELSE IF SameRid(c, h) # {}
            THEN /\ LET r == CHOOSE x \in SameRid(c, h) : TRUE
                    IN IF r.hash = Hc(c)
                         THEN AckWith(c, [ak |-> ak, rcpt |-> r])
                         ELSE /\ SetCl(c, [cl[c] EXCEPT !.pc = "conflict", !.ver = 0,
                                                        !.obs = NoC])
                              /\ UNCHANGED acked
                 /\ UNCHANGED sVars
            ELSE /\ Store!PutCreateIfAbsent(ObjKey(ak, Hc(c)), Hc(c))
                 /\ SetCl(c, [cl[c] EXCEPT !.pc = "dep", !.ver = Store!VersionOf(CK(c)),
                                           !.obs = h])
                 /\ UNCHANGED acked
    /\ UNCHANGED <<wk, co, crashedOnce, due>>

\* The enqueue CAS: links the payload, allocates accept_seq, records the
\* receipt. On success the client is answered with the handle it wrote (the
\* response is part of the step: a gap before it only delays the answer).
\* PreconditionFailed sends the client back to re-read.
EnqueueCas(c) ==
    LET ak == cl[c].ak
        h == cl[c].obs
        new == EnqueueHead(h, cl[c].rid, Hc(c))
    IN /\ Store!PutCasVersion(CK(c), cl[c].ver, new)
       /\ IF CasOk(ak, cl[c].ver)
            THEN AckWith(c, [ak |-> ak, rcpt |-> NewReceipt(h, cl[c].rid, Hc(c))])
            ELSE /\ SetCl(c, [cl[c] EXCEPT !.pc = "start", !.ver = 0, !.obs = NoC])
                 /\ UNCHANGED acked
    /\ UNCHANGED <<wk, co, crashedOnce, due>>

CasHead(c) ==
    /\ cl[c].pc = "dep"
    /\ EnqueueCas(c)

\* The CAS response is lost: the effect (if any) is applied and the client
\* sees an unknown outcome, never a success.
CasHeadLostResponse(c) ==
    /\ cl[c].pc = "dep"
    /\ Store!PutCasVersionLostResponse(CK(c), cl[c].ver, EnqueueHead(cl[c].obs, cl[c].rid, Hc(c)))
    /\ SetCl(c, [cl[c] EXCEPT !.pc = "unknown", !.ver = 0, !.obs = NoC])
    /\ UNCHANGED <<wk, co, crashedOnce, due, acked>>

\* --- Client: cancel -------------------------------------------------------

\* Cancellation linearizes at HEAD. A pending input is consumed with a
\* cancelled result; an active one is consumed too and its turn cleared, so
\* the owner's completion CAS fails and its rebase finds the turn gone. An
\* input that already has a result answers AlreadyCompleted. The command
\* carries the handle's incarnation; a tombstoned HEAD or another
\* incarnation answers AgentGone. Those two answers write nothing, so only
\* the cancel that writes is an action. Read and CAS are one step (CasNow).
Cancel(c) ==
    /\ cl[c].pc = "acked"
    /\ HeadUp(cl[c].ak)
    /\ LET ak == cl[c].ak
           h == Head(ak)
           s == acked[c].rcpt.seq
       IN /\ ~h.tomb
          /\ h.inc = acked[c].rcpt.inc
          /\ ~\E r \in h.results : r.seq = s
          /\ h.rev < MaxRevision
          /\ CasNow(ak, CancelHead(h, s, acked[c].rcpt.inc))
    /\ SetCl(c, [cl[c] EXCEPT !.pc = "done"])
    /\ UNCHANGED <<wk, co, crashedOnce, due, acked>>

\* --- Worker ---------------------------------------------------------------

WK(w) == HeadKey(wk[w].ak)
Ob(w) == wk[w].obs

SetWk(w, f) == wk' = [wk EXCEPT ![w] = f]

\* Owner commands are validated against the epoch, owner and turn of the root
\* they read, because their CAS pins that root's version. Takeover,
\* BeginTurn, ScheduleTimer, CancelTimer and the rebase read and CAS in one
\* step (CasNow). A turn's completion is two steps: its output object is
\* written between the read and the CAS, and WorkerCas is that CAS on the
\* version read before the output PUT.
WorkerCas(w, new, okPc, failPc) ==
    /\ Store!PutCasVersion(WK(w), wk[w].ver, new)
    /\ IF CasOk(wk[w].ak, wk[w].ver)
         THEN SetWk(w, IF okPc = "idle" THEN WorkerIdle ELSE [wk[w] EXCEPT !.pc = okPc])
         ELSE SetWk(w, IF failPc = "idle" THEN WorkerIdle ELSE [wk[w] EXCEPT !.pc = failPc])

\* Claim an agent with work (pending input or an armed timer) by advancing
\* its epoch.
Takeover(w, ak) ==
    /\ wk[w].pc = "idle"
    /\ HeadUp(ak)
    /\ LET h == Head(ak)
       IN /\ ~h.tomb
          /\ h.owner # w
          /\ h.epoch < MaxEpoch
          /\ (h.inbox # {} \/ h.timer.st = "armed")
          /\ CasNow(ak, [h EXCEPT !.rev = @ + 1, !.epoch = @ + 1, !.owner = w])
    /\ UNCHANGED <<cl, wk, co, crashedOnce, due, acked>>

\* Bind the lowest accepted input to a fresh turn id and the base state
\* version. The owner then resumes it from HEAD like any later owner would.
BeginTurn(w, ak) ==
    /\ wk[w].pc = "idle"
    /\ HeadUp(ak)
    /\ LET h == Head(ak)
       IN /\ ~h.tomb
          /\ h.owner = w
          /\ h.active.turn = 0
          /\ h.inbox # {}
          /\ CasNow(ak, [h EXCEPT !.rev = @ + 1, !.turns = @ + 1,
                           !.active = [turn |-> h.turns + 1, seq |-> Min(h.inbox),
                                       base |-> h.sv, inc |-> h.inc]])
    /\ UNCHANGED <<cl, wk, co, crashedOnce, due, acked>>

\* GET HEAD, run its active turn (the handler is a black box) and PUT the
\* output object (new state snapshot and result) with CreateIfAbsent. The
\* output is a function of the turn, so a resumed turn writes the same key.
\* The version read is the token for the completion CAS.
PutTurnOutput(w, ak) ==
    /\ wk[w].pc = "idle"
    /\ HeadUp(ak)
    /\ LET h == Head(ak)
           a == h.active
       IN /\ ~h.tomb
          /\ h.owner = w
          /\ a.turn # 0
          /\ Store!PutCreateIfAbsent(ObjKey(ak, OutId(a.turn)), OutId(a.turn))
          /\ SetWk(w, [pc |-> "dep", ak |-> ak, ver |-> Store!VersionOf(HeadKey(ak)),
                       obs |-> h, turn |-> a.turn, seq |-> a.seq, base |-> a.base,
                       epoch |-> h.epoch, inc |-> a.inc])
    /\ UNCHANGED <<cl, co, crashedOnce, due, acked>>

\* The final turn CAS: new state, consumed input and its result together.
CompleteTurn(w) ==
    /\ wk[w].pc = "dep"
    /\ WorkerCas(w, CompleteHead(Ob(w), wk[w], "completed", w), "idle", "rebase")
    /\ UNCHANGED <<cl, co, crashedOnce, due, acked>>

CompleteTurnLostResponse(w) ==
    /\ wk[w].pc = "dep"
    /\ Store!PutCasVersionLostResponse(WK(w), wk[w].ver, CompleteHead(Ob(w), wk[w], "completed", w))
    /\ SetWk(w, [wk[w] EXCEPT !.pc = "rebase"])
    /\ UNCHANGED <<cl, co, crashedOnce, due, acked>>

\* A handler failure consumes the input with a failure result and leaves the
\* state unchanged. A failed CAS leaves the turn active for a later attempt.
FailTurn(w) ==
    /\ wk[w].pc = "dep"
    /\ WorkerCas(w, CompleteHead(Ob(w), wk[w], "failed", w), "idle", "idle")
    /\ UNCHANGED <<cl, co, crashedOnce, due, acked>>

OwnResult(w, h) ==
    \E r \in h.results : r.seq = wk[w].seq /\ r.turn = wk[w].turn

\* After a failed or unknown completion, re-read HEAD. If it carries this
\* turn's result, the commit landed. Otherwise reapply the transition on the
\* new root only when owner, epoch, active turn and base state version still
\* match; anything else is OwnershipLost.
CompleteTurnRebase(w) ==
    /\ wk[w].pc = "rebase"
    /\ HeadUp(wk[w].ak)
    /\ LET ak == wk[w].ak
           h == Head(ak)
       IN IF ~OwnResult(w, h) /\ ~h.tomb /\ h.owner = w /\ h.epoch = wk[w].epoch
                /\ h.active.turn = wk[w].turn /\ h.sv = wk[w].base
            THEN CasNow(ak, CompleteHead(h, wk[w], "completed", w))
            ELSE UNCHANGED sVars
    /\ SetWk(w, WorkerIdle)
    /\ UNCHANGED <<cl, co, crashedOnce, due, acked>>

ScheduleTimer(w, ak) ==
    /\ wk[w].pc = "idle"
    /\ HeadUp(ak)
    /\ LET h == Head(ak)
       IN /\ ~h.tomb
          /\ h.owner = w
          /\ h.timer.st # "armed"
          /\ h.timer.gen < MaxTimerGen
          /\ CasNow(ak, [h EXCEPT !.rev = @ + 1,
                           !.timer = [gen |-> h.timer.gen + 1, st |-> "armed"]])
    /\ UNCHANGED <<cl, wk, co, crashedOnce, due, acked>>

\* Cancel an armed generation. CancelTimer is enabled only while the timer is
\* armed, so the AlreadyFired outcome of ADR-3331 decision 8 is not modelled
\* here (the README lists it under what is not modelled).
CancelTimer(w, ak) ==
    /\ wk[w].pc = "idle"
    /\ HeadUp(ak)
    /\ LET h == Head(ak)
       IN /\ ~h.tomb
          /\ h.owner = w
          /\ h.timer.st = "armed"
          /\ CasNow(ak, [h EXCEPT !.rev = @ + 1, !.timer.st = "cancelled"])
    /\ UNCHANGED <<cl, wk, co, crashedOnce, due, acked>>

\* A crash loses the worker's in-memory operation and nothing durable; the
\* restarted worker starts from idle. The downtime between crash and restart
\* is a run of other processes' steps, which interleaving already covers. A
\* crash is taken only after the turn output is written or while a
\* completion's outcome is unknown: every other worker step is a single
\* action, so a crash anywhere else loses nothing.
WorkerCrash(w) ==
    /\ wk[w].pc \in {"dep", "rebase"}
    /\ w \notin crashedOnce
    /\ crashedOnce' = crashedOnce \cup {w}
    /\ SetWk(w, WorkerIdle)
    /\ UNCHANGED <<sVars, cl, co, due, acked>>

\* --- Timers, hints and the sweep -------------------------------------------

\* Timer discovery here goes through the hint: the scheduler writes one
\* advisory hint per armed generation (Overwrite) under a key that names the
\* generation, as the due bucket does. The HEAD-listing half of the sweep is
\* not modelled (see the README).
WriteHint(ak) ==
    /\ HeadUp(ak)
    /\ Head(ak).timer.st = "armed"
    /\ co[ak] < Head(ak).timer.gen
    /\ Store!PutOverwrite(HintKey(ak, Head(ak).timer.gen),
                          <<"hint", Head(ak).inc, Head(ak).timer.gen>>)
    /\ co' = [co EXCEPT ![ak] = Head(ak).timer.gen]
    /\ UNCHANGED <<cl, wk, crashedOnce, due, acked>>

\* Time passes for the armed generation (environment; no clock is read).
TimerDue(ak) ==
    /\ HeadUp(ak)
    /\ Head(ak).timer.st = "armed"
    /\ due[ak] < Head(ak).timer.gen
    /\ due' = [due EXCEPT ![ak] = Head(ak).timer.gen]
    /\ UNCHANGED <<sVars, cl, wk, co, crashedOnce, acked>>

\* The sweep serves a due hint: it reads the HEAD the hint names and, when
\* that root has the hint's incarnation and generation armed, fires it with
\* one CAS that marks the generation delivered and enqueues the timer
\* message (read and CAS are one step, see CasNow). A hint for a generation
\* the root no longer has armed, or for another incarnation, fires nothing
\* and is deleted; within an incarnation generations never re-arm, and a
\* later incarnation writes its own hint, so nothing needs the deleted one
\* again. A sweep pass that lists a hint whose
\* generation is not yet due keeps it and changes nothing, so it is a
\* stutter and has no action; the EarlyHintForgotten negative shows that
\* deleting it instead loses the timer.
FireTimer(ak, g) ==
    /\ Store!Present(HintKey(ak, g))
    /\ g =< due[ak]
    /\ HeadUp(ak)
    /\ LET h == Head(ak)
       IN IF h.timer = [gen |-> g, st |-> "armed"] /\ ~h.tomb /\ HintInc(ak, g) = h.inc
            THEN CasNow(ak, FireHead(h, g))
            ELSE Store!Delete(HintKey(ak, g))
    /\ UNCHANGED <<cl, wk, co, crashedOnce, due, acked>>

\* --- Specification --------------------------------------------------------

Step ==
    \/ \E ak \in AKs : CreateHead(ak) \/ Destroy(ak) \/ Recreate(ak)
    \/ \E c \in Clients :
         \/ ReadHead(c) \/ CasHead(c) \/ CasHeadLostResponse(c) \/ Cancel(c)
    \/ \E w \in Workers :
         \/ CompleteTurn(w) \/ CompleteTurnLostResponse(w) \/ CompleteTurnRebase(w)
         \/ FailTurn(w) \/ WorkerCrash(w)
         \/ \E ak \in AKs :
              \/ Takeover(w, ak) \/ BeginTurn(w, ak) \/ PutTurnOutput(w, ak)
              \/ ScheduleTimer(w, ak) \/ CancelTimer(w, ak)
    \/ \E ak \in AKs :
         \/ WriteHint(ak) \/ TimerDue(ak)
         \/ \E g \in 1..MaxTimerGen : FireTimer(ak, g)

\* Every step records its HEAD write in the witness (CasWitness).
Next == Step /\ CasWitness

Spec == Init /\ [][Next]_vars

\* Liveness assumptions: a crashed worker restarts (a compatible worker
\* stays available), workers keep acting on accepted work, the scheduler
\* writes its hint and the sweep keeps serving due hints. Store
\* availability: a transient failure is a stutter, which weak fairness rules
\* out forever; contention is bounded because every writer is. Each fair
\* action is taken with the witness, as Next takes it.
Fairness ==
    /\ \A w \in Workers :
         /\ WF_vars((\E ak \in AKs : Takeover(w, ak)) /\ CasWitness)
         /\ WF_vars((\E ak \in AKs : BeginTurn(w, ak)) /\ CasWitness)
         /\ WF_vars((\E ak \in AKs : PutTurnOutput(w, ak)) /\ CasWitness)
         /\ WF_vars(CompleteTurn(w) /\ CasWitness)
         /\ WF_vars(CompleteTurnRebase(w) /\ CasWitness)
    /\ \A ak \in AKs :
         /\ WF_vars(CreateHead(ak) /\ CasWitness)
         /\ WF_vars(WriteHint(ak) /\ CasWitness)
         /\ WF_vars((\E g \in 1..MaxTimerGen : FireTimer(ak, g)) /\ CasWitness)

FairSpec == Spec /\ Fairness

\* --- Safety properties ----------------------------------------------------
\* Each reads the store, the lastCas witness, or the handles returned to
\* clients. "Same incarnation" restricts the lastCas clauses to writes that
\* did not recreate the agent.

LC == lastCas
SameInc == LC # NoCas /\ LC.before # NoC /\ LC.after.inc = LC.before.inc
NewResults == LC.after.results \ LC.before.results

\* A request a client was told is accepted stays resolvable from the store:
\* its receipt is in HEAD and its payload object is present, for as long as
\* the incarnation it was accepted under is live.
AcceptedImpliesRecoverable ==
    \A c \in Clients : acked[c] # NoHandle =>
        LET ak == acked[c].ak
            r == acked[c].rcpt
        IN (HeadUp(ak) /\ Head(ak).inc = r.inc /\ ~Head(ak).tomb) =>
             /\ r \in Head(ak).receipts
             /\ Store!Present(ObjKey(ak, r.hash))

\* Every object HEAD references is present in the store.
PublishedReferencesComplete ==
    \A ak \in AKs : HeadUp(ak) =>
        LET h == Head(ak)
        IN /\ \A x \in h.inputs : x.src[1] = "client" => Store!Present(ObjKey(ak, x.hash))
           /\ \A r \in h.results : r.kind # "cancelled" => Store!Present(ObjKey(ak, OutId(r.turn)))
           /\ h.stateRef # NoC => Store!Present(ObjKey(ak, h.stateRef))
           /\ \A s \in h.inbox : \E x \in h.inputs : x.seq = s

\* One request id names one (hash, accept_seq) pair per incarnation, the
\* receipt names the input it accepted, and no write drops a receipt.
RequestIdentityStable ==
    /\ \A ak \in AKs : HeadUp(ak) =>
         LET h == Head(ak)
         IN /\ \A r1, r2 \in h.receipts : r1.rid = r2.rid => r1 = r2
            /\ \A r \in h.receipts : \E x \in h.inputs :
                  x.seq = r.seq /\ x.src = <<"client", r.rid>> /\ x.hash = r.hash
    /\ SameInc => LC.before.receipts \subseteq LC.after.receipts

\* Each accepted input has at most one result, a consumed input is never
\* pending again, and the active turn is bound to a pending input.
OneLogicalCompletion ==
    \A ak \in AKs : HeadUp(ak) =>
        LET h == Head(ak)
        IN /\ \A r1, r2 \in h.results : r1.seq = r2.seq => r1 = r2
           /\ \A r \in h.results : r.seq \notin h.inbox
           /\ h.active.turn # 0 => h.active.seq \in h.inbox

\* A HEAD write consumes an input exactly when it publishes that input's
\* result, at most one per write, and the state version and reference move
\* with a completed result and only with one.
TurnCommitAtomic ==
    SameInc =>
        LET b == LC.before
            a == LC.after
        IN /\ {r.seq : r \in NewResults} = b.inbox \ a.inbox
           /\ Cardinality(NewResults) =< 1
           /\ \A r \in NewResults : r.kind = "completed" =>
                 a.sv = b.sv + 1 /\ r.sv = a.sv /\ a.stateRef = OutId(r.turn)
           /\ (\A r \in NewResults : r.kind # "completed") =>
                 a.sv = b.sv /\ a.stateRef = b.stateRef
           /\ \A r \in NewResults : r.kind # "cancelled" => a.active = NoTurn

\* A turn result lands only on a root whose owner, epoch and active turn are
\* the ones the committing command was bound to.
NoStaleOwnerCommit ==
    SameInc =>
        \A r \in NewResults : r.kind # "cancelled" =>
            /\ LC.before.owner = r.owner
            /\ LC.before.epoch = r.epoch
            /\ LC.before.active.turn = r.turn
            /\ LC.before.active.seq = r.seq

\* No write loses an accepted input: every allocated accept_seq is pending or
\* has a result, and a HEAD write keeps every input the previous root held.
NoLostConcurrentEnqueue ==
    /\ \A ak \in AKs : HeadUp(ak) =>
         LET h == Head(ak)
         IN \A s \in 0..(h.nextSeq - 1) :
               s \in h.inbox \/ \E r \in h.results : r.seq = s
    /\ SameInc =>
         /\ LC.before.inputs \subseteq LC.after.inputs
         /\ LC.after.nextSeq >= LC.before.nextSeq
         /\ \A s \in LC.before.inbox :
               s \in LC.after.inbox \/ \E r \in LC.after.results : r.seq = s

\* root_revision rises by exactly one per write and the store version rises
\* with it; incarnation and epoch never go back, and within an incarnation
\* the timer generation never goes back (a recreate restarts it at 0, as
\* the kernel does). The kernel keeps no turn counter, so none is checked.
MonotonicIdentity ==
    LC # NoCas =>
        /\ LC.va > LC.vb
        /\ LC.before = NoC => LC.after.rev = 1
        /\ LC.before # NoC =>
             /\ LC.after.rev = LC.before.rev + 1
             /\ LC.after.inc >= LC.before.inc
             /\ LC.after.epoch >= LC.before.epoch
        /\ SameInc => LC.after.timer.gen >= LC.before.timer.gen

\* A timer generation enqueues at most one message, only from a root where
\* that generation was armed, and a delivered generation stays delivered.
TimerGenerationDeliveredOnce ==
    /\ \A ak \in AKs : HeadUp(ak) =>
         \A g \in 1..MaxTimerGen :
            Cardinality({x \in Head(ak).inputs : x.src = <<"timer", g>>}) =< 1
    /\ SameInc =>
         /\ \A x \in LC.after.inputs \ LC.before.inputs : x.src[1] = "timer" =>
               /\ LC.before.timer = [gen |-> x.src[2], st |-> "armed"]
               /\ LC.after.timer = [gen |-> x.src[2], st |-> "delivered"]
         /\ (LC.before.timer.st = "delivered" /\ LC.after.timer.gen = LC.before.timer.gen)
               => LC.after.timer.st = "delivered"

\* A cancelled input ends cancelled, never completed or failed, and is never
\* pending or active again.
CancellationRespected ==
    /\ \A ak \in AKs : HeadUp(ak) =>
         LET h == Head(ak)
         IN /\ \A r \in h.results : r.seq \in h.cancels => r.kind = "cancelled"
            /\ h.cancels \cap h.inbox = {}
    /\ SameInc => LC.before.cancels \subseteq LC.after.cancels

\* A HEAD names its own tenant and agent, every record in it carries the
\* current incarnation, and a recreate starts from an empty mailbox.
TenantIncarnationIsolation ==
    /\ \A ak \in AKs : HeadUp(ak) =>
         LET h == Head(ak)
         IN /\ h.tenant = ak[1] /\ h.agent = ak[2]
            /\ \A x \in h.inputs \cup h.receipts \cup h.results : x.inc = h.inc
            /\ h.active.turn # 0 => h.active.inc = h.inc
    /\ (LC # NoCas /\ LC.before # NoC /\ LC.after.inc # LC.before.inc) =>
         /\ LC.after.inputs = {} /\ LC.after.receipts = {} /\ LC.after.results = {}
         /\ LC.after.inbox = {} /\ LC.after.cancels = {} /\ LC.after.nextSeq = 0
    /\ LC # NoCas => LC.after.tenant = LC.ak[1] /\ LC.after.agent = LC.ak[2]

\* --- Liveness (conditional on Fairness) ------------------------------------

PendingInput(ak, s) == HeadUp(ak) /\ ~Head(ak).tomb /\ s \in Head(ak).inbox
InputSettled(ak, s) ==
    HeadUp(ak) /\ (Head(ak).tomb \/ \E r \in Head(ak).results : r.seq = s)

\* An accepted, uncancelled input is eventually completed, failed or
\* cancelled (or its agent destroyed).
InputEventuallyResolved ==
    \A ak \in AKs, s \in 0..MaxInputs : PendingInput(ak, s) ~> InputSettled(ak, s)

DueTimer(ak, g) ==
    /\ HeadUp(ak) /\ ~Head(ak).tomb
    /\ Head(ak).timer = [gen |-> g, st |-> "armed"] /\ due[ak] >= g

\* A due, uncancelled timer generation is eventually delivered (or
\* cancelled or replaced: anything that disarms it), or its agent destroyed.
\* A destroyed agent's timer stays armed and never fires.
DueTimerEventuallyDelivered ==
    \A ak \in AKs, g \in 1..MaxTimerGen :
        DueTimer(ak, g) ~>
            (HeadUp(ak) /\ (Head(ak).tomb \/ Head(ak).timer # [gen |-> g, st |-> "armed"]))

=============================================================================
