--------------------------- MODULE MCAgentProtocol ---------------------------
(***************************************************************************)
(* Model-checking wrapper for AgentProtocol: one switch per broken         *)
(* implementation and one reachability probe per fault. With every switch  *)
(* FALSE no Broken action is enabled and MCSpec is AgentProtocol's Spec.   *)
(* Each negative config sets exactly one switch and names the property it  *)
(* must violate.                                                           *)
(***************************************************************************)
EXTENDS AgentProtocol

CONSTANTS
    AckBeforeCas,                \* respond to the sender before the enqueue CAS
    HeadBeforeDependency,        \* enqueue CAS without the payload PUT first
    OverwriteStaleRoot,          \* enqueue by Overwrite of a stale HEAD
    EpochCheckedOutsideCas,      \* rebase a completion without the owner/epoch check
    CursorWithoutResult,         \* consume the input without publishing its result
    RequestHashMismatchAccepted, \* accept a known request id with another hash
    ReceiptDiscarded,            \* completion drops the consumed input's receipt
    StaleTimerGenerationFired,   \* fire a hint without the armed-generation check
    EarlyHintForgotten,          \* the sweep deletes a hint it sees early
    StaleTurnRebase              \* rebase a completion whose turn is gone

ASSUME AckBeforeCas \in BOOLEAN
ASSUME HeadBeforeDependency \in BOOLEAN
ASSUME OverwriteStaleRoot \in BOOLEAN
ASSUME EpochCheckedOutsideCas \in BOOLEAN
ASSUME CursorWithoutResult \in BOOLEAN
ASSUME RequestHashMismatchAccepted \in BOOLEAN
ASSUME ReceiptDiscarded \in BOOLEAN
ASSUME StaleTimerGenerationFired \in BOOLEAN
ASSUME EarlyHintForgotten \in BOOLEAN
ASSUME StaleTurnRebase \in BOOLEAN

\* --- Broken client steps --------------------------------------------------

BrokenAckBeforeCas(c) ==
    /\ AckBeforeCas
    /\ cl[c].pc = "dep"
    /\ acked' = [acked EXCEPT ![c] =
                   [ak |-> cl[c].ak, rcpt |-> NewReceipt(cl[c].obs, cl[c].rid, Hc(c))]]
    /\ SetCl(c, [cl[c] EXCEPT !.pc = "acked", !.ver = 0, !.obs = NoC])
    /\ UNCHANGED <<sVars, wk, co, live, crashedOnce, due, lastCas>>

BrokenHeadBeforeDependency(c) ==
    /\ HeadBeforeDependency
    /\ cl[c].pc = "read"
    /\ SameRid(c) = {}
    /\ EnqueueCas(c)

\* Overwrite instead of CasVersion, on a root that moved only by another
\* sender's enqueue since this client read it.
BrokenOverwriteStaleRoot(c) ==
    /\ OverwriteStaleRoot
    /\ cl[c].pc = "dep"
    /\ HeadUp(cl[c].ak)
    /\ LET ak == cl[c].ak
           h == cl[c].obs
           cur == Head(ak)
       IN /\ cur # h
          /\ cur.inc = h.inc /\ cur.tomb = h.tomb /\ cur.epoch = h.epoch
          /\ cur.owner = h.owner /\ cur.timer = h.timer
          /\ cur.results = h.results /\ cur.active = h.active
          /\ Store!PutOverwrite(CK(c), EnqueueHead(h, cl[c].rid, Hc(c)))
          /\ RecordCas(ak)
          /\ SetCl(c, [cl[c] EXCEPT !.pc = "casok",
                         !.handle = [ak |-> ak, rcpt |-> NewReceipt(h, cl[c].rid, Hc(c))]])
    /\ UNCHANGED <<wk, co, live, crashedOnce, due, acked>>

\* Proceed to the payload PUT although HEAD already holds a receipt for this
\* request id under a different payload hash.
BrokenRequestHashMismatchAccepted(c) ==
    /\ RequestHashMismatchAccepted
    /\ cl[c].pc = "read"
    /\ \E r \in SameRid(c) : r.hash # Hc(c)
    /\ Store!PutCreateIfAbsent(ObjKey(cl[c].ak, Hc(c)), Hc(c))
    /\ SetCl(c, [cl[c] EXCEPT !.pc = "dep"])
    /\ UNCHANGED <<wk, co, live, crashedOnce, due, acked, lastCas>>

\* --- Broken worker steps --------------------------------------------------

BrokenEpochCheckedOutsideCas(w) ==
    /\ EpochCheckedOutsideCas
    /\ live[w]
    /\ wk[w].pc = "rread"
    /\ LET h == Ob(w)
       IN /\ ~OwnResult(w) /\ ~h.tomb
          /\ (h.owner # w \/ h.epoch # wk[w].epoch)
          /\ h.active.turn = wk[w].turn /\ h.sv = wk[w].base
          /\ WorkerCas(w, CompleteHead(h, wk[w], "completed", w), "idle", "rebase")
    /\ UNCHANGED <<cl, co, live, crashedOnce, due, acked>>

BrokenStaleTurnRebase(w) ==
    /\ StaleTurnRebase
    /\ live[w]
    /\ wk[w].pc = "rread"
    /\ LET h == Ob(w)
       IN /\ ~OwnResult(w) /\ ~h.tomb
          /\ h.owner = w /\ h.epoch = wk[w].epoch /\ h.sv = wk[w].base
          /\ h.active.turn # wk[w].turn
          /\ WorkerCas(w, CompleteHead(h, wk[w], "completed", w), "idle", "rebase")
    /\ UNCHANGED <<cl, co, live, crashedOnce, due, acked>>

BrokenCursorWithoutResult(w) ==
    /\ CursorWithoutResult
    /\ live[w]
    /\ wk[w].pc = "dep"
    /\ WorkerCas(w, [CompleteHead(Ob(w), wk[w], "completed", w) EXCEPT
                       !.results = Ob(w).results], "idle", "rebase")
    /\ UNCHANGED <<cl, co, live, crashedOnce, due, acked>>

BrokenReceiptDiscarded(w) ==
    /\ ReceiptDiscarded
    /\ live[w]
    /\ wk[w].pc = "dep"
    /\ \E r \in Ob(w).receipts : r.seq = wk[w].seq
    /\ WorkerCas(w, [CompleteHead(Ob(w), wk[w], "completed", w) EXCEPT
                       !.receipts = {r \in Ob(w).receipts : r.seq # wk[w].seq}],
                 "idle", "rebase")
    /\ UNCHANGED <<cl, co, live, crashedOnce, due, acked>>

\* --- Broken timer steps ---------------------------------------------------

\* Fire from a hint whose generation the observed root no longer has armed.
BrokenStaleTimerGenerationFired(ak) ==
    /\ StaleTimerGenerationFired
    /\ co[ak].pc = "fire"
    /\ LET h == co[ak].obs
           g == co[ak].gen
       IN /\ ~(h.timer = [gen |-> g, st |-> "armed"] /\ ~h.tomb)
          /\ Store!PutCasVersion(HeadKey(ak), co[ak].ver,
                [h EXCEPT !.rev = @ + 1, !.inputs = @ \cup {TimerInput(h, g)},
                          !.inbox = @ \cup {h.nextSeq}, !.nextSeq = @ + 1])
          /\ IF CasOk(ak, co[ak].ver) THEN RecordCas(ak) ELSE UNCHANGED lastCas
    /\ co' = [co EXCEPT ![ak] = CoordIdle(co[ak].hinted)]
    /\ UNCHANGED <<cl, wk, live, crashedOnce, due, acked>>

\* The sweep sees a hint before its generation is due and deletes it.
BrokenEarlyHintForgotten(ak, g) ==
    /\ EarlyHintForgotten
    /\ Store!Present(HintKey(ak, g))
    /\ due[ak] < g
    /\ Store!Delete(HintKey(ak, g))
    /\ UNCHANGED aVars

Broken ==
    \/ \E c \in Clients :
         \/ BrokenAckBeforeCas(c) \/ BrokenHeadBeforeDependency(c)
         \/ BrokenOverwriteStaleRoot(c) \/ BrokenRequestHashMismatchAccepted(c)
    \/ \E w \in Workers :
         \/ BrokenEpochCheckedOutsideCas(w) \/ BrokenStaleTurnRebase(w)
         \/ BrokenCursorWithoutResult(w) \/ BrokenReceiptDiscarded(w)
    \/ \E ak \in AKs :
         \/ BrokenStaleTimerGenerationFired(ak)
         \/ \E g \in 1..MaxTimerGen : BrokenEarlyHintForgotten(ak, g)

MCNext == Next \/ Broken

MCSpec == Init /\ [][MCNext]_vars

MCFairSpec == MCSpec /\ Fairness

\* --- Reachability probes (each must be reported violated) -----------------

\* Each probe reads state, not a flag the fault action sets.

\* A worker crashed with an operation in flight.
CrashUnreachable == crashedOnce = {}

\* A caller holds an unknown outcome while HEAD carries the write it lost
\* the response to: a sender's receipt, or a worker's own turn result.
LostResponseUnreachable ==
    /\ ~\E c \in Clients :
          /\ cl[c].pc = "unknown"
          /\ HeadUp(cl[c].ak)
          /\ \E r \in Head(cl[c].ak).receipts : r.rid = cl[c].rid /\ r.hash = Hc(c)
    /\ ~\E w \in Workers :
          /\ wk[w].pc = "rebase"
          /\ HeadUp(wk[w].ak)
          /\ \E r \in Head(wk[w].ak).results : r.seq = wk[w].seq /\ r.turn = wk[w].turn

\* An owner was displaced: epoch 1 is the first claim of an unowned agent,
\* so epoch 2 means a worker took over from another worker.
TakeoverUnreachable == \A ak \in AKs : HeadUp(ak) => Head(ak).epoch =< 1

\* A sender is about to CAS a root another sender's enqueue already moved.
CompetingSendersUnreachable ==
    ~\E c \in Clients :
        /\ cl[c].pc = "dep"
        /\ HeadUp(cl[c].ak)
        /\ Head(cl[c].ak).inc = cl[c].obs.inc
        /\ Head(cl[c].ak).nextSeq > cl[c].obs.nextSeq

=============================================================================
