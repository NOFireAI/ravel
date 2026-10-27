--------------------------- MODULE MCAgentProtocol ---------------------------
(***************************************************************************)
(* Model-checking wrapper for AgentProtocol: one switch per broken         *)
(* implementation and one reachability probe per fault. With every switch  *)
(* FALSE no Broken action is enabled and MCSpec is AgentProtocol's Spec.   *)
(* Each negative config sets exactly one switch and names the property it  *)
(* must violate.                                                           *)
(***************************************************************************)
EXTENDS AgentProtocol, TLC

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
    /\ AckWith(c, [ak |-> cl[c].ak, rcpt |-> NewReceipt(cl[c].obs, cl[c].rid, Hc(c))])
    /\ lastCas' = NoCas
    /\ UNCHANGED <<sVars, wk, co, crashedOnce, due>>

\* The enqueue CAS straight after the read, without the payload PUT.
BrokenHeadBeforeDependency(c) ==
    /\ HeadBeforeDependency
    /\ cl[c].pc = "start"
    /\ HeadUp(cl[c].ak)
    /\ LET ak == cl[c].ak
           h == Head(ak)
       IN /\ ~h.tomb
          /\ SameRid(c, h) = {}
          /\ CasNow(ak, EnqueueHead(h, cl[c].rid, Hc(c)))
          /\ AckWith(c, [ak |-> ak, rcpt |-> NewReceipt(h, cl[c].rid, Hc(c))])
    /\ UNCHANGED <<wk, co, crashedOnce, due>>

\* Overwrite instead of CasVersion, on a root that moved only by another
\* sender's enqueue since this client read it. The other sender used a
\* different request id, so the overwrite drops an input rather than
\* rewriting an identical one.
BrokenOverwriteStaleRoot(c) ==
    /\ OverwriteStaleRoot
    /\ cl[c].pc = "dep"
    /\ HeadUp(cl[c].ak)
    /\ LET ak == cl[c].ak
           h == cl[c].obs
           cur == Head(ak)
       IN /\ cur # h
          /\ SameRid(c, cur) = {}
          /\ cur.inc = h.inc /\ cur.tomb = h.tomb /\ cur.epoch = h.epoch
          /\ cur.owner = h.owner /\ cur.timer = h.timer
          /\ cur.results = h.results /\ cur.active = h.active
          /\ Store!PutOverwrite(CK(c), EnqueueHead(h, cl[c].rid, Hc(c)))
          /\ RecordCas(ak)
          /\ AckWith(c, [ak |-> ak, rcpt |-> NewReceipt(h, cl[c].rid, Hc(c))])
    /\ UNCHANGED <<wk, co, crashedOnce, due>>

\* Proceed to the payload PUT although HEAD already holds a receipt for this
\* request id under a different payload hash.
BrokenRequestHashMismatchAccepted(c) ==
    /\ RequestHashMismatchAccepted
    /\ cl[c].pc \in {"start", "unknown"}
    /\ HeadUp(cl[c].ak)
    /\ LET ak == cl[c].ak
           h == Head(ak)
       IN /\ ~h.tomb
          /\ \E r \in SameRid(c, h) : r.hash # Hc(c)
          /\ Store!PutCreateIfAbsent(ObjKey(ak, Hc(c)), Hc(c))
          /\ SetCl(c, [cl[c] EXCEPT !.pc = "dep", !.ver = Store!VersionOf(CK(c)), !.obs = h])
    /\ lastCas' = NoCas
    /\ UNCHANGED <<wk, co, crashedOnce, due, acked>>

\* --- Broken worker steps --------------------------------------------------

BrokenEpochCheckedOutsideCas(w) ==
    /\ EpochCheckedOutsideCas
    /\ wk[w].pc = "rebase"
    /\ HeadUp(wk[w].ak)
    /\ LET ak == wk[w].ak
           h == Head(ak)
       IN /\ ~OwnResult(w, h) /\ ~h.tomb
          /\ (h.owner # w \/ h.epoch # wk[w].epoch)
          /\ h.active.turn = wk[w].turn /\ h.sv = wk[w].base
          /\ CasNow(ak, CompleteHead(h, wk[w], "completed", w))
    /\ SetWk(w, WorkerIdle)
    /\ UNCHANGED <<cl, co, crashedOnce, due, acked>>

BrokenStaleTurnRebase(w) ==
    /\ StaleTurnRebase
    /\ wk[w].pc = "rebase"
    /\ HeadUp(wk[w].ak)
    /\ LET ak == wk[w].ak
           h == Head(ak)
       IN /\ ~OwnResult(w, h) /\ ~h.tomb
          /\ h.owner = w /\ h.epoch = wk[w].epoch /\ h.sv = wk[w].base
          /\ h.active.turn # wk[w].turn
          /\ CasNow(ak, CompleteHead(h, wk[w], "completed", w))
    /\ SetWk(w, WorkerIdle)
    /\ UNCHANGED <<cl, co, crashedOnce, due, acked>>

BrokenCursorWithoutResult(w) ==
    /\ CursorWithoutResult
    /\ wk[w].pc = "dep"
    /\ WorkerCas(w, [CompleteHead(Ob(w), wk[w], "completed", w) EXCEPT
                       !.results = Ob(w).results], "idle", "rebase")
    /\ UNCHANGED <<cl, co, crashedOnce, due, acked>>

BrokenReceiptDiscarded(w) ==
    /\ ReceiptDiscarded
    /\ wk[w].pc = "dep"
    /\ \E r \in Ob(w).receipts : r.seq = wk[w].seq
    /\ WorkerCas(w, [CompleteHead(Ob(w), wk[w], "completed", w) EXCEPT
                       !.receipts = {r \in Ob(w).receipts : r.seq # wk[w].seq}],
                 "idle", "rebase")
    /\ UNCHANGED <<cl, co, crashedOnce, due, acked>>

\* --- Broken timer steps ---------------------------------------------------

\* Fire from a hint whose generation the root no longer has armed.
BrokenStaleTimerGenerationFired(ak, g) ==
    /\ StaleTimerGenerationFired
    /\ Store!Present(HintKey(ak, g))
    /\ g =< due[ak]
    /\ HeadUp(ak)
    /\ LET h == Head(ak)
       IN /\ ~(h.timer = [gen |-> g, st |-> "armed"] /\ ~h.tomb)
          /\ CasNow(ak, [h EXCEPT !.rev = @ + 1, !.inputs = @ \cup {TimerInput(h, g)},
                                  !.inbox = @ \cup {h.nextSeq}, !.nextSeq = @ + 1])
    /\ UNCHANGED <<cl, wk, co, crashedOnce, due, acked>>

\* The sweep sees a hint before its generation is due and deletes it.
BrokenEarlyHintForgotten(ak, g) ==
    /\ EarlyHintForgotten
    /\ Store!Present(HintKey(ak, g))
    /\ due[ak] < g
    /\ Store!Delete(HintKey(ak, g))
    /\ lastCas' = NoCas
    /\ UNCHANGED <<cl, wk, co, crashedOnce, due, acked>>

Broken ==
    \/ \E c \in Clients :
         \/ BrokenAckBeforeCas(c) \/ BrokenHeadBeforeDependency(c)
         \/ BrokenOverwriteStaleRoot(c) \/ BrokenRequestHashMismatchAccepted(c)
    \/ \E w \in Workers :
         \/ BrokenEpochCheckedOutsideCas(w) \/ BrokenStaleTurnRebase(w)
         \/ BrokenCursorWithoutResult(w) \/ BrokenReceiptDiscarded(w)
    \/ \E ak \in AKs :
         \/ \E g \in 1..MaxTimerGen :
              BrokenStaleTimerGenerationFired(ak, g) \/ BrokenEarlyHintForgotten(ak, g)

MCNext == Next \/ Broken

MCSpec == Init /\ [][MCNext]_vars

MCFairSpec == MCSpec /\ Fairness

\* --- State view for the safety configs -----------------------------------

\* The store mints every version from one global counter, so two orderings of
\* independent writes (a payload PUT and another agent's HEAD CAS) reach
\* states that differ only in version integers. Nothing here reads a version
\* except to compare a held CAS token with the current HEAD version, and a
\* token that is not current never becomes current again, because versions
\* are never reused. The view keeps that comparison and drops the integers
\* (lastModified is advisory and unread). This is sound for invariants only:
\* a config with a PROPERTY runs without VIEW.
\* No action, invariant or initial state names a particular worker, so two
\* states that differ by a renaming of workers satisfy the same invariants.
\* Like the view, the symmetry is for invariant-only configs.
WorkerSymmetry == Permutations(Workers)

Current(ak, v) == HeadUp(ak) /\ v = Store!VersionOf(HeadKey(ak))

View ==
    <<{<<k, store[k].content>> : k \in {x \in AllKeys : store[x].present}},
      {<<c, cl[c].pc, cl[c].ak, cl[c].rid, cl[c].inp, Current(cl[c].ak, cl[c].ver),
         cl[c].obs, cl[c].handle>> : c \in Clients},
      {<<w, wk[w].pc, wk[w].ak, Current(wk[w].ak, wk[w].ver), wk[w].obs, wk[w].turn,
         wk[w].seq, wk[w].base, wk[w].epoch, wk[w].inc>> : w \in Workers},
      co, crashedOnce, due, acked, uploads, listState,
      IF lastCas = NoCas THEN <<>>
      ELSE <<lastCas.ak, lastCas.before, lastCas.after, lastCas.va > lastCas.vb>>>>

\* --- Reachability probes (each must be reported violated) -----------------

\* Three probes read store or operation state; CrashUnreachable reads the
\* crashedOnce witness WorkerCrash records, so it shows the action fired.

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
