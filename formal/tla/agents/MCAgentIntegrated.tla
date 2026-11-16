-------------------------- MODULE MCAgentIntegrated --------------------------
(***************************************************************************)
(* Model-checking wrapper for AgentIntegrated: one switch per broken       *)
(* implementation and one reachability probe per path. With every switch   *)
(* FALSE no Broken action is enabled and MCSpec is AgentIntegrated's Spec. *)
(* Each negative config sets exactly one switch and names the property it  *)
(* must violate.                                                           *)
(***************************************************************************)
EXTENDS AgentIntegrated, TLC

CONSTANTS
    AckBeforeCas,          \* answer the client before the enqueue CAS
    ResolveTrustsUnknown,  \* answer an unknown enqueue outcome without a re-read
    SweepOverwritesHead,   \* the sweep fires from its read by blind overwrite
    DispatchBeforeIntent,  \* dispatch the tool call before its intent
    CompleteIgnoresCancel  \* commit a cancelled turn as completed

ASSUME AckBeforeCas \in BOOLEAN
ASSUME ResolveTrustsUnknown \in BOOLEAN
ASSUME SweepOverwritesHead \in BOOLEAN
ASSUME DispatchBeforeIntent \in BOOLEAN
ASSUME CompleteIgnoresCancel \in BOOLEAN

\* The client is answered once the payload object is written, before the
\* enqueue CAS has returned.
BrokenAckBeforeCas ==
    /\ AckBeforeCas
    /\ cl.pc = "dep"
    /\ acked = NoHandle
    /\ acked' = NewReceipt(cl.obs)
    /\ UNCHANGED <<sVars, cl, wk, sw, calls, crashed, pol>>

\* After a lost enqueue response the client answers with the handle the CAS
\* would have returned, without reading HEAD for the receipt.
BrokenResolveTrustsUnknown ==
    /\ ResolveTrustsUnknown
    /\ cl.pc = "unknown"
    /\ SetCl([cl EXCEPT !.pc = "resp", !.handle = NewReceipt(cl.obs)])
    /\ UNCHANGED <<sVars, wk, sw, calls, crashed, pol, acked>>

\* The sweep fires the generation it read with an unconditional PUT of the
\* HEAD it read, not a CAS on that version.
BrokenSweepOverwritesHead ==
    /\ SweepOverwritesHead
    /\ sw.pc = "read"
    /\ sw.obs.timer = [gen |-> sw.g, st |-> "armed"]
    /\ Store!PutOverwrite(HeadKey, FireHead(sw.obs, sw.g))
    /\ sw' = SweepIdle
    /\ UNCHANGED <<cl, wk, calls, crashed, pol, acked>>

\* The step is recorded and the tool call goes out before its intent and
\* attempt are journaled.
BrokenDispatchBeforeIntent(w) ==
    /\ DispatchBeforeIntent
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ ClientTurn(O(w))
    /\ O(w).step = "completed"
    /\ O(w).eff.st = "none"
    /\ SetWk(w, [WorkerIdle EXCEPT !.pc = "exec", !.att = 1])
    /\ UNCHANGED sVars
    /\ WUnchanged

\* The owner reads a cancel request on a turn whose output is written and
\* commits the turn completed instead of cancelled.
BrokenCompleteIgnoresCancel(w) ==
    /\ CompleteIgnoresCancel
    /\ Ready(w)
    /\ Own(w)
    /\ O(w).active.turn # 0
    /\ O(w).active.creq
    /\ Store!Present(OutKey(O(w).active.turn))
    /\ CasThen(w, EndTurn([O(w) EXCEPT !.active.creq = FALSE], "completed", w), WorkerIdle)
    /\ WUnchanged

Broken ==
    \/ BrokenAckBeforeCas
    \/ BrokenResolveTrustsUnknown
    \/ BrokenSweepOverwritesHead
    \/ \E w \in Workers : BrokenDispatchBeforeIntent(w) \/ BrokenCompleteIgnoresCancel(w)

MCNext == (Step \/ Broken) /\ CasWitness

MCSpec == Init /\ [][MCNext]_vars

MCFairSpec == MCSpec /\ Fairness

\* --- State view for the safety configs -----------------------------------

\* As in MCAgentEffects: versions come from one global counter and are only
\* compared with the current HEAD version, so the view keeps that comparison
\* and drops the integers. A worker's stale read is dropped too (it can only
\* re-read it or fail a CAS back to idle); the client's and the sweep's are
\* kept, since the client resolves from a stale read and the sweep deletes
\* a hint from one. Sound for invariants only; a config with a PROPERTY runs
\* without VIEW. No action names a particular worker, so the worker
\* symmetry is sound for invariants too.
WorkerSymmetry == Permutations(Workers)

ClFresh == RootUp /\ cl.ver = Ver
SwFresh == RootUp /\ sw.ver = Ver

View ==
    <<{<<k, store[k].content>> : k \in {x \in AllKeys : store[x].present}},
      <<cl.pc, ClFresh, cl.obs, cl.handle>>,
      [w \in Workers |-> <<wk[w].pc, Fresh(w), IF Fresh(w) THEN wk[w].obs ELSE NoC,
                           wk[w].held, wk[w].att, wk[w].val>>],
      <<sw.pc, SwFresh, sw.obs, sw.g>>,
      calls, crashed, pol, acked, uploads, listState,
      IF lastCas = NoCas THEN <<>>
      ELSE <<lastCas.before, lastCas.after>>>>

\* --- Reachability probes (each must be reported violated) -----------------

\* Under at_least_once the provider executed the tool call twice.
DuplicateExecutionUnreachable == Cardinality(PhysCalls) =< 1

\* The client's tool step went in doubt and blocks its turn.
BlockedEffectUnreachable == ~(RootUp /\ Root.eff.st = "indoubt")

\* The client input's turn committed completed with its effect result
\* recorded.
ToolTurnCompletedUnreachable ==
    ~(/\ RootUp
      /\ Root.eff.st = "completed"
      /\ \E r \in Root.results : r.seq \in ClientSeq(Root) /\ r.kind = "completed")

\* The client was answered by a re-read that found its receipt (Resolve
\* keeps the read; the CAS response path does not).
ResolvedUnreachable == ~(cl.pc \in {"resp", "acked", "done"} /\ cl.obs # NoC)

\* A second worker took the agent over from the first.
TakeoverUnreachable == ~(RootUp /\ Root.epoch >= 2)

\* A crashed worker restarted.
RecoveredUnreachable == \A w \in crashed : wk[w].pc = "down"

\* The race: the client's input settled cancelled on a HEAD that also
\* delivered a timer generation.
TimerCancelRaceUnreachable ==
    ~(/\ RootUp
      /\ \E r \in Root.results : r.seq \in ClientSeq(Root) /\ r.kind = "cancelled"
      /\ \E x \in Root.inputs : x.src # "client")

\* Liveness probe: without the blocked terminals the client's input is not
\* always settled, so an effect in doubt is a reachable end state under
\* fairness. A config with this PROPERTY must report it violated (exit 13).
InputEventuallySettled ==
    \A s \in 0..MaxTurns - 1 :
        (RootUp /\ s \in Root.inbox) ~> Settled(s)

=============================================================================
