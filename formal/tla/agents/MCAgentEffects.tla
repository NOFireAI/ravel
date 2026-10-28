---------------------------- MODULE MCAgentEffects ----------------------------
(***************************************************************************)
(* Model-checking wrapper for AgentEffects: one switch per broken          *)
(* implementation and one reachability probe per fault. With every switch  *)
(* FALSE no Broken action is enabled and MCSpec is AgentEffects's Spec.    *)
(* Each negative config sets exactly one switch and names the property it  *)
(* must violate.                                                           *)
(***************************************************************************)
EXTENDS AgentEffects, TLC

CONSTANTS
    DispatchBeforeIntent,          \* dispatch before the intent is journaled
    AmbiguousManualAttemptRetried, \* re-dispatch an ambiguous manual attempt
    StepInputDrift,                \* rewrite a step record a replay disagrees with
    AuthorizeIgnoresCancelRequest, \* authorize an attempt on a cancelled turn
    LateResultOverwrites,          \* a late result replaces the recorded one
    CommitWhileInDoubt             \* commit the turn while its effect is in doubt

ASSUME DispatchBeforeIntent \in BOOLEAN
ASSUME AmbiguousManualAttemptRetried \in BOOLEAN
ASSUME StepInputDrift \in BOOLEAN
ASSUME AuthorizeIgnoresCancelRequest \in BOOLEAN
ASSUME LateResultOverwrites \in BOOLEAN
ASSUME CommitWhileInDoubt \in BOOLEAN

\* --- Broken worker steps ---------------------------------------------------

\* The step is recorded and the tool call goes out before its intent and
\* attempt are journaled.
BrokenDispatchBeforeIntent(w) ==
    /\ DispatchBeforeIntent
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).step.st = "completed"
    /\ O(w).eff.st = "none"
    /\ SetAc(w, [ActorIdle EXCEPT !.pc = "exec", !.att = 1])
    /\ UNCHANGED <<sVars, calls, crashed, cl, pol>>

\* The owner reads a manual attempt authorized without a result, treats the
\* idempotent AttemptAuthorized reply as permission, and dispatches the same
\* attempt again.
BrokenAmbiguousManualAttemptRetried(w) ==
    /\ AmbiguousManualAttemptRetried
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).eff.st = "authorized"
    /\ O(w).eff.pol = "manual"
    /\ ~ReconcilerOwns(O(w))
    /\ SetAc(w, [ActorIdle EXCEPT !.pc = "exec", !.att = O(w).eff.att])
    /\ UNCHANGED <<sVars, calls, crashed, cl, pol>>

\* A replay that computes another input hash overwrites the step record
\* instead of blocking the turn.
BrokenStepInputDrift(w, x) ==
    /\ StepInputDrift
    /\ Ready(w)
    /\ Own(w)
    /\ Running(O(w))
    /\ O(w).step.st = "completed"
    /\ x # O(w).step.hash
    /\ CasThen(w, [Bump(O(w)) EXCEPT !.step.hash = x], ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* The owner authorizes the journaled intent although the root carries a
\* cancel request on the turn.
BrokenAuthorizeIgnoresCancelRequest(w) ==
    /\ AuthorizeIgnoresCancelRequest
    /\ Ready(w)
    /\ Own(w)
    /\ O(w).active.turn # 0
    /\ O(w).active.creq
    /\ O(w).eff.st = "intent"
    /\ CasThen(w, AuthorizeHead(O(w), 1), [ActorIdle EXCEPT !.pc = "exec", !.att = 1])
    /\ UNCHANGED <<calls, crashed, cl, pol>>

\* The result endpoint records a result for a completed effect, replacing
\* the recorded one instead of answering LateResultIgnored.
BrokenLateResultOverwrites(i) ==
    /\ LateResultOverwrites
    /\ RootUp
    /\ i \in PhysCalls
    /\ Root.eff.st = "completed"
    /\ Root.eff.rval # i
    /\ calls[i].att =< Root.eff.att
    /\ Store!PutCasVersion(HeadKey, Ver, RecordResultHead(Root, calls[i].att, i))
    /\ UNCHANGED <<ac, calls, crashed, cl, pol>>

\* A cancelled turn is committed while its effect is in doubt.
BrokenCommitWhileInDoubt(w) ==
    /\ CommitWhileInDoubt
    /\ Ready(w)
    /\ Own(w)
    /\ O(w).active.turn # 0
    /\ O(w).active.creq
    /\ O(w).eff.st = "indoubt"
    /\ CasThen(w, EndTurn(O(w), "completed", w), ActorIdle)
    /\ UNCHANGED <<calls, crashed, cl, pol>>

Broken ==
    \/ \E w \in Workers :
         \/ BrokenDispatchBeforeIntent(w)
         \/ BrokenAmbiguousManualAttemptRetried(w)
         \/ BrokenAuthorizeIgnoresCancelRequest(w)
         \/ BrokenCommitWhileInDoubt(w)
         \/ \E x \in Hashes : BrokenStepInputDrift(w, x)
    \/ \E i \in 1..MaxCalls : BrokenLateResultOverwrites(i)

MCNext == (Step \/ Broken) /\ CasWitness

MCSpec == Init /\ [][MCNext]_vars

MCFairSpec == MCSpec /\ Fairness

\* --- State view for the safety configs -----------------------------------

\* As in MCAgentProtocol: versions come from one global counter and are only
\* compared with the current HEAD version, so the view keeps that comparison
\* and drops the integers. Sound for invariants only; a config with a
\* PROPERTY runs without VIEW. A stale read is dropped too: an actor holding
\* one can only re-read it or fail a CAS back to idle, and neither outcome
\* depends on what it read. No action names a particular worker, so the
\* worker symmetry is sound for invariants too.
WorkerSymmetry == Permutations(Workers)

View ==
    <<{<<k, store[k].content>> : k \in {x \in AllKeys : store[x].present}},
      [a \in Actors |-> <<ac[a].pc, Fresh(a), IF Fresh(a) THEN ac[a].obs ELSE NoC,
                          ac[a].held, ac[a].att, ac[a].val>>],
      calls, crashed, cl, pol, uploads, listState,
      IF lastCas = NoCas THEN <<>>
      ELSE <<lastCas.before, lastCas.after>>>>

\* --- Reachability probes (each must be reported violated) -----------------

\* Under at_least_once the provider executed one effect twice.
DuplicateExecutionUnreachable == Cardinality(PhysCalls) =< 1

\* A manual effect went in doubt and blocks its turn.
BlockedEffectUnreachable == ~(RootUp /\ Root.eff.st = "indoubt")

\* An admin reconciled a manual effect: its result is recorded behind an
\* audit object.
ReconciledEffectUnreachable ==
    ~(/\ RootUp
      /\ Root.eff.st = "completed"
      /\ \E k \in AuditKinds, a \in 1..MaxAttempt : AuditUp(k, a))

\* A worker crashed with an operation in flight or a response unrecorded.
CrashUnreachable == crashed = {}

\* Liveness probe: without the blocked terminals the input is not always
\* settled, so an effect in doubt (or another blocked effect) is a reachable
\* end state under fairness. A config with this PROPERTY must report it
\* violated (exit 13).
InputEventuallySettled ==
    (RootUp /\ Root.inbox # {}) ~> (RootUp /\ Root.inbox = {})

=============================================================================
