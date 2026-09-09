--------------------------- MODULE MCCommitProtocol ---------------------------
(***************************************************************************)
(* Model-check entry for CommitProtocol: small constants, the symmetry set, *)
(* the reachability obligations, and the liveness property.                 *)
(*                                                                          *)
(* The negative-control switches live in CommitProtocol's CONSTANTS and     *)
(* default to FALSE in smoke.cfg and exhaustive.cfg. A negative cfg flips   *)
(* exactly one. There is never a second copy of the specification.          *)
(***************************************************************************)
EXTENDS CommitProtocol, TLC

\* Writers are interchangeable: no invariant or property names one, and the
\* keys they own are derived from the writer itself, so permuting them maps
\* behaviours onto behaviours. Shards are NOT permuted: shardDead and the
\* per-signal partial-commit rule distinguish them.
Symmetry == Permutations(Writers)

\* --- Reachability obligations ---------------------------------------------
\* Several properties of this protocol are the ABSENCE of a guarantee, or an
\* action that a low-enough constant can disable outright. TLC checks an
\* invariant, so each is written as a predicate that must FAIL: the run is
\* correct exactly when TLC reports the violation, and a variant that removed
\* the behaviour would leave it green. Each is run in its own negative/*.cfg
\* rather than alongside the safety set.

\* Ravel offers no cross-shard atomicity. A state with one shard's commit
\* durable and another's not must be reachable.
NoCrossShardAtomicityUnreachable ==
    ~(\E s1, s2 \in Shards :
        /\ s1 # s2
        /\ \E w \in Writers : Visible(<<w, s1>>)
        /\ \A w \in Writers : ~Visible(<<w, s2>>))

\* Logs and spans are at-least-once. A retry after a lost acknowledgement,
\* with no usable idempotency marker, must be able to leave two durable
\* commit records holding the same content. AtLeastOnce alone is satisfied by
\* exactly-once delivery, so this obligation is what the deduplicating
\* negative control fails.
\* Derived from the STORE: two distinct commit records, both durable, holding
\* the same content. That is the at-least-once outcome of a client retry whose
\* first acknowledgement was lost. RetryDedups suppresses the second WRITE, so
\* flipping it makes this obligation stop firing.
DuplicateUnreachable ==
    ~(\E f, g \in FlushIds :
        /\ retryOf[g] = f
        /\ Store!Present(CommitKey(f))
        /\ Store!Present(CommitKey(g)))

\* The flush-lifetime deadline must be REACHABLE, or every property about
\* abandonment is vacuous at these bounds. Clock room is necessary and not
\* sufficient, so this is checked rather than inferred from the constants.
AbandonUnreachable == ~(\E f \in FlushIds : phase[f] = "abandoned")

\* At MaxRetries=0 the three retry actions below are permanently disabled
\* (their `retries[f] < MaxRetries` guard can never hold), so a cfg that
\* never sets MaxRetries > 0 leaves them, and everything they alone
\* exercise, unreachable. Each predicate is a post-state check on faultFired,
\* the execution witness the named action alone sets (CommitProtocol.tla),
\* in the same shape as AbandonUnreachable above: a VIOLATED report proves
\* the action actually FIRED, not merely that its enabling conjuncts were
\* satisfiable in some pre-state that never took the transition.
PutDataLostResponseUnreachable ==
    ~(\E f \in FlushIds : <<f, "putDataLost">> \in faultFired)

PutCommitLostResponseUnreachable ==
    ~(\E f \in FlushIds : <<f, "putCommitLost">> \in faultFired)

TransientFailureUnreachable ==
    ~(\E f \in FlushIds : <<f, "transient">> \in faultFired)

\* --- Liveness --------------------------------------------------------------
\* Under weak fairness on the store retry and the flush task only, a pinned
\* flush either becomes durable or the writer stops with an explicit failure.
\* The retry budget and the flush lifetime are constants, so "eventually
\* durable" is not claimed unconditionally: an abandoned flush, a crashed one
\* (whose identity is retired because the restarted process mints a fresh
\* writer id) and a stopped shard all satisfy the disjunction. This is a claim about the
\* protocol design under those assumptions, not about the implementation.
EveryPinnedFlushSettles ==
    \A f \in FlushIds :
        (phase[f] = "pinned") ~>
            (Visible(f) \/ phase[f] \in {"abandoned", "stopped", "retired"})

===============================================================================
