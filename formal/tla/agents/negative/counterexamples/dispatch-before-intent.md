# Counterexample: tool call dispatched before its intent is journaled

Config: `negative/dispatch-before-intent.cfg`
(`DispatchBeforeIntent = TRUE`; smoke bounds: two workers, all three
policies, one input hash, MaxEpoch 2, MaxTurns 2, MaxAttempt 2, `VIEW View`,
`SYMMETRY WorkerSymmetry`).
Expected: `DispatchRequiresCommittedAuthorization` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant DispatchRequiresCommittedAuthorization is violated.

Trace (11 states, depth 11). TLC labels every step `MCNext` because the
next-state relation is conjoined with the witness; the action names below
are read off the HEAD fields, the actor records and the provider log each
step changes.

    State 1:  <Initial predicate>
    State 2:  CreateHead (rev 1, input 0 accepted, no owner)
    State 3:  Read(w1)
    State 4:  Takeover(w1) (epoch 1, rev 2)
    State 5:  Read(w1)
    State 6:  BeginTurn(w1) (turn 1, rev 3)
    State 7:  Read(w1)
    State 8:  RecordStep(w1, h1) (step completed, rev 4)
    State 9:  Read(w1)
    State 10: BrokenDispatchBeforeIntent(w1) (w1 holds attempt 1, effect
              record still "none")
    State 11: Dispatch(w1) (provider log <<attempt 1, executed>>)

## What the switch breaks

The runner must journal the intent (`commit_intent`) and authorize the
attempt (`authorize_attempt`) in HEAD CASes before the tool call leaves the
process. The switch lets the owner go from a recorded step straight to the
dispatch, with no effect record on the root.

## Why it is a real bug if it ever held

After state 11 the provider has executed the call and the journal holds no
intent. If the worker crashes here, its successor reads a root with no
effect record, journals a fresh intent and authorizes attempt 1 again: the
call executes twice under every policy, manual included, and nothing on the
root says an execution is outstanding, so no reconciliation is possible.
`DispatchRequiresCommittedAuthorization` requires every provider call to be
of an attempt the journal authorized.
