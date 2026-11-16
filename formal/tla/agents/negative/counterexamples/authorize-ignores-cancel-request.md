# Counterexample: attempt authorized on a turn with a cancel request

Config: `negative/authorize-ignores-cancel-request.cfg`
(`AuthorizeIgnoresCancelRequest = TRUE`; smoke bounds).
Expected: `CancellationRespected` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant CancellationRespected is violated.

Trace (13 states, depth 13). Action names are read off the state.

    State 1:  <Initial predicate>
    State 2:  CreateHead
    State 3:  Read(w1)
    State 4:  Takeover(w1) (epoch 1)
    State 5:  Read(w1)
    State 6:  BeginTurn(w1) (turn 1)
    State 7:  Read(w1)
    State 8:  RecordStep(w1, h1)
    State 9:  Read(w1)
    State 10: CommitIntent(w1, h1) (effect "intent", policy
              provider_idempotency)
    State 11: Cancel (cancel request on turn 1, rev 6)
    State 12: Read(w1) (w1 sees the request)
    State 13: BrokenAuthorizeIgnoresCancelRequest(w1) (effect "authorized",
              attempt 1, request still set)

## What the switch breaks

Cancellation is revalidated at authorization: an owner that reads a cancel
request on its turn authorizes nothing further and commits the turn, which
publishes the input cancelled. The switch authorizes the journaled intent
on a root that already carries the request.

## Why it is a real bug if it ever held

After state 13 the owner holds attempt 1 and dispatches it, so an external
side effect starts after the client's cancel was accepted. The kernel
refuses this: `Kernel::authorize_attempt` answers an owner authorization on
a turn whose cancellation is requested with `OwnershipLost`, "the effect's
input is being cancelled" (`crates/ravel-agents-core/src/kernel.rs:1764`).
The runner refuses earlier still: `Coordinator::run_step` calls
`refuse_cancelled` before it authorizes
(`crates/ravel-agents-runtime/src/effects.rs:657`).
`CancellationRespected` requires that no HEAD write on a root with a cancel
request raises the authorized attempt without a reconcile audit object.
