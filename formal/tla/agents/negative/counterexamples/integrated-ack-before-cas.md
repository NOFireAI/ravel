# Counterexample: client answered before the enqueue CAS

Config: `negative/integrated-ack-before-cas.cfg`
(`AckBeforeCas = TRUE`; `MCAgentIntegrated` smoke bounds).
Expected: `AcceptedImpliesRecoverable` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant AcceptedImpliesRecoverable is violated.

Trace (5 states, depth 5). Action names are read off the state.

    State 1: <Initial predicate> (policy provider_idempotency)
    State 2: CreateHead
    State 3: ClientRead
    State 4: PutPayload (payload object written)
    State 5: BrokenAckBeforeCas (client answered with accept_seq 0; HEAD
             holds no receipt and no input)

## What the switch breaks

The enqueue is acknowledged once its payload object exists, before the
HEAD CAS that links the payload, allocates `accept_seq` and records the
receipt has returned. `AgentIntegrated` keeps the answer (`Ack`) as its own
action after a successful `EnqueueCas` or a `Resolve` that found the
receipt.

## Why it is a real bug if it ever held

After state 5 the client holds a handle for an input no HEAD references.
If the CAS now fails (another writer moved HEAD) or the process dies, the
request is lost while the client believes it was accepted: a retry with the
same request id finds no receipt and enqueues again, and nothing ever runs
the accepted input. `AcceptedImpliesRecoverable` requires every answered
handle to be on the HEAD's receipts with its payload present.
