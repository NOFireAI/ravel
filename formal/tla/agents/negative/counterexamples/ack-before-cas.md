# Counterexample: sender acknowledged before the enqueue CAS

Config: `negative/ack-before-cas.cfg` (`AckBeforeCas = TRUE`).
Expected: `AcceptedImpliesRecoverable` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant AcceptedImpliesRecoverable is violated.

Trace (4 states, depth 4):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <ReadHead in AgentProtocol>
    State 4: <BrokenAckBeforeCas in MCAgentProtocol>

## What the switch breaks

A send is accepted only when the enqueue CAS on HEAD succeeds: that write is
what adds the receipt and the input. `BrokenAckBeforeCas` hands the client its
handle as soon as the payload object is written, while the client still holds
the HEAD version it read and has not attempted the CAS.

## Why it is a real bug if it ever held

`AcceptedImpliesRecoverable` reads the handle the client holds and the store:
a handed-out receipt must be in the live HEAD of its incarnation. After state
4 the payload object is present but HEAD carries no receipt, so a caller that
retries with the same request id, or polls the handle, finds nothing. If the
process dies here the send is lost after the caller was told it was accepted.
The correct model (switch FALSE) acknowledges only from the successful CAS.
