# Counterexample: a reused request id with a different payload is accepted

Config: `negative/request-hash-mismatch-accepted.cfg` (`RequestHashMismatchAccepted = TRUE`).
Expected: `RequestIdentityStable` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant RequestIdentityStable is violated.

Trace (6 states, depth 6):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <ReadHead in AgentProtocol>
    State 4: <CasHead in AgentProtocol>
    State 5: <BrokenRequestHashMismatchAccepted in MCAgentProtocol>
    State 6: <CasHead in AgentProtocol>

## What the switch breaks

A send whose request id HEAD already holds must compare payload hashes: the
same hash returns the existing handle, a different hash returns
IdempotencyConflict. The switch takes the different-hash case down the normal
enqueue path instead.

## Why it is a real bug if it ever held

Both clients use request id r1, one with payload hash h2 and one with h1. The
h2 send is enqueued at seq 0 (state 4). The h1 send reads that receipt and,
under the switch, writes its payload and enqueues it (states 5 and 6). HEAD
then holds two receipts for r1:

    receipts |-> { [rid |-> r1, inc |-> 1, hash |-> h1, seq |-> 1], [rid |-> r1, inc |-> 1, hash |-> h2, seq |-> 0] }

`RequestIdentityStable` requires one (hash, accept_seq) pair per request id
per incarnation. A retry of r1 now resolves to either input depending on
which receipt it reads. The correct model (switch FALSE) answers the h1 send
with IdempotencyConflict and writes nothing.
