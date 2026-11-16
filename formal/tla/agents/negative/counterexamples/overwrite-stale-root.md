# Counterexample: enqueue overwrites a root another sender moved

Config: `negative/overwrite-stale-root.cfg` (`OverwriteStaleRoot = TRUE`).
Expected: `NoLostConcurrentEnqueue` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant NoLostConcurrentEnqueue is violated.

Trace (6 states, depth 6):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <ReadHead in AgentProtocol>
    State 4: <ReadHead in AgentProtocol>
    State 5: <CasHead in AgentProtocol>
    State 6: <BrokenOverwriteStaleRoot in MCAgentProtocol>

## What the switch breaks

The enqueue writes HEAD with CasVersion on the version it read, so a root
that moved since is refused and the sender re-reads. The switch writes with
an unconditional overwrite when the only change since the read is another
sender's enqueue (same owner, epoch, timer, results and active turn). The
switch also requires that the other sender used a different request id: with
the same request id and payload the overwrite rewrites an identical input
record, which loses nothing and only breaks `MonotonicIdentity`.

## Why it is a real bug if it ever held

Both senders read the same root (states 3 and 4). The first enqueue lands
(state 5); the second builds its root from the older read and overwrites
(state 6), so the first sender's input is no longer in HEAD. The transition
clause of `NoLostConcurrentEnqueue` (every input the previous root held is
still held) fails on that write. The first sender was acknowledged and its
message is gone. The correct model (switch FALSE) gets PreconditionFailed in
state 6 and retries from a fresh read.
