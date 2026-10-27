# Counterexample: completion rebase ignores a takeover

Config: `negative/epoch-checked-outside-cas.cfg` (`EpochCheckedOutsideCas = TRUE`).
Expected: `NoStaleOwnerCommit` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant NoStaleOwnerCommit is violated.

Trace (10 states, depth 10):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <ReadHead in AgentProtocol>
    State 4: <CasHead in AgentProtocol>
    State 5: <Takeover in AgentProtocol>
    State 6: <BeginTurn in AgentProtocol>
    State 7: <PutTurnOutput in AgentProtocol>
    State 8: <Takeover in AgentProtocol>
    State 9: <CompleteTurn in AgentProtocol>
    State 10: <BrokenEpochCheckedOutsideCas in MCAgentProtocol>

## What the switch breaks

When a turn's completion CAS fails, the worker re-reads HEAD and may reapply
the completion only if the owner, the epoch, the active turn and the base
state version it ran against are all unchanged. The switch checks the active
turn and base state version and skips the owner and epoch, so it reapplies on
a root another worker has taken over.

## Why it is a real bug if it ever held

The first worker claims the agent (state 5, epoch 1) and runs a turn (states 6
and 7). The second worker takes over (state 8, epoch 2) without disturbing the
active turn. The first worker's completion CAS fails on the moved version
(state 9), and the broken rebase commits the result anyway (state 10). That
result carries owner and epoch 1 on a root whose owner is the second worker at
epoch 2, which `NoStaleOwnerCommit` forbids: a fenced-out worker has written
the agent's state. The correct model (switch FALSE) drops the turn on the
rebase and leaves the new owner to run it.
