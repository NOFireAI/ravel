# Counterexample: turn consumes its input without publishing a result

Config: `negative/cursor-without-result.cfg` (`CursorWithoutResult = TRUE`).
Expected: `TurnCommitAtomic` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant TurnCommitAtomic is violated.

Trace (8 states, depth 8):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <ReadHead in AgentProtocol>
    State 4: <CasHead in AgentProtocol>
    State 5: <Takeover in AgentProtocol>
    State 6: <BeginTurn in AgentProtocol>
    State 7: <PutTurnOutput in AgentProtocol>
    State 8: <BrokenCursorWithoutResult in MCAgentProtocol>

## What the switch breaks

The turn-completion CAS is one HEAD write that removes the input from the
inbox, appends its result, and moves the state version and state reference.
The switch keeps the inbox removal and the state move and leaves the result
out.

## Why it is a real bug if it ever held

`TurnCommitAtomic` compares the root before and after each HEAD write: the
inputs a write consumes must be exactly the inputs it publishes results for.
In state 8 the input leaves the inbox and no result appears, so the sender
waiting on the handle never sees an outcome, and the input cannot be retried
because it is no longer pending. The correct model (switch FALSE) publishes
the result in the same CAS that consumes the input.
