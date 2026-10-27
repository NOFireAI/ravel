# Counterexample: completion rebase ignores a cancelled turn

Config: `negative/stale-turn-rebase.cfg` (`StaleTurnRebase = TRUE`).
Expected: `OneLogicalCompletion` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant OneLogicalCompletion is violated.

Trace (10 states, depth 10):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <ReadHead in AgentProtocol>
    State 4: <CasHead in AgentProtocol>
    State 5: <Takeover in AgentProtocol>
    State 6: <BeginTurn in AgentProtocol>
    State 7: <PutTurnOutput in AgentProtocol>
    State 8: <Cancel in AgentProtocol>
    State 9: <CompleteTurn in AgentProtocol>
    State 10: <BrokenStaleTurnRebase in MCAgentProtocol>

## What the switch breaks

The completion rebase must find the same active turn it ran. The switch
checks owner, epoch and base state version and skips the active turn, so it
reapplies after the turn's input was cancelled and the active turn cleared.

## Why it is a real bug if it ever held

The worker runs a turn on seq 0 (states 6 and 7). The sender cancels it
(state 8): HEAD records a cancelled result for seq 0 and clears the active
turn. The worker's completion CAS fails on the moved version (state 9) and the
broken rebase commits anyway (state 10). HEAD now holds two results for one
input:

    results |-> { [ inc |-> 1, epoch |-> 0, owner |-> noworker, turn |-> 0, sv |-> 0, seq |-> 0, kind |-> "cancelled" ], [ inc |-> 1, epoch |-> 1, owner |-> w1, turn |-> 1, sv |-> 1, seq |-> 0, kind |-> "completed" ] }

`OneLogicalCompletion` allows at most one result per accepted input. Run once
per invariant, the same mutant also breaks `TurnCommitAtomic`,
`NoStaleOwnerCommit`, `CancellationRespected` (a cancelled input completed)
and `TenantIncarnationIsolation`; TLC reports only `OneLogicalCompletion`
because it stops at the first violated invariant in config order.
The correct model (switch FALSE) drops the turn on the rebase.
