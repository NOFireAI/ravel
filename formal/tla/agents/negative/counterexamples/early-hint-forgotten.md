# Counterexample: sweep deletes a hint before it is due

Config: `negative/early-hint-forgotten.cfg` (`EarlyHintForgotten = TRUE`).
Expected: `DueTimerEventuallyDelivered` violated under `MCFairSpec`, TLC
exit 13. The harness runs this config with every PROPERTY line replaced by
`PROPERTY DueTimerEventuallyDelivered`.

Observed TLC line:

    Error: Temporal properties were violated.

Trace (18 states then stuttering; 12988 distinct states, search complete):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <ReadHead in AgentProtocol>
    State 4: <Destroy in AgentProtocol>
    State 5: <CasHeadLostResponse in AgentProtocol>
    State 6: <Recreate in AgentProtocol>
    State 7: <ReadHead in AgentProtocol>
    State 8: <CasHead in AgentProtocol>
    State 9: <Takeover in AgentProtocol>
    State 10: <BeginTurn in AgentProtocol>
    State 11: <PutTurnOutput in AgentProtocol>
    State 12: <WorkerCrash in AgentProtocol>
    State 13: <ScheduleTimer in AgentProtocol>
    State 14: <PutTurnOutput in AgentProtocol>
    State 15: <WriteHint in AgentProtocol>
    State 16: <FailTurn in AgentProtocol>
    State 17: <BrokenEarlyHintForgotten in MCAgentProtocol>
    State 18: <TimerDue in AgentProtocol>
    State 19: Stuttering

## What the switch breaks

The hint for an armed generation is written once, after the HEAD CAS that
arms it, and the sweep must leave a hint alone until its generation is due.
The switch lets the sweep delete a hint it lists early.

## Why it is a real bug if it ever held

Generation 1 is armed (state 13) and its hint written (state 15). The sweep
deletes the hint while generation 1 is not yet due (state 17), and then it
comes due (state 18). The root still has generation 1 armed, nothing will
write the hint again, and the sweep fires only from a present hint, so the
behavior stutters forever with a due, armed timer. That is the response
`DueTimerEventuallyDelivered` requires, and fairness on `FireTimer` does not
help because `FireTimer` is never enabled. States 2 to 12 are an incidental
path TLC found first (a destroy and recreate, a lost CAS response, a worker
crash); none of them is needed for the violation. The correct model (switch
FALSE) never deletes a hint whose generation is not yet due.
