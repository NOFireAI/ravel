# Counterexample: sweep fires a timer generation the root no longer arms

Config: `negative/stale-timer-generation-fired.cfg` (`StaleTimerGenerationFired = TRUE`).
Expected: `TimerGenerationDeliveredOnce` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant TimerGenerationDeliveredOnce is violated.

Trace (10 states, depth 10):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <ReadHead in AgentProtocol>
    State 4: <CasHeadLostResponse in AgentProtocol>
    State 5: <Takeover in AgentProtocol>
    State 6: <ScheduleTimer in AgentProtocol>
    State 7: <WriteHint in AgentProtocol>
    State 8: <TimerDue in AgentProtocol>
    State 9: <CancelTimer in AgentProtocol>
    State 10: <BrokenStaleTimerGenerationFired in MCAgentProtocol>

## What the switch breaks

A due hint is only a pointer. The sweep may enqueue the timer message only
through a CAS on a root where the hint's generation is still armed, and that
CAS marks it delivered. The switch enqueues from any due hint, whatever the
root's timer state.

## Why it is a real bug if it ever held

The owner arms generation 1 (state 6), the hint is written (state 7) and comes
due (state 8), and the owner then cancels the timer (state 9). The hint is
still in the store, and the broken sweep enqueues a timer input for
generation 1 on the cancelled root (state 10). `TimerGenerationDeliveredOnce`
requires every new timer input to come from a root where its generation was
armed, so the cancelled timer has fired. The correct model (switch FALSE)
deletes the stale hint instead.
