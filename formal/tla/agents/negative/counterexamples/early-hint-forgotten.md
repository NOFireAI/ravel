# Counterexample: sweep deletes a hint before it is due

Config: `negative/early-hint-forgotten.cfg` (`EarlyHintForgotten = TRUE`).
Expected: `DueTimerEventuallyDelivered` violated under `MCFairSpec`, TLC
exit 13. The harness runs this config with every PROPERTY line replaced by
`PROPERTY DueTimerEventuallyDelivered`.

Observed TLC line:

    Error: Temporal properties were violated.

Trace (9 states then stuttering; 5076 distinct states found, 1769 left on
the queue when TLC's liveness check found the cycle; 2 workers, `-Xmx4g`).
TLC labels every step `MCNext`; the action names are read off the state.
Where the liveness check runs depends on worker timing, so another run can
report a longer path to the same cycle.

    State 1:  <Initial predicate>
    State 2:  CreateHead
    State 3:  ScheduleTimer (generation 1 armed)
    State 4:  WriteHint (hint for generation 1 written)
    State 5:  PutPayload(c1)
    State 6:  Takeover(w1) (epoch 1)
    State 7:  BrokenEarlyHintForgotten (hint for generation 1 deleted while
              it is not yet due)
    State 8:  TimerDue (generation 1 due)
    State 9:  EnqueueCas(c1) fails on the moved version; the client re-reads
    State 10: Stuttering

## What the switch breaks

The hint for an armed generation is written once, after the HEAD CAS that
arms it, and the sweep must leave a hint alone until its generation is due.
The switch lets the sweep delete a hint it lists early.

## Why it is a real bug if it ever held

Generation 1 is armed (state 3) and its hint written (state 4). The sweep
deletes the hint while generation 1 is not yet due (state 7), and then it
comes due (state 8). The root still has generation 1 armed, nothing will
write the hint again, and the sweep fires only from a present hint, so the
behavior stutters forever with a due, armed timer. That is the response
`DueTimerEventuallyDelivered` requires, and fairness on `FireTimer` does not
help because `FireTimer` is never enabled. States 5, 6 and 9 are incidental
client and worker steps; none of them is needed for the violation. The
correct model (switch FALSE) never deletes a hint whose generation is not
yet due.
