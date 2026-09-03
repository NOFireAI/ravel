# Negative control: gc-config-violates-inequality

Switch: `GcConfigViolatesInequality = TRUE` (initialises `sysgc.ph` from
`BadProtectionHorizon = 0`, below `max_query_duration + grace + clock_skew`).
All other switches at base.

Target invariant: `GcConfigSatisfiesHorizon`. TLC exit 12.

```
Error: Invariant GcConfigSatisfiesHorizon is violated by the initial state:
```

Trace: the initial state alone violates the invariant. `sysgc.ph = 0` while
`sysgc.mqd + sysgc.grace + sysgc.skew = 1`, so `ph >= mqd + grace + skew` fails
at Init. This is the startup-configuration guard: a GC configured below the
horizon inequality is rejected before any action runs.
