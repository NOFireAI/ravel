# Counterexample: no-writer-fence

Config: `negative/no-writer-fence.cfg` (`WriterFenceEnabled = FALSE`,
`AppenderSkew = 2`, two admits, `MaxHour = 4`). Property violated:
`StaleWriterFailsClosed`.

A writer refreshes once and records its view's refresh hour (admit 1). Time
advances past that refresh hour plus `MinLeadHours`, so the view is now past both
the freshness window and the grace horizon; a skewed appender has activated a
decrease in the meantime. On the next admit the writer's refresh would fail, and
with the fence on it would surface `StaleProvisioningView` and stop. With the
fence off it instead routes on the expired view and admits (admit 2) on that
cached view, at a route hour more than `MinLeadHours` past its last refresh.

`StaleWriterFailsClosed` requires that any admit on a cached or grace view happen
within `MinLeadHours` of the refresh that produced it. This admit is past that
horizon, so the invariant fails. Note the read-side slack still covers the
admitted record's data at this horizon, so `EveryAdmittedWriteInScanSet` does not
break here: the sharp witness is the fence's own contract, not the downstream
data-loss property. The fence is what turns an expired view into a fail-closed
error instead of a stale route.
