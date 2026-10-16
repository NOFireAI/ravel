# Counterexample: scan-slack-zero

Config: `negative/scan-slack-zero.cfg` (`S = 0`, `AppenderSkew = 2`, one
writer, one requester, one decrease). Property violated:
`EveryAdmittedWriteInScanSet`.

Generation 0 has count 2. A writer refreshes its cached view while only
generation 0 exists, so it routes on count 2 and may place a record on shard 1.
Meanwhile the requester appends a decrease to count 1. Its activation hour is
computed on the appender's clock, which trails the routers by two hours, so the
activation lands earlier in router time than the shipped lead would have placed
it: the writer's still-fresh view has not refreshed past it.

The writer admits a record on shard 1 at an ingest hour at or after that
activation. With the slack removed, the scan set for that hour is generation 1
alone, whose count is 1, so it lists only shard 0. Shard 1 holds durable data
the reader never lists, and `EveryAdmittedWriteInScanSet` fails.

The shipped slack of three hours would have kept generation 0 in the scan set
across the activation and covered shard 1. Removing it is the whole defect; the
skew only supplies the post-activation straggler that the slack is there to
catch.
