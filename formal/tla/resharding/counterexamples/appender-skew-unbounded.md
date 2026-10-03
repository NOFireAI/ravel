# Counterexample: appender-skew-unbounded

Config: `negative/appender-skew-unbounded.cfg` (`AppenderSkew = 5` against a
tolerated bound of 1, every other margin shipped, `MaxHour = 5`). Property
violated: `EveryAdmittedWriteInScanSet`.

This is the scan-slack-zero shape with the slack left correct at three hours and
the skew doing the damage instead. A writer refreshes on generation 0's count of
2 and can route on shard 1. The requester appends a decrease to count 1, but its
activation hour is computed on an appender clock five hours behind the routers.
That pulls the activation so far earlier in router time that a straggler the
writer admits on the old count lands at an ingest hour more than three hours past
the activation.

The reader's scan set for that hour, even with the full three-hour slack, no
longer reaches back to generation 0, so shard 1 is outside it and the record is
unlisted. Unlike scan-slack-zero, no margin was removed here: a skew above the
tolerated bound alone pushes the straggler past a correct slack window. This is
the ADR's prime target, the reason `DEFAULT_SCAN_SLACK_HOURS` folds a clock-skew
term into the slack in the first place, and the reason skew above that term is a
durability hazard.
