# Counterexample: lead-one

Config: `negative/lead-one.cfg` (`L = 1` against the shipped `L = 2` that
`MIN_LEAD_HOURS` demands at `C = 1`, no skew, `MaxHour = 3`). Property violated:
`LeadCoversRefreshHorizon`. This is the shallowest control: 199 distinct states,
one second.

A decrease is appended at some router hour with an activation only one hour
later. A writer that refreshed at that same hour holds a view whose newest known
generation is generation 0; one hour later, still inside the cached and grace
window that `C = 1` and `MinLeadHours = 2` allow, generation 1 is already active.
The writer's view is now serving a route hour at which a generation it does not
know is active.

`LeadCoversRefreshHorizon` states exactly the rule a one-hour lead breaks: every
activation must sit at least `MinLeadHours` past any refresh that could still be
serving, so that a writer's view is never behind the active generation at its own
route hour. A lead of one is short of the two-hour refresh horizon, so the
invariant fails with no clock skew required. The shipped `ceil(C) + 1` lead is
what closes this gap.
