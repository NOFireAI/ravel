# Counterexample: HEAD links a payload that was never written

Config: `negative/head-before-dependency.cfg` (`HeadBeforeDependency = TRUE`).
Expected: `PublishedReferencesComplete` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant PublishedReferencesComplete is violated.

Trace (3 states, depth 3):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <BrokenHeadBeforeDependency in MCAgentProtocol>

## What the switch breaks

Dependencies are written before the root that names them: a send PUTs its
payload object first and only then CASes HEAD to add the input. The switch
CASes HEAD straight from the read, adding an input whose payload key has no
object.

## Why it is a real bug if it ever held

`PublishedReferencesComplete` checks every client input in HEAD against the
store. One CAS is enough to break it: HEAD names a payload that does not
exist, and a worker that begins a turn on that input reads a missing object.
A crash between the CAS and a late payload PUT makes the gap permanent. The
correct model (switch FALSE) cannot publish the input until its payload PUT
has succeeded.
