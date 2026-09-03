# deadline-reachable

Obligation, not a bug. `AbandonUnreachable` is a predicate that must fail:
the flush-lifetime deadline has to be reachable at the exhaustive bounds, or
every property about abandonment (`NoPublishAfterAbandon` among them) is
vacuous there.

Violated invariant: `AbandonUnreachable` (safety, TLC exit 12).

Trace, in prose: writer `w1` pins a flush on shard `s1` with content `c1` at
clock 0. Nothing else happens to that flush: no data PUT, no commit PUT,
nothing. The clock ticks three times, from 0 to 3, with the flush sitting in
the `pinned` phase throughout. Once the clock passes `openedAt + FlushLifetime`
(0 + 2), the flush is expired, and `Abandon` fires: the flush moves to the
`abandoned` phase and the client is told an explicit error, since nothing had
told it anything yet.

That is exactly the state `AbandonUnreachable` says must not exist, and TLC
found it in six steps. The obligation is correct exactly because TLC reports
it violated: a model where the deadline never actually fires would leave this
green, and every abandonment property would be checking nothing.
