# Counterexample: token-validated-against-count

Config: `negative/token-validated-against-count.cfg`
(`TokenValidatedAgainstCount = TRUE`, `AppenderSkew = 2`, two admits,
`MaxHour = 3`). Property violated: `TokenResolvesAcrossReshards`.

A writer routes a straggler on generation 0's count of 2 and lands a record on
shard 1, receiving a commit token that names shard 1. A skewed appender then
activates a decrease to count 1. The reader resolves the token.

Correct resolution is an exact-key GET: the token names a durable object key, and
that object exists regardless of the current active count, so the read-your-write
succeeds. The negative switch adds a check that rejects a token whose shard index
is at or above the currently active count. After the decrease the active count is
1, and the straggler token names shard 1, so the added check rejects a token that
points at durable data. The resolve returns not-found, and
`TokenResolvesAcrossReshards`, which requires a resolve of a real token to
succeed across a reshard, fails.

The lesson the control pins: commit-token resolution must be identity by key, not
a function of the mutable active count. A count check couples a durable read to a
value that changes under it, breaking read-your-writes exactly across the reshard
the token was meant to survive.
