# query-reads-uncommitted-data

Negative control. `QueryReadsDataDirectly = TRUE` makes `RunQuery` include a
flush whose data object is present but whose commit record is not yet
durable, which every flush passes through in the ordinary "data" phase
before its commit lands.

Violated invariant: `NoUncommittedDataVisible` (safety, TLC exit 12).

Trace, in prose: writer `w1` PUTs its data object for a flush on shard `s1`
and, before the commit record is written, a query runs. With the switch
flipped, the query resolves the data object directly instead of gating on
the commit record, so it returns content that no commit token names yet.
TLC finds this at depth 5 (98 states generated, 79 distinct).

The correct model gates query visibility on the commit record, which is
what `crates/ravel-query`'s resolution path does: a query never reads a
flush's data object until its commit record is durable.
