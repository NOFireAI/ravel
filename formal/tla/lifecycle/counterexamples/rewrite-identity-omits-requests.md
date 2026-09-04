# Negative control: rewrite-identity-omits-requests

Switch: `RewriteIdentityOmitsRequests = TRUE` (the rewrite identity key drops the
applied-request set, so two rewrites over the same input set with different
applied requests hash to the same key). All other switches at base.

Target invariant: `IdenticalInputSetsDoNotCollide`. TLC exit 12.

```text
Error: Invariant IdenticalInputSetsDoNotCollide is violated.
```

Trace: `PerformRewrite` materialises `rwA`. With `rwA` present the invariant is
evaluated: `Cardinality({RewriteKey(d) : d \in Materialized})` is less than
`Cardinality(Materialized)` because two descriptors over the same inputs and
different request sets collapse to one key, so the equality fails.
