# ADR-0038: Drop empty-valued labels at OTLP and OTAP admission

Status: Accepted

## Context

`SeriesId::compute` (ADR-0005, `crates/ravel-types/src/lib.rs`) hashes every
label in the `LabelSet` it is handed, filtering nothing. The three metric
ingest paths disagree on how they build that `LabelSet` when a label has an
empty value:

- `ravel-otlp::normalize` maps an absent `AnyValue` (`None`) and an empty
  `StringValue("")` alike to `Ok(String::new())` in
  `any_value_to_label_value`, then admits the result as a label. An attribute
  with no value, or with the empty string, becomes a label that participates
  in identity.
- `ravel-otap::normalize` does the same for the columnar path:
  `ANY_VALUE_TYPE_EMPTY` and a null string cell both resolve to
  `RawCell::Str(String::new())` and are admitted as labels.
- `ravel-remote-write::normalize` does the opposite. Its
  `resolve_series_identity` drops an empty-valued label before identity is
  computed (`if l.name == METRIC_NAME_LABEL || l.value.is_empty() { continue;
  }`), per Prometheus convention.

`ravel-promql::matchers` already assumes the remote-write behavior is the
universal rule. Its matcher reads a missing label as `""`
(`labels.get(&self.name).unwrap_or("")`) and its module comment documents that
an absent label and a label present with the empty string are the same thing
for every operator. That assumption only holds if storage never contains two
series that differ solely by one carrying an empty-valued label and the other
lacking it.

Net effect, measured for tenant `t`, metric `up`:

```
otlp   id(region="")   == 2794385e083c9bcc4462da69274191f5
otlp   id(region=None) == 2794385e083c9bcc4462da69274191f5
otlp   id(region absent) == 7a4a7708ba8b6c5fff8b750d3bf92a5e
rw     id(region="")   == 7a4a7708ba8b6c5fff8b750d3bf92a5e
```

The same logical series (metric `up`, no meaningful `region`) lands on two
different `SeriesId` values depending on which ingest path admitted it. A
`{region=""}` PromQL query, which the matcher intends to treat as "series
without a real `region`", matches both, so one logical series shows up as two.

This is the same disagreement analyzed in ADR-0031 (Proposed), which proposed
one canonical rule across all paths plus a separate empty-*name* rejection and
an RW1 decode fix. This ADR decides and implements only the empty-*value* half
of that proposal, for the OTLP and OTAP paths. The empty-name
rejection and the RW1 decode change remain future work under ADR-0031; nothing
here contradicts them.

## Decision

At OTLP and OTAP admission, a label whose value is the empty string (including
an OTel `AnyValue` that is absent or resolves to `""`) is treated as absent
from the series and dropped before `SeriesId::compute` is called. This matches
what `ravel-remote-write` already does and what `ravel-promql`'s matcher
already assumes, and it is the direction that removes the query-side ambiguity:
because the matcher reads a missing label as `""`, dropping empties at
admission makes "absent" and "present-but-empty" the single storage state the
matcher already expects.

Implementation:

- `ravel-otlp`: `push_checked` returns early (no push) when the value is
  empty, before the length checks. Every admitted label -- data-point
  attributes, resource-derived `job`/`instance`/allowlisted labels, and the
  synthesized `le`/`quantile` labels -- flows through this one funnel, so the
  drop is applied uniformly. The metric name (`__name__`) is pushed directly
  and is validated for emptiness separately, so it is unaffected.
- `ravel-otap`: the mirrored `push_checked` gets the identical early return,
  and `check_attrs_in_input_order` (the ordered validation pass that mirrors
  OTLP's per-attribute loop for the ADR-0011 identical-rejection-class
  contract) skips an empty-valued label the same way, so an empty-valued label
  with an over-long name is dropped rather than rejected on both paths, exactly
  as remote-write already drops it before its length check.

### No identity-encoding version bump

`SeriesId::compute` and its canonical byte encoding (`"ravel-series-v1\0"...`,
a frozen contract) do not change. The encoder still hashes whatever `LabelSet`
it is given, byte for byte as before. What changes is only which labels the
OTLP and OTAP admission paths put into that `LabelSet`. A `SeriesId` is an
opaque content address used as a storage key; it is never decoded, so there is
no stored-format reader that must understand a new version. The dual-reader
question the format-change procedure asks therefore has no reader side here:
already-stored ids remain valid keys on immutable objects, and only future
admission computes ids under the new rule. No domain-string bump (no
`ravel-series-v2`) is introduced or needed.

## Consequences

- **Identity changes for future OTLP/OTAP admissions of the affected shape.**
  A series that OTLP or OTAP previously keyed with an empty-valued label now
  hashes to a different `SeriesId`. Concretely, for tenant `t`, metric `up`:

  ```
  before (OTLP/OTAP, region=""):        2794385e083c9bcc4462da69274191f5
  after  (OTLP/OTAP, region="" dropped): 7a4a7708ba8b6c5fff8b750d3bf92a5e
  ```

  The "after" value equals the id OTLP already produced for `region` absent
  and the id remote-write already produced for `region=""`, so all three paths
  now converge on `7a4a7708ba8b6c5fff8b750d3bf92a5e` for this logical series.

- **No migration of already-stored data is in scope.** Data objects, commit
  records, and indexes are immutable (a core invariant). Series ingested
  through OTLP or OTAP before this change keep the ids they were written with;
  those objects stay readable and queryable. There is no rewrite, no dual
  identity path, and no attempt to merge an old empty-valued-label series with
  its new empty-dropped counterpart. A `{region=""}` query spanning both the
  pre-change and post-change data will still match both (the matcher treats
  absent and empty alike), but they remain two distinct stored series with two
  distinct ids until retention clears the old one. Only future admission is
  governed by this ADR.

- **Cross-path consistency for new data.** After this change, OTLP, OTAP, and
  remote-write hand `SeriesId::compute` a byte-identical `LabelSet` for the
  same logical series regardless of wire format, so cross-path deduplication
  and query results are consistent for newly ingested data. A cross-path test
  (`crates/ravel-otap/tests/cross_path_empty_label_identity.rs`) pins this.

- **Complex-value rejection is unaffected.** OTLP/OTAP still reject
  array/kvlist/bytes attribute values as `ComplexAttributeValue`; only the
  empty-string and no-value cases change from "admit as empty label" to "drop
  as absent".

## Alternatives considered

**Make remote-write admit empty-valued labels instead** (align on OTLP/OTAP's
old behavior). Rejected for the reasons ADR-0031 sets out in detail: it would
leave `ravel-promql::matchers` broken, since the matcher has no way to
distinguish "absent" from "present-and-empty" once it reads through
`unwrap_or("")`, so `{region!=""}` would wrongly exclude a present-but-empty
series. Fixing the matcher to draw that distinction would mean rewriting the
Prometheus absent-label semantics users expect, a far larger change than
aligning two ingest paths onto the convention the third already follows.

**Filter empties inside `SeriesId::compute`** (drop empty-valued labels in the
hasher itself). Rejected: it would change the frozen identity contract's
behavior for every caller and every wire format at once, including callers that
legitimately want to hash exactly the `LabelSet` they hold, and it would bury a
data-model admission decision inside a pure encoding primitive. The drop
belongs at the ingest boundary, where remote-write already places it.
