# ADR-0031: Empty-valued and empty-named labels in series identity

Status: Proposed (2026-07-28)

## Context

`SeriesId::compute` (ADR-0005, `crates/ravel-types/src/lib.rs`) hashes every
label in the `LabelSet` it is given, with no filtering for empty values. The
three ingest paths disagree on what they hand it:

- `ravel-otlp::normalize::any_value_to_label_value`
  (`crates/ravel-otlp/src/normalize.rs:842-845`) maps an absent `AnyValue`
  (`None`) to `Ok(String::new())`, so a resource or data-point attribute with
  no value becomes a label with an empty value and is included in identity.
- `ravel-otap::normalize::raw_cell`
  (`crates/ravel-otap/src/normalize.rs:655-656`) maps
  `ANY_VALUE_TYPE_EMPTY` to `RawCell::Str(String::new())`, the same
  admit-as-empty-string behavior, for the same underlying OTel condition.
- `ravel-remote-write::normalize` (`crates/ravel-remote-write/src/normalize.rs:233-235`)
  does the opposite: `if l.name == METRIC_NAME_LABEL || l.value.is_empty() {
  continue; }` drops an empty-valued label before it ever reaches
  `SeriesId::compute`, per Prometheus convention.

`ravel-promql::matchers` (`crates/ravel-promql/src/matchers.rs:27`) already
depends on the remote-write behavior being the universal rule: its matcher
reads a missing label as `""` (`labels.get(&self.name).unwrap_or("")`) and
documents, in its module comment, that absent and empty-string are treated
identically for every operator. That assumption is only sound if storage
never produces two distinct series that differ solely by one having an
empty-valued label and the other lacking it.

Label *names* have a separate, narrower disagreement. RW1
(`crates/ravel-remote-write/src/rw1.rs`) has no check rejecting an
empty-string label name and will admit it. RW2
(`crates/ravel-remote-write/src/rw2.rs:217-220`,
`Rw2DecodeError::EmptyLabelName`) already rejects an empty label name at
decode, before normalization or identity computation ever see it.

Net effect: the identical logical series (same metric, same real labels,
plus one label OTel or Prometheus considers "no value here") currently maps
to different `SeriesId` values depending on which of the three ingest paths
it came through, and a payload malformed in one specific way (empty label
name) is accepted by one remote-write wire format and rejected by the other.

## Decision

One canonical rule, applied at every ingest path before `SeriesId::compute`
is called:

1. **Empty-valued label, or an OTel `AnyValue` with no value set, is treated
   as absent from the label set for series-identity purposes.** This matches
   Prometheus convention, matches what `ravel-remote-write` already does,
   and matches what `ravel-promql`'s matcher already assumes.
2. **An empty-named label is always rejected as malformed input**, at every
   ingest path, independent of its value. This matches what RW2 already
   does at decode.

Code that must change to conform:

- `ravel-otlp`: `any_value_to_label_value` (or its caller) must drop the
  label from the outgoing `LabelSet` when the `AnyValue` is absent or
  resolves to an empty string, instead of encoding it as
  `Ok(String::new())` and letting it flow into identity.
- `ravel-otap`: `raw_cell`'s `ANY_VALUE_TYPE_EMPTY` arm (or the label-set
  assembly that consumes its output) must exclude the attribute from the
  label set rather than materializing it as `RawCell::Str(String::new())`
  and admitting it as a label.
- `ravel-remote-write` (RW1 only): the decode/parse path must reject a
  label with an empty name the same way `Rw2DecodeError::EmptyLabelName`
  does, rather than admitting it silently.

Code that is already correct and needs no change:

- `ravel-remote-write`'s empty-*value* handling
  (`normalize.rs:233-235`) already drops the label; this ADR ratifies that
  behavior as the cross-path standard rather than a remote-write-specific
  quirk.
- `ravel-remote-write`'s RW2 empty-*name* rejection
  (`rw2.rs:217-220`) already matches the decision; RW1 is the outlier.

`SeriesId::compute` itself does not change: it continues to hash whatever
`LabelSet` it is given. The fix is that all three ingest paths must agree on
constructing that `LabelSet` the same way before calling it.

## Consequences

- Changing which labels participate in identity changes which `SeriesId` a
  given logical input maps to, for data already ingested through the OTLP
  or OTAP paths before this change lands. A metric that previously
  included an empty-valued or absent-value label in its identity hash will,
  after the fix, hash to a different `SeriesId` than it did before, even
  though nothing about the logical series changed from a user's
  perspective. No in-place migration path for previously-ingested data is
  proposed here; that is a separate concern from the identity rule itself.
- Once this lands, all three ingest paths produce byte-identical
  `LabelSet` inputs to `SeriesId::compute` for the same logical series,
  regardless of wire format, so cross-path deduplication and query
  results are consistent for newly ingested data.
- The OTLP/OTAP rejection-on-complex-value behavior
  (`Err(Rejection::ComplexAttributeValue)` for array/kvlist/bytes) is
  unaffected; only the no-value (`None`) case changes.

## Alternatives considered

**Make remote-write match OTLP/OTAP's current behavior instead** (admit
empty-valued labels into identity everywhere, rather than dropping them
everywhere). Rejected: this would keep `ravel-promql::matchers` broken
rather than fix it. The matcher already treats a missing label and a
label present with an empty string as the same thing for every operator
(`=`, `!=`, `=~`, `!~`), documented explicitly in its module comment
(`matchers.rs:1-18`) as the load-bearing assumption behind results like
`{foo=""}` matching series without `foo` at all. If storage instead
treated "label present with empty value" and "label absent" as two
distinct series, the matcher would silently conflate query results across
series the storage layer considers different: a query for `{foo=""}`
would match both, but a query for `{foo!=""}` would incorrectly exclude
the series that has `foo` present-but-empty, since the matcher has no way
to distinguish "absent" from "present-and-empty" once it reads through
`unwrap_or("")`. Fixing the matcher to stop making that assumption would
mean rewriting the PromQL absent-label semantics that Prometheus users
expect, a far larger and more disruptive change than aligning two ingest
paths. Keeping OTLP/OTAP's current admit-empty behavior also does nothing
about the RW1/RW2 empty-*name* mismatch, which points the same direction
(reject) regardless of which way the empty-*value* decision goes.
