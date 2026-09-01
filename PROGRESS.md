# Progress

This file records delivered epics: what shipped and what was deliberately left
out. It complements [CHANGELOG.md](CHANGELOG.md), which tracks releases; this
file tracks the larger bodies of work behind them.

## Epic #8: RSPAN v2/v3/v4 trace investigation

Turned RSPAN from a correct span storage format into a span investigation
format, and made the `spans` SQL table reachable. Design in
[ADR-0041](docs/adrs/0041-rspan-v1-span-segment-format.md),
[ADR-0045](docs/adrs/0045-rspan-v2-trace-investigation.md), and
[ADR-0054](docs/adrs/0054-rspan-v3-bloom-and-service-name.md); on-disk layout in
[docs/span-segment-format.md](docs/span-segment-format.md); the query surface in
[docs/guides/traces.md](docs/guides/traces.md) and the `spans` section of
[docs/query-engine.md](docs/query-engine.md).

Shipped:

- **RSPAN v2 skip-index fields.** Per block, `min/max_duration_ns` and a
  one-byte `status_mask`. A `duration_ns` window or a `status_code` predicate
  now prunes blocks.
- **RSPAN v3 bloom and service name.** A per-block BLOOM section over
  `service.name` and span `name` tokens, and a block-local `service_name`
  column lifted out of the `attrs` map. `service_name = '...'` and `name =
  '...'` now prune by bloom membership.
- **RSPAN v4 attribute columns and span events.** Per-key typed attribute
  columns with an overflow that stays scan-queryable, and span events as nested
  columns (including exception stack traces) rather than an opaque blob.
- **Ranged RSPAN reads.** A range reader so span compaction no longer decodes
  every input object whole into memory.
- **The `spans` SQL table.** Registered on `POST /api/v1/sql` alongside
  `samples` and `logs`, under the same one-signal-per-query rule. Widen-only
  pushdown for `trace_id`, time window, `duration_ns` (computed, not stored),
  `status_code`, `service_name`, and `name`.

Deliberately not shipped:

- **Span links stay unstored.** A link to a span in another trace is out of
  RSPAN scope. It will get its own design decision when a query needs it
  (ADR-0045).
- **Span postings.** Attribute-equality pruning over span attributes, analogous
  to RLOG POSTINGS (ADR-0049), is a separate, undecided epic with its own cost
  model. It follows the log postings work; its absence is not a gap in this
  epic. Until then, an `attrs['k'] = 'v'` predicate on `spans` is evaluated
  exactly as a residual and prunes no blocks.
</content>
