# Progress

This file records delivered epics: what shipped and what was deliberately left
out. It complements [CHANGELOG.md](CHANGELOG.md), which tracks releases; this
file tracks the larger bodies of work behind them.

## Epic #1101: the `alerts` and `audit` SQL tables

Made two signals that were already written durably readable. Alert transitions
and audit records had providers, pushdown extractors and scan leaves, all
tested, and no production path constructed either one: a query naming `alerts`
or `audit` failed at planning. Design in
[ADR-1101](docs/adrs/1101-alerts-and-audit-sql-tables.md), on top of
[ADR-0040](docs/adrs/0040-alerts-and-audit-signals.md) and
[ADR-0042](docs/adrs/0042-compliance-custody.md); the operator surface in
[docs/guides/alerting.md](docs/guides/alerting.md) and
[docs/guides/audit.md](docs/guides/audit.md), the reference in the alerts and
audit section of [docs/query-engine.md](docs/query-engine.md).

Shipped:

- **The `alerts` and `audit` tables.** Registered as the fourth and fifth
  tables on `POST /api/v1/sql` and Flight SQL, under the same
  one-signal-per-query rule: `target_signal` counts five names and rejects a
  query naming two before any catalog listing. Both providers read through the
  logs fetcher, so they are cached, tenant-checked and accounted exactly as a
  `logs` query is, and cost estimation reuses the logs estimator.
- **Flight `GetTables` lists all five tables** with their public schemas, built
  from the five schema functions rather than from the per-query session, which
  still registers exactly one table. It previously listed only `samples`,
  under-reporting `logs` and `spans` too.
- **Write-identity columns on `alerts`.** `writer_id`, `writer_epoch` and
  `writer_seq`, stamped from each record's commit record, so the fold to
  current state has a total order and cannot return two current rows for one
  alert when two evaluators overlap at a lease handover.
- **A read-side shard floor for fixed-shard signals.** `Signal` gained a fixed
  read-shard count (1 for alerts, 2 for audit), and the catalog's three
  scan-set derivations take the maximum of the provisioning history and that
  floor through one shared helper. The writer shard constants assert against
  the floor at compile time. Without it, an `audit` query on a `--shards 1`
  deployment would have listed only the legal-hold shard and returned an
  exact-looking answer with every statement missing.

Deliberately not shipped:

- **Fold and maintain coverage for the two signals.** Neither signal is folded
  into the catalog, and neither rides the maintain loop's compaction pass, so
  an `alerts` or `audit` query lists commit records live for its window. That
  is the recent-hours cost a logs query already pays, and volume is one object
  per alert transition and one per executed statement. The query-audit shard
  keeps its own retention pass, which compacts and age-sweeps it on a 90-day
  window; the legal-hold shard is never deleted. Tracked as #1137; folding
  becomes worth doing if audit volume makes the listing the dominant term.
- **The bytes-scanned budget and the LIMIT fetch-stop hint.** Both are missing
  on every RLOG and RSPAN scan loop, not just these two tables, and stay out of
  scope here. Registering the tables adds two more callers to the same gap and
  changes nothing about it. Tracked as #41 and #362.
- **The alerts-on-alerts generation guard.** A rule can now read the `alerts`
  table, but the evaluator still passes no consumed generations to
  `compute_generation`, so every such record is pinned at generation 1 and
  ADR-0040 decision 4's cap never trips. Carrying the consumed rows'
  generations out of the query path changes a result type across two crates,
  which is its own change. Tracked as #1174.
- **A fencing epoch for the alerts fold.** The fold key orders by
  `writer_epoch` and `writer_seq`, but the writer epoch is a constant and each
  evaluator's sequence restarts at 1, so across a lease handover the key
  prefers the departing evaluator rather than the later write. One current row
  per alert is guaranteed either way, and the SQL fold matches the evaluator's
  own. Tracked as #1175.
- **The query-audit pipeline install.** The `audit` table reads back whatever
  is stored, and on a stock build that is legal-hold and reshard records only:
  every query surface submits its per-statement event through a sink that
  startup fills with the no-op, and nothing outside the audit crate's own
  tests constructs the real pipeline. So `attrs['kind'] = 'query'` selects
  nothing, and `audit_mode=required` cannot fail closed either, since the
  no-op always reports success. Pre-existing, found while writing these docs,
  and named in them rather than papered over. Tracked as #1187.

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
