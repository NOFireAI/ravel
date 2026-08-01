# ADR-0040: `Signal::Alerts` and `Signal::Audit`, sharing RLOG's format

## Context

Two epics in the observability/security program (issue #333) both need a
new kind of durable record that today's `Signal` enum
(`crates/ravel-types/src/lib.rs`: `Metrics`, `Logs`, `Spans`, `Profiles`)
has no slot for:

- Epic 3 (unified alerting) wants alert state itself stored as data, so
  a rule can read the alert history the same way it reads metrics or
  logs, including a rule that fires on other alerts ("alerts on
  alerts").
- Epic 4 (compliance custody) wants an immutable audit log of query and
  admin activity, independent of any tenant-writable signal.

Both are append-only streams of discrete, timestamped, structured events
- closer in shape to a log record than to a metric sample. `Signal` and
the object key layout (docs/catalog-and-mvcc.md) are a frozen contract
(CLAUDE.md), so adding to them goes through this ADR and the
format-change procedure, decided once for both epics rather than twice.

## Decision

1. Add two `Signal` variants: `Alerts` (key prefix `a`) and `Audit` (key
   prefix `u`). `crates/ravel-types/src/lib.rs`'s `key_prefix` match and
   docs/catalog-and-mvcc.md's key-layout table both gain these two rows
   in the same change that lands them (`m` metrics, `l` logs, `s` spans
   reserved, `p` profiles reserved, `a` alerts, `u` audit).
2. **No new segment format.** Both signals reuse RLOG v1
   (`crates/ravel-logseg`, docs/log-segment-format.md) verbatim - same
   writer, same reader, same section layout, same crc32c discipline. An
   alert or audit record is structurally a log record: `ts` (event
   time), `body` (human-readable summary), `attrs` (the existing
   `Map<Utf8, Utf8>`) carrying the record-specific structured fields:
   - Alerts: `alert_id` (stable hash of rule_id + label set, computed
     once per distinct alerting condition), `rule_id`, `state`
     (`firing`/`resolved`/`suppressed`), `generation` (see point 4),
     `labels_json` (or individual `label:<k>` attr entries, decided at
     implementation time by whichever reads better against the existing
     `attrs` merge convention in `docs/log-segment-format.md`).
   - Audit: `actor` (tenant/API-key identity), `action` (`query`,
     `admin.retention_change`, ...), `resource`, `result` (`ok`/`denied`/
     `error`), plus whatever per-action detail fields Epic 4 needs.
   `severity_num`/`severity_text` are reused as-is (e.g. alert state or
   audit result maps onto them) rather than adding new segment fields;
   the RLOG format does not change and needs no version bump.
3. **State is derived by fold, never by mutation.** An alert's current
   state is not a field that gets updated in place - each transition
   (fires, resolves, re-fires, is suppressed) is a new immutable record
   sharing the same `alert_id`. Current state = the record with the
   greatest `ts` among all records sharing an `alert_id`, the same
   fold-to-derive-current-state pattern the catalog already uses for
   commit records. This is not a new mechanism, it is the existing one
   applied to a new signal.
4. **Recursion guard for alerts-on-alerts.** A rule evaluation that
   consumes N alert records and produces a new alert record computes
   `generation = max(generation of every consumed alert record, default
   0) + 1`. Rule evaluation rejects (typed error, never a silent drop or
   an infinite loop) producing a record whose computed `generation`
   exceeds a configured `max_alert_generation` (default 8). This is a
   hard circuit breaker on self-triggering chains, decided here so Epic
   3 does not discover the need for it mid-implementation.
5. Audit records are written by `ravel-server` itself at defined
   interception points (a query executed, an admin action taken), never
   by tenant-submitted rules - Epic 4's wiring; this ADR only reserves
   the signal, prefix, and format.

## Dual-reader question

Not applicable. Both signals are new; no data exists in any prior format
for either, so there is no old-version reader path to keep.

## Checksum coverage

Unchanged from RLOG: per-section and per-block crc32c, since no segment
byte layout changes. `attrs`-convention correctness (e.g. that
`alert_id` is computed consistently) is an application-level invariant
Epic 3/4 test, not a format-level checksum concern.

## Rejected alternatives

- **A new segment format for alerts/audit.** Rejected: an alert or audit
  record needs nothing RLOG's columnar log shape doesn't already have
  (timestamp, free text, structured key-value attrs, bloom-prunable
  content). Inventing a second byte layout would duplicate
  `ravel-logseg`'s writer/reader, `ravel-maintain`'s `RlogCodec`
  compaction path (ADR-0032), and `ravel-sql`'s logs-table provider
  pattern for zero expressive gain.
- **One shared signal for both, e.g. reusing `Signal::Logs` with a
  `record_type` attr distinguishing alerts/audit/ordinary logs.**
  Rejected: retention and future WORM policy (Epic 4) must be
  configurable independently per signal - the existing per-tenant
  `RetentionConfig` already keys off `Signal`, so folding alerts and
  audit into `Logs` would force their retention to silently inherit
  whatever the tenant's log retention happens to be, which is exactly
  the kind of silent coupling the invariants list forbids.
- **A mutable "current state" record per alert, updated in place**
  (closer to how some Alertmanager implementations model a silence).
  Rejected outright: "Data objects, commit records, manifests, and index
  objects are immutable" has no carve-out for alerts, and mutation would
  break the exact query-repeatability guarantee every other signal
  relies on.

## Consequences

- `ravel-logseg` becomes genuinely signal-generic (three signals now
  ride its format) rather than log-specific in practice, though it keeps
  its name and RLOG acronym for continuity; docs/log-segment-format.md
  gets a short note reflecting this, not a rewrite.
- `ravel-catalog` and `ravel-maintain` need no new mechanism: `SegmentCodec`
  already dispatches on `bucket.signal` (ADR-0032); Epic 3/4 add
  `Alerts`/`Audit` as new match arms.
- `ravel-sql` gets two new tables (`alerts`, `audit`) following the
  existing `logs` table's provider/pushdown pattern - Epic 3/4's work,
  not this ADR's.
- `ravel-cli` may grow presentation-only `alert inspect`/`audit inspect`
  subcommands decoding the same RLOG object, no format change.
- Fuzz/property coverage for the RLOG decoder itself is unchanged and
  already exercises what both new signals rely on; Epic 3/4 add
  attrs-convention tests (fold-by-`alert_id` correctness, generation
  circuit breaker), not new format fuzzing.
