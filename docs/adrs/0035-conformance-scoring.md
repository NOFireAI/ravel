# ADR-0035: Conformance scoring for the PromQL and SQL query surfaces

Status: Accepted

Builds on ADR-0007/ADR-0021 (the PromQL differential approach and
full-evaluator scope), ADR-0013/ADR-0033 (the DataFusion SQL surface), and
the accepted-divergence records ADR-0025 and ADR-0030.

## Context

Ravel exposes two query languages, and both already have serious
correctness machinery, but neither has a published statement of what is
supported, what is deliberately rejected, and what is simply untested.

**PromQL.** `crates/ravel-promql-difftest` runs a differential harness
against a pinned real Prometheus binary (v3.13.1), per ADR-0007 and
ADR-0021. The corpus is 10 files, 212 entries (selectors 32, transform
59, binop 26, aggregate 20, over_time 18, rate 17, subquery 11,
histogram_classic 11, errors 10, histogram_native 8), in `corpus.rs`'s
blank-line-separated key:value format with `mode: unordered|error|
ordered|ravel_error_prom_success` and an optional per-entry `tolerance:
N` (ULP). Scoring is `RunReport { total: usize, failures: Vec<Failure> }`
in `runner.rs`: pass/fail counts only, no per-feature breakdown, no
coverage percentage, nothing persisted. Two classes of divergence are
already investigated and closed: ADR-0025's five ULP-tolerance entries
(arithmetic residue in `rate`/`deriv`/`predict_linear`, 1-904 ULP, plus
`atanh` at 169 ULP) and ADR-0030's two `ravel_error_prom_success`
entries (Ravel's per-subquery-node 11,000-point cap has no Prometheus
counterpart). A prior plan already specs a
"not-supported" table for `docs/query-engine.md` enumerating every
promql-parser AST node and stable Prometheus function as
implemented-and-covered or typed-rejected. This ADR's PromQL half is
that work; it does not add a second unrelated table beside it.

**SQL.** `crates/ravel-sql` uses DataFusion directly (`datafusion = {
version = "54", features = ["sql"] }`, no ExprPlanner, so ADR-0033's
known `attrs['k']` planning gap stands). The surface is a deliberately
narrow allowlist, not full DataFusion SQL: `validate.rs`
(`EXCLUDED_AGGREGATES`) restricts aggregates to `count, sum, min, max,
avg, mean`, rejects writes, and dispatches queries to the `samples` or
`logs` table via `referenced_base_tables` (ADR-0033). No sqllogictest
or DataFusion test corpus is vendored anywhere in the repo. The existing
coverage mechanism is the bespoke two-layer differential gate: layer 1's
scan oracle (`tests/pipeline.rs`) against an independent merge, and
layer 2's proptest-driven operator gate (`tests/differential.rs`,
`tests/util/gate.rs`) checking project/filter/count/sum/group-by/
order-by/limit/min/max against a hand-written reference executor. That
is a test fixture, not a published, scored table. A ravel-sql audit
already called for a DataFusion-differential audit under ADR-0013; this
ADR's SQL half fulfills that part of it, not an unlinked duplicate.

The gap this ADR closes: misses currently live only in ADR prose. There
is no artifact a reader can consult to answer "does Ravel support X" for
either language, and no scored baseline against which a new miss is
visible rather than a surprise.

## Decision

**Score conformance with a three-state classification per construct,
over the full surface of the underlying language.** "Conformance
against DataFusion SQL" has two degenerate framings, and both are
rejected (Rejected Alternatives, A). The denominator question is the
crux, so it is decided explicitly:

Each construct (every DataFusion SQL construct on one side; every
promql-parser AST node and stable Prometheus function on the other)
gets exactly one state:

1. **Supported and covered**: implemented, with a passing test proving
   it.
2. **Intentionally rejected**: refused with a typed error. Never a
   panic, never silently wrong data. This is the same invariant the
   repo already holds everywhere else (no unwrap/expect in production
   paths; corrupt-input tests produce typed errors, never panics or
   wrong data), applied to the query surface.
3. **Unclassified or broken**: implemented but untested, or
   claimed-supported but actually wrong.

The published table enumerates the full underlying surface with one
state per row. The score is coverage over Ravel's claimed surface:
states 1 and 2 combined, everything Ravel has taken a deliberate,
verified position on, divided by the full surface. State 3 is the
actionable miss category; states 1 and 2 are conformant by definition
once verified. This makes the score meaningful in both directions: it
cannot be inflated by defining misses out of the surface (state 3 rows
stay visible in the table), and it is not deflated by out-of-scope SQL
Ravel never claimed (a state-2 row with a verified typed rejection is
conformant, not a miss).

**Already-accepted divergences are not state 3.** ADR-0025's five ULP
entries and ADR-0030's two one-sided-divergence entries get their own
state, "accepted divergence, see ADR-00XX", cross-linking the existing
decision rather than re-litigating it as a new miss. The table reuses
the ADR vocabulary; it does not reopen closed investigations.

**Where the tables live.** The PromQL table extends `docs/query-engine.md`.
The SQL table gets a new doc,
`docs/sql-conformance.md`, cross-referenced from
`docs/adrs/0033-sql-query-over-logs.md` and README's doc index, because
ravel-sql has no equivalent existing doc section to extend.

**The score is computed, not hand-maintained.** The tables' state
column and the score are generated from test-run output (a report the
test binary emits, or a small script over it), so the published numbers
cannot silently drift from what actually passes. Hand-written prose is
limited to the per-row rationale for state-2 entries.

**Triage of current misses is a one-time step.** Once the suites land,
current state-3 rows are triaged into tracked issues manually, not by the
test suites themselves (Rejected Alternatives, D).

This ADR changes no frozen format: no RSEG, proto, series-identity,
commit-token, or object-key changes, so no version bump. It adds tests,
a report artifact, and documentation.

## Rejected alternatives

**A. Score against all of DataFusion SQL, or against Ravel's own
declared surface only.** Both degenerate framings rejected. Scoring
against all of standard/DataFusion SQL is meaningless for Ravel: the
declared surface is a deliberate six-aggregate, no-write allowlist, so
the score would sit at a tiny, permanently fixed percentage that says
nothing about quality and never moves with real work. Scoring against
Ravel's own declared surface only is equally meaningless in the other
direction: it reads ~100% by construction, because anything not
implemented is defined out of the surface. The three-state scheme in
the Decision is the resolution: enumerate the full surface, score over
the claimed part, keep the unclaimed part visible.

**B. A shared `conformance-core` crate or scoring type used by both
suites.** Rejected: the PromQL and SQL scoring work is otherwise fully
parallel, and a shared abstraction would serialize it into one crate for
a shared scoring struct with no real value: the two languages' construct
taxonomies (promql-parser AST nodes vs. SQL constructs) do not overlap, so
there is nothing to share beyond three enum variants each side can write
locally.

**C. Vendor or adopt DataFusion's own sqllogictest suite wholesale.**
Rejected: it exercises full ANSI SQL and DataFusion's complete function
library, almost entirely outside Ravel's six-aggregate, no-write
allowlisted surface, so nearly all of it would score as out-of-scope
noise rather than signal. It also does not test the `samples`/`logs`
table-dispatch behavior (`referenced_base_tables`, cross-signal
rejection) that is specific to ravel-sql. The existing two-layer
differential gate already covers the allowlisted operators against an
independent reference; the missing piece is classification and
publication, not a third-party corpus.

**D. Continuous CI-automated ticket filing for every scoring run.**
Rejected for now, flagged as a follow-up rather than rejected forever.
Automatically filing issues on every run has side effects visible to
others and produces duplicates on any retry. Current misses are triaged
into tracked issues once, manually, after the suites land. A follow-up
could add a diff-against-baseline CI check that flags (not auto-files) new
misses.

## Consequences

- The real value of this work is proving state-2 rows fail cleanly. A
  construct marked "intentionally unsupported" that actually panics, or
  returns wrong data instead of a typed error, is a bug the table must
  catch, not a false negative to ignore. The state-2 verification tests
  are the enforcement of the typed-rejection invariant on the query
  surface, not paperwork.
- ADR-0025's and ADR-0030's accepted divergences appear in the PromQL
  table as "accepted divergence" rows citing those ADRs. They are
  excluded from the state-3 miss bucket permanently; a future reader
  finds the closed investigation via the cross-link instead of retriaging.
- `RunReport` (or a sibling type) grows a per-construct breakdown so the
  PromQL table can be generated from a run; the SQL suite emits an
  equivalent report. Because the tables are generated, a regression
  (state 1 to state 3) shows up as a table diff in the same change that
  caused it, and cannot be edited away in prose.
- New state-3 rows discovered after the initial triage have no
  automation yet: whoever runs the suites files the tickets. The
  diff-against-baseline CI flag (Rejected Alternatives, D) is the named
  follow-up if that proves error-prone.
- No frozen-format changes, no version bumps. The epic's footprint is
  test code in the two suites, report generation, `docs/query-engine.md`,
  and the new `docs/sql-conformance.md`.
