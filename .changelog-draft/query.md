### Added

- **A process-wide memory budget now bounds SQL execution and the fetch layer
  together, with three new `/metrics` gauges** (issues #1170, #1254).
  `QueryEngine` and `SqlExecutor` share one `Arc<MemoryBudget>`: every RSEG
  `ensure_ranges` coalesced read, every RLOG block-range and whole-object
  fetch, and every RSPAN whole-object fetch reserves the bytes its GET will
  materialize before issuing it, for the reservation's whole lifetime, and a
  SQL statement's own pooled reservation draws on the same counter. A
  fetch-side refusal fails typed as `FetchMemoryExhausted { requested,
  reserved, limit }`, mapped to the frozen gRPC `BudgetExceeded` code rather
  than `Unavailable`: `Unavailable` is this codebase's re-dispatch-and-run-
  locally class, so mapping a budget refusal to it re-dispatched the refused
  slice to another worker and then ran it on the coordinator, amplifying the
  load the budget exists to shed. `ravel_memory_budget_bytes` (the resolved
  ceiling, `u64::MAX` meaning unlimited), `ravel_memory_reserved_bytes` by
  `{component="sql"|"fetch"}`, and `ravel_memory_handoff_overlap_bytes` are
  now on `/metrics`. Two startup defects in the derived budget were closed
  alongside this: a host where memory could not be measured (any non-Linux
  host) used to derive a budget of `0` instead of unlimited, and a
  `--cache-max-bytes`/`--catalog-cache-max-bytes` combination landing at or
  above the derived budget used to be accepted rather than refused; both
  used to leave `MemoryBudget::new(0)` in place, which refuses every real
  SQL or fetch reservation while a statement that reserves nothing (`SELECT
  1`) kept answering.

- **`stats.io` on both the SQL and PromQL JSON responses now reports
  `unfoldedRecordsServedFromCache`, the count of commit records a query's
  resolve served from the resolve cache instead of fetching** (issues #1199,
  #1219). The two engines share one `QueryIoShape`, so the field is read
  from `QueryAccountingSnapshot::commit_record_cache_hits` at the resolve
  site rather than inferred from a pooled counter, and a query that runs
  both a metrics and a log lane sums the field across both lanes' resolves,
  each of which runs its own resolve serially. This is a record count, not a
  segment count: the resolve's listing window is padded by
  `max_ingest_lag_ns` and always runs to the current hour, so a resolve can
  prewarm commit-record buckets a query's own time range never touches; the
  figure is meant as the numerator of a cold-resolve fraction, not a segment
  tally.

- **`/api/v1/sql`'s JSON response now carries `stats.phases` and `stats.io`,
  the same per-phase (resolve/plan/probe/scan) I/O accounting the PromQL
  endpoints already report** (issue #1367). The SQL executor's internal
  accounting seam is retyped from one pooled `QueryAccounting` handle to a
  `PhaseAccounting` split, and a new `sql_io_shape` helper derives dependency
  depth, list-page depth, service batches and plan classification the same
  way the PromQL engine's `io_shape_for_resolve` does. `RavelTableProvider`
  and `LogsTableProvider` (the metrics and logs tables) carry the phase
  split through to their scan operators; spans, alerts and audit stay on the
  pre-existing pooled accounting for now. Flight SQL constructs no
  `SqlOutcome` and has no stats envelope to extend, and the Arrow-IPC
  encoding of `/api/v1/sql` carries no stats at all, matching its existing
  behavior for the accounting and estimate fields it already omits: this is
  the one shipping surface that gains the new fields.

- **A SQL page planner turns a `SELECT` into a resumable, keyset-paginated
  statement, as internal plumbing for the paging tools landing on top of it**
  (issues #1374, #1571). `page_plan` is a pure text-to-text rewrite: given a
  statement and an optional resume position, it returns the statement to run
  next, the effective `ORDER BY`, and whether that ordering is a total order.
  The samples table gets a total order for free (each scan emits one winner
  per `(series_id, ts)`, appended as a deterministic tiebreak); the RLOG- and
  RSPAN-backed tables have no row identity under at-least-once ingest, so no
  tiebreak is appended and the plan reports why instead of claiming an
  ordering the scan can't back up. Every unsafe shape is a typed refusal
  rather than a silently wrong page: a statement carrying its own `LIMIT`,
  `OFFSET`, `FETCH`, `TOP`, a pipe operator, `ORDER BY ALL`, an order term
  that isn't a column reference or isn't projected, an explicit `NULLS
  FIRST`/`NULLS LAST` or `WITH FILL`, an order term the statement text can't
  prove `NOT NULL` on the queried table (a keyset comparison against `NULL`
  selects no rows, so those rows would silently never appear on any page), a
  resume tuple whose arity doesn't match the ordering terms, and a
  non-finite float in a resume value. `SqlOutcome` also now carries the
  three resolve inputs a cursor pins (ADR-1374 decision 5): the target
  signal, the declared column set the query resolved, and the erasure
  predicates pending in the snapshot it read, each read off the successful
  attempt's own snapshot so a retry can't substitute another attempt's
  values. As of this release nothing in `ravel-server` or `ravel-mcp` calls
  `page_plan` yet; it is tested directly and awaits its caller in a later
  wave.

- **A native MCP (Model Context Protocol) adapter is available behind the
  off-by-default `mcp` build feature and a `--mcp` runtime flag, serving nine
  tools over `POST /mcp`** (issue #1379, ADR-1374 decision 9). The new
  `ravel-mcp` crate ships `ravel_capabilities`, `ravel_describe_data`,
  `ravel_find_labels`, `ravel_explain_query`, `ravel_query_sql`,
  `ravel_query_promql`, `ravel_search_logs`, `ravel_get_trace` and
  `ravel_analyze_timeseries`; the `mcp` feature implies `sql`, since four of
  the nine tools execute SQL through the query service. Every tool response
  is bounded before it reaches the wire: cursors are opaque, MAC'd,
  self-describing tokens bound to the call that minted them, envelope cells
  are sized by their serialized length rather than by field count, and a
  compact text rendering is capped at 20 rows and 64 KiB with every caller
  string escaped and control characters stripped. `ravel-sql` gained a
  `pin-codec` feature so the cursor codec can reuse `FlightTicket`,
  `TicketKey` and `SegmentPin`'s keyed-MAC pattern without linking Arrow
  Flight.

- **A hex-string `trace_id` literal now plans, alongside the existing
  `X'...'` byte-literal form** (issue #1709). The traces guide documents
  looking up a trace by its 32-character hex string, but comparing the
  `FixedSizeBinary(16)` `trace_id` column against a `Utf8` literal failed
  type coercion, so the documented query returned a planning error and only
  the byte-literal spelling worked. A new expression planner rewrites a
  `trace_id` comparison against a 32-character hex literal (case-insensitive,
  either operand order) into the binary form for both `=` and `!=`; a
  literal of the wrong length or containing a non-hex character is left
  alone and still fails to plan, rather than silently matching nothing.

- **The spans table gains a structured `events` column,
  `List<Struct{ts_unix_nano, name, attrs}>`, decoded from the RSPAN v4 event
  columns instead of requiring callers to decode `_events_raw` protobuf by
  hand** (issue #1710). Event attributes use the same label-map type as the
  rest of the schema. Projections that exclude `events` still take the
  columnar fast path, and pushdown ignores the column. `_events_raw` stays
  for compatibility, and span links remain hex-attribute-only until a
  follow-up lands. The JSON output encoder gained `List` and `Struct` cases
  to render it, since the column is reachable from both the HTTP and Flight
  surfaces.

- **A SQL query over the samples table now warns in the response when it
  silently excluded native-histogram data** (issue #1738). The samples
  table's value column is a non-nullable `Float64` with no way to carry a
  native histogram, so a histogram sample never became a row: a `COUNT(*)`
  on a tenant that ingests native histograms was short by the whole
  histogram population, answered with HTTP 200 and no indication, and on a
  histogram-only tenant the answer was `0`. The JSON success body now
  carries a top-level `warnings` array of strings (omitted when empty, the
  same convention the PromQL endpoints already use), populated from
  `SqlOutcome::warnings` and sourced from a count the scalar fetch already
  produces for free while filtering histogram-kind series out of its
  results. Two surfaces still can't carry it: an Arrow-IPC response has no
  envelope to put a warning in, and a statement executed through the
  distributed scan lane counts nothing, because the worker that dropped the
  series streams rows rather than its own counters back to the coordinator.
  The samples table's column count is unchanged; this makes an existing
  exclusion visible, it does not narrow it further.

- **The log fetcher's assembly-buffer pool now reports the live (in-flight)
  buffer set on top of the pool's existing idle-retention figures** (issue
  #1771). `AssemblyBufferStats` described only what the pool retains between
  reads; nothing described what a running scan currently holds, which is the
  figure a memory question actually needs (under the byte-minimal fetch
  policy, a query holds one object-sized buffer per in-flight ranged read).
  New `live_bytes` and `peak_live_bytes` fields are charged when a buffer is
  acquired and released when it is returned, before the pool's retention
  bounds decide whether to keep it or drop it, so a buffer that gets dropped
  rather than pooled still leaves the live set. Charging is by the buffer's
  resident length rather than its requested length: a reused buffer keeps
  the length of the largest object it has ever served, and those bytes are
  held whether or not the current read addresses all of them.

### Changed

- **`LIMIT` now pushes into the logs scan instead of stopping at a
  `LocalLimitExec` DataFusion inserts above it** (issue #362). A previous
  attempt at this added an internal row-count stop inside the scan and
  measured no effect, because DataFusion's `LimitPushdown` already inserts a
  per-partition limit node above any scan that doesn't implement `fetch()`,
  and that node already stopped polling the scan. `LogsScanExec` now
  implements `fetch()`/`with_fetch()`, so `LimitPushdown` pushes the limit
  into the scan and removes the extra plan node per partition instead of
  leaving the scan's own bookkeeping unused. The per-partition fetch is a
  bound each partition may stop at on its own; the query's real limit across
  all partitions is still enforced above it, so no partition returns fewer
  rows than its share.

- **Reading one attribute through `attrs['k']` no longer costs 50x-220x the
  CPU of reading the same value through a declared column of the same name**
  (issues #913, #1768). The literal keys referenced through `attrs[...]` in
  the projection and in residual predicates are now collected up front, and
  when nothing in the plan needs the whole attributes map, only those keys'
  columns (plus `attrs_raw`) are resolved and built directly as `Utf8`,
  instead of selecting every dynamic column's pages, rebuilding a
  `Vec<(String, AttrValue)>` map per row, and materializing a full
  `Map(Utf8, Utf8)` column that `get_field` then read one key out of. On the
  measurement quoted in the commit (40 objects, 200,000 rows, 33 record
  attributes), an equality statement went from 1070.7 ms to 4.8 ms and from
  84,691 to 3,531 stored page bytes decoded. The rewrite only narrows which
  columns resolve, never what a query returns: a bare `attrs` reference,
  `SELECT *`, an aggregate argument, a grouping set, a projected filter, or
  any plan node the rewrite doesn't recognise all keep the whole-map row
  path, and a per-key column still renders the map form's value, including
  the ADR-0090 decision 7 case where a non-`Str` value under a `Str`
  declaration renders as text through the map but `NULL` through the
  declared column. This is a CPU-only change: the shipped fetch policy reads
  whole objects regardless of projection, so no wire byte or GET count
  changes.

- **The catalog now accepts only v3 per-part `.cstat` column-statistics
  objects; the v1 and v2 whole-object decode paths are retired** (issue
  #1600, ADR-1413 decision 6). `column_stats_resolve.rs` and `catalog.rs`
  drop the v2-then-v1 fallback ladder and the declared-entry-count coverage
  comparison entirely: a covered part whose per-part reference is absent,
  not found, or undecodable is now scanned directly rather than falling back
  to a whole-object read, with the existing warn-once and
  `column_stats_decode_refusals` counter still firing on that path.
  `SnapshotHead`'s whole-object v1/v2 reference fields become reserved in
  `proto/ravel/catalog.proto`, matching the fold no longer publishing either
  form. The v2 encoder is retained but gated to test code, since it is still
  useful for exercising header/envelope-mismatch and decode-refusal paths
  without a second production code path per version.

### Fixed

- **A `SELECT labels` result that a stock 4 MiB Arrow Flight client could
  not read past about a dozen distinct series now fits well past that**
  (issue #1519). `RsegDedupExec` keeps each deduplicated winner row as a
  one-row slice of its source batch; slicing a `DictionaryArray` rewrites
  only the key run and retains the whole source dictionary values buffer, so
  concatenating ~1024 such one-row slices per flush appended every slice's
  full dictionary, and the labels dictionary grew with the row count rather
  than the distinct-series count. The flushed batch's labels column is now
  rebuilt so its dictionary holds only the distinct label sets its surviving
  rows actually reference, with rows re-keyed to the compacted entries: one
  entry per distinct series in the batch regardless of how many rows
  reference it. A test over the real scan-to-dedup pipeline pins the
  flushed dictionary's length to the distinct label-set count rather than
  the row count, and asserts the largest single Arrow IPC body (25 series
  over 60,000 rows) fits inside the 4 MiB Flight default.

- **A transient labels-dictionary blowup during dedup flush, and repeated
  per-row rebuild work, are both closed** (issue #1582), following up on
  #1519's per-flush dictionary compaction. `concat_batches` still
  transiently materialized the full blown-up dictionary before that
  per-flush compaction ran, measured at 350,192,304 bytes peak on a
  many-series corpus; each one-row slice is now compacted in `finalize()`
  before it is pushed to the pending output, so `concat_batches` only ever
  sees at-most-one-entry dictionaries and the measured peak drops to
  853,232 bytes (410x). `DedupStream::finalize` also memoizes its per-row
  labels-dictionary rebuild by the source dictionary's values pointer plus
  key, so consecutive winner rows from the same upstream batch and the same
  dictionary key reuse the already-built array instead of rebuilding a
  bit-identical one; the memo compares the values array by pointer as well
  as by key; because a new upstream batch renumbers its own dictionary from
  zero, a key-only memo would have relabeled one series' rows with
  another's.

- **A per-tenant bytes-scanned or S3-request budget check on the three
  shipping SQL execution paths (`plan_pinned`, `plan_pinned_distributed`,
  `worker_fragment_stream`) no longer quadruple-counts the same bytes and
  refuses queries after a quarter of their real budget** (issue #1665).
  `RsegScanExec`'s budget checks read a reduction that sums four
  `PhaseAccounting` phase snapshots, which is only correct when the four
  phases are independent handles; those three paths instead build the
  handle with `pooled_over`, whose four phases are clones of one shared
  counter, so the same reduction read that one counter four times. A shared
  flag set once at construction now lets a new `pooled_snapshot()` method
  pick the correct reduction (the resolve phase's own snapshot when
  aliased, the existing summed reduction otherwise), so every caller reading
  an aliased handle's total is correct by construction rather than by
  remembering which constructor built it.

- **Three ways to bypass the SQL complexity guard that aborts the process on
  an over-bound statement are closed** (issue #1678), hardening the guard
  that issue #1760 (below) later made impossible to skip entirely by
  construction. A `/*! ... */` MySQL-style hint comment was scanned as an
  ordinary comment and skipped, so eleven characters of wrapping hid an
  arbitrarily long operator chain: `SELECT 1/*! +1 x2000 */` scored 7 while
  the tokenizer actually produced 4,003 tokens from it. The scan now gives
  the hint region its own mode that counts every non-whitespace character
  inside it and enters no sub-mode of its own (an earlier fix that made it
  fall through to ordinary counting mode reopened the same bypass through a
  line comment inside the hint body). Separately, the switch from counting
  characters to counting tokens undercounted a digit-then-word sequence like
  `1AND` as one alphanumeric run costing one unit for two tokens, which
  halved the guard's effective bound on a boolean chain: `SELECT 1 + AND 1`
  repeated 998 times scored exactly 1,000 units and built a 998-level parse
  tree, next to the roughly 1,050-1,080 levels at which the planner aborts
  on a 2 MiB thread. A digit run now stops at the first non-digit; a run
  starting with a letter or `_` still consumes alphanumerics, since `a1` is
  one identifier. Finally, the audit redaction path (`redact`, used when
  `--audit-text` is left at its default of `redacted`) parsed and walked
  caller text with no complexity guard at all, so a 64 KiB statement that
  `validate` had already rejected as too complex still reached `redact` and
  aborted the process there; `redact` now runs the same guard `validate`
  does before it parses.

- **`DistributedScanExec` no longer fails an entire statement for the whole
  `3 * H` staleness window because one assigned worker is dead but still
  registered** (issue #1684). Each scan slice now runs the same three-step
  sequence the PromQL lane's routing fetcher already uses: the assigned
  worker, exactly one re-dispatch to a different location, then a
  coordinator-local read of the same slice ticket (`worker_fragment` over
  the ticket's pinned segments against the same object store, which is
  byte-identical to the remote result it replaces) before finally returning
  a typed `SqlError::Execution` naming the last cause. Each attempt is
  probed for its first batch before the partition emits anything, so a
  fallback can never feed the coordinator's merge a second run of the same
  rows, and `SliceFallbackCounters` counts a re-dispatch and a
  coordinator-local read as separate figures. The coordinator's own record
  is also dropped from the SQL worker roster, since it always serves its
  own slices through the local path and dispatching one to itself over
  Flight was a wasted hop.

- **A federated coordinator that can resolve more than one local tenant can
  no longer leak one tenant's remote series to another** (issue #1295).
  Federation held one remote credential per process with no way to say which
  local tenant it belonged to, so on a coordinator serving more than one
  local tenant (two or more `--tenant-token` values, or any dynamic resolver
  such as `--dev-insecure-tenant-header`, `--oidc-issuer`, or
  `--mtls-enabled`), every local tenant's metric selectors and discovery
  calls fanned out to the remotes under that single shared credential: each
  local tenant received the remote tenant's series, and the remote tenant's
  data reached whichever local tenant happened to ask. `--remote-cluster`
  now takes a tenant key naming the one local tenant whose queries may use
  that remote's credential, and `Federation::fetch` selects remotes by the
  caller's tenant before dispatch, so an unmapped local tenant presents no
  credential and is answered from local data alone; an unkeyed
  `--remote-cluster` spec is refused at startup on any coordinator that can
  resolve more than one local tenant, naming the offending clusters and the
  remedy. A follow-up closed the same leak on the alerting path: the
  multi-tenant check originally counted only distinct `--tenant-token`
  values, missing that `--alert-rules-file` starts one evaluator per tenant
  against the same shared engine with no incoming request to carry a tenant
  key, so a single-token deployment with a second tenant's alert rules read
  as single-tenant and let that tenant's rules evaluate against the remote
  tenant's series. Both checks now read the union of the token-derived
  tenants and the alert-rules file's tenant keys.

- **Every parse of caller text in `ravel-sql` now runs the pre-parse
  complexity guard, because one function does both** (issue #1760),
  matching what issue #1817 already did on the PromQL side. Three functions
  in the crate built their own parser over caller text: `validate`, the
  audit redactor (`redact`), and the page planner (`page_plan`'s
  `parse_query`). Only two of the three ran the guard by convention: the
  redactor gained its call in a review round (issue #1678, above), and the
  page planner had never had one, resting on the unenforced assumption that
  `validate` had already accepted the same text first. `parse_guarded` is
  now the crate's only parse of caller text: it runs the complexity check
  and then builds the parser, so `validate`, `redact` and `page_plan` all go
  through it and a fourth entry point cannot reach the parser without the
  guard in front of it. A new gate script,
  `scripts/guards/check-guarded-sql-parse.sh`, refuses any mention of a SQL
  parser front end under `crates/ravel-sql/src/` outside that one function.

- **Nine PromQL evaluator code paths that used to abort the whole process on
  an ordinary query shape now return a typed error instead** (issue #1701).
  A parsed tenant query could reach an `unreachable!()` arm for: an unknown
  aggregator token, an aggregate whose inner expression evaluates to a
  non-vector, a missing or wrongly-typed `limitk`/`count_values` parameter, a
  binary operator whose operands are neither both scalar nor both vector, a
  `ManyToMany` vector match on a non-set operator, and a matrix-typed
  function argument reaching a non-matrix AST node - each on the mistaken
  assumption that promql-parser's own type checking had already ruled the
  shape out. Each arm now returns `Error::Unsupported` naming the operator
  or type, with a test built from a synthetic AST the parser cannot
  currently produce, demonstrating the arm panics if reverted. A follow-up
  found one more gap in the same area: `eval_binary` dispatches "if
  `is_comparison(op)` then `apply_cmp` else `apply_arith`", so a
  `Scalar/Scalar` or `Scalar/Vector` expression using `and`, `or`, or
  `unless` reached `apply_arith`'s fallback and aborted, because nothing in
  Ravel (only promql-parser's own `check_ast`, which sits on a caret version
  range) narrowed those shapes out. `eval_binary` now checks the operator's
  class before dispatching on operand types, rejecting a set operator over
  scalar operands with a typed error before the scalar-handling functions
  run; `Vector/Vector` set operators keep their existing path unchanged. A
  new guard script, `scripts/guards/check-promql-unreachable.sh`, requires
  every remaining `unreachable!()` under `crates/ravel-promql/src` to name
  the check that narrows it out of reach.

- **A log segment scan with one overflowing `attrs_raw` block no longer
  drops the rest of its partition's block list onto the slower row-decode
  path** (issue #1769). Blocks past a block whose attributes overflow the
  per-object dynamic-column budget used to stay on the row path for the
  remainder of the scan, even blocks with no overflow at all, because the
  scan only knew how to reopen the segment once and commit to row mode from
  there. `LogSegmentScan` now falls back for the offending block only and
  resumes columnar decoding after it, since its columnar and row cursor-
  advance paths already share one primitive. The narrowing is bounded
  rather than unconditional: a tenant with more than about a hundred
  distinct declared attribute names has overflow in most blocks of an
  object, and reopening once per block would cost quadratic redecode work
  on exactly the tenants already slowest on this path, so after two
  consecutive fallbacks with no clean block in between, the scan commits the
  rest of the partition's list to the row path in one reopen, capping any
  one segment at two reopens regardless of its block count.

- **A log lane query's reported `segments_pruned` and `segments_fetched`
  figures are now derived from the actual set of segments each fetch
  touched, instead of being summed or maxed across a query's plans** (issue
  #1228). The log lane's `stats.segments_pruned` first silently
  under-reported because `prefetch` discarded the count `fetch_log_series`
  already computed per plan and substituted the catalog resolve's own
  figure, which is structurally always `0` for this lane (the resolve
  passes no name filter to prune against). Summing each plan's own pruned
  count fixed that but introduced a double-count: every plan in a log lane
  re-walks the same resolved segment list under the same padded window, so
  a segment one plan pruned could be exactly the segment another plan
  fetched, and a two-plan query where each plan pruned the other's segment
  reported `pruned=2, fetched=1` over a 2-segment snapshot that had pruned
  nothing. `fetch_log_series` now reports which segments it fetched as
  indexes into the shared segment slice; the log lane unions these indexes
  across its plans and derives `segments_fetched` as the union's size and
  `segments_pruned` as the remainder, so the two figures sum to the
  resolved segment count by construction for any plan count, saturating at
  zero to stay fail-closed if a future caller passes a subslice.

- **A wide tenant's per-segment column statistics could silently disable
  pruning for every query, and are now split into one bounded object per
  snapshot part instead of one growing-without-bound object per tenant**
  (issues #1413, #1483, ADR-1413). The prior `.cstat` object held every
  `ColumnStatsSegment` record for a whole (tenant, signal) as one compressed
  frame, decoded whole to serve any part of it, and refused to inflate
  anything over a 256 MiB safety ceiling. On a measured 104-column,
  703-segment tenant the object decoded to a body of 2,000,102,795 bytes,
  7.5x that ceiling: the decode was refused, the refusal was silently
  degraded to "no column statistics for this tenant", and every query on it
  fell back to a full scan (7,645 GETs for a single-column `COUNT(*)` where
  statistics would have pruned). The fold now emits one per-part `.cstat`
  object alongside each part, referenced from the part's own
  `SnapshotPartRef`, so decoding one part's statistics costs only that
  part's bytes; an over-ceiling part degrades (drops to an unpruned scan for
  that part only) instead of refusing the whole tenant's statistics. Issue
  #1483 closed a gap in the migration window between the old and new
  format: the reader treated any successful v2 (whole-object) fetch as
  answering every segment and stopped consulting v1, but a published v2
  object can legitimately omit a segment its fold couldn't build
  statistics for, so a part v2 omitted with no v3 object yet got no
  statistics at all and scanned silently. The reader now tracks the parts
  still needing a fallback explicitly and only clears that list once the
  entries v2 actually decoded meet or exceed the entry count the snapshot
  HEAD declares.
