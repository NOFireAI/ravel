### Added

- **`Catalog::fold_with_refold_request` re-folds already-sealed ingest hours
  that receive a late compaction or rewrite record** (issue #526). The
  fold's fixed reconcile window and its retention-frontier band each cover
  only the hours near their own edge, so a rewrite landing in an hour
  outside both bands left its snapshot part naming pre-rewrite inputs
  indefinitely: the unreferenced-object sweep's HEAD-reachability gate kept
  holding those inputs, and they occupied storage until retention dropped
  the hour. A new `RefoldRequest` names the ingest hours to re-run through
  the same per-bucket classify-and-diff pass the two existing reconcile
  passes use, landing in the same single HEAD compare-and-swap; hours
  already covered by another pass, or named by no snapshot entry, are
  dropped, and the request is capped at `frontier_reconcile_max_hours`
  (default 168), spent oldest-first. A request is a hint, never a
  durability dependency: an unrequested or dropped hour just keeps today's
  behavior. The maintain-tier wiring that turns the unreferenced-object
  sweep's blocked hours into a `RefoldRequest` is separate follow-up work;
  until it lands, this entry point has no production caller and
  `Catalog::fold` behaves exactly as before.

- **Per-signal fold-liveness metrics:
  `ravel_catalog_fold_cycles_total`, `ravel_catalog_fold_failures_total`,
  and `ravel_catalog_fold_last_success_timestamp_seconds`** (issues #1306
  and #1625). A stalled catalog fold was invisible: the scheduled fold
  loop logged its outcome and dropped it, so nothing distinguished a fold
  running every five minutes from one that had not run in a day, and the
  first symptom was a slow query weeks later as unsealed ingest hours
  piled up. The three families are accumulated inside `Catalog::fold`
  itself, the single point every fold path (the scheduled loop, the
  on-demand admin route, the CLI, and the resolve bench) goes through, so
  a metric fed by only one caller cannot happen; a no-op fold still counts
  as a cycle, since a loop that wakes and finds nothing sealed is working
  correctly. Because the server spawns one fold loop per signal with no
  supervisor, the families are keyed by `signal` rather than global, so a
  dead loop for one signal cannot hide behind two healthy ones. The
  last-success gauge takes a plain store rather than a max, since a
  forward clock step under a max would latch permanently and mask a later
  genuine stall; a plain store only risks a bounded, self-clearing false
  stall from a backwards step. `docs/guides/observability.md` ships
  `RavelCatalogFoldStalled`, derived from the seal window
  (`max_flush_lifetime` 3600s + `clock_skew_allowance` 300s +
  `fold_safety_margin` 900s = 4800s) rather than a round number, plus an
  `absent()` branch so a scrape target list that drops every folding
  process entirely still fires.

- **Per-part (v3) column-statistics objects, written alongside the
  existing whole-snapshot v1 and v2 statistics** (issue #1482, ADR-1413).
  The fold now writes one `.cstat` object per newly-written snapshot part,
  referenced by an additive field 7 (`column_stats`) on `SnapshotPartRef`.
  A part whose statistics would exceed `DEFAULT_MAX_COLUMN_STATS_BYTES`
  (256 MiB, the same ceiling the v3 reader already enforces) degrades
  rather than stalls the fold: the largest remaining dictionary is dropped
  and the part re-measured, repeating until it fits, clearing only
  `dictionary_present`/`dictionary` and keeping min/max/count/sum exact;
  the fold fails a part only once no dictionary is left to drop and it is
  still over the ceiling. `FoldReport::column_stats_dictionaries_dropped`
  reports how many dictionaries a fold cleared this way. Objects are keyed
  by the content hash of their own bytes rather than the part's hash, so
  two folds that recompute a byte-identical part but hit different
  segment-fetch outcomes cannot collide on a key naming bytes that were
  never stored there; the v3 object is built and bound-checked before the
  part's own `.csnap` is written, so a refused part leaves no orphaned
  `.csnap` behind. The degrade loop tracks each dropped dictionary's exact
  byte contribution instead of re-measuring the whole part per drop, so a
  part needing tens of thousands of drops still finishes. An incremental
  fold also skips the v3 baseline fetch for any old part it is not
  genuinely re-deriving, cutting one object GET per untouched sealed part
  on every incremental fold. `ravel-maintain`'s unreferenced-object sweep
  now carries every part's field-7 key into its referenced-key set;
  without that, a v3 object outlived only by the sealed part naming it
  would have crossed the protection horizon and been swept from under it.

- **`ravel-cli catalog fold --json`, and full column-statistics visibility
  in `catalog fold` and `catalog inspect`** (issue #1598). `catalog
  fold`'s human report used to print only 10 of `FoldReport`'s
  then-23 fields, silently dropping counters such as
  `column_stats_dictionaries_dropped`; it now renders every field, and
  `--json` emits the whole struct as a JSON document (the store
  selection, `signal`, and `seal_margin` ride along as extra top-level
  keys rather than being lost). `catalog inspect` prints each
  column-statistics reference (HEAD fields 11 and 13, and the new
  per-part field 7) as `key=... size=...`, or an explicit `ABSENT`
  marker, so an omitted line and an unset field are no longer
  indistinguishable. A new `ravel-cli inspect cstat <key>` decodes a
  `.cstat` object's envelope and header without decompressing its body,
  so an object whose declared uncompressed length exceeds the decode
  ceiling still yields every header field and an over-ceiling verdict
  instead of an error; under the ceiling it goes on to list each
  column's `dictionary_present`. `FoldReport::put_requests` previously
  undercounted: three of the six fold PUT call sites only incremented on
  success or `AlreadyExists`, so a store-side error on an
  otherwise-issued PUT went uncounted even though the object may have
  been durably written as an orphan; all six sites now increment
  unconditionally once the request resolves.

- **`ravel_declared_stats_drops_observed_total`,
  `ravel_catalog_fold_stamped_records_total`, and
  `ravel_catalog_fold_stamped_entries_total` for ADR-0873 declared-column
  statistics coverage** (issue #1747). The per-carrier drop tally existed
  only as an unrendered crate-internal counter, and the fold reported
  nothing about how many commit records it read with statistics stamps or
  how many snapshot entries it wrote carrying them, so a deployment
  transitioning to statistics stamping had no figure to confirm the
  rollout was actually reaching the snapshot. The drop family (labeled by
  `carrier`: `commit-record`, `compaction-part`, `snapshot-entry`,
  `cstat`) renders in every mode, including `maintain`; the coverage pair
  renders only in a mode that can fold at all, by either the background
  loop or the on-demand admin route, and covers snapshot entries built
  from both commit-record and compaction-part carriage.
  `docs/guides/observability.md` ships two alerts:
  `RavelFoldStampCoverageShortfall`,
  firing when the fold reads more stamped records than it writes stamped
  entries over an hour, and `RavelFoldStampCoverageMissing`, firing on
  `absent()` of the coverage family while log flushes are still
  happening, since a fold that predates these counters cannot emit them.

- **`FoldReport::refold_hours_reconciled`, printed by `ravel-cli`'s fold
  report** (issue #1763). The targeted re-fold pass added for issue #526
  counted the hours it reconciled only in a test-only thread-local, with
  no way for an operator to read the number. The count is now a
  `FoldReport` field, always zero on a no-op fold regardless of what a
  `RefoldRequest` named, since a no-op fold returns before reaching the
  targeted pass.

### Fixed

- **f64 range predicates in `ravel-logseg` no longer prune a block that
  carries a NaN value** (issue #1699). Under the `total_cmp` order the
  skip index's min/max stats and the SQL layer's numeric-range predicate
  both use for `f64`, a `+NaN` row sorts above every finite value and a
  `-NaN` row sorts below every finite value, so a half-open range arm
  like `[x, inf)` or `(-inf, x]` can be satisfied by a NaN row that a
  block's finite `[min, max]` stat says nothing about. `stat_disjoint`
  ignored the block's `has_nan` flag entirely and could prune such a
  block, dropping a matching row from a range-scanned query. It now
  declines to prune whenever an f64 stat has `has_nan` set, before
  applying the bounds test; this is deliberately coarser than necessary
  (it declines both arm directions, since the flag does not record which
  sign of NaN was present) but always sound.

- **The read cache's single-flight `get_or_fetch` path no longer blocks a
  runtime worker thread on disk I/O** (issue #1702). The tiered disk
  cache read and wrote files with `std::fs`, called directly from the
  async single-flight path, so every disk hit and every disk fill on that
  path occupied a tokio worker for the length of the file operation,
  starving unrelated queries and ingest work sharing the runtime. The
  calls now run under `spawn_blocking`; a join error is treated as a
  cache miss on read and a dropped fill on write, never a panic, and the
  synchronous entry points keep their signatures.

- **Disk-cache entries and directories, and SQL spill scratch directories,
  are now created owner-only instead of at the ambient umask** (issue
  #1708). Neither `ravel-cache`'s disk read cache nor `ravel-sql`'s spill
  scratch set a file mode, so on a node running at the common umask
  default of 0022, cache entries (raw, unencrypted segment bytes under a
  filename carrying the tenant hash) and their shard/namespace
  directories were world-readable and world-listable, and per-query spill
  directories were world-listable. Cache inserts now create shard
  directories at mode 0700 and entry files at mode 0600; SQL spill
  scratch directories are created at mode 0700. A node upgraded in place
  also had a namespace root and shard directories an older build already
  created at the ambient umask: on startup, the cache now narrows the
  namespaced root and each shard directory it walks to owner-only and
  each live entry it seeds to owner read-write, without touching anything
  above the namespace or an operator-created root. This does not reach a
  pre-namespace cache tree left over from before the per-instance
  namespace layout; `docs/guides/caching.md` points at the existing
  `reclaim-legacy --apply` command to remove one.
