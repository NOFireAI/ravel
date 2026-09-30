# Changelog

All notable changes to Ravel are documented in this file. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Ravel aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **`ravel-server` sends a CRC64-NVME upload checksum on every S3 PUT by
  default** (issue #1696). The endpoint verifies each PUT against
  `x-amz-checksum-crc64nvme` and stores it, so a full-object read, commit
  records included, is verified against it. The new
  `--s3-upload-integrity {off,crc64nvme,sha256}` flag
  (`RAVEL_S3_UPLOAD_INTEGRITY`) selects the algorithm; an endpoint that
  rejects the header fails every PUT from the first flush on, which can
  come after the process reports ready, and `--s3-upload-integrity off` is
  the remedy.
  Stores routed by `--tenant-kms-config` apply neither flag yet (issue
  #2224). The new
  `--s3-request-stored-checksum` switch (`RAVEL_S3_REQUEST_STORED_CHECKSUM`,
  default `true`) controls the `x-amz-checksum-mode` request header;
  `--s3-request-stored-checksum=false` counts every full-object read as
  unverified. The operator exposes both as `spec.storage.s3.uploadIntegrity`
  and `spec.storage.s3.requestStoredChecksum` and applies them to its own S3
  client too.
- **The RLOG writer now chooses each i64 and string page's encoding by its stored size, and writes encoding tags 10 (GCD i64) and 11 (an `observed_ts` equal to `ts`, stored as a reference to it)** (ADR-2135 decisions 3 and 4, issue #2140); no i64 or string page stores more bytes than the encoding chosen before (a very small object can still grow by a few bytes, because a PAGE_DIR whose `ts` and `observed_ts` entries used to be identical compresses worse), each candidate encoding of a page is compressed at most once (up to six candidates for an i64 page, two for a string page), and the reader decodes both tags.
- **`s3_e2e_bench` counts adaptive-age flushes** (issue #2186). Its printed
  flush breakdown gains an `age_adaptive=` field between `age=` and
  `age_floor=`, matching `ingest_bench`, its JSON report gains
  `flushes_by_age_adaptive`, and `estimated_put_count` includes those
  flushes. The counter is zero while the run leaves `adaptive_flush_delay`
  off.
- **RLOG log objects are now written and read at trailer version 5**
  (ADR-2135, issue #2139). The footer gains a sort descriptor and a clustering
  generation, the BLOOM section starts with the list of columns its filters
  cover under its own crc32c, bloom filters are sized to a multiple of 512
  bits instead of a power of two, and encoding tags 10 to 13 are registered
  but never written. A covered list that fails its crc is refused as
  corrupt and the scan falls back to no bloom pruning. The writer still
  produces version 4's content: no sort descriptor, generation 0, and bloom
  coverage of body, severity text and every string attribute. Each filter
  is now no larger than version 4 made it for the same keys, and it runs at
  the false-positive rate its sizing rule targets rather than the lower rate
  power-of-two rounding could give, so a text predicate may scan more blocks
  than before. The reader accepts only
  version 5: a version-4 object is refused with `UnsupportedVersion` before
  any section is read, and becomes unreadable once a build with this change
  is deployed. Under the pre-v1.0 format posture there is no migration path;
  development stores holding version-4 log objects must be wiped or
  re-ingested.
- **SQL fetches now reserve against the process-wide memory budget**
  (ADR-1170 decision 2, issue #2086). The server's SQL path wires the same
  `Arc<MemoryBudget>` the SQL executor already used into its RSEG metrics,
  RLOG logs, and RSPAN span fetchers, matching the PromQL path. A SQL fetch
  that would exceed the shared budget is now refused typed
  (`FetchMemoryExhausted`) and, over HTTP, as 503 `unavailable`, instead of
  reserving against an unlimited private budget and never refusing. The
  client sees the redacted "upstream storage temporarily unavailable"; the
  server's warn log names the memory budget. The
  process budget's size and carve are
  unchanged, but SQL fetch buffers now count against it: before this change
  `ravel_memory_reserved_bytes{component="fetch"}` read 0 for every SQL
  query, so a deployment could not see how close its SQL load came to the
  limit. It now includes SQL fetches, and SQL load that fitted only because
  its fetch buffers went uncounted can now be refused with 503.

- **`ravel-cli maintain migrate` names every bucket whose refusal no re-run
  clears** (ADR-1331, issue #1331). A bucket that selective erasure has
  touched keeps a live rewrite record whose surviving parts carry the
  erasure-time format version. The re-audit counted those parts into the
  compaction-part figure, which was right (they are live objects the floor
  would otherwise be raised over) but left the report saying "stragglers"
  forever with `buckets_blocked: 0`, and nothing migrates them: a migration
  output over inputs a rewrite already covers would resurrect the records that
  rewrite dropped. The refused raise now reports three counts
  (`l0_commit_records`, `l1_compaction_parts`, `rewrite_record_parts`) and
  prints one `blocked_bucket` line per affected bucket with its shard, hour
  and reason, `rewrite_parts below_target=<n>` or `loser_only_inputs`,
  followed by how each clears. A `rewrite_parts` block clears when retention
  ages the bucket out under the format-version hold, or when a later erasure
  request supersedes the record at the current output version AND a subsequent
  `sweep` removes the superseded predecessor: `below_target` counts the parts
  of every rewrite record the bucket still LISTS, and a superseded record stays
  listed until the sweep deletes it, so the superseding rewrite alone does not
  clear the block. The `sweep` is a command the operator runs (`ravel-cli
  maintain sweep` for that tenant, signal and shard, subject to the protection
  horizon), and when a bucket is blocked only by a superseded predecessor whose
  successor is already at the current output version, that `sweep` alone clears
  it; the refusal message and the maintenance guide say so. Of the three counts,
  only `l0_commit_records` is scoped as entirely live and unmovable by a `sweep`:
  `l1_compaction_parts` is a count over listed records too, and a compaction
  record that a later rewrite superseded stays listed until a `sweep` deletes it
  and its parts, so a `sweep` lowers that figure as well even though no `migrate`
  run can. A `loser_only_inputs` block clears only when retention ages
  those inputs out, since compaction refuses a bucket that already carries a
  compaction record and so never publishes the covering record that would
  otherwise clear it. `buckets_blocked` is the number of those lines, covers
  both permanent cases, and covers the buckets the invocation examined (a walk
  resumed from a cursor does not re-report loser-only buckets an earlier
  invocation found). A below-target part of an authoritative compaction record
  blocks the floor too and is reported as an `l1_compaction_parts` count with no
  `blocked_bucket` line (a losing record's parts get one when the bucket's
  authoritative records are at the target and any rewrite record parts it
  lists are too; see the next entry):
  nothing migrates one either, because compaction and the migration rewrite
  both refuse a bucket that already carries a compaction record, so ADR-0066
  decision 4 force 2 is unimplemented (issue #2093). The explanatory prose
  between `buckets_blocked` and `records_migrated` is prefixed with `# ` so it
  is not read as a `key: value` line. `count_below_target` returns a
  `BelowTargetReport` instead of an `(l0, l1)` pair,
  `Verification::Stragglers` carries the three counts and the list, and
  `FamilyMigrateReport::buckets_blocked` is a method over `blocked_buckets`
  rather than a separate counter.
- **`ravel-cli maintain migrate` names a bucket held below the target only by
  a losing compaction record's parts** (ADR-0066, force 2 amendment item 9,
  issue #2093). When every authoritative compaction record of a bucket has its
  parts at the target and records that lost their overlap still carry parts
  below it, the report prints
  `blocked_bucket: ... reason=losing_record_parts below_target=<n>`, where
  `<n>` sums the losing records' below-target parts, with a comment line saying
  re-running `migrate` does not clear it and retention ageing the bucket out,
  under the format-version hold, does. Those parts still count in
  `l1_compaction_parts`, so the floor is still refused over them; only the
  naming is new. A bucket listing a rewrite record whose parts are all at the
  target is named this way too when its losers qualify (issue #2169). A
  bucket whose authoritative records are below the target is not named this
  way, a bucket whose rewrite record parts are below the target gets its
  `rewrite_parts` line only, and a record a version 2 record supersedes is
  not a loser. A record a rewrite record supersedes is left out of both sides
  of the test, as a winner or as a loser. A bucket the walk names
  `loser_only_inputs` that also qualifies here gets this line only, since the
  two clear the same way. `BlockedReason` gains
  `LosingRecordParts { below_target }`.
- **The background supervisor now takes an advisory claim before compacting a
  large bucket, so two processes whose ownership overlaps no longer both pay
  for the same merge** (ADR-1029 decisions 3 to 5, issue #1033). The claim is
  taken after the bucket's gates and before any read whose cost scales with the
  bucket; a claimed run then consults it at the merge's quiescent points and
  cancels without publishing once the claim is gone, leaving the parts it had
  already written where they are. A supervisor refused a claim reports the
  bucket skipped, with the holder and the reason, and holds its compaction
  until that holder's lease can have expired rather than re-requesting the
  claim every tick; retention and zone classification still run for a held
  bucket. An uncontended claim is taken with no wait: the deterministic
  jitter, by default up to 10% of the lease, is waited out only before
  stealing an expired claim or retrying a create whose claim vanished before
  it could be read. A supervisor tick's claim decisions read the tick's own clock.
  Coordination is on by default and claims are taken only at or above 64 MiB
  of listed input bytes (`claim_min_input_bytes`), so a small bucket is merged
  exactly as before. The switch that turns claiming off is the
  `CompactorConfig::coordination` field; no server flag or config file reaches
  it yet, and its operator flag lands with #1035. Claims
  stay advisory: the compaction record's `CreateIfAbsent` still decides which
  output is published, so a stale owner that finishes after losing its claim
  converges on the one record rather than publishing a second. For the same
  reason a store error while marking a claim completed is logged and the
  published compaction stands, and a claim object that cannot be decoded is
  never stolen but, once older than one lease plus the contender's jitter, no
  longer holds its bucket back: the bucket is compacted unclaimed. Claim
  traffic is counted under a new `coordinate` phase in the compaction request
  ledger, never pooled into the merge's own phases.
- **`ravel-cli maintain compact-bucket` and `compact-tenant` take the same
  advisory compaction claims, so an operator's run and a background supervisor
  no longer both merge one large bucket** (ADR-1029 decision 5, issue #1034).
  Every bucket at or above the 64 MiB claim threshold asks for a claim before
  its merge, one independent claim per bucket at any `--bucket-concurrency`,
  all under one fresh process id per invocation that the new `claims:` report
  line prints. The claim renews on the wall clock rather than on the fixed
  instant the walk judges sealing at, so a long merge keeps its claim. A
  bucket refused its claim prints `outcome=ClaimSkipped` with the reason, the
  claim's work id, the holder and the claim expiry, a bucket that loses its claim mid-merge stops
  without publishing and prints `outcome=ClaimCancelled`, and the
  `compact-tenant` summary counts both (`claim_skipped`, `claim_cancelled`)
  apart from `compacted`. Neither is a failure: the walk carries on and exits
  zero unless another bucket failed.
  `--dry-run` takes no claims. The new `--no-claim` flag on both commands takes
  none either, for repair work: correctness is unchanged, since the compaction
  record's create-if-absent still decides the published output, but the merge
  may duplicate one another maintainer is running. The claim clock is injected:
  `ravel_cli::maintain::ClaimOptions` carries an optional `clock`, which the
  binary leaves unset to get the live wall clock and a test sets to drive
  renewal and expiry deterministically. The skip line's
  `retry_after_unix_ms` field, not its `claim_expiry_unix_ms`, is the point to
  rerun from; the maintenance guide says what each of the four skip reasons
  means and why the two differ for three of them.

- **A ranged RLOG read holds only the bytes it placed, not a buffer the size
  of the object** (issue #2066). The reader now reads through a byte source,
  and a ranged read hands it just the fetched regions rather than copying
  them into an object-sized buffer; its fetch reservation (ADR-1170 decision
  2) covers those regions instead of the object size, and the object-sized
  assembly buffer pool is gone, with `AssemblyBufferStats` keeping only its
  live and peak gauge. As measured in issue #2066's heap profile, on a tenant
  of roughly 16 MB log objects, 2,948 MiB of the 4,011 MiB live at q29's peak
  had sat in those object-sized buffers, across about 180 ranged reads in
  flight over 32 partitions, which pushed the server into swap under ten
  concurrent queries.
- **This release reads provisioning record format 3 and still writes 2, and
  `ravel-cli maintain audit-versions` now classifies every recorded format
  floor** (ADR-1746 Release A, issue #1746). `FormatFloor` gains three basis
  fields (`observed_entries`, `observed_newest_created_unix_ns`,
  `observed_shards`, numbers 5 to 7) that a later release fills when
  `migrate` raises a floor. Readers accept provisioning format versions
  {1, 2, 3}; writers keep stamping 2, and `append_generation` and
  `raise_format_floor` refuse a version-3 record with
  `RefusingToRewriteNewerRecord` instead of rewriting it without the basis.
  Roll this release out fleet-wide before any release that writes format 3.
  `audit-versions` prints each floor with its basis and one of `current`,
  `stale`, `contradicted` or `unknown`, and exits nonzero when a live record
  sits below a recorded floor (`contradicted`). Every floor raised so far has
  no basis and reports `unknown` unless it is contradicted.
- **The query fetchers and PromQL evaluation can run their CPU-bound work on
  the read CPU gate** (ADR-1702 follow-up task 7, issue #1702).
  `SegmentFetcher`, `LogSegmentFetcher`, `SpanSegmentFetcher` and
  `QueryEngine` gain `with_read_gate`. With a gate set, each RSEG catalog
  decode (site `segment_section`, or `segment_sparse_catalog` for the chunked
  catalog probe) is one gate job the size of its decoded catalog, with the
  decode's memory reservation moved into the job and shrunk after the matcher
  filter as before. On `LogSegmentFetcher`, every RLOG scan open (its
  directory sections and the POSTINGS probe, which the reader runs inside the
  open) is one `log_postings` job; every block `fetch_accounted` and
  `fetch_accounted_with_tenant` decode, and every block a `LogSegmentScan`
  hands out through `next_block_on_gate` (the LogQL series path), is one
  `log_block` job covering all its pages; and every SKIP_IDX, PAGE_DIR,
  FIELD_DIR and planning section the block-range path decodes on its own is
  one `log_section` job. Each RSPAN block the span fetcher's row fetch
  decodes, and every block a `SpanColumnarScan` hands out through
  `next_block_on_gate`, is one `span_block` job. A PromQL evaluation whose
  prefetched sample count is at or above the gate's evaluation floor runs on
  the gate (`promql_eval`); a smaller one runs inline and counts as inline. A job that panicked is the fetcher's decode
  error (`Corrupt`), a 500 on the HTTP API, and an evaluation that panicked
  is the new `QueryError::CpuGate`, also a 500; a job the runtime dropped at
  shutdown is a transient store error and a 503. A log or span scan whose
  gated block decode failed refuses every later block rather than skip one,
  and every later call reports the same error class the failed call reported,
  so a panicked decode stays a 500 instead of degrading to a 503. Sizing an
  object's `log_block` jobs reads its PAGE_DIR a second time, after the scan
  open's own decode of it; that read is charged to the query's accounting
  like every other, so a gated fetch reports the decompressed bytes it
  actually produced. With no gate set every path runs inline exactly as
  before, and the server does not
  set one yet: wiring its read gate into the engine and fetchers it builds is
  a later step. Still inline with a gate set: RSEG page decodes, a
  `LogSegmentScan`'s `next_block` and `next_block_columnar` exits and a
  `SpanColumnarScan`'s `next_block`, a direct `matching_streams` call, the
  STREAM_DIR decode of `fetch_stream_dir`'s whole-object fallback, and the
  RSPAN footer sections (`span_section`), which the span fetcher decodes
  while opening a scan. The SQL logs and spans
  scans still decode inside `poll_next` (task 8).
- **The catalog can decode snapshot parts, postings and column statistics on
  the read CPU gate** (ADR-1702 follow-up task 6, issue #1702).
  `Catalog::with_read_gate` sends each decode to the gate at its declared
  uncompressed length, with the decode's memory reservation moved into the
  job. Units below the gate's inline floor still run inline and count as
  inline. The new `read_metrics_meta_on_gate` and
  `read_metrics_meta_for_serve_on_gate` run the metrics-meta body decode on
  the gate the same way; only the serve reader moves a memory reservation
  into the job, since the strict reader takes none. A job the gate cannot complete fails that decode with
  `SnapshotFormatError::DecodeJob` or `MetricsMetaError::DecodeJob`, and the
  read handles it like any other decode error. Without a gate every decode
  runs inline as before. The server does not install the gate on its catalog
  or the metadata cache yet, so no server read path runs on it in this
  release.
- **A ranged log read's chunk-run GETs against one L0 RLOG object are now
  bounded at `MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT` (4)** (ADR-2066 decision 1).
  A projection whose coalesced candidate runs exceed the cap bridges the
  smallest remaining gaps between them down to it, the same bound already
  applied to L0 metrics flushes (ADR-1306); the whole-object coverage
  crossover is computed against that bridged run set, so bridging can itself
  push a projection over the 75% threshold and convert it to one whole-object
  GET. A compacted L1 log part is exempt, as an L1 metrics part is: it can be
  far larger than a flush, so bridging would move most of the object, and its
  crossover is computed against its unbridged runs. When neither front section
  (STREAM_DIR/FIELD_DIR) is resident, the two are fetched in one combined GET,
  including on the narrow-projection path that resolves column ids before
  choosing candidates; a front section a plan-phase read already cached is
  served from cache instead of re-fetched into that GET, and a combined GET
  admits each section under its own cache key, so while those entries stay
  resident the next read of the object serves both from cache. No default or
  config surface changes.

### Security

- **A distributed SQL slice fetch no longer carries the client's credential**
  (ADR-1689 decision 2, issues #1689 and #1690). The coordinator used to copy
  the inbound request's gRPC metadata, bearer token included, onto every
  worker `DoGet`, so the long-lived client token reached every worker that
  served a slice. The slice ticket is now the whole credential: it is MAC'd
  under a slice key of its own, derived from each SQL ticket file key beside
  a separate key for client whole-set tickets, so a ticket minted for one
  surface fails the MAC on the other. The worker verifies it without
  consulting the tenant resolver (MAC under any configured key, deadline
  against the injected clock, `slice_count > 1`, listener role) and runs the
  slice under the ticket's tenant. Refusals are typed and counted in-process
  under a closed reason (`missing`, `bad_mac`, `expired`, `wrong_surface`);
  the counters are not exported at `/metrics` yet. On the combined listener
  a node without `--fragment-listener` runs, a ticket that fails the slice
  MAC falls through to the client path, which still requires the client
  credential, so only `expired` and `wrong_surface` can fire there. Without
  `--fragment-listener` the slice travels over the public gRPC listener in
  plaintext; with it, the slice rides the dedicated TLS listener (the entry
  after next). Both keys derive from the first fragment key unless
  `--sql-ticket-key-file` is set (next entry). During the rolling upgrade
  onto this release a coordinator drops workers on the other `queryfrag`
  protocol version at routing time, with no round trip, and runs their
  slices on the coordinator: parallelism drops for the rollout, results do
  not change. Client whole-set tickets are now signed under a key derived
  from the shared one, so with `--distributed-query` a `GetFlightInfo` and
  its `DoGet` that land on an old and a new process fail with
  `invalid_argument` until the rollout completes, and the query has to be
  run again.

- **The Flight SQL ticket keys come from `--sql-ticket-key-file`, not from the
  fragment key** (ADR-1689 decision 2, issues #1689 and #1690). The new flag
  takes the `--fragment-key-file` shape and rotation rule (the first key mints,
  every key verifies) and requires `--distributed-query`. With it set, the
  Flight SQL service keys every client and slice ticket off the file's keys and
  nothing is derived from the fragment key file, so one key file no longer
  covers both lanes. Without it, a node derives the SQL ticket secret from the
  first fragment key as earlier releases did; the upgrade from an earlier
  release still has the client-ticket window the previous entry describes,
  because this release turns that secret into separate client and slice keys. A
  `--distributed-query` process in `all` or `query` mode missing
  `--sql-ticket-key-file` or `--fragment-listener` logs a startup warning that
  release B (ADR-1689 decision 4) requires both. Every `--distributed-query`
  process in `all` or `query` mode without `--fragment-listener` also logs that
  SQL slice tickets travel in plaintext on the public gRPC listener and are a
  replayable read capability until their deadline (next entry). Every node in a
  cluster must read the same SQL ticket key set. A rolling switch straight onto
  a new key file has a mixed window: a client ticket that `GetFlightInfo` minted
  on a node on the file and that `DoGet` redeems on a node still on the derived
  key (or the reverse) fails with `invalid_argument`, a client-visible query
  failure, since a client ticket can be redeemed on any node behind a balancer,
  and SQL slices between two such nodes run on the coordinator instead. The
  deployment guide describes a switch without that window: first ship a key file
  holding the key each node derives today, then rotate.

- **SQL slice fetches ride the dedicated TLS fragment listener, and the
  public gRPC listener refuses slice tickets once one is configured**
  (ADR-1689 decisions 1 and 3, issues #1689 and #1690). With
  `--fragment-listener`, the dedicated listener mounts the Flight service
  beside `SeriesFetch` in a slice-only role: it serves `DoGet` for a slice
  ticket and refuses every other Flight and Flight SQL method, and no method
  other than a slice `DoGet` returns data. The client Flight SQL methods the
  service implements answer `permission_denied`; the methods it does not
  (prepared statements, `Handshake`, `ListFlights`, `PollFlightInfo`,
  `GetSchema`, `DoPut`, `DoExchange`) answer `unimplemented`, as they do on
  the public listener; `ListActions` returns its static list; a `DoGet` that is
  not a valid slice capability answers `unauthenticated` (`missing`,
  `bad_mac` or `expired`, where a handle too short to be a ticket is
  `missing`) or, for a client ticket, `permission_denied` (`wrong_surface`),
  counted on the same counters as the public listener. The public gRPC
  listener keeps the client Flight SQL surface and refuses a slice ticket
  whose MAC verifies under this node's slice keys with `permission_denied`,
  counted as `wrong_surface`, the mirror of it refusing pinned fragment
  fetches; a forged ticket, or one under a key this node lacks, takes the
  client path and is refused there uncounted. The SQL lane dials each
  worker's `fragment_endpoint` over the same pinned-CA mutual TLS the PromQL
  lane uses, with the same certificate, so a peer without a client
  certificate from `--fragment-tls-ca` fails the handshake; it no longer
  dials `flight_sql_endpoint` for slices, and a worker whose record has no
  fragment endpoint gets none. Without
  `--fragment-listener` nothing changes: the public listener serves both
  surfaces and slices travel there in plaintext, and the startup line saying
  so is now logged only in that case (and only where Flight SQL is served).
  In a build without Flight SQL the release B warning names only
  `--fragment-listener`. The `queryfrag` protocol version moves from 4 to 5,
  so during the one rolling deploy onto this release a coordinator drops
  workers on the other version at routing time and runs their PromQL and SQL
  slices coordinator-local: parallelism drops for the deploy, results do not
  change. Federation requests carry the same protocol version and a remote
  refuses a mismatch, so a cluster on this release and a remote cluster on
  an earlier one fail federated queries with a `Federation` error (or, for a
  remote with `skip-unavailable`, skip it with a partial-coverage warning)
  until both run the same release.

### Fixed

- **One refused delete no longer stops the superseded-input sweep for every
  chain** (issue #1846). Rule 2 deletes every cleared chain's input commit
  records, then their data objects, then the chains' own records, and a
  delete the store refused (for example a deny policy on part of the
  keyspace, answered 403 access denied) failed the whole pass before any
  data object was deleted. A refusal (access denied, a failed precondition,
  or a permanent error) now stops only the chain it belongs to: that chain
  deletes none of its later keys that pass, its requests are reported held,
  and the other chains are collected. The pass counts each refusal in
  `SupersededSweepOutcome::deletes_refused` and
  `SweepReport::superseded_deletes_refused` and logs it, and the server
  sums the count per signal in the new
  `ravel_maintain_superseded_deletes_refused_total` counter on `/metrics`,
  since the unit's tick no longer records a refusal as failed. A pass in which
  every delete it attempted was refused still fails, with the first
  refusal's error, so a credential without delete permission fails the pass
  as it did before. A retryable error, a read-only
  store, or a backend with no delete support still fails the pass.
- **SQL answers corrupt stored data and a panicked decode as an internal error,
  not a retryable unavailable** (issue #2097). `SqlError::class()` put every
  metrics, logs and spans fetcher error, `Corrupt` included, in the
  `Unavailable` class, so HTTP SQL answered 503 and Flight SQL `UNAVAILABLE`
  for a fault that fails the same way on every retry. A new
  `ErrorClass::Internal` now holds every error the redaction reports as
  "stored data failed integrity validation": a fetcher `Corrupt` error (which
  is also how a panicked read-gate decode job is reported), a store-side
  checksum mismatch, a carry or tenant mismatch, a corrupt `stream_attrs`
  blob, and a corrupt catalog record or column-statistics HEAD. HTTP SQL
  answers it with 500 `internal` and Flight SQL with `INTERNAL`, the PromQL
  surface's rule for the same fault. Other store errors, timeouts,
  cancellation and admission refusals keep their classes.
- **A catalog decode declared over its ceiling now evicts decoded-cache entries
  until the budget admits it or the caches are empty** (issue #2132). Such a
  decode is charged 0 bytes, and a budget pushed over its limit by
  `reserve_unchecked` refuses even that. The eviction pass
  `Catalog::reserve_decoded` runs before its one retry used to stop only once
  the free space covered the charge, which 0 bytes always did, so it evicted
  nothing and the refusal stood. It now evicts until the budget admits the
  charge by the same test `try_reserve` applies. When the over-limit bytes are
  held by cached entries, releasing them lets the retry succeed and the
  decoder's own refusal decides the outcome; when they are held elsewhere, the
  pass empties the caches and the retry is refused. The charge rule itself,
  `decoded_charge`, now lives once in `ravel-memory`, and both the query
  fetcher and the catalog call it.
- **`ravel-cli load --signal logs`, `--signal metrics` and `--signal spans`
  name the unit a negative timestamp was read in** (issues #2133, #2168). For a
  native Arrow `Timestamp` ts column the loader scales by the column's own
  unit, but the refusal named the declared `ts_unit` instead; a
  `Timestamp(Second)` cell of -5 under `ts_unit = "nanos"` reported "read as
  ts_unit = nanos". It now reports "read in the column's own Timestamp unit,
  seconds". An integer column still names `ts_unit`. The spans refusal, which
  named both declared units whatever the columns were, now names the start and
  the end each by the same rule, against `start_ts_unit` and `end_ts_unit`,
  and an end substituted from the start for a zero end cell as "taken from
  start_ts because end_ts is 0" rather than in the end column's unit, and a
  start substituted from the load time for a zero start cell as "taken from
  load time because start_ts is 0" rather than in the start column's unit.
- **The maintain role reaps dead query-worker records under
  `sys/query/workers/`** (issue #1828). The query coordinator used to delete
  them under a role with no delete grant, so every delete was refused, logged
  as one warning per key per tick, and the prefix grew without bound. The
  maintain process that owns a fixed rendezvous unit now lists the prefix on
  its cycle and deletes the keys past the reap horizon; a deployment with no
  `--mode maintain` process, including a single `--mode all` process, reaps
  nothing. `deploy/iam/maintain.json` gains list and delete on
  `sys/query/workers/*`, and `deploy/iam/query.json` is unchanged. Upgrade
  note: a deployment still on the previous `maintain.json` logs a query-worker
  listing warning on every maintain cycle until the template is applied. A
  draining coordinator overwrites its own record with a stamp no reader
  accepts as live instead of deleting it. A reap delete refused as access
  denied is logged once per pass at error and ends that pass.
- **`ravel-cli load --signal logs` loads a dictionary-encoded hex `trace_id` or
  `span_id` column** (issue #2116). A default Parquet writer dictionary-encodes
  string columns, and the columnar logs path refused such an id column with
  "expected a binary or string id column, found Dictionary(Int32, Utf8)". It
  now resolves the two id columns once per batch and stores the same ids as a
  plain column; a null cell stores no id in either form.
- **`ravel-cli load --signal logs` and `--signal metrics` refuse a negative
  timestamp** (issue #2118), as the spans load already did. A row whose `ts`
  falls before the Unix epoch is a row rejection on the logs and metrics load
  paths, naming the unit the value was read in (see the #2133 entry above),
  where before only the future-skew bound was checked. A negative result always means a negative
  cell: unit conversion never flips a sign.
- **The spans load's reserved attribute keys come from ravel-otlp** (issue
  #2123). `ravel_otlp::traces_normalize` now exports `RESERVED_ATTR_KEYS` and
  `is_reserved_key`, and the loader's `[spans]` mapping check uses them instead
  of a private copy, so a key reserved there is refused here too. The unused
  lever warning names every signal explicitly, so a new signal is a compile
  error rather than a metrics message.
- **The fold checks a previous postings object's tenant before reusing it**
  (ADR-0050 section 2, ADR-1702, issue #2081). The fold's previous-postings
  reuse path verified the object's blake3 and its binding to the HEAD's part
  hashes, but never compared the declared `tenant_hash` with the tenant being
  folded, which the query path has always done. A postings object naming
  another tenant now fails the fold with the same typed field-mismatch error
  the resolve path raises and is counted as an isolation breach, instead of
  being reused or degrading into a quiet rebuild. The check reads the already
  hash-verified header before the decode reservation, so neither the
  part-binding degrade nor a memory refusal can mask it.
- **A declared length over the decoder's ceiling is charged 0, not the
  ceiling** (ADR-1702, the decode-refusal amendment, issue #2081). A decoder
  refuses an oversized unit before it allocates anything, so reserving the
  ceiling for one charged memory nobody would ask for, and turned that refusal
  into a budget refusal on any budget with less than the ceiling free: a
  snapshot part that should fall back to listing failed the query with a
  memory error instead, and a PromQL catalog chunk frame lost its own typed
  error the same way (a whole section over the ceiling is refused earlier,
  when the segment is opened). Every decode reservation (snapshot parts,
  postings and column statistics, PromQL catalog sections, and per-frame
  `SERIES_META_CHUNKS`) now charges 0 for a declared length over its decoder's
  ceiling and lets the decoder's refusal decide the outcome. Only the PromQL
  fetcher half is visible in the shipped server, whose catalog budget is
  unlimited.
- **A refused decode reservation fails the fold instead of rebuilding its
  postings from scratch** (ADR-1702, the decode-refusal amendment, issue
  #2081). The fold answered a refused previous-postings reservation by
  rebuilding the index from every segment's names, which fetches and decodes
  far more than the one postings object the budget had just refused, so the
  reaction to memory pressure took the more expensive path. It now fails with
  the typed budget error, matching what the resolve path already did with the
  same refusal; the next fold retries. The shipped server cannot reach this,
  because its catalog budget is unlimited.
- **The catalog's decoded-part and postings caches give memory back to a
  refused decode** (ADR-1702 decision 6, issue #2088). Both caches hold each
  decoded value together with its memory reservation and were bounded only by
  an entry cap per tenant, so once a finite memory budget is wired into the
  catalog, other tenants' cached entries could hold the whole budget and every
  later decode would be refused with nothing able to release the memory. A
  refused reservation now evicts cached entries least recently used across
  every tenant of both caches until it would fit or the caches are empty, then
  retries the reservation a single time. An entry a live resolve still holds
  keeps its reservation until that resolve drops it, so a pass can empty a
  cache and free nothing; the pass is bounded by the entries it removes rather
  than by the bytes it frees. A decode wanting more than the budget's whole
  limit skips the pass and keeps its first refusal, since no eviction could
  admit it, so one oversized object does not flush every tenant's decoded
  entries. A column-statistics load takes the same pass (issue #2107): it
  reserves its declared body against the same budget these caches hold. The
  column-statistics CACHE is still unaffected: its entries carry no
  reservation and it is already bounded in bytes. The server does not yet pass
  a finite budget to the catalog, so no deployment sees the eviction pass in
  this release; one deployed change does land with it, on every catalog
  including one on the unlimited default budget, because the two caches' own
  per-tenant entry cap now evicts least recently used rather than oldest
  inserted: an entry survives while fewer than the cap's number of other
  entries for its tenant are inserted between its reads.
- **A writer whose clock lags the object store's clock refuses its flush
  instead of publishing into a sealed hour** (issue #1685, ADR-1685). At flush
  open, every metrics, log, and span shard actor compares its raw clock reading
  with the store's observed clock (the latest response `Date`). A reading more
  than `DEFAULT_CLOCK_SKEW_ALLOWANCE_NS` (five minutes) behind it fails the
  flush with the retryable `Abandoned` (503), re-buffers the rows, and counts
  `clock_lag_refused`, where before the flush was acknowledged and its commit
  record landed in an ingest hour a fold on a correct clock may already have
  sealed, invisible to token-less reads. A flush with no observation yet
  proceeds and counts `clock_lag_unchecked`. All three counters here
  (`clock_lag_refused`, `clock_lag_unchecked`, and
  `clock_lag_bypassed_at_shutdown` below) are on the `ravel-ingest` metrics
  snapshots, and `/metrics` renders all three (see the clock-lag entry
  under Added). A
  graceful shutdown is the one exception, because a lag refusal re-anchors
  nothing and so refuses every enforced pass of a drain: on the `Shutdown` and
  channel-close drains the drain then makes bypass passes, under the same pass
  cap, that skip the check and publish the buffered rows, counting each
  bypassed flush-open attempt as
  `clock_lag_bypassed_at_shutdown` and logging the lag at WARN. Those rows
  can land in an already-sealed ingest hour, visible to token-less reads
  after a HEAD rebuild, which is what they did before this change; enforcing
  the refusal there would have dropped rows buffered mode had already
  acknowledged. The monotonic floor (ADR-1307) still applies on a bypass
  pass, and a lag refusal returns before the floor is read, so a backwards
  step past the hold bound surfaces on the first bypass pass, re-anchors the
  floor there, and publishes on the next one. Teardown residue now needs every
  enforced pass refused (by the lag check or the floor) and every bypass pass
  refused by the floor, so it takes as many consecutive over-bound backwards
  steps on the bypass readings as the pass cap allows passes, the same count
  the floor alone needed before this check existed. One consequence for
  existing alerting: a bypass pass suspends the lag check only, so a teardown
  drain whose flushes are all refused by the ADR-1307 floor now runs the
  bypass passes too and is refused on each of them, and
  `clock_regressions_refused` counts twice what it counted before this change
  for the same clock. `FlushNow` and every size
  or age trigger keep refusing. Fix the host clock before restarting a writer
  that is refusing flushes.
- **Alert sink delivery is bounded per evaluation tick, and ADR-0117's stated
  per-tick publish bound is corrected** (issues #2063, #2064). The evaluator
  delivered every undelivered notification to every sink sequentially with no
  per-tick limit, so the delivery phase grew with the undelivered queue behind a
  slow or unresponsive sink and delayed every later tick's rule evaluation.
  Delivery is now bounded to half the evaluation interval, measured on the
  evaluator's injected clock from the tick's own reading. The deadline is
  checked before each attempt and the first attempt of a tick is unconditional,
  so the delivery phase ends at the latest at `max(tick start + half the
  interval, start of delivery)` plus the number of sinks times the sink HTTP
  timeout. Delivery starts after whatever precedes it in the tick: the history
  read, the lease acquire, and on the lease holder rule evaluation, the repeat
  pass and the alert state memo write. None of that earlier work is bounded,
  and a tick that overruns delays the next tick rather than overlapping it.
  Previously every tick tried every queued notification in no defined order;
  the pass now serves the oldest-queued notification first, an attempt some
  sink refused moves to the back of the queue, and one the deadline never
  reached keeps its place, so a sink that drains only a few notifications per
  tick reaches every alert in turn. A notification still leaves the queue only
  once every configured sink has accepted it, so one blackholed sink throttles
  delivery for every sink to what fits in one tick's budget; what the rotation
  guarantees is that a healthy sink receives all of them eventually. The new
  `ravel_alert_notifications_deferred_total` counter reports how many were
  deferred, once per notification per tick; it also rises when the work before
  delivery alone runs past the deadline, since every notification after the
  first is then deferred however fast the sinks answer. Separately, ADR-0117
  stated the per-tick publish worst case for one rule as `MAX_ALERTS_PER_RULE`
  (1000); the true worst case is `2 x MAX_ALERTS_PER_RULE`, because one tick
  also writes a resolution for each previously-open alert that stopped
  matching, and that holds for a tick following a fully successful one rather
  than unconditionally: pre-upgrade history, a tick whose write path fails
  partway through a rule, and two overlapping lease holders each leave more
  open identities behind than the cap. A dated amendment to the ADR and the
  alerting guide carry the corrected bound.

- **A query's fold-lag refusal threshold is now sized from the fold and the
  catalog the process is actually running** (issue #1306). The threshold that
  decides whether a request-budget refusal names fold lag is the catalog's seal
  margin plus the scheduled fold's interval plus the HEAD cache TTL, and
  `EngineConfig` carried all three, but the server set none of them: every
  deployment was classified against `ravel-query`'s compiled-in reference
  durations no matter what its own fold and catalog ran on. The server now
  builds that `EngineConfig` in one place, reading the seal margin and the HEAD
  cache TTL off the `CatalogConfig` of the catalog it hands to both resolve and
  the fold, and the fold interval off the `FoldTaskConfig` it spawns the fold
  with. That fold interval is the real one only in `--mode all` and
  `--mode maintain`, the processes that run the scheduled fold; a `query` or
  `gateway` process keeps the 300 s default even when the `maintain` processes fold
  on a longer interval, so a longer maintain interval can still make a query
  node blame a fold that is keeping up. The compiled-in values stay what an `EngineConfig` built with no
  deployment context falls back to, and a test pins `ravel-query`'s hand copy
  of the fold interval against the server's own default so the two cannot drift
  apart unnoticed.

- **A Parquet load resolves each dictionary-encoded column once per batch
  instead of once per cell** (issues #1751 and #1712). The per-row readers
  resolved a dictionary cell through `DictionaryArray::normalized_keys`, which
  builds a key vector the size of the whole batch on every call, so every
  dictionary column cost time quadratic in the batch's row count. Each column
  index now resolves its mapped dictionary columns to their value type once,
  when the batch's columns are resolved, and the row readers index the result.
  This covers the spans load (whose row path is its only path, and whose name,
  id and string attribute columns arrive dictionary encoded from an ordinary
  trace export), the metrics load's name and label columns, and the logs row
  path. The columnar logs path is unchanged: its column index resolves no
  dictionary column, and its readers key their own fast path on the dictionary
  itself and read it in place. Every value, admission decision and rejection
  message is the same as before. On the row paths, a dictionary chunk whose
  dictionary is empty under a non-null key is now a typed error rather than an
  abort inside Arrow's own assertion, and an all-null chunk with an empty
  dictionary still resolves to an all-null column. The columnar path's
  empty-dictionary handling is unchanged: its per-cell readers answer an empty
  dictionary chunk as they did before.

### Added

- **The RLOG writer can sort an object by a clustering key and limit which columns BLOOM covers** (ADR-2135 decisions 1 and 5, issue #2141): `RlogWriter::with_sort_descriptor` orders rows by `(stream_ref, ts.div_euclid(bucket), key_1, ..., key_n, ts)` on resolved per-record values, identically on the row and columnar paths, and records the descriptor and clustering generation in the footer, refusing with `InvalidSortDescriptor` a descriptor the footer decoder would refuse and accepting a key no record has a value for, which orders nothing; `RlogWriter::with_bloom_scope` covers every string column (`All`, the default), `body`, `severity_text` and the string columns not in a declared-column list (`Undeclared`), or only `body` and `severity_text` (`Text`), and an uncovered column gets no bloom keys. With neither set the writer's output is byte-identical to before; no ingest or compaction caller sets either yet.

- **`--audit-retention` sets the query-audit retention window** (ADR-0062
  decision 2c, ADR-1688, issue #2126). The server built its compactor with
  the compiled-in 90-day audit window and no way to change it. The flag takes
  a humantime duration and defaults to `90d`, so a deployment that does not
  set it sweeps exactly as before. `0` keeps every query-audit record. Any
  nonzero window is accepted, since the sweep deletes a whole immutable record
  only once its newest event is older than the window; an unparseable value
  is refused at startup, before the server writes to the bucket.
- **`ravel-server` exports the unverified-read and control-plane store
  counters at `/metrics`** (issues #2172, #1727, #1696).
  `ravel_store_get_unverified_total` counts full-object reads served without
  verifying the body against a stored checksum;
  `ravel_store_control_plane_requests_total`,
  `ravel_store_control_plane_calls_total` and
  `ravel_store_control_plane_response_bytes_total` count the bucket-protection
  control plane's read-only GETs sent, answered, and their response body bytes.
  All four carry `mode` and no `op` label, and the control-plane GETs stay out
  of the per-operation `ravel_store_*` families. `ravel-server` itself sends
  no control-plane GET yet (its startup check probes through the generic
  store, issue #2197), so the three control-plane counters read 0.
- **A read-only bucket-protection control plane in `ravel-object-store`**
  (ADR-1727 follow-up task 1, issue #1727). `S3Store` can now report, per
  condition, whether the bucket's protection configuration is Pass, Fail, or
  Unknown, by signing its own read-only SigV4 `GET`s (`?versioning`,
  `?lifecycle`, `?replication`, `?object-lock`, and `?retention` on sampled
  keys) over the `reqwest` pin the crate already holds, using the same
  credential provider the store already uses. A new `BucketControlPlane` trait
  and `BucketProtectionReport` carry one entry per ADR-1727 decision 3 condition
  (`versioning`, `noncurrent-expiration`, `expired-delete-marker`,
  `abort-multipart`, `rule-scope`, `no-foreign-rule`,
  `delete-marker-replication`, `object-lock`, `object-retention`); `Unknown`
  covers no API, access denied, and an unparseable response, and is never
  `Fail`. The sanctioned lifecycle and replication conditions pass only on an
  enabled rule whose filter covers every key under `t/` (a tag- or
  object-size-narrowed rule never does, and an unrecognised filter or status, a
  repeated element where one value is expected (a rule's `Status`,
  `NoncurrentDays`, `DeleteMarkerReplication`, and the rest), or a day count
  that does not parse, is `Unknown`); `no-foreign-rule` passes only when no
  rule that can reach `t/` or `sys/` carries a transition or a current-version
  expiration, and no rule other than a sanctioned covering rule or a member of
  the complete `t/0` .. `t/f` union carries a `NoncurrentDays` shorter than
  the reference (the expected value, else the one value the covering rules
  agree on). Such a `NoncurrentDays` fails `no-foreign-rule`,
  and also `noncurrent-expiration` when the rule reaches part of `t/`; with no
  reference to compare against, the same rule is `Unknown` there instead. On
  a sanctioned covering rule or union member it fails `noncurrent-expiration`
  only, so one misconfiguration counts once. `DeleteMarkerReplication` `Disabled` on a rule
  over part of `t/` fails `delete-marker-replication`. A 404 is "not configured"
  only when its `<Error><Code>` is that call's own code
  (`NoSuchLifecycleConfiguration`, `ReplicationConfigurationNotFoundError`,
  `ObjectLockConfigurationNotFoundError`, or `NoSuchObjectLockConfiguration`
  for retention); any other 404, redirect, body over 1 MiB (with or without a
  `Content-Length`), `ObjectLockConfiguration` whose `ObjectLockEnabled` is
  missing, empty, repeated, or anything but exactly `Enabled`, or `?versions`
  page without `IsTruncated` or
  with a version lacking
  a `VersionId` is `Unknown`. `object-retention` samples the
  newest (by `LastModified`) current and noncurrent version under each
  protected prefix and requires compliance mode with a `RetainUntilDate` still
  in the future; the `?versions` listing is followed for at most 10 pages, and
  a listing still truncated at that cap yields `Unknown` whatever the samples
  show, since the newest object may be unlisted. `MemoryStore` and every
  backend reached only through the `ObjectStoreBackend` contract report every
  condition `Unknown`; `S3Store` also
  gains `ObjectLockProbeSource`/`BucketConfigProbeSource` impls derived from the
  same report, and a `BucketProbesSource` impl that derives both from one
  report. `LifecycleRuleStatus` gains `NonCompliant(reason)` for a rule that
  covers `t/` but fails its condition (an `AbortIncompleteMultipartUpload`
  longer than 7 days, a `NoncurrentDays` other than the expected one), where
  the probe used to say `present`: `bucket_config_alarms` raises the abort
  `NOTE` and, on a versioned bucket, the noncurrent-expiration `ALARM` on it,
  each naming the reason, and `store qualify` prints `non-compliant` and the
  reason on the rule's own line. `store qualify` asks both informational
  probes through one `BucketProbesSource` call, so a source backed by the
  control plane reads the bucket once per run (3 GETs where two separate
  probes cost 6). Two new direct dependencies for the crate: `ring` (SigV4
  HMAC-SHA256 and SHA-256) and `quick-xml` 0.41 (reading the S3 XML responses),
  both already in the lock and neither pulling in an AWS SDK or a RustCrypto
  crate. `versioning` is `Unknown`, naming the element, when the
  `?versioning` document carries anything besides `Status` and `MfaDelete`
  (MinIO's `ExcludedPrefixes` and `ExcludeFolders` leave keys unversioned).
  Every control-plane request is counted in its own block of the store's
  `StoreMetrics`, read with `StoreMetrics::control_plane()` as a
  `ControlPlaneMetricsSnapshot` of `requests` (sent), `calls` (answered with
  any status) and `response_bytes` (response-body wire bytes as received);
  the block is outside `StoreOp::ALL` and `StoreMetricsSnapshot`, so the
  data plane's `get` and `list` blocks are unchanged and nothing exports it
  yet. The lifecycle conditions and `delete-marker-replication` also accept a
  union of enabled rules on exactly `t/0` through `t/f`, one per lowercase hex
  digit a tenant hash can start with, each member's values checked as a
  covering rule's are; any other set of narrower prefixes stays `Unknown`. The
  signed `host` is the authority the request carries, derived from the parsed
  URL as `reqwest` derives its `Host` (host lowercased, IDNA hosts in punycode,
  IP literals normalised, the scheme's default port dropped), so an endpoint
  configured as `https://host:443`, with an uppercase or non-ASCII host, or
  with a leading-zero port is not answered 403. An endpoint carrying userinfo
  (`https://user:secret@host`) is refused as `Unknown` rather than sent with
  a second `Authorization` header. The server's `--require-bucket-protection` gate asks both probes through one
  `BucketProbesSource` call too. Neither `store qualify` nor the server gate
  is handed an `S3Store` yet: both probe through the `ObjectStoreBackend`
  contract, so against a real bucket they report every condition `Unknown`
  (issue #2197). `BucketProtectionParams` gains
  `expect_replication` (the CLI's `--expect-replication`): when it is off, as
  on the server, `?replication` is not fetched and `delete-marker-replication`
  is `Unknown` the way unsampled `object-retention` is, never `Fail`.
  Nothing in the shipping binaries reads these reports yet, though
  `S3Store::new` now builds the control-plane client on every construction:
  `ravel-cli store verify-protection` (task 2) and the server startup gate
  (task 3) are the callers.
- **`ravel-cli export --signal metrics` writes a tenant's stored metric
  samples to a Parquet file `load --signal metrics` reads back onto the same
  series** (ADR-1751 decision 4, issue #1712). It resolves the metrics catalog
  once, fetches the RSEG objects through ravel-query's segment fetcher,
  applies pending erasure predicates with the functions the query engine
  calls, and writes the `[metrics]` mapping's columns sorted by event time.
  Samples are deduplicated per series and timestamp to the one a query
  serves, through the same `ravel_query` comparison the PromQL engine and the
  SQL operator use. A `name_column` export writes each stored name, or the
  stored name less `_total`, less the unit suffix, or less both, whichever a
  load with the same mapping turns back into the stored name, and refuses by
  name a series none reproduces, a series carrying a label the mapping does
  not name, a native-histogram series, and a sample finer than `ts_unit`. A
  native-histogram series is refused first, on its own, as the fetch reads
  it. The other refusals are gathered over every series and reported once,
  on the first kind in a fixed order, counting the series that kind covers
  and naming the first in the output file's series order ("refused on N
  series for this reason; first: ..."), so the message is the same on every
  run over the same store; a `name` literal exports only the series it names.
  A `[metrics.histogram]` mapping is refused with the scalar mapping to use
  instead. The report adds
  `series_written`, `series_skipped` and `samples_deduplicated` to the logs
  export's lines. `--signal spans` writes a tenant's
  stored spans that start in the window to a file from which
  `load --signal spans` reads back every mapped field: it fetches the RSPAN
  objects through ravel-query's span fetcher, drops the spans a pending
  erasure matches with the check the SQL spans scan makes, and writes every
  `[spans]` mapping field as stored, with no deduplication, sorted by start
  time, then trace id, then span id. Span kind, trace state, flags, events,
  links, any attribute the mapping does not name, and a parent id, status code
  or status message whose optional column the mapping omits are not written,
  and the report counts the spans that lost them as
  `spans_with_unwritten_data`. Each timestamp is written in its
  declared unit, and the export refuses, in the metrics export's gathered
  form ("refused on N spans for this reason; first: ..."), a span whose start
  the load would re-time or refuse, a timestamp finer than its unit, and a
  mapped attribute whose stored string its declared type does not read back.
  N counts distinct spans, so a span whose start and end are both finer than
  their unit counts once (issue #2213).
- **`/metrics` renders the three writer clock-lag counters, and a shipped
  alert pages on a refused flush** (ADR-1685 follow-up task 3, issue #1685).
  `ravel_ingest_clock_lag_refused_total`,
  `ravel_ingest_clock_lag_unchecked_total` and
  `ravel_ingest_clock_lag_bypassed_at_shutdown_total` render in the ingest
  family for the metrics, logs and spans signals. The new
  `RavelWriterClockLagRefused` rule in `deploy/prometheus/ravel.rules.yaml`
  fires on `increase(ravel_ingest_clock_lag_refused_total[10m]) > 0` held for
  5m: a writer whose clock runs behind the object store's clock by more than
  the clock-skew allowance. The other two counters do not page; the
  observability guide says why.
- **Readers accept and validate a version 2 compaction record** (ADR-0066,
  force 2 amendment, issue #2093). `CompactionRecord` gains
  `superseded_record_key`, naming the compaction record a version 2 record
  re-encodes. Decoding accepts `format_version` 1 or 2 and checks that a
  version 2 record names a compaction record in its own tenant, signal, shard
  and hour and carries the version 2 input-set hash over its inputs and that
  key; a version 1 record that sets the field is refused. The named key must be
  the canonical rendering of the key it parses to, so a key differing only in
  hex case is refused. The seal-divergence check behind `catalog verify` and
  scrub recomputes each compaction record's hash for its own version, so a
  valid version 2 record no longer reads as corrupt. Nothing writes a
  version 2 record yet. A build
  without this change refuses a version 2 record with its unsupported
  `format_version` error. Version 1 records encode and decode byte for byte
  as before.
- **Resolution honours a version 2 compaction record** (ADR-0066, force 2
  amendment items 2 to 5, issue #2093). The shared selector
  `ravel_catalog::select_authoritative_compaction_records` now excludes every
  compaction record a present version 2 record names before it forms overlap
  components, following chains of version 2 records with the rewrite chase's
  bound of 64 records; a cycle or an over-deep chain is the typed
  `RewriteSupersessionCycle` or `RewriteSupersessionChainTooDeep` error. A
  version 2 record whose deduplicated input set differs from its present
  predecessor's is the new typed `CompactionSupersessionInputMismatch` error
  naming both keys, never an exclusion, and the dominance step below refuses
  it too, so resolve, the fold, the erasure gate and migrate fail with it
  while scrub and the sweep act on nothing superseded. It returns an
  `AuthoritativeSelection` rather than the set of losing keys. The
  rewrite chase continues through a version 2 record to the record it names.
  A new `ravel_catalog::erasure_dominated_compaction_records` drops a version
  2 record whose predecessor a live rewrite supersedes, so the rewrite wins;
  resolve, the token fallback, the fold, scrub, the erasure completion gate and
  `migrate` call it before the selector. `migrate` gains
  `largest_overlap_component`, which counts a predecessor and its version 2
  successor as one record. The superseded-input sweep reclaims nothing on
  account of version 2 supersession: an input counts as superseded only where
  an authoritative record names it both with and without version 2
  supersession, a bucket whose supersession does not resolve (the input
  mismatch included) contributes no superseded input, and a superseded
  predecessor or a dominated version 2 record is deleted only where a rule
  that predates version 2 records, such as a rewrite chain naming it, reaches
  it. The interlock alarm leaves both a superseded predecessor and a dominated
  version 2 record out of its input-set count. No
  production path writes a version 2 record yet, so on a real bucket this is
  dormant.
- **The sweep reclaims what a version 2 compaction record supersedes, and the
  erasure rewrite pass treats one as a link** (ADR-0066, force 2 amendment
  items 5 and 6, issue #2093). The superseded-input sweep, run by the maintain
  loop and `ravel-cli maintain sweep`, deletes a record a present version 2
  record supersedes, with its parts, as one chain group entered from the
  version 2 record and followed to the end of its chain, once that record's
  protection horizon has passed, only when no reachable HEAD names a part in
  the group, never under a legal hold, parts first and records after. The
  group holds no raw L0 input; those stay under the authoritative-input rule,
  whose two-view guard is kept. A version 2 record a rewrite dominates joins
  that rewrite's chain group and is deleted with its parts under the same
  rules, and one whose predecessor is already gone joins the group of the
  rewrite whose chain ends at the key it names; one no group takes is counted
  in `SupersededSweepOutcome::dominated_records_unattached`. A rewrite's chain
  now runs through a version 2 record to the record it names, so a rewrite
  over C2 over C1 reclaims C1 too instead of leaving it to be served again,
  except in a bucket whose version 2 supersession does not resolve, where
  such a chain reclaims nothing and the pass reports it as held: its
  rewrite's requests are held and its bucket is marked truncated. The chain
  walk's depth bound counts records as the catalog's walks do, and a chain
  past it, or one that revisits a record, is held the same way instead of
  failing the pass, so the shard's other buckets are still reclaimed. The
  maintain loop's observing pass walks every chain and refuses the same way,
  so `sweep_erasure_requests` keeps a signal's `.dreq`s past their horizon for
  any refused chain. It is now rule 6's only entry:
  `sweep_erasure_requests_with_holds`, which took a deleting pass's holds, is
  removed, since a deleting pass does not walk a rewrite still inside its
  horizon and could let a `.dreq` retire while a stale HEAD still resolved
  the inputs below one. A chain group the observing pass gathers twice, from
  a predecessor's own entry and from a successor's chain walk, is gated over
  the union of both gathers' parts, so a HEAD naming a part only the
  successor's walk reached holds its requests whichever entry is listed
  first. A chain ending at
  an absent compaction record is not marked truncated, since that record
  applied no erasure request. The erasure rewrite pass picks a bucket's live
  record through the catalog's shared supersession rules, so C1
  with a version 2 C2 resolves to C2, a rewrite and a version 2 record over the
  same C1 resolve to the rewrite, and a cycle fails the pass with
  `MaintainError::Invariant` carrying the catalog's error as text instead of
  `MultipleLiveRecords` or `NoLiveRecord`. No production path writes a version
  2 record yet, so this is dormant until the writer ships.
- **`validate_rewrite` refuses a non-canonical `superseded_record_key`** (issue
  #2124). A rewrite record naming its predecessor with uppercase hex in the
  tenant or hash16 field parsed and was accepted, though no listed key matches
  it as a string. It is now refused with
  `ErasureError::NonCanonicalSupersededRecordKey`, as a version 2 compaction
  record already was.
- **`ravel-cli maintain inspect` prints a compaction record's
  `superseded_record_key`** (issue #2124), after `input_set_hash`, when the
  record sets it.
- **Advisory compaction claims report five `/metrics` counters and gain three
  server flags** (ADR-1029, issue #1035). The supervisor accumulates each
  maintenance pass's claim outcomes, per signal, into
  `ravel_maintain_claims_acquired_total`,
  `ravel_maintain_claims_stolen_total` (a subset of acquired, taken over from
  an expired claim), `ravel_maintain_claims_lost_total` (a held claim taken
  over after its lease expired; the run cancelled),
  `ravel_maintain_claim_renew_failures_total` (a store error on renewal,
  distinct from a lost claim) and `ravel_maintain_claims_skipped_total` (one
  per pass while an unexpired claim holds a bucket). A pass that ends in an
  error drops the counts it had gathered, except renewal failures, so the
  others are lower bounds. The operations guide alerts on lost claims. Three
  new flags configure claiming:
  `--maintain-claim-lease` (default `300s`, refused at zero, warns at startup
  below twice the time to encode and PUT the largest L1 segment at a
  conservative rate), `--maintain-claim-min-input-bytes` (default 64 MiB,
  refused at zero) and `--maintain-claims on|off` (default `on`), the
  fleet-wide escape hatch for a store whose qualification record predates the
  CAS probes or an emergency. Claims stay advisory either way: the
  compaction record's `CreateIfAbsent` remains the sole correctness
  mechanism.
- **`ravel-cli rlog footprint` attributes every stored byte of RLOG objects**
  (ADR-2135 decision 7, issue #2137). Given object keys, local paths, or
  `--tenant`, it reads each object's trailer, footer, FIELD_DIR and PAGE_DIR
  and reports bytes per section (footer and trailer included), stored and
  uncompressed page bytes and page counts per column, and bytes per encoding
  per column; `--json` prints the same report as one document. `--tenant`
  covers the live L0 flush objects and L1 compacted segments the catalog
  resolves, always fetched from the store. Section bytes plus any bytes no
  section covers must sum to the object size, and per-column stored page
  bytes to the BLOCKS length; an object where either does not hold is refused
  with an error naming it and both figures. `rlog inspect` now prints the
  object's own trailer version rather than the build's constant.
- **The per-tenant config record reader accepts format version 3, which adds
  a clustering key and a bloom scope** (ADR-2135, issue #2138). Readers accept
  record versions 1 to 3 and decode `clustering_key` (field 13: up to 4
  declared typed attribute columns, a one-hour, six-hour or one-day bucket
  width, and a clustering generation) and `bloom_scope` (field 14: all,
  undeclared or text).
  The clustering-key accessor reports one of three states: never set (field
  absent, generation 0), cleared (field present with no columns, at its
  generation), or set. An absent scope reads as all. The accessor refuses a
  present key with generation 0, and a set key that names more than four
  columns, a duplicate column, a column outside the tenant's effective
  declared typed attribute columns (which the caller passes, resolving the
  record's override over the deployment default), or an unspecified or unknown
  bucket width. A well-formed but invalid key is reported by the accessor, not
  by record decode, so the rest of the record, retention included, still
  reads. A key can fail record decode only by making the record invalid
  protobuf, as a key column name that is not valid UTF-8 does. Setting and
  clearing a key each store the previous generation plus one. The writer still
  stamps version 2, and the setters and `set_tenant_config` refuse to write
  either field until the writer moves to version 3 in a later release. Nothing
  reads the new fields yet.
- **The maintenance loop sweeps alert history older than `--alert-retention`,
  default 90 days** (ADR-1688 follow-up task 2, issue #1688). After upgrade
  the first tick deletes every alert transition older than 90 days except each
  alert identity's current-state record, so the `alerts` SQL table then
  answers for the window plus current states rather than for all history. Set
  `--alert-retention` before upgrading to keep more, or `--alert-retention 0`
  to keep every record as before; `0` turns off the retention sweep and its
  memo read only, and the alerts shard's orphan sweep described below still
  runs. The worker that owns a tenant's
  `(alerts, 0)` unit reads the tenant's alert state memo, keeps the `ts_ns` of
  every record it names together with its watermark hour, and runs
  `ravel_maintain::sweep_alert_retention`, clamping the memo's watermark to the
  hour of its own clock reading first. A tenant whose memo is absent while it
  has alert records, undecodable, of an unsupported version, unreadable for a
  store reason, or carrying a watermark below the expiry floor is skipped for
  that tick and counted under the new
  `ravel_alert_retention_skipped_total{reason}` family, whose `reason` is one of
  `absent`, `undecodable`, `unsupported_version`, `watermark_below_floor` and
  `store_error`. The family carries no tenant label, so a sustained nonzero rate
  says some tenant's alert history is not being swept, not which one; the
  remedy is object-storage access for `store_error` and the alert evaluator for
  every other reason. A tenant that has never written an alert transition is neither
  logged nor counted. A nonzero `--alert-retention` shorter than one hour plus
  the memo's seal margin (1 h 3 m 30 s at the default evaluation interval) is
  refused at startup, since every tick would skip under it. The same tick runs
  the shard sweep over the alerts shard whatever the window, so a data object
  left behind by a crash between the retention sweep's record delete and its
  data delete, or by an abandoned evaluator write, is moved to quarantine by
  orphan GC like any other signal's; the alert evaluator now abandons a
  transition once its write, the data PUT and the commit publish together, has
  been in flight longer than the ingest writers' `max_flush_lifetime`: it
  checks the bound before every commit attempt and drops an attempt still in
  flight when it passes, which is the interlock that orphan age gate rests on.
  A mass-orphan breaker trip on the alerts shard, or on the query-audit shard's
  input-cleanup sweep, is logged at error with the breaker runbook wording and
  counted as `ravel_maintain_orphan_breaker_tripped_total{signal="alerts"}` or
  `{signal="audit"}`, so the existing alert on that family pages on it.
- **`ravel-cli load --signal spans` loads the spans signal** (ADR-1751
  decisions 1 and 2, follow-up task 2, issues #1751 and #1712). The load
  provisions or validates the tenant's spans signal, builds a
  `SpanIngestRouter` from the same `build_ingest_config` the other loads use,
  and writes every batch in `WriteMode::Strict`. ADR-0089's admission
  decisions apply with the spans OTLP limits: the past event-time lag bound is
  relaxed, the future-skew bound is kept (anchored on the span's end, as the
  OTLP path anchors it), the span-name, status-message and attribute length
  caps are kept, the loader per-record cap of 1024 stands in for OTLP's
  `max_attributes_per_span`, and the server's admission controller is bypassed
  by construction. The refusal `--signal spans` used to get is gone.

  The `[spans]` mapping section names `trace_id_column`, `span_id_column`, an
  optional `parent_span_id_column`, `name_column`, `start_ts_column` and
  `end_ts_column` with their units, an optional `status_code_column` (OTLP's
  0/1/2 integer enum) and `status_message_column`, and
  `[[spans.resource_attribute]]` and `[[spans.attribute]]` columns coerced to
  strings. One input row is one span.

  **A span loaded from Parquet is stored as the same record the same span sent
  over OTLP produces**, field for field. The loader reuses `ravel-otlp`'s own
  normalization rather than reimplementing it: the attribute value coercion is
  `convert_value`'s mapping (integer and boolean verbatim, float through the
  shared `format_float`, bytes as lowercase hex), the resource-over-span merge
  is `ravel_rspan::merge_attrs`, and the status column goes through
  `ravel_otlp::traces_normalize::status_code_from_i32`, which is now public for
  this caller with no change to its body. An empty parent cell (empty binary,
  empty string, or a zero-width fixed-size value) is a root span, exactly as
  OTLP's own empty `parent_span_id` field is; a status outside `0..=2` is
  unset, including one too wide for `i64`; an attribute value over the
  8192-byte cap drops that attribute and keeps the span, as
  `convert_attrs_lossy` does on the OTLP path; and a null attribute cell is an
  attribute the row does not carry, as an OTLP `KeyValue` carrying no value is
  dropped as `MissingAttributeValue`.

  A dropped attribute value is reported, because the stored span is then an
  approximation of the source row and nothing else in the output says so; the
  OTLP path reports the same drop as `AttributeValueTooLong` in its
  partial-success message, and a load has no partial-success channel.
  `SpansLoadReport::attributes_dropped` counts them and the load summary prints
  the count as `attrs_dropped`, on the success path and beside the
  durable-token banner when the load fails. Zero prints too. The count is taken
  where each span is built rather than where its batch acks, so on a failed
  load it also covers batches the failure abandoned, whose spans are in no
  object; the line printed there says that, rather than claiming stored spans.
  A row rejection counts the drops of the rows in its batch built before it,
  and not the rejected row's own.

  Four differences remain, and this is the complete list of what the stored
  record can differ on for the same input. A null `start_ts`, `end_ts` or
  `name` cell is refused: OTLP has no null for any of them, its absent
  timestamp is a zero (a zero takes the same fallbacks here) and its absent
  name is the empty string. A negative timestamp is refused: OTLP's two are
  unsigned, and a negative start beside a positive end would store a span whose
  interval overlaps nearly every query window, so the refusal names both
  declared units, since a unit that does not match the column is the usual
  cause. A `parent_span_id` cell that is non-empty and of the wrong width is
  refused rather than dropped, because a mapped column producing unusable ids
  is a mapping mistake the whole file shares. And attribute keys and both
  attribute-count caps are checked against the `--mapping` rather than per
  span: an empty, over-long, reserved or twice-declared key refuses the load,
  as does a mapping with more than 1024 `[[spans.attribute]]` columns (the
  loader per-record cap, standing in for OTLP's per-span cap of 128) or more
  than 128 `[[spans.resource_attribute]]` columns (OTLP's own
  `max_resource_attributes`, which the loader had no counterpart for before).

  Span events and span links are not mappable in this version, and a mapping
  naming them is refused by name rather than as a typo. The same refusal covers
  the reserved `attrs` keys the OTLP path stores span kind, trace state, span
  flags, events and links under. Two mapped attributes sharing one key are
  refused at mapping parse, and an id column that cannot carry an id of the
  right width is refused when the batch's columns are resolved, before any row
  is built or written. A hex id column loads whether the Parquet writer stored
  it plain or dictionary-encoded.

  A spans load reads one sequential cursor and has no decode/encode queue, so
  `--read-cursors` and `--decode-queue-batches` are warned about when set to a
  value it ignores, and `0` for either is still rejected. It reports the same
  operator surface a metrics load does: the durable-token banner, the resume
  figures on a row boundary, and the resume hint naming only
  `--pipeline-depth`.

- **`/metrics` renders `ravel_health_heartbeat_age_seconds`** (ADR-1702
  decision 11, issue #2048). The gauge is the time since the main runtime's
  heartbeat task last ran, labelled `mode`, in every mode. The heartbeat now
  exists whether or not `--listen-health` is set; when it is, the dedicated
  health listener reads the same heartbeat. The task beats every second, so
  a value that keeps growing means the main runtime is not scheduling it.
  Embedders get `ravel_server::start_with_heartbeat` to pass in a heartbeat
  they built; `ravel_server::start` builds and beats its own.

- **`ravel-server` has an off-by-default `heap-profiling` cargo feature**
  that compiles jemalloc's profiler in, so live memory can be attributed to
  allocation sites with `_RJEM_MALLOC_CONF` and `jeprof` instead of a
  one-off diagnostic branch (issue #2066, where it located the ranged-read
  assembly buffer). The published image does not enable it. The
  troubleshooting guide's "Attributing server memory with a heap profile"
  section gives the build, the runtime settings and how to read a dump.

- **`ravel-cli load` takes `--signal {metrics,logs,spans}` and loads the
  metrics signal** (ADR-1751 decisions 1 and 2, follow-up task 1, issues
  #1751 and #1712). The flag defaults to `logs`, so an invocation written
  before this change is unaffected. A metrics load provisions or validates
  the metrics signal, builds an `IngestRouter` from the same
  `build_ingest_config` the logs load uses, and writes every batch in
  `WriteMode::Strict`; ADR-0089's admission decisions apply per signal, with
  the metrics OTLP limits (past event-time lag relaxed, future skew kept,
  metric-name and label length caps kept, the loader per-record cap of 1024
  in place of OTLP's `max_attributes_per_point`, the server's admission
  controller bypassed by construction). The spans half of the same flag
  shipped in the entry below.

  The `--mapping` TOML gains per-signal sections: exactly one of `[logs]`,
  `[metrics]` and `[spans]` may be present and it must match `--signal`. A
  mapping file written before this change needs no migration: its logs keys
  sit at the document root with no section, and that shape is still read as
  the `[logs]` section. Mixing the two spellings in one file is refused,
  since which one wins would otherwise be an invisible precedence rule.

  The `[metrics]` section names the metric (a literal `name` or a
  `name_column`), `value_column`, `ts_column` and `ts_unit`, an optional
  `unit`, `[[metrics.label]]` columns, an optional `kind` of `gauge` or
  `counter` that sets `is_monotonic_sum`, and an optional
  `[metrics.histogram]` classic shape (`le_column` plus `sum_column` and
  `count_column`). A loaded metric lands on the same `SeriesId` as the same
  metric admitted over OTLP: the metric name and every label name go through
  the same sanitizers `ravel_otlp::normalize` applies, then the same
  `prometheus_family_name` suffix pass, so `unit = "s"` stores `_seconds` and
  `kind = "counter"` stores `_total` exactly as a monotonic OTLP `Sum` does.
  A label cell holding the empty string is dropped from the series, as OTLP
  drops an empty attribute value, so `{job=""}` and `{}` are one series on
  both paths. `kind` may not be set together with `[metrics.histogram]`:
  OTLP has no monotonic histogram, and the key would otherwise name a
  behaviour the load cannot produce. With the
  histogram shape one input row is one bucket, and its `value` column is that
  bucket's own count (the OTLP `bucket_counts` convention, not an
  already-cumulative Prometheus `_bucket` value): the loader groups a
  contiguous run of rows sharing a metric name, label set and `ts` into one
  data point and explodes it into `_bucket`/`_sum`/`_count` series mirroring
  `ravel_otlp::normalize`'s `explode_histogram`, accumulating the per-bucket
  counts, taking the `+Inf` bucket and `_count` from the `count` column
  rather than from the accumulated total, and formatting `le` through
  `ravel_otlp::promcompat::format_float`, so the same histogram lands on the
  same `SeriesId` whichever surface admitted it. A mapping that names a
  native (exponential) histogram is rejected. Rows of one data point must be
  contiguous; interleaved rows are refused rather than exploded twice.

  A metrics load reads one sequential cursor rather than the logs path's K
  stride cursors, because a classic histogram's data point is a contiguous
  run of rows a stride read would split, and it warns when `--read-cursors`
  or `--decode-queue-batches` was set to a value it therefore ignores (0 is
  still rejected for either, as on the logs path). A failed metrics load's
  resume verdict names only `--pipeline-depth`, and its durable token list
  equals what landed at any pipeline depth: a write that fails in the
  end-of-load drain still collects every later write's tokens, and a row
  rejection or batch failure that meets a failed earlier write keeps its own
  reason and carries the drained tokens. A classic-histogram data point with
  more explicit bounds than `max_histogram_buckets` (160) is refused as its
  rows arrive rather than after the whole group is buffered, and a schema
  error in a `--mapping` section names its line and column. Everything that
  shapes the objects (`--shards`, `--batch-rows`,
  `--target-bytes`, `--max-inflight-flushes`, `--max-flush-delay`,
  `--pipeline-depth`) applies unchanged. A data point may span a batch
  boundary; its rows are credited to the write that carries its points, so
  `rows_written` and the `next --skip-rows` offset a failed load prints
  always land on a data-point boundary and a resume loads the next data point
  whole rather than a truncated one. As for logs, a historical sample buckets
  by load time, so retention runs from the load hour and a query needs a
  window that reaches it.
- **A documentation claims registry and its gate** (ADR-1658, issue #1658).
  `docs/review/claims.yaml` records, for each registered sentence of a normative
  doc, what it claims, whether the code agrees, and the code or test that makes
  it true; the seed holds 22 entries. `scripts/check-doc-claims.py` fails when a
  registered quote no longer occurs once and only once, when a bound symbol or
  test is no longer defined (a keyword inside a comment, a string or a type
  position does not count), when a line of a normative doc carries one of the
  fixed absence markers ("does not exist", "not implemented", "will land", and
  five more) without an entry, or when a contradicted entry names no issue or a
  not-implemented one's symbols have all landed. It runs in `make check-docs`
  and CI's doc-scripts job. The seed registers two sentences in
  `docs/query-engine.md` the code contradicts (#2082): the stamp carrier's write
  side, which the logs flush and RLOG compaction paths now call, and per-key
  `attrs['k']` projection, which ships.
- **An alert-signal retention sweep that keeps every identity's current-state
  record** (ADR-1688, issue #1688). The alert evaluator writes one object and
  one commit record per transition and nothing ever removed them, so the
  history grew for the life of the deployment and a cold start re-read all of
  it. `ravel_maintain::sweep_alert_retention` bounds it: it lists the alert
  shard's commit prefix, skips a record whose key-derived hour already proves
  it cannot be expired, and deletes an expired, past-horizon record's commit
  record before its data object, consulting the same legal-hold hook the other
  sweeps use. It writes no tombstone, because the evaluator's fold refuses any
  bucket entry that is not a commit or compaction record. A keep set built from
  the alert state memo spares each identity's current-state record whatever its
  age, so a firing alert older than the window keeps the one record that says
  so and a cold-start fold over the survivors still recovers every identity's
  state. The keep set is the `ts_ns` of those records, which is what the memo
  carries and what each commit record's `max_event_ts_ns` equals, together with
  the memo's watermark hour; a record is deleted only when its ingest hour is
  strictly below that watermark, since the memo is complete only below it and a
  late write into the watermark hour itself may be an identity's newest
  transition. `CompactorConfig::alert_retention_window_ns` is the
  window, default 90 days, the same value as the query-audit window; `0`
  disables the sweep and keeps the previous grow-forever behaviour. The
  maintenance loop runs it through the driver described in the
  `--alert-retention` entry above.
- **Catalog and PromQL decodes reserve their decoded output against the
  process memory budget before they run** (ADR-1702 decision 6, issue #1702).
  The catalog resolve reserves each snapshot part's, postings object's and
  column-statistics object's header-declared uncompressed length, and the
  reservation stays with the decoded value, in the decoded-part and postings
  caches included, until it is dropped; the column-statistics reservation is
  released when `load_column_stats` returns, since the loaded statistics do
  not carry it. The PromQL fetcher reserves a segment's catalog sections
  before `decode_selected` or `decode_sparse_catalog` decodes them, and the
  `/api/v1/metadata` cache reserves a record's declared decompressed size. A
  reservation that does not fit fails the read with a typed error
  (`CatalogError::MemoryExhausted`, `LoadColumnStatsError::MemoryExhausted`,
  `FetchMemoryExhausted`, `MetricsMetaError::MemoryExhausted`); a catalog
  refusal never falls back to a listing pass, and a column-statistics refusal
  reaches a SQL client as the same transient 503 a store fault does, never as
  corrupt data. **The segment fetcher's reservations are live in a running
  server now** wherever it runs under the process budget: PromQL evaluation,
  cache warming and distributed query fragments, so a read whose catalog
  decode does not fit is refused with 503 rather than decoding uncharged. The
  SQL query path's fetchers still reserve against their own unlimited budget,
  so a SQL scan's catalog decode is charged but never refused yet. The
  catalog and the metadata cache take the budget through the new
  `Catalog::with_memory_budget` and `MetadataCache::with_memory_budget`, and
  both still default to an unlimited budget, so the part, postings,
  column-statistics and metadata reservations refuse nothing until the server
  wires the real budget into them. `read_metrics_meta_for_serve` now takes the
  budget and returns the reservation alongside the entries.

  A chunked (sparse) segment's catalog is charged what it inflates to, not
  what it stores: SERIES_META_CHUNKS carries no section-level compression, so
  its footer `uncompressed_len` is the stored zstd frames, and the reservation
  instead sums the SERIES_IDX chunk directory's `frame_uncompressed_len`. Once
  the matchers have run, the whole-catalog reservation is exchanged for one
  sized to the entries that survived, so a selective query does not hold the
  whole catalog's charge through its page fetches; the exchange never fails a
  read, and a refused one keeps the whole-catalog reservation. A snapshot
  part's tenant check, and a column-statistics object's tenant, version and
  part-binding checks, now run before the reservation, so a cross-tenant object
  is still reported as an isolation breach, and a stale-bound statistics object
  still degrades to no statistics, under a budget too small to decode it.
- **The `ravel-operator` Deployment itself now has a securityContext, a
  metrics/health HTTP surface, and liveness/readiness probes** (issues #1731
  and #1923). Its own Pod and container now carry the same hardened
  `securityContext` every server container it renders already carries
  (non-root, no privilege escalation, read-only root filesystem, every Linux
  capability dropped), admitted under the `restricted` Pod Security Standard,
  where it was previously rejected. A new `health` container port serves
  `/healthz`, `/readyz`, and a hand-written `/metrics` Prometheus exposition
  (`ravel_operator_reconciles_total`,
  `ravel_operator_reconcile_duration_seconds`,
  `ravel_operator_last_successful_reconcile_timestamp_seconds`,
  `ravel_operator_watched_clusters`); `deploy/k8s/operator/operator.yaml`
  wires liveness and readiness probes at those paths and a
  `prometheus.io/scrape` annotation. `operator.yaml`'s header comment and
  `docs/guides/kubernetes.md` now point real (non-`kind`) clusters at a
  digest-pinned image rather than a moving tag. The listener binds before
  the controller starts, so an address `--listen-health` cannot bind stops
  the operator with an error naming both; `/readyz` answers `200` once the
  initial `RavelCluster` list has arrived.

- **An end-to-end proof that a stalled fold pages before it refuses a query**
  (issue #1306, ADR-1306 follow-up task 3). ADR-1306 decision 2 states the
  ordering as arithmetic over spans;
  `fold_stall_alert_fires_before_first_request_budget_refusal` runs it. One
  simulated timeline writes flushes at a scaled cadence, folds on a schedule,
  then wedges the fold with a fault on its HEAD PUT, and at every simulated
  minute evaluates the shipped `RavelCatalogFoldStalled` condition against the
  rendered `/metrics` gauge and runs a cold last-6-hours query under the
  derived budget. The alert fires nine minutes before the first
  `RequestBudgetExceeded`; the same timeline replayed against the one-hour span
  ADR-1306 replaced is refused at the first minute, before the fold has stalled
  at all. Time is injected throughout: no step waits on the wall clock.

- **`ravel_maintain_bytes_reclaimed_total` and
  `ravel_maintain_retention_lag_seconds` render on `/metrics`** (issue #1729).
  The first is a per-signal counter of bytes freed by the sweep, summed from the
  listed object size of the two deletions that already carry one, the quarantine
  reaper and the unreferenced-part delete; superseded and retention deletions
  are excluded because they delete by key without a size, so the counter
  undercounts the bytes freed and its HELP text says so (a unit swept by two
  replicas during an ownership handoff can count one object twice). The second is a per-signal gauge of how
  far past its retention deadline the oldest still-present expired bucket is, as
  observed by this process's most recent completed maintenance cycle, from the
  injected clock; it is a per-cycle maximum over the process's units and is 0
  when no expired bucket is still present. Both sit next to the existing
  `ravel_maintain_*` families under the same maintain-mode gate and carry only
  the `mode` and `signal` labels. The troubleshooting and observability guides
  gain alert suggestions for a retention lag that keeps climbing, and the
  "storage keeps growing" row now names series an alert can read,
  `absent(ravel_maintain_workers_live)` for a missing maintain process and the
  pending, lag and deleted-objects series for one falling behind, before the
  per-bucket `ravel-cli maintain status` call.
- **A full-object GET now verifies the body against the checksum the store
  recorded at upload** (ADR-1696, issue #1696). A commit record is a bare
  protobuf with no checksum of its own, so a flipped bit inside a stored record
  decoded as a valid record: a flip in `max_event_ts_ns` moved the segment out
  of a query's range and the answer came back short and error-free. The record
  layout is unchanged; the check moved to the transport that stores the bytes.
  The S3 adapter asks the endpoint for the stored checksum with
  `x-amz-checksum-mode: ENABLED` and recomputes `x-amz-checksum-crc64nvme` or
  `-crc32c` over the body that arrived, failing a mismatch with the `Corrupted`
  error the contract already reserves for one, so no reader needed a new error
  arm. MinIO-style endpoints, RustFS included, return the checksum only on an
  unranged GET, so the first request of a full-object read is unranged; its body
  is read up to the per-request bound and the rest of the response dropped, so
  the memory and timeout bound on one request is unchanged, and a commit record
  read is still exactly one request. Because the request header must be signed
  and the HTTP connector that reads the response header runs after signing, the
  header rides on the client's default headers, which `object_store` signs onto
  every request but a LIST; a LIST carries no such header.
  `S3HttpConfig::request_stored_checksum` (default on) stops sending it; no
  server flag sets it yet. `MemoryStore` keeps a CRC-32C beside each object and
  checks it the same way, so the semantics oracle matches;
  `MemoryStore::corrupt_stored_byte`, which makes that testable, sits behind a
  new `test-support` crate feature that production builds leave off. A read
  with no verifiable checksum is served and counted, never refused: that covers
  an endpoint that returns no `x-amz-checksum-*` header, a digest this adapter
  cannot recompute (SHA-256, or a composite multipart digest), and an object
  larger than one request body. The count is
  `StoreMetricsSnapshot::get_unverified` (and `S3Store::get_unverified`); it is
  not yet exported at `/metrics`. Caller-issued ranged reads are outside the
  check entirely, since the endpoint returns no checksum on them; they keep the
  format's own crc32c hierarchy as their check. Write-side upload integrity
  still defaults to off and is unchanged here.
- **`ravel-ingest` has an opt-in idle flush byte floor, off by default**
  (ADR-1737, issue #1737). `IngestConfig::idle_flush_byte_floor` defaults to
  0, which changes nothing: every buffer flushes on the same clocks as before.
  When set to a value below `min_flush_bytes` (`IngestConfig::validate`
  refuses anything else), a metrics, log, or span buffer with no strict-mode
  waiter that would write fewer object bytes than the floor waits for the
  sub-floor hold, one `flush_tick` short of `max_flush_lifetime`, instead of
  the 40 s idle clock, and each such flush is counted as
  `flushes_by_age_floor` in the pipeline's metrics snapshot. The hold gives up
  that tick because the age check runs on a tick, so the buffer is at most
  `max_flush_lifetime` old when its flush opens, which is the figure
  `ravel_catalog::FLUSH_BOUND_SLACK_HOURS` is derived from. A
  buffer that reaches the floor goes back to the idle clock, and strict-mode
  writes keep the fast clock. `ravel-server` does not expose the knob yet, so
  no deployment's flush cadence or buffered-mode loss window changes with this
  release.
- **The S3 adapter observes the store's own clock from response `Date`
  headers** (ADR-1685 decision 1, issue #1685). A writer stamps its
  ingest-hour bucket from its own clock and has had no second time source to
  check that reading against, so a host lagging the folder's clock publishes
  acknowledged commit records into an hour the fold has already sealed. Every
  S3 response carries the store's clock in its `Date` header, and the HTTP
  connector this adapter installs below `object_store`'s retry loop is the
  only layer that sees it. `ObjectStoreBackend` gains a defaulted
  `observed_store_time_ns() -> Option<i64>` returning `None`; `S3Store`
  returns the latest response's `Date` as unix nanoseconds, or `None` before
  its first response. Every response counts, an error one included; the latest
  response wins rather than a running maximum, so one wrong header from a
  proxy is corrected by the next response instead of latching for the life of
  the process; and a missing or unparseable `Date` leaves the previous
  observation standing. For a store whose `Date` is correct the value is a
  lower bound on the store's current time, never an estimate of it (a leap
  second is clamped to `:59` to keep it one), and it costs no extra request
  and no new object. Every decorator in the crate delegates to the store it
  wraps, as does `ravel-server`'s `--tenant-kms-config` wrapper, and
  `MemoryStore` reports `None` unless a test sets one through the
  `test-support` setter. Nothing consults the observation yet: the writer's
  clock-lag refusal (ADR-1685 decision 2) lands separately, so no flush
  behavior changes with this release.
  writes keep the fast clock. The `ravel-server` flag that turns it on is the
  next entry.
- **`ravel-server --idle-flush-byte-floor` exposes that floor, and
  `/metrics` reports what it holds** (ADR-1737, issue #1737). The flag takes a
  byte count, defaults to 0 (the sub-floor hold disabled), and reaches the
  `IngestConfig` of all three ingest pipelines, so a deployment that does not
  set it keeps today's flush cadence and buffered-mode loss window exactly.
  A floor at or above `--min-flush-bytes` refuses startup in every mode,
  checked during CLI validation before anything is written to the bucket (and
  again at the top of `start` for a library caller that does not go through
  the CLI), with a message naming both flags rather than silently
  putting every sub-`min_flush_bytes` buffer on the hour-long hold. The new
  `ravel_ingest_flushes_by_age_floor_total` family renders for every signal
  beside `ravel_ingest_flushes_by_age_total`, which is how an operator sees
  the floor holding buffers and sizes the buffered-mode loss window they
  accepted: a row acknowledged in buffered mode in a buffer below the floor
  can sit in memory for up to an hour before its flush opens, and a crash in
  that window loses it. Strict mode is unaffected at any setting. The
  ravel-bench PUT-count estimates count the new trigger too, and the
  `ingest_bench` and `s3_e2e_bench` reports carry `flushes_by_age_floor` in
  their flush breakdown.
- **`ObjectStoreBackend::get_pinned` reads exactly the bytes a recorded pin
  names, and `ravel-object-store` gains read-only external stores per
  credential profile** (ADR-2040, issue #2065).
  - A pin's ETag is sent as `If-Match`, so a replaced object is refused with
    `PreconditionFailed`. A pin's version, when the store reported one, is
    sent as a version selector, so the pinned version keeps being served
    after an overwrite, and a deleted version is `NotFound`. A caller's pin
    rides every request of a split whole-object read, the unranged first
    one included, and such a read is still verified against the store's
    upload checksum when the endpoint returns one.
  - `get_pinned` returns a `PinnedRead` carrying the pin of the bytes
    served. `pin_of` and `get_with_pin` report an object's pin, including
    S3's `x-amz-version-id` when the bucket has versioning on. The default
    implementation refuses with the new `Unsupported` error rather than
    falling back to an unconditional read.
  - `external::ExternalStore` opens one granted bucket per `ExternalProfile`
    and refuses every write with the new `ReadOnly` error. A profile names
    where its secrets live and never holds their values, and a credential
    failure is reported as `CredentialsRejected` without the path or secret.
  - `external::probe` qualifies a candidate bucket: it must honour
    preconditions, and it must not be Ravel's own bucket, whether under
    another name or as a copy that holds Ravel's `sys/tenancy` marker.
  - `ravel_cache::CacheKey::pinned` keys such an object by profile, bucket,
    key, ETag, version and size.

  No shipping binary reaches any of it yet; the callers are #2052, #2051
  and #2054.
- **Parquet table location grants and in-place manifest format** (ADR-2040,
  issue #2050): the new `ravel-pqtable` crate and
  `proto/ravel/parquet_table.proto`. A per-tenant grants record at
  `t/<tenant_hash>/pq/grants` holds the locations an operator admitted, each
  with the credential profile to read it under, and resolves a `LOCATION` URL
  to exactly one grant. A table manifest version pins the tenant's own
  Parquet files in place, by bucket and object key with the ETag and store
  version read at the time, rather than copying them into Ravel's bucket; a
  key that object_store's `Path` would rewrite is refused. Both records carry
  the tenant hash they were written for, and a grants record or manifest read
  under another tenant's key is refused as `Misfiled` rather than read as that
  tenant's. No shipping binary calls it yet.
- **`ravel-cli tenant parquet-grant add|remove|ls` and `ravel-cli parquet
  ls|sweep`** (ADR-2040, issue #2051): the first shipping caller of the
  Parquet location grants record, of manifest resolution, and of both
  qualification probes. `add` refuses a location whose scheme the named
  credential profile's store kind does not address, refuses a granted prefix
  that holds no object (there is nothing to probe preconditions on), refuses a
  store that serves a pinned read carrying an ETag it never issued, and
  refuses a bucket that `probe_not_ravel_bucket` does not clear as external,
  including an inconclusive answer. Only a location that clears all four is
  written. Credential profiles are read from the JSON file named by the new
  top-level `--parquet-profiles` flag or `RAVEL_PARQUET_PROFILES`. `parquet
  sweep` takes its minimum `--grace` from `sys/gc`'s `max_query_duration_ns`,
  the value ADR-0050 section 4 bounds every engine deadline against, and
  refuses a bucket that has no `sys/gc` rather than assuming a default.
- **`ravel-parquet`: DataFusion's Parquet scan over a table manifest**
  (ADR-2040 decision D3, issue #2052). Every read of a manifest file is served
  from the process `ReadCache` or by one GET under a `GetLimiter` permit,
  pinned to the ETag and version the manifest recorded; a read of a file
  overwritten or deleted after the table was created that reaches the store
  fails with `FileChanged` or `FileMissing`, telling the caller to run
  `CREATE OR REPLACE` (a read the cache serves returns the recorded bytes). The footer is
  one explicit range ending at the recorded size, charged to the Probe phase,
  and decoded footers are kept in a byte-bounded cache outside the query
  session; a footer that disagrees with the manifest, whose column chunk byte
  range is negative or runs past the data before the footer, or whose
  embedded Arrow schema panics Arrow's IPC decoder, is refused as `Corrupt`. `TenantParquetStore` names each file under
  the tenant and table version, serves `head` from the manifest and refuses
  every other path, write and list, and `SingleStoreRegistry` answers only
  that tenant's URL. `ParquetTableProvider` applies the D5 coercions
  (`binary_as_string` and the `ravel.cast.<column>` integer casts) and the D6
  file groups: up to `target_partitions` groups for a parallel scan, and one
  group in manifest order, never re-split, otherwise.
- **The logs SQL scan skips segments whose declared-column statistics exclude
  the predicate** (ADR-2121 D1, issue #2151). A declared `i64`/`bool`
  comparison or `BETWEEN`, or a declared `i64` `IN`, now also drops every
  segment whose `SegmentRef` stamp for that column proves no row can match,
  before any fetch, so the segment costs no GET. A `.cstat` entry alone never
  skips a segment: it tallies only the record-level cells, and a row whose
  value lives only in the resource or scope attributes can match outside its
  min/max. A segment with no stamp for the column is never skipped by that
  column's arm, and neither is one whose loaded `.cstat` entry disagrees
  with its stamp; another column's arm or the ts window can still drop it. A Flight SQL `DoGet`
  rebuilds segments from its ticket, which carries no stamps, so it skips
  nothing by statistics. The
  count is reported as `segments_pruned_by_stats` on the logs scan's `EXPLAIN
  ANALYZE` metrics, in `SqlStats`, and in `sql_latency_bench`'s per-statement
  scan diagnostics. `max_segments` admission and the distributed coordinator
  fan-out are unchanged.
- **The logs SQL scan does less work per cell when it builds typed attribute
  columns** (ADR-2121 D2 and D3, issue #2152). When a block stores a typed
  `i64`, `bool` or `bytes` key in exactly one column, of the declared type
  (a resource or scope value of another type adds a column and turns this
  off for the block), each row's cell is appended straight to its Arrow array with
  no intermediate attribute value; a row the record does not set still reads
  the resource or scope value. Building a typed `str` column validates each
  cell's UTF-8 at most once per block: a dictionary page validates each entry
  once and looks a row's id up, and a plain page validates a cell once for both
  the presence check and the value. Query results do not change: a non-UTF-8
  `str` cell still reads as absent and falls through to the resource or scope
  value, and a typed `str` column is still `Dictionary(Int32, Utf8)`.
- Parquet tables answer `POST /api/v1/sql` and Flight SQL (ADR-2040 D3, D4
  and D6, #2053). `ravel-server --parquet-profiles` (`RAVEL_PARQUET_PROFILES`)
  loads the credential profile file `ravel-cli` uses, and each profile's
  read-only store is opened per bucket the first time a query reads it. A
  (profile, bucket) whose bucket address overlaps that of Ravel's own data
  bucket (`--s3-bucket` at `--s3-endpoint` in `--s3-region`, path-style) is
  refused with a typed error before that store is opened. A bucket address is
  where the S3 client sends the bucket's requests: a virtual-hosted endpoint as
  written, a path-style one with `/<bucket>` appended, AWS's regional endpoint
  when there is none. Two overlap on the same AWS partition, on GCS, or on the
  same host and port (a missing port read as the scheme's default) when one's
  bucket and path segments begin with the other's, so a virtual-hosted profile
  at Ravel's host that names no bucket is refused too. An S3 profile endpoint
  carrying a path is refused whenever a store is opened from it, for a query
  and for `tenant parquet-grant add` alike. A
  statement whose only tables are Parquet tables of the caller's tenant
  resolves each table's newest live manifest and the tenant's current grants
  before its session is built: a file outside every current grant fails the
  query with `LocationNotGranted`, and without a profile file a statement
  naming a Parquet table fails with `NotConfigured` (HTTP 422). A Parquet
  table beside a signal table is `CrossSignalQuery`; another tenant's table, a
  dropped table and any other unknown name fail to plan as an unknown table
  always has. Each such name costs one LIST of its manifest prefix, so a
  statement naming more than 16 distinct tables besides the five signal tables
  (`MAX_STATEMENT_TABLE_NAMES`) fails with `TooManyTables` (HTTP 400) before
  any manifest is listed. Only a Parquet session's registry resolves a store,
  its own tenant's `ravel-pq://` URL, and a statement naming a table function
  or a URL-shaped table is refused before any store read. The reader evaluates
  filters in the scan; an exact-typed statement scans in up to
  `target_partitions` file groups with file-scan repartitioning on, any other
  in one group in manifest order. The reader does not use a file's page
  index: it hands the scan a footer with no column index or offset index, and
  the scan runs with DataFusion's `enable_page_index` off, so every column
  chunk is decoded by page header and a corrupt offset index changes no row,
  including when a join or TopK pushes a dynamic filter into the scan. A
  panic in the Parquet decoder while it decodes a corrupt file is reported as
  `Corrupt`, naming the table and the file, not as an operator panic.
  Row-group statistics pruning and `pushdown_filters` are unchanged; there is
  no page-level pruning. Decoded footers and refusals are cached per pinned
  file and footer length the manifest recorded. A refusal is cached: a footer
  length the file cannot hold, and a trailer or footer that does not decode or
  disagrees with the manifest. A read that failed is not cached, whatever it
  failed on: a store error or a read that came back short.
  `tenant parquet-grant add` lists past a zero-byte directory blob, and says so
  when its search for an object stopped at the listing page bound.
- **`ravel-parquet`: `snapshot::snapshot_location`, the file list `CREATE
  EXTERNAL TABLE` will pin** (ADR-2040 decision D2, issue #2052). Not reachable
  from SQL yet; #2054 wires it into the DDL. A location naming one object is
  read with one HEAD and no LIST; a prefix ending in `/` is listed once,
  recursively, and every key ending in exactly `.parquet` becomes a file,
  Hive-style subdirectories included with no partition columns. Directory
  markers and other suffixes are skipped and counted separately. Each footer
  is read with `If-Match` on the ETag the listing reported, under a
  `GetLimiter` permit and charged to the Probe phase, with up to the
  limiter's permits in flight, and the file's ETag, version and size are
  recorded from that read's response, not from the listing. The footer passes
  the same trailer, column chunk and embedded Arrow schema checks the reader
  applies. A typed `SnapshotError` naming the key refuses the whole snapshot
  for a file changed or deleted after the listing, an empty or truncated
  file, a footer the reader would refuse, a listed `.parquet` key the
  object-store client cannot address exactly, and a file whose schema
  differs from the first file's; it also refuses a prefix with no file or
  more than 100,000 (`MAX_TABLE_FILES`), a listing whose raw delivery
  decreases, and a snapshot that outlives its deadline.
- **Tenant resolution carries an optional `ddl` capability** (ADR-2040
  decision 4, "Who may run DDL", issue #2238). Absent by default. A
  `--tenant-token`/`--tenant-token-file` tenant ending in `;ddl` (the tenant
  is the text before the last `;`) grants it for that token; any other
  suffix, or an empty tenant before it, refuses startup. `--oidc-ddl-claim
  <CLAIM>` grants it from a verified OIDC token when that claim is present
  as the JSON boolean `true`. Nothing yet consumes the capability; it is
  plumbing for `CREATE EXTERNAL TABLE` (#2054). Behaviour changes: a
  static-map tenant containing `;` used to be accepted and now refuses
  startup (the only static form for such a tenant would be `x;y;ddl`, which
  also grants the capability); a token defined more than once with a
  different tenant or capability, across flags and file, refuses startup
  naming both positions instead of resolving last-wins (identical repeats
  stay accepted); and an empty `--oidc-ddl-claim` refuses startup.

## [0.19.0] - 2026-09-27

### Fixed

- **The chaos lane reads labeled metrics by label and passes multi-shard
  commit tokens one per parameter** (issue #534). The `scripts/chaos/`
  parser now scans a sample's label set quote-aware, reads the value before
  any timestamp, and takes `label=value` selectors, and the mid-flush trigger
  selects `signal="metrics"`. A comma-joined `x-ravel-commit-token` header is
  split into its opaque tokens, and the header name is matched without the
  gawk-only `IGNORECASE`. Both scenarios now exit 3 on a setup error:
  scenario 1 used to exit 1 there, the same code as its strict-ack oracle
  failure, and scenario 2's exit 2 stays the release-blocking verdict.
  `scripts/chaos/lib.test.sh` covers the
  helpers on every pull request, and the nightly job annotates each
  scenario's exit code.
- **OTLP and Remote Write ingest now take the in-flight permit and check the
  tenant credential before the request body is read or decoded** (issue
  #1705).
  Every ingest handler took the `--max-inflight-ingest-requests` permit and
  resolved the tenant as its first two statements, but on both transports
  those statements ran too late to bound anything: on the HTTP surfaces the
  handler's body argument is an extractor, which axum runs before the handler
  itself, so the whole body was buffered (and a gzip body inflated) first,
  and on the gRPC surfaces the handler runs only after tonic has read and
  decoded the message. An unauthenticated caller could therefore make a
  process read and decode a full request body no matter how far over the
  ceiling it was. For OTLP metrics, logs and traces on both transports and
  for Remote Write, both decisions now happen on the request head, through
  middleware on the HTTP ingest routes and a tower layer on the gRPC
  listener, so a refused request costs the bytes of one request head. The
  OTAP stream is only partly covered: the gRPC layer checks its credential
  on the stream's head, before any frame is read, but its permit is still
  taken per batch after tonic has decoded that batch's protobuf frame, so an
  authenticated OTAP client over the ceiling still costs one decoded frame
  per shed batch. Because the permit is now taken before the body arrives,
  the ceiling counts concurrent uploads as well as concurrent decodes, and
  the body wait is bounded: a body that has not fully arrived 30 seconds
  after admission is refused with 503 and `Retry-After` on HTTP or
  `DEADLINE_EXCEEDED` for a unary gRPC export, and its permit is released, so
  a trickled upload holds its slot for at most 30 seconds. The OTAP stream holds no permit while it
  waits for a frame and is not bounded this way. The gRPC layer claims only
  the ingest services the listener registers, so a process without OTLP
  ingest (for example `--mode query`) still answers `UNIMPLEMENTED` on those
  paths. No HTTP/2 stream cap is derived from the ceiling, so the Flight SQL
  and fragment listeners keep their previous stream limits. The ceiling, its
  flag, and the `ravel_ingest_concurrency_shed_total` counter are unchanged,
  and a refusal keeps the status and message it had: 429 with `Retry-After`
  and 401 for bad credentials on HTTP, `RESOURCE_EXHAUSTED` and
  `UNAUTHENTICATED` ("invalid or missing tenant credentials") on gRPC.
- **CI no longer pulls the AWS CLI image from ECR Public to create a test
  bucket** (issue #2036). ECR Public caps anonymous pulls per source IP, and
  hitting that cap failed `bench-smoke`, `quickstart` and
  `object-store-contract`. Those jobs and the MetricsBench nightly now create
  their RustFS bucket with the runner's own `aws` CLI through
  `scripts/ci-create-bucket.sh`, which retries only the create call's own
  transient failures. The CI quickstart applies a CI-only compose override,
  `deploy/docker-compose/ci-host-bucket.yml`, that turns `createbucket` into a
  no-op on the locally built image; the documented quickstart command is
  unchanged. `demo/kill-and-recover.sh` now restarts `ravel-server` with
  `--no-deps`, so replacing the server no longer re-runs the stack's one-shot
  services.

- **Retention no longer deletes logs or spans objects whose format version
  this build cannot read** (issue #530). The physical sweep's version hold
  covered metrics only, so after a binary rollback across an RLOG or RSPAN
  format bump, a tombstoned logs or spans bucket holding objects written at
  the newer version was deleted once its protection horizon elapsed, even
  though the newer build could still read them. The sweep now reads each
  data object's trailer through the gate of the bucket's own format (one
  16-byte ranged GET per object, the cost metrics already paid) and, if any
  object is outside this build's reader window, deletes nothing in the
  bucket, logs a warning, and counts the objects on the same in-process
  held-object counter, exactly as it does for metrics. A corrupt trailer is still swept. ADR-0531 and ADR-0066
  carry dated amendments recording the wider hold.

- **A PromQL alert rule raises one alert per matching series** (issue #117,
  ADR-0117). A rule used to collapse its result vector into one alert with
  the rule's labels; each series that satisfies the condition is now its own
  alert, whose labels are the series labels without `__name__` overlaid by
  the rule labels, and whose identity hashes those labels. The Alertmanager
  sink's `alertname` stays the rule id, or the rule's own `alertname` label
  when it sets one; a series label named `alertname` never replaces it and,
  when the rule sets no `alertname`, is sent as `exported_alertname` (with
  Prometheus' `exported_exported_` rule on a further conflict) so series
  differing only in that label stay distinct Alertmanager alerts. A
  series that stops matching resolves only its own alert, and
  `repeat_interval` applies to each alert on its own. A rule matching more
  than 1000 series fails the tick with `TooManyAlerts`, and two series
  merging to one label set fail it with `DuplicateAlertIdentity`, whose
  message names the label names and the colliding `alert_id` but no label
  value; neither writes a record, and both count in
  `ravel_alert_rules_failed_total`. A scalar query and a
  SQL rule keep their existing single alert and identity. Two rules in one
  tenant may no longer share a `rule_id`, even with different labels: such a
  rules file now fails startup. Per-series rules over churning label sets
  should wait for alert state pruning (#1438). A new
  `ravel_alert_undelivered_notifications` gauge reports how many
  notifications wait for every sink to accept them; while a sink keeps
  failing it grows by one for every alert identity that transitions. A rule
  whose write fails partway through its alerts now counts the records
  already written in `ravel_alert_records_written_total`.

  Upgrade notes:
  - Expect a notification burst on the first tick. A PromQL rule whose
    matching series carry labels besides `__name__` that the rule labels do
    not override gets a new identity for each series. If its single
    rule-level alert is pending or firing at upgrade, that tick writes one
    Resolved transition for it and one new transition per matching series.
    The webhook sink is notified of every one of them, and the Alertmanager
    sink of every one except a pending transition.
  - A rule that fires today can start failing every tick. It fails with
    `DuplicateAlertIdentity` when two matching series merge to one label
    set, for example a selector over several metric names
    (`{__name__=~"a|b"}`) whose series differ only in `__name__`, or a rule
    label that overrides the series label that told them apart. It fails
    with `TooManyAlerts` when more than 1000 series match. On every tick it
    fails, the rule writes no record, and its existing alerts keep their
    state.
  - A rules file in which two rules of one tenant share a `rule_id` now
    fails startup with `rule id "<id>" is used by more than one rule in
    tenant "<tenant>"`. Give each rule its own `rule_id` before upgrading.
  - Repeat notifications multiply by the firing series. `repeat_interval`
    now applies to each alert, so a rule with 500 firing series sends 500
    repeat notifications per interval to every sink where it used to send
    one. Raise `repeat_interval`, or set it to `0s`, on rules that match many
    series.
### Added

- **`ravel-server` builds a read and a write CPU gate and reports their
  queueing on `/metrics`** (ADR-1702, issue #1702). A new crate,
  `ravel-cpu-gate`, holds the gate: a job at or above its inline floor
  (256 KiB, or 100,000 samples for a PromQL evaluation) waits for one of a
  fixed number of permits, then runs on the tokio blocking pool and keeps the
  permit until it returns; a smaller job runs on the calling thread.
  `--cpu-gate-read-permits` and `--cpu-gate-write-permits` size the two gates.
  Unset, they are `max(1, cores - 1)` and `max(1, cores / 2)`, and `0` is
  refused at startup. `/metrics` renders `ravel_cpu_gate_permits`,
  `ravel_cpu_gate_running`, `ravel_cpu_gate_queued`, the
  `ravel_cpu_gate_wait_seconds` and `ravel_cpu_gate_run_seconds` sum and count
  pairs and `ravel_cpu_gate_abandoned_total` per `gate`, plus
  `ravel_cpu_gate_jobs_total` and `ravel_cpu_gate_inline_total` per `gate` and
  `site`, and the tokio runtime's `ravel_runtime_workers`,
  `ravel_runtime_alive_tasks`, `ravel_runtime_global_queue_depth` and
  `ravel_runtime_worker_busy_seconds_total`. No decode or encode path submits
  work to the gates yet; the later ADR-1702 tasks move them.
- **`ravel-cli export --signal logs` writes a tenant's stored logs back out to
  a Parquet file `ravel-cli load` reads in** (ADR-1751, issue #1712). The
  command takes the store and tenancy flags the other read commands take, plus
  `--tenant`, an RFC 3339 `--start`/`--end` window (`Z` or any numeric
  offset, converted to UTC), `--parquet` for the output path, and the same
  `--mapping` TOML a load uses to decide which column each field lands in. It
  resolves the catalog once, reads and decodes the RLOG objects that snapshot
  names, and writes the rows sorted by event time. The
  window is half-open, so exporting adjoining ranges writes no row twice and
  drops none between them. Exclusion follows the query path rather than a
  second copy of the rules: retention tombstones and compaction supersession
  are already applied by the catalog resolve, and erasure predicates from that
  same snapshot are handed to the segment fetcher and then matched against
  each decoded record's merged resource, scope and record attributes by the
  same function the SQL log scan calls, so a record a query cannot see,
  including one whose erased subject is only a resource or scope attribute, is
  a record the export does not write. The mapping's typed attribute
  columns round-trip, and the optional `attrs_map_column` adds one map column
  carrying every record attribute no typed column covers. `--signal` has no
  default and today accepts only `logs`; `metrics` and `spans` are refused
  with the bulk-import follow-up each one waits on, because an exported file
  no command can load back is not an export. `--parquet` is replaced only
  once the export finishes, by renaming a temporary file written beside it,
  so a failed export leaves an existing file untouched; a path under `/dev`
  or an existing directory is refused before anything is read.
  `--max-ingest-lag` passes the deployment's own `ravel-server
  --max-ingest-lag` through when it differs from the 2h default, and refuses
  zero as the server does. The whole window is decoded into memory
  before the first row is written, so a wide range wants several narrower
  exports. The ingest guide's bulk-export section covers that and the
  round-trip caveats, including `ts_unit` truncation and the fact that `load`
  does not read `attrs_map_column` back, and that no mapping key names a
  record's observed time, flags, instrumentation scope name and version, or
  scope attributes, so export drops them. Export costs `ravel-cli` a normal
  dependency on `ravel-query`, which is what sharing the query path's
  exclusion rules rather than copying them is worth (ADR-1751 amendment,
  2026-09-26).

- **`ravel-server --listen-health <addr>` serves liveness and readiness from
  its own thread** (ADR-1702). The listener runs on a dedicated
  single-threaded runtime and serves only `/healthz`, `/readyz`, `/-/healthy`
  and `/-/ready`, so a node whose main-runtime workers are all busy still
  answers its probes. A heartbeat task on the main runtime keeps it honest:
  `/healthz` there returns 503 once the heartbeat is older than 60 s, and
  `/readyz` once it is older than 30 s or any existing readiness condition
  fails. The flag is unset by default, so nothing binds, and the routes on
  `--listen-http` are unchanged. Startup refuses a `--listen-health` address
  equal to any other listener's, and shutdown waits at most 5 s for open
  health connections before dropping them, plus a 1 s join margin, so the
  worst case from SIGTERM to exit grows by up to 6 s (32.5 s to 38.5 s). A
  hand-written manifest that sets the flag needs a termination grace period
  to match.
- **The operator can point both probes at a dedicated health port with
  `spec.probes.dedicatedHealthPort`** (ADR-1702, issue #1702). Set to true,
  every gateway, query, and maintain container gains
  `--listen-health 0.0.0.0:4316`, a container port named `health` on 4316,
  and liveness and readiness probes on that port instead of 4318, with the
  same paths, period, timeout, and failure threshold as before. The health
  listener runs on its own thread, so a main runtime busy decoding segments
  can no longer let a probe time out and have the kubelet restart the pod.
  Those pods also get `terminationGracePeriodSeconds` 51 instead of 45, to
  cover the health listener's longer shutdown.
  The same routes stay on 4318 as well, so anything already probing the HTTP
  port is unaffected, and the `ravel-ingest-router` Deployment keeps its
  probes on 8080. The field defaults to false in this release and flips to
  true one release later; setting it needs a `ravel-server` image from this
  release or newer, since an older server rejects the unknown flag and
  restart-loops.

- **`spans.links` decodes span links into a structured, filterable column**
  (issue #1710). Symmetric to `events`, it is built from the plain
  `attrs["_links_raw"]` protobuf blob at scan time, on every RSPAN version,
  since RSPAN is a frozen persistent format and promoting links into a
  nested on-disk column would need an ADR and a version bump. NULL when a
  span carries no link, or when its `_links_raw` value is malformed (bad
  hex, bad framing, a link chunk missing a well-formed `trace_id` or
  `span_id`, a non-UTF-8 `trace_state`, or a `trace_id`, `span_id` or
  `trace_state` field that is not a length-delimited value), never an empty
  list or a fabricated field; one malformed link makes the span's whole
  `links` value NULL. Selecting `events` or `links` turns off the columnar
  fast path. On a single node, a query that selects neither never builds
  those columns; under distributed execution each worker still builds both
  for every row it returns and the coordinator drops them. A bare single-node
  `SELECT count(*) FROM spans` now returns the row count, with or without a
  pending erasure, instead of failing with "must either specify a row count or at least
  one column".
- **`/metrics` now renders a per-shard ingest skew family** (issue #1692).
  `ravel_ingest_shard_messages_enqueued_total`,
  `ravel_ingest_shard_messages_processed_total`,
  `ravel_ingest_shard_queue_depth`,
  `ravel_ingest_shard_on_actor_seconds_total`,
  `ravel_ingest_shard_flush_permit_wait_seconds_total`, and
  `ravel_ingest_shard_off_actor_seconds_total` render one series per
  configured shard, labelled `mode`, `signal`, and `shard`, with idle shards
  reading zero rather than being omitted. Zero-fill covers shards 0 to
  `--shards - 1`; a tenant resharded above `--shards` renders its extra
  shards once they record activity. The span pipeline now counts enqueued
  messages in `span_router.rs` and records on-actor and off-actor time in
  `span_shard.rs`, matching the metrics and log pipelines, so all six series
  carry live figures on all three signals.

### Changed

- **A transient object-store error on an audit-record PUT is retried before the
  batch fails closed** (issue #2035). `write_audit_batch` used to fail an
  entire batch of queries on a single object-store timeout or throttle
  response, even though the object store's own client-side retry never covers
  a conditional PUT (`PutOptions::create_if_absent()`, the mode both the data
  object and the commit record use, is never marked idempotent by the S3
  client, so a `Timeout` on it skips the client's retry loop entirely). Each
  PUT now retries up to two more times with a short jittered backoff when the
  error is one already classified as transient, and only a non-retryable
  error or one that keeps failing across every attempt still fails the batch
  closed, exactly as before. A 30 s budget bounds each tenant group's
  writes in a flush, both PUTs and every retry, so a hung store still fails
  closed rather than stretching out for the full retry ladder. A flush
  writes its tenant groups one after another, so against a hung store a
  query can wait up to 30 s for each group ahead of its own. A throttle
  response's retry-after hint is honoured when it fits the budget, and a
  commit record found already stored on a retry counts as written when its
  bytes match, since the earlier attempt landed and only its acknowledgement
  was lost. Every retried attempt is now counted on `/metrics` as
  `ravel_audit_put_retries_total`.
- **The default query request budget is now derived from the unsealed tail a
  healthy catalog carries plus the fold-stall alert window** (ADR-1306). At 4
  shards and a 2 s flush cadence it gives 343,400 requests instead of 15,800,
  so a wide query is not refused for fold lag before the fold-stall alert
  pages. Each unsealed flush is budgeted at 8 requests, which covers flushes
  above the fetcher's 512 KiB whole-object threshold: one fetch of an L0
  segment now issues at most 4 page-range GETs, bridging the smallest gaps
  between the page runs a query selects when there are more, so it costs at
  most 7 GETs plus its commit-record GET. The bridged gap bytes are fetched,
  reserved against the process fetch memory budget and charged to
  `max_bytes_scanned`, so a selective query over large L0 flushes can read up
  to about the object size per segment, the same order as the existing
  whole-object fallback. The per-flush figure holds per selector: each
  selector fetches the segment again, so an N-selector query can spend up to
  `1 + 7N` requests per flush, which the derived budget does not scale for. An
  explicit `--max-s3-requests` is still used as given.
- **A request-budget refusal now names fold lag when fold lag is what the
  budget was spent on** (ADR-1306 decision 6 and its 2026-09-27
  refusal-threshold amendment, issue #1306). Each resolve records the unsealed
  tail it listed live above the fold watermark, and a refusal whose tail is
  longer than the engine's `fold_lag_threshold` appends that tail's length in
  seconds and names `ravel_catalog_fold_last_success_timestamp_seconds`, the
  gauge the `RavelCatalogFoldStalled` alert reads, so an operator reading the
  error goes to the fold rather than to the budget. Every other refusal keeps
  its previous message to the byte. The threshold is `healthy_tail_max` of the
  catalog's seal margin plus the fold interval plus the HEAD cache TTL
  (8,400 + 300 + 30 = 8,730 s at the defaults): a fold leaves at most
  `healthy_tail_max` unsealed at the instant it runs, then lets the tail grow
  for one interval, and the HEAD a resolve reads may be one TTL older again,
  so classifying against `healthy_tail_max` alone would blame a fold that is
  keeping up for about five minutes of every hour. The tail comes from the
  origins the resolve already produced, never an extra object-store request,
  and it is reported only when that resolve read a folded snapshot part: a
  resolve that found no usable snapshot lists the whole window live, tags
  every key as recent including hours the fold has already sealed, and so
  names nothing. A refusal therefore names fold lag only when the resolve
  behind it read a snapshot part and the tail above that watermark exceeded
  the threshold; the rule is conservative the other way, so a stall whose
  window holds no sealed segment goes unnamed. Both forms keep their statuses:
  HTTP 422 on the PromQL path, and 422 (gRPC `FailedPrecondition` over Flight
  SQL) on the SQL path. `EngineConfig` gains `seal_margin`, `fold_interval`
  and `head_cache_ttl`, defaulting to the catalog's and the server's own
  compiled-in values; passing a running server's `CatalogConfig` and
  `FoldTaskConfig` through to them is a follow-up, so a deployment that has
  changed them is classified against the defaults until then. On the SQL path
  the clause is attached at the resolve-boundary check only; a SQL refusal
  raised mid-scan, and the exemplars read's own budget check, still read as
  plain budget refusals.
  explicit `--max-s3-requests` is still used as given. The seal margin the
  span is built from is the one the server's own catalog folds and resolves
  with, read off the `CatalogConfig` its `build_catalog` constructs rather
  than from a compiled-in reference. No flag moves that margin today; when
  one does, the budget follows it with no hand recomputation. The
  shipped `RavelCatalogFoldStalled` alert is now held to the same span by a
  test: its threshold must be that seal margin and its `for:` the 600 s the
  derivation budgets, so raising either in
  `deploy/prometheus/ravel.rules.yaml` fails a gate rather than silently
  moving the first refusal ahead of the page. `ravel-server` logs the resolved
  budget once at startup with the span it is measured against
  (`max_s3_requests`, `source`, `covered_span_secs`, `seal_margin_secs`),
  derived or explicit alike, so an operator who pins `--max-s3-requests` can
  see which span their value undercuts. Scalar and histogram
  pages are now fetched in one batch, so a `page_fetch` tracing span can
  carry `page_kind="mixed"`, where it used to be only `scalar` or
  `histogram`.
- **The scheduled catalog fold now runs only in `--mode maintain` and
  `--mode all`, and a `maintain` fleet partitions it across its replicas**
  (ADR-1693, issue #1693). A `maintain` process folds only the
  `(tenant, signal)` pairs it owns under the rendezvous hash and heartbeat
  live set that already distribute maintenance units, and it tests ownership
  before every per-tenant read, the pair's lifecycle `t/<hash>/config` record
  and its `HEAD` peek alike, so a non-owner issues no object-store request at
  all for a pair it does not own. The only request a tick makes that no owned
  pair accounts for is the single delimited listing of `t/` that discovers the
  tenants. Scaling `maintain` to N replicas therefore divides the fold's
  request cost N ways instead of running N full copies of it, and a replica
  leaving the fleet hands its pairs to the survivors within 570 s at the
  defaults: the 180 s liveness window, plus the survivor's next 60 s heartbeat
  tick, which is when it re-lists and recomputes the live set, plus its next
  fold tick, 300 s with up to 10% jitter. That bounds the survivor's first
  tick over a pair, not always its re-fold: a pair the departed replica
  folded just before leaving keeps a `HEAD` younger than the 300 s fold
  interval, a survivor tick before then skips it as fresh, and the next tick
  is at most 330 s later, so the re-fold lands within 300 + 330 = 630 s of
  departure. The liveness window alone bounds when the departure becomes
  visible, not when the pairs are folded again.
  `--mode all` computes its live set as itself alone and keeps folding
  everything. `gateway` and `query` processes no longer fold on a timer. A
  `query` process keeps the on-demand `POST /api/v1/admin/fold` route, which
  is unchanged; a `gateway` process mounts no fold route at all, as before. A
  deployment running neither `maintain` nor `all` no longer folds on a timer
  at all, and must add a `maintain` process. `--disable-fold` and
  `--fold-interval-secs` are now refused at startup in `--mode gateway` and
  `--mode query` instead of being accepted and ignored, and the operator
  renders them on the maintain Deployment: `spec.maintain.fold` replaces
  `spec.gateway.fold`, which is now refused with a `Degraded` condition
  (reason `GatewayFoldUnsupported`) naming the field that replaces it.
  `ravel_catalog_fold_last_success_timestamp_seconds` now renders only in the
  two modes that fold on a schedule, so no process publishes a permanently
  stale gauge and `RavelCatalogFoldStalled` fires through its `absent()` arm
  on a fleet with no scheduled fold; `ravel_catalog_fold_cycles_total` and
  `ravel_catalog_fold_failures_total` render wherever a fold can run, the
  `query` mode's on-demand route included, so an on-demand fold's failures
  stay visible. The fold also reads the injected maintain clock rather than
  the system clock. Each signal's fold loop now runs under a supervisor: a
  panic in a tick body is caught, counted on the new
  `ravel_catalog_fold_loop_restarts_total{signal}` counter, logged at error
  level, and the loop is respawned after a bounded backoff doubling from 1 s
  to 60 s and resetting after a completed tick; the respawned loop ticks as
  soon as its backoff ends, so a loop panicking on every tick restarts 20
  times in its first 15 minutes and 15 times in every 15 minutes after at the
  defaults. The supervision is not
  optional under a partitioned fold: a replica whose loop dies keeps
  heartbeating, so it stays in the live set, keeps its pairs, and leaves them
  unfolded while its peers' fresh gauges hold `RavelCatalogFoldStalled`
  (`max by (signal)`) under its threshold. The new
  `RavelCatalogFoldLoopCrashLooping` rule in
  `deploy/prometheus/ravel.rules.yaml` fires on more than 5 restarts in 15m,
  unaggregated, because the condition is about one replica. It carries
  `for: 15m`: a loop panicking on every tick crosses the threshold 31 s after
  its first panic and pages about 15.5 minutes after it, while a single
  transient panic counts one restart. Supervision covers panics only: a tick
  that hangs on a store call that never returns moves no restart counter, and
  no shipped alert catches it on one replica of several, since only that
  replica's own `ravel_catalog_fold_last_success_timestamp_seconds` stops
  moving and `RavelCatalogFoldStalled` reads the fleet-wide maximum.
  **On upgrade**, a
  `RavelCluster` with `spec.gateway.fold` set now fails the render before any
  tier renders, so the whole cluster stops reconciling until the field moves
  to `spec.maintain.fold`; a hand-written manifest passing `--disable-fold` or
  `--fold-interval-secs` to a gateway or query container now fails at startup
  instead of ignoring the flag; and during a rolling upgrade an old-version
  maintain pod sits in the live set without folding, so the pairs the hash
  gives it stay unfolded until the rollout completes.
- **A distributed-query worker resolves each pinned segment from that segment's
  own commit record instead of re-resolving its catalog** (issue #1721). Every
  fragment request used to re-resolve the worker's catalog to map the
  coordinator's pins back to segments, so a compaction committed between the
  coordinator's resolve and the worker's fetch could invalidate the slice and
  cost a full re-resolve and re-dispatch. The worker now rebuilds the key of
  the pinned segment's own record from the identity the coordinator shipped
  with the ADR-0010 key builders: the commit record for an L0 segment, and the
  compaction record (or, when none exists, the erasure rewrite record) for a
  compacted L1 segment. It GETs that one record, checks that the record's own
  fields address the key it was read from, verifies the data-object key with
  `verify_object_key` (or the L1 object-key reconstruction), and compares the
  full 32-byte content hash, the object size, and for L1 the full input-set
  hash and `part_index` against the identity. The segment ref it reads is
  built from the verified record alone. A worker lists nothing and reads no
  manifest on the intra-cluster fetch path; it issues one record GET per
  pinned L0 segment, one per pinned L1 segment, and two for an L1 segment only
  a rewrite record describes. Each record GET holds a permit of the worker's
  process-wide GET limiter, the one its data-object GETs use. Record GETs are
  not charged to the slice's accounting, so they count toward neither
  `max_bytes_scanned` nor `max_s3_requests` on the worker or the coordinator,
  and a query that fits its budget locally also fits it distributed; they are
  also not in the query's reported cost, which the slice summary carries as
  one pooled figure. Off the budget is not unreported: a worker exports the
  totals as `ravel_distrib_fragment_record_get_requests_total` and
  `ravel_distrib_fragment_record_get_bytes_total`, process-wide counters
  beside the other `ravel_distrib_fragment_*` series, carrying only the
  `mode` label. A record that is missing, unreadable, fails verification,
  or disagrees with the identity fails the fragment with `UNSUPPORTED`, and the
  coordinator runs the query locally; a malformed identity is `BAD_DATA`. A
  throttled, timed-out or transient error on the record GET fails an inbound
  fragment with `UNAVAILABLE`, and the coordinator re-dispatches that slice to
  another worker. On a slice the coordinator runs itself there is no other
  worker to re-dispatch to and `UNAVAILABLE` would be terminal, so that same
  failure fails the slice `SNAPSHOT_INVALIDATED` instead and the coordinator
  re-resolves and retries once, keeping the recovery the catalog re-resolve
  had. That covers both local arms: a self-mapped or unroutable slice, which
  is dispatched straight to local execution with no remote attempt at all, and
  the fallback after a remote worker and its one re-dispatch both failed. Only
  the resolve phase moves; a fetch-phase `UNAVAILABLE` still means the segment
  reads themselves are failing and stays terminal.
  Records and the objects they name are immutable, so the pinned read is
  unaffected by whatever the catalog says by then. The queryfrag wire and
  `PROTOCOL_VERSION` are unchanged, and cross-cluster federation is unchanged:
  a resolve-scope request still resolves the remote cluster's own snapshot.
- **A scrub tick's request count follows its budget, not the corpus size**
  (issue #1686, ADR-1686). Each content-tier tick used to LIST a shard's whole
  commit prefix and GET every record in it before verifying its slice. It now
  lists strictly after a start-after marker held in the per-shard cursor and
  stops once the tick's budget is filled; the rotation rolls over when the
  listing runs out. The budget is a pair of caps, one on listing entries and
  one on requests, recomputed every tick from what the walk has actually
  observed: every rotation opens with one LIST-only count of the shard's
  entries, and every later tick recounts the last two ingest hours the previous
  count met, plus anything after them, and adds their growth, so a commit from
  any writer on the shard is counted. The per-tick entry cap is the share
  needed to reach the rotation's deadline, floored at the rotation's sustained
  rate and capped at four times that rate. The deadline is the smaller of
  `--scrub-period` and half the tenant's retention window, so a rotation that
  keeps pace reaches each object by about half its retained life. When the entries per
  tick the deadline needs exceed four times the sustained rate, the tick
  increments `ravel_scrub_behind_total{signal}` and logs the entries per tick
  needed beside the number allowed, with the deadline and the period. Every
  listing page the walk draws and every record GET attempt is charged against
  the request cap; the LIST-only count passes are not. A unit where a record or
  object GET failed with a retryable error (throttled, timeout, transient) does
  not move the marker past it and counts nothing from it, so the next tick
  retries the whole unit. That hold is capped at six consecutive held ticks on
  one unit, six hours of tick cadence at the default one-hour tick (at most 6.6
  with the jitter the loop adds to every sleep): the next tick that reaches the
  unit moves past it and counts each record or object that still fails
  retryably on
  `ravel_scrub_unreadable_total{reason="retry_exhausted"}`, and the new gauge
  `ravel_scrub_marker_held_ticks{signal}` reads the current held count of the
  signal's worst shard. A record or object GET that fails with any other
  error except not-found, and a record whose bytes do not decode, is counted
  once on the new `ravel_scrub_unreadable_total{signal, level, reason}` (with
  `reason="access_denied"` or `reason="permanent"`, at the level of the object
  or record that failed), and the marker moves on, so one unreadable record
  cannot pin the rotation. Neither is counted on
  `ravel_scrub_checksum_mismatch_total`, which counts only bytes that were read
  and did not match; a new `RavelScrubUnreadable` warning alert fires on any
  increase of the unreadable counter over an hour. A compaction record that
  lands in an hour the marker has already passed is still judged against that
  whole hour, and a tick whose cursor GET fails for any reason other than `NotFound`
  is skipped with the stored cursor left alone. The cursor gains serde-default
  fields, so a cursor from an earlier release loads with defaults and its
  progress restarts: the next tick starts a fresh rotation. During a
  mixed-version rolling upgrade each version's cursor write drops the other's
  new fields, so the rotation restarts each time a shard's ownership flips
  between versions; nothing is corrupted and the cursor stays in object
  storage. A retention window whose half fits in one tick gives every tick the
  budget of a whole rotation, so every tick normally walks a full rotation and
  pays the LIST-only count of the whole commit prefix that opens it, whose cost
  is not charged against the budget.
  `ravel_scrub_cursor_position` is now a fraction of listing entries rather
  than of data objects. Every object the walk reaches and can read is still
  verified once per rotation, except that a retried unit's records and objects
  are fetched again,
  and the hour re-list that judges a late compaction record GETs that hour's
  compaction and rewrite records already consumed a second time. A record committed behind the marker waits for
  the next rotation.
- **PromQL fetches now reserve against the same process-wide memory budget
  as SQL execution** (issue #1255). `ravel-server` hands the PromQL engine
  and the startup cache warm pass the one `MemoryBudget` its SQL executor
  already uses, and with `--distributed-query` on it hands the same budget
  to the fragment service that runs metrics slices, both for remote workers
  and for the coordinator's own no-hop local path.
  An RSEG or RLOG fetch for a PromQL query that needs more than the budget's
  remainder fails with `FetchMemoryExhausted` (HTTP 503 on the PromQL API)
  instead of running unbounded, and the next query is admitted as before. A
  cache-warm fetch the budget refuses is skipped and logged rather than
  failing startup.
  The `/metrics` gauges now report real values:
  `ravel_memory_reserved_bytes{component="fetch"}` is the bytes held by live
  fetch reservations, `component="sql"` is the rest of the budget's reserved
  total rather than all of it, and `ravel_memory_handoff_overlap_bytes` is
  the part of the fetch share whose bytes went through the read cache, hit
  or miss, whether or not the cache kept them. The SQL path's own RSEG,
  RLOG and RSPAN fetchers still reserve against private unlimited budgets.
- **`ravel-server` resolves `cost-based` logs fetching on every deployment
  again, including a `--store s3` deployment against a loopback
  `--s3-endpoint`** (ADR-2023 decision 1, issue #2023). 0.18.0's loopback
  `byte-minimal` default (ADR-2014) is withdrawn: under ten concurrent
  queries on the ClickBench reference machine it cut throughput to about a
  third of `cost-based`'s (0.123 against 0.400 queries per second).
  `--logs-fetch-policy byte-minimal` remains available as
  an explicit opt-in; only the unset default changes. The startup line's
  `policy_source` now reads only `flag` or `default`; the
  `derived-loopback-endpoint` value is gone.
- **`ravel-server`'s catalog byte cache is sized independently of
  `--cache-max-bytes`** (ADR-2023, issue #2023). `--cache-max-bytes` now
  bounds the query fetcher cache only; the catalog byte cache derives its
  own share of the process memory budget (`CATALOG_CACHE_MEMORY_PERCENT`,
  5%) regardless of `--cache-max-bytes`, or takes a new
  `--catalog-cache-max-bytes <BYTES>` flag explicitly. A deployment that
  relied on `--cache-max-bytes` to grow the catalog byte cache above its
  default now also needs `--catalog-cache-max-bytes` set explicitly to get
  the same catalog cache size. The reverse also changes: a deployment that
  set `--cache-max-bytes` low to constrain memory used to get a catalog
  byte cache that small too, and now gets the catalog's own 5% share (about
  1.5 GB on a 30 GiB host) unless `--catalog-cache-max-bytes` is also set.
  Startup still refuses to start when the two caches' resolved ceilings
  together reach or exceed the process memory budget, exempting
  `--disable-cache` as before. With `--cache-dir` set, each cache's disk tier
  is bounded by that cache's own RAM ceiling, so the catalog disk tier no
  longer follows `--cache-max-bytes`.
- **`ravel-server` derives a larger fetcher-cache share on a loopback S3
  store** (ADR-2023, issue #2023). Unset, `--cache-max-bytes` used to
  always derive 25% of the process memory budget; now, a `--store s3`
  deployment whose `--s3-endpoint` is loopback (the predicate that also
  gates a plaintext endpoint) derives 40% instead, so a fetch cache holding
  the working set's whole objects serves repeated statements without going
  back to the store's disk. An
  explicit `--cache-max-bytes` always wins, and every other deployment
  keeps the 25% share. The resolved value's source (`budget-carve-loopback`)
  is logged on the `performance default resolved` startup line alongside
  `cache_max_bytes`. With `--cache-dir` set, the fetcher cache's disk tier
  grows with it, on the same disk the store reads from.

## [0.18.0] - 2026-09-26

### Changed

- **`ravel-server` defaults to `byte-minimal` logs fetching against a
  loopback S3 endpoint** (ADR-2014). Unset, `--logs-fetch-policy` used to
  always resolve to `cost-based`; now, a `--store s3` deployment whose
  `--s3-endpoint` is loopback (`localhost` or a loopback IPv4/IPv6 literal)
  resolves to `byte-minimal` instead, because on such a store the cold path
  is disk-bound rather than network-bound and the whole-object reads
  `cost-based` picks at the reference cost profile spend local disk I/O a
  ranged read would have skipped. Measured on the ClickBench reference
  machine (RustFS on loopback, 42 statements): cold wall-clock 1,720.4s to
  1,186.6s, hot wall-clock 272.3s to 88.5s. An explicit `--logs-fetch-policy`
  always wins, including an explicit `cost-based` on a loopback endpoint, and
  a non-loopback `--s3-endpoint` (or `--store memory`) sees no change. The
  resolved policy's source (`flag`, `default`, or
  `derived-loopback-endpoint`) is now logged alongside the policy on the
  `logs fetch policy resolved` startup line.

## [0.17.0] - 2026-09-25

### Fixed

- **`ravel-bench` passes `clippy -D warnings` under `--all-features`**
  (issue #1925). The `read_path_accounting` binary's `Backend` enum carried
  an unboxed `S3Config` in its S3 variant next to a data-less variant, so
  `clippy::large_enum_variant` failed `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`. The build and tests were never affected;
  only the lint gate failed. The field is now boxed, with no change in
  behavior.

### Changed

- **The remaining MinIO identifiers are renamed to RustFS** (issue #2008).
  The object store moved from MinIO to RustFS in 0.16.0 (#2002), which kept
  the old names; they now match. The `RAVEL_MINIO_*` variables that gate the
  S3 contract suite and the bench smoke tests are now `RAVEL_RUSTFS_*`, and
  the old names are no longer read, so anyone running those suites locally
  must rename them. The tests `minio_contract` and `minio_ingest_read_smoke`
  are now `rustfs_contract` and `rustfs_ingest_read_smoke`, and CI's checks
  that those tests really ran match the new names. `sql_latency_bench`
  reports the backend label `"rustfs"` instead of `"minio"`, and
  `read_path_accounting`'s `Backend::Minio` is `Backend::RustFs`. Test
  fixture hostnames and doc comments follow; where a comment stated how
  MinIO specifically behaved, it now makes a store-neutral statement
  instead.

## [0.16.1] - 2026-09-25

### Fixed

- **A release whose changelog section is too long for a GitHub release body
  now publishes with that section's entry headlines instead of failing**
  (ADR-0086, amendment 2026-09-25). The v0.16.0 tag published its images, then
  its release job failed at `gh release create` with "body is too long (maximum
  is 125000 characters)": the 0.16.0 section alone is 165,435 characters, so
  v0.16.0 has no GitHub Release. `publish-images.yml` now measures the composed
  notes against a 120,000-byte budget. Over it, the section keeps its headings
  and each entry's bold headline, followed by a link to the full section in the
  tagged `CHANGELOG.md`; the generated pull request list and the downloads text
  are unchanged. If the notes still do not fit, the job fails with both sizes
  before it creates the Release.

## [0.16.0] - 2026-09-25

### Added

- **`ravel_ingest_resource_attrs_dropped_total` counts metric resource
  attributes dropped for sitting outside the label allowlist** (issue #116).
  `build_resource_labels` turns `service.name`/`service.namespace` into `job`,
  `service.instance.id` into `instance`, and a fixed allowlist of other
  resource attributes into labels; every other attribute was silently
  dropped, with no rejection, no counter, and no partial-success detail, and
  two resources differing only in such an attribute would flatten to the same
  label set and merge into one series with no signal that it had happened.
  Attributes outside the allowlist are **still dropped**: this is visibility
  only, not a fix to the drop itself or a way to configure the allowlist
  (not configurable today). The count (not the dropped keys, which are
  caller-controlled and unbounded) is carried internally as an informational
  `Rejection::ResourceAttributesDropped` from `ravel-otlp`, rendered on
  `GET /metrics` as `ravel_ingest_resource_attrs_dropped_total` by tenant,
  for the metrics signal only (mirroring `ravel_ingest_body_conversions_total`,
  not a `reason` on `ravel_admission_rejected_total`, since it is counted
  before the series cap and the write, so not a count of stored points), and
  never reaches the OTLP partial-success response: every stock OpenTelemetry
  SDK resource carries `telemetry.sdk.*` attributes the default allowlist
  does not cover, so surfacing this to senders would flag nearly every clean
  export as partial. Covers OTLP HTTP and OTLP gRPC ingest only; OTAP builds
  no resource labels at all, so it is not covered.

- **Prometheus alert rules ship in `deploy/prometheus/ravel.rules.yaml`**
  (issue #1730). `deploy/` previously held one dashboard graphing host CPU,
  no rule file and no `PrometheusRule` manifest, so about twenty alert
  conditions lived only as prose rows in the troubleshooting guide and four
  complete rules inside fenced blocks in the observability guide. The shipped
  file carries those conditions with the thresholds and durations the guides
  state. A test parses it and asserts every metric it names appears in a
  rendered `/metrics` body, so a metric rename fails the build instead of
  leaving a rule that matches no series and pages nobody, and it compares the
  observability guide's reprinted copies against the shipped file so the two
  cannot drift.

- **`GET /api/v1/rules` serves the loaded alert rules in the Prometheus rules
  shape** (issue #1711). A process running an alert evaluator (`all` or `query`
  mode) now answers the Prometheus rules API with the rule set it parsed at
  startup, as one group per tenant named `ravel-alert-rules`, so an operator
  can confirm which rules a process is actually evaluating without reading the
  file on its host. The tenant comes from the request's credential, and a
  tenant with no rules gets an empty group list rather than anybody else's
  rules. Each rule's `query` is its whole firing expression, with a PromQL
  rule's threshold comparison appended to the query text. `health` and `state`
  are reported as `unknown` and a rule carries no `alerts` array, because the
  endpoint serves the loaded configuration and reads no evaluation outcome;
  `/api/v1/alerts` is not served.

- **A Grafana dashboard over Ravel's own metrics ships in
  `deploy/grafana/dashboards-standalone/ravel.json`** (issue #1730). `deploy/` shipped a
  rule file that pages on 37 metric names and one quickstart dashboard that
  graphs host CPU, so an operator who got paged had nothing to open. The new
  dashboard carries 36 panels in 6 rows (ingest, query, catalog fold,
  maintenance, object store and probe, alerting) over 107 distinct `ravel_`
  names, on a data-source variable rather than a hardcoded uid. The test that
  pins the rule file's names now pins the dashboard's too: it extracts metric
  names from each panel target's PromQL expression and asserts each one appears
  on a `# TYPE` line of a rendered `/metrics` body, through the same scanner and
  the same rendered bodies the rule file goes through, and it asserts that every
  name the shipped rules alert on is graphed by some panel. The check covers
  metric names; it does not validate the PromQL around them or the dashboard
  against Grafana's schema.

- **`ravel_store_probe_last_run_timestamp_seconds` gauge for the store-probe
  task's own liveness** (issue #1728). The background store-reachability
  probe (`store_probe::spawn`) runs as a single `tokio::spawn` with no restart
  path and no `JoinHandle` observation; if it dies, `ravel_store_reachable`
  and `ravel_store_probe_failures_total` freeze at their last values and
  `/readyz` reads that stale state as healthy forever. The new gauge is set
  from an injected clock at the end of every completed probe cycle, whatever
  its outcome, and once by `store_probe::spawn` before the loop's first sleep,
  so its AGE (not its value) is the signal that the task itself has stopped: a
  failing-but-alive probe keeps advancing it every cycle, and a task that dies
  before its first cycle ages out from its spawn stamp. A single shipped alert
  covers every dead-probe state:
  `RavelStoreProbeStalled` in `deploy/prometheus/ravel.rules.yaml` fires on
  `time() - ravel_store_probe_last_run_timestamp_seconds > 132` held for
  `5m`, with no sentinel guard term and no companion never-ran rule.
  `docs/guides/observability.md` documents the alert and derives its
  threshold from the probe interval, noting that it must be recomputed for a
  non-default `--store-probe-interval`.

- **`ravel-bench`'s ingest and end-to-end reports break out queue-deadline
  abandonment as its own `abandoned_queue_deadline` counter instead of
  leaving it unreported** (issue #1823). `ingest_bench` and `s3_e2e_bench`
  already reported `abandoned_retry_exhausted` and `abandoned_input_rejected`
  on the `abandoned` line of their human-readable and JSON report output, but
  a flush `ravel-ingest`'s pre-acquire queue-deadline guard abandons before
  any store call had no field in either `Report` type and was silently
  absent from bench output. Both `Report` types
  (`ravel_bench::ingest::Report`, `ravel_bench::e2e::Report`) now carry the
  field, both `run()` implementations copy it from the router's
  `IngestMetricsSnapshot`, and both bins render it on the `abandoned` line,
  pinned by `ingest_bench::tests::abandoned_queue_deadline_appears_in_rendered_report`
  and `s3_e2e_bench::tests::abandoned_queue_deadline_appears_in_rendered_report`.
  `ravel_bench::ingest::tests::queue_deadline_abandonment_is_reported_under_its_own_reason`
  and the equivalent test in `ravel_bench::e2e` assert the exact counter
  split (`abandoned_queue_deadline == 1`, `abandoned_retry_exhausted == 0`,
  `abandoned_input_rejected == 0`) for a flush whose deadline has already
  elapsed while queued for a permit.

- **A teardown drain that loses acknowledged buffered rows, or overruns
  `--shutdown-timeout`, is now visible on `/metrics`** (issue #1742).
  `ravel_ingest_flush_all_residue_tenants_total` renders the residual tenant
  count a `DrainIntent::Teardown` flush already logged at ERROR, for all
  three ingest signals; `ravel_shutdown_drain_overrun_total` counts graceful
  shutdowns that ran past their timeout, incremented only on that branch. The
  client-facing listener (which also serves `/metrics`) stops accepting new
  connections before either value can change during the shutdown that sets
  it, so a live scrape is unlikely to observe the exact event; see
  [Reachability during shutdown](docs/guides/observability.md#reachability-during-shutdown).
  The accompanying log line remains the reliable single-event signal.

- **The operator-facing record-cache figures in `docs/guides/caching.md`,
  `docs/guides/operations.md` and `docs/catalog-and-mvcc.md` are checked
  against the `ravel-catalog` constants they are derived from** (issue
  #1927). Issue #1904 let the derived per-tenant capacity change while five
  prose restatements of the capacity, the per-entry byte rate, the memory
  budget and the derived entry cap drifted out of sync with each other and
  with the shipped constants, which would have led an operator sizing a host
  from the docs to under-provision. A new test,
  `ravel-catalog`'s `operator_docs_record_cache_figures.rs`, computes each
  figure from `RECORD_CACHE_ENTRY_BYTES`, `RECORD_CACHES_PER_TENANT`,
  `MAX_RECORD_CACHE_BYTES_PER_TENANT`, `DEFAULT_CACHE_CAPACITY_PER_TENANT`
  and `MAX_CACHE_CAPACITY_PER_TENANT` and asserts the three guides state
  exactly that value, so the next constant change fails in this file instead
  of shipping stale prose. Figures the guides state more than once are
  pinned by an occurrence count, so a restatement in different words cannot
  drift unpinned.

- **A cargo-free guard checks that every operator-facing record-cache figure
  matches the constants it is derived from** (issue #1927).
  `scripts/guards/check-doc-figures.sh` re-derives each figure from
  `RECORD_CACHE_ENTRY_BYTES`, `RECORD_CACHES_PER_TENANT`,
  `MAX_RECORD_CACHE_BYTES_PER_TENANT` and
  `DEFAULT_CACHE_CAPACITY_PER_TENANT`, then counts every occurrence in the
  three guides and in `--disable-cache`'s long help, so a restatement that
  drifts, or one nobody accounted for, fails in under a second rather than in
  CI. The cargo tests that pin the same figures still run in CI; this is the
  half that runs where cargo does not.

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

- **Per-part (v3) column-statistics objects** (issue #1482, ADR-1413; the
  whole-snapshot v1 and v2 statistics they were first written alongside are
  retired later in this release, see the #1600 entry). The fold now writes one
  `.cstat` object per newly-written snapshot part, referenced by an additive
  field 7 (`column_stats`) on `SnapshotPartRef`. A part whose statistics would
  exceed `DEFAULT_MAX_COLUMN_STATS_BYTES` (256 MiB, the same ceiling the v3
  reader already enforces) degrades rather than stalls the fold: the largest
  remaining dictionary is dropped and the part re-measured, repeating until it
  fits, clearing only `dictionary_present`/`dictionary` and keeping
  min/max/count/sum exact; the fold fails a part only once no dictionary is left
  to drop and it is still over the ceiling.
  `FoldReport::column_stats_dictionaries_dropped` reports how many dictionaries
  a fold cleared this way. Objects are keyed by the content hash of their own
  bytes rather than the part's hash, so two folds that recompute a
  byte-identical part but hit different segment-fetch outcomes cannot collide on
  a key naming bytes that were never stored there; the v3 object is built and
  bound-checked before the part's own `.csnap` is written, so a refused part
  leaves no orphaned `.csnap` behind. The degrade loop tracks each dropped
  dictionary's exact byte contribution instead of re-measuring the whole part
  per drop, so a part needing tens of thousands of drops still finishes. An
  incremental fold also skips the v3 baseline fetch for any old part it is not
  genuinely re-deriving, cutting one object GET per untouched sealed part on
  every incremental fold. `ravel-maintain`'s unreferenced-object sweep now
  carries every part's field-7 key into its referenced-key set; without that, a
  v3 object outlived only by the sealed part naming it would have crossed the
  protection horizon and been swept from under it.

- **`ravel-cli catalog fold --json`, and full column-statistics visibility in
  `catalog fold` and `catalog inspect`** (issue #1598). `catalog fold`'s human
  report used to print only 10 of `FoldReport`'s then-23 fields, silently
  dropping counters such as `column_stats_dictionaries_dropped`; it now renders
  every field, and `--json` emits the whole struct as a JSON document (the store
  selection, `signal`, and `seal_margin` ride along as extra top-level keys
  rather than being lost). `catalog inspect` prints each column-statistics
  reference (the per-part field 7) as `key=... size=...`, or an explicit
  `ABSENT` marker, so an omitted line and an unset field are no longer
  indistinguishable. A new `ravel-cli inspect cstat <key>` decodes a `.cstat`
  object's envelope and header without decompressing its body, so an object
  whose declared uncompressed length exceeds the decode ceiling still yields
  every header field and an over-ceiling verdict instead of an error; under the
  ceiling it goes on to list each column's `dictionary_present`.
  `FoldReport::put_requests` previously undercounted: three of the six fold PUT
  call sites only incremented on success or `AlreadyExists`, so a store-side
  error on an otherwise-issued PUT went uncounted even though the object may
  have been durably written as an orphan; all six sites now increment
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

- **A structured OTLP log body (an array or a map) is now stored instead of
  being rejected, and every admission-layer rejection reason is now counted**
  (issues #1308, #1309). Normalization is admission layer 3, but its
  decisions were invisible to operators: a delta-temporality metric or a
  too-old log record was reported to the sender through OTLP partial success
  and then dropped with no counter movement, and on the OTAP surface a fully
  rejected batch left no trace at all beyond the missing points.
  `ravel_admission_rejected_total`'s reason label now classifies every
  rejection as skew or structural, so a tenant switching transport between
  OTLP and OTAP sees the same figures. Separately, a structured log body
  converts to canonical JSON text (map keys ordered by the canonical
  attribute ordering already used for stream identity, duplicate keys kept
  and ordered by encoded value, array order preserved, bytes as lowercase
  hex, non-finite doubles as `"NaN"`, `"+Inf"`, or `"-Inf"`) rather than
  being rejected outright; two exports of the same body always produce
  byte-identical stored text. A string-table-reference body is still
  rejected, since no string table travels with the export request and the
  referenced text is not reachable. Delta-temporality metrics are still
  rejected too: converting them needs a running total held between
  requests, which a disposable compute process cannot keep, so a
  collector-side `deltatocumulative` processor ahead of `batch` is the
  supported path, and the quickstart collector config now ships that way.
  Two review fixes landed alongside the conversion: a log record that was
  stored with an oversized attribute dropped, but no whole record rejected,
  previously reported nothing on the partial-success response, reading as a
  fully clean write; it now reports the drop even though the rejected-record
  count is correctly zero. And a request whose structured body was large
  enough to make conversion itself costly was, before conversion ran under a
  running byte budget, measured re-encoding a comparator on every duplicate
  key it compared; a body built to maximize that cost previously cost
  about 16 seconds of one core and built a 44 MB `String` for a 412 KB
  gzip request the size limit was about to reject anyway.

- **PromQL binary operators (`+`, `-`, `*`, `/`, comparisons) now work
  between two native-histogram series, matching the exact operator set
  Prometheus supports rather than refusing every histogram pairing with an
  error** (issue #1700). Arithmetic between differently-shaped histograms
  needed reconciliation this crate did not have: two operands at different
  exponential scales were combined bucket-index-to-bucket-index without
  first aligning scale, silently merging unrelated value ranges, and two
  custom-bucket histograms with different bounds, or two exponential
  histograms with different zero thresholds, were treated as unalignable
  and dropped, when Prometheus actually reconciles both onto a common
  layout before combining. Both gaps are now closed: a scale mismatch is
  down-converted to the coarser scale before merging, mismatched
  custom-bucket bounds are re-bucketed onto their intersection, and
  mismatched zero thresholds widen to the larger one, folding the buckets
  it swallows into the zero count, matching Prometheus' own reconciliation.
  A genuinely unalignable pair, one exponential and one custom-buckets
  operand, still drops the sample, now carrying the same warning text
  Prometheus' own evaluator emits rather than Ravel-authored wording. Two
  further correctness bugs surfaced during that work: a merge on the
  equal-threshold fast path, the common case, was deleting any bucket that
  merely straddled the zero threshold instead of keeping it, so a
  histogram added to itself could answer with no buckets at all and
  silently lose its count; and a zero threshold recorded as NaN sent the
  reconciliation routine into a comparison that never terminates, hanging
  the query thread until the process restarted. Both are fixed, and the
  NaN case is now also refused before it can reach storage: an exponential
  histogram's `zero_threshold` that is NaN, infinite, or negative is
  rejected at normalization on both the OTLP and Remote Write surfaces,
  through one shared predicate, closing the root cause rather than only
  the query-side symptom.

- **The in-flight-flush and flush-permit-wait gauges are now rendered for
  every ingest signal, not only metrics** (issue #1741). The log and span
  ingest pipelines moved their flush-permit acquire off the shard actor
  alongside the metrics pipeline, but the gauges that make a stalled permit
  wait visible, `ravel_ingest_in_flight_flushes` and the new
  `ravel_ingest_flush_permit_wait_seconds_total`, were rendered only inside
  a metrics-only code path. A logs-only or spans-only process therefore
  rendered no sample for either gauge at all, even though a real (and
  possibly zero) value existed for it. Both are now flat fields rendered
  for every `{mode, signal}` combination.

- **A process-wide memory budget now bounds SQL execution, with three new
  `/metrics` gauges** (issues #1170, #1254). A SQL statement's pooled
  reservation draws on one process-wide `MemoryBudget`. The fetch layer can
  reserve against the same budget (every RSEG `ensure_ranges` coalesced read,
  every RLOG block-range and whole-object fetch, and every RSPAN whole-object
  fetch reserves the bytes its GET will materialize before issuing it), but no
  shipped binary hands the fetchers that budget yet, so fetch buffers are not
  counted against it and `component="fetch"` reads 0 (see the #1255 entry).
  Where a fetcher is given the budget, a fetch-side refusal fails typed as
  `FetchMemoryExhausted { requested, reserved, limit }`, mapped to the frozen
  gRPC `BudgetExceeded` code rather than `Unavailable`: `Unavailable` is this
  codebase's re-dispatch-and-run-locally class, so mapping a budget refusal to
  it re-dispatched the refused slice to another worker and then ran it on the
  coordinator, amplifying the load the budget exists to shed.
  `ravel_memory_budget_bytes` (the resolved ceiling, `u64::MAX` meaning
  unlimited), `ravel_memory_reserved_bytes` by `{component="sql"|"fetch"}`, and
  `ravel_memory_handoff_overlap_bytes` are now on `/metrics`. Two startup
  defects in the derived budget were closed alongside this: a host where memory
  could not be measured (any non-Linux host) used to derive a budget of `0`
  instead of unlimited, and a `--cache-max-bytes` value that, with the derived
  catalog cache, landed at or above the derived budget used to be accepted
  rather than refused; both used to leave `MemoryBudget::new(0)` in place, which
  refuses every real SQL or fetch reservation while a statement that reserves
  nothing (`SELECT 1`) kept answering.

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
  signal, the typed attribute column set the query resolved, and the
  erasure
  predicates pending in the snapshot it read, each read off the successful
  attempt's own snapshot so a retry can't substitute another attempt's
  values. As of this release nothing in `ravel-server` or `ravel-mcp` calls
  `page_plan` yet; it is tested directly and awaits its caller in a later
  wave.

- **A native MCP (Model Context Protocol) adapter is available behind the
  off-by-default `mcp` build feature and a `--mcp` runtime flag, exposing its
  nine-tool catalog over `POST /mcp`** (issue #1379, ADR-1374 decision 9). The
  new `ravel-mcp` crate ships `ravel_capabilities`, `ravel_describe_data`,
  `ravel_find_labels`, `ravel_explain_query`, `ravel_query_sql`,
  `ravel_query_promql`, `ravel_search_logs`, `ravel_get_trace` and
  `ravel_analyze_timeseries` by name. Only `ravel_capabilities` has a body in
  this release: the other eight answer every call with a typed `NotShipped`
  protocol error naming the tool, and `ravel_capabilities` reports which tools
  are served apart from the full catalog. The `mcp` feature implies `sql`, since
  four of the nine tools are built to execute SQL through the query service.
  Every tool response is bounded before it reaches the wire: cursors are opaque,
  MAC'd, self-describing tokens bound to the call that minted them, envelope
  cells are sized by their serialized length rather than by field count, and a
  compact text rendering is capped at 20 rows and 64 KiB with every caller
  string escaped and control characters stripped. `ravel-sql` gained a
  `pin-codec` feature so the cursor codec can reuse `FlightTicket`, `TicketKey`
  and `SegmentPin`'s keyed-MAC pattern without linking Arrow Flight.

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

- **`ravel_maintain_l0_records_pending` and
  `ravel_maintain_objects_deleted_total` render on `/metrics`** (issue #1729).
  The compaction scan and the retention sweep previously reported these figures
  to tracing only, so an operator could not see how much L0 compaction work was
  queued, or how many objects maintenance had actually deleted, without reading
  logs on every process. `ravel_maintain_l0_records_pending` is now published by
  `signal`, summed across every bucket this process owns and republished once
  per maintenance cycle (default 300 seconds) after the cycle has covered all of
  them, so a mid-cycle scrape reads the previous cycle's complete total rather
  than a partial sum; a bucket the cadence memo skipped for being safely below
  threshold still contributes its last-known record count, so the buckets an
  operator most needs to watch cannot silently drop out of the total.
  `ravel_maintain_objects_deleted_total` is published by `kind`, one series for
  each of the four `SweepReport` counts that record a physical delete (including
  `kind="quarantine_reaped"`; a move to quarantine and a withheld candidate are
  not deletes and are not counted). The troubleshooting guide documents both,
  and says that a dip in the pending gauge is not corroborated by
  `ravel_maintain_units_stalled`: that gauge only moves for a per-unit failure
  repeated past its stall threshold, and the paths that remove the most records
  from the pending total (a tenant skipped whole-tick for a failed legal-hold
  refresh, or by the provisioning or shard-generation check) never reach
  per-unit accounting, so `units_stalled` can sit at zero while the pending
  population moves for an unrelated reason.

- **The process memory budget now exposes gauges on `/metrics`, and startup
  refuses a container the budget cannot fit** (issues #1255, #1395).
  `ravel_memory_budget_bytes` renders the resolved ceiling (`u64::MAX` meaning
  unlimited), `ravel_memory_reserved_bytes` by component (`sql`, `fetch`), and
  `ravel_memory_handoff_overlap_bytes`; `component="fetch"` renders 0 for now,
  since nothing yet charges the fetch layer against this budget. The budget
  itself is now derived from the cgroup-effective memory ceiling minus a fixed
  overhead reserve, not raw `MemTotal`, so a container capped below the host's
  total memory is sized correctly rather than against memory it can never use.
  A process whose derived budget cannot cover its resolved cache ceilings now
  refuses to start with a typed `MemoryBudgetExceeded` error naming the
  shortfall, instead of starting and letting the caches or the SQL executor
  exceed the container's real limit later.

- **`--disable-cache` now passes the memory budget startup check** (issue
  #1436). The check previously compared the fetcher and catalog cache ceilings
  against the derived budget without accounting for `--disable-cache`, so a
  host with cache limits set above the budget was refused even though it
  builds no cache at all, and any container whose effective memory sat at or
  below the overhead reserve derived a 0 budget that no flag value could
  satisfy, including `--disable-cache` itself. The check now recognizes that
  `--disable-cache` builds neither the fetcher cache nor the catalog byte
  cache, so the full budget is available to the shared SQL and fetch
  accounting and the container starts.

- **A new MCP (Model Context Protocol) surface can be mounted on the
  query-serving listeners, behind its own feature and flag** (issue #1381).
  `--mcp` opts a build carrying the `mcp` cargo feature into serving `POST
  /mcp`; `--mcp-allowed-origins` is a mandatory origin allowlist (an empty list
  is accepted only on a loopback listener, and fails startup on any other
  address); `--mcp-max-body-bytes` caps the request body (default 1 MiB) and
  refuses a value of 0. The route runs the same tenant resolution, origin check,
  and body cap as the HTTP query surfaces before anything reaches the protocol
  layer, and each tool call is billed through the same admission permit,
  deadline clamp, cost record, usage guard, audit submission, partial-result
  gate, and error redaction as an HTTP query. Starting the process with `--mcp`
  under `--mode gateway` or `--mode maintain` now fails at startup, naming the
  flag and the mode, instead of silently mounting nothing. `ravel_capabilities`
  reports which tools are actually served (`tools.enabled`) separately from the
  full catalog (`tools.catalogued`). A `finish` response now names
  `visibility.snapshot_id`, `visibility.watermark_hour`, `ids.query_id`, or
  `ids.audit_ref` in its `warnings` list when the underlying operation left that
  field unmeasured, rather than rendering an empty string a caller cannot
  distinguish from a genuinely empty value. A malformed MCP budget argument is
  now refused rather than silently defaulted, and `row_cap_hit` together with
  the produced row count now survive through to `finish` instead of being
  dropped along the way.

- **A `--max-ingest-lag` flag replaces the hardcoded 2h ingest admission
  bound** (issue #1682). The value drives both the catalog listing window and
  the OTLP, OTAP, Remote Write, and span-surface admission bounds together, so
  the two can never be set inconsistently: the listing window widens first and
  the admission bound is derived from it. Startup validates the configured
  window against the ADR-0019 retention floor and refuses a lag that would
  outrun retention, and refuses a bound that admits data wider than the
  catalog can list. This lets a deployment replay telemetry older than 2h
  after an outage or a bulk import, which the previous hardcoded bound
  rejected outright with `Rejection::TooOld` on every ingest surface.

- **A `--tenant-token-file` flag loads static bearer tokens from a file
  instead of the command line** (issue #1706). The file source strips a
  leading UTF-8 byte-order mark before parsing, and an empty or comment-only
  file parses to an empty map with no startup error. In the same change, a
  malformed token line (missing `=`, or an empty tenant after the split) no
  longer echoes the offending pair back into the error message: for the file
  source that text is the bearer token itself, and the previous error text
  printed the secret straight into the process's stderr log on the most likely
  operator mistake, a Secret mounted with the token alone and no `=TENANT`.

- **The operator now detects the cluster's Kubernetes minor version and gates
  the preStop `SleepAction` on it** (issue #1714). Kubernetes reports
  `PodLifecycleSleepAction` (KEP-3960) as beta and enabled by default from
  1.30, not GA from 1.32 as first assumed; the floor is now minor version 30.
  A cluster below the floor gets `KubernetesVersionUnsupported` and no preStop
  hook rendered, rather than a field the API server silently drops or rejects.
  An unreadable version check (a transient API error) fails open and logs once
  rather than flapping the condition.

- **A separate admission class bounds federated fragment resolves** (issue
  #1722). `--max-inflight-federated-resolves` (default 8) caps this new
  "Resolve" class independently of the existing `--max-inflight-fragments`
  (default 32), which continues to bound the "Pinned" class alone, so a burst
  of federated resolves can no longer starve pinned fragment reads of their
  own permits.

- **The Kubernetes operator now renders default CPU and memory requests when a
  `RavelCluster` spec omits them** (issue #1726). Gateway and maintain pods
  default to 100m CPU and 256Mi memory; query pods default to 200m CPU and
  512Mi memory. A spec that sets its own requests is unaffected.

- **A new CRD field, `spec.gateway.maxInflightFlushes`, renders
  `--max-inflight-flushes` on the gateway Deployment** (issue #1743). This is
  the per-shard cross-tenant flush isolation bound: without it, an
  operator-managed cluster was stuck at the server's compiled-in default of 1,
  so one tenant's stalled flush could block every co-resident tenant's flush on
  that shard. The field is gateway-only, since ingest and its flush isolation
  only run under `--mode gateway`; when unset, nothing is rendered and the
  cluster keeps the server's own default. A value of 0 is refused, matching the
  server's own refusal of `--max-inflight-flushes 0` as a flush deadlock.

- **New maintenance-safety and admission-reconciliation counters and gauges
  are exported on `/metrics`** (issue #1762).
  `ravel_maintain_orphans_quarantined_total`,
  `ravel_maintain_orphans_quarantine_refused_total`, and
  `ravel_maintain_quarantine_reaped_total` (all labelled by mode and signal)
  report what each sweep pass did to orphaned data, figures the sweep already
  computed but the exporter did not read.
  `ravel_admission_reconciliation_cycle_duration_seconds`,
  `ravel_admission_reconciliation_siblings_observed`, and
  `ravel_admission_reconciliation_stale_keys_skipped` (labelled by mode alone,
  since one cycle reconciles every tenant the process tracks) report the last
  completed reconciliation cycle and can fall between scrapes;
  `ravel_admission_reconciliation_keys_reaped_total` accumulates across cycles
  instead.

- **Alerting pipeline metrics are exported on `/metrics`** (issue #532). Six
  cumulative quantities (rules evaluated, rules failed, records written,
  repeats queued, notifications delivered, notifications failed) become
  counters labelled by mode, and the three mutually-exclusive per-tick
  outcomes (history unavailable, lease not held, lease unavailable) collapse
  into one `ravel_alert_ticks_total` counter split by outcome alongside the
  evaluated case. `ravel_alert_last_tick_completed_timestamp_seconds` stamps
  on every tick, including one that skipped evaluation because a peer holds
  the lease, so only its age signals a stalled loop. The whole family is
  omitted unless this process built an evaluator. Because the evaluator spawns
  one task per tenant but every task stamps the same process-global gauge, a
  dead evaluator for one tenant stays hidden as long as any other tenant on
  the process keeps ticking; this is a process-wide signal, not a per-tenant
  one.

### Changed

- **The quickstart and CI object store moved from MinIO to RustFS, and the
  `mc` client to the AWS CLI** (issue #2001). MinIO withdrew anonymous access
  to its public images on 2026-09-24, from Docker Hub and quay.io alike, so
  every compose file, Kubernetes manifest and CI job that started MinIO failed
  at `docker run` with `unauthorized`, and a mirror is no fix because the
  images cannot be pulled without credentials at all. The object store is now
  `ghcr.io/rustfs/rustfs:1.0.0`, the S3 client is
  `public.ecr.aws/aws-cli/aws-cli:2.37.2`, and both stay pinned by digest;
  neither registry shares Docker Hub's per-IP anonymous pull allowance, though
  ECR Public throttles unauthenticated pulls per source IP on its own, so the
  CI steps that pull the client retry a throttled pull with backoff. An
  operator running the old quickstart compose file should stop the stack
  (`docker compose -f deploy/docker-compose/ravel.yml down`), pull the current
  files, and bring the stack back up. RustFS starts from an empty
  `rustfs-data/`, and nothing here establishes that it can read a MinIO data
  directory, so treat `minio-data/` as local development data to delete rather
  than to rename; the store-qualify one-shot qualifies the fresh bucket. The
  endpoint, bucket name and development credentials are unchanged, so nothing
  else in a local configuration moves. `make minio` and `make minio-down` are
  now `make rustfs` and `make rustfs-down`, `deploy/docker-compose/minio.yml` is
  `deploy/docker-compose/rustfs.yml`, `deploy/k8s/minio.yaml` is
  `deploy/k8s/rustfs.yaml`, and `RAVEL_FAKE_S3_BACKEND=minio` is
  `RAVEL_FAKE_S3_BACKEND=rustfs`. The `RAVEL_MINIO_*` variables that gate the
  object-store contract test, and the `minio_contract` test name itself, keep
  their names: they name the gate rather than the vendor, and every checkout
  and lane that sets them would otherwise break.

- **The shipped admission defaults are now one value, `ravel-ingest`'s
  `AdmissionLimits::default()`, instead of two that had drifted** (issue #23).
  **No deployment's effective limits move.** A shipped `ravel-server` already
  applied 200,000 for `max_active_series` and `max_active_streams`, and it
  still does; what changed is where that number lives. `ravel-server`'s
  `config::limits::shipped_defaults` built its own `AdmissionLimits` literal
  and never called the `Default` impl, so `ravel-ingest` kept 1,000,000 for
  every other caller with nothing failing when the two disagreed. The library
  constants are now 200,000 and the server returns them, so an embedder
  building an `AdmissionController` from `AdmissionLimits::default()` gets the
  caps the server ships rather than a 5x looser pair. A test asserts the two
  are equal field by field.

- **ADR-0051 section 2's per-entry memory estimate is corrected from about 16
  bytes to the measured 35 to 56 bytes** (issue #22). The worst case also
  multiplies by the two tracked signals, not just the two rotating epochs:
  `cap x bytes-per-entry x 2 epochs x 2 signals`. At the 1,000,000 the ADR
  proposed that is 140,000,000 to 224,000,000 bytes (134 to 214 MiB) per fully
  active tenant, 4.4x to 7x what the original figure implied; at the 200,000
  that ships it is 28,000,000 to 44,800,000 bytes (27 to 43 MiB). The ADR's section 3
  defaults table now carries the shipped figure with the proposed one beside
  it. Documentation only, no behavior change.

- **The catalog resolve path's request ceiling now derives from the server's
  query concurrency instead of sitting at a flat 128** (issue #1733, ADR-1733).
  Only the unset default moves: an explicit `--catalog-resolve-concurrency` is
  still used verbatim, and `0` or a value above 4,096 is still a startup
  refusal. Unset, the ceiling resolves to `clamp(Q * 128, 128, 4096)`, where
  `Q` is `--max-concurrent-queries` when that flag bounds queries and the
  derived `max(8, 2 x cores)` when it does not. So a server run with
  `--max-concurrent-queries 1` keeps 128, one run with
  `--max-concurrent-queries 4` gets 512, and an unbounded 8-core host
  (`Q` = 16, deriving 2,048) gets 1,024. The resolved number is logged at
  startup beside the other derived performance defaults, with the `Q` and the
  input it came from. A second bound, per key prefix and with no flag, holds
  any single prefix to 128 requests whatever the ceiling is, so a higher
  ceiling buys concurrency across prefixes and never more pressure on one.
  Every resolve-path request is bounded that way, keyed by its own key
  prefix: a commit record by its shard-hour prefix, a snapshot's parts by the
  one directory they share, its postings and column stats by theirs, and a
  LIST by the prefix it lists. A prefix's semaphore is created on the first
  request that needs it and removed once no request holds or waits on it,
  including when the last requests on that prefix finish at the same moment,
  so a long-running process holds one entry per prefix in flight rather than
  one per prefix the bucket has ever had. The 1,024 is an interim cap: the ADR bounds
  in-flight resolve memory by reserving each request's listed size against
  the ADR-1170 process budget, that reservation is not wired up yet, and
  until it is a derived ceiling is held at 1,024 rather than allowed to reach
  the 4,096 the clamp permits.

- **A shard now refuses a flush trigger once `--max-queued-flushes` (default
  8) flush tasks are spawned and unacked, leaving the rows buffered for the
  next tick** (issue #1740). Before this, every trigger spawned a task, so a
  shard whose writes were slow kept spawning while each task held its built
  batch resident, with the worst case set by how long the object store stayed
  slow rather than by anything configured. `/metrics` reports it with two new
  families, both by `{mode, signal}`: `ravel_ingest_queued_flushes`, the
  spawned-and-unreaped depth summed across shards, and
  `ravel_ingest_flush_trigger_deferred_total`, the refusals that bound it.
  Two things to know: a
  flush that crosses the per-tenant memory backstop is **exempt** and spawns
  even at the cap, because a bounded queue of tasks is worth less than a
  bounded buffer, so the queue can exceed the cap and under
  `--max-ingest-buffer-bytes 0` only the length of a store stall bounds the
  overshoot; and an `--max-inflight-flushes` above `--max-queued-flushes`
  **raises the effective cap to match**, logging a warning that names both
  numbers, rather than refusing to start. Only a spawned task can hold a
  permit, so the cap has to be at least the permit count; raising it there
  keeps a cluster running `spec.gateway.maxInflightFlushes` above 8 starting
  on upgrade, which a refusal would have crash-looped with no field on the
  `RavelCluster` CRD able to raise the cap in response.

  A third thing to know: a deferred flush pins the ingest hour it eventually
  opens in, not the one its refused trigger fired in, so a long deferral moves
  which ingest hour the rows land in. Past two hours that is more than the
  read side's scan slack covers, and a straggler deferred at the cap while a
  `shard_count` decrease is activating can land in an hour the retiring
  generation no longer scans. Watch
  `ravel_ingest_flush_trigger_deferred_total`: a nonzero rate is the signal,
  and it means the object store is the thing to look at. Pinning the hour
  before the deferral instead was tried and reverted, because it moves the
  same overrun onto the catalog's sealed-hour watermark, where a late record
  is never read again rather than missed by one generation; `docs/ingest.md`
  and ADR-1642 carry the arithmetic. Bounding the deferral itself is issue
  #1916.
- **The at-rest scrub corpus now covers compaction and rewrite output parts,
  not only original L0 segments, and `ravel_scrub_checksum_mismatch_total`
  carries a new `level` label** (issue #1686). The corpus previously skipped
  every compaction and rewrite record it listed, so a bit flip in an L1 or
  rewrite part was never checksummed; a live compaction or rewrite record's
  parts now join the same rotation an L0 segment does. The lineage filter
  applies to the parts only, and leaves out three shapes: a compaction or
  rewrite record another rewrite record names in `superseded_record_key`, a
  compaction record that loses its bucket's overlap component to another
  compaction record (the state a compactor race leaves behind, resolved through
  the same selection the snapshot resolver, the index fold and the sweep use),
  and either kind in a bucket a retention tombstone has retired. No query reads
  a superseded or tombstoned record's parts, and an overlap loser's parts are
  read by no node that has adopted the overlap rule, so no operator is paged on
  rot in bytes nothing serves. L0 commit records carry no supersession, overlap
  or tombstone check and are scrubbed whatever their lineage, so a `level="l0"`
  mismatch on an hour a live compaction has already folded may name a redundant
  copy rather than data a query can still reach. The mismatch counter now
  carries `level="l0"`, `level="l1"`, or `level="rewrite"` instead of one
  undifferentiated series per signal; a dashboard or alert rule that sums
  over `signal` alone still sees the same total, but one that names the
  metric without also grouping by `level` now gets three series back instead
  of one. Neither the tick cadence nor the rotation period changes: the
  per-tick byte budget is `total_corpus_bytes * tick / period`, so it scales
  with the corpus and a full rotation still completes in about the configured
  `--scrub-period`. What rises is the scrubber's steady-state read cost,
  approximately in proportion to the share of the shard's bytes that now sit
  in compaction or rewrite parts. On a fully compacted shard, whose compaction
  output parts hold roughly as many bytes as the L0 segments they folded and
  which are still listed alongside them, that is close to a doubling of scrub
  GET bytes per tick. Size scrub read bandwidth against the corpus with parts
  included, not against the L0 total.

  of one. The scrub tick cadence is unchanged: an operator should expect the
  first tick after upgrading to cover a larger corpus within the same
  per-tick byte budget, which can extend how long a full rotation takes on a
  bucket with many compacted or rewritten hours.
- **A distributed query coordinator now decodes a slice incrementally under a
  frame cap and a byte cap instead of draining the whole stream into memory
  first** (issue #1687). Both fetch clients, intra-cluster and federated,
  buffered every response frame a remote sent and only then decoded them, so
  the remote decided how much the coordinator held. Each slice is now capped at
  1048576 response frames and at 230331648 wire bytes, both checked before a
  frame is decoded, and the client stops pulling at the first breach, which
  cancels the RPC. The byte ceiling is derived from the sample budget rather
  than picked as a round number: it is `DEFAULT_MAX_SAMPLES` (10000000) times
  the widest wire cost of one scalar sample (18 bytes), plus the frame cap
  times 48 bytes of per-frame framing as headroom, so a slice of plain scalar
  runs carrying the whole sample budget, every sample at its widest encoding,
  encodes inside it. The ceiling is reachable and is meant to be: per-sample
  provenance columns, long labels, and native-histogram frames all cost more
  than that derivation counts, and only a federated (resolve-scope) slice is
  bounded at `max_samples` by the worker itself, while an intra-cluster slice
  has no per-slice sample limit at all. A slice that does cross it is refused
  as a budget error naming both figures. The ceiling is fixed: it applies with
  no configuration at all, and nothing raises or lowers it. In particular it
  is independent of `max_bytes_scanned`, which budgets the compressed store
  bytes a slice reads rather than the uncompressed bytes it sends back. Both
  caps are per slice, and what multiplies them depends on the path: a local
  fan-out runs up to `promql_fetch_fanout` times `max_parallel_slices`
  decoders at once (64 at the defaults), while a federated query runs one per
  remote cluster and `max_parallel_slices` does not bound it. A breach is a
  refusal rather than an outage: HTTP 422 naming the observed figure and the
  cap, in the same class a local budget trip uses, not the redacted 503 other
  slice failures become. A refusal fails
  the query and so carries no stats block; the wire bytes a slice made the
  coordinator accept are reported as `wireBytesConsumed` in `stats.fragments[]`
  on the slices that completed.

  1048576 response frames and at the coordinator's own `max_bytes_scanned` in
  wire bytes, both checked before a frame is decoded, and the client stops
  pulling at the first breach, which cancels the RPC. A breach is a refusal
  rather than an outage: HTTP 422 naming the observed count and the cap, in the
  same class a local budget trip uses, not the redacted 503 other slice
  failures become. The wire bytes a slice made the coordinator accept are
  reported as `wireBytesConsumed` in `stats.fragments[]`.
- **The catalog's per-tenant commit-record cache capacity is now derived from
  the shard count and the configured max flush delay, not a flat 10,000-entry
  constant** (issue #1735). The new `ravel_catalog::derive_cache_capacity_per_tenant`
  computes `shards * ceil(3600 / flush_secs) * 3`, floored at the old
  constant; `build_catalog` calls it with the server's resolved
  `--max-flush-delay` instead of the flat default. At the shipped defaults (4
  shards, a 2-second max flush delay) this holds 21,600 entries, about 16 MB
  per actively-queried tenant at roughly 750 bytes per cached record, up from
  the old flat bound. No new CLI flag is added; the capacity is a function of
  existing ingest configuration. A repository guard now fails a pull request
  that touches `crates/` or `services/` and carries no changelog entry, which
  is why this internal sizing change carries one.
- **The published PromQL conformance figure now says what it measures**
  (issue #1698). The committed table read `132/132 = 100%` under a heading
  that invites it to be read as agreement with Prometheus, while the block is
  regenerated with no Prometheus in the loop: the number counted constructs
  Ravel *reaches*. The row is now split. `reached` keeps the old meaning and
  the old number; `agreed with Prometheus` is scored only over the constructs
  a differential run actually compared, reads `not measured in this run` when
  no run report was folded in, and reports `not compared` and
  `accepted divergence` as their own counts rather than folding either into
  the ratio. An ADR-accepted divergence is never counted as a match: a
  construct whose every entry is one is counted on its own line, and a
  construct that mixes them with ordinary entries is scored on the ordinary
  ones and names the rest in its evidence column. The difftest lane now
  writes the report the agreed row reads, so
  the figure can move at all.
- **`--s3-endpoint` now decides whether plaintext is allowed, and a
  non-loopback `http://` endpoint is refused at startup** (issue #1707).
  `allow_http` was true whenever any endpoint was set, so a deployment
  pointing at an `https` endpoint still permitted a downgrade, and a plaintext
  endpoint naming a host on the network moved credentials and telemetry in
  clear with nothing refusing it. The flag now follows the URL scheme. An
  endpoint carrying no scheme at all enables no plaintext, and issue #1911
  below refuses it at startup rather than letting it reach the S3 client. A
  non-loopback
  `http://` endpoint needs `--s3-allow-http` (env `RAVEL_S3_ALLOW_HTTP`), and
  the refusal names the flag; loopback `http` is unchanged, which is what the
  dev compose stack, kind, and the tests use. `ravel-cli` applies the same rule
  from the same function rather than a second copy of it, with the same
  `--s3-allow-http` flag and `RAVEL_S3_ALLOW_HTTP` variable: it ships in the
  server image and reaches the same bucket with the same credentials, and the
  operator's store-qualification Job runs it before any server pod exists. The
  operator gains `spec.storage.s3.allowHttp` for an in-cluster MinIO,
  rendering the flag on every server container and `RAVEL_S3_ALLOW_HTTP=true`
  on the qualify Job. The operator applies the rule from the same function at
  its own two remaining sites: it refuses a plaintext non-loopback endpoint at
  render time, with a `Degraded` condition whose reason is
  `PlaintextS3Endpoint` and whose message names
  `spec.storage.s3.allowHttp`, rather than creating Deployments
  that crashloop with the refusal only in their pod logs; and its own S3
  client, the one that reconciles `sys/auth` and applies `shardOverrides` with
  the cluster's credentials, no longer derives plaintext from endpoint presence
  either. Editing `allowHttp` re-runs store qualification, so the remediation
  is re-checked instead of skipped. **On upgrade**, a deployment already
  pointing at a plaintext non-loopback endpoint will not start until the flag
  or the environment variable is set, a `RavelCluster` in that state is refused
  with the field named in its status, and a `ravel-cli` invocation against one
  is refused the same way.
- **`ravel-cli load` reports a `--skip-rows` value past the end of the file
  instead of succeeding quietly** (issue #1713). The value was clamped to the
  file's row count and the run exited 0 having written nothing, and the
  summary printed the clamped figure, so a resume script reading the exit code
  recorded the load as done and a human reading the output could not see that
  the requested offset had missed the file. A request strictly larger than the
  row count now prints a warning naming both numbers. A request equal to the
  row count is the legitimate resume of an already-complete file and stays
  silent.
- **A distributed query now sends each slice the full byte budget instead of
  an even share, and a worker clamps every wire budget to its own
  `EngineConfig`** (issues #1725 and #1687). The per-slice share failed a query
  that was well under its total budget whenever one slice scanned more than an
  even fraction, which is the normal shape of a skewed fan-out, and the
  residual surfaced as a retryable 503. The coordinator still enforces the
  total on the folded figures, so the worst case is a bounded over-scan before
  it refuses. A cap refusal now renders as HTTP 422 wherever it comes from,
  including across clusters; only a fan-out failure or a worker out of fetch
  memory keeps the retryable 503. On the worker side a wire budget of `0` or
  absent now means the worker's own limit rather than unlimited, and the
  federation `Resolve` path enforces the worker's `max_series` and
  `max_samples` too.
- **The catalog's per-tenant record cache capacity is now derived from the
  shard count, the signal count and the configured max flush delay, not a flat
  10,000-entry constant, and is capped at a stated per-tenant memory budget**
  (issue #1735). The new `ravel_catalog::derive_cache_capacity_per_tenant`
  computes `shards * 6 signals * ceil(3600 / flush_secs) * 3 unsealed hours`,
  floored at the old constant and capped at 25,000 entries;
  `build_catalog` calls it with the server's resolved `--max-flush-delay`
  instead of the flat default. The signal term matters because the caches are
  partitioned by tenant and not by (tenant, signal): a tenant ingesting
  metrics, logs and spans keeps three unsealed tails in one LRU, and a
  single-signal derivation under-sizes it by that multiple and leaves it
  thrashing. The cap matters because one capacity bounds two caches per tenant
  (commit records and L1 compaction records), so the worst case is
  `25,000 x 900 bytes x 2` = 45 MB per actively-queried tenant, held constant
  across every deployment shape: `--shards 64` would otherwise derive
  2,073,600 entries and 3.7 GB per tenant, with nothing process-wide bounding
  the next tenant. NEITHER cache is bounded by its entry count alone, because
  neither record type has a bounded size: each is also held to an equal share
  of that budget in BYTES (22.5 MB at the cap), charging each entry an
  estimate of the live heap it holds and evicting until the tenant's summed
  charge is inside the share. `CommitRecord.declared_column_stats` is a
  repeated field capped neither by the proto, by `validate`, nor by the
  tenant-config declared-column path, so a record declaring 200 typed
  attribute columns charges about 20 KB against the 864 an ordinary one does;
  the commit-record cache evicts least-recently-used against
  `commit_cache_max_bytes_per_tenant`. A `CompactionRecord` carries one
  `CompactionInputIdentity` per compacted L0 segment, capped neither by the
  proto nor by `validate_compaction`, so one L1 record over 1,800 L0 segments
  charges about 137 KB, 150 times the per-entry planning rate; the L1
  compaction-record cache evicts oldest-first against
  `compaction_cache_max_bytes_per_tenant`. An entry-count bound would have let
  one tenant exceed its share of the 45 MB by two orders of magnitude on
  either side. The 900 bytes is a planning rate the capacity is derived
  against, not a per-entry cap, so the capacity is an entry cap rather than a
  guaranteed residency: a tenant whose records carry typed attribute column
  statistics holds proportionally fewer of them and the memory stays inside
  the figure. These caches sit outside ADR-1170's carved shares, so an
  operator budgets 45 MB times the number of
  concurrently queried tenants on top of them. The capacity covers a tenant's
  unsealed tail up to the cap, not whatever the tail actually is: at the
  shipped defaults the estimate is already 129,600 entries, the flush-cadence
  term counts the age trigger only, so a tenant flushing on object size seals
  more records per shard-hour than it assumes, and the three-unsealed-hours
  term assumes the default seal parameters, under-counting by about 1.8x at
  `--gc-max-flush-lifetime 4h`. Over the bound a resolve pays per-record GETs;
  a tenant whose records carry declared-column statistics now holds fewer than
  the flat 10,000 the old cache held, so it can pay GETs it did not pay before,
  which is the hit rate the enforced memory bound costs. `--disable-cache` keeps the flat
  10,000-entry capacity rather than the derived one, and with it a 9 MB byte
  budget for each of the two caches, so the memory-constrained-container flag
  stays on the lowest capacity the code supports short of disabling the
  resolve path's record cache entirely. No new CLI flag is added; the
  capacity is a function of existing ingest configuration.
  10,000-entry capacity rather than the derived one, and with it a 7.5 MB
  compaction-record byte budget, so the memory-constrained-container flag
  never costs more record-cache memory than it did before this change. No new
  CLI flag is added; the capacity is a function of existing ingest
  configuration.
  the next tenant. These caches sit outside ADR-1170's carved shares, so an
  operator budgets 45 MB times the number of concurrently queried tenants on
  top of them. The capacity covers a tenant's unsealed tail up to the cap, not
  whatever the tail actually is: at the shipped defaults the estimate is
  already 129,600 entries, and the flush-cadence term counts the age trigger
  only, so a tenant flushing on object size seals more records per shard-hour
  than it assumes. Over the bound a resolve pays per-record GETs as it did
  before, so the direction is safe. `--disable-cache` keeps the flat
  10,000-entry capacity rather than the derived one, so the
  memory-constrained-container flag never costs more record-cache memory than
  it did before this change. No new CLI flag is added; the capacity is a
  function of existing ingest configuration.
  top of them. No new CLI flag is added; the capacity is a function of
  existing ingest configuration. A repository guard now fails a pull request
  that touches `crates/` or `services/` and carries no changelog entry, which
  is why this internal sizing change carries one.
- **A distributed deployment now refuses three unsafe listener shapes at
  startup** (issues #1724, #1703, #1690). Starting with `--distributed-query`
  and a wildcard bind refuses unless `--advertise-fragment-endpoint` names the
  host peers should dial, because the server previously advertised `0.0.0.0`
  to its peers; in the combined layout, where both lanes share one socket,
  that flag takes a host only and a `host:port` value is refused. A
  non-loopback `--mtls-listener` refuses unless
  `--mtls-trust-forwarded-header` says the operator meant to trust a forwarded
  identity from anything that can reach the port. The dedicated fragment
  listener now requires a client certificate signed by the configured CA, and
  the coordinator presents its own identity when dialling; a certificate
  provisioned against the previous documentation carries `serverAuth` only, so
  startup parses it and refuses when `clientAuth` is missing rather than
  letting every outbound dial fail at the handshake and fall back to local
  execution. Regenerate the fragment certificate with both usages before
  upgrading.

- **The physical retention sweep is now all-or-nothing under a legal
  hold** (issue #1697). A hold on any key the pass would delete (a commit,
  compaction or rewrite record, an L0 data object, an L1 object, or the
  tombstone) makes the pass delete nothing and return `SweptPartial`; before,
  the pass skipped only the held keys and deleted the commit records and
  tombstone that named the held bytes. A bucket parked this way counts on the
  new `held_by_lease_buckets_total` counter. `ravel hold set --scope` now
  refuses a scope that covers only part of one shard's three hold prefixes
  (for example `t/<hex>/m/l0/0000/`); tenant-wide and signal-wide scopes are
  still accepted. A script that set such a partial scope must move to the
  `--signal`/`--shard` form.
- **`POST /api/v1/sql` now refuses a request body over 64 KiB, down from
  1 MiB, and refuses any statement over 1,000 structural tokens** (issue
  #1680). Both bounds return 400 on the HTTP surface, `InvalidArgument` on
  Flight SQL, and a `validation` error on MCP. The token bound is a pre-parse
  scan: a deep expression tree, which a flat operator chain can build without
  nesting anywhere, previously reached the planner and aborted the process on
  stack overflow, taking every tenant on the node with it. A statement that
  now returns 400 was previously executed, so a generated or machine-built
  statement near either bound is the case to check on upgrade. The token
  count is not a character count: a literal, an identifier and a keyword each
  cost one whatever their length, so quoting does not change the verdict.
  `docs/query-engine.md` states how the bound is calibrated and
  `docs/reference/http-api.md` documents the body cap.
- **Retention no longer deletes a metric object whose format version this
  build's reader does not admit** (issue #530). The horizon-gated physical
  sweep probes each object's trailer first and distinguishes an unadmitted
  version from corruption: an unadmitted version holds the whole bucket (the
  tombstone stays, the outcome is `SweptPartial`, and the new
  `ravel_maintain_retention_held_out_of_window_objects_total` counter rises),
  because the other side of a rolling upgrade reads that object normally. A
  corrupt object is still swept. A held bucket retains data past its retention
  window until the upgrade, migration, or rollback completes, so a nonzero
  counter rate needs operator action. Metrics (RSEG) only; logs and spans keep
  today's sweep.
- **RavelClusters without a deployment key must now reference an audit token
  key Secret through `spec.auditTokenKeySecretRef`.** Until they do, the
  operator reports `AuditTokenKeyMissing` and leaves the query Deployment as
  it is. Clusters with `deploymentKeySecretRef` need no action.
- **The operator's qualified-input hash now distinguishes an absent credentials
  `resourceVersion` from an empty one** (issue #36). The credentials
  `resourceVersion` slot carries the same one-byte presence marker the S3
  `endpoint` slot got in 0.15.0, so an unresolved credentials Secret no longer
  collides with one whose `resourceVersion` resolved to the empty string. The
  encoding of that slot changes for every cluster, so every persisted
  `status.storeQualifiedHash` changes and the first reconcile after upgrading
  the operator re-qualifies each existing cluster once against its unchanged
  store. The qualify Job is a one-shot that touches no Deployment, so no serving
  pod is restarted and there is no downtime. Subsequent reconciles are stable.
- **The distributed query client no longer carries an unbounded slice decode
  path** (issue #1912). `ravel-query`'s `RemoteSliceFetcher` drained every
  frame a remote sent into a `Vec` and decoded it afterwards, so the remote
  decided how much the coordinator held. Issue #1687 replaced that with an
  incremental capped decode for the metrics signal only, leaving the log and
  span helpers (`decode_log_slice_frames`, `decode_span_slice_frames`) and the
  collect-then-decode fetch that fed them as the last unbounded path. Both
  helpers and that fetch are removed, and `RemoteSliceFetcher::fetch` now
  decodes through the same `SliceStreamDecoder` the rest of the coordinator
  uses, under the per-slice caps #1687 introduced (1048576 response frames and
  230331648 wire bytes, refused as HTTP 422 naming both figures). No deployed
  query changes behavior: nothing served a log or span slice through
  `RemoteSliceFetcher`, and the log and span fetches on the `SliceFetcher`
  trait were, and remain, the defaults that report `Unsupported` and send the
  coordinator to whole-query local execution. The removed items were public in
  `ravel_query::distrib::client`, so any out-of-tree caller of them has to move
  to `SliceStreamDecoder`. `RemoteSliceFetcher` gains `with_max_frames` and
  `with_max_bytes`, `pub(crate)` test seams that replace either cap outright
  (not lower it).
- **Native histogram samples whose shape Prometheus itself rejects are now
  refused at ingest, on both the OTLP and Remote Write surfaces**
  (issue #1858). This is a behaviour change at the ingest boundary: a sender
  emitting any shape below had the sample accepted and stored before this
  release and now has it refused, so check your senders before upgrading.
  Refused on both surfaces: a `zero_threshold` that is NaN, positive or
  negative infinity, or negative. Refused on Remote Write for a custom-buckets
  histogram (`schema == -53`), which has neither a negative side nor a zero
  bucket: non-empty negative spans, a `zero_threshold` that is not zero, and a
  `zero_count` that is not zero. The boundary-list rule is tightened at the
  same time: on top of non-empty and strictly ascending (and absent under any
  other schema), every bound must be finite and the positive buckets sent must
  not outnumber the bounds by more than one, since `n` bounds define at most
  `n + 1` buckets and the last `+Inf` one is implicit. Those last two are
  separate rules rather than consequences of ascendingness: a lone `NaN` has no
  adjacent pair to compare, and a trailing `+Inf` is strictly greater than its
  predecessor, so both passed before and reached the query side, where reading
  past the boundary list yields `+Inf` and leaves the final bucket spanning the
  degenerate interval `[+Inf, +Inf]` for `histogram_quantile` to interpolate
  over. OTLP enforces all of these by refusing
  `scale == -53` outright, since it has no field to carry bucket boundaries,
  so no custom-buckets shape reaches its normalizer at all. Under the
  exponential schemas the zero side is untouched: a populated zero bucket
  stays admitted, as does a `zero_threshold` of `+0.0`, `-0.0`, or a
  subnormal, because Prometheus' `Histogram.Validate` reads `ZeroThreshold`
  only under the
  custom-buckets schema and never screens an exponential-schema value for
  magnitude. For the custom-buckets rules `+0.0` and `-0.0` both count as
  zero, matching Prometheus writing that rule as `ZeroThreshold == 0` in Go;
  every other pattern, a subnormal and a NaN included, does not. Each refusal
  is per-sample, not per-request, and behaves like every other structural
  ingest refusal on its surface: on OTLP the sample is counted in
  `rejected_data_points` with the reason in the partial-success
  `error_message` and under the `structural` normalize-reject counter; on
  Remote Write the request still answers `204`, the sample is counted into the
  surface's dropped-points counter, and the
  `X-Prometheus-Remote-Write-Histograms-Written` header excludes it. Each of
  the four Remote Write messages names the field it refused on. Refusing new
  samples does not clean up data already stored with any of these shapes, so
  the query-side zero-bucket guards remain in place.

- **Every PromQL parse of caller text runs the pre-parse complexity guard,
  because one function now does both** (issue #1817). The guard that keeps an
  over-bound query from overflowing the stack inside promql-parser, which
  aborts the process and takes every tenant on the node with it, used to be a
  separate call each parse site was expected to make first. The five sites that
  parse PromQL, including one in the query coordinator's federated path, now
  call `complexity_guard::parse_guarded`, which checks and then parses. No
  query that was accepted before is rejected now and no error message changes:
  each caller maps the funnel's two failure modes onto the error it already
  reported. A new gate check refuses a parse that reaches promql-parser by
  naming or importing it anywhere else in either crate, so a future entry point
  cannot skip the guard by not knowing about it.

- **ADR-0057's cost argument for the fleet admission reconciliation loop is
  marked as resting on a premise ADR-0069 reversed** (issue #1922). ADR-0057
  sizes the loop on "most processes see most tenants never" and on a process
  dropping a `(tenant, signal)` once the tenant goes idle; ADR-0069 decided
  that the admission map grows with tenant count instead, and the code
  implements ADR-0069. The ADR now records what the loop costs on the code as
  it stands -- a floor of `2 * T * S` sequential object-store round trips per
  cycle, with `T` counting every tenant the process has served, including one
  whose only request was rejected -- rather than a justification that has not
  held since ADR-0069 landed. No decision and no code changed; bounding the
  loop belongs with the ADR-0069 follow-up.

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
  CPU of reading the same value through a typed attribute column of the
  same name**
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
  typed attribute column. This is a CPU-only change: the shipped fetch
  policy reads
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

- **Orphan garbage collection now quarantines a candidate instead of deleting
  it, and runs on the full-sweep cadence instead of every maintain tick**
  (issues #528, #1734). Previously, a data object whose commit record was lost
  out of band (a bucket lifecycle rule, a prefix delete, a persistent LIST
  omission) was deleted outright at the orphan horizon (about 25 hours) with
  no recovery window, and a loss small enough to stay under
  `orphan_breaker_min_count` and `orphan_breaker_max_ratio` never tripped the
  mass-orphan breaker that exists to catch exactly this. A candidate is now
  moved to a `quarantine/<original key>/q<timestamp>` copy first, and the live
  key is deleted only after the copy succeeds, so a crash between the two
  steps can never destroy the only copy. The copy is physically deleted only
  after a second, independent horizon, `quarantine_horizon_ns` (default 7
  days), giving an operator a real window to notice and recover before data is
  gone for good. `ravel_maintain_orphans_present` (the existing mass-orphan
  gauge) now also counts a candidate whose quarantine copy failed, since a
  refused quarantine leaves the object live and is exactly the store-fault
  case the gauge exists to surface; new `orphans_quarantined`,
  `orphans_quarantine_refused` and `quarantine_reaped` counters make the event
  itself visible, each logged at warn level on any nonzero count.
  Because listing the whole L0 data prefix on every tick (every 300 seconds by
  default) was the single most expensive thing a maintain tick did, and it
  answers a question that changes slowly, orphan candidate selection, the
  quarantine reaper, and the mass-orphan breaker check now all run together on
  the same full-sweep cadence (6 hours by default, `interior_reverify_ns`)
  rather than on every tick. Gating the two together this way opened a gap of
  its own: a tick that skips selection never evaluates the breaker either, so
  it reads as "not tripped" and would have let the reaper run through a live
  incident on every skipped tick; the reaper is now chained to run only on a
  pass that actually ran selection and found the breaker clear, so a record
  loss that widens past the breaker's thresholds days after it started still
  holds its earliest quarantined copies rather than reaping them on the next
  skipped tick. Both gauges now hold their last completed-pass value across a
  skipped tick rather than reporting zero, and the per-tick sweep log line and
  both gauges say which pass kind produced them, so a tick that skipped
  selection no longer logs `orphans=0` in a way that reads as a measurement.
  `docs/deletion-and-gc.md` and ADR-0058 describe the quarantine mechanism,
  its horizon, and the incident runbook's restore-from-quarantine step
  normatively.

- **The object-store conformance suite now refuses a bucket qualified under an
  older, smaller probe set** (issue #1302). The suite grew from four probes to
  eight, but `CONFORMANCE_SUITE_VERSION` had stayed at 1, so a bucket
  qualified under the old four-probe suite still read as a current pass on
  startup while the four newer properties were never checked.
  `CONFORMANCE_SUITE_VERSION` is now 2, and the once-per-bucket re-record rule
  relaxes to once-per-suite-version, so `ravel-cli store qualify` overwrites a
  below-floor record with a current pass instead of leaving startup
  permanently refused. Startup also now compares the record's
  `backend_identity` against the connecting backend's own, warning (not
  refusing) on a mismatch: the identity is endpoint-derived, so an endpoint
  rename or a path-style/virtual-host switch changes it with no actual backend
  change underneath.

### Fixed

- **The shipped Maintain IAM template now grants the delete the dead-worker
  reaper issues** (issue #1975). `MaintainDelete` named nothing under `sys/`,
  so every `sys/maintain/workers/<process_id>` delete from
  `WorkerSet::reap_keys` came back `AccessDenied` on a deployment using the
  template. No heartbeat key was ever removed, and the prefix the live-set
  LIST reads once per maintain tick grew with every maintain process that had
  ever run. `deploy/iam/maintain.json` now allows `s3:DeleteObject` on
  `sys/maintain/workers/*` only: the memo snapshots and compaction claims that
  share `sys/maintain/` are not the reaper's to delete. Re-apply the Maintain
  policy to pick this up. A test drives all four heartbeat operations against
  the shipped policy with keys built by `heartbeat_key` itself.

- **What a `0` on `ravel_store_probe_last_run_timestamp_seconds` means is
  documented in one place, and it states all three causes** (issue #1982). The
  explanation was written out across the store-probe source, its
  tests, the shipped Prometheus rule file and the observability guide, and
  every copy gave the reading a single cause. Two were missing, and one of the
  two pages until an operator fixes something the alert's description does not
  mention. Four sweeps had already tried to keep the copies consistent; two of
  them added copies while removing others. The "What `0` means" section of
  `docs/guides/observability.md` is now the one statement of the causes, every
  other site points at it, and
  `scripts/guards/check-claim-single-source.sh` fails the build on a
  restatement that is not a pointer, on a registered pointer that stops
  pointing, and on a canonical block that has moved or lost a cause. The one
  exception is the gauge's `HELP` line, which ships in `/metrics` output where
  a reader has no link to follow, so it carries a one-line summary naming the
  three; the guard checks that summary too. No behaviour changed.

- **`--max-ingest-buffer-bytes`'s help text and generated reference page now
  state what `0` actually leaves unbounded, instead of calling it a disabled
  ceiling and nothing more** (issue #1740). `-h` and the generator that renders
  `docs/reference/ravel-server-flags.md` both show only the doc comment's first
  paragraph (`--help` shows all of it), so the fuller explanation further down
  the comment never reached either surface. An operator reading either one saw
  "`0` disables the ceiling (the gauge is still tracked for `/metrics`)" and
  nothing else, which is how a `0` setting produced a flush queue bounded only
  by host memory under a sustained object-store stall with no warning in the
  documentation they read. The first paragraph now says directly that `0` does
  not leave spawned-flush memory unbounded on its own -- `--max-queued-flushes`
  still caps the ordinary flush queue at every setting of this flag -- and names
  the one exemption from that cap that can keep growing under `0`: a buffer that
  has crossed its per-(shard, tenant) memory backstop, which with the byte
  ceiling disabled is bounded only by the length of the stall. A test pins both
  to the short help. No runtime behavior changes; the reference page is
  regenerated from the updated doc comment with `RAVEL_UPDATE_CLI_REFERENCE=1
  cargo test -p ravel-server --test cli_reference`.

- **A distributed query's recorded cost now covers every slice attempt, not
  only the one that survived** (issue #1723). `RoutingSliceFetcher::dispatch`
  runs a slice up to three times (the primary worker, one re-dispatch to the
  next rendezvous worker, then coordinator-local), and all three sit below the
  `SliceFetcher` seam. A worker that had already fetched part of its slice and
  then took a store error reported its failure with a zero accounting snapshot;
  the retry classification in `try_remote` dropped whatever an abandoned attempt
  had spent; and a slice whose final attempt ended in `Err` was mapped straight
  to a `QueryError`, folding nothing. The recorded cost was therefore one
  attempt's spend where the store had really served up to three, and a slice
  that failed on every attempt was charged for nothing at all. As an
  illustration of the scale, a tenant that had configured an 8 GiB byte budget
  could drive 24 GiB of real GET traffic with the recorded total still inside
  it; the shipped default for `max_bytes_scanned` is `Unlimited`, so this is a
  gap in what an operator who sets a budget gets, not in a default deployment.
  A failed attempt's real spend now travels on its terminal summary frame, in
  the same shape the byte-budget short-circuit already used, is carried across
  re-dispatches, and is folded into the coordinator's live accounting handle on
  every terminal status and on the error path too, where a slice that failed
  outright carries on the error itself whatever its attempts had managed to
  report. This is an operator-visible behavior change: byte-budget enforcement
  (`bytes_scanned_exceeded`, and the coordinator's in-loop check) now reads the
  sum over all attempts, so a tenant near its limit is refused earlier than
  before, and a query that retried slices and previously completed can now trip
  `TooManyBytesScanned`. The bytes it is refused for are bytes the store really
  served. Per-fragment stats report the same figure: a successful fragment's
  `bytes_reported` is the sum over its attempts, and a failed one reports what
  its attempts carried instead of a flat zero. Two gaps remain. When one slice
  of a fan-out fails, the query stops and its in-flight sibling slices are
  cancelled, so the GETs their workers already issued are not reported. And
  an attempt reports its own cost only once its terminal
  summary is decoded, so any attempt that ends before that point contributes
  zero rather than a guess. That covers a stream broken mid-flight, a decode
  fault before the summary, and EVERY coordinator byte-cap or frame-cap
  refusal, since a worker streams its summary last and
  `SliceStreamDecoder::push` checks both caps before it stores a frame. Such a
  slice still carries the spend of any EARLIER abandoned attempt, so a refusal
  on a re-dispatch reports the primary's cost and not its own. Closing that gap
  needs the worker to send its accounting ahead of the frames, which is a wire
  change.

- **`ravel-cli gc-config set --max-flush-lifetime`'s help text and generated
  reference page now state the floor the flag is refused below** (issue
  #1961). `set_gc_config` has always rejected a `max_flush_lifetime` below the
  ingest pipeline's own default `max_flush_lifetime`
  (`ingest_max_flush_lifetime_floor_ns`, read from
  `ravel_ingest::IngestConfig::default()` rather than duplicated as a
  constant, so the validator tracks that default automatically), but
  neither the flag's clap doc comment nor
  `docs/reference/ravel-cli-flags.md` said so: an operator who supplied a
  shorter duration got a refusal with no indication of what value would have
  worked. The doc comment now names the floor and where it comes from, and
  the reference page is regenerated from it. The help text quotes the
  current value as `currently 1h`; that figure is prose and is not derived,
  so it is hedged rather than stated flat.

- **The shipped Maintain IAM template now grants the permissions the orphan
  quarantine needs** (issue #1957). ADR-0058 decision 6 has the orphan sweep
  copy an orphaned L0 object to a top-level `quarantine/` key space, delete the
  original, and physically remove the copy once the quarantine horizon elapses,
  but `deploy/iam/maintain.json` named no resource and no `s3:prefix` under
  `quarantine/` at all. IAM is default-deny, so on a deployment running the
  shipped template the quarantine was refused at its first `PutObject`: the
  sweep could not quarantine a single orphan, and because the original is
  deleted only after the copy succeeds, nothing was lost but nothing was
  reclaimed either, and the reaper that drains the quarantine had no keys to
  find and no authority to list for them. The grant set is derived from every
  call site that touches the prefix rather than from one function:
  `MaintainWrite` reaches `quarantine/t/*/*/l0/*` for the copy
  `quarantine_object` PUTs, `MaintainList` admits the `s3:prefix`
  `quarantine/t/*/*/l0/*` that `sweep_quarantine` LISTs, and `MaintainDelete`
  reaches `quarantine/t/*/*/l0/*` for the reaper's physical delete. The GET of
  the original and the delete of the original are the same live L0 key the
  existing `t/*/*/l0/*` read and delete grants already cover, so neither needs a
  new pattern, and nothing reads a quarantined object back, so no `GetObject`
  is granted under `quarantine/`. No new pattern reaches a key outside
  `quarantine/`, and no other role's template reaches one at all. An operator
  who already applied an earlier copy of `maintain.json` must re-apply it; the
  fix is in the template, not in any running binary, so upgrading Ravel alone
  changes nothing. Re-applying lets the sweep quarantine orphans again, and the
  copies it writes become deletable once each one's `quarantine_horizon_ns`
  (7 days by default) elapses, not at once.

- **The shipped IAM templates now grant every permission the selective-erasure
  lifecycle needs** (issue #1849). ADR-0064 section 6 gives Maintain read on
  `del/**` and delete on `del/*.dreq`, but `deploy/iam/maintain.json` named no
  resource and no `s3:prefix` under `del/` at all. IAM is default-deny, so on a
  deployment running the shipped template the `.dreq` sweep was refused
  outright: completed erasure requests were never retired, and the query-time
  exclusion filter that reads them grew without bound for the life of the
  deployment. The grant set is now derived from every call site that touches
  the prefix, not from one function: `MaintainList` admits the `s3:prefix`
  `t/*/*/del/*` the sweep LISTs; `MaintainRead` reaches `t/*/*/del/*`, which
  covers both the completion record whose timestamp anchors the protection
  horizon and the `.dreq` body the erasure rewrite pass decodes;
  `MaintainWrite` reaches `t/*/*/del/*.done` for the completion record that
  pass writes; `MaintainDelete` reaches `t/*/*/del/*.dreq` for the request
  object itself; and `AdminWrite` reaches `t/*/*/del/*.dreq`, which is what
  `ravel-cli erase submit` PUTs. Each grant alone is unreachable without the
  others: the sweep fails on the `ListBucket` before it sees a request object,
  and with no `.done` writable it counts every request as still pending and
  deletes nothing. Completion records remain undeletable by every role,
  including Maintain, as the ADR requires, and no new pattern reaches a key
  outside `del/`. An operator who already applied an earlier copy of either
  template must re-apply it; the fix is in the templates, not in any running
  binary, so upgrading Ravel alone changes nothing. Re-applying lets the
  lifecycle run again and the backlog drains over subsequent passes as each
  request's protection horizon elapses, not at once.
- **An `--s3-endpoint` written with no URL scheme is refused at startup**
  (issue #1911). `minio:9000` used to be accepted by the endpoint rule, which
  only decides whether plaintext is allowed, and then killed the process from
  inside the S3 client the first time it signed a request, on
  `request valid: InvalidUri(InvalidUri(InvalidFormat))` and exit code 101: a
  message naming neither the endpoint, nor the flag, nor the fix. The one
  decision every binary routes through now refuses an endpoint that begins
  with neither `https://` nor `http://`, quoting it as it was written and
  asking for the scheme, so `ravel-server`, `ravel-cli`, and the operator all
  fail in their own pre-flight pass instead of at first request.
  `--s3-allow-http` does not accept such an endpoint: the flag chooses between
  TLS and plaintext, and an endpoint with no scheme has asked for neither. The
  scheme is matched at the front of the endpoint and without regard to case,
  so `HTTPS://minio:9000` is an `https` endpoint and a host named
  `my-http-proxy:9000` still carries no scheme. Under the operator the
  refusal is its own render error rather than the plaintext one: a
  `RavelCluster` whose `spec.storage.s3.endpoint` has no scheme goes
  `Degraded=True` with reason `SchemelessS3Endpoint` and a message naming the
  field and the endpoint, and no Deployment, Service, or store-qualification
  Job is created.
- **A `RavelCluster` held at the store-qualification gate keeps its
  `StoreQualified` condition when the reconcile then fails** (issue #36). The
  degraded status write replaces the whole `conditions` array and previously
  reconstructed only the `StoreQualified=True` case, so a pass held at the gate
  that then failed a write left a `Degraded` object carrying no `StoreQualified`
  condition and nothing to say the store had not qualified. The gate now carries
  out the exact condition it built, `True` or `False` with its Pending or Failed
  reason, and records it before it creates or deletes the qualify Job, so a
  failure in that API call cannot drop it either.
- **`sum`/`avg`, `rate`/`increase`/`delta`, `irate`/`idelta` and `resets` over
  custom-bucket (NHCB) native histograms now compare the bucket boundaries,
  not just the custom-buckets schema sentinel** (issue #1851). Three reducers
  guarded a mixed exponential/custom group by comparing `uses_custom_buckets()`
  alone: `histogram::sum_histograms` (behind `sum` and `avg`),
  `histogram::histogram_rate` (behind `rate`, `increase` and `delta`) and
  `functions::rate::instant_value_hist` (behind `irate` and `idelta`). Two
  custom-bucket histograms both carry the `-53` sentinel scale, so that check
  passed them through whatever boundaries they held, and the fold then merged
  bucket `i` of one boundary set into bucket `i` of another: a `sum` over
  series with different bounds, a `rate` window whose bounds changed
  mid-window, or an `idelta` over two adjacent samples whose bounds differ,
  each produced a histogram whose buckets combined unrelated value ranges,
  with nothing to say so. All three now compare the boundaries with
  `FloatHistogram::custom_bounds_match`, the same bit-pattern comparison the
  `h + h`/`h - h` binary-operator path already aligns its operands with, so
  bounds differing only in the sign of zero count as different. A group,
  window or adjacent pair that fails the comparison yields no sample.
  `irate`/`idelta` also raise a `vector contains histograms with mismatched
  custom buckets` warning, since that path has an annotation channel; the
  other two return a bare `Option` and drop silently. The drop is
  conservative rather than a match for Prometheus v3.13.1, which re-buckets
  differing bounds onto the intersection of the two boundary sets and raises
  an info annotation, as `combine_custom_reconciled` already does for the
  binary-operator path. Reconciling in the reducers instead would need their
  callers in `aggregate.rs` and `over_time.rs` and is not done here.
  `FloatHistogram::detect_reset` carried the same defect in comparison rather
  than combination form: it compares per-bucket populations by absolute index,
  so a boundary change read as "no reset" whenever no index happened to shrink.
  It now reports a boundary change as a reset, the same answer it already gave
  an exponential/custom mix. `resets` is the only caller whose answer changes:
  the three reducers above drop a differing-bounds window before any reset
  detection runs.
- **`--gc-max-flush-lifetime`, and its compiled-in default when the flag is
  absent, now refuse a resolved value below the ingest pipeline's own
  compiled-in flush lifetime, instead of accepting one** (issue #1744). The
  flag had no lower floor, and neither did the default it falls back to when
  unset: a resolved value below ravel-ingest's fixed (and currently
  unconfigurable) `max_flush_lifetime` let the compactor call a bucket sealed
  before a real writer's flush interlock had actually elapsed, which could
  void the erasure completion gate (`bucket_erasure_completion` reporting a
  pending erasure request complete while a flush that can still publish into
  that bucket was still in flight) and undercut the retention floor derived
  from the same value. `--gc-max-flush-lifetime`'s startup check
  (`Cli::validate` and `resolve_gc_runtime`, the latter being the single
  point the real compactor is built from) now floor-checks the resolved
  value whether it came from the flag or the default, read fresh from
  `ravel_ingest::IngestConfig::default().max_flush_lifetime` rather than a
  duplicated constant, so a `ravel-server` invocation with no flag at all is
  held to the same floor as one that names a value explicitly. A new test
  pins `DEFAULT_MAX_FLUSH_LIFETIME_NS` equal to that same ingest default, so
  the two compiled-in constants cannot drift apart silently.
  The durable `sys/gc` mutation path (`gc-config set`) and bootstrap path
  (a fresh bucket's first touch) also enforce the same floor read from the
  same function, so `sys/gc` -- the durable, operator-facing record of the
  deployment's intended GC values -- can never hold a value the process
  itself would refuse to run with. `sys/gc`'s own `max_flush_lifetime_ns`
  field is not currently read into any compactor config (that floor is
  defence in depth on the record, not a mechanism that itself prevents an
  early seal); the compactor's actual value comes only from
  `--gc-max-flush-lifetime`/its default, floor-checked as described above.

- **No disk-tier file operation runs on a runtime worker thread any more**
  (issue #1891). A process configured with a disk cache tier read and wrote
  cache files on the async runtime's worker threads in two remaining places:
  the block-range read path, which peeks both tiers per extent and admits the
  bytes it fetched, and the ADR-0064 background age sweeper, whose periodic
  tick walks the whole cache directory. A slow or contended disk parked a
  worker for the length of that file operation, so unrelated queries and
  ingest work sharing the runtime stalled behind it, the same starvation
  `get_or_fetch` was moved off the worker for. Both now run under
  `spawn_blocking`: async callers reach the tiered cache through
  `TieredCache::get_off_worker` / `insert_off_worker`, which keep the RAM tier
  inline (it is not I/O, and a RAM hit stays the fast path) and dispatch only
  the file operation, and each sweeper tick dispatches its directory walk.
  Cache semantics are unchanged: the same read-through, the same dual-tier
  admission, the same max-age bound in sweep intervals. A RAM-only cache
  (no `--cache-dir`) is unaffected, since it never touched the disk tier.

- **The shipped IAM templates now let the unreferenced-catalog sweep delete
  what it finds** (issue #1847). `deploy/iam/maintain.json`'s
  `DenyDeleteProtected` statement denied `s3:DeleteObject` and
  `s3:DeleteObjectVersion` on the whole catalog family, `t/*/catalog/*/*`,
  which also covers the snapshot and index objects
  (`t/*/catalog/<signal>/snap/*`, `t/*/catalog/<signal>/idx/*`) that the
  unreferenced-catalog sweep physically removes once they are unreferenced,
  aged past the protection horizon, and unleased. IAM's explicit Deny
  overrides any Allow for the actions it names, and the template's
  `ListBucket` prefixes did not admit the catalog `snap/` and `idx/` paths at
  all, so on a deployment running the shipped template every sweep pass was
  refused at its first listing, before any delete was attempted: catalog
  garbage, including any unreferenced snapshot or index object
  holding an erased subject's value, was never reclaimed. The deny is now
  narrowed to the catalog HEAD pointer alone (`t/*/catalog/*/HEAD`), the one
  catalog object the sweep never deletes, and `MaintainDelete` now grants
  delete on `snap/` and `idx/` keys to match what the sweep already does. The
  template also gains the reads the sweep needs before it can delete:
  `MaintainList`'s `s3:prefix` adds `t/*/catalog/*/snap/*` and
  `t/*/catalog/*/idx/*`, and `MaintainRead` adds `t/*/catalog/*/HEAD`,
  `t/*/catalog/*/snap/*` and `t/*/catalog/*/idx/*`. A
  new pinning test asserts both directions: HEAD stays denied, and a snap
  key and an idx key built from the same key constructors the sweep uses are
  deletable. This changes a shipped IAM template: an operator running
  `maintain.json` from before this change must re-apply it. Until they do,
  every sweep pass is refused at its `ListBucket`, exactly as before.
- **Four axis and description issues in the standalone Grafana dashboard are
  fixed** (issue #1962). "Workers and units" and "Declared-stat stamp
  coverage" carried `"fieldConfig": {"defaults": {}, "overrides": []}`, so
  every series on each panel shared one unitless auto-scaled axis: the L0
  records pending backlog crowded out the three small worker/unit counts on
  the first, and the drop counter sat flat at the bottom of the same axis as
  two hourly volume counters on the second. Both now carry overrides that
  move the outlier series to its own axis. The drop counter's override uses
  `byRegexp` against `^drops .*$`, not `byName`, because its legend format is
  `drops {{carrier}}`, which a `byName` match on `"drops"` never matches.
  "Cache residency and disk tier" rendered four axes over seven series
  because the unit overrides for `resident entries`, `bytes (served|admitted)`
  and the disk-tier rates all set `axisPlacement: right`, so the disk-error
  rates, the panel's alarm signal, shared a side with the byte-rate series
  instead of standing alone. The disk-tier rates keep the right-hand axis to
  themselves; `resident entries` and `bytes (served|admitted)` move to
  `hidden`, so they still scale on their own units without drawing an axis.
  Two visible axes remain: the default byte axis for resident/max bytes, and
  the disk-tier ops axis on the right. "Log block pruning"'s description
  named the pruned-share metric's `clamp_min(..., 1e-9)` denominator guard
  but never said what an idle system renders as, so an idle fleet showed the
  same falling-to-zero shape the description calls the alarm; it now carries
  the same idle clause as its sibling panel, "Query result cache hit ratio".

- **A denied read on a catalog HEAD, a covered snapshot part, or a
  column-stats object no longer degrades silently into "nothing here yet"**
  (issue #1976). This is the follow-up #1964 left open. Five more GETs
  treated any store error, including `AccessDenied` from a missing IAM read
  grant, the same as a genuine `NotFound`: the column-stats HEAD and stats
  object reads, the catalog HEAD and per-part reads behind the scrubber's
  postings tier, and the catalog HEAD read in seal-divergence verification.
  A permission fault there read as "no statistics yet", "not covered" or
  "nothing folded yet" on every attempt, with nothing an operator could see.
  `NotFound` still degrades exactly as before at all five. Any other error
  now surfaces, naming the failing key. What changes for a caller: on the
  column-stats HEAD and stats-object reads, a retryable error (throttling, a
  timeout, a transient blip) still degrades quietly, so only a non-retryable
  error such as `AccessDenied` fails the query (the client sees "upstream
  storage temporarily unavailable", not an integrity failure), instead of
  running without column-statistics pruning; the scrubber logs the postings-tier and
  seal-divergence reads (at `error` and `warn` respectively) and retries next
  tick, as it already did for other failures there; and `ravel-cli catalog
  verify` exits nonzero instead of reporting "nothing folded yet". The
  shipped query policy in `deploy/iam/query.json` already grants these
  reads.

- **A denied read on a catalog `idx/` object no longer degrades silently
  into "nothing to reuse"** (issue #1964). Three GETs of catalog index
  objects treated any store error, including `AccessDenied` from a missing
  IAM read grant, the same as a genuine `NotFound`: `load_covering_postings`
  returned `Ok(None)` as if no postings ref existed yet, and `fold_inner`'s
  two reuse-baseline reads (the prior part's column-stats object and the
  prior postings object) logged the same `warn!` and fell back to a full
  rebuild regardless of why the read failed. A permission fault recurs on
  every tick, so this reads as the postings tier or the reuse baseline
  never applying, with no signal an operator could act on.
  `NotFound` still degrades exactly as before: `load_covering_postings`
  returns `Ok(None)` and `fold_inner` falls back to a rebuild with a
  `warn!`. Any other error now surfaces: `load_covering_postings` returns
  `Err(LoadPostingsError::Store)` naming the key, which the scrub tick logs
  at `error!` before skipping the postings tier for that tick, and
  `fold_inner`'s two reads log at `error!` with the failing key instead of
  `warn!`, though the fold still falls back to a rebuild either way, since
  reuse is an optimization and not a correctness gate. The new signal is a
  log line: no metric counts these faults, so an alert on them has to come
  from logs rather than from `/metrics`.

- **A `/readyz` test now pins the false-healthy window the store-probe
  liveness gauge exists to expose** (issue #1963).
  `store_probe_last_run_gauge_goes_stale_while_readyz_stays_green` never
  started a server or queried `/readyz` despite its name, so nothing
  covered the behaviour issue #1728 added the gauge for: a dead probe task
  leaves `store_reachable()` frozen at `true`, so `/readyz` keeps answering
  200 while the gauge goes stale. The test now starts a server as the
  neighbouring `/readyz` tests do and asserts the endpoint through the
  whole sequence -- 200 once the store is reachable, still 200 while the
  gauge goes stale and reachability holds, and 503 once reachability flips.
  That last assertion is what makes the coverage real: removing
  `store_reachable()` from `Readiness::is_ready`'s conjunction leaves
  `/readyz` at 200 and breaks it, where a test asserting only the 200 cases
  would have passed against the broken code. It also pins that a failing
  probe cycle still advances the gauge, which a success-branch-only
  implementation would leave stuck.

- **The shipped rule file's test now validates `for:` and `expr:` values, not
  just structure** (issue #1928). `shipped_rules_name_emitted_metrics.rs`
  parsed `deploy/prometheus/ravel.rules.yaml` into groups and rules but read
  both fields as opaque scalar text, so `for: 10 minutes` and an unbalanced
  bracket inside an `expr` block scalar each parsed there while Prometheus
  refuses either on load, killing every alert in the file with no signal from
  the test. Every `for:` is now checked against Prometheus's duration
  grammar, and every `expr:` is parsed with Ravel's own PromQL parser
  (`ravel_promql::complexity_guard::parse_guarded`), each pinned as a literal
  count so a silently-empty extractor fails loudly rather than passing over
  nothing. No shipped rule changed.

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

- **A stalled flush no longer stalls every co-resident tenant on the same
  shard** (issues #1292, #1641). A shard actor acquired the
  `--max-inflight-flushes` permit on the actor task itself, before spawning
  the flush. At the default bound of one permit, a flush stalled retrying a
  slow object-store PUT held the only permit, which parked the actor's
  whole event loop: the channel stopped draining, the age-flush tick
  stopped firing, and finished flushes stopped being reaped. Because object
  keys are tenant-prefixed and S3 throttles per key prefix, one tenant being
  throttled by the store stalled every other tenant sharing that shard,
  including their age-triggered flushes, with no data lost but no
  availability either. Fixed first for the metrics shard actor, then for
  logs and spans, by acquiring the permit inside the spawned flush task
  instead: the actor now returns the moment it spawns a flush, so a stalled
  flush parks only its own task. Backpressure at the permit bound now comes
  from the process-wide ingest byte budget instead of from the actor
  blocking, since a flush holds its byte charge from the moment it leaves
  the actor. `--max-inflight-flushes` is documented as the per-shard flush
  isolation control it is: raising it lets healthy tenants keep flushing
  while one key prefix is being throttled.

- **A shard actor that dies and is respawned no longer silently halves
  ingest capacity forever, and the process now reacts to it** (issue
  #1299). A metrics shard actor whose flush task panicked left its channel
  closed, so every later write routed to that shard failed for the rest of
  the process lifetime with no signal to the orchestrator. The router now
  respawns a dead shard actor with a fresh writer identity, up to a bound
  of 3 respawns, and once that budget is exhausted the next death condemns
  the shard: `/readyz` turns unhealthy and Kubernetes replaces the pod,
  which is the only recovery left for a shard that cannot come back in
  process. A respawn restores write capacity but not the dead actor's
  buffered rows, which were never acknowledged and so are lost within the
  documented at-least-once contract, and the write that observed the death
  still gets a retryable error so the client retries onto the fresh actor.
  Three follow-on corrections shipped with it: the respawn budget is now a
  decaying allowance rather than a process-lifetime count, so a shard that
  fails once an hour is no longer condemned on its fourth outage regardless
  of how far apart those outages are; the shard's monotonic clock floor
  used to detect backwards clock steps (per-shard, not per-writer, so it
  survives the writer identity changing) is now carried across a respawn
  instead of resetting to zero, so the guarantee it provides holds for the
  shard's whole process lifetime rather than resetting on every respawn;
  and a readiness-registration bug that held an extra reference to the
  ingest router was fixed, which had silently prevented shard actors from
  being joined during every graceful shutdown.

- **The log and span ingest pipelines now condemn a shard and shed the pod
  on its first dead actor, matching the metrics pipeline** (issue #1691).
  Unlike the metrics pipeline, the log and span routers never respawn a
  dead shard actor, so its first death is already permanent; before this
  change only the metrics pipeline reported that condemnation to
  `/readyz` and to a `shards_condemned` counter, so a permanently dead log
  or span shard kept serving errors with its pod still in the Kubernetes
  Service rotation. Both pipelines now expose the same readiness signal and
  counter the metrics pipeline already had, so Kubernetes sheds the pod as
  soon as either shard actor dies for good.

- **A flush queued behind a stalled tenant no longer has its abandonment
  deadline silently spent by the wait** (issue #1739). A flush's
  abandonment deadline was pinned when the flush opened, before it waited
  for the shard's flush-concurrency permit. At the default
  `--max-inflight-flushes` of 1, a tenant whose object-store writes were
  slow held the shard's only permit, so a co-resident tenant's flush queued
  behind it spent its whole deadline sitting in that queue; if the stall
  outlasted `--max-flush-lifetime`, every flush that had opened during it
  was abandoned before it ever attempted a store write, dropping rows that
  had already been acknowledged to the client in buffered mode, with no
  crash and no store error. The deadline is now re-derived from the moment
  the permit is actually granted, so a flush that queues behind a stall
  gets its full flush lifetime for its own store calls once it holds a
  permit; a flush already past its deadline before it even reaches the
  queue is abandoned there without taking a permit. The abandonment counter
  is also now split by cause: `ravel_ingest_abandoned_queue_deadline_total`
  counts a flush abandoned while queued for a permit, separately from
  abandonments after a permit was held and the store calls themselves
  failed.

- **The flush trigger no longer over- or under-charges the object it is
  about to write, and the per-buffer memory ceiling now scales with a
  smaller configured budget** (issue #1305). The size-based flush trigger
  compared a buffer's in-memory footprint, which counts a fixed struct
  overhead behind every label or attribute, against `target_bytes`, so a
  buffer of many small series could accumulate several times
  `target_bytes` of real payload before it was ever charged with reaching
  the target: a nominal 8 MiB target fired at a small fraction of that in
  actual object bytes. The trigger now estimates the object's own size (a
  fixed per-series and per-record term, one copy of label and attribute
  text per series, no struct overhead) so the written object lands at or
  under the configured target instead of drifting over it. Separately, the
  per-buffer memory backstop that exists to flush before one tenant's
  buffer can exhaust process memory was a flat 64 MiB constant justified as
  an eighth of the 512 MiB default `--max-ingest-buffer-bytes` ceiling. On
  a process started with a smaller ceiling, one label-heavy buffer could
  fill to the full 64 MiB and consume the entire configured budget by
  itself, shedding every other tenant's writes with HTTP 429 while the
  backstop itself never triggered. The backstop now scales down with a
  configured ceiling below the default, so it can no longer exceed the
  budget it exists to protect.

- **A backward host clock step across a graceful shutdown or rolling
  restart no longer destroys already-acknowledged buffered rows** (issue
  #1307). The commit record's creation timestamp, the primary key of
  query-time duplicate resolution, was range-checked but never
  order-checked against a shard's previous flush, so an NTP step backward
  could let a stale write outrank the correction that was meant to replace
  it. A per-writer monotonic floor was added to keep each shard's
  timestamps non-decreasing within one process lifetime, but the flush a
  large backward step could not absorb was refused by dropping its
  tenant's entire buffered rows outright, including rows already
  acknowledged to the client in buffered mode; if that refusal landed
  during a graceful shutdown or a channel close, those rows were destroyed
  with the actor exiting right behind it. The refusal is now retryable
  rather than terminal: a flush that crosses the absorption bound
  re-anchors the floor and re-inserts its buffer instead of dropping it, so
  the next trigger retries it against the corrected floor, and the
  shutdown drain itself now retries over fresh snapshots until every
  tenant's buffer empties or a bounded number of passes is exhausted. Only
  a clock that keeps regressing across every one of those passes can still
  leave residue at teardown; that narrow remaining case is what
  `ravel_ingest_flush_all_residue_tenants_total` reports.

- **An OTLP Remote Write request's decompressed size is now charged against
  the ingest byte budget** (issue #1419). The process-wide ingest byte
  budget is meant to bound the transient memory ingest inflates during
  decompression, but only the OTLP HTTP gzip path charged it: a Remote
  Write request could snappy-decompress up to 64 MiB with nothing charged
  against any ceiling, so as many concurrent requests as
  `--max-inflight-ingest-requests` allows could each inflate that far
  outside every configured budget. The handler now reads the exact
  decompressed size from the snappy block's own header and charges it
  before allocating, so the charge matches what is actually allocated
  rather than the request's compressed size or its wire-size cap; a
  request already over the wire-size cap still gets rejected before it
  takes a charge, and a request the budget cannot admit is shed with a
  retryable response before its output buffer exists. OTLP gRPC and the
  OTAP zstd payload remain outside this ceiling; the codec that inflates
  them cannot be intercepted before the fact, and the documentation now
  says so plainly instead of claiming coverage it does not have.

- **A `RavelCluster` upgrade with `spec.gateway.maxInflightFlushes` set
  above the queued-flush cap no longer crash-loops every gateway pod**
  (issue #1642). The server refused to start when `--max-inflight-flushes`
  exceeded `--max-queued-flushes` (default 8), but the operator CRD exposes
  `maxInflightFlushes` with no corresponding field for the queue cap, so an
  already-running cluster configured above 8 would fail every gateway pod
  on upgrade with no custom-resource edit able to recover it. Startup now
  raises the effective queue cap to match the configured permit count
  instead of refusing, logging a warning that names both values.

- **OTLP no longer allocates unbounded memory exploding a wide classic
  histogram, closing a memory-exhaustion vector** (issue #1681). One
  histogram data point with N explicit bucket boundaries normalizes into
  roughly N+3 points, each copying the point's full label set, and nothing
  bounded N: a single request under the existing wire-size limit, holding
  a handful of histograms whose bucket-boundary lists filled the body,
  could explode into on the order of a million and a half points and
  allocate accordingly before any per-tenant series limit had a chance to
  run. A data point with more than 160 explicit bucket boundaries (an
  order of magnitude above what real exporters emit) is now rejected before
  any per-bucket allocation happens, and the total point count a request
  would explode into is also compared against the per-request data-point
  limit before that allocation, closing the remaining case of many
  small-but-numerous histograms in one request.

- **The OTAP gRPC metrics surface gained the same histogram-explosion cap
  OTLP already has, closing the same memory-exhaustion vector on that
  surface** (issue #1753). OTAP exploded a classic histogram the same way
  OTLP does but carried neither of OTLP's two bounds: a single very wide
  histogram on the OTAP surface could exhaust process memory in a way the
  equivalent OTLP request would already have rejected. OTAP now applies the
  same per-point bucket-count cap and the same post-explosion
  per-request total check OTLP applies, verified by a differential test
  that both surfaces now reject the same over-cap and over-total requests
  identically. Three related exemplar-accounting gaps closed alongside it:
  a request rejected for exceeding the exploded-point total had its
  exemplars decoded before that check ran, so the rejection discarded
  them without ever counting them as dropped; the check now runs before
  exemplar decode, so that path has nothing left to discard uncounted. A
  second rejection, on raw wire bucket count, never decoded exemplars at
  all and so could not count them by the same means; it now derives the
  dropped count from the columnar payload's own row count instead of
  decoding. And a third payload type, exponential-histogram data points,
  was missing from the dropped-exemplar count entirely on the path that
  rejects them as unsupported. All three OTAP rejection paths now report
  dropped exemplars with the same fidelity OTLP does.

- **The ingest router no longer forwards a client-supplied identity header
  to the upstream store when the mTLS header has been renamed away from
  its default** (issue #1704). The router trusted a client-supplied
  identity header for tenant resolution with no certificate verification
  of its own, so any client could set that header and pick its own tenant.
  The router now refuses `--mtls-enabled` outright rather than installing
  an unverified mTLS resolver into its shared chain (a dedicated mTLS
  listener able to isolate the resolver safely does not exist yet;
  `--tenant-token` and `--oidc-issuer`/`--oidc-jwks-url` are the supported
  alternatives), and it strips the client-supplied identity header from
  every forwarded request on both the HTTP and gRPC paths before dialing
  the upstream. That strip initially covered only the header's default
  name: a deployment that renamed it via `--mtls-header` still forwarded
  the client-supplied value upstream untouched, reopening the same
  spoofing gap on exactly the deployments that had customized the header.
  The configured header name, not just the default one, is now resolved
  once and stripped under every key source. An operator who set a custom
  `--mtls-header` should upgrade to pick up the fix; routing selection by
  the mTLS-subject key source is unaffected, since a client can already
  choose its own shard by that mechanism today and that is unchanged.

- **Admission reconciliation's read cost no longer grows without bound as
  processes come and go** (issue #1679). A per-`(tenant, signal)` admission
  cycle lists a snapshot prefix and reads every key in it to enforce a
  fleet-wide series cap; the prefix gained one key per process that had
  ever run and nothing ever removed one, so the read cost grew with every
  process that had ever existed. Past a few thousand stale keys a
  reconciliation cycle took longer than its own staleness window, every
  sibling snapshot read as stale, and each process silently fell back to
  enforcing the whole fleet cap on its own, with every listing and read
  still succeeding and no counter to show it happening. A stale key is now
  skipped without a read once its last-modified time is already past the
  staleness window, and a key past a wider horizon is deleted from the same
  listing, bounding both the reads and the listing itself. The same fix
  shape was applied to the maintenance worker heartbeat registry, which
  had the identical unbounded-growth defect one prefix over.

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

- **Three ways to bypass the SQL complexity guard that aborts the process on an
  over-bound statement are closed** (issue #1678), hardening the guard that
  issue #1760 (below) later made impossible to skip entirely by construction. A
  `/*! ... */` MySQL-style hint comment was scanned as an ordinary comment and
  skipped, so five characters of wrapping hid an arbitrarily long operator
  chain: `SELECT 1/*!` followed by `+1` 2,000 times and `*/` scored 7 while the
  tokenizer actually produced 4,003 tokens from it. The scan now gives the hint
  region its own mode that counts every non-whitespace character inside it and
  enters no sub-mode of its own (an earlier fix that made it fall through to
  ordinary counting mode reopened the same bypass through a line comment inside
  the hint body). Separately, the switch from counting characters to counting
  tokens undercounted a digit-then-word sequence like `1AND` as one alphanumeric
  run costing one unit for two tokens, which halved the guard's effective bound
  on a boolean chain: `SELECT 1` followed by `AND 1` 998 times scored exactly
  1,000 units and built a 998-level parse tree, next to the roughly 1,050-1,080
  levels at which the planner aborts on a 2 MiB thread. A digit run now stops at
  the first non-digit; a run starting with a letter or `_` still consumes
  alphanumerics, since `a1` is one identifier. Finally, the audit redaction path
  (`redact`, used when `--audit-text` is left at its default of `redacted`)
  parsed and walked caller text with no complexity guard at all, so a 64 KiB
  statement that `validate` had already rejected as too complex still reached
  `redact` and aborted the process there; `redact` now runs the same guard
  `validate` does before it parses.

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

- **A log segment scan with one overflowing `attrs_raw` block no longer drops
  the rest of its partition's block list onto the slower row-decode path**
  (issue #1769). Blocks past a block whose attributes overflow the per-object
  dynamic-column budget used to stay on the row path for the remainder of the
  scan, even blocks with no overflow at all, because the scan only knew how to
  reopen the segment once and commit to row mode from there. `LogSegmentScan`
  now falls back for the offending block only and resumes columnar decoding
  after it, since its columnar and row cursor-advance paths already share one
  primitive. The narrowing is bounded rather than unconditional: a tenant with
  more than about a hundred distinct declared attribute names has overflow in
  most blocks of an object, and reopening once per block would cost quadratic
  redecode work on exactly the tenants already slowest on this path, so after
  two consecutive fallbacks with no clean block in between, the scan commits the
  rest of the partition's list to the row path in one reopen, capping any one
  segment at two reopens regardless of its block count.

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

- **Age-based retention now counts selective-erasure rewrite records, not
  only L0 commit records and compaction records** (issues #1313, #1321). A
  bucket's newest-event computation and its physical delete sweep both read
  only two of the three record kinds a bucket can hold, so an
  ADR-0064 rewrite record was invisible to both. Once the rewrite's own
  superseded inputs were swept, the rewrite record became the only live
  record the bucket held (its durable steady state, since compaction and
  migration both decline a bucket carrying one), so expiry evaluation saw no
  records at all and treated the bucket as never expired: the retention
  window stopped applying to it permanently. If such a bucket was tombstoned
  before its inputs were swept, the physical sweep deleted every other
  record but left the rewrite record behind, so the verifying listing found
  residue on every pass and the sweep outcome stayed `SweptPartial` forever,
  with the tombstone never deleted. Expiry evaluation now decodes and
  verifies every rewrite record it lists and folds its timestamp into the
  same maximum as the other two kinds (a rewrite record with no surviving
  parts, which the schema permits, contributes its own publish time rather
  than pinning the bucket forever); the physical sweep deletes rewrite
  records in the same pass, between compaction records and L0 data objects,
  so the existing delete ordering and the tombstone-deleted-last invariant
  are unchanged. The tombstone's recorded object count now includes rewrite
  records too, so a rewrite-only bucket's audit evidence no longer reads as
  a bucket that was never written to.

- **A production panic in selective-erasure rewrite on an empty, never-compacted
  L0 bucket is fixed** (issue #1410). A windowless erasure request (one covering
  a whole series, with no time-range restriction) passed the rewrite's overlap
  prefilter for every bucket with any live record, because that filter
  short-circuits to true for a windowless request before it can apply its usual
  empty-range check. Against a bucket with zero L0 commits and nothing ever
  compacted, that left the rewrite build with an empty input set and no
  superseded record to point to, which is a caller-contract violation the code
  enforces with a panic. Any caller that fed the derived set of not-yet-sealed
  hours into rewrite would panic on almost every request, since that set is
  empty for most shards once sealed, making this a live production path rather
  than an edge case. The rewrite now checks for this one shape before it builds
  anything, and reports it the same way it already reports a bucket with no
  overlapping request at all: nothing to do, nothing written. No data was at
  risk; the defect was an availability one, a panic instead of a no-op.

- **The listing conformance suite now certifies both entry points a listing
  call can use, and bounds every page-drain against a backend that never
  terminates a listing** (issue #1448). The suite's key-ordering probe
  drained its full pass through `list_after` only, and `list` and
  `list_after` are separately implemented on a real backend (a native S3
  client overrides each), so a backend whose `list` delivered keys out of
  order, or re-delivered an earlier key across a page boundary, while its
  `list_after` stayed correct, could pass qualification and then fail every
  production catalog scan, which drains through `list`. The suite now runs
  the ordering and distinct-set checks against a full pass through each
  entry point, and every listing failure the suite reports now names which
  one failed. Separately, the page-drain loop looped until a page carried no
  continuation token, so a backend that kept returning the same token spun
  forever, silently, because the existing de-duplication hid the repeat
  without ever stopping it; every caller that drains a prefix, including
  every catalog scan, inherited that risk. A drain now recognizes a repeated
  continuation token as a spinning backend and returns a typed error rather
  than looping, and is capped at 100,000 pages (100 million keys at the
  contract's 1000-key page size) against a backend that keeps returning new
  tokens without ever finishing. `docs/object-store-contract.md` documents
  both entry points as judged on their raw delivery sequence and states the
  new page and repeat bounds.

- **The listing conformance suite's delete-visibility check now actually
  exercises the `list_after` entry point it claims to test** (issue #1498).
  The probe previously drained only `list` and asserted a deleted key was
  absent there, so a backend whose `list_after` kept showing a deleted key
  as present could still pass qualification: `list` and `list_after` are
  separately implemented methods, and a delete visible through one but not
  the other is a real defect the probe must catch on its own rather than
  depend on which call a caller happens to use. The probe now drains both
  and asserts the deleted key is absent from each; a new fixture whose
  delete genuinely applies (so `get` and `list` see it gone) but whose
  `list_after` still re-injects the deleted key proves the new half of the
  check actually fails when it should, since the assertion could otherwise
  read correct while no fixture in the suite ever reached it. Every
  listing-drain failure the suite reports (a listing error, or a pager that
  never terminates) now also names the entry point it happened on, matching
  the ordering and distinct-set failures, which already did.

- **Selective-erasure rewrite carries exemplars through its output, filtered
  per record so an erased instant's exemplar is dropped exactly like its
  sample** (issue #1512). A rewrite previously wrote every output segment
  with no exemplars at all, silently dropping every exemplar in a rewritten
  bucket, including exemplars belonging to series an erasure request never
  touched; this went unnoticed because exemplars are not counted samples, so
  the rewrite's own sample-count conservation check stayed clean regardless.
  Exemplars are now carried into the output and matched one at a time
  against the same per-record predicate the sample rows use: an exemplar
  survives only if its own series has at least one surviving sample in the
  output and no pending erasure request's window covers the exemplar's own
  timestamp. This distinction matters because once a request's completion
  record is written, its erasure request record is removed and no later
  query-time filter applies, so this rewrite pass is the only place a
  windowed request's exemplars are ever checked against the erasure window
  they fall in; a series-level check alone (keep every exemplar on a series
  with any surviving sample) would carry forward the value, trace ID, span
  ID and attributes of an exemplar sitting inside an otherwise-erased
  window. A series whose labels cannot be resolved during the rewrite now
  drops the exemplar rather than keeps it, since an erasure path must favor
  deletion over retention when it cannot verify a candidate. The rewrite
  report now counts `exemplars_kept` and `exemplars_dropped` so this is
  visible at the point of the rewrite rather than only inferable later.

- **The maintenance loop (retention, compaction, and garbage collection) now
  survives a panic and reports its own liveness, instead of silently dying
  and leaving every maintenance metric frozen** (issue #1683). The loop ran
  as a single spawned task with no restart and no completion signal of its
  own; every maintenance gauge on `/metrics` is written only at the end of a
  completed cycle, so a panic anywhere in the loop's discovery or sweep call
  graph left the process Running and Ready while `tenants_maintained`,
  `units_stalled`, and every safety gauge froze at their last healthy
  values. Retention stopped deleting expired data, compaction stopped
  folding, and the sweeper stopped reclaiming space, with nothing on
  `/metrics` moving to say so until a query failed on stale or missing data,
  potentially days later. The loop is now wrapped so a panicking cycle is
  caught rather than taking the process down, counted on the new
  `ravel_maintain_loop_panics_total`, and restarted after a backoff (1
  second, doubling to a 60-second ceiling, reset whenever an attempt
  completes at least one cycle, whether or not it later panics); a new gauge,
  `ravel_maintain_last_cycle_completed_timestamp_seconds`, is stamped at the
  end of every completed cycle, so its age, not its value, is the operator
  signal that the loop itself has stopped making progress. `docs/guides/
  observability.md` documents a `RavelMaintenanceLoopStalled` alert on the
  gauge's age (staleness over 1800 seconds, six default 5-minute maintain
  intervals) and a `RavelMaintenanceLoopCrashLooping` alert on a sustained
  rate of panics (more than 3 in an hour, held 15 minutes), because a loop
  that completes a cycle
  between every panic re-stamps the liveness gauge and resets its own
  backoff, so the gauge alone would stay quiet through that crash-loop
  shape. A related no-full-sweep alert is gated on the process actually
  owning at least one unit, so an empty cluster or a replica holding no
  units under the current ownership split does not page for correctly
  completing zero sweeps.

- **The listing conformance suite's pagination probes now force a real
  continuation-token boundary instead of passing on a fixed, small key count**
  (issue #1695). `S3Store`'s declared page size does not change the wire-level
  page size a real S3-compatible backend uses: each listing call opens its own
  lazy stream, pulls at most the declared number of entries off it, and drops
  the stream, so the backend's own continuation token goes unfollowed only when
  the declared size is smaller than what one real response actually carries. The
  suite's probes wrote a fixed handful of keys and accepted "more than one page"
  as proof of correct pagination, which an in-memory backend could satisfy with
  a trailing empty page emitted purely to mark the end of a listing, proving
  nothing about a real backend's pagination at all. Both probes now write enough
  keys, relative to the declared page size, to force at least two pages that
  actually carry objects, and fail qualification naming the real page count
  otherwise. `ravel-cli store qualify` gains a `--list-page-size` flag (default:
  the production S3 page size) so a qualification run exercises a real boundary
  against the store it is qualifying; a qualification run against the default
  page size now leaves about 2,018 scratch objects behind rather than a handful,
  which `docs/object-store-contract.md` now states so an operator's cleanup
  sweep sizes for the right number. The conformance suite version is unchanged:
  this changes how existing probes size their input, not which properties are
  checked, so no previously qualified store needs re-qualification.

- **The quarantine reaper is now held for the full duration of a live
  mass-orphan incident, and a legal hold now reaches a quarantined copy**
  (issue #1748). The reaper previously ran on every pass regardless of
  whether the mass-orphan breaker had tripped, so a record loss that grows
  over days (quarantining a few objects early, then widening until the
  breaker trips on every pass by day 7) still had its earliest quarantined
  copies physically deleted at the first quarantine horizon, which is
  exactly the permanence the quarantine mechanism exists to prevent, just
  arriving one horizon later. The reaper is now skipped for the whole time
  the breaker is tripped; a `force_orphan_gc` override is not a trip and
  still reclaims for an operator who has made that call. Separately, a
  legal hold on a tenant's data could not protect an object once it was
  quarantined, because hold scopes and the hold check are both plain
  prefix matches under `t/<tenant>/`, and a `quarantine/...` key never
  matches that prefix; the reap now also checks the hold against the
  recovered original key. `quarantine/` is a root-level key prefix alongside
  `t/` and `sys/` that was named in no key-layout document, leaving an
  operator writing a lifecycle rule or an IAM prefix policy with no
  documented reason to include it; `docs/deletion-and-gc.md`'s incident
  runbook also gained a restore-from-quarantine step, since the previous
  procedure (reconstruct from the live prefix) reads nothing once an object
  has been quarantined past the first horizon. Separately in this same
  review round, `docs/query-engine.md`'s worked example of the derived
  per-query S3 request budget is corrected from 48,200 to 15,800: the
  48,200 figure used
  a 500ms flush-cadence reference pair that a stock server, which ships a
  2-second flush cadence, does not actually run at.

- **A query coordinator's per-worker heartbeat key is now reaped, bounding a
  cost that previously grew for the life of the deployment** (issue #1761).
  A query-worker heartbeat key under `sys/query/workers/` was deleted only
  on a graceful drain, so a worker lost to a panic, a kill, an
  out-of-memory event, or a node loss left its key behind forever; the
  coordinator's liveness check lists the whole prefix and pays one read per
  key found, so the cost of a call made on every distributed query grew
  with the total count of query workers that had ever run, not the count
  currently alive. Liveness itself stayed correct throughout, since a stale
  heartbeat is read as dead, so nothing failed loudly while the cost grew.
  The liveness check now skips the extra read for a key the listing already
  shows as older than the liveness window, and a key past the reap horizon
  (twice that window, a clock-skew margin between the object store's clock
  and the reader's, not a second independent duration) is deleted outright,
  reusing the same shared predicates already shipped for the maintain and
  admission worker sets. On a deployment running the shipped query IAM
  template, this delete is currently denied
  (the template grants no `s3:DeleteObject` at all), so the reap logs a
  warning and the prefix does not shrink even though the extra-read savings
  still apply; granting `s3:DeleteObject` on `sys/query/workers/*` to the
  query role is required for the reap itself to take effect.

- **A graceful shutdown no longer drops acknowledged, buffered rows, and an
  overrun of `--shutdown-timeout` now fails the process instead of exiting
  clean** (issue #1291). Two ordering defects previously lost buffered records
  even though a drain ran: the router flush depended on an `Arc` unwrap that a
  still-live sweep task's clone made fail silently, and the drain awaited
  every open listener, under the same overall budget, before flushing at all,
  so a slow client holding a connection open could exhaust the whole shutdown
  budget before any flush ran. The flush now runs unconditionally before the
  listener join, the listener join runs inside its own sub-budget (four fifths
  of `--shutdown-timeout`), and an overrun of `--shutdown-timeout` now returns
  an error and a non-zero exit rather than logging "shutdown complete" after
  silently dropping the tail of the drain. This is a fix to the drain ordering
  itself, distinct from the residual-tenant and overrun metrics already
  covered under issue #1742. To keep the grace period ahead of the new drain's
  worst case, the operator now sizes `terminationGracePeriodSeconds` on every
  rendered `ravel-server` pod at 45 seconds (a 10 second preStop sleep plus
  the server's pinned 32.5 second SIGTERM-to-exit worst case at shipped
  defaults, plus headroom) and adds the preStop sleep itself, rather than
  leaving Kubernetes' 30 second default in place.

- **Flight SQL clients dialed the wrong listener** (issue #1296). The SQL
  distributed lane now dials a query worker's dedicated Flight SQL endpoint (a
  new `flight_sql_endpoint` field on the worker record) instead of the TLS-only
  fragment listener, which never spoke the Flight SQL protocol.

- **MCP cursors now pin the inputs a page was resolved from, not an
  enumeration of the segments that resolution produced** (issues #1501,
  #1529). Enumerating segments could push a token past its own bound and past
  the 256 KiB response floor on a large range; the cursor now carries the
  signal, the half-open event-time range, the minimum commit-token watermark,
  the pending erasure predicates in force, the typed attribute column set, and
  a keyset position, and is re-resolved deterministically on redemption. The
  cursor now has its own 4 KiB bound, tracked separately from the shared
  scalar allowance it previously competed with the plan and failure message
  for; a cursor that would exceed its own bound is now an internal-error
  condition rather than a silent drop with a warning, since only a server
  defect can produce one. Redemption now also expires a cursor whose pinned
  data a compaction has since taken apart, or whose signal and event-time
  range now intersect an erasure predicate that came into force after the
  cursor was minted (`cursor_expired`), keeping this distinct from
  `cursor_invalid` for a tampered or wrong-tenant token. An envelope that
  already carried a failure (`budget_exceeded`, `unavailable`) no longer has
  that failure overwritten by the generic message an over-bound cursor
  produces; the first, more specific failure is now preserved.
  `CURSOR_VERSION` is 4; a cursor minted under an earlier version is refused.

- **An MCP tool result can now carry a large unsigned 64-bit integer as an
  exact integer cell** (issue #1525). The wire cell type was a 64-bit signed
  integer, so a value above `i64::MAX` fell back to a string cell, giving an
  unsigned column a different cell type from a signed one. The cell now
  carries the union of the signed and unsigned 64-bit ranges, and only a
  genuine float becomes a float cell.

- **The MCP protection horizon is minted as a future instant** (issue #1560).
  It was computed in the past, which made every cursor redemption's deadline
  re-clamp against it fail immediately.

- **OTLP metric writes that silently dropped informational data (histogram
  min/max, exemplars, integer precision) now report that drop in the
  partial-success response** (issue #1585). The partial-success gate previously
  keyed off the rejected point count, and an informational drop rejects nothing,
  so it took the `None` arm and discarded the `error_message` naming what was
  dropped; a sender lost min/max on every write and saw a response identical to
  a clean one. The gate now keys off whether anything was rejected at all, so a
  `rejected_data_points` count of 0 can still carry a populated `error_message`,
  which is what the OTLP proto reserves that field for.

- **The quickstart deploy's MinIO images and OpenTelemetry Collector image move
  off Docker Hub** (issue #1645). Docker Hub's anonymous pull allowance is
  scoped by source IP and shared with every other project on a runner, so an
  exhausted allowance failed the quickstart job with a message that pointed at
  credentials rather than at the real limit. Eight MinIO image references (both
  images in `docker-compose/minio.yml`, `docker-compose/ravel.yml`,
  `k8s/minio.yaml`, and `metricsbench/docker-compose.yml`) and the two `mc`
  invocations in the chaos and demo scripts move to quay.io, preserving the
  existing digest pins, which quay.io serves under the identical digest. The
  Collector moves to the `ghcr.io` path the upstream project publishes it under.
  Grafana's image stays on Docker Hub: no anonymous mirror was found on
  `ghcr.io`, `quay.io`, or `public.ecr.aws`, so the quickstart job still makes
  one anonymous Docker Hub pull, down from three.

- **Every image the quickstart deploy compose files and Kubernetes manifests
  pull is now pinned to a release tag plus an immutable digest** (issue #1720).
  Across the two quickstart compose files, 8 image lines are scanned and 6
  require a digest (Ravel's own released image is exempt by exact match); of the
  6 images referenced by the Kubernetes manifests, 4 are now pinned the same
  way, and Ravel's own locally built `ravel-server:latest` and
  `ravel-operator:latest` are exempt by exact match. A repo-wide check now scans
  both compose files and the manifests so the two cannot drift apart unnoticed.

- **`absent_field_list_indexes_nothing_and_flags_round_trip` no longer asserts
  on bloom counters** (issue #1926). The test compared a whole `WriteStats`
  against a mostly-default value, and `WriteStats` also carries
  `bloom_total_ns` and `bloom_blocks`, which bloom construction populates
  unconditionally per block and have nothing to do with the postings field
  list the test is about. Those two fields exist only when `ravel-logseg`
  compiles with its `stage-timing` feature, which a package-scoped
  `-p ravel-server` build never reaches but `--workspace --all-features`
  does, so the test passed normally and failed only under the latter. It now
  asserts the POSTINGS and dynamic-column counters by field instead of
  comparing the whole struct. No production behavior changed.

## [0.15.0] - 2026-09-08

### Added

- **The operator qualifies the object store before serving** (issue #36). Each
  reconcile pass renders a one-shot `<cluster>-qualify` Job running
  `ravel-cli store qualify` with the server image, credentials Secret, bucket,
  region, and endpoint of the tiers it gates, and creates the gateway, query,
  and maintain Deployments only once that Job reports Complete. A
  `StoreQualified` status condition carries the gate state (Pending while the
  Job runs, Succeeded on completion, Failed with the Job's message once its
  backoffLimit is exhausted). The qualified inputs (bucket, region, endpoint,
  image, credentials Secret name and resourceVersion) are hashed into
  `status.storeQualifiedHash`, so a later pass with unchanged inputs proceeds
  without re-running qualification even after the Job's TTL garbage-collects
  it; an input change deletes and recreates the Job and flips the condition
  back to Pending. The Job as a whole is bounded by `activeDeadlineSeconds`
  (1400 s across both attempts), so a hung attempt fails rather than holding
  `StoreQualified` at Pending; a Failed Job is
  deleted and recreated on the next pass instead of sitting until its TTL,
  and a status-only reconcile no longer re-triggers the gate. The kind
  lane's hand-run qualification Job is removed since the lane now deploys
  through the operator.
- **The object-store conformance suite probes concurrent creates, listing
  order, and deletes** (issue #1302). Four new probes close gaps the
  qualification suite left open: `ConcurrentCreateIfAbsentSingleWinner` races
  eight writers on one absent key and checks for exactly one winner,
  `LexicographicListingOrder` and `CrossPageListing` check listing order and
  `start_after` resumption across page boundaries, and `DeleteVisibility`
  checks that a delete is reflected in both a follow-up get and a follow-up
  listing. The suite tolerates the behaviors the object-store contract
  explicitly permits: a key repeated across a listing page boundary, and a
  losing racer's write landing before its own retryable-conflict response is
  retried. The shipped Admin IAM template gains `s3:DeleteObject` on
  `sys/qualify/*` so the delete probe can run under it; no other resource
  gets a delete grant. `CONFORMANCE_SUITE_VERSION` stays at 1 on purpose, so
  a bucket that already carries a `sys/qualification` record does not
  re-qualify and never runs these four new probes.
- **A derived per-tenant alert-state memo bounds alert evaluation cost**
  (issue #1294). The alert evaluator previously re-folded a tenant's entire
  `Signal::Alerts` transition history on every tick, so cost grew with the
  cumulative transition count rather than the rule count. A durable memo at
  `t/<tenant_hash>/a/state/latest` now seeds each tick's fold, and a tick
  reads only the commit records after the memo's watermark. The watermark is
  bound to the reader's own seal-bound hour: a watermark the memo carries
  above that seal-bound hour is clamped down to it rather than trusted, so
  an evaluator with a stale or ahead clock cannot skip a legally late
  transition. A memo with a duplicate `alert_id` is treated as a decode
  failure rather than served, and a failed memo encode leaves the previous
  memo untouched instead of overwriting it with corrupt bytes. A missing or
  unreadable memo falls back to a full fold.
- **A bounded top-k optimizer rule for `GROUP BY ... ORDER BY ... LIMIT k`**
  (issue #1402). A grouped aggregate whose only consumer is an
  `ORDER BY ... LIMIT k` on a `min`/`max` aggregate over a non-nullable,
  non-float column now keeps only the top k groups in a priority map instead
  of materializing one accumulator per distinct group; ADR-0013's
  exact-semantics contract is unaffected because the rule fires only for an
  aggregate expression that is provably exact under the bound (max under
  DESC, min under ASC).
- **The operator hardens every rendered pod and scopes its own Secrets RBAC
  per namespace, keeping a cluster-wide watch on `RavelCluster`.** Every
  rendered gateway, query, maintain, and ingest-router container now carries
  a `SecurityContext`: `runAsNonRoot`, no privilege escalation, every Linux
  capability dropped, and a read-only root filesystem (safe because the only
  local write any of these processes can opt into is the disk cache, and the
  operator renders no `--cache-dir` flag). The operator's ServiceAccount no
  longer holds a cluster-wide read on every Secret; it reads a
  `RavelCluster`'s referenced Secrets through a namespaced grant in that
  object's own namespace, while its watch on `RavelCluster` objects stays
  cluster-wide so a cluster outside `ravel-system` still reconciles. Serving
  a namespace other than `ravel-system` also needs the namespaced
  RoleBinding this release ships (`deploy/k8s/operator/secrets-rolebinding.yaml`)
  copied into that namespace, or Secret resolution 403s. The same change
  adds a PodDisruptionBudget per tier (`maxUnavailable: 1`) and preferred
  pod anti-affinity to every rendered Deployment.

### Changed

- **`ravel-server` in mode `all` or `query` now installs the query-audit
  pipeline and records query text tokenized by default (`--audit-text
  redacted`).** A process with no key refuses to start: an unkeyed deployment
  (`--tenant-hash-unkeyed`) must set `RAVEL_AUDIT_TOKEN_KEY` to 64 hex
  characters or pass `--audit-text plaintext`; a keyed deployment
  (`--tenant-hash-key-file`) derives the key and needs no action. Gateway and
  maintain processes are unaffected. The docker-compose quickstart now ships a
  development key.
- **CAS-mutable sys records (provisioning, tenant config, metric metadata)
  move to format_version 2** (ADR-0066). An earlier release added fields to
  these records without a version bump, so a lagging binary reading a
  current record silently dropped the fields it did not model and wrote the
  stripped record back under compare-and-swap. Every reader gate for these
  three records now accepts exactly the version set {1, 2} instead of a
  ceiling-only check, and a CAS rewrite now refuses a record whose version
  exceeds what this build's writer stamps rather than re-encoding it through
  an older field set. Every writer now stamps new records at version 2. The
  background metric-metadata refresh reads through the same permissive
  reader the cache miss path uses, so a tenant's record upgraded to version 2
  during the writer rollout is no longer rejected by refresh and stuck
  serving a stale pre-upgrade snapshot for the rest of the process's life.
  The auth token map and key epoch record read gates gained the same floor:
  a version-0, unstamped record now fails closed instead of being decoded
  and rewritten.
- **Every query transport (HTTP, gRPC, mTLS, Flight SQL) now runs through one
  query service layer** (issue #1377). Each surface previously carried its
  own copy of admission, cost accounting, and audit controls, and the copies
  had drifted: analytics and exemplars ran outside the fleet-wide
  concurrency ceiling, exemplars recorded no cost, only the SQL surface
  billed a query a client disconnected mid-flight, admission was taken
  before authentication on some routes, and a malformed `match[]` selector
  could consume an admission permit before being rejected. All four
  transports now authenticate, then admit, then run, then audit in the same
  order; a malformed selector is rejected with 400 before any permit is
  taken; a cancelled, timed-out, or failed PromQL, metadata, or analytics
  query now bills the spend it reached instead of zero; `label_values` audit
  events are tagged as their own audit language, distinguishable from the
  other label route; an audit-sink submission failure is reported with the
  audit pipeline's own failure message instead of the storage-outage string;
  and the mTLS listener gets its own instance of the service layer so a
  certificate-identified caller is resolved against the mTLS tenant resolver
  rather than the public listener's bearer-token resolver.
- **The store-request ceiling (`max_s3_requests`, `--max-s3-requests`)
  is now checked immediately after catalog resolution, combining every
  lane's spend so far, not only during segment fetches** (issue #1376). A
  query whose snapshot resolved to zero segments never reached the
  incremental checks in the fetch loop, so it could spend more catalog
  requests during resolve than the ceiling allowed and still succeed; a
  mixed metrics-and-logs query is now checked against its combined resolve
  cost across both lanes. Applies to PromQL, SQL execute, SQL explain, and
  the Flight SQL `resolve_snapshot` path.
- **Metrics compaction releases each L1 segment's encoded bytes at PUT
  instead of holding them until the record publishes.** Peak memory during
  RSEG compaction previously carried a term that grew with the whole
  bucket's L1 output; the RLOG path already had this shape (ADR-0979
  decision 3), and this applies it to RSEG.
- **The RLOG plan phase's whole-object read is carried into the scan, bound
  by plan fan-out rather than the corpus.** On the whole-object fallback,
  the plan phase and the scan each fetched the same object; the plan
  phase's bytes are now handed to the scan and charged to `bytesReused`,
  with retention bounded to the plan fan-out in objects times the object
  size. On ClickBench q20 that was 6,785 GETs and 21.1 GB, where a
  corpus-sized cache gives 4,533 GETs and 11.24 GB.
- **The operator's qualified-input hash distinguishes an absent S3 `endpoint`
  from an empty one** (issue #36). The endpoint carries a one-byte presence
  marker before its value, so `endpoint: null` no longer hashes the same as
  `endpoint: ""`. The two select different stores, so editing between them now
  re-runs qualification instead of being read as an unchanged input.
- **The operator now bounds qualify-Job recreations for a store that keeps
  failing qualification** (issue #36). A failing `ravel-cli store qualify` Job is
  recreated on a capped exponential backoff (30 s doubling to a 480 s ceiling)
  and, after 6 consecutive failures, held in a one-hour terminal cooldown that
  only an input change or the cooldown's expiry clears, rather than looping
  delete/recreate every retry interval. The failure count and next-retry instant
  are persisted in `RavelCluster` status, and the `StoreQualified=False`
  condition names the attempt count and the next retry time.
- **The operator's Secret change-detection checksum is now a `blake3` digest, 64
  hex characters** (issue #36). It replaces a 16-character standard-hasher value
  that was not stable across Rust toolchain versions. Because the annotation
  value changes, the first reconcile after upgrading the operator rolls every
  rendered gateway, query, and enabled maintain Deployment once, even when their
  referenced Secrets are unchanged; subsequent reconciles are stable. Schedule
  the operator upgrade in a maintenance window that tolerates one rolling restart
  of each serving tier.
- **`latency-first`'s published trade is re-measured and now names the commit it
  was taken on** (issue #1316). Over 3 reps on the reference corpus,
  42-statement basis, true cold in the warm-up-empty state, at concurrency 256:
  **5.30x the GET requests (570,752 against 107,781) for 52% less cold time
  (235.7 s against 493.0 s mean), per-rep range 50.3% to 54.2%**. ADR-1196
  records the commit and basis. The 0.14.0 entry below states 5.45x for 41%,
  which is correct about the run it described but predates the changes to the
  `cost-based` arm that form the ratio's denominator. The numerator (570,752)
  is unchanged and was bit-identical across all three reps, so only the
  denominator moved. The ratio is a measurement of two code paths at a point in
  time, not a property of the policy. The timing half also carries more noise
  than previously assumed (6.7% to 14.8% per-arm spread across reps on the
  measurement host), so the figure is a mean with a range, not a constant.

### Fixed

- **OTLP HTTP gzip inflate is charged against the ingest byte budget as it
  decompresses** (issue #1297). The OTLP HTTP metrics/logs/traces handlers
  decompressed request bodies up to 64 MiB before the router's own buffer
  charge ran, so up to `--max-inflight-ingest-requests` copies of a 64 MiB
  inflate could exist at once, entirely outside `--max-ingest-buffer-bytes`.
  Each chunk is now charged to the process-wide ingest budget before it is
  retained, in exactly-sized allocations rather than a growing buffer, and a
  decompression whose running total would cross the ceiling is shed
  mid-inflate (429) rather than allocated in full; a body over
  `MAX_DECOMPRESSED_OTLP_BODY_BYTES` is refused with 413 at the first byte
  past the cap rather than after the whole body is decompressed.
- **`--dev-insecure-tenant-header` is refused unless every listener (HTTP
  and gRPC) binds loopback, not just `--listen-http`** (ADR-0009, issue #94).
  The flag was reachable, unguarded, on a non-loopback `--listen-grpc`,
  letting an unauthenticated request forge tenant identity on the gRPC and
  Flight SQL surfaces. The same commit also refuses `--distributed-query`
  and `--fragment-listener` under gateway-only and maintain mode; a
  configuration that previously started with either flag set under one of
  those modes now fails at startup.
- **RLOG scan decompressed-byte accounting is complete** (issue #1401). A
  logs scan now reports the zstd work it actually does: the directory
  sections a segment open decompresses, each block page decode, the POSTINGS
  probe's term-block decode, and the plan phase's fallback and eager-funnel
  decodes (the alerts and audit scans) all charge `ScanStats.decompressed_bytes`.
  Several of these sites previously charged nothing, so a query relying on a
  text arm, a stream filter, or a below-threshold object could report zero
  decompressed bytes however much it actually decoded.
- **A column-stats object the reader refuses to decode (for example one
  whose uncompressed body exceeds the 256 MiB decode ceiling) now logs one
  warning per (tenant, signal, key) and degrades to no statistics, instead
  of doing so silently.** Previously this folded into the same silent
  "no statistics" outcome as a segment with no stats object at all. The
  per-tenant refusal marks are swept so a tenant that never resolves does
  not accumulate them without bound.
- **Duplicate attribute keys within one record now resolve last-wins by a
  fixed, documented order, consistently across SQL, ingest, and
  maintenance.** The winner is fixed by the order `rebuild_record` lays a
  record's attributes out: columnar occurrences first, ascending by
  FIELD_DIR type byte, then `attrs_raw` overflow occurrences ascending by
  canonical encoded value bytes, last entry wins. This is the record's
  on-disk layout order, not its write order, which the format does not
  preserve; four consumers of the merged attribute view previously
  disagreed about which occurrence a query resolved to.
- **RLOG corruption returns HTTP 500, not a retryable 503.** The local
  PromQL log path folded every RLOG fault, including corruption, into the
  same error class as a transient store error, so a Prometheus client
  retried a query against corrupted stored data forever.
- **Non-retryable OTLP HTTP write errors return 400 instead of 503** (issue
  #1298). A permanent failure such as a series value-kind mismatch
  previously fell through a catch-all that mapped it to 503, so a
  well-behaved exporter retried a request that could never succeed; the HTTP
  handlers now mirror the gRPC side's retryable/non-retryable distinction.
- **A raced `CreateIfAbsent` write that the backend answers with a
  retryable 409 is retried instead of reported as a permanent conflict.**
  `object_store` 0.14 maps every raw 409 to `AlreadyExists`, including AWS's
  own retryable `ConditionalRequestConflict` 409, which is distinct from a
  genuine already-exists. The S3 adapter now disambiguates with a HEAD: key
  present stays `AlreadyExists`, key absent becomes a retryable `Transient`.
  A HEAD probe that itself fails transiently (throttled, timed out) is now
  surfaced as retryable rather than folded into `AlreadyExists`, which had
  made the commit-publish path treat a race it did not lose as lost.
- **Erasure requests no longer complete while a bucket in their scope is
  still unsealed at acknowledgement** (ADR-0064). A pending erasure request
  could be marked done while an in-scope bucket was still unsealed,
  including one that had not yet published a commit record at all and so
  never appeared in the completion pass's own listing; the subject's
  records then reappeared as soon as the request's exclusion filter was
  released. Completion now blocks on every ack-time-open bucket, whether or
  not the listing discovers it.
- **The shipped KMS IAM templates scope their KMS actions to a placeholder
  tenant key ARN instead of every key in the account and region.** The four
  templates (gateway, query, maintain, admin) previously granted their KMS
  actions on `key/*`: gateway, query, and maintain hold
  `kms:Encrypt`/`GenerateDataKey*`/`Decrypt`, and admin holds `kms:Decrypt`
  only. An operator using SSE-KMS must add, not replace: one array entry
  for the `--s3-kms-key` ARN if that flag is set, plus one for every key in
  `--tenant-kms-config`, in every role's KMS Resource array; otherwise
  gateway, query, and maintain PUTs fail with AccessDenied and reads of
  objects written under that key fail decryption for every role.
- **`ravel-cli maintain migrate` reports a permanently blocked straggler
  correctly and only counts a bucket as permanently blocked when it
  actually is.** A below-target L0 segment that only a losing compaction
  record names is live data the migration walk cannot touch, and CLI/guide
  text now says re-running will not help instead of telling the operator to
  re-run; conversely, a bucket flagged by a concurrent compaction or
  erasure landing mid-walk is no longer counted as permanently blocked, and
  a record retention deletes between the walk's listing and its read no
  longer aborts the migration or produces a false permanent-block report.
- **Compaction refuses to compact a bucket whose erasure-rewrite record is
  already durable when the compactor lists it.** Publishing a second
  compaction record set over inputs a rewrite already covered could
  resurrect records the rewrite had erased once the pending-request filter
  that was hiding them was removed. The two passes still list-then-act with
  no CAS between them, so the maintenance driver's per-bucket serialization
  stays load-bearing.
- **The catalog's min-token fallback no longer serves parts from a losing
  compaction record.** Two overlapping live compaction records in one
  bucket could serve the losing record's parts in addition to the winning
  record's already-resolved parts; logs and spans have no query-time dedup,
  so this returned duplicate rows.
- **A carried whole-object read is now bound to the exact segment and
  tenant that produced it.** A carry could previously be handed to a read
  of a different object with no error, decoding the wrong object's rows
  wherever both objects were decodable.

## [0.14.0]

### Added

- **A `latency-first` logs fetch policy** (ADR-0996 amendment, superseded by
  ADR-1196). Measured on a reference cold-cache corpus, the `cost-based`
  default resolves to whole-object reads and moves 3x the bytes of
  `byte-minimal` at a deployment where transfer and retrieval are free,
  because the derived per-request rate saturates. `--logs-fetch-policy
  latency-first` resolves the same byte quantities as `byte-minimal`. It is
  an intent, not a tuning constant: it carries no concurrency default of its
  own, and resolves `--store-get-concurrency`, `--sql-partition-count`, and
  `--promql-fetch-fanout` exactly as every other policy does. `cost-based`
  stays the default; `latency-first` is an operator opt-in for deployments
  where cold wall-clock matters more than the request bill, and it pays off
  only once the operator raises concurrency explicitly to the measured
  configuration (`ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY`, 256), at
  a measured cost of about 5.45x the GET requests for about 41% less cold
  time (re-measured after this release as 5.30x for 52%; see the Unreleased
  entry and issue #1316). Raising that concurrency also raises in-flight
  fetch memory, which
  is not yet bounded by a process-wide budget (issues #1170, #1007); the
  flag's own documentation and a startup log line under `latency-first` make
  that precondition operator-visible.
- **The `alerts` and `audit` SQL tables** (ADR-1101). `POST /api/v1/sql` and
  Flight SQL serve five tables, and Flight `GetTables` lists all five; naming
  two in one query is still rejected before any listing. `alerts` is alert
  history, one row per state transition, and each row carries the write
  identity of its record (`writer_id`, `writer_epoch`, `writer_seq`), so a
  `ROW_NUMBER()` fold ordered by time and write identity returns exactly one
  current row per alert even when two evaluators overlap at a lease handover.
  `audit` reads back a tenant's own legal-hold and reshard records, which the
  maintenance process writes directly. It also serves query-audit records, but
  no shipped startup path installs the pipeline those go through, so
  `attrs['kind'] = 'query'` selects nothing until a deployment attaches one.
- **A read-side shard floor for fixed-shard signals** (ADR-1101). Alert and
  audit writers pin their shards by constant and neither signal is provisioned,
  so the catalog's scan-set derivations now take the maximum of the
  provisioning history and the signal's fixed shard count. An `audit` query on
  a `--shards 1` deployment reads the query-audit shard instead of silently
  omitting it, and a wider deployment scans exactly as before.
- **`ravel-memory`, one process-wide memory budget** (ADR-1170). The server
  derived four memory ceilings from the host and enforced each in a component
  that knew nothing of the others, so N tenants could each reserve half the
  box. `ravel-memory` is a leaf crate holding one counter every ledger draws
  from: a compare-and-swap reserve for the SQL adapter and an RAII reservation
  for the fetch layer.
- **Bloom pruning for PromQL `__body__` matchers** (ADR-1103 follow-up). A
  `__body__` equality, or an anchored regex with a token-bounded mandatory
  literal run, now pushes that literal onto the scan as a `has_word`
  predicate, so the RLOG block bloom skips blocks before decode. The
  per-record check still runs on every decoded record: the pushed word only
  prunes, it never decides. Negated matchers, unsupported metacharacters and
  `+`-quantified patterns are rejected by the extractor rather than pushed,
  because token matching is not a superset of substring matching.
- **A gate against wall-clock waits in injected-clock tests.**
  `scripts/check-injected-clock-helpers.sh`, run by `gates.sh` and CI, fails on
  `thread::sleep`, `tokio::time::sleep`, `tokio::time::timeout`, `Instant::`,
  `SystemTime`, a bare `sleep(...)` or `.elapsed()` inside a helper that takes
  a `TestClock` or `FixedClock`, unless the line carries
  `// allow-wall-clock: <reason>`. Its default scope is the loader's test
  module in `ravel-cli`; the one wait it found there was made clock-driven
  rather than exempted.
- **`scripts/verify-dispatch-gates.sh --with-gates`** runs `gates.sh` itself
  inside the cold worktree instead of a hand-listed command set, so a
  dispatched branch is checked against the same feature lanes CI runs and the
  run leaves a gate receipt the merge script can reuse.

### Changed

- **Cache warm-up keys off each tenant's latest ingest hour, not the current
  hour.** On a tenant whose data is older than the warm-up window the previous
  pass issued about 1,900 small object reads from the first query's own path
  before warming nothing; those reads are gone, and the first query on a cold
  process is about 3 s faster on the reference corpus. The replacement probe
  costs about 1 s at startup, so the end-to-end saving on a cold start is
  about 2.5 s, not 3. The probe fans out on the configured resolve concurrency,
  asserts tenant isolation on every listed key, and is bounded per shard.
- **Catalog resolve GET concurrency is configurable, default 128.** Every
  record GET in `Catalog::resolve_impl` passed through a fixed bound of 16.
  Measured against S3 on a 10,000-record unsealed tail, one cold resolve each:
  23.2 s at 16, 4.4 s at 64, 2.3 s at 128, with the same 10,001 GETs at every
  level. Cold resolve is concurrency-bound; this was the lever.
- **SQL reservations are charged to the process budget** (ADR-1170). Every SQL
  reservation now flows query, then tenant, then process on the way up, with a
  process refusal rolling the tenant and query charges back before surfacing
  as `ResourcesExhausted` naming the process figures. The infallible grow path
  trips a ceiling breach when the process limit is exceeded, so a DataFusion
  overshoot still ends in a typed error on the stream's next poll rather than
  an unaccounted allocation.

- **`--fetch-concurrency` unbundled into three flags** (ADR-1195): the SQL
  scan partition count, the PromQL/analytics per-query fetch fan-out, and the
  object-store GET concurrency were one knob with three coupled effects; they
  are now `--sql-partition-count`, `--promql-fetch-fanout`, and
  `--store-get-concurrency`, each independently sizeable. `--fetch-concurrency`
  still sets all three together for a config that predates the split (source
  `legacy-flag` in the startup log). Combining it with any of the three new
  flags is a startup error naming both flags, and a value of `0` in any of the
  four is a startup error naming that flag, raised before any fetcher, engine,
  or SQL session exists.
- **GET concurrency is process-wide, not per engine** (ADR-1195): `ravel-server`
  now builds exactly one `Arc<GetLimiter>` where it assembles its shared state
  and hands that same `Arc` to every fetcher- and engine-construction site in
  the process (the PromQL query path, the SQL executor's RSEG/RLOG/RSPAN
  fetchers, the distributed fragment path, cache warming, exemplars, and
  alerting). Before this, each RSEG and RLOG fetcher held its own semaphore, so
  N fetchers each configured to "8 concurrent GETs" could together put 8N GETs
  in flight against the store. Two behaviour changes follow. RSEG fetchers now
  honour the configured limit instead of the compiled default of 16, so a host
  whose derived value is below 16 issues fewer concurrent GETs than before.
  RSPAN fetchers are bounded for the first time: span reads previously ran with
  no GET limit at all, so a span-heavy deployment can see lower read
  concurrency and should size `--store-get-concurrency` for it. No fetcher in
  the server process owns a private limiter anymore.

### Fixed

- **The RLOG plan phase's whole-object read is carried into the scan.** On the
  whole-object fallback, `plan_segment` fetched each object to plan it and the
  scan fetched the same object again. The plan phase now hands its bytes to
  the scan, which short-circuits on them before any GET and charges them to a
  `bytesReused` figure rather than a cache hit. Retention is bounded: the
  first segments to complete their plan keep their buffer, up to the SQL
  partition count, and every later segment is re-fetched exactly as before, so
  peak retained bytes are the partition count times the object size, never
  the corpus. The saving is therefore about one duplicate read per unit of
  plan fan-out; removing the rest needs the carry to stream per partition
  instead of being held at the plan barrier, tracked separately.
  **Correction.** This change landed after the 0.14.0 tag and ships in
  0.15.0, where the retention bound is the plan fan-out rather than the SQL
  partition count.
- **The RLOG raw prefetch is gated on the cursor budget before `try_join!`**
  (ADR-0979 decision 4). The merge cursor's refill fetched the next two
  row-group blocks before the budget had been checked, so up to twice the
  group size was allocated and only accounted for on the following iteration.
  The pending fetch window is now priced from resident metadata and reserved
  before the fetch is issued.

## [0.13.0]

A stock `ravel-server` now sizes its query budgets from the host it runs on, so
a deployment no longer has to know six flags to scan a large tenant, and a
container sizes against the memory it may actually use. The two catalog defects
the 0.12.0 notes listed as known limitations are fixed, and the object-store
contract is checked by a TLA+ harness in CI.

### Added

- **TLA+ verification harness** (ADR-1113). `scripts/check-tla.sh` runs TLC over
  every area under `formal/tla` with `smoke`, `exhaustive`, `negative`,
  `traceability`, `ci`, and `all` subcommands; the TLC jar is pinned by sha256
  (or supplied through `RAVEL_TLA_TOOLS_JAR` and verified, never downloaded),
  Java 17 or newer is required, and every run writes one row per config to
  `.cache/tla/last-run.tsv`. The first area models the object-store contract
  (`docs/object-store-contract.md`): create-if-absent single winner, CAS on a
  fresh version, read-after-write including lost responses, monotonic versions
  across delete and recreate, multipart invisible until complete, listing
  completeness and consumer consistency. Three negative controls must fail with
  the exit code and property their `.expect` file pins (two invariants, one
  liveness property), state-space bands are enforced on passing runs, and a
  traceability table maps each requirement to its invariant and Rust symbol,
  naming the rows whose backend half is still an assumption. CI runs the fast
  lane when a formal area, the harness, an implementation crate the models cite,
  or a normative document changes; `tla-nightly.yml` runs the exhaustive lane on
  a schedule.
- **`ravel-cli cache reclaim-legacy --cache-dir <dir> [--apply]`** (#826).
  Lists (dry run) or deletes cache entry files left at the pre-namespacing
  `<cache-dir>/<shard>/<file>` layout, which the current cache never reads,
  evicts, or counts. Only entry files whose names map back to a cache key are
  touched; a foreign file keeps its directory. Safe while a node is live.
- **Partial multi-shard commits are reported for metrics and spans** (#1130).
  `WriteError` and `SpanWriteError` gain a `PartialWrite` variant matching the
  log router's: both routers now await every shard's acknowledgement and return
  the durable sibling tokens when some shards committed and others failed. The
  partial-commit count is exported as `ravel_ingest_partial_writes_total` for
  all three signals.
- **`sql_latency_bench --logs-fetch-policy` and `--logs-block-range-threshold`**
  (#1139), mirroring the server's flags with the same names and defaults, so the
  in-process lane routes logs fetches the way `ravel-server` does: at the default
  cost-based policy every object is read whole in one covering GET.
  `--logs-request-cost-bytes` is now optional and wins over the policy when set.
  Report provenance records the policy and the effective threshold, and a figure
  the report cannot know is labelled "not recorded" rather than as the server's
  configuration.

### Changed

- **Server budgets are resolved at startup, most of them from the host**
  (#1141, amending ADR-0088). When the flag is unset, `ravel-server` now
  resolves: `--fetch-concurrency` to twice the available cores (floor 8), the
  fetcher read cache (`--cache-max-bytes`) to 80% of usable memory and the
  catalog byte cache to 5%, `--sql-max-query-bytes` to 25% and
  `--sql-tenant-max-bytes` to 50%. `--max-segments` (1,000,000) and
  `--gc-max-query-duration` (11 minutes, still validated against the durable
  `sys/gc` ceiling) are fixed defaults that do not vary with the host. Usable
  memory is `/proc/meminfo`'s `MemTotal` **capped by the cgroup memory limit**
  (cgroup v2 `memory.max`, else v1 `memory.limit_in_bytes`; `max`, the v1
  no-limit sentinel, `0`, and malformed content are treated as no cap), so a
  container no longer sizes its caches and pools against host memory it cannot
  use. An explicit flag wins; an explicit per-query SQL pool raises a
  non-explicit tenant ceiling rather than being clamped by it, and an explicit
  `--cache-max-bytes` bounds both caches as before. Where memory cannot be read
  (a non-Linux host), the memory-derived values fall back to the previous
  constants. The startup log names each resolved value, its source, and the
  resolved deadline in milliseconds. These ceilings are LRU caps, not
  reservations. Before this change a freshly loaded ClickBench tenant (8,424
  objects) could not be scanned at all against the previous 1,024 segment cap;
  the measured ClickBench figures for a server at these defaults are recorded on
  #968.
- **Overlapping compaction records resolve to one authoritative record**
  (#1070). When two compaction records in one sealed bucket name overlapping
  input sets, the catalog keeps one winner per overlap group (largest input set,
  then smallest `input_set_hash`, then record key), serves its parts, and serves
  an input only a losing record names as a raw L0 segment, so logs and spans are
  served once instead of twice. The superseded-input sweep and the erasure
  completion gate follow the same choice, so an input only a loser names is
  never deleted from under a query. Publish-time refusal of a second overlapping
  record is left to a follow-up in `ravel-maintain`.
- **Declared-column statistics are stamped in one slot-keyed pass per record**
  (#1135). The bulk-load stamp no longer rescans a record's occurrences per slot
  or allocates per record; on the 104-column ClickBench shape it measured 11.39x
  faster per record on the measuring host, with byte-identical output. The
  bundled benchmark enforces a 2x floor, not the measured ratio, which is host
  dependent.
- **A timed-out or cancelled query records the cost it incurred** (#840) instead
  of a zero-cost outcome; an object-store GET is counted when it is issued, its
  bytes when it completes.
- **Alerts and audit scan sets are floored at their pinned shard counts.** A
  `--shards 1` deployment silently dropped every query-audit record from every
  audit query, with no error and no counter; the fixed read-shard count is now a
  floor (1 for alerts, 2 for audit).
- **CI: each push to `main` has its own concurrency group** (#1145), so a queued
  main run is no longer cancelled by the next merge and a release commit can
  always obtain the green `ci.yml` run the publish gate needs.

### Fixed

- **Erasure and GC holds** (#1085, ADR-0064 amended in #1140). The
  superseded-input sweep is gated on live-HEAD reachability, so an input a
  HEAD-named snapshot part still resolves is held rather than deleted; a
  supersession chain is deleted as one unit, its own records last of all, so a
  rewrite record outlives every input it superseded; an erasure request's
  `.dreq` and its query-time filter are held past their horizon while any input
  a rewrite applying that request superseded is still in the store, with the
  hold read off the sweep itself rather than a completion field the production
  writer never populates; the hold is observed on every chain in scope, young or
  aged; request ids are compared in one canonical form; a chain group with a
  legally held key is skipped whole; and a part reference whose declared bounds
  disagree with its header blocks fail-closed. Before these fixes an erased
  subject could become servable again after its filter was retired while its
  pre-rewrite inputs were still present.
- **Idempotent retry of a partially committed write** (#1130). The consistency
  model and the counter comments claimed a keyed retry of a timed-out or
  partially committed write is deduplicated; the idempotency marker is written
  only after a fully acknowledged write, so the key deduplicates from the first
  retry that commits in full. Every partial-commit warning carries the tenant
  hash.
- **`cache reclaim-legacy`** removes regular files only (a symlink or directory
  with an entry-shaped name is left alone) and fails on a listing error instead
  of under-reporting (#826).

### Documentation

- ADR-1103 decides PromQL over logs: the logs signal exposed to the existing
  PromQL engine as `ravel_log_lines` and `ravel_log_bytes`, with a `__body__`
  matcher. A decision record only; no endpoint ships in this release.
- ADR-0873 is amended to the shipped behaviour: an erasure rewrite part carries
  no declared min/max stamp at all, replacing decision 3's never-implemented
  recompute.
- The catalog and concepts pages state the overlapping-record guarantee and the
  full tie-break; the deletion and GC document states the real inputs of the
  erasure hold and why it terminates; the ingest and consistency pages qualify
  partial-commit retryability; the query, configuration, caching and
  admission-limits guides state which budgets resolve from host resources and
  which are fixed; the ClickBench internal pages record the new bench flags and
  note that passes taken before them are not comparable with passes at defaults.

### Known limitations

- Query latency still depends on the tenant's working set fitting in the read
  cache; removing the full-scan floor is tracked in #849. The derived cache
  default makes that working set fit on a host sized for the tenant, but does
  not remove the floor.
- The heaviest ClickBench aggregates over the whole table can exceed the derived
  per-query SQL pool on a 30 GB host and abort with `query memory budget
  exhausted`. Raise `--sql-max-query-bytes` (and the tenant ceiling with it) to
  run them.
- The read cache and the SQL pools are sized independently, so their ceilings
  can sum past the host's memory. They are LRU caps rather than reservations, so
  this is a policy gap rather than a measured fault; coordinating them under one
  process-wide budget is tracked in #1170.
- Completion records carry no per-bucket dropped counts from the production
  writer; the erasure hold no longer depends on them.

## [0.12.0]

Object-store request cost becomes an input that the logs read path and
compaction plan against, typed attribute column statistics ride on commit
records so aggregates over the live tail are answered without a scan, and the
RLOG compaction merge runs under a memory budget. The RLOG version 3 reader is
removed.

### Added

- **Request-cost-aware logs fetching** (ADR-0996). `--logs-fetch-policy`
  (`request-minimal`, `byte-minimal`, or the default `cost-based`) is resolved
  at startup into the byte quantities the fetch layer runs on, and
  `--logs-max-fetch-run-bytes` bounds one covering GET (default 64 MiB).
  `--logs-request-cost-bytes` states what one saved object-store round trip is
  worth in saved transfer bytes, and `--store-cost-profile` loads this
  deployment's per-request and per-GiB prices; a profile that fails to parse
  is refused at startup.
- **An S3 request ledger.** Billed HTTP requests are counted below the retry
  loop, so a GET that retried nine times counts ten attempts instead of one
  call, and KMS-routed traffic is counted too. GET requests are split per phase
  beside the wire bytes, the number of distinct data objects a query touched
  rides on the distributed query protocol as an additive field, and PromQL
  `query` and `query_range` responses render the per-phase split under
  `stats.phases`. Bench reports model request cost from the same ledger on the
  instrumented lanes; the Flight lane reports no cost rather than a false zero.
- **Typed attribute column statistics on commit records** (ADR-0873). Log
  ingest stamps each typed attribute column's exact min, max, and null count on
  the commit record, the catalog carries the stamps onto the segment reference,
  and compaction recomputes them for the segments it writes. SQL `MIN`/`MAX`
  over a typed attribute column is answered from the union of those stamps and
  the fold-built `.cstat` statistics with zero data GETs, which covers the live
  tail and token-resolved segments for the first time. Column statistics also
  carry an exact per-object integer sum, so `SUM(col + k)` and `AVG` over an
  integer column are answered from statistics as well.
- **`.cstat` re-keyed to snapshot-part binding** (ADR-0942): an envelope
  version 2 keyed by data-object content hash, and an additive snapshot HEAD
  field that references it. The column-statistics cache runs under a byte
  budget.
- **Bounded ephemeral spill** (ADR-0954). An opt-in, bounded scratch area for
  SQL operators whose exactness does not depend on holding the whole input,
  configured with `RAVEL_SQL_SPILL_DIR` and `RAVEL_SQL_SPILL_MAX_BYTES`. Off by
  default; a statement that exceeds its memory budget without it is still
  refused rather than approximated.
- **Advisory compaction claims** (ADR-1029). One small advisory object per unit
  of compaction work under `sys/maintain/claims/compaction/`, so two processes
  that would merge the same sealed bucket do not both pay for the whole merge.
  Correctness still rests on the compaction record's create-if-absent publish;
  a claim only saves cost.
- **MetricsBench** (ADR-0927): a versioned metrics workload and PromQL corpus,
  a Remote Write 1.0 ingest lane that replays one sample stream into Ravel and
  into config-supplied comparators, pinned comparator deployments, and a
  request-cost regression gate that fails a candidate report outside its
  per-figure bands.
- **Operator surfaces**: `spec.gc.protectionHorizon` and `spec.gc.grace` render
  the GC horizon flags on the maintain Deployment, so a bucket whose `sys/gc`
  holds non-default values no longer crash-loops. On a fresh cluster under
  per-role credentials the operator applies maintain first and holds the
  request-serving Deployments until `sys/gc` exists; a cluster whose
  request-serving Deployments already exist is never held. A bootstrap that
  has stalled for five minutes is reported on the cluster's conditions.
- **`ravel-cli` levers**: `maintain compact-tenant --bucket-concurrency`
  compacts independent buckets at once, its memory knobs
  (`--l1-part-memory-target-bytes`, `--max-l1-part-bytes`,
  `--input-read-concurrency`) are reachable, and its report attributes peak
  memory by phase. `load --max-flush-delay` raises the age trigger so a large
  `--target-bytes` is reachable, a `--target-bytes` that changed no object
  layout is reported rather than silently ignored, and the load report counts
  each shard's flushes by trigger (size, age, final).
- **`/metrics`** renders the ingest exemplar counters and the remaining flush
  counters (adaptive age flushes, grace-extended stale flushes, in-flight
  flushes).
- **Server-verified upload checksums** in the object-store crate. The S3
  backend can attach an `x-amz-checksum` value (CRC64-NVME or SHA-256) on
  single-part writes so the store verifies or rejects the bytes it received.
  Multipart uploads are excluded, and no `ravel-server` or `ravel-cli` flag
  exposes the setting yet, so the shipped binaries still write without one.
- **Documentation** (ADR-1040): a documentation architecture with a docs gate
  in CI, an HTTP API reference, generated `ravel-server` and `ravel-cli` flag
  references, a concepts page, an alerting guide, and operations pages for
  configuration, deployment, maintenance, and troubleshooting.

### Changed

- **The published `ravel-server` image builds every opt-in surface.** It is now
  built with `--features sql,flight-sql,otap`, so Flight SQL answers on the gRPC
  listener and `--otap` is accepted at startup without a source build. OTAP
  ingest is still registered only when `--otap` is given. The CI lanes that
  assemble images from host-built binaries build the same feature set.
- **Bounded-memory RLOG compaction merge** (ADR-0979). The merge opens an
  input's cursor only once its timestamp range can overlap the record about to
  be emitted, holds decoded blocks in their columnar form and charges them at
  their heap estimate, prices cursor admission from block shape and reconciles
  after decode, releases each closed segment's bytes at PUT, and runs under a
  merge budget; `compact-tenant` divides the budget across concurrent buckets
  only while it still carries the box-sized default. The admission change
  emits the same records and the same segment boundaries as opening every
  cursor at once; the number of open cursors becomes the input overlap depth
  rather than the input count.
- **`--max-l1-part-bytes` bounds encoded object bytes** (#872). The RLOG
  merge closed an L1 segment against a pre-compression payload proxy, so
  stored sizes missed the target in both directions, by several times on a
  compressible schema. The merge now encodes to measure the real object bytes
  and closes on that count, with the probe step capped so overshoot past the
  target is bounded. For the same inputs, segment boundaries differ from
  those 0.11.0 wrote.
- **Equality matchers resolve by dictionary ordinal.** Below the sparse-series
  threshold, a metrics catalog decode whose matchers are all positive
  equalities resolves each value to its dictionary ordinal once and
  materializes a label set only for a series that matched. Fetched bytes are
  unchanged; on a deterministic in-memory fixture of 4000 series the decode
  took 38.1 percent less wall time at 1 percent selectivity.
- **The catalog fold** reads each covered object once in the dual publish and
  keeps its statistics tally cache across HEAD CAS retries, so a lost CAS no
  longer refetches every object.
- **Typed attribute column reads** in SQL build their resolvers once per block
  rather than once per chunk.
- **Distributed query protocol**: the data-objects-touched count is an
  additive slice field. An older peer omits it and the merged figure degrades
  to the coordinator's own count. The protocol version is unchanged.

### Removed

- **The RLOG version 3 reader** (ADR-0892). RLOG now accepts exactly one
  trailer version, as RSEG and RSPAN already did under ADR-0027 decision 7 and
  ADR-0066 decision 1. Log objects written by releases before 0.11.0 are no
  longer readable, and `maintain migrate` reads the same single-version window,
  so a tenant that still holds them is wiped or re-ingested.

### Fixed

- Column-statistics objects a resolvable snapshot still referenced were
  treated as orphans by the unreferenced-catalog-object sweep and deleted once
  past the protection horizon, which broke queries that resolve typed-column
  statistics through the snapshot. Both statistics carriers on HEAD are now in
  the sweep's reachability set.
- Three SQL exact-aggregate paths (`COUNT` under a not-equal predicate,
  `GROUP BY` counts, and `SUM`/`AVG`) answered from a `.cstat` entry whose row
  accounting had not been reconciled against the segment it was joined to. All
  four readers now go through one reconciliation.
- The shipped IAM templates granted the maintain role no write on
  `sys/maintain/` and the query role nothing under `sys/query/`, so a maintain
  process failed closed with `AccessDenied` on its first liveness heartbeat,
  and a query worker on its membership heartbeat.
- On a fresh bucket with per-role credential Secrets, gateway and query pods
  raced maintain to create `sys/gc`, failed the create, and crash-looped. The
  operator now orders the bootstrap, and validates `spec.gc` even when
  maintain is disabled.
- Compaction convergence reported a bucket converged while the winner record
  referenced a segment that was absent and could not be re-put from this run;
  it now fails so the bucket is retried. The scope opener emits its request
  report on every outcome, and opener election is atomic and
  cancellation-safe.
- A refused row-major write into a columnar ingest buffer still left its
  records' extrema in the typed attribute column statistics accumulator, so
  the next flush stamped min, max, and non-null count for records the object
  does not hold. A refused write no longer contributes.
- `make demo` had failed on a fresh bucket since the keyed-tenancy gate
  landed; the dev bucket is pinned unkeyed, as the compose quickstart already
  was.
- The startup log reported a Flight SQL listener state the build was not in.
- A `load` at a raised `--max-flush-delay` did not complete at its own
  settings; the drain now sweeps tail stragglers with a re-flush ticker and
  leaves reserve headroom in the delay ceiling.
- `ravel-cli` walk-shaped commands name the effective store in their header
  and refuse a defaulted in-memory walk that reaches no data, instead of
  reporting zero counters at exit 0.

### Known limitations

- Query latency still depends on the tenant's working set fitting in the read
  cache; removing the full-scan floor is tracked in #849. ClickBench `q33`
  still exceeds the per-query memory budget (#837): the bounded spill relieves
  the aggregate, and the scan's share of the memory remains.
- Logs and spans return overlapping records twice when two compaction records
  with overlapping input sets are published for one bucket (#1070). Metrics
  are unaffected, because query-time dedup collapses the overlap. A fix is in
  review.
- After a selective-erasure rewrite lands in a sealed hour outside the fold's
  reconcile window, the superseded-input sweep can delete inputs a HEAD-named
  snapshot part still resolves, and queries over that hour then fail closed
  with `SnapshotInvalidated` until the fold reconciles the hour (#1085).
  Subject erasure stays correct throughout. A fix is in review.

## [0.11.0]

The log segment format moves to RLOG v4 and the logs query path becomes
columnar end to end. Measured on the ClickBench `hits` corpus (12.03 GB, 99.99M
rows, 42 timed statements on an r6a.4xlarge against in-region S3), the hot total
falls from 96.40 s to 72.52 s and the cold total from 320.18 s to 222.19 s.

### Added

- **Typed column statistics for logs** (ADR-0850). The fold writes exact
  per-object statistics for typed attribute columns, and `MIN`/`MAX` over a
  typed attribute column can be answered from the catalog without opening a
  segment.
- **SQL surface**: a fail-closed scalar and window function registry
  (ADR-0097), `LIKE`/`NOT LIKE` on the logs table with substring pruning
  (ADR-0105), and typed predicate pushdown for declared logs columns. Functions
  outside the registry now produce a typed error rather than a late failure.
- **Aggregation pushdown** for order-insensitive aggregates (ADR-0103), and a
  metadata-only rewrite that answers predicate-free `COUNT(*)` shapes with zero
  object-store GETs.
- **Native histograms through range evaluation** (ADR-0108): range counter and
  `_over_time` functions carry native histograms, and they distribute over the
  fan-out path for the first time.
- **Operator surfaces**: `--cache-dir` attaches the ADR-0046 disk cache tier end
  to end; `--s3-auth` and the S3 credential flags add an instance-role
  credential source (ADR-0106); `ravel-cli maintain compact-tenant` compacts a
  whole tenant and can seal sooner for measurement.
- **Intra-segment scan partitioning and a spill policy** for logs (ADR-0102),
  and late materialization for wide `TopK` projections (ADR-0774) so a sort
  reads the narrow set and fetches the rest only for surviving rows.

### Changed

- **RLOG bumped to v4** (ADR-0699): row groups plus a `PAGE_DIR` section, which
  makes per-column extents individually addressable. A narrow projection over a
  v4 object can fetch only the columns it needs instead of the whole object.
  The reader accepts v3 and v4; writers emit v4.
- **Columnar decode to Arrow** (ADR-0099). Logs and metrics scans build batches
  from a borrowed columnar block view, and declared string columns keep their
  dictionary form end to end rather than being materialized per row.
- **Pruning-proportional logs fetch** (ADR-0107): the fetch layer issues block
  ranges proportional to what pruning actually selected, and the whole-segment
  fast path now consults projection width before choosing a whole-object read.
- **Distributed query protocol bumped to version 4**, adding a
  `PartialAggregate` wire frame so pushed-down aggregates cross the fan-out
  boundary. Version 3 (ADR-0096) added per-sample dedup provenance and resolved
  0.10.0's run-merged limitation below.
- **Clustered compaction and object pruning** (ADR-0815), and a bulk-load
  columnar fast path with revised write-concurrency defaults (ADR-0109,
  ADR-0807).

### Fixed

- The 0.10.0 known limitation on run-merged series and the distributed query
  path is resolved. `ravel.queryfrag.v1` (protocol version 3, ADR-0096) carries
  per-sample dedup provenance on the wire, native histograms distribute for the
  first time, and both the run-merged and histogram refusals are removed. A
  distributed query over either shape now returns results bit-identical to the
  same query run locally.
- Native histograms were being silently dropped in three PromQL paths; they now
  carry through. `histogram_rate`/`sum_histograms` no longer panic on a schema
  mismatch, and `irate`/`idelta` had their reset direction corrected.
- Query text is guarded against a parser stack overflow.
- A fold lifetime whose seal margin would overflow is refused rather than
  accepted and silently sealing nothing.

### Known limitations

- Query latency still depends on the tenant's working set fitting in the read
  cache. When it does not, every full-scan statement re-reads its objects from
  object storage on each run: the eviction policy is scan-resistant (S3-FIFO,
  ADR-0046) but cannot create reuse that a scan-everything access pattern does
  not have. The published ClickBench figures above were measured with a cache
  larger than the corpus and do not characterize a tenant whose data greatly
  exceeds its cache. Removing the full-scan floor is tracked in #849.
- One ClickBench statement (`q33`) fails on connection-pool exhaustion (#837),
  so the totals above are over 42 of the suite's 43 statements.

## [0.10.0]

The metrics segment format moves to RSEG v7 and the L1 compactor stops
copying runs verbatim. Measured over 500 series at a 15-second scrape, a
merged L1 object costs 2.50 to 3.00 bytes per sample on representative value
shapes (integer and low-precision-decimal gauges and counters), the arms
ADR-0092's 2026-08-21 amendment identifies as representative. The 26.52 to
8.88 bytes per sample and 2.99x reduction quoted here previously is the
incompressible-value control arm (full-mantissa random floats), which that
amendment reclassifies as a worst-case bound rather than the representative
cost.

### Changed

- RSEG segment format bumped to v7 (ADR-0092). v7 is v6 plus three additive
  changes: an optional per-sample dedup provenance extension in the whole
  SERIES_META (so an L1 run can merge several writes' samples and still preserve
  exact dedup order); two value page encodings, `VAL_ALP` (18) and
  `VAL_GCD_DELTA_FOR` (19), and one timestamp encoding, `TS_GCD_I64` (2), each
  selected per page against the prior encoding and kept only when smaller; and
  two page-level byte savings (a run's first timestamp stored as a delta from
  the run minimum, and single-sample raw-`f64` value pages dropping the 8-byte
  alignment pad). `docs/segment-format.md` is rewritten as the self-contained v7
  specification.
- Pre-release single-version policy (ADR-0027): v6 read and write support is
  deleted in the same change. The reader accepts trailer `version = 7` only and
  fails closed on any other version, including a stray v6 object, with a typed
  `UnsupportedVersion`. There is no v6 reader and no v6-to-v7 migration path.
- L1 compaction merges runs instead of preserving them verbatim (ADR-0092,
  reversing ADR-0018's choice). An L1 object now holds one run per series
  rather than one run per input object per series, carrying each sample's
  dedup key in v7's per-sample provenance columns so late duplicates still
  resolve exactly. A series with a single contributing run keeps its bytes
  and carries no column, so an L0 flush is unchanged. Part splitting now
  accumulates encoded output bytes rather than predicted input bytes, since
  per-page codec selection makes output size a function of the data's shape.

### Known limitations

- A run-merged series cannot be executed over the distributed query path.
  `ravel.queryfrag.v1`'s `Run` message carries run-wide dedup provenance
  only, so a distributed fetch would resolve an overlapping timestamp to a
  different winner than the same query run locally. The worker refuses the
  merged shape and the coordinator falls back to local execution, which is
  exact. Any query touching run-merged L1 therefore loses read fan-out until
  the wire format carries per-sample provenance (#348). Results stay correct;
  the cost is parallelism.

## [0.9.5]

Documentation only. No code changed since 0.9.4, so the binaries and images
this release publishes are rebuilt from the same source.

### Added

- An interactive architecture explorer in the documentation.
- A release badge in README.md, pointing at the latest release.

### Changed

- ADR-0086 records that its required-checks decision has been applied:
  `supply-chain`, `docker-build`, `fuzz`, `object-store-contract`,
  `promql-difftest` and `actionlint` now gate merges to `main`.

## [0.9.4]

### Added

- GitHub Releases are published for every `vX.Y.Z` tag, carrying per-architecture
  binaries for `ravel-server`, `ravel-cli`, `ravel-operator` and
  `ravel-ingest-router`, separated debug symbols, a `SHA256SUMS` file, and a
  keyless cosign signature over it. The binaries are extracted from the
  published images rather than rebuilt, so each is byte-identical to the one
  inside the signed image.
- CI lints workflow files with actionlint and shellcheck, and fails if
  shellcheck is not genuinely available rather than silently checking less.
- CI fails when a path dependency's version drifts from
  `[workspace.package] version`.

### Changed

- Container images are roughly a quarter of their previous size. The builder
  now separates debug info with `objcopy` and ships stripped binaries carrying
  a `.gnu_debuglink`, so the `ravel-server` image drops from 923 MB to 209 MB.
  Symbols are published with each release. `[profile.release] debug = 1` is
  unchanged.
- A release compiles the workspace twice instead of six times. The publish
  matrix is now one job per platform, building all three image targets against
  a shared builder layer.

## [0.9.3]

### Added

- `ravel-ingest-router`, a Ravel-native ingest router that steers OTLP over
  HTTP and gRPC (HTTP/2) to a stable subset of ingest replicas, published as
  its own container image.
- gzip-compressed OTLP ingest over HTTP.
- Exemplars carried end to end over the OTLP HTTP ingest path.
- An optional `Authorization` credential on alert-sink delivery.
- Operator support for Gateway API ingress exposure and a Ravel-native
  ingest-affinity backend, per-tenant shard overrides, and an
  operator-settable flush cadence.
- Durable per-tenant indexed-field overrides applied at ingest, and a
  per-tenant PUT attribution metric family.
- Multi-architecture container images: `linux/amd64` and `linux/arm64` are
  each built on a native runner and the merged index is signed.
- A container-first quickstart whose marked README command blocks are
  asserted against a live stack in CI.

### Fixed

- Bump `h2` to 0.4.16 for RUSTSEC-2026-0258.
- `ravel-ingest-router` supervises its background tasks and redacts secrets
  from `Debug` output.

## [0.9.2]

### Added

- RSPAN v4 span segment format: per-key typed attribute columns replace the
  single opaque per-row attribute blob, and span events, including the
  exception stack traces they carry, are promoted into scan-queryable nested
  columns.

### Fixed

- Set the workspace version to the real release version so the image-publish
  version-tag gate passes; `0.9.0` and `0.9.1` had shipped from a `0.1.0`
  placeholder.

## [0.9.1]

### Added

- Selective subject erasure across metrics, logs, and traces: `ravel-cli
  erase submit` and `erase status`, resolver-side exclusion of erased
  subjects, and a segment-rewrite pass that removes their data from stored
  objects.
- A `spans` SQL table alongside `samples` and `logs`, over both HTTP and
  Flight SQL, with service name, duration, and status-code predicate
  pushdown.
- OIDC and mTLS tenant resolvers, the latter served on a dedicated listener,
  for authenticating tenants without static bearer tokens.
- Per-tenant query cost governance: bytes-scanned and S3-request budgets
  enforced during scans, with per-query cost accounting exported on
  `/metrics`.
- Online resharding through a generation-versioned shard count, with
  maintenance work leased across workers.
- Query-path OTLP trace export, enabled with `--otlp-trace-endpoint`.
- A local read-cache tier over RAM and disk in front of object-store reads.
- Signed and attested release images: every published index is cosign-signed
  in keyless mode and carries an SBOM and build provenance, and a tag publish
  is gated on a passing CI run for the tagged commit.

### Changed

- Cross-cluster federation defaults to TLS and warns on plaintext.
- Ingest flushes are pipelined with an adaptive flush delay, and process-wide
  ingest memory is bounded with idle-tenant eviction.

### Security

- Constant-time bearer-token lookup and a decode panic guard on the OTAP
  ingest path.
- Require an OIDC audience, and bump `jsonwebtoken` to 10.4 for
  CVE-2026-25537.

## [0.9.0]

First public release. Ravel is an OpenTelemetry-native observability database
whose only durable backend is S3-compatible object storage; every compute
process is disposable.

### Added

- OTLP ingest over HTTP and gRPC for metrics, logs, and traces, plus
  Prometheus Remote Write 1.0/2.0, with per-tenant admission limits and
  strict or buffered acknowledgement.
- Immutable segment formats on object storage: RSEG for metrics (including
  native exponential histograms and exemplars), RLOG for logs, and RSPAN for
  traces, each committed through a two-object create-if-absent protocol.
- PromQL query over `/api/v1/query` and `/api/v1/query_range`, with a
  differential-tested evaluator, and the Prometheus exemplar and HTTP API
  compatibility surface for Grafana.
- SQL query through Apache DataFusion over `samples` and `logs` tables,
  exposed over HTTP and Arrow Flight SQL.
- A post-evaluation analytics endpoint for change point detection and
  robust (median and scaled median absolute deviation) summary statistics.
- A unified alerting and detection engine that stores every rule transition
  as immutable, queryable data.
- Compaction, age-based retention, and garbage collection across all signals,
  with per-tenant SSE-KMS encryption, legal hold, and custody verification.
- Optional distributed read fan-out and cross-cluster federation, off by
  default and byte-identical to local execution.
- A Kubernetes operator with a `RavelCluster` custom resource, and published
  `ravel-server` and `ravel-operator` container images.
