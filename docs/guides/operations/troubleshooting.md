# Troubleshooting

Symptom to action. Every entry names what you saw, what usually causes it, a
command or a metric that confirms it, and what to do. If you are here because
something is paging, the three procedures at the top are the ones where doing
the obvious thing first makes the situation worse.

Nothing on this page asks you to read source code or a decision record to
confirm a diagnosis.

**Two things to know before you act on anything below.**

Only a `--mode maintain` process compacts, applies retention, sweeps or scrubs.
If your symptom is "storage is growing" or "retention is not deleting anything",
check that a maintain process exists before investigating anything else. See
[Maintenance](maintenance.md).

Deleting a catalog HEAD object is a supported repair. Deleting anything else by
hand is not, and the sweeper's orphan rule treats a data object whose commit
record you removed as garbage to reclaim.

- [The mass-orphan circuit breaker tripped](#the-mass-orphan-circuit-breaker-tripped)
- [Commit records were deleted out of band](#commit-records-were-deleted-out-of-band)
- [Queries are missing recently written data](#queries-are-missing-recently-written-data)
- [A process refuses to start](#a-process-refuses-to-start)
- [Readiness, storage and authentication](#readiness-storage-and-authentication)
- [Maintenance is not running, or not finishing](#maintenance-is-not-running-or-not-finishing)
- [Data integrity and correctness alarms](#data-integrity-and-correctness-alarms)
- [Query cost and results](#query-cost-and-results)

## The mass-orphan circuit breaker tripped

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| `increase(ravel_maintain_orphan_breaker_tripped_total[5m]) > 0` | A sweep pass found a large set of data objects whose commit records are gone, which usually means records were deleted out of band rather than that a lot of flushes were abandoned. | The counter increment itself is the confirmation: it only increments on a real trip. `ravel_maintain_orphans_withheld` on `/metrics` gives the size of the most recent withheld set. Re-run the same evaluation without deleting anything with `ravel-cli maintain sweep --tenant <t> --signal <s> --shard <n> --dry-run`. | Restore the missing commit records before the next pass runs. Do not wait for the trip to persist; see below. |

Alert on the **first trip**, with `increase(...) > 0`, not on a sustained
condition. The counter only increments, so any increase is a trip that really
happened, whether or not the shard is still tripping now.

A trip means both of these held on that pass: at least
`orphan_breaker_min_count` orphan candidates were found (default 50), and they
were more than `orphan_breaker_max_ratio` of the shard's listed L0 objects
(default 10%). The pass deleted nothing and halted. The other two sweep rules,
superseded-input and unreferenced-L1, are unaffected and still ran, because they
are anchored on durable records rather than on record absence.

**The trip is not self-clearing in the sense an operator expects.** The
predicate is recomputed from live counts on every pass, with no memory of a
prior trip, so a shard can stop tripping while the missing records are still
missing:

- **Dilution.** New well-recorded writes to the same shard lower the ratio below
  the threshold even though the orphan count has not changed. 55 orphans among
  500 objects trips at 11%; 200 further writes with no data loss at all give
  55/700, which is 7.9% and does not trip, and those same 55 objects are deleted
  on the next pass.
- **Partial restoration.** You restore some but not all of the missing records
  and the remaining count falls below the floor. 55 orphans trips; restoring 6
  leaves 49, under the default floor of 50, so the very next pass stops tripping
  and deletes the other 49 before they were restored.

Relying on the breaker to hold a shard open until every record is back is
relying on a guarantee that does not exist. The only durable way to stop the
deletion is to restore the missing records before the next pass runs. Follow
[commit records were deleted out of band](#commit-records-were-deleted-out-of-band).

**Forcing a pass through a trip.**

```sh
ravel-cli maintain sweep --tenant <t> --signal <metrics|logs|spans> \
  --shard <n> --override-orphan-breaker
```

This runs exactly one overridden pass, deleting the withheld candidates despite
the trip. It applies to that single invocation only: the server never sets it,
and the breaker has no memory across invocations, so an un-overridden pass
afterward evaluates fresh. Use it only after confirming that deletion is safe,
either by restoring records or by independently verifying that the candidates
really are abandoned data. The record-absence signal the orphan rule re-verifies
against is exactly what out-of-band record loss forges.

**What the breaker does not catch.** Four gaps, so you do not read a quiet
breaker as an all-clear:

- It never trips below the count floor regardless of ratio, so total loss on a
  small shard is always deletable in one pass.
- Up to the ratio ceiling of a large shard's objects can be deleted in a single
  pass without ever tripping.
- Dilution and partial restoration can let a pass through the remaining loss, as
  above.
- Each unit is evaluated in isolation, with no cross-shard or cross-tenant
  aggregation, so loss spread thin across many shards can stay under every
  single shard's threshold while the total is large.

The gauge that closes the small-scale gap is `ravel_maintain_orphans_present`,
which carries the most recent pass's total candidate count whether or not the
breaker tripped. Alert on it sustained:

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| `ravel_maintain_orphans_present > 0` for `12h` | Either a handful of commit records lost for one shard, below both breaker thresholds, or a genuinely stuck abandoned flush. | `ravel-cli maintain status --tenant <t> --signal <s> --shard <n> --hour <n>` reports that bucket's L0 record count against what is present. `ravel-cli maintain sweep ... --dry-run` prints the candidate set without deleting it. | Investigate before the grace window elapses (25h by default). If records really are missing, follow the reconstruction procedure below. |

Twelve hours is roughly half the grace window: long enough that one normal
abandoned-flush cleanup between passes does not page, short enough that real
loss alarms with hours to spare. `ravel_maintain_orphans_withheld` is not an
alert target. It reflects only the most recent pass and drops to zero on the
next non-tripping pass, including one that stopped tripping through dilution.

## Commit records were deleted out of band

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| Data that was written is invisible to queries, and the orphan breaker tripped or `ravel_maintain_orphans_present` is nonzero. | Commit records for a shard were removed outside Ravel: an accidental delete, a lifecycle rule on the wrong prefix, a mistyped prefix delete. | `ravel-cli maintain sweep --tenant <t> --signal <s> --shard <n> --dry-run` lists the record-less data objects as orphan candidates. `ravel-cli catalog list --tenant <t> --shards <n>` shows what the catalog still resolves. | Follow the four steps below, in order. Step 1 is not optional. |

The data objects those records named are invisible to readers and, once past the
orphan grace horizon, will be physically deleted by the sweeper. The recovery is
`ravel-cli commit reconstruct`, which rebuilds each record-less L0 data object's
commit record from the object's own footer. **Stop maintenance first**, or the
sweeper's orphan rule deletes the very objects you are trying to reattach while
you reattach them.

1. **Stop maintenance for the tenant.** Stop the `--mode maintain` process
   entirely. This is the one method that reliably protects a tenant under repair
   regardless of its config-record status. `--maintain-tenant` only excludes
   tenants that do not yet carry a durable config record; once a tenant carries
   one, no flag excludes it from maintenance, so restarting restricted to other
   tenants will not keep the sweeper off it. Do not rely on the orphan breaker
   to hold the shard open either: see the entry above.

2. **Reconstruct the missing records**, one shard at a time:

   ```sh
   ravel-cli commit reconstruct --tenant <name> --signal <metrics|logs> --shard <n>
   ```

   It lists the shard's record-less L0 data objects, rebuilds a commit record for
   each from its footer, and writes it create-if-absent. It never overwrites an
   existing record and never deletes. It prints a per-object report of
   reconstructed, already-present and failed, and exits nonzero if any candidate
   failed. Repeat per shard across the affected range.

3. **Verify custody and catalog state** before resuming maintenance:

   ```sh
   ravel-cli maintain verify-custody --tenant <name>
   ravel-cli catalog verify --tenant <name> --signal <signal>
   ```

   `verify-custody` re-hashes every live data object against its key and confirms
   every surviving record's data is present. `catalog verify` re-lists sealed
   records and diffs them against the snapshot for the one signal `--signal`
   names, defaulting to metrics, so run it once per signal the tenant writes.
   Both must exit zero before you trust the repair.

4. **Resume maintenance.** Restart the `--mode maintain` process. The sweeper now
   sees the reconstructed records and treats their data objects as referenced.

Two fields are rebuilt as honest approximations rather than exact copies: the
record's creation time, taken from the data object's own last-modified time
because it is in no footer, and, for logs, the ingest-hour bucket, derived from
the earliest observed sample because log footers do not carry it. The rebuilt
record is a reconstruction, not a claim of byte-for-byte provenance.
Reconstruction also does not detect bit rot: it rebuilds a record describing
whatever bytes are currently stored. Use `verify-custody` for the content-hash
check.

## Queries are missing recently written data

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| A query over a recent window returns fewer series or rows than were written, and the same query with an explicit minimum commit token returns them. | A folder whose clock ran fast beyond its seal margin sealed an hour before every writer's flush for it had landed, so a commit published into the already-sealed bucket is invisible to snapshot-reading queries. A hand fold with `--max-flush-lifetime 0s` run while a writer was live does the same thing. | `ravel-cli catalog verify --tenant <name> --signal <signal>` exits nonzero with a nonempty "missing from snapshot" count. `increase(ravel_scrub_seal_divergence_total[1h]) > 0` is the scheduled form of the same check. | Rebuild the snapshot with the four steps below. |

A query that pins an exact commit token is unaffected, because it reads that
commit key directly rather than through the snapshot. That asymmetry is the
quickest way to tell this apart from data that was never written.

1. Run `ravel-cli catalog verify --tenant <name> --signal <signal>`, once per
   signal the tenant writes. A nonzero exit with a nonempty missing count
   confirms sealed commits the snapshot does not know about.
2. Delete the tenant's HEAD object for the affected signal,
   `t/<tenant_hash_hex>/catalog/<signal>/HEAD`. There is no `ravel-cli`
   subcommand for this; use the store's own tooling (`mc rm` against MinIO,
   `aws s3 rm` against S3). Deleting HEAD is safe: an absent HEAD means "no
   snapshot yet", and the next fold rebuilds one from a full listing rather than
   failing.
3. Run `ravel-cli catalog fold --tenant <name> --shards <n> --signal <signal>`,
   or wait for the next background fold tick. The report's `rebuilt: true` line
   confirms it rebuilt from scratch rather than extending the prior snapshot.
4. Re-run `ravel-cli catalog verify` to confirm the divergence is gone.

There is no force-rebuild flag. Deleting HEAD is the supported way to force one,
because it reuses the same absent-HEAD path a brand-new tenant takes on its
first fold.

Then fix the cause: review the folder host's clock, or the seal margins you
changed. See
[the seal margin](maintenance.md#the-seal-margin-and-why-it-matters).

## A process refuses to start

Every refusal below is a hard error before any listener binds. None of them is
transient and none clears on restart, because in each case object storage
records the true value and the process configuration disagrees with it.

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| Startup error naming a tenant, a signal, and an expected and actual shard count. | This process was configured with a different `--shards` than that tenant's data was written under. | The error text names all four values. `ravel-cli catalog list --tenant <t> --shards <n>` against the recorded value resolves records; against the wrong one it does not. | Set `--shards` back to the recorded value. Never lower it below what a tenant already used: resolution iterates `0..N`, so a lower value hides every series in the missing shards, which is why it is refused. |
| Startup error naming a configured and a stored garbage-collection value and the rule violated. | A `--gc-*` flag disagrees with the durable `sys/gc` object. In maintain mode the horizon and grace must be equal to the stored values, not merely satisfy the inequality. | `ravel-cli gc-config show` prints the stored values and whether the bucket is bootstrapped. | Align the flags with the stored object, or change the object deliberately with `ravel-cli gc-config set` and then bring every mode's flags into line. A query deadline above the stored maximum is rejected, never clamped. |
| Startup error saying the store is not qualified, or that its qualification is stale. | A fresh bucket has never been qualified, or its record predates this binary's required suite floor. | The two conditions are distinct named errors in the startup output. | Run `ravel-cli store qualify --store s3 ...` against the bucket, then start the server. On a stale record, re-run it with a current build. |
| Startup error that a fresh bucket needs a tenant-hash key. | A fresh bucket was started with neither `--tenant-hash-key-file` nor `--tenant-hash-unkeyed`. Keyed is the default and the choice is permanent for the bucket. | The error names both flags. | Pass the key file, or pass `--tenant-hash-unkeyed` if you intend the unkeyed scheme. Decide deliberately: there is no migration between the two. |
| Startup error that the configured key's fingerprint disagrees with the bucket marker. | The wrong deployment key was mounted. | `ravel-cli tenancy show --tenant-hash-key-file <path>` verifies a key against a bucket offline, without starting a server. | Mount the right key. A wrong key is a failed deploy, not a second namespace: do not work around it by switching schemes. |
| Startup error naming `--distributed-query` and a missing key file. | Distributed reads were enabled without `--fragment-key-file`. | The error names both flags. | Provide the key file. It holds 32-byte keys, one per non-empty line, each line 64 hexadecimal characters; a file with no key line, or a line of another length, fails startup with a line number. |
| Startup error that bucket protection is disabled, or a versioning alarm. | `--require-bucket-protection` is set and the backend affirmatively reported Object Lock or versioning off. | The error names the probe result. | Configure the bucket protection at the bucket layer, or drop the flag if this is a development deployment. An unknown probe result warns and starts; only a disabled one refuses. |
| Startup error that a retention window is below the floor. | `--retention-default` or `--retention-tenant` is shorter than the ingest lag, flush lifetime, skew allowance and one bucket span combined. | The error names the configured window and the floor. | Raise the window. It is refused rather than clamped, so that a bucket can never be tombstoned before it is sealed. |
| Startup error naming a key in the admission limits file. | The file is not valid TOML, names an unknown key, has an empty tenant id, a zero or negative count, or a burst with no rate to pair with. | The error names the offending table and key. | Fix the file. Validation is fail-closed on purpose: a bad file never silently falls back to the shipped defaults. |
| Startup error naming two conflicting credential flags. | `--s3-auth instance-role` was combined with a static credential flag. An exported `RAVEL_S3_ACCESS_KEY` counts. | The error names both the auth mode and the offending flag. | Remove the static credential, including from the environment. |
| Startup error that `--mtls-enabled` requires `--mtls-listener`, or that two listener addresses are equal. | The mTLS resolver is installed only on its own listener, and each dedicated listener must bind a distinct address. | The error names the flags and the colliding address. | Give the mTLS and fragment listeners their own addresses. |

## Readiness, storage and authentication

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| `/readyz` returns 503 across the fleet and the load balancer has taken it out. | The background store probe has failed four consecutive reads of `sys/tenancy`. | `ravel_store_reachable == 0` on `/metrics`, and `ravel_store_probe_failures_total` rising. `curl -sS -o /dev/null -w '%{http_code}' http://<host>/readyz` returns 503 with no store call of its own. | Fix the store or the credential. Readiness recovers on the first successful probe, without a restart. Do not lower the threshold: it is a fixed constant precisely so a single blip cannot eject a fleet. Liveness is deliberately unaffected, so processes are not being restarted under you. |
| A deployment gated on readiness has halted mid-roll. | The same condition. Readiness now reflects store reachability, so a rollout correctly stops while the store is unreachable. | As above. | Resolve the store outage; the roll resumes. |
| `ravel_bucket_protection_unknown == 1`. | `--require-bucket-protection` is on and the backend cannot answer the Object Lock and versioning query. Every backend reachable only through the object-store contract reports this today. | The gauge, plus the single startup warning that accompanies it. | Not necessarily a misconfiguration, but the platform cannot see the protection it depends on. Confirm the bucket settings out of band, at the provider console or with the provider CLI. |
| `multipart_abort_failures` or `multipart_uploads_unreaped` rising. | Multipart uploads are ending without a confirmed abort, so orphaned parts may be accumulating and billed. | The two store counters on `/metrics`. | Confirm the bucket has an `AbortIncompleteMultipartUpload` lifecycle rule with a cleanup period of seven days or less. Nothing in Ravel reaps those parts, so that rule is the only thing bounding the cost. |
| `increase(ravel_durable_auth_refresh_failures_total[15m]) > 0`. <a id="durable-auth-refresh-is-failing"></a> | The background refresh cannot read or decode `sys/auth`: most often the storage credential broke or lost read on that key, or the object is corrupt or was written under a different deployment key. | The counter, labeled by mode, on `/metrics`. The cached map is still serving, so requests are not failing yet. | Fix the credential or the object now. This is the early warning: the staleness gate is not advancing, and token resolution fails closed once the hard-stale horizon passes. |
| `increase(ravel_durable_auth_stale_fail_closed_total[5m]) > 0`. | The cached token map is already past the hard-stale bound and durable tokens are being refused. | The counter on `/metrics`, and clients receiving authentication failures for tokens that used to work. | This is the cliff the previous row exists to keep you off. Restore `sys/auth` readability; resolution recovers on the first successful refresh. |
| `increase(ravel_provisioning_shard_count_mismatch_total[5m]) > 0`. | A dynamically resolved tenant, from OIDC or mTLS, failed its provisioning check: either a real shard-count disagreement against the durable record, or an unreadable record caught on the maintenance loop, which skips that tenant's tick. | The counter on `/metrics`. Unlike a statically known tenant, this does not take the process down: the one request fails with a typed error. | Reconcile the configuration against the durable record for that tenant. Any nonzero increase is a configuration-against-data problem, not a rate to threshold. |
| `ravel_tenancy_v1_unkeyed_adoptions_total` incremented unexpectedly. | A bucket with data and no tenancy marker was adopted as unkeyed, permanently. | The counter, and the accompanying log line naming the adoption. | If that bucket was meant to be keyed, stop: the adoption is permanent and there is no migration. Start a fresh keyed bucket and drain into it. |

## Maintenance is not running, or not finishing

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| Storage keeps growing, retention deletes nothing, object counts per hour stay in the thousands. | There is no `--mode maintain` process. Ingest, query, the catalog fold and alert evaluation all run without one. | No process is running with `--mode maintain`. `ravel-cli maintain status --tenant <t> --signal <s> --shard <n> --hour <n>` reports a sealed, uncompacted bucket with a high L0 record count. | Deploy a maintain process. See [Maintenance](maintenance.md). |
| `ravel_maintain_workers_live == 0` while a maintain process is up. | The process cannot see itself as live: a heartbeat write persistently failing, or a liveness read persistently erroring. It then owns no units. | The gauge on that process's `/metrics`. | Fire on the level, not on an increase; there is no counter here. Check the maintain role's write and list grants on the coordination prefix. Without them the first heartbeat write fails closed with an access error. |
| `ravel_maintain_tenants_maintained < ravel_maintain_tenants_discovered` for `10m`. | A tenant prefix holds data with no maintaining owner. | The two gauges on `/metrics`. Ten minutes is two cycles at the default interval, long enough that a restart or a tenant mid-onboarding does not page. | For a dynamically resolved tenant, add it with `--maintain-tenant`. Otherwise investigate why ownership is not covering it. |
| `increase(ravel_maintain_tenant_discovery_failures_total[5m]) > 0`. | A tenant listing failed, which skips the entire cycle for every tenant, not just one. | The counter on `/metrics`. | Treat as a full maintenance outage and act on the first occurrence. The supervisor deliberately never treats a failed enumeration as "no tenants", so nothing is being maintained at all while this persists. Check the maintain credential's list grant, including the bare tenant-prefix entry. |
| `ravel_maintain_units_stalled > 0` for `30m`. | One unit's last several ticks all failed with no intervening success. A single success resets its streak, so this is the same unit failing, not a rotating cast of transient faults. | The gauge on `/metrics`. Reproduce the failing pass by hand with `ravel-cli maintain compact-tenant --tenant <t> --signal <s> --dry-run`, which prints each bucket's own outcome and error line. | A blip during a store hiccup clears itself within a cycle or two; a stall that survives multiple intervals needs an operator. Thirty minutes covers several cycles at the default interval. |
| `increase(ravel_maintain_conservation_aborts_total[15m]) > 0`. | A compaction publish was refused because input and output record counts disagreed. Nothing was written. | The counter, labeled by signal, on `/metrics`. `ravel-cli maintain compact-bucket --tenant <t> --signal <s> --shard <n> --hour <n> --dry-run` recomputes the same plan without writing. | A bucket stuck retrying every tick without ever compacting needs an operator rather than another retry. |
| `increase(ravel_maintain_legal_hold_refresh_failures_total[15m]) > 0`. | The maintenance loop could not read the hold records, so it skipped that tenant's tick entirely. | The counter on `/metrics`. `ravel-cli hold list --tenant <id>` reads the same records from the CLI. | A sustained failure means a tenant is silently receiving no maintenance at all. Fix the read path before anything else, because the skip is the fail-closed behavior working as intended. |
| A legal hold was set but data was still deleted. | The hold was set after the current tick's snapshot refresh. Each tick refreshes its hold snapshot once, before its destructive pass. | `ravel-cli hold list --tenant <id>` shows whether the scope is recorded. | Confirm with `hold list` after every urgent `hold set`. `hold set` returning success means the record was written, not that a pass has picked it up. The exposure window is one maintenance interval. |
| `ravel_scrub_cursor_position` stuck near 0 for longer than the scrub period. | The scrubber is not keeping pace with `--scrub-period`, so the effective staleness bound is no longer that period. | The gauge, per signal, on the maintain process's `/metrics`. | Lengthen `--scrub-period`, or give the maintain process more read bandwidth. Sustained scrub bandwidth is the corpus size divided by the period. |

## Data integrity and correctness alarms

Page on any nonzero increase for all three of these. None is a rate to
threshold.

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| `increase(ravel_scrub_checksum_mismatch_total[1h]) > 0`. | At-rest corruption: a whole-object hash mismatch against the recorded content hash, from bit rot or a partial write, or a section checksum failure. | The counter, labeled by signal, on `/metrics`. Confirm the extent with `ravel-cli maintain verify-custody --tenant <t>`, which re-hashes every live data object and exits nonzero on any anomaly. | There is no redundant copy to repair from, and the scrubber only detects. Identify the affected objects with `verify-custody`, then restore from your bucket-level controls. See [disaster recovery](../disaster-recovery.md). |
| `increase(ravel_scrub_postings_disagreement_total[1h]) > 0`. | A covering name-index object omitted a name the data object really carries, so a query filtering on that name silently skips matching data. | The counter, labeled by signal, on `/metrics`. | A correctness defect, not a capacity problem. Capture the affected tenant and signal and escalate; a query result computed while this is nonzero cannot be trusted for that name. |
| `increase(ravel_scrub_seal_divergence_total[1h]) > 0`. | The folded snapshot under-counts the sealed commit history. `reason="missing"` is a sealed record absent from the snapshot; `reason="mismatched"` is a snapshot entry whose content hash disagrees with the record. | The counter, labeled by signal and reason, on `/metrics`. `ravel-cli catalog verify --tenant <t> --signal <s>` is the same check on demand. | Follow [queries are missing recently written data](#queries-are-missing-recently-written-data). A snapshot entry with no surviving commit record is the expected shape once retention deletes a folded record and is never counted here, so any increase is real. |
| `increase(ravel_catalog_isolation_breach_total[5m]) > 0`. | A cross-tenant key-layout or hashing fault: a tenant-hash mismatch on a catalog HEAD or index object, or a resolve listing result whose key does not begin with the requesting tenant's prefix. | The counter, labeled by mode, on `/metrics`. Every increment corresponds to a query that already failed with an explicit isolation-fault error, so the client saw it too. | Escalate immediately. Unlike the two anomaly counters rendered beside it, which tally an overlap the query resolves past, every increment here is a failed query and a real isolation fault. Coverage is not complete: a mismatch on a commit or compaction record fails its query without incrementing this counter, and a snapshot part's own tenant hash is not checked against the requester at all, so a zero reading is not proof of isolation. |

## Query cost and results

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| Queries over recent history are slow and issue far more object-store requests than the tenant has objects. | The catalog fold is disabled or behind, so resolve is listing commit records per bucket instead of reading a snapshot. | `ravel-cli catalog inspect --tenant <t> --signal <s>` prints the watermark; compare it against the window being queried. It reports rather than errors when no HEAD exists, which is what a logs tenant inspected without `--signal logs` looks like. | Fold the affected signal with `ravel-cli catalog fold --tenant <t> --shards <n> --signal <s>`, and check that `--disable-fold` is not set. The listing path does not scale past roughly 10,000 commit records in one bucket. |
| A tenant's logs or spans queries are slow while its metrics queries are fine. | Only metrics were folded. Each signal has its own snapshot object, and both `catalog fold` and `catalog verify` default to metrics. | `ravel-cli catalog inspect --tenant <t> --signal logs` reports no HEAD. | Fold and verify each signal the tenant actually writes, naming `--signal` every time. |
| A wide scan fails or truncates on a tenant with a lot of sealed history. | The per-query segment cap. Only the recent set, roughly the last two hours, is exempt from it. | The query error names the cap. | Raise `--max-segments` for that workload, or narrow the query window. See [per-query budgets](configuration.md#per-query-budgets). |
| Duplicate samples at the same timestamp for one series. | Delivery is at-least-once. A client retry after a lost acknowledgement re-ingests the same points, and both copies are stored; a query takes the last value at a given timestamp. | Compare the client's retry log against the ingested points for that window. | Expected behavior, not a defect. Log and span ingest accept an optional `x-ravel-idempotency-key` header that collapses a retried request within its dedup window; use it on a client that retries. Metrics have no such header. |
| `ravel_typed_attr_columns_stale_fallback_total` climbing steadily. | The tenant config object is unreadable, so the typed attribute column declarations in effect are not the ones written. | The counter, labeled by mode, on `/metrics`. `ravel-cli typed-attr-column show <tenant>` reads the durable record directly and shows whether it is present, empty or absent. | A brief rise right after a config write is expected, because resolution is cache-aside on a 60 second horizon. A counter that keeps climbing is a read failure to fix. |
| Two replicas answer the same query against different typed attribute columns. | A `typed-attr-column set` is propagating. Resolution is per tenant on a 60 second staleness horizon. | `ravel-cli typed-attr-column show <tenant>` shows the durable state; the divergence closes within the horizon. | Wait out the horizon. If it does not close, treat it as the row above. |

## Background

Decision records behind this page:
[maintenance safety and coverage](../../adrs/0048-maintenance-safety-and-coverage.md),
[commit record reconstruction and the disaster-recovery posture](../../adrs/0058-commit-record-reconstruction-and-dr-posture.md),
[durability hardening](../../adrs/0059-durability-hardening.md),
[fail-closed isolation and startup invariants](../../adrs/0050-fail-closed-isolation-and-startup-invariants.md),
[leased distributed maintenance](../../adrs/0065-leased-distributed-maintenance.md),
and [query cost accounting](../../adrs/0044-query-cost-accounting.md).
