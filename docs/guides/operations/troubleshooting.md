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
| `increase(ravel_maintain_orphan_breaker_tripped_total[5m]) > 0` | A sweep pass found a large set of data objects whose commit records are gone, which usually means records were deleted out of band rather than that a lot of flushes were abandoned. | The counter increment itself is the confirmation: it only increments on a real trip. A trip can only happen on a tick that ran candidate selection, which is the full-sweep cadence (`interior_reverify_ns`, 6 hours by default), not every tick. `ravel_maintain_orphans_withheld` on `/metrics` gives the size of the set withheld by the last such pass, and a tick that skipped selection leaves it unchanged. Re-run the same evaluation without deleting anything with `ravel-cli maintain sweep --tenant <t> --signal <s> --shard <n> --dry-run`. | Restore the missing commit records before the next pass runs. Do not wait for the trip to persist; see below. |

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
breaker tripped.

"Most recent pass" means the most recent pass that actually ran the orphan
rule. Orphan candidate selection runs on the full-sweep cadence
(`interior_reverify_ns`, default 6 h), not on every maintain tick (default
300 s); the ticks in between skip the rule entirely and report nothing about
orphans, so they leave the gauge alone rather than resetting it to zero. The
gauge therefore reports the last completed orphan pass and is refreshed once
per full-sweep interval. Read a change in its value as "the last orphan pass
found this", never as "as of this scrape"; a value can be up to one
full-sweep interval old, and a genuine return to zero shows up on the next
pass that runs, not on the next tick.

The gauge also reports only one unit per tick: it is labelled by mode and
signal while the sweep runs per tenant, signal and shard, so each pass
overwrites the same series and a unit that measured zero can mask another
unit's nonzero measurement.

Alert on it sustained:

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| `ravel_maintain_orphans_present > 0` for `12h` | Either a handful of commit records lost for one shard, below both breaker thresholds, or a genuinely stuck abandoned flush. | `ravel-cli maintain status --tenant <t> --signal <s> --shard <n> --hour <n>` reports that bucket's L0 record count against what is present. `ravel-cli maintain sweep ... --dry-run` prints the candidate set without deleting it. | Investigate before `grace + max_flush_lifetime` elapses (25 h on the defaults: 24 h plus 1 h). If records really are missing, follow the reconstruction procedure below. |

Twelve hours is roughly half the grace window: long enough that one normal
abandoned-flush cleanup between passes does not page, short enough that real
loss alarms with hours to spare. It is also comfortably longer than the
default 6 h full-sweep interval that refreshes the gauge, so a sustained
alert window always spans at least one orphan pass; keep that relationship
if you raise `interior_reverify_ns`, or the window can close on a single
stale sample. `ravel_maintain_orphans_withheld` is not an
alert target. It reflects only the most recent pass that ran the orphan rule
and drops to zero on the
next non-tripping pass, including one that stopped tripping through dilution.

## Commit records were deleted out of band

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| Data that was written is invisible to queries, and the orphan breaker tripped or `ravel_maintain_orphans_present` is nonzero. | Commit records for a shard were removed outside Ravel: an accidental delete, a lifecycle rule on the wrong prefix, a mistyped prefix delete. | `ravel-cli maintain sweep --tenant <t> --signal <s> --shard <n> --dry-run` lists the record-less data objects as orphan candidates. `ravel-cli catalog list --tenant <t> --shards <n>` shows what the catalog still resolves. | Follow the five steps below, in order. Step 1 is not optional. |

The data objects those records named are invisible to readers. Once past the
orphan grace horizon the sweeper moves them out of the live keyspace to a
`quarantine/` prefix rather than deleting them, and a second reaper deletes the
quarantine copy once it is older than `quarantine_horizon_ns` (default 7 days).
So there are two clocks to beat, not one: after the grace horizon the object is
no longer where `commit reconstruct` looks for it, and after the quarantine
horizon it is gone for good.

The recovery is `ravel-cli commit reconstruct`, which rebuilds each record-less
L0 data object's commit record from the object's own footer. It reads the live
L0 prefix only, so anything already quarantined has to be copied back first
(step 2 below). **Stop maintenance first**, or the sweeper's orphan rule
quarantines the very objects you are trying to reattach while you reattach
them.

1. **Stop maintenance for the tenant.** Stop the `--mode maintain` process
   entirely. This is the one method that reliably protects a tenant under repair
   regardless of its config-record status. `--maintain-tenant` only excludes
   tenants that do not yet carry a durable config record; once a tenant carries
   one, no flag excludes it from maintenance, so restarting restricted to other
   tenants will not keep the sweeper off it. Do not rely on the orphan breaker
   to hold the shard open either: see the entry above.

2. **Restore anything already quarantined.** List the tenant's quarantine
   prefix and compare it against what the sweep reported as orphan candidates:

   ```sh
   aws s3 ls --recursive s3://<bucket>/quarantine/t/<tenant_hash>/
   ```

   Each entry is `quarantine/<original key>/q<quarantined_at_ns>`. Recover the
   live key by stripping the `quarantine/` prefix and the trailing `/q<ns>`
   segment, then copy the object back to it. The original key is preserved
   verbatim in between, so the transform is textual and needs no lookup. Copy,
   do not move, until step 4 has passed: the quarantine copy is the only other
   copy that exists.

   There is no `ravel-cli` command for this yet, so it is an object-store
   operation against whatever tooling the bucket takes. Objects whose
   quarantine timestamp is older than `quarantine_horizon_ns` are already gone
   and are not recoverable from here.

3. **Reconstruct the missing records**, one shard at a time:

   ```sh
   ravel-cli commit reconstruct --tenant <name> --signal <metrics|logs> --shard <n>
   ```

   It lists the shard's record-less L0 data objects, rebuilds a commit record for
   each from its footer, and writes it create-if-absent. It never overwrites an
   existing record and never deletes. It prints a per-object report of
   reconstructed, already-present and failed, and exits nonzero if any candidate
   failed. Repeat per shard across the affected range.

4. **Verify custody and catalog state** before resuming maintenance:

   ```sh
   ravel-cli maintain verify-custody --tenant <name>
   ravel-cli catalog verify --tenant <name> --signal <signal>
   ```

   `verify-custody` re-hashes every live data object against its key and confirms
   every surviving record's data is present. `catalog verify` re-lists sealed
   records and diffs them against the snapshot for the one signal `--signal`
   names, defaulting to metrics, so run it once per signal the tenant writes.
   Both must exit zero before you trust the repair.

5. **Resume maintenance.** Restart the `--mode maintain` process. The sweeper now
   sees the reconstructed records and treats their data objects as referenced.
   Once it does, delete the quarantine copies you restored from in step 2; the
   reaper leaves them until their own horizon otherwise.

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
   subcommand for this; use the store's own tooling (`aws s3 rm`, with
   `--endpoint-url` against RustFS or any other S3-compatible store).
   Deleting HEAD is safe: an absent HEAD means "no
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
transient and none clears on restart. Most are a disagreement between the
process configuration and what object storage records as true, but not all:
a statically-known tenant's provisioning record with a structurally invalid
generation history (`CorruptGenerations`) refuses startup on its own, with no
configuration value to disagree with -- the record itself cannot be trusted
to route on.

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| Startup error that adopting a tenant's data would hide it, naming a tenant, a signal, and an observed shard index at or above the configured `--shards`. | This process was configured with a lower `--shards` than a tenant's pre-record data already used, so writing a record at the configured value would leave existing series in higher shards unroutable. | The error names the observed index and the configured value. `ravel-cli catalog list --tenant <t> --shards <n>` against the higher value resolves those records. | Raise `--shards` to cover every observed shard index for that tenant, or run `ravel-cli provision adopt` at the correct value first. A plain difference between the live default and an already-provisioned tenant's recorded count no longer refuses startup: that tenant keeps its recorded count and routes over it. |
| Startup error `CorruptGenerations`, naming a tenant, a signal, and the specific structural defect (for example `ScalarMismatch` or `FirstActivationNonzero`). | A statically-known tenant's provisioning record decoded, but its `generations` history fails a structural invariant: the scalar `shard_count` disagrees with generation 0's count, generation 0 is not at `activation_hour` 0, the history is not dense or not activation-increasing, or a generation's count is out of range or a no-op repeat. `validate_static_provisioning` propagates the error before any listener binds; the same defect fails a dynamic tenant's first ingest touch and increments `ravel_provisioning_shard_count_mismatch_total`, and it skips that tenant's maintenance tick if the maintain loop hits it first. | The error names the tenant, signal, and defect variant. | The record needs manual repair or the tenant needs re-provisioning; there is no live-configuration flag that fixes this. Restore a valid `generations` history for that (tenant, signal) object, or delete the record and re-run `ravel-cli provision adopt` if it is safe to re-derive shard_count from currently observed data. |
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
| Startup error that `--mtls-listener` does not bind a loopback address and requires `--mtls-trust-forwarded-header`. | The resolver believes a proxy-forwarded header rather than verifying a certificate, and only a loopback bind proves by topology that a local proxy is the header's only possible source. | The error names the bound address and the flag. | Bind the listener to loopback, or add `--mtls-trust-forwarded-header` to assert that a verifying proxy fronts the address and no client can reach it directly. The flag turns nothing on; it records the deliberate choice. |
| Startup error that `--distributed-query` requires `--advertise-fragment-endpoint` because a published listener binds an unspecified address. | Under distributed query the process publishes its bound listener addresses for siblings to dial, and no peer can dial `0.0.0.0` or `::`. | The error names each offending listener flag with its address. | Pass `--advertise-fragment-endpoint <host[:port]>` with a host peers can reach, or bind the listener to a specific address. Without it the failure is silent: siblings fail at connect and fall back to local execution. |
| Startup error that `--fragment-tls-cert` is missing the `serverAuth` or `clientAuth` extended key usage, or both. | The fragment listener is mutual TLS and one certificate serves both directions, so a `serverAuth`-only certificate (what releases before this one documented) cannot dial a peer, a `clientAuth`-only one cannot be dialled, and `anyExtendedKeyUsage` alone satisfies neither verifier. | The error names the certificate path, which usage is missing, and the usages it does carry. `openssl x509 -in <cert> -noout -ext extendedKeyUsage` shows the same. | Reissue the certificate with `extendedKeyUsage = serverAuth, clientAuth` (cert-manager: `usages: [server auth, client auth]`) and restart. Without the refusal the process would start, serve the direction it does carry, and fail every handshake in the other one, falling back to coordinator-local execution with nothing reporting why. |
| Startup error that `--advertise-fragment-endpoint` carries a port but no `--fragment-listener` is configured. | In the combined layout both published endpoints are served by the public gRPC listener, and the port half of the flag reaches the fragment endpoint only. The Flight SQL endpoint would keep the bound port, so one of the two endpoints published for the same socket would be wrong. | The error names the value as written and the gRPC listener address both lanes share. | Advertise a host only, which keeps each listener's own bound port, or configure `--fragment-listener` so the fragment lane has its own port to map. |

## Readiness, storage and authentication

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| `/readyz` returns 503 across the fleet and the load balancer has taken it out. | The background store probe has failed four consecutive reads of `sys/tenancy`. | `ravel_store_reachable == 0` on `/metrics`, and `ravel_store_probe_failures_total` rising. `curl -sS -o /dev/null -w '%{http_code}' http://<host>/readyz` returns 503 with no store call of its own. | Fix the store or the credential. Readiness recovers on the first successful probe, without a restart. Do not lower the threshold: it is a fixed constant precisely so a single blip cannot eject a fleet. Liveness is deliberately unaffected, so processes are not being restarted under you. |
| A deployment gated on readiness has halted mid-roll. | The same condition. Readiness now reflects store reachability, so a rollout correctly stops while the store is unreachable. | As above. | Resolve the store outage; the roll resumes. |
| A single process reports `/readyz` 503 while the store is reachable and the fleet is otherwise healthy. | An ingest shard actor on that process was condemned: a flush kept panicking, most often a poison-pill input for one shard, not a transient fault. On the metrics pipeline this follows exhausting the respawn budget (`MAX_SHARD_RESPAWNS`, 3 deaths within one decay window); the logs and spans pipelines never respawn, so the first shard-actor death condemns immediately. A condemned shard cannot recover in-process. | `shards_condemned > 0` on `/metrics` for that process (check the `signal` label to find the pipeline), with `ravel_store_reachable == 1`. On the metrics signal `shard_deaths` will have climbed to at least the respawn budget on that shard first; on logs or spans a single `shard_deaths` is enough. Writes to the condemned shard return the typed shard-unavailable error. | Nothing replaces the process on its own: a 503 at `/readyz` only removes the pod from its Service endpoints, and `/healthz` stays 200 by design, so the pod is not restarted or rescheduled and will sit condemned indefinitely. Roll it yourself: first capture the panicking flush from that shard's error logs (if one tenant or series is the poison pill, a replacement condemns just as fast), then `kubectl delete pod <pod>` (or `kubectl rollout restart deployment/<name>` for the whole set). A fresh process starts with all shards live. On the metrics signal, `shard_deaths` alone (with `shards_condemned == 0`) is transient respawn recovery, not a page. |
| `ravel_bucket_protection_unknown == 1`. | `--require-bucket-protection` is on and the backend cannot answer the Object Lock and versioning query. Every backend reachable only through the object-store contract reports this. | The gauge, plus the single startup warning that accompanies it. | Not necessarily a misconfiguration, but the platform cannot see the protection it depends on. Confirm the bucket settings out of band, at the provider console or with the provider CLI. |
| Orphaned multipart uploads accumulating in the bucket. | Multipart uploads are ending without a confirmed abort, so parts may be left behind and billed. Ravel tracks this internally but does not yet export it on `/metrics`. | List multipart uploads directly against the bucket, for example `aws s3api list-multipart-uploads --bucket <bucket>`, or your provider's storage console. | Confirm the bucket has an **enabled** `AbortIncompleteMultipartUpload` lifecycle rule with a cleanup period of seven days or less, and that its scope actually covers Ravel's uploads: an empty prefix so the rule applies to the whole bucket, or a set of prefixes that together cover every `t/` prefix. A rule that is present but disabled, or scoped to a prefix Ravel never writes, passes a "does a rule exist" check while reaping nothing, and the parts keep accumulating and keep being billed. Nothing in Ravel reaps those parts, so that rule is the only thing bounding the cost. The [disaster recovery](../disaster-recovery.md) guide's platform-CLI checklist gives the exact `get-bucket-lifecycle-configuration` query, gated on `Status==Enabled` and that same whole-bucket-or-every-`t/`-prefix scope. |
| `increase(ravel_durable_auth_refresh_failures_total[15m]) > 0`. <a id="durable-auth-refresh-is-failing"></a> | The background refresh cannot read or decode `sys/auth`: most often the storage credential broke or lost read on that key, or the object is corrupt or was written under a different deployment key. | The counter, labeled by mode, on `/metrics`. The cached map is still serving, so requests are not failing yet. | Fix the credential or the object now. This is the early warning: the staleness gate is not advancing, and token resolution fails closed once the hard-stale horizon passes. |
| `increase(ravel_durable_auth_stale_fail_closed_total[5m]) > 0`. | The cached token map is already past the hard-stale bound and durable tokens are being refused. | The counter on `/metrics`, and clients receiving authentication failures for tokens that used to work. | This is the cliff the previous row exists to keep you off. Restore `sys/auth` readability; resolution recovers on the first successful refresh. |
| `increase(ravel_provisioning_shard_count_mismatch_total[5m]) > 0`. | A tenant failed its provisioning check hard: an unreadable record (corrupt or a future format version), a decodable record whose generation history fails its structural invariants (`CorruptGenerations`, see "A process refuses to start" above), or pre-ADR data a lower `shard_count` would hide. Caught on a dynamic tenant's first touch, or on the maintenance loop's per-tenant gate. A recorded count that merely differs from the live `--shards` default is no longer counted here; it is tolerated and tallied by `ravel_provisioning_shard_count_drift_total` instead. | The counter on `/metrics`. The two sources have different blast radii and neither takes the process down: a first-touch failure fails only the triggering request (a query resolving the tenant or an ingest write) with a typed error; a maintenance-loop failure fails no request at all, but skips that tenant's maintenance tick for the cycle. Correlate against ingest error logs versus `ravel_maintain_units_stalled` and the maintain pass log to tell which source fired. | Read the durable record for that tenant. The remedy depends on which failure it was: a future-format record needs the binary upgraded to one that reads that version; a corrupt record, or one whose generation history fails its structural invariants, needs the record restored from a known-good copy or the tenant safely re-provisioned (an upgrade does not repair persisted history); a would-hide-data case needs `--shards` raised to cover the tenant's observed shards. Any nonzero increase is a real problem, not a rate to threshold. If the source was the maintenance loop, do not assume maintenance continues for that tenant while the record is broken: retention, compaction, and everything else the loop drives are paused for it until the record is fixed. |
| `ravel_tenancy_v1_unkeyed_adoptions_total` incremented unexpectedly. | A bucket with data and no tenancy marker was adopted as unkeyed, permanently. | The counter, and the accompanying log line naming the adoption. | If that bucket was meant to be keyed, stop: the adoption is permanent and there is no migration. Start a fresh keyed bucket and drain into it. |

## Maintenance is not running, or not finishing

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| Storage keeps growing, retention deletes nothing, object counts per hour stay in the thousands. | There is no `--mode maintain` process. Ingest, query and alert evaluation all run without one; since ADR-1693 the scheduled catalog fold does not, so queries also pay listing cost for the whole unsealed span. | No process is running with `--mode maintain`. `ravel-cli maintain status --tenant <t> --signal <s> --shard <n> --hour <n>` reports a sealed, uncompacted bucket with a high L0 record count. | Deploy a maintain process. See [Maintenance](maintenance.md). |
| `ravel_maintain_workers_live == 0` while a maintain process is up. | The process cannot see itself as live: a heartbeat write persistently failing, or a liveness read persistently erroring. It then owns no units. | The gauge on that process's `/metrics`. | Fire on the level, not on an increase; there is no counter here. Check the maintain role's write and list grants on the coordination prefix. Without them the first heartbeat write fails closed with an access error. |
| `ravel_maintain_tenants_maintained < ravel_maintain_tenants_discovered` for `10m`. | A tenant prefix holds data with no maintaining owner. | The two gauges on `/metrics`. Ten minutes is two cycles at the default interval, long enough that a restart or a tenant mid-onboarding does not page. | For a dynamically resolved tenant, add it with `--maintain-tenant`. Otherwise investigate why ownership is not covering it. |
| `increase(ravel_maintain_tenant_discovery_failures_total[5m]) > 0`. | A tenant listing failed, which skips the entire cycle for every tenant, not just one. | The counter on `/metrics`. | Treat as a full maintenance outage and act on the first occurrence. The supervisor deliberately never treats a failed enumeration as "no tenants", so nothing is being maintained at all while this persists. Check the maintain credential's list grant, including the bare tenant-prefix entry. |
| `ravel_maintain_units_stalled > 0` for `30m`. | One unit's last several ticks all failed with no intervening success. A single success resets its streak, so this is the same unit failing, not a rotating cast of transient faults. | The gauge on `/metrics`. Reproduce the failing pass by hand with `ravel-cli maintain compact-tenant --tenant <t> --signal <s> --dry-run`, which prints each bucket's own outcome and error line. | A blip during a store hiccup clears itself within a cycle or two; a stall that survives multiple intervals needs an operator. Thirty minutes covers several cycles at the default interval. |
| `increase(ravel_maintain_conservation_aborts_total[15m]) > 0`. | A compaction publish was refused because input and output record counts disagreed. Nothing was written. | The counter, labeled by signal, on `/metrics`. `ravel-cli maintain compact-bucket --tenant <t> --signal <s> --shard <n> --hour <n> --dry-run` recomputes the same plan without writing. | A bucket stuck retrying every tick without ever compacting needs an operator rather than another retry. |
| `increase(ravel_maintain_legal_hold_refresh_failures_total[15m]) > 0`. | The maintenance loop could not read the hold records, so it skipped that tenant's tick entirely. | The counter on `/metrics`. `ravel-cli hold list --tenant <id>` reads the same records from the CLI. | A sustained failure means a tenant is silently receiving no maintenance at all. Fix the read path before anything else, because the skip is the fail-closed behavior working as intended. |
| A legal hold was set but data was still deleted. | The hold was set after the current tick's snapshot refresh. Each tick refreshes its hold snapshot once, before its destructive pass. | `ravel-cli hold list --tenant <id>` shows whether the scope is recorded. | Confirm with `hold list` after every urgent `hold set`. `hold set` returning success means the record was written, not that a pass has picked it up. The exposure window is one maintenance interval. |
| The `RavelCatalogFoldStalled` alert fires: for the signal the alert names, no live folding process has advanced `ravel_catalog_fold_last_success_timestamp_seconds` within the unsealed ingest span the configuration allows. The exact expression, its threshold, and its state-by-state behaviour live with the rule; see the corrective-action cell. | No process has completed a catalog fold of that signal for longer than the unsealed ingest span the configuration implies. Each signal is folded by its own task, so one signal alarming while the others stay fresh is that one task dead. All signals alarming together is the process gone, its store credentials no longer working, or tenant discovery failing every cycle. | The gauge on `/metrics`, read per `signal`, plus `ravel_catalog_fold_failures_total` for the same signal to tell a fold that is running and failing from one that is not running at all. The fold task logs the underlying error per tenant at `warn`. | Time-bounded, not cosmetic: the unsealed span grows for as long as this holds, and a cold recent-window query over a wide enough span is refused for exceeding its object-store request budget. [The observability guide](../observability.md#the-fold-stalled-alert) carries the rule, the arithmetic behind the 4800, and the two limits on what the gauge can see. |
| `ravel_scrub_cursor_position` stuck near 0 for longer than the scrub period. | The scrubber is not keeping pace with `--scrub-period`, so the effective staleness bound is no longer that period. | The gauge, per signal, on the maintain process's `/metrics`. | Lengthen `--scrub-period`, or give the maintain process more read bandwidth. Sustained scrub bandwidth is the corpus size divided by the period. |
| `ravel_maintain_l0_records_pending{signal="metrics\|logs\|spans"}` climbing and staying nonzero. | Buckets are sealing faster than they cross `min_compaction_inputs` (default 2), so more L0 segments pile up uncompacted than the compactor is clearing. A small, transient count is normal: any bucket with fewer than `min_compaction_inputs` records is waiting on its next writer flush before it can compact at all. | The gauge, labelled by signal, on `/metrics`. It is a per-process total, summed over every tenant and shard that process maintains and republished once per maintenance cycle (default 300 s) after the cycle has covered all of them, so a mid-cycle scrape reads the previous cycle's complete total rather than a partial sum. Buckets the interior memo skipped (`interior_reverify_ns`, default 6 h) still contribute their last-known count, so the figure is the whole pending population on every cycle, not only what that cycle re-read. With several maintain replicas, sum the gauge across them: each owns a disjoint share of the units. Per bucket, `ravel-cli maintain status --tenant <t> --signal <s> --shard <n> --hour <n>` prints that bucket's L0 record count as `l0_commit_records`, but not the `min_compaction_inputs` threshold it is compared against; read the threshold from the maintain process's own configuration and compare by hand. | A steady count that tracks normal per-bucket flush cadence self-clears once each bucket's next flush crosses the threshold; no action needed. A count that grows without bound, or does not fall once `ravel_maintain_units_stalled` clears, means compaction is not keeping pace with ingest; follow the stalled-unit row above. |
| `increase(ravel_maintain_objects_deleted_total[1h]) == 0` while storage keeps growing. | Either nothing has become eligible for deletion yet, or sweep is not reaching this shard at all. The counter is labelled by `kind`, and every kind has its own by-design delay before it can move: `superseded_records_deleted` and `superseded_data_deleted` wait for a compaction's own record to clear the protection horizon (about 25 h on the defaults); `quarantine_reaped` waits out `quarantine_horizon_ns` (7 days on the defaults), which is much the longest of the three; and `unreferenced_parts_deleted` waits `grace + max_compaction_lifetime` and needs an `l1/` part to exist at all, which only a compaction writes. A zero on any single kind is therefore expected for its own window rather than evidence of a stuck sweep. | The counter on `/metrics`, per `kind`. Compare against `ravel_maintain_units_stalled` and the maintain pass log to tell "nothing eligible yet" from "sweep is not running." | A flat counter alone is not an alarm. Investigate only alongside another symptom in this section: a stalled unit, no maintain process at all, or storage growth persisting well past the protection horizon with compaction otherwise healthy. |

## Data integrity and correctness alarms

Page on any nonzero increase for the four counter rows below. None is a rate
to threshold.

The last row is different in kind and is read differently: a drain that loses
acknowledged rows is a data-loss event on a process that is exiting, so its
counters may never be scraped. Its ERROR log line is the reliable signal, and
the row says which counters corroborate it.

| Symptom | Likely cause | How to confirm | Corrective action |
|---|---|---|---|
| `increase(ravel_scrub_checksum_mismatch_total[1h]) > 0`. | At-rest corruption: a whole-object hash mismatch against the recorded content hash, from bit rot or a partial write, or a section checksum failure. | The counter, labeled by signal and by `level` (`l0` an original ingested segment, `l1` a compaction output part, `rewrite` a selective-erasure rewrite output part), on `/metrics`. The `level` names the tier the corrupt object came from. Confirm the extent with `ravel-cli maintain verify-custody --tenant <t>`, which re-hashes every live data object and exits nonzero on any anomaly. | There is no redundant copy to repair from, and the scrubber only detects. For a `level="l0"` mismatch, first check whether the hour is already compacted: an L0 segment a live compaction has folded stays in the rotation, so its mismatch may name a redundant copy the catalog already excludes from queries rather than data a query can still reach. For any other mismatch, identify the affected objects with `verify-custody`, then restore from your bucket-level controls. See [disaster recovery](../disaster-recovery.md). |
| `increase(ravel_scrub_postings_disagreement_total[1h]) > 0`. | A covering name-index object omitted a name the data object really carries, so a query filtering on that name silently skips matching data. | The counter, labeled by signal, on `/metrics`. | A correctness defect, not a capacity problem. Capture the affected tenant and signal and escalate; a query result computed while this is nonzero cannot be trusted for that name. |
| `increase(ravel_scrub_seal_divergence_total[1h]) > 0`. | The folded snapshot under-counts the sealed commit history. `reason="missing"` is a sealed record absent from the snapshot; `reason="mismatched"` is a snapshot entry whose content hash disagrees with the record. | The counter, labeled by signal and reason, on `/metrics`. `ravel-cli catalog verify --tenant <t> --signal <s>` is the same check on demand. | Follow [queries are missing recently written data](#queries-are-missing-recently-written-data). A snapshot entry with no surviving commit record is the expected shape once retention deletes a folded record and is never counted here, so any increase is real. |
| `increase(ravel_catalog_isolation_breach_total[5m]) > 0`. | A cross-tenant key-layout or hashing fault: a tenant-hash mismatch on a catalog HEAD or index object, or a resolve listing result whose key does not begin with the requesting tenant's prefix. | The counter, labeled by mode, on `/metrics`. Every increment corresponds to a query that already failed with an explicit isolation-fault error, so the client saw it too. | Escalate immediately. Unlike the two anomaly counters rendered beside it, which tally an overlap the query resolves past, every increment here is a failed query and a real isolation fault. Coverage is not complete: a mismatch on a commit or compaction record fails its query without incrementing this counter, and a snapshot part's own tenant hash is not checked against the requester at all, so a zero reading is not proof of isolation. |
| A graceful shutdown (rolling deploy, pod eviction) lost buffered-mode rows that were already acknowledged to the client. | The node's clock stepped back past the monotonic flush-open floor on every one of the drain's retry passes, so each pass refused to open a flush and re-inserted the tenant's buffer; those rows were acked at enqueue in buffered mode and are gone once the process exits (see [Consistency model](../../consistency-model.md)). A drain whose STORE calls stall does not land here: the buffer is moved into a spawned task before any store call, so the tenant is already out of the map when the PUT hangs, and that loss shows up as `ravel_shutdown_drain_overrun_total` with this counter reading 0. | `ravel_ingest_flush_all_residue_tenants_total` on `/metrics`, by signal; the counter is process-global and does not survive the process exiting, so a nonzero value read on that dying process's own `/metrics` is the loss, but a scrape against the NEXT process starts back at zero and tells you nothing about the prior one. A live scrape is unlikely to catch the exact event -- see [Reachability during shutdown](../observability.md#reachability-during-shutdown) -- so grep that process's logs for `ravel-ingest: flush_all left buffered tenants unflushed after exhausting retry passes`, which names the shard, tenant count, and buffered point count. | Not recoverable for the rows already lost: buffered mode returns no commit token, so there is nothing to replay from this side. Identify the affected tenants and window from the log line, and check whether the client can detect and resend (buffered mode has no idempotency key for metrics; logs and spans can dedup a resend with `x-ravel-idempotency-key`). If this recurs, investigate the node's clock, not the object store and not `--shutdown-timeout`: a receding clock is the only condition that leaves residue here, and `--shutdown-timeout` only bounds how long the process waits before giving up. |

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
