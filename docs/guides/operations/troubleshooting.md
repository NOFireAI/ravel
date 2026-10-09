# Troubleshooting

Find your symptom, confirm the cause, then take the action. Every entry has
the same four parts: the symptom, the cause, a command or a metric that
confirms the cause, and the action.

If an alert is paging, read the first three sections in full before you act.
In those three cases the obvious first action makes the situation worse.

Two facts apply to every entry:

- Only a `--mode maintain` process compacts, applies retention, sweeps or
  scrubs. If storage grows or retention deletes nothing, first make sure that
  a maintain process exists. See [Maintenance](maintenance.md).
- You can delete a catalog HEAD object as a supported repair. Deleting any
  other object by hand is not a supported repair. The sweeper's orphan rule
  treats a data object whose commit record you removed as garbage to reclaim.

Sections:

- [The mass-orphan circuit breaker tripped](#the-mass-orphan-circuit-breaker-tripped)
- [Commit records were deleted out of band](#commit-records-were-deleted-out-of-band)
- [Queries are missing recently written data](#queries-are-missing-recently-written-data)
- [A process refuses to start](#a-process-refuses-to-start)
- [Readiness, storage and authentication](#readiness-storage-and-authentication)
- [Maintenance is not running, or not finishing](#maintenance-is-not-running-or-not-finishing)
- [Data integrity and correctness alarms](#data-integrity-and-correctness-alarms)
- [Query cost and results](#query-cost-and-results)
- [Profile server memory](#profile-server-memory)

## The mass-orphan circuit breaker tripped

### Breaker trip

**Symptom.** `increase(ravel_maintain_orphan_breaker_tripped_total[5m]) > 0`

**Cause.** A sweep pass found a large set of data objects whose commit records
are gone. This usually means that records were deleted out of band, and not
that many flushes were abandoned.

**Confirm.** The counter increment is the confirmation. The counter increments
only on a real trip.

- A trip can happen only on a tick that ran candidate selection. Candidate
  selection runs on the full-sweep cadence (`interior_reverify_ns`, 6 hours by
  default), not on every tick.
- `ravel_maintain_orphans_withheld` on `/metrics` gives the size of the set
  that the last such pass withheld. A tick that skipped selection leaves the
  gauge unchanged.
- To run the same evaluation and delete nothing, run
  `ravel-cli maintain sweep --tenant <t> --signal <s> --shard <n> --dry-run`.
- The `--signal` option of that command accepts `metrics`, `logs` and `spans`
  only. A trip under `signal="alerts"` or `signal="audit"` has no CLI dry run.
- The alerts and query-audit shards are swept on every maintain tick of the
  process that owns the unit, not only on the full-sweep cadence. The next
  such tick re-evaluates the breaker from live counts.
- `ravel_maintain_orphans_withheld` is not set for those two shards. Their
  only signals are the tripped counter and the error log line that carries
  the withheld count.

**Action.** Restore the missing commit records before the next pass runs. Do
not wait for the trip to persist. The reasons are below.

### What a trip means

Alert on the **first trip**, with `increase(...) > 0`. Do not alert on a
sustained condition. The counter only increments, so any increase is a trip
that happened, whether or not the shard still trips now.

A trip means that both of these conditions held on that pass:

- The pass found at least `orphan_breaker_min_count` orphan candidates
  (default 50).
- The candidates were more than `orphan_breaker_max_ratio` of the shard's
  listed L0 objects (default 10%).

The pass deleted nothing and halted. The other two sweep rules,
superseded-input and unreferenced-L1, still ran. They are anchored on durable
records and not on the absence of a record.

### A trip can clear unrepaired

Each pass recomputes the trip condition from live counts, with no memory of a
prior trip. So a shard can stop tripping while the missing records are still
missing:

- **Dilution.** New well-recorded writes to the same shard lower the ratio
  below the threshold, and the orphan count does not change. 55 orphans among
  500 objects trips at 11%. 200 further writes with no data loss give 55/700,
  which is 7.9% and does not trip. The next pass deletes those same 55
  objects.
- **Partial restoration.** You restore some of the missing records and the
  remaining count falls below the floor. 55 orphans trips. If you restore 6,
  49 remain, under the default floor of 50. The next pass does not trip and
  deletes the other 49 before you restore them.

Relying on the breaker to hold a shard open until every record is back is
relying on a guarantee that does not exist. The only durable way to stop the
deletion is to restore the missing records before the next pass runs. Follow
[commit records were deleted out of band](#commit-records-were-deleted-out-of-band).

### Force a pass

```sh
ravel-cli maintain sweep --tenant <t> --signal <metrics|logs|spans> \
  --shard <n> --override-orphan-breaker
```

This command runs one overridden pass, which deletes the withheld candidates
despite the trip. The override applies to that one invocation. The server
never sets it, and the breaker has no memory across invocations, so a later
pass without the override evaluates fresh.

Use the override only after you confirm that deletion is safe. Either restore
the records, or independently verify that the candidates are abandoned data.
Out-of-band record loss forges the same record-absence signal that the orphan
rule re-verifies against.

The alerts and query-audit shards have no override, because the command does
not accept those signals. The maintain tick that next sweeps the shard moves
the candidates to `quarantine/` as soon as the live counts fall below the
thresholds. So restore the records first, or restore the objects from
quarantine within its retention.

### What the breaker misses

A quiet breaker is not an all-clear. The breaker has four gaps:

- It never trips below the count floor, whatever the ratio. So one pass can
  always delete a total loss on a small shard.
- One pass can delete up to the ratio ceiling of a large shard's objects and
  never trip.
- Dilution and partial restoration can let a pass through the remaining loss.
- Each unit is evaluated in isolation, with no cross-shard or cross-tenant
  aggregation. Loss spread thin across many shards can stay under every
  shard's threshold while the total is large.

The gauge `ravel_maintain_orphans_present` closes the small-scale gap. It
carries the total candidate count of the most recent pass, whether or not the
breaker tripped.

"Most recent pass" means the most recent pass that ran the orphan rule:

- Orphan candidate selection runs on the full-sweep cadence
  (`interior_reverify_ns`, default 6 h), not on every maintain tick (default
  300 s).
- The ticks in between skip the rule and report nothing about orphans. They
  leave the gauge unchanged and do not reset it to zero.
- The gauge is refreshed once per full-sweep interval, so a value can be up
  to one full-sweep interval old.
- Read a change as "the last orphan pass found this", never as "as of this
  scrape". A return to zero shows on the next pass that runs, not on the
  next tick.

The gauge also reports only one unit per tick. Its labels are mode and signal,
while the sweep runs per tenant, signal and shard. Each pass overwrites the
same series, so a unit that measured zero can mask the nonzero measurement of
another unit.

### Orphans below the thresholds

**Symptom.** `ravel_maintain_orphans_present > 0` for `12h`

**Cause.** One of two conditions holds. A small number of commit records were
lost for one shard, below both breaker thresholds. Or an abandoned flush is
stuck.

**Confirm.**
`ravel-cli maintain status --tenant <t> --signal <s> --shard <n> --hour <n>`
reports the L0 record count of that bucket against what is present.
`ravel-cli maintain sweep ... --dry-run` prints the candidate set and deletes
nothing.

**Action.** Investigate before `grace + max_flush_lifetime` elapses (25 h on
the defaults: 24 h plus 1 h). If records are missing, follow
[commit records were deleted out of band](#commit-records-were-deleted-out-of-band).

Twelve hours is approximately half the grace window. One normal
abandoned-flush cleanup between passes does not page, and real loss alarms
with hours to spare. Twelve hours is also longer than the default 6 h
full-sweep interval that refreshes the gauge, so a sustained alert window
always spans at least one orphan pass. If you raise `interior_reverify_ns`,
keep the alert window longer than it, or the window can close on a single
stale sample.

Do not alert on `ravel_maintain_orphans_withheld`. It reflects only the most
recent pass that ran the orphan rule. It drops to zero on the next pass that
does not trip, including one that stopped tripping through dilution.

## Commit records were deleted out of band

**Symptom.** Data that was written is invisible to queries, and the orphan
breaker tripped or `ravel_maintain_orphans_present` is nonzero.

**Cause.** Commit records for a shard were removed outside Ravel: an
accidental delete, a lifecycle rule on the wrong prefix, or a mistyped prefix
delete.

**Confirm.**
`ravel-cli maintain sweep --tenant <t> --signal <s> --shard <n> --dry-run`
lists the record-less data objects as orphan candidates.
`ravel-cli catalog list --tenant <t> --shards <n>` shows what the catalog
still resolves.

**Action.** Do the five steps below in order. Step 1 is mandatory.

Readers cannot see the data objects that those records named. Two clocks run
against the repair:

- After the orphan grace horizon, the sweeper moves the objects out of the
  live keyspace to a `quarantine/` prefix. `commit reconstruct` does not look
  there.
- After `quarantine_horizon_ns` (default 7 days), a second reaper deletes the
  quarantine copy. The object is then gone permanently.

`ravel-cli commit reconstruct` does the recovery. It rebuilds the commit
record of each record-less L0 data object from the footer of that object. It
reads the live L0 prefix only, so you must first copy back anything already
quarantined (step 2).

1. **Stop maintenance for the tenant.** Stop the `--mode maintain` process
   completely. If maintenance runs, the sweeper's orphan rule quarantines the
   objects while you reattach them.

   - This is the one method that protects a tenant under repair whatever its
     config-record status.
   - `--maintain-tenant` excludes only tenants that do not yet carry a
     durable config record. After a tenant carries one, no flag excludes it
     from maintenance. A restart restricted to other tenants does not keep
     the sweeper off it.
   - Do not rely on the orphan breaker to hold the shard open. See
     [a trip can clear unrepaired](#a-trip-can-clear-unrepaired).

2. **Restore anything already quarantined.** List the tenant's quarantine
   prefix and compare it against the orphan candidates that the sweep
   reported:

   ```sh
   aws s3 ls --recursive s3://<bucket>/quarantine/t/<tenant_hash>/
   ```

   Each entry is `quarantine/<original key>/q<quarantined_at_ns>`. To get the
   live key, remove the `quarantine/` prefix and the trailing `/q<ns>`
   segment. The original key is preserved verbatim in between, so the
   transform is textual and needs no lookup. Copy the object back to the live
   key. Copy, do not move, until step 4 passes: the quarantine copy is the
   only other copy.

   `ravel-cli` has no command for this step yet, so use the object-store
   tooling that the bucket takes. Objects whose quarantine timestamp is older
   than `quarantine_horizon_ns` are already gone and are not recoverable from
   here.

3. **Reconstruct the missing records**, one shard at a time:

   ```sh
   ravel-cli commit reconstruct --tenant <name> --signal <metrics|logs> --shard <n>
   ```

   The command lists the shard's record-less L0 data objects. It rebuilds a
   commit record for each one from its footer and writes the record
   create-if-absent. It never overwrites an existing record and never deletes.
   It prints a per-object report of reconstructed, already-present and
   failed, and exits nonzero if any candidate failed. Repeat for each shard in
   the affected range.

4. **Verify custody and catalog state** before you resume maintenance:

   ```sh
   ravel-cli maintain verify-custody --tenant <name>
   ravel-cli catalog verify --tenant <name> --signal <signal>
   ```

   `verify-custody` re-hashes every live data object against its key. It also
   confirms that the data of every surviving record is present.
   `catalog verify` re-lists sealed records and diffs them against the
   snapshot for the one signal that `--signal` names. The default is metrics,
   so run it once for each signal that the tenant writes. Both commands must
   exit zero before you trust the repair.

5. **Resume maintenance.** Restart the `--mode maintain` process. The sweeper
   now sees the reconstructed records and treats their data objects as
   referenced. Then delete the quarantine copies that you restored from in
   step 2. If you do not, the reaper leaves them until their own horizon.

Reconstruction rebuilds two fields as approximations:

| Field | Rebuilt from | Why |
|---|---|---|
| The record's creation time | The data object's own last-modified time | No footer carries it. |
| The ingest-hour bucket, for logs | The earliest observed sample | Log footers do not carry it. |

The rebuilt record is a reconstruction and does not claim byte-for-byte
provenance. Reconstruction also does not detect bit rot: it rebuilds a record
that describes the bytes currently stored. Use `verify-custody` for the
content-hash check.

## Queries are missing recently written data

A query that pins a commit token reads that commit key directly and not
through the snapshot, so it still returns the data. Use that difference to
tell this case apart from data that was never written.

### Sealed commits missing from the snapshot

**Symptom.** A query over a recent window returns fewer series or rows than
were written. The same query with an explicit minimum commit token returns
them.

**Cause.** A folder whose clock ran fast beyond its seal margin sealed an hour
before every writer's flush for it had landed. A commit published into the
already-sealed bucket is invisible to snapshot-reading queries. A hand fold
with `--max-flush-lifetime 0s` or `--writers-stopped`, or a `ravel-cli load
--fold-after-load`, run while another writer was live, has the same effect.

**Confirm.** `ravel-cli catalog verify --tenant <name> --signal <signal>`
exits nonzero with a nonempty "missing from snapshot" count.
`increase(ravel_scrub_seal_divergence_total[1h]) > 0` is the scheduled form of
the same check.

**Action.** [Rebuild the snapshot](#rebuild-the-snapshot).

### Flushes refused for clock lag

**Symptom.** All of these occur on one process:

- Writes return retryable 503s (strict mode), or its buffered rows never
  flush.
- Later writes get HTTP 429 after its buffer byte budget fills.
- The WARN log reads
  `flush clock lags the object store's observed clock beyond the clock-skew allowance; refusing the flush`.

**Cause.** The clock of the process host runs behind the object store's clock
by more than five minutes. Five minutes is the compiled-in clock-skew
allowance. The check does not read a configured `clock_skew_allowance_ns`.

A publish can stamp an ingest hour that the fold already sealed. So the
process refuses every flush and puts the rows back in the
buffer, where they stay until they can be published. A flush moves neither
clock, so the refusals continue until the host clock converges. The growing
buffer fills the process-wide byte budget, and then new writes are shed with
HTTP 429.

**Confirm.** `increase(ravel_ingest_clock_lag_refused_total[10m]) > 0` on that
process, by signal (the `RavelWriterClockLagRefused` alert). A nonzero
`ravel_ingest_clock_lag_bypassed_at_shutdown_total` on a process that is
shutting down means that its teardown drain published rows with the check
bypassed.

**Action.**

1. Fix NTP on the host. Refused flushes retry on the next trigger and publish
   when the lag is back inside the allowance.
2. If `ravel_ingest_clock_lag_bypassed_at_shutdown_total` moved, those rows
   can sit in a sealed hour. If a token-less read is missing them,
   [rebuild the snapshot](#rebuild-the-snapshot).

### Rebuild the snapshot

1. Run `ravel-cli catalog verify --tenant <name> --signal <signal>`, once for
   each signal that the tenant writes. A nonzero exit with a nonempty missing
   count confirms sealed commits that the snapshot does not know about.
2. Delete the tenant's HEAD object for the affected signal,
   `t/<tenant_hash_hex>/catalog/<signal>/HEAD`. `ravel-cli` has no subcommand
   for this. Use the store's own tooling (`aws s3 rm`, with `--endpoint-url`
   against RustFS or any other S3-compatible store). The delete is safe: an
   absent HEAD means "no snapshot yet", and the next fold rebuilds one from a
   full listing.
3. Run `ravel-cli catalog fold --tenant <name> --shards <n> --signal <signal>`,
   or wait for the next background fold tick. The `rebuilt: true` line in the
   report confirms that the fold rebuilt from scratch and did not extend the
   prior snapshot.
4. Run `ravel-cli catalog verify` again to confirm that the divergence is
   gone.

Ravel has no force-rebuild flag. Deleting HEAD is the supported way to force a
rebuild, because it uses the same absent-HEAD path that a new tenant takes on
its first fold.

Then fix the cause. Review the clock of the folder host, or the seal margins
that you changed. See
[the seal margin](maintenance.md#the-seal-margin-and-why-it-matters).

## A process refuses to start

Every refusal below is a hard error before any listener binds. None is
transient and none clears on restart.

Most are a disagreement between the process configuration and what object
storage records. `CorruptGenerations` is the exception: the record is invalid
on its own, with no configuration value to disagree with, and the process
cannot trust it to route on.

### Shard count hides data

**Symptom.** Startup error that adopting a tenant's data would hide it. The
error names a tenant, a signal, and an observed shard index at or above the
configured `--shards`.

**Cause.** The process is configured with a lower `--shards` than the tenant's
pre-record data already used. A record written at the configured value leaves
existing series in higher shards unroutable.

**Confirm.** The error names the observed index and the configured value.
`ravel-cli catalog list --tenant <t> --shards <n>` against the higher value
resolves those records.

**Action.** Raise `--shards` to cover every observed shard index for that
tenant, or first run `ravel-cli provision adopt` at the correct value.

A difference between the live default and the recorded count of an
already-provisioned tenant does not refuse startup. That tenant keeps its
recorded count and routes over it.

### Corrupt generation history

**Symptom.** Startup error `CorruptGenerations`. It names a tenant, a signal,
and the specific structural defect (for example `ScalarMismatch` or
`FirstActivationNonzero`).

**Cause.** The provisioning record of a statically-known tenant decoded, but
its `generations` history fails a structural invariant:

- The scalar `shard_count` disagrees with the count of generation 0.
- Generation 0 is not at `activation_hour` 0.
- The history is not dense or not activation-increasing.
- The count of a generation is out of range or is a no-op repeat.

`validate_static_provisioning` propagates the error before any listener
binds. The same defect fails the first ingest touch of a dynamic tenant and
increments `ravel_provisioning_shard_count_mismatch_total`. If the maintain
loop hits the defect first, it skips the maintenance tick of that tenant.

**Confirm.** The error names the tenant, signal, and defect variant.

**Action.** No live-configuration flag corrects this. Repair the record by
hand, or re-provision the tenant:

- Restore a valid `generations` history for that (tenant, signal) object.
- Or delete the record and run `ravel-cli provision adopt` again, if it is
  safe to re-derive shard_count from currently observed data.

### Garbage-collection value mismatch

**Symptom.** Startup error that names a configured and a stored
garbage-collection value and the rule violated.

**Cause.** A `--gc-*` flag disagrees with the durable `sys/gc` object. In
maintain mode the horizon and grace must be equal to the stored values. It is
not sufficient that they satisfy the inequality.

**Confirm.** `ravel-cli gc-config show` prints the stored values and whether
the bucket is bootstrapped.

**Action.** Align the flags with the stored object. To change the stored
values instead:

1. Change the object with `ravel-cli gc-config set`.
2. Bring the flags of every mode into line.

A query deadline above the stored maximum is rejected, never clamped.

### Store not qualified

**Symptom.** Startup error that the store is not qualified, or that its
qualification is stale.

**Cause.** A fresh bucket was never qualified, or its record predates the
required suite floor of this binary.

**Confirm.** The startup output reports the two conditions as distinct named
errors.

**Action.**

1. Run `ravel-cli store qualify --store s3 ...` against the bucket. If the
   record is stale, use a current build.
2. Start the server.

### Tenant-hash key missing

**Symptom.** Startup error that a fresh bucket needs a tenant-hash key.

**Cause.** A fresh bucket was started with neither `--tenant-hash-key-file`
nor `--tenant-hash-unkeyed`. Keyed is the default and the choice is permanent
for the bucket.

**Confirm.** The error names both flags.

**Action.** Pass the key file. If you intend the unkeyed scheme, pass
`--tenant-hash-unkeyed`. Decide before you start: no migration between the two
schemes is possible.

### Key fingerprint mismatch

**Symptom.** Startup error that the fingerprint of the configured key
disagrees with the bucket marker.

**Cause.** The wrong deployment key was mounted.

**Confirm.** `ravel-cli tenancy show --tenant-hash-key-file <path>` verifies a
key against a bucket offline, without starting a server.

**Action.** Mount the correct key. A wrong key is a failed deploy and does not
create a second namespace. Do not switch schemes to get past the error.

### Fragment key file missing

**Symptom.** Startup error that names `--distributed-query` and a missing key
file.

**Cause.** Distributed reads were enabled without `--fragment-key-file`.

**Confirm.** The error names both flags.

**Action.** Provide the key file. It holds 32-byte keys, one per non-empty
line. Each line is 64 hexadecimal characters. A file with no key line, or
with a line of another length, fails startup with a line number.

### Bucket protection violated

**Symptom.** Startup error that the bucket's protection configuration violates
the required bucket configuration.

**Cause.** `--require-bucket-protection` is set on `--store s3` and the bucket
fails a refusing condition: `object-lock`, `abort-multipart`,
`no-foreign-rule`, or `noncurrent-expiration` on a versioned bucket.

**Confirm.** The error names each failed refusing condition with the reason.
The process exits before any listener binds, so it serves no `/metrics` to
scrape. Read the error from the startup output (`kubectl logs --previous <pod>`
for a pod that the refusal restarts). `ravel-cli store verify-protection`
prints every condition.

**Action.** Correct each named condition at the bucket layer and restart. For
a development deployment, you can drop the flag instead.

An unknown condition warns and starts. Only a failed refusing condition
refuses. See [Deployment](deployment.md#bucket-protection-at-startup).

### Retention window below floor

**Symptom.** Startup error that a retention window is below the floor.

**Cause.** `--retention-default` or `--retention-tenant` is shorter than the
ingest lag, flush lifetime, skew allowance and one bucket span combined.

**Confirm.** The error names the configured window and the floor.

**Action.** Raise the window. Startup refuses a short window and does not
clamp it, so that a bucket can never be tombstoned before it is sealed.

### Admission limits file error

**Symptom.** Startup error that names a key in the admission limits file.

**Cause.** The file is not valid TOML, names an unknown key, has an empty
tenant id, has a zero or negative count, or has a burst with no rate to pair
with.

**Confirm.** The error names the offending table and key.

**Action.** Correct the file. Validation is fail-closed: a bad file never
falls back to the shipped defaults.

### Conflicting credential flags

**Symptom.** Startup error that names two conflicting credential flags.

**Cause.** `--s3-auth instance-role` was combined with a static credential
flag. An exported `RAVEL_S3_ACCESS_KEY` counts.

**Confirm.** The error names both the auth mode and the offending flag.

**Action.** Remove the static credential, including from the environment.

### mTLS listener address

**Symptom.** Startup error that `--mtls-enabled` requires `--mtls-listener`,
or that two listener addresses are equal.

**Cause.** The mTLS resolver is installed only on its own listener, and each
dedicated listener must bind a distinct address.

**Confirm.** The error names the flags and the colliding address.

**Action.** Give the mTLS and fragment listeners their own addresses.

### mTLS listener not loopback

**Symptom.** Startup error that `--mtls-listener` does not bind a loopback
address and requires `--mtls-trust-forwarded-header`.

**Cause.** The resolver believes a proxy-forwarded header and does not verify
a certificate. Only a loopback bind proves by topology that a local proxy is
the only possible source of the header.

**Confirm.** The error names the bound address and the flag.

**Action.** Bind the listener to loopback. Or add
`--mtls-trust-forwarded-header` to assert that a verifying proxy fronts the
address and that no client can reach it directly. The flag turns nothing on.
It records your choice.

### Fragment endpoint not advertised

**Symptom.** Startup error that `--distributed-query` requires
`--advertise-fragment-endpoint` because a published listener binds an
unspecified address.

**Cause.** Under distributed query the process publishes its bound listener
addresses for siblings to dial. No peer can dial `0.0.0.0` or `::`.

**Confirm.** The error names each offending listener flag with its address.

**Action.** Pass `--advertise-fragment-endpoint <host[:port]>` with a host
that peers can reach, or bind the listener to a specific address. Without the
flag the failure is silent: siblings fail at connect and fall back to local
execution.

### Fragment certificate key usage

**Symptom.** Startup error that `--fragment-tls-cert` is missing the
`serverAuth` or `clientAuth` extended key usage, or both.

**Cause.** The fragment listener is mutual TLS and one certificate serves both
directions:

- A `serverAuth`-only certificate (what earlier releases documented) cannot
  dial a peer.
- A `clientAuth`-only certificate cannot be dialled.
- `anyExtendedKeyUsage` alone satisfies neither verifier.

**Confirm.** The error names the certificate path, the missing usage, and the
usages that the certificate does carry.
`openssl x509 -in <cert> -noout -ext extendedKeyUsage` shows the same.

**Action.** Reissue the certificate with
`extendedKeyUsage = serverAuth, clientAuth` (cert-manager:
`usages: [server auth, client auth]`) and restart.

Without the refusal, the process starts and serves the direction that the
certificate carries. It fails every handshake in the other direction and
falls back to coordinator-local execution, and nothing reports why.

### Advertised port without listener

**Symptom.** Startup error that `--advertise-fragment-endpoint` carries a port
but no `--fragment-listener` is configured.

**Cause.** In the combined layout the public gRPC listener serves both
published endpoints. The port half of the flag reaches the fragment endpoint
only. The Flight SQL endpoint keeps the bound port, so one of the two
endpoints published for the same socket is then wrong.

**Confirm.** The error names the value as written and the gRPC listener
address that both lanes share.

**Action.** Advertise a host only, which keeps the bound port of each
listener. Or configure `--fragment-listener` so that the fragment lane has its
own port to map.

## Readiness, storage and authentication

### Fleet not ready

**Symptom.** `/readyz` returns 503 across the fleet and the load balancer has
taken it out.

**Cause.** The background store probe failed four consecutive reads of
`sys/tenancy`.

**Confirm.** `ravel_store_reachable == 0` on `/metrics`, and
`ravel_store_probe_failures_total` rises.
`curl -sS -o /dev/null -w '%{http_code}' http://<host>/readyz` returns 503
with no store call of its own.

**Action.** Fix the store or the credential. Readiness recovers on the first
successful probe, without a restart.

Do not lower the threshold. It is a fixed constant so that a single blip
cannot eject a fleet. Liveness is unaffected, so nothing restarts the
processes.

### Rollout halted on readiness

**Symptom.** A deployment gated on readiness halted mid-roll.

**Cause.** The same condition as [fleet not ready](#fleet-not-ready).
Readiness reflects store reachability, so a rollout stops while the store is
unreachable.

**Confirm.** As for [fleet not ready](#fleet-not-ready).

**Action.** Resolve the store outage. The roll then resumes.

### One process not ready

**Symptom.** A single process reports `/readyz` 503 while the store is
reachable and the fleet is otherwise healthy.

**Cause.** An ingest shard actor on that process was condemned because a flush
kept panicking. The usual cause is a poison-pill input for one shard, not a
transient fault.

- On the metrics pipeline, condemnation follows exhaustion of the respawn
  budget (`MAX_SHARD_RESPAWNS`, 3 deaths within one decay window).
- The logs and spans pipelines never respawn, so the first shard-actor death
  condemns immediately.

A condemned shard cannot recover in-process.

**Confirm.** `shards_condemned > 0` on `/metrics` for that process, with
`ravel_store_reachable == 1`. The `signal` label names the pipeline.

- On the metrics signal, `shard_deaths` first climbs to at least the respawn
  budget on that shard.
- On logs or spans, a single `shard_deaths` is enough.
- Writes to the condemned shard return the typed shard-unavailable error.

**Action.** Roll the process yourself. Nothing replaces it automatically: a
503 at `/readyz` only removes the pod from its Service endpoints, and
`/healthz` stays 200 by design. The pod is not restarted or rescheduled and
stays condemned indefinitely.

1. Capture the panicking flush from the error logs of that shard. If one
   tenant or series is the poison pill, a replacement condemns as fast.
2. Run `kubectl delete pod <pod>`, or
   `kubectl rollout restart deployment/<name>` for the whole set.

A fresh process starts with all shards live. On the metrics signal,
`shard_deaths` alone (with `shards_condemned == 0`) is transient respawn
recovery and is not a page.

### Bucket protection unknown

**Symptom.** `ravel_bucket_protection_unknown == 1`

**Cause.** `--require-bucket-protection` is on and the startup check could not
determine at least one bucket-protection condition. On S3 the usual causes
are:

- An access denial on one of the three configuration GETs. No IAM template
  under `deploy/iam/` grants them to a server role.
- A bucket-configuration read that did not finish within its 10-second bound.
- Lifecycle rules whose coverage of `t/` the check cannot prove.

Every backend other than S3 reports every condition unknown.

**Confirm.** The gauge, `ravel_bucket_protection_conditions_unknown` for how
many, and the single startup warning that names each unknown condition and
why.

**Action.** This is not necessarily a misconfiguration, but the platform
cannot see the protection that it depends on. Do one of these:

- Grant the read-only configuration permissions.
- Restate the lifecycle rules in a form that the check proves.
- Confirm the bucket settings out of band. `ravel-cli store verify-protection`
  reads them from the configuration of an S3 bucket and exits nonzero on
  anything that it cannot confirm. You can also use the provider console or
  the provider CLI.

Until `ravel_bucket_protection_conditions_unknown` is 0, a zero
`ravel_bucket_protection_conditions_failed` is not evidence that the bucket
is compliant.

### Bucket protection failed

**Symptom.** `ravel_bucket_protection_conditions_failed > 0`

**Cause.** `--require-bucket-protection` is on and the startup check found
that the bucket fails a condition that does not refuse to start:

- `versioning` off
- no `expired-delete-marker` cleanup
- `rule-scope` (the sanctioned rules do not cover every key under `t/`)
- a failing `noncurrent-expiration` on an unversioned bucket

**Confirm.** The gauge counts the failed conditions. The single startup
warning names each one with the reason. `ravel-cli store verify-protection`
prints every condition.

**Action.** Correct each named condition at the bucket layer. The gauge moves
at the next restart. See
[Deployment](deployment.md#bucket-protection-at-startup) for which conditions
refuse and which only warn.

### Orphaned multipart uploads

**Symptom.** Orphaned multipart uploads accumulate in the bucket.

**Cause.** Multipart uploads end without a confirmed abort, so parts can stay
behind and be billed. Ravel tracks this internally but does not yet export it
on `/metrics`.

**Confirm.** List multipart uploads directly against the bucket, for example
with `aws s3api list-multipart-uploads --bucket <bucket>`, or use the storage
console of your provider.

**Action.**

1. Make sure that the bucket has an **enabled**
   `AbortIncompleteMultipartUpload` lifecycle rule with a cleanup period of
   seven days or less.
2. Make sure that the scope of the rule covers Ravel's uploads. Use an empty
   prefix, so that the rule applies to the whole bucket, or a set of prefixes
   that together cover every `t/` prefix.

A rule that is present but disabled, or scoped to a prefix that Ravel never
writes, passes a "does a rule exist" check and reaps nothing. The parts keep
accumulating and stay billed. Nothing in Ravel reaps those parts, so that rule
is the only bound on the cost.

The platform-CLI checklist in the
[disaster recovery](../disaster-recovery.md) guide gives the
`get-bucket-lifecycle-configuration` query, gated on `Status==Enabled` and
that same whole-bucket-or-every-`t/`-prefix scope.

### Durable auth refresh fails

<a id="durable-auth-refresh-is-failing"></a>

**Symptom.** `increase(ravel_durable_auth_refresh_failures_total[15m]) > 0`

**Cause.** The background refresh cannot read or decode `sys/auth`. The usual
causes are a storage credential that broke or lost read on that key, an
object that is corrupt, or an object written under a different deployment key.

**Confirm.** The counter, labeled by mode, on `/metrics`. The cached map still
serves, so requests do not fail yet.

**Action.** Fix the credential or the object now. This is the early warning:
the staleness gate does not advance, and token resolution fails closed after
the hard-stale horizon passes.

### Durable tokens refused

**Symptom.** `increase(ravel_durable_auth_stale_fail_closed_total[5m]) > 0`

**Cause.** The cached token map is past the hard-stale bound, and durable
tokens are refused.

**Confirm.** The counter on `/metrics`, and clients that receive
authentication failures for tokens that worked before.

**Action.** Restore `sys/auth` readability. Resolution recovers on the first
successful refresh. The entry above,
[durable auth refresh fails](#durable-auth-refresh-fails), is the early
warning for this failure.

### Provisioning check failed

**Symptom.** `increase(ravel_provisioning_shard_count_mismatch_total[5m]) > 0`

**Cause.** A tenant failed its provisioning check hard. The check runs on the
first touch of a dynamic tenant and on the per-tenant gate of the maintenance
loop. One of three failures occurred:

- The record is unreadable: corrupt, or a future format version.
- The record decodes, but its generation history fails its structural
  invariants (`CorruptGenerations`, see
  [corrupt generation history](#corrupt-generation-history)).
- Data written before the record existed is present that a lower
  `shard_count` would hide.

A recorded count that only differs from the live `--shards` default is not
counted here. It is tolerated, and `ravel_provisioning_shard_count_drift_total`
counts it.

**Confirm.** The counter on `/metrics`. Neither source takes the process down,
and the two have different blast radii:

- A first-touch failure fails only the triggering request (a query that
  resolves the tenant, or an ingest write) with a typed error.
- A maintenance-loop failure fails no request, but skips the maintenance tick
  of that tenant for the cycle.

To tell which source fired, correlate ingest error logs against
`ravel_maintain_units_stalled` and the maintain pass log.

**Action.** Read the durable record for that tenant. Any nonzero increase is a
real problem, not a rate to threshold. The remedy depends on the failure:

| Failure | Remedy |
|---|---|
| Future-format record | Upgrade the binary to one that reads that version. |
| Corrupt record, or a generation history that fails its structural invariants | Restore the record from a known-good copy, or safely re-provision the tenant. An upgrade does not repair persisted history. |
| Record would hide data | Raise `--shards` to cover the observed shards of the tenant. |

If the source was the maintenance loop, maintenance does not continue for
that tenant while the record is broken. Retention, compaction, and everything
else that the loop drives are paused for the tenant until you fix the record.

### Unexpected unkeyed adoption

**Symptom.** `ravel_tenancy_v1_unkeyed_adoptions_total` incremented
unexpectedly.

**Cause.** A bucket with data and no tenancy marker was adopted as unkeyed,
permanently.

**Confirm.** The counter, and the log line beside it that names the adoption.

**Action.** If that bucket was meant to be keyed, stop. The adoption is
permanent and no migration is possible. Start a fresh keyed bucket and drain
into it.

## Maintenance is not running, or not finishing

### No maintain process

**Symptom.** Storage keeps growing, retention deletes nothing, and object
counts per hour stay in the thousands.

**Cause.** No `--mode maintain` process runs. Ingest, query and alert
evaluation all run without one. The scheduled catalog fold does not, so
queries also pay listing cost for the whole unsealed span.

**Confirm.** With no process in `--mode maintain`, no `ravel_maintain_*`
series is present at all. Alert on `absent(ravel_maintain_workers_live)`.

If a maintain process runs but falls behind, three series show which half is
behind. Each has its own entry below.

| Series | Behind |
|---|---|
| `ravel_maintain_l0_records_pending` climbing | Compaction |
| `ravel_maintain_retention_lag_seconds` climbing past one protection horizon | Retention |
| `increase(ravel_maintain_objects_deleted_total[6h]) == 0` with a flat `ravel_maintain_bytes_reclaimed_total` | The sweep |

`ravel-cli maintain status --tenant <t> --signal <s> --shard <n> --hour <n>`
then narrows it to one bucket: a sealed, uncompacted bucket with a high L0
record count.

**Action.** Deploy a maintain process. See [Maintenance](maintenance.md).

### No live maintain worker

**Symptom.** `ravel_maintain_workers_live == 0` while a maintain process is
up.

**Cause.** The process cannot see itself as live: a heartbeat write fails
persistently, or a liveness read errors persistently. It then owns no units.

**Confirm.** The gauge on the `/metrics` of that process.

**Action.** Check the write and list grants of the maintain role on the
coordination prefix. Without them the first heartbeat write fails closed with
an access error. Fire the alert on the level, not on an increase, because no
counter exists here.

### Tenants without an owner

**Symptom.**
`ravel_maintain_tenants_maintained < ravel_maintain_tenants_discovered` for
`10m`

**Cause.** A tenant prefix holds data with no maintaining owner.

**Confirm.** The two gauges on `/metrics`. Ten minutes is two cycles at the
default interval, so a restart or a tenant mid-onboarding does not page.

**Action.** For a dynamically resolved tenant, add it with
`--maintain-tenant`. For any other tenant, investigate why ownership does not
cover it.

### Tenant discovery fails

**Symptom.**
`increase(ravel_maintain_tenant_discovery_failures_total[5m]) > 0`

**Cause.** A tenant listing failed. That skips the whole cycle for every
tenant, not only one.

**Confirm.** The counter on `/metrics`.

**Action.** Treat this as a full maintenance outage and act on the first
occurrence. The supervisor never treats a failed enumeration as "no tenants",
so nothing is maintained while the failure persists. Check the list grant of
the maintain credential, including the bare tenant-prefix entry.

### Stalled unit

**Symptom.** `ravel_maintain_units_stalled > 0` for `30m`

**Cause.** The last several ticks of one unit all failed with no success in
between. A single success resets the streak, so this is the same unit failing
and not a rotation of transient faults.

**Confirm.** The gauge on `/metrics`. To reproduce the failing pass, run
`ravel-cli maintain compact-tenant --tenant <t> --signal <s> --dry-run`. It
prints the outcome and error line of each bucket.

**Action.** A stall that survives multiple intervals needs an operator. A
blip during a store hiccup clears within a cycle or two. Thirty minutes covers
several cycles at the default interval.

### Conservation abort

**Symptom.** `increase(ravel_maintain_conservation_aborts_total[15m]) > 0`

**Cause.** A compaction publish was refused because input and output record
counts disagreed. Nothing was written.

**Confirm.** The counter, labeled by signal, on `/metrics`.
`ravel-cli maintain compact-bucket --tenant <t> --signal <s> --shard <n> --hour <n> --dry-run`
recomputes the same plan and writes nothing.

**Action.** A bucket that retries every tick and never compacts needs an
operator, not another retry.

### Legal hold refresh fails

**Symptom.**
`increase(ravel_maintain_legal_hold_refresh_failures_total[15m]) > 0`

**Cause.** The maintenance loop could not read the hold records, so it skipped
the whole tick of that tenant.

**Confirm.** The counter on `/metrics`. `ravel-cli hold list --tenant <id>`
reads the same records from the CLI.

**Action.** Fix the read path before anything else. The skip is the
fail-closed behavior, and a sustained failure means that a tenant receives no
maintenance at all.

### Data deleted despite hold

**Symptom.** A legal hold was set but data was still deleted.

**Cause.** The hold was set after the snapshot refresh of the current tick.
Each tick refreshes its hold snapshot once, before its destructive pass.

**Confirm.** `ravel-cli hold list --tenant <id>` shows whether the scope is
recorded.

**Action.** Confirm with `hold list` after every urgent `hold set`. A
successful `hold set` means that the record was written, not that a pass
picked it up. The exposure window is one maintenance interval.

### Catalog fold stalled

**Symptom.** The `RavelCatalogFoldStalled` alert fires. For the signal that
the alert names, no live folding process advanced
`ravel_catalog_fold_last_success_timestamp_seconds` within the unsealed ingest
span that the configuration allows.

**Cause.** No process completed a catalog fold of that signal for longer than
the unsealed ingest span that the configuration implies. Each signal is
folded by its own task:

- One signal alarms while the others stay fresh: that one task is dead.
- All signals alarm together: the process is gone, its store credentials no
  longer work, or tenant discovery fails every cycle.

**Confirm.** The gauge on `/metrics`, read per `signal`. Read
`ravel_catalog_fold_failures_total` for the same signal to tell a fold that
runs and fails from one that does not run at all. The fold task logs the
underlying error per tenant at `warn`.

**Action.** Act within a bounded time. The unsealed span grows for as long as
the alert holds. A cold recent-window query over a wide enough span is refused
because it exceeds its object-store request budget.

[The observability guide](../observability.md#the-fold-stalled-alert) carries
the rule with its expression, threshold and state-by-state behaviour, the
arithmetic behind the 4800, and the two limits on what the gauge can see.

### Scrub cursor stuck

**Symptom.** `ravel_scrub_cursor_position` stuck near 0 for longer than the
scrub period.

**Cause.** The scrubber does not keep pace with `--scrub-period`, so the
effective staleness bound is not that period.

**Confirm.** The gauge, per signal, on the `/metrics` of the maintain process.

**Action.** Lengthen `--scrub-period`, or give the maintain process more read
bandwidth. Sustained scrub bandwidth is the corpus size divided by the period.

### Scrub cannot read

**Symptom.** `increase(ravel_scrub_unreadable_total[1h]) > 0`

**Cause.** The scrubber could not read an object or record, so it went
unverified this rotation. This is not corruption: the bytes were never read.
The `reason` label gives the cause:

| `reason` | Meaning |
|---|---|
| `reason="access_denied"` | A key policy, a bucket policy, or a credential fault on the maintain process. |
| `reason="permanent"` | Any other store error that a retry cannot clear, or a record whose bytes do not decode. |
| `reason="retry_exhausted"` | A read that kept failing with a retryable error (throttled, timeout, transient) through six held ticks and once more. The scrub then moved past it. |

**Confirm.** The counter, labeled by signal, `level` and `reason`, on the
`/metrics` of the maintain process. The ERROR log line beside each count names
the key. `ravel_scrub_marker_held_ticks` climbing toward 6 precedes a
`retry_exhausted` count.

**Action.**

| `reason` | Action |
|---|---|
| `access_denied` | Fix the read grant of the maintain role or the encryption-key policy of the object. The count recurs every rotation until then. |
| `permanent` | Read the named object with the same credentials to see the store's error. A record that does not decode needs the same investigation as a checksum mismatch. |
| `retry_exhausted` | Check the health of the store for that prefix. An error that the backend reports as transient and that never clears (an archived object, for example) lands here every rotation. |

### L0 records pending

**Symptom.**
`ravel_maintain_l0_records_pending{signal="metrics|logs|spans"}` climbs and
stays nonzero.

**Cause.** Buckets seal faster than they cross `min_compaction_inputs`
(default 2), so more L0 segments pile up uncompacted than the compactor
clears. A small, transient count is normal: a bucket with fewer than
`min_compaction_inputs` records waits for its next writer flush before it can
compact.

**Confirm.** The gauge, labelled by signal, on `/metrics`.

- The gauge is a per-process total, summed over every tenant and shard that
  the process maintains.
- It is republished once per maintenance cycle (default 300 s), after the
  cycle covers all of them. A mid-cycle scrape reads the complete total of
  the previous cycle, not a partial sum.
- Buckets that the interior memo skipped (`interior_reverify_ns`, default
  6 h) still contribute their last-known count. So the figure is the whole
  pending population on every cycle, not only what that cycle re-read.
- With several maintain replicas, sum the gauge across them. Each replica
  owns a disjoint share of the units.
- For one bucket,
  `ravel-cli maintain status --tenant <t> --signal <s> --shard <n> --hour <n>`
  prints the L0 record count as `l0_commit_records`. It does not print the
  `min_compaction_inputs` threshold. Read the threshold from the configuration
  of the maintain process and compare by hand.

**Action.** A steady count that tracks normal per-bucket flush cadence needs
no action. It clears when the next flush of each bucket crosses the
threshold.

A count that grows without bound, or that does not fall after
`ravel_maintain_units_stalled` clears, means that compaction does not keep
pace with ingest. Follow [stalled unit](#stalled-unit).

### No objects deleted

**Symptom.** `increase(ravel_maintain_objects_deleted_total[1h]) == 0` while
storage keeps growing.

**Cause.** Either nothing is eligible for deletion yet, or the sweep does not
reach this shard. The counter is labelled by `kind`, and each kind has a
by-design delay before it can move:

| `kind` | Waits for |
|---|---|
| `superseded_records_deleted`, `superseded_data_deleted` | The record of a compaction to clear the protection horizon (about 25 h on the defaults). |
| `quarantine_reaped` | `quarantine_horizon_ns` (7 days on the defaults), much the longest of the three. |
| `unreferenced_parts_deleted` | `grace + max_compaction_lifetime`. It also needs an `l1/` part to exist, which only a compaction writes. |

A zero on any single kind is expected for its own window and is not evidence
of a stuck sweep.

**Confirm.** The counter on `/metrics`, per `kind`. Compare against
`ravel_maintain_units_stalled` and the maintain pass log to tell "nothing
eligible yet" from "sweep is not running."

**Action.** A flat counter alone is not an alarm. Investigate only when
another symptom in this section is present:

- a stalled unit
- no maintain process at all
- storage growth that persists well past the protection horizon while
  compaction is otherwise healthy

### Retention lag

**Symptom.**
`min_over_time(ravel_maintain_retention_lag_seconds{signal=~"metrics|logs|spans"}[6h]) > 90300`
(with a `for: 6h` in the alert). 90300 s is one `protection_horizon` on the
defaults (25 h 5 min). Substitute your own.

**Cause.** The physical sweep of retention falls behind. The oldest
expired-but-still-present bucket for the signal sat past its retention
deadline longer than one protection horizon, so expired data is not deleted
on schedule.

A steady value near one horizon is the healthy floor: a tombstone waits out
the horizon before its physical sweep runs. The alarm is a value that stays
well above the floor or climbs. Use `min_over_time` and a long `for:`. Then a
single high scrape does not page, and neither does a brief rise while one
bucket expires before its sweep runs.

**Confirm.** The gauge, labelled by signal, on `/metrics`.

- The gauge is a per-process, per-cycle maximum over the units that the
  process owns, so it names the single worst bucket. With several maintain
  replicas, take the maximum across them, not the sum.
- The maintain pass log names why the physical sweep of a bucket was blocked:
  a HEAD-reachability block from a lagging or unreadable fold snapshot, an
  out-of-window format version, or a legal hold.
- Cross-read `ravel_maintain_units_stalled` and the age of
  `ravel_catalog_fold_last_success_timestamp_seconds`.
- `ravel-cli maintain status --tenant <t> --signal <s> --shard <n> --hour <n>`
  shows the tombstone and residue of a bucket.

**Action.**

| Block | Action |
|---|---|
| HEAD-reachability block | It clears when the fold's retention-frontier reconcile drops the bucket from the live snapshot. If the fold is stalled, fix that first: see [catalog fold stalled](#catalog-fold-stalled). |
| Out-of-window format version | Use a reader build that covers it. |
| Legal hold, or a version hold that you set | The hold keeps its bucket past the deadline by design, so this alert fires for as long as the hold stands. Accept the alert for that tenant, or exclude the held tenant from the alert. Do not release a hold to silence a page. |
| None of these, and the lag still climbs | The sweep does not reach the shard. Check that a maintain process runs, and check for a stalled unit. |

### No bytes reclaimed

**Symptom.** `increase(ravel_maintain_bytes_reclaimed_total[6h]) == 0` while
`ravel_maintain_retention_lag_seconds` climbs.

**Cause.** The sweep frees no bytes on the three size-known deletion paths,
while expired data accumulates. The three paths are quarantine reap,
unreferenced parts, and superseded inputs charged at their recorded
`object_size`.

A zero alone is not an alarm. This counter excludes retention deletions,
which delete by key without a known size. Storage can shrink from those while
the counter stays flat. Read it only against a climbing retention lag.

**Confirm.** The counter, labelled by signal, on `/metrics`. It is per
process, so sum across maintain replicas. Compare with
`ravel_maintain_objects_deleted_total{kind=...}`, which counts objects on the
same sweep paths. Neither counter sees retention deletions, and no metric
counts them. The retention lag is the series that shows the progress of
retention.

**Action.** Investigate [retention lag](#retention-lag). This counter
corroborates that entry and is not a primary alarm.

## Data integrity and correctness alarms

Page on any nonzero increase for the first four entries. None is a rate to
threshold.

The last entry is read differently. A drain that loses acknowledged rows is a
data-loss event on a process that is exiting, so its counters can go
unscraped. Its ERROR log line is the reliable signal.

### Checksum mismatch

**Symptom.** `increase(ravel_scrub_checksum_mismatch_total[1h]) > 0`

**Cause.** At-rest corruption. A whole object's hash does not match the
recorded content hash, from bit rot or a partial write, or a section checksum
fails.

**Confirm.** The counter on `/metrics`, labeled by signal and by `level`. The
`level` label names where the corrupt object came from:

| `level` | Object |
|---|---|
| `l0` | An original ingested segment |
| `l1` | A compaction output part |
| `rewrite` | A selective-erasure rewrite output part |

To confirm the extent, run `ravel-cli maintain verify-custody --tenant <t>`.
It re-hashes every live data object and exits nonzero on any anomaly.

**Action.** No redundant copy is kept to repair from, and the scrubber only
detects.

1. For a `level="l0"` mismatch, first check whether the hour is already
   compacted. An L0 segment that a live compaction has folded stays in the
   rotation. Its mismatch can name a redundant copy that the catalog already
   excludes from queries, not data that a query can still reach.
2. For any other mismatch, identify the affected objects with
   `verify-custody`.
3. Restore from your bucket-level controls. See
   [disaster recovery](../disaster-recovery.md).

### Postings disagreement

**Symptom.** `increase(ravel_scrub_postings_disagreement_total[1h]) > 0`

**Cause.** A covering name-index object omitted a name that the data object
carries. A query that filters on that name skips matching data and reports no
error.

**Confirm.** The counter, labeled by signal, on `/metrics`.

**Action.** Capture the affected tenant and signal, and escalate. This is a
correctness defect, not a capacity problem. Do not trust a query result for
that name that was computed while the counter is nonzero.

### Seal divergence

**Symptom.** `increase(ravel_scrub_seal_divergence_total[1h]) > 0`

**Cause.** The folded snapshot under-counts the sealed commit history.

| `reason` | Meaning |
|---|---|
| `reason="missing"` | A sealed record is absent from the snapshot. |
| `reason="mismatched"` | The content hash of a snapshot entry disagrees with the record. |

**Confirm.** The counter, labeled by signal and reason, on `/metrics`.
`ravel-cli catalog verify --tenant <t> --signal <s>` is the same check on
demand.

**Action.** Follow
[queries are missing recently written data](#queries-are-missing-recently-written-data).
Any increase is real. A snapshot entry with no surviving commit record is the
expected shape after retention deletes a folded record, and it is never
counted here.

### Isolation breach

**Symptom.** `increase(ravel_catalog_isolation_breach_total[5m]) > 0`

**Cause.** A cross-tenant key-layout or hashing fault. Either a tenant-hash
mismatch occurred on a catalog HEAD or index object, or a resolve listing
returned a key that does not begin with the prefix of the requesting tenant.

**Confirm.** The counter, labeled by mode, on `/metrics`. Every increment
corresponds to a query that already failed with an explicit isolation-fault
error, so the client saw it too.

**Action.** Escalate immediately. Every increment is a failed query and a real
isolation fault. The two anomaly counters rendered beside it are different:
they tally an overlap that the query resolves past.

Coverage is not complete, so a zero reading is not proof of isolation:

- A mismatch on a commit or compaction record fails its query and does not
  increment this counter.
- The tenant hash of a snapshot part is not checked against the requester at
  all.

### Shutdown lost acknowledged rows

**Symptom.** A graceful shutdown (rolling deploy, pod eviction) lost
buffered-mode rows that were already acknowledged to the client.

**Cause.** The clock of the node stepped back past the monotonic flush-open
floor on every retry pass of the drain. Each pass refused to open a flush and
re-inserted the tenant's buffer. Those rows were acked at enqueue in buffered
mode and are gone when the process exits (see
[Consistency model](../../consistency-model.md)).

A drain whose STORE calls stall is a different case. The buffer moves into a
spawned task before any store call, so the tenant is already out of the map
when the PUT hangs. That loss shows as `ravel_shutdown_drain_overrun_total`,
with this counter at 0.

**Confirm.** `ravel_ingest_flush_all_residue_tenants_total` on `/metrics`, by
signal.

- The counter is process-global and does not survive the process exit. A
  nonzero value on the `/metrics` of the dying process is the loss.
- A scrape against the NEXT process starts back at zero and tells you nothing
  about the prior one.
- A live scrape is unlikely to catch the event. See
  [Reachability during shutdown](../observability.md#reachability-during-shutdown).
- So grep the logs of that process for
  `ravel-ingest: flush_all left buffered tenants unflushed after exhausting retry passes`.
  The line names the shard, tenant count, and buffered point count.

**Action.** The rows already lost are not recoverable. Buffered mode returns
no commit token, so this side has nothing to replay from.

1. Identify the affected tenants and window from the log line.
2. Check whether the client can detect the loss and resend. Buffered mode has
   no idempotency key for metrics. Logs and spans can dedup a resend with
   `x-ravel-idempotency-key`.
3. If the loss recurs, investigate the clock of the node. Do not investigate
   the object store or `--shutdown-timeout`. A receding clock is the only
   condition that leaves residue here, and `--shutdown-timeout` only bounds
   how long the process waits before it gives up.

## Query cost and results

### Slow recent queries

**Symptom.** Queries over recent history are slow and issue far more
object-store requests than the tenant has objects.

**Cause.** The catalog fold is disabled or behind, so resolve lists commit
records per bucket and does not read a snapshot.

**Confirm.** `ravel-cli catalog inspect --tenant <t> --signal <s>` prints the
watermark. Compare it against the queried window. When no HEAD is present,
the command reports that and does not error. A logs tenant inspected without
`--signal logs` shows this.

**Action.** Fold the affected signal with
`ravel-cli catalog fold --tenant <t> --shards <n> --signal <s>`, and make sure
that `--disable-fold` is not set. The listing path does not scale past
approximately 10,000 commit records in one bucket.

### Slow logs or spans

**Symptom.** The logs or spans queries of a tenant are slow while its metrics
queries are fine.

**Cause.** Only metrics were folded. Each signal has its own snapshot object,
and both `catalog fold` and `catalog verify` default to metrics.

**Confirm.** `ravel-cli catalog inspect --tenant <t> --signal logs` reports no
HEAD.

**Action.** Fold and verify each signal that the tenant writes. Name
`--signal` every time.

### Wide scan fails

**Symptom.** A wide scan fails or truncates on a tenant with a lot of sealed
history.

**Cause.** The per-query segment cap. Only the recent set, approximately the
last two hours, is exempt from it.

**Confirm.** The query error names the cap.

**Action.** Raise `--max-segments` for that workload, or narrow the query
window. See [per-query budgets](configuration.md#per-query-budgets).

### Duplicate samples

**Symptom.** Duplicate samples at the same timestamp for one series.

**Cause.** Delivery is at-least-once. A client retry after a lost
acknowledgement re-ingests the same points, and both copies are stored. A
query takes the last value at a given timestamp.

**Confirm.** Compare the retry log of the client against the ingested points
for that window.

**Action.** This is expected behavior, not a defect. Log and span ingest
accept an optional `x-ravel-idempotency-key` header that collapses a retried
request within its dedup window. Use it on a client that retries. Metrics have
no such header.

### Typed column fallback climbs

**Symptom.** `ravel_typed_attr_columns_stale_fallback_total` climbs steadily.

**Cause.** The tenant config object is unreadable, so the typed attribute
column declarations in effect are not the ones written.

**Confirm.** The counter, labeled by mode, on `/metrics`.
`ravel-cli typed-attr-column show <tenant>` reads the durable record directly
and shows whether it is present, empty or absent.

**Action.** Fix the read failure if the counter keeps climbing. A brief rise
immediately after a config write is expected, because resolution is
cache-aside on a 60 second horizon.

### Replicas disagree on columns

**Symptom.** Two replicas answer the same query against different typed
attribute columns.

**Cause.** A `typed-attr-column set` is propagating. Resolution is per tenant
on a 60 second staleness horizon.

**Confirm.** `ravel-cli typed-attr-column show <tenant>` shows the durable
state. The divergence closes within the horizon.

**Action.** Wait out the horizon. If the divergence does not close, treat it
as [typed column fallback climbs](#typed-column-fallback-climbs).

## Profile server memory

**Symptom.** The resident memory of a server runs well above what its ledgers
account for (`ravel_memory_reserved_bytes` plus `ravel_cache_resident_bytes`).

**Confirm.** First read `ravel_process_allocator_bytes` on `/metrics`. It
splits the memory of the process three ways:

| Figure | Meaning |
|---|---|
| `stat="resident"` minus `stat="active"` | Pages that the allocator holds but has not returned to the operating system. A large figure here is allocator retention, and no buffer is to blame. |
| `stat="active"` minus `stat="allocated"` | Size-class fragmentation inside live allocations. |
| `stat="allocated"` | The live allocations. If this is close to resident, live buffers hold the memory, and a heap profile names the allocation sites. |

Freed pages that jemalloc has not yet returned count toward `stat="resident"`.
At startup the server enables jemalloc's background thread, which returns
them to the operating system on a timer. Without it, jemalloc returns them
only during later allocator calls, so an idle server keeps them. The startup
log stamps the state, as read back from the allocator:

```
INFO allocator background purge thread resolved allocator_background_thread=true source="server"
```

- `source="malloc_conf"` means that `_RJEM_MALLOC_CONF` sets
  `background_thread`, and the server left that setting as it was.
- `/metrics` reports the same state, read at scrape time, as
  `ravel_process_allocator_background_thread{allocator="jemalloc"}`: `1` when
  the thread is enabled, `0` when it is not.
- If the server cannot enable the thread, it logs a warning, stamps `false`,
  and starts anyway.
- To turn the thread off, start the server with
  `_RJEM_MALLOC_CONF=background_thread:false`. To combine it with the
  profiling options below, separate the options with commas.

**Action.** Take a heap profile:

1. Build the server with jemalloc's profiler compiled in. The profiler is off
   by default and is not in the published image. The binary lands at
   `target/release/ravel-server`:

   ```sh
   cargo build --release --locked -p ravel-server --features "sql,flight-sql,otap,heap-profiling"
   ```

   That is the feature set of the published image (the Dockerfile builds
   `sql`, `flight-sql` and `otap` with `--locked`) plus `heap-profiling`. To
   profile a different deployment, add `heap-profiling` to the feature list
   that the deployment was built with. Allocation sites under a surface that
   the profile lacks cannot appear in the dump.

2. Start that binary with profiling on, fresh for each reproduction, from an
   empty dump directory:

   ```sh
   rm -rf /tmp/ravel-heap && mkdir -p /tmp/ravel-heap
   _RJEM_MALLOC_CONF=prof:true,lg_prof_sample:17,prof_gdump:true,prof_prefix:/tmp/ravel-heap/g \
     target/release/ravel-server ...
   ```

   - The vendored jemalloc is built with an `_rjem_` symbol prefix, so it
     reads `_RJEM_MALLOC_CONF`, not `MALLOC_CONF`.
   - `prof_gdump` writes a dump each time the total virtual memory of the
     process exceeds its previous maximum. That tracks the live peak only
     while nothing earlier in the process mapped more. For this reason each
     reproduction starts a fresh server.
   - `lg_prof_sample:17` samples approximately every 128 KiB allocated.
   - jemalloc does not create the dump directory.

3. Reproduce the workload. Then read the last dump, which was written at the
   virtual-memory high-water of the run, with `jeprof` (Debian and Ubuntu
   ship it in `libjemalloc-dev`):

   ```sh
   jeprof --text --inuse_space target/release/ravel-server "$(ls -1v /tmp/ravel-heap/g.*.heap | tail -1)"
   jeprof --collapsed --inuse_space target/release/ravel-server "$(ls -1v /tmp/ravel-heap/g.*.heap | tail -1)"
   ```

   A dump is named `g.<pid>.<seq>.u<n>.heap`. The version sort picks the last
   dump only while the directory holds a single run. `--collapsed` gives full
   stacks for grouping by the innermost Ravel frame.

4. Before you trust the dump, compare its in-use total with
   `ravel_process_allocator_bytes` `stat="allocated"` observed at the peak of
   the workload. A large gap means one of two conditions: the sample interval
   is too coarse to attribute from, or the dump was not taken at the live
   peak.

A dump at every new high-water slows allocation-heavy statements, so measure
timings on a build without profiling.

## Background

Decision records behind this page:
[maintenance safety and coverage](../../adrs/0048-maintenance-safety-and-coverage.md),
[commit record reconstruction and the disaster-recovery posture](../../adrs/0058-commit-record-reconstruction-and-dr-posture.md),
[durability hardening](../../adrs/0059-durability-hardening.md),
[fail-closed isolation and startup invariants](../../adrs/0050-fail-closed-isolation-and-startup-invariants.md),
[leased distributed maintenance](../../adrs/0065-leased-distributed-maintenance.md),
and [query cost accounting](../../adrs/0044-query-cost-accounting.md).
