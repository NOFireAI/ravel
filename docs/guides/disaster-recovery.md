# Disaster recovery runbook

Normative. This runbook defines Ravel's disaster-recovery posture: three
operator-owned configuration tiers, the platform-CLI controls each requires, a
deliberate verified restore procedure, and the rehearsal record from which the
only published RPO/RTO numbers come. It replaces the earlier honest-gap posture
statement and graduates it to a runbook an operator runs (ADR-0077 decision 1
and decision 5; this amends ADR-0058 decision 5).

The shape of the posture is deliberate and stated plainly: Ravel builds **no
in-product backup, export, or failover mechanism**. DR is operator-owned
bucket-level controls — versioning, noncurrent-version expiration, and
cross-region cross-account replication — specified normatively here, verified
where the platform can see them, and proven by a rehearsed restore (ADR-0077
Decision). Object storage remains the only durable backend; the replica is
written by the platform's replication channel, never by a Ravel process.

## The three tiers

Each tier states its erasure-bound consequence next to its protection. This
disclosure is load-bearing, not a footnote: turning on the standard S3 DR
controls (versioning, Object Lock) extends the physical erasure bound, and
ADR-0064 §7's tension between DR and the erasure guarantee is resolved by
stating that cost, never by hiding it (ADR-0077 decision 1). Do not soften or
omit it.

### Tier 0 — no DR (default)

Unversioned bucket, exactly today's ADR-0064 §7 baseline. Erasure bounds hold
as stated in [../consistency-model.md](../consistency-model.md) with no
modifiers. RPO/RTO: none; bucket loss is total loss. This remains a **supported
posture** — but its blast radius is total: every durable byte (data objects,
commit records, manifests, catalog snapshots, control objects under `sys/`)
lives in one bucket, and Ravel itself cannot recover from losing it (ADR-0077
Context, decision 1).

### Tier 1 — replicated (the recommended DR posture)

On the primary bucket:

- Versioning **ON**, paired with a `NoncurrentDays = E_v` noncurrent-version
  expiration rule and expired-delete-marker cleanup. This is precisely the
  "deliberately paired" configuration ADR-0064 §7.1 already sanctions, so no
  amendment to ADR-0064 is needed; this tier instantiates it (ADR-0077
  decision 1).

Replication to a replica bucket:

- Replication **v2 configuration** with `DeleteMarkerReplication` **enabled**,
  RTC **recommended**.
- The replica lives in a **different region** and a **different account**,
  encrypted under a **different KMS key** (`ReplicaKmsKeyID`), and holds its own
  `NoncurrentDays = E_v_r` expiration rule.
- Ravel processes never hold replica-account credentials; the replication
  channel is the only writer to the replica (ADR-0077 decision 1).

**Erasure consequence, disclosed:** the primary physical erasure bound gains
`+E_v` (already an ADR-0064 §4 modifier). The replica's copy of an erased
subject is physically gone within replication lag + `E_v_r` after the primary
sweep — **provided `DeleteMarkerReplication` is enabled**. See the mandate
below (ADR-0077 decision 1).

### Tier 2 — Tier 1 plus Object Lock

Bucket-default retention `D` on the primary (and/or replica). Erasure
consequence per ADR-0064 §6: the physical erasure bound becomes
`max(bound, D)`; query-time exclusion stays immediate. ADR-0064 §6's
instruction carries forward unchanged: prefer scoped legal holds over blanket
default retention, or keep `D` inside the erasure SLA.

Tier 2 is **supported, but not part of the recommended DR baseline** (ADR-0077
decision 1, and Rejected Alternatives). Object Lock's only marginal protection
over Tier 1 is against a compromised primary credential purging version
history — and Tier 1 already contains that threat: version-id permanent deletes
are never replicated, the replica lives in an account whose credentials Ravel
never holds, and the replica retains deleted data as noncurrent versions for
`E_v_r`. Making Object Lock mandatory would impose `max(bound, D)` on every
DR-adopting deployment's erasure bound to defend against a threat the
cross-account replica already covers. Deployments whose compliance regime
demands WORM take Tier 2 as a deliberate choice, with the erasure consequence
disclosed.

## `DeleteMarkerReplication` is mandatory for erasure-obligated deployments

Every Ravel delete issues a **simple DELETE** (`object_store` never deletes by
version id). On a versioned bucket a simple DELETE becomes a **delete marker**,
and a delete marker replicates to the replica **only when
`DeleteMarkerReplication` is enabled** (ADR-0077 Context, replication facts).

Therefore, for any deployment with erasure obligations,
`DeleteMarkerReplication` is **MANDATORY**. Omitting it has a concrete,
non-negotiable consequence: **erased bytes persist on the replica
indefinitely**, because the delete marker that would reap them never arrives.
That configuration is **unsupported** for any deployment with erasure
obligations (ADR-0077 decision 1; mirrored in
[../consistency-model.md](../consistency-model.md) deletion guarantees).

Note the deliberate asymmetry this buys: version-id permanent deletes are
**never** replicated at all, so a compromised primary credential cannot purge
the replica through the replication channel (ADR-0077 Context) — the property
that lets the cross-account replica stand in for Object Lock in Tier 1.

## `E_v` is one knob controlling two windows

The load-bearing design insight of Tier 1 (ADR-0077 decision 1, stated
verbatim): **`E_v` is one knob controlling two windows.**

- It is the **disaster-detection budget**: after an accidental or malicious
  mass delete, the operator has `E_v_r` — and `E_v`, if the deletes were simple
  deletes — to notice and restore noncurrent versions.
- It is simultaneously the **erasure-residue window**: erased bytes persist as
  noncurrent versions for `E_v`.

Choosing `E_v` is a compliance decision, not a tuning default. Set it
deliberately against **both** your detection SLO and your erasure SLA. This
runbook refuses to pick a number for you.

## Platform-CLI verification checklist

Ravel cannot enforce most of this and does not pretend to. `store qualify`
already probes whether Object Lock/versioning appears enabled and reports
lifecycle-rule state (ADR-0064 §7 teeth), but **replication configuration is
invisible to `object_store`** — so verification of the replication controls is
an explicit platform-CLI step, not something Ravel checked (ADR-0077
Consequences). Run these against the actual buckets:

```sh
# Primary: versioning ON
aws s3api get-bucket-versioning --bucket <primary>

# Primary: NoncurrentDays = E_v expiration + expired-delete-marker cleanup
aws s3api get-bucket-lifecycle-configuration --bucket <primary>

# Primary: replication v2, DeleteMarkerReplication enabled, RTC (if required)
aws s3api get-bucket-replication --bucket <primary>

# Replica: versioning ON, NoncurrentDays = E_v_r expiration
aws s3api get-bucket-versioning --bucket <replica>
aws s3api get-bucket-lifecycle-configuration --bucket <replica>

# Tier 2 only: bucket-default Object Lock retention D
aws s3api get-object-lock-configuration --bucket <primary>
```

Confirm in the replication output that `DeleteMarkerReplication` is `Enabled`
(the mandate above), that the destination bucket is in a different account and
region under a different `ReplicaKmsKeyID`, and — if you need a stated RPO —
that RTC (`ReplicationTime`) is enabled. MinIO's bucket replication is
equivalent for these purposes; use `mc replicate` equivalents against a MinIO
deployment (ADR-0077 Context).

## Restore procedure: the replica is a restore source, never a live failover target

The replica is asynchronous, has no cross-bucket CAS, and its listing
consistency covers only what has arrived. Pointing a live Ravel deployment at
the replica on outage would **silently violate** the commit-then-visible
ordering, the seal/GC/compaction reasoning, and the sweeper's re-verify LIST: a
data object could be present without its commit record (or a record without its
data) because replication reordered or lagged them. So there is **no automatic
or live failover**, and no Ravel code path learns about a second bucket.
Restore is a deliberate, verified operation (ADR-0077 decision 2, Rejected
Alternatives).

1. **Freeze.** Stop every Ravel process writing to the lost or suspect primary
   (region loss usually does this for you). Nothing may write to the restore
   target until step 5.
2. **Choose the restore bucket.** Either promote the replica in place or copy it
   to a fresh bucket. Both are sanctioned: objects are immutable and
   content-addressed, so every replicated object is bit-identical to its
   original; the only skew is presence/absence.
3. **Reconcile to a consistency point.** This repairs replication's lack of
   ordering, using tools that all exist today:
   - `ravel-cli maintain verify-custody` (versioning-aware mode, ADR-0064 §7)
     finds **dangling commit records** — record replicated, data object not.
     Quarantine each (delete under the restore credential, with maintenance
     stopped — the ADR-0058 stop-maintain-first discipline; see
     [operations.md](operations.md), section "Stop maintenance before restoring
     or reconstructing commit records"). Each is counted as data loss against
     the measured RPO.
   - `ravel-cli commit reconstruct` (ADR-0058) recovers the opposite skew:
     **data object replicated, record not.** The object's footer carries
     everything a rebuilt record needs. Because ingest completes the data PUT
     before building the record, this converts "the record lagged replication"
     from loss into recovery — the effective RPO is the replication lag of the
     *data object*, not of the record pair.
   - `ravel-cli catalog verify` classifies catalog staleness. Catalog objects
     are derived; the fold rebuilds them over whatever the reconciled
     commit-record set is.
   - `sys/` control objects are each either self-healing (heartbeats,
     qualification — rewritten by the owning process on startup) or
     `CreateIfAbsent`-idempotent (seal records, provisioning). Any `sys/`
     object found not to self-heal is a **blocking finding**, not a footnote.

   **Lag beyond the protection horizon.** Skew between related maintenance
   objects is bounded by the horizons that already gate the machinery: a
   compaction or rewrite record is published at least `protection_horizon`
   (~25 h with defaults) before the sweep deletes its inputs, and an erasure
   `.dreq` is deleted at least `protection_horizon` after its `.done`. Within
   that envelope the reconciliation above is complete and the RPO definition
   below holds. If replication lag at disaster time may have exceeded
   `protection_horizon` (no RTC, replication degraded for a day or more), you
   must **also treat erasure state as suspect and re-submit any erasure request
   completed within the lag window**: a restored bucket could otherwise serve
   pre-rewrite inputs whose `RewriteRecord` — or whose exclusion-keeping
   `.dreq` — never arrived. Superseded-but-unswept compaction duplicates need no
   such care: overlap harmlessness holds for compaction, and only for compaction
   (ADR-0064 decision 3).
4. **Verify before serving.** verify-custody clean, catalog verify clean, and a
   canary query set over known-ingested data.
5. **Resume.** Start Ravel against the restored bucket. Disposable compute pays
   off here: processes mint fresh writer ids and epochs, no local state exists
   to reconcile, and the operator issues fresh per-role credentials
   (ADR-0055/0072) scoped to the restore bucket.
6. **Re-protect.** Re-establish versioning, lifecycle rules, and replication to
   a new replica before declaring the incident closed. An unreplicated restored
   primary is Tier 0.

## RPO and RTO: defined here, published only from a rehearsal

Per ADR-0077 decision 3, the RPO/RTO numbers must come from a real rehearsal,
not from estimation. This runbook therefore publishes **no number**. It defines
what the numbers mean and where they come from:

- **RPO** = the replication lag of acked data at disaster time, plus any
  dangling-record quarantine from step 3. With RTC enabled it has a published
  ceiling (15 minutes for 99.99% of objects, S3's SLA); **without RTC it has no
  bound.** A deployment that needs a stated RPO enables RTC.
- **RTO** = wall-clock time from freeze to verified resume (steps 1–5),
  dominated by reconciliation and scaling with the restored object count. It is
  deployment-sized and cannot be honestly stated in the abstract.
- **Publication rule:** the rehearsal record below carries the measured numbers.
  Until the first rehearsal record exists, the fields read **"unmeasured."** No
  number is invented to fill them. A rehearsal that surfaces a blocking finding
  (a non-self-healing `sys/` object, a reconciliation step that fails) blocks
  publication until fixed. Rehearsals re-run when the restore-relevant machinery
  changes materially, and the record keeps its history.

## Automated rehearsal workflow

`scripts/dr-rehearsal/restore-rehearsal.sh` (issue #814) automates restore
procedure steps 1-4 against a real MinIO/S3 replica and measures RPO/RTO,
rather than leaving the rehearsal above as an entirely manual exercise:

1. Refuses to proceed unless the operator passes
   `--confirm-writers-stopped` (and, optionally, `--primary-http` for a
   best-effort reachability check that the primary is actually down) —
   before touching the replica, restore bucket, or reconciliation tools
   (restore procedure step 1).
2. Refuses to restore into a non-empty restore bucket (step 2): the
   rehearsal proves the procedure, and a restore bucket that already
   holds objects would let reconciliation pass by reading pre-existing
   data instead of the copy it is meant to verify.
3. Mirrors the replica into the restore bucket, then measures RPO
   immediately as the object-count delta between the two buckets, before
   reconciliation has any chance to explain or repair the difference.
4. Runs `maintain verify-custody`, then `commit reconstruct`, then
   `catalog verify`, then a canary query, in that order, short-circuiting
   on the first failure (step 3-4). Runs `--inject-fault
   dangling-commit-record|missing-data|canary-query-error` prove each
   stage actually catches its fault (see `services/ravel-cli/tests/
   dr_rehearsal_fault_injection.rs` for the first two, proven at the
   library level; the canary fault is proven at the shell level, see
   below).
5. Gates the temporary canary query-mode server on a nonce-stamped file
   written only after reconciliation succeeds
   (`dr_require_reconciliation_complete` in `scripts/dr-rehearsal/lib.sh`)
   — a mechanical check the script asserts even though it just wrote the
   stamp itself, not an assumption drawn from statement order. No
   long-lived service is ever started against the restore bucket; the
   query-mode process this script starts is temporary and exists only to
   run the canary check.
6. Writes RTO (wall-clock seconds from the writers-stopped freeze to a
   clean canary result) and RPO (lost-object count) to a JSON artifact
   at `<--artifact-dir>/rehearsal-report.json` (`--artifact-dir` defaults
   to a fresh `mktemp -d`, printed at the end of the run, and is never a
   path under this repository). `.github/workflows/dr-rehearsal.yml`
   uploads that file as the `dr-rehearsal-report` workflow artifact on
   every run, pass or fail.

Run `scripts/dr-rehearsal/restore-rehearsal.sh --check` to validate the
script's structure and dependencies without starting MinIO or touching any
bucket; this is the only mode a MinIO/S3-less environment (a fleet
executor's sandbox has no reachable docker daemon) can exercise. A real
run needs a human or CI runner with a real or MinIO-backed S3 endpoint;
see the flags documented in `restore-rehearsal.sh --help`.

### Pre-registered RPO/RTO bands (issue #814)

Per the measurement-discipline rule that a figure gets a predicted band
before it is measured, not after: the rehearsal fixture is one L0 metrics
segment (the `gen_otlp_fixture` example's single `demo_requests_total`
series) copied into an otherwise-empty restore bucket, reconciled across
4 shards and 2 signals (metrics, logs — logs finds nothing, correctly).

- **RPO (lost objects) — expected 0, miss if > 0.** The rehearsal stops
  the seed writer before mirroring and mirrors synchronously before any
  reconciliation runs, so nothing should be in flight for the copy to
  miss. A clean (non-fault-injected) run measuring RPO > 0 means the
  mirror step itself dropped an object or a writer was not actually
  stopped, not that replication lag is being faithfully reproduced (this
  synthetic rehearsal has no replication lag to measure — that number
  comes from a real deployment's replicated bucket, per "RPO and RTO:
  defined here, published only from a rehearsal" above).
- **RTO (wall-clock seconds, freeze to clean canary) — expected 3-60s,
  miss if < 1s or > 120s.** `dr_wait_for` polls once a second with caps
  of 30s (MinIO healthy), 60s (seed server reachable), and 60s (query
  server reachable); a single L0 segment reconciles in a small constant
  number of object-store calls, so the total should be dominated by
  container and process startup, not reconciliation work, for this
  fixture size. Under 1s would mean a step was skipped rather than run;
  over 120s approaches the sum of the wait ceilings and indicates a
  stall, not slow-but-real work.

These bands describe the tiny synthetic fixture the automated workflow
seeds, not a production-sized restore: RTO for a real deployment scales
with the restored object count and is not predicted here (see "RPO and
RTO: defined here, published only from a rehearsal" above). A rehearsal
run against this fixture that lands outside its band is investigated
before its number is recorded below; a run against a real replica gets
its own pre-registered band on the tracking issue for that rehearsal,
per the same discipline.

## Rehearsal record

A rehearsal drives the restore procedure above against a real replica and
records the measured outcome here. Until a real rehearsal produces them, the
RPO and RTO fields state **unmeasured** — no estimate is published in their
place (ADR-0077 decision 3).

| Field | Value |
|---|---|
| Date | _unrehearsed_ |
| Environment (tier, store, region/account layout) | _unrehearsed_ |
| Object count restored | _unrehearsed_ |
| **Measured RPO** | **unmeasured** |
| **Measured RTO** | **unmeasured** |
| Anomalies found (blocking / non-blocking) | _unrehearsed_ |

Append a new row per rehearsal; keep prior rows as history.

### Chaos-evidence rehearsal records

ADR-0077 decision 4 defines a separate process-kill chaos-evidence lane under
`scripts/chaos/` (kill ingest mid-flush; kill a maintain worker mid-compaction
with a sibling running). That lane's scripts are built by a separate task and
are **out of scope for this runbook.** Its runs are recorded under the same
rehearsal-record discipline as above; this section exists to hold their eventual
output. Until that lane exists and runs, there is nothing to record here.

## Summary

| Tier | Controls | Erasure-bound consequence | RPO/RTO |
|---|---|---|---|
| **Tier 0** (default) | Unversioned bucket | None; bounds as in consistency-model.md | None; bucket loss is total loss |
| **Tier 1** (recommended) | Primary: versioning + `NoncurrentDays = E_v` + expired-delete-marker cleanup. Replica: different region/account/KMS key, replication v2 with `DeleteMarkerReplication`, RTC recommended, `NoncurrentDays = E_v_r` | Primary `+E_v`; replica residue = replication lag + `E_v_r` (requires `DeleteMarkerReplication`) | Defined here; **unmeasured** until a rehearsal record exists. RTC gives RPO a 15-min ceiling; without RTC, unbounded |
| **Tier 2** (optional) | Tier 1 plus bucket-default Object Lock retention `D` | `max(bound, D)`; query-time exclusion still immediate | As Tier 1 |

`DeleteMarkerReplication` is mandatory for any erasure-obligated deployment;
omitting it leaves erased bytes on the replica indefinitely and is unsupported.
No in-product backup, export, or failover exists; the replica is a restore
source only, reconciled with `verify-custody` and `commit reconstruct` before it
is served.
