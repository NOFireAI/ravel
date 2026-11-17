# Retained-data compatibility

This page is the normative statement of which persisted formats a Ravel
build reads and writes, which releases may run beside each other on one
store, and which upgrade, rollback and downgrade paths are supported.
[ADR-4233](adrs/4233-retained-data-compatibility-contract.md) decides the
contract; this page states it as the code on `main` implements it. The
operator procedure is
[Rolling a format bump](guides/operations/maintenance.md#rolling-a-format-bump)
in the maintenance guide.

## Terms

- **Family.** A persisted format whose write version is governed through
  `sys/compat`. `KNOWN_FAMILIES` in `ravel_catalog::compat` names twelve:
  the data families `rseg`, `rlog` and `rspan`; the catalog families
  `catalog-head`, `csnap`, `npost`, `cstat` and `kidx`; and the record
  families `commit-record`, `compaction-record`, `rewrite-record` and
  `retention-tombstone`.
- **Baseline release.** The first release that ships the `sys/compat`
  writer policy together with `ravel-cli` commands that create and
  activate `sys/compat` (ADR-4233 decision 1 and its baseline-release
  amendment). No published release is the baseline yet: v0.33.0 is the
  newest release, and `ravel-cli` on `main` has no `compat` subcommand.
- **Baseline version.** The version a family is written at before any
  activation: the value `compat init` records for it, and the value every
  writer emits while `sys/compat` is absent.
- **Active version.** The family's `active_write_version` in `sys/compat`.
- **Activation.** The one-step raise of a family's active version and of
  the store-wide `compat_epoch`, followed by the fence walk (ADR-4233
  decisions 3 and 4).
- **Certificate.** The census result `compat certify` writes into a class A
  family's entry; the only evidence a release that drops that family's
  older reader accepts (ADR-4233 decision 7).
- **Classes A to D.** The migration classes of ADR-0066 decision 4: A, bulk
  data objects; B, derived catalog objects; C, metadata records; D,
  identity and domain-hash encodings.

## Supported starting releases

- **v0.33.x and later.** A store written by v0.33.x is adopted in place by
  `compat init` (ADR-4233 decision 1). v0.33.0 writes RSEG 7, RLOG 5 and
  RSPAN 5, which are the baseline versions of `rseg`, `rlog` and `rspan`.
- **Before v0.33.** Stores, data and processes from releases before v0.33.0
  are outside the contract. No test covers them and no bridge exists.
- **Decided by content.** Nothing in a store records which release wrote
  it, so "v0.33.x onward" is a support statement, not a check. `compat
  init` adopts any store whose objects all sit inside this build's read
  sets and refuses one that holds any other version
  (`CompatInitError::OutsideReadSet`, naming tenant, signal, family,
  version and count). The remedy for a refused object is to re-ingest or
  discard it; the retention version hold keeps it meanwhile.

## Persisted-format inventory

Read set and written version are those of the build on `main`. "Compat
family" names the `sys/compat` family that governs the written version; a
dash means the format is not an activation family. Every constant named
here is exported by its crate.

| Format | Key | Read set | Written | Class | Compat family | Constants |
|---|---|---|---|---|---|---|
| RSEG trailer | `m/l0`, `m/l1` `.rseg` | {7} | 7 | A | `rseg` | `ravel_segment::SUPPORTED_VERSIONS`, `SegmentVersion::WINDOW`, `VERSION_V7` (and `VERSION`, `VERSION_V2` to `VERSION_V6`, no longer read), `ravel_ingest::SEGMENT_FORMAT_VERSION`, `ravel_maintain::build::OUTPUT_FORMAT_VERSION` |
| RLOG trailer | logs, alerts and audit data objects | {5} | 5 | A | `rlog` | `ravel_logseg::footer::VERSION`, `ravel_logseg::footer::SUPPORTED_VERSIONS`, `ravel_ingest::LOG_SEGMENT_FORMAT_VERSION`, `ravel_maintain::rlog::OUTPUT_FORMAT_VERSION` |
| RLOG POSTINGS | inside RLOG | {1,2} | 2 | A, nested | `rlog` | `ravel_logseg::postings::POSTINGS_VERSION`, `POSTINGS_VERSION_V1` |
| RLOG and RSPAN KEY_IDX | inside RLOG and RSPAN | {1,2} | 2 | A, nested | `rlog`, `rspan` | `ravel_codec::key_idx::KEY_IDX_VERSIONS`, `KEY_IDX_VERSION_1`, `KEY_IDX_VERSION_2`, `ravel_logseg::key_index::RLOG_KEY_IDX_VERSION` |
| RSPAN trailer | spans data objects | {5} | 5 | A | `rspan` | `ravel_rspan::footer::VERSION`, `ravel_rspan::footer::SUPPORTED_VERSIONS`, `ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION`, `ravel_maintain::rspan_codec::OUTPUT_FORMAT_VERSION` |
| Format-drill version | test builds only | N and N+1 | N+1 once activated | A | `rseg`, `rlog`, `rspan` | `ravel_segment::VERSION_DRILL`, `ravel_logseg::footer::VERSION_DRILL`, `ravel_rspan::footer::VERSION_DRILL` |
| Commit record | `c/<shard>/<hour>/…cmt` | {1} | 1 | C, immutable | `commit-record` | `ravel_commit::record::FORMAT_VERSION` |
| Compaction record | `l1.<h16>.cmt` | {1,2} | by content: 1, or 2 for a `--reencode-compaction-parts` run | C, immutable | `compaction-record` | `ravel_commit::record::COMPACTION_FORMAT_VERSION`, `COMPACTION_SUPERSEDING_FORMAT_VERSION` |
| Rewrite record, erasure request, completion | `rw.`, `del/` | {1} | 1 | C, immutable | `rewrite-record` | `ravel_commit::erasure::FORMAT_VERSION` |
| Retention tombstone | `retire.tmb` | {1} | 1 | C, immutable | `retention-tombstone` | `ravel_commit::record::TOMBSTONE_FORMAT_VERSION` |
| Catalog HEAD | `catalog/<signal>/HEAD` | {1,2} | by content: 2 only when a part ref carries a retired hour | B | `catalog-head` | `ravel_catalog::snapshot_format::head::HEAD_FORMAT_VERSIONS`, `HEAD_FORMAT_VERSION_1`, `HEAD_FORMAT_VERSION_2` |
| `.csnap` part | catalog part | {1} | 1 | B | `csnap` | `ravel_catalog::snapshot_format::VERSION` |
| `.npost` part | catalog part | {1} | 1 | B | `npost` | `ravel_catalog::snapshot_format::POSTINGS_VERSION` |
| `.cstat` object | catalog part | {3} | 3 | B | `cstat` | `ravel_catalog::snapshot_format::COLUMN_STATS_ACCEPTED_READ_VERSIONS` |
| `.kidx` leaf | catalog part | {1,2,3}: 1 for every signal but logs, 2 for every signal, 3 for metrics only | 2 | B | `kidx` | `ravel_catalog::snapshot_format::kidx::KEY_INDEX_LEAF_FORMAT_VERSION`, `KEY_INDEX_LEAF_FORMAT_VERSION_1`, `KEY_INDEX_LEAF_FORMAT_VERSION_3`, `KEY_INDEX_LEAF_FORMAT_VERSIONS_KNOWN` |
| `sys/compat` | `sys/compat` | {1} | 1 | C, CAS-mutable | none | `ravel_catalog::compat::COMPAT_FORMAT_VERSION`, `COMPAT_MIN_READ_VERSION`, `COMPAT_MAX_READ_VERSION` |
| Provisioning record | `t/<h>/<signal>/prov` | {1,2,3,4} | 3; 4 only when it carries `compat_epoch` and `compat_fenced_unix_ns` | C, CAS-mutable | none | `ravel_catalog::provisioning::PROVISIONING_FORMAT_VERSION`, `PROVISIONING_FENCED_FORMAT_VERSION`, `PROVISIONING_MIN_READ_VERSION`, `PROVISIONING_MAX_READ_VERSION` |
| Tenant config | `t/<h>/config` | {1,2,3,4} | 2; 3 and 4 behind the storage-layout opt-in | C, CAS-mutable | none | `ravel_catalog::tenant_config::TENANT_CONFIG_FORMAT_VERSION`, `TENANT_CONFIG_MIN_READ_VERSION`, `TENANT_CONFIG_MAX_READ_VERSION`, `TENANT_CONFIG_STORAGE_LAYOUT_WRITER_VERSION`, `TENANT_CONFIG_KEY_INDEX_WRITER_VERSION` |
| Metric metadata | `t/<h>/m/meta` | {1,2} | 2 | C, CAS-mutable | none | `ravel_catalog::metrics_meta::METRICS_META_FORMAT_VERSION`, `METRICS_META_MIN_READ_VERSION`, `METRICS_META_MAX_READ_VERSION` |
| Key epoch record | per tenant | {1} | 1 | C, CAS-mutable | none | `ravel_catalog::key_epoch::KEY_EPOCH_FORMAT_VERSION`, `KEY_EPOCH_MIN_READ_VERSION`, `KEY_EPOCH_MAX_READ_VERSION` |
| Auth token map | `sys/auth` | {1,2} | 2 | C, CAS-mutable | none | `ravel_catalog::auth_token_map::AUTH_TOKEN_MAP_FORMAT_VERSION`, `AUTH_TOKEN_MAP_MIN_READ_VERSION`, `AUTH_TOKEN_MAP_MAX_READ_VERSION` |
| GC configuration | `sys/gc` | up to 2 | 1 at bootstrap, 2 when a HEAD cache TTL is recorded | C, CAS-mutable | none | `ravel_maintain::gc_config::GC_FORMAT_VERSION`, `GC_FORMAT_VERSION_V1` |
| Unnamed-tenant marker | per tenant | {1} | 1 | C | none | `ravel_maintain::unnamed_marker::UNNAMED_MARKER_FORMAT_VERSION` |
| Parquet table manifest and grants | `t/<h>/pq/` | {1}, {1} | 1, 1 | C | none | `ravel_pqtable::manifest::PARQUET_TABLE_FORMAT_VERSION`, `PARQUET_TABLE_MIN_READ_VERSION`, `PARQUET_TABLE_MAX_READ_VERSION`, `ravel_pqtable::grants::PARQUET_GRANTS_FORMAT_VERSION`, `PARQUET_GRANTS_MIN_READ_VERSION`, `PARQUET_GRANTS_MAX_READ_VERSION` |
| Idempotency marker | `idem/…idm` | {1} | 1 | C, write-once | none | private to `ravel_ingest::idempotency` |
| Tenancy marker, recovery manifest | `sys/tenancy`, `sys/t/<h>` | {1} | 1 | D, write-once | none | the proto `format_version` field |
| Maintain heartbeat, compaction claim | `sys/maintain/…` | {2}, {1} | 2, 1 | advisory | none | `ravel_fleet::worker_set::HEARTBEAT_FORMAT_VERSION`, `ravel_fleet::claim::CLAIM_FORMAT_VERSION` |
| Alert state memo, alert lease | `t/<h>/a/…` | {1}, JSON | 1 | advisory | none | `ravel_server::alert_state_memo::ALERT_STATE_MEMO_FORMAT_VERSION` |
| Agents HEAD and objects | `ag/<id>/head`, `ag/<id>/o/<hash>` | {1,2}, {1} | 2, 1 | C | none | `ravel_agents_core::head::FORMAT_VERSION`, `INLINE_FORMAT_VERSION`, `OBJECT_FORMAT_VERSION` |
| Distributed-query wire protocol | network only | 5 | 5 | not persisted | none | `ravel_query::distrib::codec::PROTOCOL_VERSION`, `CAPABILITY_VERSION` |
| Commit token | client-held | `v2:` | `v2:` | D | none | the domain string in `ravel-types` |
| Series identity | domain `ravel-series-v1` | v1 | v1 | D | none | the domain string in `ravel-types` |
| Local disk cache | local disk only | RVCD 3 | 3 | none, a mismatch is a miss | none | private to `ravel-cache` |

Class D encodings (commit tokens and series identity) are never activated
through `sys/compat`; a change to one is its own ADR with per-bucket
containment (ADR-4233 decision 8).

## Regimes by family

Every family follows one of four regimes. A new version of any family is
activated, never just deployed: installing a build that can write N+1 does
not change what it writes.

| Regime | Families | Writer emits | Retiring the older reader |
|---|---|---|---|
| Class A, certified | `rseg`, `rlog`, `rspan` | the active version | only by a release that starts on a certificate (ADR-4233 decision 7) |
| Class B, catalog | `csnap`, `npost`, `cstat`, `kidx`, `catalog-head` | at or below the active version, which is a ceiling | no certificate; the release that drops a catalog version rebuilds parts below its window from commit records on its first fold |
| Content-versioned ceiling | `catalog-head`, `compaction-record` | the version the object's content needs, at or below the active version | as for the family's class |
| Immutable record, single version | `commit-record`, `rewrite-record`, `retention-tombstone` | the active version | never: an immutable record is never rewritten, so its older versions never converge |

The ceiling rule (ADR-4233 ceiling amendment): for a ceiling family a
writer refuses when the active version is not one its build can write, and
otherwise emits what its content needs, at or below the active one. The
fold checks every catalog family this way before it reads anything,
against the highest version a fold of that signal can emit, and refuses
the whole cycle, writing nothing, when that version is above the active
one. Compaction and `migrate` treat `compaction-record` as a ceiling: a
plain compaction writes version 1 records whatever the active version, and
a `--reencode-compaction-parts` run, which writes version 2, is refused
unless `compaction-record` is active at 2.

On `main` every family's writable set is its baseline alone, except
`compaction-record`, which this build writes at 1 and 2. Outside the
`format-drill` test feature no class A family has a version above its
baseline to activate.

## Reader and writer versions under `sys/compat`

- **Absent record.** A process treats an absent `sys/compat` as every
  family at its baseline version.
- **Refresh.** `ravel-server` builds one view of `sys/compat` per process
  in every mode on a keyed bucket (one started with
  `--tenant-hash-key-file`), reads it before any writer starts, and re-reads
  it every 60 seconds. On an unkeyed bucket it builds no view, and every
  writer emits its built-in version.
- **Staleness.** A writer refuses to start an object, and refuses every PUT
  attempt, once its last successful read of `sys/compat` is 10 minutes old
  or there has been none (`view_stale`). The exception is a blind view,
  below.
- **Blind view.** A view is blind while no read has succeeded, no GET of
  `sys/compat` has returned an object, and the refresh that finished last,
  if any, was refused with `AccessDenied`. A blind view writing to a bucket
  whose provisioning record carries no `compat_epoch` answers the baseline
  with no time bound, so a credential without the grant does not stop
  writers on a store that was never activated. A refresh that fails any
  other way before the first success suspends the exemption until a later
  refresh succeeds or is refused with `AccessDenied` again. A role without
  the grant never observes an activation: on a fenced bucket its writers
  refuse as stale, but the audit and legal-hold writers carry no
  provisioning record, so for them no fence ends the exemption and they
  keep writing the baseline version. That is why activation refuses unless the
  operator attests that every writer role can read `sys/compat` (ADR-4233
  blind-view amendment).
- **A build that cannot write the active version** refuses that family's
  publishes with `ActiveVersionNotWritable` (retryable), counted under
  `ravel_compat_publish_refused_total{family,reason}`. It still starts,
  serves queries over what it reads, folds, and runs the retention version
  hold.
- **Missing record after an activation.** A writer refuses with
  `CompatRecordMissing` when `sys/compat` is absent but the bucket's
  provisioning record carries a `compat_epoch`, or the process has already
  seen one. That refusal is not retryable: restore `sys/compat` from bucket
  versioning; never re-create it at the baseline. `compat init` refuses a
  store whose provisioning records carry a `compat_epoch`.
- **Monotone views.** A view never lowers a version, an epoch or a
  certificate it has seen in this run, whatever a later read returns.
- **Newer records.** A `sys/compat` above `format_version` 1 is read as
  `CompatRecordNewer`, never as absent, and is never overwritten.

The writer rules for each pipeline are in
[ingest.md](ingest.md#format-activation-policy-adr-4233) for the three
ingest flushes, the
[maintenance guide](guides/operations/maintenance.md#writer-versions-under-syscompat)
for compaction, migration, erasure and the audit and legal-hold writers, and
the [alerting guide](guides/alerting.md#the-format-write-policy-and-the-alert-lease)
for the alert evaluator.

## Compatibility matrix

For a family F with baseline version N and a release that introduces N+1:

| Release | Reads F | Writes F | Runs beside |
|---|---|---|---|
| v0.33.x (pre-baseline) | N | N | the baseline and B+1, until the first activation above the baseline; after it, not at all |
| Baseline B | N | N | B+1 before and after activation; after activation it refuses F's publishes |
| B+1, introduces N+1 | N, N+1 | the active version | B and B+2 |
| B+2, retires N | N+1 | N+1 | B+1; starts only on a certificate |

A release never ships two new versions of one family, and never ships a
writer for a version it cannot read (ADR-4233 decision 2).

## Upgrade paths

Supported:

1. v0.33.x to B: a rolling upgrade with no activation. Then run `compat
   init` once.
2. B to B+1: a rolling upgrade. B+1 writes N until F is activated.
3. On B+1: terminate every pre-baseline process, activate F at N+1,
   migrate, certify, and run the late-write pass.
4. B+1 to B+2: a rolling upgrade, which B+2 permits only on F's certificate.

Refused:

- B straight to B+2, and any store with `sys/tenancy` but no `sys/compat`:
  B+2 refuses to start (see [Refusal of unsupported paths](#refusal-of-unsupported-paths)).
- A store holding a version outside B's read sets: `compat init` refuses it.

## Mixed-version rollout

- Before any activation every baseline-or-later build writes the baseline
  version of every family, so builds of any supported release may be rolled
  in any order and run side by side.
- After F is activated at N+1, a build that cannot write N+1 refuses F's
  publishes (retryable) and fails closed with a typed `UnsupportedVersion`
  error on any N+1 object it is asked to read. It writes nothing it cannot
  read and serves every query that touches only N objects.
- A pre-baseline (v0.33.x) process must be gone before the first activation
  above the baseline. The provisioning and alert-lease fences stop the
  common returns of one (its request admission, folds, maintenance under
  `m`, `l` and `s`, queries and alert evaluation on fenced buckets), but
  they are defence in depth; the termination is the guarantee (ADR-4233
  prerequisite amendment).
- The ravel-operator rolls the gateway, query and maintain Deployments from
  one image and the ingest router from its own, each with a plain
  `RollingUpdate`. It performs no activation, termination check or
  certification.

## Rollback versus downgrade

The irreversible step is the first activation of a version some deployed
binary cannot read, not the binary install.

| Situation | Supported | What happens |
|---|---|---|
| B+1 installed, F not activated, roll back to B | yes, a rollback | B+1 wrote only versions B reads |
| F activated, no N+1 object published yet, roll back to B | the data is safe; F's writes stop | B sees F above what it writes and refuses F's publishes; the activation is not undone, so F's writes wait for a roll forward |
| F activated and N+1 objects exist, roll back to B | no, a downgrade | B refuses F's publishes and fails closed, typed, on N+1 objects, so queries touching them fail; nothing is deleted; the remedy is to roll forward |
| F activated, roll back to a pre-baseline release | no, outside the contract | excluded by the termination prerequisite; the fences make its admission, folds, maintenance under `m`, `l` and `s`, queries and alert evaluation fail closed on fenced buckets, and nothing stops its other paths (see [Residuals](#residuals)) |
| F certified, B+2 installed, roll back to B+1 | yes, a rollback | B+1 reads N+1 |

Activation never lowers a version, and no command lowers one. A deleted or
rolled-back `sys/compat` is restored from bucket versioning.

## Refusal of unsupported paths

- **`compat init`** refuses, writing nothing: an unkeyed store; a listing
  error or a key it cannot address; an object it cannot read; any object
  outside this build's read sets (named per tenant, signal, family and
  version); a provisioning record that already carries a `compat_epoch`;
  and an existing `sys/compat` that differs from the record it would write.
  An identical existing record is a no-op.
- **`compat activate`** refuses before any write: an unkeyed store; a
  missing attestation that every writer role can read `sys/compat`; an
  absent `sys/compat`; a family with no entry; a version this build does
  not write; a version not above the active one; and a version more than
  one above it.
- **`compat certify`** refuses any family but `rseg`, `rlog` and `rspan`,
  and refuses to start its census before `activated + H + P + T_req` and
  every provisioned bucket's `compat_fenced_unix_ns + G_pre` (plus the
  clock-skew allowance) have passed. Its census refuses on any member below
  the target, any member it cannot read, any trailer that disagrees with
  its record's stamp, and any incomplete listing.
- **The retired-reader startup gate.** A release that drops a class A
  reader starts, in every mode, only when the family carries an
  uncontradicted certificate at or above the oldest version it reads with
  the late-write watermark past the certified time. It treats a store with
  neither `sys/compat` nor `sys/tenancy` as fresh, and refuses one with
  `sys/tenancy` and no `sys/compat`. No current release has retired a
  reader, so the gate's reader set is empty.
- **Readers** fail closed with a typed error on a version they do not
  admit. ADR-4233 decision 4's hazard table lists, site by site, how the
  baseline refuses or holds an object written by a newer build instead of
  repairing, overwriting or deleting it (hazards H1 and H3 to H11). The
  alert evaluator refuses its tick only on a history object newer than its
  read set and skips an older one (ADR-4233 H3 amendment).

## Residuals

These are stated, not prevented:

- **A pre-baseline process that outlives the rollout.** Several v0.33.0
  write paths never read the provisioning record: the teardown flush and
  its idempotency markers, the metadata sink (`t/<h>/m/meta`), `t/<h>/enc`,
  `t/<h>/config`, audit maintenance, alert retention deletes and sweep, and
  several `ravel-cli` paths. A teardown data object lands at a version the
  baseline reads, and the certify census and the late-write pass flag it.
  Such a process can also write per-query audit records at the older RLOG
  version, which the late-write pass finds.
- **Alert and audit retention deletion.** A pre-baseline maintain process
  deletes alert history and audit objects by age with no version hold
  after an activation. No fence and no pass detects a deletion. The
  termination prerequisite is the only control (ADR-4233 audit retention
  amendment and prerequisite amendment).
- **A paused process.** A process paused between its staleness check and a
  PUT is bounded by no timer. Its late write is caught by the late-write
  pass, not prevented.
- **The baseline's ungated writers.** The baseline's metadata sink,
  idempotency markers and `enc` writer carry no `sys/compat` gate. That is
  safe only because none of those formats is an activation family. A future
  bump of any of them must add the gate first.
- **Restores.** Restoring a noncurrent object version from bucket
  versioning after certification can bring back an N object. Run the
  release that reads both versions, re-run `migrate`, then re-certify.
